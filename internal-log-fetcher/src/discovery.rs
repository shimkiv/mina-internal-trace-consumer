// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::env;
use anyhow::{Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{path::Path, ObjectStore};
use serde::Deserialize;
use std::collections::HashSet;
use tracing::{info, warn};

use crate::node::NodeIdentity;

#[derive(Debug, Deserialize)]
struct MetaToBeSaved {
    remote_addr: String,
    peer_id: String,
    submitter: String,
    graphql_control_port: u16,
}

pub struct DiscoveryParams {
    pub offset_min: u64,
    pub limit: usize,
    pub only_block_producers: bool,
}

pub struct DiscoveryService {
    gcs: AmazonS3,
    prefix: String,
}

fn offset_by_time(prefix_str: String, t: DateTime<Utc>) -> String {
    let t_str = t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let d_str = t.format("%Y-%m-%d");
    format!("{}/{}/{}", prefix_str, d_str, t_str)
}

impl DiscoveryService {
    pub fn try_new() -> Result<Self> {
        let prefix = env::var("AWS_PREFIX")?;
        let bucket = env::var("AWS_BUCKET")?;
        let gcs = AmazonS3Builder::from_env().with_bucket_name(bucket).build()?;
        Ok(Self { gcs, prefix })
    }

    pub async fn discover_participants(
        &mut self,
        params: DiscoveryParams,
    ) -> Result<HashSet<NodeIdentity>> {
        // We add 30 extra seconds of grace period because sometimes some nodes don't
        // make it in time.
        let before = Utc::now()
            - chrono::Duration::minutes(params.offset_min as i64)
            - chrono::Duration::seconds(30);
        let prefix_str = format!("{}/submissions", self.prefix);
        let prefix_str2 = prefix_str.clone();
        let offset: Path = offset_by_time(prefix_str, before).try_into()?;
        let prefix: Path = prefix_str2.into();
        info!("Obtaining list of objects in bucket...");
        let it = self.gcs.list_with_offset(Some(&prefix), &offset).await?;
        let mut results = HashSet::new();
        let list_results: Vec<_> = it.collect().await;
        info!("Results count {}", list_results.len());

        let list_results: Vec<_> = list_results
            .into_iter()
            .filter_map(|result| match result {
                Err(err) => {
                    warn!("Got error when fetching listing objects: {:?}", err);
                    None
                }
                Ok(result) => Some(result),
            })
            .rev()
            .collect();

        let futures = list_results.into_iter().map(|object_meta| async move {
            let bucket = env::var("AWS_BUCKET")?;
            let gcs = AmazonS3Builder::from_env().with_bucket_name(bucket).build()?;
            let bytes = gcs
                .get_range(&object_meta.location, 0..1_000_000_000)
                .await?;
            let meta: MetaToBeSaved = serde_json::from_slice(&bytes)?;
            Ok((object_meta.location, meta))
        });

        let gcs_results: Vec<anyhow::Result<(Path, MetaToBeSaved)>> =
            futures_util::future::join_all(futures).await;
        let gcs_results = gcs_results.into_iter().filter_map(|result| match result {
            Err(err) => {
                warn!("Failure when fetching object: {:?}", err);
                None
            }
            Ok(result) => Some(result),
        });

        for (location, meta) in gcs_results {
            let colon_ix = meta.remote_addr.find(':').unwrap_or_else(|| {
                meta.remote_addr.len()
            });
            results.insert(NodeIdentity {
                ip: meta.remote_addr[..colon_ix].to_string(),
                graphql_port: meta.graphql_control_port,
                submitter_pk: Some(meta.submitter),
            });
            if params.limit > 0 && results.len() >= params.limit {
                break;
            }
        }

        Ok(results)
    }
}
