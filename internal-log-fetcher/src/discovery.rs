// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::{path::Path, ObjectStore};
use serde::Deserialize;
use std::collections::HashSet;
use tracing::info;

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
    gcs: GoogleCloudStorage,
}

fn offset_by_time(t: DateTime<Utc>) -> String {
    let t_str = t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let d_str = t.format("%Y-%m-%d");
    format!("submissions/{}/{}", d_str, t_str)
}

impl DiscoveryService {
    pub fn try_new() -> Result<Self> {
        let gcs = GoogleCloudStorageBuilder::from_env().build()?;
        Ok(Self { gcs })
    }

    pub async fn discover_participants(
        &mut self,
        params: DiscoveryParams,
    ) -> Result<HashSet<NodeIdentity>> {
        let before = Utc::now() - chrono::Duration::minutes(params.offset_min as i64);
        let offset: Path = offset_by_time(before).try_into()?;
        let prefix: Path = "submissions".into();
        let it = self.gcs.list_with_offset(Some(&prefix), &offset).await?;
        let mut results = HashSet::new();
        let list_results: Vec<_> = it.collect().await;
        info!("Results count {}", list_results.len());

        // TODO: do something with errors (probably add logger and report them)
        let list_results: Vec<_> = list_results
            .into_iter()
            .filter_map(|result| result.ok())
            .rev()
            .collect();

        let futures = list_results.into_iter().map(|object_meta| async move {
            let gcs = GoogleCloudStorageBuilder::from_env().build()?;
            let bytes = gcs.get_range(&object_meta.location, 0..1_000_000_000).await?;
            let meta: MetaToBeSaved = serde_json::from_slice(&bytes)?;
            Ok((object_meta.location, meta))
        });

        let gcs_results: Vec<anyhow::Result<(Path, MetaToBeSaved)>> =
            futures_util::future::join_all(futures).await;
        let gcs_results = gcs_results.into_iter().filter_map(|result| result.ok());

        for (location, meta) in gcs_results {
            let colon_ix = meta.remote_addr.find(':').ok_or_else(|| {
                anyhow!(
                    "wrong remote address in submission {}: {}",
                    location,
                    meta.remote_addr
                )
            })?;
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
