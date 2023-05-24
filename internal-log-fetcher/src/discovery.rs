// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::{path::Path, ObjectStore};
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;

type NodeAddress = String;

#[derive(Deserialize)]
struct MetaToBeSaved {
    remote_addr: String,
    peer_id: String,
    submitter: String,
    graphql_control_port: u16,
}

pub struct NodeData {
    is_block_producer: bool,
    peer_id: String,
    submitted: String,
}

pub struct DiscoveryParams {
    pub offset_min: u64,
    pub limit: usize,
    pub only_block_producers: bool,
}

pub struct DiscoveryService {
    gcs: GoogleCloudStorage,
    node_data: HashMap<NodeAddress, NodeData>,
}

fn offset_by_time(t: DateTime<Utc>) -> String {
    let t_str = t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let d_str = t.format("%Y-%m-%d");
    format!("submissions/{}/{}", d_str, t_str)
}

impl DiscoveryService {
    pub fn new() -> Self {
        let gcs = GoogleCloudStorageBuilder::from_env().build().unwrap();
        let node_data = HashMap::new();
        Self { gcs, node_data }
    }

    pub async fn discover_participants(
        &mut self,
        params: DiscoveryParams,
    ) -> Result<Vec<NodeAddress>, Box<dyn std::error::Error>> {
        let before = Utc::now() - chrono::Duration::minutes(params.offset_min as i64);
        let offset: Path = offset_by_time(before).try_into().unwrap();
        let prefix: Path = "submissions".into();
        let it = self.gcs.list_with_offset(Some(&prefix), &offset).await?;
        let mut results = HashSet::new();
        // TODO: do something with errors (probably add logger and report them)
        let list_results: Vec<_> = it
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;

        for object_meta in list_results {
            let bytes = self
                .gcs
                .get(&object_meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let meta: MetaToBeSaved = serde_json::from_slice(&bytes).unwrap();
            let colon_ix = meta.remote_addr.find(':').ok_or_else(|| {
                format!(
                    "wrong remote address in submission {}: {}",
                    object_meta.location, meta.remote_addr
                )
            })?;
            let addr = format!(
                "{}:{}",
                &meta.remote_addr[..colon_ix],
                meta.graphql_control_port
            );
            if results.contains(&addr) {
                continue;
            }
            results.insert(addr.clone());
            if params.limit > 0 && results.len() >= params.limit {
                break;
            }
        }
        Ok(results.into_iter().collect())
    }
}
