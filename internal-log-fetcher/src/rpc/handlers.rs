// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use reqwest::StatusCode;
use serde::Serialize;

use crate::{node::NodeIdentity, NodeInfo, SharedAvailableNodes};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct NodeDescription {
    pub ip: String,
    pub graphql_port: u16,
    pub submitter_pk: Option<String>,
    pub internal_trace_port: u16,
    pub is_block_producer: bool,
    pub name: Option<String>,
}

impl From<(&NodeIdentity, &NodeInfo)> for NodeDescription {
    fn from(id_and_info: (&NodeIdentity, &NodeInfo)) -> Self {
        let (id, info) = id_and_info;
        Self {
            ip: id.ip.clone(),
            graphql_port: id.graphql_port,
            submitter_pk: id.submitter_pk.clone(),
            internal_trace_port: info.internal_tracing_port,
            is_block_producer: info
                .is_block_producer
                .load(std::sync::atomic::Ordering::Relaxed),
            name: info.name.clone(),
        }
    }
}

pub async fn get_nodes_handle(
    available_nodes: SharedAvailableNodes,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    let manager = available_nodes.0.read().await;
    let results: Vec<NodeDescription> = manager.iter().cloned().collect();
    Ok(warp::reply::with_status(
        warp::reply::json(&results),
        StatusCode::OK,
    ))
}
