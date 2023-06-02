// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use warp::Filter;

use crate::SharedAvailableNodes;

use super::handlers::get_nodes_handle;

pub fn filters(
    available_nodes: SharedAvailableNodes,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    // Allow cors from any origin
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET"]);

    get_nodes(available_nodes).with(cors)
}

fn get_nodes(
    available_nodes: SharedAvailableNodes,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("nodes")
        .and(warp::get())
        .and(with_shared_data(available_nodes))
        .and_then(get_nodes_handle)
}

fn with_shared_data(
    available_nodes: SharedAvailableNodes,
) -> impl Filter<Extract = (SharedAvailableNodes,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || available_nodes.clone())
}
