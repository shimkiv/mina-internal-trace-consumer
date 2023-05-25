use reqwest::StatusCode;
use tracing::error;

use crate::{node::NodeIdentity, SharedData};

pub async fn get_nodes_handle(
    data: SharedData,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    match data.read() {
        Ok(read_locked_data) => Ok(warp::reply::with_status(
            warp::reply::json(&read_locked_data.clone()),
            StatusCode::OK,
        )),
        Err(e) => {
            error!("Lock poisoned: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&Vec::<NodeIdentity>::new()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}
