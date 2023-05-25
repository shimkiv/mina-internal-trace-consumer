use warp::Filter;

use crate::SharedData;

use super::handlers::get_nodes_handle;

pub fn filters(
    data: SharedData,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    // Allow cors from any origin
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET"]);

    get_nodes(data).with(cors)
}

fn get_nodes(
    data: SharedData,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("nodes")
        .and(warp::get())
        .and(with_shared_data(data))
        .and_then(get_nodes_handle)
}

fn with_shared_data(
    data: SharedData,
) -> impl Filter<Extract = (SharedData,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || data.clone())
}