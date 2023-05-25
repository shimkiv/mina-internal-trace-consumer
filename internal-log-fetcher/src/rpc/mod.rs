pub mod filters;
pub mod handlers;

use tokio::task::JoinHandle;

use crate::SharedData;

pub fn spawn_rpc_server(rpc_port: u16, data: SharedData) -> JoinHandle<()> {
    tokio::spawn(async move {
        let api = filters::filters(data.clone());

        warp::serve(api).run(([0, 0, 0, 0], rpc_port)).await;
    })
}
