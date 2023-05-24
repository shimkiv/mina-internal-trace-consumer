// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use structopt::StructOpt;
use trace_consumer::TraceConsumer;

mod authentication;
mod discovery;
mod graphql;
mod log_entry;
mod mina_server;
mod trace_consumer;
mod utils;

#[derive(Debug, StructOpt)]
#[structopt(about = "Internal logging fetcher.")]
struct Opts {
    #[structopt(subcommand)]
    target: Target,
    #[structopt(short = "k", long, parse(from_os_str))]
    secret_key_path: PathBuf,
    #[structopt(short = "o", long, parse(from_os_str))]
    output_dir_path: PathBuf,
    #[structopt(long)]
    https: bool,
}

#[derive(Debug, StructOpt)]
enum Target {
    #[structopt(about = "Specify Node address and port separately")]
    NodeAddressPort {
        #[structopt(name = "NODE_ADDRESS")]
        address: String,
        #[structopt(short = "p", long)]
        graphql_port: u16,
    },
    #[structopt(about = "Specify full URL")]
    FullUrl {
        #[structopt(name = "URL")]
        url: String,
    },
}

#[tokio::main]
async fn main() {
    // TODO for discovery
    // - Remove cli args, not needed anymore (or instead add a subcommand to support fetching from specific node or discovery)
    // - from the discovery, obtain the list of node addresses
    // - For each result that was not seen before, create a `MinaServerConfig` and `MinaServer` with that config
    // - Launch a new tokio task that runs the mina server handle `authorize_and_run_fetch_loop` with an unique
    //   output directory for the specific node
    // - Launch a consumer program instance with that directory as input
    if false {
        let mut discovery = discovery::DiscoveryService::new();

        let participants = discovery
            .discover_participants(discovery::DiscoveryParams {
                offset_min: 15,
                limit: 10_000,
                only_block_producers: false,
            })
            .await
            .unwrap();

        println!("participants: {:?}", participants);
    }
    let opts = Opts::from_args();
    let main_trace_file_path = opts.output_dir_path.join("internal-trace.jsonl");
    let secret_key_base64 = std::fs::read_to_string(opts.secret_key_path).unwrap();
    let config = mina_server::MinaServerConfig {
        secret_key_base64,
        target: opts.target,
        output_dir_path: opts.output_dir_path,
        use_https: opts.https,
    };
    let mut mina_server = mina_server::MinaServer::new(config);

    // TODO: make consumer exe path configurable
    let mut consumer = TraceConsumer::new(
        "../_build/default/src/internal_trace_consumer.exe".into(),
        main_trace_file_path,
        3999,
    );

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    // TODO: whenever authorization fails after it initially worked
    // we have to keep retrying because that means that the node got restarted.
    // Also the old data has to be moved to another directory to start fresh (merging traces
    // from different node runs is not clean, because the new instance will re-process
    // the same blocks but through a different path, so it all gets mixed up).
    // Is it better to handle that here in the program, or have an external script do it?
    mina_server.authorize_and_run_fetch_loop().await;
}
