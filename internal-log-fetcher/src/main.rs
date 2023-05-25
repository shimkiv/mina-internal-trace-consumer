// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use node::NodeIdentity;
use std::path::PathBuf;
use structopt::StructOpt;
use trace_consumer::TraceConsumer;
use tracing::{error, info};

mod authentication;
mod discovery;
mod graphql;
mod log_entry;
mod mina_server;
mod node;
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
    #[structopt(about = "Use node discovery")]
    Discovery,
}

fn read_secret_key_base64(secret_key_path: &PathBuf) -> Result<String> {
    std::fs::read_to_string(secret_key_path)
        .with_context(|| format!("Failed to read secret key from {:?}", secret_key_path))
}

#[tokio::main]
async fn main() -> Result<()> {
    // set up logging
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let opts = Opts::from_args();
    let main_trace_file_path = opts.output_dir_path.join("internal-trace.jsonl");
    let secret_key_base64 = read_secret_key_base64(&opts.secret_key_path)?;

    // TODO for discovery
    // - x Remove cli args, not needed anymore (or instead add a subcommand to support fetching from specific node or discovery)
    // - x from the discovery, obtain the list of node addresses
    // - x For each result that was not seen before, create a `MinaServerConfig` and `MinaServer` with that config
    // - x Launch a new tokio task that runs the mina server handle `authorize_and_run_fetch_loop` with an unique
    //   output directory for the specific node
    // - Launch a consumer program instance with that directory as input

    let nodes: Vec<NodeIdentity> = match opts.target {
        Target::NodeAddressPort {
            address,
            graphql_port,
        } => vec![NodeIdentity {
            ip: address,
            graphql_port,
            submitter_pk: None,
        }],
        Target::Discovery => {
            let mut discovery = discovery::DiscoveryService::try_new()?;

            let participants = discovery
                .discover_participants(discovery::DiscoveryParams {
                    offset_min: 15,
                    limit: 10_000,
                    only_block_producers: false,
                })
                .await?;

            info!("participants: {:?}", participants);
            participants
        }
    };

    for node in nodes {
        let output_dir_path = opts.output_dir_path.join(node.construct_directory_name());
        if !output_dir_path.exists() {
            std::fs::create_dir(&output_dir_path)?
        }

        info!(
            "Creating thread for node: {}",
            node.construct_directory_name()
        );

        let config = mina_server::MinaServerConfig {
            secret_key_base64: secret_key_base64.clone(),
            address: node.ip,
            graphql_port: node.graphql_port,
            use_https: false,
            output_dir_path,
        };
        tokio::spawn(async move {
            let mut mina_server = mina_server::MinaServer::new(config);
            if let Err(e) = mina_server.authorize_and_run_fetch_loop().await {
                error!("Error: {}", e)
            }
        });
    }

    let mut signal_stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Unable to handle SIGTERM");

    tokio::select! {
        s = tokio::signal::ctrl_c() => {
            s.expect("Failed to listen for ctrl-c event");
            info!("Ctrl-c or SIGINT received!");
        }
        _ = signal_stream.recv() => {
            info!("SIGTERM received!");
        }
    }

    // TODO: make consumer exe path configurable
    // let mut consumer = TraceConsumer::new(
    //     "../_build/default/src/internal_trace_consumer.exe".into(),
    //     main_trace_file_path,
    //     3999,
    // );

    // tokio::spawn(async move {
    //     let _ = consumer.run().await;
    // });

    // TODO: whenever authorization fails after it initially worked
    // we have to keep retrying because that means that the node got restarted.
    // Also the old data has to be moved to another directory to start fresh (merging traces
    // from different node runs is not clean, because the new instance will re-process
    // the same blocks but through a different path, so it all gets mixed up).
    // Is it better to handle that here in the program, or have an external script do it?
    // mina_server.authorize_and_run_fetch_loop().await?;

    Ok(())
}
