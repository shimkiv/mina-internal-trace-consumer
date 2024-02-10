// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use node::NodeIdentity;
use rpc::handlers::NodeDescription;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};
use structopt::StructOpt;
use tokio::{sync::RwLock, time};
use trace_consumer::TraceConsumer;
use tracing::{debug, error, info};

use crate::utils::{load_node_name_map, read_secret_key_base64};

mod authentication;
mod discovery;
mod graphql;
mod log_entry;
mod mina_server;
mod node;
mod rpc;
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
    #[structopt(short = "n", long, parse(from_os_str))]
    node_names_map: Option<PathBuf>,
    #[structopt(long)]
    db_uri: Option<String>,
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

struct NodeInfo {
    internal_tracing_port: u16,
    is_block_producer: Arc<std::sync::atomic::AtomicBool>,
    active: Arc<std::sync::atomic::AtomicBool>,
    name: Option<String>,
}

enum NodeDiscoveryMode {
    Fixed(NodeIdentity),
    Discovery(discovery::DiscoveryService),
}

pub struct Manager {
    opts: Opts,
    available_nodes: SharedAvailableNodes,
    nodes: HashMap<NodeIdentity, NodeInfo>,
    next_internal_tracing_port: u16,
    node_discovery: NodeDiscoveryMode,
    secret_key_base64: String,
    consumer_executable_path: String,
    node_names: HashMap<String, String>,
}

#[derive(Clone)]
pub struct SharedManager(pub Arc<RwLock<Manager>>);

#[derive(Clone)]
pub struct SharedAvailableNodes(pub Arc<RwLock<HashSet<NodeDescription>>>);

impl Manager {
    fn try_new(opts: Opts) -> Result<Self> {
        let secret_key_base64 = read_secret_key_base64(&opts.secret_key_path)?;
        let node_discovery = match &opts.target {
            Target::NodeAddressPort {
                address,
                graphql_port,
            } => NodeDiscoveryMode::Fixed(NodeIdentity {
                ip: address.clone(),
                graphql_port: *graphql_port,
                submitter_pk: None,
            }),
            Target::Discovery => {
                NodeDiscoveryMode::Discovery(discovery::DiscoveryService::try_new()?)
            }
        };
        let consumer_executable_path = std::env::var("INTERNAL_TRACE_CONSUMER_EXE")
            .unwrap_or_else(|_| "../_build/default/src/internal_trace_consumer.exe".to_string());

        let node_names = if let Some(ref node_names_map_path) = opts.node_names_map {
            load_node_name_map(node_names_map_path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            opts,
            available_nodes: SharedAvailableNodes(Arc::new(RwLock::new(HashSet::new()))),
            nodes: HashMap::new(),
            next_internal_tracing_port: 11000,
            node_discovery,
            secret_key_base64,
            consumer_executable_path,
            node_names,
        })
    }

    async fn discover(&mut self) -> Result<HashSet<NodeIdentity>> {
        match &mut self.node_discovery {
            NodeDiscoveryMode::Fixed(id) => Ok(HashSet::from_iter(vec![id.clone()].into_iter())),
            NodeDiscoveryMode::Discovery(discovery) => {
                info!("Performing discovery...");
                let participants = discovery.discover_participants().await?;

                info!("Participants: {:?}", participants);
                Ok(participants)
            }
        }
    }

    async fn update_nodes(&mut self) -> Result<()> {
        let current_uptime_nodes = self.discover().await?;
        let uptime_nodes = HashSet::from_iter(current_uptime_nodes.iter());
        let known_nodes = HashSet::from_iter(self.nodes.keys().cloned());
        let new_nodes = current_uptime_nodes.difference(&known_nodes);
        let (current_active_nodes, current_inactive_nodes): (HashSet<_>, HashSet<_>) =
            known_nodes.iter().partition(|s| {
                self.nodes
                    .get(s)
                    .unwrap()
                    .active
                    .load(std::sync::atomic::Ordering::Acquire)
            });
        let nodes_to_reactivate = current_inactive_nodes.intersection(&uptime_nodes);
        let nodes_to_deactivate = current_active_nodes.difference(&uptime_nodes);

        info!(
            "State before update: known={} active={} inactive={} discovered={}",
            known_nodes.len(),
            current_active_nodes.len(),
            current_inactive_nodes.len(),
            uptime_nodes.len()
        );

        for node in new_nodes {
            let name = node
                .submitter_pk
                .as_ref()
                .and_then(|submitter| self.node_names.get(submitter));
            let node_info = NodeInfo {
                internal_tracing_port: self.next_internal_tracing_port,
                is_block_producer: Arc::new(AtomicBool::new(false)),
                active: Arc::new(AtomicBool::new(true)),
                name: name.cloned(),
            };
            info!(
                "New node {} at port {}",
                node.construct_directory_name(),
                node_info.internal_tracing_port
            );
            self.nodes.insert(node.clone(), node_info);
            self.next_internal_tracing_port += 1;
            if let Err(error) = self.spawn_node(node) {
                error!("Error when spawning node {:?}: {}", node, error);
            }
        }

        for node in nodes_to_reactivate {
            info!("Reactivate node {} ", node.construct_directory_name());
            if let Some(node_state) = self.nodes.get_mut(node) {
                info!(
                    "Reactivate node {} at port {}",
                    node.construct_directory_name(),
                    node_state.internal_tracing_port
                );
                node_state
                    .active
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            if let Err(error) = self.spawn_node(node) {
                error!("Error when spawning node {:?}: {}", node, error);
            }
        }

        for node in nodes_to_deactivate {
            info!("Deactivate node {} ", node.construct_directory_name());
            if let Some(node_state) = self.nodes.get_mut(node) {
                info!(
                    "Deactivate node {} at port {}",
                    node.construct_directory_name(),
                    node_state.internal_tracing_port
                );
                // Fetcher and consumer loop for this node will detect this change and shut down the loop
                node_state
                    .active
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Update the list of available nodes to contain only the currently active nodes
        let mut available_nodes = self.available_nodes.0.write().await;
        available_nodes.clear();
        available_nodes.extend(known_nodes.iter().filter_map(|s| {
            if let Some(node_state) = self.nodes.get(s) {
                if node_state.active.load(std::sync::atomic::Ordering::Acquire) {
                    Some(NodeDescription::from((s, node_state)))
                } else {
                    None
                }
            } else {
                None
            }
        }));

        Ok(())
    }

    fn spawn_node(&mut self, node: &NodeIdentity) -> Result<()> {
        debug!("Handling Node: {:?}", node);
        let node_dir_name = node.construct_directory_name();
        let output_dir_path = self.opts.output_dir_path.join(node_dir_name);
        let main_trace_file_path = output_dir_path.join(trace_consumer::internal_trace_file::MAIN);
        let db_uri = self.opts.db_uri.clone().unwrap_or_else(|| {
            let db_path = output_dir_path.join("traces.db");
            format!("sqlite3://{}", db_path.display())
        });

        if !output_dir_path.exists() {
            std::fs::create_dir_all(&output_dir_path).context(format!(
                "Creating output dir: {}",
                output_dir_path.display()
            ))?
        }

        let node_id = node.construct_directory_name();
        let node_state = self
            .nodes
            .get(node)
            .expect("Could not find node info that was expected to be there");
        let internal_trace_port = node_state.internal_tracing_port;

        info!("Creating thread for node (tracing port: {internal_trace_port}): {node_id}",);

        // cleanup old trace files before launching the consumer
        // to ensure that it doesn't load old data before this new run
        // truncates the files
        for name in trace_consumer::internal_trace_file::ALL {
            std::fs::remove_file(output_dir_path.join(name)).ok();
        }

        let config = mina_server::MinaServerConfig {
            secret_key_base64: self.secret_key_base64.clone(),
            address: node.ip.clone(),
            graphql_port: node.graphql_port,
            use_https: false,
            output_dir_path,
        };
        let consumer_executable_path = self.consumer_executable_path.clone().into();
        let active = Arc::clone(&node_state.active);
        let is_block_producer = Arc::clone(&node_state.is_block_producer);
        tokio::spawn(async move {
            let mut mina_server = mina_server::MinaServer::new(config);
            let fetch_loop_handle = mina_server.authorize_and_run_fetch_loop(is_block_producer);
            debug!(
                "Spawning consumer at port {}, with trace file: {}",
                internal_trace_port,
                main_trace_file_path.display()
            );
            let mut consumer = TraceConsumer::new(
                consumer_executable_path,
                main_trace_file_path,
                db_uri,
                internal_trace_port,
                node_id.clone(),
            );
            let mut consumer_handle = consumer.run().await.unwrap();

            let notify_exit = Arc::new(tokio::sync::Notify::new());
            tokio::task::spawn({
                let notify_exit = Arc::clone(&notify_exit);
                let active = Arc::clone(&active);
                async move {
                    loop {
                        if !active.load(std::sync::atomic::Ordering::Relaxed) {
                            notify_exit.notify_waiters();
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            });

            async fn kill_consumer_process(mut process: tokio::process::Child) {
                info!("Killing consumer process");
                if let Err(err) = process.kill().await {
                    error!("Failed to kill consumer subprocess: {err}");
                } else {
                    info!("Done killing consumer process");
                }
            }

            let stop_reason = tokio::select! {
                res = consumer_handle.wait() => {
                    if let Err(status) = res {
                        error!("consumer subprocess for node {node_id} exited with non-zero status: {status}");
                        "consumer failure"
                    } else {
                        "consumer exit"
                    }
                }
                res = fetch_loop_handle => {
                    if let Err(e) = res {
                        error!("Error when running authorize and fetch loop for node {node_id}: {}", e);
                        kill_consumer_process(consumer_handle).await;
                        "fetch loop error"
                    } else {
                        kill_consumer_process(consumer_handle).await;
                        "fetch loop exit"
                    }
                }
                _ = notify_exit.notified() => {
                    info!("Node deactivated by discovery: {node_id}");
                    kill_consumer_process(consumer_handle).await;
                    "discovery exit"
                }
            };

            // TODO: save data to another directory?

            info!("Finishing thread for node (tracing port: {internal_trace_port}) node_id={node_id} reason={stop_reason}",);

            active.store(false, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }
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

    let mut opts = Opts::from_args();

    opts.output_dir_path = opts
        .output_dir_path
        .canonicalize()
        .unwrap_or(opts.output_dir_path);
    info!("Output dir path: {}", opts.output_dir_path.display());

    let shared_manager = SharedManager(Arc::new(RwLock::new(Manager::try_new(opts)?)));

    let rest_port = 4000;
    info!("Spawning REST API server at port {rest_port}");
    rpc::spawn_rpc_server(
        rest_port,
        shared_manager.0.read().await.available_nodes.clone(),
    );
    tokio::spawn(async move {
        loop {
            {
                let mut manager = shared_manager.0.write().await;
                if let Err(error) = manager.update_nodes().await {
                    error!("Failure when updating list of nodes: {error}");
                }
            }
            time::sleep(time::Duration::from_secs(60)).await;
        }
    });

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

    Ok(())
}
