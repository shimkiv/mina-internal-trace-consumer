// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use structopt::StructOpt;

mod authentication;
mod graphql;
mod mina_server;
mod log_entry;
mod utils;

#[derive(Debug, StructOpt)]
#[structopt(about = "Internal logging fetcher.")]
struct Opts {
    #[structopt(name = "NODE_ADDRESS")]
    address: String,
    #[structopt(short = "p", long)]
    graphql_port: u16,
    #[structopt(short = "k", long, parse(from_os_str))]
    secret_key_path: PathBuf,
    #[structopt(short = "o", long, parse(from_os_str))]
    output_dir_path: PathBuf,
    #[structopt(long)]
    https: bool,
}

fn main() {
    let opts = Opts::from_args();
    let secret_key_base64 = std::fs::read_to_string(opts.secret_key_path).unwrap();
    let config = mina_server::MinaServerConfig {
        secret_key_base64,
        address: opts.address,
        graphql_port: opts.graphql_port,
        output_dir_path: opts.output_dir_path,
        use_https: opts.https,
    };
    let mut mina_server = mina_server::MinaServer::new(config);

    // TODO: whenever authorization fails after it initially worked
    // we have to keep retrying because that means that the node got restarted.
    // Also the old data has to be moved to another directory to start fresh (merging traces
    // from different node runs is not clean, because the new instance will re-process
    // the same blocks but through a different path, so it all gets mixed up).
    // Is it better to handle that here in the program, or have an external script do it?
    mina_server.authorize_and_run_fetch_loop();
}
