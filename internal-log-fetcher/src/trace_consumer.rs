// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::{fs::OpenOptions, path::PathBuf};

use tokio::process::Command;

pub mod internal_trace_file {
    pub const MAIN: &str = "internal-trace.jsonl";
    pub const VERIFIER: &str = "verifier-internal-trace.jsonl";
    pub const PROVER: &str = "prover-internal-trace.jsonl";

    pub const ALL: [&str; 3] = [MAIN, VERIFIER, PROVER];
}

pub struct TraceConsumer {
    consumer_executable_path: PathBuf,
    main_trace_file_path: PathBuf,
    db_uri: String,
    graphql_port: u16,
    node_identifier: String,
    // TODO: process handle here?
}

impl TraceConsumer {
    pub fn new(
        consumer_executable_path: PathBuf,
        main_trace_file_path: PathBuf,
        db_uri: String,
        graphql_port: u16,
        node_identifier: String,
    ) -> Self {
        Self {
            consumer_executable_path,
            main_trace_file_path,
            db_uri,
            graphql_port,
            node_identifier,
        }
    }

    pub async fn run(&mut self) -> tokio::io::Result<tokio::process::Child> {
        let base_path = self.main_trace_file_path.parent().unwrap().to_path_buf();

        let stdout_log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_path.join("consumer-stdout.log"))
            .unwrap();

        let stderr_log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_path.join("consumer-stderr.log"))
            .unwrap();

        let child = Command::new(&self.consumer_executable_path)
            .env("MINA_NODE_NAME", &self.node_identifier)
            .arg("serve")
            .arg("--trace-file")
            .arg(&self.main_trace_file_path)
            .arg("--db-uri")
            .arg(&self.db_uri)
            .arg("--port")
            .arg(format!("{}", self.graphql_port))
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .kill_on_drop(true)
            .spawn()?;

        tokio::io::Result::Ok(child)
    }
}
