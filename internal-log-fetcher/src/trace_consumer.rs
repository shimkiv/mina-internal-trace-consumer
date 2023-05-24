// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use tokio::process::Command;

pub struct TraceConsumer {
    main_trace_file_path: PathBuf,
    graphql_port: u16,
    // TODO: process handle here?
}

impl TraceConsumer {
    pub fn new(main_trace_file_path: PathBuf, graphql_port: u16) -> Self {
        Self {
            main_trace_file_path,
            graphql_port,
        }
    }

    pub async fn run(&mut self) {
        let port_str = format!("{}", self.graphql_port);
        let path_str: String = self.main_trace_file_path.display().to_string();
        let mut cmd = Command::new("../_build/default/src/internal_trace_consumer.exe");

        cmd.args([
            "serve",
            "--trace-file",
            &path_str,
            "--port",
            port_str.as_str(),
        ]);

        // TODO: do something with stdout, probably pipeout to some file
        //cmd.stdout(Stdio::piped());
        //cmd.stdin(Stdio::piped());

        // TODO: provide a way to shut down the process if required
        let mut child = cmd.spawn().expect("failed to spawn command");

        if let Err(status) = child.wait().await {
            println!("consumer subprocess exited with non-zero status: {status}");
        }
    }
}
