// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use crate::{
    authentication::{Authenticator, BasicAuthenticator, SequentialAuthenticator},
    graphql, utils,
};
use base64::{engine::general_purpose, Engine};
use graphql_client::GraphQLQuery;
use std::{error::Error, fs::File, io::Write, path::PathBuf};

#[derive(Default, Clone)]
pub struct AuthorizationInfo {
    pub(crate) server_uuid: String,
    pub(crate) signer_sequence_number: u16,
}

pub(crate) struct MinaServerConfig {
    pub(crate) address: String,
    pub(crate) graphql_port: u16,
    pub(crate) use_https: bool,
    pub(crate) secret_key_base64: String,
    pub(crate) output_dir_path: PathBuf,
}

pub(crate) struct MinaServer {
    pub(crate) graphql_uri: String,
    pub(crate) keypair: ed25519_dalek::Keypair,
    pub(crate) pk_base64: String,
    pub(crate) last_log_id: i64,
    pub(crate) authorization_info: Option<AuthorizationInfo>,
    pub(crate) output_dir_path: PathBuf,
    pub(crate) main_trace_file: Option<File>,
    pub(crate) verifier_trace_file: Option<File>,
    pub(crate) prover_trace_file: Option<File>,
}

impl MinaServer {
    pub fn new(config: MinaServerConfig) -> Self {
        let sk_bytes = general_purpose::STANDARD
            .decode(config.secret_key_base64.trim_end())
            .expect("Failed to decode base64 secret key");
        let secret_key = ed25519_dalek::SecretKey::from_bytes(&sk_bytes)
            .expect("Failed to interpret secret key bytes");
        let public_key: ed25519_dalek::PublicKey = (&secret_key).into();
        let keypair = ed25519_dalek::Keypair {
            secret: secret_key,
            public: public_key,
        };
        let pk_base64 = general_purpose::STANDARD.encode(keypair.public.as_bytes());
        let schema = if config.use_https { "https" } else { "http" };
        let graphql_uri = format!(
            "{}://{}:{}/graphql",
            schema, config.address, config.graphql_port
        );

        std::fs::create_dir_all(&config.output_dir_path).expect("Could not create output dir");

        Self {
            graphql_uri,
            keypair,
            pk_base64,
            last_log_id: 0,
            authorization_info: None,
            output_dir_path: config.output_dir_path,
            main_trace_file: None,
            verifier_trace_file: None,
            prover_trace_file: None,
        }
    }

    pub fn authorize(&mut self) {
        println!("Performing authorization");
        let auth = self.perform_auth_query().unwrap();
        self.authorization_info = Some(AuthorizationInfo {
            server_uuid: auth.server_uuid,
            signer_sequence_number: auth.signer_sequence_number.parse().unwrap(),
        });
    }

    pub fn fetch_more_logs(&mut self) -> bool {
        let prev_last_log_id = self.last_log_id;
        self.last_log_id = self.perform_fetch_internal_logs_query().unwrap();
        if let Some(auth_info) = &mut self.authorization_info {
            auth_info.signer_sequence_number += 1;
        }

        prev_last_log_id < self.last_log_id
    }

    pub fn flush_logs(&mut self) {
        self.perform_flush_internal_logs_query().unwrap();
        if let Some(auth_info) = &mut self.authorization_info {
            auth_info.signer_sequence_number += 1;
        }
    }

    pub(crate) fn post_graphql_blocking<Q: GraphQLQuery, A: Authenticator>(
        &self,
        client: &reqwest::blocking::Client,
        variables: Q::Variables,
    ) -> Result<graphql_client::Response<Q::ResponseData>, reqwest::Error> {
        let body = Q::build_query(variables);
        let body_bytes = serde_json::to_vec(&body).unwrap();
        let signature_header = A::signature_header(self, &body_bytes);
        let response = client
            .post(&self.graphql_uri)
            .json(&body)
            .header(reqwest::header::AUTHORIZATION, signature_header)
            .send()?;

        response.json()
    }

    pub fn perform_auth_query(&self) -> Result<graphql::auth_query::AuthQueryAuth, Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();
        let variables = graphql::auth_query::Variables {};
        let response = self
            .post_graphql_blocking::<graphql::AuthQuery, BasicAuthenticator>(&client, variables)?;
        println!("Auth response: {:?}", response.data);
        let auth = response.data.unwrap().auth;
        Ok(auth)
    }

    pub fn perform_fetch_internal_logs_query(&mut self) -> Result<i64, Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();
        let variables = graphql::internal_logs_query::Variables {
            log_id: self.last_log_id,
        };
        let response = self
            .post_graphql_blocking::<graphql::InternalLogsQuery, SequentialAuthenticator>(
                &client, variables,
            )?;
        let response_data = response.data.unwrap();

        let mut last_log_id = self.last_log_id;

        if let Some(last) = response_data.internal_logs.last() {
            last_log_id = last.id;
        }

        self.save_log_entries(response_data.internal_logs);

        Ok(last_log_id)
    }

    pub(crate) fn save_log_entries(
        &mut self,
        internal_logs: Vec<graphql::internal_logs_query::InternalLogsQueryInternalLogs>,
    ) {
        for item in internal_logs {
            if let Some(log_file_handle) = self.file_for_process(&item.process) {
                let log = log_entry::LogEntry::try_from(item).unwrap();
                let log_json =
                    serde_json::to_string(&log).expect("Failed to serialize LogEntry as JSON");
                println!("{log_json}");
                log_file_handle.write_all(log_json.as_bytes()).unwrap();
                log_file_handle.write_all(b"\n").unwrap();
            }
        }
    }

    pub fn perform_flush_internal_logs_query(&self) -> Result<(), Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();
        let variables = graphql::flush_internal_logs_query::Variables {
            log_id: self.last_log_id,
        };
        let response = self
            .post_graphql_blocking::<graphql::FlushInternalLogsQuery, SequentialAuthenticator>(
                &client, variables,
            )?;
        // TODO: anything to do with the response?
        let _response_data = response.data.unwrap();
        Ok(())
    }

    pub fn authorize_and_run_fetch_loop(&mut self) {
        self.authorize();
        loop {
            if self.fetch_more_logs() {
                self.flush_logs();
            }
            // TODO: handle the case where fetch_more_logs fails because of an authentication
            // that was previously successful, this means that the server crashed
            // and a new trace is being producer
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    }

    pub(crate) fn file_for_process(&mut self, process: &Option<String>) -> Option<&mut File> {
        let file = match process.as_deref() {
            None => utils::maybe_open(
                &mut self.main_trace_file,
                self.output_dir_path.join("internal-trace.jsonl"),
            ),
            Some("prover") => utils::maybe_open(
                &mut self.prover_trace_file,
                self.output_dir_path.join("prover-internal-trace.jsonl"),
            ),
            Some("verifier") => utils::maybe_open(
                &mut self.verifier_trace_file,
                self.output_dir_path.join("verifier-internal-trace.jsonl"),
            ),
            Some(process) => {
                eprintln!("[WARN] got unexpected process {process}");
                return None;
            }
        };

        Some(file)
    }
}

mod log_entry {
    use crate::{
        graphql::internal_logs_query::InternalLogsQueryInternalLogs,
        utils::convert_timestamp_to_float,
    };
    use serde::ser::{Serialize, SerializeSeq, Serializer};
    use serde_json::Map;
    use std::error::Error;

    pub(crate) enum LogEntry {
        Control { metadata: serde_json::Value },
        Checkpoint { name: String, timestamp: f64 },
    }

    impl Serialize for LogEntry {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self {
                LogEntry::Control { metadata } => metadata.serialize(serializer),
                LogEntry::Checkpoint { name, timestamp } => {
                    let mut checkpoint_seq = serializer.serialize_seq(Some(2))?;
                    checkpoint_seq.serialize_element(name)?;
                    checkpoint_seq.serialize_element(timestamp)?;
                    checkpoint_seq.end()
                }
            }
        }
    }

    impl TryFrom<InternalLogsQueryInternalLogs> for LogEntry {
        type Error = Box<dyn Error>;

        fn try_from(value: InternalLogsQueryInternalLogs) -> Result<Self, Self::Error> {
            let name = value.message;

            if name == "@control" {
                let metadata_vec = value.metadata;
                let metadata_map = Map::<String, serde_json::Value>::from_iter(
                    metadata_vec.into_iter().map(|item| (item.item, item.value)),
                );
                let metadata = serde_json::Value::Object(metadata_map);

                Ok(LogEntry::Control { metadata })
            } else {
                let timestamp = convert_timestamp_to_float(&value.timestamp)?;
                Ok(LogEntry::Checkpoint { name, timestamp })
            }
        }
    }
}
