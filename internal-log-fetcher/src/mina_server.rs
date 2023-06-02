// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use crate::{
    authentication::{Authenticator, BasicAuthenticator, SequentialAuthenticator},
    graphql,
    log_entry::LogEntry,
    utils,
};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine};
use graphql_client::GraphQLQuery;
use std::{fs::File, io::Write, path::PathBuf};
use tracing::{error, info, instrument};

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
            // TODO: this should probably be opened as soon as this instance is created and not when log entries are obtained
            // The reason is that the trace consumer expects all the files to be there, and will produce noisy warnings when
            // one is missing. Currently for some reason the graphql endpoint doesn't send some of the prover logs that
            // are present in the tracing files of non-producer nodes, so that causes the prover trace file to be missing here.
            main_trace_file: None,
            verifier_trace_file: None,
            prover_trace_file: None,
        }
    }

    pub async fn authorize(&mut self) -> Result<()> {
        let auth = self.perform_auth_query().await?;
        self.authorization_info = Some(AuthorizationInfo {
            server_uuid: auth.server_uuid,
            signer_sequence_number: auth.signer_sequence_number.parse()?,
        });
        Ok(())
    }

    pub async fn fetch_more_logs(&mut self) -> Result<bool> {
        let prev_last_log_id = self.last_log_id;
        self.last_log_id = self.perform_fetch_internal_logs_query().await?;
        if let Some(auth_info) = &mut self.authorization_info {
            auth_info.signer_sequence_number += 1;
        }

        Ok(prev_last_log_id < self.last_log_id)
    }

    pub async fn flush_logs(&mut self) -> Result<()> {
        self.perform_flush_internal_logs_query().await?;
        if let Some(auth_info) = &mut self.authorization_info {
            auth_info.signer_sequence_number += 1;
        }

        Ok(())
    }

    pub(crate) async fn post_graphql<Q: GraphQLQuery, A: Authenticator>(
        &self,
        client: &reqwest::Client,
        variables: Q::Variables,
    ) -> Result<graphql_client::Response<Q::ResponseData>> {
        let body = Q::build_query(variables);
        let body_bytes = serde_json::to_vec(&body)?;
        let signature_header = A::signature_header(self, &body_bytes)?;
        let response = client
            .post(&self.graphql_uri)
            .json(&body)
            .header(reqwest::header::AUTHORIZATION, signature_header)
            .send()
            .await?;

        Ok(response.json().await?)
    }

    pub async fn perform_auth_query(&self) -> Result<graphql::auth_query::AuthQueryAuth> {
        let client = reqwest::Client::new();
        let variables = graphql::auth_query::Variables {};
        let response = self
            .post_graphql::<graphql::AuthQuery, BasicAuthenticator>(&client, variables)
            .await?;
        let auth = response
            .data
            .ok_or_else(|| anyhow!("Response data is missing"))?
            .auth;
        Ok(auth)
    }

    pub async fn perform_fetch_internal_logs_query(&mut self) -> Result<i64> {
        let client = reqwest::Client::new();
        let variables = graphql::internal_logs_query::Variables {
            log_id: self.last_log_id,
        };
        let response = self
            .post_graphql::<graphql::InternalLogsQuery, SequentialAuthenticator>(&client, variables)
            .await?;
        let response_data = response
            .data
            .ok_or_else(|| anyhow!("Response data is missing"))?;

        let mut last_log_id = self.last_log_id;

        if let Some(last) = response_data.internal_logs.last() {
            last_log_id = last.id;
        }

        self.save_log_entries(response_data.internal_logs)?;

        Ok(last_log_id)
    }

    pub(crate) fn save_log_entries(
        &mut self,
        internal_logs: Vec<graphql::internal_logs_query::InternalLogsQueryInternalLogs>,
    ) -> Result<()> {
        for item in internal_logs {
            if let Some(log_file_handle) = self.file_for_process(&item.process)? {
                let log = LogEntry::try_from(item).unwrap();
                let log_json =
                    serde_json::to_string(&log).expect("Failed to serialize LogEntry as JSON");
                // TODO: loging
                // println!("Log entries saved");
                // println!("{log_json}");
                log_file_handle.write_all(log_json.as_bytes()).unwrap();
                log_file_handle.write_all(b"\n").unwrap();
            }
        }

        Ok(())
    }

    pub async fn perform_flush_internal_logs_query(&self) -> Result<()> {
        // TODO: make configurable, default to false
        if false {
            let client = reqwest::Client::new();
            let variables = graphql::flush_internal_logs_query::Variables {
                log_id: self.last_log_id,
            };
            let response = self
                .post_graphql::<graphql::FlushInternalLogsQuery, SequentialAuthenticator>(
                    &client, variables,
                )
                .await?;
            // TODO: anything to do with the response?
            let _response_data = response.data.unwrap();
        }
        Ok(())
    }

    #[instrument(
        skip(self),
        fields(
            node = %self.graphql_uri
        ),
    )]
    pub async fn authorize_and_run_fetch_loop(&mut self) -> Result<()> {
        match self.authorize().await {
            Ok(()) => info!("Authorization Successful"),
            Err(e) => {
                error!("Authorization failed for node: {}", e);
                Err(e)?
            }
        }

        let mut remaining_retries = 5;

        loop {
            match self.fetch_more_logs().await {
                Ok(true) => {
                    // TODO: make this configurable? we don't want to do it by default
                    // because we may have many replicas of the discovery+fetcher service running
                    if false {
                        self.flush_logs().await?;
                    }
                    remaining_retries = 5
                }
                Ok(false) => remaining_retries = 5,
                Err(error) => {
                    error!("Error when fetching logs {error}");
                    remaining_retries -= 1;

                    if remaining_retries <= 0 {
                        error!("Finishing fetcher loop");
                        return Err(error);
                    }
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    }

    pub(crate) fn file_for_process(
        &mut self,
        process: &Option<String>,
    ) -> Result<Option<&mut File>> {
        let file = match process.as_deref() {
            None => utils::maybe_open(
                &mut self.main_trace_file,
                self.output_dir_path.join("internal-trace.jsonl"),
            )?,
            Some("prover") => utils::maybe_open(
                &mut self.prover_trace_file,
                self.output_dir_path.join("prover-internal-trace.jsonl"),
            )?,
            Some("verifier") => utils::maybe_open(
                &mut self.verifier_trace_file,
                self.output_dir_path.join("verifier-internal-trace.jsonl"),
            )?,
            Some(process) => {
                eprintln!("[WARN] got unexpected process {process}");
                return Ok(None);
            }
        };

        Ok(Some(file))
    }
}
