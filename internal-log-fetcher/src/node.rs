// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct NodeIdentity {
    pub ip: String,
    pub graphql_port: u16,
    pub submitter_pk: Option<String>,
    //pub internal_trace_port: u16,
}

impl NodeIdentity {
    pub fn new(
        ip: String,
        graphql_port: u16,
        submitter_pk: Option<String>,
        //internal_trace_port: u16,
    ) -> Self {
        NodeIdentity {
            ip,
            graphql_port,
            submitter_pk,
            //internal_trace_port,
        }
    }

    pub fn construct_directory_name(&self) -> String {
        let trimmed_pk = self
            .submitter_pk
            .clone()
            .map(|pk| pk[pk.len() - 8..].to_string())
            .unwrap_or_default();

        format!("{}-{}-{}", trimmed_pk, self.ip, self.graphql_port)
    }
}
