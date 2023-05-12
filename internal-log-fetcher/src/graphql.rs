// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;

pub(crate) type Json = serde_json::Value;
pub(crate) type UInt16 = String;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/internal_logs_query.graphql",
    response_derives = "Debug"
)]
pub struct InternalLogsQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/flush_internal_logs_mutation.graphql",
    response_derives = "Debug"
)]
pub struct FlushInternalLogsQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/auth_query.graphql",
    response_derives = "Debug"
)]
pub struct AuthQuery;
