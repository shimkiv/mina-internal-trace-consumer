// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use crate::mina_server::MinaServer;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine};
use ed25519_dalek::Signer;

pub(crate) trait Authenticator {
    fn signature_header(server: &MinaServer, body_bytes: &[u8]) -> Result<String>;
}

pub(crate) struct BasicAuthenticator {}

pub(crate) struct SequentialAuthenticator {}

pub(crate) fn sign_data(keypair: &ed25519_dalek::Keypair, data: &[u8]) -> Vec<u8> {
    let signature = keypair.sign(data);
    let signature_bytes = signature.to_bytes();
    signature_bytes.into()
}

impl Authenticator for BasicAuthenticator {
    fn signature_header(server: &MinaServer, body_bytes: &[u8]) -> Result<String> {
        let signature_bytes = sign_data(&server.keypair, body_bytes);
        let signature_base64 = general_purpose::STANDARD.encode(signature_bytes);
        let pk_base64 = &server.pk_base64;
        Ok(format!("Signature {pk_base64} {signature_base64}"))
    }
}

impl Authenticator for SequentialAuthenticator {
    fn signature_header(server: &MinaServer, body_bytes: &[u8]) -> Result<String> {
        let authorization_info = server
            .authorization_info
            .as_ref()
            .ok_or_else(|| anyhow!("Authorization info is missing"))?;
        let server_uuid = &authorization_info.server_uuid;
        let signer_sequence_number = authorization_info.signer_sequence_number;
        let mut data: Vec<u8> = Vec::with_capacity(
            std::mem::size_of_val(&signer_sequence_number) + server_uuid.len() + body_bytes.len(),
        );

        data.extend(signer_sequence_number.to_be_bytes());
        data.extend(server_uuid.as_bytes());
        data.extend(body_bytes);

        let signature_bytes = sign_data(&server.keypair, &data);
        let signature_base64 = general_purpose::STANDARD.encode(signature_bytes);
        let pk_base64 = &server.pk_base64;

        Ok(format!("Signature {pk_base64} {signature_base64} ; Sequencing {server_uuid} {signer_sequence_number}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mina_server::{AuthorizationInfo, MinaServer};

    const TEST_SECRET_KEY_BASE64: &str = "SzhpFfwa6RMFQTLLTyoriUJrUXYY9kyYxbNu5DsCm1k=";
    const TEST_PUBLIC_KEY_BASE64: &str = "BaG9HYTNxkqyX7haFnz/HXL9FhYwS4gdQ7uYWiStgoY=";
    const TEST_SERVER_UUID: &str = "24511be6-320d-4ef2-8dff-89916bcb6751";

    // Helper function to create a MinaServer instance for testing
    fn create_test_server() -> MinaServer {
        let sk_bytes = general_purpose::STANDARD
            .decode(TEST_SECRET_KEY_BASE64)
            .expect("Failed to decode base64 secret key");
        let secret_key = ed25519_dalek::SecretKey::from_bytes(&sk_bytes)
            .expect("Failed to interpret secret key bytes");
        // should match `TEST_PUBLIC_KEY_BASE64`
        let public_key: ed25519_dalek::PublicKey = (&secret_key).into();
        let keypair = ed25519_dalek::Keypair {
            secret: secret_key,
            public: public_key,
        };
        MinaServer {
            pk_base64: TEST_PUBLIC_KEY_BASE64.to_string(),
            keypair,
            authorization_info: Some(AuthorizationInfo {
                server_uuid: TEST_SERVER_UUID.to_string(),
                signer_sequence_number: 1,
            }),
            graphql_uri: "http://localhost".to_string(),
            last_log_id: 1,
            output_dir_path: "/tmp".into(),
            main_trace_file: None,
            verifier_trace_file: None,
            prover_trace_file: None,
        }
    }

    #[test]
    fn test_sign_data() {
        let server = create_test_server();
        let data = b"example data";
        let signature_bytes = sign_data(&server.keypair, data);
        let signature_base64 = general_purpose::STANDARD.encode(signature_bytes);

        // This signature has been precomputed for the given data and server instance.
        let expected_signature_base64 = "FHec0MaPCe+qgApNyfTDVqldb/3D2Ri2iAJ+rjLtWTgJN9jDRgUD3z/bXE0HnBiMIjVXT+05cAgijA+GnrwcBA==";
        assert_eq!(expected_signature_base64, signature_base64);
    }

    #[test]
    fn test_basic_authenticator() {
        let server = create_test_server();
        let body_bytes = b"example body";
        let signature_header = BasicAuthenticator::signature_header(&server, body_bytes).unwrap();

        // This signature has been precomputed for the given data and server instance.
        let expected_signature_header = "Signature BaG9HYTNxkqyX7haFnz/HXL9FhYwS4gdQ7uYWiStgoY= 7a6Wx4HQ5h0JJKjJWh3zXxLAa7qql+UXAuyqbQh+XW5C/IcmRchtp/SjBPvhOQmtlYNJGjvCrbBWO0SqlrVcDQ==";
        assert_eq!(expected_signature_header, signature_header);
    }

    #[test]
    fn test_sequential_authenticator() {
        let server = create_test_server();
        let body_bytes = b"example body";
        let signature_header =
            SequentialAuthenticator::signature_header(&server, body_bytes).unwrap();

        // This signature has been precomputed for the given data and server instance.
        let expected_signature_header = "Signature BaG9HYTNxkqyX7haFnz/HXL9FhYwS4gdQ7uYWiStgoY= LSy0n5XkS2wuTWuVQbFFs+H7gmR+BIE/bqlwFGVcFv0awYMXl48vuAfvYEqqnpAVSNXt8YH6n0YNew4i0pYUBg== ; Sequencing 24511be6-320d-4ef2-8dff-89916bcb6751 1";
        assert_eq!(expected_signature_header, signature_header);
    }
}
