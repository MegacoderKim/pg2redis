use crate::config;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsAuthMode {
    No,
    Optional,
    Yes,
}

pub fn parse_auth_mode(raw: &str) -> Result<TlsAuthMode, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "no" | "off" | "false" => Ok(TlsAuthMode::No),
        "optional" => Ok(TlsAuthMode::Optional),
        "yes" | "on" | "true" => Ok(TlsAuthMode::Yes),
        other => Err(format!(
            "invalid pg_redis.tls_auth_clients value '{other}' (expected 'no', 'optional', or 'yes')"
        )),
    }
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path)
        .map_err(|e| format!("failed to open TLS certificate file '{path}': {e}"))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse PEM certificates from '{path}': {e}"))?;
    if certs.is_empty() {
        return Err(format!("no PEM certificates found in '{path}'"));
    }
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path)
        .map_err(|e| format!("failed to open TLS private key file '{path}': {e}"))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("failed to parse PEM private key from '{path}': {e}"))?
        .ok_or_else(|| format!("no PEM private key found in '{path}'"))
}

/// Build a `tokio_rustls::TlsAcceptor` from the current `pg_redis.tls_*` GUC settings.
pub fn build_tls_acceptor_from_config() -> Result<TlsAcceptor, String> {
    let cert_path = config::tls_cert_file()
        .ok_or_else(|| "pg_redis.tls_port is enabled but pg_redis.tls_cert_file is not set".to_string())?;
    let key_path = config::tls_key_file()
        .ok_or_else(|| "pg_redis.tls_port is enabled but pg_redis.tls_key_file is not set".to_string())?;
    let auth_mode = parse_auth_mode(&config::tls_auth_clients())?;
    let ca_path = config::tls_ca_cert_file();

    build_tls_acceptor(&cert_path, &key_path, ca_path.as_deref(), auth_mode)
}

pub fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
    ca_path: Option<&str>,
    auth_mode: TlsAuthMode,
) -> Result<TlsAcceptor, String> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("failed to configure TLS protocol versions: {e}"))?;

    let server_config = match auth_mode {
        TlsAuthMode::No => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("invalid TLS certificate/key pair: {e}"))?,
        TlsAuthMode::Optional | TlsAuthMode::Yes => {
            let ca_file = ca_path.ok_or_else(|| {
                "pg_redis.tls_auth_clients requires pg_redis.tls_ca_cert_file to be set".to_string()
            })?;
            let ca_certs = load_certs(ca_file)?;
            let mut roots = RootCertStore::empty();
            let (added, _ignored) = roots.add_parsable_certificates(ca_certs);
            if added == 0 {
                return Err(format!(
                    "no valid CA certificates could be parsed from '{ca_file}'"
                ));
            }
            let mut verifier_builder =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            if auth_mode == TlsAuthMode::Optional {
                verifier_builder = verifier_builder.allow_unauthenticated();
            }
            let verifier = verifier_builder
                .build()
                .map_err(|e| format!("failed to build mTLS client certificate verifier: {e}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| format!("invalid TLS certificate/key pair: {e}"))?
        }
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}
