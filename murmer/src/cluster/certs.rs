use std::io::Cursor;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::config::NodeIdentity;
use super::error::ClusterError;

/// Generate a self-signed certificate for a node (dev/testing only).
pub fn generate_self_signed_cert(
    identity: &NodeIdentity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), ClusterError> {
    let subject_alt_name = identity.to_string();
    let mut params = rcgen::CertificateParams::new(vec![subject_alt_name])
        .map_err(|e| ClusterError::Tls(e.to_string()))?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, identity.to_string());

    let key_pair = rcgen::KeyPair::generate().map_err(|e| ClusterError::Tls(e.to_string()))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| ClusterError::Tls(e.to_string()))?;
    let cert_pem = cert.pem();
    let private_key_pem = key_pair.serialize_pem();

    let cert_chain = rustls_pemfile::certs(&mut Cursor::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ClusterError::Tls(e.to_string()))?;

    let private_key = rustls_pemfile::private_key(&mut Cursor::new(private_key_pem.as_bytes()))
        .map_err(|e| ClusterError::Tls(e.to_string()))?
        .ok_or_else(|| ClusterError::Tls("no private key found".into()))?;

    Ok((cert_chain, private_key))
}

/// Create a QUIC server config that accepts any client (no client auth).
pub fn create_server_config(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig, ClusterError> {
    let crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| ClusterError::Tls(e.to_string()))?;

    let server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| ClusterError::Tls(e.to_string()))?,
    ));

    Ok(server_config)
}

/// Create a QUIC client config that skips server verification (dev only).
pub fn create_client_config() -> Result<quinn::ClientConfig, ClusterError> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    Ok(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| ClusterError::Tls(e.to_string()))?,
    )))
}

/// Accepts any server certificate without verification. Dev/testing only.
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}
