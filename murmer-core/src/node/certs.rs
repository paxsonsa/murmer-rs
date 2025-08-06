use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io::Cursor;
use std::sync::Arc;
use crate::node::NodeId;

/// Generate a self-signed certificate for development/testing
pub fn generate_self_signed_cert(node_id: &NodeId) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let mut params = rcgen::CertificateParams::new(vec![node_id.to_string()])?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.distinguished_name.push(
        rcgen::DnType::CommonName, 
        node_id.to_string()
    );
    
    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;
    let cert_pem = cert.pem();
    let private_key_pem = key_pair.serialize_pem();
    
    // Parse the PEM into rustls types
    let cert_chain = rustls_pemfile::certs(&mut Cursor::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()?;
        
    let private_key = rustls_pemfile::private_key(&mut Cursor::new(private_key_pem.as_bytes()))?
        .ok_or("No private key found")?;
    
    Ok((cert_chain, private_key))
}

/// Create a server config that accepts any client certificate (for testing)
pub fn create_insecure_server_config(
    cert_chain: Vec<CertificateDer<'static>>, 
    private_key: PrivateKeyDer<'static>
) -> Result<quinn::ServerConfig, Box<dyn std::error::Error>> {
    let crypto_server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)?;
    
    let server_config = quinn::ServerConfig::with_crypto(
        Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(crypto_server_config)?)
    );
        
    Ok(server_config)
}