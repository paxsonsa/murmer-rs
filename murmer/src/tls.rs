use std::sync::Arc;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls_pemfile;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TlsConfigError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("Private key error: {0}")]
    PrivateKeyError(String),

    #[error("Certificate generation error: {0}")]
    RcgenError(#[from] rcgen::Error),

    #[error("No private keys found in provided file")]
    NoPrivateKeyFound,

    #[error("Insecure configuration is not allowed")]
    InsecureError,
}

/// Represents server credentials, either secure (TLS) or insecure
pub enum TlsConfig {
    /// TLS with externally signed certificates (e.g., Let's Encrypt)
    Secure {
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    },
    /// TLS with self-signed certificates
    SelfSigned {
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        hostnames: Vec<String>,
    },
    /// Insecure connection without TLS
    Insecure,
}

impl TlsConfig {
    /// Load TLS credentials from paths, falling back to self-signed if None
    /// Create new secure credentials with existing certificates
    pub fn secure(key_path: PathBuf, cert_path: PathBuf) -> Result<Self, TlsConfigError> {
        let (cert_chain, private_key) = Self::load_from_paths(&key_path, &cert_path)?;
        Ok(TlsConfig::Secure {
            cert_chain,
            private_key,
        })
    }

    pub fn default_self_signed(hostnames: impl Into<Vec<String>>) -> Result<Self, TlsConfigError> {
        let hostnames = hostnames.into();
        let load_path = directories_next::ProjectDirs::from("com", "paxson", "murmur")
            .expect("Failed to get project directories")
            .config_dir()
            .to_path_buf();
        let load_path = load_path.join("certs");
        Self::self_signed(load_path, hostnames)
    }

    /// Create new self-signed credentials or use existing ones if available
    pub fn self_signed(load_path: PathBuf, hostnames: Vec<String>) -> Result<Self, TlsConfigError> {
        let hostnames = if hostnames.is_empty() {
            vec!["localhost".to_string()]
        } else {
            hostnames
        };
        let (cert_chain, private_key) = Self::load_self_signed(load_path, &hostnames)?;
        Ok(TlsConfig::SelfSigned {
            cert_chain,
            private_key,
            hostnames,
        })
    }

    /// Create new insecure credentials without TLS
    pub fn insecure() -> Self {
        TlsConfig::Insecure
    }

    /// Load certificates from provided paths or generate self-signed
    pub fn load_certificates(
        key_path: Option<PathBuf>,
        cert_path: Option<PathBuf>,
        self_signed_path: PathBuf,
        hostnames: &[String],
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
        let (cert_chain, private_key) = match (key_path, cert_path) {
            (Some(key), Some(cert)) => {
                let (cert_chain, private_key) = Self::load_from_paths(&key, &cert)?;
                (cert_chain, private_key)
            }
            _ => {
                let (cert_chain, private_key) =
                    Self::load_self_signed(self_signed_path, hostnames)?;
                (cert_chain, private_key)
            }
        };
        Ok((cert_chain, private_key))
    }

    fn load_from_paths(
        key_path: &Path,
        cert_path: &Path,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
        let key = Self::load_private_key(key_path)?;
        let cert_chain = Self::load_certificate_chain(cert_path)?;

        Ok((cert_chain, key))
    }

    fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
        let key_data = fs::read(path).map_err(|e| TlsConfigError::IoError(e))?;

        if path.extension().is_some_and(|x| x == "der") {
            Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_data)))
        } else {
            rustls_pemfile::private_key(&mut &*key_data)
                .map_err(|e| TlsConfigError::PrivateKeyError(e.to_string()))?
                .ok_or(TlsConfigError::NoPrivateKeyFound)
        }
    }

    fn load_certificate_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
        let cert_data = fs::read(path).map_err(|e| TlsConfigError::IoError(e))?;

        if path.extension().is_some_and(|x| x == "der") {
            Ok(vec![CertificateDer::from(cert_data)])
        } else {
            rustls_pemfile::certs(&mut &*cert_data)
                .collect::<Result<_, _>>()
                .map_err(|e| TlsConfigError::CertificateError(e.to_string()))
        }
    }

    fn load_self_signed(
        load_path: PathBuf,
        hostnames: &[String],
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
        let cert_path = load_path.join("cert.der");
        let key_path = load_path.join("key.der");

        Self::try_load_existing_self_signed(&cert_path, &key_path).or_else(|e| match e.kind() {
            io::ErrorKind::NotFound => {
                Self::generate_and_save_self_signed(&load_path, &cert_path, &key_path, hostnames)
            }
            _ => Err(TlsConfigError::IoError(e)),
        })
    }

    fn try_load_existing_self_signed(
        cert_path: &Path,
        key_path: &Path,
    ) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let cert = fs::read(cert_path)?;
        let key = fs::read(key_path)?;

        Ok((
            vec![CertificateDer::from(cert)],
            PrivateKeyDer::try_from(key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        ))
    }

    fn generate_and_save_self_signed(
        dir: &Path,
        cert_path: &Path,
        key_path: &Path,
        hostnames: &[String],
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
        tracing::info!(
            "generating self-signed certificate for hostnames: {:?}",
            hostnames
        );

        let cert = rcgen::generate_simple_self_signed(
            hostnames.iter().map(|s| s.clone()).collect::<Vec<_>>(),
        )?;
        let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let cert = cert.cert.into();

        fs::create_dir_all(dir).map_err(|e| TlsConfigError::IoError(e))?;
        fs::write(cert_path, &cert).map_err(|e| TlsConfigError::IoError(e))?;
        fs::write(key_path, key.secret_pkcs8_der()).map_err(|e| TlsConfigError::IoError(e))?;

        Ok((vec![cert], key.into()))
    }

    /// Create a new server configuration with the provided TLS configuration
    pub fn into_server_config(self) -> Result<rustls::ServerConfig, TlsConfigError> {
        match self {
            TlsConfig::Secure {
                cert_chain,
                private_key,
            }
            | TlsConfig::SelfSigned {
                cert_chain,
                private_key,
                ..
            } => {
                let mut config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(cert_chain, private_key)
                    .map_err(|e| TlsConfigError::CertificateError(e.to_string()))?;

                config.alpn_protocols = vec![b"murmur".to_vec()];

                Ok(config)
            }
            TlsConfig::Insecure => {
                let tls_config = Self::default_self_signed(vec!["localhost".to_string()])?;
                tls_config.into_server_config()
            }
        }
    }

    /// Create a new client configuration with the provided TLS configuration
    pub fn into_client_config(self) -> Result<rustls::ClientConfig, TlsConfigError> {
        let mut crypto = match self {
            TlsConfig::Secure { cert_chain, .. } | TlsConfig::SelfSigned { cert_chain, .. } => {
                let mut roots = rustls::RootCertStore::empty();
                cert_chain.into_iter().for_each(|cert| {
                    roots
                        .add(cert)
                        .expect("Failed to add certificate to root store");
                });
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth()
            }
            TlsConfig::Insecure => {
                // No root certificates
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipClientVerification::new())
                    .with_no_client_auth()
            }
        };
        crypto.alpn_protocols = vec![b"murmur".to_vec()];

        Ok(crypto)
    }
}

impl Clone for TlsConfig {
    fn clone(&self) -> Self {
        match self {
            TlsConfig::Secure {
                cert_chain,
                private_key,
            } => TlsConfig::Secure {
                cert_chain: cert_chain.clone(),
                private_key: private_key.clone_key(),
            },
            TlsConfig::SelfSigned {
                cert_chain,
                private_key,
                hostnames,
            } => TlsConfig::SelfSigned {
                cert_chain: cert_chain.clone(),
                private_key: private_key.clone_key(),
                hostnames: hostnames.clone(),
            },
            TlsConfig::Insecure => TlsConfig::Insecure,
        }
    }
}
/// Dummy certificate verifier that treats any certificate as valid.
#[derive(Debug)]
struct SkipClientVerification(Arc<CryptoProvider>);

impl SkipClientVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipClientVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
