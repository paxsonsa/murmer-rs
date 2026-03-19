use std::net::SocketAddr;

/// Errors that can occur in the clustering layer.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("QUIC connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("cookie mismatch from {0}")]
    CookieMismatch(SocketAddr),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("cluster already started")]
    AlreadyStarted,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),
}
