use std::net::SocketAddr;

/// Errors that can occur in the clustering layer.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("node key file error: {0}")]
    KeyFile(String),

    #[error("allowlist file error: {0}")]
    AllowlistFile(String),

    #[error("connection from {0} rejected: endpoint not in allowlist")]
    NotInAllowlist(String),

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("cookie mismatch from {0}")]
    CookieMismatch(SocketAddr),

    #[error("protocol version mismatch: local={local}, remote={remote} from {addr}")]
    ProtocolMismatch {
        local: u32,
        remote: u32,
        addr: SocketAddr,
    },

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("timeout: {0}")]
    Timeout(String),
}
