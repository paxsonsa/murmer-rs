use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::net::SocketAddr;

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(SocketAddr);

impl NodeId {
    /// Create a deterministic NodeId from a socket address
    /// This generates a deterministic UUID based on the socket address hash
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self(addr)
    }
    
    /// Get the inner socket address
    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node({})", self.0)
    }
}

