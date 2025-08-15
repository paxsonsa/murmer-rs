use foca::Identity;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identity of a node in the cluster for SWIM protocol
/// Uses socket address as the primary identity for simplicity
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub socket_addr: std::net::SocketAddr,
    pub incarnation: u64,
}

impl NodeIdentity {
    /// Create a new NodeIdentity from a socket address
    pub fn new(socket_addr: std::net::SocketAddr) -> Self {
        Self {
            socket_addr,
            incarnation: 0,
        }
    }

    /// Create a NodeIdentity from IP and port
    pub fn from_ip_port(ip: std::net::IpAddr, port: u16) -> Self {
        Self {
            socket_addr: std::net::SocketAddr::new(ip, port),
            incarnation: 0,
        }
    }

    /// Get the socket address
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        self.socket_addr
    }

    /// Get the IP address
    pub fn ip(&self) -> std::net::IpAddr {
        self.socket_addr.ip()
    }

    /// Get the port
    pub fn port(&self) -> u16 {
        self.socket_addr.port()
    }
}

impl fmt::Display for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (i={})", self.socket_addr, self.incarnation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_node_identity_creation() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);
        let identity = NodeIdentity::new(addr);

        // Test that identity implements required traits
        assert_eq!(identity.socket_addr(), addr);
        assert_eq!(identity.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(identity.port(), 8001);
    }

    #[test]
    fn test_from_ip_port() {
        let identity =
            NodeIdentity::from_ip_port(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 9001);

        assert_eq!(identity.ip(), IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
        assert_eq!(identity.port(), 9001);
        assert_eq!(identity.socket_addr().port(), 9001);
    }

    #[test]
    fn test_display() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let identity = NodeIdentity::new(addr);

        assert_eq!(format!("{}", identity), "127.0.0.1:8001 (i=0)");
    }
}
/// Extension of NodeIdentity to implement Foca's Identity trait
impl Identity for NodeIdentity {
    type Addr = std::net::SocketAddr;

    fn renew(&self) -> Option<Self> {
        Some(Self {
            socket_addr: self.socket_addr,
            incarnation: self.incarnation + 1, // Increment incarnation for renewal
        })
    }

    fn addr(&self) -> Self::Addr {
        self.socket_addr
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        // Newer incarnation wins
        self.incarnation > adversary.incarnation
    }
}
