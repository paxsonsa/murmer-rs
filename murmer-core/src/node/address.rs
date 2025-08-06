use std::net::{IpAddr, SocketAddr};
use serde::{Serialize, Deserialize};

/// Network address information for a node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAddress {
    pub ip: IpAddr,
    pub port: u16,
}

impl NodeAddress {
    pub fn new(ip: IpAddr, port: u16) -> Self {
        Self { ip, port }
    }
    
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self {
            ip: addr.ip(),
            port: addr.port(),
        }
    }
    
    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
    
    /// Parse from string like "192.168.1.100:8080"
    pub fn parse(s: &str) -> Result<Self, std::net::AddrParseError> {
        let socket_addr: SocketAddr = s.parse()?;
        Ok(Self::from_socket_addr(socket_addr))
    }
}

impl std::fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}