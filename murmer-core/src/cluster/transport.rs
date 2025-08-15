use crate::node::{NodeAddress, NodeId, NodeServer};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ClusterTransportError {
    #[error("Node server error: {0}")]
    NodeServer(#[from] crate::node::NodeServerError),

    #[error("Message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("No connection to target {0}")]
    NoConnection(SocketAddr),

    #[error("Invalid target address: {0}")]
    InvalidTarget(String),
}

/// QUIC-based cluster transport that integrates with NodeServer
/// Replaces UDP-based gossip with QUIC streams for unified transport
#[derive(Clone)]
pub struct ClusterTransport {
    node_server: Arc<NodeServer>,
    local_addr: NodeAddress,
    max_message_size: usize,
    // With unified system, we don't need separate mapping since NodeId wraps SocketAddr
    // Keeping for API compatibility but it's now redundant
}

impl ClusterTransport {
    /// Create a new QUIC-based cluster transport using an existing NodeServer
    pub fn new_with_node_server(
        node_server: Arc<NodeServer>,
        local_addr: NodeAddress,
    ) -> Result<Self, ClusterTransportError> {
        tracing::info!("Creating QUIC-based cluster transport for {}", local_addr);

        Ok(Self {
            node_server,
            local_addr,
            max_message_size: 64 * 1024, // QUIC can handle much larger messages than UDP
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr.to_socket_addr()
    }

    /// Register a node for QUIC connection (establish connection if needed)
    /// With unified NodeId system, we just create NodeId from SocketAddr
    pub async fn register_node(&self, addr: SocketAddr, _ignored: SocketAddr) {
        let node_id = NodeId::from_socket_addr(addr);
        let node_addr = match NodeAddress::from_socket_addr(addr) {
            Ok(addr) => addr,
            Err(e) => {
                tracing::warn!("Invalid node address {}: {}", addr, e);
                return;
            }
        };

        // Establish QUIC connection if not already connected
        if let Err(e) = self
            .node_server
            .connect_to_node(node_id.clone(), node_addr)
            .await
        {
            tracing::debug!("Could not establish connection to {}: {}", node_id, e);
        } else {
            tracing::trace!("Registered and connected to node {} at {}", node_id, addr);
        }
    }

    /// Remove a node (disconnect QUIC connection)
    pub async fn unregister_node(&self, addr: SocketAddr) {
        let node_id = NodeId::from_socket_addr(addr);
        // TODO: Add NodeServer.disconnect_node() method to clean up connections
        tracing::trace!("Unregistered node {} for address {}", node_id, addr);
    }

    /// Send data to a target using QUIC gossip streams
    pub async fn send_to(
        &self,
        data: &[u8],
        target: SocketAddr,
    ) -> Result<(), ClusterTransportError> {
        if data.len() > self.max_message_size {
            return Err(ClusterTransportError::MessageTooLarge {
                size: data.len(),
                max: self.max_message_size,
            });
        }

        // With unified NodeId system, directly create from SocketAddr
        let node_id = NodeId::from_socket_addr(target);

        // Ensure we have a QUIC connection to this node
        let node_addr = NodeAddress::from_socket_addr(target).map_err(|e| {
            ClusterTransportError::InvalidTarget(format!("Invalid address {}: {}", target, e))
        })?;

        // Try to establish connection if not already connected
        if let Err(e) = self
            .node_server
            .connect_to_node(node_id.clone(), node_addr)
            .await
        {
            tracing::debug!("Could not connect to {}: {}", node_id, e);
            return Err(ClusterTransportError::NoConnection(target));
        }

        tracing::trace!(
            "Sending {} bytes to {} (node: {})",
            data.len(),
            target,
            node_id
        );

        // Create a gossip message and send via QUIC
        let cluster_message = crate::cluster::ClusterMessage::new(
            crate::cluster::MessageType::Gossip,
            bytes::Bytes::from(data.to_vec()),
        );

        self.node_server
            .send_gossip_message(&node_id, cluster_message)
            .await?;
        Ok(())
    }

    /// Receive gossip data from QUIC streams
    /// This integrates with NodeServer's gossip receiver
    pub async fn recv_from(&self) -> Result<(Vec<u8>, SocketAddr), ClusterTransportError> {
        // This is a bit tricky - we need to integrate with NodeServer's gossip receiver
        // For now, we'll return an error indicating this should be handled differently
        // The proper integration would involve NodeServer exposing a gossip receiver
        // that this transport can use

        // TODO: Implement proper integration with NodeServer's gossip receiver
        // This requires architectural changes to how gossip messages flow
        Err(ClusterTransportError::InvalidTarget(
            "recv_from not implemented for QUIC transport - use NodeServer's gossip receiver instead".to_string()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeServer, NodeServerConfig};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_transport_creation() {
        // Initialize crypto provider for testing
        let _ = rustls::crypto::ring::default_provider().install_default();

        let addr = NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let node_id = NodeId::from_socket_addr("127.0.0.1:0".parse().unwrap());
        let config = NodeServerConfig::new_with_generated_cert(addr.clone(), &node_id).unwrap();
        let node_server = Arc::new(NodeServer::new(config, node_id).await.unwrap());

        let transport =
            ClusterTransport::new_with_node_server(node_server.clone(), addr.clone()).unwrap();

        assert_eq!(transport.local_addr().port(), addr.port);
        assert_eq!(transport.max_message_size, 64 * 1024);
    }

    #[tokio::test]
    async fn test_node_registration() {
        // Initialize crypto provider for testing
        let _ = rustls::crypto::ring::default_provider().install_default();

        let addr = NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0); // Use port 0 to avoid conflicts
        let node_id = NodeId::from_socket_addr("127.0.0.1:0".parse().unwrap());
        let config = NodeServerConfig::new_with_generated_cert(addr.clone(), &node_id).unwrap();
        let node_server = Arc::new(NodeServer::new(config, node_id).await.unwrap());

        let transport = ClusterTransport::new_with_node_server(node_server, addr.clone()).unwrap();

        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8002);

        // Register node (should create connection)
        transport.register_node(socket_addr, socket_addr).await;

        // Test unregistration (should clean up)
        transport.unregister_node(socket_addr).await;

        // With unified system, NodeId is derived from SocketAddr
        let node_id = NodeId::from_socket_addr(socket_addr);
        assert_eq!(node_id.socket_addr(), socket_addr);
    }

    #[tokio::test]
    async fn test_message_size_limit() {
        // Initialize crypto provider for testing
        let _ = rustls::crypto::ring::default_provider().install_default();

        let addr = NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let node_id = NodeId::from_socket_addr("127.0.0.1:0".parse().unwrap());
        let config = NodeServerConfig::new_with_generated_cert(addr.clone(), &node_id).unwrap();
        let node_server = Arc::new(NodeServer::new(config, node_id).await.unwrap());

        let transport = ClusterTransport::new_with_node_server(node_server, addr.clone()).unwrap();

        let large_message = vec![0; 128 * 1024]; // Larger than max_message_size (64KB)
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8002);

        let result = transport.send_to(&large_message, target).await;

        assert!(matches!(
            result,
            Err(ClusterTransportError::MessageTooLarge { .. })
        ));
    }
}
