use quinn::{ClientConfig, Connection, Endpoint};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use crate::cluster::framing::{ClusterMessage, MessageFramer};
use crate::node::{NodeAddress, NodeId, certs};

#[derive(Debug, thiserror::Error)]
pub enum NodeServerError {
    #[error("Failed to bind QUIC endpoint: {0}")]
    BindFailed(#[from] std::io::Error),

    #[error("Failed to connect to remote node: {0}")]
    ConnectionFailed(#[from] quinn::ConnectError),

    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Gossip stream error: {0}")]
    GossipStreamError(String),
}

/// Configuration for the node server
#[derive(Debug)]
pub struct NodeServerConfig {
    pub bind_address: NodeAddress,
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}

impl NodeServerConfig {
    /// Create a new config with generated self-signed certificate
    pub fn new_with_generated_cert(
        bind_address: NodeAddress,
        node_id: &NodeId,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (cert_chain, private_key) = certs::generate_self_signed_cert(node_id)?;

        Ok(Self {
            bind_address,
            cert_chain,
            private_key,
        })
    }
}

/// A simple ping message for testing node connectivity
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PingMessage {
    pub from_node: NodeId,
    pub message: String,
}

/// A simple pong response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PongMessage {
    pub from_node: NodeId,
    pub response: String,
}

/// Handle for a persistent gossip stream to a remote node
struct GossipStream {
    node_id: NodeId,
    message_tx: mpsc::UnboundedSender<ClusterMessage>,
    _handle: tokio::task::JoinHandle<()>,
}

pub struct NodeServer {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    gossip_streams: Arc<RwLock<HashMap<NodeId, GossipStream>>>,
    failed_nodes: Arc<RwLock<std::collections::HashSet<NodeId>>>,
    gossip_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<(NodeId, ClusterMessage)>>>>,
    gossip_tx: mpsc::UnboundedSender<(NodeId, ClusterMessage)>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl NodeServer {
    /// Create a new node server with the given configuration
    pub async fn new(config: NodeServerConfig, node_id: NodeId) -> Result<Self, NodeServerError> {
        // Create server configuration with TLS
        let server_config = certs::create_insecure_server_config(
            config.cert_chain.clone(),
            config.private_key.clone_key(),
        )
        .map_err(|e| NodeServerError::CertificateError(e.to_string()))?;

        // Create client configuration for outbound connections
        let client_config = Self::create_insecure_client_config()?;

        // Bind the QUIC endpoint
        let mut endpoint = Endpoint::server(server_config, config.bind_address.to_socket_addr())?;

        // Configure client capabilities
        endpoint.set_default_client_config(client_config);

        let (gossip_tx, gossip_rx) = mpsc::unbounded_channel();

        Ok(Self {
            node_id,
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            gossip_streams: Arc::new(RwLock::new(HashMap::new())),
            failed_nodes: Arc::new(RwLock::new(std::collections::HashSet::new())),
            gossip_rx: Arc::new(RwLock::new(Some(gossip_rx))),
            gossip_tx,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Start accepting incoming connections and handling messages
    pub async fn start(&self) {
        let endpoint = self.endpoint.clone();
        let connections = self.connections.clone();
        let cancellation = self.cancellation_token.clone();

        let gossip_tx = self.gossip_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        tracing::info!("Node server shutting down");
                        break;
                    }

                    incoming = endpoint.accept() => {
                        if let Some(incoming) = incoming {
                            let connections = connections.clone();
                            let gossip_tx = gossip_tx.clone();
                            tokio::spawn(async move {
                                match incoming.await {
                                    Ok(connection) => {
                                        Self::handle_incoming_connection(connection, connections, gossip_tx).await;
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to accept connection: {}", e);
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });
    }

    /// Connect to a remote node
    pub async fn connect_to_node(
        &self,
        node_id: NodeId,
        address: NodeAddress,
    ) -> Result<(), NodeServerError> {
        // Prevent connecting to self
        // FIXME: Why are we doing this check?
        // if node_id == self.node_id {
        //     return Err(NodeServerError::StreamError(
        //         "Cannot connect to self".to_string(),
        //     ));
        // }

        // Check if we already have a connection
        {
            let connections = self.connections.read().await;
            if connections.contains_key(&node_id) {
                // Clear failed status if we have a connection
                let mut failed_nodes = self.failed_nodes.write().await;
                failed_nodes.remove(&node_id);
                return Ok(());
            }
        }

        tracing::info!("Connecting to node {} at {}", node_id, address);

        // Establish QUIC connection
        // Use the IP address as the server name for TLS
        let server_name = address.ip.to_string();
        let connecting = self
            .endpoint
            .connect(address.to_socket_addr(), &server_name)?;
        let connection = connecting
            .await
            .map_err(|e| NodeServerError::StreamError(format!("Connection failed: {}", e)))?;

        // Store the connection
        {
            let mut connections = self.connections.write().await;
            connections.insert(node_id.clone(), connection);
        }

        // Clear failed status on successful connection
        {
            let mut failed_nodes = self.failed_nodes.write().await;
            failed_nodes.remove(&node_id);
        }

        tracing::info!("Successfully connected to node {}", node_id);
        Ok(())
    }

    /// Send a ping message to a remote node
    pub async fn ping_node(
        &self,
        target_node: &NodeId,
        message: String,
    ) -> Result<PongMessage, NodeServerError> {
        // // Prevent pinging self
        // FIXME: Why do we need this?
        // if *target_node == self.node_id {
        //     return Err(NodeServerError::StreamError("Cannot ping self".to_string()));
        // }

        let connection = {
            let connections = self.connections.read().await;
            connections
                .get(target_node)
                .cloned()
                .ok_or_else(|| NodeServerError::NodeNotFound(target_node.clone()))?
        };

        // Open a bidirectional stream
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;

        // Create and serialize ping message
        let ping = PingMessage {
            from_node: self.node_id.clone(),
            message,
        };

        let ping_data = bincode::serde::encode_to_vec(&ping, bincode::config::standard())
            .map_err(|e| NodeServerError::StreamError(format!("Serialization failed: {}", e)))?;

        // Send ping
        send.write_all(&ping_data)
            .await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        send.finish()
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;

        // Receive pong
        let response_data = recv
            .read_to_end(1024)
            .await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;

        let (pong, _): (PongMessage, _) =
            bincode::serde::decode_from_slice(&response_data, bincode::config::standard())
                .map_err(|e| {
                    NodeServerError::StreamError(format!("Deserialization failed: {}", e))
                })?;

        Ok(pong)
    }

    /// Get a connection to a specific node
    pub async fn get_connection(&self, node_id: &NodeId) -> Option<Connection> {
        let connections = self.connections.read().await;
        connections.get(node_id).cloned()
    }

    /// Remove a connection (on failure)
    pub async fn remove_connection(&self, node_id: &NodeId) {
        let mut connections = self.connections.write().await;
        if let Some(connection) = connections.remove(node_id) {
            connection.close(0u32.into(), b"Connection removed");
            tracing::info!("Removed connection to node {}", node_id);
        }

        // Also remove gossip stream
        let mut gossip_streams = self.gossip_streams.write().await;
        if let Some(stream) = gossip_streams.remove(node_id) {
            stream._handle.abort();
            tracing::info!("Removed gossip stream to node {}", node_id);
        }

        // Clear from failed nodes when we explicitly remove connection
        let mut failed_nodes = self.failed_nodes.write().await;
        failed_nodes.remove(node_id);
    }

    /// Send a gossip message to a specific node
    pub async fn send_gossip_message(
        &self,
        node_id: &NodeId,
        message: ClusterMessage,
    ) -> Result<(), NodeServerError> {
        // Prevent sending gossip to self
        if *node_id == self.node_id {
            return Err(NodeServerError::StreamError(
                "Cannot send gossip to self".to_string(),
            ));
        }

        // Check if node is marked as failed
        {
            let failed_nodes = self.failed_nodes.read().await;
            if failed_nodes.contains(node_id) {
                return Err(NodeServerError::GossipStreamError(
                    "Node marked as failed".to_string(),
                ));
            }
        }

        // Try to get existing gossip stream
        {
            let gossip_streams = self.gossip_streams.read().await;
            if let Some(stream) = gossip_streams.get(node_id) {
                if stream.message_tx.send(message.clone()).is_ok() {
                    return Ok(());
                }
                // If send failed, mark node as failed
                drop(gossip_streams);
                let mut failed_nodes = self.failed_nodes.write().await;
                failed_nodes.insert(node_id.clone());
                return Err(NodeServerError::GossipStreamError(
                    "Failed to send message".to_string(),
                ));
            }
        }

        // No existing stream, try to create a new one
        if let Err(e) = self.create_gossip_stream(node_id.clone()).await {
            let mut failed_nodes = self.failed_nodes.write().await;
            failed_nodes.insert(node_id.clone());
            return Err(e);
        }

        // Try again with new stream
        let gossip_streams = self.gossip_streams.read().await;
        if let Some(stream) = gossip_streams.get(node_id) {
            if let Err(_) = stream.message_tx.send(message) {
                drop(gossip_streams);
                let mut failed_nodes = self.failed_nodes.write().await;
                failed_nodes.insert(node_id.clone());
                return Err(NodeServerError::GossipStreamError(
                    "Failed to send message".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Get the receiver for incoming gossip messages
    pub async fn take_gossip_receiver(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<(NodeId, ClusterMessage)>> {
        let mut rx_guard = self.gossip_rx.write().await;
        rx_guard.take()
    }

    /// Create a persistent gossip stream to a node
    async fn create_gossip_stream(&self, node_id: NodeId) -> Result<(), NodeServerError> {
        // Get connection
        let connection = {
            let connections = self.connections.read().await;
            connections
                .get(&node_id)
                .cloned()
                .ok_or_else(|| NodeServerError::NodeNotFound(node_id.clone()))?
        };

        // Open bidirectional stream for gossip
        let (send_stream, recv_stream) = connection
            .open_bi()
            .await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;

        let (message_tx, mut message_rx) = mpsc::unbounded_channel::<ClusterMessage>();
        let gossip_tx = self.gossip_tx.clone();
        let node_id_for_handle = node_id.clone();

        // Spawn task to handle outgoing messages
        let send_handle = {
            let mut send_stream = send_stream;
            let node_id_for_send = node_id.clone();
            let failed_nodes = self.failed_nodes.clone();
            tokio::spawn(async move {
                while let Some(message) = message_rx.recv().await {
                    let serialized = message.serialize();
                    if let Err(e) = send_stream.write_all(&serialized).await {
                        // Don't log errors as errors - the cluster manager handles failure detection
                        tracing::debug!("Gossip send failed to {}: {}", node_id_for_send, e);
                        // Mark node as failed so future sends will fail immediately
                        let mut failed_nodes = failed_nodes.write().await;
                        failed_nodes.insert(node_id_for_send.clone());
                        break;
                    }
                }

                let _ = send_stream.finish();
                tracing::debug!("Gossip send stream to {} closed", node_id_for_send);
            })
        };

        // Spawn task to handle incoming messages
        let recv_handle = {
            let mut recv_stream = recv_stream;
            let node_id = node_id.clone();
            tokio::spawn(async move {
                let mut framer = MessageFramer::new();
                let mut buffer = [0u8; 4096];

                loop {
                    match recv_stream.read(&mut buffer).await {
                        Ok(Some(len)) => {
                            framer.add_data(&buffer[..len]);

                            // Process all complete messages
                            while let Ok(Some(message)) = framer.next_message() {
                                if gossip_tx.send((node_id.clone(), message)).is_err() {
                                    tracing::error!(
                                        "Failed to forward gossip message from {}",
                                        node_id
                                    );
                                    return;
                                }
                            }
                        }
                        Ok(None) => {
                            // Stream closed
                            tracing::debug!("Gossip recv stream from {} closed", node_id);
                            break;
                        }
                        Err(e) => {
                            match &e {
                                quinn::ReadError::ConnectionLost(_) => {
                                    tracing::debug!("Gossip connection to {} lost", node_id);
                                }
                                _ => {
                                    tracing::debug!(
                                        "Error reading from gossip stream {}: {}",
                                        node_id,
                                        e
                                    );
                                }
                            }
                            break;
                        }
                    }
                }
            })
        };

        // Combine both handles
        let combined_handle = tokio::spawn(async move {
            tokio::select! {
                _ = send_handle => {},
                _ = recv_handle => {},
            }
        });

        // Store the gossip stream
        let gossip_stream = GossipStream {
            node_id: node_id.clone(),
            message_tx,
            _handle: combined_handle,
        };

        {
            let mut gossip_streams = self.gossip_streams.write().await;
            gossip_streams.insert(node_id_for_handle, gossip_stream);
        }

        tracing::info!("Created gossip stream to node {}", node_id);
        Ok(())
    }

    /// Get the local node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the local endpoint address
    pub fn local_addr(&self) -> Result<NodeAddress, NodeServerError> {
        let addr = self.endpoint.local_addr()?;
        Ok(NodeAddress::from_socket_addr_infallible(addr))
    }

    /// Shutdown the server
    pub async fn shutdown(&self) {
        self.cancellation_token.cancel();

        // Close all gossip streams
        {
            let mut gossip_streams = self.gossip_streams.write().await;
            for (node_id, stream) in gossip_streams.drain() {
                stream._handle.abort();
                tracing::debug!("Closed gossip stream to node {}", node_id);
            }
        }

        // Close all connections
        let mut connections = self.connections.write().await;
        for (node_id, connection) in connections.drain() {
            connection.close(0u32.into(), b"Server shutting down");
            tracing::debug!("Closed connection to node {}", node_id);
        }

        // Wait for endpoint to close
        self.endpoint.wait_idle().await;
    }

    // Private helper methods

    async fn handle_incoming_connection(
        connection: Connection,
        _connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
        gossip_tx: mpsc::UnboundedSender<(NodeId, ClusterMessage)>,
    ) {
        tracing::info!("Accepted connection from {}", connection.remote_address());

        // Handle incoming streams
        loop {
            tokio::select! {
                stream_result = connection.accept_bi() => {
                    match stream_result {
                        Ok((send, recv)) => {
                            let gossip_tx = gossip_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_incoming_stream(send, recv, gossip_tx).await {
                                    // Only log errors that aren't normal connection closures
                                    match &e {
                                        NodeServerError::StreamError(msg) if msg.contains("connection lost") => {
                                            tracing::debug!("Stream connection lost (expected during shutdown)");
                                        }
                                        NodeServerError::StreamError(msg) if msg.contains("closed immediately") => {
                                            tracing::debug!("Stream closed by peer (expected during shutdown)");
                                        }
                                        _ => {
                                            tracing::warn!("Error handling incoming stream: {}", e);
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            // Only log errors that aren't expected connection closures
                            match &e {
                                quinn::ConnectionError::ApplicationClosed { .. } => {
                                    tracing::debug!("Peer closed connection gracefully");
                                }
                                quinn::ConnectionError::ConnectionClosed(_) => {
                                    tracing::debug!("Connection lost to peer");
                                }
                                quinn::ConnectionError::TimedOut => {
                                    tracing::debug!("Connection to peer timed out");
                                }
                                _ => {
                                    tracing::warn!("Connection error: {}", e);
                                }
                            }
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                    // Connection timeout check
                    break;
                }
            }
        }
    }

    async fn handle_incoming_stream(
        send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        gossip_tx: mpsc::UnboundedSender<(NodeId, ClusterMessage)>,
    ) -> Result<(), NodeServerError> {
        let mut buffer = [0u8; 4096];
        let mut framer = MessageFramer::new();

        // Read initial data to determine stream type
        match recv.read(&mut buffer).await {
            Ok(Some(len)) => {
                // First, try to parse as a framed cluster message
                framer.add_data(&buffer[..len]);

                if let Ok(Some(cluster_msg)) = framer.next_message() {
                    // This is a gossip stream, handle it as such
                    // TODO: Extract actual node_id from connection or message
                    let dummy_node_id = NodeId::from_socket_addr("127.0.0.1:0".parse().unwrap());
                    return Self::handle_gossip_stream_messages(
                        send,
                        recv,
                        dummy_node_id,
                        gossip_tx,
                        cluster_msg,
                        framer,
                    )
                    .await;
                } else {
                    // Try to parse as a legacy ping message
                    if let Ok((ping, _)) = bincode::serde::decode_from_slice::<PingMessage, _>(
                        &buffer[..len],
                        bincode::config::standard(),
                    ) {
                        // For now, use dummy node_id - this would normally be extracted from connection
                        let dummy_node_id =
                            NodeId::from_socket_addr("127.0.0.1:0".parse().unwrap());
                        return Self::handle_ping_message(send, ping, dummy_node_id).await;
                    }

                    return Err(NodeServerError::StreamError(
                        "Unknown stream type".to_string(),
                    ));
                }
            }
            Ok(None) => {
                return Err(NodeServerError::StreamError(
                    "Stream closed immediately".to_string(),
                ));
            }
            Err(e) => {
                return Err(NodeServerError::StreamError(e.to_string()));
            }
        }
    }

    async fn handle_gossip_stream_messages(
        mut _send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        remote_node_id: NodeId,
        gossip_tx: mpsc::UnboundedSender<(NodeId, ClusterMessage)>,
        first_message: ClusterMessage,
        mut framer: MessageFramer,
    ) -> Result<(), NodeServerError> {
        if gossip_tx
            .send((remote_node_id.clone(), first_message))
            .is_err()
        {
            return Err(NodeServerError::GossipStreamError(
                "Failed to forward message".to_string(),
            ));
        }

        // Continue reading messages
        let mut buffer = [0u8; 4096];
        loop {
            match recv.read(&mut buffer).await {
                Ok(Some(len)) => {
                    framer.add_data(&buffer[..len]);

                    while let Ok(Some(message)) = framer.next_message() {
                        if gossip_tx.send((remote_node_id.clone(), message)).is_err() {
                            return Err(NodeServerError::GossipStreamError(
                                "Failed to forward message".to_string(),
                            ));
                        }
                    }
                }
                Ok(None) => {
                    tracing::debug!("Gossip stream from {} closed", remote_node_id);
                    break;
                }
                Err(e) => {
                    return Err(NodeServerError::StreamError(e.to_string()));
                }
            }
        }

        Ok(())
    }

    async fn handle_ping_message(
        mut send: quinn::SendStream,
        ping: PingMessage,
        local_node_id: NodeId,
    ) -> Result<(), NodeServerError> {
        tracing::info!("Received ping from {}: {}", ping.from_node, ping.message);

        // Create pong response
        let pong = PongMessage {
            from_node: local_node_id,
            response: format!("Pong! Received: {}", ping.message),
        };

        // Serialize and send pong
        let pong_data = bincode::serde::encode_to_vec(&pong, bincode::config::standard())
            .map_err(|e| NodeServerError::StreamError(format!("Serialization failed: {}", e)))?;

        send.write_all(&pong_data)
            .await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        send.finish()
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;

        tracing::info!("Sent pong response to {}", ping.from_node);

        Ok(())
    }

    fn create_insecure_client_config() -> Result<ClientConfig, NodeServerError> {
        // For Phase 1 testing, we'll skip certificate verification
        let crypto = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();

        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
        ));
        client_config.transport_config(Arc::new({
            let mut transport = quinn::TransportConfig::default();
            transport.max_idle_timeout(Some(
                tokio::time::Duration::from_secs(30).try_into().unwrap(),
            ));
            transport
        }));

        Ok(client_config)
    }
}

// Temporary certificate verifier for Phase 1
// WARNING: This skips certificate verification - only for development!
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
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
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
