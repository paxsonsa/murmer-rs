use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use quinn::{Endpoint, Connection, ClientConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::node::{NodeId, NodeAddress, certs};

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
}

/// Configuration for the node server
#[derive(Debug)]
pub struct NodeServerConfig {
    pub bind_address: NodeAddress,
    pub node_id: NodeId,
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}

impl NodeServerConfig {
    /// Create a new config with generated self-signed certificate
    pub fn new_with_generated_cert(bind_address: NodeAddress) -> Result<Self, Box<dyn std::error::Error>> {
        let node_id = NodeId::new();
        let (cert_chain, private_key) = certs::generate_self_signed_cert(&node_id)?;
        
        Ok(Self {
            bind_address,
            node_id,
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

pub struct NodeServer {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl NodeServer {
    /// Create a new node server with the given configuration
    pub async fn new(config: NodeServerConfig) -> Result<Self, NodeServerError> {
        // Create server configuration with TLS
        let server_config = certs::create_insecure_server_config(
            config.cert_chain.clone(),
            config.private_key.clone_key()
        ).map_err(|e| NodeServerError::CertificateError(e.to_string()))?;
        
        // Create client configuration for outbound connections
        let client_config = Self::create_insecure_client_config()?;
        
        // Bind the QUIC endpoint
        let mut endpoint = Endpoint::server(
            server_config,
            config.bind_address.to_socket_addr()
        )?;
        
        // Configure client capabilities
        endpoint.set_default_client_config(client_config);
        
        Ok(Self {
            node_id: config.node_id,
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        })
    }
    
    /// Start accepting incoming connections and handling ping messages
    pub async fn start(&self) {
        let endpoint = self.endpoint.clone();
        let connections = self.connections.clone();
        let cancellation = self.cancellation_token.clone();
        let node_id = self.node_id.clone();
        
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
                            let node_id = node_id.clone();
                            tokio::spawn(async move {
                                match incoming.await {
                                    Ok(connection) => {
                                        Self::handle_incoming_connection(connection, connections, node_id).await;
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
        address: NodeAddress
    ) -> Result<(), NodeServerError> {
        // Check if we already have a connection
        {
            let connections = self.connections.read().await;
            if connections.contains_key(&node_id) {
                return Ok(());
            }
        }
        
        tracing::info!("Connecting to node {} at {}", node_id, address);
        
        // Establish QUIC connection
        let connecting = self.endpoint
            .connect(address.to_socket_addr(), &node_id.to_string())?;
        let connection = connecting.await
            .map_err(|e| NodeServerError::StreamError(format!("Connection failed: {}", e)))?;
        
        // Store the connection
        {
            let mut connections = self.connections.write().await;
            connections.insert(node_id.clone(), connection);
        }
        
        tracing::info!("Successfully connected to node {}", node_id);
        Ok(())
    }
    
    /// Send a ping message to a remote node
    pub async fn ping_node(&self, target_node: &NodeId, message: String) -> Result<PongMessage, NodeServerError> {
        let connection = {
            let connections = self.connections.read().await;
            connections.get(target_node).cloned()
                .ok_or_else(|| NodeServerError::NodeNotFound(target_node.clone()))?
        };
        
        // Open a bidirectional stream
        let (mut send, mut recv) = connection.open_bi().await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        
        // Create and serialize ping message
        let ping = PingMessage {
            from_node: self.node_id.clone(),
            message,
        };
        
        let ping_data = bincode::serialize(&ping)
            .map_err(|e| NodeServerError::StreamError(format!("Serialization failed: {}", e)))?;
        
        // Send ping
        send.write_all(&ping_data).await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        send.finish()
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        
        // Receive pong
        let response_data = recv.read_to_end(1024).await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        
        let pong: PongMessage = bincode::deserialize(&response_data)
            .map_err(|e| NodeServerError::StreamError(format!("Deserialization failed: {}", e)))?;
        
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
    }
    
    /// Get the local node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    
    /// Get the local endpoint address
    pub fn local_addr(&self) -> Result<NodeAddress, NodeServerError> {
        let addr = self.endpoint.local_addr()?;
        Ok(NodeAddress::from_socket_addr(addr))
    }
    
    /// Shutdown the server
    pub async fn shutdown(&self) {
        self.cancellation_token.cancel();
        
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
        local_node_id: NodeId,
    ) {
        tracing::info!(
            "Accepted connection from {}", 
            connection.remote_address()
        );
        
        // Handle incoming streams
        loop {
            tokio::select! {
                stream_result = connection.accept_bi() => {
                    match stream_result {
                        Ok((send, recv)) => {
                            let local_node_id = local_node_id.clone();
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_ping_stream(send, recv, local_node_id).await {
                                    tracing::error!("Error handling ping stream: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept stream: {}", e);
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
    
    async fn handle_ping_stream(
        mut send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        local_node_id: NodeId,
    ) -> Result<(), NodeServerError> {
        // Read the ping message
        let ping_data = recv.read_to_end(1024).await
            .map_err(|e| NodeServerError::StreamError(e.to_string()))?;
        
        let ping: PingMessage = bincode::deserialize(&ping_data)
            .map_err(|e| NodeServerError::StreamError(format!("Deserialization failed: {}", e)))?;
        
        tracing::info!("Received ping from {}: {}", ping.from_node, ping.message);
        
        // Create pong response
        let pong = PongMessage {
            from_node: local_node_id,
            response: format!("Pong! Received: {}", ping.message),
        };
        
        // Serialize and send pong
        let pong_data = bincode::serialize(&pong)
            .map_err(|e| NodeServerError::StreamError(format!("Serialization failed: {}", e)))?;
        
        send.write_all(&pong_data).await
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
            
        let mut client_config = ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap()));
        client_config.transport_config(Arc::new({
            let mut transport = quinn::TransportConfig::default();
            transport.max_idle_timeout(Some(tokio::time::Duration::from_secs(30).try_into().unwrap()));
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