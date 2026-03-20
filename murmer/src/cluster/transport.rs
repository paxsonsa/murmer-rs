use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use super::certs;
use super::config::{NodeClass, NodeIdentity};
use super::error::ClusterError;
use super::framing::{self, ControlMessage, FrameCodec, HandshakePayload, PROTOCOL_VERSION};

// =============================================================================
// TRANSPORT — QUIC server + client + connection pool
// =============================================================================

/// Lifecycle events for connections in the transport layer.
/// Subscribers can use these to react to connectivity changes (e.g., failing
/// pending response futures when a node disconnects).
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// A new connection to a node has been established and stored.
    Connected(String),
    /// A connection to a node has been removed (departure, failure, or
    /// replaced by a new incarnation).
    Disconnected(String),
}

/// An incoming connection that has completed the handshake.
/// Carries the surviving recv stream so the event loop can spawn
/// `run_control_stream_reader` with its own shared channel.
pub struct IncomingConnection {
    pub remote_identity: NodeIdentity,
    pub connection: quinn::Connection,
    pub control_tx: mpsc::UnboundedSender<ControlMessage>,
    /// The recv half of the handshake stream — still live, ready
    /// for the event loop to read ongoing control messages from.
    pub control_recv: quinn::RecvStream,
    /// The peer's declared node class (from handshake).
    pub node_class: NodeClass,
    /// The peer's declared metadata (from handshake).
    pub node_metadata: HashMap<String, String>,
}

/// Manages a single connection to a peer node.
pub struct NodeConnection {
    pub connection: quinn::Connection,
    pub remote_identity: NodeIdentity,
    /// Send control messages to the peer via this channel — a background task
    /// writes them onto the control stream.
    pub control_tx: mpsc::UnboundedSender<ControlMessage>,
}

/// The transport layer: owns the QUIC endpoint, accepts incoming connections,
/// connects to peers, and maintains a connection pool.
pub struct Transport {
    endpoint: quinn::Endpoint,
    identity: NodeIdentity,
    cookie: String,
    type_manifest: Vec<String>,
    node_class: NodeClass,
    node_metadata: HashMap<String, String>,
    connections: Arc<RwLock<HashMap<String, NodeConnection>>>,
    client_config: quinn::ClientConfig,
    shutdown: CancellationToken,
    connection_events_tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl Transport {
    /// Bind a QUIC endpoint and start accepting connections.
    ///
    /// Returns the Transport and a receiver for fully-handshaked incoming
    /// connections. The caller (ClusterSystem) processes those in its event loop.
    pub async fn bind(
        identity: NodeIdentity,
        cookie: String,
        type_manifest: Vec<String>,
        node_class: NodeClass,
        node_metadata: HashMap<String, String>,
        shutdown: CancellationToken,
    ) -> Result<
        (
            Arc<Self>,
            mpsc::UnboundedReceiver<IncomingConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        ClusterError,
    > {
        let (cert_chain, private_key) = certs::generate_self_signed_cert(&identity)?;
        let mut server_config = certs::create_server_config(cert_chain, private_key)?;

        // Set QUIC idle timeout so dead peers are detected promptly.
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(10)).expect("valid idle timeout"),
        ));
        let transport_config = Arc::new(transport_config);
        server_config.transport_config(Arc::clone(&transport_config));

        let mut client_config = certs::create_client_config()?;
        client_config.transport_config(transport_config);

        let endpoint = quinn::Endpoint::server(server_config, identity.socket_addr())?;

        tracing::info!("Transport bound on {}", identity.socket_addr());

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (connection_events_tx, connection_events_rx) = mpsc::unbounded_channel();

        let transport = Arc::new(Self {
            endpoint,
            identity: identity.clone(),
            cookie: cookie.clone(),
            type_manifest: type_manifest.clone(),
            node_class,
            node_metadata,
            connections: Arc::new(RwLock::new(HashMap::new())),
            client_config,
            shutdown: shutdown.clone(),
            connection_events_tx,
        });

        // Spawn the accept loop
        let transport_clone = Arc::clone(&transport);
        tokio::spawn(async move {
            transport_clone.accept_loop(incoming_tx).await;
        });

        Ok((transport, incoming_rx, connection_events_rx))
    }

    /// The actual listen port (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().unwrap()
    }

    /// Connect to a peer node, perform handshake, and store the connection.
    ///
    /// Incarnation-aware dedup: after the QUIC handshake completes and the
    /// remote `NodeIdentity` is known, we check if we already hold a
    /// connection for that node.  If the incarnation (`nid`) matches, the
    /// connect is rejected as a duplicate.  If the incarnation differs the
    /// old connection is torn down and replaced (the node restarted).
    pub async fn connect(
        self: &Arc<Self>,
        addr: SocketAddr,
    ) -> Result<IncomingConnection, ClusterError> {
        // We intentionally do NOT block here based on SocketAddr alone.
        // The pre-handshake address check is removed — dedup happens after
        // the handshake when we know the remote identity and incarnation.

        let mut endpoint = self.endpoint.clone();
        endpoint.set_default_client_config(self.client_config.clone());

        let connection = endpoint
            .connect(addr, "localhost")
            .map_err(|e| ClusterError::Transport(e.to_string()))?
            .await?;

        // Open the control stream (stream 0) and send our handshake
        let (mut send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let our_handshake = HandshakePayload {
            identity: self.identity.clone(),
            cookie: self.cookie.clone(),
            type_manifest: self.type_manifest.clone(),
            protocol_version: PROTOCOL_VERSION,
            node_class: self.node_class.clone(),
            node_metadata: self.node_metadata.clone(),
        };

        let frame = framing::encode_message(&ControlMessage::Handshake(our_handshake))
            .map_err(ClusterError::Serialization)?;
        send.write_all(&frame)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        // Read the peer's handshake response — recv stream stays alive
        let (peer_handshake, recv) = read_handshake(recv).await?;

        // Validate cookie
        if peer_handshake.cookie != self.cookie {
            connection.close(quinn::VarInt::from_u32(1), b"cookie mismatch");
            return Err(ClusterError::CookieMismatch(addr));
        }

        // Enforce protocol version — FDB-style: reject mismatches, no negotiation
        if peer_handshake.protocol_version != PROTOCOL_VERSION {
            connection.close(quinn::VarInt::from_u32(2), b"protocol version mismatch");
            return Err(ClusterError::ProtocolMismatch {
                local: PROTOCOL_VERSION,
                remote: peer_handshake.protocol_version,
                addr,
            });
        }

        let remote_identity = peer_handshake.identity.clone();
        let node_key = remote_identity.node_id_string();

        // Set up control stream writer channel
        let (control_out_tx, control_out_rx) = mpsc::unbounded_channel();

        // Spawn the control stream writer
        tokio::spawn(run_control_stream_writer(
            send,
            control_out_rx,
            self.shutdown.clone(),
        ));

        // Incarnation-aware dedup: check existing connections post-handshake
        {
            let mut conns = self.connections.write().await;

            // Look for an existing connection to the same address
            let stale_key = conns
                .iter()
                .find(|(_, nc)| nc.remote_identity.socket_addr() == addr)
                .map(|(key, nc)| (key.clone(), nc.remote_identity.incarnation));

            if let Some((existing_key, existing_nid)) = stale_key {
                if existing_nid == remote_identity.incarnation {
                    // Same incarnation — true duplicate, reject
                    connection.close(quinn::VarInt::from_u32(0), b"duplicate connection");
                    return Err(ClusterError::Transport(format!(
                        "already connected to {addr} (same incarnation)"
                    )));
                }
                // Different incarnation — node restarted. Tear down old connection.
                if let Some(old) = conns.remove(&existing_key) {
                    old.connection
                        .close(quinn::VarInt::from_u32(0), b"stale incarnation");
                    tracing::info!(
                        "Replaced stale connection {existing_key} (nid {existing_nid}) \
                         with new incarnation (nid {})",
                        remote_identity.incarnation
                    );
                    let _ = self
                        .connection_events_tx
                        .send(ConnectionEvent::Disconnected(existing_key));
                }
            }

            conns.insert(
                node_key.clone(),
                NodeConnection {
                    connection: connection.clone(),
                    remote_identity: remote_identity.clone(),
                    control_tx: control_out_tx.clone(),
                },
            );
        }

        let _ = self
            .connection_events_tx
            .send(ConnectionEvent::Connected(node_key));

        Ok(IncomingConnection {
            remote_identity,
            connection,
            control_tx: control_out_tx,
            control_recv: recv,
            node_class: peer_handshake.node_class,
            node_metadata: peer_handshake.node_metadata,
        })
    }

    /// Get the connection for a given node, if it exists.
    pub async fn get_connection(&self, node_id: &str) -> Option<quinn::Connection> {
        let conns = self.connections.read().await;
        conns.get(node_id).map(|nc| nc.connection.clone())
    }

    /// Send a control message to a specific peer.
    pub async fn send_control(
        &self,
        node_id: &str,
        msg: ControlMessage,
    ) -> Result<(), ClusterError> {
        let conns = self.connections.read().await;
        let nc = conns
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        nc.control_tx
            .send(msg)
            .map_err(|_| ClusterError::Transport("control channel closed".into()))?;
        Ok(())
    }

    /// Send a control message to all connected peers.
    pub async fn broadcast_control(&self, msg: &ControlMessage) {
        let conns = self.connections.read().await;
        for (node_id, nc) in conns.iter() {
            if nc.control_tx.send(msg.clone()).is_err() {
                tracing::warn!("Failed to send control message to {node_id}");
            }
        }
    }

    /// List all connected node IDs.
    pub async fn connected_nodes(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.keys().cloned().collect()
    }

    /// Remove a connection (called when a peer departs or fails).
    ///
    /// Emits a `Disconnected` event so subscribers can react (e.g., fail
    /// pending response futures).
    pub async fn remove_connection(&self, node_id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(nc) = conns.remove(node_id) {
            nc.connection
                .close(quinn::VarInt::from_u32(0), b"node departed");
            let _ = self
                .connection_events_tx
                .send(ConnectionEvent::Disconnected(node_id.to_string()));
        }
    }

    /// Open a new bidirectional stream to a peer for actor messaging.
    pub async fn open_actor_stream(
        &self,
        node_id: &str,
    ) -> Result<(quinn::SendStream, quinn::RecvStream), ClusterError> {
        let conns = self.connections.read().await;
        let nc = conns
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        let (send, recv) = nc
            .connection
            .open_bi()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;
        Ok((send, recv))
    }

    // =========================================================================
    // INTERNAL
    // =========================================================================

    async fn accept_loop(self: Arc<Self>, incoming_tx: mpsc::UnboundedSender<IncomingConnection>) {
        loop {
            tokio::select! {
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        tracing::info!("QUIC endpoint closed");
                        break;
                    };
                    let transport = Arc::clone(&self);
                    let tx = incoming_tx.clone();
                    tokio::spawn(async move {
                        match transport.handle_incoming(incoming).await {
                            Ok(ic) => { let _ = tx.send(ic); }
                            Err(e) => tracing::warn!("Failed to accept connection: {e}"),
                        }
                    });
                }
                _ = self.shutdown.cancelled() => {
                    tracing::info!("Transport shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_incoming(
        self: &Arc<Self>,
        incoming: quinn::Incoming,
    ) -> Result<IncomingConnection, ClusterError> {
        let connection = incoming.await?;
        let remote_addr = connection.remote_address();

        tracing::debug!("Incoming connection from {remote_addr}");

        // Accept the first bidirectional stream — this is the control stream
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        // Read peer's handshake — recv stream stays alive
        let (peer_handshake, recv) = read_handshake(recv).await?;

        // Validate cookie
        if peer_handshake.cookie != self.cookie {
            connection.close(quinn::VarInt::from_u32(1), b"cookie mismatch");
            return Err(ClusterError::CookieMismatch(remote_addr));
        }

        // Enforce protocol version — FDB-style: reject mismatches, no negotiation
        if peer_handshake.protocol_version != PROTOCOL_VERSION {
            connection.close(quinn::VarInt::from_u32(2), b"protocol version mismatch");
            return Err(ClusterError::ProtocolMismatch {
                local: PROTOCOL_VERSION,
                remote: peer_handshake.protocol_version,
                addr: remote_addr,
            });
        }

        // Send our handshake response
        let our_handshake = HandshakePayload {
            identity: self.identity.clone(),
            cookie: self.cookie.clone(),
            type_manifest: self.type_manifest.clone(),
            protocol_version: PROTOCOL_VERSION,
            node_class: self.node_class.clone(),
            node_metadata: self.node_metadata.clone(),
        };
        let frame = framing::encode_message(&ControlMessage::Handshake(our_handshake))
            .map_err(ClusterError::Serialization)?;
        let mut send = send;
        send.write_all(&frame)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let remote_identity = peer_handshake.identity.clone();
        let node_key = remote_identity.node_id_string();

        // Set up control stream writer channel
        let (control_out_tx, control_out_rx) = mpsc::unbounded_channel();

        // Spawn control stream writer
        tokio::spawn(run_control_stream_writer(
            send,
            control_out_rx,
            self.shutdown.clone(),
        ));

        // Incarnation-aware dedup for incoming connections
        {
            let mut conns = self.connections.write().await;

            let stale_key = conns
                .iter()
                .find(|(_, nc)| nc.remote_identity.socket_addr() == remote_addr)
                .map(|(key, nc)| (key.clone(), nc.remote_identity.incarnation));

            if let Some((existing_key, existing_nid)) = stale_key {
                if existing_nid == remote_identity.incarnation {
                    // Same incarnation — duplicate inbound connection, reject
                    connection.close(quinn::VarInt::from_u32(0), b"duplicate connection");
                    return Err(ClusterError::Transport(format!(
                        "already connected to {remote_addr} (same incarnation)"
                    )));
                }
                // Different incarnation — node restarted. Tear down old.
                if let Some(old) = conns.remove(&existing_key) {
                    old.connection
                        .close(quinn::VarInt::from_u32(0), b"stale incarnation");
                    tracing::info!(
                        "Replaced stale incoming connection {existing_key} (nid {existing_nid}) \
                         with new incarnation (nid {})",
                        remote_identity.incarnation
                    );
                    let _ = self
                        .connection_events_tx
                        .send(ConnectionEvent::Disconnected(existing_key));
                }
            }

            conns.insert(
                node_key.clone(),
                NodeConnection {
                    connection: connection.clone(),
                    remote_identity: remote_identity.clone(),
                    control_tx: control_out_tx.clone(),
                },
            );
        }

        let _ = self
            .connection_events_tx
            .send(ConnectionEvent::Connected(node_key));

        tracing::info!("Accepted connection from {remote_identity}");

        Ok(IncomingConnection {
            remote_identity,
            connection,
            control_tx: control_out_tx,
            control_recv: recv,
            node_class: peer_handshake.node_class,
            node_metadata: peer_handshake.node_metadata,
        })
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
    }
}

// =============================================================================
// HELPERS
// =============================================================================

/// Read and validate a handshake from a QUIC receive stream.
/// Returns the handshake payload AND the still-live recv stream so the caller
/// can continue reading control messages from it.
async fn read_handshake(
    mut recv: quinn::RecvStream,
) -> Result<(HandshakePayload, quinn::RecvStream), ClusterError> {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    // Read frame-by-frame until we get one complete handshake frame
    let frame = loop {
        match recv
            .read(&mut buf)
            .await
            .map_err(|e| ClusterError::HandshakeFailed(e.to_string()))?
        {
            Some(n) => {
                codec.push_data(&buf[..n]);
                if let Some(frame) = codec
                    .next_frame()
                    .map_err(|e| ClusterError::HandshakeFailed(e.to_string()))?
                {
                    break frame;
                }
            }
            None => {
                return Err(ClusterError::HandshakeFailed(
                    "stream closed before handshake complete".into(),
                ));
            }
        }
    };

    let msg: ControlMessage =
        framing::decode_message(&frame).map_err(ClusterError::Deserialization)?;

    match msg {
        ControlMessage::Handshake(payload) => Ok((payload, recv)),
        other => Err(ClusterError::HandshakeFailed(format!(
            "expected Handshake, got {other:?}"
        ))),
    }
}

/// Background task: reads ControlMessage from a channel and writes them as
/// length-prefixed frames to the QUIC send stream.
async fn run_control_stream_writer(
    mut send: quinn::SendStream,
    mut rx: mpsc::UnboundedReceiver<ControlMessage>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                let frame = match framing::encode_message(&msg) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!("Failed to encode control message: {e}");
                        continue;
                    }
                };
                if let Err(e) = send.write_all(&frame).await {
                    tracing::warn!("Control stream write failed: {e}");
                    break;
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
    let _ = send.finish();
}

/// Background task: reads length-prefixed frames from a QUIC receive stream
/// and sends them as ControlMessages to the provided channel.
pub async fn run_control_stream_reader(
    mut recv: quinn::RecvStream,
    tx: mpsc::UnboundedSender<(String, ControlMessage)>,
    node_id: String,
    shutdown: CancellationToken,
) {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    loop {
        tokio::select! {
            result = recv.read(&mut buf) => {
                match result {
                    Ok(Some(n)) => {
                        codec.push_data(&buf[..n]);
                        while let Ok(Some(frame)) = codec.next_frame() {
                            match framing::decode_message::<ControlMessage>(&frame) {
                                Ok(msg) => {
                                    if tx.send((node_id.clone(), msg)).is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to decode control message from {node_id}: {e}");
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::debug!("Control stream from {node_id} closed");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Control stream read error from {node_id}: {e}");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}
