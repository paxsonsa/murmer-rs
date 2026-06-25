//! iroh transport layer — the production [`Net`] implementation.
//!
//! [`Transport`] owns an `iroh::Endpoint` and maintains a connection map keyed by
//! node id (endpoint id + incarnation). It handles:
//!
//! - Outbound connections with incarnation-based deduplication
//! - Incoming connection acceptance and the cookie/identity handshake
//! - Connection lifecycle events (opened, replaced, removed)
//!
//! Peers are identified and authenticated by their iroh [`EndpointId`] (an
//! ed25519 public key verified by iroh's TLS handshake). The zero-trust
//! allowlist is enforced at the iroh layer via an [`AllowlistHook`] before any
//! application bytes flow; the cookie handshake here is a secondary coarse gate.
//!
//! `Transport` implements the [`Net`] seam (`super::net`): all iroh stream and
//! connection types are wrapped in the boxed [`SendHalf`]/[`RecvHalf`]/
//! [`Connection`](super::net::Connection) trait objects at this boundary, so the
//! rest of the cluster (event loop, remote dispatch) is iroh-agnostic and can be
//! swapped for an in-memory deterministic fabric in simulation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iroh::endpoint::{
    Connection, IdleTimeout, Incoming, QuicTransportConfig, RecvStream, VarInt, presets,
};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr};
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::instrument;

use super::allowlist::Allowlist;
use super::config::{NodeClass, NodeIdentity, TransportTuning};
use super::error::ClusterError;
use super::framing::{self, ControlMessage, FrameCodec, HandshakePayload, PROTOCOL_VERSION};
use super::net::iroh::{IrohConnection, IrohRecv, IrohSend};
use super::net::{
    self, ConnectionEvent, IncomingConnection, Net, NodeId, PeerAddr, RecvHalf, SendHalf,
};

/// ALPN identifying the murmer cluster protocol on iroh connections.
pub const MURMER_ALPN: &[u8] = b"murmer/cluster/1";

/// Convert an iroh [`EndpointAddr`] into the seam-neutral [`PeerAddr`] the
/// [`Net`] trait dials with. This is the iroh→seam direction (the inverse of
/// what [`Net::connect`] does internally); it lives here, at the iroh boundary,
/// so `net/mod.rs` stays free of iroh types.
///
/// `ip_addrs()` keeps only the direct IP sockets (relay/custom transport addrs
/// are dropped) — which is exactly what `PeerAddr.hint` models, and in
/// self-contained mode (relays disabled) those are the only addrs present.
pub fn peer_addr(addr: &EndpointAddr) -> PeerAddr {
    PeerAddr {
        id: NodeId(addr.id.to_string()),
        hint: addr.ip_addrs().copied().collect(),
    }
}

// =============================================================================
// TRANSPORT — iroh endpoint + connection pool
// =============================================================================

/// Manages a single connection to a peer node.
pub struct NodeConnection {
    pub connection: Connection,
    pub remote_identity: NodeIdentity,
    /// Send control messages to the peer via this channel — a background task
    /// writes them onto the control stream.
    pub control_tx: mpsc::UnboundedSender<ControlMessage>,
}

/// The transport layer: owns the iroh endpoint, accepts incoming connections,
/// connects to peers, and maintains a connection pool. This is the production
/// [`Net`] implementation.
pub struct Transport {
    endpoint: Endpoint,
    identity: NodeIdentity,
    cookie: String,
    type_manifest: Vec<String>,
    node_class: NodeClass,
    node_metadata: HashMap<String, String>,
    connections: Arc<RwLock<HashMap<String, NodeConnection>>>,
    shutdown: CancellationToken,
    connection_events_tx: mpsc::UnboundedSender<ConnectionEvent>,
    /// Whether this transport is a pure Edge client (no server, no actor hosting).
    is_edge_client: bool,
}

/// Translate murmer's [`TransportTuning`] into an iroh quic transport config.
fn build_transport_config(tuning: &TransportTuning) -> QuicTransportConfig {
    let mut builder = QuicTransportConfig::builder();
    builder = builder.initial_rtt(Duration::from_millis(tuning.initial_rtt_ms));
    builder = builder.max_idle_timeout(Some(
        IdleTimeout::try_from(Duration::from_secs(tuning.max_idle_timeout_secs))
            .expect("valid idle timeout"),
    ));
    if let Some(interval) = tuning.keep_alive_interval_secs {
        builder = builder.keep_alive_interval(Duration::from_secs(interval));
    }
    builder =
        builder.max_concurrent_bidi_streams(VarInt::from_u32(tuning.max_concurrent_bidi_streams));
    builder = builder.stream_receive_window(VarInt::from_u32(tuning.stream_receive_window));
    builder = builder.receive_window(VarInt::from_u32(tuning.receive_window));
    builder = builder.send_window(tuning.send_window);
    builder = builder.initial_mtu(tuning.initial_mtu);
    builder.build()
}

impl Transport {
    /// Bind an iroh endpoint and start accepting connections.
    ///
    /// Returns the Transport and a receiver for fully-handshaked incoming
    /// connections. The caller (ClusterSystem) processes those in its event loop.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind(
        identity: NodeIdentity,
        secret_key: SecretKey,
        cookie: String,
        type_manifest: Vec<String>,
        node_class: NodeClass,
        node_metadata: HashMap<String, String>,
        tuning: TransportTuning,
        allowlist: Allowlist,
        shutdown: CancellationToken,
    ) -> Result<
        (
            Arc<Self>,
            mpsc::UnboundedReceiver<IncomingConnection>,
            mpsc::UnboundedReceiver<ConnectionEvent>,
        ),
        ClusterError,
    > {
        // Self-contained: Minimal preset = crypto provider + RelayMode::Disabled,
        // no n0 DNS / relay. The allowlist hook enforces zero-trust authorization
        // on both inbound and outbound connections.
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .alpns(vec![MURMER_ALPN.to_vec()])
            .transport_config(build_transport_config(&tuning))
            .hooks(allowlist.hook())
            .bind_addr(identity.socket_addr())
            .map_err(|e| ClusterError::Transport(e.to_string()))?
            .bind()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        tracing::info!(
            endpoint_id = %identity.endpoint_id,
            addr = ?endpoint.bound_sockets(),
            "iroh transport bound"
        );

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (connection_events_tx, connection_events_rx) = mpsc::unbounded_channel();

        let transport = Arc::new(Self {
            endpoint,
            identity,
            cookie,
            type_manifest,
            node_class,
            node_metadata,
            connections: Arc::new(RwLock::new(HashMap::new())),
            shutdown: shutdown.clone(),
            connection_events_tx,
            is_edge_client: false,
        });

        // Spawn the accept loop.
        let transport_clone = Arc::clone(&transport);
        tokio::spawn(async move {
            transport_clone.accept_loop(incoming_tx).await;
        });

        // Spawn the revocation handler: when a peer is removed from the allowlist,
        // drop any live connection to it (fail-fast — no grace period).
        if let Some(revoked_rx) = allowlist.subscribe_revocations() {
            let transport_clone = Arc::clone(&transport);
            tokio::spawn(async move {
                transport_clone.revocation_loop(revoked_rx).await;
            });
        }

        Ok((transport, incoming_rx, connection_events_rx))
    }

    /// Create a client-only transport that can connect to cluster nodes but does
    /// not accept incoming connections (no accept loop). Edge clients use this.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_only(
        identity: NodeIdentity,
        secret_key: SecretKey,
        cookie: String,
        node_class: NodeClass,
        node_metadata: HashMap<String, String>,
        tuning: TransportTuning,
        allowlist: Allowlist,
        shutdown: CancellationToken,
    ) -> Result<(Arc<Self>, mpsc::UnboundedReceiver<ConnectionEvent>), ClusterError> {
        // Bind on an ephemeral port. No accept loop is spawned.
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .alpns(vec![MURMER_ALPN.to_vec()])
            .transport_config(build_transport_config(&tuning))
            .hooks(allowlist.hook())
            .bind()
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let (connection_events_tx, connection_events_rx) = mpsc::unbounded_channel();

        let transport = Arc::new(Self {
            endpoint,
            identity,
            cookie,
            type_manifest: Vec::new(),
            node_class,
            node_metadata,
            connections: Arc::new(RwLock::new(HashMap::new())),
            shutdown,
            connection_events_tx,
            is_edge_client: true,
        });

        Ok((transport, connection_events_rx))
    }

    // =========================================================================
    // INTERNAL
    // =========================================================================

    fn handshake_payload(&self) -> HandshakePayload {
        HandshakePayload {
            identity: self.identity.clone(),
            cookie: self.cookie.clone(),
            type_manifest: self.type_manifest.clone(),
            protocol_version: PROTOCOL_VERSION,
            node_class: self.node_class.clone(),
            node_metadata: self.node_metadata.clone(),
            is_edge_client: self.is_edge_client,
        }
    }

    /// Validate a peer's handshake: cookie, protocol version, and — critically —
    /// that the identity it *claims* matches the iroh-authenticated endpoint id.
    fn validate_handshake(
        &self,
        connection: &Connection,
        peer: &HandshakePayload,
        expected_id: EndpointId,
    ) -> Result<(), ClusterError> {
        let authenticated_id = connection.remote_id();

        // The handshake's claimed identity must match the cryptographically
        // authenticated endpoint id — a peer cannot lie about who it is. The
        // claimed id is a `NodeId`; map the iroh-authenticated key into one to
        // compare (this is the iroh boundary that owns `EndpointId ⇄ NodeId`).
        if peer.identity.endpoint_id != NodeId(authenticated_id.to_string()) {
            connection.close(VarInt::from_u32(4), b"identity mismatch");
            return Err(ClusterError::HandshakeFailed(format!(
                "claimed identity {} != authenticated key {authenticated_id}",
                peer.identity.endpoint_id
            )));
        }
        // For outbound dials, the peer we reached must be the one we intended.
        if authenticated_id != expected_id {
            connection.close(VarInt::from_u32(4), b"unexpected peer");
            return Err(ClusterError::HandshakeFailed(format!(
                "connected to {authenticated_id}, expected {expected_id}"
            )));
        }
        if peer.cookie != self.cookie {
            connection.close(VarInt::from_u32(1), b"cookie mismatch");
            return Err(ClusterError::NotInAllowlist(format!(
                "{} (cookie mismatch)",
                authenticated_id
            )));
        }
        if peer.protocol_version != PROTOCOL_VERSION {
            connection.close(VarInt::from_u32(2), b"protocol version mismatch");
            return Err(ClusterError::HandshakeFailed(format!(
                "protocol mismatch: local={PROTOCOL_VERSION}, remote={}",
                peer.protocol_version
            )));
        }
        Ok(())
    }

    /// Incarnation-aware dedup keyed on endpoint id, then insert the connection.
    async fn dedup_and_store(
        &self,
        connection: &Connection,
        remote_identity: &NodeIdentity,
        node_key: &str,
        control_out_tx: &mpsc::UnboundedSender<ControlMessage>,
    ) -> Result<(), ClusterError> {
        let mut conns = self.connections.write().await;

        // Find an existing connection to the same endpoint id (any incarnation).
        let stale_key = conns
            .iter()
            .find(|(_, nc)| nc.remote_identity.endpoint_id == remote_identity.endpoint_id)
            .map(|(key, nc)| (key.clone(), nc.remote_identity.incarnation));

        if let Some((existing_key, existing_nid)) = stale_key {
            if existing_nid == remote_identity.incarnation {
                connection.close(VarInt::from_u32(0), b"duplicate connection");
                return Err(ClusterError::Transport(format!(
                    "already connected to {} (same incarnation)",
                    remote_identity.endpoint_id
                )));
            }
            // Different incarnation — node restarted. Tear down the old one.
            if let Some(old) = conns.remove(&existing_key) {
                old.connection
                    .close(VarInt::from_u32(0), b"stale incarnation");
                tracing::info!(
                    "Replaced stale connection {existing_key} (nid {existing_nid}) with new \
                     incarnation (nid {})",
                    remote_identity.incarnation
                );
                instrument::connection_closed();
                let _ = self
                    .connection_events_tx
                    .send(ConnectionEvent::Disconnected(existing_key));
            }
        }

        conns.insert(
            node_key.to_string(),
            NodeConnection {
                connection: connection.clone(),
                remote_identity: remote_identity.clone(),
                control_tx: control_out_tx.clone(),
            },
        );
        Ok(())
    }

    async fn accept_loop(self: Arc<Self>, incoming_tx: mpsc::UnboundedSender<IncomingConnection>) {
        loop {
            tokio::select! {
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        tracing::info!("iroh endpoint closed");
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

    /// Drop live connections to peers revoked from the allowlist.
    async fn revocation_loop(
        self: Arc<Self>,
        mut revoked_rx: tokio::sync::broadcast::Receiver<EndpointId>,
    ) {
        loop {
            tokio::select! {
                msg = revoked_rx.recv() => {
                    let revoked = match msg {
                        Ok(id) => id,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    };
                    let keys: Vec<String> = {
                        let conns = self.connections.read().await;
                        let revoked_id = NodeId(revoked.to_string());
                        conns
                            .iter()
                            .filter(|(_, nc)| nc.remote_identity.endpoint_id == revoked_id)
                            .map(|(k, _)| k.clone())
                            .collect()
                    };
                    for key in keys {
                        tracing::warn!(node = %key, "revoking connection (removed from allowlist)");
                        self.remove_connection(&key).await;
                    }
                }
                _ = self.shutdown.cancelled() => break,
            }
        }
    }

    async fn handle_incoming(
        &self,
        incoming: Incoming,
    ) -> Result<IncomingConnection, ClusterError> {
        let connection = incoming
            .await
            .map_err(|e| ClusterError::Connection(e.to_string()))?;
        let remote_id = connection.remote_id();
        tracing::debug!(endpoint_id = %remote_id, "Incoming connection");

        // Accept the first bidirectional stream — this is the control stream.
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|e| ClusterError::Connection(e.to_string()))?;

        // Read peer's handshake — recv stream stays alive.
        let (peer_handshake, recv) = read_handshake(recv).await?;

        // Validate against the authenticated key (expected == actual for inbound).
        self.validate_handshake(&connection, &peer_handshake, remote_id)?;

        // Send our handshake response.
        let frame = framing::encode_message(&ControlMessage::Handshake(self.handshake_payload()))
            .map_err(ClusterError::Serialization)?;
        let mut send = send;
        send.write_all(&frame)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        let remote_identity = peer_handshake.identity.clone();
        let node_key = remote_identity.node_id_string();
        let (control_out_tx, control_out_rx) = mpsc::unbounded_channel();
        tokio::spawn(net::run_control_stream_writer(
            Box::new(IrohSend(send)),
            control_out_rx,
            self.shutdown.clone(),
        ));

        self.dedup_and_store(&connection, &remote_identity, &node_key, &control_out_tx)
            .await?;

        instrument::connection_opened();
        let _ = self
            .connection_events_tx
            .send(ConnectionEvent::Connected(node_key));

        tracing::info!("Accepted connection from {remote_identity}");

        Ok(IncomingConnection {
            remote_identity,
            connection: Box::new(IrohConnection(connection)),
            control_tx: control_out_tx,
            control_recv: Box::new(IrohRecv(recv)),
            node_class: peer_handshake.node_class,
            node_metadata: peer_handshake.node_metadata,
            is_edge_client: peer_handshake.is_edge_client,
        })
    }
}

// =============================================================================
// NET IMPL — the iroh production transport behind the seam
// =============================================================================

#[async_trait]
impl Net for Transport {
    fn node_id(&self) -> NodeId {
        // The stable, authenticated identity. The pool/route key elsewhere is the
        // `node_id_string` (endpoint id + incarnation); `NodeId` is the identity
        // that flows through discovery/handshake.
        self.identity.endpoint_id.clone()
    }

    /// The actual bound listen address (useful when binding to port 0).
    fn local_addr(&self) -> SocketAddr {
        self.endpoint
            .bound_sockets()
            .into_iter()
            .next()
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    /// Connect to a peer node by its abstract [`PeerAddr`] (node id + address
    /// hints), perform the handshake, and store the connection.
    ///
    /// The `PeerAddr` is converted to an iroh [`EndpointAddr`] at this boundary:
    /// `id` is the endpoint id's string form and `hint` carries direct-dial
    /// socket addresses.
    ///
    /// Incarnation-aware dedup: after the iroh handshake completes and the
    /// remote `NodeIdentity` is known, we check whether we already hold a
    /// connection for that endpoint id. Same incarnation → duplicate, rejected.
    /// Different incarnation → the node restarted; the old connection is replaced.
    async fn connect(&self, addr: PeerAddr) -> Result<IncomingConnection, ClusterError> {
        let target_id: EndpointId =
            addr.id.0.parse().map_err(|_| {
                ClusterError::Transport(format!("invalid endpoint id: {}", addr.id))
            })?;
        let endpoint_addr =
            EndpointAddr::from_parts(target_id, addr.hint.iter().copied().map(TransportAddr::Ip));

        let connection = self
            .endpoint
            .connect(endpoint_addr, MURMER_ALPN)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        // Open the control stream (stream 0) and send our handshake.
        let (mut send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| ClusterError::Connection(e.to_string()))?;

        let our_handshake = self.handshake_payload();
        let frame = framing::encode_message(&ControlMessage::Handshake(our_handshake))
            .map_err(ClusterError::Serialization)?;
        send.write_all(&frame)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))?;

        // Read the peer's handshake response — recv stream stays alive.
        let (peer_handshake, recv) = read_handshake(recv).await?;

        self.validate_handshake(&connection, &peer_handshake, target_id)?;

        let remote_identity = peer_handshake.identity.clone();
        let node_key = remote_identity.node_id_string();
        let (control_out_tx, control_out_rx) = mpsc::unbounded_channel();
        tokio::spawn(net::run_control_stream_writer(
            Box::new(IrohSend(send)),
            control_out_rx,
            self.shutdown.clone(),
        ));

        self.dedup_and_store(&connection, &remote_identity, &node_key, &control_out_tx)
            .await?;

        instrument::connection_opened();
        let _ = self
            .connection_events_tx
            .send(ConnectionEvent::Connected(node_key));

        Ok(IncomingConnection {
            remote_identity,
            connection: Box::new(IrohConnection(connection)),
            control_tx: control_out_tx,
            control_recv: Box::new(IrohRecv(recv)),
            node_class: peer_handshake.node_class,
            node_metadata: peer_handshake.node_metadata,
            is_edge_client: peer_handshake.is_edge_client,
        })
    }

    /// Send a control message to a specific peer.
    async fn send_control(&self, node_id: &str, msg: ControlMessage) -> Result<(), ClusterError> {
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
    async fn broadcast_control(&self, msg: &ControlMessage) {
        let conns = self.connections.read().await;
        for (node_id, nc) in conns.iter() {
            if nc.control_tx.send(msg.clone()).is_err() {
                tracing::warn!("Failed to send control message to {node_id}");
            }
        }
    }

    /// Open a new bidirectional stream to a peer for actor messaging.
    async fn open_actor_stream(
        &self,
        node_id: &str,
    ) -> Result<(Box<dyn SendHalf>, Box<dyn RecvHalf>), ClusterError> {
        let conns = self.connections.read().await;
        let nc = conns
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        let (send, recv) = nc
            .connection
            .open_bi()
            .await
            .map_err(|e| ClusterError::Connection(e.to_string()))?;
        Ok((Box::new(IrohSend(send)), Box::new(IrohRecv(recv))))
    }

    /// List all connected node IDs.
    async fn connected_nodes(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.keys().cloned().collect()
    }

    /// Remove a connection (called when a peer departs or fails).
    async fn remove_connection(&self, node_id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(nc) = conns.remove(node_id) {
            nc.connection.close(VarInt::from_u32(0), b"node departed");
            instrument::connection_closed();
            let _ = self
                .connection_events_tx
                .send(ConnectionEvent::Disconnected(node_id.to_string()));
        }
    }
}

// The iroh endpoint closes itself when its last handle is dropped, so no
// explicit `Drop` is needed here (and `Endpoint::close` is async — unusable
// from `Drop` anyway).

// =============================================================================
// HELPERS
// =============================================================================

/// Read and validate a handshake from an iroh receive stream.
/// Returns the handshake payload AND the still-live recv stream so the caller
/// can continue reading control messages from it.
async fn read_handshake(
    mut recv: RecvStream,
) -> Result<(HandshakePayload, RecvStream), ClusterError> {
    let mut codec = FrameCodec::new();
    let mut buf = vec![0u8; 8192];

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

// =============================================================================
// ALLOWLIST INTEGRATION TESTS — prove the zero-trust security property
// =============================================================================

#[cfg(test)]
mod allowlist_tests {
    use super::*;
    use crate::cluster::allowlist::{Allowlist, add_to_file, remove_from_file, write_file};
    use crate::cluster::config::AllowlistMode;
    use iroh::SecretKey;
    use std::collections::HashSet;

    fn install_crypto() {
        crate::cluster::install_default_crypto();
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "murmer-allow-it-{tag}-{}-{}.txt",
            std::process::id(),
            tag
        ))
    }

    /// Bind a transport with a given allowlist; returns the transport, its
    /// inbound-connection receiver, and its identity.
    async fn bind_node(
        name: &str,
        allowlist: Allowlist,
        shutdown: CancellationToken,
    ) -> (
        Arc<Transport>,
        mpsc::UnboundedReceiver<IncomingConnection>,
        NodeIdentity,
    ) {
        let sk = SecretKey::generate();
        let identity = NodeIdentity::new(name, sk.public(), "127.0.0.1", 0);
        let (t, incoming, _events) = Transport::bind(
            identity.clone(),
            sk,
            "it-cookie".to_string(),
            vec![],
            NodeClass::Worker,
            HashMap::new(),
            TransportTuning::default(),
            allowlist,
            shutdown,
        )
        .await
        .expect("bind");
        (t, incoming, identity)
    }

    /// The bound node's identity as an iroh [`EndpointId`] — for allowlist files,
    /// which authorize raw ed25519 keys. The `NodeId` holds the key's string form,
    /// so it round-trips.
    fn eid(id: &NodeIdentity) -> EndpointId {
        id.endpoint_id
            .0
            .parse()
            .expect("test node id is a valid key")
    }

    /// The abstract [`PeerAddr`] to dial a bound node on loopback: its node id
    /// plus the bound socket as the dial hint.
    fn dial_addr(t: &Transport, id: &NodeIdentity) -> PeerAddr {
        PeerAddr {
            id: id.endpoint_id.clone(),
            hint: vec![t.local_addr()],
        }
    }

    #[tokio::test]
    async fn rejects_peer_not_in_allowlist() {
        install_crypto();
        let shutdown = CancellationToken::new();

        // A enforces an EMPTY allowlist — it trusts nobody.
        let path = tmp("reject");
        write_file(&path, &HashSet::new()).unwrap();
        let a_allow =
            Allowlist::new(AllowlistMode::Enforced(path.clone()), shutdown.clone()).unwrap();
        let (a, _a_in, a_id) = bind_node("a", a_allow, shutdown.clone()).await;

        // B is open and dials A.
        let (b, _b_in, _b_id) = bind_node("b", Allowlist::open(), shutdown.clone()).await;

        let result = b.connect(dial_addr(&a, &a_id)).await;
        assert!(result.is_err(), "A must reject a peer not in its allowlist");
        assert!(a.connected_nodes().await.is_empty());

        shutdown.cancel();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn permits_peer_in_allowlist() {
        install_crypto();
        let shutdown = CancellationToken::new();

        let (b, _b_in, b_id) = bind_node("b", Allowlist::open(), shutdown.clone()).await;

        // A enforces an allowlist containing B's id.
        let path = tmp("permit");
        let mut set = HashSet::new();
        set.insert(eid(&b_id));
        write_file(&path, &set).unwrap();
        let a_allow =
            Allowlist::new(AllowlistMode::Enforced(path.clone()), shutdown.clone()).unwrap();
        let (a, _a_in, a_id) = bind_node("a", a_allow, shutdown.clone()).await;

        let result = b.connect(dial_addr(&a, &a_id)).await;
        assert!(
            result.is_ok(),
            "A must accept an allowlisted peer: {:?}",
            result.err()
        );

        shutdown.cancel();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn hot_add_lets_a_previously_rejected_peer_join() {
        install_crypto();
        let shutdown = CancellationToken::new();

        let (b, _b_in, b_id) = bind_node("b", Allowlist::open(), shutdown.clone()).await;

        // A starts trusting nobody.
        let path = tmp("hotadd");
        write_file(&path, &HashSet::new()).unwrap();
        let a_allow =
            Allowlist::new(AllowlistMode::Enforced(path.clone()), shutdown.clone()).unwrap();
        let (a, _a_in, a_id) = bind_node("a", a_allow, shutdown.clone()).await;

        assert!(
            b.connect(dial_addr(&a, &a_id)).await.is_err(),
            "rejected before add"
        );

        // Hot-add B to A's allowlist file — no restart.
        add_to_file(&path, eid(&b_id)).unwrap();
        // Wait past the 1s file-watch poll.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        assert!(
            b.connect(dial_addr(&a, &a_id)).await.is_ok(),
            "B should join after being hot-added, without A restarting"
        );

        shutdown.cancel();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn revokes_live_connection_on_removal() {
        install_crypto();
        let shutdown = CancellationToken::new();

        let (b, _b_in, b_id) = bind_node("b", Allowlist::open(), shutdown.clone()).await;

        let path = tmp("revoke");
        let mut set = HashSet::new();
        set.insert(eid(&b_id));
        write_file(&path, &set).unwrap();
        let a_allow =
            Allowlist::new(AllowlistMode::Enforced(path.clone()), shutdown.clone()).unwrap();
        let (a, mut a_in, a_id) = bind_node("a", a_allow, shutdown.clone()).await;

        // Establish the connection and let A register it (A learns of inbound
        // connections via its incoming-connection receiver).
        assert!(b.connect(dial_addr(&a, &a_id)).await.is_ok());
        let _ic = tokio::time::timeout(Duration::from_secs(2), a_in.recv())
            .await
            .expect("A should accept inbound")
            .expect("incoming connection");
        assert_eq!(a.connected_nodes().await.len(), 1, "A holds the connection");

        // Revoke B by removing it from the file; A's watcher drops the connection.
        remove_from_file(&path, &eid(&b_id)).unwrap();
        let mut revoked = false;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if a.connected_nodes().await.is_empty() {
                revoked = true;
                break;
            }
        }
        assert!(revoked, "A must drop the live connection when B is revoked");

        shutdown.cancel();
        let _ = std::fs::remove_file(&path);
    }
}
