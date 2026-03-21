pub mod certs;
pub mod config;
pub mod discovery;
pub mod error;
pub mod framing;
pub mod membership;
pub mod remote;
pub mod sync;
pub mod transport;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    Actor, DispatchRequest, Endpoint, Listing, OpType, ReceptionKey, Receptionist,
    ReceptionistConfig, RemoteDispatch, RemoteInvocation,
};

use config::{ClusterConfig, NodeClass, NodeIdentity};
use error::ClusterError;
use framing::ControlMessage;
use membership::{ClusterEvent, ClusterMembership, FocaRuntime, TimerEvent};
use sync::{SpawnRegistry, TypeRegistry};
use transport::{ConnectionEvent, Transport};

// =============================================================================
// NODE REGISTRY — tracks node class and metadata from handshakes
// =============================================================================

/// Stores class and metadata for each connected node, populated during handshake.
///
/// This information is not part of the SWIM protocol — it comes from the QUIC
/// handshake and is only available for nodes we've directly connected to.
#[derive(Debug, Clone)]
pub struct NodeRegistryEntry {
    pub class: NodeClass,
    pub metadata: HashMap<String, String>,
}

/// Thread-safe registry of node capabilities, populated during connection setup.
#[derive(Debug, Clone, Default)]
pub struct NodeRegistry {
    nodes: Arc<std::sync::RwLock<HashMap<String, NodeRegistryEntry>>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a node's class and metadata (called during handle_new_connection).
    pub fn insert(&self, node_id: &str, class: NodeClass, metadata: HashMap<String, String>) {
        self.nodes
            .write()
            .unwrap()
            .insert(node_id.to_string(), NodeRegistryEntry { class, metadata });
    }

    /// Look up a node's class and metadata.
    pub fn get(&self, node_id: &str) -> Option<NodeRegistryEntry> {
        self.nodes.read().unwrap().get(node_id).cloned()
    }

    /// Remove a node entry (called when a node departs/fails).
    pub fn remove(&self, node_id: &str) {
        self.nodes.write().unwrap().remove(node_id);
    }
}

// =============================================================================
// CLUSTER SYSTEM — the main entry point for clustered actor systems
// =============================================================================

/// A clustered actor system. Manages local actors, QUIC transport, SWIM
/// membership, mDNS/seed discovery, and OpLog-based registry replication.
pub struct ClusterSystem {
    receptionist: Receptionist,
    transport: Arc<Transport>,
    #[allow(dead_code)]
    config: ClusterConfig,
    identity: NodeIdentity,
    event_tx: broadcast::Sender<ClusterEvent>,
    #[allow(dead_code)]
    type_registry: Arc<TypeRegistry>,
    node_registry: NodeRegistry,
    shutdown: CancellationToken,
}

impl ClusterSystem {
    /// Start a new clustered actor system.
    ///
    /// This binds the QUIC transport, starts discovery, initializes SWIM
    /// membership, and spawns the main event loop.
    pub async fn start(
        config: ClusterConfig,
        type_registry: TypeRegistry,
        spawn_registry: SpawnRegistry,
    ) -> Result<Self, ClusterError> {
        let identity = config.identity.clone();
        let shutdown = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(256);

        // Receptionist with this node's identity
        let receptionist = Receptionist::with_config(ReceptionistConfig {
            node_id: identity.node_id_string(),
            origin_addr: format!("{}:{}", identity.host, identity.port),
            ..Default::default()
        });

        // Type manifest from the registry
        let type_manifest = type_registry.known_types();

        // Bind QUIC transport with tuned parameters
        let (transport, incoming_rx, connection_events_rx) = Transport::bind(
            identity.clone(),
            config.cookie.clone(),
            type_manifest,
            config.node_class.clone(),
            config.node_metadata.clone(),
            config.transport.clone(),
            shutdown.clone(),
        )
        .await?;

        // Start discovery
        let discovery_rx =
            discovery::start_discovery(&identity, &config.discovery, shutdown.clone());

        // Initialize SWIM membership
        let membership = ClusterMembership::new(identity.clone(), event_tx.clone());

        let type_registry = Arc::new(type_registry);
        let spawn_registry = Arc::new(spawn_registry);
        let node_registry = NodeRegistry::new();

        // Spawn the main event loop
        let system = Self {
            receptionist: receptionist.clone(),
            transport: Arc::clone(&transport),
            config: config.clone(),
            identity: identity.clone(),
            event_tx: event_tx.clone(),
            type_registry: Arc::clone(&type_registry),
            node_registry: node_registry.clone(),
            shutdown: shutdown.clone(),
        };

        spawn_event_loop(
            receptionist,
            transport,
            membership,
            identity,
            event_tx,
            type_registry,
            spawn_registry,
            node_registry,
            incoming_rx,
            discovery_rx,
            connection_events_rx,
            shutdown,
        );

        tracing::info!("ClusterSystem started: {}", system.identity);

        Ok(system)
    }

    /// Start a local actor and register it with the receptionist.
    pub fn start_actor<A>(&self, label: &str, actor: A, state: A::State) -> Endpoint<A>
    where
        A: Actor + RemoteDispatch + 'static,
    {
        self.receptionist.start(label, actor, state)
    }

    /// Look up an actor by label (local or remote).
    pub fn lookup<A: Actor + 'static>(&self, label: &str) -> Option<Endpoint<A>> {
        self.receptionist.lookup(label)
    }

    /// Get a listing (subscription) for a reception key.
    pub fn listing<A: Actor + RemoteDispatch + 'static>(
        &self,
        key: &ReceptionKey<A>,
    ) -> Listing<A> {
        self.receptionist.listing(key.clone())
    }

    /// Subscribe to cluster events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_tx.subscribe()
    }

    /// Get this node's identity.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Get a reference to the receptionist.
    pub fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    /// Access the node registry (class and metadata for connected nodes).
    pub fn node_registry(&self) -> &NodeRegistry {
        &self.node_registry
    }

    /// Access the underlying transport (for orchestration integration).
    pub fn transport(&self) -> &Arc<Transport> {
        &self.transport
    }

    /// Get the actual bound address (useful when binding to port 0).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.transport.local_addr()
    }

    /// Shut down the cluster system gracefully.
    ///
    /// Broadcasts a `Departure` message to all connected peers so they can
    /// prune this node's actors immediately instead of waiting for SWIM timeout.
    pub async fn shutdown(&self) {
        // Broadcast departure to all connected peers
        let departure = ControlMessage::Departure(self.identity.clone());
        self.transport.broadcast_control(&departure).await;

        // Small grace period for message delivery
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the event loop
        self.shutdown.cancel();
    }
}

// =============================================================================
// EVENT LOOP — the main select! loop driving the cluster
// =============================================================================

#[allow(clippy::too_many_arguments)]
fn spawn_event_loop(
    receptionist: Receptionist,
    transport: Arc<Transport>,
    mut membership: ClusterMembership,
    identity: NodeIdentity,
    event_tx: broadcast::Sender<ClusterEvent>,
    type_registry: Arc<TypeRegistry>,
    spawn_registry: Arc<SpawnRegistry>,
    node_registry: NodeRegistry,
    mut incoming_rx: mpsc::UnboundedReceiver<transport::IncomingConnection>,
    mut discovery_rx: mpsc::UnboundedReceiver<discovery::DiscoveryEvent>,
    mut conn_events: mpsc::UnboundedReceiver<transport::ConnectionEvent>,
    shutdown: CancellationToken,
) {
    // Channels for the foca runtime
    let (swim_tx, mut swim_rx) = mpsc::unbounded_channel::<(NodeIdentity, Vec<u8>)>();
    let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<TimerEvent>();
    let (control_in_tx, mut control_in_rx) = mpsc::unbounded_channel::<(String, ControlMessage)>();

    // Bridge for outbound connections: discovery spawns connect(), sends the
    // resulting IncomingConnection here so the event loop can set up stream
    // acceptance, foca membership, and initial sync — same as for inbound.
    let (connected_tx, mut connected_rx) =
        mpsc::unbounded_channel::<transport::IncomingConnection>();

    // Subscribe to cluster events for NodeLeft pruning
    let mut cluster_event_rx = event_tx.subscribe();

    let mut runtime = FocaRuntime::new(identity.clone(), event_tx.clone(), swim_tx, timer_tx);

    // Periodic sync interval
    let mut sync_interval = tokio::time::interval(Duration::from_secs(5));

    // Track addresses we're currently connecting to, to avoid duplicates
    let connecting = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::<
        SocketAddr,
    >::new()));

    tokio::spawn(async move {
        // Helper closure factored into an inline fn below to handle a new
        // connection identically regardless of inbound vs outbound origin.

        loop {
            tokio::select! {
                // ── Incoming handshaked connections (accepted by accept_loop) ─
                Some(incoming) = incoming_rx.recv() => {
                    handle_new_connection(
                        incoming,
                        &mut membership,
                        &mut runtime,
                        &control_in_tx,
                        &shutdown,
                        &receptionist,
                        &transport,
                        &node_registry,
                    ).await;
                }

                // ── Outbound connections (initiated by discovery) ────────
                Some(incoming) = connected_rx.recv() => {
                    handle_new_connection(
                        incoming,
                        &mut membership,
                        &mut runtime,
                        &control_in_tx,
                        &shutdown,
                        &receptionist,
                        &transport,
                        &node_registry,
                    ).await;
                }

                // ── Discovery events ────────────────────────────────────
                Some(event) = discovery_rx.recv() => {
                    match event {
                        discovery::DiscoveryEvent::PeerDiscovered(addr) => {
                            let transport = Arc::clone(&transport);
                            let connecting = Arc::clone(&connecting);
                            let mut lock = connecting.lock().await;
                            if lock.contains(&addr) {
                                continue;
                            }
                            lock.insert(addr);
                            drop(lock);

                            let connecting2 = Arc::clone(&connecting);
                            let connected_tx = connected_tx.clone();
                            tokio::spawn(async move {
                                match transport.connect(addr).await {
                                    Ok(ic) => {
                                        tracing::info!("Connected to discovered peer: {addr}");
                                        // Feed back into event loop for stream-accept handling
                                        let _ = connected_tx.send(ic);
                                    }
                                    Err(e) => {
                                        tracing::debug!("Failed to connect to {addr}: {e}");
                                    }
                                }
                                connecting2.lock().await.remove(&addr);
                            });
                        }
                    }
                }

                // ── Foca timer events ───────────────────────────────────
                Some(timer_event) = timer_rx.recv() => {
                    let _ = membership.foca.handle_timer(
                        timer_event.timer,
                        &mut runtime,
                    );
                }

                // ── Outbound SWIM data ──────────────────────────────────
                Some((target, data)) = swim_rx.recv() => {
                    let node_id = target.node_id_string();
                    let msg = ControlMessage::Swim(data);
                    if let Err(e) = transport.send_control(&node_id, msg).await {
                        tracing::trace!("Failed to send SWIM to {node_id}: {e}");
                    }
                }

                // ── Incoming control messages ───────────────────────────
                Some((node_id, msg)) = control_in_rx.recv() => {
                    match msg {
                        ControlMessage::Swim(data) => {
                            let _ = membership.foca.handle_data(
                                &data,
                                &mut runtime,
                            );
                        }
                        ControlMessage::RegistrySync(ops) => {
                            sync::apply_remote_ops(
                                ops.clone(),
                                &receptionist,
                                &type_registry,
                                &node_id,
                                &event_tx,
                                &transport,
                            );

                            // Eager connect: if ops mention nodes we're not
                            // connected to, initiate a connection immediately.
                            let connected = transport.connected_nodes().await;
                            for op in &ops {
                                if let OpType::Register { origin_addr, .. } = &op.op_type {
                                    // Skip ops from our own node
                                    if op.node_id == receptionist.node_id() {
                                        continue;
                                    }
                                    // Skip if already connected to this node
                                    if connected.contains(&op.node_id) {
                                        continue;
                                    }
                                    if let Ok(addr) = origin_addr.parse::<SocketAddr>() {
                                        let mut lock = connecting.lock().await;
                                        if lock.contains(&addr) {
                                            continue;
                                        }
                                        lock.insert(addr);
                                        drop(lock);

                                        let transport_clone = Arc::clone(&transport);
                                        let connecting_clone = Arc::clone(&connecting);
                                        let connected_tx = connected_tx.clone();
                                        tokio::spawn(async move {
                                            match transport_clone.connect(addr).await {
                                                Ok(ic) => {
                                                    tracing::info!(
                                                        "Eager connect to {addr} succeeded"
                                                    );
                                                    let _ = connected_tx.send(ic);
                                                }
                                                Err(e) => {
                                                    tracing::debug!(
                                                        "Eager connect to {addr} failed: {e}"
                                                    );
                                                }
                                            }
                                            connecting_clone.lock().await.remove(&addr);
                                        });
                                    }
                                }
                            }
                        }
                        ControlMessage::RegistrySyncRequest(peer_vv) => {
                            sync::send_sync_to_peer(
                                &transport,
                                &receptionist,
                                &node_id,
                                &peer_vv,
                            ).await;
                        }
                        ControlMessage::Ping => {
                            let _ = transport.send_control(
                                &node_id,
                                ControlMessage::Pong,
                            ).await;
                        }
                        ControlMessage::Pong => {
                            tracing::trace!("Pong from {node_id}");
                        }
                        ControlMessage::Departure(ref identity) => {
                            let departing_id = identity.node_id_string();
                            tracing::info!("Node {departing_id} departing gracefully");
                            let _ = event_tx.send(ClusterEvent::NodeLeft(identity.clone()));
                            receptionist.prune_node(&departing_id);
                            transport.remove_connection(&departing_id).await;
                            let _ = event_tx.send(ClusterEvent::NodePruned(identity.clone()));
                        }
                        ControlMessage::SpawnActor(ref request) => {
                            tracing::info!(
                                "SpawnActor request from {node_id}: label={}, type={}",
                                request.label, request.actor_type_name
                            );
                            match spawn_registry.spawn(
                                &receptionist,
                                &request.label,
                                &request.actor_type_name,
                                &request.initial_state,
                            ) {
                                Ok(()) => {
                                    tracing::info!(
                                        "Spawned {} (type: {}) successfully",
                                        request.label, request.actor_type_name
                                    );
                                    let _ = transport.send_control(
                                        &node_id,
                                        ControlMessage::SpawnAckOk {
                                            request_id: request.request_id,
                                            label: request.label.clone(),
                                        },
                                    ).await;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to spawn {}: {e}",
                                        request.label
                                    );
                                    let _ = transport.send_control(
                                        &node_id,
                                        ControlMessage::SpawnAckErr {
                                            request_id: request.request_id,
                                            error: e.to_string(),
                                        },
                                    ).await;
                                }
                            }
                        }
                        ControlMessage::SpawnAckOk { request_id, ref label } => {
                            tracing::info!(
                                "SpawnAckOk from {node_id}: request_id={request_id}, label={label}"
                            );
                            let _ = event_tx.send(ClusterEvent::SpawnAckOk {
                                request_id,
                                label: label.clone(),
                            });
                        }
                        ControlMessage::SpawnAckErr { request_id, ref error } => {
                            tracing::warn!(
                                "SpawnAckErr from {node_id}: request_id={request_id}, error={error}"
                            );
                            let _ = event_tx.send(ClusterEvent::SpawnAckErr {
                                request_id,
                                error: error.clone(),
                            });
                        }
                        ControlMessage::Handshake(_) => {
                            tracing::warn!("Unexpected handshake from {node_id}");
                        }
                    }
                }

                // ── Cluster events (NodeLeft/NodeFailed pruning) ─────
                Ok(cluster_event) = cluster_event_rx.recv() => {
                    match cluster_event {
                        ClusterEvent::NodeLeft(ref identity) | ClusterEvent::NodeFailed(ref identity) => {
                            let node_id = identity.node_id_string();
                            tracing::info!("Node departed: {node_id} — pruning actors");
                            receptionist.prune_node(&node_id);
                            transport.remove_connection(&node_id).await;
                            node_registry.remove(&node_id);
                            let _ = event_tx.send(ClusterEvent::NodePruned(identity.clone()));
                        }
                        _ => {}
                    }
                }

                // ── Connection events (disconnect handling) ────────────
                Some(event) = conn_events.recv() => {
                    if let ConnectionEvent::Disconnected(ref disconnected_node_id) = event {
                        tracing::info!(
                            "Connection lost to {disconnected_node_id}, pruning actors"
                        );
                        receptionist.prune_node(disconnected_node_id);
                    }
                }

                // ── Periodic registry sync ──────────────────────────────
                _ = sync_interval.tick() => {
                    sync::periodic_sync(&transport, &receptionist).await;
                }

                // ── Shutdown ────────────────────────────────────────────
                _ = shutdown.cancelled() => {
                    tracing::info!("ClusterSystem event loop shutting down");
                    break;
                }
            }
        }
    });
}

/// Handles a newly established connection (inbound or outbound):
/// spawns the control stream reader, announces to foca, spawns the
/// stream acceptor for subsequent bi-streams, and requests initial sync.
#[allow(clippy::too_many_arguments)]
async fn handle_new_connection(
    incoming: transport::IncomingConnection,
    membership: &mut ClusterMembership,
    runtime: &mut FocaRuntime,
    control_in_tx: &mpsc::UnboundedSender<(String, ControlMessage)>,
    shutdown: &CancellationToken,
    receptionist: &Receptionist,
    transport: &Arc<Transport>,
    node_registry: &NodeRegistry,
) {
    let node_id = incoming.remote_identity.node_id_string();
    tracing::info!("New peer connected: {node_id}");

    // Store node class and metadata from handshake
    node_registry.insert(
        &node_id,
        incoming.node_class.clone(),
        incoming.node_metadata.clone(),
    );

    // Tell foca about the new member
    let _ = membership.foca.apply_many(
        std::iter::once(foca::Member::alive(incoming.remote_identity.clone())),
        false,
        runtime,
    );

    // Spawn control stream reader — reads ongoing control messages from
    // the handshake stream (which survived read_handshake).
    tokio::spawn(transport::run_control_stream_reader(
        incoming.control_recv,
        control_in_tx.clone(),
        node_id.clone(),
        shutdown.clone(),
    ));

    // Spawn a task to accept additional bi streams from this connection.
    // New streams are either actor streams (StreamInit) or control continuations.
    let control_tx = control_in_tx.clone();
    let shutdown_clone = shutdown.clone();
    let conn = incoming.connection.clone();
    let nid = node_id.clone();
    let receptionist_for_streams = receptionist.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = conn.accept_bi() => {
                    match result {
                        Ok((send, recv)) => {
                            let receptionist_clone = receptionist_for_streams.clone();
                            let ctrl_tx = control_tx.clone();
                            let nid2 = nid.clone();
                            tokio::spawn(async move {
                                handle_incoming_stream(
                                    receptionist_clone,
                                    send,
                                    recv,
                                    ctrl_tx,
                                    nid2,
                                ).await;
                            });
                        }
                        Err(e) => {
                            tracing::debug!("Peer {nid} stopped accepting streams: {e}");
                            break;
                        }
                    }
                }
                _ = shutdown_clone.cancelled() => break,
            }
        }
    });

    // Request initial sync
    sync::request_sync_from_peer(transport, receptionist, &node_id).await;
}

// =============================================================================
// STREAM DISPATCH — determines if an incoming stream is control or actor
// =============================================================================

/// Reads the first frame from an incoming bidirectional stream to determine
/// whether it's a control message or an actor stream (StreamInit).
///
/// We use a simple heuristic: try to decode as both. StreamInit is small and
/// distinctive, while ControlMessages have different structure.
async fn handle_incoming_stream(
    receptionist: Receptionist,
    send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    control_tx: mpsc::UnboundedSender<(String, ControlMessage)>,
    node_id: String,
) {
    let mut codec = framing::FrameCodec::new();
    let mut buf = vec![0u8; 8192];

    // Read until we get the first frame
    let first_frame = loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                codec.push_data(&buf[..n]);
                if let Ok(Some(frame)) = codec.next_frame() {
                    break frame;
                }
            }
            Ok(None) | Err(_) => return,
        }
    };

    // Try StreamInit first (actor stream)
    if let Ok(init) = framing::decode_message::<framing::StreamInit>(&first_frame) {
        tracing::debug!("Incoming actor stream for: {}", init.actor_label);
        // Reconstruct the state: the first frame was StreamInit, now we need
        // to continue reading invocations. We'll create a wrapper that
        // delegates to handle_actor_stream but the StreamInit is already consumed.
        handle_actor_stream_after_init(receptionist, send, recv, codec, &init.actor_label).await;
        return;
    }

    // Otherwise, treat as control message
    if let Ok(msg) = framing::decode_message::<ControlMessage>(&first_frame) {
        let _ = control_tx.send((node_id.clone(), msg));
        // Continue reading control messages from this stream
        transport::run_control_stream_reader(recv, control_tx, node_id, CancellationToken::new())
            .await;
    } else {
        tracing::warn!("Could not decode first frame from {node_id}");
    }
}

/// Handle an actor stream where StreamInit has already been consumed.
async fn handle_actor_stream_after_init(
    receptionist: Receptionist,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    mut codec: framing::FrameCodec,
    actor_label: &str,
) {
    let dispatch_tx = match receptionist.get_dispatch_sender(actor_label) {
        Some(tx) => tx,
        None => {
            tracing::warn!("Actor stream for unknown actor: {actor_label}");
            let frame =
                framing::encode_response_frame(0, &Err(format!("actor not found: {actor_label}")));
            let _ = send.write_all(&frame).await;
            return;
        }
    };

    let mut buf = vec![0u8; 8192];

    // Process any frames already buffered in the codec from the initial read
    while let Ok(Some(frame)) = codec.next_frame() {
        if !dispatch_and_respond(&dispatch_tx, &mut send, &frame, actor_label).await {
            return;
        }
    }

    // Continue reading
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                codec.push_data(&buf[..n]);
                while let Ok(Some(frame)) = codec.next_frame() {
                    if !dispatch_and_respond(&dispatch_tx, &mut send, &frame, actor_label).await {
                        return;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("Actor stream read error for {actor_label}: {e}");
                break;
            }
        }
    }
}

/// Decode an invocation frame (lean wire format), dispatch it, await the
/// response, and write it back. Returns false if the stream should be closed.
async fn dispatch_and_respond(
    dispatch_tx: &mpsc::UnboundedSender<DispatchRequest>,
    send: &mut quinn::SendStream,
    frame: &[u8],
    actor_label: &str,
) -> bool {
    let decoded = match framing::decode_invocation_frame(frame) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!("Failed to decode invocation for {actor_label}: {e}");
            return true; // skip this frame but keep stream open
        }
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    let request = DispatchRequest {
        invocation: RemoteInvocation {
            call_id: decoded.call_id,
            actor_label: actor_label.to_string(),
            message_type: decoded.message_type.to_string(),
            payload: decoded.payload.to_vec(),
        },
        respond_to: resp_tx,
    };

    if dispatch_tx.send(request).is_err() {
        tracing::warn!("Dispatch channel closed for {actor_label}");
        return false;
    }

    match resp_rx.await {
        Ok(response) => {
            let frame = framing::encode_response_frame(response.call_id, &response.result);
            if let Err(e) = send.write_all(&frame).await {
                tracing::warn!("Failed to send response for {actor_label}: {e}");
                return false;
            }
        }
        Err(_) => {
            tracing::warn!("Response channel dropped for {actor_label}");
            // Keep stream open — actor may have been restarted
        }
    }

    true
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Actor, ActorContext, DispatchError, Handler, Message, RemoteDispatch, RemoteMessage,
        ResponseRegistry, Router, RoutingStrategy,
    };
    use config::{ClusterConfig, ClusterConfigBuilder, Discovery};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use sync::TypeRegistry;

    // ── Test actor (manual impls, no proc-macro dependency) ──────────

    #[derive(Debug)]
    struct TestCounter;

    struct TestCounterState {
        count: i64,
    }

    impl Actor for TestCounter {
        type State = TestCounterState;
    }

    // Messages

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Increment {
        amount: i64,
    }
    impl Message for Increment {
        type Result = i64;
    }
    impl RemoteMessage for Increment {
        const TYPE_ID: &'static str = "test::Increment";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct GetCount;
    impl Message for GetCount {
        type Result = i64;
    }
    impl RemoteMessage for GetCount {
        const TYPE_ID: &'static str = "test::GetCount";
    }

    // Handler impls

    impl Handler<Increment> for TestCounter {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut TestCounterState,
            msg: Increment,
        ) -> i64 {
            state.count += msg.amount;
            state.count
        }
    }

    impl Handler<GetCount> for TestCounter {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut TestCounterState,
            _msg: GetCount,
        ) -> i64 {
            state.count
        }
    }

    impl RemoteDispatch for TestCounter {
        async fn dispatch_remote<'a>(
            &'a mut self,
            ctx: &'a ActorContext<Self>,
            state: &'a mut TestCounterState,
            message_type: &'a str,
            payload: &'a [u8],
        ) -> Result<Vec<u8>, DispatchError> {
            match message_type {
                "test::Increment" => {
                    let (msg, _): (Increment, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<Increment>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                "test::GetCount" => {
                    let (msg, _): (GetCount, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<GetCount>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                other => Err(DispatchError::UnknownMessageType(other.to_string())),
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn test_type_registry() -> TypeRegistry {
        let mut reg = TypeRegistry::new();
        reg.register(
            "murmer::cluster::tests::TestCounter",
            Box::new(
                |receptionist: &crate::Receptionist,
                 label: &str,
                 wire_tx: mpsc::UnboundedSender<crate::RemoteInvocation>,
                 response_registry: ResponseRegistry,
                 node_id: &str| {
                    receptionist.register_remote_from_node::<TestCounter>(
                        label,
                        wire_tx,
                        response_registry,
                        node_id,
                    );
                },
            ),
        );
        reg
    }

    fn test_config(name: &str) -> ClusterConfig {
        ClusterConfigBuilder::new()
            .name(name)
            .listen("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .cookie("test-cookie")
            .discovery(Discovery::None)
            .build()
            .unwrap()
    }

    fn test_config_with_seed(name: &str, seed: std::net::SocketAddr) -> ClusterConfig {
        ClusterConfigBuilder::new()
            .name(name)
            .listen("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .cookie("test-cookie")
            .seed_nodes([seed])
            .build()
            .unwrap()
    }

    /// Install rustls ring crypto provider (idempotent).
    fn init_crypto() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    /// Polls a closure until it returns true, with a timeout.
    async fn poll_until<F, Fut>(timeout: Duration, mut f: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if f().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("poll_until timed out after {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_two_node_handshake() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        let config_a = test_config("node-a");

        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        // Node B uses A as a seed
        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // Wait for the connection to establish (discovery fires, connect happens)
        let transport_a = &system_a.transport;
        poll_until(Duration::from_secs(5), || async {
            !transport_a.connected_nodes().await.is_empty()
        })
        .await;

        // Both should see each other
        let a_peers = system_a.transport.connected_nodes().await;
        let b_peers = system_b.transport.connected_nodes().await;
        assert!(!a_peers.is_empty(), "A should have peers");
        assert!(!b_peers.is_empty(), "B should have peers");

        system_a.shutdown().await;
        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_end_to_end_remote_messaging() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // Start node A with a counter actor
        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        let local_ep =
            system_a.start_actor("counter/main", TestCounter, TestCounterState { count: 0 });

        // Verify local works
        let result = local_ep.send(Increment { amount: 5 }).await.unwrap();
        assert_eq!(result, 5);

        // Start node B with A as seed
        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // Wait for registry sync to propagate the actor
        poll_until(Duration::from_secs(10), || {
            let receptionist = system_b.receptionist().clone();
            async move { receptionist.lookup::<TestCounter>("counter/main").is_some() }
        })
        .await;

        // Look up the remote actor from node B and send a message
        let remote_ep = system_b
            .lookup::<TestCounter>("counter/main")
            .expect("remote actor should be discoverable");

        let result = remote_ep.send(GetCount).await.unwrap();
        assert_eq!(result, 5, "remote GetCount should return 5");

        let result = remote_ep.send(Increment { amount: 10 }).await.unwrap();
        assert_eq!(result, 15, "remote Increment should return 15");

        system_a.shutdown().await;
        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_bidirectional_messaging() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // Node A has counter/a, Node B has counter/b
        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor("counter/a", TestCounter, TestCounterState { count: 100 });

        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        system_b.start_actor("counter/b", TestCounter, TestCounterState { count: 200 });

        // Wait for both actors to be visible from both sides
        poll_until(Duration::from_secs(10), || {
            let ra = system_a.receptionist().clone();
            let rb = system_b.receptionist().clone();
            async move {
                ra.lookup::<TestCounter>("counter/b").is_some()
                    && rb.lookup::<TestCounter>("counter/a").is_some()
            }
        })
        .await;

        // Node A sends to node B's actor
        let ep_b_from_a = system_a
            .lookup::<TestCounter>("counter/b")
            .expect("B's actor visible from A");
        let result = ep_b_from_a.send(GetCount).await.unwrap();
        assert_eq!(result, 200);

        // Node B sends to node A's actor
        let ep_a_from_b = system_b
            .lookup::<TestCounter>("counter/a")
            .expect("A's actor visible from B");
        let result = ep_a_from_b.send(GetCount).await.unwrap();
        assert_eq!(result, 100);

        system_a.shutdown().await;
        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_node_failure_cleanup() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor(
            "counter/ephemeral",
            TestCounter,
            TestCounterState { count: 42 },
        );

        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // Wait for the actor to be visible from B
        poll_until(Duration::from_secs(10), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/ephemeral").is_some() }
        })
        .await;

        assert!(
            system_b
                .lookup::<TestCounter>("counter/ephemeral")
                .is_some(),
            "actor should be visible before shutdown"
        );

        // Shut down node A
        system_a.shutdown().await;
        // Give it a moment for the connection to drop
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Manually prune (in production SWIM would detect this, but in tests
        // the SWIM protocol may not trigger fast enough)
        let a_node_id = system_a.identity().node_id_string();
        system_b.receptionist().prune_node(&a_node_id);

        // The actor should be gone from B's receptionist
        assert!(
            system_b
                .lookup::<TestCounter>("counter/ephemeral")
                .is_none(),
            "actor should be pruned after node departure"
        );

        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_graceful_departure() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor(
            "counter/depart",
            TestCounter,
            TestCounterState { count: 77 },
        );

        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // Wait for the actor to be visible from B
        poll_until(Duration::from_secs(10), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/depart").is_some() }
        })
        .await;

        assert!(
            system_b.lookup::<TestCounter>("counter/depart").is_some(),
            "actor should be visible before departure"
        );

        // Graceful shutdown of node A — should broadcast Departure
        system_a.shutdown().await;

        // Node B should prune A's actors quickly (no SWIM timeout needed)
        poll_until(Duration::from_secs(5), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/depart").is_none() }
        })
        .await;

        assert!(
            system_b.lookup::<TestCounter>("counter/depart").is_none(),
            "actor should be pruned after graceful departure"
        );

        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_rejoin_after_departure() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor(
            "counter/rejoin",
            TestCounter,
            TestCounterState { count: 10 },
        );

        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_b = system_b.local_addr();

        // Wait for A's actor to be visible from B
        poll_until(Duration::from_secs(10), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/rejoin").is_some() }
        })
        .await;

        // Graceful departure of A
        system_a.shutdown().await;

        // Wait for B to prune
        poll_until(Duration::from_secs(5), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/rejoin").is_none() }
        })
        .await;

        // Now start a NEW node A and connect it to B (rejoin)
        let config_a2 = test_config_with_seed("node-a-2", addr_b);
        let system_a2 = ClusterSystem::start(config_a2, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        system_a2.start_actor(
            "counter/rejoined",
            TestCounter,
            TestCounterState { count: 20 },
        );

        // Wait for B to see the new actor from the rejoined node
        poll_until(Duration::from_secs(10), || {
            let r = system_b.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/rejoined").is_some() }
        })
        .await;

        // Verify the new actor is accessible
        let ep = system_b
            .lookup::<TestCounter>("counter/rejoined")
            .expect("rejoined actor should be discoverable");
        let count = ep.send(GetCount).await.unwrap();
        assert_eq!(count, 20, "rejoined actor should have correct state");

        system_a2.shutdown().await;
        system_b.shutdown().await;
    }

    // ── RefReceiver actor (for ActorRef-over-QUIC test) ─────────────

    #[derive(Debug)]
    struct RefReceiver;

    struct RefReceiverState;

    impl Actor for RefReceiver {
        type State = RefReceiverState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SendRef {
        label: String,
        node_id: String,
    }

    impl Message for SendRef {
        type Result = String;
    }

    impl RemoteMessage for SendRef {
        const TYPE_ID: &'static str = "test::SendRef";
    }

    impl Handler<SendRef> for RefReceiver {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut RefReceiverState,
            msg: SendRef,
        ) -> String {
            // Try to resolve the actor by looking up the label in the receptionist
            match ctx.receptionist().lookup::<TestCounter>(&msg.label) {
                Some(_) => msg.label,
                None => "not found".to_string(),
            }
        }
    }

    impl RemoteDispatch for RefReceiver {
        async fn dispatch_remote<'a>(
            &'a mut self,
            ctx: &'a ActorContext<Self>,
            state: &'a mut RefReceiverState,
            message_type: &'a str,
            payload: &'a [u8],
        ) -> Result<Vec<u8>, DispatchError> {
            match message_type {
                "test::SendRef" => {
                    let (msg, _): (SendRef, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<SendRef>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                other => Err(DispatchError::UnknownMessageType(other.to_string())),
            }
        }
    }

    /// Type registry that includes both TestCounter and RefReceiver.
    fn extended_type_registry() -> TypeRegistry {
        let mut reg = test_type_registry();
        reg.register(
            "murmer::cluster::tests::RefReceiver",
            Box::new(
                |receptionist: &crate::Receptionist,
                 label: &str,
                 wire_tx: mpsc::UnboundedSender<crate::RemoteInvocation>,
                 response_registry: ResponseRegistry,
                 node_id: &str| {
                    receptionist.register_remote_from_node::<RefReceiver>(
                        label,
                        wire_tx,
                        response_registry,
                        node_id,
                    );
                },
            ),
        );
        reg
    }

    // ── New integration tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_router_with_remote_endpoints() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // Node A: start 2 TestCounter actors
        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor("counter/r1", TestCounter, TestCounterState { count: 0 });
        system_a.start_actor("counter/r2", TestCounter, TestCounterState { count: 0 });

        // Node B: connect to A as seed
        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // Wait for both actors to appear on B via poll_until
        poll_until(Duration::from_secs(10), || {
            let r = system_b.receptionist().clone();
            async move {
                r.lookup::<TestCounter>("counter/r1").is_some()
                    && r.lookup::<TestCounter>("counter/r2").is_some()
            }
        })
        .await;

        // On B: look up both remote endpoints
        let ep1 = system_b
            .lookup::<TestCounter>("counter/r1")
            .expect("counter/r1 should be visible from B");
        let ep2 = system_b
            .lookup::<TestCounter>("counter/r2")
            .expect("counter/r2 should be visible from B");

        // Create Router with RoundRobin strategy
        let router = Router::new(vec![ep1, ep2], RoutingStrategy::RoundRobin);

        // Send 4 Increment { amount: 1 } through the router
        // RoundRobin: msg 0 → ep1, msg 1 → ep2, msg 2 → ep1, msg 3 → ep2
        for _ in 0..4 {
            router
                .send(Increment { amount: 1 })
                .await
                .expect("router send should succeed");
        }

        // Verify via GetCount: each should have count 2
        let ep1_check = system_b
            .lookup::<TestCounter>("counter/r1")
            .expect("counter/r1 still visible");
        let ep2_check = system_b
            .lookup::<TestCounter>("counter/r2")
            .expect("counter/r2 still visible");

        let count1 = ep1_check.send(GetCount).await.unwrap();
        let count2 = ep2_check.send(GetCount).await.unwrap();
        assert_eq!(count1, 2, "counter/r1 should have count 2");
        assert_eq!(count2, 2, "counter/r2 should have count 2");

        system_a.shutdown().await;
        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_actor_ref_passed_over_quic() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // Node A: start "counter/ref-target" (TestCounter, count=0)
        let config_a = test_config("node-a");
        let system_a =
            ClusterSystem::start(config_a, extended_type_registry(), SpawnRegistry::new())
                .await
                .unwrap();
        let addr_a = system_a.local_addr();
        let node_a_id = system_a.identity().node_id_string();

        system_a.start_actor(
            "counter/ref-target",
            TestCounter,
            TestCounterState { count: 0 },
        );

        // Node B: start "ref-receiver/main" (RefReceiver)
        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b =
            ClusterSystem::start(config_b, extended_type_registry(), SpawnRegistry::new())
                .await
                .unwrap();

        system_b.start_actor("ref-receiver/main", RefReceiver, RefReceiverState);

        // Wait for cross-sync (both actors visible on both nodes)
        poll_until(Duration::from_secs(10), || {
            let ra = system_a.receptionist().clone();
            let rb = system_b.receptionist().clone();
            async move {
                ra.lookup::<RefReceiver>("ref-receiver/main").is_some()
                    && rb.lookup::<TestCounter>("counter/ref-target").is_some()
            }
        })
        .await;

        // From node A: look up "ref-receiver/main" (remote endpoint on B)
        let ref_receiver_ep = system_a
            .lookup::<RefReceiver>("ref-receiver/main")
            .expect("ref-receiver/main should be visible from A");

        // Send SendRef — RefReceiver on B will try to resolve "counter/ref-target"
        // in its local receptionist (which should have it via sync)
        let result = ref_receiver_ep
            .send(SendRef {
                label: "counter/ref-target".to_string(),
                node_id: node_a_id,
            })
            .await
            .unwrap();

        assert_eq!(
            result, "counter/ref-target",
            "RefReceiver should resolve the actor ref via its receptionist"
        );

        system_a.shutdown().await;
        system_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_three_node_linear_chain() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // Topology: A <--> B <--> C (linear chain).
        // This tests that:
        // 1. B can see actors on A (direct peer)
        // 2. C can see actors on B (direct peer)
        // 3. C can discover A's actors transitively (B re-records A's ops in its oplog)
        //
        // NOTE: C's remote endpoint for A's actor is set up with B as the
        // relay node. Since B doesn't have A's actor locally, the QUIC stream
        // from C→B for that actor will fail. This is a known limitation:
        // intermediate routing is not yet implemented. We verify discovery only.

        // Node A: start "counter/on-a"
        let config_a = test_config("node-a");
        let system_a = ClusterSystem::start(config_a, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_a = system_a.local_addr();

        system_a.start_actor("counter/on-a", TestCounter, TestCounterState { count: 10 });

        // Node B: seed = A, start "counter/on-b"
        let config_b = test_config_with_seed("node-b", addr_a);
        let system_b = ClusterSystem::start(config_b, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();
        let addr_b = system_b.local_addr();

        system_b.start_actor("counter/on-b", TestCounter, TestCounterState { count: 20 });

        // Wait for A-B bidirectional sync
        poll_until(Duration::from_secs(10), || {
            let ra = system_a.receptionist().clone();
            let rb = system_b.receptionist().clone();
            async move {
                ra.lookup::<TestCounter>("counter/on-b").is_some()
                    && rb.lookup::<TestCounter>("counter/on-a").is_some()
            }
        })
        .await;

        // B can message A (direct peer)
        let ep_a_from_b = system_b
            .lookup::<TestCounter>("counter/on-a")
            .expect("A's actor visible from B");
        let count = ep_a_from_b.send(GetCount).await.unwrap();
        assert_eq!(count, 10, "B reads A's counter directly");

        // A can message B (direct peer)
        let ep_b_from_a = system_a
            .lookup::<TestCounter>("counter/on-b")
            .expect("B's actor visible from A");
        let count = ep_b_from_a.send(GetCount).await.unwrap();
        assert_eq!(count, 20, "A reads B's counter directly");

        // Node C: seed = B only (no direct connection to A)
        let config_c = test_config_with_seed("node-c", addr_b);
        let system_c = ClusterSystem::start(config_c, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        // C can see B's own actor quickly (direct peer)
        poll_until(Duration::from_secs(10), || {
            let r = system_c.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/on-b").is_some() }
        })
        .await;

        // C should be able to message B (direct peer)
        let ep_b_from_c = system_c
            .lookup::<TestCounter>("counter/on-b")
            .expect("B's actor visible from C");
        let count = ep_b_from_c.send(GetCount).await.unwrap();
        assert_eq!(count, 20, "C reads B's counter directly");

        // C discovers A's actor transitively through B (B re-records A's registration
        // in its own oplog via register_remote_from_node). This may take a sync cycle.
        poll_until(Duration::from_secs(15), || {
            let r = system_c.receptionist().clone();
            async move { r.lookup::<TestCounter>("counter/on-a").is_some() }
        })
        .await;

        // Verify transitive discovery: C's receptionist knows about A's actor
        assert!(
            system_c.lookup::<TestCounter>("counter/on-a").is_some(),
            "counter/on-a should be discovered on C via transitive sync through B"
        );

        // NOTE: Sending from C to A's actor through the remote endpoint will
        // fail because the endpoint routes through B's QUIC connection, and
        // B doesn't have "counter/on-a" as a local actor (it's a remote entry).
        // This is expected behavior — intermediate message routing (C→B→A) is
        // a future feature. For now, we verify that discovery propagates transitively.

        system_a.shutdown().await;
        system_b.shutdown().await;
        system_c.shutdown().await;
    }

    #[tokio::test]
    async fn test_concurrent_send_stress() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        init_crypto();

        // This test uses the cluster infrastructure for a single node to
        // stress-test the mailbox concurrency safety with many concurrent senders.
        let config = test_config("stress-node");
        let system = ClusterSystem::start(config, test_type_registry(), SpawnRegistry::new())
            .await
            .unwrap();

        let ep = system.start_actor("counter/stress", TestCounter, TestCounterState { count: 0 });

        // Spawn 50 tokio tasks, each sending 20 Increment { amount: 1 }
        let mut handles = Vec::new();
        for _ in 0..50 {
            let ep = ep.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..20 {
                    ep.send(Increment { amount: 1 }).await.unwrap();
                }
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Query GetCount — must equal 1000
        let count = ep.send(GetCount).await.unwrap();
        assert_eq!(
            count, 1000,
            "50 tasks * 20 increments = 1000 total increments"
        );

        system.shutdown().await;
    }
}
