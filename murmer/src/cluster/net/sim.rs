//! In-memory deterministic [`Net`] fabric for multi-node simulation
//! (`feature = "sim"`).
//!
//! This is the network-layer analogue of [`SimRuntime`](crate::sim::SimRuntime):
//! where `SimRuntime` makes one node's scheduler/clock/RNG deterministic,
//! [`SimFabric`]/[`SimNet`] make the *wire between nodes* deterministic — an
//! in-process bus of ordered byte channels, driven by the same `SimRuntime` the
//! nodes run on. No sockets, no wall clock, no real tasks.
//!
//! # Two construction paths
//!
//! - [`SimFabric::node`] + [`serve`] — the **Phase 1** hand-wired actor-stream
//!   path: two nodes' routes are wired by hand, no foca/handshake/event loop.
//!   Used by the two-node round-trip proof. Addressing is by a bare route key.
//!
//! - [`SimFabric::bind`] — the **Phase 2** full-node path, the sim analogue of
//!   [`Transport::bind`](crate::cluster::transport::Transport::bind). It
//!   registers a dial target and spawns an acceptor loop, returning the `Net`
//!   plus the two receivers a [`ClusterSystem`](crate::cluster::ClusterSystem)
//!   event loop consumes (handshaked inbound connections + connect/disconnect
//!   events). This is what lets a whole `ClusterSystem` — foca SWIM, registry
//!   sync, the lot — boot on the in-memory fabric.
//!
//! # The handshake is synthetic
//!
//! Production exchanges `Handshake` frames over the control stream and validates
//! cookie + cryptographic identity. Simulation is **allow-all**: the fabric
//! holds a table of each node's `NodeIdentity`, so [`SimNet::connect`] reads the
//! target's identity directly and hands the target's acceptor a pre-handshaked
//! connection. What it faithfully reproduces is everything *after* the
//! handshake: a live [`Connection`] (actor streams open/accept on it), a
//! bidirectional control stream carrying foca SWIM / registry sync, and the
//! `IncomingConnection` both event loops run `handle_new_connection` on.
//!
//! Both ends share the one `SimRuntime`, so every byte delivery is part of the
//! single deterministic schedule. The inbound side reuses the *real* cluster
//! dispatch ([`handle_incoming_stream`](crate::cluster::handle_incoming_stream))
//! over the real [`FrameCodec`](crate::cluster::framing) wire path, so a passing
//! test proves the production message path, not a sim-only mock.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::receptionist::Receptionist;
use crate::runtime::Runtime;
use crate::sim::SimRuntime;

use super::super::config::{NodeClass, NodeIdentity};
use super::super::error::ClusterError;
use super::super::framing::ControlMessage;
use super::{
    Connection, ConnectionEvent, IncomingConnection, Net, NodeId, PeerAddr, RecvHalf, SendHalf,
    run_control_stream_writer,
};

/// A bidirectional byte stream's two halves, as the `Net` seam hands them out.
type Stream = (Box<dyn SendHalf>, Box<dyn RecvHalf>);

/// Per-node inbound-stream delivery channels, keyed by `node_id_string` route
/// key. The Phase-1 hand-wired actor-stream table (see [`SimFabric::node`]).
type Nodes = Arc<Mutex<BTreeMap<String, mpsc::UnboundedSender<Stream>>>>;

/// Dial registry: each bound node's identity + acceptor channel, keyed by the
/// abstract [`NodeId`] (endpoint id) that [`PeerAddr`] dials with. `BTreeMap`
/// for deterministic iteration.
type Peers = Arc<Mutex<BTreeMap<String, SimDialTarget>>>;

// =============================================================================
// STREAM HALVES — ordered, lossless byte channels
// =============================================================================

/// Send half: each `write_all` delivers one ordered chunk to the peer. Dropping
/// it (or finishing) lets the peer's reads observe EOF once the channel drains.
struct SimSend {
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

#[async_trait]
impl SendHalf for SimSend {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), ClusterError> {
        // Whole-frame delivery; `FrameCodec` on the read side reconstructs
        // message boundaries. Ordered and lossless within a live stream.
        self.tx
            .send(buf.to_vec())
            .map_err(|_| ClusterError::Transport("sim stream: peer closed".into()))
    }

    fn finish(&mut self) -> Result<(), ClusterError> {
        // Half-close is implicit: the peer sees EOF when this sender is dropped.
        Ok(())
    }
}

/// Recv half: serves bytes from a `leftover` buffer, pulling the next chunk from
/// the channel when empty. `Ok(None)` once the sender is gone and nothing is
/// buffered (clean EOF).
struct SimRecv {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    leftover: Vec<u8>,
}

impl SimRecv {
    fn new(rx: mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx,
            leftover: Vec::new(),
        }
    }
}

#[async_trait]
impl RecvHalf for SimRecv {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ClusterError> {
        if self.leftover.is_empty() {
            match self.rx.recv().await {
                Some(chunk) => self.leftover = chunk,
                None => return Ok(None), // sender dropped → clean EOF
            }
        }
        let n = buf.len().min(self.leftover.len());
        buf[..n].copy_from_slice(&self.leftover[..n]);
        self.leftover.drain(..n);
        Ok(Some(n))
    }
}

/// Build one bidirectional byte stream as two ordered channels. Returns
/// `(near, far)` — opposite ends of the same stream: `near.write` is read by
/// `far.read`, and vice versa.
fn stream_pair() -> (Stream, Stream) {
    let (a_tx, b_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (b_tx, a_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let near: Stream = (Box::new(SimSend { tx: a_tx }), Box::new(SimRecv::new(a_rx)));
    let far: Stream = (Box::new(SimSend { tx: b_tx }), Box::new(SimRecv::new(b_rx)));
    (near, far)
}

// =============================================================================
// CONNECTION — one node's handle to an established peer connection
// =============================================================================

/// One node's handle to a live connection. Actor streams are opened on it
/// (`open_bi`) and accepted off it (`accept_bi`). The control stream is set up
/// out-of-band at connect time and travels in the [`IncomingConnection`]; this
/// handle carries only the per-actor stream plumbing.
struct SimConnection {
    remote_id: NodeId,
    /// Push the peer's halves of a newly opened stream onto the peer's accept
    /// queue — the "open a stream toward the peer" direction.
    peer_accept_tx: mpsc::UnboundedSender<Stream>,
    /// Streams the peer opened toward us, awaited by `accept_bi`.
    my_accept_rx: AsyncMutex<mpsc::UnboundedReceiver<Stream>>,
    /// Cancelled by `close`; unblocks `accept_bi` so its accept loop can exit.
    closed: CancellationToken,
}

#[async_trait]
impl Connection for SimConnection {
    fn remote_id(&self) -> NodeId {
        self.remote_id.clone()
    }

    async fn open_bi(&self) -> Result<Stream, ClusterError> {
        let (near, far) = stream_pair();
        self.peer_accept_tx
            .send(far)
            .map_err(|_| ClusterError::Connection("sim peer not accepting streams".into()))?;
        Ok(near)
    }

    async fn accept_bi(&self) -> Option<Stream> {
        let mut rx = self.my_accept_rx.lock().await;
        tokio::select! {
            s = rx.recv() => s,
            // `close` (partition/departure) unblocks the accept loop. The peer
            // dropping its `peer_accept_tx` also yields `None` via `recv`.
            _ = self.closed.cancelled() => None,
        }
    }

    fn close(&self, _code: u32, _reason: &[u8]) {
        self.closed.cancel();
    }
}

// =============================================================================
// FABRIC + NET
// =============================================================================

/// What the fabric stores per bound node so a dialer can reach it: the node's
/// identity + handshake metadata (sim is allow-all, so `connect` reads these
/// directly instead of exchanging handshake frames) and the channel onto its
/// acceptor loop.
#[derive(Clone)]
struct SimDialTarget {
    identity: NodeIdentity,
    node_class: NodeClass,
    node_metadata: HashMap<String, String>,
    is_edge_client: bool,
    raw_inbound_tx: mpsc::UnboundedSender<RawInbound>,
}

/// A pre-handshaked inbound connection handed from a dialer to the target's
/// acceptor loop. The dialer builds the symmetric stream pair and ships the
/// target's halves here; the acceptor wraps the control send half in a writer
/// task, records the connection, and forwards an [`IncomingConnection`] to the
/// event loop. This is the sim stand-in for iroh's accepted+validated stream 0.
struct RawInbound {
    /// The dialer's identity + metadata — the acceptor's "remote".
    remote_identity: NodeIdentity,
    remote_class: NodeClass,
    remote_metadata: HashMap<String, String>,
    remote_is_edge: bool,
    /// The acceptor's side of the connection (for accepting actor streams).
    connection: SimConnection,
    /// The acceptor writes control messages to the dialer through this half.
    control_send: Box<dyn SendHalf>,
    /// The acceptor reads control messages from the dialer here.
    control_recv: Box<dyn RecvHalf>,
    /// Stored in the acceptor's connection map so it can open actor streams
    /// back toward the dialer.
    peer_accept_tx: mpsc::UnboundedSender<Stream>,
}

/// A live connection from this node's point of view: where to send control
/// messages (foca SWIM, registry sync), and where to push actor streams opened
/// toward the peer.
struct SimPeerConn {
    control_tx: mpsc::UnboundedSender<ControlMessage>,
    peer_accept_tx: mpsc::UnboundedSender<Stream>,
}

/// One in-process bus per simulated world. Hands out a [`SimNet`] per node and
/// routes connections/streams between them. Cheap to clone (shares the node
/// tables).
#[derive(Clone)]
pub struct SimFabric {
    runtime: SimRuntime,
    nodes: Nodes,
    peers: Peers,
}

impl SimFabric {
    /// Create a fabric driven by `runtime` (the same `SimRuntime` the nodes run
    /// on, so stream delivery is part of the one deterministic schedule).
    pub fn new(runtime: SimRuntime) -> Self {
        Self {
            runtime,
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
            peers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The runtime this fabric (and its nodes) run on.
    pub fn runtime(&self) -> &SimRuntime {
        &self.runtime
    }

    /// **Phase 1.** Register a node by its `node_id_string` route `key` for the
    /// hand-wired actor-stream path. Returns the node's [`SimNet`] and the
    /// receiver of inbound streams — feed the latter to [`serve`] to run the
    /// node's inbound dispatch. No foca/handshake/event loop is involved; use
    /// [`bind`](Self::bind) for a full cluster node.
    pub fn node(&self, key: impl Into<String>) -> (SimNet, mpsc::UnboundedReceiver<Stream>) {
        let key = key.into();
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes.lock().unwrap().insert(key.clone(), tx);
        // Synthesize a minimal identity from the route key; the hand-wired path
        // only addresses by key and never calls connect/send_control.
        let identity = NodeIdentity::new_seeded(key.clone(), NodeId(key.clone()), "0.0.0.0", 0, 0);
        let (conn_events_tx, _conn_events_rx) = mpsc::unbounded_channel();
        let net = SimNet {
            node_id: NodeId(key),
            identity,
            node_class: NodeClass::Worker,
            node_metadata: HashMap::new(),
            nodes: Arc::clone(&self.nodes),
            peers: Arc::clone(&self.peers),
            connections: Arc::new(Mutex::new(BTreeMap::new())),
            conn_events_tx,
            runtime: self.runtime.clone(),
            shutdown: CancellationToken::new(),
        };
        (net, rx)
    }

    /// **Phase 2.** Bind a full cluster node onto the fabric — the sim analogue
    /// of [`Transport::bind`](crate::cluster::transport::Transport::bind).
    /// Registers a dial target so peers can [`connect`](Net::connect) to this
    /// node, and spawns the acceptor loop that turns inbound dials into
    /// [`IncomingConnection`]s.
    ///
    /// Returns the node's `Net` plus the two receivers the event loop consumes:
    /// handshaked inbound connections and connect/disconnect lifecycle events.
    /// Pass the result straight to
    /// [`ClusterSystem::start_with_net`](crate::cluster::ClusterSystem::start_with_net).
    /// The `shutdown` token must be the same one given to `start_with_net`.
    pub fn bind(
        &self,
        identity: NodeIdentity,
        node_class: NodeClass,
        node_metadata: HashMap<String, String>,
        shutdown: CancellationToken,
    ) -> (
        Arc<SimNet>,
        mpsc::UnboundedReceiver<IncomingConnection>,
        mpsc::UnboundedReceiver<ConnectionEvent>,
    ) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (conn_events_tx, conn_events_rx) = mpsc::unbounded_channel();
        let (raw_inbound_tx, raw_inbound_rx) = mpsc::unbounded_channel::<RawInbound>();

        // Register as a dial target keyed by the abstract NodeId (endpoint id) —
        // the key the event loop dials with via PeerAddr.
        self.peers.lock().unwrap().insert(
            identity.endpoint_id.0.clone(),
            SimDialTarget {
                identity: identity.clone(),
                node_class: node_class.clone(),
                node_metadata: node_metadata.clone(),
                is_edge_client: false,
                raw_inbound_tx,
            },
        );

        let net = Arc::new(SimNet {
            node_id: identity.endpoint_id.clone(),
            identity,
            node_class,
            node_metadata,
            nodes: Arc::clone(&self.nodes),
            peers: Arc::clone(&self.peers),
            connections: Arc::new(Mutex::new(BTreeMap::new())),
            conn_events_tx,
            runtime: self.runtime.clone(),
            shutdown,
        });

        // Spawn the acceptor loop: each inbound dial becomes an IncomingConnection.
        let accept_net = Arc::clone(&net);
        self.runtime.spawn(Box::pin(run_accept_loop(
            accept_net,
            raw_inbound_rx,
            incoming_tx,
        )));

        (net, incoming_rx, conn_events_rx)
    }
}

/// The acceptor side of the sim fabric: turn each inbound dial into an
/// [`IncomingConnection`], mirroring `Transport::handle_incoming` — minus the
/// byte handshake (sim is allow-all and reads identity from the dial directly).
async fn run_accept_loop(
    net: Arc<SimNet>,
    mut raw_inbound_rx: mpsc::UnboundedReceiver<RawInbound>,
    incoming_tx: mpsc::UnboundedSender<IncomingConnection>,
) {
    while let Some(raw) = raw_inbound_rx.recv().await {
        let RawInbound {
            remote_identity,
            remote_class,
            remote_metadata,
            remote_is_edge,
            connection,
            control_send,
            control_recv,
            peer_accept_tx,
        } = raw;
        let node_key = remote_identity.node_id_string();

        // Wrap the control send half in a writer task fed by a channel, exactly
        // as the iroh acceptor does for the surviving handshake stream.
        let (control_out_tx, control_out_rx) = mpsc::unbounded_channel();
        net.runtime.spawn(Box::pin(run_control_stream_writer(
            control_send,
            control_out_rx,
            net.shutdown.clone(),
        )));

        net.connections.lock().unwrap().insert(
            node_key.clone(),
            SimPeerConn {
                control_tx: control_out_tx.clone(),
                peer_accept_tx,
            },
        );

        let _ = net
            .conn_events_tx
            .send(ConnectionEvent::Connected(node_key.clone()));

        let ic = IncomingConnection {
            remote_identity,
            connection: Box::new(connection),
            control_tx: control_out_tx,
            control_recv,
            node_class: remote_class,
            node_metadata: remote_metadata,
            is_edge_client: remote_is_edge,
        };
        if incoming_tx.send(ic).is_err() {
            break; // event loop gone
        }
    }
}

/// A single node's view of the fabric — its [`Net`] implementation.
pub struct SimNet {
    node_id: NodeId,
    identity: NodeIdentity,
    node_class: NodeClass,
    node_metadata: HashMap<String, String>,
    /// Phase-1 hand-wired actor-stream routing (shared fabric table).
    nodes: Nodes,
    /// Phase-2 dial registry (shared fabric table).
    peers: Peers,
    /// Live connections from this node, keyed by peer `node_id_string`.
    connections: Arc<Mutex<BTreeMap<String, SimPeerConn>>>,
    conn_events_tx: mpsc::UnboundedSender<ConnectionEvent>,
    runtime: SimRuntime,
    shutdown: CancellationToken,
}

#[async_trait]
impl Net for SimNet {
    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn local_addr(&self) -> SocketAddr {
        // Synthetic but distinct per node (drives foca's MemberUp guard, which
        // compares socket addrs); the hand-wired `node` path collapses to 0.0.0.0:0.
        self.identity.socket_addr()
    }

    /// Dial a peer: allow-all + synthetic. Look the target up in the fabric by
    /// its [`NodeId`] (the key [`PeerAddr`] dials with), read its identity and
    /// metadata directly — no handshake frames — and hand its acceptor a
    /// pre-handshaked connection. Returns our own [`IncomingConnection`] for the
    /// event loop to run `handle_new_connection` on.
    async fn connect(&self, addr: PeerAddr) -> Result<IncomingConnection, ClusterError> {
        let target = self
            .peers
            .lock()
            .unwrap()
            .get(&addr.id.0)
            .cloned()
            .ok_or_else(|| ClusterError::NodeNotFound(addr.id.0.clone()))?;

        // Two accept queues, one per direction:
        //   a_to_b: streams the dialer (us) opens toward the target.
        //   b_to_a: streams the target opens toward us.
        let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded_channel::<Stream>();
        let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded_channel::<Stream>();

        let our_conn = SimConnection {
            remote_id: target.identity.endpoint_id.clone(),
            peer_accept_tx: a_to_b_tx.clone(),
            my_accept_rx: AsyncMutex::new(b_to_a_rx),
            closed: CancellationToken::new(),
        };
        let their_conn = SimConnection {
            remote_id: self.identity.endpoint_id.clone(),
            peer_accept_tx: b_to_a_tx.clone(),
            my_accept_rx: AsyncMutex::new(a_to_b_rx),
            closed: CancellationToken::new(),
        };

        // The control stream: one bidirectional byte stream split across the two
        // nodes. `near` stays with us, `far` goes to the target.
        let (near, far) = stream_pair();
        let (our_ctrl_send, our_ctrl_recv) = near;
        let (their_ctrl_send, their_ctrl_recv) = far;

        // Our control writer task (channel → frames → the byte stream). The
        // target's writer is spawned by its acceptor loop.
        let (our_ctrl_tx, our_ctrl_rx) = mpsc::unbounded_channel();
        self.runtime.spawn(Box::pin(run_control_stream_writer(
            our_ctrl_send,
            our_ctrl_rx,
            self.shutdown.clone(),
        )));

        let target_key = target.identity.node_id_string();
        self.connections.lock().unwrap().insert(
            target_key.clone(),
            SimPeerConn {
                control_tx: our_ctrl_tx.clone(),
                peer_accept_tx: a_to_b_tx,
            },
        );
        let _ = self
            .conn_events_tx
            .send(ConnectionEvent::Connected(target_key));

        // Hand the target's side to its acceptor loop.
        let raw = RawInbound {
            remote_identity: self.identity.clone(),
            remote_class: self.node_class.clone(),
            remote_metadata: self.node_metadata.clone(),
            remote_is_edge: false,
            connection: their_conn,
            control_send: their_ctrl_send,
            control_recv: their_ctrl_recv,
            peer_accept_tx: b_to_a_tx,
        };
        target
            .raw_inbound_tx
            .send(raw)
            .map_err(|_| ClusterError::Transport(format!("node {} not accepting", addr.id.0)))?;

        Ok(IncomingConnection {
            remote_identity: target.identity,
            connection: Box::new(our_conn),
            control_tx: our_ctrl_tx,
            control_recv: our_ctrl_recv,
            node_class: target.node_class,
            node_metadata: target.node_metadata,
            is_edge_client: target.is_edge_client,
        })
    }

    async fn send_control(&self, node_id: &str, msg: ControlMessage) -> Result<(), ClusterError> {
        let tx = self
            .connections
            .lock()
            .unwrap()
            .get(node_id)
            .map(|c| c.control_tx.clone())
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        tx.send(msg)
            .map_err(|_| ClusterError::Transport("sim control channel closed".into()))
    }

    async fn broadcast_control(&self, msg: &ControlMessage) {
        // Collect senders under the lock (sorted by node key — `BTreeMap` — so
        // SWIM/registry broadcast order is deterministic), then send unlocked.
        let txs: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .values()
            .map(|c| c.control_tx.clone())
            .collect();
        for tx in txs {
            let _ = tx.send(msg.clone());
        }
    }

    async fn open_actor_stream(&self, node_id: &str) -> Result<Stream, ClusterError> {
        // Phase 2: route over an established connection if one exists.
        let peer_accept_tx = self
            .connections
            .lock()
            .unwrap()
            .get(node_id)
            .map(|c| c.peer_accept_tx.clone());
        if let Some(peer_accept_tx) = peer_accept_tx {
            let (near, far) = stream_pair();
            peer_accept_tx.send(far).map_err(|_| {
                ClusterError::Transport(format!("node {node_id} not accepting streams"))
            })?;
            return Ok(near);
        }

        // Phase 1 fallback: the hand-wired fabric stream table (no foca/handshake;
        // see `node`/`serve`). Kept so the two-node Phase-1 proof still routes.
        let target = self
            .nodes
            .lock()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        let (near, far) = stream_pair();
        target
            .send(far)
            .map_err(|_| ClusterError::Transport(format!("node {node_id} not accepting streams")))?;
        Ok(near)
    }

    async fn connected_nodes(&self) -> Vec<String> {
        self.connections.lock().unwrap().keys().cloned().collect()
    }

    async fn remove_connection(&self, node_id: &str) {
        if self.connections.lock().unwrap().remove(node_id).is_some() {
            let _ = self
                .conn_events_tx
                .send(ConnectionEvent::Disconnected(node_id.to_string()));
        }
    }
}

// =============================================================================
// ACCEPT LOOP — reuse the real inbound dispatch (Phase 1 hand-wired path)
// =============================================================================

/// Drive a node's inbound side: pull each stream opened to it and run the
/// production inbound dispatch
/// ([`handle_incoming_stream`](crate::cluster::handle_incoming_stream)) for it.
///
/// This is what makes the sim faithful — node-side framing, `StreamInit`
/// parsing, and actor dispatch are the real cluster code, not reimplemented
/// here. `accept_rx` comes from [`SimFabric::node`]; `receptionist` is the
/// node's own (where the target actor is registered).
pub fn serve(
    runtime: &SimRuntime,
    receptionist: Receptionist,
    peer_key: String,
    mut accept_rx: mpsc::UnboundedReceiver<Stream>,
) {
    let rt = runtime.clone();
    runtime.spawn(Box::pin(async move {
        // Actor streams never touch the control channel; a throwaway sink
        // satisfies handle_incoming_stream's signature (a stray ControlMessage
        // would route here, but Phase 1 sends none).
        let (ctrl_tx, _ctrl_rx) = mpsc::unbounded_channel::<(String, ControlMessage)>();
        while let Some((send, recv)) = accept_rx.recv().await {
            let receptionist = receptionist.clone();
            let ctrl_tx = ctrl_tx.clone();
            let peer_key = peer_key.clone();
            rt.spawn(Box::pin(async move {
                crate::cluster::handle_incoming_stream(receptionist, send, recv, ctrl_tx, peer_key)
                    .await;
            }));
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorContext;
    use crate::cluster::config::{ClusterConfig, ClusterConfigBuilder, Discovery};
    use crate::cluster::membership::ClusterEvent;
    use crate::cluster::remote::run_actor_stream_writer;
    use crate::cluster::sync::{SpawnRegistry, TypeRegistry};
    use crate::cluster::ClusterSystem;
    use crate::endpoint::Endpoint;
    use crate::prelude::*;
    use crate::sim::SimWorld;
    use crate::ResponseRegistry;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::sync::broadcast;

    // A trivial actor that lives on node_a and is reached remotely from node_b.
    struct Counter;

    #[derive(Default)]
    struct CounterState {
        count: i64,
    }

    impl Actor for Counter {
        type State = CounterState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = i64, remote = "netsim::Increment")]
    struct Increment {
        amount: i64,
    }

    #[handlers]
    impl Counter {
        #[handler]
        fn increment(
            &self,
            _ctx: &ActorContext<Self>,
            state: &mut CounterState,
            msg: Increment,
        ) -> i64 {
            state.count += msg.amount;
            state.count
        }
    }

    /// Phase 1 proof: a message crosses from node_b to an actor on node_a over
    /// the in-memory `SimNet`, and the reply comes back — exercising the *real*
    /// wire path (`run_actor_stream_writer` → `FrameCodec` → `SimNet` byte
    /// channels → `handle_incoming_stream` → actor dispatch → reply).
    ///
    /// Two nodes share one `SimRuntime` (one `SimWorld` driver pumps both — node_b
    /// is represented by its `SimNet` + a hand-wired remote `Endpoint`; a full
    /// second `System` arrives with `SimCluster` in Phase 4). Routes are wired by
    /// hand: no foca, no handshake, no `ClusterSystem` event loop.
    ///
    /// What this asserts: (a) the cross-node reply value is correct, and (b)
    /// `block_on` completes — no deadlock-panic (so every stream end closed and
    /// every task drained). It does **not** prove determinism-under-entropy:
    /// there is no latency/drop/RNG in the Phase 1 path yet, so seed-invariance
    /// is trivial. That guarantee is Phase 3's, once faults are injected.
    fn two_node_roundtrip(seed: u64) -> i64 {
        let mut world = SimWorld::new(seed);
        let rt = world.runtime().clone();

        // node_a hosts the actor.
        let _counter = world
            .system()
            .start("counter/0", Counter, CounterState::default());
        let recept_a = world.system().receptionist().clone();

        // Fabric + per-node nets. Keys stand in for `node_id_string` route keys.
        let fabric = SimFabric::new(rt.clone());
        let (_net_a, accept_a) = fabric.node("node-a");
        let (net_b, _accept_b) = fabric.node("node-b");

        // node_a accepts inbound streams and runs the real inbound dispatch.
        serve(&rt, recept_a, "node-b".to_string(), accept_a);

        // node_b's outbound side: a remote Endpoint backed by the actor-stream
        // writer over node_b's SimNet, dialing node_a's route key.
        let (wire_tx, wire_rx) = mpsc::unbounded_channel();
        let response_registry = ResponseRegistry::new();
        let ep: Endpoint<Counter> =
            Endpoint::remote("counter/0".to_string(), wire_tx, response_registry.clone());

        let net_b: Arc<dyn Net> = Arc::new(net_b);
        let runtime: Arc<dyn Runtime> = Arc::new(rt.clone());
        rt.spawn(Box::pin(run_actor_stream_writer(
            net_b,
            runtime,
            "node-a".to_string(),
            "counter/0".to_string(),
            wire_rx,
            response_registry,
        )));

        // Drive the whole cascade to the reply.
        world.block_on(async move { ep.send(Increment { amount: 5 }).await.unwrap() })
    }

    #[test]
    fn cross_node_message_and_reply_over_sim_net() {
        assert_eq!(two_node_roundtrip(0xC0FFEE), 5);
        // Same outcome on a different seed: the Phase 1 path has no entropy, so
        // this only confirms the proof isn't accidentally seed-dependent.
        assert_eq!(two_node_roundtrip(1), 5);
    }

    // ── Phase 2.3: a whole ClusterSystem boots on the SimNet ─────────────────

    /// Install the rustls ring provider (idempotent). Only needed because the
    /// sim config carries a throwaway `iroh::SecretKey` (unused by
    /// `start_with_net` — identity is overridden below); nothing in the sim path
    /// touches the network.
    fn install_crypto() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    /// A `ClusterConfig` for a sim node: built through the normal builder, then
    /// its iroh-derived identity is replaced with the synthetic sim `identity`.
    /// Discovery is `None` (mDNS/seed are real-network paths; sim drives topology
    /// via `inject_discovered`). The leftover iroh-only fields (secret key,
    /// cookie, listen addr, allowlist) are never read by `start_with_net`.
    fn sim_config(name: &str, identity: NodeIdentity) -> ClusterConfig {
        let mut config = ClusterConfigBuilder::new()
            .name(name)
            .secret_key(iroh::SecretKey::generate())
            .listen("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .cookie("sim")
            .discovery(Discovery::None)
            .build()
            .unwrap();
        config.identity = identity;
        config
    }

    /// Boot one full `ClusterSystem` on the shared sim runtime, over a `SimNet`
    /// bound on `fabric`. Distinct ports give distinct `socket_addr`s, which
    /// foca's `MemberUp` guard requires to emit `NodeJoined`.
    fn boot(
        fabric: &SimFabric,
        rt: &Arc<dyn Runtime>,
        shutdown: &CancellationToken,
        name: &str,
        id: &str,
        port: u16,
    ) -> ClusterSystem {
        let identity = NodeIdentity::new_seeded(name, NodeId(id.into()), "127.0.0.1", port, 1);
        let (net, incoming_rx, conn_events_rx) = fabric.bind(
            identity.clone(),
            NodeClass::Worker,
            HashMap::new(),
            shutdown.clone(),
        );
        let net: Arc<dyn Net> = net;
        ClusterSystem::start_with_net(
            sim_config(name, identity),
            TypeRegistry::new(),
            SpawnRegistry::new(),
            Arc::clone(rt),
            net,
            incoming_rx,
            conn_events_rx,
            shutdown.clone(),
        )
    }

    /// Drain a node's event receiver, partitioning into the peers it saw come up
    /// and a count of any failure-detector firings.
    fn drain_events(rx: &mut broadcast::Receiver<ClusterEvent>) -> (Vec<NodeIdentity>, usize) {
        let mut joined = Vec::new();
        let mut failed = 0usize;
        loop {
            match rx.try_recv() {
                Ok(ClusterEvent::NodeJoined(id)) => joined.push(id),
                Ok(ClusterEvent::NodeFailed(_)) => failed += 1,
                Ok(_) => {}
                Err(_) => break, // Empty (or Closed/Lagged) — done draining
            }
        }
        (joined, failed)
    }

    /// A full mesh as three directed edges, one per unordered pair. Each
    /// handshake makes *both* endpoints apply the other as alive, so each edge
    /// yields two MemberUps — 6 in total, no gossip round required.
    const FULL_MESH: &[(usize, usize)] = &[(0, 1), (0, 2), (1, 2)];

    /// The full converged set: every node sees the other two come up.
    fn full_mesh_pairs() -> Vec<(String, String)> {
        [
            ("node-a-id", "node-b-id"),
            ("node-a-id", "node-c-id"),
            ("node-b-id", "node-a-id"),
            ("node-b-id", "node-c-id"),
            ("node-c-id", "node-a-id"),
            ("node-c-id", "node-b-id"),
        ]
        .iter()
        .map(|(o, j)| (o.to_string(), j.to_string()))
        .collect()
    }

    /// Boot 3 `ClusterSystem`s on one `SimRuntime`, wire `edges` (directed
    /// `(from, to)` dials), drive to quiescence, optionally `advance` the virtual
    /// clock, then return the sorted, de-duplicated set of `(observer, joined)`
    /// pairs each node's foca reported. Asserts no node ran its failure detector
    /// — on healthy sim streams, probe acks cross in the same virtual instant, so
    /// nobody is ever suspected.
    fn run_topology(
        seed: u64,
        edges: &[(usize, usize)],
        advance: Duration,
    ) -> Vec<(String, String)> {
        install_crypto();
        let mut world = SimWorld::new(seed);
        // All nodes share the world's one runtime (SimRuntime clones share the
        // inbox), so `world.pump()` drives every node's event loop.
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);
        let shutdown = CancellationToken::new();

        let spec = [
            ("node-a", "node-a-id", 7001u16),
            ("node-b", "node-b-id", 7002),
            ("node-c", "node-c-id", 7003),
        ];
        let sys: Vec<ClusterSystem> = spec
            .iter()
            .map(|(name, id, port)| boot(&fabric, &rt, &shutdown, name, id, *port))
            .collect();

        // Subscribe before injecting so no NodeJoined is missed.
        let mut ev: Vec<_> = sys.iter().map(|s| s.subscribe_events()).collect();

        for &(from, to) in edges {
            sys[from].inject_discovered(PeerAddr {
                id: sys[to].identity().endpoint_id.clone(),
                hint: vec![],
            });
        }

        world.pump();
        if advance > Duration::ZERO {
            world.advance(advance);
        }

        let mut pairs = std::collections::BTreeSet::new();
        for (idx, rx) in ev.iter_mut().enumerate() {
            let (joined, failed) = drain_events(rx);
            assert_eq!(failed, 0, "{} saw a NodeFailed on healthy streams", spec[idx].1);
            for id in joined {
                pairs.insert((spec[idx].1.to_string(), id.endpoint_id.0));
            }
        }
        pairs.into_iter().collect()
    }

    /// Phase 2.3 proof: three whole `ClusterSystem`s boot on the in-memory
    /// `SimNet`, dial a full mesh via `inject_discovered`, and converge — every
    /// node's foca SWIM reports the other two as `MemberUp` — driven entirely by
    /// `pump()` on one deterministic runtime. This is the first time the complete
    /// cluster substrate (event loop, handshake/accept, foca, control streams)
    /// runs end-to-end on the simulation fabric.
    #[test]
    fn three_node_full_mesh_member_up_converges() {
        let expected = full_mesh_pairs();

        // Convergence on pump alone: MemberUp is synchronous on apply_many (see
        // the membership linchpin test), so no clock advance is needed.
        assert_eq!(run_topology(1, FULL_MESH, Duration::ZERO), expected);

        // Determinism: the converged set is identical across seeds. We assert the
        // *set*, not per-node event ordering — the event loop's `tokio::select!`
        // picks among ready branches in an unseeded order, so ordering is not
        // reproducible, but the converged membership is, and that is what
        // convergence means.
        assert_eq!(
            run_topology(1, FULL_MESH, Duration::ZERO),
            run_topology(2, FULL_MESH, Duration::ZERO)
        );
        assert_eq!(run_topology(0xC0FFEE, FULL_MESH, Duration::ZERO), expected);
    }

    /// foca SWIM genuinely round-trips over the sim control stream — the
    /// mechanism Phase 2 is named for. The convergence test above is pump-only,
    /// so foca's probe timers never fire and *zero* SWIM frames cross (verified);
    /// every MemberUp there comes from the connect-time `apply_many`. Here we
    /// advance the virtual clock past many probe periods on a healthy full mesh:
    /// foca probes each peer, `send_control(Swim)` carries the probe, the peer's
    /// `handle_data` acks, and the ack crosses back in the same virtual instant —
    /// so membership holds at the full set with no node ever suspected. A
    /// `NodeFailed` here would be a real control-path bug, not flakiness (the only
    /// way a healthy-stream member gets suspected is if a probe or ack fails to
    /// cross).
    #[test]
    fn swim_holds_membership_under_advance() {
        assert_eq!(
            run_topology(1, FULL_MESH, Duration::from_secs(30)),
            full_mesh_pairs(),
            "membership must hold at the full set while foca probes a healthy mesh"
        );
    }
}
