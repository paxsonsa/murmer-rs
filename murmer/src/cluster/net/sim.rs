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
    /// The connection's liveness token. Cancelled by partition/close → writes
    /// fail fast (a severed link cannot deliver).
    severed: CancellationToken,
}

#[async_trait]
impl SendHalf for SimSend {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), ClusterError> {
        // A severed link (partition/close) cannot deliver — fail fast so the
        // control writer breaks instead of silently buffering into a dead peer.
        if self.severed.is_cancelled() {
            return Err(ClusterError::Transport("sim link severed".into()));
        }
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
    /// The connection's liveness token. Cancelled by partition/close → reads
    /// return clean EOF (a severed link looks like a dropped connection).
    severed: CancellationToken,
}

impl SimRecv {
    fn new(rx: mpsc::UnboundedReceiver<Vec<u8>>, severed: CancellationToken) -> Self {
        Self {
            rx,
            leftover: Vec::new(),
            severed,
        }
    }
}

#[async_trait]
impl RecvHalf for SimRecv {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ClusterError> {
        if self.leftover.is_empty() {
            tokio::select! {
                chunk = self.rx.recv() => match chunk {
                    Some(chunk) => self.leftover = chunk,
                    None => return Ok(None), // sender dropped → clean EOF
                },
                // Partition/close severs the link mid-read → present it as EOF,
                // the same as a dropped connection (unblocks readers awaiting bytes
                // that will never come, so advance/block_on don't hang).
                _ = self.severed.cancelled() => return Ok(None),
            }
        }
        let n = buf.len().min(self.leftover.len());
        buf[..n].copy_from_slice(&self.leftover[..n]);
        self.leftover.drain(..n);
        Ok(Some(n))
    }
}

/// Build one bidirectional byte stream as two ordered channels, all four halves
/// sharing one `severed` liveness token. Returns `(near, far)` — opposite ends
/// of the same stream: `near.write` is read by `far.read`, and vice versa.
/// Cancelling `severed` cuts both directions at once (partition/close).
fn stream_pair(severed: CancellationToken) -> (Stream, Stream) {
    let (a_tx, b_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (b_tx, a_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let near: Stream = (
        Box::new(SimSend {
            tx: a_tx,
            severed: severed.clone(),
        }),
        Box::new(SimRecv::new(a_rx, severed.clone())),
    );
    let far: Stream = (
        Box::new(SimSend {
            tx: b_tx,
            severed: severed.clone(),
        }),
        Box::new(SimRecv::new(b_rx, severed)),
    );
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
    /// The connection's liveness token, shared with every stream half and the
    /// connection-map entry. Cancelled by `close` or `partition`: it severs all
    /// streams (control + actor) at the byte level AND unblocks `accept_bi`.
    closed: CancellationToken,
}

#[async_trait]
impl Connection for SimConnection {
    fn remote_id(&self) -> NodeId {
        self.remote_id.clone()
    }

    async fn open_bi(&self) -> Result<Stream, ClusterError> {
        // Actor streams share the connection's liveness token, so a partition
        // severs them along with the control stream.
        let (near, far) = stream_pair(self.closed.clone());
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
    /// The connection's shared liveness token (so the acceptor's map entry can be
    /// severed by either side's `partition`/`close`).
    link: CancellationToken,
}

/// A live connection from this node's point of view: where to send control
/// messages (foca SWIM, registry sync), where to push actor streams opened
/// toward the peer, and the liveness token to sever the link on partition.
struct SimPeerConn {
    control_tx: mpsc::UnboundedSender<ControlMessage>,
    peer_accept_tx: mpsc::UnboundedSender<Stream>,
    severed: CancellationToken,
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
            link,
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
                severed: link,
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

        // One liveness token for the whole A↔B connection — both ends, both
        // directions, control + actor streams. Cancelling it (close/partition)
        // severs everything at once.
        let link = CancellationToken::new();

        // Two accept queues, one per direction:
        //   a_to_b: streams the dialer (us) opens toward the target.
        //   b_to_a: streams the target opens toward us.
        let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded_channel::<Stream>();
        let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded_channel::<Stream>();

        let our_conn = SimConnection {
            remote_id: target.identity.endpoint_id.clone(),
            peer_accept_tx: a_to_b_tx.clone(),
            my_accept_rx: AsyncMutex::new(b_to_a_rx),
            closed: link.clone(),
        };
        let their_conn = SimConnection {
            remote_id: self.identity.endpoint_id.clone(),
            peer_accept_tx: b_to_a_tx.clone(),
            my_accept_rx: AsyncMutex::new(a_to_b_rx),
            closed: link.clone(),
        };

        // The control stream: one bidirectional byte stream split across the two
        // nodes, sharing the connection's liveness token. `near` stays with us,
        // `far` goes to the target.
        let (near, far) = stream_pair(link.clone());
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
                severed: link.clone(),
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
            link,
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
        // Phase 2: route over an established connection if one exists. The new
        // stream shares that connection's liveness token, so a partition severs
        // it too.
        let conn = self
            .connections
            .lock()
            .unwrap()
            .get(node_id)
            .map(|c| (c.peer_accept_tx.clone(), c.severed.clone()));
        if let Some((peer_accept_tx, severed)) = conn {
            let (near, far) = stream_pair(severed);
            peer_accept_tx.send(far).map_err(|_| {
                ClusterError::Transport(format!("node {node_id} not accepting streams"))
            })?;
            return Ok(near);
        }

        // Phase 1 fallback: the hand-wired fabric stream table (no foca/handshake;
        // see `node`/`serve`). Kept so the two-node Phase-1 proof still routes.
        // No connection ⇒ no link token ⇒ a fresh never-severed one.
        let target = self
            .nodes
            .lock()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;
        let (near, far) = stream_pair(CancellationToken::new());
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

impl SimNet {
    /// Sever the link to a peer — sim-only fault injection (partition). Cancels
    /// the connection's shared liveness token, so both directions' streams fail
    /// at the byte level: writes Err, reads clean EOF. foca then probes the peer,
    /// gets no ack, and after `suspect_to_down_after` declares it down — UNLESS an
    /// indirect probe via a still-connected third node refutes the suspicion
    /// (SWIM partition tolerance). Returns `false` if there is no live connection
    /// to `peer_node_key`.
    ///
    /// The shared token means one call severs *both* directions (the peer's
    /// connection-map entry holds the same token). The connection stays in the
    /// map (severed but present) until the event loop prunes it on `NodeFailed` —
    /// mirroring a real partition, where the connection object outlives the lost
    /// link until the failure detector fires. Byte-level severing (not just
    /// map-removal) is what lets an in-flight actor `send` over a cut link fail
    /// fast instead of hanging a `block_on` forever.
    pub fn partition(&self, peer_node_key: &str) -> bool {
        match self.connections.lock().unwrap().get(peer_node_key) {
            Some(conn) => {
                conn.severed.cancel();
                true
            }
            None => false,
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
    use std::collections::BTreeSet;
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
    /// foca's `MemberUp` guard requires to emit `NodeJoined`. Incarnation 1 — use
    /// [`boot_with_incarnation`] to model a restart (a returning node bumps it).
    fn boot(
        fabric: &SimFabric,
        rt: &Arc<dyn Runtime>,
        shutdown: &CancellationToken,
        name: &str,
        id: &str,
        port: u16,
    ) -> (ClusterSystem, Arc<SimNet>) {
        boot_with_incarnation(fabric, rt, shutdown, name, id, port, 1)
    }

    /// Like [`boot`] but with an explicit incarnation. A node that crashed and
    /// returns comes back with a strictly-higher incarnation (same endpoint id),
    /// which is how foca's `win_addr_conflict` readmits it over the down instance.
    #[allow(clippy::too_many_arguments)]
    fn boot_with_incarnation(
        fabric: &SimFabric,
        rt: &Arc<dyn Runtime>,
        shutdown: &CancellationToken,
        name: &str,
        id: &str,
        port: u16,
        incarnation: u64,
    ) -> (ClusterSystem, Arc<SimNet>) {
        let identity =
            NodeIdentity::new_seeded(name, NodeId(id.into()), "127.0.0.1", port, incarnation);
        let (sim_net, incoming_rx, conn_events_rx) = fabric.bind(
            identity.clone(),
            NodeClass::Worker,
            HashMap::new(),
            shutdown.clone(),
        );
        // The concrete `Arc<SimNet>` is returned too so a test can inject
        // partitions (a sim-only fault not on the `Net` trait); the ClusterSystem
        // gets it erased behind the seam.
        let net: Arc<dyn Net> = sim_net.clone();
        let system = ClusterSystem::start_with_net(
            sim_config(name, identity),
            TypeRegistry::new(),
            SpawnRegistry::new(),
            Arc::clone(rt),
            net,
            incoming_rx,
            conn_events_rx,
            shutdown.clone(),
        );
        (system, sim_net)
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
            .map(|(name, id, port)| boot(&fabric, &rt, &shutdown, name, id, *port).0)
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

    // ── Phase 3: partition / failover / fence ────────────────────────────────

    /// Boot a converged 3-node full mesh (each node on its OWN shutdown token),
    /// crash `victim` by cancelling its token — all its tasks stop, so it goes
    /// silent without broadcasting a Departure (an abrupt crash, not a graceful
    /// leave) — then advance past foca's detection budget. Returns the
    /// `(failed, pruned)` node-id sets the *survivors* observed.
    ///
    /// Crucially the survivors' receivers are subscribed AFTER convergence, so
    /// they capture only the failure phase, and the returned sets aggregate ALL
    /// survivors — so a caller can assert not just "the victim was detected" but
    /// "exactly the victim, and no healthy node was falsely failed".
    fn crash_one(seed: u64, victim: usize) -> (BTreeSet<String>, BTreeSet<String>) {
        install_crypto();
        let mut world = SimWorld::new(seed);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);

        let spec = [
            ("node-a", "node-a-id", 7001u16),
            ("node-b", "node-b-id", 7002),
            ("node-c", "node-c-id", 7003),
        ];
        // Per-node shutdown tokens so one node can be crashed independently.
        let tokens: Vec<CancellationToken> = spec.iter().map(|_| CancellationToken::new()).collect();
        let sys: Vec<ClusterSystem> = spec
            .iter()
            .zip(&tokens)
            .map(|((name, id, port), tok)| boot(&fabric, &rt, tok, name, id, *port).0)
            .collect();

        for &(from, to) in FULL_MESH {
            sys[from].inject_discovered(PeerAddr {
                id: sys[to].identity().endpoint_id.clone(),
                hint: vec![],
            });
        }
        world.pump(); // converge — every node sees the other two up

        // Subscribe AFTER convergence so the receivers capture only the failure
        // phase (broadcast doesn't replay pre-subscribe events).
        let mut survivor_ev: Vec<_> = sys
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != victim)
            .map(|(_, s)| s.subscribe_events())
            .collect();

        // Crash the victim: its event loop, control readers/writers, accept loop,
        // and foca timer manager all stop on the cancelled token. Its
        // control-writer halves drop, landing the survivors' readers on clean EOF.
        tokens[victim].cancel();

        // Advance past foca's budget: probe_period 1.5s + suspect_to_down_after 3s
        // (~<10s worst case); 30s is comfortable for any seed's probe-target order.
        world.advance(Duration::from_secs(30));

        let mut failed = BTreeSet::new();
        let mut pruned = BTreeSet::new();
        for rx in survivor_ev.iter_mut() {
            loop {
                match rx.try_recv() {
                    Ok(ClusterEvent::NodeFailed(id)) => {
                        failed.insert(id.endpoint_id.0);
                    }
                    Ok(ClusterEvent::NodePruned(id)) => {
                        pruned.insert(id.endpoint_id.0);
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
        (failed, pruned)
    }

    /// Phase 3, step 1: SWIM failure detection under a deterministic crash. A
    /// node is killed; foca on the survivors probes it, gets no ack, suspects it,
    /// and after `suspect_to_down_after` declares it down — `MemberDown` →
    /// `NodeFailed` → the event loop prunes it (`NodePruned`).
    ///
    /// The assertion is two-sided, which is the real correctness property: the
    /// failed/pruned set is *exactly* the crashed node — the healthy survivor pair
    /// is never falsely failed (their acks cross in the same virtual instant, so
    /// neither is ever suspected). "Detect the dead AND keep the live up." The
    /// converged outcome is asserted, not the timing: foca's seeded probe-target
    /// order varies *how fast* detection happens by seed, never *whether*.
    #[test]
    fn crashed_node_is_detected_failed_and_pruned() {
        let only_a = BTreeSet::from(["node-a-id".to_string()]);
        for seed in [1u64, 2, 0xC0FFEE] {
            let (failed, pruned) = crash_one(seed, 0);
            assert_eq!(
                failed, only_a,
                "exactly the crashed node A is detected failed — no false failure of the \
                 healthy B–C pair (seed {seed})"
            );
            assert_eq!(
                pruned, only_a,
                "the cluster reacts: exactly A is pruned (seed {seed})"
            );
        }
    }

    /// Boot a converged full mesh (all nodes alive), then sever each `(from, to)`
    /// link in `cuts` with `SimNet::partition` — a byte-level cut: both
    /// directions' streams fail, but both nodes keep running. Advance past foca's
    /// detection budget and return, per node (in a/b/c order), the set of peers it
    /// declared failed. Receivers are subscribed AFTER convergence, so they
    /// capture only the post-partition phase.
    fn partition_scenario(seed: u64, cuts: &[(usize, usize)]) -> Vec<BTreeSet<String>> {
        install_crypto();
        let mut world = SimWorld::new(seed);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);
        let shutdown = CancellationToken::new();

        let spec = [
            ("node-a", "node-a-id", 7001u16),
            ("node-b", "node-b-id", 7002),
            ("node-c", "node-c-id", 7003),
        ];
        // Keep both the systems and their concrete SimNets (the latter to inject
        // partitions).
        let booted: Vec<(ClusterSystem, Arc<SimNet>)> = spec
            .iter()
            .map(|(name, id, port)| boot(&fabric, &rt, &shutdown, name, id, *port))
            .collect();

        for &(from, to) in FULL_MESH {
            booted[from].0.inject_discovered(PeerAddr {
                id: booted[to].0.identity().endpoint_id.clone(),
                hint: vec![],
            });
        }
        world.pump(); // converge

        let mut ev: Vec<_> = booted.iter().map(|(s, _)| s.subscribe_events()).collect();

        // Sever the links. One `partition` cancels the connection's shared token,
        // cutting BOTH directions, so a single call per unordered pair suffices.
        for &(from, to) in cuts {
            let peer_key = booted[to].0.identity().node_id_string();
            assert!(
                booted[from].1.partition(&peer_key),
                "partitioning a live connection {from}→{to}"
            );
        }

        // Advance past foca's budget so suspicion resolves (down, or refuted by
        // indirect probing). Same as the crash test: 30s >> ~<10s worst case.
        world.advance(Duration::from_secs(30));

        booted
            .iter()
            .zip(ev.iter_mut())
            .map(|(_, rx)| {
                let mut failed = BTreeSet::new();
                loop {
                    match rx.try_recv() {
                        Ok(ClusterEvent::NodeFailed(id)) => {
                            failed.insert(id.endpoint_id.0);
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                failed
            })
            .collect()
    }

    /// Phase 3: a single-link partition is *masked* by SWIM indirect probing —
    /// partition tolerance. Cut only A–B (A–C and B–C stay up). foca on B suspects
    /// A (its direct probe is severed), asks C to probe A on its behalf, C reaches
    /// A and relays the ack — so B never declares A down, and vice versa. The
    /// assertion is that NOBODY sees a failure: a partial cut must not partition
    /// the cluster. This also proves foca's indirect-probe machinery (PingReq →
    /// relay → ack) round-trips correctly over the sim control stream.
    ///
    /// Non-vacuity: "zero failures" alone would also hold for a no-op `partition`
    /// (cf. `swim_holds_membership_under_advance`, a healthy mesh with the same
    /// zero-failure result). What makes this meaningful is the pairing — the cut
    /// genuinely engages (A and B repeatedly hit the severed link, write_all →
    /// Err, verified) and `fully_isolated_...` below proves the *same*
    /// `partition` call has teeth (a double cut DOES produce failure). So a
    /// regression to a no-op partition would fail the isolation test, and the
    /// difference between one cut (tolerated) and two (fatal) is exactly the
    /// indirect-probe path.
    #[test]
    fn single_link_partition_is_masked_by_indirect_probing() {
        for seed in [1u64, 2, 0xC0FFEE] {
            let failed = partition_scenario(seed, &[(0, 1)]);
            for (idx, set) in failed.iter().enumerate() {
                assert!(
                    set.is_empty(),
                    "node {idx} saw a failure {set:?} — a single A–B cut must be masked by \
                     indirect probing via C (seed {seed})"
                );
            }
        }
    }

    /// Phase 3: a fully isolated node is detected by, and detects, every peer.
    /// Cut BOTH of A's links (A–B and A–C); B–C stays healthy. With no path left,
    /// indirect probing cannot refute: B and C each declare exactly A down, A
    /// declares both B and C down, and the healthy B–C pair never falsely fail
    /// each other. The mirror image of the masked case — full isolation is what
    /// actually produces failure.
    #[test]
    fn fully_isolated_node_is_detected_by_and_detects_all_peers() {
        let a = "node-a-id".to_string();
        let b = "node-b-id".to_string();
        let c = "node-c-id".to_string();
        for seed in [1u64, 2, 0xC0FFEE] {
            let failed = partition_scenario(seed, &[(0, 1), (0, 2)]);
            assert_eq!(
                failed[0],
                BTreeSet::from([b.clone(), c.clone()]),
                "isolated A detects both peers down (seed {seed})"
            );
            assert_eq!(
                failed[1],
                BTreeSet::from([a.clone()]),
                "B detects only A down — B↔C stays healthy (seed {seed})"
            );
            assert_eq!(
                failed[2],
                BTreeSet::from([a.clone()]),
                "C detects only A down — B↔C stays healthy (seed {seed})"
            );
        }
    }

    /// Phase 3, rejoin — completes the fault catalog (crash, partition, recover).
    /// A is crashed (B and C detect it failed), then "restarts" as the SAME
    /// endpoint id at incarnation 2 and re-dials the survivors. foca's
    /// `win_addr_conflict` gives the higher incarnation the win over the down
    /// instance (both share the endpoint-id `addr`), so B and C readmit A — a
    /// fresh `NodeJoined` for `node-a-id` at incarnation 2. This is the
    /// same-identity rejoin (a node returns as itself), distinct from the cluster
    /// oracle's rejoin which uses a brand-new identity.
    #[test]
    fn crashed_node_rejoins_with_higher_incarnation() {
        install_crypto();
        let mut world = SimWorld::new(1);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);

        let tok_a = CancellationToken::new();
        let tok_b = CancellationToken::new();
        let tok_c = CancellationToken::new();
        let (sys_a, _net_a) = boot(&fabric, &rt, &tok_a, "node-a", "node-a-id", 7001);
        let (sys_b, _net_b) = boot(&fabric, &rt, &tok_b, "node-b", "node-b-id", 7002);
        let (sys_c, _net_c) = boot(&fabric, &rt, &tok_c, "node-c", "node-c-id", 7003);

        let peer = |s: &ClusterSystem| PeerAddr {
            id: s.identity().endpoint_id.clone(),
            hint: vec![],
        };
        // Full mesh + converge.
        sys_a.inject_discovered(peer(&sys_b));
        sys_a.inject_discovered(peer(&sys_c));
        sys_b.inject_discovered(peer(&sys_c));
        world.pump();

        // Crash A; advance so B and C detect it failed.
        tok_a.cancel();
        world.advance(Duration::from_secs(30));

        // Watch only the readmission phase.
        let mut ev_b = sys_b.subscribe_events();
        let mut ev_c = sys_c.subscribe_events();

        // A returns: same endpoint id, incarnation 2, re-dials the survivors.
        let tok_a2 = CancellationToken::new();
        let (sys_a2, _net_a2) =
            boot_with_incarnation(&fabric, &rt, &tok_a2, "node-a", "node-a-id", 7001, 2);
        sys_a2.inject_discovered(peer(&sys_b));
        sys_a2.inject_discovered(peer(&sys_c));
        world.advance(Duration::from_secs(30));

        // B and C readmit the returned A at incarnation 2.
        let readmitted_a2 = |rx: &mut broadcast::Receiver<ClusterEvent>| {
            let mut seen = false;
            loop {
                match rx.try_recv() {
                    Ok(ClusterEvent::NodeJoined(id)) => {
                        if id.endpoint_id.0 == "node-a-id" && id.incarnation == 2 {
                            seen = true;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            seen
        };
        assert!(
            readmitted_a2(&mut ev_b),
            "B must readmit the returned A at incarnation 2 (the higher incarnation wins)"
        );
        assert!(
            readmitted_a2(&mut ev_c),
            "C must readmit the returned A at incarnation 2 (the higher incarnation wins)"
        );
        // Keep the crashed and returned systems alive through the assertions.
        let _ = (sys_a, sys_a2);
    }

    // ── The split-brain fence experiment (the Raft decision artifact) ─────────

    /// Boot a full cluster node AND its app-layer Coordinator (bridge, election,
    /// and the default in-RAM `GenerationSource`) on the sim runtime. Returns the
    /// system, its `SimNet` (to inject partitions), and the Coordinator endpoint.
    #[cfg(feature = "app")]
    #[allow(clippy::type_complexity)]
    fn boot_coordinator(
        fabric: &SimFabric,
        rt: &Arc<dyn Runtime>,
        shutdown: &CancellationToken,
        name: &str,
        id: &str,
        port: u16,
        // `None` = the default per-node in-RAM source (each Coordinator gets its
        // own ticket printer). `Some(shared)` = one source both Coordinators mint
        // from (a single ticket printer reachable from both — models a durable
        // store both sides can reach).
        source: Option<Arc<dyn crate::app::singleton::GenerationSource>>,
    ) -> (
        ClusterSystem,
        Arc<SimNet>,
        Endpoint<crate::app::coordinator::Coordinator>,
    ) {
        use crate::app::bridge;
        use crate::app::coordinator::CoordinatorState;
        use crate::app::election::OldestNode;
        use crate::app::placement::LeastLoaded;

        let (sys, net) = boot(fabric, rt, shutdown, name, id, port);
        let mut cstate = CoordinatorState::new(
            sys.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        );
        if let Some(src) = source {
            cstate = cstate.with_generation_source(src);
        }
        let coord = bridge::start_coordinator(&sys, cstate);
        (sys, net, coord)
    }

    /// THE Raft decision artifact: with the default per-node in-RAM
    /// `GenerationSource`, a network partition lets two coordinators grant the
    /// *same* fence generation to two *different* owners — a genuine split-brain
    /// the `(term, seq)` fence cannot resolve.
    ///
    /// Sequence: A (leader, oldest by name tiebreak) places `catalog` at term 1.
    /// Partition A|B and advance so foca on B detects A failed; B's bridge marks A
    /// down, so B now believes it is leader. A client re-submits `StartSingleton`
    /// to B (the spec doesn't propagate and there is no rebuild path —
    /// `GenerationSource::current()` has no callers — so re-submission is how a new
    /// leader learns a singleton). B's *own* RAM source has never seen `catalog`,
    /// so it mints term 1 again. Now A and B both own `catalog` at the identical
    /// generation: a packed-generation compare cannot order them, so both would be
    /// allowed to write.
    ///
    /// This test exercises the whole stack the simulation work built: boot on
    /// SimNet, partition + SWIM failure detection, the app bridge on the runtime
    /// seam, election, and the generation source. It asserts the *broken* current
    /// behavior on purpose — it is the concrete motivation for a consensus-minted
    /// (e.g. openraft) `GenerationSource`, under which the minority B could not
    /// commit a grant (no quorum) and this assertion would invert to safety. The
    /// singleton actor never spawns (empty `SpawnRegistry`); the generation is
    /// minted at `claim_term`, before the spawn, so the fence token is what's under
    /// test, not the actor lifecycle.
    #[cfg(feature = "app")]
    #[test]
    fn split_brain_double_grants_one_generation_to_two_owners() {
        use crate::app::coordinator::StartSingleton;
        use crate::app::singleton::{SingletonAnchor, SingletonGeneration, SingletonSpec};

        install_crypto();
        let mut world = SimWorld::new(1);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);
        let shutdown = CancellationToken::new();

        // Default sources: each Coordinator gets its OWN in-RAM ticket printer.
        let (sys_a, net_a, coord_a) =
            boot_coordinator(&fabric, &rt, &shutdown, "node-a", "node-a-id", 7001, None);
        let (sys_b, _net_b, coord_b) =
            boot_coordinator(&fabric, &rt, &shutdown, "node-b", "node-b-id", 7002, None);

        // Mesh A↔B and let the bridges learn both nodes. A is leader (equal
        // incarnations → name tiebreak, "node-a" < "node-b").
        sys_a.inject_discovered(PeerAddr {
            id: sys_b.identity().endpoint_id.clone(),
            hint: vec![],
        });
        world.pump();

        let start = |label: &str| StartSingleton {
            spec: SingletonSpec::new(label, "test::Catalog", SingletonAnchor::Leader),
        };

        // Leader A places the singleton: term 1, owned by A.
        let own_a = {
            let coord = coord_a.clone();
            let msg = start("catalog");
            world.block_on(async move { coord.send_async(msg).await.unwrap().unwrap() })
        };
        assert_eq!(own_a.generation, SingletonGeneration { term: 1, seq: 0 });
        assert_eq!(
            own_a.owner_node_id.as_deref(),
            Some(sys_a.identity().node_id_string().as_str())
        );

        // Partition A|B, then advance so foca on B detects A failed → B's bridge
        // marks A down → B believes it is the leader.
        assert!(net_a.partition(&sys_b.identity().node_id_string()));
        world.advance(Duration::from_secs(30));

        // The client re-submits to the new leader B. B's own RAM source has never
        // seen "catalog" → it mints term 1 again, for owner B.
        let own_b = {
            let coord = coord_b.clone();
            let msg = start("catalog");
            world.block_on(async move { coord.send_async(msg).await.unwrap().unwrap() })
        };

        // The split brain: identical fence token, two distinct owners.
        assert_eq!(own_b.generation, SingletonGeneration { term: 1, seq: 0 });
        assert_eq!(
            own_a.generation, own_b.generation,
            "the per-node RAM GenerationSource double-grants the SAME generation under \
             partition — the fence cannot order two owners holding equal (term, seq)"
        );
        assert_ne!(
            own_a.owner_node_id, own_b.owner_node_id,
            "two distinct owners now hold the same fence token (split brain)"
        );
    }

    /// The "shared ticket printer" experiment, the mirror of the split-brain test
    /// above: the ONLY change is that both Coordinators mint from ONE shared
    /// `GenerationSource` (a single linearization point both sides can reach —
    /// what a durable external store, e.g. appdata's catalog, gives you) instead
    /// of a per-node one. Run the identical partition + re-submit scenario.
    ///
    /// Now the clash is gone: because the shared source already recorded
    /// `catalog` at term 1 (A's grant), B's claim bumps to term 2. The fence can
    /// order them again — A's stale term-1 instance is outranked by B's term-2, so
    /// the resource rejects A's writes. This is a *handoff*, not a split brain.
    ///
    /// What it shows for the Raft decision: swapping the per-node source for a
    /// single shared one is enough to close the generation-collision hole, WITHOUT
    /// any consensus/voting. Caveat the test can't show: it assumes both sides can
    /// always reach the one source; a real durable store unreachable from the
    /// minority side would block that side from minting (safe, but unavailable) —
    /// which is the availability guarantee consensus adds on top.
    #[cfg(feature = "app")]
    #[test]
    fn one_shared_generation_source_fences_the_split_brain() {
        use crate::app::coordinator::{GetSingleton, StartSingleton};
        use crate::app::singleton::{
            CoordinatorGenerationSource, GenerationSource, SingletonAnchor, SingletonSpec,
        };

        install_crypto();
        let mut world = SimWorld::new(1);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);
        let shutdown = CancellationToken::new();

        // ONE source, shared by both Coordinators.
        let shared: Arc<dyn GenerationSource> = Arc::new(CoordinatorGenerationSource::new());
        let (sys_a, net_a, coord_a) = boot_coordinator(
            &fabric, &rt, &shutdown, "node-a", "node-a-id", 7001, Some(Arc::clone(&shared)),
        );
        let (sys_b, _net_b, coord_b) = boot_coordinator(
            &fabric, &rt, &shutdown, "node-b", "node-b-id", 7002, Some(Arc::clone(&shared)),
        );

        sys_a.inject_discovered(PeerAddr {
            id: sys_b.identity().endpoint_id.clone(),
            hint: vec![],
        });
        world.pump();

        let start = |label: &str| StartSingleton {
            spec: SingletonSpec::new(label, "test::Catalog", SingletonAnchor::Leader),
        };

        // Leader A places the singleton: term 1.
        let own_a = {
            let coord = coord_a.clone();
            let msg = start("catalog");
            world.block_on(async move { coord.send_async(msg).await.unwrap().unwrap() })
        };
        assert_eq!(own_a.generation.term, 1);

        // Partition A|B and advance so B detects A failed and becomes leader.
        assert!(net_a.partition(&sys_b.identity().node_id_string()));
        world.advance(Duration::from_secs(30));

        // With the shared backend, B does NOT need a client re-submit: it rebuilds
        // `catalog`'s spec from the shared source and adopts it on itself at a
        // strictly higher term. (A, still alive on its side of the partition,
        // keeps its term-1 instance — but it is now fenced out by B's term 2, so
        // the two never write concurrently. Contrast the per-node test above,
        // where B's separate source re-grants term 1 and the fence collides.)
        let own_b = {
            let coord = coord_b.clone();
            world.block_on(async move {
                coord
                    .send(GetSingleton {
                        label: "catalog".into(),
                    })
                    .await
                    .unwrap()
            })
        };
        let own_b = own_b.expect("B adopts catalog from the shared backend under partition");
        assert_eq!(
            own_b.owner_node_id.as_deref(),
            Some(sys_b.identity().node_id_string().as_str()),
            "B owns catalog on its side"
        );
        assert_eq!(own_b.generation.term, 2, "the shared source bumps B's term");
        assert!(
            own_b.generation > own_a.generation,
            "B's grant strictly outranks A's — the (term, seq) fence rejects the stale owner A, \
             so the two never write concurrently"
        );
    }

    /// The SECOND, independent weakness — "amnesia" — which neither ticket-source
    /// fix above touches. A singleton's spec lives ONLY in the Coordinator that
    /// placed it; it is never replicated, and there is no rebuild path
    /// (`GenerationSource::current()` has no callers). So when the *leader itself*
    /// is lost (not just a worker owner), the new leader does not know the
    /// singleton exists and silently fails to take it over.
    ///
    /// Setup: anchor = Leader, so the leader A is also the owner. A places
    /// `catalog`, then A is crashed. B detects A's failure and becomes leader —
    /// but B's singleton map is empty, so its failover loop does nothing. The
    /// singleton is orphaned: it runs nowhere, with no error raised.
    ///
    /// (Contrast: `test_singleton_fails_over_on_owner_departure_with_higher_term`
    /// in the cluster-tests oracle works precisely because there the Coordinator
    /// is a *separate* surviving node that still holds the spec — failover only
    /// works while the Coordinator outlives the owner.)
    #[cfg(feature = "app")]
    #[test]
    fn singleton_is_orphaned_when_the_leader_is_lost() {
        use crate::app::coordinator::{GetSingleton, StartSingleton};
        use crate::app::singleton::{SingletonAnchor, SingletonSpec};

        install_crypto();
        let mut world = SimWorld::new(1);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);

        // Per-node shutdown tokens so A can be crashed independently.
        let tok_a = CancellationToken::new();
        let tok_b = CancellationToken::new();
        let (sys_a, _net_a, coord_a) =
            boot_coordinator(&fabric, &rt, &tok_a, "node-a", "node-a-id", 7001, None);
        let (sys_b, _net_b, coord_b) =
            boot_coordinator(&fabric, &rt, &tok_b, "node-b", "node-b-id", 7002, None);

        sys_a.inject_discovered(PeerAddr {
            id: sys_b.identity().endpoint_id.clone(),
            hint: vec![],
        });
        world.pump();

        // Leader A places (and owns, via the Leader anchor) the singleton.
        let own_a = {
            let coord = coord_a.clone();
            let msg = StartSingleton {
                spec: SingletonSpec::new("catalog", "test::Catalog", SingletonAnchor::Leader),
            };
            world.block_on(async move { coord.send_async(msg).await.unwrap().unwrap() })
        };
        assert_eq!(
            own_a.owner_node_id.as_deref(),
            Some(sys_a.identity().node_id_string().as_str()),
            "A (the leader) owns the singleton"
        );

        // Crash the leader A. B detects the failure and becomes leader.
        tok_a.cancel();
        world.advance(Duration::from_secs(30));

        // The amnesia: B is now leader, the singleton is Leader-anchored (so it
        // *should* run on B), but B never knew the spec and there is no rebuild —
        // so B holds nothing. The singleton is silently orphaned.
        let on_b = {
            let coord = coord_b.clone();
            world.block_on(async move {
                coord
                    .send(GetSingleton {
                        label: "catalog".into(),
                    })
                    .await
                    .unwrap()
            })
        };
        assert!(
            on_b.is_none(),
            "amnesia: the new leader B never learned `catalog`, so it is orphaned (got {on_b:?})"
        );
    }

    /// The amnesia FIX, the mirror of the orphan test above: with a single shared
    /// backend (the durable-store path), the new leader rebuilds the singleton set
    /// from the backend's persisted specs and adopts the singleton instead of
    /// orphaning it — at a strictly higher term, so it's also fenced.
    ///
    /// Same scenario: A (leader, owner) places `catalog`, A is crashed, B becomes
    /// leader. The only change from the orphan test is the shared source. Now B's
    /// `load_singletons_from_backend` (run before the re-drive) reads `catalog`'s
    /// spec, and the re-drive re-places it on B at term 2. This closes both holes
    /// at once with the durable backend: the fence (higher term) AND the amnesia
    /// (spec rebuild).
    #[cfg(feature = "app")]
    #[test]
    fn shared_backend_lets_the_new_leader_adopt_the_singleton() {
        use crate::app::coordinator::{GetSingleton, StartSingleton};
        use crate::app::singleton::{
            CoordinatorGenerationSource, GenerationSource, SingletonAnchor, SingletonSpec,
        };

        install_crypto();
        let mut world = SimWorld::new(1);
        let sim_rt = world.runtime().clone();
        let rt: Arc<dyn Runtime> = Arc::new(sim_rt.clone());
        let fabric = SimFabric::new(sim_rt);

        // ONE shared backend both coordinators mint from and rebuild from.
        let shared: Arc<dyn GenerationSource> = Arc::new(CoordinatorGenerationSource::new());

        let tok_a = CancellationToken::new();
        let tok_b = CancellationToken::new();
        let (sys_a, _net_a, coord_a) = boot_coordinator(
            &fabric, &rt, &tok_a, "node-a", "node-a-id", 7001, Some(Arc::clone(&shared)),
        );
        let (sys_b, _net_b, coord_b) = boot_coordinator(
            &fabric, &rt, &tok_b, "node-b", "node-b-id", 7002, Some(Arc::clone(&shared)),
        );

        sys_a.inject_discovered(PeerAddr {
            id: sys_b.identity().endpoint_id.clone(),
            hint: vec![],
        });
        world.pump();

        // Leader A places (and owns) `catalog` at term 1.
        let own_a = {
            let coord = coord_a.clone();
            let msg = StartSingleton {
                spec: SingletonSpec::new("catalog", "test::Catalog", SingletonAnchor::Leader),
            };
            world.block_on(async move { coord.send_async(msg).await.unwrap().unwrap() })
        };
        assert_eq!(own_a.generation.term, 1);

        // Crash the leader A. B detects the failure and becomes leader.
        tok_a.cancel();
        world.advance(Duration::from_secs(30));

        // B rebuilt `catalog` from the shared backend and re-placed it on itself —
        // owner B, strictly-higher term. No orphan.
        let on_b = {
            let coord = coord_b.clone();
            world.block_on(async move {
                coord
                    .send(GetSingleton {
                        label: "catalog".into(),
                    })
                    .await
                    .unwrap()
            })
        };
        let on_b =
            on_b.expect("new leader adopts catalog from the shared backend (not orphaned)");
        assert_eq!(
            on_b.owner_node_id.as_deref(),
            Some(sys_b.identity().node_id_string().as_str()),
            "B now owns catalog"
        );
        assert_eq!(on_b.generation.term, 2, "adopted at a bumped term");
        assert!(
            on_b.generation > own_a.generation,
            "the adoption strictly outranks A's grant — the fence holds too"
        );
    }
}
