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
//!
//! # Network latency fault injection
//!
//! [`SimFabric::set_latency`] installs a [`LatencyConfig`] that delays each
//! chunk's delivery by `base + rand(0..=jitter)` of virtual time. The model is
//! faithful to QUIC's reliable ordered streams:
//!
//! - **FIFO within a stream** is preserved by a per-`SimSend` monotonic delivery
//!   clock: `deliver_at = max(last_delivery, now) + sample_delay()`. A fast burst
//!   of writes can never overtake one another — each chunk's delivery instant is
//!   at least as late as the previous chunk's.
//!
//! - **Cross-stream reorder** emerges naturally from differing per-stream delays,
//!   exactly as QUIC allows independent streams to interleave. No explicit
//!   reordering is added.
//!
//! - **In-flight drops on partition** are handled inside the delayed-delivery
//!   task: it checks `severed.is_cancelled()` before sending the chunk into the
//!   channel, so a byte that was already in-flight when `partition` fires is
//!   silently discarded instead of arriving after the link is gone.
//!
//! - **Default is off** (`None`): delivery is immediate and byte-for-byte
//!   identical to the old behavior, so all existing tests are unaffected.
//!
//! The shared `Arc<Mutex<ChaCha8Rng>>` seeded from
//! `runtime.derive_seed("net-faults")` is the single deterministic draw stream
//! for all links: every delay sample comes from one RNG in a fixed draw order
//! (the `write_all` call order the sim's task scheduler produces), so the whole
//! fault schedule is reproducible from the root seed.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
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

// =============================================================================
// LATENCY CONFIG — seeded, shared delay sampler for network fault injection
// =============================================================================

/// Per-fabric latency configuration: a seeded RNG shared across all links so
/// the draw order is the single deterministic fault schedule. `None` everywhere
/// means no latency — delivery is immediate, byte-for-byte the old behavior.
///
/// Cheap to clone (the RNG is behind `Arc<Mutex<...>>`). Install via
/// [`SimFabric::set_latency`]; the fabric clones it into every [`SimNet`] and
/// [`SimConnection`] at bind/connect time.
#[derive(Clone)]
struct LatencyConfig {
    runtime: SimRuntime,
    base: Duration,
    jitter: Duration,
    /// Single draw stream for ALL links — seeded from
    /// `runtime.derive_seed("net-faults")` so the fault schedule descends from
    /// the root seed without consuming from the actor RNG stream.
    rng: Arc<Mutex<ChaCha8Rng>>,
}

impl LatencyConfig {
    /// Draw one delay sample: `base + uniform(0..=jitter)`.
    /// If jitter is zero the result is exactly `base` (no RNG draw needed).
    fn sample_delay(&self) -> Duration {
        if self.jitter.is_zero() {
            return self.base;
        }
        let jitter_ns = self.jitter.as_nanos() as u64;
        let n = self.rng.lock().unwrap().next_u64() % (jitter_ns + 1);
        self.base + Duration::from_nanos(n)
    }
}

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
///
/// When `latency` is `Some`, delivery is deferred to virtual time
/// `deliver_at = max(last_delivery, now) + sample_delay()`. The monotonic
/// `last_delivery` clock is the FIFO invariant: it prevents a shorter delay on
/// chunk N+1 from overtaking chunk N that was written just before it. Cross-
/// stream reorder emerges naturally from independent per-stream delays; no
/// explicit reordering is added.
struct SimSend {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    /// The connection's liveness token. Cancelled by partition/close → writes
    /// fail fast (a severed link cannot deliver).
    severed: CancellationToken,
    /// Latency config, or `None` for immediate (no-latency) delivery.
    latency: Option<LatencyConfig>,
    /// Monotonic per-stream delivery clock (FIFO guard). Tracks when the
    /// previously-scheduled chunk will arrive; the next chunk may not land before
    /// it. Starts at zero (no history → the first chunk is not artificially
    /// delayed beyond its own sample).
    last_delivery: Duration,
}

#[async_trait]
impl SendHalf for SimSend {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), ClusterError> {
        // A severed link (partition/close) cannot deliver — fail fast so the
        // control writer breaks instead of silently buffering into a dead peer.
        if self.severed.is_cancelled() {
            return Err(ClusterError::Transport("sim link severed".into()));
        }

        if let Some(cfg) = &self.latency {
            // Latency path: schedule chunk delivery as a background task that
            // sleeps for `sleep_for` virtual time then puts the chunk into the
            // receive channel — but ONLY if the link is still live at wakeup.
            //
            // FIFO invariant: `deliver_at = max(last_delivery, now) + delay`
            // ensures this chunk cannot arrive before the previous one, even if
            // its random delay is smaller. This faithfully models QUIC's ordered
            // reliable streams.
            //
            // Severed-drops-in-flight invariant: checking `severed` again inside
            // the spawned task means a chunk whose delivery was scheduled before a
            // `partition` call is discarded on wakeup instead of arriving after the
            // link is logically gone — a byte in-flight when the wire is cut does
            // not survive the cut.
            let now = cfg.runtime.now();
            let delay = cfg.sample_delay();
            let deliver_at = self.last_delivery.max(now) + delay;
            self.last_delivery = deliver_at;
            let sleep_for = deliver_at.saturating_sub(now);

            let tx = self.tx.clone();
            let severed = self.severed.clone();
            let rt = cfg.runtime.clone();
            let chunk = buf.to_vec();
            cfg.runtime.spawn(Box::pin(async move {
                rt.sleep(sleep_for).await;
                // Drop the chunk if the link was severed while it was in-flight.
                if !severed.is_cancelled() {
                    let _ = tx.send(chunk);
                }
            }));
            Ok(())
        } else {
            // No-latency path: immediate delivery, byte-for-byte the original
            // behavior. All existing tests run through here (latency is `None`
            // by default), so they are unaffected.
            self.tx
                .send(buf.to_vec())
                .map_err(|_| ClusterError::Transport("sim stream: peer closed".into()))
        }
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
                // Deterministic branch priority (see supervisor.rs): if a buffered
                // chunk and a same-step sever are both ready, deliver the chunk
                // before observing EOF — so a delivery racing a partition replays
                // identically instead of letting select!'s RNG pick per run.
                biased;
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
///
/// `latency` is cloned into both send halves. When `None`, delivery is
/// immediate (original behavior). When `Some`, each direction's send half gets
/// its own `last_delivery` clock starting at zero, so the two directions are
/// independent — FIFO is per-direction, matching QUIC's stream semantics.
fn stream_pair(severed: CancellationToken, latency: Option<LatencyConfig>) -> (Stream, Stream) {
    let (a_tx, b_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (b_tx, a_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let near: Stream = (
        Box::new(SimSend {
            tx: a_tx,
            severed: severed.clone(),
            latency: latency.clone(),
            last_delivery: Duration::ZERO,
        }),
        Box::new(SimRecv::new(a_rx, severed.clone())),
    );
    let far: Stream = (
        Box::new(SimSend {
            tx: b_tx,
            severed: severed.clone(),
            latency,
            last_delivery: Duration::ZERO,
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
    /// Latency config for actor streams opened on this connection, or `None`
    /// for immediate delivery. Cloned from the fabric at connect time.
    latency: Option<LatencyConfig>,
}

#[async_trait]
impl Connection for SimConnection {
    fn remote_id(&self) -> NodeId {
        self.remote_id.clone()
    }

    async fn open_bi(&self) -> Result<Stream, ClusterError> {
        // Actor streams share the connection's liveness token, so a partition
        // severs them along with the control stream. Latency (if any) is
        // carried into each new stream's send halves.
        let (near, far) = stream_pair(self.closed.clone(), self.latency.clone());
        self.peer_accept_tx
            .send(far)
            .map_err(|_| ClusterError::Connection("sim peer not accepting streams".into()))?;
        Ok(near)
    }

    async fn accept_bi(&self) -> Option<Stream> {
        let mut rx = self.my_accept_rx.lock().await;
        tokio::select! {
            // Deterministic branch priority (see supervisor.rs): accept a ready
            // stream before a same-step close, so the order replays identically.
            biased;
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
    /// Latency config applied to all new streams. `None` (the default) means
    /// immediate delivery — no latency, no change from the original behavior.
    latency: Option<LatencyConfig>,
}

impl SimFabric {
    /// Create a fabric driven by `runtime` (the same `SimRuntime` the nodes run
    /// on, so stream delivery is part of the one deterministic schedule).
    /// Starts with no latency: all delivery is immediate.
    pub fn new(runtime: SimRuntime) -> Self {
        Self {
            runtime,
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
            peers: Arc::new(Mutex::new(BTreeMap::new())),
            latency: None,
        }
    }

    /// The runtime this fabric (and its nodes) run on.
    pub fn runtime(&self) -> &SimRuntime {
        &self.runtime
    }

    /// Install a latency model on this fabric. Every stream created after this
    /// call will delay each chunk by `base + rand(0..=jitter)` of virtual time.
    ///
    /// This is faithful to QUIC's reliable ordered streams:
    /// - Per-stream FIFO is preserved by a monotonic delivery clock in each
    ///   [`SimSend`] — a burst of writes cannot reorder within the same stream.
    /// - Cross-stream reorder emerges from differing per-stream delays: that is
    ///   the faithful "reorder" QUIC allows across independent streams.
    /// - A chunk in-flight when `partition` fires is discarded (the delivery
    ///   task checks the liveness token at wakeup).
    ///
    /// Call this BEFORE booting nodes via [`bind`](Self::bind) or
    /// [`node`](Self::node) — bound [`SimNet`]s copy the config at construction.
    /// The shared RNG is seeded from `derive_seed("net-faults")` so the fault
    /// schedule is reproducible from the root seed and never consumes from the
    /// actor RNG stream.
    pub fn set_latency(&mut self, base: Duration, jitter: Duration) {
        let seed = self.runtime.derive_seed("net-faults");
        self.latency = Some(LatencyConfig {
            runtime: self.runtime.clone(),
            base,
            jitter,
            rng: Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(seed))),
        });
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
            latency: self.latency.clone(),
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
            latency: self.latency.clone(),
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
    /// Latency config cloned from the fabric at node construction, or `None`
    /// for immediate delivery. Propagated into connections and streams.
    latency: Option<LatencyConfig>,
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
            latency: self.latency.clone(),
        };
        let their_conn = SimConnection {
            remote_id: self.identity.endpoint_id.clone(),
            peer_accept_tx: b_to_a_tx.clone(),
            my_accept_rx: AsyncMutex::new(a_to_b_rx),
            closed: link.clone(),
            latency: self.latency.clone(),
        };

        // The control stream: one bidirectional byte stream split across the two
        // nodes, sharing the connection's liveness token. `near` stays with us,
        // `far` goes to the target. Latency applies to control frames too —
        // SWIM probes and acks are just bytes on the control stream.
        let (near, far) = stream_pair(link.clone(), self.latency.clone());
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
        // it too. Latency (if configured) applies to the new stream's send halves.
        let conn = self
            .connections
            .lock()
            .unwrap()
            .get(node_id)
            .map(|c| (c.peer_accept_tx.clone(), c.severed.clone()));
        if let Some((peer_accept_tx, severed)) = conn {
            let (near, far) = stream_pair(severed, self.latency.clone());
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
        let (near, far) = stream_pair(CancellationToken::new(), self.latency.clone());
        target.send(far).map_err(|_| {
            ClusterError::Transport(format!("node {node_id} not accepting streams"))
        })?;
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
    use crate::ResponseRegistry;
    use crate::actor::ActorContext;
    use crate::cluster::remote::run_actor_stream_writer;
    use crate::endpoint::Endpoint;
    use crate::prelude::*;
    use crate::sim::SimWorld;
    use serde::{Deserialize, Serialize};

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

    // ── latency fault injection (stream_pair / SimSend) ───────────────────────

    /// A [`SimSend`] with latency must preserve chunk arrival order within the
    /// same stream (the FIFO invariant). We write two chunks in order and confirm
    /// the receive channel produces them in the same order, even though each chunk
    /// gets an independent random delay. The monotonic `last_delivery` clock is
    /// what enforces this: the second chunk's `deliver_at` is at least as large as
    /// the first chunk's, so it cannot arrive earlier.
    ///
    /// This test is self-contained: it drives `stream_pair` directly through a
    /// `SimRuntime` / `SimWorld` rather than going through the full cluster stack,
    /// so it isolates the send-half invariant without the handshake or foca
    /// overhead.
    #[test]
    fn latency_in_stream_fifo_order_preserved() {
        use crate::sim::SimWorld;

        let mut world = SimWorld::new(0xABCD);
        let rt = world.runtime().clone();

        // A latency config with noticeable jitter so the two delay samples are
        // unlikely to be equal (which would still pass FIFO but hide a bug).
        let cfg = LatencyConfig {
            runtime: rt.clone(),
            base: Duration::from_millis(10),
            jitter: Duration::from_millis(40),
            rng: Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(
                rt.derive_seed("fifo-test"),
            ))),
        };

        let severed = CancellationToken::new();
        let (near, far) = stream_pair(severed, Some(cfg));
        let (mut send, _) = near;
        let (_, mut recv) = far;

        // Write two chunks. Due to latency, delivery is deferred; each write
        // spawns a background task that delivers at `deliver_at`.
        world.block_on(async move {
            send.write_all(&[1, 2, 3]).await.unwrap();
            send.write_all(&[4, 5, 6]).await.unwrap();
        });

        // Advance time past the maximum possible delivery (base + jitter = 50ms).
        // Both delivery tasks fire, in their scheduled order.
        world.advance(Duration::from_millis(100));

        // Read both chunks and confirm FIFO: [1,2,3] arrives before [4,5,6].
        // Use Arc<Mutex> to share `got` with the 'static async block.
        let got: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let got2 = got.clone();
        world.block_on(async move {
            let mut buf = [0u8; 8];
            // First chunk.
            let n = recv.read(&mut buf).await.unwrap().expect("first chunk");
            got2.lock().unwrap().extend_from_slice(&buf[..n]);
            // Second chunk.
            let n = recv.read(&mut buf).await.unwrap().expect("second chunk");
            got2.lock().unwrap().extend_from_slice(&buf[..n]);
        });
        let got = got.lock().unwrap();
        assert_eq!(&got[..3], &[1, 2, 3], "first chunk arrived first (FIFO)");
        assert_eq!(&got[3..6], &[4, 5, 6], "second chunk arrived second (FIFO)");
    }

    /// A chunk that is in-flight (delivery task spawned but not yet woken) when
    /// the link is severed via `partition` must NOT be delivered. This validates
    /// the "severed-drops-in-flight" invariant: the delivery task checks
    /// `severed.is_cancelled()` before putting the chunk into the channel.
    #[test]
    fn latency_in_flight_chunk_dropped_on_sever() {
        use crate::sim::SimWorld;

        let mut world = SimWorld::new(0xBEEF);
        let rt = world.runtime().clone();

        // Long latency so the delivery task is definitely still sleeping when we
        // cancel.
        let cfg = LatencyConfig {
            runtime: rt.clone(),
            base: Duration::from_millis(200),
            jitter: Duration::ZERO,
            rng: Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(
                rt.derive_seed("inflight-test"),
            ))),
        };

        let severed = CancellationToken::new();
        let (near, far) = stream_pair(severed.clone(), Some(cfg));
        let (mut send, _) = near;
        let (_, mut recv) = far;

        // Write the chunk → delivery scheduled 200ms from now.
        world.block_on(async move {
            send.write_all(&[9, 8, 7]).await.unwrap();
        });

        // Sever the link BEFORE advancing time past the delivery point.
        severed.cancel();

        // Advance past the delivery point. The delivery task wakes, sees
        // `severed.is_cancelled()` == true, and discards the chunk.
        world.advance(Duration::from_millis(300));

        // The recv's peer send half was dropped when `near` was destructured and
        // `send` was moved into block_on. After block_on returns, send is gone,
        // so the recv channel is closed → the next read should return EOF (None).
        // A Some(n) here would mean the in-flight chunk was wrongly delivered.
        let result: Arc<Mutex<Option<Option<usize>>>> = Arc::new(Mutex::new(None));
        let result2 = result.clone();
        world.block_on(async move {
            let mut buf = [0u8; 8];
            let r = recv.read(&mut buf).await.unwrap();
            *result2.lock().unwrap() = Some(r);
        });
        match *result.lock().unwrap() {
            Some(None) => {} // EOF: channel closed with no chunk — correct
            Some(Some(n)) => {
                panic!("in-flight chunk should have been dropped on sever, but got {n} bytes")
            }
            None => panic!("block_on did not complete"),
        }
    }
}
