//! The `Net` seam — abstract inter-node transport for deterministic multi-node
//! simulation.
//!
//! This is the network-layer analogue of the [`Runtime`](crate::runtime::Runtime)
//! seam: it abstracts the one transport primitive murmer actually uses — a
//! reliable, ordered, closeable bidirectional byte stream keyed by an abstract
//! node id — behind a trait, so the cluster can run either over real iroh/QUIC
//! (production) or over an in-memory deterministic fabric (simulation).
//!
//! **Status: foundation only.** These trait/type definitions are landed
//! additively and are not yet wired into [`ClusterSystem`](super::ClusterSystem)
//! — today's cluster code still uses [`super::transport::Transport`] directly.
//! The wiring (Phase 0: make `IrohTransport` implement `Net`, switch
//! `ClusterSystem` to `Arc<dyn Net>`, box the iroh stream types where they leak)
//! is the next focused chunk, done against the `murmer-cluster-tests` suite as
//! the oracle. See `.llm/shared/context/2026-06-21-net-seam-blueprint.md` and
//! `.llm/shared/plans/2026-06-21-net-seam-execution.md`.
//!
//! # Why one primitive
//!
//! murmer has no datagram path: foca's SWIM bytes are tunneled as
//! [`ControlMessage::Swim`](super::framing::ControlMessage) over the same
//! reliable control stream as the handshake, registry sync, and spawn commands.
//! So drop/latency/partition are injected at the message-and-connection layer
//! (close/fail whole streams, delay whole frames), not a packet layer — and
//! SWIM's failure detector fires naturally when the reliable channel underneath
//! it is cut.

use std::collections::HashMap;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::config::{NodeClass, NodeIdentity};
use super::error::ClusterError;
use super::framing::ControlMessage;

/// Abstract, transport-neutral node identity.
///
/// Replaces the authenticated peer identity that iroh exposes as `EndpointId`
/// (the thing handshake validation compares against `NodeIdentity`'s id). The
/// iroh impl wraps `iroh::EndpointId`; the sim impl wraps a synthetic, stable
/// per-node id rendered as a string.
///
/// Note: the cluster's pool/route key stays the `node_id_string`
/// ("`endpoint_id#incarnation`") — `NodeId` is only the identity type that flows
/// through discovery, handshake, and foca.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub String);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Abstract dial target. Replaces `iroh::EndpointAddr` (id + socket hint). In
/// simulation the socket hint is meaningless and ignored; addressing collapses
/// to the id.
#[derive(Clone, Debug)]
pub struct PeerAddr {
    pub id: NodeId,
    /// Best-effort dial hint (host:port). `None` / ignored in sim.
    pub hint: Option<SocketAddr>,
}

/// Connection lifecycle event delivered on the `Net`'s event channel.
#[derive(Clone, Debug)]
pub enum ConnectionEvent {
    /// A peer's connection became live (carries `node_id_string`).
    Connected(String),
    /// A peer's connection dropped (carries `node_id_string`).
    Disconnected(String),
}

/// A handshaked, live connection delivered into the cluster event loop — the
/// transport-neutral version of [`super::transport::IncomingConnection`].
pub struct IncomingConnection {
    pub remote_identity: NodeIdentity,
    pub connection: Box<dyn Connection>,
    pub control_tx: mpsc::UnboundedSender<ControlMessage>,
    /// The recv half of the handshake stream — still live, ready for the event
    /// loop to read ongoing control messages from.
    pub control_recv: Box<dyn RecvHalf>,
    pub node_class: NodeClass,
    pub node_metadata: HashMap<String, String>,
    pub is_edge_client: bool,
}

/// The endpoint owner: bind/connect, per-peer control channel, per-actor streams.
///
/// One-to-one with the public surface of [`super::transport::Transport`]. `bind`
/// / `connect_only` are impl-specific constructors (not trait methods) because
/// each impl needs different arguments (iroh: secret key, cookie, tuning,
/// allowlist; sim: a node id and a fabric handle) — they return the `Net` plus
/// its `incoming` and `connection_events` receivers.
#[async_trait]
pub trait Net: Send + Sync + 'static {
    /// This node's authenticated id.
    fn node_id(&self) -> NodeId;

    /// The bound listen address (after port-0 resolution). Synthetic in sim.
    fn local_addr(&self) -> SocketAddr;

    /// Dial and handshake a peer. `addr` abstracts `EndpointAddr`; in sim only
    /// `addr.id` matters.
    async fn connect(&self, addr: PeerAddr) -> Result<IncomingConnection, ClusterError>;

    /// Send a control message to a peer over its reliable control channel
    /// (foca SWIM, registry sync, pings, spawn commands all ride here).
    async fn send_control(&self, node_id: &str, msg: ControlMessage) -> Result<(), ClusterError>;

    /// Broadcast a control message to every connected peer.
    async fn broadcast_control(&self, msg: &ControlMessage);

    /// Open a fresh per-actor byte stream to a peer (remote dispatch rides here).
    async fn open_actor_stream(
        &self,
        node_id: &str,
    ) -> Result<(Box<dyn SendHalf>, Box<dyn RecvHalf>), ClusterError>;

    /// The set of currently-connected peer `node_id_string`s.
    async fn connected_nodes(&self) -> Vec<String>;

    /// Tear down the connection to a peer.
    async fn remove_connection(&self, node_id: &str);
}

/// A live connection to one peer. Streams are opened/accepted on it.
#[async_trait]
pub trait Connection: Send + Sync {
    /// The authenticated peer id.
    fn remote_id(&self) -> NodeId;

    /// Open a fresh outbound bidirectional stream.
    async fn open_bi(&self) -> Result<(Box<dyn SendHalf>, Box<dyn RecvHalf>), ClusterError>;

    /// Accept the next inbound bidirectional stream. `None` => connection closed
    /// (drives the accept loop's exit).
    async fn accept_bi(&self) -> Option<(Box<dyn SendHalf>, Box<dyn RecvHalf>)>;

    /// Close with a semantic code: 0 = duplicate/departed/stale, 1 = cookie
    /// mismatch, 2 = protocol mismatch, 4 = identity mismatch.
    fn close(&self, code: u32, reason: &[u8]);
}

/// The send half of a bidirectional byte stream. `FrameCodec` sits on top, so
/// writes are whole framed chunks of ordered bytes.
#[async_trait]
pub trait SendHalf: Send {
    /// Write a whole frame of bytes.
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), ClusterError>;

    /// Half-close (finish) the send direction.
    fn finish(&mut self) -> Result<(), ClusterError>;
}

/// The recv half of a bidirectional byte stream. Raw ordered bytes — no message
/// boundaries (the codec reconstructs frames).
#[async_trait]
pub trait RecvHalf: Send {
    /// Read up to `buf.len()` bytes. `Ok(None)` = clean EOF / peer-close,
    /// `Err` = stream error. **Invariant:** bytes within a live stream are never
    /// reordered or lost; drop/partition fail the whole stream instead.
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ClusterError>;
}
