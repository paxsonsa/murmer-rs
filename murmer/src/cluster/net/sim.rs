//! In-memory deterministic [`Net`] fabric for multi-node simulation
//! (`feature = "sim"`).
//!
//! This is the network-layer analogue of [`SimRuntime`](crate::sim::SimRuntime):
//! where `SimRuntime` makes one node's scheduler/clock/RNG deterministic,
//! [`SimFabric`]/[`SimNet`] make the *wire between nodes* deterministic — an
//! in-process bus of ordered byte channels, driven by the same `SimRuntime` the
//! nodes run on. No sockets, no wall clock, no real tasks.
//!
//! # Phase 1 scope (the two-node proof)
//!
//! Only the **actor-stream path** is wired: [`SimNet::open_actor_stream`] plus a
//! node-side accept loop ([`serve`]). There is no foca, no handshake, and no
//! `ClusterSystem` event loop — two nodes' routing is wired by hand. Crucially
//! the inbound side reuses the *real* cluster dispatch
//! ([`handle_incoming_stream`](crate::cluster::handle_incoming_stream)) over the
//! real [`FrameCodec`](crate::cluster::framing)/wire path, so a passing test
//! proves the production message path, not a sim-only mock. `SimNet` supplies
//! only the byte transport.
//!
//! `connect`/`send_control`/`broadcast_control` are deliberate stubs here; the
//! handshake + control-stream (foca) path arrives in Phase 2.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::receptionist::Receptionist;
use crate::runtime::Runtime;
use crate::sim::SimRuntime;

use super::super::error::ClusterError;
use super::super::framing::ControlMessage;
use super::{IncomingConnection, Net, NodeId, PeerAddr, RecvHalf, SendHalf};

/// A bidirectional byte stream's two halves, as the `Net` seam hands them out.
type Stream = (Box<dyn SendHalf>, Box<dyn RecvHalf>);

/// Per-node inbound-stream delivery channels, keyed by `node_id_string` route key.
type Nodes = Arc<Mutex<BTreeMap<String, mpsc::UnboundedSender<Stream>>>>;

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

// =============================================================================
// FABRIC + NET
// =============================================================================

/// One in-process bus per simulated world. Hands out a [`SimNet`] per node and
/// routes opened streams to the target node's inbound queue. Cheap to clone
/// (shares the node table).
#[derive(Clone)]
pub struct SimFabric {
    runtime: SimRuntime,
    nodes: Nodes,
}

impl SimFabric {
    /// Create a fabric driven by `runtime` (the same `SimRuntime` the nodes run
    /// on, so stream delivery is part of the one deterministic schedule).
    pub fn new(runtime: SimRuntime) -> Self {
        Self {
            runtime,
            nodes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The runtime this fabric (and its nodes) run on.
    pub fn runtime(&self) -> &SimRuntime {
        &self.runtime
    }

    /// Register a node by its `node_id_string` route `key`. Returns the node's
    /// [`SimNet`] (its `Net` impl) and the receiver of inbound streams — feed the
    /// latter to [`serve`] to run the node's inbound dispatch.
    pub fn node(&self, key: impl Into<String>) -> (SimNet, mpsc::UnboundedReceiver<Stream>) {
        let key = key.into();
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes.lock().unwrap().insert(key.clone(), tx);
        let net = SimNet {
            node_id: NodeId(key),
            nodes: Arc::clone(&self.nodes),
        };
        (net, rx)
    }
}

/// A single node's view of the fabric — its [`Net`] implementation.
pub struct SimNet {
    node_id: NodeId,
    nodes: Nodes,
}

#[async_trait]
impl Net for SimNet {
    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn local_addr(&self) -> SocketAddr {
        // Synthetic — addressing in sim collapses to the node id.
        SocketAddr::from(([0, 0, 0, 0], 0))
    }

    async fn open_actor_stream(
        &self,
        node_id: &str,
    ) -> Result<(Box<dyn SendHalf>, Box<dyn RecvHalf>), ClusterError> {
        let target = self
            .nodes
            .lock()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.to_string()))?;

        // Two ordered channels make one bidirectional stream: caller→target and
        // target→caller. We keep the caller's halves and deliver the mirrored
        // halves into the target's inbound queue (where `serve` accepts them).
        let (caller_tx, target_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (target_tx, caller_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let caller: Stream = (
            Box::new(SimSend { tx: caller_tx }),
            Box::new(SimRecv::new(caller_rx)),
        );
        let peer: Stream = (
            Box::new(SimSend { tx: target_tx }),
            Box::new(SimRecv::new(target_rx)),
        );

        target
            .send(peer)
            .map_err(|_| ClusterError::Transport(format!("node {node_id} not accepting streams")))?;
        Ok(caller)
    }

    async fn connect(&self, _addr: PeerAddr) -> Result<IncomingConnection, ClusterError> {
        Err(ClusterError::Transport(
            "SimNet::connect: handshake/foca path arrives in Phase 2".into(),
        ))
    }

    async fn send_control(&self, _node_id: &str, _msg: ControlMessage) -> Result<(), ClusterError> {
        Err(ClusterError::Transport(
            "SimNet::send_control: control-stream path arrives in Phase 2".into(),
        ))
    }

    async fn broadcast_control(&self, _msg: &ControlMessage) {}

    async fn connected_nodes(&self) -> Vec<String> {
        let me = &self.node_id.0;
        self.nodes
            .lock()
            .unwrap()
            .keys()
            .filter(|k| *k != me)
            .cloned()
            .collect()
    }

    async fn remove_connection(&self, _node_id: &str) {}
}

// =============================================================================
// ACCEPT LOOP — reuse the real inbound dispatch
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
    use crate::cluster::remote::run_actor_stream_writer;
    use crate::endpoint::Endpoint;
    use crate::prelude::*;
    use crate::sim::SimWorld;
    use crate::ResponseRegistry;
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
}
