//! Internal channel for the Coordinator to request remote spawns.
//!
//! The [`SpawnSender`] wraps an unbounded channel. The Coordinator pushes
//! `(node_id, SpawnRequest, enqueue_time)` tuples into it, and the bridge drain
//! loop translates them into `transport.send_control()` calls or local spawns.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::cluster::framing::SpawnRequest;
use tokio::sync::mpsc;

pub(crate) type SpawnItem = (String, SpawnRequest, Instant);

/// A channel-based handle the Coordinator uses to request remote spawns.
///
/// Not public — created internally by [`crate::app::bridge::start_coordinator`].
pub(crate) struct SpawnSender {
    tx: mpsc::UnboundedSender<SpawnItem>,
    queue_depth: Arc<AtomicUsize>,
}

impl SpawnSender {
    pub fn new(tx: mpsc::UnboundedSender<SpawnItem>, queue_depth: Arc<AtomicUsize>) -> Self {
        Self { tx, queue_depth }
    }

    /// Queue a spawn request for the given target node.
    pub fn send_spawn(&self, target_node_id: &str, request: SpawnRequest) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        if self
            .tx
            .send((target_node_id.to_string(), request, Instant::now()))
            .is_err()
        {
            self.queue_depth.fetch_sub(1, Ordering::Relaxed);
            tracing::warn!("Spawn sender channel closed — spawn request dropped");
        }
    }
}

/// A request to gracefully stop a cluster singleton instance on a node, so
/// ownership can hand off.
pub(crate) struct SingletonStopRequest {
    /// The node currently running the singleton instance to stop.
    pub target_node_id: String,
    /// The singleton label.
    pub label: String,
    /// The packed `(term, seq)` the owner was granted — echoed in the ack so a
    /// superseded owner's late stop is ignored.
    pub generation: u64,
}

/// A channel-based handle the Coordinator uses to request a singleton stop
/// (the inner half of the graceful drain handoff).
///
/// Not public — created internally by [`crate::app::bridge::start_coordinator`].
pub(crate) struct SingletonStopSender {
    tx: mpsc::UnboundedSender<SingletonStopRequest>,
}

impl SingletonStopSender {
    pub fn new(tx: mpsc::UnboundedSender<SingletonStopRequest>) -> Self {
        Self { tx }
    }

    /// Queue a stop request for the node currently running the singleton.
    pub fn send_stop(&self, request: SingletonStopRequest) {
        if self.tx.send(request).is_err() {
            tracing::warn!("Singleton stop sender channel closed — stop request dropped");
        }
    }
}
