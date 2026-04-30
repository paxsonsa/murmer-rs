//! Internal channel for the Coordinator to request remote spawns.
//!
//! The [`SpawnSender`] wraps an unbounded channel. The Coordinator pushes
//! `(node_id, SpawnRequest, enqueue_time)` tuples into it, and the bridge drain
//! loop translates them into `transport.send_control()` calls or local spawns.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
        if self.tx.send((target_node_id.to_string(), request, Instant::now())).is_err() {
            self.queue_depth.fetch_sub(1, Ordering::Relaxed);
            tracing::warn!("Spawn sender channel closed — spawn request dropped");
        }
    }
}
