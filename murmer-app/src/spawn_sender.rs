//! Internal channel for the Coordinator to request remote spawns.
//!
//! The [`SpawnSender`] wraps an unbounded channel. The Coordinator pushes
//! `(node_id, SpawnRequest)` pairs into it, and the bridge drain loop
//! translates them into `transport.send_control()` calls.

use murmer::cluster::framing::SpawnRequest;
use tokio::sync::mpsc;

/// A channel-based handle the Coordinator uses to request remote spawns.
///
/// Not public — created internally by [`crate::bridge::start_coordinator`].
pub(crate) struct SpawnSender {
    tx: mpsc::UnboundedSender<(String, SpawnRequest)>,
}

impl SpawnSender {
    pub fn new(tx: mpsc::UnboundedSender<(String, SpawnRequest)>) -> Self {
        Self { tx }
    }

    /// Queue a spawn request for the given target node.
    pub fn send_spawn(&self, target_node_id: &str, request: SpawnRequest) {
        if self
            .tx
            .send((target_node_id.to_string(), request))
            .is_err()
        {
            tracing::warn!("Spawn sender channel closed — spawn request dropped");
        }
    }
}
