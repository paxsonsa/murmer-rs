//! Structured monitoring events emitted via the `tracing` crate.
//!
//! Each function emits a tracing event at the appropriate level with
//! structured fields. This lets you capture cluster lifecycle events
//! in whatever format your tracing subscriber supports (JSON logs,
//! OpenTelemetry spans, Prometheus metrics, etc.).

/// Emit a tracing event when a node joins the cluster.
pub fn trace_node_joined(node_id: &str, name: &str, class: &str) {
    tracing::info!(
        target: "murmer::cluster",
        event = "node_joined",
        node_id,
        node_name = name,
        node_class = class,
        "Node joined the cluster"
    );
}

/// Emit a tracing event when a node fails.
pub fn trace_node_failed(node_id: &str) {
    tracing::warn!(
        target: "murmer::cluster",
        event = "node_failed",
        node_id,
        "Node failure detected"
    );
}

/// Emit a tracing event when a node leaves gracefully.
pub fn trace_node_left(node_id: &str) {
    tracing::info!(
        target: "murmer::cluster",
        event = "node_left",
        node_id,
        "Node left gracefully"
    );
}

/// Emit a tracing event when an actor is placed on a node.
pub fn trace_actor_placed(label: &str, node_id: &str, reason: &str) {
    tracing::info!(
        target: "murmer::actor",
        event = "actor_placed",
        actor_label = label,
        node_id,
        reason,
        "Actor placed on node"
    );
}

/// Emit a tracing event when an actor is displaced due to node failure.
pub fn trace_actor_displaced(label: &str, node_id: &str, strategy: &str) {
    tracing::warn!(
        target: "murmer::actor",
        event = "actor_displaced",
        actor_label = label,
        node_id,
        crash_strategy = strategy,
        "Actor displaced by node departure"
    );
}

/// Emit a tracing event when an actor is redistributed to a new node.
pub fn trace_actor_redistributed(label: &str, from_node: &str, to_node: &str) {
    tracing::info!(
        target: "murmer::actor",
        event = "actor_redistributed",
        actor_label = label,
        from_node,
        to_node,
        "Actor redistributed to new node"
    );
}

/// Emit a tracing event for a cluster health snapshot.
pub fn trace_cluster_health(alive: usize, total: usize, actors: usize) {
    tracing::info!(
        target: "murmer::cluster",
        event = "health_snapshot",
        alive_nodes = alive,
        total_nodes = total,
        total_actors = actors,
        "Cluster health snapshot"
    );
}
