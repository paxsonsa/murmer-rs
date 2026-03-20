//! Cluster view and node information.
//!
//! The [`ClusterView`] is the Coordinator's world model — a snapshot of all
//! nodes in the cluster with their capabilities and running actors. The
//! Coordinator maintains this by subscribing to `ClusterEvent`s and
//! `ActorEvent`s from the receptionist.
//!
//! [`NodeInfo`] describes a single node: its identity, class, metadata,
//! and what actors it's currently running.

use std::collections::HashMap;

use murmer::cluster::config::{NodeClass, NodeIdentity};

// =============================================================================
// NODE INFO
// =============================================================================

/// Information about a single node in the cluster.
///
/// The Coordinator maintains one `NodeInfo` per known node, updating it
/// as nodes join, fail, and actors are spawned or removed.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// The node's cluster identity (name, host, port, incarnation).
    pub identity: NodeIdentity,
    /// The node's declared class (from handshake).
    pub class: NodeClass,
    /// The node's declared metadata (from handshake).
    pub metadata: HashMap<String, String>,
    /// Labels of actors currently running on this node.
    pub running_actors: Vec<String>,
    /// Whether this node is currently reachable.
    pub is_alive: bool,
}

impl NodeInfo {
    /// Create a new NodeInfo for a node that just joined.
    pub fn new(identity: NodeIdentity, class: NodeClass, metadata: HashMap<String, String>) -> Self {
        Self {
            identity,
            class,
            metadata,
            running_actors: Vec::new(),
            is_alive: true,
        }
    }

    /// The node's unique ID string (includes incarnation).
    pub fn node_id(&self) -> String {
        self.identity.node_id_string()
    }

    /// Number of actors running on this node.
    pub fn actor_count(&self) -> usize {
        self.running_actors.len()
    }
}

// =============================================================================
// CLUSTER VIEW
// =============================================================================

/// A snapshot of the entire cluster's state, as seen by the Coordinator.
///
/// Provides methods for querying nodes by capability, finding eligible
/// placement targets, and tracking actor locations.
#[derive(Debug, Clone, Default)]
pub struct ClusterView {
    /// All known nodes, keyed by node_id_string().
    pub nodes: HashMap<String, NodeInfo>,
}

impl ClusterView {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a node in the view.
    pub fn upsert_node(&mut self, info: NodeInfo) {
        self.nodes.insert(info.node_id(), info);
    }

    /// Mark a node as failed (not alive). Does not remove it — the node
    /// might rejoin (relevant for `WaitForReturn` crash strategy).
    pub fn mark_failed(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.is_alive = false;
        }
    }

    /// Mark a node as alive again (rejoined after failure).
    pub fn mark_alive(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.is_alive = true;
        }
    }

    /// Remove a node from the view entirely.
    pub fn remove_node(&mut self, node_id: &str) -> Option<NodeInfo> {
        self.nodes.remove(node_id)
    }

    /// Record that an actor has been placed on a node.
    pub fn add_actor(&mut self, node_id: &str, label: &str) {
        if let Some(node) = self.nodes.get_mut(node_id)
            && !node.running_actors.contains(&label.to_string())
        {
            node.running_actors.push(label.to_string());
        }
    }

    /// Record that an actor has been removed from a node.
    pub fn remove_actor(&mut self, node_id: &str, label: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.running_actors.retain(|l| l != label);
        }
    }

    /// Remove an actor from whatever node it's on. Returns the node_id if found.
    pub fn remove_actor_anywhere(&mut self, label: &str) -> Option<String> {
        for (node_id, node) in &mut self.nodes {
            if let Some(pos) = node.running_actors.iter().position(|l| l == label) {
                node.running_actors.remove(pos);
                return Some(node_id.clone());
            }
        }
        None
    }

    /// Find which node is running a given actor label.
    pub fn find_actor(&self, label: &str) -> Option<&str> {
        for (node_id, node) in &self.nodes {
            if node.running_actors.iter().any(|l| l == label) {
                return Some(node_id);
            }
        }
        None
    }

    /// All nodes that are currently alive.
    pub fn alive_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values().filter(|n| n.is_alive)
    }

    /// Number of alive nodes.
    pub fn alive_count(&self) -> usize {
        self.alive_nodes().count()
    }

    /// Total number of tracked nodes (alive and failed).
    pub fn total_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(name: &str, incarnation: u64) -> NodeIdentity {
        NodeIdentity {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: 7100,
            incarnation,
        }
    }

    #[test]
    fn test_cluster_view_upsert_and_query() {
        let mut view = ClusterView::new();
        let node = NodeInfo::new(
            test_identity("alpha", 1),
            NodeClass::Worker,
            HashMap::new(),
        );
        let node_id = node.node_id();
        view.upsert_node(node);

        assert_eq!(view.alive_count(), 1);
        assert_eq!(view.total_count(), 1);
        assert!(view.nodes.get(&node_id).unwrap().is_alive);
    }

    #[test]
    fn test_cluster_view_mark_failed() {
        let mut view = ClusterView::new();
        let node = NodeInfo::new(
            test_identity("alpha", 1),
            NodeClass::Worker,
            HashMap::new(),
        );
        let node_id = node.node_id();
        view.upsert_node(node);

        view.mark_failed(&node_id);
        assert_eq!(view.alive_count(), 0);
        assert_eq!(view.total_count(), 1);
        assert!(!view.nodes.get(&node_id).unwrap().is_alive);
    }

    #[test]
    fn test_cluster_view_actor_tracking() {
        let mut view = ClusterView::new();
        let node = NodeInfo::new(
            test_identity("alpha", 1),
            NodeClass::Worker,
            HashMap::new(),
        );
        let node_id = node.node_id();
        view.upsert_node(node);

        view.add_actor(&node_id, "worker/0");
        view.add_actor(&node_id, "worker/1");

        assert_eq!(view.find_actor("worker/0"), Some(node_id.as_str()));
        assert_eq!(view.nodes.get(&node_id).unwrap().actor_count(), 2);

        view.remove_actor(&node_id, "worker/0");
        assert_eq!(view.find_actor("worker/0"), None);
        assert_eq!(view.nodes.get(&node_id).unwrap().actor_count(), 1);
    }

    #[test]
    fn test_cluster_view_remove_actor_anywhere() {
        let mut view = ClusterView::new();
        let n1 = NodeInfo::new(test_identity("alpha", 1), NodeClass::Worker, HashMap::new());
        let n2 = NodeInfo::new(test_identity("beta", 2), NodeClass::Worker, HashMap::new());
        let n1_id = n1.node_id();
        let n2_id = n2.node_id();
        view.upsert_node(n1);
        view.upsert_node(n2);

        view.add_actor(&n2_id, "worker/0");

        let removed_from = view.remove_actor_anywhere("worker/0");
        assert_eq!(removed_from.as_deref(), Some(n2_id.as_str()));
        assert_eq!(view.find_actor("worker/0"), None);

        // Not found case
        let _ = n1_id; // suppress unused warning
        assert_eq!(view.remove_actor_anywhere("nonexistent"), None);
    }
}
