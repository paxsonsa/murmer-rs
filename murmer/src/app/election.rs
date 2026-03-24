//! Leader election — determining which node runs the Coordinator.
//!
//! The [`LeaderElection`] trait is pluggable. The default implementation,
//! [`OldestNode`], picks the node with the lowest incarnation counter.
//! This is deterministic — all nodes independently compute the same answer
//! without a consensus round.
//!
//! # Custom election
//!
//! Implement [`LeaderElection`] for stronger guarantees:
//!
//! ```rust,ignore
//! struct RaftElection { /* ... */ }
//!
//! impl LeaderElection for RaftElection {
//!     fn elect(&self, view: &ClusterView) -> Option<String> {
//!         // Your Raft implementation here
//!     }
//! }
//! ```

use crate::cluster::config::NodeClass;

use crate::app::node_info::ClusterView;

// =============================================================================
// LEADER ELECTION TRAIT
// =============================================================================

/// Determines which node should be the cluster Coordinator.
///
/// The election runs whenever the cluster topology changes (node join/leave).
/// All nodes run the same election algorithm independently — if it's
/// deterministic (like [`OldestNode`]), they all agree without communication.
pub trait LeaderElection: Send + Sync {
    /// Given the current cluster view, return the node_id of the leader.
    /// Returns `None` if no eligible node exists.
    fn elect(&self, view: &ClusterView) -> Option<String>;

    /// Optional: human-readable name for logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

// =============================================================================
// OLDEST NODE — default deterministic election
// =============================================================================

/// Elects the alive node with the lowest incarnation counter.
///
/// Simple, deterministic, and convergent — all nodes independently arrive
/// at the same answer. No communication overhead.
///
/// Optionally restricted to nodes of a specific class (e.g., only
/// `NodeClass::Coordinator` nodes are eligible for election).
pub struct OldestNode {
    /// If set, only nodes of this class are eligible for leadership.
    pub required_class: Option<NodeClass>,
}

impl OldestNode {
    /// Any alive node can be leader.
    pub fn any() -> Self {
        Self {
            required_class: None,
        }
    }

    /// Only nodes of the given class can be leader.
    pub fn with_class(class: NodeClass) -> Self {
        Self {
            required_class: Some(class),
        }
    }
}

impl Default for OldestNode {
    fn default() -> Self {
        Self::any()
    }
}

impl LeaderElection for OldestNode {
    fn elect(&self, view: &ClusterView) -> Option<String> {
        view.alive_nodes()
            .filter(|node| match &self.required_class {
                Some(required) => &node.class == required,
                None => true,
            })
            .min_by_key(|node| (node.identity.incarnation, node.identity.name.clone()))
            .map(|node| node.node_id())
    }

    fn name(&self) -> &str {
        "OldestNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::node_info::NodeInfo;
    use crate::cluster::config::NodeIdentity;
    use std::collections::HashMap;

    fn make_node(name: &str, incarnation: u64, class: NodeClass) -> NodeInfo {
        NodeInfo::new(
            NodeIdentity {
                name: name.into(),
                host: "127.0.0.1".into(),
                port: 7100,
                incarnation,
            },
            class,
            HashMap::new(),
        )
    }

    #[test]
    fn test_oldest_node_picks_lowest_incarnation() {
        let mut view = ClusterView::new();
        view.upsert_node(make_node("gamma", 300, NodeClass::Worker));
        view.upsert_node(make_node("alpha", 100, NodeClass::Worker));
        view.upsert_node(make_node("beta", 200, NodeClass::Worker));

        let election = OldestNode::any();
        let leader = election.elect(&view).unwrap();
        assert!(leader.contains("alpha"), "expected alpha, got {leader}");
    }

    #[test]
    fn test_oldest_node_skips_dead() {
        let mut view = ClusterView::new();
        let oldest = make_node("alpha", 100, NodeClass::Worker);
        let oldest_id = oldest.node_id();
        view.upsert_node(oldest);
        view.upsert_node(make_node("beta", 200, NodeClass::Worker));

        view.mark_failed(&oldest_id);

        let election = OldestNode::any();
        let leader = election.elect(&view).unwrap();
        assert!(leader.contains("beta"), "expected beta, got {leader}");
    }

    #[test]
    fn test_oldest_node_with_class_filter() {
        let mut view = ClusterView::new();
        // Alpha is oldest but is a Worker
        view.upsert_node(make_node("alpha", 100, NodeClass::Worker));
        // Beta is younger but is a Coordinator
        view.upsert_node(make_node("beta", 200, NodeClass::Coordinator));

        let election = OldestNode::with_class(NodeClass::Coordinator);
        let leader = election.elect(&view).unwrap();
        assert!(leader.contains("beta"), "expected beta, got {leader}");
    }

    #[test]
    fn test_oldest_node_no_eligible_returns_none() {
        let view = ClusterView::new();
        let election = OldestNode::any();
        assert!(election.elect(&view).is_none());
    }

    #[test]
    fn test_oldest_node_all_dead_returns_none() {
        let mut view = ClusterView::new();
        let node = make_node("alpha", 100, NodeClass::Worker);
        let node_id = node.node_id();
        view.upsert_node(node);
        view.mark_failed(&node_id);

        let election = OldestNode::any();
        assert!(election.elect(&view).is_none());
    }
}
