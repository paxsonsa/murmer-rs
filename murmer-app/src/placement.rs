//! Placement strategies — deciding where to run actors.
//!
//! The [`PlacementStrategy`] trait defines a fitness function that scores
//! nodes for hosting a given actor spec. The Coordinator evaluates all
//! eligible nodes and picks the one with the highest fitness score.
//!
//! # Built-in strategies
//!
//! | Strategy | Behavior |
//! |----------|----------|
//! | [`LeastLoaded`] | Fewest running actors wins |
//! | [`Random`] | Uniform random selection |
//! | [`Pinned`] | Always prefer a specific node |
//!
//! # Custom strategies
//!
//! Implement [`PlacementStrategy`] to encode domain-specific placement logic:
//!
//! ```rust,ignore
//! struct GpuPreference;
//!
//! impl PlacementStrategy for GpuPreference {
//!     fn fitness(&self, node: &NodeInfo, spec: &ActorSpec, view: &ClusterView) -> f64 {
//!         let base = if node.metadata.get("gpu") == Some(&"true".to_string()) {
//!             100.0
//!         } else {
//!             1.0
//!         };
//!         // Penalize heavily loaded nodes
//!         base / (node.actor_count() as f64 + 1.0)
//!     }
//! }
//! ```

use crate::node_info::{ClusterView, NodeInfo};
use crate::spec::ActorSpec;

// =============================================================================
// PLACEMENT DECISION
// =============================================================================

/// The result of a placement decision — which node to place an actor on and why.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PlacementDecision {
    /// The chosen node's ID.
    pub node_id: String,
    /// Human-readable reason for the decision (for logging/debugging).
    pub reason: String,
}

// =============================================================================
// PLACEMENT STRATEGY TRAIT
// =============================================================================

/// Scores nodes for hosting a given actor spec. Higher score = better fit.
///
/// The Coordinator calls `fitness()` for each eligible node (after filtering
/// through `PlacementConstraints`) and picks the highest-scoring one.
///
/// # Contract
///
/// - Return `0.0` or negative to indicate "do not place here"
/// - Higher values indicate stronger preference
/// - The function receives the full `ClusterView` for global-aware decisions
///   (e.g., spreading actors across failure domains)
pub trait PlacementStrategy: Send + Sync {
    /// Score a node for hosting this spec. Higher = better.
    fn fitness(&self, node: &NodeInfo, spec: &ActorSpec, view: &ClusterView) -> f64;

    /// Optional: human-readable name for logging.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// Select the best node for an actor spec from a cluster view.
///
/// Filters nodes through placement constraints, scores survivors with the
/// strategy's fitness function, and returns the winner.
pub fn select_node(
    strategy: &dyn PlacementStrategy,
    spec: &ActorSpec,
    view: &ClusterView,
) -> Option<PlacementDecision> {
    let mut best: Option<(String, f64)> = None;

    for node in view.alive_nodes() {
        // Filter: must satisfy placement constraints
        if !spec
            .placement
            .is_satisfied_by(&node.class, &node.metadata, &node.running_actors)
        {
            continue;
        }

        let score = strategy.fitness(node, spec, view);
        if score <= 0.0 {
            continue;
        }

        match &best {
            Some((_, best_score)) if score <= *best_score => {}
            _ => {
                best = Some((node.node_id(), score));
            }
        }
    }

    best.map(|(node_id, score)| PlacementDecision {
        reason: format!(
            "strategy={}, score={score:.2}, node={node_id}",
            strategy.name()
        ),
        node_id,
    })
}

// =============================================================================
// BUILT-IN STRATEGIES
// =============================================================================

/// Place actors on the node with the fewest running actors.
///
/// Ties are broken by whichever node is encountered first in HashMap
/// iteration order, which is non-deterministic across process restarts.
pub struct LeastLoaded;

impl PlacementStrategy for LeastLoaded {
    fn fitness(&self, node: &NodeInfo, _spec: &ActorSpec, _view: &ClusterView) -> f64 {
        // Invert actor count: fewer actors = higher fitness
        1.0 / (node.actor_count() as f64 + 1.0)
    }

    fn name(&self) -> &str {
        "LeastLoaded"
    }
}

/// Place actors randomly across eligible nodes.
///
/// All eligible nodes get equal fitness, so the winner depends on
/// floating-point comparison of random values — effectively uniform random.
pub struct RandomPlacement;

impl PlacementStrategy for RandomPlacement {
    fn fitness(&self, _node: &NodeInfo, _spec: &ActorSpec, _view: &ClusterView) -> f64 {
        use rand::Rng;
        rand::rng().random::<f64>()
    }

    fn name(&self) -> &str {
        "Random"
    }
}

/// Always prefer a specific node. Falls back to any eligible node if the
/// pinned node is unavailable.
pub struct Pinned {
    /// The preferred node's ID.
    pub preferred_node_id: String,
}

impl PlacementStrategy for Pinned {
    fn fitness(&self, node: &NodeInfo, _spec: &ActorSpec, _view: &ClusterView) -> f64 {
        if node.node_id() == self.preferred_node_id {
            1000.0 // Strong preference
        } else {
            1.0 // Fallback: any eligible node
        }
    }

    fn name(&self) -> &str {
        "Pinned"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmer::cluster::config::{NodeClass, NodeIdentity};
    use std::collections::HashMap;

    fn make_node(name: &str, incarnation: u64, actors: Vec<String>) -> NodeInfo {
        NodeInfo {
            identity: NodeIdentity {
                name: name.into(),
                host: "127.0.0.1".into(),
                port: 7100,
                incarnation,
            },
            class: NodeClass::Worker,
            metadata: HashMap::new(),
            running_actors: actors,
            is_alive: true,
        }
    }

    fn make_view(nodes: Vec<NodeInfo>) -> ClusterView {
        let mut view = ClusterView::new();
        for node in nodes {
            view.upsert_node(node);
        }
        view
    }

    #[test]
    fn test_least_loaded_prefers_empty_node() {
        let view = make_view(vec![
            make_node("alpha", 1, vec!["w/0".into(), "w/1".into()]),
            make_node("beta", 2, vec![]),
        ]);

        let spec = ActorSpec::new("w/2", "app::Worker");
        let decision = select_node(&LeastLoaded, &spec, &view).unwrap();

        // Beta has 0 actors, alpha has 2 — beta should win
        assert!(
            decision.node_id.contains("beta"),
            "expected beta, got {}",
            decision.node_id
        );
    }

    #[test]
    fn test_pinned_prefers_target_node() {
        let n1 = make_node("alpha", 1, vec![]);
        let n2 = make_node("beta", 2, vec![]);
        let beta_id = n2.node_id();
        let view = make_view(vec![n1, n2]);

        let strategy = Pinned {
            preferred_node_id: beta_id.clone(),
        };
        let spec = ActorSpec::new("w/0", "app::Worker");
        let decision = select_node(&strategy, &spec, &view).unwrap();

        assert_eq!(decision.node_id, beta_id);
    }

    #[test]
    fn test_no_eligible_nodes_returns_none() {
        let view = make_view(vec![make_node("alpha", 1, vec![])]);

        // Require Coordinator class, but only Worker nodes exist
        let spec = ActorSpec::new("coord/0", "app::Coordinator").with_constraints(
            crate::spec::PlacementConstraints {
                required_classes: vec![NodeClass::Coordinator],
                ..Default::default()
            },
        );

        let decision = select_node(&LeastLoaded, &spec, &view);
        assert!(decision.is_none());
    }

    #[test]
    fn test_dead_nodes_excluded() {
        let mut view = make_view(vec![
            make_node("alpha", 1, vec![]),
            make_node("beta", 2, vec!["w/0".into()]),
        ]);

        // Kill alpha
        let alpha_id = view
            .nodes
            .keys()
            .find(|k| k.contains("alpha"))
            .unwrap()
            .clone();
        view.mark_failed(&alpha_id);

        let spec = ActorSpec::new("w/1", "app::Worker");
        let decision = select_node(&LeastLoaded, &spec, &view).unwrap();

        // Only beta is alive
        assert!(
            decision.node_id.contains("beta"),
            "expected beta, got {}",
            decision.node_id
        );
    }
}
