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
//! | [`RandomPlacement`] | Uniform random selection |
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

use crate::app::node_info::{ClusterView, NodeInfo};
use crate::app::spec::ActorSpec;

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
        if !spec.placement.is_satisfied_by(
            &node.node_id(),
            &node.class,
            &node.metadata,
            &node.running_actors,
        ) {
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
pub struct RandomPlacement {
    /// Seeded RNG behind a mutex. `fitness` takes `&self`, so the draw needs
    /// interior mutability; the sim is single-threaded so this never contends.
    /// Seeding off the world seed is what makes placement replay deterministically
    /// (the old `rand::rng()` form drew from global OS entropy and diverged
    /// run-to-run on the same seed, silently breaking the simulation guarantee).
    rng: std::sync::Mutex<rand::rngs::StdRng>,
}

impl RandomPlacement {
    /// Seed the placement RNG. Under simulation pass `world.derive_seed("placement")`
    /// (or any per-run derived seed) so placement decisions descend from the world
    /// seed; in production seed from a source of your choosing.
    pub fn new(seed: u64) -> Self {
        use rand::SeedableRng;
        Self {
            rng: std::sync::Mutex::new(rand::rngs::StdRng::seed_from_u64(seed)),
        }
    }
}

impl PlacementStrategy for RandomPlacement {
    fn fitness(&self, _node: &NodeInfo, _spec: &ActorSpec, _view: &ClusterView) -> f64 {
        use rand::Rng;
        self.rng.lock().unwrap().random::<f64>()
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
    use crate::cluster::config::{NodeClass, NodeIdentity};
    use std::collections::HashMap;

    fn make_node(name: &str, incarnation: u64, actors: Vec<String>) -> NodeInfo {
        NodeInfo {
            identity: NodeIdentity::for_test(name, incarnation),
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
            decision.node_id.contains(
                &crate::cluster::config::NodeIdentity::test_endpoint_id("beta").to_string()
            ),
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
            crate::app::spec::PlacementConstraints {
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
            .find(|k| {
                k.contains(
                    &crate::cluster::config::NodeIdentity::test_endpoint_id("alpha").to_string(),
                )
            })
            .unwrap()
            .clone();
        view.mark_failed(&alpha_id);

        let spec = ActorSpec::new("w/1", "app::Worker");
        let decision = select_node(&LeastLoaded, &spec, &view).unwrap();

        // Only beta is alive
        assert!(
            decision.node_id.contains(
                &crate::cluster::config::NodeIdentity::test_endpoint_id("beta").to_string()
            ),
            "expected beta, got {}",
            decision.node_id
        );
    }

    #[test]
    fn test_colocate_with_pins_to_anchor_node() {
        // beta already runs the anchor "writer/c1"; alpha is emptier (would win
        // LeastLoaded) but does not run the anchor, so colocate_with must force beta.
        let view = make_view(vec![
            make_node("alpha", 1, vec![]),
            make_node("beta", 2, vec!["writer/c1".into()]),
        ]);
        let spec = ActorSpec::new("reader/c1/0", "app::Reader").with_constraints(
            crate::app::spec::PlacementConstraints {
                colocate_with: Some("writer/c1".into()),
                ..Default::default()
            },
        );
        let decision = select_node(&LeastLoaded, &spec, &view).unwrap();
        assert!(
            decision.node_id.contains(
                &crate::cluster::config::NodeIdentity::test_endpoint_id("beta").to_string()
            ),
            "colocate_with should pin to the anchor's node (beta), got {}",
            decision.node_id
        );
    }

    #[test]
    fn test_colocate_with_no_anchor_returns_none() {
        // No node runs the anchor → hard filter yields NoEligibleNodes (never
        // silently relocates), unlike the soft Pinned strategy.
        let view = make_view(vec![
            make_node("alpha", 1, vec![]),
            make_node("beta", 2, vec![]),
        ]);
        let spec = ActorSpec::new("reader/c1/0", "app::Reader").with_constraints(
            crate::app::spec::PlacementConstraints {
                colocate_with: Some("writer/c1".into()),
                ..Default::default()
            },
        );
        assert!(select_node(&LeastLoaded, &spec, &view).is_none());
    }

    #[test]
    fn test_required_node_id_pins_exactly_or_none() {
        let alpha = make_node("alpha", 1, vec![]); // emptier → LeastLoaded would pick it
        let beta = make_node("beta", 2, vec!["x".into()]);
        let beta_id = beta.node_id();
        let view = make_view(vec![alpha, beta]);

        // Hard-pin to beta: chosen even though alpha is less loaded.
        let spec = ActorSpec::new("w/0", "app::Worker").with_constraints(
            crate::app::spec::PlacementConstraints {
                required_node_id: Some(beta_id.clone()),
                ..Default::default()
            },
        );
        assert_eq!(
            select_node(&LeastLoaded, &spec, &view).unwrap().node_id,
            beta_id
        );

        // Pin to a node that does not exist → NoEligibleNodes (no silent relocate).
        let spec_missing = ActorSpec::new("w/1", "app::Worker").with_constraints(
            crate::app::spec::PlacementConstraints {
                required_node_id: Some("ghost@127.0.0.1:9999#0".into()),
                ..Default::default()
            },
        );
        assert!(select_node(&LeastLoaded, &spec_missing, &view).is_none());
    }

    #[test]
    fn test_default_constraints_unchanged_backward_compat() {
        // Empty constraints (the default) must still place anywhere — the new
        // fields are additive Option/None and must not regress existing behavior.
        let view = make_view(vec![make_node("alpha", 1, vec![])]);
        let spec = ActorSpec::new("w/0", "app::Worker");
        assert!(select_node(&LeastLoaded, &spec, &view).is_some());
    }
}
