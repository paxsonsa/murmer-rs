//! Actor specifications and crash strategies.
//!
//! An [`ActorSpec`] describes an actor that the orchestrator should place and
//! manage across the cluster. It captures what to run, how to recover from
//! crashes, and where to place the actor.
//!
//! # Example
//!
//! ```rust,ignore
//! let spec = ActorSpec::new("worker/0", "my_app::Worker")
//!     .with_state(serialized_state)
//!     .with_crash_strategy(CrashStrategy::Redistribute)
//!     .with_constraint(PlacementConstraints {
//!         required_classes: vec![NodeClass::Worker],
//!         ..Default::default()
//!     });
//! ```

use std::collections::HashMap;
use std::time::Duration;

use crate::cluster::config::NodeClass;
use serde::{Deserialize, Serialize};

// =============================================================================
// CRASH STRATEGY
// =============================================================================

/// What the Coordinator does when a node hosting this actor fails.
///
/// This is per-`ActorSpec` — different actors in the same cluster can have
/// different crash strategies based on their statefulness and importance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum CrashStrategy {
    /// Move the actor to another eligible node immediately.
    /// Best for stateless workers or actors with serializable state.
    #[default]
    Redistribute,
    /// Wait for the failed node to rejoin. If it doesn't return within
    /// the duration, fall back to `Redistribute`.
    WaitForReturn(Duration),
    /// Let the actor die with the node. No recovery attempted.
    /// Use for ephemeral computations or "down with the ship" semantics.
    Abandon,
}

// =============================================================================
// PLACEMENT CONSTRAINTS
// =============================================================================

/// Constraints that limit which nodes are eligible for hosting an actor.
///
/// The Coordinator filters candidate nodes through these constraints
/// *before* scoring them with the `PlacementStrategy` fitness function.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlacementConstraints {
    /// Node must be one of these classes. Empty means any class is acceptable.
    pub required_classes: Vec<NodeClass>,
    /// Node must have all of these metadata key-value pairs.
    pub required_metadata: HashMap<String, String>,
    /// Actor must not be placed on a node already running actors with these labels.
    /// Useful for spreading replicas across failure domains.
    pub anti_affinity_labels: Vec<String>,
    /// HARD positive affinity: the node must ALREADY be running an actor with
    /// this label. The inverse of `anti_affinity_labels` — use it to co-locate
    /// a member with an anchor (e.g. pin a reader to the node hosting its
    /// writer). Unlike the soft `Pinned` strategy this is a filter, so if no
    /// node runs the anchor the spec gets `NoEligibleNodes` rather than landing
    /// elsewhere. `None` = no co-location requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub colocate_with: Option<String>,
    /// HARD pin to exactly this node id. If that node is not alive/eligible the
    /// spec gets `NoEligibleNodes` (it is NEVER silently relocated — that is the
    /// difference from the soft `Pinned` strategy). `None` = no node pin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_node_id: Option<String>,
}

impl PlacementConstraints {
    /// Check if a node satisfies all constraints.
    ///
    /// `node_id` is the candidate node's id, needed for `required_node_id`.
    pub fn is_satisfied_by(
        &self,
        node_id: &str,
        node_class: &NodeClass,
        node_metadata: &HashMap<String, String>,
        running_actors: &[String],
    ) -> bool {
        // Check required classes
        if !self.required_classes.is_empty() && !self.required_classes.contains(node_class) {
            return false;
        }

        // Check required metadata
        for (key, value) in &self.required_metadata {
            match node_metadata.get(key) {
                Some(v) if v == value => {}
                _ => return false,
            }
        }

        // Check anti-affinity (repel)
        for label in &self.anti_affinity_labels {
            if running_actors.contains(label) {
                return false;
            }
        }

        // Check positive affinity (attract): node must already run the anchor.
        if let Some(anchor) = &self.colocate_with
            && !running_actors.iter().any(|l| l == anchor)
        {
            return false;
        }

        // Check hard node pin: must be exactly this node.
        if let Some(required) = &self.required_node_id
            && node_id != required
        {
            return false;
        }

        true
    }
}

// =============================================================================
// ACTOR SPEC
// =============================================================================

/// Describes an actor the orchestrator should place and manage.
///
/// Submit specs to the Coordinator — it decides which node to spawn
/// each actor on, monitors for failures, and re-places actors according
/// to their `CrashStrategy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorSpec {
    /// Label to register the actor under (e.g., "worker/0").
    pub label: String,
    /// Key into the SpawnRegistry — identifies the actor type.
    pub actor_type_name: String,
    /// Serialized initial state (via MigratableActor).
    pub initial_state: Vec<u8>,
    /// What to do when the hosting node fails.
    pub crash_strategy: CrashStrategy,
    /// Constraints on eligible nodes.
    pub placement: PlacementConstraints,
}

impl ActorSpec {
    /// Create a new spec with default crash strategy and no constraints.
    pub fn new(label: impl Into<String>, actor_type_name: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            actor_type_name: actor_type_name.into(),
            initial_state: Vec::new(),
            crash_strategy: CrashStrategy::default(),
            placement: PlacementConstraints::default(),
        }
    }

    /// Set the serialized initial state.
    pub fn with_state(mut self, state: Vec<u8>) -> Self {
        self.initial_state = state;
        self
    }

    /// Set the crash strategy.
    pub fn with_crash_strategy(mut self, strategy: CrashStrategy) -> Self {
        self.crash_strategy = strategy;
        self
    }

    /// Set placement constraints.
    pub fn with_constraints(mut self, constraints: PlacementConstraints) -> Self {
        self.placement = constraints;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A placeholder node id for constraints that don't exercise `required_node_id`.
    const ANY_NODE: &str = "node-a@127.0.0.1:7100#1";

    #[test]
    fn test_placement_constraints_empty_allows_all() {
        let constraints = PlacementConstraints::default();
        assert!(constraints.is_satisfied_by(ANY_NODE, &NodeClass::Worker, &HashMap::new(), &[]));
    }

    #[test]
    fn test_placement_constraints_required_class() {
        let constraints = PlacementConstraints {
            required_classes: vec![NodeClass::Worker],
            ..Default::default()
        };
        assert!(constraints.is_satisfied_by(ANY_NODE, &NodeClass::Worker, &HashMap::new(), &[]));
        assert!(!constraints.is_satisfied_by(ANY_NODE, &NodeClass::Edge, &HashMap::new(), &[]));
    }

    #[test]
    fn test_placement_constraints_required_metadata() {
        let constraints = PlacementConstraints {
            required_metadata: [("region".into(), "us-west".into())].into(),
            ..Default::default()
        };
        let good_meta: HashMap<String, String> = [("region".into(), "us-west".into())].into();
        let bad_meta: HashMap<String, String> = [("region".into(), "eu-west".into())].into();

        assert!(constraints.is_satisfied_by(ANY_NODE, &NodeClass::Worker, &good_meta, &[]));
        assert!(!constraints.is_satisfied_by(ANY_NODE, &NodeClass::Worker, &bad_meta, &[]));
        assert!(!constraints.is_satisfied_by(ANY_NODE, &NodeClass::Worker, &HashMap::new(), &[]));
    }

    #[test]
    fn test_placement_constraints_anti_affinity() {
        let constraints = PlacementConstraints {
            anti_affinity_labels: vec!["replica/0".into()],
            ..Default::default()
        };
        assert!(constraints.is_satisfied_by(
            ANY_NODE,
            &NodeClass::Worker,
            &HashMap::new(),
            &["worker/1".into()],
        ));
        assert!(!constraints.is_satisfied_by(
            ANY_NODE,
            &NodeClass::Worker,
            &HashMap::new(),
            &["replica/0".into(), "worker/1".into()],
        ));
    }

    #[test]
    fn test_actor_spec_builder() {
        let spec = ActorSpec::new("worker/0", "app::Worker")
            .with_state(vec![1, 2, 3])
            .with_crash_strategy(CrashStrategy::Abandon);

        assert_eq!(spec.label, "worker/0");
        assert_eq!(spec.actor_type_name, "app::Worker");
        assert_eq!(spec.initial_state, vec![1, 2, 3]);
        assert!(matches!(spec.crash_strategy, CrashStrategy::Abandon));
    }
}
