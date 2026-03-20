//! Coordinator — the orchestrator actor that manages actor placement.
//!
//! The Coordinator is itself a murmer actor, dogfooding the framework.
//! It maintains a [`ClusterView`], accepts [`SubmitSpec`] messages to
//! place actors across the cluster, and handles crash recovery when
//! nodes fail.
//!
//! # Lifecycle
//!
//! 1. The Coordinator starts on one node (chosen by leader election)
//! 2. It subscribes to `ClusterEvent`s to track node joins/failures
//! 3. Users send `SubmitSpec` messages to declare what actors should run
//! 4. The Coordinator evaluates the placement strategy and sends
//!    `SpawnActor` control messages to target nodes
//! 5. When a node fails, the Coordinator re-places affected actors
//!    according to each spec's `CrashStrategy`
//!
//! # Example
//!
//! ```rust,ignore
//! let coordinator = system.lookup::<Coordinator>("coordinator").unwrap();
//!
//! coordinator.send(SubmitSpec {
//!     spec: ActorSpec::new("worker/0", "app::Worker")
//!         .with_state(serialized_state)
//!         .with_crash_strategy(CrashStrategy::Redistribute),
//! }).await?;
//! ```

use std::collections::HashMap;

use murmer::prelude::*;
use murmer_macros::{Message, handlers};
use serde::{Deserialize, Serialize};

use crate::election::LeaderElection;
use crate::error::OrchestratorError;
use crate::node_info::{ClusterView, NodeInfo};
use crate::placement::{self, PlacementDecision, PlacementStrategy};
use crate::spec::{ActorSpec, CrashStrategy};

// =============================================================================
// COORDINATOR ACTOR
// =============================================================================

/// The orchestrator actor. Manages actor placement across the cluster.
#[derive(Debug)]
pub struct Coordinator;

/// State for the Coordinator actor.
pub struct CoordinatorState {
    /// Snapshot of the cluster topology.
    pub cluster_view: ClusterView,
    /// All submitted actor specs, keyed by label.
    pub specs: HashMap<String, ActorSpec>,
    /// Placement strategy for deciding which node gets which actor.
    pub placement_strategy: Box<dyn PlacementStrategy>,
    /// Leader election algorithm.
    pub election: Box<dyn LeaderElection>,
    /// This node's ID — used to determine if we're the leader.
    pub local_node_id: String,
    /// Pending spawn requests awaiting acks, keyed by request_id.
    pub pending_spawns: HashMap<u64, PendingSpawn>,
    /// Specs waiting for a failed node to return, keyed by label.
    pub waiting_for_return: HashMap<String, WaitingSpec>,
    /// Monotonic counter for generating unique request IDs.
    next_request_id: u64,
}

/// A spawn request that's been sent but not yet acknowledged.
#[derive(Debug, Clone)]
pub struct PendingSpawn {
    pub spec_label: String,
    pub target_node_id: String,
}

/// A spec whose node failed but is using WaitForReturn strategy.
#[derive(Debug, Clone)]
pub struct WaitingSpec {
    pub spec: ActorSpec,
    pub failed_node_id: String,
}

impl Actor for Coordinator {
    type State = CoordinatorState;
}

// =============================================================================
// MESSAGES
// =============================================================================

/// Submit a new actor spec for placement.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Result<PlacementDecision, String>, remote = "coordinator::SubmitSpec")]
pub struct SubmitSpec {
    pub spec: ActorSpec,
}

/// Remove a previously submitted spec (and stop the actor if running).
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Result<(), String>, remote = "coordinator::RemoveSpec")]
pub struct RemoveSpec {
    pub label: String,
}

/// Query the current cluster view.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = ClusterViewSnapshot, remote = "coordinator::GetClusterView")]
pub struct GetClusterView;

/// Query the status of all managed specs.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Vec<SpecStatus>, remote = "coordinator::GetSpecs")]
pub struct GetSpecs;

/// Notify the coordinator that a node has joined.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "coordinator::NotifyNodeJoined")]
pub struct NotifyNodeJoined {
    pub node_id: String,
    pub info: SerializableNodeInfo,
}

/// Notify the coordinator that a node has failed.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "coordinator::NotifyNodeFailed")]
pub struct NotifyNodeFailed {
    pub node_id: String,
}

/// Notify the coordinator that a node has left gracefully.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "coordinator::NotifyNodeLeft")]
pub struct NotifyNodeLeft {
    pub node_id: String,
}

/// Notify the coordinator that a spawn succeeded.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "coordinator::NotifySpawnAck")]
pub struct NotifySpawnAck {
    pub request_id: u64,
    pub success: bool,
    pub error: Option<String>,
}

// =============================================================================
// RESPONSE TYPES
// =============================================================================

/// Serializable snapshot of the cluster view for responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterViewSnapshot {
    pub nodes: Vec<NodeSnapshot>,
    pub alive_count: usize,
    pub total_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub name: String,
    pub class: String,
    pub actor_count: usize,
    pub is_alive: bool,
}

/// Status of a managed actor spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecStatus {
    pub label: String,
    pub actor_type: String,
    pub placed_on: Option<String>,
    pub state: SpecState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpecState {
    /// Placed and running on a node.
    Running,
    /// Spawn request sent, waiting for ack.
    Pending,
    /// Node failed, waiting for return before redistributing.
    WaitingForReturn,
    /// Not yet placed.
    Unplaced,
}

/// Serializable node info for messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableNodeInfo {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub incarnation: u64,
    pub class: murmer::cluster::config::NodeClass,
    pub metadata: HashMap<String, String>,
}

// =============================================================================
// HANDLERS
// =============================================================================

#[handlers]
impl Coordinator {
    #[handler]
    fn submit_spec(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: SubmitSpec,
    ) -> Result<PlacementDecision, String> {
        let label = msg.spec.label.clone();

        if state.specs.contains_key(&label) {
            return Err(OrchestratorError::SpecAlreadyExists { label }.to_string());
        }

        // Run placement
        let decision = placement::select_node(
            state.placement_strategy.as_ref(),
            &msg.spec,
            &state.cluster_view,
        )
        .ok_or_else(|| {
            OrchestratorError::NoEligibleNodes {
                reason: format!("no node satisfies constraints for {label}"),
            }
            .to_string()
        })?;

        tracing::info!(
            "Placing {} on {} ({})",
            label,
            decision.node_id,
            decision.reason
        );

        // Track the spec
        state.specs.insert(label.clone(), msg.spec);

        // Record as pending — actual placement happens on spawn ack
        let request_id = state.next_request_id();
        state.pending_spawns.insert(
            request_id,
            PendingSpawn {
                spec_label: label,
                target_node_id: decision.node_id.clone(),
            },
        );

        Ok(decision)
    }

    #[handler]
    fn remove_spec(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: RemoveSpec,
    ) -> Result<(), String> {
        if state.specs.remove(&msg.label).is_none() {
            return Err(OrchestratorError::SpecNotFound {
                label: msg.label.clone(),
            }
            .to_string());
        }

        // Remove from pending spawns
        state.pending_spawns.retain(|_, ps| ps.spec_label != msg.label);

        // Remove from waiting
        state.waiting_for_return.remove(&msg.label);

        // Remove from cluster view tracking
        state.cluster_view.remove_actor_anywhere(&msg.label);

        tracing::info!("Removed spec: {}", msg.label);
        Ok(())
    }

    #[handler]
    fn get_cluster_view(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        _msg: GetClusterView,
    ) -> ClusterViewSnapshot {
        ClusterViewSnapshot {
            alive_count: state.cluster_view.alive_count(),
            total_count: state.cluster_view.total_count(),
            nodes: state
                .cluster_view
                .nodes
                .values()
                .map(|n| NodeSnapshot {
                    node_id: n.node_id(),
                    name: n.identity.name.clone(),
                    class: n.class.to_string(),
                    actor_count: n.actor_count(),
                    is_alive: n.is_alive,
                })
                .collect(),
        }
    }

    #[handler]
    fn get_specs(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        _msg: GetSpecs,
    ) -> Vec<SpecStatus> {
        state
            .specs
            .iter()
            .map(|(label, spec)| {
                let placed_on = state.cluster_view.find_actor(label).map(|s| s.to_string());
                let spec_state = if state.waiting_for_return.contains_key(label) {
                    SpecState::WaitingForReturn
                } else if state
                    .pending_spawns
                    .values()
                    .any(|ps| ps.spec_label == *label)
                {
                    SpecState::Pending
                } else if placed_on.is_some() {
                    SpecState::Running
                } else {
                    SpecState::Unplaced
                };

                SpecStatus {
                    label: label.clone(),
                    actor_type: spec.actor_type_name.clone(),
                    placed_on,
                    state: spec_state,
                }
            })
            .collect()
    }

    #[handler]
    fn notify_node_joined(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifyNodeJoined,
    ) {
        let info = NodeInfo::new(
            murmer::cluster::config::NodeIdentity {
                name: msg.info.name,
                host: msg.info.host,
                port: msg.info.port,
                incarnation: msg.info.incarnation,
            },
            msg.info.class,
            msg.info.metadata,
        );

        tracing::info!("Node joined: {}", msg.node_id);
        state.cluster_view.upsert_node(info);

        // Check if any waiting specs can now be resolved
        // (node that previously failed has rejoined)
        let rejoined: Vec<String> = state
            .waiting_for_return
            .iter()
            .filter(|(_, ws)| ws.failed_node_id == msg.node_id)
            .map(|(label, _)| label.clone())
            .collect();

        for label in rejoined {
            if let Some(ws) = state.waiting_for_return.remove(&label) {
                tracing::info!(
                    "Node {} returned — re-spawning spec {} on it",
                    msg.node_id,
                    ws.spec.label
                );
                // Create a pending spawn rather than phantom-placing
                let request_id = state.next_request_id();
                state.pending_spawns.insert(
                    request_id,
                    PendingSpawn {
                        spec_label: ws.spec.label,
                        target_node_id: msg.node_id.clone(),
                    },
                );
            }
        }
    }

    #[handler]
    fn notify_node_failed(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifyNodeFailed,
    ) {
        tracing::warn!("Node failed: {}", msg.node_id);
        state.cluster_view.mark_failed(&msg.node_id);
        state.handle_node_departure(&msg.node_id, false);
    }

    #[handler]
    fn notify_node_left(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifyNodeLeft,
    ) {
        tracing::info!("Node left gracefully: {}", msg.node_id);
        state.handle_node_departure(&msg.node_id, true);
        state.cluster_view.remove_node(&msg.node_id);
    }

    #[handler]
    fn notify_spawn_ack(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifySpawnAck,
    ) {
        if let Some(pending) = state.pending_spawns.remove(&msg.request_id) {
            if msg.success {
                tracing::info!(
                    "Spawn confirmed: {} on {}",
                    pending.spec_label,
                    pending.target_node_id
                );
                state
                    .cluster_view
                    .add_actor(&pending.target_node_id, &pending.spec_label);
            } else {
                tracing::warn!(
                    "Spawn failed for {}: {}",
                    pending.spec_label,
                    msg.error.as_deref().unwrap_or("unknown error")
                );
            }
        }
    }
}

// =============================================================================
// COORDINATOR BUILDER
// =============================================================================

impl CoordinatorState {
    /// Create a new CoordinatorState with the given strategy and election.
    pub fn new(
        local_node_id: impl Into<String>,
        placement_strategy: Box<dyn PlacementStrategy>,
        election: Box<dyn LeaderElection>,
    ) -> Self {
        Self {
            cluster_view: ClusterView::new(),
            specs: HashMap::new(),
            placement_strategy,
            election,
            local_node_id: local_node_id.into(),
            pending_spawns: HashMap::new(),
            waiting_for_return: HashMap::new(),
            next_request_id: 0,
        }
    }

    /// Generate a unique request ID.
    fn next_request_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    /// Check if this node is the current leader.
    pub fn is_leader(&self) -> bool {
        self.election
            .elect(&self.cluster_view)
            .is_some_and(|leader| leader == self.local_node_id)
    }

    /// Handle a node departure — shared logic for both failure and graceful leave.
    ///
    /// When `graceful` is true (node left voluntarily), WaitForReturn is
    /// collapsed to Redistribute since the node chose to leave.
    fn handle_node_departure(&mut self, node_id: &str, graceful: bool) {
        // Find all specs placed on the departing node (running or pending spawn)
        let affected_labels: Vec<String> = self
            .specs
            .keys()
            .filter(|label| {
                // Check if running on this node
                let running_on = self
                    .cluster_view
                    .find_actor(label)
                    .is_some_and(|n| n == node_id);
                // Check if pending spawn on this node
                let pending_on = self
                    .pending_spawns
                    .values()
                    .any(|ps| ps.spec_label == **label && ps.target_node_id == node_id);
                running_on || pending_on
            })
            .cloned()
            .collect();

        // Clear any pending spawns targeting this node
        self.pending_spawns
            .retain(|_, ps| ps.target_node_id != node_id);

        for label in affected_labels {
            let spec = match self.specs.get(&label) {
                Some(s) => s.clone(),
                None => continue,
            };

            self.cluster_view.remove_actor(node_id, &label);

            match &spec.crash_strategy {
                CrashStrategy::Abandon => {
                    tracing::info!("Abandoning {label} (node {node_id} departed)");
                    self.specs.remove(&label);
                }
                CrashStrategy::WaitForReturn(duration) if !graceful => {
                    tracing::warn!(
                        "Waiting {:?} for node {} to return (spec: {label}) — \
                         NOTE: timeout fallback to Redistribute is not yet implemented; \
                         spec will wait indefinitely until the node returns or spec is removed",
                        duration,
                        node_id,
                    );
                    self.waiting_for_return.insert(
                        label.clone(),
                        WaitingSpec {
                            spec: spec.clone(),
                            failed_node_id: node_id.to_string(),
                        },
                    );
                    // TODO: spawn a timer that converts to Redistribute after duration
                }
                _ => {
                    // Redistribute (or WaitForReturn on graceful departure)
                    let reason = if graceful { "graceful departure" } else { "failure" };
                    tracing::info!("Redistributing {label} (node {node_id} {reason})");

                    if let Some(decision) = placement::select_node(
                        self.placement_strategy.as_ref(),
                        &spec,
                        &self.cluster_view,
                    ) {
                        self.cluster_view
                            .add_actor(&decision.node_id, &label);
                        tracing::info!(
                            "Re-placed {label} on {} ({})",
                            decision.node_id,
                            decision.reason
                        );
                    } else {
                        tracing::warn!("No eligible node for {label} — spec remains unplaced");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::OldestNode;
    use crate::placement::LeastLoaded;
    use murmer::cluster::config::{NodeClass, NodeIdentity};

    fn make_system_and_coordinator() -> (murmer::System, Endpoint<Coordinator>) {
        let system = murmer::System::local();
        let mut state = CoordinatorState::new(
            "local",
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        );

        // Add a couple nodes to the view
        state.cluster_view.upsert_node(NodeInfo::new(
            NodeIdentity {
                name: "alpha".into(),
                host: "127.0.0.1".into(),
                port: 7100,
                incarnation: 1,
            },
            NodeClass::Worker,
            HashMap::new(),
        ));
        state.cluster_view.upsert_node(NodeInfo::new(
            NodeIdentity {
                name: "beta".into(),
                host: "127.0.0.1".into(),
                port: 7200,
                incarnation: 2,
            },
            NodeClass::Worker,
            HashMap::new(),
        ));

        let ep = system.start("coordinator", Coordinator, state);
        (system, ep)
    }

    #[tokio::test]
    async fn test_submit_spec() {
        let (_system, coordinator) = make_system_and_coordinator();

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker"),
            })
            .await
            .unwrap();

        assert!(result.is_ok());
        let decision = result.unwrap();
        assert!(!decision.node_id.is_empty());
    }

    #[tokio::test]
    async fn test_submit_duplicate_spec() {
        let (_system, coordinator) = make_system_and_coordinator();

        let _ = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker"),
            })
            .await
            .unwrap();

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker"),
            })
            .await
            .unwrap();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remove_spec() {
        let (_system, coordinator) = make_system_and_coordinator();

        let _ = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker"),
            })
            .await
            .unwrap();

        let result = coordinator
            .send(RemoveSpec {
                label: "worker/0".into(),
            })
            .await
            .unwrap();

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_specs() {
        let (_system, coordinator) = make_system_and_coordinator();

        let _ = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker"),
            })
            .await
            .unwrap();

        let _ = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/1", "app::Worker"),
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 2);
    }

    #[tokio::test]
    async fn test_node_failure_redistributes() {
        let (_system, coordinator) = make_system_and_coordinator();

        // Place a worker — it'll go to alpha (least loaded, lowest incarnation)
        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker")
                    .with_crash_strategy(CrashStrategy::Redistribute),
            })
            .await
            .unwrap()
            .unwrap();

        let original_node = result.node_id.clone();

        // Fail the node it was placed on
        coordinator
            .send(NotifyNodeFailed {
                node_id: original_node.clone(),
            })
            .await
            .unwrap();

        // Check specs — should still exist but on a different node
        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 1);
        assert!(matches!(specs[0].state, SpecState::Running));
        // Should now be on a different node
        assert_ne!(specs[0].placed_on.as_deref(), Some(original_node.as_str()));
    }

    #[tokio::test]
    async fn test_node_failure_abandon() {
        let (_system, coordinator) = make_system_and_coordinator();

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker")
                    .with_crash_strategy(CrashStrategy::Abandon),
            })
            .await
            .unwrap()
            .unwrap();

        coordinator
            .send(NotifyNodeFailed {
                node_id: result.node_id,
            })
            .await
            .unwrap();

        // Spec should be gone
        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 0);
    }

    #[tokio::test]
    async fn test_get_cluster_view() {
        let (_system, coordinator) = make_system_and_coordinator();

        let view = coordinator.send(GetClusterView).await.unwrap();
        assert_eq!(view.alive_count, 2);
        assert_eq!(view.total_count, 2);
        assert_eq!(view.nodes.len(), 2);
    }
}
