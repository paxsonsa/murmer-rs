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
use std::sync::Arc;

use crate::prelude::*;
use serde::{Deserialize, Serialize};

use crate::cluster::framing::SpawnRequest;

use crate::app::election::LeaderElection;
use crate::app::error::OrchestratorError;
use crate::app::node_info::{ClusterView, NodeInfo};
use crate::app::placement::{self, PlacementDecision, PlacementStrategy};
use crate::app::singleton::{
    CoordinatorGenerationSource, GenerationSource, SingletonOwnership, SingletonPhase,
    SingletonSpec, resolve_owner,
};
use crate::app::spawn_sender::SpawnSender;
use crate::app::spec::{ActorSpec, CrashStrategy};

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
    /// Cluster singletons being managed, keyed by singleton label.
    pub singletons: HashMap<String, SingletonRuntime>,
    /// Pluggable authority that mints `(term, seq)` generations for singletons.
    /// Defaults to an in-RAM source (single-node/tests); inject a durable one
    /// (e.g. catalog-backed) for multi-node write deployments.
    pub generation_source: Arc<dyn GenerationSource>,
    /// Monotonic counter for generating unique request IDs.
    next_request_id: u64,
    /// Channel for sending spawn requests to the transport layer.
    spawn_sender: Option<SpawnSender>,
}

/// Which managed aggregate a pending spawn belongs to, so its ack can be routed
/// to the right bookkeeping. `None` = an ordinary standalone spec.
///
/// (A `Group(label)` variant is added when PlacementGroup / M2 lands; unifying
/// the back-reference here keeps `notify_spawn_ack` a single code path.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerKey {
    /// The spawn is the active instance of this cluster singleton.
    Singleton(String),
}

/// A spawn request that's been sent but not yet acknowledged.
#[derive(Debug, Clone)]
pub struct PendingSpawn {
    pub spec_label: String,
    pub target_node_id: String,
    /// The managed aggregate this spawn belongs to (singleton/group), if any.
    pub owner: Option<OwnerKey>,
}

/// Coordinator-side runtime record for a managed cluster singleton.
///
/// `ownership` is a soft cache of the authoritative record (the durable copy
/// lives in the [`GenerationSource`]'s backing store); it is `None` between spec
/// registration and the first grant.
#[derive(Debug, Clone)]
pub struct SingletonRuntime {
    pub spec: SingletonSpec,
    pub ownership: Option<SingletonOwnership>,
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

/// Declare (or idempotently re-assert) a cluster singleton. The Coordinator
/// resolves the anchor to one owner node, mints a fresh `(term, seq)`
/// generation, and spawns the single instance there. Returns the granted
/// ownership.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Result<SingletonOwnership, String>, remote = "coordinator::StartSingleton")]
pub struct StartSingleton {
    pub spec: SingletonSpec,
}

/// Query the current ownership of a cluster singleton (`None` if unknown).
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Option<SingletonOwnership>, remote = "coordinator::GetSingleton")]
pub struct GetSingleton {
    pub label: String,
}

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

/// Internal: WaitForReturn timeout expired — fall back to Redistribute.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "coordinator::WaitForReturnTimeout")]
pub struct WaitForReturnTimeout {
    pub label: String,
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
    pub class: crate::cluster::config::NodeClass,
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
        if !state.is_leader() {
            return Err(OrchestratorError::NotLeader.to_string());
        }

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
        state.specs.insert(label.clone(), msg.spec.clone());

        // Record as pending — actual placement happens on spawn ack
        let request_id = state.next_request_id();
        state.pending_spawns.insert(
            request_id,
            PendingSpawn {
                spec_label: label,
                target_node_id: decision.node_id.clone(),
                owner: None,
            },
        );

        // Send the spawn request to the target node
        state.send_spawn(&decision.node_id, request_id, &msg.spec);

        Ok(decision)
    }

    #[handler]
    fn remove_spec(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: RemoveSpec,
    ) -> Result<(), String> {
        if !state.is_leader() {
            return Err(OrchestratorError::NotLeader.to_string());
        }

        if state.specs.remove(&msg.label).is_none() {
            return Err(OrchestratorError::SpecNotFound {
                label: msg.label.clone(),
            }
            .to_string());
        }

        // Remove from pending spawns
        state
            .pending_spawns
            .retain(|_, ps| ps.spec_label != msg.label);

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
            crate::cluster::config::NodeIdentity {
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
                let request_id = state.next_request_id();
                state.pending_spawns.insert(
                    request_id,
                    PendingSpawn {
                        spec_label: ws.spec.label.clone(),
                        target_node_id: msg.node_id.clone(),
                        owner: None,
                    },
                );
                state.send_spawn(&msg.node_id, request_id, &ws.spec);
            }
        }
    }

    #[handler]
    async fn notify_node_failed(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifyNodeFailed,
    ) {
        tracing::warn!("Node failed: {}", msg.node_id);
        state.cluster_view.mark_failed(&msg.node_id);
        let timers = state.handle_node_departure(&msg.node_id, false);

        // Spawn timeout tasks for WaitForReturn specs
        for (label, duration) in timers {
            let endpoint = ctx.endpoint();
            tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let _ = endpoint.send(WaitForReturnTimeout { label }).await;
            });
        }

        // Re-place any singleton the failed node owned. The failed owner is gone
        // (SWIM-confirmed), so we do NOT drain it — we mint a strictly-higher
        // term and re-spawn; a zombie ex-owner is fenced by the `(term, seq)`
        // compare on its next write. (Graceful drain handoff is M3.)
        state.redrive_singletons_after_loss(&msg.node_id).await;
    }

    #[handler]
    async fn notify_node_left(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: NotifyNodeLeft,
    ) {
        tracing::info!("Node left gracefully: {}", msg.node_id);
        // Graceful departures never produce WaitForReturn timers (guarded by !graceful)
        let _timers = state.handle_node_departure(&msg.node_id, true);
        state.cluster_view.remove_node(&msg.node_id);

        // Re-place any singleton the departed node owned (fenced by a new term).
        state.redrive_singletons_after_loss(&msg.node_id).await;
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
                // A singleton's instance is now running — promote it to Active.
                if let Some(OwnerKey::Singleton(label)) = &pending.owner
                    && let Some(rt) = state.singletons.get_mut(label)
                    && let Some(ownership) = &mut rt.ownership
                {
                    ownership.phase = SingletonPhase::Active;
                }
            } else {
                tracing::warn!(
                    "Spawn failed for {}: {}",
                    pending.spec_label,
                    msg.error.as_deref().unwrap_or("unknown error")
                );
                // A failed singleton spawn stays in `Starting`; re-placement /
                // fenced handoff retry is handled by the M4 step-4 machinery.
            }
        }
    }

    #[handler]
    fn wait_for_return_timeout(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: WaitForReturnTimeout,
    ) {
        // If the spec is still waiting, the node didn't return in time — redistribute
        if let Some(ws) = state.waiting_for_return.remove(&msg.label) {
            tracing::warn!(
                "WaitForReturn timeout for {} (was on {}) — falling back to Redistribute",
                msg.label,
                ws.failed_node_id,
            );

            if let Some(decision) = placement::select_node(
                state.placement_strategy.as_ref(),
                &ws.spec,
                &state.cluster_view,
            ) {
                let request_id = state.next_request_id();
                state.pending_spawns.insert(
                    request_id,
                    PendingSpawn {
                        spec_label: msg.label.clone(),
                        target_node_id: decision.node_id.clone(),
                        owner: None,
                    },
                );
                state.send_spawn(&decision.node_id, request_id, &ws.spec);
                tracing::info!(
                    "Re-placed {} on {} after timeout ({})",
                    msg.label,
                    decision.node_id,
                    decision.reason
                );
            } else {
                tracing::warn!(
                    "No eligible node for {} after WaitForReturn timeout — spec remains unplaced",
                    msg.label
                );
            }
        }
        // If not in waiting_for_return, the node already returned — timer is a no-op
    }

    #[handler]
    async fn start_singleton(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: StartSingleton,
    ) -> Result<SingletonOwnership, String> {
        if !state.is_leader() {
            return Err(OrchestratorError::NotLeader.to_string());
        }

        let label = msg.spec.label.clone();

        // Resolve the anchor to exactly one owner node.
        let owner = resolve_owner(
            &msg.spec.anchor,
            &state.cluster_view,
            state.election.as_ref(),
        )
        .ok_or_else(|| {
            OrchestratorError::NoEligibleNodes {
                reason: format!("no node satisfies anchor for singleton {label}"),
            }
            .to_string()
        })?;

        // Idempotent re-assert: the same owner is already Active — bump `seq`
        // within the term (liveness renew), do NOT spawn a second instance.
        let reassert = state
            .singletons
            .get(&label)
            .and_then(|rt| rt.ownership.as_ref())
            .is_some_and(|own| {
                own.owner_node_id.as_deref() == Some(owner.as_str())
                    && own.phase == SingletonPhase::Active
            });
        if reassert {
            let gen_source = state.generation_source.clone();
            let generation = gen_source.claim_seq(&label).await?;
            let ownership = SingletonOwnership {
                label: label.clone(),
                owner_node_id: Some(owner),
                generation,
                phase: SingletonPhase::Active,
            };
            if let Some(rt) = state.singletons.get_mut(&label) {
                rt.ownership = Some(ownership.clone());
            }
            tracing::info!("Re-asserted singleton {label} (gen={generation:?})");
            return Ok(ownership);
        }

        // Fresh grant: bump `term`, reset `seq` — a new ownership epoch.
        let gen_source = state.generation_source.clone();
        let generation = gen_source.claim_term(&label, &owner).await?;
        let ownership = SingletonOwnership {
            label: label.clone(),
            owner_node_id: Some(owner.clone()),
            generation,
            // Active is set when the spawn ack lands (notify_spawn_ack).
            phase: SingletonPhase::Starting,
        };
        state.singletons.insert(
            label.clone(),
            SingletonRuntime {
                spec: msg.spec.clone(),
                ownership: Some(ownership.clone()),
            },
        );

        // Spawn the single instance on the owner, stamping the packed generation
        // so the actor's fence rides the same value a downstream compare uses.
        let request_id = state.next_request_id();
        state.pending_spawns.insert(
            request_id,
            PendingSpawn {
                spec_label: label.clone(),
                target_node_id: owner.clone(),
                owner: Some(OwnerKey::Singleton(label.clone())),
            },
        );
        state.send_singleton_spawn(&owner, request_id, &msg.spec, generation.packed());

        tracing::info!("Starting singleton {label} on {owner} (gen={generation:?})");
        Ok(ownership)
    }

    #[handler]
    fn get_singleton(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CoordinatorState,
        msg: GetSingleton,
    ) -> Option<SingletonOwnership> {
        state
            .singletons
            .get(&msg.label)
            .and_then(|rt| rt.ownership.clone())
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
            singletons: HashMap::new(),
            generation_source: Arc::new(CoordinatorGenerationSource::new()),
            next_request_id: 0,
            spawn_sender: None,
        }
    }

    /// Set the spawn sender for sending spawn requests to the transport layer.
    pub(crate) fn with_spawn_sender(mut self, sender: SpawnSender) -> Self {
        self.spawn_sender = Some(sender);
        self
    }

    /// Inject a durable [`GenerationSource`] for minting singleton generations.
    /// Multi-node write deployments MUST set this (the default in-RAM source is
    /// not durable across a Coordinator restart).
    pub fn with_generation_source(mut self, source: Arc<dyn GenerationSource>) -> Self {
        self.generation_source = source;
        self
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

    /// Send a spawn request to the target node via the spawn sender channel.
    fn send_spawn(&self, target_node_id: &str, request_id: u64, spec: &ActorSpec) {
        if let Some(sender) = &self.spawn_sender {
            sender.send_spawn(
                target_node_id,
                SpawnRequest {
                    request_id,
                    label: spec.label.clone(),
                    actor_type_name: spec.actor_type_name.clone(),
                    initial_state: spec.initial_state.clone(),
                    singleton_generation: None,
                },
            );
        } else {
            tracing::warn!(
                "No spawn sender configured — spawn request for {} dropped",
                spec.label
            );
        }
    }

    /// Send a spawn request for a fenced cluster singleton, stamping the packed
    /// `(term, seq)` generation onto the request.
    fn send_singleton_spawn(
        &self,
        target_node_id: &str,
        request_id: u64,
        spec: &SingletonSpec,
        generation: u64,
    ) {
        if let Some(sender) = &self.spawn_sender {
            sender.send_spawn(
                target_node_id,
                SpawnRequest {
                    request_id,
                    label: spec.label.clone(),
                    actor_type_name: spec.actor_type_name.clone(),
                    initial_state: spec.initial_state.clone(),
                    singleton_generation: Some(generation),
                },
            );
        } else {
            tracing::warn!(
                "No spawn sender configured — singleton spawn for {} dropped",
                spec.label
            );
        }
    }

    /// Re-place every singleton that was owned by a node that just left/failed.
    ///
    /// The lost owner is already excluded from the view (`mark_failed` /
    /// `remove_node`), so `resolve_owner` lands on a survivor. Each re-drive
    /// mints a fresh `term` (strictly greater than the lost owner's), so even a
    /// zombie ex-owner that resurfaces is fenced by the `(term, seq)` compare.
    async fn redrive_singletons_after_loss(&mut self, lost_node_id: &str) {
        let affected: Vec<String> = self
            .singletons
            .iter()
            .filter(|(_, rt)| {
                rt.ownership
                    .as_ref()
                    .and_then(|o| o.owner_node_id.as_deref())
                    == Some(lost_node_id)
            })
            .map(|(label, _)| label.clone())
            .collect();

        for label in affected {
            let Some(spec) = self.singletons.get(&label).map(|rt| rt.spec.clone()) else {
                continue;
            };

            let Some(new_owner) =
                resolve_owner(&spec.anchor, &self.cluster_view, self.election.as_ref())
            else {
                tracing::warn!(
                    "Singleton {label} lost owner {lost_node_id} — no eligible node remains"
                );
                if let Some(rt) = self.singletons.get_mut(&label)
                    && let Some(ownership) = &mut rt.ownership
                {
                    ownership.owner_node_id = None;
                }
                continue;
            };

            let gen_source = self.generation_source.clone();
            let generation = match gen_source.claim_term(&label, &new_owner).await {
                Ok(generation) => generation,
                Err(e) => {
                    tracing::error!("claim_term failed re-driving singleton {label}: {e}");
                    continue;
                }
            };

            if let Some(rt) = self.singletons.get_mut(&label) {
                rt.ownership = Some(SingletonOwnership {
                    label: label.clone(),
                    owner_node_id: Some(new_owner.clone()),
                    generation,
                    phase: SingletonPhase::Starting,
                });
            }

            let request_id = self.next_request_id();
            self.pending_spawns.insert(
                request_id,
                PendingSpawn {
                    spec_label: label.clone(),
                    target_node_id: new_owner.clone(),
                    owner: Some(OwnerKey::Singleton(label.clone())),
                },
            );
            self.send_singleton_spawn(&new_owner, request_id, &spec, generation.packed());

            tracing::info!(
                "Re-drove singleton {label} to {new_owner} after loss of {lost_node_id} (gen={generation:?})"
            );
        }
    }

    /// Handle a node departure — shared logic for both failure and graceful leave.
    ///
    /// When `graceful` is true (node left voluntarily), WaitForReturn is
    /// collapsed to Redistribute since the node chose to leave.
    ///
    /// Returns `(label, duration)` pairs for specs that need WaitForReturn timers.
    fn handle_node_departure(
        &mut self,
        node_id: &str,
        graceful: bool,
    ) -> Vec<(String, std::time::Duration)> {
        let mut timers_needed = Vec::new();
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
                        "Waiting {:?} for node {} to return (spec: {label})",
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
                    timers_needed.push((label, *duration));
                }
                _ => {
                    // Redistribute (or WaitForReturn on graceful departure)
                    let reason = if graceful {
                        "graceful departure"
                    } else {
                        "failure"
                    };
                    tracing::info!("Redistributing {label} (node {node_id} {reason})");

                    if let Some(decision) = placement::select_node(
                        self.placement_strategy.as_ref(),
                        &spec,
                        &self.cluster_view,
                    ) {
                        let request_id = self.next_request_id();
                        self.pending_spawns.insert(
                            request_id,
                            PendingSpawn {
                                spec_label: label.clone(),
                                target_node_id: decision.node_id.clone(),
                                owner: None,
                            },
                        );
                        self.send_spawn(&decision.node_id, request_id, &spec);
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

        timers_needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::election::OldestNode;
    use crate::app::placement::LeastLoaded;
    use crate::app::singleton::{SingletonAnchor, SingletonGeneration, SingletonSpec};
    use crate::cluster::config::{NodeClass, NodeIdentity};

    fn make_system_and_coordinator() -> (crate::System, Endpoint<Coordinator>) {
        let system = crate::System::local();

        let alpha_identity = NodeIdentity {
            name: "alpha".into(),
            host: "127.0.0.1".into(),
            port: 7100,
            incarnation: 1,
        };
        let local_node_id = alpha_identity.node_id_string();

        let mut state = CoordinatorState::new(
            &local_node_id,
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        );

        state.cluster_view.upsert_node(NodeInfo::new(
            alpha_identity,
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

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker")
                    .with_crash_strategy(CrashStrategy::Redistribute),
            })
            .await
            .unwrap()
            .unwrap();

        let original_node = result.node_id.clone();

        coordinator
            .send_async(NotifyNodeFailed {
                node_id: original_node.clone(),
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 1);
        assert!(matches!(specs[0].state, SpecState::Pending));

        coordinator
            .send(NotifySpawnAck {
                request_id: 1,
                success: true,
                error: None,
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 1);
        assert!(matches!(specs[0].state, SpecState::Running));
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
            .send_async(NotifyNodeFailed {
                node_id: result.node_id,
            })
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_wait_for_return_timeout_redistributes() {
        let (_system, coordinator) = make_system_and_coordinator();

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker").with_crash_strategy(
                    CrashStrategy::WaitForReturn(std::time::Duration::from_millis(50)),
                ),
            })
            .await
            .unwrap()
            .unwrap();

        let original_node = result.node_id.clone();

        coordinator
            .send_async(NotifyNodeFailed {
                node_id: original_node.clone(),
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert!(matches!(specs[0].state, SpecState::WaitingForReturn));

        coordinator
            .send(WaitForReturnTimeout {
                label: "worker/0".into(),
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 1);
        assert!(!matches!(specs[0].state, SpecState::WaitingForReturn));
    }

    #[tokio::test]
    async fn test_wait_for_return_node_rejoins() {
        let (_system, coordinator) = make_system_and_coordinator();

        let result = coordinator
            .send(SubmitSpec {
                spec: ActorSpec::new("worker/0", "app::Worker").with_crash_strategy(
                    CrashStrategy::WaitForReturn(std::time::Duration::from_secs(60)),
                ),
            })
            .await
            .unwrap()
            .unwrap();

        let original_node = result.node_id.clone();

        coordinator
            .send_async(NotifyNodeFailed {
                node_id: original_node.clone(),
            })
            .await
            .unwrap();

        coordinator
            .send(NotifyNodeJoined {
                node_id: original_node.clone(),
                info: SerializableNodeInfo {
                    name: "alpha".into(),
                    host: "127.0.0.1".into(),
                    port: 7100,
                    incarnation: 1,
                    class: NodeClass::Worker,
                    metadata: HashMap::new(),
                },
            })
            .await
            .unwrap();

        let specs = coordinator.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 1);
        assert!(matches!(specs[0].state, SpecState::Pending));
    }

    // ── Cluster singletons (M4 step 3) ──────────────────────────────────────
    // The harness configures no spawn_sender, so the spawn warn-drops and the
    // ack must be injected manually; the first allocated request_id is 0.

    #[tokio::test]
    async fn test_start_singleton_grants_ownership_on_leader() {
        let (_system, coordinator) = make_system_and_coordinator();

        let ownership = coordinator
            .send_async(StartSingleton {
                spec: SingletonSpec::new("catalog", "app::Catalog", SingletonAnchor::Leader),
            })
            .await
            .unwrap()
            .unwrap();

        // Leader (oldest = alpha, incarnation 1) owns it; first grant is term 1.
        assert!(
            ownership
                .owner_node_id
                .as_deref()
                .unwrap()
                .contains("alpha"),
            "leader anchor should resolve to alpha, got {:?}",
            ownership.owner_node_id
        );
        assert_eq!(
            ownership.generation,
            SingletonGeneration { term: 1, seq: 0 }
        );
        // Active only lands once the spawn ack is observed.
        assert_eq!(ownership.phase, SingletonPhase::Starting);

        let queried = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queried.generation, ownership.generation);
    }

    #[tokio::test]
    async fn test_singleton_ack_promotes_to_active() {
        let (_system, coordinator) = make_system_and_coordinator();

        coordinator
            .send_async(StartSingleton {
                spec: SingletonSpec::new("catalog", "app::Catalog", SingletonAnchor::Leader),
            })
            .await
            .unwrap()
            .unwrap();

        // Simulate the owner's spawn ack (request_id 0 is the first allocated).
        coordinator
            .send(NotifySpawnAck {
                request_id: 0,
                success: true,
                error: None,
            })
            .await
            .unwrap();

        let queried = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queried.phase, SingletonPhase::Active);
        assert_eq!(queried.generation, SingletonGeneration { term: 1, seq: 0 });
    }

    #[tokio::test]
    async fn test_start_singleton_idempotent_reasserts_with_seq_bump() {
        let (_system, coordinator) = make_system_and_coordinator();

        let spec = SingletonSpec::new("catalog", "app::Catalog", SingletonAnchor::Leader);
        coordinator
            .send_async(StartSingleton { spec: spec.clone() })
            .await
            .unwrap()
            .unwrap();
        coordinator
            .send(NotifySpawnAck {
                request_id: 0,
                success: true,
                error: None,
            })
            .await
            .unwrap();

        // Re-assert against the same Active owner: bump seq within the term,
        // no new ownership epoch, no second spawn.
        let reasserted = coordinator
            .send_async(StartSingleton { spec })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reasserted.generation,
            SingletonGeneration { term: 1, seq: 1 }
        );
        assert_eq!(reasserted.phase, SingletonPhase::Active);
    }

    #[tokio::test]
    async fn test_start_singleton_unsatisfiable_anchor_errors() {
        let (_system, coordinator) = make_system_and_coordinator();

        let result = coordinator
            .send_async(StartSingleton {
                spec: SingletonSpec::new(
                    "catalog",
                    "app::Catalog",
                    SingletonAnchor::Node("ghost@127.0.0.1:9999#0".into()),
                ),
            })
            .await
            .unwrap();
        assert!(result.is_err(), "pinning to a missing node should fail");

        // And nothing was registered.
        let queried = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap();
        assert!(queried.is_none());
    }

    #[tokio::test]
    async fn test_singleton_redrives_on_owner_failure_with_higher_term() {
        let (_system, coordinator) = make_system_and_coordinator();

        coordinator
            .send_async(StartSingleton {
                spec: SingletonSpec::new("catalog", "app::Catalog", SingletonAnchor::Leader),
            })
            .await
            .unwrap()
            .unwrap();
        coordinator
            .send(NotifySpawnAck {
                request_id: 0,
                success: true,
                error: None,
            })
            .await
            .unwrap();

        let before = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        let original_owner = before.owner_node_id.clone().unwrap();
        assert!(original_owner.contains("alpha"));
        assert_eq!(before.generation, SingletonGeneration { term: 1, seq: 0 });

        // The owner fails: re-drive to the survivor with a strictly-higher term
        // (the fence), Starting until the re-spawn acks.
        coordinator
            .send_async(NotifyNodeFailed {
                node_id: original_owner.clone(),
            })
            .await
            .unwrap();

        let after = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            after.owner_node_id.as_deref().unwrap().contains("beta"),
            "must move off the failed node to beta, got {:?}",
            after.owner_node_id
        );
        assert_eq!(after.generation, SingletonGeneration { term: 2, seq: 0 });
        assert!(
            after.generation > before.generation,
            "term must strictly increase across failover (the fence)"
        );
        assert_eq!(after.phase, SingletonPhase::Starting);

        // The re-drive's spawn (request_id 1) acks -> Active.
        coordinator
            .send(NotifySpawnAck {
                request_id: 1,
                success: true,
                error: None,
            })
            .await
            .unwrap();
        let active = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.phase, SingletonPhase::Active);
    }

    #[tokio::test]
    async fn test_singleton_node_anchor_owner_loss_clears_owner() {
        let (_system, coordinator) = make_system_and_coordinator();
        let alpha_id = NodeIdentity {
            name: "alpha".into(),
            host: "127.0.0.1".into(),
            port: 7100,
            incarnation: 1,
        }
        .node_id_string();

        coordinator
            .send_async(StartSingleton {
                spec: SingletonSpec::new(
                    "catalog",
                    "app::Catalog",
                    SingletonAnchor::Node(alpha_id.clone()),
                ),
            })
            .await
            .unwrap()
            .unwrap();
        coordinator
            .send(NotifySpawnAck {
                request_id: 0,
                success: true,
                error: None,
            })
            .await
            .unwrap();

        // The pinned node fails and the Node anchor cannot resolve elsewhere —
        // ownership is cleared (no silent relocation off the pin).
        coordinator
            .send_async(NotifyNodeFailed { node_id: alpha_id })
            .await
            .unwrap();

        let after = coordinator
            .send(GetSingleton {
                label: "catalog".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            after.owner_node_id.is_none(),
            "a Node-pinned singleton whose anchor died has no owner, got {:?}",
            after.owner_node_id
        );
    }
}
