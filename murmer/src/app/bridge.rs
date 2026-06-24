//! Cluster event bridge — connects the murmer cluster layer to the Coordinator.
//!
//! The bridge subscribes to [`ClusterEvent`]s from the cluster system and
//! translates them into Coordinator messages (`NotifyNodeJoined`,
//! `NotifyNodeFailed`, `NotifyNodeLeft`, `NotifySpawnAck`). This keeps the
//! Coordinator decoupled from the raw cluster machinery.
//!
//! # Usage
//!
//! Use [`start_coordinator`] to set up the Coordinator with its bridge and
//! spawn sender in one call:
//!
//! ```rust,ignore
//! let coordinator_ep = start_coordinator(&cluster, state).await;
//! coordinator_ep.send(SubmitSpec { spec }).await?;
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cluster::ClusterSystem;
use crate::cluster::framing::ControlMessage;
use crate::cluster::membership::ClusterEvent;
use crate::cluster::net::Net;
use crate::prelude::*;
use crate::runtime::Runtime;
use tokio::sync::mpsc;

use crate::app::coordinator::{
    Coordinator, CoordinatorState, NotifyNodeFailed, NotifyNodeJoined, NotifyNodeLeft,
    NotifySingletonStopped, NotifySpawnAck, SerializableNodeInfo,
};
use crate::app::node_info::NodeInfo;
use crate::app::singleton::SingletonGeneration;
use crate::app::spawn_sender::{SingletonStopRequest, SingletonStopSender, SpawnSender};

/// Start the Coordinator actor and the cluster bridge.
///
/// This is the recommended way to set up orchestration. It:
/// 1. Auto-registers the local node in the coordinator's ClusterView
/// 2. Creates a SpawnSender channel
/// 3. Starts the Coordinator actor with the sender wired in
/// 4. Spawns the bridge loop (ClusterEvents → Coordinator messages)
/// 5. Spawns the spawn drain loop (SpawnRequests → transport.send_control)
///
/// Returns the Coordinator endpoint for submitting specs.
///
/// The local node auto-registration means a single-node cluster is fully
/// functional immediately: `is_leader()` returns `true` and `SubmitSpec`
/// can place actors without waiting for any `NodeJoined` events.
pub fn start_coordinator(
    cluster: &ClusterSystem,
    mut state: CoordinatorState,
) -> Endpoint<Coordinator> {
    // A node always knows about itself — auto-insert so single-node clusters
    // work without manual NotifyNodeJoined calls and the coordinator sees
    // itself as leader immediately.
    let local_info = NodeInfo::new(
        cluster.identity().clone(),
        cluster.node_class().clone(),
        cluster.node_metadata().clone(),
    );
    state.cluster_view.upsert_node(local_info);

    let queue_depth = Arc::new(AtomicUsize::new(0));
    let (spawn_tx, spawn_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = mpsc::unbounded_channel();
    let state = state
        .with_spawn_sender(SpawnSender::new(spawn_tx, Arc::clone(&queue_depth)))
        .with_singleton_stop_sender(SingletonStopSender::new(stop_tx));

    let coordinator_ep = cluster.start_actor("coordinator", Coordinator, state);

    // The runtime seam (Tokio in prod, the deterministic SimRuntime under
    // simulation). The bridge's background loops route through it so the whole
    // app orchestration can boot under `SimWorld` — `runtime.spawn` is an
    // `#[inline]` passthrough to `tokio::spawn` in production.
    let runtime = cluster.receptionist().runtime().clone();

    // Spawn bridge (ClusterEvents → Coordinator)
    let bridge_ep = coordinator_ep.clone();
    let mut events = cluster.subscribe_events();
    let node_registry = cluster.node_registry().clone();
    runtime.spawn(Box::pin(async move {
        run_bridge_loop(&mut events, &node_registry, &bridge_ep).await;
    }));

    // Spawn drain loop (SpawnRequests → net or local spawn)
    let net = Arc::clone(cluster.net());
    let local_node_id = cluster.identity().node_id_string();
    let spawn_registry = Arc::clone(cluster.spawn_registry());
    let receptionist = cluster.receptionist().clone();
    let ack_ep = coordinator_ep.clone();
    runtime.spawn(Box::pin(run_spawn_drain_loop(
        Arc::clone(&runtime),
        net,
        local_node_id,
        spawn_registry,
        receptionist,
        ack_ep,
        spawn_rx,
        queue_depth,
    )));

    // Drain loop for graceful singleton stops (the inner half of the drain handoff).
    let stop_net = Arc::clone(cluster.net());
    let stop_local_node_id = cluster.identity().node_id_string();
    let stop_receptionist = cluster.receptionist().clone();
    let stop_ep = coordinator_ep.clone();
    runtime.spawn(Box::pin(run_singleton_stop_drain_loop(
        stop_net,
        stop_local_node_id,
        stop_receptionist,
        stop_ep,
        stop_rx,
    )));

    coordinator_ep
}

/// Bridge loop: reads ClusterEvents and forwards them to the Coordinator.
async fn run_bridge_loop(
    events: &mut tokio::sync::broadcast::Receiver<ClusterEvent>,
    node_registry: &crate::cluster::NodeRegistry,
    coordinator: &Endpoint<Coordinator>,
) {
    loop {
        match events.recv().await {
            Ok(event) => match event {
                ClusterEvent::NodeJoined(identity) => {
                    let node_id = identity.node_id_string();

                    let (class, metadata) = match node_registry.get(&node_id) {
                        Some(entry) => (entry.class, entry.metadata),
                        None => {
                            tracing::warn!(
                                "Node {} joined but no registry entry found — using defaults",
                                node_id
                            );
                            (
                                crate::cluster::config::NodeClass::Worker,
                                std::collections::HashMap::new(),
                            )
                        }
                    };

                    let _ = coordinator
                        .send(NotifyNodeJoined {
                            node_id,
                            info: SerializableNodeInfo {
                                name: identity.name,
                                endpoint_id: identity.endpoint_id,
                                host: identity.host,
                                port: identity.port,
                                incarnation: identity.incarnation,
                                class,
                                metadata,
                            },
                        })
                        .await;
                }
                ClusterEvent::NodeFailed(identity) => {
                    // async handler (re-drives owned singletons) — use send_async.
                    let _ = coordinator
                        .send_async(NotifyNodeFailed {
                            node_id: identity.node_id_string(),
                        })
                        .await;
                }
                ClusterEvent::NodeLeft(identity) => {
                    let _ = coordinator
                        .send_async(NotifyNodeLeft {
                            node_id: identity.node_id_string(),
                        })
                        .await;
                }
                ClusterEvent::SpawnAckOk { request_id, .. } => {
                    let _ = coordinator
                        .send(NotifySpawnAck {
                            request_id,
                            success: true,
                            error: None,
                        })
                        .await;
                }
                ClusterEvent::SpawnAckErr { request_id, error } => {
                    let _ = coordinator
                        .send(NotifySpawnAck {
                            request_id,
                            success: false,
                            error: Some(error),
                        })
                        .await;
                }
                ClusterEvent::SingletonStopped {
                    label,
                    stopped_generation,
                } => {
                    // A drained owner confirmed it stopped — advance the handoff.
                    let _ = coordinator
                        .send(NotifySingletonStopped {
                            label,
                            stopped_generation: SingletonGeneration::from_packed(
                                stopped_generation,
                            ),
                        })
                        .await;
                }
                // NodePruned and actor events are informational — the Coordinator
                // handles its own bookkeeping when it processes the join/fail/leave.
                _ => {}
            },
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("Cluster bridge lagged, missed {n} events");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("Cluster event channel closed — bridge shutting down");
                break;
            }
        }
    }
}

/// RAII guard that guarantees a `NotifySpawnAck` is always delivered.
///
/// On the happy path call `ack(success, error)` to consume the guard and send
/// the ack. If the task holding the guard panics or is cancelled before
/// reaching `ack`, `Drop` fires a detached task to send a failure ack so
/// `pending_spawns` in the Coordinator never leaks a stale entry.
struct AckGuard {
    runtime: Arc<dyn Runtime>,
    coordinator: Endpoint<Coordinator>,
    request_id: u64,
    sent: bool,
}

impl AckGuard {
    fn new(runtime: Arc<dyn Runtime>, coordinator: Endpoint<Coordinator>, request_id: u64) -> Self {
        Self {
            runtime,
            coordinator,
            request_id,
            sent: false,
        }
    }

    fn ack(mut self, success: bool, error: Option<String>) {
        self.sent = true;
        let coord = self.coordinator.clone();
        let id = self.request_id;
        self.runtime.spawn(Box::pin(async move {
            let _ = coord
                .send(NotifySpawnAck {
                    request_id: id,
                    success,
                    error,
                })
                .await;
        }));
    }
}

impl Drop for AckGuard {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        // Guard against spawning into a dead runtime (prod teardown), via the
        // seam's `can_spawn` rather than `tokio::runtime::Handle`. Under sim
        // `can_spawn` is true, so the failure-ack IS delivered — previously the
        // raw Handle check early-returned here and silently swallowed it, leaking
        // the Coordinator's `pending_spawns` entry, so a sim test driving a
        // panicking factory hung instead of seeing the failure ack.
        if !self.runtime.can_spawn() {
            return;
        }
        let coord = self.coordinator.clone();
        let id = self.request_id;
        self.runtime.spawn(Box::pin(async move {
            let _ = coord
                .send(NotifySpawnAck {
                    request_id: id,
                    success: false,
                    error: Some("spawn task panicked or was cancelled".into()),
                })
                .await;
        }));
    }
}

/// Drain loop: reads spawn requests from the channel and dispatches each one
/// as an independent `tokio::spawn` task so factories run concurrently.
///
/// Requests are dequeued in receive order but execute concurrently — acks may
/// arrive at the Coordinator in any order (which is fine: `notify_spawn_ack`
/// keys on `request_id`). A panicking or cancelled factory task delivers a
/// failure ack via `AckGuard::drop` so `pending_spawns` always converges.
///
/// Fan-out is naturally bounded by the upstream admission control on the
/// caller's side (e.g. the supervisor semaphore in datastorekit). Murmer does
/// not impose its own cap.
#[allow(clippy::too_many_arguments)]
async fn run_spawn_drain_loop(
    runtime: Arc<dyn Runtime>,
    net: Arc<dyn Net>,
    local_node_id: String,
    spawn_registry: Arc<crate::cluster::sync::SpawnRegistry>,
    receptionist: crate::receptionist::Receptionist,
    coordinator: Endpoint<Coordinator>,
    mut rx: mpsc::UnboundedReceiver<crate::app::spawn_sender::SpawnItem>,
    queue_depth: Arc<AtomicUsize>,
) {
    while let Some((node_id, request, enqueued_at)) = rx.recv().await {
        let depth = queue_depth
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        crate::instrument::spawn_drain_queue_depth(depth as f64);
        crate::instrument::spawn_drain_dispatch(enqueued_at.elapsed());

        if node_id == local_node_id {
            tracing::debug!(
                "Spawning actor locally: label={}, type={}",
                request.label,
                request.actor_type_name
            );
            let registry = Arc::clone(&spawn_registry);
            let receptionist = receptionist.clone();
            let guard = AckGuard::new(
                Arc::clone(&runtime),
                coordinator.clone(),
                request.request_id,
            );
            runtime.spawn(Box::pin(async move {
                let factory_start = std::time::Instant::now(); // determinism-gate: allow — monitor instrumentation (spawn_drain_factory timing, never feeds control flow)
                let result = registry
                    .spawn(
                        receptionist,
                        &request.label,
                        &request.actor_type_name,
                        &request.initial_state,
                    )
                    .await;
                crate::instrument::spawn_drain_factory("local", factory_start.elapsed());
                match result {
                    Ok(()) => guard.ack(true, None),
                    Err(e) => guard.ack(false, Some(e.to_string())),
                }
            }));
        } else {
            tracing::debug!(
                "Sending SpawnActor to {node_id}: label={}, type={}",
                request.label,
                request.actor_type_name
            );
            let net = Arc::clone(&net);
            runtime.spawn(Box::pin(async move {
                let factory_start = std::time::Instant::now(); // determinism-gate: allow — monitor instrumentation (spawn_drain_factory timing, never feeds control flow)
                if let Err(e) = net
                    .send_control(&node_id, ControlMessage::SpawnActor(request))
                    .await
                {
                    tracing::warn!("Failed to send spawn request to {node_id}: {e}");
                }
                crate::instrument::spawn_drain_factory("remote", factory_start.elapsed());
            }));
        }
    }
}

/// Drain loop for graceful singleton stops: reads stop requests and either stops
/// a local instance directly (then self-notifies the Coordinator) or sends a
/// `StopSingleton` control message to the remote owner (whose ack returns as
/// `ClusterEvent::SingletonStopped`).
async fn run_singleton_stop_drain_loop(
    net: Arc<dyn Net>,
    local_node_id: String,
    receptionist: crate::receptionist::Receptionist,
    coordinator: Endpoint<Coordinator>,
    mut rx: mpsc::UnboundedReceiver<SingletonStopRequest>,
) {
    while let Some(req) = rx.recv().await {
        if req.target_node_id == local_node_id {
            tracing::debug!("Stopping local singleton {} for handoff", req.label);
            receptionist.stop(&req.label);
            let _ = coordinator
                .send(NotifySingletonStopped {
                    label: req.label,
                    stopped_generation: SingletonGeneration::from_packed(req.generation),
                })
                .await;
        } else {
            tracing::debug!(
                "Sending StopSingleton to {} for {}",
                req.target_node_id,
                req.label
            );
            if let Err(e) = net
                .send_control(
                    &req.target_node_id,
                    ControlMessage::StopSingleton {
                        label: req.label.clone(),
                        generation: req.generation,
                    },
                )
                .await
            {
                tracing::warn!(
                    "Failed to send StopSingleton to {}: {e}",
                    req.target_node_id
                );
            }
        }
    }
}
