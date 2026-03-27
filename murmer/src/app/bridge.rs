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

use crate::cluster::ClusterSystem;
use crate::cluster::framing::ControlMessage;
use crate::cluster::membership::ClusterEvent;
use crate::cluster::transport::Transport;
use crate::prelude::*;
use tokio::sync::mpsc;

use crate::app::coordinator::{
    Coordinator, CoordinatorState, NotifyNodeFailed, NotifyNodeJoined, NotifyNodeLeft,
    NotifySpawnAck, SerializableNodeInfo,
};
use crate::app::node_info::NodeInfo;
use crate::app::spawn_sender::SpawnSender;

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

    let (spawn_tx, spawn_rx) = mpsc::unbounded_channel();
    let state = state.with_spawn_sender(SpawnSender::new(spawn_tx));

    let coordinator_ep = cluster.start_actor("coordinator", Coordinator, state);

    // Spawn bridge (ClusterEvents → Coordinator)
    let bridge_ep = coordinator_ep.clone();
    let mut events = cluster.subscribe_events();
    let node_registry = cluster.node_registry().clone();
    tokio::spawn(async move {
        run_bridge_loop(&mut events, &node_registry, &bridge_ep).await;
    });

    // Spawn drain loop (SpawnRequests → transport or local spawn)
    let transport = Arc::clone(cluster.transport());
    let local_node_id = cluster.identity().node_id_string();
    let spawn_registry = Arc::clone(cluster.spawn_registry());
    let receptionist = cluster.receptionist().clone();
    let ack_ep = coordinator_ep.clone();
    tokio::spawn(run_spawn_drain_loop(
        transport,
        local_node_id,
        spawn_registry,
        receptionist,
        ack_ep,
        spawn_rx,
    ));

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
                    let _ = coordinator
                        .send(NotifyNodeFailed {
                            node_id: identity.node_id_string(),
                        })
                        .await;
                }
                ClusterEvent::NodeLeft(identity) => {
                    let _ = coordinator
                        .send(NotifyNodeLeft {
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

/// Drain loop: reads spawn requests from the channel and either spawns locally
/// (if target == local node) or sends a `SpawnActor` control message via transport.
async fn run_spawn_drain_loop(
    transport: Arc<Transport>,
    local_node_id: String,
    spawn_registry: Arc<crate::cluster::sync::SpawnRegistry>,
    receptionist: crate::receptionist::Receptionist,
    coordinator: Endpoint<Coordinator>,
    mut rx: mpsc::UnboundedReceiver<(String, crate::cluster::framing::SpawnRequest)>,
) {
    while let Some((node_id, request)) = rx.recv().await {
        if node_id == local_node_id {
            // Local spawn — bypass transport, invoke spawn registry directly
            tracing::debug!(
                "Spawning actor locally: label={}, type={}",
                request.label,
                request.actor_type_name
            );
            let result = spawn_registry.spawn(
                &receptionist,
                &request.label,
                &request.actor_type_name,
                &request.initial_state,
            );
            let _ = coordinator
                .send(NotifySpawnAck {
                    request_id: request.request_id,
                    success: result.is_ok(),
                    error: result.err().map(|e| e.to_string()),
                })
                .await;
        } else {
            tracing::debug!(
                "Sending SpawnActor to {node_id}: label={}, type={}",
                request.label,
                request.actor_type_name
            );
            if let Err(e) = transport
                .send_control(&node_id, ControlMessage::SpawnActor(request))
                .await
            {
                tracing::warn!("Failed to send spawn request to {node_id}: {e}");
            }
        }
    }
}
