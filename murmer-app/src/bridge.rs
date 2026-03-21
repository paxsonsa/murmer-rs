//! Cluster event bridge — connects the murmer cluster layer to the Coordinator.
//!
//! The bridge subscribes to [`ClusterEvent`]s from the cluster system and
//! translates them into Coordinator messages (`NotifyNodeJoined`,
//! `NotifyNodeFailed`, `NotifyNodeLeft`). This keeps the Coordinator
//! decoupled from the raw cluster machinery.
//!
//! # Usage
//!
//! ```rust,ignore
//! let coordinator_ep = system.start("coordinator", Coordinator, state);
//! let handle = tokio::spawn(run_cluster_bridge(cluster_system, coordinator_ep));
//! ```

use murmer::cluster::ClusterSystem;
use murmer::cluster::membership::ClusterEvent;
use murmer::prelude::*;

use crate::coordinator::{
    Coordinator, NotifyNodeFailed, NotifyNodeJoined, NotifyNodeLeft, SerializableNodeInfo,
};

/// Run the cluster event bridge loop.
///
/// Subscribes to `ClusterEvent`s from the cluster system and forwards
/// them to the Coordinator actor. Returns when the event channel closes
/// (i.e., the cluster system shuts down).
pub async fn run_cluster_bridge(cluster: &ClusterSystem, coordinator: Endpoint<Coordinator>) {
    let mut events = cluster.subscribe_events();
    let node_registry = cluster.node_registry().clone();

    loop {
        match events.recv().await {
            Ok(event) => match event {
                ClusterEvent::NodeJoined(identity) => {
                    let node_id = identity.node_id_string();

                    // Look up class and metadata from the node registry
                    // (populated during QUIC handshake)
                    let (class, metadata) = match node_registry.get(&node_id) {
                        Some(entry) => (entry.class, entry.metadata),
                        None => {
                            tracing::warn!(
                                "Node {} joined but no registry entry found — using defaults",
                                node_id
                            );
                            (
                                murmer::cluster::config::NodeClass::Worker,
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
