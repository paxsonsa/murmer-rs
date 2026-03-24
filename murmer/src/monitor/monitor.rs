//! ClusterMonitor actor — real-time cluster health tracking.
//!
//! The monitor subscribes to cluster events (via the bridge) and maintains
//! cumulative counters and current state. It can be queried at any time
//! for a health snapshot.

use std::collections::HashMap;
use std::time::Instant;

use crate::cluster::ClusterSystem;
use crate::cluster::membership::ClusterEvent;
use crate::prelude::*;
use serde::{Deserialize, Serialize};

use crate::monitor::events;

// =============================================================================
// MONITOR ACTOR
// =============================================================================

/// A monitoring actor that tracks cluster health and lifecycle events.
#[derive(Debug)]
pub struct ClusterMonitor;

/// State for the ClusterMonitor.
pub struct ClusterMonitorState {
    /// Tracks which nodes are alive.
    alive_nodes: HashMap<String, NodeRecord>,
    /// Cumulative event counters.
    counters: EventCounters,
    /// When the monitor started.
    started_at: Instant,
}

/// Record of a live node.
#[derive(Debug, Clone)]
struct NodeRecord {
    name: String,
    class: String,
    joined_at: Instant,
}

/// Cumulative counters for cluster lifecycle events.
#[derive(Debug, Clone, Default)]
struct EventCounters {
    nodes_joined: u64,
    nodes_failed: u64,
    nodes_left: u64,
}

impl Actor for ClusterMonitor {
    type State = ClusterMonitorState;
}

impl ClusterMonitorState {
    pub fn new() -> Self {
        Self {
            alive_nodes: HashMap::new(),
            counters: EventCounters::default(),
            started_at: Instant::now(),
        }
    }
}

impl Default for ClusterMonitorState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// MESSAGES
// =============================================================================

/// Internal: a cluster event forwarded by the bridge.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "monitor::MonitorNodeJoined")]
pub struct MonitorNodeJoined {
    pub node_id: String,
    pub name: String,
    pub class: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "monitor::MonitorNodeFailed")]
pub struct MonitorNodeFailed {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "monitor::MonitorNodeLeft")]
pub struct MonitorNodeLeft {
    pub node_id: String,
}

/// Query: get a snapshot of cluster health.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = ClusterHealth, remote = "monitor::GetClusterHealth")]
pub struct GetClusterHealth;

/// Response: cluster health snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    pub alive_nodes: usize,
    pub uptime_secs: u64,
    pub total_joins: u64,
    pub total_failures: u64,
    pub total_departures: u64,
    pub nodes: Vec<NodeHealth>,
}

/// Health info for a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: String,
    pub name: String,
    pub class: String,
    pub uptime_secs: u64,
}

// =============================================================================
// HANDLERS
// =============================================================================

#[handlers]
impl ClusterMonitor {
    #[handler]
    fn monitor_node_joined(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ClusterMonitorState,
        msg: MonitorNodeJoined,
    ) {
        events::trace_node_joined(&msg.node_id, &msg.name, &msg.class);

        state.alive_nodes.insert(
            msg.node_id,
            NodeRecord {
                name: msg.name,
                class: msg.class,
                joined_at: Instant::now(),
            },
        );
        state.counters.nodes_joined += 1;
    }

    #[handler]
    fn monitor_node_failed(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ClusterMonitorState,
        msg: MonitorNodeFailed,
    ) {
        events::trace_node_failed(&msg.node_id);
        state.alive_nodes.remove(&msg.node_id);
        state.counters.nodes_failed += 1;
    }

    #[handler]
    fn monitor_node_left(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ClusterMonitorState,
        msg: MonitorNodeLeft,
    ) {
        events::trace_node_left(&msg.node_id);
        state.alive_nodes.remove(&msg.node_id);
        state.counters.nodes_left += 1;
    }

    #[handler]
    fn get_cluster_health(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut ClusterMonitorState,
        _msg: GetClusterHealth,
    ) -> ClusterHealth {
        let now = Instant::now();

        let nodes = state
            .alive_nodes
            .iter()
            .map(|(id, record)| NodeHealth {
                node_id: id.clone(),
                name: record.name.clone(),
                class: record.class.clone(),
                uptime_secs: now.duration_since(record.joined_at).as_secs(),
            })
            .collect();

        let health = ClusterHealth {
            alive_nodes: state.alive_nodes.len(),
            uptime_secs: now.duration_since(state.started_at).as_secs(),
            total_joins: state.counters.nodes_joined,
            total_failures: state.counters.nodes_failed,
            total_departures: state.counters.nodes_left,
            nodes,
        };

        events::trace_cluster_health(
            health.alive_nodes,
            health.total_joins as usize,
            0, // actor count would come from coordinator
        );

        health
    }
}

// =============================================================================
// MONITOR BRIDGE — connects ClusterSystem events to the monitor actor
// =============================================================================

/// Run the monitor bridge loop.
///
/// Subscribes to `ClusterEvent`s from the cluster system and forwards
/// them to the `ClusterMonitor` actor as typed messages.
pub async fn run_monitor_bridge(cluster: &ClusterSystem, monitor: Endpoint<ClusterMonitor>) {
    let mut events = cluster.subscribe_events();

    loop {
        match events.recv().await {
            Ok(event) => match event {
                ClusterEvent::NodeJoined(identity) => {
                    let _ = monitor
                        .send(MonitorNodeJoined {
                            node_id: identity.node_id_string(),
                            name: identity.name,
                            class: "Worker".into(), // TODO: look up from NodeRegistry
                        })
                        .await;
                }
                ClusterEvent::NodeFailed(identity) => {
                    let _ = monitor
                        .send(MonitorNodeFailed {
                            node_id: identity.node_id_string(),
                        })
                        .await;
                }
                ClusterEvent::NodeLeft(identity) => {
                    let _ = monitor
                        .send(MonitorNodeLeft {
                            node_id: identity.node_id_string(),
                        })
                        .await;
                }
                _ => {}
            },
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("Monitor bridge lagged, missed {n} events");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("Cluster event channel closed — monitor bridge shutting down");
                break;
            }
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_monitor_tracks_nodes() {
        let system = crate::System::local();
        let monitor = system.start("murmer/monitor", ClusterMonitor, ClusterMonitorState::new());

        // Simulate nodes joining
        monitor
            .send(MonitorNodeJoined {
                node_id: "alpha@127.0.0.1:7100#1".into(),
                name: "alpha".into(),
                class: "Worker".into(),
            })
            .await
            .unwrap();

        monitor
            .send(MonitorNodeJoined {
                node_id: "beta@127.0.0.1:7200#2".into(),
                name: "beta".into(),
                class: "Worker".into(),
            })
            .await
            .unwrap();

        let health = monitor.send(GetClusterHealth).await.unwrap();
        assert_eq!(health.alive_nodes, 2);
        assert_eq!(health.total_joins, 2);
        assert_eq!(health.total_failures, 0);
    }

    #[tokio::test]
    async fn test_monitor_tracks_failures() {
        let system = crate::System::local();
        let monitor = system.start("murmer/monitor", ClusterMonitor, ClusterMonitorState::new());

        monitor
            .send(MonitorNodeJoined {
                node_id: "alpha@127.0.0.1:7100#1".into(),
                name: "alpha".into(),
                class: "Worker".into(),
            })
            .await
            .unwrap();

        monitor
            .send(MonitorNodeFailed {
                node_id: "alpha@127.0.0.1:7100#1".into(),
            })
            .await
            .unwrap();

        let health = monitor.send(GetClusterHealth).await.unwrap();
        assert_eq!(health.alive_nodes, 0);
        assert_eq!(health.total_joins, 1);
        assert_eq!(health.total_failures, 1);
    }

    #[tokio::test]
    async fn test_monitor_tracks_departures() {
        let system = crate::System::local();
        let monitor = system.start("murmer/monitor", ClusterMonitor, ClusterMonitorState::new());

        monitor
            .send(MonitorNodeJoined {
                node_id: "alpha@127.0.0.1:7100#1".into(),
                name: "alpha".into(),
                class: "Worker".into(),
            })
            .await
            .unwrap();

        monitor
            .send(MonitorNodeLeft {
                node_id: "alpha@127.0.0.1:7100#1".into(),
            })
            .await
            .unwrap();

        let health = monitor.send(GetClusterHealth).await.unwrap();
        assert_eq!(health.alive_nodes, 0);
        assert_eq!(health.total_departures, 1);
    }
}
