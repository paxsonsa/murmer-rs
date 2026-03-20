//! murmer-monitor — cluster monitoring and observability for murmer.
//!
//! This crate provides a [`ClusterMonitor`] actor that subscribes to cluster
//! events and maintains real-time metrics about the cluster. It emits
//! structured tracing events for each lifecycle transition, making it easy
//! to integrate with any tracing subscriber (stdout, JSON, OpenTelemetry, etc.).
//!
//! # Quick start
//!
//! ```rust,ignore
//! use murmer_monitor::{ClusterMonitor, ClusterMonitorState, run_monitor_bridge};
//!
//! // Start the monitor on your system
//! let monitor = system.start(
//!     "murmer/monitor",
//!     ClusterMonitor,
//!     ClusterMonitorState::new(),
//! );
//!
//! // Bridge cluster events into the monitor
//! tokio::spawn(run_monitor_bridge(&cluster_system, monitor.clone()));
//!
//! // Query health at any time
//! let health = monitor.send(GetClusterHealth).await?;
//! println!("Cluster: {} alive, {} total", health.alive_nodes, health.total_nodes);
//! ```

pub mod events;
pub mod monitor;

pub use events::*;
pub use monitor::*;
