//! Monitoring and observability for murmer actor systems.
//!
//! This module provides two layers of observability:
//!
//! 1. **Metrics** (via the [`metrics`] crate facade) — Prometheus-compatible counters,
//!    gauges, and histograms covering actor lifecycle, message processing, networking,
//!    and cluster membership. These are recorded inline via [`crate::instrument`] and
//!    require no actor — just install a metrics recorder.
//!
//! 2. **ClusterMonitor actor** — An actor that subscribes to cluster events and
//!    maintains a queryable health snapshot (alive nodes, cumulative counters).
//!
//! Enable with `murmer = { features = ["monitor"] }`.
//!
//! # Quick start — Prometheus metrics
//!
//! ```rust,ignore
//! use murmer::monitor::start_prometheus;
//!
//! // One call installs the recorder and starts HTTP on :9000
//! start_prometheus(9000).expect("failed to start prometheus");
//! // curl localhost:9000/metrics → all murmer_* metrics
//! ```
//!
//! # Quick start — ClusterMonitor actor
//!
//! ```rust,ignore
//! use murmer::monitor::{ClusterMonitor, ClusterMonitorState, run_monitor_bridge};
//!
//! let monitor = system.start(
//!     "murmer/monitor",
//!     ClusterMonitor,
//!     ClusterMonitorState::new(),
//! );
//! tokio::spawn(run_monitor_bridge(&cluster_system, monitor.clone()));
//!
//! let health = monitor.send(GetClusterHealth).await?;
//! ```

pub mod events;
pub mod monitor;
pub mod prometheus_setup;

pub use events::*;
pub use monitor::*;
pub use prometheus_setup::start_prometheus;
