//! murmer-app — orchestration layer for murmer actor systems.
//!
//! This crate provides the application-level abstractions for managing actors
//! across a cluster: placement strategies, leader election, crash recovery,
//! and the Coordinator actor that ties them together.
//!
//! # Modules
//!
//! - [`spec`] — Actor specifications and crash strategies
//! - [`node_info`] — Cluster view and node information
//! - [`placement`] — Pluggable placement strategies (LeastLoaded, Random, Pinned)
//! - [`election`] — Pluggable leader election (default: OldestNode)
//! - [`coordinator`] — The Coordinator actor that orchestrates placement
//! - [`bridge`] — Cluster event bridge connecting cluster layer to Coordinator
//! - [`error`] — Error types

pub mod bridge;
pub mod coordinator;
pub mod election;
pub mod error;
pub mod node_info;
pub mod placement;
pub mod spec;
