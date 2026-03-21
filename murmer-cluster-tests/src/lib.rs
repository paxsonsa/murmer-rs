//! ClusterSim — a simulation framework for cluster integration tests.
//!
//! Wraps the public `System` API so test scenarios are short and declarative.
//! Each test describes a topology (seed nodes, joiners), starts actors, and
//! asserts on cross-node discovery and messaging — without manual boilerplate
//! for config construction, port passing, or polling loops.

pub mod actors;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use murmer::cluster::config::{ClusterConfigBuilder, Discovery};
use murmer::cluster::sync::SpawnRegistry;
use murmer::cluster::sync::TypeRegistry;
use murmer::prelude::*;

// =============================================================================
// ClusterSim — the main simulation handle
// =============================================================================

/// A cluster simulation containing named nodes, each running a `System`.
pub struct ClusterSim {
    nodes: HashMap<String, SimNode>,
}

struct SimNode {
    system: System,
    #[allow(dead_code)]
    name: String,
}

// =============================================================================
// Builder
// =============================================================================

/// Builder for constructing a `ClusterSim` topology.
pub struct ClusterSimBuilder {
    nodes: Vec<NodeSpec>,
}

struct NodeSpec {
    name: String,
    seed_from: Option<String>,
}

/// Configures a single node in the simulation.
pub struct NodeBuilder {
    spec: NodeSpec,
}

impl NodeBuilder {
    /// Mark this node as a seed node (no seeds of its own).
    pub fn seed_node(self) -> Self {
        // seed_from is already None by default
        self
    }

    /// This node will connect to the named node as its seed.
    pub fn seed_from(mut self, seed_name: &str) -> Self {
        self.spec.seed_from = Some(seed_name.to_string());
        self
    }
}

impl ClusterSimBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Add a node to the topology.
    ///
    /// The closure receives a `NodeBuilder` for configuring whether the node
    /// is a seed or joins via another node.
    pub fn node(mut self, name: &str, configure: impl FnOnce(NodeBuilder) -> NodeBuilder) -> Self {
        let builder = NodeBuilder {
            spec: NodeSpec {
                name: name.to_string(),
                seed_from: None,
            },
        };
        let builder = configure(builder);
        self.nodes.push(builder.spec);
        self
    }

    /// Build the simulation: starts all nodes in dependency order.
    ///
    /// Seed nodes start first so their addresses are available to joiners.
    /// Uses `TypeRegistry::from_auto()` — all `#[handlers]` actors
    /// are auto-registered via linkme.
    pub async fn build(self) -> ClusterSim {
        let registry_fn = TypeRegistry::from_auto;

        // Partition into seed nodes (no seed_from) and joiners.
        let mut seed_specs: Vec<NodeSpec> = Vec::new();
        let mut joiner_specs: Vec<NodeSpec> = Vec::new();

        for spec in self.nodes {
            if spec.seed_from.is_none() {
                seed_specs.push(spec);
            } else {
                joiner_specs.push(spec);
            }
        }

        let mut nodes = HashMap::new();

        // Start seed nodes first.
        for spec in seed_specs {
            let config = ClusterConfigBuilder::new()
                .name(&spec.name)
                .listen("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .cookie("cluster-sim-cookie")
                .discovery(Discovery::None)
                .build()
                .unwrap();

            let system = System::clustered(config, registry_fn(), SpawnRegistry::new())
                .await
                .expect("failed to start seed node");

            nodes.insert(
                spec.name.clone(),
                SimNode {
                    system,
                    name: spec.name,
                },
            );
        }

        // Start joiner nodes, resolving seed addresses.
        for spec in joiner_specs {
            let seed_name = spec.seed_from.as_ref().unwrap();
            let seed_addr = nodes
                .get(seed_name)
                .unwrap_or_else(|| panic!("seed node '{}' not found (start it first)", seed_name))
                .system
                .local_addr()
                .expect("seed node must be clustered");

            let config = ClusterConfigBuilder::new()
                .name(&spec.name)
                .listen("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .cookie("cluster-sim-cookie")
                .seed_nodes([seed_addr])
                .build()
                .unwrap();

            let system = System::clustered(config, registry_fn(), SpawnRegistry::new())
                .await
                .expect("failed to start joiner node");

            nodes.insert(
                spec.name.clone(),
                SimNode {
                    system,
                    name: spec.name,
                },
            );
        }

        ClusterSim { nodes }
    }
}

impl Default for ClusterSimBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// ClusterSim methods
// =============================================================================

impl ClusterSim {
    /// Create a new builder.
    pub fn builder() -> ClusterSimBuilder {
        ClusterSimBuilder::new()
    }

    /// Get a reference to a node's `System`.
    pub fn system(&self, node: &str) -> &System {
        &self
            .nodes
            .get(node)
            .unwrap_or_else(|| panic!("node '{}' not found", node))
            .system
    }

    /// Start an actor on a specific node.
    pub fn start_actor<A>(&self, node: &str, label: &str, actor: A, state: A::State) -> Endpoint<A>
    where
        A: Actor + RemoteDispatch + 'static,
    {
        self.system(node).start(label, actor, state)
    }

    /// Look up an actor from a specific node's perspective.
    pub fn lookup<A: Actor + 'static>(&self, node: &str, label: &str) -> Option<Endpoint<A>> {
        self.system(node).lookup(label)
    }

    /// Wait until an actor is discoverable from a node (default 10s timeout).
    pub async fn wait_discovery<A: Actor + 'static>(&self, node: &str, label: &str) -> Endpoint<A> {
        self.wait_discovery_timeout::<A>(node, label, Duration::from_secs(10))
            .await
    }

    /// Wait until an actor is discoverable from a node, with a custom timeout.
    pub async fn wait_discovery_timeout<A: Actor + 'static>(
        &self,
        node: &str,
        label: &str,
        timeout: Duration,
    ) -> Endpoint<A> {
        let system = self.system(node);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(ep) = system.lookup::<A>(label) {
                return ep;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "wait_discovery timed out: node '{}' never discovered '{}' within {:?}",
                    node, label, timeout
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until an actor is NOT discoverable from a node (for departure tests).
    pub async fn wait_gone<A: Actor + 'static>(&self, node: &str, label: &str) {
        self.wait_gone_timeout::<A>(node, label, Duration::from_secs(10))
            .await;
    }

    /// Wait until an actor is NOT discoverable, with a custom timeout.
    pub async fn wait_gone_timeout<A: Actor + 'static>(
        &self,
        node: &str,
        label: &str,
        timeout: Duration,
    ) {
        let system = self.system(node);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if system.lookup::<A>(label).is_none() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "wait_gone timed out: node '{}' still sees '{}' after {:?}",
                    node, label, timeout
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Gracefully shut down a single node.
    pub async fn shutdown_node(&mut self, name: &str) {
        if let Some(node) = self.nodes.remove(name) {
            node.system.shutdown().await;
        }
    }

    /// Shut down all remaining nodes.
    pub async fn shutdown_all(&mut self) {
        let names: Vec<String> = self.nodes.keys().cloned().collect();
        for name in names {
            if let Some(node) = self.nodes.remove(&name) {
                node.system.shutdown().await;
            }
        }
    }
}
