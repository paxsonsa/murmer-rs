//! Orchestrator Example — Filesystem RPC with Coordinator-driven Placement
//!
//! Demonstrates the murmer-app orchestration loop:
//!
//! 1. Three nodes form a cluster: gateway (Edge), store-a & store-b (Workers)
//! 2. Each worker node advertises capabilities via metadata ("volume" tag)
//! 3. The gateway runs a Coordinator that places StorageAgent actors on
//!    workers matching their placement constraints
//! 4. Clients query storage agents for directory listings and file reads
//! 5. Node failure triggers crash strategy handling (WaitForReturn / Redistribute)
//!
//! Run with: `cargo run --bin orchestrator`

fn main() {
    println!("Run this example via `cargo test --bin orchestrator` to execute the test scenarios.");
}

// =============================================================================
// STORAGE AGENT ACTOR
// =============================================================================

use std::collections::HashMap;

use murmer::prelude::*;
use murmer_macros::{Message, handlers};
use serde::{Deserialize, Serialize};

/// A storage agent that owns a named root directory with in-memory files.
#[derive(Debug)]
pub struct StorageAgent;

/// State for a storage agent — its root name and directory tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageState {
    /// Name of this storage root (e.g. "photos", "docs").
    pub root_name: String,
    /// Directory tree: path → list of entries.
    pub dirs: HashMap<String, Vec<String>>,
}

impl Actor for StorageAgent {
    type State = StorageState;
}

// -- Messages ----------------------------------------------------------------

/// List entries in a directory.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Vec<String>, remote = "orchestrator::ListDir")]
pub struct ListDir {
    pub path: String,
}

/// Read a file (returns its full path if found).
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = Option<String>, remote = "orchestrator::ReadFile")]
pub struct ReadFile {
    pub name: String,
}

/// Get this agent's root name.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = String, remote = "orchestrator::GetRoot")]
pub struct GetRoot;

// -- Handlers ----------------------------------------------------------------

#[handlers]
impl StorageAgent {
    #[handler]
    fn list_dir(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut StorageState,
        msg: ListDir,
    ) -> Vec<String> {
        state.dirs.get(&msg.path).cloned().unwrap_or_default()
    }

    #[handler]
    fn read_file(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut StorageState,
        msg: ReadFile,
    ) -> Option<String> {
        for (dir, entries) in &state.dirs {
            if entries.contains(&msg.name) {
                return Some(format!("{}/{}/{}", state.root_name, dir, msg.name));
            }
        }
        None
    }

    #[handler]
    fn get_root(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut StorageState,
        _msg: GetRoot,
    ) -> String {
        state.root_name.clone()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Duration;

    use murmer::cluster::config::{ClusterConfigBuilder, Discovery, NodeClass};
    use murmer::cluster::sync::{SpawnRegistry, TypeRegistry};
    use murmer::System;
    use murmer_app::bridge;
    use murmer_app::coordinator::{
        CoordinatorState, GetClusterView, GetSpecs, NotifyNodeJoined,
        SerializableNodeInfo, SubmitSpec,
    };
    use murmer_app::election::OldestNode;
    use murmer_app::placement::LeastLoaded;
    use murmer_app::spec::{ActorSpec, CrashStrategy, PlacementConstraints};

    fn init_tracing() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();
    }

    /// Create a storage agent spawn registry entry.
    fn make_spawn_registry() -> SpawnRegistry {
        let mut reg = SpawnRegistry::new();
        reg.register(
            "orchestrator::StorageAgent",
            Box::new(|receptionist, label, state_bytes| {
                let (state, _): (StorageState, _) = bincode::serde::decode_from_slice(
                    state_bytes,
                    bincode::config::standard(),
                )
                .map_err(|e| murmer::cluster::sync::SpawnError::DeserializeFailed(e.to_string()))?;
                receptionist.start(label, StorageAgent, state);
                Ok(())
            }),
        );
        reg
    }

    /// Build a cluster config for a named node with given class and metadata.
    fn node_config(
        name: &str,
        class: NodeClass,
        metadata: Vec<(&str, &str)>,
        seeds: &[SocketAddr],
    ) -> murmer::cluster::config::ClusterConfig {
        let mut builder = ClusterConfigBuilder::new()
            .name(name)
            .listen("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .cookie("orchestrator-example")
            .discovery(Discovery::None)
            .node_class(class);

        for (k, v) in metadata {
            builder = builder.metadata(k, v);
        }

        if !seeds.is_empty() {
            builder = builder.seed_nodes(seeds.to_vec());
        }

        builder.build().unwrap()
    }

    /// Wait for an actor to be discoverable from a node.
    async fn wait_for<A: Actor + 'static>(system: &System, label: &str) -> Endpoint<A> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(ep) = system.lookup::<A>(label) {
                return ep;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("Timed out waiting for {label}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── Test: Full orchestration loop ────────────────────────────────────

    #[tokio::test]
    async fn test_orchestrator_placement_and_query() {
        init_tracing();

        // 1. Start the gateway node (Edge class, runs the Coordinator)
        let gateway = System::clustered(
            node_config("gateway", NodeClass::Edge, vec![], &[]),
            TypeRegistry::from_auto(),
            SpawnRegistry::new(), // gateway doesn't host storage agents
        )
        .await
        .unwrap();
        let gateway_addr = gateway.local_addr().unwrap();

        // 2. Start store-a (Worker, volume=photos)
        let store_a = System::clustered(
            node_config(
                "store-a",
                NodeClass::Worker,
                vec![("volume", "photos")],
                &[gateway_addr],
            ),
            TypeRegistry::from_auto(),
            make_spawn_registry(),
        )
        .await
        .unwrap();

        // 3. Start store-b (Worker, volume=docs)
        let store_b = System::clustered(
            node_config(
                "store-b",
                NodeClass::Worker,
                vec![("volume", "docs")],
                &[gateway_addr],
            ),
            TypeRegistry::from_auto(),
            make_spawn_registry(),
        )
        .await
        .unwrap();

        // Wait for cluster to mesh
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 4. Start the Coordinator on the gateway
        let cluster = gateway.cluster_system().unwrap();
        let coordinator_state = CoordinatorState::new(
            cluster.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::with_class(NodeClass::Edge)),
        );

        // Seed the Coordinator's cluster view with the nodes we know about
        // In production, the bridge handles this via ClusterEvents — but we
        // also need to manually add nodes that joined before the Coordinator
        // started. For this test, we'll add them manually.

        let coordinator_ep = bridge::start_coordinator(cluster, coordinator_state);

        // Give the bridge a moment to start receiving events
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Manually notify the Coordinator about nodes (since they joined
        // before the bridge started)
        let store_a_cluster = store_a.cluster_system().unwrap();
        let store_b_cluster = store_b.cluster_system().unwrap();

        coordinator_ep
            .send(NotifyNodeJoined {
                node_id: store_a_cluster.identity().node_id_string(),
                info: SerializableNodeInfo {
                    name: "store-a".into(),
                    host: store_a_cluster.identity().host.clone(),
                    port: store_a_cluster.identity().port,
                    incarnation: store_a_cluster.identity().incarnation,
                    class: NodeClass::Worker,
                    metadata: [("volume".into(), "photos".into())].into(),
                },
            })
            .await
            .unwrap();

        coordinator_ep
            .send(NotifyNodeJoined {
                node_id: store_b_cluster.identity().node_id_string(),
                info: SerializableNodeInfo {
                    name: "store-b".into(),
                    host: store_b_cluster.identity().host.clone(),
                    port: store_b_cluster.identity().port,
                    incarnation: store_b_cluster.identity().incarnation,
                    class: NodeClass::Worker,
                    metadata: [("volume".into(), "docs".into())].into(),
                },
            })
            .await
            .unwrap();

        // Also add the gateway itself to the view
        coordinator_ep
            .send(NotifyNodeJoined {
                node_id: cluster.identity().node_id_string(),
                info: SerializableNodeInfo {
                    name: "gateway".into(),
                    host: cluster.identity().host.clone(),
                    port: cluster.identity().port,
                    incarnation: cluster.identity().incarnation,
                    class: NodeClass::Edge,
                    metadata: HashMap::new(),
                },
            })
            .await
            .unwrap();

        // 5. Submit actor specs with placement constraints

        // Photos storage — constrained to nodes with volume=photos
        let photos_state = StorageState {
            root_name: "photos".into(),
            dirs: [
                ("/".into(), vec!["vacation".into(), "family".into()]),
                (
                    "/vacation".into(),
                    vec!["beach.jpg".into(), "sunset.jpg".into()],
                ),
            ]
            .into(),
        };
        let photos_state_bytes =
            bincode::serde::encode_to_vec(&photos_state, bincode::config::standard()).unwrap();

        let photos_result = coordinator_ep
            .send(SubmitSpec {
                spec: ActorSpec::new("storage/photos", "orchestrator::StorageAgent")
                    .with_state(photos_state_bytes)
                    .with_crash_strategy(CrashStrategy::WaitForReturn(Duration::from_secs(30)))
                    .with_constraints(PlacementConstraints {
                        required_classes: vec![NodeClass::Worker],
                        required_metadata: [("volume".into(), "photos".into())].into(),
                        ..Default::default()
                    }),
            })
            .await
            .unwrap();

        assert!(
            photos_result.is_ok(),
            "Photos placement failed: {:?}",
            photos_result
        );
        let photos_decision = photos_result.unwrap();
        tracing::info!("Photos placed: {:?}", photos_decision);

        // Docs storage — constrained to nodes with volume=docs
        let docs_state = StorageState {
            root_name: "docs".into(),
            dirs: [
                ("/".into(), vec!["reports".into(), "notes".into()]),
                (
                    "/reports".into(),
                    vec!["q1.pdf".into(), "q2.pdf".into()],
                ),
            ]
            .into(),
        };
        let docs_state_bytes =
            bincode::serde::encode_to_vec(&docs_state, bincode::config::standard()).unwrap();

        let docs_result = coordinator_ep
            .send(SubmitSpec {
                spec: ActorSpec::new("storage/docs", "orchestrator::StorageAgent")
                    .with_state(docs_state_bytes)
                    .with_crash_strategy(CrashStrategy::Redistribute)
                    .with_constraints(PlacementConstraints {
                        required_classes: vec![NodeClass::Worker],
                        required_metadata: [("volume".into(), "docs".into())].into(),
                        ..Default::default()
                    }),
            })
            .await
            .unwrap();

        assert!(
            docs_result.is_ok(),
            "Docs placement failed: {:?}",
            docs_result
        );
        let docs_decision = docs_result.unwrap();
        tracing::info!("Docs placed: {:?}", docs_decision);

        // 6. Wait for the spawn ack (actors appear via SpawnAckOk)
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 7. Verify actors are running by querying them from the gateway
        let photos_ep: Endpoint<StorageAgent> =
            wait_for(&gateway, "storage/photos").await;

        let root = photos_ep.send(GetRoot).await.unwrap();
        assert_eq!(root, "photos");

        let entries = photos_ep
            .send(ListDir {
                path: "/".into(),
            })
            .await
            .unwrap();
        assert_eq!(entries, vec!["vacation", "family"]);

        let file = photos_ep
            .send(ReadFile {
                name: "beach.jpg".into(),
            })
            .await
            .unwrap();
        assert_eq!(file, Some("photos//vacation/beach.jpg".into()));

        // Query docs storage
        let docs_ep: Endpoint<StorageAgent> =
            wait_for(&gateway, "storage/docs").await;

        let root = docs_ep.send(GetRoot).await.unwrap();
        assert_eq!(root, "docs");

        let entries = docs_ep
            .send(ListDir {
                path: "/reports".into(),
            })
            .await
            .unwrap();
        assert_eq!(entries, vec!["q1.pdf", "q2.pdf"]);

        // 8. Check Coordinator state
        let specs = coordinator_ep.send(GetSpecs).await.unwrap();
        assert_eq!(specs.len(), 2);
        tracing::info!("Managed specs: {specs:#?}");

        // 9. Verify placement decisions respected constraints
        // Photos should be on store-a (volume=photos)
        assert!(
            photos_decision
                .node_id
                .contains("store-a"),
            "Photos should be placed on store-a, got: {}",
            photos_decision.node_id
        );
        // Docs should be on store-b (volume=docs)
        assert!(
            docs_decision
                .node_id
                .contains("store-b"),
            "Docs should be placed on store-b, got: {}",
            docs_decision.node_id
        );

        // 10. Clean shutdown
        store_a.shutdown().await;
        store_b.shutdown().await;
        gateway.shutdown().await;
    }

    // ── Test: Coordinator placement with GetClusterView ──────────────────

    #[tokio::test]
    async fn test_coordinator_cluster_view() {
        init_tracing();

        // Simple 2-node cluster to verify the Coordinator sees the topology
        let node_a = System::clustered(
            node_config("alpha", NodeClass::Worker, vec![], &[]),
            TypeRegistry::from_auto(),
            SpawnRegistry::new(),
        )
        .await
        .unwrap();
        let a_addr = node_a.local_addr().unwrap();

        let node_b = System::clustered(
            node_config("beta", NodeClass::Worker, vec![("gpu", "true")], &[a_addr]),
            TypeRegistry::from_auto(),
            SpawnRegistry::new(),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let cluster = node_a.cluster_system().unwrap();
        let coord_state = CoordinatorState::new(
            cluster.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        );
        let coordinator_ep = bridge::start_coordinator(cluster, coord_state);

        // Manually seed the cluster view
        let b_cluster = node_b.cluster_system().unwrap();
        coordinator_ep
            .send(NotifyNodeJoined {
                node_id: cluster.identity().node_id_string(),
                info: SerializableNodeInfo {
                    name: "alpha".into(),
                    host: cluster.identity().host.clone(),
                    port: cluster.identity().port,
                    incarnation: cluster.identity().incarnation,
                    class: NodeClass::Worker,
                    metadata: HashMap::new(),
                },
            })
            .await
            .unwrap();
        coordinator_ep
            .send(NotifyNodeJoined {
                node_id: b_cluster.identity().node_id_string(),
                info: SerializableNodeInfo {
                    name: "beta".into(),
                    host: b_cluster.identity().host.clone(),
                    port: b_cluster.identity().port,
                    incarnation: b_cluster.identity().incarnation,
                    class: NodeClass::Worker,
                    metadata: [("gpu".into(), "true".into())].into(),
                },
            })
            .await
            .unwrap();

        let view = coordinator_ep.send(GetClusterView).await.unwrap();
        assert_eq!(view.alive_count, 2);
        assert_eq!(view.total_count, 2);

        tracing::info!("Cluster view: {view:#?}");

        node_a.shutdown().await;
        node_b.shutdown().await;
    }
}
