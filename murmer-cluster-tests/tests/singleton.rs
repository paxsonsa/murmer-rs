//! Cluster singleton (C-prime M4) integration scenarios over real transport.
//!
//! Proves the ClusterSingleton primitive end-to-end through a real `System` +
//! Coordinator + SpawnRegistry (not the in-process unit harness):
//!
//! 1. `start_singleton` places + spawns exactly one instance on its anchor and
//!    promotes it to `Active` once the spawn ack lands; the instance is reachable.
//! 2. When the owner node departs, the singleton fails over to a survivor with a
//!    strictly-higher `term` — the fence that rejects a zombie ex-owner.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use murmer::System;
use murmer::app::bridge;
use murmer::app::coordinator::{
    Coordinator, CoordinatorState, GetSingleton, NotifyNodeJoined, SerializableNodeInfo,
    StartSingleton,
};
use murmer::app::election::OldestNode;
use murmer::app::placement::LeastLoaded;
use murmer::app::singleton::{SingletonAnchor, SingletonOwnership, SingletonPhase, SingletonSpec};
use murmer::cluster::config::{ClusterConfigBuilder, Discovery, NodeClass, NodeIdentity};
use murmer::cluster::sync::{SpawnRegistry, TypeRegistry};
use murmer::prelude::*;

use murmer_cluster_tests::actors::{CatalogLike, CatalogState, Whoami};

fn init() {
    murmer::cluster::install_default_crypto();
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
}

/// Build a cluster config for a named node with a class and optional seeds.
fn node_config(
    name: &str,
    class: NodeClass,
    seeds: &[iroh::EndpointAddr],
) -> murmer::cluster::config::ClusterConfig {
    let mut builder = ClusterConfigBuilder::new()
        .name(name)
        // Distinct per-node iroh identity (avoid the shared-default-key collision).
        .secret_key(iroh::SecretKey::generate())
        .listen("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .cookie("singleton-test")
        .discovery(Discovery::None)
        .node_class(class);
    if !seeds.is_empty() {
        builder = builder.seed_nodes(seeds.to_vec());
    }
    builder.build().unwrap()
}

/// A SpawnRegistry that can spawn the `CatalogLike` singleton actor.
fn catalog_registry() -> SpawnRegistry {
    let mut reg = SpawnRegistry::new();
    reg.register(
        "cluster_test::CatalogLike",
        Box::new(|receptionist, label, state_bytes: Vec<u8>| {
            Box::pin(async move {
                let (state, _): (CatalogState, _) =
                    bincode::serde::decode_from_slice(&state_bytes, bincode::config::standard())
                        .map_err(|e| {
                            murmer::cluster::sync::SpawnError::DeserializeFailed(e.to_string())
                        })?;
                receptionist.start(&label, CatalogLike, state);
                Ok(())
            })
        }),
    );
    reg
}

fn catalog_state_bytes(name: &str) -> Vec<u8> {
    bincode::serde::encode_to_vec(
        &CatalogState { name: name.into() },
        bincode::config::standard(),
    )
    .unwrap()
}

/// Poll until an actor is discoverable from a node.
async fn wait_for<A: Actor + 'static>(
    system: &System,
    label: &str,
    timeout: Duration,
) -> Endpoint<A> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(ep) = system.lookup::<A>(label) {
            return ep;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for actor {label}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `GetSingleton` until the singleton reaches `Active`.
async fn wait_for_singleton_active(
    coord: &Endpoint<Coordinator>,
    label: &str,
    timeout: Duration,
) -> SingletonOwnership {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(own) = coord
            .send(GetSingleton {
                label: label.into(),
            })
            .await
            .unwrap()
            && own.phase == SingletonPhase::Active
        {
            return own;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for singleton {label} to become Active");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `GetSingleton` until the singleton's term reaches at least `want_term`.
async fn wait_for_singleton_term(
    coord: &Endpoint<Coordinator>,
    label: &str,
    want_term: u64,
    timeout: Duration,
) -> SingletonOwnership {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(own) = coord
            .send(GetSingleton {
                label: label.into(),
            })
            .await
            .unwrap()
            && own.generation.term >= want_term
        {
            return own;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for singleton {label} to reach term {want_term}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn notify_joined(
    coord: &Endpoint<Coordinator>,
    identity: &NodeIdentity,
    name: &str,
    class: NodeClass,
) {
    coord
        .send(NotifyNodeJoined {
            node_id: identity.node_id_string(),
            info: SerializableNodeInfo {
                name: name.into(),
                endpoint_id: identity.endpoint_id.clone(),
                host: identity.host.clone(),
                port: identity.port,
                incarnation: identity.incarnation,
                class,
                metadata: HashMap::new(),
            },
        })
        .await
        .unwrap();
}

// ── Test 1: single-node real spawn + Active + reachable ─────────────────────

#[tokio::test]
async fn test_singleton_starts_and_runs_on_single_node() {
    init();

    let node = System::clustered(
        node_config("solo", NodeClass::Worker, &[]),
        TypeRegistry::from_auto(),
        catalog_registry(),
    )
    .await
    .unwrap();

    let cluster = node.cluster_system().unwrap();
    let coord_ep = bridge::start_coordinator(
        cluster,
        CoordinatorState::new(
            cluster.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::any()),
        ),
    );

    let ownership = coord_ep
        .send_async(StartSingleton {
            spec: SingletonSpec::new(
                "catalog",
                "cluster_test::CatalogLike",
                SingletonAnchor::Leader,
            )
            .with_state(catalog_state_bytes("catalog-0")),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ownership.generation.term, 1);
    assert_eq!(ownership.generation.seq, 0);

    // The catalog actor really spawns via the registry and answers messages.
    let ep: Endpoint<CatalogLike> = wait_for(&node, "catalog", Duration::from_secs(10)).await;
    assert_eq!(ep.send(Whoami).await.unwrap(), "catalog-0");

    // Ownership flips to Active once the spawn ack is observed.
    let active = wait_for_singleton_active(&coord_ep, "catalog", Duration::from_secs(10)).await;
    assert_eq!(active.phase, SingletonPhase::Active);
    assert_eq!(
        active.owner_node_id.as_deref(),
        Some(cluster.identity().node_id_string().as_str()),
        "the single node owns the singleton"
    );

    node.shutdown().await;
}

// ── Test 2: failover to a survivor with a strictly-higher term ──────────────

#[tokio::test]
async fn test_singleton_fails_over_on_owner_departure_with_higher_term() {
    init();

    // Gateway (Edge) hosts the Coordinator; two workers can host the catalog.
    let gateway = System::clustered(
        node_config("gateway", NodeClass::Edge, &[]),
        TypeRegistry::from_auto(),
        SpawnRegistry::new(),
    )
    .await
    .unwrap();
    let gw_addr = gateway.endpoint_addr().unwrap();

    let worker_a = System::clustered(
        node_config(
            "worker-a",
            NodeClass::Worker,
            std::slice::from_ref(&gw_addr),
        ),
        TypeRegistry::from_auto(),
        catalog_registry(),
    )
    .await
    .unwrap();
    let worker_b = System::clustered(
        node_config(
            "worker-b",
            NodeClass::Worker,
            std::slice::from_ref(&gw_addr),
        ),
        TypeRegistry::from_auto(),
        catalog_registry(),
    )
    .await
    .unwrap();

    // Let the cluster mesh.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let cluster = gateway.cluster_system().unwrap();
    let coord_ep = bridge::start_coordinator(
        cluster,
        CoordinatorState::new(
            cluster.identity().node_id_string(),
            Box::new(LeastLoaded),
            Box::new(OldestNode::with_class(NodeClass::Edge)),
        ),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Seed the Coordinator's view (nodes joined before it started).
    let a_id = worker_a.cluster_system().unwrap().identity().clone();
    let b_id = worker_b.cluster_system().unwrap().identity().clone();
    notify_joined(&coord_ep, cluster.identity(), "gateway", NodeClass::Edge).await;
    notify_joined(&coord_ep, &a_id, "worker-a", NodeClass::Worker).await;
    notify_joined(&coord_ep, &b_id, "worker-b", NodeClass::Worker).await;

    // Anchor to the Worker class so the catalog runs on a worker, not the gateway.
    let first = coord_ep
        .send_async(StartSingleton {
            spec: SingletonSpec::new(
                "catalog",
                "cluster_test::CatalogLike",
                SingletonAnchor::Class(NodeClass::Worker),
            )
            .with_state(catalog_state_bytes("catalog-0")),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.generation.term, 1);
    let first_owner = first.owner_node_id.clone().unwrap();
    let a_node_id = a_id.node_id_string();
    let b_node_id = b_id.node_id_string();
    assert!(
        first_owner == a_node_id || first_owner == b_node_id,
        "owner should be a worker, got {first_owner}"
    );
    wait_for_singleton_active(&coord_ep, "catalog", Duration::from_secs(10)).await;

    // Gracefully shut down the owner worker → NodeLeft → fenced re-drive.
    if first_owner == a_node_id {
        worker_a.shutdown().await;
    } else {
        worker_b.shutdown().await;
    }

    // The Coordinator re-drives to the survivor with a strictly-higher term.
    let after = wait_for_singleton_term(&coord_ep, "catalog", 2, Duration::from_secs(15)).await;
    assert_eq!(after.generation.term, 2);
    assert!(
        after.generation > first.generation,
        "term must strictly increase across failover (the fence)"
    );
    assert_ne!(
        after.owner_node_id.as_deref(),
        Some(first_owner.as_str()),
        "singleton must move off the departed owner"
    );

    gateway.shutdown().await;
    if first_owner == a_node_id {
        worker_b.shutdown().await;
    } else {
        worker_a.shutdown().await;
    }
}
