//! Example: simulation-testing a multi-node cluster with `SimCluster`.
//!
//! This is the multi-node companion to `sim_world.rs`. It is an *external*
//! consumer of murmer — it uses only the public API, exactly as a downstream
//! crate would. Where `SimWorld` makes one node's actors deterministic,
//! `SimCluster` boots N nodes on one virtual clock, wires them over an in-memory
//! fabric, and lets you inject crash / partition / rejoin faults and network
//! latency, all reproducible from a seed.
//!
//! This file is the template to copy when you want to fault-test your own
//! cluster: boot a cluster, drive it to convergence, inject a fault, advance the
//! virtual clock past the detection budget, and assert on what each node saw.
//!
//! Run with: `cargo test -p murmer --features sim --test sim_cluster`
#![cfg(feature = "sim")]

use std::collections::BTreeSet;
use std::time::Duration;

use murmer::SimCluster;
use murmer::actor::ActorContext;
use murmer::prelude::*;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// A small remotely-dispatchable actor, so we can show cross-node messaging.
// Any actor with `#[handlers]` and a `#[message(remote = ...)]` works.
// ---------------------------------------------------------------------------

struct Counter;

#[derive(Default)]
struct CounterState {
    count: i64,
}

impl Actor for Counter {
    type State = CounterState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = i64, remote = "sim_cluster_example::Increment")]
struct Increment {
    amount: i64,
}

#[handlers]
impl Counter {
    #[handler]
    fn increment(
        &self,
        _ctx: &ActorContext<Self>,
        state: &mut CounterState,
        msg: Increment,
    ) -> i64 {
        state.count += msg.amount;
        state.count
    }
}

/// Boot a three-node cluster, mesh it, and pump to convergence. Convergence is
/// pump-only: foca applies membership synchronously when the handshake lands, so
/// no clock advance is needed yet.
fn converged_trio(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::builder(seed)
        .node("node-a")
        .node("node-b")
        .node("node-c")
        .build();
    cluster.mesh();
    cluster.pump();
    cluster
}

#[test]
fn three_nodes_converge_to_a_full_mesh() {
    // Every node sees the other two come up. We drain each node's events and
    // collect the endpoint ids it observed joining.
    let mut cluster = converged_trio(0xC0FFEE);

    for (me, peers) in [
        ("node-a", ["node-b-id", "node-c-id"]),
        ("node-b", ["node-a-id", "node-c-id"]),
        ("node-c", ["node-a-id", "node-b-id"]),
    ] {
        let want: BTreeSet<String> = peers.iter().map(|s| s.to_string()).collect();
        assert_eq!(cluster.events(me).joined_ids(), want, "{me} converged set");
    }
}

#[test]
fn a_crashed_node_is_detected_exactly() {
    // Crash one node and advance past foca's detection budget. The survivors must
    // detect EXACTLY the crashed node — never a false failure of a healthy peer.
    let mut cluster = converged_trio(1);

    // Drain the convergence joins so the next drain reads only the failure phase.
    let _ = (cluster.events("node-b"), cluster.events("node-c"));

    cluster.crash("node-a");
    cluster.advance(Duration::from_secs(30));

    let only_a = BTreeSet::from(["node-a-id".to_string()]);
    for survivor in ["node-b", "node-c"] {
        assert_eq!(
            cluster.events(survivor).failed,
            only_a,
            "{survivor} detects exactly the crashed node"
        );
    }
}

#[test]
fn a_single_link_partition_is_tolerated() {
    // Cut one link in a healthy mesh. foca probes the suspected peer indirectly
    // through the third node, so nobody is ever declared down. A partial cut must
    // not partition the cluster.
    let mut cluster = converged_trio(1);
    for n in ["node-a", "node-b", "node-c"] {
        let _ = cluster.events(n); // drain convergence
    }

    assert!(
        cluster.partition("node-a", "node-b"),
        "the A-B link was live"
    );
    cluster.advance(Duration::from_secs(30));

    for n in ["node-a", "node-b", "node-c"] {
        assert!(
            !cluster.events(n).any_failed(),
            "{n} saw a failure, but a single cut should be masked by the third node"
        );
    }
}

#[test]
fn the_same_seed_replays_identically() {
    // The whole point of the harness: a fault run is a pure function of the seed.
    // Run the same crash scenario twice at the same seed and assert the survivors
    // saw the identical failure set. A different seed still converges and detects.
    fn detect(seed: u64) -> BTreeSet<String> {
        let mut cluster = converged_trio(seed);
        let _ = (cluster.events("node-b"), cluster.events("node-c"));
        cluster.crash("node-a");
        cluster.advance(Duration::from_secs(30));
        let mut failed = cluster.events("node-b").failed;
        failed.extend(cluster.events("node-c").failed);
        failed
    }

    let only_a = BTreeSet::from(["node-a-id".to_string()]);
    assert_eq!(detect(7), detect(7), "same seed must replay identically");
    assert_eq!(detect(7), only_a);
    assert_eq!(
        detect(99),
        only_a,
        "a different seed still detects exactly A"
    );
}

#[test]
fn a_remote_actor_is_reachable_across_nodes() {
    // Start a discoverable actor on one node and message it from another. The
    // registration syncs across the cluster, then a lookup from the far node
    // returns a remote endpoint that carries the message over the sim fabric.
    let mut cluster = SimCluster::builder(1).node("node-a").node("node-b").build();
    cluster.mesh();
    cluster.pump();

    // Discoverable actor on node-a.
    let _local =
        cluster
            .system("node-a")
            .start_actor("counter/0", Counter, CounterState::default());

    // Advance a registry-sync interval so node-b learns about node-a's actor.
    cluster.advance(Duration::from_secs(6));

    // From node-b, resolve the remote endpoint and message it.
    let endpoint = cluster
        .system("node-b")
        .lookup::<Counter>("counter/0")
        .expect("node-b discovers node-a's actor after the registry sync");
    let reply =
        cluster.block_on(async move { endpoint.send(Increment { amount: 5 }).await.unwrap() });
    assert_eq!(
        reply, 5,
        "the remote increment round-trips over the sim fabric"
    );
}

#[test]
fn convergence_survives_a_slow_network() {
    // Inject network latency: every chunk is delayed by 50ms plus up to 20ms of
    // seeded jitter. Membership must still converge once the clock advances far
    // enough for the delayed control frames to land. The model is faithful to
    // QUIC: in-stream order is preserved, cross-stream order can vary.
    let mut cluster = SimCluster::builder(1)
        .node("node-a")
        .node("node-b")
        .node("node-c")
        .network_latency(Duration::from_millis(50), Duration::from_millis(20))
        .build();
    cluster.mesh();
    cluster.pump();
    cluster.advance(Duration::from_secs(30));

    for (me, peers) in [
        ("node-a", ["node-b-id", "node-c-id"]),
        ("node-b", ["node-a-id", "node-c-id"]),
        ("node-c", ["node-a-id", "node-b-id"]),
    ] {
        let want: BTreeSet<String> = peers.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            cluster.events(me).joined_ids(),
            want,
            "{me} converges even over a slow, jittery network"
        );
    }
}
