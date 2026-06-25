//! Simulation demo — a deterministic multi-node fault run you can watch.
//!
//! Unlike the other examples, this one touches no network and no real clock. It
//! boots a three-node cluster on murmer's deterministic simulation runtime (one
//! virtual clock, a seeded scheduler), crashes a node, and shows the survivors
//! detect it — then replays the identical run from the same seed to show the
//! fault is reproducible. Thirty seconds of "virtual" detection time elapses in
//! well under a millisecond of real time.
//!
//! This is the "watch the rig work" companion to the copyable test template in
//! `murmer/tests/sim_cluster.rs`. See the book's "Simulation Testing" chapter for
//! the full API.
//!
//! Run with: `cargo run -p murmer-examples --bin sim_cluster_demo`

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use murmer::SimCluster;

/// Run a full crash scenario and return the set of nodes the survivors detected
/// as failed. Used to show the outcome is a pure function of the seed.
fn detect_crash(seed: u64) -> BTreeSet<String> {
    let mut cluster = SimCluster::builder(seed)
        .node("node-a")
        .node("node-b")
        .node("node-c")
        .build();
    cluster.mesh();
    cluster.pump();
    for n in ["node-a", "node-b", "node-c"] {
        let _ = cluster.events(n); // discard the convergence joins
    }
    cluster.crash("node-a");
    cluster.advance(Duration::from_secs(30));
    let mut failed = cluster.events("node-b").failed;
    failed.extend(cluster.events("node-c").failed);
    failed
}

fn main() {
    let seed: u64 = 0xC0FFEE;
    println!("murmer simulation demo — a deterministic three-node fault run\n");

    let wall = Instant::now();
    let mut cluster = SimCluster::builder(seed)
        .node("node-a")
        .node("node-b")
        .node("node-c")
        .build();
    println!("[seed {seed:#x}] booted node-a, node-b, node-c on one virtual clock");

    cluster.mesh();
    cluster.pump();
    println!("  mesh + pump -> converged:");
    for n in ["node-a", "node-b", "node-c"] {
        let peers: Vec<String> = cluster.events(n).joined_ids().into_iter().collect();
        println!("    {n} sees {}", peers.join(", "));
    }

    println!("  crash node-a, then advance 30s of virtual time");
    cluster.crash("node-a");
    cluster.advance(Duration::from_secs(30));
    for survivor in ["node-b", "node-c"] {
        let failed: Vec<String> = cluster.events(survivor).failed.into_iter().collect();
        println!("    {survivor} detected failed: {}", failed.join(", "));
    }
    println!("  (virtual time: 30s    real time: {:?})\n", wall.elapsed());

    println!("reproducibility: re-run the identical scenario at seed {seed:#x}");
    let run1 = detect_crash(seed);
    let run2 = detect_crash(seed);
    println!("  run 1 detected: {run1:?}");
    println!("  run 2 detected: {run2:?}");
    println!(
        "  -> {}",
        if run1 == run2 {
            "identical. the fault is reproducible from the seed."
        } else {
            "DIVERGED — this should never happen under the sim runtime."
        }
    );
}
