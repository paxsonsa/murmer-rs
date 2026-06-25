//! Example: simulation-testing your own actors with `SimWorld`.
//!
//! This file is an *external* consumer of murmer — it uses only the public API,
//! exactly as a downstream crate would. It is the template for "let clients
//! sim-test their own actors": no scheduler to write, no tokio runtime, all
//! virtual time, reproducible from a seed.
//!
//! Run with: `cargo test -p murmer --features sim --test sim_world`
#![cfg(feature = "sim")]

use std::time::Duration;

use murmer::SimWorld;
use murmer::actor::ActorContext;
use murmer::prelude::*;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// A small domain actor: a rate-limited job queue that drains one job per tick.
// ---------------------------------------------------------------------------

struct Worker;

#[derive(Default)]
struct WorkerState {
    pending: u64,
    completed: u64,
    drain: Option<ScheduleHandle>,
}

impl Actor for Worker {
    type State = WorkerState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "ex::Submit")]
struct Submit {
    jobs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "ex::StartDraining")]
struct StartDraining {
    every_ms: u64,
}

// Internal tick message the actor sends to itself via schedule_repeat.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "ex::Drain")]
struct Drain;

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = u64, remote = "ex::Completed")]
struct Completed;

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = u64, remote = "ex::Pending")]
struct Pending;

#[handlers]
impl Worker {
    #[handler]
    fn submit(&self, _ctx: &ActorContext<Self>, state: &mut WorkerState, msg: Submit) {
        state.pending += msg.jobs;
    }

    #[handler]
    fn start_draining(
        &self,
        ctx: &ActorContext<Self>,
        state: &mut WorkerState,
        msg: StartDraining,
    ) {
        state.drain = Some(ctx.schedule_repeat(Duration::from_millis(msg.every_ms), Drain));
    }

    #[handler]
    fn drain(&self, _ctx: &ActorContext<Self>, state: &mut WorkerState, _msg: Drain) {
        if state.pending > 0 {
            state.pending -= 1;
            state.completed += 1;
        }
    }

    #[handler]
    fn completed(
        &self,
        _ctx: &ActorContext<Self>,
        state: &mut WorkerState,
        _msg: Completed,
    ) -> u64 {
        state.completed
    }

    #[handler]
    fn pending(&self, _ctx: &ActorContext<Self>, state: &mut WorkerState, _msg: Pending) -> u64 {
        state.pending
    }
}

#[test]
fn drains_one_job_per_tick_in_virtual_time() {
    let mut world = SimWorld::new(0xC0FFEE);
    let worker = world
        .system()
        .start("worker/0", Worker, WorkerState::default());

    world.send(&worker, Submit { jobs: 5 }).unwrap();
    world
        .send(&worker, StartDraining { every_ms: 100 })
        .unwrap();
    assert_eq!(world.send(&worker, Completed).unwrap(), 0);

    // 250ms of virtual time → 2 drains (at 100ms, 200ms). No real sleeping.
    world.advance(Duration::from_millis(250));
    assert_eq!(world.send(&worker, Completed).unwrap(), 2);
    assert_eq!(world.send(&worker, Pending).unwrap(), 3);

    // Run long enough to drain the rest; it should stop at 5 (queue empties).
    world.advance(Duration::from_secs(10));
    assert_eq!(world.send(&worker, Completed).unwrap(), 5);
    assert_eq!(world.send(&worker, Pending).unwrap(), 0);
}

#[test]
fn seeded_workload_replays_identically() {
    // A seeded interaction plan: random submissions interleaved with time.
    fn run(seed: u64) -> u64 {
        let mut world = SimWorld::new(seed);
        let worker = world
            .system()
            .start("worker/0", Worker, WorkerState::default());
        world.send(&worker, StartDraining { every_ms: 10 }).unwrap();

        for _ in 0..20 {
            let jobs = world.rng_u64() % 4; // deterministic "random" load
            world.send(&worker, Submit { jobs }).unwrap();
            world.advance(Duration::from_millis(10));
        }
        world.advance(Duration::from_secs(1)); // drain remainder
        world.send(&worker, Completed).unwrap()
    }

    // Same seed → identical result, every run. Different seed → (likely) different.
    assert_eq!(run(1), run(1));
    assert_eq!(run(2), run(2));
}
