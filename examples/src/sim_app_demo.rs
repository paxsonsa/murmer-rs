//! Simulation demo — running a whole actor stack deterministically.
//!
//! This is the most common reason to reach for the sim: you have an application
//! made of several actors that talk to each other, and you want to run the whole
//! thing on one virtual clock, reproducibly, fast-forwarding time instead of
//! sleeping. No network and no real clock are involved.
//!
//! The stack here is a small job pipeline:
//!
//!   Dispatcher --Process--> Worker (x3) --Completed--> Ledger
//!
//! The dispatcher round-robins jobs to three workers. Each worker drains its
//! queue at its own rate (a repeating timer on virtual time) and reports each
//! finished job to the ledger. We submit a seeded batch, advance the clock until
//! the pipeline empties, and read the ledger. Then we replay the identical run
//! from the same seed.
//!
//! The same actor code runs unchanged on a multi-node `SimCluster` when you want
//! the workers distributed across nodes and faults injected. On one node, this is
//! the lighter `SimWorld`. See the book's "Simulation Testing" chapter.
//!
//! Run with: `cargo run -p murmer-examples --bin sim_app_demo`

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use murmer::SimWorld;
use murmer::actor::ActorContext;
use murmer::prelude::*;
use serde::{Deserialize, Serialize};

// ── Ledger: tallies finished jobs, overall and per worker ───────────────────

struct Ledger;

#[derive(Default)]
struct LedgerState {
    total: u64,
    per_worker: BTreeMap<String, u64>,
}

impl Actor for Ledger {
    type State = LedgerState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "app::Completed")]
struct Completed {
    worker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = LedgerReport, remote = "app::Report")]
struct Report;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerReport {
    total: u64,
    per_worker: BTreeMap<String, u64>,
}

#[handlers]
impl Ledger {
    #[handler]
    fn completed(&self, _ctx: &ActorContext<Self>, state: &mut LedgerState, msg: Completed) {
        state.total += 1;
        *state.per_worker.entry(msg.worker).or_default() += 1;
    }

    #[handler]
    fn report(
        &self,
        _ctx: &ActorContext<Self>,
        state: &mut LedgerState,
        _msg: Report,
    ) -> LedgerReport {
        LedgerReport {
            total: state.total,
            per_worker: state.per_worker.clone(),
        }
    }
}

// ── Worker: drains its queue one job per tick, reports each to the ledger ────

struct Worker;

struct WorkerState {
    name: String,
    ledger: Endpoint<Ledger>,
    queue: VecDeque<u64>,
    // The repeating drain ticker. Held in state because dropping the handle
    // cancels the timer; keeping it alive is what lets the worker keep draining.
    ticker: Option<ScheduleHandle>,
}

impl Actor for Worker {
    type State = WorkerState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "app::Start")]
struct Start {
    every_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "app::Process")]
struct Process {
    job_id: u64,
}

// Internal self-tick driving the drain.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "app::Tick")]
struct Tick;

#[handlers]
impl Worker {
    #[handler]
    fn start(&self, ctx: &ActorContext<Self>, state: &mut WorkerState, msg: Start) {
        state.ticker = Some(ctx.schedule_repeat(Duration::from_millis(msg.every_ms), Tick));
    }

    #[handler]
    fn process(&self, _ctx: &ActorContext<Self>, state: &mut WorkerState, msg: Process) {
        state.queue.push_back(msg.job_id);
    }

    #[handler]
    fn tick(&self, ctx: &ActorContext<Self>, state: &mut WorkerState, _msg: Tick) {
        if state.queue.pop_front().is_some() {
            // Notify the ledger. A handler is sync, so the cross-actor send goes
            // on a spawned task (the documented pattern).
            let ledger = state.ledger.clone();
            let worker = state.name.clone();
            ctx.spawn(async move {
                ledger.send(Completed { worker }).await.ok();
            });
        }
    }
}

// ── Dispatcher: round-robins a batch of jobs across the workers ─────────────

struct Dispatcher;

struct DispatcherState {
    workers: Vec<Endpoint<Worker>>,
}

impl Actor for Dispatcher {
    type State = DispatcherState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = (), remote = "app::Dispatch")]
struct Dispatch {
    jobs: Vec<u64>,
}

#[handlers]
impl Dispatcher {
    #[handler]
    fn dispatch(&self, ctx: &ActorContext<Self>, state: &mut DispatcherState, msg: Dispatch) {
        let workers = state.workers.clone();
        ctx.spawn(async move {
            for (i, job_id) in msg.jobs.into_iter().enumerate() {
                workers[i % workers.len()]
                    .send(Process { job_id })
                    .await
                    .ok();
            }
        });
    }
}

// ── Driver: stand the stack up and run it ───────────────────────────────────

/// Build the pipeline, submit a seeded batch, drain it, and return the ledger
/// report. The whole run is a pure function of the seed.
fn run_pipeline(seed: u64) -> LedgerReport {
    let mut world = SimWorld::new(seed);

    let ledger = world
        .system()
        .start("ledger", Ledger, LedgerState::default());

    // Three workers, each at its own drain rate (a heterogeneous pool).
    let rates = [("worker-a", 50u64), ("worker-b", 80), ("worker-c", 120)];
    let workers: Vec<Endpoint<Worker>> = rates
        .iter()
        .map(|(name, every_ms)| {
            let ep = world.system().start(
                &format!("worker/{name}"),
                Worker,
                WorkerState {
                    name: name.to_string(),
                    ledger: ledger.clone(),
                    queue: VecDeque::new(),
                    ticker: None,
                },
            );
            world
                .send(
                    &ep,
                    Start {
                        every_ms: *every_ms,
                    },
                )
                .unwrap();
            ep
        })
        .collect();

    let dispatcher = world.system().start(
        "dispatcher",
        Dispatcher,
        DispatcherState {
            workers: workers.clone(),
        },
    );

    // A seeded batch size, so the run varies by seed but replays exactly.
    let job_count = 20 + (world.rng_u64() % 20);
    let jobs: Vec<u64> = (0..job_count).collect();
    world.send(&dispatcher, Dispatch { jobs }).unwrap();

    // Let the dispatch fan out, then advance until every queue is drained.
    world.pump();
    world.advance(Duration::from_secs(10));

    world.send(&ledger, Report).unwrap()
}

fn main() {
    let seed: u64 = 0xC0FFEE;
    println!("murmer simulation demo — a whole actor stack on one virtual clock\n");
    println!("  pipeline: dispatcher --> 3 workers (50/80/120ms each) --> ledger\n");

    let wall = Instant::now();
    let report = run_pipeline(seed);
    let real = wall.elapsed();

    println!(
        "[seed {seed:#x}] dispatched {} jobs, drained the pipeline:",
        report.total
    );
    for (worker, count) in &report.per_worker {
        println!("    {worker} finished {count}");
    }
    println!("  ledger total: {}", report.total);
    println!("  (virtual time: 10s    real time: {real:?})\n");

    println!("reproducibility: re-run at seed {seed:#x}");
    let a = run_pipeline(seed);
    let b = run_pipeline(seed);
    println!("  run 1 total {} / per-worker {:?}", a.total, a.per_worker);
    println!("  run 2 total {} / per-worker {:?}", b.total, b.per_worker);
    println!(
        "  -> {}",
        if a.per_worker == b.per_worker && a.total == b.total {
            "identical. the whole stack replays from the seed."
        } else {
            "DIVERGED — this should never happen under the sim runtime."
        }
    );
}
