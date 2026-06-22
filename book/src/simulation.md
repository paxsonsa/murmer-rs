# Simulation Testing

murmer can run your actors under a deterministic, single-threaded runtime with a
virtual clock. You drive the world by hand, and time only moves when you advance
it. The scheduling, the timer firings, and the seeded randomness all replay from
the seed. This is the murmer side of the FoundationDB approach to testing:
instead of hoping a race shows up under load, you replay it from a seed.

Enable the `sim` feature:

```toml
[dev-dependencies]
murmer = { version = "0.3", features = ["sim"] }
```

## The idea

Normally a `System` runs on Tokio. Tasks spawn on real threads, timers fire on
the real clock, and `Instant::now()` reads wall time. None of that is
reproducible. The `sim` feature swaps the runtime under the whole actor
framework for a `SimRuntime`: one thread, a seeded scheduler, and a clock that
only advances when you tell it to. Your actor code does not change. You write the
same handlers, send the same messages. Only the test harness differs.

`SimWorld` is that harness. It owns a `System` built on the `SimRuntime` plus the
executor that drives it.

## A first test

```rust,ignore
use std::time::Duration;
use murmer::SimWorld;

#[test]
fn worker_drains_on_a_timer() {
    let mut world = SimWorld::new(0xC0FFEE);

    let worker = world.system().start("worker/0", Worker, WorkerState::default());

    world.send(&worker, Submit { jobs: 5 }).unwrap();
    world.send(&worker, StartDraining { every_ms: 100 }).unwrap();

    // Move 250ms of virtual time. The drain timer fires at 100ms and 200ms.
    // This returns immediately. Nothing sleeps in real time.
    world.advance(Duration::from_millis(250));

    assert_eq!(world.send(&worker, Completed).unwrap(), 2);
}
```

Three methods do most of the work:

- `world.send(&endpoint, msg)` sends a message and drives the world until the
  reply comes back, then returns it. Use this instead of `endpoint.send(..).await`.
- `world.advance(duration)` moves virtual time forward, firing every timer that
  comes due and running the tasks they wake.
- `world.block_on(future)` drives the world until any future completes,
  advancing time as needed. `send` is built on top of it.

For lower-level control, `world.pump()` runs ready tasks to quiescence without
moving the clock, and `world.now()` reads the current virtual instant.

## Determinism and seeds

`SimWorld::new(seed)` seeds the scheduler and a PRNG. Draw from that PRNG with
`world.rng_u64()` to make your test's own choices reproducible:

```rust,ignore
fn run(seed: u64) -> u64 {
    let mut world = SimWorld::new(seed);
    let worker = world.system().start("worker/0", Worker, WorkerState::default());
    world.send(&worker, StartDraining { every_ms: 10 }).unwrap();

    for _ in 0..20 {
        let jobs = world.rng_u64() % 4;        // deterministic "random" load
        world.send(&worker, Submit { jobs }).unwrap();
        world.advance(Duration::from_millis(10));
    }
    world.advance(Duration::from_secs(1));
    world.send(&worker, Completed).unwrap()
}

assert_eq!(run(1), run(1)); // same seed, same outcome, every run
```

When a seed surfaces a bug, it stays surfaced. You keep the seed, you keep the
repro.

## What works under sim

Everything on a single node's local path: actor lifecycle, the supervisor loop,
`ctx.spawn`, `ctx.schedule_once`, `ctx.schedule_repeat`, message send and reply,
and restart backoff. Timers run on virtual time, so a test can fast-forward an
hour of heartbeats in microseconds.

`block_on` will panic with a clear message if the future it is driving can never
complete (every task is parked, no timer is pending). That almost always means
the future is awaiting a reply or message that no actor will ever send, which is
a real bug worth seeing.

## Current limits

This is single-node. `SimWorld` boots one `System`. It does not yet simulate a
network between nodes, so partition, latency, reorder, and node-death injection
across a cluster are not here. That arrives with the transport (`Net`) seam,
which lets the cluster run over an in-memory deterministic bus. Until then,
simulation covers the per-node actor logic, which is where most of the
non-network bugs live.

Scheduling order within a tick is plain FIFO today. A pluggable order policy,
for exploring adversarial interleavings, is a planned addition.

Determinism covers scheduling, virtual time, the seeded RNG, and the iteration
order of the registries that drive decisions. Those registries (the
receptionist's actor table, placement and coordinator maps) are `BTreeMap`s, so
they iterate in sorted key order instead of the per-process-random order a
`HashMap` would give. A check script, `scripts/check-determinism.sh`, keeps
Tokio and unseeded-RNG calls off the core path so the guarantee does not quietly
erode.

The real boundary right now is which features are wired to the runtime seam. The
basic actor path is: start and prepare, send and reply, `ctx.spawn`,
`schedule_once`/`schedule_repeat`, and restart. Two higher-level features are not
sim-ready yet: `PoolRouter`/`Router` and the `app` orchestration actors (the
Coordinator's drain and timeout loops). They still spawn on Tokio directly, so
they would panic under `SimWorld`. Routing them through the seam is the next
step, tracked in the check script's deferred list.

## How it fits together

The `sim` runtime is built on the `Runtime` seam (`murmer::runtime::Runtime`).
`TokioRuntime` is the default and production is unchanged. `SimRuntime`
implements the same trait with a seeded scheduler and virtual clock, and
`System::with_runtime(..)` is the injection point. You will not normally touch
those directly; `SimWorld::new` wires them for you.
