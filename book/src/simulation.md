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

## Multi-node testing with SimCluster

A single `SimWorld` boots one `System`. To test membership, failure detection,
and the cluster's reaction to faults, you need several nodes talking to each
other over a wire you control. `SimCluster` gives you that. It boots N
`ClusterSystem`s on one shared `SimRuntime`, wires them over an in-memory fabric,
and gives you a small set of verbs to inject faults and read what each node saw.

You build a cluster, mesh the nodes, and pump until they converge:

```rust,ignore
use std::time::Duration;
use murmer::cluster::net::sim_cluster::SimCluster;

let mut cluster = SimCluster::builder(1)
    .node("node-a")
    .node("node-b")
    .node("node-c")
    .build();

cluster.mesh();   // inject a full mesh of discovery edges
cluster.pump();   // converge membership
```

`builder(seed)` takes the same kind of seed `SimWorld` does, and the same seed
replays the same schedule across every node. `.node(name)` adds one node. Its
endpoint id is `"<name>-id"` and it gets its own port, so `.node("node-a")` is
reachable later as `"node-a"`. `.nodes(3)` is shorthand for `node-a`, `node-b`,
`node-c`.

`mesh()` injects a discovery edge between every pair of nodes. `pump()` runs the
ready tasks to quiescence. Convergence here is pump-only. You do not advance the
clock, because foca applies membership synchronously when the handshake lands. A
node coming up is seen right away, no probe round needed.

### Reading what each node saw

Every node holds an event receiver from the moment it boots. `events(name)`
drains everything that node has seen since the last time you called it, sorted
into three buckets:

- `joined`: the identities that came up, in arrival order. Each carries its full
  identity, so you can read the incarnation.
- `failed`: the endpoint ids the failure detector declared down.
- `pruned`: the endpoint ids removed from the registry.

The drain-since-last behavior is the whole trick. You drain once after `pump` to
throw away the convergence joins, then you drain again after a fault to read only
the failure phase. The two phases never mix in one read.

### Crashing a node

`crash(name)` cancels a node's shutdown token. Its event loop, readers, writers,
accept loop, and foca timer manager all stop. The node goes silent without
broadcasting a departure, so it looks like an abrupt crash rather than a graceful
leave. The survivors only notice once you advance the clock past foca's detection
budget. Thirty seconds is comfortable at any seed.

The real test, `crash_is_detected_failed_and_pruned_exactly`, checks that the
survivors detect exactly the crashed node and nothing else:

```rust,ignore
let only_a = BTreeSet::from(["node-a-id".to_string()]);
for seed in [1u64, 2, 0xC0FFEE] {
    let mut c = converged_trio(seed);
    // Discard the convergence joins so the next drain is the failure phase.
    let _ = (c.events("node-b"), c.events("node-c"));

    c.crash("node-a");
    c.advance(Duration::from_secs(30));

    for survivor in ["node-b", "node-c"] {
        let ev = c.events(survivor);
        assert_eq!(ev.failed, only_a, "{survivor} fails exactly A (seed {seed})");
        assert_eq!(ev.pruned, only_a, "{survivor} prunes exactly A (seed {seed})");
    }
}
```

`converged_trio` is the three-node mesh from above, pumped to convergence. The
test runs at three seeds and asserts the same outcome at each one.

### Partitioning a link

`partition(a, b)` severs the link between two nodes at the byte level. Both nodes
keep running. The streams between them fail in both directions. One call cuts both
ways. It returns `false` if there was no live connection to cut, which catches a
test that forgot to mesh first.

A single cut between two nodes in a healthy mesh should heal itself. foca probes
indirectly through the third node, so the membership never drops anyone. The test
`single_link_partition_is_masked_by_indirect_probing` asserts exactly that:

```rust,ignore
for seed in [1u64, 2, 0xC0FFEE] {
    let mut c = converged_trio(seed);
    for n in ["node-a", "node-b", "node-c"] {
        let _ = c.events(n); // drain convergence
    }
    assert!(c.partition("node-a", "node-b"), "A–B link is live");
    c.advance(Duration::from_secs(30));
    for n in ["node-a", "node-b", "node-c"] {
        assert!(
            !c.events(n).any_failed(),
            "{n} saw a failure — a single A–B cut must be masked by C (seed {seed})"
        );
    }
}
```

`any_failed()` is a quick "did this node suspect anyone" check on the drained
events. After thirty seconds of probing, nobody should have.

### Rejoining at a higher incarnation

`rejoin(name)` brings a crashed node back as the same endpoint id at a strictly
higher incarnation. The old cancelled system is dropped and a fresh one is bound
in its place. It does not re-establish links, so you `dial` or `mesh` it back to
the survivors and then advance. foca's conflict resolution readmits the returning
node because its incarnation outranks its own down instance.

`crashed_node_rejoins_at_higher_incarnation` walks the full cycle:

```rust,ignore
let mut c = converged_trio(1);
for n in ["node-a", "node-b", "node-c"] {
    let _ = c.events(n);
}
// Crash A; B and C detect it failed.
c.crash("node-a");
c.advance(Duration::from_secs(30));
let _ = (c.events("node-b"), c.events("node-c")); // clear the failure phase

// A returns as itself at incarnation 2 and re-dials the survivors.
c.rejoin("node-a");
assert_eq!(c.identity("node-a").incarnation, 2, "rejoin bumps the incarnation");
c.dial("node-a", "node-b");
c.dial("node-a", "node-c");
c.advance(Duration::from_secs(30));

// B and C readmit the returned A at incarnation 2 (higher incarnation wins).
let readmitted = |ev: &DrainedEvents| {
    ev.joined.iter().any(|id| id.endpoint_id.0 == "node-a-id" && id.incarnation == 2)
};
assert!(readmitted(&c.events("node-b")), "B readmits A@2");
assert!(readmitted(&c.events("node-c")), "C readmits A@2");
```

Notice the rhythm. Crash, advance to detect, drain the failure phase, rejoin,
re-dial, advance again, then drain the readmission. Each drain reads one phase.

## Adversarial scheduling

The default scheduler runs ready tasks in FIFO order, the order they were woken.
That is deterministic, but it is only one order. A bug that only shows up under a
different interleaving will not show up under FIFO no matter how many seeds you
throw at it.

Adversarial scheduling fixes that. Instead of FIFO, the executor picks a random
ready task each step. The choice comes from a seeded stream, so the interleaving
is still reproducible from the seed, but it is a deliberately shuffled order
rather than the natural one. On `SimWorld` you turn it on with
`world.use_random_scheduling()`. On a cluster you set it at build time with
`.random_scheduling()`.

The scheduler draws from its own seed stream, derived off the root seed, so it
never consumes from the actor RNG. Turning random scheduling on does not change
the value sequence your actors draw. What it changes is which actor gets to run
first when several are ready, so it changes which actor draws from the shared RNG
first. That is the point. You are reordering who runs when, while the randomness
stays the same.

### The oracle pattern

Random scheduling does not give you identical outcomes to compare against. A
different task order can change the cross-actor draw order, so you cannot just
assert "random run equals FIFO run" on every detail. What you assert is that the
property you care about holds either way. Run the same workload under FIFO and
under shuffled scheduling, and check that the observable outcome is invariant.

That is the oracle. The outcome you assert is the invariant. The scheduling is
the thing trying to break it. A green result under both is much stronger than a
green result under FIFO alone, because the property survived the interleaving
space instead of one lucky order.

`convergence_is_invariant_under_adversarial_scheduling` is the worked example.
The converged membership set must not depend on task order:

```rust,ignore
fn converged(seed: u64, adversarial: bool) -> BTreeSet<(String, String)> {
    let mut b = SimCluster::builder(seed)
        .node("node-a")
        .node("node-b")
        .node("node-c");
    if adversarial {
        b = b.random_scheduling();
    }
    let mut c = b.build();
    c.mesh();
    c.pump();
    let mut pairs = BTreeSet::new();
    for me in ["node-a", "node-b", "node-c"] {
        for id in c.events(me).joined_ids() {
            pairs.insert((me.to_string(), id));
        }
    }
    pairs
}

for seed in [1u64, 2, 0xC0FFEE] {
    let fifo = converged(seed, false);
    assert_eq!(fifo.len(), 6, "full mesh: each of 3 nodes sees 2 peers");
    assert_eq!(
        converged(seed, true),
        fifo,
        "convergence is invariant under adversarial scheduling (seed {seed})"
    );
}
```

The `converged` helper runs the real cluster substrate: the event loop, the
handshake and accept path, foca's SWIM protocol, the control streams. It collects
the membership each node ended up with, as a set of `(observer, peer)` pairs. The
test asserts the FIFO run produced the full mesh of six pairs, then asserts the
shuffled run produced the same six. The membership converged regardless of order.

This pattern carries past convergence. The same approach validates the failure
detector under shuffled order, and the cluster's coordination layer too. A short
coordination example: with one shared generation source behind every node's
coordinator, a partition that splits the leader off does not split the brain. The
new leader rebuilds the singleton's spec from the shared backend and adopts it at
a strictly higher term, so the fence rejects the stale owner. The
`send`/`send_async` calls in those tests are local to each node's own
coordinator. What crosses the cluster is membership and the shared source, not
application-actor messages between nodes.

## The layer model

It helps to see the simulation stack as three layers.

Layer 1 is the deterministic runtime engine: the seeded scheduler, the virtual
clock, the seeded RNG. That is `SimRuntime` and the executor inside `SimWorld`.
It makes one node's actor logic reproducible.

Layer 2 is the multi-node fault rig: `SimCluster`, the in-memory fabric, and the
crash, partition, and rejoin verbs. It makes a cluster's membership and failure
handling reproducible.

Both of those are murmer's, and you get them with the `sim` feature.

Layer 3 is yours. Disk faults, your own workloads, and the oracles that say
whether a run was correct belong to the code under test. The framework does not
own them. murmer hands you `derive_seed(label)`, which forks an independent reproducible
seed off the root, so your storage layer and your fault injector can each seed
their own stream without colliding with the actor RNG or with each other. The
whole stack descends from one seed.

## What the network models

The network model is faithful to murmer's transport, which is reliable ordered
streams. iroh is QUIC underneath, so a stream never drops a byte and never
reorders within itself. The fault injection respects that.

You inject latency with `.network_latency(base, jitter)` on the cluster builder.
It delays delivery on the virtual clock and adds seeded jitter, so a slow or
variable network replays from the seed like everything else. Order within a
stream is preserved, because a reliable stream preserves it. Across streams the
differing delays let messages to different actors arrive in a different relative
order, which is the faithful form of reordering. A lost connection is the
`partition` verb, a clean cut in both directions. What you will not find is
byte-level packet loss or in-stream reordering, because the real transport does
neither, and modeling them would surface bugs that cannot happen in production.

## Determinism, and what to assert

Outcomes are deterministic and replay from the seed. The scheduler is seeded, the
clock is virtual, the RNG is seeded, and the decision-bearing `tokio::select!`
branches on the sim path use biased polling, a fixed branch order, the same
policy the supervisor loop uses. A step with several ready branches resolves the
same way every run, so the converged set and the final state are reproducible.

The guidance that follows is to assert on the outcome a run produced rather than
on the incidental order unrelated events happened to arrive in. Membership
converges to the same set every run, so the membership tests assert on that set.
That habit is also what makes adversarial scheduling work: the outcome is the
invariant you pin, and the interleaving is the thing trying to break it.

## Runnable starting points

Two files in the repo are the place to start. One you run, one you copy.

`cargo run -p murmer-examples --bin sim_cluster_demo` boots a three-node cluster
on the sim runtime, crashes a node, shows the survivors detect it, and replays
the same scenario from the same seed to prove it is reproducible. The thirty
seconds of detection time it advances elapse in a few milliseconds of real time.
It is the quickest way to see the rig work.

`murmer/tests/sim_cluster.rs` is the template to copy. It is an external consumer
of murmer that uses only the public API, the way your own crate would. It covers
convergence, crash detection, partition tolerance, a remote actor reached across
nodes, a slow-network run, and the same-seed replay check. Its single-node
sibling, `murmer/tests/sim_world.rs`, is the same idea for one node's actors.

## How it fits together

The `sim` runtime is built on the `Runtime` seam (`murmer::runtime::Runtime`).
`TokioRuntime` is the default and production is unchanged. `SimRuntime`
implements the same trait with a seeded scheduler and virtual clock, and
`System::with_runtime(..)` is the injection point. `SimCluster` boots its nodes on
that same runtime through `ClusterSystem::start_with_net`, over an in-memory
fabric in place of the real QUIC transport. You will not normally touch those
directly. `SimWorld::new` and `SimCluster::builder` wire them for you.
