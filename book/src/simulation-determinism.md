# The Determinism Contract

The previous chapter showed you how to drive a `SimWorld`. This one is about the
rule that makes those green checks mean something. If you are writing tests on
top of the sim, read this first. It is short, and the whole thing rests on one
promise and one rule.

## The promise

Same seed, same run. `SimWorld::new(seed)` seeds the scheduler, the virtual
clock, and the PRNG, and from there the run is a pure function of the seed. Poll
order, timer firings, every randomness draw: all of it replays.

That promise is the entire point of the harness. It buys you two things.

A green run you can trust. If a test passes under sim, it passed because the
logic is correct, not because the scheduler happened to interleave tasks in a
lucky order this time. There is no lucky order. There is one order per seed.

A red run you can replay. When a seed surfaces a bug, you keep the seed and you
keep the repro. You can rerun it as many times as you need, add logging, step
through it, and it lands in the same place every time. A flaky failure you
cannot reproduce is worthless. A deterministic failure is a fixed target.

Both of those depend on the run actually being a function of the seed. The
moment anything in your code pulls from a source that is not the seed, both
break at once. The green run stops being trustworthy and the red seed stops
replaying. So there is a rule.

## The rule

Your code goes through the seams.

For concurrency and time and randomness, route through the `Runtime` seam.
Inside an actor that means `ctx.spawn`, `ctx.schedule_once`,
`ctx.schedule_repeat`, and for randomness the world's seeded RNG. The sim swaps
a `SimRuntime` under the whole framework, so when your actor calls `ctx.spawn`
the task lands on the deterministic executor, and when it sleeps it sleeps on
virtual time.

Never reach around the seam to touch the real runtime, the real clock, or the
global RNG on a path that runs under sim. Concretely, never:

- `tokio::spawn` or any `tokio::time::*` timer
- `Instant::now()` or `SystemTime::now()` feeding a decision
- `rand::random()`, `rand::thread_rng()`, or any unseeded draw

Each of those is a source the seed does not control. Each one is a hole in the
promise.

There is a second seam, and it is already here. The `Net` seam makes the
transport between nodes deterministic the same way `Runtime` does for one node,
which is what lets `SimCluster` boot a whole cluster on a single virtual clock.
The same rule applies to code on that path. A raw spawn or an unseeded draw on
the cross-node path breaks replay just as it does on a single node, and the
multi-node suites run over the `Net` seam to keep it honest.

## Two ways to break it, one that is dangerous

Break the rule and you land in one of two failure modes. They are not equally
bad, and the difference is the whole reason this chapter exists.

The loud one is a raw `tokio::spawn` or `tokio::time` call on a sim path. There
is no Tokio runtime under the sim executor, so the call panics the instant it
runs. You will see a panic about there being no reactor running. It is jarring
the first time, but it is the safe failure. The run stops, you see exactly where,
and you route the call through the seam. Nothing silently rots. The panic is the
sim telling you it found a corner of your code it cannot make deterministic yet.
That is a coverage gap, and you found it on the spot.

The quiet one is the enemy. An unseeded `rand::random()`, a wall-clock read that
steers a branch, or iteration over a `HashMap` on a decision path. None of these
panic. They run fine. They just draw from a source the seed does not own, so the
run quietly stops being a function of the seed. The test still goes green,
because most of the time the nondeterminism does not change the observable
outcome. Then one day it does, and now you have a green test that was lying and a
red seed that will not replay. You debug it, the repro evaporates, and you have
lost the one thing the harness was built to give you.

So the silent failure is the one to fear. The loud one tells on itself. The
quiet one corrupts the promise while every check stays green.

The `HashMap` case is worth calling out because it does not look like
randomness. A `HashMap` iterates in per-process random order. Iterate one on a
path that decides something, an order of operations, who wins a tie, and the
decision changes from run to run even though no `rand` call is in sight. The fix
is a `BTreeMap`: it iterates in sorted key order, so the iteration is part of the
seeded run. The receptionist's actor table (`entries`) is a `BTreeMap` for
exactly this reason. Keep decision-path registries sorted.

## How the gate enforces it

The discipline is backed by a check script, `scripts/check-determinism.sh`. It
scans the core modules that are routed through the seam and fails if any of the
banned tokens appears unmarked:

```
tokio::spawn | tokio::time:: | Instant::now | SystemTime::now | rand::(rng|random|thread_rng)
```

A comment line is fine. So is a line carrying an explicit marker:

```rust
// determinism-gate: allow — <reason>
```

The marker is for the handful of sites that genuinely do not affect determinism,
or that are consciously deferred. Two kinds show up in the tree today. One is
measurement-only instrumentation, like a monitor timing a remote send, where the
`Instant::now()` feeds a metric and never a branch. The other is a deferred
path, like a debounce that still spawns on Tokio because it has not been routed
yet. Both carry the marker with a reason so the exception is visible and
reviewable, not silent.

Run the gate from a pre-commit hook or as a CI step. The script is honest about
its reach: it gates only where it is invoked, so wire it in. It is also honest
about what it cannot see. It catches Tokio, clock, and RNG escape hatches by
token, the loud-adjacent class. It does not catch a new decision-bearing
`HashMap` iteration, because a blanket map ban has too many false positives.
That half is on you in review, guarded by one sim test
(`listing_backfill_order_is_deterministic`).

Read that together with the previous section and the shape is clear. The runtime
catches the loud failure for you, it panics. The gate catches the escape-hatch
tokens. The dangerous failure, the silent decision-path `HashMap`, is precisely
the one the gate cannot see by token, which is why the `BTreeMap` rule is a rule
and not a suggestion.

## One seed, one root

You will often have sub-systems that need their own randomness: the scheduler
when you run adversarial interleavings, a fault injector, a consumer's simulated
disk faults. You do not want each of them seeded separately, because then a repro
needs N seeds instead of one, and two consumers drawing from the same stream
perturb each other's draw order.

`derive_seed(label)` is the primitive that keeps it to one seed. Each sub-stream
seeds itself off the one root by label:

```rust,ignore
let scheduler_seed = world.derive_seed("scheduler");
let disk_seed      = world.derive_seed("consumer/disk-faults");
```

Each call is a pure function of the root seed and the label, so the whole stack
descends from a single `u64`. The streams are independent, so the fault injector
drawing does not shift what the scheduler draws. The adversarial scheduler is
built on exactly this: `world.use_random_scheduling()` seeds its `RandomPolicy`
from `derive_seed("scheduler")`, a stream separate from the actor RNG, so turning
random scheduling on does not change the value sequence the actors draw. One root
seed reproduces the entire run, every sub-stream included.

## Troubleshooting

**Panic about no reactor running.** A raw `tokio::spawn` or `tokio::time` call
reached a sim path. There is no Tokio runtime under the sim executor. Find the
call and route it through the seam: `ctx.spawn`, `ctx.schedule_once`, or
`ctx.schedule_repeat`. This is the safe failure. You found a path that is not
sim-ready yet.

**Not reproducible across runs.** Something is drawing from a source the seed
does not control. Look for an unseeded `rand` call, a wall-clock read
(`Instant::now` / `SystemTime::now`) that steers a decision, or a `HashMap`
iterated on a decision path. Run the gate to catch the first two. For the third,
switch the registry to a `BTreeMap`. Replace any test randomness with
`world.rng_u64()` so it draws from the seeded stream.

**`block_on` panics with a sim deadlock.** Every task is parked, no timer is
pending, and the future you are driving still has not completed. That almost
always means the future is awaiting a reply or a message that no actor will ever
send. It is a real bug worth seeing, not a harness quirk. Check what the awaited
future is waiting on and whether anything in the run actually produces it.
