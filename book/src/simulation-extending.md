# Extending Simulation

murmer gives you a deterministic world. It does not give you every fault you will
want to test. The clock, the scheduler, and the in-memory network belong to murmer.
The disk faults and the oracles that check them are yours to write. This chapter is
the handoff guide. It walks through building your own simulation layer on top
of the seams murmer exposes, using a fault-injecting filesystem as the running
example.

## The ownership boundary

murmer owns the engine. The `SimRuntime` is the seeded scheduler and the virtual
clock. `SimWorld` drives one node. `SimCluster` boots N nodes over an in-memory
wire and injects crash, partition, and rejoin. All of that replays from one seed.
You build on top of it.

You own your faults. murmer ships no disk dependency. There is no storage trait in
the framework, no file abstraction, nothing for a disk-fault simulator to hook.
That is on purpose. A storage application has its own idea of what durable state
looks like and how it should fail, and murmer does not want to guess. So the
disk-fault layer is yours to write. murmer's job is to hand you a seeded
deterministic world to hang it on.

Call this Layer 3. Layer 1 is the runtime and the network seams. Layer 2 is the
harness (`SimWorld`, `SimCluster`). Layer 3 is your domain: the faults murmer does
not know about and the oracles that check your application's invariants.

## The seam primitives you build on

Three primitives carry the whole Layer-3 pattern.

**One seed, one root.** `world.derive_seed("vfs")` gives you an independent `u64`
seeded off the world's root seed. You feed it into whatever PRNG your fault stream
uses. Because it descends from the one root seed, your faults replay alongside
everything else murmer does, from a single `u64`. Distinct labels give distinct
streams, so your fault stream and murmer's own draws do not perturb each other:

```rust,ignore
let mut world = SimWorld::new(0xC0FFEE);
let fs_seed = world.derive_seed("vfs");    // independent, reproducible
let other  = world.derive_seed("faults");  // a different sub-stream
assert_ne!(fs_seed, other);
```

`derive_seed` does not draw from the world's main RNG stream. Calling it does not
shift the values `world.rng_u64()` will return. Two consumers can each derive their
own seed without stealing each other's draw order. This is the same property the
sim tests assert in `derive_seed_is_reproducible_and_label_independent`.

One honesty note. `derive_seed` is reproducible only under the sim runtime. Under
the default `TokioRuntime` it hands back fresh entropy on every call, because
production is not meant to replay. Reproducibility is a sim-only property.

**Synchronous storage calls.** Real storage code blocks. You read a file, you
fsync, you wait on the kernel. The `Runtime` trait has `run_blocking` for exactly
this. In production it offloads to a blocking thread so real I/O never stalls the
actor pool. In sim it runs the work inline on the deterministic thread as one
atomic step, because sim "blocking" work returns promptly from in-memory state and
a sim scheduler cannot observe an uncontrolled thread pool.

The signature matters. `run_blocking` takes a closure returning `()` and gives back
a future that resolves to `()`:

```rust,ignore
fn run_blocking(&self, work: Box<dyn FnOnce() + Send + 'static>) -> BoxFuture<'static, ()>;
```

To get a value back out of the closure, capture a channel or a shared cell and
write into it from inside the work, then read it after the future resolves. Do not
expect `run_blocking` to return your bytes directly.

**The virtual clock.** Anything in your Layer-3 code that waits on time goes
through the runtime, not through `tokio::time` or `Instant::now`. The runtime's
`sleep` and `now` are virtual under sim, so a fault that fires after a delay (a slow
disk, a write that lands late) advances when the test advances the clock. If you
reach for the real clock, your fault stops being deterministic and you have a hole.

## The shape of a Layer-3 add-on

Here is the shape, with a fault-injecting filesystem as the example. Three pieces.

First, a trait your application code calls. Your code never touches `std::fs`
directly. It goes through this seam, the same way murmer's actor code never touches
`tokio::spawn` directly.

```rust,ignore
// Your trait. murmer does not ship this. You write it.
trait Filesystem: Send + Sync {
    fn read(&self, path: &str) -> std::io::Result<Vec<u8>>;
    fn write(&self, path: &str, bytes: &[u8]) -> std::io::Result<()>;
    fn fsync(&self, path: &str) -> std::io::Result<()>;
}
```

Second, a real implementation for production. It does the obvious thing: reads and
writes actual files.

```rust,ignore
struct RealFs;

impl Filesystem for RealFs {
    fn read(&self, path: &str) -> std::io::Result<Vec<u8>> {
        std::fs::read(path)
    }
    // ...write, fsync against the real disk
}
```

Third, a deterministic fault implementation for sim, seeded off `derive_seed`. It
holds an in-memory store and a seeded PRNG. Before each operation it draws from the
PRNG and decides whether to inject a fault: a torn write, a short read, an fsync
that reports success but drops the data, a disk-full error. Same seed, same faults,
every run.

```rust,ignore
struct SimFaultFs {
    store: Mutex<HashMap<String, Vec<u8>>>,
    rng: Mutex<ChaCha8Rng>,
}

impl SimFaultFs {
    fn new(seed: u64) -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            rng: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }
}

impl Filesystem for SimFaultFs {
    fn write(&self, path: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Draw from the seeded stream, decide whether to fault.
        if self.rng.lock().unwrap().next_u64() % 100 < 5 {
            return Err(std::io::Error::other("simulated disk full"));
        }
        self.store.lock().unwrap().insert(path.into(), bytes.to_vec());
        Ok(())
    }
    // ...read, fsync with their own fault draws
}
```

You build the `SimFaultFs` with a seed from the world, then hand it to your actors:

```rust,ignore
let mut world = SimWorld::new(0xC0FFEE);
let fs = Arc::new(SimFaultFs::new(world.derive_seed("vfs")));
let rt: Arc<dyn Runtime> = Arc::new(world.runtime().clone());
let store = world.system().start("store/0", StoreActor::new(fs, rt), StoreState::default());
```

Inside the actor, the `Filesystem` calls are synchronous, so they go through
`run_blocking` to run as one atomic sim step instead of stalling the deterministic
thread. Give the actor a handle to the `Runtime` (you constructed the actor, so you
wire it in), and call `run_blocking` on that. The closure returns `()`, so capture a
channel to carry the result out:

```rust,ignore
// `rt: Arc<dyn Runtime>` and `fs: Arc<dyn Filesystem>` are held by the actor.
let fs = fs.clone();
let (tx, rx) = tokio::sync::oneshot::channel();
rt.run_blocking(Box::new(move || {
    let _ = tx.send(fs.read("store/segment-0"));
})).await;
let bytes = rx.await.unwrap();
```

The `Filesystem` trait, `RealFs`, and `SimFaultFs` are yours. murmer does not ship
them, and it should not. They are illustrations of the pattern you write for your
own domain. Swap "filesystem" for whatever your application's durable surface
actually is.

### This mirrors how murmer seeds its own scheduler

You are not inventing this pattern. murmer already uses it on itself, and the
parallel is exact.

murmer's scheduler has a `ReadyPolicy` trait, the seam that decides which ready
task to poll next. The default is `FifoPolicy`, which draws no randomness. The
adversarial implementation is `RandomPolicy`, which picks a random ready task each
step. And `RandomPolicy` is seeded off the root seed through `derive_seed`. When you
call `world.use_random_scheduling()`, murmer does this:

```rust,ignore
let seed = self.derive_seed("scheduler");
self.set_policy(Box::new(RandomPolicy::new(seed)));
```

Line that up against your filesystem:

- `ReadyPolicy` is the seam trait. Your `Filesystem` is the seam trait.
- `FifoPolicy` is the plain implementation. `RealFs` is the plain implementation.
- `RandomPolicy` is the fault implementation. `SimFaultFs` is the fault implementation.
- `RandomPolicy` is seeded from `derive_seed("scheduler")`. `SimFaultFs` is seeded
  from `derive_seed("vfs")`.

murmer seeds its scheduler's fault stream off `derive_seed("scheduler")`. You seed
your disk's fault stream off `derive_seed("vfs")`. Same root seed, a different
label. Copy the pattern.

## The discipline travels with it

Layer 3 only stays deterministic if your code obeys the same seam rules murmer's
core does. The discipline does not stop at the boundary. It comes with the pattern.

No `tokio::spawn` in your fault layer. No `tokio::time`. No `Instant::now`. No
unseeded `rand`. No `HashMap` iteration in a path that drives a decision your oracle
checks, because `HashMap` order is per-process random and will desync your replay.
Every one of these is a way to smuggle nondeterminism past the seed, and any one of
them turns a reproducible failure back into a flaky one.

This is the same set of rules the determinism chapter spells out for murmer's own
code, and the same rules `scripts/check-determinism.sh` enforces on the core path.
See [the determinism chapter](./simulation-determinism.md) for the full list and
the reasoning. When you build Layer 3, you are signing up for that discipline in
your own code.

## End-to-end oracles

The other half of Layer 3 is the oracle: the check that your application's
invariant held across the whole run. murmer's sim tests already do this. The cluster
oracles run a workload under FIFO and again under adversarial scheduling and assert
the observable outcome is the same either way. You write the equivalent for your
domain.

With the fault filesystem in place, an oracle looks like a property you assert after
driving the world. Crash an actor mid-write, advance the clock, restart it, then
read back and assert no acknowledged write was lost. Run the same workload across a
range of seeds. The faults change with the seed, the invariant does not. When a seed
breaks it, you keep the seed and you keep the repro, the same way you would for any
sim failure.

## Where the boundary could move

Today the boundary is clean. murmer holds no durable state of its own, so a
disk-fault simulator touches only your code and never murmer's. The framework keeps
its actors and registries in memory, and persistence is the application's concern.

That could change. If murmer ever takes on durable state of its own, a Raft log for
the singleton fence being the obvious candidate, then disk faults would start
touching murmer too. At that point murmer would owe you a storage seam of its own, so
its log could run against your fault filesystem the way its scheduler runs against
your fault policy. It does not today. As long as murmer's state lives in memory, the
disk layer is entirely yours, and the seeded deterministic world is what murmer
gives you to build it on.
