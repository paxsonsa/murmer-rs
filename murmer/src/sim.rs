//! Deterministic simulation runtime and single-node harness (`feature = "sim"`).
//!
//! [`SimRuntime`] implements [`Runtime`] with a single-threaded, seeded
//! scheduler and a virtual clock. [`SimWorld`] drives it: it boots a `System`
//! on the sim runtime and lets a test step the world deterministically —
//! `block_on` a send, `advance` virtual time to fire timers, draw seeded
//! randomness — all reproducible from one seed.
//!
//! This is the murmer-owned, reusable engine: any client can simulation-test
//! their own actors against it without writing a scheduler. Domain-specific
//! seams (storage faults, oracles, workloads) are the client's to layer on top.
//!
//! # What this layer is and isn't
//!
//! This is **single-node** (Layer 1 + a single-node harness). It makes one
//! `System`'s actor lifecycle, timers, and randomness deterministic. Booting N
//! *communicating* nodes with partition/latency injection needs the network
//! (`Net`) seam and is the follow-up — there is no inter-node transport here.
//!
//! # Determinism model
//!
//! The executor core is [`SimExecutor`], a minimal single-threaded task pool: a
//! task store keyed by a monotonic id, plus a ready set the wakers feed. The
//! driver consults a pluggable [`ReadyPolicy`] to choose which ready task to poll
//! next. The default [`FifoPolicy`] polls in wake order (a plain FIFO pool, and
//! the behavior the old `futures_executor::LocalPool` gave us) and draws no
//! randomness — so it is deterministic given deterministic input. `SimWorld` owns
//! the executor alongside the timer heap and virtual clock: when no task can make
//! progress, it jumps the clock to the next timer (FDB's "no work → advance to
//! next event"). The policy seam is what adversarial interleaving / buggify plugs
//! into: a seeded policy can deliberately pick nasty ready-orders the FIFO default
//! would never reach.
//!
//! **Scope of determinism today:** task scheduling, virtual-time timers, the
//! seeded RNG, and decision-path collection iteration order are all
//! reproducible. The decision-bearing registries (receptionist `entries`,
//! placement/coordinator maps) are `BTreeMap`s, so iteration is sorted-by-key
//! rather than `HashMap`'s per-process-random order, and a CI gate
//! (`scripts/check-determinism.sh`) keeps Tokio/RNG escape hatches off the core
//! local path.
//!
//! The remaining boundary is *feature coverage*, not nondeterminism: the basic
//! actor path (start/prepare, send/reply, `ctx.spawn`, `schedule_*`, restart)
//! is fully on the runtime seam. Higher-level features — `PoolRouter`/`Router`
//! and the `app` orchestration actors (Coordinator drain/timeout loops) — still
//! spawn on Tokio directly, so they are not sim-ready yet (they would panic
//! under `SimWorld`). Routing them is a tracked follow-up.
//!
//! # Example
//!
//! ```rust,ignore
//! let mut world = SimWorld::new(0xC0FFEE);
//! let counter = world.system().start("counter/0", Counter, CounterState { count: 0 });
//! let n = world.block_on(counter.send(Increment { amount: 5 }));
//! assert_eq!(n, 5);
//! world.advance(Duration::from_secs(1)); // fire any scheduled timers
//! ```

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::future::LocalBoxFuture;
use futures_util::task::{ArcWake, waker};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::actor::{Actor, Handler, RemoteMessage};
use crate::endpoint::Endpoint;
use crate::runtime::{BoxFuture, Runtime, SpawnHandle};
use crate::wire::SendError;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A pending timer: wake `waker` once virtual time reaches `deadline`.
struct Timer {
    deadline: Duration,
    waker: Waker,
}

/// Shared mutable state of the sim runtime. Behind a `Mutex` so the runtime can
/// be `Send + Sync` (required by `Arc<dyn Runtime>`), even though it is only
/// ever touched from the single driver thread.
struct SimShared {
    now: Duration,
    seed: u64,
    rng: ChaCha8Rng,
    /// Futures handed to `Runtime::spawn`, awaiting drain into the executor by
    /// the driver. Decouples spawning (`Send`, called from anywhere) from the
    /// `!Send` [`SimExecutor`] that actually owns the tasks.
    inbox: Vec<BoxFuture<'static, ()>>,
    timers: Vec<Timer>,
}

/// Deterministic [`Runtime`]: seeded scheduler + virtual clock.
///
/// Cheap to clone (shares one inner state). Build a [`SimWorld`] to drive it,
/// or pass `Arc::new(rt)` to [`System::with_runtime`](crate::System::with_runtime)
/// directly for full control.
#[derive(Clone)]
pub struct SimRuntime {
    shared: Arc<Mutex<SimShared>>,
}

impl SimRuntime {
    /// Create a runtime seeded with `seed`. The same seed reproduces the same
    /// schedule, timer firings, and randomness.
    pub fn new(seed: u64) -> Self {
        Self {
            shared: Arc::new(Mutex::new(SimShared {
                now: Duration::ZERO,
                seed,
                rng: ChaCha8Rng::seed_from_u64(seed),
                inbox: Vec::new(),
                timers: Vec::new(),
            })),
        }
    }
}

impl Runtime for SimRuntime {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> SpawnHandle {
        self.shared.lock().unwrap().inbox.push(fut);
        // Top-level task abort is a no-op in sim; scheduled futures cancel
        // cooperatively via their own flags (see `ScheduleHandle`).
        SpawnHandle::noop()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(Sleep {
            shared: self.shared.clone(),
            deadline: None,
            dur,
        })
    }

    fn now(&self) -> Duration {
        self.shared.lock().unwrap().now
    }

    fn rng_u64(&self) -> u64 {
        self.shared.lock().unwrap().rng.next_u64()
    }

    fn derive_seed(&self, label: &str) -> u64 {
        // Deterministic function of (root seed, label). DefaultHasher uses fixed
        // keys (unlike RandomState), so this is reproducible across runs.
        use std::hash::{Hash, Hasher};
        let seed = self.shared.lock().unwrap().seed;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut h);
        label.hash(&mut h);
        h.finish()
    }

    fn run_blocking(&self, work: Box<dyn FnOnce() + Send + 'static>) -> BoxFuture<'static, ()> {
        // Inline-atomic: run synchronously on the deterministic thread. Valid
        // because sim "blocking" work returns promptly from in-memory state.
        work();
        Box::pin(async {})
    }
}

/// Future returned by [`SimRuntime::sleep`]. Registers a timer with the shared
/// state on first poll and parks until virtual time reaches its deadline.
struct Sleep {
    shared: Arc<Mutex<SimShared>>,
    deadline: Option<Duration>,
    dur: Duration,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut s = this.shared.lock().unwrap();
        let now = s.now;
        let deadline = *this.deadline.get_or_insert(now + this.dur);
        if now >= deadline {
            Poll::Ready(())
        } else {
            s.timers.push(Timer {
                deadline,
                waker: cx.waker().clone(),
            });
            Poll::Pending
        }
    }
}

// =============================================================================
// DETERMINISTIC EXECUTOR — a minimal single-threaded task pool with a pluggable
// ready-order policy (the seam adversarial scheduling / buggify plugs into).
// =============================================================================

/// Monotonic, never-reused task id. Never recycling ids is a safety invariant: a
/// timer's `Waker` captures the id at poll time, so if an id were reused after a
/// task completed, a stale wake could land on a different task (an ABA bug). A
/// wake for an absent id is simply a no-op.
type TaskId = u64;

/// A spawned task: its future plus the one `Waker` reused across polls (the waker
/// only carries an id + the shared ready handle, so one per task suffices).
struct SimTask {
    future: LocalBoxFuture<'static, ()>,
    waker: Waker,
}

/// The `Send + Sync` half of the executor: the ready set, shared with every
/// `Waker`. Wakers only `push`; the driver thread is the sole drainer. It carries
/// task ids only — never the (`!Send`) futures.
struct ReadyState {
    /// Ready ids in wake order (first-wake order); [`FifoPolicy`] pops the front.
    queue: VecDeque<TaskId>,
    /// Dedup: an id already queued is not enqueued twice. Covers double-wakes and
    /// a task waking itself mid-poll (`yield_now`).
    queued: HashSet<TaskId>,
}

impl ReadyState {
    fn push(&mut self, id: TaskId) {
        if self.queued.insert(id) {
            self.queue.push_back(id);
        }
    }

    /// Take all currently-ready ids for the policy to order. Clears the dedup set:
    /// a task re-woken while its id is already in the driver's working batch will
    /// be re-enqueued — at worst one extra, harmless poll (futures tolerate
    /// spurious polls).
    fn drain(&mut self) -> Vec<TaskId> {
        self.queued.clear();
        self.queue.drain(..).collect()
    }
}

/// Wakes a task by pushing its id into the shared ready set. Implementing
/// [`ArcWake`] gives correct clone/wake/drop refcounting without a hand-rolled
/// `RawWakerVTable`.
struct SimWaker {
    id: TaskId,
    ready: Arc<Mutex<ReadyState>>,
}

impl ArcWake for SimWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.ready.lock().unwrap().push(arc_self.id);
    }
}

/// Decides which ready task to poll next: given the currently-ready ids, return
/// the index of the one to poll. The default ([`FifoPolicy`]) reproduces a plain
/// FIFO pool; a seeded adversarial policy is how the sim deliberately explores
/// nasty interleavings. Implementations must be deterministic given their own
/// state, and must not spawn tasks or touch the runtime — they only choose order.
pub trait ReadyPolicy {
    /// `ready` is always non-empty. The returned index must be in bounds.
    fn pick(&mut self, ready: &[TaskId]) -> usize;
}

/// The default policy: always poll the earliest-woken ready task. Draws no
/// randomness, so swapping the old `LocalPool` for this executor is observably a
/// no-op for every existing test.
pub struct FifoPolicy;

impl ReadyPolicy for FifoPolicy {
    fn pick(&mut self, _ready: &[TaskId]) -> usize {
        0
    }
}

/// A minimal single-threaded executor: a task store keyed by id, plus the shared
/// ready set the wakers feed. `!Send` (it owns the futures); lives inside
/// [`SimWorld`] on the driver thread.
struct SimExecutor {
    tasks: BTreeMap<TaskId, SimTask>,
    next_id: TaskId,
    ready: Arc<Mutex<ReadyState>>,
}

impl SimExecutor {
    fn new() -> Self {
        Self {
            tasks: BTreeMap::new(),
            next_id: 0,
            ready: Arc::new(Mutex::new(ReadyState {
                queue: VecDeque::new(),
                queued: HashSet::new(),
            })),
        }
    }

    /// Admit a future as a fresh task, immediately ready for its first poll.
    fn spawn(&mut self, future: LocalBoxFuture<'static, ()>) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        let w = waker(Arc::new(SimWaker {
            id,
            ready: Arc::clone(&self.ready),
        }));
        self.tasks.insert(id, SimTask { future, waker: w });
        self.ready.lock().unwrap().push(id);
        id
    }

    /// Poll one task. A missing id (already completed) is a no-op — that is how a
    /// stale timer wake for a finished task is absorbed. A `Ready` task is removed.
    fn poll_task(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        let w = task.waker.clone();
        let mut cx = Context::from_waker(&w);
        if task.future.as_mut().poll(&mut cx).is_ready() {
            self.tasks.remove(&id);
        }
    }

    /// Take the currently-ready ids (drains the shared ready set).
    fn take_ready(&self) -> Vec<TaskId> {
        self.ready.lock().unwrap().drain()
    }
}

/// A single-node deterministic harness: a `System` on a [`SimRuntime`], plus the
/// executor that drives it.
///
/// `SimWorld` is `!Send` (it owns the [`SimExecutor`]); it lives on the test
/// thread and is stepped explicitly. All time is virtual: nothing here sleeps in
/// real wall-clock time.
pub struct SimWorld {
    runtime: SimRuntime,
    executor: SimExecutor,
    /// The ready-order policy. Defaults to [`FifoPolicy`]; swap it to drive
    /// adversarial scheduling.
    policy: Box<dyn ReadyPolicy>,
    system: crate::System,
}

impl SimWorld {
    /// Build a world seeded with `seed`. The `System` inside runs entirely on
    /// the deterministic runtime.
    pub fn new(seed: u64) -> Self {
        let runtime = SimRuntime::new(seed);
        let system = crate::System::with_runtime(Arc::new(runtime.clone()));
        Self {
            runtime,
            executor: SimExecutor::new(),
            policy: Box::new(FifoPolicy),
            system,
        }
    }

    /// The simulated system. Start actors, look them up, subscribe to events —
    /// the same `System` API as production.
    pub fn system(&self) -> &crate::System {
        &self.system
    }

    /// The underlying runtime handle (e.g. to clone into a second `System`).
    pub fn runtime(&self) -> &SimRuntime {
        &self.runtime
    }

    /// Send `msg` to `ep` and drive the world until the reply arrives.
    ///
    /// Convenience over [`block_on`](Self::block_on): clones the endpoint and
    /// owns the send future so it satisfies the `'static` bound. This is the
    /// common way a sim test calls into actors.
    pub fn send<A, M>(&mut self, ep: &Endpoint<A>, msg: M) -> Result<M::Result, SendError>
    where
        A: Actor + Handler<M> + 'static,
        M: RemoteMessage + 'static,
        M::Result: Serialize + DeserializeOwned + 'static,
    {
        let ep = ep.clone();
        self.block_on(async move { ep.send(msg).await })
    }

    /// Current virtual time (since the world started).
    pub fn now(&self) -> Duration {
        self.runtime.shared.lock().unwrap().now
    }

    /// Draw a deterministic `u64` from the world's seeded RNG. Use this to make
    /// test choices (which actor to message, what payload) reproducible.
    pub fn rng_u64(&self) -> u64 {
        self.runtime.rng_u64()
    }

    /// Derive an independent, reproducible seed for `label` off this world's
    /// root seed (the "one seed, one root" primitive). A subsystem under the
    /// same simulation seeds its own RNG/fault stream from this, so the whole
    /// stack descends from one seed without cross-coupling.
    pub fn derive_seed(&self, label: &str) -> u64 {
        self.runtime.derive_seed(label)
    }

    /// Move every queued task from the runtime inbox into the executor.
    fn drain_inbox(&mut self) {
        let futs: Vec<BoxFuture<'static, ()>> = {
            let mut s = self.runtime.shared.lock().unwrap();
            s.inbox.drain(..).collect()
        };
        for fut in futs {
            // The inbox holds `Send` futures (`BoxFuture`); the executor stores
            // them as `!Send` `LocalBoxFuture`s. Every `Send + 'static` future
            // satisfies the looser `'static` bound, so this coercion is sound.
            let local: LocalBoxFuture<'static, ()> = fut;
            self.executor.spawn(local);
        }
    }

    fn inbox_nonempty(&self) -> bool {
        !self.runtime.shared.lock().unwrap().inbox.is_empty()
    }

    fn earliest_deadline(&self) -> Option<Duration> {
        self.runtime
            .shared
            .lock()
            .unwrap()
            .timers
            .iter()
            .map(|t| t.deadline)
            .min()
    }

    /// Advance virtual time to `target` (must be >= now) and wake every timer
    /// due at or before it.
    fn set_now_and_fire(&mut self, target: Duration) {
        let due: Vec<Waker> = {
            let mut s = self.runtime.shared.lock().unwrap();
            s.now = target;
            let mut due = Vec::new();
            let mut i = 0;
            while i < s.timers.len() {
                if s.timers[i].deadline <= target {
                    due.push(s.timers.swap_remove(i).waker);
                } else {
                    i += 1;
                }
            }
            due
        };
        for w in due {
            w.wake();
        }
    }

    /// Run all ready tasks to quiescence *without* advancing time. Drains any
    /// tasks they spawn. Returns when no task can make progress at the current
    /// virtual instant.
    ///
    /// Each step drains the inbox into the executor, then runs the ready batch:
    /// the [`ReadyPolicy`] picks the next task, it is polled, and any tasks it
    /// wakes are folded into the batch. When the batch empties and nothing new was
    /// spawned, the world has quiesced.
    pub fn pump(&mut self) {
        loop {
            self.drain_inbox();
            let mut ready = self.executor.take_ready();
            if ready.is_empty() {
                // A poll may have spawned more via `Runtime::spawn` → drain again;
                // otherwise we have reached quiescence.
                if self.inbox_nonempty() {
                    continue;
                }
                break;
            }
            while !ready.is_empty() {
                let idx = self.policy.pick(&ready);
                let id = ready.remove(idx);
                self.executor.poll_task(id);
                // Fold in anything this poll woke before picking the next task.
                ready.extend(self.executor.take_ready());
            }
        }
    }

    /// Drive the world until `fut` completes, advancing virtual time as needed
    /// to fire timers the future is waiting on. Returns the future's output.
    ///
    /// Panics if the future can never complete (all tasks stalled with no
    /// pending timers and nothing left to run) — a sim deadlock, which almost
    /// always means the future is awaiting something no actor will produce.
    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + 'static,
    {
        let out: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let sink = out.clone();
        // Inject the sink task straight into the executor (we hold `&mut self`, so
        // no inbox round-trip is needed).
        self.executor.spawn(Box::pin(async move {
            let r = fut.await;
            *sink.lock().unwrap() = Some(r);
        }));

        loop {
            self.pump();
            if out.lock().unwrap().is_some() {
                break;
            }
            match self.earliest_deadline() {
                Some(deadline) => self.set_now_and_fire(deadline),
                None => panic!(
                    "sim deadlock in block_on: all tasks stalled at t={:?} with no pending \
                     timers — the awaited future cannot complete (waiting on a message or reply \
                     that no actor will send?)",
                    self.now()
                ),
            }
        }

        out.lock().unwrap().take().expect("block_on output was set")
    }

    /// Advance virtual time by `by`, firing every timer that comes due along the
    /// way and running the tasks they wake. Used to exercise scheduled work
    /// (`schedule_once` / `schedule_repeat`, heartbeats, backoff) deterministically.
    pub fn advance(&mut self, by: Duration) {
        let target = self.now() + by;
        loop {
            self.pump();
            match self.earliest_deadline() {
                Some(deadline) if deadline <= target => self.set_now_and_fire(deadline),
                _ => break,
            }
        }
        // Land exactly on the target even if no timer sits there.
        self.set_now_and_fire(target);
        self.pump();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorContext;
    use crate::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    // A trivial counter actor exercised entirely under the sim runtime. It also
    // schedules a self-tick on start, to exercise the timer path under sim.
    struct Counter;

    #[derive(Default)]
    struct CounterState {
        count: i64,
        ticks: u64,
        timer: Option<ScheduleHandle>,
    }

    impl Actor for Counter {
        type State = CounterState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = i64, remote = "sim::Increment")]
    struct Increment {
        amount: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = i64, remote = "sim::Get")]
    struct Get;

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = i64, remote = "sim::GetTicks")]
    struct GetTicks;

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = (), remote = "sim::Tick")]
    struct Tick;

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    #[message(result = (), remote = "sim::StartTicking")]
    struct StartTicking {
        every_ms: u64,
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

        #[handler]
        fn get(&self, _ctx: &ActorContext<Self>, state: &mut CounterState, _msg: Get) -> i64 {
            state.count
        }

        #[handler]
        fn get_ticks(
            &self,
            _ctx: &ActorContext<Self>,
            state: &mut CounterState,
            _msg: GetTicks,
        ) -> i64 {
            state.ticks as i64
        }

        #[handler]
        fn tick(&self, _ctx: &ActorContext<Self>, state: &mut CounterState, _msg: Tick) {
            state.ticks += 1;
        }

        #[handler]
        fn start_ticking(
            &self,
            ctx: &ActorContext<Self>,
            state: &mut CounterState,
            msg: StartTicking,
        ) {
            state.timer = Some(ctx.schedule_repeat(Duration::from_millis(msg.every_ms), Tick));
        }
    }

    #[test]
    fn send_and_reply_under_sim() {
        let mut world = SimWorld::new(1);
        let ep = world
            .system()
            .start("counter/0", Counter, CounterState::default());
        assert_eq!(world.send(&ep, Increment { amount: 5 }).unwrap(), 5);
        assert_eq!(world.send(&ep, Increment { amount: 3 }).unwrap(), 8);
        assert_eq!(world.send(&ep, Get).unwrap(), 8);
    }

    #[test]
    fn virtual_time_does_not_really_sleep() {
        let mut world = SimWorld::new(1);
        let ep = world
            .system()
            .start("counter/0", Counter, CounterState::default());

        let started = std::time::Instant::now();
        world.advance(Duration::from_secs(3600)); // an hour of virtual time
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "advancing virtual time must not sleep in real time"
        );
        assert_eq!(world.now(), Duration::from_secs(3600));
        let _ = ep;
    }

    #[test]
    fn scheduled_repeat_fires_deterministically_on_advance() {
        let mut world = SimWorld::new(7);
        let ep = world
            .system()
            .start("counter/0", Counter, CounterState::default());

        // Arm a 100ms repeating tick.
        world.send(&ep, StartTicking { every_ms: 100 }).unwrap();
        assert_eq!(world.send(&ep, GetTicks).unwrap(), 0);

        // Advance 350ms of virtual time → exactly 3 ticks (at 100/200/300ms).
        world.advance(Duration::from_millis(350));
        assert_eq!(world.send(&ep, GetTicks).unwrap(), 3);

        // Another 100ms → one more tick.
        world.advance(Duration::from_millis(100));
        assert_eq!(world.send(&ep, GetTicks).unwrap(), 4);
    }

    #[test]
    fn block_on_panics_on_deadlock() {
        // A future awaiting a reply no actor will ever send: all tasks park,
        // no timer is pending, so block_on must detect the deadlock and panic
        // rather than spin forever. (Relies on flush_interval defaulting to
        // None, so no perpetual timer keeps a deadline alive.)
        let mut world = SimWorld::new(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            world.block_on(async {
                let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
                let _ = rx.await;
            });
        }));
        assert!(result.is_err(), "block_on should panic on a sim deadlock");
    }

    #[test]
    fn replay_is_stable_across_runs() {
        // Replay covers what SimWorld controls: task scheduling, virtual-time
        // timer firing, and the seeded RNG stream. (Iteration-order determinism
        // is covered separately by `listing_backfill_order_is_deterministic`.)
        // Same seed → identical observable outcome across independent runs.
        fn run(seed: u64) -> (i64, i64) {
            let mut world = SimWorld::new(seed);
            let ep = world
                .system()
                .start("counter/0", Counter, CounterState::default());
            world.send(&ep, StartTicking { every_ms: 50 }).unwrap();
            // Interleave seeded "random" increments with time advances.
            let mut total = 0;
            for _ in 0..10 {
                let amount = (world.rng_u64() % 7) as i64;
                total += world.send(&ep, Increment { amount }).unwrap();
                world.advance(Duration::from_millis(50));
            }
            let ticks = world.send(&ep, GetTicks).unwrap();
            (total, ticks)
        }

        assert_eq!(run(99), run(99), "same seed must replay identically");
    }

    #[test]
    fn listing_backfill_order_is_deterministic() {
        // The receptionist registry is a BTreeMap, so listing() backfill emits
        // matching actors in sorted-label order. Before that fix the order came
        // from HashMap memory layout (random per process) and a deterministic
        // runtime could not pin it. This is the multi-actor iteration-order case
        // the single-actor replay test cannot exercise.
        fn collect_counts(seed: u64) -> Vec<i64> {
            let mut world = SimWorld::new(seed);
            let key = ReceptionKey::<Counter>::new("workers");

            // Insert in scrambled label order; the count encodes sorted position
            // (alpha=1, bravo=2, charlie=3), so a correct sorted backfill yields
            // [1, 2, 3] regardless of insertion or hash order.
            let mut kept = Vec::new();
            for (label, count) in [("w/charlie", 3), ("w/alpha", 1), ("w/bravo", 2)] {
                let ep = world.system().start(
                    label,
                    Counter,
                    CounterState {
                        count,
                        ..Default::default()
                    },
                );
                world.system().check_in(label, key.clone());
                kept.push(ep); // keep endpoints alive for the duration
            }

            // Backfill is enqueued synchronously when the listing is created.
            let mut listing = world.system().listing(key);
            let mut eps = Vec::new();
            while let Some(ep) = listing.try_next() {
                eps.push(ep);
            }

            let counts = eps
                .iter()
                .map(|ep| {
                    let ep = ep.clone();
                    world.block_on(async move { ep.send(Get).await.unwrap() })
                })
                .collect();
            let _ = kept;
            counts
        }

        let a = collect_counts(1);
        let b = collect_counts(2);
        assert_eq!(
            a,
            vec![1, 2, 3],
            "backfill must follow sorted label order (alpha, bravo, charlie)"
        );
        assert_eq!(
            a, b,
            "backfill order is identical across seeds — it is deterministic, not hash-random"
        );
    }

    #[test]
    fn pool_router_fills_deterministically_under_sim() {
        let mut world = SimWorld::new(5);
        let key = ReceptionKey::<Counter>::new("pool");
        let mut kept = Vec::new();
        for label in ["p/charlie", "p/alpha", "p/bravo"] {
            let ep = world
                .system()
                .start(label, Counter, CounterState::default());
            world.system().check_in(label, key.clone());
            kept.push(ep);
        }

        // PoolRouter spawns its membership watcher on the receptionist's runtime.
        // Before routing was on the seam this called tokio::spawn directly and
        // would panic under sim (no tokio runtime). Now it runs on the sim
        // runtime; pump so the watcher drains the backfill into the pool.
        let router = PoolRouter::new(
            world.system().receptionist(),
            key,
            RoutingStrategy::RoundRobin,
        );
        world.pump();

        assert_eq!(router.len(), 3, "watcher ran under sim and filled the pool");
        assert_eq!(
            router.labels(),
            vec!["p/alpha", "p/bravo", "p/charlie"],
            "pool order is deterministic (sorted by label, not hash-random)"
        );
        let _ = kept;
    }

    #[test]
    fn same_seed_same_rng_sequence() {
        let a = SimWorld::new(42);
        let b = SimWorld::new(42);
        let seq_a: Vec<u64> = (0..16).map(|_| a.rng_u64()).collect();
        let seq_b: Vec<u64> = (0..16).map(|_| b.rng_u64()).collect();
        assert_eq!(seq_a, seq_b, "same seed must reproduce the same RNG stream");

        let c = SimWorld::new(43);
        let seq_c: Vec<u64> = (0..16).map(|_| c.rng_u64()).collect();
        assert_ne!(seq_a, seq_c, "different seed should diverge");
    }

    #[test]
    fn derive_seed_is_reproducible_and_label_independent() {
        // Same root seed + same label → same derived seed, across worlds.
        let a = SimWorld::new(100);
        let b = SimWorld::new(100);
        assert_eq!(a.derive_seed("vfs"), b.derive_seed("vfs"));
        assert_eq!(a.derive_seed("faults"), b.derive_seed("faults"));

        // Distinct labels give distinct sub-streams (so consumers don't collide).
        assert_ne!(a.derive_seed("vfs"), a.derive_seed("faults"));

        // Different root seed → different derived seed for the same label.
        let c = SimWorld::new(101);
        assert_ne!(a.derive_seed("vfs"), c.derive_seed("vfs"));

        // derive_seed must not perturb the main rng stream (independent source).
        let d = SimWorld::new(100);
        let _ = d.derive_seed("anything");
        assert_eq!(
            d.rng_u64(),
            SimWorld::new(100).rng_u64(),
            "derive_seed must not consume from the primary rng stream"
        );
    }
}
