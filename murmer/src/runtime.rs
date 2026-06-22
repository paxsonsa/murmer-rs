//! Runtime seam — the single swappable abstraction murmer's concurrency routes
//! through.
//!
//! Every task spawn, timer, clock read, and randomness draw inside murmer's
//! local-actor path goes through a [`Runtime`]. The default, [`TokioRuntime`],
//! is an `#[inline]` passthrough to Tokio, so production behavior is unchanged.
//! A deterministic implementation (`SimRuntime`, under the `sim` feature) swaps
//! the same seam for a single-threaded, seeded scheduler with virtual time —
//! the basis for simulation testing.
//!
//! # Why a seam
//!
//! FoundationDB can simulate an entire database deterministically because all
//! of its concurrency funnels through one runtime abstraction (`INetwork`),
//! swappable between a real impl and a simulated one. `Runtime` is murmer's
//! analogue: actor code never touches `tokio::spawn` / `tokio::time` directly,
//! so a test can substitute a deterministic world without changing a line of
//! actor logic.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// A boxed, `Send` future with a `'static` lifetime — the unit of work the
/// runtime spawns and the return type of its timer futures.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The swappable runtime seam.
///
/// Implementors provide task spawning, timers, a monotonic clock, randomness,
/// and a blocking-execution context. Held by the `System`/`Receptionist` as an
/// `Arc<dyn Runtime>` and threaded into every internal background task.
pub trait Runtime: Send + Sync + 'static {
    /// Spawn a fire-and-forget task. Replaces every internal `tokio::spawn`.
    ///
    /// The returned [`SpawnHandle`] can request cancellation; cancellation
    /// semantics are implementation-specific (see [`SpawnHandle`]).
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> SpawnHandle;

    /// A future that completes after `dur` of (virtual, in sim) time.
    /// Replaces `tokio::time::sleep` / `interval` / `sleep_until`.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;

    /// Monotonic time since this runtime started. Replaces `Instant::now()`
    /// for relative measurements. In sim this is virtual time.
    fn now(&self) -> Duration;

    /// Draw a `u64` from the runtime's randomness source. Seeded and
    /// reproducible in sim; OS entropy in production.
    fn rng_u64(&self) -> u64;

    /// Derive an independent, reproducible seed from this runtime's root seed
    /// and `label`.
    ///
    /// This is the "one seed, one root" primitive: a consumer (e.g. an external
    /// subsystem running under the same simulation) can seed its *own* RNG or
    /// fault stream off the same root, so the whole stack descends from one seed
    /// without two consumers perturbing each other's draw order. Deterministic
    /// under a seeded sim runtime (a pure function of root-seed + label);
    /// arbitrary per call under the Tokio default, where reproducibility is not
    /// promised. Returning a `u64` keeps murmer free of a public `rand`
    /// dependency — the caller seeds whatever PRNG it likes.
    fn derive_seed(&self, label: &str) -> u64;

    /// Run a synchronous, potentially blocking unit of work.
    ///
    /// Production offloads to a dedicated blocking thread so real I/O never
    /// stalls the actor pool. Sim runs it inline on the deterministic thread as
    /// one atomic step. This is deliberately **not** `tokio::spawn_blocking`:
    /// a sim scheduler cannot observe an uncontrolled thread pool.
    fn run_blocking(&self, work: Box<dyn FnOnce() + Send + 'static>) -> BoxFuture<'static, ()>;
}

/// A handle to a spawned task. Dropping it does not cancel the task; call
/// [`abort`](SpawnHandle::abort) to request cancellation.
///
/// On [`TokioRuntime`] this aborts the underlying Tokio task. On the sim
/// runtime, top-level task abort is a no-op — sim cancellation is cooperative,
/// driven by cancel flags inside the scheduled futures themselves (see
/// `ScheduleHandle`).
pub struct SpawnHandle {
    abort: Box<dyn Fn() + Send + Sync>,
}

impl SpawnHandle {
    /// Build a handle from an arbitrary abort action.
    pub fn from_abort(abort: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            abort: Box::new(abort),
        }
    }

    /// A handle whose `abort` does nothing.
    pub fn noop() -> Self {
        Self::from_abort(|| {})
    }

    /// Request cancellation of the spawned task.
    pub fn abort(&self) {
        (self.abort)()
    }
}

/// The default runtime: thin passthrough to Tokio. Production behavior is
/// identical to calling `tokio::*` directly.
#[derive(Default, Clone, Copy)]
pub struct TokioRuntime;

fn tokio_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

impl Runtime for TokioRuntime {
    #[inline]
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> SpawnHandle {
        let handle = tokio::spawn(fut);
        let abort = handle.abort_handle();
        SpawnHandle::from_abort(move || abort.abort())
    }

    #[inline]
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(dur))
    }

    #[inline]
    fn now(&self) -> Duration {
        tokio_base().elapsed()
    }

    #[inline]
    fn rng_u64(&self) -> u64 {
        rand::random()
    }

    #[inline]
    fn derive_seed(&self, _label: &str) -> u64 {
        // Production is not reproducible by design; hand back fresh entropy.
        rand::random()
    }

    #[inline]
    fn run_blocking(&self, work: Box<dyn FnOnce() + Send + 'static>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let _ = tokio::task::spawn_blocking(work).await;
        })
    }
}

/// A runtime-agnostic "yield once" future.
///
/// Returns `Pending` exactly once (re-waking itself), then `Ready`. Unlike
/// `tokio::task::yield_now`, it touches no runtime reactor, so it is safe to
/// poll under the sim executor as well as under Tokio.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by [`yield_now`].
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
