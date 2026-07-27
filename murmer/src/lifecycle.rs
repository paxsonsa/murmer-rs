//! Lifecycle and restart types for actor supervision.
//!
//! This module defines the types that control how actors are supervised:
//!
//! - [`TerminationReason`] — why an actor stopped (clean shutdown, panic, restart limit)
//! - [`RestartPolicy`] — whether and when to restart (Temporary, Transient, Permanent)
//! - [`RestartConfig`] — full restart specification with limits, rolling window, and backoff
//! - [`ActorFactory`] — creates new actor instances on restart
//!
//! # Restart policies (OTP-inspired)
//!
//! | Policy | Restart on panic? | Restart on clean stop? |
//! |--------|-------------------|------------------------|
//! | `Temporary` | No | No |
//! | `Transient` | Yes | No |
//! | `Permanent` | Yes | Yes |
//!
//! # Backoff
//!
//! When an actor crashes repeatedly, [`BackoffConfig`] applies exponential
//! backoff between restarts to avoid tight crash loops. The delay starts at
//! `initial` and doubles (by `multiplier`) up to `max`.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::actor::{Actor, RemoteDispatch};

// =============================================================================
// LIFECYCLE & RESTART TYPES
// =============================================================================

/// Why an actor terminated — used by watches and restart policies.
///
/// # Examples
///
/// ```rust,ignore
/// impl Actor for Supervisor {
///     type State = SupervisorState;
///
///     fn on_actor_terminated(&mut self, state: &mut SupervisorState, event: &ActorTerminated) {
///         match &event.reason {
///             TerminationReason::Stopped => println!("{} stopped cleanly", event.label),
///             TerminationReason::Panicked(msg) => println!("{} panicked: {msg}", event.label),
///             TerminationReason::RestartLimitExceeded => println!("{} gave up", event.label),
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub enum TerminationReason {
    /// Clean shutdown (receptionist.stop() or channel close)
    Stopped,
    /// catch_unwind caught a panic
    Panicked(String),
    /// Restart limit exceeded within the configured time window
    RestartLimitExceeded,
}

/// Notification delivered to watchers when a watched actor terminates.
///
/// # Examples
///
/// ```rust,ignore
/// fn on_actor_terminated(&mut self, state: &mut MyState, event: &ActorTerminated) {
///     match event.tag.as_deref() {
///         Some("writer") => state.writer_down = true,
///         Some("reader") => state.reader_down = true,
///         _ => {}
///     }
///     tracing::warn!("Watched actor {} terminated: {:?}", event.label, event.reason);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ActorTerminated {
    pub label: String,
    pub reason: TerminationReason,
    /// Optional tag set by [`ActorContext::watch_with_tag`](crate::actor::ActorContext::watch_with_tag)
    /// to identify the *role* of the terminated actor without parsing the label string.
    ///
    /// `None` when set via plain [`ActorContext::watch`](crate::actor::ActorContext::watch).
    pub tag: Option<String>,
}

/// Restart strategy for an actor — mirrors Erlang/OTP child spec strategies.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::{RestartConfig, RestartPolicy};
///
/// // Restart on crash only (most common for workers)
/// let config = RestartConfig { policy: RestartPolicy::Transient, ..Default::default() };
///
/// // Always restart (for essential services)
/// let config = RestartConfig { policy: RestartPolicy::Permanent, ..Default::default() };
///
/// // Never restart (for one-shot tasks)
/// let config = RestartConfig { policy: RestartPolicy::Temporary, ..Default::default() };
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub enum RestartPolicy {
    /// Never restart (current behavior)
    #[default]
    Temporary,
    /// Restart only on crash (panic), not clean stop
    Transient,
    /// Always restart regardless of reason
    Permanent,
}

/// Configuration for restart limits and backoff behavior.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::{RestartConfig, RestartPolicy, BackoffConfig};
/// use std::time::Duration;
///
/// let config = RestartConfig {
///     policy: RestartPolicy::Transient,  // restart on panic only
///     max_restarts: 3,                   // max 3 restarts...
///     window: Duration::from_secs(30),   // ...within 30 seconds
///     backoff: BackoffConfig {
///         initial: Duration::from_millis(200),
///         max: Duration::from_secs(10),
///         multiplier: 2.0,
///     },
/// };
///
/// let endpoint = system.start_with_config("worker/0", MyFactory, config);
/// ```
#[derive(Debug, Clone)]
pub struct RestartConfig {
    pub policy: RestartPolicy,
    /// Maximum number of restarts allowed within the rolling `window`.
    pub max_restarts: u32,
    /// Rolling time window for counting restarts.
    pub window: Duration,
    pub backoff: BackoffConfig,
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            policy: RestartPolicy::Temporary,
            max_restarts: 5,
            window: Duration::from_secs(60),
            backoff: BackoffConfig::default(),
        }
    }
}

/// Exponential backoff configuration for actor restarts.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::BackoffConfig;
/// use std::time::Duration;
///
/// let backoff = BackoffConfig {
///     initial: Duration::from_millis(500),   // first retry after 500ms
///     max: Duration::from_secs(30),          // cap at 30s
///     multiplier: 2.0,                       // 500ms → 1s → 2s → 4s → ...
/// };
/// ```
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before the first restart.
    pub initial: Duration,
    /// Maximum delay between restarts.
    pub max: Duration,
    /// Multiplier applied after each restart.
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

/// Factory for creating actor instances on restart.
///
/// `&mut self` allows stateful factories (e.g. incrementing restart counters,
/// loading config from disk).
///
/// # Examples
///
/// ```rust,ignore
/// struct CounterFactory { initial_count: i64 }
///
/// impl ActorFactory for CounterFactory {
///     type Actor = Counter;
///     fn create(&mut self) -> (Counter, CounterState) {
///         (Counter, CounterState { count: self.initial_count })
///     }
/// }
///
/// let ep = system.start_with_policy(
///     "counter/0",
///     CounterFactory { initial_count: 0 },
///     RestartPolicy::Permanent,
/// );
/// ```
pub trait ActorFactory: Send + 'static {
    type Actor: Actor + RemoteDispatch;
    fn create(&mut self) -> (Self::Actor, <Self::Actor as Actor>::State);
}

// =============================================================================
// TERMINATE HOOK (fault-injection seam)
// =============================================================================

/// Fault-injection seam fired between an actor's supervisor loop exiting and
/// the actor's removal from the receptionist.
///
/// # Why this exists
///
/// Deregistration is normally instantaneous: the supervisor loop breaks and the
/// `DeregisterGuard` fires in `Drop`, which is synchronous. Real systems are not
/// so tidy — an actor can accept its stop, then take time to drain a mailbox or
/// tear down external resources, staying *registered but not serving* for a
/// while. Callers must tolerate that window (murmer's own eviction paths wait on
/// deregistration with a deadline), but without a seam there is no way to *test*
/// that tolerance, because the window is zero-width by construction.
///
/// Install a hook and the returned future is awaited inside that window. While
/// it is pending the actor is still in the receptionist — `lookup` finds it,
/// listings include it, watches have not fired — and its supervisor is no longer
/// processing messages. That is precisely the "slow to die" state.
///
/// # Determinism
///
/// The hook returns a future, so a simulation test builds its delay from the
/// [`Runtime`](crate::runtime::Runtime) seam (`runtime.sleep(d)`) and the wait
/// runs on virtual time — reproducible from the world seed like everything else.
/// Do not sleep on wall-clock time inside a hook used under simulation.
///
/// # Scope
///
/// This is a test/fault-injection seam. It is not a place to do real work:
/// it runs on the supervisor's task, it blocks the actor's deregistration for as
/// long as it is pending, and a hook that never completes leaks a registry entry.
/// It fires on every terminating actor on the node, including murmer's internal
/// ones, so a hook that only wants one actor must filter on `label`.
///
/// It does *not* fire on the restart-limit-exceeded path
/// (`TerminationReason::RestartLimitExceeded`), where the receptionist
/// deregisters directly rather than through a supervisor exit.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::lifecycle::{TerminateHook, TerminationReason};
/// use murmer::runtime::{BoxFuture, Runtime};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// /// Makes one labelled actor take `delay` of virtual time to de-register.
/// struct SlowToDie {
///     label: String,
///     delay: Duration,
///     runtime: Arc<dyn Runtime>,
/// }
///
/// impl TerminateHook for SlowToDie {
///     fn before_deregister(
///         &self,
///         label: &str,
///         _reason: &TerminationReason,
///     ) -> BoxFuture<'static, ()> {
///         if label == self.label {
///             self.runtime.sleep(self.delay)
///         } else {
///             Box::pin(std::future::ready(()))
///         }
///     }
/// }
///
/// world.system().receptionist().set_terminate_hook(Some(Arc::new(SlowToDie {
///     label: "cache/user".into(),
///     delay: Duration::from_secs(5),
///     runtime: Arc::new(world.runtime().clone()),
/// })));
/// ```
pub trait TerminateHook: Send + Sync + 'static {
    /// Called with the terminating actor's label and exit reason. The actor is
    /// still registered until the returned future completes.
    fn before_deregister(
        &self,
        label: &str,
        reason: &TerminationReason,
    ) -> crate::runtime::BoxFuture<'static, ()>;
}

/// Internal: signals delivered to actors from the system.
pub(crate) enum SystemSignal {
    ActorTerminated(ActorTerminated),
    /// Self-stop request from the actor (via ctx.stop())
    Stop,
}

/// Internal: a watch entry stored in the receptionist.
pub(crate) struct WatchEntry {
    #[allow(dead_code)]
    pub(crate) watcher_label: String,
    pub(crate) system_tx: mpsc::UnboundedSender<SystemSignal>,
    /// Tag supplied by `watch_with_tag`; `None` for plain `watch`.
    pub(crate) tag: Option<String>,
}
