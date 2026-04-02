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
    /// Optional tag set by [`ActorContext::watch_with_tag`] to identify the
    /// *role* of the terminated actor without parsing the label string.
    ///
    /// `None` when set via plain [`ActorContext::watch`].
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
