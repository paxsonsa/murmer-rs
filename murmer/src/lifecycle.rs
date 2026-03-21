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
#[derive(Debug, Clone)]
pub struct ActorTerminated {
    pub label: String,
    pub reason: TerminationReason,
}

/// Restart strategy for an actor — mirrors Erlang/OTP child spec strategies.
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
/// `&mut self` allows stateful factories (e.g. incrementing restart counters).
pub trait ActorFactory: Send + 'static {
    type Actor: Actor + RemoteDispatch;
    fn create(&mut self) -> (Self::Actor, <Self::Actor as Actor>::State);
}

/// Internal: signals delivered to actors from the system.
pub(crate) enum SystemSignal {
    ActorTerminated(ActorTerminated),
}

/// Internal: a watch entry stored in the receptionist.
pub(crate) struct WatchEntry {
    #[allow(dead_code)]
    pub(crate) watcher_label: String,
    pub(crate) system_tx: mpsc::UnboundedSender<SystemSignal>,
}
