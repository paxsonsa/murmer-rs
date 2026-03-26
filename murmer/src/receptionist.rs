//! Receptionist — type-erased actor registry with typed lookup.
//!
//! The [`Receptionist`] is the central coordination point for all actors in a
//! murmer system. It provides:
//!
//! - **Start actors**: `receptionist.start("label", actor, state)` spawns a supervised actor
//! - **Lookup actors**: `receptionist.lookup::<A>("label")` returns a typed `Endpoint<A>`
//! - **Lifecycle events**: subscribe to registration/deregistration notifications
//! - **Group discovery**: check actors into [`ReceptionKey`] groups
//!   and subscribe via [`Listing`] streams
//! - **Supervision**: `start_with_config` enables restart policies with backoff
//! - **Distributed replication**: OpLog-based sync with version vectors
//! - **Node pruning**: remove all actors from a departed cluster node
//!
//! # Type erasure with safe downcasts
//!
//! Internally, the receptionist stores entries as type-erased `ActorEntry` values
//! (no actor type parameter). At lookup time, the caller provides the expected type
//! via `lookup::<A>()`, and a `TypeId` guard ensures the internal downcast is safe.
//! This design means the receptionist never needs to be generic over actor types.

use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::actor::{Actor, ActorContext, RemoteDispatch};
use crate::endpoint::Endpoint;
use crate::instrument;
use crate::lifecycle::{
    ActorFactory, ActorTerminated, RestartConfig, RestartPolicy, SystemSignal, TerminationReason,
    WatchEntry,
};
use crate::listing::{
    ErasedListingSender, ErasedReceptionKey, ErasedWatchedListingSender, Listing, ReceptionKey,
    TypedListingSender, TypedWatchedListingSender, WatchedListing,
};
use crate::oplog::{Op, OpLog, OpType, VersionVector};
use crate::ready::ReadyHandle;
use crate::supervisor::{run_supervisor, should_restart};
use crate::wire::{DispatchRequest, EnvelopeProxy, RemoteInvocation, ResponseRegistry};

// =============================================================================
// LIFECYCLE EVENTS
// =============================================================================

/// Lifecycle events. Fully type-erased — subscribers don't need to know the
/// actor type. If they want an endpoint, they call `lookup::<A>()` with the
/// type they expect.
///
/// # Examples
///
/// ```rust,ignore
/// let mut events = receptionist.subscribe();
/// tokio::spawn(async move {
///     while let Some(event) = events.recv().await {
///         match event {
///             ActorEvent::Registered { label, actor_type } => {
///                 println!("Actor {label} ({actor_type}) registered");
///             }
///             ActorEvent::Deregistered { label, actor_type } => {
///                 println!("Actor {label} ({actor_type}) deregistered");
///             }
///         }
///     }
/// });
/// ```
#[derive(Debug, Clone)]
pub enum ActorEvent {
    Registered { label: String, actor_type: String },
    Deregistered { label: String, actor_type: String },
}

// =============================================================================
// ENDPOINT FACTORY — type-erased endpoint creation
// =============================================================================

/// Type-erased endpoint factory stored in the receptionist.
/// Created at registration time when the actor type is known.
/// Used at lookup time to produce a typed Endpoint<A>.
pub(crate) trait ErasedEndpointFactory: Send + Sync {
    fn create_endpoint_any(&self) -> Box<dyn Any + Send>;
}

pub(crate) struct LocalEndpointFactory<A: Actor> {
    mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
}

impl<A: Actor + 'static> ErasedEndpointFactory for LocalEndpointFactory<A> {
    fn create_endpoint_any(&self) -> Box<dyn Any + Send> {
        Box::new(Endpoint::<A>::local(self.mailbox_tx.clone()))
    }
}

// =============================================================================
// ACTOR ENTRY
// =============================================================================

/// What the receptionist stores per actor. No actor type parameter.
pub(crate) struct ActorEntry {
    pub(crate) label: String,
    pub(crate) type_id: TypeId,
    pub(crate) actor_type_name: String,
    pub(crate) location: EntryLocation,
    pub(crate) keys: Vec<ErasedReceptionKey>,
    pub(crate) node_id: String,
}

pub(crate) enum EntryLocation {
    Local {
        /// Produces typed Endpoint<A> at lookup time (via internal downcast)
        endpoint_factory: Box<dyn ErasedEndpointFactory>,
        /// Non-generic channel for routing incoming remote messages
        dispatch_tx: mpsc::UnboundedSender<DispatchRequest>,
        /// Shutdown signal for the supervisor
        shutdown_tx: Option<oneshot::Sender<()>>,
    },
    Remote {
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    },
}

// =============================================================================
// RECEPTIONIST CONFIG
// =============================================================================

/// Receptionist configuration for clustering features.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::receptionist::ReceptionistConfig;
/// use std::time::Duration;
///
/// let config = ReceptionistConfig {
///     node_id: "node-1".to_string(),
///     origin_addr: "192.168.1.10:9001".to_string(),
///     flush_interval: Some(Duration::from_millis(50)),
///     blip_window: Some(Duration::from_millis(200)),
/// };
/// let receptionist = Receptionist::with_config(config);
/// ```
#[derive(Debug, Clone)]
pub struct ReceptionistConfig {
    pub node_id: String,
    /// "host:port" of this node — embedded in Register ops so remote nodes
    /// can route back to the actor's home node.
    pub origin_addr: String,
    /// If set, listing notifications are batched and flushed on this interval.
    pub flush_interval: Option<Duration>,
    /// If set, oplog entries are delayed by this window. If the actor deregisters
    /// before the window expires, the op is never committed (blip avoidance).
    pub blip_window: Option<Duration>,
}

impl Default for ReceptionistConfig {
    fn default() -> Self {
        Self {
            node_id: "local".to_string(),
            origin_addr: "127.0.0.1:0".to_string(),
            flush_interval: None,
            blip_window: None,
        }
    }
}

// =============================================================================
// DEREGISTER GUARD
// =============================================================================

/// Deregister guard — dropped when the supervisor exits, triggering auto-deregister.
/// Carries the termination reason so watches receive accurate information.
pub(crate) struct DeregisterGuard {
    pub(crate) receptionist: Receptionist,
    pub(crate) label: String,
    pub(crate) reason: Arc<Mutex<TerminationReason>>,
}

impl Drop for DeregisterGuard {
    fn drop(&mut self) {
        let reason = self.reason.lock().unwrap().clone();
        self.receptionist
            .deregister_with_reason(&self.label, reason);
    }
}

/// Pending listing notification for delayed flush.
struct PendingListingNotification {
    label: String,
    key_id: String,
    type_id: TypeId,
}

struct ReceptionistInner {
    config: ReceptionistConfig,
    entries: Mutex<HashMap<String, ActorEntry>>,
    event_subscribers: Mutex<Vec<mpsc::UnboundedSender<ActorEvent>>>,
    listing_subscribers: Mutex<Vec<Box<dyn ErasedListingSender>>>,
    watched_listing_subscribers: Mutex<Vec<Box<dyn ErasedWatchedListingSender>>>,
    oplog: Mutex<OpLog>,
    observed_versions: Mutex<VersionVector>,
    watches: Mutex<HashMap<String, Vec<WatchEntry>>>,
    pending_notifications: Mutex<Vec<PendingListingNotification>>,
    blip_pending: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

// =============================================================================
// RECEPTIONIST
// =============================================================================

/// The receptionist: type-erased actor registry with typed lookup,
/// subscription-based discovery, and distributed replication support.
///
/// Cheap to clone (wraps Arc).
///
/// # Examples
///
/// ```rust,ignore
/// let receptionist = Receptionist::new();
///
/// // Start an actor
/// let ep = receptionist.start("counter/0", Counter, CounterState { count: 0 });
///
/// // Look it up later
/// let ep: Option<Endpoint<Counter>> = receptionist.lookup("counter/0");
///
/// // Subscribe to lifecycle events
/// let mut events = receptionist.subscribe();
///
/// // Group discovery with reception keys
/// let key = ReceptionKey::<Counter>::new("counters");
/// receptionist.check_in("counter/0", key.clone());
/// let mut listing = receptionist.listing(key);
/// ```
#[derive(Clone)]
pub struct Receptionist {
    inner: Arc<ReceptionistInner>,
}

impl Receptionist {
    pub fn new() -> Self {
        Self::with_config(ReceptionistConfig::default())
    }

    pub fn with_node_id(node_id: impl Into<String>) -> Self {
        Self::with_config(ReceptionistConfig {
            node_id: node_id.into(),
            ..Default::default()
        })
    }

    pub fn with_config(config: ReceptionistConfig) -> Self {
        let node_id = config.node_id.clone();
        let flush_interval = config.flush_interval;

        let receptionist = Self {
            inner: Arc::new(ReceptionistInner {
                config,
                entries: Mutex::new(HashMap::new()),
                event_subscribers: Mutex::new(Vec::new()),
                listing_subscribers: Mutex::new(Vec::new()),
                watched_listing_subscribers: Mutex::new(Vec::new()),
                oplog: Mutex::new(OpLog::new(node_id)),
                observed_versions: Mutex::new(VersionVector::new()),
                watches: Mutex::new(HashMap::new()),
                pending_notifications: Mutex::new(Vec::new()),
                blip_pending: Mutex::new(HashMap::new()),
            }),
        };

        // Spawn the flush task if configured
        if let Some(interval) = flush_interval {
            let r = receptionist.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    r.flush_pending_notifications();
                }
            });
        }

        receptionist
    }

    pub fn node_id(&self) -> &str {
        &self.inner.config.node_id
    }

    // =========================================================================
    // CORE REGISTRATION & LOOKUP
    // =========================================================================

    /// Start a local actor, register it, and return its typed endpoint.
    /// Uses `Temporary` restart policy (never restarts).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let ep = receptionist.start("counter/0", Counter, CounterState { count: 0 });
    /// let count = ep.send(Increment { amount: 1 }).await?;
    /// ```
    pub fn start<A>(&self, label: &str, actor: A, state: A::State) -> Endpoint<A>
    where
        A: Actor + RemoteDispatch + 'static,
    {
        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<A>>>();
        let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        // Create deregister guard for auto-cleanup on supervisor exit
        let guard = DeregisterGuard {
            receptionist: self.clone(),
            label: label.to_string(),
            reason: exit_reason.clone(),
        };

        // Build the actor context
        let ctx = ActorContext {
            label: label.to_string(),
            node_id: self.inner.config.node_id.clone(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: self.clone(),
            system_tx,
            reply_token: std::sync::Mutex::new(None),
        };

        // Spawn the supervisor task
        tokio::spawn(async move {
            let _ = run_supervisor(
                actor,
                state,
                ctx,
                mailbox_rx,
                dispatch_rx,
                shutdown_rx,
                system_rx,
                exit_reason,
                guard,
            )
            .await;
        });

        // Create the user's endpoint
        let endpoint = Endpoint::local(mailbox_tx.clone());

        // Register in the receptionist
        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>().to_string(),
            location: EntryLocation::Local {
                endpoint_factory: Box::new(LocalEndpointFactory { mailbox_tx }),
                dispatch_tx,
                shutdown_tx: Some(shutdown_tx),
            },
            keys: Vec::new(),
            node_id: self.inner.config.node_id.clone(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        // Record in oplog (respects blip window if configured)
        self.record_register_op(label, std::any::type_name::<A>(), &[]);

        instrument::actor_started(std::any::type_name::<A>());
        instrument::receptionist_registration();
        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<A>().to_string(),
        });

        endpoint
    }

    /// Prepare a local actor for deferred start.
    ///
    /// Like [`start()`](Self::start), this creates channels, registers the actor
    /// in the receptionist, and returns a typed `Endpoint<A>`. However, instead
    /// of spawning the supervisor immediately, it returns a [`ReadyHandle`] that
    /// the caller can use to spawn the supervisor later.
    ///
    /// Messages sent to the endpoint queue in the unbounded mailbox until
    /// [`ReadyHandle::start()`] is called. If the `ReadyHandle` is dropped
    /// without calling `start()`, the actor is automatically deregistered.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let (ep, ready) = receptionist.prepare("cache/0", Cache, CacheState::new());
    /// ep.send(Warmup { key: "users".into() }).await.ok(); // queues
    /// ready.start(); // begins processing
    /// ```
    pub fn prepare<A>(
        &self,
        label: &str,
        actor: A,
        state: A::State,
    ) -> (Endpoint<A>, ReadyHandle<A>)
    where
        A: Actor + RemoteDispatch + 'static,
    {
        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<A>>>();
        let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        // Create deregister guard — owned by ReadyHandle initially.
        // If ReadyHandle is dropped without start(), this fires and deregisters.
        // If start() is called, ownership transfers to the supervisor task.
        let guard = DeregisterGuard {
            receptionist: self.clone(),
            label: label.to_string(),
            reason: exit_reason.clone(),
        };

        // Build the actor context
        let ctx = ActorContext {
            label: label.to_string(),
            node_id: self.inner.config.node_id.clone(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: self.clone(),
            system_tx,
            reply_token: std::sync::Mutex::new(None),
        };

        // Create the user's endpoint
        let endpoint = Endpoint::local(mailbox_tx.clone());

        // Register in the receptionist (same as start())
        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>().to_string(),
            location: EntryLocation::Local {
                endpoint_factory: Box::new(LocalEndpointFactory { mailbox_tx }),
                dispatch_tx,
                shutdown_tx: Some(shutdown_tx),
            },
            keys: Vec::new(),
            node_id: self.inner.config.node_id.clone(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        // Record in oplog (respects blip window if configured)
        self.record_register_op(label, std::any::type_name::<A>(), &[]);

        instrument::actor_started(std::any::type_name::<A>());
        instrument::receptionist_registration();
        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<A>().to_string(),
        });

        let ready = ReadyHandle::new(
            actor,
            state,
            ctx,
            mailbox_rx,
            dispatch_rx,
            shutdown_rx,
            system_rx,
            exit_reason,
            guard,
        );

        (endpoint, ready)
    }

    /// Start an actor with a restart policy and factory.
    ///
    /// The factory is called to create each incarnation of the actor (initial + restarts).
    /// The returned endpoint remains valid across restarts — the underlying mailbox
    /// channel is reused so existing `Endpoint<A>` handles keep working.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let ep = receptionist.start_with_policy("worker/0", WorkerFactory, RestartPolicy::Transient);
    /// ```
    pub fn start_with_policy<F: ActorFactory>(
        &self,
        label: &str,
        factory: F,
        policy: RestartPolicy,
    ) -> Endpoint<F::Actor> {
        self.start_with_config(
            label,
            factory,
            RestartConfig {
                policy,
                ..Default::default()
            },
        )
    }

    /// Start an actor with full restart configuration (limits, backoff, policy).
    ///
    /// Like `start_with_policy`, the returned endpoint remains valid across restarts.
    /// When the restart limit is exceeded within the configured window, the actor
    /// terminates with `TerminationReason::RestartLimitExceeded` and watchers are notified.
    pub fn start_with_config<F: ActorFactory>(
        &self,
        label: &str,
        mut factory: F,
        config: RestartConfig,
    ) -> Endpoint<F::Actor> {
        // Stable channels that survive across restarts
        let (mailbox_tx, mut mailbox_rx) =
            mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<F::Actor>>>();
        let (dispatch_tx, mut dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();

        // Create the user's endpoint (stable across restarts)
        let endpoint = Endpoint::local(mailbox_tx.clone());

        // Initial registration
        let (actor, state) = factory.create();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        let guard = DeregisterGuard {
            receptionist: self.clone(),
            label: label.to_string(),
            reason: exit_reason.clone(),
        };

        let ctx = ActorContext {
            label: label.to_string(),
            node_id: self.inner.config.node_id.clone(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: self.clone(),
            system_tx,
            reply_token: std::sync::Mutex::new(None),
        };

        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<F::Actor>(),
            actor_type_name: std::any::type_name::<F::Actor>().to_string(),
            location: EntryLocation::Local {
                endpoint_factory: Box::new(LocalEndpointFactory {
                    mailbox_tx: mailbox_tx.clone(),
                }),
                dispatch_tx: dispatch_tx.clone(),
                shutdown_tx: Some(shutdown_tx),
            },
            keys: Vec::new(),
            node_id: self.inner.config.node_id.clone(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        self.record_register_op(label, std::any::type_name::<F::Actor>(), &[]);

        instrument::actor_started(std::any::type_name::<F::Actor>());
        instrument::receptionist_registration();
        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<F::Actor>().to_string(),
        });

        // Spawn restart wrapper
        let receptionist = self.clone();
        let label_owned = label.to_string();
        let node_id = self.inner.config.node_id.clone();

        tokio::spawn(async move {
            let mut restart_times: VecDeque<Instant> = VecDeque::new();
            let mut backoff_duration = config.backoff.initial;

            // First run uses the actor/state/channels created above
            let (mb, dp) = run_supervisor(
                actor,
                state,
                ctx,
                mailbox_rx,
                dispatch_rx,
                shutdown_rx,
                system_rx,
                exit_reason.clone(),
                guard,
            )
            .await;
            mailbox_rx = mb;
            dispatch_rx = dp;

            let reason = exit_reason.lock().unwrap().clone();
            if !should_restart(&config.policy, &reason) {
                return;
            }

            // Restart loop with limits and backoff
            loop {
                // Prune timestamps outside the rolling window
                let now = Instant::now();
                while restart_times
                    .front()
                    .is_some_and(|t| now.duration_since(*t) > config.window)
                {
                    restart_times.pop_front();
                }

                // Check if we've exceeded the restart limit
                if restart_times.len() >= config.max_restarts as usize {
                    instrument::actor_restart_limit_exceeded(std::any::type_name::<F::Actor>());
                    // The previous incarnation's guard already removed the entry.
                    // Re-register briefly so deregister_with_reason can fire watches.
                    let entry = ActorEntry {
                        label: label_owned.clone(),
                        type_id: TypeId::of::<F::Actor>(),
                        actor_type_name: std::any::type_name::<F::Actor>().to_string(),
                        location: EntryLocation::Local {
                            endpoint_factory: Box::new(LocalEndpointFactory {
                                mailbox_tx: mailbox_tx.clone(),
                            }),
                            dispatch_tx: dispatch_tx.clone(),
                            shutdown_tx: None,
                        },
                        keys: Vec::new(),
                        node_id: node_id.clone(),
                    };
                    receptionist
                        .inner
                        .entries
                        .lock()
                        .unwrap()
                        .insert(label_owned.clone(), entry);
                    receptionist.deregister_with_reason(
                        &label_owned,
                        TerminationReason::RestartLimitExceeded,
                    );
                    break;
                }

                restart_times.push_back(now);

                // Apply backoff delay
                tokio::time::sleep(backoff_duration).await;
                backoff_duration = Duration::from_secs_f64(
                    (backoff_duration.as_secs_f64() * config.backoff.multiplier)
                        .min(config.backoff.max.as_secs_f64()),
                );

                let (actor, state) = factory.create();
                let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
                let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

                let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

                let guard = DeregisterGuard {
                    receptionist: receptionist.clone(),
                    label: label_owned.clone(),
                    reason: exit_reason.clone(),
                };

                let ctx = ActorContext {
                    label: label_owned.clone(),
                    node_id: node_id.clone(),
                    mailbox_tx: mailbox_tx.clone(),
                    receptionist: receptionist.clone(),
                    system_tx,
                    reply_token: std::sync::Mutex::new(None),
                };

                // Re-register with the same label and stable channels
                let entry = ActorEntry {
                    label: label_owned.clone(),
                    type_id: TypeId::of::<F::Actor>(),
                    actor_type_name: std::any::type_name::<F::Actor>().to_string(),
                    location: EntryLocation::Local {
                        endpoint_factory: Box::new(LocalEndpointFactory {
                            mailbox_tx: mailbox_tx.clone(),
                        }),
                        dispatch_tx: dispatch_tx.clone(),
                        shutdown_tx: Some(shutdown_tx),
                    },
                    keys: Vec::new(),
                    node_id: node_id.clone(),
                };
                receptionist
                    .inner
                    .entries
                    .lock()
                    .unwrap()
                    .insert(label_owned.clone(), entry);

                instrument::actor_restarted(std::any::type_name::<F::Actor>());
                instrument::receptionist_registration();
                receptionist.emit(ActorEvent::Registered {
                    label: label_owned.clone(),
                    actor_type: std::any::type_name::<F::Actor>().to_string(),
                });

                let (mb, dp) = run_supervisor(
                    actor,
                    state,
                    ctx,
                    mailbox_rx,
                    dispatch_rx,
                    shutdown_rx,
                    system_rx,
                    exit_reason.clone(),
                    guard,
                )
                .await;
                mailbox_rx = mb;
                dispatch_rx = dp;

                let reason = exit_reason.lock().unwrap().clone();
                if !should_restart(&config.policy, &reason) {
                    break;
                }
            }
        });

        endpoint
    }

    /// Register a remote actor (discovered via cluster gossip).
    pub fn register_remote<A: Actor + 'static>(
        &self,
        label: &str,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    ) {
        self.register_remote_from_node::<A>(
            label,
            wire_tx,
            response_registry,
            &self.inner.config.node_id.clone(),
        );
    }

    /// Register a remote actor from a specific node.
    pub fn register_remote_from_node<A: Actor + 'static>(
        &self,
        label: &str,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
        node_id: &str,
    ) {
        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>().to_string(),
            location: EntryLocation::Remote {
                wire_tx,
                response_registry,
            },
            keys: Vec::new(),
            node_id: node_id.to_string(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        self.record_register_op(label, std::any::type_name::<A>(), &[]);

        instrument::actor_started(std::any::type_name::<A>());
        instrument::receptionist_registration();
        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<A>().to_string(),
        });
    }

    /// Deregister an actor. Idempotent — no-op if already gone.
    pub fn deregister(&self, label: &str) {
        self.deregister_with_reason(label, TerminationReason::Stopped);
    }

    /// Deregister an actor with a specific termination reason.
    /// Fires any watches registered for this actor.
    pub fn deregister_with_reason(&self, label: &str, reason: TerminationReason) {
        // Cancel any pending blip timer
        if let Some(handle) = self.inner.blip_pending.lock().unwrap().remove(label) {
            handle.abort();
        }

        if let Some(entry) = self.inner.entries.lock().unwrap().remove(label) {
            let reason_str = match &reason {
                TerminationReason::Stopped => "stopped",
                TerminationReason::Panicked(_) => "panicked",
                TerminationReason::RestartLimitExceeded => "restart_limit_exceeded",
            };
            instrument::actor_stopped(&entry.actor_type_name, reason_str);
            instrument::receptionist_deregistration();

            // Record remove op in oplog (immediate, no blip window for removes)
            self.inner.oplog.lock().unwrap().append(OpType::Remove {
                label: label.to_string(),
            });

            // Fire watches (one-shot: remove drains the list)
            if let Some(watchers) = self.inner.watches.lock().unwrap().remove(label) {
                let terminated = ActorTerminated {
                    label: label.to_string(),
                    reason: reason.clone(),
                };
                for watcher in watchers {
                    let _ = watcher
                        .system_tx
                        .send(SystemSignal::ActorTerminated(terminated.clone()));
                }
            }

            // Notify watched listing subscribers of removal
            if !entry.keys.is_empty() {
                let mut watched_subs = self.inner.watched_listing_subscribers.lock().unwrap();
                watched_subs.retain(|sub| !sub.is_closed());
                for sub in watched_subs.iter() {
                    // Only notify if the entry had a matching key
                    if sub.actor_type_id() == entry.type_id
                        && entry.keys.iter().any(|k| k.id == sub.key_id())
                    {
                        sub.try_send_removed(label);
                    }
                }
            }

            self.emit(ActorEvent::Deregistered {
                label: entry.label,
                actor_type: entry.actor_type_name,
            });
        }
    }

    /// Register a watch on an actor. Erlang semantics: if the watched actor
    /// doesn't exist, fires immediately.
    pub(crate) fn add_watch(
        &self,
        watched_label: &str,
        watcher_label: &str,
        system_tx: mpsc::UnboundedSender<SystemSignal>,
    ) {
        let exists = self
            .inner
            .entries
            .lock()
            .unwrap()
            .contains_key(watched_label);
        if !exists {
            // Erlang semantics: immediate fire if watched actor doesn't exist
            let _ = system_tx.send(SystemSignal::ActorTerminated(ActorTerminated {
                label: watched_label.to_string(),
                reason: TerminationReason::Stopped,
            }));
            return;
        }

        self.inner
            .watches
            .lock()
            .unwrap()
            .entry(watched_label.to_string())
            .or_default()
            .push(WatchEntry {
                watcher_label: watcher_label.to_string(),
                system_tx,
            });
    }

    /// Stop a local actor gracefully. Sends shutdown signal to supervisor.
    /// The supervisor will exit, and the DeregisterGuard will auto-deregister.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// receptionist.stop("worker/0");
    /// ```
    pub fn stop(&self, label: &str) {
        let mut entries = self.inner.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(label)
            && let EntryLocation::Local { shutdown_tx, .. } = &mut entry.location
            && let Some(tx) = shutdown_tx.take()
        {
            let _ = tx.send(());
        }
        // Don't remove entry here — the supervisor's DeregisterGuard will handle it
    }

    /// Lookup an actor by label. **The caller provides the expected type.**
    /// Returns `None` if not found or if the type doesn't match.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// if let Some(counter) = receptionist.lookup::<Counter>("counter/0") {
    ///     let count = counter.send(GetCount).await?;
    /// }
    /// ```
    pub fn lookup<A: Actor + 'static>(&self, label: &str) -> Option<Endpoint<A>> {
        instrument::receptionist_lookup();
        let entries = self.inner.entries.lock().unwrap();
        let entry = entries.get(label)?;

        // TypeId guard — ensures the downcast inside is safe
        if entry.type_id != TypeId::of::<A>() {
            return None;
        }

        match &entry.location {
            EntryLocation::Local {
                endpoint_factory, ..
            } => {
                let any_endpoint = endpoint_factory.create_endpoint_any();
                any_endpoint.downcast::<Endpoint<A>>().ok().map(|b| *b)
            }
            EntryLocation::Remote {
                wire_tx,
                response_registry,
            } => Some(Endpoint::remote(
                entry.label.clone(),
                wire_tx.clone(),
                response_registry.clone(),
            )),
        }
    }

    /// Get the dispatch channel for routing incoming remote messages
    /// to a local actor's supervisor.
    pub fn get_dispatch_sender(
        &self,
        label: &str,
    ) -> Option<mpsc::UnboundedSender<DispatchRequest>> {
        let entries = self.inner.entries.lock().unwrap();
        let entry = entries.get(label)?;
        match &entry.location {
            EntryLocation::Local { dispatch_tx, .. } => Some(dispatch_tx.clone()),
            EntryLocation::Remote { .. } => None,
        }
    }

    /// Check if an entry exists for the given label.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// assert!(receptionist.has_entry("counter/0"));
    /// assert!(!receptionist.has_entry("nonexistent"));
    /// ```
    pub fn has_entry(&self, label: &str) -> bool {
        self.inner.entries.lock().unwrap().contains_key(label)
    }

    // =========================================================================
    // LIFECYCLE EVENTS
    // =========================================================================

    /// Subscribe to lifecycle events (type-erased).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut events = receptionist.subscribe_events();
    /// tokio::spawn(async move {
    ///     while let Some(event) = events.recv().await {
    ///         println!("{event:?}");
    ///     }
    /// });
    /// ```
    pub fn subscribe_events(&self) -> mpsc::UnboundedReceiver<ActorEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.event_subscribers.lock().unwrap().push(tx);
        rx
    }

    fn emit(&self, event: ActorEvent) {
        let mut subs = self.inner.event_subscribers.lock().unwrap();
        subs.retain(|tx| tx.send(event.clone()).is_ok());
    }

    // =========================================================================
    // RECEPTION KEYS & LISTINGS
    // =========================================================================

    /// Check in an actor with a group key. The actor must already be
    /// registered (via `start()` or `register_remote()`).
    ///
    /// Multiple actors can share the same key. This is how you build
    /// actor groups — register several actors with the same key, then
    /// subscribe to the key to discover them all.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let key = ReceptionKey::<Worker>::new("workers");
    /// receptionist.check_in("worker/0", key.clone());
    /// receptionist.check_in("worker/1", key.clone());
    /// ```
    pub fn check_in<A: Actor + 'static>(&self, label: &str, key: ReceptionKey<A>) {
        let erased_key = ErasedReceptionKey {
            id: key.id.clone(),
            type_id: TypeId::of::<A>(),
        };

        // Lock both in consistent order: entries → listing_subscribers
        let entries = self.inner.entries.lock().unwrap();
        let Some(entry) = entries.get(label) else {
            return;
        };
        if entry.type_id != TypeId::of::<A>() {
            return;
        }

        // Notify matching listing subscribers
        if self.inner.config.flush_interval.is_some() {
            // Delayed flush mode: buffer the notification
            self.inner
                .pending_notifications
                .lock()
                .unwrap()
                .push(PendingListingNotification {
                    label: label.to_string(),
                    key_id: key.id.clone(),
                    type_id: TypeId::of::<A>(),
                });
        } else {
            // Immediate mode: notify now
            let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();
            listing_subs.retain(|sub| !sub.is_closed());
            for sub in listing_subs.iter() {
                if sub.key_id() == key.id && sub.actor_type_id() == TypeId::of::<A>() {
                    sub.try_send_from_entry(entry);
                }
            }
        }

        // Notify watched listing subscribers (always immediate — no buffering)
        {
            let mut watched_subs = self.inner.watched_listing_subscribers.lock().unwrap();
            watched_subs.retain(|sub| !sub.is_closed());
            for sub in watched_subs.iter() {
                if sub.key_id() == key.id && sub.actor_type_id() == TypeId::of::<A>() {
                    sub.try_send_added(entry);
                }
            }
        }

        // Must drop entries before mutating it (need mutable borrow)
        drop(entries);

        // Add key to entry
        if let Some(e) = self.inner.entries.lock().unwrap().get_mut(label) {
            e.keys.push(erased_key);
        }
    }

    /// Get a live listing of actors registered with the given key.
    ///
    /// Returns existing matches immediately (backfill), then streams
    /// new actors as they check in with the matching key.
    ///
    /// Analogous to Swift's `receptionist.listing(of: key)`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let key = ReceptionKey::<Worker>::new("workers");
    /// let mut listing = receptionist.listing(key);
    /// while let Some(ep) = listing.next().await {
    ///     ep.send(Ping).await.ok();
    /// }
    /// ```
    pub fn listing<A: Actor + 'static>(&self, key: ReceptionKey<A>) -> Listing<A> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Lock both atomically: entries → listing_subscribers
        // This prevents a race where an actor checks in between
        // backfill and subscription registration.
        let entries = self.inner.entries.lock().unwrap();
        let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();

        // Backfill: send all existing actors that match this key
        for entry in entries.values() {
            if entry.type_id == TypeId::of::<A>()
                && entry
                    .keys
                    .iter()
                    .any(|k| k.id == key.id && k.type_id == TypeId::of::<A>())
            {
                let temp_sender = TypedListingSender::<A> {
                    key_id: key.id.clone(),
                    tx: tx.clone(),
                };
                temp_sender.try_send_from_entry(entry);
            }
        }

        // Register for future notifications
        listing_subs.push(Box::new(TypedListingSender::<A> {
            key_id: key.id.clone(),
            tx,
        }));

        // Clean up closed subscribers while we have the lock
        listing_subs.retain(|sub| !sub.is_closed());

        Listing::new(rx)
    }

    /// Get a watched listing that streams both additions and removals with labels.
    ///
    /// Unlike [`listing`](Self::listing), this returns [`ListingEvent`](crate::ListingEvent) values
    /// that include the actor's label and distinguish between additions and
    /// removals. Use this to build reactive pools that auto-track membership.
    ///
    /// Backfills existing matches as `Added` events, then streams live updates.
    pub fn watched_listing<A: Actor + 'static>(&self, key: ReceptionKey<A>) -> WatchedListing<A> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Lock atomically: entries → watched_listing_subscribers
        let entries = self.inner.entries.lock().unwrap();
        let mut watched_subs = self.inner.watched_listing_subscribers.lock().unwrap();

        // Backfill: send Added events for all existing matches
        for entry in entries.values() {
            if entry.type_id == TypeId::of::<A>()
                && entry
                    .keys
                    .iter()
                    .any(|k| k.id == key.id && k.type_id == TypeId::of::<A>())
            {
                let temp_sender = TypedWatchedListingSender::<A> {
                    key_id: key.id.clone(),
                    tx: tx.clone(),
                };
                temp_sender.try_send_added(entry);
            }
        }

        // Register for future add + remove notifications
        watched_subs.push(Box::new(TypedWatchedListingSender::<A> {
            key_id: key.id.clone(),
            tx,
        }));

        watched_subs.retain(|sub| !sub.is_closed());

        WatchedListing::new(rx)
    }

    // =========================================================================
    // OPLOG REPLICATION
    // =========================================================================

    /// Get this receptionist's current version vector.
    /// Includes our own latest sequence + observed sequences from peers.
    pub fn version_vector(&self) -> VersionVector {
        let mut vv = self.inner.observed_versions.lock().unwrap().clone();
        let oplog = self.inner.oplog.lock().unwrap();
        let latest = oplog.latest_seq();
        if latest > 0 {
            vv.update(&self.inner.config.node_id, latest);
        }
        vv
    }

    /// Get ops from our local log that the peer hasn't seen yet.
    /// The peer provides their version vector so we know what to send.
    pub fn ops_since(&self, peer_versions: &VersionVector) -> Vec<Op> {
        let oplog = self.inner.oplog.lock().unwrap();
        let peer_has_seen = peer_versions.get(&self.inner.config.node_id);
        oplog.ops_since(peer_has_seen).to_vec()
    }

    /// Apply operations received from a remote peer.
    /// Uses version vectors for deduplication — already-seen ops are skipped.
    pub fn apply_ops(&self, ops: Vec<Op>) {
        for op in ops {
            {
                let mut versions = self.inner.observed_versions.lock().unwrap();
                let already_seen = versions.get(&op.node_id) >= op.seq;
                if already_seen {
                    continue;
                }
                versions.update(&op.node_id, op.seq);
            }

            match &op.op_type {
                OpType::Register {
                    label,
                    actor_type_name,
                    ..
                } => {
                    // For remote ops, we emit an event but don't create a full entry
                    // (the actual entry would be created by register_remote when
                    // the connection is established).
                    self.emit(ActorEvent::Registered {
                        label: label.clone(),
                        actor_type: actor_type_name.clone(),
                    });
                }
                OpType::Remove { label } => {
                    self.deregister(label);
                }
            }
        }
    }

    /// Remove all actors owned by a specific node.
    /// Used when the cluster detects a node has left or crashed.
    ///
    /// Returns the labels of actors that were removed, so the orchestrator
    /// can determine which `ActorSpec`s need re-placement.
    pub fn prune_node(&self, node_id: &str) -> Vec<String> {
        let labels_to_remove: Vec<String> = {
            let entries = self.inner.entries.lock().unwrap();
            entries
                .values()
                .filter(|e| e.node_id == node_id)
                .map(|e| e.label.clone())
                .collect()
        };

        for label in &labels_to_remove {
            // deregister handles oplog recording + event emission
            self.deregister(label);
        }

        labels_to_remove
    }

    /// Get a snapshot of the oplog for inspection/testing.
    pub fn oplog_snapshot(&self) -> Vec<Op> {
        self.inner.oplog.lock().unwrap().ops.clone()
    }

    // =========================================================================
    // DELAYED FLUSH & BLIP AVOIDANCE
    // =========================================================================

    /// Record a register op, respecting blip window if configured.
    fn record_register_op(&self, label: &str, actor_type_name: &str, key_ids: &[String]) {
        let blip_window = self.inner.config.blip_window;
        let origin_addr = self.inner.config.origin_addr.clone();

        if let Some(window) = blip_window {
            // Blip avoidance: delay the oplog entry
            let receptionist = self.clone();
            let label_owned = label.to_string();
            let actor_type_name = actor_type_name.to_string();
            let key_ids = key_ids.to_vec();

            let label_for_insert = label_owned.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(window).await;

                // If the actor is still registered after the window, commit the op
                let still_exists = receptionist
                    .inner
                    .entries
                    .lock()
                    .unwrap()
                    .contains_key(&label_owned);

                if still_exists {
                    receptionist
                        .inner
                        .oplog
                        .lock()
                        .unwrap()
                        .append(OpType::Register {
                            label: label_owned.clone(),
                            actor_type_name,
                            key_ids,
                            origin_addr,
                        });
                }

                // Remove from pending
                receptionist
                    .inner
                    .blip_pending
                    .lock()
                    .unwrap()
                    .remove(&label_owned);
            });

            self.inner
                .blip_pending
                .lock()
                .unwrap()
                .insert(label_for_insert, handle);
        } else {
            // No blip window: commit immediately
            self.inner.oplog.lock().unwrap().append(OpType::Register {
                label: label.to_string(),
                actor_type_name: actor_type_name.to_string(),
                key_ids: key_ids.to_vec(),
                origin_addr,
            });
        }
    }

    /// Flush pending listing notifications (called by the flush timer task).
    fn flush_pending_notifications(&self) {
        let pending: Vec<PendingListingNotification> = {
            let mut pending = self.inner.pending_notifications.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        if pending.is_empty() {
            return;
        }

        let entries = self.inner.entries.lock().unwrap();
        let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();
        listing_subs.retain(|sub| !sub.is_closed());

        for notif in &pending {
            if let Some(entry) = entries.get(&notif.label) {
                for sub in listing_subs.iter() {
                    if sub.key_id() == notif.key_id && sub.actor_type_id() == notif.type_id {
                        sub.try_send_from_entry(entry);
                    }
                }
            }
        }
    }
}

impl Default for Receptionist {
    fn default() -> Self {
        Self::new()
    }
}
