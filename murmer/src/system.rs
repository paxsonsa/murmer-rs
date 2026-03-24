//! System — unified entry point for local and clustered actor systems.
//!
//! `System` provides a single API surface that works identically whether
//! you're running actors in a single process or across a multi-node cluster.
//! Your actor code never changes — only the system construction differs.
//!
//! # Local development
//!
//! ```rust,ignore
//! let system = System::local();
//! let room = system.start("room/main", ChatRoom, state);
//! room.send(PostMessage { ... }).await?;
//! ```
//!
//! # Clustered deployment
//!
//! ```rust,ignore
//! let system = System::clustered(config, type_registry).await?;
//! let room = system.start("room/main", ChatRoom, state);
//! room.send(PostMessage { ... }).await?;  // identical API
//! ```
//!
//! # Design rationale
//!
//! In BEAM/OTP, you write a GenServer and it works the same regardless of
//! whether it runs on the local node or a remote one. The topology is a
//! deployment concern, not a code concern. `System` brings that same
//! philosophy to murmer: write your actors once, and swap between local
//! and distributed execution by changing a single line at startup.

use crate::cluster::ClusterSystem;
use crate::cluster::config::ClusterConfig;
use crate::cluster::error::ClusterError;
use crate::cluster::sync::{SpawnRegistry, TypeRegistry};
use crate::lifecycle::{ActorFactory, RestartConfig, RestartPolicy};
use crate::listing::{Listing, ReceptionKey};
use crate::ready::ReadyHandle;
use crate::receptionist::{ActorEvent, Receptionist};
use crate::{Actor, Endpoint, RemoteDispatch};

/// A unified actor system that can run locally or as part of a cluster.
///
/// All actor operations go through the same methods regardless of mode.
/// The only difference is construction:
///
/// - [`System::local()`] — in-memory, no networking
/// - [`System::clustered()`] — QUIC transport, SWIM membership, registry replication
pub struct System {
    inner: SystemInner,
}

enum SystemInner {
    Local { receptionist: Receptionist },
    Clustered { cluster: Box<ClusterSystem> },
}

impl System {
    /// Create a local system with no networking.
    ///
    /// All actors run in the same process and communicate through in-memory
    /// channels. Zero serialization cost, instant startup. Ideal for
    /// development and testing.
    pub fn local() -> Self {
        Self {
            inner: SystemInner::Local {
                receptionist: Receptionist::new(),
            },
        }
    }

    /// Create a clustered system with QUIC transport and automatic discovery.
    ///
    /// Binds a QUIC listener, starts SWIM membership, and begins peer
    /// discovery according to the config. Actors started on this system
    /// are discoverable by other nodes in the cluster.
    pub async fn clustered(
        config: ClusterConfig,
        type_registry: TypeRegistry,
        spawn_registry: SpawnRegistry,
    ) -> Result<Self, ClusterError> {
        let cluster = ClusterSystem::start(config, type_registry, spawn_registry).await?;
        Ok(Self {
            inner: SystemInner::Clustered {
                cluster: Box::new(cluster),
            },
        })
    }

    /// Create a clustered system with auto-discovered type registry.
    ///
    /// Uses [`TypeRegistry::from_auto()`] to automatically register all actor
    /// types annotated with `#[handlers]` — no manual registry setup needed.
    pub async fn clustered_auto(config: ClusterConfig) -> Result<Self, ClusterError> {
        Self::clustered(config, TypeRegistry::from_auto(), SpawnRegistry::new()).await
    }

    // =========================================================================
    // ACTOR LIFECYCLE
    // =========================================================================

    /// Start an actor and return its endpoint.
    ///
    /// The returned `Endpoint<A>` works identically in both local and
    /// clustered modes. In local mode, messages are dispatched through
    /// in-memory channels. In clustered mode, the actor is also registered
    /// for discovery by remote nodes.
    pub fn start<A>(&self, label: &str, actor: A, state: A::State) -> Endpoint<A>
    where
        A: Actor + RemoteDispatch + 'static,
    {
        self.receptionist().start(label, actor, state)
    }

    /// Prepare an actor for deferred start.
    ///
    /// Returns an `Endpoint<A>` that can immediately queue messages, and a
    /// [`ReadyHandle`] whose `start()` method spawns the supervisor.
    /// If the `ReadyHandle` is dropped without calling `start()`, the actor
    /// is automatically deregistered.
    pub fn prepare<A>(
        &self,
        label: &str,
        actor: A,
        state: A::State,
    ) -> (Endpoint<A>, ReadyHandle<A>)
    where
        A: Actor + RemoteDispatch + 'static,
    {
        self.receptionist().prepare(label, actor, state)
    }

    /// Start an actor with a simple restart policy.
    pub fn start_with_policy<F: ActorFactory>(
        &self,
        label: &str,
        factory: F,
        policy: RestartPolicy,
    ) -> Endpoint<F::Actor>
    where
        F::Actor: RemoteDispatch,
    {
        self.receptionist()
            .start_with_policy(label, factory, policy)
    }

    /// Start an actor with full restart configuration (policy, limits, backoff).
    pub fn start_with_config<F: ActorFactory>(
        &self,
        label: &str,
        factory: F,
        config: RestartConfig,
    ) -> Endpoint<F::Actor>
    where
        F::Actor: RemoteDispatch,
    {
        self.receptionist()
            .start_with_config(label, factory, config)
    }

    // =========================================================================
    // ACTOR LOOKUP & DISCOVERY
    // =========================================================================

    /// Look up an actor by label.
    ///
    /// Returns `None` if no actor with the given label exists, or if the
    /// type doesn't match. In clustered mode, this also finds actors on
    /// remote nodes that have been replicated to the local registry.
    pub fn lookup<A: Actor + 'static>(&self, label: &str) -> Option<Endpoint<A>> {
        self.receptionist().lookup(label)
    }

    /// Check an actor into a reception key group for discovery.
    pub fn check_in<A: Actor + 'static>(&self, label: &str, key: ReceptionKey<A>) {
        self.receptionist().check_in(label, key);
    }

    /// Subscribe to a reception key and receive endpoints as actors check in.
    pub fn listing<A: Actor + 'static>(&self, key: ReceptionKey<A>) -> Listing<A> {
        self.receptionist().listing(key)
    }

    /// Subscribe to lifecycle events (registrations and deregistrations).
    pub fn subscribe_events(&self) -> tokio::sync::mpsc::UnboundedReceiver<ActorEvent> {
        self.receptionist().subscribe_events()
    }

    // =========================================================================
    // LIFECYCLE
    // =========================================================================

    /// Stop an actor by label.
    pub fn stop(&self, label: &str) {
        self.receptionist().stop(label);
    }

    /// Shut down the system gracefully.
    ///
    /// In clustered mode, broadcasts a departure message to peers before
    /// shutting down. In local mode, this is a no-op (actors are cleaned
    /// up when the system is dropped).
    pub async fn shutdown(&self) {
        match &self.inner {
            SystemInner::Local { .. } => {}
            SystemInner::Clustered { cluster } => {
                cluster.shutdown().await;
            }
        }
    }

    /// Get a reference to the underlying receptionist.
    ///
    /// Useful for advanced operations like `register_remote` or direct
    /// OpLog access. For typical use, prefer the `System` methods.
    pub fn receptionist(&self) -> &Receptionist {
        match &self.inner {
            SystemInner::Local { receptionist } => receptionist,
            SystemInner::Clustered { cluster } => cluster.receptionist(),
        }
    }

    /// Returns `true` if this is a clustered system.
    pub fn is_clustered(&self) -> bool {
        matches!(self.inner, SystemInner::Clustered { .. })
    }

    /// Access the underlying `ClusterSystem`, if this is a clustered system.
    ///
    /// Returns `None` for local systems. Used by orchestration (`app` module)
    /// to access the transport and event bus.
    pub fn cluster_system(&self) -> Option<&ClusterSystem> {
        match &self.inner {
            SystemInner::Local { .. } => None,
            SystemInner::Clustered { cluster } => Some(cluster),
        }
    }

    /// Returns the local QUIC address if this is a clustered system.
    ///
    /// Useful for reading the OS-assigned port after binding to `127.0.0.1:0`,
    /// e.g. to pass as a seed address to other nodes in tests.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            SystemInner::Local { .. } => None,
            SystemInner::Clustered { cluster } => Some(cluster.local_addr()),
        }
    }
}
