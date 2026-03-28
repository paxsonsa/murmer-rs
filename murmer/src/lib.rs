//! # Murmer — A distributed actor framework for Rust
//!
//! Murmer provides typed, location-transparent actors that communicate through
//! message passing. Whether an actor lives in the same process or on a remote
//! node across the network, you interact with it through the same [`Endpoint<A>`] API.
//!
//! **[Read the book →](https://paxsonsa.github.io/murmer-rs/)**
//! &nbsp;·&nbsp;
//! **[GitHub](https://github.com/paxsonsa/murmer-rs)**
//!
//! ---
//!
//! ## Murmer in 1 minute
//!
//! ```toml
//! [dependencies]
//! murmer = "0.1"
//! serde = { version = "1", features = ["derive"] }
//! tokio = { version = "1", features = ["full"] }
//! ```
//!
//! ```rust,ignore
//! use murmer::prelude::*;
//!
//! // ① Define your actor — state lives separately
//! #[derive(Debug)]
//! struct Counter;
//! struct CounterState { count: i64 }
//!
//! impl Actor for Counter {
//!     type State = CounterState;
//! }
//!
//! // ② Handlers become the actor's API
//! #[handlers]
//! impl Counter {
//!     #[handler]
//!     fn increment(
//!         &mut self,
//!         _ctx: &ActorContext<Self>,
//!         state: &mut CounterState,
//!         amount: i64,
//!     ) -> i64 {
//!         state.count += amount;
//!         state.count
//!     }
//!
//!     #[handler]
//!     fn get_count(
//!         &mut self,
//!         _ctx: &ActorContext<Self>,
//!         state: &mut CounterState,
//!     ) -> i64 {
//!         state.count
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // ③ Create a local actor system
//!     let system = System::local();
//!
//!     // ④ Start an actor — returns a typed Endpoint<Counter>
//!     let counter = system.start("counter/main", Counter, CounterState { count: 0 });
//!
//!     // ⑤ Send messages via auto-generated extension methods
//!     let result = counter.increment(5).await.unwrap();
//!     println!("Count: {result}"); // → Count: 5
//!
//!     // ⑥ Look up actors by label — works for local and remote
//!     let found = system.lookup::<Counter>("counter/main").unwrap();
//!     let count = found.get_count().await.unwrap();
//!     println!("Looked up: {count}"); // → Looked up: 5
//! }
//! ```
//!
//! ## What it gives you
//!
//! - **Send messages without caring where the actor lives.** `counter.increment(5)` works
//!   identically whether the actor is local or on a remote node — [`Endpoint<A>`] abstracts
//!   the difference away.
//! - **Test distributed systems from a single process.** [`System::local`] runs everything
//!   in-memory. Swap to `System::clustered_auto` when ready for real networking — your
//!   actor code stays identical.
//! - **Define actors with minimal boilerplate.** The `#[handlers]` macro auto-generates
//!   message structs, dispatch tables, serialization, and ergonomic extension methods.
//! - **Get networking and encryption handled for you.** QUIC transport with automatic TLS,
//!   SWIM-based cluster membership, and mDNS discovery — all configured, not hand-rolled.
//! - **Supervise actors like OTP.** Restart policies with configurable limits and
//!   exponential backoff keep your system running through failures.
//!
//! ## Core concepts
//!
//! | Concept | Type | Purpose |
//! |---------|------|---------|
//! | **Actor** | [`Actor`] | Stateful message processor. State lives in an associated `State` type. |
//! | **Message** | [`Message`] | Defines a request and its response type. |
//! | **RemoteMessage** | [`RemoteMessage`] | A message that can cross the wire (serializable + `TYPE_ID`). |
//! | **Endpoint** | [`Endpoint<A>`] | Opaque send handle. Abstracts local vs remote — callers never know which. |
//! | **Receptionist** | [`Receptionist`] | Type-erased actor registry. Start, lookup, and subscribe to actors. |
//! | **Router** | [`Router<A>`] | Distributes messages across a pool of endpoints (round-robin, broadcast). |
//! | **Listing** | [`Listing<A>`] | Async stream of endpoints matching a [`ReceptionKey`]. |
//!
//! ## Location transparency
//!
//! The key design principle: [`Endpoint<A>`] hides whether the actor is local or remote.
//!
//! - **Local actors** use the [envelope pattern](wire::EnvelopeProxy) — zero serialization
//!   cost, direct in-memory dispatch through a type-erased trait object.
//! - **Remote actors** serialize messages with [bincode], send them over QUIC streams,
//!   and deserialize responses on return.
//!
//! The caller's code is identical in both cases:
//!
//! ```rust,ignore
//! let result = endpoint.send(Increment { amount: 5 }).await?;
//! ```
//!
//! ## Actor discovery
//!
//! The [`Receptionist`] is the central registry for actor discovery:
//!
//! - **Labels** identify actors with path-like strings (`"cache/user"`, `"worker/0"`).
//! - **Typed lookup** via `receptionist.lookup::<MyActor>("label")` returns `Option<Endpoint<A>>`.
//! - **Reception keys** group actors by type for subscription-based discovery.
//! - **Listings** provide async streams of endpoints as actors register and deregister.
//!
//! ## Supervision
//!
//! Actors are managed by supervisors that handle lifecycle and crash recovery:
//!
//! - [`RestartPolicy::Temporary`] — never restart (default)
//! - [`RestartPolicy::Transient`] — restart only on panic
//! - [`RestartPolicy::Permanent`] — always restart
//!
//! Restart limits and exponential backoff are configured via [`RestartConfig`].
//!
//! ## Clustering
//!
//! The [`cluster`] module provides multi-node actor systems over QUIC:
//!
//! - **SWIM protocol** membership via [`foca`](https://docs.rs/foca) for failure detection
//! - **mDNS discovery** for zero-configuration LAN clustering
//! - **OpLog replication** with version vectors for consistent registry views
//! - **Per-actor QUIC streams** — one multiplexed connection per node pair
//!
//! ## Going from local to clustered
//!
//! Only the system construction changes — all actor code stays identical:
//!
//! ```rust,ignore
//! // Local
//! let system = System::local();
//!
//! // Clustered
//! let config = ClusterConfig::builder()
//!     .name("my-node")
//!     .listen("0.0.0.0:7100".parse()?)
//!     .cookie("my-cluster-secret")
//!     .build()?;
//!
//! let system = System::clustered_auto(config).await?;
//! ```
//!
//! ## Learn more
//!
//! - **[The Murmer Book](https://paxsonsa.github.io/murmer-rs/)** — full guide with
//!   examples, diagrams, and deep-dives into every component
//! - [`cluster`] — multi-node clustering and networking
//! - [`receptionist`] — actor discovery and subscriptions
//! - [`lifecycle`] — supervision, restart policies, and actor factories
//! - [`router`] — round-robin and broadcast routing across actor pools

// Allow proc-macro-generated code (e.g. `#[handlers]`) to reference `murmer::`
// and `::murmer::` when used inside this crate — same pattern as serde.
extern crate self as murmer;

pub mod actor;
#[cfg(feature = "app")]
pub mod app;
pub mod client;
pub mod cluster;
pub mod endpoint;
#[allow(dead_code)]
pub(crate) mod instrument;
pub mod lifecycle;
pub mod listing;
#[cfg(feature = "monitor")]
pub mod monitor;
pub mod node;
pub mod oplog;
pub mod ready;
pub mod receptionist;
pub mod router;
pub mod supervisor;
pub mod system;
pub mod wire;

/// Re-export dependencies so generated code can reference them without the user
/// needing them in their Cargo.toml.
pub mod __reexport {
    pub use bincode;
    pub use linkme;
}

// =============================================================================
// AUTO-REGISTRATION — linkme distributed slice for TypeRegistry entries
// =============================================================================

/// An entry for auto-registration of an actor type in the cluster's [`cluster::sync::TypeRegistry`].
///
/// The `#[handlers]` macro emits one of these per actor type into the
/// [`ACTOR_TYPE_ENTRIES`] distributed slice. At startup, [`cluster::sync::TypeRegistry::from_auto()`]
/// iterates the slice to build the registry automatically.
pub struct TypeRegistryEntry {
    /// Returns the fully-qualified actor type name (via `std::any::type_name::<A>()`).
    pub actor_type_name: fn() -> &'static str,
    /// Registers a remote endpoint for this actor type in the receptionist.
    pub register: fn(
        &crate::Receptionist,
        &str,
        tokio::sync::mpsc::UnboundedSender<crate::RemoteInvocation>,
        crate::ResponseRegistry,
        &str,
        crate::receptionist::Visibility,
    ),
}

// TypeRegistryEntry contains only function pointers — inherently Send + Sync.
unsafe impl Sync for TypeRegistryEntry {}

/// Distributed slice populated by `#[handlers]` macro expansions across all crates.
///
/// Each actor type annotated with `#[handlers]` contributes one [`TypeRegistryEntry`]
/// to this slice at link time. Use [`cluster::sync::TypeRegistry::from_auto()`] to
/// collect them into a ready-to-use registry.
#[__reexport::linkme::distributed_slice]
pub static ACTOR_TYPE_ENTRIES: [TypeRegistryEntry];

// =============================================================================
// FEATURE RE-EXPORTS
// =============================================================================

/// Re-export proc macros from `murmer-macros` (enabled by the `macros` feature, on by default).
///
/// This lets users write `use murmer::handlers;` instead of depending on `murmer-macros` directly.
#[cfg(feature = "macros")]
pub use murmer_macros::{Message, handlers};

/// Convenience prelude — import everything you need for typical actor definitions.
///
/// ```rust,ignore
/// use murmer::prelude::*;
/// ```
pub mod prelude {
    pub use crate::actor::DispatchError;
    pub use crate::actor::{
        Actor, ActorContext, ActorRef, AsyncHandler, Handler, Message, MigratableActor,
        RemoteDispatch, RemoteMessage,
    };
    pub use crate::endpoint::Endpoint;
    pub use crate::lifecycle::{
        ActorFactory, ActorTerminated, BackoffConfig, RestartConfig, RestartPolicy,
        TerminationReason,
    };
    pub use crate::listing::{Listing, ListingEvent, ReceptionKey, WatchedListing};
    pub use crate::ready::ReadyHandle;
    pub use crate::receptionist::{ActorEvent, Receptionist, ReceptionistConfig, Visibility};
    pub use crate::router::{PoolRouter, Router, RoutingStrategy};
    pub use crate::system::System;
    pub use crate::wire::{ReplySender, SendError};

    // Re-export macros into the prelude so `use murmer::prelude::*` brings them in
    #[cfg(feature = "macros")]
    pub use murmer_macros::{Message, handlers};
}

// Re-export core types for convenience
pub use actor::{
    Actor, ActorContext, ActorRef, AsyncHandler, DispatchError, Handler, Message, MigratableActor,
    RemoteDispatch, RemoteMessage,
};
pub use client::{ClientOptions, MurmerClient};
pub use endpoint::Endpoint;
pub use lifecycle::{
    ActorFactory, ActorTerminated, BackoffConfig, RestartConfig, RestartPolicy, TerminationReason,
};
pub use listing::{Listing, ListingEvent, ReceptionKey, WatchedListing};
pub use node::run_node_receiver;
pub use oplog::{Op, OpType, VersionVector};
pub use ready::ReadyHandle;
pub use receptionist::{ActorEvent, Receptionist, ReceptionistConfig, Visibility};
pub use router::{PoolRouter, Router, RoutingStrategy};
pub use system::System;
pub use wire::{
    DispatchRequest, RemoteInvocation, RemoteResponse, ReplySender, ResponseRegistry, SendError,
};
