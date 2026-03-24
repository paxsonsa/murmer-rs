//! # Murmer — A distributed actor framework for Rust
//!
//! Murmer provides typed, location-transparent actors that communicate through
//! message passing. Whether an actor lives in the same process or on a remote
//! node across the network, you interact with it through the same
//! [`Endpoint<A>`] API.
//!
//! ## Core concepts
//!
//! | Concept | Type | Purpose |
//! |---------|------|---------|
//! | **Actor** | [`Actor`] | Stateful message processor. Holds no state itself — state lives in an associated `State` type. |
//! | **Message** | [`Message`] | Defines a request and its response type. |
//! | **RemoteMessage** | [`RemoteMessage`] | A message that can cross the wire (serializable + `TYPE_ID`). |
//! | **Endpoint** | [`Endpoint<A>`] | Opaque send handle. Abstracts local vs remote — callers never know which. |
//! | **Receptionist** | [`Receptionist`] | Type-erased actor registry. Start, lookup, and subscribe to actors. |
//! | **Router** | [`Router<A>`] | Distributes messages across a pool of endpoints (round-robin, broadcast). |
//! | **Listing** | [`Listing<A>`] | Async stream of endpoints matching a [`ReceptionKey`]. |
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use murmer::prelude::*;
//!
//! // 1. Define your actor
//! #[derive(Debug)]
//! struct Greeter;
//!
//! struct GreeterState { greeting: String }
//!
//! impl Actor for Greeter {
//!     type State = GreeterState;
//! }
//!
//! // 2. Implement handlers — macro auto-generates message structs,
//! //    Handler impls, RemoteDispatch, and a GreeterExt extension trait
//! #[handlers]
//! impl Greeter {
//!     #[handler]
//!     fn greet(&mut self, _ctx: &ActorContext<Self>, state: &mut GreeterState, name: String) -> String {
//!         format!("{}, {}!", state.greeting, name)
//!     }
//! }
//!
//! // 3. Start and use
//! #[tokio::main]
//! async fn main() {
//!     let system = System::local();
//!     let endpoint = system.start("greeter/main", Greeter, GreeterState {
//!         greeting: "Hello".into(),
//!     });
//!
//!     // Call via auto-generated extension trait
//!     let reply = endpoint.greet("world".into()).await.unwrap();
//!     assert_eq!(reply, "Hello, world!");
//! }
//! ```
//!
//! ## Location transparency
//!
//! The key design principle: [`Endpoint<A>`] hides whether the actor is local or remote.
//!
//! - **Local actors** use the [envelope pattern](wire::EnvelopeProxy) — zero serialization cost,
//!   direct in-memory dispatch through a type-erased trait object.
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

// Allow proc-macro-generated code (e.g. `#[handlers]`) to reference `murmer::`
// and `::murmer::` when used inside this crate — same pattern as serde.
extern crate self as murmer;

pub mod actor;
#[cfg(feature = "app")]
pub mod app;
pub mod cluster;
pub mod endpoint;
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
    pub use crate::receptionist::{ActorEvent, Receptionist, ReceptionistConfig};
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
pub use endpoint::Endpoint;
pub use lifecycle::{
    ActorFactory, ActorTerminated, BackoffConfig, RestartConfig, RestartPolicy, TerminationReason,
};
pub use listing::{Listing, ListingEvent, ReceptionKey, WatchedListing};
pub use node::run_node_receiver;
pub use oplog::{Op, OpType, VersionVector};
pub use ready::ReadyHandle;
pub use receptionist::{ActorEvent, Receptionist, ReceptionistConfig};
pub use router::{PoolRouter, Router, RoutingStrategy};
pub use system::System;
pub use wire::{
    DispatchRequest, RemoteInvocation, RemoteResponse, ReplySender, ResponseRegistry, SendError,
};
