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
//! use murmer_macros::{Message, handlers};
//! use serde::{Serialize, Deserialize};
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
//! // 2. Define messages — derive does the boilerplate
//! #[derive(Debug, Clone, Serialize, Deserialize, Message)]
//! #[message(result = String, remote = "greeter::Greet")]
//! struct Greet { name: String }
//!
//! // 3. Implement handlers
//! #[handlers]
//! impl Greeter {
//!     #[handler]
//!     fn greet(&mut self, _ctx: &ActorContext<Self>, state: &mut GreeterState, msg: Greet) -> String {
//!         format!("{}, {}!", state.greeting, msg.name)
//!     }
//! }
//!
//! // 4. Start and use
//! #[tokio::main]
//! async fn main() {
//!     let receptionist = Receptionist::new();
//!     let endpoint = receptionist.start("greeter/main", Greeter, GreeterState {
//!         greeting: "Hello".into(),
//!     });
//!
//!     let reply = endpoint.send(Greet { name: "world".into() }).await.unwrap();
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

pub mod actor;
pub mod cluster;
pub mod endpoint;
pub mod lifecycle;
pub mod listing;
pub mod node;
pub mod oplog;
pub mod receptionist;
pub mod router;
pub mod supervisor;
pub mod wire;

/// Re-export bincode so generated code can reference it without the user
/// needing bincode in their Cargo.toml.
pub mod __reexport {
    pub use bincode;
}

/// Convenience prelude — import everything you need for typical actor definitions.
///
/// ```rust,ignore
/// use murmer::prelude::*;
/// ```
pub mod prelude {
    pub use crate::actor::DispatchError;
    pub use crate::actor::{
        Actor, ActorContext, ActorRef, Handler, Message, RemoteDispatch, RemoteMessage,
    };
    pub use crate::endpoint::Endpoint;
    pub use crate::lifecycle::{
        ActorFactory, ActorTerminated, BackoffConfig, RestartConfig, RestartPolicy,
        TerminationReason,
    };
    pub use crate::listing::{Listing, ReceptionKey};
    pub use crate::receptionist::{ActorEvent, Receptionist, ReceptionistConfig};
    pub use crate::router::{Router, RoutingStrategy};
    pub use crate::wire::SendError;
}

// Re-export core types for convenience
pub use actor::{
    Actor, ActorContext, ActorRef, DispatchError, Handler, Message, RemoteDispatch, RemoteMessage,
};
pub use endpoint::Endpoint;
pub use lifecycle::{
    ActorFactory, ActorTerminated, BackoffConfig, RestartConfig, RestartPolicy, TerminationReason,
};
pub use listing::{Listing, ReceptionKey};
pub use node::run_node_receiver;
pub use oplog::{Op, OpType, VersionVector};
pub use receptionist::{ActorEvent, Receptionist, ReceptionistConfig};
pub use router::{Router, RoutingStrategy};
pub use wire::{DispatchRequest, RemoteInvocation, RemoteResponse, ResponseRegistry, SendError};
