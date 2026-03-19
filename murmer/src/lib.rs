//! Murmer: a distributed actor framework for Rust.
//!
//! Key design decisions:
//! - Endpoint<A> abstracts local vs remote — caller never knows which
//! - All messages through endpoints require RemoteMessage (serialization bounds)
//! - Receptionist stores type-erased entries, typed lookup via caller-supplied generic
//! - Lifecycle events are fully type-erased (no actor type parameter)
//! - Internal downcast is guarded by TypeId, never exposed to user
//! - ReceptionKey<A> groups actors by type for subscription-based discovery
//! - Listing<A> provides async streams of endpoints as actors register
//! - OpLog replication protocol enables cross-node registry sync
//! - Delayed flush and blip avoidance optimize network efficiency

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

// Re-export bincode so generated code can reference it without the user
// needing bincode in their Cargo.toml
pub mod __reexport {
    pub use bincode;
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
