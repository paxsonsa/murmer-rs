//! Commonly used actor system types and traits.
//!
//! This module re-exports the most commonly used items from the actor system.
//! Import this module to get started with the basic actor functionality.

pub use super::actor::{
    Actor,      // Core actor trait
    ActorError, // Error types for actor operations
    Context,    // Execution context for actors
    Handler,    // Message handling trait
};
pub use super::id::Id; // An identifier type used for actors and other unique entities.
pub use super::message::Message; // Message trait for actor communication
pub use super::path::*;
pub use super::system::{
    AnyEndpoint, // A type-erased endpoint to communicate with an actor.
    Endpoint,    // An endpoint to communicate with an actor.
    System,      // The actor system itself (renamed from System)
    SystemError, // System-level errors
};
