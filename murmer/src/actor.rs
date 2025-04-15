//! Core actor traits and types for the actor system.
use crate::context::Context;

use super::message::{Message, RemoteMessage};
pub use async_trait::async_trait;

/// The core actor trait that must be implemented by all actors.
///
/// Actors are the fundamental unit of computation in the actor system. They:
/// - Process messages one at a time
/// - Maintain private state
/// - Can send messages to other actors
/// - Have a lifecycle managed by the system
#[async_trait]
pub trait Actor: Unpin + Sized + Send + 'static {
    /// Called when the actor is started but before it begins processing messages.
    /// Use this to perform any initialization.
    async fn started(&mut self, _ctx: &mut Context<Self>) {}

    /// Called when the actor is about to be shut down, before processing remaining messages.
    /// Use this to prepare for shutdown.
    async fn stopping(&mut self, _ctx: &mut Context<Self>) {}

    /// Called after the actor has been shut down and finished processing messages.
    /// Use this for final cleanup.
    async fn stopped(&mut self, _ctx: &mut Context<Self>) {}
}

pub trait Registered
where
    Self: Actor + Handler<RemoteMessage>,
{
    const RECEPTIONIST_KEY: &'static str;
}

/// A trait for handling specific message types.
///
/// Implement this trait for your actor for each message type it should handle.
/// The associated Result type defines what will be returned to the sender.
#[async_trait]
pub trait Handler<M>
where
    Self: Actor,
    M: Message,
{
    /// Handle a message of type M and return a result of type M::Result
    async fn handle(&mut self, ctx: &mut Context<Self>, message: M) -> M::Result;
}
