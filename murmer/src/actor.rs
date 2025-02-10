//! Core actor traits and types for the actor system.

use super::message::Message;

/// Errors that can occur during actor operations
#[derive(thiserror::Error, Debug)]
pub enum ActorError {
    /// Returned when trying to send a message to an actor whose mailbox has been closed
    #[error("Actor mailbox has been closed")]
    MailboxClosed,
    /// Returned when the response channel was dropped before receiving the result
    #[error("Actor response was dropped unexpectedly")]
    ResponseDropped,
}

/// The core actor trait that must be implemented by all actors.
///
/// Actors are the fundamental unit of computation in the actor system. They:
/// - Process messages one at a time
/// - Maintain private state
/// - Can send messages to other actors
/// - Have a lifecycle managed by the system
pub trait Actor: Unpin + Sized + Send + 'static {
    /// Called when the actor is started but before it begins processing messages.
    /// Use this to perform any initialization.
    fn started(&mut self, _ctx: &mut Context) {}

    /// Called when the actor is about to be shut down, before processing remaining messages.
    /// Use this to prepare for shutdown.
    fn stopping(&mut self, _ctx: &mut Context) {}

    /// Called after the actor has been shut down and finished processing messages.
    /// Use this for final cleanup.
    fn stopped(&mut self, _ctx: &mut Context) {}
}

/// A trait for handling specific message types.
///
/// Implement this trait for your actor for each message type it should handle.
/// The associated Result type defines what will be returned to the sender.
pub trait Handler<M>
where
    Self: Actor,
    M: Message,
{
    /// Handle a message of type M and return a result of type M::Result
    fn handle(&mut self, ctx: &mut Context, message: M) -> M::Result;
}

/// Provides context and capabilities to actors during message handling.
///
/// The context gives actors access to:
/// - Their own address for self-messaging
/// - The actor system
/// - Other runtime capabilities
#[derive(Default)]
pub struct Context {}
