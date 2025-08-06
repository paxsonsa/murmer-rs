/// Provides context and capabilities to actors during message handling.
///
/// The context gives actors access to:
/// - Their own address for self-messaging
/// - The actor system
/// - Other runtime capabilities
pub struct Context<A>
where
    A: Actor,
{
    // TODO: Eventual fields for the context behaviors.
    // endpoint: Endpoint<A>,
    // cancellation: CancellationToken,
    pub(crate) _phantom: std::marker::PhantomData<A>,
}

#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    /// An error that occurs during actor initialization
    #[error("Actor initialization failed: {0}")]
    InitError(String),
}

/// A trait that represents the actor.
///
/// The core actor trait that must be implemented by all actors.
/// A type that implements this trait can be used with the actor system.
///
/// Actors are the fundamental unit of computation in the actor system. They:
/// - Process messages one at a time
/// - Maintain private state
/// - Can send messages to other actors
/// - Have a lifecycle managed by the system
#[async_trait::async_trait]
pub trait Actor: Unpin + Sized + Send + 'static {
    /// The state that the actor maintains.
    type State: Sync + Send;

    /// A unique key for identifying the actor across the system.
    const ACTOR_TYPE_KEY: &'static str;

    /// Initialize the actor's state. This is called when the actor is created by the system.
    async fn init(&mut self, ctx: &mut Context<Self>) -> Result<Self::State, ActorError>;

    /// Called when the actor is started but before it begins processing messages.
    /// Use this to perform any initialization.
    async fn started(&mut self, _ctx: &mut Context<Self>, _state: &mut Self::State) {}

    /// Called when the actor is about to be shut down, before processing remaining messages.
    /// Use this to prepare for shutdown.
    async fn stopping(&mut self, _ctx: &mut Context<Self>, _state: &mut Self::State) {}

    /// Called after the actor has been shut down and finished processing messages.
    /// Use this for final cleanup.
    async fn stopped(&mut self, _ctx: &mut Context<Self>, _state: Self::State) {}
}

/// A trait for handling specific message types.
///
/// Implement this trait for your actor for each message type it should handle.
/// The associated Result type defines what will be returned to the sender.
#[async_trait::async_trait]
pub trait Handler<M>
where
    Self: Actor,
    M: crate::message::Message,
{
    /// Handle a message of type M and return a result of type M::Result
    async fn handle(
        &mut self,
        ctx: &mut Context<Self>,
        state: &mut Self::State,
        message: M,
    ) -> M::Result;
}