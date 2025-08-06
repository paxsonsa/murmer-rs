use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "lib.test.rs"]
mod lib_test;

/// A trait that represents a message that can be sent to an actor.
pub trait Message: std::fmt::Debug + Send + 'static {
    /// The type of result that will be returned when this message is handled
    type Result: Send + 'static;
}

/// A trait for handling the asynchronous execution of messages.
pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle_async<'a>(
        &'a mut self,
        ctx: &'a mut Context<A>,
        state: &'a mut A::State,
        actor: &'a mut A,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub enum ResponseExpectation<M: Message> {
    /// Used when a response is expected
    Expected(Option<oneshot::Sender<M::Result>>),

    /// Used when no response is expected
    NotExpected,
}

pub struct Envelope<A: Actor>(pub Box<dyn EnvelopeProxy<A> + Send>);

impl<A: Actor> Envelope<A> {
    pub fn new<M>(msg: M, tx: oneshot::Sender<M::Result>) -> Self
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        Envelope(Box::new(AsyncEnvelope {
            msg: Some(msg),
            response_expectation: ResponseExpectation::Expected(Some(tx)),
        }))
    }

    pub fn no_response<M>(msg: M) -> Self
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        Envelope(Box::new(AsyncEnvelope {
            msg: Some(msg),
            response_expectation: ResponseExpectation::NotExpected,
        }))
    }
}

struct AsyncEnvelope<M: Message> {
    msg: Option<M>,
    response_expectation: ResponseExpectation<M>,
}

impl<A, M> EnvelopeProxy<A> for AsyncEnvelope<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    #[tracing::instrument(
        name = "handle",
        skip_all,
        fields(
            actor = tracing::field::debug(&std::any::type_name::<A>()),
            message = tracing::field::debug(&std::any::type_name::<M>()),
        )
    )]
    fn handle_async<'a>(
        &'a mut self,
        ctx: &'a mut Context<A>,
        state: &'a mut A::State,
        actor: &'a mut A,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let msg = self.msg.take().expect("message is missing, this is a bug.");

            match &mut self.response_expectation {
                ResponseExpectation::Expected(tx) => {
                    let tx = tx
                        .take()
                        .expect("response expection for expected is missing, this is a bug.");

                    if tx.is_closed() {
                        tracing::error!("response channel is closed before handling");
                        return;
                    }

                    let result = actor.handle(ctx, state, msg).await;

                    let _ = tx.send(result);
                }
                ResponseExpectation::NotExpected => {
                    let _ = actor.handle(ctx, state, msg).await;
                }
            }
        })
    }
}
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
    _phantom: std::marker::PhantomData<A>,
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
    M: Message,
{
    /// Handle a message of type M and return a result of type M::Result
    async fn handle(
        &mut self,
        ctx: &mut Context<Self>,
        state: &mut Self::State,
        message: M,
    ) -> M::Result;
}

#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    /// Returned when trying to send a message to an actor whose mailbox has been closed
    #[error("Actor mailbox has been closed")]
    MailboxClosed,

    /// Returned when the response channel was dropped before receiving the result
    #[error("Actor response was dropped unexpectedly")]
    ResponseDropped,
}

pub struct Endpoint<A>
where
    A: Actor,
{
    channel: tokio::sync::mpsc::UnboundedSender<Envelope<A>>,
}

impl<A: Actor> Endpoint<A> {
    pub(crate) fn new(channel: tokio::sync::mpsc::UnboundedSender<Envelope<A>>) -> Self {
        Self { channel }
    }

    pub async fn send_in_background<M>(&self, message: M) -> Result<(), EndpointError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = Envelope::new(message, tx);
        self.channel
            .send(envelope)
            .map_err(|_| EndpointError::MailboxClosed)?;

        // Wait for the response in the background
        tokio::spawn(async move {
            match rx.await {
                Ok(_) => {}
                Err(_) => {
                    tracing::error!("Response channel was dropped before receiving the result");
                }
            }
        });
        Ok(())
    }

    pub async fn send<M>(&self, message: M) -> Result<M::Result, EndpointError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = Envelope::new(message, tx);
        self.channel
            .send(envelope)
            .map_err(|_| EndpointError::MailboxClosed)?;

        // Wait for the response in the background
        match rx.await {
            Ok(result) => Ok(result),
            Err(_) => Err(EndpointError::ResponseDropped),
        }
    }
}

pub trait State {}

pub struct Uninitialized<A: Actor> {
    actor: A,
}

impl<A> State for Uninitialized<A> where A: Actor {}

#[derive(Clone)]
pub struct Initialized<A: Actor> {
    cancellation: tokio_util::sync::CancellationToken,
    sender: tokio::sync::mpsc::UnboundedSender<Envelope<A>>,
}

impl<A> State for Initialized<A> where A: Actor {}

pub struct Supervisor<A, S>
where
    A: Actor,
    S: State,
{
    state: S,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Supervisor<A, Initialized<A>>
where
    A: Actor,
{
    pub fn endpoint(&self) -> Endpoint<A> {
        let sender = self.state.sender.clone();
        Endpoint::new(sender)
    }
}

impl<A> Supervisor<A, Uninitialized<A>>
where
    A: Actor,
{
    pub fn construct(actor: A) -> Supervisor<A, Uninitialized<A>> {
        Supervisor {
            state: Uninitialized { actor },
            _phantom: std::marker::PhantomData,
        }
    }

    fn start_within(self, system: &System) -> Supervisor<A, Initialized<A>> {
        let cancellation = system.root_cancellation.child_token();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Envelope<A>>();

        let supervisor = Supervisor {
            state: Initialized {
                cancellation,
                sender: sender.clone(),
            },
            _phantom: std::marker::PhantomData,
        };

        let mut actor = self.state.actor;
        let cancellation = supervisor.state.cancellation.child_token();
        tokio::spawn(async move {
            let mut ctx = Context {
                _phantom: std::marker::PhantomData,
            };

            let mut state = match actor.init(&mut ctx).await {
                Ok(state) => state,
                Err(e) => {
                    tracing::error!("Failed to initialize actor: {}", e);
                    return;
                }
            };
            actor.started(&mut ctx, &mut state).await;

            loop {
                // TODO: Support receiving system commands for restarting and checking status.
                tokio::select!(
                    _ = cancellation.cancelled() => {
                        tracing::info!("Actor cancelled, stopping actor.");
                        break;
                    }
                    envelope = receiver.recv() => {
                        if let Some(mut envelope) = envelope {
                            envelope
                                .0
                                .handle_async(&mut ctx, &mut state, &mut actor)
                                .await;
                        } else {
                            tracing::warn!("Actor mailbox closed, stopping actor.");
                            break;
                        }
                    }
                );
            }

            actor.stopping(&mut ctx, &mut state).await;
            actor.stopped(&mut ctx, state).await;
        });
        supervisor
    }
}

/// Builder for creating an new system.
pub struct SystemBuilder {}

impl SystemBuilder {
    /// Create a new system builder.
    pub fn new() -> Self {
        SystemBuilder {}
    }

    /// Build the actor system.
    pub fn build(self) -> System {
        System {
            root_cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SystemError {}

pub struct System {
    root_cancellation: tokio_util::sync::CancellationToken,
}

impl System {
    pub fn spawn<A: Actor + Default>(
        &self,
    ) -> Result<(Supervisor<A, Initialized<A>>, Endpoint<A>), SystemError> {
        let actor: A = Default::default();
        self.spawn_with(actor)
    }

    pub fn spawn_with<A: Actor>(
        &self,
        actor: A,
    ) -> Result<(Supervisor<A, Initialized<A>>, Endpoint<A>), SystemError> {
        let supervisor = Supervisor::construct(actor);
        let supervisor = supervisor.start_within(self);
        let endpoint = supervisor.endpoint();
        Ok((supervisor, endpoint))
    }
}
