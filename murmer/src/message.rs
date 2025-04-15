//! Message traits and envelope types for actor communication.

use std::future::Future;
use std::pin::Pin;

use super::actor::*;
use bytes::Bytes;
use dyn_clone::DynClone;
use tokio::sync::oneshot;

use crate::context::Context;

/// Errors that can occur during actor operations
#[derive(thiserror::Error, Debug)]
pub enum SendError {
    /// Returned when trying to send a message to an actor whose mailbox has been closed
    #[error("Actor mailbox has been closed")]
    MailboxClosed,
    /// Returned when the response channel was dropped before receiving the result
    #[error("Actor response was dropped unexpectedly")]
    ResponseDropped,
}

/// A trait for representing a Sender of some message type.
#[async_trait::async_trait]
pub trait MessageSender<M>: DynClone + Send + Sync
where
    M: Message + Send,
    M::Result: Send,
{
    /// Sends a message to the actor.
    async fn send(&self, msg: M) -> Result<M::Result, SendError>;

    /// Sends a message to the actor without expecting a response.
    async fn send_no_response(&self, msg: M) -> Result<(), SendError>;
}

dyn_clone::clone_trait_object!(<M> MessageSender<M> where M: Message + Send, M::Result: Send);

/// A trait for messages that can be sent to actors.
///
/// Messages must define their result type through the associated Result type.
/// Both the message and its result must be Send + 'static to support
/// asynchronous processing across thread boundaries.
pub trait Message: std::fmt::Debug + Send + 'static {
    /// The type of result that will be returned when this message is handled
    type Result: Send + 'static;
}

pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle_async<'a>(
        &'a mut self,
        ctx: &'a mut Context<A>,
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

                    let result = actor.handle(ctx, msg).await;

                    let _ = tx.send(result);
                }
                ResponseExpectation::NotExpected => {
                    let _ = actor.handle(ctx, msg).await;
                }
            }
        })
    }
}

/// A special remote message that is used to send messages to remote actors
/// through the networking layer.
#[derive(Debug)]
pub struct RemoteMessage {
    pub type_name: String,
    pub message_data: Bytes,
}

impl Message for RemoteMessage {
    type Result = ();
}

pub struct RecepientOf<M: Message> {
    pub sender: Box<dyn MessageSender<M>>,
}

impl<M: Message> RecepientOf<M> {
    pub fn new(sender: Box<dyn MessageSender<M>>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, msg: M) -> Result<M::Result, SendError> {
        self.sender.send(msg).await
    }

    pub async fn send_no_response(&self, msg: M) -> Result<(), SendError> {
        self.sender.send_no_response(msg).await
    }
}

impl<M: Message> std::fmt::Debug for RecepientOf<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecepientOf")
            .field("sender", &std::any::type_name::<M>())
            .finish()
    }
}
