use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;

use crate::actor::{Actor, Handler, Context};

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