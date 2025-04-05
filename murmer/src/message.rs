//! Message traits and envelope types for actor communication.

use crate::context::Context;

use super::actor::*;
use tokio::sync::oneshot;

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
    fn handle(&mut self, ctx: &mut Context<A>, actor: &mut A);
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
        Envelope(Box::new(SyncEnvelope {
            msg: Some(msg),
            response_expectation: ResponseExpectation::Expected(Some(tx)),
        }))
    }

    pub fn no_response<M>(msg: M) -> Self
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        Envelope(Box::new(SyncEnvelope {
            msg: Some(msg),
            response_expectation: ResponseExpectation::NotExpected,
        }))
    }
}

struct SyncEnvelope<M: Message> {
    msg: Option<M>,
    response_expectation: ResponseExpectation<M>,
}

impl<A, M> EnvelopeProxy<A> for SyncEnvelope<M>
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
    fn handle(&mut self, ctx: &mut Context<A>, actor: &mut A) {
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

                let result = actor.handle(ctx, msg);

                let _ = tx.send(result);
            }
            ResponseExpectation::NotExpected => {
                let _ = actor.handle(ctx, msg);
            }
        }
    }
}
