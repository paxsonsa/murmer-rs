//! Message traits and envelope types for actor communication.

use super::actor::*;
use tokio::sync::oneshot;

/// A trait for messages that can be sent to actors.
///
/// Messages must define their result type through the associated Result type.
/// Both the message and its result must be Send + 'static to support
/// asynchronous processing across thread boundaries.
pub trait Message: Send + 'static {
    /// The type of result that will be returned when this message is handled
    type Result: Send + 'static;
}

pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle(&mut self, ctx: &mut Context, actor: &mut A);
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
            tx: Some(tx),
        }))
    }
}

struct SyncEnvelope<M: Message> {
    msg: Option<M>,
    tx: Option<oneshot::Sender<M::Result>>,
}

impl<A, M> EnvelopeProxy<A> for SyncEnvelope<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn handle(&mut self, ctx: &mut Context, actor: &mut A) {
        let Some(tx) = self.tx.take() else {
            return;
        };

        if tx.is_closed() {
            return;
        }

        let Some(msg) = self.msg.take() else {
            return;
        };

        let result = actor.handle(ctx, msg);
        let _ = tx.send(result);
    }
}
