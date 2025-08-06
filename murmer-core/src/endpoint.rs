use tokio::sync::oneshot;

use crate::actor::{Actor, Handler};
use crate::message::{Envelope, Message};

#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    /// Returned when trying to send a message to an actor whose mailbox has been closed
    #[error("Actor mailbox has been closed")]
    MailboxClosed,

    /// Returned when the response channel was dropped before receiving the result
    #[error("Actor response was dropped unexpectedly")]
    ResponseDropped,
}

#[derive(Debug)]
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


impl <A: Actor> Clone for Endpoint<A> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}
