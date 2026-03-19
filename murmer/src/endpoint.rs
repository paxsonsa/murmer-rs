//! Endpoint — the user-facing send handle (abstracts local vs remote).

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, Handler, RemoteMessage};
use crate::wire::{
    EnvelopeProxy, RemoteInvocation, ResponseRegistry, SendError, TypedEnvelope, next_call_id,
};

// =============================================================================
// ENDPOINT
// =============================================================================

pub struct Endpoint<A: Actor> {
    inner: EndpointInner<A>,
}

impl<A: Actor> Clone for Endpoint<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

enum EndpointInner<A: Actor> {
    Local {
        mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
    },
    Remote {
        label: String,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    },
}

impl<A: Actor> Clone for EndpointInner<A> {
    fn clone(&self) -> Self {
        match self {
            Self::Local { mailbox_tx } => Self::Local {
                mailbox_tx: mailbox_tx.clone(),
            },
            Self::Remote {
                label,
                wire_tx,
                response_registry,
            } => Self::Remote {
                label: label.clone(),
                wire_tx: wire_tx.clone(),
                response_registry: response_registry.clone(),
            },
        }
    }
}

impl<A: Actor + 'static> Endpoint<A> {
    pub(crate) fn local(mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>) -> Self {
        Self {
            inner: EndpointInner::Local { mailbox_tx },
        }
    }

    pub(crate) fn remote(
        label: String,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    ) -> Self {
        Self {
            inner: EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            },
        }
    }

    /// Send a message to the actor. **Identical API for local and remote.**
    ///
    /// Compile-time checked:
    /// - A must implement Handler<M>
    /// - M must implement RemoteMessage (serializable)
    ///
    /// For local actors: uses the envelope pattern (zero serialization cost).
    /// For remote actors: serializes, sends over wire, awaits response.
    pub async fn send<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage,
        M::Result: Serialize + DeserializeOwned,
    {
        match &self.inner {
            EndpointInner::Local { mailbox_tx } => {
                let (tx, rx) = oneshot::channel();
                let envelope = TypedEnvelope {
                    message,
                    respond_to: tx,
                };
                mailbox_tx
                    .send(Box::new(envelope))
                    .map_err(|_| SendError::MailboxClosed)?;
                rx.await.map_err(|_| SendError::ResponseDropped)
            }
            EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            } => {
                let call_id = next_call_id();
                let payload = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                    .map_err(|e| SendError::SerializationFailed(e.to_string()))?;

                let (resp_tx, resp_rx) = oneshot::channel();
                response_registry.register(call_id, resp_tx);

                let invocation = RemoteInvocation {
                    call_id,
                    actor_label: label.clone(),
                    message_type: M::TYPE_ID.to_string(),
                    payload,
                };

                wire_tx
                    .send(invocation)
                    .map_err(|_| SendError::WireClosed)?;

                let response = resp_rx.await.map_err(|_| SendError::ResponseDropped)?;
                let result_bytes = response.result.map_err(SendError::RemoteError)?;
                let (result, _): (M::Result, _) =
                    bincode::serde::decode_from_slice(&result_bytes, bincode::config::standard())
                        .map_err(|e| SendError::DeserializationFailed(e.to_string()))?;
                Ok(result)
            }
        }
    }
}
