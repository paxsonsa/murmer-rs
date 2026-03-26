//! Endpoint — the user-facing send handle that abstracts local vs remote actors.
//!
//! [`Endpoint<A>`] is the primary API for sending messages to actors. It is
//! generic over the actor type `A`, which provides compile-time guarantees
//! that the actor can handle the message being sent.
//!
//! # Location transparency
//!
//! Under the hood, an endpoint is either:
//! - **Local** — wraps an `mpsc` channel to the actor's supervisor (zero serialization cost)
//! - **Remote** — serializes the message with bincode, sends it over a QUIC stream,
//!   and awaits the deserialized response
//!
//! The caller never knows which variant they hold — the `send()` API is identical.
//!
//! # Obtaining endpoints
//!
//! - `receptionist.start(label, actor, state)` returns an `Endpoint<A>` for the new actor
//! - `receptionist.lookup::<A>(label)` returns `Option<Endpoint<A>>`
//! - `ctx.endpoint()` inside a handler returns a self-endpoint
//! - `listing.next().await` yields endpoints from a subscription stream

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, AsyncHandler, Handler, Message, RemoteMessage};
use crate::instrument;
use crate::wire::{
    AsyncTypedEnvelope, EnvelopeProxy, ForwardedEnvelope, RemoteInvocation, ReplySender,
    ResponseRegistry, SendError, TypedEnvelope, next_call_id,
};

// =============================================================================
// ENDPOINT
// =============================================================================

/// The primary handle for interacting with an actor.
///
/// `Endpoint<A>` is generic over actor type `A`, providing compile-time
/// guarantees that the actor can handle the message being sent. Endpoints
/// are cheap to clone and can be shared across tasks.
///
/// Under the hood, an endpoint is either local (in-memory channel) or remote
/// (serializes over QUIC). The caller never knows which — the API is identical.
///
/// # Examples
///
/// ```rust,ignore
/// // Obtain from system.start() or system.lookup()
/// let counter = system.start("counter/0", Counter, CounterState { count: 0 });
///
/// // Send a sync message
/// let count = counter.send(Increment { amount: 5 }).await?;
///
/// // Send an async message
/// let data = counter.send_async(FetchData { key: "x".into() }).await?;
///
/// // Clone and share across tasks
/// let counter2 = counter.clone();
/// tokio::spawn(async move {
///     counter2.send(Increment { amount: 1 }).await.ok();
/// });
/// ```
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
    /// - `A` must implement `Handler<M>`
    /// - `M` must implement `RemoteMessage` (serializable)
    ///
    /// For local actors: uses the envelope pattern (zero serialization cost).
    /// For remote actors: serializes, sends over wire, awaits response.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let count = counter.send(Increment { amount: 5 }).await?;
    ///
    /// // Error handling
    /// match counter.send(Increment { amount: 1 }).await {
    ///     Ok(new_count) => println!("Count: {new_count}"),
    ///     Err(SendError::MailboxClosed) => println!("Actor stopped"),
    ///     Err(e) => println!("Send failed: {e}"),
    /// }
    /// ```
    pub async fn send<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage,
        M::Result: Serialize + DeserializeOwned,
    {
        let actor_type = std::any::type_name::<A>();
        match &self.inner {
            EndpointInner::Local { mailbox_tx } => {
                instrument::send_local(actor_type);
                let (tx, rx) = oneshot::channel();
                let envelope = TypedEnvelope {
                    message,
                    respond_to: tx,
                };
                mailbox_tx.send(Box::new(envelope)).map_err(|_| {
                    instrument::send_error(actor_type, "mailbox_closed");
                    SendError::MailboxClosed
                })?;
                rx.await.map_err(|_| {
                    instrument::send_error(actor_type, "response_dropped");
                    SendError::ResponseDropped
                })
            }
            EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            } => {
                instrument::send_remote(actor_type);
                #[cfg(feature = "monitor")]
                let start = std::time::Instant::now();

                let call_id = next_call_id();
                let payload = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                    .map_err(|e| {
                        instrument::send_error(actor_type, "serialization_failed");
                        SendError::SerializationFailed(e.to_string())
                    })?;

                let (resp_tx, resp_rx) = oneshot::channel();
                response_registry.register(call_id, resp_tx);

                let invocation = RemoteInvocation {
                    call_id,
                    actor_label: label.clone(),
                    message_type: M::TYPE_ID.to_string(),
                    payload,
                };

                wire_tx.send(invocation).map_err(|_| {
                    instrument::send_error(actor_type, "wire_closed");
                    SendError::WireClosed
                })?;

                let response = resp_rx.await.map_err(|_| {
                    instrument::send_error(actor_type, "response_dropped");
                    SendError::ResponseDropped
                })?;
                let result_bytes = response.result.map_err(|e| {
                    instrument::send_error(actor_type, "remote_error");
                    SendError::RemoteError(e)
                })?;
                let (result, _): (M::Result, _) =
                    bincode::serde::decode_from_slice(&result_bytes, bincode::config::standard())
                        .map_err(|e| {
                        instrument::send_error(actor_type, "deserialization_failed");
                        SendError::DeserializationFailed(e.to_string())
                    })?;

                #[cfg(feature = "monitor")]
                instrument::remote_roundtrip_duration(actor_type, start.elapsed());

                Ok(result)
            }
        }
    }

    /// Send a message to an async handler. **Identical API for local and remote.**
    ///
    /// Like [`send`](Self::send) but dispatches through [`AsyncHandler<M>`] so the
    /// handler can `.await` inside its body.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // For actors that implement AsyncHandler<FetchData>
    /// let data = endpoint.send_async(FetchData { key: "users".into() }).await?;
    /// ```
    pub async fn send_async<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        A: AsyncHandler<M>,
        M: RemoteMessage,
        M::Result: Serialize + DeserializeOwned,
    {
        let actor_type = std::any::type_name::<A>();
        match &self.inner {
            EndpointInner::Local { mailbox_tx } => {
                instrument::send_local(actor_type);
                let (tx, rx) = oneshot::channel();
                let envelope = AsyncTypedEnvelope {
                    message,
                    respond_to: tx,
                };
                mailbox_tx.send(Box::new(envelope)).map_err(|_| {
                    instrument::send_error(actor_type, "mailbox_closed");
                    SendError::MailboxClosed
                })?;
                rx.await.map_err(|_| {
                    instrument::send_error(actor_type, "response_dropped");
                    SendError::ResponseDropped
                })
            }
            EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            } => {
                instrument::send_remote(actor_type);
                #[cfg(feature = "monitor")]
                let start = std::time::Instant::now();

                // Remote path is identical — serialization doesn't care about sync/async
                let call_id = next_call_id();
                let payload = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                    .map_err(|e| {
                        instrument::send_error(actor_type, "serialization_failed");
                        SendError::SerializationFailed(e.to_string())
                    })?;

                let (resp_tx, resp_rx) = oneshot::channel();
                response_registry.register(call_id, resp_tx);

                let invocation = RemoteInvocation {
                    call_id,
                    actor_label: label.clone(),
                    message_type: M::TYPE_ID.to_string(),
                    payload,
                };

                wire_tx.send(invocation).map_err(|_| {
                    instrument::send_error(actor_type, "wire_closed");
                    SendError::WireClosed
                })?;

                let response = resp_rx.await.map_err(|_| {
                    instrument::send_error(actor_type, "response_dropped");
                    SendError::ResponseDropped
                })?;
                let result_bytes = response.result.map_err(|e| {
                    instrument::send_error(actor_type, "remote_error");
                    SendError::RemoteError(e)
                })?;
                let (result, _): (M::Result, _) =
                    bincode::serde::decode_from_slice(&result_bytes, bincode::config::standard())
                        .map_err(|e| {
                        instrument::send_error(actor_type, "deserialization_failed");
                        SendError::DeserializationFailed(e.to_string())
                    })?;

                #[cfg(feature = "monitor")]
                instrument::remote_roundtrip_duration(actor_type, start.elapsed());

                Ok(result)
            }
        }
    }

    /// Internal: send a message using an existing `ReplySender` (for `ctx.forward()`).
    /// Returns true if the envelope was queued successfully.
    pub(crate) fn send_with_reply_sender<M>(
        &self,
        message: M,
        reply_sender: ReplySender<M::Result>,
    ) -> bool
    where
        A: Handler<M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        match &self.inner {
            EndpointInner::Local { mailbox_tx } => {
                let envelope = ForwardedEnvelope {
                    message,
                    reply_sender,
                };
                mailbox_tx.send(Box::new(envelope)).is_ok()
            }
            EndpointInner::Remote { .. } => {
                tracing::warn!("ctx.forward() to remote endpoint is not yet supported");
                false
            }
        }
    }
}
