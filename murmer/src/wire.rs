//! Wire types for remote message dispatch.
//!
//! These types define the protocol for sending messages between nodes:
//!
//! - [`RemoteInvocation`] — a serialized message heading to a remote actor
//! - [`RemoteResponse`] — the serialized response coming back
//! - [`DispatchRequest`] — internal routing from the wire layer to a local supervisor
//! - [`SendError`] — errors that can occur during message delivery
//! - [`ResponseRegistry`] — correlates in-flight call IDs to response channels
//!
//! # Wire protocol
//!
//! Messages are serialized with bincode. Each invocation carries a `call_id` for
//! response correlation, the target `actor_label`, and a `message_type` string
//! (the `RemoteMessage::TYPE_ID`) so the receiver knows which type to deserialize into.
//!
//! # Envelope pattern (local dispatch)
//!
//! For local actors, the [`EnvelopeProxy`] trait provides zero-cost type-erased
//! dispatch. A [`TypedEnvelope<M>`] wraps the concrete message and a oneshot
//! channel for the response, then erases the message type behind `dyn EnvelopeProxy<A>`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::actor::{Actor, ActorContext, AsyncHandler, Handler, Message};

// =============================================================================
// WIRE TYPES
// =============================================================================

/// What crosses the wire — analogous to Swift's InvocationMessage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteInvocation {
    pub call_id: u64,
    pub actor_label: String,
    pub message_type: String,
    pub payload: Vec<u8>,
}

/// Response from the remote side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteResponse {
    pub call_id: u64,
    pub result: Result<Vec<u8>, String>,
}

/// Internal: routes an incoming remote invocation to a local actor's supervisor.
pub struct DispatchRequest {
    pub invocation: RemoteInvocation,
    pub respond_to: oneshot::Sender<RemoteResponse>,
}

// =============================================================================
// SEND ERROR
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("actor mailbox closed")]
    MailboxClosed,
    #[error("response channel dropped")]
    ResponseDropped,
    #[error("wire connection closed")]
    WireClosed,
    #[error("serialization failed: {0}")]
    SerializationFailed(String),
    #[error("deserialization failed: {0}")]
    DeserializationFailed(String),
    #[error("remote error: {0}")]
    RemoteError(String),
}

// =============================================================================
// REPLY SENDER — shared, consumable reply channel
// =============================================================================

/// A shared, consumable reply channel for reply control.
///
/// Wraps a `oneshot::Sender` in `Arc<Mutex<Option<...>>>` so both the
/// envelope (auto-reply after handler returns) and the handler (via
/// [`ActorContext::reply`] or [`ActorContext::reply_sender`]) can access it.
/// Taking from the `Option` ensures exactly-once delivery.
pub struct ReplySender<R: Send + 'static> {
    inner: Arc<Mutex<Option<oneshot::Sender<R>>>>,
}

impl<R: Send + 'static> Clone for ReplySender<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: Send + 'static> ReplySender<R> {
    /// Create a new ReplySender from a oneshot::Sender.
    pub fn new(sender: oneshot::Sender<R>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(sender))),
        }
    }

    /// Send a reply, consuming the channel. Returns true if the reply was sent.
    /// Returns false if the channel was already consumed or the receiver dropped.
    pub fn send(&self, value: R) -> bool {
        if let Some(sender) = self.inner.lock().unwrap().take() {
            sender.send(value).is_ok()
        } else {
            false
        }
    }

    /// Check if the reply has already been sent.
    pub fn is_consumed(&self) -> bool {
        self.inner.lock().unwrap().is_none()
    }
}

impl<R: Send + 'static> Drop for ReplySender<R> {
    fn drop(&mut self) {
        // Only warn if this is the last Arc reference and the sender wasn't consumed.
        // This detects forgotten replies from reply_sender().
        if Arc::strong_count(&self.inner) == 1 && !self.is_consumed() {
            tracing::warn!("ReplySender dropped without sending a reply (forgotten reply?)");
        }
    }
}

// =============================================================================
// ENVELOPE — type-erased local message dispatch
// =============================================================================

/// Type-erased message that can be dispatched to an actor.
///
/// Returns a pinned future so that both sync and async handlers can be
/// dispatched through the same trait-object interface.
pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle<'a>(
        self: Box<Self>,
        ctx: &'a ActorContext<A>,
        actor: &'a mut A,
        state: &'a mut A::State,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Envelope for sync [`Handler`] messages.
pub(crate) struct TypedEnvelope<M: Message> {
    pub(crate) message: M,
    pub(crate) respond_to: oneshot::Sender<M::Result>,
}

impl<A, M> EnvelopeProxy<A> for TypedEnvelope<M>
where
    A: Handler<M>,
    M: Message,
{
    fn handle<'a>(
        self: Box<Self>,
        ctx: &'a ActorContext<A>,
        actor: &'a mut A,
        state: &'a mut A::State,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let reply = ReplySender::new(self.respond_to);

            // Inject the reply token so ctx.reply() / ctx.forward() can use it
            *ctx.reply_token.lock().unwrap() = Some(Box::new(reply.clone()));

            let result = actor.handle(ctx, state, self.message);

            // Check if the handler claimed the reply (via reply/reply_sender/forward).
            // If the token was taken from the slot, the handler owns the reply channel.
            let token_was_claimed = ctx.reply_token.lock().unwrap().take().is_none();

            if !token_was_claimed {
                // Nobody touched the token — auto-reply with the return value
                reply.send(result);
            }
        })
    }
}

/// Envelope for async [`AsyncHandler`] messages.
pub(crate) struct AsyncTypedEnvelope<M: Message> {
    pub(crate) message: M,
    pub(crate) respond_to: oneshot::Sender<M::Result>,
}

impl<A, M> EnvelopeProxy<A> for AsyncTypedEnvelope<M>
where
    A: AsyncHandler<M>,
    M: Message,
{
    fn handle<'a>(
        self: Box<Self>,
        ctx: &'a ActorContext<A>,
        actor: &'a mut A,
        state: &'a mut A::State,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let reply = ReplySender::new(self.respond_to);

            // Inject the reply token so ctx.reply() / ctx.forward() can use it
            *ctx.reply_token.lock().unwrap() = Some(Box::new(reply.clone()));

            let result = actor.handle(ctx, state, self.message).await;

            // Check if the handler claimed the reply
            let token_was_claimed = ctx.reply_token.lock().unwrap().take().is_none();

            if !token_was_claimed {
                reply.send(result);
            }
        })
    }
}

/// Envelope for forwarded messages (via `ctx.forward()`).
/// Uses a `ReplySender` instead of a raw `oneshot::Sender`.
pub(crate) struct ForwardedEnvelope<M: Message> {
    pub(crate) message: M,
    pub(crate) reply_sender: ReplySender<M::Result>,
}

impl<A, M> EnvelopeProxy<A> for ForwardedEnvelope<M>
where
    A: Handler<M>,
    M: Message,
{
    fn handle<'a>(
        self: Box<Self>,
        ctx: &'a ActorContext<A>,
        actor: &'a mut A,
        state: &'a mut A::State,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Same reply-token injection as TypedEnvelope
            *ctx.reply_token.lock().unwrap() = Some(Box::new(self.reply_sender.clone()));

            let result = actor.handle(ctx, state, self.message);

            let token_was_claimed = ctx.reply_token.lock().unwrap().take().is_none();

            if !token_was_claimed {
                self.reply_sender.send(result);
            }
        })
    }
}

// =============================================================================
// RESPONSE REGISTRY — correlates wire responses to in-flight calls
// =============================================================================

#[derive(Clone)]
pub struct ResponseRegistry {
    inner: Arc<Mutex<HashMap<u64, oneshot::Sender<RemoteResponse>>>>,
}

impl ResponseRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, call_id: u64, tx: oneshot::Sender<RemoteResponse>) {
        self.inner.lock().unwrap().insert(call_id, tx);
    }

    pub fn complete(&self, response: RemoteResponse) {
        if let Some(tx) = self.inner.lock().unwrap().remove(&response.call_id) {
            let _ = tx.send(response);
        }
    }

    /// Fail all pending responses — called when the connection is lost.
    /// Each pending caller receives an error response so they surface the
    /// failure immediately instead of waiting forever.
    pub fn fail_all(&self, error_msg: &str) {
        let mut map = self.inner.lock().unwrap();
        for (call_id, tx) in map.drain() {
            let _ = tx.send(RemoteResponse {
                call_id,
                result: Err(error_msg.to_string()),
            });
        }
    }
}

impl Default for ResponseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// UTILITIES
// =============================================================================

pub(crate) fn next_call_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
