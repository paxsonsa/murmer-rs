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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::actor::{Actor, ActorContext, Handler, Message};

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

#[derive(Debug)]
pub enum SendError {
    MailboxClosed,
    ResponseDropped,
    WireClosed,
    SerializationFailed(String),
    DeserializationFailed(String),
    RemoteError(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for SendError {}

// =============================================================================
// ENVELOPE — type-erased local message dispatch
// =============================================================================

/// Type-erased message that can be dispatched to an actor.
pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle(self: Box<Self>, ctx: &ActorContext<A>, actor: &mut A, state: &mut A::State);
}

pub(crate) struct TypedEnvelope<M: Message> {
    pub(crate) message: M,
    pub(crate) respond_to: oneshot::Sender<M::Result>,
}

impl<A, M> EnvelopeProxy<A> for TypedEnvelope<M>
where
    A: Handler<M>,
    M: Message,
{
    fn handle(self: Box<Self>, ctx: &ActorContext<A>, actor: &mut A, state: &mut A::State) {
        let result = actor.handle(ctx, state, self.message);
        let _ = self.respond_to.send(result);
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
