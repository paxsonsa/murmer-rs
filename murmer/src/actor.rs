//! Core actor traits and types.

use std::fmt::Debug;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::endpoint::Endpoint;
use crate::lifecycle::{ActorTerminated, SystemSignal};
use crate::receptionist::Receptionist;
use crate::wire::EnvelopeProxy;

// =============================================================================
// CORE TRAITS
// =============================================================================

/// A message that can be sent to an actor. Defines its response type.
pub trait Message: Debug + Send + 'static {
    type Result: Send + 'static;
}

/// A message that can cross the wire. Requires serialization for both
/// the message and its result. TYPE_ID is the dispatch key on the receiver.
pub trait RemoteMessage: Message + Serialize + DeserializeOwned
where
    Self::Result: Serialize + DeserializeOwned,
{
    const TYPE_ID: &'static str;
}

/// An actor — a stateful message processor.
pub trait Actor: Send + 'static {
    type State: Send + 'static;

    /// Called when a watched actor terminates. Override to react to failures.
    fn on_actor_terminated(&mut self, _state: &mut Self::State, _terminated: &ActorTerminated) {
        // default: no-op — actors opt in by overriding
    }
}

/// Intrinsic context available to every actor handler invocation.
/// Provides the actor's identity, a self-endpoint, and receptionist access.
pub struct ActorContext<A: Actor> {
    pub(crate) label: String,
    pub(crate) node_id: String,
    pub(crate) mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
    pub(crate) receptionist: Receptionist,
    pub(crate) system_tx: mpsc::UnboundedSender<SystemSignal>,
}

impl<A: Actor + 'static> ActorContext<A> {
    /// This actor's label in the receptionist.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The node ID this actor is running on.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get an endpoint to this actor (useful for self-sends or passing identity).
    pub fn endpoint(&self) -> Endpoint<A> {
        Endpoint::local(self.mailbox_tx.clone())
    }

    /// Access the receptionist for lookups or subscriptions.
    pub fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    /// Watch another actor for termination. Erlang-style one-shot monitor.
    /// If the watched actor doesn't exist, fires immediately.
    pub fn watch(&self, label: &str) {
        self.receptionist
            .add_watch(label, &self.label, self.system_tx.clone());
    }

    /// Get a serializable reference to this actor.
    /// The returned `ActorRef<A>` can be embedded in messages and sent to other actors/nodes.
    pub fn actor_ref(&self) -> ActorRef<A> {
        ActorRef {
            label: self.label.clone(),
            node_id: self.node_id.clone(),
            _phantom: PhantomData,
        }
    }
}

// =============================================================================
// ACTOR REF — serializable distributed actor identity
// =============================================================================

/// A serializable, typed reference to an actor. Can be embedded in messages
/// and sent across the wire to other nodes. Resolve it via a receptionist
/// to get a live `Endpoint<A>`.
#[derive(Debug)]
pub struct ActorRef<A: Actor> {
    pub label: String,
    pub node_id: String,
    _phantom: PhantomData<A>,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            label: self.label.clone(),
            node_id: self.node_id.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Wire format for ActorRef serialization (no PhantomData).
#[derive(Serialize, Deserialize)]
struct ActorRefWire {
    label: String,
    node_id: String,
}

impl<A: Actor> Serialize for ActorRef<A> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ActorRefWire {
            label: self.label.clone(),
            node_id: self.node_id.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de, A: Actor> Deserialize<'de> for ActorRef<A> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ActorRefWire::deserialize(deserializer)?;
        Ok(ActorRef {
            label: wire.label,
            node_id: wire.node_id,
            _phantom: PhantomData,
        })
    }
}

impl<A: Actor + 'static> ActorRef<A> {
    /// Create a new ActorRef with the given label and node ID.
    pub fn new(label: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            node_id: node_id.into(),
            _phantom: PhantomData,
        }
    }

    /// Resolve this reference to a live Endpoint via the receptionist.
    /// Returns `None` if the actor is not registered or has a different type.
    pub fn resolve(&self, receptionist: &Receptionist) -> Option<Endpoint<A>> {
        receptionist.lookup::<A>(&self.label)
    }
}

/// Declares that actor A can handle message M.
pub trait Handler<M: Message>: Actor + Sized {
    fn handle(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut Self::State,
        message: M,
    ) -> M::Result;
}

/// Generated by proc macro: dispatches a remote message by type tag.
/// Each match arm deserializes to the concrete type, calls the handler,
/// and serializes the result. This is the Rust equivalent of Swift's
/// compiler-generated accessor thunks.
pub trait RemoteDispatch: Actor + Sized {
    fn dispatch_remote(
        &mut self,
        ctx: &ActorContext<Self>,
        state: &mut Self::State,
        message_type: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, DispatchError>;
}

#[derive(Debug)]
pub enum DispatchError {
    UnknownMessageType(String),
    DeserializeFailed(String),
    SerializeFailed(String),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownMessageType(t) => write!(f, "unknown message type: {t}"),
            Self::DeserializeFailed(e) => write!(f, "deserialize failed: {e}"),
            Self::SerializeFailed(e) => write!(f, "serialize failed: {e}"),
        }
    }
}

impl std::error::Error for DispatchError {}
