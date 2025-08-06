use crate::actor::Actor;
use crate::endpoint::EndpointError;
use crate::message::Message;
use super::registry::{ActorKey, ActorLabel, AnyEndpoint};

/// Message to register an actor with the receptionist.
#[derive(Debug)]
pub struct Register {
    pub endpoint: AnyEndpoint,
    pub key: ActorKey,
    pub label: ActorLabel,
}

impl Register {
    /// Create a new registration message.
    pub fn new<A: Actor>(endpoint: crate::endpoint::Endpoint<A>, label: impl Into<ActorLabel>) -> Self {
        Self {
            endpoint: AnyEndpoint::new(endpoint),
            key: ActorKey(A::ACTOR_TYPE_KEY),
            label: label.into(),
        }
    }
}

impl Message for Register {
    type Result = Result<(), EndpointError>;
}

/// Message to look up an actor by type and label.
#[derive(Debug)]
pub struct Lookup {
    pub key: ActorKey,
    pub label: ActorLabel,
}

impl Lookup {
    /// Create a new lookup message.
    pub fn new<A: Actor>(label: impl Into<ActorLabel>) -> Self {
        Self {
            key: ActorKey(A::ACTOR_TYPE_KEY),
            label: label.into(),
        }
    }

    /// Create a lookup message with an explicit actor key.
    pub fn with_key(key: ActorKey, label: impl Into<ActorLabel>) -> Self {
        Self {
            key,
            label: label.into(),
        }
    }
}

impl Message for Lookup {
    type Result = Option<AnyEndpoint>;
}