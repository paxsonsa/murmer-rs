mod macros;
mod sender;
#[cfg(test)]
mod examples;

// Re-export the public parts of the sender module
// Note that implementations actually use these types, even if the linter doesn't detect it
#[allow(unused_imports)]
pub use sender::{RemoteEndpointSender, TypedRemoteEndpointSender, create_typed_endpoint_sender};

// We don't need to export the macros explicitly since they're #[macro_export]
use std::any::Any;

use bytes::Bytes;
use crate::actor::{Actor, Registered as RegisteredActor};
use crate::message::Message;
use crate::path::ActorPath;
use crate::system::AnyEndpoint;
use serde::{Deserialize, Serialize};

/// Message type for communicating with RemoteProxy
#[derive(Debug)]
pub enum RemoteProxyMessage {
    /// Send a message to a remote actor
    SendMessage {
        /// The message to send
        message: crate::message::RemoteMessage,
        /// Response channel
        response_tx: tokio::sync::oneshot::Sender<Result<crate::message::RemoteMessage, crate::message::RemoteMessageError>>,
    },
}

/// Registration for actor types in the inventory system
pub struct ActorTypeRegistration {
    /// String identifier for this actor type
    pub key: &'static str,
    /// Type name for debugging and error messages
    pub type_name: &'static str,
    /// Factory function to create remote endpoints for this actor type
    /// 
    /// The function takes:
    /// - The actor path
    /// - A channel to communicate with the RemoteProxy
    pub create_endpoint: fn(ActorPath, tokio::sync::mpsc::Sender<RemoteProxyMessage>) -> AnyEndpoint,
}

impl ActorTypeRegistration {
    pub const fn new(
        key: &'static str,
        type_name: &'static str,
        create_endpoint: fn(ActorPath, tokio::sync::mpsc::Sender<RemoteProxyMessage>) -> AnyEndpoint,
    ) -> Self {
        Self {
            key,
            type_name,
            create_endpoint,
        }
    }
}

// Register the ActorTypeRegistration with inventory
inventory::collect!(ActorTypeRegistration);

/// Registration for message types that can be sent to remote actors
pub struct MessageTypeRegistration {
    /// Actor type this message applies to
    pub actor_type: &'static str,
    /// Message type identifier
    pub message_type: &'static str,
    /// Function to serialize message, send to remote, and deserialize response
    pub handler: fn(Bytes) -> Result<Bytes, String>,
}

impl MessageTypeRegistration {
    pub const fn new(
        actor_type: &'static str,
        message_type: &'static str,
        handler: fn(Bytes) -> Result<Bytes, String>,
    ) -> Self {
        Self {
            actor_type,
            message_type,
            handler,
        }
    }
}

// Register MessageTypeRegistration with inventory
inventory::collect!(MessageTypeRegistration);

/// Trait for messages that can be sent remotely
pub trait RemoteMessageType: Message + Serialize + for<'de> Deserialize<'de> + bincode::Encode + bincode::Decode<()> {
    /// Unique identifier for this message type
    const TYPE_ID: &'static str;
}

/// Remote actor proxy factory trait
pub trait RemoteActorFactory {
    /// Creates a remote actor proxy from an actor path
    fn create_remote_proxy(path: ActorPath) -> AnyEndpoint;
}

/// Marker trait for actors that can create remote proxies
pub trait RemoteProxy: Actor {
    /// Creates a new remote proxy for this actor type
    fn new_remote(path: ActorPath) -> Box<dyn Any + Send + Sync>;
}

/// Extended registration trait for actors that can be accessed remotely
pub trait RemoteRegistered: RegisteredActor {
    /// Creates a properly typed endpoint for a remote actor of this type
    fn create_remote_endpoint(
        path: ActorPath, 
        proxy_tx: tokio::sync::mpsc::Sender<RemoteProxyMessage>
    ) -> AnyEndpoint;

    /// Returns handlers for all message types supported by this actor remotely
    fn message_handlers() -> Vec<(&'static str, fn(Bytes) -> Result<Bytes, String>)>;
}