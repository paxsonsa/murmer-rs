//! RemoteEndpointSender implementation for remote actor communication
//!
//! This module provides the implementation of the RemoteEndpointSender, which is responsible for
//! sending messages to remote actors and receiving responses.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::message::{MessageSender, RemoteMessage, SendError};
use crate::path::ActorPath;
use crate::remote::{RemoteMessageType, RemoteProxyMessage};
// Use what we need from system module

/// A specialized EndpointSender for remote actors
///
/// This sender communicates with a RemoteProxy through a channel, sending serialized messages
/// and receiving responses asynchronously.
pub struct RemoteEndpointSender {
    /// Channel to communicate with the RemoteProxy
    proxy_tx: mpsc::Sender<RemoteProxyMessage>,
    /// Path to the remote actor
    path: Arc<ActorPath>,
    /// Type identifier for the actor
    actor_type: String,
}

impl RemoteEndpointSender {
    /// Creates a new RemoteEndpointSender
    pub fn new(
        proxy_tx: mpsc::Sender<RemoteProxyMessage>,
        path: ActorPath,
        actor_type: String,
    ) -> Self {
        Self {
            proxy_tx,
            path: Arc::new(path),
            actor_type,
        }
    }

    /// Sends a RemoteMessage to the remote actor and waits for a response
    pub async fn send_remote(&self, message: RemoteMessage) -> Result<RemoteMessage, SendError> {
        // Create a oneshot channel for the response
        let (response_tx, response_rx) = oneshot::channel();

        // Send the message to the RemoteProxy
        if let Err(_) = self.proxy_tx.send(RemoteProxyMessage::SendMessage {
            message,
            response_tx,
        }).await {
            return Err(SendError::MailboxClosed);
        }

        // Wait for the response with a timeout
        match timeout(Duration::from_secs(30), response_rx).await {
            Ok(result) => match result {
                Ok(response) => match response {
                    Ok(response_msg) => Ok(response_msg),
                    Err(_) => Err(SendError::MailboxClosed),
                },
                Err(_) => Err(SendError::ResponseDropped),
            },
            Err(_) => Err(SendError::ResponseDropped),
        }
    }
}

impl Debug for RemoteEndpointSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteEndpointSender")
            .field("path", &self.path)
            .field("actor_type", &self.actor_type)
            .finish()
    }
}

impl Clone for RemoteEndpointSender {
    fn clone(&self) -> Self {
        Self {
            proxy_tx: self.proxy_tx.clone(),
            path: self.path.clone(),
            actor_type: self.actor_type.clone(),
        }
    }
}

/// A typed version of RemoteEndpointSender for a specific message type
///
/// This implements MessageSender<M> for a specific message type M, handling
/// serialization and deserialization of messages.
pub struct TypedRemoteEndpointSender<M: RemoteMessageType + Send + Sync> {
    /// The underlying RemoteEndpointSender
    inner: RemoteEndpointSender,
    /// Phantom data for the message type
    _phantom: PhantomData<M>,
}

impl<M: RemoteMessageType + Send + Sync + 'static> TypedRemoteEndpointSender<M> {
    /// Creates a new TypedRemoteEndpointSender
    pub fn new(inner: RemoteEndpointSender) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Serializes a message to bytes using bincode
    fn serialize_message(msg: &M) -> Result<Bytes, String> {
        let config = bincode::config::standard();
        bincode::encode_to_vec(msg, config)
            .map(|v| Bytes::from(v))
            .map_err(|e| format!("Failed to serialize message: {}", e))
    }

    /// Deserializes a response from bytes using bincode
    fn deserialize_response(bytes: &[u8]) -> Result<M::Result, String> 
        where M::Result: bincode::Decode<()> 
    {
        let config = bincode::config::standard();
        bincode::decode_from_slice::<M::Result, _>(bytes, config)
            .map(|(result, _)| result)
            .map_err(|e| format!("Failed to deserialize response: {}", e))
    }
}

#[async_trait::async_trait]
impl<M: RemoteMessageType + Send + Sync + 'static> MessageSender<M> for TypedRemoteEndpointSender<M>
where 
    M::Result: bincode::Decode<()>
{
    async fn send(&self, msg: M) -> Result<M::Result, SendError> {
        // Serialize the message
        let serialized = Self::serialize_message(&msg)
            .map_err(|_| SendError::MailboxClosed)?;

        // Create a RemoteMessage
        let remote_msg = RemoteMessage {
            type_name: M::TYPE_ID.to_string(),
            message_data: serialized,
        };

        // Send the message and get the response
        let response = self.inner.send_remote(remote_msg).await?;

        // Deserialize the response
        Self::deserialize_response(&response.message_data)
            .map_err(|_| SendError::MailboxClosed)
    }

    async fn send_no_response(&self, msg: M) -> Result<(), SendError> {
        // For now, just send the message and ignore the response
        let _ = self.send(msg).await?;
        Ok(())
    }
}

impl<M: RemoteMessageType + Send + Sync> Clone for TypedRemoteEndpointSender<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Creates a properly typed EndpointSender for a remote actor
pub fn create_typed_endpoint_sender<A, M>(
    proxy_tx: mpsc::Sender<RemoteProxyMessage>,
    path: ActorPath,
    actor_type: String,
) -> Box<dyn MessageSender<M>>
where
    A: crate::actor::Actor,
    M: RemoteMessageType + Send + Sync + 'static,
    M::Result: bincode::Decode<()>,
{
    // Create the RemoteEndpointSender
    let sender = RemoteEndpointSender::new(proxy_tx, path, actor_type);
    
    // Create a TypedRemoteEndpointSender for this message type
    let typed_sender = TypedRemoteEndpointSender::<M>::new(sender);
    
    // Return it as a boxed MessageSender
    Box::new(typed_sender)
}