use crate::actor::{Actor, Handler, Registered};
use crate::context::Context;
use crate::message::{Message, RemoteMessage};
use crate::path::ActorPath;
use crate::prelude::*;
use crate::register_remote_actor;
use crate::remote::RemoteRegistered;
use crate::remote_message;
use crate::system::AnyEndpoint;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// First, define a simple actor
pub struct UserActor {
    name: String,
    user_id: String,
}

impl Actor for UserActor {
    const ACTOR_TYPE_KEY: &'static str = "user-actor";
}

impl Registered for UserActor {
    const RECEPTIONIST_KEY: &'static str = "user-actor";
}

// Implement RemoteRegistered trait manually
impl RemoteRegistered for UserActor {
    fn create_remote_endpoint(
        path: ActorPath,
        proxy_tx: tokio::sync::mpsc::Sender<crate::remote::RemoteProxyMessage>,
    ) -> AnyEndpoint {
        use crate::remote::{RemoteEndpointSender, TypedRemoteEndpointSender};

        // Create the RemoteEndpointSender that communicates with the RemoteProxy
        let sender = RemoteEndpointSender::new(proxy_tx, path.clone(), "user-actor".to_string());

        // Create a TypedRemoteEndpointSender for GetUserProfile messages
        let get_profile_sender = TypedRemoteEndpointSender::<GetUserProfile>::new(sender.clone());

        // Create and return the AnyEndpoint with the properly typed sender
        AnyEndpoint {
            endpoint_sender: Box::new(get_profile_sender),
            type_id: std::any::TypeId::of::<UserActor>(),
            type_name: std::any::type_name::<UserActor>(),
            path: Arc::new(path),
            actor_type_string: Some("user-actor".to_string()),
        }
    }

    fn message_handlers() -> Vec<(&'static str, fn(Bytes) -> Result<Bytes, String>)> {
        vec![
            // Handler for GetUserProfile messages
            ("get-user-profile", |bytes| {
                // Deserialize the message
                let config = bincode::config::standard();
                let msg = match bincode::decode_from_slice::<GetUserProfile, _>(&bytes, config) {
                    Ok((msg, _)) => msg,
                    Err(e) => return Err(format!("Deserialization error: {}", e)),
                };

                // Create a response (in a real implementation, this would call the actual handler)
                let response: Result<UserProfile, UserError> = Ok(UserProfile {
                    user_id: msg.user_id,
                    name: "Example User".to_string(),
                    email: None,
                });

                // Serialize the response
                match bincode::encode_to_vec(&response, config) {
                    Ok(r) => Ok(Bytes::from(r)),
                    Err(e) => Err(format!("Serialization error: {}", e)),
                }
            }),
        ]
    }
}

// Register the actor type with the remote system
register_remote_actor!(UserActor, "user-actor");

// Define a message that can be sent to our actor
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct GetUserProfile {
    pub user_id: String,
}

// Implement Message trait
impl Message for GetUserProfile {
    type Result = Result<UserProfile, UserError>;
}

// Register the message as remote-capable
remote_message!(UserActor, GetUserProfile, "get-user-profile");

// Define the response data structure
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct UserProfile {
    pub user_id: String,
    pub name: String,
    pub email: Option<String>,
}

// Define error types
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode, thiserror::Error)]
pub enum UserError {
    #[error("User not found")]
    NotFound,
    #[error("Permission denied")]
    PermissionDenied,
}

// Implement message handler
#[async_trait::async_trait]
impl Handler<GetUserProfile> for UserActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        msg: GetUserProfile,
    ) -> Result<UserProfile, UserError> {
        // Check if this is the user we're looking for
        if msg.user_id == self.user_id {
            Ok(UserProfile {
                user_id: self.user_id.clone(),
                name: self.name.clone(),
                email: None,
            })
        } else {
            Err(UserError::NotFound)
        }
    }
}

// Handle RemoteMessage (needed for all remote actors)
#[async_trait::async_trait]
impl Handler<RemoteMessage> for UserActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        _msg: RemoteMessage,
    ) -> Result<RemoteMessage, crate::message::RemoteMessageError> {
        // This will be implemented in later phases
        // For now, we'll just return an error
        Err(crate::message::RemoteMessageError::InvalidType)
    }
}

// Simple test for the inventory registrations
#[test]
fn test_actor_registration() {
    // Just verify that we can iterate and find our registration
    let mut found = false;

    // Iterate through all registered actor types
    for registration in inventory::iter::<crate::remote::ActorTypeRegistration>() {
        if registration.key == "user-actor" {
            found = true;
            assert_eq!(registration.type_name, "UserActor");
        }
    }

    assert!(found, "Actor registration was not found in inventory");
}

#[test]
fn test_message_registration() {
    // Just verify that we can iterate and find our message registration
    let mut found = false;

    // Iterate through all registered message types
    for registration in inventory::iter::<crate::remote::MessageTypeRegistration>() {
        if registration.actor_type == "user-actor"
            && registration.message_type == "get-user-profile"
        {
            found = true;
        }
    }

    assert!(found, "Message registration was not found in inventory");
}

#[test]
fn test_registration_and_lookup() {
    // This test verifies that we can find our actor type in the inventory system
    // through string-based lookup using the registration key

    let mut found = false;
    for registration in inventory::iter::<crate::remote::ActorTypeRegistration>() {
        if registration.key == "user-actor" {
            // Found the right registration, mark it as found
            found = true;

            // Verify basic properties
            assert_eq!(registration.type_name, "UserActor");

            // Create a dummy ActorPath and channel
            let path = crate::path::ActorPath::remote(
                "127.0.0.1".to_string(),
                8080,
                "user-actor".to_string(),
                crate::id::Id::new(),
            );

            let (tx, _) = tokio::sync::mpsc::channel(32);

            // Verify we can call the factory function without errors
            let _endpoint = (registration.create_endpoint)(path, tx);
        }
    }

    assert!(found, "Could not find UserActor registration in inventory");
}

