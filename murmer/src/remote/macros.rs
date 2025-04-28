/// Macro for registering an actor type with the remote actor system
///
/// This macro implements the RemoteRegistered trait for an actor type and registers
/// the actor type with the inventory system, enabling type-safe remote communication.
#[macro_export]
macro_rules! register_remote_actor {
    ($actor_type:ty, $key:expr) => {
        // Create a module to avoid name conflicts
        mod __actor_registration_module {
            use super::*;
            use $crate::system::AnyEndpoint;
            use $crate::path::ActorPath;
            use std::sync::Arc;
            use std::any::TypeId;
            use tokio::sync::mpsc;
            
            // Define a function for creating remote endpoints for this actor type
            pub fn create_endpoint(
                path: ActorPath,
                proxy_tx: mpsc::Sender<$crate::remote::RemoteProxyMessage>
            ) -> AnyEndpoint {
                // Use the actor's RemoteRegistered implementation to create the endpoint
                <$actor_type as $crate::remote::RemoteRegistered>::create_remote_endpoint(path, proxy_tx)
            }
            
            // Create the static registration entry
            inventory::submit! {
                $crate::remote::ActorTypeRegistration::new(
                    $key,
                    stringify!($actor_type),
                    create_endpoint
                )
            }
        }
    };
}

/// Macro for registering a message type for remote communication
///
/// This macro marks a message type as remote-capable and registers it with
/// the inventory system, enabling serialization and remote handling.
#[macro_export]
macro_rules! remote_message {
    ($actor_type:ty, $message_type:ty, $type_id:expr) => {
        impl $crate::remote::RemoteMessageType for $message_type {
            const TYPE_ID: &'static str = $type_id;
        }
        
        // Create a module to avoid name conflicts
        paste::paste! {
            mod [<__message_registration_module_ $message_type>] {
                use super::*;
                use bytes::Bytes;
                
                // Define a handler function for this message type
                pub fn handle_message(bytes: Bytes) -> Result<Bytes, String> {
                    // Deserialize the message
                    let config = bincode::config::standard();
                    let msg = bincode::decode_from_slice::<$message_type, _>(&bytes, config)
                        .map_err(|e| format!("Deserialization error: {}", e))?
                        .0;
        
                    // Create a temporary endpoint to handle this message
                    // This is a placeholder - in the actual implementation
                    // we'd send the message to the actor and get its response
                    let response = match bincode::encode_to_vec(&msg, config) {
                        Ok(r) => r,
                        Err(e) => return Err(format!("Serialization error: {}", e)),
                    };
        
                    Ok(Bytes::from(response))
                }
                
                // Register the message handler with the inventory system
                inventory::submit! {
                    $crate::remote::MessageTypeRegistration::new(
                        <$actor_type as $crate::actor::Registered>::RECEPTIONIST_KEY,
                        $type_id,
                        handle_message
                    )
                }
            }
        }
    };
}

/// Macro for marking a message handler implementation as remote-accessible
///
/// This macro registers a handler implementation with the inventory system,
/// enabling it to be called remotely.
#[macro_export]
macro_rules! remote {
    (impl Handler<$message_type:ty> for $actor_type:ty) => {
        // This is just a marker for documentation purposes
        // The actual implementation is provided by the user
    };
}