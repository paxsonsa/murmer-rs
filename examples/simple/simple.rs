use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

// Error types
#[derive(Error, Debug)]
pub enum DistributedActorError {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] bincode::Error),
    
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),
    
    #[error("Actor not found: {0}")]
    ActorNotFound(ActorId),
    
    #[error("Context not found: {0}")]
    ContextNotFound(ActorId),
    
    #[error("Actor type mismatch")]
    ActorTypeMismatch,
}

// Basic types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorId(Uuid);

impl ActorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Core traits
pub trait Actor: Send + Sync + 'static {}

pub trait Message: Send + 'static {
    type Result: Send + 'static;
    const MESSAGE_TYPE: &'static str;
}

#[async_trait]
pub trait Handler<M: Message>: Actor {
    async fn handle(&mut self, ctx: &mut Context<Self>, message: M) -> M::Result;
}

pub trait DistributedActor: Actor {
    const RECEPTIONIST_KEY: &'static str;

    // Register all remote messages with the system
    fn register_remote_messages(registry: &mut ActorRegistry);
    
    // Handle remote message (dispatching to correct handler)
    async fn handle_remote_message(
        &mut self, 
        ctx: &mut Context<Self>, 
        msg: RemoteMessage
    ) -> Result<RemoteMessage, DistributedActorError>;
}

// Remote message handling
pub struct RemoteMessage {
    pub message_type: String,
    pub serialized_data: Vec<u8>,
}

impl RemoteMessage {
    // Create a new remote message from a message type and value
    pub fn new<M: Message + Serialize>(value: M) -> Result<Self, DistributedActorError> {
        let serialized = bincode::serialize(&value)
            .map_err(|e| DistributedActorError::SerializationError(e))?;
        
        Ok(Self {
            message_type: M::MESSAGE_TYPE.to_string(),
            serialized_data: serialized,
        })
    }
    
    // Create a response message
    pub fn from<T: Serialize>(msg_type: &str, value: T) -> Result<Self, DistributedActorError> {
        let serialized = bincode::serialize(&value)
            .map_err(|e| DistributedActorError::SerializationError(e))?;
        
        Ok(Self {
            message_type: msg_type.to_string(),
            serialized_data: serialized,
        })
    }
    
    // Deserialize as a specific type
    pub fn deserialize_as<T: DeserializeOwned>(&self) -> Result<T, DistributedActorError> {
        bincode::deserialize(&self.serialized_data)
            .map_err(|e| DistributedActorError::DeserializationError(e))
    }
}

// Actor context
pub struct Context<A: ?Sized> {
    actor_id: ActorId,
    _phantom: PhantomData<A>,
}

impl<A> Context<A> {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            _phantom: PhantomData,
        }
    }
    
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

// Registry for actor types and their remote messages
pub struct ActorRegistry {
    // Maps message types to their registration info
    messages: RwLock<HashMap<String, TypeId>>,
    actor: Box<dyn DistributedActor>,
}

impl ActorRegistry {
    pub fn new<A: DistributedActor + 'static>(actor: A) -> Self {
        let mut registry = Self {
            messages: RwLock::new(HashMap::new()),
            actor: Box::new(actor),
        };
        
        // Register the actor's remote messages
        A::register_remote_messages(&mut registry);
        
        registry
    }
    
    // Register a remote message
    pub fn register<M: Message + 'static>(&mut self) {
        let message_type = M::MESSAGE_TYPE;
        self.messages.write().unwrap().insert(
            message_type.to_string(),
            TypeId::of::<M>(),
        );
    }
    
    // Get the actor
    pub fn actor(&mut self) -> &mut dyn DistributedActor {
        self.actor.as_mut()
    }
    
    // Check if a message type is registered
    pub fn is_registered(&self, message_type: &str) -> bool {
        self.messages.read().unwrap().contains_key(message_type)
    }
}

// Actor endpoint for sending messages
pub struct Endpoint<A: Actor> {
    actor_id: ActorId,
    system: Arc<ActorSystem>,
    _phantom: PhantomData<A>,
}

impl<A: Actor> Endpoint<A> {
    pub fn new(actor_id: ActorId, system: Arc<ActorSystem>) -> Self {
        Self {
            actor_id,
            system,
            _phantom: PhantomData,
        }
    }
    
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, DistributedActorError>
    where
        M: Message + Serialize + DeserializeOwned,
        A: Handler<M>,
    {
        self.system.send::<A, M>(self.actor_id, msg).await
    }
}

// Actor system for managing actors
pub struct ActorSystem {
    actors: RwLock<HashMap<ActorId, Box<dyn Actor>>>,
    contexts: RwLock<HashMap<ActorId, Box<dyn std::any::Any>>>,
    registries: RwLock<HashMap<ActorId, ActorRegistry>>,
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            actors: RwLock::new(HashMap::new()),
            contexts: RwLock::new(HashMap::new()),
            registries: RwLock::new(HashMap::new()),
        }
    }
    
    pub fn register<A>(&self, actor: A) -> Endpoint<A>
    where
        A: DistributedActor + 'static,
    {
        let actor_id = ActorId::new();
        let context = Context::new(actor_id);
        let registry = ActorRegistry::new(actor);
        
        let mut actors = self.actors.write().unwrap();
        let mut contexts = self.contexts.write().unwrap();
        let mut registries = self.registries.write().unwrap();
        
        actors.insert(actor_id, Box::new(DummyActor));  // Placeholder, will handle via registry
        contexts.insert(actor_id, Box::new(context));
        registries.insert(actor_id, registry);
        
        Endpoint::new(actor_id, Arc::new(self.clone()))
    }
    
    async fn send<A, M>(&self, actor_id: ActorId, msg: M) -> Result<M::Result, DistributedActorError>
    where
        A: Actor + Handler<M>,
        M: Message + Serialize + DeserializeOwned,
    {
        // In a real implementation, this would handle local vs remote dispatch
        // For this example, we'll just use the remote message path
        
        let remote_msg = RemoteMessage::new(msg)?;
        
        // Find the registry for this actor
        let mut registries = self.registries.write().unwrap();
        let registry = registries.get_mut(&actor_id)
            .ok_or_else(|| DistributedActorError::ActorNotFound(actor_id))?;
        
        // Get the actor from the registry
        let actor = registry.actor();
        
        // Create a context for the call
        let mut ctx = Context::new(actor_id);
        
        // Call the remote message handler
        let response = actor.handle_remote_message(&mut ctx, remote_msg).await?;
        
        // Deserialize the response
        let result = response.deserialize_as::<M::Result>()?;
        
        Ok(result)
    }
}

impl Clone for ActorSystem {
    fn clone(&self) -> Self {
        // In a real implementation, we'd make this proper Clone
        // For this example, we'll just create a new empty system
        Self::new()
    }
}

// Dummy actor for placeholder in actors map
struct DummyActor;
impl Actor for DummyActor {}

/// A simple actor implementation example
struct Counter {
    count: i32,
}

impl Counter {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Actor for Counter {}

// Message definitions
#[derive(Serialize, Deserialize)]
struct Increment;

impl Message for Increment {
    type Result = i32;
    const MESSAGE_TYPE: &'static str = "Counter/Increment";
}

#[async_trait]
impl Handler<Increment> for Counter {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: Increment) -> i32 {
        self.count += 1;
        self.count
    }
}

#[derive(Serialize, Deserialize)]
struct Decrement;

impl Message for Decrement {
    type Result = i32;
    const MESSAGE_TYPE: &'static str = "Counter/Decrement";
}

#[async_trait]
impl Handler<Decrement> for Counter {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: Decrement) -> i32 {
        self.count -= 1;
        self.count
    }
}

#[derive(Serialize, Deserialize)]
struct Reset;

impl Message for Reset {
    type Result = i32;
    const MESSAGE_TYPE: &'static str = "Counter/Reset";
}

#[async_trait]
impl Handler<Reset> for Counter {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: Reset) -> i32 {
        self.count = 0;
        self.count
    }
}

#[derive(Serialize, Deserialize)]
struct Get;

impl Message for Get {
    type Result = i32;
    const MESSAGE_TYPE: &'static str = "Counter/Get";
}

#[async_trait]
impl Handler<Get> for Counter {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _message: Get) -> i32 {
        self.count
    }
}

#[async_trait]
impl DistributedActor for Counter {
    const RECEPTIONIST_KEY: &'static str = "Counter";

    fn register_remote_messages(registry: &mut ActorRegistry) {
        registry.register::<Increment>();
        registry.register::<Decrement>();
        registry.register::<Reset>();
        registry.register::<Get>();
    }

    async fn handle_remote_message(
        &mut self, 
        ctx: &mut Context<Self>, 
        msg: RemoteMessage
    ) -> Result<RemoteMessage, DistributedActorError> {
        match msg.message_type.as_str() {
            Increment::MESSAGE_TYPE => {
                let increment = Increment;
                let result = self.handle(ctx, increment).await;
                Ok(RemoteMessage::from(Increment::MESSAGE_TYPE, result)?)
            }
            Decrement::MESSAGE_TYPE => {
                let decrement = Decrement;
                let result = self.handle(ctx, decrement).await;
                Ok(RemoteMessage::from(Decrement::MESSAGE_TYPE, result)?)
            }
            Reset::MESSAGE_TYPE => {
                let reset = Reset;
                let result = self.handle(ctx, reset).await;
                Ok(RemoteMessage::from(Reset::MESSAGE_TYPE, result)?)
            }
            Get::MESSAGE_TYPE => {
                let get = Get;
                let result = self.handle(ctx, get).await;
                Ok(RemoteMessage::from(Get::MESSAGE_TYPE, result)?)
            }
            _ => Err(DistributedActorError::UnknownMessageType(msg.message_type)),
        }
    }
}

#[tokio::main]
async fn main() {
    // Create a counter actor
    let counter = Counter::new();
    
    // Create an actor system
    let system = ActorSystem::new();
    
    // Register the counter with the system
    let endpoint = system.register(counter);
    
    // Send a message to the counter
    let result = endpoint.send(Increment).await.unwrap();
    println!("Counter after increment: {}", result);
    
    // Send a few more messages
    let result = endpoint.send(Increment).await.unwrap();
    println!("Counter after another increment: {}", result);
    
    let result = endpoint.send(Decrement).await.unwrap();
    println!("Counter after decrement: {}", result);
    
    let result = endpoint.send(Reset).await.unwrap();
    println!("Counter after reset: {}", result);
    
    let result = endpoint.send(Get).await.unwrap();
    println!("Current counter value: {}", result);
}
