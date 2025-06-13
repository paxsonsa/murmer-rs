use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

use crate::message::{Message, SendError};
use crate::{
    actor::{Actor, Handler},
    context::Context,
};
use crate::{
    path::ActorPath,
    system::{AnyEndpoint, Endpoint},
};

#[cfg(test)]
#[path = "receptionist.test.rs"]
mod tests;

/// Events emitted by the receptionist when actors are registered or deregistered
#[derive(Debug, Clone)]
pub enum RegistryEvent {
    /// An actor was added to the registry
    Added(ActorPath, AnyEndpoint),
    /// An actor was removed from the registry
    Removed(ActorPath),
}

/// Filter configuration for registry event subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    /// Exact actor type to subscribe to (required - no wildcards allowed)
    pub actor_type: String,
    /// Optional glob pattern for group matching (e.g., "region*", "default")
    pub group_pattern: Option<String>,
}

impl Default for SubscriptionFilter {
    fn default() -> Self {
        Self {
            actor_type: "*".to_string(), // Match all types by default (for backward compatibility)
            group_pattern: None,         // No group filtering by default
        }
    }
}

/// A subscription to registry events
pub struct Subscription {
    /// Channel receiver for registry events
    receiver: mpsc::UnboundedReceiver<RegistryEvent>,
}

impl Subscription {
    /// Create a new subscription with the given receiver
    fn new(receiver: mpsc::UnboundedReceiver<RegistryEvent>) -> Self {
        Self { receiver }
    }

    /// Get the next registry event
    pub async fn next(&mut self) -> Option<RegistryEvent> {
        self.receiver.recv().await
    }
}

/// Internal subscription tracking
#[derive(Debug)]
struct SubscriberInfo {
    /// Channel sender for this subscription
    sender: mpsc::UnboundedSender<RegistryEvent>,
    /// Filter configuration
    filter: SubscriptionFilter,
}

/// Errors that can occur during subscription operations
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// Failed to send message to receptionist
    #[error("Failed to communicate with receptionist: {0}")]
    CommunicationError(#[from] SendError),
}

/// Factory trait for creating endpoints on demand
pub trait EndpointFactory: Send + Sync + 'static {
    fn create(&self) -> AnyEndpoint;
    fn clone_factory(&self) -> Box<dyn EndpointFactory>;
}

/// A lazy endpoint that can create the actual endpoint on demand
#[derive(Clone)]
struct LazyEndpoint {
    /// Cached endpoint, created on first access
    endpoint: Arc<Mutex<Option<AnyEndpoint>>>,
    /// Factory function to create the endpoint when needed
    endpoint_factory: Box<dyn EndpointFactory>,
}

impl LazyEndpoint {
    /// Create a new lazy endpoint with an existing endpoint (for local actors)
    fn from_endpoint(endpoint: AnyEndpoint) -> Self {
        Self {
            endpoint: Arc::new(Mutex::new(Some(endpoint))),
            endpoint_factory: Box::new(NoOpFactory),
        }
    }

    /// Create a new lazy endpoint with a factory (for remote actors)
    fn from_factory(factory: Box<dyn EndpointFactory>) -> Self {
        Self {
            endpoint: Arc::new(Mutex::new(None)),
            endpoint_factory: factory,
        }
    }

    /// Get the endpoint, creating it if necessary
    fn get_endpoint(&self) -> AnyEndpoint {
        let mut endpoint_guard = self.endpoint.lock().unwrap();
        if endpoint_guard.is_none() {
            *endpoint_guard = Some(self.endpoint_factory.create());
        }
        endpoint_guard.as_ref().unwrap().clone()
    }
}

impl Clone for Box<dyn EndpointFactory> {
    fn clone(&self) -> Self {
        self.clone_factory()
    }
}

/// No-op factory for endpoints that are already created
struct NoOpFactory;

impl EndpointFactory for NoOpFactory {
    fn create(&self) -> AnyEndpoint {
        panic!("NoOpFactory should never be called - endpoint should already exist")
    }

    fn clone_factory(&self) -> Box<dyn EndpointFactory> {
        Box::new(NoOpFactory)
    }
}


impl PartialEq for LazyEndpoint {
    fn eq(&self, other: &Self) -> bool {
        // For equality, we compare the actual endpoints if available
        // This is a simplified comparison - in practice you might want to compare paths
        std::ptr::eq(self.endpoint.as_ref(), other.endpoint.as_ref())
    }
}

impl Eq for LazyEndpoint {}

impl std::hash::Hash for LazyEndpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based on the pointer to the Arc - this is a simplified approach
        std::ptr::hash(self.endpoint.as_ref(), state);
    }
}

impl std::fmt::Debug for LazyEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyEndpoint")
            .field("has_endpoint", &self.endpoint.lock().unwrap().is_some())
            .finish()
    }
}

/// Entry representing a registered actor
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Entry {
    actor_path: ActorPath,
    endpoint: LazyEndpoint,
}

#[derive(Default)]
pub struct ReceptionistActor {
    /// Map of the actors registered with the receptionist.
    /// This stores the actors under their actor type key.
    registered_actors: HashMap<String, HashSet<Entry>>,
    /// Active subscriptions to registry events
    subscribers: Vec<SubscriberInfo>,
    /// Next subscription ID for tracking
    next_subscription_id: usize,
}

impl Actor for ReceptionistActor {
    const ACTOR_TYPE_KEY: &'static str = "system.receptionist";
}

/// Message to register an actor with the receptionist
pub struct RegisterMessage {
    /// The actor path of the actor to register
    pub path: ActorPath,
    /// Optional endpoint for local actors, None for remote actors that will be created lazily
    pub endpoint: Option<AnyEndpoint>,
    /// Optional factory for creating remote endpoints
    pub endpoint_factory: Option<Box<dyn EndpointFactory>>,
}

impl std::fmt::Debug for RegisterMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterMessage")
            .field("path", &self.path)
            .field("has_endpoint", &self.endpoint.is_some())
            .field("has_factory", &self.endpoint_factory.is_some())
            .finish()
    }
}

impl RegisterMessage {
    /// Create a registration message for a local actor with an existing endpoint
    pub fn local(path: ActorPath, endpoint: AnyEndpoint) -> Self {
        Self {
            path,
            endpoint: Some(endpoint),
            endpoint_factory: None,
        }
    }

    /// Create a registration message for a remote actor with a factory
    pub fn remote(path: ActorPath, factory: Box<dyn EndpointFactory>) -> Self {
        Self {
            path,
            endpoint: None,
            endpoint_factory: Some(factory),
        }
    }
}

impl Message for RegisterMessage {
    type Result = bool;
}

#[async_trait]
impl Handler<RegisterMessage> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: RegisterMessage) -> bool {
        let actor_path = msg.path.clone();
        let key = msg.path.type_id.to_string();

        let lazy_endpoint = if let Some(endpoint) = msg.endpoint {
            LazyEndpoint::from_endpoint(endpoint)
        } else if let Some(factory) = msg.endpoint_factory {
            LazyEndpoint::from_factory(factory)
        } else {
            tracing::error!("RegisterMessage must have either endpoint or endpoint_factory");
            return false;
        };

        let event = RegistryEvent::Added(actor_path.clone(), lazy_endpoint.get_endpoint());
        let entry = Entry {
            actor_path,
            endpoint: lazy_endpoint,
        };
        self.registered_actors
            .entry(key)
            .or_insert_with(HashSet::new)
            .insert(entry);

        // Broadcast the registration event to subscribers
        self.broadcast_event(event);

        true
    }
}

/// Message to deregister an actor from the receptionist
#[derive(Debug)]
pub struct DeregisterMessage {
    /// The actor path of the actor to deregister
    pub path: ActorPath,
}

impl Message for DeregisterMessage {
    type Result = bool;
}

#[async_trait]
impl Handler<DeregisterMessage> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: DeregisterMessage) -> bool {
        let key = msg.path.type_id.to_string();

        if let Some(entries) = self.registered_actors.get_mut(&key) {
            let original_len = entries.len();
            entries.retain(|entry| entry.actor_path != msg.path);
            let was_removed = entries.len() < original_len;

            if was_removed {
                let event = RegistryEvent::Removed(msg.path);
                self.broadcast_event(event);
            }

            was_removed
        } else {
            false
        }
    }
}

/// Message to lookup actors by type key
#[derive(Debug)]
pub struct LookupMessage {
    /// The actor type key to lookup
    pub actor_type_key: String,
}

/// Message to subscribe to registry events
#[derive(Debug)]
pub struct SubscribeMessage {
    /// Filter for subscription events
    pub filter: SubscriptionFilter,
    /// Channel sender for delivering events
    pub sender: mpsc::UnboundedSender<RegistryEvent>,
}

impl Message for LookupMessage {
    type Result = Vec<AnyEndpoint>;
}

impl Message for SubscribeMessage {
    type Result = ();
}

#[async_trait]
impl Handler<LookupMessage> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: LookupMessage) -> Vec<AnyEndpoint> {
        self.registered_actors
            .get(&msg.actor_type_key)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| entry.endpoint.get_endpoint())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl Handler<SubscribeMessage> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SubscribeMessage) -> () {
        let subscriber_info = SubscriberInfo {
            sender: msg.sender,
            filter: msg.filter,
        };

        self.subscribers.push(subscriber_info);

        tracing::debug!(
            "Added new subscription, total subscribers: {}",
            self.subscribers.len()
        );
    }
}

impl ReceptionistActor {
    /// Broadcast a registry event to all matching subscribers
    fn broadcast_event(&mut self, event: RegistryEvent) {
        let mut to_remove = Vec::new();

        for (index, subscriber) in self.subscribers.iter().enumerate() {
            if self.should_send_event(&event, &subscriber.filter) {
                if let Err(_) = subscriber.sender.send(event.clone()) {
                    // Subscriber channel is closed, mark for removal
                    tracing::debug!("Removing disconnected subscriber");
                    to_remove.push(index);
                }
            }
        }

        // Remove disconnected subscribers in reverse order to maintain indices
        for &index in to_remove.iter().rev() {
            self.subscribers.remove(index);
        }
    }

    /// Check if an event should be sent to a subscriber based on their filter
    fn should_send_event(&self, event: &RegistryEvent, filter: &SubscriptionFilter) -> bool {
        let path = match event {
            RegistryEvent::Added(path, _) => path,
            RegistryEvent::Removed(path) => path,
        };

        // Check actor type match (exact match required, unless "*" for all types)
        if filter.actor_type != "*" && path.type_id.as_ref() != filter.actor_type {
            return false;
        }

        // Check group pattern if specified
        if let Some(group_pattern) = &filter.group_pattern {
            match glob::Pattern::new(group_pattern) {
                Ok(pattern) => pattern.matches(path.group_id.as_ref()),
                Err(_) => {
                    tracing::warn!("Invalid glob pattern: {}", group_pattern);
                    false
                }
            }
        } else {
            // No group filtering, type matched, so send event
            true
        }
    }
}

/// Errors that can occur during actor lookup operations
#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    /// No actor was found registered under the given key
    #[error("no actor registered for key")]
    NotFound,
    /// Multiple actors were found when expecting only one
    #[error("multiple actors registered for unique key")]
    MultipleFound,
    /// An error occurred in the underlying actor system
    #[error("actor system error: {0}")]
    SystemError(#[from] SendError),
}

/// Client interface for interacting with the ReceptionistActor
///
/// This struct provides a high-level API for registering, deregistering,
/// looking up actors, and subscribing to registration updates.
#[derive(Clone)]
pub struct Receptionist {
    /// Endpoint to the underlying ReceptionistActor
    inner_endpoint: Endpoint<ReceptionistActor>,
}

impl Receptionist {
    /// Creates a new Receptionist client connected to the given ReceptionistActor endpoint
    pub fn new(endpoint: Endpoint<ReceptionistActor>) -> Self {
        Self {
            inner_endpoint: endpoint,
        }
    }


    /// Register a local actor with an existing endpoint
    pub async fn register_local(
        &self,
        path: ActorPath,
        endpoint: AnyEndpoint,
    ) -> Result<bool, SendError> {
        self.inner_endpoint
            .send(RegisterMessage::local(path, endpoint))
            .await
    }

    /// Register a remote actor with a factory for lazy endpoint creation
    pub async fn register_remote(
        &self,
        path: ActorPath,
        factory: Box<dyn EndpointFactory>,
    ) -> Result<bool, SendError> {
        self.inner_endpoint
            .send(RegisterMessage::remote(path, factory))
            .await
    }

    /// Deregister an actor from the receptionist
    pub async fn deregister(&self, path: ActorPath) -> Result<bool, SendError> {
        self.inner_endpoint.send(DeregisterMessage { path }).await
    }

    /// Lookup all actors registered under a specific actor type key
    pub async fn lookup(&self, actor_type_key: String) -> Result<Vec<AnyEndpoint>, SendError> {
        self.inner_endpoint
            .send(LookupMessage { actor_type_key })
            .await
    }

    /// Lookup a single actor by type key, returns error if none or multiple found
    pub async fn lookup_one(&self, actor_type_key: String) -> Result<AnyEndpoint, LookupError> {
        let endpoints = self.lookup(actor_type_key).await?;
        match endpoints.len() {
            0 => Err(LookupError::NotFound),
            1 => Ok(endpoints.into_iter().next().unwrap()),
            _ => Err(LookupError::MultipleFound),
        }
    }

    /// Lookup actors with type safety - converts AnyEndpoint to typed Endpoint<T>
    pub async fn lookup_typed<T>(
        &self,
        actor_type_key: String,
    ) -> Result<Vec<Endpoint<T>>, LookupError>
    where
        T: Actor + 'static,
    {
        let any_endpoints = self.lookup(actor_type_key).await?;
        let typed_endpoints: Vec<Endpoint<T>> = any_endpoints
            .into_iter()
            .filter_map(|any_endpoint| any_endpoint.downcast())
            .collect();

        Ok(typed_endpoints)
    }

    /// Lookup a single typed actor
    pub async fn lookup_one_typed<T>(
        &self,
        actor_type_key: String,
    ) -> Result<Endpoint<T>, LookupError>
    where
        T: Actor + 'static,
    {
        let endpoints = self.lookup_typed::<T>(actor_type_key).await?;
        match endpoints.len() {
            0 => Err(LookupError::NotFound),
            1 => Ok(endpoints.into_iter().next().unwrap()),
            _ => Err(LookupError::MultipleFound),
        }
    }

    /// Subscribe to registry events with optional filtering
    pub async fn subscribe(
        &self,
        filter: SubscriptionFilter,
    ) -> Result<Subscription, SubscriptionError> {
        let (sender, receiver) = mpsc::unbounded_channel();

        // Send subscribe message to the receptionist actor
        self.inner_endpoint
            .send(SubscribeMessage { filter, sender })
            .await?;

        // Return the subscription handle
        Ok(Subscription::new(receiver))
    }
}
