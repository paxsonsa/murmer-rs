use futures::Stream;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use tokio::sync::mpsc;

use crate::message::Message;
use crate::system::{AnyEndpoint, Endpoint};
use crate::{
    actor::{Actor, ActorError, Handler},
    context::Context,
};

#[cfg(test)]
#[path = "receptionist.test.rs"]
mod tests;

/// A type-safe key used for registering and looking up actors
///
/// Keys are used to identify groups of actors of the same type.
/// The phantom data ensures type safety at compile time.
pub struct Key<T: Actor> {
    /// The string identifier for this key
    id: &'static str,
    /// Phantom data to maintain type information
    _phantom: PhantomData<T>,
}

impl<T: Actor> Key<T> {
    /// Creates a new key with the given static string identifier
    pub const fn new(id: &'static str) -> Self {
        Self {
            id: &id,
            _phantom: PhantomData,
        }
    }

    /// Creates a default key with the identifier "default"
    pub const fn default() -> Self {
        Self {
            id: "default",
            _phantom: PhantomData,
        }
    }
}

impl<T: Actor> fmt::Debug for Key<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Key").field("id", &self.id).finish()
    }
}

impl<T: Actor> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            _phantom: PhantomData,
        }
    }
}

/// Updates sent to subscribers when actors are registered or deregistered
pub enum ListingUpdate<T: Actor> {
    /// Indicates a new actor has been registered
    Registered(Endpoint<T>),
    /// Indicates an actor has been deregistered
    Deregistered(Endpoint<T>),
}

/// Type-erased version of ListingUpdate used internally
enum AnyListingUpdate {
    /// A new registration with type-erased endpoint
    Registered(AnyEndpoint),
    /// A deregistration with type-erased endpoint
    Deregistered(AnyEndpoint),
}

impl AnyListingUpdate {
    /// Attempts to convert a type-erased update into a typed update
    /// Returns None if the type conversion fails
    fn into_typed<T: Actor>(self) -> Option<ListingUpdate<T>> {
        match self {
            Self::Registered(endpoint) => endpoint.into_downcast().map(ListingUpdate::Registered),
            Self::Deregistered(endpoint) => {
                endpoint.into_downcast().map(ListingUpdate::Deregistered)
            }
        }
    }
}

/// Internal representation of an actor registration.
/// Stores the type-erased endpoint along with its key and type information.
#[derive(Debug)]
struct Registration {
    /// The type-erased endpoint of the registered actor
    endpoint: AnyEndpoint,
    /// The string key under which this actor is registered
    key: String,
    /// The type ID of the actor for type safety checks
    type_id: TypeId,
}

impl PartialEq for Registration {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.type_id == other.type_id
    }
}

impl Eq for Registration {}

impl std::hash::Hash for Registration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.type_id.hash(state);
        self.endpoint.path().hash(state);
    }
}

/// Contains the result of a lookup operation, including all registered endpoints for a key
#[derive(Debug)]
pub struct Listing<T: Actor> {
    /// Vector of all registered endpoints for the given key and type
    pub endpoints: Vec<Endpoint<T>>,
    /// The key used for the lookup
    pub key: Key<T>,
}

/// Message to look up all actors registered under a specific key
pub struct Lookup<T: Actor> {
    /// The key to look up
    pub key: Key<T>,
}

impl<T: Actor> Message for Lookup<T> {
    type Result = Option<Listing<T>>;
}

impl<T> std::fmt::Debug for Lookup<T>
where
    T: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lookup").field("key", &self.key).finish()
    }
}

/// Message to subscribe to registration updates for a specific key
pub struct Subscribe<T: Actor> {
    /// The key to subscribe to
    pub key: Key<T>,
}

impl<T: Actor> Message for Subscribe<T> {
    type Result = Option<ListingSubscription<T>>;
}

impl<T> std::fmt::Debug for Subscribe<T>
where
    T: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscribe").field("key", &self.key).finish()
    }
}

/// Internal type representing a subscription to registration updates
struct Subscriber {
    /// Thread-safe callback function to handle registration updates
    notify: Arc<Box<dyn Fn(AnyListingUpdate) + Send + Sync>>,
}

impl Subscriber {
    /// Creates a new subscriber with a channel for receiving typed updates
    fn new<T: Actor>() -> (Self, mpsc::Receiver<ListingUpdate<T>>) {
        let (tx, rx) = mpsc::channel(32);
        let notify: Arc<Box<dyn Fn(AnyListingUpdate) + Send + Sync>> =
            Arc::new(Box::new(move |update: AnyListingUpdate| {
                if let Some(typed_update) = update.into_typed() {
                    let _ = tx.try_send(typed_update);
                }
            }));

        (Self { notify }, rx)
    }

    /// Notifies the subscriber of a new registration update
    fn notify(&self, update: AnyListingUpdate) {
        (self.notify)(update)
    }
}

/// A stream of listing updates for a specific key.
/// A subscription to registration updates for a specific key
///
/// Provides a stream of updates about actor registrations and deregistrations.
/// Initially yields all existing registrations before streaming live updates.
pub struct ListingSubscription<T: Actor> {
    /// Queue of initial endpoints to yield as Registered events
    initial: VecDeque<Endpoint<T>>,

    /// The key this subscription is monitoring
    key: Key<T>,

    /// Channel for receiving ongoing subscription updates
    receiver: mpsc::Receiver<ListingUpdate<T>>,
}

impl<T: Actor> ListingSubscription<T> {
    /// Returns the key this subscription is monitoring
    pub fn key(&self) -> &Key<T> {
        return &self.key;
    }

    /// Attempts to get the next update without blocking
    /// Returns None if no update is available
    pub fn some_next(&mut self) -> Option<ListingUpdate<T>> {
        if let Some(endpoint) = self.initial.pop_front() {
            return Some(ListingUpdate::Registered(endpoint));
        }
        self.receiver.try_recv().ok()
    }
}

impl<T: Actor> Stream for ListingSubscription<T> {
    type Item = ListingUpdate<T>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(endpoint) = self.initial.pop_front() {
            return Poll::Ready(Some(ListingUpdate::Registered(endpoint)));
        }

        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

/// The core actor that manages service discovery and registration
pub struct ReceptionistActor {
    /// Nested map of type ID -> key -> set of registrations
    registrations: HashMap<TypeId, HashMap<&'static str, HashSet<Registration>>>,
    /// Nested map of type ID -> key -> list of subscribers
    subscriptions: HashMap<TypeId, HashMap<&'static str, Vec<Subscriber>>>,
}

impl Default for ReceptionistActor {
    fn default() -> Self {
        Self {
            registrations: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }
}

impl ReceptionistActor {}

// Actor Implementation for Receptionist

impl Actor for ReceptionistActor {}

/// Message indicating successful registration of an actor
pub struct Registered<T: Actor> {
    /// The endpoint of the registered actor
    pub actor: Endpoint<T>,
    /// The key under which the actor was registered
    pub key: Key<T>,
}

/// Message to register an actor with the receptionist
pub struct Register<T: Actor> {
    /// The endpoint of the actor to register
    pub endpoint: Endpoint<T>,
    /// The key under which to register the actor
    pub key: Key<T>,
}

impl<T: Actor> Message for Register<T> {
    type Result = bool;
}

impl<T> std::fmt::Debug for Register<T>
where
    T: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Register")
            .field("key", &self.key)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl<T: Actor> Handler<Register<T>> for ReceptionistActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: Register<T>) -> bool {
        let type_id = TypeId::of::<T>();
        let key = msg.key.id;
        let registration = Registration {
            endpoint: msg.endpoint.clone().into(),
            key: key.to_string(),
            type_id,
        };

        self.registrations
            .entry(type_id)
            .or_default()
            .entry(msg.key.id)
            .or_default()
            .insert(registration);

        // Notify subscribers
        if let Some(subs) = self.subscriptions.get(&type_id) {
            if let Some(subscribers) = subs.get(msg.key.id) {
                for subscriber in subscribers {
                    subscriber.notify(AnyListingUpdate::Registered(msg.endpoint.clone().into()));
                }
            }
        }

        true
    }
}

pub struct Deregister<T: Actor> {
    pub endpoint: Endpoint<T>,
    pub key: Key<T>,
}

impl<T: Actor> Message for Deregister<T> {
    type Result = bool;
}

impl<T> std::fmt::Debug for Deregister<T>
where
    T: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deregister")
            .field("key", &self.key)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl<T: Actor> Handler<Deregister<T>> for ReceptionistActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: Deregister<T>) -> bool {
        let type_id = TypeId::of::<T>();
        let key = msg.key.id;

        let registration = Registration {
            endpoint: msg.endpoint.clone().into(),
            key: key.to_string(),
            type_id,
        };

        // Deregister the given endpoint for the given key
        self.registrations
            .get_mut(&type_id)
            .and_then(|regs| regs.get_mut(key))
            .and_then(|set| set.remove(&registration).then_some(()))
            .and_then(|_| {
                // Notify subscribers
                if let Some(subs) = self.subscriptions.get(&type_id) {
                    if let Some(subscribers) = subs.get(msg.key.id) {
                        for subscriber in subscribers {
                            subscriber.notify(AnyListingUpdate::Deregistered(
                                msg.endpoint.clone().into(),
                            ));
                        }
                    }
                }
                Some(())
            })
            // Return whether the endpoint was deregistered
            .is_some()
    }
}
impl<T: Actor> Handler<Lookup<T>> for ReceptionistActor {
    fn handle(&mut self, _: &mut Context<Self>, msg: Lookup<T>) -> Option<Listing<T>> {
        let type_id = TypeId::of::<T>();
        let key = msg.key;

        tracing::debug!(
            "Looking up actors for type '{}' with key '{}'",
            std::any::type_name::<T>(),
            key.id
        );

        let actors = self
            .registrations
            .get(&type_id)
            .and_then(|reg| reg.get(&key.id))
            .map(|set| {
                let endpoints: Vec<_> = set
                    .iter()
                    .filter_map(|reg| reg.endpoint.downcast().cloned())
                    .collect();
                tracing::debug!(
                    "Found {} endpoints for type '{}' with key '{}'",
                    endpoints.len(),
                    std::any::type_name::<T>(),
                    key.id
                );
                endpoints
            })?;

        Some(Listing {
            endpoints: actors,
            key,
        })
    }
}

impl<T: Actor> Handler<Subscribe<T>> for ReceptionistActor {
    fn handle(
        &mut self,
        _: &mut Context<Self>,
        msg: Subscribe<T>,
    ) -> Option<ListingSubscription<T>> {
        let type_id = TypeId::of::<T>();
        let key = msg.key.clone();

        // Create new subscriber and get its receiver
        let (subscriber, receiver) = Subscriber::new::<T>();

        // Get initial set of endpoints
        let initial: VecDeque<_> = self
            .registrations
            .get(&type_id)
            .and_then(|reg| reg.get(&key.id))
            .map(|set| {
                set.iter()
                    .filter_map(|reg| reg.endpoint.downcast().cloned())
                    .collect()
            })
            .unwrap_or_default();

        // Store the subscriber
        self.subscriptions
            .entry(type_id)
            .or_default()
            .entry(key.id)
            .or_default()
            .push(subscriber);

        Some(ListingSubscription {
            initial,
            key,
            receiver,
        })
    }
}

/// Lookup a single actor by key, if more than one is registered, an error is returned.
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
    SystemError(#[from] ActorError),
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

    pub async fn register<T: Actor>(
        &self,
        key: Key<T>,
        endpoint: &Endpoint<T>,
    ) -> Result<bool, ActorError> {
        self.inner_endpoint
            .send(Register {
                key,
                endpoint: endpoint.clone(),
            })
            .await
    }

    pub async fn deregister<T: Actor>(
        &self,
        key: Key<T>,
        endpoint: &Endpoint<T>,
    ) -> Result<bool, ActorError> {
        self.inner_endpoint
            .send(Deregister {
                key,
                endpoint: endpoint.clone(),
            })
            .await
    }

    pub async fn lookup_one<T: Actor>(&self, key: Key<T>) -> Result<Endpoint<T>, LookupError> {
        let endpoints = self
            .lookup(key)
            .await
            .ok_or(LookupError::NotFound)?
            .endpoints;
        match endpoints.len() {
            0 => Err(LookupError::NotFound),
            1 => Ok(endpoints.into_iter().next().unwrap()),
            _ => Err(LookupError::MultipleFound),
        }
    }

    pub async fn lookup<T: Actor>(&self, key: Key<T>) -> Option<Listing<T>> {
        match self.inner_endpoint.send(Lookup { key: key.clone() }).await {
            Ok(listing) => listing,
            Err(_) => {
                tracing::error!("Failed to lookup actors for key: {:?}", &key);
                None
            }
        }
    }

    pub async fn subscribe<T: Actor>(&self, key: Key<T>) -> Option<ListingSubscription<T>> {
        self.inner_endpoint
            .send(Subscribe { key: key.clone() })
            .await
            .expect("failed to reach receptionist")
    }
}
