use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use tokio::sync::mpsc;

use crate::message::{Message, SendError};
use crate::{
    actor::{Actor, Handler, Registered as RegisteredActor},
    context::Context,
};
use crate::{
    message::RecepientOf,
    system::{AnyEndpoint, Endpoint},
};

#[cfg(test)]
#[path = "receptionist.test.rs"]
mod tests;

/// A raw key used for registering and looking up actors when you
/// cannot utilize a type-safe key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawKey {
    /// The string identifier for this key
    receptionist_key: String,
    /// The unique identifier for this instance
    group_id: String,
}

impl RawKey {
    /// Creates a new raw key with the given type ID and group identifier
    pub fn new(receptionist_id: String) -> Self {
        Self {
            receptionist_key: receptionist_id,
            group_id: "default".to_string(),
        }
    }

    pub fn new_with_id(key: String, group_id: String) -> Self {
        Self {
            receptionist_key: key,
            group_id,
        }
    }
}

impl<T: RegisteredActor> From<&Key<T>> for RawKey {
    fn from(key: &Key<T>) -> Self {
        Self {
            receptionist_key: T::RECEPTIONIST_KEY.to_string(),
            group_id: key.group_id.to_string(),
        }
    }
}
/// A static type-safe key used for registering and looking up actors
///
/// Keys are used to identify groups of actors of the same type registered
/// with the receptionist.
pub struct StaticKey<T: RegisteredActor> {
    /// The unique identifier for this key
    group_id: &'static str,
    /// Phantom data to maintain type information
    _phantom: PhantomData<T>,
}

impl<T: RegisteredActor> StaticKey<T> {
    /// Creates a new key with the given static string identifier
    pub const fn new(group_id: &'static str) -> Self {
        Self {
            group_id,
            _phantom: PhantomData,
        }
    }

    /// Creates a new key with some identifier
    pub const fn default() -> Self {
        Self {
            group_id: "default",
            _phantom: PhantomData,
        }
    }
}

impl<T: RegisteredActor> fmt::Debug for StaticKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticKey")
            .field("family_id", &T::RECEPTIONIST_KEY)
            .finish()
    }
}

impl<T: RegisteredActor> Clone for StaticKey<T> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            _phantom: PhantomData,
        }
    }
}

impl<T: RegisteredActor> From<StaticKey<T>> for RawKey {
    fn from(val: StaticKey<T>) -> Self {
        RawKey::new_with_id(T::RECEPTIONIST_KEY.to_string(), val.group_id.to_string())
    }
}

/// A type-safe key used for registering and looking up actors
///
/// Keys are used to identify groups of actors of the same type registered
/// with the receptionist.
pub struct Key<T: RegisteredActor> {
    /// The unique identifier for this key
    group_id: String,
    /// Phantom data to maintain type information
    _phantom: PhantomData<T>,
}

impl<T: RegisteredActor> Key<T> {
    /// Creates a new key with the given static string identifier
    pub fn new<S: Into<String>>(group_id: S) -> Self {
        Self {
            group_id: group_id.into(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new key with some identifier
    pub fn default() -> Self {
        Self {
            group_id: "default".into(),
            _phantom: PhantomData,
        }
    }
}

impl<T: RegisteredActor> fmt::Debug for Key<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Key")
            .field("family_id", &T::RECEPTIONIST_KEY)
            .finish()
    }
}

impl<T: RegisteredActor> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: RegisteredActor> From<Key<T>> for RawKey {
    fn from(val: Key<T>) -> Self {
        RawKey::new_with_id(T::RECEPTIONIST_KEY.to_string(), val.group_id)
    }
}

impl<T: RegisteredActor> From<StaticKey<T>> for Key<T> {
    fn from(key: StaticKey<T>) -> Self {
        Self {
            group_id: key.group_id.to_string(),
            _phantom: PhantomData,
        }
    }
}

/// Updates sent to subscribers when actors are registered or deregistered
pub enum ListingUpdate<T: RegisteredActor> {
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
    fn into_typed<T: RegisteredActor>(self) -> Option<ListingUpdate<T>> {
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
    /// The instance id for this actor instance
    group_id: String,
}

impl PartialEq for Registration {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Registration {}

impl std::hash::Hash for Registration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.endpoint.path().hash(state);
    }
}

/// Message to subscribe to registration updates for a specific key
pub struct Subscribe<T: RegisteredActor> {
    /// The key to subscribe to
    pub key: RawKey,

    /// The type of the actor to subscribe to
    pub _phantom: PhantomData<T>,
}

impl<T: RegisteredActor> Message for Subscribe<T> {
    type Result = Option<ListingSubscription<T>>;
}

impl<T> std::fmt::Debug for Subscribe<T>
where
    T: RegisteredActor,
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
    fn new<T: RegisteredActor>() -> (Self, mpsc::Receiver<ListingUpdate<T>>) {
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
pub struct ListingSubscription<T: RegisteredActor> {
    /// Queue of initial endpoints to yield as Registered events
    initial: VecDeque<Endpoint<T>>,

    /// The key this subscription is monitoring
    key: RawKey,

    /// Channel for receiving ongoing subscription updates
    receiver: mpsc::Receiver<ListingUpdate<T>>,
}

impl<T: RegisteredActor> ListingSubscription<T> {
    /// Returns the key this subscription is monitoring
    pub fn key(&self) -> Key<T> {
        Key::new(self.key.group_id.clone())
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

impl<T: RegisteredActor> Stream for ListingSubscription<T> {
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
#[derive(Default)]
pub struct ReceptionistActor {
    /// Nested map of type ID -> key -> set of registrations
    registrations: HashMap<String, HashMap<String, HashSet<Registration>>>,
    /// Nested map of type ID -> key -> list of subscribers
    subscriptions: HashMap<String, HashMap<String, Vec<Subscriber>>>,
}

impl ReceptionistActor {}

// Actor Implementation for Receptionist

impl Actor for ReceptionistActor {}

/// Message indicating successful registration of an actor
pub struct Registered<T: RegisteredActor> {
    /// The endpoint of the registered actor
    pub actor: Endpoint<T>,
    /// The key under which the actor was registered
    pub key: RawKey,
}

/// Message to register an actor with the receptionist
pub struct Register<T: RegisteredActor> {
    /// The endpoint of the actor to register
    pub endpoint: Endpoint<T>,
    /// The key under which to register the actor
    pub key: RawKey,
}

impl<T: RegisteredActor> Message for Register<T> {
    type Result = bool;
}

impl<T> std::fmt::Debug for Register<T>
where
    T: RegisteredActor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Register")
            .field("key", &self.key)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait::async_trait]
impl<T: RegisteredActor> Handler<Register<T>> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Register<T>) -> bool {
        let key = T::RECEPTIONIST_KEY.to_string();
        let group_id = msg.key.group_id;
        let registration = Registration {
            endpoint: msg.endpoint.clone().into(),
            key: key.clone(),
            group_id: group_id.clone(),
        };

        self.registrations
            .entry(key.clone())
            .or_default()
            .entry(group_id.clone())
            .or_default()
            .insert(registration);

        // Notify subscribers
        if let Some(subs) = self.subscriptions.get(&key) {
            if let Some(subscribers) = subs.get(&group_id) {
                for subscriber in subscribers {
                    subscriber.notify(AnyListingUpdate::Registered(msg.endpoint.clone().into()));
                }
            }
        }

        true
    }
}

pub struct Deregister<T: RegisteredActor> {
    pub endpoint: Endpoint<T>,
    pub key: RawKey,
}

impl<T: RegisteredActor> Message for Deregister<T> {
    type Result = bool;
}

impl<T> std::fmt::Debug for Deregister<T>
where
    T: RegisteredActor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deregister")
            .field("key", &self.key)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait::async_trait]
impl<T: RegisteredActor> Handler<Deregister<T>> for ReceptionistActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Deregister<T>) -> bool {
        let key = T::RECEPTIONIST_KEY.to_string();
        let group_id = msg.key.group_id;

        let registration = Registration {
            endpoint: msg.endpoint.clone().into(),
            key: key.to_string(),
            group_id: group_id.clone(),
        };

        // Deregister the given endpoint for the given key
        self.registrations
            .get_mut(&key)
            .and_then(|regs| regs.get_mut(&group_id))
            .and_then(|set| set.remove(&registration).then_some(()))
            .map(|_| {
                // Notify subscribers
                if let Some(subs) = self.subscriptions.get(&key) {
                    if let Some(subscribers) = subs.get(&group_id) {
                        for subscriber in subscribers {
                            subscriber.notify(AnyListingUpdate::Deregistered(
                                msg.endpoint.clone().into(),
                            ));
                        }
                    }
                }
                ()
            })
            // Return whether the endpoint was deregistered
            .is_some()
    }
}

/// Contains the result of a lookup operation, including all registered endpoints for a key
#[derive(Debug)]
pub struct Listing<T: RegisteredActor> {
    /// Vector of all registered endpoints for the given key and type
    pub endpoints: Vec<Endpoint<T>>,
    /// The key used for the lookup
    pub key: RawKey,
}

/// Message to look up all actors registered under a specific key
pub struct Lookup<T: RegisteredActor> {
    /// The key to look up
    pub key: RawKey,
    /// The type of the actor to look up
    pub _phantom: PhantomData<T>,
}

impl<T: RegisteredActor> Message for Lookup<T> {
    type Result = Option<Listing<T>>;
}

impl<T> std::fmt::Debug for Lookup<T>
where
    T: RegisteredActor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lookup").field("key", &self.key).finish()
    }
}

#[async_trait::async_trait]
impl<T: RegisteredActor> Handler<Lookup<T>> for ReceptionistActor {
    async fn handle(&mut self, _: &mut Context<Self>, msg: Lookup<T>) -> Option<Listing<T>> {
        let key = T::RECEPTIONIST_KEY.to_string();

        tracing::debug!(
            "Looking up actors for family '{}' with key '{}'",
            key,
            msg.key.group_id
        );

        let actors = self
            .registrations
            .get(&key)
            .and_then(|reg| reg.get(&msg.key.group_id))
            .map(|set| {
                let endpoints: Vec<_> = set
                    .iter()
                    .filter_map(|reg| reg.endpoint.downcast())
                    .collect();
                tracing::debug!(
                    "Found {} endpoints for type '{}' with key '{}'",
                    endpoints.len(),
                    key,
                    msg.key.group_id
                );
                endpoints
            })?;

        Some(Listing {
            endpoints: actors,
            key: msg.key,
        })
    }
}
/// Contains the result of a lookup operation, including all registered endpoints for a key
#[derive(Debug)]
pub struct RecepientListing<M: Message> {
    /// Vector of all registered endpoints for the given key and type
    pub recepients: Vec<RecepientOf<M>>,
    /// The key used for the lookup
    pub key: RawKey,
}

/// Message to look up all actors registered under a specific key
#[derive(Debug)]
pub struct RecepientLookup<M: Message> {
    /// The key to look up
    pub key: RawKey,
    /// The message type for the recepient
    pub _phantom: PhantomData<M>,
}

impl<M: Message> Message for RecepientLookup<M> {
    type Result = Option<RecepientListing<M>>;
}

#[async_trait::async_trait]
impl<M: Message> Handler<RecepientLookup<M>> for ReceptionistActor {
    async fn handle(
        &mut self,
        _: &mut Context<Self>,
        msg: RecepientLookup<M>,
    ) -> Option<RecepientListing<M>> {
        let key = msg.key.receptionist_key.clone();
        let group_id = msg.key.group_id.clone();

        tracing::debug!(
            "Looking up actors for family '{}' with key '{}'",
            key,
            group_id
        );

        let recepients = self
            .registrations
            .get(&key)
            .and_then(|reg| reg.get(&group_id))
            .map(|set| {
                let endpoints: Vec<_> = set
                    .iter()
                    .filter_map(|reg| reg.endpoint.as_recepient_of())
                    .collect();
                tracing::debug!(
                    "Found {} endpoints for type '{}' with key '{}'",
                    endpoints.len(),
                    key,
                    group_id
                );
                endpoints
            })?;

        Some(RecepientListing {
            recepients,
            key: msg.key,
        })
    }
}

#[async_trait::async_trait]
impl<T: RegisteredActor> Handler<Subscribe<T>> for ReceptionistActor {
    async fn handle(
        &mut self,
        _: &mut Context<Self>,
        msg: Subscribe<T>,
    ) -> Option<ListingSubscription<T>> {
        let registration_key = T::RECEPTIONIST_KEY.to_string();
        let key = msg.key.clone();

        // Create new subscriber and get its receiver
        let (subscriber, receiver) = Subscriber::new::<T>();

        // Get initial set of endpoints
        let initial: VecDeque<_> = self
            .registrations
            .get(&registration_key)
            .and_then(|reg| reg.get(&key.group_id))
            .map(|set| {
                set.iter()
                    .filter_map(|reg| reg.endpoint.downcast())
                    .collect()
            })
            .unwrap_or_default();

        // Store the subscriber
        self.subscriptions
            .entry(registration_key)
            .or_default()
            .entry(key.group_id.clone())
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

    pub async fn register<T: RegisteredActor>(
        &self,
        key: Key<T>,
        endpoint: &Endpoint<T>,
    ) -> Result<bool, SendError> {
        self.inner_endpoint
            .send(Register {
                key: key.into(),
                endpoint: endpoint.clone(),
            })
            .await
    }

    pub async fn deregister<T: RegisteredActor>(
        &self,
        key: Key<T>,
        endpoint: &Endpoint<T>,
    ) -> Result<bool, SendError> {
        self.inner_endpoint
            .send(Deregister {
                key: key.into(),
                endpoint: endpoint.clone(),
            })
            .await
    }

    pub async fn lookup_one_recepient_of<M: Message>(
        &self,
        key: RawKey,
    ) -> Result<RecepientOf<M>, LookupError> {
        let recepients = self
            .lookup_recepients_of(key)
            .await
            .ok_or(LookupError::NotFound)?
            .recepients;
        match recepients.len() {
            0 => Err(LookupError::NotFound),
            1 => Ok(recepients.into_iter().next().unwrap()),
            _ => Err(LookupError::MultipleFound),
        }
    }
    pub async fn lookup_recepients_of<M: Message>(
        &self,
        key: RawKey,
    ) -> Option<RecepientListing<M>> {
        match self
            .inner_endpoint
            .send(RecepientLookup {
                key: key.clone(),
                _phantom: PhantomData,
            })
            .await
        {
            Ok(listing) => listing,
            Err(_) => {
                tracing::error!("Failed to lookup actors for key: {:?}", &key);
                None
            }
        }
    }

    pub async fn lookup_one<T: RegisteredActor>(
        &self,
        key: Key<T>,
    ) -> Result<Endpoint<T>, LookupError> {
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

    pub async fn lookup<T: RegisteredActor>(&self, key: Key<T>) -> Option<Listing<T>> {
        match self
            .inner_endpoint
            .send(Lookup {
                key: key.clone().into(),
                _phantom: PhantomData,
            })
            .await
        {
            Ok(listing) => listing,
            Err(_) => {
                tracing::error!("Failed to lookup actors for key: {:?}", &key);
                None
            }
        }
    }

    pub async fn subscribe<T: RegisteredActor>(
        &self,
        key: Key<T>,
    ) -> Option<ListingSubscription<T>> {
        self.inner_endpoint
            .send(Subscribe {
                key: key.clone().into(),
                _phantom: PhantomData,
            })
            .await
            .expect("failed to reach receptionist")
    }
}
