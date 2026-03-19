//! POC Framework: typed endpoints, receptionist with type erasure, local/remote transparency.
//!
//! Key design decisions:
//! - Endpoint<A> abstracts local vs remote — caller never knows which
//! - All messages through endpoints require RemoteMessage (serialization bounds)
//! - Receptionist stores type-erased entries, typed lookup via caller-supplied generic
//! - Lifecycle events are fully type-erased (no actor type parameter)
//! - Internal downcast is guarded by TypeId, never exposed to user
//! - ReceptionKey<A> groups actors by type for subscription-based discovery
//! - Listing<A> provides async streams of endpoints as actors register
//! - OpLog replication protocol enables cross-node registry sync
//! - Delayed flush and blip avoidance optimize network efficiency

use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

// Re-export bincode so generated code can reference it without the user
// needing bincode in their Cargo.toml
pub mod __reexport {
    pub use bincode;
}

pub mod cluster;

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
    label: String,
    node_id: String,
    mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
    receptionist: Receptionist,
    system_tx: mpsc::UnboundedSender<SystemSignal>,
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

// =============================================================================
// LIFECYCLE & RESTART TYPES
// =============================================================================

/// Why an actor terminated — used by watches and restart policies.
#[derive(Debug, Clone)]
pub enum TerminationReason {
    /// Clean shutdown (receptionist.stop() or channel close)
    Stopped,
    /// Handler returned error (future use)
    Failed(String),
    /// catch_unwind caught a panic
    Panicked(String),
    /// Restart limit exceeded within the configured time window
    RestartLimitExceeded,
}

/// Notification delivered to watchers when a watched actor terminates.
#[derive(Debug, Clone)]
pub struct ActorTerminated {
    pub label: String,
    pub reason: TerminationReason,
}

/// Restart strategy for an actor — mirrors Erlang/OTP child spec strategies.
#[derive(Debug, Clone, Copy, Default)]
pub enum RestartPolicy {
    /// Never restart (current behavior)
    #[default]
    Temporary,
    /// Restart only on crash (panic), not clean stop
    Transient,
    /// Always restart regardless of reason
    Permanent,
}

/// Configuration for restart limits and backoff behavior.
#[derive(Debug, Clone)]
pub struct RestartConfig {
    pub policy: RestartPolicy,
    /// Maximum number of restarts allowed within the rolling `window`.
    pub max_restarts: u32,
    /// Rolling time window for counting restarts.
    pub window: Duration,
    pub backoff: BackoffConfig,
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            policy: RestartPolicy::Temporary,
            max_restarts: 5,
            window: Duration::from_secs(60),
            backoff: BackoffConfig::default(),
        }
    }
}

/// Exponential backoff configuration for actor restarts.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before the first restart.
    pub initial: Duration,
    /// Maximum delay between restarts.
    pub max: Duration,
    /// Multiplier applied after each restart.
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

/// Factory for creating actor instances on restart.
/// `&mut self` allows stateful factories (e.g. incrementing restart counters).
pub trait ActorFactory: Send + 'static {
    type Actor: Actor + RemoteDispatch;
    fn create(&mut self) -> (Self::Actor, <Self::Actor as Actor>::State);
}

/// Internal: signals delivered to actors from the system.
enum SystemSignal {
    ActorTerminated(ActorTerminated),
}

/// Internal: a watch entry stored in the receptionist.
struct WatchEntry {
    #[allow(dead_code)]
    watcher_label: String,
    system_tx: mpsc::UnboundedSender<SystemSignal>,
}

// =============================================================================
// WIRE TYPES
// =============================================================================

/// What crosses the wire — analogous to Swift's InvocationMessage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteInvocation {
    pub call_id: u64,
    pub actor_label: String,
    pub message_type: String,
    pub payload: Vec<u8>,
}

/// Response from the remote side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteResponse {
    pub call_id: u64,
    pub result: Result<Vec<u8>, String>,
}

/// Internal: routes an incoming remote invocation to a local actor's supervisor.
pub struct DispatchRequest {
    pub invocation: RemoteInvocation,
    pub respond_to: oneshot::Sender<RemoteResponse>,
}

// =============================================================================
// SEND ERROR
// =============================================================================

#[derive(Debug)]
pub enum SendError {
    MailboxClosed,
    ResponseDropped,
    WireClosed,
    SerializationFailed(String),
    DeserializationFailed(String),
    RemoteError(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for SendError {}

// =============================================================================
// ENVELOPE — type-erased local message dispatch
// =============================================================================

/// Type-erased message that can be dispatched to an actor.
pub trait EnvelopeProxy<A: Actor>: Send {
    fn handle(self: Box<Self>, ctx: &ActorContext<A>, actor: &mut A, state: &mut A::State);
}

struct TypedEnvelope<M: Message> {
    message: M,
    respond_to: oneshot::Sender<M::Result>,
}

impl<A, M> EnvelopeProxy<A> for TypedEnvelope<M>
where
    A: Handler<M>,
    M: Message,
{
    fn handle(self: Box<Self>, ctx: &ActorContext<A>, actor: &mut A, state: &mut A::State) {
        let result = actor.handle(ctx, state, self.message);
        let _ = self.respond_to.send(result);
    }
}

// =============================================================================
// RECEPTION KEY — typed group key for actor discovery
// =============================================================================

/// A typed key for grouping actors by type + group name.
/// Multiple actors can share the same key. Used with `listing()` for
/// subscription-based discovery (like Swift's `DistributedReception.Key<Guest>`).
pub struct ReceptionKey<A: Actor> {
    id: String,
    _marker: PhantomData<A>,
}

impl<A: Actor> ReceptionKey<A> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<A: Actor> Clone for ReceptionKey<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A: Actor> Debug for ReceptionKey<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceptionKey<{}>({:?})",
            std::any::type_name::<A>(),
            self.id
        )
    }
}

/// Type-erased key for internal storage.
#[derive(Clone, Debug)]
struct ErasedReceptionKey {
    id: String,
    type_id: TypeId,
}

// =============================================================================
// LISTING — async stream of endpoints for a reception key
// =============================================================================

/// An async stream of endpoints matching a reception key.
/// Returns existing matches immediately (backfill), then streams new ones
/// as actors register with the matching key.
///
/// Analogous to Swift's `GuestListing<Guest>: AsyncSequence`.
pub struct Listing<A: Actor> {
    rx: mpsc::UnboundedReceiver<Endpoint<A>>,
}

impl<A: Actor> Listing<A> {
    /// Await the next endpoint. Returns None when the subscription is closed.
    pub async fn next(&mut self) -> Option<Endpoint<A>> {
        self.rx.recv().await
    }

    /// Try to receive without blocking. Returns None if no endpoint is ready.
    pub fn try_next(&mut self) -> Option<Endpoint<A>> {
        self.rx.try_recv().ok()
    }
}

/// Type-erased listing sender stored in the receptionist.
/// Created when listing() is called, notified when matching actors register.
trait ErasedListingSender: Send + Sync {
    fn key_id(&self) -> &str;
    fn actor_type_id(&self) -> TypeId;
    fn try_send_from_entry(&self, entry: &ActorEntry) -> bool;
    fn is_closed(&self) -> bool;
}

struct TypedListingSender<A: Actor + 'static> {
    key_id: String,
    tx: mpsc::UnboundedSender<Endpoint<A>>,
}

impl<A: Actor + 'static> ErasedListingSender for TypedListingSender<A> {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn try_send_from_entry(&self, entry: &ActorEntry) -> bool {
        if entry.type_id != TypeId::of::<A>() {
            return true; // wrong type, keep subscription alive
        }

        let endpoint = match &entry.location {
            EntryLocation::Local {
                endpoint_factory, ..
            } => {
                let any_ep = endpoint_factory.create_endpoint_any();
                match any_ep.downcast::<Endpoint<A>>() {
                    Ok(ep) => *ep,
                    Err(_) => return true,
                }
            }
            EntryLocation::Remote {
                wire_tx,
                response_registry,
            } => Endpoint::remote(
                entry.label.clone(),
                wire_tx.clone(),
                response_registry.clone(),
            ),
        };
        self.tx.send(endpoint).is_ok()
    }
}

// =============================================================================
// ENDPOINT — the user-facing send handle (abstracts local vs remote)
// =============================================================================

pub struct Endpoint<A: Actor> {
    inner: EndpointInner<A>,
}

impl<A: Actor> Clone for Endpoint<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

enum EndpointInner<A: Actor> {
    Local {
        mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
    },
    Remote {
        label: String,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    },
}

impl<A: Actor> Clone for EndpointInner<A> {
    fn clone(&self) -> Self {
        match self {
            Self::Local { mailbox_tx } => Self::Local {
                mailbox_tx: mailbox_tx.clone(),
            },
            Self::Remote {
                label,
                wire_tx,
                response_registry,
            } => Self::Remote {
                label: label.clone(),
                wire_tx: wire_tx.clone(),
                response_registry: response_registry.clone(),
            },
        }
    }
}

impl<A: Actor + 'static> Endpoint<A> {
    fn local(mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>) -> Self {
        Self {
            inner: EndpointInner::Local { mailbox_tx },
        }
    }

    fn remote(
        label: String,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    ) -> Self {
        Self {
            inner: EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            },
        }
    }

    /// Send a message to the actor. **Identical API for local and remote.**
    ///
    /// Compile-time checked:
    /// - A must implement Handler<M>
    /// - M must implement RemoteMessage (serializable)
    ///
    /// For local actors: uses the envelope pattern (zero serialization cost).
    /// For remote actors: serializes, sends over wire, awaits response.
    pub async fn send<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage,
        M::Result: Serialize + DeserializeOwned,
    {
        match &self.inner {
            EndpointInner::Local { mailbox_tx } => {
                let (tx, rx) = oneshot::channel();
                let envelope = TypedEnvelope {
                    message,
                    respond_to: tx,
                };
                mailbox_tx
                    .send(Box::new(envelope))
                    .map_err(|_| SendError::MailboxClosed)?;
                rx.await.map_err(|_| SendError::ResponseDropped)
            }
            EndpointInner::Remote {
                label,
                wire_tx,
                response_registry,
            } => {
                let call_id = next_call_id();
                let payload = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                    .map_err(|e| SendError::SerializationFailed(e.to_string()))?;

                let (resp_tx, resp_rx) = oneshot::channel();
                response_registry.register(call_id, resp_tx);

                let invocation = RemoteInvocation {
                    call_id,
                    actor_label: label.clone(),
                    message_type: M::TYPE_ID.to_string(),
                    payload,
                };

                wire_tx
                    .send(invocation)
                    .map_err(|_| SendError::WireClosed)?;

                let response = resp_rx.await.map_err(|_| SendError::ResponseDropped)?;
                let result_bytes = response.result.map_err(SendError::RemoteError)?;
                let (result, _): (M::Result, _) =
                    bincode::serde::decode_from_slice(&result_bytes, bincode::config::standard())
                        .map_err(|e| SendError::DeserializationFailed(e.to_string()))?;
                Ok(result)
            }
        }
    }
}

// =============================================================================
// ROUTER — distribute messages across a pool of actor endpoints
// =============================================================================

/// Strategy for how a Router distributes messages across its endpoints.
#[derive(Debug, Clone, Default)]
pub enum RoutingStrategy {
    #[default]
    RoundRobin,
    Random,
    Broadcast,
}

/// A router distributes messages across a group of endpoints for the same actor type.
///
/// Useful for load balancing, fan-out, and work distribution patterns.
pub struct Router<A: Actor> {
    endpoints: Vec<Endpoint<A>>,
    strategy: RoutingStrategy,
    counter: AtomicUsize,
}

impl<A: Actor + 'static> Router<A> {
    pub fn new(endpoints: Vec<Endpoint<A>>, strategy: RoutingStrategy) -> Self {
        Self {
            endpoints,
            strategy,
            counter: AtomicUsize::new(0),
        }
    }

    /// Send a message to one endpoint based on the routing strategy.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return Err(SendError::MailboxClosed);
        }

        match self.strategy {
            RoutingStrategy::RoundRobin => {
                let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Random => {
                use rand::Rng;
                let idx = rand::rng().random_range(0..self.endpoints.len());
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Broadcast => {
                // For broadcast via send(), just send to the first endpoint
                self.endpoints[0].send(msg).await
            }
        }
    }

    /// Send a message to ALL endpoints. Returns a Vec of results.
    pub async fn broadcast<M>(&self, msg: M) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let mut results = Vec::with_capacity(self.endpoints.len());
        for ep in &self.endpoints {
            results.push(ep.send(msg.clone()).await);
        }
        results
    }

    pub fn add(&mut self, endpoint: Endpoint<A>) {
        self.endpoints.push(endpoint);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.endpoints.len() {
            self.endpoints.remove(index);
        }
    }

    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }
}

// =============================================================================
// RESPONSE REGISTRY — correlates wire responses to in-flight calls
// =============================================================================

#[derive(Clone)]
pub struct ResponseRegistry {
    inner: Arc<Mutex<HashMap<u64, oneshot::Sender<RemoteResponse>>>>,
}

impl ResponseRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, call_id: u64, tx: oneshot::Sender<RemoteResponse>) {
        self.inner.lock().unwrap().insert(call_id, tx);
    }

    pub fn complete(&self, response: RemoteResponse) {
        if let Some(tx) = self.inner.lock().unwrap().remove(&response.call_id) {
            let _ = tx.send(response);
        }
    }

    /// Fail all pending responses — called when the connection is lost.
    /// Each pending caller receives an error response so they surface the
    /// failure immediately instead of waiting forever.
    pub fn fail_all(&self, error_msg: &str) {
        let mut map = self.inner.lock().unwrap();
        for (call_id, tx) in map.drain() {
            let _ = tx.send(RemoteResponse {
                call_id,
                result: Err(error_msg.to_string()),
            });
        }
    }
}

impl Default for ResponseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// OPLOG — operation log for distributed receptionist replication (Phase 2)
// =============================================================================

/// Operation types recorded in the receptionist's operation log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    Register {
        label: String,
        actor_type_name: String,
        key_ids: Vec<String>,
        /// "host:port" of the node where the actor lives.
        origin_addr: String,
    },
    Remove {
        label: String,
    },
}

/// A single operation in the log, tagged with sequence number and originating node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    pub seq: u64,
    pub node_id: String,
    pub op_type: OpType,
}

/// The operation log. Each receptionist maintains its own log of local operations.
/// Peers exchange ops to converge on the same view of the actor registry.
pub struct OpLog {
    ops: Vec<Op>,
    next_seq: u64,
    node_id: String,
}

impl OpLog {
    fn new(node_id: String) -> Self {
        Self {
            ops: Vec::new(),
            next_seq: 1,
            node_id,
        }
    }

    fn append(&mut self, op_type: OpType) -> Op {
        let op = Op {
            seq: self.next_seq,
            node_id: self.node_id.clone(),
            op_type,
        };
        self.next_seq += 1;
        self.ops.push(op.clone());
        op
    }

    fn ops_since(&self, seq: u64) -> &[Op] {
        let start = self.ops.partition_point(|op| op.seq <= seq);
        &self.ops[start..]
    }

    fn latest_seq(&self) -> u64 {
        if self.next_seq > 1 {
            self.next_seq - 1
        } else {
            0
        }
    }
}

// =============================================================================
// VERSION VECTOR — tracks per-node replication progress (Phase 2)
// =============================================================================

/// Tracks the latest sequence number seen from each node.
/// Used to deduplicate operations during replication.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VersionVector {
    versions: HashMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.versions.get(node_id).copied().unwrap_or(0)
    }

    pub fn update(&mut self, node_id: &str, seq: u64) {
        let entry = self.versions.entry(node_id.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (node_id, &seq) in &other.versions {
            self.update(node_id, seq);
        }
    }
}

// =============================================================================
// RECEPTIONIST — type-erased actor registry with typed lookup
// =============================================================================

/// Lifecycle events. Fully type-erased — subscribers don't need to know the
/// actor type. If they want an endpoint, they call lookup::<A>() with the
/// type they expect.
#[derive(Debug, Clone)]
pub enum ActorEvent {
    Registered { label: String, actor_type: String },
    Deregistered { label: String, actor_type: String },
}

/// Type-erased endpoint factory stored in the receptionist.
/// Created at registration time when the actor type is known.
/// Used at lookup time to produce a typed Endpoint<A>.
trait ErasedEndpointFactory: Send + Sync {
    fn actor_type_id(&self) -> TypeId;
    fn create_endpoint_any(&self) -> Box<dyn Any + Send>;
}

struct LocalEndpointFactory<A: Actor> {
    mailbox_tx: mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A>>>,
}

impl<A: Actor + 'static> ErasedEndpointFactory for LocalEndpointFactory<A> {
    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }

    fn create_endpoint_any(&self) -> Box<dyn Any + Send> {
        Box::new(Endpoint::<A>::local(self.mailbox_tx.clone()))
    }
}

/// What the receptionist stores per actor. No actor type parameter.
struct ActorEntry {
    label: String,
    type_id: TypeId,
    actor_type_name: String,
    location: EntryLocation,
    keys: Vec<ErasedReceptionKey>,
    node_id: String,
}

enum EntryLocation {
    Local {
        /// Produces typed Endpoint<A> at lookup time (via internal downcast)
        endpoint_factory: Box<dyn ErasedEndpointFactory>,
        /// Non-generic channel for routing incoming remote messages
        dispatch_tx: mpsc::UnboundedSender<DispatchRequest>,
        /// Shutdown signal for the supervisor
        shutdown_tx: Option<oneshot::Sender<()>>,
    },
    Remote {
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    },
}

/// Receptionist configuration for Phase 3 features.
#[derive(Debug, Clone)]
pub struct ReceptionistConfig {
    pub node_id: String,
    /// "host:port" of this node — embedded in Register ops so remote nodes
    /// can route back to the actor's home node.
    pub origin_addr: String,
    /// If set, listing notifications are batched and flushed on this interval.
    pub flush_interval: Option<Duration>,
    /// If set, oplog entries are delayed by this window. If the actor deregisters
    /// before the window expires, the op is never committed (blip avoidance).
    pub blip_window: Option<Duration>,
}

impl Default for ReceptionistConfig {
    fn default() -> Self {
        Self {
            node_id: "local".to_string(),
            origin_addr: "127.0.0.1:0".to_string(),
            flush_interval: None,
            blip_window: None,
        }
    }
}

/// Deregister guard — dropped when the supervisor exits, triggering auto-deregister.
/// Carries the termination reason so watches receive accurate information.
struct DeregisterGuard {
    receptionist: Receptionist,
    label: String,
    reason: Arc<Mutex<TerminationReason>>,
}

impl Drop for DeregisterGuard {
    fn drop(&mut self) {
        let reason = self.reason.lock().unwrap().clone();
        self.receptionist
            .deregister_with_reason(&self.label, reason);
    }
}

/// Pending listing notification for delayed flush (Phase 3).
struct PendingListingNotification {
    label: String,
    key_id: String,
    type_id: TypeId,
}

struct ReceptionistInner {
    config: ReceptionistConfig,
    entries: Mutex<HashMap<String, ActorEntry>>,
    event_subscribers: Mutex<Vec<mpsc::UnboundedSender<ActorEvent>>>,
    listing_subscribers: Mutex<Vec<Box<dyn ErasedListingSender>>>,
    oplog: Mutex<OpLog>,
    observed_versions: Mutex<VersionVector>,
    watches: Mutex<HashMap<String, Vec<WatchEntry>>>,
    // Phase 3
    pending_notifications: Mutex<Vec<PendingListingNotification>>,
    blip_pending: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// The receptionist: type-erased actor registry with typed lookup,
/// subscription-based discovery, and distributed replication support.
///
/// Cheap to clone (wraps Arc).
#[derive(Clone)]
pub struct Receptionist {
    inner: Arc<ReceptionistInner>,
}

impl Receptionist {
    pub fn new() -> Self {
        Self::with_config(ReceptionistConfig::default())
    }

    pub fn with_node_id(node_id: impl Into<String>) -> Self {
        Self::with_config(ReceptionistConfig {
            node_id: node_id.into(),
            ..Default::default()
        })
    }

    pub fn with_config(config: ReceptionistConfig) -> Self {
        let node_id = config.node_id.clone();
        let flush_interval = config.flush_interval;

        let receptionist = Self {
            inner: Arc::new(ReceptionistInner {
                config,
                entries: Mutex::new(HashMap::new()),
                event_subscribers: Mutex::new(Vec::new()),
                listing_subscribers: Mutex::new(Vec::new()),
                oplog: Mutex::new(OpLog::new(node_id)),
                observed_versions: Mutex::new(VersionVector::new()),
                watches: Mutex::new(HashMap::new()),
                pending_notifications: Mutex::new(Vec::new()),
                blip_pending: Mutex::new(HashMap::new()),
            }),
        };

        // Spawn the flush task if configured (Phase 3)
        if let Some(interval) = flush_interval {
            let r = receptionist.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    r.flush_pending_notifications();
                }
            });
        }

        receptionist
    }

    pub fn node_id(&self) -> &str {
        &self.inner.config.node_id
    }

    // =========================================================================
    // CORE REGISTRATION & LOOKUP
    // =========================================================================

    /// Start a local actor, register it, and return its typed endpoint.
    /// Uses `Temporary` restart policy (never restarts).
    pub fn start<A>(&self, label: &str, actor: A, state: A::State) -> Endpoint<A>
    where
        A: Actor + RemoteDispatch + 'static,
    {
        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<A>>>();
        let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        // Create deregister guard for auto-cleanup on supervisor exit
        let guard = DeregisterGuard {
            receptionist: self.clone(),
            label: label.to_string(),
            reason: exit_reason.clone(),
        };

        // Build the actor context
        let ctx = ActorContext {
            label: label.to_string(),
            node_id: self.inner.config.node_id.clone(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: self.clone(),
            system_tx,
        };

        // Spawn the supervisor task
        tokio::spawn(async move {
            let _ = run_supervisor(
                actor,
                state,
                ctx,
                mailbox_rx,
                dispatch_rx,
                shutdown_rx,
                system_rx,
                exit_reason,
                guard,
            )
            .await;
        });

        // Create the user's endpoint
        let endpoint = Endpoint::local(mailbox_tx.clone());

        // Register in the receptionist
        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>().to_string(),
            location: EntryLocation::Local {
                endpoint_factory: Box::new(LocalEndpointFactory { mailbox_tx }),
                dispatch_tx,
                shutdown_tx: Some(shutdown_tx),
            },
            keys: Vec::new(),
            node_id: self.inner.config.node_id.clone(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        // Record in oplog (respects blip window if configured)
        self.record_register_op(label, std::any::type_name::<A>(), &[]);

        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<A>().to_string(),
        });

        endpoint
    }

    /// Start an actor with a restart policy and factory.
    ///
    /// The factory is called to create each incarnation of the actor (initial + restarts).
    /// The returned endpoint remains valid across restarts — the underlying mailbox
    /// channel is reused so existing `Endpoint<A>` handles keep working.
    pub fn start_with_policy<F: ActorFactory>(
        &self,
        label: &str,
        factory: F,
        policy: RestartPolicy,
    ) -> Endpoint<F::Actor> {
        self.start_with_config(
            label,
            factory,
            RestartConfig {
                policy,
                ..Default::default()
            },
        )
    }

    /// Start an actor with full restart configuration (limits, backoff, policy).
    ///
    /// Like `start_with_policy`, the returned endpoint remains valid across restarts.
    /// When the restart limit is exceeded within the configured window, the actor
    /// terminates with `TerminationReason::RestartLimitExceeded` and watchers are notified.
    pub fn start_with_config<F: ActorFactory>(
        &self,
        label: &str,
        mut factory: F,
        config: RestartConfig,
    ) -> Endpoint<F::Actor> {
        // Stable channels that survive across restarts
        let (mailbox_tx, mut mailbox_rx) =
            mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<F::Actor>>>();
        let (dispatch_tx, mut dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();

        // Create the user's endpoint (stable across restarts)
        let endpoint = Endpoint::local(mailbox_tx.clone());

        // Initial registration
        let (actor, state) = factory.create();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        let guard = DeregisterGuard {
            receptionist: self.clone(),
            label: label.to_string(),
            reason: exit_reason.clone(),
        };

        let ctx = ActorContext {
            label: label.to_string(),
            node_id: self.inner.config.node_id.clone(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: self.clone(),
            system_tx,
        };

        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<F::Actor>(),
            actor_type_name: std::any::type_name::<F::Actor>().to_string(),
            location: EntryLocation::Local {
                endpoint_factory: Box::new(LocalEndpointFactory {
                    mailbox_tx: mailbox_tx.clone(),
                }),
                dispatch_tx: dispatch_tx.clone(),
                shutdown_tx: Some(shutdown_tx),
            },
            keys: Vec::new(),
            node_id: self.inner.config.node_id.clone(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        self.record_register_op(label, std::any::type_name::<F::Actor>(), &[]);

        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<F::Actor>().to_string(),
        });

        // Spawn restart wrapper
        let receptionist = self.clone();
        let label_owned = label.to_string();
        let node_id = self.inner.config.node_id.clone();

        tokio::spawn(async move {
            let mut restart_times: VecDeque<Instant> = VecDeque::new();
            let mut backoff_duration = config.backoff.initial;

            // First run uses the actor/state/channels created above
            let (mb, dp) = run_supervisor(
                actor,
                state,
                ctx,
                mailbox_rx,
                dispatch_rx,
                shutdown_rx,
                system_rx,
                exit_reason.clone(),
                guard,
            )
            .await;
            mailbox_rx = mb;
            dispatch_rx = dp;

            let reason = exit_reason.lock().unwrap().clone();
            if !should_restart(&config.policy, &reason) {
                return;
            }

            // Restart loop with limits and backoff
            loop {
                // Prune timestamps outside the rolling window
                let now = Instant::now();
                while restart_times
                    .front()
                    .is_some_and(|t| now.duration_since(*t) > config.window)
                {
                    restart_times.pop_front();
                }

                // Check if we've exceeded the restart limit
                if restart_times.len() >= config.max_restarts as usize {
                    // The previous incarnation's guard already removed the entry.
                    // Re-register briefly so deregister_with_reason can fire watches.
                    let entry = ActorEntry {
                        label: label_owned.clone(),
                        type_id: TypeId::of::<F::Actor>(),
                        actor_type_name: std::any::type_name::<F::Actor>().to_string(),
                        location: EntryLocation::Local {
                            endpoint_factory: Box::new(LocalEndpointFactory {
                                mailbox_tx: mailbox_tx.clone(),
                            }),
                            dispatch_tx: dispatch_tx.clone(),
                            shutdown_tx: None,
                        },
                        keys: Vec::new(),
                        node_id: node_id.clone(),
                    };
                    receptionist
                        .inner
                        .entries
                        .lock()
                        .unwrap()
                        .insert(label_owned.clone(), entry);
                    receptionist.deregister_with_reason(
                        &label_owned,
                        TerminationReason::RestartLimitExceeded,
                    );
                    break;
                }

                restart_times.push_back(now);

                // Apply backoff delay
                tokio::time::sleep(backoff_duration).await;
                backoff_duration = Duration::from_secs_f64(
                    (backoff_duration.as_secs_f64() * config.backoff.multiplier)
                        .min(config.backoff.max.as_secs_f64()),
                );

                let (actor, state) = factory.create();
                let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
                let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();

                let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

                let guard = DeregisterGuard {
                    receptionist: receptionist.clone(),
                    label: label_owned.clone(),
                    reason: exit_reason.clone(),
                };

                let ctx = ActorContext {
                    label: label_owned.clone(),
                    node_id: node_id.clone(),
                    mailbox_tx: mailbox_tx.clone(),
                    receptionist: receptionist.clone(),
                    system_tx,
                };

                // Re-register with the same label and stable channels
                let entry = ActorEntry {
                    label: label_owned.clone(),
                    type_id: TypeId::of::<F::Actor>(),
                    actor_type_name: std::any::type_name::<F::Actor>().to_string(),
                    location: EntryLocation::Local {
                        endpoint_factory: Box::new(LocalEndpointFactory {
                            mailbox_tx: mailbox_tx.clone(),
                        }),
                        dispatch_tx: dispatch_tx.clone(),
                        shutdown_tx: Some(shutdown_tx),
                    },
                    keys: Vec::new(),
                    node_id: node_id.clone(),
                };
                receptionist
                    .inner
                    .entries
                    .lock()
                    .unwrap()
                    .insert(label_owned.clone(), entry);

                receptionist.emit(ActorEvent::Registered {
                    label: label_owned.clone(),
                    actor_type: std::any::type_name::<F::Actor>().to_string(),
                });

                let (mb, dp) = run_supervisor(
                    actor,
                    state,
                    ctx,
                    mailbox_rx,
                    dispatch_rx,
                    shutdown_rx,
                    system_rx,
                    exit_reason.clone(),
                    guard,
                )
                .await;
                mailbox_rx = mb;
                dispatch_rx = dp;

                let reason = exit_reason.lock().unwrap().clone();
                if !should_restart(&config.policy, &reason) {
                    break;
                }
            }
        });

        endpoint
    }

    /// Register a remote actor (discovered via cluster gossip).
    pub fn register_remote<A: Actor + 'static>(
        &self,
        label: &str,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    ) {
        self.register_remote_from_node::<A>(
            label,
            wire_tx,
            response_registry,
            &self.inner.config.node_id.clone(),
        );
    }

    /// Register a remote actor from a specific node.
    pub fn register_remote_from_node<A: Actor + 'static>(
        &self,
        label: &str,
        wire_tx: mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
        node_id: &str,
    ) {
        let entry = ActorEntry {
            label: label.to_string(),
            type_id: TypeId::of::<A>(),
            actor_type_name: std::any::type_name::<A>().to_string(),
            location: EntryLocation::Remote {
                wire_tx,
                response_registry,
            },
            keys: Vec::new(),
            node_id: node_id.to_string(),
        };
        self.inner
            .entries
            .lock()
            .unwrap()
            .insert(label.to_string(), entry);

        self.record_register_op(label, std::any::type_name::<A>(), &[]);

        self.emit(ActorEvent::Registered {
            label: label.to_string(),
            actor_type: std::any::type_name::<A>().to_string(),
        });
    }

    /// Deregister an actor. Idempotent — no-op if already gone.
    pub fn deregister(&self, label: &str) {
        self.deregister_with_reason(label, TerminationReason::Stopped);
    }

    /// Deregister an actor with a specific termination reason.
    /// Fires any watches registered for this actor.
    pub fn deregister_with_reason(&self, label: &str, reason: TerminationReason) {
        // Cancel any pending blip timer
        if let Some(handle) = self.inner.blip_pending.lock().unwrap().remove(label) {
            handle.abort();
        }

        if let Some(entry) = self.inner.entries.lock().unwrap().remove(label) {
            // Record remove op in oplog (immediate, no blip window for removes)
            self.inner.oplog.lock().unwrap().append(OpType::Remove {
                label: label.to_string(),
            });

            // Fire watches (one-shot: remove drains the list)
            if let Some(watchers) = self.inner.watches.lock().unwrap().remove(label) {
                let terminated = ActorTerminated {
                    label: label.to_string(),
                    reason: reason.clone(),
                };
                for watcher in watchers {
                    let _ = watcher
                        .system_tx
                        .send(SystemSignal::ActorTerminated(terminated.clone()));
                }
            }

            self.emit(ActorEvent::Deregistered {
                label: entry.label,
                actor_type: entry.actor_type_name,
            });
        }
    }

    /// Register a watch on an actor. Erlang semantics: if the watched actor
    /// doesn't exist, fires immediately.
    pub(crate) fn add_watch(
        &self,
        watched_label: &str,
        watcher_label: &str,
        system_tx: mpsc::UnboundedSender<SystemSignal>,
    ) {
        let exists = self
            .inner
            .entries
            .lock()
            .unwrap()
            .contains_key(watched_label);
        if !exists {
            // Erlang semantics: immediate fire if watched actor doesn't exist
            let _ = system_tx.send(SystemSignal::ActorTerminated(ActorTerminated {
                label: watched_label.to_string(),
                reason: TerminationReason::Stopped,
            }));
            return;
        }

        self.inner
            .watches
            .lock()
            .unwrap()
            .entry(watched_label.to_string())
            .or_default()
            .push(WatchEntry {
                watcher_label: watcher_label.to_string(),
                system_tx,
            });
    }

    /// Stop a local actor gracefully. Sends shutdown signal to supervisor.
    /// The supervisor will exit, and the DeregisterGuard will auto-deregister.
    pub fn stop(&self, label: &str) {
        let mut entries = self.inner.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(label) {
            if let EntryLocation::Local { shutdown_tx, .. } = &mut entry.location {
                if let Some(tx) = shutdown_tx.take() {
                    let _ = tx.send(());
                }
            }
        }
        // Don't remove entry here — the supervisor's DeregisterGuard will handle it
    }

    /// Lookup an actor by label. **The caller provides the expected type.**
    /// Returns None if not found or if the type doesn't match.
    pub fn lookup<A: Actor + 'static>(&self, label: &str) -> Option<Endpoint<A>> {
        let entries = self.inner.entries.lock().unwrap();
        let entry = entries.get(label)?;

        // TypeId guard — ensures the downcast inside is safe
        if entry.type_id != TypeId::of::<A>() {
            return None;
        }

        match &entry.location {
            EntryLocation::Local {
                endpoint_factory, ..
            } => {
                let any_endpoint = endpoint_factory.create_endpoint_any();
                any_endpoint.downcast::<Endpoint<A>>().ok().map(|b| *b)
            }
            EntryLocation::Remote {
                wire_tx,
                response_registry,
            } => Some(Endpoint::remote(
                entry.label.clone(),
                wire_tx.clone(),
                response_registry.clone(),
            )),
        }
    }

    /// Get the dispatch channel for routing incoming remote messages
    /// to a local actor's supervisor.
    pub fn get_dispatch_sender(
        &self,
        label: &str,
    ) -> Option<mpsc::UnboundedSender<DispatchRequest>> {
        let entries = self.inner.entries.lock().unwrap();
        let entry = entries.get(label)?;
        match &entry.location {
            EntryLocation::Local { dispatch_tx, .. } => Some(dispatch_tx.clone()),
            EntryLocation::Remote { .. } => None,
        }
    }

    /// Check if an entry exists for the given label.
    pub fn has_entry(&self, label: &str) -> bool {
        self.inner.entries.lock().unwrap().contains_key(label)
    }

    // =========================================================================
    // LIFECYCLE EVENTS
    // =========================================================================

    /// Subscribe to lifecycle events (type-erased).
    pub fn subscribe_events(&self) -> mpsc::UnboundedReceiver<ActorEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.event_subscribers.lock().unwrap().push(tx);
        rx
    }

    fn emit(&self, event: ActorEvent) {
        let mut subs = self.inner.event_subscribers.lock().unwrap();
        subs.retain(|tx| tx.send(event.clone()).is_ok());
    }

    // =========================================================================
    // RECEPTION KEYS & LISTINGS (Phase 1)
    // =========================================================================

    /// Check in an actor with a group key. The actor must already be
    /// registered (via `start()` or `register_remote()`).
    ///
    /// Multiple actors can share the same key. This is how you build
    /// actor groups — register several actors with the same key, then
    /// subscribe to the key to discover them all.
    pub fn check_in<A: Actor + 'static>(&self, label: &str, key: ReceptionKey<A>) {
        let erased_key = ErasedReceptionKey {
            id: key.id.clone(),
            type_id: TypeId::of::<A>(),
        };

        // Lock both in consistent order: entries → listing_subscribers
        let entries = self.inner.entries.lock().unwrap();
        let Some(entry) = entries.get(label) else {
            return;
        };
        if entry.type_id != TypeId::of::<A>() {
            return;
        }

        // Notify matching listing subscribers
        if self.inner.config.flush_interval.is_some() {
            // Delayed flush mode: buffer the notification
            self.inner
                .pending_notifications
                .lock()
                .unwrap()
                .push(PendingListingNotification {
                    label: label.to_string(),
                    key_id: key.id.clone(),
                    type_id: TypeId::of::<A>(),
                });
        } else {
            // Immediate mode: notify now
            let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();
            listing_subs.retain(|sub| !sub.is_closed());
            for sub in listing_subs.iter() {
                if sub.key_id() == key.id && sub.actor_type_id() == TypeId::of::<A>() {
                    sub.try_send_from_entry(entry);
                }
            }
        }

        // Must drop entries before mutating it (need mutable borrow)
        drop(entries);

        // Add key to entry
        self.inner
            .entries
            .lock()
            .unwrap()
            .get_mut(label)
            .map(|e| e.keys.push(erased_key));
    }

    /// Get a live listing of actors registered with the given key.
    ///
    /// Returns existing matches immediately (backfill), then streams
    /// new actors as they check in with the matching key.
    ///
    /// Analogous to Swift's `receptionist.listing(of: key)`.
    pub fn listing<A: Actor + 'static>(&self, key: ReceptionKey<A>) -> Listing<A> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Lock both atomically: entries → listing_subscribers
        // This prevents a race where an actor checks in between
        // backfill and subscription registration.
        let entries = self.inner.entries.lock().unwrap();
        let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();

        // Backfill: send all existing actors that match this key
        for entry in entries.values() {
            if entry.type_id == TypeId::of::<A>()
                && entry
                    .keys
                    .iter()
                    .any(|k| k.id == key.id && k.type_id == TypeId::of::<A>())
            {
                let temp_sender = TypedListingSender::<A> {
                    key_id: key.id.clone(),
                    tx: tx.clone(),
                };
                temp_sender.try_send_from_entry(entry);
            }
        }

        // Register for future notifications
        listing_subs.push(Box::new(TypedListingSender::<A> {
            key_id: key.id.clone(),
            tx,
        }));

        // Clean up closed subscribers while we have the lock
        listing_subs.retain(|sub| !sub.is_closed());

        Listing { rx }
    }

    // =========================================================================
    // OPLOG REPLICATION (Phase 2)
    // =========================================================================

    /// Get this receptionist's current version vector.
    /// Includes our own latest sequence + observed sequences from peers.
    pub fn version_vector(&self) -> VersionVector {
        let mut vv = self.inner.observed_versions.lock().unwrap().clone();
        let oplog = self.inner.oplog.lock().unwrap();
        let latest = oplog.latest_seq();
        if latest > 0 {
            vv.update(&self.inner.config.node_id, latest);
        }
        vv
    }

    /// Get ops from our local log that the peer hasn't seen yet.
    /// The peer provides their version vector so we know what to send.
    pub fn ops_since(&self, peer_versions: &VersionVector) -> Vec<Op> {
        let oplog = self.inner.oplog.lock().unwrap();
        let peer_has_seen = peer_versions.get(&self.inner.config.node_id);
        oplog.ops_since(peer_has_seen).to_vec()
    }

    /// Apply operations received from a remote peer.
    /// Uses version vectors for deduplication — already-seen ops are skipped.
    pub fn apply_ops(&self, ops: Vec<Op>) {
        for op in ops {
            {
                let mut versions = self.inner.observed_versions.lock().unwrap();
                let already_seen = versions.get(&op.node_id) >= op.seq;
                if already_seen {
                    continue;
                }
                versions.update(&op.node_id, op.seq);
            }

            match &op.op_type {
                OpType::Register {
                    label,
                    actor_type_name,
                    ..
                } => {
                    // For remote ops, we emit an event but don't create a full entry
                    // (the actual entry would be created by register_remote when
                    // the connection is established).
                    self.emit(ActorEvent::Registered {
                        label: label.clone(),
                        actor_type: actor_type_name.clone(),
                    });
                }
                OpType::Remove { label } => {
                    self.deregister(label);
                }
            }
        }
    }

    /// Remove all actors owned by a specific node.
    /// Used when the cluster detects a node has left or crashed.
    pub fn prune_node(&self, node_id: &str) {
        let labels_to_remove: Vec<String> = {
            let entries = self.inner.entries.lock().unwrap();
            entries
                .values()
                .filter(|e| e.node_id == node_id)
                .map(|e| e.label.clone())
                .collect()
        };

        for label in &labels_to_remove {
            // deregister handles oplog recording + event emission
            self.deregister(label);
        }
    }

    /// Get a snapshot of the oplog for inspection/testing.
    pub fn oplog_snapshot(&self) -> Vec<Op> {
        self.inner.oplog.lock().unwrap().ops.clone()
    }

    // =========================================================================
    // PHASE 3: DELAYED FLUSH & BLIP AVOIDANCE
    // =========================================================================

    /// Record a register op, respecting blip window if configured.
    fn record_register_op(&self, label: &str, actor_type_name: &str, key_ids: &[String]) {
        let blip_window = self.inner.config.blip_window;
        let origin_addr = self.inner.config.origin_addr.clone();

        if let Some(window) = blip_window {
            // Blip avoidance: delay the oplog entry
            let receptionist = self.clone();
            let label_owned = label.to_string();
            let actor_type_name = actor_type_name.to_string();
            let key_ids = key_ids.to_vec();

            let label_for_insert = label_owned.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(window).await;

                // If the actor is still registered after the window, commit the op
                let still_exists = receptionist
                    .inner
                    .entries
                    .lock()
                    .unwrap()
                    .contains_key(&label_owned);

                if still_exists {
                    receptionist
                        .inner
                        .oplog
                        .lock()
                        .unwrap()
                        .append(OpType::Register {
                            label: label_owned.clone(),
                            actor_type_name,
                            key_ids,
                            origin_addr,
                        });
                }

                // Remove from pending
                receptionist
                    .inner
                    .blip_pending
                    .lock()
                    .unwrap()
                    .remove(&label_owned);
            });

            self.inner
                .blip_pending
                .lock()
                .unwrap()
                .insert(label_for_insert, handle);
        } else {
            // No blip window: commit immediately
            self.inner.oplog.lock().unwrap().append(OpType::Register {
                label: label.to_string(),
                actor_type_name: actor_type_name.to_string(),
                key_ids: key_ids.to_vec(),
                origin_addr,
            });
        }
    }

    /// Flush pending listing notifications (called by the flush timer task).
    fn flush_pending_notifications(&self) {
        let pending: Vec<PendingListingNotification> = {
            let mut pending = self.inner.pending_notifications.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        if pending.is_empty() {
            return;
        }

        let entries = self.inner.entries.lock().unwrap();
        let mut listing_subs = self.inner.listing_subscribers.lock().unwrap();
        listing_subs.retain(|sub| !sub.is_closed());

        for notif in &pending {
            if let Some(entry) = entries.get(&notif.label) {
                for sub in listing_subs.iter() {
                    if sub.key_id() == notif.key_id && sub.actor_type_id() == notif.type_id {
                        sub.try_send_from_entry(entry);
                    }
                }
            }
        }
    }
}

impl Default for Receptionist {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SUPERVISOR — runs an actor, handles both local and remote messages
// =============================================================================

/// Returns the mailbox and dispatch receivers so they can be reused on restart.
/// The exit reason is communicated via the shared Arc<Mutex<TerminationReason>>.
async fn run_supervisor<A>(
    mut actor: A,
    mut state: A::State,
    ctx: ActorContext<A>,
    mut mailbox_rx: mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>,
    mut dispatch_rx: mpsc::UnboundedReceiver<DispatchRequest>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut system_rx: mpsc::UnboundedReceiver<SystemSignal>,
    exit_reason: Arc<Mutex<TerminationReason>>,
    _deregister_guard: DeregisterGuard,
) -> (
    mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>,
    mpsc::UnboundedReceiver<DispatchRequest>,
)
where
    A: Actor + RemoteDispatch + 'static,
{
    loop {
        tokio::select! {
            msg = mailbox_rx.recv() => {
                match msg {
                    Some(envelope) => {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            envelope.handle(&ctx, &mut actor, &mut state);
                        }));
                        if let Err(panic_info) = result {
                            let msg = extract_panic_message(&panic_info);
                            *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                            break;
                        }
                    }
                    None => break,
                }
            }
            req = dispatch_rx.recv() => {
                match req {
                    Some(request) => {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            actor.dispatch_remote(
                                &ctx,
                                &mut state,
                                &request.invocation.message_type,
                                &request.invocation.payload,
                            )
                        }));
                        match result {
                            Ok(dispatch_result) => {
                                let response = RemoteResponse {
                                    call_id: request.invocation.call_id,
                                    result: dispatch_result.map_err(|e| e.to_string()),
                                };
                                let _ = request.respond_to.send(response);
                            }
                            Err(panic_info) => {
                                let msg = extract_panic_message(&panic_info);
                                // Send error response before breaking
                                let response = RemoteResponse {
                                    call_id: request.invocation.call_id,
                                    result: Err(format!("actor panicked: {msg}")),
                                };
                                let _ = request.respond_to.send(response);
                                *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
            signal = system_rx.recv() => {
                match signal {
                    Some(SystemSignal::ActorTerminated(terminated)) => {
                        actor.on_actor_terminated(&mut state, &terminated);
                    }
                    None => {} // system channel closed, ignore
                }
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }
    // _deregister_guard is dropped here → calls receptionist.deregister_with_reason(label, reason)
    (mailbox_rx, dispatch_rx)
}

/// Extract a human-readable message from a panic payload.
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Decide whether an actor should be restarted given its policy and exit reason.
fn should_restart(policy: &RestartPolicy, reason: &TerminationReason) -> bool {
    match policy {
        RestartPolicy::Temporary => false,
        RestartPolicy::Permanent => true,
        RestartPolicy::Transient => matches!(reason, TerminationReason::Panicked(_)),
    }
}

// =============================================================================
// SIMULATED NODE SERVER — processes incoming remote invocations
// =============================================================================

/// Simulates the receiving side of a node. Routes incoming RemoteInvocations
/// to the correct actor via the receptionist's dispatch channel.
pub async fn run_node_receiver(
    receptionist: Arc<Receptionist>,
    mut wire_rx: mpsc::UnboundedReceiver<RemoteInvocation>,
    response_tx: mpsc::UnboundedSender<RemoteResponse>,
) {
    while let Some(invocation) = wire_rx.recv().await {
        let dispatch_tx = receptionist.get_dispatch_sender(&invocation.actor_label);

        match dispatch_tx {
            Some(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let request = DispatchRequest {
                    invocation,
                    respond_to: resp_tx,
                };
                if tx.send(request).is_ok() {
                    if let Ok(response) = resp_rx.await {
                        let _ = response_tx.send(response);
                    }
                }
            }
            None => {
                let response = RemoteResponse {
                    call_id: invocation.call_id,
                    result: Err(format!("actor not found: {}", invocation.actor_label)),
                };
                let _ = response_tx.send(response);
            }
        }
    }
}

// =============================================================================
// UTILITIES
// =============================================================================

fn next_call_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// =============================================================================
// TESTS — watch mechanism and restart policies
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    // ── Watcher Actor: watches other actors, reports terminations ────

    #[derive(Debug)]
    struct WatcherActor;

    struct WatcherState {
        terminated_tx: mpsc::UnboundedSender<ActorTerminated>,
    }

    impl Actor for WatcherActor {
        type State = WatcherState;

        fn on_actor_terminated(&mut self, state: &mut Self::State, terminated: &ActorTerminated) {
            let _ = state.terminated_tx.send(terminated.clone());
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct WatchTarget(String);
    impl Message for WatchTarget {
        type Result = ();
    }
    impl RemoteMessage for WatchTarget {
        const TYPE_ID: &'static str = "test::WatchTarget";
    }

    impl Handler<WatchTarget> for WatcherActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut WatcherState,
            msg: WatchTarget,
        ) {
            ctx.watch(&msg.0);
        }
    }

    impl RemoteDispatch for WatcherActor {
        fn dispatch_remote(
            &mut self,
            ctx: &ActorContext<Self>,
            state: &mut WatcherState,
            message_type: &str,
            payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            match message_type {
                "test::WatchTarget" => {
                    let (msg, _): (WatchTarget, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<WatchTarget>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                other => Err(DispatchError::UnknownMessageType(other.to_string())),
            }
        }
    }

    // ── Simple Actor: stores a value, can panic on command ──────────

    #[derive(Debug)]
    struct SimpleActor;

    struct SimpleState {
        value: i64,
    }

    impl Actor for SimpleActor {
        type State = SimpleState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct GetValue;
    impl Message for GetValue {
        type Result = i64;
    }
    impl RemoteMessage for GetValue {
        const TYPE_ID: &'static str = "test::GetValue";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SetValue(i64);
    impl Message for SetValue {
        type Result = ();
    }
    impl RemoteMessage for SetValue {
        const TYPE_ID: &'static str = "test::SetValue";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PanicNow;
    impl Message for PanicNow {
        type Result = ();
    }
    impl RemoteMessage for PanicNow {
        const TYPE_ID: &'static str = "test::PanicNow";
    }

    impl Handler<GetValue> for SimpleActor {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut SimpleState,
            _msg: GetValue,
        ) -> i64 {
            state.value
        }
    }

    impl Handler<SetValue> for SimpleActor {
        fn handle(&mut self, _ctx: &ActorContext<Self>, state: &mut SimpleState, msg: SetValue) {
            state.value = msg.0;
        }
    }

    impl Handler<PanicNow> for SimpleActor {
        fn handle(&mut self, _ctx: &ActorContext<Self>, _state: &mut SimpleState, _msg: PanicNow) {
            panic!("PanicNow triggered");
        }
    }

    impl RemoteDispatch for SimpleActor {
        fn dispatch_remote(
            &mut self,
            ctx: &ActorContext<Self>,
            state: &mut SimpleState,
            message_type: &str,
            payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            match message_type {
                "test::GetValue" => {
                    let (msg, _): (GetValue, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<GetValue>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                "test::SetValue" => {
                    let (msg, _): (SetValue, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<SetValue>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                "test::PanicNow" => {
                    let (msg, _): (PanicNow, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<PanicNow>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                other => Err(DispatchError::UnknownMessageType(other.to_string())),
            }
        }
    }

    // ── Factory for SimpleActor ─────────────────────────────────────

    struct SimpleFactory {
        initial_value: i64,
    }

    impl ActorFactory for SimpleFactory {
        type Actor = SimpleActor;
        fn create(&mut self) -> (SimpleActor, SimpleState) {
            (
                SimpleActor,
                SimpleState {
                    value: self.initial_value,
                },
            )
        }
    }

    // ═════════════════════════════════════════════════════════════════
    // WATCH TESTS
    // ═════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_watch_fires_on_stop() {
        let receptionist = Receptionist::new();
        let (term_tx, mut term_rx) = mpsc::unbounded_channel::<ActorTerminated>();

        let watcher_ep = receptionist.start(
            "watcher",
            WatcherActor,
            WatcherState {
                terminated_tx: term_tx,
            },
        );

        let _target_ep = receptionist.start("target", SimpleActor, SimpleState { value: 42 });

        // Tell watcher to watch target
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();

        // Stop target
        receptionist.stop("target");

        // Watcher should receive termination notification
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout waiting for termination")
            .expect("channel closed");

        assert_eq!(terminated.label, "target");
        assert!(matches!(terminated.reason, TerminationReason::Stopped));
    }

    #[tokio::test]
    async fn test_watch_is_one_shot() {
        let receptionist = Receptionist::new();
        let (term_tx, mut term_rx) = mpsc::unbounded_channel::<ActorTerminated>();

        let watcher_ep = receptionist.start(
            "watcher",
            WatcherActor,
            WatcherState {
                terminated_tx: term_tx,
            },
        );

        // Start, watch, stop target
        let _target_ep = receptionist.start("target", SimpleActor, SimpleState { value: 0 });
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();
        receptionist.stop("target");

        // Should fire once
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(terminated.label, "target");

        // Wait for deregister to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Start a NEW actor with the same label and stop it
        let _target_ep2 = receptionist.start("target", SimpleActor, SimpleState { value: 0 });
        receptionist.stop("target");

        // Give some time — should NOT receive a second notification (one-shot)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(term_rx.try_recv().is_err(), "watch should be one-shot");
    }

    #[tokio::test]
    async fn test_watch_nonexistent_fires_immediately() {
        let receptionist = Receptionist::new();
        let (term_tx, mut term_rx) = mpsc::unbounded_channel::<ActorTerminated>();

        let watcher_ep = receptionist.start(
            "watcher",
            WatcherActor,
            WatcherState {
                terminated_tx: term_tx,
            },
        );

        // Watch a label that doesn't exist
        watcher_ep
            .send(WatchTarget("nonexistent".to_string()))
            .await
            .unwrap();

        // Should fire immediately
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(terminated.label, "nonexistent");
        assert!(matches!(terminated.reason, TerminationReason::Stopped));
    }

    // ═════════════════════════════════════════════════════════════════
    // RESTART POLICY TESTS
    // ═════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_restart_policy_permanent() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start_with_policy(
            "permanent",
            SimpleFactory { initial_value: 100 },
            RestartPolicy::Permanent,
        );

        // Verify initial state
        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 100);

        // Cause a panic
        let _ = ep.send(PanicNow).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Actor should be restarted with fresh state
        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 100, "permanent actor should restart with fresh state");
    }

    #[tokio::test]
    async fn test_restart_policy_transient_panic() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start_with_policy(
            "transient",
            SimpleFactory { initial_value: 50 },
            RestartPolicy::Transient,
        );

        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 50);

        // Panic should trigger restart (Transient)
        let _ = ep.send(PanicNow).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 50, "transient actor should restart after panic");
    }

    #[tokio::test]
    async fn test_restart_policy_transient_stop() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start_with_policy(
            "transient-stop",
            SimpleFactory { initial_value: 50 },
            RestartPolicy::Transient,
        );

        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 50);

        // Clean stop should NOT trigger restart (Transient)
        receptionist.stop("transient-stop");
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Endpoint should now fail (actor is dead)
        let result = ep.send(GetValue).await;
        assert!(
            result.is_err(),
            "transient actor should stay dead after clean stop"
        );
    }

    #[tokio::test]
    async fn test_restart_policy_temporary() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start_with_policy(
            "temporary",
            SimpleFactory { initial_value: 25 },
            RestartPolicy::Temporary,
        );

        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 25);

        // Panic should NOT trigger restart (Temporary)
        let _ = ep.send(PanicNow).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = ep.send(GetValue).await;
        assert!(
            result.is_err(),
            "temporary actor should stay dead after panic"
        );
    }

    #[tokio::test]
    async fn test_watch_with_restart() {
        let receptionist = Receptionist::new();
        let (term_tx, mut term_rx) = mpsc::unbounded_channel::<ActorTerminated>();

        let watcher_ep = receptionist.start(
            "watcher",
            WatcherActor,
            WatcherState {
                terminated_tx: term_tx,
            },
        );

        // Start permanent actor
        let target_ep = receptionist.start_with_policy(
            "target",
            SimpleFactory { initial_value: 0 },
            RestartPolicy::Permanent,
        );

        // Watch it
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();

        // Cause a crash
        let _ = target_ep.send(PanicNow).await;

        // Watch should fire with Panicked reason
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(terminated.label, "target");
        assert!(matches!(terminated.reason, TerminationReason::Panicked(_)));

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Actor should be alive again
        let val = target_ep.send(GetValue).await.unwrap();
        assert_eq!(val, 0, "restarted actor should have fresh state");

        // Install a second watch (new incarnation)
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();

        // Crash again
        let _ = target_ep.send(PanicNow).await;

        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(terminated.label, "target");
        assert!(matches!(terminated.reason, TerminationReason::Panicked(_)));
    }

    // ═════════════════════════════════════════════════════════════════
    // RESTART LIMITS + BACKOFF TESTS
    // ═════════════════════════════════════════════════════════════════

    /// Factory that counts how many times it has been called.
    struct CountingFactory {
        initial_value: i64,
        create_count: Arc<std::sync::atomic::AtomicU32>,
    }

    impl ActorFactory for CountingFactory {
        type Actor = SimpleActor;
        fn create(&mut self) -> (SimpleActor, SimpleState) {
            self.create_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (
                SimpleActor,
                SimpleState {
                    value: self.initial_value,
                },
            )
        }
    }

    #[tokio::test]
    async fn test_restart_limit_exceeded() {
        let receptionist = Receptionist::new();
        let create_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let ep = receptionist.start_with_config(
            "limited",
            CountingFactory {
                initial_value: 0,
                create_count: create_count.clone(),
            },
            RestartConfig {
                policy: RestartPolicy::Permanent,
                max_restarts: 3,
                window: Duration::from_secs(60),
                backoff: BackoffConfig {
                    initial: Duration::from_millis(10),
                    max: Duration::from_millis(50),
                    multiplier: 1.5,
                },
            },
        );

        // Trigger panics rapidly — should stop after max_restarts
        for _ in 0..6 {
            let _ = ep.send(PanicNow).await;
            tokio::time::sleep(Duration::from_millis(80)).await;
        }

        // Wait for the restart loop to give up
        tokio::time::sleep(Duration::from_millis(500)).await;

        // The actor should be dead now (endpoint fails)
        let result = ep.send(GetValue).await;
        assert!(
            result.is_err(),
            "actor should be dead after restart limit exceeded"
        );

        // Factory was called: 1 initial + max_restarts times
        let count = create_count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            count <= 4,
            "factory should be called at most 1 + max_restarts times, got {count}"
        );
    }

    #[tokio::test]
    async fn test_restart_window_rolling() {
        let receptionist = Receptionist::new();
        let create_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let ep = receptionist.start_with_config(
            "rolling",
            CountingFactory {
                initial_value: 0,
                create_count: create_count.clone(),
            },
            RestartConfig {
                policy: RestartPolicy::Permanent,
                max_restarts: 2,
                window: Duration::from_millis(200), // very short window
                backoff: BackoffConfig {
                    initial: Duration::from_millis(10),
                    max: Duration::from_millis(10),
                    multiplier: 1.0,
                },
            },
        );

        // First panic — restart 1
        let _ = ep.send(PanicNow).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the window to expire
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Second panic — restart counter should be reset (window expired)
        let _ = ep.send(PanicNow).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Actor should still be alive since each restart was in a different window
        let val = ep.send(GetValue).await.unwrap();
        assert_eq!(val, 0, "actor should survive restarts spaced beyond window");
    }

    #[tokio::test]
    async fn test_backoff_applies() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start_with_config(
            "backoff",
            SimpleFactory { initial_value: 0 },
            RestartConfig {
                policy: RestartPolicy::Permanent,
                max_restarts: 5,
                window: Duration::from_secs(60),
                backoff: BackoffConfig {
                    initial: Duration::from_millis(100),
                    max: Duration::from_secs(30),
                    multiplier: 2.0,
                },
            },
        );

        let start = std::time::Instant::now();

        // First panic
        let _ = ep.send(PanicNow).await;
        // Wait for the first restart (100ms backoff)
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = ep.send(GetValue).await; // should work now

        // Second panic
        let _ = ep.send(PanicNow).await;
        // Wait for the second restart (200ms backoff)
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = ep.send(GetValue).await;

        let elapsed = start.elapsed();
        // Total should be at least 300ms (100ms + 200ms backoff)
        assert!(
            elapsed >= Duration::from_millis(250),
            "backoff should cause delays: elapsed {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_restart_limit_fires_watch() {
        // Watches are one-shot (Erlang semantics): each crash fires the watch
        // with Panicked, consuming it. After restarts are exhausted, the
        // restart loop deregisters with RestartLimitExceeded. A watcher that
        // watches the dead actor after the limit gets an immediate notification.
        let receptionist = Receptionist::new();
        let (term_tx, mut term_rx) = mpsc::unbounded_channel::<ActorTerminated>();

        let watcher_ep = receptionist.start(
            "watcher",
            WatcherActor,
            WatcherState {
                terminated_tx: term_tx,
            },
        );

        let target_ep = receptionist.start_with_config(
            "target",
            SimpleFactory { initial_value: 0 },
            RestartConfig {
                policy: RestartPolicy::Permanent,
                max_restarts: 2,
                window: Duration::from_secs(60),
                backoff: BackoffConfig {
                    initial: Duration::from_millis(10),
                    max: Duration::from_millis(50),
                    multiplier: 1.5,
                },
            },
        );

        // Watch the target — watcher receives Panicked on each intermediate crash
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();

        // First crash — watch fires with Panicked
        let _ = target_ep.send(PanicNow).await;
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(terminated.label, "target");
        assert!(matches!(terminated.reason, TerminationReason::Panicked(_)));

        // Wait for restart, then crash again
        tokio::time::sleep(Duration::from_millis(100)).await;
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();
        let _ = target_ep.send(PanicNow).await;
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert!(matches!(terminated.reason, TerminationReason::Panicked(_)));

        // Wait for restart + one more crash to exhaust the limit
        tokio::time::sleep(Duration::from_millis(100)).await;
        // This crash might succeed or fail (actor may already be dead from limit)
        let _ = target_ep.send(PanicNow).await;

        // Wait for the restart loop to give up
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Actor should be permanently dead now
        let result = target_ep.send(GetValue).await;
        assert!(result.is_err(), "actor should be dead after restart limit");

        // Watch a dead actor → fires immediately (Erlang semantics)
        watcher_ep
            .send(WatchTarget("target".to_string()))
            .await
            .unwrap();
        let terminated = tokio::time::timeout(Duration::from_secs(2), term_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(terminated.label, "target");
        // Watching a non-existent actor fires with Stopped (entry is gone)
        assert!(
            matches!(terminated.reason, TerminationReason::Stopped),
            "watch on dead actor fires immediately"
        );
    }

    // ═════════════════════════════════════════════════════════════════
    // ROUTER TESTS
    // ═════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_router_round_robin() {
        let receptionist = Receptionist::new();

        let ep0 = receptionist.start("rr/0", SimpleActor, SimpleState { value: 0 });
        let ep1 = receptionist.start("rr/1", SimpleActor, SimpleState { value: 0 });
        let ep2 = receptionist.start("rr/2", SimpleActor, SimpleState { value: 0 });

        let router = Router::new(
            vec![ep0.clone(), ep1.clone(), ep2.clone()],
            RoutingStrategy::RoundRobin,
        );

        // Send 6 SetValue messages — should round-robin through all 3
        for i in 0..6 {
            router.send(SetValue(i + 1)).await.unwrap();
        }

        // Each actor should have received 2 messages:
        // ep0: SetValue(1), SetValue(4) → last value 4
        // ep1: SetValue(2), SetValue(5) → last value 5
        // ep2: SetValue(3), SetValue(6) → last value 6
        let v0 = ep0.send(GetValue).await.unwrap();
        let v1 = ep1.send(GetValue).await.unwrap();
        let v2 = ep2.send(GetValue).await.unwrap();

        assert_eq!(v0, 4, "ep0 should have value 4");
        assert_eq!(v1, 5, "ep1 should have value 5");
        assert_eq!(v2, 6, "ep2 should have value 6");
    }

    #[tokio::test]
    async fn test_router_random() {
        let receptionist = Receptionist::new();

        let ep0 = receptionist.start("rand/0", SimpleActor, SimpleState { value: 0 });
        let ep1 = receptionist.start("rand/1", SimpleActor, SimpleState { value: 0 });

        let router = Router::new(vec![ep0.clone(), ep1.clone()], RoutingStrategy::Random);

        // Send enough messages that both endpoints should receive at least one
        for i in 0..20 {
            router.send(SetValue(i + 1)).await.unwrap();
        }

        let v0 = ep0.send(GetValue).await.unwrap();
        let v1 = ep1.send(GetValue).await.unwrap();

        // Both should have received at least one message
        assert!(v0 > 0, "ep0 should have received at least one message");
        assert!(v1 > 0, "ep1 should have received at least one message");
    }

    #[tokio::test]
    async fn test_router_broadcast() {
        let receptionist = Receptionist::new();

        let ep0 = receptionist.start("bc/0", SimpleActor, SimpleState { value: 0 });
        let ep1 = receptionist.start("bc/1", SimpleActor, SimpleState { value: 0 });
        let ep2 = receptionist.start("bc/2", SimpleActor, SimpleState { value: 0 });

        let router = Router::new(
            vec![ep0.clone(), ep1.clone(), ep2.clone()],
            RoutingStrategy::Broadcast,
        );

        // Broadcast SetValue(42) — all endpoints should receive it
        let results = router.broadcast(SetValue(42)).await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
        }

        // All endpoints should have value 42
        assert_eq!(ep0.send(GetValue).await.unwrap(), 42);
        assert_eq!(ep1.send(GetValue).await.unwrap(), 42);
        assert_eq!(ep2.send(GetValue).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_router_empty() {
        let router = Router::<SimpleActor>::new(vec![], RoutingStrategy::RoundRobin);

        let result = router.send(GetValue).await;
        assert!(result.is_err(), "send on empty router should return error");
    }

    // ═════════════════════════════════════════════════════════════════
    // ACTOR REF TESTS
    // ═════════════════════════════════════════════════════════════════

    // ── RefReporter Actor: returns its own ActorRef when asked ────────

    #[derive(Debug)]
    struct RefReporterActor;

    struct RefReporterState;

    impl Actor for RefReporterActor {
        type State = RefReporterState;
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct GetMyRef;
    impl Message for GetMyRef {
        type Result = ActorRef<RefReporterActor>;
    }
    impl RemoteMessage for GetMyRef {
        const TYPE_ID: &'static str = "test::GetMyRef";
    }

    // A message that embeds an ActorRef
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ForwardRef {
        target_ref: ActorRef<SimpleActor>,
    }
    impl Message for ForwardRef {
        type Result = Option<i64>;
    }
    impl RemoteMessage for ForwardRef {
        const TYPE_ID: &'static str = "test::ForwardRef";
    }

    impl Handler<GetMyRef> for RefReporterActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut RefReporterState,
            _msg: GetMyRef,
        ) -> ActorRef<RefReporterActor> {
            ctx.actor_ref()
        }
    }

    impl Handler<ForwardRef> for RefReporterActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut RefReporterState,
            msg: ForwardRef,
        ) -> Option<i64> {
            // Try to resolve the embedded ActorRef and send GetValue
            // We can't await here (sync handler), so just check if it resolves
            msg.target_ref.resolve(ctx.receptionist()).map(|_| 42)
        }
    }

    impl RemoteDispatch for RefReporterActor {
        fn dispatch_remote(
            &mut self,
            ctx: &ActorContext<Self>,
            state: &mut RefReporterState,
            message_type: &str,
            payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            match message_type {
                "test::GetMyRef" => {
                    let (msg, _): (GetMyRef, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<GetMyRef>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                "test::ForwardRef" => {
                    let (msg, _): (ForwardRef, _) =
                        bincode::serde::decode_from_slice(payload, bincode::config::standard())
                            .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                    let result = <Self as Handler<ForwardRef>>::handle(self, ctx, state, msg);
                    bincode::serde::encode_to_vec(&result, bincode::config::standard())
                        .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                }
                other => Err(DispatchError::UnknownMessageType(other.to_string())),
            }
        }
    }

    #[tokio::test]
    async fn test_actor_ref_resolve() {
        let receptionist = Receptionist::new();

        let reporter_ep = receptionist.start("reporter", RefReporterActor, RefReporterState);

        // Ask the actor for its own ref
        let actor_ref = reporter_ep.send(GetMyRef).await.unwrap();
        assert_eq!(actor_ref.label, "reporter");
        assert_eq!(actor_ref.node_id, "local");

        // Resolve back to a working endpoint
        let resolved_ep = actor_ref.resolve(&receptionist);
        assert!(resolved_ep.is_some(), "should resolve to an endpoint");
    }

    #[tokio::test]
    async fn test_actor_ref_serialization() {
        let actor_ref = ActorRef::<SimpleActor>::new("cache/user/1", "node-alpha");

        // Serialize with bincode
        let bytes = bincode::serde::encode_to_vec(&actor_ref, bincode::config::standard()).unwrap();

        // Deserialize
        let (deserialized, _): (ActorRef<SimpleActor>, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(deserialized.label, "cache/user/1");
        assert_eq!(deserialized.node_id, "node-alpha");
    }

    #[tokio::test]
    async fn test_actor_ref_in_message() {
        let receptionist = Receptionist::new();

        // Start a SimpleActor
        let _simple_ep = receptionist.start("target/actor", SimpleActor, SimpleState { value: 99 });

        // Start a RefReporterActor that can resolve refs
        let reporter_ep = receptionist.start("reporter", RefReporterActor, RefReporterState);

        // Create an ActorRef and embed it in a message
        let target_ref = ActorRef::<SimpleActor>::new("target/actor", "local");
        let result = reporter_ep.send(ForwardRef { target_ref }).await.unwrap();

        // The handler resolved the ref and returned Some(42)
        assert_eq!(result, Some(42), "embedded ActorRef should resolve");
    }

    #[tokio::test]
    async fn test_actor_ref_resolve_missing() {
        let receptionist = Receptionist::new();

        let actor_ref = ActorRef::<SimpleActor>::new("nonexistent", "local");
        let resolved = actor_ref.resolve(&receptionist);
        assert!(
            resolved.is_none(),
            "resolving missing actor should return None"
        );
    }
}
