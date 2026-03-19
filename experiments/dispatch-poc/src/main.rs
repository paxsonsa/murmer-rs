//! # Dispatch POC
//!
//! Proves that we can:
//! 1. Keep compile-time type checking on the sender side (Endpoint<A> + Handler<M>)
//! 2. Erase types to bytes for the wire
//! 3. Recover types on the receiver via a generated dispatch table
//! 4. Get a typed response back
//!
//! This simulates what a proc macro would generate — the dispatch_remote() match
//! is the Rust equivalent of Swift's compiler-generated accessor thunks.

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::oneshot;

// ============================================================================
// FRAMEWORK LAYER — these are the core traits (like murmer's existing ones)
// ============================================================================

/// A message that can be sent to an actor. Defines its response type.
pub trait Message: Debug + Send + 'static {
    type Result: Send + 'static;
}

/// Marker trait for messages that can cross the wire.
/// Requires serialization bounds on both the message and its result.
pub trait RemoteMessage: Message + Serialize + DeserializeOwned
where
    Self::Result: Serialize + DeserializeOwned,
{
    /// Stable identifier for this message type. The proc macro would generate this.
    const TYPE_ID: &'static str;
}

/// An actor that can handle messages.
pub trait Actor: Send + 'static {
    type State: Send + 'static;
}

/// Handler for a specific message type.
pub trait Handler<M: Message>: Actor {
    fn handle(&mut self, state: &mut Self::State, message: M) -> M::Result;
}

/// Trait for actors that can dispatch remote messages.
/// THE PROC MACRO GENERATES THIS IMPL for each actor.
pub trait RemoteDispatch: Actor {
    fn dispatch_remote(
        &mut self,
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
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for DispatchError {}

// ============================================================================
// WIRE FORMAT — what travels between nodes
// ============================================================================

/// The envelope that crosses the wire. Analogous to Swift's InvocationMessage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteInvocation {
    pub call_id: u64,
    pub actor_label: String,
    pub message_type: String,
    pub payload: Vec<u8>,
}

/// The response that comes back over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteResponse {
    pub call_id: u64,
    pub result: Result<Vec<u8>, String>,
}

// ============================================================================
// ENDPOINT — the user-facing send handle
// ============================================================================

/// Sender side of an endpoint. For this POC we simulate both local and remote.
pub enum EndpointSender<A: Actor> {
    /// Local: direct channel to the supervisor
    Local {
        tx: tokio::sync::mpsc::UnboundedSender<Box<dyn EnvelopeProxy<A> + Send>>,
    },
    /// Remote: serializes and sends over the "wire" (simulated channel)
    Remote {
        actor_label: String,
        wire_tx: tokio::sync::mpsc::UnboundedSender<RemoteInvocation>,
        response_registry: ResponseRegistry,
    },
}

pub struct Endpoint<A: Actor> {
    sender: EndpointSender<A>,
}

impl<A: Actor> Endpoint<A> {
    /// Send a message to the actor. Compile-time checked: A must implement Handler<M>.
    pub async fn send<M>(&self, message: M) -> Result<M::Result, Box<dyn std::error::Error>>
    where
        A: Handler<M>,
        M: Message,
    {
        match &self.sender {
            EndpointSender::Local { tx } => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let envelope = TypedEnvelope {
                    message,
                    respond_to: resp_tx,
                };
                tx.send(Box::new(envelope))
                    .map_err(|_| "mailbox closed")?;
                Ok(resp_rx.await?)
            }
            EndpointSender::Remote { .. } => {
                panic!("use send_remote for remote endpoints in this poc")
            }
        }
    }

    /// Send a remote message. Requires RemoteMessage bounds for serialization.
    /// In the real framework, `send()` would handle both paths transparently.
    pub async fn send_remote<M>(
        &self,
        message: M,
    ) -> Result<M::Result, Box<dyn std::error::Error>>
    where
        A: Handler<M>,
        M: RemoteMessage,
        M::Result: Serialize + DeserializeOwned,
    {
        match &self.sender {
            EndpointSender::Remote {
                actor_label,
                wire_tx,
                response_registry,
            } => {
                let call_id = rand_id();

                // Serialize the message — sender knows the concrete type
                let payload =
                    bincode::serde::encode_to_vec(&message, bincode::config::standard())?;

                // Register a response listener BEFORE sending
                let (resp_tx, resp_rx) = oneshot::channel::<RemoteResponse>();
                response_registry.register(call_id, resp_tx);

                // Build the wire envelope
                let invocation = RemoteInvocation {
                    call_id,
                    actor_label: actor_label.clone(),
                    message_type: M::TYPE_ID.to_string(),
                    payload,
                };

                wire_tx.send(invocation).map_err(|_| "wire closed")?;

                // Await response
                let response = resp_rx.await?;
                let result_bytes = response.result.map_err(|e| e)?;
                let (result, _): (M::Result, _) =
                    bincode::serde::decode_from_slice(&result_bytes, bincode::config::standard())?;
                Ok(result)
            }
            EndpointSender::Local { .. } => self.send(message).await,
        }
    }
}

// ============================================================================
// ENVELOPE — type-erased message dispatch (existing murmer pattern)
// ============================================================================

pub trait EnvelopeProxy<A: Actor> {
    fn handle(self: Box<Self>, actor: &mut A, state: &mut A::State);
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
    fn handle(self: Box<Self>, actor: &mut A, state: &mut A::State) {
        let result = actor.handle(state, self.message);
        let _ = self.respond_to.send(result);
    }
}

// ============================================================================
// RESPONSE REGISTRY — correlate responses to in-flight calls
// ============================================================================

#[derive(Clone)]
pub struct ResponseRegistry {
    inner: std::sync::Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<RemoteResponse>>>>,
}

impl ResponseRegistry {
    fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn register(&self, call_id: u64, tx: oneshot::Sender<RemoteResponse>) {
        self.inner.lock().unwrap().insert(call_id, tx);
    }

    fn complete(&self, response: RemoteResponse) {
        if let Some(tx) = self.inner.lock().unwrap().remove(&response.call_id) {
            let _ = tx.send(response);
        }
    }
}

// ============================================================================
// USER CODE — define an actor, messages, and handlers
// ============================================================================

#[derive(Debug)]
pub struct CounterActor;

#[derive(Debug)]
pub struct CounterState {
    count: i64,
    name: String,
}

// --- Messages ---

#[derive(Debug, Serialize, Deserialize)]
pub struct Increment {
    pub amount: i64,
}
impl Message for Increment {
    type Result = i64; // returns new count
}
impl RemoteMessage for Increment {
    const TYPE_ID: &'static str = "counter::Increment";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetCount;
impl Message for GetCount {
    type Result = i64;
}
impl RemoteMessage for GetCount {
    const TYPE_ID: &'static str = "counter::GetCount";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetInfo;
impl Message for GetInfo {
    type Result = CounterInfo;
}
impl RemoteMessage for GetInfo {
    const TYPE_ID: &'static str = "counter::GetInfo";
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CounterInfo {
    pub name: String,
    pub count: i64,
}

// --- Actor + Handler impls ---

impl Actor for CounterActor {
    type State = CounterState;
}

impl Handler<Increment> for CounterActor {
    fn handle(&mut self, state: &mut CounterState, msg: Increment) -> i64 {
        state.count += msg.amount;
        println!("  [actor] Increment by {} → count is now {}", msg.amount, state.count);
        state.count
    }
}

impl Handler<GetCount> for CounterActor {
    fn handle(&mut self, state: &mut CounterState, _msg: GetCount) -> i64 {
        println!("  [actor] GetCount → {}", state.count);
        state.count
    }
}

impl Handler<GetInfo> for CounterActor {
    fn handle(&mut self, state: &mut CounterState, _msg: GetInfo) -> CounterInfo {
        let info = CounterInfo {
            name: state.name.clone(),
            count: state.count,
        };
        println!("  [actor] GetInfo → {:?}", info);
        info
    }
}

// ============================================================================
// GENERATED BY PROC MACRO — the dispatch table
// ============================================================================
//
// This is what `#[remote_actor]` or `#[actor(remote)]` would generate.
// It's the Rust equivalent of Swift's compiler-generated accessor thunks.
// Each arm knows the concrete message type at compile time.

impl RemoteDispatch for CounterActor {
    fn dispatch_remote(
        &mut self,
        state: &mut Self::State,
        message_type: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, DispatchError> {
        match message_type {
            // --- Each arm is generated per Handler<M> impl where M: RemoteMessage ---

            <Increment as RemoteMessage>::TYPE_ID => {
                let (msg, _): (Increment, _) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                let result = self.handle(state, msg);
                bincode::serde::encode_to_vec(&result, bincode::config::standard())
                    .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
            }

            <GetCount as RemoteMessage>::TYPE_ID => {
                let (msg, _): (GetCount, _) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                let result = self.handle(state, msg);
                bincode::serde::encode_to_vec(&result, bincode::config::standard())
                    .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
            }

            <GetInfo as RemoteMessage>::TYPE_ID => {
                let (msg, _): (GetInfo, _) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                let result = self.handle(state, msg);
                bincode::serde::encode_to_vec(&result, bincode::config::standard())
                    .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
            }

            other => Err(DispatchError::UnknownMessageType(other.to_string())),
        }
    }
}

// ============================================================================
// SIMULATED RECEIVER NODE — processes incoming RemoteInvocations
// ============================================================================

/// Simulates the receiving side of a node. In the real system this would be
/// the QUIC stream handler + receptionist lookup.
async fn run_receiver(
    mut wire_rx: tokio::sync::mpsc::UnboundedReceiver<RemoteInvocation>,
    response_tx: tokio::sync::mpsc::UnboundedSender<RemoteResponse>,
) {
    // The "node" has one actor registered
    let mut actor = CounterActor;
    let mut state = CounterState {
        count: 0,
        name: "my-counter".to_string(),
    };

    while let Some(invocation) = wire_rx.recv().await {
        println!(
            "  [receiver] Got invocation: call_id={}, type={}, {} bytes",
            invocation.call_id,
            invocation.message_type,
            invocation.payload.len()
        );

        // In real system: look up actor by label in receptionist
        // For POC: we just dispatch directly
        let result = actor.dispatch_remote(&mut state, &invocation.message_type, &invocation.payload);

        let response = RemoteResponse {
            call_id: invocation.call_id,
            result: result.map_err(|e| e.to_string()),
        };
        response_tx.send(response).unwrap();
    }
}

// ============================================================================
// MAIN — run the proof of concept
// ============================================================================

#[tokio::main]
async fn main() {
    println!("=== Dispatch POC ===\n");

    // --- Test 1: Local endpoint (existing pattern, should still work) ---
    println!("--- Test 1: Local dispatch (existing pattern) ---");
    {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<CounterActor> + Send>>();
        let endpoint = Endpoint::<CounterActor> {
            sender: EndpointSender::Local { tx },
        };

        let mut actor = CounterActor;
        let mut state = CounterState { count: 0, name: "local".to_string() };

        // Spawn "supervisor"
        let handle = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                envelope.handle(&mut actor, &mut state);
            }
        });

        let result = endpoint.send(Increment { amount: 5 }).await.unwrap();
        println!("  [sender] Increment(5) → {}", result);
        assert_eq!(result, 5);

        let result = endpoint.send(GetCount).await.unwrap();
        println!("  [sender] GetCount → {}", result);
        assert_eq!(result, 5);

        drop(endpoint);
        handle.await.unwrap();
    }
    println!("  ✓ Local dispatch works\n");

    // --- Test 2: Remote dispatch via generated dispatch table ---
    println!("--- Test 2: Remote dispatch (the new mechanism) ---");
    {
        // Simulated "wire" channels
        let (wire_tx, wire_rx) = tokio::sync::mpsc::unbounded_channel::<RemoteInvocation>();
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<RemoteResponse>();

        let response_registry = ResponseRegistry::new();
        let response_registry_clone = response_registry.clone();

        // Spawn receiver node
        let receiver_handle = tokio::spawn(run_receiver(wire_rx, resp_tx));

        // Spawn response router
        let router_handle = tokio::spawn(async move {
            while let Some(response) = resp_rx.recv().await {
                response_registry_clone.complete(response);
            }
        });

        // Create remote endpoint — still typed as Endpoint<CounterActor>!
        let endpoint = Endpoint::<CounterActor> {
            sender: EndpointSender::Remote {
                actor_label: "counter/main".to_string(),
                wire_tx,
                response_registry: response_registry.clone(),
            },
        };

        // Send messages — compile-time type checked!
        // endpoint.send_remote(SomeUnhandledMsg) would NOT compile.

        let result = endpoint.send_remote(Increment { amount: 10 }).await.unwrap();
        println!("  [sender] Increment(10) → {}", result);
        assert_eq!(result, 10);

        let result = endpoint.send_remote(Increment { amount: 7 }).await.unwrap();
        println!("  [sender] Increment(7) → {}", result);
        assert_eq!(result, 17);

        let result = endpoint.send_remote(GetCount).await.unwrap();
        println!("  [sender] GetCount → {}", result);
        assert_eq!(result, 17);

        let info = endpoint.send_remote(GetInfo).await.unwrap();
        println!("  [sender] GetInfo → {:?}", info);
        assert_eq!(
            info,
            CounterInfo {
                name: "my-counter".to_string(),
                count: 17,
            }
        );

        drop(endpoint);
        receiver_handle.await.unwrap();
        router_handle.await.unwrap();
    }
    println!("  ✓ Remote dispatch works\n");

    // --- Test 3: Unknown message type (error case) ---
    println!("--- Test 3: Unknown message type error ---");
    {
        let mut actor = CounterActor;
        let mut state = CounterState { count: 0, name: "test".to_string() };
        let result = actor.dispatch_remote(&mut state, "nonexistent::Message", &[]);
        assert!(matches!(result, Err(DispatchError::UnknownMessageType(_))));
        println!("  ✓ Unknown message type correctly rejected\n");
    }

    println!("=== All tests passed! ===");
    println!("\nKey takeaways:");
    println!("  1. Endpoint<A> preserves compile-time type checking on sender side");
    println!("  2. Messages serialize to bytes with a type tag (TYPE_ID)");
    println!("  3. Receiver dispatches via generated match table — types recovered at compile time");
    println!("  4. Complex return types (CounterInfo) roundtrip correctly");
    println!("  5. The dispatch table is what a #[remote_actor] proc macro would generate");
}

fn rand_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
