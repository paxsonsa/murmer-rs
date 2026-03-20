use std::sync::Arc;

use crate::{Op, OpType, Receptionist, RemoteInvocation, ResponseRegistry};

use super::framing::ControlMessage;
use super::membership::ClusterEvent;
use super::remote;
use super::transport::Transport;

use tokio::sync::{broadcast, mpsc};

// =============================================================================
// REGISTRY SYNC — exchanges OpLog deltas between peers
// =============================================================================

/// Request a delta from a peer by sending our version vector.
pub async fn request_sync_from_peer(
    transport: &Arc<Transport>,
    receptionist: &Receptionist,
    node_id: &str,
) {
    let our_vv = receptionist.version_vector();
    if let Err(e) = transport
        .send_control(node_id, ControlMessage::RegistrySyncRequest(our_vv))
        .await
    {
        tracing::warn!("Failed to request sync from {node_id}: {e}");
    }
}

/// Send our delta to a peer (ops they haven't seen).
pub async fn send_sync_to_peer(
    transport: &Arc<Transport>,
    receptionist: &Receptionist,
    node_id: &str,
    peer_vv: &crate::VersionVector,
) {
    let delta = receptionist.ops_since(peer_vv);
    if delta.is_empty() {
        return;
    }
    tracing::debug!("Sending {} ops to {node_id}", delta.len());
    if let Err(e) = transport
        .send_control(node_id, ControlMessage::RegistrySync(delta))
        .await
    {
        tracing::warn!("Failed to send sync to {node_id}: {e}");
    }
}

/// Periodic sync: exchange deltas with all connected peers.
pub async fn periodic_sync(transport: &Arc<Transport>, receptionist: &Receptionist) {
    let nodes = transport.connected_nodes().await;
    for node_id in &nodes {
        request_sync_from_peer(transport, receptionist, node_id).await;
    }
}

// =============================================================================
// TYPE REGISTRY — maps TYPE_ID strings to registration closures
// =============================================================================

/// A closure that registers a remote actor in the receptionist. The POC is a
/// single binary so all types are known at compile time. Each actor type
/// registers a factory function that creates the `Endpoint::Remote` plumbing.
pub type RemoteRegistrationFn = Box<
    dyn Fn(&Receptionist, &str, mpsc::UnboundedSender<RemoteInvocation>, ResponseRegistry, &str)
        + Send
        + Sync,
>;

/// Registry of known actor types, mapping TYPE_ID → registration function.
/// Populated at startup; used when applying remote Register ops.
pub struct TypeRegistry {
    factories: std::collections::HashMap<String, RemoteRegistrationFn>,
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self {
            factories: std::collections::HashMap::new(),
        }
    }

    /// Register a factory for a given actor type name.
    pub fn register(&mut self, actor_type_name: impl Into<String>, factory: RemoteRegistrationFn) {
        self.factories.insert(actor_type_name.into(), factory);
    }

    /// Look up a factory by actor type name.
    pub fn get(&self, actor_type_name: &str) -> Option<&RemoteRegistrationFn> {
        self.factories.get(actor_type_name)
    }

    pub fn known_types(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

impl Default for TypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SPAWN REGISTRY — maps actor type names to local instantiation factories
// =============================================================================

/// Error returned when a remote spawn request fails.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("unknown actor type: {0}")]
    UnknownType(String),
    #[error("failed to deserialize state: {0}")]
    DeserializeFailed(String),
    #[error("failed to start actor: {0}")]
    StartFailed(String),
}

/// A factory function that can instantiate an actor locally from serialized state.
///
/// Given a receptionist, a label, and serialized state bytes (from `MigratableActor`),
/// the factory deserializes the state and calls `receptionist.start(label, actor, state)`.
pub type SpawnFactory = Box<
    dyn Fn(&Receptionist, &str, &[u8]) -> Result<(), SpawnError> + Send + Sync,
>;

/// Registry of actor types that can be spawned remotely.
///
/// Parallel to [`TypeRegistry`] (which creates remote *endpoints*), the
/// `SpawnRegistry` creates actual *running actors* from serialized state.
/// Used when a Coordinator sends a `SpawnActor` control message.
///
/// # Example
///
/// ```rust,ignore
/// let mut spawn_registry = SpawnRegistry::new();
/// spawn_registry.register("my_app::Worker", Box::new(|receptionist, label, state_bytes| {
///     let (state, _): (WorkerState, _) = bincode::serde::decode_from_slice(
///         state_bytes, bincode::config::standard()
///     ).map_err(|e| SpawnError::DeserializeFailed(e.to_string()))?;
///     receptionist.start(label, Worker, state);
///     Ok(())
/// }));
/// ```
pub struct SpawnRegistry {
    factories: std::collections::HashMap<String, SpawnFactory>,
}

impl SpawnRegistry {
    pub fn new() -> Self {
        Self {
            factories: std::collections::HashMap::new(),
        }
    }

    /// Register a factory for spawning actors of the given type.
    pub fn register(&mut self, actor_type_name: impl Into<String>, factory: SpawnFactory) {
        self.factories.insert(actor_type_name.into(), factory);
    }

    /// Look up a factory by actor type name.
    pub fn get(&self, actor_type_name: &str) -> Option<&SpawnFactory> {
        self.factories.get(actor_type_name)
    }

    /// Spawn an actor using the registered factory.
    pub fn spawn(
        &self,
        receptionist: &Receptionist,
        label: &str,
        actor_type_name: &str,
        state_bytes: &[u8],
    ) -> Result<(), SpawnError> {
        let factory = self
            .get(actor_type_name)
            .ok_or_else(|| SpawnError::UnknownType(actor_type_name.to_string()))?;
        factory(receptionist, label, state_bytes)
    }

    /// List all registered actor type names.
    pub fn known_types(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

impl Default for SpawnRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// APPLY REMOTE OPS — process ops received from peers
// =============================================================================

/// Apply ops received from a peer. For Register ops where we know the actor
/// type, creates the remote endpoint plumbing. For unknown types, we still
/// store the op for relay but don't create an endpoint.
pub fn apply_remote_ops(
    ops: Vec<Op>,
    receptionist: &Receptionist,
    type_registry: &TypeRegistry,
    _remote_node_id: &str,
    event_tx: &broadcast::Sender<ClusterEvent>,
    transport: &Arc<Transport>,
) {
    // First, let the receptionist apply ops for its VersionVector tracking
    // and event emission (Register events are emitted even for unknown types).
    receptionist.apply_ops(ops.clone());

    // For Register ops where we know the type, set up the remote endpoint.
    // We check if an entry already exists (apply_ops might have been called
    // before, or the actor is local to us).
    for op in &ops {
        if let OpType::Register {
            label,
            actor_type_name,
            ..
        } = &op.op_type
        {
            // Skip if this is our own op
            if op.node_id == receptionist.node_id() {
                continue;
            }

            // Check if an entry already exists for this label
            if receptionist.has_entry(label) {
                continue;
            }

            if let Some(factory) = type_registry.get(actor_type_name) {
                let (wire_tx, wire_rx) = mpsc::unbounded_channel();
                let response_registry = ResponseRegistry::new();

                // Register the remote actor
                factory(
                    receptionist,
                    label,
                    wire_tx,
                    response_registry.clone(),
                    &op.node_id,
                );

                // Use the origin node's identity (op.node_id) as the stream
                // target, NOT the relay node that forwarded this op.  In a
                // 3-node setup (A → B → C), if B relays A's ops to C, C must
                // open the stream to A (the origin), not B (the relay).
                //
                // op.node_id is the origin's `node_id_string()` which is also
                // the key in Transport.connections.  If no direct connection to
                // the origin exists yet the stream writer will fail fast and
                // the next sync cycle will retry after the event loop
                // establishes the connection.
                let target_node_id = op.node_id.clone();

                tokio::spawn(remote::run_actor_stream_writer(
                    Arc::clone(transport),
                    target_node_id,
                    label.clone(),
                    wire_rx,
                    response_registry,
                ));

                let _ = event_tx.send(ClusterEvent::ActorRegistered {
                    label: label.clone(),
                    node_id: op.node_id.clone(),
                    actor_type: actor_type_name.clone(),
                });

                tracing::info!(
                    "Registered remote actor {label} (type {actor_type_name}) from node {}",
                    op.node_id
                );
            } else {
                tracing::debug!(
                    "Unknown actor type {actor_type_name} for {label} — stored op for relay"
                );
            }
        }
    }
}
