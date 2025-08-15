use crate::cluster::{
    ClusterConfig, ClusterRuntime, ClusterTransport, ClusterTransportError, MessageType,
    NodeIdentity, TimerEvent,
};
use crate::node::NodeServer;
use foca::{BincodeCodec, Foca};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
/// Errors that can occur while managing the cluster runtime, transport, or node server.
pub enum ClusterError {
    /// Error originating from the cluster transport (network I/O, serialization, addressing).
    #[error("Transport error: {0}")]
    Transport(#[from] ClusterTransportError),

    /// Error originating from the FOCA SWIM implementation used for membership and gossip.
    #[error("Foca error: {0}")]
    Foca(#[from] foca::Error),

    /// Error originating from the NodeServer QUIC layer (connections, streams, framing).
    #[error("Node server error: {0}")]
    NodeServer(#[from] crate::node::NodeServerError),

    /// The cluster manager or its runtime components have not been started/initialized.
    /// This is typically returned when attempting to start twice or when a one-shot
    /// receiver (e.g., gossip receiver or timer receiver) was already taken.
    #[error("Cluster not started")]
    NotStarted,
}

#[derive(Debug, Clone)]
/// Events emitted by the cluster subsystem to describe membership changes and updates.
///
/// Subscribe via `ClusterManager::subscribe()` to receive a stream of these events.
pub enum ClusterEvent {
    /// A node successfully joined the cluster and is considered healthy.
    NodeJoined(NodeIdentity),
    /// A node voluntarily left the cluster (graceful shutdown or explicit leave).
    NodeLeft(NodeIdentity),
    /// A node is considered failed/unreachable by the failure detector (e.g. SWIM suspicion timeout).
    NodeFailed(NodeIdentity),
    /// An existing node's metadata was updated (e.g. address, incarnation, labels).
    NodeUpdated(NodeIdentity),
}

/// Cluster manager orchestrates SWIM membership and gossip using FOCA over QUIC.
/// It bridges the NodeServer transport with FOCA via a custom runtime integrated with Tokio.
pub struct ClusterManager {
    /// FOCA instance that implements SWIM membership, failure detection, and gossip.
    foca: Foca<
        NodeIdentity,
        BincodeCodec<bincode::config::Configuration>,
        rand::rngs::ThreadRng,
        foca::NoCustomBroadcast,
    >,

    /// Identity of this local node (id, address, incarnation, metadata).
    local_identity: NodeIdentity,

    /// Snapshot of known members keyed by their socket address for quick lookups and introspection.
    members: Arc<RwLock<HashMap<std::net::SocketAddr, NodeIdentity>>>,

    /// Broadcast channel for emitting higher-level cluster events to subscribers.
    event_tx: tokio::sync::broadcast::Sender<ClusterEvent>,

    /// QUIC-backed server handling connections/streams used to carry cluster gossip.
    node_server: Arc<NodeServer>,

    /// Indicates whether the manager's async loop is currently running.
    running: Arc<tokio::sync::RwLock<bool>>,

    /// Watch channel used to request a graceful shutdown of the manager loop.
    shutdown_tx: tokio::sync::watch::Sender<bool>,

    /// Custom FOCA runtime that adapts timers, I/O, and notifications to Tokio/NodeServer.
    custom_runtime: ClusterRuntime,

    /// Receiver for runtime timer events; consumed by start() to drive FOCA timers.
    timer_rx: Option<tokio::sync::mpsc::UnboundedReceiver<TimerEvent>>,
}

impl ClusterManager {
    pub async fn new(
        local_identity: NodeIdentity,
        node_server: Arc<NodeServer>,
        cluster_config: ClusterConfig,
    ) -> Result<Self, ClusterError> {
        let config = cluster_config.to_foca_config();

        // Create QUIC-based transport
        let local_addr = node_server.local_addr()?;
        let transport = ClusterTransport::new_with_node_server(node_server.clone(), local_addr)?;

        let (event_tx, _) = tokio::sync::broadcast::channel(100);
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        // Create custom runtime that integrates directly with tokio
        let mut custom_runtime =
            ClusterRuntime::new(transport, local_identity.clone(), event_tx.clone());
        let timer_rx = custom_runtime.take_timer_receiver();

        // Create standard Foca - we'll manually integrate our custom runtime
        let codec = BincodeCodec(bincode::config::standard());
        let foca = Foca::new(local_identity.clone(), config, rand::rng(), codec);

        Ok(Self {
            foca,
            local_identity,
            members: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            node_server,
            running: Arc::new(tokio::sync::RwLock::new(false)),
            shutdown_tx,
            custom_runtime,
            timer_rx,
        })
    }

    /// Join an existing cluster by contacting a known member
    pub async fn join_cluster(
        &mut self,
        seed_nodes: Vec<NodeIdentity>,
    ) -> Result<(), ClusterError> {
        tracing::info!("Joining cluster via {} seed nodes", seed_nodes.len());

        for seed_node in seed_nodes {
            tracing::info!("Adding seed node to cluster: {}", seed_node);

            // Announce to Foca using our custom runtime
            self.foca
                .announce(seed_node.clone(), &mut self.custom_runtime)?;
        }

        Ok(())
    }

    /// Start the cluster management runtime
    pub async fn start(&mut self) -> Result<(), ClusterError> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Ok(());
            }
            *running = true;
        }

        tracing::info!("Starting cluster manager for {}", self.local_identity);

        // Take the gossip receiver from the node server
        let mut gossip_rx = self
            .node_server
            .take_gossip_receiver()
            .await
            .ok_or(ClusterError::NotStarted)?;

        // Take the timer receiver from our custom runtime
        let mut timer_rx = self.timer_rx.take().ok_or(ClusterError::NotStarted)?;

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut gossip_interval = tokio::time::interval(Duration::from_millis(1000));

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }

                // Handle incoming gossip messages from QUIC streams
                recv_result = gossip_rx.recv() => {
                    if let Some((from_node_id, cluster_message)) = recv_result {
                        match cluster_message.message_type {
                            MessageType::Gossip => {
                                // Handle gossip message with our custom runtime
                                if let Err(e) = self.foca.handle_data(&cluster_message.payload, &mut self.custom_runtime) {
                                    tracing::debug!("Failed to handle gossip message from {}: {}", from_node_id, e);
                                }
                            }
                            _ => {
                                tracing::trace!("Ignoring cluster message type: {:?}", cluster_message.message_type);
                            }
                        }
                    }
                }

                // Handle timer events from our custom runtime
                timer_event = timer_rx.recv() => {
                    if let Some(event) = timer_event {
                        if let Err(e) = self.foca.handle_timer(event.timer, &mut self.custom_runtime) {
                            tracing::debug!("Failed to handle timer event: {}", e);
                        }
                        // Clean up the expired timer
                        self.custom_runtime.handle_timer_expired(event.timer_id);
                    }
                }

                // Send periodic gossip
                _ = gossip_interval.tick() => {
                    if let Err(e) = self.foca.gossip(&mut self.custom_runtime) {
                        tracing::debug!("Failed to initiate gossip: {}", e);
                    }
                }
            }
        }

        tracing::info!("Cluster manager stopped");
        Ok(())
    }

    /// Stop the cluster manager
    pub async fn shutdown(&mut self) {
        {
            let mut running = self.running.write().await;
            *running = false;
        }
        let _ = self.shutdown_tx.send(true);
        tracing::info!("Cluster manager shutdown requested");
    }

    /// Get current cluster members
    pub async fn get_members(&self) -> Vec<NodeIdentity> {
        let members = self.members.read().await;
        members.values().cloned().collect()
    }

    /// Get number of cluster members
    pub async fn member_count(&self) -> usize {
        let members = self.members.read().await;
        members.len()
    }

    /// Subscribe to cluster events
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ClusterEvent> {
        self.event_tx.subscribe()
    }

    /// Get local node identity
    pub fn local_identity(&self) -> &NodeIdentity {
        &self.local_identity
    }

    // Private methods - with custom runtime, most logic is handled automatically
    // The custom runtime handles sends, timers, and notifications directly
}

impl Drop for ClusterManager {
    fn drop(&mut self) {
        // Send shutdown signal when dropped
        let _ = self.shutdown_tx.send(true);
    }
}
