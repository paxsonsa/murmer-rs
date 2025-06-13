use crate::message::RemoteMessageError;
use crate::net::{NetworkAddr, NetworkAddrRef, QuicConnectionDriver};
use std::collections::{HashMap, HashSet};
use std::{net::SocketAddr, sync::Arc};

use super::node::*;
use super::prelude::*;
use super::tls::TlsConfig;
use super::tls::TlsConfigError;

#[cfg(test)]
#[path = "cluster.test.rs"]
mod test;

#[derive(Clone)]
pub struct Name(String);

impl Name {
    pub fn hostname() -> Self {
        Name(
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
        )
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Name {
    fn from(s: &str) -> Self {
        Name(s.to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ClusterError {
    #[error("TLS Configuration error: {0}")]
    TlsError(#[from] TlsConfigError),

    #[error("Server Error: {0}")]
    ServerError(String),
}

pub enum State {
    Starting,
    Running,
    Stopping,
    Stopped,
}

pub struct ClusterId {
    pub id: Id,
    pub name: Name,
}

impl From<&str> for ClusterId {
    fn from(s: &str) -> Self {
        ClusterId {
            id: Id::new(),
            name: Name::from(s),
        }
    }
}

pub struct Config {
    pub cluster_id: Arc<ClusterId>,
    pub bind_addr: SocketAddr,
    pub peers: Vec<NetworkAddrRef>,
    pub tls: TlsConfig,
}

pub struct NodeStatus {
    pub node: Node,
    pub status: MembershipStatus,
    pub reachability: Reachability,
    pub is_configured: bool,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

pub struct ClusterActor {
    config: Config,
    socket: quinn::Endpoint,
    cancellation: tokio_util::sync::CancellationToken,
    state: State,
    // Track more detailed node status information
    members: HashMap<Id, NodeStatus>,
    // Set of configured node IDs (from initial config)
    configured_node_ids: HashSet<Id>,
}

impl ClusterActor {
    pub fn new(config: Config) -> Result<Self, ClusterError> {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let mut crypto = config.tls.clone().into_server_config()?;

        // Enable ALPN with custom protocol for murmur
        crypto.alpn_protocols = vec![b"murmur".to_vec()];

        // Create QUIC Listener with TLS
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(crypto)
                .expect("Failed to create QUIC server config"),
        ));
        let endpoint = quinn::Endpoint::server(server_config, config.bind_addr)
            .map_err(|e| ClusterError::ServerError(e.to_string()))?;

        // Create an empty set for configured node IDs
        let configured_node_ids = HashSet::new();

        let cluster = ClusterActor {
            config,
            socket: endpoint,
            cancellation: cancellation.clone(),
            state: State::Stopped,
            members: HashMap::new(),
            configured_node_ids,
        };

        // Start the monitors
        // Create a Node Instance for each peer.
        Ok(cluster)
    }

    fn spawn_server(&self, endpoint: Endpoint<ClusterActor>) -> Result<(), ClusterError> {
        let socket = self.socket.clone();
        let cancellation = self.cancellation.clone();
        tracing::info!(addr=%self.config.bind_addr, node_id=%self.config.cluster_id.name, "Starting cluster server");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        break;
                    },
                    res = socket.accept() => {
                        let Some(incoming) = res else {
                            continue;
                        };
                        accept_connection(incoming, endpoint.clone(), cancellation.child_token()).await;
                    }
                }
            }
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor for ClusterActor {
    const ACTOR_TYPE_KEY: &'static str = "system.cluster";

    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!(node_id=%self.config.cluster_id.name, "Cluster started");
        self.state = State::Starting;
        if let Err(err) = self.spawn_server(ctx.endpoint()) {
            tracing::error!(error=%err, "Failed to start cluster server");
            self.state = State::Stopped;
            return;
        }

        // Connect to each peer defined in the initial configuration
        for peer in self.config.peers.iter() {
            let node_info = NodeInfo::from(peer.clone());
            let driver = Box::new(QuicConnectionDriver::unconnected(
                node_info.clone(),
                self.socket.clone(),
                self.config.tls.clone(),
            ));

            let Some(member) = Node::spawn(
                ctx.system(),
                self.config.cluster_id.clone(),
                node_info,
                driver,
            ) else {
                tracing::error!("Failed to spawn member actor");
                continue;
            };

            // Add this node ID to the configured nodes set
            self.configured_node_ids.insert(member.id);

            // Initialize node status
            let node_status = NodeStatus {
                node: member.clone(),
                status: MembershipStatus::Pending,
                reachability: Reachability::Pending,
                is_configured: true,
                last_updated: chrono::Utc::now(),
            };

            // Add to members map
            self.members.insert(member.id, node_status);
        }

        self.state = State::Running;

        // Register with the receptionist so nodes can find us
        if let Err(e) = ctx
            .system()
            .receptionist_ref()
            .register_local(ctx.endpoint().path().clone(), ctx.endpoint().clone().into())
            .await
        {
            tracing::error!(error=?e, "Failed to register cluster actor with receptionist");
        } else {
            tracing::info!("Registered cluster actor with receptionist");
        }

        // Schedule a periodic cleanup task to remove unreachable non-configured nodes
        ctx.interval(std::time::Duration::from_secs(30), move |_| {
            CleanupUnreachableNodes {}
        });
    }
}

#[async_trait::async_trait]
impl Handler<RemoteMessage> for ClusterActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        msg: RemoteMessage,
    ) -> Result<RemoteMessage, RemoteMessageError> {
        tracing::info!(msg=?msg, "Received remote message");
        panic!("Remote message handling not implemented");
    }
}

#[async_trait::async_trait]
impl Handler<NewIncomingConnection> for ClusterActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NewIncomingConnection) {
        let addr = msg.addr;
        let addr = NetworkAddr::Socket(addr);
        let addr = NetworkAddrRef(Arc::new(addr));
        tracing::info!(addr=%addr, "new incoming connection");

        // Here we'd create a NodeActor for the incoming connection
        // For now, we'll just log it and accept it as a dynamic node

        // This is where we would:
        // 1. Create a NodeInfo for the dynamic connection
        // 2. Spawn a NodeActor for it
        // 3. Add it to our members list as a non-configured node
        let node_info = NodeInfo::new(addr);
        let connection = msg.connection;
        let driver = Box::new(QuicConnectionDriver::connected(
            node_info.clone(),
            connection,
            self.config.tls.clone(),
        ));

        let Some(node) = Node::spawn(
            ctx.system(),
            self.config.cluster_id.clone(),
            node_info,
            driver,
        ) else {
            tracing::error!("Failed to spawn member actor");
            return;
        };
        let node_status = NodeStatus {
            node: node.clone(),
            status: MembershipStatus::Pending,
            reachability: Reachability::Pending,
            is_configured: true,
            last_updated: chrono::Utc::now(),
        };

        // Add to members map
        self.members.insert(node.id, node_status);
    }
}

#[async_trait::async_trait]
impl Handler<NodeStatusUpdate> for ClusterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: NodeStatusUpdate) {
        tracing::info!(
            node_id=%msg.id,
            status=?msg.status,
            reachability=?msg.reachability,
            is_configured=%msg.is_configured,
            "Node status update"
        );

        // Update the node's status in our tracking map
        if let Some(node_status) = self.members.get_mut(&msg.id) {
            node_status.status = msg.status;
            node_status.reachability = msg.reachability;
            node_status.last_updated = msg.timestamp;

            // If the node is Down or Failed and not a configured node, consider cleaning it up
            if matches!(
                node_status.status,
                MembershipStatus::Down | MembershipStatus::Failed
            ) && !node_status.is_configured
                && matches!(node_status.reachability, Reachability::Unreachable { .. })
            {
                tracing::info!(node_id=%msg.id, "Non-configured node is unreachable, marked for cleanup");
                // We'll let the cleanup interval task handle actual removal
                // TODO: CleanUp
            }
        } else {
            tracing::warn!(node_id=%msg.id, "Received status update for unknown node");
        }
    }
}

#[async_trait::async_trait]
impl Handler<RemoveNode> for ClusterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: RemoveNode) -> bool {
        // Remove the node if it exists
        if let Some(node_status) = self.members.remove(&msg.node_id) {
            tracing::info!(
                node_id=%msg.node_id,
                status=?node_status.status,
                is_configured=%node_status.is_configured,
                "Removed node from cluster"
            );

            // If it was a configured node, keep track of that
            if node_status.is_configured {
                // We could add it to a "removed_configured_nodes" list if needed
                tracing::warn!(node_id=%msg.node_id, "Removed a configured node from cluster");
            }

            return true;
        }

        tracing::warn!(node_id=%msg.node_id, "Attempted to remove non-existent node");
        false
    }
}

#[async_trait::async_trait]
impl Handler<CleanupUnreachableNodes> for ClusterActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: CleanupUnreachableNodes) -> usize {
        // Find all non-configured nodes that are unreachable and clean them up
        let unreachable_nodes: Vec<Id> = self
            .members
            .iter()
            .filter(|(_, status)| {
                !status.is_configured
                    && matches!(
                        status.status,
                        MembershipStatus::Down | MembershipStatus::Failed
                    )
                    && matches!(status.reachability, Reachability::Unreachable { .. })
            })
            .map(|(id, _)| *id)
            .collect();

        let count = unreachable_nodes.len();

        if count > 0 {
            tracing::info!(count=%count, "Cleaning up unreachable non-configured nodes");

            // Remove each unreachable node
            for node_id in unreachable_nodes {
                self.members.remove(&node_id);
                tracing::debug!(node_id=%node_id, "Removed unreachable non-configured node");
            }
        }

        count
    }
}

async fn accept_connection(
    incoming: quinn::Incoming,
    endpoint: Endpoint<ClusterActor>,
    _cancellation: tokio_util::sync::CancellationToken,
) {
    let addr = incoming.remote_address();
    let span = tracing::debug_span!("incoming", addr=%addr);
    let _guard = span.enter();
    let connecting = match incoming.accept() {
        Ok(connection) => connection,
        Err(e) => {
            tracing::error!(error=%e, "Failed to accept connection");
            return;
        }
    };

    let connection = match connecting.await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error=%e, "Failed to establish connection");
            return;
        }
    };

    // Send a message to the cluster actor about the new incoming connection
    if let Err(err) = endpoint
        .send(NewIncomingConnection { connection, addr })
        .await
    {
        tracing::error!(error=%err, "Failed to notify cluster about new connection");
    }
}

// Message sent by a node actor to update its status
#[derive(Debug, Clone)]
pub struct NodeStatusUpdate {
    pub id: Id,
    pub status: MembershipStatus,
    pub reachability: Reachability,
    pub is_configured: bool, // Whether this node was in the initial configuration
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Message for NodeStatusUpdate {
    type Result = ();
}

// Message sent when a new connection is accepted by the server
#[derive(Debug)]
pub struct NewIncomingConnection {
    connection: quinn::Connection,
    addr: SocketAddr,
}

impl Message for NewIncomingConnection {
    type Result = ();
}

// Message to request removal of a node from the cluster
#[derive(Debug)]
pub struct RemoveNode {
    pub node_id: Id,
}

impl Message for RemoveNode {
    type Result = bool; // Returns true if the node was found and removed
}

// Message to trigger cleanup of unreachable non-configured nodes
#[derive(Debug)]
pub struct CleanupUnreachableNodes {}

impl Message for CleanupUnreachableNodes {
    type Result = usize; // Returns the number of nodes cleaned up
}

#[derive(Clone)]
pub struct Cluster {
    endpoint: Endpoint<ClusterActor>,
}

impl Cluster {
    pub fn new(endpoint: Endpoint<ClusterActor>) -> Self {
        Cluster { endpoint }
    }

    // Send a status update to the cluster
    pub async fn update_node_status(&self, update: NodeStatusUpdate) -> Result<(), SendError> {
        self.endpoint.send(update).await
    }

    // Request removal of a node from the cluster
    pub async fn remove_node(&self, node_id: Id) -> Result<bool, SendError> {
        self.endpoint.send(RemoveNode { node_id }).await
    }
}
