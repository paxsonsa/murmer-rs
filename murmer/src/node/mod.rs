//! # Node Module
//! 
//! The node module provides distributed actor communication capabilities through the NodeActor.
//! 
//! ## Purpose & Role
//! 
//! The NodeActor represents a **connection to a remote node** in a distributed actor cluster. 
//! It's responsible for maintaining the network connection, managing the cluster membership 
//! lifecycle, and facilitating communication between local and remote actors.
//! 
//! ## Core Architecture
//! 
//! ### State Management
//! 
//! The NodeActor tracks multiple state dimensions:
//! - **NodeState**: Lifecycle (Initiating → Accepting → Running → Stopped/Failed)
//! - **MembershipStatus**: Cluster membership (Joining → Up → Down) 
//! - **ReachabilityStatus**: Health monitoring (Pending → Reachable ⇄ Unreachable)
//! - **ConnectionState**: Network layer (Pending → Established → Disconnected)
//! 
//! ### Connection Establishment Flow
//! 
//! **Connector (initiating) side:**
//! ```text
//! 1. Send Initialize message
//! 2. Receive Join message (with remote node ID)
//! 3. Send JoinAck 
//! 4. Start heartbeats → Running state
//! ```
//! 
//! **Acceptor (receiving) side:**
//! ```text
//! 1. Receive Initialize message  
//! 2. Send Join message (with own node ID)
//! 3. Receive JoinAck
//! 4. Start heartbeats → Running state
//! ```
//! 
//! ### Message Processing Pipeline
//! 
//! The actor runs a continuous loop that:
//! 1. **Reads frames** from the network connection
//! 2. **Deserializes** them into Payload types
//! 3. **Dispatches** to appropriate handlers:
//!    - `Initialize` → connection setup
//!    - `Join/JoinAck` → membership negotiation  
//!    - `Heartbeat` → health monitoring
//!    - `Leave` → graceful shutdown
//!    - `Info` → metadata exchange (stub)
//!    - `ActorAdd/Remove` → distributed registry sync (stubs)
//! 
//! ### Heartbeat System
//! 
//! Two concurrent processes maintain connection health:
//! 
//! **SendHeartbeat task** (every 3s):
//! - Sends Heartbeat messages to remote node
//! - Cancels itself if connection fails
//! 
//! **CheckHeartbeat task** (every 3s):  
//! - Monitors if heartbeats were received since last check
//! - Tracks missed heartbeats (>3 = Unreachable)
//! - Tracks successful heartbeats (>3 = Reachable again)
//! 
//! ### Lifecycle Management
//! 
//! **Actor startup (`started()`):**
//! - Transitions connection from Pending → Established
//! - Spawns network reading task 
//! - Begins message processing
//! 
//! **Actor shutdown (`stopping()`):**
//! - Sends Leave message for graceful disconnect
//! - Allows remote node to clean up
//! 
//! ## Key Design Patterns
//! 
//! ### Bidirectional Communication
//! Both nodes can initiate messages and both maintain heartbeats, creating a symmetric relationship.
//! 
//! ### Fault Tolerance
//! - Heartbeat timeouts detect network partitions
//! - State machines handle reconnection scenarios
//! - Graceful Leave messages prevent resource leaks
//! 
//! ### Distributed Actor Registry
//! The ActorAdd/Remove messages (when implemented) will synchronize which actors are available 
//! on each node, enabling the receptionist pattern for distributed actor discovery.
//! 
//! ## Data Flow
//! ```text
//! Local Actor → System → Receptionist → NodeActor → Network → Remote NodeActor → Remote Receptionist → Remote Actor
//! ```
//! 
//! The NodeActor essentially acts as a **network proxy** that bridges the local actor system 
//! with remote actor systems, handling all the distributed systems concerns (connection management, 
//! health monitoring, membership) while presenting a clean interface to the actor layer above.

use crate::cluster::ClusterId;
use crate::net::{self, NetworkAddrRef};
use crate::prelude::*;
use chrono::{DateTime, Utc};
use std::sync::Arc;

mod actor;
use actor::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipStatus {
    Pending,
    Up,
    Down,
    Joining,
    Leaving,
    Removed,
    Failed,
}

#[derive(Debug, Clone)]
pub enum Reachability {
    Pending,
    Reachable {
        // The number of consecutive heartbeat misses.
        misses: u32,
        // The last time the node's heartbeat/message was received.
        last_seen: DateTime<Utc>,
    },
    Unreachable {
        pings: u32,
        last_seen: DateTime<Utc>,
    },
}

impl PartialEq for Reachability {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pending, Self::Pending) => true,
            (Self::Reachable { misses: s_m, .. }, Self::Reachable { misses: o_m, .. }) => {
                s_m == o_m
            }
            (Self::Unreachable { pings: s_p, .. }, Self::Unreachable { pings: o_p, .. }) => {
                s_p == o_p
            }
            _ => false,
        }
    }
}

impl Eq for Reachability {}

impl Reachability {
    pub fn reachable_now() -> Self {
        Reachability::Reachable {
            misses: 0,
            last_seen: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct NodeInfo {
    /// The name of the node.
    pub display_name: String,
    /// The network address of the node.
    pub addr: NetworkAddrRef,
}

impl NodeInfo {
    pub fn new(addr: NetworkAddrRef) -> Self {
        NodeInfo {
            display_name: format!("{}", addr),
            addr,
        }
    }

    fn new_with_name(name: impl Into<String>, addr: NetworkAddrRef) -> Self {
        NodeInfo {
            display_name: name.into(),
            addr,
        }
    }
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "node(name={}, addr={})", self.display_name, self.addr)
    }
}

impl From<NetworkAddrRef> for NodeInfo {
    fn from(addr: NetworkAddrRef) -> Self {
        NodeInfo::new(addr)
    }
}

#[derive(Clone)]
pub struct Node {
    pub id: Id,
    pub endpoint: crate::system::LocalEndpoint<NodeActor>,
}

impl Node {
    pub fn spawn(
        system: System,
        cluster_id: Arc<ClusterId>,
        node_info: NodeInfo,
        driver: Box<dyn net::ConnectionDriver>,
    ) -> Option<Node> {
        todo!("Implement Node::spawn");
    }

    /// Create a remote proxy endpoint for a given actor path
    /// This method creates an endpoint that will communicate with a remote actor
    /// through this node's connection
    pub fn create_remote_proxy<A: crate::actor::Actor>(
        &self,
        path: crate::path::ActorPath,
    ) -> Result<crate::system::Endpoint<A>, NodeError> {
        // Create a RemoteProxy for network communication
        let proxy = crate::system::RemoteProxy::new(self.clone(), path.clone());
        
        // Create an EndpointSender using the remote proxy
        let sender = crate::system::EndpointSender::from_remote_proxy(proxy);
        
        // Create and return the typed endpoint
        Ok(crate::system::Endpoint::new(sender, std::sync::Arc::new(path)))
    }
}

impl Eq for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("Member not connected")]
    NotConnected,

    #[error("Node network error: {0}")]
    NodeNetworkError(#[from] net::NetError),
}
