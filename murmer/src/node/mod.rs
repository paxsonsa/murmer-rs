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
//! ## Usage
//! 
//! ```rust
//! // Create outgoing connection
//! let node = Node::connect(system, cluster_id, address, connection).await?;
//! 
//! // Accept incoming connection  
//! let node = Node::accept(system, cluster_id, address, connection).await?;
//! 
//! // Create remote proxy for distributed actor communication
//! let proxy = node.create_remote_proxy::<MyActor>(actor_path)?;
//! ```

// Core types
pub mod actor;
pub mod connection;
pub mod errors;
pub mod messages;
pub mod node;
pub mod status;

// Public API re-exports
pub use actor::NodeActor;
pub use errors::{NodeError, NodeCreationError};
pub use messages::NodeInfo;
pub use node::Node;
pub use status::{ReachabilityStatus, MembershipStatus};

#[cfg(test)]
#[path = "actor.test.rs"]
mod tests;