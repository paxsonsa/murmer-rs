//! Message types and payloads for NodeActor communication

use serde::{Deserialize, Serialize};
use crate::{
    message::Message,
    net::Frame,
    receptionist::RegistryEvent,
};
// Forward declaration - NodeActor is defined in actor.rs

/// Node information for network communication
#[derive(Debug)]
pub struct NodeInfo {
    /// Unique identifier for the node.
    pub remote_id: Option<crate::id::Id>,
    /// Network address of the node.
    pub network_address: crate::net::NetworkAddrRef,
}

/// The message payloads used for communication between nodes in the cluster.
///
/// This enum defines the different types of messages that can be sent and received
/// between nodes in the cluster.
///
/// In a connection there are is a sender and receiver which we will call the connector and
/// acceptor. The connector is the node that initiates the connection, while the acceptor is the node that
/// accepts the connection.
///
/// 1) The `Initialize` payload is sent by the connector to initiate the connection.
/// 2) The `Join` payload is sent by the acceptor to join the cluster.
/// 3) The `JoinAck` payload is sent by the connector to acknowledge the join request. At this
///    point the link is considered established and the connector can start sending messages to the
///    acceptor.
///
/// **Heartbeats**
/// Both the connector and acceptor send heartbeats to each other to ensure that the connection is
/// still alive. It is important that both nodes send heartbeats to each other, as this allows
/// the nodes to detect if the other node is still reachable. The heartbeat controls the
/// reachability status of the node, which is used to determine if the node is healthy enough to
/// communicate with.
///
/// **Leave**
/// The `Leave` payload can be sent by either the connector or acceptor to indicate that they are
/// leaving the cluster. This payload is used to gracefully close the connection and inform the
/// other node that it is no longer part of the cluster.
///
/// **Info**
/// The `Info` payload can be used to send information about the node, such as its capabilities and
/// certain metadata.
///
/// **ActorAdd/ActorRemove**
/// These payloads are used to add or remove actors from the node. These payloads are used to
/// exchange the current actor state between the nodes in the cluster and ensure that any
/// publically accessible actors are available on both nodes. These should be exchanged by both
/// sides of the connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// Represents the initialization payload sent when establishing a connection.
    Initialize,
    /// Represents a message sent to join the cluster.
    Join,
    /// Represents an acknowledgment of a join request.
    JoinAck,
    /// Represents a heartbeat message to check the health of the connection.
    Heartbeat,
    /// Represents a leave message to gracefully disconnect.
    Leave,
    /// Represents an info message for metadata exchange.
    Info,
    /// Represents a message to add an actor to the remote node's registry
    ActorAdd(crate::path::ActorPath),
    /// Represents a message to remove an actor from the remote node's registry
    ActorRemove(crate::path::ActorPath),
}

/// Message for receiving frames from the network
#[derive(Debug)]
pub struct RecvFrame {
    pub frame: Frame<Payload>,
}

impl Message for RecvFrame {
    type Result = ();
}

/// Message for handling registry events from the receptionist
#[derive(Debug)]
pub struct HandleRegistryEvent {
    pub event: RegistryEvent,
}

impl Message for HandleRegistryEvent {
    type Result = ();
}

/// Message for sending heartbeats
#[derive(Debug)]
pub struct SendHeartbeat {
    /// Cancellation token for the heartbeat task.
    pub cancellation: tokio_util::sync::CancellationToken,
}

impl Message for SendHeartbeat {
    type Result = ();
}

/// Message for checking heartbeats
#[derive(Debug)]
pub struct CheckHeartbeat {
    /// Timestamp of the last heartbeat check.
    pub reference_time: chrono::DateTime<chrono::Utc>,
    /// Cancellation token for the heartbeat check task.
    pub cancellation: tokio_util::sync::CancellationToken,
}

impl Message for CheckHeartbeat {
    type Result = ();
}