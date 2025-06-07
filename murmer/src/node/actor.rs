// TODO: Handle the actor lifecycle properly, including starting, stopping, and error handling.
// ✅ DONE: Handle Leave - implemented in handle_leave() and stopping() lifecycle
// ✅ DONE: Handle Info - stub implemented in handle_info()
// ✅ DONE: Handle ActorAdd/ActorRemove - stubs implemented
// ✅ DONE: Handle Initialize - implemented acceptor flow in handle_initialize()
// TODO: ActorAdd: Create a new remote endpoint for the actor and add it the receptionist
// TODO: ActorRemove: Remove the remote endpoint for the actor and remove it from the receptionist
// TODO: Send ActorAdd/ActorRemove messages to the remote node when actors are added or removed to
// the receptionist.
// TODO: Consider having a receptionist public status?
// Actor <- -> Node <- -> Node <- -> Actor
use std::{fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    actor::{Actor, Handler},
    context::Context,
    id::Id,
    message::Message,
    net::{
        Connection, ConnectionError, Frame, FrameBody, FrameReader, FrameWriter, NetworkAddrRef,
    },
};

use super::MembershipStatus;

pub struct NodeInfo {
    /// Unique identifier for the node.
    pub remote_id: Option<Id>,
    /// Network address of the node.
    pub network_address: NetworkAddrRef,
}

#[cfg(test)]
#[path = "actor.test.rs"]
mod tests;

/// Represents the reachability status of a node in the cluster.
/// Indicates whether the node is reachable or unreachable based on heartbeat checks.
/// The reachability status is used to determine if the node is healthy enough to communicate with.
///
#[derive(Debug)]
pub enum ReachabilityStatus {
    /// The node's reachability is currently unknown or not yet determined.
    Pending,
    /// The node is reachable and has been successfully communicating.
    Reachable {
        /// The node is reachable and can be communicated with.
        last_seen: chrono::DateTime<chrono::Utc>,
        /// The number of missed heartbeats since the last successful communication.
        /// After a certain threshold, the node may be considered unreachable until it responds
        /// again.
        missed_heartbeats: u32,
    },
    Unreachable {
        /// The node is unreachable and cannot be communicated with.
        last_seen: chrono::DateTime<chrono::Utc>,
        /// The number of successful heartbeats since the last missed heartbeat.
        /// After a certain threshold, the node may be considered reachable again.
        successful_heartbeat: u32,
    },
}

impl ReachabilityStatus {
    /// Get the last time the node was seen, if available
    pub fn last_seen_time(&self) -> chrono::DateTime<chrono::Utc> {
        match self {
            ReachabilityStatus::Reachable { last_seen, .. } => *last_seen,
            ReachabilityStatus::Unreachable { last_seen, .. } => *last_seen,
            ReachabilityStatus::Pending => chrono::Utc::now(), // Default to now for pending nodes
        }
    }
}

#[derive(Debug)]
pub enum NodeState {
    Initiating,
    Accepting,
    Running,
    Stopped,
    Failed {
        /// The reason for the failure, if available.
        reason: String,
    },
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
///
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Payload {
    /// Represents the initialization payload sent when establishing a connection.
    Initialize,
    /// Represents a message sent to join the cluster.
    Join,
    /// Represents an acknowledgment of a join request.
    JoinAck,
    /// Represents a heartbeat message to check the health of the connection.
    Heartbeat,
    Leave,
    Info,
    ActorAdd,
    ActorRemove,
}

pub(super) enum ConnectionState {
    /// Represents a connection that is currently being established.
    Pending {
        /// The handle to the connection driver used to establish the connection.
        handle: Option<Box<dyn Connection>>,

        /// The stream used for communication with the remote node, if available.
        read_stream: Option<FrameReader<Payload>>,

        /// The stream used for sending data to the remote node, if available.
        send_stream: Option<FrameWriter<Payload>>,
    },
    Established {
        handle: Option<Box<dyn Connection>>,
        /// The stream used for communication with the remote node.
        send_stream: FrameWriter<Payload>,
    },
    /// Represents a closed or disconnected connection.
    Disconnected {
        handle: Option<Box<dyn Connection>>,
        /// The reason for the disconnection, if available.
        reason: Option<ConnectionError>,
    },
}

impl ConnectionState {
    /// Checks if the connection is in a pending state.
    pub fn is_pending(&self) -> bool {
        matches!(self, ConnectionState::Pending { .. })
    }

    /// Checks if the connection is established.
    pub fn is_established(&self) -> bool {
        matches!(self, ConnectionState::Established { .. })
    }

    /// Checks if the connection is disconnected.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, ConnectionState::Disconnected { .. })
    }

    pub fn to_established(&mut self) -> Option<(ConnectionState, FrameReader<Payload>)> {
        // Transition from Pending to Established state and return the read stream.
        if let ConnectionState::Pending {
            handle,
            read_stream,
            send_stream,
        } = self
        {
            let new_state = ConnectionState::Established {
                handle: Some(handle.take().expect("Connection handle must be available")),
                send_stream: send_stream.take().expect("Send stream must be available"),
            };
            let read_stream = read_stream.take().expect("Read stream must be available");
            Some((new_state, read_stream))
        } else {
            None
        }
    }
}

impl Debug for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Pending { .. } => write!(f, "ConnectionState::Pending"),
            ConnectionState::Established { .. } => write!(f, "ConnectionState::Established"),
            ConnectionState::Disconnected { reason, .. } => {
                write!(f, "ConnectionState::Disconnected({:?})", reason)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("Failed while initiating connection: {0}")]
    InitiationFailed(String),
    #[error("Connection error: {0}")]
    ConnectionError(#[from] ConnectionError),
    #[error("Network error: {0}")]
    NetworkError(#[from] crate::net::NetError),
}

pub struct NodeActor {
    pub(super) info: NodeInfo,
    pub(super) state: NodeState,
    pub(super) membership_status: MembershipStatus,
    pub(super) reachability: ReachabilityStatus,
    pub(super) connection: ConnectionState,
}

impl NodeActor {
    pub async fn connect(
        network_address: NetworkAddrRef,
        mut connection: Box<dyn Connection>,
    ) -> Result<Self, NodeError> {
        // Connection is established without error, attempt to open a new stream
        // as the main pipe for communication with the remote node.
        let stream = connection
            .open_stream()
            .await
            .map_err(NodeError::ConnectionError)?;

        let (mut sender, receiver) = stream.split_into::<Payload>();
        let payload = Payload::Initialize {};
        sender
            .send(payload)
            .await
            .map_err(NodeError::NetworkError)?;

        Ok(NodeActor {
            info: NodeInfo {
                remote_id: None,
                network_address,
            },
            state: NodeState::Initiating,
            reachability: ReachabilityStatus::Pending,
            membership_status: MembershipStatus::Joining,
            connection: ConnectionState::Pending {
                handle: Some(connection),
                read_stream: Some(receiver),
                send_stream: Some(sender),
            },
        })
    }

    pub async fn accept(
        network_address: NetworkAddrRef,
        mut connection: Box<dyn Connection>,
    ) -> Result<Self, NodeError> {
        // Connection is established without error, attempt to open a new stream
        // as the main pipe for communication with the remote node.
        let stream = connection
            .accept_stream()
            .await
            .map_err(NodeError::ConnectionError)?
            .accept()
            .await?;

        let (sender, receiver) = stream.split_into::<Payload>();
        let node = NodeActor {
            info: NodeInfo {
                remote_id: None,
                network_address,
            },
            state: NodeState::Accepting,
            membership_status: MembershipStatus::Joining,
            reachability: ReachabilityStatus::Pending,
            connection: ConnectionState::Pending {
                handle: Some(connection),
                read_stream: Some(receiver),
                send_stream: Some(sender),
            },
        };
        Ok(node)
    }

    async fn handle_join(&mut self, remote_id: Id, ctx: &mut Context<Self>) {
        // Update the node's info with the remote ID.
        tracing::debug!(
            "Node {} is joining with remote ID: {}",
            self.info.network_address,
            remote_id
        );
        self.info.remote_id = Some(remote_id);

        // Send a JoinAck response back to the joining node.
        let payload = Payload::JoinAck;
        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            if let Err(err) = send_stream
                .send(payload)
                .await
                .map_err(NodeError::NetworkError)
            {
                tracing::error!("Failed to send JoinAck: {}", err);
                self.state = NodeState::Failed {
                    reason: format!("Failed to send JoinAck: {}", err),
                };
                self.membership_status = MembershipStatus::Down;
                return;
            };
        }
        self.membership_status = MembershipStatus::Up;
        self.state = NodeState::Running;
        
        // Start heartbeat tasks for the connector side (we sent JoinAck, so handshake is complete)
        self.start_heartbeat_tasks(ctx).await;
    }

    async fn handle_join_ack(&mut self, ctx: &mut Context<Self>) {
        // Handle the JoinAck message from the remote node.
        tracing::debug!(
            "Node {} received JoinAck from remote node.",
            self.info.network_address
        );

        // Update the membership status to Running.
        self.membership_status = MembershipStatus::Up;

        // Start heartbeat tasks to maintain connection health.
        self.start_heartbeat_tasks(ctx).await;
    }

    async fn handle_heartbeat(&mut self) {
        // Handle the heartbeat message from the remote node.
        tracing::debug!(
            "Node {} received heartbeat from remote node.",
            self.info.network_address
        );

        // Update the reachability status to reachable.
        match &mut self.reachability {
            ReachabilityStatus::Reachable {
                last_seen,
                missed_heartbeats,
            } => {
                *last_seen = chrono::Utc::now();
                *missed_heartbeats = 0; // Reset missed heartbeats on successful heartbeat.
            }
            ReachabilityStatus::Unreachable {
                last_seen,
                successful_heartbeat,
            } => {
                *last_seen = chrono::Utc::now();
                *successful_heartbeat += 1; // Increment successful heartbeat count.
            }
            _ => {}
        }
    }

    async fn handle_leave(&mut self) {
        // Handle the Leave message from the remote node.
        tracing::info!(
            "Node {} received Leave message from remote node {:?}",
            self.info.network_address,
            self.info.remote_id
        );

        // Update the membership status to Down and state to Stopped for graceful shutdown
        self.membership_status = MembershipStatus::Down;
        self.state = NodeState::Stopped;
        
        // Mark the node as unreachable since it's leaving
        self.reachability = ReachabilityStatus::Unreachable {
            last_seen: chrono::Utc::now(),
            successful_heartbeat: 0,
        };

        // TODO: Implement graceful connection cleanup
        // For now, we just log the Leave message and update the state
    }

    async fn handle_info(&mut self, _ctx: &mut Context<Self>) {
        // Handle the Info message from the remote node.
        tracing::info!(
            "Node {} received Info message from remote node {:?}",
            self.info.network_address,
            self.info.remote_id
        );

        // TODO: Implement Info message handling
        // This could include exchanging node capabilities, metadata, or status information
    }

    async fn handle_actor_add(&mut self, _ctx: &mut Context<Self>) {
        // Handle the ActorAdd message from the remote node.
        tracing::info!(
            "Node {} received ActorAdd message from remote node {:?}",
            self.info.network_address,
            self.info.remote_id
        );

        // TODO: Create a new remote endpoint for the actor and add it to the receptionist
        // This will enable remote actor discovery and communication
    }

    async fn handle_actor_remove(&mut self, _ctx: &mut Context<Self>) {
        // Handle the ActorRemove message from the remote node.
        tracing::info!(
            "Node {} received ActorRemove message from remote node {:?}",
            self.info.network_address,
            self.info.remote_id
        );

        // TODO: Remove the remote endpoint for the actor and remove it from the receptionist
        // This will clean up remote actor references
    }

    async fn handle_initialize(&mut self, remote_id: Id, _ctx: &mut Context<Self>) {
        // Handle the Initialize message from the remote node.
        tracing::info!(
            "Node {} received Initialize message from remote node {}",
            self.info.network_address,
            remote_id
        );

        // Set the remote node ID from the Initialize message
        self.info.remote_id = Some(remote_id);

        // Respond with a Join message to complete the acceptor handshake
        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            let payload = Payload::Join;
            if let Err(err) = send_stream.send(payload).await {
                tracing::error!(
                    "Failed to send Join response to Initialize from remote node {}: {}",
                    remote_id,
                    err
                );
                self.state = NodeState::Failed {
                    reason: format!("Failed to send Join response: {}", err),
                };
                self.membership_status = MembershipStatus::Down;
            } else {
                tracing::info!(
                    "Sent Join response to Initialize from remote node {}",
                    remote_id
                );
                self.membership_status = MembershipStatus::Joining;
                self.state = NodeState::Running;
            }
        } else {
            tracing::error!(
                "Connection not established, cannot send Join response to remote node {}",
                remote_id
            );
        }
    }

    async fn start_heartbeat_tasks(&mut self, ctx: &mut Context<Self>) {
        ctx.interval(Duration::from_secs(3), move |cancellation| SendHeartbeat {
            cancellation: cancellation.clone(),
        });
        ctx.interval(Duration::from_secs(3), move |cancellation| CheckHeartbeat {
            reference_time: chrono::Utc::now(),
            cancellation: cancellation.clone(),
        });
    }
}

#[async_trait::async_trait]
impl Actor for NodeActor {
    const ACTOR_TYPE_KEY: &'static str = "NodeActor";

    /// Called when the actor is started but before it begins processing messages.
    /// Use this to perform any initialization.
    async fn started(&mut self, ctx: &mut Context<Self>) {
        // Initialize the connection state and start processing messages.
        let Some((state, mut read_stream)) = self.connection.to_established() else {
            tracing::error!("Failed to establish connection state.");
            return;
        };
        self.connection = state;
        self.membership_status = MembershipStatus::Joining;

        // Start the read stream for the node connection.
        let endpoint = ctx.endpoint();
        let inner_ctx = ctx.clone();
        ctx.spawn(async move {
            loop {
                match read_stream.read_frame().await {
                    Ok(frame) => {
                        // Process the received frame.
                        // Handle the frame according to your application logic.
                        let Some(frame) = frame else {
                            // If the frame is None, it indicates the stream has been closed.
                            tracing::info!("Node stream closed.");
                            continue;
                        };
                        let recv_frame = RecvFrame { frame };
                        if let Err(err) = endpoint.send(recv_frame).await {
                            // Handle the error, possibly log or notify the actor.
                            tracing::error!(
                                "Failed to send received frame, stopping the read loop: {}",
                                err
                            );
                            return inner_ctx.cancel();
                        }
                    }
                    Err(e) => {
                        // Handle read error, possibly log or notify the actor.
                        tracing::error!("Error reading from node stream: {}", e);
                    }
                }
            }
        });
    }

    /// Called when the actor is about to be shut down, before processing remaining messages.
    /// Use this to prepare for shutdown.
    async fn stopping(&mut self, _ctx: &mut Context<Self>) {
        // Send a Leave message to gracefully disconnect from the remote node
        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            let payload = Payload::Leave;
            if let Err(err) = send_stream.send(payload).await {
                tracing::error!(
                    "Failed to send Leave message during shutdown to remote node {:?}: {}", 
                    self.info.remote_id, 
                    err
                );
            } else {
                tracing::info!(
                    "Sent Leave message to remote node {:?} during shutdown", 
                    self.info.remote_id
                );
            }
        } else {
            tracing::debug!("Connection not established, skipping Leave message");
        }
    }

    /// Called after the actor has been shut down and finished processing messages.
    /// Use this for final cleanup.
    async fn stopped(&mut self, _ctx: &mut Context<Self>) {}
}

#[derive(Debug)]
struct RecvFrame {
    frame: Frame<Payload>,
}

impl Message for RecvFrame {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<RecvFrame> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: RecvFrame) -> () {
        // Handle the received frame message.
        tracing::info!("Handling received frame: {:?}", msg.frame);
        // ctx.interval(Duration::from_secs(3), move || SendHeartbeat {});
        let header = msg.frame.header;
        let payload = match msg.frame.body {
            FrameBody::Ok(payload) => payload,
            FrameBody::UnhandledFailure(err) => {
                tracing::error!("Unhandled failure in frame: {}", err);
                return;
            }
        };

        // Process the frame according to your application logic.
        match payload {
            Payload::Initialize => self.handle_initialize(header.sender_id, ctx).await,
            Payload::Join => self.handle_join(header.sender_id, ctx).await,
            Payload::JoinAck => self.handle_join_ack(ctx).await,
            Payload::Heartbeat => self.handle_heartbeat().await,
            Payload::Leave => self.handle_leave().await,
            Payload::Info => self.handle_info(ctx).await,
            Payload::ActorAdd => self.handle_actor_add(ctx).await,
            Payload::ActorRemove => self.handle_actor_remove(ctx).await,
        }
    }
}

#[derive(Debug)]
struct SendHeartbeat {
    /// Cancellation token for the heartbeat task.
    pub cancellation: tokio_util::sync::CancellationToken,
}

impl Message for SendHeartbeat {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<SendHeartbeat> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SendHeartbeat) -> () {
        // Send a heartbeat message to the remote node.
        tracing::debug!(
            "Sending heartbeat to remote node: {:?}",
            self.info.remote_id.unwrap()
        );

        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            let payload = Payload::Heartbeat;
            if let Err(err) = send_stream
                .send(payload)
                .await
                .map_err(NodeError::NetworkError)
            {
                tracing::error!("Failed to send heartbeat: {}", err);
            }
        } else {
            tracing::error!("Connection is not established, cannot send heartbeat.");
            self.state = NodeState::Failed {
                reason: "Connection is not established, cannot send heartbeat.".to_string(),
            };
            self.membership_status = MembershipStatus::Down;
            msg.cancellation.cancel();
        }
    }
}

#[derive(Debug)]
struct CheckHeartbeat {
    /// Timestamp of the last heartbeat check.
    pub reference_time: chrono::DateTime<chrono::Utc>,
    /// Cancellation token for the heartbeat check task.
    pub cancellation: tokio_util::sync::CancellationToken,
}

impl Message for CheckHeartbeat {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<CheckHeartbeat> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: CheckHeartbeat) -> () {
        tracing::info!(
            "Checking heartbeat for remote node: {:?}",
            self.info.remote_id.expect("Remote ID must be set")
        );

        // Check the node state and invoke the cancellation token when
        // the node is in a failed state.
        if matches!(self.state, NodeState::Failed { .. }) {
            tracing::warn!(
                "Node {} is in a failed state, cancelling heartbeat check.",
                self.info.network_address
            );
            msg.cancellation.cancel();
            return;
        }

        // Get the current time
        let now = chrono::Utc::now();

        // Check if the node has sent a heartbeat since the reference time
        // If not, we consider it a missed heartbeat
        let exceeded_threshold = match &self.reachability {
            ReachabilityStatus::Reachable { last_seen, .. } => {
                // If the last_seen time is older than our reference time,
                // it means we haven't received a heartbeat since the check started
                *last_seen <= msg.reference_time
            }
            ReachabilityStatus::Unreachable { last_seen, .. } => {
                // Same check for unreachable nodes
                *last_seen <= msg.reference_time
            }
            ReachabilityStatus::Pending => {
                // For pending nodes, we consider it as exceeded
                true
            }
        };

        if exceeded_threshold {
            // If we haven't received a heartbeat since the reference time
            match &mut self.reachability {
                ReachabilityStatus::Reachable {
                    last_seen,
                    missed_heartbeats,
                } => {
                    *missed_heartbeats += 1; // Increment missed heartbeat count.
                    tracing::debug!(
                        "Node {} missed heartbeat. Count: {}",
                        self.info.network_address,
                        missed_heartbeats
                    );

                    if *missed_heartbeats > 3 {
                        // If missed heartbeats exceed threshold, mark as unreachable.
                        self.reachability = ReachabilityStatus::Unreachable {
                            last_seen: *last_seen,
                            successful_heartbeat: 0,
                        };
                        self.membership_status = MembershipStatus::Down;
                        tracing::warn!(
                            "Node {} is marked as unreachable due to missed heartbeats.",
                            self.info.network_address
                        );
                    }
                }
                ReachabilityStatus::Unreachable { last_seen, .. } => {
                    // Reset successful heartbeat count for unreachable nodes
                    self.reachability = ReachabilityStatus::Unreachable {
                        last_seen: *last_seen,
                        successful_heartbeat: 0,
                    };
                }
                ReachabilityStatus::Pending => {
                    // Initialize as unreachable after timeout
                    self.reachability = ReachabilityStatus::Unreachable {
                        last_seen: now,
                        successful_heartbeat: 0,
                    };
                    self.membership_status = MembershipStatus::Down;
                }
            }
        } else {
            // We received a heartbeat since the reference time
            match &mut self.reachability {
                ReachabilityStatus::Unreachable {
                    successful_heartbeat,
                    ..
                } => {
                    *successful_heartbeat += 1; // Increment successful heartbeat count.
                    tracing::debug!(
                        "Node {} successful heartbeat. Count: {}",
                        self.info.network_address,
                        successful_heartbeat
                    );

                    if *successful_heartbeat > 3 {
                        // If successful heartbeats exceed threshold, mark as reachable again.
                        self.reachability = ReachabilityStatus::Reachable {
                            last_seen: now,
                            missed_heartbeats: 0,
                        };
                        tracing::info!(
                            "Node {} is marked as reachable again after successful heartbeats.",
                            self.info.network_address
                        );
                        self.membership_status = MembershipStatus::Up;
                    }
                }
                ReachabilityStatus::Reachable {
                    last_seen,
                    missed_heartbeats,
                } => {
                    *last_seen = now; // Update last seen time.
                    *missed_heartbeats = 0; // Reset missed heartbeat count.
                }
                ReachabilityStatus::Pending => {
                    // Initialize as reachable
                    self.reachability = ReachabilityStatus::Reachable {
                        last_seen: now,
                        missed_heartbeats: 0,
                    };
                    self.membership_status = MembershipStatus::Up;
                }
            }
        }
    }
}
