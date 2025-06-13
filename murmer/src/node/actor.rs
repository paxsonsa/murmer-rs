//! Core NodeActor implementation for distributed node communication

use crate::{
    actor::{Actor, Handler},
    context::Context,
    net::{Connection, FrameBody, NetworkAddrRef},
};
use super::{
    connection::ConnectionState,
    errors::NodeError,
    messages::{NodeInfo, Payload, RecvFrame},
    status::{NodeState, ReachabilityStatus, MembershipStatus},
};

// Include trait implementations from submodules
use std::time::Duration;

/// The main NodeActor that handles distributed communication
pub struct NodeActor {
    pub(super) info: NodeInfo,
    pub(super) state: NodeState,
    pub(super) membership_status: MembershipStatus,
    pub(super) reachability: ReachabilityStatus,
    pub(super) connection: ConnectionState,
    pub(super) receptionist: crate::receptionist::Receptionist,
}

impl NodeActor {
    /// Create a NodeActor for outgoing connections (we initiate)
    pub async fn connect(
        network_address: NetworkAddrRef,
        mut connection: Box<dyn Connection>,
        receptionist: crate::receptionist::Receptionist,
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
            receptionist,
        })
    }

    /// Create a NodeActor for incoming connections (they initiate)
    pub async fn accept(
        network_address: NetworkAddrRef,
        mut connection: Box<dyn Connection>,
        receptionist: crate::receptionist::Receptionist,
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
            receptionist,
        };
        Ok(node)
    }

    /// Handle Join message from remote node
    async fn handle_join(&mut self, remote_id: crate::id::Id, ctx: &mut Context<Self>) {
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

    /// Handle JoinAck message from remote node
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

    /// Handle heartbeat message from remote node
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

    /// Handle leave message from remote node
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

    /// Handle info message from remote node
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

    /// Handle ActorAdd message from remote node
    async fn handle_actor_add(&mut self, remote_path: crate::path::ActorPath, ctx: &mut Context<Self>) {
        // Handle the ActorAdd message from the remote node.
        tracing::info!(
            "Node {} received ActorAdd message for actor {:?} from remote node {:?}",
            self.info.network_address,
            remote_path,
            self.info.remote_id
        );

        // FIXME: AI Generated Please Review
        let Some(remote_id) = self.info.remote_id else {
            tracing::error!(
                "Cannot handle ActorAdd - remote node ID not set for connection {}",
                self.info.network_address
            );
            return;
        };

        // Transform the path to reflect the actual remote location
        let transformed_path = self.transform_remote_path(remote_path);
        
        // Create a Node from this NodeActor's endpoint
        let node_endpoint = ctx.endpoint();
        let local_endpoint = crate::system::LocalEndpoint::from_endpoint(node_endpoint);
        let node = crate::node::Node {
            id: remote_id,
            endpoint: local_endpoint,
        };
        
        // Create a RemoteEndpointFactory for lazy proxy creation
        let factory = Box::new(crate::system::RemoteEndpointFactory::new(
            node,
            transformed_path.clone(),
        ));
        
        // Register the remote actor with the local receptionist
        if let Err(err) = self.receptionist.register_remote(transformed_path.clone(), factory).await {
            tracing::error!(
                "Failed to register remote actor {:?} from node {:?}: {}",
                transformed_path,
                remote_id,
                err
            );
        } else {
            tracing::debug!(
                "Successfully registered remote actor {:?} from node {:?}",
                transformed_path,
                remote_id
            );
        }
    }

    /// Handle ActorRemove message from remote node
    async fn handle_actor_remove(&mut self, remote_path: crate::path::ActorPath, _ctx: &mut Context<Self>) {
        // Handle the ActorRemove message from the remote node.
        tracing::info!(
            "Node {} received ActorRemove message for actor {:?} from remote node {:?}",
            self.info.network_address,
            remote_path,
            self.info.remote_id
        );

        // FIXME: AI Generated Please Review
        let Some(remote_id) = self.info.remote_id else {
            tracing::error!(
                "Cannot handle ActorRemove - remote node ID not set for connection {}",
                self.info.network_address
            );
            return;
        };

        // Transform the path to reflect the actual remote location
        let transformed_path = self.transform_remote_path(remote_path);
        
        // Deregister the remote actor from the local receptionist
        if let Err(err) = self.receptionist.deregister(transformed_path.clone()).await {
            tracing::error!(
                "Failed to deregister remote actor {:?} from node {:?}: {}",
                transformed_path,
                remote_id,
                err
            );
        } else {
            tracing::debug!(
                "Successfully deregistered remote actor {:?} from node {:?}",
                transformed_path,
                remote_id
            );
        }
    }

    /// Start heartbeat monitoring tasks
    async fn start_heartbeat_tasks(&mut self, ctx: &mut Context<Self>) {
        // Send heartbeat every 3 seconds
        ctx.interval(Duration::from_secs(3), move |cancellation| super::messages::SendHeartbeat {
            cancellation: cancellation.clone(),
        });
        
        // Check for missed heartbeats every 3 seconds
        ctx.interval(Duration::from_secs(3), move |cancellation| super::messages::CheckHeartbeat {
            reference_time: chrono::Utc::now(),
            cancellation: cancellation.clone(),
        });
    }

    /// Subscribe to receptionist events and start broadcasting local actor changes to remote nodes
    async fn start_receptionist_subscription(&mut self, ctx: &mut Context<Self>) {
        // Subscribe to all local actor events
        let filter = crate::receptionist::SubscriptionFilter {
            actor_type: "*".to_string(), // Subscribe to all actor types
            group_pattern: None,         // No group filtering
        };

        match self.receptionist.subscribe(filter).await {
            Ok(mut subscription) => {
                tracing::info!(
                    "NodeActor {} subscribed to receptionist events",
                    self.info.network_address
                );

                // Start a task to handle registry events
                let endpoint = ctx.endpoint();
                ctx.spawn(async move {
                    while let Some(event) = subscription.next().await {
                        tracing::debug!("NodeActor received registry event: {:?}", event);

                        // Send the event back to the NodeActor for processing
                        let handle_event_msg = super::messages::HandleRegistryEvent { event };
                        
                        if let Err(err) = endpoint.send(handle_event_msg).await {
                            tracing::error!(
                                "Failed to send registry event to NodeActor: {}",
                                err
                            );
                            break;
                        }
                    }
                    tracing::info!("NodeActor registry event subscription ended");
                });
            }
            Err(err) => {
                tracing::error!(
                    "Failed to subscribe to receptionist events for NodeActor {}: {}",
                    self.info.network_address,
                    err
                );
            }
        }
    }

    /// Transform a remote actor path to reflect its actual remote location
    fn transform_remote_path(&self, remote_path: crate::path::ActorPath) -> crate::path::ActorPath {
        let _remote_id = self.info.remote_id.expect("Remote ID must be set");
        
        // For now, we'll create a remote scheme based on the network address
        // TODO: Extract actual host/port from network address
        let scheme = crate::path::AddressScheme::Remote {
            host: "127.0.0.1".to_string(), // TODO: Extract from network_address
            port: 4000, // TODO: Extract from network_address
        };
        
        crate::path::ActorPath {
            scheme,
            type_id: remote_path.type_id,
            group_id: remote_path.group_id,
            instance_id: remote_path.instance_id,
        }
    }

    /// Broadcast an ActorAdd message to the remote node
    async fn broadcast_actor_add(&mut self, actor_path: crate::path::ActorPath) {
        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            let payload = Payload::ActorAdd(actor_path.clone());
            if let Err(err) = send_stream.send(payload).await {
                tracing::error!(
                    "Failed to broadcast ActorAdd for {:?} to remote node {:?}: {}",
                    actor_path,
                    self.info.remote_id,
                    err
                );
            } else {
                tracing::debug!(
                    "Successfully broadcast ActorAdd for {:?} to remote node {:?}",
                    actor_path,
                    self.info.remote_id
                );
            }
        }
    }

    /// Broadcast an ActorRemove message to the remote node
    async fn broadcast_actor_remove(&mut self, actor_path: crate::path::ActorPath) {
        if let ConnectionState::Established { send_stream, .. } = &mut self.connection {
            let payload = Payload::ActorRemove(actor_path.clone());
            if let Err(err) = send_stream.send(payload).await {
                tracing::error!(
                    "Failed to broadcast ActorRemove for {:?} to remote node {:?}: {}",
                    actor_path,
                    self.info.remote_id,
                    err
                );
            } else {
                tracing::debug!(
                    "Successfully broadcast ActorRemove for {:?} to remote node {:?}",
                    actor_path,
                    self.info.remote_id
                );
            }
        }
    }

    /// Handle Initialize message from remote node
    async fn handle_initialize(&mut self, remote_id: crate::id::Id, _ctx: &mut Context<Self>) {
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

        // Subscribe to receptionist events to broadcast local actor changes
        self.start_receptionist_subscription(ctx).await;

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

#[async_trait::async_trait]
impl Handler<RecvFrame> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: RecvFrame) -> () {
        // Handle the received frame message.
        tracing::info!("Handling received frame: {:?}", msg.frame);
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
            Payload::ActorAdd(actor_path) => self.handle_actor_add(actor_path, ctx).await,
            Payload::ActorRemove(actor_path) => self.handle_actor_remove(actor_path, ctx).await,
        }
    }
}

// Handler implementations for message types

#[async_trait::async_trait]
impl Handler<super::messages::SendHeartbeat> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: super::messages::SendHeartbeat) -> () {
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

#[async_trait::async_trait]
impl Handler<super::messages::CheckHeartbeat> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: super::messages::CheckHeartbeat) -> () {
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

#[async_trait::async_trait]
impl Handler<super::messages::HandleRegistryEvent> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: super::messages::HandleRegistryEvent) -> () {
        // Only broadcast events for established connections
        if !matches!(self.connection, ConnectionState::Established { .. }) {
            tracing::debug!("Connection not established, skipping registry event broadcast");
            return;
        }

        match msg.event {
            crate::receptionist::RegistryEvent::Added(actor_path, _endpoint) => {
                // Only broadcast local actors (don't re-broadcast remote actors)
                if actor_path.is_local() {
                    tracing::debug!(
                        "Broadcasting ActorAdd for local actor {:?} to remote node {:?}",
                        actor_path,
                        self.info.remote_id
                    );
                    self.broadcast_actor_add(actor_path).await;
                }
            }
            crate::receptionist::RegistryEvent::Removed(actor_path) => {
                // Only broadcast local actors (don't re-broadcast remote actors)
                if actor_path.is_local() {
                    tracing::debug!(
                        "Broadcasting ActorRemove for local actor {:?} to remote node {:?}",
                        actor_path,
                        self.info.remote_id
                    );
                    self.broadcast_actor_remove(actor_path).await;
                }
            }
        }
    }
}