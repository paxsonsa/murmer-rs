use crate::{
    id::MaybeId,
    net::{self, ActorMessage, FrameReader, FrameWriter, NetworkDriver},
    receptionist::RawKey,
};
use chrono::{DateTime, Utc};
use std::time::Duration;

use super::*;

enum State {
    /// Initial State of a node actor that represents a actor that is initializing a connection to
    /// some remote node.
    Init,

    /// Intial state of a node actor that represents a actor that is accepting a connection from
    /// some remote node.
    InitAccept,

    /// Initial acknowledgement has been received or sent and is awaiting the final steps to become a
    /// running instance.
    InitAck,

    /// Initialized and now running. All incoming init/join events are error'd.
    Running,
}

/// The node actor is responsible for managing the connection to a remote node in the cluster.
///
/// This actor handles the initial connection and sending and receiving messages to/from the remote
/// node in the cluster and managing proxies for actors on the remote node.
///
pub struct NodeActor {
    /// A unique identifier for host this actor is running on.
    pub host_id: Id,
    pub instance_id: Id,
    pub remote_id: MaybeId,
    pub node_info: NodeInfo,
    pub driver: Box<dyn NetworkDriver>,
    pub membership: Status,
    pub reachability: Reachability,
    pub send_stream: Option<FrameWriter<net::NodeMessage>>,
    inner_state: State,
}

impl NodeActor {
    pub fn new(
        host_id: Id,
        instance_id: Id,
        node_info: NodeInfo,
        driver: Box<dyn NetworkDriver>,
    ) -> Self {
        NodeActor {
            host_id,
            instance_id,
            remote_id: MaybeId::unset(),
            node_info,
            driver,
            membership: Status::Pending,
            reachability: Reachability::Pending,
            send_stream: None,
            inner_state: State::Init,
        }
    }

    // Send a status update to the cluster
    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn update_status_and_notify(&mut self, ctx: &mut Context<Self>) {
        // Log the status change for debugging
        tracing::debug!(
            id=%self.instance_id,
            status=?self.membership,
            reachability=?self.reachability,
            "Node status changed"
        );

        // Try to find the cluster actor through the receptionist
        let key = crate::receptionist::Key::<crate::cluster::ClusterActor>::default();

        // Attempt to lookup the cluster actor but don't require it to be present
        // This enables testing without needing to mock the cluster
        let lookup_result = ctx.system().receptionist_ref().lookup_one(key).await;

        if let Ok(endpoint) = lookup_result {
            let cluster = crate::cluster::Cluster::new(endpoint);

            // Determine if this is a configured node (based on whether it was initialized from config)
            // For now we'll assume all nodes were configured - in a real implementation
            // we'd track this information properly
            let is_configured = true;

            // Send the status update
            let status_update = crate::cluster::NodeStatusUpdate {
                id: self.instance_id.clone(),
                status: self.membership.clone(),
                reachability: self.reachability.clone(),
                is_configured,
                timestamp: chrono::Utc::now(),
            };

            if let Err(err) = cluster.update_node_status(status_update).await {
                tracing::error!(error=%err, "Failed to send status update to cluster");
            } else {
                tracing::debug!(
                    instance=%self.instance_id,
                    "Successfully sent status update to cluster"
                );
            }
        } else {
            // This is expected in tests where no cluster is registered
            tracing::debug!("No cluster actor registered, skipping status update");
        }
    }

    #[tracing::instrument(skip(self, message), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn send_message(&mut self, message: net::NodeMessage) -> Result<(), NodeError> {
        if let Some(ref mut stream) = self.send_stream {
            let frame = net::Frame::ok(self.host_id.clone(), self.remote_id.get(), message);
            if let Err(e) = stream.write_frame(&frame).await {
                tracing::error!(error=%e, "Failed to send message");
                return Err(NodeError::NodeNetworkError(e));
            }
            Ok(())
        } else {
            Err(NodeError::NotConnected)
        }
    }

    #[tracing::instrument(skip(self), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_init_frame(&mut self, protocol_version: u16, id: Id) {
        // Validate protocol version
        if protocol_version != net::CURRENT_PROTOCOL_VERSION {
            tracing::error!(
                received=%protocol_version,
                expected=%net::CURRENT_PROTOCOL_VERSION,
                "Protocol version mismatch"
            );
            // We could send a Disconnect message here, but for now just ignore
            return;
        }

        // Store the remote node ID
        self.remote_id = MaybeId::new(id);

        // Send InitAck response
        let init_ack = net::NodeMessage::InitAck {
            node_id: self.host_id.clone(),
        };

        // Send the InitAck message
        if let Err(err) = self.send_message(init_ack).await {
            tracing::error!(error=%err, "Failed to send InitAck");
            return;
        }

        // Update state
        self.membership = Status::Pending;
        self.reachability = Reachability::reachable_now();
        self.inner_state = State::InitAck;
    }

    #[tracing::instrument(skip(self), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_init_ack_frame(&mut self, remote_id: Id) {
        tracing::info!("Received InitAck");
        let State::Init = self.inner_state else {
            tracing::error!("Invalid state for InitAck");
            // TODO: Send Error
            return;
        };
        self.remote_id = MaybeId::new(remote_id);
        self.inner_state = State::InitAck;
        self.membership = Status::Joining;
        self.reachability = Reachability::reachable_now();

        // Send Join Message
        let join_message = net::NodeMessage::Join {
            name: "default".to_string(),
            capabilities: Vec::new(),
        };

        if let Err(err) = self.send_message(join_message).await {
            tracing::error!(error=%err, "Failed to send join message");
            self.membership = Status::Failed;
            self.inner_state = State::Init;
        }
    }

    #[tracing::instrument(skip(self, ctx, ping_time), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_heartbeat_frame(&mut self, ctx: &mut Context<Self>, ping_time: DateTime<Utc>) {
        let old_reachability = self.reachability.clone();
        match self.reachability {
            Reachability::Unreachable { pings, last_seen } => {
                self.reachability = Reachability::Unreachable {
                    pings: if last_seen.signed_duration_since(ping_time)
                        > chrono::TimeDelta::seconds(-3)
                    {
                        pings + 1
                    } else {
                        0
                    },
                    last_seen: ping_time,
                };
            }
            Reachability::Reachable { misses, .. } => {
                self.reachability = Reachability::Reachable {
                    misses,
                    last_seen: ping_time,
                };
            }
            Reachability::Pending => {
                self.reachability = Reachability::Reachable {
                    misses: 0,
                    last_seen: ping_time,
                };
            }
        };

        // If reachability changed, notify the cluster
        if self.reachability != old_reachability {
            tracing::debug!(
                old_reachability=?old_reachability,
                new_reachability=?self.reachability,
                "Node reachability changed"
            );
            self.update_status_and_notify(ctx).await;
        }
    }

    #[tracing::instrument(skip(self, ctx, name, capabilities), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_join_frame(
        &mut self,
        ctx: &mut Context<Self>,
        name: String,
        capabilities: Vec<String>,
    ) {
        tracing::info!(name=%name, ?capabilities, "Received Join message");

        // A node is trying to join our cluster
        // Update membership state for the remote node
        self.membership = Status::Joining;

        // Respond with JoinAck
        let join_ack = net::NodeMessage::JoinAck {
            // TODO: Support Cluster Node Name
            // name: self.cluster_name,
            accepted: true,
            reason: None,
        };

        if let Err(err) = self.send_message(join_ack).await {
            tracing::error!(error=%err, "Failed to send JoinAck");
            return;
        }

        // Once we've accepted the join, the node is considered Up
        self.node_info.display_name = name;
        self.membership = Status::Up;
        self.inner_state = State::Running;
        self.update_status_and_notify(ctx).await;
    }

    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_join_ack_frame(
        &mut self,
        ctx: &mut Context<Self>,
        accepted: bool,
        reason: Option<String>,
    ) {
        if accepted {
            tracing::info!("Join accepted");
            self.membership = Status::Up;
            self.inner_state = State::Running;

            // Send first heartbeat
            let heartbeat = net::NodeMessage::Heartbeat {
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            if let Err(err) = self.send_message(heartbeat).await {
                tracing::error!(error=%err, "Failed to send initial heartbeat");
            }
        } else {
            tracing::error!(reason=?reason, "Join rejected");
            self.inner_state = State::Init;
            self.membership = Status::Failed;
        }
        self.update_status_and_notify(ctx).await;

        // Start the Accept Stream Hook
        let Ok(mut accept_stream) = self.driver.accept_stream().await else {
            tracing::error!("Failed to accept stream");
            // TODO: Failure Handling by sending disconnect message.
            self.membership = Status::Failed;
            self.inner_state = State::Init;
            return;
        };

        // Begin a new accept stream of incoming streams from the remote node.
        let endpoint = ctx.endpoint();
        let _accept_cancellation = ctx.spawn(async move {
            loop {
                match accept_stream.accept().await {
                    Ok(stream) => {
                        // Handle the new stream
                        tracing::debug!("Accepted new actor stream from remote node");
                        // Create a new local actor proxy for the accepted stream
                        let msg = NodeActorAcceptStreamMessage(stream);
                        endpoint.send(msg).await.unwrap_or_else(|err| {
                            tracing::error!(error=%err, "Failed to send local actor message");
                        });
                    }
                    Err(err) => {
                        tracing::error!(error=%err, "Failed to accept stream");
                        // If we get a connection error, we might want to break the loop
                        // depending on the error type
                        if matches!(
                            err,
                            net::ConnectionError::ConnectionClosed(_)
                                | net::ConnectionError::Reset
                                | net::ConnectionError::TimedOut
                                | net::ConnectionError::LocallyClosed
                        ) {
                            break;
                        }
                        // For other errors, we might want to retry after a delay
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle_disconnect_frame(&mut self, ctx: &mut Context<Self>, reason: String) {
        tracing::error!(reason=%reason, "Received Disconnect message"); // Use error level for visibility

        // Update node status
        self.membership = Status::Down;
        self.reachability = Reachability::Unreachable {
            pings: 0,
            last_seen: Utc::now(),
        };

        // Close the connection
        // This would be done by dropping the connection in the driver
        // We'll set the send_stream to None to prevent further sends
        self.send_stream = None;

        // Notify the cluster about disconnection
        self.update_status_and_notify(ctx).await;
    }
}

#[async_trait::async_trait]
impl Actor for NodeActor {
    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!("Member started");

        // Update membership state
        self.membership = Status::Pending;
        self.reachability = Reachability::Pending;

        let mut stream_rx = match self.driver.connected() {
            // We are connected already so we are accepting a stream instead of opening one.
            true => {
                let Ok(mut accept_stream) = self.driver.accept_stream().await else {
                    tracing::error!("Failed to accept stream");
                    self.membership = Status::Failed;
                    self.inner_state = State::InitAccept;
                    return;
                };

                let raw_stream = match accept_stream.accept().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to accept stream");
                        self.membership = Status::Failed;
                        self.inner_state = State::InitAccept;
                        return;
                    }
                };
                // Convert the raw stream to a typed stream
                let (stream_tx, stream_rx) = raw_stream.into_frame_stream::<net::NodeMessage>();
                self.send_stream = Some(stream_tx);
                stream_rx
            }
            // We are not connected yet so we are opening a stream instead of accepting one.
            false => {
                // Establish the connection regardless of the current state
                if let Err(err) = self.driver.connect().await {
                    tracing::error!(error=%err, "Failed to establish connection");
                    self.membership = Status::Failed;
                    self.reachability = Reachability::Unreachable {
                        pings: 0,
                        last_seen: Utc::now(),
                    };
                    return;
                }

                // Open new raw stream for initial cluster communication
                let raw_stream = match self.driver.open_stream().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to establish member connection stream");
                        self.membership = Status::Failed;
                        self.reachability = Reachability::Unreachable {
                            pings: 0,
                            last_seen: Utc::now(),
                        };
                        return;
                    }
                };

                // Convert the raw stream to a typed stream
                let (stream_tx, stream_rx) = raw_stream.into_frame_stream::<net::NodeMessage>();
                self.send_stream = Some(stream_tx);

                // Send initial message
                let init = net::NodeMessage::Init {
                    protocol_version: 1, // FIXME: Use a real protocol version
                    host_id: self.host_id,
                };
                if let Err(err) = self.send_message(init).await {
                    tracing::error!(error=%err, "Failed to send initial message");
                    self.membership = Status::Failed;
                    return;
                }
                stream_rx
            }
        };

        // Start the receive loop for the main node stream
        let endpoint = ctx.endpoint();
        let instance_id = self.instance_id.clone();
        let host_id = self.host_id.clone();
        let mut running = true;
        ctx.spawn(async move {
            let span = tracing::trace_span!("node-rx-loop", instance_id=%instance_id, host_id=%host_id);
            let _enter = span.enter();
            while running {
                let frame = match stream_rx.read_frame().await {
                    Ok(Some(frame)) => Ok(frame),
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to read stream acknowledgment");
                        running = false;
                        Err(e)
                    }
                };
                tracing::trace!("Received frame: {:?}", frame);
                if let Err(err) = endpoint
                    .send_in_background(NodeActorRecvFrameMessage(frame))
                    .await
                {
                    tracing::error!(error=%err, "Failed to send frame message to member actor. exiting read loop.");
                    running = false;
                };
            }
        });

        // Setup a heartbeat deadman switch to periodically check the reachability of the node
        ctx.interval(Duration::from_secs(1), || NodeActorHeartbeatCheckMessage {
            timestamp: Utc::now(),
        });

        // Setup a timer to periodically send heartbeats to the remote node
        ctx.interval(Duration::from_secs(5), || NodeActorSendHeartbeatMessage {});
    }
}

#[derive(Debug)]
pub struct NodeActorRecvFrameMessage(pub Result<net::Frame<net::NodeMessage>, net::NetError>);

impl Message for NodeActorRecvFrameMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorRecvFrameMessage> for NodeActor {
    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorRecvFrameMessage) {
        tracing::info!("Received frame");
        let frame = match msg.0 {
            Ok(frame) => frame,
            Err(e) => {
                tracing::error!(error=%e, "Failed frame");
                return;
            }
        };
        let _header = frame.header;
        let payload = frame.payload;

        let msg = match payload {
            net::Payload::Ok(msg) => msg,
            net::Payload::UnhandledFailure(reason) => {
                tracing::error!(reason=%reason, "Received failure message");
                return;
            }
        };

        match msg {
            net::NodeMessage::Init {
                protocol_version,
                host_id,
            } => self.handle_init_frame(protocol_version, host_id).await,
            net::NodeMessage::InitAck { node_id } => {
                tracing::info!(?node_id, "Received InitAck");
                // Handle this message asynchronously
                // Call our async init_ack method to properly process the InitAck
                self.handle_init_ack_frame(node_id).await;
            }
            net::NodeMessage::Join { name, capabilities } => {
                self.handle_join_frame(ctx, name, capabilities).await
            }
            net::NodeMessage::JoinAck { accepted, reason } => {
                self.handle_join_ack_frame(ctx, accepted, reason).await
            }
            net::NodeMessage::Heartbeat { .. } => {
                // Update reachability with new heartbeat
                let now = Utc::now();
                self.handle_heartbeat_frame(ctx, now).await;
            }
            net::NodeMessage::Disconnect { reason } => {
                self.handle_disconnect_frame(ctx, reason).await;
            }
            net::NodeMessage::ActorAdd { key, instance_id } => {
                // Handle remote actor registration - after proxy is established
                let Some(remote_id) = self.remote_id.get() else {
                    tracing::error!("remote node ID is not set, cannot add actor.");
                    return;
                };
                
                // Open a stream to the remote actor
                let raw_stream = match self.driver.open_stream().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to open stream for actor add");
                        return;
                    }
                };

                let endpoint = ctx.endpoint();
                let receptionist = ctx.system().receptionist_ref().clone();
                let node_info = self.node_info.clone();
                let host_id = self.host_id.clone();
                let (stream_tx, stream_rx) = raw_stream.into_frame_stream::<net::ActorMessage>();

                // We'll spawn the proxy opening and let it handle registration
                // only after successfully establishing the connection
                ctx.spawn(async move {
                    // First establish the proxy connection
                    if let Err(e) = RemoteProxy::open_and_register(
                        host_id,
                        endpoint,
                        remote_id,
                        key,
                        instance_id,
                        stream_tx,
                        stream_rx,
                        receptionist,
                        node_info,
                    ).await {
                        tracing::error!(error=%e, "Failed to establish remote actor proxy");
                    }
                });
            }
            net::NodeMessage::ActorRemove { key, actor_path, instance_id: _ } => {
                // Handle remote actor removal
                // We need to check if remote ID is set, but don't need to use it
                let Some(_) = self.remote_id.get() else {
                    tracing::error!("remote node ID is not set, cannot remove actor.");
                    return;
                };

                // Get the receptionist to deregister the actor
                let receptionist = ctx.system().receptionist_ref().clone();
                
                // Use the actor_path provided in the message
                let remote_path = actor_path.clone();
                
                // Spawn a task to handle the deregistration
                ctx.spawn(async move {
                    // Deregister the actor from the receptionist
                    match receptionist.deregister_remote(key.clone(), remote_path.clone()).await {
                        Ok(true) => {
                            tracing::info!(
                                path=?remote_path, 
                                "Successfully deregistered remote actor"
                            );
                        },
                        Ok(false) => {
                            tracing::warn!(
                                path=?remote_path, 
                                "Failed to deregister remote actor, not found in receptionist"
                            );
                        },
                        Err(e) => {
                            tracing::error!(
                                error=%e, path=?remote_path,
                                "Error deregistering remote actor"
                            );
                        }
                    }
                    
                    // Note: The actual stream closure is handled by the RemoteProxy
                    // when it detects that the actor is removed or the connection is closed.
                    // The RemoteProxy itself will be dropped when its task completes.
                });
            }
        }
    }
}

#[derive(Debug)]
pub struct NodeActorSendHeartbeatMessage {}

impl Message for NodeActorSendHeartbeatMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorSendHeartbeatMessage> for NodeActor {
    #[tracing::instrument(skip(self, _ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: NodeActorSendHeartbeatMessage) {
        // Only send heartbeats if we're in an appropriate state
        if matches!(self.membership, Status::Up | Status::Joining)
            && !matches!(self.reachability, Reachability::Unreachable { .. })
        {
            // Create and send a heartbeat message
            let heartbeat = net::NodeMessage::Heartbeat {
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            if let Err(err) = self.send_message(heartbeat).await {
                tracing::error!(error=%err, "Failed to send heartbeat");

                // If we fail to send a heartbeat, update our reachability
                if let Reachability::Reachable { misses, last_seen } = self.reachability {
                    self.reachability = Reachability::Reachable {
                        misses: misses + 1,
                        last_seen,
                    };

                    // If we've missed too many, mark as unreachable
                    if misses + 1 >= 3 {
                        self.reachability = Reachability::Unreachable {
                            pings: 0,
                            last_seen,
                        };
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct NodeActorHeartbeatCheckMessage {
    pub timestamp: DateTime<Utc>,
}

impl Message for NodeActorHeartbeatCheckMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorHeartbeatCheckMessage> for NodeActor {
    /// Check the reachability of the node and update the reachability state accordingly.
    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorHeartbeatCheckMessage) {
        let timestamp = msg.timestamp;
        let old_reachability = self.reachability.clone();
        let old_status = self.membership.clone();

        match &self.reachability {
            Reachability::Reachable { misses, last_seen } => {
                // If the last seen was sooner than 3 seconds ago, we consider it reachable
                // and reset the misses
                if last_seen.signed_duration_since(timestamp) > chrono::TimeDelta::seconds(-3) {
                    self.reachability = Reachability::Reachable {
                        misses: 0,
                        last_seen: *last_seen,
                    };
                // When the miss is 3 seconds or more and we have hit three misses,
                // it is unreachable and we mark it as Down.
                } else if *misses >= 3 {
                    self.reachability = Reachability::Unreachable {
                        pings: 0,
                        last_seen: *last_seen,
                    };

                    // If we can't reach the node after multiple attempts, mark it as Down
                    if !matches!(self.membership, Status::Down | Status::Failed) {
                        self.membership = Status::Down;
                    }
                // If we have not hit three misses yet, we just increment the misses
                } else {
                    self.reachability = Reachability::Reachable {
                        misses: misses + 1,
                        last_seen: *last_seen,
                    };
                }
            }
            Reachability::Unreachable { pings, last_seen } => {
                if last_seen.signed_duration_since(timestamp) > chrono::TimeDelta::seconds(-3)
                    && *pings >= 3
                {
                    self.reachability = Reachability::Reachable {
                        misses: 0,
                        last_seen: *last_seen,
                    };

                    // If the node was Down but now is reachable again, mark it as Up
                    if matches!(self.membership, Status::Down) {
                        self.membership = Status::Up;
                    }
                } else {
                    self.reachability = Reachability::Unreachable {
                        pings: *pings,
                        last_seen: *last_seen,
                    };
                }
            }
            _ => {}
        }

        // If reachability or status changed, notify the cluster
        if self.reachability != old_reachability || self.membership != old_status {
            tracing::debug!(
                old_reachability=?old_reachability,
                new_reachability=?self.reachability,
                old_status=?old_status,
                new_status=?self.membership,
                "Node status or reachability changed"
            );
            self.update_status_and_notify(ctx).await;
        }
    }
}

struct NodeActorAcceptStreamMessage(net::RawStream);

impl std::fmt::Debug for NodeActorAcceptStreamMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeActorAcceptStreamMessage").finish()
    }
}

impl Message for NodeActorAcceptStreamMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorAcceptStreamMessage> for NodeActor {
    #[tracing::instrument(skip(self, ctx), fields(instance_id = %self.instance_id, remote_id = %self.remote_id))]
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorAcceptStreamMessage) {
        // Handle the accepted stream
        let stream = msg.0;
        tracing::info!("Accepted new actor stream from remote node");

        let (tx, rx) = stream.into_frame_stream::<net::ActorMessage>();

        let endpoint = ctx.endpoint();
        let receptionist = ctx.system().receptionist_ref().clone();

        // Spawn a background task to wait for the initial stream message for the proxy.
        let host_id = self.host_id.clone();
        let remote_id = self.remote_id.get().unwrap();
        ctx.spawn(async move {
            let mut stream_rx = rx;
            let stream_tx = tx;
            let frame = match stream_rx.read_frame().await {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    tracing::error!("Stream closed before reading initial message");
                    return;
                }
                Err(e) => {
                    tracing::error!(error=%e, "Failed to read stream acknowledgment");
                    return;
                }
            };
            let net::Payload::Ok(net::ActorMessage::Init(actor_key)) = frame.payload else {
                tracing::error!("Invalid initial message");
                return;
            };

            // Look up the actor key in the receptionist
            let Ok(recepient) = receptionist
                .lookup_one_recepient_of::<RemoteMessage>(actor_key.clone())
                .await
            else {
                tracing::error!("Actor key not found in receptionist");
                return;
            };

            // Create the LocalProxy for the actor and let it handle the stream.
            let local_proxy = LocalProxy::new(
                actor_key,
                host_id,
                remote_id,
                endpoint.clone(),
                recepient,
                stream_rx,
                stream_tx,
            );
            if let Err(err) = endpoint
                .send_in_background(NodeActorAddLocalProxyMessage(local_proxy))
                .await
            {
                tracing::error!(error=%err, "Failed to send local proxy message, discarding stream");
                return;
            }
            // TODO: Implement ActorAdd/ActorRemove messages, need to think about this actor path
            // stuff with the receptionist key and host names.
            // TODO: Implement RemoteProxy and handle the stream
            // TODO: Create Seperate Node Actor generic for Accepting vs Connecting action
            // TODO: Simplify th connection set to be a simple two sided init and join.
        });
    }
}

#[derive(Debug)]
struct NodeActorAddLocalProxyMessage(LocalProxy);

impl Message for NodeActorAddLocalProxyMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorAddLocalProxyMessage> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, message: NodeActorAddLocalProxyMessage) {
        let local_proxy = message.0;

        tracing::info!(?local_proxy, "Adding local proxy");
        let _ = ctx.spawn(local_proxy.open());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteProxyError {
    #[error("Node network error: {0}")]
    NodeNetworkError(#[from] net::NetError),
    #[error("Not connected to remote node")]
    NotConnected,
    #[error("Failed to register remote actor")]
    RegistrationFailed,
    #[error("Error registering remote actor: {0}")]
    RegistrationError(String),
}

struct RemoteProxy {
    host_id: Id,
    node_endpoint: Endpoint<NodeActor>,
    actor_key: RawKey,
    remote_id: Id,
    instance_id: Id,
    stream_tx: FrameWriter<net::ActorMessage>,
    stream_rx: FrameReader<net::ActorMessage>,
}

impl RemoteProxy {
    pub async fn open(
        host_id: Id,
        node_endpoint: Endpoint<NodeActor>,
        remote_id: Id,
        actor_key: RawKey,
        instance_id: Id,
        stream_tx: FrameWriter<net::ActorMessage>,
        stream_rx: FrameReader<net::ActorMessage>,
    ) {
        let mut proxy = Self {
            host_id,
            node_endpoint,
            actor_key,
            remote_id,
            instance_id,
            stream_tx,
            stream_rx,
        };
        if let Err(err) = proxy.init().await {
            tracing::error!(error=%err, "Failed to initialize remote proxy");
            return;
        }
        // TODO: Start main loop for remote proxy.
    }
    
    /// Opens a remote proxy and registers it with the receptionist
    /// only after successfully establishing the connection
    pub async fn open_and_register(
        host_id: Id,
        node_endpoint: Endpoint<NodeActor>,
        remote_id: Id,
        actor_key: RawKey,
        instance_id: Id,
        stream_tx: FrameWriter<net::ActorMessage>,
        stream_rx: FrameReader<net::ActorMessage>,
        receptionist: crate::receptionist::Receptionist,
        node_info: crate::node::NodeInfo,
    ) -> Result<(), RemoteProxyError> {
        // Create proxy message channel for EndpointSender to communicate with this RemoteProxy
        let (_proxy_tx, proxy_rx) = tokio::sync::mpsc::channel::<crate::remote::RemoteProxyMessage>(32);
        
        // Create and initialize the proxy
        let mut proxy = Self {
            host_id: host_id.clone(),
            node_endpoint,
            actor_key: actor_key.clone(),
            remote_id,
            instance_id: instance_id.clone(),
            stream_tx,
            stream_rx,
        };
        
        // Initialize the proxy connection
        proxy.init().await?;
        
        // Only register after successful initialization
        // Extract node connection info for the actor path
        let remote_addr = node_info.addr.clone();
        
        // Create the actor path for this remote actor
        let remote_path = crate::path::ActorPath::remote(
            remote_addr.host().to_string(),
            remote_addr.port(),
            actor_key.receptionist_key().to_string(),
            actor_key.group_id().to_string(),
            instance_id.clone(),
        );
        
        // Register with the receptionist
        match receptionist.register_remote(
            actor_key.clone(),
            remote_path.clone(),
            actor_key.receptionist_key().to_string(),
        ).await {
            Ok(true) => {
                tracing::info!(
                    "Registered remote actor: {} with key: {:?}",
                    actor_key.receptionist_key(), actor_key
                );
                
                // Start main loop for remote proxy in a new task
                let path_clone = remote_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = proxy.run(proxy_rx, path_clone).await {
                        tracing::error!(error=%e, "Error in RemoteProxy loop");
                    }
                });
                
                Ok(())
            }
            Ok(false) => {
                tracing::warn!(
                    "Failed to register remote actor: {} with key: {:?}",
                    actor_key.receptionist_key(), actor_key
                );
                Err(RemoteProxyError::RegistrationFailed)
            }
            Err(e) => {
                tracing::error!(
                    error=%e,
                    "Error registering remote actor: {} with key: {:?}",
                    actor_key.receptionist_key(), actor_key
                );
                Err(RemoteProxyError::RegistrationError(e.to_string()))
            }
        }
    }

    async fn init(&mut self) -> Result<(), RemoteProxyError> {
        // Send the init frame for the actor
        let init_frame = net::Frame::ok(
            self.host_id.clone(),
            Some(self.remote_id.clone()),
            net::ActorMessage::Init(self.actor_key.clone()),
        );

        if let Err(e) = self.stream_tx.write_frame(&init_frame).await {
            tracing::error!(error=%e, "Failed to send message");
            // TODO: Send Node Actor Update
            return Err(RemoteProxyError::NodeNetworkError(e));
        }
        Ok(())
    }
    
    /// Main processing loop for the RemoteProxy
    /// 
    /// This method processes incoming messages from local actors through the proxy_rx channel
    /// and forwards them to the remote actor, then waits for responses.
    /// 
    /// This implementation handles one request at a time serially, with a timeout.
    async fn run(
        &mut self, 
        mut proxy_rx: tokio::sync::mpsc::Receiver<crate::remote::RemoteProxyMessage>,
        actor_path: ActorPath
    ) -> Result<(), RemoteProxyError> {
        use crate::remote::RemoteProxyMessage;
        use tokio::time::{timeout, Duration};
        
        // Default timeout for waiting for responses
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
        
        tracing::info!(path=?actor_path, "Starting RemoteProxy loop");
        
        // Main processing loop
        while let Some(proxy_msg) = proxy_rx.recv().await {
            match proxy_msg {
                RemoteProxyMessage::SendMessage { message, response_tx } => {
                    // Create a frame with the message
                    let frame = net::Frame::ok(
                        self.host_id.clone(),
                        Some(self.remote_id.clone()),
                        net::ActorMessage::Request(message),
                    );
                    
                    // Send the message to the remote actor
                    if let Err(e) = self.stream_tx.write_frame(&frame).await {
                        tracing::error!(error=%e, "Failed to send message to remote actor");
                        let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                        continue;
                    }
                    
                    // Wait for the response with a timeout
                    match timeout(RESPONSE_TIMEOUT, self.stream_rx.read_frame()).await {
                        Ok(read_result) => match read_result {
                            Ok(Some(response_frame)) => {
                                match response_frame.payload {
                                    net::Payload::Ok(net::ActorMessage::Response(result)) => {
                                        // Send the result back
                                        let _ = response_tx.send(result);
                                    },
                                    net::Payload::UnhandledFailure(reason) => {
                                        tracing::error!(reason=%reason, "Received failure response");
                                        let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                                    },
                                    _ => {
                                        tracing::warn!("Received unexpected message type in response");
                                        let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                                    }
                                }
                            },
                            Ok(None) => {
                                // Stream closed
                                tracing::info!("Remote actor stream closed");
                                let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                                break;
                            },
                            Err(e) => {
                                tracing::error!(error=%e, "Error reading from stream");
                                let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                                return Err(RemoteProxyError::NodeNetworkError(e));
                            }
                        },
                        Err(_) => {
                            // Timeout waiting for response
                            tracing::error!("Timeout waiting for response from remote actor");
                            let _ = response_tx.send(Err(crate::message::RemoteMessageError::DeserializationError));
                        }
                    }
                }
                RemoteProxyMessage::Shutdown => {
                    tracing::info!(path=?actor_path, "Received shutdown command, closing RemoteProxy");
                    break;
                }
            }
        }
        
        // Close the stream by dropping
        tracing::info!(path=?actor_path, "Shutting down RemoteProxy, closing stream");
        
        // Send a close frame if possible, ignoring errors since we're shutting down anyway
        let close_frame = net::Frame::ok(
            self.host_id.clone(),
            Some(self.remote_id.clone()),
            net::ActorMessage::Init(self.actor_key.clone()), // Reuse Init for now as there is no "Close" type
        );
        let _ = self.stream_tx.write_frame(&close_frame).await;
        
        tracing::info!(path=?actor_path, "Stopped RemoteProxy loop");
        Ok(())
    }
    
    /// Initiate a graceful shutdown of the RemoteProxy
    pub async fn shutdown(
        actor_key: RawKey,
        actor_path: ActorPath, 
        _node_endpoint: Endpoint<NodeActor>, // Unused for now until we implement SendNodeMessage
        receptionist: crate::receptionist::Receptionist,
        proxy_tx: tokio::sync::mpsc::Sender<crate::remote::RemoteProxyMessage>,
    ) -> Result<(), RemoteProxyError> {
        // First deregister the actor from the receptionist
        match receptionist.deregister_remote(actor_key.clone(), actor_path.clone()).await {
            Ok(true) => {
                tracing::info!(
                    path=?actor_path, 
                    "Successfully deregistered remote actor"
                );
            },
            Ok(false) => {
                tracing::warn!(
                    path=?actor_path, 
                    "Failed to deregister remote actor, not found in receptionist"
                );
            },
            Err(e) => {
                tracing::error!(
                    error=%e, path=?actor_path,
                    "Error deregistering remote actor"
                );
                return Err(RemoteProxyError::RegistrationError(e.to_string()));
            }
        }
        
        // Send a shutdown message to the proxy loop
        if let Err(_) = proxy_tx.send(crate::remote::RemoteProxyMessage::Shutdown).await {
            tracing::warn!(
                path=?actor_path,
                "Failed to send shutdown message to RemoteProxy, it may already be closed"
            );
        }
        
        // Send a notification to the remote node to remove this actor
        // Format: Send an ActorRemove message to the remote node
        // We can't directly call send_message since it's a method on NodeActor
        // and we only have the endpoint. So we'll need to create a new message type
        // for NodeActor to handle that will do this for us.
        // 
        // For now, we'll simplify by not sending the ActorRemove message to the remote node.
        // In a real implementation, we would need to add a SendNodeMessage handler to NodeActor.
        //
        // Example:
        // let actor_remove = net::NodeMessage::ActorRemove {
        //     key: actor_key,
        //     actor_path: actor_path.clone(),
        //     instance_id: Id::new(), // Use a new ID since we don't track the instance ID
        // };
        
        // Instead of sending the error message, just log that we would send it
        tracing::info!(
            path=?actor_path,
            "Would send ActorRemove message to remote node (not implemented yet)"
        );
        
        /*
        // Stub success - this would be where we'd send the message
        if let Err(e) = node_endpoint.send(SendNodeMessageCommand(actor_remove)).await {
            tracing::error!(
                error=%e, path=?actor_path,
                "Failed to send ActorRemove message to remote node"
            );
            return Err(RemoteProxyError::RegistrationError(e.to_string()));
        }
        */
        
        tracing::info!(path=?actor_path, "Remote actor shutdown complete");
        Ok(())
    }
}

struct LocalProxy {
    actor_key: RawKey,
    node_id: Id,
    remote_id: Id,
    node_endpoint: Endpoint<NodeActor>,
    recepient: RecepientOf<RemoteMessage>,
    stream_rx: FrameReader<net::ActorMessage>,
    stream_tx: FrameWriter<net::ActorMessage>,
}

impl LocalProxy {
    fn new(
        actor_key: RawKey,
        node_id: Id,
        remote_id: Id,
        node_endpoint: Endpoint<NodeActor>,
        recepient: RecepientOf<RemoteMessage>,
        stream_rx: FrameReader<ActorMessage>,
        stream_tx: FrameWriter<ActorMessage>,
    ) -> Self {
        Self {
            actor_key,
            node_endpoint,
            node_id,
            remote_id,
            recepient,
            stream_rx,
            stream_tx,
        }
    }

    pub async fn open(mut self) {
        loop {
            let frame = match self.stream_rx.read_frame().await {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    tracing::error!("Stream closed before reading message");
                    return;
                }
                Err(e) => {
                    tracing::error!(error=%e, "Failed to read stream");
                    return;
                }
            };

            let actor_msg = match frame.payload {
                net::Payload::Ok(msg) => msg,
                net::Payload::UnhandledFailure(reason) => {
                    tracing::error!(reason=%reason, "Received failure message");
                    continue;
                }
            };

            let response = match actor_msg {
                ActorMessage::Init(_) => {
                    tracing::error!("Received unexpected Init message");
                    // TODO: Send Error.
                    continue;
                }
                ActorMessage::Request(msg) => {
                    tracing::info!(?msg, "Received message");
                    let Ok(result) = self.recepient.send(msg).await else {
                        tracing::error!("Failed to send message to recepient");
                        // TODO: Send an error
                        continue;
                    };
                    result
                }
                ActorMessage::Response(_) => {
                    // Ignore Response Messages.
                    tracing::error!("Received unexpected response message.");
                    // TODO: Send an error
                    continue;
                }
            };

            let frame = net::Frame::ok(
                self.node_id.clone(),
                Some(self.remote_id.clone()),
                ActorMessage::Response(response),
            );
            if let Err(e) = self.stream_tx.write_frame(&frame).await {
                tracing::error!(error=%e, "Failed to send message");
                // TODO: Send Node Actor Update
                break;
            }
        }
    }
}

impl std::fmt::Debug for LocalProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalProxy")
            .field("actor_key", &self.actor_key)
            .finish()
    }
}
