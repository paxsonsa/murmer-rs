use crate::net::{self, FrameWriter, NetworkDriver};
use crate::receptionist::Key;
use chrono::{DateTime, Utc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;

enum State {
    Init,
    InitAck,
}

pub struct NodeActor {
    pub cluster_node_id: Id,
    pub node_info: NodeInfo,
    pub driver: Box<dyn NetworkDriver>,
    pub membership: Status,
    pub reachability: Reachability,
    pub send_stream: Option<FrameWriter<net::NodeMessage>>,
    inner_state: State,
    node_id: Option<Id>,
}

impl NodeActor {
    pub fn new(id: Id, node_info: NodeInfo, driver: Box<dyn NetworkDriver>) -> Self {
        NodeActor {
            cluster_node_id: id,
            node_info,
            driver,
            membership: Status::Pending,
            reachability: Reachability::Pending,
            send_stream: None,
            inner_state: State::Init,
            node_id: None,
        }
    }
    
    // Send a status update to the cluster
    async fn update_status_and_notify(&mut self, ctx: &mut Context<Self>) {
        // Log the status change for debugging
        tracing::debug!(
            node_id=%self.node_info.node_id,
            status=?self.membership,
            reachability=?self.reachability,
            "Node status changed"
        );
        
        // Try to find the cluster actor through the receptionist
        let key = crate::receptionist::Key::<crate::cluster::ClusterActor>::default();
        
        // Attempt to lookup the cluster actor but don't require it to be present
        // This enables testing without needing to mock the cluster
        let lookup_result = ctx.system().receptionist().lookup_one(key).await;
        
        if let Ok(endpoint) = lookup_result {
            let cluster = crate::cluster::Cluster::new(endpoint);
            
            // Determine if this is a configured node (based on whether it was initialized from config)
            // For now we'll assume all nodes were configured - in a real implementation
            // we'd track this information properly
            let is_configured = true;
            
            // Send the status update
            let status_update = crate::cluster::NodeStatusUpdate {
                node_id: self.node_info.node_id.clone(),
                status: self.membership.clone(),
                reachability: self.reachability.clone(),
                is_configured,
                timestamp: chrono::Utc::now(),
            };
            
            if let Err(err) = cluster.update_node_status(status_update).await {
                tracing::error!(error=%err, "Failed to send status update to cluster");
            } else {
                tracing::debug!(
                    node_id=%self.node_info.node_id,
                    "Successfully sent status update to cluster"
                );
            }
        } else {
            // This is expected in tests where no cluster is registered
            tracing::debug!("No cluster actor registered, skipping status update");
        }
    }

    async fn send_message(&mut self, message: net::NodeMessage) -> Result<(), NodeError> {
        if let Some(ref mut stream) = self.send_stream {
            let frame = net::Frame::ok(self.node_info.node_id.clone(), None, message);
            if let Err(e) = stream.write_frame(&frame).await {
                tracing::error!(error=%e, "Failed to send message");
                return Err(NodeError::NodeNetworkError(e));
            }
            Ok(())
        } else {
            Err(NodeError::NotConnected)
        }
    }

    async fn init_ack(&mut self, node_id: Id) {
        tracing::info!(node_id=%node_id, "Received InitAck");
        let State::Init = self.inner_state else {
            tracing::error!("Invalid state for InitAck");
            // TODO: Send Error
            return;
        };
        self.node_id = Some(node_id.clone());
        self.inner_state = State::InitAck;
        self.membership = Status::Joining;
        self.reachability = Reachability::reachable_now();

        // Send Join Message
        let join_message = net::NodeMessage::Join {
            name: "default".to_string(),
            capabilities: Vec::new(),
        };

        // TODO: Make this a background task that can return a result/error
        if let Err(err) = self.send_message(join_message).await {
            tracing::error!(error=%err, "Failed to send join message");
            self.membership = Status::Failed;
            self.inner_state = State::Init;
            return;
        }
    }
}

#[async_trait::async_trait]
impl Actor for NodeActor {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!(node_id=%self.node_info.node_id, "Member started");

        // Update membership state
        self.membership = Status::Pending;
        self.reachability = Reachability::Pending;

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
        let raw_stream = match self.driver.open_raw_stream().await {
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
        let (stream_tx, mut stream_rx) = raw_stream.into_frame_stream::<net::NodeMessage>();
        self.send_stream = Some(stream_tx);

        // Send initial message
        let init = net::NodeMessage::Init {
            protocol_version: 1, // FIXME: Use a real protocol version
            id: self.cluster_node_id.clone(),
        };
        if let Err(err) = self.send_message(init).await {
            tracing::error!(error=%err, "Failed to send initial message");
            self.membership = Status::Failed;
            return;
        }

        // Start the receive loop for the main node stream
        let endpoint = ctx.endpoint();
        let node_id = self.node_info.node_id.clone();
        let mut running = true;
        ctx.spawn(async move {
            let span = tracing::trace_span!("member-rx", node_id=%node_id);
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

        // Use the new interval pattern for periodic tasks
        
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
            net::Payload::UnknownFailure(reason) => {
                tracing::error!(reason=%reason, "Received failure message");
                return;
            }
        };

        match msg {
            net::NodeMessage::Init {
                protocol_version,
                id,
            } => {
                tracing::info!(protocol_version=%protocol_version, id=%id, "Received Init message");
                
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
                self.node_id = Some(id.clone());
                
                // Send InitAck response
                let init_ack = net::NodeMessage::InitAck { 
                    node_id: self.node_info.node_id.clone() 
                };
                
                // Send the InitAck message
                if let Err(err) = self.send_message(init_ack).await {
                    tracing::error!(error=%err, "Failed to send InitAck");
                    return;
                }
                
                // Update state
                self.membership = Status::Pending;
                self.reachability = Reachability::reachable_now();
            },
            net::NodeMessage::InitAck { node_id } => {
                tracing::info!(?node_id, "Received InitAck");
                
                // Handle this message asynchronously
                ctx.send(NodeActorInitAckMessage { 
                    node_id: node_id.clone() 
                });
            },
            net::NodeMessage::Join { name, capabilities } => {
                tracing::info!(name=%name, ?capabilities, "Received Join message");
                
                // A node is trying to join our cluster
                // Update membership state for the remote node
                self.membership = Status::Joining;
                
                // Respond with JoinAck
                let join_ack = net::NodeMessage::JoinAck {
                    accepted: true,
                    reason: None,
                };
                
                if let Err(err) = self.send_message(join_ack).await {
                    tracing::error!(error=%err, "Failed to send JoinAck");
                    return;
                }
                
                // Once we've accepted the join, the node is considered Up
                self.membership = Status::Up;
            },
            net::NodeMessage::JoinAck { accepted, reason } => {
                if accepted {
                    tracing::info!("Join accepted");
                    self.membership = Status::Up;
                    
                    // Send first heartbeat
                    let heartbeat = net::NodeMessage::Heartbeat {
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    };
                    
                    if let Err(err) = self.send_message(heartbeat).await {
                        tracing::error!(error=%err, "Failed to send initial heartbeat");
                    }
                    
                    // We don't need to set up heartbeat interval here since it's already
                    // set up in the started method
                } else {
                    tracing::error!(reason=?reason, "Join rejected");
                    self.membership = Status::Failed;
                }
            },
            net::NodeMessage::Handshake { capabilities } => {
                tracing::info!(?capabilities, "Received Handshake");
                
                // Store capabilities or negotiate features here if needed
                // For now, just acknowledge with our capabilities
                let handshake_ack = net::NodeMessage::HandshakeAck {
                    capabilities: Vec::new(), // Add actual capabilities here
                };
                
                if let Err(err) = self.send_message(handshake_ack).await {
                    tracing::error!(error=%err, "Failed to send HandshakeAck");
                }
            },
            net::NodeMessage::HandshakeAck { capabilities } => {
                tracing::info!(?capabilities, "Received HandshakeAck");
                // Store negotiated capabilities if needed
            },
            net::NodeMessage::Heartbeat { timestamp: _ } => {
                // Update reachability with new heartbeat
                let now = Utc::now();
                
                // Update our heartbeat tracking
                ctx.send(NodeActorHeartbeatUpdateMessage { 
                    timestamp: now 
                });
                
                // Respond with our own heartbeat
                let response_heartbeat = net::NodeMessage::Heartbeat {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                };
                
                if let Err(err) = self.send_message(response_heartbeat).await {
                    tracing::error!(error=%err, "Failed to send heartbeat response");
                }
            },
            net::NodeMessage::Disconnect { reason } => {
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
            },
        }
    }
}

#[derive(Debug)]
pub struct NodeActorInitAckMessage {
    pub node_id: Id,
}

impl Message for NodeActorInitAckMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorInitAckMessage> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorInitAckMessage) {
        // Call our async init_ack method to properly process the InitAck
        self.init_ack(msg.node_id).await;
        
        // Notify the cluster about our state change
        self.update_status_and_notify(ctx).await;
    }
}

#[derive(Debug)]
pub struct NodeActorHeartbeatUpdateMessage {
    pub timestamp: DateTime<Utc>,
}

impl Message for NodeActorHeartbeatUpdateMessage {
    type Result = ();
}
#[async_trait]
impl Handler<NodeActorHeartbeatUpdateMessage> for NodeActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorHeartbeatUpdateMessage) {
        let ping_time = msg.timestamp;
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
        }
        
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
}

#[derive(Debug)]
pub struct NodeActorHeartbeatCheckMessage {
    pub timestamp: DateTime<Utc>,
}

impl Message for NodeActorHeartbeatCheckMessage {
    type Result = ();
}

#[derive(Debug)]
pub struct NodeActorSendHeartbeatMessage {}

impl Message for NodeActorSendHeartbeatMessage {
    type Result = ();
}

#[async_trait]
impl Handler<NodeActorSendHeartbeatMessage> for NodeActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: NodeActorSendHeartbeatMessage) {
        // Only send heartbeats if we're in an appropriate state
        if matches!(self.membership, Status::Up | Status::Joining) && 
           !matches!(self.reachability, Reachability::Unreachable { .. }) {
            
            // Create and send a heartbeat message
            let heartbeat = net::NodeMessage::Heartbeat {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };
            
            if let Err(err) = self.send_message(heartbeat).await {
                tracing::error!(error=%err, "Failed to send heartbeat");
                
                // If we fail to send a heartbeat, update our reachability
                match self.reachability {
                    Reachability::Reachable { misses, last_seen } => {
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
                    },
                    _ => {},
                }
            }
        }
    }
}

#[async_trait]
impl Handler<NodeActorHeartbeatCheckMessage> for NodeActor {
    /// Check the reachability of the node and update the reachability state accordingly.
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: NodeActorHeartbeatCheckMessage) {
        let timestamp = msg.timestamp;
        let old_reachability = self.reachability.clone();
        let old_status = self.membership.clone();
        
        match &self.reachability {
            Reachability::Reachable { misses, last_seen } => {
                if last_seen.signed_duration_since(timestamp) > chrono::TimeDelta::seconds(-3) {
                    self.reachability = Reachability::Reachable {
                        misses: 0,
                        last_seen: last_seen.clone(),
                    };
                } else if *misses >= 3 {
                    self.reachability = Reachability::Unreachable {
                        pings: 0,
                        last_seen: last_seen.clone(),
                    };
                    
                    // If we can't reach the node after multiple attempts, mark it as Down
                    if !matches!(self.membership, Status::Down | Status::Failed) {
                        self.membership = Status::Down;
                    }
                } else {
                    self.reachability = Reachability::Reachable {
                        misses: misses + 1,
                        last_seen: last_seen.clone(),
                    };
                }
            }
            Reachability::Unreachable { pings, last_seen } => {
                if last_seen.signed_duration_since(timestamp) > chrono::TimeDelta::seconds(-3)
                    && *pings >= 3
                {
                    self.reachability = Reachability::Reachable {
                        misses: 0,
                        last_seen: last_seen.clone(),
                    };
                    
                    // If the node was Down but now is reachable again, mark it as Up
                    if matches!(self.membership, Status::Down) {
                        self.membership = Status::Up;
                    }
                } else {
                    self.reachability = Reachability::Unreachable {
                        pings: *pings,
                        last_seen: last_seen.clone(),
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
