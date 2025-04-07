use crate::net::{self, FrameWriter, NetworkDriver};
use chrono::{DateTime, Utc};

use super::*;

pub struct NodeActor {
    pub id: Id,
    pub node_info: NodeInfo,
    pub driver: Box<dyn NetworkDriver>,
    pub membership: Status,
    pub reachability: Reachability,
    pub send_stream: Option<FrameWriter<net::NodeMessage>>,
}

impl NodeActor {
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
            id: self.id.clone(),
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

        // Setup a heartbeat deadman switch.
        // We need a timer that will periodically check the reachability of the node
        // and update the reachability state accordingly.
        // ctx.interval(Duration::from_secs(1), |actor, ctx| {
        //     ctx.send(MemberActorHeartbeatCheck {
        //         timestamp: Utc::now(),
        //     });
        // });

        // Start Receive Loop for Cluster Stream
        // let endpoint = ctx.endpoint();
        // ctx.spawn(async move {
        //     loop {
        //         match stream_rx.read_frame().await {
        //             Ok(Some(frame)) => {
        //                 // Process the frame
        //                 match frame.payload {
        //                     net::Payload::Ok(msg) => {
        //                         // Handle the message
        //                         match msg {
        //                             net::NodeMessage::InitAck => {
        //                                 // Send join message
        //                                 // TODO: Implement join message handling
        //                             }
        //                             net::NodeMessage::JoinAck { accepted, reason } => {
        //                                 // Handle join response
        //                                 // TODO: Implement join response handling
        //                             }
        //                             net::NodeMessage::Heartbeat { timestamp } => {
        //                                 // Update heartbeat
        //                                 endpoint.send(MemberActorHeartbeatUpdate {
        //                                     timestamp: Utc::now(),
        //                                 });
        //                             }
        //                             _ => {
        //                                 // Handle other message types
        //                             }
        //                         }
        //                     }
        //                     net::Payload::UnknownFailure(reason) => {
        //                         tracing::error!(reason=%reason, "Received failure message");
        //                     }
        //                 }
        //             }
        //             Ok(None) => {
        //                 tracing::info!("Stream closed");
        //                 break;
        //             }
        //             Err(e) => {
        //                 tracing::error!(error=%e, "Failed to read from stream");
        //                 break;
        //             }
        //         }
        //     }
        // });
    }
}

#[derive(Debug)]
pub struct NodeActorRecvFrameMessage(pub Result<net::Frame<net::NodeMessage>, net::NetError>);

impl Message for NodeActorRecvFrameMessage {
    type Result = ();
}

impl Handler<NodeActorRecvFrameMessage> for NodeActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: NodeActorRecvFrameMessage) {
        tracing::info!(node_id=%self.node_info.node_id, "Received frame");
        match msg.0 {
            Ok(frame) => {}
            Err(e) => {}
        }
    }
}

#[derive(Debug)]
pub struct NodeActorHeartbeatUpdateMessage {
    pub timestamp: DateTime<Utc>,
}

impl Message for NodeActorHeartbeatUpdateMessage {
    type Result = ();
}
impl Handler<NodeActorHeartbeatUpdateMessage> for NodeActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: NodeActorHeartbeatUpdateMessage) {
        let ping_time = msg.timestamp;
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
    }
}

#[derive(Debug)]
pub struct NodeActorHeartbeatCheckMessage {
    pub timestamp: DateTime<Utc>,
}

impl Message for NodeActorHeartbeatCheckMessage {
    type Result = ();
}

impl Handler<NodeActorHeartbeatCheckMessage> for NodeActor {
    /// Check the reachability of the node and update the reachability state accordingly.
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: NodeActorHeartbeatCheckMessage) {
        let timestamp = msg.timestamp;
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
                } else {
                    self.reachability = Reachability::Unreachable {
                        pings: *pings,
                        last_seen: last_seen.clone(),
                    };
                }
            }
            _ => {}
        }
    }
}
