use crate::cluster::Connection;
use crate::cluster::ConnectionError;
use crate::cluster::ConnectionState;
use crate::cluster::Node;
use crate::net;
use crate::prelude::*;
use crate::tls::TlsConfig;
use chrono::{DateTime, Utc};
use quinn::crypto::rustls::QuicClientConfig;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
#[path = "membership.test.rs"]
mod tests;

#[async_trait]
pub trait ConnectionDriver: Send {
    async fn connect(&mut self) -> Result<(), ConnectionError>;
}

pub struct QuicConnectionDriver {
    node: Node,
    socket: quinn::Endpoint,
    tls: TlsConfig,
    state: ConnectionState,
}

impl QuicConnectionDriver {
    pub fn new(node: Node, socket: quinn::Endpoint, tls: TlsConfig) -> Self {
        QuicConnectionDriver {
            node,
            socket,
            tls,
            state: ConnectionState::NotReady,
        }
    }
}

#[async_trait]
impl ConnectionDriver for QuicConnectionDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        match &self.state {
            ConnectionState::NotReady | ConnectionState::Disconnected { .. } => {
                let tls = self.tls.clone();
                let addr = self.node.addr.clone();
                let socket = self.socket.clone();

                let crypto = tls.into_client_config()?;
                let sock_addr = addr.to_sock_addrs()?;

                let Ok(client_config) = QuicClientConfig::try_from(crypto) else {
                    return Err(ConnectionError::FailedToConnect(
                        "Failed to create TLS QUIC client config".to_string(),
                    ));
                };
                let client_config = quinn::ClientConfig::new(Arc::new(client_config));
                let connection = match socket
                    .connect_with(client_config, sock_addr, &addr.host())
                    .map_err(|err| match err {
                        quinn::ConnectError::EndpointStopping => ConnectionError::ConnectionClosed(
                            "internal endpoint is stopping, cannot create new connections"
                                .to_string(),
                        ),
                        quinn::ConnectError::CidsExhausted => ConnectionError::FailedToConnect(
                            "CID space are exhausted, cannot create new connections".to_string(),
                        ),
                        quinn::ConnectError::InvalidServerName(name) => {
                            ConnectionError::FailedToConnect(format!(
                                "Invalid server name: {}",
                                name
                            ))
                        }
                        quinn::ConnectError::InvalidRemoteAddress(socket_addr) => {
                            ConnectionError::FailedToConnect(format!(
                                "Invalid remote address: {}",
                                socket_addr
                            ))
                        }
                        quinn::ConnectError::NoDefaultClientConfig => {
                            ConnectionError::InvalidConfiguration
                        }
                        quinn::ConnectError::UnsupportedVersion => {
                            ConnectionError::FailedToConnect(
                                "Peer does not support the required QUIC version".to_string(),
                            )
                        }
                    }) {
                    Ok(connection) => connection,
                    Err(e) => {
                        self.state = ConnectionState::Disconnected {
                            reason: ConnectionError::ConnectionFailed(
                                "Failed to open initial connection to node.".to_string(),
                            ),
                        };
                        return Err(e);
                    }
                }
                .await?;

                self.state = ConnectionState::Connected { connection };
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug)]
pub enum Membership {
    Pending,
    Up,
    Down,
    Joining,
    Leaving,
    Removed,
    Failed,
}

#[derive(Debug)]
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

impl Reachability {
    pub fn reachable_now() -> Self {
        Reachability::Reachable {
            misses: 0,
            last_seen: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct Member {
    node_id: Id,
    endpoint: Endpoint<MemberActor>,
}

impl Member {
    pub fn spawn(
        system: System,
        node: Node,
        socket: quinn::Endpoint,
        tls: TlsConfig,
    ) -> Option<Member> {
        let node_id = node.node_id.clone();

        let driver = Box::new(QuicConnectionDriver::new(node.clone(), socket, tls));
        let endpoint = system.spawn_with(MemberActor {
            node,
            driver,
            membership: Membership::Pending,
            reachability: Reachability::Pending,
        });
        endpoint
            .map(|e| Member {
                node_id,
                endpoint: e,
            })
            .ok()
    }
}

impl Eq for Member {}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl std::hash::Hash for Member {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MemberError {
    #[error("Member not connected")]
    NotConnected,
}

pub struct MemberActorHeartbeat;

impl Message for MemberActorHeartbeat {
    type Result = Result<(), MemberError>;
}

pub struct MemberActorSetConnectionState(pub ConnectionState);

impl Message for MemberActorSetConnectionState {
    type Result = Result<(), MemberError>;
}

pub struct MemberActorHeartbeatCheck {
    pub timestamp: DateTime<Utc>,
}

impl Message for MemberActorHeartbeatCheck {
    type Result = ();
}

pub struct MemberActorHeartbeatUpdate {
    pub timestamp: DateTime<Utc>,
}

impl Message for MemberActorHeartbeatUpdate {
    type Result = ();
}

pub struct MemberActor {
    pub node: Node,
    pub driver: Box<dyn ConnectionDriver>,
    pub membership: Membership,
    pub reachability: Reachability,
}

impl MemberActor {}

#[async_trait::async_trait]
impl Actor for MemberActor {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!(node_id=%self.node.node_id, "Member started");

        // Update membership state
        self.membership = Membership::Pending;
        self.reachability = Reachability::Pending;

        // Establish the connection regardless of the current state
        if let Err(err) = self.driver.connect().await {
            tracing::error!(error=%err, "Failed to establish connection");
            self.membership = Membership::Failed;
            self.reachability = Reachability::Unreachable {
                pings: 0,
                last_seen: Utc::now(),
            };
            return;
        }

        // Open new pair for for initial cluster communication.
        // let (mut stream_tx, mut stream_rx) = match self.driver.open_stream().await {
        //     Ok((stream_tx, stream_rx)) => (stream_tx, stream_rx),
        //     Err(e) => {
        //         tracing::error!(error=%e, "Failed to establish member connection stream");
        //         self.membership = Membership::Failed;
        //         self.reachability = Reachability::Unreachable;
        //         return;
        //     }
        // };

        // Send initial message
        let init = net::NodeMessage::Init {
            protocol_version: 1, // FIXME: Use a real protocol version
        };
        //     let frame = net::Frame::node(self.connection.node.node_id.clone(), None, init);
        //     let encoded = match frame.encode() {
        //         Ok(encoded) => encoded,
        //         Err(e) => {
        //             tracing::error!(error=%e, "Failed to encode frame");
        //             self.membership = Membership::Failed;
        //             self.connection.state = ConnectionState::Disconnected {
        //                 reason: ConnectionError::ConnectionFailed("Failed to encode frame".to_string()),
        //             };
        //             return;
        //         }
        //     };
        //     match stream_tx.write_all(&encoded).await {
        //         Ok(_) => {
        //             tracing::debug!("Initial message sent");
        //         }
        //         Err(e) => {
        //             // FIXME: Decode write error for more detailed error handling
        //             tracing::error!(error=%e, "Failed to send initial message");
        //             self.membership = Membership::Failed;
        //             self.connection.state = ConnectionState::Disconnected {
        //                 reason: ConnectionError::ConnectionFailed(
        //                     "Failed to send initial message".to_string(),
        //                 ),
        //             };
        //             return;
        //         }
        //     };
        //
        //     // Start accept loop for new streams
        //     self.connection.state = ConnectionState::Connected {
        //         connection: connection.clone(),
        //         stream: stream_tx,
        //     };
        //     self.membership = Membership::Pending;
        //     self.reachability = Reachability::Pending;
        //
        //     // Setup a heartbeat deadman switch.
        //     // We need a timer that will periodically check the reachability of the node
        //     // and update the reachability state accordingly.
        //     ctx.interval(Duration::from_secs(1), |ctx: &Context<Self>| {
        //         ctx.send(MemberActorHeartbeatCheck);
        //     });
        //
        //     // Start Receive Loop for Cluster Stream
        //     ctx.spawn(async move {
        //         // Use our new net format reader
        //         let mut reader = crate::net::FrameReader::new();
        //
        //         loop {
        //             let chunk = match stream_rx.read_chunk(1024, true).await {
        //                 Ok(Some(chunk)) => chunk,
        //                 Ok(None) => {
        //                     tracing::info!("Stream closed");
        //                     break;
        //                 }
        //                 Err(e) => {
        //                     tracing::error!(error=%e, "Failed to read from stream");
        //                     break;
        //                 }
        //             };
        //
        //             // Add data to the reader
        //             reader.extend(&chunk.bytes);
        //
        //             let frames = match reader.parse_all() {
        //                 Ok(frames) => frames,
        //                 Err(e) => {
        //                     tracing::error!(error=%e, "Failed to parse frames");
        //                     // TODO: How do handle this issue? Corrupted buffer?
        //                     break;
        //                 }
        //             };
        //
        //             for frame in frames {
        //                 match &frame.payload {
        //                     net::Payload::Node(msg) => {}
        //                     net::Payload::Actor(msg) => {}
        //                     net::Payload::Cluster(msg) => {}
        //                 }
        //             }
        //         }
        //     });
        // }
    }
}

impl Handler<MemberActorHeartbeatUpdate> for MemberActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: MemberActorHeartbeatUpdate) {
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

impl Handler<MemberActorHeartbeatCheck> for MemberActor {
    /// Check the reachability of the node and update the reachability state accordingly.
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: MemberActorHeartbeatCheck) {
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
