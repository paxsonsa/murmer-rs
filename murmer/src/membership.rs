use crate::cluster::ClusterId;
use crate::cluster::Connection;
use crate::cluster::ConnectionError;
use crate::cluster::ConnectionState;
use crate::cluster::Node;
use crate::net;
use crate::prelude::*;
use crate::tls::TlsConfig;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use quinn::crypto::rustls::QuicClientConfig;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(test)]
#[path = "membership.test.rs"]
mod tests;

/// A stream for reading frames of type T
pub struct FrameReader<T>
where
    T: Serialize + DeserializeOwned,
{
    inner: Box<dyn AsyncRead + Send + Unpin>,
    reader: net::FrameParser<T>,
}

impl<T> FrameReader<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(stream: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        FrameReader {
            inner: stream,
            reader: net::FrameParser::new(),
        }
    }

    /// Read the next frame from the stream
    pub async fn read_frame(&mut self) -> Result<Option<net::Frame<T>>, net::NetError> {
        // Try to parse a frame from the existing buffer
        if let Some(frame) = self.reader.parse()? {
            return Ok(Some(frame));
        }

        // Read more data from the stream
        let mut buffer = [0u8; 1024];
        loop {
            match self.inner.read(&mut buffer).await {
                Ok(0) => return Ok(None), // End of stream
                Ok(n) => {
                    // Add the data to the reader
                    self.reader.extend(&buffer[..n]);

                    // Try to parse a frame
                    if let Some(frame) = self.reader.parse()? {
                        return Ok(Some(frame));
                    }
                }
                Err(e) => {
                    return Err(net::NetError::InvalidFrame(format!("Read error: {}", e)));
                }
            }
        }
    }
}

/// A stream for writing frames of type T
pub struct FrameWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    inner: Box<dyn AsyncWrite + Send + Unpin>,
    _phantom: PhantomData<T>,
}

impl<T> FrameWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(stream: Box<dyn AsyncWrite + Send + Unpin>) -> Self {
        FrameWriter {
            inner: stream,
            _phantom: PhantomData,
        }
    }

    /// Write a frame to the stream
    pub async fn write_frame(&mut self, frame: &net::Frame<T>) -> Result<(), net::NetError> {
        let encoded = frame.encode()?;
        self.inner
            .write_all(&encoded)
            .await
            .map_err(|e| net::NetError::InvalidFrame(format!("Write error: {}", e)))?;
        self.inner
            .flush()
            .await
            .map_err(|e| net::NetError::InvalidFrame(format!("Flush error: {}", e)))?;
        Ok(())
    }
}

/// A type-erased stream that can be used to read and write raw bytes
pub struct RawStream {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
}

impl RawStream {
    pub fn new(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Self {
        RawStream { reader, writer }
    }

    /// Convert this raw stream into a typed frame stream
    pub fn into_frame_stream<T>(self) -> (FrameWriter<T>, FrameReader<T>)
    where
        T: Serialize + DeserializeOwned,
    {
        let writer = FrameWriter::new(self.writer);
        let reader = FrameReader::new(self.reader);
        (writer, reader)
    }
}

#[async_trait]
pub trait ConnectionDriver: Send {
    async fn connect(&mut self) -> Result<(), ConnectionError>;

    /// Open a new bidirectional stream for communication
    /// Returns a type-erased stream that can be converted to a typed stream
    async fn open_raw_stream(&mut self) -> Result<RawStream, ConnectionError>;
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

    async fn open_raw_stream(&mut self) -> Result<RawStream, ConnectionError> {
        match &self.state {
            ConnectionState::Connected { connection } => {
                // Open a bidirectional stream
                let (send, recv) = connection.open_bi().await.map_err(|e| {
                    ConnectionError::ConnectionFailed(format!("Failed to open stream: {}", e))
                })?;

                // Create the raw stream
                Ok(RawStream::new(Box::new(recv), Box::new(send)))
            }
            ConnectionState::NotReady => Err(ConnectionError::NotConnected),
            ConnectionState::Disconnected { reason } => Err(ConnectionError::ConnectionFailed(
                format!("Connection is disconnected: {}", reason),
            )),
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
        cluster_id: Arc<ClusterId>,
        node: Node,
        socket: quinn::Endpoint,
        tls: TlsConfig,
    ) -> Option<Member> {
        let node_id = node.node_id.clone();

        let driver = Box::new(QuicConnectionDriver::new(node.clone(), socket, tls));
        let endpoint = system.spawn_with(MemberActor {
            node,
            id: cluster_id.id.clone(),
            driver,
            membership: Membership::Pending,
            reachability: Reachability::Pending,
            send_stream: None,
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

    #[error("Node network error: {0}")]
    NodeNetworkError(#[from] net::NetError),
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

pub struct MemberActorRecvFrame(pub Result<net::Frame<net::NodeMessage>, net::NetError>);

impl Message for MemberActorRecvFrame {
    type Result = ();
}

pub struct MemberActor {
    pub id: Id,
    pub node: Node,
    pub driver: Box<dyn ConnectionDriver>,
    pub membership: Membership,
    pub reachability: Reachability,
    pub send_stream: Option<FrameWriter<net::NodeMessage>>,
}

impl MemberActor {
    async fn send_message(&mut self, message: net::NodeMessage) -> Result<(), MemberError> {
        if let Some(ref mut stream) = self.send_stream {
            let frame = net::Frame::ok(self.node.node_id.clone(), None, message);
            if let Err(e) = stream.write_frame(&frame).await {
                tracing::error!(error=%e, "Failed to send message");
                return Err(MemberError::NodeNetworkError(e));
            }
            Ok(())
        } else {
            Err(MemberError::NotConnected)
        }
    }
}

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

        // Open new raw stream for initial cluster communication
        let raw_stream = match self.driver.open_raw_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!(error=%e, "Failed to establish member connection stream");
                self.membership = Membership::Failed;
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
            self.membership = Membership::Failed;
            return;
        }

        // Start the receive loop for the main node stream
        let endpoint = ctx.endpoint();
        ctx.spawn(async move {
            let mut running = true;
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
                if let Err(err) = endpoint
                    .send_in_background(MemberActorRecvFrame(frame))
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

impl Handler<MemberActorRecvFrame> for MemberActor {
    fn handle(&mut self, _ctx: &mut Context<Self>, msg: MemberActorRecvFrame) {
        match msg.0 {
            Ok(frame) => {}
            Err(e) => {}
        }
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
