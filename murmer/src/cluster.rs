/*

1. Core Types & States
---------------------
- NodeId, NodeState (Connected/Disconnected)
- ClusterState (Available, Unreachable, Down, Dead)
- ActorState (Starting, Running, Stopping, Stopped, Restarting, Failed)

2. Component Responsibilities
---------------------------
a) Cluster (Top-Level Coordinator)
   - Manages overall cluster state
   - Holds references to Monitor, Receptionist, Network, System
   - Coordinates actor registration/deregistration
   - Handles cluster-wide broadcasts

b) Node & Connection Management
   - Node: Represents cluster member with state and actor registry
   - NodeConnection: Handles QUIC connection lifecycle
   - Connection state management and monitoring
   - Message routing and relay

c) Remote Supervisor
   - Local proxy for remote actors
   - Handles message routing and lifecycle
   - Manages timeouts and retries
   - Stream management for data transfer
   - State synchronization

d) Monitor
   - Cluster state monitoring
   - Node state tracking
   - Actor registration coordination
   - Receptionist integration
   - Gossip protocol management

e) Network Layer
   - QUIC transport management
   - Connection handling
   - Stream management
   - Message serialization/deserialization

3. Communication Patterns
------------------------
- Node-to-Node: QUIC connections with dedicated streams
- Actor-to-Actor: Through Remote Supervisors
- Gossip Protocol: State synchronization between nodes
- Registration Updates: Through Monitor to Receptionist

4. State Management
------------------
- Actor State: Managed by Supervisors
- Node State: Managed by Monitor
- Cluster State: Managed by Monitor
- Connection State: Managed by NodeConnection

5. Implementation Strategy
-------------------------
Phase 1: P2P Implementation
- Direct node connections
- Basic actor registration
- Simple message routing

Phase 2: Gossip Protocol (Future)
- Dynamic node discovery
- State propagation
- Just-in-time connections

6. Message Flow
--------------
- Actor Registration → Monitor → Receptionist → Cluster Broadcast
- Actor Message → Remote Supervisor → NodeConnection → Remote Node
- Node State Changes → Monitor → Cluster State Update

7. Clustering Requirements
-------------------------
- System transparency
- Actor communication handling
- State tracking and synchronization
- Message relay capabilities
- Network management
- Actor registration coordination
- Gossip protocol support

8. Additional Considerations
--------------------------
- Stream management for data transfer
- Connection pooling strategy
- Actor path translation
- State verification mechanisms
- Timeout and retry policies
- Error handling and recovery
*/
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::time::Duration;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use hostname;
use parking_lot::Mutex;
use quinn::crypto::rustls::QuicClientConfig;
use tokio_util::sync::CancellationToken;

use super::membership::*;
use super::prelude::*;
use super::tls::TlsConfig;
use super::tls::TlsConfigError;

#[cfg(test)]
#[path = "cluster.test.rs"]
mod test;

#[derive(Clone)]
pub struct Name(String);

impl Name {
    pub fn hostname() -> Self {
        Name(
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
        )
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Name {
    fn from(s: &str) -> Self {
        Name(s.to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NetworkAddrError {
    #[error("Failed to parse network address: {0}")]
    ParseError(String),
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Invalid Socket Address: {0}")]
    InvalidSocketAddr(String),
    #[error("Addr parsing error: {0}")]
    AddrParseError(#[from] std::net::AddrParseError),
    #[error("failed to parse url into socket addr: {0}")]
    ToSocketAddrError(String),
}

pub enum NetworkAddr {
    Socket(SocketAddr),
    HostPort(String, u16),
}

impl NetworkAddr {
    pub fn host(&self) -> String {
        match self {
            NetworkAddr::Socket(addr) => addr.ip().to_string().as_str().to_string(),
            NetworkAddr::HostPort(host, _) => host.clone(),
        }
    }

    pub fn to_sock_addrs(&self) -> Result<SocketAddr, NetworkAddrError> {
        match self {
            NetworkAddr::Socket(addr) => Ok(*addr),
            NetworkAddr::HostPort(host, port) => (host.as_str(), *port)
                .to_socket_addrs()
                .map_err(|_| {
                    NetworkAddrError::ToSocketAddrError(
                        "failed to parse host/port into socket addr".to_string(),
                    )
                })?
                .next()
                .ok_or(NetworkAddrError::ToSocketAddrError(
                    "failed to resolve host".to_string(),
                )),
        }
    }
}

impl FromStr for NetworkAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try parsing as SocketAddr
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(NetworkAddr::Socket(addr));
        }

        // Try parsing as hostname:port
        if let Some((host, port_str)) = s.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                if !host.is_empty() {
                    return Ok(NetworkAddr::HostPort(host.to_string(), port));
                }
            }
        }
        Err(format!(
            "Failed to parse '{}' as SocketAddr, URL, or hostname:port",
            s
        ))
    }
}

impl std::fmt::Display for NetworkAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NetworkAddr::Socket(addr) => write!(f, "{}", addr),
            NetworkAddr::HostPort(host, port) => write!(f, "{}:{}", host, port),
        }
    }
}

// Newtype wrapper for Arc<NetworkAddr>
#[derive(Clone)]
pub struct NetworkAddrRef(pub Arc<NetworkAddr>);

impl FromStr for NetworkAddrRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the string into NetworkAddr first
        let addr = NetworkAddr::from_str(s)?;
        // Then wrap it in an Arc and our newtype
        Ok(NetworkAddrRef(Arc::new(addr)))
    }
}

// Allow easy dereferencing to access the inner NetworkAddr
impl std::ops::Deref for NetworkAddrRef {
    type Target = NetworkAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Allow conversion from NetworkAddrRef to Arc<NetworkAddr>
impl From<NetworkAddrRef> for Arc<NetworkAddr> {
    fn from(addr_ref: NetworkAddrRef) -> Self {
        addr_ref.0
    }
}

// Allow conversion from &str to NetworkAddrRef
impl From<&str> for NetworkAddrRef {
    fn from(s: &str) -> Self {
        s.parse()
            .unwrap_or_else(|e| panic!("Failed to parse address: {}", e))
    }
}

impl std::fmt::Display for NetworkAddrRef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ClusterError {
    #[error("TLS Configuration error: {0}")]
    TlsError(#[from] TlsConfigError),

    #[error("Server Error: {0}")]
    ServerError(String),
}

pub enum State {
    Starting,
    Running,
    Stopping,
    Stopped,
}

pub struct Config {
    pub name: Name,
    pub bind_addr: SocketAddr,
    pub peers: Vec<NetworkAddrRef>,
    pub tls: TlsConfig,
}

pub struct ClusterActor {
    config: Config,
    socket: quinn::Endpoint,
    cancellation: tokio_util::sync::CancellationToken,
    state: State,
    members: HashSet<Member>,
}

impl ClusterActor {
    pub fn new(config: Config) -> Result<Self, ClusterError> {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let mut crypto = config.tls.clone().into_server_config()?;

        // Enable ALPN with custom protocol for murmur
        crypto.alpn_protocols = vec![b"murmur".to_vec()];

        // Create QUIC Listener with TLS
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(crypto)
                .expect("Failed to create QUIC server config"),
        ));
        let endpoint = quinn::Endpoint::server(server_config, config.bind_addr)
            .map_err(|e| ClusterError::ServerError(e.to_string()))?;

        let cluster = ClusterActor {
            config,
            socket: endpoint,
            cancellation: cancellation.clone(),
            state: State::Stopped,
            members: HashSet::new(),
        };

        // Start the monitors
        // Create a Node Instance for each peer.
        Ok(cluster)
    }

    fn spawn_server(&self, endpoint: Endpoint<ClusterActor>) -> Result<(), ClusterError> {
        let socket = self.socket.clone();
        let cancellation = self.cancellation.clone();
        tracing::info!(addr=%self.config.bind_addr, node_id=%self.config.name, "Starting cluster server");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        break;
                    },
                    res = socket.accept() => {
                        let Some(incoming) = res else {
                            continue;
                        };
                        accept_connection(incoming, endpoint.clone(), cancellation.child_token()).await;
                    }
                }
            }
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor for ClusterActor {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!(node_id=%self.config.name, "Cluster started");
        self.state = State::Starting;
        if let Err(err) = self.spawn_server(ctx.endpoint()) {
            tracing::error!(error=%err, "Failed to start cluster server");
            self.state = State::Stopped;
            return;
        }

        // Connect to each peer
        for peer in self.config.peers.iter() {
            // let connection = match Connection::connect(
            //     peer.clone(),
            //     self.socket.clone(),
            //     self.config.tls.clone(),
            // ) {
            //     Ok(conn) => conn,
            //     Err(e) => {
            //         tracing::error!(error=%e, "Failed to connect to peer");
            //         continue;
            //     }
            // };

            let Some(member) = Member::spawn(
                ctx.subsystem(),
                Node::new(peer.clone()),
                self.socket.clone(),
                self.config.tls.clone(),
            ) else {
                tracing::error!("Failed to spawn member actor");
                continue;
            };
            self.members.insert(member);
        }
        self.state = State::Running;
    }
}

async fn accept_connection(
    incoming: quinn::Incoming,
    endpoint: Endpoint<ClusterActor>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let addr = incoming.remote_address();
    let span = tracing::debug_span!("incoming", addr=%addr);
    let _guard = span.enter();
    let connecting = match incoming.accept() {
        Ok(connection) => connection,
        Err(e) => {
            tracing::error!(error=%e, "Failed to accept connection");
            return;
        }
    };

    let connection = match connecting.await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error=%e, "Failed to establish connection");
            return;
        }
    };

    // ConnectionState::accept(connection, endpoint, cancellation);
}

#[derive(Clone)]
pub struct Cluster {
    endpoint: Endpoint<ClusterActor>,
}

impl Cluster {
    pub fn new(endpoint: Endpoint<ClusterActor>) -> Self {
        Cluster { endpoint }
    }
}

pub enum Membership {
    Pending,
    Up,
    Down,
    Joining,
    Leaving,
    Removed,
    Failed,
}
pub enum Reachability {
    Pending,
    Reachable {
        last_seen: chrono::DateTime<chrono::Utc>,
    },
    Unreachable,
}

impl Reachability {
    pub fn reachable_now() -> Self {
        Reachability::Reachable {
            last_seen: chrono::Utc::now(),
        }
    }
}

enum StreamState {
    Init,
    Joining,
    Handshake,
    Ready,
    Closed,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NodeId {
    id: u128,
}

impl NodeId {
    fn new() -> Self {
        NodeId {
            id: uuid::Uuid::new_v4().as_u128(),
        }
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:x}", self.id)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        NodeId::new()
    }
}

pub struct NodeAddr {
    pub name: String,
    pub addr: NetworkAddr,
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub addr: NetworkAddrRef,
    pub node_id: NodeId,
}

impl Node {
    fn new(addr: NetworkAddrRef) -> Self {
        Node {
            name: "".to_string(),
            addr,
            node_id: NodeId::new(),
        }
    }

    fn new_with_name(name: impl Into<String>, addr: NetworkAddrRef) -> Self {
        Node {
            name: name.into(),
            addr,
            node_id: NodeId::new(),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "node(name={}, addr={}, node_id={})",
            self.name, self.addr, self.node_id
        )
    }
}

#[derive(Clone)]
pub struct Member {
    node_id: NodeId,
    endpoint: Endpoint<MemberActor>,
}

impl Member {
    fn spawn(
        system: System,
        node: Node,
        socket: quinn::Endpoint,
        tls: TlsConfig,
    ) -> Option<Member> {
        let node_id = node.node_id.clone();
        let endpoint = system.spawn_with(MemberActor {
            connection: Connection {
                node,
                socket,
                tls,
                state: ConnectionState::NotReady,
            },
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

mod net {
    use std::sync::Arc;

    use bytes::{Buf, BufMut};
    use serde::{Deserialize, Serialize};

    pub struct Frame {
        pub length: usize,
        pub data: bytes::Bytes,
    }

    pub struct Frames(Arc<Vec<Frame>>);

    enum FrameReaderMode {
        Length,
        Data(usize),
    }

    pub struct FrameReader {
        state: FrameReaderMode,
        buffer: bytes::BytesMut,
    }

    impl FrameReader {
        pub fn new() -> Self {
            FrameReader {
                state: FrameReaderMode::Length,
                buffer: bytes::BytesMut::new(),
            }
        }

        pub fn extend(&mut self, data: bytes::Bytes) {
            self.buffer.extend_from_slice(&data);
        }

        pub fn parse(&mut self) -> Option<Frames> {
            let mut frames = Vec::new();
            loop {
                match self.state {
                    FrameReaderMode::Length => {
                        if self.buffer.len() < 8 {
                            break;
                        }
                        // Split off the length bytes
                        let length = self.buffer.split_to(8).get_u64() as usize;
                        self.state = FrameReaderMode::Data(length);
                    }
                    FrameReaderMode::Data(length) => {
                        if self.buffer.len() < length {
                            break;
                        }
                        let frame = Frame {
                            length,
                            data: self.buffer.split_to(length).freeze(),
                        };
                        self.state = FrameReaderMode::Length;
                        frames.push(frame);
                    }
                }
            }
            if frames.is_empty() {
                None
            } else {
                Some(Frames(Arc::new(frames)))
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum NodeMessage {
        Join,
        JoinAck,
        Handshake,
        HandshakeAck,
        Heartbeat,
    }

    impl Into<Frame> for NodeMessage {
        fn into(self) -> Frame {
            let data = bincode::serialize(&self).unwrap();
            Frame {
                length: data.len(),
                data: bytes::Bytes::from(data),
            }
        }
    }
}

struct ProcessFrame(pub net::Frame);

impl Message for ProcessFrame {
    type Result = ();
}

#[async_trait::async_trait]
pub trait ConnectionDriver: Send + Sync + 'static {
    async fn connect(&self) -> Result<(), ConnectionError>;
    fn send(&self, frame: net::Frame);
    fn close(&self);
}

#[derive(Clone)]
pub struct ConnectionDriverRef(pub Arc<Box<dyn ConnectionDriver>>);

impl Deref for ConnectionDriverRef {
    type Target = Box<dyn ConnectionDriver>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub struct QuicConnectionDriver {
    addr: NetworkAddrRef,
    socket: quinn::Endpoint,
    tls: TlsConfig,
    inner_connection: Mutex<Option<quinn::Connection>>,
}

impl QuicConnectionDriver {
    pub fn new(addr: NetworkAddrRef, socket: quinn::Endpoint, tls: TlsConfig) -> Self {
        QuicConnectionDriver {
            addr,
            socket,
            tls,
            inner_connection: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionDriver for QuicConnectionDriver {
    async fn connect(&self) -> Result<(), ConnectionError> {
        let crypto = self.tls.clone().into_client_config()?;
        let sock_addr = self.addr.to_sock_addrs()?;

        let Ok(client_config) = QuicClientConfig::try_from(crypto) else {
            return Err(ConnectionError::FailedToConnect(
                "Failed to create TLS QUIC client config".to_string(),
            ));
        };
        let client_config = quinn::ClientConfig::new(Arc::new(client_config));
        let connection = self
            .socket
            .connect_with(client_config, sock_addr, &self.addr.host())
            .map_err(|err| match err {
                quinn::ConnectError::EndpointStopping => ConnectionError::ConnectionClosed(
                    "internal endpoint is stopping, cannot create new connections".to_string(),
                ),
                quinn::ConnectError::CidsExhausted => ConnectionError::FailedToConnect(
                    "CID space are exhausted, cannot create new connections".to_string(),
                ),
                quinn::ConnectError::InvalidServerName(name) => {
                    ConnectionError::FailedToConnect(format!("Invalid server name: {}", name))
                }
                quinn::ConnectError::InvalidRemoteAddress(socket_addr) => {
                    ConnectionError::FailedToConnect(format!(
                        "Invalid remote address: {}",
                        socket_addr
                    ))
                }
                quinn::ConnectError::NoDefaultClientConfig => ConnectionError::InvalidConfiguration,
                quinn::ConnectError::UnsupportedVersion => ConnectionError::FailedToConnect(
                    "Peer does not support the required QUIC version".to_string(),
                ),
            })?
            .await?;

        *self.inner_connection.lock() = Some(connection);
        Ok(())
    }
    fn send(&self, frame: net::Frame) {}

    fn close(&self) {}
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

pub struct MemberActor {
    pub connection: Connection,
    pub membership: Membership,
    pub reachability: Reachability,
}

impl MemberActor {
    pub async fn connect(&mut self) -> Result<(), ConnectionError> {
        match &self.connection.state {
            ConnectionState::NotReady | ConnectionState::Disconnected { .. } => {
                let tls = self.connection.tls.clone();
                let addr = self.connection.node.addr.clone();
                let socket = self.connection.socket.clone();

                let crypto = tls.into_client_config()?;
                let sock_addr = addr.to_sock_addrs()?;

                let Ok(client_config) = QuicClientConfig::try_from(crypto) else {
                    return Err(ConnectionError::FailedToConnect(
                        "Failed to create TLS QUIC client config".to_string(),
                    ));
                };
                let client_config = quinn::ClientConfig::new(Arc::new(client_config));
                let connection = socket
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
                    })?
                    .await?;
                self.connection.state = ConnectionState::Connecting { connection };
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl Actor for MemberActor {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        tracing::info!(node_id=%self.connection.node.node_id, "Member started");

        // Establish the connection regardless of the current state
        if let Err(err) = self.connect().await {
            tracing::error!(error=%err, "Failed to establish connection");
            self.membership = Membership::Failed;
            self.reachability = Reachability::Unreachable;
            self.connection.state = ConnectionState::Disconnected { reason: err };
            return;
        }

        let ConnectionState::Connecting { connection } = &self.connection.state else {
            return;
        };

        // FIXME: Below section can be generalized to address actors on remote nodes.
        // in this case the actor in question is the remote cluster actor.

        let (stream_tx, mut stream_rx) = match connection.open_bi().await {
            Ok((stream_tx, stream_rx)) => (stream_tx, stream_rx),
            Err(e) => {
                tracing::error!(error=%e, "Failed to establish member connection stream");
                self.membership = Membership::Failed;
                self.reachability = Reachability::Unreachable;
                return;
            }
        };

        // Start accept loop for new streams
        let connection_ref = connection.clone();
        ctx.spawn(async move {
            loop {
                let (stream_tx, stream_rx) = match connection_ref.accept_bi().await {
                    Ok((stream_tx, stream_rx)) => (stream_tx, stream_rx),
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to accept new stream");
                        break;
                    }
                };
                // Establish Stream and Start Processing.
            }
        });

        // Start Monitor Loop with Cluster Actor after we have established the membership.
        // ctx.interval(Duration::from_secs(1), || MemberActorCheck);

        // Start Receive Loop for Cluster Stream
        ctx.spawn(async move {
            let mut reader = net::FrameReader::new();
            loop {
                let chunk = match stream_rx.read_chunk(1024, true).await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => {
                        tracing::info!("Stream closed");
                        break;
                    }
                    Err(e) => {
                        tracing::error!(error=%e, "Failed to read from stream");
                        break;
                    }
                };

                reader.extend(chunk.bytes);
                while let Some(frames) = reader.parse() {
                    // TODO: Process Frames by decode and forwarding to the actor.
                    // How do we know what type to decode into? (has to be runtime for decoding).
                    // Keep being straight ahead on this.
                }
            }
        });

        // Monitor the member connection with HEARTBEAT
        // Cluster Actor Starts
        // Each Peer Connection Made
        // Each Connection added as member
        // TODO: Member Actor Starts with opening a bi stream and send a HELLO message.
        // Remote Cluster Accepts the connection, creates a new member to await the bi stream and then awaits the first
        // message on its own member actor.
        // Remote member receives HELLO and responses with HELLO_ACK and waits for HANDSHAKE.
        // Local member receives HELLO_ACK and sends HANDSHAKE and waits for HANDSHAKE_ACK with
        // node information. This pipe will now be used for heartbeats and node gossip.
        //
        // HEARTBEATS are sent out at intervals through the member actor and it also waits for
        // heartbeats from the remote node.
        //
        // Once the member has been up for a grace period, it notifies the cluster actor of its
        // status update after a special cluster actor is established fro the remote node.
        //
        // When the member receives a new open stream, the first message sets the kind for the
        // stream: MONITOR or ACTOR.
        // - MONITOR is the initial stream that is open for establish the membership and
        // monitoring, only one MONITOR stream is allowed per member and is started when the member
        // is started.
        // - ACTOR opens a new stream for a specific addressable actor on the remote node. On the
        // remote node the actor is looked up in its receptionist and if the actor is available the
        // stream is opened and a new actor pipe is created. (NOTE: Pipe can be closed
        // periodically to free up resources). The actor pipe is simply a loop that listens for
        // stream messages and then forwards them to the actor via its local endpoint. It will then
        // send the response back to the remote node it came from. Pipe's are lightweight and we
        // can have many. On the node connecting the actor pipe, it establishes a local supervisor
        // that like a normal supervisor in the sytem except the actor is remote. The supervisor
        // gives out endpoints locally and then handles the marshalling of messages to the remote
        // endpoint and handles the responses back.
        //
        // NOTE: On the receptionist
        // The receptionist first line of defense for checking if an actor is available. Only the
        // cluster actor can be looked up by the receptionist without it being registered for a
        // particular node. For all other actors, the receptionist must be told about the actor for
        // it be available.
        //
        // On the cluster actor side is establish, the cluster actor will use the member to send
        // updates to the remote node using its actor endpoint. For the remote node, it also
        // establishes a cluster actor endpoint and sends its updstes.
        //
        // The supervisor and actor pipes maintain a reference to the member id so that each
        // incoming message can be routed out. For the supervisor, each outgoing message sent
        // needs a unique 'request' id that is used to route the response back to the correct
        // local singleshot pipe.
        //
        // Endpoint -> Remote Supervisor -> Stream TX -> <Network> -> Stream RX -> Actor Pipe ->
        // Endpoint -> Actor -> Response -> Actor Pipe -> Stream TX -> <Network> -> Stream RX ->
        // -> Remote Supervisor -> Channel Pipe -> Endpoint

        ctx.interval(Duration::from_millis(100), || {});
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("TLS Configuration error: {0}")]
    TlsError(#[from] TlsConfigError),

    #[error("Invalid configuration")]
    InvalidConfiguration,

    #[error("Network Address error: {0}")]
    NetworkAddrError(#[from] NetworkAddrError),

    #[error("Failed to connect: {0}")]
    FailedToConnect(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection closed: {0}")]
    ConnectionClosed(String),

    #[error("Application closed: {0}")]
    ApplicationClosed(String),

    #[error("Connection reset")]
    Reset,

    #[error("Connection timed out")]
    TimedOut,

    #[error("Connection locally closed")]
    LocallyClosed,
}

impl From<quinn::ConnectionError> for ConnectionError {
    fn from(err: quinn::ConnectionError) -> Self {
        match err {
            quinn::ConnectionError::VersionMismatch => ConnectionError::FailedToConnect(
                "Peer does not support the required QUIC version".to_string(),
            ),
            quinn::ConnectionError::TransportError(e) => {
                ConnectionError::ConnectionFailed(format!("Transport error: {}", e))
            }
            quinn::ConnectionError::CidsExhausted => ConnectionError::FailedToConnect(
                "CID space are exhausted, cannot create new connections".to_string(),
            ),
            quinn::ConnectionError::LocallyClosed => ConnectionError::LocallyClosed,
            quinn::ConnectionError::Reset => ConnectionError::Reset,
            quinn::ConnectionError::TimedOut => ConnectionError::TimedOut,
            quinn::ConnectionError::ApplicationClosed(code) => {
                ConnectionError::ApplicationClosed(format!("code: {}", code))
            }
            quinn::ConnectionError::ConnectionClosed(code) => {
                ConnectionError::ConnectionClosed(format!("code: {}", code))
            }
        }
    }
}

pub struct Connection {
    pub node: Node,
    pub socket: quinn::Endpoint,
    pub tls: TlsConfig,
    pub state: ConnectionState,
}

pub enum ConnectionState {
    NotReady,
    Connecting {
        connection: quinn::Connection,
    },
    Connected {
        connection: quinn::Connection,
        stream: quinn::SendStream,
    },
    Disconnected {
        reason: ConnectionError,
    },
}
