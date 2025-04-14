use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::str::FromStr;
use std::{
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::id::Id;
use crate::node::NodeInfo;
use crate::path::ActorPath;
use crate::receptionist::RawKey;
use crate::tls::TlsConfig;
use crate::tls::TlsConfigError;

#[cfg(test)]
#[path = "./net.test.rs"]
mod tests;

/// Protocol version for wire format
pub const CURRENT_PROTOCOL_VERSION: u16 = 1;

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
/// Error types for wire format operations
#[derive(thiserror::Error, Debug)]
pub enum NetError {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::error::EncodeError),

    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] bincode::error::DecodeError),

    #[error("Insufficient data: expected {expected} bytes, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    ProtocolVersionMismatch { expected: u16, actual: u16 },

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

/// Frame header with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// Unique identifier for the sender node
    pub sender_id: Id,
    /// Optional target (might be broadcast)
    pub target_id: Option<Id>,
    /// Timestamp in UTC milliseconds
    pub timestamp: u64,
    /// Protocol version for future compatibility
    pub protocol_version: u16,
    /// Optional correlation ID for request/response patterns
    pub correlation_id: Option<u64>,
}

impl Header {
    pub fn new(sender_id: Id, target_id: Option<Id>) -> Self {
        Header {
            sender_id,
            target_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            correlation_id: None,
        }
    }

    pub fn with_correlation_id(mut self, id: u64) -> Self {
        self.correlation_id = Some(id);
        self
    }
}

/// System messages for node-to-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMessage {
    Init {
        protocol_version: u16,
        id: Id,
    },
    InitAck {
        node_id: Id,
    },
    Join {
        name: String,
        capabilities: Vec<String>,
    },
    JoinAck {
        accepted: bool,
        reason: Option<String>,
    },
    Heartbeat {
        timestamp: i64,
    },
    Disconnect {
        reason: String,
    },
    ActorAdd {
        actor_path: ActorPath,
    },
    ActorRemove {
        actor_path: ActorPath,
    },
}

/// Actor messages with addressing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActorMessage {
    Init { actor_key: RawKey },
    Error { reason: String },
}

/// Member information for cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub node_id: Id,
    pub name: String,
    pub status: MemberStatus,
    pub address: String,
    pub last_seen: u64,
}

/// Member status in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Joining,
    Up,
    Leaving,
    Down,
    Removed,
}

/// Cluster state representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub members: Vec<MemberInfo>,
    pub leader_id: Option<Id>,
    pub term: u64,
}

/// Cluster management messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    MembershipUpdate { members: Vec<MemberInfo> },
    StateSync { state: ClusterState },
    LeaderElection { candidate_id: Id, term: u64 },
}

/// Enum for payload result with success or failure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload<T> {
    /// Successfully decoded payload
    Ok(T),
    /// Failed to decode with reason
    UnknownFailure(String),
}

impl<T> Payload<T> {
    /// Returns true if the payload is Ok
    pub fn is_ok(&self) -> bool {
        matches!(self, Payload::Ok(_))
    }

    /// Returns true if the payload is an UnknownFailure
    pub fn is_failure(&self) -> bool {
        matches!(self, Payload::UnknownFailure(_))
    }

    /// Returns the inner value if Ok, or None if failure
    pub fn ok(self) -> Option<T> {
        match self {
            Payload::Ok(value) => Some(value),
            Payload::UnknownFailure(_) => None,
        }
    }

    /// Returns a reference to the inner value if Ok, or None if failure
    pub fn as_ok(&self) -> Option<&T> {
        match self {
            Payload::Ok(value) => Some(value),
            Payload::UnknownFailure(_) => None,
        }
    }

    /// Returns the failure reason if UnknownFailure, or None if Ok
    pub fn failure_reason(&self) -> Option<&String> {
        match self {
            Payload::Ok(_) => None,
            Payload::UnknownFailure(reason) => Some(reason),
        }
    }

    /// Maps the inner value using the provided function if Ok
    pub fn map<U, F>(self, f: F) -> Payload<U>
    where
        U: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(T) -> U,
    {
        match self {
            Payload::Ok(value) => Payload::Ok(f(value)),
            Payload::UnknownFailure(reason) => Payload::UnknownFailure(reason),
        }
    }
}

/// Top-level frame that gets encoded/decoded from the wire
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame<T> {
    pub header: Header,
    /// Typed payload
    pub payload: Payload<T>,
}

impl<T> Frame<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Create a new frame with a failure payload
    pub fn failure(sender_id: Id, target_id: Option<Id>, reason: String) -> Self {
        Frame {
            header: Header::new(sender_id, target_id),
            payload: Payload::UnknownFailure(reason),
        }
    }

    /// Create a new frame with a successful payload
    pub fn ok(sender_id: Id, target_id: Option<Id>, value: T) -> Self {
        Frame {
            header: Header::new(sender_id, target_id),
            payload: Payload::Ok(value),
        }
    }

    /// Returns true if the payload is Ok
    pub fn is_ok(&self) -> bool {
        self.payload.is_ok()
    }

    /// Returns true if the payload is an UnknownFailure
    pub fn is_failure(&self) -> bool {
        self.payload.is_failure()
    }

    /// Maps the payload using the provided function
    pub fn map_payload<U, F>(self, f: F) -> Frame<U>
    where
        U: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(T) -> U,
    {
        Frame {
            header: self.header,
            payload: self.payload.map(f),
        }
    }

    /// Encode the frame to bytes
    pub fn encode(&self) -> Result<Bytes, NetError> {
        let config = bincode::config::standard();
        let mut buffer = BytesMut::new().writer();

        // Serialize the frame to bytes
        let _ = bincode::serde::encode_into_std_write(self, &mut buffer, config)?;

        // Calculate the length of the serialized data
        let buffer = buffer.into_inner();
        let length = buffer.len() as u64;

        // Create a buffer with capacity for length (8 bytes) + data
        let mut final_buffer = BytesMut::with_capacity(8 + buffer.len());

        // Write length in network byte order (big-endian)
        final_buffer.put_u64(length);
        final_buffer.extend_from_slice(&buffer);

        let final_buffer = final_buffer.freeze();

        Ok(final_buffer)
    }

    /// Decode a frame from bytes
    pub fn decode(bytes: Bytes) -> Result<Self, NetError> {
        // Use a consistent bincode configuration
        let config = bincode::config::standard();

        // Deserialize the frame
        let frame: Frame<T> = match bincode::serde::decode_from_slice(&bytes, config) {
            Ok((frame, _)) => frame,
            Err(err) => return Err(NetError::DeserializationError(err)),
        };

        // Check protocol version
        if frame.header.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(NetError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                actual: frame.header.protocol_version,
            });
        }

        Ok(frame)
    }
}

/// Frame reader for processing incoming data
pub struct FrameParser<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    state: ReaderState,
    buffer: BytesMut,
    _phantom: std::marker::PhantomData<T>,
}

enum ReaderState {
    ReadingLength,
    ReadingData(usize),
}

impl<T> FrameParser<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new frame reader
    pub fn new() -> Self {
        FrameParser {
            state: ReaderState::ReadingLength,
            buffer: BytesMut::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Add data to the reader buffer
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to parse a complete frame from the buffer
    pub fn parse(&mut self) -> Result<Option<Frame<T>>, NetError> {
        loop {
            match &self.state {
                ReaderState::ReadingLength => {
                    // Need at least 8 bytes for the length
                    if self.buffer.len() < 8 {
                        return Ok(None);
                    }

                    // Read the length prefix
                    let length = (&self.buffer[0..8]).get_u64() as usize;

                    // Remove the length bytes from the buffer
                    self.buffer.advance(8);

                    // Switch to reading data state
                    self.state = ReaderState::ReadingData(length);
                }

                ReaderState::ReadingData(length) => {
                    // Check if we have enough data
                    if self.buffer.len() < *length {
                        return Ok(None);
                    }

                    // Extract the frame data
                    let frame_data = self.buffer.split_to(*length).freeze();

                    // Reset state to read the next frame length
                    self.state = ReaderState::ReadingLength;

                    // Decode the frame
                    let frame = Frame::decode(frame_data)?;

                    return Ok(Some(frame));
                }
            }
        }
    }

    /// Try to parse multiple frames from the buffer
    pub fn parse_all(&mut self) -> Result<Vec<Frame<T>>, NetError> {
        let mut frames = Vec::new();

        while let Some(frame) = self.parse()? {
            frames.push(frame);
        }

        Ok(frames)
    }
}

/// A stream for reading frames of type T
pub struct FrameReader<T>
where
    T: Serialize + DeserializeOwned,
{
    inner: Box<dyn AsyncRead + Send + Unpin>,
    reader: FrameParser<T>,
}

impl<T> FrameReader<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(stream: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        FrameReader {
            inner: stream,
            reader: FrameParser::new(),
        }
    }

    /// Read the next frame from the stream
    pub async fn read_frame(&mut self) -> Result<Option<Frame<T>>, NetError> {
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
                    return Err(NetError::InvalidFrame(format!("Read error: {}", e)));
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
    pub async fn write_frame(&mut self, frame: &Frame<T>) -> Result<(), NetError> {
        let encoded = frame.encode()?;
        self.inner
            .write_all(&encoded)
            .await
            .map_err(|e| NetError::InvalidFrame(format!("Write error: {}", e)))?;
        self.inner
            .flush()
            .await
            .map_err(|e| NetError::InvalidFrame(format!("Flush error: {}", e)))?;
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

//
// Network Driver Implementations
//

/// A stream-like object that can be used to accept multiple connections.
/// This is designed to be used in a loop to accept multiple connections over time.
pub struct AcceptStream {
    accept_fn: Box<
        dyn FnMut() -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<RawStream, ConnectionError>> + Send>,
            > + Send,
    >,
    // The current future being polled, if any
    current_future: Option<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<RawStream, ConnectionError>> + Send>,
        >,
    >,
}

impl AcceptStream {
    /// Create a new AcceptStream with a custom implementation
    pub fn new<F, Fut>(mut accept_fn: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<RawStream, ConnectionError>> + Send + 'static,
    {
        AcceptStream {
            accept_fn: Box::new(move || Box::pin(accept_fn())),
            current_future: None,
        }
    }

    /// Accept the next connection
    pub async fn accept(&mut self) -> Result<RawStream, ConnectionError> {
        // Create a future that polls the next connection
        AcceptFuture { stream: self }.await
    }
}

/// A future that represents a single accept operation
struct AcceptFuture<'a> {
    stream: &'a mut AcceptStream,
}

impl<'a> std::future::Future for AcceptFuture<'a> {
    type Output = Result<RawStream, ConnectionError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = &mut *self;

        // If we don't have a future yet, create one
        if this.stream.current_future.is_none() {
            this.stream.current_future = Some((this.stream.accept_fn)());
        }

        // Poll the current future
        match this
            .stream
            .current_future
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(cx)
        {
            std::task::Poll::Ready(result) => {
                // Clear the current future so we'll create a new one next time
                this.stream.current_future = None;
                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Network driver trait for establishing connections and opening streams
#[async_trait::async_trait]
pub trait NetworkDriver: Send {
    /// Connect to a remote node
    async fn connect(&mut self) -> Result<(), ConnectionError>;

    /// Open a new bidirectional stream for communication
    /// Returns a type-erased stream that can be converted to a typed stream
    async fn open_stream(&mut self) -> Result<RawStream, ConnectionError>;

    /// Accept a new bidirectional stream for communication
    /// Returns an AcceptStream future that will resolve to a RawStream when awaited
    async fn accept_stream(&mut self) -> Result<AcceptStream, ConnectionError>;
}

#[derive(Debug)]
pub enum QuicConnectionState {
    NotReady,
    Connected { connection: quinn::Connection },
    Disconnected { reason: ConnectionError },
}

/// QUIC-based implementation of the NetworkDriver trait
pub struct QuicConnectionDriver {
    node_info: NodeInfo,
    socket: quinn::Endpoint,
    tls: TlsConfig,
    state: QuicConnectionState,
}

impl QuicConnectionDriver {
    pub fn new(node_info: NodeInfo, socket: quinn::Endpoint, tls: TlsConfig) -> Self {
        QuicConnectionDriver {
            node_info,
            socket,
            tls,
            state: QuicConnectionState::NotReady,
        }
    }
}

#[async_trait::async_trait]
impl NetworkDriver for QuicConnectionDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        match &self.state {
            QuicConnectionState::NotReady | QuicConnectionState::Disconnected { .. } => {
                let tls = self.tls.clone();
                let addr = self.node_info.addr.clone();
                let socket = self.socket.clone();

                let crypto = tls.into_client_config()?;
                let sock_addr = addr.to_sock_addrs()?;

                let Ok(client_config) = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
                else {
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
                        self.state = QuicConnectionState::Disconnected {
                            reason: ConnectionError::ConnectionFailed(
                                "Failed to open initial connection to node.".to_string(),
                            ),
                        };
                        return Err(e);
                    }
                }
                .await?;

                self.state = QuicConnectionState::Connected { connection };
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn open_stream(&mut self) -> Result<RawStream, ConnectionError> {
        match &self.state {
            QuicConnectionState::Connected { connection } => {
                // Open a bidirectional stream
                let (send, recv) = connection.open_bi().await.map_err(|e| {
                    ConnectionError::ConnectionFailed(format!("Failed to open stream: {}", e))
                })?;

                // Create the raw stream
                Ok(RawStream::new(Box::new(recv), Box::new(send)))
            }
            QuicConnectionState::NotReady => Err(ConnectionError::NotConnected),
            QuicConnectionState::Disconnected { reason } => Err(ConnectionError::ConnectionFailed(
                format!("Connection is disconnected: {}", reason),
            )),
        }
    }

    async fn accept_stream(&mut self) -> Result<AcceptStream, ConnectionError> {
        match &self.state {
            QuicConnectionState::Connected { connection } => {
                // Clone the connection to move it into the AcceptStream future
                let connection_clone = connection.clone();

                // Return an AcceptStream future that will resolve to a RawStream when awaited
                Ok(AcceptStream::new(move || {
                    let connection = connection_clone.clone();
                    async move {
                        // Accept a bidirectional stream
                        let (send, recv) = connection.accept_bi().await.map_err(|e| {
                            ConnectionError::ConnectionFailed(format!(
                                "Failed to accept stream: {}",
                                e
                            ))
                        })?;

                        // Create the raw stream
                        Ok(RawStream::new(Box::new(recv), Box::new(send)))
                    }
                }))
            }
            QuicConnectionState::NotReady => Err(ConnectionError::NotConnected),
            QuicConnectionState::Disconnected { reason } => Err(ConnectionError::ConnectionFailed(
                format!("Connection is disconnected: {}", reason),
            )),
        }
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

    #[error("Not connected")]
    NotConnected,
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
