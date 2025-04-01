use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::time::Duration;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use async_trait::async_trait;
use hostname;
use parking_lot::Mutex;
use quinn::crypto::rustls::QuicClientConfig;
use tokio_util::sync::CancellationToken;

use super::membership::*;
use super::net;
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
            //

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

enum StreamState {
    Init,
    Joining,
    Handshake,
    Ready,
    Closed,
}

pub struct NodeAddr {
    pub name: String,
    pub addr: NetworkAddr,
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub addr: NetworkAddrRef,
    pub node_id: Id,
}

impl Node {
    fn new(addr: NetworkAddrRef) -> Self {
        Node {
            name: "".to_string(),
            addr,
            node_id: Id::new(),
        }
    }

    fn new_with_name(name: impl Into<String>, addr: NetworkAddrRef) -> Self {
        Node {
            name: name.into(),
            addr,
            node_id: Id::new(),
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

pub struct Connection {
    pub node: Node,
    pub socket: quinn::Endpoint,
    pub tls: TlsConfig,
    pub state: ConnectionState,
}

pub enum ConnectionState {
    NotReady,
    Connected { connection: quinn::Connection },
    Disconnected { reason: ConnectionError },
}
