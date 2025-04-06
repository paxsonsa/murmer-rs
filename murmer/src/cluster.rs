use crate::net::{NetworkAddrError, NetworkAddrRef};
use std::collections::HashSet;
use std::{net::SocketAddr, sync::Arc};

use hostname;

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

pub struct ClusterId {
    pub id: Id,
    pub name: Name,
}

impl From<&str> for ClusterId {
    fn from(s: &str) -> Self {
        ClusterId {
            id: Id::new(),
            name: Name::from(s),
        }
    }
}

pub struct Config {
    pub id: Arc<ClusterId>,
    pub bind_addr: SocketAddr,
    pub peers: Vec<NetworkAddrRef>,
    pub tls: TlsConfig,
}

pub struct ClusterActor {
    config: Config,
    socket: quinn::Endpoint,
    cancellation: tokio_util::sync::CancellationToken,
    state: State,
    members: HashSet<Node>,
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
        tracing::info!(addr=%self.config.bind_addr, node_id=%self.config.id.name, "Starting cluster server");
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
        tracing::info!(node_id=%self.config.id.name, "Cluster started");
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

            let Some(member) = Node::spawn(
                ctx.subsystem(),
                self.config.id.clone(),
                NodeInfo::from(peer.clone()),
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
