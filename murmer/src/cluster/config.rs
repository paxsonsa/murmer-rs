use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

// =============================================================================
// NODE IDENTITY — uniquely identifies a node in the cluster
// =============================================================================

/// Identifies a node in the cluster. The `incarnation` field starts as a
/// random value at startup and is incremented monotonically by SWIM on
/// each refutation, allowing the protocol to distinguish between a node
/// that restarted (new random value) and one resolving a suspicion (increment).
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub struct NodeIdentity {
    /// User-provided or auto-generated name (e.g. "worker-1")
    pub name: String,
    /// IP or hostname
    pub host: String,
    /// QUIC listen port
    pub port: u16,
    /// Random incarnation discriminator — changes on restart
    pub incarnation: u64,
}

impl NodeIdentity {
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
            incarnation: rand::random(),
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        let ip: std::net::IpAddr = self.host.parse().unwrap_or([127, 0, 0, 1].into());
        SocketAddr::new(ip, self.port)
    }

    /// String used as node_id in OpLog / VersionVector / Receptionist.
    /// Includes the incarnation counter so restarted nodes get distinct IDs.
    pub fn node_id_string(&self) -> String {
        format!(
            "{}@{}:{}#{}",
            self.name, self.host, self.port, self.incarnation
        )
    }

    /// Returns just the "name@host:port" portion without incarnation.
    /// Useful for human-readable logging.
    pub fn display_id(&self) -> String {
        format!("{}@{}:{}", self.name, self.host, self.port)
    }
}

impl fmt::Display for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}:{}#{}",
            self.name, self.host, self.port, self.incarnation
        )
    }
}

impl PartialEq for NodeIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.host == other.host
            && self.port == other.port
            && self.incarnation == other.incarnation
    }
}

impl std::hash::Hash for NodeIdentity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.host.hash(state);
        self.port.hash(state);
        self.incarnation.hash(state);
    }
}

/// foca requires Identity to support `Renew` (bump incarnation) and provide
/// an address for SWIM protocol communication.
impl foca::Identity for NodeIdentity {
    type Addr = SocketAddr;

    fn addr(&self) -> SocketAddr {
        self.socket_addr()
    }

    fn renew(&self) -> Option<Self> {
        Some(Self {
            name: self.name.clone(),
            host: self.host.clone(),
            port: self.port,
            incarnation: self.incarnation.wrapping_add(1),
        })
    }

    /// When two identities share the same address, the one with the higher
    /// incarnation wins (most recently started node takes precedence).
    fn win_addr_conflict(&self, other: &Self) -> bool {
        self.incarnation > other.incarnation
    }
}

// =============================================================================
// CLUSTER CONFIG — builder pattern for cluster setup
// =============================================================================

/// How the cluster discovers peers.
#[derive(Debug, Clone, Default)]
pub enum Discovery {
    /// Use mDNS for zero-config discovery on the local network.
    Mdns { service_name: String },
    /// Connect to known seed node addresses.
    SeedNodes(Vec<SocketAddr>),
    /// Use both mDNS and seed nodes.
    Both {
        service_name: String,
        seed_nodes: Vec<SocketAddr>,
    },
    /// No automatic discovery — only manual connections.
    #[default]
    None,
}

/// Configuration for starting a clustered actor system.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub identity: NodeIdentity,
    pub listen_addr: SocketAddr,
    pub cookie: String,
    pub discovery: Discovery,
}

/// Builder for `ClusterConfig`.
pub struct ClusterConfigBuilder {
    name: Option<String>,
    listen_addr: Option<SocketAddr>,
    advertise_addr: Option<SocketAddr>,
    cookie: Option<String>,
    discovery: Discovery,
}

impl ClusterConfigBuilder {
    pub fn new() -> Self {
        Self {
            name: None,
            listen_addr: None,
            advertise_addr: None,
            cookie: None,
            discovery: Discovery::default(),
        }
    }

    /// Set an explicit advertise address for this node.
    ///
    /// Use this when binding on `0.0.0.0` or a wildcard — the advertise
    /// address is what other nodes will use to connect back to us.
    /// If not set, the listen address is used.
    pub fn advertise(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn listen(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.listen_addr = Some(addr.into());
        self
    }

    pub fn cookie(mut self, cookie: impl Into<String>) -> Self {
        self.cookie = Some(cookie.into());
        self
    }

    pub fn seed_nodes(mut self, seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        let seeds: Vec<SocketAddr> = seeds.into_iter().collect();
        self.discovery = match self.discovery {
            Discovery::Mdns { service_name } => Discovery::Both {
                service_name,
                seed_nodes: seeds,
            },
            _ => Discovery::SeedNodes(seeds),
        };
        self
    }

    pub fn discovery(mut self, discovery: Discovery) -> Self {
        self.discovery = discovery;
        self
    }

    pub fn build(self) -> Result<ClusterConfig, &'static str> {
        let listen_addr = self.listen_addr.ok_or("listen address is required")?;
        let cookie = self.cookie.ok_or("cookie is required")?;

        // Use advertise_addr for the identity if set, otherwise fall back to listen_addr.
        // This matters when binding on 0.0.0.0 — the advertise addr should be routable.
        let peer_addr = self.advertise_addr.unwrap_or(listen_addr);

        let name = self.name.unwrap_or_else(|| {
            let host = peer_addr.ip();
            let port = peer_addr.port();
            format!("{host}-{port}")
        });

        let identity = NodeIdentity::new(name, peer_addr.ip().to_string(), peer_addr.port());

        Ok(ClusterConfig {
            identity,
            listen_addr,
            cookie,
            discovery: self.discovery,
        })
    }
}

impl Default for ClusterConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterConfig {
    pub fn builder() -> ClusterConfigBuilder {
        ClusterConfigBuilder::new()
    }
}
