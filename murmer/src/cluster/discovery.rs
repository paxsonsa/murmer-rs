use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use iroh::{EndpointAddr, EndpointId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::{Discovery, NodeIdentity};
use super::net::{NodeId, PeerAddr};
use super::transport::peer_addr;

/// Events emitted by the discovery subsystem.
///
/// A discovered peer is a seam-neutral [`PeerAddr`]: the peer's node id plus
/// direct-address hints. The id is required — the cluster dials by identity,
/// not by bare socket address — while hints are best-effort (and ignored by the
/// in-memory sim fabric).
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    PeerDiscovered(PeerAddr),
}

/// TXT-record key under which a node advertises its endpoint id over mDNS.
const MDNS_ENDPOINT_ID_KEY: &str = "endpoint-id";

/// Start the discovery mechanism(s) based on the config.
///
/// Returns a receiver of `DiscoveryEvent` that the ClusterSystem event loop
/// should poll. Discovered peers are candidates for connection — but discovery
/// only conveys *addressing hints*; the allowlist decides authorization.
pub fn start_discovery(
    identity: &NodeIdentity,
    discovery: &Discovery,
    shutdown: CancellationToken,
) -> mpsc::UnboundedReceiver<DiscoveryEvent> {
    let (tx, rx) = mpsc::unbounded_channel();

    match discovery {
        Discovery::Mdns { service_name } => {
            start_mdns(identity.clone(), service_name.clone(), tx.clone(), shutdown);
        }
        Discovery::SeedNodes(seeds) => {
            start_seed_connector(identity.clone(), seeds.clone(), tx.clone(), shutdown);
        }
        Discovery::Both {
            service_name,
            seed_nodes,
        } => {
            start_mdns(
                identity.clone(),
                service_name.clone(),
                tx.clone(),
                shutdown.clone(),
            );
            start_seed_connector(identity.clone(), seed_nodes.clone(), tx, shutdown);
        }
        Discovery::None => {}
    }

    rx
}

// =============================================================================
// mDNS DISCOVERY — zero-config LAN discovery via mdns-sd
// =============================================================================

fn start_mdns(
    identity: NodeIdentity,
    service_name: String,
    tx: mpsc::UnboundedSender<DiscoveryEvent>,
    shutdown: CancellationToken,
) {
    // mdns-sd is internally threaded — we bridge it into tokio via spawn_blocking
    tokio::spawn(async move {
        let service_type = format!("_{service_name}._tcp.local.");
        let mdns = match mdns_sd::ServiceDaemon::new() {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Failed to start mDNS daemon: {e}");
                return;
            }
        };

        // Register our service, advertising our endpoint id so peers can dial us.
        let endpoint_id = identity.endpoint_id.to_string();
        let our_service = mdns_sd::ServiceInfo::new(
            &service_type,
            &identity.name,
            &format!("{}.local.", identity.name),
            identity.host.as_str(),
            identity.port,
            [
                ("node-name", identity.name.as_str()),
                ("incarnation", &identity.incarnation.to_string()),
                (MDNS_ENDPOINT_ID_KEY, endpoint_id.as_str()),
            ]
            .as_slice(),
        );

        match our_service {
            Ok(info) => {
                if let Err(e) = mdns.register(info) {
                    tracing::error!("Failed to register mDNS service: {e}");
                    return;
                }
                tracing::info!("Registered mDNS service: {}", identity.name);
            }
            Err(e) => {
                tracing::error!("Failed to create mDNS service info: {e}");
                return;
            }
        }

        // Browse for peers
        let receiver = match mdns.browse(&service_type) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to browse mDNS: {e}");
                return;
            }
        };

        let mut seen: HashSet<EndpointId> = HashSet::new();

        loop {
            tokio::select! {
                event = tokio::task::spawn_blocking({
                    let receiver = receiver.clone();
                    move || receiver.recv_timeout(Duration::from_millis(500))
                }) => {
                    match event {
                        Ok(Ok(mdns_sd::ServiceEvent::ServiceResolved(info))) => {
                            // Parse the peer's endpoint id from the TXT records.
                            let Some(peer_id) = info
                                .get_property_val_str(MDNS_ENDPOINT_ID_KEY)
                                .and_then(|s| s.parse::<EndpointId>().ok())
                            else {
                                continue; // not a murmer/iroh peer, or missing id
                            };
                            // Don't discover ourselves.
                            if peer_id == identity.endpoint_id {
                                continue;
                            }
                            let port = info.get_port();
                            let hint: Vec<SocketAddr> = info
                                .get_addresses()
                                .iter()
                                .map(|ip| SocketAddr::new(ip.to_ip_addr(), port))
                                .collect();
                            if hint.is_empty() {
                                continue;
                            }
                            if seen.insert(peer_id) {
                                let peer = PeerAddr { id: NodeId(peer_id.to_string()), hint };
                                tracing::info!("mDNS discovered peer: {peer_id}");
                                let _ = tx.send(DiscoveryEvent::PeerDiscovered(peer));
                            }
                        }
                        Ok(Ok(_)) => {} // other mDNS events, ignore
                        Ok(Err(_)) => {} // timeout, retry
                        Err(e) => {
                            tracing::warn!("mDNS browse task error: {e}");
                            break;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    tracing::debug!("mDNS discovery shutting down");
                    let _ = mdns.shutdown();
                    break;
                }
            }
        }
    });
}

// =============================================================================
// SEED NODE CONNECTOR — connect to well-known endpoints
// =============================================================================

fn start_seed_connector(
    identity: NodeIdentity,
    seeds: Vec<EndpointAddr>,
    tx: mpsc::UnboundedSender<DiscoveryEvent>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Emit each seed as a discovered peer. The ClusterSystem event loop
        // will attempt to connect and retry on failure.
        let emit = |seed: &EndpointAddr| {
            if seed.id == identity.endpoint_id {
                return; // skip ourselves
            }
            let _ = tx.send(DiscoveryEvent::PeerDiscovered(peer_addr(seed)));
        };
        for seed in &seeds {
            emit(seed);
        }

        // Retry seeds that haven't connected after a delay
        let mut attempt = 0u32;
        loop {
            let delay = Duration::from_secs(2u64.pow(attempt.min(5))); // max 32s
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    for seed in &seeds {
                        emit(seed);
                    }
                    attempt += 1;
                    if attempt > 10 {
                        tracing::debug!("Seed connector: stopping retries after 10 attempts");
                        break;
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}
