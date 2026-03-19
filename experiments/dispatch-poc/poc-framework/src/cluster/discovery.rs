use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::{Discovery, NodeIdentity};

/// Events emitted by the discovery subsystem.
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    PeerDiscovered(SocketAddr),
}

/// Start the discovery mechanism(s) based on the config.
///
/// Returns a receiver of `DiscoveryEvent` that the ClusterSystem event loop
/// should poll. Discovered peers are candidates for connection.
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

        // Register our service
        let our_service = mdns_sd::ServiceInfo::new(
            &service_type,
            &identity.name,
            &format!("{}.local.", identity.name),
            identity.host.as_str(),
            identity.port,
            [
                ("node-name", identity.name.as_str()),
                ("incarnation", &identity.incarnation.to_string()),
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

        let mut seen: HashSet<SocketAddr> = HashSet::new();

        loop {
            tokio::select! {
                event = tokio::task::spawn_blocking({
                    let receiver = receiver.clone();
                    move || receiver.recv_timeout(Duration::from_millis(500))
                }) => {
                    match event {
                        Ok(Ok(mdns_sd::ServiceEvent::ServiceResolved(info))) => {
                            let port = info.get_port();
                            for scoped_ip in info.get_addresses() {
                                let socket_addr = SocketAddr::new(scoped_ip.to_ip_addr(), port);
                                // Don't discover ourselves
                                if socket_addr == identity.socket_addr() {
                                    continue;
                                }
                                if seen.insert(socket_addr) {
                                    tracing::info!("mDNS discovered peer: {socket_addr}");
                                    let _ = tx.send(DiscoveryEvent::PeerDiscovered(socket_addr));
                                }
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
// SEED NODE CONNECTOR — connect to well-known addresses
// =============================================================================

fn start_seed_connector(
    identity: NodeIdentity,
    seeds: Vec<SocketAddr>,
    tx: mpsc::UnboundedSender<DiscoveryEvent>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Emit each seed as a discovered peer. The ClusterSystem event loop
        // will attempt to connect and retry on failure.
        for seed in &seeds {
            if *seed == identity.socket_addr() {
                continue; // skip ourselves
            }
            let _ = tx.send(DiscoveryEvent::PeerDiscovered(*seed));
        }

        // Retry seeds that haven't connected after a delay
        let mut attempt = 0u32;
        loop {
            let delay = Duration::from_secs(2u64.pow(attempt.min(5))); // max 32s
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    for seed in &seeds {
                        if *seed == identity.socket_addr() {
                            continue;
                        }
                        let _ = tx.send(DiscoveryEvent::PeerDiscovered(*seed));
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
