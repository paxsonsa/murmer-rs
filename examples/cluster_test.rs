use murmer_core::cluster::{ClusterEvent, ClusterManager, NodeIdentity};
use murmer_core::node::NodeId;
use murmer_core::node::{NodeAddress, NodeServer, NodeServerConfig};
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "Failed to install crypto provider")?;

    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!(
            "  {} seed <port>                    - Start a seed node",
            args[0]
        );
        eprintln!(
            "  {} join <port> <seed-addr>        - Join cluster via seed node",
            args[0]
        );
        eprintln!(
            "  {} demo                           - Run 3-node demo",
            args[0]
        );
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} seed 8001", args[0]);
        eprintln!("  {} join 8002 127.0.0.1:8001", args[0]);
        eprintln!("  {} demo", args[0]);
        return Ok(());
    }

    match args[1].as_str() {
        "seed" => {
            if args.len() < 3 {
                eprintln!("Error: Port required for seed mode");
                eprintln!("Usage: {} seed <port>", args[0]);
                return Ok(());
            }

            let port: u16 = args[2]
                .parse()
                .map_err(|_| format!("Invalid port: {}", args[2]))?;

            run_seed_node(port).await?;
        }

        "join" => {
            if args.len() < 4 {
                eprintln!("Error: Port and seed address required for join mode");
                eprintln!("Usage: {} join <port> <seed-addr>", args[0]);
                return Ok(());
            }

            let port: u16 = args[2]
                .parse()
                .map_err(|_| format!("Invalid port: {}", args[2]))?;

            let seed_addr = NodeAddress::parse(&args[3])
                .map_err(|_| format!("Invalid seed address: {}", args[3]))?;

            run_join_node(port, seed_addr).await?;
        }

        "demo" => {
            run_demo().await?;
        }

        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Valid commands: seed, join, demo");
        }
    }

    Ok(())
}

async fn run_seed_node(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("🌱 Starting seed node on port {}", port);

    // Create node server configuration
    let bind_addr = NodeAddress::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let node_id = NodeId::from_socket_addr(bind_addr.to_socket_addr());
    let config = NodeServerConfig::new_with_generated_cert(bind_addr.clone(), &node_id)?;

    println!("📋 Node ID: {}", node_id);
    println!("🔗 Binding to: {}", bind_addr);

    // Create and start node server
    let node_server = Arc::new(NodeServer::new(config, node_id.clone()).await?);
    node_server.start().await;

    let cluster_config = murmer_core::cluster::ClusterConfig {
        ..Default::default()
    };

    // Create cluster manager with socket-based identity
    let local_identity = NodeIdentity::new(bind_addr.to_socket_addr());
    let mut cluster_manager =
        ClusterManager::new(local_identity.clone(), node_server.clone(), cluster_config).await?;

    println!(
        "🌐 Starting cluster manager for identity: {}",
        local_identity
    );

    // Subscribe to cluster events
    let mut event_rx = cluster_manager.subscribe();

    println!("🚀 Seed node running! Press Ctrl+C to stop.");
    println!(
        "📡 Other nodes can join with: cargo run --bin cluster_test join <port> 127.0.0.1:{}",
        port
    );

    // Start event monitoring in background
    let event_handle = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match event {
                ClusterEvent::NodeJoined(identity) => {
                    println!("✅ Node joined cluster: {}", identity);
                }
                ClusterEvent::NodeLeft(identity) => {
                    println!("❌ Node left cluster: {}", identity);
                }
                ClusterEvent::NodeFailed(identity) => {
                    println!("💥 Node failed: {}", identity);
                }
                ClusterEvent::NodeUpdated(identity) => {
                    println!("🔄 Node updated: {}", identity);
                }
            }
        }
    });

    // Wait for shutdown signal
    let shutdown_signal = tokio::signal::ctrl_c();

    tokio::select! {
        _ = shutdown_signal => {
            println!("\n🛑 Shutdown signal received");
        }
        result = cluster_manager.start() => {
            if let Err(e) = result {
                tracing::error!("Cluster manager error: {}", e);
            }
        }
    }

    // Cleanup
    event_handle.abort();
    node_server.shutdown().await;

    println!("👋 Seed node stopped");
    Ok(())
}

async fn run_join_node(
    port: u16,
    seed_addr: NodeAddress,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "🔗 Starting join node on port {}, connecting to seed at {}",
        port, seed_addr
    );

    // Create node server configuration
    let bind_addr = NodeAddress::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let node_id = NodeId::from_socket_addr(bind_addr.to_socket_addr());
    let config = NodeServerConfig::new_with_generated_cert(bind_addr.clone(), &node_id)?;

    println!("📋 Node ID: {}", node_id);
    println!("🔗 Binding to: {}", bind_addr);

    // Create and start node server
    let node_server = Arc::new(NodeServer::new(config, node_id.clone()).await?);
    node_server.start().await;

    // Create cluster manager with socket-based identity
    let cluster_config = murmer_core::cluster::ClusterConfig {
        ..Default::default()
    };
    let local_identity = NodeIdentity::new(bind_addr.to_socket_addr());
    let mut cluster_manager =
        ClusterManager::new(local_identity.clone(), node_server.clone(), cluster_config).await?;

    println!(
        "🌐 Starting cluster manager for identity: {}",
        local_identity
    );

    // Create seed identity from socket address
    let seed_identity = NodeIdentity::new(seed_addr.to_socket_addr());

    println!("🎯 Attempting to join cluster via seed: {}", seed_identity);

    // Subscribe to cluster events
    let mut event_rx = cluster_manager.subscribe();

    // Join the cluster
    if let Err(e) = cluster_manager.join_cluster(vec![seed_identity]).await {
        println!("❌ Failed to join cluster: {}", e);
        return Err(e.into());
    }

    println!("✅ Cluster join initiated");

    println!("🚀 Join node running! Press Ctrl+C to stop.");

    // Start event monitoring in background
    let event_handle = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match event {
                ClusterEvent::NodeJoined(identity) => {
                    println!("✅ Node joined cluster: {}", identity);
                }
                ClusterEvent::NodeLeft(identity) => {
                    println!("❌ Node left cluster: {}", identity);
                }
                ClusterEvent::NodeFailed(identity) => {
                    println!("💥 Node failed: {}", identity);
                }
                ClusterEvent::NodeUpdated(identity) => {
                    println!("🔄 Node updated: {}", identity);
                }
            }
        }
    });

    // Wait for shutdown signal
    let shutdown_signal = tokio::signal::ctrl_c();

    tokio::select! {
        _ = shutdown_signal => {
            println!("\n🛑 Shutdown signal received");
        }
        result = cluster_manager.start() => {
            if let Err(e) = result {
                tracing::error!("Cluster manager error: {}", e);
            }
        }
    }

    // Cleanup
    event_handle.abort();
    node_server.shutdown().await;

    println!("👋 Join node stopped");
    Ok(())
}

async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Running 3-node cluster demo with Foca SWIM + QUIC");
    println!();
    println!("📋 Demo instructions:");
    println!("1. In terminal 1: cargo run --bin cluster_test seed 8001");
    println!("2. In terminal 2: cargo run --bin cluster_test join 8002 127.0.0.1:8001");
    println!("3. In terminal 3: cargo run --bin cluster_test join 8003 127.0.0.1:8001");
    println!("4. Watch the nodes discover each other through SWIM gossip!");
    println!();
    println!("✨ Features demonstrated:");
    println!("  • Foca SWIM protocol for cluster membership");
    println!("  • QUIC transport for reliable gossip messages");
    println!("  • Unified ports (same port for actors + cluster)");
    println!("  • Automatic failure detection and recovery");
    println!("  • Multi-hop cluster joining (node3 → node2 → node1)");
    println!();
    println!("🔍 What to look for:");
    println!("  • Node join/leave events");
    println!("  • Cluster membership updates");
    println!("  • SWIM gossip message exchange");
    println!("  • Graceful shutdown handling");

    Ok(())
}
