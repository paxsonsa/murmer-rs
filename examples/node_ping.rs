use murmer_core::node::{NodeServer, NodeServerConfig, NodeAddress};
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize crypto provider
    rustls::crypto::ring::default_provider().install_default()
        .map_err(|_| "Failed to install crypto provider")?;
        
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  {} server <port>                    - Start a server node", args[0]);
        eprintln!("  {} client <server-addr>             - Start a client node that connects to server", args[0]);
        eprintln!("  {} ping <node1-port> <node2-addr>   - Connect two nodes and ping between them", args[0]);
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} server 8001", args[0]);
        eprintln!("  {} client 127.0.0.1:8001", args[0]);
        eprintln!("  {} ping 8001 127.0.0.1:8002", args[0]);
        return Ok(());
    }

    match args[1].as_str() {
        "server" => {
            if args.len() < 3 {
                eprintln!("Error: Port required for server mode");
                eprintln!("Usage: {} server <port>", args[0]);
                return Ok(());
            }
            
            let port: u16 = args[2].parse()
                .map_err(|_| format!("Invalid port: {}", args[2]))?;
            
            run_server(port).await?;
        }
        
        "client" => {
            if args.len() < 3 {
                eprintln!("Error: Server address required for client mode");
                eprintln!("Usage: {} client <server-addr>", args[0]);
                return Ok(());
            }
            
            let server_addr = NodeAddress::parse(&args[2])
                .map_err(|_| format!("Invalid server address: {}", args[2]))?;
            
            run_client(server_addr).await?;
        }
        
        "ping" => {
            if args.len() < 4 {
                eprintln!("Error: Both node addresses required for ping mode");
                eprintln!("Usage: {} ping <node1-port> <node2-addr>", args[0]);
                return Ok(());
            }
            
            let node1_port: u16 = args[2].parse()
                .map_err(|_| format!("Invalid port for node1: {}", args[2]))?;
                
            let node2_addr = NodeAddress::parse(&args[3])
                .map_err(|_| format!("Invalid address for node2: {}", args[3]))?;
            
            run_ping_test(node1_port, node2_addr).await?;
        }
        
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Valid commands: server, client, ping");
        }
    }

    Ok(())
}

async fn run_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server node on port {}", port);
    
    let config = NodeServerConfig::new_with_generated_cert(
        NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    )?;
    
    let node_id = config.node_id.clone();
    let server = NodeServer::new(config).await?;
    let actual_addr = server.local_addr()?;
    
    println!("Server node {} listening on {}", node_id, actual_addr);
    
    // Start accepting connections
    server.start().await;
    
    println!("Server started! Press Ctrl+C to shutdown");
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    
    println!("Shutting down server...");
    server.shutdown().await;
    
    Ok(())
}

async fn run_client(server_addr: NodeAddress) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting client node, connecting to {}", server_addr);
    
    let config = NodeServerConfig::new_with_generated_cert(
        NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0) // Let OS pick port
    )?;
    
    let node_id = config.node_id.clone();
    let server = NodeServer::new(config).await?;
    let actual_addr = server.local_addr()?;
    
    println!("Client node {} listening on {}", node_id, actual_addr);
    
    // Start the client server
    server.start().await;
    
    // Give server time to start
    sleep(Duration::from_millis(100)).await;
    
    // Connect to the server - we need a dummy NodeId since we don't know the server's ID
    println!("Attempting to connect to server at {}", server_addr);
    
    // For this simple example, we'll create a dummy server NodeId
    // In a real system, we'd discover this through cluster membership
    let dummy_server_id = murmer_core::node::NodeId::new();
    
    match server.connect_to_node(dummy_server_id.clone(), server_addr).await {
        Ok(()) => println!("Successfully connected to server!"),
        Err(e) => println!("Failed to connect to server: {}", e),
    }
    
    println!("Client running! Press Ctrl+C to shutdown");
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    
    println!("Shutting down client...");
    server.shutdown().await;
    
    Ok(())
}

async fn run_ping_test(node1_port: u16, node2_addr: NodeAddress) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running ping test between node1 (port {}) and node2 ({})", node1_port, node2_addr);
    
    // Create node 1
    let node1_config = NodeServerConfig::new_with_generated_cert(
        NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), node1_port)
    )?;
    let node1_id = node1_config.node_id.clone();
    let node1_server = NodeServer::new(node1_config).await?;
    let node1_actual_addr = node1_server.local_addr()?;
    
    // Create node 2
    let node2_config = NodeServerConfig::new_with_generated_cert(node2_addr)?;
    let node2_id = node2_config.node_id.clone();
    let node2_server = NodeServer::new(node2_config).await?;
    let node2_actual_addr = node2_server.local_addr()?;
    
    println!("Node1 {} listening on {}", node1_id, node1_actual_addr);
    println!("Node2 {} listening on {}", node2_id, node2_actual_addr);
    
    // Start both servers
    node1_server.start().await;
    node2_server.start().await;
    
    // Give servers time to start
    sleep(Duration::from_millis(200)).await;
    
    // Connect the nodes to each other
    println!("Connecting nodes...");
    
    node1_server.connect_to_node(node2_id.clone(), node2_actual_addr).await?;
    node2_server.connect_to_node(node1_id.clone(), node1_actual_addr).await?;
    
    // Give connections time to establish
    sleep(Duration::from_millis(200)).await;
    
    println!("Sending ping from Node1 to Node2...");
    match node1_server.ping_node(&node2_id, "Hello from Node1!".to_string()).await {
        Ok(response) => {
            println!("✅ Node1 -> Node2: {}", response.response);
        }
        Err(e) => {
            println!("❌ Node1 -> Node2 failed: {}", e);
        }
    }
    
    sleep(Duration::from_millis(100)).await;
    
    println!("Sending ping from Node2 to Node1...");
    match node2_server.ping_node(&node1_id, "Hello from Node2!".to_string()).await {
        Ok(response) => {
            println!("✅ Node2 -> Node1: {}", response.response);
        }
        Err(e) => {
            println!("❌ Node2 -> Node1 failed: {}", e);
        }
    }
    
    println!("Ping test completed! Shutting down nodes...");
    
    node1_server.shutdown().await;
    node2_server.shutdown().await;
    
    Ok(())
}