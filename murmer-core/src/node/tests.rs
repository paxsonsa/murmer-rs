#[cfg(test)]
mod tests {
    use crate::node::{NodeServer, NodeServerConfig, NodeAddress, NodeId, generate_self_signed_cert};
    use tokio::time::{timeout, Duration};
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_two_node_ping_pong() {
        // Initialize crypto provider
        let _ = rustls::crypto::ring::default_provider().install_default();
        
        // Initialize tracing for better debugging
        let _ = tracing_subscriber::fmt::try_init();

        // Node 1 configuration
        let node1_config = NodeServerConfig::new_with_generated_cert(
            NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0) // Let OS pick port
        ).expect("Failed to create node1 config");

        // Node 2 configuration  
        let node2_config = NodeServerConfig::new_with_generated_cert(
            NodeAddress::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0) // Let OS pick port
        ).expect("Failed to create node2 config");

        // Create both servers
        let server1 = NodeServer::new(node1_config).await
            .expect("Failed to create server1");
        let server2 = NodeServer::new(node2_config).await
            .expect("Failed to create server2");

        // Get the actual listening addresses
        let node1_addr = server1.local_addr().expect("Failed to get node1 address");
        let node2_addr = server2.local_addr().expect("Failed to get node2 address");
        
        println!("Node1 ({}) listening on {}", server1.node_id(), node1_addr);
        println!("Node2 ({}) listening on {}", server2.node_id(), node2_addr);

        // Start both servers
        server1.start().await;
        server2.start().await;

        // Give servers time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect node2 to node1
        server2.connect_to_node(
            server1.node_id().clone(), 
            node1_addr
        ).await.expect("Failed to connect node2 to node1");

        // Connect node1 to node2
        server1.connect_to_node(
            server2.node_id().clone(), 
            node2_addr
        ).await.expect("Failed to connect node1 to node2");

        // Give connections time to establish
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test ping from node1 to node2
        let pong_response = timeout(
            Duration::from_secs(5),
            server1.ping_node(server2.node_id(), "Hello from node1!".to_string())
        ).await.expect("Ping timed out")
            .expect("Ping failed");

        println!("Received pong from {}: {}", pong_response.from_node, pong_response.response);

        assert_eq!(pong_response.from_node, *server2.node_id());
        assert!(pong_response.response.contains("Hello from node1!"));

        // Test ping from node2 to node1
        let pong_response = timeout(
            Duration::from_secs(5),
            server2.ping_node(server1.node_id(), "Hello from node2!".to_string())
        ).await.expect("Ping timed out")
            .expect("Ping failed");

        println!("Received pong from {}: {}", pong_response.from_node, pong_response.response);

        assert_eq!(pong_response.from_node, *server1.node_id());
        assert!(pong_response.response.contains("Hello from node2!"));

        // Shutdown servers
        server1.shutdown().await;
        server2.shutdown().await;

        println!("Test completed successfully!");
    }

    #[tokio::test]
    async fn test_node_id_generation() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        
        // IDs should be unique
        assert_ne!(id1, id2);
        
        // IDs should display properly
        let id_str = id1.to_string();
        assert!(id_str.starts_with("node-"));
    }

    #[tokio::test]
    async fn test_node_address_parsing() {
        let addr_str = "127.0.0.1:8080";
        let addr = NodeAddress::parse(addr_str).expect("Failed to parse address");
        
        assert_eq!(addr.ip, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(addr.port, 8080);
        assert_eq!(addr.to_string(), addr_str);
    }

    #[tokio::test]
    async fn test_certificate_generation() {
        let node_id = NodeId::new();
        let (cert_chain, _private_key) = generate_self_signed_cert(&node_id)
            .expect("Failed to generate certificate");
        
        assert!(!cert_chain.is_empty());
        // Private key should not be empty (we can't easily test its content)
        // Just ensure it was created without panicking
    }
}