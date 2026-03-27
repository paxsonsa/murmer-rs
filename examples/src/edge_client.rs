//! Edge Client Example — lightweight client connecting to a murmer cluster
//!
//! Demonstrates the `MurmerClient` API:
//!
//! 1. A single cluster node registers both a **public** and an **internal** actor.
//! 2. An Edge client connects and syncs — it discovers only the public actor.
//! 3. `lookup_wait` blocks until a public actor appears (even if it registered
//!    after the client connected).
//! 4. A **short-lived** client (pull-once) vs a **long-lived** client
//!    (periodic `sync_interval`) are demonstrated.
//!
//! Run with: `cargo test --bin edge_client`

fn main() {
    println!("Run this example via `cargo test --bin edge_client` to execute the test scenarios.");
}

// =============================================================================
// ACTOR DEFINITIONS
// =============================================================================

use murmer::prelude::*;
use serde::{Deserialize, Serialize};

/// A public-facing greeter actor — exposed to Edge clients.
#[derive(Debug)]
pub struct GreetingActor;

pub struct GreetingState {
    pub greeting: String,
}

impl Actor for GreetingActor {
    type State = GreetingState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = String, remote = "edge_client::SayHello")]
pub struct SayHello {
    pub name: String,
}

#[handlers]
impl GreetingActor {
    #[handler]
    fn say_hello(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut GreetingState,
        msg: SayHello,
    ) -> String {
        format!("{}, {}!", state.greeting, msg.name)
    }
}

/// An internal metrics actor — hidden from Edge clients.
#[derive(Debug)]
pub struct MetricsActor;

pub struct MetricsState {
    pub request_count: u64,
}

impl Actor for MetricsActor {
    type State = MetricsState;
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(result = u64, remote = "edge_client::GetCount")]
pub struct GetCount;

#[handlers]
impl MetricsActor {
    #[handler]
    fn get_count(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut MetricsState,
        _msg: GetCount,
    ) -> u64 {
        state.request_count
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use murmer::cluster::config::{ClusterConfigBuilder, Discovery, NodeClass};
    use murmer::cluster::sync::TypeRegistry;
    use murmer::{ClientOptions, MurmerClient, System};

    fn init_tracing() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();
    }

    fn server_config(name: &str) -> murmer::cluster::config::ClusterConfig {
        ClusterConfigBuilder::new()
            .name(name)
            .listen("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .cookie("edge-client-example")
            .discovery(Discovery::None)
            .node_class(NodeClass::Worker)
            .build()
            .unwrap()
    }

    // ── Test 1: Public actor visible, internal actor hidden ───────────────

    /// An Edge client only sees actors registered as Public.
    /// Internal actors are filtered from the OpLog delta before sending.
    #[tokio::test]
    async fn test_edge_client_sees_only_public_actors() {
        init_tracing();

        // Start a single-node cluster
        let server = System::clustered(
            server_config("greeting-server"),
            TypeRegistry::from_auto(),
            murmer::cluster::sync::SpawnRegistry::new(),
        )
        .await
        .unwrap();
        let server_addr = server.local_addr().unwrap();

        // Register a PUBLIC actor — Edge clients will discover this
        server.start_public(
            "api/greet",
            GreetingActor,
            GreetingState {
                greeting: "Hello".into(),
            },
        );

        // Register an INTERNAL actor — Edge clients will NOT see this
        server.start("metrics/requests", MetricsActor, MetricsState { request_count: 0 });

        // Connect a short-lived Edge client (pull once on connect)
        let client = MurmerClient::connect(server_addr, "edge-client-example")
            .await
            .unwrap();

        // Give the pull response a moment to arrive
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Edge client can look up the public greeter
        let ep = client.lookup::<GreetingActor>("api/greet");
        assert!(ep.is_some(), "Edge client should see the public greeter");

        // Edge client cannot see the internal metrics actor
        let internal_ep = client.lookup::<MetricsActor>("metrics/requests");
        assert!(
            internal_ep.is_none(),
            "Edge client must NOT see internal actors"
        );

        // Send a message through the remote endpoint
        let reply = ep
            .unwrap()
            .send(SayHello { name: "World".into() })
            .await
            .unwrap();
        assert_eq!(reply, "Hello, World!");

        client.disconnect().await;
        server.shutdown().await;
    }

    // ── Test 2: lookup_wait resolves for a late-registering actor ─────────

    /// `lookup_wait` subscribes to receptionist events and resolves as soon
    /// as the requested actor's OpLog sync arrives — even if the actor
    /// didn't exist when the client connected.
    #[tokio::test]
    async fn test_lookup_wait_for_late_actor() {
        init_tracing();

        let server = System::clustered(
            server_config("late-server"),
            TypeRegistry::from_auto(),
            murmer::cluster::sync::SpawnRegistry::new(),
        )
        .await
        .unwrap();
        let server_addr = server.local_addr().unwrap();

        // Connect BEFORE the actor exists
        let client = MurmerClient::connect(server_addr, "edge-client-example")
            .await
            .unwrap();

        // Nothing registered yet — lookup returns None immediately
        assert!(client.lookup::<GreetingActor>("api/greet").is_none());

        // Register the actor 300 ms after the client connected, concurrently
        // with lookup_wait so the wait resolves as soon as the pull response lands.
        let (ep_result, _) = tokio::join!(
            client.lookup_wait::<GreetingActor>("api/greet", Duration::from_secs(5)),
            async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                server.start_public(
                    "api/greet",
                    GreetingActor,
                    GreetingState {
                        greeting: "Hey".into(),
                    },
                );
            }
        );
        let ep = ep_result.unwrap();

        let reply = ep.send(SayHello { name: "Alice".into() }).await.unwrap();
        assert_eq!(reply, "Hey, Alice!");

        client.disconnect().await;
        server.shutdown().await;
    }

    // ── Test 3: Long-lived client with periodic sync ──────────────────────

    /// A long-lived Edge client with `sync_interval` re-pulls periodically,
    /// picking up actors that registered after the initial connect.
    #[tokio::test]
    async fn test_long_lived_client_periodic_sync() {
        init_tracing();

        let server = System::clustered(
            server_config("periodic-server"),
            TypeRegistry::from_auto(),
            murmer::cluster::sync::SpawnRegistry::new(),
        )
        .await
        .unwrap();
        let server_addr = server.local_addr().unwrap();

        // Connect with a 500 ms pull interval (fast for testing)
        let client = MurmerClient::connect_with_options(
            server_addr,
            "edge-client-example".into(),
            ClientOptions {
                sync_interval: Some(Duration::from_millis(500)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Register an actor AFTER connecting — periodic pull will discover it
        tokio::time::sleep(Duration::from_millis(100)).await;
        server.start_public(
            "api/greet",
            GreetingActor,
            GreetingState {
                greeting: "Greetings".into(),
            },
        );

        // Wait for the periodic pull to deliver the actor (up to 2 s)
        let ep = client
            .lookup_wait::<GreetingActor>("api/greet", Duration::from_secs(2))
            .await
            .unwrap();

        let reply = ep
            .send(SayHello { name: "Bob".into() })
            .await
            .unwrap();
        assert_eq!(reply, "Greetings, Bob!");

        assert!(client.is_connected());
        client.disconnect().await;
        server.shutdown().await;
    }
}
