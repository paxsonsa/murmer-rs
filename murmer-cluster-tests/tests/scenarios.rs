//! Cluster integration test scenarios using ClusterSim.
//!
//! Each test builds a topology, starts actors, and asserts on cross-node
//! discovery and messaging through the public `System` API.

use murmer_cluster_tests::actors::*;
use murmer_cluster_tests::ClusterSim;

fn init() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();
}

// =============================================================================
// Discovery & Messaging
// =============================================================================

#[tokio::test]
async fn test_two_node_ping_pong() {
    init();

    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor("alpha", "ping/alpha", PingPong, PingPongState::new("alpha"));
    sim.start_actor("beta", "ping/beta", PingPong, PingPongState::new("beta"));

    // Beta discovers Alpha's actor
    let ep_alpha = sim.wait_discovery::<PingPong>("beta", "ping/alpha").await;
    let resp = ep_alpha
        .send(Ping {
            from: "beta".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from alpha (pinged by beta)");

    // Alpha discovers Beta's actor
    let ep_beta = sim.wait_discovery::<PingPong>("alpha", "ping/beta").await;
    let resp = ep_beta
        .send(Ping {
            from: "alpha".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from beta (pinged by alpha)");

    sim.shutdown_all().await;
}

#[tokio::test]
async fn test_three_node_mesh() {
    init();

    // Alpha is the seed. Beta and Gamma connect directly to Alpha.
    // NOTE: Beta↔Gamma messaging goes through Alpha's endpoint (transitive
    // routing is not yet implemented), so we only test direct-peer messaging
    // (alpha↔beta, alpha↔gamma) and verify transitive *discovery* for beta↔gamma.
    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))
        .node("gamma", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor("alpha", "ping/alpha", PingPong, PingPongState::new("alpha"));
    sim.start_actor("beta", "ping/beta", PingPong, PingPongState::new("beta"));
    sim.start_actor(
        "gamma",
        "ping/gamma",
        PingPong,
        PingPongState::new("gamma"),
    );

    // All nodes discover all actors (including transitively)
    for observer in &["alpha", "beta", "gamma"] {
        for target in &["ping/alpha", "ping/beta", "ping/gamma"] {
            sim.wait_discovery::<PingPong>(observer, target).await;
        }
    }

    // Direct peer messaging: beta → alpha (direct connection)
    let ep = sim.lookup::<PingPong>("beta", "ping/alpha").unwrap();
    let resp = ep
        .send(Ping {
            from: "beta".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from alpha (pinged by beta)");

    // Direct peer messaging: alpha → gamma (direct connection)
    let ep = sim.lookup::<PingPong>("alpha", "ping/gamma").unwrap();
    let resp = ep
        .send(Ping {
            from: "alpha".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from gamma (pinged by alpha)");

    // Direct peer messaging: gamma → alpha (direct connection)
    let ep = sim.lookup::<PingPong>("gamma", "ping/alpha").unwrap();
    let resp = ep
        .send(Ping {
            from: "gamma".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from alpha (pinged by gamma)");

    // Direct peer messaging: alpha → beta (direct connection)
    let ep = sim.lookup::<PingPong>("alpha", "ping/beta").unwrap();
    let resp = ep
        .send(Ping {
            from: "alpha".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from beta (pinged by alpha)");

    // Transitive discovery verified: beta sees gamma, gamma sees beta
    // (messaging between non-directly-connected peers is a future feature)
    assert!(sim.lookup::<PingPong>("beta", "ping/gamma").is_some());
    assert!(sim.lookup::<PingPong>("gamma", "ping/beta").is_some());

    sim.shutdown_all().await;
}

#[tokio::test]
async fn test_transitive_discovery() {
    init();

    // A → B → C chain. C seeds from B only, not A.
    let mut sim = ClusterSim::builder()
        .node("a", |n| n.seed_node())
        .node("b", |n| n.seed_from("a"))
        .node("c", |n| n.seed_from("b"))

        .build()
        .await;

    sim.start_actor("a", "counter/on-a", Counter, CounterState::new(10));

    // C should discover A's actor transitively through B
    sim.wait_discovery_timeout::<Counter>("c", "counter/on-a", std::time::Duration::from_secs(15))
        .await;

    // Discovery succeeded — the actor is visible from C's perspective
    assert!(sim.lookup::<Counter>("c", "counter/on-a").is_some());

    sim.shutdown_all().await;
}

#[tokio::test]
async fn test_cross_node_chat_room() {
    init();

    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor(
        "alpha",
        "chat/general",
        ChatRoom,
        ChatRoomState::new("general"),
    );

    // Beta discovers the chat room
    let ep = sim.wait_discovery::<ChatRoom>("beta", "chat/general").await;

    // Post messages from Beta
    let count = ep
        .send(PostMessage {
            from: "beta-user".into(),
            text: "hello from beta".into(),
        })
        .await
        .unwrap();
    assert_eq!(count, 1);

    let count = ep
        .send(PostMessage {
            from: "beta-user".into(),
            text: "second message".into(),
        })
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Read history back
    let history = ep.send(GetHistory).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0], "[general] beta-user: hello from beta");
    assert_eq!(history[1], "[general] beta-user: second message");

    sim.shutdown_all().await;
}

// =============================================================================
// Fault Tolerance
// =============================================================================

#[tokio::test]
async fn test_graceful_departure() {
    init();

    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor("alpha", "ping/depart", PingPong, PingPongState::new("alpha"));

    // Beta sees it
    sim.wait_discovery::<PingPong>("beta", "ping/depart").await;

    // Alpha departs
    sim.shutdown_node("alpha").await;

    // Beta should stop seeing it
    sim.wait_gone::<PingPong>("beta", "ping/depart").await;

    sim.shutdown_all().await;
}

#[tokio::test]
async fn test_actor_survives_peer_departure() {
    init();

    // Two directly-connected nodes. When alpha departs, beta's own actor
    // continues working and its local endpoint remains functional.
    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor("alpha", "ping/alpha", PingPong, PingPongState::new("alpha"));
    let beta_ep = sim.start_actor("beta", "counter/beta", Counter, CounterState::new(0));

    // Beta discovers Alpha's actor; Alpha discovers Beta's
    sim.wait_discovery::<PingPong>("beta", "ping/alpha").await;
    sim.wait_discovery::<Counter>("alpha", "counter/beta").await;

    // Both work before departure
    let ep = sim.lookup::<PingPong>("beta", "ping/alpha").unwrap();
    let resp = ep
        .send(Ping {
            from: "beta".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from alpha (pinged by beta)");

    beta_ep.send(Increment { amount: 5 }).await.unwrap();

    // Alpha departs
    sim.shutdown_node("alpha").await;

    // Alpha's actor disappears from Beta's view
    sim.wait_gone::<PingPong>("beta", "ping/alpha").await;

    // Beta's own local actor still works perfectly
    let count = beta_ep.send(Increment { amount: 10 }).await.unwrap();
    assert_eq!(count, 15, "beta's local actor should still work after peer departure");

    let count = beta_ep.send(GetCount).await.unwrap();
    assert_eq!(count, 15);

    sim.shutdown_all().await;
}

// =============================================================================
// Stateful Scenarios
// =============================================================================

#[tokio::test]
async fn test_counter_state_isolation() {
    init();

    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))

        .build()
        .await;

    sim.start_actor("alpha", "counter/alpha", Counter, CounterState::new(0));
    sim.start_actor("beta", "counter/beta", Counter, CounterState::new(0));

    // Cross-discover
    sim.wait_discovery::<Counter>("beta", "counter/alpha").await;
    sim.wait_discovery::<Counter>("alpha", "counter/beta").await;

    // Increment Alpha's counter 3 times from Beta
    let ep_alpha = sim.lookup::<Counter>("beta", "counter/alpha").unwrap();
    for _ in 0..3 {
        ep_alpha.send(Increment { amount: 10 }).await.unwrap();
    }

    // Beta's counter should be untouched
    let ep_beta = sim.lookup::<Counter>("alpha", "counter/beta").unwrap();
    let beta_count = ep_beta.send(GetCount).await.unwrap();
    assert_eq!(beta_count, 0, "beta counter should be unaffected");

    // Alpha's counter should be 30
    let alpha_count = ep_alpha.send(GetCount).await.unwrap();
    assert_eq!(alpha_count, 30, "alpha counter should be 30");

    sim.shutdown_all().await;
}

#[tokio::test]
async fn test_multi_actor_multi_node() {
    init();

    // Three nodes with different actor types. Alpha is the hub (seed node),
    // so all messaging goes through direct alpha↔beta and alpha↔gamma connections.
    // Gamma↔beta messaging would be transitive (not yet supported), so we
    // only test discovery for that pair.
    let mut sim = ClusterSim::builder()
        .node("alpha", |n| n.seed_node())
        .node("beta", |n| n.seed_from("alpha"))
        .node("gamma", |n| n.seed_from("alpha"))

        .build()
        .await;

    // Mix of actor types across nodes
    sim.start_actor("alpha", "ping/alpha", PingPong, PingPongState::new("alpha"));
    sim.start_actor("beta", "counter/beta", Counter, CounterState::new(100));
    sim.start_actor(
        "gamma",
        "chat/gamma",
        ChatRoom,
        ChatRoomState::new("gamma-room"),
    );

    // Direct peers discover each other's actors
    sim.wait_discovery::<PingPong>("beta", "ping/alpha").await;
    sim.wait_discovery::<Counter>("alpha", "counter/beta").await;
    sim.wait_discovery::<ChatRoom>("alpha", "chat/gamma").await;

    // Beta pings Alpha (direct connection)
    let ep = sim.lookup::<PingPong>("beta", "ping/alpha").unwrap();
    let resp = ep
        .send(Ping {
            from: "beta".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, "pong from alpha (pinged by beta)");

    // Alpha increments Beta's counter (direct connection)
    let ep = sim.lookup::<Counter>("alpha", "counter/beta").unwrap();
    let count = ep.send(Increment { amount: 5 }).await.unwrap();
    assert_eq!(count, 105);

    // Alpha posts to Gamma's chat (direct connection)
    let ep = sim.lookup::<ChatRoom>("alpha", "chat/gamma").unwrap();
    let msg_count = ep
        .send(PostMessage {
            from: "alpha-user".into(),
            text: "hello from alpha".into(),
        })
        .await
        .unwrap();
    assert_eq!(msg_count, 1);

    let history = ep.send(GetHistory).await.unwrap();
    assert_eq!(history[0], "[gamma-room] alpha-user: hello from alpha");

    // Transitive discovery: gamma sees beta's counter, beta sees gamma's chat
    sim.wait_discovery::<Counter>("gamma", "counter/beta").await;
    sim.wait_discovery::<ChatRoom>("beta", "chat/gamma").await;

    sim.shutdown_all().await;
}
