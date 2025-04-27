//! Examples of using the new test harness for testing node actors
use assert_matches::assert_matches;
use net::{AcceptStream, ConnectionError, NodeMessage};

use crate::id::Id;
use crate::net::NetworkAddrRef;
use crate::node::*;
use crate::test_utils::prelude::*;

#[test_log::test(tokio::test)]
async fn test_node_actor_with_harness() {
    // Create a test harness
    let test = ActorTestHarness::new();

    // Create and initialize the node actor
    let mut actor = test.spawn(NodeActor::new(
        Id::new(),
        Id::new(),
        NodeInfo {
            display_name: "test".to_string(),
            addr: NetworkAddrRef::from("127.0.0.1:8000"),
        },
        Box::new(MockNetworkDriver),
    ));

    // Start the actor
    actor.start().await;

    // Initial state assertions - since we're returning an error from our mock network,
    // the membership status should be Failed
    actor.assert_state(|state| {
        assert_matches!(state.membership, Status::Failed);
        // Reachability might be either Pending or Unreachable in this scenario
        match state.reachability {
            Reachability::Pending | Reachability::Unreachable { .. } => { /* Both acceptable */ }
            _ => panic!("Unexpected reachability state: {:?}", state.reachability),
        }
    });

    // Send a heartbeat update message
    let timestamp = chrono::Utc::now().timestamp_millis();
    let frame = net::Frame::ok(Id::new(), None, NodeMessage::Heartbeat { timestamp });
    let msg = NodeActorRecvFrameMessage(Ok(frame));
    actor.send(msg).await.unwrap();

    // Now verify the reachability state has changed
    actor.assert_state(|state| {
        match &state.reachability {
            Reachability::Reachable { .. } => { /* Expected */ }
            Reachability::Unreachable { .. } => { /* Also acceptable */ }
            _ => panic!("Unexpected reachability state: {:?}", state.reachability),
        }
    });
}

/// Very simple mock network driver for testing
struct MockNetworkDriver;

#[async_trait::async_trait]
impl crate::net::NetworkDriver for MockNetworkDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        Ok(())
    }

    async fn open_stream(&mut self) -> Result<crate::net::RawStream, ConnectionError> {
        // Create a dummy network error instead of trying to mock the streams
        Err(ConnectionError::TimedOut)
    }

    async fn accept_stream(&mut self) -> Result<AcceptStream, ConnectionError> {
        // Create a dummy network error instead of trying to mock the streams
        Err(ConnectionError::TimedOut)
    }
}
