use super::*;
use assert_matches::assert_matches;
use parking_lot::Mutex;

struct TestingDriver {
    read_stream: Arc<Mutex<Vec<bytes::Bytes>>>,
}

impl TestingDriver {
    fn new() -> Self {
        TestingDriver {
            read_stream: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn push(&self, data: bytes::Bytes) {
        let mut read_stream = self.read_stream.lock();
        read_stream.push(data);
    }

    async fn expect_one_frame<T>(&self) -> net::Frame<T> {
        todo!("implement the test driver to expect one frame")
    }
}

struct MockConnectionDriverSpy {}

impl MockConnectionDriverSpy {
    fn new() -> Self {
        MockConnectionDriverSpy {}
    }
}

struct MockConnectionDriver {
    spy: Option<Arc<Mutex<MockConnectionDriverSpy>>>,
}

impl MockConnectionDriver {
    fn mocked() -> Self {
        MockConnectionDriver { spy: None }
    }

    fn with_spy(test_driver: TestingDriver) -> (Arc<Mutex<MockConnectionDriverSpy>>, Self) {
        let spy = Arc::new(Mutex::new(MockConnectionDriverSpy::new()));
        let driver = MockConnectionDriver {
            spy: Some(spy.clone()),
        };
        (spy, driver)
    }
}

#[async_trait]
impl ConnectionDriver for MockConnectionDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_member_actor_reachability_to_unreachable() {
    let driver = MockConnectionDriver::mocked();
    let mut ctx = Context::new();
    let mut actor = MemberActor {
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
    };

    let timestamp = chrono::Utc::now();
    let msg = MemberActorHeartbeatCheck { timestamp };

    actor.handle(&mut ctx, msg);
    assert_matches!(actor.reachability, Reachability::Pending);

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: timestamp
        }
    );

    // 2) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 2,
            last_seen: timestamp
        }
    );

    // 3) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 3,
            last_seen: timestamp
        }
    );

    // 4) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    // SHOULD BE UNREACHABLE NOW.
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 0,
            last_seen: timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_reachability_reset() {
    let driver = MockConnectionDriver::mocked();
    let mut ctx = Context::new();
    let mut actor = MemberActor {
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
    };

    let timestamp = chrono::Utc::now();
    let msg = MemberActorHeartbeatCheck { timestamp };

    actor.handle(&mut ctx, msg);
    assert_matches!(actor.reachability, Reachability::Pending);

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: timestamp
        }
    );

    // 2) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 2,
            last_seen: timestamp
        }
    );

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    // Re-check to reset the misses.
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: check_timestamp,
        }
    );

    // 3) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = check_timestamp + chrono::Duration::seconds(5);
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 1,
            last_seen: timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_unreachable_to_reachable() {
    let driver = MockConnectionDriver::mocked();
    let mut ctx = Context::new();
    let mut actor = MemberActor {
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Unreachable {
            pings: 0,
            last_seen: chrono::Utc::now(),
        },
    };

    let timestamp = chrono::Utc::now();

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 1,
            last_seen: timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let check_timestamp = timestamp + chrono::Duration::seconds(2);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);

    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 2,
            last_seen: timestamp
        }
    );

    // 2) Simulate a heartbeat check
    let msg = MemberActorHeartbeatUpdate {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 3,
            last_seen: check_timestamp
        }
    );

    // 4) Simulate a heartbeat check
    let msg = MemberActorHeartbeatCheck {
        timestamp: check_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    // SHOULD BE REACHABLE NOW.
    assert_matches!(
        actor.reachability,
        Reachability::Reachable {
            misses: 0,
            last_seen: timestamp
        }
    );
}

#[tokio::test]
async fn test_member_actor_unreachable_reset() {
    let driver = MockConnectionDriver::mocked();
    let mut ctx = Context::new();
    let mut actor = MemberActor {
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Unreachable {
            pings: 0,
            last_seen: chrono::Utc::now(),
        },
    };

    let timestamp = chrono::Utc::now();

    // Simulate a successful heartbeat
    let msg = MemberActorHeartbeatUpdate {
        timestamp: timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 1,
            last_seen: timestamp
        }
    );

    // 1) Simulate a heartbeat check failure
    // Move the check time forward by 5 seconds.
    let future_timestamp = timestamp + chrono::Duration::seconds(2);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: future_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 2,
            last_seen: futures_timestamp,
        }
    );

    // 2) Simulate a heartbeat update in the far future
    let future_timestamp = timestamp + chrono::Duration::seconds(20);
    let msg = MemberActorHeartbeatUpdate {
        timestamp: future_timestamp.clone(),
    };
    actor.handle(&mut ctx, msg);
    assert_matches!(
        actor.reachability,
        Reachability::Unreachable {
            pings: 0,
            last_seen: future_timestamp
        }
    );
}

#[tokio::test]
async fn test_membership_initiation() {
    let test_driver = TestingDriver::new();
    let (_, driver) = MockConnectionDriver::with_spy(test_driver);
    let mut ctx = Context::new();
    let mut actor = MemberActor {
        node: Node {
            name: "test".to_string(),
            node_id: Id::new(),
            addr: crate::cluster::NetworkAddrRef::from("127.0.0.1:8000"),
        },
        driver: Box::new(driver),
        membership: Membership::Pending,
        reachability: Reachability::Pending,
    };

    // 0) Connect to the Node.
    // 1) Send Init Message and Wait for Response. Pending
    // 2) Receive InitAccept Response and Send Join Message. Joining.
    // 2) Receive InitReject Response and update Membership. Down.
    // 3) Receive JoinAccept Response and update Membership. Up.
    // 4) Receive JoinReject

    actor.started(&mut ctx).await;
    assert_matches!(actor.membership, Membership::Pending);
    assert_matches!(actor.reachability, Reachability::Pending);

    // 1) Send Init Message and Wait for Response. Pending
    let frame: net::Frame<NodeMessage> = test_driver.expect_one_frame().await;
    // TODO: Implies a FramePayload::* variant
    assert_matches!(frame.payload, FramePayload::Ok(NodeMessage::Init { .. }));

    // TODO: Test inner stream handler endpoint callers.
    // Each open stream gets a dual loop for reading and writing.
    // The reading side is given an endpoint to forward frame messages to.
    // The writing side is stowed wrapped into a StreamHandle.
    // We need to test the reading side of the stream handler in a deterministic way.
}
