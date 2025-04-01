use super::*;
use assert_matches::assert_matches;

struct MockConnectionDriver {}

#[async_trait]
impl ConnectionDriver for MockConnectionDriver {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_member_actor_reachability_to_unreachable() {
    let driver = MockConnectionDriver {};
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
    let driver = MockConnectionDriver {};
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
    let driver = MockConnectionDriver {};
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
    let driver = MockConnectionDriver {};
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
