//! Counter example — demonstrates:
//!
//! 1. `#[derive(Message)]` generates Message + RemoteMessage impls
//! 2. `#[handlers]` + `#[handler]` generates Handler + RemoteDispatch
//! 3. Endpoint<A> abstracts local vs remote (single send() API)
//! 4. Receptionist stores type-erased entries, typed lookup by caller
//! 5. Lifecycle events are type-erased
//! 6. Same actor accessible locally and remotely through the same API
//! 7. ReceptionKey<A> groups actors by type for subscription-based discovery
//! 8. Listing<A> provides async streams with backfill + live updates
//! 9. Auto-deregister on supervisor shutdown via DeregisterGuard
//! 10. OpLog replication protocol with version vectors
//! 11. Node pruning when cluster members leave
//! 12. Delayed listing flush batches notifications
//! 13. Blip avoidance skips transient actors in the oplog
//! 14. Restart policies with limits and backoff
//! 15. Router round-robin and broadcast
//! 16. Auto-generated message structs from handler params
//! 17. Extension trait for ergonomic endpoint usage

use murmer::prelude::*;
use murmer_macros::handlers;
use serde::{Deserialize, Serialize};

// =============================================================================
// ACTOR DEFINITION
// =============================================================================

/// A simple counter actor that maintains a running total.
#[derive(Debug)]
pub struct CounterActor;

pub struct CounterState {
    count: i64,
    name: String,
}

impl Actor for CounterActor {
    type State = CounterState;
}

// =============================================================================
// RESPONSE TYPES (complex types that can't be auto-generated)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CounterInfo {
    pub name: String,
    pub count: i64,
}

// =============================================================================
// HANDLER IMPL — auto-generates message structs + extension trait
// =============================================================================

#[handlers]
impl CounterActor {
    #[handler]
    fn increment(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut CounterState,
        amount: i64,
    ) -> i64 {
        state.count += amount;
        state.count
    }

    #[handler]
    fn get_count(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState) -> i64 {
        state.count
    }

    #[handler]
    fn get_info(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState) -> CounterInfo {
        CounterInfo {
            name: state.name.clone(),
            count: state.count,
        }
    }
}

// =============================================================================
// MAIN
// =============================================================================

fn main() {
    println!("Run tests with: cargo nextest run -p murmer-examples");
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use murmer::{
        ActorEvent, ActorFactory, ActorRef, BackoffConfig, DispatchError, Handler, OpType,
        ReceptionKey, Receptionist, ReceptionistConfig, RemoteDispatch, RemoteInvocation,
        RemoteResponse, ResponseRegistry, RestartConfig, RestartPolicy, Router, RoutingStrategy,
        VersionVector,
    };

    // -------------------------------------------------------------------------
    // Test 1: Local actor via receptionist (using extension trait)
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_local_actor_via_receptionist() {
        let receptionist = Receptionist::new();

        // Subscribe to lifecycle events BEFORE starting actors
        let mut events = receptionist.subscribe_events();

        // Start actor — receptionist spawns supervisor and registers it
        let endpoint = receptionist.start(
            "counter/main",
            CounterActor,
            CounterState {
                count: 0,
                name: "main-counter".to_string(),
            },
        );

        // Check lifecycle event
        let event = events.recv().await.unwrap();
        assert!(matches!(event, ActorEvent::Registered { .. }));

        // Send via extension trait
        let result = endpoint.increment(5).await.unwrap();
        assert_eq!(result, 5);

        // Lookup the SAME actor from the receptionist — caller provides the type
        let looked_up = receptionist.lookup::<CounterActor>("counter/main").unwrap();
        let result = looked_up.get_count().await.unwrap();
        assert_eq!(result, 5);

        // Wrong type lookup returns None
        #[derive(Debug)]
        struct OtherActor;
        impl Actor for OtherActor {
            type State = ();
        }
        let bad_lookup = receptionist.lookup::<OtherActor>("counter/main");
        assert!(bad_lookup.is_none());

        // Non-existent label returns None
        let missing = receptionist.lookup::<CounterActor>("nonexistent");
        assert!(missing.is_none());
    }

    // -------------------------------------------------------------------------
    // Test 2: Remote actor via receptionist (simulated wire)
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_remote_actor_via_simulated_wire() {
        // === RECEIVER NODE ===
        let receiver_receptionist = Arc::new(Receptionist::new());

        // Start a local actor on the receiver
        let _local_endpoint = receiver_receptionist.start(
            "counter/worker-0",
            CounterActor,
            CounterState {
                count: 100,
                name: "worker-zero".to_string(),
            },
        );

        // Simulated wire channels (would be QUIC streams in the real system)
        let (wire_tx, wire_rx) = tokio::sync::mpsc::unbounded_channel::<RemoteInvocation>();
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<RemoteResponse>();

        // Spawn the receiver's "node server"
        let recv_receptionist = receiver_receptionist.clone();
        let receiver_handle = tokio::spawn(murmer::run_node_receiver(
            recv_receptionist,
            wire_rx,
            resp_tx,
        ));

        // Spawn response router
        let response_registry = ResponseRegistry::new();
        let response_registry_clone = response_registry.clone();
        let router_handle = tokio::spawn(async move {
            while let Some(response) = resp_rx.recv().await {
                response_registry_clone.complete(response);
            }
        });

        // === SENDER NODE ===
        let sender_receptionist = Receptionist::new();

        // Register the remote actor — sender knows the type and the wire channel
        sender_receptionist.register_remote::<CounterActor>(
            "counter/worker-0",
            wire_tx,
            response_registry,
        );

        // Lookup returns a remote Endpoint<CounterActor> — same API as local!
        let remote_endpoint = sender_receptionist
            .lookup::<CounterActor>("counter/worker-0")
            .unwrap();

        // Send messages via extension trait — IDENTICAL API to local
        let result = remote_endpoint.increment(10).await.unwrap();
        assert_eq!(result, 110);

        let result = remote_endpoint.increment(7).await.unwrap();
        assert_eq!(result, 117);

        let result = remote_endpoint.get_count().await.unwrap();
        assert_eq!(result, 117);

        let info = remote_endpoint.get_info().await.unwrap();
        assert_eq!(
            info,
            CounterInfo {
                name: "worker-zero".to_string(),
                count: 117,
            }
        );

        // Cleanup
        drop(remote_endpoint);
        drop(sender_receptionist);
        drop(receiver_receptionist);
        receiver_handle.await.unwrap();
        router_handle.await.unwrap();
    }

    // -------------------------------------------------------------------------
    // Test 3: Lifecycle events
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_lifecycle_events() {
        let receptionist = Receptionist::new();
        let mut events = receptionist.subscribe_events();

        // Start two actors
        let _e1 = receptionist.start(
            "counter/a",
            CounterActor,
            CounterState {
                count: 0,
                name: "a".to_string(),
            },
        );
        let _e2 = receptionist.start(
            "counter/b",
            CounterActor,
            CounterState {
                count: 0,
                name: "b".to_string(),
            },
        );

        // Should get two Registered events
        let ev1 = events.recv().await.unwrap();
        let ev2 = events.recv().await.unwrap();
        assert!(matches!(ev1, ActorEvent::Registered { .. }));
        assert!(matches!(ev2, ActorEvent::Registered { .. }));

        // Deregister one
        receptionist.deregister("counter/a");
        let ev3 = events.recv().await.unwrap();
        assert!(matches!(ev3, ActorEvent::Deregistered { .. }));

        // Lookup of deregistered actor returns None
        let gone = receptionist.lookup::<CounterActor>("counter/a");
        assert!(gone.is_none());

        // Other actor still works
        let still_there = receptionist.lookup::<CounterActor>("counter/b").unwrap();
        let result = still_there.get_count().await.unwrap();
        assert_eq!(result, 0);
    }

    // -------------------------------------------------------------------------
    // Test 4: Multiple actors, same type, different labels
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_multiple_actors_same_type() {
        let receptionist = Receptionist::new();

        let ep1 = receptionist.start(
            "counter/1",
            CounterActor,
            CounterState {
                count: 0,
                name: "one".to_string(),
            },
        );
        let ep2 = receptionist.start(
            "counter/2",
            CounterActor,
            CounterState {
                count: 0,
                name: "two".to_string(),
            },
        );

        // They are independent
        ep1.increment(10).await.unwrap();
        ep2.increment(20).await.unwrap();

        // Lookup each independently
        let l1 = receptionist.lookup::<CounterActor>("counter/1").unwrap();
        let l2 = receptionist.lookup::<CounterActor>("counter/2").unwrap();

        let c1 = l1.get_count().await.unwrap();
        let c2 = l2.get_count().await.unwrap();
        assert_eq!(c1, 10);
        assert_eq!(c2, 20);
    }

    // -------------------------------------------------------------------------
    // Test 5: Typed keys and listing (subscription-based discovery)
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_typed_keys_and_listing() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("workers");

        // Start two workers and check them in with the same key
        let _ep1 = receptionist.start(
            "counter/w1",
            CounterActor,
            CounterState {
                count: 10,
                name: "worker-1".to_string(),
            },
        );
        receptionist.check_in("counter/w1", worker_key.clone());

        let _ep2 = receptionist.start(
            "counter/w2",
            CounterActor,
            CounterState {
                count: 20,
                name: "worker-2".to_string(),
            },
        );
        receptionist.check_in("counter/w2", worker_key.clone());

        // Subscribe to the listing — should backfill with both existing workers
        let mut listing = receptionist.listing(worker_key.clone());

        let ep_a = listing.next().await.unwrap();
        let ep_b = listing.next().await.unwrap();

        // Both should be accessible (order may vary, so collect counts)
        let count_a = ep_a.get_count().await.unwrap();
        let count_b = ep_b.get_count().await.unwrap();
        let mut counts = vec![count_a, count_b];
        counts.sort();
        assert_eq!(counts, vec![10, 20]);

        // Now start a THIRD worker and check it in — listing should get it live
        let _ep3 = receptionist.start(
            "counter/w3",
            CounterActor,
            CounterState {
                count: 30,
                name: "worker-3".to_string(),
            },
        );
        receptionist.check_in("counter/w3", worker_key.clone());

        let ep_c = listing.next().await.unwrap();
        let count_c = ep_c.get_count().await.unwrap();
        assert_eq!(count_c, 30);

        // Wrong type key doesn't match
        #[derive(Debug)]
        struct OtherActor;
        impl Actor for OtherActor {
            type State = ();
        }
        let other_key = ReceptionKey::<OtherActor>::new("workers");
        let mut other_listing = receptionist.listing(other_key);
        assert!(other_listing.try_next().is_none());
    }

    // -------------------------------------------------------------------------
    // Test 6: Auto-deregister on supervisor shutdown
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_auto_deregister_on_stop() {
        let receptionist = Receptionist::new();
        let mut events = receptionist.subscribe_events();

        let endpoint = receptionist.start(
            "counter/ephemeral",
            CounterActor,
            CounterState {
                count: 0,
                name: "ephemeral".to_string(),
            },
        );

        // Consume the Registered event
        let ev = events.recv().await.unwrap();
        assert!(matches!(ev, ActorEvent::Registered { .. }));

        // Verify it's accessible
        let result = endpoint.get_count().await.unwrap();
        assert_eq!(result, 0);

        // Stop the actor — supervisor receives shutdown, guard triggers deregister
        receptionist.stop("counter/ephemeral");

        // Wait for the Deregistered event (supervisor needs a moment to exit)
        let ev = events.recv().await.unwrap();
        assert!(matches!(ev, ActorEvent::Deregistered { .. }));

        // Lookup should now return None
        let gone = receptionist.lookup::<CounterActor>("counter/ephemeral");
        assert!(gone.is_none());
    }

    // -------------------------------------------------------------------------
    // Test 7: OpLog basics
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_oplog_basics() {
        let receptionist = Receptionist::with_node_id("node-A");

        let _ep1 = receptionist.start(
            "counter/x",
            CounterActor,
            CounterState {
                count: 0,
                name: "x".to_string(),
            },
        );
        let _ep2 = receptionist.start(
            "counter/y",
            CounterActor,
            CounterState {
                count: 0,
                name: "y".to_string(),
            },
        );

        // OpLog should have 2 register entries
        let ops = receptionist.oplog_snapshot();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].node_id, "node-A");
        assert_eq!(ops[0].seq, 1);
        assert_eq!(ops[1].seq, 2);

        // Deregister one — should add a Remove op
        receptionist.deregister("counter/x");
        let ops = receptionist.oplog_snapshot();
        assert_eq!(ops.len(), 3);

        // Version vector should reflect our latest seq
        let vv = receptionist.version_vector();
        assert_eq!(vv.get("node-A"), 3);

        // ops_since should return only unseen ops
        let mut peer_vv = VersionVector::new();
        peer_vv.update("node-A", 1); // peer has seen seq 1
        let unseen = receptionist.ops_since(&peer_vv);
        assert_eq!(unseen.len(), 2); // seq 2 and 3
    }

    // -------------------------------------------------------------------------
    // Test 8: Node pruning
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_node_pruning() {
        let receptionist = Receptionist::with_node_id("node-A");
        let mut events = receptionist.subscribe_events();

        // Start local actors (owned by node-A)
        let _ep1 = receptionist.start(
            "counter/local-1",
            CounterActor,
            CounterState {
                count: 0,
                name: "local-1".to_string(),
            },
        );

        // Register remote actors from node-B
        let (wire_tx1, _wire_rx1) = tokio::sync::mpsc::unbounded_channel();
        let (wire_tx2, _wire_rx2) = tokio::sync::mpsc::unbounded_channel();
        receptionist.register_remote_from_node::<CounterActor>(
            "counter/remote-1",
            wire_tx1,
            ResponseRegistry::new(),
            "node-B",
        );
        receptionist.register_remote_from_node::<CounterActor>(
            "counter/remote-2",
            wire_tx2,
            ResponseRegistry::new(),
            "node-B",
        );

        // Drain the 3 Registered events
        for _ in 0..3 {
            events.recv().await.unwrap();
        }

        // All 3 should be lookupable
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/local-1")
                .is_some()
        );
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/remote-1")
                .is_some()
        );
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/remote-2")
                .is_some()
        );

        // Prune node-B — all its actors should be removed
        receptionist.prune_node("node-B");

        // Should get 2 Deregistered events
        let ev1 = events.recv().await.unwrap();
        let ev2 = events.recv().await.unwrap();
        assert!(matches!(ev1, ActorEvent::Deregistered { .. }));
        assert!(matches!(ev2, ActorEvent::Deregistered { .. }));

        // node-B actors gone, node-A actor still there
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/local-1")
                .is_some()
        );
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/remote-1")
                .is_none()
        );
        assert!(
            receptionist
                .lookup::<CounterActor>("counter/remote-2")
                .is_none()
        );
    }

    // -------------------------------------------------------------------------
    // Test 9: Cross-node op exchange simulation
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_cross_node_op_exchange() {
        let node_a = Receptionist::with_node_id("node-A");
        let node_b = Receptionist::with_node_id("node-B");

        // Each node registers actors locally
        let _a1 = node_a.start(
            "counter/a1",
            CounterActor,
            CounterState {
                count: 0,
                name: "a1".to_string(),
            },
        );
        let _b1 = node_b.start(
            "counter/b1",
            CounterActor,
            CounterState {
                count: 0,
                name: "b1".to_string(),
            },
        );

        // Subscribe to events on both to see replication
        let mut events_a = node_a.subscribe_events();
        let mut events_b = node_b.subscribe_events();

        // Simulate gossip: node-A sends its ops to node-B
        let a_ops = node_a.ops_since(&node_b.version_vector());
        assert_eq!(a_ops.len(), 1); // the Register for counter/a1
        node_b.apply_ops(a_ops);

        // node-B should have received the Registered event
        let ev = events_b.recv().await.unwrap();
        assert!(matches!(ev, ActorEvent::Registered { .. }));

        // Simulate gossip: node-B sends its ops to node-A
        let b_ops = node_b.ops_since(&node_a.version_vector());
        assert_eq!(b_ops.len(), 1); // the Register for counter/b1
        node_a.apply_ops(b_ops);

        let ev = events_a.recv().await.unwrap();
        assert!(matches!(ev, ActorEvent::Registered { .. }));

        // After exchange, version vectors should be up to date
        let vv_a = node_a.version_vector();
        let vv_b = node_b.version_vector();
        assert_eq!(vv_a.get("node-A"), 1);
        assert_eq!(vv_a.get("node-B"), 1);
        assert_eq!(vv_b.get("node-A"), 1);
        assert_eq!(vv_b.get("node-B"), 1);

        // Re-exchange should be a no-op (version vectors match)
        let a_ops = node_a.ops_since(&node_b.version_vector());
        let b_ops = node_b.ops_since(&node_a.version_vector());
        assert_eq!(a_ops.len(), 0);
        assert_eq!(b_ops.len(), 0);
    }

    // -------------------------------------------------------------------------
    // Test 10: Delayed listing flush
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_delayed_listing_flush() {
        let receptionist = Receptionist::with_config(ReceptionistConfig {
            node_id: "flush-node".to_string(),
            flush_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        });

        let worker_key = ReceptionKey::<CounterActor>::new("batch-workers");

        // Subscribe first
        let mut listing = receptionist.listing(worker_key.clone());

        // Start 3 actors and check them in rapidly
        for i in 0..3 {
            let label = format!("counter/batch-{}", i);
            let _ep = receptionist.start(
                &label,
                CounterActor,
                CounterState {
                    count: i as i64,
                    name: format!("batch-{}", i),
                },
            );
            receptionist.check_in(&label, worker_key.clone());
        }

        // Immediately after check_in, listing should have nothing (buffered)
        assert!(listing.try_next().is_none());

        // Wait for flush interval to fire
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now all 3 should arrive
        let mut received = Vec::new();
        while let Some(ep) = listing.try_next() {
            let count = ep.get_count().await.unwrap();
            received.push(count);
        }
        received.sort();
        assert_eq!(received.len(), 3);
        assert_eq!(received, vec![0, 1, 2]);
    }

    // -------------------------------------------------------------------------
    // Test 11: Blip avoidance
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_blip_avoidance() {
        let receptionist = Receptionist::with_config(ReceptionistConfig {
            node_id: "blip-node".to_string(),
            blip_window: Some(Duration::from_millis(100)),
            ..Default::default()
        });

        // Start an actor then immediately deregister it (blip)
        let _ep = receptionist.start(
            "counter/blip",
            CounterActor,
            CounterState {
                count: 0,
                name: "blip".to_string(),
            },
        );

        // OpLog should be empty (blip window hasn't expired)
        let ops = receptionist.oplog_snapshot();
        assert_eq!(ops.len(), 0);

        // Deregister immediately — blip timer should be cancelled
        receptionist.deregister("counter/blip");

        // Wait for the blip window to pass
        tokio::time::sleep(Duration::from_millis(150)).await;

        // OpLog should have only the Remove, not the Register (blip avoided!)
        let ops = receptionist.oplog_snapshot();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0].op_type, OpType::Remove { .. }));

        // Now test a non-blip: actor that lives past the window
        let ep = receptionist.start(
            "counter/stable",
            CounterActor,
            CounterState {
                count: 42,
                name: "stable".to_string(),
            },
        );

        // Wait for blip window to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // This time the Register should be committed
        let ops = receptionist.oplog_snapshot();
        let register_count = ops
            .iter()
            .filter(|op| matches!(op.op_type, OpType::Register { .. }))
            .count();
        assert_eq!(register_count, 1);

        // And the actor should still work
        let result = ep.get_count().await.unwrap();
        assert_eq!(result, 42);
    }

    // =========================================================================
    // SUPERVISION & RESTART TESTS
    // =========================================================================

    // -------------------------------------------------------------------------
    // Test 12: Restart limit causes permanent death
    // -------------------------------------------------------------------------

    #[derive(Debug)]
    struct PanicActor;

    struct PanicActorState;

    impl Actor for PanicActor {
        type State = PanicActorState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PanicMsg;

    impl Message for PanicMsg {
        type Result = ();
    }

    impl RemoteMessage for PanicMsg {
        const TYPE_ID: &'static str = "test::PanicMsg";
    }

    impl Handler<PanicMsg> for PanicActor {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            _state: &mut PanicActorState,
            _msg: PanicMsg,
        ) {
            panic!("intentional panic for restart test");
        }
    }

    impl RemoteDispatch for PanicActor {
        fn dispatch_remote<'a>(
            &'a mut self,
            ctx: &'a ActorContext<Self>,
            state: &'a mut PanicActorState,
            message_type: &'a str,
            payload: &'a [u8],
        ) -> impl std::future::Future<Output = Result<Vec<u8>, DispatchError>> + Send + 'a {
            async move {
                match message_type {
                    "test::PanicMsg" => {
                        let (msg, _): (PanicMsg, _) =
                            bincode::serde::decode_from_slice(payload, bincode::config::standard())
                                .map_err(|e| DispatchError::DeserializeFailed(e.to_string()))?;
                        let result = <Self as Handler<PanicMsg>>::handle(self, ctx, state, msg);
                        bincode::serde::encode_to_vec(&result, bincode::config::standard())
                            .map_err(|e| DispatchError::SerializeFailed(e.to_string()))
                    }
                    other => Err(DispatchError::UnknownMessageType(other.to_string())),
                }
            }
        }
    }

    struct PanicFactory {
        create_count: Arc<AtomicUsize>,
    }

    impl ActorFactory for PanicFactory {
        type Actor = PanicActor;
        fn create(&mut self) -> (PanicActor, PanicActorState) {
            self.create_count.fetch_add(1, Ordering::SeqCst);
            (PanicActor, PanicActorState)
        }
    }

    #[tokio::test]
    async fn test_restart_limit_permanent_death() {
        let receptionist = Receptionist::new();
        let create_count = Arc::new(AtomicUsize::new(0));

        let factory = PanicFactory {
            create_count: create_count.clone(),
        };

        let endpoint = receptionist.start_with_config(
            "panic/test",
            factory,
            RestartConfig {
                policy: RestartPolicy::Permanent,
                max_restarts: 3,
                window: Duration::from_secs(60),
                backoff: BackoffConfig {
                    initial: Duration::from_millis(10),
                    max: Duration::from_millis(50),
                    multiplier: 2.0,
                },
            },
        );

        // Initial create = 1
        assert_eq!(create_count.load(Ordering::SeqCst), 1);

        // Send PanicMsg to trigger panics. Each panic triggers a restart.
        // After max_restarts (3) restarts + 1 original = 4 creates total,
        // the actor should be permanently dead.
        for _ in 0..4 {
            let _ = endpoint.send(PanicMsg).await;
            // Wait for the restart backoff + processing
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for the restart loop to finish processing
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Factory should have been called 4 times (1 initial + 3 restarts)
        assert_eq!(create_count.load(Ordering::SeqCst), 4);

        // Receptionist should no longer have the actor
        let lookup = receptionist.lookup::<PanicActor>("panic/test");
        assert!(
            lookup.is_none(),
            "actor should be gone after restart limit exceeded"
        );

        // Endpoint should fail
        let result = endpoint.send(PanicMsg).await;
        assert!(result.is_err(), "endpoint should fail for dead actor");
    }

    // -------------------------------------------------------------------------
    // Test 13: Router round-robin and broadcast
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_router_round_robin_and_broadcast() {
        let receptionist = Receptionist::new();

        // Start 3 CounterActor instances with different labels
        let ep1 = receptionist.start(
            "counter/rr-1",
            CounterActor,
            CounterState {
                count: 0,
                name: "rr-1".to_string(),
            },
        );
        let ep2 = receptionist.start(
            "counter/rr-2",
            CounterActor,
            CounterState {
                count: 0,
                name: "rr-2".to_string(),
            },
        );
        let ep3 = receptionist.start(
            "counter/rr-3",
            CounterActor,
            CounterState {
                count: 0,
                name: "rr-3".to_string(),
            },
        );

        // Create Router with RoundRobin strategy
        let router = Router::new(
            vec![ep1.clone(), ep2.clone(), ep3.clone()],
            RoutingStrategy::RoundRobin,
        );

        // Send 6 Increment messages through router — each actor should get 2
        for _ in 0..6 {
            router.send(Increment { amount: 1 }).await.unwrap();
        }

        // Verify via extension trait on each endpoint
        let c1 = ep1.get_count().await.unwrap();
        let c2 = ep2.get_count().await.unwrap();
        let c3 = ep3.get_count().await.unwrap();
        assert_eq!(c1, 2, "actor 1 should have received 2 increments");
        assert_eq!(c2, 2, "actor 2 should have received 2 increments");
        assert_eq!(c3, 2, "actor 3 should have received 2 increments");

        // Create second Router with Broadcast strategy, broadcast GetCount
        let broadcast_router = Router::new(
            vec![ep1.clone(), ep2.clone(), ep3.clone()],
            RoutingStrategy::Broadcast,
        );

        let results = broadcast_router.broadcast(GetCount).await;
        assert_eq!(results.len(), 3);
        let counts: Vec<i64> = results.into_iter().map(|r| r.unwrap()).collect();
        // All 3 actors should report their current count (2 each)
        assert_eq!(counts, vec![2, 2, 2]);
    }

    // -------------------------------------------------------------------------
    // Test 14: Graceful departure scenario
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_graceful_departure_scenario() {
        // Create two Receptionist instances (node-A and node-B)
        let node_a = Receptionist::with_node_id("node-A");
        let node_b = Receptionist::with_node_id("node-B");

        // Register actors on node-A
        let _ep_a1 = node_a.start(
            "counter/service-1",
            CounterActor,
            CounterState {
                count: 10,
                name: "service-1".to_string(),
            },
        );
        let _ep_a2 = node_a.start(
            "counter/service-2",
            CounterActor,
            CounterState {
                count: 20,
                name: "service-2".to_string(),
            },
        );

        // Sync node-A's ops to node-B via apply_ops
        let a_ops = node_a.ops_since(&node_b.version_vector());
        assert_eq!(a_ops.len(), 2);
        node_b.apply_ops(a_ops);

        // Also register node-A's actors as remote on node-B so lookup works
        let (wire_tx1, _wire_rx1) = tokio::sync::mpsc::unbounded_channel();
        let (wire_tx2, _wire_rx2) = tokio::sync::mpsc::unbounded_channel();
        node_b.register_remote_from_node::<CounterActor>(
            "counter/service-1",
            wire_tx1,
            ResponseRegistry::new(),
            "node-A",
        );
        node_b.register_remote_from_node::<CounterActor>(
            "counter/service-2",
            wire_tx2,
            ResponseRegistry::new(),
            "node-A",
        );

        // Verify actors exist on node-B
        assert!(node_b.lookup::<CounterActor>("counter/service-1").is_some());
        assert!(node_b.lookup::<CounterActor>("counter/service-2").is_some());

        // Simulate node-A departing: prune node-A on node-B
        node_b.prune_node("node-A");

        // Verify actors gone from node-B
        assert!(
            node_b.lookup::<CounterActor>("counter/service-1").is_none(),
            "service-1 should be gone from node-B after prune"
        );
        assert!(
            node_b.lookup::<CounterActor>("counter/service-2").is_none(),
            "service-2 should be gone from node-B after prune"
        );

        // Actors still present on node-A
        assert!(
            node_a.lookup::<CounterActor>("counter/service-1").is_some(),
            "service-1 should still exist on node-A"
        );
        assert!(
            node_a.lookup::<CounterActor>("counter/service-2").is_some(),
            "service-2 should still exist on node-A"
        );
    }

    // -------------------------------------------------------------------------
    // Test 15: ActorRef create and resolve
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_actor_ref_create_and_resolve() {
        let receptionist = Receptionist::new();

        // Start a CounterActor via receptionist
        let ep = receptionist.start(
            "counter/ref-test",
            CounterActor,
            CounterState {
                count: 42,
                name: "ref-test".to_string(),
            },
        );

        // Verify the actor is working
        let result = ep.get_count().await.unwrap();
        assert_eq!(result, 42);

        // Create ActorRef
        let actor_ref = ActorRef::<CounterActor>::new("counter/ref-test", "local");

        // Serialize to bincode and deserialize back
        let encoded = bincode::serde::encode_to_vec(&actor_ref, bincode::config::standard())
            .expect("serialization should succeed");
        let (decoded, _): (ActorRef<CounterActor>, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("deserialization should succeed");

        // Verify the deserialized ref has the correct fields
        assert_eq!(decoded.label, "counter/ref-test");
        assert_eq!(decoded.node_id, "local");

        // Resolve the ref through the receptionist
        let resolved_ep = decoded
            .resolve(&receptionist)
            .expect("resolve should return Some(endpoint)");

        // Send GetCount through resolved endpoint — should work
        let result = resolved_ep.get_count().await.unwrap();
        assert_eq!(result, 42, "resolved endpoint should access the same actor");

        // Also test that resolving a non-existent ref returns None
        let bad_ref = ActorRef::<CounterActor>::new("counter/nonexistent", "local");
        assert!(bad_ref.resolve(&receptionist).is_none());
    }

    // =========================================================================
    // NEW MACRO SYSTEM TESTS
    // =========================================================================

    // -------------------------------------------------------------------------
    // Test 16: Auto-generated message struct works with send()
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_auto_generated_message_struct() {
        let receptionist = Receptionist::new();
        let ep = receptionist.start(
            "counter/auto-gen",
            CounterActor,
            CounterState {
                count: 0,
                name: "auto-gen".to_string(),
            },
        );

        // Increment is an auto-generated struct from the handler params
        let result = ep.send(Increment { amount: 42 }).await.unwrap();
        assert_eq!(result, 42);

        // GetCount is an auto-generated unit struct
        let count = ep.send(GetCount).await.unwrap();
        assert_eq!(count, 42);

        // GetInfo is also auto-generated (unit struct returning complex type)
        let info = ep.send(GetInfo).await.unwrap();
        assert_eq!(
            info,
            CounterInfo {
                name: "auto-gen".to_string(),
                count: 42
            }
        );
    }

    // -------------------------------------------------------------------------
    // Test 17: Extension trait methods work
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_extension_trait_methods() {
        let receptionist = Receptionist::new();
        let ep = receptionist.start(
            "counter/ext",
            CounterActor,
            CounterState {
                count: 100,
                name: "ext-test".to_string(),
            },
        );

        // Extension trait: endpoint.increment(amount) instead of endpoint.send(Increment { amount })
        let result = ep.increment(5).await.unwrap();
        assert_eq!(result, 105);

        let result = ep.increment(-10).await.unwrap();
        assert_eq!(result, 95);

        // Unit struct handler via extension trait
        let count = ep.get_count().await.unwrap();
        assert_eq!(count, 95);

        let info = ep.get_info().await.unwrap();
        assert_eq!(info.name, "ext-test");
        assert_eq!(info.count, 95);
    }

    // -------------------------------------------------------------------------
    // Test 18: ctx.spawn() fire-and-forget
    // -------------------------------------------------------------------------
    #[derive(Debug)]
    struct SpawnTestActor;

    struct SpawnTestState {
        value: Arc<std::sync::atomic::AtomicI64>,
    }

    impl Actor for SpawnTestActor {
        type State = SpawnTestState;
    }

    #[handlers]
    impl SpawnTestActor {
        #[handler]
        fn trigger_spawn(&mut self, ctx: &ActorContext<Self>, state: &mut SpawnTestState) {
            let value = state.value.clone();
            ctx.spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                value.store(42, Ordering::SeqCst);
            });
        }

        #[handler]
        fn get_value(&mut self, _ctx: &ActorContext<Self>, state: &mut SpawnTestState) -> i64 {
            state.value.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn test_ctx_spawn_fire_and_forget() {
        let receptionist = Receptionist::new();
        let value = Arc::new(std::sync::atomic::AtomicI64::new(0));

        let ep = receptionist.start(
            "spawn/test",
            SpawnTestActor,
            SpawnTestState {
                value: value.clone(),
            },
        );

        // Trigger the spawn
        ep.trigger_spawn().await.unwrap();

        // Value should still be 0 (spawn is async, hasn't completed yet)
        assert_eq!(value.load(Ordering::SeqCst), 0);

        // Wait for the spawned task to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now it should be 42
        assert_eq!(value.load(Ordering::SeqCst), 42);
    }

    // -------------------------------------------------------------------------
    // Test 19: Async handler with .await inside
    // -------------------------------------------------------------------------
    #[derive(Debug)]
    struct AsyncActor;

    struct AsyncActorState {
        data: String,
    }

    impl Actor for AsyncActor {
        type State = AsyncActorState;
    }

    #[handlers]
    impl AsyncActor {
        #[handler]
        async fn fetch_delayed(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut AsyncActorState,
            delay_ms: u64,
        ) -> String {
            // This .await yields the tokio thread — the whole point of async handlers
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            format!("fetched: {}", state.data)
        }

        #[handler]
        fn get_data(&mut self, _ctx: &ActorContext<Self>, state: &mut AsyncActorState) -> String {
            state.data.clone()
        }

        #[handler]
        async fn set_data(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut AsyncActorState,
            new_data: String,
        ) {
            // Async handler that modifies state
            tokio::task::yield_now().await;
            state.data = new_data;
        }
    }

    #[tokio::test]
    async fn test_async_handler_with_await() {
        let receptionist = Receptionist::new();
        let ep = receptionist.start(
            "async/test",
            AsyncActor,
            AsyncActorState {
                data: "hello".to_string(),
            },
        );

        // Async handler that sleeps
        let result = ep.fetch_delayed(10).await.unwrap();
        assert_eq!(result, "fetched: hello");

        // Sync handler on same actor
        let data = ep.get_data().await.unwrap();
        assert_eq!(data, "hello");

        // Async handler that modifies state
        ep.set_data("world".to_string()).await.unwrap();
        let data = ep.get_data().await.unwrap();
        assert_eq!(data, "world");
    }

    // -------------------------------------------------------------------------
    // Test 20: Mixed sync/async handlers on same actor
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mixed_sync_async_handlers() {
        let receptionist = Receptionist::new();
        let ep = receptionist.start(
            "mixed/test",
            AsyncActor,
            AsyncActorState {
                data: "initial".to_string(),
            },
        );

        // Sync → async → sync → verify ordering preserved
        let d1 = ep.get_data().await.unwrap();
        assert_eq!(d1, "initial");

        ep.set_data("updated".to_string()).await.unwrap();
        let d2 = ep.get_data().await.unwrap();
        assert_eq!(d2, "updated");

        let result = ep.fetch_delayed(1).await.unwrap();
        assert_eq!(result, "fetched: updated");
    }

    // -------------------------------------------------------------------------
    // Test 21: Yield budget doesn't break message ordering
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_yield_budget_message_ordering() {
        let receptionist = Receptionist::new();
        let ep = receptionist.start(
            "counter/ordering",
            CounterActor,
            CounterState {
                count: 0,
                name: "ordering".to_string(),
            },
        );

        // Send 200 increments (exceeds 64-message yield budget)
        for i in 1..=200 {
            let result = ep.increment(1).await.unwrap();
            assert_eq!(result, i, "message {i} should produce count {i}");
        }

        let final_count = ep.get_count().await.unwrap();
        assert_eq!(final_count, 200);
    }

    // =========================================================================
    // Reply Control Tests (ctx.reply, ctx.reply_sender, ctx.forward)
    // =========================================================================

    // -- Test actors for reply control --

    /// An actor that demonstrates ctx.reply() for early reply.
    #[derive(Debug)]
    struct ReplyControlActor;

    struct ReplyControlState {
        side_effect_done: bool,
    }

    impl Actor for ReplyControlActor {
        type State = ReplyControlState;
    }

    // Messages for reply control tests
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ReplyEarly;
    impl Message for ReplyEarly {
        type Result = String;
    }
    impl RemoteMessage for ReplyEarly {
        const TYPE_ID: &'static str = "test::ReplyEarly";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DeferReply;
    impl Message for DeferReply {
        type Result = String;
    }
    impl RemoteMessage for DeferReply {
        const TYPE_ID: &'static str = "test::DeferReply";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DoubleReply;
    impl Message for DoubleReply {
        type Result = String;
    }
    impl RemoteMessage for DoubleReply {
        const TYPE_ID: &'static str = "test::DoubleReply";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct NormalReply;
    impl Message for NormalReply {
        type Result = String;
    }
    impl RemoteMessage for NormalReply {
        const TYPE_ID: &'static str = "test::NormalReply";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CheckSideEffect;
    impl Message for CheckSideEffect {
        type Result = bool;
    }
    impl RemoteMessage for CheckSideEffect {
        const TYPE_ID: &'static str = "test::CheckSideEffect";
    }

    impl Handler<ReplyEarly> for ReplyControlActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            state: &mut ReplyControlState,
            _msg: ReplyEarly,
        ) -> String {
            ctx.reply("early response".to_string());
            state.side_effect_done = true;
            String::new() // discarded
        }
    }

    impl Handler<DeferReply> for ReplyControlActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut ReplyControlState,
            _msg: DeferReply,
        ) -> String {
            let sender = ctx.reply_sender::<String>();
            ctx.spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                sender.send("deferred!".to_string());
            });
            String::new() // discarded
        }
    }

    impl Handler<DoubleReply> for ReplyControlActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            _state: &mut ReplyControlState,
            _msg: DoubleReply,
        ) -> String {
            let first = ctx.reply("first".to_string());
            let second = ctx.reply("second".to_string());
            assert!(first, "first reply should succeed");
            assert!(!second, "second reply should be no-op");
            String::new() // discarded
        }
    }

    impl Handler<NormalReply> for ReplyControlActor {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            _state: &mut ReplyControlState,
            _msg: NormalReply,
        ) -> String {
            "normal return value".to_string()
        }
    }

    impl Handler<CheckSideEffect> for ReplyControlActor {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut ReplyControlState,
            _msg: CheckSideEffect,
        ) -> bool {
            state.side_effect_done
        }
    }

    impl RemoteDispatch for ReplyControlActor {
        async fn dispatch_remote(
            &mut self,
            _ctx: &ActorContext<Self>,
            _state: &mut ReplyControlState,
            _message_type: &str,
            _payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            Err(DispatchError::UnknownMessageType(
                "not used in reply control tests".into(),
            ))
        }
    }

    // -- Forwarding target actor --

    #[derive(Debug)]
    struct ForwardTarget;

    struct ForwardTargetState {
        value: i64,
    }

    impl Actor for ForwardTarget {
        type State = ForwardTargetState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct AddAndReturn {
        amount: i64,
    }
    impl Message for AddAndReturn {
        type Result = i64;
    }
    impl RemoteMessage for AddAndReturn {
        const TYPE_ID: &'static str = "test::AddAndReturn";
    }

    impl Handler<AddAndReturn> for ForwardTarget {
        fn handle(
            &mut self,
            _ctx: &ActorContext<Self>,
            state: &mut ForwardTargetState,
            msg: AddAndReturn,
        ) -> i64 {
            state.value += msg.amount;
            state.value
        }
    }

    impl RemoteDispatch for ForwardTarget {
        async fn dispatch_remote(
            &mut self,
            _ctx: &ActorContext<Self>,
            _state: &mut ForwardTargetState,
            _message_type: &str,
            _payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            Err(DispatchError::UnknownMessageType(
                "not used in reply control tests".into(),
            ))
        }
    }

    // -- Forwarder actor that delegates to ForwardTarget --

    #[derive(Debug)]
    struct ForwarderActor;

    struct ForwarderState {
        target: Endpoint<ForwardTarget>,
    }

    impl Actor for ForwarderActor {
        type State = ForwarderState;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ForwardAdd {
        amount: i64,
    }
    impl Message for ForwardAdd {
        type Result = i64;
    }
    impl RemoteMessage for ForwardAdd {
        const TYPE_ID: &'static str = "test::ForwardAdd";
    }

    impl Handler<ForwardAdd> for ForwarderActor {
        fn handle(
            &mut self,
            ctx: &ActorContext<Self>,
            state: &mut ForwarderState,
            msg: ForwardAdd,
        ) -> i64 {
            ctx.forward(
                &state.target,
                AddAndReturn {
                    amount: msg.amount,
                },
            );
            0 // discarded — target's response goes to original caller
        }
    }

    impl RemoteDispatch for ForwarderActor {
        async fn dispatch_remote(
            &mut self,
            _ctx: &ActorContext<Self>,
            _state: &mut ForwarderState,
            _message_type: &str,
            _payload: &[u8],
        ) -> Result<Vec<u8>, DispatchError> {
            Err(DispatchError::UnknownMessageType(
                "not used in reply control tests".into(),
            ))
        }
    }

    // -------------------------------------------------------------------------
    // Test: ctx.reply() sends early response, handler continues
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_ctx_reply_early() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start(
            "reply-control/early",
            ReplyControlActor,
            ReplyControlState {
                side_effect_done: false,
            },
        );

        // reply() sends "early response" immediately
        let resp = ep.send(ReplyEarly).await.unwrap();
        assert_eq!(resp, "early response");

        // Side effect should have completed (handler continues after reply)
        let done = ep.send(CheckSideEffect).await.unwrap();
        assert!(done, "side effect should be done after handler completes");
    }

    // -------------------------------------------------------------------------
    // Test: ctx.reply_sender() extracts raw channel for deferred reply
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_ctx_reply_sender() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start(
            "reply-control/deferred",
            ReplyControlActor,
            ReplyControlState {
                side_effect_done: false,
            },
        );

        // Handler spawns a task that replies after 50ms
        let resp = ep.send(DeferReply).await.unwrap();
        assert_eq!(resp, "deferred!");
    }

    // -------------------------------------------------------------------------
    // Test: ctx.forward() delegates reply to another actor
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_ctx_forward_local() {
        let receptionist = Receptionist::new();

        // Start the target actor
        let target_ep = receptionist.start(
            "forward/target",
            ForwardTarget,
            ForwardTargetState { value: 100 },
        );

        // Start the forwarder with a reference to the target
        let forwarder_ep = receptionist.start(
            "forward/router",
            ForwarderActor,
            ForwarderState {
                target: target_ep.clone(),
            },
        );

        // Send to forwarder — it forwards to target, we get target's response
        let result = forwarder_ep.send(ForwardAdd { amount: 42 }).await.unwrap();
        assert_eq!(result, 142, "should get target's response (100 + 42)");

        // Target's state should be updated
        let target_val = target_ep.send(AddAndReturn { amount: 0 }).await.unwrap();
        assert_eq!(target_val, 142, "target's state should reflect the forwarded message");
    }

    // -------------------------------------------------------------------------
    // Test: default behavior unchanged — handlers that don't use ctx.reply()
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_reply_default_behavior() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start(
            "reply-control/normal",
            ReplyControlActor,
            ReplyControlState {
                side_effect_done: false,
            },
        );

        // Handler returns normally without using ctx.reply()
        let resp = ep.send(NormalReply).await.unwrap();
        assert_eq!(resp, "normal return value");
    }

    // -------------------------------------------------------------------------
    // Test: double reply — second ctx.reply() is a no-op
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_double_reply_ignored() {
        let receptionist = Receptionist::new();

        let ep = receptionist.start(
            "reply-control/double",
            ReplyControlActor,
            ReplyControlState {
                side_effect_done: false,
            },
        );

        // Handler calls reply() twice — first wins, second is no-op
        let resp = ep.send(DoubleReply).await.unwrap();
        assert_eq!(resp, "first", "first reply should win");
    }

    // =========================================================================
    // Pool Router Tests (reactive router with WatchedListing)
    // =========================================================================

    use murmer::{ListingEvent, PoolRouter};

    // -------------------------------------------------------------------------
    // Test: PoolRouter auto-populates from checked-in actors
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_pool_router_basic_routing() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("pool-workers");

        // Start actors and check them in
        let _ep1 = receptionist.start(
            "pool/w1",
            CounterActor,
            CounterState { count: 0, name: "w1".into() },
        );
        receptionist.check_in("pool/w1", worker_key.clone());

        let _ep2 = receptionist.start(
            "pool/w2",
            CounterActor,
            CounterState { count: 0, name: "w2".into() },
        );
        receptionist.check_in("pool/w2", worker_key.clone());

        // Create pool router — should backfill with both workers
        let pool = PoolRouter::new(&receptionist, worker_key, RoutingStrategy::RoundRobin);

        // Give the background task a moment to process backfill
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(pool.len(), 2, "pool should have 2 workers");

        // Round-robin send — both workers should get messages
        pool.send(Increment { amount: 10 }).await.unwrap();
        pool.send(Increment { amount: 10 }).await.unwrap();

        // Verify both workers received a message
        let ep1 = receptionist.lookup::<CounterActor>("pool/w1").unwrap();
        let ep2 = receptionist.lookup::<CounterActor>("pool/w2").unwrap();
        let c1 = ep1.get_count().await.unwrap();
        let c2 = ep2.get_count().await.unwrap();
        assert_eq!(c1 + c2, 20, "total increments should be 20");
        assert!(c1 > 0 && c2 > 0, "round-robin should hit both workers");
    }

    // -------------------------------------------------------------------------
    // Test: PoolRouter tracks new actors added after creation
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_pool_router_dynamic_add() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("dynamic-workers");

        // Create pool router first — empty
        let pool = PoolRouter::new(&receptionist, worker_key.clone(), RoutingStrategy::RoundRobin);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(pool.len(), 0, "pool should start empty");

        // Now start an actor and check it in
        let _ep = receptionist.start(
            "dynamic/w1",
            CounterActor,
            CounterState { count: 0, name: "w1".into() },
        );
        receptionist.check_in("dynamic/w1", worker_key.clone());

        // Give the background watcher time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(pool.len(), 1, "pool should now have 1 worker");

        let result = pool.send(Increment { amount: 42 }).await.unwrap();
        assert_eq!(result, 42);
    }

    // -------------------------------------------------------------------------
    // Test: PoolRouter removes actors that deregister
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_pool_router_removal_on_stop() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("removal-workers");

        // Start two workers
        let _ep1 = receptionist.start(
            "removal/w1",
            CounterActor,
            CounterState { count: 0, name: "w1".into() },
        );
        receptionist.check_in("removal/w1", worker_key.clone());

        let _ep2 = receptionist.start(
            "removal/w2",
            CounterActor,
            CounterState { count: 0, name: "w2".into() },
        );
        receptionist.check_in("removal/w2", worker_key.clone());

        let pool = PoolRouter::new(&receptionist, worker_key, RoutingStrategy::RoundRobin);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pool.len(), 2);

        // Stop one worker
        receptionist.stop("removal/w1");
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(pool.len(), 1, "pool should shrink to 1 after stop");
        assert_eq!(pool.labels(), vec!["removal/w2".to_string()]);

        // Remaining worker still works
        let result = pool.send(Increment { amount: 7 }).await.unwrap();
        assert_eq!(result, 7);
    }

    // -------------------------------------------------------------------------
    // Test: PoolRouter broadcast sends to all pool members
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_pool_router_broadcast() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("broadcast-workers");

        for i in 0..3 {
            let label = format!("broadcast/w{i}");
            receptionist.start(
                &label,
                CounterActor,
                CounterState { count: 0, name: format!("w{i}") },
            );
            receptionist.check_in(&label, worker_key.clone());
        }

        let pool = PoolRouter::new(&receptionist, worker_key, RoutingStrategy::RoundRobin);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pool.len(), 3);

        let results = pool.broadcast(Increment { amount: 5 }).await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(*r.as_ref().unwrap(), 5);
        }
    }

    // -------------------------------------------------------------------------
    // Test: WatchedListing streams both adds and removes
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_watched_listing_events() {
        let receptionist = Receptionist::new();
        let worker_key = ReceptionKey::<CounterActor>::new("watched-workers");

        // Start an actor first
        receptionist.start(
            "watched/w1",
            CounterActor,
            CounterState { count: 0, name: "w1".into() },
        );
        receptionist.check_in("watched/w1", worker_key.clone());

        // Subscribe — should get backfill
        let mut watched = receptionist.watched_listing(worker_key.clone());

        let event = watched.next().await.unwrap();
        match event {
            ListingEvent::Added { label, .. } => assert_eq!(label, "watched/w1"),
            ListingEvent::Removed { .. } => panic!("expected Added event"),
        }

        // Add another
        receptionist.start(
            "watched/w2",
            CounterActor,
            CounterState { count: 0, name: "w2".into() },
        );
        receptionist.check_in("watched/w2", worker_key.clone());

        let event = watched.next().await.unwrap();
        match event {
            ListingEvent::Added { label, .. } => assert_eq!(label, "watched/w2"),
            ListingEvent::Removed { .. } => panic!("expected Added event"),
        }

        // Stop one — should get Removed
        receptionist.stop("watched/w1");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let event = watched.try_next().unwrap();
        match event {
            ListingEvent::Removed { label } => assert_eq!(label, "watched/w1"),
            ListingEvent::Added { .. } => panic!("expected Removed event"),
        }
    }
}
