use std::sync::Arc;

use super::*;
use crate::prelude::*;
use crate::system::EndpointSender;

struct FakeActor;

impl Actor for FakeActor {
    const ACTOR_TYPE_KEY: &'static str = "fake";
}

/// Helper function to create a simple endpoint for a FakeActor
fn create_endpoint(name: &str) -> Endpoint<FakeActor> {
    let path = Arc::new(ActorPath::local(
        FakeActor::ACTOR_TYPE_KEY.to_string(),
        name.to_string(),
        Id::new(),
    ));
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let sender = EndpointSender::<FakeActor>::from_channel(tx);
    Endpoint::new(sender, path)
}

/// Helper function to create a receptionist context
fn create_receptionist_context() -> Context<ReceptionistActor> {
    let path = Arc::new(ActorPath::local_default(
        "receptionist".to_string(),
        Id::new(),
    ));
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let sender = EndpointSender::from_channel(tx);
    let endpoint = Endpoint::new(sender, path);
    Context::new(endpoint, tokio_util::sync::CancellationToken::new())
}

/// Helper function to register an endpoint with a receptionist
async fn register_endpoint<T: Actor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    endpoint: &Endpoint<T>,
) {
    let message = RegisterMessage::local(
        endpoint.path().clone(),
        endpoint.clone().into(),
    );
    assert!(receptionist.handle(ctx, message).await);
}

/// Helper function to lookup endpoints by actor type key
async fn lookup_endpoints(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    actor_type_key: &str,
) -> Vec<AnyEndpoint> {
    let message = LookupMessage {
        actor_type_key: actor_type_key.to_string(),
    };
    receptionist.handle(ctx, message).await
}

/// Helper function to deregister an endpoint
async fn deregister_endpoint<T: Actor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    endpoint: &Endpoint<T>,
) {
    let message = DeregisterMessage {
        path: endpoint.path().clone(),
    };
    assert!(receptionist.handle(ctx, message).await);
}

#[tokio::test]
async fn test_receptionist_lifecycle() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    let endpoint = create_endpoint("testA");
    
    // Register the endpoint
    register_endpoint(&mut receptionist, &mut ctx, &endpoint).await;

    // Lookup should return the registered endpoint
    let result = lookup_endpoints(&mut receptionist, &mut ctx, FakeActor::ACTOR_TYPE_KEY).await;
    assert_eq!(result.len(), 1);

    // Deregister the endpoint
    deregister_endpoint(&mut receptionist, &mut ctx, &endpoint).await;

    // Lookup should return empty list
    let result = lookup_endpoints(&mut receptionist, &mut ctx, FakeActor::ACTOR_TYPE_KEY).await;
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn test_multiple_registrations() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    let endpoint1 = create_endpoint("testA");
    let endpoint2 = create_endpoint("testB");
    
    // Register both endpoints
    register_endpoint(&mut receptionist, &mut ctx, &endpoint1).await;
    register_endpoint(&mut receptionist, &mut ctx, &endpoint2).await;

    // Lookup should return both endpoints
    let result = lookup_endpoints(&mut receptionist, &mut ctx, FakeActor::ACTOR_TYPE_KEY).await;
    assert_eq!(result.len(), 2);

    // Deregister one endpoint
    deregister_endpoint(&mut receptionist, &mut ctx, &endpoint1).await;

    // Lookup should return only one endpoint
    let result = lookup_endpoints(&mut receptionist, &mut ctx, FakeActor::ACTOR_TYPE_KEY).await;
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_lazy_endpoint_creation() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create a test factory
    struct TestFactory {
        path: Arc<ActorPath>,
    }
    
    impl EndpointFactory for TestFactory {
        fn create(&self) -> AnyEndpoint {
            let (tx, _rx) = tokio::sync::mpsc::channel(1024);
            let sender = EndpointSender::<FakeActor>::from_channel(tx);
            let endpoint = Endpoint::new(sender, self.path.clone());
            endpoint.into()
        }
        
        fn clone_factory(&self) -> Box<dyn EndpointFactory> {
            Box::new(TestFactory {
                path: self.path.clone(),
            })
        }
    }
    
    let path = Arc::new(ActorPath::local(
        FakeActor::ACTOR_TYPE_KEY.to_string(),
        "remote_test".to_string(),
        Id::new(),
    ));
    let factory = Box::new(TestFactory { path: path.clone() });
    
    // Register with factory
    let message = RegisterMessage::remote(path.as_ref().clone(), factory);
    assert!(receptionist.handle(&mut ctx, message).await);

    // Lookup should create the endpoint and return it
    let result = lookup_endpoints(&mut receptionist, &mut ctx, FakeActor::ACTOR_TYPE_KEY).await;
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_subscription_basic_functionality() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create subscription channel
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    
    // Subscribe to all fake actors
    let filter = SubscriptionFilter {
        actor_type: FakeActor::ACTOR_TYPE_KEY.to_string(),
        group_pattern: None,
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Register an actor
    let endpoint = create_endpoint("test");
    register_endpoint(&mut receptionist, &mut ctx, &endpoint).await;
    
    // Should receive an Added event
    let event = rx.try_recv().expect("Should receive Added event");
    match event {
        RegistryEvent::Added(path, _) => {
            assert_eq!(path.type_id.as_ref(), FakeActor::ACTOR_TYPE_KEY);
        }
        _ => panic!("Expected Added event"),
    }
    
    // Deregister the actor
    deregister_endpoint(&mut receptionist, &mut ctx, &endpoint).await;
    
    // Should receive a Removed event
    let event = rx.try_recv().expect("Should receive Removed event");
    match event {
        RegistryEvent::Removed(path) => {
            assert_eq!(path.type_id.as_ref(), FakeActor::ACTOR_TYPE_KEY);
        }
        _ => panic!("Expected Removed event"),
    }
}

#[tokio::test]
async fn test_subscription_type_filtering() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create another actor type for testing
    struct OtherActor;
    impl Actor for OtherActor {
        const ACTOR_TYPE_KEY: &'static str = "other";
    }
    
    // Create subscription for only FakeActor
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let filter = SubscriptionFilter {
        actor_type: FakeActor::ACTOR_TYPE_KEY.to_string(),
        group_pattern: None,
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Register a FakeActor
    let fake_endpoint = create_endpoint("fake");
    register_endpoint(&mut receptionist, &mut ctx, &fake_endpoint).await;
    
    // Should receive event for FakeActor
    assert!(rx.try_recv().is_ok());
    
    // Register an "other" actor manually
    let other_path = ActorPath::local(
        OtherActor::ACTOR_TYPE_KEY.to_string(),
        "default".to_string(),
        Id::new(),
    );
    let (tx_other, _rx_other) = tokio::sync::mpsc::channel(1024);
    let sender_other = EndpointSender::<OtherActor>::from_channel(tx_other);
    let other_endpoint = Endpoint::new(sender_other, Arc::new(other_path));
    register_endpoint(&mut receptionist, &mut ctx, &other_endpoint).await;
    
    // Should NOT receive event for OtherActor
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_subscription_group_filtering() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create subscription for FakeActors in "region*" groups
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let filter = SubscriptionFilter {
        actor_type: FakeActor::ACTOR_TYPE_KEY.to_string(),
        group_pattern: Some("region*".to_string()),
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Create actors in different groups
    let region1_path = ActorPath::local(
        FakeActor::ACTOR_TYPE_KEY.to_string(),
        "region1".to_string(),
        Id::new(),
    );
    let region2_path = ActorPath::local(
        FakeActor::ACTOR_TYPE_KEY.to_string(),
        "region2".to_string(),
        Id::new(),
    );
    let default_path = ActorPath::local(
        FakeActor::ACTOR_TYPE_KEY.to_string(),
        "default".to_string(),
        Id::new(),
    );
    
    // Register region1 actor
    let (tx1, _rx1) = tokio::sync::mpsc::channel(1024);
    let sender1 = EndpointSender::<FakeActor>::from_channel(tx1);
    let region1_endpoint = Endpoint::new(sender1, Arc::new(region1_path));
    register_endpoint(&mut receptionist, &mut ctx, &region1_endpoint).await;
    
    // Should receive event (matches "region*")
    assert!(rx.try_recv().is_ok());
    
    // Register region2 actor
    let (tx2, _rx2) = tokio::sync::mpsc::channel(1024);
    let sender2 = EndpointSender::<FakeActor>::from_channel(tx2);
    let region2_endpoint = Endpoint::new(sender2, Arc::new(region2_path));
    register_endpoint(&mut receptionist, &mut ctx, &region2_endpoint).await;
    
    // Should receive event (matches "region*")
    assert!(rx.try_recv().is_ok());
    
    // Register default actor
    let (tx3, _rx3) = tokio::sync::mpsc::channel(1024);
    let sender3 = EndpointSender::<FakeActor>::from_channel(tx3);
    let default_endpoint = Endpoint::new(sender3, Arc::new(default_path));
    register_endpoint(&mut receptionist, &mut ctx, &default_endpoint).await;
    
    // Should NOT receive event (doesn't match "region*")
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_subscription_wildcard_type() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create subscription for all types
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let filter = SubscriptionFilter {
        actor_type: "*".to_string(),
        group_pattern: None,
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Create another actor type
    struct TestActor;
    impl Actor for TestActor {
        const ACTOR_TYPE_KEY: &'static str = "test";
    }
    
    // Register FakeActor
    let fake_endpoint = create_endpoint("fake");
    register_endpoint(&mut receptionist, &mut ctx, &fake_endpoint).await;
    
    // Should receive event
    assert!(rx.try_recv().is_ok());
    
    // Register TestActor
    let test_path = ActorPath::local(
        TestActor::ACTOR_TYPE_KEY.to_string(),
        "default".to_string(),
        Id::new(),
    );
    let (tx_test, _rx_test) = tokio::sync::mpsc::channel(1024);
    let sender_test = EndpointSender::<TestActor>::from_channel(tx_test);
    let test_endpoint = Endpoint::new(sender_test, Arc::new(test_path));
    register_endpoint(&mut receptionist, &mut ctx, &test_endpoint).await;
    
    // Should also receive event (wildcard matches all)
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn test_subscription_invalid_glob_pattern() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create subscription with invalid glob pattern
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let filter = SubscriptionFilter {
        actor_type: FakeActor::ACTOR_TYPE_KEY.to_string(),
        group_pattern: Some("[invalid".to_string()), // Invalid glob pattern
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Register an actor
    let endpoint = create_endpoint("test");
    register_endpoint(&mut receptionist, &mut ctx, &endpoint).await;
    
    // Should NOT receive event due to invalid pattern
    assert!(rx.try_recv().is_err());
}

#[tokio::test] 
async fn test_subscription_cleanup_on_disconnect() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    
    // Create subscription and immediately drop receiver
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let filter = SubscriptionFilter {
        actor_type: FakeActor::ACTOR_TYPE_KEY.to_string(),
        group_pattern: None,
    };
    let subscribe_msg = SubscribeMessage {
        filter,
        sender: tx,
    };
    receptionist.handle(&mut ctx, subscribe_msg).await;
    
    // Drop the receiver to simulate disconnection
    drop(rx);
    
    // Verify subscription was added
    assert_eq!(receptionist.subscribers.len(), 1);
    
    // Register an actor (this should trigger cleanup)
    let endpoint = create_endpoint("test");
    register_endpoint(&mut receptionist, &mut ctx, &endpoint).await;
    
    // Subscription should be cleaned up
    assert_eq!(receptionist.subscribers.len(), 0);
}
