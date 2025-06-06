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
