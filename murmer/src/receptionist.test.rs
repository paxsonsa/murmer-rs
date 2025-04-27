use std::sync::Arc;

use futures::StreamExt;
use parking_lot::Mutex;

use super::*;
use crate::actor::Registered as RegisteredActor;
use crate::message::RemoteMessageError;
use crate::prelude::*;
use crate::system::EndpointSender;

struct FakeActor;

impl RegisteredActor for FakeActor {
    const RECEPTIONIST_KEY: &'static str = "fake";
}

#[async_trait::async_trait]
impl Handler<RemoteMessage> for FakeActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        _message: RemoteMessage,
    ) -> Result<RemoteMessage, RemoteMessageError> {
        panic!("RemoteMessage is not supported for FakeActor");
    }
}

impl Actor for FakeActor {}

/// Helper function to create a simple endpoint for a FakeActor
fn create_endpoint(name: &str) -> Endpoint<FakeActor> {
    let path = Arc::new(ActorPath::local(name.to_string(), Id::new()));
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let sender = EndpointSender::<FakeActor>::from_channel(tx);
    Endpoint::new(sender, path)
}

/// Helper function to create a direct endpoint for a FakeActor
fn create_direct_endpoint(name: &str) -> Endpoint<FakeActor> {
    let path = Arc::new(ActorPath::local(name.to_string(), Id::new()));
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let sender = EndpointSender::<FakeActor>::from_channel(tx);
    let endpoint = Endpoint::new(sender, path.clone());
    let context = Context::new(endpoint, tokio_util::sync::CancellationToken::new());
    let actor = Arc::new(Mutex::new(FakeActor));
    let context = Arc::new(Mutex::new(context));
    Endpoint::direct(path, actor, context)
}

/// Helper function to create a receptionist context
fn create_receptionist_context() -> Context<ReceptionistActor> {
    let path = Arc::new(ActorPath::local("receptionist".to_string(), Id::new()));
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let sender = EndpointSender::from_channel(tx);
    let endpoint = Endpoint::new(sender, path);
    Context::new(endpoint, tokio_util::sync::CancellationToken::new())
}

/// Helper function to register an endpoint with a receptionist
async fn register_endpoint<T: RegisteredActor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    key: &Key<T>,
    endpoint: &Endpoint<T>,
) {
    let message = Register {
        key: key.clone().into(),
        endpoint: endpoint.clone(),
    };
    assert!(receptionist.handle(ctx, message).await);
}

/// Helper function to deregister an endpoint from a receptionist
async fn deregister_endpoint<T: RegisteredActor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    key: &Key<T>,
    endpoint: &Endpoint<T>,
) {
    let message = Deregister {
        key: key.into(),
        endpoint: endpoint.clone(),
    };
    let _ = receptionist.handle(ctx, message).await;
}

/// Helper function to lookup endpoints for a key
async fn lookup_endpoints<T: RegisteredActor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    key: &Key<T>,
) -> Listing<T> {
    let message = Lookup {
        key: key.into(),
        _phantom: PhantomData,
    };
    receptionist
        .handle(ctx, message)
        .await
        .expect("lookup failed")
}

/// Helper function to subscribe to a key
async fn subscribe_to_key<T: RegisteredActor>(
    receptionist: &mut ReceptionistActor,
    ctx: &mut Context<ReceptionistActor>,
    key: &Key<T>,
) -> ListingSubscription<T> {
    let message = Subscribe {
        key: key.into(),
        _phantom: PhantomData,
    };
    receptionist
        .handle(ctx, message)
        .await
        .expect("subscribe failed")
}

/// Helper function to assert a registered update
fn assert_registered_update<T: RegisteredActor>(
    listing: &mut ListingSubscription<T>,
    expected_endpoint: &Endpoint<T>,
) {
    let update = listing.some_next().expect("update was not received");
    let ListingUpdate::Registered(recv_endpoint) = update else {
        panic!("unexpected update type");
    };
    assert_eq!(recv_endpoint, *expected_endpoint);
}

#[tokio::test]
async fn test_receptionist_lifecycle() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    let endpoint = create_endpoint("testA");
    let key = Key::<FakeActor>::new("actor");

    // Register the endpoint
    register_endpoint(&mut receptionist, &mut ctx, &key, &endpoint).await;

    // Lookup should return the registered endpoint
    let result = lookup_endpoints(&mut receptionist, &mut ctx, &key).await;
    assert_eq!(result.endpoints.len(), 1);
    assert_eq!(result.endpoints[0], endpoint);

    // Deregister the endpoint
    deregister_endpoint(&mut receptionist, &mut ctx, &key, &endpoint).await;

    // Lookup should return empty list
    let result = lookup_endpoints(&mut receptionist, &mut ctx, &key).await;
    assert_eq!(result.endpoints.len(), 0);
}

#[tokio::test]
async fn test_receptionist_subscription() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    let key = Key::<FakeActor>::new("actor");

    // Create and register first endpoint
    let endpoint1 = create_direct_endpoint("test1");
    register_endpoint(&mut receptionist, &mut ctx, &key, &endpoint1).await;

    // Subscribe to the key
    let mut listing = subscribe_to_key(&mut receptionist, &mut ctx, &key).await;

    // Assert we get a registered update for the previously registered actor
    assert_registered_update(&mut listing, &endpoint1);

    // Create and register second endpoint
    let endpoint2 = create_direct_endpoint("test2");
    register_endpoint(&mut receptionist, &mut ctx, &key, &endpoint2).await;

    // Assert we get a registered update for the newly registered actor
    assert_registered_update(&mut listing, &endpoint2);
}

#[tokio::test]
async fn test_receptionist_subscription_stream() {
    let mut receptionist = ReceptionistActor::default();
    let mut ctx = create_receptionist_context();
    let key = Key::<FakeActor>::new("fake");

    // Create and register first endpoint
    let endpoint_a = create_endpoint("testA");
    register_endpoint(&mut receptionist, &mut ctx, &key, &endpoint_a).await;

    // Subscribe to the key
    let mut listing = subscribe_to_key(&mut receptionist, &mut ctx, &key).await;

    // Create and register second endpoint
    let endpoint_b = create_endpoint("testB");
    register_endpoint(&mut receptionist, &mut ctx, &key, &endpoint_b).await;

    // Deregister the second endpoint
    deregister_endpoint(&mut receptionist, &mut ctx, &key, &endpoint_b).await;

    // Read the stream and expect to receive the registered and deregistered updates in order
    // First registered update
    let Some(ListingUpdate::Registered(recv_endpoint)) = listing.next().await else {
        panic!("expected registered update");
    };
    assert_eq!(recv_endpoint, endpoint_a);

    // Second registered update
    let Some(ListingUpdate::Registered(recv_endpoint)) = listing.next().await else {
        panic!("expected registered update");
    };
    assert_eq!(recv_endpoint, endpoint_b);

    // Last deregistered update
    let Some(ListingUpdate::Deregistered(recv_endpoint)) = listing.next().await else {
        panic!("expected deregistered update");
    };
    assert_eq!(recv_endpoint, endpoint_b);
}
