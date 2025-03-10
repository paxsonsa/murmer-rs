use std::sync::Arc;

use futures::StreamExt;
use parking_lot::Mutex;

use super::*;
use crate::prelude::*;

struct FakeActor;

impl Actor for FakeActor {}

#[tokio::test]
async fn test_receptionist_lifecycle() {
    let mut receptionist = ReceptionistActor::default();
    let actor = FakeActor;
    let context = Context::new();

    let path = Arc::new(ActorPath::local("test".to_string(), Id::new()));
    let actor = Arc::new(Mutex::new(actor));
    let context = Arc::new(Mutex::new(context));

    let endpoint = Endpoint::direct(path, actor, context);

    let key = Key::new("actor");
    let message = Register {
        key: key.clone(),
        endpoint: endpoint.clone(),
    };

    let mut ctx = Context::new();
    let _ = receptionist.handle(&mut ctx, message);

    let message = Lookup { key: key.clone() };
    let result = receptionist
        .handle(&mut ctx, message)
        .expect("lookup failed");

    assert_eq!(result.endpoints.len(), 1);
    assert_eq!(result.endpoints[0], endpoint);

    let message = Deregister {
        key: key.clone(),
        endpoint,
    };
    let _ = receptionist.handle(&mut ctx, message);

    let message = Lookup { key };
    let result = receptionist
        .handle(&mut ctx, message)
        .expect("lookup failed");

    assert_eq!(result.endpoints.len(), 0);
}

#[tokio::test]
async fn test_receptionist_subscription() {
    let mut receptionist = ReceptionistActor::default();
    let actor = FakeActor;
    let context = Context::new();

    let path = Arc::new(ActorPath::local("test".to_string(), Id::new()));
    let actor = Arc::new(Mutex::new(actor));
    let context = Arc::new(Mutex::new(context));

    let endpoint = Endpoint::direct(path, actor, context);

    // Register an existing actor to the receptionist
    let key = Key::new("actor");
    let message = Register {
        key: key.clone(),
        endpoint: endpoint.clone(),
    };

    let mut ctx = Context::new();
    let _ = receptionist.handle(&mut ctx, message);

    // Create a subscription to the key
    let message = Subscribe { key: key.clone() };

    let mut ctx = Context::new();
    let mut listing = receptionist
        .handle(&mut ctx, message)
        .expect("subscribe failed");

    // Assert we get a registered for the previously registered actor
    let update = listing.some_next().expect("update was not received");
    let ListingUpdate::Registered(recv_endpoint) = update else {
        panic!("unexpected update type");
    };
    assert_eq!(recv_endpoint, endpoint);

    // Register another actor to the receptionist
    let actor = FakeActor;
    let context = Context::new();

    let path = Arc::new(ActorPath::local("test".to_string(), Id::new()));
    let actor = Arc::new(Mutex::new(actor));
    let context = Arc::new(Mutex::new(context));

    let endpoint = Endpoint::direct(path, actor, context);

    // Register an existing actor to the receptionist
    let key = Key::new("actor");
    let message = Register {
        key: key.clone(),
        endpoint: endpoint.clone(),
    };
    let mut ctx = Context::new();
    let _ = receptionist.handle(&mut ctx, message);

    // Expect to receive the next registered update
    let update = listing.some_next().expect("update was not received");
    let ListingUpdate::Registered(recv_endpoint) = update else {
        panic!("unexpected update type");
    };
    assert_eq!(recv_endpoint, endpoint);
}

#[tokio::test]
async fn test_receptionist_subscription_stream() {
    let mut receptionist = ReceptionistActor::default();
    let actor = FakeActor;
    let context = Context::new();

    let path = Arc::new(ActorPath::local("test".to_string(), Id::new()));
    let actor = Arc::new(Mutex::new(actor));
    let context = Arc::new(Mutex::new(context));

    let endpoint_a = Endpoint::direct(path, actor, context);

    // Register an existing actor to the receptionist
    let key = Key::new("actor");
    let message = Register {
        key: key.clone(),
        endpoint: endpoint_a.clone(),
    };
    let mut ctx = Context::new();
    let _ = receptionist.handle(&mut ctx, message);

    // Create a subscription to the key
    let message = Subscribe { key: key.clone() };

    let mut ctx = Context::new();
    let mut listing = receptionist
        .handle(&mut ctx, message)
        .expect("subscribe failed");

    // Register another actor to the receptionist
    let actor = FakeActor;
    let context = Context::new();

    let path = Arc::new(ActorPath::local("test".to_string(), Id::new()));
    let actor = Arc::new(Mutex::new(actor));
    let context = Arc::new(Mutex::new(context));

    let endpoint_b = Endpoint::direct(path, actor, context);

    // Register an existing actor to the receptionist
    let key = Key::new("actor");
    let message = Register {
        key: key.clone(),
        endpoint: endpoint_b.clone(),
    };
    let mut ctx = Context::new();
    let _ = receptionist.handle(&mut ctx, message);

    let message = Deregister {
        key: key.clone(),
        endpoint: endpoint_b.clone(),
    };
    let mut ctx = Context::new();
    receptionist
        .handle(&mut ctx, message)
        .then_some(())
        .expect("deregister failed");

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
