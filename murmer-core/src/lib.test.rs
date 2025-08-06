use crate::*;

#[derive(Debug)]
struct PingMessage;

impl Message for PingMessage {
    type Result = String;
}

#[derive(Debug)]
struct PingActor {
    name: String,
}

impl PingActor {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl Actor for PingActor {
    type State = ();
    const ACTOR_TYPE_KEY: &'static str = "ping_actor";

    async fn init(&mut self, _ctx: &mut Context<Self>) -> Result<Self::State, ActorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<PingMessage> for PingActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        _state: &mut Self::State,
        _message: PingMessage,
    ) -> String {
        format!("Pong from {}", self.name)
    }
}

#[tokio::test]
async fn test_receptionist_register_and_lookup() {
    // Create system with built-in receptionist
    let system = SystemBuilder::new().build();
    let receptionist = system.receptionist();

    // Create a test actor
    let ping_actor = PingActor::new("test_actor");
    let (_ping_supervisor, ping_endpoint) = system.spawn_with(ping_actor).unwrap();

    // Register the actor with the receptionist
    receptionist
        .register(ping_endpoint, "test/ping")
        .await
        .expect("Failed to register actor");

    // Look up the actor
    let found_endpoint = receptionist
        .lookup::<PingActor>("test/ping")
        .await
        .expect("Failed to lookup actor");

    // Verify we found the actor
    assert!(found_endpoint.is_some(), "Actor should be found in registry");

    // Test the found endpoint works
    let endpoint = found_endpoint.unwrap();
    let response = endpoint
        .send(PingMessage)
        .await
        .expect("Failed to send ping message");

    assert_eq!(response, "Pong from test_actor");
}

#[tokio::test]
async fn test_receptionist_lookup_nonexistent() {
    // Create system with built-in receptionist
    let system = SystemBuilder::new().build();
    let receptionist = system.receptionist();

    // Try to look up a nonexistent actor
    let found_endpoint = receptionist
        .lookup::<PingActor>("nonexistent/actor")
        .await
        .expect("Failed to lookup actor");

    // Verify the actor was not found
    assert!(found_endpoint.is_none(), "Nonexistent actor should not be found");
}

#[tokio::test]
async fn test_receptionist_multiple_actors() {
    // Create system with built-in receptionist
    let system = SystemBuilder::new().build();
    let receptionist = system.receptionist();

    // Create multiple test actors
    let actor1 = PingActor::new("actor1");
    let actor2 = PingActor::new("actor2");
    let (_supervisor1, endpoint1) = system.spawn_with(actor1).unwrap();
    let (_supervisor2, endpoint2) = system.spawn_with(actor2).unwrap();

    // Register both actors
    receptionist
        .register(endpoint1, "group/actor1")
        .await
        .expect("Failed to register actor1");

    receptionist
        .register(endpoint2, "group/actor2")
        .await
        .expect("Failed to register actor2");

    // Look up both actors
    let found1 = receptionist
        .lookup::<PingActor>("group/actor1")
        .await
        .expect("Failed to lookup actor1");
    let found2 = receptionist
        .lookup::<PingActor>("group/actor2")
        .await
        .expect("Failed to lookup actor2");

    // Verify both actors were found
    assert!(found1.is_some(), "Actor1 should be found");
    assert!(found2.is_some(), "Actor2 should be found");

    // Test both endpoints work and return different responses
    let response1 = found1
        .unwrap()
        .send(PingMessage)
        .await
        .expect("Failed to ping actor1");
    let response2 = found2
        .unwrap()
        .send(PingMessage)
        .await
        .expect("Failed to ping actor2");

    assert_eq!(response1, "Pong from actor1");
    assert_eq!(response2, "Pong from actor2");
}

#[tokio::test]
async fn test_receptionist_cloning() {
    // Test that the receptionist client can be cloned and shared
    let system = SystemBuilder::new().build();
    let receptionist = system.receptionist();
    let receptionist_clone = receptionist.clone();
    
    // Create and register an actor with the original
    let ping_actor = PingActor::new("clone_test");
    let (_supervisor, endpoint) = system.spawn_with(ping_actor).unwrap();
    
    receptionist
        .register(endpoint, "clone/test")
        .await
        .expect("Failed to register with original");
    
    // Look up with the clone
    let found = receptionist_clone
        .lookup::<PingActor>("clone/test")
        .await
        .expect("Failed to lookup with clone");
    
    assert!(found.is_some(), "Actor should be found via clone");
    
    // Test the endpoint works
    let response = found
        .unwrap()
        .send(PingMessage)
        .await
        .expect("Failed to send ping message");
    
    assert_eq!(response, "Pong from clone_test");
}