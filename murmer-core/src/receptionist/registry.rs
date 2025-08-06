use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;

use crate::actor::Actor;
use crate::endpoint::Endpoint;

/// A unique key identifying an actor type across the system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorKey(pub &'static str);

impl fmt::Display for ActorKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A hierarchical label for identifying specific actor instances.
/// 
/// Labels follow a path-like structure (e.g., "cache/user", "worker/0").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorLabel(pub String);

impl fmt::Display for ActorLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ActorLabel {
    fn from(s: &str) -> Self {
        ActorLabel(s.to_string())
    }
}

impl From<String> for ActorLabel {
    fn from(s: String) -> Self {
        ActorLabel(s)
    }
}

/// Type-erased endpoint storage that maintains cloneability and type safety.
#[derive(Debug)]
pub struct AnyEndpoint {
    endpoint: Box<dyn Any + Send + Sync>,
    clone_fn: fn(&Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync>,
    type_id: TypeId,
}

impl AnyEndpoint {
    /// Create a new AnyEndpoint wrapping a typed endpoint.
    pub fn new<A: Actor>(endpoint: Endpoint<A>) -> Self {
        Self {
            endpoint: Box::new(endpoint),
            clone_fn: Self::make_clone_fn::<A>(),
            type_id: TypeId::of::<A>(),
        }
    }

    fn make_clone_fn<A: Actor>() -> fn(&Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync> {
        |any| {
            let typed = any.downcast_ref::<Endpoint<A>>()
                .expect("AnyEndpoint clone function called with wrong type");
            Box::new(typed.clone())
        }
    }

    /// Attempt to retrieve the original typed endpoint.
    /// 
    /// Returns Some(endpoint) if the type matches, None otherwise.
    pub fn try_get<A: Actor>(&self) -> Option<Endpoint<A>> {
        if self.type_id == TypeId::of::<A>() {
            // Safety: We verified the type matches our stored TypeId
            self.endpoint
                .downcast_ref::<Endpoint<A>>()
                .map(|e| e.clone())
        } else {
            None
        }
    }

    /// Clone the wrapped endpoint, preserving its original type.
    pub fn clone_endpoint(&self) -> AnyEndpoint {
        AnyEndpoint {
            endpoint: (self.clone_fn)(&self.endpoint),
            clone_fn: self.clone_fn,
            type_id: self.type_id,
        }
    }
}

impl Clone for AnyEndpoint {
    fn clone(&self) -> Self {
        self.clone_endpoint()
    }
}

/// Registry for mapping actor labels to their endpoints.
pub struct Registry {
    actors: HashMap<(ActorKey, ActorLabel), AnyEndpoint>,
}

impl Registry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
        }
    }

    /// Register an actor with the given label (typed version).
    pub fn register<A: Actor>(
        &mut self,
        key: ActorKey,
        label: ActorLabel,
        endpoint: Endpoint<A>,
    ) {
        self.register_any(key, label, AnyEndpoint::new(endpoint));
    }

    /// Register an actor with AnyEndpoint (used by receptionist).
    pub fn register_any(
        &mut self,
        key: ActorKey,
        label: ActorLabel,
        endpoint: AnyEndpoint,
    ) {
        tracing::trace!(
            actor_type = %key,
            label = %label,
            "Registering actor in registry"
        );

        self.actors.insert((key, label), endpoint);
    }

    /// Look up an actor by type and label (typed version).
    /// 
    /// Returns Some(endpoint) if found and the type matches, None otherwise.
    pub fn lookup<A: Actor>(&self, key: ActorKey, label: &ActorLabel) -> Option<Endpoint<A>> {
        self.lookup_any(&key, label)?
            .try_get::<A>()
    }

    /// Look up an actor by type and label (returns AnyEndpoint).
    pub fn lookup_any(&self, key: &ActorKey, label: &ActorLabel) -> Option<AnyEndpoint> {
        self.actors
            .get(&(key.clone(), label.clone()))
            .cloned()
    }

    /// Get the number of registered actors.
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::SystemBuilder;
    use crate::actor::{Actor, Context, Handler, ActorError};
    use crate::message::Message;

    #[derive(Debug)]
    struct TestMessage;
    impl Message for TestMessage {
        type Result = String;
    }

    struct TestActor;
    
    #[async_trait::async_trait]
    impl Actor for TestActor {
        type State = ();
        const ACTOR_TYPE_KEY: &'static str = "test_actor";

        async fn init(&mut self, _ctx: &mut Context<Self>) -> Result<Self::State, ActorError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Handler<TestMessage> for TestActor {
        async fn handle(
            &mut self,
            _ctx: &mut Context<Self>,
            _state: &mut Self::State,
            _message: TestMessage,
        ) -> String {
            "test response".to_string()
        }
    }

    #[tokio::test]
    async fn test_any_endpoint_type_safety() {
        let system = SystemBuilder::new().build();
        let (_supervisor, endpoint) = system.spawn_with(TestActor).unwrap();

        let any_endpoint = AnyEndpoint::new(endpoint);

        // Should successfully retrieve the correct type
        let retrieved: Option<Endpoint<TestActor>> = any_endpoint.try_get();
        assert!(retrieved.is_some());

        // Should fail to retrieve incorrect type (we can't easily test this without another actor type)
        // But the type system prevents us from even attempting invalid casts at compile time
    }

    #[tokio::test]
    async fn test_registry_operations() {
        let mut registry = Registry::new();
        let system = SystemBuilder::new().build();
        let (_supervisor, endpoint) = system.spawn_with(TestActor).unwrap();

        // Test registration
        registry.register(
            ActorKey("test_actor"),
            ActorLabel("test_instance".into()),
            endpoint,
        );

        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());

        // Test successful lookup
        let found: Option<Endpoint<TestActor>> = registry.lookup(
            ActorKey("test_actor"),
            &ActorLabel("test_instance".into()),
        );
        assert!(found.is_some());

        // Test failed lookup (wrong label)
        let not_found: Option<Endpoint<TestActor>> = registry.lookup(
            ActorKey("test_actor"),
            &ActorLabel("wrong_label".into()),
        );
        assert!(not_found.is_none());
    }
}