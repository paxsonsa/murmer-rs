mod messages;
pub mod registry;

use messages::{Lookup, Register};
use registry::Registry;
pub use registry::{ActorKey, ActorLabel, AnyEndpoint};

use crate::actor::{Actor, ActorError, Context, Handler};
use crate::endpoint::{Endpoint, EndpointError};

/// The receptionist actor that manages the registry internally.
pub(crate) struct ReceptionistActor {
    registry: Registry,
}

/// Client handle for interacting with the receptionist.
///
/// Provides a clean typed API for registering and looking up actors.
/// Can be cloned and shared across the system.
pub struct Receptionist {
    endpoint: Endpoint<ReceptionistActor>,
}

impl Clone for Receptionist {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
        }
    }
}

impl Receptionist {
    /// Create a new receptionist client from an actor endpoint.
    pub(crate) fn new(endpoint: Endpoint<ReceptionistActor>) -> Self {
        Self { endpoint }
    }

    /// Register an actor with the receptionist.
    pub async fn register<A: Actor>(
        &self,
        endpoint: Endpoint<A>,
        label: impl Into<ActorLabel>,
    ) -> Result<(), EndpointError> {
        let msg = Register::new::<A>(endpoint, label);
        self.endpoint.send(msg).await?
    }

    /// Look up an actor by type and label.
    pub async fn lookup<A: Actor>(
        &self,
        label: impl Into<ActorLabel>,
    ) -> Result<Option<Endpoint<A>>, EndpointError> {
        let msg = Lookup::new::<A>(label);
        let any_endpoint = self.endpoint.send(msg).await?;
        Ok(any_endpoint.and_then(|ep| ep.try_get::<A>()))
    }
}

impl Default for ReceptionistActor {
    fn default() -> Self {
        Self {
            registry: Registry::new(),
        }
    }
}

#[async_trait::async_trait]
impl Actor for ReceptionistActor {
    type State = ();
    const ACTOR_TYPE_KEY: &'static str = "system_receptionist";

    async fn init(&mut self, _ctx: &mut Context<Self>) -> Result<Self::State, ActorError> {
        tracing::info!("Receptionist starting");
        Ok(())
    }

    async fn started(&mut self, _ctx: &mut Context<Self>, _state: &mut Self::State) {
        tracing::info!("Receptionist started and ready");
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>, _state: ()) {
        tracing::info!("Receptionist stopped");
    }
}

#[async_trait::async_trait]
impl Handler<Register> for ReceptionistActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        _state: &mut Self::State,
        message: Register,
    ) -> Result<(), EndpointError> {
        tracing::debug!(
            actor_type = %message.key,
            label = %message.label,
            "Registering actor"
        );

        self.registry
            .register_any(message.key, message.label, message.endpoint);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<Lookup> for ReceptionistActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        _state: &mut Self::State,
        message: Lookup,
    ) -> Option<AnyEndpoint> {
        tracing::debug!(
            actor_type = %message.key,
            label = %message.label,
            "Looking up actor"
        );

        let result = self.registry.lookup_any(&message.key, &message.label);

        if result.is_some() {
            tracing::debug!(
                actor_type = %message.key,
                label = %message.label,
                "Actor found"
            );
        } else {
            tracing::debug!(
                actor_type = %message.key,
                label = %message.label,
                "Actor not found"
            );
        }

        result
    }
}

