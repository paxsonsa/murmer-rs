use crate::actor::Actor;
use crate::endpoint::Endpoint;
use crate::supervisor::{Supervisor, Initialized};
use crate::receptionist::{Receptionist, ReceptionistActor};

/// Builder for creating a new system.
pub struct SystemBuilder {}

impl SystemBuilder {
    /// Create a new system builder.
    pub fn new() -> Self {
        SystemBuilder {}
    }

    /// Build the actor system with built-in receptionist service.
    pub fn build(self) -> System {
        System::new_with_receptionist()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SystemError {}

pub struct System {
    root_cancellation: tokio_util::sync::CancellationToken,
    receptionist: Receptionist,
}

impl System {
    /// Internal method to create a system with a built-in receptionist.
    fn new_with_receptionist() -> Self {
        let root_cancellation = tokio_util::sync::CancellationToken::new();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        
        // Create the receptionist endpoint
        let endpoint = crate::endpoint::Endpoint::new(sender);
        let receptionist = Receptionist::new(endpoint);
        
        // Start the receptionist actor task
        let mut actor = ReceptionistActor::default();
        let actor_cancellation = root_cancellation.child_token();
        tokio::spawn(async move {
            use crate::actor::Actor;
            let mut ctx = crate::actor::Context {
                _phantom: std::marker::PhantomData,
            };
            
            let mut state = match actor.init(&mut ctx).await {
                Ok(state) => state,
                Err(e) => {
                    tracing::error!("Failed to initialize receptionist: {}", e);
                    return;
                }
            };
            actor.started(&mut ctx, &mut state).await;
            
            loop {
                tokio::select!(
                    _ = actor_cancellation.cancelled() => {
                        tracing::info!("Receptionist cancelled, stopping.");
                        break;
                    }
                    envelope = receiver.recv() => {
                        if let Some(mut envelope) = envelope {
                            envelope.0.handle_async(&mut ctx, &mut state, &mut actor).await;
                        } else {
                            tracing::warn!("Receptionist mailbox closed, stopping.");
                            break;
                        }
                    }
                );
            }
            
            actor.stopping(&mut ctx, &mut state).await;
            actor.stopped(&mut ctx, state).await;
        });
        
        System {
            root_cancellation,
            receptionist,
        }
    }
    pub fn spawn<A: Actor + Default>(
        &self,
    ) -> Result<(Supervisor<A, Initialized<A>>, Endpoint<A>), SystemError> {
        let actor: A = Default::default();
        self.spawn_with(actor)
    }

    pub fn spawn_with<A: Actor>(
        &self,
        actor: A,
    ) -> Result<(Supervisor<A, Initialized<A>>, Endpoint<A>), SystemError> {
        let supervisor = Supervisor::construct(actor);
        let supervisor = supervisor.start_within(self);
        let endpoint = supervisor.endpoint();
        Ok((supervisor, endpoint))
    }

    /// Get a reference to the system's built-in receptionist service.
    pub fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    pub(crate) fn root_cancellation(&self) -> &tokio_util::sync::CancellationToken {
        &self.root_cancellation
    }
}