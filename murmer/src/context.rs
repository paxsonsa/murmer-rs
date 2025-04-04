use std::future::Future;

use futures::channel::oneshot::Cancellation;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::Actor,
    message::Message,
    system::{Endpoint, System},
};

/// Provides context and capabilities to actors during message handling.
///
/// The context gives actors access to:
/// - Their own address for self-messaging
/// - The actor system
/// - Other runtime capabilities
pub struct Context<A>
where
    A: Actor,
{
    endpoint: Endpoint<A>,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Context<A>
where
    A: Actor,
{
    /// Create a new context for the given actor.
    pub fn new(endpoint: Endpoint<A>) -> Self {
        Context {
            endpoint,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<A> Context<A>
where
    A: Actor,
{
    /// Access the actor's endpoint for sending messages to itself.
    pub fn endpoint(&self) -> Endpoint<A> {
        self.endpoint.clone()
    }

    /// Access the actor system.
    pub fn system(&self) -> System {
        todo!()
    }

    /// Return a subsystem that is a child to actor's system.
    pub fn subsystem(&self) -> System {
        todo!()
    }

    /// Return a cancellation that is a child to actor's cancellation token.
    pub fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        todo!()
    }

    /// Establish an interval to execute a given closure at a fixed interval.
    pub fn interval<F>(&self, interval: std::time::Duration, f: F)
    where
        F: FnMut(&Context<A>) + Send + 'static,
    {
        todo!()
    }

    /// Spawn the future on the actor's runtime/lifecycle.
    ///
    /// The given future will be cancelled if the actor is shutdown/dropped.
    /// As such, the future should be cancel safe as the future may be cancelled
    /// without notice. Once the future is cancelled, it will not be restarted.
    ///
    pub fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) {
        let cancellation = self.cancellation();
        tokio::spawn(async move {
            tokio::select! {
                _ = f => {},
                _ = cancellation.cancelled() => {}
            };
        });
    }

    /// Send a message to the actor's endpoint.
    pub fn send(&self, msg: impl Message) {
        todo!()
    }

    /// Send a message to the actor's endpoint with a priority.
    pub fn send_priority(&self, msg: impl Message) {
        todo!()
    }
}
