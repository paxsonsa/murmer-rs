use std::future::Future;

use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, Handler},
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
    cancellation: CancellationToken,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Context<A>
where
    A: Actor,
{
    /// Create a new context for the given actor.
    pub fn new(endpoint: Endpoint<A>, cancellation: CancellationToken) -> Self {
        Context {
            endpoint,
            cancellation,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<A> Clone for Context<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Context {
            endpoint: self.endpoint.clone(),
            cancellation: self.cancellation.clone(),
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
        System::current()
    }

    /// Return a cancellation token for this actor.
    pub(crate) fn inner_cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation.clone()
    }

    /// Establish an interval that sends a message to the actor at a fixed interval.
    ///
    /// This method takes a message factory function that creates a new message instance
    /// each time the interval triggers. The actor must implement Handler<M> for the
    /// message type.
    ///
    /// # Example
    /// ```
    /// // Create an interval that sends a Tick message every second
    /// ctx.interval(Duration::from_secs(1), || Tick);
    ///
    /// // Create an interval with dynamic message content
    /// let counter = Arc::new(AtomicUsize::new(0));
    /// let counter_clone = counter.clone();
    /// ctx.interval(Duration::from_secs(5), move || {
    ///     let count = counter_clone.fetch_add(1, Ordering::SeqCst);
    ///     StatusUpdate { count }
    /// });
    /// ```
    pub fn interval<F, M>(&self, interval: std::time::Duration, message_factory: F)
    where
        F: Fn() -> M + Send + 'static,
        M: Message + Send + 'static,
        A: Handler<M>,
        M::Result: Send,
    {
        let endpoint = self.endpoint();
        let cancellation = self.cancellation.child_token();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        let msg = message_factory();
                        if let Err(err) = endpoint.send(msg).await {
                            tracing::error!(error=%err, "Failed to send interval message");
                            break;
                        }
                    },
                    _ = cancellation.cancelled() => {
                        break;
                    }
                }
            }
        });
    }

    /// Spawn the future on the actor's runtime/lifecycle.
    ///
    /// The given future will be cancelled if the actor is shutdown/dropped.
    /// As such, the future should be cancel safe as the future may be cancelled
    /// without notice. Once the future is cancelled, it will not be restarted.
    ///
    pub fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) -> CancellationToken {
        let cancellation = self.cancellation.child_token();
        let cancellation_ret = cancellation.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = f => {},
                _ = cancellation.cancelled() => {}
            };
        });
        cancellation_ret
    }

    /// Spawn the actor in the actor's runtime/lifecycle.
    ///
    /// The given actor will be cancelled if the parent actor is shutdown/dropped.
    /// As such, the actor should be cancel safe as the actor may be cancelled
    /// without notice. Once the actor is cancelled, it will not be restarted.
    ///
    /// Sub-actors are not remote accessible and are not registered with the system.
    ///
    pub fn spawn_actor<C: Actor>(&self, child_actor: C) -> CancellationToken {
        // TODO: Needs to have a supervisor trait so it can receive lifecycle events.
        todo!("implement spawn_actor");
    }

    /// Send a message to the actor's endpoint.
    pub fn send<M>(&self, msg: M)
    where
        A: crate::actor::Handler<M>,
        M: Message + Send + 'static,
        M::Result: Send,
    {
        let endpoint = self.endpoint();
        tokio::spawn(async move {
            if let Err(err) = endpoint.send(msg).await {
                tracing::error!(error=%err, "Failed to send message");
            }
        });
    }
}
