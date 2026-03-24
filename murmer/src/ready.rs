//! ReadyHandle — deferred actor startup.
//!
//! [`ReadyHandle`] is returned by [`Receptionist::prepare()`](crate::Receptionist::prepare)
//! and [`System::prepare()`](crate::System::prepare). It represents an actor that is
//! registered in the receptionist (and therefore discoverable / message-queueable)
//! but whose supervisor has not yet been spawned.
//!
//! Messages sent to the actor's endpoint will queue in the unbounded mailbox
//! channel until [`ReadyHandle::start()`] is called to spawn the supervisor.
//!
//! If the `ReadyHandle` is dropped without calling `start()`, the internal
//! [`DeregisterGuard`](crate::receptionist::DeregisterGuard) fires and the actor
//! is automatically removed from the receptionist.

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, ActorContext, RemoteDispatch};
use crate::lifecycle::{SystemSignal, TerminationReason};
use crate::receptionist::DeregisterGuard;
use crate::supervisor::run_supervisor;
use crate::wire::{DispatchRequest, EnvelopeProxy};

/// A handle to a prepared (but not yet running) actor.
///
/// The actor is already registered in the receptionist and its endpoint is
/// functional — messages sent to the endpoint queue in the unbounded mailbox.
/// Calling [`start()`](ReadyHandle::start) spawns the supervisor which begins
/// draining the mailbox and processing messages.
///
/// Dropping without calling `start()` automatically deregisters the actor.
pub struct ReadyHandle<A: Actor + RemoteDispatch + 'static> {
    actor: Option<A>,
    state: Option<A::State>,
    ctx: Option<ActorContext<A>>,
    mailbox_rx: Option<mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>>,
    dispatch_rx: Option<mpsc::UnboundedReceiver<DispatchRequest>>,
    shutdown_rx: Option<oneshot::Receiver<()>>,
    system_rx: Option<mpsc::UnboundedReceiver<SystemSignal>>,
    exit_reason: Arc<Mutex<TerminationReason>>,
    _deregister_guard: Option<DeregisterGuard>,
}

impl<A: Actor + RemoteDispatch + 'static> ReadyHandle<A> {
    /// Create a new ReadyHandle. Called internally by `Receptionist::prepare()`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        actor: A,
        state: A::State,
        ctx: ActorContext<A>,
        mailbox_rx: mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>,
        dispatch_rx: mpsc::UnboundedReceiver<DispatchRequest>,
        shutdown_rx: oneshot::Receiver<()>,
        system_rx: mpsc::UnboundedReceiver<SystemSignal>,
        exit_reason: Arc<Mutex<TerminationReason>>,
        deregister_guard: DeregisterGuard,
    ) -> Self {
        Self {
            actor: Some(actor),
            state: Some(state),
            ctx: Some(ctx),
            mailbox_rx: Some(mailbox_rx),
            dispatch_rx: Some(dispatch_rx),
            shutdown_rx: Some(shutdown_rx),
            system_rx: Some(system_rx),
            exit_reason,
            _deregister_guard: Some(deregister_guard),
        }
    }

    /// Spawn the supervisor, beginning message processing.
    ///
    /// All messages that were queued in the mailbox while the actor was in
    /// the prepared state will be processed in order.
    ///
    /// This consumes the `ReadyHandle`. The [`DeregisterGuard`] ownership
    /// transfers to the supervisor task — when the supervisor exits, the
    /// guard fires and deregisters the actor.
    pub fn start(mut self) {
        let actor = self.actor.take().expect("ReadyHandle: actor already taken");
        let state = self.state.take().expect("ReadyHandle: state already taken");
        let ctx = self.ctx.take().expect("ReadyHandle: ctx already taken");
        let mailbox_rx = self
            .mailbox_rx
            .take()
            .expect("ReadyHandle: mailbox_rx already taken");
        let dispatch_rx = self
            .dispatch_rx
            .take()
            .expect("ReadyHandle: dispatch_rx already taken");
        let shutdown_rx = self
            .shutdown_rx
            .take()
            .expect("ReadyHandle: shutdown_rx already taken");
        let system_rx = self
            .system_rx
            .take()
            .expect("ReadyHandle: system_rx already taken");
        let exit_reason = self.exit_reason.clone();
        let guard = self
            ._deregister_guard
            .take()
            .expect("ReadyHandle: deregister_guard already taken");

        tokio::spawn(async move {
            let _ = run_supervisor(
                actor,
                state,
                ctx,
                mailbox_rx,
                dispatch_rx,
                shutdown_rx,
                system_rx,
                exit_reason,
                guard,
            )
            .await;
        });
    }
}
