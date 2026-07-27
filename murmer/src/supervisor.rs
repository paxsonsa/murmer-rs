//! Supervisor — runs an actor, processing both local and remote messages.
//!
//! Each actor is driven by a supervisor task that:
//!
//! 1. Receives local messages from the mailbox (typed `EnvelopeProxy<A>`)
//! 2. Receives remote messages from the dispatch channel (`DispatchRequest`)
//! 3. Receives system signals (actor termination notifications from watches)
//! 4. Handles graceful shutdown via a oneshot signal
//!
//! The supervisor wraps every handler invocation in `catch_unwind`, so a panicking
//! handler doesn't crash the entire runtime — it records the panic as a
//! [`TerminationReason::Panicked`] and exits cleanly.
//!
//! On exit, the `DeregisterGuard` RAII type automatically removes the actor
//! from the receptionist and fires any registered watches.
//!
//! Between those two events sits the [`TerminateHook`](crate::lifecycle::TerminateHook)
//! seam: a fault-injection point that holds the actor in the "stopped but still
//! registered" state a test otherwise can't reach, because deregistration rides
//! on a synchronous `Drop`. `None` in production.

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, ActorContext, RemoteDispatch};
use crate::instrument;
use crate::lifecycle::{RestartPolicy, SystemSignal, TerminationReason};
use crate::receptionist::DeregisterGuard;
use crate::wire::{DispatchRequest, EnvelopeProxy, RemoteResponse};

/// Consecutive local mailbox messages processed before the dispatch channel
/// gets one priority check. `select!`'s `biased` polling means a saturated
/// mailbox would otherwise starve remote callers forever (issue #6); this
/// bounds the wait for a remote dispatch to one local burst.
pub(crate) const LOCAL_BURST_LIMIT: u64 = 64;

/// Returns the mailbox and dispatch receivers so they can be reused on restart.
/// The exit reason is communicated via the shared Arc<Mutex<TerminationReason>>.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_supervisor<A>(
    mut actor: A,
    mut state: A::State,
    ctx: ActorContext<A>,
    mut mailbox_rx: mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>,
    mut dispatch_rx: mpsc::UnboundedReceiver<DispatchRequest>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut system_rx: mpsc::UnboundedReceiver<SystemSignal>,
    exit_reason: Arc<Mutex<TerminationReason>>,
    _deregister_guard: DeregisterGuard,
) -> (
    mpsc::UnboundedReceiver<Box<dyn EnvelopeProxy<A>>>,
    mpsc::UnboundedReceiver<DispatchRequest>,
)
where
    A: Actor + RemoteDispatch + 'static,
{
    let actor_type = std::any::type_name::<A>();
    let mut msg_count: u64 = 0;
    let mut local_burst: u64 = 0;

    loop {
        // Fairness slot: after a full burst of consecutive local messages,
        // check the dispatch channel once before the mailbox. `try_recv` keeps
        // the schedule a pure function of channel state — no randomness, no
        // reliance on `select!`'s arm rotation — so simulation replay stays
        // deterministic.
        if local_burst >= LOCAL_BURST_LIMIT {
            local_burst = 0;
            if let Ok(request) = dispatch_rx.try_recv() {
                if handle_dispatch(
                    &mut actor,
                    &mut state,
                    &ctx,
                    request,
                    actor_type,
                    &exit_reason,
                )
                .await
                {
                    break;
                }
                msg_count += 1;
                if msg_count.is_multiple_of(64) {
                    crate::runtime::yield_now().await;
                }
                continue;
            }
        }

        tokio::select! {
            // Deterministic branch priority: under a deterministic runtime,
            // `select!`'s default random branch order would break replay. The
            // ordering here (shutdown-ish signals after messages) is a
            // deliberate, stable policy. Remote-dispatch starvation under a
            // saturated mailbox is bounded by the fairness slot above.
            biased;
            msg = mailbox_rx.recv() => {
                match msg {
                    Some(envelope) => {
                        #[cfg(feature = "monitor")]
                        let start = std::time::Instant::now(); // determinism-gate: allow — monitor instrumentation (measurement, not control flow)

                        let fut = envelope.handle(&ctx, &mut actor, &mut state);
                        let result = AssertUnwindSafe(fut).catch_unwind().await;
                        if let Err(panic_info) = result {
                            instrument::message_failed(actor_type);
                            let msg = extract_panic_message(&panic_info);
                            *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                            break;
                        }
                        instrument::message_processed(actor_type);
                        #[cfg(feature = "monitor")]
                        instrument::message_processing_duration(actor_type, start.elapsed());

                        local_burst += 1;
                        msg_count += 1;
                        if msg_count.is_multiple_of(64) {
                            crate::runtime::yield_now().await;
                        }
                    }
                    None => break,
                }
            }
            req = dispatch_rx.recv() => {
                match req {
                    Some(request) => {
                        local_burst = 0;
                        if handle_dispatch(
                            &mut actor,
                            &mut state,
                            &ctx,
                            request,
                            actor_type,
                            &exit_reason,
                        )
                        .await
                        {
                            break;
                        }
                        msg_count += 1;
                        if msg_count.is_multiple_of(64) {
                            crate::runtime::yield_now().await;
                        }
                    }
                    None => break,
                }
            }
            signal = system_rx.recv() => {
                match signal {
                    Some(SystemSignal::ActorTerminated(terminated)) => {
                        actor.on_actor_terminated(&mut state, &terminated);
                    }
                    Some(SystemSignal::Stop) => {
                        break;
                    }
                    None => {} // system channel closed, ignore
                }
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }
    // Fault-injection seam. The loop has exited — this actor no longer serves
    // messages — but its `DeregisterGuard` has not fired, so it is still in the
    // receptionist: `lookup` finds it, listings include it, watches are silent.
    // Awaiting here holds the actor in that "accepted its stop, not yet gone"
    // state, which is zero-width otherwise (deregistration rides on a
    // synchronous `Drop`) and therefore untestable. `None` in production.
    if let Some(hook) = ctx.receptionist.terminate_hook() {
        let reason = exit_reason.lock().unwrap().clone();
        hook.before_deregister(&ctx.label, &reason).await;
    }

    // _deregister_guard is dropped here → calls receptionist.deregister_with_reason(label, reason)
    (mailbox_rx, dispatch_rx)
}

/// Process one remote dispatch request and send the response. Returns `true`
/// if the handler panicked and the supervisor should exit.
async fn handle_dispatch<A>(
    actor: &mut A,
    state: &mut A::State,
    ctx: &ActorContext<A>,
    request: DispatchRequest,
    actor_type: &'static str,
    exit_reason: &Mutex<TerminationReason>,
) -> bool
where
    A: Actor + RemoteDispatch + 'static,
{
    #[cfg(feature = "monitor")]
    let start = std::time::Instant::now(); // determinism-gate: allow — monitor instrumentation (measurement, not control flow)

    let fut = actor.dispatch_remote(
        ctx,
        state,
        &request.invocation.message_type,
        &request.invocation.payload,
    );
    let result = AssertUnwindSafe(fut).catch_unwind().await;
    match result {
        Ok(dispatch_result) => {
            instrument::message_processed(actor_type);
            #[cfg(feature = "monitor")]
            instrument::message_processing_duration(actor_type, start.elapsed());

            let response = RemoteResponse {
                call_id: request.invocation.call_id,
                result: dispatch_result.map_err(|e| e.to_string()),
            };
            let _ = request.respond_to.send(response);
            false
        }
        Err(panic_info) => {
            instrument::message_failed(actor_type);
            let msg = extract_panic_message(&panic_info);
            // Send error response before breaking
            let response = RemoteResponse {
                call_id: request.invocation.call_id,
                result: Err(format!("actor panicked: {msg}")),
            };
            let _ = request.respond_to.send(response);
            *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
            true
        }
    }
}

/// Extract a human-readable message from a panic payload.
pub(crate) fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Decide whether an actor should be restarted given its policy and exit reason.
pub(crate) fn should_restart(policy: &RestartPolicy, reason: &TerminationReason) -> bool {
    match policy {
        RestartPolicy::Temporary => false,
        RestartPolicy::Permanent => true,
        RestartPolicy::Transient => matches!(reason, TerminationReason::Panicked(_)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::actor::{DispatchError, Handler, Message};
    use crate::receptionist::Receptionist;
    use crate::wire::{RemoteInvocation, TypedEnvelope};

    struct FloodActor;

    struct FloodState {
        local_processed: u64,
    }

    impl Actor for FloodActor {
        type State = FloodState;
    }

    #[derive(Debug)]
    struct Bump;

    impl Message for Bump {
        type Result = ();
    }

    impl Handler<Bump> for FloodActor {
        fn handle(&mut self, _ctx: &ActorContext<Self>, state: &mut FloodState, _msg: Bump) {
            state.local_processed += 1;
        }
    }

    impl RemoteDispatch for FloodActor {
        async fn dispatch_remote<'a>(
            &'a mut self,
            _ctx: &'a ActorContext<Self>,
            state: &'a mut FloodState,
            _message_type: &'a str,
            _payload: &'a [u8],
        ) -> Result<Vec<u8>, DispatchError> {
            // Report how many local messages had been processed when this
            // remote dispatch finally ran.
            Ok(state.local_processed.to_le_bytes().to_vec())
        }
    }

    /// Regression test for issue #6: `biased;` must not let a saturated local
    /// mailbox starve remote dispatch. The mailbox is pre-filled with far more
    /// messages than one local burst; the dispatch request must be served
    /// within the first burst, not after the mailbox drains.
    #[tokio::test]
    async fn remote_dispatch_not_starved_by_saturated_mailbox() {
        const FLOOD: u64 = 1000;

        let receptionist = Receptionist::new();
        let (mailbox_tx, mailbox_rx) =
            mpsc::unbounded_channel::<Box<dyn EnvelopeProxy<FloodActor>>>();
        let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel::<DispatchRequest>();
        let (_shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (system_tx, system_rx) = mpsc::unbounded_channel::<SystemSignal>();
        let exit_reason = Arc::new(Mutex::new(TerminationReason::Stopped));

        let guard = DeregisterGuard {
            receptionist: receptionist.clone(),
            label: "flood/test".to_string(),
            reason: exit_reason.clone(),
        };
        let ctx = ActorContext {
            label: "flood/test".to_string(),
            node_id: "local".to_string(),
            mailbox_tx: mailbox_tx.clone(),
            receptionist: receptionist.clone(),
            system_tx: system_tx.clone(),
            reply_token: std::sync::Mutex::new(None),
        };

        // Saturate the mailbox before the supervisor runs, then queue one
        // remote dispatch behind it. With `biased;` alone the dispatch would
        // only run after all FLOOD local messages.
        let mut reply_rxs = Vec::new();
        for _ in 0..FLOOD {
            let (tx, rx) = oneshot::channel();
            mailbox_tx
                .send(Box::new(TypedEnvelope {
                    message: Bump,
                    respond_to: tx,
                }))
                .unwrap();
            reply_rxs.push(rx);
        }
        let (resp_tx, mut resp_rx) = oneshot::channel::<RemoteResponse>();
        dispatch_tx
            .send(DispatchRequest {
                invocation: RemoteInvocation {
                    call_id: 1,
                    actor_label: "flood/test".to_string(),
                    message_type: "test::Snapshot".to_string(),
                    payload: Vec::new(),
                },
                respond_to: resp_tx,
            })
            .unwrap();
        // Stop is only seen once the mailbox and dispatch arms are pending,
        // so it cleanly ends the supervisor after everything drains.
        system_tx.send(SystemSignal::Stop).unwrap();

        let (_mb, _dp) = run_supervisor(
            FloodActor,
            FloodState { local_processed: 0 },
            ctx,
            mailbox_rx,
            dispatch_rx,
            shutdown_rx,
            system_rx,
            exit_reason,
            guard,
        )
        .await;

        let response = resp_rx
            .try_recv()
            .expect("remote dispatch should have been served");
        let payload = response.result.expect("dispatch should succeed");
        let processed_at_dispatch = u64::from_le_bytes(payload.try_into().unwrap());

        assert!(
            processed_at_dispatch < FLOOD,
            "remote dispatch was starved: ran only after all {FLOOD} local messages"
        );
        assert!(
            processed_at_dispatch <= LOCAL_BURST_LIMIT,
            "remote dispatch should run within one local burst \
             (ran after {processed_at_dispatch} local messages, limit {LOCAL_BURST_LIMIT})"
        );

        // All local messages must still have been processed.
        let mut delivered = 0;
        for rx in reply_rxs {
            if rx.await.is_ok() {
                delivered += 1;
            }
        }
        assert_eq!(delivered, FLOOD, "every local message should be handled");
    }

    /// The terminate hook is a real seam on the production (Tokio) path too, not
    /// just under simulation — the sim tests in `sim.rs` cover the virtual-clock
    /// behavior; this one covers that the branch is live on `TokioRuntime`.
    #[tokio::test]
    async fn terminate_hook_delays_deregistration_on_the_tokio_path() {
        use crate::lifecycle::TerminateHook;
        use crate::runtime::{BoxFuture, Runtime, TokioRuntime};
        use std::time::Duration;

        struct SlowToDie;

        impl TerminateHook for SlowToDie {
            fn before_deregister(
                &self,
                label: &str,
                _reason: &TerminationReason,
            ) -> BoxFuture<'static, ()> {
                assert_eq!(label, "flood/dying");
                // Through the seam, like a real hook would: the same impl works
                // unchanged when handed a `SimRuntime` instead.
                TokioRuntime.sleep(Duration::from_millis(200))
            }
        }

        let receptionist = Receptionist::new();
        receptionist.set_terminate_hook(Some(Arc::new(SlowToDie)));

        let _ep = receptionist.start("flood/dying", FloodActor, FloodState { local_processed: 0 });
        receptionist.stop("flood/dying");

        // Well inside the hook's delay: stop accepted, actor still registered.
        TokioRuntime.sleep(Duration::from_millis(50)).await;
        assert!(
            receptionist.lookup::<FloodActor>("flood/dying").is_some(),
            "actor must stay registered while the terminate hook is pending"
        );

        // Past it: the hook completes, the guard drops, the entry goes.
        TokioRuntime.sleep(Duration::from_millis(300)).await;
        assert!(
            receptionist.lookup::<FloodActor>("flood/dying").is_none(),
            "actor must de-register once the terminate hook completes"
        );
    }
}
