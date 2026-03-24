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
//! [`TerminationReason::Panicked`](crate::TerminationReason::Panicked) and exits cleanly.
//!
//! On exit, the [`DeregisterGuard`] RAII type automatically removes the actor
//! from the receptionist and fires any registered watches.

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, ActorContext, RemoteDispatch};
use crate::lifecycle::{RestartPolicy, SystemSignal, TerminationReason};
use crate::receptionist::DeregisterGuard;
use crate::wire::{DispatchRequest, EnvelopeProxy, RemoteResponse};

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
    let mut msg_count: u64 = 0;

    loop {
        tokio::select! {
            msg = mailbox_rx.recv() => {
                match msg {
                    Some(envelope) => {
                        let fut = envelope.handle(&ctx, &mut actor, &mut state);
                        let result = AssertUnwindSafe(fut).catch_unwind().await;
                        if let Err(panic_info) = result {
                            let msg = extract_panic_message(&panic_info);
                            *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                            break;
                        }
                        msg_count += 1;
                        if msg_count.is_multiple_of(64) {
                            tokio::task::yield_now().await;
                        }
                    }
                    None => break,
                }
            }
            req = dispatch_rx.recv() => {
                match req {
                    Some(request) => {
                        let fut = actor.dispatch_remote(
                            &ctx,
                            &mut state,
                            &request.invocation.message_type,
                            &request.invocation.payload,
                        );
                        let result = AssertUnwindSafe(fut).catch_unwind().await;
                        match result {
                            Ok(dispatch_result) => {
                                let response = RemoteResponse {
                                    call_id: request.invocation.call_id,
                                    result: dispatch_result.map_err(|e| e.to_string()),
                                };
                                let _ = request.respond_to.send(response);
                            }
                            Err(panic_info) => {
                                let msg = extract_panic_message(&panic_info);
                                // Send error response before breaking
                                let response = RemoteResponse {
                                    call_id: request.invocation.call_id,
                                    result: Err(format!("actor panicked: {msg}")),
                                };
                                let _ = request.respond_to.send(response);
                                *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                                break;
                            }
                        }
                        msg_count += 1;
                        if msg_count.is_multiple_of(64) {
                            tokio::task::yield_now().await;
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
    // _deregister_guard is dropped here → calls receptionist.deregister_with_reason(label, reason)
    (mailbox_rx, dispatch_rx)
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
