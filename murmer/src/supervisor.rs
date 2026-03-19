//! Supervisor — runs an actor, handles both local and remote messages.

use std::sync::{Arc, Mutex};

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
    loop {
        tokio::select! {
            msg = mailbox_rx.recv() => {
                match msg {
                    Some(envelope) => {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            envelope.handle(&ctx, &mut actor, &mut state);
                        }));
                        if let Err(panic_info) = result {
                            let msg = extract_panic_message(&panic_info);
                            *exit_reason.lock().unwrap() = TerminationReason::Panicked(msg);
                            break;
                        }
                    }
                    None => break,
                }
            }
            req = dispatch_rx.recv() => {
                match req {
                    Some(request) => {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            actor.dispatch_remote(
                                &ctx,
                                &mut state,
                                &request.invocation.message_type,
                                &request.invocation.payload,
                            )
                        }));
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
                    }
                    None => break,
                }
            }
            signal = system_rx.recv() => {
                match signal {
                    Some(SystemSignal::ActorTerminated(terminated)) => {
                        actor.on_actor_terminated(&mut state, &terminated);
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
