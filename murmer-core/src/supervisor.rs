use crate::actor::{Actor, Context};
use crate::endpoint::Endpoint;
use crate::message::Envelope;
use crate::system::System;

pub trait State {}

pub struct Uninitialized<A: Actor> {
    actor: A,
}

impl<A> State for Uninitialized<A> where A: Actor {}

#[derive(Clone)]
pub struct Initialized<A: Actor> {
    cancellation: tokio_util::sync::CancellationToken,
    sender: tokio::sync::mpsc::UnboundedSender<Envelope<A>>,
}

impl<A> State for Initialized<A> where A: Actor {}

pub struct Supervisor<A, S>
where
    A: Actor,
    S: State,
{
    state: S,
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Supervisor<A, Initialized<A>>
where
    A: Actor,
{
    pub fn endpoint(&self) -> Endpoint<A> {
        let sender = self.state.sender.clone();
        Endpoint::new(sender)
    }
}

impl<A> Supervisor<A, Uninitialized<A>>
where
    A: Actor,
{
    pub fn construct(actor: A) -> Supervisor<A, Uninitialized<A>> {
        Supervisor {
            state: Uninitialized { actor },
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn start_within(self, system: &System) -> Supervisor<A, Initialized<A>> {
        let cancellation = system.root_cancellation().child_token();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Envelope<A>>();

        let supervisor = Supervisor {
            state: Initialized {
                cancellation,
                sender: sender.clone(),
            },
            _phantom: std::marker::PhantomData,
        };

        let mut actor = self.state.actor;
        let cancellation = supervisor.state.cancellation.child_token();
        tokio::spawn(async move {
            let mut ctx = Context {
                _phantom: std::marker::PhantomData,
            };

            let mut state = match actor.init(&mut ctx).await {
                Ok(state) => state,
                Err(e) => {
                    tracing::error!("Failed to initialize actor: {}", e);
                    return;
                }
            };
            actor.started(&mut ctx, &mut state).await;

            loop {
                // TODO: Support receiving system commands for restarting and checking status.
                tokio::select!(
                    _ = cancellation.cancelled() => {
                        tracing::info!("Actor cancelled, stopping actor.");
                        break;
                    }
                    envelope = receiver.recv() => {
                        if let Some(mut envelope) = envelope {
                            envelope
                                .0
                                .handle_async(&mut ctx, &mut state, &mut actor)
                                .await;
                        } else {
                            tracing::warn!("Actor mailbox closed, stopping actor.");
                            break;
                        }
                    }
                );
            }

            actor.stopping(&mut ctx, &mut state).await;
            actor.stopped(&mut ctx, state).await;
        });
        supervisor
    }
}