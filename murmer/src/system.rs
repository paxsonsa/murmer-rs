//! Actor system implementation providing actor lifecycle management and message routing.
//!
//! This module contains the core actor system implementation, which is responsible for:
//! - Managing actor lifecycles (creation, supervision, shutdown)
//! - Message routing between actors
//! - System-wide coordination and configuration
//! - Actor supervision hierarchies
use parking_lot::RwLock;
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tokio::{
    sync::{mpsc, oneshot},
    task_local,
};

use crate::context::Context;

use super::actor::*;
use super::cluster;
use super::id::Id;
use super::mailbox::*;
use super::message::*;
use super::path::ActorPath;
use super::receptionist::*;

task_local! {
    static ACTIVE_SYSTEM: System;
}

/// Global counter for generating unique system IDs
pub type SystemId = usize;
static NEXT_SYSTEM_ID: AtomicUsize = AtomicUsize::new(0);

/// Errors that can occur during actor system operations
#[derive(thiserror::Error, Debug)]
pub enum SystemError {
    /// Indicates a failure to spawn a new actor
    #[error("Failed to spawn actor")]
    SpawnError,

    /// Indicates some cluster-related error
    #[error("Cluster error: {0}")]
    ClusterError(#[from] cluster::ClusterError),
}

/// Internal trait for actor supervisors
///
/// Supervisors are responsible for:
/// - Managing actor lifecycle
/// - Handling actor failures
/// - Coordinating actor shutdown
trait SystemSupervisor: Send + Sync {
    /// Returns the unique identifier of the supervised actor
    fn id(&self) -> Arc<Id>;
    /// Initiates supervisor shutdown sequence
    fn shutdown(&mut self);
}

/// Internal state of the actor system
///
/// Maintains:
/// - System-wide unique identifier
/// - Counter for generating actor IDs
/// - Registry of all active supervisors
struct SystemContext {
    /// Unique identifier for this system instance
    id: SystemId,
    /// Registry of all active actor supervisors
    supervisors: HashMap<Arc<Id>, Box<dyn SystemSupervisor>>,
}

impl SystemContext {
    /// Creates a new system context with a unique system ID
    fn new() -> Self {
        let id = NEXT_SYSTEM_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            id,
            supervisors: HashMap::new(),
        }
    }

    fn id(&self) -> SystemId {
        self.id
    }

    fn register<A: Actor>(&mut self, id: Arc<Id>, supervisor: Supervisor<A, Initialized>) {
        self.supervisors.insert(id, Box::new(supervisor));
    }

    /// Shutdown the system and its actors
    ///
    /// During shutdown, each running actor will be shutdown. This will not
    /// ensure that the all queued messages for the actors is processed or perform
    /// a wait.
    ///
    fn shutdown(&mut self) {
        for (_, mut supervisor) in self.supervisors.drain() {
            supervisor.shutdown();
        }
    }
}

/// Configuration options for the actor system
///
/// The system can be configured to run in different modes:
/// - Local: Single-node system where all actors run in the same runtime instance locally.
/// - Clustered: Multi-node system where actors can be distributed across a network of nodes
/// including locally.
///
#[derive(Clone)]
enum Mode {
    Local { name: String },
    Clustered(cluster::Cluster),
}

/// The actor system that manages actor lifecycles and message delivery.
///
/// The System is the main entry point for:
/// - Creating new actors
/// - Managing actor lifecycles
/// - Coordinating system-wide operations
/// - Handling system shutdown
///
/// # Example
/// ```
/// use cinemotion::actors::prelude::*;
///
/// let system = System::new();
/// let actor = system.spawn::<MyActor>()?;
/// actor.send(MyMessage).await?;
/// ```
#[derive(Clone)]
pub enum System {
    Local {
        name: String,
        context: Arc<RwLock<SystemContext>>,
        receptionist: Receptionist,
    },
    Clustered {
        cluster: cluster::Cluster,
        context: Arc<RwLock<SystemContext>>,
        receptionist: Receptionist,
    },
}

impl System {
    pub fn current() -> Self {
        ACTIVE_SYSTEM.with(|system| system.clone())
    }

    pub fn try_current() -> Option<Self> {
        ACTIVE_SYSTEM.try_with(|system| system.clone()).ok()
    }

    pub fn local<S: AsRef<str> + Into<String>>(name: S) -> Self {
        let actor = ReceptionistActor::default();
        let receptionist_supervisor = Supervisor::construct(actor);
        let receptionist = Receptionist::new(receptionist_supervisor.endpoint());

        // Create the system context and system handle
        let context = Arc::new(RwLock::new(SystemContext::new()));

        Self::Local {
            name: name.into(),
            context,
            receptionist,
        }
    }

    pub fn clustered(config: cluster::Config) -> Result<Self, SystemError> {
        let actor = cluster::ClusterActor::new(config)?;
        let cluster_supervisor = Supervisor::construct(actor);
        let cluster = cluster::Cluster::new(cluster_supervisor.endpoint());

        // Create a receptionist stack for the system.
        let actor = ReceptionistActor::default();
        let receptionist_supervisor = Supervisor::construct(actor);
        let receptionist = Receptionist::new(receptionist_supervisor.endpoint());

        // Create the system context and system handle
        let context = Arc::new(RwLock::new(SystemContext::new()));

        let system = System::Clustered {
            cluster,
            context,
            receptionist,
        };

        receptionist_supervisor.start_within(system.clone());
        cluster_supervisor.start_within(system.clone());

        Ok(system)
    }

    pub fn receptionist(&self) -> &Receptionist {
        match self {
            Self::Local { receptionist, .. } => receptionist,
            Self::Clustered { receptionist, .. } => receptionist,
        }
    }

    pub fn id(&self) -> SystemId {
        match self {
            Self::Local { context, .. } => context.read().id(),
            Self::Clustered { context, .. } => context.read().id(),
        }
    }

    pub fn spawn<A: Actor + Default>(&self) -> Result<Endpoint<A>, SystemError> {
        let actor: A = Default::default();
        self.spawn_with(actor)
    }

    pub fn spawn_with<A: Actor>(&self, actor: A) -> Result<Endpoint<A>, SystemError> {
        let supervisor = Supervisor::construct(actor);
        let supervisor = supervisor.start_within(self.clone());
        Ok(supervisor.into())
    }

    pub fn shutdown(&mut self) {
        let mut context = match self {
            Self::Local { context, .. } => context.write(),
            Self::Clustered { context, .. } => context.write(),
        };
        context.shutdown();
    }

    fn context(&self) -> &Arc<RwLock<SystemContext>> {
        match self {
            Self::Local { context, .. } => context,
            Self::Clustered { context, .. } => context,
        }
    }
}

/// Commands that can be sent to an actor's supervisor
///
/// These commands control the actor's lifecycle and message processing:
/// - `Envelope`: Contains a message to be processed by the actor
/// - `Shutdown`: Signals the actor to begin its shutdown sequence
pub enum SupervisorCommand<A>
where
    A: Actor,
{
    /// A message envelope containing the actual message and response channel
    Envelope(Envelope<A>),
    /// Command to initiate actor shutdown
    Shutdown,
}

struct SupervisorRuntime<A>
where
    A: Actor,
{
    actor: A,
    mailbox: PrioritizedMailbox<SupervisorCommand<A>>,
}

impl<A> SupervisorRuntime<A>
where
    A: Actor,
{
    pub fn construct(actor: A) -> (Self, MailboxSender<SupervisorCommand<A>>) {
        let (tx, rx) = mpsc::channel(64);
        let mailbox_sender = MailboxSender::new(tx);
        let runtime = SupervisorRuntime {
            actor,
            mailbox: PrioritizedMailbox::new(rx),
        };
        (runtime, mailbox_sender)
    }

    fn actor_ref(&self) -> &A {
        &self.actor
    }

    async fn handle_command(&mut self, ctx: &mut Context<A>, cmd: SupervisorCommand<A>) -> bool {
        match cmd {
            SupervisorCommand::Envelope(envelope) => {
                let mut handler = envelope.0;
                handler.handle(ctx, &mut self.actor);
                true
            }
            SupervisorCommand::Shutdown => {
                self.actor.stopping(ctx).await;
                false
            }
        }
    }

    async fn run(mut self, _id: Arc<Id>, mut ctx: Context<A>) {
        self.actor.started(&mut ctx).await;
        loop {
            if !self.tick(&mut ctx).await {
                break;
            }
        }
        self.actor.stopped(&mut ctx).await;
    }

    async fn tick(&mut self, ctx: &mut Context<A>) -> bool {
        match self.mailbox.recv().await {
            Some(cmd) => !self.handle_command(ctx, cmd).await,
            None => false,
        }
    }
}

/// Testing Supervisor for Unit Testing
#[cfg(test)]
pub(crate) struct TestSupervisor<A>
where
    A: Actor,
{
    path: Arc<ActorPath>,
    ctx: Context<A>,
    runtime: SupervisorRuntime<A>,
    sender: MailboxSender<SupervisorCommand<A>>,
}

#[cfg(test)]
impl<A> TestSupervisor<A>
where
    A: Actor,
{
    pub fn new(actor: A) -> Self {
        let id = Id::new();
        let path = Arc::new(ActorPath::local(std::any::type_name::<A>().to_string(), id));
        let (runtime, sender) = SupervisorRuntime::construct(actor);

        let endpoint = Endpoint::new(EndpointSender::new(sender.clone()), path.clone());
        let ctx = Context::new(endpoint);

        Self {
            path,
            ctx,
            runtime,
            sender,
        }
    }

    pub fn actor_ref(&self) -> &A {
        self.runtime.actor_ref()
    }

    pub async fn started(&mut self) {
        self.runtime.actor.started(&mut self.ctx).await;
    }

    pub async fn send<M>(&mut self, system: &System, msg: M) -> Result<M::Result, ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let sender = EndpointSender::new(self.sender.clone());
        let endpoint_path = self.path.clone();
        ACTIVE_SYSTEM
            .scope(system.clone(), async move {
                let sender = sender.clone();
                let endpoint = Endpoint::new(sender, endpoint_path);
                let task = tokio::spawn(async move { endpoint.send(msg).await });
                self.runtime.tick(&mut self.ctx).await;
                task.await.unwrap()
            })
            .await
    }

    fn shutdown(&mut self) {
        let _ = self
            .sender
            .send_blocking(QoSLevel::Supervisor, SupervisorCommand::Shutdown);
    }

    async fn tick(&mut self, system: System) {
        ACTIVE_SYSTEM
            .scope(system, async move {
                self.runtime.tick(&mut self.ctx).await;
            })
            .await
    }
}

trait State {}

struct Uninitialized<A: Actor> {
    runtime: SupervisorRuntime<A>,
}

impl<A: Actor> State for Uninitialized<A> {}

#[derive(Clone)]
struct Initialized {}

impl State for Initialized {}

struct Supervisor<A, S>
where
    A: Actor,
    S: State,
{
    path: Arc<ActorPath>,
    sender: MailboxSender<SupervisorCommand<A>>,
    state: S,
}

impl<A: Actor, S: State + Clone> Clone for Supervisor<A, S> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

impl<A: Actor> Into<Endpoint<A>> for Supervisor<A, Initialized> {
    fn into(self) -> Endpoint<A> {
        let sender = EndpointSender::new(self.sender);
        Endpoint::new(sender, self.path)
    }
}

impl<A, S> Supervisor<A, S>
where
    A: Actor,
    S: State,
{
    fn endpoint(&self) -> Endpoint<A> {
        let sender = EndpointSender::new(self.sender.clone());
        Endpoint::new(sender, self.path.clone())
    }
}

impl<A> Supervisor<A, Uninitialized<A>>
where
    A: Actor,
{
    fn construct(actor: A) -> Supervisor<A, Uninitialized<A>> {
        let id = Id::new();
        let path = Arc::new(ActorPath::local(std::any::type_name::<A>().to_string(), id));

        let (runtime, mailbox_sender) = SupervisorRuntime::construct(actor);
        Supervisor {
            path,
            sender: mailbox_sender,
            state: Uninitialized { runtime },
        }
    }

    fn start_within(self, system: System) -> Supervisor<A, Initialized> {
        let id = self.path.instance_id.clone();
        let supervisor = Supervisor {
            path: self.path,
            sender: self.sender,
            state: Initialized {},
        };
        let runtime = self.state.runtime;

        system
            .context()
            .write()
            .register(id.clone(), supervisor.clone());
        let local_endpoint = supervisor.endpoint();

        let ctx = Context::new(local_endpoint);

        tokio::spawn(async move {
            ACTIVE_SYSTEM
                .scope(system, async move {
                    runtime.run(id, ctx).await;
                })
                .await;
        });
        supervisor
    }
}

impl<A> SystemSupervisor for Supervisor<A, Initialized>
where
    A: Actor,
{
    fn id(&self) -> Arc<Id> {
        self.path.instance_id.clone()
    }

    fn shutdown(&mut self) {
        let _ = self
            .sender
            .send_blocking(QoSLevel::Supervisor, SupervisorCommand::Shutdown);
    }
}

/// A handle to an actor that allows sending messages to it.
///
/// Endpoints are the primary way to interact with actors:
/// - They are cloneable and can be shared across threads
/// - They provide methods to send messages with different QoS levels
/// - They handle message delivery and response routing
///
/// # Example
/// ```
/// let result = endpoint.send(MyMessage { data: 42 }).await?;
/// ```
pub struct Endpoint<A>
where
    A: Actor,
{
    sender: EndpointSender<A>,
    path: Arc<ActorPath>,
}

impl<A: Actor> Endpoint<A> {
    fn new(sender: EndpointSender<A>, path: Arc<ActorPath>) -> Self {
        Self { sender, path }
    }

    #[cfg(test)]
    pub fn direct(
        path: Arc<ActorPath>,
        actor: Arc<parking_lot::Mutex<A>>,
        ctx: Arc<parking_lot::Mutex<Context<A>>>,
    ) -> Self {
        let sender = EndpointSender::new_direct(ctx, actor);
        Self { sender, path }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub async fn send_in_background<M>(&self, message: M) -> Result<(), ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.background_send(message)
    }

    pub async fn send<M>(&self, message: M) -> Result<M::Result, ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.send(message).await
    }
}

impl<A: Actor> PartialEq for Endpoint<A> {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl<A: Actor> Clone for Endpoint<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            path: self.path.clone(),
        }
    }
}

impl<A: Actor> Debug for Endpoint<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("type", &std::any::type_name::<A>())
            .finish()
    }
}

/// Type-erased version of an Endpoint that can hold any actor type
pub struct AnyEndpoint {
    // Box containing the type-erased endpoint
    endpoint: Box<dyn Any + Send + Sync>,
    // Type ID for runtime type checking during downcasting
    type_id: TypeId,
    // Type name for better debugging and error messages
    type_name: &'static str,
    // Actor's path in the system
    path: Arc<ActorPath>,
}

impl<A: Actor> From<Endpoint<A>> for AnyEndpoint {
    fn from(endpoint: Endpoint<A>) -> Self {
        Self {
            endpoint: Box::new(endpoint.clone()),
            type_id: TypeId::of::<A>(),
            type_name: std::any::type_name::<A>(),
            path: endpoint.path.clone(),
        }
    }
}

impl Debug for AnyEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyEndpoint")
            .field("type_id", &format!("{:?}", self.type_id))
            .field("type_name", &self.type_name)
            .finish()
    }
}

impl AnyEndpoint {
    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type
    pub fn downcast<A: Actor + 'static>(&self) -> Option<&Endpoint<A>> {
        if self.type_id == TypeId::of::<A>() {
            self.endpoint.downcast_ref::<Endpoint<A>>()
        } else {
            None
        }
    }

    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type, consuming self
    pub fn into_downcast<A: Actor + 'static>(self) -> Option<Endpoint<A>> {
        if self.type_id == TypeId::of::<A>() {
            self.endpoint.downcast::<Endpoint<A>>().ok().map(|b| *b)
        } else {
            None
        }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }
}

struct EndpointSender<A: Actor> {
    send_fn: Arc<
        Box<
            dyn Fn(
                    Envelope<A>,
                ) -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), ActorError>> + Send>,
                > + Send
                + Sync,
        >,
    >,
}

impl<A: Actor> EndpointSender<A> {
    fn new(sender: MailboxSender<SupervisorCommand<A>>) -> Self {
        let send_fn = Box::new(move |envelope| {
            let sender = sender.clone();
            Box::pin(async move {
                let cmd = SupervisorCommand::Envelope(envelope);
                sender
                    .send(QoSLevel::Normal, cmd)
                    .await
                    .map_err(|_| ActorError::MailboxClosed)
            }) as Pin<Box<dyn Future<Output = Result<(), ActorError>> + Send>>
        });
        Self {
            send_fn: Arc::new(send_fn),
        }
    }

    /// Creates a new endpoint sender that directly calls the actor's handle method
    ///
    /// This is used for testing purposes to avoid the mailbox and supervisor layer
    /// and cannot be used in production code.
    #[cfg(test)]
    fn new_direct(
        ctx: Arc<parking_lot::Mutex<Context<A>>>,
        actor: Arc<parking_lot::Mutex<A>>,
    ) -> Self {
        let send_fn = Box::new(move |mut envelope: Envelope<A>| {
            let mut ctx = ctx.lock();
            let mut actor = actor.lock();
            envelope.0.handle(&mut *ctx, &mut *actor);

            Box::pin(async { Ok(()) })
                as Pin<Box<dyn Future<Output = Result<(), ActorError>> + Send>>
        });
        Self {
            send_fn: Arc::new(send_fn),
        }
    }

    /// Sends a message to the actor without waiting for a response
    pub fn background_send<M>(&self, msg: M) -> Result<(), ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let (tx, _) = oneshot::channel();
        let envelope = Envelope::new(msg, tx);
        let fut = (self.send_fn)(envelope);
        tokio::spawn(fut);
        Ok(())
    }

    pub async fn send<M>(&self, msg: M) -> Result<M::Result, ActorError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = Envelope::new(msg, tx);

        if let Err(_) = (self.send_fn)(envelope).await {
            return Err(ActorError::MailboxClosed);
        }

        Ok(rx.await.map_err(|_| ActorError::ResponseDropped)?)
    }
}

impl<A: Actor> Clone for EndpointSender<A> {
    fn clone(&self) -> Self {
        Self {
            send_fn: self.send_fn.clone(),
        }
    }
}
