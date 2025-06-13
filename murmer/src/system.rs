//! Actor system implementation providing actor lifecycle management and message routing.
//!
//! This module contains the core actor system implementation, which is responsible for:
//! - Managing actor lifecycles (creation, supervision, shutdown)
//! - Message routing between actors
//! - System-wide coordination and configuration
//! - Actor supervision hierarchies
use parking_lot::RwLock;
use std::any::Any;
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
use super::receptionist::{EndpointFactory, *};

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
    /// Cancellation token for the system
    cancellation: tokio_util::sync::CancellationToken,
    /// Registry of all active actor supervisors
    supervisors: HashMap<Arc<Id>, Box<dyn SystemSupervisor>>,
}

impl SystemContext {
    /// Creates a new system context with a unique system ID
    fn new() -> Self {
        let id = NEXT_SYSTEM_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            id,
            cancellation: tokio_util::sync::CancellationToken::new(),
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
    async fn shutdown(&mut self) {
        self.cancellation.cancel();
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
        root_cancellation: tokio_util::sync::CancellationToken,
        context: Arc<RwLock<SystemContext>>,
        receptionist: Receptionist,
    },
    Clustered {
        cluster: cluster::Cluster,
        root_cancellation: tokio_util::sync::CancellationToken,
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
            root_cancellation: tokio_util::sync::CancellationToken::new(),
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
            root_cancellation: tokio_util::sync::CancellationToken::new(),
            context,
            receptionist,
        };

        receptionist_supervisor.start_within(system.clone());
        cluster_supervisor.start_within(system.clone());

        Ok(system)
    }

    pub fn receptionist_ref(&self) -> &Receptionist {
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

    fn context(&self) -> &Arc<RwLock<SystemContext>> {
        match self {
            Self::Local { context, .. } => context,
            Self::Clustered { context, .. } => context,
        }
    }

    fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        match self {
            Self::Local {
                root_cancellation, ..
            } => root_cancellation.child_token(),
            Self::Clustered {
                root_cancellation, ..
            } => root_cancellation.child_token(),
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

    pub async fn spawn_registered_with<A: Actor>(
        &self,
        actor: A,
    ) -> Result<Endpoint<A>, SystemError> {
        let endpoint = self.spawn_with(actor)?;
        self.receptionist_ref()
            .register_local(endpoint.path().clone(), endpoint.clone().into())
            .await
            .map_err(|_| {
                tracing::error!("Failed to register actor with receptionist");
                SystemError::SpawnError
            })?;
        Ok(endpoint)
    }

    pub async fn shutdown(&mut self) {
        match self {
            Self::Local {
                root_cancellation, ..
            } => root_cancellation.cancel(),
            Self::Clustered {
                root_cancellation, ..
            } => root_cancellation.cancel(),
        }
    }

    /// Creates a new subsystem that inherits properties from the parent system.
    ///
    /// This is useful for creating actor hierarchies with different supervision
    /// scopes while still maintaining a connection to the parent system.
    pub fn create_subsystem(&self) -> Self {
        match self {
            Self::Local {
                name,
                root_cancellation,
                context: _,
                receptionist,
            } => {
                let subsystem_name = format!("{}/subsystem-{}", name, self.id());
                let child_cancellation = root_cancellation.child_token();
                let context = Arc::new(RwLock::new(SystemContext::new()));

                Self::Local {
                    name: subsystem_name,
                    root_cancellation: child_cancellation,
                    context,
                    receptionist: receptionist.clone(),
                }
            }
            Self::Clustered {
                cluster,
                root_cancellation,
                context: _,
                receptionist,
            } => {
                let child_cancellation = root_cancellation.child_token();
                let context = Arc::new(RwLock::new(SystemContext::new()));

                Self::Clustered {
                    cluster: cluster.clone(),
                    root_cancellation: child_cancellation,
                    context,
                    receptionist: receptionist.clone(),
                }
            }
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
    Message(Envelope<A>),
}

impl<A> std::fmt::Debug for SupervisorCommand<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupervisorCommand::Message(_) => write!(f, "SupervisorCommand::Message"),
        }
    }
}

struct SupervisorRuntime<A>
where
    A: Actor,
{
    actor: A,
    path: Arc<ActorPath>,
    mailbox: PrioritizedMailbox<SupervisorCommand<A>>,
}

impl<A> SupervisorRuntime<A>
where
    A: Actor,
{
    pub fn construct(
        path: Arc<ActorPath>,
        actor: A,
    ) -> (Self, MailboxSender<SupervisorCommand<A>>) {
        let (tx, rx) = mpsc::channel(1024);
        let mailbox_sender = MailboxSender::new(tx);
        let runtime = SupervisorRuntime {
            actor,
            path,
            mailbox: PrioritizedMailbox::new(rx),
        };
        (runtime, mailbox_sender)
    }

    fn actor_ref(&self) -> &A {
        &self.actor
    }

    fn actor_ref_mut(&mut self) -> &mut A {
        &mut self.actor
    }

    #[tracing::instrument(level = "trace", skip(ctx))]
    async fn handle_command(&mut self, ctx: &mut Context<A>, cmd: SupervisorCommand<A>) -> bool {
        match cmd {
            SupervisorCommand::Message(envelope) => {
                tracing::trace!("handling message");
                let mut handler = envelope.0;
                handler.handle_async(ctx, &mut self.actor).await;
                true
            }
        }
    }

    async fn start(&mut self, ctx: &mut Context<A>) {
        self.actor.started(ctx).await;
    }

    #[tracing::instrument(skip(endpoint, cancellation))]
    async fn run(
        mut self,
        endpoint: Endpoint<A>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        tracing::info!("starting supervisor loop");
        let mut ctx = Context::new(endpoint, cancellation.clone());
        let cancellation = ctx.inner_cancellation();
        tracing::debug!("starting supervisor");
        self.actor.started(&mut ctx).await;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    tracing::debug!("stopping supervisor loop");
                    self.actor.stopping(&mut ctx).await;
                    break;
                }
                _ = self.tick(&mut ctx) => {
                    // Continue processing messages
                }
            }
        }
        self.actor.stopped(&mut ctx).await;
    }

    #[tracing::instrument(level = "trace", skip(ctx))]
    async fn tick(&mut self, ctx: &mut Context<A>) {
        if let Some(cmd) = self.mailbox.recv().await {
            tracing::trace!(?cmd, "command received");
            self.handle_command(ctx, cmd).await;
        };
    }
}

impl<A> std::fmt::Debug for SupervisorRuntime<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorRuntime")
            .field("path", &self.path)
            .finish()
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
    cancellation: tokio_util::sync::CancellationToken,
}

#[cfg(test)]
impl<A> TestSupervisor<A>
where
    A: Actor,
{
    pub fn new(actor: A) -> Self {
        let id = Id::new();
        let path = Arc::new(ActorPath::local_default(A::ACTOR_TYPE_KEY.to_string(), id));
        let (runtime, sender) = SupervisorRuntime::construct(path.clone(), actor);
        let cancellation = tokio_util::sync::CancellationToken::new();

        let endpoint = Endpoint::new(EndpointSender::from_sender(sender.clone()), path.clone());
        let ctx = Context::new(endpoint, cancellation.clone());

        Self {
            path,
            ctx,
            runtime,
            sender,
            cancellation,
        }
    }

    pub fn actor_ref(&self) -> &A {
        self.runtime.actor_ref()
    }

    pub fn actor_ref_mut(&mut self) -> &mut A {
        self.runtime.actor_ref_mut()
    }

    pub async fn started(&mut self) {
        self.runtime.actor.started(&mut self.ctx).await;
    }

    pub async fn send<M>(&mut self, system: &System, msg: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let sender = EndpointSender::from_sender(self.sender.clone());
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

    pub async fn tick(&mut self, system: System, timeout: Option<std::time::Duration>) -> bool {
        let timeout_duration = timeout.unwrap_or(std::time::Duration::from_millis(500));
        ACTIVE_SYSTEM
            .scope(system, async move {
                tokio::time::timeout(timeout_duration, self.runtime.tick(&mut self.ctx)).await
            })
            .await
            .is_ok()
    }

    pub async fn stopping(&mut self) {
        self.runtime.actor.stopping(&mut self.ctx).await;
    }

    pub async fn stopped(&mut self) {
        self.runtime.actor.stopped(&mut self.ctx).await;
    }
}

trait State {}

pub struct Uninitialized<A: Actor> {
    runtime: SupervisorRuntime<A>,
}

impl<A: Actor> State for Uninitialized<A> {}

#[derive(Clone)]
struct Initialized {
    cancellation: tokio_util::sync::CancellationToken,
}

impl State for Initialized {}

pub struct Supervisor<A, S>
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

impl<A: Actor> From<Supervisor<A, Initialized>> for Endpoint<A> {
    fn from(val: Supervisor<A, Initialized>) -> Self {
        let sender = EndpointSender::from_sender(val.sender);
        Endpoint::new(sender, val.path)
    }
}

impl<A, S> Supervisor<A, S>
where
    A: Actor,
    S: State,
{
    pub fn endpoint(&self) -> Endpoint<A> {
        let sender = EndpointSender::from_sender(self.sender.clone());
        Endpoint::new(sender, self.path.clone())
    }
}

impl<A> Supervisor<A, Uninitialized<A>>
where
    A: Actor,
{
    pub fn construct(actor: A) -> Supervisor<A, Uninitialized<A>> {
        let id = Id::new();
        // TODO: make it so every actor has a identifier string.
        let path = Arc::new(ActorPath::local_default(A::ACTOR_TYPE_KEY.to_string(), id));

        let (runtime, mailbox_sender) = SupervisorRuntime::construct(path.clone(), actor);
        Supervisor {
            path,
            sender: mailbox_sender,
            state: Uninitialized { runtime },
        }
    }

    fn start_within(self, system: System) -> Supervisor<A, Initialized> {
        let cancellation = system.cancellation();
        let id = self.path.instance_id.clone();
        let supervisor = Supervisor {
            path: self.path,
            sender: self.sender,
            state: Initialized {
                cancellation: cancellation.clone(),
            },
        };
        let runtime = self.state.runtime;

        system
            .context()
            .write()
            .register(id.clone(), supervisor.clone());

        let local_endpoint = supervisor.endpoint();

        tokio::spawn(async move {
            ACTIVE_SYSTEM
                .scope(system, async move {
                    runtime.run(local_endpoint, cancellation).await;
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
        self.state.cancellation.cancel();
    }
}

/// Local-only endpoint that bypasses SenderKind enum to avoid recursive types
pub struct LocalEndpoint<A: Actor> {
    sender: LocalEndpointSender<A>,
    path: Arc<ActorPath>,
}

impl<A: Actor> LocalEndpoint<A> {
    /// Create a new local endpoint
    pub fn new(sender: LocalEndpointSender<A>, path: Arc<ActorPath>) -> Self {
        Self { sender, path }
    }

    /// Get the actor path
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Send a message and wait for response
    async fn send<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.send(message).await
    }

    /// Send a message without waiting for response
    async fn send_in_background<M>(&self, message: M) -> Result<(), SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.background_send(message)
    }
}

impl<A: Actor> Clone for LocalEndpoint<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            path: self.path.clone(),
        }
    }
}

impl<A: Actor> LocalEndpoint<A> {
    /// Convert a regular Endpoint to LocalEndpoint (assumes it's local)
    /// This will panic if the endpoint contains a remote sender
    pub fn from_endpoint(endpoint: Endpoint<A>) -> Self {
        match endpoint.sender.kind {
            SenderKind::Local { send_fn } => {
                let sender = LocalEndpointSender { send_fn };
                LocalEndpoint::new(sender, endpoint.path)
            }
            SenderKind::Remote { .. } => {
                panic!("Cannot convert remote endpoint to LocalEndpoint")
            }
        }
    }
}

/// Local-only endpoint sender that directly wraps mailbox communication
pub struct LocalEndpointSender<A: Actor> {
    send_fn: Arc<
        Box<
            dyn Fn(Envelope<A>) -> Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
                + Send
                + Sync,
        >,
    >,
}

impl<A: Actor> LocalEndpointSender<A> {
    /// Create from a mailbox sender (supervisor communication)
    pub fn from_sender(sender: MailboxSender<SupervisorCommand<A>>) -> Self {
        let send_fn = Box::new(move |envelope| {
            let sender = sender.clone();
            Box::pin(async move {
                let cmd = SupervisorCommand::Message(envelope);
                sender
                    .send(QoSLevel::Normal, cmd)
                    .await
                    .map_err(|_| SendError::MailboxClosed)
            }) as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            send_fn: Arc::new(send_fn),
        }
    }

    /// Create from a direct channel
    fn from_channel(tx: mpsc::Sender<Envelope<A>>) -> Self {
        let send_fn = Box::new(move |envelope| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(envelope)
                    .await
                    .map_err(|_| SendError::MailboxClosed)
            }) as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            send_fn: Arc::new(send_fn),
        }
    }

    /// Send a message and wait for response
    async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = Envelope::new(msg, tx);

        if let Err(_) = (self.send_fn)(envelope).await {
            return Err(SendError::MailboxClosed);
        }

        rx.await.map_err(|_| SendError::ResponseDropped)
    }

    /// Send a message without waiting for response
    fn background_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let envelope = Envelope::no_response(msg);
        let fut = (self.send_fn)(envelope);
        tokio::spawn(fut);
        Ok(())
    }
}

impl<A: Actor> Clone for LocalEndpointSender<A> {
    fn clone(&self) -> Self {
        Self {
            send_fn: self.send_fn.clone(),
        }
    }
}

/// Remote proxy for handling actor communication across network boundaries
pub struct RemoteProxy {
    /// Node managing the connection (always local)
    node: crate::node::Node,
    /// Path to the remote actor
    actor_path: ActorPath,
}

impl RemoteProxy {
    pub fn new(node: crate::node::Node, actor_path: ActorPath) -> Self {
        Self { node, actor_path }
    }

    /// Send a message to the remote actor with type safety
    pub async fn send<A, M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Actor + Handler<M>,
        M: Message + 'static,
    {
        // TODO: Implement actual remote message sending
        // This will involve:
        // 1. Serializing the message M
        // 2. Sending it through the NodeActor connection
        // 3. Waiting for the response
        // 4. Deserializing the response back to M::Result

        // For now, return an error indicating this is not implemented
        Err(SendError::MailboxClosed)
    }

    /// Send a message to the remote actor without waiting for response
    pub fn send_background<A, M>(&self, msg: M) -> Result<(), SendError>
    where
        A: Actor + Handler<M>,
        M: Message + 'static,
    {
        // TODO: Implement actual remote background message sending
        // Similar to send() but without waiting for response

        // For now, return an error indicating this is not implemented
        Err(SendError::MailboxClosed)
    }
}

impl Clone for RemoteProxy {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            actor_path: self.actor_path.clone(),
        }
    }
}

/// Factory for creating remote actor endpoints on demand
pub struct RemoteEndpointFactory {
    /// Node managing the connection
    node: crate::node::Node,
    /// Path to the remote actor
    actor_path: ActorPath,
}

impl RemoteEndpointFactory {
    pub fn new(node: crate::node::Node, actor_path: ActorPath) -> Self {
        Self { node, actor_path }
    }
}

impl EndpointFactory for RemoteEndpointFactory {
    fn create(&self) -> AnyEndpoint {
        // Create AnyEndpoint::Remote variant with factory data for on-demand proxy creation
        AnyEndpoint::Remote {
            node: self.node.clone(),
            path: self.actor_path.clone(),
        }
    }

    fn clone_factory(&self) -> Box<dyn EndpointFactory> {
        Box::new(RemoteEndpointFactory {
            node: self.node.clone(),
            actor_path: self.actor_path.clone(),
        })
    }
}

/// Enum representing different ways to send messages to actors
enum SenderKind<A: Actor> {
    /// Local actor using existing supervisor/mailbox infrastructure
    Local {
        send_fn: Arc<
            Box<
                dyn Fn(
                        Envelope<A>,
                    ) -> std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<(), SendError>> + Send>,
                    > + Send
                    + Sync,
            >,
        >,
    },
    /// Remote actor using network communication
    Remote { proxy: RemoteProxy },
}

/// A handle to an actor that allows sending messages to it.
///
/// Endpoints are the primary way to interact with actors:
/// - They are cloneable and can be shared across threads
/// - They provide methods to send messages with different QoS levels
/// - They handle message delivery and response routing
/// - Support both local and remote actors transparently
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
    pub(crate) fn new(sender: EndpointSender<A>, path: Arc<ActorPath>) -> Self {
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

    pub async fn send_in_background<M>(&self, message: M) -> Result<(), SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.background_send(message)
    }

    pub async fn send<M>(&self, message: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        self.sender.send(message).await
    }

    pub fn recepient_of<M>(&self) -> RecepientOf<M>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        RecepientOf::new(Box::new(self.sender.clone()))
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
            .field("path", &self.path)
            .finish()
    }
}

/// Trait for endpoints that can be downcast back to their concrete type
pub trait DowncastableEndpoint: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn clone_box(&self) -> Box<dyn DowncastableEndpoint>;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl Clone for Box<dyn DowncastableEndpoint> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl<A: Actor> DowncastableEndpoint for Endpoint<A> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn DowncastableEndpoint> {
        Box::new(self.clone())
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Trait for message senders that can be cloned
pub trait CloneableSender: Send + Sync + 'static {
    fn clone_box(&self) -> Box<dyn CloneableSender>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl Clone for Box<dyn CloneableSender> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl<A: Actor> CloneableSender for EndpointSender<A> {
    fn clone_box(&self) -> Box<dyn CloneableSender> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Type-erased version of an Endpoint that can hold any actor type
/// Uses hybrid storage: concrete endpoints for local actors, factory data for remote actors
#[derive(Clone)]
pub enum AnyEndpoint {
    /// Local actor with concrete endpoint stored
    Local {
        endpoint: Box<dyn DowncastableEndpoint>,
        path: Arc<ActorPath>,
    },
    /// Remote actor with factory data for on-demand creation
    Remote {
        node: crate::node::Node,
        path: ActorPath,
    },
}

impl<A: Actor> From<Endpoint<A>> for AnyEndpoint {
    fn from(endpoint: Endpoint<A>) -> Self {
        AnyEndpoint::Local {
            endpoint: Box::new(endpoint.clone()),
            path: endpoint.path,
        }
    }
}

impl Debug for AnyEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyEndpoint::Local { path, .. } => f
                .debug_struct("AnyEndpoint::Local")
                .field("actor_key", &path.type_id)
                .field("path", path)
                .finish(),
            AnyEndpoint::Remote { path, node } => f
                .debug_struct("AnyEndpoint::Remote")
                .field("actor_key", &path.type_id)
                .field("path", path)
                .field("node_id", &node.id)
                .finish(),
        }
    }
}

impl AnyEndpoint {
    /// Checks if this AnyEndpoint contains an Endpoint of type T
    ///
    /// This method allows checking if the AnyEndpoint can be downcast to a specific actor type.
    /// It compares the path's type_id with the ACTOR_TYPE_KEY of the requested actor type.
    /// This is useful for generic type checking before attempting to downcast.
    pub fn is<T: Actor>(&self) -> bool {
        match self {
            AnyEndpoint::Local { path, .. } => path.type_id.as_ref() == T::ACTOR_TYPE_KEY,
            AnyEndpoint::Remote { path, .. } => path.type_id.as_ref() == T::ACTOR_TYPE_KEY,
        }
    }

    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type
    pub fn downcast<A: Actor + 'static>(&self) -> Option<Endpoint<A>> {
        match self {
            AnyEndpoint::Local { endpoint, path } => {
                if path.type_id.as_ref() == A::ACTOR_TYPE_KEY {
                    endpoint
                        .as_any()
                        .downcast_ref::<Endpoint<A>>()
                        .map(|e| e.clone())
                } else {
                    None
                }
            }
            AnyEndpoint::Remote { node, path } => {
                if path.type_id.as_ref() == A::ACTOR_TYPE_KEY {
                    node.create_remote_proxy(path.clone()).ok()
                } else {
                    None
                }
            }
        }
    }

    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type, consuming self
    pub fn into_downcast<A: Actor + 'static>(self) -> Option<Endpoint<A>> {
        match self {
            AnyEndpoint::Local { endpoint, path } => {
                if path.type_id.as_ref() == A::ACTOR_TYPE_KEY {
                    let any_box = endpoint.into_any();
                    any_box.downcast::<Endpoint<A>>().ok().map(|boxed| *boxed)
                } else {
                    None
                }
            }
            AnyEndpoint::Remote { node, path } => {
                if path.type_id.as_ref() == A::ACTOR_TYPE_KEY {
                    node.create_remote_proxy(path).ok()
                } else {
                    None
                }
            }
        }
    }

    pub fn path(&self) -> &ActorPath {
        match self {
            AnyEndpoint::Local { path, .. } => path,
            AnyEndpoint::Remote { path, .. } => path,
        }
    }
}

pub(crate) struct EndpointSender<A: Actor> {
    kind: SenderKind<A>,
}

impl<A: Actor> EndpointSender<A> {
    pub(crate) fn from_sender(sender: MailboxSender<SupervisorCommand<A>>) -> Self {
        let send_fn = Box::new(move |envelope| {
            let sender = sender.clone();
            Box::pin(async move {
                let cmd = SupervisorCommand::Message(envelope);
                sender
                    .send(QoSLevel::Normal, cmd)
                    .await
                    .map_err(|_| SendError::MailboxClosed)
            }) as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            kind: SenderKind::Local {
                send_fn: Arc::new(send_fn),
            },
        }
    }

    pub(crate) fn from_remote_proxy(proxy: RemoteProxy) -> Self {
        Self {
            kind: SenderKind::Remote { proxy },
        }
    }

    pub(crate) fn from_channel(tx: mpsc::Sender<Envelope<A>>) -> Self {
        let send_fn = Box::new(move |envelope| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(envelope)
                    .await
                    .map_err(|_| SendError::MailboxClosed)
            }) as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            kind: SenderKind::Local {
                send_fn: Arc::new(send_fn),
            },
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
            let fut = envelope.0.handle_async(&mut *ctx, &mut *actor);
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut));

            Box::pin(async { Ok(()) })
                as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            kind: SenderKind::Local {
                send_fn: Arc::new(send_fn),
            },
        }
    }

    /// Sends a message to the actor without waiting for a response
    pub fn background_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        match &self.kind {
            SenderKind::Local { send_fn } => {
                let envelope = Envelope::no_response(msg);
                let fut = (send_fn)(envelope);
                tokio::spawn(fut);
                Ok(())
            }
            SenderKind::Remote { proxy } => proxy.send_background::<A, M>(msg),
        }
    }

    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        match &self.kind {
            SenderKind::Local { send_fn } => {
                let (tx, rx) = oneshot::channel();
                let envelope = Envelope::new(msg, tx);

                if ((send_fn)(envelope).await).is_err() {
                    return Err(SendError::MailboxClosed);
                }

                rx.await.map_err(|_| SendError::ResponseDropped)
            }
            SenderKind::Remote { proxy } => proxy.send::<A, M>(msg).await,
        }
    }
}

/// Trait for message senders that can handle specific message types
#[async_trait::async_trait]
pub trait ErasedEndpointSender: Send + Sync + 'static {
    /// Clones the sender into a new boxed trait object
    fn clone_box(&self) -> Box<dyn ErasedEndpointSender>;

    /// Converts the sender to a trait object that can be downcast
    fn as_any(&self) -> &dyn Any;

    /// Converts the sender to a mutable trait object that can be downcast
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Consumes the boxed sender and converts it to a boxed Any
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<A: Actor> ErasedEndpointSender for EndpointSender<A> {
    fn clone_box(&self) -> Box<dyn ErasedEndpointSender> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl<A: Actor> Clone for EndpointSender<A> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
        }
    }
}

impl<A: Actor> Clone for SenderKind<A> {
    fn clone(&self) -> Self {
        match self {
            SenderKind::Local { send_fn } => SenderKind::Local {
                send_fn: send_fn.clone(),
            },
            SenderKind::Remote { proxy } => SenderKind::Remote {
                proxy: proxy.clone(),
            },
        }
    }
}

#[async_trait::async_trait]
impl<A: Actor, M: Message> MessageSender<M> for EndpointSender<A>
where
    A: Handler<M>,
    M::Result: Send,
    M: Message + Send + 'static,
{
    async fn send(&self, msg: M) -> Result<M::Result, SendError> {
        self.send(msg).await
    }

    async fn send_no_response(&self, msg: M) -> Result<(), SendError> {
        self.background_send(msg)
    }
}
