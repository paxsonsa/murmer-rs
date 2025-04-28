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
        let path = Arc::new(ActorPath::local_default(std::any::type_name::<A>().to_string(), id));
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
}

trait State {}

struct Uninitialized<A: Actor> {
    runtime: SupervisorRuntime<A>,
}

impl<A: Actor> State for Uninitialized<A> {}

#[derive(Clone)]
struct Initialized {
    cancellation: tokio_util::sync::CancellationToken,
}

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
    fn endpoint(&self) -> Endpoint<A> {
        let sender = EndpointSender::from_sender(self.sender.clone());
        Endpoint::new(sender, self.path.clone())
    }
}

impl<A> Supervisor<A, Uninitialized<A>>
where
    A: Actor,
{
    fn construct(actor: A) -> Supervisor<A, Uninitialized<A>> {
        let id = Id::new();
        let path = Arc::new(ActorPath::local_default(std::any::type_name::<A>().to_string(), id));

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

/// Type-erased version of an Endpoint that can hold any actor type
pub struct AnyEndpoint {
    // Box containing the type-erased endpoint sender
    pub endpoint_sender: Box<dyn Any + Send + Sync>,
    // Type ID for runtime type checking during downcasting
    pub type_id: TypeId,
    // Type name for better debugging and error messages
    pub type_name: &'static str,
    // Actor's path in the system
    pub path: Arc<ActorPath>,
    // Optional string identifier for remote actor types
    pub actor_type_string: Option<String>,
}

impl<A: Actor> From<Endpoint<A>> for AnyEndpoint {
    fn from(endpoint: Endpoint<A>) -> Self {
        Self {
            endpoint_sender: Box::new(endpoint.sender.clone()),
            type_id: TypeId::of::<A>(),
            type_name: std::any::type_name::<A>(),
            path: endpoint.path.clone(),
            actor_type_string: None,
        }
    }
}

impl AnyEndpoint {
    /// Creates a new AnyEndpoint from a remote actor
    pub fn from_remote<A: Actor>(path: ActorPath, proxy: Box<dyn Any + Send + Sync>) -> Self {
        let type_string = match path.is_remote() {
            true => {
                // For remote actors, use the type_id field from ActorPath
                Some(path.type_id.to_string())
            }
            false => None,
        };

        Self {
            endpoint_sender: proxy,
            type_id: TypeId::of::<A>(),
            type_name: std::any::type_name::<A>(),
            path: Arc::new(path),
            actor_type_string: type_string,
        }
    }
}

impl Debug for AnyEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyEndpoint")
            .field("type_id", &format!("{:?}", self.type_id))
            .field("type_name", &self.type_name)
            .field("actor_type", &self.actor_type_string)
            .finish()
    }
}

impl AnyEndpoint {
    /// Private method to downcast only by TypeId, without attempting remote resolution
    fn direct_downcast<A: Actor + 'static>(&self) -> Option<Endpoint<A>> {
        if self.type_id == TypeId::of::<A>() {
            return self.endpoint_sender
                .downcast_ref::<EndpointSender<A>>()
                .map(|b| {
                    let sender = b.clone();
                    Endpoint::new(sender, self.path.clone())
                });
        }
        None
    }

    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type
    pub fn downcast<A: Actor + 'static>(&self) -> Option<Endpoint<A>> {
        // For local actors - try direct TypeId-based downcasting
        if self.path.is_local() {
            return self.direct_downcast::<A>();
        }
        
        // For remote actors - use the actor_type_string with inventory lookup
        if self.path.is_remote() {
            // First try direct downcast in case the types match
            if let Some(endpoint) = self.direct_downcast::<A>() {
                return Some(endpoint);
            }
            
            // Check if A implements RegisteredActor first
            if let Some(receptionist_key) = self.actor_type_string.as_ref() {
                // Search the inventory for a matching registration
                for registration in inventory::iter::<crate::remote::ActorTypeRegistration>() {
                    if registration.key == receptionist_key {
                        // For testing purposes, we'll create a dummy channel
                        // In a real implementation, this would come from somewhere else
                        let (proxy_tx, _) = tokio::sync::mpsc::channel(32);
                        
                        // Found matching registration, create typed endpoint using the proxy channel
                        let any_endpoint = (registration.create_endpoint)((*self.path).clone(), proxy_tx);
                        
                        // Use direct downcast to prevent infinite recursion
                        return any_endpoint.direct_downcast::<A>();
                    }
                }
            }
        }
        
        None
    }

    /// Private method to downcast only by TypeId, without attempting remote resolution
    fn direct_into_downcast<A: Actor + 'static>(self) -> Option<Endpoint<A>> {
        if self.type_id == TypeId::of::<A>() {
            return self.endpoint_sender
                .downcast::<EndpointSender<A>>()
                .ok()
                .map(|b| {
                    let sender = *b;
                    Endpoint::new(sender, self.path.clone())
                });
        }
        None
    }

    /// Attempt to downcast the AnyEndpoint back to a specific Endpoint<A> type, consuming self
    pub fn into_downcast<A: Actor + 'static>(self) -> Option<Endpoint<A>> {
        // For local actors - try direct TypeId-based downcasting
        if self.path.is_local() {
            return self.direct_into_downcast::<A>();
        }
        
        // For remote actors - use the actor_type_string with inventory lookup
        if self.path.is_remote() {
            // First try direct downcast in case the types match
            if self.type_id == TypeId::of::<A>() {
                return self.direct_into_downcast::<A>();
            }
            
            // Extract necessary information before moving self
            let actor_path = self.path.clone();
            let actor_type_string = self.actor_type_string;
            
            // Check if A implements RegisteredActor first
            if let Some(receptionist_key) = actor_type_string {
                // Search the inventory for a matching registration
                for registration in inventory::iter::<crate::remote::ActorTypeRegistration>() {
                    if registration.key == receptionist_key {
                        // Clone path for each iteration
                        let path_clone = (*actor_path).clone();
                        
                        // For testing purposes, we'll create a dummy channel
                        // In a real implementation, this would come from somewhere else
                        let (proxy_tx, _) = tokio::sync::mpsc::channel(32);
                        
                        // Found matching registration, create typed endpoint
                        let any_endpoint = (registration.create_endpoint)(path_clone, proxy_tx);
                        
                        // Use direct downcast to prevent infinite recursion
                        let endpoint_result = any_endpoint.direct_downcast::<A>();
                        if let Some(endpoint) = endpoint_result {
                            return Some(endpoint);
                        }
                    }
                }
            }
        }
        
        None
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn as_recepient_of<M>(&self) -> Option<RecepientOf<M>>
    where
        M: Message + 'static,
    {
        // For local actors - try direct downcasting
        if !self.path.is_remote() {
            if let Some(sender) = self.endpoint_sender
                .downcast_ref::<Box<dyn MessageSender<M>>>()
                .map(|b| (*b).clone())
            {
                return Some(RecepientOf::new(sender));
            }
        }
        
        // For remote actors, we just return None for now until we have a better design
        // In the future, we'll need to implement a more robust system for remote actor
        // communication that can handle type erasure and the RemoteMessageType trait
        
        // Comment explaining future direction:
        // To handle remote actors properly, we'd need to:
        // 1. Get a proxy channel from the actor registry
        // 2. Verify that M implements RemoteMessageType 
        // 3. Create a TypedRemoteEndpointSender<M>
        // 4. Return it wrapped in RecepientOf
        
        None
    }
}

pub(crate) struct EndpointSender<A: Actor> {
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
            send_fn: Arc::new(send_fn),
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
            let fut = envelope.0.handle_async(&mut *ctx, &mut *actor);
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut));

            Box::pin(async { Ok(()) })
                as Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>>
        });
        Self {
            send_fn: Arc::new(send_fn),
        }
    }

    /// Sends a message to the actor without waiting for a response
    pub fn background_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Message + 'static,
        A: Handler<M>,
    {
        let envelope = Envelope::no_response(msg);
        let fut = (self.send_fn)(envelope);
        tokio::spawn(fut);
        Ok(())
    }

    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
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
}

impl<A: Actor> Clone for EndpointSender<A> {
    fn clone(&self) -> Self {
        Self {
            send_fn: self.send_fn.clone(),
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
