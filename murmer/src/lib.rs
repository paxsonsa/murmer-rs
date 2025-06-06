mod actor;
mod cluster;
mod context;
mod id;
mod mailbox;
mod message;
mod net;
mod node;
mod path;
pub mod prelude;
mod receptionist;
mod remote;
mod system;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod test_utils_example;
mod tls;

// TODO: Add clustering functionality for both local and remote actors
// TODO: Implement receptionist auto register traits for actors
// TODO: Add shutdown/restart functionality to actors.
// TODO: Create macro system for defining actors and messages.

#[cfg(test)]
mod tests {
    use crate::message::RemoteMessageError;
    use crate::system::SystemId;
    use crate::tls::TlsConfig;

    use super::prelude::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    /// Initialize tracing for tests
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("murmer=debug".parse().unwrap())
                    .add_directive("test=debug".parse().unwrap()),
            )
            .with_span_events(FmtSpan::FULL)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_target(false)
            .try_init();
    }

    struct MyActor {
        on_handle: Box<dyn FnMut(&mut Context<Self>, MyMessage) -> () + Send>,
    }

    impl Actor for MyActor {
        const ACTOR_TYPE_KEY: &'static str = "MyActor";
    }

    impl Default for MyActor {
        fn default() -> Self {
            Self {
                on_handle: Box::new(|_, _| {}),
            }
        }
    }

    impl MyActor {
        fn new<F>(on_handle: F) -> Self
        where
            F: FnMut(&mut Context<Self>, MyMessage) -> () + Send + 'static,
        {
            Self {
                on_handle: Box::new(on_handle),
            }
        }
    }

    #[async_trait]
    impl Handler<MyMessage> for MyActor {
        async fn handle(&mut self, ctx: &mut Context<Self>, message: MyMessage) -> SystemId {
            (self.on_handle)(ctx, message);
            System::current().id()
        }
    }

    #[derive(Debug)]
    struct MyMessage;

    impl Message for MyMessage {
        type Result = SystemId;
    }

    #[tokio::test]
    async fn test_system() {
        let system = System::local("default");
        let actor = system.spawn::<MyActor>().expect("Failed to spawn actor");
        actor.send(MyMessage).await.unwrap();
    }

    #[tokio::test]
    async fn test_multi_system() {
        let system_a = System::local("system_a");
        let system_b = System::local("system_b");
        let actor_a = system_a.spawn::<MyActor>().expect("Failed to spawn actor");
        let actor_b = system_b.spawn::<MyActor>().expect("Failed to spawn actor");
        assert_eq!(actor_a.send(MyMessage).await.unwrap(), system_a.id());
        assert_eq!(actor_b.send(MyMessage).await.unwrap(), system_b.id());
    }

    #[tokio::test]
    async fn test_message_priority() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let system = System::local("default");
        let actor = system
            .spawn_with(MyActor::new(move |_, _| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }))
            .expect("Failed to spawn actor");

        // Send messages with different priorities
        let _ = actor.send(MyMessage).await; // Should be processed last

        // Give some time for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    enum CounterMessage {
        Increment,
    }

    impl Message for CounterMessage {
        type Result = usize;
    }

    struct Counter {
        counter: usize,
        notify: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Actor for Counter {
        const ACTOR_TYPE_KEY: &'static str = "counter";
        async fn started(&mut self, _ctx: &mut Context<Self>) {
            self.notify.notify_one();
        }
    }

    impl Counter {
        fn new(notify: Arc<tokio::sync::Notify>) -> Self {
            Self { counter: 0, notify }
        }
    }

    #[async_trait]
    impl Handler<RemoteMessage> for Counter {
        async fn handle(
            &mut self,
            _ctx: &mut Context<Self>,
            _message: RemoteMessage,
        ) -> Result<RemoteMessage, RemoteMessageError> {
            panic!("RemoteMessage is not supported for CounterActor");
        }
    }

    #[async_trait]
    impl Handler<CounterMessage> for Counter {
        async fn handle(&mut self, _ctx: &mut Context<Self>, message: CounterMessage) -> usize {
            match message {
                CounterMessage::Increment => {
                    self.counter += 1;
                }
            };
            self.counter
        }
    }

    #[tokio::test]
    async fn test_cluster_system() {
        init_tracing();
        // Install default crypto provider for rustls
        let crypto = rustls::crypto::ring::default_provider();
        crypto.install_default().unwrap();
        let system_a = System::clustered(ClusterConfig {
            cluster_id: Arc::new("A".into()),
            bind_addr: "127.0.0.1:7000".parse().unwrap(),
            peers: vec![], // No peers
            tls: TlsConfig::insecure(),
        })
        .expect("failed to create system");
        let system_b = System::clustered(ClusterConfig {
            cluster_id: Arc::new("B".into()),
            bind_addr: "127.0.0.1:7001".parse().unwrap(),
            peers: vec!["127.0.0.1:7000".parse().unwrap()], // No peers
            tls: TlsConfig::insecure(),
        })
        .expect("failed to create system");

        let notify = Arc::new(tokio::sync::Notify::new());
        let notification = notify.clone();

        let local_actor = Counter::new(notify);
        let local_actor = system_a
            .spawn_registered_with(local_actor)
            .await
            .expect("Failed to spawn actor");

        // Wait for the actor to be started so we can ensure we are registered.
        // notification.notified().await;

        // Allow time for the systems to establish connection
        // tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

        // let remote_actor = system_b
        //     .receptionist_ref()
        //     .lookup_one::<Counter>()
        //     .await
        //     .expect("Failed to loopup actor");
        //
        // // Send Message and Assert Response
        // let counter = remote_actor
        //     .send(CounterMessage::Increment)
        //     .await
        //     .expect("Failed to send message");
        // assert_eq!(counter, 1);

        // TODO: Change Receptionist to be Actor Type Key + Actor Path = Endpoint Mappings
        // TODO: Change the way remote actor remote proxy instances are created.
        // We don't need to generate the endpoint for each actor registered locally immediately
        // We can generate them on demand and use a keep alive to keep the connection open for
        // sometime. But for now we can just do a open an open and keep alive.
        // This means that the endpoint registered on the receptionist may not be an active
        // endpoint so we might need a new endpoint sub type for
    }
}
