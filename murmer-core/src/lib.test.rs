use super::*;

#[derive(Default, Debug)]
struct TestActor;

#[async_trait::async_trait]
impl Actor for TestActor {
    type State = i32;

    const ACTOR_TYPE_KEY: &'static str = "TestActor";

    async fn init(&mut self, _ctx: &mut Context<Self>) -> Result<Self::State, ActorError> {
        Ok(0)
    }

    async fn started(&mut self, _ctx: &mut Context<Self>, _state: &mut Self::State) {}

    async fn stopping(&mut self, _ctx: &mut Context<Self>, _state: &mut Self::State) {}
}

#[derive(Debug)]
struct Increment;
impl Message for Increment {
    type Result = i32;
}
#[async_trait::async_trait]
impl Handler<Increment> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        state: &mut Self::State,
        _message: Increment,
    ) -> i32 {
        *state += 1;
        *state
    }
}

#[derive(Debug)]
struct Decrement;
impl Message for Decrement {
    type Result = i32;
}
#[async_trait::async_trait]
impl Handler<Decrement> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        state: &mut Self::State,
        _message: Decrement,
    ) -> i32 {
        *state -= 1;
        *state
    }
}
#[tokio::test]
async fn test_basic_actor_trait() {
    let mut actor = TestActor;
    let mut ctx = Context::<TestActor> {
        _phantom: std::marker::PhantomData,
    };

    let mut state = actor.init(&mut ctx).await.expect("failed to init actor");
    assert_eq!(actor.handle(&mut ctx, &mut state, Increment).await, 1);
    assert_eq!(state, 1);
    assert_eq!(actor.handle(&mut ctx, &mut state, Decrement).await, 0);
    assert_eq!(state, 0);
}

#[tokio::test]
async fn test_simple_system() {
    let system = SystemBuilder::new().build();

    let (_, endpoint) = system.spawn::<TestActor>().expect("failed to spawn actor");

    let state = endpoint
        .send(Increment)
        .await
        .expect("failed to send message");
    assert_eq!(state, 1);

    let state = endpoint
        .send(Decrement)
        .await
        .expect("failed to send message");

    assert_eq!(state, 0);
}
