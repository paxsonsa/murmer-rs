//! Supervisor example — demonstrates application-level supervision with:
//!
//! - Tagged watches to identify which child terminated
//! - `schedule_repeat` for periodic health checks
//! - Clean actor restart pattern at the application level

use murmer::prelude::*;
use std::time::Duration;

// =============================================================================
// WORKER ACTOR
// =============================================================================

#[derive(Debug)]
struct Worker;

struct WorkerState {
    count: u64,
}

impl Actor for Worker {
    type State = WorkerState;
}

#[handlers]
impl Worker {
    #[handler]
    fn increment(&mut self, _ctx: &ActorContext<Self>, state: &mut WorkerState) -> u64 {
        state.count += 1;
        state.count
    }

    #[handler]
    fn get_count(&mut self, _ctx: &ActorContext<Self>, state: &mut WorkerState) -> u64 {
        state.count
    }
}

// =============================================================================
// SUPERVISOR ACTOR
// =============================================================================

#[derive(Debug)]
struct WorkerSupervisor;

struct SupervisorState {
    worker_a: Option<Endpoint<Worker>>,
    worker_b: Option<Endpoint<Worker>>,
    health_check: Option<ScheduleHandle>,
    terminated_tags: Vec<String>,
}

impl Actor for WorkerSupervisor {
    type State = SupervisorState;

    fn on_actor_terminated(&mut self, state: &mut SupervisorState, event: &ActorTerminated) {
        if let Some(tag) = &event.tag {
            state.terminated_tags.push(tag.clone());
            match tag.as_str() {
                "worker_a" => state.worker_a = None,
                "worker_b" => state.worker_b = None,
                _ => {}
            }
        }
    }
}

#[handlers]
impl WorkerSupervisor {
    #[handler]
    fn bootstrap(&mut self, ctx: &ActorContext<Self>, state: &mut SupervisorState) {
        let r = ctx.receptionist();

        let a = r.start("worker/a", Worker, WorkerState { count: 0 });
        ctx.watch_with_tag("worker/a", "worker_a");
        state.worker_a = Some(a);

        let b = r.start("worker/b", Worker, WorkerState { count: 0 });
        ctx.watch_with_tag("worker/b", "worker_b");
        state.worker_b = Some(b);

        state.health_check = Some(ctx.schedule_repeat(Duration::from_millis(500), HealthCheck));
    }

    #[handler]
    fn health_check(&mut self, _ctx: &ActorContext<Self>, state: &mut SupervisorState) {
        let _a_alive = state.worker_a.is_some();
        let _b_alive = state.worker_b.is_some();
    }

    #[handler]
    fn get_terminated(
        &mut self,
        _ctx: &ActorContext<Self>,
        state: &mut SupervisorState,
    ) -> Vec<String> {
        state.terminated_tags.clone()
    }
}

// =============================================================================
// MAIN
// =============================================================================

fn main() {
    println!("Run tests with: cargo nextest run -p murmer-examples --bin supervisor");
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn supervisor_state() -> SupervisorState {
        SupervisorState {
            worker_a: None,
            worker_b: None,
            health_check: None,
            terminated_tags: vec![],
        }
    }

    /// Test: tagged watch notifies supervisor with the correct role tag.
    #[tokio::test]
    async fn test_supervisor_tagged_watch() {
        let receptionist = Receptionist::new();

        let sup = receptionist.start("supervisor/0", WorkerSupervisor, supervisor_state());
        sup.bootstrap().await.unwrap();

        // Workers should be running
        let a = receptionist.lookup::<Worker>("worker/a").unwrap();
        let count = a.increment().await.unwrap();
        assert_eq!(count, 1);

        // Stop worker_a — supervisor should detect it via tagged watch
        receptionist.stop("worker/a");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let terminated = sup.get_terminated().await.unwrap();
        assert_eq!(terminated, vec!["worker_a"]);
    }

    /// Test: schedule_repeat delivers health check ticks without panicking.
    #[tokio::test]
    async fn test_supervisor_health_check_schedule() {
        let receptionist = Receptionist::new();

        let sup = receptionist.start("supervisor/1", WorkerSupervisor, supervisor_state());
        sup.bootstrap().await.unwrap();

        // Let a couple of health check ticks fire (500ms interval, wait 1.2s)
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // Workers are still alive and functioning
        let a = receptionist.lookup::<Worker>("worker/a").unwrap();
        assert_eq!(a.get_count().await.unwrap(), 0);
    }
}
