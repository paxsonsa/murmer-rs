//! Router — distribute messages across a pool of actor endpoints.
//!
//! A [`Router<A>`] holds multiple [`Endpoint<A>`] handles and distributes
//! messages according to a [`RoutingStrategy`]:
//!
//! - **RoundRobin** — cycles through endpoints sequentially (default)
//! - **Random** — picks a random endpoint per message
//! - **Broadcast** — via [`Router::broadcast`], sends to all endpoints
//!
//! # Example
//!
//! ```rust,ignore
//! let router = Router::new(
//!     vec![ep1, ep2, ep3],
//!     RoutingStrategy::RoundRobin,
//! );
//!
//! // Each send goes to the next endpoint in sequence
//! router.send(Increment { amount: 1 }).await?;
//!
//! // Or send to all at once
//! let results = router.broadcast(GetCount).await;
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::actor::{Actor, Handler, RemoteMessage};
use crate::endpoint::Endpoint;
use crate::listing::{ListingEvent, ReceptionKey, WatchedListing};
use crate::receptionist::Receptionist;
use crate::runtime::{Runtime, TokioRuntime};
use crate::wire::SendError;

/// Strategy for how a Router distributes messages across its endpoints.
#[derive(Debug, Clone, Default)]
pub enum RoutingStrategy {
    #[default]
    RoundRobin,
    Random,
    Broadcast,
}

/// A router distributes messages across a group of endpoints for the same actor type.
///
/// Useful for load balancing, fan-out, and work distribution patterns.
pub struct Router<A: Actor> {
    endpoints: Vec<Endpoint<A>>,
    strategy: RoutingStrategy,
    counter: AtomicUsize,
    /// Runtime seam for Random selection, timers, and spawns — so a router can
    /// run deterministically under the sim runtime.
    runtime: Arc<dyn Runtime>,
}

impl<A: Actor + 'static> Router<A> {
    /// Create a router on the default (Tokio) runtime. For a deterministic sim,
    /// use [`with_runtime`](Self::with_runtime) and pass the sim runtime.
    pub fn new(endpoints: Vec<Endpoint<A>>, strategy: RoutingStrategy) -> Self {
        Self::with_runtime(endpoints, strategy, Arc::new(TokioRuntime))
    }

    /// Create a router on a specific runtime (e.g. a `SimWorld`'s runtime), so
    /// Random selection, `scatter_gather` timeouts, and `fan_out` spawns are
    /// deterministic.
    pub fn with_runtime(
        endpoints: Vec<Endpoint<A>>,
        strategy: RoutingStrategy,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        Self {
            endpoints,
            strategy,
            counter: AtomicUsize::new(0),
            runtime,
        }
    }

    /// Send a message to one endpoint based on the routing strategy.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return Err(SendError::MailboxClosed);
        }

        match self.strategy {
            RoutingStrategy::RoundRobin => {
                let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Random => {
                let idx = (self.runtime.rng_u64() % self.endpoints.len() as u64) as usize;
                self.endpoints[idx].send(msg).await
            }
            RoutingStrategy::Broadcast => {
                // For broadcast via send(), just send to the first endpoint
                self.endpoints[0].send(msg).await
            }
        }
    }

    /// Send a message to ALL endpoints. Returns a Vec of results.
    pub async fn broadcast<M>(&self, msg: M) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let mut results = Vec::with_capacity(self.endpoints.len());
        for ep in &self.endpoints {
            results.push(ep.send(msg.clone()).await);
        }
        results
    }

    /// Send a message to ALL endpoints concurrently and return when `k` respond
    /// successfully (or all have completed).
    ///
    /// This enables quorum patterns like "write to 3 replicas, succeed when 2
    /// acknowledge." Results are returned in completion order, not endpoint order.
    ///
    /// # Panics
    ///
    /// Does not panic if `k` exceeds the number of endpoints — returns all
    /// available results instead.
    pub async fn send_quorum<M>(&self, msg: M, k: usize) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return vec![Err(SendError::MailboxClosed)];
        }

        // FuturesUnordered (not JoinSet): polls all sends concurrently within
        // this task without spawning, so it runs under any executor including
        // the deterministic sim runtime.
        let mut inflight = FuturesUnordered::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            inflight.push(async move { ep.send(msg).await });
        }

        let target = k.min(self.endpoints.len());
        let mut results = Vec::with_capacity(target);
        let mut successes = 0;

        while let Some(result) = inflight.next().await {
            if result.is_ok() {
                successes += 1;
            }
            results.push(result);

            if successes >= target {
                // Quorum reached — drop the rest (cancels remaining sends).
                break;
            }
        }

        results
    }

    /// Send a message to ALL endpoints concurrently and collect all results
    /// within a timeout. Returns partial results if the timeout expires.
    ///
    /// Unlike `broadcast()` which sends sequentially, this fires all sends
    /// concurrently and returns results in completion order.
    pub async fn scatter_gather<M>(
        &self,
        msg: M,
        timeout: std::time::Duration,
    ) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return vec![Err(SendError::MailboxClosed)];
        }

        let mut inflight = FuturesUnordered::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            inflight.push(async move { ep.send(msg).await });
        }

        let mut results = Vec::with_capacity(self.endpoints.len());
        // Timer on the runtime seam (virtual time under sim). Created once so the
        // deadline is stable across loop iterations.
        let mut timeout_fut = self.runtime.sleep(timeout);

        loop {
            tokio::select! {
                biased;
                Some(result) = inflight.next() => {
                    results.push(result);
                    if results.len() == self.endpoints.len() {
                        break;
                    }
                }
                _ = &mut timeout_fut => {
                    break; // dropping `inflight` cancels remaining sends
                }
            }
        }

        results
    }

    /// Send a message to ALL endpoints concurrently and return the first
    /// successful response. Remaining in-flight sends are cancelled.
    ///
    /// Returns `Err` only if ALL endpoints fail.
    pub async fn first_response<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        if self.endpoints.is_empty() {
            return Err(SendError::MailboxClosed);
        }

        let mut inflight = FuturesUnordered::new();
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            inflight.push(async move { ep.send(msg).await });
        }

        let mut last_error = SendError::MailboxClosed;
        while let Some(result) = inflight.next().await {
            match result {
                Ok(result) => {
                    // First success — drop the rest (cancels remaining sends).
                    return Ok(result);
                }
                Err(e) => last_error = e,
            }
        }

        Err(last_error)
    }

    /// Send a message to ALL endpoints concurrently without waiting for responses.
    ///
    /// Fire-and-forget pattern. The sends are spawned as background tasks.
    /// Errors are silently dropped.
    pub fn fan_out<M>(&self, msg: M)
    where
        A: Handler<M>,
        M: RemoteMessage + Clone + 'static,
        M::Result: Serialize + DeserializeOwned,
    {
        for ep in &self.endpoints {
            let ep = ep.clone();
            let msg = msg.clone();
            self.runtime.spawn(Box::pin(async move {
                let _ = ep.send(msg).await;
            }));
        }
    }

    pub fn add(&mut self, endpoint: Endpoint<A>) {
        self.endpoints.push(endpoint);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.endpoints.len() {
            self.endpoints.remove(index);
        }
    }

    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }
}

// =============================================================================
// POOL ROUTER — reactive router that auto-tracks membership via WatchedListing
// =============================================================================

/// A reactive router that automatically maintains its endpoint pool from a
/// [`WatchedListing`]. As actors check in or deregister, the pool updates
/// in the background.
///
/// # Example
///
/// ```rust,ignore
/// let worker_key = ReceptionKey::<Worker>::new("workers");
///
/// // Pool auto-fills with existing workers and tracks changes
/// let pool = PoolRouter::new(&receptionist, worker_key, RoutingStrategy::RoundRobin);
///
/// // Send routes to a live worker — pool shrinks/grows automatically
/// let result = pool.send(DoWork { task: "hello".into() }).await?;
/// ```
/// Shared pool state: label → endpoint pairs, protected by RwLock.
type Pool<A> = Arc<RwLock<Vec<(String, Endpoint<A>)>>>;

pub struct PoolRouter<A: Actor> {
    pool: Pool<A>,
    strategy: RoutingStrategy,
    counter: AtomicUsize,
    runtime: Arc<dyn Runtime>,
}

impl<A: Actor + 'static> PoolRouter<A> {
    /// Create a reactive pool router that tracks all actors checked in with the
    /// given key. Runs a background task (on the receptionist's runtime) to
    /// consume the watched listing — so under a sim runtime the pool updates
    /// deterministically.
    pub fn new(
        receptionist: &Receptionist,
        key: ReceptionKey<A>,
        strategy: RoutingStrategy,
    ) -> Self {
        let watched = receptionist.watched_listing(key);
        Self::build(watched, strategy, receptionist.runtime().clone())
    }

    /// Create a pool router from an already-obtained watched listing, on the
    /// default (Tokio) runtime. Prefer [`new`](Self::new) for sim — it picks up
    /// the receptionist's runtime automatically.
    pub fn from_watched_listing(watched: WatchedListing<A>, strategy: RoutingStrategy) -> Self {
        Self::build(watched, strategy, Arc::new(TokioRuntime))
    }

    fn build(
        watched: WatchedListing<A>,
        strategy: RoutingStrategy,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        let pool: Pool<A> = Arc::new(RwLock::new(Vec::new()));

        // Background task to consume the watched listing, on the runtime seam.
        let pool_clone = Arc::clone(&pool);
        runtime.spawn(Box::pin(Self::run_watcher(pool_clone, watched)));

        Self {
            pool,
            strategy,
            counter: AtomicUsize::new(0),
            runtime,
        }
    }

    async fn run_watcher(pool: Pool<A>, mut watched: WatchedListing<A>) {
        while let Some(event) = watched.next().await {
            match event {
                ListingEvent::Added { label, endpoint } => {
                    let mut pool = pool.write().unwrap();
                    // Avoid duplicates (same label)
                    if !pool.iter().any(|(l, _)| l == &label) {
                        pool.push((label, endpoint));
                    }
                }
                ListingEvent::Removed { label } => {
                    let mut pool = pool.write().unwrap();
                    pool.retain(|(l, _)| l != &label);
                }
            }
        }
    }

    /// Send a message to one endpoint based on the routing strategy.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let endpoint = {
            let pool = self.pool.read().unwrap();
            if pool.is_empty() {
                return Err(SendError::MailboxClosed);
            }

            let idx = match self.strategy {
                RoutingStrategy::RoundRobin => {
                    self.counter.fetch_add(1, Ordering::Relaxed) % pool.len()
                }
                RoutingStrategy::Random => {
                    (self.runtime.rng_u64() % pool.len() as u64) as usize
                }
                RoutingStrategy::Broadcast => 0,
            };

            pool[idx].1.clone()
        }; // lock released here

        endpoint.send(msg).await
    }

    /// Send a message to ALL endpoints concurrently and collect results.
    pub async fn broadcast<M>(&self, msg: M) -> Vec<Result<M::Result, SendError>>
    where
        A: Handler<M>,
        M: RemoteMessage + Clone,
        M::Result: Serialize + DeserializeOwned,
    {
        let endpoints: Vec<_> = {
            let pool = self.pool.read().unwrap();
            pool.iter().map(|(_, ep)| ep.clone()).collect()
        };

        let mut inflight = FuturesUnordered::new();
        for ep in endpoints {
            let msg = msg.clone();
            inflight.push(async move { ep.send(msg).await });
        }

        let mut results = Vec::new();
        while let Some(result) = inflight.next().await {
            results.push(result);
        }
        results
    }

    /// Number of endpoints currently in the pool.
    pub fn len(&self) -> usize {
        self.pool.read().unwrap().len()
    }

    /// Whether the pool is currently empty.
    pub fn is_empty(&self) -> bool {
        self.pool.read().unwrap().is_empty()
    }

    /// Get a snapshot of current labels in the pool.
    pub fn labels(&self) -> Vec<String> {
        self.pool
            .read()
            .unwrap()
            .iter()
            .map(|(l, _)| l.clone())
            .collect()
    }
}
