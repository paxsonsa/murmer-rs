//! Prometheus metrics exporter setup.
//!
//! Provides a one-call setup function that installs a Prometheus recorder
//! and starts an HTTP listener for scraping.
//!
//! ```rust,ignore
//! use murmer::monitor::start_prometheus;
//!
//! // Start serving metrics on :9000/metrics
//! start_prometheus(9000).expect("failed to start prometheus exporter");
//! ```

use metrics_exporter_prometheus::PrometheusBuilder;

/// Install the Prometheus metrics recorder and start an HTTP listener.
///
/// After calling this, all metrics recorded via the `metrics` crate (including
/// all murmer instrumentation) will be served at `http://0.0.0.0:{port}/metrics`
/// in the Prometheus exposition format.
///
/// This should be called once, early in your application's startup, before
/// starting any actor systems.
///
/// # Examples
///
/// ```rust,ignore
/// use murmer::monitor::start_prometheus;
/// use murmer::prelude::*;
///
/// #[tokio::main]
/// async fn main() {
///     // Start Prometheus exporter on port 9000
///     start_prometheus(9000).expect("failed to start prometheus");
///
///     // Start your actor system — all metrics are automatically recorded
///     let system = System::local();
///     let counter = system.start("counter/0", Counter, CounterState { count: 0 });
///
///     // curl http://localhost:9000/metrics to see:
///     //   murmer_actors_active{actor_type="my_app::Counter"} 1
///     //   murmer_actors_started_total{actor_type="my_app::Counter"} 1
/// }
/// ```
///
/// # Errors
///
/// Returns an error if a metrics recorder is already installed (you can only
/// install one recorder per process).
pub fn start_prometheus(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()?;

    describe_metrics();

    tracing::info!("Prometheus metrics exporter listening on 0.0.0.0:{port}");
    Ok(())
}

/// Register human-readable descriptions for all murmer metrics.
///
/// These show up in Prometheus as `# HELP` comments.
fn describe_metrics() {
    // Actor lifecycle
    metrics::describe_counter!(
        "murmer_actors_started_total",
        "Total number of actors started"
    );
    metrics::describe_gauge!("murmer_actors_active", "Number of currently active actors");
    metrics::describe_counter!(
        "murmer_actors_stopped_total",
        "Total number of actors stopped"
    );
    metrics::describe_counter!(
        "murmer_actors_restarts_total",
        "Total number of actor restarts"
    );
    metrics::describe_counter!(
        "murmer_actors_restart_limit_exceeded_total",
        "Total number of times actor restart limits were exceeded"
    );

    // Message processing
    metrics::describe_counter!(
        "murmer_messages_processed_total",
        "Total number of messages successfully processed"
    );
    metrics::describe_counter!(
        "murmer_messages_failed_total",
        "Total number of messages that failed (handler panicked)"
    );
    metrics::describe_histogram!(
        "murmer_message_processing_duration_seconds",
        "Time spent processing each message in the handler"
    );

    // Endpoint / sends
    metrics::describe_counter!(
        "murmer_sends_total",
        "Total number of messages sent via endpoints"
    );
    metrics::describe_counter!("murmer_send_errors_total", "Total number of send errors");
    metrics::describe_histogram!(
        "murmer_network_roundtrip_duration_seconds",
        "End-to-end latency for remote message sends"
    );

    // Networking
    metrics::describe_gauge!(
        "murmer_network_connections_active",
        "Number of active QUIC connections to peer nodes"
    );
    metrics::describe_gauge!(
        "murmer_network_streams_active",
        "Number of active QUIC streams for actor messaging"
    );
    metrics::describe_counter!(
        "murmer_network_bytes_sent_total",
        "Total bytes sent over actor streams"
    );
    metrics::describe_counter!(
        "murmer_network_bytes_received_total",
        "Total bytes received over actor streams"
    );
    metrics::describe_gauge!(
        "murmer_network_inflight_calls",
        "Number of in-flight remote calls awaiting responses"
    );
    metrics::describe_counter!(
        "murmer_network_dead_letters_total",
        "Total number of dead letters (failed in-flight calls due to connection loss)"
    );

    // Cluster membership
    metrics::describe_gauge!("murmer_cluster_nodes", "Number of nodes in the cluster");
    metrics::describe_counter!(
        "murmer_cluster_membership_changes_total",
        "Total number of cluster membership change events"
    );

    // Receptionist
    metrics::describe_counter!(
        "murmer_receptionist_lookups_total",
        "Total number of actor lookups"
    );
    metrics::describe_counter!(
        "murmer_receptionist_registrations_total",
        "Total number of actor registrations"
    );
    metrics::describe_counter!(
        "murmer_receptionist_deregistrations_total",
        "Total number of actor deregistrations"
    );
}
