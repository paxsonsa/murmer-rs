//! Metrics instrumentation facade.
//!
//! This module is the sole bridge between murmer's core code and the metrics
//! subsystem. When the `monitor` feature is enabled, each function records a
//! real metric via the [`metrics`] crate. When disabled, every function is an
//! empty `#[inline(always)]` no-op that the compiler eliminates entirely.
//!
//! Call sites are unconditional — no `#[cfg]` needed outside this file.

// ─── Actor Lifecycle ─────────────────────────────────────────────────

#[inline(always)]
pub(crate) fn actor_started(actor_type: &str) {
    #[cfg(feature = "monitor")]
    {
        metrics::counter!("murmer_actors_started_total", "actor_type" => actor_type.to_string())
            .increment(1);
        metrics::gauge!("murmer_actors_active", "actor_type" => actor_type.to_string())
            .increment(1.0);
    }
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn actor_stopped(actor_type: &str, reason: &str) {
    #[cfg(feature = "monitor")]
    {
        metrics::counter!(
            "murmer_actors_stopped_total",
            "actor_type" => actor_type.to_string(),
            "reason" => reason.to_string(),
        )
        .increment(1);
        metrics::gauge!("murmer_actors_active", "actor_type" => actor_type.to_string())
            .decrement(1.0);
    }
    #[cfg(not(feature = "monitor"))]
    {
        let _ = (actor_type, reason);
    }
}

#[inline(always)]
pub(crate) fn actor_restarted(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_actors_restarts_total",
        "actor_type" => actor_type.to_string(),
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn actor_restart_limit_exceeded(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_actors_restart_limit_exceeded_total",
        "actor_type" => actor_type.to_string(),
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

// ─── Message Processing ─────────────────────────────────────────────

#[inline(always)]
pub(crate) fn message_processed(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_messages_processed_total",
        "actor_type" => actor_type.to_string(),
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn message_failed(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_messages_failed_total",
        "actor_type" => actor_type.to_string(),
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn message_processing_duration(actor_type: &str, duration: std::time::Duration) {
    #[cfg(feature = "monitor")]
    metrics::histogram!(
        "murmer_message_processing_duration_seconds",
        "actor_type" => actor_type.to_string(),
    )
    .record(duration);
    #[cfg(not(feature = "monitor"))]
    let _ = (actor_type, duration);
}

// ─── Endpoint / Send ─────────────────────────────────────────────────

#[inline(always)]
pub(crate) fn send_local(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_sends_total",
        "actor_type" => actor_type.to_string(),
        "locality" => "local",
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn send_remote(actor_type: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_sends_total",
        "actor_type" => actor_type.to_string(),
        "locality" => "remote",
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = actor_type;
}

#[inline(always)]
pub(crate) fn send_error(actor_type: &str, error_kind: &str) {
    #[cfg(feature = "monitor")]
    metrics::counter!(
        "murmer_send_errors_total",
        "actor_type" => actor_type.to_string(),
        "error_kind" => error_kind.to_string(),
    )
    .increment(1);
    #[cfg(not(feature = "monitor"))]
    let _ = (actor_type, error_kind);
}

#[inline(always)]
pub(crate) fn remote_roundtrip_duration(actor_type: &str, duration: std::time::Duration) {
    #[cfg(feature = "monitor")]
    metrics::histogram!(
        "murmer_network_roundtrip_duration_seconds",
        "actor_type" => actor_type.to_string(),
    )
    .record(duration);
    #[cfg(not(feature = "monitor"))]
    let _ = (actor_type, duration);
}

// ─── Networking ──────────────────────────────────────────────────────

#[inline(always)]
pub(crate) fn connection_opened() {
    #[cfg(feature = "monitor")]
    metrics::gauge!("murmer_network_connections_active").increment(1.0);
}

#[inline(always)]
pub(crate) fn connection_closed() {
    #[cfg(feature = "monitor")]
    metrics::gauge!("murmer_network_connections_active").decrement(1.0);
}

#[inline(always)]
pub(crate) fn stream_opened() {
    #[cfg(feature = "monitor")]
    metrics::gauge!("murmer_network_streams_active").increment(1.0);
}

#[inline(always)]
pub(crate) fn stream_closed() {
    #[cfg(feature = "monitor")]
    metrics::gauge!("murmer_network_streams_active").decrement(1.0);
}

#[inline(always)]
pub(crate) fn network_bytes_sent(n: u64) {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_network_bytes_sent_total").increment(n);
    #[cfg(not(feature = "monitor"))]
    let _ = n;
}

#[inline(always)]
pub(crate) fn network_bytes_received(n: u64) {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_network_bytes_received_total").increment(n);
    #[cfg(not(feature = "monitor"))]
    let _ = n;
}

#[inline(always)]
pub(crate) fn inflight_calls_set(n: f64) {
    #[cfg(feature = "monitor")]
    metrics::gauge!("murmer_network_inflight_calls").set(n);
    #[cfg(not(feature = "monitor"))]
    let _ = n;
}

#[inline(always)]
pub(crate) fn dead_letters(count: u64) {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_network_dead_letters_total").increment(count);
    #[cfg(not(feature = "monitor"))]
    let _ = count;
}

// ─── Cluster Membership ─────────────────────────────────────────────

#[inline(always)]
pub(crate) fn cluster_node_joined() {
    #[cfg(feature = "monitor")]
    {
        metrics::counter!(
            "murmer_cluster_membership_changes_total",
            "event_type" => "joined",
        )
        .increment(1);
        metrics::gauge!("murmer_cluster_nodes", "status" => "active").increment(1.0);
    }
}

#[inline(always)]
pub(crate) fn cluster_node_failed() {
    #[cfg(feature = "monitor")]
    {
        metrics::counter!(
            "murmer_cluster_membership_changes_total",
            "event_type" => "failed",
        )
        .increment(1);
        metrics::gauge!("murmer_cluster_nodes", "status" => "active").decrement(1.0);
    }
}

#[inline(always)]
pub(crate) fn cluster_node_left() {
    #[cfg(feature = "monitor")]
    {
        metrics::counter!(
            "murmer_cluster_membership_changes_total",
            "event_type" => "left",
        )
        .increment(1);
        metrics::gauge!("murmer_cluster_nodes", "status" => "active").decrement(1.0);
    }
}

// ─── Receptionist ────────────────────────────────────────────────────

#[inline(always)]
pub(crate) fn receptionist_lookup() {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_receptionist_lookups_total").increment(1);
}

#[inline(always)]
pub(crate) fn receptionist_registration() {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_receptionist_registrations_total").increment(1);
}

#[inline(always)]
pub(crate) fn receptionist_deregistration() {
    #[cfg(feature = "monitor")]
    metrics::counter!("murmer_receptionist_deregistrations_total").increment(1);
}
