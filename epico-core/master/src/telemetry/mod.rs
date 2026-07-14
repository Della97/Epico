//! Run telemetry — everything the agent measures, in one module.
//!
//! Layout:
//!   * `mod.rs` (here)  — the shared [`RunTelemetry`] state + event structs and
//!                        the writer discipline that governs them.
//!   * `collector.rs`   — [`collector::CollectorStats`], the collector thread's
//!                        lock-free per-event accumulator (hops, e2e, reservoir).
//!   * `summary.rs`     — [`summary::build_summary`] and the per-block builders
//!                        that shape `master_<ts>_summary.json`.
//!   * `resources.rs`   — the CPU + RSS sampler thread.
//!   * `stats.rs`       — percentile / rounding / subsampling helpers shared by
//!                        the builders.
//!
//! # Writer discipline
//!
//! `RunTelemetry` sits behind one `Arc<Mutex<_>>` shared by three writer
//! classes with very different rates:
//!
//! * **Autoscalers** (1 ms tick, per stage) write `scaling_events`,
//!   `total_us_samples`, `serde_us_samples`, and `queue_depth_samples`
//!   directly under the lock — low rate, contention-free in practice.
//! * **The resource sampler** (~1 Hz) writes `resource_samples` under the lock.
//! * **The collector** (per event — the hot path) NEVER touches the lock while
//!   running: it accumulates its fields in a thread-local
//!   [`collector::CollectorStats`] and moves them in exactly once at exit.
//!   `run_agent` joins the collector thread before calling
//!   [`summary::build_summary`], so the summary always sees the merged state.

use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) mod collector;
pub(crate) mod resources;
pub(crate) mod stats;
pub(crate) mod summary;

// ── Shared telemetry collected across the whole run ──────────────────────────

/// A single point in the master's CPU + RSS time series.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ResourceSample {
    /// Seconds since test_start.
    pub t_s:       f64,
    /// Process CPU utilization normalized to the CPUs available to this
    /// process (typically affinity/cgroup constrained). 100% means this
    /// process saturates all CPUs it can run on.
    pub cpu_pct:   f32,
    /// Resident Set Size in bytes.
    pub rss_bytes: u64,
}

/// A single scale-up/scale-down/cold-start/drain event recorded by an
/// autoscaler. Written once per decision; used to reconstruct the
/// replica-count timeline for the paper's scaling plots.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ScalingEvent {
    /// Seconds since test_start.
    pub t_s:         f64,
    pub stage:       String,
    /// "cold_start" | "spawn" | "drain".
    pub action:      String,
    /// Active replica count *after* this action took effect.
    pub new_count:   usize,
    /// For `cold_start` events only: how long the first event took to
    /// reach `process-event` and return. None for spawn/drain.
    pub cold_start_ms:      Option<f64>,
    /// Cranelift JIT compilation time for this stage's component. Only
    /// set on "init" events, None on spawn/drain/cold_start.
    pub compile_ms:         Option<f64>,
    /// linker.instantiate_pre() time. Only set on "init" events.
    pub instantiate_pre_ms: Option<f64>,
}

/// Shared run statistics, read at shutdown to build the summary.
/// See the module docs for the writer discipline.
#[derive(Default)]
pub(crate) struct RunTelemetry {
    // ── E2E latency ───────────────────────────────────────────────────────
    /// All e2e latencies in milliseconds (recv_ts_wall - bench_ts_wall).
    pub e2e_ms:         Vec<f64>,
    /// All ingress waits in milliseconds (entry-stage hop[0].enter_ts -
    /// bench_ts_wall). This is the time each event spent sitting in the
    /// first dispatcher's frontend queue before the entry stage's worker
    /// started processing it. Tracked in lockstep with `e2e_ms` — same
    /// length, same sampling regime, so percentiles computed from the
    /// two are directly comparable. At saturation this dominates e2e;
    /// at steady state it should be sub-millisecond.
    pub ingress_wait_ms: Vec<f64>,
    /// Per-second received count (for throughput chart).
    pub recv_per_second: std::collections::HashMap<u64, u64>,

    // ── Per-stage timing ──────────────────────────────────────────────────
    /// For each stage name, the list of per-event residence times in ms
    /// (hop.exit_ts - hop.enter_ts). Populated by the collector when it
    /// parses bench_hops off arriving events.
    pub per_stage_latency_ms: std::collections::HashMap<String, Vec<f64>>,
    /// For each stage name, the count of events that passed through it.
    /// At steady state this equals the collector's recv_count; during
    /// scale-up it may briefly lag.
    pub per_stage_count:      std::collections::HashMap<String, u64>,

    // ── Per-replica timing ────────────────────────────────────────────────
    /// Residence times keyed by the full hop label `stage#replica`. Same
    /// samples as per_stage_latency_ms, but split by which replica actually
    /// processed the event — powers per-replica box/violin plots and
    /// load-balance checks. Empty on runs predating replica-tagged hops.
    pub per_replica_latency_ms: std::collections::HashMap<String, Vec<f64>>,
    /// Event count per `stage#replica` label (load-balance fairness check).
    pub per_replica_count:      std::collections::HashMap<String, u64>,

    // ── Autoscaler events ─────────────────────────────────────────────────
    pub scaling_events: Vec<ScalingEvent>,

    // ── Per-event raw log ─────────────────────────────────────────────────
    /// One row per event arriving at the collector, capturing enough to
    /// reconstruct per-second latency percentiles and per-edge transport
    /// delays in post-processing. Fields:
    ///   .0 — recv_t_s:  seconds since test start at collector recv
    ///   .1 — e2e_ms:    end-to-end latency in milliseconds
    ///   .2 — hops:      Vec<(stage_name, enter_ts_wall, exit_ts_wall)>
    ///
    /// Bounded to `collector::EVENTS_LIVE_CAP` rows via Algorithm R
    /// reservoir sampling (uniform over the whole run, unordered — summary
    /// time sorts by recv_t_s). The summary-time subsampler operates on
    /// this already-bounded reservoir.
    pub per_event_log: Vec<(f64, f64, Vec<(String, f64, f64)>)>,

    // ── Resource sampling ─────────────────────────────────────────────────
    pub resource_samples: Vec<ResourceSample>,

    // ── Worker timing breakdown ───────────────────────────────────────────
    /// Per-stage total worker iteration time in NANOSECONDS (recv → deser →
    /// wasm → ser → push). Keyed by stage name. Populated by autoscaler polling
    /// the dispatcher ctrl socket and forwarding consumer metrics. (Field name
    /// kept `_us` for churn reasons; values are ns since the worker switched to
    /// as_nanos. Converted to µs floats in the worker_timing summary block.)
    pub total_us_samples: std::collections::HashMap<String, Vec<u64>>,

    /// Per-stage serialization time in NANOSECONDS (JSON parse + JSON serialize,
    /// but NOT the wasm call). Subset of total_us_samples.
    pub serde_us_samples: std::collections::HashMap<String, Vec<u64>>,

    // ── Dispatcher queue depth time-series ────────────────────────────────
    /// Sampled queue depth per stage over time. Each entry is
    /// (t_s, queue_depth) where t_s is seconds since test_start.
    pub queue_depth_samples: std::collections::HashMap<String, Vec<(f64, u64)>>,
}

/// Record one autoscaler scaling event (spawn / drain / cold_start / init_*).
/// Shared by every autoscaler thread; takes the lock briefly (low rate).
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_event(
    telemetry:  &Arc<Mutex<RunTelemetry>>,
    test_start: Instant,
    stage:      &str,
    action:     &str,
    new_count:  usize,
    cold_start_ms:      Option<f64>,
    compile_ms:         Option<f64>,
    instantiate_pre_ms: Option<f64>,
) {
    if let Ok(mut tel) = telemetry.lock() {
        tel.scaling_events.push(ScalingEvent {
            t_s:                test_start.elapsed().as_secs_f64(),
            stage:              stage.to_string(),
            action:             action.to_string(),
            new_count,
            cold_start_ms,
            compile_ms,
            instantiate_pre_ms,
        });
    }
}
