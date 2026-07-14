//! The collector thread's per-event accounting, accumulated WITHOUT any
//! locking and merged into the shared telemetry exactly once at loop exit.
//!
//! The pre-refactor design pushed into the shared mutex under `try_lock()`
//! per event and silently dropped the sample whenever an autoscaler's 1 ms
//! tick held it — biasing e2e/throughput exactly when the system was
//! busiest. The collector is the sole writer of every field it touches, so
//! thread-local accumulation changes no semantics, only removes the loss.

use std::sync::{Arc, Mutex};

use epico_logger::Logger;
use epico_logger::error;
use rand::Rng;

use super::RunTelemetry;

// Maximum number of raw per-event rows kept live in the collector's per-event
// reservoir during a run. Once full, event N replaces a uniformly random slot
// with probability CAP/N (Algorithm R), so the reservoir stays a uniform
// sample of the whole run. This bounds RSS growth to roughly
// EVENTS_LIVE_CAP * ~200 bytes ≈ 10 MB regardless of run duration.
// The summary-time subsampler (EVENTS_SAMPLE_CAP) operates on this already-
// bounded reservoir, so nothing downstream needs to change.
pub(crate) const EVENTS_LIVE_CAP: usize = 200_000;

/// Thread-local accumulator owned by the collector loop. `observe` per event
/// (no lock), `merge_into` once at exit.
pub(crate) struct CollectorStats {
    local: RunTelemetry,
    /// Total events that reached the reservoir insertion point (not just the
    /// ones currently stored) — the `N` of Algorithm R.
    events_seen: u64,
    rng: rand::rngs::ThreadRng,
}

impl CollectorStats {
    pub fn new() -> Self {
        CollectorStats {
            local:       RunTelemetry::default(),
            events_seen: 0,
            rng:         rand::thread_rng(),
        }
    }

    /// Account one finished event (already parsed; EOS markers are filtered
    /// by the caller). No-op when the event carries no usable
    /// `bench_ts_wall` or its clock reads ahead of ours.
    pub fn observe(&mut self, recv_ts: f64, test_start: f64, ev: &serde_json::Value) {
        let bench_ts = match ev["bench_ts_wall"].as_f64() {
            Some(v) if recv_ts > v => v,
            _ => return,
        };
        let lat_ms = (recv_ts - bench_ts) * 1000.0;
        let bucket = recv_ts as u64; // absolute second

        // Parse hops once into a plain Vec so the accounting below never
        // re-walks the JSON.
        let mut hops_vec: Vec<(String, f64, f64)> = Vec::new();
        if let Some(hops) = ev.get("bench_hops").and_then(|v| v.as_array()) {
            hops_vec.reserve(hops.len());
            for hop in hops {
                let arr = match hop.as_array() { Some(a) => a, None => continue };
                if arr.len() < 3 { continue; }
                let name = match arr[0].as_str() { Some(n) => n, None => continue };
                let enter = match arr[1].as_f64() { Some(v) => v, None => continue };
                let exit  = match arr[2].as_f64() { Some(v) => v, None => continue };
                if exit >= enter {
                    hops_vec.push((name.to_string(), enter, exit));
                }
            }
        }

        // Ingress wait: `enter_ts[stage 0] - bench_ts_wall`. Computed here
        // (rather than at summary time from per_event_log) so it tracks the
        // full event population in lockstep with `e2e_ms`, not the reservoir
        // subsample — same sampling regime, same n, directly comparable
        // percentiles. Negative values are dropped defensively (clock skew
        // between processes can produce them in rare cases).
        let ingress_wait_ms = hops_vec.first().map(|(_, enter, _)| {
            (enter - bench_ts) * 1000.0
        }).filter(|v| *v >= 0.0);

        self.local.e2e_ms.push(lat_ms);
        if let Some(w) = ingress_wait_ms {
            self.local.ingress_wait_ms.push(w);
        }
        *self.local.recv_per_second.entry(bucket).or_default() += 1;

        // Per-stage latencies and counts. Hop names carry a `#replica`
        // suffix since replica-tagged hops landed; strip it here so
        // per_stage_* keys stay bare stage names (analyze scripts
        // unchanged), and aggregate the full label separately for the
        // per_replica block.
        for (name, enter, exit) in &hops_vec {
            let res_ms = (exit - enter) * 1000.0;
            let base = name.split('#').next().unwrap_or(name);
            self.local.per_stage_latency_ms
                .entry(base.to_string())
                .or_default()
                .push(res_ms);
            *self.local.per_stage_count
                .entry(base.to_string())
                .or_default() += 1;
            if name.contains('#') {
                self.local.per_replica_latency_ms
                    .entry(name.clone())
                    .or_default()
                    .push(res_ms);
                *self.local.per_replica_count
                    .entry(name.clone())
                    .or_default() += 1;
            }
        }

        // Per-event raw row. recv_t_s is normalized to the run's start so
        // Python can bin without knowing wall-clock zero; hops keep raw
        // wall-clock timestamps (Python differences them per segment).
        // Algorithm R keeps the log a uniform, bounded sample of the whole
        // run; the reservoir is unordered — summary time sorts by recv_t_s.
        let recv_t_s = recv_ts - test_start;
        self.events_seen += 1;
        if self.local.per_event_log.len() < EVENTS_LIVE_CAP {
            self.local.per_event_log.push((recv_t_s, lat_ms, hops_vec));
        } else {
            let j = self.rng.gen_range(0..self.events_seen) as usize;
            if j < EVENTS_LIVE_CAP {
                self.local.per_event_log[j] = (recv_t_s, lat_ms, hops_vec);
            }
        }
    }

    /// Move the accumulated stats into the shared telemetry. The collector
    /// is the sole writer of every field assigned here, so this is a move,
    /// not a merge. `run_agent` joins the collector thread before building
    /// the summary, so the summary always sees the merged state.
    pub fn merge_into(mut self, telemetry: &Arc<Mutex<RunTelemetry>>, log: &Logger) {
        match telemetry.lock() {
            Ok(mut tel) => {
                tel.e2e_ms                 = std::mem::take(&mut self.local.e2e_ms);
                tel.ingress_wait_ms        = std::mem::take(&mut self.local.ingress_wait_ms);
                tel.recv_per_second        = std::mem::take(&mut self.local.recv_per_second);
                tel.per_stage_latency_ms   = std::mem::take(&mut self.local.per_stage_latency_ms);
                tel.per_stage_count        = std::mem::take(&mut self.local.per_stage_count);
                tel.per_replica_latency_ms = std::mem::take(&mut self.local.per_replica_latency_ms);
                tel.per_replica_count      = std::mem::take(&mut self.local.per_replica_count);
                tel.per_event_log          = std::mem::take(&mut self.local.per_event_log);
            }
            Err(_) => error!(log, "telemetry mutex poisoned; collector stats lost"),
        }
    }
}
