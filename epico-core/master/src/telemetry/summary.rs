//! Summary construction — shapes everything accumulated in [`RunTelemetry`]
//! into `master_<ts>_summary.json`.
//!
//! The JSON layout is a STABLE INTERFACE: the Python analysis/plot scripts
//! (bench/, tests/plot_leak.py, external notebooks) consume it by key. Add
//! blocks freely; do not rename or restructure existing ones.

use std::sync::{Arc, Mutex};

use epico_logger::Logger;
use epico_logger::info;
use serde_json::json;

use super::stats::{mean, pct, pct_r5, round3, subsample_events, subsample_f64, subsample_rate};
use super::{RunTelemetry, ScalingEvent};

pub(crate) fn build_summary(
    log:           &Logger,
    telemetry:     Arc<Mutex<RunTelemetry>>,
    test_start:    f64,
    test_duration: f64,
    stage_names:   &[String],
    run_config:    serde_json::Value,
) -> serde_json::Value {
    let tel = match telemetry.lock() {
        Ok(t)  => t,
        Err(_) => return json!({}),
    };

    let mut e2e = tel.e2e_ms.clone();
    e2e.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = e2e.len();

    // Histogram (50 buckets, capped at 3×p99) — kept for backward compat
    // with the existing plot pipeline. Paper plots should be generated
    // from the raw `e2e_ms_samples` list below, not these 50 buckets.
    let (hist_labels, hist_counts) = if n > 0 {
        let max_lat = (pct_r5(&e2e, 0.99) * 3.0).max(1.0).min(*e2e.last().unwrap());
        let bw      = max_lat / 50.0;
        let labels: Vec<f64> = (0..50).map(|i| (i as f64 * bw + bw / 2.0) * 1e5).map(|v| v.round() / 1e5).collect();
        let counts: Vec<usize> = (0..50)
            .map(|i| e2e.iter().filter(|&&l| l >= i as f64 * bw && l < (i + 1) as f64 * bw).count())
            .collect();
        (labels, counts)
    } else { (vec![], vec![]) };

    // CDF
    let step = (n / 200).max(1);
    let cdf_x: Vec<f64> = (0..n).step_by(step).map(|i| pct_r5(&e2e, i as f64 / n as f64)).collect();
    let cdf_y: Vec<f64> = (0..n).step_by(step).map(|i| ((i + 1) as f64 / n as f64 * 100.0 * 100.0).round() / 100.0).collect();

    // Per-second received counts, aligned to test_start.
    let recv_buckets = &tel.recv_per_second;
    let mut bucket_keys: Vec<u64> = recv_buckets.keys().copied().collect();
    bucket_keys.sort();
    let lat_ts_labels: Vec<u64> = bucket_keys.iter()
        .map(|&b| b.saturating_sub(test_start as u64))
        .collect();
    let recv_per_s: Vec<u64> = bucket_keys.iter().map(|b| recv_buckets[b]).collect();

    let dispatcher_names: Vec<String> = stage_names.iter()
        .map(|s| format!("dispatch-{}", s.trim_start_matches("fn-")))
        .collect();

    info!(log, "e2e summary",
          n = n,
          p50 = format!("{:.3}ms", pct_r5(&e2e, 0.50)),
          p99 = format!("{:.3}ms", pct_r5(&e2e, 0.99)),
          max = format!("{:.3}ms", if n > 0 { e2e[n-1] } else { 0.0 }));

    // ── Paper-grade blocks ────────────────────────────────────────────────

    // Environment. Captured once at summary time; static for the run.
    let env_block = build_environment_block();

    // Counters. recv_count is the total event arrivals at the collector.
    let recv_count: u64 = recv_per_s.iter().sum();
    let counters_block = json!({
        "events_received":     recv_count,
        "per_stage_count":     tel.per_stage_count,
    });

    // Throughput. Sustained ev/s over the full test_duration; reviewers
    // typically want a warm-up-trimmed version too, so we supply both.
    let sustained_eps = if test_duration > 0.0 { recv_count as f64 / test_duration } else { 0.0 };
    let trim = 5.0_f64.min(test_duration * 0.1);        // skip first max(5s, 10%) of the run
    let warm_from = (test_start + trim) as u64;
    let warm_count: u64 = recv_buckets.iter()
        .filter(|(&k, _)| k >= warm_from)
        .map(|(_, v)| *v)
        .sum();
    let warm_window = (test_duration - trim).max(0.001);
    let warm_eps = warm_count as f64 / warm_window;

    let per_stage_eps: std::collections::HashMap<String, f64> = tel.per_stage_count
        .iter()
        .map(|(k, v)| (k.clone(), if test_duration > 0.0 { *v as f64 / test_duration } else { 0.0 }))
        .collect();

    let throughput_block = json!({
        "sustained_eps":                 round3(sustained_eps),
        "sustained_eps_warmup_trimmed":  round3(warm_eps),
        "warmup_trim_s":                 round3(trim),
        "per_stage_eps":                 per_stage_eps,
    });

    // Latency. E2E percentiles + full sample list (so a Python consumer
    // can compute any percentile it wants) + per-stage percentiles.
    let per_stage_latency_block: serde_json::Map<String, serde_json::Value> = tel
        .per_stage_latency_ms
        .iter()
        .map(|(name, samples)| {
            let mut s = samples.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let stats = json!({
                "count":   s.len(),
                "p50":     pct_r5(&s, 0.50),
                "p95":     pct_r5(&s, 0.95),
                "p99":     pct_r5(&s, 0.99),
                "p999":    pct_r5(&s, 0.999),
                "max":     s.last().copied().unwrap_or(0.0),
                "mean":    mean(&s),
            });
            (name.clone(), stats)
        })
        .collect();

    let latency_block = json!({
        "e2e": {
            "count":   n,
            "p50":     pct_r5(&e2e, 0.50),
            "p90":     pct_r5(&e2e, 0.90),
            "p95":     pct_r5(&e2e, 0.95),
            "p99":     pct_r5(&e2e, 0.99),
            "p999":    pct_r5(&e2e, 0.999),
            "max":     if n > 0 { e2e[n-1] } else { 0.0 },
            "mean":    mean(&e2e),
            // Full sample list enabling arbitrary percentile / CDF
            // computation downstream. Subsampled at 1 in K if the run is
            // large, to keep JSON size bounded — a 10 M event run with
            // raw samples would be ~80 MB of JSON.
            "samples":              subsample_f64(&e2e, 100_000),
            "samples_subsample_rate": subsample_rate(n, 100_000),
        },
        "per_stage": per_stage_latency_block,
    });

    // Scaling. Per-stage replica-count-over-time reconstructed from the
    // event log by prefix-summing spawn/drain actions.
    let scaling_block = build_scaling_block(&tel.scaling_events, stage_names);

    // Inter-stage transport latency: per-edge gap between hop[i].exit_ts and
    // hop[i+1].enter_ts — queue + serialize + network + parse time.
    let inter_stage_block = build_inter_stage_block(&tel.per_event_log);

    // Ingress wait at stage 0 — see CollectorStats::observe for how the
    // samples are produced and why they track the full event population.
    let ingress_wait_block = build_ingress_wait_block(&tel.ingress_wait_ms, stage_names);

    // Per-event raw log. Powers latency-over-time plots and any ad-hoc
    // analysis downstream. Subsampled to EVENTS_SAMPLE_CAP to keep the
    // JSON file reasonable at high rates (300k events * ~100 bytes per
    // row ≈ 30 MB uncapped).
    const EVENTS_SAMPLE_CAP: usize = 100_000;
    // The reservoir replaces random slots, so the live log is unordered;
    // sort by receive time to restore the documented arrival order of `rows`.
    let mut events_sorted = tel.per_event_log.clone();
    events_sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let events_sample = subsample_events(&events_sorted, EVENTS_SAMPLE_CAP);
    let events_block = json!({
        "count":             tel.per_event_log.len(),
        "subsample_rate":    subsample_rate(tel.per_event_log.len(), EVENTS_SAMPLE_CAP),
        // Each row: [recv_t_s, e2e_ms, [[stage, enter_ts, exit_ts], ...]].
        // Compact positional encoding — ~3x smaller than named-field JSON.
        "rows":              events_sample,
    });

    // Resources. Direct sample list — rounded to avoid gigantic floats.
    let resource_cpu: Vec<(f64, f32)> = tel.resource_samples.iter()
        .map(|s| (round3(s.t_s), (s.cpu_pct * 100.0).round() / 100.0))
        .collect();
    let resource_rss: Vec<(f64, u64)> = tel.resource_samples.iter()
        .map(|s| (round3(s.t_s), s.rss_bytes / 1024 / 1024))
        .collect();
    let resources_block = json!({
        "cpu_pct_unit":    "percent_of_available_cpus",
        "cpu_pct_samples": resource_cpu,
        "rss_mb_samples":  resource_rss,
        "sample_count":    tel.resource_samples.len(),
    });

    // Worker timing breakdown.
    let worker_timing_block = build_worker_timing_block(
        &tel.total_us_samples,
        &tel.serde_us_samples,
        &tel.per_stage_latency_ms,
    );

    // Per-replica residence times: stage -> replica -> stats. Built from the
    // `stage#replica` hop labels; empty object on runs without tagged hops.
    let per_replica_block = build_per_replica_block(
        &tel.per_replica_latency_ms,
        &tel.per_replica_count,
    );

    // Dispatcher queue depth time-series per stage.
    let queue_depth_block: serde_json::Map<String, serde_json::Value> = tel
        .queue_depth_samples
        .iter()
        .map(|(stage, samples)| (stage.clone(), json!(samples)))
        .collect();

    json!({
        // ── Run configuration (for cross-run comparison/sweeps) ──────────────
        "run_config":    run_config,

        // ── Paper-grade blocks (primary export) ───────────────────────────────
        "environment":   env_block,
        "counters":      counters_block,
        "throughput":    throughput_block,
        "latency_ms":    latency_block,
        "inter_stage":   inter_stage_block,
        "ingress_wait":  ingress_wait_block,
        "scaling":       scaling_block,
        "resources":     resources_block,
        "events":        events_block,
        "worker_timing": worker_timing_block,
        "per_replica":   per_replica_block,
        "queue_depth":   serde_json::Value::Object(queue_depth_block),

        // ── Legacy fields (kept for existing plot scripts) ────────────────────
        "stage_names":       stage_names,
        "dispatcher_names":  dispatcher_names,
        "test_start_wall":   test_start,
        "duration":          (test_duration * 10.0).round() / 10.0,
        "e2e_count":         n,
        "p50":               pct_r5(&e2e, 0.50),
        "p90":               pct_r5(&e2e, 0.90),
        "p99":               pct_r5(&e2e, 0.99),
        "p999":              pct_r5(&e2e, 0.999),
        "min":               if n > 0 { pct_r5(&e2e, 0.0) } else { 0.0 },
        "max":               if n > 0 { e2e[n-1] }       else { 0.0 },
        "mean":              if n > 0 { (e2e.iter().sum::<f64>() / n as f64 * 1e5).round() / 1e5 } else { 0.0 },
        "cold_start_e2e_ms": if n > 0 { Some(pct_r5(&e2e, 0.0)) } else { None::<f64> },
        "hist_labels":       hist_labels,
        "hist_counts":       hist_counts,
        "cdf_x":             cdf_x,
        "cdf_y":             cdf_y,
        "lat_ts_labels":     lat_ts_labels,
        "recv_per_second":   recv_per_s,
        "lat_ts_p50": [], "lat_ts_p99": [],
        "has_bench_hops":    !tel.per_stage_latency_ms.is_empty(),
        "cold_start_data":   [],
        "cold_start_list":   [],
        "cold_start_milestones": {},
    })
}

/// Build the worker_timing block for the summary JSON.
///
/// For each stage, computes percentile statistics for:
///   - `total_us`  — full worker iteration (recv → deser → wasm → ser → push)
///   - `serde_us`  — JSON deserialization + serialization only
///   - `wasm_us`   — WASM process-event call (derived from per_stage_latency_ms)
///   - `overhead_us` — total_us − wasm_us (everything except WASM itself)
///
/// The `wasm_us` values come from `per_stage_latency_ms` (already collected
/// by the collector thread from bench_hops). `total_us` and `serde_us` come
/// from the autoscaler forwarding worker metric payloads.
fn build_worker_timing_block(
    total_us_samples:  &std::collections::HashMap<String, Vec<u64>>,
    serde_us_samples:  &std::collections::HashMap<String, Vec<u64>>,
    wasm_ms_samples:   &std::collections::HashMap<String, Vec<f64>>,
) -> serde_json::Value {
    // Collect all stage names across all three maps.
    let mut all_stages: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for k in total_us_samples.keys() { all_stages.insert(k.clone()); }
    for k in serde_us_samples.keys() { all_stages.insert(k.clone()); }
    for k in wasm_ms_samples.keys()  { all_stages.insert(k.clone()); }

    if all_stages.is_empty() {
        return serde_json::Value::Object(Default::default());
    }

    let mut per_stage: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

    for stage in &all_stages {
        // total/serde samples arrive in NANOSECONDS (the worker uses as_nanos
        // so sub-µs serialization doesn't floor to 0). Convert to µs as f64
        // here: the summary keys stay named *_us and stay in microseconds for
        // downstream consumers, but carry sub-µs precision.
        let mut total: Vec<f64> = total_us_samples.get(stage).cloned().unwrap_or_default()
            .iter().map(|&ns| ns as f64 / 1000.0).collect();
        let mut serde: Vec<f64> = serde_us_samples.get(stage).cloned().unwrap_or_default()
            .iter().map(|&ns| ns as f64 / 1000.0).collect();
        // wasm residence comes from bench hops in ms → µs.
        let mut wasm_us: Vec<f64> = wasm_ms_samples.get(stage).cloned().unwrap_or_default()
            .iter().map(|&ms| ms * 1000.0).collect();

        total.sort_by(|a, b| a.partial_cmp(b).unwrap());
        serde.sort_by(|a, b| a.partial_cmp(b).unwrap());
        wasm_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Overhead = total - wasm. Pair-wise on sorted arrays is not
        // meaningful, so we compute it from percentile arithmetic. For the
        // sample list we approximate via the means.
        let overhead_p50  = (pct(&total, 0.50) - pct(&wasm_us, 0.50)).max(0.0);
        let overhead_p99  = (pct(&total, 0.99) - pct(&wasm_us, 0.99)).max(0.0);
        let overhead_mean = (mean(&total)       - mean(&wasm_us)).max(0.0);

        // Serde fraction at p50 (how much of total is serialization).
        let serde_frac_p50 = if pct(&total, 0.50) > 0.0 {
            pct(&serde, 0.50) / pct(&total, 0.50)
        } else { 0.0 };

        let n = total.len().max(wasm_us.len()).max(serde.len());

        // 3-decimal µs == ns precision; keeps JSON compact.
        let r3 = |x: f64| (x * 1000.0).round() / 1000.0;

        per_stage.insert(stage.clone(), serde_json::json!({
            "n": n,
            "total_us": {
                "p50":  r3(pct(&total, 0.50)),
                "p95":  r3(pct(&total, 0.95)),
                "p99":  r3(pct(&total, 0.99)),
                "p999": r3(pct(&total, 0.999)),
                "max":  r3(total.last().copied().unwrap_or(0.0)),
                "mean": r3(mean(&total)),
                "samples": subsample_f64(&total, 50_000),
            },
            "serde_us": {
                "p50":  r3(pct(&serde, 0.50)),
                "p95":  r3(pct(&serde, 0.95)),
                "p99":  r3(pct(&serde, 0.99)),
                "max":  r3(serde.last().copied().unwrap_or(0.0)),
                "mean": r3(mean(&serde)),
                "samples": subsample_f64(&serde, 50_000),
            },
            "wasm_us": {
                "p50":  r3(pct(&wasm_us, 0.50)),
                "p95":  r3(pct(&wasm_us, 0.95)),
                "p99":  r3(pct(&wasm_us, 0.99)),
                "p999": r3(pct(&wasm_us, 0.999)),
                "max":  r3(wasm_us.last().copied().unwrap_or(0.0)),
                "mean": r3(mean(&wasm_us)),
                "samples": subsample_f64(&wasm_us, 50_000),
            },
            // overhead = total − wasm (recv + deser + val construction + ser + push)
            "overhead_us": {
                "p50":  r3(overhead_p50),
                "p99":  r3(overhead_p99),
                "mean": r3(overhead_mean),
            },
            "serde_frac_p50": (serde_frac_p50 * 1000.0).round() / 1000.0,
        }));
    }

    serde_json::Value::Object(per_stage)
}

/// Build the per_replica block: `{ stage: { "0": {stats}, "1": {stats} } }`.
///
/// Stats per replica: event count, residence-time percentiles (ms), and a
/// capped sample list for distribution plots (box/violin per replica). The
/// `share` field is this replica's fraction of the stage's events — a direct
/// load-balance fairness readout (1/R when the transport balances perfectly).
fn build_per_replica_block(
    latency_ms: &std::collections::HashMap<String, Vec<f64>>,
    counts:     &std::collections::HashMap<String, u64>,
) -> serde_json::Value {
    use std::collections::BTreeMap;
    if latency_ms.is_empty() {
        return serde_json::Value::Object(Default::default());
    }
    let r4 = |x: f64| (x * 10_000.0).round() / 10_000.0;

    // stage -> replica -> sorted samples
    let mut grouped: BTreeMap<String, BTreeMap<u32, Vec<f64>>> = BTreeMap::new();
    for (label, samples) in latency_ms {
        let mut it = label.splitn(2, '#');
        let stage = it.next().unwrap_or(label).to_string();
        let rep: u32 = it.next().and_then(|r| r.parse().ok()).unwrap_or(0);
        let mut s = samples.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        grouped.entry(stage).or_default().insert(rep, s);
    }

    let mut out: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (stage, reps) in &grouped {
        let stage_total: u64 = reps.keys()
            .map(|r| counts.get(&format!("{stage}#{r}")).copied().unwrap_or(0))
            .sum();
        let mut rep_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        for (rep, samples) in reps {
            let n_evt = counts.get(&format!("{stage}#{rep}")).copied().unwrap_or(samples.len() as u64);
            rep_map.insert(rep.to_string(), json!({
                "count":   n_evt,
                "share":   if stage_total > 0 { r4(n_evt as f64 / stage_total as f64) } else { 0.0 },
                "p50":     r4(pct(samples, 0.50)),
                "p95":     r4(pct(samples, 0.95)),
                "p99":     r4(pct(samples, 0.99)),
                "max":     r4(samples.last().copied().unwrap_or(0.0)),
                "mean":    r4(mean(samples)),
                "samples": subsample_f64(samples, 20_000),
            }));
        }
        out.insert(stage.clone(), serde_json::Value::Object(rep_map));
    }
    serde_json::Value::Object(out)
}

/// Compute per-edge transport-latency distributions from the raw per-event
/// log. An "edge" is a consecutive pair of stages in the hop sequence
/// (e.g. `normalize → detect`); the gap measurement is
/// `hop[i+1].enter_ts − hop[i].exit_ts`, which captures serialize + push +
/// pull + dispatch + worker-recv + parse time between two `process-event`
/// boundaries. Events with fewer than 2 hops contribute nothing.
fn build_inter_stage_block(
    events: &[(f64, f64, Vec<(String, f64, f64)>)],
) -> serde_json::Value {
    use std::collections::HashMap;

    let mut per_edge: HashMap<(String, String), Vec<f64>> = HashMap::new();
    for (_, _, hops) in events {
        for pair in hops.windows(2) {
            let (from_name, _, from_exit) = &pair[0];
            let (to_name, to_enter, _)    = &pair[1];
            if *to_enter >= *from_exit {
                let gap_ms = (to_enter - from_exit) * 1000.0;
                // Hop labels are `stage#replica`; the edge key aggregates
                // over replicas so it stays `relay -> forward`, matching the
                // pipeline topology and existing analyze scripts.
                let from_base = from_name.split('#').next().unwrap_or(from_name).to_string();
                let to_base   = to_name.split('#').next().unwrap_or(to_name).to_string();
                per_edge
                    .entry((from_base, to_base))
                    .or_default()
                    .push(gap_ms);
            }
        }
    }

    if per_edge.is_empty() {
        // Single-stage pipelines land here — signal explicitly so the
        // plotter can print a friendly "no inter-stage data" message.
        return json!({ "edges": [], "note": "single-stage pipeline or no multi-hop events observed" });
    }

    let mut edges: Vec<serde_json::Value> = per_edge.into_iter()
        .map(|((from, to), mut samples)| {
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = samples.len();
            json!({
                "from":      from,
                "to":        to,
                "count":     n,
                "p50":       pct(&samples, 0.50),
                "p95":       pct(&samples, 0.95),
                "p99":       pct(&samples, 0.99),
                "p999":      pct(&samples, 0.999),
                "max":       samples.last().copied().unwrap_or(0.0),
                "mean":      mean(&samples),
                // Sample list for downstream distribution plots. Capped
                // at the same 100k limit used for e2e samples.
                "samples":   subsample_f64(&samples, 100_000),
            })
        })
        .collect();
    // Stable order: by (from, to) alphabetically.
    edges.sort_by(|a, b| {
        let ka = (a["from"].as_str().unwrap_or(""), a["to"].as_str().unwrap_or(""));
        let kb = (b["from"].as_str().unwrap_or(""), b["to"].as_str().unwrap_or(""));
        ka.cmp(&kb)
    });
    json!({ "edges": edges })
}

/// Build the ingress-wait block: time between loadgen "send" and the entry
/// stage's worker starting to process, from the full-population samples
/// collected per event (see `CollectorStats::observe`). Reported under the
/// entry stage's name so the JSON shape is uniform with `inter_stage`.
fn build_ingress_wait_block(
    samples:     &[f64],
    stage_names: &[String],
) -> serde_json::Value {
    if samples.is_empty() || stage_names.is_empty() {
        return json!({ "stages": [], "note": "no ingress wait samples" });
    }

    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();

    json!({
        "stages": [
            {
                "stage":  &stage_names[0],
                "count":  n,
                "p50":    pct(&sorted, 0.50),
                "p95":    pct(&sorted, 0.95),
                "p99":    pct(&sorted, 0.99),
                "p999":   pct(&sorted, 0.999),
                "max":    sorted.last().copied().unwrap_or(0.0),
                "mean":   mean(&sorted),
            }
        ]
    })
}

/// Collect static information about the machine and build the run was
/// produced on. Exposed in the summary so reviewers (and future us)
/// know what hardware a given number came from.
fn build_environment_block() -> serde_json::Value {
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu();
    sys.refresh_memory();

    let cpu_model = sys.cpus().first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let cpu_cores_logical = sys.cpus().len();
    let cpu_cores_physical = sys.physical_core_count().unwrap_or(cpu_cores_logical);
    let ram_total_kb = sys.total_memory() / 1024;

    let os_name = sysinfo::System::name().unwrap_or_else(|| "unknown".to_string());
    let os_version = sysinfo::System::os_version().unwrap_or_else(|| "unknown".to_string());
    let kernel = sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".to_string());
    let host = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());

    // Tool versions. `rustc_version` and `git_commit` are baked at build
    // time by build.rs; they're static strings here.
    // The release version this binary was built at, plus what `git describe`
    // said. Recorded on EVERY run so a results table can always be traced back
    // to the exact build that produced it.
    let version = option_env!("EPICO_VERSION").unwrap_or("unknown");
    let git_tag = option_env!("EPICO_GIT_TAG").unwrap_or("unknown");
    let rustc   = option_env!("EPICO_RUSTC_VERSION").unwrap_or("unknown");
    let commit  = option_env!("EPICO_GIT_COMMIT").unwrap_or("unknown");
    let dirty   = option_env!("EPICO_GIT_DIRTY").unwrap_or("0") == "1";
    // Baked by build.rs from the workspace Cargo.lock, so a dependency bump
    // can't leave a stale version in a results table (this used to be a
    // hardcoded literal).
    let wasmtime_version = option_env!("EPICO_WASMTIME_VERSION").unwrap_or("unknown");

    json!({
        "epico_version":       version,
        "git_tag":             git_tag,
        "host":                host,
        "os_name":             os_name,
        "os_version":          os_version,
        "kernel":              kernel,
        "cpu_model":           cpu_model,
        "cpu_cores_physical":  cpu_cores_physical,
        "cpu_cores_logical":   cpu_cores_logical,
        "ram_total_mb":        ram_total_kb / 1024,
        "rustc":               rustc,
        "wasmtime":            wasmtime_version,
        "git_commit":          commit,
        "git_dirty":           dirty,
    })
}

/// Reconstruct per-stage replica-count timeline and summarize scaling
/// activity from the raw event log. `(t_s, replica_count)` pairs are
/// ready to plot as a step function.
fn build_scaling_block(
    events:      &[ScalingEvent],
    stage_names: &[String],
) -> serde_json::Value {
    let mut per_stage: serde_json::Map<String, serde_json::Value> =
        serde_json::Map::new();

    for name in stage_names {
        let stage_events: Vec<&ScalingEvent> = events.iter()
            .filter(|e| e.stage == *name).collect();

        // Replicas-over-time: start at 0, emit a point for every event.
        let mut replicas: Vec<(f64, usize)> = vec![(0.0, 0)];
        for e in &stage_events {
            replicas.push((round3(e.t_s), e.new_count));
        }

        let scale_up_count   = stage_events.iter().filter(|e| e.action == "spawn"
                                                    || e.action == "cold_start").count();
        let scale_down_count = stage_events.iter().filter(|e| e.action == "drain").count();
        let cold_start_count = stage_events.iter().filter(|e| e.action == "cold_start").count();
        let cold_start_ms: Vec<f64> = stage_events.iter()
            .filter_map(|e| e.cold_start_ms).collect();
        let compile_ms: Vec<f64> = stage_events.iter()
            .filter_map(|e| e.compile_ms).collect();
        let instantiate_pre_ms: Vec<f64> = stage_events.iter()
            .filter_map(|e| e.instantiate_pre_ms).collect();

        per_stage.insert(name.clone(), json!({
            "replicas_over_time":   replicas,
            "scale_up_count":       scale_up_count,
            "scale_down_count":     scale_down_count,
            "cold_start_count":     cold_start_count,
            "cold_start_ms":        cold_start_ms,
            "compile_ms":           compile_ms,
            "instantiate_pre_ms":   instantiate_pre_ms,
            "events":               stage_events,
        }));
    }

    json!({
        "per_stage":    per_stage,
        "total_events": events.len(),
    })
}
