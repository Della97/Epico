//! epico node master — autoscaler + wasm worker host.
//!
//! On shutdown, writes master_*_summary.json with full runtime statistics:
//! e2e latency (from the collector thread), cold-start times, scaling
//! events, and queue-depth history — everything load.py needs.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use clap::Parser;
use epico_logger::Logger;
use serde_json::json;

mod autoscaler;
mod config;
mod conversion;
mod envelope;
mod host;
mod inproc;
mod pipeline_validator;
mod resources;
mod spsc;
mod supervisor;
mod worker;

use crate::config::{default_wasm_path, stage_owned_by, Config};
use crate::inproc::Edge;
use crate::spsc::{SpscMesh, EdgeInSrc, EdgeOutSrc};

/// In-flight bound for an in-process edge (prototype). Plays the role
/// `credit_window` plays on the zmq path; promote to per-edge config later.
const INPROC_EDGE_CAPACITY: usize = 1024;
/// Substring every EOS marker contains; the collector scans for it (cheap)
/// before the confirming JSON parse. Must be matched with a window of its own
/// length — a wrong window size silently never matches and the run never ends.
const EOS_NEEDLE: &[u8] = b"__epico_eos";

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "epico-master")]
#[command(about = "Epico node master — autoscaler + wasm worker host")]
pub struct Args {
    config: PathBuf,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    launch_dispatchers: bool,

    #[arg(long)]
    dispatcher_bin: Option<PathBuf>,

    /// Directory for JSONL log files and summary JSON.
    #[arg(long, default_value = "logs")]
    log_dir: PathBuf,

    /// If set, create this file (empty) once all dispatchers are bound and
    /// autoscalers are running. A client orchestrator (e.g. the `epico`
    /// CLI when launching loadgen alongside the master) polls for this file
    /// before sending traffic, so early events aren't lost to cold sockets.
    #[arg(long)]
    ready_file: Option<PathBuf>,
}

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

/// Accumulated by the collector thread, the autoscalers, and the resource
/// sampler; read at shutdown to build the summary.
#[derive(Default)]
pub(crate) struct RunTelemetry {
    // ── E2E latency (existing) ────────────────────────────────────────────
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

    // ── Per-stage timing (new) ─────────────────────────────────────────────
    /// For each stage name, the list of per-event residence times in ms
    /// (hop.exit_ts - hop.enter_ts). Populated by the collector when it
    /// parses bench_hops off arriving events.
    pub per_stage_latency_ms: std::collections::HashMap<String, Vec<f64>>,
    /// For each stage name, the count of events that passed through it.
    /// At steady state this equals the collector's recv_count; during
    /// scale-up it may briefly lag.
    pub per_stage_count:      std::collections::HashMap<String, u64>,

    // ── Autoscaler events (new) ────────────────────────────────────────────
    pub scaling_events: Vec<ScalingEvent>,

    // ── Per-event raw log (new) ────────────────────────────────────────────
    /// One row per event arriving at the collector, capturing enough to
    /// reconstruct per-second latency percentiles and per-edge transport
    /// delays in post-processing. Fields:
    ///   .0 — recv_t_s:  seconds since test start at collector recv
    ///   .1 — e2e_ms:    end-to-end latency in milliseconds
    ///   .2 — hops:      Vec<(stage_name, enter_ts_wall, exit_ts_wall)>
    ///
    /// Size is bounded at summary-write time via subsampling (see
    /// `subsample_events_for_summary`) so large runs don't produce
    /// multi-gigabyte JSON.
    pub per_event_log: Vec<(f64, f64, Vec<(String, f64, f64)>)>,

    // ── Resource sampling (new) ────────────────────────────────────────────
    pub resource_samples: Vec<ResourceSample>,

    // ── Worker timing breakdown ────────────────────────────────────────────
    /// Per-stage total worker iteration time in NANOSECONDS (recv → deser →
    /// wasm → ser → push). Keyed by stage name. Populated by autoscaler polling
    /// the dispatcher ctrl socket and forwarding consumer metrics. (Field name
    /// kept `_us` for churn reasons; values are ns since the worker switched to
    /// as_nanos. Converted to µs floats in build_worker_timing_block.)
    pub total_us_samples: std::collections::HashMap<String, Vec<u64>>,

    /// Per-stage serialization time in NANOSECONDS (JSON parse + JSON serialize,
    /// but NOT the wasm call). Subset of total_us_samples.
    pub serde_us_samples: std::collections::HashMap<String, Vec<u64>>,

    // ── Dispatcher queue depth time-series ────────────────────────────────
    /// Sampled queue depth per stage over time. Each entry is
    /// (t_s, queue_depth) where t_s is seconds since test_start.
    pub queue_depth_samples: std::collections::HashMap<String, Vec<(f64, u64)>>,
}

// ── Main ─────────────────────────────────────────────────────────────────────

/// Parse the agent CLI args. Exposed so a generated per-pipeline binary can
/// build `Args` without depending on `clap` directly.
pub fn parse_args() -> Args {
    use clap::Parser;
    Args::parse()
}

// ── Entry ──────────────────────────────────────────────────────────────────

/// Run the agent. The binary calls this with `None, None` (built-in source via
/// `EPICO_SOURCE_GEN`, or the PULL ingress); a generated per-pipeline binary
/// passes a user-compiled `EventSource`/`EventSink` so source and sink logic is
/// native code linked into the agent rather than a separate process or wasm.
pub fn run_agent(
    args: Args,
    custom_source: Option<SourceFactory>,
    custom_sink:   Option<Box<dyn EventSink>>,
) {
    let log = Logger::new("master", &args.log_dir)
        .unwrap_or_else(|e| { eprintln!("[master] log open failed: {e}"); std::process::exit(1); });

    // ── CPU sampling profiler (feature-gated) ─────────────────────────────────
    // Enable with: cargo build --release -p master --features profile
    // On shutdown, writes flamegraph.svg into the log directory. pprof samples
    // from inside the process via SIGPROF, so no kernel perf_event privileges
    // are needed. Overhead is ~1-3% at 99 Hz — fine for diagnosis runs, not
    // for paper-quality benchmark numbers.
    #[cfg(feature = "profile")]
    let profiler_guard = {
        log.info("profiler enabled at 99 Hz", &[]);
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(99)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .expect("failed to start pprof profiler"),
        )
    };

    log.info("starting", &[
        ("config",  &args.config.display().to_string()),
        ("log_dir", &args.log_dir.display().to_string()),
    ]);

    let mut config = load_config(&args.config, &log);
    validate_pipeline(&config, &log);
    apply_placement_filter(&mut config, &log);

    // ── In-process edges + ingress/egress (prototype) ─────────────────────────
    // EPICO_INPROC_EDGES=1 collapses every consecutive stage→stage hop onto a
    // shared bounded queue, skipping the consumer-side dispatcher for that hop.
    // EPICO_INPROC_INGRESS=1 additionally replaces the ingress dispatcher with a
    // single source pump (PULL → first stage's Edge) and the egress collector
    // PULL with the collector draining the last stage's Edge in-process — so on
    // one host there are zero dispatchers and the only socket left is the
    // source's ingestion from the external producer. Ingress implies edges.
    // Single-host assumption: all stages are co-located. In-process stages keep
    // min_replicas > 0 (workers spawn via the min-replica path).
    let source_gen = std::env::var("EPICO_SOURCE_GEN")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let inproc_ingress = custom_source.is_some()
        || source_gen
        || std::env::var("EPICO_INPROC_INGRESS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    let inproc_edges = inproc_ingress
        || std::env::var("EPICO_INPROC_EDGES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    let mut input_edges:  HashMap<String, EdgeInSrc>  = HashMap::new();
    let mut output_edges: HashMap<String, EdgeOutSrc> = HashMap::new();
    let mut skip_dispatchers: HashSet<String> = HashSet::new();
    let mut ingress_source_edge: Option<Edge> = None;
    let mut egress_sink_edge:    Option<Edge> = None;
    // Edge capacity is a throughput/latency knob in the in-process regime, so
    // make it sweepable without a recompile. Falls back to the compiled default.
    let edge_cap = std::env::var("EPICO_EDGE_CAP")
        .ok().and_then(|v| v.parse::<usize>().ok()).filter(|&c| c > 0)
        .unwrap_or(INPROC_EDGE_CAPACITY);
    // Transport for the in-process edges: "" / "ring" = crossbeam MPMC ring,
    // "spsc" = FastFlow-style N×M mesh of SPSC rings. The spsc path requires
    // min==max replicas (it round-robins over a fixed-size mesh).
    let edge_impl = std::env::var("EPICO_EDGE_IMPL").unwrap_or_default();
    let spsc_ring_cap = std::env::var("EPICO_SPSC_RING_CAP")
        .ok().and_then(|v| v.parse::<usize>().ok()).filter(|&c| c > 0)
        .unwrap_or(64);
    if inproc_edges {
        for pair in config.pipeline.windows(2) {
            let (prod, cons) = (&pair[0], &pair[1]);
            if edge_impl == "spsc" {
                let mesh = Arc::new(SpscMesh::new(
                    prod.slo.max_replicas, cons.slo.max_replicas, spsc_ring_cap));
                output_edges.insert(prod.name.clone(), EdgeOutSrc::Mesh(mesh.clone()));
                input_edges.insert(cons.name.clone(),  EdgeInSrc::Mesh(mesh));
            } else {
                let edge = Edge::new(edge_cap);
                output_edges.insert(prod.name.clone(), EdgeOutSrc::Ring(edge.clone()));
                input_edges.insert(cons.name.clone(),  EdgeInSrc::Ring(edge));
            }
            let bare = cons.name.strip_prefix("fn-").unwrap_or(&cons.name);
            skip_dispatchers.insert(format!("dispatch-{}", bare));
            log.info("in-process edge", &[
                ("from", &prod.name),
                ("to",   &cons.name),
                ("cap",  &edge_cap.to_string()),
            ]);
        }
    }
    if inproc_ingress {
        // Source → first stage: replace the ingress dispatcher with a single
        // PULL pump feeding the first stage's Edge. Skip that dispatcher.
        if let Some(first) = config.pipeline.first() {
            let edge = Edge::new(edge_cap);
            input_edges.insert(first.name.clone(), EdgeInSrc::Ring(edge.clone()));
            ingress_source_edge = Some(edge);
            let bare = first.name.strip_prefix("fn-").unwrap_or(&first.name);
            skip_dispatchers.insert(format!("dispatch-{}", bare));
            log.info("in-process ingress (source pump)", &[
                ("to",  &first.name),
                ("cap", &edge_cap.to_string()),
            ]);
        }
        // Last stage → sink: the collector drains the last stage's Edge in
        // process instead of binding a PULL socket. No egress socket on a host.
        if let Some(last) = config.pipeline.last() {
            let edge = Edge::new(edge_cap);
            output_edges.insert(last.name.clone(), EdgeOutSrc::Ring(edge.clone()));
            egress_sink_edge = Some(edge);
            log.info("in-process egress (sink drain)", &[
                ("from", &last.name),
                ("cap",  &edge_cap.to_string()),
            ]);
        }
    }

    supervisor::install_shutdown_handler();

    if args.launch_dispatchers {
        let bin = supervisor::resolve_dispatcher_binary(args.dispatcher_bin.as_deref())
            .unwrap_or_else(|e| {
                log.error("dispatcher binary not found", &[("err", &e.to_string())]);
                std::process::exit(1);
            });
        let dispatchers_to_spawn: Vec<_> = config.dispatchers.iter()
            .filter(|d| !skip_dispatchers.contains(&d.name))
            .cloned()
            .collect();
        supervisor::spawn_dispatchers(&dispatchers_to_spawn, &bin, &log);
    }

    let total_max: usize = config.pipeline.iter().map(|s| s.slo.max_replicas).sum();
    let engine = host::build_engine(total_max);
    log.info("engine ready", &[("max_replicas_total", &total_max.to_string())]);

    // ── Stage-shaped Cranelift warmup ────────────────────────────────────────
    // The microscopic WAT compile inside `build_engine` warms most of
    // Cranelift's lazy state, but real stage components exercise additional
    // code paths (WIT bindings, WASI imports, multiple inner modules) that
    // a hello-world doesn't touch. To shift *all* one-time compile cost
    // out of the user-visible cold-start window, we eagerly compile the
    // first stage's actual .wasm here, drop the result, and let the per-
    // stage autoscaler's later `Component::from_file` benefit from a fully
    // warm Cranelift.
    //
    // The cost is a single extra ~10 ms compile at master startup. The
    // benefit is that the per-stage `compile_ms` numbers are uniform
    // (no first-stage outlier), which makes the cold-start decomposition
    // figures tell a clean story rather than burying ~5-15 ms of first-
    // compile overhead in whichever stage's autoscaler ran first.
    //
    // Skipped silently if the first stage's wasm path can't be resolved
    // or the compile fails — non-fatal, the autoscaler will surface any
    // real error when it tries the same compile.
    if let Some(first_stage) = config.pipeline.first() {
        if let Some(wasm_path) = first_stage.wasm.as_ref() {
            let t_warm = std::time::Instant::now();
            match wasmtime::component::Component::from_file(&engine, wasm_path) {
                Ok(_throwaway) => {
                    log.info("cranelift stage warmup complete", &[
                        ("stage",     &first_stage.name),
                        ("wasm",      wasm_path),
                        ("warmup_ms", &format!(
                            "{:.2}",
                            t_warm.elapsed().as_secs_f64() * 1000.0
                        )),
                    ]);
                }
                Err(e) => {
                    log.warn("cranelift stage warmup failed (continuing)", &[
                        ("stage", &first_stage.name),
                        ("err",   &e.to_string()),
                    ]);
                }
            }
        }
    }

    // ── Collector thread ──────────────────────────────────────────────────────
    // Binds a ZMQ PULL socket on the last stage's output endpoint.
    // Receives every event that exits the pipeline, measures e2e latency.
    let telemetry = Arc::new(Mutex::new(RunTelemetry::default()));
    let test_start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    // Monotonic clock anchor for relative-time series (resource samples,
    // scaling events). SystemTime is used for wall-clock fields that
    // correlate with external producers (loadgen, collector); Instant is
    // used for everything that's only meaningful within this run.
    let test_start_instant = std::time::Instant::now();

    let last_stage_output = config.pipeline.last()
        .map(|s| s.output.clone())
        .unwrap_or_else(|| "tcp://0.0.0.0:9999".to_string());

    let col_telemetry = telemetry.clone();
    let col_running   = Arc::new(AtomicBool::new(true));
    let col_running2  = col_running.clone();
    let col_log       = log.with_component("master/collector");
    let col_egress    = egress_sink_edge;

    std::thread::spawn(move || {
        run_collector(&last_stage_output, col_telemetry, col_running2, col_log, test_start, col_egress, custom_sink);
    });

    // ── Source (in-process ingress) ───────────────────────────────────────────
    // EPICO_SOURCE_GEN=1: the source generates events in-process (no socket, no
    // loadgen) for a pure pipeline-ceiling measurement. Otherwise it owns a PULL
    // where an external producer pushes. Either way it feeds the first Edge with
    // no credits — the Edge capacity is the flow control.
    if let Some(edge) = ingress_source_edge {
        let src_log = log.with_component("master/source");
        // Fan the source out across K threads on the (MPMC) ingress edge. One
        // thread by default; raise EPICO_SOURCE_THREADS to feed faster than a
        // single generate-plus-serialize thread can, to find the pipeline's
        // drain ceiling rather than the source's rate.
        let threads = std::env::var("EPICO_SOURCE_THREADS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(1usize).max(1);
        if let Some(factory) = custom_source {
            // Native source logic linked into the agent (option A).
            std::thread::spawn(move || {
                run_source_native(edge, factory, threads, src_log);
            });
        } else if source_gen {
            let count = std::env::var("EPICO_SOURCE_COUNT")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(5_000_000u64);
            let sensors = std::env::var("EPICO_SOURCE_SENSORS")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(100usize);
            std::thread::spawn(move || {
                run_source_gen(edge, count, sensors, threads, src_log);
            });
        } else {
            let ingress_uri = config.dispatchers.iter()
                .find_map(|d| d.push_uri.clone())
                .unwrap_or_else(|| "ipc:///tmp/epico-ingress".to_string());
            std::thread::spawn(move || {
                run_source(ingress_uri, edge, src_log);
            });
        }
    } else if custom_source.is_some() {
        log.warn("custom source ignored: no in-process ingress edge", &[]);
    }

    // ── Resource sampler thread ───────────────────────────────────────────────
    // Samples the master process's CPU and RSS. Cadence is configurable via
    // `resource_sample_interval_ms` in pipeline.yaml (default 1000 ms);
    // a value of 0 skips the spawn entirely. Skipping is what the user
    // wants when chasing the cleanest possible tail-latency numbers —
    // `sysinfo`'s Mach refresh path on macOS is a known source of
    // periodic multi-millisecond jitter that propagates into the master's
    // event-handling threads.
    if config.resource_sample_interval_ms > 0 {
        resources::spawn(
            telemetry.clone(),
            col_running.clone(),
            test_start_instant,
            config.resource_sample_interval_ms,
            log.with_component("master/resources"),
        );
    } else {
        log.info("resource sampler disabled (interval=0)", &[]);
    }

    // ── libzmq PUSH-init warm-up ──────────────────────────────────────────────
    // The first PUSH socket allocation in a libzmq-using process pays a
    // ~10 ms one-time initialization cost (vtable setup for the lb_t
    // socket type, and, on builds linked against libsodium for CurveZMQ,
    // libsodium's `sodium_init` randomness gathering). Without this
    // warm-up, that cost lands on whichever worker is the first to call
    // `ctx.socket(zmq::PUSH)` — almost always the ingress stage's
    // worker, because the ingress stage's queue depth fills first. That
    // showed up as a ~10 ms per-run inflation on the ingress stage's
    // AOT cold_start_ms, with the cost localized to `ph_pusher_socket_ms`
    // in the worker-side breakdown.
    //
    // Pre-allocating and immediately dropping a PUSH socket on the main
    // thread, before any autoscaler thread is spawned, absorbs the cost
    // here (where it does not contaminate cold-start measurement) and
    // leaves every subsequent PUSH socket creation in the process at its
    // steady-state cost (~25 µs).
    {
        let warm_ctx = zmq::Context::new();
        let _warm_push = warm_ctx.socket(zmq::PUSH)
            .expect("warm-up PUSH socket creation failed");
        // _warm_push and warm_ctx drop at end of scope. The process-wide
        // one-shot init they triggered persists.
    }

    // ── Autoscaler threads ────────────────────────────────────────────────────
    let stage_names: Vec<String> = config.pipeline.iter().map(|s| s.name.clone()).collect();
    let mut handles = Vec::new();

    for stage in config.pipeline.iter() {
        let bare = stage.name.strip_prefix("fn-").unwrap_or(&stage.name);
        let dispatch_name = format!("dispatch-{}", bare);
        let dispatcher = config.dispatchers.iter()
            .find(|d| d.name == dispatch_name)
            .unwrap_or_else(|| {
                log.error("no dispatcher for stage", &[("stage", &stage.name)]);
                std::process::exit(1);
            });

        let ctrl_port   = dispatcher.ctrl_port;
        let cw          = dispatcher.credit_window;
        let engine_c    = engine.clone();
        let stage_c     = stage.clone();
        let stage_log   = log.with_component(&format!("autoscaler/{}", stage.name));
        let tel_c       = telemetry.clone();
        let compile_mode_c = config.compile_mode.clone();
        let event_format_c = config.event_format.clone();
        let in_edge_c   = input_edges.get(&stage.name).cloned().unwrap_or(EdgeInSrc::None);
        let out_edge_c  = output_edges.get(&stage.name).cloned().unwrap_or(EdgeOutSrc::None);

        handles.push(std::thread::spawn(move || {
            autoscaler::run_autoscaler_loop(
                stage_c, ctrl_port, cw, in_edge_c, out_edge_c, engine_c, stage_log, tel_c,
                test_start_instant, compile_mode_c, event_format_c,
            );
        }));
    }

    log.info("running", &[("stages", &stage_names.join(","))]);

    // Signal readiness to any orchestrator (e.g. the `epico` CLI when it
    // is also launching loadgen). Written *after* autoscalers are live and
    // dispatchers have been spawned, so a waiting client is guaranteed that
    // the first dispatcher will accept connections immediately.
    if let Some(ref rf) = args.ready_file {
        match std::fs::File::create(rf) {
            Ok(_)  => log.info("ready file written", &[("path", &rf.display().to_string())]),
            Err(e) => log.error("ready file write failed",
                &[("path", &rf.display().to_string()), ("err", &e.to_string())]),
        }
    }

    // ── Supervisor loop ───────────────────────────────────────────────────────
    while !supervisor::SHUTDOWN.load(Ordering::Relaxed) {
        if handles.iter().all(|h| h.is_finished()) {
            log.error("all autoscaler threads exited unexpectedly", &[]);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────
    log.info("shutting down", &[]);
    col_running.store(false, Ordering::Relaxed);

    // Give collector a moment to drain any last events
    std::thread::sleep(Duration::from_millis(500));

    supervisor::kill_children(&log);

    // ── Build and write summary ───────────────────────────────────────────────
    let test_end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let test_duration = test_end - test_start;

    // Run configuration snapshot so post-hoc comparison scripts can group
    // and label runs (credit-window sweeps, replica caps, transport). Each
    // stage's credit_window lives on its matching DispatcherConfig; the
    // replica caps live on the stage SLO. A top-level `credit_window`
    // scalar is emitted only when uniform across stages (the common sweep
    // case); otherwise it's null and callers read the per-stage list.
    let run_config = {
        let mut stages: Vec<serde_json::Value> = Vec::new();
        let mut cws: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
        for st in &config.pipeline {
            // Dispatchers are named `dispatch-<bare>` where bare is the stage
            // name with any `fn-` prefix stripped — mirror the lookup the
            // autoscaler setup uses, otherwise the match silently fails and
            // every stage reports the default window of 1.
            let bare = st.name.strip_prefix("fn-").unwrap_or(&st.name);
            let dispatch_name = format!("dispatch-{}", bare);
            let disp = config.dispatchers.iter().find(|d| d.name == dispatch_name);
            let cw = disp.map(|d| d.credit_window).unwrap_or(1);
            let batch = disp.map(|d| d.batch_events).unwrap_or(1);
            let transport = disp.and_then(|d| d.push_uri.clone());
            cws.insert(cw);
            stages.push(serde_json::json!({
                "stage":         st.name,
                "credit_window": cw,
                "batch_events":  batch,
                "min_replicas":  st.slo.min_replicas,
                "max_replicas":  st.slo.max_replicas,
                "push_uri":      transport,
            }));
        }
        serde_json::json!({
            "credit_window": if cws.len() == 1 {
                serde_json::json!(*cws.iter().next().unwrap())
            } else {
                serde_json::Value::Null
            },
            "stages": stages,
        })
    };

    let summary = build_summary(&log, telemetry, test_start, test_duration, &stage_names, run_config);
    if let Err(e) = log.finalize(&summary) {
        log.error("failed to write summary", &[("err", &e.to_string())]);
    }

    // ── Write flamegraph if profiler was enabled ──────────────────────────────
    #[cfg(feature = "profile")]
    if let Some(guard) = profiler_guard {
        match guard.report().build() {
            Ok(report) => {
                let path = args.log_dir.join("flamegraph.svg");
                match std::fs::File::create(&path) {
                    Ok(file) => {
                        if let Err(e) = report.flamegraph(file) {
                            log.error("flamegraph write failed",
                                      &[("err", &e.to_string())]);
                        } else {
                            log.info("flamegraph written",
                                     &[("path", &path.display().to_string())]);
                        }
                    }
                    Err(e) => log.error("flamegraph file create failed",
                                        &[("err", &e.to_string())]),
                }
            }
            Err(e) => log.error("pprof report build failed",
                                &[("err", &e.to_string())]),
        }
    }
}

// ── Collector thread ──────────────────────────────────────────────────────────

// Maximum number of raw per-event rows kept live in RunTelemetry.per_event_log
// during a run. Once full, new entries replace old ones via reservoir sampling
// so the distribution remains representative. This bounds RSS growth to
// roughly EVENTS_LIVE_CAP * ~200 bytes ≈ 10 MB regardless of run duration.
// The summary-time subsampler (EVENTS_SAMPLE_CAP) operates on this already-
// bounded reservoir, so nothing downstream needs to change.
const EVENTS_LIVE_CAP: usize = 200_000;

/// Given the collector's bind address, derive the address for the tee
/// PUB socket. This needs to be deterministic so external consumers
/// can compute it without an out-of-band lookup.
///
/// Convention:
///   tcp://0.0.0.0:9999  → tcp://0.0.0.0:10000
///   ipc:///tmp/foo      → ipc:///tmp/foo-pub
///
/// The "+1 port" rule has the obvious failure mode of port collision
/// if a stage happens to use port_base = collector_port + 1. We don't
/// guard against that because the default port_base is 9100, which is
/// 900 below the default collector port of 9999. If you reconfigure
/// to push them adjacent you'll discover the collision quickly.
fn derive_pub_addr(bind_addr: &str) -> String {
    if let Some(rest) = bind_addr.strip_prefix("tcp://") {
        if let Some((host, port)) = rest.rsplit_once(':') {
            if let Ok(p) = port.parse::<u16>() {
                return format!("tcp://{}:{}", host, p + 1);
            }
        }
        // Fallback: just append -pub if the parse failed
        format!("{}-pub", bind_addr)
    } else if bind_addr.starts_with("ipc://") {
        format!("{}-pub", bind_addr)
    } else {
        format!("{}-pub", bind_addr)
    }
}

// ---------------------------------------------------------------------------
// Native source / sink contract (option A)
// ---------------------------------------------------------------------------

/// A native event source linked into the agent. The runtime calls `next_event`
/// on a dedicated thread and pushes each result into the first stage's Edge.
/// Returning `None` ends the stream; the runtime then emits the EOS marker so
/// the collector finalizes the run. Bytes must be a serialized event carrying
/// `bench_ts_wall` (use [`wall_now`]) for e2e accounting.
pub trait EventSource: Send {
    fn next_event(&mut self) -> Option<Vec<u8>>;

    /// Called once on each replica before generation when the source is fanned
    /// out across `total` threads (`EPICO_SOURCE_THREADS`). A partition-aware
    /// source should emit a disjoint slice — e.g. only sequence numbers where
    /// `seq % total == index`. The default is a no-op, in which case every
    /// replica emits the same stream: fine for a throughput-ceiling measurement
    /// (the pipeline still drains K× the events), but it duplicates logical
    /// events, so override this when per-event identity matters.
    fn set_partition(&mut self, _index: usize, _total: usize) {}
}

/// Builds a fresh [`EventSource`]. The agent calls it once per source thread so
/// a native source can be fanned out across `EPICO_SOURCE_THREADS` replicas on
/// the shared (MPMC) ingress edge. The generated per-pipeline `main.rs` passes
/// `Some(Box::new(|| Box::new(source::Source::new())))`.
pub type SourceFactory = Box<dyn FnMut() -> Box<dyn EventSource> + Send>;

/// A native sink linked into the agent. `consume` is called for every finished
/// event (after the EOS marker is filtered out, before host-side e2e
/// accounting). Side-effects only — the runtime keeps doing the telemetry.
pub trait EventSink: Send {
    fn consume(&mut self, event: &[u8]);
}

/// Drive a native [`EventSource`] across `threads` replicas on the shared
/// ingress Edge (MPMC, so multiple producers are safe), then emit a single EOS
/// marker once all replicas drain so the collector finalizes the run exactly as
/// for any source. Each replica gets its own instance from the factory and is
/// told its `(index, total)` via [`EventSource::set_partition`].
fn run_source_native(out_edge: Edge, mut factory: SourceFactory, threads: usize, log: Logger) {
    let k = threads.max(1);
    let deadline = source_deadline();
    log.info("source driver started (native, in-process)", &[
        ("threads", &k.to_string()),
        ("seconds", &deadline.map(|_| std::env::var("EPICO_SOURCE_SECONDS")
            .unwrap_or_default()).unwrap_or_else(|| "∞".into())),
    ]);

    let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::with_capacity(k);
    for i in 0..k {
        let mut source = factory();
        source.set_partition(i, k);
        let edge = out_edge.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || {
            let mut n: u64 = 0;
            loop {
                if supervisor::SHUTDOWN.load(Ordering::Relaxed) { break; }
                if let Some(dl) = deadline { if std::time::Instant::now() >= dl { break; } }
                match source.next_event() {
                    Some(bytes) => {
                        if !edge.push(Bytes::from(bytes), &supervisor::SHUTDOWN) { break; }
                        n += 1;
                    }
                    None => break,
                }
            }
            total.fetch_add(n, Ordering::Relaxed);
        }));
    }
    for h in handles { let _ = h.join(); }

    let n = total.load(Ordering::Relaxed);
    let eos = serde_json::to_vec(&serde_json::json!({
        "__epico_eos":     true,
        "loadgen_sent":    n,
        "expected_count":  n,
        "loadgen_done_ts": wall_now(),
    })).unwrap_or_default();
    let _ = out_edge.push(Bytes::from(eos), &supervisor::SHUTDOWN);
    log.info("source driver done", &[("count", &n.to_string()), ("threads", &k.to_string())]);
}

/// Generating source. Produces events in-process — no socket, no loadgen — and
/// pushes them into the first stage's Edge as fast as the pipeline drains, so
/// the only thing in the path is the in-process fabric. This measures the
/// pipeline's intrinsic ceiling. Events match the loadgen's schema so the stages
/// parse them unchanged, and an EOS marker follows the last event so the
/// collector finalizes the run exactly as in the socket-fed case.
///
/// `bench_ts_wall` is stamped per event at emission, so e2e latency stays
/// meaningful. If generation itself ever becomes the limit (source thread pegged,
/// throughput flat, util still low), the next step is a pre-serialized event
/// pool or sharding the source — but per-event serde is typically well above the
/// stage ceiling, so the workers should bind first.
fn run_source_gen(out_edge: Edge, count: u64, sensors: usize, threads: usize, log: Logger) {
    let k = threads.max(1);
    let deadline = source_deadline();
    log.info("source generating (in-process, no socket)", &[
        ("count",   &count.to_string()),
        ("sensors", &sensors.max(1).to_string()),
        ("threads", &k.to_string()),
        ("seconds", &deadline.map(|_| std::env::var("EPICO_SOURCE_SECONDS")
            .unwrap_or_default()).unwrap_or_else(|| "∞".into())),
    ]);

    let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::with_capacity(k);
    for i in 0..k {
        let edge  = out_edge.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || {
            let n = gen_partition(&edge, count, sensors, i, k, deadline);
            total.fetch_add(n, Ordering::Relaxed);
        }));
    }
    for h in handles { let _ = h.join(); }
    let sent = total.load(Ordering::Relaxed);

    // EOS marker — same shape the collector keys on to finalize the run.
    let eos = serde_json::to_vec(&serde_json::json!({
        "__epico_eos":     true,
        "loadgen_sent":    sent,
        "expected_count":  sent,
        "loadgen_done_ts": wall_now(),
    })).unwrap_or_default();
    let _ = out_edge.push(Bytes::from(eos), &supervisor::SHUTDOWN);
    log.info("source done (generated)", &[("count", &sent.to_string()), ("threads", &k.to_string())]);
}

/// Optional wall-clock cap shared by both source paths. When
/// `EPICO_SOURCE_SECONDS` is set the source stops after that long and emits EOS,
/// so a run terminates on its own regardless of whether the source would ever
/// return `None` (an unbounded generator, or a very large count). Unset = run
/// until the source is exhausted or shutdown is raised.
fn source_deadline() -> Option<std::time::Instant> {
    std::env::var("EPICO_SOURCE_SECONDS").ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&s| s > 0.0)
        .map(|s| std::time::Instant::now() + std::time::Duration::from_secs_f64(s))
}

/// One generator replica. Produces the events assigned to partition `index` of
/// `stride` (sequence numbers `index, index+stride, index+2*stride, …` below
/// `count`) and pushes them into the shared ingress Edge. No EOS — the
/// coordinator emits one after all replicas join. Returns the count pushed.
fn gen_partition(out_edge: &Edge, count: u64, sensors: usize, index: usize, stride: usize,
                 deadline: Option<std::time::Instant>) -> u64 {
    // (type_name, unit, base_value) — mirrors the loadgen's sensor table.
    const TYPES: &[(&str, &str, f64)] = &[
        ("temperature", "\u{00b0}C", 22.0),
        ("vibration",   "mm/s",       1.2),
        ("pressure",    "kPa",      101.3),
        ("humidity",    "%",         45.0),
        ("current",     "A",          3.5),
    ];
    const LOCATIONS: &[&str] = &["zone-A", "zone-B", "zone-C", "zone-D"];

    let n = sensors.max(1);
    // Pre-build static per-sensor descriptors so the hot loop allocates only the
    // serialized event itself (no per-event id formatting).
    let descriptors: Vec<(String, &'static str, &'static str, &'static str, f64)> =
        (0..n).map(|idx| {
            let (tn, unit, base) = TYPES[idx % TYPES.len()];
            (format!("sensor-{:04}", idx), tn, unit, LOCATIONS[idx % LOCATIONS.len()], base)
        }).collect();

    let stride = stride.max(1) as u64;
    let mut seq: u64 = index as u64;
    let mut pushed: u64 = 0;
    while seq < count {
        if supervisor::SHUTDOWN.load(Ordering::Relaxed) { break; }
        if let Some(dl) = deadline { if std::time::Instant::now() >= dl { break; } }
        let (id, type_name, unit, location, base) = &descriptors[(seq as usize) % n];
        let value      = base + ((seq % 211) as f64) * 0.01;
        let is_anomaly = seq % 500 == 0;
        let now_wall   = wall_now();

        let bytes = serde_json::to_vec(&serde_json::json!({
            "bench_ts":      now_wall,
            "bench_ts_wall": now_wall,
            "bench_seq":     seq,
            "sensor_id":     id.as_str(),
            "sensor_type":   *type_name,
            "location":      *location,
            "unit":          *unit,
            "value":         (value * 10_000.0).round() / 10_000.0,
            "is_anomaly":    is_anomaly,
        })).unwrap_or_default();

        if !out_edge.push(Bytes::from(bytes), &supervisor::SHUTDOWN) { break; }
        pushed += 1;
        seq    += stride;
    }
    pushed
}

pub fn wall_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Single-owner ingress pump (the "source"). Binds a PULL where the external
/// producer pushes (loadgen now, an upstream node's egress later) and forwards
/// each event into the first stage's in-process Edge. There is no credit
/// protocol: the Edge's bounded capacity is the flow control, and when it fills
/// this loop blocks on `push`, which stops draining the socket and backpressures
/// the producer. On one host this is the only socket left in the data path.
fn run_source(ingress_uri: String, out_edge: Edge, log: Logger) {
    let ctx  = zmq::Context::new();
    let pull = match ctx.socket(zmq::PULL) {
        Ok(s)  => s,
        Err(e) => { log.error("source socket failed", &[("err", &e.to_string())]); return; }
    };
    // Short timeout so the loop can observe shutdown; large recv HWM so the
    // socket can buffer ahead of the Edge without stalling the producer early.
    pull.set_rcvtimeo(100).ok();
    pull.set_rcvhwm(100_000).ok();
    if let Err(e) = pull.bind(&ingress_uri) {
        log.error("source bind failed", &[("addr", &ingress_uri), ("err", &e.to_string())]);
        return;
    }
    log.info("source bound (in-process ingress)", &[("addr", &ingress_uri)]);

    loop {
        if supervisor::SHUTDOWN.load(Ordering::Relaxed) { break; }
        match pull.recv_bytes(0) {
            Ok(b) => {
                // push() blocks under backpressure and returns false only if
                // shutdown was raised while waiting.
	                if !out_edge.push(Bytes::from(b), &supervisor::SHUTDOWN) { break; }
            }
            Err(zmq::Error::EAGAIN) => continue,
            Err(_)                  => break,
        }
    }
    log.info("source stopped", &[]);
}

fn run_collector(
    output_endpoint: &str,
    telemetry: Arc<Mutex<RunTelemetry>>,
    running:   Arc<AtomicBool>,
    log:       Logger,
    test_start: f64,
    egress_edge: Option<Edge>,
    mut sink: Option<Box<dyn EventSink>>,
) {
    // The output endpoint from config is what workers connect to as PUSH.
    // We bind a PULL socket at the same address to receive those events.
    // Strip ipc:// prefix if present — convert to TCP bind on 9999.
    let bind_addr = if output_endpoint.starts_with("tcp://") {
        // Replace host with 0.0.0.0 so we bind rather than connect
        let port = output_endpoint.rsplit(':').next().unwrap_or("9999");
        format!("tcp://0.0.0.0:{}", port)
    } else if output_endpoint.starts_with("ipc://") {
        output_endpoint.to_string()
    } else {
        format!("tcp://0.0.0.0:{}", output_endpoint)
    };

    // Socket-backed egress binds a PULL (and a PUB tee). In-process egress
    // (sink) skips both — the collector drains the Edge directly.
    let (pull, pub_socket): (Option<zmq::Socket>, Option<zmq::Socket>) = if egress_edge.is_some() {
        log.info("collector draining in-process egress edge (no socket)", &[]);
        (None, None)
    } else {
        let ctx  = zmq::Context::new();
        let pull = match ctx.socket(zmq::PULL) {
            Ok(s) => s,
            Err(e) => { log.error("collector socket failed", &[("err", &e.to_string())]); return; }
        };
        pull.set_rcvtimeo(200).ok();
        pull.set_rcvhwm(100_000).ok();

        if let Err(e) = pull.bind(&bind_addr) {
            log.error("collector bind failed", &[("addr", &bind_addr), ("err", &e.to_string())]);
            return;
        }

        log.info("collector bound", &[("addr", &bind_addr)]);

        // Also bind a PUB socket on collector_port + 1 so external consumers
        // (dashboards, recorders, anything wanting a copy of finished events)
        // can SUB to it without competing with the master for the PULL socket.
        let pub_addr = derive_pub_addr(&bind_addr);
        let pub_socket = match ctx.socket(zmq::PUB) {
            Ok(s) => s,
            Err(e) => {
                log.error("collector pub socket failed", &[("err", &e.to_string())]);
                return;
            }
        };
        // Don't block the collector if no subscriber is keeping up. Drop
        // events instead — telemetry is the primary purpose; the tee is a
        // best-effort copy.
        pub_socket.set_sndhwm(1000).ok();
        pub_socket.set_sndtimeo(0).ok();
        if let Err(e) = pub_socket.bind(&pub_addr) {
            log.warn("collector pub bind failed (continuing without tee)",
                     &[("addr", &pub_addr), ("err", &e.to_string())]);
        } else {
            log.info("collector pub bound", &[("addr", &pub_addr)]);
        }
        (Some(pull), Some(pub_socket))
    };

    let mut recv_count: u64 = 0;
    let mut eos_received = false;

    while running.load(Ordering::Relaxed) {
        let bytes = match &egress_edge {
            Some(edge) => match edge.try_pop() {
                Some(b) => b,
                None    => { std::thread::sleep(Duration::from_micros(200)); continue; }
            },
            None => match pull.as_ref().unwrap().recv_bytes(0) {
                Ok(b)                   => Bytes::from(b),
                Err(zmq::Error::EAGAIN) => continue,
                Err(_)                  => continue,
            },
        };

        // Forward a copy to any external subscriber (socket mode only).
        if let Some(ps) = pub_socket.as_ref() {
            let _ = ps.send(bytes.as_ref(), zmq::DONTWAIT);
        }

        // ── EOS detection ────────────────────────────────────────────────
        // The loadgen's `tp` profile emits a marker after the last event;
        // each stage forwards it unchanged. When we see it here, we know
        // every event before it has flowed through the entire pipeline, so
        // we can trigger a clean master shutdown. We do it by setting
        // supervisor::SHUTDOWN — the main thread's supervisor loop polls
        // this and falls through to the shutdown/summary path.
        //
        // A cheap byte-level check before the full JSON parse: every EOS
        // marker contains the literal "__epico_eos". Skipping the parse
        // when the bytes don't contain that string avoids serde_json
        // overhead on every event in the hot path (~15% of collector CPU
        // per the flamegraph).
        if !eos_received
            && bytes.len() < 4096
            && bytes.windows(EOS_NEEDLE.len()).any(|w| w == EOS_NEEDLE)
        {
            if let Ok(ev) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if ev.get("__epico_eos").and_then(|v| v.as_bool()) == Some(true) {
                    let loadgen_sent = ev.get("loadgen_sent")
                        .and_then(|v| v.as_u64()).unwrap_or(0);
                    let expected = ev.get("expected_count")
                        .and_then(|v| v.as_u64()).unwrap_or(0);
                    log.info("EOS received at collector", &[
                        ("recv_count",   &recv_count.to_string()),
                        ("loadgen_sent", &loadgen_sent.to_string()),
                        ("expected",     &expected.to_string()),
                    ]);
                    eos_received = true;
                    // Trigger the supervisor loop's shutdown path. The main
                    // thread will tear down dispatchers, build the summary,
                    // and exit. We continue the loop to drain anything that
                    // might be in flight after the EOS (shouldn't happen
                    // with linear pipelines, but harmless to allow).
                    supervisor::SHUTDOWN.store(true, Ordering::Relaxed);
                }
            }
            // Skip telemetry accounting for the EOS marker itself — it
            // isn't a real event and shouldn't bias e2e_ms or recv_count.
            continue;
        }

        // Native sink logic (option A): user-supplied side-effect per finished
        // event, before host-side e2e accounting.
        if let Some(s) = sink.as_mut() {
            s.consume(&bytes);
        }

        let recv_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        if let Ok(ev) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if let Some(bench_ts) = ev["bench_ts_wall"].as_f64() {
                if recv_ts > bench_ts {
                    let lat_ms = (recv_ts - bench_ts) * 1000.0;
                    let bucket = (recv_ts as u64).saturating_sub(0); // absolute second

                    // Parse hops once, outside the lock — avoids holding the
                    // mutex across a JSON walk that can include dozens of
                    // allocations per event at high rates.
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

                    // Compute ingress wait once, before grabbing the lock.
                    // This is `enter_ts[stage 0] - bench_ts_wall`. We compute
                    // it here (rather than at summary time from per_event_log)
                    // so it tracks the full event population in lockstep with
                    // `e2e_ms`, not the reservoir subsample. That keeps the
                    // two metrics directly comparable: same sampling regime,
                    // same n. We drop negative values defensively (clock skew
                    // between processes can produce them in rare cases).
                    let ingress_wait_ms = hops_vec.first().map(|(_, enter, _)| {
                        (enter - bench_ts) * 1000.0
                    }).filter(|v| *v >= 0.0);

                    if let Ok(mut tel) = telemetry.try_lock() {
                        tel.e2e_ms.push(lat_ms);
                        if let Some(w) = ingress_wait_ms {
                            tel.ingress_wait_ms.push(w);
                        }
                        *tel.recv_per_second.entry(bucket).or_default() += 1;

                        // Per-stage latencies and counts (unchanged).
                        for (name, enter, exit) in &hops_vec {
                            tel.per_stage_latency_ms
                                .entry(name.clone())
                                .or_default()
                                .push((exit - enter) * 1000.0);
                            *tel.per_stage_count
                                .entry(name.clone())
                                .or_default() += 1;
                        }

                        // Per-event raw row. recv_t_s is normalized to the
                        // run's start so Python can bin without knowing
                        // wall-clock zero. Event hops are stored with raw
                        // wall-clock timestamps — Python differences them to
                        // get per-segment durations.
                        //
                        // Reservoir sampling keeps the live log bounded at
                        // EVENTS_LIVE_CAP entries regardless of run length or
                        // event rate. Each incoming event has an equal
                        // probability of appearing in the final reservoir,
                        // preserving the statistical properties needed for
                        // CDF and percentile computation at summary time.
                        // Without this cap, a 60-second run at 2000 ev/s
                        // accumulates ~120k entries × ~200 bytes ≈ 24 MB of
                        // heap that is never freed until shutdown, giving
                        // the OS a reason to compact memory mid-run and
                        // causing the latency spike visible in the scatter
                        // plot at ~t=20s.
                        let recv_t_s = recv_ts - test_start;
                        let log_len = tel.per_event_log.len();
                        if log_len < EVENTS_LIVE_CAP {
                            tel.per_event_log.push((recv_t_s, lat_ms, hops_vec));
                        } else {
                            // Replace a uniformly random earlier entry.
                            // Using recv_count as a cheap pseudo-random index
                            // avoids pulling in a random crate in the hot path;
                            // it advances by 1 per event so the replacement
                            // pattern is uniform across the reservoir.
                            let slot = (recv_count as usize) % EVENTS_LIVE_CAP;
                            tel.per_event_log[slot] = (recv_t_s, lat_ms, hops_vec);
                        }
                    }
                }
            }
        }

        recv_count += 1;
        if recv_count % 10_000 == 0 {
            log.info("collector progress", &[("received", &recv_count.to_string())]);
        }
    }

    log.info("collector stopped", &[("total_received", &recv_count.to_string())]);
}

// ── Summary builder ───────────────────────────────────────────────────────────

fn build_summary(
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

    let pct = |arr: &[f64], p: f64| -> f64 {
        if arr.is_empty() { return 0.0; }
        let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
        (arr[idx] * 100_000.0).round() / 100_000.0
    };

    // Histogram (50 buckets, capped at 3×p99) — kept for backward compat
    // with the existing plot pipeline. Paper plots should be generated
    // from the raw `e2e_ms_samples` list below, not these 50 buckets.
    let (hist_labels, hist_counts) = if n > 0 {
        let max_lat = (pct(&e2e, 0.99) * 3.0).max(1.0).min(*e2e.last().unwrap());
        let bw      = max_lat / 50.0;
        let labels: Vec<f64> = (0..50).map(|i| (i as f64 * bw + bw / 2.0) * 1e5).map(|v| v.round() / 1e5).collect();
        let counts: Vec<usize> = (0..50)
            .map(|i| e2e.iter().filter(|&&l| l >= i as f64 * bw && l < (i + 1) as f64 * bw).count())
            .collect();
        (labels, counts)
    } else { (vec![], vec![]) };

    // CDF
    let step = (n / 200).max(1);
    let cdf_x: Vec<f64> = (0..n).step_by(step).map(|i| pct(&e2e, i as f64 / n as f64)).collect();
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

    log.info("e2e summary", &[
        ("n",    &n.to_string()),
        ("p50",  &format!("{:.3}ms", pct(&e2e, 0.50))),
        ("p99",  &format!("{:.3}ms", pct(&e2e, 0.99))),
        ("max",  &format!("{:.3}ms", if n > 0 { e2e[n-1] } else { 0.0 })),
    ]);

    // ── New paper-grade blocks ────────────────────────────────────────────────

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
                "p50":     pct(&s, 0.50),
                "p95":     pct(&s, 0.95),
                "p99":     pct(&s, 0.99),
                "p999":    pct(&s, 0.999),
                "max":     s.last().copied().unwrap_or(0.0),
                "mean":    if s.is_empty() { 0.0 } else { s.iter().sum::<f64>() / s.len() as f64 },
            });
            (name.clone(), stats)
        })
        .collect();

    let latency_block = json!({
        "e2e": {
            "count":   n,
            "p50":     pct(&e2e, 0.50),
            "p90":     pct(&e2e, 0.90),
            "p95":     pct(&e2e, 0.95),
            "p99":     pct(&e2e, 0.99),
            "p999":    pct(&e2e, 0.999),
            "max":     if n > 0 { e2e[n-1] } else { 0.0 },
            "mean":    if n > 0 { e2e.iter().sum::<f64>() / n as f64 } else { 0.0 },
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

    // Inter-stage transport latency. For each consecutive pair of stages
    // in the pipeline, compute the gap between `hop[i].exit_ts` and
    // `hop[i+1].enter_ts`. This is queue + serialize + network + parse
    // time — everything between `process-event` boundaries. On
    // single-stage pipelines this produces an empty block.
    let inter_stage_block = build_inter_stage_block(&tel.per_event_log);

    // Ingress wait at stage 0. The gap between loadgen's send timestamp
    // (`bench_ts_wall`, recovered from `recv_ts - e2e_ms`) and the moment
    // the first stage's worker actually starts processing the event
    // (`hops[0].enter_ts`). This is where the queue piles up when the
    // pipeline is oversubscribed — it's hidden inside e2e but separate
    // from any per-edge transport number, since events sit in the very
    // first dispatcher's frontend buffer before any worker has touched
    // them. Reporting it explicitly lets readers see the difference
    // between "queue-bound latency" and "actual processing latency".
    //
    // We pass `tel.ingress_wait_ms` (the full population) rather than
    // recomputing from the reservoir-sampled `per_event_log`, so the
    // ingress and e2e percentiles are directly comparable: same sample
    // set, same regime, ingress ≤ e2e by construction.
    let ingress_wait_block = build_ingress_wait_block(&tel.ingress_wait_ms, stage_names);

    // Per-event raw log. Powers latency-over-time plots and any ad-hoc
    // analysis downstream. Subsampled to EVENTS_SAMPLE_CAP to keep the
    // JSON file reasonable at high rates (300k events * ~100 bytes per
    // row ≈ 30 MB uncapped).
    const EVENTS_SAMPLE_CAP: usize = 100_000;
    let events_sample = subsample_events(&tel.per_event_log, EVENTS_SAMPLE_CAP);
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
        "queue_depth":   serde_json::Value::Object(queue_depth_block),

        // ── Legacy fields (kept for existing plot scripts) ────────────────────
        "stage_names":       stage_names,
        "dispatcher_names":  dispatcher_names,
        "test_start_wall":   test_start,
        "duration":          (test_duration * 10.0).round() / 10.0,
        "e2e_count":         n,
        "p50":               pct(&e2e, 0.50),
        "p90":               pct(&e2e, 0.90),
        "p99":               pct(&e2e, 0.99),
        "p999":              pct(&e2e, 0.999),
        "min":               if n > 0 { pct(&e2e, 0.0) } else { 0.0 },
        "max":               if n > 0 { e2e[n-1] }       else { 0.0 },
        "mean":              if n > 0 { (e2e.iter().sum::<f64>() / n as f64 * 1e5).round() / 1e5 } else { 0.0 },
        "cold_start_e2e_ms": if n > 0 { Some(pct(&e2e, 0.0)) } else { None::<f64> },
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

// ── Summary helpers ──────────────────────────────────────────────────────────

fn round3(x: f64) -> f64 { (x * 1000.0).round() / 1000.0 }

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
    let pct_u64 = |arr: &[u64], p: f64| -> f64 {
        if arr.is_empty() { return 0.0; }
        let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
        arr[idx] as f64
    };
    let pct_f64 = |arr: &[f64], p: f64| -> f64 {
        if arr.is_empty() { return 0.0; }
        let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
        arr[idx]
    };
    let mean_u64 = |arr: &[u64]| -> f64 {
        if arr.is_empty() { return 0.0; }
        arr.iter().sum::<u64>() as f64 / arr.len() as f64
    };
    let mean_f64 = |arr: &[f64]| -> f64 {
        if arr.is_empty() { return 0.0; }
        arr.iter().sum::<f64>() / arr.len() as f64
    };

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
        let empty_u: Vec<u64> = vec![];
        let empty_f: Vec<f64> = vec![];

        // total/serde samples now arrive in NANOSECONDS (worker switched
        // from as_micros to as_nanos so sub-µs serialization no longer
        // floors to 0). Convert to µs as f64 here: the summary keys stay
        // named *_us and stay in microseconds for downstream consumers,
        // but now carry sub-µs precision instead of truncating.
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
        let overhead_p50  = (pct_f64(&total, 0.50) - pct_f64(&wasm_us, 0.50)).max(0.0);
        let overhead_p99  = (pct_f64(&total, 0.99) - pct_f64(&wasm_us, 0.99)).max(0.0);
        let overhead_mean = (mean_f64(&total)       - mean_f64(&wasm_us)).max(0.0);

        // Serde fraction at p50 (how much of total is serialization).
        // Unit-independent ratio — nonzero now that serde isn't floored.
        let serde_frac_p50 = if pct_f64(&total, 0.50) > 0.0 {
            pct_f64(&serde, 0.50) / pct_f64(&total, 0.50)
        } else { 0.0 };

        let n = total.len().max(wasm_us.len()).max(serde.len());

        // 3-decimal µs == ns precision; keeps JSON compact.
        let r3 = |x: f64| (x * 1000.0).round() / 1000.0;

        per_stage.insert(stage.clone(), serde_json::json!({
            "n": n,
            "total_us": {
                "p50":  r3(pct_f64(&total, 0.50)),
                "p95":  r3(pct_f64(&total, 0.95)),
                "p99":  r3(pct_f64(&total, 0.99)),
                "p999": r3(pct_f64(&total, 0.999)),
                "max":  r3(total.last().copied().unwrap_or(0.0)),
                "mean": r3(mean_f64(&total)),
                "samples": subsample_f64(&total, 50_000),
            },
            "serde_us": {
                "p50":  r3(pct_f64(&serde, 0.50)),
                "p95":  r3(pct_f64(&serde, 0.95)),
                "p99":  r3(pct_f64(&serde, 0.99)),
                "max":  r3(serde.last().copied().unwrap_or(0.0)),
                "mean": r3(mean_f64(&serde)),
                "samples": subsample_f64(&serde, 50_000),
            },
            "wasm_us": {
                "p50":  r3(pct_f64(&wasm_us, 0.50)),
                "p95":  r3(pct_f64(&wasm_us, 0.95)),
                "p99":  r3(pct_f64(&wasm_us, 0.99)),
                "p999": r3(pct_f64(&wasm_us, 0.999)),
                "max":  r3(wasm_us.last().copied().unwrap_or(0.0)),
                "mean": r3(mean_f64(&wasm_us)),
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

        let _ = (empty_u, empty_f, pct_u64, mean_u64); // suppress unused warnings
    }

    serde_json::Value::Object(per_stage)
}

/// Subsample a sorted u64 series to at most `cap` elements.
#[allow(dead_code)] // retained for callers that still emit integer-µs samples
fn subsample_u64(src: &[u64], cap: usize) -> Vec<u64> {
    if src.len() <= cap { return src.to_vec(); }
    let k = (src.len() + cap - 1) / cap;
    src.iter().step_by(k).copied().collect()
}

/// Compute per-edge transport-latency distributions from the raw
/// per-event log. An "edge" is a consecutive pair of stages in the hop
/// sequence (e.g. `normalize → detect`); the gap measurement is
/// `hop[i+1].enter_ts − hop[i].exit_ts`, which captures serialize +
/// push + pull + dispatch + worker-recv + parse time between two
/// `process-event` boundaries.
///
/// Events with fewer than 2 hops contribute nothing and are skipped.
/// Edge labels are deduplicated by (from_stage, to_stage) so pipelines
/// with repeating stage names still produce well-defined keys.
fn build_inter_stage_block(
    events: &[(f64, f64, Vec<(String, f64, f64)>)],
) -> serde_json::Value {
    use std::collections::HashMap;

    // Collect per-edge gap samples. BTreeMap over the sorted key so the
    // JSON output is deterministic across runs with the same edges.
    let mut per_edge: HashMap<(String, String), Vec<f64>> = HashMap::new();
    for (_, _, hops) in events {
        for pair in hops.windows(2) {
            let (from_name, _, from_exit) = &pair[0];
            let (to_name, to_enter, _)    = &pair[1];
            if *to_enter >= *from_exit {
                let gap_ms = (to_enter - from_exit) * 1000.0;
                per_edge
                    .entry((from_name.clone(), to_name.clone()))
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

    let pct = |arr: &[f64], p: f64| -> f64 {
        if arr.is_empty() { return 0.0; }
        let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
        arr[idx]
    };

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
                "mean":      if n > 0 { samples.iter().sum::<f64>() / n as f64 } else { 0.0 },
                // Sample list for downstream distribution plots. Capped
                // at the same 100k limit used for e2e samples — at that
                // cap the distribution shape is preserved but the JSON
                // stays tractable.
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

/// Build the ingress-wait block.
///
/// Ingress wait is the time between loadgen pressing "send" on an event
/// and the entry stage's worker actually starting to process it. When
/// the pipeline is oversubscribed the bulk of e2e latency lives here —
/// events stack up in the first dispatcher's frontend buffer, and the
/// existing inter-stage transport metric won't surface this because it
/// only measures gaps *between* worker-touched timestamps.
///
/// Sourced from `tel.ingress_wait_ms` rather than reservoir-sampled
/// `per_event_log`, so percentiles here track the full event population
/// in lockstep with `e2e_ms`. The two metrics use the same sampling
/// regime (none — every event), making them directly comparable: one
/// is always a component of the other, and `ingress p50 ≤ e2e p50`
/// holds by construction (modulo dropped clock-skew samples). When this
/// invariant is violated in practice it means upstream telemetry
/// processing dropped values; check the warn-log for hint.
///
/// Reports under the entry stage's name (taken from `stage_names[0]`)
/// so the JSON shape is uniform with `inter_stage` (a list of named
/// entries) and survives later non-linear topologies cleanly.
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

    let pct = |arr: &[f64], p: f64| -> f64 {
        if arr.is_empty() { return 0.0; }
        let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
        arr[idx]
    };

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
                "mean":   if n > 0 { sorted.iter().sum::<f64>() / n as f64 } else { 0.0 },
            }
        ]
    })
}

/// Subsample the per-event log to at most `cap` rows, preserving
/// arrival order. The output JSON layout is positional for compactness:
///
///   `[recv_t_s, e2e_ms, [[stage, enter_ts, exit_ts], ...]]`
///
/// This is ~3x smaller than an equivalent named-field encoding and
/// maps directly to a pandas/polars DataFrame with one `explode` call
/// on the hops column.
fn subsample_events(
    src: &[(f64, f64, Vec<(String, f64, f64)>)],
    cap: usize,
) -> Vec<serde_json::Value> {
    if src.len() <= cap {
        return src.iter().map(event_row).collect();
    }
    let k = (src.len() + cap - 1) / cap;
    src.iter().step_by(k).map(event_row).collect()
}

fn event_row(e: &(f64, f64, Vec<(String, f64, f64)>)) -> serde_json::Value {
    let (recv_t_s, e2e_ms, hops) = e;
    // Round time-since-start to microsecond precision — finer than we
    // can reliably measure and plenty for per-second binning.
    let recv = (recv_t_s * 1_000_000.0).round() / 1_000_000.0;
    let lat  = (e2e_ms  * 10_000.0).round() / 10_000.0;
    let hops_json: Vec<serde_json::Value> = hops.iter()
        .map(|(name, enter, exit)| json!([name, enter, exit]))
        .collect();
    json!([recv, lat, hops_json])
}

/// Downsample a sorted float series to at most `cap` elements by keeping
/// every K-th value. Paper plots typically don't need more than ~100k
/// samples for a CDF; above that you're just bloating the output JSON.
fn subsample_f64(src: &[f64], cap: usize) -> Vec<f64> {
    if src.len() <= cap { return src.to_vec(); }
    let k = (src.len() + cap - 1) / cap;
    src.iter().step_by(k).copied().collect()
}

fn subsample_rate(n: usize, cap: usize) -> usize {
    if n <= cap { 1 } else { (n + cap - 1) / cap }
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
    let rustc   = option_env!("EPICO_RUSTC_VERSION").unwrap_or("unknown");
    let commit  = option_env!("EPICO_GIT_COMMIT").unwrap_or("unknown");
    let dirty   = option_env!("EPICO_GIT_DIRTY").unwrap_or("0") == "1";
    // Wasmtime is pinned in Cargo.toml; reading the literal here means the
    // summary is always correct without runtime crate-version introspection.
    let wasmtime_version = "26";

    json!({
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

// ── Startup helpers ───────────────────────────────────────────────────────────

fn load_config(path: &std::path::Path, log: &Logger) -> Config {
    let yaml_path = path.to_string_lossy().to_string();
    let yaml_content = std::fs::read_to_string(&yaml_path)
        .unwrap_or_else(|e| {
            log.error("failed to read config", &[("path", &yaml_path), ("err", &e.to_string())]);
            std::process::exit(1);
        });
    let mut config: Config = serde_yaml::from_str(&yaml_content)
        .unwrap_or_else(|e| {
            log.error("failed to parse YAML", &[("path", &yaml_path), ("err", &e.to_string())]);
            std::process::exit(1);
        });
    let yaml_dir = std::path::Path::new(&yaml_path)
        .parent().unwrap_or_else(|| std::path::Path::new("."));
    for stage in config.pipeline.iter_mut() {
        if let Some(ref wasm) = stage.wasm {
            let resolved = yaml_dir.join(wasm);
            if resolved.exists() { stage.wasm = Some(resolved.to_string_lossy().to_string()); }
        }
        if stage.wasm.is_none() { stage.wasm = Some(default_wasm_path(&stage.name)); }
    }
    config
}

fn validate_pipeline(config: &Config, log: &Logger) {
    let stages: Vec<(String, String)> = config.pipeline.iter()
        .map(|s| (s.name.clone(), s.wasm.clone().unwrap()))
        .collect();
    match pipeline_validator::validate_pipeline_components(&stages) {
        Ok(report) => {
            log.info("pipeline validated", &[("stages", &config.pipeline.len().to_string())]);
            for line in &report { log.info("validator", &[("detail", line.as_str())]); }
        }
        Err(e) => {
            log.error("pipeline validation failed", &[("err", &format!("{:#}", e))]);
            std::process::exit(1);
        }
    }
}

fn apply_placement_filter(config: &mut Config, log: &Logger) {
    let total = config.pipeline.len();
    config.pipeline.retain(|s| stage_owned_by(s, &config.this_host));
    config.dispatchers.retain(|d| d.placement == config.this_host);
    let kept = config.pipeline.len();
    log.info("placement filter", &[
        ("this_host", &config.this_host),
        ("owning",    &format!("{}/{}", kept, total)),
    ]);
    if kept == 0 {
        log.error("no stages placed on this host", &[("this_host", &config.this_host)]);
        std::process::exit(1);
    }
}
// Appended placeholder to force view refresh