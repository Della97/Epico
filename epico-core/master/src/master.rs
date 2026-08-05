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
use epico_logger::{error, info, warn};

mod autoscaler;
mod config;
mod conversion;
pub mod envelope;
mod eos;
mod host;
mod inproc;
mod morph;
mod pipeline_validator;
mod spsc;
mod supervisor;
mod telemetry;
pub mod typed;
mod worker;

// Re-exported so `typed`'s public trait signatures don't leak private paths.
pub use host::HostState;

// Re-exported so native source/sink crates (compiled into the per-pipeline
// agent) can build the binary envelope without declaring their own dependency:
// `epico_master::wire::EventBuilder`.
pub use epico_wire as wire;

use crate::config::{default_wasm_path, stage_owned_by, Config};
use crate::inproc::Edge;
use crate::spsc::{SpscMesh, EdgeInSrc, EdgeOutSrc};
use crate::telemetry::{collector::CollectorStats, summary::build_summary, RunTelemetry};

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

    // Compile-time features, into the record. `cold-start-opt` changes the
    // wasmtime config (pooling allocator, CoW init) and so changes what the
    // cold-start numbers in this run mean — but it is baked in at bootstrap
    // and was previously only announced on stderr, which no log file keeps.
    info!(log, "epico",
          version = option_env!("EPICO_VERSION").unwrap_or("unknown"),
          git_tag = option_env!("EPICO_GIT_TAG").unwrap_or("unknown"),
          cold_start_opt = cfg!(feature = "cold-start-opt"));

    // Publish the run directory so the dispatcher children spawned later join
    // this run's folder rather than each minting their own. A no-op when the
    // CLI launched us (it already set this); it is the directly-launched agent
    // that needs it. Safe here: still single-threaded, before any spawn.
    std::env::set_var(epico_logger::RUN_DIR_ENV, &log.run_dir);

    // ── CPU sampling profiler (feature-gated) ─────────────────────────────────
    // Enable with: cargo build --release -p master --features profile
    // On shutdown, writes flamegraph.svg into the log directory. pprof samples
    // from inside the process via SIGPROF, so no kernel perf_event privileges
    // are needed.
    #[cfg(feature = "profile")]
    let profiler_guard = {
        info!(log, "profiler enabled at 99 Hz");
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(99)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .expect("failed to start pprof profiler"),
        )
    };

    info!(log, "starting", config = args.config.display(), log_dir = args.log_dir.display());

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
            .unwrap_or(false)
        || matches!(config.ingress_mode.as_str(), "inprocess" | "inproc");
    let inproc_edges = inproc_ingress
        || std::env::var("EPICO_INPROC_EDGES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        || matches!(config.edge_impl.as_str(), "mpmc" | "spsc");
    let mut input_edges:  HashMap<String, EdgeInSrc>  = HashMap::new();
    // Per stage: one handle per out-edge, index-aligned with `config.succs()`
    // (and therefore with the stage's `outputs:` endpoints).
    let mut output_edges: HashMap<String, Vec<EdgeOutSrc>> = HashMap::new();
    let mut skip_dispatchers: HashSet<String> = HashSet::new();
    let mut ingress_source_edge: Option<Edge> = None;
    let mut egress_sink_edge:    Option<Edge> = None;
    // Env var overrides the config value for ad-hoc experiments; config drives
    // the default so the YAML is the single source of truth for reproducible runs.
    let edge_cap = std::env::var("EPICO_EDGE_CAP")
        .ok().and_then(|v| v.parse::<usize>().ok()).filter(|&c| c > 0)
        .unwrap_or(config.edge_cap);
    // Transport: env var overrides config; config drives the default.
    // "" / "zmq" / anything unknown → MPMC (crossbeam ring) for backward compat.
    let edge_impl = std::env::var("EPICO_EDGE_IMPL")
        .unwrap_or_else(|_| config.edge_impl.clone());
    let spsc_ring_cap = std::env::var("EPICO_SPSC_RING_CAP")
        .ok().and_then(|v| v.parse::<usize>().ok()).filter(|&c| c > 0)
        .unwrap_or(config.spsc_ring_cap);
    if inproc_edges {
        // One input queue per CONSUMER stage, shared by every upstream stage
        // that feeds it (M1 design D1) — that is what makes fan-in native:
        // the mpmc ring is already multi-producer, and the spsc mesh gives
        // each upstream stage a disjoint range of producer columns.
        //
        // Producers append their handle to `output_edges[producer]`, so a stage
        // feeding K consumers ends up with K handles in `succs()` order —
        // that is fan-out.
        for stage in config.pipeline.iter() {
            let preds = config.preds(&stage.name);
            if preds.is_empty() { continue; }
            let pred_stages: Vec<&crate::config::PipelineStage> = preds.iter()
                .filter_map(|n| config.pipeline.iter().find(|s| &s.name == n))
                .collect();
            if pred_stages.is_empty() { continue; }

            if edge_impl == "spsc" {
                // Global producer index space: upstream u owns
                // [base(u), base(u) + width(u)).
                //
                // M2 (D1): `width` is NOT simply `u.max_replicas`. A declared
                // fusible pair `(x, u)` means a fused stage may one day produce
                // on this very edge with up to `max_x + max_u` replicas, and
                // the mesh is dimension-fixed once built. Reserving the wider
                // range here — a startup sizing decision — is what lets fusion
                // avoid a runtime mesh swap entirely. Symmetrically,
                // `max_consumers` widens the consumer dimension for a pair
                // `(stage, y)`, whose fused stage would consume on this edge.
                //
                // Cost is `(max_ab - max) × n_prod × ring_cap` extra slots on
                // the affected edges, bounded and paid once.
                let n_prod: usize = pred_stages.iter()
                    .map(|p| config.max_producers(&p.name)).sum();
                let n_cons = config.max_consumers(&stage.name);
                let mesh = Arc::new(SpscMesh::new(
                    n_prod.max(1), n_cons.max(1), spsc_ring_cap));
                let mut base = 0usize;
                for p in &pred_stages {
                    let width = config.max_producers(&p.name);
                    output_edges.entry(p.name.clone()).or_default()
                        .push(EdgeOutSrc::Mesh { mesh: mesh.clone(), base });
                    info!(log, "in-process edge (spsc)",
                          from = p.name, to = stage.name, base = base,
                          producer_width = width, producers = n_prod,
                          consumers = n_cons, cap = spsc_ring_cap);
                    base += width;
                }
                input_edges.insert(stage.name.clone(), EdgeInSrc::Mesh(mesh));
            } else {
                let edge = Edge::new(edge_cap);
                for p in &pred_stages {
                    output_edges.entry(p.name.clone()).or_default()
                        .push(EdgeOutSrc::Ring(edge.clone()));
                    info!(log, "in-process edge (mpmc)",
                          from = p.name, to = stage.name, cap = edge_cap);
                }
                input_edges.insert(stage.name.clone(), EdgeInSrc::Ring(edge));
            }
            let bare = stage.name.strip_prefix("fn-").unwrap_or(&stage.name);
            skip_dispatchers.insert(format!("dispatch-{}", bare));
        }
    }
    if inproc_ingress {
        // Source → entry stage: replace the ingress dispatcher with a single
        // PULL pump feeding the entry stage's Edge. Skip that dispatcher.
        // M1 supports exactly one source stage (in-degree 0).
        let source_name = config.sources().first().map(|s| s.name.clone());
        if let Some(first) = source_name {
            let edge = Edge::new(edge_cap);
            input_edges.insert(first.clone(), EdgeInSrc::Ring(edge.clone()));
            ingress_source_edge = Some(edge);
            let bare = first.strip_prefix("fn-").unwrap_or(&first);
            skip_dispatchers.insert(format!("dispatch-{}", bare));
            info!(log, "in-process ingress (source pump)", to = first, cap = edge_cap);
        }
        // Sink stages → collector: the collector drains their Edge in process
        // instead of binding a PULL socket. No egress socket on a host. Every
        // sink shares ONE egress edge (the mpmc ring is multi-producer), which
        // is what lets a DAG have several terminal stages.
        let sink_names: Vec<String> = config.sinks().iter().map(|s| s.name.clone()).collect();
        if !sink_names.is_empty() {
            let edge = Edge::new(edge_cap);
            for sink in &sink_names {
                output_edges.entry(sink.clone()).or_default()
                    .push(EdgeOutSrc::Ring(edge.clone()));
                info!(log, "in-process egress (sink drain)", from = sink, cap = edge_cap);
            }
            egress_sink_edge = Some(edge);
        }
    }

    supervisor::install_shutdown_handler();

    if args.launch_dispatchers {
        let bin = supervisor::resolve_dispatcher_binary(args.dispatcher_bin.as_deref())
            .unwrap_or_else(|e| {
                error!(log, "dispatcher binary not found", err = e);
                std::process::exit(1);
            });
        let dispatchers_to_spawn: Vec<_> = config.dispatchers.iter()
            .filter(|d| !skip_dispatchers.contains(&d.name))
            .cloned()
            .collect();
        supervisor::spawn_dispatchers(&dispatchers_to_spawn, &bin, &log);
    }

    // ── Instance-pool sizing, with fusion headroom ────────────────────────────
    // A FUSED replica instantiates BOTH halves' components, so a pool sized for
    // the unfused pipeline would fail fused spawns at exactly the moment the
    // system is under pressure. Reserve `2 × max_ab` slots per declared fusible
    // pair up front. The reservation is generous (protocol #1 fully retires
    // both halves before the fused pool starts, so the two never coexist), but
    // it is bounded, paid once at boot, and turns a runtime failure into a
    // startup number the operator can see.
    let fusible = config.fusible_pairs();
    for entry in config.fusible.iter().filter(|p| p.len() != 2) {
        warn!(log, "ignoring malformed `fusible:` entry (expected [a, b])",
              entry = format!("{entry:?}"));
    }
    for (a, b) in &fusible {
        match config.fusion_illegal_reason(a, b) {
            None => info!(log, "fusible pair declared", a = a, b = b,
                          max_ab = config.fused_max_replicas(a, b).unwrap_or(0)),
            // Refused by NAME at validation time, not discovered during a run.
            Some(reason) => warn!(log, "declared fusible pair can never be fused",
                                  a = a, b = b, reason = reason.as_str()),
        }
    }
    let base_max: usize = config.pipeline.iter().map(|s| s.slo.max_replicas).sum();
    let fusion_headroom: usize = fusible.iter()
        .filter_map(|(a, b)| config.fused_max_replicas(a, b))
        .map(|max_ab| 2 * max_ab)
        .sum();
    let total_max = base_max + fusion_headroom;
    let engine = host::build_engine(total_max);
    info!(log, "engine ready",
          max_replicas_total = total_max,
          stage_replicas = base_max,
          fusion_headroom = fusion_headroom);

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
    if let Some(first_stage) = config.sources().first().copied().or_else(|| config.pipeline.first()) {
        if let Some(wasm_path) = first_stage.wasm.as_ref() {
            let t_warm = std::time::Instant::now();
            match wasmtime::component::Component::from_file(&engine, wasm_path) {
                Ok(_throwaway) => {
                    info!(log, "cranelift stage warmup complete",
                          stage = first_stage.name,
                          wasm = wasm_path,
                          warmup_ms = format!(
                            "{:.2}",
                            t_warm.elapsed().as_secs_f64() * 1000.0
                        ));
                }
                Err(e) => {
                    warn!(log, "cranelift stage warmup failed (continuing)",
                          stage = first_stage.name,
                          err = e);
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

    // Terminal sink URI. `normalize_topology` already backfilled this from the
    // last stage's legacy `output` for pre-DAG configs, so this is the same
    // address as before for linear pipelines.
    let collector_uri = config.collector.clone()
        .unwrap_or_else(|| "tcp://0.0.0.0:9999".to_string());

    let col_telemetry = telemetry.clone();
    let col_running   = Arc::new(AtomicBool::new(true));
    let col_running2  = col_running.clone();
    let col_log       = log.with_component("master/collector");
    let col_egress    = egress_sink_edge;

    let collector_handle = std::thread::spawn(move || {
        run_collector(&collector_uri, col_telemetry, col_running2, col_log, test_start, col_egress, custom_sink);
    });

    // ── Source (in-process ingress) ───────────────────────────────────────────
    // EPICO_SOURCE_GEN=1: the source generates events in-process (no socket, no
    // loadgen) for a pure pipeline-ceiling measurement. Otherwise it owns a PULL
    // where an external producer pushes. Either way it feeds the first Edge with
    // no credits — the Edge capacity is the flow control.
    if let Some(edge) = ingress_source_edge {
        let src_log = log.with_component("master/source");
        // Fan the source out across K threads on the (MPMC) ingress edge. One
        // thread by default. Precedence: EPICO_SOURCE_THREADS env override >
        // `source_threads:` from runtime.yaml (deploy field, roadmap item 1) > 1.
        let threads = std::env::var("EPICO_SOURCE_THREADS")
            .ok().and_then(|v| v.parse().ok())
            .or(config.source_threads)
            .unwrap_or(1usize).max(1);
        // Native/in-process source emits binary when the pipeline asks for it
        // (`source_format: binary`, roadmap item 2). EOS stays JSON regardless.
        // EPICO_SOURCE_FORMAT env overrides for a same-binary A/B.
        let source_binary = std::env::var("EPICO_SOURCE_FORMAT")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "binary" || v == "epico-binary"
            })
            .unwrap_or_else(|| {
                matches!(config.source_format.as_str(), "binary" | "epico-binary")
            });
        // Native sources (`run_source_native`, compiled from a user `source.rs`)
        // read the format from the EPICO_SOURCE_FORMAT env, since the host can't
        // change bytes the EventSource produces. Bridge the YAML knob onto the
        // env (before spawning the source thread) so `source_format: binary`
        // works for native sources without an explicit env var.
        if source_binary {
            std::env::set_var("EPICO_SOURCE_FORMAT", "binary");
        }
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
                run_source_gen(edge, count, sensors, threads, source_binary, src_log);
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
        warn!(log, "custom source ignored: no in-process ingress edge");
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
        telemetry::resources::spawn(
            telemetry.clone(),
            col_running.clone(),
            test_start_instant,
            config.resource_sample_interval_ms,
            log.with_component("master/resources"),
        );
    } else {
        info!(log, "resource sampler disabled (interval=0)");
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

    // ── Stage registry (M2: the stage set is mutable at runtime) ──────────────
    // Every stage's wiring is captured as a reusable deployment template rather
    // than being consumed by a one-shot spawn, so a stage can be retired and
    // brought back — which is what a split does when it restores the halves a
    // fusion contracted.
    let mut templates: HashMap<String, morph::StageDeployment> = HashMap::new();
    for stage in config.pipeline.iter() {
        let bare = stage.name.strip_prefix("fn-").unwrap_or(&stage.name);
        let dispatch_name = format!("dispatch-{}", bare);
        let dispatcher = config.dispatchers.iter()
            .find(|d| d.name == dispatch_name)
            .unwrap_or_else(|| {
                error!(log, "no dispatcher for stage", stage = stage.name);
                std::process::exit(1);
            });

        templates.insert(stage.name.clone(), morph::StageDeployment {
            stage:         stage.clone(),
            in_edge:       input_edges.get(&stage.name).cloned().unwrap_or(EdgeInSrc::None),
            out_edges:     output_edges.get(&stage.name).cloned().unwrap_or_default(),
            ctrl_port:     dispatcher.ctrl_port,
            credit_window: dispatcher.credit_window,
            // The EOS barrier's `expected_in`: a fan-in stage only finishes
            // once EVERY upstream branch has delivered its marker. The entry
            // stage has in-degree 0 in the DAG but receives one marker from the
            // source, hence the max(1) applied when the barrier is built.
            expected_in:   config.in_degree(&stage.name).max(1),
        });
    }

    // Summary stage list. It must ALSO name the stages a scheduled morph can
    // bring into existence, or their scaling blocks would be missing from the
    // very summary the break-even analysis reads.
    let mut stage_names: Vec<String> = config.pipeline.iter().map(|s| s.name.clone()).collect();
    for (a, b) in &fusible {
        stage_names.push(crate::config::fused_stage_name(a, b));
    }

    let deployer = Arc::new(morph::Deployer::new(
        engine.clone(),
        telemetry.clone(),
        test_start_instant,
        config.compile_mode.clone(),
        config.event_format.clone(),
        log.clone(),
        config.clone(),
        templates.clone(),
    ));

    for stage in config.pipeline.iter() {
        let dep = templates.get(&stage.name).cloned().expect("template per stage");
        // 0: no warm-up floor at boot — `min_replicas` and the queue-depth
        // signal drive the initial ramp exactly as they always have.
        deployer.spawn_stage(dep, 0);
    }

    // ── Morph channel ─────────────────────────────────────────────────────────
    // Mechanism and policy are separated here: everything downstream of this
    // channel is the actuator, everything upstream is a policy that decides
    // WHEN. Today the only producer is the deterministic YAML schedule.
    let (morph_tx, morph_rx) = std::sync::mpsc::channel::<morph::MorphRequest>();
    let morph_in_flight = deployer.morph_in_flight();
    {
        let dep_c = deployer.clone();
        std::thread::spawn(move || morph::run_actuator(morph_rx, dep_c));
    }
    if !config.morphs.is_empty() {
        let specs = config.morphs.clone();
        let sched_log = log.with_component("morph/schedule");
        std::thread::spawn(move || {
            morph::run_schedule(specs, morph_tx, test_start_instant, sched_log);
        });
    }

    info!(log, "running", stages = deployer.live_stage_names().join(","));

    // Signal readiness to any orchestrator (e.g. the `epico` CLI when it
    // is also launching loadgen). Written *after* autoscalers are live and
    // dispatchers have been spawned, so a waiting client is guaranteed that
    // the first dispatcher will accept connections immediately.
    if let Some(ref rf) = args.ready_file {
        match std::fs::File::create(rf) {
            Ok(_)  => info!(log, "ready file written", path = rf.display()),
            Err(e) => error!(log, "ready file write failed", path = rf.display(), err = e),
        }
    }

    // ── Supervisor loop ───────────────────────────────────────────────────────
    // "Every stage's thread has exited" used to mean the agent had crashed. It
    // still does — EXCEPT in the middle of a morph, where a stage is legitimately
    // retired before its replacement is deployed. Consulting `morph_in_flight`
    // is what keeps a transition from being mistaken for a crash.
    while !supervisor::SHUTDOWN.load(Ordering::Relaxed) {
        if !morph_in_flight.load(Ordering::Relaxed) && deployer.all_stages_finished() {
            error!(log, "all autoscaler threads exited unexpectedly");
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────
    info!(log, "shutting down");
    col_running.store(false, Ordering::Relaxed);

    // Give collector a moment to drain any last events
    std::thread::sleep(Duration::from_millis(500));

    supervisor::kill_children(&log);

    // The collector accumulates its stats thread-locally and merges them into
    // the shared telemetry once, on exit — the summary must not be built
    // before that merge. Join is bounded: with col_running now false the
    // collector exits within one recv timeout (200 ms).
    if collector_handle.join().is_err() {
        error!(log, "collector thread panicked; summary will be incomplete");
    }

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
        error!(log, "failed to write summary", err = e);
    }

    // ── Write flamegraph if profiler was enabled ──────────────────────────────
    #[cfg(feature = "profile")]
    if let Some(guard) = profiler_guard {
        match guard.report().build() {
            Ok(report) => {
                let path = log.run_dir.join("flamegraph.svg");
                match std::fs::File::create(&path) {
                    Ok(file) => {
                        if let Err(e) = report.flamegraph(file) {
                            error!(log, "flamegraph write failed", err = e);
                        } else {
                            info!(log, "flamegraph written", path = path.display());
                        }
                    }
                    Err(e) => error!(log, "flamegraph file create failed", err = e),
                }
            }
            Err(e) => error!(log, "pprof report build failed", err = e),
        }
    }
}

// ── Collector thread ──────────────────────────────────────────────────────────

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
    info!(log, "source driver started (native, in-process)",
          threads = k,
          seconds = deadline.map(|_| std::env::var("EPICO_SOURCE_SECONDS")
            .unwrap_or_default()).unwrap_or_else(|| "∞".into()));

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
    info!(log, "source driver done", count = n, threads = k);
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
fn run_source_gen(out_edge: Edge, count: u64, sensors: usize, threads: usize, binary: bool, log: Logger) {
    let k = threads.max(1);
    let deadline = source_deadline();
    info!(log, "source generating (in-process, no socket)",
          count = count,
          sensors = sensors.max(1),
          threads = k,
          format = if binary { "binary" } else { "json" },
          seconds = deadline.map(|_| std::env::var("EPICO_SOURCE_SECONDS")
            .unwrap_or_default()).unwrap_or_else(|| "∞".into()));

    let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::with_capacity(k);
    for i in 0..k {
        let edge  = out_edge.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || {
            let n = gen_partition(&edge, count, sensors, i, k, binary, deadline);
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
    info!(log, "source done (generated)", count = sent, threads = k);
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
                 binary: bool, deadline: Option<std::time::Instant>) -> u64 {
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
        let value      = (value * 10_000.0).round() / 10_000.0;
        let is_anomaly = seq % 500 == 0;
        let now_wall   = wall_now();

        // Same fields and values either way — only the encoding differs, so a
        // binary-vs-JSON ingest run is a clean A/B (roadmap item 2).
        let bytes = if binary {
            epico_wire::EventBuilder::new()
                .ts_wall(now_wall)
                .ts(now_wall)
                .seq(seq)
                .key_hash(epico_wire::fnv1a64(id.as_bytes()))
                .str_field("sensor_id", id.as_str())
                .str_field("sensor_type", *type_name)
                .str_field("location", *location)
                .str_field("unit", *unit)
                .f64_field("value", value)
                .bool_field("is_anomaly", is_anomaly)
                .finish()
        } else {
            serde_json::to_vec(&serde_json::json!({
                "bench_ts":      now_wall,
                "bench_ts_wall": now_wall,
                "bench_seq":     seq,
                "sensor_id":     id.as_str(),
                "sensor_type":   *type_name,
                "location":      *location,
                "unit":          *unit,
                "value":         value,
                "is_anomaly":    is_anomaly,
            })).unwrap_or_default()
        };

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
        Err(e) => { error!(log, "source socket failed", err = e); return; }
    };
    // Short timeout so the loop can observe shutdown; large recv HWM so the
    // socket can buffer ahead of the Edge without stalling the producer early.
    pull.set_rcvtimeo(100).ok();
    pull.set_rcvhwm(100_000).ok();
    if let Err(e) = pull.bind(&ingress_uri) {
        error!(log, "source bind failed", addr = ingress_uri, err = e);
        return;
    }
    info!(log, "source bound (in-process ingress)", addr = ingress_uri);

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
    info!(log, "source stopped");
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
        info!(log, "collector draining in-process egress edge (no socket)");
        (None, None)
    } else {
        let ctx  = zmq::Context::new();
        let pull = match ctx.socket(zmq::PULL) {
            Ok(s) => s,
            Err(e) => { error!(log, "collector socket failed", err = e); return; }
        };
        pull.set_rcvtimeo(200).ok();
        pull.set_rcvhwm(100_000).ok();

        if let Err(e) = pull.bind(&bind_addr) {
            error!(log, "collector bind failed", addr = bind_addr, err = e);
            return;
        }

        info!(log, "collector bound", addr = bind_addr);

        // Also bind a PUB socket on collector_port + 1 so external consumers
        // (dashboards, recorders, anything wanting a copy of finished events)
        // can SUB to it without competing with the master for the PULL socket.
        let pub_addr = derive_pub_addr(&bind_addr);
        let pub_socket = match ctx.socket(zmq::PUB) {
            Ok(s) => s,
            Err(e) => {
                error!(log, "collector pub socket failed", err = e);
                return;
            }
        };
        // Don't block the collector if no subscriber is keeping up. Drop
        // events instead — telemetry is the primary purpose; the tee is a
        // best-effort copy.
        pub_socket.set_sndhwm(1000).ok();
        pub_socket.set_sndtimeo(0).ok();
        if let Err(e) = pub_socket.bind(&pub_addr) {
            warn!(log, "collector pub bind failed (continuing without tee)",
                  addr = pub_addr,
                  err = e);
        } else {
            info!(log, "collector pub bound", addr = pub_addr);
        }
        (Some(pull), Some(pub_socket))
    };

    let mut recv_count: u64 = 0;
    let mut eos_received = false;

    // Collector-owned stats: accumulated lock-free per event, merged into the
    // shared telemetry once at loop exit (see telemetry::collector docs).
    let mut stats = CollectorStats::new();

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
                    info!(log, "EOS received at collector",
                          recv_count = recv_count,
                          loadgen_sent = loadgen_sent,
                          expected = expected);
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

        // Binary envelopes are sniffed by magic and decoded header-only into
        // the same JSON shape the accounting below already consumes —
        // strictly cheaper than the JSON parse it replaces (domain fields
        // are skipped; the collector never reads them). EOS markers stay
        // JSON end to end and were already handled above.
        let ev_parsed: Option<serde_json::Value> =
            if crate::envelope::is_binary(bytes.as_ref()) {
                crate::envelope::binary_to_telemetry_json(bytes.as_ref())
            } else {
                serde_json::from_slice::<serde_json::Value>(&bytes).ok()
            };
        if let Some(ev) = ev_parsed {
            stats.observe(recv_ts, test_start, &ev);
        }

        recv_count += 1;
        //if recv_count % 10_000 == 0 {
        //    log.info("collector progress", &[("received", &recv_count.to_string())]);
        //}
    }

    stats.merge_into(&telemetry, &log);

    info!(log, "collector stopped", total_received = recv_count);
}

// ── Startup helpers ───────────────────────────────────────────────────────────

fn load_config(path: &std::path::Path, log: &Logger) -> Config {
    let yaml_path = path.to_string_lossy().to_string();
    let yaml_content = std::fs::read_to_string(&yaml_path)
        .unwrap_or_else(|e| {
            error!(log, "failed to read config", path = yaml_path, err = e);
            std::process::exit(1);
        });
    let mut config: Config = serde_yaml::from_str(&yaml_content)
        .unwrap_or_else(|e| {
            error!(log, "failed to parse YAML", path = yaml_path, err = e);
            std::process::exit(1);
        });
    let yaml_dir = std::path::Path::new(&yaml_path)
        .parent().unwrap_or_else(|| std::path::Path::new("."));
    let resolve = |wasm: &str| -> Option<String> {
        let resolved = yaml_dir.join(wasm);
        resolved.exists().then(|| resolved.to_string_lossy().to_string())
    };
    for stage in config.pipeline.iter_mut() {
        if let Some(ref wasm) = stage.wasm {
            if let Some(r) = resolve(wasm) { stage.wasm = Some(r); }
        }
        if stage.wasm.is_none() { stage.wasm = Some(default_wasm_path(&stage.name)); }
        // A fused stage's halves carry their own components; resolve them the
        // same way so a YAML-declared fusion works from a relative path too.
        for half in stage.fused_from.iter_mut() {
            match half.wasm.as_deref() {
                Some(w) => { if let Some(r) = resolve(w) { half.wasm = Some(r); } }
                None    => {
                    let d = default_wasm_path(&half.name);
                    half.wasm = Some(resolve(&d).unwrap_or(d));
                }
            }
        }
    }
    // Backfill derived topology (edges / outputs / collector) so everything
    // downstream can read `config.edges` unconditionally — including
    // hand-written and pre-DAG runtime.yaml files.
    config.normalize_topology();
    config
}

fn validate_pipeline(config: &Config, log: &Logger) {
    // Every component a worker will instantiate has to be checked, which for a
    // fused stage is one per half. The stage itself is registered under its own
    // name (so the edge checks below resolve); the remaining halves are
    // registered under a qualified name so they are validated without being
    // mistaken for edge endpoints.
    let mut stages: Vec<(String, String)> = Vec::new();
    for s in config.pipeline.iter() {
        let halves = s.halves();
        for (i, (logical, wasm)) in halves.iter().enumerate() {
            let key = if i == 0 { s.name.clone() } else { format!("{}::{}", s.name, logical) };
            stages.push((key, wasm.clone()));
        }
    }
    let edges: Vec<(String, String)> = config.edges.iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();
    match pipeline_validator::validate_pipeline_components(&stages, &edges) {
        Ok(report) => {
            info!(log, "pipeline validated", stages = config.pipeline.len());
            for line in &report { info!(log, "validator", detail = line); }
        }
        Err(e) => {
            error!(log, "pipeline validation failed", err = format!("{:#}", e));
            std::process::exit(1);
        }
    }
}

fn apply_placement_filter(config: &mut Config, log: &Logger) {
    let total = config.pipeline.len();
    config.pipeline.retain(|s| stage_owned_by(s, &config.this_host));
    config.dispatchers.retain(|d| d.placement == config.this_host);
    // Drop edges whose endpoints aren't both owned here, so the topology
    // helpers describe the subgraph this agent actually runs. Single-host
    // today (nothing is dropped); cross-host edges arrive with M5.
    let owned: HashSet<String> = config.pipeline.iter().map(|s| s.name.clone()).collect();
    config.edges.retain(|e| owned.contains(&e.from) && owned.contains(&e.to));
    let kept = config.pipeline.len();
    info!(log, "placement filter",
          this_host = config.this_host,
          owning = format!("{}/{}", kept, total));
    if kept == 0 {
        error!(log, "no stages placed on this host", this_host = config.this_host);
        std::process::exit(1);
    }
}
// Appended placeholder to force view refresh