//! Per-stage autoscaler — one thread per stage, ticks every 20ms.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wasmtime::component::{Component, Linker as ComponentLinker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use epico_logger::Logger;

use crate::config::{make_pull_endpoint, make_push_endpoint, PipelineStage};
use crate::host::HostState;
use crate::spsc::{EdgeInSrc, EdgeOutSrc};
use crate::worker::{spawn_worker, WorkerHandle};
use crate::{RunTelemetry, ScalingEvent};

const TICK_MS: u64 = 1;
const SPAWN_SETTLE_TICKS: u32 = 3;

fn record_event(
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

/// Load a stage's Wasm component, preferring an AOT .cwasm artifact next
/// to the .wasm if it exists. Returns the loaded component, the wall-clock
/// time spent loading, and the mode label ("aot" or "jit") for telemetry.
fn load_component(
    engine: &Engine,
    wasm_path: &str,
    log: &Logger,
) -> (Component, f64, &'static str) {
    let cwasm_path = Path::new(wasm_path).with_extension("cwasm");
    let t_load = Instant::now();

    if cwasm_path.exists() {
        // SAFETY: deserialize_file requires the file to be a Wasmtime
        // artifact produced by the same major.minor wasmtime version
        // that the agent is linked against. The CLI's `aot_compile_stages`
        // produces it via the bundled `wasmtime` CLI; if versions mismatch
        // wasmtime returns an error here rather than miscompiling.
        match unsafe { Component::deserialize_file(engine, &cwasm_path) } {
            Ok(c) => {
                let load_ms = t_load.elapsed().as_secs_f64() * 1000.0;
                log.info("AOT component loaded", &[
                    ("path",        &cwasm_path.display().to_string()),
                    ("load_ms",     &format!("{:.3}", load_ms)),
                ]);
                return (c, load_ms, "aot");
            }
            Err(e) => {
                log.warn("AOT deserialize failed; falling back to JIT", &[
                    ("path", &cwasm_path.display().to_string()),
                    ("err",  &e.to_string()),
                ]);
            }
        }
    }

    let t_jit = Instant::now();
    let component = Component::from_file(engine, wasm_path)
        .unwrap_or_else(|e| {
            log.error("failed to load wasm component", &[("err", &e.to_string())]);
            std::process::exit(1);
        });
    let compile_ms = t_jit.elapsed().as_secs_f64() * 1000.0;
    log.info("JIT component compiled", &[
        ("path",       wasm_path),
        ("compile_ms", &format!("{:.3}", compile_ms)),
    ]);
    (component, compile_ms, "jit")
}

/// JIT-compile `.wasm` → `.cwasm` at cold-start time and create an
/// `InstancePre`. Called only in `compile_mode == "jit"` on the first
/// spawn; subsequent spawns reuse the cached `InstancePre`.
///
/// The `.cwasm` is written next to the `.wasm` so that if the agent
/// is restarted (e.g. for a second run in the same target dir) it can
/// fall back to deserializing the cached artifact instead of recompiling.
fn jit_compile_and_instantiate(
    engine:   &Engine,
    wasm_path: &str,
    linker:   ComponentLinker<HostState>,
    log:      &Logger,
) -> (wasmtime::component::InstancePre<HostState>, f64, f64) {
    let t_jit = Instant::now();
    let wasm_bytes = std::fs::read(wasm_path).unwrap_or_else(|e| {
        log.error("JIT: failed to read wasm", &[("path", wasm_path), ("err", &e.to_string())]);
        std::process::exit(1);
    });
    let cwasm_bytes = engine.precompile_component(&wasm_bytes).unwrap_or_else(|e| {
        log.error("JIT: precompile_component failed", &[("path", wasm_path), ("err", &e.to_string())]);
        std::process::exit(1);
    });
    let cwasm_path = Path::new(wasm_path).with_extension("cwasm");
    std::fs::write(&cwasm_path, &cwasm_bytes).unwrap_or_else(|e| {
        log.error("JIT: cwasm write failed", &[
            ("path", &cwasm_path.display().to_string()), ("err", &e.to_string()),
        ]);
        std::process::exit(1);
    });
    let compile_ms = t_jit.elapsed().as_secs_f64() * 1000.0;
    // SAFETY: we just wrote this artifact from the same engine version.
    let component = unsafe { Component::deserialize_file(engine, &cwasm_path) }
        .unwrap_or_else(|e| {
            log.error("JIT: cwasm deserialize failed", &[("err", &e.to_string())]);
            std::process::exit(1);
        });
    let t_pre = Instant::now();
    let instance_pre = linker
        .instantiate_pre(&component)
        .expect("JIT: failed to create InstancePre");
    let instantiate_pre_ms = t_pre.elapsed().as_secs_f64() * 1000.0;
    log.info("JIT: compiled at cold-start", &[
        ("wasm",                wasm_path),
        ("compile_ms",          &format!("{:.3}", compile_ms)),
        ("instantiate_pre_ms",  &format!("{:.3}", instantiate_pre_ms)),
    ]);
    (instance_pre, compile_ms, instantiate_pre_ms)
}

pub(crate) fn run_autoscaler_loop(
    stage:         PipelineStage,
    ctrl_port:     u16,
    credit_window: u32,
    input_edge:    EdgeInSrc,
    output_edge:   EdgeOutSrc,
    engine:        Engine,
    log:           Logger,
    telemetry:     Arc<Mutex<RunTelemetry>>,
    test_start:    Instant,
    compile_mode:  String,
    event_format:  String,
) {
    let min_rep  = stage.slo.min_replicas;
    let max_rep  = stage.slo.max_replicas;
    let queue_up = stage.slo.queue_up.unwrap_or(50.0);
    let queue_down = stage.slo.queue_down.unwrap_or(0.0);
    let cooldown_up_ticks =
        ((stage.slo.cooldown_up_s.unwrap_or(0.1) * 1000.0) as u64 / TICK_MS).max(1) as u32;
    let cooldown_down_ticks =
        ((stage.slo.cooldown_down_s.unwrap_or(5.0) * 1000.0) as u64 / TICK_MS).max(1) as u32;

    let in_endpoint  = make_pull_endpoint(&stage.input);
    let out_endpoint = make_push_endpoint(&stage.output);

    let wasm_path = stage.wasm.clone().expect("wasm path resolved in main()");
    log.info("loading component", &[
        ("wasm",         &wasm_path),
        ("stage",        &stage.name),
        ("compile_mode", &compile_mode),
    ]);

    // ── Component loading: AOT/startup-JIT at startup; deferred-JIT at cold-start ──
    //
    // `jit_pending_linker`: holds the linker in JIT mode until the first spawn.
    // `shared_instance_pre`: the InstancePre shared by all workers.  In JIT
    //   mode it starts as None and is filled on the first spawn; in AOT/startup
    //   mode it is populated here before the loop.
    let (mut jit_pending_linker, mut shared_instance_pre): (
        Option<ComponentLinker<HostState>>,
        Option<Arc<wasmtime::component::InstancePre<HostState>>>,
    ) = if compile_mode == "jit" {
        // Deferred: build the linker now but don't touch the .wasm yet.
        let mut linker: ComponentLinker<HostState> = ComponentLinker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .expect("Failed to add wasi to component linker");
        if let Err(e) = wasmtime_wasi_http::add_only_http_to_linker_sync(&mut linker) {
            log.warn("wasi:http not wired", &[("err", &e.to_string())]);
        }
        log.info("autoscaler ready (JIT: compilation deferred to cold-start)", &[
            ("stage",   &stage.name),
            ("max_rep", &stage.slo.max_replicas.to_string()),
            ("min_rep", &stage.slo.min_replicas.to_string()),
        ]);
        (Some(linker), None)
    } else {
        // AOT or startup-JIT: compile/load now, before the loop.
        let (component, compile_ms, mode) = load_component(&engine, &wasm_path, &log);

        let mut linker: ComponentLinker<HostState> = ComponentLinker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .expect("Failed to add wasi to component linker");
        if let Err(e) = wasmtime_wasi_http::add_only_http_to_linker_sync(&mut linker) {
            log.warn("wasi:http not wired", &[("err", &e.to_string())]);
        }

        let t_pre = Instant::now();
        let instance_pre = linker
            .instantiate_pre(&component)
            .expect("Failed to create component InstancePre");
        let instantiate_pre_ms = t_pre.elapsed().as_secs_f64() * 1000.0;
        log.info("instance_pre ready", &[
            ("stage",               &stage.name),
            ("mode",                mode),
            ("instantiate_pre_ms",  &format!("{:.3}", instantiate_pre_ms)),
        ]);

        // ── Warmup instantiate ────────────────────────────────────────────────
        // The first `instance_pre.instantiate(&mut store)` in a process is
        // significantly slower than subsequent ones because of one-time
        // Wasmtime engine setup (Cranelift relocation patching, pool
        // first-slot mmap + page-fault, signal handler install, etc.) plus
        // first-call WASI context construction.
        //
        // We do that throwaway instantiation here, at autoscaler startup,
        // before any user-facing cold start. The cost (~0.5 ms on first
        // stage, ~0.05 ms on subsequent stages) is shifted out of the
        // measured cold_start_ms window and into agent boot time.
        //
        // The `Store` is dropped at end of scope, returning its pool slot.
        // The dropped instance has no side effects — no events processed,
        // no sockets touched, no exports called. Pure init/teardown.
        let t_warm = Instant::now();
        {
            let host_state = HostState {
                table: ResourceTable::new(),
                wasi:  WasiCtxBuilder::new().build(),
                http:  WasiHttpCtx::new(),
                limits: crate::host::default_store_limits(),
            };
            let mut warmup_store = Store::new(&engine, host_state);
            warmup_store.limiter(|s| &mut s.limits);
            match instance_pre.instantiate(&mut warmup_store) {
                Ok(_inst) => {
                    let warmup_ms = t_warm.elapsed().as_secs_f64() * 1000.0;
                    log.info("wasmtime warmup complete", &[
                        ("stage",     &stage.name),
                        ("warmup_ms", &format!("{:.3}", warmup_ms)),
                    ]);
                }
                Err(e) => {
                    // Non-fatal: if warmup fails, the real first worker will
                    // surface the same error. Don't abort agent startup over
                    // an instrumentation step.
                    log.warn("wasmtime warmup failed (continuing)", &[
                        ("stage", &stage.name),
                        ("err",   &e.to_string()),
                    ]);
                }
            }
        }

        let init_action = if mode == "aot" { "init_aot" } else { "init_jit" };
        record_event(
            &telemetry, test_start, &stage.name, init_action,
            0, None, Some(compile_ms), Some(instantiate_pre_ms),
        );

        log.info("autoscaler ready", &[
            ("max_rep",         &stage.slo.max_replicas.to_string()),
            ("min_rep",         &stage.slo.min_replicas.to_string()),
            ("queue_up",        &stage.slo.queue_up.unwrap_or(50.0).to_string()),
            ("queue_down",      &stage.slo.queue_down.unwrap_or(0.0).to_string()),
            ("mode",            mode),
        ]);

        (None, Some(Arc::new(instance_pre)))
    };

    let zmq_ctx = zmq::Context::new();
    let ctrl_socket = zmq_ctx.socket(zmq::REQ).expect("ctrl REQ socket");
    ctrl_socket.set_rcvtimeo(10).ok();
    ctrl_socket.set_sndtimeo(10).ok();
    ctrl_socket
        .connect(&format!("tcp://127.0.0.1:{}", ctrl_port))
        .expect("connect ctrl");

    // Pre-warmed context handed off to every worker we spawn from this
    // autoscaler. libzmq spawns the context's I/O thread lazily on the
    // first `ctx.socket()` call, and on a busy agent process the kernel
    // scheduler can take 5-10 ms to actually run that newly-pthread_created
    // I/O thread. Without pre-warming, that cost lands inside the worker's
    // cold-start measurement window and produces the per-pipeline-topology
    // first-stage-tax we chased through ph_push_socket_ms.
    //
    // We create the context here (on the autoscaler thread, well before
    // any cold-start spawn decision is taken) and trigger I/O thread
    // creation by allocating and immediately dropping a throwaway PUSH
    // socket. The cost lands on the autoscaler's startup, where it does
    // not contaminate any measurement. We then clone the warm context
    // into each worker; libzmq contexts are reference-counted (Arc), so
    // sockets allocated by different workers from clones share the same
    // I/O thread but otherwise operate independently. With max_replicas
    // = 4 per stage and modest per-event traffic, one I/O thread per
    // stage is comfortably within libzmq's throughput envelope.
    let worker_ctx = zmq::Context::new();
    {
        let _warm = worker_ctx.socket(zmq::PUSH)
            .expect("warm-up PUSH socket creation failed");
    }

    let last_active_ts = Arc::new(AtomicU64::new(
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
    ));
    let avg_latency_us = Arc::new(AtomicU64::new(100));

    let mut workers: Vec<WorkerHandle> = Vec::new();
    let mut up_votes: u32 = 0;
    let mut down_votes: u32 = 0;
    let mut ticks_since_spawn: u32 = u32::MAX;
    // Index into telemetry.scaling_events of the most-recently recorded
    // cold_start event whose cold_start_ms is still None.  We back-fill it
    // once the worker's first refill message arrives via the dispatcher metrics
    // poll (usually within one 20 ms tick).  None means no pending back-fill.
    let mut pending_cs_event_idx: Option<usize> = None;

    loop {
        std::thread::sleep(Duration::from_millis(TICK_MS));
        workers.retain(|w| !w.done.load(Ordering::Relaxed));
        let current = workers.len();
        ticks_since_spawn = ticks_since_spawn.saturating_add(1);

        // Queue-depth signal. An in-process consumer stage has no dispatcher to
        // poll, so its input Edge's occupancy is the signal — and we must NOT
        // `continue` on a missing dispatcher, or the min-replica spawn below
        // never runs, no worker ever drains the ring, and the pipeline
        // deadlocks behind backpressure.
        let (qd, dispatcher_metrics) = if input_edge.is_some() {
            (input_edge.len() as f64, None)
        } else {
            match fetch_dispatcher_metrics(&ctrl_socket) {
                Some(m) => (m.queue_depth, Some(m)),
                None    => continue,
            }
        };

        if let Some(metrics) = dispatcher_metrics.as_ref() {
        if !metrics.worker_samples.is_empty() {
            if let Ok(mut tel) = telemetry.try_lock() {
                let t_s = test_start.elapsed().as_secs_f64();
                for (total, serde) in &metrics.worker_samples {
                    tel.total_us_samples
                        .entry(stage.name.clone())
                        .or_default()
                        .push(*total);
                    tel.serde_us_samples
                        .entry(stage.name.clone())
                        .or_default()
                        .push(*serde);
                }
                tel.queue_depth_samples
                    .entry(stage.name.clone())
                    .or_default()
                    .push((round3(t_s), qd as u64));

                // Back-fill cold_start_ms into the pending cold_start event
                // once the worker's first refill message has propagated back.
                if let Some(idx) = pending_cs_event_idx {
                    if let Some(cs_ms) = metrics.cold_start_ms_seen.first().copied() {
                        if let Some(ev) = tel.scaling_events.get_mut(idx) {
                            ev.cold_start_ms = Some(cs_ms);
                        }
                        pending_cs_event_idx = None;
                    }
                }
            }
        }
        }

        if current > max_rep {
            log.warn("invariant breach: current > max_rep, draining surplus", &[
                ("current", &current.to_string()),
                ("max_rep", &max_rep.to_string()),
            ]);
            for w in workers.iter().take(current - max_rep) {
                w.drain_flag.store(true, Ordering::Relaxed);
            }
            up_votes = 0;
            continue;
        }

        if qd > queue_up {
            up_votes = up_votes.saturating_add(1);
            down_votes = 0;
        } else if qd <= queue_down {
            down_votes = down_votes.saturating_add(1);
            up_votes = 0;
        } else {
            up_votes   = up_votes.saturating_sub(1);
            down_votes = down_votes.saturating_sub(1);
        }

        if current == 0 && qd > 0.0 && current < max_rep {
            let decision_ts = now_secs_f64();
            // JIT: .wasm→.cwasm compilation happens here, AFTER decision_ts is
            // captured, so the cold-start measurement includes compile time.
            if shared_instance_pre.is_none() {
                let linker = jit_pending_linker.take()
                    .expect("JIT linker already consumed — this is a bug");
                let (ip, cm, ipm) = jit_compile_and_instantiate(&engine, &wasm_path, linker, &log);
                record_event(&telemetry, test_start, &stage.name, "init_jit",
                             0, None, Some(cm), Some(ipm));
                shared_instance_pre = Some(Arc::new(ip));
            }
            let instance_pre = shared_instance_pre.as_ref().unwrap();
            log.info("cold start: spawning replica", &[
                ("qd",      &format!("{:.0}", qd)),
                ("max_rep", &max_rep.to_string()),
            ]);
            let replica_idx = workers.len();
            workers.push(spawn_worker(
                &stage, &in_endpoint, &out_endpoint,
                input_edge.for_replica(replica_idx), output_edge.for_replica(replica_idx),
                credit_window,
                &engine, instance_pre,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                log.with_component(&format!("worker/{}", stage.name)),
            ));
            if let Ok(mut tel) = telemetry.lock() {
                pending_cs_event_idx = Some(tel.scaling_events.len());
                tel.scaling_events.push(ScalingEvent {
                    t_s:                test_start.elapsed().as_secs_f64(),
                    stage:              stage.name.clone(),
                    action:             "cold_start".to_string(),
                    new_count:          workers.len(),
                    cold_start_ms:      None,
                    compile_ms:         None,
                    instantiate_pre_ms: None,
                });
            }
            ticks_since_spawn = 0;
            up_votes = 0;
            continue;
        }

        if current < min_rep {
            let decision_ts = now_secs_f64();
            if shared_instance_pre.is_none() {
                let linker = jit_pending_linker.take()
                    .expect("JIT linker already consumed — this is a bug");
                let (ip, cm, ipm) = jit_compile_and_instantiate(&engine, &wasm_path, linker, &log);
                record_event(&telemetry, test_start, &stage.name, "init_jit",
                             0, None, Some(cm), Some(ipm));
                shared_instance_pre = Some(Arc::new(ip));
            }
            let instance_pre = shared_instance_pre.as_ref().unwrap();
            log.info("below min: spawning replica", &[
                ("current", &current.to_string()),
                ("min_rep", &min_rep.to_string()),
            ]);
            let replica_idx = workers.len();
            workers.push(spawn_worker(
                &stage, &in_endpoint, &out_endpoint,
                input_edge.for_replica(replica_idx), output_edge.for_replica(replica_idx),
                credit_window,
                &engine, instance_pre,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                log.with_component(&format!("worker/{}", stage.name)),
            ));
            record_event(&telemetry, test_start, &stage.name, "spawn",
                         workers.len(), None, None, None);
            ticks_since_spawn = 0;
            continue;
        }

        if up_votes >= cooldown_up_ticks
            && ticks_since_spawn >= SPAWN_SETTLE_TICKS
            && current < max_rep
        {
            let decision_ts = now_secs_f64();
            if shared_instance_pre.is_none() {
                let linker = jit_pending_linker.take()
                    .expect("JIT linker already consumed — this is a bug");
                let (ip, cm, ipm) = jit_compile_and_instantiate(&engine, &wasm_path, linker, &log);
                record_event(&telemetry, test_start, &stage.name, "init_jit",
                             0, None, Some(cm), Some(ipm));
                shared_instance_pre = Some(Arc::new(ip));
            }
            let instance_pre = shared_instance_pre.as_ref().unwrap();
            log.info("scale up", &[
                ("qd",      &format!("{:.0}", qd)),
                ("current", &current.to_string()),
                ("new",     &(current + 1).to_string()),
                ("max_rep", &max_rep.to_string()),
            ]);
            let replica_idx = workers.len();
            workers.push(spawn_worker(
                &stage, &in_endpoint, &out_endpoint,
                input_edge.for_replica(replica_idx), output_edge.for_replica(replica_idx),
                credit_window,
                &engine, instance_pre,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                log.with_component(&format!("worker/{}", stage.name)),
            ));
            record_event(&telemetry, test_start, &stage.name, "spawn",
                         workers.len(), None, None, None);
            ticks_since_spawn = 0;
            up_votes = 0;
            continue;
        }

        if down_votes >= cooldown_down_ticks && current > min_rep {
            let mut drained = false;
            for w in workers.iter() {
                if !w.drain_flag.load(Ordering::Relaxed) {
                    w.drain_flag.store(true, Ordering::Relaxed);
                    drained = true;
                    break;
                }
            }
            if drained {
                log.info("scale down", &[
                    ("qd",      &format!("{:.0}", qd)),
                    ("current", &current.to_string()),
                    ("min_rep", &min_rep.to_string()),
                ]);
                record_event(&telemetry, test_start, &stage.name, "drain",
                             current.saturating_sub(1), None, None, None);
            }
            down_votes = 0;
        }

        if qd > queue_up && current >= max_rep {
            if up_votes == cooldown_up_ticks.saturating_sub(1) || up_votes == 0 {
                log.warn("SLO breach: queue depth at max replicas", &[
                    ("qd",      &format!("{:.0}", qd)),
                    ("max_rep", &max_rep.to_string()),
                ]);
            }
        }
    }
}

struct DispatcherMetrics {
    queue_depth: f64,
    worker_samples: Vec<(u64, u64)>,
    /// cold_start_ms values reported by workers that booted since the last poll.
    /// The dispatcher forwards the first refill payload from each worker, which
    /// carries the wall-clock cold_start_ms measured by the worker itself.
    cold_start_ms_seen: Vec<f64>,
}

fn fetch_dispatcher_metrics(ctrl: &zmq::Socket) -> Option<DispatcherMetrics> {
    if ctrl.send("get_metrics", 0).is_err() {
        let _ = ctrl.recv_bytes(zmq::DONTWAIT);
        return None;
    }
    let reply = match ctrl.recv_bytes(0) {
        Ok(r) => r,
        Err(_) => {
            let _ = ctrl.recv_bytes(zmq::DONTWAIT);
            return None;
        }
    };
    let json = match serde_json::from_slice::<serde_json::Value>(&reply) {
        Ok(v) => v,
        Err(_) => return None,
    };

    let queue_depth = json.get("dispatcher_queue_depth")
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0);
    if queue_depth < 0.0 { return None; }

    let mut worker_samples: Vec<(u64, u64)> = Vec::new();
    let mut cold_start_ms_seen: Vec<f64> = Vec::new();
    if let Some(consumers) = json.get("consumers").and_then(|v| v.as_array()) {
        for c in consumers {
            // Worker now reports nanoseconds (`total_ns`/`serde_ns`) so
            // sub-µs serde survives. Fall back to the legacy µs keys
            // (scaled to ns) if an older worker is in the mix. Stored
            // throughout as nanoseconds; converted to µs at summary time.
            let total = c.get("total_ns").and_then(|v| v.as_u64())
                .or_else(|| c.get("total_us").and_then(|v| v.as_u64()).map(|us| us * 1000))
                .unwrap_or(0);
            let serde = c.get("serde_ns").and_then(|v| v.as_u64())
                .or_else(|| c.get("serde_us").and_then(|v| v.as_u64()).map(|us| us * 1000))
                .unwrap_or(0);
            if total > 0 {
                worker_samples.push((total, serde));
            }
            if let Some(cs) = c.get("cold_start_ms").and_then(|v| v.as_f64()) {
                if cs > 0.0 {
                    cold_start_ms_seen.push(cs);
                }
            }
        }
    }

    Some(DispatcherMetrics { queue_depth, worker_samples, cold_start_ms_seen })
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn round3(x: f64) -> f64 { (x * 1000.0).round() / 1000.0 }