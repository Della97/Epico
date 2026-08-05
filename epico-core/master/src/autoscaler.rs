//! Per-stage autoscaler — one thread per stage, ticks every `TICK_MS` (1 ms).

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wasmtime::component::{Component, Linker as ComponentLinker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use epico_logger::Logger;
use epico_logger::{error, info, warn};

use crate::config::{make_pull_endpoint, make_push_endpoint, PipelineStage};
use crate::eos::StageEosBarrier;
use crate::host::HostState;
use crate::spsc::{EdgeInSrc, EdgeOutSrc};
use crate::telemetry::{record_event, stats::round3, RunTelemetry, ScalingEvent};
use crate::worker::{spawn_worker, OutputSpec, WorkerChain, WorkerHandle};

const TICK_MS: u64 = 1;
const SPAWN_SETTLE_TICKS: u32 = 3;

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
                info!(log, "AOT component loaded",
                      path = cwasm_path.display(),
                      load_ms = format!("{:.3}", load_ms));
                return (c, load_ms, "aot");
            }
            Err(e) => {
                warn!(log, "AOT deserialize failed; falling back to JIT",
                      path = cwasm_path.display(),
                      err = e);
            }
        }
    }

    let t_jit = Instant::now();
    let component = Component::from_file(engine, wasm_path)
        .unwrap_or_else(|e| {
            error!(log, "failed to load wasm component", err = e);
            std::process::exit(1);
        });
    let compile_ms = t_jit.elapsed().as_secs_f64() * 1000.0;
    info!(log, "JIT component compiled",
          path = wasm_path,
          compile_ms = format!("{:.3}", compile_ms));
    (component, compile_ms, "jit")
}

/// A component linker with WASI (and wasi:http when available) wired in.
/// One per component: a fused stage builds several, one per half, so each
/// half's imports resolve exactly as they did before fusion.
fn new_linker(engine: &Engine, log: &Logger) -> ComponentLinker<HostState> {
    let mut linker: ComponentLinker<HostState> = ComponentLinker::new(engine);
    wasmtime_wasi::add_to_linker_sync(&mut linker)
        .expect("Failed to add wasi to component linker");
    if let Err(e) = wasmtime_wasi_http::add_only_http_to_linker_sync(&mut linker) {
        warn!(log, "wasi:http not wired", err = e);
    }
    linker
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
        error!(log, "JIT: failed to read wasm", path = wasm_path, err = e);
        std::process::exit(1);
    });
    let cwasm_bytes = engine.precompile_component(&wasm_bytes).unwrap_or_else(|e| {
        error!(log, "JIT: precompile_component failed", path = wasm_path, err = e);
        std::process::exit(1);
    });
    let cwasm_path = Path::new(wasm_path).with_extension("cwasm");
    std::fs::write(&cwasm_path, &cwasm_bytes).unwrap_or_else(|e| {
        error!(log, "JIT: cwasm write failed", path = cwasm_path.display(), err = e);
        std::process::exit(1);
    });
    let compile_ms = t_jit.elapsed().as_secs_f64() * 1000.0;
    // SAFETY: we just wrote this artifact from the same engine version.
    let component = unsafe { Component::deserialize_file(engine, &cwasm_path) }
        .unwrap_or_else(|e| {
            error!(log, "JIT: cwasm deserialize failed", err = e);
            std::process::exit(1);
        });
    let t_pre = Instant::now();
    let instance_pre = linker
        .instantiate_pre(&component)
        .expect("JIT: failed to create InstancePre");
    let instantiate_pre_ms = t_pre.elapsed().as_secs_f64() * 1000.0;
    info!(log, "JIT: compiled at cold-start",
          wasm = wasm_path,
          compile_ms = format!("{:.3}", compile_ms),
          instantiate_pre_ms = format!("{:.3}", instantiate_pre_ms));
    (instance_pre, compile_ms, instantiate_pre_ms)
}

/// One stage's control loop.
///
/// M2 adds two parameters that make the stage set MUTABLE at runtime:
///
/// * `stop` — raised by the morph actuator to retire this stage. The loop
///   drains every replica, waits for each to confirm exit, then returns. Drain
///   is the same mechanism scale-down already uses, so the residue path that
///   conserves events under scale-down conserves them across a morph too.
///   Never raised for a pipeline that does not morph.
/// * `initial_replicas` — a replica count to bring up immediately, ABOVE
///   `min_replicas`, before normal scaling rules take over. A freshly fused
///   stage starts at `R_ab = Ra + Rb`, not `max(Ra, Rb)`: replica counts encode
///   service times (Little: Ra ≈ λ·tA) and a fused worker holds its thread for
///   `tA + tB` per event. Undershooting at the moment the SLO was already under
///   pressure is the wrong direction to be wrong in; the autoscaler trims down
///   from here as the deleted edge overhead shows up. 0 for an ordinary boot.
/// * `live_replicas` — published every tick so the morph actuator can read
///   `Ra` and `Rb` without racing this loop's private worker list.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_autoscaler_loop(
    stage:         PipelineStage,
    ctrl_port:     u16,
    credit_window: u32,
    input_edge:    EdgeInSrc,
    output_edges:  Vec<EdgeOutSrc>,
    engine:        Engine,
    log:           Logger,
    telemetry:     Arc<Mutex<RunTelemetry>>,
    test_start:    Instant,
    compile_mode:  String,
    event_format:  String,
    barrier:       Arc<StageEosBarrier>,
    stop:          Arc<AtomicBool>,
    initial_replicas: usize,
    live_replicas: Arc<std::sync::atomic::AtomicUsize>,
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
    // One endpoint per out-edge, index-aligned with `output_edges` and with
    // the stage's successors. An in-proc handle wins when present; otherwise
    // the worker connects a PUSH socket to the endpoint.
    let out_endpoints: Vec<String> = stage.outputs.iter()
        .map(|o| make_push_endpoint(o))
        .collect();
    let n_outputs = out_endpoints.len().max(output_edges.len());

    // Resolve this stage's out-edges for one replica: in-proc queue handle
    // where we have one, zmq endpoint otherwise.
    let build_output_specs = |replica_idx: usize| -> Vec<OutputSpec> {
        (0..n_outputs)
            .map(|i| match output_edges.get(i).and_then(|src| src.for_replica(replica_idx)) {
                Some(edge) => OutputSpec::Queue(edge),
                None => OutputSpec::Zmq(out_endpoints.get(i).cloned().unwrap_or_default()),
            })
            .collect()
    };

    // The logical stages this physical stage runs: one for an ordinary stage,
    // N for a fused one. Everything below is per-half, so fusion adds no
    // special case to the scaling logic — only to how many components a
    // replica instantiates.
    let halves = stage.halves();
    info!(log, "loading components",
          stage = stage.name,
          halves = halves.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>().join(" -> "),
          compile_mode = compile_mode);

    // ── Component loading: AOT/startup-JIT at startup; deferred-JIT at cold-start ──
    //
    // `jit_deferred`: in JIT mode the .wasm is not touched until the first
    //   spawn, so cold-start measurement includes Cranelift compile time.
    // `worker_chain`: the (logical name, InstancePre) list every worker
    //   instantiates from. Empty until the deferred JIT compile happens.
    let jit_deferred = compile_mode == "jit";
    let mut worker_chain: WorkerChain = Vec::new();

    if jit_deferred {
        info!(log, "autoscaler ready (JIT: compilation deferred to cold-start)",
              stage = stage.name,
              max_rep = stage.slo.max_replicas,
              min_rep = stage.slo.min_replicas);
    } else {
        // AOT or startup-JIT: compile/load now, before the loop.
        for (idx, (logical_name, wasm_path)) in halves.iter().enumerate() {
            let (component, compile_ms, mode) = load_component(&engine, wasm_path, &log);
            let linker = new_linker(&engine, &log);

            let t_pre = Instant::now();
            let instance_pre = linker
                .instantiate_pre(&component)
                .expect("Failed to create component InstancePre");
            let instantiate_pre_ms = t_pre.elapsed().as_secs_f64() * 1000.0;
            info!(log, "instance_pre ready",
                  stage = logical_name,
                  mode = mode,
                  instantiate_pre_ms = format!("{:.3}", instantiate_pre_ms));

            // ── Warmup instantiate ────────────────────────────────────────────
            // The first `instance_pre.instantiate(&mut store)` in a process is
            // significantly slower than subsequent ones because of one-time
            // Wasmtime engine setup (Cranelift relocation patching, pool
            // first-slot mmap + page-fault, signal handler install, etc.) plus
            // first-call WASI context construction.
            //
            // We do that throwaway instantiation here, at autoscaler startup,
            // before any user-facing cold start. The cost (~0.5 ms on first
            // stage, ~0.05 ms on subsequent stages) is shifted out of the
            // measured cold_start_ms window and into agent boot time. Only the
            // first half pays it: the effect is per-engine, not per-component.
            //
            // The `Store` is dropped at end of scope, returning its pool slot.
            // The dropped instance has no side effects — no events processed,
            // no sockets touched, no exports called. Pure init/teardown.
            if idx == 0 {
                let t_warm = Instant::now();
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
                        info!(log, "wasmtime warmup complete",
                              stage = stage.name,
                              warmup_ms = format!("{:.3}", warmup_ms));
                    }
                    Err(e) => {
                        // Non-fatal: if warmup fails, the real first worker will
                        // surface the same error. Don't abort agent startup over
                        // an instrumentation step.
                        warn!(log, "wasmtime warmup failed (continuing)",
                              stage = stage.name,
                              err = e);
                    }
                }
            }

            let init_action = if mode == "aot" { "init_aot" } else { "init_jit" };
            record_event(
                &telemetry, test_start, &stage.name, init_action,
                0, None, Some(compile_ms), Some(instantiate_pre_ms),
            );
            worker_chain.push((logical_name.clone(), Arc::new(instance_pre)));
        }

        info!(log, "autoscaler ready",
              stage = stage.name,
              max_rep = stage.slo.max_replicas,
              min_rep = stage.slo.min_replicas,
              queue_up = stage.slo.queue_up.unwrap_or(50.0),
              queue_down = stage.slo.queue_down.unwrap_or(0.0));
    }

    // Deferred-JIT compile of every half. Called from the spawn paths so the
    // cost lands inside the measured cold-start window, exactly as before.
    let compile_chain_now = |chain: &mut WorkerChain| {
        for (logical_name, wasm_path) in halves.iter() {
            let linker = new_linker(&engine, &log);
            let (ip, cm, ipm) =
                jit_compile_and_instantiate(&engine, wasm_path, linker, &log);
            record_event(&telemetry, test_start, &stage.name, "init_jit",
                         0, None, Some(cm), Some(ipm));
            chain.push((logical_name.clone(), Arc::new(ip)));
        }
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
    // Free replica indices. Popped (lowest first) at spawn, reclaimed when a
    // worker's `done` flag confirms its thread has fully exited. `workers.len()`
    // must NOT be used as the index: after a drain+respawn it collides with a
    // still-live worker's index, which on the SPSC mesh path puts two consumers
    // on one ring (UB) and on the zmq path merges two workers' hop labels.
    let mut free_indices: std::collections::BTreeSet<usize> = (0..max_rep).collect();
    // Monotonic tick counter, for decimating time-series samples below.
    let mut tick: u64 = 0;
    let mut up_votes: u32 = 0;
    let mut down_votes: u32 = 0;
    let mut ticks_since_spawn: u32 = u32::MAX;
    // Spawn floor for the first moments of this stage's life, cleared once
    // reached. A morph-spawned stage uses it to come up at `R_ab` without
    // permanently raising `min_replicas` — the autoscaler must stay free to
    // trim back down once the deleted edge overhead stops showing up.
    let mut warmup_target: usize = initial_replicas.min(max_rep);
    // Index into telemetry.scaling_events of the most-recently recorded
    // cold_start event whose cold_start_ms is still None.  We back-fill it
    // once the worker's first refill message arrives via the dispatcher metrics
    // poll (usually within a few ticks).  None means no pending back-fill.
    let mut pending_cs_event_idx: Option<usize> = None;

    loop {
        std::thread::sleep(Duration::from_millis(TICK_MS));
        tick = tick.wrapping_add(1);
        workers.retain(|w| {
            if w.done.load(Ordering::Relaxed) {
                free_indices.insert(w.replica_idx);
                false
            } else {
                true
            }
        });
        let current = workers.len();
        live_replicas.store(current, Ordering::Relaxed);
        ticks_since_spawn = ticks_since_spawn.saturating_add(1);
        if current >= warmup_target { warmup_target = 0; }

        // ── Stop (morph teardown) ────────────────────────────────────────────
        // Raise every replica's drain flag and exit once each has confirmed it
        // finished. Drain is the SAME mechanism scale-down already uses, so the
        // residue path that conserves events under scale-down conserves them
        // here too: an in-proc consumer closes its mesh column and hands out
        // what is left before returning None.
        //
        // Checked before the queue-depth fetch, which `continue`s when a
        // dispatcher is unreachable — a stopping stage must not be able to get
        // stuck behind that.
        if stop.load(Ordering::Relaxed) {
            for w in workers.iter() {
                w.drain_flag.store(true, Ordering::Relaxed);
            }
            if workers.is_empty() {
                live_replicas.store(0, Ordering::Relaxed);
                info!(log, "autoscaler stopped (all replicas drained)", stage = stage.name);
                return;
            }
            continue;
        }

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

        // Queue-depth time series — recorded for BOTH transports every 50th
        // tick (~50 ms). Previously this lived inside the dispatcher-metrics
        // block above, so in-proc stages (dispatcher_metrics == None) produced
        // an empty queue_depth block in the summary and nothing to plot.
        if tick % 50 == 0 {
            if let Ok(mut tel) = telemetry.try_lock() {
                let t_s = test_start.elapsed().as_secs_f64();
                tel.queue_depth_samples
                    .entry(stage.name.clone())
                    .or_default()
                    .push((round3(t_s), qd as u64));
            }
        }

        // ── EOS finishing gate (M0 barrier, see eos.rs) ─────────────────────
        // Once every expected upstream marker has been reported, drain the
        // whole stage: raise every replica's drain flag and stop scaling. On
        // the zmq path, wait for the dispatcher's buffer to empty first —
        // drained workers stop taking credits, so anything still queued there
        // would strand. (In-proc queues drain THROUGH the workers' residue
        // path, so no such wait is needed.) These drains are shutdown, not
        // scaling — deliberately not recorded as scaling events.
        if barrier.all_markers_seen() && !barrier.is_finishing() {
            let dispatcher_empty = input_edge.is_some() || qd <= 0.0;
            if dispatcher_empty && barrier.begin_finishing() {
                info!(log, "EOS barrier complete; draining all replicas",
                      stage = stage.name, live = workers.len());
                for w in workers.iter() {
                    w.drain_flag.store(true, Ordering::Relaxed);
                }
            }
        }
        if barrier.is_finishing() {
            // No spawns or scaling while finishing; keep ticking so the
            // retain() above reaps exiting workers (and frees indices).
            continue;
        }

        if current > max_rep {
            warn!(log, "invariant breach: current > max_rep, draining surplus",
                  current = current,
                  max_rep = max_rep);
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
            if worker_chain.is_empty() {
                compile_chain_now(&mut worker_chain);
            }
            info!(log, "cold start: spawning replica",
                  qd = format!("{:.0}", qd),
                  max_rep = max_rep);
            let replica_idx = match free_indices.pop_first() {
                Some(i) => i,
                None => {
                    // All max_rep indices are held by live or still-draining
                    // workers; retry next tick once one is reclaimed.
                    warn!(log, "no free replica index; deferring spawn", live = workers.len());
                    continue;
                }
            };
            workers.push(spawn_worker(
                &stage, replica_idx, &in_endpoint,
                build_output_specs(replica_idx),
                input_edge.for_replica(replica_idx),
                credit_window,
                &engine, &worker_chain,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                barrier.clone(),
                stop.clone(),
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

        // Below the spawn floor: `min_replicas`, or the (higher, transient)
        // warm-up target a morph-spawned stage comes up at.
        let spawn_floor = min_rep.max(warmup_target);
        if current < spawn_floor {
            let decision_ts = now_secs_f64();
            if worker_chain.is_empty() {
                compile_chain_now(&mut worker_chain);
            }
            info!(log, "below min: spawning replica",
                  current = current, min_rep = min_rep, floor = spawn_floor);
            let replica_idx = match free_indices.pop_first() {
                Some(i) => i,
                None => {
                    // All max_rep indices are held by live or still-draining
                    // workers; retry next tick once one is reclaimed.
                    warn!(log, "no free replica index; deferring spawn", live = workers.len());
                    continue;
                }
            };
            workers.push(spawn_worker(
                &stage, replica_idx, &in_endpoint,
                build_output_specs(replica_idx),
                input_edge.for_replica(replica_idx),
                credit_window,
                &engine, &worker_chain,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                barrier.clone(),
                stop.clone(),
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
            if worker_chain.is_empty() {
                compile_chain_now(&mut worker_chain);
            }
            info!(log, "scale up",
                  qd = format!("{:.0}", qd),
                  current = current,
                  new = (current + 1),
                  max_rep = max_rep);
            let replica_idx = match free_indices.pop_first() {
                Some(i) => i,
                None => {
                    // All max_rep indices are held by live or still-draining
                    // workers; retry next tick once one is reclaimed.
                    warn!(log, "no free replica index; deferring spawn", live = workers.len());
                    continue;
                }
            };
            workers.push(spawn_worker(
                &stage, replica_idx, &in_endpoint,
                build_output_specs(replica_idx),
                input_edge.for_replica(replica_idx),
                credit_window,
                &engine, &worker_chain,
                &last_active_ts, &avg_latency_us,
                decision_ts,
                worker_ctx.clone(),
                event_format.clone(),
                barrier.clone(),
                stop.clone(),
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
                info!(log, "scale down",
                      qd = format!("{:.0}", qd),
                      current = current,
                      min_rep = min_rep);
                record_event(&telemetry, test_start, &stage.name, "drain",
                             current.saturating_sub(1), None, None, None);
            }
            down_votes = 0;
        }

        if qd > queue_up && current >= max_rep {
            if up_votes == cooldown_up_ticks.saturating_sub(1) || up_votes == 0 {
                warn!(log, "SLO breach: queue depth at max replicas",
                      qd = format!("{:.0}", qd),
                      max_rep = max_rep);
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