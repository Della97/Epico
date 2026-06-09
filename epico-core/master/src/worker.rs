//! Worker thread — one per replica.
//!
//! Owns one Wasmtime Store + Instance and a pair of ZMQ sockets for its
//! entire life. Loops: recv event → call process-event → push output →
//! signal readiness back to dispatcher.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Func, InstancePre, Type, Val};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use epico_logger::Logger;

use crate::config::PipelineStage;
use crate::conversion::{extract_record_fields, extract_result_event_fields};
use crate::envelope::{EnvelopeFormat, EventEnvelope};
use crate::host::HostState;
use crate::inproc::Edge;

// ---------------------------------------------------------------------------
// Worker handle
// ---------------------------------------------------------------------------

pub(crate) struct WorkerHandle {
    _handle:    std::thread::JoinHandle<()>,
    pub drain_flag: Arc<AtomicBool>,
    pub done:       Arc<AtomicBool>,
}

// ---------------------------------------------------------------------------
// Spawn
// ---------------------------------------------------------------------------

pub(crate) fn spawn_worker(
    stage:          &PipelineStage,
    in_endpoint:    &str,
    out_endpoint:   &str,
    input_edge:     Option<Edge>,
    output_edge:    Option<Edge>,
    credit_window:  u32,
    engine:         &Engine,
    instance_pre:   &Arc<InstancePre<HostState>>,
    heartbeat:      &Arc<AtomicU64>,
    avg_latency_us: &Arc<AtomicU64>,
    decision_ts:    f64,
    worker_ctx:     zmq::Context,
    event_format:   String,
    log:            Logger,
) -> WorkerHandle {
    let stage_clone       = stage.clone();
    let in_ep             = in_endpoint.to_string();
    let out_ep            = out_endpoint.to_string();
    let engine_clone      = engine.clone();
    let instance_pre_clone = instance_pre.clone();
    let heartbeat_clone   = heartbeat.clone();
    let avg_lat_clone     = avg_latency_us.clone();
    let drain_flag        = Arc::new(AtomicBool::new(false));
    let drain_clone       = drain_flag.clone();
    let done              = Arc::new(AtomicBool::new(false));
    let done_clone        = done.clone();

    let handle = std::thread::spawn(move || {
        run_wasm_worker(
            stage_clone, in_ep, out_ep, input_edge, output_edge, credit_window,
            engine_clone, instance_pre_clone,
            heartbeat_clone, avg_lat_clone,
            drain_clone, decision_ts, worker_ctx, event_format, log,
        );
        done_clone.store(true, Ordering::Relaxed);
    });

    WorkerHandle { _handle: handle, drain_flag, done }
}

// ---------------------------------------------------------------------------
// Worker transport
// ---------------------------------------------------------------------------

/// Where a worker reads its input events from.
///
/// `Zmq` is the existing path: a DEALER fed by this stage's dispatcher, with
/// the credit-window control protocol (hello / refill / per-drop credit return)
/// layered on top. `Queue` is an in-process edge shared with the upstream
/// stage's workers — no control protocol, because the bounded ring is itself
/// the flow control. Phase 2 only ever constructs `Zmq`; `Queue` is wired in
/// Phase 3.
enum WorkerInput {
    Zmq {
        dealer: zmq::Socket,
        /// Extra events from a batched ROUTER message, drained one per loop
        /// iteration before the socket is touched again.
        pending: VecDeque<Bytes>,
    },
    #[allow(dead_code)]
    Queue(Edge),
}

impl WorkerInput {
    /// Next event, or `None` when the worker should exit (drain raised, or the
    /// input is gone). Encapsulates batch-unpacking on the zmq path and the
    /// blocking pop on the queue path. Identical receive semantics to the old
    /// inline loop: drain is checked first, then buffered batch events, then
    /// the socket; an `EAGAIN` recv timeout retries, a hard error exits.
    fn next_event(&mut self, drain: &AtomicBool) -> Option<Bytes> {
        match self {
            WorkerInput::Zmq { dealer, pending } => loop {
                if drain.load(Ordering::Relaxed) {
                    return None;
                }
                if let Some(ev) = pending.pop_front() {
                    return Some(ev);
                }
                match dealer.recv_multipart(0) {
                    Ok(frames) => {
                        let start = if !frames.is_empty() && frames[0].is_empty() { 1 } else { 0 };
                        let mut iter = frames.into_iter().skip(start);
                        match iter.next() {
                            Some(first) => {
                                for extra in iter {
                                    if !extra.is_empty() {
                                        pending.push_back(Bytes::from(extra));
                                    }
                                }
                                return Some(Bytes::from(first));
                            }
                            None => continue,
                        }
                    }
                    Err(zmq::Error::EAGAIN) => continue,
                    Err(_)                  => return None,
                }
            },
            WorkerInput::Queue(edge) => edge.pop(drain),
        }
    }

    /// True if this input runs the credit-window control protocol. The queue
    /// path returns false: the bounded ring is its flow control, so hello /
    /// refill / credit-return are skipped (and their payloads not even built).
    fn wants_credits(&self) -> bool {
        matches!(self, WorkerInput::Zmq { .. })
    }

    /// Send a control frame on the input channel (hello, credit refill, or an
    /// empty credit-return). No-op on the queue path.
    fn send_control(&self, bytes: &[u8]) {
        if let WorkerInput::Zmq { dealer, .. } = self {
            let _ = dealer.send(bytes, 0);
        }
    }
}

/// Where a worker sends its output events.
enum WorkerOutput {
    Zmq { pusher: zmq::Socket },
    #[allow(dead_code)]
    Queue(Edge),
}

impl WorkerOutput {
    /// Forward one event downstream. On the zmq path this is a PUSH; on the
    /// queue path it's a bounded enqueue that applies backpressure (and returns
    /// early if `drain` is raised mid-wait).
    fn send(&self, bytes: Bytes, drain: &AtomicBool) {
        match self {
            WorkerOutput::Zmq { pusher } => {
                let _ = pusher.send(bytes.as_ref(), 0);
            }
            WorkerOutput::Queue(edge) => {
                edge.push(bytes, drain);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

fn run_wasm_worker(
    stage:          PipelineStage,
    in_endpoint:    String,
    out_endpoint:   String,
    input_edge:     Option<Edge>,
    output_edge:    Option<Edge>,
    credit_window:  u32,
    engine:         Engine,
    instance_pre:   Arc<InstancePre<HostState>>,
    heartbeat:      Arc<AtomicU64>,
    avg_latency_us: Arc<AtomicU64>,
    drain_flag:     Arc<AtomicBool>,
    decision_ts:    f64,
    worker_ctx:     zmq::Context,
    event_format:   String,
    log:            Logger,
) {
    let spawn_ts   = decision_ts;
    // First wall-clock read in this worker thread. Used to bound the gap
    // between `decision_ts` (captured in the autoscaler before
    // `std::thread::spawn`) and the moment this thread actually started
    // running. That gap is the OS scheduler's contribution to cold start.
    let thread_entry_ts = now_secs_f64();
    let spawn_to_thread_ms = (thread_entry_ts - spawn_ts) * 1000.0;
    let start_time = Instant::now();
    let rid: u64   = rand::random();
    let rid_str    = format!("{:016x}", rid);

    // ── ZMQ sockets ──────────────────────────────────────────────────────────
    // The worker uses a context that the autoscaler has already warmed
    // (created and exercised with a throwaway socket) before this worker
    // was spawned. This moves libzmq's first-socket I/O thread spawn
    // cost (~5-10 ms on a busy agent process due to kernel scheduler
    // latency for the freshly pthread_create'd I/O thread) out of the
    // worker's cold-start measurement window and into agent startup.
    //
    // Without this hand-off, the first `ctx.socket(zmq::PUSH)` call in
    // a fresh-context worker was responsible for the entire ~5-10 ms
    // inflation of `cold_start_ms` on whichever stage's worker happened
    // to lose the scheduler race; the rest of the boot phase (other
    // libzmq calls, instantiate, export resolution) measures <100 us
    // each.
    let t_before_ctx_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    let ctx = worker_ctx;
    let t_ctx_ready_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    let pusher = ctx.socket(zmq::PUSH).expect("push socket");
    let t_push_socket_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    pusher.set_sndhwm(1000).ok();
    let t_push_setopt_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    let dealer = ctx.socket(zmq::DEALER).expect("dealer socket");
    let t_dealer_socket_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    dealer.set_identity(rid_str.as_bytes()).ok();
    dealer.set_rcvtimeo(50).ok();
    let t_sockets_created_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    if output_edge.is_none() {
        if let Err(e) = pusher.connect(&out_endpoint) {
            log.error("PUSH connect failed", &[("addr", &out_endpoint), ("err", &e.to_string())]);
            return;
        }
    }
    let t_pusher_connect_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    if input_edge.is_none() {
        if let Err(e) = dealer.connect(&in_endpoint) {
            log.error("DEALER connect failed", &[("addr", &in_endpoint), ("err", &e.to_string())]);
            return;
        }
    }
    let t_dealer_connect_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // ── Component instantiation ───────────────────────────────────────────────
    let host_state = HostState {
        table: ResourceTable::new(),
        wasi:  WasiCtxBuilder::new().build(),
        http:  WasiHttpCtx::new(),
        limits: crate::host::default_store_limits(),
    };
    let mut store = Store::new(&engine, host_state);
    // Bound this instance's resource growth and make it interruptible: a guest
    // that exceeds its memory ceiling gets a graceful error, and one that runs
    // past its per-call epoch deadline (armed before each call below) traps
    // instead of pinning this worker thread.
    store.limiter(|s| &mut s.limits);
    store.epoch_deadline_trap();

    let t_before_instantiate_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    let instance = match instance_pre.instantiate(&mut store) {
        Ok(i)  => i,
        Err(e) => {
            log.error("component instantiation failed", &[("err", &e.to_string())]);
            return;
        }
    };
    let t_instantiate_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // ── Locate process-event ──────────────────────────────────────────────────
    let component_ref = instance_pre.component();
    let mut func_ty_opt = None;
    let mut process_fn_opt: Option<Func> = None;

    for iface_name in &[
        "epico:pipeline/process@0.1.0",
        "epico:pipeline/process",
    ] {
        if let Some((_iface_item, iface_idx)) = component_ref.export_index(None, iface_name) {
            if let Some((ComponentItem::ComponentFunc(ft), fn_idx)) =
                component_ref.export_index(Some(&iface_idx), "process-event")
            {
                process_fn_opt = instance.get_func(&mut store, &fn_idx);
                func_ty_opt    = Some(ft);
                break;
            }
        }
    }

    let process_fn = match process_fn_opt {
        Some(f) => f,
        None    => {
            log.error("no process-event export found", &[("stage", &stage.name)]);
            return;
        }
    };
    let func_ty = match func_ty_opt {
        Some(t) => t,
        None    => {
            log.error("could not introspect process-event type", &[("stage", &stage.name)]);
            return;
        }
    };

    let param_types:  Vec<Type> = func_ty.params().collect();
    let result_types: Vec<Type> = func_ty.results().collect();

    if param_types.len() < 2 {
        log.error("process-event has wrong param count", &[
            ("stage",    &stage.name),
            ("expected", "2"),
            ("got",      &param_types.len().to_string()),
        ]);
        return;
    }

    let in_fields  = extract_record_fields(&param_types[0]);
    let out_fields = result_types
        .first()
        .map(extract_result_event_fields)
        .unwrap_or_default();
    let envelope_format = match EnvelopeFormat::parse(&event_format) {
        Ok(f) => f,
        Err(e) => {
            log.error("bad event_format", &[("err", &e.to_string())]);
            return;
        }
    };

    let t_export_lookup_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // DEBUG: report the actual variant of result_types[0]. We expect a
    // Type::Record (the event), but the WIT signature returns
    // tuple<event, bench-ctx>, so what wasmtime gives us here might be
    // a Tuple — in which case extract_record_fields returns []
    // because it only handles Record.
    let _result0_kind = match &result_types[0] {
        Type::Record(_)  => "Record",
        Type::Tuple(_)   => "Tuple",
        Type::List(_)    => "List",
        Type::String     => "String",
        Type::Bool       => "Bool",
        _                => "Other",
    };

    let boot_ms     = start_time.elapsed().as_secs_f64() * 1000.0;
    let now_ts      = now_secs_f64();
    let cold_start_ms = (now_ts - spawn_ts) * 1000.0;

    // Per-phase deltas. Each is the time spent IN that phase, not the
    // cumulative checkpoint.
    let phase_ctx_ms             = t_ctx_ready_ms          - t_before_ctx_ms;
    let phase_push_socket_ms     = t_push_socket_ms        - t_ctx_ready_ms;
    let phase_push_setopt_ms     = t_push_setopt_ms        - t_push_socket_ms;
    let phase_dealer_socket_ms   = t_dealer_socket_ms      - t_push_setopt_ms;
    let phase_dealer_setopt_ms   = t_sockets_created_ms    - t_dealer_socket_ms;
    let phase_pusher_connect_ms  = t_pusher_connect_ms     - t_sockets_created_ms;
    let phase_dealer_connect_ms  = t_dealer_connect_ms     - t_pusher_connect_ms;
    let phase_pre_inst_ms        = t_before_instantiate_ms - t_dealer_connect_ms;
    let phase_instantiate_ms     = t_instantiate_ms        - t_before_instantiate_ms;
    let phase_export_ms          = t_export_lookup_ms      - t_instantiate_ms;
    let phase_tail_ms            = boot_ms                 - t_export_lookup_ms;

    log.info("worker booted", &[
        ("rid",                  &rid_str[..8]),
        ("boot_ms",              &format!("{:.3}", boot_ms)),
        ("cold_start_ms",        &format!("{:.3}", cold_start_ms)),
        ("spawn_to_thread_ms",   &format!("{:.3}", spawn_to_thread_ms)),
        ("ph_ctx_ms",            &format!("{:.3}", phase_ctx_ms)),
        ("ph_push_socket_ms",    &format!("{:.3}", phase_push_socket_ms)),
        ("ph_push_setopt_ms",    &format!("{:.3}", phase_push_setopt_ms)),
        ("ph_dealer_socket_ms",  &format!("{:.3}", phase_dealer_socket_ms)),
        ("ph_dealer_setopt_ms",  &format!("{:.3}", phase_dealer_setopt_ms)),
        ("ph_pusher_connect_ms", &format!("{:.3}", phase_pusher_connect_ms)),
        ("ph_dealer_connect_ms", &format!("{:.3}", phase_dealer_connect_ms)),
        ("ph_pre_inst_ms",       &format!("{:.3}", phase_pre_inst_ms)),
        ("ph_instantiate_ms",    &format!("{:.3}", phase_instantiate_ms)),
        ("ph_export_ms",         &format!("{:.3}", phase_export_ms)),
        ("ph_tail_ms",           &format!("{:.3}", phase_tail_ms)),
        ("credit_window",        &credit_window.to_string()),
    ]);

    // Initial credit grant. The dispatcher reads `n_credits` and
    // populates this worker's credit balance accordingly.
    let hello = format!(
        "{{\"_ctrl\":\"hello\",\"rid\":\"{}\",\"fn\":\"{}\",\"n_credits\":{}}}",
        rid_str, stage.name, credit_window,
    );
    // Build the transport handles. An in-process edge replaces the zmq socket
    // on that side; the socket created above was never connected, so it's just
    // dropped here. The credit-window hello is sent only on a zmq input — the
    // queue path has no credit protocol.
    let worker_output = match output_edge {
        Some(edge) => WorkerOutput::Queue(edge),
        None       => WorkerOutput::Zmq { pusher },
    };
    let mut worker_input = match input_edge {
        Some(edge) => WorkerInput::Queue(edge),
        None       => {
            let _ = dealer.send(hello.as_bytes(), 0);
            WorkerInput::Zmq { dealer, pending: VecDeque::new() }
        }
    };

    let mut invocation_count: u64 = 0;
    // Events processed since the last credit refill was sent. We refill
    // when this hits half the window — early enough that the worker
    // doesn't starve waiting for the dispatcher to know about freed
    // credits, late enough that refill traffic stays well below per-
    // event traffic. With credit_window=1 the threshold is 1, which
    // means we send one refill per event — exactly the legacy behaviour
    // with no extra round-trips.
    let refill_threshold: u32 = (credit_window / 2).max(1);
    let mut processed_since_refill: u32 = 0;

    // ── Event loop ────────────────────────────────────────────────────────────
    // Events may arrive batched: one ROUTER message carries
    // [<delimiter>, ev1, ev2, ...]. We process one event per iteration and
    // stash the rest of the batch in `pending`, touching the socket only once
    // the batch drains. This is the receive side of the dispatcher's event
    // batching — it amortises one recv/poll across the whole batch while the
    // per-event body below is unchanged. With batch_events=1 each message holds
    // a single event and `pending` stays empty (legacy behaviour).
    loop {
        let event_owned = match worker_input.next_event(&drain_flag) {
            Some(ev) => ev,
            None     => break,
        };
        let event_bytes: &[u8] = event_owned.as_ref();

        if event_bytes.is_empty() { continue; }

        let total_t0 = Instant::now();

        heartbeat.store(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Ordering::Relaxed,
        );

        // Nanosecond resolution: at small payloads parse+serialize is
        // sub-microsecond, so `as_micros()` floored serde to 0 and made
        // serde_frac collapse. Measure in ns; the summary still reports
        // µs (as floats), so sub-µs costs survive instead of truncating.
        let mut serde_ns: u64 = 0;

        let parse_t0 = Instant::now();
        let envelope = match EventEnvelope::decode(envelope_format, event_owned.clone()) {
            Ok(v)  => v,
            Err(e) => {
                log.warn("bad event envelope from dispatcher", &[("err", &e.to_string())]);
                worker_input.send_control(b"");
                continue;
            }
        };
        serde_ns += parse_t0.elapsed().as_nanos() as u64;

        if envelope.is_eos() {
            log.info("EOS received; forwarding and exiting", &[
                ("stage", &stage.name),
            ]);
            worker_output.send(event_owned.clone(), &drain_flag);
            worker_input.send_control(b"");
            break;
        }

        let ev_val = match envelope.input_val(&in_fields, &param_types[0]) {
            Ok(v) => v,
            Err(e) => {
                log.error("event decode failed", &[("err", &e.to_string())]);
                worker_input.send_control(b"");
                continue;
            }
        };
        let bench_val = match envelope.bench_val(&param_types[1]) {
            Ok(v) => v,
            Err(e) => {
                log.error("bench decode failed", &[("err", &e.to_string())]);
                worker_input.send_control(b"");
                continue;
            }
        };

        let enter_ts = now_secs_f64();
        let t0       = Instant::now();

        let mut results = vec![Val::Bool(false); result_types.len()];
        // Give this invocation a fresh CPU budget. If the guest runs past it the
        // call returns a trap (handled below) rather than hanging the worker.
        store.set_epoch_deadline(crate::host::MAX_CALL_EPOCH_TICKS);
        let call_result = process_fn.call(&mut store, &[ev_val, bench_val], &mut results);

        if let Err(e) = call_result {
            log.error("process-event call error", &[("err", &e.to_string())]);
            worker_input.send_control(b"");
            let _ = process_fn.post_return(&mut store);
            continue;
        }

        let exit_ts    = now_secs_f64();
        let latency_us = t0.elapsed().as_micros() as u64;
        let prev_us    = avg_latency_us.load(Ordering::Relaxed);
        avg_latency_us.store((prev_us * 3 + latency_us) / 4, Ordering::Relaxed);

        let final_bytes = if !results.is_empty() {
            // The WIT signature is `process-event(...) -> tuple<event, bench-ctx>`.
            // wasmtime exposes that as a single result of Type::Tuple,
            // not as two separate results. Drill into the tuple:
            // - results[0] is Val::Tuple([event_val, bench_val])
            // - We want event_val for downstream serialization, and
            //   bench_val for the bench_json call below.
            //
            // Fall back to results[0] verbatim if we don't see a Tuple,
            // for compatibility with any future WIT that returns the
            // event directly.
            let (event_val, bench_val_from_tuple) = match (&results[0], &result_types[0]) {
                (Val::Tuple(elems), Type::Tuple(_)) if elems.len() >= 2 => {
                    (elems[0].clone(), Some(elems[1].clone()))
                }
                _ => (results[0].clone(), None),
            };
            // Stash bench_val into results[1] slot for the bench_json
            // call below, which expects results.get(1).
            if let Some(bv) = bench_val_from_tuple {
                if results.len() < 2 {
                    results.push(bv);
                } else {
                    results[1] = bv;
                }
            }
            let bench_result = results.get(1).unwrap_or(&Val::Bool(false));
            let fields = if out_fields.is_empty() { &in_fields } else { &out_fields };
            match envelope.encode_output(
                &event_val,
                fields,
                bench_result,
                &stage.name,
                enter_ts,
                exit_ts,
            ) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log.error("event encode failed", &[("err", &e.to_string())]);
                    worker_input.send_control(b"");
                    let _ = process_fn.post_return(&mut store);
                    continue;
                }
            }
        } else {
            Bytes::new()
        };

        let serialize_t0 = Instant::now();
        serde_ns += serialize_t0.elapsed().as_nanos() as u64;

        let _ = process_fn.post_return(&mut store);

        if !final_bytes.is_empty() {
            worker_output.send(final_bytes, &drain_flag);
        }

        let total_ns = total_t0.elapsed().as_nanos() as u64;
        invocation_count += 1;
        processed_since_refill += 1;

        // Send a refill once we've processed at least `refill_threshold`
        // events since the last one. With larger windows the refill batches credits and
        // metrics together — the metrics_payload for the autoscaler
        // arrives less often but the latest sample is always carried.
        if worker_input.wants_credits() && processed_since_refill >= refill_threshold {
            let latency_ms = latency_us / 1000;
            let refill_payload = format!(
                "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                 \"cold_start_ms\":{:.5},\"spawn_ts\":{:.6},\
                 \"spawn_to_thread_ms\":{:.5},\
                 \"ph_ctx_ms\":{:.5},\
                 \"ph_push_socket_ms\":{:.5},\"ph_push_setopt_ms\":{:.5},\
                 \"ph_dealer_socket_ms\":{:.5},\"ph_dealer_setopt_ms\":{:.5},\
                 \"ph_pusher_connect_ms\":{:.5},\"ph_dealer_connect_ms\":{:.5},\
                 \"ph_pre_inst_ms\":{:.5},\
                 \"ph_instantiate_ms\":{:.5},\"ph_export_ms\":{:.5},\
                 \"ph_tail_ms\":{:.5},\
                 \"is_leader\":false,\"p99_latency_ms\":{},\
                 \"total_ns\":{},\"serde_ns\":{},\"n_credits\":{}}}",
                rid_str, stage.name, cold_start_ms, spawn_ts,
                spawn_to_thread_ms,
                phase_ctx_ms,
                phase_push_socket_ms, phase_push_setopt_ms,
                phase_dealer_socket_ms, phase_dealer_setopt_ms,
                phase_pusher_connect_ms, phase_dealer_connect_ms,
                phase_pre_inst_ms,
                phase_instantiate_ms, phase_export_ms,
                phase_tail_ms,
                latency_ms, total_ns, serde_ns, processed_since_refill,
            );
            worker_input.send_control(refill_payload.as_bytes());
            processed_since_refill = 0;
        }
    }

    log.info("worker drained", &[
        ("rid",         &rid_str[..8]),
        ("invocations", &invocation_count.to_string()),
    ]);
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
