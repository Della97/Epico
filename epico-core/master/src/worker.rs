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
use epico_logger::{debug, error, info, warn};

use crate::config::PipelineStage;
use crate::eos::StageEosBarrier;
use crate::conversion::{extract_record_fields, extract_result_event_fields, RecordField};
use crate::envelope::{EnvelopeFormat, EventEnvelope};
use crate::host::HostState;
use crate::spsc::{EdgeIn, EdgeOut};
use crate::telemetry::live::{StageTiming, TimingAccum};

/// Events between two flushes of the worker's local [`TimingAccum`] into the
/// stage's shared counters. Small enough that a controller reading once per
/// second never sees stale timing, large enough that the atomic traffic is
/// negligible against the per-event work.
const TIMING_FLUSH_EVENTS: u64 = 64;

/// Upper bound on how long a partial window may sit unpublished. At low event
/// rates 64 events can span many seconds, which would starve the controller of
/// exactly the diagnostic it needs; the deadline is checked against the
/// per-event `Instant` the loop already takes, so it costs no extra clock read.
const TIMING_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// One logical stage's live instance inside a worker: its own `Store`, its own
/// resolved `process-event`, its own cached field layout.
///
/// An ordinary worker holds exactly one of these. A FUSED worker holds several
/// and calls them back to back in the same thread with the intermediate value
/// never leaving the host — that is the whole mechanism behind stage fusion
/// (FUSION_SCISSION §2, Option A). Keeping one `Store` per half preserves the
/// sandbox boundary between halves exactly as it was before fusion, and lets
/// each half re-arm its own epoch budget so isolation semantics stay faithful
/// to the unfused pipeline.
struct StageInstance {
    store:        Store<HostState>,
    process_fn:   Func,
    param_types:  Vec<Type>,
    result_types: Vec<Type>,
    in_fields:    Vec<RecordField>,
    out_fields:   Vec<RecordField>,
    /// Telemetry label `logical_stage#replica`. The collector strips `#r` for
    /// per-stage aggregation, so a fused worker's halves keep showing up under
    /// their ORIGINAL logical names — the morph is visible in the data without
    /// any collector or summary change.
    hop_label:    String,
    /// Reused across events so the hot loop allocates nothing for results.
    results:      Vec<Val>,
    typed:        Option<Box<dyn crate::typed::PreparedDispatch>>,
}

/// One half's post-call output: the lifted values plus its hop timestamps.
struct HalfOutput {
    event_val:  Val,
    bench_val:  Val,
    enter:      f64,
    exit:       f64,
    latency_us: u64,
}

impl StageInstance {
    /// Invoke this half and lift its results into owned host-side values.
    ///
    /// Ordering is strictly `call → clone results → post_return`. The clone is
    /// what makes chaining sound: component-model `Val`s are lifted into owned
    /// Rust data, so the returned event does not borrow guest memory and stays
    /// valid as the NEXT half's parameter after this half's `post_return`.
    ///
    /// `post_return` is only legal after a SUCCESSFUL call, so the error path
    /// returns before it (calling it after a failed call panics inside
    /// wasmtime).
    fn call(&mut self, ev: Val, bench: Val) -> anyhow::Result<HalfOutput> {
        // `Func::call` requires the result slice length to equal the function's
        // declared result count; earlier iterations may have grown it.
        self.results.truncate(self.result_types.len());
        // Fresh CPU budget per half: a runaway guest traps instead of pinning
        // this worker thread, and a fused chain of two halves gets two budgets
        // rather than one shared one.
        self.store.set_epoch_deadline(crate::host::MAX_CALL_EPOCH_TICKS);

        let enter = now_secs_f64();
        let t0    = Instant::now();
        self.process_fn.call(&mut self.store, &[ev, bench], &mut self.results)?;
        let exit       = now_secs_f64();
        let latency_us = t0.elapsed().as_micros() as u64;

        // The WIT signature is `process-event(...) -> tuple<event, bench-ctx>`,
        // which wasmtime exposes as ONE result of `Type::Tuple`. Fall back to
        // results[0] verbatim if a future WIT returns the event directly.
        let (event_val, bench_val) = match (self.results.first(), self.result_types.first()) {
            (Some(Val::Tuple(elems)), Some(Type::Tuple(_))) if elems.len() >= 2 => {
                (elems[0].clone(), elems[1].clone())
            }
            (Some(v), _) => (v.clone(), Val::Bool(false)),
            (None, _)    => (Val::Bool(false), Val::Bool(false)),
        };

        let _ = self.process_fn.post_return(&mut self.store);
        Ok(HalfOutput { event_val, bench_val, enter, exit, latency_us })
    }

    /// Output field layout for encoding. Some stages declare no distinct output
    /// record, in which case the input layout is the right one.
    fn encode_fields(&self) -> &[RecordField] {
        if self.out_fields.is_empty() { &self.in_fields } else { &self.out_fields }
    }
}

// ---------------------------------------------------------------------------
// Worker handle
// ---------------------------------------------------------------------------

pub(crate) struct WorkerHandle {
    _handle:    std::thread::JoinHandle<()>,
    pub drain_flag: Arc<AtomicBool>,
    pub done:       Arc<AtomicBool>,
    /// Mesh column / hop-label index owned by this worker for its whole life.
    /// The autoscaler reclaims it into its free-list only after `done` is set,
    /// so two live workers can never share an index — required by the SPSC
    /// mesh contract (one consumer per ring) and by per-replica telemetry.
    pub replica_idx: usize,
}

// ---------------------------------------------------------------------------
// Spawn
// ---------------------------------------------------------------------------

/// The components a worker instantiates, in call order: `(logical stage name,
/// cached InstancePre)`. One entry for an ordinary stage; N for a fused one.
pub(crate) type WorkerChain = Vec<(String, Arc<InstancePre<HostState>>)>;

pub(crate) fn spawn_worker(
    stage:          &PipelineStage,
    replica_idx:    usize,
    in_endpoint:    &str,
    output_specs:   Vec<OutputSpec>,
    input_edge:     Option<EdgeIn>,
    credit_window:  u32,
    engine:         &Engine,
    chain:          &WorkerChain,
    heartbeat:      &Arc<AtomicU64>,
    avg_latency_us: &Arc<AtomicU64>,
    decision_ts:    f64,
    worker_ctx:     zmq::Context,
    event_format:   String,
    barrier:        Arc<StageEosBarrier>,
    stage_stop:     Arc<AtomicBool>,
    // This stage's shared timing block, written by every replica. Mirrors the
    // `live_replicas` pattern: the worker owns the measurement, a reader that
    // keeps its own cursor owns the sampling rate. See `telemetry::live`.
    timing:         Arc<StageTiming>,
    log:            Logger,
) -> WorkerHandle {
    let stage_clone       = stage.clone();
    let in_ep             = in_endpoint.to_string();
    let engine_clone      = engine.clone();
    let chain_clone       = chain.clone();
    let heartbeat_clone   = heartbeat.clone();
    let avg_lat_clone     = avg_latency_us.clone();
    let drain_flag        = Arc::new(AtomicBool::new(false));
    let drain_clone       = drain_flag.clone();
    let done              = Arc::new(AtomicBool::new(false));
    let done_clone        = done.clone();

    let handle = std::thread::spawn(move || {
        run_wasm_worker(
            stage_clone, replica_idx, in_ep, output_specs, input_edge, credit_window,
            engine_clone, chain_clone,
            heartbeat_clone, avg_lat_clone,
            drain_clone, stage_stop, decision_ts, worker_ctx, event_format, barrier, timing, log,
        );
        done_clone.store(true, Ordering::Relaxed);
    });

    WorkerHandle { _handle: handle, drain_flag, done, replica_idx }
}

/// One of a stage's out-edges, resolved for a specific replica: either an
/// in-process queue handle or a zmq endpoint to connect a PUSH socket to.
/// The autoscaler builds one per successor, in `succs()` order.
pub(crate) enum OutputSpec {
    Queue(EdgeOut),
    Zmq(String),
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
    Queue(EdgeIn),
}

impl WorkerInput {
    /// Next event, or `None` when the worker should exit (drain raised, or the
    /// input is gone). Encapsulates batch-unpacking on the zmq path and the
    /// blocking pop on the queue path. Identical receive semantics to the old
    /// inline loop: drain is checked first, then buffered batch events, then
    /// the socket; an `EAGAIN` recv timeout retries, a hard error exits.
    fn next_event(&mut self, drain: &AtomicBool, stop: &AtomicBool) -> Option<Bytes> {
        match self {
            WorkerInput::Zmq { dealer, pending } => loop {
                if drain.load(Ordering::Relaxed) {
                    // Flush work this worker has already accepted before
                    // exiting: buffered batch events first, then whatever sits
                    // in the socket buffer (non-blocking). Previously a raised
                    // drain discarded both — the zmq flavor of the 1.6 leak.
                    if let Some(ev) = pending.pop_front() {
                        return Some(ev);
                    }
                    match dealer.recv_multipart(zmq::DONTWAIT) {
                        Ok(frames) => {
                            let start =
                                if !frames.is_empty() && frames[0].is_empty() { 1 } else { 0 };
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
                        Err(_) => return None, // EAGAIN: socket empty — done
                    }
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
            // The zmq arm needs no `stop` case: a draining worker sends no more
            // credit refills, so the dispatcher stops feeding it and the
            // DONTWAIT recv above returns EAGAIN within one window.
            WorkerInput::Queue(edge) => edge.pop(drain, stop),
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
    Queue(EdgeOut),
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

/// Forward one event to EVERY out-edge (broadcast fan-out).
///
/// `Bytes::clone` is an atomic refcount bump on a shared buffer — the payload
/// is never copied, so a K-way fan-out costs K sends, not K serializations.
/// The last output takes the original handle so a linear stage (K = 1) does
/// exactly one send with no clone at all.
fn send_all(outs: &[WorkerOutput], bytes: Bytes, drain: &AtomicBool) {
    match outs.split_last() {
        None => {}
        Some((last, rest)) => {
            for o in rest {
                o.send(bytes.clone(), drain);
            }
            last.send(bytes, drain);
        }
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

fn run_wasm_worker(
    stage:          PipelineStage,
    replica_idx:    usize,
    in_endpoint:    String,
    output_specs:   Vec<OutputSpec>,
    input_edge:     Option<EdgeIn>,
    credit_window:  u32,
    engine:         Engine,
    chain_pres:     WorkerChain,
    heartbeat:      Arc<AtomicU64>,
    avg_latency_us: Arc<AtomicU64>,
    drain_flag:     Arc<AtomicBool>,
    // The stage-level hard-retirement flag, shared with every sibling replica
    // and with the autoscaler. See `EdgeIn::pop` for why it differs from
    // `drain_flag` on the shared MPMC ring.
    stage_stop:     Arc<AtomicBool>,
    decision_ts:    f64,
    worker_ctx:     zmq::Context,
    event_format:   String,
    barrier:        Arc<StageEosBarrier>,
    timing:         Arc<StageTiming>,
    log:            Logger,
) {
    let spawn_ts   = decision_ts;
    // Physical-stage label, for logs and refill payloads. Per-EVENT telemetry
    // labels are per-half and live on each `StageInstance` — a fused worker
    // emits one hop per logical stage, under the logical stage's own name.
    let hop_label = format!("{}#{}", stage.name, replica_idx);
    let fused     = chain_pres.len() > 1;

    // Output sends must never abort: an event that reaches the output side has
    // already been consumed from the input, so dropping it because OUR drain
    // flag happened to be raised while the downstream edge was momentarily
    // full would leak it. Blocking here cannot wedge shutdown — worker threads
    // die with the process, and a blocked mesh push registers in the edge's
    // occupancy signal so the downstream autoscaler respawns a consumer.
    let never_drain = AtomicBool::new(false);

    // Native-bypass experiment mode (`EPICO_NATIVE_STAGE`):
    //   "passthrough" — forward the input bytes untouched: no JSON parse, no
    //                   hop append. Measures transport + recv/push plumbing
    //                   only. Per-stage telemetry is empty (no hops); e2e and
    //                   throughput remain valid since bench_ts_wall rides
    //                   through unchanged.
    //   "serde"       — full envelope path (parse + hop append + serialize)
    //                   but no Val construction and no wasm call.
    // Together with the normal wasm path these decompose the stage service
    // time into transport/plumbing, serde, and wasm+dynamic-dispatch shares.
    // Identity pipelines only — user code is NOT executed in either mode.
    let native_mode: Option<&'static str> =
        match std::env::var("EPICO_NATIVE_STAGE").ok().as_deref() {
            Some("passthrough") => Some("passthrough"),
            Some("serde")       => Some("serde"),
            Some(other) => {
                warn!(log, "unknown EPICO_NATIVE_STAGE value; running normal wasm path",
                      value = other);
                None
            }
            None => None,
        };
    if let Some(m) = native_mode {
        info!(log, "NATIVE BYPASS ACTIVE — wasm is not being called", mode = m);
    }

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

    // Outputs: one handle per out-edge (broadcast fan-out sends to all).
    // In-proc edges need no socket; each zmq edge gets its own PUSH.
    let mut worker_outputs: Vec<WorkerOutput> = Vec::with_capacity(output_specs.len());
    for spec in output_specs {
        match spec {
            OutputSpec::Queue(edge) => worker_outputs.push(WorkerOutput::Queue(edge)),
            OutputSpec::Zmq(addr) => {
                let pusher = ctx.socket(zmq::PUSH).expect("push socket");
                pusher.set_sndhwm(1000).ok();
                if let Err(e) = pusher.connect(&addr) {
                    error!(log, "PUSH connect failed", addr = addr, err = e);
                    return;
                }
                worker_outputs.push(WorkerOutput::Zmq { pusher });
            }
        }
    }
    let t_outputs_ready_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    let dealer = ctx.socket(zmq::DEALER).expect("dealer socket");
    let t_dealer_socket_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    dealer.set_identity(rid_str.as_bytes()).ok();
    dealer.set_rcvtimeo(50).ok();
    let t_sockets_created_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    if input_edge.is_none() {
        if let Err(e) = dealer.connect(&in_endpoint) {
            error!(log, "DEALER connect failed", addr = in_endpoint, err = e);
            return;
        }
    }
    let t_dealer_connect_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    let envelope_format = match EnvelopeFormat::parse(&event_format) {
        Ok(f) => f,
        Err(e) => {
            error!(log, "bad event_format", err = e);
            return;
        }
    };
    // Outgoing wire format. `EPICO_BINARY_EDGES=1` switches stage OUTPUT to
    // the compact binary envelope; decode is always magic-sniffed, so the
    // first stage keeps accepting JSON from the loadgen and external
    // producers with zero coordination. EOS markers are forwarded verbatim
    // (they stay JSON), so EOS detection is unaffected end to end.
    let out_format = if std::env::var("EPICO_BINARY_EDGES").map(|v| v == "1").unwrap_or(false) {
        crate::envelope::EnvelopeFormat::Binary
    } else {
        envelope_format
    };
    if out_format == crate::envelope::EnvelopeFormat::Binary {
        info!(log, "binary edges active: stage output uses binary envelope");
    }

    // ── Component instantiation (one per fused half) ──────────────────────────
    // Nothing is compiled here: every half comes from an already-cached
    // `InstancePre`, so a fused replica's cold start is exactly N instantiates —
    // the same cost as spawning one replica of each half today.
    let t_before_instantiate_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    let mut instantiate_acc_ms  = 0.0_f64;
    let mut chain: Vec<StageInstance> = Vec::with_capacity(chain_pres.len());

    for (logical_name, instance_pre) in chain_pres.iter() {
        let host_state = HostState {
            table: ResourceTable::new(),
            wasi:  WasiCtxBuilder::new().build(),
            http:  WasiHttpCtx::new(),
            limits: crate::host::default_store_limits(),
        };
        let mut store = Store::new(&engine, host_state);
        // Bound this instance's resource growth and make it interruptible: a
        // guest that exceeds its memory ceiling gets a graceful error, and one
        // that runs past its per-call epoch deadline (armed before each call)
        // traps instead of pinning this worker thread. Per-half, so fusing two
        // stages does not weaken either one's sandbox.
        store.limiter(|s| &mut s.limits);
        store.epoch_deadline_trap();

        let t_inst0 = start_time.elapsed().as_secs_f64() * 1000.0;
        let instance = match instance_pre.instantiate(&mut store) {
            Ok(i)  => i,
            Err(e) => {
                error!(log, "component instantiation failed",
                       stage = logical_name, err = e);
                return;
            }
        };
        instantiate_acc_ms += start_time.elapsed().as_secs_f64() * 1000.0 - t_inst0;

        // ── Locate process-event ──────────────────────────────────────────────
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
                error!(log, "no process-event export found", stage = logical_name);
                return;
            }
        };
        let func_ty = match func_ty_opt {
            Some(t) => t,
            None    => {
                error!(log, "could not introspect process-event type", stage = logical_name);
                return;
            }
        };

        let param_types:  Vec<Type> = func_ty.params().collect();
        let result_types: Vec<Type> = func_ty.results().collect();

        if param_types.len() < 2 {
            error!(log, "process-event has wrong param count",
                  stage = logical_name,
                  expected = "2",
                  got = param_types.len());
            return;
        }

        let in_fields  = extract_record_fields(&param_types[0]);
        let out_fields = result_types
            .first()
            .map(extract_result_event_fields)
            .unwrap_or_default();

        // Typed fast path: if the generated agent registered concrete types for
        // this stage (and EPICO_DYNAMIC_DISPATCH != 1), type the resolved Func
        // once and skip the per-event Val layer entirely. Falls back to the
        // dynamic path on any prepare failure.
        //
        // A FUSED chain always runs dynamic: `PreparedDispatch::call` goes
        // envelope→bytes in one shot and so cannot be chained without a new
        // trait method. Comparing a typed unfused baseline against a dynamic
        // fused arm would measure typed-vs-dynamic rather than unfused-vs-fused,
        // so BOTH arms must be run with `EPICO_DYNAMIC_DISPATCH=1` for the M2
        // numbers. Chained typed dispatch is real work, deferred until
        // break-even is known.
        let typed: Option<Box<dyn crate::typed::PreparedDispatch>> = if fused {
            None
        } else {
            match crate::typed::lookup(logical_name) {
                Some(d) => match d.prepare(&mut store, process_fn) {
                    Ok(p) => {
                        info!(log, "TYPED DISPATCH ACTIVE — Val layer bypassed",
                              stage = logical_name);
                        Some(p)
                    }
                    Err(e) => {
                        warn!(log, "typed dispatch prepare failed; using dynamic path",
                              stage = logical_name, err = e);
                        None
                    }
                },
                None => None,
            }
        };

        let results = vec![Val::Bool(false); result_types.len()];
        chain.push(StageInstance {
            store,
            process_fn,
            param_types,
            result_types,
            in_fields,
            out_fields,
            hop_label: format!("{}#{}", logical_name, replica_idx),
            results,
            typed,
        });
    }

    if chain.is_empty() {
        error!(log, "worker has no components to run", stage = stage.name);
        return;
    }
    if fused {
        info!(log, "FUSED WORKER — halves run back to back, edge deleted",
              stage = stage.name,
              halves = chain.iter().map(|c| c.hop_label.clone())
                            .collect::<Vec<_>>().join(" -> "));
    }

    let t_instantiate_ms   = t_before_instantiate_ms + instantiate_acc_ms;
    let t_export_lookup_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    let boot_ms     = start_time.elapsed().as_secs_f64() * 1000.0;
    let now_ts      = now_secs_f64();
    let cold_start_ms = (now_ts - spawn_ts) * 1000.0;

    // Per-phase deltas. Each is the time spent IN that phase, not the
    // cumulative checkpoint.
    let phase_ctx_ms             = t_ctx_ready_ms          - t_before_ctx_ms;
    // All output handles (sockets created + set + connected) in one phase now
    // that a stage can have several out-edges.
    let phase_outputs_ms         = t_outputs_ready_ms      - t_ctx_ready_ms;
    let phase_dealer_socket_ms   = t_dealer_socket_ms      - t_outputs_ready_ms;
    let phase_dealer_setopt_ms   = t_sockets_created_ms    - t_dealer_socket_ms;
    let phase_dealer_connect_ms  = t_dealer_connect_ms     - t_sockets_created_ms;
    let phase_pre_inst_ms        = t_before_instantiate_ms - t_dealer_connect_ms;
    let phase_instantiate_ms     = t_instantiate_ms        - t_before_instantiate_ms;
    let phase_export_ms          = t_export_lookup_ms      - t_instantiate_ms;
    let phase_tail_ms            = boot_ms                 - t_export_lookup_ms;

    // Human view: one readable line with the cold-start decomposition grouped
    // into its four meaningful phases. Full ph_* granularity goes to a debug
    // record below — hidden from stderr by default (EPICO_LOG=debug shows it)
    // but ALWAYS present in the JSONL, so nothing is lost for analysis.
    info!(log, "worker booted",
          replica = replica_idx,
          rid = &rid_str[..8],
          cold_start_ms = format!("{:.3}", cold_start_ms),
          spawn_ms = format!("{:.3}", spawn_to_thread_ms),
          sockets_ms = format!("{:.3}",
              phase_ctx_ms + phase_outputs_ms
              + phase_dealer_socket_ms + phase_dealer_setopt_ms
              + phase_dealer_connect_ms),
          instantiate_ms = format!("{:.3}", phase_pre_inst_ms + phase_instantiate_ms),
          export_ms = format!("{:.3}", phase_export_ms + phase_tail_ms),
          credit_window = credit_window);
    debug!(log, "worker boot phases",
          rid = &rid_str[..8],
          boot_ms = format!("{:.3}", boot_ms),
          cold_start_ms = format!("{:.3}", cold_start_ms),
          spawn_to_thread_ms = format!("{:.3}", spawn_to_thread_ms),
          ph_ctx_ms = format!("{:.3}", phase_ctx_ms),
          ph_outputs_ms = format!("{:.3}", phase_outputs_ms),
          ph_dealer_socket_ms = format!("{:.3}", phase_dealer_socket_ms),
          ph_dealer_setopt_ms = format!("{:.3}", phase_dealer_setopt_ms),
          ph_dealer_connect_ms = format!("{:.3}", phase_dealer_connect_ms),
          ph_pre_inst_ms = format!("{:.3}", phase_pre_inst_ms),
          ph_instantiate_ms = format!("{:.3}", phase_instantiate_ms),
          ph_export_ms = format!("{:.3}", phase_export_ms),
          ph_tail_ms = format!("{:.3}", phase_tail_ms));

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
    // ── Per-stage timing (D7) ────────────────────────────────────────────────
    // The refill payload above carries the same numbers, but ONLY on the zmq
    // path: an in-proc stage has no dispatcher to send them to, which is why
    // every `worker_timing.*.total_us` in the phase-4 spsc/mpmc summaries reads
    // 0.0. These counters are transport-independent, so the controller's
    // compute-vs-overhead diagnosis works on the transports M2 actually
    // measures. Accumulated locally, published on a window boundary.
    let mut timing_acc = TimingAccum::default();
    let mut last_timing_flush = Instant::now();
    // Per-half `(enter_ts, exit_ts)` for the event in flight, index-aligned with
    // `chain`. Hoisted so the hot loop allocates nothing.
    let mut hop_times: Vec<(f64, f64)> = Vec::with_capacity(chain.len());
    // The boot-phase refill fields (cold_start_ms, ph_*) never change after
    // worker boot; send them once and use a compact payload afterwards. The
    // autoscaler parses refills with `.get` + defaults, so absent keys are
    // fine, and the dispatcher only substring-scans for n_credits.
    let mut sent_boot_refill = false;

    // Boot succeeded: this replica now counts toward the stage's live set for
    // the EOS barrier (see eos.rs — the LAST worker out forwards the marker).
    barrier.worker_started();

    /// Account one finished event into the local timing window, publishing it
    /// into the shared stage counters when the window closes. Every path out of
    /// the event loop below (passthrough, serde bypass, typed fast path, and
    /// the dynamic path) records exactly once, so `events` counts events rather
    /// than iterations. `wasm_ns` is the summed per-half call time — 0 on the
    /// host-native bypasses, which genuinely run no wasm.
    macro_rules! record_timing {
        ($total_t0:expr, $serde_ns:expr, $wasm_ns:expr) => {{
            timing_acc.observe($total_t0.elapsed().as_nanos() as u64, $serde_ns, $wasm_ns);
            if timing_acc.events >= TIMING_FLUSH_EVENTS
                || $total_t0.saturating_duration_since(last_timing_flush)
                    >= TIMING_FLUSH_INTERVAL
            {
                timing_acc.flush_into(&timing);
                last_timing_flush = $total_t0;
            }
        }};
    }

    // ── Event loop ────────────────────────────────────────────────────────────
    // Events may arrive batched: one ROUTER message carries
    // [<delimiter>, ev1, ev2, ...]. We process one event per iteration and
    // stash the rest of the batch in `pending`, touching the socket only once
    // the batch drains. This is the receive side of the dispatcher's event
    // batching — it amortises one recv/poll across the whole batch while the
    // per-event body below is unchanged. With batch_events=1 each message holds
    // a single event and `pending` stays empty (legacy behaviour).
    loop {
        let event_owned = match worker_input.next_event(&drain_flag, &stage_stop) {
            Some(ev) => ev,
            None     => break,
        };
        let event_bytes: &[u8] = event_owned.as_ref();

        if event_bytes.is_empty() { continue; }

        let total_t0 = Instant::now();

        // Heartbeat every 16th event instead of every event: one
        // clock_gettime + atomic store saved on 15/16 iterations. Staleness
        // is bounded by 16 events, which at any rate above ~16 ev/s keeps it
        // under the autoscaler's seconds-scale idle thresholds; below that
        // rate the worker spends its life blocked in recv anyway.
        if invocation_count & 0xF == 0 {
            heartbeat.store(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Relaxed,
            );
        }

        // ── Native bypass: passthrough ────────────────────────────────────
        // Forward bytes untouched. EOS is detected with a byte scan so this
        // path pays no JSON parse at all. The reduced refill keeps the
        // credit protocol and worker_timing (total_ns) alive; serde_ns is 0
        // by construction.
        if native_mode == Some("passthrough") {
            const EOS_PAT: &[u8] = b"\"__epico_eos\"";
            let is_eos = event_bytes.len() >= EOS_PAT.len()
                && event_bytes.windows(EOS_PAT.len()).any(|w| w == EOS_PAT);
            if is_eos {
                // Report to the stage barrier and keep processing; the
                // autoscaler drains the whole stage and the LAST worker out
                // forwards the marker (see eos.rs).
                barrier.report(event_owned.clone());
                worker_input.send_control(b"");
                continue;
            }
            send_all(&worker_outputs, event_owned.clone(), &never_drain);
            invocation_count += 1;
            processed_since_refill += 1;
            record_timing!(total_t0, 0, 0);
            if worker_input.wants_credits() && processed_since_refill >= refill_threshold {
                let total_ns = total_t0.elapsed().as_nanos() as u64;
                let refill_payload = format!(
                    "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                     \"is_leader\":false,\"p99_latency_ms\":0,\
                     \"total_ns\":{},\"serde_ns\":0,\"n_credits\":{}}}",
                    rid_str, stage.name, total_ns, processed_since_refill,
                );
                worker_input.send_control(refill_payload.as_bytes());
                processed_since_refill = 0;
            }
            continue;
        }

        // Nanosecond resolution: at small payloads parse+serialize is
        // sub-microsecond, so `as_micros()` floored serde to 0 and made
        // serde_frac collapse. Measure in ns; the summary still reports
        // µs (as floats), so sub-µs costs survive instead of truncating.
        let mut serde_ns: u64 = 0;

        let parse_t0 = Instant::now();
        let envelope = match EventEnvelope::decode(envelope_format, event_owned.clone()) {
            Ok(v)  => v,
            Err(e) => {
                warn!(log, "bad event envelope from dispatcher", err = e);
                worker_input.send_control(b"");
                continue;
            }
        };
        serde_ns += parse_t0.elapsed().as_nanos() as u64;

        if envelope.is_eos() {
            // Control-plane EOS (M0 barrier): report to the stage barrier and
            // keep processing. The autoscaler transitions the stage to
            // finishing (drains every replica); the LAST worker out forwards
            // the marker, so nothing can be in flight behind it — this
            // replaces the per-worker deferral AND fixes the sibling-overtake
            // race (TODO 1.5) structurally.
            info!(log, "EOS marker received; reported to stage barrier", stage = stage.name);
            barrier.report(event_owned.clone());
            worker_input.send_control(b"");
            continue;
        }

        // ── Native bypass: serde ──────────────────────────────────────────
        // Full envelope path (parse above + hop append + serialize below),
        // zero Val construction, zero wasm. enter==exit so the hop records a
        // zero-width residence — per-stage/per-replica telemetry stays alive
        // but contributes ~nothing, as a host-native identity stage should.
        if native_mode == Some("serde") {
            let enter_ts = now_secs_f64();
            let exit_ts  = enter_ts;
            let ser_t0 = Instant::now();
            let out = match envelope.encode_identity(&hop_label, enter_ts, exit_ts) {
                Ok(b) => b,
                Err(e) => {
                    error!(log, "identity encode failed", err = e);
                    worker_input.send_control(b"");
                    continue;
                }
            };
            serde_ns += ser_t0.elapsed().as_nanos() as u64;
            send_all(&worker_outputs, out, &never_drain);
            invocation_count += 1;
            processed_since_refill += 1;
            record_timing!(total_t0, serde_ns, 0);
            if worker_input.wants_credits() && processed_since_refill >= refill_threshold {
                let total_ns = total_t0.elapsed().as_nanos() as u64;
                let refill_payload = format!(
                    "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                     \"is_leader\":false,\"p99_latency_ms\":0,\
                     \"total_ns\":{},\"serde_ns\":{},\"n_credits\":{}}}",
                    rid_str, stage.name, total_ns, serde_ns, processed_since_refill,
                );
                worker_input.send_control(refill_payload.as_bytes());
                processed_since_refill = 0;
            }
            continue;
        }

        // ── Typed fast path ───────────────────────────────────────────────
        // Never taken by a fused chain (see the `prepare` site above).
        if let Some((tp, st_store, st_hop)) = chain.first_mut().and_then(|c| {
            let StageInstance { typed, store, hop_label, .. } = c;
            typed.as_mut().map(|t| (t, store, &*hop_label))
        }) {
            let mut enter_exit = (0.0_f64, 0.0_f64);
            match tp.call(st_store, &envelope, st_hop, out_format, &mut enter_exit) {
                Ok(out_bytes) => {
                    let (enter_ts, exit_ts) = enter_exit;
                    let latency_us = ((exit_ts - enter_ts) * 1e6).max(0.0) as u64;
                    let prev_us = avg_latency_us.load(Ordering::Relaxed);
                    avg_latency_us.store((prev_us * 3 + latency_us) / 4, Ordering::Relaxed);
                    send_all(&worker_outputs, out_bytes, &never_drain);
                    invocation_count += 1;
                    processed_since_refill += 1;
                    record_timing!(total_t0, serde_ns, latency_us.saturating_mul(1_000));
                    if worker_input.wants_credits()
                        && processed_since_refill >= refill_threshold
                    {
                        let total_ns = total_t0.elapsed().as_nanos() as u64;
                        let refill_payload = format!(
                            "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                             \"is_leader\":false,\"p99_latency_ms\":{},\
                             \"total_ns\":{},\"serde_ns\":{},\"n_credits\":{}}}",
                            rid_str, stage.name, latency_us / 1000,
                            total_ns, serde_ns, processed_since_refill,
                        );
                        worker_input.send_control(refill_payload.as_bytes());
                        processed_since_refill = 0;
                    }
                }
                Err(e) => {
                    error!(log, "typed call failed", err = e);
                    worker_input.send_control(b"");
                }
            }
            continue;
        }

        // ── Dynamic path — and the ONLY path for a fused chain ─────────────
        //
        //   recv → decode ONCE → A.call → post_return(A)
        //                      → B.call → post_return(B)
        //        → encode ONCE, appending one hop per half → send
        //
        // The intermediate value never leaves the host: `A`'s result `Val` is
        // handed straight to `B` as its parameter. Per-edge WIT compatibility
        // (`A.out == B.in`) is validated at CLI parse time, so the structure is
        // acceptable by construction. Nothing is serialized, no queue is
        // touched, no thread is handed off — which is exactly the cost fusion
        // deletes.
        let ev_val = match envelope.input_val(&chain[0].in_fields, &chain[0].param_types[0]) {
            Ok(v) => v,
            Err(e) => {
                error!(log, "event decode failed", err = e);
                worker_input.send_control(b"");
                continue;
            }
        };
        let bench_val = match envelope.bench_val(&chain[0].param_types[1]) {
            Ok(v) => v,
            Err(e) => {
                error!(log, "bench decode failed", err = e);
                worker_input.send_control(b"");
                continue;
            }
        };

        hop_times.clear();
        let mut latency_us: u64 = 0;
        // The value travelling down the chain. `None` after a failed half —
        // which is also how the loop signals that this event must be dropped.
        let mut carry: Option<(Val, Val)> = Some((ev_val, bench_val));
        for half in chain.iter_mut() {
            let Some((ev, bench)) = carry.take() else { break };
            match half.call(ev, bench) {
                Ok(out) => {
                    hop_times.push((out.enter, out.exit));
                    latency_us += out.latency_us;
                    carry = Some((out.event_val, out.bench_val));
                }
                Err(e) => {
                    error!(log, "process-event call error",
                           stage = half.hop_label.as_str(), err = e);
                    break;
                }
            }
        }
        let Some((ev_val, bench_val)) = carry else {
            worker_input.send_control(b"");
            continue;
        };

        let prev_us = avg_latency_us.load(Ordering::Relaxed);
        avg_latency_us.store((prev_us * 3 + latency_us) / 4, Ordering::Relaxed);

        // Serialization timing starts BEFORE the encode, so the reported
        // serde_us covers parse AND serialize rather than parse alone.
        let serialize_t0 = Instant::now();
        // The LAST half's output layout is what leaves this physical stage.
        let last = chain.last().expect("chain is non-empty");
        let encoded = if chain.len() == 1 {
            let (enter_ts, exit_ts) = hop_times[0];
            envelope.encode_output(
                &ev_val, last.encode_fields(), &bench_val,
                &last.hop_label, enter_ts, exit_ts, out_format,
            )
        } else {
            // One hop per half, adjacent in time: the contracted edge's
            // `inter_stage` gap collapses to ~0 in the existing summary while
            // `per_stage_latency_ms` still attributes time to each half.
            let hops: Vec<(&str, f64, f64)> = chain.iter().zip(hop_times.iter())
                .map(|(h, (e, x))| (h.hop_label.as_str(), *e, *x))
                .collect();
            envelope.encode_output_hops(
                &ev_val, last.encode_fields(), &bench_val, &hops, out_format,
            )
        };
        let final_bytes = match encoded {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(log, "event encode failed", err = e);
                worker_input.send_control(b"");
                continue;
            }
        };

        serde_ns += serialize_t0.elapsed().as_nanos() as u64;

        if !final_bytes.is_empty() {
            send_all(&worker_outputs, final_bytes, &never_drain);
        }

        let total_ns = total_t0.elapsed().as_nanos() as u64;
        invocation_count += 1;
        processed_since_refill += 1;
        // Wasm cost is summed from the per-half hop windows rather than from
        // `latency_us`, which the wasm layer already floored to whole
        // microseconds — at these payload sizes a call is often sub-µs, and
        // flooring it to 0 is precisely what would make a compute-bound stage
        // look edge-bound to the controller.
        let wasm_ns: u64 = hop_times.iter()
            .map(|(enter, exit)| ((exit - enter) * 1e9).max(0.0) as u64)
            .sum();
        record_timing!(total_t0, serde_ns, wasm_ns);

        // Send a refill once we've processed at least `refill_threshold`
        // events since the last one. With larger windows the refill batches credits and
        // metrics together — the metrics_payload for the autoscaler
        // arrives less often but the latest sample is always carried.
        if worker_input.wants_credits() && processed_since_refill >= refill_threshold {
            let latency_ms = latency_us / 1000;
            let refill_payload = if !sent_boot_refill {
                sent_boot_refill = true;
                format!(
                    "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                     \"cold_start_ms\":{:.5},\"spawn_ts\":{:.6},\
                     \"spawn_to_thread_ms\":{:.5},\
                     \"ph_ctx_ms\":{:.5},\
                     \"ph_outputs_ms\":{:.5},\
                     \"ph_dealer_socket_ms\":{:.5},\"ph_dealer_setopt_ms\":{:.5},\
                     \"ph_dealer_connect_ms\":{:.5},\
                     \"ph_pre_inst_ms\":{:.5},\
                     \"ph_instantiate_ms\":{:.5},\"ph_export_ms\":{:.5},\
                     \"ph_tail_ms\":{:.5},\
                     \"is_leader\":false,\"p99_latency_ms\":{},\
                     \"total_ns\":{},\"serde_ns\":{},\"n_credits\":{}}}",
                    rid_str, stage.name, cold_start_ms, spawn_ts,
                    spawn_to_thread_ms,
                    phase_ctx_ms,
                    phase_outputs_ms,
                    phase_dealer_socket_ms, phase_dealer_setopt_ms,
                    phase_dealer_connect_ms,
                    phase_pre_inst_ms,
                    phase_instantiate_ms, phase_export_ms,
                    phase_tail_ms,
                    latency_ms, total_ns, serde_ns, processed_since_refill,
                )
            } else {
                // Boot-phase fields are static per worker and were already
                // delivered; the steady-state refill carries only what the
                // dispatcher (n_credits) and autoscaler (timing samples)
                // actually consume per tick.
                format!(
                    "{{\"_ctrl\":\"refill\",\"rid\":\"{}\",\"fn\":\"{}\",\
                     \"is_leader\":false,\"p99_latency_ms\":{},\
                     \"total_ns\":{},\"serde_ns\":{},\"n_credits\":{}}}",
                    rid_str, stage.name,
                    latency_ms, total_ns, serde_ns, processed_since_refill,
                )
            };
            worker_input.send_control(refill_payload.as_bytes());
            processed_since_refill = 0;
        }
    }

    // Publish whatever the last window collected. A drain retires the replica
    // mid-window, and during a morph that partial window is the most
    // interesting one there is — it covers the moment the controller is trying
    // to reason about.
    timing_acc.flush_into(&timing);

    // EOS barrier: if the stage is finishing and this was the last live
    // replica, forward the marker — every sibling has already drained and
    // exited, so the marker is genuinely last for the whole stage.
    if let Some(marker) = barrier.worker_finished() {
        info!(log, "stage drained; forwarding EOS downstream", stage = stage.name);
        send_all(&worker_outputs, marker, &never_drain);
        worker_input.send_control(b"");
    }

    info!(log, "worker drained", rid = &rid_str[..8], invocations = invocation_count);
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}