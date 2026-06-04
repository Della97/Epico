//! epico dispatcher — ZeroMQ worker-pull load-balancing broker
//!
//! Architecture:
//!   PULL socket  – producers push events here
//!   ROUTER socket – workers connect (DEALER), send ready+metrics, receive events
//!   REP socket   – agent polls queue depth / consumer metrics
//!
//! Dispatch strategy: worker-pull (inherently least-loaded).
//! Transport: binds both tcp:// and ipc:// on every socket.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use epico_logger::Logger;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "dispatcher")]
struct Args {
    /// Legacy TCP port for the frontend (producer PUSH -> dispatcher PULL).
    /// Only used when `--push-uri` is not set. When both are absent the
    /// dispatcher has no frontend bind address and refuses to start.
    #[arg(long)]
    push_port: Option<u16>,

    /// Explicit frontend bind URI. When set, this is the *only* address
    /// the frontend socket binds to — no auto TCP, no parallel IPC. This
    /// lets the pipeline's `deploy.ingress:` declaration reach the
    /// dispatcher verbatim. Accepts `tcp://host:port` and `ipc:///path`.
    #[arg(long)]
    push_uri: Option<String>,

    #[arg(long)]
    pull_port: u16,

    #[arg(long)]
    ctrl_port: u16,

    #[arg(long, default_value = "dispatcher")]
    name: String,

    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    #[arg(long, default_value_t = 50_000)]
    max_queue: usize,

    #[arg(long)]
    stage: String,

    /// Bind IPC endpoints alongside TCP (for same-machine workers).
    /// Only affects the backend (worker-facing) and ctrl sockets — the
    /// frontend's transport is governed by `--push-uri` / `--push-port`
    /// exclusively, so same-host producers that need IPC should be sent
    /// there explicitly via `--push-uri ipc:///...`.
    #[arg(long, default_value_t = true)]
    enable_ipc: bool,

    /// Poll timeout in ms. 0 = busy-poll (lowest latency, one full core).
    #[arg(long, default_value_t = 0)]
    poll_timeout_ms: i64,

    /// Max events dispatched per loop iteration before re-polling.
    #[arg(long, default_value_t = 256)]
    dispatch_batch: usize,

    /// Credit window per worker. Each worker is allowed up to this many
    /// in-flight events before the dispatcher must wait for a credit
    /// refill. A value of 1 reproduces the legacy strict request/reply
    /// behaviour (one event in flight per worker). Higher values let the
    /// dispatcher keep multiple events queued at the worker, amortising
    /// the round-trip overhead and lifting per-worker throughput at the
    /// cost of weakening per-event backpressure to per-window.
    #[arg(long, default_value_t = 1)]
    credit_window: u32,

    /// Max events packed into a single ROUTER message to a worker. 1 = one
    /// event per message (legacy, byte-identical wire). Larger values amortise
    /// the per-message zmq poll/command overhead (the dominant dispatcher cost
    /// at high rates) across the batch. Credits are still counted in events, so
    /// the in-flight bound (`credit_window`) is unchanged — only the packing
    /// changes. Trades zmq overhead against load-balancing granularity: a batch
    /// commits up to this many events to one worker at once, so very large
    /// values coarsen the least-loaded dispatch.
    #[arg(long, default_value_t = 1)]
    batch_events: usize,

    /// Directory for JSONL log files.
    #[arg(long, default_value = "logs")]
    log_dir: String,
}

// ---------------------------------------------------------------------------
// Worker tracker
// ---------------------------------------------------------------------------

struct WorkerInfo {
    /// Latest refill payload, stored RAW (unparsed). The dispatcher never
    /// parses this on the hot path — only the agent's ctrl poll (~1/s) turns
    /// it into JSON. Keeping it unparsed is what stops per-refill cost from
    /// scaling with the metrics schema and with worker count.
    metrics_raw: Vec<u8>,
    /// Credits remaining for this worker — counted in EVENTS. The dispatcher
    /// may send up to this many more events (across one or more batched
    /// messages) before waiting for a refill. Batching consumes >1 credit per
    /// dispatch turn but the unit is still events, so the credit_window tuning
    /// is unchanged by batch size.
    credits:    u32,
    /// True iff this worker currently has an entry in `ready_workers`. Each
    /// eligible worker appears at most once; the credit count (above) tracks
    /// how many events it can still receive. Replaces the old "one queue entry
    /// per credit" scheme so a dispatch turn can send a whole batch at once.
    in_ready:   bool,
    dispatched: u64,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = Args::parse();

    let log = Logger::new(
        &format!("dispatcher/{}", args.name),
        &args.log_dir,
    )
    .unwrap_or_else(|e| {
        eprintln!("[dispatcher/{}] failed to open log: {e}", args.name);
        std::process::exit(1);
    });

    let ctx = zmq::Context::new();

    // ── Frontend: producers push events here ──────────────────────────────────
    // Resolution order: `--push-uri` wins (single bind, exact URI), otherwise
    // fall back to the legacy behavior of a TCP bind at `--push-port` plus an
    // optional sibling IPC bind. The strict mode is what the pipeline's
    // `deploy.ingress:` declaration flows into — it lets the YAML say "the
    // entry point is this exact URI, nothing more" without the runtime
    // creating additional bind addresses that blur the contract.
    let frontend = ctx.socket(zmq::PULL).expect("frontend socket");
    frontend.set_rcvhwm(args.max_queue as i32).ok();
    frontend.set_rcvbuf(1024 * 1024).ok();
    let frontend_bound = match (&args.push_uri, args.push_port) {
        (Some(uri), _) => {
            frontend.bind(uri).expect("bind frontend push_uri");
            uri.clone()
        }
        (None, Some(port)) => {
            let tcp = format!("tcp://{}:{}", args.bind, port);
            frontend.bind(&tcp).expect("bind frontend tcp");
            if args.enable_ipc {
                frontend
                    .bind(&format!("ipc:///tmp/epico-{}-push", args.name))
                    .expect("bind frontend ipc");
            }
            tcp
        }
        (None, None) => {
            // Supervisor validates this, but we re-check here because the
            // dispatcher can also be invoked by hand from the command line.
            eprintln!(
                "[dispatcher/{}] neither --push-uri nor --push-port was provided; \
                 the frontend has no bind address",
                args.name
            );
            std::process::exit(2);
        }
    };

    // ── Backend: workers connect with DEALER ──────────────────────────────────
    let backend = ctx.socket(zmq::ROUTER).expect("backend socket");
    backend.set_router_mandatory(true).ok();
    backend.set_sndhwm(args.max_queue as i32).ok();
    backend.set_sndbuf(1024 * 1024).ok();
    backend
        .bind(&format!("tcp://{}:{}", args.bind, args.pull_port))
        .expect("bind backend tcp");
    if args.enable_ipc {
        backend
            .bind(&format!("ipc:///tmp/epico-{}-pull", args.name))
            .expect("bind backend ipc");
    }

    // ── Ctrl: agent polls metrics via REQ/REP ─────────────────────────────────
    let ctrl = ctx.socket(zmq::REP).expect("ctrl socket");
    ctrl.bind(&format!("tcp://{}:{}", args.bind, args.ctrl_port))
        .expect("bind ctrl tcp");
    if args.enable_ipc {
        ctrl.bind(&format!("ipc:///tmp/epico-{}-ctrl", args.name))
            .expect("bind ctrl ipc");
    }

    log.info("ready", &[
        ("frontend",        &frontend_bound),
        ("pull_port",       &args.pull_port.to_string()),
        ("ctrl_port",       &args.ctrl_port.to_string()),
        ("ipc",             &args.enable_ipc.to_string()),
        ("poll_timeout_ms", &args.poll_timeout_ms.to_string()),
        ("dispatch_batch",  &args.dispatch_batch.to_string()),
        ("credit_window",   &args.credit_window.to_string()),
        ("batch_events",    &args.batch_events.to_string()),
    ]);

    // ── Shutdown flag ─────────────────────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || s.store(true, Ordering::Relaxed)).ok();
    }

    // ── Broker state ──────────────────────────────────────────────────────────
    // Under the credit-window protocol each eligible worker appears EXACTLY
    // ONCE in ready_workers (guarded by WorkerInfo.in_ready); its credit count
    // says how many events it can still receive. A dispatch turn pops a worker,
    // sends it a batch of up to `batch_events` events (bounded by its credits),
    // and re-enqueues it if credits remain. With credit_window=1 and
    // batch_events=1 this reduces to the legacy one-event-per-turn behaviour.
    let mut ready_workers: VecDeque<Vec<u8>>          = VecDeque::with_capacity(256);
    let mut event_buffer: VecDeque<Vec<u8>>          = VecDeque::with_capacity(args.max_queue);
    let mut workers:      HashMap<Vec<u8>, WorkerInfo> = HashMap::with_capacity(256);
    let mut wake_logged = false;

    // ── Main loop ─────────────────────────────────────────────────────────────
    loop {
        if shutdown.load(Ordering::Relaxed) {
            log.info("shutting down", &[
                ("events_buffered", &event_buffer.len().to_string()),
                ("workers_active",  &workers.len().to_string()),
            ]);
            break;
        }

        // Fast path: dispatch without entering poll when work is already queued.
        if !event_buffer.is_empty() && !ready_workers.is_empty() {
            drain_dispatch(
                &backend, &mut event_buffer, &mut ready_workers,
                &mut workers, args.dispatch_batch, args.batch_events.max(1), &log,
            );
        }

        let poll_frontend = !ready_workers.is_empty() || event_buffer.len() < args.max_queue;
        let mut items = vec![
            backend.as_poll_item(zmq::POLLIN),
            ctrl.as_poll_item(zmq::POLLIN),
        ];
        if poll_frontend {
            items.push(frontend.as_poll_item(zmq::POLLIN));
        }

        if zmq::poll(&mut items, args.poll_timeout_ms).unwrap_or(0) < 0 {
            continue;
        }

        // ── Backend: drain ready signals + metrics ────────────────────────────
        if items[0].is_readable() {
            loop {
                let frames = match backend.recv_multipart(zmq::DONTWAIT) {
                    Ok(f)                   => f,
                    Err(zmq::Error::EAGAIN) => break,
                    Err(_)                  => break,
                };
                if frames.len() < 2 { continue; }

                let identity = frames[0].clone();
                let payload  = if frames.len() >= 3 { &frames[2] } else { &frames[1] };

                // HOT PATH — runs on every refill, and refill frequency scales
                // with worker count. Extract just the credit count with a byte
                // scan; do NOT parse the full payload here. The metrics blob is
                // only needed when the agent polls (~1/s), so we stash the raw
                // bytes and parse them lazily in the ctrl handler below. This is
                // what keeps dispatcher cost flat as replicas are added (it was
                // the source of the negative scaling past ~4 workers).
                //
                // Wire contract is unchanged and still permissive: an empty
                // frame (legacy boot/error) or a payload with no `n_credits`
                // field grants exactly one credit — strict request/reply.
                let granted: u32 = if payload.is_empty() {
                    1
                } else {
                    scan_n_credits(payload).unwrap_or(1)
                };

                let worker_count = workers.len();
                let is_new       = !workers.contains_key(&identity);
                let entry        = workers.entry(identity.clone()).or_insert_with(|| WorkerInfo {
                    metrics_raw: Vec::new(),
                    credits:     0,
                    in_ready:    false,
                    dispatched:  0,
                });

                if is_new {
                    log.info("worker connected", &[
                        ("rid",    &String::from_utf8_lossy(&identity).to_string()),
                        ("active", &(worker_count + 1).to_string()),
                    ]);
                    wake_logged = false;
                }

                // Stash the raw payload (last-write-wins, same as before) for the
                // agent to parse on its next poll. Reuse the buffer to avoid a
                // fresh allocation per refill.
                if !payload.is_empty() {
                    entry.metrics_raw.clear();
                    entry.metrics_raw.extend_from_slice(payload);
                }
                entry.credits = entry.credits.saturating_add(granted);

                // Enqueue the worker once if it now has credits and isn't
                // already queued. (Credits are in events; a dispatch turn sends
                // a whole batch and re-enqueues if any remain — see drain_dispatch.)
                if entry.credits > 0 && !entry.in_ready {
                    entry.in_ready = true;
                    ready_workers.push_back(identity.clone());
                }
            }
        }

        // ── Ctrl: respond to agent metrics polls ──────────────────────────────
        if items[1].is_readable() {
            if let Ok(_request) = ctrl.recv_bytes(zmq::DONTWAIT) {
                // Cold path (~1/s): parse each worker's stashed raw metrics
                // exactly once, here, instead of on every refill.
                let consumers: Vec<serde_json::Value> = workers.values()
                    .map(|w| {
                        if w.metrics_raw.is_empty() {
                            serde_json::Value::Object(Default::default())
                        } else {
                            serde_json::from_slice(&w.metrics_raw)
                                .unwrap_or_else(|_| serde_json::Value::Object(Default::default()))
                        }
                    })
                    .collect();
                let resp = serde_json::json!({
                    "_ctrl":                    "metrics_response",
                    "dispatcher_queue_depth":   event_buffer.len(),
                    "consumers":                consumers,
                });
                let _ = ctrl.send(resp.to_string().as_bytes(), 0);
            }
        }

        // ── Frontend: receive events in one pass ──────────────────────────────
        if poll_frontend && items.len() > 2 && items[2].is_readable() {
            loop {
                match frontend.recv_bytes(zmq::DONTWAIT) {
                    Ok(event) if !event.is_empty() => { event_buffer.push_back(event); }
                    Ok(_)                          => continue,
                    Err(zmq::Error::EAGAIN)        => break,
                    Err(_)                         => break,
                }
            }
        }

        // Post-poll dispatch
        drain_dispatch(
            &backend, &mut event_buffer, &mut ready_workers,
            &mut workers, args.dispatch_batch, args.batch_events.max(1), &log,
        );

        // Scale-from-zero visibility: log once per "dry spell"
        if !event_buffer.is_empty() && workers.is_empty() && !wake_logged {
            log.warn("events queued but no workers yet", &[
                ("queue_depth", &event_buffer.len().to_string()),
            ]);
            wake_logged = true;
        }
    }
}

// ---------------------------------------------------------------------------
// scan_n_credits
// ---------------------------------------------------------------------------

/// Extract the integer value of the `"n_credits"` field from a flat JSON
/// object without a full serde_json parse. Returns None if the field is
/// absent or has no digits. This runs on every refill (the hot path); the
/// refill payload is a flat object, so a substring search plus integer scan
/// is correct and far cheaper than parsing the whole ~20-field blob.
fn scan_n_credits(buf: &[u8]) -> Option<u32> {
    const KEY: &[u8] = b"\"n_credits\"";
    let pos = buf.windows(KEY.len()).position(|w| w == KEY)?;
    let mut i = pos + KEY.len();
    let end = buf.len();
    // skip whitespace and the ':' separator
    while i < end && matches!(buf[i], b' ' | b'\t' | b'\n' | b'\r' | b':') {
        i += 1;
    }
    let mut val: u64 = 0;
    let mut any = false;
    while i < end && buf[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((buf[i] - b'0') as u64);
        i += 1;
        any = true;
    }
    if any {
        Some(val.min(u32::MAX as u64) as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// drain_dispatch
// ---------------------------------------------------------------------------

fn drain_dispatch(
    backend:       &zmq::Socket,
    event_buffer:  &mut VecDeque<Vec<u8>>,
    ready_workers: &mut VecDeque<Vec<u8>>,
    workers:       &mut HashMap<Vec<u8>, WorkerInfo>,
    max_events:    usize,   // per-call cap (dispatch_batch): total events before re-poll
    batch_events:  usize,   // max events packed into one message to a worker
    log:           &Logger,
) {
    let empty: &[u8] = &[];
    let mut dispatched = 0;

    while dispatched < max_events {
        if event_buffer.is_empty() {
            break;
        }
        let worker_id = match ready_workers.pop_front() {
            Some(id) => id,
            None     => break,
        };

        // Popped: no longer queued. Read its credit balance.
        let credits = match workers.get_mut(&worker_id) {
            Some(w) => {
                w.in_ready = false;
                w.credits
            }
            None => continue, // worker vanished between enqueue and now
        };
        if credits == 0 {
            continue; // defensive; only enqueued with credits > 0
        }

        // Events for this turn: bounded by batch size, this worker's credits,
        // what's buffered, and the remaining per-call budget.
        let n = batch_events
            .min(credits as usize)
            .min(event_buffer.len())
            .min(max_events - dispatched);
        if n == 0 {
            // No events available though the worker has credits — put it back
            // and stop (event_buffer is empty).
            if let Some(w) = workers.get_mut(&worker_id) {
                w.in_ready = true;
            }
            ready_workers.push_back(worker_id);
            break;
        }

        // Send [identity, <delimiter>, ev1..evN] as ONE multipart message.
        // Borrows of worker_id/event_buffer are confined to this block so the
        // mutations below are free to pop and move.
        let send_result = {
            let mut frames: Vec<&[u8]> = Vec::with_capacity(n + 2);
            frames.push(worker_id.as_slice());
            frames.push(empty);
            for i in 0..n {
                frames.push(event_buffer[i].as_slice());
            }
            backend.send_multipart(&frames, zmq::DONTWAIT)
        };

        match send_result {
            Ok(()) => {
                for _ in 0..n {
                    event_buffer.pop_front();
                }
                let mut requeue = false;
                if let Some(w) = workers.get_mut(&worker_id) {
                    w.credits = w.credits.saturating_sub(n as u32);
                    w.dispatched += n as u64;
                    if w.credits > 0 {
                        w.in_ready = true;
                        requeue = true;
                    }
                }
                if requeue {
                    ready_workers.push_back(worker_id);
                }
                dispatched += n;
            }
            Err(zmq::Error::EHOSTUNREACH) => {
                // Worker disconnected — drop it; leave the events for retry by
                // another worker. It was already removed from ready_workers
                // (we popped it), so no stale entries remain.
                workers.remove(&worker_id);
                log.warn("worker gone", &[
                    ("rid",    &String::from_utf8_lossy(&worker_id).to_string()),
                    ("active", &workers.len().to_string()),
                ]);
            }
            Err(_) => {
                // Transient error — return worker to the front, leave events,
                // and stop this batch.
                if let Some(w) = workers.get_mut(&worker_id) {
                    w.in_ready = true;
                }
                ready_workers.push_front(worker_id);
                break;
            }
        }
    }
}