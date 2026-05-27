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

    /// Directory for JSONL log files.
    #[arg(long, default_value = "logs")]
    log_dir: String,
}

// ---------------------------------------------------------------------------
// Worker tracker
// ---------------------------------------------------------------------------

struct WorkerInfo {
    metrics:    serde_json::Value,
    /// Credits remaining for this worker — i.e. how many more events
    /// the dispatcher is allowed to send before waiting for a refill.
    /// Stays in sync with the number of times this worker's identity
    /// appears in `ready_queue`: each credit corresponds to one queue
    /// entry, and `drain_dispatch` decrements both atomically.
    credits:    u32,
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
    ]);

    // ── Shutdown flag ─────────────────────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || s.store(true, Ordering::Relaxed)).ok();
    }

    // ── Broker state ──────────────────────────────────────────────────────────
    // Under the credit-window protocol a single worker may legitimately
    // appear multiple times in ready_queue — once per credit it currently
    // holds — and `drain_dispatch` pops one entry per event sent. This
    // keeps the round-robin fairness of the original FIFO protocol while
    // amortising the dispatcher↔worker round-trip across the entire
    // window. With `credit_window: 1` (legacy default) every worker
    // appears at most once at any moment, exactly reproducing the old
    // request/reply behaviour.
    let mut ready_queue:  VecDeque<Vec<u8>>          = VecDeque::with_capacity(1024);
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
        if !event_buffer.is_empty() && !ready_queue.is_empty() {
            drain_dispatch(
                &backend, &mut event_buffer, &mut ready_queue,
                &mut workers, args.dispatch_batch, &log,
            );
        }

        let poll_frontend = !ready_queue.is_empty() || event_buffer.len() < args.max_queue;
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

                // Parse the JSON payload once — we need both the metric
                // fields and the n_credits hint (if any).
                let parsed: Option<serde_json::Value> = if !payload.is_empty() {
                    serde_json::from_slice(payload).ok()
                } else {
                    None
                };

                let worker_count = workers.len();
                let is_new       = !workers.contains_key(&identity);
                let entry        = workers.entry(identity.clone()).or_insert_with(|| WorkerInfo {
                    metrics:    serde_json::Value::Object(Default::default()),
                    credits:    0,
                    dispatched: 0,
                });

                if is_new {
                    log.info("worker connected", &[
                        ("rid",    &String::from_utf8_lossy(&identity).to_string()),
                        ("active", &(worker_count + 1).to_string()),
                    ]);
                    wake_logged = false;
                }

                // How many credits does this message grant? The wire
                // contract is intentionally permissive so the legacy
                // worker — which sends an empty frame on boot/error or
                // a per-event metrics payload with no `n_credits` field
                // — keeps working unchanged: any message without an
                // explicit `n_credits` field grants exactly one credit,
                // matching the strict request/reply semantics.
                let granted: u32 = match &parsed {
                    Some(serde_json::Value::Object(map)) => {
                        match map.get("n_credits").and_then(|v| v.as_u64()) {
                            Some(n) => n.min(u32::MAX as u64) as u32,
                            None    => 1,
                        }
                    }
                    _ => 1,
                };
                if let Some(val) = parsed {
                    entry.metrics = val;
                }
                entry.credits = entry.credits.saturating_add(granted);

                for _ in 0..granted {
                    ready_queue.push_back(identity.clone());
                }
            }
        }

        // ── Ctrl: respond to agent metrics polls ──────────────────────────────
        if items[1].is_readable() {
            if let Ok(_request) = ctrl.recv_bytes(zmq::DONTWAIT) {
                let consumers: Vec<serde_json::Value> =
                    workers.values().map(|w| w.metrics.clone()).collect();
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
            &backend, &mut event_buffer, &mut ready_queue,
            &mut workers, args.dispatch_batch, &log,
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
// drain_dispatch
// ---------------------------------------------------------------------------

fn drain_dispatch(
    backend:      &zmq::Socket,
    event_buffer: &mut VecDeque<Vec<u8>>,
    ready_queue:  &mut VecDeque<Vec<u8>>,
    workers:      &mut HashMap<Vec<u8>, WorkerInfo>,
    batch:        usize,
    log:          &Logger,
) {
    let mut dispatched = 0;

    while dispatched < batch {
        let event = match event_buffer.front() {
            Some(e) => e,
            None    => break,
        };
        let worker_id = match ready_queue.pop_front() {
            Some(id) => id,
            None     => break,
        };

        match backend.send_multipart(
            &[worker_id.as_slice(), &[], event.as_slice()],
            zmq::DONTWAIT,
        ) {
            Ok(()) => {
                event_buffer.pop_front();
                if let Some(w) = workers.get_mut(&worker_id) {
                    // Credits track in-flight events; decrement on send.
                    // The matching increment happens when the worker
                    // sends its next refill (or per-event message in
                    // legacy mode).
                    w.credits = w.credits.saturating_sub(1);
                    w.dispatched += 1;
                }
                dispatched += 1;
            }
            Err(zmq::Error::EHOSTUNREACH) => {
                // Worker disconnected — drop from map, leave event for retry.
                workers.remove(&worker_id);
                // Purge any remaining entries this worker still has in
                // ready_queue. With credit_window=1 there's at most one
                // (we already popped it above, so zero); with larger
                // windows we may have several stale entries to scrub.
                // O(n) over the queue, but disconnects are rare events.
                ready_queue.retain(|id| id != &worker_id);
                log.warn("worker gone", &[
                    ("rid",    &String::from_utf8_lossy(&worker_id).to_string()),
                    ("active", &workers.len().to_string()),
                ]);
            }
            Err(_) => {
                // Transient error — return worker to front and stop this batch.
                ready_queue.push_front(worker_id);
                break;
            }
        }
    }
}