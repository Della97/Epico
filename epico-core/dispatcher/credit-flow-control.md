# Credit-based flow control

Internal design doc for the `credit-flow-control` branch.

## TL;DR

Per-worker throughput in the original protocol was bounded by
the dispatcher↔worker IPC round-trip (~110 µs on M4), capping a
single worker at ~9 kev/s no matter how cheap the WASM stage
was. The new protocol keeps up to N events in flight per worker
("credit window N"). At N=16 single-worker throughput rises to
~30 kev/s — a 3.5× improvement on a 10 µs WASM stage.

The default `credit_window: 1` reproduces the legacy strict
request/reply behaviour and is wire-compatible with workers and
dispatchers built before this change. Production pipelines need
to opt in via `deploy.credit_window:` in `pipeline.yaml`.

## The problem

Each stage is a worker pool fronted by a dispatcher process.
The dispatcher receives events from upstream (via PULL),
distributes them to a pool of workers (via ROUTER), and the
workers send results downstream (via PUSH). Workers and
dispatchers are separate OS processes communicating over ZMQ
DEALER↔ROUTER on IPC sockets.

In the original protocol, the dispatcher and a given worker
exchange one event at a time. The sequence per event was:

```
                 dispatcher                 worker
                     │                         │
                     │ ─── event N ──────────► │
                     │                         │ process_event()  ~10 µs
                     │ ◄─── reply N ────────── │
                     │                         │
                     │ ─── event N+1 ────────► │
                     │                         │ process_event()
                     │ ◄─── reply N+1 ──────── │
```

The "reply" is a JSON metric payload (`{"_ctrl":"metrics", ...}`)
that doubles as both telemetry and a "ready for the next event"
signal — the dispatcher pushes the worker back onto its
ready queue when this message arrives.

The problem is that `event N+1` cannot be sent until `reply N`
is received. The total per-event cost on the dispatcher↔worker
side is therefore:

```
T_per_event = T_send + T_recv_at_worker + T_wasm + T_send_reply + T_recv_at_dispatcher
            = ZMQ_IPC_RTT + T_wasm
            ≈ 110 µs + 10 µs    (tested on macOS laptop)
            ≈ 120 µs
```

Single-worker capacity = `1 / 120 µs ≈ 8.3 kev/s`. The wasm
work is only ~8% of the total — the rest is round-trip. **Even
with a zero-cost WASM stage, a single worker tops out at
roughly 9 kev/s.** Adding workers helps linearly (each is its
own DEALER, the ROUTER addresses them in parallel), but each
individual worker is bottlenecked on the round-trip.

This was visible in early benchmarking: a stage with 9 µs WASM
cost saturated at 8.5 kev/s per worker, whereas the
compute-bound bound (`1 / 9 µs`) suggests it should saturate
above 100 kev/s.

## What credit-based flow control is

Credit-based flow control is the same idea as TCP's sliding
window. The receiver (worker) advertises a "window size" — say
16. The sender (dispatcher) is allowed to send up to 16 events
ahead of acknowledgements. Acknowledgements come back in
batches: the worker tells the dispatcher "I finished K events,
here are K credits back". The window protects the receiver from
being overwhelmed; sending up to N events in flight without
waiting amortises the per-event round-trip cost across the
whole window.

In our implementation:

1. **Worker boots** and sends a `hello` message advertising its
   window N. The dispatcher records that this worker has N credits.
2. **Dispatcher sends events freely**, decrementing the worker's
   credit count on each send. Events go out at line rate, no
   per-event waiting.
3. **Worker processes events** as they arrive. It tracks how
   many it has finished since the last credit replenishment.
4. **Once that count reaches N/2**, the worker sends a `refill`
   message saying "I finished K events, here's K credits back +
   my latest metrics".
5. **Dispatcher increments the worker's credit count by K** and
   resumes sending if it had stalled.

```
                 dispatcher                 worker
                     │                         │
                     │ ◄─── hello, N=16 ─────  │  (boot)
                     │                         │
                     │ ─── event 1 ──────────► │
                     │ ─── event 2 ──────────► │  events flow without waiting
                     │ ─── event 3 ──────────► │  for replies
                     │ ─── event 4 ──────────► │  ...
                     │  ...                    │  (worker is processing
                     │ ─── event 8 ──────────► │   events 1..8 in parallel
                     │                         │   with the dispatcher
                     │                         │   sending 9..)
                     │                         │
                     │ ◄─── refill, K=8 ────── │  (after 8 events)
                     │       ↑ +metrics        │
                     │  +8 credits             │
                     │                         │
                     │ ─── event 9 ──────────► │
                     │ ─── event 10 ─────────► │
                     │  ...                    │
```

At any moment the worker holds up to N events queued at its
DEALER socket, plus one being processed. The dispatcher only
blocks when **all** workers in the pool have run out of credits.

## What this buys you

The amortisation factor is approximately N. With N=16 and a
refill threshold of N/2=8, the dispatcher pays one round-trip
per 8 events instead of per 1 event. Per-event overhead drops
from ~110 µs to ~14 µs, and the per-worker capacity formula
becomes:

```
capacity_per_worker ≈ 1 / (T_amortised + T_wasm)
                    ≈ 1 / (14 µs + 10 µs)   for N=16, 10 µs stage
                    ≈ 42 kev/s
```

We measured 30 kev/s in practice — somewhat below the theoretical
maximum because there's still per-event work on the dispatcher
side (JSON parsing, event_buffer push/pop) that isn't amortised
by the window. But it's a clean 3.5× over the legacy 8.5 kev/s.

## What this costs

Three things get worse, in order of significance:

**1. Backpressure is per-window, not per-event.** In the strict
protocol, if a worker stalls (GC pause, page fault, scheduler
preemption), the dispatcher notices immediately because no
reply comes back. With a window of N, the dispatcher will keep
shovelling events until the window fills, then block. So a
stalled worker sees up to N events queued at its DEALER socket
before the dispatcher applies backpressure. With N=16 and 10 µs
events, that's ~160 µs of queue, which is small enough to be
invisible at our latencies — but at large windows (N=64+) it
could matter.

**2. Loss on disconnect grows with the window.** If a worker
crashes with K events in flight, those K events are dropped by
the dispatcher's existing disconnect handler. Pre-patch K=1;
patched K can be up to N. For an experimental branch this is
acceptable. A production-grade version would buffer recently-
sent events at the dispatcher and replay them on a different
worker after a disconnect (this is what Kafka, Pulsar etc. do).

**3. Metric reporting cadence drops.** Worker metrics
(`total_us`, `serde_us`) used to arrive at the autoscaler one
sample per event. They now arrive once per N/2 events. The
autoscaler smooths over a sliding window so this is fine in
practice, but if you ever see noisy scaling decisions at large
N, this is the first place to look.

## How the wire format changed

The same DEALER↔ROUTER socket carries both events and the
new credit traffic. The encoding is JSON, same as before; one
new field carries the credit count.

**Worker → dispatcher messages:**

| message kind | when sent | wire format |
|---|---|---|
| boot signal (legacy) | worker boots | empty frame |
| metrics (legacy) | after every event | `{"_ctrl":"metrics", ..., "total_us":N, ...}` |
| **hello (new)** | worker boots | `{"_ctrl":"hello", ..., "n_credits":N}` |
| **refill (new)** | every N/2 events | `{"_ctrl":"refill", ..., "total_us":N, ..., "n_credits":K}` |
| empty (error path) | bad JSON or call error | empty frame (unchanged) |

The dispatcher's parsing rule:

```
if payload is empty:                 → grant 1 credit
if payload is JSON with n_credits:   → grant n_credits credits
if payload is JSON without n_credits → grant 1 credit (legacy)
```

This rule is what makes the protocol bidirectionally compatible
at window=1: a legacy worker's empty frames and per-event
metrics both grant 1 credit each, exactly the original semantics.
A patched worker at window=1 sends one refill per event, also
granting 1 credit each. Both wire forms produce identical
behaviour. (The two are NOT compatible at higher windows — if
you mix patched and unpatched binaries with `credit_window > 1`
in the YAML, the patched worker advertises a window of N but
the legacy dispatcher only ever sends one event at a time, so
you get legacy throughput regardless.)

## What stayed the same

- The error path (`process_fn.call` failures, malformed JSON
  events). The worker still sends an empty frame on errors,
  which under the new protocol grants 1 credit — exactly
  compensating for the 1 credit the dispatcher debited on
  send. The credit-balance invariant holds across error events.
- The dispatcher's disconnect handler. Worker disappearance
  still drops the worker from the active map and leaks any
  in-flight events. We added an O(n) scan to scrub stale
  entries from `ready_queue`, which only fires on disconnect.
- The control-channel protocol (autoscaler ↔ dispatcher). The
  autoscaler reads `dispatcher_queue_depth` and per-worker
  metrics over a separate REQ/REP socket; that interface is
  untouched.
- All upstream/downstream socket shapes: loadgen still uses
  PUSH→PULL into the first dispatcher, workers still use PUSH
  to ship results downstream, etc. Only the
  dispatcher↔worker pair switched.

## Files changed

- `epico-core/dispatcher/src/dispatcher.rs` — credit-aware
  ready queue, parses `n_credits` from worker messages, scrubs
  stale entries on disconnect.
- `epico-core/master/src/worker.rs` — `hello` on boot,
  `refill` every N/2 events instead of metrics-per-event.
- `epico-core/master/src/config.rs` — `credit_window` field
  on `DispatcherConfig`, defaults to 1.
- `epico-core/master/src/master.rs` — reads
  `dispatcher.credit_window` and forwards it to the autoscaler.
- `epico-core/master/src/autoscaler.rs` — accepts
  `credit_window`, passes through to `spawn_worker`.
- `epico-core/master/src/supervisor.rs` — passes
  `--credit-window` to the dispatcher subprocess.
- `epico-cli/src/config.rs` — `deploy.credit_window` in raw
  spec and in `PipelineSpec`.
- `epico-cli/src/codegen.rs` — emits `credit_window:` in
  the dispatcher block of `runtime.yaml`.

## How to enable

Add to `pipeline.yaml`:

```yaml
deploy:
  ingress: ipc:///tmp/epico-dispatch-normalize-push
  collector: tcp://localhost:9999
  credit_window: 16   # <-- new
```

If the field is omitted, `credit_window: 1` is used (legacy
strict protocol). The runtime.yaml emitted by the CLI carries
the value through to each dispatcher block:

```yaml
dispatchers:
  - name: dispatch-normalize
    push_port: 9100
    pull_port: 9101
    ctrl_port: 9102
    placement: local
    credit_window: 16   # <-- propagated
    push_uri: ipc:///tmp/epico-dispatch-normalize-push
```

The dispatcher subprocess gets `--credit-window 16` from the
supervisor; workers get the same value as a function argument.

## Measurements

All runs on Apple M4 (10/10 cores), macOS Darwin 26.2, single
host, single-stage pipeline (`normalize` → `detect` →
`finalize`), 1 worker per stage (`min: 1, max: 1`), sampler
disabled (`resource_sample_interval_ms: 0`), 60-second runs.
The first stage carries a calibrated synthetic workload at
~9 µs of WASM compute per event; the other two stages are
trivial.

### Single-worker throughput ceiling

| run | offered | window | sustained (M4) | p50 | p99 | max | notes |
|---|---|---|---|---|---|---|---|
| A | 6 kev/s | 1  | 5,933 ev/s | 0.65 ms | 1.82 ms | 38 ms | strict protocol, sub-saturation |
| B | 6 kev/s | 16 | 5,976 ev/s | 0.36 ms | 0.53 ms | 27 ms | window protocol, same load |
| C | 30 kev/s | 16 | 29,657 ev/s | 0.71 ms | 44.6 ms | 64 ms | window protocol, near saturation |

**Run A vs Run B (same load, different protocol).** With a 6 kev/s
offered rate — well below saturation either way — the credit
protocol roughly halves both p50 (0.65 → 0.36 ms) and p99
(1.82 → 0.53 ms). The improvement is purely from amortising
the round-trip: at low load the window doesn't fill, but the
round-trip cost is still in the per-event critical path under
the strict protocol. Inter-stage transport p99 dropped from
0.24 ms to 0.14 ms across these two runs, which accounts for
most of the e2e improvement.

**Run C (saturation under window protocol).** At 30 kev/s
offered, the patched single-worker pipeline sustained
29,657 ev/s — within 1.2% of offered. Under the strict
protocol the same pipeline saturates at ~8.5 kev/s and the
remaining 21.5 kev/s would queue indefinitely (verified
separately: a 30 kev/s strict run produces a queue ~22 seconds
deep at 60 s). p50 stays under a millisecond at saturation;
p99 grows to 44 ms because the system is operating near its
new knee where queue depth fluctuates.

### Throughput lift summary

Per-worker capacity for a 9 µs WASM stage:

| protocol | per-worker capacity | dominant cost |
|---|---|---|
| strict (window=1) | ~8.5 kev/s | dispatcher↔worker round-trip |
| credit (window=16) | ~30 kev/s | wasm execution + amortised round-trip |
| theoretical compute-bound | ~110 kev/s | wasm execution only |

The 3.5× improvement closes most of the gap to the
compute-bound bound. The remaining gap is per-event
dispatcher work (JSON parse on backend recv,
`event_buffer.pop_front`, ZMQ frame construction) that the
window doesn't amortise — addressing that would require a
batched ROUTER protocol, which is a much bigger change than
the credit window.

### Tail latency caveat

p999 and max in all three runs sit in the 7–60 ms range,
considerably above what the per-event compute would suggest.
We've separately attributed this to macOS scheduler quanta
and `sysinfo` Mach-call jitter; it's an environmental artifact
of running on a desktop OS with the resource sampler enabled
or implicit scheduling pressure. On Linux with appropriate
scheduling priorities, the floor would likely be lower. The
credit-flow change neither helps nor hurts this floor; it
shows up identically across all three runs.

## When to use it

- **Use `credit_window: 16`** when the WASM stage is short
  (sub-100 µs) and you need throughput. The 3.5× lift comes
  from amortising what is a fixed per-event cost; for short
  stages that fixed cost dominates, so the lift is large.
- **Use `credit_window: 1`** when the WASM stage is long
  (multi-millisecond) and per-event backpressure matters. For
  long stages the round-trip cost is a small fraction of total
  per-event time, so the throughput lift is small (`1 / (110 µs
  + 1 ms)` vs `1 / (14 µs + 1 ms)` is only an 8% improvement),
  and the window's weakened backpressure isn't worth that.
- **Don't go above `credit_window: 32`** without measuring. The
  refill threshold is half the window, so larger windows mean
  longer gaps in the metrics stream feeding the autoscaler;
  scaling decisions get noisier.

## Future work

The obvious next step is durability — the disconnect-loss
problem. The dispatcher could maintain a "recently sent"
ring per worker and replay un-acked events to a different
worker after a disconnect. This is what real production stream
processors do. It adds memory cost and complicates the credit
accounting (in-flight ≠ sent, you have to track sent-but-not-acked
separately) but unlocks at-least-once delivery semantics.

A separate optimisation would be batching at the dispatcher
itself: send multiple events in a single ROUTER frame. That
would reduce the per-event ZMQ syscall cost on the dispatcher
side, addressing the gap to the compute-bound bound. Probably
worth ~2× more on top of what the credit window already gives,
at the cost of substantially more complex worker-side framing.