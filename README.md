<div align="center">

<img src="assets/logo.png" alt="Epico" width="220"/>
</div>

# Epico

**A broker-free stream-processing runtime where every stage is a WebAssembly component.**

No Kafka. No Redis. No Kubernetes. One binary, one YAML, one `.rs` file per stage.

[![CI](https://github.com/Della97/Epico/actions/workflows/ci.yml/badge.svg)](https://github.com/Della97/Epico/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-2024-orange)](#)
[![WASI](https://img.shields.io/badge/wasi-p2-purple)](https://github.com/WebAssembly/WASI)

---

## Why Epico

Modern stream-processing stacks bolt application code onto a fleet of brokers, schedulers, and JVMs. Epico inverts the model: **one Rust host, WebAssembly stages, and a data path that only leaves the process when it has to.** The host autoscales replicas per stage against a queue-depth SLO; stages are sandboxed `wasm32-wasip2` components dispatched dynamically — no host rebuild when stages or types change.

This makes Epico a **research vehicle for serverless stream processing on a single node** (Pi-class edge → multi-core server), with the lowest plausible operational surface area: no message broker, no orchestrator, no control plane.

| | Epico | Flink | Kafka Streams | AWS Lambda + SQS |
|---|---|---|---|---|
| Broker required | ❌ | ✅ (Kafka) | ✅ (Kafka) | ✅ (SQS) |
| Per-stage isolation | WASM component | JVM thread | JVM thread | container |
| Dynamic stage dispatch | ✅ (WIT introspection) | ❌ (recompile job) | ❌ | ✅ |
| Scale-to-zero per stage | ✅ | ❌ | ❌ | ✅ |
| Single-host footprint | ~9 100 LOC Rust | JVM + ZK | JVM + ZK | cloud-only |

---

## Quickstart

```bash
./setup.sh                        # rustup target + install CLI + bootstrap binaries
cd examples/three-stage-test
epico run
```

First run takes ~1–2 min (cold build of the host + stage components). Subsequent runs are near-instant via Cargo incremental.

When a `source:` block is present in `pipeline.yaml`, `epico run` also launches the load generator and waits for it to finish before exiting cleanly. On `Ctrl+C` or natural completion, dispatchers are torn down and `master_summary.json` is written with per-event e2e latency percentiles, per-stage breakdowns, scaling events, queue-depth history, and resource samples.

---

## A pipeline in one file

**`pipeline.yaml`** — types, stages, topology, scaling, and the event source:

```yaml
package: epico:temps@1.0.0

nodes:
  - name: local
    host: 127.0.0.1

types:
  reading:
    sensor_id: string
    value:     f64
    unit:      string?
  enriched:
    sensor_id: string
    value:     f64
    anomaly:   bool?

stages:
  - name: normalize
    in:  reading
    out: reading
    src: ./stages/normalize.rs
    scaling:
      min: 0
      max: 4
      queue_up: 50
      cooldown_up_s: 0.5
      cooldown_down_s: 5.0

  - name: detect
    in:  reading
    out: enriched
    src: ./stages/detect.rs
    scaling:
      min: 0
      max: 8
      queue_up: 100

edges:
  - normalize -> detect

source:
  kind: loadgen
  profile: steady
  rate: 5000
  duration: 60
  sensors: 100

deploy:
  this_host: local
  ingress: ipc:///tmp/epico-dispatch-normalize-push
  ingress_mode: inprocess     # collapse the ingress socket into a ring
  edge_impl: spsc             # lock-free in-process stage-to-stage edges
  credit_window: 16
  resource_sample_interval_ms: 1000
```

**`stages/normalize.rs`** — the transform:

```rust
use epico_sdk::stage;

stage! {
    fn normalize(ev: Reading) -> Reading {
        Reading {
            value: ev.value.clamp(-50.0, 150.0),
            unit: Some(ev.unit.unwrap_or_else(|| "celsius".to_string())),
            ..ev
        }
    }
}
```

The `stage!` macro expands into the full `wit_bindgen::generate!` + `Guest` impl + `export!` glue. Both typed (`fn f(ev: InType) -> OutType`) and bench-context-aware (`fn f(ev: InType, bench: BenchCtx) -> (OutType, BenchCtx)`) signatures are supported.

---

## Topologies: DAGs, not just chains

`edges:` is the single source of truth for topology. Stage array order means nothing — it is only used to imply a linear chain when `edges:` is omitted entirely. Everything downstream (edge construction, EOS barriers, type checks) is driven from the edge list, so fan-out, fan-in, cross edges, and unequal-depth branches are all first-class.

```yaml
edges:
  - ingest -> left
  - ingest -> right      # fan-out: out-degree 2
  - left   -> merge
  - right  -> merge      # fan-in:  in-degree 2
```

**Semantics**

| Shape | Behaviour |
|---|---|
| Fan-out (out-degree > 1) | **Broadcast.** Every event is copied to every out-edge. A stage with K successors emits K copies. |
| Fan-in (in-degree > 1) | **One input queue per consumer stage**, shared by all upstreams. The MPMC ring is natively multi-producer; the SPSC mesh gives each upstream stage a disjoint range of producer columns. |
| Multiple sources | Not supported — the runtime binds one entry stage (in-degree 0) to the single `ingress:` URI. |
| Multiple sinks | Supported at the transport level (all out-degree-0 stages share one egress ring), but the collector ends the run on the **first** EOS marker it sees, so a single sink is the safe shape today. |

Because fan-out is a broadcast, event counts are **exact, not approximate**: the number of events reaching the collector is the number of distinct source→sink paths, and the number of times a stage appears across all those paths is `P(v)·D(v)` (paths into it × paths out of it). That identity is what the DAG tests assert — see [Tests](#tests).

### EOS as a control-plane stage barrier

End-of-stream used to be a data-plane event: whichever worker received the marker forwarded it and exited. That breaks outright on fan-in — the first finished branch would shut the run down. It is now a **per-stage barrier** ([`eos.rs`](epico-core/master/src/eos.rs)):

1. A worker that decodes an EOS marker reports it to the stage barrier and keeps processing.
2. The barrier releases only when all `in_degree` upstream markers have arrived. Branch markers are **merged**, not first-wins: `loadgen_sent` and `expected_count` sum, so a fan-in stage forwards a marker describing the whole stage rather than whichever branch arrived first.
3. The autoscaler then transitions the stage to *finishing*: it raises every replica's drain flag and stops spawning.
4. The **last worker out** gets the merged marker back and forwards it through its own output handle — emission always happens on a thread that owns a valid producer slot, and by then no data event exists behind the marker anywhere in the stage.

The merge rule composes through nesting, so on a DAG with 12 source→sink paths the marker arriving at the collector reads `expected_count = 12 × sent` — exactly the number of events the collector should have received.

---

## Architecture

### Two spines

Epico has two interchangeable data paths, selected per pipeline. The default is the ZeroMQ dispatcher spine; the in-process spine is opt-in and is what the DAG and leak tests exercise.

| | `edge_impl: zmq` (default) | `edge_impl: mpmc` | `edge_impl: spsc` |
|---|---|---|---|
| Stage-to-stage hop | dispatcher process, PULL→ROUTER/DEALER | bounded crossbeam ring, shared | FastFlow-style N×M SPSC mesh |
| Load balancing | dispatcher ROUTER fan-out | MPMC pop (work-stealing) | consumer round-robins its incoming rings |
| Backpressure | credit window + refill protocol | ring capacity | ring capacity |
| Kernel crossings per event | 1 round-trip | none | none |
| Contended atomics on the hot path | — | ring head/tail | **none** (one writer, one reader per ring) |

The rationale for the in-process spine ([`inproc.rs`](epico-core/master/src/inproc.rs)): profiling showed the dispatcher thread spending ~66% of its time blocked in zmq's signaler waiting on the credit round-trip, capping a stage at ~130 k ev/s while every worker sat below 20% utilised. All workers are already threads in one master process — WASM is the isolation boundary, not the OS process — so an intra-host edge never needs to cross the kernel at all. The wait path uses spin → yield → short-sleep backoff, so an idle edge does not burn a core.

`ingress_mode: inprocess` does the same for the source hop: the ingress dispatcher is replaced by a pump feeding the entry stage's ring, and the collector drains the sink stages' egress ring in-process instead of binding a PULL socket.

**Policy:** in-node edges use the in-process rings; the ZeroMQ dispatcher is what carries edges that cross nodes.

```
# ZMQ spine
loadgen ──PUSH──► [dispatch-normalize] ──ROUTER/DEALER──► workers (wasm)
                                                              │ PUSH
                  [dispatch-detect]    ──ROUTER/DEALER──► workers (wasm)
                                                              │ PUSH
                  [collector]  ◄──────────────────────────────┘

# in-process spine (ingress_mode: inprocess, edge_impl: spsc|mpmc)
loadgen ──PUSH──► [pump] ──ring──► workers ──ring──► workers ──ring──► [collector]
```

### The SPSC mesh

`edge_impl: spsc` builds one `Swsr` ring per (producer replica, consumer replica) pair — a Rust port of FastFlow's `SWSR_Ptr_Buffer`. No CAS and no shared atomic counter: the producer owns `pwrite`, the consumer owns `pread`, and the only cross-thread synchronisation is a per-slot state byte. Producer *i* only ever touches column *i* and consumer *j* only row *j*, which is what statically upholds the single-writer/single-reader contract.

On a fan-in the producer index space is **global**: upstream *u* owns `[base(u), base(u) + u.max_replicas)`, with `base` accumulating across upstreams. That means a 4-way fan-in over upstreams with different `max` replica counts lays out disjoint ranges of different widths — exercised deliberately by [`examples/dag-nested`](examples/dag-nested).

Column close uses a Dekker-style handshake (`busy` / `closed`, both SeqCst) so a push that started before a close is fully visible to the drain, and a push that starts after it sees `closed` and skips the column. That is what makes scale-down lossless.

### Autoscaler

One autoscaler thread per stage, ticking every 1 ms. The queue-depth signal comes from the stage's input ring length on the in-process spine, or from the dispatcher's REP control socket on the ZMQ spine; it then votes to scale up or down against `queue_up` / `queue_down` with separate up/down cooldowns. Cold-start latency is tracked per replica and recorded in the summary JSON.

Scaling defaults per stage: `min: 0`, `max: 2`, `queue_up: 50`, `queue_down: 0`, `cooldown_up_s: 0.1`, `cooldown_down_s: 5.0`, `p99_ms: 50`.

### WebAssembly Component Model

Epico uses the **Component Model (WASIp2)** with **dynamic WIT-introspected dispatch**: at startup the master inspects each component's exported `epico:pipeline/process` interface and validates it, then checks type compatibility **per declared edge** — not per adjacent array position, which silently validated the wrong pairs on any non-linear topology.

After loading, a single shared `InstancePre` is reused across all replicas of a stage, so replica spawn cost is thread-spawn + Wasmtime instantiation only.

### Compilation modes

| Mode | Flag | When `.wasm` → `.cwasm` | Cold-start includes |
|---|---|---|---|
| startup-JIT (default) | _(none)_ | agent startup | instantiation only |
| AOT | `--aot` | `epico build` / `epico run` | instantiation only |
| cold-start JIT | `--jit` | first replica spawn | Cranelift compile time |

AOT yields the fastest and most reproducible cold-start numbers. Startup-JIT is the default for simplicity. Cold-start JIT is useful when you want to measure compile time as part of the cold-start budget.

### Wire formats

Events cross edges either as JSON or in Epico's binary envelope ([`epico-wire`](epico-wire/src/lib.rs) is the format spec, depended on by both the host and the loadgen so encoder and decoder can never drift). The host **sniffs the format per event** via a magic byte, so the two can coexist on one spine.

`source_format` governs what the producer emits at the ingress; `event_format` governs interior edges. Both default to `binary`. The envelope carries the bench context (wall/monotonic timestamps, sequence, optional FNV-1a key hash) and the per-stage hop chain inline, so telemetry costs no extra parse. EOS markers stay JSON end to end regardless.

### Credit-based flow control (ZMQ spine only)

On the ZMQ spine the dispatcher and each worker exchange one event at a time by default (strict request/reply). `credit_window: 16` enables a sliding window where the dispatcher keeps up to N events in flight per worker. At N=16 with a ~10 µs WASM stage, per-worker throughput rises from ~8.5 kev/s to ~30 kev/s — a **3.5× improvement** — by amortising the ~110 µs IPC round-trip across the window.

Use `credit_window: 1` when per-event backpressure matters more than throughput (long-running WASM stages, multi-millisecond processing). Avoid values above 32 without measuring: larger windows widen the metric reporting gap and can make autoscaler decisions noisier. On the in-process spine this setting is irrelevant — ring capacity *is* the backpressure.

---

## `deploy:` reference

| Key | Default | Meaning |
|---|---|---|
| `this_host` | first node | Which `nodes:` entry this agent instance represents |
| `ingress` | _(required)_ | Entry URI (`tcp://host:port` or `ipc:///path`) any producer connects to |
| `collector` | `tcp://localhost:9999` | Terminal sink URI for out-degree-0 stages |
| `port_base` | `9000` | First port allocated to dispatchers (three consecutive each) |
| `ingress_mode` | `socket` | `socket` \| `inprocess` — collapse the ingress dispatcher into a ring |
| `edge_impl` | `zmq` | `zmq` \| `mpmc` \| `spsc` — stage-to-stage transport |
| `spsc_ring_cap` | `256` | Slots per individual SPSC ring (`edge_impl: spsc`) |
| `source_format` | `binary` | `json` \| `binary` — what the source emits at the ingress |
| `event_format` | `binary` | `json` \| `binary` — encoding on interior edges |
| `credit_window` | `1` | In-flight events per worker (ZMQ spine) |
| `batch_events` | `1` | Events per ROUTER message; orthogonal to `credit_window` |
| `source_threads` | host default | In-process source fan-in width (pump threads) |
| `resource_sample_interval_ms` | `1000` | CPU + RSS sampling cadence; `0` disables the sampler entirely |

Per-node: `force_tcp: false` forces TCP even for same-host edges (same-host defaults to IPC, measurably faster at high rates).

---

## CLI

| Command | What it does |
|---|---|
| `epico build` | Scaffolds stage crates, compiles them to `.wasm`. Does not launch. |
| `epico run` | `build` + launch + optional source. Auto-bootstraps host binaries on first use. |
| `epico validate` | Parses YAML, checks every stage exports `epico:pipeline/process`, type-checks every edge. |
| `epico clean` | Wipes `target/`. Forces a full rebuild. |
| `epico bootstrap` | Pre-builds master + dispatcher (useful in CI). |

**Common flags** (on `build` and `run`)

| Flag | Effect |
|---|---|
| `-c / --config <path>` | Pipeline YAML (default: `./pipeline.yaml`) |
| `--project-root <path>` | Override auto-detected project root |
| `--log-dir <path>` | Parent of the per-run log directories (default: `./logs`) |
| `--aot` | AOT-precompile `.wasm` → `.cwasm` at build time |
| `--jit` | Defer compilation to cold-start time |
| `--no-build` | Skip the stage rebuild; still regenerates `runtime.yaml`, so launch-time knobs (`credit_window`, `batch_events`, ports, scaling caps, sampling) take effect without a wasm recompile. Only safe when stage sources are unchanged. |

`epico bootstrap` additionally accepts `--cold-start-opt` to compile the agent with Wasmtime pooling allocator, CoW memory initialisation, Cranelift Speed optimisation level, and parallel stage compilation — useful when measuring cold-start latency.

### Environment overrides

For ad-hoc experiments without editing the YAML (the config remains the source of truth for reproducible runs):

| Var | Overrides |
|---|---|
| `EPICO_EDGE_IMPL` | `deploy.edge_impl` |
| `EPICO_EDGE_CAP` | In-process ring capacity (default 1024) |
| `EPICO_SPSC_RING_CAP` | `deploy.spsc_ring_cap` |
| `EPICO_INPROC_INGRESS` / `EPICO_INPROC_EDGES` | Force the in-process spine on |
| `EPICO_SOURCE_GEN` | Drive the agent from the in-process generator (no rate pacing; throughput only) |
| `EPICO_SOURCE_THREADS` / `_COUNT` / `_SECONDS` / `_SENSORS` / `_FORMAT` | In-process source knobs |
| `EPICO_EOS_DRAIN_SECS` | Drain grace period after EOS |
| `EPICO_RUN_DIR` | The run's log directory. Set it to make a process join an existing run instead of minting its own folder |
| `EPICO_LOG` | Log verbosity (`debug` \| `info` \| `warn` \| `error`) |

---

## Load generator

`epico-loadgen` is a configurable IoT sensor stream producer. It is launched automatically when `pipeline.yaml` includes a `source: { kind: loadgen, ... }` block, or can be run standalone.

| Profile | Behaviour |
|---|---|
| `steady` | Constant rate for the full duration |
| `ramp` | Linear ramp from `ramp_start` to `rate` over `ramp_time` seconds |
| `burst` | Steady baseline with periodic spikes to `rate` |
| `wave` | Sinusoidal between `wave_min` and `rate` |
| `tp` | Send `count` events as fast as possible, then emit EOS; used for peak-throughput measurement |
| `pulse` | Send `count` events in bursts of `pulse_events` separated by `pulse_idle_s` of silence, then EOS |

`tp` and `pulse` both send **blocking**, so backpressure slows the producer instead of dropping events — that is what makes `loadgen_sent` exact, the property every conservation test depends on. `pulse` additionally chooses idle gaps longer than the stages' `cooldown_down_s`, so every burst re-triggers scale-up from (near) zero and every gap triggers scale-down: one run exercises repeated 0→N→0 replica cycles while the event count stays exactly known.

`--blast` (tp only) reuses one pre-serialized buffer to remove the loadgen's per-event rng + serde cost, so a single producer thread can saturate any in-process transport. Throughput only — `bench_ts` is stamped once, so latency is meaningless under it.

---

## Examples

| Example | Shape | What it is for |
|---|---|---|
| [`two-stage-min`](examples/two-stage-min) | `relay -> forward` | Minimal pipeline; the transport × codec benchmark and scaling-leak test run on it |
| [`three-stage-test`](examples/three-stage-test) | `normalize -> detect -> finalize` | The demo pipeline from the quickstart |
| [`dag-diamond`](examples/dag-diamond) | one fan-out, one fan-in | The smallest pipeline that is **not** a line; 2 paths → 2 × sent |
| [`dag-nested`](examples/dag-nested) | 14 stages, nested fan-out/fan-in | Fan-outs inside fan-out branches, a fan-in of fan-ins, unequal-depth branches at one barrier, a stage feeding two joins, a stage that is both fan-in and fan-out; 12 paths → 12 × sent |

Every stage in the two DAG examples is an identity function on purpose: they exercise **topology**, not compute, so any deviation in the counts is attributable to fan-out / fan-in bookkeeping alone.

---

## Tests

```bash
tests/no_leak_scaling.sh          # conservation across repeated 0→N→0 scale cycles
tests/no_leak_dag.sh              # conservation on the diamond   (2 paths)
tests/no_leak_dag_nested.sh       # conservation on the nested DAG (12 paths)
TRANSPORTS=mpmc tests/no_leak_dag.sh     # single transport instead of spsc + mpmc
```

Each iterates the in-process transports (`spsc`, `mpmc` by default) and asserts **equality in both directions** on the event counts — short means a branch leaked, long means something was duplicated on a path that should not have carried it. An "extra events" bug can't hide behind the expected multiplication.

The DAG scripts recompute the expected per-stage table from `edges:` in the pipeline YAML rather than hardcoding it, so the topology can be edited without the assertions going stale. Sample output:

```
sent=20000  paths=12  expected=240000  received=240000
stage         P   D  expected    actual  ups/downs
alpha         1   4     80000     80000  4/3
...
joinleft      4   2    160000    160000  6/3
fuse          6   2    240000    240000  6/3
terminal     12   1    240000    240000  6/3
PASS: nested DAG conserved exactly on every path
```

`bench/` holds the transport × codec harness (`zeromq | mpmc | spsc` × `json | binary`) — see [bench/README.md](bench/README.md) for the two honest caveats about what "spsc" measures today.

---

## Repository layout

```
epico/
├── epico-sdk/           # `stage!` macro + canonical WIT definition
├── epico-wire/          # binary event-envelope format spec (zero deps; shared by host + loadgen)
├── epico-cli/           # build | run | validate | clean | bootstrap  (+ codegen, config parsing)
├── epico-core/
│   ├── master/          # autoscaler + wasm host + spine + collector
│   │   ├── autoscaler.rs        # per-stage scale-up/down loop (1 ms tick)
│   │   ├── worker.rs            # wasm worker thread (recv → wasm → push)
│   │   ├── eos.rs               # per-stage EOS barrier + fan-in marker merge
│   │   ├── inproc.rs            # MPMC in-process edge
│   │   ├── spsc.rs              # FastFlow-style SWSR ring + N×M mesh
│   │   ├── envelope.rs          # binary ↔ telemetry decoding
│   │   ├── config.rs            # runtime config + topology (edges, in/out-degree, sources, sinks)
│   │   ├── pipeline_validator.rs# per-edge component interface validation
│   │   └── telemetry/           # collector, stats, resources sampler, summary writer
│   ├── dispatcher/      # per-stage ZeroMQ PULL→ROUTER broker + credit flow control
│   └── logger/          # structured JSONL writer
├── epico-loadgen/       # IoT sensor load generator (steady/ramp/burst/wave/tp/pulse)
├── examples/            # two-stage-min · three-stage-test · dag-diamond · dag-nested
├── tests/               # event-conservation test scripts
└── bench/               # transport × codec benchmark harness + analysis scripts
```

---

## Telemetry

Each run gets its own directory — `logs/run_<YYYYMMDD_HHMMSS>/` — holding every component's log, so one run's files sit together instead of interleaving with every other run's in a flat `logs/`:

```
logs/run_20260802_174113/
├── master.jsonl                        # agent: autoscaler, workers, EOS, collector
├── master_summary.json                 # the run summary (below)
├── loadgen.jsonl                       # producer: burst completions, sent/dropped
└── dispatcher-dispatch-relay.jsonl     # one per dispatcher, ZMQ spine only
```

The CLI mints the directory and exports it as `EPICO_RUN_DIR`; child processes inherit it, which is how the dispatchers the agent spawns join the same folder without any argument plumbing. A component launched directly with the variable unset mints its own run directory under `--log-dir` and publishes it to its children.

On shutdown the master writes `master_summary.json` containing:

- **e2e latency** — full distribution (p50, p99, p999, max), CDF, and per-second time series
- **ingress wait** — time each event spent queued at the ingress before processing began
- **per-stage latency and counts** — residence time distribution and event count for every stage
- **per-replica breakdown** — same, keyed by `stage#replica`, so a slow or cold replica is visible
- **scaling events** — each scale-up/scale-down/cold-start with replica count and compile/instantiate timing
- **queue-depth history** — sampled input-queue depth per stage over time (both spines)
- **worker timing breakdown** — per-stage total iteration time and serialization time (µs)
- **resource samples** — agent process CPU % and RSS over time (disable with `resource_sample_interval_ms: 0`)
- **environment** — git commit + dirty flag, rustc and Wasmtime versions, so a summary identifies the build that produced it

`per_stage_count` is derived from the hop chain each event carries, which is exactly why the DAG conservation identity (`P(v)·D(v)·sent`) is checkable from the summary alone.

---

## Status & roadmap

Actively developed. Core runtime is ~9 100 LOC of Rust (code only, excluding examples and tests; `tokei`).

**Supported today:** arbitrary DAGs — broadcast fan-out, fan-in, cross edges, unequal-depth branches · WASIp2 components with per-edge type validation · per-stage autoscaling with scale-to-zero · three interchangeable spines (ZMQ dispatcher / MPMC ring / SPSC mesh) with in-process ingress and egress · control-plane EOS barrier with fan-in marker merge · JSON and binary wire formats, sniffed per event · credit-based flow control on the ZMQ spine · AOT / JIT / startup-JIT compilation modes · native in-process source and sink nodes · structured JSONL telemetry + summary JSON · declarative load-generator source · single-host deployment.

**On the roadmap:** multi-host deployment (ZMQ carrying only cross-node edges) · multiple terminal sinks with an EOS quorum at the collector · at-least-once delivery during scale-down (dispatcher-side replay buffer) · full wasmparser-based structural type extraction at load time (today's per-edge check is syntactic, with structural verification deferred to Wasmtime at worker boot) · key-affine routing on the shard-key hash already in the envelope · OpenTelemetry export · Hailo-8L AI Hat+ offload.

---
