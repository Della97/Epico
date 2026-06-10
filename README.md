<div align="center">

<img src="assets/logo.png" alt="Epico" width="220"/>
</div>
# Epico

**A broker-free stream-processing runtime where every stage is a WebAssembly component.**

No Kafka. No Redis. No Kubernetes. One binary, one YAML, one `.rs` file per stage.

[![Build](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![Rust](https://img.shields.io/badge/rust-2024-orange)](#)
[![WASI](https://img.shields.io/badge/wasi-p2-purple)](https://github.com/WebAssembly/WASI)

---

## Why Epico

Modern stream-processing stacks bolt application code onto a fleet of brokers, schedulers, and JVMs. Epico inverts the model: **one Rust host, WebAssembly stages, ZeroMQ between them.** The host autoscales replicas per stage against a queue-depth SLO; stages are sandboxed `wasm32-wasip2` components dispatched dynamically — no host rebuild when stages or types change.

This makes Epico a **research vehicle for serverless stream processing on a single node** (Pi-class edge → multi-core server), with the lowest plausible operational surface area: no message broker, no orchestrator, no control plane.

| | Epico | Flink | Kafka Streams | AWS Lambda + SQS |
|---|---|---|---|---|
| Broker required | ❌ | ✅ (Kafka) | ✅ (Kafka) | ✅ (SQS) |
| Per-stage isolation | WASM component | JVM thread | JVM thread | container |
| Dynamic stage dispatch | ✅ (WIT introspection) | ❌ (recompile job) | ❌ | ✅ |
| Scale-to-zero per stage | ✅ | ❌ | ❌ | ✅ |
| Single-host footprint | ~7 400 LOC Rust | JVM + ZK | JVM + ZK | cloud-only |

---

## Quickstart

```bash
./setup.sh                        # rustup target + install CLI + bootstrap binaries
cd examples/three-stage-test
epico run
```

First run takes ~1–2 min (cold build of the host + stage components). Subsequent runs are near-instant via Cargo incremental.

When a `source:` block is present in `pipeline.yaml`, `epico run` also launches the load generator and waits for it to finish before exiting cleanly. On `Ctrl+C` or natural completion, dispatchers are torn down and `master_<ts>_summary.json` is written with per-event e2e latency percentiles, per-stage breakdowns, scaling events, queue-depth history, and resource samples.

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
  ingress:   ipc:///tmp/epico-dispatch-normalize-push
  collector: tcp://localhost:9999
  port_base: 9100
  this_host: local
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

## Architecture

### ZeroMQ topology

Each stage is fronted by a **dispatcher** process. Producers push events to the dispatcher's PULL socket; the dispatcher load-balances across a pool of WASM workers over a ROUTER↔DEALER socket pair; workers push results to the next dispatcher's PULL socket. The final stage delivers to a collector PULL socket in the master.

Same-host edges use IPC sockets (faster than loopback TCP at high rates). The `ingress:` URI in `deploy:` lets any external producer connect directly to the first dispatcher.

```
loadgen ──PUSH──► [dispatch-normalize] ──ROUTER/DEALER──► workers (wasm)
                                                              │
                                                            PUSH
                                                              ▼
                  [dispatch-detect]    ──ROUTER/DEALER──► workers (wasm)
                                                              │
                                                            PUSH
                                                              ▼
                  [collector]  ◄──────────────────────────── PUSH
```

### Autoscaler

One autoscaler thread per stage ticks every 1 ms. It polls the dispatcher's REP control socket for queue depth, then votes to scale up or down against configurable thresholds (`queue_up`, `queue_down`) with separate up/down cooldowns. Cold-start latency is tracked per replica and recorded in the summary JSON.

### WebAssembly Component Model

Epico uses the **Component Model (WASIp2)** with **dynamic WIT-introspected dispatch**: at startup the master inspects each component's exported `epico:pipeline/process` interface and validates type compatibility between consecutive stages. Events are serialized to JSON at each stage boundary, deserialized into WIT record types inside the component, and re-serialized on output — no host rebuild when stages or record schemas change.

The component is loaded via one of three compilation modes (see below). After loading, a single shared `InstancePre` is reused across all replicas of a stage, so replica spawn cost is thread-spawn + Wasmtime instantiation only.

### Compilation modes

| Mode | Flag | When `.wasm` → `.cwasm` | Cold-start includes |
|---|---|---|---|
| startup-JIT (default) | _(none)_ | agent startup | instantiation only |
| AOT | `--aot` | `epico build` / `epico run` | instantiation only |
| cold-start JIT | `--jit` | first replica spawn | Cranelift compile time |

AOT yields the fastest and most reproducible cold-start numbers. Startup-JIT is the default for simplicity. Cold-start JIT is useful when you want to measure compile time as part of the cold-start budget.

### Credit-based flow control

By default the dispatcher and each worker exchange one event at a time (strict request/reply). Set `credit_window: 16` in `deploy:` to enable a sliding-window protocol where the dispatcher keeps up to N events in flight per worker. At N=16 with a ~10 µs WASM stage, per-worker throughput rises from ~8.5 kev/s to ~30 kev/s — a **3.5× improvement** — by amortising the ~110 µs IPC round-trip across the window.

```yaml
deploy:
  credit_window: 16   # default: 1 (strict)
```

Use `credit_window: 1` when per-event backpressure matters more than throughput (long-running WASM stages, multi-millisecond processing). Avoid values above 32 without measuring: larger windows widen the metric reporting gap and can make autoscaler decisions noisier.

---

## CLI

| Command | What it does |
|---|---|
| `epico build` | Scaffolds stage crates, compiles them to `.wasm`. Does not launch. |
| `epico run` | `build` + launch + optional source. Auto-bootstraps host binaries on first use. |
| `epico validate` | Parses YAML, checks every stage exports `epico:pipeline/process`. |
| `epico clean` | Wipes `target/`. Forces a full rebuild. |
| `epico bootstrap` | Pre-builds master + dispatcher (useful in CI). |

**Common flags** (on `build` and `run`)

| Flag | Effect |
|---|---|
| `-c / --config <path>` | Pipeline YAML (default: `./pipeline.yaml`) |
| `--project-root <path>` | Override auto-detected project root |
| `--log-dir <path>` | Log directory (default: `./logs`) |
| `--aot` | AOT-precompile `.wasm` → `.cwasm` at build time |
| `--jit` | Defer compilation to cold-start time |

`epico bootstrap` additionally accepts `--cold-start-opt` to compile the agent with Wasmtime pooling allocator, CoW memory initialisation, Cranelift Speed optimisation level, and parallel stage compilation — useful when measuring cold-start latency.

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

In `tp` mode the loadgen emits an end-of-stream marker after all events are sent; the collector forwards it through the pipeline and triggers a clean agent shutdown so `epico run` exits automatically.

---

## Repository layout

```
epico/
├── epico-sdk/           # `stage!` macro + canonical WIT definition
├── epico-cli/           # build | run | validate | clean | bootstrap
├── epico-core/
│   ├── master/          # autoscaler + wasm host + dispatcher supervisor + collector
│   │                    #   autoscaler.rs  — per-stage scale-up/down loop (1 ms tick)
│   │                    #   worker.rs      — wasm worker thread (IPC recv → wasm → push)
│   │                    #   pipeline_validator.rs — component interface validation
│   │                    #   resources.rs   — CPU + RSS sampler thread
│   ├── dispatcher/      # per-stage ZeroMQ PULL→ROUTER broker + credit flow control
│   └── logger/          # structured JSONL writer
├── epico-loadgen/       # IoT sensor load generator (steady/ramp/burst/wave/tp)
└── examples/
    └── three-stage-test/  # normalize → detect → finalize demo pipeline
```

---

## Telemetry

On shutdown the master writes `master_<ts>_summary.json` containing:

- **e2e latency** — full distribution (p50, p99, max) and per-second time series
- **ingress wait** — time each event spent queued in the first dispatcher before processing began
- **per-stage latency** — residence time distribution for every stage
- **scaling events** — each scale-up/scale-down/cold-start with replica count and compile/instantiate timing
- **queue-depth history** — sampled dispatcher queue depth per stage over time
- **worker timing breakdown** — per-stage total iteration time and serialization time (µs)
- **resource samples** — agent process CPU % and RSS over time (disable with `resource_sample_interval_ms: 0`)

---

## Status & roadmap

Actively developed. Core runtime is ~7 400 LOC of Rust.

**Supported today:** linear DAGs · WASIp2 components · per-stage autoscaling · credit-based flow control (sliding-window dispatcher↔worker protocol) · AOT / JIT / startup-JIT compilation modes · pipeline component validation · structured JSONL telemetry + summary JSON · declarative load-generator source · single-host deployment.

**On the roadmap:** fan-out / fan-in topologies · multi-host deployment · at-least-once delivery during scale-down (dispatcher-side replay buffer) · full wasmparser-based inter-stage type validation · OpenTelemetry export · Hailo-8L AI Hat+ offload.

---