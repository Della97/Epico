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
| Single-host footprint | ~2 100 LOC Rust | JVM + ZK | JVM + ZK | cloud-only |

---

## Quickstart

```bash
./setup.sh                # rustup target + install CLI + bootstrap binaries
cd examples/temps
epico run
```

First run takes ~1–2 min (cold build of the host + stage components). Subsequent runs are near-instant via Cargo incremental.

`Ctrl+C` cleanly tears down dispatchers and writes `master_<ts>_summary.json` with per-event e2e latency percentiles and throughput.

---

## A pipeline in two files

**`pipeline.yaml`** — the DAG, types, and scaling policy:

```yaml
package: epico:temps@1.0.0

types:
  reading:
    sensor_id: string
    value:     f64
  enriched:
    sensor_id: string
    value:     f64
    anomaly:   bool?

stages:
  - name: normalize
    in:  reading
    out: reading
    src: ./stages/normalize.rs
    scaling: { max: 4, queue_up: 50 }

  - name: detect
    in:  reading
    out: enriched
    src: ./stages/detect.rs
    scaling: { max: 8, queue_up: 100 }

deploy:
  collector: tcp://localhost:9999
  port_base: 9100
```

**`stages/normalize.rs`** — the transform:

```rust
use epico_sdk::stage;

stage! {
    fn normalize(ev: Reading) -> Reading {
        Reading { value: ev.value.clamp(-50.0, 150.0), ..ev }
    }
}
```

The `stage!` macro expands into the full `wit_bindgen::generate!` + `Guest` impl + `export!` glue. Field updates use Rust's struct-update syntax.

---

## WebAssembly Component Model, used properly

Epico does not embed WASM as a glorified plugin format. The runtime uses the **Component Model (WASIp2)** with **dynamic WIT-introspected dispatch**: the master inspects each component's exported interface at load time and routes typed records through ZeroMQ without any host-side codegen.

The cost is a known ~12× per-call overhead vs. hardcoded `bindgen` — paid intentionally in exchange for **YAML-configurable pipelines without master recompilation**. One master binary runs any pipeline. See [ARCHITECTURE.md § 6](ARCHITECTURE.md) for the dispatch design.

---

## CLI

| Command | What it does |
|---|---|
| `epico build` | Scaffolds stage crates, compiles them to `.wasm`. Does not launch. |
| `epico run` | `build` + launch. Auto-bootstraps host binaries on first use. |
| `epico validate` | Parses YAML, verifies every referenced `.wasm` exists. |
| `epico clean` | Wipes `target/`. Forces a full rebuild. |
| `epico bootstrap` | Pre-builds master + dispatcher (useful in CI). |

Flags: `--config <path>` (default `./pipeline.yaml`), `--project-root <path>` (auto-detected), `--log-dir <path>` (default `./logs`).

---

## Repository layout

```
epico/
├── epico-sdk/           # `stage!` macro + canonical WIT
├── epico-cli/           # build | run | validate | clean | bootstrap
├── epico-core/
│   ├── master/          # autoscaler + wasm host + dispatcher supervisor + collector
│   ├── dispatcher/      # per-stage ZeroMQ broker
│   └── logger/          # structured JSONL
├── epico-loadgen/       # Rust IoT load generator
└── examples/
    └── temps/           # demo pipeline
```

The full architectural story — ZeroMQ topology, the vote-with-cooldown autoscaler, Wasm execution model, concurrency guarantees — lives in **[ARCHITECTURE.md](ARCHITECTURE.md)**. Author's-eye view of writing stages and tuning scaling knobs in **[DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)**.

---

## Status & roadmap

Actively developed. Core runtime is ~2 100 LOC of Rust.

**Supported today:** linear DAGs · WASIp2 components · per-stage autoscaling · credit-based flow control · structured JSONL telemetry · single-host deployment.

**On the roadmap:** fan-out / fan-in topologies · multi-host deployment · at-least-once delivery during scale-down · end-to-end typed pipeline validation · OpenTelemetry export · Hailo-8L AI Hat+ offload.

---