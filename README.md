<div align="center">

<img src="assets/logo.png" alt="Epico" width="220"/>

</div>

# Epico

Epico is a broker-free stream-processing runtime where each transform stage is
a WebAssembly component and the host stays generic.

The current codebase is a research prototype for single-node stream processing:
one Rust agent, generated WASI Preview 2 stage components, ZeroMQ dispatchers
between stages, optional in-process source/sink nodes, and per-stage autoscaling
from queue-depth signals.

The README reflects the current repository state. Some older docs and comments
may still mention the legacy all-optional `event` record path.

## What It Is

Epico lets you describe a pipeline in YAML:

- record types under `types:`
- stages with `in:`, `out:`, and `src:`
- linear edges between stages
- scaling knobs per stage
- optional native source and sink Rust files
- runtime knobs such as ingress, placement, resource sampling, credit windows,
  and batching

The CLI turns that YAML into:

- generated per-stage Rust crates under `target/epico/stages/`
- per-stage WIT files
- `wasm32-wasip2` component binaries
- a generated `runtime.yaml` consumed by the agent
- optionally, a generated per-pipeline native agent when `source:` or `sink:`
  uses Rust source files

At runtime, the agent dynamically loads each component, introspects its
`process-event` function, converts JSON events into Wasmtime component values,
calls the stage, converts the result back to JSON, and forwards it downstream.

## Repository Layout

```text
epico/
├── epico-sdk/              # stage! macro and shared WIT
├── epico-cli/              # build, run, validate, clean, bootstrap
├── epico-core/
│   ├── master/             # agent, workers, autoscaler, collector
│   ├── dispatcher/         # ZeroMQ worker-pull dispatcher
│   └── logger/             # JSONL logging
├── epico-loadgen/          # external synthetic source
├── examples/
│   └── three-stage-test/   # current end-to-end example
└── analysis/               # run-summary analysis helpers
```

## Quickstart

```bash
./setup.sh
cd examples/three-stage-test
epico run --project-root ../..
```

The first run builds the runtime and stage components. Later runs can reuse
existing Wasm artifacts when only launch-time knobs changed:

```bash
epico run --project-root ../.. --no-build
```

Run summaries are written to the selected log directory as
`master_<timestamp>_summary.json`.

## Pipeline YAML

The current example is `examples/three-stage-test/pipeline.yaml`.

```yaml
package: epico:temps@1.0.0

nodes:
  - name: local
    host: 127.0.0.1
    force_tcp: false

types:
  reading:
    sensor_id: string
    value:     f64
    unit:      string?
  enriched:
    sensor_id: string
    value:     f64
    anomaly:   bool?
    severity:  string?

source:
  placement: local
  src: ./stages/source.rs

sink:
  placement: local
  src: ./stages/sink.rs

stages:
  - name: normalize
    placement: local
    in:  reading
    out: reading
    src: ./stages/normalize.rs
    scaling:
      min: 1
      max: 1
      queue_up: 50

  - name: detect
    placement: local
    in:  reading
    out: enriched
    src: ./stages/detect.rs
    scaling:
      min: 1
      max: 1
      queue_up: 50

edges:
  - normalize -> detect

deploy:
  host: localhost
  this_host: local
```

If no `edges:` block is declared, the CLI assumes a linear pipeline in stage
declaration order. Non-linear DAGs are parsed but not yet supported by the
runtime.

## Stage Code

Stages use the SDK macro and compile into WASI component-model modules.

```rust
use epico_sdk::stage;

stage! {
    fn detect(ev: Reading) -> Enriched {
        Enriched {
            sensor_id: ev.sensor_id,
            value: ev.value,
            anomaly: Some(ev.value > 75.0),
            severity: Some("high".to_string()),
        }
    }
}
```

The macro supports two typed shapes:

```rust
fn stage_name(ev: InType) -> OutType
fn stage_name(ev: InType, bench: BenchCtx) -> (OutType, BenchCtx)
```

Legacy all-optional `Event` stage shapes are still supported for older
pipelines.

## Native Source And Sink

A pipeline can use native Rust source and sink files compiled into a generated
agent. This bypasses an external load generator and lets the runtime drive the
source in-process.

Native sources implement `epico_master::EventSource`:

```rust
use epico_master::{EventSource, wall_now};

pub struct Source;

impl Source {
    pub fn new() -> Self {
        Source
    }
}

impl EventSource for Source {
    fn next_event(&mut self) -> Option<Vec<u8>> {
        let now = wall_now();
        serde_json::to_vec(&serde_json::json!({
            "bench_ts_wall": now,
            "bench_ts": now,
            "sensor_id": "sensor-0001",
            "value": 42.0
        })).ok()
    }
}
```

Native sinks implement `epico_master::EventSink`:

```rust
use epico_master::EventSink;

pub struct Sink;

impl Sink {
    pub fn new() -> Self {
        Sink
    }
}

impl EventSink for Sink {
    fn consume(&mut self, event: &[u8]) {
        let _ = event.len();
    }
}
```

For source fan-out, set `EPICO_SOURCE_THREADS`. Override
`EventSource::set_partition(index, total)` when logical event uniqueness
matters.

## CLI

```bash
epico build      --config pipeline.yaml --project-root ../..
epico run        --config pipeline.yaml --project-root ../..
epico validate   --config pipeline.yaml --project-root ../..
epico clean      --project-root ../..
epico bootstrap  --project-root ../..
```

Useful run/build flags:

- `--aot`: precompile stage `.wasm` files to `.cwasm` before running.
- `--jit`: defer `.wasm` to `.cwasm` compilation to first cold start.
- `--no-build`: regenerate runtime config but reuse existing stage Wasm files.
- `--log-dir <path>`: choose where logs and summaries are written.

Useful pipeline deploy knobs:

- `deploy.ingress`: explicit external entry URI when there is no native source.
- `deploy.resource_sample_interval_ms`: set to `0` to disable resource sampling.
- `deploy.credit_window`: in-flight event credits per worker.
- `deploy.batch_events`: events packed into one dispatcher-to-worker message.

## Runtime Model

The generated runtime is currently linear:

```text
source -> dispatch-stage-1 -> worker(s) -> dispatch-stage-2 -> worker(s) -> sink
```

Each stage has:

- one dispatcher process unless in-process edges are enabled
- one autoscaler thread in the agent
- zero or more worker threads
- one Wasmtime `Store` and component instance per worker

The dispatcher uses a worker-pull protocol:

- workers connect with `DEALER`
- dispatchers route through `ROUTER`
- queue depth is exposed through a control socket
- workers return credits as they process events

The autoscaler samples queue depth and applies per-stage `min`, `max`,
`queue_up`, `queue_down`, and cooldown settings.

## Telemetry

The agent writes structured logs and a summary JSON file containing:

- end-to-end latency samples
- ingress wait samples
- per-stage residence time
- queue-depth history
- scaling and cold-start events
- worker timing breakdowns
- optional CPU and RSS samples

The scripts in `analysis/` can compare and inspect run summaries.

## Current Limitations

Epico is not production-ready yet. The important current limitations are:

- The runtime supports linear pipelines only. Fan-out and fan-in are not wired.
- Typed WIT generation exists, but runtime output serialization still needs
  stronger support for different input and output record shapes.
- Component validation checks that stages are valid components exporting
  `epico:pipeline/process`, but full structural record compatibility is still
  a work in progress.
- Events move between stages as JSON bytes, so conversion cost and missing-field
  defaults are part of the runtime model.
- Nested records and list-valued domain fields are not supported by the dynamic
  JSON-to-WIT conversion layer yet.
- Placement metadata exists, but multi-host execution is still experimental.
- Delivery guarantees during scale-down are research-prototype level, not a
  durable at-least-once contract.

## Status

Supported today:

- single-host linear pipelines
- Rust stages compiled to `wasm32-wasip2` components
- typed YAML records for generated stage WIT
- legacy all-optional `Event` stages
- per-stage autoscaling from queue depth
- ZeroMQ dispatcher transport with credit windows and batching
- optional native in-process source and sink
- AOT, deferred-JIT, and startup-JIT component modes
- structured logs and benchmark summaries

Near-term work:

- complete typed output serialization
- implement real structural component signature validation
- clean up stale comments and legacy terminology
- add focused integration tests around `reading -> enriched` typed pipelines
- fan-out/fan-in runtime support
- stronger delivery semantics during draining and scale-down
