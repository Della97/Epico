![Alt text](assets/logo.png)

# Epico

A broker-free stream-processing runtime. Each stage of your
pipeline is a WebAssembly component; the runtime autoscales replicas per
stage from 0 to N against a queue-depth SLO, and stages talk over
ZeroMQ. No Redis, no Kafka, no Kubernetes.

You write:
- **one YAML** describing the DAG and scaling policy,
- **one `.rs` file per stage** containing the transformation logic.

`epico run` does the rest: compiles the runtime if needed, compiles
your stages to `wasm32-wasip2`, launches the dispatchers, starts the
master, and terminates the pipeline in a built-in collector that records
per-event e2e latency and writes a run summary on shutdown.

---

## Quickstart

```bash
# One-time setup (or just run ./setup.sh from the repo root — it does all three)
rustup target add wasm32-wasip2
cargo install --path epico-cli        # installs the `epico` CLI
epico bootstrap                       # pre-builds the master + dispatcher

# Run the demo
cd examples/temps
epico run
```

That's it. First run takes a minute or two (cold cargo build for the
master, dispatcher, and the stage components). Subsequent runs are
near-instant — cargo's incremental compilation handles the no-op case.

Press `Ctrl+C` to stop; the master tears down its dispatchers cleanly
and writes `master_<timestamp>_summary.json` to the log directory with
e2e latency percentiles and throughput data.

---

## The CLI subcommands

| Command | What it does |
|---|---|
| `epico build` | Scaffolds stage crates + compiles stages to `.wasm`. Does NOT launch anything. |
| `epico run` | `build`, then launch. Auto-bootstraps the runtime binaries on first use. |
| `epico validate` | Parses the YAML, verifies every referenced `.wasm` exists. Quick sanity check. |
| `epico clean` | Removes the entire `target/` tree — stage wasm, master/dispatcher binaries, cargo caches. Forces a full clean rebuild. |
| `epico bootstrap` | Pre-builds the master + dispatcher binaries (useful in CI). Normally not needed. |

All commands accept `--config <path>` (defaults to `./pipeline.yaml`)
and `--project-root <path>` (auto-detected by walking up from the
config file looking for `epico-sdk/Cargo.toml`). `epico run` also
accepts `--log-dir <path>` (defaults to `./logs`) for where the master
and dispatchers write structured JSONL logs and the run summary.

---

## Writing a pipeline

### Pipeline YAML

```yaml
package: epico:temps@1.0.0

types:                             # Typed records — drive per-stage WIT codegen
  reading:
    sensor_id: string
    value:     f64
  enriched:
    sensor_id: string
    value:     f64
    anomaly:   bool?

stages:                            # The DAG
  - name: normalize
    in:  reading
    out: reading
    src: ./stages/normalize.rs
    scaling:
      max: 4
      queue_up: 50

  - name: detect
    in:  reading
    out: enriched
    src: ./stages/detect.rs
    scaling:
      max: 8
      queue_up: 100

deploy:
  collector: tcp://localhost:9999
  port_base: 9100                  # dispatcher ports auto-allocated from here
```

Only linear DAGs are supported today. Fan-out/fan-in is on the roadmap.

### A stage source file

```rust
use epico_sdk::stage;

stage! {
    fn normalize(ev: Reading) -> Reading {
        Reading {
            value: ev.value.clamp(-50.0, 150.0),
            ..ev
        }
    }
}
```

The `stage!` macro expands into the full `wit_bindgen::generate!` +
`Guest` impl + `export!` glue. Field updates use Rust's struct-update
syntax — whatever you don't override is inherited from the input event.

See `DEVELOPER_GUIDE.md` for the alternate function shape (explicit
bench-ctx access) and the full list of scaling knobs.

---

## Repository layout

```
epico/
├── Cargo.toml                     # Root workspace
├── setup.sh                       # First-time setup helper (rustup + install + bootstrap)
├── epico-sdk/
│   ├── wit/epico.wit            # Canonical WIT (single source of truth)
│   └── src/lib.rs                 # The `stage!` macro
├── epico-cli/                   # `epico build|run|validate|clean|bootstrap`
├── epico-core/
│   ├── master/                    # Node master: autoscaler + wasm host + dispatcher supervisor + built-in collector
│   ├── dispatcher/                # Per-stage ZeroMQ broker
│   └── logger/                    # Shared structured JSONL logger (used by master, dispatcher, loadgen)
├── epico-loadgen/               # Rust IoT load generator (replaces the old Python loadgen)
├── examples/
│   └── temps/                     # Demo pipeline: one YAML + three .rs files
└── deprecated/                    # Previous-generation artifacts (fn_a/b/c, Python loadgen, old dags) — kept for reference
```

For the full architectural story — ZeroMQ topology, the autoscaler's
vote-with-cooldown controller, the Wasm execution model, end-to-end
data flow, concurrency guarantees — see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

For the author's-eye view — writing stages, the `stage!` macro, tuning
scaling knobs — see **[DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)**.

---

## Building without the CLI

If you want to build the runtime by hand (e.g. in CI before running
`epico build` explicitly):

```bash
cargo build --release --workspace
```

That produces `target/release/master` (the master),
`target/release/dispatcher`, and `target/release/epico-loadgen`. The
CLI normally handles this for you the first time `epico run` is
invoked.

---

## Status

Actively developed. The core runtime is ~2100 LOC of Rust across the
master and dispatcher crates; the component-model + dynamic-dispatch
design means one master binary runs any pipeline — no recompilation of
the host when stages or types change. See `ARCHITECTURE.md` § 6 for
the details.