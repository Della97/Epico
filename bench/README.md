# two-stage-min — transport × codec benchmark

A/B (well, 3×2) over two independent axes on the two-stage-min pipeline,
everything else held constant (loadgen source, 1 replica/stage, credit window,
sensors).

## Axes

**TRANSPORT** — the spine that moves events between stages:

| name   | how it's selected                                   | spine |
|--------|-----------------------------------------------------|-------|
| zeromq | (default, no flags)                                 | ZMQ dispatchers: ingress + interior + egress (IPC, + loopback TCP to the collector) |
| mpmc   | `EPICO_INPROC_INGRESS=1`                             | in-process crossbeam **MPMC** ring spine (pump + interior + egress) |
| spsc   | `EPICO_INPROC_INGRESS=1 EPICO_EDGE_IMPL=spsc`        | interior `relay→forward` on the FastFlow **SPSC** mesh; ingress/egress MPMC |

**CODEC** — event encoding on that spine: `json` vs `binary`
(`source_format` + `event_format`).

The loadgen is the source for **all** transports, so latency stays
rate-controllable and the one ingestion socket (loadgen → agent) is identical
across the three — it cancels out of the comparison. The mpmc/spsc arms replace
the agent-side dispatchers with rings.

## Two honest caveats (architecture, not the harness)

1. **"spsc" is SPSC on the interior hop only.** In the current runtime the
   ingress and egress edges are hardcoded MPMC (`master.rs` `Edge::new` at the
   ingress-pump and egress-drain); only stage→stage edges honor
   `EPICO_EDGE_IMPL=spsc`. On this 2-stage pipeline that's the single
   `relay→forward` hop. A literally-100%-SPSC path needs a small runtime change
   (make the pump/collector edges `EdgeOut/InSrc::Mesh`-capable). For a 1-replica
   pipeline SPSC vs MPMC is expected to be ~indistinguishable anyway (1×1 mesh ≈
   a ring) — matches the earlier finding that they tie at low replica counts.

2. **The loadgen ingestion socket is always present.** With an external load
   generator you can't be 100% socket-free; mpmc/spsc remove the *interior +
   egress* sockets/dispatchers but keep the loadgen→agent ingestion socket
   (constant across all arms, so the comparison is still clean). For a literally
   zero-socket ceiling, drive the agent with the in-process generator
   (`EPICO_SOURCE_GEN=1`) — but that has no rate pacing, so it's throughput-only.

## Arms

- **THROUGHPUT** — `profile=tp` + `blast` → max sustained ev/s. `blast` freezes
  `bench_ts`, so latency is **not** reported here.
- **LATENCY** — `profile=steady` at a fixed rate (same for every cell) → valid
  per-event `bench_ts` → e2e p50/p99/p999 + per-stage `serde_us` p50.

Each cell runs `REPS` times; the table shows the **median**.

## Run

```bash
cd examples/two-stage-min
./bench/bench_codec.sh
```

Knobs:

```bash
TRANSPORTS="zeromq mpmc spsc" CODECS="json binary" REPS=3 ./bench/bench_codec.sh
TRANSPORTS="mpmc spsc" CODECS="binary" LAT_RATE=12000 ./bench/bench_codec.sh
EDGE_CAP=1024 SPSC_RING_CAP=1024 ./bench/bench_codec.sh   # bigger in-proc buffers
PIN="taskset -c 0-3" ./bench/bench_codec.sh               # Linux only; drop on macOS
```

## Output

Two tables to stdout — a throughput grid (transport × codec, with binary-vs-json
and transport-vs-zeromq deltas) and a latency table (per transport×codec:
p50/p99/p999/mean + serde + achieved rate) — plus `bench/results/results.csv`
and the raw `master_*_summary.json` per run.

## Reading it

- **zeromq → mpmc** is the big mover: removing the dispatcher sockets on the
  interior + egress hops drops e2e latency a lot and lifts throughput. That gap
  is the case for roadmap item 1 (collapse the spine).
- **mpmc vs spsc** on this topology should be close to a wash (1 replica → 1×1
  mesh). If you want it to differ, bump replicas (`max > 1`) so the mesh becomes
  N×M, or apply the ingress/egress-SPSC runtime change.
- **json vs binary** within each transport is the codec delta; the cleanest
  single number is `relay serde p50`.
- Keep `LAT_RATE` below saturation (the parser flags cells that saturate). The
  in-process arms can sustain far more than the ZMQ arm, so a rate that's clean
  for mpmc/spsc may still saturate zeromq — that's expected and visible in the
  `achieved` column.
