# Design note — Fan-out / fan-in: arbitrary DAGs in the YAML

Untracked working note (2026-07-09). Future work #1: full support for declaring and
running arbitrary DAGs, not just linear pipelines.

Companion: [FUSION_SCISSION.md](FUSION_SCISSION.md) builds on this (fusion = runtime
edge contraction on the DAG).

---

## 1. Where linearity lives today (integration inventory)

The good news: **the YAML and the CLI are already DAG-shaped.** `edges:` is parsed as
arbitrary `(from, to)` pairs and validated per-edge (`epico-cli/src/config.rs:584-660`:
known-stage check + per-edge type compatibility `A.out == B.in`). Nothing in the parser
rejects fan-out or fan-in. Linearity is enforced downstream, implicitly:

| Where | Assumption |
|---|---|
| `epico-cli/src/config.rs` (agent-config synthesis) | each stage gets ONE `output` = next stage's dispatcher input; last stage → collector |
| `epico-core/master/src/config.rs` | `PipelineStage.output: String` — single output endpoint |
| `epico-core/master/src/master.rs:283` | in-proc edges built from `config.pipeline.windows(2)` — consecutive *stage order*, ignores `edges` entirely |
| `master.rs:307/320` | ingress = `pipeline.first()`, egress = `pipeline.last()` |
| `master.rs:407` | collector binds the *last* stage's output |
| `epico-core/master/src/worker.rs` | one `WorkerOutput` per worker |
| EOS protocol (`worker.rs`, collector) | ONE marker forwarded hop-by-hop; one replica sees it, forwards, exits |
| `pipeline_validator.rs` | validates *consecutive* stages, not declared edges |

So the work is: make the runtime consume the edge list the CLI already produces.

## 2. YAML surface

Keep `edges:` as the single source of topology; add an optional per-stage fan-out mode:

```yaml
stages:
  - name: normalize
    in:  reading
    out: reading
    fanout: broadcast          # broadcast (default) | route | split
    # route mode only:
    # route_key: sensor_type   # field whose hash picks ONE successor

edges:
  - normalize -> detect        # fan-out: normalize feeds two stages
  - normalize -> archive
  - detect  -> merge           # fan-in: merge consumes from two stages
  - archive -> merge
```

**Fan-out semantics (per producer stage, applies to all its out-edges):**
- `broadcast` — every successor gets a copy. Copying is cheap (`Bytes` is refcounted;
  same buffer, N sends). One input event becomes N logical events.
- `route` — exactly one successor per event, chosen by `hash(route_key) % successors`
  (or an explicit `when:` predicate later). Preserves event count.
- `split` — round-robin across successors (load splitting, same type on all edges).

**Fan-in needs no mode:** a consumer with multiple predecessors is just multiple
producers feeding one input. This is nearly free on every transport we have.
Transport policy: same-node edges use the in-proc queues (spsc/mpmc); the zmq
dispatcher is the cross-node boundary only — so the first two bullets are the
primary paths and zmq matters where a fan-in crosses hosts:
- in-proc `Edge` (mpmc): crossbeam ring is MPMC — multiple producers already safe.
- `SpscMesh` (spsc): needs a global producer-index space sized
  `sum(upstream max_replicas)` instead of one upstream's `max_replicas` (mesh is
  N×M; N grows, contract unchanged). Producer indices from the upstream stages'
  free-lists must map into disjoint ranges of that global space.
- zmq (cross-node edges): dispatcher frontend is a PULL — N upstream PUSH
  connections already fan in.

**Validation (CLI, parse time):** reject cycles (topological sort), require weak
connectivity, require exactly one component; all fan-in predecessors must produce the
consumer's input type (already enforced per-edge today). Multiple sources/sinks are
allowed by the model but Phase 1 can require a single source and single sink set.

## 3. Runtime integration plan

### 3.1 Config plumbing
- `PipelineStage.output: String` → `outputs: Vec<(String /*endpoint*/, ...)>` plus
  `fanout_mode`. CLI synthesizes one endpoint per out-edge (the successor's dispatcher
  input, or the collector URI for terminal stages).
- Master stops using stage *order*: build an adjacency list from `edges`, compute
  `sources = in-degree 0`, `sinks = out-degree 0`; replace `pipeline.first()/last()`
  and `windows(2)` at `master.rs:283/307/320/407`.

### 3.2 Worker output (`worker.rs`)
- `WorkerOutput` → `Vec<WorkerOutput>` + mode:
  - broadcast: `for out in outs { out.send(bytes.clone(), drain) }` — `Bytes::clone`
    is an atomic refcount bump, no copy.
  - route: pick index by key hash; requires the key readable without a full parse →
    put the key hash in the binary envelope header (same requirement as key-affine
    routing in [STATE_MANAGEMENT.md](STATE_MANAGEMENT.md) — do it once, serve both).
  - split: per-worker round-robin counter.

### 3.3 Collector / egress
- Simplest correct model: ALL sink stages push to the same collector URI (PULL fan-in
  is free). e2e accounting per *arrival* — under broadcast one source event legally
  produces K terminal latencies; summary should report per-sink blocks so broadcast
  branches don't blur each other's percentiles.

### 3.4 EOS becomes a real protocol (blocking issue)
Today's single-marker relay is already racy with >1 replica (TODO 1.5) and simply
wrong on a DAG (a fan-in stage would shut down when the FIRST branch finishes).
Fix by moving EOS handling from workers to a counting barrier:

- **Dispatcher-level barrier:** each dispatcher knows `eos_expected = in-degree` of
  its stage (CLI passes it as a flag). It absorbs EOS markers from upstream; when the
  count reaches in-degree AND its queue is empty, it emits ONE EOS to each worker →
  workers drain and exit → dispatcher forwards one EOS per out-edge.
- Broadcast producers emit EOS on every out-edge; route/split producers likewise
  (EOS always goes to ALL successors regardless of mode).
- The collector's `expected_count` accounting must become per-source-branch aware
  (broadcast multiplies counts; route preserves them).
- This barrier also fixes TODO 1.5's "sibling replicas never see EOS" for linear
  pipelines — do the EOS rework once, here.

### 3.5 Validation (`pipeline_validator.rs`)
- Validate the declared edge list (WIT-level `A.out == B.in` per edge), not
  consecutive array positions. Data already available; loop shape changes only.

### 3.6 Telemetry
Mostly survives untouched, because `bench_hops` records the *actual path* each event
took (a fan-in event has one linear hop list; broadcast copies each carry their own
branch's hops):
- `inter_stage` already derives edges from consecutive hop pairs → works for DAGs as-is.
- `per_stage_*` / `per_replica_*` — unchanged.
- `build_scaling_block` / stage_names — unchanged (per-stage, topology-agnostic).
- The `ingress_wait` block assumes `stage_names[0]` is the entry — generalize to the
  source set.

### 3.7 Autoscaler
Unchanged. It is already per-stage and signal-driven (own dispatcher queue depth /
own input-edge occupancy); it does not know or care about topology.

## 4. Phasing

1. **Fan-in only** (cheapest, immediately useful: N sensors pipelines merging into one
   sink): adjacency in master, EOS barrier, validator on edges. No worker changes —
   fan-in is transport-native.
2. **Broadcast fan-out:** `outputs: Vec<_>` + `Bytes::clone` loop; per-sink summary
   blocks; EOS to all successors.
3. **Routed/split fan-out:** key hash in envelope header (shared prerequisite with
   state work), route table in worker.
4. **Multi-source / multi-sink**, non-loadgen sources.

## 5. Open questions

- Broadcast + `tp` throughput accounting: is "events/s" measured at source or at sinks?
  (Recommend: report both; sustained_eps stays sink-side.)
- Does `route` need user predicates (`when: value > 0.5`) or is key-hash enough for the
  paper? Predicates require either host-evaluated expressions or a guest call — start
  with key-hash.
- Per-edge `credit_window` / `batch_events` overrides (currently per-dispatcher, which
  stays 1:1 with stages — probably fine).
- Placement: DAG + multi-host means edges can cross hosts; `deploy.this_host` filtering
  already exists per stage, but cross-host edges need TCP endpoints chosen at synthesis
  time (exists today for linear; keep per-edge).
