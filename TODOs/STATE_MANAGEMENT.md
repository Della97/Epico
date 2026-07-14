# Design note — Stateful stages with dynamic replica counts

Untracked working note (2026-07-09, extended 2026-07-13 with the replica-churn
protocol, WIT-declared state, and distributed-readiness). Goal: let a stage's replicas
share state while the autoscaler grows/shrinks the replica pool, **prioritizing
performance**, designed so cross-node state remains possible later without redesign.

## Design principle (added 2026-07-13)

**State belongs to shards; shards belong to the LOGICAL stage; a shard map
(`shard → owner`, explicit and mutable) is a first-class runtime object.** Never let
state belong to a worker.

- Fixed shard count S at deploy (e.g. 8× max_replicas); `owner[shard] = replica_idx`.
- Replica churn is trivial by construction: scale-up/down = reassign shards;
  scale-to-zero = shards sit unowned in stage-owned host memory (nothing lost), next
  cold start takes ownership.
- Distributed-later costs nothing now: `owner` becomes `(node, replica)` and "move"
  changes from pointer flip to byte transfer — the protocol (quiesce → flip → resume)
  is identical.
- **The invariant that scales: exactly one writer per shard, ever.** This is why NOT
  a shared concurrent map even though it's easy on one node — cross-replica shared
  mutable access is exactly what does not generalize across nodes.

## Replica-churn handoff protocol (mostly already built)

- **Scale-down:** the TODO-1.6 consumer-close protocol IS the quiesce point: close
  column → drain residue → [release my shards to survivors] → done. Per-ring FIFO
  guarantees every event routed to the drained worker was processed before the flip.
- **Scale-up:** in-band handoff markers (same trick as EOS deferral): flip ROUTING for
  the moving shard first, push a marker through the old owner's queue; when the old
  owner processes it, flip OWNERSHIP. New owner waits/buffers that shard's events
  until the marker lands (µs).
- Ownership keys off replica_idx; the 1.1 free-list already guarantees index
  uniqueness with reclaim-only-after-exit.
- **The one new build item: key-affine routing** (`hash(key) → shard → owner` instead
  of round-robin in mesh push / dispatcher) — shared prerequisite with DAG `route`
  mode and fusion. Key hash goes in the binary envelope header.

## API: declare state in WIT (Mealy-machine stages)

```yaml
types:
  counter_state: { count: u64, sum: f64 }
stages:
  - name: aggregate
    in: reading
    out: enriched
    state: counter_state
    state_key: sensor_id
```

WIT: `process-event(ev, state: counter-state, bench) -> (out, counter-state, bench)`.
Host looks up the key's shard state pre-call, stores the returned value post-call.
Wins: ZERO extra boundary crossings per event; typed dispatch applies to state like
any record; **state is a WIT record → serializable by construction** via the existing
binary envelope codec — snapshots, cross-node migration, and debug dumps are all
"encode the record", no new serialization design ever.

## Distributed (MAYBE — design for, don't build)

| Pattern | Single node, churn | Cross-node later |
|---|---|---|
| Keyed (main case) | shard map + affine routing + handoff above | map spans nodes; dispatcher (cross-node transport per policy) routes by shard; move = envelope bytes |
| Read-mostly (ArcSwap) | unchanged by churn | versioned snapshot broadcast per node |
| Global aggregate | per-replica accumulate, merge at collector | per-NODE accumulate, merge at collector |

Fault tolerance (shard replication / changelog à la Flink) is the genuinely hard
distributed problem — parked; single-writer shards keep it cleanly bolt-on-able.

**Build order:** (1) key hash in envelope header + affine routing → (2) shard map +
stage-owned store + WIT-state signature → (3) handoff on the existing drain/close
hooks → (4) STOP until multi-host lands. 1–3 alone are a paper result (stateful
autoscaling with zero-loss rescaling).

## Core insight

All replicas of a stage are **threads in the same host process** — WASM is the isolation
boundary, not the OS process. So shared state never needs a socket, a broker, or an
external store (no Redis — rejected: ~100 µs+ per access vs ns in-process, and it breaks
the broker-free thesis). State lives in **host memory, owned by the stage** (the
autoscaler already outlives every worker, same lifetime pattern as `instance_pre`), and
replicas reach it through **host functions imported by the component** (a small
`epico:pipeline/state` WIT interface). Guests can't share linear memory across Stores, so
the host-call boundary is the mechanism; in Wasmtime that's tens of ns per call —
viable if the API is coarse-grained.

Replica churn becomes a non-event: state outlives workers because workers never own it.

## Pick the discipline by access pattern (not by replica count)

### 1. Read-mostly state (reference tables, thresholds, models) → RCU / `ArcSwap`
- Stage holds `Arc<ArcSwap<StateSnapshot>>`; workers clone the `Arc` at spawn.
- Reads = one atomic pointer load. Zero contention, scales to any replica count.
- Writes build a new snapshot and swap (copy-on-write).

### 2. Keyed state (per-sensor aggregates, windows) → **key-partitioned ownership**
The high-performance answer. NOT a shared map.

- Hash key → fixed shard count (e.g. 4–8× `max_replicas`, decided once).
- **Key-affine routing:** an event for shard *s* always goes to the replica that
  currently owns *s* → each shard touched by exactly one thread → **zero
  synchronization on the hot path**. (Flink/Kafka-Streams model.)
- **Rescale = pointer handoff, not data movement.** Shards live in host memory, so
  when the autoscaler spawns/drains a replica, rebalancing is reassigning
  shard→replica entries in a routing table. Distributed systems ship bytes over the
  network here; we flip an owner.
- Ordering during a move: quiesce the shard — stop routing to it, let the old owner
  drain in-flight events (the existing `drain_flag` path is the natural hook), flip
  owner. Microseconds of pause per moved shard.

**Costs / trade-offs:**
- Routing changes: dispatcher worker-pull and `SpscMesh::push` round-robin must become
  `shard(key) → replica`. Producer side needs the key without parsing the payload —
  put the key hash in the binary envelope header.
- **Hot-key skew** replaces least-loaded balance: one hot sensor pins one replica.
  Mitigate with shards ≫ replicas. Fundamental trade of affinity vs. current
  least-loaded dispatch.
- Autoscaler semantics shift: one hot shard can't be fixed by adding replicas —
  queue-depth signal no longer implies "more replicas helps".

### 3. Global mutable aggregates (one counter across all keys) → accumulate-and-merge
- Per-replica unsynchronized accumulators, periodically merged by a single reader
  (autoscaler tick or collector) — same pattern telemetry already uses.
- Only if the guest must READ an up-to-date global value: atomics or a sharded
  concurrent map (dashmap-style); accept contention as the price of that semantic.

## Ruled out

- **External store (Redis etc.)** — breaks broker-free design, ~100 µs+ per access.
- **Single `Mutex<HashMap>`** — serializes the stage; adding replicas makes it *worse*
  → poisons the scaling plots.

## Epico-specific gotchas

- **WIT API granularity:** component-model calls lift/lower strings — a chatty
  `get(key: string)` per event can cost more than the state access. Prefer `u64` key
  hashes, whole-record get/put, batch ops. Best case: host looks up the state slice
  pre-call and passes it INTO `process-event` alongside the event, applies mutations
  from the returned value → zero extra boundary crossings per event.
- **Prerequisite: fix TODO 1.1 (replica-index reuse).** With owned shards, an index
  collision means two threads own the same state — upgrades from "corrupted
  telemetry" to "corrupted data".
- **Durability is a separate axis:** host-owned state dies with the process. If ever
  needed: periodic per-shard snapshots to disk are easy *because* shards are
  single-owner (no consistent-cut problem within a shard). Not worth building now for
  a single-node research vehicle.

## TL;DR

State lives in the host. Keyed state → shard-partitioned ownership + key-affine
routing + pointer-handoff rebalancing on scale events. Read-mostly → `ArcSwap`.
Global aggregates → per-replica accumulate-and-merge. Hot path stays
synchronization-free at any replica count — the only way state won't show up in the
scaling curves.
