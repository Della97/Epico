# Design note — Runtime stage fusion & scission (a DAG that morphs under SLO)

Untracked working note (2026-07-09). Future work #2: exploit the component model to
fuse two stages into one (A→B becomes AB) or split them back at runtime, keeping the
logical DAG identical while the *physical* topology adapts to SLO pressure — e.g.
"path A→B is about to breach → fuse A and B, eliminating the edge cost".

Depends on: [DAG_SUPPORT.md](DAG_SUPPORT.md) (fusion is edge contraction on the DAG),
[STATE_MANAGEMENT.md](STATE_MANAGEMENT.md) (fused stages must carry both stages'
state), TODO 1.5 (EOS barrier), and the mutable routing tables both of those need.

---

## 1. Why fusion pays

An edge A→B costs, per event: serialize A's output → transport (zmq: ~110 µs IPC
round-trip amortized by credit window; in-proc: ring + backoff) → queue wait in B's
dispatcher → deserialize → Val construction. For µs-scale stages this dominates:
`worker_timing` shows `overhead_us` (total − wasm) is most of the iteration, and the
`inter_stage` block measures the gap directly. Fusion deletes ALL of it for that edge:
A's output feeds B **in the same worker thread, in the same host call chain, with zero
serialization**. On the zmq path it also deletes an entire dispatcher process.

Scission is the inverse trade: fused AB scales as one unit (service time tA+tB); if B
alone becomes hot you want independent replica counts back.

## 2. What "fusion" means with the component model — two options

### Option A — host-level fusion (RECOMMENDED, phase 1)
A fused worker holds **two instances** (A's and B's, from their already-cached
`InstancePre`s — nothing is recompiled) and the host calls them back-to-back:

```
recv → decode once → A.process-event(ev, bench) → (ev', bench')
                   → B.process-event(ev', bench') → (ev'', bench'')
     → encode once → send
```

- **No new compile:** cold start of a fused replica = 2× instantiate (~µs–ms with the
  pooling allocator), same as spawning one replica of each stage today.
- **The intermediate value never leaves the host:** A returns a `Val` (or a typed
  struct on the typed-dispatch path); the edge's WIT compatibility (`A.out == B.in`)
  is already validated, so the host passes it straight into B. Dynamic path: reuse the
  result `Val` as the next call's param. Typed path (`typed.rs`): codegen can register
  a fused `PreparedDispatch` chaining the two concrete types — still zero Val layer.
- **Isolation is preserved:** two Stores (or one Store, two instances) — A and B stay
  sandboxed from each other exactly as before. One epoch deadline arms the whole
  chain (budget = sum of the per-stage budgets).
- Telemetry: the fused worker appends TWO hops (`A#r`, `B#r`) with a zero-width gap,
  so per-stage latencies, per-replica blocks, and `inter_stage` (gap → ~0) all remain
  truthful without any collector changes. The morph is *visible* in the data — which
  is exactly what the paper wants to show.

### Option B — true component composition (research phase 2)
Statically link A.wasm + B.wasm into one component (WAC / `wasm-compose` style: glue
component whose export calls A then B through component-level imports), then
Cranelift-compile the result at runtime. Pros: one instance, direct component-to-
component calls, potentially cross-component inlining. Cons: needs a generated glue
component; pays a full compile (~10 ms+, or an AOT cache keyed by the fused pair);
canonical-ABI lift/lower still happens at the A→B boundary inside the composed
component, so the win over Option A is small while the machinery is much bigger.
**Measure Option A first; do B only if the residual per-call host overhead matters.**

## 3. Legality — which edges may fuse

Fusion is **edge contraction** on the DAG. Contract edge A→B only when:
- `out-degree(A) == 1` and `in-degree(B) == 1` (a linear chain segment). Fusing across
  a fan-out/fan-in would change semantics (broadcast copies, merge ordering) — forbid.
- A and B are placed on the same host (fusion is intra-process by definition).
- State key-domain compatibility — see §6.
- Chains generalize: A→B→C can fuse to ABC by repeated contraction.

## 4. The SLO controller (when to morph)

A small controller thread (or an extension of each autoscaler tick) watches per-edge
and per-stage signals already collected today:

**Fuse A→B when** the edge, not the compute, is the problem:
- B's dispatcher queue depth persistently high while B's `wasm_us` is small, or
- `inter_stage(A→B)` p99 + serde share > α × (tA + tB), and
- the projected fused pool fits: R_ab ≈ λ·(tA+tB) ≈ Ra+Rb replicas, each holding TWO
  component instances → needs 2·(Ra+Rb) pool slots and Ra+Rb cores (both were already
  in use pre-fusion, so cores always fit; the instance pool is the real bound — see
  §5 step 1).

**Split AB when** independent scaling would win:
- AB's queue grows while only one half's `wasm_us` grew (the per-stage hops still
  attribute time to A and B separately — we know WHICH half is hot), and
- R_ab is pinned at max while a split would let hot-half replicas exceed R_ab.

Add hysteresis (separate fuse/split cooldowns, like `cooldown_up_s`/`cooldown_down_s`)
so the DAG doesn't oscillate; morphing is much more expensive than a replica spawn.

### 4.1 Controller architecture (the DAG-health module)

One GLOBAL thread in the master ("dag-controller"), not per-edge logic: morphs consume
shared resources (instance-pool slots, cores) and adjacent morphs conflict, so the
decision is inherently cross-stage. It already has everything in-process: the
telemetry Arc, dispatcher ctrl metrics, replica counts, scaling events.

- **Diagnose before actuating.** "Path A→B about to break" has two causes with
  opposite remedies. overhead/inter-stage-gap dominated → fuse (deletes the cost).
  `wasm_us` dominated at max_replicas → fusion is useless (compute-bound); the remedy
  is replicas, scission of a previously fused pair, or nothing. The per-refill
  metrics already carry the total/serde/wasm split needed to tell these apart.
- **Leading indicators, not breach detection:** queue-depth SLOPE while at
  max_replicas (today's "SLO breach: queue depth at max replicas" warn becomes a
  controller input), credit-starvation frequency, inter-stage p99 trending toward
  the latency budget.
- **Cadence and serialization:** decide at 1–5 s (autoscalers tick at 1 ms; a morph
  is ~10³× a replica spawn), one morph in flight globally, dead band between fuse
  and split thresholds.
- **Failure-avoidance bonus:** fusion removes a failure point outright — dispatch-B
  is a separate busy-polling OS process that ceases to exist. The drain-based
  transition doubles as a recovery action: a wedged dispatcher can be routed around
  by fusing across it with the same protocol.

Build note — what's missing at runtime today is small: (1) the fused worker loop
(chain two prepared calls, two hops, no re-encode between); (2) START/STOP lifecycle
for autoscaler loops (they currently spawn once and run forever — the stage set must
become mutable); (3) LIVE per-edge stats (inter_stage gaps and the wasm/overhead
split exist but are only computed at summary time — the controller needs rolling
windows). No recompilation, no process restart, no config reload is needed anywhere
in the morph path.

## 5. Transition protocol (replica-aware, zero event loss)

Reuse the machinery that already exists: `InstancePre` caches, `drain_flag`, the
1.1 free-list, and the EOS/queue discipline. Fusing A (Ra replicas) with B (Rb):

1. **Prepare:** initial `R_ab = Ra + Rb` — NOT max(Ra, Rb). Replica counts encode
   service times (Little: Ra ≈ λ·tA, Rb ≈ λ·tB); a fused worker holds its thread for
   tA+tB per event, so the busy-server need is λ·(tA+tB) ≈ Ra+Rb. Starting lower
   undershoots capacity at the exact moment the SLO was already under pressure; the
   autoscaler then trims DOWN as the deleted edge overhead shows up (safe direction).
   Total threads are conserved (Ra+Rb workers existed before) and B's busy-polling
   dispatcher core is freed, so the overshoot costs nothing. Create the fused stage's
   index space `0..max_ab` (a fresh free-list) and, on the in-proc path, fresh
   edges/meshes for (pred(A) → AB) and (AB → succ(B)) sized to `max_ab`.

   **Worked example (Ra=10, Rb=2):** start 12 fused replicas. Rb ≪ Ra means tB is
   small, so a large share of B's measured cost was edge overhead that fusion deletes
   — the fused need settles near λ·(tA + wasmB) ≈ 10–11 and the autoscaler trims
   within its normal cooldowns. Pool check (§3 precondition, concrete form): each
   fused replica instantiates BOTH components → 12 replicas = 24 instance slots; the
   pooling allocator was sized at sum(max_replicas)·1.2, so fuse only if
   2·(Ra+Rb) fits — effectively fused max ≈ (maxA+maxB)/2 unless the pool is sized
   for fusion upfront.
2. **Spawn fused replicas:** instantiate A and B from their cached `InstancePre`s in
   each fused worker. They *connect but do not yet receive* (no credits announced /
   not yet registered on the input edge).
3. **Flip routing.** Transport policy: same-node edges run on in-proc queues
   (spsc/mpmc), the dispatcher is for cross-node edges only — and fusion is
   intra-node by definition (§3), so the in-proc path is the PRIMARY mechanism:
   - MPMC `Edge`: trivial — it's Clone and multi-consumer; hand the fused pool
     pred(A)'s input handle, drained A-workers just stop popping.
   - `SpscMesh`: dimension-fixed, so use the new-mesh-and-drain from step 1: fused
     consumers get columns of the new pred(A)→AB mesh; the producer side switches
     meshes on its next push (a mesh-handle swap behind an ArcSwap), and the old
     mesh drains through the old pool before retirement.
   - zmq (applies when the fused pair's INPUT is a cross-node dispatcher, e.g.
     fusing the first two stages behind a remote ingress): nearly free — the
     worker-pull dispatcher is consumer-set agnostic (learns consumers from
     `hello`, balances to whoever holds credits), so fused workers just connect
     and old workers starve out. No restart, rebind, or config reload.
   New events now take the fused path.
4. **Drain the old path in order:** raise `drain_flag` on old A workers → they finish
   in-flight events into B's dispatcher → B's workers drain that queue → raise drain
   on B workers when B's queue hits zero. During the overlap window events flow on
   BOTH paths; that reorders events across paths — acceptable because replicas already
   reorder today (no ordering guarantee exists to preserve). Old indices return to
   their free-lists only on `done` (1.1 discipline), old meshes are dropped when the
   last handle goes.
5. **Retire:** B's dispatcher process is killed (zmq path); telemetry records a
   `morph` ScalingEvent (`fuse`, stages, t_s, old/new replica counts) so the plots can
   mark the transition.

**Scission is the same protocol mirrored:** spawn A- and B-pools (+ B's dispatcher or
a new intermediate edge), flip routing at A's input, drain the fused pool. State
handoff is §6's shard-ownership flip — pointers, not copies, same process. Initial
split sizes need no guessing: the fused workers' hops still time A and B separately,
so allocate Ra:Rb = observed tA:tB share of R_ab.

## 6. State interaction (the hard constraint)

With host-owned state (see STATE_MANAGEMENT.md) fusion is *tractable at all* — state
lives outside the components, so fusing does NOT merge linear memories or migrate data:

- A fused worker's imported state API routes to A's shards and B's shards under their
  original logical-stage namespaces. **State identity follows the logical stage, not
  the physical worker** — fusion and scission never touch the bytes, only shard
  *ownership* (which thread may touch which shard), flipped during step 4's drain.
- **Keyed state constraint:** key-affine routing must hold for BOTH halves inside one
  fused worker. If A and B are keyed by the same field (common: sensor_id end to end),
  align their shard maps and fusion preserves affinity for free. If they're keyed by
  *different* fields, one half's affinity cannot be preserved → either that half falls
  back to synchronized shared-map access (slower, correct), or the controller refuses
  to fuse that edge. Make key-domain compatibility an explicit fusibility rule in §3.
- Replica-count coupling: shard count was chosen per stage (k × max_replicas); the
  fused stage serves A-shards and B-shards with R_ab workers — fine, since shard
  counts already exceed replica counts by design.

## 7. What this buys the paper

A serverless runtime that **rewrites its own physical topology** under SLO pressure,
with morphs measured end-to-end: per-event cost of the edge (before) vs fused chain
(after) is directly visible in `inter_stage` and `worker_timing`, and the morph events
annotate the timeline. The controlled comparison is cheap to run: same YAML with
`fusion: off | on`.

## 8. Prerequisites & sequencing

1. TODO 1.5 EOS barrier (a morph mid-`tp`-run must not lose or double-count EOS).
2. DAG runtime support ([DAG_SUPPORT.md](DAG_SUPPORT.md)) — mutable adjacency +
   routing tables are the same machinery step 3 flips.
3. State design ([STATE_MANAGEMENT.md](STATE_MANAGEMENT.md)) if stages are stateful;
   stateless fusion (Option A, steps 1–5) is implementable before state lands and is
   the right first milestone.
4. Typed-dispatch fused paths and Option B composition: later optimizations.

## 9. Open questions

- Controller placement: per-edge thread vs one global controller reading all
  autoscaler signals (recommend global — fusion decisions are inherently cross-stage).
- Epoch budget for chains: sum of per-stage budgets, or per-half re-arming between the
  two calls (per-half is more faithful to the unfused isolation semantics)?
- Should `fanout: route` stages be fusable with their single chosen successor per
  event (per-event fusion)? No — keep §3's static rule; per-event would reintroduce
  branchy dispatch in the hot loop.
- Cost model constants (α, hysteresis windows) need calibration runs — the ablation
  harness in `bench/` is the natural place.
