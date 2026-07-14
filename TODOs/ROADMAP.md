# Roadmap — DAG support · fusion/scission · state management

Untracked working note (2026-07-13). Sequencing rationale for the three design docs:
[DAG_SUPPORT.md](DAG_SUPPORT.md) · [FUSION_SCISSION.md](FUSION_SCISSION.md) ·
[STATE_MANAGEMENT.md](STATE_MANAGEMENT.md).

**Ordering principle:** the three features share hidden prerequisites — the ideal
roadmap builds each shared piece exactly once, and every milestone lands with its own
conservation test (`sent == received`, later `== per-key exact`).

---

## Shared infrastructure (what actually decides the order)

| Enabler | Needed by | Status |
|---|---|---|
| EOS counting barrier (TODO 1.5 rework: dispatcher/stage absorbs in-degree markers, forwards one per out-edge) | DAG fan-in (BLOCKING — first finished branch would kill the run), fusion (morph mid-run must not lose/double EOS), linear pipelines (existing 1.5 race) | not started |
| Key hash in binary envelope header | DAG `route` mode, state key-affine routing, fusion state-legality check | not started |
| Mutable routing / stage set (start/stop autoscaler loops at runtime) | fusion (the one real structural change), state handoff (flip shard routing), DAG reconfig later | not started |
| SpscMesh evolution: global producer-index space; runtime mesh swap (ArcSwap handle) | DAG fan-in, fusion — both on the preferred in-node transport | not started |
| Drain/quiesce hooks: replica-index free-list (1.1), consumer-close protocol (1.6) | state handoff, fusion transitions | **DONE** |
| Live per-edge stats (rolling inter-stage gaps + wasm/overhead split; today summary-only) | fusion controller | not started |

---

## Milestones

### M0 — Enablers (small; do first)
- EOS counting barrier + key hash in the envelope header.
- Neither is a feature; both unblock everything. The EOS rework pays immediately
  (fixes 1.5 for plain linear pipelines).
- Test: existing `tests/no_leak_scaling.sh` must stay green on both transports;
  add a multi-replica EOS-ordering case.

### M1 — DAG phase 1+2: fan-in, then broadcast fan-out
- Cheapest feature (fan-in is nearly transport-native: MPMC ring + PULL already
  fan in; SpscMesh needs the global producer-index space).
- Table stakes for the runtime's claim (README comparison table lists topologies)
  and the first real exercise of the EOS barrier.
- Test: DAG-topology conservation (`sent == received` across fan-in/fan-out; under
  broadcast, per-sink counts).

### M2 — Stateless fusion/scission MVP
- Controller (global, §4.1 of FUSION_SCISSION.md), fused worker loop, autoscaler
  start/stop, live per-edge stats.
- **Before state, deliberately:**
  1. It is the NOVEL result (the paper differentiator) — de-risk it early.
  2. Stateless fusion has zero state prerequisites (scoped that way in the doc).
  3. The state design guarantees forward-compat (state follows the LOGICAL stage;
     fusion never touches state bytes) — retrofit risk is designed out.
- Test: conservation through a morph; before/after `inter_stage` + `worker_timing`
  as the headline measurement.

### M3 — State: affine routing → shard map + WIT state → handoff
- Key-affine routing (`hash(key) → shard → owner`), shard map as first-class object,
  WIT-declared Mealy state (`process-event(ev, state, bench) -> (out, state, bench)`),
  handoff on the existing drain/close hooks.
- DAG `route` mode falls out nearly free here (same routing mechanism).
- Standalone headline: "stateful autoscaling with zero-loss rescaling".
- Test: PER-KEY conservation across rescales (each key's running aggregate exact) —
  direct extension of the existing harness.

### M4 — Stateful fusion + informed scission
- Key-domain legality rule for fusing stateful stages; scission sized by the
  observed tA:tB hop split. Small once M2+M3 exist.

### M5 — Distributed (PARKED)
- Waits for multi-host. Everything above is designed so this is routing +
  byte-transfer (shard map spans nodes; state records are WIT → envelope-serializable
  by construction), not a redesign.

---

## The one defensible swap

M2 ↔ M3: do state first if a paper deadline needs stateful operators more than the
morphing story (state is expected-but-standard; fusion is the novelty). M0/M1 feed
both, so nothing else changes.

## Meta: the conservation-test thread

Every milestone ships with its own `no_leak_scaling` variant. Cheap each time now
that the harness + pulse profile + plot tooling exist, and it is exactly the evidence
a reviewer wants that a morphing, rescaling runtime does not quietly eat events.
