# TODOs — from repo review (2026-07-09)

Untracked working list. Ordered by severity. File/line refs are as of commit `901b8b3`.

Design notes (future work):
- [ROADMAP.md](ROADMAP.md) — milestone sequencing for the three designs below
  (M0 enablers → DAG → stateless fusion → state → stateful fusion → distributed),
  with the shared-infrastructure table that determines the order.
- [STATE_MANAGEMENT.md](STATE_MANAGEMENT.md) — stateful stages with dynamic replica
  counts (host-owned state, key-partitioned ownership, no Redis).
- [DAG_SUPPORT.md](DAG_SUPPORT.md) — fan-out / fan-in: arbitrary DAGs declared in the
  YAML (CLI already parses edges; runtime work + EOS barrier).
- [FUSION_SCISSION.md](FUSION_SCISSION.md) — runtime stage fusion/scission: the DAG
  morphs under SLO pressure (host-level fusion via cached InstancePres, edge
  contraction rules, replica-aware transition protocol, state constraints).

---

## 1. Correctness

### 1.1 Replica-index reuse → two consumers on one SPSC ring (UB) — **DONE (2026-07-09)**
- ~~**Problem:** `replica_idx = workers.len()` assigned after `workers.retain(...)` →
  index collision with a still-live worker → two consumers on one `Swsr` ring (UB) /
  merged hop labels on the zmq path.~~
- Fixed: `WorkerHandle` now carries its `replica_idx`; the autoscaler hands out indices
  from a `BTreeSet` free-list (lowest first) and reclaims them in the `retain` pass only
  after `done` confirms the thread exited — a draining worker keeps its index (and its
  mesh column) until it is truly gone. If all `max_rep` indices are held, the spawn is
  deferred to the next tick with a warning (also fixes the mesh out-of-bounds risk when
  `min_replicas > max_replicas` is misconfigured).
- Possible follow-up: extract the index pool into a small tested struct.

### 1.2 "Reservoir sampling" is actually a sliding window — **DONE (2026-07-09)**
- ~~Replacement slot was `recv_count % EVENTS_LIVE_CAP` → sliding window of the last
  200k events, biasing `events`/`inter_stage` toward the end of the run.~~
- Fixed: proper Algorithm R — event N replaces a uniformly random slot with probability
  CAP/N (`rand` was already a dependency). The reservoir is now unordered, so
  `build_summary` sorts by `recv_t_s` before subsampling to preserve the documented
  arrival order of `events.rows`.

### 1.3 Collector `try_lock` silently drops telemetry under contention — **DONE (2026-07-09)**
- ~~On `try_lock` failure the event's `e2e_ms` sample AND its `recv_per_second`
  increment were dropped → e2e/throughput undercounted exactly when busiest.~~
- Fixed via the "shard the state" option, taken to its endpoint: the collector
  accumulates all its per-event fields in a thread-local `RunTelemetry` (NO lock on the
  hot path at all — strictly faster than the old `try_lock`) and moves them into the
  shared telemetry once at exit. `run_agent` now keeps the collector's `JoinHandle` and
  joins it before `build_summary`, so the summary always sees the merged state.
  Autoscalers/resource sampler keep writing their own fields under the lock (low rate).
  Writer discipline documented on `RunTelemetry`.

### 1.4 `cargo test --workspace` fails out of the box — **DONE (2026-07-09)**
- ~~**Where:** doctest in `epico-sdk/src/lib.rs:7-15`.~~
- Fixed: fence changed `no_run` → `ignore` with a comment explaining why (wit dir only
  exists inside generated stage crates). `cargo test --workspace` is green (10 tests).

### 1.5 EOS vs. multi-replica stages is racy — **MEDIUM**
- **Where:** `epico-core/master/src/worker.rs:587-594` (EOS forward + exit), `master.rs:1085` (shutdown trigger), `master.rs:577-580` (grace sleep).
- **Problem:**
  - EOS is delivered to exactly one replica per stage (round-robin) and can overtake events still in flight on sibling replicas → collector may shut down while events are in flight.
  - The 500 ms grace sleep happens *after* `col_running` is set false, so the collector has already stopped and cannot drain — swap the sleep-then-stop order.
  - Sibling replicas never see EOS and live until `kill_children`.
- **Fix:** swap sleep/stop order; longer term, EOS-per-replica counting or a drain barrier before trusting `tp`-mode counts with `max > 1`.

### 1.6 SPSC mesh scale-down strands events — **DONE (2026-07-11)**
- **Fixed** via the consumer-close protocol below, plus two worker-side ordering fixes
  it exposed:
  - `worker.rs`: output sends now use a never-raised flag — an event that reached the
    output side was already consumed from the input, so aborting the push on OUR drain
    flag while downstream was momentarily full was the same leak class.
  - `worker.rs`: on an in-proc input, EOS is DEFERRED — the worker raises its own drain
    flag, processes the residue its column/ring still holds (EOS is last only within
    one producer's ring), and forwards EOS at exit. Without this, the close fix would
    just move the leak to the EOS race.
  - `SpscMesh::len()` now also counts producers blocked in push backoff, so an
    all-closed all-empty mesh still shows demand and the autoscaler respawns/reopens a
    consumer instead of deadlocking; `for_replica` reopens the column on respawn.
  - Close is race-free via a per-ring `busy` / per-column `closed` Dekker handshake
    (SeqCst pairs) — a producer mid-push is waited out before the residue drain.
- **Verified:** new unit test `mesh_close_mid_stream_conserves_events` (400k events,
  mid-stream close, exact conservation + payload checksum); e2e
  `tests/no_leak_scaling.sh` now PASSES on BOTH transports (spsc: 200000/200000 across
  11 ups / 9 downs / 4 cold starts); `plots/leak_spsc_fixed.png` shows every queue
  spike returning to zero — the stranded-residue plateau is gone.

Original finding (kept for reference):
- **Found:** 2026-07-09, by the new event-conservation test. `edge_impl: spsc` with
  min:0/max:4 pulse load: sent=200000, received=197952 — **leaked 2048 = 4 × 512 =
  exactly one full consumer column** (n_prod rings × `spsc_ring_cap`). `mpmc` PASSES
  the same test (shared ring: drained workers stop popping, others keep consuming).
- **Why:** two compounding behaviors in `epico-core/master/src/spsc.rs`:
  1. `SpscMesh::push` round-robins and only skips a consumer's ring when FULL — it
     keeps filling a draining consumer's column until cap.
  2. `MeshRx::pop(drain)` does ONE "last look" when drain is raised, then the worker
     exits — up to n_prod × ring_cap events stay stranded in its column forever.
  (With the 1.1 free-list, a later respawn on the same index would consume leftovers —
  masking, not fixing; final drains before EOS have no respawn.)
- **Fix (consumer-close protocol):**
  1. Per-consumer `closed: AtomicBool` in `SpscMesh`; `push` skips closed columns
     (backpressure via Err only if all OPEN columns are full).
  2. Scale-down order: mark closed FIRST, then the draining worker keeps popping until
     its column is fully empty (terminates, since producers no longer feed it), then
     exits.
  3. `for_replica(r)` reopens column r on (re)spawn; scale-from-zero keeps working
     (mesh `len()` still counts closed-column residue so the autoscaler wakes).
- **Test:** `tests/no_leak_scaling.sh` + `examples/two-stage-min/.leak_test.yaml` +
  loadgen `pulse` profile (blocking sends, exact EOS count, bursts with idle gaps that
  out-wait cooldown_down_s → repeated 0→N→0 cycles). Re-run after fix; both transports
  must PASS.
- **Visualization:** `tests/plot_leak.py` (newest summary + loadgen jsonl → 3 panels:
  cumulative sent vs received with the leak as residual gap, replicas + drain markers,
  queue depth). See `plots/leak_spsc.png` (FAIL — received flatlines 2 048 short AND
  forward's queue depth plateaus at ~10³ at run end = the stranded mesh residue,
  visible directly) vs `plots/leak_mpmc.png` (PASS — every gap closes, queues drain
  to 0). Reproduced twice; the 2 048 signature is deterministic.
  Side fix while building this: in-proc stages produced NO queue_depth samples (the
  push sat inside the dispatcher-metrics block) — now sampled every 50 ms for both
  transports in `autoscaler.rs`.

### 1.7 Loadgen `linger=0` drops queued tail + EOS under blast — **DONE (2026-07-12)**
- **Symptom:** tp blast 500k received exactly 450,058 with 30 s AND 240 s EOS grace —
  deterministic, so not a timeout. Thread sample of the stalled agent: every thread
  idle in input-wait backoff → pipeline STARVED, not slow/deadlocked.
- **Root cause:** `epico-loadgen` set `push.set_linger(0)`. `send()` returning only
  means the message entered zmq's local queue; blast outruns the pipeline, so at
  process exit ~50k events + the EOS marker were still queued — linger=0 discards
  them. Non-blast profiles never hit it (generation slower than the pipeline).
- **Fix:** bounded `set_linger(120_000)` — exit blocks until the queue drains, capped
  so a dead consumer can't hang the loadgen. Verified twice: 500,000/500,000, EOS
  self-exit <1 s after loadgen finish. (Also proved the pipeline runs ~450k ev/s and
  the 1.6 EOS-deferral drains in-flight residue exactly: collector saw EOS at
  499,983/499,554 and still ended at exactly 500,000.)
- **Ops note:** the EOS grace is configurable via `EPICO_EOS_DRAIN_SECS` (CLI, default 30).

---

## 2. Smaller code issues

- [ ] **Doc drift:** `autoscaler.rs:1` says "ticks every 20ms", `autoscaler.rs:328` says "20 ms tick" — actual `TICK_MS = 1`. (README says 1 ms, correctly.)
- [ ] **Hardcoded `wasmtime_version = "26"`** at `master.rs:1896` — goes silently stale on a dependency bump; no enforcement link to the `Cargo.toml` pin.
- [ ] **Autoscaler REQ/REP wedge risk:** on a 10 ms recv timeout a late reply can leave the REQ socket in EFSM; the single `DONTWAIT` discard (`autoscaler.rs:566-576`) usually resyncs but isn't guaranteed. Set `ZMQ_REQ_RELAXED` / `ZMQ_REQ_CORRELATE`.
- [ ] **Dead code (6 warnings):** `SpscMesh::n_prod/n_cons`, `EdgeOutSrc::is_some`, the `let _ = (empty_u, empty_f, ...)` unused-suppression at `master.rs:1618`. Also `last_active_ts`/heartbeat is plumbed through every worker but never read by any scaling decision — use it or remove it.

---

## 3. Repo hygiene

- [ ] **Commit `Cargo.lock`.** Workspace produces binaries (master, dispatcher, CLI, loadgen); lockfile belongs in git for reproducible builds — half the point of a research artifact. Remove `Cargo.lock` from `.gitignore`; also delete the stray `epico-wire/Cargo.lock` (implicit workspace member).
- [ ] **Remove committed run artifacts:** `profile.json.gz` (repo root) and `examples/two-stage-min/results_test/master_20260623_185251_summary.json`.
- [x] **Fake build badge / no CI** — DONE (2026-07-09): added `.github/workflows/ci.yml`
      (libzmq3-dev + stable toolchain + rust-cache; `cargo check --workspace --all-targets`
      + `cargo test --workspace`) and pointed the README badge at the real workflow.
- [x] **README title doesn't render** — DONE (fixed by hand): blank line added after `</div>`.
- [ ] **Stale LOC claim:** README says ~7,400 LOC; tracked Rust is ~11.9k including examples.

---

## 4. Testing gap

- [x] **Event-conservation e2e test** — DONE (2026-07-09): `tests/no_leak_scaling.sh`
      runs `.leak_test.yaml` under the new loadgen `pulse` profile per transport and
      asserts sent == received plus real up/down scaling activity. Found 1.6 on its
      first run (spsc FAIL, mpmc PASS). Not in CI yet (needs wasm32-wasip2 target +
      stage-crate builds on the runner — heavier job, do as a follow-up).
- [ ] **No end-to-end test in CI.** 10 unit tests, all on codecs + SPSC ring (good choices), but zero CI coverage of the dispatcher credit protocol and the autoscaler state machine.
- [ ] **Cheap win:** `examples/two-stage-min` + `tp` mode is already a self-terminating integration test (`epico run` exits on EOS and writes a summary to assert against) — wire into CI.
- [ ] **Unit-test candidates:** `scan_n_credits` and `drain_dispatch` in `dispatcher.rs` are pure/near-pure functions.
