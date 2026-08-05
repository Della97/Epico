#!/usr/bin/env bash
# no_leak_morph.sh — event conservation ACROSS A MORPH BOUNDARY (roadmap M2).
#
# The existing conservation tests prove the runtime does not eat events within a
# FIXED topology:
#
#   no_leak_scaling.sh    linear pipeline, replicas scaling 0 -> N -> 0
#   no_leak_dag.sh        fan-out / fan-in, sent x path multiplicity
#   no_leak_dag_nested.sh nested DAGs, per-path multiplicity
#
# This one proves it does not eat them while the topology CHANGES underneath the
# traffic. Runs examples/fusion-chain/.morph_test.yaml:
#
#   ingest -> normalize -> detect -> finalize          (DAG1)
#   ingest -> [normalize|detect]  -> finalize          (DAG2, contracted)
#
# with three transitions mid-run — identity, fuse, split — while the source is
# pushing. The assertion is EXACT equality in both directions: fewer received
# means the switch dropped events, more means it duplicated them. Neither can
# hide behind the other.
#
# What makes the assertion meaningful, and would silently void it if changed:
#
#   * The `pulse` source uses BLOCKING sends. A source that dropped under the
#     morph's backpressure would turn a real leak into a green run.
#   * All three morphs must actually FIRE. A run where the schedule was
#     out-waited by EOS proves nothing, so the morph count is asserted too.
#   * min_replicas is 0, so the fused stage cold-starts from nothing — the path
#     that has to reopen the mesh columns the retired stages closed.
#
# Usage:  tests/no_leak_morph.sh              # spsc + mpmc
#         TRANSPORTS=mpmc tests/no_leak_morph.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/fusion-chain"
YAML=".morph_test.yaml"
EXPECTED=240000               # must match `count:` in .morph_test.yaml
EXPECTED_MORPHS=3             # must match the `morphs:` block
TRANSPORTS="${TRANSPORTS:-spsc mpmc}"
TIMEOUT_S="${TIMEOUT_S:-240}"

echo "==> building release binaries"
(cd "$ROOT" && cargo build --release --workspace) || exit 1
EPICO="$ROOT/target/release/epico"

run_with_timeout() {
    local secs=$1; shift
    "$@" &
    local pid=$! t=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1; t=$((t + 1))
        if [ "$t" -ge "$secs" ]; then
            echo "    TIMEOUT after ${secs}s — killing run"
            kill -INT "$pid" 2>/dev/null; sleep 3
            kill -9 "$pid" 2>/dev/null
            return 124
        fi
    done
    wait "$pid"
}

overall=0
for impl in $TRANSPORTS; do
    echo
    echo "=== transport: $impl ==="
    stamp="$(mktemp)"
    (
        cd "$EXAMPLE" || exit 1
        export EPICO_EDGE_IMPL="$impl"
        # Both arms of the M2 comparison run dynamic dispatch: the fused chain
        # cannot use the typed fast path, so a typed baseline would measure
        # typed-vs-dynamic rather than unfused-vs-fused.
        export EPICO_DYNAMIC_DISPATCH=1
        run_with_timeout "$TIMEOUT_S" "$EPICO" run -c "$YAML"
    )
    rc=$?
    [ "$rc" -ne 0 ] && echo "    (epico run exited rc=$rc; checking summary anyway)"

    summary="$(find "$EXAMPLE/logs" -name 'master*summary.json' -newer "$stamp" 2>/dev/null | sort | tail -1)"
    rm -f "$stamp"
    if [ -z "$summary" ]; then
        echo "    FAIL: no summary produced by this run"
        overall=1
        continue
    fi
    echo "    summary: $summary"

    python3 - "$summary" "$EXPECTED" "$EXPECTED_MORPHS" <<'PYEOF'
import json, sys

summary_path, expected, expected_morphs = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
d = json.load(open(summary_path))
received  = d["counters"]["events_received"]
per_stage = d["counters"].get("per_stage_count", {})
scaling   = d.get("scaling", {}).get("per_stage", {})

# Every transition the actuator carried out, in time order. `morph_begin` /
# `morph_end` bracket each one on the subject stage's timeline with the verb in
# between, so this reads the whole switch history off the standard summary.
VERBS = ("fuse", "split", "identity")
morphs = sorted(
    (e["t_s"], stage, e["action"])
    for stage, blk in scaling.items()
    for e in blk.get("events", [])
    if e["action"] in VERBS
)
brackets = {
    stage: [e for e in blk.get("events", []) if e["action"].startswith("morph_")]
    for stage, blk in scaling.items()
}

print(f"    sent={expected}  received={received}")
print(f"    morphs applied ({len(morphs)}):")
for t_s, stage, action in morphs:
    print(f"      t={t_s:7.3f}s  {action:<9} -> {stage}")
for stage, evs in brackets.items():
    failed = [e for e in evs if e["action"] == "morph_failed"]
    for e in failed:
        print(f"      t={e['t_s']:7.3f}s  MORPH FAILED on {stage}")

fail = False

if len(morphs) != expected_morphs:
    print(f"    FAIL: {len(morphs)} morphs applied, expected {expected_morphs} — "
          f"a run whose schedule did not fire proves nothing about conservation")
    fail = True

if any(e["action"] == "morph_failed" for evs in brackets.values() for e in evs):
    print("    FAIL: a morph was refused or aborted")
    fail = True

# The fused stage must genuinely have carried traffic — otherwise the fuse
# happened over an idle pipeline and the switch boundary was never crossed by a
# single event.
fused = [s for s in per_stage if "_" in s and s not in ("normalize", "detect")]
print(f"    per-stage hop counts: " +
      ", ".join(f"{k}={per_stage[k]}" for k in sorted(per_stage)))

# Both halves keep their OWN logical identity through the fusion, so each must
# still have seen every event exactly once — this is what makes informed
# scission possible, and it breaks the moment a fused worker stops emitting one
# hop per half.
for half in ("normalize", "detect"):
    if half in per_stage and per_stage[half] != expected:
        print(f"    FAIL: half {half} saw {per_stage[half]} events, expected {expected} "
              f"(a fused worker must emit one hop per half)")
        fail = True

if received != expected:
    delta = expected - received
    verb  = "lost" if delta > 0 else "duplicated"
    print(f"    FAIL: {abs(delta)} events {verb} across the switch boundary "
          f"(received {received}, expected {expected})")
    fail = True

sys.exit(1 if fail else 0)
PYEOF
    if [ $? -ne 0 ]; then
        overall=1
    else
        echo "    PASS: conserved exactly across $EXPECTED_MORPHS morph boundaries"
    fi
done

echo
[ "$overall" -eq 0 ] && echo "ALL TRANSPORTS PASS" || echo "FAILURES DETECTED"
exit "$overall"
