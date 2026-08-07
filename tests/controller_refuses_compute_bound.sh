#!/usr/bin/env bash
# controller_refuses_compute_bound.sh — the cost-model controller correctly
# doing NOTHING (roadmap M2, phase 5e).
#
#   > 5e is the one to not skip. A controller that morphs when it should not is
#   > worse than no controller, and only a negative test catches it.
#
# The companion test (controller_finds_the_fuse.sh) proves the controller ACTS
# on an edge-dominated pipeline. This proves it does not act on a compute-bound
# one — which is the harder half, because "propose nothing" is also what a
# controller that is silently broken, misconfigured, or never armed does. So the
# assertions are two-sided:
#
#   1. zero morphs were proposed and zero applied            (it did nothing)
#   2. it armed, evaluated the declared pair, and refused ON  (it did nothing
#      COST GROUNDS, naming the compute-bound diagnosis        FOR THE RIGHT
#                                                              REASON)
#
# Without (2) this test would pass against a controller that had been deleted.
#
# The fixture is the third one the plan says is needed:
#
#   > Diagnosis needs a compute-bound counter-case. The refusal branch is
#   > untested until there is a pipeline where fusion genuinely does not help.
#   > The skew arm from phase 4 is close: `detect` at 34 us against a ~2 ms edge
#   > is still edge-dominated, so a THIRD fixture is needed with per-event
#   > compute above the edge cost.
#
# Usage:  tests/controller_refuses_compute_bound.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/fusion-chain"
YAML=".controller_negative_test.yaml"
TRANSPORTS="${TRANSPORTS:-spsc}"
TIMEOUT_S="${TIMEOUT_S:-300}"

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
    runlog="$(mktemp)"
    sleep 1
    (
        cd "$EXAMPLE" || exit 1
        export EPICO_EDGE_IMPL="$impl"
        export EPICO_DYNAMIC_DISPATCH=1
        run_with_timeout "$TIMEOUT_S" "$EPICO" run -c "$YAML"
    ) > "$runlog" 2>&1
    rc=$?
    [ "$rc" -ne 0 ] && echo "    (epico run exited rc=$rc; checking anyway)"

    summary="$(find "$EXAMPLE/logs" -name 'master*summary.json' -newer "$stamp" 2>/dev/null | sort | tail -1)"
    rm -f "$stamp"
    if [ -z "$summary" ]; then
        echo "    FAIL: no summary produced by this run"
        rm -f "$runlog"
        overall=1
        continue
    fi
    echo "    summary: $summary"

    python3 - "$summary" "$runlog" <<'PYEOF'
import json, re, sys

d = json.load(open(sys.argv[1]))
log = open(sys.argv[2], errors="replace").read()

fail = False

# ── 1. It did nothing ────────────────────────────────────────────────────
proposals = re.findall(r"controller proposing a morph\s+decision=(.*)", log)
if proposals:
    print(f"    FAIL: the controller proposed {len(proposals)} morph(s) on a "
          f"compute-bound pipeline — fusing here deletes an edge that is not "
          f"the bottleneck:")
    for p in proposals:
        print(f"      {p.strip()}")
    fail = True

scaling = d.get("scaling", {}).get("per_stage", {})
applied = [
    (e["t_s"], stage, e["action"])
    for stage, blk in scaling.items()
    for e in blk.get("events", [])
    if e["action"] in ("fuse", "split", "identity")
]
if applied:
    print(f"    FAIL: {len(applied)} morph(s) were applied:")
    for t_s, stage, action in sorted(applied):
        print(f"      t={t_s:7.3f}s  {action} -> {stage}")
    fail = True

if not fail:
    print("    zero morphs proposed, zero applied")

# ── 2. ...for the right reason ───────────────────────────────────────────
# Everything below guards against this test passing vacuously.
if "cost-model controller armed" not in log:
    print("    FAIL: the controller never armed — this run proves nothing about "
          "the decision rule, only that a disabled controller is quiet")
    fail = True

holds = re.findall(r"controller holding\s+reason=(.*)", log)
reasons = {h.strip() for h in holds}
print(f"    distinct hold reasons ({len(reasons)}):")
for r in sorted(reasons)[:8]:
    print(f"      {r[:150]}")

# It must have actually looked at the declared pair, not just sat in cooldown
# or idled on stale statistics for the whole run.
evaluated = [r for r in reasons if "normalize->detect" in r]
if not evaluated:
    print("    FAIL: the controller never evaluated the declared fusible pair — "
          "every hold was hysteresis or staleness, so the cost rule was never "
          "exercised at all")
    fail = True

# And the binding reason must be the compute-bound diagnosis specifically.
# Refusing for lack of pressure would also give zero morphs, but it would leave
# the branch this fixture exists to test unexecuted.
compute_bound = [r for r in reasons if "compute-bound at max replicas" in r]
if not compute_bound:
    print("    FAIL: the controller refused, but never on compute-bound grounds. "
          "The refusal branch the plan calls out as untested is STILL untested; "
          "check that the heavy stage variants are the ones being built.")
    fail = True
else:
    print(f"    refused on the compute-bound diagnosis {len(compute_bound)} "
          f"distinct time(s), e.g.:")
    print(f"      {sorted(compute_bound)[0][:220]}")

    # The numbers in the refusal are the point of the whole phase: the p50
    # comparison must favour compute while the p99 comparison — the one the
    # design note originally specified — would have got it backwards. If that
    # inversion ever stops showing up here, the fixture has drifted out of the
    # regime it was built to cover.
    m = re.search(r"wasm p50 (\d+)us > edge p50 (\d+)us; p99s (\d+)us vs (\d+)us",
                  sorted(compute_bound)[0])
    if m:
        w50, e50, w99, e99 = (int(g) for g in m.groups())
        print(f"    p50: compute {w50}us vs edge {e50}us  (compute wins — correct)")
        print(f"    p99: compute {w99}us vs edge {e99}us  "
              f"({'edge wins — a p99 rule would have fused here' if e99 > w99 else 'compute wins'})")
        if w50 <= e50:
            print("    FAIL: the fixture is not compute-bound at p50; it cannot "
                  "test the refusal it exists for")
            fail = True

sys.exit(1 if fail else 0)
PYEOF
    if [ $? -ne 0 ]; then
        overall=1
    else
        echo "    PASS: the controller refused a compute-bound pipeline, by name"
    fi
    rm -f "$runlog"
done

echo
if [ "$overall" -eq 0 ]; then
    echo "ALL TRANSPORTS PASS"
else
    echo "FAILURES — see above"
fi
exit "$overall"
