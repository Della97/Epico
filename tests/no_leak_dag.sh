#!/usr/bin/env bash
# no_leak_dag.sh — event conservation on a NON-LINEAR topology (roadmap M1).
#
# Runs examples/dag-diamond:
#
#            ┌──► left ──┐
#   ingest ──┤           ├──► merge
#            └──► right ─┘
#
# `ingest` broadcasts (out-degree 2) and `merge` fans in (in-degree 2), so a
# correct run delivers EXACTLY 2 × sent events to the collector: fewer means a
# branch leaked, more means an event was duplicated somewhere it shouldn't be.
# The assertion is equality in both directions — an "extra events" bug can't
# hide behind the expected multiplication.
#
# Usage:  tests/no_leak_dag.sh              # spsc + mpmc
#         TRANSPORTS=mpmc tests/no_leak_dag.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/dag-diamond"
SENT=40000                    # must match `count:` in pipeline.yaml
MULTIPLICITY=2                # paths from source to sink (broadcast fan-out)
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
        run_with_timeout "$TIMEOUT_S" "$EPICO" run
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

    python3 - "$summary" "$SENT" "$MULTIPLICITY" <<'PYEOF'
import json, sys
summary_path, sent, mult = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
d = json.load(open(summary_path))
received = d["counters"]["events_received"]
expected = sent * mult
per_stage = d["counters"].get("per_stage_count", {})
scaling = d.get("scaling", {}).get("per_stage", {})

print(f"    sent={sent}  x{mult} paths  expected={expected}  received={received}")
for name in sorted(per_stage):
    print(f"    stage {name}: events={per_stage[name]} "
          f"ups={scaling.get(name, {}).get('scale_up_count', 0)} "
          f"downs={scaling.get(name, {}).get('scale_down_count', 0)}")

fail = False
# Branch stages must each see every source event exactly once; merge sees both.
if per_stage:
    for branch in ("left", "right"):
        if branch in per_stage and per_stage[branch] != sent:
            print(f"    FAIL: branch {branch} saw {per_stage[branch]}, expected {sent}")
            fail = True
    if "merge" in per_stage and per_stage["merge"] != expected:
        print(f"    FAIL: fan-in stage merge saw {per_stage['merge']}, expected {expected}")
        fail = True

if received != expected:
    delta = expected - received
    verb = "leaked" if delta > 0 else "duplicated"
    print(f"    FAIL: {abs(delta)} events {verb} (received {received}, expected {expected})")
    fail = True

sys.exit(1 if fail else 0)
PYEOF
    [ $? -ne 0 ] && overall=1 || echo "    PASS: DAG conserved exactly (fan-out ×$MULTIPLICITY, fan-in merged)"
done

echo
[ "$overall" -eq 0 ] && echo "ALL TRANSPORTS PASS" || echo "FAILURES DETECTED"
exit "$overall"
