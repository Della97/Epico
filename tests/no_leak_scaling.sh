#!/usr/bin/env bash
# no_leak_scaling.sh — event-conservation test across scale-up/down cycles.
#
# Runs the .leak_test.yaml pipeline (min:0, aggressive cooldowns) with the
# loadgen's `pulse` profile: an exactly known event count sent in bursts with
# idle gaps that out-wait cooldown_down_s, so every burst re-triggers scale-up
# and every gap triggers scale-down. Asserts, per transport:
#   1. counters.events_received == events sent  (no leakage, no duplication)
#   2. the scaling block shows real up AND down activity (else inconclusive)
#
# Usage:  tests/no_leak_scaling.sh              # spsc + mpmc
#         TRANSPORTS=mpmc tests/no_leak_scaling.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/two-stage-min"
YAML=".leak_test.yaml"
EXPECTED=200000                       # must match `count:` in .leak_test.yaml
TRANSPORTS="${TRANSPORTS:-spsc mpmc}"
TIMEOUT_S="${TIMEOUT_S:-240}"

echo "==> building release binaries"
(cd "$ROOT" && cargo build --release --workspace) || exit 1
EPICO="$ROOT/target/release/epico"

# Portable timeout (macOS has no coreutils `timeout` by default).
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

    python3 - "$summary" "$EXPECTED" <<'PYEOF'
import json, sys
summary_path, expected = sys.argv[1], int(sys.argv[2])
d = json.load(open(summary_path))
received = d["counters"]["events_received"]
leaked = expected - received
scaling = d.get("scaling", {}).get("per_stage", {})
ups   = sum(s.get("scale_up_count", 0)   for s in scaling.values())
downs = sum(s.get("scale_down_count", 0) for s in scaling.values())
print(f"    sent={expected}  received={received}  leaked={leaked}")
for name, s in sorted(scaling.items()):
    print(f"    stage {name}: scale_ups={s.get('scale_up_count', 0)} "
          f"scale_downs={s.get('scale_down_count', 0)} "
          f"cold_starts={s.get('cold_start_count', 0)}")
if ups < 2 or downs < 1:
    print("    INCONCLUSIVE: not enough scaling activity to test conservation")
    sys.exit(2)
if leaked > 0:
    print(f"    FAIL: {leaked} events leaked during scale up/down")
    sys.exit(1)
if leaked < 0:
    print(f"    FAIL: {-leaked} duplicate events observed")
    sys.exit(1)

# EOS barrier discipline (M0): once the collector has seen EOS, the stage set
# is finishing — no worker may boot after that point. Catches the old
# min-replica respawn-after-EOS behaviour.
jsonl = summary_path.replace("_summary.json", ".jsonl")
try:
    lines = open(jsonl).read().splitlines()
    eos_at = next((i for i, l in enumerate(lines)
                   if "EOS received at collector" in l), None)
    if eos_at is not None:
        late_boots = [l for l in lines[eos_at:] if "worker booted" in l]
        if late_boots:
            print(f"    FAIL: {len(late_boots)} worker(s) booted after EOS reached the collector")
            sys.exit(1)
        print("    PASS: no worker boots after EOS (barrier discipline holds)")
except OSError:
    print(f"    (skip EOS-ordering check: {jsonl} not found)")

print("    PASS: all events conserved across scaling cycles")
PYEOF
    [ $? -ne 0 ] && overall=1
done

echo
[ "$overall" -eq 0 ] && echo "ALL TRANSPORTS PASS" || echo "FAILURES DETECTED"
exit "$overall"
