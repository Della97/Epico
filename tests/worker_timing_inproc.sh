#!/usr/bin/env bash
# worker_timing_inproc.sh — per-stage worker timing on the IN-PROCESS transports
# (roadmap M2, phase 5a / design decision D7).
#
# The worker has computed `total_ns` and `serde_ns` per event since before M2,
# but shipped them only inside the zmq credit-refill message. The autoscaler
# read them back solely via `fetch_dispatcher_metrics`, which runs only when a
# stage has no in-process input edge — so on `spsc`/`mpmc`, the transports M2
# actually measures, every `worker_timing.*.total_us` and `.serde_us` field in
# the phase-4 summaries read exactly 0.0, while `wasm_us` was populated because
# it comes from bench hops.
#
# That gap blocks the controller's FIRST question, which is diagnostic rather
# than actuating:
#
#   overhead / inter-stage-gap dominated -> fuse (the edge is the cost)
#   wasm_us dominated at max_replicas    -> fusion is USELESS (compute-bound);
#                                           the remedy is replicas or nothing
#
# Without total/serde on the in-proc path the runtime can see that an edge is
# expensive but cannot tell whether the stage behind it is compute-bound — so a
# controller shipped against this telemetry would fuse indiscriminately.
#
# The assertion is therefore deliberately weak-but-decisive: p50 > 0. Before 5a
# these fields were identically zero; any positive, finite, sane-fraction result
# means the in-proc path now reports real numbers. Pinning a magnitude would
# only encode this laptop.
#
# Usage:  tests/worker_timing_inproc.sh              # spsc + mpmc
#         TRANSPORTS=spsc tests/worker_timing_inproc.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/fusion-chain"
YAML=".morph_test.yaml"
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
    sleep 1
    (
        cd "$EXAMPLE" || exit 1
        export EPICO_EDGE_IMPL="$impl"
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

    python3 - "$summary" <<'PYEOF'
import json, sys

d = json.load(open(sys.argv[1]))
wt = d.get("worker_timing", {})
if not wt:
    print("    FAIL: no worker_timing block at all")
    sys.exit(1)

# Only stages that actually carried traffic can be asserted on. A stage that
# processed nothing contributes no timing samples BY DESIGN — an idle stage must
# push no samples rather than a run of zeros, which would drag every quantile
# down exactly when the pipeline is quiet.
per_stage_count = d.get("counters", {}).get("per_stage_count", {})

print(f"    {'stage':<20}{'samples':>9}{'total_p50':>11}{'serde_p50':>11}"
      f"{'serde_frac':>12}{'overhead_p50':>14}")

fail = False
checked = 0
for stage in sorted(wt):
    blk   = wt[stage]
    total = blk.get("total_us", {})
    serde = blk.get("serde_us", {})
    over  = blk.get("overhead_us", {})
    n     = len(total.get("samples", []))
    frac  = blk.get("serde_frac_p50", 0.0)
    print(f"    {stage:<20}{n:>9}{total.get('p50', 0):>11}{serde.get('p50', 0):>11}"
          f"{frac:>12}{over.get('p50', 0):>14}")

    if n == 0:
        # No samples is only acceptable for a stage that saw no events.
        if per_stage_count.get(stage, 0) > 0:
            print(f"    FAIL: {stage} processed {per_stage_count[stage]} events "
                  f"but produced no worker-timing samples")
            fail = True
        continue

    checked += 1

    # The regression this test exists for: identically 0.0 on the in-proc path.
    if not total.get("p50", 0) > 0:
        print(f"    FAIL: {stage}.total_us.p50 = {total.get('p50')} — the in-proc "
              f"path is not reporting worker timing (this is the D7 regression)")
        fail = True

    # serde is a strict subset of total, so its fraction is a real fraction.
    # 0 would mean serde never got measured; >= 1 would mean total is not
    # actually the enclosing interval, i.e. the two are measured on different
    # scopes and `overhead = total - wasm` is meaningless.
    if not (0.0 < frac < 1.0):
        print(f"    FAIL: {stage}.serde_frac_p50 = {frac}, expected strictly "
              f"between 0 and 1")
        fail = True

    # Overhead is what fusion actually deletes. It is derived as total - wasm,
    # so a negative would mean the two are not on the same clock.
    if over.get("p50", 0) < 0:
        print(f"    FAIL: {stage}.overhead_us.p50 = {over.get('p50')} < 0")
        fail = True

if checked == 0:
    print("    FAIL: no stage produced any timing samples")
    fail = True

sys.exit(1 if fail else 0)
PYEOF
    if [ $? -ne 0 ]; then
        overall=1
    else
        echo "    PASS: in-proc worker timing is populated"
    fi
done

echo
if [ "$overall" -eq 0 ]; then
    echo "ALL TRANSPORTS PASS"
else
    echo "FAILURES — see above"
fi
exit "$overall"
