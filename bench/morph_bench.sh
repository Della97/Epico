#!/usr/bin/env bash
# morph_bench.sh — measure switch cost S and break-even for runtime morphs (M2).
#
# This is the M2 deliverable. Not "fusion works" — a number: how long must the
# rewritten topology run before the rewrite has paid for itself, plus the switch
# cost S for the protocol implemented.
#
# Protocol under test: #1, STOP-THE-WORLD (drain -> teardown -> deploy ->
# resume). It is deliberately the worst of the three in the break-even table and
# is implemented first precisely so the cheaper two have a baseline to beat:
#
#   stop-the-world            baseline S, worst          <- measured here
#   shadow + cutover          near-zero stall, 2x resources during transition
#   in-place surgical rewire  lowest; the systems contribution
#
# Ordered DAG pairs, run in this order and for this reason:
#
#   1. identity  DAG1 -> DAG1, same topology redeployed. Zero expected delta BY
#                CONSTRUCTION, which isolates switch mechanism cost from any
#                workload effect.
#   2. fuse      A->B->C becomes A->BC. The DIRECTION of the change is
#                predictable; predictability is the feature. This validates the
#                harness, it does not discover an optimization.
#   3. split     the inverse.
#
#   A surprising number on 1 or 2 means the harness is wrong, not the intuition.
#
# Each run applies all three twice; N runs give a distribution rather than a
# single number, because the tail is the part a controller has to respect —
# never propose a morph faster than break-even.
#
# Both arms run EPICO_DYNAMIC_DISPATCH=1. Typed dispatch cannot be chained
# through a fused pair (PreparedDispatch::call goes envelope->bytes in one shot),
# so a typed baseline against a dynamic fused arm would measure typed-vs-dynamic
# rather than unfused-vs-fused. Absolute numbers therefore sit ABOVE the
# production typed path, and every figure derived from this harness must say so.
#
# Usage:  bench/morph_bench.sh              # 10 runs, spsc
#         RUNS=3 bench/morph_bench.sh       # quick check
#         TRANSPORTS="spsc mpmc" bench/morph_bench.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/fusion-chain"
YAML=".morph_bench.yaml"
RUNS="${RUNS:-10}"
TRANSPORTS="${TRANSPORTS:-spsc}"
TIMEOUT_S="${TIMEOUT_S:-300}"
OUT_DIR="${OUT_DIR:-$ROOT/bench/results}"

mkdir -p "$OUT_DIR"

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

for impl in $TRANSPORTS; do
    echo
    echo "=== transport: $impl — $RUNS runs ==="
    summaries=()
    for i in $(seq 1 "$RUNS"); do
        echo "--- run $i/$RUNS"
        stamp="$(mktemp)"
        (
            cd "$EXAMPLE" || exit 1
            export EPICO_EDGE_IMPL="$impl"
            export EPICO_DYNAMIC_DISPATCH=1
            run_with_timeout "$TIMEOUT_S" "$EPICO" run -c "$YAML" >/dev/null 2>&1
        )
        s="$(find "$EXAMPLE/logs" -name 'master*summary.json' -newer "$stamp" 2>/dev/null | sort | tail -1)"
        rm -f "$stamp"
        if [ -n "$s" ]; then
            summaries+=("$s")
            echo "    $s"
        else
            echo "    (no summary produced — run skipped)"
        fi
    done

    if [ "${#summaries[@]}" -eq 0 ]; then
        echo "no runs produced a summary for $impl"
        continue
    fi

    json="$OUT_DIR/morph_${impl}.json"
    echo
    echo "=== analysis: $impl ==="
    python3 "$ROOT/bench/analyze_morph.py" \
        --edge normalize:detect \
        --json "$json" \
        "${summaries[@]}"
done
