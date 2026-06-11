#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Epico service-time decomposition: how far is the stage boundary from native?
#
# Runs the minimal 2-stage identity pipeline (relay -> forward, R=1) in three
# WORKER MODES on two TRANSPORTS, tp/blast profile:
#
#   mode=wasm        : normal path (parse -> Val -> component call -> encode)
#   mode=serde       : EPICO_NATIVE_STAGE=serde       (parse + hop + serialize,
#                      no Val, no wasm)
#   mode=passthrough : EPICO_NATIVE_STAGE=passthrough (bytes in -> bytes out,
#                      no parse at all)
#
# Decomposition (per-stage service time T, from worker_timing total_us and
# cross-checked against 1/throughput):
#   wasm + dynamic dispatch share = T(wasm)  - T(serde)
#   JSON parse/serialize + hop    = T(serde) - T(passthrough)
#   recv/push + transport plumbing= T(passthrough)
#
# CAVEAT: in serde/passthrough modes the worker may no longer be the
# bottleneck — the loadgen->ingress-dispatcher path can cap the pipeline
# first. When that happens, 1/throughput is the INGEST ceiling, not the
# service time; trust worker_timing.total_us p50/mean for T instead (the
# analyzer reports both and flags disagreement). The dispatcher arm always
# has full worker_timing for both stages; the edge arm only for relay.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
EXAMPLE_DIR="${EXAMPLE_DIR:-$PROJECT_ROOT/examples/two-stage-min}"
EPICO="${EPICO:-$PROJECT_ROOT/target/release/epico}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS="${RESULTS:-$PROJECT_ROOT/bench/results/decomp_$STAMP}"
BENCH_YAML="$EXAMPLE_DIR/.bench_pipeline.yaml"

REPEATS="${REPEATS:-3}"
TP_COUNT="${TP_COUNT:-3000000}"
SENSORS="${SENSORS:-100}"
COOLDOWN="${COOLDOWN:-8}"
ARMS="${ARMS:-dispatcher edge}"
MODES="${MODES:-wasm serde passthrough}"
CREDIT_WINDOW="${CREDIT_WINDOW:-16}"
EDGE_CAP="${EDGE_CAP:-256}"

emit_yaml() {
  cat <<EOF
package: epico:minbench@1.0.0
nodes:
  - name: local
    host: 127.0.0.1
    force_tcp: false
types:
  event:
    sensor_id: string
    value:     f64
source:
  kind: loadgen
  profile: tp
  rate: 0
  count: $TP_COUNT
  duration: 0
  sensors: $SENSORS
  blast: true
stages:
  - name: relay
    placement: local
    in:  event
    out: event
    src: ./stages/relay.rs
    scaling: { min: 1, max: 1, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
  - name: forward
    placement: local
    in:  event
    out: event
    src: ./stages/forward.rs
    scaling: { min: 1, max: 1, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
edges:
  - relay -> forward
deploy:
  host: localhost
  this_host: local
  ingress: ipc:///tmp/epico-minbench-push
  credit_window: $CREDIT_WINDOW
  batch_events: 1
  resource_sample_interval_ms: 0
EOF
}

run_one() {  # arm mode rep outdir
  local arm="$1" mode="$2" rep="$3" outdir="$4"
  mkdir -p "$outdir"
  emit_yaml > "$BENCH_YAML"

  local env_kv=()
  case "$arm" in
    edge) env_kv+=(EPICO_INPROC_EDGES=1 "EPICO_EDGE_CAP=$EDGE_CAP") ;;
    spsc) env_kv+=(EPICO_INPROC_EDGES=1 EPICO_EDGE_IMPL=spsc) ;;
    dispatcher) : ;;
  esac
  case "$mode" in
    serde|passthrough) env_kv+=("EPICO_NATIVE_STAGE=$mode") ;;
    wasm) : ;;
  esac

  echo ">> arm=$arm mode=$mode rep=$rep"
  if env ${env_kv[@]+"${env_kv[@]}"} \
        "$EPICO" run --config "$BENCH_YAML" --project-root "$PROJECT_ROOT" \
        --aot --log-dir "$outdir" >"$outdir/run.log" 2>&1; then
    local sum
    sum="$(ls -t "$outdir"/master_*_summary.json 2>/dev/null | head -1 || true)"
    if [ -n "$sum" ]; then cp "$sum" "$outdir/summary.json"
    else echo "   !! no summary (see $outdir/run.log)"; fi
  else
    echo "   !! run failed (see $outdir/run.log)"
  fi
  cat > "$outdir/meta.json" <<EOF
{"arm":"$arm","mode":"$mode","repeat":$rep,"count":$TP_COUNT,"credit_window":$CREDIT_WINDOW}
EOF
  sleep "$COOLDOWN"
}

[ -x "$EPICO" ] || { echo "epico CLI not found at $EPICO"; exit 1; }
mkdir -p "$RESULTS"
echo "Results -> $RESULTS"

echo "== warmup/build pass (discarded) =="
emit_yaml > "$BENCH_YAML"
"$EPICO" run --config "$BENCH_YAML" --project-root "$PROJECT_ROOT" --aot \
    --log-dir "$RESULTS/_build" >"$RESULTS/_build.log" 2>&1 \
    || { echo "build pass failed, see $RESULTS/_build.log"; exit 1; }
sleep "$COOLDOWN"

for arm in $ARMS; do
  for mode in $MODES; do
    r=1
    while [ "$r" -le "$REPEATS" ]; do
      run_one "$arm" "$mode" "$r" "$RESULTS/$arm/$mode/run$r"
      r=$((r+1))
    done
  done
done

rm -f "$BENCH_YAML"
echo
echo "Done. Analyze with:"
echo "  python3 $SCRIPT_DIR/analyze_decomp.py $RESULTS"
