#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Epico 3-way transport throughput test on a MINIMAL 2-stage pipeline.
#
#   relay -> forward     (identity WASM stages, 2-field record)
#
# Arms (only the single interior hop relay->forward changes):
#   dispatcher : ZeroMQ PULL -> ROUTER/DEALER broker process   (baseline)
#   edge       : in-process MPMC crossbeam ring  (EPICO_INPROC_EDGES=1)
#   spsc       : FastFlow-style N x M SPSC mesh  (EPICO_EDGE_IMPL=spsc)
#
# Isolation: all arms share the SAME loadgen socket source, SAME ingress
# dispatcher, SAME egress collector. Do NOT switch to a native source: block
# here — a custom source forces inproc ingress in master.rs and the dispatcher
# arm would silently become an in-process run.
#
# Why minimal compute: with ~0 WASM work the per-worker ceiling is set by the
# fixed stage boundary (~serde), which is constant across arms, so the replica
# sweep exposes the TRANSPORT ceiling sooner and with less noise than the
# three-stage example. Caveat: with only one interior hop, the shared ingress
# can become the common bottleneck before the interior transport does — that
# shows up as all three arms plateauing at the same number with per-stage ev/s
# pinned at the ingress rate. blast=1 removes the loadgen serde cap; if arms
# still tie, check CPU of the ingress dispatcher during the run.
#
# SPSC constraint: the mesh is sized N x M from max_replicas and round-robins
# over fixed indices, so min==max is required — the sweep pins both to R.
# Capacity fairness: edge uses one shared ring of EDGE_CAP slots; spsc total
# in-flight is SPSC_RING_CAP * R * R. Defaults match your 3-stage harness
# (256 vs 64); override SPSC_RING_CAP if you want matched aggregate depth.
# ---------------------------------------------------------------------------
set -euo pipefail

# --- paths -----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
EXAMPLE_DIR="${EXAMPLE_DIR:-$PROJECT_ROOT/examples/two-stage-min}"
EPICO="${EPICO:-$PROJECT_ROOT/target/release/epico}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS="${RESULTS:-$PROJECT_ROOT/bench/results/min2_$STAMP}"
BENCH_YAML="$EXAMPLE_DIR/.bench_pipeline.yaml"

# --- knobs (override via env) ----------------------------------------------
REPEATS="${REPEATS:-3}"
TP_COUNT="${TP_COUNT:-3000000}"          # events per throughput run
SENSORS="${SENSORS:-100}"
COOLDOWN="${COOLDOWN:-8}"
ARMS="${ARMS:-dispatcher edge spsc}"

REPLICAS="${REPLICAS:-1 2 4}"            # workers per stage (min==max), swept
BLAST="${BLAST:-1}"                      # cached-blast source (kills loadgen serde cap)

EDGE_CAP="${EDGE_CAP:-256}"              # MPMC ring slots (shared)
SPSC_RING_CAP="${SPSC_RING_CAP:-64}"     # per-ring slots; total = cap * R * R
CREDIT_WINDOW="${CREDIT_WINDOW:-16}"     # dispatcher arm flow-control window
BATCH="${BATCH:-1}"

# Optional latency-vs-load pass (1 replica, no blast). Off by default since
# this harness is about the throughput ceiling. Enable with LATENCY=1.
LATENCY="${LATENCY:-0}"
STEADY_DUR="${STEADY_DUR:-30}"
RATES="${RATES:-10000 20000 30000 35000}"

# ---------------------------------------------------------------------------
emit_yaml() {  # profile rate count duration credit_window replicas blast
  local profile="$1" rate="$2" count="$3" dur="$4" cw="$5" reps="$6" blast="$7"
  local blast_line=""
  if [ "$blast" = "1" ] && [ "$profile" = "tp" ]; then
    blast_line="  blast: true"
  fi
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
  profile: $profile
  rate: $rate
  count: $count
  duration: $dur
  sensors: $SENSORS
$blast_line
stages:
  - name: relay
    placement: local
    in:  event
    out: event
    src: ./stages/relay.rs
    scaling: { min: $reps, max: $reps, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
  - name: forward
    placement: local
    in:  event
    out: event
    src: ./stages/forward.rs
    scaling: { min: $reps, max: $reps, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
edges:
  - relay -> forward
deploy:
  host: localhost
  this_host: local
  ingress: ipc:///tmp/epico-minbench-push
  credit_window: $cw
  batch_events: $BATCH
  resource_sample_interval_ms: 0
EOF
}

run_one() {  # arm profile rate count dur reps blast rep outdir
  local arm="$1" profile="$2" rate="$3" count="$4" dur="$5" reps="$6" blast="$7" rep="$8" outdir="$9"
  mkdir -p "$outdir"
  emit_yaml "$profile" "$rate" "$count" "$dur" "$CREDIT_WINDOW" "$reps" "$blast" > "$BENCH_YAML"

  local env_kv=()
  case "$arm" in
    edge) env_kv=(EPICO_INPROC_EDGES=1 "EPICO_EDGE_CAP=$EDGE_CAP") ;;
    spsc) env_kv=(EPICO_INPROC_EDGES=1 EPICO_EDGE_IMPL=spsc "EPICO_SPSC_RING_CAP=$SPSC_RING_CAP") ;;
    dispatcher) : ;;
  esac

  echo ">> $arm | $profile rate=$rate count=$count reps=$reps blast=$blast rep=$rep"
  if env ${env_kv[@]+"${env_kv[@]}"} \
        "$EPICO" run --config "$BENCH_YAML" --project-root "$PROJECT_ROOT" \
        --aot --no-build --log-dir "$outdir" >"$outdir/run.log" 2>&1; then
    local sum
    sum="$(ls -t "$outdir"/master_*_summary.json 2>/dev/null | head -1 || true)"
    if [ -n "$sum" ]; then cp "$sum" "$outdir/summary.json"
    else echo "   !! no summary (see $outdir/run.log)"; fi
  else
    echo "   !! run failed (see $outdir/run.log)"
  fi

  cat > "$outdir/meta.json" <<EOF
{"arm":"$arm","profile":"$profile","rate":$rate,"count":$count,"duration":$dur,"edge_cap":$EDGE_CAP,"spsc_ring_cap":$SPSC_RING_CAP,"credit_window":$CREDIT_WINDOW,"replicas":$reps,"blast":$blast,"repeat":$rep}
EOF
  sleep "$COOLDOWN"
}

# ---------------------------------------------------------------------------
[ -x "$EPICO" ] || { echo "epico CLI not found at $EPICO. Build: (cd $PROJECT_ROOT && ./setup.sh && cargo build --release)"; exit 1; }
[ -d "$EXAMPLE_DIR" ] || { echo "example dir not found: $EXAMPLE_DIR"; exit 1; }
mkdir -p "$RESULTS"
echo "Results -> $RESULTS"

# Build pass: compiles the two identity stages + agent once, discarded.
echo "== warmup/build pass (discarded) =="
emit_yaml tp 1000 50000 0 "$CREDIT_WINDOW" 1 0 > "$BENCH_YAML"
"$EPICO" run --config "$BENCH_YAML" --project-root "$PROJECT_ROOT" --aot \
    --log-dir "$RESULTS/_build" >"$RESULTS/_build.log" 2>&1 \
    || { echo "build pass failed, see $RESULTS/_build.log"; exit 1; }
sleep "$COOLDOWN"

echo "== throughput ceiling: 3 arms x replica sweep (blast=$BLAST) =="
for arm in $ARMS; do
  for reps in $REPLICAS; do
    r=1
    while [ "$r" -le "$REPEATS" ]; do
      run_one "$arm" tp 0 "$TP_COUNT" 0 "$reps" "$BLAST" "$r" \
        "$RESULTS/throughput/$arm/rep_count$reps/run$r"
      r=$((r+1))
    done
  done
done

if [ "$LATENCY" = "1" ]; then
  echo "== latency-vs-load (1 replica, no blast) =="
  for arm in $ARMS; do
    for rate in $RATES; do
      r=1
      while [ "$r" -le "$REPEATS" ]; do
        run_one "$arm" steady "$rate" 0 "$STEADY_DUR" 1 0 "$r" \
          "$RESULTS/latency/$arm/rate$rate/run$r"
        r=$((r+1))
      done
    done
  done
fi

rm -f "$BENCH_YAML"
echo
echo "Done. Analyze with:"
echo "  python3 $SCRIPT_DIR/analyze_ab.py $RESULTS"
