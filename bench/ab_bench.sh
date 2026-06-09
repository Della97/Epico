#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Epico A/B: ZeroMQ dispatcher  vs  in-process MPMC edge.
#
# Isolation: both arms use the SAME external loadgen source, the SAME ingress
# dispatcher, and the SAME egress collector socket. Only the interior
# stage->stage hop changes (dispatcher arm: ZMQ; edge arm: EPICO_INPROC_EDGES=1).
#
# Two experiments:
#   throughput : max SUSTAINED ev/s. profile=tp. Two things let us find the
#                real ceiling rather than an artifact:
#                  (a) --blast source (BLAST=1): the loadgen resends one cached
#                      buffer, so its single thread can outpace the transport
#                      instead of capping it at ~60k (its per-event serde rate).
#                  (b) a REPLICAS sweep: with 1 worker/stage the per-stage serde
#                      (~15us) caps throughput before the transport does, so we
#                      pin min==max==R and sweep R until throughput plateaus.
#                      The plateau is the transport's sustained ceiling (or the
#                      core count -- on the M4 you'll likely hit cores first,
#                      which is itself a valid result).
#                Read throughput.sustained_eps_warmup_trimmed; confirm
#                throughput.per_stage_eps are balanced and loadgen dropped ~0.
#   latency    : e2e p50/p99 vs offered load. profile=steady, BLAST OFF
#                (steady needs fresh bench_ts), REPLICAS fixed at 1. Set RATES
#                BELOW the dispatcher's saturation so every point is matched.
#
# macOS: AC power, close other apps. Numbers are directional; the paper numbers
# come from n16 / titanic where you can pin cores and add more of them.
# ---------------------------------------------------------------------------
set -euo pipefail

# --- paths -----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
EXAMPLE_DIR="${EXAMPLE_DIR:-$PROJECT_ROOT/examples/three-stage-test}"
EPICO="${EPICO:-$PROJECT_ROOT/target/release/epico}"
STAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS="${RESULTS:-$PROJECT_ROOT/bench/results/$STAMP}"
BENCH_YAML="$EXAMPLE_DIR/.bench_pipeline.yaml"

# --- knobs (override via env) ----------------------------------------------
REPEATS="${REPEATS:-3}"
TP_COUNT="${TP_COUNT:-3000000}"         # events per throughput run
STEADY_DUR="${STEADY_DUR:-30}"          # seconds per latency run
SENSORS="${SENSORS:-100}"
COOLDOWN="${COOLDOWN:-8}"
ARMS="${ARMS:-dispatcher edge spsc}"

# throughput: saturate via parallelism. Pinned min==max==R, swept.
REPLICAS="${REPLICAS:-1 2 3}"           # workers per stage to sweep
BLAST="${BLAST:-1}"                     # 1 = cached-blast source (removes loadgen cap)

# fairness knobs (held equal so this measures transport, not buffer depth)
EDGE_CAPS="${EDGE_CAPS:-256}"
SPSC_RING_CAP="${SPSC_RING_CAP:-64}"   # per-ring cap for spsc mesh (total in-flight = cap * N * M)
CREDIT_WINDOWS="${CREDIT_WINDOWS:-16}"
BATCH="${BATCH:-1}"

# latency: rates BELOW dispatcher saturation. 1 replica, no blast.
RATES="${RATES:-10000 20000 30000 35000}"

# ---------------------------------------------------------------------------
emit_yaml() {  # profile rate count duration credit_window replicas blast
  local profile="$1" rate="$2" count="$3" dur="$4" cw="$5" reps="$6" blast="$7"
  local blast_line=""
  if [ "$blast" = "1" ] && [ "$profile" = "tp" ]; then
    blast_line="  blast: true"
  fi
  cat <<EOF
package: epico:temps@1.0.0
nodes:
  - name: local
    host: 127.0.0.1
    force_tcp: false
types:
  reading:
    sensor_id: string
    value:     f64
    unit:      string?
  enriched:
    sensor_id: string
    value:     f64
    anomaly:   bool?
    severity:  string?
source:
  kind: loadgen
  profile: $profile
  rate: $rate
  count: $count
  duration: $dur
  sensors: $SENSORS
$blast_line
stages:
  - name: normalize
    placement: local
    in:  reading
    out: reading
    src: ./stages/normalize.rs
    scaling: { min: $reps, max: $reps, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
  - name: detect
    placement: local
    in:  reading
    out: enriched
    src: ./stages/detect.rs
    scaling: { min: $reps, max: $reps, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
  - name: finalize
    placement: local
    in:  enriched
    out: enriched
    src: ./stages/finalize.rs
    scaling: { min: $reps, max: $reps, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
edges:
  - normalize -> detect
  - detect -> finalize
deploy:
  host: localhost
  this_host: local
  ingress: ipc:///tmp/epico-bench-push
  credit_window: $cw
  batch_events: $BATCH
  resource_sample_interval_ms: 0
EOF
}

run_one() {  # arm profile rate count dur cap cw reps blast rep outdir
  local arm="$1" profile="$2" rate="$3" count="$4" dur="$5" cap="$6" cw="$7" reps="$8" blast="$9" rep="${10}" outdir="${11}"
  mkdir -p "$outdir"
  emit_yaml "$profile" "$rate" "$count" "$dur" "$cw" "$reps" "$blast" > "$BENCH_YAML"

  local env_kv=()
  case "$arm" in
    edge) env_kv=(EPICO_INPROC_EDGES=1 "EPICO_EDGE_CAP=$cap") ;;
    spsc) env_kv=(EPICO_INPROC_EDGES=1 EPICO_EDGE_IMPL=spsc "EPICO_SPSC_RING_CAP=$SPSC_RING_CAP") ;;
    dispatcher) : ;;
  esac

  echo ">> $arm | $profile rate=$rate count=$count reps=$reps blast=$blast cap=$cap cw=$cw rep=$rep"
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
{"arm":"$arm","profile":"$profile","rate":$rate,"count":$count,"duration":$dur,"edge_cap":$cap,"credit_window":$cw,"replicas":$reps,"blast":$blast,"repeat":$rep}
EOF
  sleep "$COOLDOWN"
}

# ---------------------------------------------------------------------------
[ -x "$EPICO" ] || { echo "epico CLI not found at $EPICO. Build: (cd $PROJECT_ROOT && ./setup.sh && cargo build --release)"; exit 1; }
mkdir -p "$RESULTS"
echo "Results -> $RESULTS"

echo "== warmup/build pass (discarded) =="
emit_yaml tp 1000 50000 1 16 1 0 > "$BENCH_YAML"
"$EPICO" run --config "$BENCH_YAML" --project-root "$PROJECT_ROOT" --aot \
    --log-dir "$RESULTS/_build" >"$RESULTS/_build.log" 2>&1 \
    || { echo "build pass failed, see $RESULTS/_build.log"; exit 1; }
sleep "$COOLDOWN"

# Experiment 1: max sustained throughput -- replica sweep, blast source.
echo "== throughput experiment (replica sweep, blast=$BLAST) =="
cap0="$(echo $EDGE_CAPS | awk '{print $1}')"
cw0="$(echo $CREDIT_WINDOWS | awk '{print $1}')"
for arm in $ARMS; do
  for reps in $REPLICAS; do
    r=1
    while [ "$r" -le "$REPEATS" ]; do
      run_one "$arm" tp 0 "$TP_COUNT" 0 "$cap0" "$cw0" "$reps" "$BLAST" "$r" \
        "$RESULTS/throughput/$arm/rep_count$reps/run$r"
      r=$((r+1))
    done
  done
done

# Experiment 2: latency vs offered load -- 1 replica, no blast.
echo "== latency-vs-load experiment =="
for arm in $ARMS; do
  for rate in $RATES; do
    r=1
    while [ "$r" -le "$REPEATS" ]; do
      run_one "$arm" steady "$rate" 0 "$STEADY_DUR" "$cap0" "$cw0" 1 0 "$r" \
        "$RESULTS/latency/$arm/rate$rate/run$r"
      r=$((r+1))
    done
  done
done

rm -f "$BENCH_YAML"
echo
echo "Done. Analyze with:"
echo "  python3 $SCRIPT_DIR/analyze_ab.py $RESULTS"