#!/usr/bin/env bash
# bench_codec.sh — transport x codec A/B for the two-stage-min pipeline.
#
# Two independent axes, held otherwise constant (loadgen source, 1 replica/stage,
# credit_window, sensors):
#
#   TRANSPORT (spine that moves events between stages):
#     zeromq : ZMQ dispatchers — ingress + interior + egress over IPC/TCP   (default)
#     mpmc   : in-process crossbeam MPMC ring spine   (EPICO_INPROC_INGRESS=1)
#     spsc   : interior stage->stage hop on the FastFlow SPSC mesh
#              (EPICO_INPROC_INGRESS=1 EPICO_EDGE_IMPL=spsc)
#              NOTE: ingress/egress edges are MPMC by design in the current
#              runtime, so on this 2-stage pipeline "spsc" = SPSC on the single
#              relay->forward hop, MPMC on the source/sink boundary edges.
#
#   CODEC (event encoding on that spine):
#     json   : source_format=json,   event_format=json
#     binary : source_format=binary, event_format=binary
#
# The loadgen is the source for ALL transports (so latency is rate-controllable
# and the one ingestion socket is identical across arms). The mpmc/spsc spine
# replaces the agent-side dispatchers with rings; the loadgen->agent ingestion
# socket is the only socket left and is constant across all three.
#
# Two measurement arms per (transport,codec), because they can't share a config:
#   THROUGHPUT : profile=tp + blast  -> max sustained ev/s (bench_ts frozen;
#                latency NOT reported here).
#   LATENCY    : profile=steady at a fixed rate (same for every cell) -> valid
#                per-event bench_ts -> e2e p50/p99/p999 + per-stage serde p50.
#
# Each cell runs REPS times; parse_summary.py takes the median.
#
# Usage:
#   ./bench/bench_codec.sh
#   TRANSPORTS="zeromq mpmc spsc" CODECS="json binary" REPS=3 ./bench/bench_codec.sh
#   TRANSPORTS="mpmc spsc" CODECS="binary" LAT_RATE=12000 ./bench/bench_codec.sh
#   EDGE_CAP=1024 SPSC_RING_CAP=1024 ./bench/bench_codec.sh   # bigger in-proc buffers
#
# Requires: epico on PATH (run setup.sh first), python3.

set -euo pipefail

# -- knobs (env-overridable) --------------------------------------------------
TRANSPORTS=${TRANSPORTS:-"zeromq mpmc spsc"}
CODECS=${CODECS:-"json binary"}
REPS=${REPS:-3}
TP_COUNT=${TP_COUNT:-2000000}
LAT_RATE=${LAT_RATE:-30000}
LAT_DURATION=${LAT_DURATION:-20}
SENSORS=${SENSORS:-100}
PIN=${PIN:-}                     # optional taskset/numactl prefix (Linux)
EOS_DRAIN=${EOS_DRAIN:-3}
EDGE_CAP=${EDGE_CAP:-}           # optional EPICO_EDGE_CAP for in-proc arms
SPSC_RING_CAP=${SPSC_RING_CAP:-} # optional EPICO_SPSC_RING_CAP for spsc arm

INGRESS="ipc:///tmp/epico-minbench-push"

# -- locate this bench dir, and the target example dir ------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="${EXAMPLE_DIR:-${1:-}}"
if [[ -z "$EXAMPLE_DIR" ]]; then
    if [[ -f "$(dirname "$HERE")/pipeline.yaml" && -d "$(dirname "$HERE")/stages" ]]; then
        EXAMPLE_DIR="$(dirname "$HERE")"
    else
        for base in "$(dirname "$HERE")" "$HERE" "$PWD" "$(dirname "$HERE")/.."; do
            if [[ -f "$base/examples/two-stage-min/pipeline.yaml" ]]; then
                EXAMPLE_DIR="$(cd "$base/examples/two-stage-min" && pwd)"; break
            fi
        done
    fi
fi
if [[ -z "${EXAMPLE_DIR:-}" || ! -f "$EXAMPLE_DIR/pipeline.yaml" || ! -d "$EXAMPLE_DIR/stages" ]]; then
    echo "ERROR: couldn't locate the two-stage-min example (need pipeline.yaml + stages/)."
    echo "  This bench folder is at: $HERE"
    echo "  Fix: move bench/ into examples/two-stage-min/, or set EXAMPLE_DIR=/abs/path/to/examples/two-stage-min"
    exit 1
fi
EXAMPLE_DIR="$(cd "$EXAMPLE_DIR" && pwd)"

command -v epico   >/dev/null 2>&1 || { echo "ERROR: epico not on PATH — run setup.sh"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }

echo "==> example dir: $EXAMPLE_DIR"
cd "$EXAMPLE_DIR"

RESULTS="$HERE/results"
rm -rf "$RESULTS"; mkdir -p "$RESULTS"

cleanup_configs() { rm -f "$EXAMPLE_DIR"/.bench_*.yaml 2>/dev/null || true; }
trap cleanup_configs EXIT

cleanup_stale() {
    pkill -f 'release/master'        2>/dev/null || true
    pkill -f 'release/dispatcher'    2>/dev/null || true
    pkill -f 'release/epico-loadgen' 2>/dev/null || true
    sleep 0.3
    rm -f /tmp/epico-minbench-push /tmp/epico-dispatch-*relay* /tmp/epico-dispatch-*forward* 2>/dev/null || true
}

# -- transport -> extra agent env ---------------------------------------------
transport_env() {
    local base=""
    [[ -n "$EDGE_CAP"      ]] && base="$base EPICO_EDGE_CAP=$EDGE_CAP"
    [[ -n "$SPSC_RING_CAP" ]] && base="$base EPICO_SPSC_RING_CAP=$SPSC_RING_CAP"
    case "$1" in
        zeromq) echo "$base" ;;
        mpmc)   echo "EPICO_INPROC_INGRESS=1 EPICO_EDGE_IMPL=ring $base" ;;
        spsc)   echo "EPICO_INPROC_INGRESS=1 EPICO_EDGE_IMPL=spsc $base" ;;
        *)      echo "ERROR: unknown transport '$1'" >&2; exit 1 ;;
    esac
}

# -- config generator (codec determines YAML; transport is env, not YAML) -----
# gen_config <name> <profile> <blast> <rate> <duration> <count> <fmt> -> path
gen_config() {
    local name="$1" profile="$2" blast="$3" rate="$4" duration="$5" count="$6" fmt="$7"
    local out="$EXAMPLE_DIR/.bench_${name}.yaml"
    {
        cat <<YAML
package: epico:minbench@1.0.0
nodes:
  - name: local
    host: 127.0.0.1
    force_tcp: false
types:
  msg:
    sensor_id: string
    value:     f64
source:
  kind: loadgen
  profile: $profile
  sensors: $SENSORS
YAML
        if [[ "$profile" == "tp" ]]; then
            echo "  count: $count"
            [[ "$blast" == "true" ]] && echo "  blast: true"
        else
            echo "  rate: $rate"
            echo "  duration: $duration"
        fi
        cat <<YAML
stages:
  - name: relay
    placement: local
    in:  msg
    out: msg
    src: ./stages/relay.rs
    scaling: { min: 1, max: 1, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
  - name: forward
    placement: local
    in:  msg
    out: msg
    src: ./stages/forward.rs
    scaling: { min: 1, max: 1, queue_up: 50, cooldown_up_s: 20, cooldown_down_s: 20 }
edges:
  - relay -> forward
deploy:
  host: localhost
  this_host: local
  source_format: $fmt
  event_format: $fmt
  ingress: $INGRESS
  credit_window: 16
  batch_events: 1
  resource_sample_interval_ms: 0
YAML
    } > "$out"
    echo "$out"
}

# -- single run -> summary path (or empty) ------------------------------------
run_one() {
    local cfg="$1" logdir="$2" tenv="$3"
    rm -rf "$logdir"; mkdir -p "$logdir"
    cleanup_stale
    # shellcheck disable=SC2086
    env $tenv EPICO_EOS_DRAIN_SECS="$EOS_DRAIN" $PIN \
        epico run --no-build --config "$cfg" --log-dir "$logdir" >"$logdir/run.out" 2>&1 || true
    ls -t "$logdir"/master_*_summary.json 2>/dev/null | head -1
}

# -- build the wasm ONCE (codec- and transport-agnostic) ----------------------
echo "==> building stages once"
if ! epico build --config pipeline.yaml >"$RESULTS/build.out" 2>&1; then
    echo "ERROR: epico build failed — see $RESULTS/build.out"; tail -20 "$RESULTS/build.out"; exit 1
fi

echo "==> TRANSPORTS='$TRANSPORTS'  CODECS='$CODECS'  REPS=$REPS"
echo "==> TP_COUNT=$TP_COUNT  LAT_RATE=$LAT_RATE  LAT_DURATION=$LAT_DURATION  PIN='${PIN:-none}'"

# -- run the matrix -----------------------------------------------------------
for codec in $CODECS; do
    tp_cfg="$(gen_config  "tp_$codec"  tp     true  0          0             "$TP_COUNT" "$codec")"
    lat_cfg="$(gen_config "lat_$codec" steady false "$LAT_RATE" "$LAT_DURATION" 0         "$codec")"
    for transport in $TRANSPORTS; do
        tenv="$(transport_env "$transport")"
        echo ""
        echo "==> [$transport/$codec] THROUGHPUT (tp+blast)"
        for r in $(seq 1 "$REPS"); do
            printf '    rep %d/%d ... ' "$r" "$REPS"
            s="$(run_one "$tp_cfg" "$RESULTS/tp_${transport}_${codec}_$r" "$tenv")"
            if [[ -n "$s" ]]; then cp "$s" "$RESULTS/tp_${transport}_${codec}_$r.json"; echo "ok"; else echo "NO SUMMARY (see $RESULTS/tp_${transport}_${codec}_$r/run.out)"; fi
        done
        echo "==> [$transport/$codec] LATENCY (steady @ ${LAT_RATE})"
        for r in $(seq 1 "$REPS"); do
            printf '    rep %d/%d ... ' "$r" "$REPS"
            s="$(run_one "$lat_cfg" "$RESULTS/lat_${transport}_${codec}_$r" "$tenv")"
            if [[ -n "$s" ]]; then cp "$s" "$RESULTS/lat_${transport}_${codec}_$r.json"; echo "ok"; else echo "NO SUMMARY (see $RESULTS/lat_${transport}_${codec}_$r/run.out)"; fi
        done
    done
done

cleanup_stale
echo ""
python3 "$HERE/parse_summary.py" "$RESULTS" "$LAT_RATE"
echo ""
echo "==> raw summaries + per-run stdout in: $RESULTS"
echo "==> results table written to:        $RESULTS/results.csv"
