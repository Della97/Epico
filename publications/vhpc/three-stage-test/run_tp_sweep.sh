#!/usr/bin/env bash
# run_tp_sweep.sh
#
# Runs 20 tp-profile repetitions for each credit_window (1, 8, 16, 32)
# on both the three-stage-test (simple) and complex-test pipelines.
#
# Output layout:
#   tp-logs/simple-1/    20 × agent_*_summary.json
#   tp-logs/simple-8/    ...
#   tp-logs/simple-16/
#   tp-logs/simple-32/
#   tp-logs/complex-1/
#   tp-logs/complex-8/
#   tp-logs/complex-16/
#   tp-logs/complex-32/
#
# Usage:
#   cd examples/three-stage-test
#   bash run_tp_sweep.sh [--runs N]   # default: 20

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SIMPLE_PIPELINE="${SCRIPT_DIR}/pipeline.yaml"
COMPLEX_PIPELINE="${REPO_ROOT}/examples/complex-test/pipeline.yaml"

TP_LOGS="${SCRIPT_DIR}/tp-logs"

RUNS=20

# Parse optional --runs argument
while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Locate metapod binary
if [[ -x "${REPO_ROOT}/target/release/metapod" ]]; then
  METAPOD="${REPO_ROOT}/target/release/metapod"
elif command -v metapod >/dev/null 2>&1; then
  METAPOD="$(command -v metapod)"
else
  echo "ERROR: metapod binary not found (expected target/release/metapod or metapod in PATH)" >&2
  exit 1
fi

echo "metapod : ${METAPOD}"
echo "runs    : ${RUNS}"
echo "output  : ${TP_LOGS}"
echo ""

# ---------------------------------------------------------------------------
# Helper: set credit_window in a pipeline.yaml (in-place, no temp file)
# ---------------------------------------------------------------------------
set_credit_window() {
  local yaml="$1"
  local cw="$2"
  # Replace the first occurrence of `credit_window: <number>`
  sed -i "s/credit_window: [0-9]*/credit_window: ${cw}/" "${yaml}"
}

# ---------------------------------------------------------------------------
# Helper: run one batch of $RUNS repetitions
#   $1  pipeline yaml
#   $2  output directory  (tp-logs/<label>/)
# ---------------------------------------------------------------------------
run_batch() {
  local pipeline="$1"
  local out_dir="$2"
  mkdir -p "${out_dir}"

  for i in $(seq 1 "${RUNS}"); do
    local tag
    tag="run$(printf '%02d' "${i}")_$(date +%Y%m%d_%H%M%S)"
    local tmp_log="${out_dir}/.tmp_${tag}"
    mkdir -p "${tmp_log}"

    echo "  [${i}/${RUNS}] tag=${tag}"
    "${METAPOD}" run \
      --config    "${pipeline}" \
      --project-root "${REPO_ROOT}" \
      --log-dir   "${tmp_log}" \
      --aot

    # Copy only the agent summary JSON into the flat output dir
    local summary
    summary="$(find "${tmp_log}" -maxdepth 1 -name 'agent_*_summary.json' | sort | tail -n 1)"
    if [[ -z "${summary}" ]]; then
      echo "  WARNING: no summary JSON found for run ${i}, skipping" >&2
    else
      cp "${summary}" "${out_dir}/agent_${tag}_summary.json"
      echo "    -> saved $(basename "${out_dir}/agent_${tag}_summary.json")"
    fi

    # Remove the temp log tree to save disk space
    rm -rf "${tmp_log}"
  done
}

# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------
for CW in 1 8 16 32; do
  # --- Simple (three-stage-test) ---
  echo "========================================="
  echo " simple  |  credit_window=${CW}  |  ${RUNS} runs"
  echo "========================================="
  set_credit_window "${SIMPLE_PIPELINE}" "${CW}"
  run_batch "${SIMPLE_PIPELINE}" "${TP_LOGS}/simple-${CW}"

  # --- Complex ---
  echo "========================================="
  echo " complex |  credit_window=${CW}  |  ${RUNS} runs"
  echo "========================================="
  set_credit_window "${COMPLEX_PIPELINE}" "${CW}"
  run_batch "${COMPLEX_PIPELINE}" "${TP_LOGS}/complex-${CW}"
done

# Restore both yamls to credit_window=16 (neutral default) when done
set_credit_window "${SIMPLE_PIPELINE}"  16
set_credit_window "${COMPLEX_PIPELINE}" 16

echo ""
echo "All done.  Results in: ${TP_LOGS}"
