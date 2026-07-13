#!/usr/bin/env bash
set -euo pipefail

# Runs the three-stage pipeline with a 600s wave profile:
# - duration: 600
# - max rate: 10000
# - min wave rate: 2000
# - wave period: 120s
#
# Notes:
# - This loadgen does not use --wave-start or --wave-warmup.
# - -p is not a supported loadgen flag in this repo.
# - sensors/parallel tuning is intentionally left unchanged here.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PIPELINE="${SCRIPT_DIR}/pipeline.yaml"

if [[ -x "${REPO_ROOT}/target/release/metapod" ]]; then
  METAPOD_BIN="${REPO_ROOT}/target/release/metapod"
elif command -v metapod >/dev/null 2>&1; then
  METAPOD_BIN="$(command -v metapod)"
else
  echo "ERROR: metapod-cli binary not found (expected ./target/release/metapod or metapod in PATH)" >&2
  exit 1
fi

RUN_TAG="wave600_$(date +%Y%m%d_%H%M%S)"
LOG_DIR="${SCRIPT_DIR}/logs/${RUN_TAG}"
OUT_DIR="${SCRIPT_DIR}/logs/${RUN_TAG}/timeseries"
mkdir -p "${LOG_DIR}" "${OUT_DIR}"

echo "Running pipeline: ${PIPELINE}"
echo "Log dir: ${LOG_DIR}"
echo "CLI: ${METAPOD_BIN}"

echo "Starting metapod run (this will take about 600 seconds)..."
"${METAPOD_BIN}" run --config "${PIPELINE}" --project-root "${REPO_ROOT}" --log-dir "${LOG_DIR}"

SUMMARY_JSON="$(find "${LOG_DIR}" -maxdepth 1 -type f -name 'agent_*_summary.json' | sort | tail -n 1)"
LOADGEN_JSONL="$(find "${LOG_DIR}" -maxdepth 1 -type f -name 'loadgen_*.jsonl' | sort | tail -n 1)"

if [[ -z "${SUMMARY_JSON}" ]]; then
  echo "ERROR: no agent summary JSON found in ${LOG_DIR}" >&2
  exit 1
fi
if [[ -z "${LOADGEN_JSONL}" ]]; then
  echo "ERROR: no loadgen JSONL found in ${LOG_DIR}" >&2
  exit 1
fi

python3 "${SCRIPT_DIR}/extract_timeseries.py" \
  --summary "${SUMMARY_JSON}" \
  --loadgen-jsonl "${LOADGEN_JSONL}" \
  --out-dir "${OUT_DIR}"

echo "Done. Key outputs:"
echo "  ${SUMMARY_JSON}"
echo "  ${LOADGEN_JSONL}"
echo "  ${OUT_DIR}/cpu_memory_timeseries.csv"
echo "  ${OUT_DIR}/loadgen_timeseries.csv"
echo "  ${OUT_DIR}/run_overview.json"
