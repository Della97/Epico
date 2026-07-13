#!/usr/bin/env bash
# run_experiment.sh
#
# Three-way comparison: serverfull vs serverless-AOT vs serverless-JIT.
#
# Serverfull replica counts (min == max, grid-search winner sf_n5_d5_f6):
#   normalize → 5   detect → 5   finalize → 6
#
# Serverless scaling params (grid-search winner sl_n5_d5_f6_qu150_cd5):
#   min=0, max as above
#   queue_up=150  queue_down=15  cooldown_up_s=0  cooldown_down_s=5
#
# Output layout:
#   data/serverfull/run01_<timestamp>_summary.json
#   data/serverless_aot/run01_<timestamp>_summary.json
#   data/serverless_jit/run01_<timestamp>_summary.json
#
# Usage:
#   cd publications/vhpc/serverfull-vs-serverless
#   bash run_experiment.sh [--runs N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

PIPELINE_DIR="${REPO_ROOT}/examples/three-stage-test"
PIPELINE_YAML="${PIPELINE_DIR}/pipeline.yaml"
PIPELINE_SERVERFULL="${PIPELINE_DIR}/pipeline_serverfull.yaml"
PIPELINE_SERVERLESS="${PIPELINE_DIR}/pipeline_serverless.yaml"
LOGS_DIR="${PIPELINE_DIR}/logs"

SERVERFULL_OUT="${SCRIPT_DIR}/data/serverfull"
SERVERLESS_AOT_OUT="${SCRIPT_DIR}/data/serverless_aot"
SERVERLESS_JIT_OUT="${SCRIPT_DIR}/data/serverless_jit"

RUNS=20

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

command -v epico >/dev/null 2>&1 || { echo "ERROR: epico not in PATH" >&2; exit 1; }
[[ -f "${PIPELINE_YAML}" ]] || { echo "ERROR: pipeline not found: ${PIPELINE_YAML}" >&2; exit 1; }

mkdir -p "${SERVERFULL_OUT}" "${SERVERLESS_AOT_OUT}" "${SERVERLESS_JIT_OUT}" "${LOGS_DIR}"

log() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }

# ---------------------------------------------------------------------------
# Build serverfull pipeline config.
#   normalize → min=max=5
#   detect    → min=max=5
#   finalize  → min=max=6
# ---------------------------------------------------------------------------
build_serverfull_yaml() {
  local src="$1" dst="$2"
  python3 - "${src}" "${dst}" <<'PYEOF'
import sys, re

src, dst = sys.argv[1], sys.argv[2]

STAGE_REPLICAS = {
    "normalize": 5,
    "detect":    5,
    "finalize":  6,
}

with open(src) as f:
    lines = f.readlines()

current_stage = None
in_scaling    = False
result        = []

for line in lines:
    stripped = line.strip()

    m = re.match(r'^\s*-\s+name:\s+(\w+)', line)
    if m:
        current_stage = m.group(1)
        in_scaling    = False
        result.append(line)
        continue

    if stripped == "scaling:":
        in_scaling = True
        result.append(line)
        continue

    if in_scaling and current_stage in STAGE_REPLICAS:
        r = STAGE_REPLICAS[current_stage]
        m_min = re.match(r'^(\s+min:\s*)\d+', line)
        m_max = re.match(r'^(\s+max:\s*)\d+', line)
        if m_min:
            line = m_min.group(1) + str(r) + "\n"
        elif m_max:
            line = m_max.group(1) + str(r) + "\n"

    if in_scaling and stripped and not stripped.startswith('#'):
        indent = len(line) - len(line.lstrip())
        if indent <= 4 and stripped not in ("scaling:",):
            in_scaling = False

    result.append(line)

with open(dst, "w") as f:
    f.writelines(result)

print(f"Created serverfull config: {dst}")
for stage, r in STAGE_REPLICAS.items():
    print(f"  {stage}: min=max={r}")
PYEOF
}

# ---------------------------------------------------------------------------
# Build serverless pipeline config.
#   min=0, max per stage, uniform autoscaling params from grid search winner.
#   normalize → max=5   detect → max=5   finalize → max=6
#   queue_up=150  queue_down=15  cooldown_up_s=0  cooldown_down_s=5
# ---------------------------------------------------------------------------
build_serverless_yaml() {
  local src="$1" dst="$2"
  python3 - "${src}" "${dst}" <<'PYEOF'
import sys, re

src, dst = sys.argv[1], sys.argv[2]

STAGE_MAX = {
    "normalize": 5,
    "detect":    5,
    "finalize":  6,
}
QUEUE_UP       = 150
QUEUE_DOWN     = 15
COOLDOWN_UP_S  = 0
COOLDOWN_DOWN_S = 5

with open(src) as f:
    lines = f.readlines()

current_stage = None
in_scaling    = False
result        = []

for line in lines:
    stripped = line.strip()

    m = re.match(r'^\s*-\s+name:\s+(\w+)', line)
    if m:
        current_stage = m.group(1)
        in_scaling    = False
        result.append(line)
        continue

    if stripped == "scaling:":
        in_scaling = True
        result.append(line)
        continue

    if in_scaling and current_stage in STAGE_MAX:
        m_min  = re.match(r'^(\s+min:\s*)\d+', line)
        m_max  = re.match(r'^(\s+max:\s*)\d+', line)
        m_qup  = re.match(r'^(\s+queue_up:\s*)\d+', line)
        m_qdn  = re.match(r'^(\s+queue_down:\s*)\d+', line)
        m_cdup = re.match(r'^(\s+cooldown_up_s:\s*)[\d.]+', line)
        m_cddn = re.match(r'^(\s+cooldown_down_s:\s*)[\d.]+', line)

        if m_min:
            line = m_min.group(1) + "0\n"
        elif m_max:
            line = m_max.group(1) + str(STAGE_MAX[current_stage]) + "\n"
        elif m_qup:
            line = m_qup.group(1) + str(QUEUE_UP) + "\n"
        elif m_qdn:
            line = m_qdn.group(1) + str(QUEUE_DOWN) + "\n"
        elif m_cdup:
            line = m_cdup.group(1) + str(COOLDOWN_UP_S) + "\n"
        elif m_cddn:
            line = m_cddn.group(1) + str(COOLDOWN_DOWN_S) + "\n"

    if in_scaling and stripped and not stripped.startswith('#'):
        indent = len(line) - len(line.lstrip())
        if indent <= 4 and stripped not in ("scaling:",):
            in_scaling = False

    result.append(line)

with open(dst, "w") as f:
    f.writelines(result)

print(f"Created serverless config: {dst}")
for stage, mx in STAGE_MAX.items():
    print(f"  {stage}: min=0 max={mx} queue_up={QUEUE_UP} queue_down={QUEUE_DOWN}"
          f" cooldown_up={COOLDOWN_UP_S}s cooldown_down={COOLDOWN_DOWN_S}s")
PYEOF
}

# ---------------------------------------------------------------------------
# Find the new master_*_summary.json produced since a snapshot.
# ---------------------------------------------------------------------------
snapshot_summaries() {
  find "${LOGS_DIR}" -maxdepth 1 -name 'master_*_summary.json' -type f 2>/dev/null | sort
}

find_new_summary() {
  local before_file="$1"
  local after
  after=$(snapshot_summaries)
  comm -13 "${before_file}" <(printf '%s\n' "${after}")
}

# ---------------------------------------------------------------------------
# Run one pipeline execution and save the summary.
# ---------------------------------------------------------------------------
run_one() {
  local label="$1"
  local config="$2"
  local output_dir="$3"
  local compile="$4"   # "aot" or "jit"
  local iter="$5"

  log "  [${iter}/${RUNS}] ${label} (${compile})"

  local before_file
  before_file=$(mktemp)
  snapshot_summaries > "${before_file}"

  pushd "${PIPELINE_DIR}" > /dev/null
  epico run "--${compile}" --config "$(basename "${config}")"
  popd > /dev/null

  local new_file
  new_file=$(find_new_summary "${before_file}")
  rm -f "${before_file}"

  if [[ -z "${new_file}" ]]; then
    log "    WARN: no new summary found for run ${iter}, skipping"
    return 1
  fi

  if [[ $(printf '%s\n' "${new_file}" | wc -l) -gt 1 ]]; then
    new_file=$(printf '%s\n' "${new_file}" | sort | tail -n 1)
  fi

  local dest="${output_dir}/run$(printf '%02d' "${iter}")_$(date +%Y%m%d_%H%M%S)_summary.json"
  cp "${new_file}" "${dest}"
  log "    -> $(basename "${dest}")"
}

# ---------------------------------------------------------------------------
# Run a full batch of RUNS executions.
# ---------------------------------------------------------------------------
run_batch() {
  local label="$1"
  local config="$2"
  local output_dir="$3"
  local compile="$4"   # "aot" or "jit"

  log "======================================="
  log " ${label}  |  ${RUNS} runs  |  ${compile}"
  log "======================================="

  for i in $(seq 1 "${RUNS}"); do
    run_one "${label}" "${config}" "${output_dir}" "${compile}" "${i}" || true
  done

  log " ${label}: done — $(ls "${output_dir}"/*.json 2>/dev/null | wc -l) summaries collected"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

log "repo root      : ${REPO_ROOT}"
log "runs each      : ${RUNS}"
log "serverfull     : normalize=5  detect=5  finalize=6  (min=max)"
log "serverless     : normalize=5  detect=5  finalize=6  (min=0, qu150, cd5)"
log ""

# ---------------------------------------------------------------------------
# Bootstrap the agent runtime with cold-start-opt enabled.
#
# This compiles the agent (master) binary with:
#   - cranelift_opt_level = Speed   (full optimization)
#   - parallel_compilation = true   (concurrent per-function JIT)
#   - memory_init_cow = true        (CoW for linear memory init)
#   - Pooling allocator             (pre-allocated instance slots)
#
# Using the same engine for all three batches (serverfull, AOT, JIT) keeps
# the comparison fair: the only variable is whether Cranelift runs at
# cold-start (JIT) or is pre-serialised (AOT).  Without this flag JIT
# compiles single-threaded with OptLevel::None and takes ~29 ms instead of
# ~10 ms — matching what the paper measured.
# ---------------------------------------------------------------------------
log "==> Bootstrapping runtime (cold-start-opt enabled)..."
pushd "${PIPELINE_DIR}" > /dev/null
epico bootstrap --cold-start-opt
popd > /dev/null
log ""

build_serverfull_yaml "${PIPELINE_YAML}" "${PIPELINE_SERVERFULL}"
build_serverless_yaml "${PIPELINE_YAML}" "${PIPELINE_SERVERLESS}"

log ""

# Serverfull — AOT only (fixed replicas, compilation mode irrelevant for scaling)
run_batch "serverfull"     "${PIPELINE_SERVERFULL}" "${SERVERFULL_OUT}"     "aot"

# Serverless — AOT
run_batch "serverless-aot" "${PIPELINE_SERVERLESS}" "${SERVERLESS_AOT_OUT}" "aot"

# Serverless — JIT
run_batch "serverless-jit" "${PIPELINE_SERVERLESS}" "${SERVERLESS_JIT_OUT}" "jit"

rm -f "${PIPELINE_SERVERFULL}" "${PIPELINE_SERVERLESS}"

log ""
log "All done."
log "  serverfull     : ${SERVERFULL_OUT}"
log "  serverless-aot : ${SERVERLESS_AOT_OUT}"
log "  serverless-jit : ${SERVERLESS_JIT_OUT}"
log ""
log "Plot: python3 plot_cdf.py"
