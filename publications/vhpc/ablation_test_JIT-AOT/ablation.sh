#!/usr/bin/env bash
#
# Epico cold-start ablation harness: AOT vs JIT.
#
# Runs `epico run --jit` and `epico run --aot` N times each (interleaved
# to reduce thermal-drift bias) for both the simple and complex pipelines,
# and collects each run's summary JSON into per-pipeline subdirectories.
#
# Run from anywhere; paths are absolute.
# Override repo root:  EPICO_ROOT=/path/to/repo ./ablation.sh
# Skip cache drop:     SKIP_PURGE=1 ./ablation.sh

set -euo pipefail

# ---- Config -----------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${EPICO_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"

SIMPLE_DIR="${REPO_ROOT}/examples/three-stage-test"
COMPLEX_DIR="${REPO_ROOT}/publications/vhpc/complex-test"

SIMPLE_OUT="${SCRIPT_DIR}/simple"
COMPLEX_OUT="${SCRIPT_DIR}/complex"

RUNS_PER_MODE=20
COOLDOWN_SECONDS=8        # idle gap after purge, lets the SoC cool a bit
RUN_TIMEOUT_SECONDS=200   # safety net if a run hangs

AGENT_BIN="${REPO_ROOT}/target/release/master"
DISPATCHER_BIN="${REPO_ROOT}/target/release/dispatcher"

# ---- Helpers ----------------------------------------------------------------

log() {
    printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}

# Snapshot the set of summary JSONs in a logs/ dir before a run, so we can
# find the new one afterwards by set-difference.
snapshot_summaries() {
    local logs_dir="$1"
    find "${logs_dir}" -maxdepth 1 -name 'agent_*_summary.json' -type f 2>/dev/null | sort
}

# Find the single new summary file produced since the snapshot.
find_new_summary() {
    local before_file="$1"
    local logs_dir="$2"
    local after
    after=$(snapshot_summaries "${logs_dir}")
    comm -13 "${before_file}" <(printf '%s\n' "${after}")
}

run_one() {
    local label="$1"       # "simple" or "complex"
    local pipeline_dir="$2"
    local logs_dir="$3"
    local output_dir="$4"
    local mode="$5"        # "aot" or "jit"
    local iter="$6"        # 1..N
    local mode_upper
    mode_upper=$(printf '%s' "${mode}" | tr '[:lower:]' '[:upper:]')

    log "=== [${label}] ${mode_upper} run ${iter}/${RUNS_PER_MODE} ==="

    # 1) Drop file-backed page cache so the .cwasm mmap re-faults from disk.
    if [[ "${SKIP_PURGE:-0}" == "1" ]]; then
        log "skipping page cache purge (SKIP_PURGE=1)"
    elif command -v purge >/dev/null 2>&1 && sudo -n purge 2>/dev/null; then
        log "page cache purged (macOS purge)"
    elif [[ -w /proc/sys/vm/drop_caches ]]; then
        sync && echo 3 > /proc/sys/vm/drop_caches
        log "page cache dropped (Linux drop_caches)"
    elif sudo -n sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
        log "page cache dropped (Linux drop_caches via sudo)"
    else
        log "WARN: cannot drop page cache (no sudo, no purge) — results may include warm-cache effects"
    fi

    # 2) For JIT specifically, delete any cached .cwasm so the deferred-compile
    #    path actually runs. Without this, run 2+ would reuse run 1's .cwasm.
    if [[ "${mode}" == "jit" ]]; then
        log "removing cached .cwasm artifacts for clean JIT measurement..."
        find "${pipeline_dir}" -name '*.cwasm' -type f -delete 2>/dev/null || true
    fi

    # 3) Brief idle gap so thermal state and scheduler are quiet.
    log "cooldown ${COOLDOWN_SECONDS}s..."
    sleep "${COOLDOWN_SECONDS}"

    # 4) Snapshot summary files before the run.
    local before_file
    before_file=$(mktemp)
    snapshot_summaries "${logs_dir}" > "${before_file}"

    # 5) Run the pipeline via epico run.
    log "running: epico run --${mode}"
    pushd "${pipeline_dir}" > /dev/null
    epico run --"${mode}" || { log "ERROR: epico run --${mode} failed"; popd > /dev/null; rm -f "${before_file}"; return 1; }
    popd > /dev/null

    # 6) Locate the new summary file and rename/move it.
    local new_file
    new_file=$(find_new_summary "${before_file}" "${logs_dir}")
    rm -f "${before_file}"

    if [[ -z "${new_file}" ]]; then
        log "WARN: no new summary file found for [${label}] ${mode_upper} run ${iter}; skipping rename"
        return 1
    fi

    if [[ $(printf '%s\n' "${new_file}" | wc -l) -gt 1 ]]; then
        log "WARN: multiple new summary files found, picking newest:"
        printf '%s\n' "${new_file}" | sed 's/^/    /' >&2
        # stat format differs: Linux uses -c '%Y %n', macOS uses -f '%m %N'
        if stat --version 2>/dev/null | grep -q GNU; then
            new_file=$(printf '%s\n' "${new_file}" \
                | xargs -I{} stat -c '%Y %n' "{}" \
                | sort -n | tail -1 | cut -d' ' -f2-)
        else
            new_file=$(printf '%s\n' "${new_file}" \
                | xargs -I{} stat -f '%m %N' "{}" \
                | sort -n | tail -1 | cut -d' ' -f2-)
        fi
    fi

    local base dest
    base=$(basename "${new_file}" .json)
    dest="${output_dir}/${base}-${mode_upper}-${iter}.json"

    mv "${new_file}" "${dest}"
    log "saved -> ${dest}"
}

run_pipeline() {
    local label="$1"
    local pipeline_dir="$2"
    local output_dir="$3"
    local logs_dir="${pipeline_dir}/logs"

    log "### Starting ablation for [${label}]: ${pipeline_dir}"

    [[ -d "${pipeline_dir}" ]] || die "pipeline dir not found: ${pipeline_dir}"
    [[ -d "${logs_dir}"     ]] || die "logs dir not found: ${logs_dir}"
    mkdir -p "${output_dir}"

    # Interleave AOT and JIT to spread thermal drift across both modes evenly.
    for ((i=1; i<=RUNS_PER_MODE; i++)); do
        run_one "${label}" "${pipeline_dir}" "${logs_dir}" "${output_dir}" aot "${i}" || true
        run_one "${label}" "${pipeline_dir}" "${logs_dir}" "${output_dir}" jit "${i}" || true
    done

    log "### Done with [${label}]"
}

# ---- Main -------------------------------------------------------------------

command -v epico >/dev/null 2>&1 || die "epico not in PATH"
[[ -x "${AGENT_BIN}" ]]      || die "agent binary not found: ${AGENT_BIN} (run: cd ${REPO_ROOT} && cargo build --release -p master -p dispatcher)"
[[ -x "${DISPATCHER_BIN}" ]] || die "dispatcher binary not found: ${DISPATCHER_BIN}"

log "starting ablation: ${RUNS_PER_MODE} JIT + ${RUNS_PER_MODE} AOT runs × 2 pipelines"
log "simple  output: ${SIMPLE_OUT}"
log "complex output: ${COMPLEX_OUT}"

run_pipeline "simple"  "${SIMPLE_DIR}"  "${SIMPLE_OUT}"
run_pipeline "complex" "${COMPLEX_DIR}" "${COMPLEX_OUT}"

log "done. Collected files:"
ls -1 "${SIMPLE_OUT}"/*.json "${COMPLEX_OUT}"/*.json 2>/dev/null | sed 's/^/    /' || log "(no JSON files found)"
