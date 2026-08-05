#!/usr/bin/env bash
# setup.sh — first-time setup for epico.
#
# Run this from the epico repo root. It does:
#   1. Verifies the Rust toolchain is present (does NOT install rust).
#   2. Adds the wasm32-wasip2 target via rustup (needed for stage compilation).
#   3. Builds the full runtime workspace in release mode:
#        master + dispatcher + epico-loadgen  (and the epico-wire crate they
#        share). This also refreshes Cargo.lock so the --locked CLI install
#        below stays reproducible.
#   4. Installs the `epico` CLI to ~/.cargo/bin via cargo install.
#   5. Verifies the produced binaries and that ~/.cargo/bin is on PATH.
#
# After setup:
#   - rebuild the CLI itself:   re-run this script (or `cargo install --path epico-cli --force`)
#   - rebuild the runtime:      `epico bootstrap`   (master + dispatcher)
#                               re-run this script   (also rebuilds the loadgen)
#   - build a pipeline:         `cd examples/X && epico build`
#   - run a pipeline:           `cd examples/X && epico run`
#   - wipe all build artifacts: `epico clean`        (from anywhere in the repo)
#
# Re-running setup.sh is safe.

set -euo pipefail

# ── pretty output (matches the logger) ───────────────────────────────────────

if [[ -t 2 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    DIM=$'\033[38;5;244m'
    YEL=$'\033[33m'
    RED=$'\033[31m'
    GRN=$'\033[32m'
    CYN=$'\033[36m'
    BLD=$'\033[1m'
    RST=$'\033[0m'
    # Brand colour: sRGB(0.953, 0.882, 0.769) = #F3E1C4, a warm cream.
    # 24-bit where the terminal advertises truecolor, otherwise xterm-256
    # colour 223 (#FFD7AF) — the nearest entry that keeps the R>G>B falloff
    # the brand tone depends on. (Colour 224 is marginally closer by raw
    # distance but flattens G==B, reading pink rather than cream.)
    case "${COLORTERM:-}" in
        truecolor|24bit) BRAND=$'\033[38;2;243;225;196m' ;;
        *)               BRAND=$'\033[38;5;223m'         ;;
    esac
else
    DIM='' ; YEL='' ; RED='' ; GRN='' ; CYN='' ; BLD='' ; RST='' ; BRAND=''
fi


ts()    { date +%H:%M:%S ; }
info()  { printf '%s%s%s  [info ]  %s\n' "$DIM" "$(ts)" "$RST" "$*" >&2 ; }
warn()  { printf '%s%s%s  %s[warn ]%s  %s%s%s\n' "$DIM" "$(ts)" "$RST" "$YEL" "$RST" "$YEL" "$*" "$RST" >&2 ; }
fail()  { printf '%s%s%s  %s[error]%s  %s%s%s\n' "$DIM" "$(ts)" "$RST" "$RED" "$RST" "$RED" "$*" "$RST" >&2 ; exit 1 ; }

# ── locate repo root ─────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Sanity-check the workspace layout — these paths are specific to epico
# and confirm we're in the right place.
[[ -f Cargo.toml ]]             || fail "no Cargo.toml in $SCRIPT_DIR — put setup.sh at the epico repo root"
[[ -f epico-cli/Cargo.toml ]]   || fail "epico-cli/Cargo.toml missing — is this a epico checkout?"
[[ -f epico-sdk/Cargo.toml ]]   || fail "epico-sdk/Cargo.toml missing — is this a epico checkout?"
[[ -f epico-wire/Cargo.toml ]]  || fail "epico-wire/Cargo.toml missing — the shared binary wire-format crate is required (master + loadgen depend on it for binary ingest)."
[[ -f epico-loadgen/Cargo.toml ]] || fail "epico-loadgen/Cargo.toml missing — is this a epico checkout?"

info "repo root: $SCRIPT_DIR"

# Release version — read from the ONE place that defines it. Everything else
# (`epico --version`, the agent's startup log, every summary JSON) derives from
# the same field via cargo, so this parse cannot disagree with the binaries.
# Read after the cd above, so it resolves no matter where the script is invoked.
EPICO_VERSION_STR="$(awk '/^\[workspace\.package\]/{f=1;next} f&&/^version[[:space:]]*=/{gsub(/[",]/,"");print $3;exit}' Cargo.toml 2>/dev/null)"
EPICO_VERSION_STR="${EPICO_VERSION_STR:-unknown}"
info "version: v$EPICO_VERSION_STR" 

# ── 1. toolchain check ───────────────────────────────────────────────────────

info "checking rust toolchain"

command -v cargo  >/dev/null 2>&1 || fail "cargo not found — install rust via https://rustup.rs then re-run"
command -v rustc  >/dev/null 2>&1 || fail "rustc not found — install rust via https://rustup.rs then re-run"
command -v rustup >/dev/null 2>&1 || fail "rustup not found — the wasm32-wasip2 target needs rustup to install"

RUSTC_VERSION="$(rustc --version 2>/dev/null || echo 'unknown')"
info "rustc=$RUSTC_VERSION"

# The host crates (master/cli) pull wasmtime 26, which needs rustc >= 1.79;
# wasm32-wasip2 stabilized in 1.82. Warn (don't block) below 1.82.
RUSTC_SEMVER="$(rustc --version | awk '{print $2}')"
RUSTC_MAJOR="$(echo "$RUSTC_SEMVER" | cut -d. -f1)"
RUSTC_MINOR="$(echo "$RUSTC_SEMVER" | cut -d. -f2)"
if [[ "$RUSTC_MAJOR" -lt 1 ]] || { [[ "$RUSTC_MAJOR" -eq 1 ]] && [[ "$RUSTC_MINOR" -lt 82 ]]; }; then
    warn "rustc $RUSTC_SEMVER is older than 1.82 — wasm32-wasip2 + wasmtime 26 may not build; consider 'rustup update'"
fi

# ── 2. wasm target ───────────────────────────────────────────────────────────
# Pipelines compile user stages to wasm32-wasip2. Missing target is the #1
# source of confusing first-run failures, so install it eagerly.

info "ensuring wasm32-wasip2 target is installed"
if rustup target list --installed 2>/dev/null | grep -q '^wasm32-wasip2$'; then
    info "wasm32-wasip2 already installed"
else
    info "installing wasm32-wasip2 via rustup"
    rustup target add wasm32-wasip2 \
        || fail "rustup failed to add wasm32-wasip2 — check network and rustup toolchain"
fi

# ── 3. build the runtime workspace ───────────────────────────────────────────
# One cargo invocation builds every native runtime binary:
#   master         — the wasm worker host / agent
#   dispatcher     — the credit-flow router (socket transport)
#   epico-loadgen  — the load generator (REQUIRED: `epico run` with a
#                    `source: { kind: loadgen }` spawns target/release/epico-loadgen;
#                    bootstrap alone does NOT build it, which is the usual
#                    "epico-loadgen binary not found" failure on a fresh clone).
# The shared `epico-wire` crate is compiled transitively. Building here also
# writes/refreshes Cargo.lock so the --locked CLI install below is reproducible.

info "building runtime: master + dispatcher + epico-loadgen (release)"
info "this may take several minutes on first run (wasmtime is large)"

BUILD_PKGS=(-p master -p dispatcher -p epico-loadgen)
if ! cargo build --release "${BUILD_PKGS[@]}" ; then
    fail "runtime build failed — see the cargo error above"
fi
info "runtime built: target/release/{master,dispatcher,epico-loadgen}"

# ── 4. install the CLI ───────────────────────────────────────────────────────
# `cargo install --path epico-cli` builds the CLI crate in release mode and
# copies the resulting `epico` binary to $CARGO_HOME/bin.
#   --locked   honors Cargo.lock (refreshed in step 3) so setup is reproducible
#   --force    overwrites any previously-installed epico

CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"

info "building and installing the epico CLI (release)"

if ! cargo install --path epico-cli --locked --force ; then
    warn "install with --locked failed; retrying without it"
    cargo install --path epico-cli --force \
        || fail "cargo install failed — see error above"
fi

EPICO_BIN="$CARGO_BIN/epico"
[[ -x "$EPICO_BIN" ]] \
    || fail "expected binary at $EPICO_BIN but it's missing — check [[bin]].name in epico-cli/Cargo.toml"

info "installed $EPICO_BIN"

# ── 5. verify the runtime binaries ───────────────────────────────────────────

REL="$SCRIPT_DIR/target/release"
for bin in master dispatcher epico-loadgen ; do
    if [[ -x "$REL/$bin" ]]; then
        info "ok: target/release/$bin"
    else
        warn "expected target/release/$bin but it's missing — 'epico run' may re-bootstrap or fail to find it"
    fi
done

# ── 6. PATH check ────────────────────────────────────────────────────────────

PATH_OK=1
case ":$PATH:" in
    *":$CARGO_BIN:"*) : ;;
    *) PATH_OK=0 ;;
esac

if [[ "$PATH_OK" -eq 0 ]]; then
    warn "$CARGO_BIN is not on your PATH"
    warn "add this line to ~/.bashrc or ~/.zshrc:"
    warn "    export PATH=\"$CARGO_BIN:\$PATH\""
    warn "then open a new shell before running 'epico'"
else
    info "path ok"
fi

# ── 7. active configuration ──────────────────────────────────────────────────
# Everything below is REPORTING, not configuration: it prints what this build
# and this shell will actually do, so a run's behaviour is never a surprise.
#
# Two independent layers, and they are easy to confuse:
#   * BUILD FEATURES are compiled in. Changing one needs a rebuild.
#   * ENV OVERRIDES are read at process start and BEAT the pipeline YAML.
#     An env var left over from an earlier experiment silently changes what a
#     run measures, which is exactly why they are listed here.

printf '\n'
printf '%s  ── build features ──────────────────────────────────────────────%s\n' "$DIM" "$RST"

# Source of truth for what a plain `cargo build` produces: master's default
# feature set. `epico build` propagates the same choice into the generated
# per-pipeline agent, which is the binary that actually runs.
if grep -qE '^default[[:space:]]*=[[:space:]]*\[[^]]*"cold-start-opt"' epico-core/master/Cargo.toml 2>/dev/null; then
    printf '    %-22s %sON%s   %spooling allocator, CoW memory init, Cranelift Speed, parallel compilation%s\n' \
        "cold-start-opt" "$GRN" "$RST" "$DIM" "$RST"
    printf '    %-22s      %sbaseline arm: epico build --no-cold-start-opt%s\n' "" "$DIM" "$RST"
else
    printf '    %-22s %sOFF%s  %sbaseline arm — cold-start numbers are not deployment numbers%s\n' \
        "cold-start-opt" "$YEL" "$RST" "$DIM" "$RST"
fi
printf '    %-22s %sOFF%s  %sCPU sampling profiler; build with --features master/profile%s\n' \
    "profile" "$DIM" "$RST" "$DIM" "$RST"

printf '\n'
printf '%s  ── environment overrides ───────────────────────────────────────%s\n' "$DIM" "$RST"
printf '%s     set values WIN over pipeline.yaml. Unset = the default shown.%s\n' "$DIM" "$RST"
printf '\n'

n_set=0
# env_row NAME DEFAULT DESCRIPTION
env_row() {
    local name="$1" def="$2" desc="$3"
    if [[ -n "${!name+x}" ]]; then
        n_set=$((n_set + 1))
        printf '    %s%s%-24s%s %s%-14s%s %s%s%s\n' \
            "$BLD" "$YEL" "$name" "$RST" "$GRN" "${!name}" "$RST" "$DIM" "$desc" "$RST"
    else
        printf '    %-24s %s%-14s%s %s%s%s\n' \
            "$name" "$DIM" "$def" "$RST" "$DIM" "$desc" "$RST"
    fi
}

printf '%s    transport%s\n' "$CYN" "$RST"
env_row EPICO_EDGE_IMPL        "(yaml)"  "zmq | mpmc | spsc — stage-to-stage transport"
env_row EPICO_EDGE_CAP         "1024"    "mpmc ring slots (flow-control window)"
env_row EPICO_SPSC_RING_CAP    "256"     "slots per SPSC ring in the mesh"
env_row EPICO_INPROC_EDGES     "(yaml)"  "1 = collapse stage->stage hops to in-proc queues"
env_row EPICO_INPROC_INGRESS   "(yaml)"  "1 = also collapse ingress/egress (implies EDGES)"
env_row EPICO_BINARY_EDGES     "(yaml)"  "1 = stage OUTPUT uses the binary envelope"

printf '%s    execution%s\n' "$CYN" "$RST"
env_row EPICO_DYNAMIC_DISPATCH "0"       "1 = force the dynamic Val path, bypassing typed dispatch"
env_row EPICO_NATIVE_STAGE     "(off)"   "passthrough | serde — BYPASS: wasm is NOT called"

printf '%s    source%s\n' "$CYN" "$RST"
env_row EPICO_SOURCE_GEN       "0"       "1 = in-process generating source (no socket, no loadgen)"
env_row EPICO_SOURCE_FORMAT    "(yaml)"  "json | binary at the ingress"
env_row EPICO_SOURCE_THREADS   "1"       "source pump fan-out"
env_row EPICO_SOURCE_COUNT     "5000000" "events for the generating source"
env_row EPICO_SOURCE_SENSORS   "100"     "distinct sensor ids"
env_row EPICO_SOURCE_SECONDS   "(none)"  "wall-clock cap; source emits EOS and stops"

printf '%s    orchestration%s\n' "$CYN" "$RST"
env_row EPICO_EOS_DRAIN_SECS   "30"      "CLI grace period for the pipeline to drain after EOS"
env_row EPICO_DISPATCHER       "(auto)"  "explicit dispatcher binary path"
env_row EPICO_LOG              "info"    "debug | info | warn | error (JSONL keeps everything)"
env_row EPICO_RUN_DIR          "(auto)"  "run directory for JSONL logs"

printf '\n'
if [[ "$n_set" -gt 0 ]]; then
    warn "$n_set environment override(s) are ACTIVE in this shell (highlighted above)"
    warn "they take precedence over pipeline.yaml — unset them for a clean baseline run"
else
    info "no environment overrides set — runs will follow pipeline.yaml exactly"
fi

# ── done ─────────────────────────────────────────────────────────────────────

printf '\n%s' "$BRAND"
cat <<'BANNER'
      ███████╗██████╗ ██╗ ██████╗ ██████╗
      ██╔════╝██╔══██╗██║██╔════╝██╔═══██╗
      █████╗  ██████╔╝██║██║     ██║   ██║
      ██╔══╝  ██╔═══╝ ██║██║     ██║   ██║
      ███████╗██║     ██║╚██████╗╚██████╔╝
      ╚══════╝╚═╝     ╚═╝ ╚═════╝ ╚═════╝
BANNER
printf '%s' "$RST"
printf '%s        a serverless runtime that rewrites its own topology%s\n' "$DIM" "$RST"
printf '%s        v%s%s' "$BRAND" "$EPICO_VERSION_STR" "$RST"

# GitHub Releases are cut from git TAGS, so the tag is what the release page
# shows — while `epico --version` reports the Cargo version. They are two
# different sources and drift silently, which is how a binary ends up claiming
# to be a release it is not. Surface the disagreement here instead.
GIT_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"
if [[ -z "$GIT_TAG" ]]; then
    printf '  %s(no git tag yet — tag v%s to cut the GitHub release)%s\n\n' \
        "$DIM" "$EPICO_VERSION_STR" "$RST"
elif [[ "$GIT_TAG" == "v$EPICO_VERSION_STR" ]]; then
    printf '  %s(git tag %s — in sync)%s\n\n' "$DIM" "$GIT_TAG" "$RST"
else
    printf '\n\n'
    warn "version drift: Cargo.toml says v$EPICO_VERSION_STR but the latest git tag is $GIT_TAG"
    warn "GitHub Releases follow the TAG. To release v$EPICO_VERSION_STR:"
    warn "    git tag -a v$EPICO_VERSION_STR -m \"v$EPICO_VERSION_STR\" && git push --tags"
fi

info "setup complete"
info "try:  cd examples/two-stage-min && epico run     # binary ingest via loadgen"
info " or:  cd examples/three-stage-test && epico run  # binary ingest via native source"
info " or:  cd examples/fusion-chain && epico run      # runtime stage fusion (M2)"