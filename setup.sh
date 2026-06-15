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
    RST=$'\033[0m'
else
    DIM='' ; YEL='' ; RED='' ; RST=''
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

# ── done ─────────────────────────────────────────────────────────────────────

info "setup complete"
info "try:  cd examples/two-stage-min && epico run     # binary ingest via loadgen"
info " or:  cd examples/three-stage-test && epico run  # binary ingest via native source"