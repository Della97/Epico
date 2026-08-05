//! Captures metadata about the build environment and exposes it to the
//! agent via environment variables, read at runtime via `option_env!`.
//!
//! - EPICO_RUSTC_VERSION    — output of `rustc --version`.
//! - EPICO_GIT_COMMIT       — `git rev-parse HEAD` if inside a repo.
//! - EPICO_GIT_DIRTY        — "1" if the working tree has uncommitted
//!                             changes, else "0".
//! - EPICO_WASMTIME_VERSION — resolved wasmtime version, read from the
//!                             workspace `Cargo.lock`.
//! - EPICO_VERSION          — the release version, inherited from
//!                             `[workspace.package] version` (one source of
//!                             truth for the whole workspace).
//! - EPICO_GIT_TAG          — `git describe --tags`, i.e. what the GitHub
//!                             release page will show. Recorded alongside
//!                             EPICO_VERSION so a mismatch between the two is
//!                             visible in the run record rather than silent.
//!
//! All are best-effort: if `git` or `rustc` aren't available, the
//! variables are unset and the agent defaults to "unknown".

use std::process::Command;

fn main() {
    // Re-run only when the script itself changes, not on every rebuild.
    // The captured values are static within a build; we don't need them
    // re-evaluated on every touch of an unrelated source file.
    println!("cargo:rerun-if-changed=build.rs");

    // The release version, straight from the manifest chain. Cargo sets this
    // for us, so it cannot drift from `[workspace.package] version`.
    println!(
        "cargo:rustc-env=EPICO_VERSION={}",
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".into())
    );
    // What `git describe` says — the tag a GitHub release is cut from. Falls
    // back to a bare commit when no tag is reachable, which is the normal
    // state between releases.
    if let Some(v) = run_cmd("git", &["describe", "--tags", "--always", "--dirty"]) {
        println!("cargo:rustc-env=EPICO_GIT_TAG={}", v);
    }

    if let Some(v) = run_cmd("rustc", &["--version"]) {
        println!("cargo:rustc-env=EPICO_RUSTC_VERSION={}", v);
    }
    if let Some(v) = wasmtime_version() {
        println!("cargo:rustc-env=EPICO_WASMTIME_VERSION={}", v);
    }
    if let Some(v) = run_cmd("git", &["rev-parse", "HEAD"]) {
        println!("cargo:rustc-env=EPICO_GIT_COMMIT={}", v);
    }
    if let Some(out) = run_cmd("git", &["status", "--porcelain"]) {
        let dirty = if out.is_empty() { "0" } else { "1" };
        println!("cargo:rustc-env=EPICO_GIT_DIRTY={}", dirty);
    }
}

/// Resolved wasmtime version, parsed out of the workspace `Cargo.lock`.
///
/// The summary JSON reports which wasmtime compiled a run's components; that
/// used to be a hardcoded `"26"` string in the source, which goes silently
/// wrong on a dependency bump — exactly the kind of error that invalidates a
/// results table after the fact. The lockfile is tracked, so it is the
/// authoritative answer and costs nothing to read at build time.
///
/// Returns None (→ "unknown" at runtime) when the lockfile can't be found or
/// parsed, e.g. when `master` is built as a path dependency of the generated
/// per-pipeline agent from a different workspace root.
fn wasmtime_version() -> Option<String> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    // epico-core/master → epico-core → repo root
    let lock = std::path::Path::new(&manifest)
        .parent()?
        .parent()?
        .join("Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock.display());
    let text = std::fs::read_to_string(&lock).ok()?;

    // Lockfile entries are `[[package]] name = "..." version = "..."`, with
    // name always preceding version inside a block.
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == r#"name = "wasmtime""# {
            for next in lines.by_ref().take(3) {
                if let Some(rest) = next.trim().strip_prefix(r#"version = ""#) {
                    return Some(rest.trim_end_matches('"').to_string());
                }
            }
            return None;
        }
    }
    None
}

fn run_cmd(bin: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(bin).args(args).output().ok()?;
    if !out.status.success() { return None; }
    let s = String::from_utf8(out.stdout).ok()?;
    Some(s.trim().to_string())
}