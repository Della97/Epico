//! Captures metadata about the build environment and exposes it to the
//! agent via environment variables, read at runtime via `option_env!`.
//!
//! - EPICO_RUSTC_VERSION — output of `rustc --version`.
//! - EPICO_GIT_COMMIT    — `git rev-parse HEAD` if inside a repo.
//! - EPICO_GIT_DIRTY     — "1" if the working tree has uncommitted
//!                          changes, else "0".
//!
//! All are best-effort: if `git` or `rustc` aren't available, the
//! variables are unset and the agent defaults to "unknown".

use std::process::Command;

fn main() {
    // Re-run only when the script itself changes, not on every rebuild.
    // The captured values are static within a build; we don't need them
    // re-evaluated on every touch of an unrelated source file.
    println!("cargo:rerun-if-changed=build.rs");

    if let Some(v) = run_cmd("rustc", &["--version"]) {
        println!("cargo:rustc-env=EPICO_RUSTC_VERSION={}", v);
    }
    if let Some(v) = run_cmd("git", &["rev-parse", "HEAD"]) {
        println!("cargo:rustc-env=EPICO_GIT_COMMIT={}", v);
    }
    if let Some(out) = run_cmd("git", &["status", "--porcelain"]) {
        let dirty = if out.is_empty() { "0" } else { "1" };
        println!("cargo:rustc-env=EPICO_GIT_DIRTY={}", dirty);
    }
}

fn run_cmd(bin: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(bin).args(args).output().ok()?;
    if !out.status.success() { return None; }
    let s = String::from_utf8(out.stdout).ok()?;
    Some(s.trim().to_string())
}