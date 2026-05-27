//! Build orchestration: invokes `cargo` to compile stage crates to
//! wasm32-wasip2 components. Thin wrapper around `std::process::Command`
//! that gives useful error messages.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Run `cargo build --release --target wasm32-wasip2` against the
/// generated workspace manifest. Returns Ok(()) on success; returns
/// an error with cargo's stderr attached on failure.
pub fn build_stages(workspace_manifest: &Path) -> Result<()> {
    println!("==> Building stage components (target: wasm32-wasip2)");
    println!("    manifest: {}", workspace_manifest.display());

    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasip2")
        .arg("--manifest-path")
        .arg(workspace_manifest)
        .status()
        .context("failed to invoke `cargo` — is it installed and on PATH?")?;

    if !status.success() {
        bail!(
            "cargo build failed (exit code {:?}). \
             Inspect the output above for the specific error. \
             Common causes: missing wasm32-wasip2 target \
             (`rustup target add wasm32-wasip2`), \
             wit-bindgen version mismatch, or a syntax error in a stage .rs file.",
            status.code()
        );
    }

    Ok(())
}

/// Precompile each stage's .wasm into a .cwasm AOT artifact next to it.
/// At runtime the agent prefers the .cwasm if present and skips Cranelift
/// entirely (Component::deserialize_file instead of Component::from_file).
///
/// Uses Engine::precompile_component from the same wasmtime crate version
/// that the agent links against, guaranteeing the artifact is always
/// compatible — no external wasmtime CLI required.
pub fn aot_compile_stages(wasm_paths: &[(String, PathBuf)]) -> Result<()> {
    use wasmtime::{Config, Engine};

    println!("==> AOT precompiling stages (.wasm → .cwasm)");
    println!("    wasmtime: 26.0.1 (in-process, version-matched to agent)");

    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config)
        .context("failed to create wasmtime Engine for AOT compilation")?;

    let mut compiled = 0;
    for (stage_name, wasm_path) in wasm_paths {
        if !wasm_path.exists() {
            println!("    [{}] SKIP — wasm not found at {}", stage_name, wasm_path.display());
            continue;
        }
        let cwasm_path = wasm_path.with_extension("cwasm");
        println!(
            "    [{:<12}] {} → {}",
            stage_name,
            wasm_path.file_name().unwrap().to_string_lossy(),
            cwasm_path.file_name().unwrap().to_string_lossy()
        );

        let wasm_bytes = std::fs::read(wasm_path)
            .with_context(|| format!("failed to read {}", wasm_path.display()))?;
        let cwasm_bytes = engine
            .precompile_component(&wasm_bytes)
            .with_context(|| format!("AOT compilation failed for {}", wasm_path.display()))?;
        std::fs::write(&cwasm_path, &cwasm_bytes)
            .with_context(|| format!("failed to write {}", cwasm_path.display()))?;

        compiled += 1;
    }

    println!("==> AOT compiled {} component(s)", compiled);
    Ok(())
}

/// Remove any .cwasm files next to the given .wasm paths. Used when
/// switching back to JIT mode so stale AOT artifacts don't get picked up
/// by the agent's "cwasm if present, else wasm" loader.
pub fn remove_aot_artifacts(wasm_paths: &[(String, PathBuf)]) -> Result<()> {
    let mut removed = 0;
    for (_, wasm_path) in wasm_paths {
        let cwasm = wasm_path.with_extension("cwasm");
        if cwasm.exists() {
            std::fs::remove_file(&cwasm)
                .with_context(|| format!("removing {:?}", cwasm))?;
            removed += 1;
        }
    }
    if removed > 0 {
        println!("==> Removed {} stale .cwasm artifact(s)", removed);
    }
    Ok(())
}

/// Build the runtime binaries (agent + dispatcher) from the workspace
/// root. Used by `epico bootstrap` and by `epico run` when the
/// binaries are missing.
pub fn bootstrap_runtime(workspace_root: &Path, cold_start_opt: bool) -> Result<()> {
    println!("==> Building runtime (agent + dispatcher)");
    println!("    workspace: {}", workspace_root.display());
    if cold_start_opt {
        println!("    cold-start-opt: enabled");
    } else {
        println!("    cold-start-opt: disabled (baseline)");
    }

    let manifest = workspace_root.join("Cargo.toml");
    if !manifest.exists() {
        bail!(
            "workspace Cargo.toml not found at {:?}. \
             Is this a Epico project root?",
            manifest
        );
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("-p")
        .arg("master")
        .arg("-p")
        .arg("dispatcher");

    if cold_start_opt {
        cmd.arg("--features").arg("master/cold-start-opt");
    }

    let status = cmd
        .status()
        .context("failed to invoke `cargo` — is it installed and on PATH?")?;

    if !status.success() {
        bail!(
            "cargo build failed (exit code {:?}). \
             Inspect the output above for the specific error.",
            status.code()
        );
    }

    Ok(())
}

pub fn run_pipeline(
    agent_path:      &Path,
    runtime_yaml:    &Path,
    dispatcher_path: Option<&Path>,
    log_dir:         &Path,
) -> Result<()> {
    println!("==> Starting pipeline");
    println!("    agent:      {}", agent_path.display());
    println!("    runtime:    {}", runtime_yaml.display());
    println!("    log_dir:    {}", log_dir.display());
    if let Some(dp) = dispatcher_path {
        println!("    dispatcher: {}", dp.display());
    }

    let mut cmd = Command::new(agent_path);
    cmd.arg(runtime_yaml);
    cmd.arg("--log-dir").arg(log_dir);
    if let Some(dp) = dispatcher_path {
        cmd.arg("--dispatcher-bin").arg(dp);
    }

    let status = cmd
        .status()
        .with_context(|| format!("failed to invoke agent at {:?}", agent_path))?;

    if !status.success() {
        bail!("agent exited with status {:?}", status.code());
    }
    Ok(())
}

pub fn spawn_agent(
    agent_path:      &Path,
    runtime_yaml:    &Path,
    dispatcher_path: Option<&Path>,
    log_dir:         &Path,
    ready_file:      &Path,
) -> Result<std::process::Child> {
    let mut cmd = Command::new(agent_path);
    cmd.arg(runtime_yaml);
    cmd.arg("--log-dir").arg(log_dir);
    cmd.arg("--ready-file").arg(ready_file);
    if let Some(dp) = dispatcher_path {
        cmd.arg("--dispatcher-bin").arg(dp);
    }
    cmd.spawn()
        .with_context(|| format!("failed to spawn agent at {:?}", agent_path))
}