//! epico CLI — build and run pipelines from a single YAML file.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

mod build;
mod codegen;
mod config;

#[derive(Parser)]
#[command(name = "epico")]
#[command(about = "Build and run epico stream-processing pipelines", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate stage crates and compile wasm components.
    Build {
        #[arg(short, long, default_value = "pipeline.yaml")]
        config: PathBuf,
        #[arg(long)]
        project_root: Option<PathBuf>,
        /// AOT-precompile each stage's .wasm into a .cwasm artifact.
        /// At runtime the agent skips Cranelift if .cwasm is present.
        /// Mutually exclusive with --jit.
        #[arg(long)]
        aot: bool,
        /// JIT mode: stage source is compiled to .wasm (always), but
        /// .wasm→.cwasm compilation is deferred to cold-start time.
        /// The agent writes the .cwasm on first spawn so subsequent
        /// spawns are fast. Mutually exclusive with --aot.
        #[arg(long)]
        jit: bool,
    },
    /// Build and launch the full pipeline (dispatchers + agent).
    Run {
        #[arg(short, long, default_value = "pipeline.yaml")]
        config: PathBuf,
        #[arg(long)]
        project_root: Option<PathBuf>,
        #[arg(long, default_value = "logs")]
        log_dir: PathBuf,
        /// AOT-precompile stages before running (.wasm→.cwasm at build time).
        /// Cold-start = thread spawn + Wasm instantiation only.
        /// Mutually exclusive with --jit.
        #[arg(long)]
        aot: bool,
        /// JIT-compile stages at cold-start time (.wasm→.cwasm deferred to
        /// first spawn). Cold-start measurement includes compilation.
        /// Mutually exclusive with --aot.
        #[arg(long)]
        jit: bool,
        /// Skip the stage (re)build and launch against the .wasm already on
        /// disk. runtime.yaml is still regenerated from the pipeline YAML, so
        /// launch-time knobs (batch_events, credit_window, ports, collector,
        /// scaling caps, resource sampling) take effect immediately. Use this
        /// to sweep those without paying a wasm recompile each run. Only safe
        /// when the stage .rs sources are unchanged since the last build.
        #[arg(long)]
        no_build: bool,
    },
    /// Generate stage crates + validate components.
    Validate {
        #[arg(short, long, default_value = "pipeline.yaml")]
        config: PathBuf,
        #[arg(long)]
        project_root: Option<PathBuf>,
    },
    /// Remove the entire target/ directory.
    Clean {
        #[arg(short, long, default_value = "pipeline.yaml")]
        config: PathBuf,
        #[arg(long)]
        project_root: Option<PathBuf>,
    },
    /// Build the runtime binaries (agent + dispatcher).
    Bootstrap {
        #[arg(long)]
        project_root: Option<PathBuf>,
        /// Enable cold-start optimizations in the agent: pooling allocator,
        /// CoW memory init, Cranelift Speed opt level, parallel compilation.
        #[arg(long)]
        cold_start_opt: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build { config, project_root, aot, jit } =>
            cmd_build(&config, project_root.as_deref(), aot, jit),
        Commands::Run { config, project_root, log_dir, aot, jit, no_build } =>
            cmd_run(&config, project_root.as_deref(), &log_dir, aot, jit, no_build),
        Commands::Validate { config, project_root } =>
            cmd_validate(&config, project_root.as_deref()),
        Commands::Clean { config, project_root } =>
            cmd_clean(&config, project_root.as_deref()),
        Commands::Bootstrap { project_root, cold_start_opt } =>
            cmd_bootstrap(project_root.as_deref(), cold_start_opt),
    }
}

fn cmd_build(config_path: &Path, project_root: Option<&Path>, aot: bool, jit: bool) -> Result<()> {
    if aot && jit {
        anyhow::bail!("--aot and --jit are mutually exclusive");
    }
    let compile_mode: Option<&str> = if aot { Some("aot") } else if jit { Some("jit") } else { None };
    let (spec, root) = load_and_resolve(config_path, project_root)?;
    let output_dir = root.join("target").join("epico");

    let out = codegen::generate(&spec, &root, &output_dir, compile_mode)?;

    if out.needs_build {
        build::build_stages(&out.workspace_manifest)?;
    } else {
        println!("==> No Rust stages to build (all stages use prebuilt wasm).");
    }

    if let Some(manifest) = &out.agent_manifest {
        build::build_agent(manifest)?;
    }

    // AOT: precompile .wasm→.cwasm now so the agent deserializes at startup.
    // JIT: remove any stale .cwasm so the agent defers compilation to cold-start.
    // Neither flag: also remove stale .cwasm (legacy startup-JIT behaviour).
    if aot {
        build::aot_compile_stages(&out.wasm_by_stage)?;
    } else {
        build::remove_aot_artifacts(&out.wasm_by_stage)?;
    }

    println!("==> runtime.yaml written to {}", out.runtime_yaml.display());
    println!("==> wasm outputs:");
    for (name, path) in &out.wasm_by_stage {
        let exists = path.exists();
        let cwasm  = path.with_extension("cwasm");
        let cwasm_exists = cwasm.exists();
        let mode = if cwasm_exists { "AOT" } else if exists && jit { "JIT (deferred)" } else if exists { "JIT (startup)" } else { "MISSING" };
        println!(
            "    {:<20} {} ({})",
            name, path.display(), mode
        );
    }
    println!("==> Build complete.");
    Ok(())
}

fn cmd_validate(config_path: &Path, project_root: Option<&Path>) -> Result<()> {
    let (spec, root) = load_and_resolve(config_path, project_root)?;
    let output_dir = root.join("target").join("epico");
    let out = codegen::generate(&spec, &root, &output_dir, None)?;

    let mut any_missing = false;
    for (name, path) in &out.wasm_by_stage {
        if !path.exists() {
            println!(
                "  [{}] MISSING wasm at {} (run `epico build` first)",
                name,
                path.display()
            );
            any_missing = true;
        }
    }
    if any_missing {
        bail!("one or more stages have no built component yet");
    }
    println!("==> Pipeline YAML parses, all {} stages have .wasm on disk.", spec.stages.len());
    Ok(())
}

fn cmd_run(config_path: &Path, project_root: Option<&Path>, log_dir: &Path, aot: bool, jit: bool, no_build: bool) -> Result<()> {
    if aot && jit {
        anyhow::bail!("--aot and --jit are mutually exclusive");
    }
    let compile_mode: Option<&str> = if aot { Some("aot") } else if jit { Some("jit") } else { None };
    let (spec, root) = load_and_resolve(config_path, project_root)?;
    let output_dir = root.join("target").join("epico");

    let out = codegen::generate(&spec, &root, &output_dir, compile_mode)?;

    if out.needs_build && !no_build {
        build::build_stages(&out.workspace_manifest)?;
    } else if no_build {
        // Launch-time-only change (e.g. batch_events / credit_window): skip the
        // wasm rebuild and use the .wasm already on disk. Sanity-check it exists
        // so we fail clearly rather than inside the agent.
        let missing: Vec<_> = out.wasm_by_stage.iter()
            .filter(|(_, p)| !p.exists())
            .map(|(s, _)| s.clone())
            .collect();
        if !missing.is_empty() {
            bail!(
                "--no-build but wasm is missing for stage(s): {}. Run once without \
                 --no-build to build them first.",
                missing.join(", ")
            );
        }
        println!("==> --no-build: reusing existing .wasm; runtime.yaml regenerated \
                  (launch-time config applied)");
    }

    // Reconcile AOT/JIT artifacts. AOT: precompile .cwasm now.
    // JIT or neither: remove stale .cwasm so agent doesn't accidentally
    // pick up an AOT artifact from a previous run.
    if aot {
        build::aot_compile_stages(&out.wasm_by_stage)?;
    } else {
        build::remove_aot_artifacts(&out.wasm_by_stage)?;
    }

    let release_dir = root.join("target").join("release");
    let agent      = release_dir.join("master");
    let dispatcher = release_dir.join("dispatcher");

    if !agent.exists() || !dispatcher.exists() {
        println!(
            "==> Runtime binaries missing (agent at {} or dispatcher at {}).",
            agent.display(),
            dispatcher.display()
        );
        println!("    Running bootstrap first (no cold-start-opt)...");
        build::bootstrap_runtime(&root, false)?;

        if !agent.exists() || !dispatcher.exists() {
            bail!(
                "bootstrap finished but {} or {} still missing.",
                agent.display(),
                dispatcher.display()
            );
        }
    }

    let log_dir_abs = std::env::current_dir()
        .unwrap_or_else(|_| root.clone())
        .join(log_dir);

    // Native source/sink (option A): build the per-pipeline agent with the
    // user's source/sink compiled in, and launch it instead of stock `master`.
    // The bootstrap above still ensured the dispatcher binary exists (the agent
    // resolves it even when it spawns none) and that epico_master compiles.
    let agent = match &out.agent_manifest {
        Some(manifest) => build::build_agent(manifest)?,
        None => agent,
    };

    println!("==> Log directory: {}", log_dir_abs.display());
    println!("==> Component mode: {}",
        if aot { "AOT (.cwasm precompiled)" }
        else if jit { "JIT (.wasm->.cwasm at cold-start)" }
        else { "startup-JIT (.wasm compiled at autoscaler start)" }
    );

    match &spec.source {
        Some(src) => run_orchestrated(
            &agent,
            &out.runtime_yaml,
            &dispatcher,
            &log_dir_abs,
            &root,
            src,
        ),
        None => build::run_pipeline(&agent, &out.runtime_yaml, Some(&dispatcher), &log_dir_abs),
    }
}

fn run_orchestrated(
    agent:        &Path,
    runtime_yaml: &Path,
    dispatcher:   &Path,
    log_dir:      &Path,
    root:         &Path,
    source:       &config::SourceSpec,
) -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let ready_tmp = tempfile::Builder::new()
        .prefix("epico-ready-")
        .suffix(".flag")
        .tempfile()
        .context("creating ready-file")?;
    let ready_path = ready_tmp.path().to_path_buf();
    drop(ready_tmp);
    if ready_path.exists() {
        let _ = std::fs::remove_file(&ready_path);
    }

    println!("==> Starting agent");
    println!("    agent:      {}", agent.display());
    println!("    runtime:    {}", runtime_yaml.display());
    println!("    dispatcher: {}", dispatcher.display());
    println!("    ready-file: {}", ready_path.display());

    let mut agent_child = build::spawn_agent(
        agent, runtime_yaml, Some(dispatcher), log_dir, &ready_path,
    )?;
    let agent_pid = agent_child.id();

    let sig_count = Arc::new(AtomicUsize::new(0));
    let sig_count_c = sig_count.clone();
    let _ = ctrlc::set_handler(move || {
        let n = sig_count_c.fetch_add(1, Ordering::Relaxed) + 1;
        eprintln!("\n==> Ctrl+C received (#{n}) — propagating to children");
    });

    let ready_timeout = Duration::from_secs(30);
    let start = Instant::now();
    loop {
        if ready_path.exists() {
            println!("==> Agent ready ({} ms)", start.elapsed().as_millis());
            break;
        }
        if let Ok(Some(status)) = agent_child.try_wait() {
            bail!(
                "agent exited before becoming ready (status {:?})",
                status.code()
            );
        }
        if start.elapsed() > ready_timeout {
            let _ = agent_child.kill();
            bail!("agent did not become ready within {} s", ready_timeout.as_secs());
        }
        if sig_count.load(Ordering::Relaxed) > 0 {
            let _ = agent_child.kill();
            bail!("interrupted while waiting for agent readiness");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let src_child_res = match source.kind.as_str() {
        "loadgen" => spawn_loadgen(root, runtime_yaml, log_dir, &source.params),
        other     => Err(anyhow::anyhow!(
            "unknown source kind {:?} (expected: loadgen)", other
        )),
    };
    let mut src_child = match src_child_res {
        Ok(c) => c,
        Err(e) => {
            let _ = send_sigint(agent_pid);
            let _ = agent_child.wait();
            return Err(e);
        }
    };

    let source_status = loop {
        if let Ok(Some(status)) = src_child.try_wait() {
            break status;
        }
        if sig_count.load(Ordering::Relaxed) == 1 {
            println!("==> Forwarding SIGINT to source");
            let _ = send_sigint(src_child.id());
        }
        if sig_count.load(Ordering::Relaxed) >= 2 {
            eprintln!("==> Second Ctrl+C — killing children");
            let _ = src_child.kill();
            let _ = agent_child.kill();
            bail!("aborted by user");
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    println!("==> Source exited (code {:?}); shutting down agent", source_status.code());
    let _ = send_sigint(agent_pid);

    let shutdown_deadline = Instant::now() + Duration::from_secs(10);
    let agent_status = loop {
        if let Ok(Some(status)) = agent_child.try_wait() {
            break status;
        }
        if Instant::now() > shutdown_deadline {
            eprintln!("==> Agent did not shut down in 10 s — killing");
            let _ = agent_child.kill();
            break agent_child.wait().unwrap_or_else(|_| std::process::exit(1));
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    if !source_status.success() {
        bail!("source exited with status {:?}", source_status.code());
    }
    if !agent_status.success() {
        bail!("agent exited with status {:?}", agent_status.code());
    }
    println!("==> Run complete");
    Ok(())
}

fn spawn_loadgen(
    root:         &Path,
    runtime_yaml: &Path,
    log_dir:      &Path,
    params:       &std::collections::BTreeMap<String, serde_yaml::Value>,
) -> Result<std::process::Child> {
    let loadgen = root.join("target").join("release").join("epico-loadgen");
    if !loadgen.exists() {
        bail!(
            "epico-loadgen binary not found at {}. Run `cargo build \
             --release -p epico-loadgen` first.",
            loadgen.display()
        );
    }

    println!("==> Starting source (loadgen)");
    println!("    binary:     {}", loadgen.display());
    println!("    runtime:    {}", runtime_yaml.display());

    let mut cmd = std::process::Command::new(&loadgen);
    cmd.arg("--config").arg(runtime_yaml);
    cmd.arg("--log-dir").arg(log_dir);

    for (k, v) in params {
        let flag = format!("--{}", k.replace('_', "-"));
        let s = match v {
            serde_yaml::Value::Null          => continue,
            serde_yaml::Value::Bool(true)    => { cmd.arg(&flag); continue; }
            serde_yaml::Value::Bool(false)   => continue,
            serde_yaml::Value::Number(n)     => n.to_string(),
            serde_yaml::Value::String(s)     => s.clone(),
            other => {
                bail!("source param {:?} has unsupported type {:?}", k, other);
            }
        };
        cmd.arg(&flag).arg(&s);
        println!("    {} {}", flag, s);
    }

    cmd.spawn()
        .with_context(|| format!("spawning {:?}", loadgen))
}

#[cfg(unix)]
fn send_sigint(pid: u32) -> std::io::Result<()> {
    let r = unsafe { libc::kill(pid as libc::pid_t, libc::SIGINT) };
    if r == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

#[cfg(not(unix))]
fn send_sigint(_pid: u32) -> std::io::Result<()> { Ok(()) }

fn cmd_bootstrap(project_root: Option<&Path>, cold_start_opt: bool) -> Result<()> {
    let root = resolve_root_for_rootless_cmd(Path::new("pipeline.yaml"), project_root)?;

    build::bootstrap_runtime(&root, cold_start_opt)?;

    let release_dir = root.join("target").join("release");
    let agent = release_dir.join("master");
    let dispatcher = release_dir.join("dispatcher");
    println!("==> Bootstrap complete.");
    println!("    agent:      {}", agent.display());
    println!("    dispatcher: {}", dispatcher.display());
    Ok(())
}

fn cmd_clean(config_path: &Path, project_root: Option<&Path>) -> Result<()> {
    let root = resolve_root_for_rootless_cmd(config_path, project_root)?;

    let target_dir = root.join("target");
    if target_dir.exists() {
        std::fs::remove_dir_all(&target_dir)
            .with_context(|| format!("removing {:?}", target_dir))?;
        println!("Removed {}", target_dir.display());
    } else {
        println!("Nothing to clean at {}", target_dir.display());
    }
    Ok(())
}

fn load_and_resolve(
    config_path: &Path,
    explicit_root: Option<&Path>,
) -> Result<(config::PipelineSpec, PathBuf)> {
    let spec = config::load(config_path)?;

    let root = match explicit_root {
        Some(r) => r.canonicalize().context("resolving --project-root")?,
        None => autodetect_project_root(config_path)?,
    };

    let wit_sdk = root.join("epico-sdk").join("wit").join("epico.wit");
    let wit_legacy = root.join("wit").join("epico.wit");
    let sdk = root.join("epico-sdk").join("Cargo.toml");
    if !sdk.exists() || (!wit_sdk.exists() && !wit_legacy.exists()) {
        bail!(
            "project root {:?} is missing epico-sdk/ or the WIT file. \
             Pass --project-root explicitly if the autodetection is wrong.",
            root
        );
    }

    Ok((spec, root))
}

fn autodetect_project_root(config_path: &Path) -> Result<PathBuf> {
    let start = config_path
        .canonicalize()
        .with_context(|| format!("resolving {:?}", config_path))?;
    let mut cursor = start.parent().unwrap_or_else(|| Path::new("/"));

    for _ in 0..10 {
        if cursor.join("epico-sdk").join("Cargo.toml").exists() {
            return Ok(cursor.to_path_buf());
        }
        match cursor.parent() {
            Some(p) => cursor = p,
            None => break,
        }
    }

    bail!(
        "could not autodetect project root starting from {:?}.",
        config_path
    )
}

fn find_project_root_from_cwd() -> Result<PathBuf> {
    let cwd = std::env::current_dir()
        .context("resolving current working directory")?
        .canonicalize()
        .context("canonicalizing current working directory")?;

    let mut cursor: &Path = &cwd;
    for _ in 0..20 {
        if cursor.join("epico-sdk").join("Cargo.toml").exists()
            && cursor.join("Cargo.toml").exists()
        {
            return Ok(cursor.to_path_buf());
        }
        match cursor.parent() {
            Some(p) => cursor = p,
            None => break,
        }
    }

    bail!("not inside a epico project — walked up from {:?} without finding epico-sdk/Cargo.toml.", cwd)
}

fn resolve_root_for_rootless_cmd(
    config_path: &Path,
    explicit_root: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(r) = explicit_root {
        return r.canonicalize().context("resolving --project-root");
    }

    match find_project_root_from_cwd() {
        Ok(root) => Ok(root),
        Err(cwd_err) => {
            if config_path.exists() {
                autodetect_project_root(config_path)
            } else {
                Err(cwd_err)
            }
        }
    }
}