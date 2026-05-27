//! Dispatcher subprocess supervision and graceful shutdown.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{bail, Result};
use epico_logger::Logger;

use crate::config::DispatcherConfig;

pub(crate) static CHILDREN: Mutex<Vec<Child>> = Mutex::new(Vec::new());
pub(crate) static SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn install_shutdown_handler() {
    ctrlc::set_handler(|| {
        eprintln!("\n[agent] Ctrl+C received; shutting down...");
        SHUTDOWN.store(true, Ordering::Relaxed);
    })
    .expect("installing Ctrl+C handler");
}

pub(crate) fn spawn_dispatchers(
    dispatchers: &[DispatcherConfig],
    dispatcher_bin: &Path,
    log: &Logger,
) {
    log.info("dispatcher binary", &[("path", &dispatcher_bin.display().to_string())]);

    let mut children = CHILDREN.lock().unwrap();
    for d in dispatchers.iter().rev() {
        // push_uri is authoritative when set — the dispatcher will bind
        // exactly that URI and nothing else. push_port stays optional and
        // is only consulted when push_uri is absent (inner dispatchers,
        // which we still auto-assign). ctrl_port and pull_port are always
        // required; the ingress override only affects the frontend side.
        let push_port = d.push_port;
        let push_uri  = d.push_uri.as_deref();
        if push_uri.is_none() && push_port.is_none() {
            log.error(
                "dispatcher missing both push_port and push_uri",
                &[("name", &d.name)],
            );
            std::process::exit(1);
        }
        let pull = d.pull_port.unwrap_or_else(|| {
            log.error("dispatcher missing pull_port", &[("name", &d.name)]);
            std::process::exit(1);
        });
        let ctrl = d.ctrl_port;
        let target_stage = d.name.replacen("dispatch-", "fn-", 1);

        let mut info: Vec<(&str, String)> = vec![
            ("name", d.name.clone()),
            ("pull", pull.to_string()),
            ("ctrl", ctrl.to_string()),
        ];
        if let Some(uri) = push_uri {
            info.push(("push_uri", uri.to_string()));
        }
        if let Some(p) = push_port {
            info.push(("push_port", p.to_string()));
        }
        log.info(
            "starting dispatcher",
            &info
                .iter()
                .map(|(k, v)| (*k, v.as_str()))
                .collect::<Vec<_>>(),
        );

        let mut cmd = Command::new(dispatcher_bin);
        cmd.arg("--name").arg(&d.name)
            .arg("--pull-port").arg(pull.to_string())
            .arg("--ctrl-port").arg(ctrl.to_string())
            .arg("--stage").arg(&target_stage);
        if let Some(uri) = push_uri {
            cmd.arg("--push-uri").arg(uri);
        }
        if let Some(p) = push_port {
            cmd.arg("--push-port").arg(p.to_string());
        }
        // Forward the credit window. With the default of 1 this is a
        // no-op compatibility-wise but it keeps the agent and the
        // dispatcher subprocess in agreement on the protocol contract.
        cmd.arg("--credit-window").arg(d.credit_window.to_string());

        let child = cmd.spawn().unwrap_or_else(|e| {
            log.error("failed to spawn dispatcher", &[
                ("name", &d.name),
                ("err",  &e.to_string()),
            ]);
            std::process::exit(1);
        });

        children.push(child);
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(children);
    std::thread::sleep(Duration::from_millis(300));
}

pub(crate) fn kill_children(log: &Logger) {
    let mut children = CHILDREN.lock().unwrap();
    log.info("killing dispatcher children", &[("count", &children.len().to_string())]);
    for child in children.iter_mut() {
        let _ = child.kill();
    }
    for child in children.iter_mut() {
        let _ = child.wait();
    }
}

pub(crate) fn resolve_dispatcher_binary(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() { return Ok(p.to_path_buf()); }
        bail!("--dispatcher-bin {:?} does not exist", p);
    }

    if let Ok(env_path) = std::env::var("EPICO_DISPATCHER") {
        let p = PathBuf::from(env_path);
        if p.exists() { return Ok(p); }
    }

    let exe     = std::env::current_exe()?;
    let exe_dir = exe.parent().map(|p| p.to_path_buf()).unwrap_or_default();

    let sibling = exe_dir.join("dispatcher");
    if sibling.exists() { return Ok(sibling); }

    let mut cursor = exe_dir.as_path();
    for _ in 0..8 {
        let candidate = cursor.join("target").join("release").join("dispatcher");
        if candidate.exists() { return Ok(candidate); }
        match cursor.parent() {
            Some(p) => cursor = p,
            None    => break,
        }
    }

    bail!(
        "dispatcher binary not found next to agent ({:?}) or at target/release/dispatcher. \
         Run `cargo build --release --workspace` first.",
        exe_dir
    )
}