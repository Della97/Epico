//! Resource sampler. Spawns a background thread that polls the agent
//! process's CPU and RSS once per second via the `sysinfo` crate and
//! pushes the samples into `RunTelemetry`. Cross-platform (Linux +
//! macOS) without per-OS `/proc` vs. Mach code in the hot path.
//!
//! Overhead is dominated by sysinfo's `refresh_process` call (a few ms
//! per tick) — negligible at 1 Hz. The sampler starts immediately after
//! the agent's startup sequence and terminates when the shared
//! `running` flag flips to false.
//!
//! The t_s field on each sample is *seconds since test_start*, so the
//! sample series aligns with the other per-stage timestamps emitted by
//! the autoscaler and collector.

use crate::{ResourceSample, RunTelemetry};
use epico_logger::Logger;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};

/// Start the sampler thread. Returns immediately; the caller does not
/// need to join — the thread exits on its own when `running` is cleared.
///
/// `interval_ms` is the cadence between samples. A value of 0 means the
/// caller has chosen to disable resource sampling; that's handled at the
/// call site (we don't spawn at all), so this function asserts a positive
/// interval and panics otherwise to surface call-site bugs immediately.
pub(crate) fn spawn(
    telemetry:   Arc<Mutex<RunTelemetry>>,
    running:     Arc<AtomicBool>,
    test_start:  Instant,
    interval_ms: u64,
    log:         Logger,
) {
    assert!(
        interval_ms > 0,
        "resources::spawn called with interval_ms=0 — caller should skip the spawn entirely",
    );
    std::thread::spawn(move || sample_loop(telemetry, running, test_start, interval_ms, log));
}

fn sample_loop(
    telemetry:   Arc<Mutex<RunTelemetry>>,
    running:     Arc<AtomicBool>,
    test_start:  Instant,
    interval_ms: u64,
    log:         Logger,
) {
    // Build a sysinfo::System that only refreshes what we need — process
    // CPU + memory. Default RefreshKind refreshes everything (disks,
    // networks, users, …) and that's pure overhead here.
    let refresh = RefreshKind::new().with_processes(
        ProcessRefreshKind::new()
            .with_cpu()
            .with_memory(),
    );
    let mut sys = System::new_with_specifics(refresh);
    let pid = Pid::from_u32(std::process::id());
    let cpu_norm_cpus = detect_available_cpus();

    // First refresh establishes baseline CPU counters. The reported
    // cpu_usage() is zero until the second refresh, so we discard the
    // first sample to avoid polluting the series with a leading zero.
    sys.refresh_processes_specifics(
        ProcessRefreshKind::new().with_cpu().with_memory(),
    );
    std::thread::sleep(Duration::from_millis(interval_ms));

    log.info("resource sampler started", &[
        ("interval_ms", &interval_ms.to_string()),
        ("pid",         &pid.as_u32().to_string()),
        ("cpu_norm_cpus", &cpu_norm_cpus.to_string()),
    ]);

    while running.load(Ordering::Relaxed) {
        sys.refresh_processes_specifics(
            ProcessRefreshKind::new().with_cpu().with_memory(),
        );

        let sample = match sys.process(pid) {
            Some(p) => ResourceSample {
                t_s:       test_start.elapsed().as_secs_f64(),
                // sysinfo reports process CPU in core-percent units
                // (100 == one saturated core). Normalize by CPUs available to
                // this process so cpu_pct is true utilization percentage.
                cpu_pct:   p.cpu_usage() / cpu_norm_cpus as f32,
                rss_bytes: p.memory(),
            },
            None => {
                // We're our own process — this shouldn't happen except in
                // extreme teardown scenarios.
                log.warn("resource sampler: own pid missing", &[]);
                break;
            }
        };

        if let Ok(mut tel) = telemetry.lock() {
            tel.resource_samples.push(sample);
        }

        std::thread::sleep(Duration::from_millis(interval_ms));
    }

    log.info("resource sampler stopped", &[]);
}

fn detect_available_cpus() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Some(n) = linux_affinity_cpu_count() {
            return n.max(1);
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(target_os = "linux")]
fn linux_affinity_cpu_count() -> Option<usize> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    let list = text
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))?;
    parse_linux_cpu_list_count(list)
}

#[cfg(target_os = "linux")]
fn parse_linux_cpu_list_count(list: &str) -> Option<usize> {
    let mut total = 0usize;
    for part in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if let Some((a, b)) = part.split_once('-') {
            let start = a.trim().parse::<usize>().ok()?;
            let end = b.trim().parse::<usize>().ok()?;
            if end < start {
                return None;
            }
            total += end - start + 1;
        } else {
            let _ = part.parse::<usize>().ok()?;
            total += 1;
        }
    }
    if total == 0 { None } else { Some(total) }
}