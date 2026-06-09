//! `epico-loadgen` — IoT sensor stream load generator.
//!
//! Profiles:
//!   steady  — constant rate for the whole run (duration-bounded)
//!   ramp    — linear ramp from --ramp-start to --rate over --ramp-time s
//!   burst   — steady baseline with periodic spikes to --rate
//!   wave    — sinusoidal between --wave-min and --rate
//!   tp      — throughput-max: send --count events as fast as the socket
//!             accepts them, then emit an EOS marker. No duration target,
//!             no token bucket, no per-tick sleeping. Used for measuring
//!             peak end-to-end throughput.
//!
//! # End-of-stream (EOS)
//!
//! In `tp` mode the loadgen emits an EOS marker after the requested
//! `--count` events have been pushed. The marker is a JSON payload with
//! a sentinel field:
//!
//!     {"__epico_eos": true,
//!      "loadgen_sent":  <count>,
//!      "expected_count":<count>,
//!      "loadgen_done_ts": <unix_secs_f64>}
//!
//! The agent's collector and every worker recognize this field, forward
//! it unchanged to the next stage, and treat it as the signal that the
//! stream is finished. The collector additionally triggers agent shutdown
//! so `epico run` returns a clean exit code.

use anyhow::{bail, Context, Result};
use clap::Parser;
use epico_logger::Logger;
use rand::Rng;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// CLI
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "epico-loadgen")]
struct Args {
    /// Path to runtime YAML produced by `epico build` or to a raw pipeline YAML.
    #[arg(long)]
    config: String,

    /// Override entry-stage push endpoint. Default: derived from config.
    #[arg(long)]
    entry: Option<String>,

    /// JSONL log directory. Defaults to `./logs`.
    #[arg(long, default_value = "logs")]
    log_dir: PathBuf,

    /// Target events per second for steady/ramp/burst/wave. Ignored for tp.
    #[arg(long, default_value_t = 1000)]
    rate: u64,

    /// Run duration in seconds. Ignored for tp.
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Number of distinct sensors to simulate.
    #[arg(long, default_value_t = 100)]
    sensors: usize,

    /// Load profile: steady | ramp | burst | wave | tp
    #[arg(long, default_value = "steady")]
    profile: String,

    /// (tp profile) Total events to send before emitting EOS.
    #[arg(long, default_value_t = 1_000_000)]
    count: u64,

    /// (ramp profile) Initial rate at t=0.
    #[arg(long, default_value_t = 100)]
    ramp_start: u64,
    /// (ramp profile) Seconds to reach --rate from --ramp-start.
    #[arg(long, default_value_t = 30)]
    ramp_time: u64,

    /// (burst profile) Baseline rate outside spikes.
    #[arg(long, default_value_t = 500)]
    burst_base_rate: u64,
    /// (burst profile) Seconds between spike starts.
    #[arg(long, default_value_t = 10)]
    burst_interval: u64,
    /// (burst profile) Seconds each spike lasts.
    #[arg(long, default_value_t = 2)]
    burst_duration: u64,

    /// (wave profile) Minimum rate (sine trough).
    #[arg(long, default_value_t = 200)]
    wave_min: u64,
    /// (wave profile) Period of the sine in seconds.
    #[arg(long, default_value_t = 60)]
    wave_period: u64,

    /// Force entry to be ipc/tcp regardless of config. For benchmarking.
    #[arg(long)]
    force_entry_scheme: Option<String>,

    /// External entry-point override (rarely used; only relevant when the
    /// loadgen connects to a non-epico ingress for cross-stack tests).
    #[arg(long)]
    external_entry: Option<String>,

    /// (tp profile) Reuse ONE pre-serialized event buffer instead of
    /// generating a fresh event per send. Removes the loadgen's per-event
    /// rng + serde cost (~10-15 us) so a single producer thread can saturate
    /// any in-process transport. THROUGHPUT ONLY: `bench_ts` is stamped once,
    /// so e2e latency is meaningless under --blast. Ignored unless profile=tp.
    #[arg(long)]
    blast: bool,
}

// ============================================================================
// Time helpers
// ============================================================================

fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn perf_now() -> f64 {
    // Monotonic seconds since some unspecified epoch. Used internally for
    // per-hop latency math where wall-clock drift would corrupt results.
    let dur = Instant::now().elapsed();
    dur.as_secs_f64()
}

// ============================================================================
// Config parsing (runtime.yaml from CLI) — minimal subset
//
// We parse the YAML as a generic `serde_yaml::Value` and pluck out only the
// fields the loadgen needs (entry-stage ingress URI, push port, stage names).
// This avoids pulling in `serde` as a direct dependency just for one tiny
// struct, and is forgiving of unknown fields the CLI may add.
// ============================================================================

fn load_entry_addr(path: &str) -> Result<(String, u16, Vec<String>)> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("opening {path}"))?;
    let yaml: serde_yaml::Value = serde_yaml::from_str(&raw)
        .with_context(|| format!("parsing {path}"))?;

    let pipeline = yaml.get("pipeline")
        .and_then(|v| v.as_sequence())
        .ok_or_else(|| anyhow::anyhow!("config {} has no pipeline", path))?;

    if pipeline.is_empty() {
        bail!("config {} has empty pipeline", path);
    }

    let stage_names: Vec<String> = pipeline.iter()
        .filter_map(|s| s.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect();

    // Prefer the first stage's `ingress` field; fall back to the first
    // dispatcher's `push_uri` if the stage doesn't have one.
    let entry_stage = &pipeline[0];
    if let Some(uri) = entry_stage.get("ingress").and_then(|v| v.as_str()) {
        return Ok((uri.to_string(), 0, stage_names));
    }

    if let Some(dispatchers) = yaml.get("dispatchers").and_then(|v| v.as_sequence()) {
        if let Some(d) = dispatchers.first() {
            if let Some(uri) = d.get("push_uri").and_then(|v| v.as_str()) {
                let port = d.get("push_port")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u16;
                return Ok((uri.to_string(), port, stage_names));
            }
        }
    }

    bail!("could not derive entry ingress URI from {path}");
}

// ============================================================================
// Sensors
// ============================================================================

#[derive(Clone)]
struct SensorType {
    unit:        &'static str,
    mean:        f64,
    std:         f64,
    drift_per_s: f64,
    anomaly_mag: f64,
}

const SENSOR_TYPES: &[(&str, SensorType)] = &[
    ("temperature", SensorType { unit: "°C",   mean: 22.0,  std: 0.5, drift_per_s: 0.002, anomaly_mag: 15.0 }),
    ("vibration",   SensorType { unit: "mm/s", mean: 1.2,   std: 0.3, drift_per_s: 0.001, anomaly_mag: 12.0 }),
    ("pressure",    SensorType { unit: "kPa",  mean: 101.3, std: 0.2, drift_per_s: 0.001, anomaly_mag:  8.0 }),
    ("humidity",    SensorType { unit: "%",    mean: 45.0,  std: 2.0, drift_per_s: 0.005, anomaly_mag: 25.0 }),
    ("current",     SensorType { unit: "A",    mean: 3.5,   std: 0.4, drift_per_s: 0.003, anomaly_mag:  6.0 }),
];
const LOCATIONS: &[&str] = &["zone-A", "zone-B", "zone-C", "zone-D"];
const ANOMALY_PROB: f64  = 0.002;

struct Sensor {
    id:          String,
    type_idx:    usize,
    location:    &'static str,
    value:       f64,
    drift_dir:   f64,
    drift_acc:   f64,
}

impl Sensor {
    fn new(idx: usize) -> Self {
        let type_idx = idx % SENSOR_TYPES.len();
        let st       = &SENSOR_TYPES[type_idx].1;
        let mut rng  = rand::thread_rng();
        Sensor {
            id:        format!("sensor-{:04}", idx),
            type_idx,
            location:  LOCATIONS[idx % LOCATIONS.len()],
            value:     st.mean + rng.gen::<f64>() * st.std,
            drift_dir: if rng.gen_bool(0.5) { 1.0 } else { -1.0 },
            drift_acc: 0.0,
        }
    }

    /// Returns a JSON-serialised event bytes + whether it is an anomaly.
    fn reading(&mut self, seq: u64) -> (Vec<u8>, bool) {
        let (type_name, st) = &SENSOR_TYPES[self.type_idx];
        self.drift_acc += st.drift_per_s * self.drift_dir;
        if self.drift_acc.abs() > 3.0 * st.std {
            self.drift_dir *= -1.0;
        }

        let mut rng    = rand::thread_rng();
        let mut value  = self.value + self.drift_acc + rng.gen::<f64>() * st.std * 0.3;
        let is_anomaly = rng.gen::<f64>() < ANOMALY_PROB;
        if is_anomaly {
            let sign  = if rng.gen_bool(0.5) { 1.0 } else { -1.0 };
            value    += sign * st.anomaly_mag * (0.8 + rng.gen::<f64>() * 0.4);
        }
        self.value = value;

        let now_wall = wall_now();
        let now_perf = perf_now();

        let bytes = serde_json::to_vec(&serde_json::json!({
            "bench_ts":      now_perf,
            "bench_ts_wall": now_wall,
            "bench_seq":     seq,
            "sensor_id":     self.id,
            "sensor_type":   type_name,
            "location":      self.location,
            "unit":          st.unit,
            "value":         (value * 10_000.0).round() / 10_000.0,
            "is_anomaly":    is_anomaly,
        }))
        .unwrap_or_default();

        (bytes, is_anomaly)
    }
}

/// Build the canonical EOS marker payload. The runtime detects EOS via the
/// top-level boolean field `__epico_eos`; any other fields are advisory
/// metadata that travels with the marker for diagnostics.
fn eos_payload(loadgen_sent: u64, expected_count: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "__epico_eos":     true,
        "loadgen_sent":      loadgen_sent,
        "expected_count":    expected_count,
        "loadgen_done_ts":   wall_now(),
        "loadgen_done_perf": perf_now(),
    }))
    .unwrap_or_default()
}

// ============================================================================
// Load profiles
// ============================================================================

/// Sub-struct so we can ship just the profile knobs into the producer thread.
struct ProfileArgs {
    rate:             u64,
    profile:          String,
    ramp_start:       u64,
    ramp_time:        u64,
    burst_base_rate:  u64,
    burst_interval:   u64,
    burst_duration:   u64,
    wave_min:         u64,
    wave_period:      u64,
}

fn profile_rate(args: &ProfileArgs, elapsed: f64) -> u64 {
    match args.profile.as_str() {
        "ramp" => {
            let t = args.ramp_time as f64;
            if elapsed >= t {
                args.rate
            } else {
                (args.ramp_start as f64
                    + (args.rate - args.ramp_start) as f64 * elapsed / t) as u64
            }
        }
        "burst" => {
            let interval = args.burst_interval as f64;
            let dur      = args.burst_duration as f64;
            if elapsed % interval < dur { args.rate } else { args.burst_base_rate }
        }
        "wave" => {
            let t = (f64::sin(2.0 * std::f64::consts::PI * elapsed / args.wave_period as f64)
                     + 1.0) / 2.0;
            (args.wave_min as f64 + (args.rate - args.wave_min) as f64 * t) as u64
        }
        _ => args.rate, // steady (and tp; tp doesn't use this path)
    }
}

fn profile_description(args: &Args) -> String {
    match args.profile.as_str() {
        "ramp"  => format!("ramp {}→{} ev/s over {}s", args.ramp_start, args.rate, args.ramp_time),
        "burst" => format!("burst {}→{} ev/s every {}s for {}s",
                           args.burst_base_rate, args.rate,
                           args.burst_interval, args.burst_duration),
        "wave"  => format!("wave {}–{} ev/s period {}s", args.wave_min, args.rate, args.wave_period),
        "tp"    => format!("throughput-max ({} events as fast as possible, then EOS)",
                           args.count),
        _       => format!("steady {} ev/s", args.rate),
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<()> {
    let args = Args::parse();

    let log = Logger::new("loadgen", &args.log_dir)
        .with_context(|| format!("opening log dir {}", args.log_dir.display()))?;

    let (entry_uri, entry_port, pipeline) = load_entry_addr(&args.config)?;

    let addr = match (&args.entry, &args.external_entry) {
        (Some(e), _) => e.clone(),
        (_, Some(e)) => e.clone(),
        _ => match args.force_entry_scheme.as_deref() {
            Some("tcp") if entry_port > 0 => format!("tcp://127.0.0.1:{entry_port}"),
            _ => entry_uri,
        },
    };

    log.info("configuration", &[
        ("config",    &args.config),
        ("profile",   &profile_description(&args)),
        ("rate",      &args.rate.to_string()),
        ("duration",  &args.duration.to_string()),
        ("count",     &args.count.to_string()),
        ("sensors",   &args.sensors.to_string()),
        ("entry_addr",&addr),
        ("stages",    &pipeline.len().to_string()),
    ]);

    // ── ZMQ PUSH ──────────────────────────────────────────────────────────────
    let ctx  = zmq::Context::new();
    let push = ctx.socket(zmq::PUSH).context("creating PUSH socket")?;
    push.set_sndhwm(100_000).ok();
    push.set_linger(0).ok();
    push.connect(&addr).with_context(|| format!("connecting to {addr}"))?;

    log.info("producer connected", &[("addr", &addr)]);

    let sent      = Arc::new(AtomicU64::new(0));
    let dropped   = Arc::new(AtomicU64::new(0));
    let anomalies = Arc::new(AtomicU64::new(0));
    let running   = Arc::new(AtomicBool::new(true));

    {
        let r = running.clone();
        ctrlc::set_handler(move || {
            r.store(false, Ordering::Relaxed);
        })
        .ok();
    }

    let mut sensors  = (0..args.sensors).map(Sensor::new).collect::<Vec<_>>();
    let n_sensors    = sensors.len();
    let duration     = Duration::from_secs(args.duration);

    let sent_c    = sent.clone();
    let dropped_c = dropped.clone();
    let anom_c    = anomalies.clone();
    let running_c = running.clone();
    let log_prod  = log.with_component("loadgen/producer");

    let tp_count   = args.count;

    let args_rate      = args.rate;
    let args_profile   = args.profile.clone();
    let args_ramp_start   = args.ramp_start;
    let args_ramp_time    = args.ramp_time;
    let args_burst_base   = args.burst_base_rate;
    let args_burst_int    = args.burst_interval;
    let args_burst_dur    = args.burst_duration;
    let args_wave_min     = args.wave_min;
    let args_wave_period  = args.wave_period;
    let args_blast        = args.blast;

    let producer = std::thread::spawn(move || {
        if args_profile == "tp" {
            // ── Throughput-max mode ───────────────────────────────────────────
            // Push events as fast as the socket accepts them; no token bucket,
            // no per-tick sleep. The socket's SNDHWM (100k) plus PUSH's natural
            // backpressure provide the upper bound. After tp_count successful
            // sends we emit the EOS marker and stop.
            //
            // We use a blocking send so backpressure from a saturated downstream
            // dispatcher actually slows us down rather than dropping events on
            // the floor. The throughput we measure at the collector is then the
            // true sustained capacity of the pipeline, not the loadgen's
            // unbounded transmit rate.
            log_prod.info("tp mode: starting", &[("count", &tp_count.to_string())]);
            let mut si  = 0usize;
            let mut seq = 0u64;
            let mut sent_n = 0u64;
            let mut anom_n = 0u64;

            if args_blast {
                // Pre-serialize a single event once, then resend the same
                // bytes so the hot loop is nothing but push.send(). This
                // removes the per-event rng + serde cost that otherwise caps a
                // single producer thread at ~60-100k ev/s, letting the loadgen
                // outpace (and therefore actually measure) the transport under
                // test. bench_ts is frozen at build time, so DO NOT read e2e
                // latency from a --blast run.
                let (cached, _) = sensors[0].reading(0);
                log_prod.info("tp blast mode (cached event, latency invalid)", &[
                    ("bytes", &cached.len().to_string()),
                    ("count", &tp_count.to_string()),
                ]);
                while running_c.load(Ordering::Relaxed) && sent_n < tp_count {
                    match push.send(&cached as &[u8], 0) {
                        Ok(_)  => sent_n += 1,
                        Err(_) => { dropped_c.fetch_add(1, Ordering::Relaxed); }
                    }
                }
            } else {
                while running_c.load(Ordering::Relaxed) && sent_n < tp_count {
                    let sensor = &mut sensors[si % n_sensors];
                    si += 1;
                    let (bytes, is_anom) = sensor.reading(seq);
                    seq += 1;
                    match push.send(&bytes as &[u8], 0) {
                        Ok(_)  => {
                            sent_n += 1;
                            if is_anom { anom_n += 1; }
                        }
                        Err(_) => {
                            dropped_c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            sent_c.fetch_add(sent_n, Ordering::Relaxed);
            anom_c.fetch_add(anom_n, Ordering::Relaxed);

            // ── EOS marker ────────────────────────────────────────────────────
            // Sent with a blocking send so we don't lose the marker under HWM
            // pressure. If the loop above sent N < count because of Ctrl+C,
            // include the actual N in the marker for diagnostics.
            let eos = eos_payload(sent_n, tp_count);
            match push.send(&eos as &[u8], 0) {
                Ok(_) => log_prod.info("EOS sent", &[
                    ("loadgen_sent", &sent_n.to_string()),
                    ("expected",     &tp_count.to_string()),
                ]),
                Err(e) => log_prod.error("EOS send failed", &[("err", &e.to_string())]),
            }
            running_c.store(false, Ordering::Relaxed);
            log_prod.info("producer finished (tp)", &[
                ("sent",     &sent_n.to_string()),
                ("dropped",  &dropped_c.load(Ordering::Relaxed).to_string()),
                ("anomalies",&anom_n.to_string()),
            ]);
            return;
        }

        // ── Rate-controlled mode (steady / ramp / burst / wave) ───────────────
        const TICK: Duration = Duration::from_millis(1);
        let start   = Instant::now();
        let mut si  = 0usize;
        let mut seq = 0u64;
        let mut tokens = 0.0f64;
        let mut last   = Instant::now();

        let profile_args = ProfileArgs {
            rate: args_rate, profile: args_profile,
            ramp_start: args_ramp_start, ramp_time: args_ramp_time,
            burst_base_rate: args_burst_base,
            burst_interval: args_burst_int, burst_duration: args_burst_dur,
            wave_min: args_wave_min, wave_period: args_wave_period,
        };

        while running_c.load(Ordering::Relaxed) && start.elapsed() < duration {
            let now     = Instant::now();
            let elapsed = start.elapsed().as_secs_f64();
            let dt      = now.duration_since(last).as_secs_f64();
            last        = now;

            let target = profile_rate(&profile_args, elapsed).max(1);
            tokens = (tokens + target as f64 * dt).min(target as f64);

            let n_emit = tokens as u64;
            if n_emit == 0 {
                std::thread::sleep(TICK);
                continue;
            }
            tokens -= n_emit as f64;

            let mut sent_n = 0u64;
            let mut anom_n = 0u64;
            for _ in 0..n_emit {
                let sensor = &mut sensors[si % n_sensors];
                si += 1;
                let (bytes, is_anom) = sensor.reading(seq);
                seq += 1;
                if is_anom { anom_n += 1; }
                match push.send(&bytes as &[u8], zmq::DONTWAIT) {
                    Ok(_)  => { sent_n += 1; }
                    Err(_) => { dropped_c.fetch_add(1, Ordering::Relaxed); }
                }
            }
            sent_c.fetch_add(sent_n, Ordering::Relaxed);
            anom_c.fetch_add(anom_n, Ordering::Relaxed);

            std::thread::sleep(TICK);
        }
        running_c.store(false, Ordering::Relaxed);
        log_prod.info("producer finished", &[
            ("sent",     &sent_c.load(Ordering::Relaxed).to_string()),
            ("dropped",  &dropped_c.load(Ordering::Relaxed).to_string()),
            ("anomalies",&anom_c.load(Ordering::Relaxed).to_string()),
        ]);
    });

    // ── Progress loop ─────────────────────────────────────────────────────────
    let mut last_print = Instant::now();
    let log_progress   = log.with_component("loadgen/progress");

    while running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(500));
        if last_print.elapsed() >= Duration::from_secs(2) {
            log_progress.info("progress", &[
                ("sent",     &sent.load(Ordering::Relaxed).to_string()),
                ("dropped",  &dropped.load(Ordering::Relaxed).to_string()),
                ("anomalies",&anomalies.load(Ordering::Relaxed).to_string()),
            ]);
            last_print = Instant::now();
        }
    }

    let _ = producer.join();
    log.info("exiting", &[
        ("total_sent",      &sent.load(Ordering::Relaxed).to_string()),
        ("total_dropped",   &dropped.load(Ordering::Relaxed).to_string()),
        ("total_anomalies", &anomalies.load(Ordering::Relaxed).to_string()),
    ]);

    Ok(())
}