//! Native in-process source for the three-stage pipeline.
//!
//! Generates synthetic sensor events matching the loadgen/bench schema so the
//! stages parse them unchanged. Supports fan-out: with K source threads
//! (`EPICO_SOURCE_THREADS=K`) the runtime calls `set_partition(i, K)` on each
//! replica, and each emits a disjoint slice of the sequence space
//! (`seq = i, i+K, i+2K, …`), so K replicas produce the full `count` exactly
//! once — no duplicates.
//!
//! NOTE: this carries the full bench schema, so its per-event cost is ~the
//! built-in generator's (~15 µs). If you had a leaner source, keep its event
//! body and only graft on the `stride` field, `set_partition`, and the
//! `seq += self.stride` step.

use epico_master::{EventSource, wall_now};

// (type_name, unit, base_value) — mirrors the loadgen's sensor table.
const TYPES: &[(&str, &str, f64)] = &[
    ("temperature", "\u{00b0}C", 22.0),
    ("vibration",   "mm/s",       1.2),
    ("pressure",    "kPa",      101.3),
    ("humidity",    "%",         45.0),
    ("current",     "A",          3.5),
];
const LOCATIONS: &[&str] = &["zone-A", "zone-B", "zone-C", "zone-D"];

pub struct Source {
    seq:     u64, // next sequence number this replica will emit
    count:   u64, // global cap across all replicas
    stride:  u64, // 1 single-threaded; = num_threads when fanned out
    sensors: u64,
}

impl Source {
    pub fn new() -> Self {
        let count = std::env::var("EPICO_SOURCE_COUNT")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(4_000_000);
        let sensors = std::env::var("EPICO_SOURCE_SENSORS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(100);
        Source { seq: 0, count, stride: 1, sensors }
    }
}

impl EventSource for Source {
    fn set_partition(&mut self, index: usize, total: usize) {
        self.seq    = index as u64;
        self.stride = (total as u64).max(1);
    }

    fn next_event(&mut self) -> Option<Vec<u8>> {
        let seq = self.seq;
        if seq >= self.count {
            return None;
        }
        self.seq += self.stride;

        let n   = self.sensors.max(1);
        let idx = (seq % n) as usize;
        let (type_name, unit, base) = TYPES[idx % TYPES.len()];
        let location   = LOCATIONS[idx % LOCATIONS.len()];
        let value      = base + ((seq % 211) as f64) * 0.01;
        let is_anomaly = seq % 500 == 0;
        let now_wall   = wall_now();

        serde_json::to_vec(&serde_json::json!({
            "bench_ts":      now_wall,
            "bench_ts_wall": now_wall,
            "bench_seq":     seq,
            "sensor_id":     format!("sensor-{:04}", idx),
            "sensor_type":   type_name,
            "location":      location,
            "unit":          unit,
            "value":         (value * 10_000.0).round() / 10_000.0,
            "is_anomaly":    is_anomaly,
        }))
        .ok()
    }
}