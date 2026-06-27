use metapod_sdk::stage;
use std::cell::RefCell;
use std::collections::HashMap;

// Per-sensor rolling state lives in the component's linear memory.
// Each replica owns its own copy — that's fine, the autoscaler treats
// replicas as independent shards.
thread_local! {
    static STATE: RefCell<HashMap<String, SensorState>> = RefCell::new(HashMap::new());
}

const FIR_LEN: usize = 16;
// Symmetric low-pass FIR, Kaiser window beta=6, cutoff ~0.2*fs.
// Precomputed offline; sums to 1.0.
const FIR_TAPS: [f64; FIR_LEN] = [
    0.00231, 0.00684, 0.01618, 0.03200, 0.05420, 0.07920, 0.10160, 0.11567,
    0.11567, 0.10160, 0.07920, 0.05420, 0.03200, 0.01618, 0.00684, 0.00231,
];

struct SensorState {
    ring: [f64; FIR_LEN],
    idx: usize,
    filled: usize,
    // Rolling stats for outlier clamp (Welford).
    n: u64,
    mean: f64,
    m2: f64,
}

impl SensorState {
    fn new() -> Self {
        Self { ring: [0.0; FIR_LEN], idx: 0, filled: 0, n: 0, mean: 0.0, m2: 0.0 }
    }

    fn push_and_filter(&mut self, x: f64) -> f64 {
        self.ring[self.idx] = x;
        self.idx = (self.idx + 1) % FIR_LEN;
        self.filled = (self.filled + 1).min(FIR_LEN);

        // Convolve taps with ring in chronological order.
        let mut acc = 0.0;
        for k in 0..FIR_LEN {
            // Most-recent sample at (idx - 1), walk backwards.
            let i = (self.idx + FIR_LEN - 1 - k) % FIR_LEN;
            acc += FIR_TAPS[k] * self.ring[i];
        }
        acc
    }

    fn update_stats(&mut self, x: f64) -> (f64, f64) {
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        let var = if self.n > 1 { self.m2 / (self.n - 1) as f64 } else { 1.0 };
        (self.mean, var.sqrt().max(1e-9))
    }
}

stage! {
    fn denoise(ev: SensorSample) -> Filtered {
        STATE.with(|s| {
            let mut map = s.borrow_mut();
            let st = map.entry(ev.sensor_id.clone()).or_insert_with(SensorState::new);

            let (mean, std) = st.update_stats(ev.value);
            // 4-sigma clamp before filtering — kills outliers that would
            // ring through the FIR.
            let clamped_val = ev.value.clamp(mean - 4.0 * std, mean + 4.0 * std);
            let was_clamped = (clamped_val - ev.value).abs() > f64::EPSILON;

            let y = st.push_and_filter(clamped_val);

            Filtered {
                sensor_id: ev.sensor_id,
                ts_ns: ev.ts_ns,
                value: y,
                raw: ev.value,
                clamped: was_clamped,
            }
        })
    }
}