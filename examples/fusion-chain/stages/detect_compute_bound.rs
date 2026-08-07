// Stage: detect (COMPUTE-BOUND variant) — the `b` half of the negative control
// for the cost-model controller. See `normalize_compute_bound.rs` for why this
// fixture exists; the two are calibrated to the same `SPIN` so neither half is
// the obvious bottleneck and the pair is compute-bound as a pair.
//
// Distinct from `detect_heavy.rs`, which is the phase-4 SKEW arm: that one is
// 20x `normalize` in order to make the two halves UNBALANCED, and at ~34 us
// against a ~2 ms edge it is still edge-dominated. This one is about absolute
// cost, not the ratio between halves.

use epico_sdk::stage;

/// Matches `normalize_compute_bound.rs`. Balanced on purpose: skew is a
/// different question, and mixing the two would leave a failure ambiguous.
const SPIN: u32 = 200_000;

#[inline(never)]
fn heavy(seed: f64) -> f64 {
    let mut acc = seed;
    for i in 0..SPIN {
        acc = acc * 1.000_000_1 + (i as f64) * 1e-12;
    }
    acc
}

stage! {
    fn detect(ev: Reading) -> Reading {
        let acc = heavy(ev.value);
        Reading {
            sensor_id: ev.sensor_id,
            value: if acc.is_finite() {
                if ev.value > 75.0 { ev.value } else { ev.value * 0.5 }
            } else {
                -1.0
            },
        }
    }
}
