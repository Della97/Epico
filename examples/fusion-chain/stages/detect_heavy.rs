// Stage: detect (SKEWED variant) — deliberately ~20x the cost of `normalize`.
//
// Used only by `.morph_skew.yaml`. The uniform-cost `detect.rs` is what every
// other config runs; this one exists to create the ONE regime the design note
// says scission is for: "AB's queue grows while only one half's wasm_us grew —
// the per-stage hops still attribute time to A and B separately, so we know
// WHICH half is hot."
//
// Two things it has to get right to be a fair probe:
//
//   * The work must be REAL and unremovable. The accumulator feeds a branch that
//     decides the output, so neither Cranelift nor LLVM can drop the loop. It
//     stays deterministic — `acc` is always finite for any sane input — so the
//     event stream is bit-identical to the cheap variant's and conservation
//     assertions still hold.
//   * The cost must be CPU, not sleep. A blocking half would confound the
//     measurement: it would test thread-occupancy effects rather than the
//     service-time skew the controller would actually diagnose from `wasm_us`.

use epico_sdk::stage;

/// Iterations of the spin loop. Calibrated so this half costs roughly 20x
/// `normalize`, which is a skew large enough that a controller could not
/// mistake it for noise.
const SPIN: u32 = 20_000;

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
            // Same value the cheap variant emits, guarded by a branch on the
            // accumulator so the loop above cannot be optimized away.
            value: if acc.is_finite() { ev.value } else { -1.0 },
        }
    }
}
