// Stage: normalize (COMPUTE-BOUND variant) — the `a` half of the negative
// control for the cost-model controller.
//
// Every other fixture in this example makes the halves minimal-compute on
// purpose, because that is the regime where fusion pays and therefore the
// regime break-even has to cover. This one exists to build the opposite
// regime, which the M2 plan flags as the gap that leaves the controller's
// refusal branch untested:
//
//   > Diagnosis needs a compute-bound counter-case. The refusal branch is
//   > untested until there is a pipeline where fusion genuinely does not help.
//   > The skew arm from phase 4 is close: `detect` at 34 us against a ~2 ms
//   > edge is still edge-dominated, so a THIRD fixture is needed with
//   > per-event compute above the edge cost.
//
// This is that third fixture. `SPIN` is an order of magnitude above the skew
// arm's, putting per-event wasm cost in the hundreds of microseconds while the
// paced source in `.controller_negative_test.yaml` keeps the queues shallow so
// the edge gap stays at its true transport cost. In that regime contracting
// the edge deletes something that was never the problem, and a controller that
// fuses anyway is broken.
//
// The same two fairness constraints as `detect_heavy.rs` apply:
//
//   * The work must be REAL and unremovable — the accumulator feeds a branch
//     that decides the output, so neither Cranelift nor LLVM can drop the loop.
//     It stays deterministic, so the event stream is bit-identical to the cheap
//     variant's and conservation assertions still hold.
//   * The cost must be CPU, not sleep. A blocking half would test
//     thread-occupancy effects rather than the service-time the controller
//     actually diagnoses from `wasm_us`.

use epico_sdk::stage;

/// Iterations of the spin loop. Chosen to put this half well above the
/// in-process edge's transport cost rather than merely above `normalize`'s —
/// being 20x a negligible number is still negligible, which is exactly why the
/// skew arm did not exercise the refusal.
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
    fn normalize(ev: Reading) -> Reading {
        let acc = heavy(ev.value);
        Reading {
            sensor_id: ev.sensor_id,
            // The value the cheap variant emits, guarded by a branch on the
            // accumulator so the loop above cannot be optimized away.
            value: if acc.is_finite() { ev.value.clamp(-50.0, 150.0) } else { -1.0 },
        }
    }
}
