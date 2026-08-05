// Stage: normalize — the `a` half of the fusible pair.
//
// Minimal compute on purpose. When per-event compute is small the EDGE, not the
// stage, dominates service time — which is exactly the regime where fusion
// pays, and therefore the regime the break-even measurement needs to cover.

use epico_sdk::stage;

stage! {
    fn normalize(ev: Reading) -> Reading {
        Reading {
            sensor_id: ev.sensor_id,
            value:     ev.value.clamp(-50.0, 150.0),
        }
    }
}
