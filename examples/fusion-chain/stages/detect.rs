// Stage: detect — the `b` half of the fusible pair.
//
// Same input and output record as `normalize`, which keeps the contraction
// trivially type-compatible: fusing hands `normalize`'s result Val straight
// into `detect` as its parameter, with no conversion in between.

use epico_sdk::stage;

stage! {
    fn detect(ev: Reading) -> Reading {
        Reading {
            sensor_id: ev.sensor_id,
            value:     if ev.value > 75.0 { ev.value } else { ev.value * 0.5 },
        }
    }
}
