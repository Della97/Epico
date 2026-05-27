// Stage: normalize — clamps sensor values to a sane range.
// Reading { sensor_id: string, value: f64, unit: option<string> }

use epico_sdk::stage;

stage! {
    fn normalize(ev: Reading) -> Reading {
        Reading {
            sensor_id: ev.sensor_id,
            value: ev.value.clamp(-50.0, 150.0),
            unit: Some(ev.unit.unwrap_or_else(|| "celsius".to_string())),
        }
    }
}