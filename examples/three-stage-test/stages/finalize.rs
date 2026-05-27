// Stage: finalize — marks event as processed, defaults severity if missing.
// Enriched { sensor_id: string, value: f64, anomaly: option<bool>, severity: option<string> }

use epico_sdk::stage;

stage! {
    fn finalize(ev: Enriched) -> Enriched {
        Enriched {
            sensor_id: ev.sensor_id,
            value: ev.value,
            anomaly: ev.anomaly,
            severity: Some(ev.severity.unwrap_or_else(|| "unknown".to_string())),
        }
    }
}