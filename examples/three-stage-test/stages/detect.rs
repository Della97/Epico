// Stage: detect — anomaly detection.

use epico_sdk::stage;

fn severity_for(v: f64) -> &'static str {
    if v > 85.0 {
        "high"
    } else if v > 75.0 {
        "medium"
    } else {
        "normal"
    }
}

stage! {
    fn detect(ev: Reading) -> Enriched {
        Enriched {
            sensor_id: ev.sensor_id,
            value: ev.value,
            anomaly: Some(ev.value > 75.0),
            severity: Some(severity_for(ev.value).to_string()),
        }
    }
}