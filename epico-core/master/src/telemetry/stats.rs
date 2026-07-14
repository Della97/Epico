//! Small numeric helpers shared by the summary builders. These were
//! previously duplicated as local closures in five different block builders;
//! behaviour is preserved exactly (the summary JSON is a stable interface for
//! the Python analysis scripts).

/// Nearest-rank percentile over a SORTED slice. No rounding — callers that
/// need rounded output apply it at insertion (r3/r4), matching the original
/// per-block closures.
pub(crate) fn pct(arr: &[f64], p: f64) -> f64 {
    if arr.is_empty() { return 0.0; }
    let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
    arr[idx]
}

/// Percentile rounded to 5 decimals — the variant `build_summary` uses for
/// the e2e/latency blocks and the legacy top-level fields.
pub(crate) fn pct_r5(arr: &[f64], p: f64) -> f64 {
    if arr.is_empty() { return 0.0; }
    let idx = ((arr.len() as f64 * p) as usize).min(arr.len() - 1);
    (arr[idx] * 100_000.0).round() / 100_000.0
}

pub(crate) fn mean(arr: &[f64]) -> f64 {
    if arr.is_empty() { return 0.0; }
    arr.iter().sum::<f64>() / arr.len() as f64
}

pub(crate) fn round3(x: f64) -> f64 { (x * 1000.0).round() / 1000.0 }

/// Downsample a sorted float series to at most `cap` elements by keeping
/// every K-th value. Paper plots typically don't need more than ~100k
/// samples for a CDF; above that you're just bloating the output JSON.
pub(crate) fn subsample_f64(src: &[f64], cap: usize) -> Vec<f64> {
    if src.len() <= cap { return src.to_vec(); }
    let k = (src.len() + cap - 1) / cap;
    src.iter().step_by(k).copied().collect()
}

pub(crate) fn subsample_rate(n: usize, cap: usize) -> usize {
    if n <= cap { 1 } else { (n + cap - 1) / cap }
}

/// Subsample the per-event log to at most `cap` rows, preserving input
/// order. The output JSON layout is positional for compactness:
///
///   `[recv_t_s, e2e_ms, [[stage, enter_ts, exit_ts], ...]]`
///
/// This is ~3x smaller than an equivalent named-field encoding and
/// maps directly to a pandas/polars DataFrame with one `explode` call
/// on the hops column.
pub(crate) fn subsample_events(
    src: &[(f64, f64, Vec<(String, f64, f64)>)],
    cap: usize,
) -> Vec<serde_json::Value> {
    if src.len() <= cap {
        return src.iter().map(event_row).collect();
    }
    let k = (src.len() + cap - 1) / cap;
    src.iter().step_by(k).map(event_row).collect()
}

fn event_row(e: &(f64, f64, Vec<(String, f64, f64)>)) -> serde_json::Value {
    let (recv_t_s, e2e_ms, hops) = e;
    // Round time-since-start to microsecond precision — finer than we
    // can reliably measure and plenty for per-second binning.
    let recv = (recv_t_s * 1_000_000.0).round() / 1_000_000.0;
    let lat  = (e2e_ms  * 10_000.0).round() / 10_000.0;
    let hops_json: Vec<serde_json::Value> = hops.iter()
        .map(|(name, enter, exit)| serde_json::json!([name, enter, exit]))
        .collect();
    serde_json::json!([recv, lat, hops_json])
}
