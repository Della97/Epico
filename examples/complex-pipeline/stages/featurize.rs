use metapod_sdk::stage;
use rustfft::{FftPlanner, num_complex::Complex};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

const WIN: usize = 256;

thread_local! {
    static STATE: RefCell<FeaturizeState> = RefCell::new(FeaturizeState::new());
}

struct WindowBuf {
    buf: Vec<f64>,           // capacity WIN, push-back ring semantics
    idx: usize,
    filled: usize,
    // Cached features — refreshed when the window rolls over.
    last_rms: f64,
    last_centroid: f64,
    last_flatness: f64,
    last_kurtosis: f64,
}

impl WindowBuf {
    fn new() -> Self {
        Self {
            buf: vec![0.0; WIN], idx: 0, filled: 0,
            last_rms: 0.0, last_centroid: 0.0, last_flatness: 0.0, last_kurtosis: 0.0,
        }
    }
}

struct FeaturizeState {
    per_sensor: HashMap<String, WindowBuf>,
    fft: Arc<dyn rustfft::Fft<f32>>,
    scratch: Vec<Complex<f32>>,
}

impl FeaturizeState {
    fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        let fft = planner.plan_fft_forward(WIN);
        Self {
            per_sensor: HashMap::new(),
            fft,
            scratch: vec![Complex::new(0.0, 0.0); WIN],
        }
    }
}

fn compute_features(buf: &[f64], scratch: &mut [Complex<f32>], fft: &Arc<dyn rustfft::Fft<f32>>)
    -> (f64, f64, f64, f64)
{
    // RMS in time domain.
    let rms = (buf.iter().map(|x| x * x).sum::<f64>() / buf.len() as f64).sqrt();

    // Excess kurtosis, computed inline. Population formula:
    //   m4 / m2^2 - 3
    // where m_k is the k-th central moment. Returns 0.0 for degenerate
    // (constant) windows where the variance underflows.
    let n = buf.len() as f64;
    let mean = buf.iter().sum::<f64>() / n;
    let mut m2 = 0.0;
    let mut m4 = 0.0;
    for &x in buf {
        let d = x - mean;
        let d2 = d * d;
        m2 += d2;
        m4 += d2 * d2;
    }
    m2 /= n;
    m4 /= n;
    let kurt = if m2 > 1e-18 { m4 / (m2 * m2) - 3.0 } else { 0.0 };

    // FFT for spectral features.
    for (i, x) in buf.iter().enumerate() {
        scratch[i] = Complex::new(*x as f32, 0.0);
    }
    fft.process(scratch);

    // Magnitudes on the positive half.
    let half = scratch.len() / 2;
    let mags: Vec<f64> = scratch[..half].iter().map(|c| c.norm() as f64).collect();
    let total: f64 = mags.iter().sum::<f64>() + 1e-12;

    // Spectral centroid (bin-weighted mean).
    let centroid = mags.iter().enumerate()
        .map(|(k, m)| k as f64 * m)
        .sum::<f64>() / total;

    // Spectral flatness = geomean / arithmean.
    let log_mean = mags.iter().map(|m| (m + 1e-12).ln()).sum::<f64>() / mags.len() as f64;
    let geom = log_mean.exp();
    let arith = total / mags.len() as f64;
    let flatness = geom / (arith + 1e-12);

    (rms, centroid, flatness, kurt)
}

stage! {
    fn featurize(ev: Filtered) -> Featured {
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            let FeaturizeState { per_sensor, fft, scratch } = &mut *st;
            let w = per_sensor.entry(ev.sensor_id.clone()).or_insert_with(WindowBuf::new);

            w.buf[w.idx] = ev.value;
            w.idx = (w.idx + 1) % WIN;
            w.filled = (w.filled + 1).min(WIN);

            let window_full = w.filled == WIN;
            // Refresh features once per full window roll. Cheap path otherwise.
            if window_full && w.idx == 0 {
                let (rms, centroid, flatness, kurt) =
                    compute_features(&w.buf, scratch, fft);
                w.last_rms = rms;
                w.last_centroid = centroid;
                w.last_flatness = flatness;
                w.last_kurtosis = kurt;
            }

            Featured {
                sensor_id: ev.sensor_id,
                ts_ns: ev.ts_ns,
                value: ev.value,
                rms: w.last_rms,
                centroid: w.last_centroid,
                flatness: w.last_flatness,
                kurtosis: w.last_kurtosis,
                window_full,
            }
        })
    }
}