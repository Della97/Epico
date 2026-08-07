//! Live, in-run statistics — the telemetry a *controller* reads, as opposed to
//! the summary telemetry an *analysis* reads.
//!
//! The distinction is the whole reason this module exists. Everything in
//! [`super::RunTelemetry`] is written to be read once, at shutdown, by
//! [`super::summary::build_summary`]: unbounded `Vec`s, a reservoir sample of
//! per-event rows that is uniform over the *whole run*, and quantiles computed
//! by sorting at the end. None of that answers "what is this edge doing right
//! now", which is the only question a morphing controller ever asks.
//!
//! Two consumers share one schema here, deliberately:
//!
//! * the cost-model controller ([`crate::controller`]), which reads the most
//!   recent few seconds every tick;
//! * a later LLM proposer, which reads the same fields *plus retained trend* —
//!   "this edge's p99 has been climbing for four minutes" is a different
//!   proposal from "this edge's p99 is high".
//!
//! The retained history is the part that is easy to skip and expensive to
//! retrofit, so the windows are rings from day one even though today's only
//! consumer looks exclusively at the newest bucket.
//!
//! # Who writes what
//!
//! | Structure | Writer | Cadence |
//! |---|---|---|
//! | [`StageTiming`] | every worker of that stage | one batched flush per ~64 events or ~50 ms |
//! | [`LiveStats::edges`] | the collector thread, single-threaded | per event (fold), per second (seal) |
//! | [`LiveStats::stages`] | the collector thread, from [`StageTiming`] deltas | per second |
//!
//! The collector owns [`LiveStats`] outright — it is the only place the whole
//! hop chain exists, so it is the only place a per-edge gap can be computed at
//! all (a worker knows its own exit but never the downstream enter). It
//! publishes a snapshot once per second through a plain `Mutex<Arc<LiveStats>>`;
//! contention is two uncontended acquisitions per second, which is not worth an
//! `arc_swap` dependency. That mutex is deliberately *not*
//! `Arc<Mutex<RunTelemetry>>`, whose writer discipline (see `mod.rs`) depends on
//! the collector never touching it while running.

// Parts of the query surface below have no caller in M2. That is deliberate and
// is the entire argument for building this module once rather than twice: the
// cost-model controller only ever asks about *now*, so a version written to its
// requirements alone would keep a single window and no per-key sketch — and the
// slower proposer that needs trend and skew would then have to add history to a
// structure not designed for it, re-deriving every consumer on the way. The
// unused methods are `trend`, `key_skew`, `edge_bytes_rate` and `stage_rate`;
// each is exercised by the tests at the bottom of this file, so they are
// verified rather than merely present.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ─────────────────────────────────────────────────────────────────────────────
// Histogram
// ─────────────────────────────────────────────────────────────────────────────

/// Number of buckets in a [`Hist`]: 24 octaves × [`SUB`] sub-buckets.
pub(crate) const HIST_BUCKETS: usize = 96;
/// Sub-buckets per octave. 4 gives a worst-case relative error of
/// `2^(1/4) - 1` ≈ 19 % on any reported quantile.
const SUB: u32 = 4;
const SUB_SHIFT: u32 = 2; // log2(SUB)
/// Octaves covered, starting at 1 µs.
const OCTAVES: u32 = (HIST_BUCKETS as u32) / SUB;

/// Log-spaced fixed-bucket histogram over **microseconds**, covering
/// 1 µs … 2^24 µs ≈ 16.8 s with a saturating top bucket.
///
/// Bounded, allocation-free, mergeable by element-wise add, and quantiles are a
/// cumulative scan. Chosen over a t-digest because merge-across-windows must be
/// exact and cheap: a trend query asks for p99 over the last four minutes,
/// which is 240 bucket merges and must not cost an allocation.
///
/// # Bucket layout
///
/// The design note specified "96 buckets, 1 µs … 100 s, ~1.47× per step", which
/// does not close: 1.47^95 ≈ 8e15, some eight orders of magnitude past 100 s.
/// A geometric layout that actually spans 1 µs → 100 s in 96 buckets needs
/// `10^(8/95)` ≈ 1.214× per step, and computing that index costs a float
/// logarithm per observation. The octave-and-sub-bucket layout used here gives
/// essentially the same resolution (1.189× per step) for two integer ops via
/// `leading_zeros`, and trades the unused 16.8 s … 100 s tail for it — every
/// quantity measured through this type (per-edge transport gaps, per-event wasm
/// and overhead) is µs-to-ms scale, and anything past 16.8 s saturates into the
/// top bucket rather than being lost.
#[derive(Clone)]
pub(crate) struct Hist {
    counts: [u32; HIST_BUCKETS],
}

impl Default for Hist {
    fn default() -> Self {
        Hist { counts: [0; HIST_BUCKETS] }
    }
}

/// Bucket index for a microsecond value. `0` for everything under 1 µs (which
/// is a real measurement, not an error — a contracted in-process edge reports
/// sub-µs gaps, and that is the entire point of fusing it).
#[inline]
pub(crate) fn bucket_of(us: u64) -> usize {
    if us < 1 {
        return 0;
    }
    let octave = 63 - us.leading_zeros() as u32; // floor(log2(us))
    if octave >= OCTAVES {
        return HIST_BUCKETS - 1;
    }
    // Top SUB_SHIFT bits below the leading one select the sub-bucket.
    let sub = if octave >= SUB_SHIFT {
        ((us >> (octave - SUB_SHIFT)) as u32) & (SUB - 1)
    } else {
        0
    };
    (octave * SUB + sub) as usize
}

/// Representative (geometric-midpoint) microsecond value for a bucket. Used
/// when a quantile lands in this bucket.
fn bucket_value(idx: usize) -> f64 {
    let octave = (idx as u32) / SUB;
    let sub = (idx as u32) % SUB;
    if octave < SUB_SHIFT {
        // Below 4 µs an integer microsecond count has no bits left under the
        // leading one, so a whole octave collapses into its first sub-bucket
        // and genuinely covers `[2^o, 2^(o+1))`. Reporting the sub-bucket's
        // narrow midpoint here would understate those values by ~25 %.
        //
        // Bucket 0 additionally absorbs everything under 1 µs. That is not a
        // defect to work around: a contracted in-process edge legitimately
        // reports a sub-microsecond gap, and ~1 µs is the honest answer for it
        // — which is also what the M2 measurements report post-fusion.
        let lower = if octave == 0 { 0.0 } else { (1u64 << octave) as f64 };
        let upper = (1u64 << (octave + 1)) as f64;
        return (lower + upper) * 0.5;
    }
    // Lower edge is 2^octave * (1 + sub/SUB); take the midpoint of the step.
    let lower = (1u64 << octave) as f64 * (1.0 + sub as f64 / SUB as f64);
    let upper = (1u64 << octave) as f64 * (1.0 + (sub + 1) as f64 / SUB as f64);
    (lower + upper) * 0.5
}

impl Hist {
    #[inline]
    pub(crate) fn observe_us(&mut self, us: u64) {
        self.counts[bucket_of(us)] += 1;
    }

    pub(crate) fn total(&self) -> u64 {
        self.counts.iter().map(|c| *c as u64).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.counts.iter().all(|c| *c == 0)
    }

    pub(crate) fn clear(&mut self) {
        self.counts = [0; HIST_BUCKETS];
    }

    /// Element-wise add. This is what makes a multi-second window query cheap:
    /// merge the buckets, then scan once.
    pub(crate) fn merge(&mut self, other: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a = a.saturating_add(*b);
        }
    }

    /// Quantile in microseconds. Returns 0.0 for an empty histogram — callers
    /// treat "no observations" and "zero cost" the same way, and both mean
    /// "this edge is not evidence for a morph".
    pub(crate) fn quantile_us(&self, q: f64) -> f64 {
        let total = self.total();
        if total == 0 {
            return 0.0;
        }
        let target = (q.clamp(0.0, 1.0) * total as f64).ceil().max(1.0) as u64;
        let mut cum = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            cum += *c as u64;
            if cum >= target {
                return bucket_value(i);
            }
        }
        bucket_value(HIST_BUCKETS - 1)
    }
}

/// The atomic mirror of [`Hist`], written by worker threads and sampled by the
/// collector. Counters are monotonic for the life of the stage; a reader takes
/// deltas against its own previous snapshot, so multiple independent readers
/// (the autoscaler for the summary, the collector for live stats) never
/// interfere.
pub(crate) struct AtomicHist {
    counts: [AtomicU32; HIST_BUCKETS],
}

impl Default for AtomicHist {
    // `[T; N]: Default` only reaches N = 32, and HIST_BUCKETS is 96.
    fn default() -> Self {
        AtomicHist { counts: std::array::from_fn(|_| AtomicU32::new(0)) }
    }
}

impl AtomicHist {
    /// Fold a thread-local histogram in, touching only the non-empty buckets.
    /// A worker's flush window typically spans three or four buckets, so this
    /// is a handful of relaxed `fetch_add`s per *window* rather than one per
    /// event.
    pub(crate) fn merge_from(&self, local: &Hist) {
        for (slot, c) in self.counts.iter().zip(local.counts.iter()) {
            if *c != 0 {
                slot.fetch_add(*c, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Hist {
        let mut out = Hist::default();
        for (dst, src) in out.counts.iter_mut().zip(self.counts.iter()) {
            *dst = src.load(Ordering::Relaxed);
        }
        out
    }
}

/// `cur - prev`, per bucket, saturating. The delta of two snapshots of a
/// monotonic [`AtomicHist`] is exactly the observations that landed between
/// them.
pub(crate) fn hist_delta(cur: &Hist, prev: &Hist) -> Hist {
    let mut out = Hist::default();
    for i in 0..HIST_BUCKETS {
        out.counts[i] = cur.counts[i].saturating_sub(prev.counts[i]);
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-stage timing — the D7 fix
// ─────────────────────────────────────────────────────────────────────────────

/// Per-stage worker timing, shared by every replica of one stage.
///
/// # Why this exists
///
/// The controller's first question is *diagnostic*, not actuating: an expensive
/// edge is a reason to fuse only when the stages it joins are **not**
/// compute-bound. Fusion deletes an edge; if the cost is in the wasm call, it
/// deletes the wrong thing and the remedy is replicas or nothing.
///
/// Answering that needs `total_ns` and `serde_ns` per stage. The worker has
/// computed both per event since before M2 — but shipped them *only* inside the
/// zmq credit-refill message, and [`crate::autoscaler`] only reads them via
/// `fetch_dispatcher_metrics`, which runs solely when the stage has no in-proc
/// input edge. In-proc stages have no dispatcher, so on `spsc`/`mpmc` — the
/// transports M2 actually measures — `total_us_samples` and `serde_us_samples`
/// were empty and every `worker_timing.*.total_us` field in the phase-4
/// summaries reads `0.0`.
///
/// This mirrors the `live_replicas` pattern that already works: one shared
/// counter block per stage, written by its workers, sampled by readers that
/// keep their own previous values.
///
/// # Hot-path cost
///
/// Workers accumulate into plain locals and flush on a window boundary (see
/// `worker.rs`), so the event loop pays four relaxed `fetch_add`s plus a
/// handful of histogram buckets per *window*, not per event.
#[derive(Default)]
pub(crate) struct StageTiming {
    /// Whole iteration: recv → decode → call → encode → send.
    pub total_ns: AtomicU64,
    /// Decode + encode only.
    pub serde_ns: AtomicU64,
    /// Sum of the per-half wasm call durations.
    pub wasm_ns: AtomicU64,
    /// Events contributing to the sums above.
    pub events: AtomicU64,
    /// Per-event wasm cost. Kept as a histogram, not just a sum, because the
    /// decision rule compares a *p99* against the edge's p99 — a mean would
    /// understate the compute tail and bias every marginal case toward fusing.
    pub wasm_hist: AtomicHist,
    /// Per-event `total - wasm`. Derived per event *before* bucketing, so
    /// overhead and wasm cannot drift apart the way two independently
    /// accumulated sums would.
    pub overhead_hist: AtomicHist,
}

/// A worker's thread-local accumulator. Flushed into the shared
/// [`StageTiming`] on a window boundary and cleared.
#[derive(Default)]
pub(crate) struct TimingAccum {
    pub total_ns: u64,
    pub serde_ns: u64,
    pub wasm_ns: u64,
    pub events: u64,
    pub wasm: Hist,
    pub overhead: Hist,
}

impl TimingAccum {
    /// Record one finished event. `wasm_ns` is the summed per-half call time,
    /// which for an unfused stage is its single call and for a fused stage is
    /// both halves — the quantity fusion does *not* remove.
    #[inline]
    pub(crate) fn observe(&mut self, total_ns: u64, serde_ns: u64, wasm_ns: u64) {
        self.total_ns += total_ns;
        self.serde_ns += serde_ns;
        self.wasm_ns += wasm_ns;
        self.events += 1;
        self.wasm.observe_us(wasm_ns / 1_000);
        self.overhead.observe_us(total_ns.saturating_sub(wasm_ns) / 1_000);
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.events == 0
    }

    /// Publish and reset. Called on a window boundary, never per event.
    pub(crate) fn flush_into(&mut self, shared: &StageTiming) {
        if self.events == 0 {
            return;
        }
        shared.total_ns.fetch_add(self.total_ns, Ordering::Relaxed);
        shared.serde_ns.fetch_add(self.serde_ns, Ordering::Relaxed);
        shared.wasm_ns.fetch_add(self.wasm_ns, Ordering::Relaxed);
        shared.events.fetch_add(self.events, Ordering::Relaxed);
        shared.wasm_hist.merge_from(&self.wasm);
        shared.overhead_hist.merge_from(&self.overhead);
        self.total_ns = 0;
        self.serde_ns = 0;
        self.wasm_ns = 0;
        self.events = 0;
        self.wasm.clear();
        self.overhead.clear();
    }
}

/// One reader's view of a [`StageTiming`]'s progress, so several readers can
/// take independent deltas off the same monotonic counters.
#[derive(Default, Clone)]
pub(crate) struct TimingCursor {
    pub total_ns: u64,
    pub serde_ns: u64,
    pub wasm_ns: u64,
    pub events: u64,
    pub wasm: Hist,
    pub overhead: Hist,
}

/// What accumulated on a stage since the cursor was last advanced.
pub(crate) struct TimingDelta {
    pub events: u64,
    /// Mean nanoseconds per event over the delta window.
    pub total_ns_per_event: u64,
    pub serde_ns_per_event: u64,
    pub wasm_ns_per_event: u64,
    pub wasm: Hist,
    pub overhead: Hist,
}

impl TimingCursor {
    /// Read the shared counters and return what changed, advancing the cursor.
    /// `None` when no event has been processed since the last call — an idle
    /// stage must contribute no samples rather than a run of zeros, which would
    /// drag every quantile down exactly when the pipeline is quiet.
    pub(crate) fn advance(&mut self, shared: &StageTiming) -> Option<TimingDelta> {
        let events = shared.events.load(Ordering::Relaxed);
        let d_events = events.saturating_sub(self.events);
        if d_events == 0 {
            return None;
        }
        let total_ns = shared.total_ns.load(Ordering::Relaxed);
        let serde_ns = shared.serde_ns.load(Ordering::Relaxed);
        let wasm_ns = shared.wasm_ns.load(Ordering::Relaxed);
        let wasm_snap = shared.wasm_hist.snapshot();
        let overhead_snap = shared.overhead_hist.snapshot();

        let delta = TimingDelta {
            events: d_events,
            total_ns_per_event: total_ns.saturating_sub(self.total_ns) / d_events,
            serde_ns_per_event: serde_ns.saturating_sub(self.serde_ns) / d_events,
            wasm_ns_per_event: wasm_ns.saturating_sub(self.wasm_ns) / d_events,
            wasm: hist_delta(&wasm_snap, &self.wasm),
            overhead: hist_delta(&overhead_snap, &self.overhead),
        };

        self.events = events;
        self.total_ns = total_ns;
        self.serde_ns = serde_ns;
        self.wasm_ns = wasm_ns;
        self.wasm = wasm_snap;
        self.overhead = overhead_snap;
        Some(delta)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry
// ─────────────────────────────────────────────────────────────────────────────

/// The per-stage timing blocks, keyed by *physical* stage name.
///
/// A morph creates and retires physical stages, so this has to be a live
/// registry rather than a boot-time map: `fn-a+fn-b` does not exist until the
/// fusion that creates it. [`crate::morph::Deployer::spawn_stage`] registers a
/// stage on the way up; entries are kept after a stage retires so a reader that
/// samples across the switch boundary sees the counters stop rather than
/// vanish.
#[derive(Default)]
pub(crate) struct TimingRegistry {
    stages: Mutex<HashMap<String, Arc<StageTiming>>>,
}

impl TimingRegistry {
    /// The timing block for `stage`, creating it on first use. Re-registering
    /// an existing name (a split-then-refuse, an identity morph) returns the
    /// same block, so its counters stay monotonic across a redeploy and every
    /// cursor holding it stays valid.
    pub(crate) fn get_or_insert(&self, stage: &str) -> Arc<StageTiming> {
        let mut stages = self.stages.lock().expect("timing registry poisoned");
        stages
            .entry(stage.to_string())
            .or_insert_with(|| Arc::new(StageTiming::default()))
            .clone()
    }

    /// Every registered stage, for a reader that has to discover stages created
    /// after it started (which is every reader, since morphs create stages).
    pub(crate) fn snapshot(&self) -> Vec<(String, Arc<StageTiming>)> {
        let stages = self.stages.lock().expect("timing registry poisoned");
        stages.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-key occupancy sketch
// ─────────────────────────────────────────────────────────────────────────────

/// Space-Saving top-k, k = [`TOPK`].
///
/// Per-key occupancy is the highest-value skew input there is and it is
/// completely invisible in aggregates: an edge carrying 40 k ev/s spread evenly
/// and an edge carrying 40 k ev/s of which 90 % share one key have identical
/// counts, identical bytes, and identical gap distributions, but only one of
/// them is a scission candidate.
///
/// Space-Saving is chosen over exact counting because the key space is
/// unbounded and this sits on the collector's hot path: it is O(1) per
/// observation with a fixed 16-slot table, and its error is bounded by the
/// smallest retained count — which for a skew *ratio* is exactly the regime
/// where accuracy stops mattering.
#[derive(Clone, Default)]
pub(crate) struct TopK {
    /// `(key_hash, count)`, unsorted. Small enough that a linear scan beats any
    /// index.
    slots: Vec<(u64, u64)>,
    total: u64,
}

/// Slots per [`TopK`]. Sized against the decision it feeds: "is one key
/// dominating this edge" needs the head of the distribution, not its shape.
const TOPK: usize = 16;

impl TopK {
    #[inline]
    pub(crate) fn observe(&mut self, key: u64) {
        self.total += 1;
        if let Some(slot) = self.slots.iter_mut().find(|(k, _)| *k == key) {
            slot.1 += 1;
            return;
        }
        if self.slots.len() < TOPK {
            self.slots.push((key, 1));
            return;
        }
        // Space-Saving: evict the minimum and inherit its count, so a key that
        // has genuinely taken over is reached in one step rather than being
        // starved out by whichever keys happened to arrive first.
        let min = self
            .slots
            .iter_mut()
            .min_by_key(|(_, c)| *c)
            .expect("slots is non-empty here");
        *min = (key, min.1 + 1);
    }

    pub(crate) fn merge(&mut self, other: &TopK) {
        for (k, c) in &other.slots {
            // Merging counts rather than replaying observations: `observe`
            // would inflate `total` by one per distinct key instead of by the
            // count it carries.
            if let Some(slot) = self.slots.iter_mut().find(|(sk, _)| sk == k) {
                slot.1 += c;
            } else if self.slots.len() < TOPK {
                self.slots.push((*k, *c));
            } else if let Some(min) = self.slots.iter_mut().min_by_key(|(_, mc)| *mc) {
                if min.1 < *c {
                    *min = (*k, min.1 + c);
                }
            }
        }
        self.total += other.total;
    }

    /// Share of traffic held by the single busiest key, in `[0, 1]`. 0 when
    /// nothing was observed or no event carried a key — "no key information" and
    /// "perfectly uniform" both mean *this is not evidence of skew*, which is
    /// the only thing a caller does with the answer.
    pub(crate) fn top_share(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let top = self.slots.iter().map(|(_, c)| *c).max().unwrap_or(0);
        top as f64 / self.total as f64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rolling windows
// ─────────────────────────────────────────────────────────────────────────────

/// One sealed second of an edge's traffic.
#[derive(Clone, Default)]
pub(crate) struct EdgeBucket {
    pub count: u64,
    /// Payload bytes crossing the edge. Counts drive fusion and replication;
    /// **bytes** drive transport selection, and they are free to collect here
    /// because the collector already holds the frame.
    pub bytes: u64,
    /// Per-event transport cost in µs: `next.enter_ts - prev.exit_ts`.
    pub gap: Hist,
    /// Per-key occupancy. `None` on buckets old enough to have been trimmed —
    /// it is the only field whose size is not trivially bounded, so its
    /// retention is shorter than the window's (see [`KEY_RETAIN`]).
    pub keys: Option<Box<TopK>>,
}

impl EdgeBucket {
    fn reset(&mut self) {
        self.count = 0;
        self.bytes = 0;
        self.gap.clear();
        self.keys = None;
    }
}

/// One sealed second of a stage's compute.
#[derive(Clone, Default)]
pub(crate) struct StageBucket {
    pub events: u64,
    /// Per-event wasm call cost, µs.
    pub wasm: Hist,
    /// Per-event `total - wasm`, µs. This is the quantity fusion actually
    /// deletes, which is why it is tracked separately from the wasm cost that
    /// fusion cannot touch.
    pub overhead: Hist,
}

impl StageBucket {
    fn reset(&mut self) {
        self.events = 0;
        self.wasm.clear();
        self.overhead.clear();
    }
}

/// Seconds of history retained per edge and per stage.
///
/// A controller only ever wants *now*, so the tempting implementation keeps one
/// window — and then a trend consumer has to add history to a structure that
/// was not designed for it, re-deriving every reader on the way. The ring is
/// here from the start because retrofitting it is the expensive direction:
/// adding it now costs `RETAIN * sizeof(EdgeBucket)` ≈ 120 KB for a four-edge
/// pipeline, which is nothing next to a single second of `per_event_log`.
pub(crate) const RETAIN: usize = 300;

/// Seconds of per-key history retained. Shorter than [`RETAIN`] on purpose:
/// [`TopK`] is the only per-bucket field whose footprint scales with anything
/// other than the bucket count, and skew is a *current* property — a four-minute
/// old key distribution informs no decision this controller makes.
pub(crate) const KEY_RETAIN: usize = 60;

/// A fixed-size ring of sealed buckets plus the one currently being folded
/// into. `head` indexes the OPEN bucket; sealed history runs backwards from
/// `head - 1`.
#[derive(Clone)]
pub(crate) struct Window<B> {
    ring: Vec<B>,
    head: usize,
    /// Sealed buckets available, saturating at `RETAIN`. Without this a query
    /// over the first minute of a run would average against zeroed buckets that
    /// never held anything and report a rate an order of magnitude low.
    filled: usize,
}

impl<B: Default + Clone> Default for Window<B> {
    fn default() -> Self {
        Window { ring: vec![B::default(); RETAIN], head: 0, filled: 0 }
    }
}

impl<B: Default + Clone> Window<B> {
    /// The bucket currently accepting observations.
    #[inline]
    pub(crate) fn current_mut(&mut self) -> &mut B {
        &mut self.ring[self.head]
    }

    /// Close the open bucket and open the next. Returns the index just sealed.
    fn advance(&mut self) -> usize {
        let sealed = self.head;
        self.head = (self.head + 1) % RETAIN;
        self.filled = (self.filled + 1).min(RETAIN);
        sealed
    }

    /// The `n` most recently sealed buckets, newest first. Never yields the
    /// open bucket: a partially-filled second would make every rate query
    /// jitter with wherever in the second it happened to be asked.
    pub(crate) fn recent(&self, n: usize) -> impl Iterator<Item = &B> {
        let take = n.min(self.filled);
        (1..=take).map(move |back| &self.ring[(self.head + RETAIN - back) % RETAIN])
    }

    pub(crate) fn filled(&self) -> usize {
        self.filled
    }

    /// Seal the open bucket. Exposed for tests that build a window literal
    /// instead of driving the collector — the queries under test then run
    /// against the same sealed-bucket geometry production uses, rather than
    /// against a hand-poked ring that could disagree with it.
    #[cfg(test)]
    pub(crate) fn advance_for_test(&mut self) {
        self.advance();
    }
}

pub(crate) type EdgeWindow = Window<EdgeBucket>;
pub(crate) type StageWindow = Window<StageBucket>;

/// An edge, named by the physical stages at its ends.
pub(crate) type EdgeKey = (String, String);

// ─────────────────────────────────────────────────────────────────────────────
// The published snapshot
// ─────────────────────────────────────────────────────────────────────────────

/// What a consumer reads. Immutable once published; the collector builds the
/// next one while this one is being read.
#[derive(Default, Clone)]
pub(crate) struct LiveStats {
    pub edges: HashMap<EdgeKey, EdgeWindow>,
    pub stages: HashMap<String, StageWindow>,
    /// Seconds since test start at the moment this snapshot was sealed. A
    /// consumer compares it against its own clock to decide whether the
    /// snapshot is fresh enough to act on — the collector cannot publish while
    /// no events are arriving, and "the pipeline is idle" must not be read as
    /// "every edge is cheap".
    pub t_s: f64,
}

impl LiveStats {
    fn edge(&self, e: &EdgeKey) -> Option<&EdgeWindow> {
        self.edges.get(e)
    }

    /// Quantile of the per-event transport gap on `e` over the last
    /// `window_s` sealed seconds, in **microseconds**.
    ///
    /// This is the number the whole controller turns on: it is what an edge
    /// costs per event, and fusing is the operation that deletes it. Merging
    /// the buckets first and scanning once keeps a multi-minute query as cheap
    /// as a one-second one.
    pub(crate) fn edge_p(&self, e: &EdgeKey, q: f64, window_s: u64) -> f64 {
        let Some(w) = self.edge(e) else { return 0.0 };
        let mut merged = Hist::default();
        for b in w.recent(window_s as usize) {
            merged.merge(&b.gap);
        }
        merged.quantile_us(q)
    }

    /// Events per second on `e`, averaged over the sealed buckets actually
    /// available — not over `window_s`, which would under-report during the
    /// first `window_s` seconds of a run.
    pub(crate) fn edge_rate(&self, e: &EdgeKey, window_s: u64) -> f64 {
        let Some(w) = self.edge(e) else { return 0.0 };
        let n = (window_s as usize).min(w.filled());
        if n == 0 {
            return 0.0;
        }
        w.recent(n).map(|b| b.count).sum::<u64>() as f64 / n as f64
    }

    /// Bytes per second on `e`. Kept alongside the event rate rather than
    /// derived from it: mean event size is not stable across a pipeline, and
    /// transport selection is a byte-driven decision while fusion is a
    /// count-driven one.
    pub(crate) fn edge_bytes_rate(&self, e: &EdgeKey, window_s: u64) -> f64 {
        let Some(w) = self.edge(e) else { return 0.0 };
        let n = (window_s as usize).min(w.filled());
        if n == 0 {
            return 0.0;
        }
        w.recent(n).map(|b| b.bytes).sum::<u64>() as f64 / n as f64
    }

    /// Share of `e`'s traffic held by its single busiest key over the last
    /// `window_s` seconds (capped at [`KEY_RETAIN`]).
    pub(crate) fn key_skew(&self, e: &EdgeKey, window_s: u64) -> f64 {
        let Some(w) = self.edge(e) else { return 0.0 };
        let n = (window_s as usize).min(KEY_RETAIN);
        let mut merged = TopK::default();
        for b in w.recent(n) {
            if let Some(k) = b.keys.as_ref() {
                merged.merge(k);
            }
        }
        merged.top_share()
    }

    fn stage(&self, s: &str) -> Option<&StageWindow> {
        self.stages.get(s)
    }

    /// Quantile of per-event wasm cost on `s`, in µs. The compute half of the
    /// diagnosis: when this dominates the edge, fusing deletes a cost that was
    /// not the problem.
    pub(crate) fn stage_wasm_p(&self, s: &str, q: f64, window_s: u64) -> f64 {
        let Some(w) = self.stage(s) else { return 0.0 };
        let mut merged = Hist::default();
        for b in w.recent(window_s as usize) {
            merged.merge(&b.wasm);
        }
        merged.quantile_us(q)
    }

    /// Quantile of per-event non-wasm cost on `s`, in µs.
    pub(crate) fn stage_overhead_p(&self, s: &str, q: f64, window_s: u64) -> f64 {
        let Some(w) = self.stage(s) else { return 0.0 };
        let mut merged = Hist::default();
        for b in w.recent(window_s as usize) {
            merged.merge(&b.overhead);
        }
        merged.quantile_us(q)
    }

    pub(crate) fn stage_rate(&self, s: &str, window_s: u64) -> f64 {
        let Some(w) = self.stage(s) else { return 0.0 };
        let n = (window_s as usize).min(w.filled());
        if n == 0 {
            return 0.0;
        }
        w.recent(n).map(|b| b.events).sum::<u64>() as f64 / n as f64
    }

    /// Slope of `e`'s quantile over the window, in µs per second.
    ///
    /// Nothing in the M2 controller reads this — it is here because it is the
    /// field a slower, reasoning proposer needs and the one that cannot be
    /// recovered after the fact. Least squares over the per-second quantiles,
    /// oldest to newest; 0.0 when there are fewer than two sealed buckets.
    pub(crate) fn trend(&self, e: &EdgeKey, q: f64, window_s: u64) -> f64 {
        let Some(w) = self.edge(e) else { return 0.0 };
        let n = (window_s as usize).min(w.filled());
        if n < 2 {
            return 0.0;
        }
        // `recent` is newest-first; x runs forward in time so a rising p99
        // yields a positive slope.
        let ys: Vec<f64> = w
            .recent(n)
            .map(|b| b.gap.quantile_us(q))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let n_f = ys.len() as f64;
        let mean_x = (n_f - 1.0) / 2.0;
        let mean_y = ys.iter().sum::<f64>() / n_f;
        let mut num = 0.0;
        let mut den = 0.0;
        for (i, y) in ys.iter().enumerate() {
            let dx = i as f64 - mean_x;
            num += dx * (y - mean_y);
            den += dx * dx;
        }
        if den == 0.0 { 0.0 } else { num / den }
    }
}

/// The publication point. One `Mutex<Arc<LiveStats>>` swapped once per second
/// by the collector and read once per tick by a consumer: two uncontended
/// acquisitions per second, which is not worth pulling in an `arc_swap`
/// dependency for.
#[derive(Default)]
pub(crate) struct LiveStatsHandle {
    inner: Mutex<Arc<LiveStats>>,
}

impl LiveStatsHandle {
    pub(crate) fn load(&self) -> Arc<LiveStats> {
        self.inner.lock().expect("live stats mutex poisoned").clone()
    }

    fn publish(&self, stats: Arc<LiveStats>) {
        *self.inner.lock().expect("live stats mutex poisoned") = stats;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The collector-side folder
// ─────────────────────────────────────────────────────────────────────────────

const BUCKET_S: f64 = 1.0;

/// Owned by the collector thread; folds each finished event into the open
/// bucket and publishes a snapshot once a second.
///
/// # Why here and not in a worker
///
/// A per-edge gap is `next.enter_ts - prev.exit_ts`. A worker knows its own
/// exit but never the downstream enter, so no worker can compute one. The
/// collector sees the complete hop list on every finished event, already
/// decodes it header-only, and is single-threaded — so it can own a mutable
/// structure with no synchronisation on the hot path at all.
///
/// # Why not at summary time
///
/// That is what `inter_stage` does today, and it reads `per_event_log`, which
/// is a *reservoir sample* (Algorithm R, `EVENTS_LIVE_CAP`) — uniform over the
/// whole run and therefore structurally unable to answer "what is this edge
/// doing right now".
pub(crate) struct LiveCollector {
    edges: HashMap<EdgeKey, EdgeWindow>,
    stages: HashMap<String, StageWindow>,
    /// One cursor per stage over the shared [`StageTiming`] counters, private
    /// to this thread so the autoscalers' own sampling is unaffected.
    cursors: HashMap<String, TimingCursor>,
    timings: Arc<TimingRegistry>,
    handle: Arc<LiveStatsHandle>,
    /// Seconds since test start at which the open bucket closes.
    next_seal_t_s: f64,
}

impl LiveCollector {
    pub(crate) fn new(timings: Arc<TimingRegistry>, handle: Arc<LiveStatsHandle>) -> Self {
        LiveCollector {
            edges: HashMap::new(),
            stages: HashMap::new(),
            cursors: HashMap::new(),
            timings,
            handle,
            next_seal_t_s: BUCKET_S,
        }
    }

    /// Fold one finished event. `hops` is the parsed hop list the collector
    /// already built for its own accounting — walked pairwise here, so an event
    /// crossing N edges contributes N gaps.
    ///
    /// Allocation-free by construction: every structure it touches is
    /// pre-sized, and the only map lookups are on edges that already exist
    /// after the first event of a run.
    pub(crate) fn observe(
        &mut self,
        recv_t_s: f64,
        hops: &[(String, f64, f64)],
        bytes: usize,
        key_hash: Option<u64>,
    ) {
        self.seal_if_due(recv_t_s);

        for pair in hops.windows(2) {
            let (ref from, _, exit) = pair[0];
            let (ref to, enter, _) = pair[1];
            // Hop labels carry a `#replica` suffix; edges are between STAGES.
            // Keeping the replica in the key would split one edge into
            // `Ra * Rb` sparse edges, none of which has enough observations to
            // support a p99 — and the decision is per-edge anyway.
            let from = from.split('#').next().unwrap_or(from);
            let to = to.split('#').next().unwrap_or(to);

            // A negative gap means the two hops were stamped on clocks that
            // disagree, which happens across processes. Clamp rather than drop:
            // a contracted edge legitimately reports ~0, and dropping those
            // would delete the evidence that the fusion worked.
            let gap_us = ((enter - exit) * 1e6).max(0.0) as u64;

            let w = self
                .edges
                .entry((from.to_string(), to.to_string()))
                .or_default();
            let b = w.current_mut();
            b.count += 1;
            b.bytes += bytes as u64;
            b.gap.observe_us(gap_us);
            // Allocated on first keyed event rather than up front: a bucket on
            // a pipeline whose events carry no key costs one null pointer, and
            // a bucket whose keys were trimmed re-arms itself if it is reused.
            if let Some(k) = key_hash {
                b.keys.get_or_insert_with(Box::default).observe(k);
            }
        }
    }

    /// Close the open bucket if its second has elapsed, then publish. Safe to
    /// call from the collector's idle path — it is a float comparison until the
    /// second actually turns over.
    pub(crate) fn seal_if_due(&mut self, now_t_s: f64) {
        if now_t_s < self.next_seal_t_s {
            return;
        }
        // A long gap with no events (or a blocking recv) can leave several
        // seconds unsealed. Advance one bucket per elapsed second so the ring
        // stays a true time series rather than compressing an idle minute into
        // one bucket — but cap the catch-up at the ring size, since more than
        // RETAIN empty seconds is indistinguishable from a fresh ring.
        let behind = ((now_t_s - self.next_seal_t_s) / BUCKET_S).floor() as usize + 1;
        for _ in 0..behind.min(RETAIN) {
            self.seal_one();
        }
        self.next_seal_t_s = (now_t_s / BUCKET_S).floor() * BUCKET_S + BUCKET_S;
        self.publish(now_t_s);
    }

    fn seal_one(&mut self) {
        for w in self.edges.values_mut() {
            let sealed = w.advance();
            // Trim per-key history: it is the one field whose size is not
            // bounded by the bucket count alone.
            let trim = (sealed + RETAIN - KEY_RETAIN) % RETAIN;
            w.ring[trim].keys = None;
            w.ring[w.head].reset();
        }
        // Stage buckets are pulled rather than pushed: the collector reads each
        // stage's shared counters through its own cursor, so a stage created by
        // a morph starts contributing the moment it is registered without any
        // wiring between the actuator and this thread.
        for (name, timing) in self.timings.snapshot() {
            let cursor = self.cursors.entry(name.clone()).or_default();
            let delta = cursor.advance(&timing);
            let w = self.stages.entry(name).or_default();
            let sealed_head = w.head;
            if let Some(d) = delta {
                let b = &mut w.ring[sealed_head];
                b.events = d.events;
                b.wasm = d.wasm;
                b.overhead = d.overhead;
            }
            w.advance();
            let head = w.head;
            w.ring[head].reset();
        }
    }

    fn publish(&self, t_s: f64) {
        self.handle.publish(Arc::new(LiveStats {
            edges: self.edges.clone(),
            stages: self.stages.clone(),
            t_s,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_is_monotonic_and_bounded() {
        let mut last = 0usize;
        for us in [0u64, 1, 2, 3, 4, 7, 8, 15, 100, 1_000, 1_000_000, u64::MAX] {
            let b = bucket_of(us);
            assert!(b < HIST_BUCKETS, "bucket {b} out of range for {us} us");
            assert!(b >= last, "bucket went backwards at {us} us");
            last = b;
        }
        assert_eq!(bucket_of(0), 0);
        assert_eq!(bucket_of(u64::MAX), HIST_BUCKETS - 1);
    }

    #[test]
    fn quantiles_track_a_sorted_vec_within_bucket_width() {
        // A histogram quantile must land in the same bucket as the exact one;
        // the guarantee is relative error under 2^(1/4)-1 ≈ 19 %, not equality.
        let mut h = Hist::default();
        let mut raw: Vec<u64> = Vec::new();
        for i in 1..=10_000u64 {
            let v = i * 7 % 5_000 + 1;
            h.observe_us(v);
            raw.push(v);
        }
        raw.sort_unstable();
        for q in [0.5, 0.9, 0.99] {
            let exact = raw[((q * raw.len() as f64).ceil() as usize - 1).min(raw.len() - 1)] as f64;
            let got = h.quantile_us(q);
            let err = (got - exact).abs() / exact;
            assert!(err < 0.20, "q{q}: hist {got} vs exact {exact} (err {err:.3})");
        }
    }

    #[test]
    fn empty_hist_reports_zero_not_a_panic() {
        let h = Hist::default();
        assert_eq!(h.quantile_us(0.99), 0.0);
        assert_eq!(h.total(), 0);
        assert!(h.is_empty());
    }

    #[test]
    fn merge_is_elementwise_add() {
        let mut a = Hist::default();
        let mut b = Hist::default();
        a.observe_us(10);
        a.observe_us(10);
        b.observe_us(10);
        b.observe_us(1_000);
        a.merge(&b);
        assert_eq!(a.total(), 4);
        assert_eq!(a.counts[bucket_of(10)], 3);
        assert_eq!(a.counts[bucket_of(1_000)], 1);
    }

    #[test]
    fn cursor_takes_independent_deltas() {
        let shared = StageTiming::default();
        let mut acc = TimingAccum::default();
        // 100 events at 10 us total, 4 us wasm.
        for _ in 0..100 {
            acc.observe(10_000, 2_000, 4_000);
        }
        acc.flush_into(&shared);

        // Two readers of the same counters must not disturb each other.
        let mut fast = TimingCursor::default();
        let mut slow = TimingCursor::default();

        let d = fast.advance(&shared).expect("first delta");
        assert_eq!(d.events, 100);
        assert_eq!(d.total_ns_per_event, 10_000);
        assert_eq!(d.wasm_ns_per_event, 4_000);
        // overhead = total - wasm = 6 us
        assert!((d.overhead.quantile_us(0.5) - 6.0).abs() < 1.5);

        // No new events: no sample, rather than a zero that would drag
        // quantiles down while the pipeline is idle.
        assert!(fast.advance(&shared).is_none());

        // The slow reader still sees the whole history.
        let d2 = slow.advance(&shared).expect("slow reader delta");
        assert_eq!(d2.events, 100);
    }

    #[test]
    fn accum_flush_resets() {
        let shared = StageTiming::default();
        let mut acc = TimingAccum::default();
        acc.observe(1_000, 100, 500);
        assert!(!acc.is_empty());
        acc.flush_into(&shared);
        assert!(acc.is_empty());
        acc.flush_into(&shared); // second flush is a no-op
        assert_eq!(shared.events.load(Ordering::Relaxed), 1);
    }

    // ── Per-key sketch ───────────────────────────────────────────────────

    #[test]
    fn topk_finds_a_dominant_key_even_after_its_slot_was_taken() {
        let mut t = TopK::default();
        // Fill every slot with a different cold key first, so the hot key has
        // to evict its way in — the case a naive "first 16 keys win" table
        // gets wrong, and the one that matters (skew usually starts mid-run).
        for k in 0..TOPK as u64 {
            t.observe(k);
        }
        for _ in 0..900 {
            t.observe(9999);
        }
        assert!(t.top_share() > 0.9, "top share was {}", t.top_share());
    }

    #[test]
    fn topk_reports_no_skew_for_uniform_traffic() {
        let mut t = TopK::default();
        for i in 0..1_000u64 {
            t.observe(i);
        }
        // Every key distinct: whatever survives in the table holds a
        // vanishing share of the total.
        assert!(t.top_share() < 0.1, "top share was {}", t.top_share());
    }

    #[test]
    fn topk_merge_preserves_totals_not_key_counts() {
        let mut a = TopK::default();
        let mut b = TopK::default();
        for _ in 0..10 { a.observe(1); }
        for _ in 0..30 { b.observe(1); }
        for _ in 0..10 { b.observe(2); }
        a.merge(&b);
        assert_eq!(a.total, 50);
        assert!((a.top_share() - 40.0 / 50.0).abs() < 1e-9);
    }

    #[test]
    fn empty_topk_is_not_skewed() {
        assert_eq!(TopK::default().top_share(), 0.0);
    }

    // ── Windows ──────────────────────────────────────────────────────────

    #[test]
    fn recent_never_yields_the_open_bucket() {
        let mut w: EdgeWindow = EdgeWindow::default();
        w.current_mut().count = 7;
        // Nothing sealed yet.
        assert_eq!(w.recent(5).count(), 0);
        w.advance();
        assert_eq!(w.recent(5).map(|b| b.count).sum::<u64>(), 7);
    }

    #[test]
    fn rates_average_over_sealed_buckets_only_not_the_asked_window() {
        // Three sealed seconds of 100 ev/s. A naive `sum / window_s` would
        // report 60 ev/s for a 5 s query and make a busy edge look idle for
        // the first seconds of every run.
        let mut w: EdgeWindow = EdgeWindow::default();
        for _ in 0..3 {
            w.current_mut().count = 100;
            w.advance();
        }
        let mut stats = LiveStats::default();
        stats.edges.insert(("a".into(), "b".into()), w);
        let e = ("a".to_string(), "b".to_string());
        assert!((stats.edge_rate(&e, 5) - 100.0).abs() < 1e-9);
        assert!((stats.edge_rate(&e, 2) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn queries_on_an_unknown_edge_or_stage_return_zero() {
        let stats = LiveStats::default();
        let e = ("nope".to_string(), "nothing".to_string());
        assert_eq!(stats.edge_p(&e, 0.99, 5), 0.0);
        assert_eq!(stats.edge_rate(&e, 5), 0.0);
        assert_eq!(stats.edge_bytes_rate(&e, 5), 0.0);
        assert_eq!(stats.key_skew(&e, 5), 0.0);
        assert_eq!(stats.trend(&e, 0.99, 5), 0.0);
        assert_eq!(stats.stage_wasm_p("nope", 0.99, 5), 0.0);
        assert_eq!(stats.stage_overhead_p("nope", 0.99, 5), 0.0);
    }

    #[test]
    fn trend_signs_match_the_direction_of_travel() {
        let build = |gaps: &[u64]| {
            let mut w: EdgeWindow = EdgeWindow::default();
            for g in gaps {
                for _ in 0..50 {
                    w.current_mut().gap.observe_us(*g);
                }
                w.advance();
            }
            let mut s = LiveStats::default();
            s.edges.insert(("a".into(), "b".into()), w);
            s
        };
        let e = ("a".to_string(), "b".to_string());
        // Oldest first: a p50 climbing 100 -> 800 us must slope up.
        assert!(build(&[100, 200, 400, 800]).trend(&e, 0.5, 10) > 0.0);
        assert!(build(&[800, 400, 200, 100]).trend(&e, 0.5, 10) < 0.0);
        assert_eq!(build(&[300, 300, 300, 300]).trend(&e, 0.5, 10), 0.0);
        // Fewer than two sealed buckets is not a trend.
        assert_eq!(build(&[300]).trend(&e, 0.5, 10), 0.0);
    }

    // ── The collector-side fold ──────────────────────────────────────────

    /// Hops for one event crossing `a -> b -> c` with the given gaps in µs.
    fn hops_with_gaps(t0: f64, ab_gap_us: f64, bc_gap_us: f64) -> Vec<(String, f64, f64)> {
        let us = 1e-6;
        let a_exit = t0 + 10.0 * us;
        let b_enter = a_exit + ab_gap_us * us;
        let b_exit = b_enter + 10.0 * us;
        let c_enter = b_exit + bc_gap_us * us;
        vec![
            ("a#0".to_string(), t0, a_exit),
            ("b#1".to_string(), b_enter, b_exit),
            ("c#0".to_string(), c_enter, c_enter + 10.0 * us),
        ]
    }

    fn collector() -> (LiveCollector, Arc<LiveStatsHandle>) {
        let handle = Arc::new(LiveStatsHandle::default());
        let reg = Arc::new(TimingRegistry::default());
        (LiveCollector::new(reg, handle.clone()), handle)
    }

    #[test]
    fn fold_attributes_each_gap_to_its_own_edge() {
        let (mut lc, handle) = collector();
        // 2000 us on a->b, 5 us on b->c: the shape of an unfused pipeline
        // whose first edge is the bottleneck.
        for i in 0..500 {
            lc.observe(0.1 + i as f64 * 1e-4, &hops_with_gaps(1000.0, 2000.0, 5.0), 128, Some(7));
        }
        lc.seal_if_due(1.5);

        let s = handle.load();
        let ab = ("a".to_string(), "b".to_string());
        let bc = ("b".to_string(), "c".to_string());
        let ab_p50 = s.edge_p(&ab, 0.5, 5);
        let bc_p50 = s.edge_p(&bc, 0.5, 5);
        assert!((ab_p50 - 2000.0).abs() / 2000.0 < 0.20, "a->b p50 = {ab_p50}");
        assert!((bc_p50 - 5.0).abs() / 5.0 < 0.20, "b->c p50 = {bc_p50}");
        // Replica suffixes must not split one edge into many.
        assert_eq!(s.edges.len(), 2, "edges: {:?}", s.edges.keys().collect::<Vec<_>>());
    }

    #[test]
    fn fold_counts_events_and_bytes_per_edge() {
        let (mut lc, handle) = collector();
        for i in 0..300 {
            lc.observe(0.1 + i as f64 * 1e-4, &hops_with_gaps(0.5, 100.0, 100.0), 64, None);
        }
        lc.seal_if_due(1.5);
        let s = handle.load();
        let ab = ("a".to_string(), "b".to_string());
        assert!((s.edge_rate(&ab, 5) - 300.0).abs() < 1.0);
        assert!((s.edge_bytes_rate(&ab, 5) - 300.0 * 64.0).abs() < 64.0);
        // No key on the wire is not evidence of uniformity OR of skew.
        assert_eq!(s.key_skew(&ab, 5), 0.0);
    }

    #[test]
    fn fold_detects_key_skew() {
        let (mut lc, handle) = collector();
        for i in 0..1_000 {
            // 90 % of traffic on one key.
            let key = if i % 10 == 0 { i as u64 } else { 42 };
            lc.observe(0.1 + i as f64 * 1e-5, &hops_with_gaps(0.5, 50.0, 50.0), 32, Some(key));
        }
        lc.seal_if_due(1.5);
        let s = handle.load();
        let ab = ("a".to_string(), "b".to_string());
        assert!(s.key_skew(&ab, 5) > 0.8, "skew = {}", s.key_skew(&ab, 5));
    }

    #[test]
    fn a_contracted_edge_reports_a_near_zero_gap() {
        // What a successful fusion looks like in this telemetry: the gap
        // collapses to sub-microsecond. Clamping (rather than dropping)
        // negatives is what keeps that visible instead of deleting the very
        // evidence the morph worked.
        let (mut lc, handle) = collector();
        for i in 0..200 {
            lc.observe(0.1 + i as f64 * 1e-4, &hops_with_gaps(0.5, -0.2, 5.0), 128, None);
        }
        lc.seal_if_due(1.5);
        let s = handle.load();
        // The bottom bucket absorbs everything under 1 µs and reports ~1 µs —
        // three orders of magnitude below the ~2 ms this edge cost unfused,
        // which is the distinction the controller actually acts on.
        let p99 = s.edge_p(&("a".to_string(), "b".to_string()), 0.99, 5);
        assert!(p99 < 2.0, "a contracted edge should read sub-2 us, got {p99}");
    }

    #[test]
    fn an_idle_second_seals_an_empty_bucket_and_drops_the_rate() {
        let (mut lc, handle) = collector();
        for i in 0..100 {
            lc.observe(0.1 + i as f64 * 1e-4, &hops_with_gaps(0.5, 100.0, 100.0), 32, None);
        }
        lc.seal_if_due(1.5);
        let ab = ("a".to_string(), "b".to_string());
        assert!((handle.load().edge_rate(&ab, 1) - 100.0).abs() < 1.0);

        // Four idle seconds. The rate over the last 5 s must fall, or a
        // controller would keep acting on a pipeline that stopped.
        lc.seal_if_due(5.5);
        let s = handle.load();
        assert!(s.edge_rate(&ab, 1) < 1.0, "1 s rate = {}", s.edge_rate(&ab, 1));
        assert!(s.edge_rate(&ab, 5) < 30.0, "5 s rate = {}", s.edge_rate(&ab, 5));
    }

    #[test]
    fn a_long_idle_gap_does_not_blow_the_ring() {
        let (mut lc, _handle) = collector();
        lc.observe(0.1, &hops_with_gaps(0.5, 10.0, 10.0), 32, None);
        // An hour with nothing: catch-up must be capped at the ring size
        // rather than looping 3600 times per seal.
        lc.seal_if_due(3_600.0);
        lc.observe(3_600.2, &hops_with_gaps(0.5, 10.0, 10.0), 32, None);
        lc.seal_if_due(3_601.5);
    }

    #[test]
    fn stage_windows_follow_the_timing_registry_including_stages_born_later() {
        let handle = Arc::new(LiveStatsHandle::default());
        let reg = Arc::new(TimingRegistry::default());
        let mut lc = LiveCollector::new(reg.clone(), handle.clone());

        let a = reg.get_or_insert("fn-a");
        let mut acc = TimingAccum::default();
        for _ in 0..100 {
            acc.observe(10_000, 2_000, 4_000); // 10 us total, 4 us wasm
        }
        acc.flush_into(&a);
        lc.seal_if_due(1.5);

        let s = handle.load();
        assert!((s.stage_wasm_p("fn-a", 0.5, 5) - 4.0).abs() < 1.0);
        assert!((s.stage_overhead_p("fn-a", 0.5, 5) - 6.0).abs() < 1.5);
        assert!((s.stage_rate("fn-a", 5) - 100.0).abs() < 1.0);

        // A stage created by a morph appears without any wiring between the
        // actuator and the collector.
        let fused = reg.get_or_insert("fn-a+fn-b");
        let mut acc2 = TimingAccum::default();
        for _ in 0..50 {
            acc2.observe(20_000, 3_000, 9_000);
        }
        acc2.flush_into(&fused);
        lc.seal_if_due(2.5);
        let s2 = handle.load();
        assert!((s2.stage_wasm_p("fn-a+fn-b", 0.5, 5) - 9.0).abs() < 2.0);
    }

    #[test]
    fn snapshot_carries_its_own_timestamp() {
        let (mut lc, handle) = collector();
        assert_eq!(handle.load().t_s, 0.0);
        lc.observe(0.5, &hops_with_gaps(0.5, 10.0, 10.0), 32, None);
        lc.seal_if_due(4.25);
        assert!((handle.load().t_s - 4.25).abs() < 1e-9);
    }

    #[test]
    fn registry_returns_the_same_block_for_a_redeployed_stage() {
        let reg = TimingRegistry::default();
        let a1 = reg.get_or_insert("fn-a");
        a1.events.fetch_add(5, Ordering::Relaxed);
        let a2 = reg.get_or_insert("fn-a");
        assert_eq!(a2.events.load(Ordering::Relaxed), 5, "counters must survive a redeploy");
        assert_eq!(reg.snapshot().len(), 1);
    }
}
