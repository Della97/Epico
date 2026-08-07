//! The cost-model controller — the *policy* half of runtime morphing.
//!
//! Phases 1–4 of M2 delivered the mechanism (fusion, scission, and the measured
//! switch cost *S*); this decides WHEN to use it. Everything downstream of
//! `morph_tx` is unchanged: the controller is simply a second producer of the
//! same [`MorphRequest`]s the YAML schedule already emits, which is the property
//! that keeps a later LLM proposer and this cost model swappable behind one
//! interface — and therefore keeps a head-to-head comparison between them
//! honest rather than a demo.
//!
//! # Shape: one global thread, one pure function
//!
//! The decision is **global**, not per-edge: morphs consume shared resources
//! (instance-pool slots, cores) and adjacent morphs conflict, so exactly one
//! transition may be in flight and the choice between candidates is inherently
//! cross-stage.
//!
//! It is split so the policy is testable without a runtime. [`decide`] is a
//! pure function of a [`Signals`] value — no clock, no I/O, no telemetry
//! handle. That matters beyond tidiness: if the policy could reach out and read
//! things the other arm cannot, an M10 comparison against an LLM proposer
//! degrades into "the arm that could see more", and the honest head-to-head is
//! lost. Everything time-dependent (`since_last_morph_s`, `now_t_s`) is passed
//! IN as data.
//!
//! # The rule, and why each clause is there
//!
//! ```text
//! FUSE (a, b) when ALL hold:
//!   1. legality        the pair is declared fusible and legally contractible
//!   2. edge-dominated  edge_p99(a->b) > alpha * (wasm_p99(a) + wasm_p99(b))
//!   3. pressure        b's queue is rising, or b is pinned at max_replicas
//!                      with a queue over its scale-up threshold
//!   4. admission       pool_free >= 2 * (live(a) + live(b))
//!   5. significance    the per-event gain clears the calibrated noise floor
//!   6. hysteresis      cooldown_fuse_s has elapsed since the last morph
//!
//! REFUSE when compute-bound: wasm dominates at max replicas. Fusion deletes
//! an edge; if the cost is in the wasm call it deletes the wrong thing, and
//! the remedy is replicas, scission, or nothing.
//! ```
//!
//! # Why SPLIT is deliberately crippled
//!
//! Phase 4 measured scission LOSING on throughput in every stateless
//! configuration tested, including 35.9x induced skew — by up to 47 %. A fused
//! stage gets `R_ab = Ra + Rb` replicas and so puts every worker on the
//! bottleneck half, which turns out to beat splitting even when the halves are
//! badly unbalanced. Proposing scission as an optimisation would, on current
//! evidence, make things worse. So splitting exists only to undo *this
//! controller's own* fusions once they stop being justified, and never as a
//! standalone move.

use std::collections::HashMap;

use crate::morph::MorphRequest;
use crate::telemetry::live::{EdgeKey, LiveStats};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Tunables, with defaults derived from M2's measurements rather than picked.
#[derive(Debug, Clone)]
pub(crate) struct ControllerCfg {
    /// Off unless a pipeline asks for it. A `morphs:` schedule and this
    /// controller are alternative producers of the same requests, and a run
    /// measuring break-even at known instants must not have a second opinion.
    pub enabled: bool,
    /// Seconds between decisions. Autoscalers tick at 1 ms; a morph costs
    /// ~10^3 times what a replica spawn does, so deciding at 1 Hz is already
    /// far faster than the mechanism can be exercised.
    pub period_s: f64,
    /// Edge-dominance factor. At 1.0 the edge must cost more per event than
    /// the compute it joins before contracting it is worth considering.
    pub alpha: f64,
    /// Minimum seconds between a morph and the next fuse. 5 s is >= 10x the
    /// measured 0.20 s break-even, so a fusion has paid for itself many times
    /// over before another transition can be proposed.
    pub cooldown_fuse_s: f64,
    /// Minimum seconds before undoing a fusion. Asymmetric on purpose:
    /// splitting is never a win on current evidence, so it is made reluctant.
    pub cooldown_split_s: f64,
    /// Smallest per-event edge cost, in microseconds, that counts as a real
    /// gain rather than measurement noise.
    ///
    /// **Calibrated, not fixed.** Break-even goes as `S^2 / (2*delta)`, so a
    /// near-zero delta drives the estimate toward zero and produces
    /// confident-looking nonsense — the identity control produced *finite*
    /// break-even in 13 of 20 morphs at a p50 of 0.022 s, an order of magnitude
    /// "better" than a real fuse's 0.204 s. The floor also moves with the
    /// workload (15 ms uniform vs 74 ms skewed in the phase-4 arms), so it
    /// belongs in configuration, defaulted from a calibration run of the
    /// deployed build and logged on every refusal so the decision is auditable.
    pub significance_floor_us: f64,
    /// Seconds of history each query averages over. Long enough to smooth a
    /// bursty source, short enough that the controller is reacting to now.
    pub window_s: u64,
    /// Refuse to decide on a snapshot older than this. The collector cannot
    /// publish while no events are arriving, and "the pipeline is idle" must
    /// never be read as "every edge is cheap".
    pub max_stats_age_s: f64,
    /// Below this event rate the edge quantiles rest on too few observations
    /// to act on, and a morph would cost more than the traffic is worth.
    pub min_rate_eps: f64,
}

impl Default for ControllerCfg {
    fn default() -> Self {
        ControllerCfg {
            enabled: false,
            period_s: 1.0,
            alpha: 1.0,
            cooldown_fuse_s: 5.0,
            cooldown_split_s: 30.0,
            significance_floor_us: 50.0,
            window_s: 5,
            max_stats_age_s: 3.0,
            min_rate_eps: 100.0,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Inputs
// ─────────────────────────────────────────────────────────────────────────────

/// What the decision knows about one physical stage.
#[derive(Debug, Clone, Default)]
pub(crate) struct StageSignal {
    pub live: usize,
    pub max: usize,
    pub queue_depth: u64,
    /// The previous tick's depth. Carried as data so the "is pressure rising"
    /// test lives inside the pure function instead of becoming hidden state in
    /// the loop — which is what would make it untestable.
    pub queue_depth_prev: u64,
    /// This stage's own scale-up threshold, so the rule reads the same
    /// pressure signal the autoscaler does rather than inventing a second one.
    pub queue_up: f64,
}

/// A declared fusible pair, with its legality already resolved.
///
/// Legality is precomputed by the caller rather than evaluated here so that
/// `decide` needs no `Config` and no method calls on live objects — a table of
/// these is a literal in a test. `illegal_reason` carries the *named* reason
/// the runtime would refuse the contraction, and it is propagated into the
/// verdict so a refusal is auditable rather than silent.
#[derive(Debug, Clone)]
pub(crate) struct PairSignal {
    pub a: String,
    pub b: String,
    pub illegal_reason: Option<String>,
}

/// A fusion currently in force that this controller created.
#[derive(Debug, Clone)]
pub(crate) struct FusedSignal {
    /// Physical stage name of the contraction.
    pub name: String,
    pub a: String,
    pub b: String,
    /// Seconds since this fusion was applied.
    pub age_s: f64,
}

/// Everything the decision reads. Borrowed, so building one costs nothing per
/// tick, and entirely inert — there is no handle in here that could perform I/O.
pub(crate) struct Signals<'a> {
    pub stats: &'a LiveStats,
    pub stages: &'a HashMap<String, StageSignal>,
    pub pairs: &'a [PairSignal],
    /// Fusions this controller is responsible for. A fusion applied by a YAML
    /// schedule is deliberately absent: the controller undoes its own decisions
    /// and nothing else.
    pub fused: &'a [FusedSignal],
    /// Free instance-pool slots.
    pub pool_free: usize,
    pub since_last_morph_s: f64,
    /// Seconds since test start, for judging snapshot staleness.
    pub now_t_s: f64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Output
// ─────────────────────────────────────────────────────────────────────────────

/// A decision plus the reason for it. The request is what the actuator needs;
/// the reason is what makes the run auditable — a controller that declines to
/// act is making a claim, and that claim has to be inspectable after the fact.
#[derive(Debug, Clone)]
pub(crate) struct Decision {
    pub request: Option<MorphRequest>,
    pub reason: String,
}

impl Decision {
    fn hold(reason: impl Into<String>) -> Self {
        Decision { request: None, reason: reason.into() }
    }
    fn act(request: MorphRequest, reason: impl Into<String>) -> Self {
        Decision { request: Some(request), reason: reason.into() }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The decision
// ─────────────────────────────────────────────────────────────────────────────

/// PURE. No I/O, no clock, no telemetry handle — the whole policy is decidable
/// from a literal [`Signals`], which is what makes an M10 comparison against a
/// different proposer meaningful rather than a demonstration.
pub(crate) fn decide(sig: &Signals, cfg: &ControllerCfg) -> Decision {
    if !cfg.enabled {
        return Decision::hold("controller disabled");
    }

    // Freshness before anything else. Every rule below reads "the edge is
    // cheap" as evidence against fusing, and a stale snapshot of an idle
    // pipeline says exactly that about every edge in it.
    let age = sig.now_t_s - sig.stats.t_s;
    if age > cfg.max_stats_age_s {
        return Decision::hold(format!(
            "stats are {age:.1}s old (max {:.1}s) — the pipeline is idle or the \
             collector is blocked; not acting on a stale view",
            cfg.max_stats_age_s
        ));
    }

    // Undoing a fusion outranks making one: a contraction that has stopped
    // paying is consuming `Ra + Rb` replicas on the wrong shape, and stacking a
    // second morph on top of it would only compound the mistake.
    if let Some(d) = consider_split(sig, cfg) {
        return d;
    }

    if sig.since_last_morph_s <= cfg.cooldown_fuse_s {
        return Decision::hold(format!(
            "hysteresis: {:.1}s since the last morph, cooldown is {:.1}s",
            sig.since_last_morph_s, cfg.cooldown_fuse_s
        ));
    }

    // Candidates are scored so the busiest genuinely-edge-bound pair wins, not
    // whichever happens to be declared first. Refusals are collected for the
    // log — a controller that does nothing must be able to say why.
    let mut best: Option<(f64, MorphRequest, String)> = None;
    let mut refusals: Vec<String> = Vec::new();

    for pair in sig.pairs {
        match consider_fuse(sig, cfg, pair) {
            Ok((score, req, why)) => {
                if best.as_ref().is_none_or(|(s, _, _)| score > *s) {
                    best = Some((score, req, why));
                }
            }
            Err(why) => refusals.push(why),
        }
    }

    match best {
        Some((_, req, why)) => Decision::act(req, why),
        None if refusals.is_empty() => Decision::hold("no fusible pairs declared"),
        None => Decision::hold(refusals.join("; ")),
    }
}

/// `Ok((score, request, why))` when this pair should be fused; `Err(why)` with
/// the binding reason otherwise. The reason names the FIRST failing clause, in
/// rule order, so a log line reads as a diagnosis rather than a dump.
fn consider_fuse(
    sig: &Signals,
    cfg: &ControllerCfg,
    pair: &PairSignal,
) -> Result<(f64, MorphRequest, String), String> {
    let (a, b) = (&pair.a, &pair.b);
    let tag = format!("{a}->{b}");

    // ── 1. Legality ─────────────────────────────────────────────────────────
    if let Some(reason) = &pair.illegal_reason {
        return Err(format!("{tag}: illegal ({reason})"));
    }
    // Already contracted: `a` and `b` do not both exist as live stages.
    let (Some(sa), Some(sb)) = (sig.stages.get(a), sig.stages.get(b)) else {
        return Err(format!("{tag}: not both live (already fused, or not started)"));
    };

    let edge: EdgeKey = (a.clone(), b.clone());
    let window = cfg.window_s;

    // Rate gate. Quantiles over a handful of events are not evidence, and a
    // morph on a near-idle edge costs more than the traffic it would help.
    let rate = sig.stats.edge_rate(&edge, window);
    if rate < cfg.min_rate_eps {
        return Err(format!(
            "{tag}: {rate:.0} ev/s is below the {:.0} ev/s floor",
            cfg.min_rate_eps
        ));
    }

    // ── 2. Edge-dominated, and its contrapositive ───────────────────────────
    //
    // Both sides are compared at the MEDIAN, not at p99, and that choice is the
    // difference between a working diagnosis and one that fuses everything.
    //
    // The per-edge gap is `next.enter_ts - prev.exit_ts`, which is transport
    // cost PLUS whatever time the event spent waiting in the edge's ring. Those
    // two are not separable from hop timestamps, and they call for opposite
    // actions: transport cost is what a contraction deletes, while queue wait
    // is caused by the consumer being slow and merely MOVES upstream when the
    // stages are merged — a fused worker still has to run both halves.
    //
    // At p99 the queueing term dominates the gap on any pipeline that is even
    // briefly behind, including a compute-bound one whose queue is nominally
    // empty. Measured on the negative fixture: a pipeline with ~576 us halves
    // and a queue oscillating between 0 and 1 reported an edge p99 of 6656 us
    // against a compute p99 of 1152 us — "edge-dominated", and wrong. The same
    // instant at p50 read 144 us of edge against ~1100 us of compute, which is
    // the correct reading. The tail is measuring the consumer, not the edge.
    //
    // p99 is still computed and still reported, because a bad tail is
    // diagnostic information an operator wants; it just does not get a vote.
    let edge_p50 = sig.stats.edge_p(&edge, 0.5, window);
    let edge_p99 = sig.stats.edge_p(&edge, 0.99, window);
    let compute = sig.stats.stage_wasm_p(a, 0.5, window) + sig.stats.stage_wasm_p(b, 0.5, window);
    let compute_p99 =
        sig.stats.stage_wasm_p(a, 0.99, window) + sig.stats.stage_wasm_p(b, 0.99, window);

    // The compute-bound refusal is checked FIRST and reported by name, because
    // it is a different statement from "the edge is merely not dominant": at
    // max replicas with the compute winning, no amount of waiting will make
    // fusing correct, and the operator needs to see that rather than a generic
    // "not dominant" that reads like a transient.
    let at_max = sa.live >= sa.max && sb.live >= sb.max && sa.max > 0 && sb.max > 0;
    if at_max && compute > edge_p50 {
        return Err(format!(
            "{tag}: compute-bound at max replicas (wasm p50 {compute:.0}us > edge p50 \
             {edge_p50:.0}us; p99s {compute_p99:.0}us vs {edge_p99:.0}us) — fusion \
             deletes an edge that is not the problem; the remedy is replicas or nothing"
        ));
    }
    if edge_p50 <= cfg.alpha * compute {
        return Err(format!(
            "{tag}: edge p50 {edge_p50:.0}us <= alpha {:.2} x compute p50 {compute:.0}us \
             (p99s {edge_p99:.0}us vs {compute_p99:.0}us)",
            cfg.alpha
        ));
    }

    // ── 3. Pressure ─────────────────────────────────────────────────────────
    // Either the downstream queue is growing, or it is already deep with no
    // replicas left to add. Without this the controller would fuse a pipeline
    // that is keeping up perfectly well, paying S for nothing.
    let rising = sb.queue_depth > sb.queue_depth_prev;
    let pinned = sb.max > 0 && sb.live >= sb.max && (sb.queue_depth as f64) > sb.queue_up;
    if !rising && !pinned {
        return Err(format!(
            "{tag}: no pressure on {b} (queue {} -> {}, live {}/{})",
            sb.queue_depth_prev, sb.queue_depth, sb.live, sb.max
        ));
    }

    // ── 4. Admission ────────────────────────────────────────────────────────
    // The boot-time reservation is already `2 * max_ab` per declared pair; this
    // is the runtime re-check, refused by name like every other rule. A fused
    // spawn that fails for want of pool slots would fail exactly when the
    // system is under pressure — the worst possible moment.
    let need = 2 * (sa.live + sb.live);
    if sig.pool_free < need {
        return Err(format!(
            "{tag}: pool has {} free, needs {need} (2 x (live {} + live {}))",
            sig.pool_free, sa.live, sb.live
        ));
    }

    // ── 5. Significance ─────────────────────────────────────────────────────
    // The typical per-event cost the contraction deletes, against the
    // calibrated noise floor.
    let gain_us = edge_p50;
    if gain_us <= cfg.significance_floor_us {
        return Err(format!(
            "{tag}: projected gain {gain_us:.1}us is below the calibrated \
             significance floor {:.1}us — refusing to act on noise",
            cfg.significance_floor_us
        ));
    }

    // ── 6. Hysteresis is global and was checked by the caller ───────────────

    // Score: aggregate transport time deleted per second. This ranks a cheap
    // edge carrying enormous traffic above an expensive one carrying almost
    // none, which is the correct order — the gain is per event.
    let score = gain_us * rate;
    let why = format!(
        "fuse {tag}: edge p50 {edge_p50:.0}us > {:.2} x compute p50 {compute:.0}us \
         (p99s {edge_p99:.0}us vs {compute_p99:.0}us), gain {gain_us:.1}us/ev at \
         {rate:.0} ev/s (score {score:.0}us/s), queue {} -> {} on {b}, \
         pool {} free of {need} needed",
        cfg.alpha, sb.queue_depth_prev, sb.queue_depth, sig.pool_free
    );
    Ok((score, MorphRequest::Fuse { a: a.clone(), b: b.clone() }, why))
}

/// Should any fusion this controller made be undone?
///
/// # Why the premise cannot be tested the obvious way
///
/// The design note phrased this as "condition 2 no longer holds for the
/// contracted edge". That reading is not implementable, and the reason is worth
/// recording: a fused worker still emits **one hop per half**, so the `a -> b`
/// edge is still present in the live stats after the contraction — reading
/// ~1 us, by construction, because that is what fusing it did. Testing
/// condition 2 against it would therefore be false the instant the fusion
/// succeeded, and the controller would split immediately, refuse, fuse, and
/// oscillate at exactly the cooldown period. The gain is no longer observable
/// once taken; only its absence is.
///
/// What IS observable is whether the fused stage has become compute-bound: at
/// max replicas with wasm dominating its own overhead, the contraction is no
/// longer what limits it, so the replicas it monopolises would do more good
/// split across two independently-scaled stages. That is the same diagnosis
/// rule the fuse side uses, applied to the merged stage.
fn consider_split(sig: &Signals, cfg: &ControllerCfg) -> Option<Decision> {
    for f in sig.fused {
        if f.age_s <= cfg.cooldown_split_s {
            continue;
        }
        let Some(s) = sig.stages.get(&f.name) else { continue };
        if s.max == 0 || s.live < s.max {
            continue;
        }
        let wasm = sig.stats.stage_wasm_p(&f.name, 0.99, cfg.window_s);
        let overhead = sig.stats.stage_overhead_p(&f.name, 0.99, cfg.window_s);
        if wasm > overhead && (s.queue_depth as f64) > s.queue_up {
            return Some(Decision::act(
                MorphRequest::Split { fused: f.name.clone() },
                format!(
                    "split {} (restoring {} -> {}): compute-bound at max replicas \
                     ({}/{}) after {:.0}s — wasm p99 {wasm:.0}us > overhead p99 \
                     {overhead:.0}us with queue {} over threshold {:.0}; the \
                     contraction is no longer what limits it",
                    f.name, f.a, f.b, s.live, s.max, f.age_s, s.queue_depth, s.queue_up
                ),
            ));
        }
    }
    None
}

impl ControllerCfg {
    /// Resolve the YAML block against the measurement-derived defaults.
    pub(crate) fn from_spec(spec: &crate::config::ControllerSpec) -> Self {
        let d = ControllerCfg::default();
        ControllerCfg {
            enabled: spec.enabled,
            period_s: spec.period_s.unwrap_or(d.period_s).max(0.05),
            alpha: spec.alpha.unwrap_or(d.alpha).max(0.0),
            cooldown_fuse_s: spec.cooldown_fuse_s.unwrap_or(d.cooldown_fuse_s).max(0.0),
            cooldown_split_s: spec.cooldown_split_s.unwrap_or(d.cooldown_split_s).max(0.0),
            significance_floor_us: spec
                .significance_floor_us
                .unwrap_or(d.significance_floor_us)
                .max(0.0),
            window_s: spec.window_s.unwrap_or(d.window_s).max(1),
            max_stats_age_s: d.max_stats_age_s,
            min_rate_eps: spec.min_rate_eps.unwrap_or(d.min_rate_eps).max(0.0),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The loop
// ─────────────────────────────────────────────────────────────────────────────

/// The controller thread. Deliberately trivial: it gathers signals, calls
/// [`decide`], and forwards the result. All the thinking is in the pure
/// function, and `morph_tx` is the SAME channel the YAML schedule uses — so
/// nothing downstream of it can tell which producer spoke.
pub(crate) fn run_controller(
    cfg: ControllerCfg,
    dep: std::sync::Arc<crate::morph::Deployer>,
    live: std::sync::Arc<crate::telemetry::live::LiveStatsHandle>,
    tx: std::sync::mpsc::Sender<MorphRequest>,
    total_pool: usize,
    test_start: std::time::Instant,
    log: epico_logger::Logger,
) {
    use epico_logger::{info, warn};
    use std::sync::atomic::Ordering;

    if !cfg.enabled {
        return;
    }
    info!(log, "cost-model controller armed",
          period_s = format!("{:.2}", cfg.period_s),
          alpha = format!("{:.2}", cfg.alpha),
          significance_floor_us = format!("{:.1}", cfg.significance_floor_us),
          cooldown_fuse_s = format!("{:.1}", cfg.cooldown_fuse_s),
          cooldown_split_s = format!("{:.1}", cfg.cooldown_split_s),
          window_s = cfg.window_s,
          pool = total_pool);

    let morph_in_flight = dep.morph_in_flight();
    let pairs: Vec<PairSignal> = dep
        .config
        .fusible_pairs()
        .into_iter()
        .map(|(a, b)| {
            let illegal_reason = dep.config.fusion_illegal_reason(&a, &b);
            PairSignal { a, b, illegal_reason }
        })
        .collect();

    // Fusions THIS controller applied, with the instant each was requested.
    // Kept here rather than in the Deployer because it is a property of the
    // policy, not of the runtime: a fusion a YAML schedule applied is none of
    // this controller's business to undo.
    let mut mine: Vec<(String, String, String, std::time::Instant)> = Vec::new();
    let mut last_morph = std::time::Instant::now();
    let mut prev_depths: HashMap<String, u64> = HashMap::new();
    // Only log a verdict when it changes, so a run's log carries the decision
    // history rather than one line per second saying the same thing.
    let mut last_reason = String::new();

    let period = std::time::Duration::from_secs_f64(cfg.period_s);
    loop {
        std::thread::sleep(period);
        if crate::supervisor::SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }
        // The actuator serialises transitions anyway, but proposing into a
        // morph already in flight would mean deciding from statistics measured
        // against a topology that no longer exists.
        if morph_in_flight.load(Ordering::SeqCst) {
            continue;
        }

        let mut stages = dep.stage_signals();
        // A stage that vanished (retired by a morph) must not leave a stale
        // depth behind to be read as "flat" next time it appears.
        prev_depths.retain(|k, _| stages.contains_key(k));
        for (name, s) in stages.iter_mut() {
            s.queue_depth_prev = prev_depths.get(name).copied().unwrap_or(s.queue_depth);
        }

        // Drop any fusion that is no longer live — the actuator refused it, or
        // something else retired it. Either way it is not ours to undo.
        let live_names = dep.live_stage_names();
        mine.retain(|(fused, _, _, _)| live_names.iter().any(|n| n == fused));
        let fused: Vec<FusedSignal> = mine
            .iter()
            .map(|(name, a, b, at)| FusedSignal {
                name: name.clone(),
                a: a.clone(),
                b: b.clone(),
                age_s: at.elapsed().as_secs_f64(),
            })
            .collect();

        let stats = live.load();
        let decision = decide(
            &Signals {
                stats: &stats,
                stages: &stages,
                pairs: &pairs,
                fused: &fused,
                pool_free: total_pool.saturating_sub(dep.live_replica_total()),
                since_last_morph_s: last_morph.elapsed().as_secs_f64(),
                now_t_s: test_start.elapsed().as_secs_f64(),
            },
            &cfg,
        );

        for (name, s) in stages.iter() {
            prev_depths.insert(name.clone(), s.queue_depth);
        }

        match decision.request {
            Some(req) => {
                info!(log, "controller proposing a morph", decision = decision.reason.as_str());
                if let MorphRequest::Fuse { ref a, ref b } = req {
                    mine.push((
                        crate::config::fused_stage_name(a, b),
                        a.clone(),
                        b.clone(),
                        std::time::Instant::now(),
                    ));
                }
                if let MorphRequest::Split { ref fused } = req {
                    mine.retain(|(name, _, _, _)| name != fused);
                }
                if tx.send(req).is_err() {
                    warn!(log, "morph actuator is gone; controller stopping");
                    break;
                }
                last_morph = std::time::Instant::now();
                last_reason.clear();
            }
            None => {
                // Logged at info, not debug, and only when the verdict CHANGES.
                // A controller declining to act is making a claim — "this pair
                // is compute-bound", "this gain is below the calibrated floor" —
                // and a claim that leaves no trace cannot be audited after the
                // run. Change-only keeps that from becoming one line a second
                // saying the same thing.
                if decision.reason != last_reason {
                    info!(log, "controller holding", reason = decision.reason.as_str());
                    last_reason = decision.reason;
                }
            }
        }
    }
    info!(log, "cost-model controller stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::live::{EdgeWindow, StageWindow};

    // ── Fixtures ─────────────────────────────────────────────────────────

    /// A LiveStats with one `a -> b` edge at a fixed per-event gap and rate,
    /// and per-stage wasm/overhead costs. Built through the same sealed-bucket
    /// path the collector uses, so the queries under test are the real ones.
    struct Fixture {
        stats: LiveStats,
        stages: HashMap<String, StageSignal>,
    }

    impl Fixture {
        fn new() -> Self {
            Fixture { stats: LiveStats { t_s: 100.0, ..Default::default() }, stages: HashMap::new() }
        }

        fn edge(mut self, from: &str, to: &str, gap_us: u64, rate: u64) -> Self {
            let mut w = EdgeWindow::default();
            for _ in 0..5 {
                {
                    let b = w.current_mut();
                    b.count = rate;
                    b.bytes = rate * 128;
                    for _ in 0..64 {
                        b.gap.observe_us(gap_us);
                    }
                }
                w.advance_for_test();
            }
            self.stats.edges.insert((from.to_string(), to.to_string()), w);
            self
        }

        fn compute(mut self, stage: &str, wasm_us: u64, overhead_us: u64) -> Self {
            let mut w = StageWindow::default();
            for _ in 0..5 {
                {
                    let b = w.current_mut();
                    b.events = 1_000;
                    for _ in 0..64 {
                        b.wasm.observe_us(wasm_us);
                        b.overhead.observe_us(overhead_us);
                    }
                }
                w.advance_for_test();
            }
            self.stats.stages.insert(stage.to_string(), w);
            self
        }

        fn stage(mut self, name: &str, live: usize, max: usize, qd_prev: u64, qd: u64) -> Self {
            self.stages.insert(
                name.to_string(),
                StageSignal { live, max, queue_depth: qd, queue_depth_prev: qd_prev, queue_up: 50.0 },
            );
            self
        }
    }

    fn pairs() -> Vec<PairSignal> {
        vec![PairSignal { a: "a".into(), b: "b".into(), illegal_reason: None }]
    }

    fn cfg() -> ControllerCfg {
        ControllerCfg { enabled: true, ..Default::default() }
    }

    /// The canonical fusible situation: a 2 ms edge joining two ~30 us stages,
    /// downstream queue growing, replicas and pool slots available. Every test
    /// below perturbs exactly one thing about this.
    fn edge_bound() -> Fixture {
        Fixture::new()
            .edge("a", "b", 2_000, 20_000)
            .compute("a", 30, 20)
            .compute("b", 30, 20)
            .stage("a", 2, 4, 0, 0)
            .stage("b", 2, 4, 10, 400)
    }

    fn run(fx: &Fixture, ps: &[PairSignal], cfg: &ControllerCfg) -> Decision {
        run_with(fx, ps, cfg, &[], 999.0, 64)
    }

    fn run_with(
        fx: &Fixture,
        ps: &[PairSignal],
        cfg: &ControllerCfg,
        fused: &[FusedSignal],
        since_last_morph_s: f64,
        pool_free: usize,
    ) -> Decision {
        decide(
            &Signals {
                stats: &fx.stats,
                stages: &fx.stages,
                pairs: ps,
                fused,
                pool_free,
                since_last_morph_s,
                now_t_s: fx.stats.t_s + 0.5,
            },
            cfg,
        )
    }

    // ── The rule, clause by clause ───────────────────────────────────────

    #[test]
    fn edge_dominated_and_under_pressure_fuses() {
        let d = run(&edge_bound(), &pairs(), &cfg());
        match d.request {
            Some(MorphRequest::Fuse { ref a, ref b }) => {
                assert_eq!((a.as_str(), b.as_str()), ("a", "b"), "{}", d.reason);
            }
            other => panic!("expected a fuse, got {other:?} — {}", d.reason),
        }
    }

    #[test]
    fn compute_bound_at_max_replicas_refuses_by_name() {
        // The counter-case the whole diagnosis exists for: an expensive edge
        // that is nonetheless dwarfed by the compute it joins.
        let fx = Fixture::new()
            .edge("a", "b", 2_000, 20_000)
            .compute("a", 4_000, 20)
            .compute("b", 4_000, 20)
            .stage("a", 4, 4, 0, 0)
            .stage("b", 4, 4, 10, 400);
        let d = run(&fx, &pairs(), &cfg());
        assert!(d.request.is_none(), "must not fuse a compute-bound pair");
        assert!(d.reason.contains("compute-bound"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_merely_non_dominant_edge_refuses_differently_than_a_compute_bound_one() {
        // Same cost ordering, but replicas are still available — so this is a
        // transient, not a verdict, and the log must not claim otherwise.
        let fx = Fixture::new()
            .edge("a", "b", 20, 20_000)
            .compute("a", 4_000, 20)
            .compute("b", 4_000, 20)
            .stage("a", 1, 4, 0, 0)
            .stage("b", 1, 4, 10, 400);
        let d = run(&fx, &pairs(), &cfg());
        assert!(d.request.is_none());
        assert!(!d.reason.contains("compute-bound"), "reason was: {}", d.reason);
        assert!(d.reason.contains("alpha"), "reason was: {}", d.reason);
    }

    #[test]
    fn an_illegal_pair_is_refused_with_the_runtimes_own_reason() {
        let ps = vec![PairSignal {
            a: "a".into(),
            b: "b".into(),
            illegal_reason: Some("fan-out at the source".into()),
        }];
        let d = run(&edge_bound(), &ps, &cfg());
        assert!(d.request.is_none());
        assert!(d.reason.contains("fan-out at the source"), "reason was: {}", d.reason);
    }

    #[test]
    fn no_pressure_means_no_morph_however_expensive_the_edge() {
        // The pipeline is keeping up: queue flat and replicas spare. Paying S
        // here buys nothing.
        let fx = edge_bound().stage("b", 2, 4, 400, 0);
        let d = run(&fx, &pairs(), &cfg());
        assert!(d.request.is_none());
        assert!(d.reason.contains("no pressure"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_pinned_stage_with_a_deep_queue_is_pressure_even_when_flat() {
        // Not rising, but at max replicas with a queue over threshold: nothing
        // more the autoscaler can do, which is precisely when a morph is the
        // only remaining move.
        let fx = edge_bound().stage("b", 4, 4, 400, 400);
        let d = run(&fx, &pairs(), &cfg());
        assert!(matches!(d.request, Some(MorphRequest::Fuse { .. })), "{}", d.reason);
    }

    #[test]
    fn admission_is_rechecked_at_runtime() {
        let d = run_with(&edge_bound(), &pairs(), &cfg(), &[], 999.0, 3);
        assert!(d.request.is_none());
        assert!(d.reason.contains("pool has 3 free"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_gain_below_the_calibrated_floor_is_refused_and_says_so() {
        // 60 us edge against a 100 us floor: dominant over the compute, under
        // pressure, admissible — and still not worth a morph.
        let fx = Fixture::new()
            .edge("a", "b", 60, 20_000)
            .compute("a", 5, 5)
            .compute("b", 5, 5)
            .stage("a", 2, 4, 0, 0)
            .stage("b", 2, 4, 10, 400);
        let c = ControllerCfg { significance_floor_us: 100.0, ..cfg() };
        let d = run(&fx, &pairs(), &c);
        assert!(d.request.is_none());
        assert!(d.reason.contains("significance floor"), "reason was: {}", d.reason);
        // The floor in force must appear in the refusal, or the decision is
        // not auditable after the fact.
        assert!(d.reason.contains("100.0"), "reason was: {}", d.reason);
    }

    #[test]
    fn hysteresis_blocks_a_second_morph_inside_the_cooldown() {
        let d = run_with(&edge_bound(), &pairs(), &cfg(), &[], 1.0, 64);
        assert!(d.request.is_none());
        assert!(d.reason.contains("hysteresis"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_quiet_edge_is_not_evidence() {
        let fx = edge_bound().edge("a", "b", 2_000, 5);
        let d = run(&fx, &pairs(), &cfg());
        assert!(d.request.is_none());
        assert!(d.reason.contains("below the"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_stale_snapshot_is_never_acted_on() {
        let fx = edge_bound();
        let d = decide(
            &Signals {
                stats: &fx.stats,
                stages: &fx.stages,
                pairs: &pairs(),
                fused: &[],
                pool_free: 64,
                since_last_morph_s: 999.0,
                // Ten seconds past the snapshot: the pipeline went quiet, and
                // every edge in a quiet pipeline reads cheap.
                now_t_s: fx.stats.t_s + 10.0,
            },
            &cfg(),
        );
        assert!(d.request.is_none());
        assert!(d.reason.contains("stale"), "reason was: {}", d.reason);
    }

    #[test]
    fn a_disabled_controller_proposes_nothing() {
        let d = run(&edge_bound(), &pairs(), &ControllerCfg::default());
        assert!(d.request.is_none());
        assert!(d.reason.contains("disabled"));
    }

    #[test]
    fn no_declared_pairs_is_reported_as_such_not_as_a_refusal() {
        let d = run(&edge_bound(), &[], &cfg());
        assert!(d.request.is_none());
        assert!(d.reason.contains("no fusible pairs"), "reason was: {}", d.reason);
    }

    #[test]
    fn the_busiest_edge_bound_pair_wins() {
        // Two legal candidates; the one deleting more aggregate transport time
        // per second must be chosen, not the one declared first.
        let fx = Fixture::new()
            .edge("a", "b", 200, 1_000)
            .edge("c", "d", 200, 50_000)
            .compute("a", 5, 5).compute("b", 5, 5)
            .compute("c", 5, 5).compute("d", 5, 5)
            .stage("a", 2, 4, 0, 0).stage("b", 2, 4, 10, 400)
            .stage("c", 2, 4, 0, 0).stage("d", 2, 4, 10, 400);
        let ps = vec![
            PairSignal { a: "a".into(), b: "b".into(), illegal_reason: None },
            PairSignal { a: "c".into(), b: "d".into(), illegal_reason: None },
        ];
        let d = run(&fx, &ps, &cfg());
        match d.request {
            Some(MorphRequest::Fuse { ref a, ref b }) => {
                assert_eq!((a.as_str(), b.as_str()), ("c", "d"), "{}", d.reason)
            }
            other => panic!("expected a fuse, got {other:?} — {}", d.reason),
        }
    }

    // ── Split ────────────────────────────────────────────────────────────

    #[test]
    fn a_fusion_that_went_compute_bound_is_undone() {
        let fx = Fixture::new()
            .compute("a_b", 4_000, 20)
            .stage("a_b", 4, 4, 400, 400);
        let fused = vec![FusedSignal {
            name: "a_b".into(), a: "a".into(), b: "b".into(), age_s: 60.0,
        }];
        let d = run_with(&fx, &[], &cfg(), &fused, 999.0, 64);
        match d.request {
            Some(MorphRequest::Split { ref fused }) => assert_eq!(fused, "a_b"),
            other => panic!("expected a split, got {other:?} — {}", d.reason),
        }
    }

    #[test]
    fn a_fresh_fusion_is_never_undone_however_it_looks() {
        // This is the oscillation guard. Immediately after a fuse the
        // contracted edge reads ~1 us BY CONSTRUCTION, so any rule keyed on it
        // would fire instantly; the asymmetric cooldown is what stops a
        // fuse/split loop at the cooldown period.
        let fx = Fixture::new()
            .compute("a_b", 4_000, 20)
            .stage("a_b", 4, 4, 400, 400);
        let fused = vec![FusedSignal {
            name: "a_b".into(), a: "a".into(), b: "b".into(), age_s: 2.0,
        }];
        let d = run_with(&fx, &[], &cfg(), &fused, 999.0, 64);
        assert!(d.request.is_none(), "reason was: {}", d.reason);
    }

    #[test]
    fn a_fusion_that_is_still_overhead_bound_is_left_alone() {
        let fx = Fixture::new()
            .compute("a_b", 20, 4_000)
            .stage("a_b", 4, 4, 400, 400);
        let fused = vec![FusedSignal {
            name: "a_b".into(), a: "a".into(), b: "b".into(), age_s: 60.0,
        }];
        let d = run_with(&fx, &[], &cfg(), &fused, 999.0, 64);
        assert!(d.request.is_none(), "reason was: {}", d.reason);
    }

    #[test]
    fn a_fusion_with_replica_headroom_is_left_alone() {
        // Not at max: the autoscaler still has moves, so a morph is premature.
        let fx = Fixture::new()
            .compute("a_b", 4_000, 20)
            .stage("a_b", 2, 4, 400, 400);
        let fused = vec![FusedSignal {
            name: "a_b".into(), a: "a".into(), b: "b".into(), age_s: 60.0,
        }];
        let d = run_with(&fx, &[], &cfg(), &fused, 999.0, 64);
        assert!(d.request.is_none(), "reason was: {}", d.reason);
    }

    #[test]
    fn splitting_outranks_fusing_when_both_apply() {
        let mut fx = edge_bound();
        fx = fx.compute("a_b", 4_000, 20).stage("a_b", 4, 4, 400, 400);
        let fused = vec![FusedSignal {
            name: "a_b".into(), a: "x".into(), b: "y".into(), age_s: 60.0,
        }];
        let d = run_with(&fx, &pairs(), &cfg(), &fused, 999.0, 64);
        assert!(matches!(d.request, Some(MorphRequest::Split { .. })), "{}", d.reason);
    }

    #[test]
    fn a_fusion_this_controller_did_not_make_is_not_undone() {
        // `fused` carries only this controller's own contractions; a YAML
        // schedule's fusion is deliberately invisible here.
        let fx = Fixture::new()
            .compute("a_b", 4_000, 20)
            .stage("a_b", 4, 4, 400, 400);
        let d = run_with(&fx, &[], &cfg(), &[], 999.0, 64);
        assert!(d.request.is_none(), "reason was: {}", d.reason);
    }
}
