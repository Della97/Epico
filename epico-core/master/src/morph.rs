//! Runtime topology morphing — stage fusion and scission (roadmap M2).
//!
//! The *logical* DAG never changes. What changes is the *physical* topology:
//! an edge `a -> b` is contracted so one worker runs both halves back to back
//! with no serialization, no queue, and no thread hand-off between them — or
//! the contraction is undone so the halves scale independently again.
//!
//! # Fusion is edge contraction, so no edge is rewired
//!
//! The fused stage's input **is** `a`'s existing input edge and its output
//! **is** `b`'s existing out-edges. The `a -> b` edge is not moved anywhere: it
//! is drained and abandoned. That is why this milestone needs no runtime mesh
//! swap. What it does need is index-space *width* — a fused replica must be a
//! valid consumer index on `a`'s in-mesh and a valid producer index on `b`'s
//! out-mesh — and that is a boot-time sizing decision driven by the `fusible:`
//! declaration (see [`crate::config::Config::max_consumers`]).
//!
//! # Protocol #1: stop-the-world
//!
//! ```text
//!   drain a  →  wait a→b empty  →  drain b  →  deploy fused  →  resume
//! ```
//!
//! This is deliberately the WORST of the three switch protocols in the M2
//! break-even table, and the point of implementing it first: it is trivial, it
//! is obviously correct, and it is the baseline the cheaper protocols
//! (shadow+cutover, in-place surgical rewiring) have to beat. Because the old
//! path is fully quiesced before the new one starts, there is no overlap
//! window at all — so this protocol conserves events across the switch
//! boundary AND preserves whatever per-key ordering the replica set already
//! provided. The protocols that overlap the two topologies will have to give
//! up the second of those; this one does not.
//!
//! Upstream is not dropped during the stall, it is BACKPRESSURED: producers
//! block on the bounded input edge exactly as they do under any slow consumer.
//! The stall is therefore the honest switch cost *S* rather than a hole in the
//! event stream.
//!
//! # Morph × EOS are mutually exclusive
//!
//! Barriers are per-stage with `expected_in` fixed at boot, so after a
//! contraction `b`'s barrier could never receive its marker. A morph is refused
//! outright once any barrier has seen all its markers, and a marker that lands
//! inside the teardown window is moved onto the replacement stage's barrier
//! ([`StageEosBarrier::drain_into`]). Lifting the restriction — a genuinely
//! mid-stream EOS — needs the transition epoch marker, which belongs to the
//! in-place-rewiring protocol.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use epico_logger::Logger;
use epico_logger::{error, info, warn};
use wasmtime::Engine;

use crate::config::{fused_stage_name, Config, FusedHalf, PipelineStage};
use crate::eos::StageEosBarrier;
use crate::spsc::{EdgeInSrc, EdgeOutSrc};
use crate::telemetry::live::TimingRegistry;
use crate::telemetry::{record_event, RunTelemetry};

/// How long the actuator waits for the eliminated edge to empty before giving
/// up. Generous: at any sane event rate a quiesced edge empties in
/// milliseconds, so hitting this means something is genuinely wedged and the
/// operator needs to see it rather than have the run hang.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(1);

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// What the actuator is asked to do.
///
/// These are deliberately the first verbs of the action grammar a proposer will
/// later emit, so a cost-model controller and an LLM proposer are swappable
/// behind one interface — which is the property that keeps a later head-to-head
/// comparison honest. Today the only producer is the scripted schedule.
#[derive(Debug, Clone)]
pub(crate) enum MorphRequest {
    /// Contract the edge `a -> b` into one fused physical stage.
    Fuse { a: String, b: String },
    /// Undo a contraction, restoring both halves as independent stages.
    Split { fused: String },
    /// Redeploy a stage unchanged. Zero expected steady-state delta BY
    /// CONSTRUCTION, which is exactly what isolates switch mechanism cost from
    /// any workload effect. A surprising number here means the harness is
    /// wrong, not the intuition.
    Identity { stage: String },
}

impl MorphRequest {
    fn describe(&self) -> String {
        match self {
            Self::Fuse { a, b }      => format!("fuse {a} -> {b}"),
            Self::Split { fused }    => format!("split {fused}"),
            Self::Identity { stage } => format!("identity {stage}"),
        }
    }

    /// The stage this transition's telemetry is filed under: the one that
    /// EXISTS after it. The summary's scaling block only keeps events whose
    /// stage is in the run's declared stage list, so filing a morph under a
    /// free-text description would drop it from the very summary the
    /// break-even analysis reads.
    fn subject(&self) -> String {
        match self {
            Self::Fuse { a, b }      => fused_stage_name(a, b),
            Self::Split { fused }    => fused.clone(),
            Self::Identity { stage } => stage.clone(),
        }
    }

    fn verb(&self) -> &'static str {
        match self {
            Self::Fuse { .. }     => "fuse",
            Self::Split { .. }    => "split",
            Self::Identity { .. } => "identity",
        }
    }
}

// ---------------------------------------------------------------------------
// Deployment templates + registry
// ---------------------------------------------------------------------------

/// Everything needed to start one stage's autoscaler. Built once per stage at
/// boot and kept, so a stage can be torn down and brought back later (a split
/// restores exactly the halves' original wiring).
#[derive(Clone)]
pub(crate) struct StageDeployment {
    pub stage:         PipelineStage,
    pub in_edge:       EdgeInSrc,
    pub out_edges:     Vec<EdgeOutSrc>,
    pub ctrl_port:     u16,
    pub credit_window: u32,
    /// The EOS barrier's `expected_in`: this stage's in-degree in the LOGICAL
    /// DAG, so a fan-in stage only finishes once every upstream branch has
    /// delivered its marker.
    pub expected_in:   usize,
}

/// A live stage: its control-loop thread and the handles needed to retire it.
pub(crate) struct StageRuntime {
    stop:          Arc<AtomicBool>,
    handle:        std::thread::JoinHandle<()>,
    barrier:       Arc<StageEosBarrier>,
    live_replicas: Arc<AtomicUsize>,
    /// Backlog on this stage's input, republished every autoscaler tick. The
    /// controller's pressure test reads the same signal the autoscaler scales
    /// on, rather than inventing a second notion of "busy".
    queue_depth:   Arc<std::sync::atomic::AtomicU64>,
    /// The deployment this stage was started from — a split reads the fused
    /// stage's halves back out of here.
    deployment:    StageDeployment,
}

impl StageRuntime {
    pub(crate) fn live(&self) -> usize {
        self.live_replicas.load(Ordering::Relaxed)
    }
}

/// The mutable stage set. Replaces the boot-time `Vec<JoinHandle>`: stages can
/// now be started and stopped while the pipeline runs, which is the one real
/// structural change M2 needs from the pre-existing runtime.
pub(crate) struct Deployer {
    pub engine:       Engine,
    pub telemetry:    Arc<Mutex<RunTelemetry>>,
    pub test_start:   Instant,
    pub compile_mode: String,
    pub event_format: String,
    pub log:          Logger,
    pub config:       Config,
    /// Immutable after boot: one entry per stage declared in the pipeline.
    pub templates:    HashMap<String, StageDeployment>,
    /// Per-stage worker timing, keyed by physical stage name. It lives here
    /// rather than in `templates` because a morph *creates* physical stages:
    /// `fn-a+fn-b` has no template and no counters until the fusion that makes
    /// it. Registered on the way up by `spawn_stage`; read by the collector for
    /// the live stats and by each stage's own autoscaler for the summary.
    pub timings:      Arc<TimingRegistry>,
    stages:           Mutex<HashMap<String, StageRuntime>>,
    /// True while a transition is in flight. Read by the supervisor loop, which
    /// must not mistake a transient empty/quiescing stage set for a crash, and
    /// by the EOS finishing gate.
    morph_in_flight:  Arc<AtomicBool>,
}

impl Deployer {
    pub(crate) fn new(
        engine:       Engine,
        telemetry:    Arc<Mutex<RunTelemetry>>,
        test_start:   Instant,
        compile_mode: String,
        event_format: String,
        log:          Logger,
        config:       Config,
        templates:    HashMap<String, StageDeployment>,
        // Shared with the collector rather than created here: the collector
        // starts before any stage does, and it is the registry's other reader.
        timings:      Arc<TimingRegistry>,
    ) -> Self {
        Deployer {
            engine, telemetry, test_start, compile_mode, event_format, log, config, templates,
            timings,
            stages: Mutex::new(HashMap::new()),
            morph_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn morph_in_flight(&self) -> Arc<AtomicBool> {
        self.morph_in_flight.clone()
    }

    /// True when every live stage's control thread has exited. Pre-M2 this
    /// meant "everything crashed"; with a mutable stage set it is also the
    /// transient state in the middle of a morph, so the supervisor has to
    /// consult `morph_in_flight` before treating it as fatal.
    pub(crate) fn all_stages_finished(&self) -> bool {
        let stages = self.stages.lock().expect("stage registry poisoned");
        !stages.is_empty() && stages.values().all(|s| s.handle.is_finished())
    }

    pub(crate) fn live_stage_names(&self) -> Vec<String> {
        let stages = self.stages.lock().expect("stage registry poisoned");
        let mut names: Vec<String> = stages.keys().cloned().collect();
        names.sort();
        names
    }

    /// Start one stage. `initial_replicas` is a transient spawn floor above
    /// `min_replicas`, used to bring a morphed stage up at the right size
    /// immediately instead of waiting for the queue to build.
    pub(crate) fn spawn_stage(&self, dep: StageDeployment, initial_replicas: usize) {
        let name          = dep.stage.name.clone();
        let stop          = Arc::new(AtomicBool::new(false));
        let barrier       = Arc::new(StageEosBarrier::new(dep.expected_in.max(1)));
        let live_replicas = Arc::new(AtomicUsize::new(0));
        let queue_depth   = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let stage_c   = dep.stage.clone();
        let in_edge   = dep.in_edge.clone();
        let out_edges = dep.out_edges.clone();
        let ctrl_port = dep.ctrl_port;
        let cw        = dep.credit_window;
        let engine_c  = self.engine.clone();
        let tel_c     = self.telemetry.clone();
        let stage_log = self.log.with_component(&format!("autoscaler/{name}"));
        let compile_c = self.compile_mode.clone();
        let format_c  = self.event_format.clone();
        let test_start = self.test_start;
        let barrier_c = barrier.clone();
        let stop_c    = stop.clone();
        let live_c    = live_replicas.clone();
        let qd_c      = queue_depth.clone();
        // Registered before the thread starts, so a controller that samples
        // between spawn and the stage's first tick sees an idle stage rather
        // than a missing one.
        let timing_c  = self.timings.get_or_insert(&name);

        let handle = std::thread::spawn(move || {
            crate::autoscaler::run_autoscaler_loop(
                stage_c, ctrl_port, cw, in_edge, out_edges, engine_c, stage_log, tel_c,
                test_start, compile_c, format_c, barrier_c, stop_c, initial_replicas, live_c,
                timing_c, qd_c,
            );
        });

        let mut stages = self.stages.lock().expect("stage registry poisoned");
        stages.insert(name, StageRuntime {
            stop, handle, barrier, live_replicas, queue_depth, deployment: dep,
        });
    }

    /// Per-stage runtime state for the controller, in one lock acquisition.
    ///
    /// Taken as a single consistent-enough snapshot rather than field by field:
    /// the decision compares `live` against `max` and reads `queue_depth` in the
    /// same breath, and sampling those across separate lock acquisitions is how
    /// a controller ends up reasoning about a stage that no longer exists.
    pub(crate) fn stage_signals(&self) -> HashMap<String, crate::controller::StageSignal> {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages
            .iter()
            .map(|(name, rt)| {
                let slo = &rt.deployment.stage.slo;
                (
                    name.clone(),
                    crate::controller::StageSignal {
                        live: rt.live(),
                        max: slo.max_replicas,
                        queue_depth: rt.queue_depth.load(Ordering::Relaxed),
                        // Filled in by the controller loop from its own previous
                        // tick: "rising" is a property of the time series, and
                        // the registry holds only the present.
                        queue_depth_prev: 0,
                        queue_up: slo.queue_up.unwrap_or(50.0),
                    },
                )
            })
            .collect()
    }

    /// Instance-pool slots currently committed to live replicas. The engine was
    /// sized at boot for `total_max`, so the controller's admission check is
    /// `total_max - this`.
    pub(crate) fn live_replica_total(&self) -> usize {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages.values().map(|s| s.live()).sum()
    }

    /// Retire a stage: raise its stop flag and block until its control loop has
    /// drained every replica and exited. Returns the retired runtime so the
    /// caller can move EOS state onto the replacement.
    fn stop_stage(&self, name: &str) -> Option<StageRuntime> {
        let runtime = {
            let mut stages = self.stages.lock().expect("stage registry poisoned");
            stages.remove(name)?
        };
        runtime.stop.store(true, Ordering::SeqCst);
        let t0 = Instant::now();
        // Bounded in practice by the drain, not by this loop: a stopping stage's
        // workers stop accepting new input and exit as soon as what they already
        // hold is flushed downstream.
        while !runtime.handle.is_finished() {
            if t0.elapsed() > DRAIN_TIMEOUT {
                error!(self.log, "stage did not stop within the drain timeout",
                       stage = name, waited_s = format!("{:.1}", t0.elapsed().as_secs_f64()));
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        info!(self.log, "stage retired",
              stage = name,
              drain_ms = format!("{:.3}", t0.elapsed().as_secs_f64() * 1000.0));
        Some(runtime)
    }

    fn barrier_of(&self, name: &str) -> Option<Arc<StageEosBarrier>> {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages.get(name).map(|s| s.barrier.clone())
    }

    fn live_of(&self, name: &str) -> usize {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages.get(name).map(|s| s.live()).unwrap_or(0)
    }

    fn deployment_of(&self, name: &str) -> Option<StageDeployment> {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages.get(name).map(|s| s.deployment.clone())
    }

    /// True if ANY live stage has already seen every EOS marker it expects.
    /// Once that is true the run is finishing and a morph must be refused: the
    /// stage set is about to change shape underneath a barrier that can no
    /// longer be satisfied.
    fn eos_in_progress(&self) -> bool {
        let stages = self.stages.lock().expect("stage registry poisoned");
        stages.values().any(|s| s.barrier.all_markers_seen() || s.barrier.is_finishing())
    }

    fn record(&self, stage: &str, action: &str, new_count: usize) {
        record_event(&self.telemetry, self.test_start, stage, action,
                     new_count, None, None, None);
    }

    /// Block until `edge` reports zero occupancy, or the timeout expires.
    /// Returns false on timeout — which means events are stranded in an edge
    /// that is about to be abandoned, and the conservation test will say so.
    fn wait_edge_empty(&self, label: &str, edge: &EdgeInSrc) -> bool {
        let t0 = Instant::now();
        loop {
            if edge.len() == 0 {
                return true;
            }
            if t0.elapsed() > DRAIN_TIMEOUT {
                error!(self.log, "eliminated edge did not drain; events may be stranded",
                       edge = label, depth = edge.len());
                return false;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

// ---------------------------------------------------------------------------
// The actuator
// ---------------------------------------------------------------------------

/// Consume morph requests, one at a time, forever.
///
/// Serialization is deliberate and global: morphs consume shared resources
/// (instance-pool slots, cores) and adjacent morphs conflict, so exactly one
/// transition is in flight at any moment.
pub(crate) fn run_actuator(
    rx:  std::sync::mpsc::Receiver<MorphRequest>,
    dep: Arc<Deployer>,
) {
    while let Ok(req) = rx.recv() {
        if crate::supervisor::SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }
        apply(&dep, req);
    }
    info!(dep.log, "morph actuator stopped");
}

fn apply(dep: &Deployer, req: MorphRequest) {
    let what    = req.describe();
    let subject = req.subject();

    // ── D5: morph × EOS are mutually exclusive ───────────────────────────────
    if dep.eos_in_progress() {
        warn!(dep.log, "morph refused: the run is finishing (EOS barrier armed)",
              request = what.as_str());
        return;
    }

    let t0 = Instant::now();
    dep.morph_in_flight.store(true, Ordering::SeqCst);
    info!(dep.log, "morph begin", request = what.as_str());
    // `morph_begin` and `morph_end` BRACKET the transition on the subject
    // stage's timeline, with the verb event in between. That is what lets an
    // offline analysis line the switch window up against the per-event log
    // without any new telemetry.
    dep.record(&subject, "morph_begin", 0);

    let ok = match &req {
        MorphRequest::Fuse { a, b }      => do_fuse(dep, a, b),
        MorphRequest::Split { fused }    => do_split(dep, fused),
        MorphRequest::Identity { stage } => do_identity(dep, stage),
    };

    let switch_ms = t0.elapsed().as_secs_f64() * 1000.0;
    dep.morph_in_flight.store(false, Ordering::SeqCst);
    if ok {
        info!(dep.log, "morph end", request = what.as_str(),
              verb = req.verb(),
              switch_ms = format!("{:.3}", switch_ms));
    } else {
        warn!(dep.log, "morph aborted", request = what.as_str(),
              switch_ms = format!("{:.3}", switch_ms));
    }
    // The CONTROL-PLANE switch window: teardown + deploy, as the actuator sees
    // it. The headline S is measured on the DATA plane instead — the arrival
    // gap and e2e spike around the morph, read off the per-event log — because
    // that is what the events actually experienced. The two should bracket each
    // other, and a large divergence means the stall is somewhere this timer
    // does not cover.
    dep.record(&subject, if ok { "morph_end" } else { "morph_failed" }, 0);
}

/// Contract `a -> b`. See the module docs for the protocol and why it stalls.
fn do_fuse(dep: &Deployer, a: &str, b: &str) -> bool {
    if let Some(reason) = dep.config.fusion_illegal_reason(a, b) {
        warn!(dep.log, "fuse refused: illegal edge contraction",
              a = a, b = b, reason = reason.as_str());
        return false;
    }
    let (Some(a_dep), Some(b_dep)) = (dep.deployment_of(a), dep.deployment_of(b)) else {
        warn!(dep.log, "fuse refused: a half is not currently live", a = a, b = b);
        return false;
    };

    // R_ab = Ra + Rb, captured BEFORE teardown. Replica counts encode service
    // times, and a fused worker holds its thread for tA + tB per event.
    let max_ab = dep.config.fused_max_replicas(a, b).unwrap_or(1);
    let r_ab   = (dep.live_of(a) + dep.live_of(b)).clamp(1, max_ab);

    // ── Step 1: quiesce `a`. ─────────────────────────────────────────────────
    let a_rt = dep.stop_stage(a);

    // ── Step 2: let `b` finish everything `a` left in the edge being deleted.
    // This is the whole conservation argument for the contracted edge: once it
    // reads empty with no producer left alive, nothing can be stranded in it.
    if !dep.wait_edge_empty(&format!("{a} -> {b}"), &b_dep.in_edge) {
        // Keep going: aborting here would leave the pipeline with `a` already
        // retired, which is worse. The stranded count surfaces in the
        // conservation test, which is where it should surface.
    }

    // ── Step 3: quiesce `b`. ─────────────────────────────────────────────────
    let b_rt = dep.stop_stage(b);

    // ── Step 4: deploy the fused stage. ──────────────────────────────────────
    // Input IS `a`'s in-edge; output IS `b`'s out-edges. Nothing is rewired.
    let name = fused_stage_name(a, b);
    let mut stage = a_dep.stage.clone();
    stage.name       = name.clone();
    stage.outputs    = b_dep.stage.outputs.clone();
    stage.output     = b_dep.stage.output.clone();
    stage.slo        = a_dep.stage.slo.clone();
    stage.slo.max_replicas = max_ab;
    stage.slo.min_replicas =
        (a_dep.stage.slo.min_replicas + b_dep.stage.slo.min_replicas).min(max_ab);
    stage.fused_from = halves_of(&a_dep.stage)
        .into_iter()
        .chain(halves_of(&b_dep.stage))
        .collect();

    let fused_dep = StageDeployment {
        stage,
        in_edge:       a_dep.in_edge.clone(),
        out_edges:     b_dep.out_edges.clone(),
        ctrl_port:     a_dep.ctrl_port,
        credit_window: a_dep.credit_window,
        expected_in:   a_dep.expected_in,
    };

    dep.spawn_stage(fused_dep, r_ab);
    carry_eos_forward(dep, &name, [a_rt, b_rt]);

    info!(dep.log, "fused", a = a, b = b, stage = name.as_str(), r_ab = r_ab, max_ab = max_ab);
    dep.record(&name, "fuse", r_ab);
    true
}

/// Undo a contraction. The mirror of `do_fuse`: quiesce the fused stage, then
/// restore both halves from the templates captured at boot — the `a -> b` edge
/// they used still exists, it was only left idle.
fn do_split(dep: &Deployer, fused: &str) -> bool {
    let Some(fused_dep) = dep.deployment_of(fused) else {
        warn!(dep.log, "split refused: no such live stage", stage = fused);
        return false;
    };
    if !fused_dep.stage.is_fused() {
        warn!(dep.log, "split refused: stage is not fused", stage = fused);
        return false;
    }
    let halves: Vec<String> = fused_dep.stage.fused_from.iter()
        .map(|h| h.name.clone()).collect();
    let missing: Vec<&String> = halves.iter()
        .filter(|h| !dep.templates.contains_key(*h)).collect();
    if !missing.is_empty() {
        warn!(dep.log, "split refused: no boot deployment for a half",
              stage = fused, missing = format!("{missing:?}"));
        return false;
    }

    let r_ab = dep.live_of(fused);
    let fused_rt = dep.stop_stage(fused);

    // Split sizes need no guessing in the general case — the fused worker's
    // hops still time each half separately, so the observed tA:tB share is
    // available offline. Live per-half timing is a later enabler, so until it
    // exists the split allocates by the halves' declared ceilings, which is the
    // same information the pipeline author used to size them.
    let weights: Vec<usize> = halves.iter()
        .map(|h| dep.templates[h].stage.slo.max_replicas.max(1))
        .collect();
    let total_w: usize = weights.iter().sum();

    let mut allocated = 0usize;
    for (i, half) in halves.iter().enumerate() {
        let template = dep.templates[half].clone();
        let want = if i + 1 == halves.len() {
            r_ab.saturating_sub(allocated)
        } else {
            (r_ab * weights[i]) / total_w.max(1)
        };
        allocated += want;
        let initial = want
            .max(template.stage.slo.min_replicas)
            .min(template.stage.slo.max_replicas);
        dep.spawn_stage(template, initial);
        info!(dep.log, "split half restored", stage = half, initial_replicas = initial);
    }

    // The fused barrier's markers belong to the FIRST half — it inherited the
    // fused stage's input edge, so that is where an upstream marker lands now.
    carry_eos_forward(dep, &halves[0], [fused_rt, None]);

    dep.record(fused, "split", r_ab);
    true
}

/// Redeploy a stage unchanged. Same teardown and same deploy as a real morph,
/// with no topology change at all, so what it measures is purely the switch
/// mechanism — the control condition for the break-even numbers.
fn do_identity(dep: &Deployer, name: &str) -> bool {
    let Some(current) = dep.deployment_of(name) else {
        warn!(dep.log, "identity refused: no such live stage", stage = name);
        return false;
    };
    let live = dep.live_of(name);
    let old  = dep.stop_stage(name);
    dep.spawn_stage(current, live.max(1));
    carry_eos_forward(dep, name, [old, None]);
    dep.record(name, "identity", live);
    true
}

/// Move any EOS markers the retired stages accumulated onto the replacement's
/// barrier. See the module docs — this is what closes the race between the
/// mutual-exclusion check and the teardown.
fn carry_eos_forward(
    dep:         &Deployer,
    replacement: &str,
    retired:     [Option<StageRuntime>; 2],
) {
    let Some(new_barrier) = dep.barrier_of(replacement) else { return };
    for rt in retired.into_iter().flatten() {
        let moved = rt.barrier.drain_into(&new_barrier);
        if moved > 0 {
            warn!(dep.log, "EOS marker arrived during the morph; moved to the new stage",
                  stage = replacement, markers = moved);
        }
    }
}

/// The logical halves a physical stage contributes to a fusion. Fusing a chain
/// segment twice (`a→b` then `ab→c`) flattens rather than nesting.
fn halves_of(stage: &PipelineStage) -> Vec<FusedHalf> {
    if stage.fused_from.is_empty() {
        vec![FusedHalf { name: stage.name.clone(), wasm: stage.wasm.clone() }]
    } else {
        stage.fused_from.clone()
    }
}

// ---------------------------------------------------------------------------
// Scripted schedule
// ---------------------------------------------------------------------------

/// Drive the actuator from the YAML `morphs:` block.
///
/// Break-even needs morphs at KNOWN instants, repeated: a controller deciding
/// for itself actively obstructs that measurement. A cost-model controller is a
/// later *producer* of these same requests and changes nothing downstream of
/// this channel.
pub(crate) fn run_schedule(
    specs: Vec<crate::config::MorphSpec>,
    tx:    std::sync::mpsc::Sender<MorphRequest>,
    start: Instant,
    log:   Logger,
) {
    let mut plan: Vec<(f64, MorphRequest)> = Vec::new();
    for spec in specs {
        let req = match (&spec.fuse, &spec.split, &spec.identity) {
            (Some(pair), None, None) if pair.len() == 2 =>
                MorphRequest::Fuse { a: pair[0].clone(), b: pair[1].clone() },
            (None, Some(f), None)  => MorphRequest::Split { fused: f.clone() },
            (None, None, Some(s))  => MorphRequest::Identity { stage: s.clone() },
            _ => {
                warn!(log, "ignoring malformed morph entry: need exactly one of \
                            fuse: [a, b] / split: name / identity: name",
                      at_s = spec.at_s);
                continue;
            }
        };
        plan.push((spec.at_s, req));
    }
    plan.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap_or(std::cmp::Ordering::Equal));
    if plan.is_empty() {
        return;
    }
    info!(log, "morph schedule armed", entries = plan.len());

    for (at_s, req) in plan {
        loop {
            if crate::supervisor::SHUTDOWN.load(Ordering::Relaxed) {
                return;
            }
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed >= at_s {
                break;
            }
            let remaining = Duration::from_secs_f64(at_s - elapsed);
            std::thread::sleep(remaining.min(Duration::from_millis(20)));
        }
        info!(log, "scheduled morph due",
              at_s = format!("{:.3}", at_s),
              request = req.describe().as_str());
        if tx.send(req).is_err() {
            return; // actuator gone
        }
    }
}
