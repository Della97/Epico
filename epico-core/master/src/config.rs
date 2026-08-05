//! Runtime configuration — deserialization targets for runtime.yaml.
//!
//! The agent consumes the "old format" YAML produced by epico-cli's
//! codegen: a flat `pipeline:` + `dispatchers:` shape with placement
//! annotations. The new-format pipeline.yaml (with `types:`, `stages:`,
//! `deploy:`) is a CLI concept; by the time the agent sees anything,
//! the CLI has translated it.
//!
//! Everything here is plain data. No I/O, no validation, no Wasmtime.

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Per-stage scaling / SLO knobs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct StageSlo {
    pub p99_ms: Option<f64>,
    pub max_replicas: usize,
    pub min_replicas: usize,
    #[serde(default)]
    pub queue_up: Option<f64>,
    #[serde(default)]
    pub queue_down: Option<f64>,
    #[serde(default)]
    pub replica_capacity_eps: Option<f64>,
    #[serde(default)]
    pub cooldown_up_s: Option<f64>,
    #[serde(default)]
    pub cooldown_down_s: Option<f64>,
    #[serde(default)]
    pub calm_window: Option<f64>,
}

// ---------------------------------------------------------------------------
// Pipeline stage
// ---------------------------------------------------------------------------

/// One logical stage executed inside a fused physical stage.
///
/// Host-level fusion (FUSION_SCISSION §2, Option A) runs several components
/// back to back in one worker thread, from their already-cached `InstancePre`s.
/// Each half keeps its own logical identity: its own component, its own `Store`,
/// and its own telemetry hop. Nothing is recompiled or statically linked.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct FusedHalf {
    /// The LOGICAL stage name. This is what appears in the telemetry hop label
    /// (`name#replica`), so `per_stage_latency_ms` keeps attributing time to
    /// each half separately — which is what makes informed scission possible.
    pub name: String,
    /// Component path. Falls back to `default_wasm_path(name)` when absent.
    #[serde(default)]
    pub wasm: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct PipelineStage {
    pub name: String,
    pub input: String,
    /// Legacy single-output field. Kept for hand-written and pre-DAG
    /// runtime.yaml files; `normalize_topology` folds it into `outputs`,
    /// which is what the runtime actually reads.
    #[serde(default)]
    pub output: Option<String>,
    /// Downstream endpoints, one per out-edge, index-aligned with this
    /// stage's out-edges in `Config::edges` declaration order. A sink stage
    /// carries exactly one entry: the collector URI.
    #[serde(default)]
    pub outputs: Vec<String>,
    #[serde(default)]
    pub wasm: Option<String>,
    #[serde(default)]
    pub binary: Option<String>,
    pub slo: StageSlo,
    /// Which node runs this stage. Populated by epico-cli; old-format
    /// YAMLs default to "local" so the field is always present.
    #[serde(default = "default_placement")]
    pub placement: String,
    /// PHYSICAL fusion: when non-empty, this one physical stage executes the
    /// listed logical stages back to back in every worker. Empty (the default)
    /// means an ordinary one-component stage — the entire pre-M2 runtime.
    ///
    /// A fused stage is edge contraction on the DAG: its input IS the first
    /// half's original input edge and its output IS the last half's original
    /// out-edges. The eliminated interior edge is drained and abandoned, not
    /// rewired.
    #[serde(default)]
    pub fused_from: Vec<FusedHalf>,
}

impl PipelineStage {
    /// The logical stages this physical stage executes, in call order, as
    /// `(logical name, wasm path)`. Exactly one entry for an ordinary stage,
    /// N for a fused one. The single place the rest of the runtime asks "what
    /// does this worker actually run", so nothing else has to branch on
    /// `fused_from`.
    pub(crate) fn halves(&self) -> Vec<(String, String)> {
        if self.fused_from.is_empty() {
            let wasm = self.wasm.clone()
                .unwrap_or_else(|| default_wasm_path(&self.name));
            return vec![(self.name.clone(), wasm)];
        }
        self.fused_from.iter()
            .map(|h| {
                let wasm = h.wasm.clone()
                    .unwrap_or_else(|| default_wasm_path(&h.name));
                (h.name.clone(), wasm)
            })
            .collect()
    }

    pub(crate) fn is_fused(&self) -> bool {
        self.fused_from.len() > 1
    }
}

/// Name of the physical stage produced by fusing `a` into `b`. Deterministic so
/// the YAML `morphs:` schedule can name the fused stage for a later `split:`
/// without the runtime having to hand an id back.
pub(crate) fn fused_stage_name(a: &str, b: &str) -> String {
    format!("{a}_{b}")
}

pub(crate) fn default_placement() -> String {
    "local".to_string()
}

/// True when this stage should be launched by the agent whose
/// `this_host` matches. Centralized so the rule is in one place —
/// future distributed deployments may extend it (e.g. allow a stage
/// to list multiple acceptable placements for HA), and we want the
/// filter callers to stay identical.
pub(crate) fn stage_owned_by(stage: &PipelineStage, this_host: &str) -> bool {
    stage.placement == this_host
}

// ---------------------------------------------------------------------------
// Dispatchers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct DispatcherConfig {
    pub name: String,
    pub ctrl_port: u16,
    #[serde(default)]
    pub push_port: Option<u16>,
    #[serde(default)]
    pub pull_port: Option<u16>,
    /// Explicit URI for the dispatcher's push (frontend) bind. When set,
    /// the dispatcher binds only here and ignores `push_port` — that's
    /// how the pipeline's `deploy.ingress:` declaration reaches the
    /// runtime. Today only stage 0's dispatcher carries this field
    /// (inner dispatchers still use the auto `push_port`), but the shape
    /// generalizes if we ever want to let any dispatcher override its
    /// entry URI. Supports `tcp://host:port` and `ipc:///path`.
    #[serde(default)]
    pub push_uri: Option<String>,
    /// Credit window for this stage's workers. Each worker may have up
    /// to this many in-flight events at once before the dispatcher has
    /// to wait for a credit refill. Default 1 reproduces the legacy
    /// strict request/reply behaviour. The CLI emits this field in the
    /// generated runtime.yaml; old hand-written YAMLs that don't carry
    /// it parse as window=1 via the `serde(default)` initializer, so
    /// existing pipelines keep working unchanged.
    #[serde(default = "default_credit_window")]
    pub credit_window: u32,
    /// Events packed into a single ROUTER message to a worker. Default 1 keeps
    /// the legacy one-event-per-message wire. Larger values amortise the
    /// per-message zmq overhead; credits are still counted in events so this is
    /// orthogonal to `credit_window`.
    #[serde(default = "default_batch_events")]
    pub batch_events: usize,
    /// Mirrors the stage's placement — the dispatcher always runs on
    /// the same node as its stage. Used by the placement filter.
    #[serde(default = "default_placement")]
    pub placement: String,
}

pub(crate) fn default_credit_window() -> u32 { 1 }
pub(crate) fn default_batch_events() -> usize { 1 }

// ---------------------------------------------------------------------------
// Nodes (declared but unused at runtime today)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct NodeConfig {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub host: String,
    #[allow(dead_code)]
    #[serde(default)]
    pub force_tcp: bool,
}

// ---------------------------------------------------------------------------
// Top-level config (what the agent parses from runtime.yaml)
// ---------------------------------------------------------------------------

/// One directed edge of the pipeline DAG, by stage name.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct EdgeSpec {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Config {
    pub pipeline: Vec<PipelineStage>,
    pub dispatchers: Vec<DispatcherConfig>,
    /// Pipeline topology. Emitted by the CLI from the YAML's `edges:`.
    /// When absent (hand-written or pre-DAG runtime.yaml) it is synthesized
    /// as the linear chain implied by `pipeline` order — see
    /// [`Config::normalize_topology`], the single place that fallback lives.
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    /// Terminal sink URI. Sink stages (out-degree 0) push here and the
    /// collector binds it. Falls back to the last stage's legacy `output`
    /// when absent, which is how every pre-DAG runtime.yaml behaves.
    #[serde(default)]
    pub collector: Option<String>,
    /// The node this agent instance represents. Every stage whose
    /// placement matches this value is owned by this agent; stages
    /// placed elsewhere are ignored (they're owned by another agent
    /// on another host). Defaults to "local" so single-host YAMLs
    /// without this field still work.
    #[serde(default = "default_placement")]
    pub this_host: String,
    /// Declared physical nodes. Unused today (everything is single-
    /// host), but present so the data model is complete. The agent
    /// reads this to resolve cross-host TCP endpoints when routing
    /// traffic to remote dispatchers in a distributed deployment.
    #[allow(dead_code)]
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
    /// Sampling interval for the agent's resource sampler (CPU + RSS),
    /// in ms. The CLI emits this as `resource_sample_interval_ms` in the
    /// generated runtime.yaml; old hand-written YAMLs without this field
    /// keep the historical 1 Hz cadence via the default below. A value
    /// of 0 disables the sampler thread entirely — useful for clean
    /// tail-latency benchmarking, since `sysinfo`'s Mach-based refresh
    /// path on macOS occasionally introduces multi-millisecond jitter.
    #[serde(default = "default_resource_sample_interval_ms")]
    pub resource_sample_interval_ms: u64,
    /// Compilation mode emitted by the CLI:
    ///   "aot"     — agent deserializes a pre-built .cwasm at autoscaler startup;
    ///               cold-start = thread spawn + Wasm instantiation only.
    ///   "jit"     — agent defers .wasm→.cwasm compilation to the first cold-start;
    ///               cold-start measurement includes Cranelift compile time.
    ///   "startup" — (default) agent JIT-compiles at autoscaler startup before
    ///               the loop; compilation is NOT counted in cold-start latency.
    #[serde(default = "default_compile_mode")]
    pub compile_mode: String,
    /// Event envelope format. `json` is the compatible default. `binary` is
    /// scaffolded as the next transport mode, but not yet accepted for real runs.
    #[serde(default = "default_event_format")]
    pub event_format: String,
    /// Wire format the in-process/native SOURCE emits at the ingress
    /// (`json` | `binary`). Independent of `event_format` (interior edges).
    /// Roadmap item 2. The loadgen reads the same key directly from runtime.yaml.
    #[serde(default = "default_source_format")]
    pub source_format: String,
    /// Ingress spine mode for co-located sources: `socket` (default) keeps the
    /// ZMQ ingress dispatcher; `inprocess` lets the host collapse the spine to
    /// in-process rings when the source resolves to this host.
    #[serde(default = "default_ingress_mode")]
    pub ingress_mode: String,
    /// In-process source fan-in width (pump threads). `None` = host default /
    /// `EPICO_SOURCE_THREADS` env override.
    #[serde(default)]
    pub source_threads: Option<usize>,
    /// Transport for stage-to-stage in-process edges:
    ///   "zmq"  — use ZMQ dispatcher (default, same as no inproc)
    ///   "mpmc" — crossbeam bounded ring, shared across all replica pairs
    ///   "spsc" — FastFlow-style N×M SPSC mesh (one ring per producer/consumer
    ///            pair); requires min_replicas == max_replicas on every stage
    /// Env var `EPICO_EDGE_IMPL` overrides this value for ad-hoc experiments.
    #[serde(default = "default_edge_impl")]
    pub edge_impl: String,
    /// Capacity (slots) of each in-process edge ring. Only relevant when
    /// `edge_impl` is "mpmc" or "spsc". Default 1024.
    /// Env var `EPICO_EDGE_CAP` overrides.
    #[serde(default = "default_edge_cap")]
    pub edge_cap: usize,
    /// Capacity of each individual SPSC ring in the mesh. Only relevant when
    /// `edge_impl` is "spsc". Default 64.
    /// Env var `EPICO_SPSC_RING_CAP` overrides.
    #[serde(default = "default_spsc_ring_cap")]
    pub spsc_ring_cap: usize,
    /// Pre-declared physical alternatives (M2 design decision D1). Each entry
    /// is an ordered pair `[a, b]` naming an edge `a -> b` that MAY be
    /// contracted into one fused stage at runtime.
    ///
    /// Declaring the pair up front is what lets the runtime widen the affected
    /// index spaces at boot: the fused stage needs a valid consumer index on
    /// `a`'s in-mesh and a valid producer index on `b`'s out-mesh, and those are
    /// dimension-fixed once built. Widening is a startup sizing decision, so
    /// fusion needs no runtime mesh swap.
    ///
    /// ```yaml
    /// fusible:
    ///   - [normalize, detect]
    /// ```
    #[serde(default)]
    pub fusible: Vec<Vec<String>>,
    /// Deterministic morph schedule. Break-even needs morphs at KNOWN instants,
    /// repeated N >= 10 times; a controller deciding for itself actively
    /// obstructs that measurement. The cost-model controller becomes another
    /// producer of the same requests and changes nothing downstream.
    ///
    /// ```yaml
    /// morphs:
    ///   - { at_s: 10.0, fuse: [normalize, detect] }
    ///   - { at_s: 25.0, split: normalize_detect }
    ///   - { at_s: 40.0, identity: normalize }
    /// ```
    #[serde(default)]
    pub morphs: Vec<MorphSpec>,
}

/// One scheduled morph from the YAML `morphs:` block. Exactly one of
/// `fuse` / `split` / `identity` must be set.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct MorphSpec {
    /// Seconds after agent start at which to issue the request.
    pub at_s: f64,
    /// `[a, b]` — contract the edge `a -> b` into one fused stage.
    #[serde(default)]
    pub fuse: Option<Vec<String>>,
    /// Name of a fused stage to split back into its halves.
    #[serde(default)]
    pub split: Option<String>,
    /// Redeploy a stage unchanged. Zero expected steady-state delta by
    /// construction, which is exactly what isolates SWITCH MECHANISM COST from
    /// any workload effect — the first of the three ordered DAG pairs.
    #[serde(default)]
    pub identity: Option<String>,
}

pub(crate) fn default_source_format() -> String {
    "binary".to_string()
}

pub(crate) fn default_ingress_mode() -> String {
    "socket".to_string()
}

pub(crate) fn default_edge_impl() -> String {
    "zmq".to_string()
}

pub(crate) fn default_edge_cap() -> usize {
    1024
}

pub(crate) fn default_spsc_ring_cap() -> usize {
    256
}

pub(crate) fn default_resource_sample_interval_ms() -> u64 {
    1000
}

pub(crate) fn default_compile_mode() -> String {
    "startup".to_string()
}

pub(crate) fn default_event_format() -> String {
    "binary".to_string()
}

// ---------------------------------------------------------------------------
// Topology (M1)
// ---------------------------------------------------------------------------
//
// The runtime is topology-driven: nothing reads `pipeline` array ORDER to
// decide who feeds whom. `edges` is the single source of truth, and the
// linear-chain fallback below is the only place that array order is still
// consulted (for pre-DAG configs).

impl Config {
    /// Fill in derived topology so the rest of the runtime can assume
    /// `edges` and `outputs` are always populated:
    ///   * empty `edges` → the linear chain implied by `pipeline` order;
    ///   * empty `outputs` → the legacy single `output` field;
    ///   * missing `collector` → the last stage's legacy `output`.
    ///
    /// Idempotent. Called once, right after deserialization.
    pub(crate) fn normalize_topology(&mut self) {
        if self.edges.is_empty() {
            for pair in self.pipeline.windows(2) {
                self.edges.push(EdgeSpec {
                    from: pair[0].name.clone(),
                    to:   pair[1].name.clone(),
                });
            }
        }
        if self.collector.is_none() {
            self.collector = self.pipeline.last().and_then(|s| s.output.clone());
        }
        for stage in self.pipeline.iter_mut() {
            if stage.outputs.is_empty() {
                if let Some(o) = stage.output.clone() {
                    stage.outputs.push(o);
                }
            }
        }
    }

    /// Upstream stage names feeding `stage`.
    pub(crate) fn preds(&self, stage: &str) -> Vec<&str> {
        self.edges.iter()
            .filter(|e| e.to == stage)
            .map(|e| e.from.as_str())
            .collect()
    }

    /// Downstream stage names fed by `stage`, in edge-declaration order —
    /// the same order as that stage's `outputs` entries.
    pub(crate) fn succs(&self, stage: &str) -> Vec<&str> {
        self.edges.iter()
            .filter(|e| e.from == stage)
            .map(|e| e.to.as_str())
            .collect()
    }

    /// Number of upstream branches. This is the EOS barrier's `expected_in`:
    /// a stage is finished only after every in-edge has delivered a marker.
    pub(crate) fn in_degree(&self, stage: &str) -> usize {
        self.edges.iter().filter(|e| e.to == stage).count()
    }

    pub(crate) fn out_degree(&self, stage: &str) -> usize {
        self.edges.iter().filter(|e| e.from == stage).count()
    }

    /// Entry stages (in-degree 0). M1 expects exactly one.
    pub(crate) fn sources(&self) -> Vec<&PipelineStage> {
        self.pipeline.iter()
            .filter(|s| self.in_degree(&s.name) == 0)
            .collect()
    }

    /// Terminal stages (out-degree 0). All of them push to `collector`.
    pub(crate) fn sinks(&self) -> Vec<&PipelineStage> {
        self.pipeline.iter()
            .filter(|s| self.out_degree(&s.name) == 0)
            .collect()
    }

    pub(crate) fn stage(&self, name: &str) -> Option<&PipelineStage> {
        self.pipeline.iter().find(|s| s.name == name)
    }

    /// Declared fusible pairs, as `(a, b)`. Malformed entries (not exactly two
    /// names, or naming a stage this host doesn't run) are dropped — the caller
    /// logs them; a bad declaration must not stop the agent booting.
    pub(crate) fn fusible_pairs(&self) -> Vec<(String, String)> {
        self.fusible.iter()
            .filter(|p| p.len() == 2)
            .filter(|p| self.stage(&p[0]).is_some() && self.stage(&p[1]).is_some())
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect()
    }

    /// Replica-index space width the physical stage occupying `stage`'s INPUT
    /// edge may need.
    ///
    /// Ordinarily that is just the stage's own `max_replicas`. But if `stage` is
    /// the `a` half of a declared fusible pair, the fused stage consumes on
    /// exactly this edge with up to `max_a + max_b` replicas — so the mesh is
    /// built that wide at boot and a fused replica `r` is a valid consumer
    /// index by construction, with no resize, ever.
    pub(crate) fn max_consumers(&self, stage: &str) -> usize {
        let own = self.stage(stage).map(|s| s.slo.max_replicas).unwrap_or(0);
        self.fusible_pairs().iter()
            .filter(|(a, _)| a == stage)
            .filter_map(|(a, b)| self.fused_max_replicas(a, b))
            .fold(own, usize::max)
    }

    /// Producer-index space width `stage` may need on each of its OUT-edges.
    /// Mirror of [`Config::max_consumers`]: if `stage` is the `b` half of a
    /// fusible pair, the fused stage produces on this edge.
    pub(crate) fn max_producers(&self, stage: &str) -> usize {
        let own = self.stage(stage).map(|s| s.slo.max_replicas).unwrap_or(0);
        self.fusible_pairs().iter()
            .filter(|(_, b)| b == stage)
            .filter_map(|(a, b)| self.fused_max_replicas(a, b))
            .fold(own, usize::max)
    }

    /// `max_ab = max_a + max_b`. Replica counts encode service times (Little:
    /// Ra ≈ λ·tA), and a fused worker holds its thread for tA + tB per event,
    /// so the fused ceiling is the SUM, not the max.
    pub(crate) fn fused_max_replicas(&self, a: &str, b: &str) -> Option<usize> {
        Some(self.stage(a)?.slo.max_replicas + self.stage(b)?.slo.max_replicas)
    }

    /// Why the edge `a -> b` may not be contracted, or `None` if it may.
    ///
    /// Fusion is edge contraction on the DAG, so it is only legal on a linear
    /// chain segment: fusing across a fan-out or a fan-in would change
    /// semantics (broadcast copies, merge ordering). Co-location is required
    /// because host-level fusion is intra-process by definition.
    pub(crate) fn fusion_illegal_reason(&self, a: &str, b: &str) -> Option<String> {
        let (Some(sa), Some(sb)) = (self.stage(a), self.stage(b)) else {
            return Some(format!("stage {a:?} or {b:?} is not on this host"));
        };
        if !self.edges.iter().any(|e| e.from == a && e.to == b) {
            return Some(format!("no edge {a} -> {b} in the topology"));
        }
        if self.out_degree(a) != 1 {
            return Some(format!("out-degree({a}) = {} != 1 (fan-out)", self.out_degree(a)));
        }
        if self.in_degree(b) != 1 {
            return Some(format!("in-degree({b}) = {} != 1 (fan-in)", self.in_degree(b)));
        }
        if sa.placement != sb.placement {
            return Some(format!(
                "{a} is placed on {:?} but {b} on {:?}; fusion is intra-process",
                sa.placement, sb.placement
            ));
        }
        if sa.is_fused() || sb.is_fused() {
            return Some(format!("{a} or {b} is already a fused stage"));
        }
        if !self.fusible_pairs().iter().any(|(x, y)| x == a && y == b) {
            return Some(format!(
                "[{a}, {b}] is not declared under `fusible:`; index spaces were \
                 not widened for it at boot"
            ));
        }
        None
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;

    fn stage(name: &str, output: Option<&str>) -> PipelineStage {
        PipelineStage {
            name:      name.to_string(),
            input:     format!("ipc:///tmp/in-{name}"),
            output:    output.map(str::to_string),
            outputs:   Vec::new(),
            wasm:      None,
            binary:    None,
            slo:       StageSlo {
                p99_ms: None, max_replicas: 1, min_replicas: 0,
                queue_up: None, queue_down: None, replica_capacity_eps: None,
                cooldown_up_s: None, cooldown_down_s: None, calm_window: None,
            },
            placement: "local".to_string(),
            fused_from: Vec::new(),
        }
    }

    fn cfg(stages: Vec<PipelineStage>, edges: Vec<(&str, &str)>) -> Config {
        let mut c: Config = serde_yaml::from_str(
            "pipeline: []\ndispatchers: []\n"
        ).unwrap();
        c.pipeline = stages;
        c.edges = edges.into_iter()
            .map(|(f, t)| EdgeSpec { from: f.into(), to: t.into() })
            .collect();
        c
    }

    /// Pre-DAG runtime.yaml: no `edges:`, no `outputs:`, just `output:`.
    /// Must normalize into exactly the linear chain it always behaved as.
    #[test]
    fn legacy_config_normalizes_to_linear_chain() {
        let mut c = cfg(
            vec![
                stage("a", Some("ipc:///tmp/b-push")),
                stage("b", Some("tcp://localhost:9999")),
            ],
            vec![],
        );
        c.normalize_topology();

        assert_eq!(c.edges, vec![EdgeSpec { from: "a".into(), to: "b".into() }]);
        assert_eq!(c.pipeline[0].outputs, vec!["ipc:///tmp/b-push"]);
        assert_eq!(c.pipeline[1].outputs, vec!["tcp://localhost:9999"]);
        // Collector backfills from the last stage's legacy output.
        assert_eq!(c.collector.as_deref(), Some("tcp://localhost:9999"));
        assert_eq!(c.in_degree("a"), 0);
        assert_eq!(c.in_degree("b"), 1);
        assert_eq!(c.sources().len(), 1);
        assert_eq!(c.sinks()[0].name, "b");
    }

    /// Explicit topology wins; array order is irrelevant.
    #[test]
    fn declared_edges_are_authoritative() {
        let mut c = cfg(
            vec![stage("b", None), stage("a", None), stage("c", None)],
            vec![("a", "b"), ("a", "c")],
        );
        c.normalize_topology();

        assert_eq!(c.sources().len(), 1);
        assert_eq!(c.sources()[0].name, "a");           // NOT pipeline[0]
        let mut sinks: Vec<&str> = c.sinks().iter().map(|s| s.name.as_str()).collect();
        sinks.sort();
        assert_eq!(sinks, vec!["b", "c"]);              // two terminal stages
        assert_eq!(c.out_degree("a"), 2);               // fan-out
        assert_eq!(c.succs("a"), vec!["b", "c"]);       // declaration order
    }

    /// Fan-in: in_degree drives the EOS barrier's expected marker count.
    #[test]
    fn fan_in_degree_counts_every_branch() {
        let mut c = cfg(
            vec![stage("src", None), stage("l", None), stage("r", None), stage("merge", None)],
            vec![("src", "l"), ("src", "r"), ("l", "merge"), ("r", "merge")],
        );
        c.normalize_topology();

        assert_eq!(c.in_degree("merge"), 2);
        let mut preds = c.preds("merge");
        preds.sort();
        assert_eq!(preds, vec!["l", "r"]);
        assert_eq!(c.sinks().len(), 1);
        assert_eq!(c.sinks()[0].name, "merge");
    }

    fn scaled(name: &str, max: usize) -> PipelineStage {
        let mut s = stage(name, None);
        s.slo.max_replicas = max;
        s
    }

    /// Fusion is edge contraction, so it is legal only on a linear chain
    /// segment. Fan-out at `a` or fan-in at `b` would change semantics
    /// (broadcast copies / merge ordering) and must be refused by NAME, not
    /// discovered during a benchmark run.
    #[test]
    fn fusion_legality_refuses_fan_out_fan_in_and_undeclared_pairs() {
        let mut c = cfg(
            vec![
                scaled("src", 1), scaled("l", 2), scaled("r", 2), scaled("merge", 3),
            ],
            vec![("src", "l"), ("src", "r"), ("l", "merge"), ("r", "merge")],
        );
        c.normalize_topology();
        c.fusible = vec![
            vec!["src".into(), "l".into()],
            vec!["l".into(), "merge".into()],
        ];

        // src fans out (out-degree 2).
        assert!(c.fusion_illegal_reason("src", "l").unwrap().contains("fan-out"));
        // merge fans in (in-degree 2).
        assert!(c.fusion_illegal_reason("l", "merge").unwrap().contains("fan-in"));
        // No such edge at all.
        assert!(c.fusion_illegal_reason("l", "r").unwrap().contains("no edge"));
    }

    /// A legal, declared pair passes; the same pair undeclared is refused,
    /// because its index spaces were never widened at boot.
    #[test]
    fn fusion_requires_a_declared_pair_and_widens_its_index_spaces() {
        let mut c = cfg(
            vec![scaled("a", 4), scaled("b", 2), scaled("c", 1)],
            vec![("a", "b"), ("b", "c")],
        );
        c.normalize_topology();

        assert!(c.fusion_illegal_reason("a", "b").unwrap().contains("not declared"));
        // Undeclared: every index space stays at its own stage's ceiling.
        assert_eq!(c.max_consumers("a"), 4);
        assert_eq!(c.max_producers("b"), 2);

        c.fusible = vec![vec!["a".into(), "b".into()]];
        assert_eq!(c.fusion_illegal_reason("a", "b"), None);
        // max_ab = 4 + 2: a fused worker holds its thread for tA + tB, so the
        // ceiling is the SUM. `a`'s in-mesh must accept 6 consumers and `b`'s
        // out-edge 6 producers, or a fused replica would have no valid index.
        assert_eq!(c.fused_max_replicas("a", "b"), Some(6));
        assert_eq!(c.max_consumers("a"), 6);
        assert_eq!(c.max_producers("b"), 6);
        // Untouched stages keep their own width — widening is not global.
        assert_eq!(c.max_consumers("b"), 2);
        assert_eq!(c.max_producers("a"), 4);
    }

    /// Normalization must not clobber explicit values on a second call.
    #[test]
    fn normalize_is_idempotent() {
        let mut c = cfg(vec![stage("a", Some("x")), stage("b", Some("y"))], vec![]);
        c.normalize_topology();
        let snapshot = (c.edges.clone(), c.pipeline[0].outputs.clone(), c.collector.clone());
        c.normalize_topology();
        assert_eq!((c.edges.clone(), c.pipeline[0].outputs.clone(), c.collector.clone()), snapshot);
    }
}

// ---------------------------------------------------------------------------
// Endpoint string helpers
// ---------------------------------------------------------------------------
//
// Pass-through today — URIs in runtime.yaml already include the `tcp://`
// or `ipc://` scheme, so there's nothing to rewrite. The wrappers stay
// so callers don't have to care whether the URI arrived clean, and so a
// future format change has one obvious hook point.

pub(crate) fn make_pull_endpoint(uri: &str) -> String {
    uri.to_string()
}

pub(crate) fn make_push_endpoint(uri: &str) -> String {
    uri.to_string()
}

/// Where Cargo puts a stage's .wasm under the legacy `functions-rs/`
/// layout. Used only when a stage doesn't declare an explicit `wasm:`
/// in runtime.yaml; the CLI always sets it explicitly, so this is a
/// fallback for hand-written YAMLs.
pub(crate) fn default_wasm_path(stage_name: &str) -> String {
    let function_name = stage_name.replace('-', "_");
    format!(
        "./functions-rs/target/wasm32-wasip2/release/{}.wasm",
        function_name
    )
}
