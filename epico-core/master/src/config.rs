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
