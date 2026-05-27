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
    pub output: String,
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
    /// Mirrors the stage's placement — the dispatcher always runs on
    /// the same node as its stage. Used by the placement filter.
    #[serde(default = "default_placement")]
    pub placement: String,
}

pub(crate) fn default_credit_window() -> u32 { 1 }

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

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Config {
    pub pipeline: Vec<PipelineStage>,
    pub dispatchers: Vec<DispatcherConfig>,
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
}

pub(crate) fn default_resource_sample_interval_ms() -> u64 {
    1000
}

pub(crate) fn default_compile_mode() -> String {
    "startup".to_string()
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
