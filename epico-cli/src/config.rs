//! Pipeline config parsing — supports two YAML formats.
//!
//! **New format** (what developers write going forward):
//! ```yaml
//! package: epico:temps@1.0.0
//! types:
//!   reading:
//!     sensor_id: string
//!     value:     f64
//! stages:
//!   - name: normalize
//!     in:  reading
//!     out: reading
//!     src: ./stages/normalize.rs
//!     scaling: { max: 4, queue_up: 50 }
//! edges:
//!   - normalize -> detect
//! deploy:
//!   collector: tcp://localhost:9999
//!   port_base: 9000
//! ```
//!
//! **Old format** (what the agent already consumes, what `dags/pipeline.yaml`
//! uses today):
//! ```yaml
//! dispatchers:
//!   - name: dispatch-a
//!     push_port: 9004
//!     pull_port: 9014
//!     ctrl_port: 9024
//! pipeline:
//!   - name: fn-a
//!     wasm: path/to/fn_a.wasm
//!     input: tcp://localhost:9014
//!     output: tcp://localhost:9002
//!     slo: { p99_ms: 100, max_replicas: 2, min_replicas: 0, ... }
//! ```
//!
//! We detect by the presence of `types:` or `stages:` at the root (new)
//! vs `pipeline:` + `dispatchers:` (old).
//!
//! Both formats parse into the same `PipelineSpec` struct. Callers don't
//! care which file format produced it.

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Internal unified representation. Everything downstream of config
/// parsing works with this type.
#[derive(Debug, Clone)]
pub struct PipelineSpec {
    pub package: String,
    /// Physical node placements. If the YAML has no `nodes:` block, this
    /// contains a single implicit node named "local" at 127.0.0.1. That
    /// collapses cleanly to today's single-host behavior while letting
    /// the rest of the pipeline reference a real named placement.
    pub nodes: Vec<NodeSpec>,
    /// Which node this agent instance represents. When single-host, every
    /// stage is placed here and nothing is filtered. When distributed,
    /// the agent only launches dispatchers and autoscalers for stages
    /// whose placement matches this value.
    pub this_host: String,
    /// Per-stage spec. Order defines default linear topology if `edges`
    /// is empty.
    pub stages: Vec<StageSpec>,
    /// Explicit topology. If empty, implied as `stages[i] -> stages[i+1]`.
    pub edges: Vec<(String, String)>,
    /// Ingress (initial stage) URI. Required in the new format — the first
    /// dispatcher binds here, and any external producer (loadgen or a
    /// user-supplied pusher) connects here. Supports `tcp://host:port` and
    /// `ipc:///path` schemes. In the old format there was no declaration for
    /// this, so we synthesize one from host + port_base for backwards
    /// compatibility; either way, downstream code can rely on a value.
    pub ingress: String,
    /// Collector (final sink) URI.
    pub collector: String,
    /// First port to allocate for dispatchers. Each dispatcher gets three
    /// consecutive ports starting from this base.
    pub port_base: u16,
    /// Hostname for TCP endpoints. Defaults to localhost. Historically
    /// this was the only "where does anything run" setting; superseded by
    /// per-stage `placement` plus the `nodes` block, but kept for legacy
    /// old-format YAML compatibility.
    pub host: String,
    /// Informational. Host stays generic (Path A), so these aren't used
    /// for code generation yet. Kept so we can later switch to Path B
    /// without changing the YAML format.
    pub types: BTreeMap<String, TypeDef>,
    /// Optional declarative source. If present, `epico run` will launch
    /// the specified event source alongside the agent (typically our own
    /// `epico-loadgen` binary). If absent, the agent is launched alone
    /// and the user is expected to push events in from a separate process.
    pub source: Option<SourceSpec>,
    /// Native source node (option A): `source: { placement, src: ./x.rs }`.
    /// Distinct from the loadgen `source: { kind: ... }` above — when this is
    /// set, the CLI compiles the `.rs` into the per-pipeline agent and the
    /// runtime drives it as an in-process `EventSource`. Mutually exclusive
    /// with the loadgen `source` in practice.
    pub source_node: Option<BoundaryNode>,
    /// Native sink node (option A): `sink: { placement, src: ./x.rs }`. Compiled
    /// into the agent and driven as an in-process `EventSink`.
    pub sink_node: Option<BoundaryNode>,
    /// Interval between agent-process resource samples (CPU + RSS), in ms.
    /// Default is 1000 ms — the cadence used in baseline evaluations. Set
    /// to 0 to disable the sampler entirely; this skips the kernel calls
    /// `sysinfo` makes on every tick and avoids the periodic scheduler
    /// jitter they introduce, at the cost of losing the resource plot.
    /// Useful when the goal is the cleanest possible tail-latency numbers
    /// and resource numbers are coming from elsewhere (e.g. a sidecar).
    pub resource_sample_interval_ms: u64,
    /// Credit window for the dispatcher↔worker protocol. Default 1
    /// reproduces legacy strict-request/reply behaviour. Larger values
    /// amortise round-trip cost across multiple in-flight events,
    /// raising per-worker throughput at the cost of weakening per-event
    /// backpressure to per-window. Reasonable values: 1 (legacy), 16
    /// (typical sweet spot), 64 (high-throughput, looser backpressure).
    pub credit_window: u32,
    /// Events per ROUTER message to a worker. Default 1. Amortises the
    /// per-message zmq poll/command overhead; orthogonal to credit_window.
    pub batch_events: usize,
    /// Transport envelope format for events. `json` is the compatible default;
    /// `binary` is reserved for the generated binary envelope path.
    pub event_format: String,
}

/// A native boundary node (source or sink) compiled into the per-pipeline
/// agent. `src` is the resolved absolute path to a `.rs` implementing the
/// `EventSource`/`EventSink` contract.
#[derive(Debug, Clone)]
pub struct BoundaryNode {
    pub placement: String,
    pub src: PathBuf,
}

/// Declarative source specification from the pipeline YAML. Today only
/// `kind: loadgen` is handled; the YAML shape is extensible so that
/// future source kinds (file replay, network listener, custom binary)
/// can be added without breaking existing pipelines.
#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub kind: String,
    /// Kind-specific free-form config. For `kind: loadgen` this maps
    /// onto `epico-loadgen`'s CLI flags (rate, duration_s, profile,
    /// sensors, ramp_start, …). Unknown keys are ignored by the CLI so
    /// adding new loadgen flags doesn't require a schema change.
    pub params: BTreeMap<String, serde_yaml::Value>,
}

/// A named physical placement. Today all stages resolve to `host:
/// 127.0.0.1` because everything is single-host, but the data model is
/// already shaped to support multi-host without schema churn.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    /// Referenced by `placement:` on stages and `this_host:` on the
    /// pipeline. Arbitrary identifier; conventionally lowercase.
    pub name: String,
    /// IP or hostname. Used when codegen needs to emit a TCP endpoint
    /// for an edge that crosses hosts.
    pub host: String,
    /// Safety valve: force TCP endpoints even for same-host edges. Only
    /// flip this on if IPC causes trouble for some reason (tmpfs full,
    /// weird permission model, debugging across namespaces). Default
    /// false — same-host edges use IPC, which is measurably faster than
    /// loopback TCP at high event rates.
    pub force_tcp: bool,
}

#[derive(Debug, Clone)]
pub struct StageSpec {
    pub name: String,
    /// Name of input type. For now only used as documentation — the host
    /// uses a single all-option<T> Event. When Path B lands, this drives
    /// per-stage WIT generation.
    pub input_type: String,
    pub output_type: String,
    /// Either a path to a .rs file (new format) or an absolute path to
    /// a prebuilt .wasm component (old format / manual workflow).
    pub source: StageSource,
    pub scaling: ScalingSpec,
    /// Which node runs this stage. Resolved at parse time: if the YAML
    /// omits the field, this is set to the first node's name (or the
    /// implicit "local" if no `nodes:` block was declared). Always
    /// populated by the time anyone downstream sees a StageSpec.
    pub placement: String,
    pub deps: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone)]
pub enum StageSource {
    /// A Rust source file the CLI will wrap into a stage crate and build.
    RustFile(PathBuf),
    /// A prebuilt wasm component. CLI does not try to compile.
    PrebuiltWasm(PathBuf),
}

#[derive(Debug, Clone)]
pub struct ScalingSpec {
    pub min_replicas: usize,
    pub max_replicas: usize,
    pub queue_up: f64,
    pub queue_down: f64,
    pub cooldown_up_s: f64,
    pub cooldown_down_s: f64,
    pub p99_ms: f64,
}

impl Default for ScalingSpec {
    fn default() -> Self {
        // Conservative defaults aimed at low-latency stream workloads.
        // The new autoscaler in master.rs reads these from its SLO block.
        Self {
            min_replicas: 0,
            max_replicas: 2,
            queue_up: 50.0,
            queue_down: 0.0,
            cooldown_up_s: 0.1,
            cooldown_down_s: 5.0,
            p99_ms: 50.0,
        }
    }
}

/// Informational type definition — today this is "here are the fields
/// this stage expects/produces" but the host doesn't enforce it yet
/// (Path A). Kept for validator hookup and future Path B migration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TypeDef {
    #[serde(flatten)]
    pub fields: BTreeMap<String, String>,
}

// ---------------------------------------------------------------------------
// Raw YAML structs — deserialize targets, one set per format.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct NewFormat {
    #[serde(default = "default_package")]
    package: String,
    #[serde(default)]
    types: BTreeMap<String, TypeDef>,
    /// Physical nodes. Omitted = implicit single "local" node. Future
    /// distributed deployments populate this to pin stages to machines.
    #[serde(default)]
    nodes: Vec<RawNode>,
    stages: Vec<NewStage>,
    #[serde(default)]
    edges: Vec<String>,
    #[serde(default)]
    deploy: DeploySpec,
    /// Optional event source launched alongside the agent. See `SourceSpec`.
    #[serde(default)]
    source: Option<RawSource>,
    /// Optional native sink node compiled into the agent. See `BoundaryNode`.
    #[serde(default)]
    sink: Option<RawBoundary>,
}

#[derive(Debug, Deserialize)]
struct RawSource {
    /// Loadgen form: `kind: loadgen`. Absent when this is a native source node.
    #[serde(default)]
    kind: Option<String>,
    /// Native-node form: `src: ./x.rs`. Absent for the loadgen form.
    #[serde(default)]
    src: Option<String>,
    #[serde(default)]
    placement: Option<String>,
    #[serde(flatten)]
    params: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct RawBoundary {
    src: String,
    #[serde(default)]
    placement: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawNode {
    name: String,
    #[serde(default = "default_host")]
    host: String,
    #[serde(default)]
    force_tcp: bool,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_package() -> String {
    "epico:pipeline@0.1.0".to_string()
}

#[derive(Debug, Deserialize)]
struct NewStage {
    name: String,
    #[serde(rename = "in", default = "default_type")]
    input_type: String,
    #[serde(rename = "out", default = "default_type")]
    output_type: String,
    src: Option<String>,
    wasm: Option<String>,
    #[serde(default)]
    scaling: NewScaling,
    #[serde(default)]
    placement: Option<String>,
    /// Extra Cargo deps. See `StageSpec::deps`. Default = empty.
    #[serde(default)]
    deps: BTreeMap<String, serde_yaml::Value>,
}

fn default_type() -> String {
    "event".to_string()
}

#[derive(Debug, Deserialize, Default)]
struct NewScaling {
    min: Option<usize>,
    max: Option<usize>,
    queue_up: Option<f64>,
    queue_down: Option<f64>,
    cooldown_up_s: Option<f64>,
    cooldown_down_s: Option<f64>,
    p99_ms: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct DeploySpec {
    collector: Option<String>,
    ingress: Option<String>,
    port_base: Option<u16>,
    host: Option<String>,
    /// Which node this agent instance represents. Used to filter stages
    /// and dispatchers at agent startup in multi-host deployments.
    /// Omitted in single-host YAMLs; defaults to the first node's name.
    this_host: Option<String>,
    /// Sampling interval for the agent's CPU/RSS resource sampler, in ms.
    /// Default 1000. Set to 0 to disable the sampler entirely (no kernel
    /// trap once per tick — useful when chasing clean tail-latency numbers).
    resource_sample_interval_ms: Option<u64>,
    /// Credit window for dispatcher↔worker flow control. Default 1.
    credit_window: Option<u32>,
    /// Events packed into one ROUTER message to a worker. Default 1 (one
    /// event per message). Larger values amortise per-message zmq overhead;
    /// orthogonal to credit_window (credits are still counted in events).
    batch_events: Option<usize>,
    /// Event envelope format. Defaults to `json`. `binary` is currently emitted
    /// as an experimental runtime knob and schema artifact, but JSON remains the
    /// only fully implemented codec.
    event_format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OldFormat {
    pipeline: Vec<OldStage>,
    dispatchers: Vec<OldDispatcher>,
    #[serde(default)]
    sinks: Vec<OldSink>,
}

#[derive(Debug, Deserialize)]
struct OldStage {
    name: String,
    #[serde(default)]
    wasm: Option<String>,
    #[serde(default)]
    input: Option<String>,
    #[serde(default)]
    output: Option<String>,
    slo: OldSlo,
}

#[derive(Debug, Deserialize)]
struct OldSlo {
    #[serde(default)]
    p99_ms: Option<f64>,
    max_replicas: usize,
    min_replicas: usize,
    #[serde(default)]
    queue_up: Option<f64>,
    #[serde(default)]
    queue_down: Option<f64>,
    #[serde(default)]
    cooldown_up_s: Option<f64>,
    #[serde(default)]
    cooldown_down_s: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OldDispatcher {
    name: String,
    push_port: u16,
    pull_port: u16,
    ctrl_port: u16,
}

#[derive(Debug, Deserialize)]
struct OldSink {
    #[allow(dead_code)]
    name: String,
    push_port: u16,
}

// ---------------------------------------------------------------------------
// Public entry point: load a pipeline file, detect format, produce spec.
// ---------------------------------------------------------------------------

pub fn load(path: &Path) -> Result<PipelineSpec> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading pipeline file {:?}", path))?;

    // First parse as untyped to decide which format. `stages:` at the top
    // level is the decisive new-format marker (old format uses `pipeline:`).
    let peek: serde_yaml::Value =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {:?} as YAML", path))?;

    let is_new = peek.get("stages").is_some() || peek.get("types").is_some();
    let yaml_dir = path.parent().unwrap_or_else(|| Path::new("."));

    if is_new {
        let raw: NewFormat = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {:?} as new-format pipeline", path))?;
        from_new_format(raw, yaml_dir)
    } else {
        let raw: OldFormat = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {:?} as old-format pipeline", path))?;
        from_old_format(raw, yaml_dir)
    }
}

// ---------------------------------------------------------------------------
// Format conversions.
// ---------------------------------------------------------------------------

fn from_new_format(raw: NewFormat, yaml_dir: &Path) -> Result<PipelineSpec> {
    if raw.stages.is_empty() {
        bail!("pipeline has no stages");
    }

    // Resolve nodes. If the YAML omits the `nodes:` block, synthesize
    // one implicit "local" node so downstream code never has to deal
    // with an empty list. Parse the declared host for old-format
    // compatibility, though it's only used for TCP endpoints.
    let nodes: Vec<NodeSpec> = if raw.nodes.is_empty() {
        vec![NodeSpec {
            name: "local".to_string(),
            host: raw
                .deploy
                .host
                .clone()
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            force_tcp: false,
        }]
    } else {
        raw.nodes
            .into_iter()
            .map(|n| NodeSpec {
                name: n.name,
                host: n.host,
                force_tcp: n.force_tcp,
            })
            .collect()
    };

    let default_placement = nodes[0].name.clone();
    let node_names: std::collections::HashSet<&str> =
        nodes.iter().map(|n| n.name.as_str()).collect();

    let stages: Result<Vec<StageSpec>> = raw
        .stages
        .into_iter()
        .map(|s| {
            // Resolve source or prebuilt wasm, resolve path relative to yaml file.
            let source = match (s.src.as_ref(), s.wasm.as_ref()) {
                (Some(_), Some(_)) => bail!(
                    "stage {} has both `src` and `wasm`; pick one",
                    s.name
                ),
                (Some(src), None) => {
                    let abs = yaml_dir.join(src);
                    if !abs.exists() {
                        bail!(
                            "stage {}: src file not found: {:?}",
                            s.name,
                            abs
                        );
                    }
                    StageSource::RustFile(abs)
                }
                (None, Some(w)) => {
                    let abs = yaml_dir.join(w);
                    StageSource::PrebuiltWasm(abs)
                }
                (None, None) => bail!(
                    "stage {} has neither `src` nor `wasm`; one is required",
                    s.name
                ),
            };

            let defaults = ScalingSpec::default();
            let scaling = ScalingSpec {
                min_replicas: s.scaling.min.unwrap_or(defaults.min_replicas),
                max_replicas: s.scaling.max.unwrap_or(defaults.max_replicas),
                queue_up: s.scaling.queue_up.unwrap_or(defaults.queue_up),
                queue_down: s.scaling.queue_down.unwrap_or(defaults.queue_down),
                cooldown_up_s: s.scaling.cooldown_up_s.unwrap_or(defaults.cooldown_up_s),
                cooldown_down_s: s.scaling.cooldown_down_s.unwrap_or(defaults.cooldown_down_s),
                p99_ms: s.scaling.p99_ms.unwrap_or(defaults.p99_ms),
            };

            // Resolve placement. Explicit values must reference a
            // declared node; missing values fall back to the first
            // node so toy pipelines "just work" without ceremony.
            let placement = match s.placement {
                Some(p) => {
                    if !node_names.contains(p.as_str()) {
                        bail!(
                            "stage {}: placement {:?} does not match any declared node",
                            s.name,
                            p
                        );
                    }
                    p
                }
                None => default_placement.clone(),
            };

            Ok(StageSpec {
                name: s.name,
                input_type: s.input_type,
                output_type: s.output_type,
                source,
                scaling,
                placement,
                deps: s.deps,
            })
        })
        .collect();
    let stages = stages?;

    // Parse edges. Format is "a -> b", whitespace-flexible.
    let edges: Result<Vec<(String, String)>> = raw
        .edges
        .iter()
        .map(|e| {
            let parts: Vec<&str> = e.split("->").map(str::trim).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                bail!("bad edge spec {:?}; expected 'from -> to'", e);
            }
            Ok((parts[0].to_string(), parts[1].to_string()))
        })
        .collect();
    let mut edges = edges?;

    // If no edges declared, imply linear topology from stage order.
    if edges.is_empty() {
        for pair in stages.windows(2) {
            edges.push((pair[0].name.clone(), pair[1].name.clone()));
        }
    }

    // Validate edges reference known stages.
    let known: std::collections::HashSet<&str> =
        stages.iter().map(|s| s.name.as_str()).collect();
    for (from, to) in &edges {
        if !known.contains(from.as_str()) {
            bail!("edge source {:?} is not a declared stage", from);
        }
        if !known.contains(to.as_str()) {
            bail!("edge target {:?} is not a declared stage", to);
        }
    }

    // Validate edge type compatibility: for every edge A -> B, the
    // declared output type of A must equal the declared input type of B.
    // If the YAML provides a `types:` block, both endpoints' types must
    // be entries in it. When no `types:` is declared we skip the
    // existence check so YAMLs that rely on the default "event" type
    // keep working unchanged.
    //
    // This is a purely syntactic (string-name) check. Structural
    // verification of the wasm components' actual WIT signatures still
    // happens later, at agent startup, in master/pipeline_validator.rs.
    // Running this check here just moves the common class of mismatch
    // to parse time — before any cargo build has been kicked off.
    let stage_by_name: std::collections::HashMap<&str, &StageSpec> =
        stages.iter().map(|s| (s.name.as_str(), s)).collect();
    let types_declared = !raw.types.is_empty();
    for (from, to) in &edges {
        // Unwraps are safe: the loop above already bailed on unknown names.
        let a = stage_by_name.get(from.as_str()).unwrap();
        let b = stage_by_name.get(to.as_str()).unwrap();

        if a.output_type != b.input_type {
            bail!(
                "edge {} -> {} type mismatch: '{}' produces `{}`, but '{}' consumes `{}`",
                from, to, from, a.output_type, to, b.input_type
            );
        }

        if types_declared {
            if !raw.types.contains_key(&a.output_type) {
                bail!(
                    "stage '{}' declares out: '{}', but no such type in `types:`",
                    a.name, a.output_type
                );
            }
            if !raw.types.contains_key(&b.input_type) {
                bail!(
                    "stage '{}' declares in: '{}', but no such type in `types:`",
                    b.name, b.input_type
                );
            }
        }
    }

    // Resolve this_host: explicit field wins, otherwise default to the
    // first node. This means a single-host YAML with no `this_host:`
    // still works — everything gets placed on the one node, and that's
    // also where `this_host` points.
    let this_host = match raw.deploy.this_host {
        Some(h) => {
            if !node_names.contains(h.as_str()) {
                bail!(
                    "this_host {:?} does not match any declared node",
                    h
                );
            }
            h
        }
        None => default_placement,
    };

    // Ingress is the declared entry URI of the pipeline. It's required in the
    // new format — the first dispatcher binds here and the loadgen (or any
    // external producer) connects here. Omitting it would leave the entry
    // endpoint implicit, which is the ambiguity we're moving away from.
    // A native source node generates events in-process, so there's no ingress
    // URI to bind — only require one otherwise.
    let has_native_source = raw.source.as_ref().map(|s| s.src.is_some()).unwrap_or(false);
    let ingress = match raw.deploy.ingress.clone() {
        Some(uri) => uri,
        None if has_native_source => "inproc://source".to_string(),
        None => {
            return Err(anyhow!(
                "deploy.ingress: is required. Declare the pipeline entry URI \
                 explicitly, e.g.\n  \
                 deploy:\n    \
                   ingress:   tcp://localhost:9100   # or ipc:///tmp/my-pipeline-push\n    \
                   collector: tcp://localhost:9999"
            ));
        }
    };

    // Source can be either the loadgen declaration (`kind: loadgen`) or a
    // native source node (`src: ./x.rs`). The presence of `src` selects the
    // native node; otherwise it's the legacy loadgen source.
    let (loadgen_source, source_node) = match raw.source {
        Some(s) => {
            if let Some(src_rel) = s.src {
                let abs = yaml_dir.join(&src_rel);
                if !abs.exists() {
                    bail!("source: src file not found: {:?}", abs);
                }
                let placement = s.placement.unwrap_or_else(|| this_host.clone());
                (None, Some(BoundaryNode { placement, src: abs }))
            } else if let Some(kind) = s.kind {
                (Some(SourceSpec { kind, params: s.params }), None)
            } else {
                bail!("source: needs either `kind: loadgen` or `src: ./file.rs`");
            }
        }
        None => (None, None),
    };
    let sink_node = match raw.sink {
        Some(b) => {
            let abs = yaml_dir.join(&b.src);
            if !abs.exists() {
                bail!("sink: src file not found: {:?}", abs);
            }
            let placement = b.placement.unwrap_or_else(|| this_host.clone());
            Some(BoundaryNode { placement, src: abs })
        }
        None => None,
    };

    let event_format = raw.deploy.event_format.unwrap_or_else(|| "json".to_string());
    match event_format.as_str() {
        "json" | "binary" | "epico-binary" => {}
        other => bail!("deploy.event_format {:?} is invalid; expected `json` or `binary`", other),
    }

    Ok(PipelineSpec {
        package: raw.package,
        nodes,
        this_host,
        stages,
        edges,
        ingress,
        collector: raw
            .deploy
            .collector
            .unwrap_or_else(|| "tcp://localhost:9999".to_string()),
        port_base: raw.deploy.port_base.unwrap_or(9000),
        host: raw.deploy.host.unwrap_or_else(|| "localhost".to_string()),
        types: raw.types,
        source: loadgen_source,
        source_node,
        sink_node,
        resource_sample_interval_ms: raw.deploy.resource_sample_interval_ms.unwrap_or(1000),
        credit_window: raw.deploy.credit_window.unwrap_or(1),
        batch_events: raw.deploy.batch_events.unwrap_or(1),
        event_format,
    })
}

fn from_old_format(raw: OldFormat, yaml_dir: &Path) -> Result<PipelineSpec> {
    if raw.pipeline.is_empty() {
        bail!("pipeline has no stages");
    }

    // Old-format YAMLs predate multi-host placement. Synthesize a single
    // implicit "local" node and pin every stage to it. Behavior stays
    // identical to before the placement field existed.
    let implicit_placement = "local".to_string();

    // Old format requires a wasm path per stage (we don't build from src).
    let stages: Result<Vec<StageSpec>> = raw
        .pipeline
        .into_iter()
        .map(|s| {
            let wasm = s
                .wasm
                .ok_or_else(|| anyhow!("old-format stage {} missing `wasm` field", s.name))?;
            let abs = yaml_dir.join(&wasm);
            let scaling = ScalingSpec {
                min_replicas: s.slo.min_replicas,
                max_replicas: s.slo.max_replicas,
                p99_ms: s.slo.p99_ms.unwrap_or(50.0),
                queue_up: s.slo.queue_up.unwrap_or(50.0),
                queue_down: s.slo.queue_down.unwrap_or(0.0),
                cooldown_up_s: s.slo.cooldown_up_s.unwrap_or(0.1),
                cooldown_down_s: s.slo.cooldown_down_s.unwrap_or(5.0),
            };
            Ok(StageSpec {
                name: s.name,
                input_type: "event".to_string(),
                output_type: "event".to_string(),
                source: StageSource::PrebuiltWasm(abs),
                scaling,
                placement: implicit_placement.clone(),
                deps: BTreeMap::new(),
            })
        })
        .collect();
    let stages = stages?;

    // Linear topology from stage order.
    let edges: Vec<(String, String)> = stages
        .windows(2)
        .map(|w| (w[0].name.clone(), w[1].name.clone()))
        .collect();

    // Collector: look in sinks block; fallback to last stage's output if
    // explicit; fallback to default.
    let collector = raw
        .sinks
        .first()
        .map(|s| format!("tcp://localhost:{}", s.push_port))
        .unwrap_or_else(|| "tcp://localhost:9999".to_string());

    // port_base for old format: use the lowest ctrl_port as base. This
    // is informational only; we keep the explicit dispatcher ports when
    // we emit runtime.yaml later.
    let port_base = raw
        .dispatchers
        .iter()
        .map(|d| d.ctrl_port.min(d.push_port).min(d.pull_port))
        .min()
        .unwrap_or(9000);

    // Synthesize an ingress URI for old-format YAMLs. The old format never
    // declared an explicit entry point; it implicitly relied on connecting
    // to the first dispatcher's push port on localhost. We materialize that
    // convention so downstream code (codegen, loadgen) can treat ingress as
    // always-present without special-casing the format.
    let ingress = match raw.dispatchers.first() {
        Some(d) => format!("tcp://localhost:{}", d.push_port),
        None => format!("tcp://localhost:{}", port_base),
    };

    Ok(PipelineSpec {
        package: "epico:legacy@0.1.0".to_string(),
        nodes: vec![NodeSpec {
            name: implicit_placement.clone(),
            host: "127.0.0.1".to_string(),
            force_tcp: false,
        }],
        this_host: implicit_placement,
        stages,
        edges,
        ingress,
        collector,
        port_base,
        host: "localhost".to_string(),
        types: BTreeMap::new(),
        source: None,
        source_node: None,
        sink_node: None,
        // Old-format YAMLs predate this knob; keep the historical 1 Hz cadence
        // so existing legacy pipelines produce identical resource plots.
        resource_sample_interval_ms: 1000,
        // Old-format YAMLs predate flow control too; keep the strict
        // request/reply protocol that they were designed against.
        credit_window: 1,
        // Old-format YAMLs predate batching; one event per message.
        batch_events: 1,
        event_format: "json".to_string(),
    })
}
