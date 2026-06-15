//! Codegen: turns a PipelineSpec into a filesystem tree that cargo can
//! build and the existing agent can consume.
//!
//! Layout produced at `target/epico/`:
//!
//!   target/epico/
//!   ├── Cargo.toml                   (workspace)
//!   ├── wit/epico.wit              (copied from epico-sdk/wit/)
//!   ├── stages/
//!   │   ├── <stage1>/
//!   │   │   ├── Cargo.toml
//!   │   │   ├── wit/world.wit        (per-stage world naming)
//!   │   │   └── src/lib.rs           (includes user's .rs)
//!   │   └── ...
//!   └── runtime.yaml                 (what master consumes)
//!
//! Three kinds of stages coexist:
//!   - src pointing to .rs: full scaffold generated, cargo builds the .wasm
//!   - prebuilt wasm: no scaffold needed, just referenced in runtime.yaml
//!   - prebuilt wasm is also what old-format pipelines always produce

use crate::config::{NodeSpec, PipelineSpec, StageSource, StageSpec};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Result of codegen: paths the caller needs to build and run.
pub struct CodegenOutput {
    /// Path to the generated workspace Cargo.toml. Pass to `cargo build`.
    pub workspace_manifest: PathBuf,
    /// Path to the generated runtime.yaml. Pass to the agent.
    pub runtime_yaml: PathBuf,
    /// Map of stage name -> absolute path of the .wasm the runtime should load.
    /// For src-based stages this is inside target/ (filled in after cargo build);
    /// for prebuilt stages it's whatever the user provided.
    pub wasm_by_stage: Vec<(String, PathBuf)>,
    /// Whether any stage needs compilation. If false, caller can skip cargo.
    pub needs_build: bool,
    /// If the pipeline declares a native `source:`/`sink:`, the path to the
    /// generated per-pipeline agent crate's Cargo.toml. `cargo build` this for
    /// the host target and launch its binary instead of stock `master`.
    pub agent_manifest: Option<PathBuf>,
}

/// The sdk_path is the absolute or relative path from target/epico/ to
/// the epico-sdk crate, used in generated Cargo.tomls.
pub fn generate(
    spec: &PipelineSpec,
    project_root: &Path,
    output_dir: &Path,
    compile_mode: Option<&str>,
) -> Result<CodegenOutput> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating codegen dir {:?}", output_dir))?;

    // Copy the canonical WIT file into output/wit/. Per-stage crates then
    // copy THIS one (we always want stages to match what the host loaded).
    //
    // The canonical WIT lives under epico-sdk/wit/epico.wit — the SDK
    // crate owns it so there's one source of truth. The old repo-root
    // wit/epico.wit location is supported as a fallback for projects
    // that haven't migrated yet.
    let wit_src = {
        let sdk_path = project_root.join("epico-sdk").join("wit").join("epico.wit");
        let root_path = project_root.join("wit").join("epico.wit");
        if sdk_path.exists() {
            sdk_path
        } else if root_path.exists() {
            root_path
        } else {
            anyhow::bail!(
                "canonical WIT not found. Expected one of:\n  {:?}\n  {:?}\n\
                 Is this a epico project root?",
                sdk_path, root_path
            );
        }
    };
    let wit_dst_dir = output_dir.join("wit");
    std::fs::create_dir_all(&wit_dst_dir)?;
    std::fs::copy(&wit_src, wit_dst_dir.join("epico.wit"))
        .context("copying epico.wit to output dir")?;

    // Scaffold each src-based stage, collect workspace members.
    let mut workspace_members: Vec<String> = Vec::new();
    let mut wasm_by_stage: Vec<(String, PathBuf)> = Vec::new();
    let mut needs_build = false;

    let stages_root = output_dir.join("stages");
    std::fs::create_dir_all(&stages_root)?;

    for stage in &spec.stages {
        match &stage.source {
            StageSource::RustFile(src_rs) => {
                let crate_name = stage_crate_name(&stage.name);
                let crate_dir = stages_root.join(&crate_name);
                scaffold_rust_stage(&crate_dir, stage, src_rs, &wit_src, project_root, &spec.types)
                    .with_context(|| format!("scaffolding stage {}", stage.name))?;

                let member_rel = format!("stages/{}", crate_name);
                workspace_members.push(member_rel);

                // Where cargo will produce the wasm. After `cargo build`
                // this path will exist; before, the file is absent but
                // the runtime.yaml still points here.
                let wasm_path = output_dir
                    .join("target")
                    .join("wasm32-wasip2")
                    .join("release")
                    .join(format!("{}.wasm", crate_name));
                wasm_by_stage.push((stage.name.clone(), wasm_path));
                needs_build = true;
            }
            StageSource::PrebuiltWasm(abs_path) => {
                // Nothing to generate — just point runtime.yaml at the file.
                wasm_by_stage.push((stage.name.clone(), abs_path.clone()));
            }
        }
    }

    // Emit workspace Cargo.toml only if we have Rust stages to build.
    let workspace_manifest = output_dir.join("Cargo.toml");
    if needs_build {
        write_workspace_cargo(&workspace_manifest, &workspace_members, project_root)?;
    }

    // Emit runtime.yaml the agent can consume.
    let runtime_yaml_path = output_dir.join("runtime.yaml");
    write_runtime_yaml(&runtime_yaml_path, spec, &wasm_by_stage, compile_mode)?;
    write_binary_schema_artifact(output_dir, spec)?;

    // The per-pipeline agent crate is now ALWAYS scaffolded: besides linking
    // native source/sink (option A), it carries the generated typed-dispatch
    // module (concrete host-side types per YAML record), which is what lets
    // workers bypass the per-event Val layer. Legacy all-optional `event`
    // pipelines still build an agent, just with an empty typed registry.
    let agent_manifest = {
        let agent_dir = output_dir.join("agent");
        scaffold_agent_crate(
            &agent_dir,
            spec.source_node.as_ref(),
            spec.sink_node.as_ref(),
            project_root,
            &spec.stages,
            &spec.types,
        )
        .context("scaffolding per-pipeline agent crate")?;
        Some(agent_dir.join("Cargo.toml"))
    };

    Ok(CodegenOutput {
        workspace_manifest,
        runtime_yaml: runtime_yaml_path,
        wasm_by_stage,
        needs_build,
        agent_manifest,
    })
}

/// Generate the per-pipeline agent crate: a standalone host binary that links
/// the user's native source/sink and `epico_master`, then calls `run_agent`.
/// Built for the host target (not wasm), separate from the stage workspace.
fn scaffold_agent_crate(
    agent_dir: &Path,
    source_node: Option<&crate::config::BoundaryNode>,
    sink_node: Option<&crate::config::BoundaryNode>,
    project_root: &Path,
    stages: &[StageSpec],
    types: &std::collections::BTreeMap<String, crate::config::TypeDef>,
) -> Result<()> {
    let src_dir = agent_dir.join("src");
    std::fs::create_dir_all(&src_dir)?;

    let master_dir = project_root.join("epico-core").join("master");
    let master_dir = std::fs::canonicalize(&master_dir).unwrap_or(master_dir);

    // Cargo.toml — depends on the agent library by path, plus serde for the
    // user's source/sink event (de)serialization. No clap: the binary uses
    // epico_master::parse_args().
    let cargo = format!(
        r#"[package]
name = "epico-agent"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "epico-agent"
path = "src/main.rs"

[dependencies]
epico_master = {{ path = "{master}", package = "master" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
# Typed dispatch: the generated structs derive wasmtime component traits.
# MUST stay semver-compatible with master's wasmtime so cargo unifies them
# into one copy (foreign-crate trait impls would not typecheck otherwise).
wasmtime = {{ version = "26", features = ["component-model"] }}

# Standalone crate: it lives under target/epico/ next to the generated stage
# workspace, so declare an empty workspace table to keep Cargo from treating it
# as a member of that workspace.
[workspace]
"#,
        master = master_dir.display().to_string().replace('\\', "/"),
    );
    std::fs::write(agent_dir.join("Cargo.toml"), cargo).context("writing agent Cargo.toml")?;

    // Copy the user's source/sink modules verbatim.
    if let Some(s) = source_node {
        std::fs::copy(&s.src, src_dir.join("source.rs"))
            .with_context(|| format!("copying source src {:?}", s.src))?;
    }
    if let Some(s) = sink_node {
        std::fs::copy(&s.src, src_dir.join("sink.rs"))
            .with_context(|| format!("copying sink src {:?}", s.src))?;
    }

    // Generate main.rs wiring the modules into run_agent. A missing source/sink
    // is passed as None, so the runtime falls back (PULL ingress / collector).
    let (src_mod, src_expr) = match source_node {
        Some(_) => (
            "mod source;\n",
            "Some(Box::new(|| -> Box<dyn epico_master::EventSource> { \
             Box::new(source::Source::new()) }))",
        ),
        None    => ("", "None"),
    };
    let (sink_mod, sink_expr) = match sink_node {
        Some(_) => ("mod sink;\n", "Some(Box::new(sink::Sink::new()))"),
        None    => ("", "None"),
    };
    let main_rs = format!(
        r#"//! Generated per-pipeline agent. Do not edit — regenerated by `epico build`.
//!
//! Links the pipeline's native source/sink and typed dispatch into the agent
//! library and runs it.
{src_mod}{sink_mod}mod typed_dispatch;

fn main() {{
    epico_master::typed::register(typed_dispatch::registry());
    epico_master::run_agent(
        epico_master::parse_args(),
        {src_expr},
        {sink_expr},
    );
}}
"#,
    );
    std::fs::write(src_dir.join("main.rs"), main_rs).context("writing agent main.rs")?;

    // Generated typed-dispatch module: concrete structs per YAML record +
    // FromEvent/ToEvent impls + per-stage registry.
    let typed_rs = emit_typed_dispatch(stages, types);
    std::fs::write(src_dir.join("typed_dispatch.rs"), typed_rs)
        .context("writing agent typed_dispatch.rs")?;

    Ok(())
}

/// Cargo-compatible crate name derived from stage name. Rust crate names
/// can't contain dashes in the name field — well, they can, but they
/// resolve to underscores in `extern crate`. Be explicit: force snake_case.
fn stage_crate_name(stage_name: &str) -> String {
    let mut out = String::with_capacity(stage_name.len() + 6);
    out.push_str("stage_");
    for ch in stage_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out
}

/// Generate one stage crate: Cargo.toml + wit/world.wit + src/lib.rs.
fn scaffold_rust_stage(
    crate_dir: &Path,
    stage: &StageSpec,
    user_rs: &Path,
    wit_src: &Path,
    project_root: &Path,
    types: &std::collections::BTreeMap<String, crate::config::TypeDef>,
) -> Result<()> {
    std::fs::create_dir_all(crate_dir.join("src"))?;
    std::fs::create_dir_all(crate_dir.join("wit"))?;

    // Write a per-stage WIT.
    //
    // wit-bindgen 0.34's generate! macro refuses to build when a WIT file
    // contains multiple worlds unless a `world: "..."` option is passed.
    // The shared wit/epico.wit declares fn-a/fn-b/fn-c worlds (kept for
    // the host-side binding which uses fn-a). For the guest, we want
    // exactly one world. Simplest: extract the `package` / `interface types`
    // / `interface process` sections from the shared WIT and append a
    // Write a per-stage WIT with real typed records from the pipeline's
    // types: block. Falls back to the shared all-optional WIT for legacy.
    let shared = std::fs::read_to_string(wit_src)
        .with_context(|| format!("reading shared WIT {:?}", wit_src))?;
    let stage_wit = make_typed_stage_wit(stage, types, &shared)?;
    std::fs::write(crate_dir.join("wit").join("world.wit"), stage_wit)
        .context("writing per-stage world.wit")?;

    // Cargo.toml for the stage crate.
    // - cdylib so it produces a .wasm file (wit-bindgen + wasm32-wasip2
    //   target produces a component automatically).
    // - wit-bindgen for the guest-side type generation.
    // - epico-sdk for the stage! macro.
    // - any extra crates the user declared under `deps:` for this stage.
    let sdk_path = relative_path(crate_dir, &project_root.join("epico-sdk"));
    let crate_name = stage_crate_name(&stage.name);

    let extra_deps = render_extra_deps(&stage.deps)
        .with_context(|| format!("rendering deps for stage {}", stage.name))?;

    let cargo_toml = format!(
        r#"# GENERATED by epico-cli. Do not edit directly — edit the source .rs
# or the pipeline.yaml and re-run `epico build`.
[package]
name = "{crate_name}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.34"
epico-sdk = {{ path = "{sdk_path}" }}
{extra_deps}"#,
        crate_name = crate_name,
        sdk_path = sdk_path.display(),
        extra_deps = extra_deps,
    );
    std::fs::write(crate_dir.join("Cargo.toml"), cargo_toml)?;

    // src/lib.rs: include the user's .rs file verbatim. The user's file
    // invokes `epico_sdk::stage! { ... }` which expands to the full
    // component glue. The `include!` keeps the user's editing surface
    // at their original path (no copy-back synchronization).
    //
    // Use the absolute path so cargo never has to reason about how deep
    // in the target/epico/stages/<name>/src/ tree we are.
    let user_rs_abs = user_rs
        .canonicalize()
        .with_context(|| format!("resolving stage source {:?}", user_rs))?;
    let lib_rs = format!(
        r#"// GENERATED by epico-cli. Do not edit directly.
// The real source lives at: {user_rs_display}

include!("{user_rs_abs}");
"#,
        user_rs_display = user_rs.display(),
        user_rs_abs = user_rs_abs.display(),
    );
    std::fs::write(crate_dir.join("src").join("lib.rs"), lib_rs)?;

    Ok(())
}

/// Render the user's per-stage `deps:` YAML map as Cargo `[dependencies]`
/// lines. Each entry becomes one line of the form `name = <toml-value>`.
///
/// Supported value shapes (which is the subset Cargo actually accepts):
///   - YAML string                   → `"version"` (quoted TOML string)
///   - YAML mapping {k: v, ...}      → inline TOML table `{ k = v, ... }`
///   - YAML scalars inside a mapping: string / bool / int / float
///
/// Anything else (sequences, nulls, nested tables) is rejected with a
/// clear error rather than emitted as garbage TOML that cargo would
/// reject with a less useful message.
fn render_extra_deps(
    deps: &std::collections::BTreeMap<String, serde_yaml::Value>,
) -> Result<String> {
    if deps.is_empty() {
        return Ok(String::new());
    }
    let mut out = String::new();
    for (name, value) in deps {
        let rendered = yaml_to_toml_value(value)
            .with_context(|| format!("dep {:?}", name))?;
        out.push_str(&format!("{} = {}\n", name, rendered));
    }
    Ok(out)
}

/// Convert one YAML value into its TOML text representation. Intentionally
/// limited to the shapes Cargo accepts in `[dependencies]` entries.
fn yaml_to_toml_value(v: &serde_yaml::Value) -> Result<String> {
    use serde_yaml::Value;
    match v {
        Value::String(s) => Ok(format!("\"{}\"", escape_toml_string(s))),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Mapping(m) => {
            // Inline TOML table: { k = v, k = v }
            let mut parts = Vec::with_capacity(m.len());
            for (k, val) in m {
                let k_str = k.as_str().ok_or_else(|| {
                    anyhow::anyhow!("dep table key must be a string, got {:?}", k)
                })?;
                // Inside an inline table we only support scalars and
                // (recursively) inline tables. Sequences in Cargo are rare
                // (features = [...] uses one, but features-as-array is a
                // valid use case worth supporting).
                let val_str = yaml_to_inline_toml(val)
                    .with_context(|| format!("key {:?}", k_str))?;
                parts.push(format!("{} = {}", k_str, val_str));
            }
            Ok(format!("{{ {} }}", parts.join(", ")))
        }
        Value::Null => anyhow::bail!(
            "null is not a valid dependency spec; use a version string \
             like \"1.0\" or an inline table like {{ version = \"1.0\" }}"
        ),
        Value::Sequence(_) => anyhow::bail!(
            "a top-level sequence is not a valid dependency spec; \
             use an inline table {{ version = \"...\", features = [...] }}"
        ),
        Value::Tagged(t) => yaml_to_toml_value(&t.value),
    }
}

/// Like `yaml_to_toml_value`, but additionally accepts sequences (for
/// `features = ["..."]` etc.). Used for values inside an inline table.
fn yaml_to_inline_toml(v: &serde_yaml::Value) -> Result<String> {
    use serde_yaml::Value;
    match v {
        Value::Sequence(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for it in items {
                parts.push(yaml_to_inline_toml(it)?);
            }
            Ok(format!("[{}]", parts.join(", ")))
        }
        _ => yaml_to_toml_value(v),
    }
}

/// Minimal TOML string escaper: backslashes, double quotes, control chars.
/// Dependency-spec strings are short and almost always plain ASCII version
/// requirements or paths, so this doesn't need to be exhaustive.
fn escape_toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Generate a per-stage WIT file from the pipeline's `types:` block.
///
/// If the stage has `in: reading, out: enriched` and the types block
/// defines both, the WIT declares concrete records for `reading` and
/// `enriched` with real fields and a `process-event` function that
/// takes the input type and returns the output type.
///
/// If the `types:` block is empty (legacy pipeline), falls back to the
/// shared WIT with the all-optional `event` record.
fn make_typed_stage_wit(
    stage: &StageSpec,
    types: &std::collections::BTreeMap<String, crate::config::TypeDef>,
    shared_wit: &str,
) -> Result<String> {
    // Fallback: if no types defined or both in/out are "event", use the
    // shared WIT's all-optional event record (Path A compatibility).
    if types.is_empty()
        || (stage.input_type == "event" && stage.output_type == "event")
    {
        return Ok(make_stage_wit_legacy(shared_wit));
    }

    let in_type = types.get(&stage.input_type).ok_or_else(|| {
        anyhow::anyhow!(
            "stage {:?} input type {:?} not found in types: block",
            stage.name,
            stage.input_type
        )
    })?;
    let out_type = types.get(&stage.output_type).ok_or_else(|| {
        anyhow::anyhow!(
            "stage {:?} output type {:?} not found in types: block",
            stage.name,
            stage.output_type
        )
    })?;

    let in_wit_name = to_wit_ident(&stage.input_type);
    let out_wit_name = to_wit_ident(&stage.output_type);

    let mut wit = String::with_capacity(2048);
    wit.push_str("// GENERATED per-stage typed WIT. Do not edit.\n");
    wit.push_str("package epico:pipeline@0.1.0;\n\n");

    wit.push_str("interface types {\n");

    // Emit input record.
    emit_record(&mut wit, &in_wit_name, in_type);

    // Emit output record only if it differs from input.
    if stage.input_type != stage.output_type {
        emit_record(&mut wit, &out_wit_name, out_type);
    }

    // Bench-ctx is always the same — infrastructure, not user-defined.
    wit.push_str(
        "    record bench-ctx {\n\
         \x20       bench-ts-wall: option<f64>,\n\
         \x20       bench-ts:      option<f64>,\n\
         \x20       bench-seq:     option<u64>,\n\
         \x20       bench-hops:    list<bench-hop>,\n\
         \x20   }\n\
         \x20   record bench-hop {\n\
         \x20       stage:    string,\n\
         \x20       enter-ts: f64,\n\
         \x20       exit-ts:  f64,\n\
         \x20   }\n",
    );
    wit.push_str("}\n\n");

    // Process interface — uses the concrete types.
    wit.push_str("interface process {\n");
    wit.push_str(&format!(
        "    use types.{{{in_wit}, {out_wit}, bench-ctx}};\n",
        in_wit = in_wit_name,
        out_wit = if stage.input_type == stage.output_type {
            // Same type for in and out — only one use needed.
            "bench-ctx".to_string() // already in the use, skip duplicate
        } else {
            out_wit_name.clone()
        },
    ));
    // Fix the use statement to always include exactly the right set.
    // Rebuild it cleanly:
    wit.clear();
    wit.push_str("// GENERATED per-stage typed WIT. Do not edit.\n");
    wit.push_str("package epico:pipeline@0.1.0;\n\n");
    wit.push_str("interface types {\n");
    emit_record(&mut wit, &in_wit_name, in_type);
    if stage.input_type != stage.output_type {
        emit_record(&mut wit, &out_wit_name, out_type);
    }
    wit.push_str(
        "    record bench-ctx {\n\
         \x20       bench-ts-wall: option<f64>,\n\
         \x20       bench-ts:      option<f64>,\n\
         \x20       bench-seq:     option<u64>,\n\
         \x20       bench-hops:    list<bench-hop>,\n\
         \x20   }\n\
         \x20   record bench-hop {\n\
         \x20       stage:    string,\n\
         \x20       enter-ts: f64,\n\
         \x20       exit-ts:  f64,\n\
         \x20   }\n",
    );
    wit.push_str("}\n\n");

    wit.push_str("interface process {\n");
    if stage.input_type == stage.output_type {
        wit.push_str(&format!(
            "    use types.{{{}, bench-ctx}};\n",
            in_wit_name
        ));
        wit.push_str(&format!(
            "    process-event: func(ev: {0}, bench: bench-ctx) -> tuple<{0}, bench-ctx>;\n",
            in_wit_name
        ));
    } else {
        wit.push_str(&format!(
            "    use types.{{{}, {}, bench-ctx}};\n",
            in_wit_name, out_wit_name
        ));
        wit.push_str(&format!(
            "    process-event: func(ev: {}, bench: bench-ctx) -> tuple<{}, bench-ctx>;\n",
            in_wit_name, out_wit_name
        ));
    }
    wit.push_str("}\n\n");

    wit.push_str("world stage {\n    export process;\n}\n");

    Ok(wit)
}

/// Emit the agent's typed_dispatch.rs: one struct per YAML record type
/// (deriving the wasmtime component traits against the SAME generated WIT —
/// field iteration order is the TypeDef BTreeMap's alphabetical order, which
/// is exactly what emit_record writes), FromEvent/ToEvent impls, and a
/// registry mapping each stage to TypedStage<In, Out>.
///
/// Stages on the legacy all-optional `event` record are NOT registered
/// (mirrors make_typed_stage_wit's fallback); they keep the dynamic path.
fn emit_typed_dispatch(
    stages: &[StageSpec],
    types: &std::collections::BTreeMap<String, crate::config::TypeDef>,
) -> String {
    use std::collections::BTreeSet;

    let legacy = |st: &StageSpec| {
        types.is_empty() || (st.input_type == "event" && st.output_type == "event")
    };

    // Which types do non-legacy stages actually use?
    let mut used: BTreeSet<&str> = BTreeSet::new();
    for st in stages {
        if legacy(st) {
            continue;
        }
        if types.contains_key(&st.input_type) && types.contains_key(&st.output_type) {
            used.insert(st.input_type.as_str());
            used.insert(st.output_type.as_str());
        }
    }

    let mut out = String::with_capacity(4096);
    out.push_str(
        "// GENERATED by epico-cli. Do not edit — regenerated by `epico build`.\n\
         //\n\
         // Typed dispatch: concrete host-side mirrors of this pipeline's WIT\n\
         // records. Field order matches the generated WIT (TypeDef BTreeMap\n\
         // order); component names are the kebab-case WIT field names.\n\n\
         #![allow(dead_code)]\n\n\
         use std::collections::HashMap;\n\
         use std::sync::Arc;\n\
         use epico_master::typed::{\n\
             EventView, FromEvent, ToEvent, TypedDispatch, TypedStage, WireValue,\n\
         };\n\
         use wasmtime::component::{ComponentType, Lift, Lower};\n\n",
    );

    for ty_name in &used {
        let td = &types[*ty_name];
        let struct_name = camel_case(ty_name);

        out.push_str(&format!(
            "#[derive(ComponentType, Lift, Lower, Clone, Debug)]\n\
             #[component(record)]\n\
             pub struct {struct_name} {{\n"
        ));
        for (fname, ftype) in &td.fields {
            let (rust_ty, _optional, _kind) = yaml_type_to_rust(ftype);
            let wit_name = to_wit_ident(fname);
            if wit_name != *fname {
                out.push_str(&format!("    #[component(name = \"{wit_name}\")]\n"));
            }
            out.push_str(&format!("    pub {fname}: {rust_ty},\n"));
        }
        out.push_str("}\n\n");

        // FromEvent
        out.push_str(&format!(
            "impl FromEvent for {struct_name} {{\n\
             \x20   fn from_event(v: &dyn EventView) -> Self {{\n\
             \x20       Self {{\n"
        ));
        for (fname, ftype) in &td.fields {
            let (_rust_ty, optional, kind) = yaml_type_to_rust(ftype);
            let getter = format!("v.{kind}_field(\"{fname}\")");
            if optional {
                out.push_str(&format!("            {fname}: {getter},\n"));
            } else {
                out.push_str(&format!("            {fname}: {getter}.unwrap_or_default(),\n"));
            }
        }
        out.push_str("        }\n    }\n}\n\n");

        // ToEvent
        out.push_str(&format!(
            "impl ToEvent for {struct_name} {{\n\
             \x20   fn to_fields(&self, out: &mut Vec<(&'static str, WireValue)>) {{\n"
        ));
        for (fname, ftype) in &td.fields {
            let (_rust_ty, optional, kind) = yaml_type_to_rust(ftype);
            let variant = wire_variant(kind);
            let expr = if optional {
                let inner = if kind == "str" {
                    format!("WireValue::{variant}(x.clone())")
                } else {
                    format!("WireValue::{variant}(*x)")
                };
                format!(
                    "match &self.{fname} {{ Some(x) => {inner}, None => WireValue::Absent }}"
                )
            } else if kind == "str" {
                format!("WireValue::{variant}(self.{fname}.clone())")
            } else {
                format!("WireValue::{variant}(self.{fname})")
            };
            out.push_str(&format!("        out.push((\"{fname}\", {expr}));\n"));
        }
        out.push_str("    }\n}\n\n");
    }

    // Registry
    out.push_str(
        "pub fn registry() -> HashMap<String, Arc<dyn TypedDispatch>> {\n\
         \x20   let mut m: HashMap<String, Arc<dyn TypedDispatch>> = HashMap::new();\n",
    );
    for st in stages {
        if legacy(st)
            || !types.contains_key(&st.input_type)
            || !types.contains_key(&st.output_type)
        {
            continue;
        }
        let in_s = camel_case(&st.input_type);
        let out_s = camel_case(&st.output_type);
        out.push_str(&format!(
            "    m.insert(\"{name}\".to_string(), \
             Arc::new(TypedStage::<{in_s}, {out_s}>::new()));\n",
            name = st.name,
        ));
    }
    out.push_str("    m\n}\n");
    out
}

/// YAML field type -> (rust type, is_option, accessor kind for EventView).
fn yaml_type_to_rust(s: &str) -> (String, bool, &'static str) {
    let (base, optional) = match s.strip_suffix('?') {
        Some(inner) => (inner, true),
        None => (s, false),
    };
    let (rust, kind) = match base {
        "string" | "str" => ("String", "str"),
        "f64" | "float" | "double" => ("f64", "f64"),
        "f32" | "float32" => ("f32", "f32"),
        "u64" | "uint64" => ("u64", "u64"),
        "u32" | "uint32" | "uint" => ("u32", "u32"),
        "s64" | "int64" | "int" => ("i64", "s64"),
        "s32" | "int32" => ("i32", "s32"),
        "bool" | "boolean" => ("bool", "bool"),
        other => (other, "str"), // mirrors yaml_type_to_wit's passthrough
    };
    let rust_ty = if optional {
        format!("Option<{rust}>")
    } else {
        rust.to_string()
    };
    (rust_ty, optional, kind)
}

fn wire_variant(kind: &str) -> &'static str {
    match kind {
        "str" => "Str",
        "f64" => "F64",
        "f32" => "F32",
        "u64" => "U64",
        "u32" => "U32",
        "s64" => "S64",
        "s32" => "S32",
        _ => "Bool",
    }
}

fn camel_case(s: &str) -> String {
    s.split(['_', '-'])
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut c = p.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Emit a WIT record definition from a TypeDef.
fn emit_record(wit: &mut String, name: &str, typedef: &crate::config::TypeDef) {
    wit.push_str(&format!("    record {} {{\n", name));
    for (field_name, field_type) in &typedef.fields {
        let wit_field = to_wit_ident(field_name);
        let wit_type = yaml_type_to_wit(field_type);
        wit.push_str(&format!("        {}: {},\n", wit_field, wit_type));
    }
    wit.push_str("    }\n");
}

/// Convert a YAML type string to a WIT type.
/// Supported: string, f64, f32, u64, u32, s64, s32, bool.
/// Trailing `?` means `option<T>`.
fn yaml_type_to_wit(s: &str) -> String {
    let (base, optional) = if let Some(inner) = s.strip_suffix('?') {
        (inner, true)
    } else {
        (s.as_ref(), false)
    };
    let wit_base = match base {
        "string" | "str" => "string",
        "f64" | "float" | "double" => "f64",
        "f32" | "float32" => "f32",
        "u64" | "uint64" => "u64",
        "u32" | "uint32" | "uint" => "u32",
        "s64" | "int64" | "int" => "s64",
        "s32" | "int32" => "s32",
        "bool" | "boolean" => "bool",
        other => other, // pass through for user-defined types
    };
    if optional {
        format!("option<{}>", wit_base)
    } else {
        wit_base.to_string()
    }
}

/// Convert a user-facing name to a WIT identifier.
/// WIT uses kebab-case; underscores → dashes.
fn to_wit_ident(s: &str) -> String {
    s.replace('_', "-")
}

/// Legacy fallback: strip multi-world declarations from shared WIT,
/// add a single `world stage { export process; }`.
fn make_stage_wit_legacy(shared: &str) -> String {
    let mut cut = shared.len();
    for (i, line) in shared.lines().enumerate() {
        if line.trim_start().starts_with("world ") {
            cut = shared
                .lines()
                .take(i)
                .map(|l| l.len() + 1)
                .sum();
            break;
        }
    }
    let mut out = String::with_capacity(shared.len() + 128);
    out.push_str(&shared[..cut]);
    out.push_str(
        "\nworld stage {\n    export process;\n}\n",
    );
    out
}

/// Workspace Cargo.toml listing all stage crates. We also exclude the
/// `epico-sdk` crate from the workspace — it lives at the project
/// root and has its own compilation target (host-side, not wasm).
/// `resolver = "2"` is required for workspace-level target-specific
/// deps to work with wasm targets.
fn write_workspace_cargo(path: &Path, members: &[String], project_root: &Path) -> Result<()> {
    let mut toml = String::from(
        "# GENERATED by epico-cli. Workspace for all wasm stage crates.\n\
         [workspace]\n\
         resolver = \"2\"\n\
         members = [\n",
    );
    for m in members {
        toml.push_str(&format!("    \"{}\",\n", m));
    }
    toml.push_str("]\n\n");

    // profile.release: aggressive optimization since these ship as binaries.
    toml.push_str(
        "[profile.release]\n\
         opt-level = 3\n\
         lto = true\n\
         codegen-units = 1\n\
         strip = true\n",
    );

    // Suppress clippy warnings in this generated workspace. Not our code.
    let _ = project_root; // reserved for future path-based config
    std::fs::write(path, toml)?;
    Ok(())
}

/// Parse a TCP port number out of a `tcp://host:PORT` URI.
/// Returns `None` for IPC URIs or anything that doesn't match the pattern.
fn port_from_tcp_uri(uri: &str) -> Option<u16> {
    let rest = uri.strip_prefix("tcp://")?;
    let port_str = rest.rsplit_once(':')?.1;
    port_str.parse().ok()
}

/// Emit runtime.yaml — the format the existing agent consumes. This is
/// the "old format" structure: `dispatchers:` + `pipeline:`. The CLI
/// auto-allocates ports from port_base: dispatcher i gets ports
/// port_base + 3i, +1, +2 for push/pull/ctrl.
///
/// `spec.ingress` is always set (required in the new format, synthesized
/// for the old format) and is propagated to the first dispatcher as
/// `push_uri:`. That dispatcher binds only at the declared URI — no
/// parallel TCP+IPC dual bind, and no fallback to the auto-allocated
/// push_port. Inner dispatchers keep the legacy dual-bind behavior so
/// same-host workers can reach them over IPC.
fn write_runtime_yaml(
    path: &Path,
    spec: &PipelineSpec,
    wasm_by_stage: &[(String, PathBuf)],
    compile_mode: Option<&str>,
) -> Result<()> {
    // We only support linear topology in the generated runtime.yaml
    // because the existing master.rs expects it. Non-linear DAGs can be
    // expressed in the new yaml but require master.rs fan-out support,
    // which is out of scope for this change.
    let mut is_linear = true;
    if spec.edges.len() != spec.stages.len().saturating_sub(1) {
        is_linear = false;
    } else {
        for (i, (from, to)) in spec.edges.iter().enumerate() {
            if from != &spec.stages[i].name || to != &spec.stages[i + 1].name {
                is_linear = false;
                break;
            }
        }
    }
    if !is_linear {
        anyhow::bail!(
            "non-linear DAGs are not yet supported by the runtime \
             (edges must be stage[i] -> stage[i+1] in declaration order). \
             Fan-out/fan-in support is the next milestone."
        );
    }

    // Build a node lookup. Every stage's placement is guaranteed valid
    // by the parser, so unwrap_or here is impossible to hit in practice.
    let node_by_name: std::collections::HashMap<&str, &NodeSpec> =
        spec.nodes.iter().map(|n| (n.name.as_str(), n)).collect();
    let node_for = |stage_idx: usize| -> &NodeSpec {
        let placement = &spec.stages[stage_idx].placement;
        node_by_name
            .get(placement.as_str())
            .copied()
            .expect("stage placement references a node that doesn't exist — parser bug")
    };

    // Endpoint resolution. Every dispatcher lives on its stage's node,
    // so its IPC endpoint is bound on that node. A worker reading from
    // its own dispatcher is always same-host (IPC). A worker writing to
    // the next stage's dispatcher is IPC if the two stages are on the
    // same node, TCP otherwise. The `force_tcp` knob on a node flips
    // every edge touching that node to TCP — useful for debugging.
    //
    // IPC naming matches the dispatcher's own bind path, which is
    // derived from its `--name` argument:
    //   ipc:///tmp/epico-dispatch-<bare>-{push,pull,ctrl}
    let ipc_endpoint = |dispatch_name: &str, kind: &str| -> String {
        format!("ipc:///tmp/epico-{}-{}", dispatch_name, kind)
    };
    let tcp_endpoint = |host: &str, port: u16| -> String {
        format!("tcp://{}:{}", host, port)
    };

    let mut dispatchers = String::new();
    let mut pipeline = String::new();
    let mut nodes_block = String::new();

    nodes_block.push_str("nodes:\n");
    for n in &spec.nodes {
        nodes_block.push_str(&format!(
            "  - name: {name}\n    host: {host}\n    force_tcp: {ft}\n",
            name = n.name,
            host = n.host,
            ft = n.force_tcp,
        ));
    }

    dispatchers.push_str("dispatchers:\n");
    pipeline.push_str("pipeline:\n");

    for (i, stage) in spec.stages.iter().enumerate() {
        let push_port = spec.port_base + (i as u16) * 3;
        let pull_port = push_port + 1;
        let ctrl_port = push_port + 2;
        let dispatch_name = format!("dispatch-{}", stage.name.trim_start_matches("fn-"));

        // Stage 0 is the pipeline's entry point. Its dispatcher binds at
        // `spec.ingress` — always, no fallback. We propagate that to the
        // agent via a `push_uri:` field on the dispatcher entry (new,
        // honored by the runtime post-change) and additionally align the
        // legacy `push_port:` field when the URI is TCP so the YAML stays
        // internally consistent. For IPC ingress the `push_port` value is
        // irrelevant to the runtime — the dispatcher ignores it when
        // `push_uri` is set — but we keep the auto-allocated value there
        // to avoid a port of 0 showing up in the generated file.
        let (effective_push_port, push_uri_line) = if i == 0 {
            let uri = &spec.ingress;
            let port = port_from_tcp_uri(uri).unwrap_or(push_port);
            (port, format!("    push_uri: {}\n", uri))
        } else {
            (push_port, String::new())
        };

        dispatchers.push_str(&format!(
            "  - name: {dispatch_name}\n\
             \x20   push_port: {push_port}\n\
             \x20   pull_port: {pull_port}\n\
             \x20   ctrl_port: {ctrl_port}\n\
             \x20   placement: {placement}\n\
             \x20   credit_window: {credit_window}\n\
             \x20   batch_events: {batch_events}\n\
             {push_uri}",
            dispatch_name = dispatch_name,
            push_port = effective_push_port,
            pull_port = pull_port,
            ctrl_port = ctrl_port,
            placement = stage.placement,
            credit_window = spec.credit_window,
            batch_events = spec.batch_events,
            push_uri = push_uri_line,
        ));

        // Input endpoint: this stage's worker reads from this stage's
        // dispatcher. Always same-host (they co-locate by construction).
        let my_node = node_for(i);
        let input = if my_node.force_tcp {
            tcp_endpoint(&my_node.host, pull_port)
        } else {
            ipc_endpoint(&dispatch_name, "pull")
        };

        // Output endpoint: this stage's worker writes to either the
        // next stage's dispatcher, or the collector if this is the last
        // stage. Same-node vs cross-node depends on placement.
        let output = if i + 1 == spec.stages.len() {
            spec.collector.clone()
        } else {
            let next = node_for(i + 1);
            let next_push = spec.port_base + ((i + 1) as u16) * 3;
            let next_dispatch_name = format!(
                "dispatch-{}",
                spec.stages[i + 1].name.trim_start_matches("fn-")
            );
            if my_node.name == next.name && !my_node.force_tcp && !next.force_tcp {
                ipc_endpoint(&next_dispatch_name, "push")
            } else {
                tcp_endpoint(&next.host, next_push)
            }
        };

        let wasm_path = &wasm_by_stage[i].1;
        let scaling = &stage.scaling;
        pipeline.push_str(&format!(
            "  - name: {name}\n\
             \x20   wasm: {wasm}\n\
             \x20   placement: {placement}\n\
             \x20   input: {input}\n\
             \x20   output: {output}\n\
             \x20   slo:\n\
             \x20     p99_ms: {p99}\n\
             \x20     min_replicas: {min}\n\
             \x20     max_replicas: {max}\n\
             \x20     queue_up: {qu}\n\
             \x20     queue_down: {qd}\n\
             \x20     cooldown_up_s: {cu}\n\
             \x20     cooldown_down_s: {cd}\n",
            name = stage.name,
            wasm = wasm_path.display(),
            placement = stage.placement,
            input = input,
            output = output,
            p99 = scaling.p99_ms,
            min = scaling.min_replicas,
            max = scaling.max_replicas,
            qu = scaling.queue_up,
            qd = scaling.queue_down,
            cu = scaling.cooldown_up_s,
            cd = scaling.cooldown_down_s,
        ));
    }

    let compile_mode_line = compile_mode
        .map(|m| format!("compile_mode: {}\n", m))
        .unwrap_or_default();

    // Source fan-in width only emitted when set, so the host keeps its own
    // default (and the EPICO_SOURCE_THREADS override) when YAML is silent.
    let source_threads_line = spec
        .source_threads
        .map(|n| format!("source_threads: {}\n", n))
        .unwrap_or_default();

    let combined = format!(
        "# GENERATED by epico-cli from pipeline.yaml. Do not edit directly.\n\
         # Source package: {pkg}\n\n\
         this_host: {this_host}\n\
         ingress:   {ingress}\n\
         collector: {collector}\n\
         resource_sample_interval_ms: {rsi}\n\
         event_format: {event_format}\n\
         source_format: {source_format}\n\
         ingress_mode: {ingress_mode}\n\
         {st}\
         {cm}\n\
         {nodes}\n\
         {d}\n\
         {p}",
        pkg = spec.package,
        this_host = spec.this_host,
        ingress = spec.ingress,
        collector = spec.collector,
        rsi = spec.resource_sample_interval_ms,
        event_format = spec.event_format,
        source_format = spec.source_format,
        ingress_mode = spec.ingress_mode,
        st = source_threads_line,
        cm = compile_mode_line,
        nodes = nodes_block,
        d = dispatchers,
        p = pipeline,
    );
    std::fs::write(path, combined)?;
    Ok(())
}

/// Emit the first binary-envelope schema artifact.
///
/// This is intentionally descriptive rather than executable: it freezes the
/// field order, WIT names, JSON names, and primitive tags the eventual binary
/// row codec should use. Keeping it generated from `types:` means the next
/// implementation step can consume this artifact or replace it with generated
/// Rust without changing the user-facing YAML.
fn write_binary_schema_artifact(output_dir: &Path, spec: &PipelineSpec) -> Result<()> {
    let schema_path = output_dir.join("binary-schema.json");
    let mut type_entries = serde_json::Map::new();

    for (type_name, typedef) in &spec.types {
        let fields: Vec<serde_json::Value> = typedef
            .fields
            .iter()
            .enumerate()
            .map(|(idx, (field_name, field_type))| {
                let optional = field_type.ends_with('?');
                let base = field_type.strip_suffix('?').unwrap_or(field_type);
                serde_json::json!({
                    "id": idx,
                    "json_name": field_name,
                    "wit_name": to_wit_ident(field_name),
                    "type": base,
                    "optional": optional,
                })
            })
            .collect();
        type_entries.insert(type_name.clone(), serde_json::json!({ "fields": fields }));
    }

    let schema = serde_json::json!({
        "format": "epico-binary-envelope",
        "version": 1,
        "status": "schema-artifact-only",
        "notes": [
            "JSON is still the only fully implemented event codec.",
            "This artifact records generated field ids/order for the binary row codec."
        ],
        "types": type_entries,
        "stages": spec.stages.iter().map(|s| serde_json::json!({
            "name": s.name,
            "in": s.input_type,
            "out": s.output_type,
        })).collect::<Vec<_>>(),
    });

    std::fs::write(schema_path, serde_json::to_vec_pretty(&schema)?)?;
    Ok(())
}

/// Best-effort relative path from `from_dir` to `to`. Falls back to the
/// absolute path if they're not comparable (e.g. different drives on
/// Windows). Good enough for Cargo.toml `path = "..."` entries.
fn relative_path(from_dir: &Path, to: &Path) -> PathBuf {
    // Normalize both to absolute paths where possible.
    let from_abs = from_dir.canonicalize().unwrap_or_else(|_| from_dir.to_path_buf());
    let to_abs = to.canonicalize().unwrap_or_else(|_| to.to_path_buf());

    // Walk up from_abs until to_abs is a descendant, counting .. entries.
    let mut ups: Vec<&Path> = Vec::new();
    let mut anchor = from_abs.as_path();
    loop {
        if let Ok(rel) = to_abs.strip_prefix(anchor) {
            let mut result = PathBuf::new();
            for _ in 0..ups.len() {
                result.push("..");
            }
            result.push(rel);
            return result;
        }
        match anchor.parent() {
            Some(p) => {
                ups.push(anchor);
                anchor = p;
            }
            None => return to_abs,
        }
    }
}
