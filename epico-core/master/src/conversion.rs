//! JSON ↔ Wasmtime `Val` conversion — the dynamic-dispatch layer.
//!
//! Stages are invoked with `Func::call` using the untyped `Vec<Val>`
//! API, so the host never imports a stage-specific type. This module
//! does the two conversions that happen on every event:
//!
//!   1. Incoming JSON  → `Val::Record` for the domain event, plus a
//!      `Val::Record` for `bench-ctx`. Field layout is learned at
//!      worker boot time via `extract_record_fields` and cached for
//!      the lifetime of the worker.
//!
//!   2. Outgoing `Val::Record` → JSON map, merged with a bench-ctx
//!      JSON object that carries the current stage's hop.
//!
//! Scope boundaries on purpose:
//!   - Only scalar field types are handled today (strings, ints, floats,
//!     bools, and `option<T>` of each). Nested records and lists are
//!     not supported — a stage requiring those would need new cases
//!     in `type_to_kind` / `json_scalar_to_val` / `scalar_val_to_json`.
//!   - Unknown WIT types fall back to `FieldKind::String` rather than
//!     erroring. That's wrong if the stage actually declares e.g. a
//!     list field, but we'd notice immediately at worker boot when the
//!     call traps. Worth revisiting when we grow the supported set.

use wasmtime::component::{Type, Val};

// ---------------------------------------------------------------------------
// Field metadata cached per worker
// ---------------------------------------------------------------------------

/// Field metadata extracted from a component's type at boot time.
/// Cached per worker, used for JSON ↔ Val conversion on every event.
#[derive(Debug, Clone)]
pub(crate) struct RecordField {
    /// WIT name (kebab-case, e.g. "sensor-id").
    pub name: String,
    /// JSON key (snake_case, e.g. "sensor_id").
    pub json_name: String,
    /// Whether the WIT type is `option<T>`.
    pub is_option: bool,
    pub inner_kind: FieldKind,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FieldKind {
    String,
    F64,
    F32,
    U64,
    U32,
    S64,
    S32,
    Bool,
}

/// Bench-ctx field layout (fixed — not user-defined).
///
/// Pairs are `(wit-name, json-name)`; the host constructs bench-ctx
/// records from a known shape rather than introspecting per worker.
pub(crate) const BENCH_FIELDS: &[(&str, &str)] = &[
    ("bench-ts-wall", "bench_ts_wall"),
    ("bench-ts", "bench_ts"),
    ("bench-seq", "bench_seq"),
    ("bench-hops", "bench_hops"),
];

pub(crate) fn wit_name_to_json(wit: &str) -> String {
    wit.replace('-', "_")
}

// ---------------------------------------------------------------------------
// Type introspection
// ---------------------------------------------------------------------------

/// Extract the record's field layout from a component Func's parameter
/// or result type. Produces the cached layout used on every event.
pub(crate) fn extract_record_fields(ty: &Type) -> Vec<RecordField> {
    let mut fields = Vec::new();
    if let Type::Record(rec) = ty {
        for field in rec.fields() {
            let name = field.name.to_string();
            let field_ty = field.ty;
            let (is_option, inner) = match &field_ty {
                Type::Option(opt) => (true, opt.ty()),
                other => (false, other.clone()),
            };
            let inner_kind = type_to_kind(&inner);
            fields.push(RecordField {
                json_name: wit_name_to_json(&name),
                name,
                is_option,
                inner_kind,
            });
        }
    }
    fields
}

fn type_to_kind(ty: &Type) -> FieldKind {
    match ty {
        Type::String => FieldKind::String,
        Type::Float64 => FieldKind::F64,
        Type::Float32 => FieldKind::F32,
        Type::U64 => FieldKind::U64,
        Type::U32 => FieldKind::U32,
        Type::S64 => FieldKind::S64,
        Type::S32 => FieldKind::S32,
        Type::Bool => FieldKind::Bool,
        _ => FieldKind::String, // fallback — see module docs
    }
}

// ---------------------------------------------------------------------------
// JSON → Val (input path)
// ---------------------------------------------------------------------------

/// Build a `Val::Record` from JSON using the cached field layout.
///
/// `_record_type` is kept in the signature for future use (nested
/// records would need access to the WIT type to recurse); today it's
/// unread because everything is scalar.
pub(crate) fn json_to_record_val(
    json: &serde_json::Value,
    fields: &[RecordField],
    _record_type: &Type,
) -> Val {
    let vals: Vec<(String, Val)> = fields
        .iter()
        .map(|f| {
            let val = json_field_to_val(json.get(&f.json_name), f);
            (f.name.clone(), val)
        })
        .collect();
    Val::Record(vals)
}

fn json_field_to_val(jv: Option<&serde_json::Value>, field: &RecordField) -> Val {
    if field.is_option {
        let inner = jv
            .and_then(|v| if v.is_null() { None } else { Some(v) })
            .map(|v| json_scalar_to_val(v, field.inner_kind));
        match inner {
            Some(v) => Val::Option(Some(Box::new(v))),
            None => Val::Option(None),
        }
    } else {
        match jv {
            Some(v) => json_scalar_to_val(v, field.inner_kind),
            None => default_val(field.inner_kind),
        }
    }
}

fn json_scalar_to_val(v: &serde_json::Value, kind: FieldKind) -> Val {
    match kind {
        FieldKind::String => Val::String(v.as_str().unwrap_or("").to_string()),
        FieldKind::F64 => Val::Float64(v.as_f64().unwrap_or(0.0)),
        FieldKind::F32 => Val::Float32(v.as_f64().unwrap_or(0.0) as f32),
        FieldKind::U64 => Val::U64(v.as_u64().unwrap_or(0)),
        FieldKind::U32 => Val::U32(v.as_u64().unwrap_or(0) as u32),
        FieldKind::S64 => Val::S64(v.as_i64().unwrap_or(0)),
        FieldKind::S32 => Val::S32(v.as_i64().unwrap_or(0) as i32),
        FieldKind::Bool => Val::Bool(v.as_bool().unwrap_or(false)),
    }
}

fn default_val(kind: FieldKind) -> Val {
    match kind {
        FieldKind::String => Val::String(String::new()),
        FieldKind::F64 => Val::Float64(0.0),
        FieldKind::F32 => Val::Float32(0.0),
        FieldKind::U64 => Val::U64(0),
        FieldKind::U32 => Val::U32(0),
        FieldKind::S64 => Val::S64(0),
        FieldKind::S32 => Val::S32(0),
        FieldKind::Bool => Val::Bool(false),
    }
}

// ---------------------------------------------------------------------------
// Val → JSON (output path)
// ---------------------------------------------------------------------------

/// Convert a `Val::Record` back to a JSON object using the cached
/// field layout.
///
/// Fields are matched positionally against `fields` — this assumes
/// the guest preserves record field order, which wit-bindgen does by
/// construction. If output-vs-input type ordering ever diverges we'd
/// want to key by name instead.
pub(crate) fn record_val_to_json(val: &Val, fields: &[RecordField]) -> serde_json::Value {
    use serde_json::{Map, Value};
    use std::collections::HashMap;
    let mut map = Map::new();
    if let Val::Record(rec) = val {
        // Build a lookup once, use it for every field. The fields slice
        // is small (typically <20 entries), so a HashMap is overkill
        // for scan cost but cheap to build and removes the positional
        // assumption entirely.
        let by_name: HashMap<&str, &RecordField> =
            fields.iter().map(|f| (f.name.as_str(), f)).collect();
        for (name, field_val) in rec.iter() {
            if let Some(f) = by_name.get(name.as_str()) {
                if let Some(jv) = val_to_json_value(field_val, f) {
                    map.insert(f.json_name.clone(), jv);
                }
            }
        }
    }
    Value::Object(map)
}

fn val_to_json_value(val: &Val, field: &RecordField) -> Option<serde_json::Value> {
    if field.is_option {
        if let Val::Option(opt) = val {
            match opt {
                Some(inner) => scalar_val_to_json(inner),
                None => None, // omit None fields from JSON
            }
        } else {
            None
        }
    } else {
        scalar_val_to_json(val)
    }
}

fn scalar_val_to_json(val: &Val) -> Option<serde_json::Value> {
    use serde_json::json;
    match val {
        Val::String(s) => Some(json!(s)),
        Val::Float64(f) => Some(json!(f)),
        Val::Float32(f) => Some(json!(f)),
        Val::U64(n) => Some(json!(n)),
        Val::U32(n) => Some(json!(n)),
        Val::S64(n) => Some(json!(n)),
        Val::S32(n) => Some(json!(n)),
        Val::Bool(b) => Some(json!(b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Bench-ctx conversion (fixed-shape, not user-defined)
// ---------------------------------------------------------------------------

/// Build `bench-ctx` as a `Val` from JSON. Bench-ctx has a fixed shape
/// that doesn't change across pipelines, so we hardcode the field list
/// rather than introspecting.
pub(crate) fn json_to_bench_val(json: &serde_json::Value, bench_type: &Type) -> Val {
    if let Type::Record(_rt) = bench_type {
        let vals: Vec<(String, Val)> = vec![
            (
                "bench-ts-wall".to_string(),
                json_to_opt_f64(json.get("bench_ts_wall")),
            ),
            ("bench-ts".to_string(), json_to_opt_f64(json.get("bench_ts"))),
            (
                "bench-seq".to_string(),
                json_to_opt_u64(json.get("bench_seq")),
            ),
            (
                "bench-hops".to_string(),
                json_to_bench_hops(json.get("bench_hops"), bench_type),
            ),
        ];
        Val::Record(vals)
    } else {
        Val::Bool(false)
    }
}

fn json_to_opt_f64(v: Option<&serde_json::Value>) -> Val {
    let inner = v.and_then(|x| x.as_f64()).map(Val::Float64);
    match inner {
        Some(v) => Val::Option(Some(Box::new(v))),
        None => Val::Option(None),
    }
}

fn json_to_opt_u64(v: Option<&serde_json::Value>) -> Val {
    let inner = v.and_then(|x| x.as_u64()).map(Val::U64);
    match inner {
        Some(v) => Val::Option(Some(Box::new(v))),
        None => Val::Option(None),
    }
}

fn json_to_bench_hops(_v: Option<&serde_json::Value>, _bench_type: &Type) -> Val {
    // For now, pass an empty list. The host appends hops after the call.
    // Full hop passthrough requires constructing a Val::List of Val::Record
    // for bench-hop, which we can add when needed.
    Val::List(vec![])
}

/// Merge bench-ctx output back into JSON, appending the current
/// stage's hop entry.
///
/// `_val` is unused: the guest is contractually forbidden from mutating
/// bench-ctx (the SDK macro preserves it unchanged), so we rebuild from
/// `original_json` instead. Kept in the signature so a later revision
/// can verify the guest didn't tamper.
pub(crate) fn bench_val_to_json(
    _val: &Val,
    original_json: &serde_json::Value,
    stage_name: &str,
    enter_ts: f64,
    exit_ts: f64,
) -> serde_json::Value {
    use serde_json::{json, Map, Value};
    let mut map = Map::new();

    // Preserve bench fields from original input JSON.
    for &(_, json_key) in BENCH_FIELDS {
        if let Some(v) = original_json.get(json_key) {
            map.insert(json_key.to_string(), v.clone());
        }
    }

    // Append new hop.
    let mut hops = original_json
        .get("bench_hops")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    hops.push(json!([stage_name, enter_ts, exit_ts]));
    map.insert("bench_hops".to_string(), Value::Array(hops));

    Value::Object(map)
}
