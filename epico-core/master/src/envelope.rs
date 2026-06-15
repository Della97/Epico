//! Event envelope abstraction.
//!
//! The transport moves opaque bytes. This module is the stage-boundary adapter:
//! it decodes those bytes into Wasmtime component values before `process-event`
//! and encodes the returned event back into bytes after the call.
//!
//! Two wire formats:
//!   * JSON — the compatible default. Self-describing, debuggable with `jq`,
//!     accepted from any external producer at the ingress.
//!   * Binary (v1) — a compact little-endian row codec for interior hops.
//!     Decode is format-SNIFFED (magic byte), so mixed pipelines work without
//!     coordination: the loadgen keeps emitting JSON, the first stage decodes
//!     JSON and (with `EPICO_BINARY_EDGES=1`) encodes binary out, downstream
//!     stages and the collector sniff binary. EOS markers stay JSON end to end
//!     (stages forward EOS bytes verbatim), so every EOS detector is unchanged.
//!
//! Binary layout (all integers/floats little-endian):
//!   [0]   u8  magic   = 0xEB
//!   [1]   u8  version = 0x01
//!   [2]   u8  flags        (bit0 = EOS; unused in practice, see above)
//!   [3]   u8  bench bitmap (bit0 = ts_wall, bit1 = ts, bit2 = seq)
//!   ...   [f64 ts_wall] [f64 ts] [u64 seq]  — present per bitmap
//!   u16 hop_count, then per hop:
//!         u8 name_len, name bytes, f64 enter, f64 exit
//!   u16 field_count, then per field:
//!         u8 kind (FieldKind tag), u8 present (0|1),
//!         u8 name_len, name bytes (JSON/snake_case name),
//!         payload if present:
//!           string: u16 len + utf8 | f64/u64/s64: 8B | f32/u32/s32: 4B | bool: 1B

use anyhow::{bail, Result};
use bytes::Bytes;
use wasmtime::component::{Type, Val};

// The binary wire format lives in the shared `epico-wire` crate so the loadgen
// can emit the identical layout without depending on `master`. We re-use its
// constants, the `Scalar` type (aliased to the historical `BinScalar` name),
// the header/field writers, and the reader/decoder here.
use epico_wire::{self as wire, Scalar as BinScalar, BIN_MAGIC, BIN_VERSION};
pub(crate) use epico_wire::is_binary;

use crate::conversion::{
    bench_val_to_json, json_to_bench_val, json_to_record_val, record_val_to_json, FieldKind,
    RecordField,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeFormat {
    Json,
    Binary,
}

impl EnvelopeFormat {
    pub(crate) fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "json" => Ok(Self::Json),
            "binary" | "epico-binary" => Ok(Self::Binary),
            other => bail!("unknown event_format {other:?}; expected `json` or `binary`"),
        }
    }
}

/// True when the buffer carries the binary envelope magic.
/// (Implementation re-exported from `epico-wire` — see the `use` above.)

/// Collector-side adapter: decode ONLY the telemetry-relevant header of a
/// binary event (bench fields + hops) into the same JSON shape the collector
/// already consumes (`bench_ts_wall`, `bench_ts`, `bench_seq`, `bench_hops`).
/// Domain fields are skipped entirely — the collector never reads them — so
/// this is strictly cheaper than the JSON parse it replaces.
pub(crate) fn binary_to_telemetry_json(bytes: &[u8]) -> Option<serde_json::Value> {
    let env = BinaryEnvelope::decode_header_only(bytes).ok()?;
    let mut map = serde_json::Map::new();
    if let Some(v) = env.ts_wall {
        map.insert("bench_ts_wall".into(), serde_json::json!(v));
    }
    if let Some(v) = env.ts {
        map.insert("bench_ts".into(), serde_json::json!(v));
    }
    if let Some(v) = env.seq {
        map.insert("bench_seq".into(), serde_json::json!(v));
    }
    let hops: Vec<serde_json::Value> = env
        .hops
        .iter()
        .map(|(n, e, x)| serde_json::json!([n, e, x]))
        .collect();
    map.insert("bench_hops".into(), serde_json::Value::Array(hops));
    Some(serde_json::Value::Object(map))
}

pub enum EventEnvelope {
    Json(JsonEnvelope),
    Binary(BinaryEnvelope),
}

impl EventEnvelope {
    /// Decode is format-sniffed: the declared `format` is only a fallback for
    /// payloads without the binary magic (i.e. everything JSON today). This
    /// lets the first stage of a binary-edges pipeline accept JSON from the
    /// loadgen with zero configuration.
    pub(crate) fn decode(_format: EnvelopeFormat, bytes: Bytes) -> Result<Self> {
        if is_binary(bytes.as_ref()) {
            Ok(Self::Binary(BinaryEnvelope::decode(bytes)?))
        } else {
            Ok(Self::Json(JsonEnvelope::decode(bytes)?))
        }
    }

    pub(crate) fn is_eos(&self) -> bool {
        match self {
            Self::Json(j) => j.is_eos(),
            Self::Binary(b) => b.is_eos(),
        }
    }

    pub(crate) fn input_val(&self, fields: &[RecordField], record_type: &Type) -> Result<Val> {
        match self {
            Self::Json(j) => Ok(j.input_val(fields, record_type)),
            Self::Binary(b) => Ok(b.input_val(fields)),
        }
    }

    pub(crate) fn bench_val(&self, bench_type: &Type) -> Result<Val> {
        match self {
            Self::Json(j) => Ok(j.bench_val(bench_type)),
            Self::Binary(b) => Ok(b.bench_val(bench_type)),
        }
    }

    /// Encode the post-call event. `out` selects the wire format of the
    /// OUTGOING bytes independently of the input format, so a JSON-in
    /// stage can emit binary (first stage of a binary-edges pipeline) and
    /// vice versa.
    pub(crate) fn encode_output(
        &self,
        event_val: &Val,
        output_fields: &[RecordField],
        bench_val: &Val,
        stage_name: &str,
        enter_ts: f64,
        exit_ts: f64,
        out: EnvelopeFormat,
    ) -> Result<Bytes> {
        match out {
            EnvelopeFormat::Json => {
                let bench_json = match self {
                    Self::Json(j) => bench_val_to_json(
                        bench_val, &j.value, stage_name, enter_ts, exit_ts,
                    ),
                    Self::Binary(b) => b.bench_json_with_hop(stage_name, enter_ts, exit_ts),
                };
                let out_json = record_val_to_json(event_val, output_fields);
                let mut final_obj = match out_json {
                    serde_json::Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                };
                if let serde_json::Value::Object(bm) = bench_json {
                    for (k, v) in bm {
                        final_obj.insert(k, v);
                    }
                }
                Ok(Bytes::from(serde_json::to_vec(
                    &serde_json::Value::Object(final_obj),
                )?))
            }
            EnvelopeFormat::Binary => {
                let (ts_wall, ts, seq, hops) = self.bench_parts();
                Ok(write_binary(
                    ts_wall,
                    ts,
                    seq,
                    &hops,
                    Some((stage_name, enter_ts, exit_ts)),
                    Some((event_val, output_fields)),
                ))
            }
        }
    }

    /// Bench metadata of the INPUT event, format-independent:
    /// (ts_wall, ts, seq, hops).
    fn bench_parts(&self) -> (Option<f64>, Option<f64>, Option<u64>, Vec<(String, f64, f64)>) {
        match self {
            Self::Json(j) => {
                let v = &j.value;
                let hops = v
                    .get("bench_hops")
                    .and_then(|h| h.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|hop| {
                                let a = hop.as_array()?;
                                Some((
                                    a.first()?.as_str()?.to_string(),
                                    a.get(1)?.as_f64()?,
                                    a.get(2)?.as_f64()?,
                                ))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (
                    v.get("bench_ts_wall").and_then(|x| x.as_f64()),
                    v.get("bench_ts").and_then(|x| x.as_f64()),
                    v.get("bench_seq").and_then(|x| x.as_u64()),
                    hops,
                )
            }
            Self::Binary(b) => (b.ts_wall, b.ts, b.seq, b.hops.clone()),
        }
    }

    /// Identity re-encode for the native-bypass experiment
    /// (`EPICO_NATIVE_STAGE=serde`): re-serialize the original event
    /// unchanged except for the appended bench hop, in the SAME format it
    /// arrived in. Replays the full envelope cost with zero Val construction
    /// and zero wasm involvement, isolating the serde share of service time.
    pub(crate) fn encode_identity(
        &self,
        stage_name: &str,
        enter_ts: f64,
        exit_ts: f64,
    ) -> Result<Bytes> {
        match self {
            Self::Json(j) => {
                let mut obj = j.value.clone();
                if let serde_json::Value::Object(ref mut map) = obj {
                    let mut hops = map
                        .get("bench_hops")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    hops.push(serde_json::json!([stage_name, enter_ts, exit_ts]));
                    map.insert("bench_hops".to_string(), serde_json::Value::Array(hops));
                }
                Ok(Bytes::from(serde_json::to_vec(&obj)?))
            }
            Self::Binary(b) => Ok(write_binary_raw_fields(
                b.ts_wall,
                b.ts,
                b.seq,
                &b.hops,
                Some((stage_name, enter_ts, exit_ts)),
                &b.fields,
            )),
        }
    }
}

// ── JSON envelope ─────────────────────────────────────────────────────────────

pub struct JsonEnvelope {
    value: serde_json::Value,
}

impl JsonEnvelope {
    fn decode(bytes: Bytes) -> Result<Self> {
        let value = serde_json::from_slice(bytes.as_ref())?;
        Ok(Self { value })
    }

    fn is_eos(&self) -> bool {
        self.value.get("__epico_eos").and_then(|v| v.as_bool()) == Some(true)
    }

    fn input_val(&self, fields: &[RecordField], record_type: &Type) -> Val {
        json_to_record_val(&self.value, fields, record_type)
    }

    fn bench_val(&self, bench_type: &Type) -> Val {
        json_to_bench_val(&self.value, bench_type)
    }
}

// ── Binary envelope ───────────────────────────────────────────────────────────

// `BinScalar` is `epico_wire::Scalar` (aliased in the imports above). Variants
// and pattern matches below are unchanged; the type now lives in the shared
// wire crate so encoder and decoder can never drift.

#[derive(Debug, Clone)]
pub(crate) struct BinField {
    name: String,
    scalar: Option<BinScalar>,
}

pub struct BinaryEnvelope {
    flags: u8,
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    hops: Vec<(String, f64, f64)>,
    fields: Vec<BinField>,
}

impl BinaryEnvelope {
    fn decode(bytes: Bytes) -> Result<Self> {
        Self::decode_inner(bytes.as_ref(), true)
    }

    fn decode_header_only(bytes: &[u8]) -> Result<Self> {
        Self::decode_inner(bytes, false)
    }

    fn decode_inner(b: &[u8], with_fields: bool) -> Result<Self> {
        let mut c = wire::Reader::new(b);
        let magic = c.u8()?;
        let version = c.u8()?;
        if magic != BIN_MAGIC || version != BIN_VERSION {
            bail!("not a binary envelope (magic {magic:#x} ver {version})");
        }
        let flags = c.u8()?;
        let bitmap = c.u8()?;
        let ts_wall = if bitmap & 1 != 0 { Some(c.f64()?) } else { None };
        let ts = if bitmap & 2 != 0 { Some(c.f64()?) } else { None };
        let seq = if bitmap & 4 != 0 { Some(c.u64()?) } else { None };

        let hop_count = c.u16()? as usize;
        let mut hops = Vec::with_capacity(hop_count);
        for _ in 0..hop_count {
            let nlen = c.u8()? as usize;
            let name = c.str_n(nlen)?;
            let enter = c.f64()?;
            let exit = c.f64()?;
            hops.push((name, enter, exit));
        }

        let mut fields = Vec::new();
        if with_fields {
            let field_count = c.u16()? as usize;
            fields.reserve(field_count);
            for _ in 0..field_count {
                let kind = c.u8()?;
                let present = c.u8()? != 0;
                let nlen = c.u8()? as usize;
                let name = c.str_n(nlen)?;
                // `Reader::scalar` reads the payload for this kind tag and
                // rejects unknown kinds — the single shared decode path.
                let scalar = if present { Some(c.scalar(kind)?) } else { None };
                fields.push(BinField { name, scalar });
            }
        }

        Ok(Self { flags, ts_wall, ts, seq, hops, fields })
    }

    fn is_eos(&self) -> bool {
        self.flags & 1 != 0
    }

    /// Build the event `Val::Record` from decoded fields, honoring the cached
    /// field layout exactly like `json_to_record_val` does for JSON: missing
    /// or kind-mismatched fields fall back to defaults (non-option) or None.
    fn input_val(&self, fields: &[RecordField]) -> Val {
        let vals: Vec<(String, Val)> = fields
            .iter()
            .map(|f| {
                let found = self
                    .fields
                    .iter()
                    .find(|bf| bf.name == f.json_name)
                    .and_then(|bf| bf.scalar.as_ref());
                let val = match (f.is_option, found) {
                    (true, Some(s)) => match scalar_to_val(s, f.inner_kind) {
                        Some(v) => Val::Option(Some(Box::new(v))),
                        None => Val::Option(None),
                    },
                    (true, None) => Val::Option(None),
                    (false, Some(s)) => {
                        scalar_to_val(s, f.inner_kind).unwrap_or_else(|| default_val(f.inner_kind))
                    }
                    (false, None) => default_val(f.inner_kind),
                };
                (f.name.clone(), val)
            })
            .collect();
        Val::Record(vals)
    }

    /// Same fixed shape as `json_to_bench_val`.
    fn bench_val(&self, bench_type: &Type) -> Val {
        if let Type::Record(_) = bench_type {
            let opt_f64 = |v: Option<f64>| match v {
                Some(x) => Val::Option(Some(Box::new(Val::Float64(x)))),
                None => Val::Option(None),
            };
            let opt_u64 = |v: Option<u64>| match v {
                Some(x) => Val::Option(Some(Box::new(Val::U64(x)))),
                None => Val::Option(None),
            };
            Val::Record(vec![
                ("bench-ts-wall".to_string(), opt_f64(self.ts_wall)),
                ("bench-ts".to_string(), opt_f64(self.ts)),
                ("bench-seq".to_string(), opt_u64(self.seq)),
                ("bench-hops".to_string(), Val::List(vec![])),
            ])
        } else {
            Val::Bool(false)
        }
    }

    /// Bench JSON (for binary-in -> json-out), with the new hop appended —
    /// mirrors `bench_val_to_json` semantics.
    fn bench_json_with_hop(
        &self,
        stage_name: &str,
        enter_ts: f64,
        exit_ts: f64,
    ) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(v) = self.ts_wall {
            map.insert("bench_ts_wall".into(), serde_json::json!(v));
        }
        if let Some(v) = self.ts {
            map.insert("bench_ts".into(), serde_json::json!(v));
        }
        if let Some(v) = self.seq {
            map.insert("bench_seq".into(), serde_json::json!(v));
        }
        let mut hops: Vec<serde_json::Value> = self
            .hops
            .iter()
            .map(|(n, e, x)| serde_json::json!([n, e, x]))
            .collect();
        hops.push(serde_json::json!([stage_name, enter_ts, exit_ts]));
        map.insert("bench_hops".into(), serde_json::Value::Array(hops));
        serde_json::Value::Object(map)
    }
}

fn scalar_to_val(s: &BinScalar, kind: FieldKind) -> Option<Val> {
    // Exact-kind match only: a wire/type mismatch means the producing stage's
    // output schema differs from this stage's input schema, which the
    // validator should have rejected; default rather than coerce.
    Some(match (s, kind) {
        (BinScalar::Str(v), FieldKind::String) => Val::String(v.clone()),
        (BinScalar::F64(v), FieldKind::F64) => Val::Float64(*v),
        (BinScalar::F32(v), FieldKind::F32) => Val::Float32(*v),
        (BinScalar::U64(v), FieldKind::U64) => Val::U64(*v),
        (BinScalar::U32(v), FieldKind::U32) => Val::U32(*v),
        (BinScalar::S64(v), FieldKind::S64) => Val::S64(*v),
        (BinScalar::S32(v), FieldKind::S32) => Val::S32(*v),
        (BinScalar::Bool(v), FieldKind::Bool) => Val::Bool(*v),
        _ => return None,
    })
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

// ── Binary writer ─────────────────────────────────────────────────────────────

fn kind_tag(kind: FieldKind) -> u8 {
    match kind {
        FieldKind::String => wire::tag::STR,
        FieldKind::F64 => wire::tag::F64,
        FieldKind::F32 => wire::tag::F32,
        FieldKind::U64 => wire::tag::U64,
        FieldKind::U32 => wire::tag::U32,
        FieldKind::S64 => wire::tag::S64,
        FieldKind::S32 => wire::tag::S32,
        FieldKind::Bool => wire::tag::BOOL,
    }
}

// `write_header` and `write_field_payload` now live in `epico-wire` (imported as
// `wire::write_header` / `wire::write_field` / `wire::write_field_payload`).

/// Encode from a post-call `Val::Record` (the normal output path).
fn write_binary(
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    hops: &[(String, f64, f64)],
    new_hop: Option<(&str, f64, f64)>,
    event: Option<(&Val, &[RecordField])>,
) -> Bytes {
    let mut out = Vec::with_capacity(96 + hops.len() * 40);
    wire::write_header(&mut out, ts_wall, ts, seq, hops, new_hop);

    let count_pos = out.len();
    out.extend_from_slice(&0u16.to_le_bytes());
    let mut n_fields: u16 = 0;

    if let Some((val, fields)) = event {
        if let Val::Record(rec) = val {
            for (name, fval) in rec.iter() {
                let Some(f) = fields.iter().find(|f| f.name == *name) else { continue };
                let scalar = val_to_scalar(fval, f);
                wire::write_field(&mut out, &f.json_name, kind_tag(f.inner_kind), scalar.as_ref());
                n_fields += 1;
            }
        }
    }
    out[count_pos..count_pos + 2].copy_from_slice(&n_fields.to_le_bytes());
    Bytes::from(out)
}

/// Encode from already-decoded `BinField`s (the serde-bypass identity path).
fn write_binary_raw_fields(
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    hops: &[(String, f64, f64)],
    new_hop: Option<(&str, f64, f64)>,
    fields: &[BinField],
) -> Bytes {
    let mut out = Vec::with_capacity(96 + hops.len() * 40);
    wire::write_header(&mut out, ts_wall, ts, seq, hops, new_hop);
    out.extend_from_slice(&(fields.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for f in fields.iter().take(u16::MAX as usize) {
        // An absent field still carries its original kind tag so the schema
        // kind survives the hop; derive it from the scalar when present.
        let tag = f.scalar.as_ref().map(BinScalar::tag).unwrap_or(wire::tag::STR);
        wire::write_field(&mut out, &f.name, tag, f.scalar.as_ref());
    }
    Bytes::from(out)
}

/// Extract a wire scalar from a post-call field Val, honoring option-ness.
fn val_to_scalar(val: &Val, field: &RecordField) -> Option<BinScalar> {
    let inner: &Val = if field.is_option {
        match val {
            Val::Option(Some(b)) => b.as_ref(),
            Val::Option(None) => return None,
            other => other,
        }
    } else {
        val
    };
    Some(match inner {
        Val::String(s) => BinScalar::Str(s.clone()),
        Val::Float64(f) => BinScalar::F64(*f),
        Val::Float32(f) => BinScalar::F32(*f),
        Val::U64(n) => BinScalar::U64(*n),
        Val::U32(n) => BinScalar::U32(*n),
        Val::S64(n) => BinScalar::S64(*n),
        Val::S32(n) => BinScalar::S32(*n),
        Val::Bool(b) => BinScalar::Bool(*b),
        _ => return None,
    })
}

// ── Cursor ────────────────────────────────────────────────────────────────────
// The byte cursor now lives in `epico-wire` as `wire::Reader`; `decode_inner`
// uses it directly.

#[cfg(test)]
mod tests {
    use super::*;

    fn rf(name: &str, kind: FieldKind, opt: bool) -> RecordField {
        RecordField {
            name: name.replace('_', "-"),
            json_name: name.to_string(),
            is_option: opt,
            inner_kind: kind,
        }
    }

    #[test]
    fn binary_roundtrip() {
        let fields = vec![
            rf("sensor_id", FieldKind::String, false),
            rf("value", FieldKind::F64, false),
            rf("unit", FieldKind::String, true),
        ];
        let val = Val::Record(vec![
            ("sensor-id".to_string(), Val::String("s-42".into())),
            ("value".to_string(), Val::Float64(21.5)),
            ("unit".to_string(), Val::Option(None)),
        ]);
        let hops = vec![("relay#0".to_string(), 1.0, 1.5)];
        let bytes = write_binary(
            Some(123.4),
            Some(123.4),
            Some(7),
            &hops,
            Some(("forward#1", 2.0, 2.5)),
            Some((&val, &fields)),
        );
        assert!(is_binary(&bytes));
        let env = BinaryEnvelope::decode(bytes).unwrap();
        assert_eq!(env.ts_wall, Some(123.4));
        assert_eq!(env.seq, Some(7));
        assert_eq!(env.hops.len(), 2);
        assert_eq!(env.hops[1].0, "forward#1");
        let v = env.input_val(&fields);
        if let Val::Record(rec) = v {
            assert_eq!(rec[0].1, Val::String("s-42".into()));
            assert_eq!(rec[1].1, Val::Float64(21.5));
            assert_eq!(rec[2].1, Val::Option(None));
        } else {
            panic!("not a record");
        }
        // telemetry adapter
        let bytes2 = write_binary(Some(9.0), None, None, &[], None, None);
        let tj = binary_to_telemetry_json(&bytes2).unwrap();
        assert_eq!(tj["bench_ts_wall"].as_f64(), Some(9.0));
    }
}

// ── Typed-dispatch support ────────────────────────────────────────────────────

use crate::typed::{EventView, WireValue};

impl EventEnvelope {
    /// Bench scalars of the input event: (ts_wall, ts, seq).
    pub(crate) fn bench_scalars(&self) -> (Option<f64>, Option<f64>, Option<u64>) {
        match self {
            Self::Json(j) => (
                j.value.get("bench_ts_wall").and_then(|x| x.as_f64()),
                j.value.get("bench_ts").and_then(|x| x.as_f64()),
                j.value.get("bench_seq").and_then(|x| x.as_u64()),
            ),
            Self::Binary(b) => (b.ts_wall, b.ts, b.seq),
        }
    }

    /// Encode a typed stage's output: wire fields + carried bench metadata +
    /// the new hop, in the requested format. The typed twin of encode_output.
    pub(crate) fn encode_typed(
        &self,
        fields: &[(&'static str, WireValue)],
        hop_label: &str,
        enter_ts: f64,
        exit_ts: f64,
        out: EnvelopeFormat,
    ) -> Result<Bytes> {
        match out {
            EnvelopeFormat::Json => {
                let mut map = serde_json::Map::with_capacity(fields.len() + 4);
                for (name, v) in fields {
                    let jv = match v {
                        WireValue::Str(s) => serde_json::json!(s),
                        WireValue::F64(x) => serde_json::json!(x),
                        WireValue::F32(x) => serde_json::json!(x),
                        WireValue::U64(x) => serde_json::json!(x),
                        WireValue::U32(x) => serde_json::json!(x),
                        WireValue::S64(x) => serde_json::json!(x),
                        WireValue::S32(x) => serde_json::json!(x),
                        WireValue::Bool(x) => serde_json::json!(x),
                        WireValue::Absent => continue,
                    };
                    map.insert((*name).to_string(), jv);
                }
                let bench_json = match self {
                    Self::Json(j) => {
                        // Reuse the existing hop-append semantics.
                        let mut bm = serde_json::Map::new();
                        for key in ["bench_ts_wall", "bench_ts", "bench_seq"] {
                            if let Some(v) = j.value.get(key) {
                                bm.insert(key.to_string(), v.clone());
                            }
                        }
                        let mut hops = j
                            .value
                            .get("bench_hops")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        hops.push(serde_json::json!([hop_label, enter_ts, exit_ts]));
                        bm.insert("bench_hops".to_string(), serde_json::Value::Array(hops));
                        serde_json::Value::Object(bm)
                    }
                    Self::Binary(b) => b.bench_json_with_hop(hop_label, enter_ts, exit_ts),
                };
                if let serde_json::Value::Object(bm) = bench_json {
                    for (k, v) in bm {
                        map.insert(k, v);
                    }
                }
                Ok(Bytes::from(serde_json::to_vec(&serde_json::Value::Object(map))?))
            }
            EnvelopeFormat::Binary => {
                let (ts_wall, ts, seq, hops) = self.bench_parts();
                Ok(write_binary_typed(
                    ts_wall, ts, seq, &hops,
                    Some((hop_label, enter_ts, exit_ts)),
                    fields,
                ))
            }
        }
    }
}

impl EventView for EventEnvelope {
    fn str_field(&self, n: &str) -> Option<String> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_str()).map(str::to_string),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::Str(v) => Some(v.clone()),
                _ => None,
            }),
        }
    }
    fn f64_field(&self, n: &str) -> Option<f64> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_f64()),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::F64(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn f32_field(&self, n: &str) -> Option<f32> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_f64()).map(|v| v as f32),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::F32(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn u64_field(&self, n: &str) -> Option<u64> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_u64()),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::U64(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn u32_field(&self, n: &str) -> Option<u32> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_u64()).map(|v| v as u32),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::U32(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn s64_field(&self, n: &str) -> Option<i64> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_i64()),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::S64(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn s32_field(&self, n: &str) -> Option<i32> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_i64()).map(|v| v as i32),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::S32(v) => Some(*v),
                _ => None,
            }),
        }
    }
    fn bool_field(&self, n: &str) -> Option<bool> {
        match self {
            Self::Json(j) => j.value.get(n).and_then(|v| v.as_bool()),
            Self::Binary(b) => b.field(n).and_then(|s| match s {
                BinScalar::Bool(v) => Some(*v),
                _ => None,
            }),
        }
    }
}

impl BinaryEnvelope {
    fn field(&self, json_name: &str) -> Option<&BinScalar> {
        self.fields
            .iter()
            .find(|f| f.name == json_name)
            .and_then(|f| f.scalar.as_ref())
    }
}

/// Encode a binary envelope from typed wire fields.
fn write_binary_typed(
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    hops: &[(String, f64, f64)],
    new_hop: Option<(&str, f64, f64)>,
    fields: &[(&'static str, WireValue)],
) -> Bytes {
    let mut out = Vec::with_capacity(96 + hops.len() * 40);
    wire::write_header(&mut out, ts_wall, ts, seq, hops, new_hop);
    out.extend_from_slice(&(fields.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for (name, v) in fields.iter().take(u16::MAX as usize) {
        let (tag, scalar): (u8, Option<BinScalar>) = match v {
            WireValue::Str(s) => (wire::tag::STR, Some(BinScalar::Str(s.clone()))),
            WireValue::F64(x) => (wire::tag::F64, Some(BinScalar::F64(*x))),
            WireValue::F32(x) => (wire::tag::F32, Some(BinScalar::F32(*x))),
            WireValue::U64(x) => (wire::tag::U64, Some(BinScalar::U64(*x))),
            WireValue::U32(x) => (wire::tag::U32, Some(BinScalar::U32(*x))),
            WireValue::S64(x) => (wire::tag::S64, Some(BinScalar::S64(*x))),
            WireValue::S32(x) => (wire::tag::S32, Some(BinScalar::S32(*x))),
            WireValue::Bool(x) => (wire::tag::BOOL, Some(BinScalar::Bool(*x))),
            WireValue::Absent => (wire::tag::STR, None),
        };
        wire::write_field(&mut out, name, tag, scalar.as_ref());
    }
    Bytes::from(out)
}
