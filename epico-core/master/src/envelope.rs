//! Event envelope abstraction.
//!
//! The transport moves opaque bytes. This module is the stage-boundary adapter:
//! it decodes those bytes into Wasmtime component values before `process-event`
//! and encodes the returned event back into bytes after the call.
//!
//! JSON is the compatible default. `Binary` is intentionally present as a
//! first-class format hook, but the binary row codec still needs to be filled in
//! before users can opt into it for real runs.

use anyhow::{bail, Result};
use bytes::Bytes;
use wasmtime::component::{Type, Val};

use crate::conversion::{
    bench_val_to_json, json_to_bench_val, json_to_record_val, record_val_to_json, RecordField,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeFormat {
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

pub(crate) enum EventEnvelope {
    Json(JsonEnvelope),
    Binary(BinaryEnvelope),
}

impl EventEnvelope {
    pub(crate) fn decode(format: EnvelopeFormat, bytes: Bytes) -> Result<Self> {
        match format {
            EnvelopeFormat::Json => Ok(Self::Json(JsonEnvelope::decode(bytes)?)),
            EnvelopeFormat::Binary => Ok(Self::Binary(BinaryEnvelope::decode(bytes)?)),
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
            Self::Binary(b) => b.input_val(fields, record_type),
        }
    }

    pub(crate) fn bench_val(&self, bench_type: &Type) -> Result<Val> {
        match self {
            Self::Json(j) => Ok(j.bench_val(bench_type)),
            Self::Binary(b) => b.bench_val(bench_type),
        }
    }

    pub(crate) fn encode_output(
        &self,
        event_val: &Val,
        output_fields: &[RecordField],
        bench_val: &Val,
        stage_name: &str,
        enter_ts: f64,
        exit_ts: f64,
    ) -> Result<Bytes> {
        match self {
            Self::Json(j) => j.encode_output(
                event_val,
                output_fields,
                bench_val,
                stage_name,
                enter_ts,
                exit_ts,
            ),
            Self::Binary(b) => b.encode_output(
                event_val,
                output_fields,
                bench_val,
                stage_name,
                enter_ts,
                exit_ts,
            ),
        }
    }

    /// Identity re-encode for the native-bypass experiment
    /// (`EPICO_NATIVE_STAGE=serde`): re-serialize the original event JSON
    /// unchanged except for the appended bench hop. This replays the full
    /// envelope cost (parse happened at decode + serialize + hop bookkeeping)
    /// with zero Val construction and zero wasm involvement, isolating the
    /// serde share of the stage service time.
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
            Self::Binary(_) => bail!(
                "EPICO_NATIVE_STAGE=serde requires event_format: json \
                 (binary envelopes are not implemented yet)"
            ),
        }
    }
}

pub(crate) struct JsonEnvelope {
    value: serde_json::Value,
}

impl JsonEnvelope {
    fn decode(bytes: Bytes) -> Result<Self> {
        let value = serde_json::from_slice(bytes.as_ref())?;
        Ok(Self { value })
    }

    fn is_eos(&self) -> bool {
        self.value
            .get("__epico_eos")
            .and_then(|v| v.as_bool())
            == Some(true)
    }

    fn input_val(&self, fields: &[RecordField], record_type: &Type) -> Val {
        json_to_record_val(&self.value, fields, record_type)
    }

    fn bench_val(&self, bench_type: &Type) -> Val {
        json_to_bench_val(&self.value, bench_type)
    }

    fn encode_output(
        &self,
        event_val: &Val,
        output_fields: &[RecordField],
        bench_val: &Val,
        stage_name: &str,
        enter_ts: f64,
        exit_ts: f64,
    ) -> Result<Bytes> {
        let out_json = record_val_to_json(event_val, output_fields);
        let bench_json = bench_val_to_json(bench_val, &self.value, stage_name, enter_ts, exit_ts);

        let mut final_obj = match out_json {
            serde_json::Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        if let serde_json::Value::Object(bm) = bench_json {
            for (k, v) in bm {
                final_obj.insert(k, v);
            }
        }

        Ok(Bytes::from(serde_json::to_vec(&serde_json::Value::Object(final_obj))?))
    }
}

pub(crate) struct BinaryEnvelope {
    _bytes: Bytes,
}

impl BinaryEnvelope {
    fn decode(bytes: Bytes) -> Result<Self> {
        Ok(Self { _bytes: bytes })
    }

    fn is_eos(&self) -> bool {
        false
    }

    fn input_val(&self, _fields: &[RecordField], _record_type: &Type) -> Result<Val> {
        bail!(
            "binary event envelopes are scaffolded but not implemented yet; \
             keep event_format: json until the binary row codec is completed"
        )
    }

    fn bench_val(&self, _bench_type: &Type) -> Result<Val> {
        bail!(
            "binary event envelopes are scaffolded but not implemented yet; \
             keep event_format: json until the binary row codec is completed"
        )
    }

    fn encode_output(
        &self,
        _event_val: &Val,
        _output_fields: &[RecordField],
        _bench_val: &Val,
        _stage_name: &str,
        _enter_ts: f64,
        _exit_ts: f64,
    ) -> Result<Bytes> {
        bail!(
            "binary event envelopes are scaffolded but not implemented yet; \
             keep event_format: json until the binary row codec is completed"
        )
    }
}
