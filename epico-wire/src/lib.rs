//! Epico binary event-envelope wire format — the single source of truth.
//!
//! This crate is the *format spec*. Both `master` (the host data path) and
//! `epico-loadgen` (the producer) depend on it so the binary layout is defined
//! exactly once and can never drift between encoder and decoder. It has zero
//! dependencies on purpose: it speaks only `Vec<u8>` and primitive scalars, so
//! the heavyweight `master` (wasmtime) crate and the lightweight loadgen can
//! both pull it in cheaply.
//!
//! # Layout (binary v1, all integers/floats little-endian)
//!
//! ```text
//!   [0]   u8  magic   = 0xEB
//!   [1]   u8  version = 0x01
//!   [2]   u8  flags        (bit0 = EOS)
//!   [3]   u8  bench bitmap (bit0 = ts_wall, bit1 = ts, bit2 = seq)
//!   ...   [f64 ts_wall] [f64 ts] [u64 seq]  — present per bitmap, in this order
//!   u16 hop_count, then per hop:
//!         u8 name_len, name bytes, f64 enter, f64 exit
//!   u16 field_count, then per field:
//!         u8 kind (tag), u8 present (0|1),
//!         u8 name_len, name bytes (JSON/snake_case name),
//!         payload if present:
//!           string: u16 len + utf8 | f64/u64/s64: 8B | f32/u32/s32: 4B | bool: 1B
//! ```
//!
//! EOS markers are intentionally *not* expressed in this format. They stay JSON
//! end to end (the host sniffs a JSON EOS even inside a binary stream), so the
//! `flags` EOS bit exists in the layout but is unused by Epico's producers.

use std::fmt;

/// Magic byte that prefixes every binary envelope.
pub const BIN_MAGIC: u8 = 0xEB;
/// Wire format version. Bump on any incompatible layout change.
pub const BIN_VERSION: u8 = 0x01;

/// Field-kind wire tags. The numeric value is part of the on-wire format and
/// must never be reordered; append new kinds at the end.
pub mod tag {
    pub const STR: u8 = 0;
    pub const F64: u8 = 1;
    pub const F32: u8 = 2;
    pub const U64: u8 = 3;
    pub const U32: u8 = 4;
    pub const S64: u8 = 5;
    pub const S32: u8 = 6;
    pub const BOOL: u8 = 7;
}

/// A single decoded/encodable scalar field value, format-independent.
///
/// This is the shared spelling of what `master` historically called
/// `BinScalar`; `master` re-exports it under that name.
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Str(String),
    F64(f64),
    F32(f32),
    U64(u64),
    U32(u32),
    S64(i64),
    S32(i32),
    Bool(bool),
}

impl Scalar {
    /// The wire tag matching this scalar's runtime kind.
    pub fn tag(&self) -> u8 {
        match self {
            Scalar::Str(_) => tag::STR,
            Scalar::F64(_) => tag::F64,
            Scalar::F32(_) => tag::F32,
            Scalar::U64(_) => tag::U64,
            Scalar::U32(_) => tag::U32,
            Scalar::S64(_) => tag::S64,
            Scalar::S32(_) => tag::S32,
            Scalar::Bool(_) => tag::BOOL,
        }
    }
}

/// Errors raised while decoding a binary envelope. Implements
/// `std::error::Error` so callers using `anyhow` can `?` it directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// Buffer was too short to satisfy a read at the given byte offset.
    Truncated(usize),
    /// Magic/version prefix did not match.
    BadMagic { magic: u8, version: u8 },
    /// Field carried an unrecognized kind tag.
    UnknownKind(u8),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Truncated(at) => write!(f, "binary envelope truncated at byte {at}"),
            WireError::BadMagic { magic, version } => {
                write!(f, "not a binary envelope (magic {magic:#x} ver {version})")
            }
            WireError::UnknownKind(k) => write!(f, "binary envelope: unknown field kind {k}"),
        }
    }
}

impl std::error::Error for WireError {}

/// True when the buffer carries the binary envelope magic + version prefix.
#[inline]
pub fn is_binary(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] == BIN_MAGIC && bytes[1] == BIN_VERSION
}

// ── Writer ──────────────────────────────────────────────────────────────────

/// A hop already on the event: `(name, enter_ts, exit_ts)`.
pub type Hop<'a> = (&'a str, f64, f64);

/// Append the fixed envelope header — magic, version, flags, the bench bitmap +
/// present scalars, and the hop list (existing `hops` followed by an optional
/// freshly-appended `new_hop`). Does NOT write the field section; the caller
/// appends the `u16 field_count` and per-field bytes afterward.
pub fn write_header(
    out: &mut Vec<u8>,
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    hops: &[(String, f64, f64)],
    new_hop: Option<Hop<'_>>,
) {
    out.push(BIN_MAGIC);
    out.push(BIN_VERSION);
    out.push(0); // flags
    let mut bitmap = 0u8;
    if ts_wall.is_some() {
        bitmap |= 1;
    }
    if ts.is_some() {
        bitmap |= 2;
    }
    if seq.is_some() {
        bitmap |= 4;
    }
    out.push(bitmap);
    if let Some(v) = ts_wall {
        out.extend_from_slice(&v.to_le_bytes());
    }
    if let Some(v) = ts {
        out.extend_from_slice(&v.to_le_bytes());
    }
    if let Some(v) = seq {
        out.extend_from_slice(&v.to_le_bytes());
    }

    let n_hops = hops.len() + usize::from(new_hop.is_some());
    out.extend_from_slice(&(n_hops.min(u16::MAX as usize) as u16).to_le_bytes());
    let write_hop = |out: &mut Vec<u8>, name: &str, enter: f64, exit: f64| {
        let nb = name.as_bytes();
        let nlen = nb.len().min(u8::MAX as usize);
        out.push(nlen as u8);
        out.extend_from_slice(&nb[..nlen]);
        out.extend_from_slice(&enter.to_le_bytes());
        out.extend_from_slice(&exit.to_le_bytes());
    };
    for (n, e, x) in hops {
        write_hop(out, n, *e, *x);
    }
    if let Some((n, e, x)) = new_hop {
        write_hop(out, n, e, x);
    }
}

/// Append a scalar payload (no tag / no presence / no name — payload only).
pub fn write_field_payload(out: &mut Vec<u8>, s: &Scalar) {
    match s {
        Scalar::Str(v) => {
            let b = v.as_bytes();
            let len = b.len().min(u16::MAX as usize);
            out.extend_from_slice(&(len as u16).to_le_bytes());
            out.extend_from_slice(&b[..len]);
        }
        Scalar::F64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::F32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::U64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::U32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::S64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::S32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::Bool(v) => out.push(u8::from(*v)),
    }
}

/// Append a complete field record: `kind tag`, `present` byte, name, and the
/// payload when present. `tag` is the *declared* field kind — it is preserved
/// even for an absent (`present=0`) field so the schema kind survives the hop.
pub fn write_field(out: &mut Vec<u8>, name: &str, tag: u8, scalar: Option<&Scalar>) {
    out.push(tag);
    out.push(u8::from(scalar.is_some()));
    let nb = name.as_bytes();
    let nlen = nb.len().min(u8::MAX as usize);
    out.push(nlen as u8);
    out.extend_from_slice(&nb[..nlen]);
    if let Some(s) = scalar {
        write_field_payload(out, s);
    }
}

// ── Builder (producer-side convenience) ───────────────────────────────────────

/// Fluent builder for a fresh event at the ingress — the loadgen's path.
///
/// Collects header scalars and domain fields, then emits a complete binary
/// envelope with an empty hop list (a freshly-produced event has no hops yet).
/// Field insertion order is preserved on the wire; decoders look fields up by
/// name, so order is purely cosmetic.
#[derive(Default)]
pub struct EventBuilder {
    ts_wall: Option<f64>,
    ts: Option<f64>,
    seq: Option<u64>,
    fields: Vec<(String, Scalar)>,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn ts_wall(mut self, v: f64) -> Self {
        self.ts_wall = Some(v);
        self
    }
    pub fn ts(mut self, v: f64) -> Self {
        self.ts = Some(v);
        self
    }
    pub fn seq(mut self, v: u64) -> Self {
        self.seq = Some(v);
        self
    }
    pub fn field(mut self, name: impl Into<String>, scalar: Scalar) -> Self {
        self.fields.push((name.into(), scalar));
        self
    }
    pub fn str_field(self, name: impl Into<String>, v: impl Into<String>) -> Self {
        self.field(name, Scalar::Str(v.into()))
    }
    pub fn f64_field(self, name: impl Into<String>, v: f64) -> Self {
        self.field(name, Scalar::F64(v))
    }
    pub fn f32_field(self, name: impl Into<String>, v: f32) -> Self {
        self.field(name, Scalar::F32(v))
    }
    pub fn u64_field(self, name: impl Into<String>, v: u64) -> Self {
        self.field(name, Scalar::U64(v))
    }
    pub fn u32_field(self, name: impl Into<String>, v: u32) -> Self {
        self.field(name, Scalar::U32(v))
    }
    pub fn s64_field(self, name: impl Into<String>, v: i64) -> Self {
        self.field(name, Scalar::S64(v))
    }
    pub fn s32_field(self, name: impl Into<String>, v: i32) -> Self {
        self.field(name, Scalar::S32(v))
    }
    pub fn bool_field(self, name: impl Into<String>, v: bool) -> Self {
        self.field(name, Scalar::Bool(v))
    }

    /// Serialize to a complete binary envelope.
    pub fn finish(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(96 + self.fields.len() * 16);
        write_header(&mut out, self.ts_wall, self.ts, self.seq, &[], None);
        let n = self.fields.len().min(u16::MAX as usize);
        out.extend_from_slice(&(n as u16).to_le_bytes());
        for (name, scalar) in self.fields.iter().take(u16::MAX as usize) {
            write_field(&mut out, name, scalar.tag(), Some(scalar));
        }
        out
    }
}

// ── Reader / decoder ──────────────────────────────────────────────────────────

/// A forward-only cursor over a binary envelope buffer. Mirrors the helper API
/// `master` already relied on, so its decode logic can sit on top unchanged.
pub struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader { b, pos: 0 }
    }
    pub fn pos(&self) -> usize {
        self.pos
    }
    pub fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.pos)
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        if self.pos + n > self.b.len() {
            return Err(WireError::Truncated(self.pos));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.arr8()?))
    }
    pub fn f64(&mut self) -> Result<f64, WireError> {
        Ok(f64::from_le_bytes(self.arr8()?))
    }
    pub fn arr4(&mut self) -> Result<[u8; 4], WireError> {
        Ok(self.take(4)?.try_into().unwrap())
    }
    pub fn arr8(&mut self) -> Result<[u8; 8], WireError> {
        Ok(self.take(8)?.try_into().unwrap())
    }
    pub fn str_n(&mut self, n: usize) -> Result<String, WireError> {
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }

    /// Read one scalar payload given its kind tag.
    pub fn scalar(&mut self, kind: u8) -> Result<Scalar, WireError> {
        Ok(match kind {
            tag::STR => {
                let slen = self.u16()? as usize;
                Scalar::Str(self.str_n(slen)?)
            }
            tag::F64 => Scalar::F64(self.f64()?),
            tag::F32 => Scalar::F32(f32::from_le_bytes(self.arr4()?)),
            tag::U64 => Scalar::U64(self.u64()?),
            tag::U32 => Scalar::U32(u32::from_le_bytes(self.arr4()?)),
            tag::S64 => Scalar::S64(i64::from_le_bytes(self.arr8()?)),
            tag::S32 => Scalar::S32(i32::from_le_bytes(self.arr4()?)),
            tag::BOOL => Scalar::Bool(self.u8()? != 0),
            other => return Err(WireError::UnknownKind(other)),
        })
    }
}

/// The decoded header (everything before the field section).
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub flags: u8,
    pub ts_wall: Option<f64>,
    pub ts: Option<f64>,
    pub seq: Option<u64>,
    pub hops: Vec<(String, f64, f64)>,
}

impl Header {
    /// True when the EOS flag bit is set.
    pub fn is_eos(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// A decoded field: name plus the scalar value, or `None` when `present=0`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedField {
    pub name: String,
    pub kind: u8,
    pub scalar: Option<Scalar>,
}

/// Decode just the header (magic, bench scalars, hops), leaving the reader
/// positioned at the start of the field-count. Cheaper than a full decode for
/// telemetry-only consumers (the collector).
pub fn read_header(r: &mut Reader<'_>) -> Result<Header, WireError> {
    let magic = r.u8()?;
    let version = r.u8()?;
    if magic != BIN_MAGIC || version != BIN_VERSION {
        return Err(WireError::BadMagic { magic, version });
    }
    let flags = r.u8()?;
    let bitmap = r.u8()?;
    let ts_wall = if bitmap & 1 != 0 { Some(r.f64()?) } else { None };
    let ts = if bitmap & 2 != 0 { Some(r.f64()?) } else { None };
    let seq = if bitmap & 4 != 0 { Some(r.u64()?) } else { None };

    let hop_count = r.u16()? as usize;
    let mut hops = Vec::with_capacity(hop_count);
    for _ in 0..hop_count {
        let nlen = r.u8()? as usize;
        let name = r.str_n(nlen)?;
        let enter = r.f64()?;
        let exit = r.f64()?;
        hops.push((name, enter, exit));
    }
    Ok(Header {
        flags,
        ts_wall,
        ts,
        seq,
        hops,
    })
}

/// Decode header + every domain field.
pub fn decode(bytes: &[u8]) -> Result<(Header, Vec<DecodedField>), WireError> {
    let mut r = Reader::new(bytes);
    let header = read_header(&mut r)?;
    let field_count = r.u16()? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let kind = r.u8()?;
        let present = r.u8()? != 0;
        let nlen = r.u8()? as usize;
        let name = r.str_n(nlen)?;
        let scalar = if present { Some(r.scalar(kind)?) } else { None };
        fields.push(DecodedField { name, kind, scalar });
    }
    Ok((header, fields))
}

/// Decode only the telemetry header (no domain fields).
pub fn decode_header_only(bytes: &[u8]) -> Result<Header, WireError> {
    let mut r = Reader::new(bytes);
    read_header(&mut r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_roundtrips() {
        let bytes = EventBuilder::new()
            .ts_wall(123.4)
            .ts(99.9)
            .seq(7)
            .str_field("sensor_id", "sensor-0001")
            .str_field("unit", "°C")
            .f64_field("value", 21.5)
            .bool_field("is_anomaly", false)
            .finish();

        assert!(is_binary(&bytes));

        let (h, fields) = decode(&bytes).unwrap();
        assert_eq!(h.ts_wall, Some(123.4));
        assert_eq!(h.ts, Some(99.9));
        assert_eq!(h.seq, Some(7));
        assert!(h.hops.is_empty());
        assert!(!h.is_eos());

        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].name, "sensor_id");
        assert_eq!(fields[0].scalar, Some(Scalar::Str("sensor-0001".into())));
        assert_eq!(fields[1].scalar, Some(Scalar::Str("°C".into())));
        assert_eq!(fields[2].scalar, Some(Scalar::F64(21.5)));
        assert_eq!(fields[3].scalar, Some(Scalar::Bool(false)));
    }

    #[test]
    fn absent_field_preserves_kind() {
        let mut out = Vec::new();
        write_header(&mut out, Some(1.0), None, None, &[], None);
        out.extend_from_slice(&1u16.to_le_bytes());
        // an absent option<u32> field: kind preserved, no payload
        write_field(&mut out, "maybe", tag::U32, None);

        let (_, fields) = decode(&out).unwrap();
        assert_eq!(fields[0].name, "maybe");
        assert_eq!(fields[0].kind, tag::U32);
        assert_eq!(fields[0].scalar, None);
    }

    #[test]
    fn header_with_hops_and_new_hop() {
        let mut out = Vec::new();
        let hops = vec![("relay#0".to_string(), 1.0, 1.5)];
        write_header(&mut out, Some(5.0), Some(6.0), Some(2), &hops, Some(("forward#1", 2.0, 2.5)));
        out.extend_from_slice(&0u16.to_le_bytes()); // no fields

        let h = decode_header_only(&out).unwrap();
        assert_eq!(h.hops.len(), 2);
        assert_eq!(h.hops[0].0, "relay#0");
        assert_eq!(h.hops[1], ("forward#1".to_string(), 2.0, 2.5));
    }

    #[test]
    fn truncated_is_error_not_panic() {
        let bytes = EventBuilder::new().ts_wall(1.0).f64_field("v", 2.0).finish();
        for cut in 0..bytes.len() {
            // Every prefix must decode-or-error, never panic.
            let _ = decode(&bytes[..cut]);
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let err = decode(&[0x00, 0x00, 0, 0]).unwrap_err();
        assert!(matches!(err, WireError::BadMagic { .. }));
    }

    /// Byte-exact reference: this is the exact prefix the format guarantees.
    /// If this test ever changes, the wire version MUST be bumped.
    #[test]
    fn header_byte_layout_is_stable() {
        let mut out = Vec::new();
        write_header(&mut out, Some(0.0), None, Some(0), &[], None);
        // magic, version, flags=0, bitmap = ts_wall(1)|seq(4) = 0x05
        assert_eq!(out[0], 0xEB);
        assert_eq!(out[1], 0x01);
        assert_eq!(out[2], 0x00);
        assert_eq!(out[3], 0x05);
        // ts_wall f64 = 0.0  → 8 bytes of 0
        assert_eq!(&out[4..12], &[0u8; 8]);
        // seq u64 = 0 → 8 bytes of 0
        assert_eq!(&out[12..20], &[0u8; 8]);
        // hop_count u16 = 0
        assert_eq!(&out[20..22], &[0u8, 0u8]);
    }
}
