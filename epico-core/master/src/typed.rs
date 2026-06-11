//! Typed dispatch — the static fast path past the dynamic `Val` layer.
//!
//! The dynamic path (conversion.rs) builds `wasmtime::component::Val` trees
//! per event and calls the untyped `Func::call`, which re-typechecks every
//! invocation. This module lets the GENERATED per-pipeline agent register
//! concrete host-side types instead: codegen emits one struct per YAML record
//! (deriving `ComponentType`/`Lift`/`Lower` against the same generated WIT)
//! plus `FromEvent`/`ToEvent` impls, and registers a `TypedStage<In, Out>`
//! per stage. The worker types its already-resolved `Func` once at boot
//! (`Func::typed`) and thereafter calls through `TypedFunc` — no Vals, no
//! per-call typecheck, no per-event results Vec.
//!
//! Selection at runtime:
//!   * agent registered the stage AND `EPICO_DYNAMIC_DISPATCH` != 1 → typed
//!   * otherwise → existing dynamic path (also covers legacy all-optional
//!     `event` pipelines, which codegen deliberately does not register)
//!
//! The env flag gives a same-binary A/B: typed vs dynamic.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use bytes::Bytes;
use wasmtime::component::{ComponentNamedList, ComponentType, Func, Lift, Lower, TypedFunc};
use wasmtime::Store;

use crate::envelope::{EnvelopeFormat, EventEnvelope};
use crate::host::HostState;

// Re-export so generated agent code can name everything through one path
// (`epico_master::typed::*`) and the derive-generated `wasmtime::...` paths
// resolve without the agent declaring its own wasmtime dependency. (The
// agent's Cargo.toml pins wasmtime anyway, belt and braces — version must
// match master's or the derived traits are foreign types.)
pub use wasmtime;

/// Host-side mirror of the generated WIT `bench-ctx` record. Field order and
/// kebab-case names must match `make_typed_stage_wit` in epico-cli exactly.
#[derive(ComponentType, Lift, Lower, Clone, Debug, Default)]
#[component(record)]
pub struct BenchCtx {
    #[component(name = "bench-ts-wall")]
    pub bench_ts_wall: Option<f64>,
    #[component(name = "bench-ts")]
    pub bench_ts: Option<f64>,
    #[component(name = "bench-seq")]
    pub bench_seq: Option<u64>,
    #[component(name = "bench-hops")]
    pub bench_hops: Vec<BenchHop>,
}

/// Host-side mirror of the generated WIT `bench-hop` record.
#[derive(ComponentType, Lift, Lower, Clone, Debug)]
#[component(record)]
pub struct BenchHop {
    pub stage: String,
    #[component(name = "enter-ts")]
    pub enter_ts: f64,
    #[component(name = "exit-ts")]
    pub exit_ts: f64,
}

/// A scalar field value on its way OUT of a typed stage, format-independent.
/// `Absent` is an `option<T>` field that is `None` — omitted from JSON,
/// written with presence=0 in binary.
#[derive(Debug, Clone)]
pub enum WireValue {
    Str(String),
    F64(f64),
    F32(f32),
    U64(u64),
    U32(u32),
    S64(i64),
    S32(i32),
    Bool(bool),
    Absent,
}

/// Read access to a decoded input event, independent of wire format.
/// Implemented by `EventEnvelope`; field names are JSON/snake_case.
pub trait EventView {
    fn str_field(&self, json_name: &str) -> Option<String>;
    fn f64_field(&self, json_name: &str) -> Option<f64>;
    fn f32_field(&self, json_name: &str) -> Option<f32>;
    fn u64_field(&self, json_name: &str) -> Option<u64>;
    fn u32_field(&self, json_name: &str) -> Option<u32>;
    fn s64_field(&self, json_name: &str) -> Option<i64>;
    fn s32_field(&self, json_name: &str) -> Option<i32>;
    fn bool_field(&self, json_name: &str) -> Option<bool>;
}

/// Build the typed input struct from the decoded envelope. Generated.
pub trait FromEvent: Sized {
    fn from_event(view: &dyn EventView) -> Self;
}

/// Flatten the typed output struct to wire fields. Generated.
/// Names pushed are JSON/snake_case; order is the schema's field order.
pub trait ToEvent {
    fn to_fields(&self, out: &mut Vec<(&'static str, WireValue)>);
}

/// Per-stage factory the generated agent registers. `prepare` is called once
/// per worker at boot with that worker's `Store` and resolved `Func`.
pub trait TypedDispatch: Send + Sync {
    fn prepare(
        &self,
        store: &mut Store<HostState>,
        func: Func,
    ) -> Result<Box<dyn PreparedDispatch>>;
}

/// Per-worker prepared fast path: holds the `TypedFunc` for this worker's
/// instance and performs decode → call → encode for one event.
pub trait PreparedDispatch: Send {
    /// Returns the encoded output bytes; writes the wasm-call (enter, exit)
    /// wall timestamps into `enter_exit` for telemetry.
    fn call(
        &mut self,
        store: &mut Store<HostState>,
        env: &EventEnvelope,
        hop_label: &str,
        out_format: EnvelopeFormat,
        enter_exit: &mut (f64, f64),
    ) -> Result<Bytes>;
}

/// The generic stage implementation generated code instantiates:
/// `TypedStage::<Reading, Enriched>::new()`.
pub struct TypedStage<In, Out> {
    _p: PhantomData<fn(In) -> Out>,
}

impl<In, Out> TypedStage<In, Out> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        TypedStage { _p: PhantomData }
    }
}

impl<In, Out> TypedDispatch for TypedStage<In, Out>
where
    In: FromEvent + ComponentType + Lower + Send + Sync + 'static,
    Out: ToEvent + ComponentType + Lift + Send + Sync + 'static,
    (In, BenchCtx): ComponentNamedList + Lower,
    ((Out, BenchCtx),): ComponentNamedList + Lift,
{
    fn prepare(
        &self,
        store: &mut Store<HostState>,
        func: Func,
    ) -> Result<Box<dyn PreparedDispatch>> {
        let typed = func.typed::<(In, BenchCtx), ((Out, BenchCtx),)>(&mut *store)?;
        Ok(Box::new(PreparedTyped { typed }))
    }
}

struct PreparedTyped<In, Out>
where
    (In, BenchCtx): ComponentNamedList + Lower,
    ((Out, BenchCtx),): ComponentNamedList + Lift,
{
    typed: TypedFunc<(In, BenchCtx), ((Out, BenchCtx),)>,
}

impl<In, Out> PreparedDispatch for PreparedTyped<In, Out>
where
    In: FromEvent + ComponentType + Lower + Send + Sync + 'static,
    Out: ToEvent + ComponentType + Lift + Send + Sync + 'static,
    (In, BenchCtx): ComponentNamedList + Lower,
    ((Out, BenchCtx),): ComponentNamedList + Lift,
{
    fn call(
        &mut self,
        store: &mut Store<HostState>,
        env: &EventEnvelope,
        hop_label: &str,
        out_format: EnvelopeFormat,
        enter_exit: &mut (f64, f64),
    ) -> Result<Bytes> {
        let ev = In::from_event(env as &dyn EventView);
        let (ts_wall, ts, seq) = env.bench_scalars();
        let bench = BenchCtx {
            bench_ts_wall: ts_wall,
            bench_ts: ts,
            bench_seq: seq,
            // Same contract as the dynamic path: the guest receives an empty
            // hop list and must forward bench-ctx untouched; the host appends
            // the real hop after the call from its own timestamps.
            bench_hops: Vec::new(),
        };

        store.set_epoch_deadline(crate::host::MAX_CALL_EPOCH_TICKS);
        let enter = now_secs_f64();
        let ((out, _bench),) = self.typed.call(&mut *store, (ev, bench))?;
        self.typed.post_return(&mut *store)?;
        let exit = now_secs_f64();
        *enter_exit = (enter, exit);

        let mut fields: Vec<(&'static str, WireValue)> = Vec::with_capacity(8);
        out.to_fields(&mut fields);
        env.encode_typed(&fields, hop_label, enter, exit, out_format)
    }
}

fn now_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

// ── Registry ──────────────────────────────────────────────────────────────────

static TYPED_REGISTRY: OnceLock<HashMap<String, Arc<dyn TypedDispatch>>> = OnceLock::new();

/// Called once by the generated agent's `main`, before `run_agent`.
pub fn register(map: HashMap<String, Arc<dyn TypedDispatch>>) {
    let _ = TYPED_REGISTRY.set(map);
}

/// Worker-boot lookup. `EPICO_DYNAMIC_DISPATCH=1` forces the dynamic path
/// even when a typed dispatch is registered — the same-binary A/B switch.
pub(crate) fn lookup(stage: &str) -> Option<Arc<dyn TypedDispatch>> {
    if std::env::var("EPICO_DYNAMIC_DISPATCH").map(|v| v == "1").unwrap_or(false) {
        return None;
    }
    TYPED_REGISTRY.get().and_then(|m| m.get(stage).cloned())
}
