//! Wasmtime host integration — the per-worker `Store` data and the
//! engine-construction helper.
//!
//! `HostState` is what every worker carries in its `Store<HostState>`.
//! It holds the resource table plus the two WASI contexts the stages
//! can import (core WASI and wasi:http). The view trait impls are the
//! glue that makes `wasmtime_wasi::add_to_linker_sync` and
//! `wasmtime_wasi_http::add_only_http_to_linker_sync` work against it.
//!
//! `build_engine` centralizes the Wasmtime `Config` choices so there's
//! one place to tune pooling sizes, Cranelift opt level, CoW memory,
//! etc. The pool size is derived from the pipeline's aggregate
//! `max_replicas` with 20% headroom, floored at 100 slots.
//!
//! Cold-start optimizations (pooling allocator, CoW memory init,
//! Cranelift Speed opt level, parallel compilation) are compiled in
//! only when the `cold-start-opt` feature is enabled. Build with:
//!
//!   cargo build --release --features cold-start-opt
//!
//! or via the CLI:
//!
//!   epico bootstrap --cold-start-opt

#[cfg(feature = "cold-start-opt")]
use wasmtime::{InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig};
use wasmtime::Engine;
use wasmtime::{StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

// ---------------------------------------------------------------------------
// Store payload
// ---------------------------------------------------------------------------

/// Per-`Store` state carried by every worker.
///
/// Created fresh on each worker spawn; never shared across workers.
/// The `ResourceTable` is what wasmtime-wasi uses to hand resource
/// handles to guests; WASI and HTTP each need their own context
/// (they're independent subsystems).
pub struct HostState {
    pub table: ResourceTable,
    pub wasi: WasiCtx,
    pub http: WasiHttpCtx,
    /// Per-instance resource ceiling (linear memory, tables). Bounds how much
    /// a single guest can allocate so one component can't exhaust the shared
    /// host process. Hooked up per-Store via `store.limiter(|s| &mut s.limits)`.
    pub limits: StoreLimits,
}

// ---------------------------------------------------------------------------
// Guest resource & compute bounds
// ---------------------------------------------------------------------------

/// The epoch ticker advances the engine's epoch once per this many ms. Each
/// Store arms a per-call deadline in these ticks, so it doubles as the
/// resolution of the compute-interrupt timer.
pub(crate) const EPOCH_TICK_MS: u64 = 1;

/// Per-call CPU budget, in epoch ticks (≈ ms). A single guest invocation that
/// runs longer than this is interrupted with a trap, so a runaway guest can't
/// pin a worker thread forever in the shared-process model. Generous enough
/// that well-behaved stream stages never approach it.
pub(crate) const MAX_CALL_EPOCH_TICKS: u64 = 1_000;

/// Default per-instance resource ceiling. Caps linear-memory and table growth
/// with ample headroom for legitimate stages — the point is to bound the worst
/// case (a memory-bomb guest OOM-killing the host), not to constrain normal use.
pub(crate) fn default_store_limits() -> StoreLimits {
    StoreLimitsBuilder::new()
        .memory_size(256 * 1024 * 1024) // 256 MiB per instance
        .table_elements(1_000_000)
        .build()
}

impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

// ---------------------------------------------------------------------------
// Engine construction
// ---------------------------------------------------------------------------

/// Minimal WAT component compiled at engine creation to force Cranelift's
/// lazy per-engine state to materialize. Without this, the first call to
/// `Component::from_file` for any stage pays a one-time Cranelift setup
/// cost (~10-15 ms) for register allocator pools, the engine's code-page
/// allocator, parallel compilation worker pool, and various relocation
/// tables. Subsequent compilations reuse all of that state and run in the
/// steady-state ~10 ms.
///
/// We deliberately keep the warmup module microscopic — we want to amortize
/// Cranelift's per-engine init, not arbitrary per-function compilation
/// work. A core module with a single empty function inside a component
/// wrapper is enough to traverse the same compilation path that real
/// stages take.
///
/// The compiled `Component` is dropped immediately after compilation;
/// only the side effect on the engine's internal state matters.
const ENGINE_WARMUP_WAT: &str = r#"
(component
  (core module
    (func (export "noop"))
  )
)
"#;

/// Build the shared Wasmtime `Engine`.
///
/// One engine is reused across every stage and every worker. JIT output
/// is cached in the engine, so two stages built from the same component
/// compile once.
///
/// `total_max_replicas` is the sum of every stage's `max_replicas`.
///
/// When the `cold-start-opt` feature is enabled:
///   - Cranelift opt level: Speed
///   - Parallel compilation: on
///   - CoW memory init: on
///   - Pooling allocator: capacity = ceil(total_max_replicas * 1.2), min 100
///
/// Without the feature all of those are disabled and Wasmtime uses its
/// defaults (OnDemand allocator, None opt level, no CoW, single-threaded
/// compilation). This gives a clean baseline for cold-start benchmarking.
///
/// After the engine is built, we eagerly compile a tiny throwaway
/// component to populate Cranelift's lazy internal state. This shifts
/// the first-compile penalty out of the user-visible cold-start window
/// and into agent startup. Without this step, whichever real stage
/// compiles first absorbs ~13 ms of extra time that the others don't
/// pay — visible as a misleading "compile_ms" outlier for the leading
/// stage in the decomposition figures.
pub(crate) fn build_engine(total_max_replicas: usize) -> Engine {
    #[cfg(feature = "cold-start-opt")]
    eprintln!("[epico] cold-start-opt: ENABLED");
    #[cfg(not(feature = "cold-start-opt"))]
    eprintln!("[epico] cold-start-opt: DISABLED");

    let mut wasm_config = wasmtime::Config::new();
    wasm_config.wasm_component_model(true);
    // Enable cooperative compute interruption: combined with the epoch ticker
    // thread (spawned below) and a per-call deadline on each Store, this lets a
    // runaway guest be trapped instead of pinning its worker thread. Applies to
    // both the AOT and JIT paths so they share the same isolation guarantees.
    wasm_config.epoch_interruption(true);

    #[cfg(feature = "cold-start-opt")]
    {
        // Components may contain multiple inner modules (user code + wasi
        // adapter + wasi-http adapter). Size core/memory/table slots with
        // headroom for up to 4 modules per component.
        let component_slots = ((total_max_replicas as f64) * 1.2).ceil() as u32;
        let component_slots = component_slots.max(100);
        let core_slots = component_slots * 4;

        wasm_config.cranelift_opt_level(OptLevel::Speed);
        wasm_config.parallel_compilation(true);
        wasm_config.memory_init_cow(true);

        let mut pool = PoolingAllocationConfig::default();
        pool.total_core_instances(core_slots);
        pool.total_memories(core_slots);
        pool.total_tables(core_slots);
        pool.table_elements(10_000);
        pool.total_component_instances(component_slots);
        wasm_config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
    }

    #[cfg(not(feature = "cold-start-opt"))]
    {
        // Explicit baseline: disable every optimization so measurements
        // are not contaminated by partial defaults.
        use wasmtime::OptLevel;
        wasm_config.cranelift_opt_level(OptLevel::None);
        wasm_config.parallel_compilation(false);
        wasm_config.memory_init_cow(false);
        // allocation_strategy defaults to OnDemand — no pool needed.
        let _ = total_max_replicas;
    }

    let engine = Engine::new(&wasm_config)
        .expect("Failed to create Wasmtime engine");

    // ── Cranelift warmup ──────────────────────────────────────────────────────
    // Compile a microscopic throwaway component to populate Cranelift's
    // lazy per-engine state. Drop the result immediately — we only need
    // the side effect on engine internals. Non-fatal on failure: if the
    // warmup compile somehow errors, we log and continue rather than
    // refusing to start.
    let t_warm = std::time::Instant::now();
    match wasmtime::component::Component::new(&engine, ENGINE_WARMUP_WAT) {
        Ok(_throwaway) => {
            eprintln!(
                "[engine] cranelift warmup complete in {:.2} ms",
                t_warm.elapsed().as_secs_f64() * 1000.0
            );
        }
        Err(e) => {
            eprintln!(
                "[engine] cranelift warmup failed (continuing without it): {}",
                e
            );
        }
    }

    // Epoch ticker: advances the engine's epoch once per EPOCH_TICK_MS so the
    // per-call deadlines armed on each worker Store actually fire. Detached
    // daemon for the life of the process; ~1k wakeups/s, negligible cost. The
    // Engine is Arc-backed, so the clone just shares the same engine.
    {
        let ticker_engine = engine.clone();
        std::thread::Builder::new()
            .name("epoch-ticker".into())
            .spawn(move || loop {
                std::thread::sleep(std::time::Duration::from_millis(EPOCH_TICK_MS));
                ticker_engine.increment_epoch();
            })
            .expect("failed to spawn epoch ticker");
    }

    engine
}