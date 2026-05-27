//! # epico-sdk
//!
//! Developer SDK for epico stage functions. One macro, no boilerplate.
//!
//! ## Quick example
//!
//! ```no_run
//! use epico_sdk::stage;
//!
//! stage! {
//!     fn normalize(ev: Reading) -> Reading {
//!         Reading { value: ev.value.clamp(-50.0, 150.0), ..ev }
//!     }
//! }
//! ```
//!
//! ## Function shapes
//!
//! **Shape 1: `fn(ev: InType) -> OutType`**
//! Bench-ctx is auto-forwarded. Use when your stage only transforms
//! the domain event.
//!
//! **Shape 2: `fn(ev: InType, bench: BenchCtx) -> (OutType, BenchCtx)`**
//! Full access to bench context. Use when you need to read timing data.
//!
//! **Legacy shape: `fn(ev) -> Event`**
//! For stages using the all-optional Event record (Path A compatibility).
//! No explicit type annotation needed — `Event` is assumed.

/// Declare a stage. Expands into the full wit-bindgen + Guest + export
/// scaffolding. The user writes only the transformation body.
#[macro_export]
macro_rules! stage {
    // ── Shape 1a: fn(ev: InType) -> OutType (typed, bench auto-forward) ──
    (
        fn $name:ident ( $ev:ident : $in_ty:ident ) -> $out_ty:ident $body:block
    ) => {
        $crate::__stage_common!();

        fn $name($ev: $in_ty) -> $out_ty $body

        struct __EpicoStage;

        impl Guest for __EpicoStage {
            fn process_event(
                ev: $in_ty,
                bench: BenchCtx,
            ) -> ($out_ty, BenchCtx) {
                ($name(ev), bench)
            }
        }

        export!(__EpicoStage);
    };

    // ── Shape 2: fn(ev: InType, bench: BenchCtx) -> (OutType, BenchCtx) ──
    (
        fn $name:ident ( $ev:ident : $in_ty:ident, $bench:ident : BenchCtx ) -> ( $out_ty:ident, BenchCtx ) $body:block
    ) => {
        $crate::__stage_common!();

        fn $name($ev: $in_ty, $bench: BenchCtx) -> ($out_ty, BenchCtx) $body

        struct __EpicoStage;

        impl Guest for __EpicoStage {
            fn process_event(
                ev: $in_ty,
                bench: BenchCtx,
            ) -> ($out_ty, BenchCtx) {
                $name(ev, bench)
            }
        }

        export!(__EpicoStage);
    };

    // ── Legacy shape 1b: fn(ev) -> Event (untyped, Path A compat) ──
    (
        fn $name:ident ( $ev:ident ) -> Event $body:block
    ) => {
        $crate::__stage_common!();

        fn $name($ev: Event) -> Event $body

        struct __EpicoStage;

        impl Guest for __EpicoStage {
            fn process_event(
                ev: Event,
                bench: BenchCtx,
            ) -> (Event, BenchCtx) {
                ($name(ev), bench)
            }
        }

        export!(__EpicoStage);
    };

    // ── Legacy shape 2b: fn(ev, bench) -> (Event, BenchCtx) ──
    (
        fn $name:ident ( $ev:ident, $bench:ident ) -> (Event, BenchCtx) $body:block
    ) => {
        $crate::__stage_common!();

        fn $name($ev: Event, $bench: BenchCtx) -> (Event, BenchCtx) $body

        struct __EpicoStage;

        impl Guest for __EpicoStage {
            fn process_event(
                ev: Event,
                bench: BenchCtx,
            ) -> (Event, BenchCtx) {
                $name(ev, bench)
            }
        }

        export!(__EpicoStage);
    };
}

/// Internal helper: calls wit-bindgen and hoists types into scope.
///
/// Uses wildcard imports from both the exports and types namespaces so
/// that whatever record names the per-stage WIT declares (Reading,
/// Enriched, Event, etc.) are visible without qualification.
#[doc(hidden)]
#[macro_export]
macro_rules! __stage_common {
    () => {
        wit_bindgen::generate!({
            path: "wit",
        });

        // Import all types from the exported process interface. This
        // includes Guest (the trait), plus all record types that appear
        // in process-event's signature (Reading, Enriched, BenchCtx, etc).
        //
        // We do NOT also glob-import from epico::pipeline::types::*
        // because wit-bindgen re-exports the same types under both paths,
        // and two glob imports of the same name cause E0659 (ambiguous).
        // The exports::* path is sufficient for everything the user needs.
        #[allow(unused_imports)]
        use exports::epico::pipeline::process::*;
    };
}