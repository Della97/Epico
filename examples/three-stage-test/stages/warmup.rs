// Stage: warmup — identity pass-through to absorb first-stage setup costs.
//
// This stage exists so that `normalize` is no longer the first stage in
// the pipeline. The first stage of any agent pays one-time costs that
// aren't repeated for subsequent stages:
//   - kernel page-cache miss on the .wasm/.cwasm file
//   - first pthread_create's TLS + stack-guard initialization
//   - ZMQ context's I/O thread spawn
//   - first event arrival from loadgen (which itself is warming up)
//   - first heap touch pages on the worker thread
//
// By making `warmup` absorb those costs, the cold_start_ms numbers for
// `normalize`, `detect`, and `finalize` become directly comparable across
// AOT and JIT without first-stage contamination.
//
// The function is intentionally identity (no clamp, no string ops) so that
// the data section and code section are as small as possible — we don't
// want this stage's own per-replica cost to be measurably different from
// the others.

use epico_sdk::stage;

stage! {
    fn warmup(ev: Reading) -> Reading {
        ev
    }
}