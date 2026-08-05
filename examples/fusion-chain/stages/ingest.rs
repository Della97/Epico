// Stage: ingest — the head of the chain, deliberately untouched by fusion.
//
// Its job is to keep `normalize` from being the entry stage, so the contracted
// edge `normalize -> detect` sits in the pipeline INTERIOR where a real
// stage-to-stage transport (spsc mesh / mpmc ring) carries it. Fusing an entry
// stage would delete an edge that the ingress path, not the interior fabric,
// was paying for — a different measurement.

use epico_sdk::stage;

stage! {
    fn ingest(ev: Reading) -> Reading {
        ev
    }
}
