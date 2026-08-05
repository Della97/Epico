// Stage: finalize — the tail of the chain, deliberately untouched by fusion.
//
// Keeps `detect -> finalize` as a live interior edge across the morph, so the
// summary has an UNCHANGED edge to compare the contracted one against. Without
// it, a drop in `inter_stage` after the morph could just as easily be the whole
// pipeline going quiet.

use epico_sdk::stage;

stage! {
    fn finalize(ev: Reading) -> Reading {
        ev
    }
}
