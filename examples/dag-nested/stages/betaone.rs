// Stage: betaone — identity pass-through.
//
// Leaf of the beta fan-out; feeds joinright only.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn betaone(ev: Msg) -> Msg {
        ev
    }
}
