// Stage: alphaone — identity pass-through.
//
// Leaf of the alpha fan-out; feeds joinleft.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn alphaone(ev: Msg) -> Msg {
        ev
    }
}
