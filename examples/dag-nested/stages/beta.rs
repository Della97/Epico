// Stage: beta — identity pass-through.
//
// The other nested fan-out: one input from ingest, broadcast to
// betaone and betatwo.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn beta(ev: Msg) -> Msg {
        ev
    }
}
