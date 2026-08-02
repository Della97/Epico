// Stage: alpha — identity pass-through.
//
// A fan-out nested inside a fan-out branch: one input from ingest,
// broadcast to alphaone and alphatwo.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn alpha(ev: Msg) -> Msg {
        ev
    }
}
