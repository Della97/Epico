// Stage: ingest — identity pass-through.
//
// Root of the DAG (out-degree 3): every event is copied to alpha,
// beta and gamma.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn ingest(ev: Msg) -> Msg {
        ev
    }
}
