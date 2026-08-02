// Stage: left — identity pass-through.
//
// The diamond example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn left(ev: Msg) -> Msg {
        ev
    }
}
