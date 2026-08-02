// Stage: tailup — identity pass-through.
//
// One arm of the final broadcast; feeds terminal.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn tailup(ev: Msg) -> Msg {
        ev
    }
}
