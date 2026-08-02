// Stage: fuse — identity pass-through.
//
// Fan-in of fan-ins AND fan-out: in-degree 2 (joinleft, joinright)
// and out-degree 2 (tailup, taildown) on the same stage.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn fuse(ev: Msg) -> Msg {
        ev
    }
}
