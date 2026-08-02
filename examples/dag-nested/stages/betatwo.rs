// Stage: betatwo — identity pass-through.
//
// The cross edge: feeds BOTH joinright and joinleft, so two separate
// fan-in barriers share one upstream stage.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn betatwo(ev: Msg) -> Msg {
        ev
    }
}
