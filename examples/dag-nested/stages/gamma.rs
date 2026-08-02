// Stage: gamma — identity pass-through.
//
// The short branch. It skips a level and lands straight on joinleft,
// so that barrier's markers arrive from paths of unequal depth.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn gamma(ev: Msg) -> Msg {
        ev
    }
}
