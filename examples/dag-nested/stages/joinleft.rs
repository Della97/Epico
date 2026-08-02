// Stage: joinleft — identity pass-through.
//
// 4-way fan-in (gamma, alphaone, alphatwo, betatwo) over upstreams
// with different max_replicas — the widest barrier in the example.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn joinleft(ev: Msg) -> Msg {
        ev
    }
}
