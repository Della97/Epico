// Stage: joinright — identity pass-through.
//
// 2-way fan-in (betaone, betatwo), sharing betatwo with joinleft.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn joinright(ev: Msg) -> Msg {
        ev
    }
}
