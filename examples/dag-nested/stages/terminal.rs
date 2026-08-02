// Stage: terminal — identity pass-through.
//
// The only out-degree-0 stage. It fans in tailup and taildown and
// pushes to the collector, so exactly one merged EOS marker ends the run.
//
// The nested-DAG example exercises TOPOLOGY, not compute: every stage is an
// identity so any difference in event counts is attributable to fan-out /
// fan-in behaviour alone.

use epico_sdk::stage;

stage! {
    fn terminal(ev: Msg) -> Msg {
        ev
    }
}
