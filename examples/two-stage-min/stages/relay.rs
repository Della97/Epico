// Stage: relay — identity pass-through, minimal compute.
//
// The function body is a move of the record; all measurable per-event cost is
// the fixed stage boundary (decode JSON -> WIT record -> call -> encode), which
// is identical across transport arms and therefore cancels in the comparison.

use epico_sdk::stage;

stage! {
    fn relay(ev: Event) -> Event {
        ev
    }
}
