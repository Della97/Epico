// Stage: forward — identity pass-through, minimal compute.

use epico_sdk::stage;

stage! {
    fn forward(ev: Msg) -> Msg {
        ev
    }
}
