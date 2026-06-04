//! Native in-process sink for the three-stage pipeline.
//!
//! Runs on the collector thread; `consume` is called once per finished event
//! (after the EOS marker is filtered and before host-side e2e accounting). It
//! must stay cheap, or the sink becomes the bottleneck and caps throughput the
//! same way a slow source does. Here it just counts and logs occasionally —
//! swap in file/socket writes as needed, but keep the hot path light.

use epico_master::EventSink;

pub struct Sink {
    n: u64,
}

impl Sink {
    pub fn new() -> Self {
        Sink { n: 0 }
    }
}

impl EventSink for Sink {
    fn consume(&mut self, event: &[u8]) {
        self.n += 1;
        // Touch the payload so the parameter is used without real work.
        let _ = event.len();
        if self.n % 1_000_000 == 0 {
            eprintln!("[sink] consumed {} events", self.n);
        }
    }
}