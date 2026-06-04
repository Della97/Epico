//! Default agent binary.
//!
//! Runs the agent with no compiled-in source or sink: the source is the PULL
//! ingress (external producer) or the built-in generator when
//! `EPICO_SOURCE_GEN=1`, and the sink is the host collector alone. A generated
//! per-pipeline binary instead calls `epico_master::run_agent` with a native
//! `EventSource`/`EventSink` compiled from the pipeline's `source:`/`sink:`
//! declarations (option A).

fn main() {
    epico_master::run_agent(epico_master::parse_args(), None, None);
}