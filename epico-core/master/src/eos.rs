//! Stage-level EOS barrier (roadmap M0).
//!
//! EOS used to be a data-plane event: whichever worker happened to receive the
//! marker forwarded it downstream and exited. That leaves sibling replicas
//! running, lets the marker overtake their in-flight events (TODO 1.5), and
//! breaks outright on fan-in (the first finished branch would shut the run
//! down). This module inverts it into a control-plane signal:
//!
//! **A stage forwards EOS only when the stage as a whole is finished** —
//! all `expected_in` upstream markers seen AND every replica drained.
//!
//! Protocol (one barrier per stage, shared by autoscaler + workers):
//!   1. A worker that decodes an EOS marker calls [`report`] (payload stored
//!      first-wins, forwarded verbatim so collector accounting keys survive)
//!      and simply keeps processing.
//!   2. The autoscaler tick observes `all_markers_seen()` and transitions the
//!      stage to *finishing* ([`begin_finishing`]): it raises every worker's
//!      drain flag and stops spawning. (On the zmq path it first waits for
//!      the dispatcher queue to empty — drained workers stop taking credits.)
//!   3. Workers drain their residue (mesh column close / shared-ring
//!      cooperative drain) and exit; each calls [`worker_finished`].
//!   4. The LAST worker out gets the marker back from `worker_finished` and
//!      forwards it through its own output handle — emission stays on a
//!      thread that owns a valid producer slot, and by then no data event
//!      exists behind the marker anywhere in the stage.
//!
//! `expected_in` is 1 for linear pipelines; DAG fan-in (M1) sets it to the
//! stage's in-degree and adds the marker merge rule (sum of per-branch sent
//! counts).
//!
//! Known edge (accepted for M0): if every worker of a finishing stage exits
//! through an error path before the last normal drain, the marker has no
//! forwarding thread and the run falls back to the CLI's EOS-drain timeout.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use bytes::Bytes;

pub(crate) struct StageEosBarrier {
    expected_in:  usize,
    seen:         AtomicUsize,
    finishing:    AtomicBool,
    live_workers: AtomicUsize,
    marker:       Mutex<Option<Bytes>>,
}

impl StageEosBarrier {
    pub fn new(expected_in: usize) -> Self {
        StageEosBarrier {
            expected_in:  expected_in.max(1),
            seen:         AtomicUsize::new(0),
            finishing:    AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
            marker:       Mutex::new(None),
        }
    }

    /// A worker decoded an EOS marker on this stage's input. Counts it toward
    /// `expected_in` and folds its payload into the stage's outgoing marker.
    ///
    /// With in-degree 1 the payload passes through byte-for-byte. With fan-in
    /// the branch markers are MERGED (see [`merge_markers`]) — first-wins would
    /// silently under-report the event count the moment a stage has more than
    /// one upstream branch.
    pub fn report(&self, marker: Bytes) {
        if let Ok(mut slot) = self.marker.lock() {
            *slot = Some(match slot.take() {
                None           => marker,
                Some(existing) => merge_markers(&existing, &marker),
            });
        }
        self.seen.fetch_add(1, Ordering::SeqCst);
    }

    /// True once every expected upstream marker has arrived.
    pub fn all_markers_seen(&self) -> bool {
        self.seen.load(Ordering::SeqCst) >= self.expected_in
    }

    /// Transition to finishing. Returns true exactly once (CAS), so the
    /// caller that wins performs the raise-all-drains side effect.
    pub fn begin_finishing(&self) -> bool {
        !self.finishing.swap(true, Ordering::SeqCst)
    }

    pub fn is_finishing(&self) -> bool {
        self.finishing.load(Ordering::SeqCst)
    }

    /// Called by a worker after its boot succeeded, before its event loop.
    pub fn worker_started(&self) {
        self.live_workers.fetch_add(1, Ordering::SeqCst);
    }

    /// Called by a worker at event-loop exit (any reason: EOS drain,
    /// scale-down, error). If the stage is finishing and this was the last
    /// live worker, the stored marker is handed back — the caller must
    /// forward it downstream before exiting.
    pub fn worker_finished(&self) -> Option<Bytes> {
        let was_last = self.live_workers.fetch_sub(1, Ordering::SeqCst) == 1;
        if was_last && self.is_finishing() {
            self.marker.lock().ok().and_then(|mut m| m.take())
        } else {
            None
        }
    }
}

/// Fold two EOS markers from different upstream branches into one.
///
/// Counters (`loadgen_sent`, `expected_count`) SUM: each branch reports what
/// flowed down it, and under broadcast an event legitimately arrives on more
/// than one branch, so the total is what a fan-in stage forwards. The done
/// timestamp takes the later of the two. Anything unparseable keeps the first
/// marker rather than dropping the run's end signal.
fn merge_markers(a: &[u8], b: &[u8]) -> Bytes {
    let (Ok(mut va), Ok(vb)) = (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) else {
        return Bytes::copy_from_slice(a);
    };
    let Some(obj) = va.as_object_mut() else { return Bytes::copy_from_slice(a) };

    for key in ["loadgen_sent", "expected_count"] {
        let sum = obj.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
            + vb.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
        if sum > 0 {
            obj.insert(key.to_string(), serde_json::json!(sum));
        }
    }
    let ts = f64::max(
        obj.get("loadgen_done_ts").and_then(|v| v.as_f64()).unwrap_or(0.0),
        vb.get("loadgen_done_ts").and_then(|v| v.as_f64()).unwrap_or(0.0),
    );
    if ts > 0.0 {
        obj.insert("loadgen_done_ts".to_string(), serde_json::json!(ts));
    }
    serde_json::to_vec(&va)
        .map(Bytes::from)
        .unwrap_or_else(|_| Bytes::copy_from_slice(a))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> Bytes {
        Bytes::from_static(b"{\"__epico_eos\":true,\"loadgen_sent\":42}")
    }

    #[test]
    fn last_worker_out_gets_the_marker() {
        let b = StageEosBarrier::new(1);
        b.worker_started();
        b.worker_started();
        b.worker_started();

        b.report(marker());
        assert!(b.all_markers_seen());
        assert!(b.begin_finishing());
        assert!(!b.begin_finishing(), "finishing transition must be once-only");

        assert_eq!(b.worker_finished(), None);
        assert_eq!(b.worker_finished(), None);
        let m = b.worker_finished().expect("last worker must get the marker");
        assert_eq!(m, marker());
    }

    #[test]
    fn marker_payload_is_first_wins_and_taken_once() {
        let b = StageEosBarrier::new(1);
        b.worker_started();
        b.report(marker());
        b.report(Bytes::from_static(b"{\"__epico_eos\":true,\"late\":true}"));
        b.begin_finishing();
        assert_eq!(b.worker_finished(), Some(marker()));
        // A hypothetical extra exit cannot yield a second marker.
        b.worker_started();
        assert_eq!(b.worker_finished(), None);
    }

    /// Linear pipelines must be untouched: one marker in, same bytes out.
    #[test]
    fn single_marker_passes_through_verbatim() {
        let b = StageEosBarrier::new(1);
        b.worker_started();
        b.report(marker());
        b.begin_finishing();
        assert_eq!(b.worker_finished(), Some(marker()));
    }

    /// Fan-in: branch counts sum, so the forwarded marker describes the whole
    /// stage rather than whichever branch happened to arrive first.
    #[test]
    fn fan_in_markers_merge_counts() {
        let b = StageEosBarrier::new(2);
        b.worker_started();
        b.report(Bytes::from_static(
            b"{\"__epico_eos\":true,\"loadgen_sent\":100,\"expected_count\":100,\"loadgen_done_ts\":5.0}",
        ));
        b.report(Bytes::from_static(
            b"{\"__epico_eos\":true,\"loadgen_sent\":50,\"expected_count\":50,\"loadgen_done_ts\":7.5}",
        ));
        assert!(b.all_markers_seen());
        b.begin_finishing();

        let merged = b.worker_finished().expect("merged marker");
        let v: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert_eq!(v["__epico_eos"], serde_json::json!(true));
        assert_eq!(v["loadgen_sent"].as_u64(), Some(150));
        assert_eq!(v["expected_count"].as_u64(), Some(150));
        assert_eq!(v["loadgen_done_ts"].as_f64(), Some(7.5)); // later of the two
    }

    #[test]
    fn fan_in_waits_for_all_branches() {
        let b = StageEosBarrier::new(3); // DAG M1 shape
        b.worker_started();
        b.report(marker());
        b.report(marker());
        assert!(!b.all_markers_seen(), "2 of 3 branches must not finish the stage");
        b.report(marker());
        assert!(b.all_markers_seen());
    }

    #[test]
    fn non_finishing_exits_do_not_release_the_marker() {
        // A scale-down drain (not EOS) must never forward a marker.
        let b = StageEosBarrier::new(1);
        b.worker_started();
        b.worker_started();
        b.report(marker());
        // No begin_finishing yet: even a last-worker exit yields nothing.
        assert_eq!(b.worker_finished(), None);
        assert_eq!(b.worker_finished(), None);
    }
}
