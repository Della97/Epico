//! In-process pipeline edges.
//!
//! Replaces the per-stage zmq dispatcher hop between two *co-located* stages
//! with a bounded, lock-free MPMC ring shared directly between the producer
//! stage's worker threads and the consumer stage's worker threads.
//!
//! Why this exists: profiling showed the dispatcher app-thread spends ~66% of
//! its time blocked in zmq's signaler waiting on the credit round-trip, capping
//! a stage at ~130k ev/s while every worker sat <20% utilised. Because all
//! workers are already threads in one master process (WASM is the isolation
//! boundary, not the OS process), an intra-host edge never needs to cross the
//! kernel at all. This ring removes the round-trip entirely:
//!
//!   * Backpressure is the ring's capacity — a full ring makes the producer
//!     wait. That replaces the credit_window + refill protocol (and the
//!     signaler round-trip that came with it) with a single shared bound.
//!   * Load-balancing is the MPMC pop — the next idle consumer takes the next
//!     event (work-stealing), replacing the dispatcher's ROUTER fan-out.
//!   * No serialization is forced by the transport: the event is moved as a
//!     shared byte buffer (`Bytes`); nothing is re-encoded to cross the edge.
//!
//! The wait path uses spin → yield → short-sleep backoff so an idle edge does
//! not burn a core — i.e. it does not reintroduce the busy-poll cost that the
//! zmq dispatcher paid with poll_timeout_ms=0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use bytes::Bytes;

/// A single in-process edge between two stages.
///
/// Cheap to `clone` — clones share the same underlying ring (it's an
/// `Arc<ArrayQueue>`), so every producer worker and every consumer worker on
/// the edge holds its own `Edge` handle pointing at one ring.
#[derive(Clone)]
pub(crate) struct Edge {
    ring: Arc<ArrayQueue<Bytes>>,
}

impl Edge {
    /// Create an edge with room for `capacity` in-flight events. This is the
    /// edge's flow-control window — the analogue of `credit_window` on the zmq
    /// path, but a single shared bound rather than per-worker credits. Sized at
    /// least 1 so the ring is always usable.
    pub fn new(capacity: usize) -> Self {
        Edge {
            ring: Arc::new(ArrayQueue::new(capacity.max(1))),
        }
    }

    /// Current occupancy. This is the scaling signal for the consumer stage —
    /// it stands in for the dispatcher queue depth the autoscaler used to read.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Maximum occupancy (the configured capacity).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    /// Producer side. Enqueue one event, applying backpressure: if the ring is
    /// full, wait (spin → yield → short sleep) until a slot frees up. This is
    /// what stops a fast upstream stage from outrunning a slow downstream one,
    /// and it is the only flow control the edge needs.
    ///
    /// Returns `false` if `drain` is raised while waiting — i.e. the pipeline is
    /// shutting down and the event should be dropped rather than block forever.
    #[inline]
    pub fn push(&self, item: Bytes, drain: &AtomicBool) -> bool {
        // Fast path: room immediately.
        let mut pending = match self.ring.push(item) {
            Ok(()) => return true,
            Err(returned) => returned, // ring full; `returned` is our item back
        };
        let mut backoff = Backoff::new();
        loop {
            if drain.load(Ordering::Relaxed) {
                return false;
            }
            backoff.wait();
            match self.ring.push(pending) {
                Ok(()) => return true,
                Err(returned) => pending = returned,
            }
        }
    }

    /// Consumer side. Dequeue one event, waiting (spin → yield → short sleep)
    /// until one is available. Returns `None` if `drain` is raised while the
    /// ring is empty (clean shutdown).
    #[inline]
    pub fn pop(&self, drain: &AtomicBool) -> Option<Bytes> {
        // Fast path: item immediately.
        if let Some(item) = self.ring.pop() {
            return Some(item);
        }
        let mut backoff = Backoff::new();
        loop {
            if drain.load(Ordering::Relaxed) {
                // One last look so we don't drop an event that landed during
                // the shutdown window.
                return self.ring.pop();
            }
            backoff.wait();
            if let Some(item) = self.ring.pop() {
                return Some(item);
            }
        }
    }

    /// Non-blocking dequeue — used when a consumer wants to interleave queue
    /// draining with other work without committing to a blocking wait.
    #[inline]
    pub fn try_pop(&self) -> Option<Bytes> {
        self.ring.pop()
    }
}

/// Adaptive backoff for a contended/empty ring.
///
/// Stays in a tight `spin_loop` for the first handful of iterations (lowest
/// latency when the peer is about to make progress), then `yield_now` to give
/// the scheduler a chance, then falls back to a short sleep so a genuinely idle
/// edge costs ~0% CPU instead of pinning a core. The thresholds are deliberately
/// small: under load the ring is rarely empty/full for more than a few spins.
struct Backoff {
    step: u32,
}

impl Backoff {
    #[inline]
    fn new() -> Self {
        Backoff { step: 0 }
    }

    #[inline]
    fn wait(&mut self) {
        if self.step < SPIN_LIMIT {
            for _ in 0..(1u32 << self.step.min(6)) {
                std::hint::spin_loop();
            }
        } else if self.step < YIELD_LIMIT {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(SLEEP_US));
        }
        self.step = self.step.saturating_add(1);
    }
}

const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 16;
const SLEEP_US: u64 = 50;
