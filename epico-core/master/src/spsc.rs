//! FastFlow-style SPSC transport for Epico local edges.
//!
//! Two pieces:
//!   * `Swsr`     -- a bounded, lock-free single-producer/single-consumer ring,
//!                   a Rust port of FastFlow's `SWSR_Ptr_Buffer`. No CAS, no
//!                   shared atomic counter: the producer owns `pwrite`, the
//!                   consumer owns `pread`, and the only cross-thread sync is a
//!                   per-slot state byte (the analogue of FastFlow's NULL-slot
//!                   sentinel). Payload is moved in/out inline (no per-event
//!                   allocation), so this is a fair cost comparison against
//!                   crossbeam's ArrayQueue.
//!   * `SpscMesh` -- an N-producer x M-consumer edge built from N*M Swsr rings
//!                   (the FastFlow "all-to-all" idea). Producer i round-robins
//!                   across its M outgoing rings; consumer j round-robins across
//!                   its N incoming rings. Because every ring has exactly one
//!                   writer and one reader, there is NO contended atomic on the
//!                   hot path -- which is the whole point versus a single shared
//!                   MPMC ring whose head/tail bounce between cores.
//!
//! SAFETY CONTRACT: for each ring, `push` must only ever be called from one
//! thread and `pop` from one thread. `SpscMesh` enforces this by indexing:
//! producer i only ever touches column i, consumer j only ever touches row j.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crate::inproc::Edge;

const EMPTY: u8 = 0;
const FULL: u8 = 1;

/// Force a field onto its own cache line to kill false sharing between the
/// producer-owned write index and the consumer-owned read index.
#[repr(align(64))]
struct CachePad<T>(T);

struct Slot {
    state: AtomicU8,
    val: UnsafeCell<MaybeUninit<Bytes>>,
}

/// Bounded lock-free SPSC ring. One producer, one consumer. Capacity `size`.
pub struct Swsr {
    slots: Box<[Slot]>,
    size: usize,
    pwrite: CachePad<UnsafeCell<usize>>, // producer-owned
    pread: CachePad<UnsafeCell<usize>>,  // consumer-owned
}

// The per-slot state byte provides all cross-thread ordering; the indices are
// each touched by exactly one thread. Safe to share by reference under the
// single-producer/single-consumer contract above.
unsafe impl Send for Swsr {}
unsafe impl Sync for Swsr {}

impl Swsr {
    pub fn new(size: usize) -> Self {
        assert!(size >= 1);
        let slots = (0..size)
            .map(|_| Slot {
                state: AtomicU8::new(EMPTY),
                val: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Swsr {
            slots,
            size,
            pwrite: CachePad(UnsafeCell::new(0)),
            pread: CachePad(UnsafeCell::new(0)),
        }
    }

    #[inline]
    fn next(&self, i: usize) -> usize {
        let n = i + 1;
        if n >= self.size {
            0
        } else {
            n
        }
    }

    /// Producer side. Returns Err(item) if the ring is full (backpressure).
    #[inline]
    pub fn push(&self, item: Bytes) -> Result<(), Bytes> {
        let w = unsafe { *self.pwrite.0.get() };
        let slot = &self.slots[w];
        // Acquire: if EMPTY, the consumer's prior read of this slot has
        // completed, so it's safe to overwrite.
        if slot.state.load(Ordering::Acquire) != EMPTY {
            return Err(item); // full
        }
        unsafe {
            (*slot.val.get()).write(item);
        }
        // Release: publish the payload write before marking FULL.
        slot.state.store(FULL, Ordering::Release);
        unsafe {
            *self.pwrite.0.get() = self.next(w);
        }
        Ok(())
    }

    /// Consumer side. Returns None if the ring is empty.
    #[inline]
    pub fn pop(&self) -> Option<Bytes> {
        let r = unsafe { *self.pread.0.get() };
        let slot = &self.slots[r];
        // Acquire: if FULL, the producer's payload write is visible.
        if slot.state.load(Ordering::Acquire) != FULL {
            return None; // empty
        }
        let item = unsafe { (*slot.val.get()).assume_init_read() };
        // Release: mark EMPTY only after we've taken the value.
        slot.state.store(EMPTY, Ordering::Release);
        unsafe {
            *self.pread.0.get() = self.next(r);
        }
        Some(item)
    }
}

impl Drop for Swsr {
    fn drop(&mut self) {
        // Reclaim any payloads still parked in FULL slots.
        for slot in self.slots.iter() {
            if slot.state.load(Ordering::Acquire) == FULL {
                unsafe {
                    let _ = (*slot.val.get()).assume_init_read();
                }
            }
        }
    }
}

/// N-producer x M-consumer edge: a mesh of N*M SPSC rings, FastFlow A2A style.
/// `rings[i * n_cons + j]` carries producer i -> consumer j.
pub struct SpscMesh {
    n_prod: usize,
    n_cons: usize,
    rings: Box<[Swsr]>,
    prod_cursor: Box<[CachePad<UnsafeCell<usize>>]>, // per producer: next consumer to try
    cons_cursor: Box<[CachePad<UnsafeCell<usize>>]>, // per consumer: next producer to poll
}

unsafe impl Send for SpscMesh {}
unsafe impl Sync for SpscMesh {}

impl SpscMesh {
    pub fn new(n_prod: usize, n_cons: usize, per_ring_cap: usize) -> Self {
        assert!(n_prod >= 1 && n_cons >= 1);
        let rings = (0..n_prod * n_cons)
            .map(|_| Swsr::new(per_ring_cap))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let prod_cursor = (0..n_prod)
            .map(|_| CachePad(UnsafeCell::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let cons_cursor = (0..n_cons)
            .map(|_| CachePad(UnsafeCell::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SpscMesh {
            n_prod,
            n_cons,
            rings,
            prod_cursor,
            cons_cursor,
        }
    }

    pub fn n_prod(&self) -> usize {
        self.n_prod
    }
    pub fn n_cons(&self) -> usize {
        self.n_cons
    }

    /// Producer `pi` enqueues `item`. Round-robins across its M outgoing rings,
    /// trying the next consumer if the chosen one is full (spreads load / steals
    /// around a slow consumer). Returns Err(item) only if ALL M rings are full,
    /// which is the edge-level backpressure signal -- caller should back off.
    #[inline]
    pub fn push(&self, pi: usize, item: Bytes) -> Result<(), Bytes> {
        debug_assert!(pi < self.n_prod);
        let cur = unsafe { &mut *self.prod_cursor[pi].0.get() };
        let mut item = item;
        for k in 0..self.n_cons {
            let j = (*cur + k) % self.n_cons;
            match self.rings[pi * self.n_cons + j].push(item) {
                Ok(()) => {
                    *cur = if j + 1 >= self.n_cons { 0 } else { j + 1 };
                    return Ok(());
                }
                Err(returned) => item = returned, // that ring full, try next consumer
            }
        }
        Err(item) // every outgoing ring full
    }

    /// Consumer `ci` dequeues one item. Round-robins across its N incoming rings
    /// for fairness across producers. Returns None if ALL N are empty.
    #[inline]
    pub fn pop(&self, ci: usize) -> Option<Bytes> {
        debug_assert!(ci < self.n_cons);
        let cur = unsafe { &mut *self.cons_cursor[ci].0.get() };
        for k in 0..self.n_prod {
            let i = (*cur + k) % self.n_prod;
            if let Some(item) = self.rings[i * self.n_cons + ci].pop() {
                *cur = if i + 1 >= self.n_prod { 0 } else { i + 1 };
                return Some(item);
            }
        }
        None
    }

    /// Total occupancy across all rings -- the SpscMesh analogue of the bounded
    /// ring's `len()`, for feeding the autoscaler's queue-depth signal.
    pub fn len(&self) -> usize {
        self.rings
            .iter()
            .map(|r| {
                r.slots
                    .iter()
                    .filter(|s| s.state.load(Ordering::Relaxed) == FULL)
                    .count()
            })
            .sum()
    }
}

// ──────────────────── blocking handles + transport abstraction ─────────────
//
// The worker calls `pop(drain)` / `push(bytes, drain)` and expects them to
// BLOCK with backoff until data/space or shutdown, exactly like `inproc::Edge`.
// We mirror Edge's spin→yield→sleep policy here so an A/B measures the queue,
// not the wait policy.

const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 16;
const SLEEP_US: u64 = 50;

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

/// Per-worker producer handle into the mesh: producer index baked in.
pub struct MeshTx {
    mesh: Arc<SpscMesh>,
    pi: usize,
}
impl MeshTx {
    pub fn new(mesh: Arc<SpscMesh>, pi: usize) -> Self {
        MeshTx { mesh, pi }
    }
    /// Blocking enqueue with backpressure. `false` if drain is raised mid-wait.
    #[inline]
    pub fn push(&self, item: Bytes, drain: &AtomicBool) -> bool {
        let mut pending = match self.mesh.push(self.pi, item) {
            Ok(()) => return true,
            Err(back) => back,
        };
        let mut backoff = Backoff::new();
        loop {
            if drain.load(Ordering::Relaxed) {
                return false;
            }
            backoff.wait();
            match self.mesh.push(self.pi, pending) {
                Ok(()) => return true,
                Err(back) => pending = back,
            }
        }
    }
}

/// Per-worker consumer handle into the mesh: consumer index baked in.
pub struct MeshRx {
    mesh: Arc<SpscMesh>,
    ci: usize,
}
impl MeshRx {
    pub fn new(mesh: Arc<SpscMesh>, ci: usize) -> Self {
        MeshRx { mesh, ci }
    }
    /// Blocking dequeue. `None` only if drain is raised while empty.
    #[inline]
    pub fn pop(&self, drain: &AtomicBool) -> Option<Bytes> {
        if let Some(item) = self.mesh.pop(self.ci) {
            return Some(item);
        }
        let mut backoff = Backoff::new();
        loop {
            if drain.load(Ordering::Relaxed) {
                return self.mesh.pop(self.ci); // last look during shutdown
            }
            backoff.wait();
            if let Some(item) = self.mesh.pop(self.ci) {
                return Some(item);
            }
        }
    }
}

/// Per-worker INPUT handle. Same surface as `Edge` so the worker loop is
/// transport-agnostic.
pub enum EdgeIn {
    Ring(Edge),
    Mesh(MeshRx),
}
impl EdgeIn {
    #[inline]
    pub fn pop(&self, drain: &AtomicBool) -> Option<Bytes> {
        match self {
            EdgeIn::Ring(e) => e.pop(drain),
            EdgeIn::Mesh(m) => m.pop(drain),
        }
    }
}

/// Per-worker OUTPUT handle.
pub enum EdgeOut {
    Ring(Edge),
    Mesh(MeshTx),
}
impl EdgeOut {
    #[inline]
    pub fn push(&self, bytes: Bytes, drain: &AtomicBool) -> bool {
        match self {
            EdgeOut::Ring(e) => e.push(bytes, drain),
            EdgeOut::Mesh(m) => m.push(bytes, drain),
        }
    }
}

/// Per-stage INPUT source. The master builds one of these per stage; the
/// autoscaler calls `for_replica(r)` when it spawns replica r. `Ring` hands
/// every replica a clone of the one shared ring; `Mesh` hands replica r its
/// own column (`ci = r`) of the N×M mesh.
#[derive(Clone)]
pub enum EdgeInSrc {
    None,
    Ring(Edge),
    Mesh(Arc<SpscMesh>),
}
impl EdgeInSrc {
    pub fn for_replica(&self, r: usize) -> Option<EdgeIn> {
        match self {
            EdgeInSrc::None => None,
            EdgeInSrc::Ring(e) => Some(EdgeIn::Ring(e.clone())),
            EdgeInSrc::Mesh(m) => Some(EdgeIn::Mesh(MeshRx::new(m.clone(), r))),
        }
    }
    /// Queue-depth signal for the autoscaler (ring occupancy / total mesh occupancy).
    pub fn len(&self) -> usize {
        match self {
            EdgeInSrc::None => 0,
            EdgeInSrc::Ring(e) => e.len(),
            EdgeInSrc::Mesh(m) => m.len(),
        }
    }
    pub fn is_some(&self) -> bool {
        !matches!(self, EdgeInSrc::None)
    }
}

/// Per-stage OUTPUT source.
#[derive(Clone)]
pub enum EdgeOutSrc {
    None,
    Ring(Edge),
    Mesh(Arc<SpscMesh>),
}
impl EdgeOutSrc {
    pub fn for_replica(&self, r: usize) -> Option<EdgeOut> {
        match self {
            EdgeOutSrc::None => None,
            EdgeOutSrc::Ring(e) => Some(EdgeOut::Ring(e.clone())),
            EdgeOutSrc::Mesh(m) => Some(EdgeOut::Mesh(MeshTx::new(m.clone(), r))),
        }
    }
    pub fn is_some(&self) -> bool {
        !matches!(self, EdgeOutSrc::None)
    }
}

// ───────────────────────────── tests ──────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as O};
    use std::thread;

    fn ev(n: u64) -> Bytes {
        Bytes::from(n.to_le_bytes().to_vec())
    }
    fn num(v: &[u8]) -> u64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&v[..8]);
        u64::from_le_bytes(b)
    }

    #[test]
    fn single_thread_fifo_and_full_empty() {
        let q = Swsr::new(4);
        assert!(q.pop().is_none());
        for i in 0..3 {
            assert!(q.push(ev(i)).is_ok());
        }
        // capacity 4 but one slot can't be distinguished? our design uses all
        // slots (state byte), so 4 pushes should succeed.
        assert!(q.push(ev(3)).is_ok());
        assert!(q.push(ev(99)).is_err()); // full
        for i in 0..4 {
            assert_eq!(num(&q.pop().unwrap()), i);
        }
        assert!(q.pop().is_none()); // empty
    }

    #[test]
    fn spsc_two_threads_in_order_no_loss() {
        let q = Arc::new(Swsr::new(64));
        const N: u64 = 2_000_000;
        let qp = q.clone();
        let prod = thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                if qp.push(ev(i)).is_ok() {
                    i += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let mut got = 0u64;
        let mut expect = 0u64;
        while got < N {
            if let Some(v) = q.pop() {
                assert_eq!(num(&v), expect, "SPSC must preserve order");
                expect += 1;
                got += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        prod.join().unwrap();
        assert_eq!(got, N);
    }

    #[test]
    fn mesh_no_loss_under_concurrency() {
        const NP: usize = 3;
        const NC: usize = 2;
        const PER: u64 = 500_000;
        let mesh = Arc::new(SpscMesh::new(NP, NC, 256));
        let received = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        // producers
        for pi in 0..NP {
            let m = mesh.clone();
            handles.push(thread::spawn(move || {
                for k in 0..PER {
                    let payload = (pi as u64) * PER + k; // globally unique
                    let mut item = ev(payload);
                    loop {
                        match m.push(pi, item) {
                            Ok(()) => break,
                            Err(back) => {
                                item = back;
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            }));
        }
        // consumers
        let total = (NP as u64) * PER;
        for ci in 0..NC {
            let m = mesh.clone();
            let recv = received.clone();
            let s = sum.clone();
            handles.push(thread::spawn(move || loop {
                if recv.load(O::Relaxed) >= total {
                    break;
                }
                if let Some(v) = m.pop(ci) {
                    s.fetch_add(num(&v), O::Relaxed);
                    recv.fetch_add(1, O::Relaxed);
                } else {
                    std::hint::spin_loop();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(O::Relaxed), total, "no events lost");
        // sum of 0..total-1 = total*(total-1)/2  -> verifies no dup/corruption
        let expect_sum = total.wrapping_mul(total - 1) / 2;
        assert_eq!(sum.load(O::Relaxed), expect_sum, "no dup/corruption");
    }
}