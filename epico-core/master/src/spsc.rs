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
    /// True while the producer is inside a push attempt on this ring. Half of
    /// the column-close handshake (Dekker pattern with `SpscMesh::closed`):
    /// the producer sets `busy`, THEN checks `closed`; the closing consumer
    /// sets `closed`, THEN waits for `!busy` before draining the residue. So a
    /// push that started before the close is fully visible to the drain, and a
    /// push that starts after sees `closed` and skips the column. Both sides'
    /// stores/loads are SeqCst so they cannot reorder past each other.
    busy: CachePad<AtomicBool>,
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
            busy: CachePad(AtomicBool::new(false)),
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
    /// Per-consumer close flag. A closed column is skipped by `push`.
    ///
    /// The invariant is: **a column with no live consumer never accepts an
    /// event.** Producers round-robin into every open column until it is full,
    /// so a column nobody is reading strands up to `n_prod × ring_cap` events
    /// forever. That is what this flag prevents, on both of its edges:
    ///
    /// * A consumer that DRAINS closes its column, so it can empty its residue
    ///   knowing nothing new will land behind it (without this, scale-down
    ///   stranded exactly 2048 = 4 × 512 events in the conservation test).
    /// * A column that has NEVER been occupied starts closed and is opened only
    ///   when a consumer binds it ([`MeshRx::new`]). Columns exist for every
    ///   index a consumer *may* one day take — `max_replicas`, widened further
    ///   for a declared fusible pair whose fused stage would consume here — and
    ///   the ones no replica has reached yet must not swallow traffic.
    closed: Box<[AtomicBool]>,
    /// Producers currently blocked in `MeshTx::push` backoff. Counted into
    /// `len()` so the consumer stage's autoscaler still sees demand when every
    /// column is closed AND empty — otherwise ring occupancy reads 0, no
    /// consumer is ever respawned to reopen a column, and the blocked
    /// producer deadlocks.
    waiting: std::sync::atomic::AtomicUsize,
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
        // Every column starts CLOSED and opens when a consumer binds it. See
        // the `closed` field docs: an unoccupied open column silently absorbs
        // events that no one will ever pop. A producer that finds every column
        // closed blocks and registers in `waiting`, which keeps `len()` above
        // zero so the consumer stage's autoscaler cold-starts a replica and
        // reopens one — the same path that recovers from an all-drained stage.
        let closed = (0..n_cons)
            .map(|_| AtomicBool::new(true))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SpscMesh {
            n_prod,
            n_cons,
            rings,
            prod_cursor,
            cons_cursor,
            closed,
            waiting: std::sync::atomic::AtomicUsize::new(0),
        }
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
            let ring = &self.rings[pi * self.n_cons + j];
            // Close handshake: declare intent (busy) BEFORE checking closed;
            // close_consumer does the mirror (set closed, then wait !busy).
            // SeqCst so the store/load pairs can't reorder — either we see
            // closed and skip, or the closer sees busy and waits us out.
            ring.busy.0.store(true, Ordering::SeqCst);
            if self.closed[j].load(Ordering::SeqCst) {
                ring.busy.0.store(false, Ordering::Release);
                continue; // column closed, try the next consumer
            }
            let res = ring.push(item);
            ring.busy.0.store(false, Ordering::Release);
            match res {
                Ok(()) => {
                    *cur = if j + 1 >= self.n_cons { 0 } else { j + 1 };
                    return Ok(());
                }
                Err(returned) => item = returned, // that ring full, try next consumer
            }
        }
        Err(item) // every OPEN outgoing ring full (or every column closed)
    }

    /// Close consumer `ci`'s column: producers stop selecting it, and any
    /// producer already mid-push into it is waited out (the busy handshake),
    /// so once this returns, the column's contents are final and the caller
    /// can drain the residue race-free. Idempotent.
    pub fn close_consumer(&self, ci: usize) {
        debug_assert!(ci < self.n_cons);
        if self.closed[ci].swap(true, Ordering::SeqCst) {
            return; // already closed
        }
        for i in 0..self.n_prod {
            let ring = &self.rings[i * self.n_cons + ci];
            while ring.busy.0.load(Ordering::SeqCst) {
                std::hint::spin_loop(); // window is a few instructions
            }
        }
    }

    /// Reopen consumer `ci`'s column — called when a replica (re)spawns on
    /// this index, before it starts popping.
    pub fn reopen_consumer(&self, ci: usize) {
        debug_assert!(ci < self.n_cons);
        self.closed[ci].store(false, Ordering::SeqCst);
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

    /// Total occupancy across all rings, PLUS producers currently blocked in
    /// backpressure -- the SpscMesh analogue of the bounded ring's `len()`,
    /// for feeding the autoscaler's queue-depth signal. Counting blocked
    /// producers means an all-closed, all-empty mesh with a producer waiting
    /// still reads > 0, so the autoscaler cold-starts a consumer that reopens
    /// a column instead of deadlocking.
    pub fn len(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
            + self
                .rings
                .iter()
                .map(|r| {
                    r.slots
                        .iter()
                        .filter(|s| s.state.load(Ordering::Relaxed) == FULL)
                        .count()
                })
                .sum::<usize>()
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
    /// While blocked (every open column full, or every column closed), the
    /// producer registers in `mesh.waiting` so the edge's `len()` still shows
    /// demand and the consumer-stage autoscaler can respawn/reopen a column.
    #[inline]
    pub fn push(&self, item: Bytes, drain: &AtomicBool) -> bool {
        let mut pending = match self.mesh.push(self.pi, item) {
            Ok(()) => return true,
            Err(back) => back,
        };
        self.mesh.waiting.fetch_add(1, Ordering::SeqCst);
        let mut backoff = Backoff::new();
        let delivered = loop {
            if drain.load(Ordering::Relaxed) {
                break false;
            }
            backoff.wait();
            match self.mesh.push(self.pi, pending) {
                Ok(()) => break true,
                Err(back) => pending = back,
            }
        };
        self.mesh.waiting.fetch_sub(1, Ordering::SeqCst);
        delivered
    }
}

/// Per-worker consumer handle into the mesh: consumer index baked in.
pub struct MeshRx {
    mesh: Arc<SpscMesh>,
    ci: usize,
}
impl MeshRx {
    /// Bind a consumer to column `ci`, opening it. Constructing the handle IS
    /// the moment a consumer occupies the column, so this is the one place the
    /// column becomes eligible to receive — whether the previous occupant
    /// drained and closed it, or nothing has ever occupied it.
    pub fn new(mesh: Arc<SpscMesh>, ci: usize) -> Self {
        mesh.reopen_consumer(ci);
        MeshRx { mesh, ci }
    }
    /// Blocking dequeue. `None` only after drain is raised AND this consumer's
    /// column is fully empty. On the first pop after drain is observed, the
    /// column is closed (producers stop feeding it — see the busy handshake),
    /// then the residue is handed out event by event until exhausted. The
    /// pre-fix behaviour ("one last look") stranded up to n_prod × ring_cap
    /// events in the column of every drained consumer.
    #[inline]
    pub fn pop(&self, drain: &AtomicBool) -> Option<Bytes> {
        if drain.load(Ordering::Relaxed) {
            self.mesh.close_consumer(self.ci); // idempotent
            return self.mesh.pop(self.ci);     // residue until empty, then None
        }
        if let Some(item) = self.mesh.pop(self.ci) {
            return Some(item);
        }
        let mut backoff = Backoff::new();
        loop {
            if drain.load(Ordering::Relaxed) {
                self.mesh.close_consumer(self.ci);
                return self.mesh.pop(self.ci);
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
    /// Next event, or `None` when this worker should exit.
    ///
    /// `drain` is the cooperative shutdown used by scale-down and by the EOS
    /// finishing gate: work already accepted is flushed, and on the mesh the
    /// consumer's column is closed and its residue handed out before `None`.
    ///
    /// `stop` is the HARD retirement raised when a morph retires the whole
    /// stage. It differs only on the shared MPMC ring, and it has to:
    ///
    /// * On the mesh, a cooperative drain terminates by construction — closing
    ///   the column makes producers reroute, so the residue is finite. `stop`
    ///   needs no special case.
    /// * On the ring there is no per-consumer partition, so a cooperative drain
    ///   keeps handing out events for as long as upstream keeps producing and
    ///   would never terminate under sustained load. It also never needs to:
    ///   an event this worker does not pop simply stays in the shared ring for
    ///   a sibling replica or for the stage's next generation — a fused stage
    ///   inherits the very same handle — so returning immediately strands
    ///   nothing.
    #[inline]
    pub fn pop(&self, drain: &AtomicBool, stop: &AtomicBool) -> Option<Bytes> {
        match self {
            EdgeIn::Ring(e) => {
                if stop.load(Ordering::Relaxed) { return None; }
                e.pop(drain)
            }
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
            // `MeshRx::new` opens the column: whether a drained predecessor on
            // this index closed it or no replica has ever held it, producers
            // start selecting it only now that a consumer exists.
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
    /// A producer stage's handle on a consumer stage's mesh. `base` is where
    /// this stage's producer slots start in the mesh's global producer index
    /// space, so replica `r` writes to column `base + r`. With one upstream
    /// stage `base == 0` (identical to pre-fan-in behaviour); with several,
    /// each upstream owns a disjoint contiguous range, which is what keeps the
    /// one-writer-per-ring invariant under fan-in.
    Mesh { mesh: Arc<SpscMesh>, base: usize },
}
impl EdgeOutSrc {
    pub fn for_replica(&self, r: usize) -> Option<EdgeOut> {
        match self {
            EdgeOutSrc::None => None,
            EdgeOutSrc::Ring(e) => Some(EdgeOut::Ring(e.clone())),
            // `base + r` is a MESH producer index (`mesh_pi`), a different
            // space from the stage-local `replica_idx` — never conflate them.
            EdgeOutSrc::Mesh { mesh, base } => {
                Some(EdgeOut::Mesh(MeshTx::new(mesh.clone(), base + r)))
            }
        }
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

    /// Fan-in: two upstream stages, each with its own base, writing into one
    /// consumer stage's mesh. Every event must arrive exactly once and no two
    /// producers may share a ring.
    #[test]
    fn mesh_fan_in_from_two_upstream_stages() {
        const A_REPLICAS: usize = 2;
        const B_REPLICAS: usize = 3;
        const NC: usize = 2;
        const PER: u64 = 50_000;

        let mesh = Arc::new(SpscMesh::new(A_REPLICAS + B_REPLICAS, NC, 64));
        // Producer-side handles as master.rs builds them: stage A at base 0,
        // stage B at base A_REPLICAS.
        let a_src = EdgeOutSrc::Mesh { mesh: mesh.clone(), base: 0 };
        let b_src = EdgeOutSrc::Mesh { mesh: mesh.clone(), base: A_REPLICAS };

        let received = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0));
        let total = ((A_REPLICAS + B_REPLICAS) as u64) * PER;

        let mut handles = Vec::new();
        for (src, stage_id) in [(&a_src, 0u64), (&b_src, 1u64)] {
            let replicas = if stage_id == 0 { A_REPLICAS } else { B_REPLICAS };
            for r in 0..replicas {
                let out = src.for_replica(r).unwrap();
                handles.push(thread::spawn(move || {
                    let never = AtomicBool::new(false);
                    for k in 0..PER {
                        // Globally unique payload per (stage, replica, k).
                        let payload = (stage_id * 100 + r as u64) * PER + k;
                        assert!(out.push(ev(payload), &never));
                    }
                }));
            }
        }
        let drain = Arc::new(AtomicBool::new(false));
        for ci in 0..NC {
            let rx = MeshRx::new(mesh.clone(), ci);
            let recv = received.clone();
            let s = sum.clone();
            let d = drain.clone();
            handles.push(thread::spawn(move || {
                while let Some(v) = rx.pop(&d) {
                    s.fetch_add(num(&v), O::Relaxed);
                    recv.fetch_add(1, O::Relaxed);
                }
            }));
        }
        // Producers finish, then let consumers drain to empty.
        while received.load(O::Relaxed) < total {
            std::thread::yield_now();
        }
        drain.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(O::Relaxed), total, "fan-in lost or duplicated events");
    }

    #[test]
    fn mesh_close_mid_stream_conserves_events() {
        // Regression test for the scale-down stranding leak (TODO 1.6): a
        // consumer drained mid-stream must close its column and hand out the
        // residue; producers must reroute to the surviving consumer. Every
        // pushed event must come out exactly once.
        const NP: usize = 2;
        const NC: usize = 2;
        const PER: u64 = 200_000;
        let mesh = Arc::new(SpscMesh::new(NP, NC, 64));
        let received = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0));
        let c1_drain = Arc::new(AtomicBool::new(false));
        let c0_drain = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for pi in 0..NP {
            let tx = MeshTx::new(mesh.clone(), pi);
            let never = AtomicBool::new(false);
            handles.push(thread::spawn(move || {
                for k in 0..PER {
                    let payload = (pi as u64) * PER + k; // globally unique
                    assert!(tx.push(ev(payload), &never), "push must not abort");
                }
            }));
        }
        // Consumer 1: drained after ~5k events — must close its column, drain
        // its residue completely, then observe None.
        {
            let rx = MeshRx::new(mesh.clone(), 1);
            let recv = received.clone();
            let s = sum.clone();
            let drain = c1_drain.clone();
            handles.push(thread::spawn(move || {
                let mut n = 0u64;
                while let Some(v) = rx.pop(&drain) {
                    s.fetch_add(num(&v), O::Relaxed);
                    recv.fetch_add(1, O::Relaxed);
                    n += 1;
                    if n == 5_000 {
                        drain.store(true, Ordering::Relaxed); // self-drain, like EOS deferral
                    }
                }
                // Column must be genuinely empty after None.
                assert!(rx.mesh.pop(1).is_none(), "residue left after drain-None");
            }));
        }
        // Consumer 0: keeps consuming; main raises its drain once producers
        // are done, after which pop drains the rest and returns None.
        let c0_handle = {
            let rx = MeshRx::new(mesh.clone(), 0);
            let recv = received.clone();
            let s = sum.clone();
            let drain = c0_drain.clone();
            thread::spawn(move || {
                while let Some(v) = rx.pop(&drain) {
                    s.fetch_add(num(&v), O::Relaxed);
                    recv.fetch_add(1, O::Relaxed);
                }
            })
        };
        for h in handles {
            h.join().unwrap();
        }
        c0_drain.store(true, Ordering::Relaxed);
        c0_handle.join().unwrap();

        let total = (NP as u64) * PER;
        assert_eq!(received.load(O::Relaxed), total, "events lost or duplicated");
        let expect_sum = total.wrapping_mul(total - 1) / 2;
        assert_eq!(sum.load(O::Relaxed), expect_sum, "payload corruption");
    }

    /// Regression: a mesh sized wider than the consumers that actually bind it
    /// must not swallow events into the spare columns.
    ///
    /// M2 widens a fusible pair's consumer dimension at boot to `max_a + max_b`
    /// so a fused replica has a valid index by construction. If those extra
    /// columns were open, producers would round-robin into them and every event
    /// that landed there would be stranded forever — which is exactly the leak
    /// this reproduces: 2 producers × 6 unbound columns × 8 slots.
    #[test]
    fn widened_mesh_does_not_strand_events_in_unbound_columns() {
        const NP: usize = 2;
        const N_COLS: usize = 8;   // widened for a fusible pair
        const N_BOUND: usize = 2;  // replicas that actually exist
        const PER: u64 = 20_000;

        let mesh = Arc::new(SpscMesh::new(NP, N_COLS, 8));
        let received = Arc::new(AtomicU64::new(0));
        let drain = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for ci in 0..N_BOUND {
            let rx = MeshRx::new(mesh.clone(), ci);
            let recv = received.clone();
            let d = drain.clone();
            handles.push(thread::spawn(move || {
                while rx.pop(&d).is_some() {
                    recv.fetch_add(1, O::Relaxed);
                }
            }));
        }
        for pi in 0..NP {
            let tx = MeshTx::new(mesh.clone(), pi);
            handles.push(thread::spawn(move || {
                let never = AtomicBool::new(false);
                for k in 0..PER {
                    assert!(tx.push(ev((pi as u64) * PER + k), &never));
                }
            }));
        }

        let total = (NP as u64) * PER;
        while received.load(O::Relaxed) < total {
            std::thread::yield_now();
        }
        drain.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(O::Relaxed), total,
                   "events stranded in columns no consumer ever bound");
    }

    #[test]
    fn mesh_no_loss_under_concurrency() {
        const NP: usize = 3;
        const NC: usize = 2;
        const PER: u64 = 500_000;
        let mesh = Arc::new(SpscMesh::new(NP, NC, 256));
        let received = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0));

        // This test drives the mesh rawly (no `MeshRx`), so it must open the
        // columns itself — a column with no bound consumer is closed.
        for ci in 0..NC {
            mesh.reopen_consumer(ci);
        }

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