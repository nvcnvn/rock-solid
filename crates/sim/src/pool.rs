//! Heap-backed buffer pool of explicit capacity for deterministic simulation.

use rock_core::buf::{BufPool, IoBuf, PoolExhausted};

#[derive(Clone, Debug)]
struct Slot {
    data: Vec<u8>,
    generation: u32,
    allocated: bool,
}

/// Heap-backed [`BufPool`] enforcing an explicit byte capacity and optional slot limit.
#[derive(Clone, Debug)]
pub struct HeapPool {
    capacity: usize,
    max_slots: usize,
    used: usize,
    live_count: usize,
    slots: Vec<Slot>,
    free_slots: Vec<u32>,
}

impl HeapPool {
    /// Creates a heap buffer pool with an explicit total byte `capacity`.
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            max_slots: usize::MAX,
            used: 0,
            live_count: 0,
            slots: Vec::new(),
            free_slots: Vec::new(),
        }
    }

    /// Creates a heap buffer pool bounded by both byte `capacity` and `max_slots` handles.
    pub const fn with_max_slots(capacity: usize, max_slots: usize) -> Self {
        Self {
            capacity,
            max_slots,
            used: 0,
            live_count: 0,
            slots: Vec::new(),
            free_slots: Vec::new(),
        }
    }

    /// Number of buffers currently checked out of the pool.
    pub const fn live_buffers(&self) -> usize {
        self.live_count
    }

    fn validate_slot(&self, buf: IoBuf) -> usize {
        let idx = usize::try_from(buf.slot()).expect("IoBuf slot index overflows usize");
        let slot = self
            .slots
            .get(idx)
            .expect("IoBuf slot index out of bounds for pool");
        assert!(slot.allocated, "IoBuf handle refers to an unallocated slot");
        assert_eq!(
            slot.generation,
            buf.generation(),
            "stale IoBuf handle (generation mismatch)"
        );
        assert_eq!(
            slot.data.len(),
            buf.len(),
            "IoBuf length does not match allocated slot length"
        );
        idx
    }
}

impl BufPool for HeapPool {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn used(&self) -> usize {
        self.used
    }

    fn alloc(&mut self, len: usize) -> Result<IoBuf, PoolExhausted> {
        let available = self.available();
        if len > available || self.live_count >= self.max_slots {
            return Err(PoolExhausted {
                requested: len,
                available,
            });
        }

        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            let slot = &mut self.slots[usize::try_from(idx).expect("slot index fits in usize")];
            slot.data.clear();
            slot.data.resize(len, 0);
            slot.allocated = true;
            idx
        } else {
            let idx = u32::try_from(self.slots.len()).map_err(|_| PoolExhausted {
                requested: len,
                available,
            })?;
            self.slots.push(Slot {
                data: vec![0u8; len],
                generation: 1,
                allocated: true,
            });
            idx
        };

        let generation =
            self.slots[usize::try_from(slot_idx).expect("slot index fits in usize")].generation;
        self.used += len;
        self.live_count += 1;
        Ok(IoBuf::new(slot_idx, generation, len))
    }

    fn as_slice(&self, buf: IoBuf) -> &[u8] {
        let idx = self.validate_slot(buf);
        &self.slots[idx].data
    }

    fn as_mut_slice(&mut self, buf: IoBuf) -> &mut [u8] {
        let idx = self.validate_slot(buf);
        &mut self.slots[idx].data
    }

    fn free(&mut self, buf: IoBuf) {
        let idx = self.validate_slot(buf);
        let slot = &mut self.slots[idx];
        slot.allocated = false;
        slot.generation = slot.generation.wrapping_add(1);
        slot.data.clear();
        self.used -= buf.len();
        self.live_count -= 1;
        self.free_slots.push(buf.slot());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rock_core::buf::{BufPool, IoBuf, PoolExhausted};
    use rock_core::event::{Action, BLOCK_BYTES, Event, IoId, Lba, NodeId, Tick, TimerId};

    use super::HeapPool;

    pub(super) struct BufferedWriterMachine {
        next_io: u64,
        retry_timer: TimerId,
        retry_delay_ticks: u64,
        pending_writes: Vec<(Lba, [u8; 4])>,
        in_flight: BTreeMap<IoId, IoBuf>,
        exhaustions_seen: usize,
    }

    impl BufferedWriterMachine {
        pub(super) fn new(retry_timer: TimerId, retry_delay_ticks: u64) -> Self {
            Self {
                next_io: 1,
                retry_timer,
                retry_delay_ticks,
                pending_writes: Vec::new(),
                in_flight: BTreeMap::new(),
                exhaustions_seen: 0,
            }
        }

        pub(super) fn enqueue_write(
            &mut self,
            now: Tick,
            lba: Lba,
            tag: [u8; 4],
            pool: &mut dyn BufPool,
        ) -> Vec<Action> {
            self.pending_writes.push((lba, tag));
            self.flush_pending(now, pool)
        }

        #[allow(
            clippy::large_types_passed_by_value,
            reason = "sans-IO state machine on_event signature takes Event by value"
        )]
        pub(super) fn on_event(
            &mut self,
            now: Tick,
            ev: Event,
            pool: &mut dyn BufPool,
        ) -> Vec<Action> {
            match ev {
                Event::DiskDone(id, result) => {
                    if let Some(buf) = self.in_flight.remove(&id) {
                        pool.free(buf);
                    }
                    assert!(result.is_ok());
                    self.flush_pending(now, pool)
                }
                Event::PoolExhausted(_) => self.schedule_retry(now),
                Event::Timer(timer) if timer == self.retry_timer => self.flush_pending(now, pool),
                Event::Timer(_) | Event::NetRecv(_, _) | Event::PeerLost(_) => Vec::new(),
            }
        }

        fn schedule_retry(&mut self, now: Tick) -> Vec<Action> {
            self.exhaustions_seen += 1;
            vec![Action::SetTimer(
                self.retry_timer,
                now.saturating_add(self.retry_delay_ticks),
            )]
        }

        fn flush_pending(&mut self, now: Tick, pool: &mut dyn BufPool) -> Vec<Action> {
            let mut actions = Vec::new();
            while let Some(&(lba, tag)) = self.pending_writes.first() {
                match pool.alloc(BLOCK_BYTES) {
                    Ok(buf) => {
                        self.pending_writes.remove(0);
                        pool.as_mut_slice(buf)[..4].copy_from_slice(&tag);
                        let id = IoId::new(self.next_io);
                        self.next_io += 1;
                        self.in_flight.insert(id, buf);
                        actions.push(Action::DiskWrite(id, lba, buf));
                    }
                    Err(exhausted) => {
                        actions.extend(self.on_event(now, Event::PoolExhausted(exhausted), pool));
                        break;
                    }
                }
            }
            let _ = NodeId::new(0);
            actions
        }
    }

    #[test]
    fn explicit_size_pool_allocates_reuses_and_zero_initializes() {
        let mut pool = HeapPool::new(2 * BLOCK_BYTES);
        assert_eq!(pool.capacity(), 2 * BLOCK_BYTES);
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.available(), 2 * BLOCK_BYTES);

        let b1 = pool.alloc_from_slice(&[0xAA; BLOCK_BYTES]).unwrap();
        let b2 = pool.alloc(BLOCK_BYTES).unwrap();
        assert_eq!(pool.used(), 2 * BLOCK_BYTES);
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.live_buffers(), 2);
        assert!(pool.as_slice(b2).iter().all(|&b| b == 0));

        assert_eq!(
            pool.alloc(1),
            Err(PoolExhausted {
                requested: 1,
                available: 0,
            })
        );

        pool.free(b1);
        assert_eq!(pool.available(), BLOCK_BYTES);
        assert_eq!(pool.live_buffers(), 1);

        let b3 = pool.alloc(BLOCK_BYTES).unwrap();
        assert_eq!(b3.slot(), b1.slot());
        assert_ne!(b3.generation(), b1.generation());
        assert!(pool.as_slice(b3).iter().all(|&b| b == 0));

        pool.free(b2);
        pool.free(b3);
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.live_buffers(), 0);
    }

    #[test]
    fn max_slots_limit_returns_pool_exhausted() {
        let mut pool = HeapPool::with_max_slots(4 * BLOCK_BYTES, 1);
        let b1 = pool.alloc(BLOCK_BYTES).unwrap();
        assert_eq!(
            pool.alloc(BLOCK_BYTES),
            Err(PoolExhausted {
                requested: BLOCK_BYTES,
                available: 3 * BLOCK_BYTES,
            })
        );
        pool.free(b1);
        let b2 = pool.alloc(BLOCK_BYTES).unwrap();
        pool.free(b2);
    }

    #[test]
    fn exhausted_pool_is_handled_as_an_event_without_panic() {
        let mut pool = HeapPool::new(BLOCK_BYTES);
        let retry_timer = TimerId::new(77);
        let mut machine = BufferedWriterMachine::new(retry_timer, 10);

        let a1 = machine.enqueue_write(Tick::new(100), Lba::new(0), *b"blk0", &mut pool);
        let expected_buf1 = IoBuf::new(0, 1, BLOCK_BYTES);
        assert_eq!(
            a1,
            vec![Action::DiskWrite(IoId::new(1), Lba::new(0), expected_buf1)]
        );
        assert_eq!(&pool.as_slice(expected_buf1)[..4], b"blk0");

        let a2 = machine.enqueue_write(Tick::new(105), Lba::new(1), *b"blk1", &mut pool);
        assert_eq!(a2, vec![Action::SetTimer(retry_timer, Tick::new(115))]);
        assert_eq!(machine.exhaustions_seen, 1);

        let a3 = machine.on_event(Tick::new(115), Event::Timer(retry_timer), &mut pool);
        assert_eq!(a3, vec![Action::SetTimer(retry_timer, Tick::new(125))]);
        assert_eq!(machine.exhaustions_seen, 2);

        let a4 = machine.on_event(
            Tick::new(120),
            Event::DiskDone(IoId::new(1), Ok(())),
            &mut pool,
        );
        let expected_buf2 = IoBuf::new(0, 2, BLOCK_BYTES);
        assert_eq!(
            a4,
            vec![Action::DiskWrite(IoId::new(2), Lba::new(1), expected_buf2)]
        );
        assert_eq!(&pool.as_slice(expected_buf2)[..4], b"blk1");

        let a5 = machine.on_event(
            Tick::new(130),
            Event::DiskDone(IoId::new(2), Ok(())),
            &mut pool,
        );
        assert_eq!(a5, Vec::new());
        assert_eq!(pool.live_buffers(), 0);
    }

    #[test]
    #[should_panic(expected = "IoBuf handle refers to an unallocated slot")]
    fn reading_freed_buffer_panics() {
        let mut pool = HeapPool::new(BLOCK_BYTES);
        let buf = pool.alloc(BLOCK_BYTES).unwrap();
        pool.free(buf);
        let _ = pool.as_slice(buf);
    }

    #[test]
    #[should_panic(expected = "stale IoBuf handle (generation mismatch)")]
    fn reading_stale_generation_handle_panics() {
        let mut pool = HeapPool::new(BLOCK_BYTES);
        let old = pool.alloc(BLOCK_BYTES).unwrap();
        pool.free(old);
        let _new = pool.alloc(BLOCK_BYTES).unwrap();
        let _ = pool.as_slice(old);
    }
}
