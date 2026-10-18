//! Simulated disk: writes stay volatile until a flush, and a crash lands any subset of their blocks.

use std::collections::BTreeMap;

use rock_core::buf::BufPool;
use rock_core::event::{Action, BLOCK_BYTES, DiskError, Event, Lba};
use rock_core::rng::Rng;

/// In-memory disk of 4 KiB blocks; unwritten blocks read as zeros.
#[derive(Clone, Debug)]
pub struct SimDisk {
    blocks: u64,
    durable: BTreeMap<u64, Vec<u8>>,
    volatile: Vec<(usize, u64, Vec<u8>)>,
    writes: usize,
}

impl SimDisk {
    /// Creates a zeroed disk of `blocks` blocks.
    pub const fn new(blocks: u64) -> Self {
        Self {
            blocks,
            durable: BTreeMap::new(),
            volatile: Vec::new(),
            writes: 0,
        }
    }

    /// Size in blocks.
    pub const fn blocks(&self) -> u64 {
        self.blocks
    }

    /// Executes a disk action and returns its completion; other actions return `None`.
    pub fn execute(&mut self, pool: &mut impl BufPool, action: &Action) -> Option<Event> {
        let (io, result) = match *action {
            Action::DiskWrite(io, lba, buf) => (
                io,
                self.buf_range(lba, buf.len()).map(|()| {
                    for (i, block) in pool.as_slice(buf).chunks(BLOCK_BYTES).enumerate() {
                        self.volatile
                            .push((self.writes, lba.0 + i as u64, block.to_vec()));
                    }
                    self.writes += 1;
                }),
            ),
            Action::DiskRead(io, lba, buf) => (
                io,
                self.buf_range(lba, buf.len()).map(|()| {
                    for (i, block) in pool.as_mut_slice(buf).chunks_mut(BLOCK_BYTES).enumerate() {
                        block.copy_from_slice(&self.read_block(Lba(lba.0 + i as u64)));
                    }
                }),
            ),
            Action::DiskFsync(io) => {
                for (_, lba, block) in self.volatile.drain(..) {
                    self.durable.insert(lba, block);
                }
                (io, Ok(()))
            }
            Action::DiskTrim(io, lba, blocks) => (
                io,
                self.range(lba, blocks).map(|()| {
                    for b in lba.0..lba.0 + blocks {
                        self.durable.remove(&b);
                        self.volatile.retain(|(_, l, _)| *l != b);
                    }
                }),
            ),
            Action::NetSend(..) | Action::SetTimer(..) => return None,
        };
        Some(Event::DiskDone(io, result))
    }

    /// Current contents of a block, unflushed writes included.
    pub fn read_block(&self, lba: Lba) -> Vec<u8> {
        self.volatile
            .iter()
            .rev()
            .find(|(_, l, _)| *l == lba.0)
            .map(|(_, _, b)| b.clone())
            .or_else(|| self.durable.get(&lba.0).cloned())
            .unwrap_or_else(|| vec![0; BLOCK_BYTES])
    }

    /// Number of blocks written but not yet flushed.
    pub fn unflushed_blocks(&self) -> usize {
        self.volatile.len()
    }

    /// Crashes: each unflushed block lands when `landed(write, block_in_write)` says so.
    pub fn crash_with(&mut self, mut landed: impl FnMut(usize, usize) -> bool) {
        let mut first: BTreeMap<usize, u64> = BTreeMap::new();
        for (write, lba, block) in self.volatile.drain(..) {
            let start = *first.entry(write).or_insert(lba);
            let offset = usize::try_from(lba - start).expect("write fits usize");
            if landed(write, offset) {
                self.durable.insert(lba, block);
            }
        }
    }

    /// Crashes, landing each unflushed block with probability one half.
    pub fn crash(&mut self, rng: &mut Rng) {
        self.crash_with(|_, _| rng.chance(1, 2));
    }

    /// XORs `mask` into byte `byte` of a durable block: a bit flip.
    pub fn flip(&mut self, lba: Lba, byte: usize, mask: u8) {
        let mut block = self.read_block(lba);
        block[byte] ^= mask;
        self.durable.insert(lba.0, block);
    }

    fn buf_range(&self, lba: Lba, len: usize) -> Result<(), DiskError> {
        if !len.is_multiple_of(BLOCK_BYTES) {
            return Err(DiskError::InvalidRange);
        }
        self.range(lba, (len / BLOCK_BYTES) as u64)
    }

    fn range(&self, lba: Lba, blocks: u64) -> Result<(), DiskError> {
        if lba
            .0
            .checked_add(blocks)
            .is_none_or(|end| end > self.blocks)
        {
            return Err(DiskError::InvalidRange);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rock_core::buf::BufPool;
    use rock_core::event::{Action, DiskError, Event, IoId};

    use super::*;
    use crate::pool::HeapPool;

    #[test]
    fn writes_are_lost_or_torn_until_flushed() {
        let mut pool = HeapPool::new(1 << 20);
        let mut disk = SimDisk::new(16);
        let flushed = pool.alloc_from_slice(&[1; BLOCK_BYTES]).unwrap();
        disk.execute(&mut pool, &Action::DiskWrite(IoId(1), Lba(0), flushed));
        disk.execute(&mut pool, &Action::DiskFsync(IoId(2)));
        let torn = pool.alloc_from_slice(&[2; 3 * BLOCK_BYTES]).unwrap();
        disk.execute(&mut pool, &Action::DiskWrite(IoId(3), Lba(0), torn));
        assert_eq!(disk.read_block(Lba(2)), vec![2; BLOCK_BYTES]);
        assert_eq!(disk.unflushed_blocks(), 3);

        disk.crash_with(|_, block| block == 1);
        assert_eq!(disk.read_block(Lba(0)), vec![1; BLOCK_BYTES]);
        assert_eq!(disk.read_block(Lba(1)), vec![2; BLOCK_BYTES]);
        assert_eq!(disk.read_block(Lba(2)), vec![0; BLOCK_BYTES]);
        assert_eq!(disk.unflushed_blocks(), 0);
    }

    #[test]
    fn out_of_range_io_fails() {
        let mut pool = HeapPool::new(1 << 20);
        let mut disk = SimDisk::new(2);
        let buf = pool.alloc(2 * BLOCK_BYTES).unwrap();
        assert_eq!(
            disk.execute(&mut pool, &Action::DiskRead(IoId(1), Lba(1), buf)),
            Some(Event::DiskDone(IoId(1), Err(DiskError::InvalidRange)))
        );
    }
}
