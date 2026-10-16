//! Driver-owned buffer pool interface and buffer handles.

use core::fmt;

/// Handle to a buffer owned by a driver's [`BufPool`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IoBuf {
    slot: u32,
    generation: u32,
    len: usize,
}

impl IoBuf {
    /// Creates a handle for a pool slot, generation counter, and byte length.
    pub const fn new(slot: u32, generation: u32, len: usize) -> Self {
        Self {
            slot,
            generation,
            len,
        }
    }

    /// Pool slot index of this buffer.
    pub const fn slot(self) -> u32 {
        self.slot
    }

    /// Generation counter identifying the allocation epoch of the slot.
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// Length of this buffer in bytes.
    pub const fn len(self) -> usize {
        self.len
    }

    /// Returns true if the buffer has zero length.
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// Error returned when a [`BufPool`] cannot satisfy an allocation within its fixed capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolExhausted {
    /// Number of bytes requested by the caller.
    pub requested: usize,
    /// Number of free bytes available in the pool.
    pub available: usize,
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "buffer pool exhausted: requested {} bytes with {} bytes available",
            self.requested, self.available
        )
    }
}

impl core::error::Error for PoolExhausted {}

/// Driver-owned pool of explicit byte capacity issuing [`IoBuf`] handles.
pub trait BufPool {
    /// Total byte capacity of the pool.
    fn capacity(&self) -> usize;

    /// Bytes currently allocated from the pool.
    fn used(&self) -> usize;

    /// Free bytes remaining in the pool.
    fn available(&self) -> usize {
        self.capacity().saturating_sub(self.used())
    }

    /// Allocates a zero-initialized buffer of `len` bytes or returns [`PoolExhausted`].
    fn alloc(&mut self, len: usize) -> Result<IoBuf, PoolExhausted>;

    /// Allocates a buffer initialized with `data` or returns [`PoolExhausted`].
    fn alloc_from_slice(&mut self, data: &[u8]) -> Result<IoBuf, PoolExhausted> {
        let buf = self.alloc(data.len())?;
        self.as_mut_slice(buf).copy_from_slice(data);
        Ok(buf)
    }

    /// Borrows the byte slice backing `buf`.
    fn as_slice(&self, buf: IoBuf) -> &[u8];

    /// Mutably borrows the byte slice backing `buf`.
    fn as_mut_slice(&mut self, buf: IoBuf) -> &mut [u8];

    /// Releases `buf` back to the pool so its capacity can be reused.
    fn free(&mut self, buf: IoBuf);
}

