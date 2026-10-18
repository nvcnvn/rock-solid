//! Volume, inode, chunk, and placement-group identifiers and XXH3-64 hashing (ADR 0004).

use core::fmt;
use twox_hash::xxhash3_64::Hasher as Xxh3_64;

use crate::event::BLOCK_BYTES;

/// Size of a single chunk in bytes (4 MiB).
pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Number of 4 KiB blocks in a single 4 MiB chunk.
pub const CHUNK_BLOCKS: u32 = 1024;

const _: () = assert!(CHUNK_BLOCKS as u64 * BLOCK_BYTES as u64 == CHUNK_SIZE);

/// Pool identifier.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolId(pub u32);

/// 32-bit volume identifier (never reused).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeId(pub u32);

/// Error returned when an inode's slot or sequence number exceeds its bit width.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidInodeId {
    /// Proposed 24-bit slot number.
    pub slot: u32,
    /// Proposed 40-bit sequence number.
    pub seq: u64,
}

impl fmt::Display for InvalidInodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid inode components: slot={} (max {}), seq={} (max {})",
            self.slot,
            InodeId::MAX_SLOT,
            self.seq,
            InodeId::MAX_SEQ
        )
    }
}

impl core::error::Error for InvalidInodeId {}

/// 64-bit inode identifier packing a 24-bit slot in the high bits and a 40-bit sequence number.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodeId(pub u64);

impl InodeId {
    /// Reserved inode number used by block volumes.
    pub const BLOCK_INODE: Self = Self(0);

    /// Inode number of a filesystem volume's root directory (slot 0, sequence 1).
    pub const ROOT_INODE: Self = Self(1);

    /// Maximum 24-bit slot value (`2^24 - 1`).
    pub const MAX_SLOT: u32 = (1 << 24) - 1;

    /// Maximum 40-bit sequence value (`2^40 - 1`).
    pub const MAX_SEQ: u64 = (1 << 40) - 1;

    /// Packs a 24-bit `slot` and a 40-bit `seq` into a 64-bit [`InodeId`].
    pub const fn from_slot_seq(slot: u32, seq: u64) -> Result<Self, InvalidInodeId> {
        if slot > Self::MAX_SLOT || seq > Self::MAX_SEQ {
            return Err(InvalidInodeId { slot, seq });
        }
        Ok(Self(((slot as u64) << 40) | seq))
    }

    /// Returns the reserved sequence-0 inode identifier for `slot`'s metadata keys.
    pub const fn slot_meta(slot: u32) -> Result<Self, InvalidInodeId> {
        Self::from_slot_seq(slot, 0)
    }

    /// Extracts the 24-bit slot number from the top bits.
    pub const fn slot(self) -> u32 {
        let b = self.0.to_be_bytes();
        u32::from_be_bytes([0, b[0], b[1], b[2]])
    }

    /// Extracts the 40-bit sequence number from the bottom bits.
    pub const fn seq(self) -> u64 {
        self.0 & Self::MAX_SEQ
    }

    /// Encodes the inode identifier as 8 big-endian bytes.
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decodes an inode identifier from 8 big-endian bytes.
    pub const fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

/// Error returned when a byte offset exceeds the 32-bit chunk index limit (16 PiB).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OffsetOverflow {
    /// Byte offset that overflowed the 32-bit chunk index.
    pub byte_offset: u64,
}

impl fmt::Display for OffsetOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "byte offset {} exceeds 16 PiB chunk index range",
            self.byte_offset
        )
    }
}

impl core::error::Error for OffsetOverflow {}

/// Error returned when a placement-group count is zero or not a power of two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidPgCount(pub u32);

impl fmt::Display for InvalidPgCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "placement-group count {} must be a non-zero power of two",
            self.0
        )
    }
}

impl core::error::Error for InvalidPgCount {}

/// Placement-group identifier pairing a [`PoolId`] with a zero-based PG index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgId {
    /// Pool that owns the placement group.
    pub pool: PoolId,
    /// Placement-group index within `0..pg_count`.
    pub index: u32,
}

/// Computes the placement-group index from a 64-bit hash and power-of-two `pg_count`.
pub const fn pg_index_from_hash(hash: u64, pg_count: u32) -> Result<u32, InvalidPgCount> {
    if !pg_count.is_power_of_two() {
        return Err(InvalidPgCount(pg_count));
    }
    let bits = pg_count.trailing_zeros();
    let shifted = if bits == 0 { 0 } else { hash >> (64 - bits) };
    let b = shifted.to_be_bytes();
    Ok(u32::from_be_bytes([b[4], b[5], b[6], b[7]]))
}

/// 128-bit chunk identifier packing `(volume: u32, inode: u64, index: u32)`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(u128);

impl ChunkId {
    /// Packs `volume`, `inode`, and `index` into a 128-bit [`ChunkId`].
    pub const fn new(volume: VolumeId, inode: InodeId, index: u32) -> Self {
        let packed = ((volume.0 as u128) << 96) | ((inode.0 as u128) << 32) | (index as u128);
        Self(packed)
    }

    /// Computes the [`ChunkId`] for `byte_offset` within `(volume, inode)`.
    pub const fn for_offset(
        volume: VolumeId,
        inode: InodeId,
        byte_offset: u64,
    ) -> Result<Self, OffsetOverflow> {
        let idx = byte_offset / CHUNK_SIZE;
        if (idx >> 32) != 0 {
            return Err(OffsetOverflow { byte_offset });
        }
        let b = idx.to_be_bytes();
        Ok(Self::new(
            volume,
            inode,
            u32::from_be_bytes([b[4], b[5], b[6], b[7]]),
        ))
    }

    /// Computes the [`ChunkId`] for `byte_offset` within a block volume (`inode = 0`).
    pub const fn for_block_offset(
        volume: VolumeId,
        byte_offset: u64,
    ) -> Result<Self, OffsetOverflow> {
        Self::for_offset(volume, InodeId::BLOCK_INODE, byte_offset)
    }

    /// Extracts the 32-bit [`VolumeId`].
    pub const fn volume(self) -> VolumeId {
        let b = self.0.to_be_bytes();
        VolumeId(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Extracts the 64-bit [`InodeId`].
    pub const fn inode(self) -> InodeId {
        let b = self.0.to_be_bytes();
        InodeId(u64::from_be_bytes([
            b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        ]))
    }

    /// Extracts the 32-bit chunk index.
    pub const fn index(self) -> u32 {
        let b = self.0.to_be_bytes();
        u32::from_be_bytes([b[12], b[13], b[14], b[15]])
    }

    /// Returns the packed 128-bit integer value.
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// Wraps a raw packed 128-bit integer value.
    pub const fn from_u128(raw: u128) -> Self {
        Self(raw)
    }

    /// Encodes the chunk identifier as 16 big-endian bytes (frozen on-disk format).
    pub const fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// Decodes a chunk identifier from 16 big-endian bytes.
    pub const fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }

    /// Computes the frozen XXH3-64 hash (seed 0) over the 16 big-endian bytes of the chunk ID.
    pub fn hash(self) -> u64 {
        Xxh3_64::oneshot_with_seed(0, &self.to_be_bytes())
    }

    /// Computes the zero-based placement-group index within a pool of `pg_count` placement groups.
    pub fn pg_index(self, pg_count: u32) -> Result<u32, InvalidPgCount> {
        pg_index_from_hash(self.hash(), pg_count)
    }

    /// Computes the [`PgId`] for this chunk in `pool` with `pg_count` placement groups.
    pub fn pg(self, pool: PoolId, pg_count: u32) -> Result<PgId, InvalidPgCount> {
        Ok(PgId {
            pool,
            index: self.pg_index(pg_count)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_slot_and_sequence_packing() {
        assert_eq!(InodeId::BLOCK_INODE.0, 0);
        assert_eq!(InodeId::BLOCK_INODE.slot(), 0);
        assert_eq!(InodeId::BLOCK_INODE.seq(), 0);

        assert_eq!(InodeId::ROOT_INODE.0, 1);
        assert_eq!(InodeId::ROOT_INODE.slot(), 0);
        assert_eq!(InodeId::ROOT_INODE.seq(), 1);
        assert_eq!(InodeId::from_slot_seq(0, 1).unwrap(), InodeId::ROOT_INODE);

        let meta = InodeId::slot_meta(0x0012_3456).unwrap();
        assert_eq!(meta.slot(), 0x0012_3456);
        assert_eq!(meta.seq(), 0);
        assert_eq!(meta.0, 0x1234_5600_0000_0000);

        let ino = InodeId::from_slot_seq(0x00AB_CDEF, 0x0000_0098_7654_3210).unwrap();
        assert_eq!(ino.slot(), 0x00AB_CDEF);
        assert_eq!(ino.seq(), 0x0000_0098_7654_3210);
        assert_eq!(
            ino.to_be_bytes(),
            [0xAB, 0xCD, 0xEF, 0x98, 0x76, 0x54, 0x32, 0x10]
        );
        assert_eq!(InodeId::from_be_bytes(ino.to_be_bytes()), ino);

        assert!(InodeId::from_slot_seq(InodeId::MAX_SLOT + 1, 0).is_err());
        assert!(InodeId::from_slot_seq(0, InodeId::MAX_SEQ + 1).is_err());
    }

    #[test]
    fn chunk_id_packing_and_offset_computation() {
        let vol = VolumeId(0x0102_0304);
        let ino = InodeId(0x0506_0708_090A_0B0C);
        let idx = 0x0D0E_0F10;
        let cid = ChunkId::new(vol, ino, idx);

        assert_eq!(cid.volume(), vol);
        assert_eq!(cid.inode(), ino);
        assert_eq!(cid.index(), idx);
        assert_eq!(
            cid.to_be_bytes(),
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                0x0F, 0x10
            ]
        );
        assert_eq!(ChunkId::from_be_bytes(cid.to_be_bytes()), cid);
        assert_eq!(ChunkId::from_u128(cid.as_u128()), cid);

        let c0 = ChunkId::for_block_offset(vol, 0).unwrap();
        assert_eq!(c0, ChunkId::new(vol, InodeId::BLOCK_INODE, 0));

        let c0_end = ChunkId::for_block_offset(vol, CHUNK_SIZE - 1).unwrap();
        assert_eq!(c0_end.index(), 0);

        let c1 = ChunkId::for_block_offset(vol, CHUNK_SIZE).unwrap();
        assert_eq!(c1.index(), 1);

        let max_offset = (u64::from(u32::MAX) + 1) * CHUNK_SIZE - 1;
        let c_max = ChunkId::for_offset(vol, ino, max_offset).unwrap();
        assert_eq!(c_max.index(), u32::MAX);

        let overflow_offset = (u64::from(u32::MAX) + 1) * CHUNK_SIZE;
        assert_eq!(
            ChunkId::for_offset(vol, ino, overflow_offset),
            Err(OffsetOverflow {
                byte_offset: overflow_offset
            })
        );
    }

    #[test]
    fn pg_splitting_preserves_contiguous_hash_ranges() {
        assert_eq!(pg_index_from_hash(0, 0), Err(InvalidPgCount(0)));
        assert_eq!(pg_index_from_hash(0, 3), Err(InvalidPgCount(3)));
        assert_eq!(pg_index_from_hash(u64::MAX, 1).unwrap(), 0);

        let vol = VolumeId(7);
        let ino = InodeId::from_slot_seq(3, 100).unwrap();
        for idx in 0..64 {
            let cid = ChunkId::new(vol, ino, idx);
            for shift in 0..12 {
                let pg_count = 1u32 << shift;
                let doubled = pg_count * 2;
                let p = cid.pg_index(pg_count).unwrap();
                let p2 = cid.pg_index(doubled).unwrap();
                assert!(p2 == 2 * p || p2 == 2 * p + 1);
            }
        }
    }

    #[test]
    fn golden_vectors_match_checked_in_fixture() {
        let fixture = include_str!("../testdata/chunk_id_golden.txt");
        let mut count = 0usize;
        for line in fixture.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let cols: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(cols.len(), 8, "malformed golden line: {line}");

            let vol = u32::from_str_radix(cols[0].trim_start_matches("0x"), 16).unwrap();
            let slot = u32::from_str_radix(cols[1].trim_start_matches("0x"), 16).unwrap();
            let seq = u64::from_str_radix(cols[2].trim_start_matches("0x"), 16).unwrap();
            let idx = u32::from_str_radix(cols[3].trim_start_matches("0x"), 16).unwrap();
            let expected_packed = u128::from_str_radix(cols[4], 16).unwrap();
            let expected_hash = u64::from_str_radix(cols[5].trim_start_matches("0x"), 16).unwrap();
            let expected_pg64: u32 = cols[6].parse().unwrap();
            let expected_pg2048: u32 = cols[7].parse().unwrap();

            let ino = InodeId::from_slot_seq(slot, seq).unwrap();
            let cid = ChunkId::new(VolumeId(vol), ino, idx);

            assert_eq!(cid.as_u128(), expected_packed);
            assert_eq!(cid.to_be_bytes(), expected_packed.to_be_bytes());
            assert_eq!(cid.hash(), expected_hash);
            assert_eq!(cid.pg_index(64).unwrap(), expected_pg64);
            assert_eq!(cid.pg_index(2048).unwrap(), expected_pg2048);
            assert_eq!(
                cid.pg(PoolId(1), 2048).unwrap(),
                PgId {
                    pool: PoolId(1),
                    index: expected_pg2048,
                }
            );
            count += 1;
        }
        assert_eq!(count, 10);
    }
}
