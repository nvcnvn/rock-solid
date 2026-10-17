//! The per-disk log: segments, records, group commit and recovery (ADR 0003).

pub mod commit;
pub mod manifest;
pub mod record;
pub mod recovery;
pub mod segment;

use core::fmt;

use crate::crc::Crc32cHasher;
use crate::event::Lba;
use record::RecordHeader;

/// On-disk format version written into superblocks, segment headers, records and manifests.
pub const FORMAT_VERSION: u16 = 1;

/// Write stream a segment belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stream {
    /// New chunk copies and Raft entries.
    Hot = 0,
    /// Encoded shards, blocks moved by the cleaner, and index snapshots.
    Cold = 1,
}

impl Stream {
    /// Both streams, in tag order.
    pub const ALL: [Self; 2] = [Self::Hot, Self::Cold];

    /// Index of the stream in per-stream arrays.
    pub const fn index(self) -> usize {
        self as usize
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Hot),
            1 => Some(Self::Cold),
            _ => None,
        }
    }
}

/// Position of a record in the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordPos {
    /// Segment holding the record.
    pub segment: u32,
    /// Block offset of the record's header within the segment.
    pub block: u32,
    /// Log sequence number of the record.
    pub lsn: u64,
}

/// A durable record: where it is, its decoded header and the payloads of its blob entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Position of the record.
    pub pos: RecordPos,
    /// Disk address of the record's first header block.
    pub lba: Lba,
    /// Decoded record header.
    pub header: RecordHeader,
    /// Payload of each blob entry, in entry order (empty when recovery skipped the data).
    pub blobs: Vec<Vec<u8>>,
}

/// Reason a superblock, segment header, record or manifest failed to decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DecodeError {
    /// Magic bytes do not match; the block holds something else.
    BadMagic,
    /// Format version this code cannot read.
    BadVersion(u16),
    /// Header checksum mismatch: torn, bit-flipped or never written.
    BadChecksum,
    /// Checksummed contents are inconsistent.
    Malformed,
    /// Valid header belonging to another log, segment incarnation or position.
    Mismatch,
    /// Data block `block` (0-based within the record's data) fails its CRC32C.
    DataChecksum {
        /// Index of the failing data block.
        block: u32,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => f.write_str("bad magic"),
            Self::BadVersion(v) => write!(f, "unsupported format version {v}"),
            Self::BadChecksum => f.write_str("header checksum mismatch"),
            Self::Malformed => f.write_str("malformed contents"),
            Self::Mismatch => f.write_str("header belongs to another log, segment or position"),
            Self::DataChecksum { block } => write!(f, "data block {block} checksum mismatch"),
        }
    }
}

impl core::error::Error for DecodeError {}

/// CRC32C of `bytes` with the four bytes at `hole` read as zeros.
fn crc_with_hole(bytes: &[u8], hole: usize) -> u32 {
    let mut hasher = Crc32cHasher::new();
    hasher.update(&bytes[..hole]);
    hasher.update(&[0; 4]);
    hasher.update(&bytes[hole + 4..]);
    hasher.finalize()
}

/// Little-endian reader over a checksummed buffer.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let end = self.at.checked_add(N).ok_or(DecodeError::Malformed)?;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or(DecodeError::Malformed)?
            .try_into()
            .map_err(|_| DecodeError::Malformed)?;
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        self.take().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        self.take().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        self.take().map(u64::from_le_bytes)
    }

    fn is_empty(&self) -> bool {
        self.at == self.bytes.len()
    }
}
