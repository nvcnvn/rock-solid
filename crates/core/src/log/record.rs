//! Record codec: header blocks listing the round's entries with a CRC32C per data block, then the data.
//!
//! ```text
//! header (header_blocks × 4 KiB, checksummed as a whole):
//!   magic "RSRC" | crc u32 | version u16 | header_blocks u16 | record_blocks u32
//!   log_id u64 | segment_seq u64 | lsn u64 | offset u32 | entry_count u32
//!   entries | crc32c u32 per data block | zero padding
//! data: each write's blocks in entry order, then blob payloads packed back to back, zero padded
//! ```

use core::ops::Range;

use super::{DecodeError, FORMAT_VERSION, Reader, crc_with_hole};
use crate::crc::crc32c;
use crate::event::BLOCK_BYTES;
use crate::id::{CHUNK_BLOCKS, ChunkId, PoolId};

const MAGIC: [u8; 4] = *b"RSRC";
const CRC_AT: usize = 4;
const PRELUDE_LEN: usize = 48;
const EXTENT_LEN: usize = 33;
const BLOB_LEN: usize = 14;
const TAG_WRITE: u8 = 1;
const TAG_PUNCH: u8 = 2;
const TAG_BLOB: u8 = 3;

/// Owner-defined type of a blob entry.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKind(pub u8);

impl BlobKind {
    /// One piece of an index's full snapshot.
    pub const SNAPSHOT: Self = Self(1);
    /// A [`Manifest`](super::manifest::Manifest).
    pub const MANIFEST: Self = Self(2);
}

/// Block range of one chunk version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Extent {
    /// Pool the chunk belongs to.
    pub pool: PoolId,
    /// Chunk written or punched.
    pub chunk: ChunkId,
    /// Chunk version this extent belongs to.
    pub version: u64,
    /// First 4 KiB block within the chunk.
    pub first_block: u16,
    /// Number of blocks, at least one.
    pub blocks: u16,
}

impl Extent {
    /// Whether the extent covers at least one block and stays inside its chunk.
    pub fn is_valid(&self) -> bool {
        self.blocks > 0 && u32::from(self.first_block) + u32::from(self.blocks) <= CHUNK_BLOCKS
    }
}

/// One entry of a record header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Entry {
    /// Chunk blocks stored in the record's data.
    Write(Extent),
    /// Chunk blocks that read as zeros from this version on; the whole chunk deletes it.
    Punch(Extent),
    /// Opaque bytes owned by an index or a Raft shard.
    Blob {
        /// Type of the payload.
        kind: BlobKind,
        /// Index or shard the payload belongs to.
        owner: u64,
        /// Payload length in bytes.
        len: u32,
    },
}

impl Entry {
    const fn encoded_len(&self) -> usize {
        match self {
            Self::Write(_) | Self::Punch(_) => EXTENT_LEN,
            Self::Blob { .. } => BLOB_LEN,
        }
    }
}

/// Block counts of a record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Layout {
    /// Header blocks.
    pub header_blocks: u32,
    /// Data blocks.
    pub data_blocks: u32,
}

impl Layout {
    /// Layout of a record holding `entries`.
    pub fn of(entries: &[Entry]) -> Self {
        entries
            .iter()
            .fold(LayoutBuilder::default(), LayoutBuilder::with)
            .layout()
    }

    /// Header and data blocks together.
    pub const fn record_blocks(self) -> u32 {
        self.header_blocks + self.data_blocks
    }
}

/// Running totals for sizing a record one entry at a time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LayoutBuilder {
    entry_bytes: u64,
    write_blocks: u64,
    blob_bytes: u64,
}

impl LayoutBuilder {
    /// Totals with `entry` added.
    #[must_use]
    pub fn with(mut self, entry: &Entry) -> Self {
        self.entry_bytes += entry.encoded_len() as u64;
        match *entry {
            Entry::Write(e) => self.write_blocks += u64::from(e.blocks),
            Entry::Punch(_) => {}
            Entry::Blob { len, .. } => self.blob_bytes += u64::from(len),
        }
        self
    }

    /// Layout of the entries added so far.
    pub fn layout(self) -> Layout {
        let block = BLOCK_BYTES as u64;
        let data_blocks = self.write_blocks + self.blob_bytes.div_ceil(block);
        let header_bytes = PRELUDE_LEN as u64 + self.entry_bytes + 4 * data_blocks;
        Layout {
            header_blocks: u32::try_from(header_bytes.div_ceil(block)).unwrap_or(u32::MAX),
            data_blocks: u32::try_from(data_blocks).unwrap_or(u32::MAX),
        }
    }
}

/// Where a record must sit, checked against what its header claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Expected {
    /// Log the record must belong to.
    pub log_id: u64,
    /// Open sequence number of the segment holding it.
    pub segment_seq: u64,
    /// Block offset of the record within that segment.
    pub offset: u32,
    /// Blocks left in the segment from `offset`.
    pub max_blocks: u32,
}

/// Decoded record header.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordHeader {
    /// Log the record belongs to.
    pub log_id: u64,
    /// Open sequence number of the segment holding the record.
    pub segment_seq: u64,
    /// Block offset of the record within its segment.
    pub offset: u32,
    /// Log sequence number; one record per group-commit round.
    pub lsn: u64,
    /// Entries in record order.
    pub entries: Vec<Entry>,
    /// CRC32C of each data block.
    pub data_crcs: Vec<u32>,
}

impl RecordHeader {
    /// Block counts of this record.
    pub fn layout(&self) -> Layout {
        Layout::of(&self.entries)
    }

    /// Byte range of each entry's payload within the data region; empty for punches.
    pub fn data_spans(&self) -> Vec<Range<usize>> {
        data_spans(&self.entries)
    }

    /// Checks every data block against its CRC32C.
    pub fn verify_data(&self, data: &[u8]) -> Result<(), DecodeError> {
        if data.len() != self.data_crcs.len() * BLOCK_BYTES {
            return Err(DecodeError::Malformed);
        }
        for (block, (chunk, &crc)) in data
            .as_chunks::<BLOCK_BYTES>()
            .0
            .iter()
            .zip(&self.data_crcs)
            .enumerate()
        {
            if crc32c(chunk) != crc {
                return Err(DecodeError::DataChecksum {
                    block: u32::try_from(block).expect("record blocks fit u32"),
                });
            }
        }
        Ok(())
    }

    /// Payload of each blob entry, in entry order.
    pub fn blobs(&self, data: &[u8]) -> Vec<Vec<u8>> {
        self.entries
            .iter()
            .zip(self.data_spans())
            .filter(|(e, _)| matches!(e, Entry::Blob { .. }))
            .map(|(_, span)| data[span].to_vec())
            .collect()
    }
}

/// Byte range of each entry's payload within the data region of a record holding `entries`.
pub fn data_spans(entries: &[Entry]) -> Vec<Range<usize>> {
    let write_bytes: usize = entries
        .iter()
        .map(|e| match e {
            Entry::Write(x) => usize::from(x.blocks) * BLOCK_BYTES,
            _ => 0,
        })
        .sum();
    let (mut writes_at, mut blobs_at) = (0, write_bytes);
    entries
        .iter()
        .map(|e| match *e {
            Entry::Write(x) => {
                let start = writes_at;
                writes_at += usize::from(x.blocks) * BLOCK_BYTES;
                start..writes_at
            }
            Entry::Punch(_) => writes_at..writes_at,
            Entry::Blob { len, .. } => {
                let start = blobs_at;
                blobs_at += len as usize;
                start..blobs_at
            }
        })
        .collect()
}

/// Writes the header of a record whose data region in `record` is already filled; returns it decoded.
///
/// `record` must be exactly `Layout::of(entries).record_blocks()` blocks long.
pub fn encode(
    log_id: u64,
    segment_seq: u64,
    offset: u32,
    lsn: u64,
    entries: Vec<Entry>,
    record: &mut [u8],
) -> RecordHeader {
    let layout = Layout::of(&entries);
    assert_eq!(
        record.len(),
        layout.record_blocks() as usize * BLOCK_BYTES,
        "record buffer does not match its layout"
    );
    let (header, data) = record.split_at_mut(layout.header_blocks as usize * BLOCK_BYTES);
    let data_crcs: Vec<u32> = data
        .as_chunks::<BLOCK_BYTES>()
        .0
        .iter()
        .map(|block| crc32c(block))
        .collect();

    let mut out = Vec::with_capacity(header.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(
        &u16::try_from(layout.header_blocks)
            .expect("header blocks fit u16")
            .to_le_bytes(),
    );
    out.extend_from_slice(&layout.record_blocks().to_le_bytes());
    out.extend_from_slice(&log_id.to_le_bytes());
    out.extend_from_slice(&segment_seq.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(entries.len())
            .expect("entry count fits u32")
            .to_le_bytes(),
    );
    for entry in &entries {
        match *entry {
            Entry::Write(x) | Entry::Punch(x) => {
                out.push(if matches!(entry, Entry::Write(_)) {
                    TAG_WRITE
                } else {
                    TAG_PUNCH
                });
                out.extend_from_slice(&x.pool.0.to_le_bytes());
                out.extend_from_slice(&x.chunk.to_be_bytes());
                out.extend_from_slice(&x.version.to_le_bytes());
                out.extend_from_slice(&x.first_block.to_le_bytes());
                out.extend_from_slice(&x.blocks.to_le_bytes());
            }
            Entry::Blob { kind, owner, len } => {
                out.push(TAG_BLOB);
                out.push(kind.0);
                out.extend_from_slice(&owner.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
            }
        }
    }
    for crc in &data_crcs {
        out.extend_from_slice(&crc.to_le_bytes());
    }
    out.resize(header.len(), 0);
    let crc = crc_with_hole(&out, CRC_AT);
    out[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    header.copy_from_slice(&out);

    RecordHeader {
        log_id,
        segment_seq,
        offset,
        lsn,
        entries,
        data_crcs,
    }
}

/// Unverified `(header_blocks, record_blocks)` from a record's first block, to size the next read.
pub fn peek(first_block: &[u8]) -> Result<(u32, u32), DecodeError> {
    let prelude = first_block
        .get(..PRELUDE_LEN)
        .ok_or(DecodeError::Malformed)?;
    if prelude[..4] != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let mut r = Reader::new(&prelude[8..16]);
    let version = r.u16()?;
    if version != FORMAT_VERSION {
        return Err(DecodeError::BadVersion(version));
    }
    let header_blocks = u32::from(r.u16()?);
    let record_blocks = r.u32()?;
    if header_blocks == 0 || record_blocks < header_blocks {
        return Err(DecodeError::Malformed);
    }
    Ok((header_blocks, record_blocks))
}

/// Decodes and validates the header blocks of a record expected at `expected`.
pub fn decode_header(header: &[u8], expected: &Expected) -> Result<RecordHeader, DecodeError> {
    let (header_blocks, record_blocks) = peek(header)?;
    if header.len() != header_blocks as usize * BLOCK_BYTES {
        return Err(DecodeError::Malformed);
    }
    let stored = u32::from_le_bytes(header[CRC_AT..CRC_AT + 4].try_into().expect("four bytes"));
    if crc_with_hole(header, CRC_AT) != stored {
        return Err(DecodeError::BadChecksum);
    }
    let mut r = Reader::new(&header[16..]);
    let log_id = r.u64()?;
    let segment_seq = r.u64()?;
    let lsn = r.u64()?;
    let offset = r.u32()?;
    let entry_count = r.u32()?;
    if log_id != expected.log_id || segment_seq != expected.segment_seq || offset != expected.offset
    {
        return Err(DecodeError::Mismatch);
    }
    if record_blocks > expected.max_blocks || entry_count == 0 {
        return Err(DecodeError::Malformed);
    }
    if entry_count as usize > header.len() / BLOB_LEN {
        return Err(DecodeError::Malformed);
    }
    let mut entries = Vec::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        entries.push(decode_entry(&mut r)?);
    }
    let layout = Layout::of(&entries);
    if layout.header_blocks != header_blocks || layout.record_blocks() != record_blocks {
        return Err(DecodeError::Malformed);
    }
    let data_crcs = (0..layout.data_blocks)
        .map(|_| r.u32())
        .collect::<Result<_, _>>()?;
    Ok(RecordHeader {
        log_id,
        segment_seq,
        offset,
        lsn,
        entries,
        data_crcs,
    })
}

/// Decodes a whole record image (header and data) expected at `expected`.
pub fn decode(record: &[u8], expected: &Expected) -> Result<RecordHeader, DecodeError> {
    let (header_blocks, record_blocks) = peek(record)?;
    if record.len() < record_blocks as usize * BLOCK_BYTES {
        return Err(DecodeError::Malformed);
    }
    let (header, data) = record[..record_blocks as usize * BLOCK_BYTES]
        .split_at(header_blocks as usize * BLOCK_BYTES);
    let decoded = decode_header(header, expected)?;
    decoded.verify_data(data)?;
    Ok(decoded)
}

fn decode_entry(r: &mut Reader<'_>) -> Result<Entry, DecodeError> {
    let tag = r.u8()?;
    match tag {
        TAG_WRITE | TAG_PUNCH => {
            let extent = Extent {
                pool: PoolId(r.u32()?),
                chunk: ChunkId::from_be_bytes(r.take()?),
                version: r.u64()?,
                first_block: r.u16()?,
                blocks: r.u16()?,
            };
            if !extent.is_valid() {
                return Err(DecodeError::Malformed);
            }
            Ok(if tag == TAG_WRITE {
                Entry::Write(extent)
            } else {
                Entry::Punch(extent)
            })
        }
        TAG_BLOB => Ok(Entry::Blob {
            kind: BlobKind(r.u8()?),
            owner: r.u64()?,
            len: r.u32()?,
        }),
        _ => Err(DecodeError::Malformed),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::id::{InodeId, VolumeId};
    use crate::log::segment::tests::hex;
    use crate::rng::Rng;

    pub(crate) const EXPECTED: Expected = Expected {
        log_id: 0xDEAD_BEEF,
        segment_seq: 5,
        offset: 1,
        max_blocks: 1023,
    };

    pub(crate) fn extent(rng: &mut Rng, blocks: u16) -> Extent {
        let first_block = u16::try_from(rng.below(u64::from(1024 - blocks) + 1)).unwrap();
        Extent {
            pool: PoolId(rng.next_u32() % 4),
            chunk: ChunkId::new(
                VolumeId(rng.next_u32() % 8),
                InodeId(rng.below(100)),
                rng.next_u32() % 16,
            ),
            version: rng.below(1000) + 1,
            first_block,
            blocks,
        }
    }

    pub(crate) fn random_entries(rng: &mut Rng, max_data_blocks: u64) -> Vec<Entry> {
        let mut entries = Vec::new();
        let mut budget = max_data_blocks;
        for _ in 0..rng.range_inclusive(1, 5) {
            match rng.below(3) {
                0 if budget > 0 => {
                    let blocks = rng.range_inclusive(1, budget.min(3));
                    budget -= blocks;
                    entries.push(Entry::Write(extent(rng, u16::try_from(blocks).unwrap())));
                }
                1 => {
                    let blocks = u16::try_from(rng.range_inclusive(1, 512)).unwrap();
                    entries.push(Entry::Punch(extent(rng, blocks)));
                }
                _ if budget > 0 => {
                    let len = u32::try_from(rng.below(2 * BLOCK_BYTES as u64)).unwrap();
                    budget = budget.saturating_sub(2);
                    entries.push(Entry::Blob {
                        kind: BlobKind(u8::try_from(rng.below(4)).unwrap()),
                        owner: rng.next_u64(),
                        len,
                    });
                }
                _ => entries.push(Entry::Punch(extent(rng, 1))),
            }
        }
        entries
    }

    /// Encodes `entries` with random payloads; returns the decoded header and the record image.
    pub(crate) fn build(rng: &mut Rng, lsn: u64, entries: Vec<Entry>) -> (RecordHeader, Vec<u8>) {
        let layout = Layout::of(&entries);
        let mut record = vec![0u8; layout.record_blocks() as usize * BLOCK_BYTES];
        let data_at = layout.header_blocks as usize * BLOCK_BYTES;
        for span in data_spans(&entries) {
            rng.fill_bytes(&mut record[data_at + span.start..data_at + span.end]);
        }
        let header = encode(
            EXPECTED.log_id,
            EXPECTED.segment_seq,
            EXPECTED.offset,
            lsn,
            entries,
            &mut record,
        );
        (header, record)
    }

    #[test]
    fn round_trips_random_records() {
        let mut rng = Rng::from_seed(1);
        for lsn in 1..500 {
            let entries = random_entries(&mut rng, 8);
            let (header, record) = build(&mut rng, lsn, entries.clone());
            assert_eq!(decode(&record, &EXPECTED), Ok(header.clone()));
            assert_eq!(header.entries, entries);
            assert_eq!(header.lsn, lsn);
            let data = &record[header.layout().header_blocks as usize * BLOCK_BYTES..];
            let blobs = header.blobs(data);
            let blob_lens: Vec<usize> = entries
                .iter()
                .filter_map(|e| match e {
                    Entry::Blob { len, .. } => Some(*len as usize),
                    _ => None,
                })
                .collect();
            assert_eq!(blobs.iter().map(Vec::len).collect::<Vec<_>>(), blob_lens);
        }
    }

    #[test]
    fn headers_spill_into_more_blocks() {
        let mut rng = Rng::from_seed(2);
        let entries: Vec<Entry> = (0..200)
            .map(|_| Entry::Write(extent(&mut rng, 1)))
            .collect();
        let layout = Layout::of(&entries);
        assert_eq!(layout.header_blocks, 2);
        assert_eq!(layout.data_blocks, 200);
        let (header, record) = build(&mut rng, 9, entries);
        assert_eq!(decode(&record, &EXPECTED), Ok(header));
    }

    #[test]
    fn layout_counts_entries_crcs_and_packed_blobs() {
        let blob = |len| Entry::Blob {
            kind: BlobKind::SNAPSHOT,
            owner: 1,
            len,
        };
        assert_eq!(
            Layout::of(&[blob(1), blob(4095)]),
            Layout {
                header_blocks: 1,
                data_blocks: 1
            }
        );
        assert_eq!(Layout::of(&[blob(4097)]).data_blocks, 2);
        let mut rng = Rng::from_seed(3);
        let one_write = Layout::of(&[Entry::Write(extent(&mut rng, 1))]);
        assert_eq!(one_write.record_blocks(), 2);
        // Largest single-header record: 48 + 33 + 4 × 1003 = 4093 bytes.
        let max = Layout::of(&[Entry::Write(extent(&mut rng, 1003))]);
        assert_eq!(max.header_blocks, 1);
        assert_eq!(
            Layout::of(&[Entry::Write(extent(&mut rng, 1004))]).header_blocks,
            2
        );
    }

    /// For every subset of a record's blocks that landed, over every kind of old content, decoding
    /// returns the whole record or rejects it. A torn prefix is one such subset.
    #[test]
    fn torn_writes_decode_whole_or_not_at_all() {
        let mut rng = Rng::from_seed(4);
        let mut checked = 0u64;
        for case in 0..40 {
            let entries = random_entries(&mut rng, 7);
            let (header, record) = build(&mut rng, 100 + case, entries);
            let blocks = record.len() / BLOCK_BYTES;
            assert!(blocks <= 10);

            let other_entries = random_entries(&mut rng, 7);
            let (other_header, other) = build(&mut rng, 100 + case, other_entries);
            let mut random_old = vec![0u8; record.len()];
            rng.fill_bytes(&mut random_old);
            let mut other_old = vec![0u8; record.len().max(other.len())];
            other_old[..other.len()].copy_from_slice(&other);
            let olds = [vec![0u8; record.len()], random_old, other_old];

            for old in &olds {
                for landed in 0u32..(1 << blocks) {
                    let mut image = old.clone();
                    for b in 0..blocks {
                        if landed & (1 << b) != 0 {
                            let range = b * BLOCK_BYTES..(b + 1) * BLOCK_BYTES;
                            image[range.clone()].copy_from_slice(&record[range]);
                        }
                    }
                    let is_new = image[..record.len()] == record[..];
                    let is_old = image.get(..other.len()) == Some(&other[..]);
                    match decode(&image, &EXPECTED) {
                        Ok(got) => assert!(
                            (got == header && is_new) || (got == other_header && is_old),
                            "case {case} landed {landed:b}: decoded a mixture"
                        ),
                        Err(_) => assert!(!is_new, "case {case}: a landed record was rejected"),
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked > 1000);
    }

    /// Torn 512-byte sectors inside 4 KiB blocks, for drives with 512-byte atomic writes.
    #[test]
    fn torn_sectors_decode_whole_or_not_at_all() {
        const SECTOR: usize = 512;
        let mut rng = Rng::from_seed(5);
        for case in 0..300 {
            let entries = random_entries(&mut rng, 6);
            let (header, record) = build(&mut rng, case + 1, entries);
            let mut image = vec![0u8; record.len()];
            rng.fill_bytes(&mut image);
            let mut all_landed = true;
            for (s, chunk) in record.chunks(SECTOR).enumerate() {
                if rng.chance(1, 2) {
                    image[s * SECTOR..s * SECTOR + chunk.len()].copy_from_slice(chunk);
                } else {
                    all_landed = false;
                }
            }
            match decode(&image, &EXPECTED) {
                Ok(got) => assert_eq!(got, header),
                Err(_) => assert!(!all_landed),
            }
        }
    }

    // Ported from RocksDB db/log_test.cc (a3b0f187ff4b501c7fe4a67dbcc4ad6d433a454c), which keeps
    // LevelDB's log tests; each case names the original.

    fn one_write_record() -> (RecordHeader, Vec<u8>) {
        let mut rng = Rng::from_seed(6);
        let entries = vec![Entry::Write(extent(&mut rng, 2))];
        build(&mut rng, 3, entries)
    }

    fn refresh_crc(record: &mut [u8], header_blocks: usize) {
        let crc = crc_with_hole(&record[..header_blocks * BLOCK_BYTES], CRC_AT);
        record[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn rocksdb_checksum_mismatch() {
        let (_, record) = one_write_record();
        for byte in 0..record.len() {
            let mut copy = record.clone();
            copy[byte] ^= 0x10;
            assert!(decode(&copy, &EXPECTED).is_err(), "byte {byte}");
        }
    }

    #[test]
    fn rocksdb_bad_record_type() {
        let (_, mut record) = one_write_record();
        record[PRELUDE_LEN] = 100;
        refresh_crc(&mut record, 1);
        assert_eq!(decode(&record, &EXPECTED), Err(DecodeError::Malformed));
    }

    #[test]
    fn rocksdb_bad_length() {
        let (_, mut record) = one_write_record();
        record[12] += 1;
        refresh_crc(&mut record, 1);
        assert_eq!(decode(&record, &EXPECTED), Err(DecodeError::Malformed));
    }

    #[test]
    fn rocksdb_truncated_trailing_record_is_ignored() {
        let (_, record) = one_write_record();
        for len in [0, 4, PRELUDE_LEN, BLOCK_BYTES, record.len() - 1] {
            assert!(decode(&record[..len], &EXPECTED).is_err(), "{len}");
        }
    }

    #[test]
    fn rocksdb_bad_length_at_end_is_ignored() {
        let (_, record) = one_write_record();
        let tight = Expected {
            max_blocks: 2,
            ..EXPECTED
        };
        assert_eq!(decode(&record, &tight), Err(DecodeError::Malformed));
    }

    #[test]
    fn rocksdb_recycle_rejects_records_of_an_earlier_incarnation() {
        let (_, record) = one_write_record();
        for expected in [
            Expected {
                segment_seq: EXPECTED.segment_seq + 1,
                ..EXPECTED
            },
            Expected {
                log_id: EXPECTED.log_id + 1,
                ..EXPECTED
            },
            Expected {
                offset: EXPECTED.offset + 2,
                ..EXPECTED
            },
        ] {
            assert_eq!(decode(&record, &expected), Err(DecodeError::Mismatch));
        }
    }

    #[test]
    fn golden_bytes_match_checked_in_fixture() {
        let entries = vec![
            Entry::Write(Extent {
                pool: PoolId(1),
                chunk: ChunkId::new(VolumeId(2), InodeId(3), 4),
                version: 5,
                first_block: 6,
                blocks: 1,
            }),
            Entry::Punch(Extent {
                pool: PoolId(1),
                chunk: ChunkId::new(VolumeId(2), InodeId(3), 4),
                version: 6,
                first_block: 0,
                blocks: 1024,
            }),
            Entry::Blob {
                kind: BlobKind::MANIFEST,
                owner: 7,
                len: 3,
            },
        ];
        let mut record = vec![0u8; Layout::of(&entries).record_blocks() as usize * BLOCK_BYTES];
        record[BLOCK_BYTES..2 * BLOCK_BYTES].fill(0xAB);
        record[2 * BLOCK_BYTES..2 * BLOCK_BYTES + 3].copy_from_slice(b"abc");
        encode(0x0123_4567_89AB_CDEF, 9, 1, 10, entries, &mut record);
        let used = PRELUDE_LEN + 2 * EXTENT_LEN + BLOB_LEN + 2 * 4;
        let fixture = include_str!("../../testdata/log_format_golden.txt");
        let golden = fixture
            .lines()
            .find_map(|l| l.strip_prefix("record_header "))
            .unwrap();
        assert_eq!(hex(&record[..used]), golden.trim());
        assert!(record[used..BLOCK_BYTES].iter().all(|&b| b == 0));
    }
}
