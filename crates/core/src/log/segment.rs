//! Disk layout: segment geometry, the superblock in segment 0 and the header of every other segment.

use core::fmt;

use super::{DecodeError, FORMAT_VERSION, Reader, RecordPos, Stream, crc_with_hole};
use crate::event::{BLOCK_BYTES, Lba};

/// Largest segment size in bytes.
pub const MAX_SEGMENT_BYTES: u64 = 64 << 20;

/// Segment count a disk is divided into when segments are below their maximum size.
pub const TARGET_SEGMENTS: u64 = 4096;

/// Smallest segment: a segment header plus a record of one header and one data block, rounded up.
pub const MIN_SEGMENT_BLOCKS: u32 = 4;

/// Fewest segments a log needs: the superblock, one open segment per stream and one to roll into.
pub const MIN_SEGMENTS: u32 = 4;

const BLOCK: u64 = BLOCK_BYTES as u64;
const SUPERBLOCK_MAGIC: [u8; 8] = *b"RSOLIDSB";
const SEGMENT_MAGIC: [u8; 8] = *b"RSOLIDSG";
const CRC_AT: usize = 12;
const SUPERBLOCK_LEN: usize = 32;
const SEGMENT_HEADER_LEN: usize = 56;

/// Error returned when a disk cannot hold [`MIN_SEGMENTS`] segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DiskTooSmall {
    /// Disk size in 4 KiB blocks.
    pub disk_blocks: u64,
}

impl fmt::Display for DiskTooSmall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "disk of {} blocks holds fewer than {MIN_SEGMENTS} segments",
            self.disk_blocks
        )
    }
}

impl core::error::Error for DiskTooSmall {}

/// Segment size and count of a disk; segment 0 holds the superblock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Geometry {
    segment_blocks: u32,
    segment_count: u32,
}

impl Geometry {
    /// Segments of min(64 MiB, disk ÷ 4096) rounded down to a power of two, at least [`MIN_SEGMENT_BLOCKS`].
    pub fn for_disk(disk_blocks: u64) -> Result<Self, DiskTooSmall> {
        let target = (disk_blocks / TARGET_SEGMENTS)
            .clamp(u64::from(MIN_SEGMENT_BLOCKS), MAX_SEGMENT_BYTES / BLOCK);
        let segment_blocks = 1u64 << target.ilog2();
        let segment_count = u32::try_from(disk_blocks / segment_blocks)
            .ok()
            .filter(|&count| count >= MIN_SEGMENTS)
            .ok_or(DiskTooSmall { disk_blocks })?;
        Ok(Self {
            segment_blocks: u32::try_from(segment_blocks).expect("at most 16384 blocks"),
            segment_count,
        })
    }

    /// Blocks per segment.
    pub const fn segment_blocks(self) -> u32 {
        self.segment_blocks
    }

    /// Segments on the disk, segment 0 included.
    pub const fn segment_count(self) -> u32 {
        self.segment_count
    }

    /// Disk address of `block` within `segment`.
    pub const fn lba(self, segment: u32, block: u32) -> Lba {
        Lba(segment as u64 * self.segment_blocks as u64 + block as u64)
    }
}

/// Block 0 of the disk: identifies the log and fixes its geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Superblock {
    /// Random identifier of this log; every segment and record header repeats it.
    pub log_id: u64,
    /// Segment layout chosen at format time.
    pub geometry: Geometry,
}

impl Superblock {
    /// Encodes the superblock into the start of a zeroed block.
    pub fn encode(&self, block: &mut [u8]) {
        let out = &mut block[..SUPERBLOCK_LEN];
        out[0..8].copy_from_slice(&SUPERBLOCK_MAGIC);
        out[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[10..12].fill(0);
        out[12..16].fill(0);
        out[16..24].copy_from_slice(&self.log_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.geometry.segment_blocks.to_le_bytes());
        out[28..32].copy_from_slice(&self.geometry.segment_count.to_le_bytes());
        let crc = crc_with_hole(out, CRC_AT);
        out[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decodes and validates a superblock.
    pub fn decode(block: &[u8]) -> Result<Self, DecodeError> {
        let bytes = check_header(block, SUPERBLOCK_MAGIC, SUPERBLOCK_LEN)?;
        let mut r = Reader::new(&bytes[16..]);
        let log_id = r.u64()?;
        let segment_blocks = r.u32()?;
        let segment_count = r.u32()?;
        if !segment_blocks.is_power_of_two()
            || segment_blocks < MIN_SEGMENT_BLOCKS
            || u64::from(segment_blocks) > MAX_SEGMENT_BYTES / BLOCK
            || segment_count < MIN_SEGMENTS
        {
            return Err(DecodeError::Malformed);
        }
        Ok(Self {
            log_id,
            geometry: Geometry {
                segment_blocks,
                segment_count,
            },
        })
    }
}

/// Block 0 of every segment but segment 0, written with the first record when the segment opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SegmentHeader {
    /// Log the segment belongs to.
    pub log_id: u64,
    /// Open sequence number, unique within the log and increasing; records repeat it.
    pub seq: u64,
    /// Index of this segment, which catches misdirected writes.
    pub index: u32,
    /// Stream the segment's records belong to.
    pub stream: Stream,
    /// Newest durable manifest when the segment opened.
    pub manifest: Option<RecordPos>,
}

impl SegmentHeader {
    /// Encodes the header into the start of a zeroed block.
    pub fn encode(&self, block: &mut [u8]) {
        let out = &mut block[..SEGMENT_HEADER_LEN];
        let manifest = self.manifest.unwrap_or(RecordPos {
            segment: 0,
            block: 0,
            lsn: 0,
        });
        out[0..8].copy_from_slice(&SEGMENT_MAGIC);
        out[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[10] = self.stream as u8;
        out[11] = 0;
        out[12..16].fill(0);
        out[16..24].copy_from_slice(&self.log_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.seq.to_le_bytes());
        out[32..36].copy_from_slice(&self.index.to_le_bytes());
        out[36..40].copy_from_slice(&manifest.segment.to_le_bytes());
        out[40..44].copy_from_slice(&manifest.block.to_le_bytes());
        out[44..48].fill(0);
        out[48..56].copy_from_slice(&manifest.lsn.to_le_bytes());
        let crc = crc_with_hole(out, CRC_AT);
        out[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decodes a segment header; segment 0 in the manifest fields means no manifest.
    pub fn decode(block: &[u8]) -> Result<Self, DecodeError> {
        let bytes = check_header(block, SEGMENT_MAGIC, SEGMENT_HEADER_LEN)?;
        let stream = Stream::from_tag(bytes[10]).ok_or(DecodeError::Malformed)?;
        let mut r = Reader::new(&bytes[16..]);
        let log_id = r.u64()?;
        let seq = r.u64()?;
        let index = r.u32()?;
        let segment = r.u32()?;
        let block = r.u32()?;
        r.u32()?;
        let lsn = r.u64()?;
        if seq == 0 || index == 0 {
            return Err(DecodeError::Malformed);
        }
        Ok(Self {
            log_id,
            seq,
            index,
            stream,
            manifest: (segment != 0).then_some(RecordPos {
                segment,
                block,
                lsn,
            }),
        })
    }
}

fn check_header(block: &[u8], magic: [u8; 8], len: usize) -> Result<&[u8], DecodeError> {
    let bytes = block.get(..len).ok_or(DecodeError::Malformed)?;
    if bytes[0..8] != magic {
        return Err(DecodeError::BadMagic);
    }
    let version = u16::from_le_bytes([bytes[8], bytes[9]]);
    if version != FORMAT_VERSION {
        return Err(DecodeError::BadVersion(version));
    }
    let stored = u32::from_le_bytes(
        bytes[CRC_AT..CRC_AT + 4]
            .try_into()
            .expect("four checksum bytes"),
    );
    if crc_with_hole(bytes, CRC_AT) != stored {
        return Err(DecodeError::BadChecksum);
    }
    Ok(bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn blocks(bytes: u64) -> u64 {
        bytes / BLOCK
    }

    #[test]
    fn segment_size_follows_disk_size() {
        let cases = [
            (16 * 1024 * GIB, 64 << 20, 262_144),
            (1024 * GIB, 64 << 20, 16_384),
            (256 * GIB, 64 << 20, 4096),
            (255 * GIB, 32 << 20, 8160),
            (16 * GIB, 4 << 20, 4096),
            (GIB, 256 << 10, 4096),
            (GIB + 3 * BLOCK, 256 << 10, 4096),
            (64 << 20, 16 << 10, 4096),
            (1 << 20, 16 << 10, 64),
            (64 << 10, 16 << 10, 4),
        ];
        for (disk, segment_bytes, count) in cases {
            let g = Geometry::for_disk(blocks(disk)).unwrap();
            assert_eq!(
                u64::from(g.segment_blocks()) * BLOCK,
                segment_bytes,
                "{disk}"
            );
            assert_eq!(g.segment_count(), count, "{disk}");
        }
        assert_eq!(
            Geometry::for_disk(15),
            Err(DiskTooSmall { disk_blocks: 15 })
        );
    }

    #[test]
    fn lba_of_segment_block() {
        let g = Geometry::for_disk(blocks(GIB)).unwrap();
        assert_eq!(g.lba(0, 0), Lba(0));
        assert_eq!(g.lba(3, 5), Lba(3 * 64 + 5));
    }

    fn segment_header() -> SegmentHeader {
        SegmentHeader {
            log_id: 0x0123_4567_89AB_CDEF,
            seq: 7,
            index: 3,
            stream: Stream::Cold,
            manifest: Some(RecordPos {
                segment: 2,
                block: 9,
                lsn: 41,
            }),
        }
    }

    #[test]
    fn headers_round_trip() {
        let sb = Superblock {
            log_id: 42,
            geometry: Geometry::for_disk(blocks(GIB)).unwrap(),
        };
        let mut block = vec![0; BLOCK_BYTES];
        sb.encode(&mut block);
        assert_eq!(Superblock::decode(&block), Ok(sb));

        for header in [
            segment_header(),
            SegmentHeader {
                manifest: None,
                stream: Stream::Hot,
                ..segment_header()
            },
        ] {
            let mut block = vec![0; BLOCK_BYTES];
            header.encode(&mut block);
            assert_eq!(SegmentHeader::decode(&block), Ok(header));
        }
    }

    #[test]
    fn every_flipped_bit_is_rejected() {
        let mut block = vec![0; BLOCK_BYTES];
        segment_header().encode(&mut block);
        for bit in 0..SEGMENT_HEADER_LEN * 8 {
            let mut copy = block.clone();
            copy[bit / 8] ^= 1 << (bit % 8);
            assert!(SegmentHeader::decode(&copy).is_err(), "bit {bit}");
        }
        assert_eq!(
            SegmentHeader::decode(&[0; BLOCK_BYTES]),
            Err(DecodeError::BadMagic)
        );
    }

    #[test]
    fn unknown_stream_tag_is_rejected_even_with_a_valid_checksum() {
        let mut block = vec![0; BLOCK_BYTES];
        segment_header().encode(&mut block);
        block[10] = 2;
        let crc = crc_with_hole(&block[..SEGMENT_HEADER_LEN], CRC_AT);
        block[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(SegmentHeader::decode(&block), Err(DecodeError::Malformed));
    }

    #[test]
    fn golden_bytes_match_checked_in_fixture() {
        let fixture = include_str!("../../testdata/log_format_golden.txt");
        let golden = |name: &str| {
            fixture
                .lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
                .unwrap()
                .trim()
                .to_owned()
        };
        let mut block = vec![0; BLOCK_BYTES];
        Superblock {
            log_id: 0x0123_4567_89AB_CDEF,
            geometry: Geometry::for_disk(blocks(GIB)).unwrap(),
        }
        .encode(&mut block);
        assert_eq!(hex(&block[..SUPERBLOCK_LEN]), golden("superblock"));

        let mut block = vec![0; BLOCK_BYTES];
        segment_header().encode(&mut block);
        assert_eq!(hex(&block[..SEGMENT_HEADER_LEN]), golden("segment_header"));
    }

    pub(crate) fn hex(bytes: &[u8]) -> String {
        use core::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut s, b| {
            write!(s, "{b:02x}").unwrap();
            s
        })
    }
}
