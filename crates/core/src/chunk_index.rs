//! Per-disk chunk index: `(pool, h, chunk_id, version, first block) → (location, block count,
//! record header)`, kept in an LSM [`Tree`] and rebuilt from record headers when lost.

use crate::event::{BLOCK_BYTES, Lba};
use crate::id::{ChunkId, PoolId};
use crate::log::LogRecord;
use crate::log::record::{Entry, Extent};
use crate::lsm::{MemTree, Tree};

const KEY_LEN: usize = 38;
const VALUE_LEN: usize = 19;
const KIND_DATA: u8 = 0;
const KIND_ZERO: u8 = 1;

/// Key of one extent; byte order equals `(pool, chunk hash, chunk, version, first_block)` order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExtentKey {
    /// Pool the chunk belongs to.
    pub pool: PoolId,
    /// Chunk.
    pub chunk: ChunkId,
    /// Chunk version.
    pub version: u64,
    /// First block of the extent within the chunk.
    pub first_block: u16,
}

impl ExtentKey {
    /// Encodes the key as `pool | xxh3(chunk) | chunk | version | first_block`, all big-endian.
    pub fn encode(&self) -> Vec<u8> {
        let mut key = chunk_prefix(self.pool, self.chunk);
        key.extend_from_slice(&self.version.to_be_bytes());
        key.extend_from_slice(&self.first_block.to_be_bytes());
        key
    }

    /// Decodes a key written by [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; KEY_LEN] = bytes.try_into().ok()?;
        Some(Self {
            pool: PoolId(u32::from_be_bytes(bytes[0..4].try_into().ok()?)),
            chunk: ChunkId::from_be_bytes(bytes[12..28].try_into().ok()?),
            version: u64::from_be_bytes(bytes[28..36].try_into().ok()?),
            first_block: u16::from_be_bytes(bytes[36..38].try_into().ok()?),
        })
    }
}

/// Where an extent's blocks are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExtentValue {
    /// Blocks stored on disk; their CRCs are in the record header at `header`.
    Data {
        /// Number of blocks.
        blocks: u16,
        /// Disk address of the first block.
        data: Lba,
        /// Disk address of the record header.
        header: Lba,
    },
    /// Punched blocks, which read as zeros.
    Zero {
        /// Number of blocks.
        blocks: u16,
        /// Disk address of the record header that punched them.
        header: Lba,
    },
}

impl ExtentValue {
    fn encode(self) -> Vec<u8> {
        let (kind, blocks, data, header) = match self {
            Self::Data {
                blocks,
                data,
                header,
            } => (KIND_DATA, blocks, data, header),
            Self::Zero { blocks, header } => (KIND_ZERO, blocks, Lba(0), header),
        };
        let mut value = Vec::with_capacity(VALUE_LEN);
        value.push(kind);
        value.extend_from_slice(&blocks.to_le_bytes());
        value.extend_from_slice(&data.0.to_le_bytes());
        value.extend_from_slice(&header.0.to_le_bytes());
        value
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; VALUE_LEN] = bytes.try_into().ok()?;
        let blocks = u16::from_le_bytes(bytes[1..3].try_into().ok()?);
        let data = Lba(u64::from_le_bytes(bytes[3..11].try_into().ok()?));
        let header = Lba(u64::from_le_bytes(bytes[11..19].try_into().ok()?));
        match bytes[0] {
            KIND_DATA => Some(Self::Data {
                blocks,
                data,
                header,
            }),
            KIND_ZERO => Some(Self::Zero { blocks, header }),
            _ => None,
        }
    }
}

fn chunk_prefix(pool: PoolId, chunk: ChunkId) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_LEN);
    key.extend_from_slice(&pool.0.to_be_bytes());
    key.extend_from_slice(&chunk.hash().to_be_bytes());
    key.extend_from_slice(&chunk.to_be_bytes());
    key
}

/// Extent map of every chunk on one disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChunkIndex<T = MemTree> {
    tree: T,
}

impl<T: Tree> ChunkIndex<T> {
    /// Wraps `tree`, which may hold a restored snapshot.
    pub const fn new(tree: T) -> Self {
        Self { tree }
    }

    /// The underlying tree, for checkpoints.
    pub const fn tree(&self) -> &T {
        &self.tree
    }

    /// Adds the extents written or punched by a durable record. Records must come in LSN order.
    pub fn apply(&mut self, record: &LogRecord) {
        let header_blocks = u64::from(record.header.layout().header_blocks);
        let spans = record.header.data_spans();
        for (entry, span) in record.header.entries.iter().zip(spans) {
            let (extent, value) = match *entry {
                Entry::Write(extent) => (
                    extent,
                    ExtentValue::Data {
                        blocks: extent.blocks,
                        data: record
                            .lba
                            .advance(header_blocks + (span.start / BLOCK_BYTES) as u64),
                        header: record.lba,
                    },
                ),
                Entry::Punch(extent) => (
                    extent,
                    ExtentValue::Zero {
                        blocks: extent.blocks,
                        header: record.lba,
                    },
                ),
                Entry::Blob { .. } => continue,
            };
            self.tree.insert(key_of(&extent).encode(), value.encode());
        }
    }

    /// The extent stored under `key`.
    pub fn get(&self, key: &ExtentKey) -> Option<ExtentValue> {
        ExtentValue::decode(self.tree.get(&key.encode())?)
    }

    /// Every extent of `chunk` in `pool`, ordered by version then first block.
    pub fn extents(&self, pool: PoolId, chunk: ChunkId) -> Vec<(ExtentKey, ExtentValue)> {
        let prefix = chunk_prefix(pool, chunk);
        self.tree
            .scan(&prefix)
            .filter_map(|(k, v)| Some((ExtentKey::decode(k)?, ExtentValue::decode(v)?)))
            .collect()
    }
}

const fn key_of(extent: &Extent) -> ExtentKey {
    ExtentKey {
        pool: extent.pool,
        chunk: extent.chunk,
        version: extent.version,
        first_block: extent.first_block,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{InodeId, VolumeId};
    use crate::log::RecordPos;
    use crate::log::record::{BlobKind, Extent, Layout, RecordHeader};
    use crate::rng::Rng;

    fn random_key(rng: &mut Rng) -> ExtentKey {
        ExtentKey {
            pool: PoolId(rng.next_u32() % 3),
            chunk: ChunkId::new(
                VolumeId(rng.next_u32() % 3),
                InodeId(rng.below(3)),
                rng.next_u32() % 3,
            ),
            version: rng.next_u64() >> rng.below(64),
            first_block: u16::try_from(rng.below(1024)).unwrap(),
        }
    }

    #[test]
    fn key_byte_order_equals_logical_order() {
        let mut rng = Rng::from_seed(21);
        let logical = |k: &ExtentKey| (k.pool, k.chunk.hash(), k.chunk, k.version, k.first_block);
        for _ in 0..5000 {
            let (a, b) = (random_key(&mut rng), random_key(&mut rng));
            assert_eq!(a.encode().cmp(&b.encode()), logical(&a).cmp(&logical(&b)));
            assert_eq!(ExtentKey::decode(&a.encode()), Some(a));
        }
    }

    fn extent(chunk: u32, version: u64, first_block: u16, blocks: u16) -> Extent {
        Extent {
            pool: PoolId(1),
            chunk: ChunkId::new(VolumeId(1), InodeId(1), chunk),
            version,
            first_block,
            blocks,
        }
    }

    fn record(lba: u64, entries: Vec<Entry>) -> LogRecord {
        let data_blocks = Layout::of(&entries).data_blocks as usize;
        LogRecord {
            pos: RecordPos {
                segment: 1,
                block: 1,
                lsn: 1,
            },
            lba: Lba(lba),
            header: RecordHeader {
                log_id: 1,
                segment_seq: 1,
                offset: 1,
                lsn: 1,
                entries,
                data_crcs: vec![0; data_blocks],
            },
            blobs: Vec::new(),
        }
    }

    #[test]
    fn writes_and_punches_map_to_their_blocks() {
        let mut index = ChunkIndex::new(MemTree::new());
        index.apply(&record(
            100,
            vec![
                Entry::Write(extent(7, 1, 0, 2)),
                Entry::Blob {
                    kind: BlobKind(9),
                    owner: 3,
                    len: 10,
                },
                Entry::Write(extent(7, 2, 5, 1)),
                Entry::Punch(extent(8, 3, 0, 1024)),
            ],
        ));
        let pool = PoolId(1);
        let chunk7 = ChunkId::new(VolumeId(1), InodeId(1), 7);
        assert_eq!(
            index.extents(pool, chunk7),
            vec![
                (
                    key_of(&extent(7, 1, 0, 2)),
                    ExtentValue::Data {
                        blocks: 2,
                        data: Lba(101),
                        header: Lba(100)
                    }
                ),
                (
                    key_of(&extent(7, 2, 5, 1)),
                    ExtentValue::Data {
                        blocks: 1,
                        data: Lba(103),
                        header: Lba(100)
                    }
                ),
            ]
        );
        assert_eq!(
            index.get(&key_of(&extent(8, 3, 0, 1024))),
            Some(ExtentValue::Zero {
                blocks: 1024,
                header: Lba(100)
            })
        );
    }

    #[test]
    fn a_later_record_moves_an_extent() {
        let mut index = ChunkIndex::new(MemTree::new());
        index.apply(&record(100, vec![Entry::Write(extent(7, 1, 0, 1))]));
        index.apply(&record(200, vec![Entry::Write(extent(7, 1, 0, 1))]));
        assert_eq!(
            index.get(&key_of(&extent(7, 1, 0, 1))),
            Some(ExtentValue::Data {
                blocks: 1,
                data: Lba(201),
                header: Lba(200)
            })
        );
    }
}
