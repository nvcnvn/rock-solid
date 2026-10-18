//! Manifest: where recovery starts replaying and which blobs hold each index's snapshot.

use super::{DecodeError, FORMAT_VERSION, Reader, RecordPos};

/// A blob entry inside a durable record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRef {
    /// Record holding the blob.
    pub record: RecordPos,
    /// Index of the blob's entry in the record.
    pub entry: u16,
}

/// Bytes one [`BlobRef`] adds to an encoded manifest.
pub const BLOB_REF_LEN: usize = 18;

/// Replay start and snapshot locations, written as a [`BlobKind::MANIFEST`](super::record::BlobKind::MANIFEST) blob.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Manifest {
    /// Last LSN the snapshots include; replay starts after it.
    pub base_lsn: u64,
    /// Highest segment sequence open at the base; newer segments are replayed from their start.
    pub base_seq: u64,
    /// Per stream, the `(segment, block)` its next record went to at the base.
    pub starts: [Option<(u32, u32)>; 2],
    /// Each index's snapshot pieces, in order.
    pub trees: Vec<(u64, Vec<BlobRef>)>,
}

impl Manifest {
    /// Encodes the manifest.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.base_lsn.to_le_bytes());
        out.extend_from_slice(&self.base_seq.to_le_bytes());
        for start in self.starts {
            let (segment, block) = start.unwrap_or((0, 0));
            out.extend_from_slice(&segment.to_le_bytes());
            out.extend_from_slice(&block.to_le_bytes());
        }
        out.extend_from_slice(&len_u32(self.trees.len()).to_le_bytes());
        for (owner, refs) in &self.trees {
            out.extend_from_slice(&owner.to_le_bytes());
            out.extend_from_slice(&len_u32(refs.len()).to_le_bytes());
            for r in refs {
                out.extend_from_slice(&r.record.segment.to_le_bytes());
                out.extend_from_slice(&r.record.block.to_le_bytes());
                out.extend_from_slice(&r.record.lsn.to_le_bytes());
                out.extend_from_slice(&r.entry.to_le_bytes());
            }
        }
        out
    }

    /// Decodes a manifest blob; its record's CRCs already vouch for the bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u16()?;
        if version != FORMAT_VERSION {
            return Err(DecodeError::BadVersion(version));
        }
        let base_lsn = r.u64()?;
        let base_seq = r.u64()?;
        let mut starts = [None; 2];
        for start in &mut starts {
            let segment = r.u32()?;
            let block = r.u32()?;
            *start = (segment != 0).then_some((segment, block));
        }
        let mut trees = Vec::new();
        for _ in 0..r.u32()? {
            let owner = r.u64()?;
            let mut refs = Vec::new();
            for _ in 0..r.u32()? {
                refs.push(BlobRef {
                    record: RecordPos {
                        segment: r.u32()?,
                        block: r.u32()?,
                        lsn: r.u64()?,
                    },
                    entry: r.u16()?,
                });
            }
            trees.push((owner, refs));
        }
        if !r.is_empty() {
            return Err(DecodeError::Malformed);
        }
        Ok(Self {
            base_lsn,
            base_seq,
            starts,
            trees,
        })
    }
}

fn len_u32(len: usize) -> u32 {
    u32::try_from(len).expect("manifest counts fit u32")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_rejects_trailing_bytes() {
        let manifest = Manifest {
            base_lsn: 77,
            base_seq: 5,
            starts: [Some((3, 17)), None],
            trees: vec![
                (
                    1,
                    vec![BlobRef {
                        record: RecordPos {
                            segment: 4,
                            block: 2,
                            lsn: 78,
                        },
                        entry: 3,
                    }],
                ),
                (2, Vec::new()),
            ],
        };
        let mut bytes = manifest.encode();
        assert_eq!(Manifest::decode(&bytes), Ok(manifest));
        bytes.push(0);
        assert_eq!(Manifest::decode(&bytes), Err(DecodeError::Malformed));
        assert_eq!(Manifest::decode(&bytes[..10]), Err(DecodeError::Malformed));
    }
}
