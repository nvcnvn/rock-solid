//! Ordered key-value trees kept in the disk log. Stage 1 holds everything in a memtable and
//! checkpoints it as a full snapshot; stage 2 will add sorted runs and compaction behind [`Tree`].

use std::collections::BTreeMap;

use crate::log::DecodeError;

/// An ordered key-value index whose durable form lives in the disk log.
pub trait Tree {
    /// Value stored under `key`.
    fn get(&self, key: &[u8]) -> Option<&[u8]>;

    /// Entries whose key starts with `prefix`, in key order.
    fn scan<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a;

    /// Stores `value` under `key`, replacing any previous value.
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);

    /// Removes `key`.
    fn remove(&mut self, key: &[u8]);

    /// Pieces of at most `max_piece` bytes that [`restore`](Self::restore) turns back into this tree.
    fn checkpoint(&self, max_piece: usize) -> Vec<Vec<u8>>;

    /// Replaces the contents with those saved by [`checkpoint`](Self::checkpoint).
    fn restore(&mut self, pieces: &[Vec<u8>]) -> Result<(), DecodeError>;
}

/// Stage 1 tree: a memtable saved whole at every checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemTree {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemTree {
    /// Creates an empty tree.
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Tree for MemTree {
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    fn scan<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.entries
            .range(prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.entries.insert(key, value);
    }

    fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
    }

    /// Each piece is a run of `key_len u16 | key | value_len u32 | value`.
    fn checkpoint(&self, max_piece: usize) -> Vec<Vec<u8>> {
        let mut pieces = Vec::new();
        let mut piece = Vec::new();
        for (key, value) in &self.entries {
            let len = 6 + key.len() + value.len();
            assert!(len <= max_piece, "entry of {len} bytes exceeds a piece");
            if piece.len() + len > max_piece {
                pieces.push(core::mem::take(&mut piece));
            }
            piece.extend_from_slice(
                &u16::try_from(key.len())
                    .expect("keys fit u16")
                    .to_le_bytes(),
            );
            piece.extend_from_slice(key);
            piece.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("values fit u32")
                    .to_le_bytes(),
            );
            piece.extend_from_slice(value);
        }
        if !piece.is_empty() {
            pieces.push(piece);
        }
        pieces
    }

    fn restore(&mut self, pieces: &[Vec<u8>]) -> Result<(), DecodeError> {
        let mut entries = BTreeMap::new();
        for piece in pieces {
            let mut rest = piece.as_slice();
            while !rest.is_empty() {
                let key_len = usize::from(u16::from_le_bytes(take(&mut rest)?));
                let key = take_slice(&mut rest, key_len)?.to_vec();
                let value_len = u32::from_le_bytes(take(&mut rest)?) as usize;
                let value = take_slice(&mut rest, value_len)?.to_vec();
                entries.insert(key, value);
            }
        }
        self.entries = entries;
        Ok(())
    }
}

fn take<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], DecodeError> {
    let bytes = take_slice(rest, N)?;
    bytes.try_into().map_err(|_| DecodeError::Malformed)
}

fn take_slice<'a>(rest: &mut &'a [u8], len: usize) -> Result<&'a [u8], DecodeError> {
    if rest.len() < len {
        return Err(DecodeError::Malformed);
    }
    let (head, tail) = rest.split_at(len);
    *rest = tail;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;

    #[test]
    fn checkpoint_restores_random_trees_at_any_piece_size() {
        let mut rng = Rng::from_seed(11);
        for round in 0..50 {
            let mut tree = MemTree::new();
            for _ in 0..rng.below(200) {
                let mut key = vec![0; usize::try_from(rng.range_inclusive(1, 40)).unwrap()];
                rng.fill_bytes(&mut key);
                let mut value = vec![0; usize::try_from(rng.below(60)).unwrap()];
                rng.fill_bytes(&mut value);
                if rng.chance(1, 5) {
                    tree.remove(&key);
                } else {
                    tree.insert(key, value);
                }
            }
            let max_piece = 106 + usize::try_from(rng.below(4000)).unwrap();
            let pieces = tree.checkpoint(max_piece);
            assert!(pieces.iter().all(|p| p.len() <= max_piece && !p.is_empty()));
            let mut restored = MemTree::new();
            restored.insert(b"stale".to_vec(), Vec::new());
            restored.restore(&pieces).unwrap();
            assert_eq!(restored, tree, "round {round}");
        }
    }

    #[test]
    fn scan_returns_prefix_in_order() {
        let mut tree = MemTree::new();
        for key in [&b"ab"[..], b"a", b"abc", b"b", b"aa"] {
            tree.insert(key.to_vec(), key.to_vec());
        }
        let keys: Vec<&[u8]> = tree.scan(b"a").map(|(k, _)| k).collect();
        assert_eq!(keys, [&b"a"[..], b"aa", b"ab", b"abc"]);
        assert_eq!(tree.scan(b"ab").count(), 2);
        assert_eq!(tree.get(b"b"), Some(&b"b"[..]));
    }

    #[test]
    fn restore_rejects_truncated_pieces() {
        let mut tree = MemTree::new();
        tree.insert(b"key".to_vec(), b"value".to_vec());
        let mut pieces = tree.checkpoint(4096);
        pieces[0].pop();
        assert_eq!(MemTree::new().restore(&pieces), Err(DecodeError::Malformed));
    }
}
