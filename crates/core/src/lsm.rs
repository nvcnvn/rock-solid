//! Ordered key-value trees kept in the disk log. Stage 1 holds everything in a memtable and
//! checkpoints it as a full snapshot; stage 2 will add sorted runs and compaction behind [`Tree`].

use core::borrow::Borrow;
use core::cmp::Ordering;
use core::ops::Bound;
use std::collections::BTreeSet;

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

    /// Freezes the contents for a snapshot that [`next_piece`](Self::next_piece) hands out while
    /// inserts and removes go on; drops any unfinished snapshot.
    fn begin_snapshot(&mut self);

    /// The snapshot's next piece of at most `max_piece` bytes, allocated at its final length;
    /// `None` once every entry is out, which ends the snapshot.
    fn next_piece(&mut self, max_piece: usize) -> Option<Vec<u8>>;

    /// Drops an unfinished snapshot, such as one whose checkpoint failed.
    fn end_snapshot(&mut self);

    /// Replaces the contents with the pieces of a snapshot, in order.
    fn restore(&mut self, pieces: &[Vec<u8>]) -> Result<(), DecodeError>;
}

/// One allocation per entry: `key_len u16 | key | value`, ordered and compared by key.
#[derive(Clone, Debug)]
struct Entry(Box<[u8]>);

impl Entry {
    fn new(key: &[u8], value: &[u8]) -> Self {
        let key_len = u16::try_from(key.len()).expect("keys fit u16");
        let mut bytes = Vec::with_capacity(2 + key.len() + value.len());
        bytes.extend_from_slice(&key_len.to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        Self(bytes.into_boxed_slice())
    }

    fn key_len(&self) -> usize {
        usize::from(u16::from_le_bytes([self.0[0], self.0[1]]))
    }

    fn key(&self) -> &[u8] {
        &self.0[2..2 + self.key_len()]
    }

    fn value(&self) -> &[u8] {
        self.split().1
    }

    fn split(&self) -> (&[u8], &[u8]) {
        self.0[2..].split_at(self.key_len())
    }

    /// Bytes of the entry in a snapshot piece: `key_len u16 | key | value_len u32 | value`.
    fn piece_len(&self) -> usize {
        self.0.len() + 4
    }

    fn encode_into(&self, piece: &mut Vec<u8>) {
        let value = self.value();
        piece.extend_from_slice(&self.0[..2 + self.key_len()]);
        piece.extend_from_slice(
            &u32::try_from(value.len())
                .expect("values fit u32")
                .to_le_bytes(),
        );
        piece.extend_from_slice(value);
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(other.key())
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Borrow<[u8]> for Entry {
    fn borrow(&self) -> &[u8] {
        self.key()
    }
}

/// What a running snapshot still has to hand out, beyond the live entries after `cursor`.
#[derive(Clone, Debug, Default)]
struct Snapshot {
    /// Last key handed out; `None` before the first piece.
    cursor: Option<Box<[u8]>>,
    /// Frozen entries after `cursor` that were since replaced or removed.
    replaced: BTreeSet<Entry>,
    /// Keys after `cursor` inserted since the freeze, absent from the frozen view.
    added: BTreeSet<Box<[u8]>>,
}

impl Snapshot {
    fn is_out(&self, key: &[u8]) -> bool {
        self.cursor.as_deref().is_some_and(|cursor| key <= cursor)
    }

    fn after(&self) -> Bound<&[u8]> {
        self.cursor
            .as_deref()
            .map_or(Bound::Unbounded, Bound::Excluded)
    }
}

/// Stage 1 tree: a memtable saved whole at every checkpoint.
#[derive(Clone, Debug, Default)]
pub struct MemTree {
    entries: BTreeSet<Entry>,
    snapshot: Option<Snapshot>,
}

impl MemTree {
    /// Creates an empty tree.
    pub const fn new() -> Self {
        Self {
            entries: BTreeSet::new(),
            snapshot: None,
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

    /// Keeps the frozen view of `key` before it changes.
    fn preserve(&mut self, key: &[u8]) {
        let Some(snapshot) = &mut self.snapshot else {
            return;
        };
        if snapshot.is_out(key) || snapshot.replaced.contains(key) || snapshot.added.contains(key) {
            return;
        }
        match self.entries.take(key) {
            Some(entry) => {
                snapshot.replaced.insert(entry);
            }
            None => {
                snapshot.added.insert(key.into());
            }
        }
    }

    /// Frozen entries after the cursor in key order, until `f` returns false.
    fn frozen_while(&self, mut f: impl FnMut(&Entry) -> bool) {
        let Some(snapshot) = &self.snapshot else {
            return;
        };
        let mut live = self
            .entries
            .range::<[u8], _>((snapshot.after(), Bound::Unbounded))
            .filter(|e| !snapshot.added.contains(e.key()))
            .peekable();
        let mut replaced = snapshot
            .replaced
            .range::<[u8], _>((snapshot.after(), Bound::Unbounded))
            .peekable();
        loop {
            let next = match (live.peek(), replaced.peek()) {
                (Some(l), Some(r)) => match l.cmp(r) {
                    Ordering::Less => live.next(),
                    Ordering::Equal => {
                        live.next();
                        replaced.next()
                    }
                    Ordering::Greater => replaced.next(),
                },
                (Some(_), None) => live.next(),
                (None, _) => replaced.next(),
            };
            if !next.is_some_and(&mut f) {
                return;
            }
        }
    }
}

impl PartialEq for MemTree {
    fn eq(&self, other: &Self) -> bool {
        self.entries
            .iter()
            .map(Entry::split)
            .eq(other.entries.iter().map(Entry::split))
    }
}

impl Eq for MemTree {}

impl Tree for MemTree {
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries.get(key).map(Entry::value)
    }

    fn scan<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.entries
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .map(Entry::split)
            .take_while(move |(key, _)| key.starts_with(prefix))
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.preserve(&key);
        self.entries.replace(Entry::new(&key, &value));
    }

    fn remove(&mut self, key: &[u8]) {
        self.preserve(key);
        self.entries.remove(key);
    }

    fn begin_snapshot(&mut self) {
        self.snapshot = Some(Snapshot::default());
    }

    fn next_piece(&mut self, max_piece: usize) -> Option<Vec<u8>> {
        let (mut len, mut last) = (0, None);
        self.frozen_while(|entry| {
            let entry_len = entry.piece_len();
            assert!(
                entry_len <= max_piece,
                "entry of {entry_len} bytes exceeds a piece"
            );
            if len + entry_len > max_piece {
                return false;
            }
            len += entry_len;
            last = Some(Box::<[u8]>::from(entry.key()));
            true
        });
        let Some(last) = last else {
            self.snapshot = None;
            return None;
        };
        let mut piece = Vec::with_capacity(len);
        self.frozen_while(|entry| {
            entry.encode_into(&mut piece);
            entry.key() < &*last
        });
        let snapshot = self.snapshot.as_mut().expect("frozen above");
        while snapshot.replaced.first().is_some_and(|e| e.key() <= &*last) {
            snapshot.replaced.pop_first();
        }
        while snapshot.added.first().is_some_and(|k| **k <= *last) {
            snapshot.added.pop_first();
        }
        snapshot.cursor = Some(last);
        Some(piece)
    }

    fn end_snapshot(&mut self) {
        self.snapshot = None;
    }

    fn restore(&mut self, pieces: &[Vec<u8>]) -> Result<(), DecodeError> {
        let mut entries = Vec::new();
        for piece in pieces {
            let mut rest = piece.as_slice();
            while !rest.is_empty() {
                let key_len = usize::from(u16::from_le_bytes(take(&mut rest)?));
                let key = take_slice(&mut rest, key_len)?;
                let value_len = u32::from_le_bytes(take(&mut rest)?) as usize;
                let value = take_slice(&mut rest, value_len)?;
                entries.push(Entry::new(key, value));
            }
        }
        self.entries = entries.into_iter().collect();
        self.snapshot = None;
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

    fn random_bytes(rng: &mut Rng, min: u64, max: u64) -> Vec<u8> {
        let mut bytes = vec![0; usize::try_from(rng.range_inclusive(min, max)).unwrap()];
        rng.fill_bytes(&mut bytes);
        bytes
    }

    fn mutate(tree: &mut MemTree, rng: &mut Rng) {
        let key = random_bytes(rng, 1, 3);
        if rng.chance(1, 4) {
            tree.remove(&key);
        } else {
            tree.insert(key, random_bytes(rng, 0, 60));
        }
    }

    fn snapshot(tree: &mut MemTree, max_piece: usize) -> Vec<Vec<u8>> {
        tree.begin_snapshot();
        core::iter::from_fn(|| tree.next_piece(max_piece)).collect()
    }

    #[test]
    fn snapshots_restore_random_trees_at_any_piece_size() {
        let mut rng = Rng::from_seed(11);
        for round in 0..50 {
            let mut tree = MemTree::new();
            for _ in 0..rng.below(200) {
                let key = random_bytes(&mut rng, 1, 40);
                if rng.chance(1, 5) {
                    tree.remove(&key);
                } else {
                    tree.insert(key, random_bytes(&mut rng, 0, 59));
                }
            }
            let max_piece = 106 + usize::try_from(rng.below(4000)).unwrap();
            let pieces = snapshot(&mut tree, max_piece);
            assert!(
                pieces
                    .iter()
                    .all(|p| p.len() <= max_piece && !p.is_empty() && p.capacity() == p.len())
            );
            let mut restored = MemTree::new();
            restored.insert(b"stale".to_vec(), Vec::new());
            restored.restore(&pieces).unwrap();
            assert_eq!(restored, tree, "round {round}");
        }
    }

    /// Inserts and removes between pieces, on keys before and after the cursor, never change
    /// what the snapshot hands out.
    #[test]
    fn a_snapshot_keeps_its_frozen_view_while_the_tree_changes() {
        let mut rng = Rng::from_seed(12);
        for round in 0..200 {
            let mut tree = MemTree::new();
            for _ in 0..rng.below(300) {
                mutate(&mut tree, &mut rng);
            }
            let frozen = tree.clone();
            let max_piece = 70 + usize::try_from(rng.below(500)).unwrap();
            tree.begin_snapshot();
            let mut pieces = Vec::new();
            while let Some(piece) = tree.next_piece(max_piece) {
                pieces.push(piece);
                for _ in 0..rng.below(20) {
                    mutate(&mut tree, &mut rng);
                }
            }
            let mut restored = MemTree::new();
            restored.restore(&pieces).unwrap();
            assert_eq!(restored, frozen, "round {round}");
            assert!(tree.snapshot.is_none());
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
        tree.insert(b"b".to_vec(), b"new".to_vec());
        assert_eq!((tree.get(b"b"), tree.len()), (Some(&b"new"[..]), 5));
    }

    #[test]
    fn restore_rejects_truncated_pieces() {
        let mut tree = MemTree::new();
        tree.insert(b"key".to_vec(), b"value".to_vec());
        let mut pieces = snapshot(&mut tree, 4096);
        pieces[0].pop();
        assert_eq!(MemTree::new().restore(&pieces), Err(DecodeError::Malformed));
    }
}
