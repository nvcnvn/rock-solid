// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// storage.go. Licensed under the Apache License 2.0.
//
// Deviation: MemoryStorage has no mutex (raft and the application reach it
// through RawNode::store/store_mut) and no call statistics. Storage methods
// return owned entries.

use crate::Error;
use crate::raftpb::{ConfState, Entry, HardState, Snapshot};
use crate::util::limit_size;

/// Read access to the persisted log, hard state and snapshot.
pub trait Storage {
    /// Returns the saved HardState and ConfState.
    fn initial_state(&self) -> Result<(HardState, ConfState), Error>;
    /// Returns entries in [lo, hi), limited to `max_size` bytes but at least one.
    fn entries(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>, Error>;
    /// Returns the term of entry `i`, in [FirstIndex()-1, LastIndex()].
    fn term(&self, i: u64) -> Result<u64, Error>;
    /// Index of the last entry.
    fn last_index(&self) -> Result<u64, Error>;
    /// Index of the first available entry.
    fn first_index(&self) -> Result<u64, Error>;
    /// The most recent snapshot.
    fn snapshot(&self) -> Result<Snapshot, Error>;
}

/// In-memory Storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryStorage {
    pub(crate) hard_state: HardState,
    pub(crate) snapshot: Snapshot,
    pub(crate) ents: Vec<Entry>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        MemoryStorage::new()
    }
}

impl MemoryStorage {
    /// Creates an empty MemoryStorage with a dummy entry at term zero.
    pub fn new() -> MemoryStorage {
        MemoryStorage {
            hard_state: HardState::default(),
            snapshot: Snapshot::default(),
            ents: vec![Entry::default()],
        }
    }

    /// Saves the HardState.
    pub fn set_hard_state(&mut self, st: HardState) -> Result<(), Error> {
        self.hard_state = st;
        Ok(())
    }

    /// The saved HardState.
    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    /// Mutable access to the snapshot's ConfState, for tests (`withPeers`).
    pub fn conf_state_mut(&mut self) -> &mut ConfState {
        &mut self.snapshot.metadata.conf_state
    }

    fn last_index_inner(&self) -> u64 {
        self.ents[0].index + self.ents.len() as u64 - 1
    }

    fn first_index_inner(&self) -> u64 {
        self.ents[0].index + 1
    }

    /// Overwrites the contents with the snapshot.
    pub fn apply_snapshot(&mut self, snap: Snapshot) -> Result<(), Error> {
        let ms_index = self.snapshot.metadata.index;
        let snap_index = snap.metadata.index;
        if ms_index != 0 && ms_index >= snap_index {
            return Err(Error::SnapOutOfDate);
        }
        self.ents = vec![Entry {
            term: snap.metadata.term,
            index: snap.metadata.index,
            ..Default::default()
        }];
        self.snapshot = snap;
        Ok(())
    }

    /// Makes a snapshot at index `i`.
    pub fn create_snapshot(
        &mut self,
        i: u64,
        cs: Option<&ConfState>,
        data: Vec<u8>,
    ) -> Result<Snapshot, Error> {
        if i <= self.snapshot.metadata.index {
            return Err(Error::SnapOutOfDate);
        }
        let offset = self.ents[0].index;
        if i > self.last_index_inner() {
            panic!(
                "snapshot {} is out of bound lastindex({})",
                i,
                self.last_index_inner()
            );
        }
        self.snapshot.metadata.index = i;
        self.snapshot.metadata.term = self.ents[(i - offset) as usize].term;
        if let Some(cs) = cs {
            self.snapshot.metadata.conf_state = cs.clone();
        }
        self.snapshot.data = data;
        Ok(self.snapshot.clone())
    }

    /// Discards entries before `compact_index`.
    pub fn compact(&mut self, compact_index: u64) -> Result<(), Error> {
        let offset = self.ents[0].index;
        if compact_index <= offset {
            return Err(Error::Compacted);
        }
        if compact_index > self.last_index_inner() {
            panic!(
                "compact {} is out of bound lastindex({})",
                compact_index,
                self.last_index_inner()
            );
        }
        let i = (compact_index - offset) as usize;
        let mut ents = Vec::with_capacity(self.ents.len() - i);
        ents.push(Entry {
            index: self.ents[i].index,
            term: self.ents[i].term,
            ..Default::default()
        });
        ents.extend(self.ents.drain(i + 1..));
        self.ents = ents;
        Ok(())
    }

    /// Appends entries, truncating any conflicting suffix.
    pub fn append(&mut self, entries: &[Entry]) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let first = self.first_index_inner();
        let last = entries[0].index + entries.len() as u64 - 1;
        if last < first {
            return Ok(());
        }
        let mut entries = entries;
        if first > entries[0].index {
            entries = &entries[(first - entries[0].index) as usize..];
        }
        let offset = entries[0].index - self.ents[0].index;
        let len = self.ents.len() as u64;
        if len > offset {
            self.ents.truncate(offset as usize);
            self.ents.extend_from_slice(entries);
        } else if len == offset {
            self.ents.extend_from_slice(entries);
        } else {
            panic!(
                "missing log entry [last: {}, append at: {}]",
                self.last_index_inner(),
                entries[0].index
            );
        }
        Ok(())
    }
}

impl Storage for MemoryStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState), Error> {
        Ok((self.hard_state, self.snapshot.metadata.conf_state.clone()))
    }

    fn entries(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>, Error> {
        let offset = self.ents[0].index;
        if lo <= offset {
            return Err(Error::Compacted);
        }
        if hi > self.last_index_inner() + 1 {
            panic!(
                "entries' hi({}) is out of bound lastindex({})",
                hi,
                self.last_index_inner()
            );
        }
        if self.ents.len() == 1 {
            return Err(Error::Unavailable);
        }
        let s = &self.ents[(lo - offset) as usize..(hi - offset) as usize];
        Ok(s[..limit_size(s, max_size)].to_vec())
    }

    fn term(&self, i: u64) -> Result<u64, Error> {
        let offset = self.ents[0].index;
        if i < offset {
            return Err(Error::Compacted);
        }
        if (i - offset) as usize >= self.ents.len() {
            return Err(Error::Unavailable);
        }
        Ok(self.ents[(i - offset) as usize].term)
    }

    fn last_index(&self) -> Result<u64, Error> {
        Ok(self.last_index_inner())
    }

    fn first_index(&self) -> Result<u64, Error> {
        Ok(self.first_index_inner())
    }

    fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(self.snapshot.clone())
    }
}
