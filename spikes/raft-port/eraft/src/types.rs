// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// types.go. Licensed under the Apache License 2.0.

use crate::raftpb::Entry;

/// Identifies a raft log entry by term and index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntryId {
    /// Term.
    pub term: u64,
    /// Index.
    pub index: u64,
}

/// Returns the ID of the entry.
pub fn pb_entry_id(e: &Entry) -> EntryId {
    EntryId {
        term: e.term,
        index: e.index,
    }
}

/// A slice of a raft log in the context of a leader term.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogSlice {
    /// Leader term containing the entries in its log.
    pub term: u64,
    /// ID of the entry preceding the entries.
    pub prev: EntryId,
    /// Consecutive entries.
    pub entries: Vec<Entry>,
}

impl LogSlice {
    /// Index of the last entry, or prev.index if empty.
    pub fn last_index(&self) -> u64 {
        self.prev.index + self.entries.len() as u64
    }

    /// ID of the last entry, or prev if empty.
    pub fn last_entry_id(&self) -> EntryId {
        self.entries.last().map(pb_entry_id).unwrap_or(self.prev)
    }

    /// Ok if the slice is well formed.
    pub fn valid(&self) -> Result<(), String> {
        let mut prev = self.prev;
        for e in &self.entries {
            let id = pb_entry_id(e);
            if id.term < prev.term || id.index != prev.index + 1 {
                return Err(format!(
                    "leader term {}: entries {{term:{} index:{}}} and {{term:{} index:{}}} not consistent",
                    self.term, prev.term, prev.index, id.term, id.index
                ));
            }
            prev = id;
        }
        if self.term < prev.term {
            return Err(format!(
                "leader term {}: entry {{term:{} index:{}}} has a newer term",
                self.term, prev.term, prev.index
            ));
        }
        Ok(())
    }
}
