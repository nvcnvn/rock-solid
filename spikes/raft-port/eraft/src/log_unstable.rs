// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// log_unstable.go. Licensed under the Apache License 2.0.
//
// Deviation: Go reslices the entries array in stableTo and copies it only when
// less than half is used; here stable entries are drained from the front of
// the Vec and the Vec is shrunk under the same condition.

use std::rc::Rc;

use crate::logger::Logger;
use crate::raftpb::{Entry, Snapshot};
use crate::types::EntryId;

/// Log entries and snapshot not yet written to Storage.
pub struct Unstable {
    /// Incoming snapshot, if any.
    pub snapshot: Option<Snapshot>,
    /// Entries not yet written to storage; entries[i] has index i+offset.
    pub entries: Vec<Entry>,
    /// Index of entries[0].
    pub offset: u64,
    /// True if the snapshot is being written.
    pub snapshot_in_progress: bool,
    /// entries[..offset_in_progress-offset] are being written.
    pub offset_in_progress: u64,
    pub(crate) logger: Rc<dyn Logger>,
}

impl Unstable {
    /// First index if there is a snapshot.
    pub fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot.as_ref().map(|s| s.metadata.index + 1)
    }

    /// Last index if there is an entry or snapshot.
    pub fn maybe_last_index(&self) -> Option<u64> {
        if !self.entries.is_empty() {
            return Some(self.offset + self.entries.len() as u64 - 1);
        }
        self.snapshot.as_ref().map(|s| s.metadata.index)
    }

    /// Term of entry `i`, if known.
    pub fn maybe_term(&self, i: u64) -> Option<u64> {
        if i < self.offset {
            if let Some(s) = &self.snapshot
                && s.metadata.index == i
            {
                return Some(s.metadata.term);
            }
            return None;
        }
        let last = self.maybe_last_index()?;
        if i > last {
            return None;
        }
        Some(self.entries[(i - self.offset) as usize].term)
    }

    /// Entries not already being written.
    pub fn next_entries(&self) -> &[Entry] {
        let in_progress = (self.offset_in_progress - self.offset) as usize;
        &self.entries[in_progress..]
    }

    /// The snapshot, if it is not already being written.
    pub fn next_snapshot(&self) -> Option<&Snapshot> {
        if self.snapshot_in_progress {
            return None;
        }
        self.snapshot.as_ref()
    }

    /// Marks all entries and the snapshot as being written.
    pub fn accept_in_progress(&mut self) {
        if let Some(e) = self.entries.last() {
            self.offset_in_progress = e.index + 1;
        }
        if self.snapshot.is_some() {
            self.snapshot_in_progress = true;
        }
    }

    /// Marks entries up to `id` as written to stable storage.
    pub fn stable_to(&mut self, id: EntryId) {
        let Some(gt) = self.maybe_term(id.index) else {
            infof!(
                self.logger,
                "entry at index {} missing from unstable log; ignoring",
                id.index
            );
            return;
        };
        if id.index < self.offset {
            infof!(
                self.logger,
                "entry at index {} matched unstable snapshot; ignoring",
                id.index
            );
            return;
        }
        if gt != id.term {
            infof!(
                self.logger,
                "entry at (index,term)=({},{}) mismatched with entry at ({},{}) in unstable log; ignoring",
                id.index,
                id.term,
                id.index,
                gt
            );
            return;
        }
        let num = (id.index + 1 - self.offset) as usize;
        self.entries.drain(..num);
        self.offset = id.index + 1;
        self.offset_in_progress = self.offset_in_progress.max(self.offset);
        self.shrink_entries_array();
    }

    fn shrink_entries_array(&mut self) {
        const LEN_MULTIPLE: usize = 2;
        if self.entries.is_empty() {
            self.entries = Vec::new();
        } else if self.entries.len() * LEN_MULTIPLE < self.entries.capacity() {
            self.entries.shrink_to_fit();
        }
    }

    /// Drops the snapshot once it is stable at index `i`.
    pub fn stable_snap_to(&mut self, i: u64) {
        if self
            .snapshot
            .as_ref()
            .is_some_and(|s| s.metadata.index == i)
        {
            self.snapshot = None;
            self.snapshot_in_progress = false;
        }
    }

    /// Resets to the snapshot.
    pub fn restore(&mut self, s: Snapshot) {
        self.offset = s.metadata.index + 1;
        self.offset_in_progress = self.offset;
        self.entries = Vec::new();
        self.snapshot = Some(s);
        self.snapshot_in_progress = false;
    }

    /// Appends entries, truncating any conflicting suffix.
    pub fn truncate_and_append(&mut self, ents: Vec<Entry>) {
        let from_index = ents[0].index;
        if from_index == self.offset + self.entries.len() as u64 {
            self.entries.extend(ents);
        } else if from_index <= self.offset {
            infof!(
                self.logger,
                "replace the unstable entries from index {}",
                from_index
            );
            self.entries = ents;
            self.offset = from_index;
            self.offset_in_progress = self.offset;
        } else {
            infof!(
                self.logger,
                "truncate the unstable entries before index {}",
                from_index
            );
            self.must_check_out_of_bounds(self.offset, from_index);
            self.entries.truncate((from_index - self.offset) as usize);
            self.entries.extend(ents);
            self.offset_in_progress = self.offset_in_progress.min(from_index);
        }
    }

    /// Entries in [lo, hi), which must all be in the unstable log.
    pub fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        self.must_check_out_of_bounds(lo, hi);
        &self.entries[(lo - self.offset) as usize..(hi - self.offset) as usize]
    }

    fn must_check_out_of_bounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            panicf!(self.logger, "invalid unstable.slice {} > {}", lo, hi);
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            panicf!(
                self.logger,
                "unstable.slice[{},{}) out of bound [{},{}]",
                lo,
                hi,
                self.offset,
                upper
            );
        }
    }
}
