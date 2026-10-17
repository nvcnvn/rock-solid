// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// log.go. Licensed under the Apache License 2.0.
//
// Deviation: slices of entries are returned as owned Vecs (Go shares the
// backing array).

use std::fmt;
use std::rc::Rc;

use crate::Error;
use crate::log_unstable::Unstable;
use crate::logger::Logger;
use crate::raftpb::{Entry, Snapshot};
use crate::storage::Storage;
use crate::types::{EntryId, LogSlice, pb_entry_id};
use crate::util::{ents_size, limit_size};

pub(crate) const NO_LIMIT: u64 = u64::MAX;

/// The raft log: stable entries in Storage plus unstable ones in memory.
pub struct RaftLog<S: Storage> {
    /// Stable entries since the last snapshot.
    pub storage: S,
    /// Unstable entries and snapshot.
    pub unstable: Unstable,
    /// Highest index known to be stable on a quorum.
    pub committed: u64,
    /// Highest index handed to the application for applying.
    pub applying: u64,
    /// Highest index the application has applied.
    pub applied: u64,
    logger: Rc<dyn Logger>,
    pub(crate) max_applying_ents_size: u64,
    pub(crate) applying_ents_size: u64,
    pub(crate) applying_ents_paused: bool,
}

impl<S: Storage> fmt::Display for RaftLog<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "committed={}, applied={}, applying={}, unstable.offset={}, unstable.offsetInProgress={}, len(unstable.Entries)={}",
            self.committed,
            self.applied,
            self.applying,
            self.unstable.offset,
            self.unstable.offset_in_progress,
            self.unstable.entries.len()
        )
    }
}

impl<S: Storage> RaftLog<S> {
    /// Creates a log over `storage` with no limit on applying entries.
    pub fn new(storage: S, logger: Rc<dyn Logger>) -> RaftLog<S> {
        Self::new_with_size(storage, logger, NO_LIMIT)
    }

    /// Creates a log over `storage` limiting outstanding applying bytes.
    pub fn new_with_size(
        storage: S,
        logger: Rc<dyn Logger>,
        max_applying_ents_size: u64,
    ) -> RaftLog<S> {
        let first_index = storage.first_index().unwrap_or_else(|e| panic!("{e}"));
        let last_index = storage.last_index().unwrap_or_else(|e| panic!("{e}"));
        RaftLog {
            storage,
            unstable: Unstable {
                snapshot: None,
                entries: Vec::new(),
                offset: last_index + 1,
                snapshot_in_progress: false,
                offset_in_progress: last_index + 1,
                logger: logger.clone(),
            },
            committed: first_index - 1,
            applying: first_index - 1,
            applied: first_index - 1,
            logger,
            max_applying_ents_size,
            applying_ents_size: 0,
            applying_ents_paused: false,
        }
    }

    /// Appends the slice if its prev entry matches; returns the last new index.
    pub fn maybe_append(&mut self, a: LogSlice, committed: u64) -> Option<u64> {
        if !self.match_term(a.prev) {
            return None;
        }
        let lastnewi = a.prev.index + a.entries.len() as u64;
        let ci = self.find_conflict(&a.entries);
        if ci == 0 {
        } else if ci <= self.committed {
            panicf!(
                self.logger,
                "entry {} conflict with committed entry [committed({})]",
                ci,
                self.committed
            );
        } else {
            let offset = a.prev.index + 1;
            if ci - offset > a.entries.len() as u64 {
                panicf!(
                    self.logger,
                    "index, {}, is out of range [{}]",
                    ci - offset,
                    a.entries.len()
                );
            }
            let mut ents = a.entries;
            ents.drain(..(ci - offset) as usize);
            self.append(ents);
        }
        self.commit_to(committed.min(lastnewi));
        Some(lastnewi)
    }

    /// Appends entries to the unstable log; returns the new last index.
    pub fn append(&mut self, ents: Vec<Entry>) -> u64 {
        if ents.is_empty() {
            return self.last_index();
        }
        let after = ents[0].index - 1;
        if after < self.committed {
            panicf!(
                self.logger,
                "after({}) is out of range [committed({})]",
                after,
                self.committed
            );
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    /// Index of the first conflicting or new entry, or 0.
    pub fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for e in ents {
            let id = pb_entry_id(e);
            if !self.match_term(id) {
                if id.index <= self.last_index() {
                    infof!(
                        self.logger,
                        "found conflict at index {} [existing term: {}, conflicting term: {}]",
                        id.index,
                        self.zero_term_on_out_of_bounds(self.term(id.index)),
                        id.term
                    );
                }
                return id.index;
            }
        }
        0
    }

    /// Best guess of where this log stops matching a log with entry (index, term).
    pub fn find_conflict_by_term(&self, mut index: u64, term: u64) -> (u64, u64) {
        while index > 0 {
            match self.term(index) {
                Err(_) => return (index, 0),
                Ok(our_term) if our_term <= term => return (index, our_term),
                Ok(_) => {}
            }
            index -= 1;
        }
        (0, 0)
    }

    /// Unstable entries not already being written.
    pub fn next_unstable_ents(&self) -> &[Entry] {
        self.unstable.next_entries()
    }

    /// True if there are unstable entries not already being written.
    pub fn has_next_unstable_ents(&self) -> bool {
        !self.next_unstable_ents().is_empty()
    }

    /// True if there are unstable entries, written or not.
    pub fn has_next_or_in_progress_unstable_ents(&self) -> bool {
        !self.unstable.entries.is_empty()
    }

    /// Committed entries available for applying.
    pub fn next_committed_ents(&self, allow_unstable: bool) -> Vec<Entry> {
        if self.applying_ents_paused {
            return Vec::new();
        }
        if self.has_next_or_in_progress_snapshot() {
            return Vec::new();
        }
        let (lo, hi) = (
            self.applying + 1,
            self.max_appliable_index(allow_unstable) + 1,
        );
        if lo >= hi {
            return Vec::new();
        }
        let max_size = self.max_applying_ents_size - self.applying_ents_size;
        if max_size == 0 {
            panicf!(
                self.logger,
                "applying entry size ({}-{})={} not positive",
                self.max_applying_ents_size,
                self.applying_ents_size,
                max_size
            );
        }
        match self.slice(lo, hi, max_size) {
            Ok(ents) => ents,
            Err(e) => panicf!(
                self.logger,
                "unexpected error when getting unapplied entries ({})",
                e
            ),
        }
    }

    /// True if committed entries are available for applying.
    pub fn has_next_committed_ents(&self, allow_unstable: bool) -> bool {
        if self.applying_ents_paused {
            return false;
        }
        if self.has_next_or_in_progress_snapshot() {
            return false;
        }
        let (lo, hi) = (
            self.applying + 1,
            self.max_appliable_index(allow_unstable) + 1,
        );
        lo < hi
    }

    fn max_appliable_index(&self, allow_unstable: bool) -> u64 {
        let mut hi = self.committed;
        if !allow_unstable {
            hi = hi.min(self.unstable.offset - 1);
        }
        hi
    }

    /// The unstable snapshot, if not already being written.
    pub fn next_unstable_snapshot(&self) -> Option<&Snapshot> {
        self.unstable.next_snapshot()
    }

    /// True if there is an unstable snapshot not already being written.
    pub fn has_next_unstable_snapshot(&self) -> bool {
        self.unstable.next_snapshot().is_some()
    }

    /// True if there is an unstable snapshot.
    pub fn has_next_or_in_progress_snapshot(&self) -> bool {
        self.unstable.snapshot.is_some()
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        if let Some(s) = &self.unstable.snapshot {
            return Ok(s.clone());
        }
        self.storage.snapshot()
    }

    /// First index in the log.
    pub fn first_index(&self) -> u64 {
        if let Some(i) = self.unstable.maybe_first_index() {
            return i;
        }
        self.storage.first_index().unwrap_or_else(|e| panic!("{e}"))
    }

    /// Last index in the log.
    pub fn last_index(&self) -> u64 {
        if let Some(i) = self.unstable.maybe_last_index() {
            return i;
        }
        self.storage.last_index().unwrap_or_else(|e| panic!("{e}"))
    }

    /// Advances the commit index (never decreases it).
    pub fn commit_to(&mut self, tocommit: u64) {
        if self.committed < tocommit {
            if self.last_index() < tocommit {
                panicf!(
                    self.logger,
                    "tocommit({}) is out of range [lastIndex({})]. Was the raft log corrupted, truncated, or lost?",
                    tocommit,
                    self.last_index()
                );
            }
            self.committed = tocommit;
        }
    }

    /// Records that the application applied up to `i`.
    pub fn applied_to(&mut self, i: u64, size: u64) {
        if self.committed < i || i < self.applied {
            panicf!(
                self.logger,
                "applied({}) is out of range [prevApplied({}), committed({})]",
                i,
                self.applied,
                self.committed
            );
        }
        self.applied = i;
        self.applying = self.applying.max(i);
        if self.applying_ents_size > size {
            self.applying_ents_size -= size;
        } else {
            self.applying_ents_size = 0;
        }
        self.applying_ents_paused = self.applying_ents_size >= self.max_applying_ents_size;
    }

    /// Records that entries up to `i` were handed out for applying.
    pub fn accept_applying(&mut self, i: u64, size: u64, allow_unstable: bool) {
        if self.committed < i {
            panicf!(
                self.logger,
                "applying({}) is out of range [prevApplying({}), committed({})]",
                i,
                self.applying,
                self.committed
            );
        }
        self.applying = i;
        self.applying_ents_size += size;
        self.applying_ents_paused = self.applying_ents_size >= self.max_applying_ents_size
            || i < self.max_appliable_index(allow_unstable);
    }

    /// Marks entries up to `id` stable.
    pub fn stable_to(&mut self, id: EntryId) {
        self.unstable.stable_to(id)
    }

    /// Marks the snapshot at `i` stable.
    pub fn stable_snap_to(&mut self, i: u64) {
        self.unstable.stable_snap_to(i)
    }

    /// Marks the current unstable entries as being written.
    pub fn accept_unstable(&mut self) {
        self.unstable.accept_in_progress()
    }

    /// ID of the last entry.
    pub fn last_entry_id(&self) -> EntryId {
        let index = self.last_index();
        match self.term(index) {
            Ok(t) => EntryId { term: t, index },
            Err(e) => panicf!(
                self.logger,
                "unexpected error when getting the last term at {}: {}",
                index,
                e
            ),
        }
    }

    /// Term of entry `i`.
    pub fn term(&self, i: u64) -> Result<u64, Error> {
        if let Some(t) = self.unstable.maybe_term(i) {
            return Ok(t);
        }
        if i + 1 < self.first_index() {
            return Err(Error::Compacted);
        }
        if i > self.last_index() {
            return Err(Error::Unavailable);
        }
        match self.storage.term(i) {
            Ok(t) => Ok(t),
            Err(e @ (Error::Compacted | Error::Unavailable)) => Err(e),
            Err(e) => panic!("{e}"),
        }
    }

    /// Entries from `i` to the end, limited to `max_size` bytes.
    pub fn entries(&self, i: u64, max_size: u64) -> Result<Vec<Entry>, Error> {
        if i > self.last_index() {
            return Ok(Vec::new());
        }
        self.slice(i, self.last_index() + 1, max_size)
    }

    /// All entries in the log.
    pub fn all_entries(&self) -> Vec<Entry> {
        match self.entries(self.first_index(), NO_LIMIT) {
            Ok(e) => e,
            Err(Error::Compacted) => self.all_entries(),
            Err(e) => panic!("{e}"),
        }
    }

    /// True if a log ending at `their` is at least as up to date as this one.
    pub fn is_up_to_date(&self, their: EntryId) -> bool {
        let our = self.last_entry_id();
        their.term > our.term || (their.term == our.term && their.index >= our.index)
    }

    /// True if the log has an entry with this ID.
    pub fn match_term(&self, id: EntryId) -> bool {
        match self.term(id.index) {
            Ok(t) => t == id.term,
            Err(_) => false,
        }
    }

    /// Commits up to `at` if it is in this log at a non-zero term.
    pub fn maybe_commit(&mut self, at: EntryId) -> bool {
        if at.term != 0 && at.index > self.committed && self.match_term(at) {
            self.commit_to(at.index);
            return true;
        }
        false
    }

    /// Resets the log to the snapshot.
    pub fn restore(&mut self, s: Snapshot) {
        infof!(
            self.logger,
            "log [{}] starts to restore snapshot [index: {}, term: {}]",
            self,
            s.metadata.index,
            s.metadata.term
        );
        self.committed = s.metadata.index;
        self.unstable.restore(s);
    }

    /// Visits entries in [lo, hi) in pages of up to `page_size` bytes; stops early
    /// with the callback's error.
    pub fn scan<E: From<Error>>(
        &self,
        mut lo: u64,
        hi: u64,
        page_size: u64,
        mut v: impl FnMut(&[Entry]) -> Result<(), E>,
    ) -> Result<(), E> {
        while lo < hi {
            let ents = self.slice(lo, hi, page_size)?;
            if ents.is_empty() {
                return Err(Error::Other(format!("got 0 entries in [{lo}, {hi})")).into());
            }
            v(&ents)?;
            lo += ents.len() as u64;
        }
        Ok(())
    }

    /// Entries in [lo, hi), limited to `max_size` bytes but at least one.
    pub fn slice(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>, Error> {
        self.must_check_out_of_bounds(lo, hi)?;
        if lo == hi {
            return Ok(Vec::new());
        }
        if lo >= self.unstable.offset {
            let u = self.unstable.slice(lo, hi);
            return Ok(u[..limit_size(u, max_size)].to_vec());
        }
        let cut = hi.min(self.unstable.offset);
        let mut ents = match self.storage.entries(lo, cut, max_size) {
            Ok(e) => e,
            Err(Error::Compacted) => return Err(Error::Compacted),
            Err(Error::Unavailable) => panicf!(
                self.logger,
                "entries[{}:{}) is unavailable from storage",
                lo,
                cut
            ),
            Err(e) => panic!("{e}"),
        };
        if hi <= self.unstable.offset {
            return Ok(ents);
        }
        if (ents.len() as u64) < cut - lo {
            return Ok(ents);
        }
        let size = ents_size(&ents);
        if size >= max_size {
            return Ok(ents);
        }
        let u = self.unstable.slice(self.unstable.offset, hi);
        let u = &u[..limit_size(u, max_size - size)];
        if u.len() == 1 && size + ents_size(u) > max_size {
            return Ok(ents);
        }
        ents.extend_from_slice(u);
        Ok(ents)
    }

    pub(crate) fn must_check_out_of_bounds(&self, lo: u64, hi: u64) -> Result<(), Error> {
        if lo > hi {
            panicf!(self.logger, "invalid slice {} > {}", lo, hi);
        }
        let fi = self.first_index();
        if lo < fi {
            return Err(Error::Compacted);
        }
        let length = self.last_index() + 1 - fi;
        if hi > fi + length {
            panicf!(
                self.logger,
                "slice[{},{}) out of bound [{},{}]",
                lo,
                hi,
                fi,
                self.last_index()
            );
        }
        Ok(())
    }

    /// The term, or 0 if `t` is a compaction or unavailability error.
    pub fn zero_term_on_out_of_bounds(&self, t: Result<u64, Error>) -> u64 {
        match t {
            Ok(t) => t,
            Err(Error::Compacted | Error::Unavailable) => 0,
            Err(e) => panicf!(self.logger, "unexpected error ({})", e),
        }
    }
}
