// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// log_test.go. Licensed under the Apache License 2.0.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use crate::Error;
use crate::log::{NO_LIMIT, RaftLog};
use crate::logger::{Logger, NopLogger};
use crate::raftpb::{Entry, EntryType, Snapshot, SnapshotMetadata, entry_size};
use crate::storage::MemoryStorage;
use crate::testutil::{index_term_range, index_terms};
use crate::types::{EntryId, LogSlice, pb_entry_id};
use crate::util::ents_size;

fn logger() -> Rc<dyn Logger> {
    Rc::new(NopLogger)
}

fn new_log(s: MemoryStorage) -> RaftLog<MemoryStorage> {
    RaftLog::new(s, logger())
}

fn snap(index: u64, term: u64) -> Snapshot {
    Snapshot {
        metadata: SnapshotMetadata {
            index,
            term,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn id(term: u64, index: u64) -> EntryId {
    EntryId { term, index }
}

fn panics(f: impl FnOnce()) -> bool {
    catch_unwind(AssertUnwindSafe(f)).is_err()
}

#[test]
fn test_find_conflict() {
    let previous_ents = index_terms(1, &[1, 2, 3]);
    let tests = [
        (vec![], 0),
        (index_terms(1, &[1, 2, 3]), 0),
        (index_terms(2, &[2, 3]), 0),
        (index_terms(3, &[3]), 0),
        (index_terms(1, &[1, 2, 3, 4, 4]), 4),
        (index_terms(2, &[2, 3, 4, 5]), 4),
        (index_terms(3, &[3, 4, 4]), 4),
        (index_terms(4, &[4, 4]), 4),
        (index_terms(1, &[4, 4]), 1),
        (index_terms(2, &[1, 4, 4]), 2),
        (index_terms(3, &[1, 2, 4, 4]), 3),
    ];
    for (i, (ents, wconflict)) in tests.into_iter().enumerate() {
        let mut l = new_log(MemoryStorage::new());
        l.append(previous_ents.clone());
        assert_eq!(wconflict, l.find_conflict(&ents), "#{i}");
    }
}

#[test]
fn test_find_conflict_by_term() {
    let a = || index_terms(0, &[0, 2, 2, 5, 5, 5]);
    let b = || index_terms(10, &[3, 3, 3, 4, 4, 4]);
    let tests = [
        (a(), 100, 2, 100),
        (a(), 5, 6, 5),
        (a(), 5, 5, 5),
        (a(), 5, 4, 2),
        (a(), 5, 2, 2),
        (a(), 5, 1, 0),
        (a(), 1, 2, 1),
        (a(), 1, 1, 0),
        (a(), 0, 0, 0),
        (b(), 30, 3, 30),
        (b(), 14, 9, 14),
        (b(), 14, 4, 14),
        (b(), 14, 3, 12),
        (b(), 14, 2, 9),
        (b(), 11, 5, 11),
        (b(), 10, 5, 10),
        (b(), 10, 3, 10),
        (b(), 10, 2, 9),
        (b(), 9, 2, 9),
        (b(), 4, 2, 4),
        (b(), 0, 0, 0),
    ];
    for (i, (ents, index, term, want)) in tests.into_iter().enumerate() {
        let mut st = MemoryStorage::new();
        let _ = st.apply_snapshot(snap(ents[0].index, ents[0].term));
        let mut l = new_log(st);
        l.append(ents[1..].to_vec());
        let (gi, gt) = l.find_conflict_by_term(index, term);
        assert_eq!(want, gi, "#{i}");
        let want_term = l.zero_term_on_out_of_bounds(l.term(gi));
        assert_eq!(want_term, gt, "#{i}");
    }
}

#[test]
fn test_is_up_to_date() {
    let mut l = new_log(MemoryStorage::new());
    l.append(index_terms(1, &[1, 2, 3]));
    let li = l.last_index();
    let tests = [
        (li - 1, 4, true),
        (li, 4, true),
        (li + 1, 4, true),
        (li - 1, 2, false),
        (li, 2, false),
        (li + 1, 2, false),
        (li - 1, 3, false),
        (li, 3, true),
        (li + 1, 3, true),
    ];
    for (i, (last_index, term, w)) in tests.into_iter().enumerate() {
        assert_eq!(w, l.is_up_to_date(id(term, last_index)), "#{i}");
    }
}

#[test]
fn test_append() {
    let previous_ents = index_terms(1, &[1, 2]);
    let tests = [
        (vec![], 2, index_terms(1, &[1, 2]), 3),
        (index_terms(3, &[2]), 3, index_terms(1, &[1, 2, 2]), 3),
        (index_terms(1, &[2]), 1, index_terms(1, &[2]), 1),
        (index_terms(2, &[3, 3]), 3, index_terms(1, &[1, 3, 3]), 2),
    ];
    for (i, (ents, windex, wents, wunstable)) in tests.into_iter().enumerate() {
        let mut storage = MemoryStorage::new();
        storage.append(&previous_ents).unwrap();
        let mut l = new_log(storage);
        assert_eq!(windex, l.append(ents), "#{i}");
        assert_eq!(Ok(wents), l.entries(1, NO_LIMIT), "#{i}");
        assert_eq!(wunstable, l.unstable.offset, "#{i}");
    }
}

#[test]
fn test_log_maybe_append() {
    let previous_ents = index_terms(1, &[1, 2, 3]);
    let lastindex = 3;
    let lastterm = 3;
    let commit = 1;
    let tests = [
        (
            id(lastterm - 1, lastindex),
            lastindex,
            index_terms(lastindex + 1, &[4]),
            0,
            false,
            commit,
            false,
        ),
        (
            id(lastterm, lastindex + 1),
            lastindex,
            index_terms(lastindex + 2, &[4]),
            0,
            false,
            commit,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex,
            vec![],
            lastindex,
            true,
            lastindex,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex + 1,
            vec![],
            lastindex,
            true,
            lastindex,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex - 1,
            vec![],
            lastindex,
            true,
            lastindex - 1,
            false,
        ),
        (
            id(lastterm, lastindex),
            0,
            vec![],
            lastindex,
            true,
            commit,
            false,
        ),
        (
            EntryId::default(),
            lastindex,
            vec![],
            0,
            true,
            commit,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex,
            index_terms(lastindex + 1, &[4]),
            lastindex + 1,
            true,
            lastindex,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex + 1,
            index_terms(lastindex + 1, &[4]),
            lastindex + 1,
            true,
            lastindex + 1,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex + 2,
            index_terms(lastindex + 1, &[4]),
            lastindex + 1,
            true,
            lastindex + 1,
            false,
        ),
        (
            id(lastterm, lastindex),
            lastindex + 2,
            index_terms(lastindex + 1, &[4, 4]),
            lastindex + 2,
            true,
            lastindex + 2,
            false,
        ),
        (
            id(lastterm - 1, lastindex - 1),
            lastindex,
            index_terms(lastindex, &[4]),
            lastindex,
            true,
            lastindex,
            false,
        ),
        (
            id(lastterm - 2, lastindex - 2),
            lastindex,
            index_terms(lastindex - 1, &[4]),
            lastindex - 1,
            true,
            lastindex - 1,
            false,
        ),
        (
            id(lastterm - 3, lastindex - 3),
            lastindex,
            index_terms(lastindex - 2, &[4]),
            lastindex - 2,
            true,
            lastindex - 2,
            true,
        ),
        (
            id(lastterm - 2, lastindex - 2),
            lastindex,
            index_terms(lastindex - 1, &[4, 4]),
            lastindex,
            true,
            lastindex,
            false,
        ),
    ];
    for (i, (prev, committed, ents, wlasti, wappend, wcommit, wpanic)) in
        tests.into_iter().enumerate()
    {
        let app = LogSlice {
            term: 100,
            prev,
            entries: ents.clone(),
        };
        assert_eq!(Ok(()), app.valid(), "#{i}");
        let mut l = new_log(MemoryStorage::new());
        l.append(previous_ents.clone());
        l.committed = commit;
        let res = catch_unwind(AssertUnwindSafe(|| {
            let r = l.maybe_append(app, committed);
            let (glasti, gappend) = (r.unwrap_or(0), r.is_some());
            assert_eq!(wlasti, glasti, "#{i}");
            assert_eq!(wappend, gappend, "#{i}");
            assert_eq!(wcommit, l.committed, "#{i}");
            if gappend && !ents.is_empty() {
                let li = l.last_index();
                let gents = l
                    .slice(li - ents.len() as u64 + 1, li + 1, NO_LIMIT)
                    .unwrap();
                assert_eq!(ents, gents, "#{i}");
            }
        }));
        assert_eq!(wpanic, res.is_err(), "#{i}");
    }
}

#[test]
fn test_compaction_side_effects() {
    let last_index = 1000;
    let unstable_index = 750;
    let mut storage = MemoryStorage::new();
    storage
        .append(&index_term_range(1, 1, unstable_index + 1))
        .unwrap();
    let mut l = new_log(storage);
    l.append(index_term_range(
        unstable_index + 1,
        unstable_index + 1,
        last_index + 1,
    ));
    let last = l.last_entry_id();
    assert!(l.maybe_commit(last));
    let c = l.committed;
    l.applied_to(c, 0);
    let offset = 500;
    l.storage.compact(offset).unwrap();
    assert_eq!(last_index, l.last_index());
    for j in offset..=l.last_index() {
        assert_eq!(Ok(j), l.term(j));
    }
    for j in offset..=l.last_index() {
        assert!(l.match_term(id(j, j)));
    }
    let unstable_ents = l.next_unstable_ents();
    assert_eq!(250, unstable_ents.len());
    assert_eq!(751, unstable_ents[0].index);
    let prev = l.last_index();
    let li = l.last_index();
    l.append(vec![Entry {
        index: li + 1,
        term: li + 1,
        ..Default::default()
    }]);
    assert_eq!(prev + 1, l.last_index());
    let ents = l.entries(l.last_index(), NO_LIMIT).unwrap();
    assert_eq!(1, ents.len());
}

fn next_committed_setup(
    max_size: u64,
    applied: u64,
    applying: u64,
    allow_unstable: bool,
) -> RaftLog<MemoryStorage> {
    let ents = index_terms(4, &[1, 1, 1]);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(3, 1)).unwrap();
    storage.append(&ents[..1]).unwrap();
    let mut l = RaftLog::new_with_size(storage, logger(), max_size);
    l.append(ents);
    l.stable_to(id(1, 4));
    l.maybe_commit(id(1, 5));
    l.applied_to(applied, 0);
    l.accept_applying(applying, 0, allow_unstable);
    l
}

#[test]
fn test_has_next_committed_ents() {
    let tests = [
        (3, 3, true, false, false, true),
        (3, 4, true, false, false, true),
        (3, 5, true, false, false, false),
        (4, 4, true, false, false, true),
        (4, 5, true, false, false, false),
        (5, 5, true, false, false, false),
        (3, 3, false, false, false, true),
        (3, 4, false, false, false, false),
        (3, 5, false, false, false, false),
        (4, 4, false, false, false, false),
        (4, 5, false, false, false, false),
        (5, 5, false, false, false, false),
        (3, 3, true, true, false, false),
        (3, 3, true, false, true, false),
    ];
    for (i, (applied, applying, allow_unstable, paused, with_snap, w)) in
        tests.into_iter().enumerate()
    {
        let mut l = next_committed_setup(NO_LIMIT, applied, applying, allow_unstable);
        l.applying_ents_paused = paused;
        if with_snap {
            l.restore(snap(4, 1));
        }
        assert_eq!(w, l.has_next_committed_ents(allow_unstable), "#{i}");
    }
}

#[test]
fn test_next_committed_ents() {
    let ents = index_terms(4, &[1, 1, 1]);
    let tests = [
        (3, 3, true, false, false, ents[..2].to_vec()),
        (3, 4, true, false, false, ents[1..2].to_vec()),
        (3, 5, true, false, false, vec![]),
        (4, 4, true, false, false, ents[1..2].to_vec()),
        (4, 5, true, false, false, vec![]),
        (5, 5, true, false, false, vec![]),
        (3, 3, false, false, false, ents[..1].to_vec()),
        (3, 4, false, false, false, vec![]),
        (3, 5, false, false, false, vec![]),
        (4, 4, false, false, false, vec![]),
        (4, 5, false, false, false, vec![]),
        (5, 5, false, false, false, vec![]),
        (3, 3, true, true, false, vec![]),
        (3, 3, true, false, true, vec![]),
    ];
    for (i, (applied, applying, allow_unstable, paused, with_snap, w)) in
        tests.into_iter().enumerate()
    {
        let mut l = next_committed_setup(NO_LIMIT, applied, applying, allow_unstable);
        l.applying_ents_paused = paused;
        if with_snap {
            l.restore(snap(4, 1));
        }
        assert_eq!(w, l.next_committed_ents(allow_unstable), "#{i}");
    }
}

#[test]
fn test_accept_applying() {
    let max_size = 100;
    let tests = [
        (3, true, max_size - 1, true),
        (3, true, max_size, true),
        (3, true, max_size + 1, true),
        (4, true, max_size - 1, true),
        (4, true, max_size, true),
        (4, true, max_size + 1, true),
        (5, true, max_size - 1, false),
        (5, true, max_size, true),
        (5, true, max_size + 1, true),
        (3, false, max_size - 1, true),
        (3, false, max_size, true),
        (3, false, max_size + 1, true),
        (4, false, max_size - 1, false),
        (4, false, max_size, true),
        (4, false, max_size + 1, true),
        (5, false, max_size - 1, false),
        (5, false, max_size, true),
        (5, false, max_size + 1, true),
    ];
    for (i, (index, allow_unstable, size, wpaused)) in tests.into_iter().enumerate() {
        let ents = index_terms(4, &[1, 1, 1]);
        let mut storage = MemoryStorage::new();
        storage.apply_snapshot(snap(3, 1)).unwrap();
        storage.append(&ents[..1]).unwrap();
        let mut l = RaftLog::new_with_size(storage, logger(), max_size);
        l.append(ents);
        l.stable_to(id(1, 4));
        l.maybe_commit(id(1, 5));
        l.applied_to(3, 0);
        l.accept_applying(index, size, allow_unstable);
        assert_eq!(wpaused, l.applying_ents_paused, "#{i}");
    }
}

#[test]
fn test_applied_to() {
    let max_size = 100;
    let overshoot = 5;
    let tests = [
        (4, overshoot - 1, max_size + 1, true),
        (4, overshoot, max_size, true),
        (4, overshoot + 1, max_size - 1, false),
        (5, overshoot - 1, max_size + 1, true),
        (5, overshoot, max_size, true),
        (5, overshoot + 1, max_size - 1, false),
        (4, max_size + overshoot, 0, false),
        (4, max_size + overshoot + 1, 0, false),
    ];
    for (i, (index, size, wapplying_size, wpaused)) in tests.into_iter().enumerate() {
        let ents = index_terms(4, &[1, 1, 1]);
        let mut storage = MemoryStorage::new();
        storage.apply_snapshot(snap(3, 1)).unwrap();
        storage.append(&ents[..1]).unwrap();
        let mut l = RaftLog::new_with_size(storage, logger(), max_size);
        l.append(ents);
        l.stable_to(id(1, 4));
        l.maybe_commit(id(1, 5));
        l.applied_to(3, 0);
        l.accept_applying(5, max_size + overshoot, false);
        l.applied_to(index, size);
        assert_eq!(index, l.applied, "#{i}");
        assert_eq!(5, l.applying, "#{i}");
        assert_eq!(wapplying_size, l.applying_ents_size, "#{i}");
        assert_eq!(wpaused, l.applying_ents_paused, "#{i}");
    }
}

#[test]
fn test_next_unstable_ents() {
    let previous_ents = index_terms(1, &[1, 2]);
    let tests = [(3, vec![]), (1, previous_ents.clone())];
    for (i, (unstable, wents)) in tests.into_iter().enumerate() {
        let mut storage = MemoryStorage::new();
        storage.append(&previous_ents[..unstable - 1]).unwrap();
        let mut l = new_log(storage);
        l.append(previous_ents[unstable - 1..].to_vec());
        let ents = l.next_unstable_ents().to_vec();
        if let Some(last) = ents.last() {
            l.stable_to(pb_entry_id(last));
        }
        assert_eq!(wents, ents, "#{i}");
        assert_eq!(
            previous_ents.last().unwrap().index + 1,
            l.unstable.offset,
            "#{i}"
        );
    }
}

#[test]
fn test_commit_to() {
    let previous_ents = index_terms(1, &[1, 2, 3]);
    let commit = 2;
    let tests = [(3, 3, false), (1, 2, false), (4, 0, true)];
    for (i, (c, wcommit, wpanic)) in tests.into_iter().enumerate() {
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut l = new_log(MemoryStorage::new());
            l.append(previous_ents.clone());
            l.committed = commit;
            l.commit_to(c);
            assert_eq!(wcommit, l.committed, "#{i}");
        }));
        assert_eq!(wpanic, res.is_err(), "#{i}");
    }
}

#[test]
fn test_stable_to() {
    let tests = [(1, 1, 2), (2, 2, 3), (2, 1, 1), (3, 1, 1)];
    for (i, (stablei, stablet, wunstable)) in tests.into_iter().enumerate() {
        let mut l = new_log(MemoryStorage::new());
        l.append(index_terms(1, &[1, 2]));
        l.stable_to(id(stablet, stablei));
        assert_eq!(wunstable, l.unstable.offset, "#{i}");
    }
}

#[test]
fn test_stable_to_with_snap() {
    let (snapi, snapt) = (5, 2);
    let e = || index_terms(snapi + 1, &[snapt]);
    let tests = [
        (snapi + 1, snapt, vec![], snapi + 1),
        (snapi, snapt, vec![], snapi + 1),
        (snapi - 1, snapt, vec![], snapi + 1),
        (snapi + 1, snapt + 1, vec![], snapi + 1),
        (snapi, snapt + 1, vec![], snapi + 1),
        (snapi - 1, snapt + 1, vec![], snapi + 1),
        (snapi + 1, snapt, e(), snapi + 2),
        (snapi, snapt, e(), snapi + 1),
        (snapi - 1, snapt, e(), snapi + 1),
        (snapi + 1, snapt + 1, e(), snapi + 1),
        (snapi, snapt + 1, e(), snapi + 1),
        (snapi - 1, snapt + 1, e(), snapi + 1),
    ];
    for (i, (stablei, stablet, new_ents, wunstable)) in tests.into_iter().enumerate() {
        let mut s = MemoryStorage::new();
        s.apply_snapshot(snap(snapi, snapt)).unwrap();
        let mut l = new_log(s);
        l.append(new_ents);
        l.stable_to(id(stablet, stablei));
        assert_eq!(wunstable, l.unstable.offset, "#{i}");
    }
}

#[test]
fn test_compaction() {
    let tests: [(u64, Vec<u64>, Vec<i64>, bool); 3] = [
        (1000, vec![1001], vec![-1], false),
        (
            1000,
            vec![300, 500, 800, 900],
            vec![700, 500, 200, 100],
            true,
        ),
        (1000, vec![300, 299], vec![700, -1], false),
    ];
    for (i, (last_index, compact, wleft, wallow)) in tests.into_iter().enumerate() {
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut storage = MemoryStorage::new();
            storage
                .append(&index_term_range(1, 1, last_index + 1))
                .unwrap();
            let mut l = new_log(storage);
            l.maybe_commit(id(0, last_index));
            let c = l.committed;
            l.applied_to(c, 0);
            for j in 0..compact.len() {
                if l.storage.compact(compact[j]).is_err() {
                    assert!(!wallow, "#{i}");
                    continue;
                }
                assert_eq!(wleft[j], l.all_entries().len() as i64, "#{i}");
            }
        }));
        if res.is_err() {
            assert!(!wallow, "#{i}");
        }
    }
}

#[test]
fn test_log_restore() {
    let (index, term) = (1000, 1000);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(index, term)).unwrap();
    let l = new_log(storage);
    assert!(l.all_entries().is_empty());
    assert_eq!(index + 1, l.first_index());
    assert_eq!(index, l.committed);
    assert_eq!(index + 1, l.unstable.offset);
    assert_eq!(Ok(term), l.term(index));
}

#[test]
fn test_is_out_of_bounds() {
    let (offset, num) = (100, 100);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(offset, 0)).unwrap();
    let mut l = new_log(storage);
    l.append(index_term_range(offset + 1, offset + 1, offset + num + 1));
    let first = offset + 1;
    let tests = [
        (first - 2, first + 1, false, true),
        (first - 1, first + 1, false, true),
        (first, first, false, false),
        (first + num / 2, first + num / 2, false, false),
        (first + num - 1, first + num - 1, false, false),
        (first + num, first + num, false, false),
        (first + num, first + num + 1, true, false),
        (first + num + 1, first + num + 1, true, false),
    ];
    for (i, (lo, hi, wpanic, werr_compacted)) in tests.into_iter().enumerate() {
        let mut got = None;
        let p = panics(|| got = Some(l.must_check_out_of_bounds(lo, hi)));
        assert_eq!(wpanic, p, "#{i}");
        if let Some(err) = got {
            if werr_compacted {
                assert_eq!(Err(Error::Compacted), err, "#{i}");
            } else {
                assert_eq!(Ok(()), err, "#{i}");
            }
        }
    }
}

#[test]
fn test_term() {
    let (offset, num) = (100, 100);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(offset, 1)).unwrap();
    let mut l = new_log(storage);
    l.append(index_term_range(offset + 1, 1, num));
    let tests = [
        (offset - 1, Err(Error::Compacted)),
        (offset, Ok(1)),
        (offset + num / 2, Ok(num / 2)),
        (offset + num - 1, Ok(num - 1)),
        (offset + num, Err(Error::Unavailable)),
    ];
    for (i, (idx, w)) in tests.into_iter().enumerate() {
        assert_eq!(w, l.term(idx), "#{i}");
    }
}

#[test]
fn test_term_with_unstable_snapshot() {
    let storagesnapi = 100;
    let unstablesnapi = storagesnapi + 5;
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(storagesnapi, 1)).unwrap();
    let mut l = new_log(storage);
    l.restore(snap(unstablesnapi, 1));
    let tests = [
        (storagesnapi, Err(Error::Compacted)),
        (storagesnapi + 1, Err(Error::Compacted)),
        (unstablesnapi - 1, Err(Error::Compacted)),
        (unstablesnapi, Ok(1)),
        (unstablesnapi + 1, Err(Error::Unavailable)),
    ];
    for (i, (idx, w)) in tests.into_iter().enumerate() {
        assert_eq!(w, l.term(idx), "#{i}");
    }
}

#[test]
fn test_slice() {
    let (offset, num) = (100u64, 100u64);
    let last = offset + num;
    let half = offset + num / 2;
    let halfe = Entry {
        index: half,
        term: half,
        entry_type: EntryType::EntryNormal,
        ..Default::default()
    };
    let hs = entry_size(&halfe);
    let entries = |from, to| index_term_range(from, from, to);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(offset, 0)).unwrap();
    storage.append(&entries(offset + 1, half)).unwrap();
    let mut l = new_log(storage);
    l.append(entries(half, last));
    let tests: Vec<(u64, u64, u64, Vec<Entry>, bool)> = vec![
        (offset - 1, offset + 1, NO_LIMIT, vec![], false),
        (offset, offset + 1, NO_LIMIT, vec![], false),
        (half, half - 1, NO_LIMIT, vec![], true),
        (last, last + 1, NO_LIMIT, vec![], true),
        (offset + 1, offset + 1, NO_LIMIT, vec![], false),
        (
            offset + 1,
            half - 1,
            NO_LIMIT,
            entries(offset + 1, half - 1),
            false,
        ),
        (offset + 1, half, NO_LIMIT, entries(offset + 1, half), false),
        (
            offset + 1,
            half + 1,
            NO_LIMIT,
            entries(offset + 1, half + 1),
            false,
        ),
        (offset + 1, last, NO_LIMIT, entries(offset + 1, last), false),
        (half - 1, half, NO_LIMIT, entries(half - 1, half), false),
        (
            half - 1,
            half + 1,
            NO_LIMIT,
            entries(half - 1, half + 1),
            false,
        ),
        (half - 1, last, NO_LIMIT, entries(half - 1, last), false),
        (half, half + 1, NO_LIMIT, entries(half, half + 1), false),
        (half, last, NO_LIMIT, entries(half, last), false),
        (last - 1, last, NO_LIMIT, entries(last - 1, last), false),
        (offset + 1, last, 0, entries(offset + 1, offset + 2), false),
        (half - 1, half + 1, 0, entries(half - 1, half), false),
        (half, last, 0, entries(half, half + 1), false),
        (half + 1, last, 0, entries(half + 1, half + 2), false),
        (
            offset + 1,
            last,
            hs - 1,
            entries(offset + 1, offset + 2),
            false,
        ),
        (half - 1, half + 1, hs - 1, entries(half - 1, half), false),
        (half, last, hs - 1, entries(half, half + 1), false),
        (offset + 1, last, hs, entries(offset + 1, offset + 2), false),
        (half - 1, half + 1, hs, entries(half - 1, half), false),
        (half, last, hs, entries(half, half + 1), false),
        (
            offset + 1,
            last,
            hs + 1,
            entries(offset + 1, offset + 2),
            false,
        ),
        (half - 1, half + 1, hs + 1, entries(half - 1, half), false),
        (half, last, hs + 1, entries(half, half + 1), false),
        (
            offset + 1,
            last,
            hs * 2,
            entries(offset + 1, offset + 3),
            false,
        ),
        (half - 2, half + 1, hs * 2, entries(half - 2, half), false),
        (
            half - 1,
            half + 1,
            hs * 2,
            entries(half - 1, half + 1),
            false,
        ),
        (half, last, hs * 2, entries(half, half + 2), false),
        (
            half - 2,
            half + 1,
            hs * 3 - 1,
            entries(half - 2, half),
            false,
        ),
        (
            half - 1,
            half + 2,
            hs * 3,
            entries(half - 1, half + 2),
            false,
        ),
    ];
    let mut mismatches = Vec::new();
    for (i, (lo, hi, lim, w, wpanic)) in tests.into_iter().enumerate() {
        let mut got = None;
        let p = panics(|| got = Some(l.slice(lo, hi, lim)));
        assert_eq!(wpanic, p, "#{i}");
        let Some(g) = got else { continue };
        let want = if lo <= offset {
            Err(Error::Compacted)
        } else {
            Ok(w)
        };
        if want != g {
            mismatches.push(i);
        }
    }
    // Case #28 (log_test.go:939) differs because of the deliberate entry_size
    // deviation: Go counts the explicitly set EntryNormal type (2 bytes), so
    // entries 101..=103 take 18 bytes > 2*Size(halfe) = 16 and Go returns two
    // entries; here they take 12 bytes = 2*entry_size(halfe) and three fit.
    assert_eq!(ents_size(&entries(offset + 1, offset + 4)), hs * 2);
    assert_eq!(vec![28], mismatches, "mismatching cases");
}

#[test]
fn test_scan() {
    let (offset, num) = (47u64, 20u64);
    let last = offset + num;
    let half = offset + num / 2;
    let entries = |from, to| index_term_range(from, from, to);
    let entry_sz = ents_size(&entries(half, half + 1));
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(snap(offset, 0)).unwrap();
    storage.append(&entries(offset + 1, half)).unwrap();
    let mut l = new_log(storage);
    l.append(entries(half, last));
    for page_size in [0, 1, 10, 100, entry_sz, entry_sz + 1] {
        for lo in offset + 1..last {
            for hi in lo..=last {
                let mut got = Vec::new();
                l.scan::<Error>(lo, hi, page_size, |e| {
                    got.extend_from_slice(e);
                    assert!(e.len() == 1 || ents_size(e) <= page_size);
                    Ok(())
                })
                .unwrap();
                let want = l.slice(lo, hi, NO_LIMIT).unwrap();
                assert_eq!(
                    want, got,
                    "scan() and slice() mismatch on [{lo}, {hi}) @ {page_size}"
                );
            }
        }
    }
    let mut iters = 0;
    let brk = Error::Other("break".into());
    let res = l.scan(offset + 1, half, 0, |_| {
        iters += 1;
        if iters == 2 {
            return Err(brk.clone());
        }
        Ok(())
    });
    assert_eq!(Err(brk), res);
    assert_eq!(2, iters);
    l.scan::<Error>(offset + 1, offset + 11, entry_sz * 2, |ents| {
        assert_eq!(2, ents.len());
        assert_eq!(entry_sz * 2, ents_size(ents));
        Ok(())
    })
    .unwrap();
}
