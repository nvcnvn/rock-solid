// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// log_unstable_test.go. Licensed under the Apache License 2.0.

use std::rc::Rc;

use crate::log_unstable::Unstable;
use crate::logger::NopLogger;
use crate::raftpb::{Entry, Snapshot, SnapshotMetadata};
use crate::testutil::index_terms;
use crate::types::EntryId;

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

fn unstable(
    entries: Vec<Entry>,
    offset: u64,
    offset_in_progress: u64,
    snapshot: Option<Snapshot>,
) -> Unstable {
    Unstable {
        snapshot,
        entries,
        offset,
        snapshot_in_progress: false,
        offset_in_progress,
        logger: Rc::new(NopLogger),
    }
}

#[test]
fn test_unstable_maybe_first_index() {
    let tests = [
        (index_terms(5, &[1]), 5, None, None),
        (vec![], 0, None, None),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), Some(5)),
        (vec![], 5, Some(snap(4, 1)), Some(5)),
    ];
    for (i, (entries, offset, s, w)) in tests.into_iter().enumerate() {
        let u = unstable(entries, offset, offset, s);
        assert_eq!(w, u.maybe_first_index(), "#{i}");
    }
}

#[test]
fn test_maybe_last_index() {
    let tests = [
        (index_terms(5, &[1]), 5, None, Some(5)),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), Some(5)),
        (vec![], 5, Some(snap(4, 1)), Some(4)),
        (vec![], 0, None, None),
    ];
    for (i, (entries, offset, s, w)) in tests.into_iter().enumerate() {
        let u = unstable(entries, offset, offset, s);
        assert_eq!(w, u.maybe_last_index(), "#{i}");
    }
}

#[test]
fn test_unstable_maybe_term() {
    let tests = [
        (index_terms(5, &[1]), 5, None, 5, Some(1)),
        (index_terms(5, &[1]), 5, None, 6, None),
        (index_terms(5, &[1]), 5, None, 4, None),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), 5, Some(1)),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), 6, None),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), 4, Some(1)),
        (index_terms(5, &[1]), 5, Some(snap(4, 1)), 3, None),
        (vec![], 5, Some(snap(4, 1)), 5, None),
        (vec![], 5, Some(snap(4, 1)), 4, Some(1)),
        (vec![], 0, None, 5, None),
    ];
    for (i, (entries, offset, s, index, w)) in tests.into_iter().enumerate() {
        let u = unstable(entries, offset, offset, s);
        assert_eq!(w, u.maybe_term(index), "#{i}");
    }
}

#[test]
fn test_unstable_restore() {
    let mut u = unstable(index_terms(5, &[1]), 5, 6, Some(snap(4, 1)));
    u.snapshot_in_progress = true;
    let s = snap(6, 2);
    u.restore(s.clone());
    assert_eq!(s.metadata.index + 1, u.offset);
    assert_eq!(s.metadata.index + 1, u.offset_in_progress);
    assert!(u.entries.is_empty());
    assert_eq!(Some(s), u.snapshot);
    assert!(!u.snapshot_in_progress);
}

#[test]
fn test_unstable_next_entries() {
    let tests = [
        (index_terms(5, &[1, 1]), 5, 5, index_terms(5, &[1, 1])),
        (index_terms(5, &[1, 1]), 5, 6, index_terms(6, &[1])),
        (index_terms(5, &[1, 1]), 5, 7, vec![]),
    ];
    for (i, (entries, offset, oip, w)) in tests.into_iter().enumerate() {
        let u = unstable(entries, offset, oip, None);
        assert_eq!(w, u.next_entries(), "#{i}");
    }
}

#[test]
fn test_unstable_next_snapshot() {
    let s = snap(4, 1);
    let tests = [
        (None, false, None),
        (Some(s.clone()), false, Some(s.clone())),
        (Some(s), true, None),
    ];
    for (i, (snapshot, in_progress, w)) in tests.into_iter().enumerate() {
        let mut u = unstable(vec![], 0, 0, snapshot);
        u.snapshot_in_progress = in_progress;
        assert_eq!(w.as_ref(), u.next_snapshot(), "#{i}");
    }
}

#[test]
fn test_unstable_accept_in_progress() {
    let s = || Some(snap(4, 1));
    let tests = [
        (vec![], None, 5, false, 5, false),
        (index_terms(5, &[1]), None, 5, false, 6, false),
        (index_terms(5, &[1, 1]), None, 5, false, 7, false),
        (index_terms(5, &[1, 1]), None, 6, false, 7, false),
        (index_terms(5, &[1, 1]), None, 7, false, 7, false),
        (vec![], s(), 5, false, 5, true),
        (index_terms(5, &[1]), s(), 5, false, 6, true),
        (index_terms(5, &[1, 1]), s(), 5, false, 7, true),
        (index_terms(5, &[1, 1]), s(), 6, false, 7, true),
        (index_terms(5, &[1, 1]), s(), 7, false, 7, true),
        (vec![], s(), 5, true, 5, true),
        (index_terms(5, &[1]), s(), 5, true, 6, true),
        (index_terms(5, &[1, 1]), s(), 5, true, 7, true),
        (index_terms(5, &[1, 1]), s(), 6, true, 7, true),
        (index_terms(5, &[1, 1]), s(), 7, true, 7, true),
    ];
    for (i, (entries, snapshot, oip, sip, woip, wsip)) in tests.into_iter().enumerate() {
        let mut u = unstable(entries, 0, oip, snapshot);
        u.snapshot_in_progress = sip;
        u.accept_in_progress();
        assert_eq!(woip, u.offset_in_progress, "#{i}");
        assert_eq!(wsip, u.snapshot_in_progress, "#{i}");
    }
}

#[test]
fn test_unstable_stable_to() {
    let tests = [
        (vec![], 0, 0, None, 5, 1, 0, 0, 0),
        (index_terms(5, &[1]), 5, 6, None, 5, 1, 6, 6, 0),
        (index_terms(5, &[1, 1]), 5, 6, None, 5, 1, 6, 6, 1),
        (index_terms(5, &[1, 1]), 5, 7, None, 5, 1, 6, 7, 1),
        (index_terms(6, &[2]), 6, 7, None, 6, 1, 6, 7, 1),
        (index_terms(5, &[1]), 5, 6, None, 4, 1, 5, 6, 1),
        (index_terms(5, &[1]), 5, 6, None, 4, 2, 5, 6, 1),
        (index_terms(5, &[1]), 5, 6, Some(snap(4, 1)), 5, 1, 6, 6, 0),
        (
            index_terms(5, &[1, 1]),
            5,
            6,
            Some(snap(4, 1)),
            5,
            1,
            6,
            6,
            1,
        ),
        (
            index_terms(5, &[1, 1]),
            5,
            7,
            Some(snap(4, 1)),
            5,
            1,
            6,
            7,
            1,
        ),
        (index_terms(6, &[2]), 6, 7, Some(snap(5, 1)), 6, 1, 6, 7, 1),
        (index_terms(5, &[1]), 5, 6, Some(snap(4, 1)), 4, 1, 5, 6, 1),
        (index_terms(5, &[2]), 5, 6, Some(snap(4, 2)), 4, 1, 5, 6, 1),
    ];
    for (i, (entries, offset, oip, s, index, term, woffset, woip, wlen)) in
        tests.into_iter().enumerate()
    {
        let mut u = unstable(entries, offset, oip, s);
        u.stable_to(EntryId { term, index });
        assert_eq!(woffset, u.offset, "#{i}");
        assert_eq!(woip, u.offset_in_progress, "#{i}");
        assert_eq!(wlen, u.entries.len(), "#{i}");
    }
}

#[test]
fn test_unstable_truncate_and_append() {
    let tests = [
        (
            index_terms(5, &[1]),
            5,
            5,
            index_terms(6, &[1, 1]),
            5,
            5,
            index_terms(5, &[1, 1, 1]),
        ),
        (
            index_terms(5, &[1]),
            5,
            6,
            index_terms(6, &[1, 1]),
            5,
            6,
            index_terms(5, &[1, 1, 1]),
        ),
        (
            index_terms(5, &[1]),
            5,
            5,
            index_terms(5, &[2, 2]),
            5,
            5,
            index_terms(5, &[2, 2]),
        ),
        (
            index_terms(5, &[1]),
            5,
            5,
            index_terms(4, &[2, 2, 2]),
            4,
            4,
            index_terms(4, &[2, 2, 2]),
        ),
        (
            index_terms(5, &[1]),
            5,
            6,
            index_terms(5, &[2, 2]),
            5,
            5,
            index_terms(5, &[2, 2]),
        ),
        (
            index_terms(5, &[1, 1, 1]),
            5,
            5,
            index_terms(6, &[2]),
            5,
            5,
            index_terms(5, &[1, 2]),
        ),
        (
            index_terms(5, &[1, 1, 1]),
            5,
            5,
            index_terms(7, &[2, 2]),
            5,
            5,
            index_terms(5, &[1, 1, 2, 2]),
        ),
        (
            index_terms(5, &[1, 1, 1]),
            5,
            6,
            index_terms(6, &[2]),
            5,
            6,
            index_terms(5, &[1, 2]),
        ),
        (
            index_terms(5, &[1, 1, 1]),
            5,
            7,
            index_terms(6, &[2]),
            5,
            6,
            index_terms(5, &[1, 2]),
        ),
    ];
    for (i, (entries, offset, oip, toappend, woffset, woip, wentries)) in
        tests.into_iter().enumerate()
    {
        let mut u = unstable(entries, offset, oip, None);
        u.truncate_and_append(toappend);
        assert_eq!(woffset, u.offset, "#{i}");
        assert_eq!(woip, u.offset_in_progress, "#{i}");
        assert_eq!(wentries, u.entries, "#{i}");
    }
}
