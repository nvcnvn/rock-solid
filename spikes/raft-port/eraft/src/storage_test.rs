// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// storage_test.go. Licensed under the Apache License 2.0.

use crate::Error;
use crate::raftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata, entry_size};
use crate::storage::{MemoryStorage, Storage};
use crate::testutil::index_terms;

fn ms(ents: Vec<Entry>) -> MemoryStorage {
    MemoryStorage {
        hard_state: HardState::default(),
        snapshot: Snapshot::default(),
        ents,
    }
}

#[test]
fn test_storage_term() {
    let ents = index_terms(3, &[3, 4, 5]);
    let tests = [
        (2, Err(Error::Compacted)),
        (3, Ok(3)),
        (4, Ok(4)),
        (5, Ok(5)),
        (6, Err(Error::Unavailable)),
    ];
    for (i, (idx, w)) in tests.into_iter().enumerate() {
        let s = ms(ents.clone());
        assert_eq!(w, s.term(idx), "#{i}");
    }
}

#[test]
fn test_storage_entries() {
    let ents = index_terms(3, &[3, 4, 5, 6]);
    let sz = |i: usize| entry_size(&ents[i]);
    let max = u64::MAX;
    let tests = [
        (2, 6, max, Err(Error::Compacted)),
        (3, 4, max, Err(Error::Compacted)),
        (4, 5, max, Ok(index_terms(4, &[4]))),
        (4, 6, max, Ok(index_terms(4, &[4, 5]))),
        (4, 7, max, Ok(index_terms(4, &[4, 5, 6]))),
        (4, 7, 0, Ok(index_terms(4, &[4]))),
        (4, 7, sz(1) + sz(2), Ok(index_terms(4, &[4, 5]))),
        (4, 7, sz(1) + sz(2) + sz(3) / 2, Ok(index_terms(4, &[4, 5]))),
        (4, 7, sz(1) + sz(2) + sz(3) - 1, Ok(index_terms(4, &[4, 5]))),
        (4, 7, sz(1) + sz(2) + sz(3), Ok(index_terms(4, &[4, 5, 6]))),
    ];
    for (i, (lo, hi, maxsize, w)) in tests.into_iter().enumerate() {
        let s = ms(ents.clone());
        assert_eq!(w, s.entries(lo, hi, maxsize), "#{i}");
    }
}

#[test]
fn test_storage_last_index() {
    let mut s = ms(index_terms(3, &[3, 4, 5]));
    assert_eq!(Ok(5), s.last_index());
    s.append(&index_terms(6, &[5])).unwrap();
    assert_eq!(Ok(6), s.last_index());
}

#[test]
fn test_storage_first_index() {
    let mut s = ms(index_terms(3, &[3, 4, 5]));
    assert_eq!(Ok(4), s.first_index());
    s.compact(4).unwrap();
    assert_eq!(Ok(5), s.first_index());
}

#[test]
fn test_storage_compact() {
    let ents = index_terms(3, &[3, 4, 5]);
    let tests = [
        (2, Err(Error::Compacted), 3, 3, 3),
        (3, Err(Error::Compacted), 3, 3, 3),
        (4, Ok(()), 4, 4, 2),
        (5, Ok(()), 5, 5, 1),
    ];
    for (i, (idx, werr, windex, wterm, wlen)) in tests.into_iter().enumerate() {
        let mut s = ms(ents.clone());
        assert_eq!(werr, s.compact(idx), "#{i}");
        assert_eq!(windex, s.ents[0].index, "#{i}");
        assert_eq!(wterm, s.ents[0].term, "#{i}");
        assert_eq!(wlen, s.ents.len(), "#{i}");
    }
}

#[test]
fn test_storage_create_snapshot() {
    let ents = index_terms(3, &[3, 4, 5]);
    let cs = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let data = b"data".to_vec();
    let want = |index| Snapshot {
        data: data.clone(),
        metadata: SnapshotMetadata {
            index,
            term: index,
            conf_state: cs.clone(),
        },
    };
    for (i, idx) in [4, 5].into_iter().enumerate() {
        let mut s = ms(ents.clone());
        assert_eq!(
            Ok(want(idx)),
            s.create_snapshot(idx, Some(&cs), data.clone()),
            "#{i}"
        );
    }
}

#[test]
fn test_storage_append() {
    let ents = index_terms(3, &[3, 4, 5]);
    let tests = [
        (index_terms(1, &[1, 2]), index_terms(3, &[3, 4, 5])),
        (index_terms(3, &[3, 4, 5]), index_terms(3, &[3, 4, 5])),
        (index_terms(3, &[3, 6, 6]), index_terms(3, &[3, 6, 6])),
        (index_terms(3, &[3, 4, 5, 5]), index_terms(3, &[3, 4, 5, 5])),
        (index_terms(2, &[3, 3, 5]), index_terms(3, &[3, 5])),
        (index_terms(4, &[5]), index_terms(3, &[3, 5])),
        (index_terms(6, &[5]), index_terms(3, &[3, 4, 5, 5])),
    ];
    for (i, (entries, wentries)) in tests.into_iter().enumerate() {
        let mut s = ms(ents.clone());
        assert_eq!(Ok(()), s.append(&entries), "#{i}");
        assert_eq!(wentries, s.ents, "#{i}");
    }
}

#[test]
fn test_storage_apply_snapshot() {
    let cs = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let data = b"data".to_vec();
    let sn = |index| Snapshot {
        data: data.clone(),
        metadata: SnapshotMetadata {
            index,
            term: index,
            conf_state: cs.clone(),
        },
    };
    let cases = [
        ("normal case", vec![sn(4)], Ok(())),
        (
            "snapshot out of date",
            vec![sn(4), sn(3)],
            Err(Error::SnapOutOfDate),
        ),
        ("bootstrap with confState", vec![sn(0)], Ok(())),
    ];
    for (name, snaps, want) in cases {
        let mut s = MemoryStorage::new();
        let mut res = Ok(());
        for snap in snaps {
            res = s.apply_snapshot(snap);
        }
        assert_eq!(want, res, "{name}");
    }
}
