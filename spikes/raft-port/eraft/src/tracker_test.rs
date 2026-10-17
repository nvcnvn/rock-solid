// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// tracker/inflights_test.go and tracker/progress_test.go. Licensed under the
// Apache License 2.0.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::tracker::{Inflight, Inflights, Progress, StateType};

fn inflights_buffer(indices: &[u64], sizes: &[u64]) -> Vec<Inflight> {
    assert_eq!(indices.len(), sizes.len(), "len(indices) != len(sizes)");
    indices
        .iter()
        .zip(sizes)
        .map(|(&index, &bytes)| Inflight { index, bytes })
        .collect()
}

fn inflights(
    start: usize,
    count: usize,
    bytes: u64,
    size: usize,
    buffer: Vec<Inflight>,
) -> Inflights {
    Inflights {
        start,
        count,
        bytes,
        size,
        max_bytes: 0,
        buffer,
    }
}

#[test]
fn test_inflights_add() {
    let mut inf = inflights(0, 0, 0, 10, vec![Inflight::default(); 10]);
    for i in 0..5 {
        inf.add(i, 100 + i);
    }
    let want = inflights(
        0,
        5,
        510,
        10,
        inflights_buffer(
            &[0, 1, 2, 3, 4, 0, 0, 0, 0, 0],
            &[100, 101, 102, 103, 104, 0, 0, 0, 0, 0],
        ),
    );
    assert_eq!(want, inf);
    for i in 5..10 {
        inf.add(i, 100 + i);
    }
    let want2 = inflights(
        0,
        10,
        1045,
        10,
        inflights_buffer(
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        ),
    );
    assert_eq!(want2, inf);

    let mut in2 = inflights(5, 0, 0, 10, vec![Inflight::default(); 10]);
    for i in 0..5 {
        in2.add(i, 100 + i);
    }
    let want21 = inflights(
        5,
        5,
        510,
        10,
        inflights_buffer(
            &[0, 0, 0, 0, 0, 0, 1, 2, 3, 4],
            &[0, 0, 0, 0, 0, 100, 101, 102, 103, 104],
        ),
    );
    assert_eq!(want21, in2);
    for i in 5..10 {
        in2.add(i, 100 + i);
    }
    let want22 = inflights(
        5,
        10,
        1045,
        10,
        inflights_buffer(
            &[5, 6, 7, 8, 9, 0, 1, 2, 3, 4],
            &[105, 106, 107, 108, 109, 100, 101, 102, 103, 104],
        ),
    );
    assert_eq!(want22, in2);
}

#[test]
fn test_inflight_free_to() {
    let mut inf = Inflights::new(10, 0);
    for i in 0..10 {
        inf.add(i, 100 + i);
    }
    let buf = || {
        inflights_buffer(
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        )
    };
    inf.free_le(0);
    assert_eq!(inflights(1, 9, 945, 10, buf()), inf);
    inf.free_le(4);
    assert_eq!(inflights(5, 5, 535, 10, buf()), inf);
    inf.free_le(8);
    assert_eq!(inflights(9, 1, 109, 10, buf()), inf);
    for i in 10..15 {
        inf.add(i, 100 + i);
    }
    inf.free_le(12);
    let buf2 = || {
        inflights_buffer(
            &[10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
            &[110, 111, 112, 113, 114, 105, 106, 107, 108, 109],
        )
    };
    assert_eq!(inflights(3, 2, 227, 10, buf2()), inf);
    inf.free_le(14);
    assert_eq!(inflights(0, 0, 0, 10, buf2()), inf);
}

#[test]
fn test_inflights_full() {
    let cases: [(&str, usize, u64, u64, u64, u64); 7] = [
        ("always-full", 0, 0, 0, 0, 0),
        ("single-entry", 1, 0, 1, 1, 2),
        ("single-entry-overflow", 1, 10, 1, 1, 2),
        ("multi-entry", 15, 0, 15, 6, 22),
        ("slight-overflow", 8, 400, 4, 2, 7),
        ("exact-max-bytes", 8, 406, 4, 3, 8),
        ("larger-overflow", 15, 408, 5, 1, 6),
    ];
    for (name, size, max_bytes, full_at, free_le, again_at) in cases {
        let mut inf = Inflights::new(size, max_bytes);
        let add_until_full = |inf: &mut Inflights, begin: u64, end: u64| {
            for i in begin..end {
                assert!(!inf.full(), "{name}: full at {i}, want {end}");
                inf.add(i, 100 + i);
            }
            assert!(inf.full(), "{name}: not full at {end}");
        };
        add_until_full(&mut inf, 0, full_at);
        inf.free_le(free_le);
        add_until_full(&mut inf, full_at, again_at);
        let r = catch_unwind(AssertUnwindSafe(|| inf.add(100, 1024)));
        assert!(r.is_err(), "{name}: Add() did not panic");
    }
}

#[test]
fn test_inflights_reset() {
    let mut inf = Inflights::new(10, 1000);
    let mut index = 0;
    for _ in 0..100 {
        inf.reset();
        for _ in 0..5 {
            assert!(!inf.full());
            index += 1;
            inf.add(index, 16);
        }
        inf.free_le(index - 2);
        assert!(!inf.full());
        assert_eq!(2, inf.count());
    }
    inf.free_le(index);
    assert_eq!(0, inf.count());
}

#[test]
fn test_progress_string() {
    let mut ins = Inflights::new(1, 0);
    ins.add(123, 1);
    let mut pr = Progress::default();
    pr.match_ = 1;
    pr.next = 2;
    pr.state = StateType::StateSnapshot;
    pr.pending_snapshot = 123;
    pr.recent_active = false;
    pr.msg_app_flow_paused = true;
    pr.is_learner = true;
    pr.inflights = ins;
    assert_eq!(
        "StateSnapshot match=1 next=2 learner paused pendingSnap=123 inactive inflight=1[full]",
        pr.to_string()
    );
}

#[test]
fn test_progress_is_paused() {
    let tests = [
        (StateType::StateProbe, false, false),
        (StateType::StateProbe, true, true),
        (StateType::StateReplicate, false, false),
        (StateType::StateReplicate, true, true),
        (StateType::StateSnapshot, false, true),
        (StateType::StateSnapshot, true, true),
    ];
    for (i, (state, paused, w)) in tests.into_iter().enumerate() {
        let mut p = Progress::default();
        p.state = state;
        p.msg_app_flow_paused = paused;
        p.inflights = Inflights::new(256, 0);
        assert_eq!(w, p.is_paused(), "#{i}");
    }
}

#[test]
fn test_progress_resume() {
    let mut p = Progress::default();
    p.next = 2;
    p.msg_app_flow_paused = true;
    p.maybe_decr_to(1, 1);
    assert!(!p.msg_app_flow_paused);
    p.msg_app_flow_paused = true;
    p.maybe_update(2);
    assert!(!p.msg_app_flow_paused);
}

#[test]
fn test_progress_become_probe() {
    let m = 1;
    let mk = |state, pending_snapshot| {
        let mut p = Progress::default();
        p.state = state;
        p.match_ = m;
        p.next = 5;
        p.pending_snapshot = pending_snapshot;
        p.inflights = Inflights::new(256, 0);
        p
    };
    let tests = [
        (mk(StateType::StateReplicate, 0), 2),
        (mk(StateType::StateSnapshot, 10), 11),
        (mk(StateType::StateSnapshot, 0), 2),
    ];
    for (i, (mut p, wnext)) in tests.into_iter().enumerate() {
        p.become_probe();
        assert_eq!(StateType::StateProbe, p.state, "#{i}");
        assert_eq!(m, p.match_, "#{i}");
        assert_eq!(wnext, p.next, "#{i}");
    }
}

#[test]
fn test_progress_become_replicate() {
    let mut p = Progress::default();
    p.state = StateType::StateProbe;
    p.match_ = 1;
    p.next = 5;
    p.inflights = Inflights::new(256, 0);
    p.become_replicate();
    assert_eq!(StateType::StateReplicate, p.state);
    assert_eq!(1, p.match_);
    assert_eq!(p.match_ + 1, p.next);
}

#[test]
fn test_progress_become_snapshot() {
    let mut p = Progress::default();
    p.state = StateType::StateProbe;
    p.match_ = 1;
    p.next = 5;
    p.inflights = Inflights::new(256, 0);
    p.become_snapshot(10);
    assert_eq!(StateType::StateSnapshot, p.state);
    assert_eq!(1, p.match_);
    assert_eq!(10, p.pending_snapshot);
}

#[test]
fn test_progress_update() {
    let (prev_m, prev_n) = (3, 5);
    let tests = [
        (prev_m - 1, prev_m, prev_n, false),
        (prev_m, prev_m, prev_n, false),
        (prev_m + 1, prev_m + 1, prev_n, true),
        (prev_m + 2, prev_m + 2, prev_n + 1, true),
    ];
    for (i, (update, wm, wn, wok)) in tests.into_iter().enumerate() {
        let mut p = Progress::default();
        p.match_ = prev_m;
        p.next = prev_n;
        assert_eq!(wok, p.maybe_update(update), "#{i}");
        assert_eq!(wm, p.match_, "#{i}");
        assert_eq!(wn, p.next, "#{i}");
    }
}

#[test]
fn test_progress_maybe_decr() {
    use StateType::{StateProbe, StateReplicate};
    let tests = [
        (StateReplicate, 5, 10, 5, 5, false, 10),
        (StateReplicate, 5, 10, 4, 4, false, 10),
        (StateReplicate, 5, 10, 9, 9, true, 6),
        (StateProbe, 0, 0, 0, 0, false, 0),
        (StateProbe, 0, 10, 5, 5, false, 10),
        (StateProbe, 0, 10, 9, 9, true, 9),
        (StateProbe, 0, 2, 1, 1, true, 1),
        (StateProbe, 0, 1, 0, 0, true, 1),
        (StateProbe, 0, 10, 9, 2, true, 3),
        (StateProbe, 0, 10, 9, 0, true, 1),
    ];
    for (i, (state, m, n, rejected, last, w, wn)) in tests.into_iter().enumerate() {
        let mut p = Progress::default();
        p.state = state;
        p.match_ = m;
        p.next = n;
        assert_eq!(w, p.maybe_decr_to(rejected, last), "#{i}");
        assert_eq!(m, p.match_, "#{i}");
        assert_eq!(wn, p.next, "#{i}");
    }
}
