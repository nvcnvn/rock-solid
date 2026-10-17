// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// rawnode_test.go (and ignoreSizeHintMemStorage from node_test.go). Licensed
// under the Apache License 2.0.
//
// RawNode owns its Storage here, so `s.Append(...)` becomes
// `rn.store_mut().append(...)`. readyWithoutAccept is merged into Ready() in the
// port; checks against it use the raft log's next unstable entries instead.

use std::cell::RefCell;

use crate::Error;
use crate::log::NO_LIMIT;
use crate::node::Ready;
use crate::raft::{ReadState, StateType};
use crate::raftpb::{
    ConfChange, ConfChangeI, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
    ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    entry_size,
};
use crate::rawnode::RawNode;
use crate::storage::{MemoryStorage, Storage};
use crate::testutil::{TestRaft, new_test_config, with_peers};
use crate::tracker;
use crate::util::{is_local_msg, payload_size};

type TestRawNode = RawNode<MemoryStorage>;

fn new_raw_node(s: MemoryStorage) -> TestRawNode {
    RawNode::new(&mut new_test_config(1, 10, 1), s).unwrap()
}

fn lead(rd: &Ready) -> Option<u64> {
    rd.soft_state.map(|s| s.lead)
}

fn decode_cc(ent: &Entry) -> Option<ConfChangeI> {
    match ent.entry_type {
        EntryType::EntryConfChange => Some(ConfChangeI::V1(ConfChange::decode(&ent.data).unwrap())),
        EntryType::EntryConfChangeV2 => {
            Some(ConfChangeI::V2(ConfChangeV2::decode(&ent.data).unwrap()))
        }
        EntryType::EntryNormal => None,
    }
}

fn single(change_type: ConfChangeType, node_id: u64) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type,
        node_id,
    }
}

#[test]
fn test_raw_node_step() {
    for i in 0..24u8 {
        let msgt = msg_type_by_index(i);
        let mut s = MemoryStorage::new();
        s.set_hard_state(HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        })
        .unwrap();
        s.append(&[Entry {
            term: 1,
            index: 1,
            ..Default::default()
        }])
        .unwrap();
        s.apply_snapshot(Snapshot {
            metadata: SnapshotMetadata {
                conf_state: ConfState {
                    voters: vec![1],
                    ..Default::default()
                },
                index: 1,
                term: 1,
            },
            ..Default::default()
        })
        .unwrap();
        let mut rn = new_raw_node(s);
        let err = rn.step(Message {
            msg_type: msgt,
            ..Default::default()
        });
        if is_local_msg(msgt) {
            assert_eq!(Err(Error::StepLocalMsg), err, "#{i}");
        }
    }
}

fn msg_type_by_index(i: u8) -> MessageType {
    use MessageType::*;
    [
        MsgHup,
        MsgBeat,
        MsgProp,
        MsgApp,
        MsgAppResp,
        MsgVote,
        MsgVoteResp,
        MsgSnap,
        MsgHeartbeat,
        MsgHeartbeatResp,
        MsgUnreachable,
        MsgSnapStatus,
        MsgCheckQuorum,
        MsgTransferLeader,
        MsgTimeoutNow,
        MsgReadIndex,
        MsgReadIndexResp,
        MsgPreVote,
        MsgPreVoteResp,
        MsgStorageAppend,
        MsgStorageAppendResp,
        MsgStorageApply,
        MsgStorageApplyResp,
        MsgForgetLeader,
    ][i as usize]
}

fn cs(
    voters: &[u64],
    outgoing: &[u64],
    learners: &[u64],
    next: &[u64],
    auto_leave: bool,
) -> ConfState {
    ConfState {
        voters: voters.to_vec(),
        voters_outgoing: outgoing.to_vec(),
        learners: learners.to_vec(),
        learners_next: next.to_vec(),
        auto_leave,
    }
}

#[test]
fn test_raw_node_propose_and_conf_change() {
    use ConfChangeTransition::*;
    use ConfChangeType::*;
    let v2 = |changes: Vec<ConfChangeSingle>, transition| {
        ConfChangeI::V2(ConfChangeV2 {
            transition,
            changes,
            context: vec![],
        })
    };
    let demote = || {
        vec![
            single(ConfChangeAddNode, 2),
            single(ConfChangeAddLearnerNode, 1),
            single(ConfChangeAddLearnerNode, 3),
        ]
    };
    let test_cases: Vec<(ConfChangeI, ConfState, Option<ConfState>)> = vec![
        (
            ConfChangeI::V1(ConfChange {
                change_type: ConfChangeAddNode,
                node_id: 2,
                ..Default::default()
            }),
            cs(&[1, 2], &[], &[], &[], false),
            None,
        ),
        (
            v2(vec![single(ConfChangeAddNode, 2)], ConfChangeTransitionAuto),
            cs(&[1, 2], &[], &[], &[], false),
            None,
        ),
        (
            v2(
                vec![single(ConfChangeAddLearnerNode, 2)],
                ConfChangeTransitionAuto,
            ),
            cs(&[1], &[], &[2], &[], false),
            None,
        ),
        (
            v2(
                vec![single(ConfChangeAddLearnerNode, 2)],
                ConfChangeTransitionJointExplicit,
            ),
            cs(&[1], &[1], &[2], &[], false),
            Some(cs(&[1], &[], &[2], &[], false)),
        ),
        (
            v2(
                vec![single(ConfChangeAddLearnerNode, 2)],
                ConfChangeTransitionJointImplicit,
            ),
            cs(&[1], &[1], &[2], &[], true),
            Some(cs(&[1], &[], &[2], &[], false)),
        ),
        (
            v2(demote(), ConfChangeTransitionAuto),
            cs(&[2], &[1], &[3], &[1], true),
            Some(cs(&[2], &[], &[1, 3], &[], false)),
        ),
        (
            v2(demote(), ConfChangeTransitionJointExplicit),
            cs(&[2], &[1], &[3], &[1], false),
            Some(cs(&[2], &[], &[1, 3], &[], false)),
        ),
        (
            v2(demote(), ConfChangeTransitionJointImplicit),
            cs(&[2], &[1], &[3], &[1], true),
            Some(cs(&[2], &[], &[1, 3], &[], false)),
        ),
    ];
    for (i, (cc, exp, exp2)) in test_cases.into_iter().enumerate() {
        let mut rn = new_raw_node(with_peers(&[1]));
        rn.campaign().unwrap();
        let mut proposed = false;
        let mut ccdata = Vec::new();
        let mut got_cs = None;
        while got_cs.is_none() {
            let rd = rn.ready();
            rn.store_mut().append(&rd.entries).unwrap();
            for ent in &rd.committed_entries {
                if let Some(c) = decode_cc(ent) {
                    got_cs = Some(rn.apply_conf_change(&c));
                }
            }
            let l = lead(&rd);
            rn.advance(rd);
            if !proposed && l == Some(rn.raft.id) {
                rn.propose(b"somedata".to_vec()).unwrap();
                match &cc {
                    ConfChangeI::V1(c) => {
                        ccdata = c.encode();
                        rn.propose_conf_change(c.clone()).unwrap();
                    }
                    ConfChangeI::V2(c) => {
                        ccdata = c.encode();
                        rn.propose_conf_change(c.clone()).unwrap();
                    }
                }
                proposed = true;
            }
        }
        let last_index = rn.store().last_index().unwrap();
        let entries = rn
            .store()
            .entries(last_index - 1, last_index + 1, NO_LIMIT)
            .unwrap();
        assert_eq!(2, entries.len(), "#{i}");
        assert_eq!(b"somedata".to_vec(), entries[0].data, "#{i}");
        let typ = if cc.as_v1().is_some() {
            EntryType::EntryConfChange
        } else {
            EntryType::EntryConfChangeV2
        };
        assert_eq!(typ, entries[1].entry_type, "#{i}");
        assert_eq!(ccdata, entries[1].data, "#{i}");
        assert_eq!(Some(exp.clone()), got_cs, "#{i}");
        let (auto_leave, ok) = cc.as_v2().enter_joint();
        let maybe_plus_one = u64::from(ok && auto_leave);
        assert_eq!(
            last_index + maybe_plus_one,
            rn.raft.pending_conf_index,
            "#{i}"
        );

        let mut rd = rn.ready();
        let mut context = Vec::new();
        if !exp.auto_leave {
            assert!(rd.entries.is_empty(), "#{i}");
            rn.advance(rd);
            if exp2.is_none() {
                continue;
            }
            context = b"manual".to_vec();
            rn.propose_conf_change(ConfChangeV2 {
                context: context.clone(),
                ..Default::default()
            })
            .unwrap();
            rd = rn.ready();
        }
        assert_eq!(1, rd.entries.len(), "#{i}");
        assert_eq!(
            EntryType::EntryConfChangeV2,
            rd.entries[0].entry_type,
            "#{i}"
        );
        let c = ConfChangeV2::decode(&rd.entries[0].data).unwrap();
        assert_eq!(
            ConfChangeV2 {
                context: context.clone(),
                ..Default::default()
            },
            c,
            "#{i}"
        );
        let got = rn.apply_conf_change(&ConfChangeI::V2(c));
        assert_eq!(exp2, Some(got), "#{i}");
        rn.advance(rd);
    }
}

#[test]
fn test_raw_node_joint_auto_leave() {
    let test_cc = ConfChangeV2 {
        changes: vec![single(ConfChangeType::ConfChangeAddLearnerNode, 2)],
        transition: ConfChangeTransition::ConfChangeTransitionJointImplicit,
        context: vec![],
    };
    let exp_cs = cs(&[1], &[1], &[2], &[], true);
    let exp2_cs = cs(&[1], &[], &[2], &[], false);
    let mut rn = new_raw_node(with_peers(&[1]));
    rn.campaign().unwrap();
    let mut proposed = false;
    let mut ccdata = Vec::new();
    let mut got_cs = None;
    while got_cs.is_none() {
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        for ent in &rd.committed_entries {
            if ent.entry_type == EntryType::EntryConfChangeV2 {
                let c = ConfChangeV2::decode(&ent.data).unwrap();
                let term = rn.raft.term + 1;
                let _ = rn.step(Message {
                    msg_type: MessageType::MsgHeartbeatResp,
                    from: 1,
                    term,
                    ..Default::default()
                });
                got_cs = Some(rn.apply_conf_change(&ConfChangeI::V2(c)));
            }
        }
        let l = lead(&rd);
        rn.advance(rd);
        if !proposed && l == Some(rn.raft.id) {
            rn.propose(b"somedata".to_vec()).unwrap();
            ccdata = test_cc.encode();
            rn.propose_conf_change(test_cc.clone()).unwrap();
            proposed = true;
        }
    }
    let last_index = rn.store().last_index().unwrap();
    let entries = rn
        .store()
        .entries(last_index - 1, last_index + 1, NO_LIMIT)
        .unwrap();
    assert_eq!(2, entries.len());
    assert_eq!(b"somedata".to_vec(), entries[0].data);
    assert_eq!(EntryType::EntryConfChangeV2, entries[1].entry_type);
    assert_eq!(ccdata, entries[1].data);
    assert_eq!(Some(exp_cs), got_cs);
    assert_eq!(0, rn.raft.pending_conf_index);
    // readyWithoutAccept().Entries == nextUnstableEnts.
    assert!(rn.raft.raft_log.next_unstable_ents().is_empty());

    rn.campaign().unwrap();
    for _ in 0..3 {
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        rn.advance(rd);
    }
    let rd = rn.ready();
    rn.store_mut().append(&rd.entries).unwrap();
    assert_eq!(1, rd.entries.len());
    assert_eq!(EntryType::EntryConfChangeV2, rd.entries[0].entry_type);
    let c = ConfChangeV2::decode(&rd.entries[0].data).unwrap();
    assert_eq!(ConfChangeV2::default(), c);
    let got = rn.apply_conf_change(&ConfChangeI::V2(c));
    assert_eq!(exp2_cs, got);
}

#[test]
fn test_raw_node_propose_add_duplicate_node() {
    let mut rn = new_raw_node(with_peers(&[1]));
    let rd = rn.ready();
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    rn.campaign().unwrap();
    loop {
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        let l = lead(&rd);
        rn.advance(rd);
        if l == Some(rn.raft.id) {
            break;
        }
    }
    let propose_conf_change_and_apply = |rn: &mut TestRawNode, cc: &ConfChange| {
        let _ = rn.propose_conf_change(cc.clone());
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        for entry in &rd.committed_entries {
            if entry.entry_type == EntryType::EntryConfChange {
                let cc = ConfChange::decode(&entry.data).unwrap();
                rn.apply_conf_change(&ConfChangeI::V1(cc));
            }
        }
        rn.advance(rd);
    };
    let cc1 = ConfChange {
        change_type: ConfChangeType::ConfChangeAddNode,
        node_id: 1,
        ..Default::default()
    };
    let ccdata1 = cc1.encode();
    propose_conf_change_and_apply(&mut rn, &cc1);
    propose_conf_change_and_apply(&mut rn, &cc1);
    let cc2 = ConfChange {
        change_type: ConfChangeType::ConfChangeAddNode,
        node_id: 2,
        ..Default::default()
    };
    let ccdata2 = cc2.encode();
    propose_conf_change_and_apply(&mut rn, &cc2);
    let last_index = rn.store().last_index().unwrap();
    let entries = rn
        .store()
        .entries(last_index - 2, last_index + 1, NO_LIMIT)
        .unwrap();
    assert_eq!(3, entries.len());
    assert_eq!(ccdata1, entries[0].data);
    assert_eq!(ccdata2, entries[2].data);
}

thread_local! {
    static READ_INDEX_MSGS: RefCell<Vec<Message>> = const { RefCell::new(Vec::new()) };
}

fn append_step(_: &mut TestRaft, m: Message) -> Result<(), Error> {
    READ_INDEX_MSGS.with(|v| v.borrow_mut().push(m));
    Ok(())
}

#[test]
fn test_raw_node_read_index() {
    READ_INDEX_MSGS.with(|v| v.borrow_mut().clear());
    let wrs = vec![ReadState {
        index: 1,
        request_ctx: b"somedata".to_vec(),
    }];
    let mut rn = new_raw_node(with_peers(&[1]));
    rn.raft.read_states = wrs.clone();
    assert!(rn.has_ready());
    let rd = rn.ready();
    assert_eq!(wrs, rd.read_states);
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    assert!(rn.raft.read_states.is_empty());

    let wrequest_ctx = b"somedata2".to_vec();
    rn.campaign().unwrap();
    loop {
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        let l = lead(&rd);
        rn.advance(rd);
        if l == Some(rn.raft.id) {
            rn.raft.set_step(append_step);
            rn.read_index(wrequest_ctx.clone());
            break;
        }
    }
    let msgs = READ_INDEX_MSGS.with(|v| v.borrow().clone());
    assert_eq!(1, msgs.len());
    assert_eq!(MessageType::MsgReadIndex, msgs[0].msg_type);
    assert_eq!(wrequest_ctx, msgs[0].entries[0].data);
}

#[test]
fn test_raw_node_start() {
    let entries = vec![
        Entry {
            term: 1,
            index: 2,
            ..Default::default()
        },
        Entry {
            term: 1,
            index: 3,
            data: b"foo".to_vec(),
            ..Default::default()
        },
    ];
    let want = Ready {
        hard_state: Some(HardState {
            term: 1,
            commit: 3,
            vote: 1,
        }),
        committed_entries: entries.clone(),
        must_sync: false,
        ..Default::default()
    };
    let mut storage = MemoryStorage::new();
    storage.ents[0].index = 1;
    // bootstrap
    let fi = storage.first_index().unwrap();
    assert!(fi >= 2, "FirstIndex >= 2 is prerequisite for bootstrap");
    assert!(
        storage.entries(fi, fi, u64::MAX).is_err(),
        "should not have been able to load first index"
    );
    let li = storage.last_index().unwrap();
    assert!(
        storage.entries(li, li, u64::MAX).is_err(),
        "should not have been able to load last index"
    );
    let (hs, ics) = storage.initial_state().unwrap();
    assert!(crate::is_empty_hard_state(Some(&hs)));
    assert!(ics.voters.is_empty());
    storage
        .apply_snapshot(Snapshot {
            metadata: SnapshotMetadata {
                index: 1,
                term: 0,
                conf_state: ConfState {
                    voters: vec![1],
                    ..Default::default()
                },
            },
            ..Default::default()
        })
        .unwrap();

    let mut rn = new_raw_node(storage);
    assert!(!rn.has_ready());
    rn.campaign().unwrap();
    let rd = rn.ready();
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    rn.propose(b"foo".to_vec()).unwrap();
    assert!(rn.has_ready());
    let rd = rn.ready();
    assert_eq!(entries, rd.entries);
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    assert!(rn.has_ready());
    let mut rd = rn.ready();
    assert!(rd.entries.is_empty());
    assert!(!rd.must_sync);
    rn.advance(rd.clone());
    rd.soft_state = None;
    assert_eq!(want, rd);
    assert!(!rn.has_ready());
}

#[test]
fn test_raw_node_restart() {
    let entries = vec![
        Entry {
            term: 1,
            index: 1,
            ..Default::default()
        },
        Entry {
            term: 1,
            index: 2,
            data: b"foo".to_vec(),
            ..Default::default()
        },
    ];
    let st = HardState {
        term: 1,
        commit: 1,
        ..Default::default()
    };
    let want = Ready {
        committed_entries: entries[..1].to_vec(),
        must_sync: false,
        ..Default::default()
    };
    let mut storage = with_peers(&[1]);
    storage.set_hard_state(st).unwrap();
    storage.append(&entries).unwrap();
    let mut rn = new_raw_node(storage);
    let rd = rn.ready();
    assert_eq!(want, rd);
    rn.advance(rd);
    assert!(!rn.has_ready());
}

#[test]
fn test_raw_node_restart_from_snapshot() {
    let snap = Snapshot {
        metadata: SnapshotMetadata {
            conf_state: ConfState {
                voters: vec![1, 2],
                ..Default::default()
            },
            index: 2,
            term: 1,
        },
        ..Default::default()
    };
    let entries = vec![Entry {
        term: 1,
        index: 3,
        data: b"foo".to_vec(),
        ..Default::default()
    }];
    let st = HardState {
        term: 1,
        commit: 3,
        ..Default::default()
    };
    let want = Ready {
        committed_entries: entries.clone(),
        must_sync: false,
        ..Default::default()
    };
    let mut s = MemoryStorage::new();
    s.set_hard_state(st).unwrap();
    s.apply_snapshot(snap).unwrap();
    s.append(&entries).unwrap();
    let mut rn = new_raw_node(s);
    let rd = rn.ready();
    assert_eq!(want, rd);
    rn.advance(rd);
    assert!(!rn.has_ready());
}

#[test]
fn test_raw_node_status() {
    let mut rn = new_raw_node(with_peers(&[1]));
    assert!(rn.status().progress.is_empty());
    rn.campaign().unwrap();
    let rd = rn.ready();
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    let status = rn.status();
    assert_eq!(1, status.basic.soft_state.lead);
    assert_eq!(StateType::StateLeader, status.basic.soft_state.raft_state);
    assert_eq!(rn.raft.trk.progress[&1], status.progress[&1]);
    let mut exp_cfg = tracker::Config::default();
    exp_cfg.voters.0[0].insert(1);
    assert_eq!(exp_cfg, status.config);
}

struct IgnoreSizeHintMemStorage(MemoryStorage);

impl Storage for IgnoreSizeHintMemStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState), Error> {
        self.0.initial_state()
    }
    fn entries(&self, lo: u64, hi: u64, _: u64) -> Result<Vec<Entry>, Error> {
        self.0.entries(lo, hi, u64::MAX)
    }
    fn term(&self, i: u64) -> Result<u64, Error> {
        self.0.term(i)
    }
    fn last_index(&self) -> Result<u64, Error> {
        self.0.last_index()
    }
    fn first_index(&self) -> Result<u64, Error> {
        self.0.first_index()
    }
    fn snapshot(&self) -> Result<Snapshot, Error> {
        self.0.snapshot()
    }
}

#[test]
fn test_raw_node_commit_pagination_after_restart() {
    let mut s = with_peers(&[1]);
    s.hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 10,
    };
    s.ents = (0..10)
        .map(|i| Entry {
            term: 1,
            index: i + 1,
            entry_type: EntryType::EntryNormal,
            data: b"a".to_vec(),
        })
        .collect();
    let size: u64 = s.ents.iter().map(entry_size).sum();
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_size_per_msg = size - entry_size(s.ents.last().unwrap()) - 1;
    s.ents.push(Entry {
        term: 1,
        index: 11,
        entry_type: EntryType::EntryNormal,
        data: b"boom".to_vec(),
    });
    let mut rn = RawNode::new(&mut cfg, IgnoreSizeHintMemStorage(s)).unwrap();
    let mut highest_applied = 0;
    while highest_applied != 11 {
        let rd = rn.ready();
        let n = rd.committed_entries.len();
        assert_ne!(0, n, "stopped applying entries at index {highest_applied}");
        let next = rd.committed_entries[0].index;
        assert!(
            !(highest_applied != 0 && highest_applied + 1 != next),
            "attempting to apply index {next} after index {highest_applied}, leaving a gap"
        );
        highest_applied = rd.committed_entries[n - 1].index;
        rn.advance(rd);
        let _ = rn.step(Message {
            msg_type: MessageType::MsgHeartbeat,
            to: 1,
            from: 2,
            term: 1,
            commit: 11,
            ..Default::default()
        });
    }
}

#[test]
fn test_raw_node_bounded_log_growth_with_partition() {
    const MAX_ENTRIES: u64 = 16;
    let data = b"testdata".to_vec();
    let test_entry = Entry {
        data: data.clone(),
        ..Default::default()
    };
    let max_entry_size = MAX_ENTRIES * payload_size(&test_entry);
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_uncommitted_entries_size = max_entry_size;
    let mut rn = RawNode::new(&mut cfg, with_peers(&[1])).unwrap();
    rn.campaign().unwrap();
    loop {
        let rd = rn.ready();
        rn.store_mut().append(&rd.entries).unwrap();
        let done = !rd.committed_entries.is_empty();
        rn.advance(rd);
        if done {
            break;
        }
    }
    for _ in 0..1024 {
        let _ = rn.propose(data.clone());
    }
    assert_eq!(max_entry_size, rn.raft.uncommitted_size);
    let rd = rn.ready();
    assert_eq!(MAX_ENTRIES as usize, rd.entries.len());
    rn.store_mut().append(&rd.entries).unwrap();
    rn.advance(rd);
    assert_eq!(max_entry_size, rn.raft.uncommitted_size);
    let rd = rn.ready();
    assert!(rd.entries.is_empty());
    assert_eq!(MAX_ENTRIES as usize, rd.committed_entries.len());
    rn.advance(rd);
    assert_eq!(0, rn.raft.uncommitted_size);
}

#[test]
fn test_raw_node_consume_ready() {
    // The readyWithoutAccept half of the Go test is not ported: the port has no
    // separate readyWithoutAccept (it is merged into Ready()).
    let mut rn = RawNode::new(&mut new_test_config(1, 3, 1), with_peers(&[1])).unwrap();
    let m1 = Message {
        context: b"foo".to_vec(),
        ..Default::default()
    };
    let m2 = Message {
        context: b"bar".to_vec(),
        ..Default::default()
    };
    rn.raft.msgs.push(m1.clone());
    let rd = rn.ready();
    assert!(rn.raft.msgs.is_empty());
    assert_eq!(vec![m1], rd.messages);
    rn.raft.msgs.push(m2.clone());
    rn.advance(rd);
    assert_eq!(vec![m2], rn.raft.msgs);
}
