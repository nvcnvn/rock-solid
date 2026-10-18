// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// raft_test.go. Licensed under the Apache License 2.0.
//
// One Rust test per Go Test function, same names in snake case. Rafts placed
// into a Network are reached through `nt.raft(id)` (Go keeps aliasing pointers).

use std::cell::RefCell;
use std::rc::Rc;

use crate::Error;
use crate::log::NO_LIMIT;
use crate::raft::{NONE, ReadOnlyOption, StateType, step_candidate, step_follower, step_leader};
use crate::raftpb::{
    ConfChange, ConfChangeSingle, ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType,
    HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};
use crate::storage::MemoryStorage;
use crate::testutil::*;
use crate::tracker::StateType as PrState;
use crate::util::{payload_size, payloads_size, vote_resp_msg_type};

fn m(from: u64, to: u64, t: MessageType) -> Message {
    Message {
        from,
        to,
        msg_type: t,
        ..Default::default()
    }
}

fn hup(id: u64) -> Message {
    m(id, id, MessageType::MsgHup)
}

fn beat(id: u64) -> Message {
    m(id, id, MessageType::MsgBeat)
}

fn prop(from: u64, to: u64, data: &[u8]) -> Message {
    Message {
        from,
        to,
        msg_type: MessageType::MsgProp,
        entries: vec![Entry {
            data: data.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn prop_empty(id: u64) -> Message {
    Message {
        from: id,
        to: id,
        msg_type: MessageType::MsgProp,
        entries: vec![Entry::default()],
        ..Default::default()
    }
}

fn cc_v1(t: ConfChangeType, node_id: u64) -> ConfChangeV2 {
    ConfChange {
        change_type: t,
        node_id,
        ..Default::default()
    }
    .as_v2()
}

fn snap(index: u64, term: u64, voters: &[u64], learners: &[u64]) -> Snapshot {
    Snapshot {
        data: Vec::new(),
        metadata: SnapshotMetadata {
            index,
            term,
            conf_state: ConfState {
                voters: voters.to_vec(),
                learners: learners.to_vec(),
                ..Default::default()
            },
        },
    }
}

fn must_term(r: Result<u64, Error>) -> u64 {
    r.expect("term")
}

fn pre_vote_cfg(pre_vote: bool) -> Option<&'static dyn Fn(&mut crate::raft::Config)> {
    if pre_vote {
        Some(&pre_vote_config)
    } else {
        None
    }
}

#[test]
fn test_progress_leader() {
    let s = with_peers(&[1, 2]);
    let mut r = new_test_raft(1, 5, 1, s);
    r.become_candidate();
    r.become_leader();
    r.trk.progress.get_mut(&2).unwrap().become_replicate();

    let prop_msg = prop(1, 1, b"foo");
    for i in 0..5 {
        assert!(r.step(prop_msg.clone()).is_ok(), "#{i}");
    }
    assert_eq!(0, r.trk.progress[&1].match_);
    let ents = r.raft_log.next_unstable_ents();
    assert_eq!(ents.len(), 6);
    assert!(ents[0].data.is_empty());
    assert_eq!(b"foo", &ents[5].data[..]);

    r.advance_messages_after_append();
    assert_eq!(6, r.trk.progress[&1].match_);
    assert_eq!(7, r.trk.progress[&1].next);
}

#[test]
fn test_progress_resume_by_heartbeat_resp() {
    let mut r = new_test_raft(1, 5, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    r.trk.progress.get_mut(&2).unwrap().msg_app_flow_paused = true;

    let _ = r.step(beat(1));
    assert!(r.trk.progress[&2].msg_app_flow_paused);

    r.trk.progress.get_mut(&2).unwrap().become_replicate();
    assert!(!r.trk.progress[&2].msg_app_flow_paused);
    r.trk.progress.get_mut(&2).unwrap().msg_app_flow_paused = true;
    let _ = r.step(m(2, 1, MessageType::MsgHeartbeatResp));
    assert!(!r.trk.progress[&2].msg_app_flow_paused);
}

#[test]
fn test_progress_paused() {
    let mut r = new_test_raft(1, 5, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    for _ in 0..3 {
        let _ = r.step(prop(1, 1, b"somedata"));
    }
    let ms = r.read_messages();
    assert_eq!(ms.len(), 1);
}

#[test]
fn test_progress_flow_control() {
    let mut cfg = new_test_config(1, 5, 1);
    cfg.max_inflight_msgs = 3;
    cfg.max_size_per_msg = 2048;
    cfg.max_inflight_bytes = 9000;
    let mut r = crate::raft::Raft::new(&mut cfg, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    r.read_messages();

    r.trk.progress.get_mut(&2).unwrap().become_probe();
    let blob = vec![b'a'; 1000];
    let large = vec![b'b'; 5000];
    for i in 0..22 {
        let b = if (10..16).contains(&i) { &large } else { &blob };
        let _ = r.step(prop(1, 1, b));
    }

    let ms = r.read_messages();
    assert_eq!(ms.len(), 1);
    assert_eq!(MessageType::MsgApp, ms[0].msg_type);
    assert_eq!(ms[0].entries.len(), 2);
    assert!(ms[0].entries[0].data.is_empty());
    assert_eq!(ms[0].entries[1].data.len(), 1000);

    let mut ack_and_verify = |index: u64, exp_entries: &[usize]| -> u64 {
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgAppResp,
            index,
            ..Default::default()
        });
        let ms = r.read_messages();
        assert_eq!(exp_entries.len(), ms.len());
        for (i, m) in ms.iter().enumerate() {
            assert_eq!(MessageType::MsgApp, m.msg_type, "#{i}");
            assert_eq!(m.entries.len(), exp_entries[i], "#{i}");
        }
        match ms.last().unwrap().entries.last() {
            None => index,
            Some(e) => e.index,
        }
    };

    let index = ack_and_verify(ms[0].entries[1].index, &[2, 2, 2]);
    let index = ack_and_verify(index, &[2, 1, 1]);
    let index = ack_and_verify(index, &[1, 1]);
    let index = ack_and_verify(index, &[1, 1]);
    let index = ack_and_verify(index, &[1, 2, 2]);
    ack_and_verify(index, &[2]);
}

#[test]
fn test_uncommitted_entry_limit() {
    const MAX_ENTRIES: usize = 1024;
    let test_entry = Entry {
        data: b"testdata".to_vec(),
        ..Default::default()
    };
    let max_entry_size = MAX_ENTRIES as u64 * payload_size(&test_entry);
    assert_eq!(0, payload_size(&Entry::default()));

    let mut cfg = new_test_config(1, 5, 1);
    cfg.max_uncommitted_entries_size = max_entry_size;
    cfg.max_inflight_msgs = 2 * 1024;
    let mut r = crate::raft::Raft::new(&mut cfg, with_peers(&[1, 2, 3]));
    r.become_candidate();
    r.become_leader();
    assert_eq!(0, r.uncommitted_size);

    const NUM_FOLLOWERS: usize = 2;
    r.trk.progress.get_mut(&2).unwrap().become_replicate();
    r.trk.progress.get_mut(&3).unwrap().become_replicate();
    r.uncommitted_size = 0;

    let prop_msg = Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgProp,
        entries: vec![test_entry.clone()],
        ..Default::default()
    };
    let mut prop_ents = Vec::new();
    for i in 0..MAX_ENTRIES {
        assert!(r.step(prop_msg.clone()).is_ok(), "#{i}");
        prop_ents.push(test_entry.clone());
    }
    assert_eq!(Err(Error::ProposalDropped), r.step(prop_msg.clone()));

    let ms = r.read_messages();
    assert_eq!(ms.len(), MAX_ENTRIES * NUM_FOLLOWERS);
    r.reduce_uncommitted_size(payloads_size(&prop_ents));
    assert_eq!(0, r.uncommitted_size);

    let prop_ents = vec![test_entry.clone(); 2 * MAX_ENTRIES];
    let prop_msg_large = Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgProp,
        entries: prop_ents.clone(),
        ..Default::default()
    };
    assert!(r.step(prop_msg_large).is_ok());
    assert_eq!(Err(Error::ProposalDropped), r.step(prop_msg.clone()));
    assert!(r.step(prop_empty(1)).is_ok());

    let ms = r.read_messages();
    assert_eq!(ms.len(), 2 * NUM_FOLLOWERS);
    r.reduce_uncommitted_size(payloads_size(&prop_ents));
    assert_eq!(0, r.uncommitted_size);
}

#[test]
fn test_leader_election() {
    test_leader_election_impl(false);
}

#[test]
fn test_leader_election_pre_vote() {
    test_leader_election_impl(true);
}

fn test_leader_election_impl(pre_vote: bool) {
    let cfg = pre_vote_cfg(pre_vote);
    let (cand_state, cand_term) = if pre_vote {
        (StateType::StatePreCandidate, 0)
    } else {
        (StateType::StateCandidate, 1)
    };
    let tests: Vec<(Network, StateType, u64)> = vec![
        (
            new_network_with_config(cfg, vec![None, None, None]),
            StateType::StateLeader,
            1,
        ),
        (
            new_network_with_config(cfg, vec![None, None, black_hole()]),
            StateType::StateLeader,
            1,
        ),
        (
            new_network_with_config(cfg, vec![None, black_hole(), black_hole()]),
            cand_state,
            cand_term,
        ),
        (
            new_network_with_config(cfg, vec![None, black_hole(), black_hole(), None]),
            cand_state,
            cand_term,
        ),
        (
            new_network_with_config(cfg, vec![None, black_hole(), black_hole(), None, None]),
            StateType::StateLeader,
            1,
        ),
        (
            new_network_with_config(
                cfg,
                vec![
                    None,
                    raft_sm(ents_with_config(cfg, &[1])),
                    raft_sm(ents_with_config(cfg, &[1])),
                    raft_sm(ents_with_config(cfg, &[1, 1])),
                    None,
                ],
            ),
            StateType::StateFollower,
            1,
        ),
    ];
    for (i, (mut nt, state, exp_term)) in tests.into_iter().enumerate() {
        nt.send(vec![hup(1)]);
        let sm = nt.raft(1);
        assert_eq!(state, sm.state, "#{i}");
        assert_eq!(exp_term, sm.term, "#{i}");
    }
}

#[test]
fn test_learner_election_timeout() {
    let mut n1 = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    let mut n2 = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n2.randomized_election_timeout = n2.election_timeout;
    for _ in 0..n2.election_timeout {
        n2.tick();
    }
    assert_eq!(StateType::StateFollower, n2.state);
}

#[test]
fn test_learner_promotion() {
    let mut n1 = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    let mut n2 = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    let mut nt = new_network(vec![raft_sm(n1), raft_sm(n2)]);
    assert_ne!(StateType::StateLeader, nt.raft(1).state);

    let n1 = nt.raft(1);
    n1.randomized_election_timeout = n1.election_timeout;
    for _ in 0..n1.election_timeout {
        n1.tick();
    }
    n1.advance_messages_after_append();
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    nt.send(vec![beat(1)]);

    nt.raft(1)
        .apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 2));
    nt.raft(2)
        .apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 2));
    assert!(!nt.raft(2).is_learner);

    let n2 = nt.raft(2);
    n2.randomized_election_timeout = n2.election_timeout;
    for _ in 0..n2.election_timeout {
        n2.tick();
    }
    n2.advance_messages_after_append();
    nt.send(vec![beat(2)]);

    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    assert_eq!(StateType::StateLeader, nt.raft(2).state);
}

#[test]
fn test_learner_can_vote() {
    let mut n2 = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    n2.become_follower(1, NONE);
    let _ = n2.step(Message {
        from: 1,
        to: 2,
        term: 2,
        msg_type: MessageType::MsgVote,
        log_term: 11,
        index: 11,
        ..Default::default()
    });
    let msgs = n2.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].msg_type, MessageType::MsgVoteResp);
    assert!(!msgs[0].reject, "expected learner to not reject vote");
}

#[test]
fn test_leader_cycle() {
    test_leader_cycle_impl(false);
}

#[test]
fn test_leader_cycle_pre_vote() {
    test_leader_cycle_impl(true);
}

fn test_leader_cycle_impl(pre_vote: bool) {
    let mut n = new_network_with_config(pre_vote_cfg(pre_vote), vec![None, None, None]);
    for campaigner_id in 1..=3u64 {
        n.send(vec![hup(campaigner_id)]);
        for id in 1..=3u64 {
            let sm = n.raft(id);
            if sm.id == campaigner_id {
                assert_eq!(
                    StateType::StateLeader,
                    sm.state,
                    "preVote={pre_vote}: campaigning node {}",
                    sm.id
                );
            } else {
                assert_eq!(
                    StateType::StateFollower,
                    sm.state,
                    "preVote={pre_vote}: campaigning node {campaigner_id}, current node {}",
                    sm.id
                );
            }
        }
    }
}

#[test]
fn test_leader_election_overwrite_newer_logs() {
    test_leader_election_overwrite_newer_logs_impl(false);
}

#[test]
fn test_leader_election_overwrite_newer_logs_pre_vote() {
    test_leader_election_overwrite_newer_logs_impl(true);
}

fn test_leader_election_overwrite_newer_logs_impl(pre_vote: bool) {
    let cfg = pre_vote_cfg(pre_vote);
    let mut n = new_network_with_config(
        cfg,
        vec![
            raft_sm(ents_with_config(cfg, &[1])),
            raft_sm(ents_with_config(cfg, &[1])),
            raft_sm(ents_with_config(cfg, &[2])),
            raft_sm(voted_with_config(cfg, 3, 2)),
            raft_sm(voted_with_config(cfg, 3, 2)),
        ],
    );
    n.send(vec![hup(1)]);
    assert_eq!(StateType::StateFollower, n.raft(1).state);
    assert_eq!(2, n.raft(1).term);

    n.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, n.raft(1).state);
    assert_eq!(3, n.raft(1).term);

    for id in 1..=5u64 {
        let entries = n.raft(id).raft_log.all_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(1, entries[0].term);
        assert_eq!(3, entries[1].term);
    }
}

#[test]
fn test_vote_from_any_state() {
    test_vote_from_any_state_impl(MessageType::MsgVote);
}

#[test]
fn test_pre_vote_from_any_state() {
    test_vote_from_any_state_impl(MessageType::MsgPreVote);
}

fn test_vote_from_any_state_impl(vt: MessageType) {
    for st in [
        StateType::StateFollower,
        StateType::StateCandidate,
        StateType::StateLeader,
        StateType::StatePreCandidate,
    ] {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        r.term = 1;
        match st {
            StateType::StateFollower => r.become_follower(r.term, 3),
            StateType::StatePreCandidate => r.become_pre_candidate(),
            StateType::StateCandidate => r.become_candidate(),
            StateType::StateLeader => {
                r.become_candidate();
                r.become_leader();
            }
        }
        let orig_term = r.term;
        let new_term = r.term + 1;
        let msg = Message {
            from: 2,
            to: 1,
            msg_type: vt,
            term: new_term,
            log_term: new_term,
            index: 42,
            ..Default::default()
        };
        assert!(r.step(msg).is_ok(), "{vt},{st}");
        let msgs = r.read_messages();
        assert_eq!(msgs.len(), 1, "{vt},{st}");
        assert_eq!(vote_resp_msg_type(vt), msgs[0].msg_type, "{vt},{st}");
        assert!(!msgs[0].reject, "{vt},{st}");

        if vt == MessageType::MsgVote {
            assert_eq!(StateType::StateFollower, r.state, "{vt},{st}");
            assert_eq!(new_term, r.term, "{vt},{st}");
            assert_eq!(2, r.vote, "{vt},{st}");
        } else {
            assert_eq!(st, r.state, "{vt},{st}");
            assert_eq!(orig_term, r.term, "{vt},{st}");
            assert!(r.vote == NONE || r.vote == 1, "{vt},{st}: vote {}", r.vote);
        }
    }
}

#[test]
fn test_log_replication() {
    let tests: Vec<(Network, Vec<Message>, u64)> = vec![
        (
            new_network(vec![None, None, None]),
            vec![prop(1, 1, b"somedata")],
            2,
        ),
        (
            new_network(vec![None, None, None]),
            vec![
                prop(1, 1, b"somedata"),
                m(1, 2, MessageType::MsgHup),
                prop(1, 2, b"somedata"),
            ],
            4,
        ),
    ];
    for (i, (mut nt, msgs, wcommitted)) in tests.into_iter().enumerate() {
        nt.send(vec![hup(1)]);
        for msg in &msgs {
            nt.send(vec![msg.clone()]);
        }
        for j in 1..=3u64 {
            let sm = nt.raft(j);
            assert_eq!(wcommitted, sm.raft_log.committed, "#{i}.{j}");
            let ents: Vec<Entry> = next_ents(sm)
                .into_iter()
                .filter(|e| !e.data.is_empty())
                .collect();
            let props: Vec<&Message> = msgs
                .iter()
                .filter(|m| m.msg_type == MessageType::MsgProp)
                .collect();
            for (k, mm) in props.iter().enumerate() {
                assert_eq!(mm.entries[0].data, ents[k].data, "#{i}.{j}");
            }
        }
    }
}

#[test]
fn test_learner_log_replication() {
    let n1 = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    let n2 = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    let mut nt = new_network(vec![raft_sm(n1), raft_sm(n2)]);
    nt.raft(1).become_follower(1, NONE);
    nt.raft(2).become_follower(1, NONE);

    let n1 = nt.raft(1);
    n1.randomized_election_timeout = n1.election_timeout;
    for _ in 0..n1.election_timeout {
        n1.tick();
    }
    n1.advance_messages_after_append();
    nt.send(vec![beat(1)]);

    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert!(nt.raft(2).is_learner);

    let next_committed = 2;
    nt.send(vec![prop(1, 1, b"somedata")]);
    assert_eq!(next_committed, nt.raft(1).raft_log.committed);
    let c1 = nt.raft(1).raft_log.committed;
    assert_eq!(c1, nt.raft(2).raft_log.committed);
    let match_ = nt.raft(1).trk.progress[&2].match_;
    assert_eq!(nt.raft(2).raft_log.committed, match_);
}

#[test]
fn test_single_node_commit() {
    let r = new_test_raft(1, 10, 1, with_peers(&[1]));
    let mut tt = new_network(vec![raft_sm(r)]);
    tt.send(vec![hup(1)]);
    tt.send(vec![prop(1, 1, b"some data")]);
    tt.send(vec![prop(1, 1, b"some data")]);
    assert_eq!(3, tt.raft(1).raft_log.committed);
}

#[test]
fn test_cannot_commit_without_new_term_entry() {
    let mut tt = new_network(vec![None, None, None, None, None]);
    tt.send(vec![hup(1)]);
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);
    tt.send(vec![prop(1, 1, b"some data")]);
    tt.send(vec![prop(1, 1, b"some data")]);
    assert_eq!(1, tt.raft(1).raft_log.committed);

    tt.recover();
    tt.ignore(MessageType::MsgApp);
    tt.send(vec![hup(2)]);
    assert_eq!(1, tt.raft(2).raft_log.committed);

    tt.recover();
    tt.send(vec![beat(2)]);
    tt.send(vec![prop(2, 2, b"some data")]);
    assert_eq!(5, tt.raft(2).raft_log.committed);
}

#[test]
fn test_commit_without_new_term_entry() {
    let mut tt = new_network(vec![None, None, None, None, None]);
    tt.send(vec![hup(1)]);
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);
    tt.send(vec![prop(1, 1, b"some data")]);
    tt.send(vec![prop(1, 1, b"some data")]);
    assert_eq!(1, tt.raft(1).raft_log.committed);

    tt.recover();
    tt.send(vec![hup(2)]);
    assert_eq!(4, tt.raft(1).raft_log.committed);
}

#[test]
fn test_dueling_candidates() {
    let a = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    let b = new_test_raft(2, 10, 1, with_peers(&[1, 2, 3]));
    let c = new_test_raft(3, 10, 1, with_peers(&[1, 2, 3]));
    let mut nt = new_network(vec![raft_sm(a), raft_sm(b), raft_sm(c)]);
    nt.cut(1, 3);
    nt.send(vec![hup(1)]);
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);

    nt.recover();
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    let tests = [
        (1, StateType::StateFollower, 2, 1),
        (2, StateType::StateFollower, 2, 1),
        (3, StateType::StateFollower, 2, 0),
    ];
    for (i, (id, state, term, last_index)) in tests.into_iter().enumerate() {
        let sm = nt.raft(id);
        assert_eq!(state, sm.state, "#{i}");
        assert_eq!(term, sm.term, "#{i}");
        assert_eq!(last_index, sm.raft_log.last_index(), "#{i}");
    }
}

#[test]
fn test_dueling_pre_candidates() {
    let mk = |id| {
        let mut cfg = new_test_config(id, 10, 1);
        cfg.pre_vote = true;
        crate::raft::Raft::new(&mut cfg, with_peers(&[1, 2, 3]))
    };
    let mut nt = new_network(vec![raft_sm(mk(1)), raft_sm(mk(2)), raft_sm(mk(3))]);
    nt.cut(1, 3);
    nt.send(vec![hup(1)]);
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    nt.recover();
    nt.send(vec![hup(3)]);
    let tests = [
        (1, StateType::StateLeader, 1, 1),
        (2, StateType::StateFollower, 1, 1),
        (3, StateType::StateFollower, 1, 0),
    ];
    for (i, (id, state, term, last_index)) in tests.into_iter().enumerate() {
        let sm = nt.raft(id);
        assert_eq!(state, sm.state, "#{i}");
        assert_eq!(term, sm.term, "#{i}");
        assert_eq!(last_index, sm.raft_log.last_index(), "#{i}");
    }
}

fn want_log_two_entries(data: &[u8]) -> String {
    ltoa(&log_from_ents(&[
        Entry::default(),
        Entry {
            term: 1,
            index: 1,
            ..Default::default()
        },
        Entry {
            term: 1,
            index: 2,
            data: data.to_vec(),
            ..Default::default()
        },
    ]))
}

#[test]
fn test_candidate_concede() {
    let mut tt = new_network(vec![None, None, None]);
    tt.isolate(1);
    tt.send(vec![hup(1)]);
    tt.send(vec![hup(3)]);
    tt.recover();
    tt.send(vec![beat(3)]);
    let data = b"force follower";
    tt.send(vec![prop(3, 3, data)]);
    tt.send(vec![beat(3)]);

    assert_eq!(StateType::StateFollower, tt.raft(1).state);
    assert_eq!(1, tt.raft(1).term);

    let want_log = want_log_two_entries(data);
    for id in 1..=3u64 {
        let l = ltoa(&tt.raft(id).raft_log);
        assert_eq!(want_log, l, "#{id}");
    }
}

#[test]
fn test_single_node_candidate() {
    let mut tt = new_network(vec![None]);
    tt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, tt.raft(1).state);
}

#[test]
fn test_single_node_pre_candidate() {
    let mut tt = new_network_with_config(Some(&pre_vote_config), vec![None]);
    tt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, tt.raft(1).state);
}

#[test]
fn test_old_messages() {
    let mut tt = new_network(vec![None, None, None]);
    tt.send(vec![hup(1)]);
    tt.send(vec![hup(2)]);
    tt.send(vec![hup(1)]);
    tt.send(vec![Message {
        from: 2,
        to: 1,
        msg_type: MessageType::MsgApp,
        term: 2,
        entries: index_terms(3, &[2]),
        ..Default::default()
    }]);
    tt.send(vec![prop(1, 1, b"somedata")]);

    let mut ents = index_terms(0, &[0, 1, 2, 3, 3]);
    ents[4].data = b"somedata".to_vec();
    let base = ltoa(&log_from_ents(&ents));
    for id in 1..=3u64 {
        let l = ltoa(&tt.raft(id).raft_log);
        assert_eq!(base, l, "#{id}");
    }
}

#[test]
fn test_proposal() {
    let tests: Vec<(Network, bool)> = vec![
        (new_network(vec![None, None, None]), true),
        (new_network(vec![None, None, black_hole()]), true),
        (new_network(vec![None, black_hole(), black_hole()]), false),
        (
            new_network(vec![None, black_hole(), black_hole(), None]),
            false,
        ),
        (
            new_network(vec![None, black_hole(), black_hole(), None, None]),
            true,
        ),
    ];
    for (j, (mut tt, success)) in tests.into_iter().enumerate() {
        let data = b"somedata";
        tt.send(vec![hup(1)]);
        tt.send(vec![prop(1, 1, data)]);

        let base = if success {
            want_log_two_entries(data)
        } else {
            ltoa(&log_from_ents(&[Entry::default()]))
        };
        let ids: Vec<u64> = tt.peers.keys().copied().collect();
        for id in ids {
            if let StateMachine::Raft(sm) = tt.peers.get_mut(&id).unwrap() {
                let l = ltoa(&sm.raft_log);
                assert_eq!(base, l, "#{j}, peer {id}");
            }
        }
        assert_eq!(1, tt.raft(1).term, "#{j}");
    }
}

#[test]
fn test_proposal_by_proxy() {
    let data = b"somedata";
    let tests = vec![
        new_network(vec![None, None, None]),
        new_network(vec![None, None, black_hole()]),
    ];
    for (j, mut tt) in tests.into_iter().enumerate() {
        tt.send(vec![hup(1)]);
        tt.send(vec![prop(2, 2, data)]);
        let base = want_log_two_entries(data);
        let ids: Vec<u64> = tt.peers.keys().copied().collect();
        for id in ids {
            if let StateMachine::Raft(sm) = tt.peers.get_mut(&id).unwrap() {
                assert_eq!(base, ltoa(&sm.raft_log), "#{j}.{id}");
            }
        }
        assert_eq!(1, tt.raft(1).term, "#{j}");
    }
}

#[test]
fn test_commit() {
    let tests: Vec<(Vec<u64>, Vec<Entry>, u64, u64)> = vec![
        (vec![1], index_terms(1, &[1]), 1, 1),
        (vec![1], index_terms(1, &[1]), 2, 0),
        (vec![2], index_terms(1, &[1, 2]), 2, 2),
        (vec![1], index_terms(1, &[2]), 2, 1),
        (vec![2, 1, 1], index_terms(1, &[1, 2]), 1, 1),
        (vec![2, 1, 1], index_terms(1, &[1, 1]), 2, 0),
        (vec![2, 1, 2], index_terms(1, &[1, 2]), 2, 2),
        (vec![2, 1, 2], index_terms(1, &[1, 1]), 2, 0),
        (vec![2, 1, 1, 1], index_terms(1, &[1, 2]), 1, 1),
        (vec![2, 1, 1, 1], index_terms(1, &[1, 1]), 2, 0),
        (vec![2, 1, 1, 2], index_terms(1, &[1, 2]), 1, 1),
        (vec![2, 1, 1, 2], index_terms(1, &[1, 1]), 2, 0),
        (vec![2, 1, 2, 2], index_terms(1, &[1, 2]), 2, 2),
        (vec![2, 1, 2, 2], index_terms(1, &[1, 1]), 2, 0),
    ];
    for (i, (matches, logs, sm_term, w)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1]);
        storage.append(&logs).unwrap();
        storage
            .set_hard_state(HardState {
                term: sm_term,
                ..Default::default()
            })
            .unwrap();
        let mut sm = new_test_raft(1, 10, 2, storage);
        for (j, &mt) in matches.iter().enumerate() {
            let id = j as u64 + 1;
            if id > 1 {
                sm.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, id));
            }
            let pr = sm.trk.progress.get_mut(&id).unwrap();
            pr.match_ = mt;
            pr.next = mt + 1;
        }
        sm.maybe_commit();
        assert_eq!(w, sm.raft_log.committed, "#{i}");
    }
}

#[test]
fn test_past_election_timeout() {
    let tests = [
        (5, 0.0, false),
        (10, 0.1, true),
        (13, 0.4, true),
        (15, 0.6, true),
        (18, 0.9, true),
        (20, 1.0, false),
    ];
    for (i, (elapse, wprobability, round)) in tests.into_iter().enumerate() {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10000 {
            sm.reset_randomized_election_timeout();
            if sm.past_election_timeout() {
                c += 1;
            }
        }
        let mut got = c as f64 / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        assert_eq!(wprobability, got, "#{i}");
    }
}

thread_local! {
    static CALLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn fake_step(_: &mut TestRaft, _: Message) -> Result<(), Error> {
    CALLED.with(|c| c.set(true));
    Ok(())
}

#[test]
fn test_step_ignore_old_term_msg() {
    CALLED.with(|c| c.set(false));
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
    sm.set_step(fake_step);
    sm.term = 2;
    let _ = sm.step(Message {
        msg_type: MessageType::MsgApp,
        term: sm.term - 1,
        ..Default::default()
    });
    assert!(!CALLED.with(|c| c.get()));
}

#[test]
fn test_handle_msg_app() {
    let app = |term, log_term, index, commit, ents: Vec<Entry>| Message {
        msg_type: MessageType::MsgApp,
        term,
        log_term,
        index,
        commit,
        entries: ents,
        ..Default::default()
    };
    let e = |index, term| Entry {
        index,
        term,
        ..Default::default()
    };
    let tests = vec![
        (app(2, 3, 2, 3, vec![]), 2, 0, true),
        (app(2, 3, 3, 3, vec![]), 2, 0, true),
        (app(2, 1, 1, 1, vec![]), 2, 1, false),
        (app(2, 0, 0, 1, vec![e(1, 2)]), 1, 1, false),
        (app(2, 2, 2, 3, vec![e(3, 2), e(4, 2)]), 4, 3, false),
        (app(2, 2, 2, 4, vec![e(3, 2)]), 3, 3, false),
        (app(2, 1, 1, 4, vec![e(2, 2)]), 2, 2, false),
        (app(1, 1, 1, 3, vec![]), 2, 1, false),
        (app(1, 1, 1, 3, vec![e(2, 2)]), 2, 2, false),
        (app(2, 2, 2, 3, vec![]), 2, 2, false),
        (app(2, 2, 2, 4, vec![]), 2, 2, false),
    ];
    for (i, (msg, w_index, w_commit, w_reject)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1]);
        storage.append(&index_terms(1, &[1, 2])).unwrap();
        let mut sm = new_test_raft(1, 10, 1, storage);
        sm.become_follower(2, NONE);
        sm.handle_append_entries(msg);
        assert_eq!(w_index, sm.raft_log.last_index(), "#{i}");
        assert_eq!(w_commit, sm.raft_log.committed, "#{i}");
        let ms = sm.read_messages();
        assert_eq!(ms.len(), 1, "#{i}");
        assert_eq!(w_reject, ms[0].reject, "#{i}");
    }
}

#[test]
fn test_handle_heartbeat() {
    let commit = 2;
    let hb = |c| Message {
        from: 2,
        to: 1,
        msg_type: MessageType::MsgHeartbeat,
        term: 2,
        commit: c,
        ..Default::default()
    };
    let tests = [(hb(commit + 1), commit + 1), (hb(commit - 1), commit)];
    for (i, (msg, w_commit)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1, 2]);
        storage.append(&index_terms(1, &[1, 2, 3])).unwrap();
        let mut sm = new_test_raft(1, 5, 1, storage);
        sm.become_follower(2, 2);
        sm.raft_log.commit_to(commit);
        sm.handle_heartbeat(msg);
        assert_eq!(w_commit, sm.raft_log.committed, "#{i}");
        let ms = sm.read_messages();
        assert_eq!(ms.len(), 1, "#{i}");
        assert_eq!(MessageType::MsgHeartbeatResp, ms[0].msg_type, "#{i}");
    }
}

#[test]
fn test_handle_heartbeat_resp() {
    let mut storage = with_peers(&[1, 2]);
    storage.append(&index_terms(1, &[1, 2, 3])).unwrap();
    let mut sm = new_test_raft(1, 5, 1, storage);
    sm.become_candidate();
    sm.become_leader();
    let li = sm.raft_log.last_index();
    sm.raft_log.commit_to(li);

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgHeartbeatResp,
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(MessageType::MsgApp, msgs[0].msg_type);

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgHeartbeatResp,
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(MessageType::MsgApp, msgs[0].msg_type);

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgAppResp,
        index: msgs[0].index + msgs[0].entries.len() as u64,
        ..Default::default()
    });
    sm.read_messages();

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgHeartbeatResp,
        ..Default::default()
    });
    assert!(sm.read_messages().is_empty());
}

#[test]
fn test_raft_frees_read_only_mem() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2]));
    sm.become_candidate();
    sm.become_leader();
    let li = sm.raft_log.last_index();
    sm.raft_log.commit_to(li);

    let req_ctx = b"ctx".to_vec();
    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgReadIndex,
        entries: vec![Entry {
            data: req_ctx,
            ..Default::default()
        }],
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(MessageType::MsgHeartbeat, msgs[0].msg_type);
    assert_eq!(sm.read_only.unconfirmed_reads.len(), 1);

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgHeartbeatResp,
        context: msgs[0].context.clone(),
        ..Default::default()
    });
    assert!(sm.read_only.unconfirmed_reads.is_empty());
}

#[test]
fn test_msg_app_resp_wait_reset() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.become_candidate();
    sm.become_leader();
    next_ents(&mut sm);

    let _ = sm.step(Message {
        from: 2,
        msg_type: MessageType::MsgAppResp,
        index: 1,
        ..Default::default()
    });
    assert_eq!(1, sm.raft_log.committed);
    sm.read_messages();

    let _ = sm.step(Message {
        from: 1,
        msg_type: MessageType::MsgProp,
        entries: vec![Entry::default()],
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(MessageType::MsgApp, msgs[0].msg_type);
    assert_eq!(2, msgs[0].to);
    assert_eq!(msgs[0].entries.len(), 1);
    assert_eq!(2, msgs[0].entries[0].index);

    let _ = sm.step(Message {
        from: 3,
        msg_type: MessageType::MsgAppResp,
        index: 1,
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(MessageType::MsgApp, msgs[0].msg_type);
    assert_eq!(3, msgs[0].to);
    assert_eq!(msgs[0].entries.len(), 1);
    assert_eq!(2, msgs[0].entries[0].index);
}

#[test]
fn test_recv_msg_vote() {
    test_recv_msg_vote_impl(MessageType::MsgVote);
}

#[test]
fn test_recv_msg_pre_vote() {
    test_recv_msg_vote_impl(MessageType::MsgPreVote);
}

fn set_state_and_step(sm: &mut TestRaft, state: StateType) {
    sm.state = state;
    match state {
        StateType::StateFollower => sm.set_step(step_follower),
        StateType::StateCandidate | StateType::StatePreCandidate => sm.set_step(step_candidate),
        StateType::StateLeader => sm.set_step(step_leader),
    }
}

fn test_recv_msg_vote_impl(msg_type: MessageType) {
    use StateType::*;
    let tests = [
        (StateFollower, 0, 0, NONE, true),
        (StateFollower, 0, 1, NONE, true),
        (StateFollower, 0, 2, NONE, true),
        (StateFollower, 0, 3, NONE, false),
        (StateFollower, 1, 0, NONE, true),
        (StateFollower, 1, 1, NONE, true),
        (StateFollower, 1, 2, NONE, true),
        (StateFollower, 1, 3, NONE, false),
        (StateFollower, 2, 0, NONE, true),
        (StateFollower, 2, 1, NONE, true),
        (StateFollower, 2, 2, NONE, false),
        (StateFollower, 2, 3, NONE, false),
        (StateFollower, 3, 0, NONE, true),
        (StateFollower, 3, 1, NONE, true),
        (StateFollower, 3, 2, NONE, false),
        (StateFollower, 3, 3, NONE, false),
        (StateFollower, 3, 2, 2, false),
        (StateFollower, 3, 2, 1, true),
        (StateLeader, 3, 3, 1, true),
        (StatePreCandidate, 3, 3, 1, true),
        (StateCandidate, 3, 3, 1, true),
    ];
    for (i, (state, index, log_term, vote_for, wreject)) in tests.into_iter().enumerate() {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
        set_state_and_step(&mut sm, state);
        sm.vote = vote_for;
        sm.raft_log = log_from_ents(&index_terms(0, &[0, 2, 2]));
        let term = sm.raft_log.last_entry_id().term.max(log_term);
        sm.term = term;
        let _ = sm.step(Message {
            msg_type,
            term,
            from: 2,
            index,
            log_term,
            ..Default::default()
        });
        let msgs = sm.read_messages();
        assert_eq!(msgs.len(), 1, "#{i}");
        assert_eq!(vote_resp_msg_type(msg_type), msgs[0].msg_type, "#{i}");
        assert_eq!(wreject, msgs[0].reject, "#{i}");
    }
}

#[test]
fn test_state_transition() {
    use StateType::*;
    let tests = [
        (StateFollower, StateFollower, true, 1, NONE),
        (StateFollower, StatePreCandidate, true, 0, NONE),
        (StateFollower, StateCandidate, true, 1, NONE),
        (StateFollower, StateLeader, false, 0, NONE),
        (StatePreCandidate, StateFollower, true, 0, NONE),
        (StatePreCandidate, StatePreCandidate, true, 0, NONE),
        (StatePreCandidate, StateCandidate, true, 1, NONE),
        (StatePreCandidate, StateLeader, true, 0, 1),
        (StateCandidate, StateFollower, true, 0, NONE),
        (StateCandidate, StatePreCandidate, true, 0, NONE),
        (StateCandidate, StateCandidate, true, 1, NONE),
        (StateCandidate, StateLeader, true, 0, 1),
        (StateLeader, StateFollower, true, 1, NONE),
        (StateLeader, StatePreCandidate, false, 0, NONE),
        (StateLeader, StateCandidate, false, 1, NONE),
        (StateLeader, StateLeader, true, 0, 1),
    ];
    for (i, (from, to, wallow, wterm, wlead)) in tests.into_iter().enumerate() {
        let res = std::panic::catch_unwind(move || {
            let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
            sm.state = from;
            match to {
                StateFollower => sm.become_follower(wterm, wlead),
                StatePreCandidate => sm.become_pre_candidate(),
                StateCandidate => sm.become_candidate(),
                StateLeader => sm.become_leader(),
            }
            (sm.term, sm.lead)
        });
        match res {
            Ok((term, lead)) => {
                assert_eq!(wterm, term, "#{i}");
                assert_eq!(wlead, lead, "#{i}");
            }
            Err(_) => assert!(!wallow, "#{i}"),
        }
    }
}

#[test]
fn test_all_server_stepdown() {
    use StateType::*;
    let tests = [
        (StateFollower, StateFollower, 3, 0),
        (StatePreCandidate, StateFollower, 3, 0),
        (StateCandidate, StateFollower, 3, 0),
        (StateLeader, StateFollower, 3, 1),
    ];
    let tterm = 3;
    for (i, (state, wstate, wterm, windex)) in tests.into_iter().enumerate() {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        match state {
            StateFollower => sm.become_follower(1, NONE),
            StatePreCandidate => sm.become_pre_candidate(),
            StateCandidate => sm.become_candidate(),
            StateLeader => {
                sm.become_candidate();
                sm.become_leader();
            }
        }
        for (j, msg_type) in [MessageType::MsgVote, MessageType::MsgApp]
            .into_iter()
            .enumerate()
        {
            let _ = sm.step(Message {
                from: 2,
                msg_type,
                term: tterm,
                log_term: tterm,
                ..Default::default()
            });
            assert_eq!(wstate, sm.state, "#{i}.{j}");
            assert_eq!(wterm, sm.term, "#{i}.{j}");
            assert_eq!(windex, sm.raft_log.last_index(), "#{i}.{j}");
            assert_eq!(sm.raft_log.all_entries().len() as u64, windex, "#{i}.{j}");
            let wlead = if msg_type == MessageType::MsgVote {
                NONE
            } else {
                2
            };
            assert_eq!(wlead, sm.lead, "#{i}.{j}");
        }
    }
}

#[test]
fn test_candidate_reset_term_msg_heartbeat() {
    test_candidate_reset_term(MessageType::MsgHeartbeat);
}

#[test]
fn test_candidate_reset_term_msg_app() {
    test_candidate_reset_term(MessageType::MsgApp);
}

fn three_rafts() -> Network {
    let a = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    let b = new_test_raft(2, 10, 1, with_peers(&[1, 2, 3]));
    let c = new_test_raft(3, 10, 1, with_peers(&[1, 2, 3]));
    new_network(vec![raft_sm(a), raft_sm(b), raft_sm(c)])
}

fn test_candidate_reset_term(mt: MessageType) {
    let mut nt = three_rafts();
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    nt.isolate(3);
    nt.send(vec![hup(2)]);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    let c = nt.raft(3);
    c.reset_randomized_election_timeout();
    for _ in 0..c.randomized_election_timeout {
        c.tick();
    }
    c.advance_messages_after_append();
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);

    nt.recover();
    let a_term = nt.raft(1).term;
    nt.send(vec![Message {
        from: 1,
        to: 3,
        term: a_term,
        msg_type: mt,
        ..Default::default()
    }]);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);
    let t1 = nt.raft(1).term;
    assert_eq!(t1, nt.raft(3).term);
}

#[test]
fn test_candidate_self_vote_after_lost_election() {
    test_candidate_self_vote_after_lost_election_impl(false);
}

#[test]
fn test_candidate_self_vote_after_lost_election_pre_vote() {
    test_candidate_self_vote_after_lost_election_impl(true);
}

fn test_candidate_self_vote_after_lost_election_impl(pre_vote: bool) {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.pre_vote = pre_vote;
    let _ = sm.step(hup(1));
    let steps = sm.take_messages_after_append();
    let _ = sm.step(Message {
        from: 2,
        to: 1,
        term: sm.term,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    });
    assert_eq!(StateType::StateFollower, sm.state);
    let _ = sm.step_or_send(steps);
    assert_eq!(StateType::StateFollower, sm.state);
    let (granted, _, _) = sm.trk.tally_votes();
    assert_eq!(0, granted);
}

#[test]
fn test_candidate_delivers_pre_candidate_self_vote_after_becoming_candidate() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.pre_vote = true;
    let _ = sm.step(hup(1));
    assert_eq!(StateType::StatePreCandidate, sm.state);
    let steps = sm.take_messages_after_append();
    for from in [2, 3] {
        let _ = sm.step(Message {
            from,
            to: 1,
            term: sm.term + 1,
            msg_type: MessageType::MsgPreVoteResp,
            ..Default::default()
        });
    }
    assert_eq!(StateType::StateCandidate, sm.state);
    let _ = sm.step_or_send(steps);
    assert_eq!(StateType::StateCandidate, sm.state);
    let steps = sm.take_messages_after_append();
    let (granted, _, _) = sm.trk.tally_votes();
    assert_eq!(0, granted);
    let _ = sm.step(Message {
        from: 2,
        to: 1,
        term: sm.term,
        msg_type: MessageType::MsgVoteResp,
        ..Default::default()
    });
    assert_eq!(StateType::StateCandidate, sm.state);
    let _ = sm.step_or_send(steps);
    assert_eq!(StateType::StateLeader, sm.state);
}

#[test]
fn test_leader_msg_app_self_ack_after_term_change() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.become_candidate();
    sm.become_leader();
    let _ = sm.step(prop(1, 1, b"somedata"));
    let steps = sm.take_messages_after_append();
    let _ = sm.step(Message {
        from: 2,
        to: 1,
        term: sm.term + 1,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    });
    assert_eq!(StateType::StateFollower, sm.state);
    let _ = sm.step_or_send(steps);
    assert_eq!(StateType::StateFollower, sm.state);
}

#[test]
fn test_leader_stepdown_when_quorum_active() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();
    for _ in 0..sm.election_timeout + 1 {
        let _ = sm.step(Message {
            from: 2,
            msg_type: MessageType::MsgHeartbeatResp,
            term: sm.term,
            ..Default::default()
        });
        sm.tick();
    }
    assert_eq!(StateType::StateLeader, sm.state);
}

#[test]
fn test_leader_stepdown_when_quorum_lost() {
    let mut sm = new_test_raft(1, 5, 1, with_peers(&[1, 2, 3]));
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();
    for _ in 0..sm.election_timeout + 1 {
        sm.tick();
    }
    assert_eq!(StateType::StateFollower, sm.state);
}

fn three_rafts_check_quorum() -> Network {
    let mut rs = Vec::new();
    for id in 1..=3 {
        let mut r = new_test_raft(id, 10, 1, with_peers(&[1, 2, 3]));
        r.check_quorum = true;
        rs.push(raft_sm(r));
    }
    new_network(rs)
}

fn tick_n(r: &mut TestRaft, n: usize) {
    for _ in 0..n {
        r.tick();
    }
}

#[test]
fn test_leader_superseding_with_check_quorum() {
    let mut nt = three_rafts_check_quorum();
    let b = nt.raft(2);
    b.randomized_election_timeout = b.election_timeout + 1;
    let et = b.election_timeout;
    tick_n(b, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);

    tick_n(nt.raft(2), et);
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(3).state);
}

#[test]
fn test_leader_election_with_check_quorum() {
    let mut nt = three_rafts_check_quorum();
    let et = nt.raft(1).election_timeout;
    nt.raft(1).randomized_election_timeout = et + 1;
    nt.raft(2).randomized_election_timeout = et + 2;
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    nt.raft(1).randomized_election_timeout = et + 1;
    nt.raft(2).randomized_election_timeout = et + 2;
    tick_n(nt.raft(1), et);
    tick_n(nt.raft(2), et);
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    assert_eq!(StateType::StateLeader, nt.raft(3).state);
}

#[test]
fn test_free_stuck_candidate_with_check_quorum() {
    let mut nt = three_rafts_check_quorum();
    let et = nt.raft(2).election_timeout;
    nt.raft(2).randomized_election_timeout = et + 1;
    tick_n(nt.raft(2), et);
    nt.send(vec![hup(1)]);

    nt.isolate(1);
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);
    assert_eq!(nt.raft(2).term + 1, nt.raft(3).term);

    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);
    assert_eq!(nt.raft(2).term + 2, nt.raft(3).term);

    nt.recover();
    let a_term = nt.raft(1).term;
    nt.send(vec![Message {
        from: 1,
        to: 3,
        msg_type: MessageType::MsgHeartbeat,
        term: a_term,
        ..Default::default()
    }]);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    let t1 = nt.raft(1).term;
    assert_eq!(t1, nt.raft(3).term);

    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(3).state);
}

#[test]
fn test_non_promotable_voter_with_check_quorum() {
    let mut a = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    let mut b = new_test_raft(2, 10, 1, with_peers(&[1]));
    a.check_quorum = true;
    b.check_quorum = true;
    let mut nt = new_network(vec![raft_sm(a), raft_sm(b)]);
    let b = nt.raft(2);
    b.randomized_election_timeout = b.election_timeout + 1;
    b.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 2));
    assert!(!b.promotable());
    let et = b.election_timeout;
    tick_n(b, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(1, nt.raft(2).lead);
}

fn three_followers(check_quorum: bool, pre_vote: bool) -> Network {
    let mut rs = Vec::new();
    for id in 1..=3 {
        let mut r = new_test_raft(id, 10, 1, with_peers(&[1, 2, 3]));
        r.check_quorum = check_quorum;
        r.pre_vote = pre_vote;
        r.become_follower(1, NONE);
        rs.push(raft_sm(r));
    }
    new_network(rs)
}

#[test]
fn test_disruptive_follower() {
    let mut nt = three_followers(true, false);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    let n3 = nt.raft(3);
    n3.randomized_election_timeout = n3.election_timeout + 2;
    let n = n3.randomized_election_timeout - 1;
    tick_n(n3, n);
    n3.tick();

    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);
    assert_eq!(2, nt.raft(1).term);
    assert_eq!(2, nt.raft(2).term);
    assert_eq!(3, nt.raft(3).term);

    let t1 = nt.raft(1).term;
    nt.send(vec![Message {
        from: 1,
        to: 3,
        term: t1,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    }]);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);
    assert_eq!(3, nt.raft(1).term);
    assert_eq!(2, nt.raft(2).term);
    assert_eq!(3, nt.raft(3).term);
}

#[test]
fn test_disruptive_follower_pre_vote() {
    let mut nt = three_followers(true, false);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);

    nt.isolate(3);
    for _ in 0..3 {
        nt.send(vec![prop(1, 1, b"somedata")]);
    }
    for id in 1..=3 {
        nt.raft(id).pre_vote = true;
    }
    nt.recover();
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);
    assert_eq!(2, nt.raft(1).term);
    assert_eq!(2, nt.raft(2).term);
    assert_eq!(2, nt.raft(3).term);

    let t1 = nt.raft(1).term;
    nt.send(vec![Message {
        from: 1,
        to: 3,
        term: t1,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    }]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
}

fn read_index(id: u64, ctx: &[u8]) -> Message {
    Message {
        from: id,
        to: id,
        msg_type: MessageType::MsgReadIndex,
        entries: vec![Entry {
            data: ctx.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn check_read_states(
    nt: &mut Network,
    tests: &[(u64, usize, u64, &[u8])],
    next_ents_on_leader: bool,
) {
    for (i, &(id, proposals, wri, wctx)) in tests.iter().enumerate() {
        for _ in 0..proposals {
            nt.send(vec![prop_empty(1)]);
            if next_ents_on_leader {
                next_ents(nt.raft(1));
            }
        }
        nt.send(vec![read_index(id, wctx)]);
        let r = nt.raft(id);
        assert!(!r.read_states.is_empty(), "#{i}");
        let rs = &r.read_states[0];
        assert_eq!(wri, rs.index, "#{i}");
        assert_eq!(wctx, &rs.request_ctx[..], "#{i}");
        r.read_states.clear();
    }
}

#[test]
fn test_read_only_option_safe() {
    let mut nt = three_rafts();
    let b = nt.raft(2);
    b.randomized_election_timeout = b.election_timeout + 1;
    let et = b.election_timeout;
    tick_n(b, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    let tests: [(u64, usize, u64, &[u8]); 6] = [
        (1, 10, 11, b"ctx1"),
        (2, 10, 21, b"ctx2"),
        (3, 10, 31, b"ctx3"),
        (1, 10, 41, b"ctx4"),
        (2, 10, 51, b"ctx5"),
        (3, 10, 61, b"ctx6"),
    ];
    check_read_states(&mut nt, &tests, false);
}

#[test]
fn test_read_only_with_learner() {
    let a = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    let b = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    let mut nt = new_network(vec![raft_sm(a), raft_sm(b)]);
    let b = nt.raft(2);
    b.randomized_election_timeout = b.election_timeout + 1;
    let et = b.election_timeout;
    tick_n(b, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    let tests: [(u64, usize, u64, &[u8]); 4] = [
        (1, 10, 11, b"ctx1"),
        (2, 10, 21, b"ctx2"),
        (1, 10, 31, b"ctx3"),
        (2, 10, 41, b"ctx4"),
    ];
    check_read_states(&mut nt, &tests, true);
}

#[test]
fn test_read_only_option_lease() {
    let mut rs = Vec::new();
    for id in 1..=3 {
        let mut r = new_test_raft(id, 10, 1, with_peers(&[1, 2, 3]));
        r.read_only.option = ReadOnlyOption::ReadOnlyLeaseBased;
        r.check_quorum = true;
        rs.push(raft_sm(r));
    }
    let mut nt = new_network(rs);
    let b = nt.raft(2);
    b.randomized_election_timeout = b.election_timeout + 1;
    let et = b.election_timeout;
    tick_n(b, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    let tests: [(u64, usize, u64, &[u8]); 6] = [
        (1, 10, 11, b"ctx1"),
        (2, 10, 21, b"ctx2"),
        (3, 10, 31, b"ctx3"),
        (1, 10, 41, b"ctx4"),
        (2, 10, 51, b"ctx5"),
        (3, 10, 61, b"ctx6"),
    ];
    check_read_states(&mut nt, &tests, false);
}

#[test]
fn test_read_only_for_new_leader() {
    let node_configs = [(1, 1, 1, 0), (2, 2, 2, 2), (3, 2, 2, 2)];
    let mut peers = Vec::new();
    for (id, committed, applied, compact_index) in node_configs {
        let mut storage = with_peers(&[1, 2, 3]);
        storage.append(&index_terms(1, &[1, 1])).unwrap();
        storage
            .set_hard_state(HardState {
                term: 1,
                commit: committed,
                vote: 0,
            })
            .unwrap();
        if compact_index != 0 {
            storage.compact(compact_index).unwrap();
        }
        let mut cfg = new_test_config(id, 10, 1);
        cfg.applied = applied;
        peers.push(raft_sm(crate::raft::Raft::new(&mut cfg, storage)));
    }
    let mut nt = new_network(peers);
    nt.ignore(MessageType::MsgApp);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);

    let windex = 4;
    let wctx = b"ctx";
    nt.send(vec![read_index(1, wctx)]);
    assert!(nt.raft(1).read_states.is_empty());

    nt.recover();
    let ht = nt.raft(1).heartbeat_timeout;
    tick_n(nt.raft(1), ht);
    nt.send(vec![prop_empty(1)]);
    let sm = nt.raft(1);
    assert_eq!(4, sm.raft_log.committed);
    let last_log_term = sm
        .raft_log
        .zero_term_on_out_of_bounds(sm.raft_log.term(sm.raft_log.committed));
    assert_eq!(sm.term, last_log_term);
    assert_eq!(sm.read_states.len(), 1);
    assert_eq!(windex, sm.read_states[0].index);
    assert_eq!(wctx, &sm.read_states[0].request_ctx[..]);

    nt.send(vec![read_index(1, wctx)]);
    let sm = nt.raft(1);
    assert_eq!(sm.read_states.len(), 2);
    assert_eq!(windex, sm.read_states[1].index);
    assert_eq!(wctx, &sm.read_states[1].request_ctx[..]);
}

#[test]
fn test_read_only_duplicate_request() {
    let mut net = three_rafts();
    let delayed: Rc<RefCell<Vec<Message>>> = Rc::new(RefCell::new(Vec::new()));

    net.send(vec![hup(1)]);
    let read_index_msg_a = Message {
        from: 2,
        to: 1,
        msg_type: MessageType::MsgReadIndex,
        entries: vec![Entry {
            data: b"A".to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let read_index_minimum_a = net.raft(1).raft_log.committed;
    delayed.borrow_mut().push(read_index_msg_a.clone());

    let d = delayed.clone();
    net.msg_hook = Some(Box::new(move |m: &Message| {
        if m.msg_type == MessageType::MsgHeartbeatResp {
            d.borrow_mut().push(m.clone());
            return false;
        }
        true
    }));
    net.send(vec![read_index_msg_a]);
    net.msg_hook = None;

    let r1 = net.raft(1);
    r1.tick();
    r1.advance_messages_after_append();
    let msgs = std::mem::take(&mut r1.msgs);
    net.send(msgs);

    net.isolate(1);
    net.send(vec![hup(2)]);
    net.send(vec![prop(2, 2, b"someOp")]);

    let read_index_msg_b = read_index(1, b"B");
    let read_index_minimum_b = net.raft(2).raft_log.committed;
    net.send(vec![read_index_msg_b]);
    net.recover();
    let d = std::mem::take(&mut *delayed.borrow_mut());
    net.send(d);

    for id in 1..=3 {
        for rd in &net.raft(id).read_states {
            if rd.request_ctx == b"A" {
                assert!(
                    rd.index >= read_index_minimum_a,
                    "readIndex for A is {}",
                    rd.index
                );
            } else if rd.request_ctx == b"B" {
                assert!(
                    rd.index >= read_index_minimum_b,
                    "readIndex for B is {}",
                    rd.index
                );
            }
        }
    }
}

#[test]
fn test_leader_app_resp() {
    let tests = [
        (3, true, 0, 3, 0, 0, 0),
        (2, true, 0, 2, 1, 1, 0),
        (2, false, 2, 4, 2, 2, 2),
        (0, false, 0, 4, 1, 0, 0),
    ];
    for (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted) in tests {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        sm.raft_log = log_from_ents(&index_terms(0, &[0, 1, 1]));
        sm.become_candidate();
        sm.become_leader();
        sm.read_messages();
        let term = sm.term;
        assert!(
            sm.step(Message {
                from: 2,
                msg_type: MessageType::MsgAppResp,
                index,
                term,
                reject,
                reject_hint: index,
                ..Default::default()
            })
            .is_ok()
        );
        let p = &sm.trk.progress[&2];
        assert_eq!(wmatch, p.match_);
        assert_eq!(wnext, p.next);
        let msgs = sm.read_messages();
        assert_eq!(msgs.len(), wmsg_num);
        for msg in &msgs {
            assert_eq!(
                windex,
                msg.index,
                "{}",
                crate::util::describe_message(msg, None)
            );
            assert_eq!(
                wcommitted,
                msg.commit,
                "{}",
                crate::util::describe_message(msg, None)
            );
        }
    }
}

#[test]
fn test_bcast_beat() {
    let offset = 1000;
    let s = snap(offset, 1, &[1, 2, 3], &[]);
    let mut storage = MemoryStorage::new();
    storage.apply_snapshot(s).unwrap();
    let mut sm = new_test_raft(1, 10, 1, storage);
    sm.term = 1;
    sm.become_candidate();
    sm.become_leader();
    for i in 0..10 {
        must_append_entry(
            &mut sm,
            vec![Entry {
                index: i + 1,
                ..Default::default()
            }],
        );
    }
    sm.advance_messages_after_append();

    let li = sm.raft_log.last_index();
    {
        let p2 = sm.trk.progress.get_mut(&2).unwrap();
        p2.match_ = 5;
        p2.next = 6;
    }
    {
        let p3 = sm.trk.progress.get_mut(&3).unwrap();
        p3.match_ = li;
        p3.next = li + 1;
    }
    let _ = sm.step(Message {
        msg_type: MessageType::MsgBeat,
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 2);
    let mut want_commit = std::collections::BTreeMap::new();
    want_commit.insert(2, sm.raft_log.committed.min(sm.trk.progress[&2].match_));
    want_commit.insert(3, sm.raft_log.committed.min(sm.trk.progress[&3].match_));
    for (i, m) in msgs.iter().enumerate() {
        assert_eq!(MessageType::MsgHeartbeat, m.msg_type, "#{i}");
        assert_eq!(0, m.index, "#{i}");
        assert_eq!(0, m.log_term, "#{i}");
        let commit = want_commit.remove(&m.to).expect("unexpected recipient");
        assert_eq!(commit, m.commit, "#{i}");
        assert!(m.entries.is_empty(), "#{i}");
    }
}

#[test]
fn test_recv_msg_beat() {
    let tests = [
        (StateType::StateLeader, 2),
        (StateType::StateCandidate, 0),
        (StateType::StateFollower, 0),
    ];
    for (i, (state, w_msg)) in tests.into_iter().enumerate() {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        sm.raft_log = log_from_ents(&index_terms(0, &[0, 1, 1]));
        sm.term = 1;
        set_state_and_step(&mut sm, state);
        let _ = sm.step(beat(1));
        let msgs = sm.read_messages();
        assert_eq!(msgs.len(), w_msg, "#{i}");
        for m in &msgs {
            assert_eq!(MessageType::MsgHeartbeat, m.msg_type, "#{i}");
        }
    }
}

#[test]
fn test_leader_increase_next() {
    let previous_ents = index_terms(1, &[1, 2, 3]);
    let tests = [
        (
            PrState::StateReplicate,
            2,
            previous_ents.len() as u64 + 1 + 1 + 1,
        ),
        (PrState::StateProbe, 2, 2),
    ];
    for (i, (state, next, wnext)) in tests.into_iter().enumerate() {
        let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
        sm.raft_log.append(previous_ents.clone());
        sm.become_candidate();
        sm.become_leader();
        {
            let p = sm.trk.progress.get_mut(&2).unwrap();
            p.state = state;
            p.next = next;
        }
        let _ = sm.step(prop(1, 1, b"somedata"));
        assert_eq!(wnext, sm.trk.progress[&2].next, "#{i}");
    }
}

fn data_entry() -> Vec<Entry> {
    vec![Entry {
        data: b"somedata".to_vec(),
        ..Default::default()
    }]
}

#[test]
fn test_send_append_for_progress_probe() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.trk.progress.get_mut(&2).unwrap().become_probe();

    for i in 0..3 {
        if i == 0 {
            must_append_entry(&mut r, data_entry());
            r.send_append(2);
            let msg = r.read_messages();
            assert_eq!(msg.len(), 1);
            assert_eq!(0, msg[0].index);
        }
        assert!(r.trk.progress[&2].msg_app_flow_paused);
        for _ in 0..10 {
            must_append_entry(&mut r, data_entry());
            r.send_append(2);
            assert!(r.read_messages().is_empty());
        }
        for _ in 0..r.heartbeat_timeout {
            let _ = r.step(beat(1));
        }
        assert!(r.trk.progress[&2].msg_app_flow_paused);
        let msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(MessageType::MsgHeartbeat, msg[0].msg_type);
    }

    let _ = r.step(m(2, 1, MessageType::MsgHeartbeatResp));
    let msg = r.read_messages();
    assert_eq!(msg.len(), 1);
    assert_eq!(0, msg[0].index);
    assert!(r.trk.progress[&2].msg_app_flow_paused);
}

#[test]
fn test_send_append_for_progress_replicate() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.trk.progress.get_mut(&2).unwrap().become_replicate();
    for i in 0..10 {
        must_append_entry(&mut r, data_entry());
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 1, "#{i}");
    }
}

#[test]
fn test_send_append_for_progress_snapshot() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.trk.progress.get_mut(&2).unwrap().become_snapshot(10);
    for i in 0..10 {
        must_append_entry(&mut r, data_entry());
        r.send_append(2);
        assert!(r.read_messages().is_empty(), "#{i}");
    }
}

#[test]
fn test_recv_msg_unreachable() {
    let previous_ents = index_terms(1, &[1, 2, 3]);
    let mut s = with_peers(&[1, 2]);
    s.append(&previous_ents).unwrap();
    let mut r = new_test_raft(1, 10, 1, s);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    {
        let p = r.trk.progress.get_mut(&2).unwrap();
        p.match_ = 3;
        p.become_replicate();
        p.next = 6;
    }
    let _ = r.step(m(2, 1, MessageType::MsgUnreachable));
    assert_eq!(PrState::StateProbe, r.trk.progress[&2].state);
    let wnext = r.trk.progress[&2].match_ + 1;
    assert_eq!(wnext, r.trk.progress[&2].next);
}

#[test]
fn test_restore() {
    let s = snap(11, 11, &[1, 2, 3], &[]);
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    assert!(sm.restore(s.clone()));
    assert_eq!(s.metadata.index, sm.raft_log.last_index());
    assert_eq!(
        s.metadata.term,
        must_term(sm.raft_log.term(s.metadata.index))
    );
    assert_eq!(s.metadata.conf_state.voters, sm.trk.voter_nodes());
    assert!(!sm.restore(s));
    let n = sm.randomized_election_timeout;
    tick_n(&mut sm, n);
    assert_eq!(StateType::StateFollower, sm.state);
}

#[test]
fn test_restore_with_learner() {
    let s = snap(11, 11, &[1, 2], &[3]);
    let mut sm = new_test_learner_raft(3, 8, 2, new_test_memory_storage(&[1, 2], &[3]));
    assert!(sm.restore(s.clone()));
    assert_eq!(s.metadata.index, sm.raft_log.last_index());
    assert_eq!(
        s.metadata.term,
        must_term(sm.raft_log.term(s.metadata.index))
    );
    assert_eq!(
        sm.trk.voter_nodes().len(),
        s.metadata.conf_state.voters.len()
    );
    assert_eq!(
        sm.trk.learner_nodes().len(),
        s.metadata.conf_state.learners.len()
    );
    for n in &s.metadata.conf_state.voters {
        assert!(!sm.trk.progress[n].is_learner);
    }
    for n in &s.metadata.conf_state.learners {
        assert!(sm.trk.progress[n].is_learner);
    }
    assert!(!sm.restore(s));
}

#[test]
fn test_restore_with_voters_outgoing() {
    let mut s = snap(11, 11, &[2, 3, 4], &[]);
    s.metadata.conf_state.voters_outgoing = vec![1, 2, 3];
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    assert!(sm.restore(s.clone()));
    assert_eq!(s.metadata.index, sm.raft_log.last_index());
    assert_eq!(
        must_term(sm.raft_log.term(s.metadata.index)),
        s.metadata.term
    );
    assert_eq!(vec![1, 2, 3, 4], sm.trk.voter_nodes());
    assert!(!sm.restore(s));
    let n = sm.randomized_election_timeout;
    tick_n(&mut sm, n);
    assert_eq!(StateType::StateFollower, sm.state);
}

#[test]
fn test_restore_voter_to_learner() {
    let s = snap(11, 11, &[1, 2], &[3]);
    let mut sm = new_test_raft(3, 10, 1, with_peers(&[1, 2, 3]));
    assert!(!sm.is_learner);
    assert!(sm.restore(s));
}

#[test]
fn test_restore_learner_promotion() {
    let s = snap(11, 11, &[1, 2, 3], &[]);
    let mut sm = new_test_learner_raft(3, 10, 1, new_test_memory_storage(&[1, 2], &[3]));
    assert!(sm.is_learner);
    assert!(sm.restore(s));
    assert!(!sm.is_learner);
}

#[test]
fn test_learner_receive_snapshot() {
    let s = snap(11, 11, &[1], &[2]);
    let mut n1 = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    let n2 = new_test_learner_raft(2, 10, 1, new_test_memory_storage(&[1], &[2]));
    n1.restore(s);
    let sn = n1.raft_log.next_unstable_snapshot().cloned().unwrap();
    n1.raft_log.storage.apply_snapshot(sn.clone()).unwrap();
    n1.applied_snap(&sn);

    let mut nt = new_network(vec![raft_sm(n1), raft_sm(n2)]);
    let n1 = nt.raft(1);
    n1.randomized_election_timeout = n1.election_timeout;
    let et = n1.election_timeout;
    tick_n(n1, et);
    nt.send(vec![beat(1)]);
    let c1 = nt.raft(1).raft_log.committed;
    assert_eq!(c1, nt.raft(2).raft_log.committed);
}

#[test]
fn test_restore_ignore_snapshot() {
    let previous_ents = index_terms(1, &[1, 1, 1]);
    let commit = 1;
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    sm.raft_log.append(previous_ents);
    sm.raft_log.commit_to(commit);

    let mut s = snap(commit, 1, &[1, 2], &[]);
    assert!(!sm.restore(s.clone()));
    assert_eq!(sm.raft_log.committed, commit);

    s.metadata.index = commit + 1;
    assert!(!sm.restore(s));
    assert_eq!(sm.raft_log.committed, commit + 1);
}

#[test]
fn test_provide_snap() {
    let s = snap(11, 11, &[1, 2], &[]);
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
    sm.restore(s);
    sm.become_candidate();
    sm.become_leader();
    let fi = sm.raft_log.first_index();
    sm.trk.progress.get_mut(&2).unwrap().next = fi;
    let _ = sm.step(Message {
        from: 2,
        to: 1,
        msg_type: MessageType::MsgAppResp,
        index: fi - 1,
        reject: true,
        ..Default::default()
    });
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].msg_type, MessageType::MsgSnap);
}

#[test]
fn test_ignore_providing_snap() {
    let s = snap(11, 11, &[1, 2], &[]);
    let mut sm = new_test_raft(1, 10, 1, with_peers(&[1]));
    sm.restore(s);
    sm.become_candidate();
    sm.become_leader();
    let fi = sm.raft_log.first_index();
    {
        let p = sm.trk.progress.get_mut(&2).unwrap();
        p.next = fi - 1;
        p.recent_active = false;
    }
    let _ = sm.step(prop(1, 1, b"somedata"));
    assert!(sm.read_messages().is_empty());
}

#[test]
fn test_restore_from_snap_msg() {
    let s = snap(11, 11, &[1, 2], &[]);
    let msg = Message {
        msg_type: MessageType::MsgSnap,
        from: 1,
        term: 2,
        snapshot: Some(Box::new(s)),
        ..Default::default()
    };
    let mut sm = new_test_raft(2, 10, 1, with_peers(&[1, 2]));
    let _ = sm.step(msg);
    assert_eq!(1, sm.lead);
}

fn snapshot_and_compact_leader(nt: &mut Network) {
    let lead = nt.raft(1);
    next_ents(lead);
    let applied = lead.raft_log.applied;
    let cs = ConfState {
        voters: lead.trk.voter_nodes(),
        ..Default::default()
    };
    lead.raft_log
        .storage
        .create_snapshot(applied, Some(&cs), Vec::new())
        .unwrap();
    lead.raft_log.storage.compact(applied).unwrap();
}

#[test]
fn test_slow_node_restore() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    for _ in 0..=100 {
        nt.send(vec![prop_empty(1)]);
    }
    snapshot_and_compact_leader(&mut nt);

    nt.recover();
    loop {
        nt.send(vec![beat(1)]);
        if nt.raft(1).trk.progress[&3].recent_active {
            break;
        }
    }
    nt.send(vec![prop_empty(1)]);
    nt.send(vec![prop_empty(1)]);
    let c1 = nt.raft(1).raft_log.committed;
    assert_eq!(c1, nt.raft(3).raft_log.committed);
}

fn conf_change_prop(t: EntryType, data: Vec<u8>) -> Message {
    Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgProp,
        entries: vec![Entry {
            entry_type: t,
            data,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn test_step_config() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    let index = r.raft_log.last_index();
    let _ = r.step(conf_change_prop(EntryType::EntryConfChange, Vec::new()));
    assert_eq!(index + 1, r.raft_log.last_index());
    assert_eq!(index + 1, r.pending_conf_index);
}

#[test]
fn test_step_ignore_config() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();
    let _ = r.step(conf_change_prop(EntryType::EntryConfChange, Vec::new()));
    let index = r.raft_log.last_index();
    let pending_conf_index = r.pending_conf_index;
    let _ = r.step(conf_change_prop(EntryType::EntryConfChange, Vec::new()));
    let wents = vec![Entry {
        entry_type: EntryType::EntryNormal,
        term: 1,
        index: 3,
        data: Vec::new(),
    }];
    let ents = r.raft_log.entries(index + 1, NO_LIMIT).unwrap();
    assert_eq!(wents, ents);
    assert_eq!(pending_conf_index, r.pending_conf_index);
}

#[test]
fn test_new_leader_pending_config() {
    let tests = [(false, 0), (true, 1)];
    for (i, (add_entry, wpending_index)) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
        if add_entry {
            must_append_entry(&mut r, vec![Entry::default()]);
        }
        r.become_candidate();
        r.become_leader();
        assert_eq!(wpending_index, r.pending_conf_index, "#{i}");
    }
}

#[test]
fn test_add_node() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1]));
    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 2));
    assert_eq!(vec![1, 2], r.trk.voter_nodes());
}

#[test]
fn test_add_learner() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1]));
    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddLearnerNode, 2));
    assert!(!r.is_learner);
    assert_eq!(vec![2], r.trk.learner_nodes());
    assert!(r.trk.progress[&2].is_learner);

    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 2));
    assert!(!r.trk.progress[&2].is_learner);

    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddLearnerNode, 1));
    assert!(r.trk.progress[&1].is_learner);
    assert!(r.is_learner);

    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 1));
    assert!(!r.trk.progress[&1].is_learner);
    assert!(!r.is_learner);
}

#[test]
fn test_add_node_check_quorum() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1]));
    r.check_quorum = true;
    r.become_candidate();
    r.become_leader();
    let et = r.election_timeout;
    tick_n(&mut r, et - 1);
    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddNode, 2));
    r.tick();
    assert_eq!(StateType::StateLeader, r.state);
    tick_n(&mut r, et);
    assert_eq!(StateType::StateFollower, r.state);
}

#[test]
fn test_remove_node() {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 2));
    assert_eq!(vec![1], r.trk.voter_nodes());
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 1));
    }));
    assert!(res.is_err(), "did not panic");
}

#[test]
fn test_remove_learner() {
    let mut r = new_test_learner_raft(1, 10, 1, new_test_memory_storage(&[1], &[2]));
    r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 2));
    assert_eq!(vec![1], r.trk.voter_nodes());
    assert!(r.trk.learner_nodes().is_empty());
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 1));
    }));
    assert!(res.is_err(), "did not panic");
}

#[test]
fn test_promotable() {
    let id = 1;
    let tests: [(&[u64], bool); 4] = [
        (&[1], true),
        (&[1, 2, 3], true),
        (&[], false),
        (&[2, 3], false),
    ];
    for (i, (peers, wp)) in tests.into_iter().enumerate() {
        let r = new_test_raft(id, 5, 1, with_peers(peers));
        assert_eq!(wp, r.promotable(), "#{i}");
    }
}

#[test]
fn test_raft_nodes() {
    let tests: [(&[u64], Vec<u64>); 2] = [(&[1, 2, 3], vec![1, 2, 3]), (&[3, 2, 1], vec![1, 2, 3])];
    for (i, (ids, wids)) in tests.into_iter().enumerate() {
        let r = new_test_raft(1, 10, 1, with_peers(ids));
        assert_eq!(wids, r.trk.voter_nodes(), "#{i}");
    }
}

#[test]
fn test_campaign_while_leader() {
    test_campaign_while_leader_impl(false);
}

#[test]
fn test_pre_campaign_while_leader() {
    test_campaign_while_leader_impl(true);
}

fn test_campaign_while_leader_impl(pre_vote: bool) {
    let mut cfg = new_test_config(1, 5, 1);
    cfg.pre_vote = pre_vote;
    let mut r = crate::raft::Raft::new(&mut cfg, with_peers(&[1]));
    assert_eq!(StateType::StateFollower, r.state);
    let _ = r.step(hup(1));
    r.advance_messages_after_append();
    assert_eq!(StateType::StateLeader, r.state);
    let term = r.term;
    let _ = r.step(hup(1));
    r.advance_messages_after_append();
    assert_eq!(StateType::StateLeader, r.state);
    assert_eq!(term, r.term);
}

#[test]
fn test_commit_after_remove_node() {
    let mut r = new_test_raft(1, 5, 1, with_peers(&[1, 2]));
    r.become_candidate();
    r.become_leader();

    let cc = ConfChange {
        change_type: ConfChangeType::ConfChangeRemoveNode,
        node_id: 2,
        ..Default::default()
    };
    let _ = r.step(Message {
        msg_type: MessageType::MsgProp,
        entries: vec![Entry {
            entry_type: EntryType::EntryConfChange,
            data: cc.encode(),
            ..Default::default()
        }],
        ..Default::default()
    });
    assert!(next_ents(&mut r).is_empty());
    let cc_index = r.raft_log.last_index();

    let _ = r.step(Message {
        msg_type: MessageType::MsgProp,
        entries: vec![Entry {
            entry_type: EntryType::EntryNormal,
            data: b"hello".to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let _ = r.step(Message {
        msg_type: MessageType::MsgAppResp,
        from: 2,
        index: cc_index,
        ..Default::default()
    });
    let ents = next_ents(&mut r);
    assert_eq!(ents.len(), 2);
    assert_eq!(EntryType::EntryNormal, ents[0].entry_type);
    assert!(ents[0].data.is_empty());
    assert_eq!(EntryType::EntryConfChange, ents[1].entry_type);

    r.apply_conf_change(&cc.as_v2());
    let ents = next_ents(&mut r);
    assert_eq!(ents.len(), 1);
    assert_eq!(EntryType::EntryNormal, ents[0].entry_type);
    assert_eq!(b"hello", &ents[0].data[..]);
}

fn transfer(from: u64, to: u64) -> Message {
    m(from, to, MessageType::MsgTransferLeader)
}

fn check_leader_transfer_state(r: &TestRaft, state: StateType, lead: u64) {
    assert_eq!(state, r.state);
    assert_eq!(lead, r.lead);
    assert_eq!(NONE, r.lead_transferee);
}

#[test]
fn test_leader_transfer_to_up_to_date_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    assert_eq!(1, nt.raft(1).lead);
    nt.send(vec![transfer(2, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 2);
    nt.send(vec![prop_empty(1)]);
    nt.send(vec![transfer(1, 2)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_to_up_to_date_node_from_follower() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    assert_eq!(1, nt.raft(1).lead);
    nt.send(vec![transfer(2, 2)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 2);
    nt.send(vec![prop_empty(1)]);
    nt.send(vec![transfer(1, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_with_check_quorum() {
    let mut nt = new_network(vec![None, None, None]);
    for i in 1..4u64 {
        let r = nt.raft(i);
        r.check_quorum = true;
        r.randomized_election_timeout = r.election_timeout + i as usize;
    }
    let f = nt.raft(2);
    let et = f.election_timeout;
    tick_n(f, et);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    nt.send(vec![transfer(2, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 2);
    nt.send(vec![prop_empty(1)]);
    nt.send(vec![transfer(1, 2)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_to_slow_follower() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![prop_empty(1)]);
    nt.recover();
    assert_eq!(1, nt.raft(1).trk.progress[&3].match_);
    nt.send(vec![transfer(3, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 3);
}

#[test]
fn test_leader_transfer_after_snapshot() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![prop_empty(1)]);
    snapshot_and_compact_leader(&mut nt);
    nt.recover();
    assert_eq!(1, nt.raft(1).trk.progress[&3].match_);

    let filtered: Rc<RefCell<Option<Message>>> = Rc::new(RefCell::new(None));
    let f = filtered.clone();
    nt.msg_hook = Some(Box::new(move |m: &Message| {
        if m.msg_type != MessageType::MsgAppResp || m.from != 3 || m.reject {
            return true;
        }
        *f.borrow_mut() = Some(m.clone());
        false
    }));
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert!(filtered.borrow().is_some());

    let follower = nt.raft(3);
    let sn = follower.raft_log.next_unstable_snapshot().cloned().unwrap();
    follower
        .raft_log
        .storage
        .apply_snapshot(sn.clone())
        .unwrap();
    follower.applied_snap(&sn);
    nt.msg_hook = None;
    let msg = filtered.borrow_mut().take().unwrap();
    nt.send(vec![msg]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 3);
}

#[test]
fn test_leader_transfer_to_self() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.send(vec![transfer(1, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_to_non_existing_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.send(vec![transfer(4, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_timeout() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    let lead = nt.raft(1);
    let (ht, et) = (lead.heartbeat_timeout, lead.election_timeout);
    tick_n(lead, ht);
    assert_eq!(3, lead.lead_transferee);
    tick_n(lead, et - ht);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_ignore_proposal() {
    let r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    let mut nt = new_network(vec![raft_sm(r), None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    next_ents(nt.raft(1));
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.send(vec![prop_empty(1)]);
    let err = nt.raft(1).step(prop_empty(1));
    assert_eq!(Err(Error::ProposalDropped), err);
    assert_eq!(1, nt.raft(1).trk.progress[&1].match_);
}

#[test]
fn test_leader_transfer_receive_higher_term_vote() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.send(vec![Message {
        from: 2,
        to: 2,
        msg_type: MessageType::MsgHup,
        index: 1,
        term: 2,
        ..Default::default()
    }]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 2);
}

#[test]
fn test_leader_transfer_remove_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.ignore(MessageType::MsgTimeoutNow);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.raft(1)
        .apply_conf_change(&cc_v1(ConfChangeType::ConfChangeRemoveNode, 3));
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_demote_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.ignore(MessageType::MsgTimeoutNow);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.raft(1).apply_conf_change(&ConfChangeV2 {
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::ConfChangeRemoveNode,
                node_id: 3,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::ConfChangeAddLearnerNode,
                node_id: 3,
            },
        ],
        ..Default::default()
    });
    nt.raft(1).apply_conf_change(&ConfChangeV2::default());
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_back() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.send(vec![transfer(1, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_leader_transfer_second_transfer_to_another_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    nt.send(vec![transfer(2, 1)]);
    check_leader_transfer_state(nt.raft(1), StateType::StateFollower, 2);
}

#[test]
fn test_leader_transfer_second_transfer_to_same_node() {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![transfer(3, 1)]);
    assert_eq!(3, nt.raft(1).lead_transferee);
    let (ht, et) = (nt.raft(1).heartbeat_timeout, nt.raft(1).election_timeout);
    tick_n(nt.raft(1), ht);
    nt.send(vec![transfer(3, 1)]);
    tick_n(nt.raft(1), et - ht);
    check_leader_transfer_state(nt.raft(1), StateType::StateLeader, 1);
}

#[test]
fn test_transfer_non_member() {
    let mut r = new_test_raft(1, 5, 1, with_peers(&[2, 3, 4]));
    let _ = r.step(m(2, 1, MessageType::MsgTimeoutNow));
    let _ = r.step(m(2, 1, MessageType::MsgVoteResp));
    let _ = r.step(m(3, 1, MessageType::MsgVoteResp));
    assert_eq!(StateType::StateFollower, r.state);
}

#[test]
fn test_node_with_smaller_term_can_complete_election() {
    let mut nt = three_followers(false, true);
    nt.cut(1, 3);
    nt.cut(2, 3);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);

    nt.send(vec![hup(2)]);
    assert_eq!(3, nt.raft(1).term);
    assert_eq!(3, nt.raft(2).term);
    assert_eq!(1, nt.raft(3).term);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    assert_eq!(StateType::StateLeader, nt.raft(2).state);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);

    nt.recover();
    nt.cut(2, 1);
    nt.cut(2, 3);
    nt.send(vec![hup(3)]);
    nt.send(vec![hup(1)]);
    let a = nt.raft(1).state;
    let b = nt.raft(3).state;
    assert!(a == StateType::StateLeader || b == StateType::StateLeader);
}

#[test]
fn test_pre_vote_with_split_vote() {
    let mut nt = three_followers(false, true);
    nt.send(vec![hup(1)]);
    nt.isolate(1);
    nt.send(vec![hup(2), hup(3)]);
    assert_eq!(3, nt.raft(2).term);
    assert_eq!(3, nt.raft(3).term);
    assert_eq!(StateType::StateCandidate, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);

    nt.send(vec![hup(2)]);
    assert_eq!(4, nt.raft(2).term);
    assert_eq!(4, nt.raft(3).term);
    assert_eq!(StateType::StateLeader, nt.raft(2).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);
}

#[test]
fn test_pre_vote_with_check_quorum() {
    let mut nt = three_followers(true, true);
    nt.send(vec![hup(1)]);
    nt.isolate(1);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateFollower, nt.raft(3).state);
    nt.send(vec![hup(3)]);
    nt.send(vec![hup(2)]);
    let s2 = nt.raft(2).state;
    let s3 = nt.raft(3).state;
    assert!(s2 == StateType::StateLeader || s3 == StateType::StateFollower);
}

#[test]
fn test_learner_campaign() {
    let mut n1 = new_test_raft(1, 10, 1, with_peers(&[1]));
    n1.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddLearnerNode, 2));
    let mut n2 = new_test_raft(2, 10, 1, with_peers(&[1]));
    n2.apply_conf_change(&cc_v1(ConfChangeType::ConfChangeAddLearnerNode, 2));
    let mut nt = new_network(vec![raft_sm(n1), raft_sm(n2)]);
    nt.send(vec![hup(2)]);
    assert!(nt.raft(2).is_learner);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    nt.send(vec![hup(1)]);
    let n1 = nt.raft(1);
    assert!(n1.state == StateType::StateLeader && n1.lead == 1);

    nt.send(vec![m(1, 2, MessageType::MsgTimeoutNow)]);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
}

fn new_pre_vote_migration_cluster() -> Network {
    let mut rs = Vec::new();
    for id in 1..=3 {
        let mut r = new_test_raft(id, 10, 1, with_peers(&[1, 2, 3]));
        r.become_follower(1, NONE);
        r.pre_vote = id != 3;
        rs.push(raft_sm(r));
    }
    let mut nt = new_network(rs);
    nt.send(vec![hup(1)]);
    nt.isolate(3);
    nt.send(vec![prop(1, 1, b"some data")]);
    nt.send(vec![hup(3)]);
    nt.send(vec![hup(3)]);

    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StateCandidate, nt.raft(3).state);
    assert_eq!(2, nt.raft(1).term);
    assert_eq!(2, nt.raft(2).term);
    assert_eq!(4, nt.raft(3).term);

    nt.raft(3).pre_vote = true;
    nt.recover();
    nt
}

#[test]
fn test_pre_vote_migration_can_complete_election() {
    let mut nt = new_pre_vote_migration_cluster();
    nt.isolate(1);
    nt.send(vec![hup(3)]);
    nt.send(vec![hup(2)]);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);
    nt.send(vec![hup(3)]);
    nt.send(vec![hup(2)]);
    let s2 = nt.raft(2).state;
    let s3 = nt.raft(3).state;
    assert!(s2 == StateType::StateLeader || s3 == StateType::StateFollower);
}

#[test]
fn test_pre_vote_migration_with_free_stuck_pre_candidate() {
    let mut nt = new_pre_vote_migration_cluster();
    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);

    nt.send(vec![hup(3)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);
    assert_eq!(StateType::StatePreCandidate, nt.raft(3).state);

    let t1 = nt.raft(1).term;
    nt.send(vec![Message {
        from: 1,
        to: 3,
        msg_type: MessageType::MsgHeartbeat,
        term: t1,
        ..Default::default()
    }]);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    let t1 = nt.raft(1).term;
    assert_eq!(t1, nt.raft(3).term);
}

fn test_conf_change_check_before_campaign_impl(v2: bool) {
    let mut nt = new_network(vec![None, None, None]);
    nt.send(vec![hup(1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);

    let cc = ConfChange {
        change_type: ConfChangeType::ConfChangeRemoveNode,
        node_id: 2,
        ..Default::default()
    };
    let (ty, data) = if v2 {
        (EntryType::EntryConfChangeV2, cc.as_v2().encode())
    } else {
        (EntryType::EntryConfChange, cc.encode())
    };
    nt.send(vec![conf_change_prop(ty, data)]);

    let n2 = nt.raft(2);
    let n = n2.randomized_election_timeout;
    tick_n(n2, n);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    nt.send(vec![transfer(2, 1)]);
    assert_eq!(StateType::StateLeader, nt.raft(1).state);
    assert_eq!(StateType::StateFollower, nt.raft(2).state);

    let et = nt.raft(1).election_timeout;
    tick_n(nt.raft(1), et);

    next_ents(nt.raft(2));

    nt.send(vec![transfer(2, 1)]);
    assert_eq!(StateType::StateFollower, nt.raft(1).state);
    assert_eq!(StateType::StateLeader, nt.raft(2).state);

    next_ents(nt.raft(1));
    let n1 = nt.raft(1);
    let n = n1.randomized_election_timeout;
    tick_n(n1, n);
    assert_eq!(StateType::StateCandidate, nt.raft(1).state);
}

#[test]
fn test_conf_change_check_before_campaign() {
    test_conf_change_check_before_campaign_impl(false);
}

#[test]
fn test_conf_change_v2_check_before_campaign() {
    test_conf_change_check_before_campaign_impl(true);
}

#[test]
fn test_fast_log_rejection() {
    struct T {
        leader_log: Vec<Entry>,
        follower_log: Vec<Entry>,
        follower_compact: u64,
        reject_hint_term: u64,
        reject_hint_index: u64,
        next_append_term: u64,
        next_append_index: u64,
    }
    let t = |l: &[u64], f: &[u64], c, rht, rhi, nat, nai| T {
        leader_log: index_terms(1, l),
        follower_log: index_terms(1, f),
        follower_compact: c,
        reject_hint_term: rht,
        reject_hint_index: rhi,
        next_append_term: nat,
        next_append_index: nai,
    };
    let tests = vec![
        t(
            &[1, 2, 2, 4, 4, 4, 4],
            &[1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3],
            0,
            3,
            7,
            2,
            3,
        ),
        t(
            &[1, 2, 2, 3, 4, 4, 4, 5],
            &[1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3],
            0,
            3,
            8,
            3,
            4,
        ),
        t(&[1, 1, 1, 1], &[1, 2, 2, 4], 0, 1, 1, 1, 1),
        t(&[1, 1, 1, 1, 1, 1], &[1, 2, 2, 4], 0, 1, 1, 1, 1),
        t(&[1, 1, 1, 1], &[1, 2, 2, 4, 4, 4], 0, 1, 1, 1, 1),
        t(&[1, 1, 1, 4, 5], &[1, 1, 1, 4], 0, 4, 4, 4, 4),
        t(
            &[2, 5, 5, 5, 5, 5, 5, 5, 5],
            &[2, 4, 4, 4, 4, 4],
            0,
            4,
            6,
            2,
            1,
        ),
        t(&[2, 2, 2, 2, 2], &[2, 4, 4, 4, 4, 4, 4, 4], 0, 2, 1, 2, 1),
        t(&[1, 1, 3], &[1, 1, 3, 3, 3], 5, 0, 3, 1, 2),
    ];
    for (i, test) in tests.into_iter().enumerate() {
        let mut s1 = with_peers(&[1, 2, 3]);
        s1.append(&test.leader_log).unwrap();
        let last = test.leader_log.last().unwrap().clone();
        s1.set_hard_state(HardState {
            term: last.term - 1,
            commit: last.index,
            vote: 0,
        })
        .unwrap();
        let mut n1 = new_test_raft(1, 10, 1, s1);
        n1.become_candidate();
        n1.become_leader();

        let mut s2 = with_peers(&[1, 2, 3]);
        s2.append(&test.follower_log).unwrap();
        s2.set_hard_state(HardState {
            term: last.term,
            vote: 1,
            commit: 0,
        })
        .unwrap();
        let mut n2 = new_test_raft(2, 10, 1, s2);
        if test.follower_compact != 0 {
            n2.raft_log.storage.compact(test.follower_compact).unwrap();
        }

        assert!(n2.step(m(1, 2, MessageType::MsgHeartbeat)).is_ok(), "#{i}");
        let mut msgs = n2.read_messages();
        assert_eq!(msgs.len(), 1, "#{i}: can't read 1 message from peer 2");
        assert_eq!(MessageType::MsgHeartbeatResp, msgs[0].msg_type, "#{i}");

        assert!(n1.step(msgs.remove(0)).is_ok(), "#{i}");
        let mut msgs = n1.read_messages();
        assert_eq!(msgs.len(), 1, "#{i}: can't read 1 message from peer 1");
        assert_eq!(MessageType::MsgApp, msgs[0].msg_type, "#{i}");

        assert!(
            n2.step(msgs.remove(0)).is_ok(),
            "#{i}: peer 2 step append fail"
        );
        let mut msgs = n2.read_messages();
        assert_eq!(msgs.len(), 1, "#{i}");
        assert_eq!(MessageType::MsgAppResp, msgs[0].msg_type, "#{i}");
        assert!(
            msgs[0].reject,
            "#{i}: expected rejected append response from peer 2"
        );
        assert_eq!(
            test.reject_hint_term, msgs[0].log_term,
            "#{i}: hint log term mismatch"
        );
        assert_eq!(
            test.reject_hint_index, msgs[0].reject_hint,
            "#{i}: hint log index mismatch"
        );

        assert!(
            n1.step(msgs.remove(0)).is_ok(),
            "#{i}: peer 1 step append fail"
        );
        let msgs = n1.read_messages();
        assert_eq!(test.next_append_term, msgs[0].log_term, "#{i}");
        assert_eq!(test.next_append_index, msgs[0].index, "#{i}");
    }
}

#[test]
fn test_log_replication_with_reordered_message() {
    let mut r1 = new_test_raft(1, 10, 1, with_peers(&[1, 2]));
    r1.become_candidate();
    r1.become_leader();
    r1.read_messages();
    r1.trk.progress.get_mut(&2).unwrap().become_replicate();

    let mut r2 = new_test_raft(2, 10, 1, with_peers(&[1, 2]));

    must_append_entry(&mut r1, data_entry());
    r1.send_append(2);
    let req1 = expect_one_message(&mut r1);
    must_append_entry(&mut r1, data_entry());
    r1.send_append(2);
    let req2 = expect_one_message(&mut r1);

    let _ = r2.step(req2);
    let resp2 = expect_one_message(&mut r2);
    assert!(resp2.reject);
    assert_eq!(0, resp2.reject_hint);
    assert_eq!(2, resp2.index);

    let _ = r2.step(req1);
    let msg = expect_one_message(&mut r2);
    assert!(!msg.reject);
    assert_eq!(2, msg.index);
    let _ = r1.step(msg);
    let _ = expect_one_message(&mut r1);
    assert_eq!(2, r1.trk.progress[&2].match_);

    let _ = r1.step(m(2, 1, MessageType::MsgUnreachable));
    assert_eq!(PrState::StateProbe, r1.trk.progress[&2].state);

    let _ = r1.step(resp2);
    let msg = expect_one_message(&mut r1);
    assert_eq!(r1.trk.progress[&2].match_, msg.index);
}
