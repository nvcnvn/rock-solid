// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// raft_paper_test.go. Licensed under the Apache License 2.0.
//
// Tests for the scenarios of the raft paper (https://raft.github.io/raft.pdf),
// one function per Go test, same names in snake case.

use std::cell::Cell;
use std::collections::BTreeMap;

use crate::raft::{NONE, StateType};
use crate::raftpb::{Entry, HardState, Message, MessageType};
use crate::testutil::*;

fn msg(from: u64, to: u64, t: MessageType) -> Message {
    Message {
        from,
        to,
        msg_type: t,
        ..Default::default()
    }
}

fn data_entry(data: &str) -> Entry {
    Entry {
        data: data.as_bytes().to_vec(),
        ..Default::default()
    }
}

#[test]
fn test_follower_update_term_from_message() {
    test_update_term_from_message(StateType::StateFollower);
}
#[test]
fn test_candidate_update_term_from_message() {
    test_update_term_from_message(StateType::StateCandidate);
}
#[test]
fn test_leader_update_term_from_message() {
    test_update_term_from_message(StateType::StateLeader);
}

fn test_update_term_from_message(state: StateType) {
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    match state {
        StateType::StateFollower => r.become_follower(1, 2),
        StateType::StateCandidate => r.become_candidate(),
        StateType::StateLeader => {
            r.become_candidate();
            r.become_leader();
        }
        StateType::StatePreCandidate => {}
    }
    let _ = r.step(Message {
        msg_type: MessageType::MsgApp,
        term: 2,
        ..Default::default()
    });
    assert_eq!(2, r.term);
    assert_eq!(StateType::StateFollower, r.state);
}

thread_local! {
    static CALLED: Cell<bool> = const { Cell::new(false) };
}

fn fake_step(_: &mut TestRaft, _: Message) -> Result<(), crate::Error> {
    CALLED.with(|c| c.set(true));
    Ok(())
}

#[test]
fn test_reject_stale_term_message() {
    CALLED.with(|c| c.set(false));
    let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    r.set_step(fake_step);
    r.load_state(&HardState {
        term: 2,
        ..Default::default()
    });
    let _ = r.step(Message {
        msg_type: MessageType::MsgApp,
        term: r.term - 1,
        ..Default::default()
    });
    assert!(!CALLED.with(|c| c.get()));
}

#[test]
fn test_start_as_follower() {
    let r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
    assert_eq!(StateType::StateFollower, r.state);
}

#[test]
fn test_leader_bcast_beat() {
    let hi = 1;
    let mut r = new_test_raft(1, 10, hi, with_peers(&[1, 2, 3]));
    r.become_candidate();
    r.become_leader();
    for i in 0..10 {
        must_append_entry(
            &mut r,
            vec![Entry {
                index: i + 1,
                ..Default::default()
            }],
        );
    }
    for _ in 0..hi {
        r.tick();
    }
    let mut msgs = r.read_messages();
    sort_messages(&mut msgs);
    assert_eq!(
        vec![
            Message {
                from: 1,
                to: 2,
                term: 1,
                msg_type: MessageType::MsgHeartbeat,
                commit: 0,
                ..Default::default()
            },
            Message {
                from: 1,
                to: 3,
                term: 1,
                msg_type: MessageType::MsgHeartbeat,
                commit: 0,
                ..Default::default()
            },
        ],
        msgs
    );
}

#[test]
fn test_follower_start_election() {
    test_nonleader_start_election(StateType::StateFollower);
}
#[test]
fn test_candidate_start_new_election() {
    test_nonleader_start_election(StateType::StateCandidate);
}

fn test_nonleader_start_election(state: StateType) {
    let et = 10;
    let mut r = new_test_raft(1, et, 1, with_peers(&[1, 2, 3]));
    match state {
        StateType::StateFollower => r.become_follower(1, 2),
        StateType::StateCandidate => r.become_candidate(),
        _ => {}
    }
    for _ in 1..2 * et {
        r.tick();
    }
    r.advance_messages_after_append();
    assert_eq!(2, r.term);
    assert_eq!(StateType::StateCandidate, r.state);
    assert_eq!(Some(&true), r.trk.votes.get(&r.id));
    let mut msgs = r.read_messages();
    sort_messages(&mut msgs);
    assert_eq!(
        vec![
            Message {
                from: 1,
                to: 2,
                term: 2,
                msg_type: MessageType::MsgVote,
                index: 0,
                log_term: 0,
                ..Default::default()
            },
            Message {
                from: 1,
                to: 3,
                term: 2,
                msg_type: MessageType::MsgVote,
                index: 0,
                log_term: 0,
                ..Default::default()
            },
        ],
        msgs
    );
}

#[test]
fn test_leader_election_in_one_round_rpc() {
    let tests: Vec<(usize, Vec<(u64, bool)>, StateType)> = vec![
        (1, vec![], StateType::StateLeader),
        (3, vec![(2, true), (3, true)], StateType::StateLeader),
        (3, vec![(2, true)], StateType::StateLeader),
        (
            5,
            vec![(2, true), (3, true), (4, true), (5, true)],
            StateType::StateLeader,
        ),
        (
            5,
            vec![(2, true), (3, true), (4, true)],
            StateType::StateLeader,
        ),
        (5, vec![(2, true), (3, true)], StateType::StateLeader),
        (3, vec![(2, false), (3, false)], StateType::StateFollower),
        (
            5,
            vec![(2, false), (3, false), (4, false), (5, false)],
            StateType::StateFollower,
        ),
        (
            5,
            vec![(2, true), (3, false), (4, false), (5, false)],
            StateType::StateFollower,
        ),
        (3, vec![], StateType::StateCandidate),
        (5, vec![(2, true)], StateType::StateCandidate),
        (5, vec![(2, false), (3, false)], StateType::StateCandidate),
        (5, vec![], StateType::StateCandidate),
    ];
    for (i, (size, votes, state)) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&ids_by_size(size)));
        let _ = r.step(msg(1, 1, MessageType::MsgHup));
        r.advance_messages_after_append();
        for (id, vote) in votes {
            let _ = r.step(Message {
                from: id,
                to: 1,
                term: r.term,
                msg_type: MessageType::MsgVoteResp,
                reject: !vote,
                ..Default::default()
            });
        }
        assert_eq!(state, r.state, "#{i}");
        assert_eq!(1, r.term, "#{i}");
    }
}

#[test]
fn test_follower_vote() {
    let tests = [
        (NONE, 2, false),
        (NONE, 3, false),
        (2, 2, false),
        (3, 3, false),
        (2, 3, true),
        (3, 2, true),
    ];
    for (i, (vote, nvote, wreject)) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        r.load_state(&HardState {
            term: 1,
            vote,
            ..Default::default()
        });
        let _ = r.step(Message {
            from: nvote,
            to: 1,
            term: 1,
            msg_type: MessageType::MsgVote,
            ..Default::default()
        });
        let expected = Message {
            from: 1,
            to: nvote,
            term: 1,
            msg_type: MessageType::MsgVoteResp,
            reject: wreject,
            ..Default::default()
        };
        assert_eq!(vec![expected], r.msgs_after_append, "#{i}");
    }
}

#[test]
fn test_candidate_fallback() {
    let tests = [
        Message {
            from: 2,
            to: 1,
            term: 1,
            msg_type: MessageType::MsgApp,
            ..Default::default()
        },
        Message {
            from: 2,
            to: 1,
            term: 2,
            msg_type: MessageType::MsgApp,
            ..Default::default()
        },
    ];
    for (i, tt) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        let _ = r.step(msg(1, 1, MessageType::MsgHup));
        assert_eq!(StateType::StateCandidate, r.state, "#{i}");
        let term = tt.term;
        let _ = r.step(tt);
        assert_eq!(StateType::StateFollower, r.state, "#{i}");
        assert_eq!(term, r.term, "#{i}");
    }
}

#[test]
fn test_follower_election_timeout_randomized() {
    test_nonleader_election_timeout_randomized(StateType::StateFollower);
}
#[test]
fn test_candidate_election_timeout_randomized() {
    test_nonleader_election_timeout_randomized(StateType::StateCandidate);
}

fn test_nonleader_election_timeout_randomized(state: StateType) {
    let et = 10;
    let mut r = new_test_raft(1, et, 1, with_peers(&[1, 2, 3]));
    let mut timeouts = BTreeMap::new();
    for _ in 0..50 * et {
        match state {
            StateType::StateFollower => {
                let t = r.term + 1;
                r.become_follower(t, 2);
            }
            StateType::StateCandidate => r.become_candidate(),
            _ => {}
        }
        let mut time = 0;
        while r.read_messages().is_empty() {
            r.tick();
            time += 1;
        }
        timeouts.insert(time, true);
    }
    for d in et..2 * et {
        assert!(
            timeouts.contains_key(&d),
            "timeout in {d} ticks should happen"
        );
    }
}

#[test]
fn test_followers_election_timeout_nonconflict() {
    test_nonleaders_election_timeout_nonconflict(StateType::StateFollower);
}
#[test]
fn test_candidates_election_timeout_nonconflict() {
    test_nonleaders_election_timeout_nonconflict(StateType::StateCandidate);
}

fn test_nonleaders_election_timeout_nonconflict(state: StateType) {
    let et = 10;
    let size = 5;
    let ids = ids_by_size(size);
    let mut rs: Vec<TestRaft> = ids
        .iter()
        .map(|&id| new_test_raft(id, et, 1, with_peers(&ids)))
        .collect();
    let mut conflicts = 0;
    for _ in 0..1000 {
        for r in rs.iter_mut() {
            match state {
                StateType::StateFollower => {
                    let t = r.term + 1;
                    r.become_follower(t, NONE);
                }
                StateType::StateCandidate => r.become_candidate(),
                _ => {}
            }
        }
        let mut timeout_num = 0;
        while timeout_num == 0 {
            for r in rs.iter_mut() {
                r.tick();
                if !r.read_messages().is_empty() {
                    timeout_num += 1;
                }
            }
        }
        if timeout_num > 1 {
            conflicts += 1;
        }
    }
    assert!(conflicts as f64 / 1000.0 <= 0.3, "conflicts {conflicts}");
}

#[test]
fn test_leader_start_replication() {
    let s = with_peers(&[1, 2, 3]);
    let mut r = new_test_raft(1, 10, 1, s);
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r);
    let li = r.raft_log.last_index();
    let ents = vec![data_entry("some data")];
    let _ = r.step(Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgProp,
        entries: ents,
        ..Default::default()
    });
    assert_eq!(li + 1, r.raft_log.last_index());
    assert_eq!(li, r.raft_log.committed);
    let mut msgs = r.read_messages();
    sort_messages(&mut msgs);
    let wents = vec![Entry {
        index: li + 1,
        term: 1,
        data: b"some data".to_vec(),
        ..Default::default()
    }];
    let app = |to| Message {
        from: 1,
        to,
        term: 1,
        msg_type: MessageType::MsgApp,
        index: li,
        log_term: 1,
        entries: wents.clone(),
        commit: li,
        ..Default::default()
    };
    assert_eq!(vec![app(2), app(3)], msgs);
    assert_eq!(wents, r.raft_log.next_unstable_ents());
}

#[test]
fn test_leader_commit_entry() {
    let s = with_peers(&[1, 2, 3]);
    let mut r = new_test_raft(1, 10, 1, s);
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r);
    let li = r.raft_log.last_index();
    let _ = r.step(Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgProp,
        entries: vec![data_entry("some data")],
        ..Default::default()
    });
    for m in r.read_messages() {
        let _ = r.step(accept_and_reply(&m));
    }
    assert_eq!(li + 1, r.raft_log.committed);
    assert_eq!(
        vec![Entry {
            index: li + 1,
            term: 1,
            data: b"some data".to_vec(),
            ..Default::default()
        }],
        r.raft_log.next_committed_ents(true)
    );
    let mut msgs = r.read_messages();
    sort_messages(&mut msgs);
    for (i, m) in msgs.iter().enumerate() {
        assert_eq!(i as u64 + 2, m.to);
        assert_eq!(MessageType::MsgApp, m.msg_type);
        assert_eq!(li + 1, m.commit);
    }
}

#[test]
fn test_leader_acknowledge_commit() {
    let tests: Vec<(usize, Vec<u64>, bool)> = vec![
        (1, vec![], true),
        (3, vec![], false),
        (3, vec![2], true),
        (3, vec![2, 3], true),
        (5, vec![], false),
        (5, vec![2], false),
        (5, vec![2, 3], true),
        (5, vec![2, 3, 4], true),
        (5, vec![2, 3, 4, 5], true),
    ];
    for (i, (size, acceptors, wack)) in tests.into_iter().enumerate() {
        let s = with_peers(&ids_by_size(size));
        let mut r = new_test_raft(1, 10, 1, s);
        r.become_candidate();
        r.become_leader();
        commit_noop_entry(&mut r);
        let li = r.raft_log.last_index();
        let _ = r.step(Message {
            from: 1,
            to: 1,
            msg_type: MessageType::MsgProp,
            entries: vec![data_entry("some data")],
            ..Default::default()
        });
        r.advance_messages_after_append();
        for m in r.msgs.clone() {
            if acceptors.contains(&m.to) {
                let _ = r.step(accept_and_reply(&m));
            }
        }
        assert_eq!(wack, r.raft_log.committed > li, "#{i}");
    }
}

#[test]
fn test_leader_commit_preceding_entries() {
    let tests: Vec<Vec<Entry>> = vec![
        vec![],
        vec![Entry {
            term: 2,
            index: 1,
            ..Default::default()
        }],
        vec![
            Entry {
                term: 1,
                index: 1,
                ..Default::default()
            },
            Entry {
                term: 2,
                index: 2,
                ..Default::default()
            },
        ],
        vec![Entry {
            term: 1,
            index: 1,
            ..Default::default()
        }],
    ];
    for (i, tt) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1, 2, 3]);
        storage.append(&tt).unwrap();
        let mut r = new_test_raft(1, 10, 1, storage);
        r.load_state(&HardState {
            term: 2,
            ..Default::default()
        });
        r.become_candidate();
        r.become_leader();
        let _ = r.step(Message {
            from: 1,
            to: 1,
            msg_type: MessageType::MsgProp,
            entries: vec![data_entry("some data")],
            ..Default::default()
        });
        for m in r.read_messages() {
            let _ = r.step(accept_and_reply(&m));
        }
        let li = tt.len() as u64;
        let mut want = tt.clone();
        want.push(Entry {
            term: 3,
            index: li + 1,
            ..Default::default()
        });
        want.push(Entry {
            term: 3,
            index: li + 2,
            data: b"some data".to_vec(),
            ..Default::default()
        });
        assert_eq!(want, r.raft_log.next_committed_ents(true), "#{i}");
    }
}

#[test]
fn test_follower_commit_entry() {
    let e = |index, data: &str| Entry {
        term: 1,
        index,
        data: data.as_bytes().to_vec(),
        ..Default::default()
    };
    let tests = vec![
        (vec![e(1, "some data")], 1),
        (vec![e(1, "some data"), e(2, "some data2")], 2),
        (vec![e(1, "some data2"), e(2, "some data")], 2),
        (vec![e(1, "some data"), e(2, "some data2")], 1),
    ];
    for (i, (ents, commit)) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        r.become_follower(1, 2);
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgApp,
            term: 1,
            entries: ents.clone(),
            commit,
            ..Default::default()
        });
        assert_eq!(commit, r.raft_log.committed, "#{i}");
        assert_eq!(
            ents[..commit as usize].to_vec(),
            r.raft_log.next_committed_ents(true),
            "#{i}"
        );
    }
}

#[test]
fn test_follower_check_msg_app() {
    let ents = vec![
        Entry {
            term: 1,
            index: 1,
            ..Default::default()
        },
        Entry {
            term: 2,
            index: 2,
            ..Default::default()
        },
    ];
    let tests = [
        (0, 0, 1, false, 0, 0),
        (ents[0].term, ents[0].index, 1, false, 0, 0),
        (ents[1].term, ents[1].index, 2, false, 0, 0),
        (ents[0].term, ents[1].index, ents[1].index, true, 1, 1),
        (
            ents[1].term + 1,
            ents[1].index + 1,
            ents[1].index + 1,
            true,
            2,
            2,
        ),
    ];
    for (i, (term, index, windex, wreject, wreject_hint, wlogterm)) in tests.into_iter().enumerate()
    {
        let mut storage = with_peers(&[1, 2, 3]);
        storage.append(&ents).unwrap();
        let mut r = new_test_raft(1, 10, 1, storage);
        r.load_state(&HardState {
            commit: 1,
            ..Default::default()
        });
        r.become_follower(2, 2);
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgApp,
            term: 2,
            log_term: term,
            index,
            ..Default::default()
        });
        let mut expected = Message {
            from: 1,
            to: 2,
            msg_type: MessageType::MsgAppResp,
            term: 2,
            index: windex,
            ..Default::default()
        };
        if wreject {
            expected.reject = true;
            expected.reject_hint = wreject_hint;
            expected.log_term = wlogterm;
        }
        assert_eq!(vec![expected], r.read_messages(), "#{i}");
    }
}

#[test]
fn test_follower_append_entries() {
    let tests = vec![
        (
            2,
            2,
            index_terms(3, &[3]),
            index_terms(1, &[1, 2, 3]),
            index_terms(3, &[3]),
        ),
        (
            1,
            1,
            index_terms(2, &[3, 4]),
            index_terms(1, &[1, 3, 4]),
            index_terms(2, &[3, 4]),
        ),
        (0, 0, index_terms(1, &[1]), index_terms(1, &[1, 2]), vec![]),
        (
            0,
            0,
            index_terms(1, &[3]),
            index_terms(1, &[3]),
            index_terms(1, &[3]),
        ),
    ];
    for (i, (index, term, ents, wents, wunstable)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1, 2, 3]);
        storage
            .append(&[
                Entry {
                    term: 1,
                    index: 1,
                    ..Default::default()
                },
                Entry {
                    term: 2,
                    index: 2,
                    ..Default::default()
                },
            ])
            .unwrap();
        let mut r = new_test_raft(1, 10, 1, storage);
        r.become_follower(2, 2);
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgApp,
            term: 2,
            log_term: term,
            index,
            entries: ents,
            ..Default::default()
        });
        assert_eq!(wents, r.raft_log.all_entries(), "#{i}");
        assert_eq!(wunstable, r.raft_log.next_unstable_ents(), "#{i}");
    }
}

#[test]
fn test_leader_sync_follower_log() {
    let ents = index_terms(0, &[0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6]);
    let term = 8;
    let tests = vec![
        index_terms(0, &[0, 1, 1, 1, 4, 4, 5, 5, 6, 6]),
        index_terms(0, &[0, 1, 1, 1, 4, 4]),
        index_terms(0, &[0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        index_terms(0, &[0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        index_terms(0, &[0, 1, 1, 1, 4, 4, 4, 4]),
        index_terms(0, &[0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
    ];
    for (i, tt) in tests.into_iter().enumerate() {
        let mut lead_storage = with_peers(&[1, 2, 3]);
        lead_storage.append(&ents).unwrap();
        let mut lead = new_test_raft(1, 10, 1, lead_storage);
        let li = lead.raft_log.last_index();
        lead.load_state(&HardState {
            commit: li,
            term,
            ..Default::default()
        });
        let mut follower_storage = with_peers(&[1, 2, 3]);
        follower_storage.append(&tt).unwrap();
        let mut follower = new_test_raft(2, 10, 1, follower_storage);
        follower.load_state(&HardState {
            term: term - 1,
            ..Default::default()
        });
        let mut n = new_network(vec![
            Some(StateMachine::Raft(Box::new(lead))),
            Some(StateMachine::Raft(Box::new(follower))),
            Some(StateMachine::BlackHole),
        ]);
        n.send(vec![msg(1, 1, MessageType::MsgHup)]);
        n.send(vec![Message {
            from: 3,
            to: 1,
            msg_type: MessageType::MsgVoteResp,
            term: term + 1,
            ..Default::default()
        }]);
        n.send(vec![Message {
            from: 1,
            to: 1,
            msg_type: MessageType::MsgProp,
            entries: vec![Entry::default()],
            ..Default::default()
        }]);
        let a = ltoa(&n.peers.get_mut(&1).unwrap().raft().raft_log);
        let b = ltoa(&n.peers.get_mut(&2).unwrap().raft().raft_log);
        assert_eq!(a, b, "#{i}");
    }
}

#[test]
fn test_vote_request() {
    let tests = vec![(index_terms(1, &[1]), 2), (index_terms(1, &[1, 2]), 3)];
    for (j, (ents, wterm)) in tests.into_iter().enumerate() {
        let mut r = new_test_raft(1, 10, 1, with_peers(&[1, 2, 3]));
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgApp,
            term: wterm - 1,
            log_term: 0,
            index: 0,
            entries: ents.clone(),
            ..Default::default()
        });
        r.read_messages();
        for _ in 1..r.election_timeout() * 2 {
            r.tick_election();
        }
        let mut msgs = r.read_messages();
        sort_messages(&mut msgs);
        assert_eq!(2, msgs.len(), "#{j}");
        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(MessageType::MsgVote, m.msg_type, "#{j}.{i}");
            assert_eq!(i as u64 + 2, m.to, "#{j}.{i}");
            assert_eq!(wterm, m.term, "#{j}.{i}");
            let last = ents.last().unwrap();
            assert_eq!(last.index, m.index, "#{j}.{i}");
            assert_eq!(last.term, m.log_term, "#{j}.{i}");
        }
    }
}

#[test]
fn test_voter() {
    let tests = vec![
        (index_terms(1, &[1]), 1, 1, false),
        (index_terms(1, &[1]), 1, 2, false),
        (index_terms(1, &[1, 1]), 1, 1, true),
        (index_terms(1, &[1]), 2, 1, false),
        (index_terms(1, &[1]), 2, 2, false),
        (
            vec![
                Entry {
                    term: 1,
                    index: 1,
                    ..Default::default()
                },
                Entry {
                    term: 1,
                    index: 2,
                    ..Default::default()
                },
            ],
            2,
            1,
            false,
        ),
        (index_terms(1, &[2]), 1, 1, true),
        (index_terms(1, &[2]), 1, 2, true),
        (index_terms(1, &[2, 2]), 1, 1, true),
        (index_terms(1, &[1, 1]), 1, 1, true),
    ];
    for (i, (ents, logterm, index, wreject)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1, 2]);
        storage.append(&ents).unwrap();
        let mut r = new_test_raft(1, 10, 1, storage);
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgVote,
            term: 3,
            log_term: logterm,
            index,
            ..Default::default()
        });
        let msgs = r.read_messages();
        assert_eq!(1, msgs.len(), "#{i}");
        assert_eq!(MessageType::MsgVoteResp, msgs[0].msg_type, "#{i}");
        assert_eq!(wreject, msgs[0].reject, "#{i}");
    }
}

#[test]
fn test_leader_only_commits_log_from_current_term() {
    let ents = vec![
        Entry {
            term: 1,
            index: 1,
            ..Default::default()
        },
        Entry {
            term: 2,
            index: 2,
            ..Default::default()
        },
    ];
    let tests = [(1, 0), (2, 0), (3, 3)];
    for (i, (index, wcommit)) in tests.into_iter().enumerate() {
        let mut storage = with_peers(&[1, 2]);
        storage.append(&ents).unwrap();
        let mut r = new_test_raft(1, 10, 1, storage);
        r.load_state(&HardState {
            term: 2,
            ..Default::default()
        });
        r.become_candidate();
        r.become_leader();
        r.read_messages();
        let _ = r.step(Message {
            from: 1,
            to: 1,
            msg_type: MessageType::MsgProp,
            entries: vec![Entry::default()],
            ..Default::default()
        });
        let _ = r.step(Message {
            from: 2,
            to: 1,
            msg_type: MessageType::MsgAppResp,
            term: r.term,
            index,
            ..Default::default()
        });
        r.advance_messages_after_append();
        assert_eq!(wcommit, r.raft_log.committed, "#{i}");
    }
}

fn commit_noop_entry(r: &mut TestRaft) {
    assert_eq!(
        r.state,
        StateType::StateLeader,
        "it should only be used when it is the leader"
    );
    r.bcast_append();
    let msgs = r.read_messages();
    for m in &msgs {
        if m.msg_type != MessageType::MsgApp
            || m.entries.len() != 1
            || !m.entries[0].data.is_empty()
        {
            panic!("not a message to append noop entry");
        }
        let _ = r.step(accept_and_reply(m));
    }
    r.read_messages();
    let ents = r.raft_log.next_unstable_ents().to_vec();
    r.raft_log.storage.append(&ents).unwrap();
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed, 0);
    let last = r.raft_log.last_entry_id();
    r.raft_log.stable_to(last);
}

fn accept_and_reply(m: &Message) -> Message {
    assert_eq!(m.msg_type, MessageType::MsgApp, "type should be MsgApp");
    Message {
        from: m.to,
        to: m.from,
        term: m.term,
        msg_type: MessageType::MsgAppResp,
        index: m.index + m.entries.len() as u64,
        ..Default::default()
    }
}
