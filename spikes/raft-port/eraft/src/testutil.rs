// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// test helpers of raft_test.go, log_test.go and diff_test.go. Licensed under
// the Apache License 2.0.
//
// Deviation: the network's random drops use a seeded xorshift instead of
// math/rand.

use std::collections::BTreeMap;

use crate::log::NO_LIMIT;
use crate::raft::{Config, Raft};
use crate::raftpb::{Entry, EntryType, Message, MessageType};
use crate::storage::MemoryStorage;
use crate::tracker::{Progress, ProgressTracker};

pub type TestRaft = Raft<MemoryStorage>;

pub fn new_test_config(id: u64, election: usize, heartbeat: usize) -> Config {
    Config {
        id,
        election_tick: election,
        heartbeat_tick: heartbeat,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_memory_storage(peers: &[u64], learners: &[u64]) -> MemoryStorage {
    let mut ms = MemoryStorage::new();
    ms.conf_state_mut().voters = peers.to_vec();
    ms.conf_state_mut().learners = learners.to_vec();
    ms
}

pub fn with_peers(peers: &[u64]) -> MemoryStorage {
    new_test_memory_storage(peers, &[])
}

pub fn new_test_raft(
    id: u64,
    election: usize,
    heartbeat: usize,
    storage: MemoryStorage,
) -> TestRaft {
    Raft::new(&mut new_test_config(id, election, heartbeat), storage)
}

pub fn ids_by_size(size: usize) -> Vec<u64> {
    (1..=size as u64).collect()
}

/// `index(i).terms(...)`: entries at consecutive indices with the given terms.
pub fn index_terms(i: u64, terms: &[u64]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(k, &term)| Entry {
            term,
            index: i + k as u64,
            ..Default::default()
        })
        .collect()
}

/// `index(i).termRange(from, to)`.
pub fn index_term_range(i: u64, from: u64, to: u64) -> Vec<Entry> {
    (from..to)
        .enumerate()
        .map(|(k, term)| Entry {
            term,
            index: i + k as u64,
            entry_type: EntryType::EntryNormal,
            ..Default::default()
        })
        .collect()
}

impl TestRaft {
    pub fn read_messages(&mut self) -> Vec<Message> {
        self.advance_messages_after_append();
        std::mem::take(&mut self.msgs)
    }

    pub fn advance_messages_after_append(&mut self) {
        loop {
            let msgs = self.take_messages_after_append();
            if msgs.is_empty() {
                break;
            }
            let _ = self.step_or_send(msgs);
        }
    }

    pub fn take_messages_after_append(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.msgs_after_append)
    }

    pub fn step_or_send(&mut self, msgs: Vec<Message>) -> Result<(), crate::Error> {
        for m in msgs {
            if m.to == self.id {
                self.step(m)?;
            } else {
                self.msgs.push(m);
            }
        }
        Ok(())
    }
}

pub fn must_append_entry(r: &mut TestRaft, ents: Vec<Entry>) {
    assert!(r.append_entry(ents), "entry unexpectedly dropped");
}

/// `nextEnts`: persists unstable entries and returns the appliable entries.
pub fn next_ents(r: &mut TestRaft) -> Vec<Entry> {
    let ents = r.raft_log.next_unstable_ents().to_vec();
    r.raft_log.storage.append(&ents).unwrap();
    let last = r.raft_log.last_entry_id();
    r.raft_log.stable_to(last);
    r.advance_messages_after_append();
    let ents = r.raft_log.next_committed_ents(true);
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed, 0);
    ents
}

pub fn ltoa(l: &crate::log::RaftLog<MemoryStorage>) -> String {
    let mut s = format!("lastIndex: {}\n", l.last_index());
    s += &format!("applied:  {}\n", l.applied);
    s += &format!("applying:  {}\n", l.applying);
    for (i, e) in l.all_entries().iter().enumerate() {
        s += &format!("#{i}: {e:?}\n");
    }
    s
}

/// Sorts messages by (to, type, from) like Go's messageSlice.
pub fn sort_messages(msgs: &mut [Message]) {
    msgs.sort_by_key(|m| (m.to, m.msg_type, m.from));
}

pub enum StateMachine {
    Raft(Box<TestRaft>),
    BlackHole,
}

impl StateMachine {
    fn step(&mut self, m: Message) -> Result<(), crate::Error> {
        match self {
            StateMachine::Raft(r) => r.step(m),
            StateMachine::BlackHole => Ok(()),
        }
    }
    fn read_messages(&mut self) -> Vec<Message> {
        match self {
            StateMachine::Raft(r) => r.read_messages(),
            StateMachine::BlackHole => Vec::new(),
        }
    }
    fn advance_messages_after_append(&mut self) {
        if let StateMachine::Raft(r) = self {
            r.advance_messages_after_append();
        }
    }
    pub fn raft(&mut self) -> &mut TestRaft {
        match self {
            StateMachine::Raft(r) => r,
            StateMachine::BlackHole => panic!("not a raft"),
        }
    }
}

pub type MsgHook = Box<dyn FnMut(&Message) -> bool>;

pub struct Network {
    pub peers: BTreeMap<u64, StateMachine>,
    dropm: BTreeMap<(u64, u64), f64>,
    ignorem: BTreeMap<MessageType, bool>,
    pub msg_hook: Option<MsgHook>,
    rng: u64,
}

pub fn new_network(peers: Vec<Option<StateMachine>>) -> Network {
    new_network_with_config(None, peers)
}

pub fn new_network_with_config(
    config_func: Option<&dyn Fn(&mut Config)>,
    peers: Vec<Option<StateMachine>>,
) -> Network {
    let size = peers.len();
    let peer_addrs = ids_by_size(size);
    let mut npeers = BTreeMap::new();
    for (j, p) in peers.into_iter().enumerate() {
        let id = peer_addrs[j];
        match p {
            None => {
                let storage = with_peers(&peer_addrs);
                let mut cfg = new_test_config(id, 10, 1);
                if let Some(f) = config_func {
                    f(&mut cfg);
                }
                npeers.insert(
                    id,
                    StateMachine::Raft(Box::new(Raft::new(&mut cfg, storage))),
                );
            }
            Some(StateMachine::Raft(mut v)) => {
                let learners = v.trk.config.learners.clone();
                v.id = id;
                v.trk = ProgressTracker::new(v.trk.max_inflight, v.trk.max_inflight_bytes);
                for &pid in &peer_addrs {
                    let mut pr = Progress::default();
                    if learners.contains(&pid) {
                        pr.is_learner = true;
                        v.trk.config.learners.insert(pid);
                    } else {
                        v.trk.config.voters.0[0].insert(pid);
                    }
                    v.trk.progress.insert(pid, pr);
                }
                let term = v.term;
                v.reset(term);
                npeers.insert(id, StateMachine::Raft(v));
            }
            Some(StateMachine::BlackHole) => {
                npeers.insert(id, StateMachine::BlackHole);
            }
        }
    }
    Network {
        peers: npeers,
        dropm: BTreeMap::new(),
        ignorem: BTreeMap::new(),
        msg_hook: None,
        rng: 0x9e3779b97f4a7c15,
    }
}

pub fn pre_vote_config(c: &mut Config) {
    c.pre_vote = true;
}

impl Network {
    pub fn send(&mut self, msgs: Vec<Message>) {
        let mut queue: std::collections::VecDeque<Message> = msgs.into();
        while let Some(m) = queue.pop_front() {
            let p = self.peers.get_mut(&m.to).expect("peer");
            let _ = p.step(m);
            p.advance_messages_after_append();
            let out = p.read_messages();
            let filtered = self.filter(out);
            queue.extend(filtered);
        }
    }

    pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert((from, to), perc);
    }

    pub fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 2.0);
        self.drop(other, one, 2.0);
    }

    pub fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() as u64 {
            let nid = i + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    pub fn ignore(&mut self, t: MessageType) {
        self.ignorem.insert(t, true);
    }

    pub fn recover(&mut self) {
        self.dropm.clear();
        self.ignorem.clear();
    }

    fn rand_f64(&mut self) -> f64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as f64 / (1u64 << 53) as f64
    }

    fn filter(&mut self, msgs: Vec<Message>) -> Vec<Message> {
        let mut mm = Vec::new();
        for m in msgs {
            if self.ignorem.get(&m.msg_type).copied().unwrap_or(false) {
                continue;
            }
            if m.msg_type == MessageType::MsgHup {
                panic!("unexpected msgHup");
            }
            let perc = self.dropm.get(&(m.from, m.to)).copied().unwrap_or(0.0);
            if self.rand_f64() < perc {
                continue;
            }
            if let Some(h) = self.msg_hook.as_mut()
                && !h(&m)
            {
                continue;
            }
            mm.push(m);
        }
        mm
    }
}

pub fn new_test_learner_raft(
    id: u64,
    election: usize,
    heartbeat: usize,
    storage: MemoryStorage,
) -> TestRaft {
    new_test_raft(id, election, heartbeat, storage)
}

/// `newLog(&MemoryStorage{ents: ents}, nil)`: ents[0] is the dummy entry at index 0.
pub fn log_from_ents(ents: &[Entry]) -> crate::log::RaftLog<MemoryStorage> {
    assert_eq!(
        ents[0].index, 0,
        "log_from_ents expects a dummy entry at index 0"
    );
    let mut s = MemoryStorage::new();
    s.append(&ents[1..]).unwrap();
    crate::log::RaftLog::new(s, std::rc::Rc::new(crate::logger::NopLogger))
}

pub fn ents_with_config(config_func: Option<&dyn Fn(&mut Config)>, terms: &[u64]) -> TestRaft {
    let mut storage = MemoryStorage::new();
    for (i, &term) in terms.iter().enumerate() {
        storage
            .append(&[Entry {
                index: i as u64 + 1,
                term,
                ..Default::default()
            }])
            .unwrap();
    }
    let mut cfg = new_test_config(1, 5, 1);
    if let Some(f) = config_func {
        f(&mut cfg);
    }
    let mut sm = Raft::new(&mut cfg, storage);
    sm.reset(*terms.last().unwrap());
    sm
}

pub fn voted_with_config(
    config_func: Option<&dyn Fn(&mut Config)>,
    vote: u64,
    term: u64,
) -> TestRaft {
    let mut storage = MemoryStorage::new();
    storage
        .set_hard_state(crate::raftpb::HardState {
            vote,
            term,
            commit: 0,
        })
        .unwrap();
    let mut cfg = new_test_config(1, 5, 1);
    if let Some(f) = config_func {
        f(&mut cfg);
    }
    let mut sm = Raft::new(&mut cfg, storage);
    sm.reset(term);
    sm
}

pub fn expect_one_message(r: &mut TestRaft) -> Message {
    let mut msgs = r.read_messages();
    assert_eq!(msgs.len(), 1, "expect one message");
    msgs.remove(0)
}

pub fn raft_sm(r: TestRaft) -> Option<StateMachine> {
    Some(StateMachine::Raft(Box::new(r)))
}

pub fn black_hole() -> Option<StateMachine> {
    Some(StateMachine::BlackHole)
}

impl Network {
    pub fn raft(&mut self, id: u64) -> &mut TestRaft {
        self.peers.get_mut(&id).expect("peer").raft()
    }
}
