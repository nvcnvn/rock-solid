//! P9: break attempt. A seeded simulation of one raft group of 3 or 5 nodes
//! under message drop, delay, reordering and duplication, partitions, crashes
//! with restart from persisted MemoryStorage state (unstable entries lost),
//! log compaction with snapshot transfer, proposals and membership changes
//! (simple and joint, voters and learners). 30% of seeds use AsyncStorageWrites,
//! with append and apply threads that lag behind. After every tick it checks:
//!
//! - election safety: at most one leader per term;
//! - leader completeness: a leader of term T holds every entry reported
//!   committed by a node whose term was below T;
//! - committed entries never change: every node reports the same entry for a
//!   committed index;
//! - state machine safety: every node applies the same sequence (the running
//!   hash after each index matches, also across snapshots);
//! - log matching (every 50 ticks and at the end): two logs that agree on the
//!   term at an index agree on every entry up to it.
//!
//! Usage: cargo test --release --test sim -- [seeds] [first_seed] [ticks] [fault]
//! fault=1 is a negative control: crashed disks forget the vote.
//! guard=0 drops the application-side skip of stale applies in async mode
//! (reproduces regress/async_snapshot_overtakes_apply.txt).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use eraft::logger::{Logger, NopLogger};
use eraft::raftpb::{
    ConfChange, ConfChangeI, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
    ConfState, Entry, EntryType, Message, MessageType, Snapshot,
};
use eraft::{
    Config, LOCAL_APPEND_THREAD, LOCAL_APPLY_THREAD, MemoryStorage, RawNode, SnapshotStatus,
    StateType, Storage,
};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

struct R(ChaCha8Rng);

impl R {
    fn below(&mut self, n: u64) -> u64 {
        self.0.next_u64() % n
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.0.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

struct App {
    applied: u64,
    hash: u64,
    conf: ConfState,
}

struct Node {
    rn: Option<RawNode<MemoryStorage>>,
    disk: MemoryStorage,
    app: App,
    down_until: u64,
    append_q: VecDeque<Message>,
    apply_q: VecDeque<Message>,
}

struct InFlight {
    at: u64,
    m: Message,
}

#[derive(Default)]
struct Stats {
    commits: u64,
    leaders: u64,
    crashes: u64,
    snapshots_sent: u64,
    snapshots_applied: u64,
    conf_applied: u64,
    compactions: u64,
    partitions: u64,
    proposals: u64,
    skipped_applies: u64,
    async_seeds: u64,
}

struct Sim {
    rng: R,
    nodes: BTreeMap<u64, Node>,
    net: VecDeque<InFlight>,
    now: u64,
    partition: Option<(BTreeSet<u64>, u64)>,
    leader_of_term: BTreeMap<u64, u64>,
    committed: BTreeMap<u64, (Entry, u64)>,
    hash_at: BTreeMap<u64, u64>,
    logger: Rc<dyn Logger>,
    cfg: Config,
    seq: u64,
    st: Stats,
    pool: Vec<u64>,
    violations: Vec<String>,
    fault: bool,
    guard: bool,
}

fn mix(h: u64, e: &Entry) -> u64 {
    let mut x = h ^ e.index.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ e.term.rotate_left(17);
    for &b in &e.data {
        x = (x ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    x ^ (e.entry_type as u64).rotate_left(40)
}

fn get(v: &[Entry], idx: u64) -> &Entry {
    &v[(idx - v[0].index) as usize]
}

fn same_entry(a: &Entry, b: &Entry) -> bool {
    a.term == b.term && a.index == b.index && a.data == b.data && a.entry_type == b.entry_type
}

impl Sim {
    fn new(seed: u64, n: usize, fault: bool, guard: bool) -> Sim {
        let mut rng = R(ChaCha8Rng::seed_from_u64(seed));
        let cfg = Config {
            election_tick: 10,
            heartbeat_tick: 1 + rng.below(2) as usize,
            check_quorum: rng.chance(0.75),
            pre_vote: rng.chance(0.75),
            max_size_per_msg: [64, 1024, u64::MAX][rng.below(3) as usize],
            max_inflight_msgs: [2, 16, 256][rng.below(3) as usize],
            step_down_on_removal: rng.chance(0.5),
            async_storage_writes: rng.chance(0.3),
            rand_seed: seed,
            ..Default::default()
        };
        let voters: Vec<u64> = (1..=n as u64).collect();
        let pool: Vec<u64> = (1..=n as u64 + 2).collect();
        let mut sim = Sim {
            rng,
            nodes: BTreeMap::new(),
            net: VecDeque::new(),
            now: 0,
            partition: None,
            leader_of_term: BTreeMap::new(),
            committed: BTreeMap::new(),
            hash_at: BTreeMap::new(),
            logger: Rc::new(NopLogger),
            cfg,
            seq: 0,
            st: Stats::default(),
            pool,
            violations: Vec::new(),
            fault,
            guard,
        };
        let conf = ConfState {
            voters: voters.clone(),
            ..Default::default()
        };
        for &id in &sim.pool.clone() {
            let mut disk = MemoryStorage::new();
            let mut app = App {
                applied: 0,
                hash: 0,
                conf: ConfState::default(),
            };
            if voters.contains(&id) {
                let snap = Snapshot {
                    data: 0u64.to_le_bytes().to_vec(),
                    metadata: eraft::raftpb::SnapshotMetadata {
                        conf_state: conf.clone(),
                        index: 1,
                        term: 1,
                    },
                };
                disk.apply_snapshot(snap).unwrap();
                app = App {
                    applied: 1,
                    hash: 0,
                    conf: conf.clone(),
                };
            }
            sim.hash_at.insert(1, 0);
            sim.nodes.insert(
                id,
                Node {
                    rn: None,
                    disk,
                    app,
                    down_until: 0,
                    append_q: VecDeque::new(),
                    apply_q: VecDeque::new(),
                },
            );
            sim.start(id);
        }
        sim
    }

    fn start(&mut self, id: u64) {
        if std::env::var_os("SIM_DEBUG").is_some() {
            eprintln!("t={} node {id} start", self.now);
        }
        let node = self.nodes.get_mut(&id).unwrap();
        let snap = node.disk.snapshot().unwrap();
        let hash = if snap.data.len() >= 8 {
            u64::from_le_bytes(snap.data[..8].try_into().unwrap())
        } else {
            0
        };
        node.app = App {
            applied: snap.metadata.index,
            hash,
            conf: snap.metadata.conf_state.clone(),
        };
        let mut cfg = self.cfg.clone();
        cfg.id = id;
        cfg.applied = snap.metadata.index;
        cfg.logger = Some(self.logger.clone());
        cfg.rand_seed = self.cfg.rand_seed.wrapping_mul(31).wrapping_add(self.now);
        node.rn = Some(RawNode::new(&mut cfg, node.disk.clone()).unwrap());
    }

    fn violation(&mut self, s: String) {
        if self.violations.len() < 5 {
            self.violations.push(format!("tick {}: {s}", self.now));
        }
    }

    fn can_talk(&self, a: u64, b: u64) -> bool {
        match &self.partition {
            Some((side, _)) => side.contains(&a) == side.contains(&b),
            None => true,
        }
    }

    fn send(&mut self, msgs: Vec<Message>) {
        for m in msgs {
            if self.rng.chance(0.05) {
                if m.msg_type == MessageType::MsgSnap {
                    self.report_snap(m.from, m.to, SnapshotStatus::SnapshotFailure);
                }
                continue;
            }
            let copies = if self.rng.chance(0.02) { 2 } else { 1 };
            for _ in 0..copies {
                let at = self.now + self.rng.below(4);
                self.net.push_back(InFlight { at, m: m.clone() });
            }
            if m.msg_type == MessageType::MsgSnap {
                self.st.snapshots_sent += 1;
            }
        }
    }

    fn report_snap(&mut self, leader: u64, to: u64, status: SnapshotStatus) {
        if let Some(rn) = self.nodes.get_mut(&leader).and_then(|n| n.rn.as_mut()) {
            rn.report_snapshot(to, status);
        }
    }

    fn deliver(&mut self) {
        let mut keep = VecDeque::new();
        let mut due = Vec::new();
        for f in std::mem::take(&mut self.net) {
            if f.at <= self.now {
                due.push(f.m)
            } else {
                keep.push_back(f)
            }
        }
        self.net = keep;
        let n = due.len();
        for i in (1..n).rev() {
            let j = self.rng.below(i as u64 + 1) as usize;
            due.swap(i, j);
        }
        for m in due {
            let (from, to) = (m.from, m.to);
            let snap = m.msg_type == MessageType::MsgSnap;
            if !self.can_talk(from, to) {
                if snap {
                    self.report_snap(from, to, SnapshotStatus::SnapshotFailure);
                }
                continue;
            }
            let Some(rn) = self.nodes.get_mut(&to).and_then(|n| n.rn.as_mut()) else {
                if snap {
                    self.report_snap(from, to, SnapshotStatus::SnapshotFailure);
                }
                continue;
            };
            let _ = rn.step(m);
            if snap {
                self.report_snap(from, to, SnapshotStatus::SnapshotFinish);
            }
        }
    }

    fn process_ready(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).unwrap();
        let Some(rn) = node.rn.as_mut() else { return };
        if !rn.has_ready() {
            return;
        }
        let term = rn.raft.term;
        let mut rd = rn.ready();
        if self.cfg.async_storage_writes {
            let mut out = Vec::new();
            for m in std::mem::take(&mut rd.messages) {
                match m.to {
                    LOCAL_APPEND_THREAD => node.append_q.push_back(m),
                    LOCAL_APPLY_THREAD => node.apply_q.push_back(m),
                    _ => out.push(m),
                }
            }
            self.send(out);
            return;
        }
        if let Some(s) = rd.snapshot.clone() {
            rn.store_mut().apply_snapshot(s).unwrap();
        }
        if !rd.entries.is_empty() {
            rn.store_mut().append(&rd.entries).unwrap();
        }
        if let Some(hs) = rd.hard_state {
            rn.store_mut().set_hard_state(hs).unwrap();
        }
        let msgs = std::mem::take(&mut rd.messages);
        let committed = std::mem::take(&mut rd.committed_entries);
        let snap = rd.snapshot.take();
        if let Some(s) = snap {
            self.apply_snapshot_to_app(id, &s);
        }
        for e in committed {
            self.apply(id, term, e);
        }
        self.nodes
            .get_mut(&id)
            .unwrap()
            .rn
            .as_mut()
            .unwrap()
            .advance(rd);
        self.send(msgs);
    }

    /// Async storage writes: runs one MsgStorageAppend of the node's append thread.
    fn process_append_thread(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).unwrap();
        let (Some(rn), Some(mut m)) = (node.rn.as_mut(), node.append_q.pop_front()) else {
            return;
        };
        let s = rn.store_mut();
        let hs = eraft::raftpb::HardState {
            term: m.term,
            vote: m.vote,
            commit: m.commit,
        };
        if !eraft::is_empty_hard_state(Some(&hs)) {
            s.set_hard_state(hs).unwrap();
        }
        let snap = m.snapshot.take().map(|b| *b);
        if let Some(sn) = &snap {
            s.apply_snapshot(sn.clone()).unwrap();
        }
        s.append(&m.entries).unwrap();
        if let Some(sn) = snap {
            self.apply_snapshot_to_app(id, &sn);
        }
        self.deliver_responses(id, std::mem::take(&mut m.responses));
    }

    /// Async storage writes: runs one MsgStorageApply of the node's apply thread.
    fn process_apply_thread(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).unwrap();
        let (Some(rn), Some(mut m)) = (node.rn.as_ref(), node.apply_q.pop_front()) else {
            return;
        };
        let term = rn.raft.term;
        for e in std::mem::take(&mut m.entries) {
            self.apply(id, term, e);
        }
        self.deliver_responses(id, std::mem::take(&mut m.responses));
    }

    fn deliver_responses(&mut self, id: u64, resps: Vec<Message>) {
        let mut out = Vec::new();
        for r in resps {
            if r.to == id {
                if let Some(rn) = self.nodes.get_mut(&id).unwrap().rn.as_mut() {
                    let _ = rn.step(r);
                }
            } else {
                out.push(r);
            }
        }
        self.send(out);
    }

    fn apply_snapshot_to_app(&mut self, id: u64, s: &Snapshot) {
        if std::env::var_os("SIM_DEBUG").is_some() {
            eprintln!(
                "t={} node {id} snapshot idx={} conf={:?}",
                self.now, s.metadata.index, s.metadata.conf_state
            );
        }
        self.st.snapshots_applied += 1;
        let hash = u64::from_le_bytes(s.data[..8].try_into().unwrap());
        let idx = s.metadata.index;
        match self.hash_at.get(&idx) {
            Some(&h) if h != hash => self.violation(format!(
                "node {id} applied snapshot at {idx} with state {hash:x}, others had {h:x}"
            )),
            _ => {
                self.hash_at.insert(idx, hash);
            }
        }
        let node = self.nodes.get_mut(&id).unwrap();
        node.app = App {
            applied: idx,
            hash,
            conf: s.metadata.conf_state.clone(),
        };
    }

    fn apply(&mut self, id: u64, term: u64, e: Entry) {
        match self.committed.get(&e.index) {
            Some((c, _)) if !same_entry(c, &e) => {
                let msg = format!(
                    "node {id} committed {:?} at {}, another node committed {:?}",
                    e, e.index, c
                );
                self.violation(msg);
            }
            Some(_) => {}
            None => {
                self.committed.insert(e.index, (e.clone(), term));
                self.st.commits += 1;
            }
        }
        let node = self.nodes.get_mut(&id).unwrap();
        if self.cfg.async_storage_writes && self.guard {
            // Raft restores a snapshot when it steps MsgSnap, even while
            // older committed entries wait on the apply thread; applying a
            // stale conf change then panics (regress/async_snapshot_overtakes_apply.txt,
            // upstream raft.go:1860 restore, raft.go:1967 applyConfChange).
            // Skip entries the pending or installed snapshot covers.
            let rn = node.rn.as_ref().unwrap();
            let pending = rn
                .raft
                .raft_log
                .unstable
                .snapshot
                .as_ref()
                .map_or(0, |s| s.metadata.index);
            if e.index <= node.app.applied || e.index <= pending {
                self.st.skipped_applies += 1;
                return;
            }
        }
        if e.index != node.app.applied + 1 {
            let msg = format!("node {id} applied {} after {}", e.index, node.app.applied);
            self.violation(msg);
            return;
        }
        let rn = node.rn.as_mut().unwrap();
        if std::env::var_os("SIM_DEBUG").is_some() && e.entry_type != EntryType::EntryNormal {
            let cc = if e.entry_type == EntryType::EntryConfChange {
                format!("{:?}", ConfChange::decode(&e.data).unwrap().as_v2())
            } else {
                format!("{:?}", ConfChangeV2::decode(&e.data).unwrap())
            };
            eprintln!(
                "t={} node {id} apply idx={} term={} cfg_before={} raft_applied={} state={} cc={cc}",
                self.now,
                e.index,
                e.term,
                rn.raft.trk.config,
                rn.raft.raft_log.applied,
                rn.raft.state
            );
        }
        match e.entry_type {
            EntryType::EntryConfChange => {
                let cc = ConfChange::decode(&e.data).unwrap();
                node.app.conf = rn.apply_conf_change(&ConfChangeI::V1(cc));
                self.st.conf_applied += 1;
            }
            EntryType::EntryConfChangeV2 => {
                let cc = ConfChangeV2::decode(&e.data).unwrap();
                node.app.conf = rn.apply_conf_change(&ConfChangeI::V2(cc));
                self.st.conf_applied += 1;
            }
            EntryType::EntryNormal => {}
        }
        node.app.applied = e.index;
        node.app.hash = mix(node.app.hash, &e);
        let h = node.app.hash;
        match self.hash_at.get(&e.index) {
            Some(&want) if want != h => self.violation(format!(
                "node {id} state after {} is {h:x}, others had {want:x}",
                e.index
            )),
            Some(_) => {}
            None => {
                self.hash_at.insert(e.index, h);
            }
        }
    }

    fn leader(&self) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for (&id, n) in &self.nodes {
            if let Some(rn) = &n.rn {
                if rn.raft.state == StateType::StateLeader
                    && best.is_none_or(|(t, _)| rn.raft.term > t)
                {
                    best = Some((rn.raft.term, id));
                }
            }
        }
        best.map(|(_, id)| id)
    }

    fn propose_conf_change(&mut self) {
        let Some(l) = self.leader() else { return };
        let rn = self.nodes[&l].rn.as_ref().unwrap();
        let cfg = &rn.raft.trk.config;
        if !cfg.voters.0[1].is_empty() {
            let cc = ConfChangeV2::default();
            let _ = self
                .nodes
                .get_mut(&l)
                .unwrap()
                .rn
                .as_mut()
                .unwrap()
                .propose_conf_change(cc);
            return;
        }
        let voters: Vec<u64> = cfg.voters.0[0].iter().copied().collect();
        let learners: Vec<u64> = cfg.learners.iter().copied().collect();
        let pick = |r: &mut R, v: &[u64]| v[r.below(v.len() as u64) as usize];
        let mut changes = Vec::new();
        let n_changes = if self.rng.chance(0.3) { 2 } else { 1 };
        let mut touched = BTreeSet::new();
        let mut voters_after = voters.len() as i64;
        for _ in 0..n_changes {
            let id = pick(&mut self.rng, &self.pool);
            if !touched.insert(id) {
                continue;
            }
            let is_voter = voters.contains(&id);
            let is_learner = learners.contains(&id);
            let t = if is_voter {
                if voters_after > 3 && self.rng.chance(0.7) {
                    voters_after -= 1;
                    ConfChangeType::ConfChangeRemoveNode
                } else if voters_after > 3 {
                    voters_after -= 1;
                    ConfChangeType::ConfChangeAddLearnerNode
                } else {
                    continue;
                }
            } else if is_learner {
                if self.rng.chance(0.5) {
                    voters_after += 1;
                    ConfChangeType::ConfChangeAddNode
                } else {
                    ConfChangeType::ConfChangeRemoveNode
                }
            } else if self.rng.chance(0.5) {
                voters_after += 1;
                ConfChangeType::ConfChangeAddNode
            } else {
                ConfChangeType::ConfChangeAddLearnerNode
            };
            changes.push(ConfChangeSingle {
                change_type: t,
                node_id: id,
            });
        }
        if changes.is_empty() {
            return;
        }
        let rn = self.nodes.get_mut(&l).unwrap().rn.as_mut().unwrap();
        let _ = if changes.len() == 1 && self.rng.chance(0.3) {
            rn.propose_conf_change(ConfChange {
                change_type: changes[0].change_type,
                node_id: changes[0].node_id,
                ..Default::default()
            })
        } else {
            let transition = if self.rng.chance(0.2) {
                ConfChangeTransition::ConfChangeTransitionJointImplicit
            } else {
                ConfChangeTransition::ConfChangeTransitionAuto
            };
            rn.propose_conf_change(ConfChangeV2 {
                transition,
                changes,
                context: Vec::new(),
            })
        };
    }

    fn step_world(&mut self) {
        self.now += 1;
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        if let Some((_, until)) = &self.partition {
            if *until <= self.now {
                self.partition = None;
            }
        } else if self.rng.chance(0.01) {
            let side: BTreeSet<u64> = ids
                .iter()
                .copied()
                .filter(|_| self.rng.chance(0.5))
                .collect();
            let dur = 20 + self.rng.below(80);
            self.partition = Some((side, self.now + dur));
            self.st.partitions += 1;
        }
        for &id in &ids {
            let node = self.nodes.get_mut(&id).unwrap();
            if node.rn.is_none() {
                if node.down_until <= self.now {
                    self.start(id);
                }
                continue;
            }
            if self.rng.chance(0.004) {
                let node = self.nodes.get_mut(&id).unwrap();
                node.disk = node.rn.take().unwrap().store().clone();
                node.append_q.clear();
                node.apply_q.clear();
                if self.fault {
                    // Negative control: a disk that loses the vote on crash, as
                    // if the HardState fsync had been skipped.
                    let mut hs = node.disk.hard_state();
                    hs.vote = 0;
                    node.disk.set_hard_state(hs).unwrap();
                }
                node.down_until = self.now + 5 + self.rng.below(45);
                self.st.crashes += 1;
                continue;
            }
            self.nodes.get_mut(&id).unwrap().rn.as_mut().unwrap().tick();
        }
        self.deliver();
        if self.rng.chance(0.3) {
            let id = ids[self.rng.below(ids.len() as u64) as usize];
            self.seq += 1;
            let data = format!("p{}", self.seq).into_bytes();
            if let Some(rn) = self.nodes.get_mut(&id).unwrap().rn.as_mut() {
                if rn.propose(data).is_ok() {
                    self.st.proposals += 1;
                }
            }
        }
        if self.rng.chance(0.02) {
            self.propose_conf_change();
        }
        for &id in &ids {
            if self.rng.chance(0.7) {
                self.process_ready(id);
            }
            if self.rng.chance(0.6) {
                self.process_append_thread(id);
            }
            if self.rng.chance(0.6) {
                self.process_apply_thread(id);
            }
            if self.rng.chance(0.01) {
                self.compact(id);
            }
        }
        self.check();
        if self.now.is_multiple_of(50) {
            self.check_log_matching();
        }
    }

    fn compact(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).unwrap();
        let Some(rn) = node.rn.as_mut() else { return };
        // Never compact past the durable commit index: with async storage
        // writes the apply thread can run ahead of the HardState write, and a
        // snapshot above HardState.commit makes newRaft panic on restart
        // (loadState: "state.commit is out of range").
        let applied = node.app.applied;
        let fi = rn.store().first_index().unwrap();
        if applied < fi + 2 || applied > rn.store().hard_state().commit {
            return;
        }
        let data = node.app.hash.to_le_bytes().to_vec();
        let conf = node.app.conf.clone();
        let s = rn.store_mut();
        if s.create_snapshot(applied, Some(&conf), data).is_ok() {
            s.compact(applied).unwrap();
        }
        self.st.compactions += 1;
    }

    fn check(&mut self) {
        let mut found = Vec::new();
        let mut bad = Vec::new();
        for (&id, n) in &self.nodes {
            let Some(rn) = &n.rn else { continue };
            if rn.raft.state != StateType::StateLeader {
                continue;
            }
            let term = rn.raft.term;
            found.push((id, term));
            let log = &rn.raft.raft_log;
            let (fi, li) = (log.first_index(), log.last_index());
            for (&idx, (e, reported_term)) in self.committed.range(fi..) {
                if *reported_term >= term {
                    continue;
                }
                if idx > li {
                    bad.push(format!(
                        "leader {id} term {term} lacks committed index {idx} (last {li})"
                    ));
                    break;
                }
                if log.term(idx).ok() != Some(e.term) {
                    bad.push(format!(
                        "leader {id} term {term} has term {:?} at committed {idx}, want {}",
                        log.term(idx).ok(),
                        e.term
                    ));
                    break;
                }
            }
        }
        for b in bad {
            self.violation(b);
        }
        for (id, term) in found {
            match self.leader_of_term.get(&term) {
                Some(&l) if l != id => {
                    self.violation(format!("two leaders in term {term}: {l} and {id}"))
                }
                Some(_) => {}
                None => {
                    self.leader_of_term.insert(term, id);
                    self.st.leaders += 1;
                }
            }
        }
    }

    fn check_log_matching(&mut self) {
        let logs: Vec<(u64, Vec<Entry>)> = self
            .nodes
            .iter()
            .filter_map(|(&id, n)| n.rn.as_ref().map(|rn| (id, rn.raft.raft_log.all_entries())))
            .collect();
        for i in 0..logs.len() {
            for j in i + 1..logs.len() {
                let (a, b) = (&logs[i].1, &logs[j].1);
                let (Some(af), Some(bf)) = (a.first(), b.first()) else {
                    continue;
                };
                let lo = af.index.max(bf.index);
                let hi = a.last().unwrap().index.min(b.last().unwrap().index);
                let mut matched = None;
                let mut idx = hi;
                while idx >= lo && idx > 0 {
                    if get(a, idx).term == get(b, idx).term {
                        matched = Some(idx);
                        break;
                    }
                    idx -= 1;
                }
                if let Some(m) = matched {
                    for k in lo..=m {
                        if !same_entry(get(a, k), get(b, k)) {
                            let msg = format!(
                                "log matching: nodes {} and {} agree on term at {m} but differ at {k}",
                                logs[i].0, logs[j].0
                            );
                            self.violation(msg);
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn main() {
    let args: Vec<u64> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .filter_map(|a| a.parse().ok())
        .collect();
    let seeds = args.first().copied().unwrap_or(1000);
    let first = args.get(1).copied().unwrap_or(1);
    let ticks = args.get(2).copied().unwrap_or(3000);
    let fault = args.get(3).copied().unwrap_or(0) == 1;
    let guard = args.get(4).copied().unwrap_or(1) == 1;
    let mut bad = 0;
    for n in [3usize, 5] {
        let mut tot = Stats::default();
        let mut failed = 0;
        for seed in first..first + seeds {
            let mut sim = Sim::new(seed, n, fault, guard);
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for _ in 0..ticks {
                    sim.step_world();
                    if !sim.violations.is_empty() {
                        break;
                    }
                }
                sim.check_log_matching();
                sim
            }));
            match res {
                Ok(sim) => {
                    if !sim.violations.is_empty() {
                        failed += 1;
                        println!("[FAIL] P9 n={n} seed={seed}: {}", sim.violations.join("; "));
                    }
                    let s = &sim.st;
                    tot.commits += s.commits;
                    tot.leaders += s.leaders;
                    tot.crashes += s.crashes;
                    tot.snapshots_sent += s.snapshots_sent;
                    tot.snapshots_applied += s.snapshots_applied;
                    tot.conf_applied += s.conf_applied;
                    tot.compactions += s.compactions;
                    tot.partitions += s.partitions;
                    tot.proposals += s.proposals;
                    tot.skipped_applies += s.skipped_applies;
                    tot.async_seeds += u64::from(sim.cfg.async_storage_writes);
                }
                Err(e) => {
                    failed += 1;
                    let msg = e
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    println!("[FAIL] P9 n={n} seed={seed}: panic: {msg}");
                }
            }
        }
        println!(
            "[{}] P9{} n={n} nodes (+2 spare ids), seeds {first}..{}, {ticks} ticks each: got={failed} seeds with violations want={}",
            if (failed == 0) != fault {
                "OK "
            } else {
                "FAIL"
            },
            if fault {
                " negative control (lossy disk)"
            } else {
                ""
            },
            first + seeds - 1,
            if fault { ">0" } else { "0" }
        );
        println!(
            "      P9 n={n} totals: {} async-storage-writes seeds, {} committed entries, {} proposals accepted, {} leaders (distinct terms), {} crashes, {} partitions, {} compactions, {} snapshots sent, {} applied, {} conf changes applied, {} stale applies skipped",
            tot.async_seeds,
            tot.commits,
            tot.proposals,
            tot.leaders,
            tot.crashes,
            tot.partitions,
            tot.compactions,
            tot.snapshots_sent,
            tot.snapshots_applied,
            tot.conf_applied,
            tot.skipped_applies
        );
        bad += failed;
    }
    if (bad > 0) != fault {
        std::process::exit(1);
    }
}
