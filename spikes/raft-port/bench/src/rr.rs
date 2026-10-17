//! raft-rs (tikv/raft 0.7.0) replica, adapted from spikes/multi-raft/src/rr.rs.
//! Election timeouts come from raft-rs's own thread_rng, as shipped.

use raft::StateRole;
use raft::prelude::*;
use raft::storage::MemStorage;
use slog::{Discard, Logger, o};

use crate::common::Opts;
use crate::harness::{Msg, ReadyOut, Replica};

impl Msg for Message {
    fn from(&self) -> u64 {
        self.from
    }
    fn to(&self) -> u64 {
        self.to
    }
    fn kind(&self) -> i32 {
        self.msg_type as i32
    }
    fn term(&self) -> u64 {
        self.term
    }
    fn index(&self) -> u64 {
        self.index
    }
    fn is_heartbeat(&self) -> bool {
        self.msg_type == MessageType::MsgHeartbeat
    }
    fn append_entries(&self) -> Option<usize> {
        (self.msg_type == MessageType::MsgAppend).then(|| self.entries.len())
    }
}

pub struct RrReplica {
    node: RawNode<MemStorage>,
}

impl RrReplica {
    fn count_ops(&self, ents: &[Entry]) -> u64 {
        let leader = self.node.raft.state == StateRole::Leader;
        if !leader {
            return 0;
        }
        ents.iter()
            .filter(|e| e.entry_type == EntryType::EntryNormal && !e.data.is_empty())
            .count() as u64
    }
}

impl Replica for RrReplica {
    type M = Message;
    type Pending = Ready;

    fn new(id: u64, _group: usize, o: &Opts, tick_ms: u64) -> Self {
        let logger = Logger::root(Discard, o!());
        let et_min = (o.elect_min_ms / tick_ms) as usize;
        let et_max = (o.elect_max_ms / tick_ms) as usize;
        let cfg = Config {
            id,
            heartbeat_tick: (o.hb_ms / tick_ms) as usize,
            election_tick: et_min,
            min_election_tick: et_min,
            max_election_tick: et_max,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            ..Default::default()
        };
        let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        RrReplica {
            node: RawNode::new(&cfg, store, &logger).unwrap(),
        }
    }

    fn tick(&mut self) {
        self.node.tick();
    }
    fn step(&mut self, m: Message) {
        let _ = self.node.step(m);
    }
    fn campaign(&mut self) {
        let _ = self.node.campaign();
    }
    fn propose(&mut self, data: Vec<u8>) -> bool {
        self.node.propose(vec![], data).is_ok()
    }
    fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }
    fn term(&self) -> u64 {
        self.node.raft.term
    }
    fn has_ready(&self) -> bool {
        self.node.has_ready()
    }

    fn ready_persist(&mut self, send: &mut Vec<Message>, out: &mut ReadyOut) -> Ready {
        let mut rd = self.node.ready();
        send.extend(rd.take_messages());
        let committed = rd.take_committed_entries();
        out.ops += self.count_ops(&committed);
        let store = self.node.store().clone();
        if !rd.entries().is_empty() {
            out.persisted_entries += rd.entries().len() as u64;
            store.wl().append(rd.entries()).unwrap();
        }
        if let Some(hs) = rd.hs() {
            store.wl().set_hardstate(hs.clone());
        }
        out.must_sync = rd.must_sync();
        rd
    }

    fn after_sync(&mut self, mut rd: Ready, send: &mut Vec<Message>, out: &mut ReadyOut) -> bool {
        send.extend(rd.take_persisted_messages());
        let mut light = self.node.advance(rd);
        if let Some(c) = light.commit_index() {
            self.node.store().wl().mut_hard_state().set_commit(c);
        }
        send.extend(light.take_messages());
        let lc = light.take_committed_entries();
        out.ops += self.count_ops(&lc);
        self.node.advance_apply();
        false
    }
}
