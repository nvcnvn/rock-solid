//! eraft (the etcd-raft port) replica in the same harness.
//! Election timeouts come from the port's seeded ChaCha8, seeded from
//! --rand-seed (default --seed) and the group index.

use std::rc::Rc;

use eraft::logger::{Logger, NopLogger};
use eraft::raftpb::{Entry, EntryType, Message, MessageType};
use eraft::{Config, MemoryStorage, RawNode, Ready, StateType};

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
        (self.msg_type == MessageType::MsgApp).then(|| self.entries.len())
    }
}

thread_local! {
    static LOGGER: Rc<dyn Logger> = Rc::new(NopLogger);
}

pub struct PortReplica {
    node: RawNode<MemoryStorage>,
}

impl PortReplica {
    fn count_ops(&self, ents: &[Entry]) -> u64 {
        if self.node.raft.state != StateType::StateLeader {
            return 0;
        }
        ents.iter()
            .filter(|e| e.entry_type == EntryType::EntryNormal && !e.data.is_empty())
            .count() as u64
    }
}

impl Replica for PortReplica {
    type M = Message;
    type Pending = Ready;

    fn new(id: u64, group: usize, o: &Opts, tick_ms: u64) -> Self {
        let mut store = MemoryStorage::new();
        store.conf_state_mut().voters = vec![1, 2, 3];
        let election_tick = (o.elect_min_ms / tick_ms) as usize;
        let mut cfg = Config {
            id,
            election_tick,
            heartbeat_tick: (o.hb_ms / tick_ms) as usize,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            logger: Some(LOGGER.with(|l| l.clone())),
            rand_seed: o
                .rand_seed
                .unwrap_or(o.seed)
                .wrapping_mul(1_000_003)
                .wrapping_add(group as u64),
            ..Default::default()
        };
        // etcd-raft draws the timeout from [election, 2*election); raft-rs from
        // [min, max). Both presets use max = 2*min, so the ranges match.
        assert_eq!(
            o.elect_max_ms,
            2 * o.elect_min_ms,
            "etcd-raft fixes max election = 2 x min"
        );
        PortReplica {
            node: RawNode::new(&mut cfg, store).unwrap(),
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
        self.node.propose(data).is_ok()
    }
    fn is_leader(&self) -> bool {
        self.node.raft.state == StateType::StateLeader
    }
    fn term(&self) -> u64 {
        self.node.raft.term
    }
    fn has_ready(&self) -> bool {
        self.node.has_ready()
    }

    fn ready_persist(&mut self, _send: &mut Vec<Message>, out: &mut ReadyOut) -> Ready {
        let mut rd = self.node.ready();
        let store = self.node.store_mut();
        if !rd.entries.is_empty() {
            out.persisted_entries += rd.entries.len() as u64;
            store.append(&rd.entries).unwrap();
        }
        if let Some(hs) = rd.hard_state {
            store.set_hard_state(hs).unwrap();
        }
        if let Some(s) = rd.snapshot.take() {
            store.apply_snapshot(s).unwrap();
        }
        out.ops += self.count_ops(&rd.committed_entries);
        out.must_sync = rd.must_sync;
        rd
    }

    fn after_sync(&mut self, mut rd: Ready, send: &mut Vec<Message>, _out: &mut ReadyOut) -> bool {
        send.append(&mut rd.messages);
        self.node.advance(rd);
        self.node.has_ready()
    }
}
