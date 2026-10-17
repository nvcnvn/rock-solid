// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// rawnode.go. Licensed under the Apache License 2.0.
//
// Deviation: Ready() moves messages and read states out of raft instead of
// sharing slices, so readyWithoutAccept and acceptReady are one function.
// RawNode owns the Storage; store()/store_mut() replace the shared pointer.

use crate::Error;
use crate::node::{
    Ready, SnapshotStatus, SoftState, is_empty_hard_state, is_empty_snap, is_hard_state_equal,
};
use crate::raft::{Config, LOCAL_APPEND_THREAD, LOCAL_APPLY_THREAD, Raft};
use crate::raftpb::{ConfChangeI, ConfState, Entry, HardState, Message, MessageType};
use crate::status::{BasicStatus, Status, get_basic_status, get_status};
use crate::storage::Storage;
use crate::tracker::Progress;
use crate::util::{ents_size, is_local_msg, is_local_msg_target, is_response_msg};

/// A thread-unsafe raft node driven by the caller.
pub struct RawNode<S: Storage> {
    /// The raft state machine.
    pub raft: Raft<S>,
    async_storage_writes: bool,
    prev_soft_st: SoftState,
    pub(crate) prev_hard_st: HardState,
    steps_on_advance: Vec<Message>,
}

/// Kind of replica a Progress belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressType {
    /// A voter.
    ProgressTypePeer,
    /// A learner.
    ProgressTypeLearner,
}

impl<S: Storage> RawNode<S> {
    /// Creates a RawNode from the config and storage.
    pub fn new(config: &mut Config, storage: S) -> Result<RawNode<S>, Error> {
        let r = Raft::new(config, storage);
        let prev_soft_st = r.soft_state();
        let prev_hard_st = r.hard_state();
        Ok(RawNode {
            raft: r,
            async_storage_writes: config.async_storage_writes,
            prev_soft_st,
            prev_hard_st,
            steps_on_advance: Vec::new(),
        })
    }

    /// The storage.
    pub fn store(&self) -> &S {
        &self.raft.raft_log.storage
    }

    /// Mutable access to the storage.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.raft.raft_log.storage
    }

    /// Advances the logical clock by one tick.
    pub fn tick(&mut self) {
        self.raft.tick();
    }

    /// Advances the election timer without other processing (deprecated in Go).
    pub fn tick_quiesced(&mut self) {
        self.raft.tick_quiesced();
    }

    /// Starts an election.
    pub fn campaign(&mut self) -> Result<(), Error> {
        self.raft.step(Message {
            msg_type: MessageType::MsgHup,
            ..Default::default()
        })
    }

    /// Proposes data to be appended to the log.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), Error> {
        let from = self.raft.id;
        self.raft.step(Message {
            msg_type: MessageType::MsgProp,
            from,
            entries: vec![Entry {
                data,
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    /// Proposes a configuration change.
    pub fn propose_conf_change(&mut self, cc: impl Into<ConfChangeI>) -> Result<(), Error> {
        self.raft.propose_conf_change(&cc.into())
    }

    /// Applies a committed configuration change.
    pub fn apply_conf_change(&mut self, cc: &ConfChangeI) -> ConfState {
        self.raft.apply_conf_change(&cc.as_v2())
    }

    /// Steps a message received from the network or a local storage thread.
    pub fn step(&mut self, m: Message) -> Result<(), Error> {
        if is_local_msg(m.msg_type) && !is_local_msg_target(m.from) {
            return Err(Error::StepLocalMsg);
        }
        if is_response_msg(m.msg_type)
            && !is_local_msg_target(m.from)
            && !self.raft.trk.progress.contains_key(&m.from)
        {
            return Err(Error::StepPeerNotFound);
        }
        self.raft.step(m)
    }

    /// Returns the outstanding work and marks it as accepted.
    pub fn ready(&mut self) -> Ready {
        let allow_unstable = self.apply_unstable_entries();
        let r = &mut self.raft;
        let mut rd = Ready {
            entries: r.raft_log.next_unstable_ents().to_vec(),
            committed_entries: r.raft_log.next_committed_ents(allow_unstable),
            messages: std::mem::take(&mut r.msgs),
            ..Default::default()
        };
        let soft_st = r.soft_state();
        if soft_st != self.prev_soft_st {
            rd.soft_state = Some(soft_st);
        }
        let hard_st = r.hard_state();
        if !is_hard_state_equal(&hard_st, &self.prev_hard_st) {
            rd.hard_state = Some(hard_st);
        }
        if let Some(s) = r.raft_log.next_unstable_snapshot() {
            rd.snapshot = Some(s.clone());
        }
        rd.read_states = std::mem::take(&mut r.read_states);
        rd.must_sync = must_sync(&r.hard_state(), &self.prev_hard_st, rd.entries.len());

        let mut self_msgs = Vec::new();
        if self.async_storage_writes {
            if need_storage_append_msg(r, &rd) {
                let m = new_storage_append_msg(r, &rd);
                rd.messages.push(m);
            }
            if need_storage_apply_msg(&rd) {
                let m = new_storage_apply_msg(r, &rd);
                rd.messages.push(m);
            }
        } else {
            for m in std::mem::take(&mut r.msgs_after_append) {
                if m.to != r.id {
                    rd.messages.push(m);
                } else {
                    self_msgs.push(m);
                }
            }
        }

        // acceptReady
        if let Some(ss) = rd.soft_state {
            self.prev_soft_st = ss;
        }
        if !is_empty_hard_state(rd.hard_state.as_ref())
            && let Some(hs) = rd.hard_state
        {
            self.prev_hard_st = hs;
        }
        if !self.async_storage_writes {
            if !self.steps_on_advance.is_empty() {
                panicf!(
                    r.logger(),
                    "two accepted Ready structs without call to Advance"
                );
            }
            self.steps_on_advance = self_msgs;
            if need_storage_append_resp_msg(r, &rd) {
                let m = new_storage_append_resp_msg(r, &rd);
                self.steps_on_advance.push(m);
            }
            if need_storage_apply_resp_msg(&rd) {
                let m = new_storage_apply_resp_msg(r, rd.committed_entries.clone());
                self.steps_on_advance.push(m);
            }
        }
        r.msgs_after_append.clear();
        r.raft_log.accept_unstable();
        if let Some(last) = rd.committed_entries.last() {
            let index = last.index;
            r.raft_log
                .accept_applying(index, ents_size(&rd.committed_entries), allow_unstable);
        }
        rd
    }

    fn apply_unstable_entries(&self) -> bool {
        !self.async_storage_writes
    }

    /// True if there is a Ready pending.
    pub fn has_ready(&self) -> bool {
        let r = &self.raft;
        if r.soft_state() != self.prev_soft_st {
            return true;
        }
        let hard_st = r.hard_state();
        if !is_empty_hard_state(Some(&hard_st))
            && !is_hard_state_equal(&hard_st, &self.prev_hard_st)
        {
            return true;
        }
        if r.raft_log.has_next_unstable_snapshot() {
            return true;
        }
        if !r.msgs.is_empty() || !r.msgs_after_append.is_empty() {
            return true;
        }
        if r.raft_log.has_next_unstable_ents()
            || r.raft_log
                .has_next_committed_ents(self.apply_unstable_entries())
        {
            return true;
        }
        if !r.read_states.is_empty() {
            return true;
        }
        false
    }

    /// Notifies the node that the last Ready was persisted and applied.
    pub fn advance(&mut self, _rd: Ready) {
        if self.async_storage_writes {
            panicf!(
                self.raft.logger(),
                "Advance must not be called when using AsyncStorageWrites"
            );
        }
        for m in std::mem::take(&mut self.steps_on_advance) {
            let _ = self.raft.step(m);
        }
    }

    /// Current status, including progress on the leader.
    pub fn status(&self) -> Status {
        get_status(&self.raft)
    }

    /// Current basic status.
    pub fn basic_status(&self) -> BasicStatus {
        get_basic_status(&self.raft)
    }

    /// Visits the progress of every member.
    pub fn with_progress(&self, mut visitor: impl FnMut(u64, ProgressType, &Progress)) {
        for (id, pr) in &self.raft.trk.progress {
            let typ = if pr.is_learner {
                ProgressType::ProgressTypeLearner
            } else {
                ProgressType::ProgressTypePeer
            };
            visitor(*id, typ, pr);
        }
    }

    /// Reports that the last send to `id` failed.
    pub fn report_unreachable(&mut self, id: u64) {
        let _ = self.raft.step(Message {
            msg_type: MessageType::MsgUnreachable,
            from: id,
            ..Default::default()
        });
    }

    /// Reports the status of a sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let reject = status == SnapshotStatus::SnapshotFailure;
        let _ = self.raft.step(Message {
            msg_type: MessageType::MsgSnapStatus,
            from: id,
            reject,
            ..Default::default()
        });
    }

    /// Tries to transfer leadership to `transferee`.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let _ = self.raft.step(Message {
            msg_type: MessageType::MsgTransferLeader,
            from: transferee,
            ..Default::default()
        });
    }

    /// Forgets the current leader.
    pub fn forget_leader(&mut self) -> Result<(), Error> {
        self.raft.step(Message {
            msg_type: MessageType::MsgForgetLeader,
            ..Default::default()
        })
    }

    /// Requests a read state carrying `rctx`.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let _ = self.raft.step(Message {
            msg_type: MessageType::MsgReadIndex,
            entries: vec![Entry {
                data: rctx,
                ..Default::default()
            }],
            ..Default::default()
        });
    }

    /// Overrides the randomized election timeout (`SetRandomizedElectionTimeout` test hook).
    pub fn set_randomized_election_timeout(&mut self, v: usize) {
        self.raft.set_randomized_election_timeout(v);
    }
}

/// True if the hard state and entry count require a synchronous write.
pub fn must_sync(st: &HardState, prevst: &HardState, entsnum: usize) -> bool {
    entsnum != 0 || st.vote != prevst.vote || st.term != prevst.term
}

fn need_storage_append_msg<S: Storage>(r: &Raft<S>, rd: &Ready) -> bool {
    !rd.entries.is_empty()
        || !is_empty_hard_state(rd.hard_state.as_ref())
        || !is_empty_snap(rd.snapshot.as_ref())
        || !r.msgs_after_append.is_empty()
}

fn need_storage_append_resp_msg<S: Storage>(r: &Raft<S>, rd: &Ready) -> bool {
    r.raft_log.has_next_or_in_progress_unstable_ents() || !is_empty_snap(rd.snapshot.as_ref())
}

fn new_storage_append_msg<S: Storage>(r: &mut Raft<S>, rd: &Ready) -> Message {
    let mut m = Message {
        msg_type: MessageType::MsgStorageAppend,
        to: LOCAL_APPEND_THREAD,
        from: r.id,
        entries: rd.entries.clone(),
        ..Default::default()
    };
    if !is_empty_hard_state(rd.hard_state.as_ref()) {
        m.term = rd.term();
        m.vote = rd.vote();
        m.commit = rd.commit();
    }
    if !is_empty_snap(rd.snapshot.as_ref()) {
        m.snapshot = rd.snapshot.clone().map(Box::new);
    }
    let resp = need_storage_append_resp_msg(r, rd).then(|| new_storage_append_resp_msg(r, rd));
    m.responses = std::mem::take(&mut r.msgs_after_append);
    m.responses.extend(resp);
    m
}

fn new_storage_append_resp_msg<S: Storage>(r: &Raft<S>, rd: &Ready) -> Message {
    let mut m = Message {
        msg_type: MessageType::MsgStorageAppendResp,
        to: r.id,
        from: LOCAL_APPEND_THREAD,
        term: r.term,
        ..Default::default()
    };
    if r.raft_log.has_next_or_in_progress_unstable_ents() {
        let last = r.raft_log.last_entry_id();
        m.index = last.index;
        m.log_term = last.term;
    }
    if !is_empty_snap(rd.snapshot.as_ref()) {
        m.snapshot = rd.snapshot.clone().map(Box::new);
    }
    m
}

fn need_storage_apply_msg(rd: &Ready) -> bool {
    !rd.committed_entries.is_empty()
}

fn need_storage_apply_resp_msg(rd: &Ready) -> bool {
    need_storage_apply_msg(rd)
}

fn new_storage_apply_msg<S: Storage>(r: &Raft<S>, rd: &Ready) -> Message {
    let ents = rd.committed_entries.clone();
    Message {
        msg_type: MessageType::MsgStorageApply,
        to: LOCAL_APPLY_THREAD,
        from: r.id,
        term: 0,
        entries: ents.clone(),
        responses: vec![new_storage_apply_resp_msg(r, ents)],
        ..Default::default()
    }
}

fn new_storage_apply_resp_msg<S: Storage>(r: &Raft<S>, ents: Vec<Entry>) -> Message {
    Message {
        msg_type: MessageType::MsgStorageApplyResp,
        to: r.id,
        from: LOCAL_APPLY_THREAD,
        term: 0,
        entries: ents,
        ..Default::default()
    }
}
