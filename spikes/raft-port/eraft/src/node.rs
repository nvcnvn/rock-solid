// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// node.go (the types only; the goroutine-based Node is not ported) and the
// confChangeToMsg helper. Licensed under the Apache License 2.0.

use crate::raft::{ReadState, StateType};
use crate::raftpb::{
    ConfChangeI, Entry, HardState, Message, MessageType, Snapshot, marshal_conf_change,
};

/// Status of a sent snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStatus {
    /// The snapshot was applied.
    SnapshotFinish = 1,
    /// The snapshot failed.
    SnapshotFailure = 2,
}

/// Volatile state useful for logging and debugging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SoftState {
    /// Current leader.
    pub lead: u64,
    /// Current role.
    pub raft_state: StateType,
}

/// Work the application must do: persist, send, apply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ready {
    /// New soft state, if changed.
    pub soft_state: Option<SoftState>,
    /// New hard state to persist before sending messages, if changed.
    pub hard_state: Option<HardState>,
    /// Read states for linearizable reads.
    pub read_states: Vec<ReadState>,
    /// Entries to persist before sending messages.
    pub entries: Vec<Entry>,
    /// Snapshot to persist.
    pub snapshot: Option<Snapshot>,
    /// Entries to apply.
    pub committed_entries: Vec<Entry>,
    /// Messages to send (after persisting, unless async storage writes are on).
    pub messages: Vec<Message>,
    /// True if the hard state and entries must be synced to disk.
    pub must_sync: bool,
}

impl Ready {
    /// Term of the hard state, or 0 (`rd.GetTerm()` via embedding in Go).
    pub fn term(&self) -> u64 {
        self.hard_state.map(|h| h.term).unwrap_or(0)
    }
    /// Vote of the hard state, or 0.
    pub fn vote(&self) -> u64 {
        self.hard_state.map(|h| h.vote).unwrap_or(0)
    }
    /// Commit of the hard state, or 0.
    pub fn commit(&self) -> u64 {
        self.hard_state.map(|h| h.commit).unwrap_or(0)
    }
}

pub(crate) fn is_hard_state_equal(a: &HardState, b: &HardState) -> bool {
    a.term == b.term && a.vote == b.vote && a.commit == b.commit
}

/// True if the hard state is absent or all zero.
pub fn is_empty_hard_state(st: Option<&HardState>) -> bool {
    st.is_none_or(|s| is_hard_state_equal(s, &HardState::default()))
}

/// True if the snapshot is absent or has index 0.
pub fn is_empty_snap(sp: Option<&Snapshot>) -> bool {
    sp.is_none_or(|s| s.metadata.index == 0)
}

/// A peer for `RawNode::bootstrap`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Peer {
    /// Node ID.
    pub id: u64,
    /// Opaque context.
    pub context: Vec<u8>,
}

pub(crate) fn conf_change_to_msg(c: Option<&ConfChangeI>) -> Message {
    let (entry_type, data) = marshal_conf_change(c);
    Message {
        msg_type: MessageType::MsgProp,
        entries: vec![Entry {
            entry_type,
            data,
            ..Default::default()
        }],
        ..Default::default()
    }
}
