// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// raftpb/raft.proto, raftpb/alias.go, raftpb/confchange.go, raftpb/confstate.go,
// raftpb/util.go. Licensed under the Apache License 2.0.
//
// Deviation: the protobuf messages are plain structs. Optional proto2 scalars
// are plain values (zero = unset), `Message.snapshot` is an `Option<Box<_>>`,
// and `ConfChangeI` is an enum instead of an interface.

//! Raft wire types as plain Rust structs.

mod codec;

use std::fmt;

pub use codec::DecodeError;

/// Type of a log entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntryType {
    /// A normal entry.
    #[default]
    EntryNormal = 0,
    /// An encoded `ConfChange`.
    EntryConfChange = 1,
    /// An encoded `ConfChangeV2`.
    EntryConfChangeV2 = 2,
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EntryType::EntryNormal => "EntryNormal",
            EntryType::EntryConfChange => "EntryConfChange",
            EntryType::EntryConfChangeV2 => "EntryConfChangeV2",
        })
    }
}

/// A raft log entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    /// Entry type.
    pub entry_type: EntryType,
    /// Term of the leader that created the entry.
    pub term: u64,
    /// Log position.
    pub index: u64,
    /// Payload.
    pub data: Vec<u8>,
}

/// Metadata of a snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// Configuration at the snapshot index.
    pub conf_state: ConfState,
    /// Index of the last entry covered by the snapshot.
    pub index: u64,
    /// Term of the last entry covered by the snapshot.
    pub term: u64,
}

/// A state machine snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Application data.
    pub data: Vec<u8>,
    /// Snapshot metadata.
    pub metadata: SnapshotMetadata,
}

/// Raft message type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MessageType {
    /// Local: start an election.
    #[default]
    MsgHup = 0,
    /// Local: leader heartbeat tick.
    MsgBeat = 1,
    /// Proposal.
    MsgProp = 2,
    /// Append entries.
    MsgApp = 3,
    /// Append response.
    MsgAppResp = 4,
    /// Vote request.
    MsgVote = 5,
    /// Vote response.
    MsgVoteResp = 6,
    /// Snapshot.
    MsgSnap = 7,
    /// Heartbeat.
    MsgHeartbeat = 8,
    /// Heartbeat response.
    MsgHeartbeatResp = 9,
    /// Local: peer unreachable.
    MsgUnreachable = 10,
    /// Local: snapshot status.
    MsgSnapStatus = 11,
    /// Local: check quorum tick.
    MsgCheckQuorum = 12,
    /// Leadership transfer request.
    MsgTransferLeader = 13,
    /// Start an election immediately.
    MsgTimeoutNow = 14,
    /// Read index request.
    MsgReadIndex = 15,
    /// Read index response.
    MsgReadIndexResp = 16,
    /// Pre-vote request.
    MsgPreVote = 17,
    /// Pre-vote response.
    MsgPreVoteResp = 18,
    /// Local: append to storage.
    MsgStorageAppend = 19,
    /// Local: storage append done.
    MsgStorageAppendResp = 20,
    /// Local: apply entries.
    MsgStorageApply = 21,
    /// Local: apply done.
    MsgStorageApplyResp = 22,
    /// Forget the current leader.
    MsgForgetLeader = 23,
}

const MESSAGE_TYPE_NAMES: [&str; 24] = [
    "MsgHup",
    "MsgBeat",
    "MsgProp",
    "MsgApp",
    "MsgAppResp",
    "MsgVote",
    "MsgVoteResp",
    "MsgSnap",
    "MsgHeartbeat",
    "MsgHeartbeatResp",
    "MsgUnreachable",
    "MsgSnapStatus",
    "MsgCheckQuorum",
    "MsgTransferLeader",
    "MsgTimeoutNow",
    "MsgReadIndex",
    "MsgReadIndexResp",
    "MsgPreVote",
    "MsgPreVoteResp",
    "MsgStorageAppend",
    "MsgStorageAppendResp",
    "MsgStorageApply",
    "MsgStorageApplyResp",
    "MsgForgetLeader",
];

const MESSAGE_TYPES: [MessageType; 24] = [
    MessageType::MsgHup,
    MessageType::MsgBeat,
    MessageType::MsgProp,
    MessageType::MsgApp,
    MessageType::MsgAppResp,
    MessageType::MsgVote,
    MessageType::MsgVoteResp,
    MessageType::MsgSnap,
    MessageType::MsgHeartbeat,
    MessageType::MsgHeartbeatResp,
    MessageType::MsgUnreachable,
    MessageType::MsgSnapStatus,
    MessageType::MsgCheckQuorum,
    MessageType::MsgTransferLeader,
    MessageType::MsgTimeoutNow,
    MessageType::MsgReadIndex,
    MessageType::MsgReadIndexResp,
    MessageType::MsgPreVote,
    MessageType::MsgPreVoteResp,
    MessageType::MsgStorageAppend,
    MessageType::MsgStorageAppendResp,
    MessageType::MsgStorageApply,
    MessageType::MsgStorageApplyResp,
    MessageType::MsgForgetLeader,
];

impl MessageType {
    /// Parses a message type name (`MessageType_value` in Go).
    pub fn from_name(s: &str) -> Option<MessageType> {
        MESSAGE_TYPE_NAMES
            .iter()
            .position(|n| *n == s)
            .map(|i| MESSAGE_TYPES[i])
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(MESSAGE_TYPE_NAMES[*self as usize])
    }
}

/// A raft message.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// Message type.
    pub msg_type: MessageType,
    /// Recipient.
    pub to: u64,
    /// Sender.
    pub from: u64,
    /// Term of the sender, or 0 for local messages.
    pub term: u64,
    /// Term of the entry at `index` (append) or of the rejection hint (append response).
    pub log_term: u64,
    /// Log index.
    pub index: u64,
    /// Entries to append.
    pub entries: Vec<Entry>,
    /// Commit index.
    pub commit: u64,
    /// Vote, for MsgStorageAppend.
    pub vote: u64,
    /// Snapshot, for MsgSnap and storage messages.
    pub snapshot: Option<Box<Snapshot>>,
    /// Whether the request was rejected.
    pub reject: bool,
    /// Rejection hint.
    pub reject_hint: u64,
    /// Opaque context.
    pub context: Vec<u8>,
    /// Responses to deliver after a local storage message is processed.
    pub responses: Vec<Message>,
}

/// Persistent raft state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// Current term.
    pub term: u64,
    /// Vote in the current term.
    pub vote: u64,
    /// Commit index.
    pub commit: u64,
}

/// Joint consensus transition behaviour of a `ConfChangeV2`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConfChangeTransition {
    /// Simple protocol when possible, otherwise implicit joint consensus.
    #[default]
    ConfChangeTransitionAuto = 0,
    /// Joint consensus, left automatically.
    ConfChangeTransitionJointImplicit = 1,
    /// Joint consensus, left by the application.
    ConfChangeTransitionJointExplicit = 2,
}

impl fmt::Display for ConfChangeTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ConfChangeTransition::ConfChangeTransitionAuto => "ConfChangeTransitionAuto",
            ConfChangeTransition::ConfChangeTransitionJointImplicit => {
                "ConfChangeTransitionJointImplicit"
            }
            ConfChangeTransition::ConfChangeTransitionJointExplicit => {
                "ConfChangeTransitionJointExplicit"
            }
        })
    }
}

/// A raft configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfState {
    /// Voters in the incoming config.
    pub voters: Vec<u64>,
    /// Learners in the incoming config.
    pub learners: Vec<u64>,
    /// Voters in the outgoing config.
    pub voters_outgoing: Vec<u64>,
    /// Nodes that become learners when the outgoing config is removed.
    pub learners_next: Vec<u64>,
    /// Whether the joint config is left automatically.
    pub auto_leave: bool,
}

impl ConfState {
    /// Returns Ok if the inputs describe the same configuration.
    pub fn equivalent(&self, other: &ConfState) -> Result<(), String> {
        let norm = |c: &ConfState| {
            let mut c = c.clone();
            c.voters.sort_unstable();
            c.learners.sort_unstable();
            c.voters_outgoing.sort_unstable();
            c.learners_next.sort_unstable();
            c
        };
        let (a, b) = (norm(self), norm(other));
        if a != b {
            return Err(format!(
                "ConfStates not equivalent after sorting:\n{a:?}\n{b:?}\nInputs were:\n{self:?}\n{other:?}"
            ));
        }
        Ok(())
    }
}

/// Type of a single configuration change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConfChangeType {
    /// Add or promote a voter.
    #[default]
    ConfChangeAddNode = 0,
    /// Remove a node.
    ConfChangeRemoveNode = 1,
    /// Update a node (no-op for raft).
    ConfChangeUpdateNode = 2,
    /// Add or demote to a learner.
    ConfChangeAddLearnerNode = 3,
}

impl ConfChangeType {
    fn from_i32(v: u64) -> Option<ConfChangeType> {
        Some(match v {
            0 => ConfChangeType::ConfChangeAddNode,
            1 => ConfChangeType::ConfChangeRemoveNode,
            2 => ConfChangeType::ConfChangeUpdateNode,
            3 => ConfChangeType::ConfChangeAddLearnerNode,
            _ => return None,
        })
    }
}

impl fmt::Display for ConfChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ConfChangeType::ConfChangeAddNode => "ConfChangeAddNode",
            ConfChangeType::ConfChangeRemoveNode => "ConfChangeRemoveNode",
            ConfChangeType::ConfChangeUpdateNode => "ConfChangeUpdateNode",
            ConfChangeType::ConfChangeAddLearnerNode => "ConfChangeAddLearnerNode",
        })
    }
}

/// Legacy single configuration change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfChange {
    /// Change type.
    pub change_type: ConfChangeType,
    /// Node affected.
    pub node_id: u64,
    /// Opaque context.
    pub context: Vec<u8>,
    /// etcd-only identifier.
    pub id: u64,
}

/// One operation of a `ConfChangeV2`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConfChangeSingle {
    /// Change type.
    pub change_type: ConfChangeType,
    /// Node affected.
    pub node_id: u64,
}

/// Configuration change supporting joint consensus.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfChangeV2 {
    /// Joint consensus behaviour.
    pub transition: ConfChangeTransition,
    /// Operations.
    pub changes: Vec<ConfChangeSingle>,
    /// Opaque context.
    pub context: Vec<u8>,
}

/// Either kind of configuration change (`ConfChangeI` in Go).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfChangeI {
    /// Legacy change.
    V1(ConfChange),
    /// Joint-capable change.
    V2(ConfChangeV2),
}

impl From<ConfChange> for ConfChangeI {
    fn from(c: ConfChange) -> Self {
        ConfChangeI::V1(c)
    }
}

impl From<ConfChangeV2> for ConfChangeI {
    fn from(c: ConfChangeV2) -> Self {
        ConfChangeI::V2(c)
    }
}

impl ConfChangeI {
    /// Returns a V2 configuration change carrying out the same operation.
    pub fn as_v2(&self) -> ConfChangeV2 {
        match self {
            ConfChangeI::V1(c) => c.as_v2(),
            ConfChangeI::V2(c) => c.clone(),
        }
    }

    /// Returns the V1 change, if this is one.
    pub fn as_v1(&self) -> Option<&ConfChange> {
        match self {
            ConfChangeI::V1(c) => Some(c),
            ConfChangeI::V2(_) => None,
        }
    }
}

impl ConfChange {
    /// Returns a V2 configuration change carrying out the same operation.
    pub fn as_v2(&self) -> ConfChangeV2 {
        ConfChangeV2 {
            transition: ConfChangeTransition::ConfChangeTransitionAuto,
            changes: vec![ConfChangeSingle {
                change_type: self.change_type,
                node_id: self.node_id,
            }],
            context: self.context.clone(),
        }
    }

    /// Encodes the change in protobuf wire format.
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_conf_change(self)
    }

    /// Decodes a change from protobuf wire format.
    pub fn decode(data: &[u8]) -> Result<ConfChange, DecodeError> {
        codec::decode_conf_change(data)
    }
}

impl ConfChangeV2 {
    /// Returns (autoLeave, ok): ok is true if the change uses joint consensus.
    pub fn enter_joint(&self) -> (bool, bool) {
        if self.transition != ConfChangeTransition::ConfChangeTransitionAuto
            || self.changes.len() > 1
        {
            let auto_leave = match self.transition {
                ConfChangeTransition::ConfChangeTransitionAuto => true,
                ConfChangeTransition::ConfChangeTransitionJointImplicit => true,
                ConfChangeTransition::ConfChangeTransitionJointExplicit => false,
            };
            return (auto_leave, true);
        }
        (false, false)
    }

    /// True if the change leaves a joint configuration.
    pub fn leave_joint(&self) -> bool {
        self.transition == ConfChangeTransition::ConfChangeTransitionAuto && self.changes.is_empty()
    }

    /// Encodes the change in protobuf wire format.
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_conf_change_v2(self)
    }

    /// Decodes a change from protobuf wire format.
    pub fn decode(data: &[u8]) -> Result<ConfChangeV2, DecodeError> {
        codec::decode_conf_change_v2(data)
    }
}

/// Encodes a configuration change and returns it with its entry type. `None`
/// encodes as empty data that decodes into an empty `ConfChangeV2`.
pub fn marshal_conf_change(c: Option<&ConfChangeI>) -> (EntryType, Vec<u8>) {
    match c {
        None => (EntryType::EntryConfChangeV2, Vec::new()),
        Some(ConfChangeI::V1(c)) => (EntryType::EntryConfChange, c.encode()),
        Some(ConfChangeI::V2(c)) => (EntryType::EntryConfChangeV2, c.encode()),
    }
}

/// Parses a space-delimited sequence of operations: vN, lN, rN, uN.
pub fn conf_changes_from_string(s: &str) -> Result<Vec<ConfChangeSingle>, String> {
    let mut ccs = Vec::new();
    let s = s.trim();
    if s.is_empty() {
        return Ok(ccs);
    }
    for tok in s.split(' ') {
        if tok.len() < 2 {
            return Err(format!("unknown token {tok}"));
        }
        let change_type = match tok.as_bytes()[0] {
            b'v' => ConfChangeType::ConfChangeAddNode,
            b'l' => ConfChangeType::ConfChangeAddLearnerNode,
            b'r' => ConfChangeType::ConfChangeRemoveNode,
            b'u' => ConfChangeType::ConfChangeUpdateNode,
            _ => return Err(format!("unknown input: {tok}")),
        };
        let node_id = tok[1..]
            .parse::<u64>()
            .map_err(|e| format!("strconv.ParseUint: parsing {:?}: {e}", &tok[1..]))?;
        ccs.push(ConfChangeSingle {
            change_type,
            node_id,
        });
    }
    Ok(ccs)
}

/// Inverse of `conf_changes_from_string`.
pub fn conf_changes_to_string(ccs: &[ConfChangeSingle]) -> String {
    let mut buf = String::new();
    for (i, cc) in ccs.iter().enumerate() {
        if i > 0 {
            buf.push(' ');
        }
        buf.push(match cc.change_type {
            ConfChangeType::ConfChangeAddNode => 'v',
            ConfChangeType::ConfChangeAddLearnerNode => 'l',
            ConfChangeType::ConfChangeRemoveNode => 'r',
            ConfChangeType::ConfChangeUpdateNode => 'u',
        });
        buf.push_str(&cc.node_id.to_string());
    }
    buf
}

/// Protobuf encoding size of an entry (`proto.Size` in Go).
///
/// Deviation: Go counts a field that was explicitly set to its zero value; this
/// port treats zero as unset, so an explicitly typed `EntryNormal` entry or an
/// empty non-nil payload is 2 bytes smaller here.
pub fn entry_size(e: &Entry) -> u64 {
    let mut n = 0;
    if e.entry_type != EntryType::EntryNormal {
        n += 1 + codec::varint_len(e.entry_type as u64);
    }
    if e.term != 0 {
        n += 1 + codec::varint_len(e.term);
    }
    if e.index != 0 {
        n += 1 + codec::varint_len(e.index);
    }
    if !e.data.is_empty() {
        let l = e.data.len() as u64;
        n += 1 + codec::varint_len(l) + l;
    }
    n
}
