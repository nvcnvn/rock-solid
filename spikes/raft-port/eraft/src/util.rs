// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// util.go. Licensed under the Apache License 2.0.

use std::fmt::Write as _;

use crate::node::{Ready, SoftState, is_empty_hard_state, is_empty_snap};
use crate::raft::{LOCAL_APPEND_THREAD, LOCAL_APPLY_THREAD, NONE};
use crate::raftpb::{
    ConfChange, ConfChangeI, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
    MessageType, Snapshot, conf_changes_to_string, entry_size,
};

/// True for message types that never leave the node.
pub fn is_local_msg(t: MessageType) -> bool {
    matches!(
        t,
        MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum
            | MessageType::MsgStorageAppend
            | MessageType::MsgStorageAppendResp
            | MessageType::MsgStorageApply
            | MessageType::MsgStorageApplyResp
    )
}

/// True for response message types.
pub fn is_response_msg(t: MessageType) -> bool {
    matches!(
        t,
        MessageType::MsgAppResp
            | MessageType::MsgVoteResp
            | MessageType::MsgHeartbeatResp
            | MessageType::MsgUnreachable
            | MessageType::MsgReadIndexResp
            | MessageType::MsgPreVoteResp
            | MessageType::MsgStorageAppendResp
            | MessageType::MsgStorageApplyResp
    )
}

/// True if `id` is one of the local storage threads.
pub fn is_local_msg_target(id: u64) -> bool {
    id == LOCAL_APPEND_THREAD || id == LOCAL_APPLY_THREAD
}

pub(crate) fn vote_resp_msg_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgVote => MessageType::MsgVoteResp,
        MessageType::MsgPreVote => MessageType::MsgPreVoteResp,
        _ => panic!("not a vote message: {t}"),
    }
}

/// Formats a slice like Go's `%v`: `[1 2 3]`.
pub fn go_slice<T: std::fmt::Display>(s: &[T]) -> String {
    let mut b = String::from("[");
    for (i, v) in s.iter().enumerate() {
        if i > 0 {
            b.push(' ');
        }
        let _ = write!(b, "{v}");
    }
    b.push(']');
    b
}

/// Quotes bytes like Go's `%q` (strconv.Quote).
pub fn go_quote(data: &[u8]) -> String {
    let mut b = String::from("\"");
    let mut rest = data;
    while !rest.is_empty() {
        let (ch, n) = match std::str::from_utf8(rest) {
            Ok(s) => {
                let c = s.chars().next().unwrap_or('\0');
                (Some(c), c.len_utf8())
            }
            Err(e) if e.valid_up_to() > 0 => {
                let s = std::str::from_utf8(&rest[..e.valid_up_to()]).unwrap_or("");
                let c = s.chars().next().unwrap_or('\0');
                (Some(c), c.len_utf8())
            }
            Err(_) => (None, 1),
        };
        match ch {
            None => {
                let _ = write!(b, "\\x{:02x}", rest[0]);
            }
            Some(c) => match c {
                '"' => b.push_str("\\\""),
                '\\' => b.push_str("\\\\"),
                '\x07' => b.push_str("\\a"),
                '\x08' => b.push_str("\\b"),
                '\x0c' => b.push_str("\\f"),
                '\n' => b.push_str("\\n"),
                '\r' => b.push_str("\\r"),
                '\t' => b.push_str("\\t"),
                '\x0b' => b.push_str("\\v"),
                c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                    let _ = write!(b, "\\x{:02x}", c as u32);
                }
                c if c.is_control() => {
                    if (c as u32) < 0x10000 {
                        let _ = write!(b, "\\u{:04x}", c as u32);
                    } else {
                        let _ = write!(b, "\\U{:08x}", c as u32);
                    }
                }
                c => b.push(c),
            },
        }
        rest = &rest[n..];
    }
    b.push('"');
    b
}

/// Describes a HardState.
pub fn describe_hard_state(hs: &HardState) -> String {
    let mut buf = format!("Term:{}", hs.term);
    if hs.vote != 0 {
        let _ = write!(buf, " Vote:{}", hs.vote);
    }
    let _ = write!(buf, " Commit:{}", hs.commit);
    buf
}

/// Describes a SoftState.
pub fn describe_soft_state(ss: &SoftState) -> String {
    format!("Lead:{} State:{}", ss.lead, ss.raft_state)
}

/// Describes a ConfState.
pub fn describe_conf_state(s: &ConfState) -> String {
    format!(
        "Voters:{} VotersOutgoing:{} Learners:{} LearnersNext:{} AutoLeave:{}",
        go_slice(&s.voters),
        go_slice(&s.voters_outgoing),
        go_slice(&s.learners),
        go_slice(&s.learners_next),
        s.auto_leave
    )
}

/// Describes a Snapshot.
pub fn describe_snapshot(snap: &Snapshot) -> String {
    let m = &snap.metadata;
    format!(
        "Index:{} Term:{} ConfState:{}",
        m.index,
        m.term,
        describe_conf_state(&m.conf_state)
    )
}

/// Formats entry data for humans; `None` uses Go's `%q`.
pub type EntryFormatter<'a> = Option<&'a dyn Fn(&[u8]) -> String>;

/// Describes a Ready.
pub fn describe_ready(rd: &Ready, f: EntryFormatter<'_>) -> String {
    let mut buf = String::new();
    if let Some(ss) = &rd.soft_state {
        buf.push_str(&describe_soft_state(ss));
        buf.push('\n');
    }
    if !is_empty_hard_state(rd.hard_state.as_ref()) {
        let _ = writeln!(
            buf,
            "HardState {}",
            describe_hard_state(rd.hard_state.as_ref().unwrap_or(&HardState::default()))
        );
    }
    if !rd.read_states.is_empty() {
        let mut s = String::from("[");
        for (i, rs) in rd.read_states.iter().enumerate() {
            if i > 0 {
                s.push(' ');
            }
            let _ = write!(s, "{{{} {}}}", rs.index, go_slice(&rs.request_ctx));
        }
        s.push(']');
        let _ = writeln!(buf, "ReadStates {s}");
    }
    if !rd.entries.is_empty() {
        buf.push_str("Entries:\n");
        buf.push_str(&describe_entries(&rd.entries, f));
    }
    if !is_empty_snap(rd.snapshot.as_ref())
        && let Some(s) = &rd.snapshot
    {
        let _ = writeln!(buf, "Snapshot {}", describe_snapshot(s));
    }
    if !rd.committed_entries.is_empty() {
        buf.push_str("CommittedEntries:\n");
        buf.push_str(&describe_entries(&rd.committed_entries, f));
    }
    if !rd.messages.is_empty() {
        buf.push_str("Messages:\n");
        for m in &rd.messages {
            buf.push_str(&describe_message(m, f));
            buf.push('\n');
        }
    }
    if !buf.is_empty() {
        return format!("Ready MustSync={}:\n{}", rd.must_sync, buf);
    }
    "<empty Ready>".to_string()
}

/// Describes a Message.
pub fn describe_message(m: &Message, f: EntryFormatter<'_>) -> String {
    describe_message_with_indent("", m, f)
}

fn describe_message_with_indent(indent: &str, m: &Message, f: EntryFormatter<'_>) -> String {
    let mut buf = format!(
        "{}{}->{} {} Term:{} Log:{}/{}",
        indent,
        describe_target(m.from),
        describe_target(m.to),
        m.msg_type,
        m.term,
        m.log_term,
        m.index
    );
    if m.reject {
        let _ = write!(buf, " Rejected (Hint: {})", m.reject_hint);
    }
    if m.commit != 0 {
        let _ = write!(buf, " Commit:{}", m.commit);
    }
    if m.vote != 0 {
        let _ = write!(buf, " Vote:{}", m.vote);
    }
    if m.entries.len() == 1 {
        let _ = write!(buf, " Entries:[{}]", describe_entry(&m.entries[0], f));
    } else if m.entries.len() > 1 {
        buf.push_str(" Entries:[");
        for e in &m.entries {
            let _ = write!(buf, "\n{indent}  ");
            buf.push_str(&describe_entry(e, f));
        }
        let _ = write!(buf, "\n{indent}]");
    }
    if let Some(s) = &m.snapshot
        && !is_empty_snap(Some(s))
    {
        let _ = write!(buf, "\n{indent}  Snapshot: {}", describe_snapshot(s));
    }
    if !m.responses.is_empty() {
        buf.push_str(" Responses:[");
        let sub = format!("{indent}  ");
        for r in &m.responses {
            buf.push('\n');
            buf.push_str(&describe_message_with_indent(&sub, r, f));
        }
        let _ = write!(buf, "\n{indent}]");
    }
    buf
}

fn describe_target(id: u64) -> String {
    match id {
        NONE => "None".to_string(),
        LOCAL_APPEND_THREAD => "AppendThread".to_string(),
        LOCAL_APPLY_THREAD => "ApplyThread".to_string(),
        _ => format!("{id:x}"),
    }
}

/// Describes an Entry.
pub fn describe_entry(e: &Entry, f: EntryFormatter<'_>) -> String {
    let formatted = match e.entry_type {
        EntryType::EntryNormal => match f {
            Some(f) => f(&e.data),
            None => go_quote(&e.data),
        },
        EntryType::EntryConfChange => match ConfChange::decode(&e.data) {
            Ok(cc) => conf_changes_to_string(&cc.as_v2().changes),
            Err(err) => err.to_string(),
        },
        EntryType::EntryConfChangeV2 => match ConfChangeV2::decode(&e.data) {
            Ok(cc) => conf_changes_to_string(&cc.changes),
            Err(err) => err.to_string(),
        },
    };
    let sep = if formatted.is_empty() { "" } else { " " };
    format!(
        "{}/{} {}{}{}",
        e.term, e.index, e.entry_type, sep, formatted
    )
}

/// Describes a ConfChangeI deterministically.
pub fn describe_conf_change(cc: &ConfChangeI) -> String {
    let cv2 = cc.as_v2();
    let mut b = format!("transition:{}", cv2.transition);
    for c in &cv2.changes {
        let _ = write!(
            b,
            " changes:{{type:{} node_id:{}}}",
            c.change_type, c.node_id
        );
    }
    if !cv2.context.is_empty() {
        let _ = write!(b, " context:{}", go_quote(&cv2.context));
    }
    b
}

/// Describes entries, one per line.
pub fn describe_entries(ents: &[Entry], f: EntryFormatter<'_>) -> String {
    let mut buf = String::new();
    for e in ents {
        buf.push_str(&describe_entry(e, f));
        buf.push('\n');
    }
    buf
}

/// Protobuf encoding size of the entries.
pub fn ents_size(ents: &[Entry]) -> u64 {
    ents.iter().map(entry_size).sum()
}

/// Length of the longest prefix of `ents` within `max_size` bytes (at least 1 if non-empty).
pub fn limit_size(ents: &[Entry], max_size: u64) -> usize {
    if ents.is_empty() {
        return 0;
    }
    let mut size = entry_size(&ents[0]);
    for (limit, e) in ents.iter().enumerate().skip(1) {
        size += entry_size(e);
        if size > max_size {
            return limit;
        }
    }
    ents.len()
}

/// Payload size of an entry.
pub fn payload_size(e: &Entry) -> u64 {
    e.data.len() as u64
}

/// Payload size of the entries.
pub fn payloads_size(ents: &[Entry]) -> u64 {
    ents.iter().map(payload_size).sum()
}

pub(crate) fn assert_conf_states_equivalent(
    l: &dyn crate::logger::Logger,
    cs1: &ConfState,
    cs2: &ConfState,
) {
    if let Err(e) = cs1.equivalent(cs2) {
        panicf!(l, "{}", e);
    }
}
