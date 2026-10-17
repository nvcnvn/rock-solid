// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// status.go. Licensed under the Apache License 2.0.

use std::collections::BTreeMap;
use std::fmt;

use crate::node::SoftState;
use crate::raft::{Raft, StateType};
use crate::raftpb::HardState;
use crate::storage::Storage;
use crate::tracker::{self, Progress};

/// Basic information about a raft peer; does not allocate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BasicStatus {
    /// Node ID.
    pub id: u64,
    /// Hard state.
    pub hard_state: HardState,
    /// Soft state.
    pub soft_state: SoftState,
    /// Applied index.
    pub applied: u64,
    /// Lead transferee.
    pub lead_transferee: u64,
}

/// Information about a raft peer and its view of the group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    /// Basic status.
    pub basic: BasicStatus,
    /// Active configuration.
    pub config: tracker::Config,
    /// Progress of every member; only populated on the leader.
    pub progress: BTreeMap<u64, Progress>,
}

pub(crate) fn get_progress_copy<S: Storage>(r: &Raft<S>) -> BTreeMap<u64, Progress> {
    r.trk.progress.clone()
}

pub(crate) fn get_basic_status<S: Storage>(r: &Raft<S>) -> BasicStatus {
    BasicStatus {
        id: r.id,
        hard_state: r.hard_state(),
        soft_state: r.soft_state(),
        applied: r.raft_log.applied,
        lead_transferee: r.lead_transferee(),
    }
}

pub(crate) fn get_status<S: Storage>(r: &Raft<S>) -> Status {
    let basic = get_basic_status(r);
    let progress = if basic.soft_state.raft_state == StateType::StateLeader {
        get_progress_copy(r)
    } else {
        BTreeMap::new()
    };
    Status {
        basic,
        config: r.trk.config.clone_go(),
        progress,
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.basic;
        write!(
            f,
            "{{\"id\":\"{:x}\",\"term\":{},\"vote\":\"{:x}\",\"commit\":{},\"lead\":\"{:x}\",\"raftState\":{:?},\"applied\":{},\"progress\":{{",
            b.id,
            b.hard_state.term,
            b.hard_state.vote,
            b.hard_state.commit,
            b.soft_state.lead,
            b.soft_state.raft_state.to_string(),
            b.applied
        )?;
        let mut first = true;
        for (k, v) in &self.progress {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            write!(
                f,
                "\"{:x}\":{{\"match\":{},\"next\":{},\"state\":{:?}}}",
                k,
                v.match_,
                v.next,
                v.state.to_string()
            )?;
        }
        write!(f, "}},\"leadtransferee\":\"{:x}\"}}", b.lead_transferee)
    }
}
