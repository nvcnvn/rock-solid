// Copyright 2015 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// doc.go and the error variables of raft.go, rawnode.go and storage.go.
// Licensed under the Apache License 2.0.

//! etcd-raft ported to sans-IO Rust: no threads, clocks, OS randomness or protobuf.

#[macro_use]
pub mod logger;

mod bootstrap;
pub mod confchange;
pub mod log;
pub mod log_unstable;
pub mod node;
pub mod quorum;
pub mod raft;
pub mod raftpb;
pub mod rawnode;
pub mod read_only;
pub mod status;
pub mod storage;
pub mod tracker;
pub mod types;
pub mod util;

#[cfg(test)]
mod log_test;
#[cfg(test)]
mod log_unstable_test;
#[cfg(test)]
mod raft_paper_test;
#[cfg(test)]
mod raft_test;
#[cfg(test)]
mod rawnode_test;
#[cfg(test)]
mod storage_test;
#[cfg(test)]
mod testutil;
#[cfg(test)]
mod tracker_test;

use std::fmt;

pub use node::{Peer, Ready, SnapshotStatus, SoftState, is_empty_hard_state, is_empty_snap};
pub use raft::{
    Config, LOCAL_APPEND_THREAD, LOCAL_APPLY_THREAD, NONE, Raft, ReadOnlyOption, ReadState,
    StateType,
};
pub use rawnode::{ProgressType, RawNode, must_sync};
pub use status::{BasicStatus, Status};
pub use storage::{MemoryStorage, Storage};

/// Errors returned by raft and Storage (the Go error variables).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// ErrCompacted.
    Compacted,
    /// ErrSnapOutOfDate.
    SnapOutOfDate,
    /// ErrUnavailable.
    Unavailable,
    /// ErrSnapshotTemporarilyUnavailable.
    SnapshotTemporarilyUnavailable,
    /// ErrProposalDropped.
    ProposalDropped,
    /// ErrStepLocalMsg.
    StepLocalMsg,
    /// ErrStepPeerNotFound.
    StepPeerNotFound,
    /// Any other error.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::Compacted => "requested index is unavailable due to compaction",
            Error::SnapOutOfDate => "requested index is older than the existing snapshot",
            Error::Unavailable => "requested entry at index is unavailable",
            Error::SnapshotTemporarilyUnavailable => "snapshot is temporarily unavailable",
            Error::ProposalDropped => "raft proposal dropped",
            Error::StepLocalMsg => "raft: cannot step raft local message",
            Error::StepPeerNotFound => "raft: cannot step as peer not found",
            Error::Other(s) => s,
        })
    }
}

impl std::error::Error for Error {}
