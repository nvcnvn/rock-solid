// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// quorum/quorum.go and quorum/voteresult_string.go. Licensed under the Apache License 2.0.

//! Majority and joint quorums.

mod joint;
mod majority;

use std::collections::BTreeMap;
use std::fmt;

pub use joint::JointConfig;
pub use majority::MajorityConfig;

/// A raft log position (`quorum.Index`).
pub type Index = u64;

/// Formats an index, printing `u64::MAX` as ∞ (`Index.String` in Go).
pub fn index_string(i: Index) -> String {
    if i == u64::MAX {
        "∞".to_string()
    } else {
        i.to_string()
    }
}

/// Looks up the acked index of a voter.
pub trait AckedIndexer {
    /// Returns the acked index of the voter, if known.
    fn acked_index(&self, voter_id: u64) -> Option<Index>;
}

/// An `AckedIndexer` backed by a map (`mapAckIndexer` in Go).
pub struct MapAckIndexer(pub BTreeMap<u64, Index>);

impl AckedIndexer for MapAckIndexer {
    fn acked_index(&self, id: u64) -> Option<Index> {
        self.0.get(&id).copied()
    }
}

/// Outcome of a vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoteResult {
    /// Neither yes nor no has reached quorum yet.
    VotePending = 1,
    /// A quorum voted no.
    VoteLost = 2,
    /// A quorum voted yes.
    VoteWon = 3,
}

impl fmt::Display for VoteResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            VoteResult::VotePending => "VotePending",
            VoteResult::VoteLost => "VoteLost",
            VoteResult::VoteWon => "VoteWon",
        })
    }
}
