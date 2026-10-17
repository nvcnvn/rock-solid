// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// quorum/joint.go. Licensed under the Apache License 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::{AckedIndexer, Index, MajorityConfig, VoteResult};

/// Two majority configurations; decisions need both.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JointConfig(pub [MajorityConfig; 2]);

impl fmt::Display for JointConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0[1].is_empty() {
            return write!(f, "{}&&{}", self.0[0], self.0[1]);
        }
        write!(f, "{}", self.0[0])
    }
}

impl JointConfig {
    /// Returns the set of voters in either half.
    pub fn ids(&self) -> BTreeSet<u64> {
        self.0[0].0.union(&self.0[1].0).copied().collect()
    }

    /// Returns a multi-line representation of the commit indexes.
    pub fn describe(&self, l: &dyn AckedIndexer) -> String {
        MajorityConfig(self.ids()).describe(l)
    }

    /// Returns the largest index committed in both halves.
    pub fn committed_index(&self, l: &dyn AckedIndexer) -> Index {
        let idx0 = self.0[0].committed_index(l);
        let idx1 = self.0[1].committed_index(l);
        idx0.min(idx1)
    }

    /// Returns the joint vote result.
    pub fn vote_result(&self, votes: &BTreeMap<u64, bool>) -> VoteResult {
        let r1 = self.0[0].vote_result(votes);
        let r2 = self.0[1].vote_result(votes);
        if r1 == r2 {
            return r1;
        }
        if r1 == VoteResult::VoteLost || r2 == VoteResult::VoteLost {
            return VoteResult::VoteLost;
        }
        VoteResult::VotePending
    }
}
