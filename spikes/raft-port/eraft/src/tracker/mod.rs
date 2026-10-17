// Copyright 2019 The etcd Authors
// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// tracker/tracker.go. Licensed under the Apache License 2.0.
//
// Deviation: Go distinguishes nil from empty learner maps; the Go code keeps
// them nil whenever empty (nilAwareAdd/nilAwareDelete), so an empty BTreeSet
// stands for nil here.

//! Configuration and follower progress tracking.

mod inflights;
mod progress;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[cfg(test)]
pub(crate) use inflights::Inflight;
pub use inflights::Inflights;
pub use progress::{Progress, ProgressMap, StateType, progress_map_string};

use crate::quorum::{AckedIndexer, Index, JointConfig, MajorityConfig, VoteResult};
use crate::raftpb::ConfState;

/// The configuration tracked by a ProgressTracker.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// Voters, possibly joint.
    pub voters: JointConfig,
    /// True if a joint config is left automatically.
    pub auto_leave: bool,
    /// Learners.
    pub learners: BTreeSet<u64>,
    /// Voters in the outgoing config that become learners on leaving joint.
    pub learners_next: BTreeSet<u64>,
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "voters={}", self.voters)?;
        if !self.learners.is_empty() {
            write!(f, " learners={}", MajorityConfig(self.learners.clone()))?;
        }
        if !self.learners_next.is_empty() {
            write!(
                f,
                " learners_next={}",
                MajorityConfig(self.learners_next.clone())
            )?;
        }
        if self.auto_leave {
            f.write_str(" autoleave")?;
        }
        Ok(())
    }
}

impl Config {
    /// Go's `Config.Clone`, which copies everything but AutoLeave.
    pub fn clone_go(&self) -> Config {
        Config {
            voters: self.voters.clone(),
            auto_leave: false,
            learners: self.learners.clone(),
            learners_next: self.learners_next.clone(),
        }
    }
}

/// Tracks the active configuration and the progress of its members.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProgressTracker {
    /// Active configuration.
    pub config: Config,
    /// Progress of every member.
    pub progress: ProgressMap,
    /// Votes received in the current election.
    pub votes: BTreeMap<u64, bool>,
    /// Max in-flight messages per follower.
    pub max_inflight: usize,
    /// Max in-flight bytes per follower.
    pub max_inflight_bytes: u64,
}

struct MatchAckIndexer<'a>(&'a ProgressMap);

impl AckedIndexer for MatchAckIndexer<'_> {
    fn acked_index(&self, id: u64) -> Option<Index> {
        self.0.get(&id).map(|pr| pr.match_)
    }
}

impl ProgressTracker {
    /// Initializes a ProgressTracker (`MakeProgressTracker`).
    pub fn new(max_inflight: usize, max_bytes: u64) -> ProgressTracker {
        ProgressTracker {
            max_inflight,
            max_inflight_bytes: max_bytes,
            ..Default::default()
        }
    }

    /// Returns the active configuration as a ConfState.
    pub fn conf_state(&self) -> ConfState {
        ConfState {
            voters: self.config.voters.0[0].slice(),
            voters_outgoing: self.config.voters.0[1].slice(),
            learners: self.config.learners.iter().copied().collect(),
            learners_next: self.config.learners_next.iter().copied().collect(),
            auto_leave: self.config.auto_leave,
        }
    }

    /// True if the only voter is the leader.
    pub fn is_singleton(&self) -> bool {
        self.config.voters.0[0].len() == 1 && self.config.voters.0[1].is_empty()
    }

    /// Largest index acknowledged by a quorum.
    pub fn committed(&self) -> u64 {
        self.config
            .voters
            .committed_index(&MatchAckIndexer(&self.progress))
    }

    /// Calls `f` for every progress in ID order.
    pub fn visit(&mut self, mut f: impl FnMut(u64, &mut Progress)) {
        for (id, pr) in self.progress.iter_mut() {
            f(*id, pr);
        }
    }

    /// True if a quorum was recently active.
    pub fn quorum_active(&self) -> bool {
        let mut votes = BTreeMap::new();
        for (id, pr) in &self.progress {
            if pr.is_learner {
                continue;
            }
            votes.insert(*id, pr.recent_active);
        }
        self.config.voters.vote_result(&votes) == VoteResult::VoteWon
    }

    /// Sorted voter IDs.
    pub fn voter_nodes(&self) -> Vec<u64> {
        self.config.voters.ids().into_iter().collect()
    }

    /// Sorted learner IDs.
    pub fn learner_nodes(&self) -> Vec<u64> {
        self.config.learners.iter().copied().collect()
    }

    /// Prepares for a new round of vote counting.
    pub fn reset_votes(&mut self) {
        self.votes = BTreeMap::new();
    }

    /// Records a vote from `id` if none is recorded yet.
    pub fn record_vote(&mut self, id: u64, v: bool) {
        self.votes.entry(id).or_insert(v);
    }

    /// Returns (granted, rejected, result).
    pub fn tally_votes(&self) -> (usize, usize, VoteResult) {
        let mut granted = 0;
        let mut rejected = 0;
        for (id, pr) in &self.progress {
            if pr.is_learner {
                continue;
            }
            match self.votes.get(id) {
                None => {}
                Some(true) => granted += 1,
                Some(false) => rejected += 1,
            }
        }
        (
            granted,
            rejected,
            self.config.voters.vote_result(&self.votes),
        )
    }
}
