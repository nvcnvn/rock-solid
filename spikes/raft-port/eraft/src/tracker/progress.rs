// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// tracker/progress.go and tracker/state.go. Licensed under the Apache License 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;

use super::Inflights;

/// State of a tracked follower.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StateType {
    /// Last index unknown; probed one append at a time.
    #[default]
    StateProbe = 0,
    /// Steady replication.
    StateReplicate = 1,
    /// Needs a snapshot.
    StateSnapshot = 2,
}

impl fmt::Display for StateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StateType::StateProbe => "StateProbe",
            StateType::StateReplicate => "StateReplicate",
            StateType::StateSnapshot => "StateSnapshot",
        })
    }
}

/// A follower's progress in the view of the leader.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// Index up to which the follower's log matches the leader's.
    pub match_: u64,
    /// Next index to send.
    pub next: u64,
    sent_commit: u64,
    /// Replication state.
    pub state: StateType,
    /// Pending snapshot index in StateSnapshot.
    pub pending_snapshot: u64,
    /// True if the follower was heard from recently.
    pub recent_active: bool,
    /// True if the MsgApp flow is throttled.
    pub msg_app_flow_paused: bool,
    /// Sliding window of in-flight appends.
    pub inflights: Inflights,
    /// True if this progress tracks a learner.
    pub is_learner: bool,
}

impl Progress {
    /// Creates a progress with the given fields and zero sentCommit.
    pub fn new(
        match_: u64,
        next: u64,
        inflights: Inflights,
        is_learner: bool,
        recent_active: bool,
    ) -> Progress {
        Progress {
            match_,
            next,
            inflights,
            is_learner,
            recent_active,
            ..Default::default()
        }
    }

    /// Moves into `state`, resetting flow control.
    pub fn reset_state(&mut self, state: StateType) {
        self.msg_app_flow_paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.inflights.reset();
    }

    /// Transitions into StateProbe.
    pub fn become_probe(&mut self) {
        if self.state == StateType::StateSnapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(StateType::StateProbe);
            self.next = (self.match_ + 1).max(pending_snapshot + 1);
        } else {
            self.reset_state(StateType::StateProbe);
            self.next = self.match_ + 1;
        }
        self.sent_commit = self.sent_commit.min(self.next - 1);
    }

    /// Transitions into StateReplicate.
    pub fn become_replicate(&mut self) {
        self.reset_state(StateType::StateReplicate);
        self.next = self.match_ + 1;
    }

    /// Transitions into StateSnapshot.
    pub fn become_snapshot(&mut self, snapshoti: u64) {
        self.reset_state(StateType::StateSnapshot);
        self.pending_snapshot = snapshoti;
        self.next = snapshoti + 1;
        self.sent_commit = snapshoti;
    }

    /// Updates the progress on `entries` entries of `bytes` bytes being sent.
    pub fn sent_entries(&mut self, entries: usize, bytes: u64) {
        match self.state {
            StateType::StateReplicate => {
                if entries > 0 {
                    self.next += entries as u64;
                    self.inflights.add(self.next - 1, bytes);
                }
                self.msg_app_flow_paused = self.inflights.full();
            }
            StateType::StateProbe => {
                if entries > 0 {
                    self.msg_app_flow_paused = true;
                }
            }
            StateType::StateSnapshot => panic!("sending append in unhandled state {}", self.state),
        }
    }

    /// True if sending `index` can advance the follower's commit index.
    pub fn can_bump_commit(&self, index: u64) -> bool {
        index > self.sent_commit && self.sent_commit < self.next - 1
    }

    /// Records the highest commit index sent.
    pub fn sent_commit(&mut self, commit: u64) {
        self.sent_commit = commit;
    }

    /// Handles an acked index; false if it is outdated.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        if n <= self.match_ {
            return false;
        }
        self.match_ = n;
        self.next = self.next.max(n + 1);
        self.msg_app_flow_paused = false;
        true
    }

    /// Handles a rejected MsgApp; false if the rejection is stale.
    pub fn maybe_decr_to(&mut self, rejected: u64, match_hint: u64) -> bool {
        if self.state == StateType::StateReplicate {
            if rejected <= self.match_ {
                return false;
            }
            self.next = self.match_ + 1;
            self.sent_commit = self.sent_commit.min(self.next - 1);
            return true;
        }
        if self.next.wrapping_sub(1) != rejected {
            return false;
        }
        self.next = rejected.min(match_hint + 1).max(self.match_ + 1);
        self.sent_commit = self.sent_commit.min(self.next - 1);
        self.msg_app_flow_paused = false;
        true
    }

    /// True if sending entries to this node is throttled.
    pub fn is_paused(&self) -> bool {
        match self.state {
            StateType::StateProbe | StateType::StateReplicate => self.msg_app_flow_paused,
            StateType::StateSnapshot => true,
        }
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} match={} next={}", self.state, self.match_, self.next)?;
        if self.is_learner {
            f.write_str(" learner")?;
        }
        if self.is_paused() {
            f.write_str(" paused")?;
        }
        if self.pending_snapshot > 0 {
            write!(f, " pendingSnap={}", self.pending_snapshot)?;
        }
        if !self.recent_active {
            f.write_str(" inactive")?;
        }
        let n = self.inflights.count();
        if n > 0 {
            write!(f, " inflight={n}")?;
            if self.inflights.full() {
                f.write_str("[full]")?;
            }
        }
        Ok(())
    }
}

/// Map of progresses by node ID.
pub type ProgressMap = BTreeMap<u64, Progress>;

/// Prints a ProgressMap in key order, one Progress per line (`ProgressMap.String`).
pub fn progress_map_string(m: &ProgressMap) -> String {
    let mut buf = String::new();
    for (id, pr) in m {
        let _ = writeln!(buf, "{id}: {pr}");
    }
    buf
}
