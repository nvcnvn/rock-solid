// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// raft.go. Licensed under the Apache License 2.0.
//
// Deviations:
// - Storage is a type parameter passed next to Config, not a Config field.
// - Election timeouts come from ChaCha8 seeded by Config::rand_seed, on stream
//   Config::id, instead of the process-wide crypto/rand source (ElectionRng).
// - Step takes the message by value, so MsgProp entries are moved into the log
//   instead of cloned.
// - Tracing hooks (state_trace.go, build tag with_tla) are not ported.

use std::fmt;
use std::rc::Rc;

use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

use crate::Error;
use crate::confchange::{self, Changer};
use crate::log::{NO_LIMIT, RaftLog};
use crate::logger::{Logger, NopLogger};
use crate::node::{SoftState, conf_change_to_msg, is_empty_hard_state, is_empty_snap};
use crate::quorum::VoteResult;
use crate::raftpb::{
    ConfChange, ConfChangeI, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
    MessageType, Snapshot,
};
use crate::read_only::ReadOnly;
use crate::storage::Storage;
use crate::tracker::{self, Inflights, Progress, ProgressMap, ProgressTracker};
use crate::types::{EntryId, LogSlice};
use crate::util::{
    assert_conf_states_equivalent, describe_conf_change, ents_size, is_local_msg_target,
    payloads_size, vote_resp_msg_type,
};

/// Placeholder node ID used when there is no leader.
pub const NONE: u64 = 0;
/// Target of MsgStorageAppend messages.
pub const LOCAL_APPEND_THREAD: u64 = u64::MAX;
/// Target of MsgStorageApply messages.
pub const LOCAL_APPLY_THREAD: u64 = u64::MAX - 1;

/// Role of a node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StateType {
    /// Follower.
    #[default]
    StateFollower = 0,
    /// Candidate.
    StateCandidate = 1,
    /// Leader.
    StateLeader = 2,
    /// Pre-candidate.
    StatePreCandidate = 3,
}

impl fmt::Display for StateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StateType::StateFollower => "StateFollower",
            StateType::StateCandidate => "StateCandidate",
            StateType::StateLeader => "StateLeader",
            StateType::StatePreCandidate => "StatePreCandidate",
        })
    }
}

/// How read-only requests are served.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadOnlyOption {
    /// Confirm leadership with a quorum.
    #[default]
    ReadOnlySafe,
    /// Rely on the leader lease.
    ReadOnlyLeaseBased,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CampaignType {
    PreElection,
    Election,
    Transfer,
}

impl CampaignType {
    fn as_str(self) -> &'static str {
        match self {
            CampaignType::PreElection => "CampaignPreElection",
            CampaignType::Election => "CampaignElection",
            CampaignType::Transfer => "CampaignTransfer",
        }
    }
}

/// State for a read-only query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadState {
    /// Read index.
    pub index: u64,
    /// Request context.
    pub request_ctx: Vec<u8>,
}

/// Parameters to start a raft.
#[derive(Clone)]
pub struct Config {
    /// Identity of the local raft; must not be 0.
    pub id: u64,
    /// Ticks between elections.
    pub election_tick: usize,
    /// Ticks between heartbeats.
    pub heartbeat_tick: usize,
    /// Last applied index, when restarting.
    pub applied: u64,
    /// Write to local storage through messages instead of Ready/Advance.
    pub async_storage_writes: bool,
    /// Max bytes per append message.
    pub max_size_per_msg: u64,
    /// Max bytes of committed entries being applied at once.
    pub max_committed_size_per_ready: u64,
    /// Max bytes of uncommitted entries on a leader (0 = no limit).
    pub max_uncommitted_entries_size: u64,
    /// Max in-flight append messages per follower.
    pub max_inflight_msgs: usize,
    /// Max in-flight append bytes per follower (0 = no limit).
    pub max_inflight_bytes: u64,
    /// Leader steps down when a quorum is not active.
    pub check_quorum: bool,
    /// Enables the pre-vote phase.
    pub pre_vote: bool,
    /// How read-only requests are served.
    pub read_only_option: ReadOnlyOption,
    /// Logger; a discarding logger if None.
    pub logger: Option<Rc<dyn Logger>>,
    /// Followers drop proposals instead of forwarding them.
    pub disable_proposal_forwarding: bool,
    /// Skips propose-time validation of configuration changes.
    pub disable_conf_change_validation: bool,
    /// Leader steps down when removed or demoted.
    pub step_down_on_removal: bool,
    /// Seed of the election-timeout RNG (deviation from Go).
    pub rand_seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 0,
            election_tick: 0,
            heartbeat_tick: 0,
            applied: 0,
            async_storage_writes: false,
            max_size_per_msg: 0,
            max_committed_size_per_ready: 0,
            max_uncommitted_entries_size: 0,
            max_inflight_msgs: 0,
            max_inflight_bytes: 0,
            check_quorum: false,
            pre_vote: false,
            read_only_option: ReadOnlyOption::ReadOnlySafe,
            logger: None,
            disable_proposal_forwarding: false,
            disable_conf_change_validation: false,
            step_down_on_removal: false,
            rand_seed: 0,
        }
    }
}

impl Config {
    /// Checks the config and fills in defaults.
    pub fn validate(&mut self) -> Result<(), String> {
        if self.id == NONE {
            return Err("cannot use none as id".into());
        }
        if is_local_msg_target(self.id) {
            return Err("cannot use local target as id".into());
        }
        if self.heartbeat_tick == 0 {
            return Err("heartbeat tick must be greater than 0".into());
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err("election tick must be greater than heartbeat tick".into());
        }
        if self.max_uncommitted_entries_size == 0 {
            self.max_uncommitted_entries_size = NO_LIMIT;
        }
        if self.max_committed_size_per_ready == 0 {
            self.max_committed_size_per_ready = self.max_size_per_msg;
        }
        if self.max_inflight_msgs == 0 {
            return Err("max inflight messages must be greater than 0".into());
        }
        if self.max_inflight_bytes == 0 {
            self.max_inflight_bytes = NO_LIMIT;
        } else if self.max_inflight_bytes < self.max_size_per_msg {
            return Err("max inflight bytes must be >= max message size".into());
        }
        if self.logger.is_none() {
            self.logger = Some(Rc::new(NopLogger));
        }
        if self.read_only_option == ReadOnlyOption::ReadOnlyLeaseBased && !self.check_quorum {
            return Err(
                "CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased".into(),
            );
        }
        Ok(())
    }
}

/// Seeded source of election timeouts. Keeps only the seed, stream and draw
/// count, and replays ChaCha8 at that position on each draw, so a raft group
/// carries 24 bytes instead of a 300-byte buffered generator.
#[derive(Clone, Copy, Debug)]
struct ElectionRng {
    seed: u64,
    stream: u64,
    draws: u64,
}

impl ElectionRng {
    fn intn(&mut self, n: usize) -> usize {
        let n = n as u64;
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        rng.set_stream(self.stream);
        rng.set_word_pos(u128::from(self.draws) * 2);
        let zone = u64::MAX - (u64::MAX - n + 1) % n;
        loop {
            let v = rng.next_u64();
            self.draws += 1;
            if v <= zone {
                return (v % n) as usize;
            }
        }
    }
}

pub(crate) type StepFn<S> = fn(&mut Raft<S>, Message) -> Result<(), Error>;

/// The raft state machine.
pub struct Raft<S: Storage> {
    /// Node ID.
    pub id: u64,
    /// Current term.
    pub term: u64,
    /// Vote in the current term.
    pub vote: u64,
    pub(crate) read_states: Vec<ReadState>,
    /// The log.
    pub raft_log: RaftLog<S>,
    pub(crate) max_msg_size: u64,
    pub(crate) max_uncommitted_size: u64,
    /// Configuration and progress.
    pub trk: ProgressTracker,
    /// Current role.
    pub state: StateType,
    pub(crate) is_learner: bool,
    /// Messages to send immediately.
    pub msgs: Vec<Message>,
    /// Messages to send after unstable state is persisted.
    pub msgs_after_append: Vec<Message>,
    /// Current leader.
    pub lead: u64,
    pub(crate) lead_transferee: u64,
    pub(crate) pending_conf_index: u64,
    pub(crate) disable_conf_change_validation: bool,
    pub(crate) uncommitted_size: u64,
    pub(crate) read_only: ReadOnly,
    pub(crate) election_elapsed: usize,
    pub(crate) heartbeat_elapsed: usize,
    pub(crate) check_quorum: bool,
    pub(crate) pre_vote: bool,
    pub(crate) heartbeat_timeout: usize,
    pub(crate) election_timeout: usize,
    pub(crate) randomized_election_timeout: usize,
    pub(crate) disable_proposal_forwarding: bool,
    pub(crate) step_down_on_removal: bool,
    pub(crate) tick: fn(&mut Raft<S>),
    pub(crate) step: StepFn<S>,
    pub(crate) logger: Rc<dyn Logger>,
    pub(crate) pending_read_index_messages: Vec<Message>,
    rng: ElectionRng,
}

enum ScanStop {
    Break,
    Err(Error),
}

impl From<Error> for ScanStop {
    fn from(e: Error) -> Self {
        ScanStop::Err(e)
    }
}

impl<S: Storage> Raft<S> {
    /// Creates a raft from the config and storage (`newRaft`).
    pub fn new(c: &mut Config, storage: S) -> Raft<S> {
        if let Err(e) = c.validate() {
            panic!("{e}");
        }
        let logger = c.logger.clone().unwrap_or_else(|| Rc::new(NopLogger));
        let raftlog =
            RaftLog::new_with_size(storage, logger.clone(), c.max_committed_size_per_ready);
        let (hs, cs) = raftlog
            .storage
            .initial_state()
            .unwrap_or_else(|e| panic!("{e}"));
        let mut r = Raft {
            id: c.id,
            term: 0,
            vote: 0,
            read_states: Vec::new(),
            raft_log: raftlog,
            max_msg_size: c.max_size_per_msg,
            max_uncommitted_size: c.max_uncommitted_entries_size,
            trk: ProgressTracker::new(c.max_inflight_msgs, c.max_inflight_bytes),
            state: StateType::StateFollower,
            is_learner: false,
            msgs: Vec::new(),
            msgs_after_append: Vec::new(),
            lead: NONE,
            lead_transferee: NONE,
            pending_conf_index: 0,
            disable_conf_change_validation: c.disable_conf_change_validation,
            uncommitted_size: 0,
            read_only: ReadOnly::new(c.read_only_option),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            check_quorum: c.check_quorum,
            pre_vote: c.pre_vote,
            heartbeat_timeout: c.heartbeat_tick,
            election_timeout: c.election_tick,
            randomized_election_timeout: 0,
            disable_proposal_forwarding: c.disable_proposal_forwarding,
            step_down_on_removal: c.step_down_on_removal,
            tick: Raft::tick_election,
            step: step_follower,
            logger,
            pending_read_index_messages: Vec::new(),
            rng: ElectionRng {
                seed: c.rand_seed,
                stream: c.id,
                draws: 0,
            },
        };
        let last_id = r.raft_log.last_entry_id();
        let (cfg, trk) = match confchange::restore(
            Changer {
                tracker: &r.trk,
                last_index: last_id.index,
            },
            &cs,
        ) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        let cs2 = r.switch_to_config(cfg, trk);
        assert_conf_states_equivalent(&*r.logger, &cs, &cs2);
        if !is_empty_hard_state(Some(&hs)) {
            r.load_state(&hs);
        }
        if c.applied > 0 {
            r.raft_log.applied_to(c.applied, 0);
        }
        r.become_follower(r.term, NONE);
        let nodes_strs: Vec<String> = r
            .trk
            .voter_nodes()
            .iter()
            .map(|n| format!("{n:x}"))
            .collect();
        infof!(
            r.logger,
            "newRaft {:x} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: {}, lastterm: {}]",
            r.id,
            nodes_strs.join(","),
            r.term,
            r.raft_log.committed,
            r.raft_log.applied,
            last_id.index,
            last_id.term
        );
        r
    }

    /// True if a leader is known.
    pub fn has_leader(&self) -> bool {
        self.lead != NONE
    }

    /// Current soft state.
    pub fn soft_state(&self) -> SoftState {
        SoftState {
            lead: self.lead,
            raft_state: self.state,
        }
    }

    /// Current hard state.
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            vote: self.vote,
            commit: self.raft_log.committed,
        }
    }

    /// The logger.
    pub fn logger(&self) -> &dyn Logger {
        &*self.logger
    }

    /// The lead transferee, or NONE.
    pub fn lead_transferee(&self) -> u64 {
        self.lead_transferee
    }

    /// Overrides the randomized election timeout (test hook).
    pub fn set_randomized_election_timeout(&mut self, v: usize) {
        self.randomized_election_timeout = v;
    }

    /// Runs one tick.
    pub fn tick(&mut self) {
        (self.tick)(self)
    }

    /// Ticks the election timer without any other processing (`TickQuiesced`).
    pub fn tick_quiesced(&mut self) {
        self.election_elapsed += 1;
    }

    fn pr_mut(&mut self, id: u64) -> &mut Progress {
        self.trk.progress.get_mut(&id).expect("progress")
    }

    /// Visits progress IDs in order while allowing `f` to borrow self mutably.
    fn visit_ids(&mut self, mut f: impl FnMut(&mut Self, u64)) {
        let mut next = 0u64;
        while let Some((&id, _)) = self.trk.progress.range(next..).next() {
            f(self, id);
            if id == u64::MAX {
                break;
            }
            next = id + 1;
        }
    }

    fn send(&mut self, mut m: Message) {
        if m.from == NONE {
            m.from = self.id;
        }
        let t = m.msg_type;
        if matches!(
            t,
            MessageType::MsgVote
                | MessageType::MsgVoteResp
                | MessageType::MsgPreVote
                | MessageType::MsgPreVoteResp
        ) {
            if m.term == 0 {
                panicf!(self.logger, "term should be set when sending {}", t);
            }
        } else {
            if m.term != 0 {
                panicf!(
                    self.logger,
                    "term should not be set when sending {} (was {})",
                    t,
                    m.term
                );
            }
            if t != MessageType::MsgProp && t != MessageType::MsgReadIndex {
                m.term = self.term;
            }
        }
        if matches!(
            t,
            MessageType::MsgAppResp | MessageType::MsgVoteResp | MessageType::MsgPreVoteResp
        ) {
            self.msgs_after_append.push(m);
        } else {
            if m.to == self.id {
                panicf!(
                    self.logger,
                    "message should not be self-addressed when sending {}",
                    t
                );
            }
            self.msgs.push(m);
        }
    }

    pub(crate) fn send_append(&mut self, to: u64) {
        self.maybe_send_append(to, true);
    }

    pub(crate) fn maybe_send_append(&mut self, to: u64, send_if_empty: bool) -> bool {
        let pr = &self.trk.progress[&to];
        if pr.is_paused() {
            return false;
        }
        let prev_index = pr.next - 1;
        let Ok(prev_term) = self.raft_log.term(prev_index) else {
            return self.maybe_send_snapshot(to);
        };
        let mut ents = Vec::new();
        let mut failed = false;
        if pr.state != tracker::StateType::StateReplicate || !pr.inflights.full() {
            match self.raft_log.entries(pr.next, self.max_msg_size) {
                Ok(e) => ents = e,
                Err(_) => failed = true,
            }
        }
        if ents.is_empty() && !send_if_empty {
            return false;
        }
        if failed {
            return self.maybe_send_snapshot(to);
        }
        let n = ents.len();
        let bytes = payloads_size(&ents);
        let committed = self.raft_log.committed;
        self.send(Message {
            to,
            msg_type: MessageType::MsgApp,
            index: prev_index,
            log_term: prev_term,
            entries: ents,
            commit: committed,
            ..Default::default()
        });
        let pr = self.pr_mut(to);
        pr.sent_entries(n, bytes);
        pr.sent_commit(committed);
        true
    }

    fn maybe_send_snapshot(&mut self, to: u64) -> bool {
        if !self.trk.progress[&to].recent_active {
            debugf!(
                self.logger,
                "ignore sending snapshot to {:x} since it is not recently active",
                to
            );
            return false;
        }
        let snapshot = match self.raft_log.snapshot() {
            Ok(s) => s,
            Err(Error::SnapshotTemporarilyUnavailable) => {
                debugf!(
                    self.logger,
                    "{:x} failed to send snapshot to {:x} because snapshot is temporarily unavailable",
                    self.id,
                    to
                );
                return false;
            }
            Err(e) => panic!("{e}"),
        };
        if is_empty_snap(Some(&snapshot)) {
            panic!("need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.metadata.index, snapshot.metadata.term);
        debugf!(
            self.logger,
            "{:x} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {:x} [{}]",
            self.id,
            self.raft_log.first_index(),
            self.raft_log.committed,
            sindex,
            sterm,
            to,
            self.trk.progress[&to]
        );
        self.pr_mut(to).become_snapshot(sindex);
        debugf!(
            self.logger,
            "{:x} paused sending replication messages to {:x} [{}]",
            self.id,
            to,
            self.trk.progress[&to]
        );
        self.send(Message {
            to,
            msg_type: MessageType::MsgSnap,
            snapshot: Some(Box::new(snapshot)),
            ..Default::default()
        });
        true
    }

    pub(crate) fn send_heartbeat(&mut self, to: u64, ctx: Vec<u8>) {
        let commit = self.trk.progress[&to].match_.min(self.raft_log.committed);
        self.send(Message {
            to,
            msg_type: MessageType::MsgHeartbeat,
            commit,
            context: ctx,
            ..Default::default()
        });
        self.pr_mut(to).sent_commit(commit);
    }

    pub(crate) fn bcast_append(&mut self) {
        self.visit_ids(|r, id| {
            if id == r.id {
                return;
            }
            r.send_append(id);
        });
    }

    pub(crate) fn bcast_heartbeat(&mut self) {
        let ctx = self.read_only.heartbeat_ctx();
        self.bcast_heartbeat_with_ctx(ctx);
    }

    fn bcast_heartbeat_with_ctx(&mut self, ctx: Vec<u8>) {
        self.visit_ids(|r, id| {
            if id == r.id {
                return;
            }
            r.send_heartbeat(id, ctx.clone());
        });
    }

    pub(crate) fn applied_to(&mut self, index: u64, size: u64) {
        let old_applied = self.raft_log.applied;
        let new_applied = index.max(old_applied);
        self.raft_log.applied_to(new_applied, size);
        if self.trk.config.auto_leave
            && new_applied >= self.pending_conf_index
            && self.state == StateType::StateLeader
        {
            let m = conf_change_to_msg(None);
            match self.step(m) {
                Err(e) => debugf!(
                    self.logger,
                    "not initiating automatic transition out of joint configuration {}: {}",
                    self.trk.config,
                    e
                ),
                Ok(()) => {
                    infof!(
                        self.logger,
                        "initiating automatic transition out of joint configuration {}",
                        self.trk.config
                    )
                }
            }
        }
    }

    pub(crate) fn applied_snap(&mut self, snap: &Snapshot) {
        let index = snap.metadata.index;
        self.raft_log.stable_snap_to(index);
        self.applied_to(index, 0);
    }

    pub(crate) fn maybe_commit(&mut self) -> bool {
        let index = self.trk.committed();
        self.raft_log.maybe_commit(EntryId {
            term: self.term,
            index,
        })
    }

    pub(crate) fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = NONE;
        }
        self.lead = NONE;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.reset_randomized_election_timeout();
        self.abort_leader_transfer();
        self.trk.reset_votes();
        let last_index = self.raft_log.last_index();
        let (max_inflight, max_bytes, id) =
            (self.trk.max_inflight, self.trk.max_inflight_bytes, self.id);
        self.trk.visit(|pid, pr| {
            *pr = Progress::new(
                0,
                last_index + 1,
                Inflights::new(max_inflight, max_bytes),
                pr.is_learner,
                false,
            );
            if pid == id {
                pr.match_ = last_index;
            }
        });
        self.pending_conf_index = 0;
        self.uncommitted_size = 0;
        self.read_only = ReadOnly::new(self.read_only.option);
    }

    pub(crate) fn append_entry(&mut self, mut es: Vec<Entry>) -> bool {
        let li = self.raft_log.last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.term = self.term;
            e.index = li + 1 + i as u64;
        }
        if !self.increase_uncommitted_size(&es) {
            warningf!(
                self.logger,
                "{:x} appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
                self.id
            );
            return false;
        }
        let li = self.raft_log.append(es);
        self.send(Message {
            to: self.id,
            msg_type: MessageType::MsgAppResp,
            index: li,
            ..Default::default()
        });
        true
    }

    pub(crate) fn tick_election(&mut self) {
        self.election_elapsed += 1;
        if self.promotable() && self.past_election_timeout() {
            self.election_elapsed = 0;
            if let Err(e) = self.step(Message {
                from: self.id,
                msg_type: MessageType::MsgHup,
                ..Default::default()
            }) {
                debugf!(self.logger, "error occurred during election: {}", e);
            }
        }
    }

    pub(crate) fn tick_heartbeat(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum
                && let Err(e) = self.step(Message {
                    from: self.id,
                    msg_type: MessageType::MsgCheckQuorum,
                    ..Default::default()
                })
            {
                debugf!(
                    self.logger,
                    "error occurred during checking sending heartbeat: {}",
                    e
                );
            }
            if self.state == StateType::StateLeader && self.lead_transferee != NONE {
                self.abort_leader_transfer();
            }
        }
        if self.state != StateType::StateLeader {
            return;
        }
        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            if let Err(e) = self.step(Message {
                from: self.id,
                msg_type: MessageType::MsgBeat,
                ..Default::default()
            }) {
                debugf!(
                    self.logger,
                    "error occurred during checking sending heartbeat: {}",
                    e
                );
            }
        }
    }

    /// Becomes a follower at `term` with leader `lead`.
    pub fn become_follower(&mut self, term: u64, lead: u64) {
        self.step = step_follower;
        self.reset(term);
        self.tick = Raft::tick_election;
        self.lead = lead;
        self.state = StateType::StateFollower;
        infof!(
            self.logger,
            "{:x} became follower at term {}",
            self.id,
            self.term
        );
    }

    /// Becomes a candidate.
    pub fn become_candidate(&mut self) {
        if self.state == StateType::StateLeader {
            panic!("invalid transition [leader -> candidate]");
        }
        self.step = step_candidate;
        self.reset(self.term + 1);
        self.tick = Raft::tick_election;
        self.vote = self.id;
        self.state = StateType::StateCandidate;
        infof!(
            self.logger,
            "{:x} became candidate at term {}",
            self.id,
            self.term
        );
    }

    /// Becomes a pre-candidate.
    pub fn become_pre_candidate(&mut self) {
        if self.state == StateType::StateLeader {
            panic!("invalid transition [leader -> pre-candidate]");
        }
        self.step = step_candidate;
        self.trk.reset_votes();
        self.tick = Raft::tick_election;
        self.lead = NONE;
        self.state = StateType::StatePreCandidate;
        infof!(
            self.logger,
            "{:x} became pre-candidate at term {}",
            self.id,
            self.term
        );
    }

    /// Becomes the leader.
    pub fn become_leader(&mut self) {
        if self.state == StateType::StateFollower {
            panic!("invalid transition [follower -> leader]");
        }
        self.step = step_leader;
        self.reset(self.term);
        self.tick = Raft::tick_heartbeat;
        self.lead = self.id;
        self.state = StateType::StateLeader;
        let id = self.id;
        let pr = self.pr_mut(id);
        pr.become_replicate();
        pr.recent_active = true;
        self.pending_conf_index = self.raft_log.last_index();
        if !self.append_entry(vec![Entry::default()]) {
            panicf!(self.logger, "empty entry was dropped");
        }
        infof!(
            self.logger,
            "{:x} became leader at term {}",
            self.id,
            self.term
        );
    }

    pub(crate) fn hup(&mut self, t: CampaignType) {
        if self.state == StateType::StateLeader {
            debugf!(
                self.logger,
                "{:x} ignoring MsgHup because already leader",
                self.id
            );
            return;
        }
        if !self.promotable() {
            warningf!(
                self.logger,
                "{:x} is unpromotable and can not campaign",
                self.id
            );
            return;
        }
        if self.has_unapplied_conf_changes() {
            warningf!(
                self.logger,
                "{:x} cannot campaign at term {} since there are still pending configuration changes to apply",
                self.id,
                self.term
            );
            return;
        }
        infof!(
            self.logger,
            "{:x} is starting a new election at term {}",
            self.id,
            self.term
        );
        self.campaign(t);
    }

    fn has_unapplied_conf_changes(&self) -> bool {
        if self.raft_log.applied >= self.raft_log.committed {
            return false;
        }
        let mut found = false;
        let (lo, hi) = (self.raft_log.applied + 1, self.raft_log.committed + 1);
        let page_size = self.raft_log.max_applying_ents_size;
        let res = self.raft_log.scan(lo, hi, page_size, |ents| {
            for e in ents {
                if e.entry_type == EntryType::EntryConfChange
                    || e.entry_type == EntryType::EntryConfChangeV2
                {
                    found = true;
                    return Err(ScanStop::Break);
                }
            }
            Ok(())
        });
        if let Err(ScanStop::Err(e)) = res {
            panicf!(
                self.logger,
                "error scanning unapplied entries [{}, {}): {}",
                lo,
                hi,
                e
            );
        }
        found
    }

    pub(crate) fn campaign(&mut self, t: CampaignType) {
        if !self.promotable() {
            warningf!(
                self.logger,
                "{:x} is unpromotable; campaign() should have been called",
                self.id
            );
        }
        let (term, vote_msg) = if t == CampaignType::PreElection {
            self.become_pre_candidate();
            (self.term + 1, MessageType::MsgPreVote)
        } else {
            self.become_candidate();
            (self.term, MessageType::MsgVote)
        };
        let ids = self.trk.config.voters.ids();
        for id in ids {
            if id == self.id {
                self.send(Message {
                    to: id,
                    term,
                    msg_type: vote_resp_msg_type(vote_msg),
                    ..Default::default()
                });
                continue;
            }
            let last = self.raft_log.last_entry_id();
            infof!(
                self.logger,
                "{:x} [logterm: {}, index: {}] sent {} request to {:x} at term {}",
                self.id,
                last.term,
                last.index,
                vote_msg,
                id,
                self.term
            );
            let context = if t == CampaignType::Transfer {
                t.as_str().as_bytes().to_vec()
            } else {
                Vec::new()
            };
            self.send(Message {
                to: id,
                term,
                msg_type: vote_msg,
                index: last.index,
                log_term: last.term,
                context,
                ..Default::default()
            });
        }
    }

    pub(crate) fn poll(&mut self, id: u64, t: MessageType, v: bool) -> (usize, usize, VoteResult) {
        if v {
            infof!(
                self.logger,
                "{:x} received {} from {:x} at term {}",
                self.id,
                t,
                id,
                self.term
            );
        } else {
            infof!(
                self.logger,
                "{:x} received {} rejection from {:x} at term {}",
                self.id,
                t,
                id,
                self.term
            );
        }
        self.trk.record_vote(id, v);
        self.trk.tally_votes()
    }

    /// Advances the state machine with a message.
    pub fn step(&mut self, m: Message) -> Result<(), Error> {
        if m.term == 0 {
        } else if m.term > self.term {
            if m.msg_type == MessageType::MsgVote || m.msg_type == MessageType::MsgPreVote {
                let force = m.context == CampaignType::Transfer.as_str().as_bytes();
                let in_lease = self.check_quorum
                    && self.lead != NONE
                    && self.election_elapsed < self.election_timeout;
                if !force && in_lease {
                    let last = self.raft_log.last_entry_id();
                    infof!(
                        self.logger,
                        "{:x} [logterm: {}, index: {}, vote: {:x}] ignored {} from {:x} [logterm: {}, index: {}] at term {}: lease is not expired (remaining ticks: {})",
                        self.id,
                        last.term,
                        last.index,
                        self.vote,
                        m.msg_type,
                        m.from,
                        m.log_term,
                        m.index,
                        self.term,
                        self.election_timeout - self.election_elapsed
                    );
                    return Ok(());
                }
            }
            if m.msg_type == MessageType::MsgPreVote
                || (m.msg_type == MessageType::MsgPreVoteResp && !m.reject)
            {
            } else {
                infof!(
                    self.logger,
                    "{:x} [term: {}] received a {} message with higher term from {:x} [term: {}]",
                    self.id,
                    self.term,
                    m.msg_type,
                    m.from,
                    m.term
                );
                if matches!(
                    m.msg_type,
                    MessageType::MsgApp | MessageType::MsgHeartbeat | MessageType::MsgSnap
                ) {
                    self.become_follower(m.term, m.from);
                } else {
                    self.become_follower(m.term, NONE);
                }
            }
        } else if m.term < self.term {
            if (self.check_quorum || self.pre_vote)
                && (m.msg_type == MessageType::MsgHeartbeat || m.msg_type == MessageType::MsgApp)
            {
                self.send(Message {
                    to: m.from,
                    msg_type: MessageType::MsgAppResp,
                    ..Default::default()
                });
            } else if m.msg_type == MessageType::MsgPreVote {
                let last = self.raft_log.last_entry_id();
                infof!(
                    self.logger,
                    "{:x} [logterm: {}, index: {}, vote: {:x}] rejected {} from {:x} [logterm: {}, index: {}] at term {}",
                    self.id,
                    last.term,
                    last.index,
                    self.vote,
                    m.msg_type,
                    m.from,
                    m.log_term,
                    m.index,
                    self.term
                );
                self.send(Message {
                    to: m.from,
                    term: self.term,
                    msg_type: MessageType::MsgPreVoteResp,
                    reject: true,
                    ..Default::default()
                });
            } else if m.msg_type == MessageType::MsgStorageAppendResp {
                if m.index != 0 {
                    infof!(
                        self.logger,
                        "{:x} [term: {}] ignored entry appends from a {} message with lower term [term: {}]",
                        self.id,
                        self.term,
                        m.msg_type,
                        m.term
                    );
                }
                if let Some(s) = &m.snapshot {
                    self.applied_snap(s);
                }
            } else {
                infof!(
                    self.logger,
                    "{:x} [term: {}] ignored a {} message with lower term from {:x} [term: {}]",
                    self.id,
                    self.term,
                    m.msg_type,
                    m.from,
                    m.term
                );
            }
            return Ok(());
        }

        match m.msg_type {
            MessageType::MsgHup => {
                if self.pre_vote {
                    self.hup(CampaignType::PreElection);
                } else {
                    self.hup(CampaignType::Election);
                }
            }
            MessageType::MsgStorageAppendResp => {
                if m.index != 0 {
                    self.raft_log.stable_to(EntryId {
                        term: m.log_term,
                        index: m.index,
                    });
                }
                if let Some(s) = &m.snapshot {
                    self.applied_snap(s);
                }
            }
            MessageType::MsgStorageApplyResp => {
                if let Some(last) = m.entries.last() {
                    let index = last.index;
                    self.applied_to(index, ents_size(&m.entries));
                    self.reduce_uncommitted_size(payloads_size(&m.entries));
                }
            }
            MessageType::MsgVote | MessageType::MsgPreVote => {
                let can_vote = self.vote == m.from
                    || (self.vote == NONE && self.lead == NONE)
                    || (m.msg_type == MessageType::MsgPreVote && m.term > self.term);
                let last_id = self.raft_log.last_entry_id();
                let cand_last_id = EntryId {
                    term: m.log_term,
                    index: m.index,
                };
                if can_vote && self.raft_log.is_up_to_date(cand_last_id) {
                    infof!(
                        self.logger,
                        "{:x} [logterm: {}, index: {}, vote: {:x}] cast {} for {:x} [logterm: {}, index: {}] at term {}",
                        self.id,
                        last_id.term,
                        last_id.index,
                        self.vote,
                        m.msg_type,
                        m.from,
                        cand_last_id.term,
                        cand_last_id.index,
                        self.term
                    );
                    self.send(Message {
                        to: m.from,
                        term: m.term,
                        msg_type: vote_resp_msg_type(m.msg_type),
                        ..Default::default()
                    });
                    if m.msg_type == MessageType::MsgVote {
                        self.election_elapsed = 0;
                        self.vote = m.from;
                    }
                } else {
                    infof!(
                        self.logger,
                        "{:x} [logterm: {}, index: {}, vote: {:x}] rejected {} from {:x} [logterm: {}, index: {}] at term {}",
                        self.id,
                        last_id.term,
                        last_id.index,
                        self.vote,
                        m.msg_type,
                        m.from,
                        cand_last_id.term,
                        cand_last_id.index,
                        self.term
                    );
                    self.send(Message {
                        to: m.from,
                        term: self.term,
                        msg_type: vote_resp_msg_type(m.msg_type),
                        reject: true,
                        ..Default::default()
                    });
                }
            }
            _ => {
                (self.step)(self, m)?;
            }
        }
        Ok(())
    }

    pub(crate) fn handle_append_entries(&mut self, m: Message) {
        let a = LogSlice {
            term: m.term,
            prev: EntryId {
                term: m.log_term,
                index: m.index,
            },
            entries: m.entries,
        };
        if a.prev.index < self.raft_log.committed {
            let committed = self.raft_log.committed;
            self.send(Message {
                to: m.from,
                msg_type: MessageType::MsgAppResp,
                index: committed,
                ..Default::default()
            });
            return;
        }
        if let Some(mlast_index) = self.raft_log.maybe_append(a, m.commit) {
            self.send(Message {
                to: m.from,
                msg_type: MessageType::MsgAppResp,
                index: mlast_index,
                ..Default::default()
            });
            return;
        }
        debugf!(
            self.logger,
            "{:x} [logterm: {}, index: {}] rejected MsgApp [logterm: {}, index: {}] from {:x}",
            self.id,
            self.raft_log
                .zero_term_on_out_of_bounds(self.raft_log.term(m.index)),
            m.index,
            m.log_term,
            m.index,
            m.from
        );
        let hint_index = m.index.min(self.raft_log.last_index());
        let (hint_index, hint_term) = self.raft_log.find_conflict_by_term(hint_index, m.log_term);
        self.send(Message {
            to: m.from,
            msg_type: MessageType::MsgAppResp,
            index: m.index,
            reject: true,
            reject_hint: hint_index,
            log_term: hint_term,
            ..Default::default()
        });
    }

    pub(crate) fn handle_heartbeat(&mut self, m: Message) {
        self.raft_log.commit_to(m.commit);
        self.send(Message {
            to: m.from,
            msg_type: MessageType::MsgHeartbeatResp,
            context: m.context,
            ..Default::default()
        });
    }

    pub(crate) fn handle_snapshot(&mut self, m: Message) {
        let s = m.snapshot.map(|b| *b).unwrap_or_default();
        let (sindex, sterm) = (s.metadata.index, s.metadata.term);
        if self.restore(s) {
            infof!(
                self.logger,
                "{:x} [commit: {}] restored snapshot [index: {}, term: {}]",
                self.id,
                self.raft_log.committed,
                sindex,
                sterm
            );
            let li = self.raft_log.last_index();
            self.send(Message {
                to: m.from,
                msg_type: MessageType::MsgAppResp,
                index: li,
                ..Default::default()
            });
        } else {
            infof!(
                self.logger,
                "{:x} [commit: {}] ignored snapshot [index: {}, term: {}]",
                self.id,
                self.raft_log.committed,
                sindex,
                sterm
            );
            let committed = self.raft_log.committed;
            self.send(Message {
                to: m.from,
                msg_type: MessageType::MsgAppResp,
                index: committed,
                ..Default::default()
            });
        }
    }

    /// Restores the state machine from a snapshot; false if it was ignored.
    pub fn restore(&mut self, s: Snapshot) -> bool {
        if s.metadata.index <= self.raft_log.committed {
            return false;
        }
        if self.state != StateType::StateFollower {
            warningf!(
                self.logger,
                "{:x} attempted to restore snapshot as leader; should never happen",
                self.id
            );
            self.become_follower(self.term + 1, NONE);
            return false;
        }
        let cs = s.metadata.conf_state.clone();
        let found = [&cs.voters, &cs.learners, &cs.voters_outgoing]
            .iter()
            .any(|set| set.contains(&self.id));
        if !found {
            warningf!(
                self.logger,
                "{:x} attempted to restore snapshot but it is not in the ConfState {:?}; should never happen",
                self.id,
                cs
            );
            return false;
        }
        let id = EntryId {
            term: s.metadata.term,
            index: s.metadata.index,
        };
        if self.raft_log.match_term(id) {
            let last = self.raft_log.last_entry_id();
            infof!(
                self.logger,
                "{:x} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
                self.id,
                self.raft_log.committed,
                last.index,
                last.term,
                id.index,
                id.term
            );
            self.raft_log.commit_to(s.metadata.index);
            return false;
        }
        self.raft_log.restore(s);
        self.trk = ProgressTracker::new(self.trk.max_inflight, self.trk.max_inflight_bytes);
        let (cfg, trk) = match confchange::restore(
            Changer {
                tracker: &self.trk,
                last_index: self.raft_log.last_index(),
            },
            &cs,
        ) {
            Ok(v) => v,
            Err(e) => panic!("unable to restore config {cs:?}: {e}"),
        };
        let cs2 = self.switch_to_config(cfg, trk);
        assert_conf_states_equivalent(&*self.logger, &cs, &cs2);
        let last = self.raft_log.last_entry_id();
        infof!(
            self.logger,
            "{:x} [commit: {}, lastindex: {}, lastterm: {}] restored snapshot [index: {}, term: {}]",
            self.id,
            self.raft_log.committed,
            last.index,
            last.term,
            id.index,
            id.term
        );
        true
    }

    /// True if this node can become leader.
    pub fn promotable(&self) -> bool {
        match self.trk.progress.get(&self.id) {
            Some(pr) => !pr.is_learner && !self.raft_log.has_next_or_in_progress_snapshot(),
            None => false,
        }
    }

    /// Applies a configuration change.
    pub fn apply_conf_change(&mut self, cc: &ConfChangeV2) -> ConfState {
        let res = {
            let changer = Changer {
                tracker: &self.trk,
                last_index: self.raft_log.last_index(),
            };
            if cc.leave_joint() {
                changer.leave_joint()
            } else {
                let (auto_leave, ok) = cc.enter_joint();
                if ok {
                    changer.enter_joint(auto_leave, &cc.changes)
                } else {
                    changer.simple(&cc.changes)
                }
            }
        };
        let (cfg, trk) = match res {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        };
        self.switch_to_config(cfg, trk)
    }

    /// Switches to the given configuration and reacts to the change.
    pub fn switch_to_config(&mut self, cfg: tracker::Config, trk: ProgressMap) -> ConfState {
        self.trk.config = cfg;
        self.trk.progress = trk;
        infof!(
            self.logger,
            "{:x} switched to configuration {}",
            self.id,
            self.trk.config
        );
        let cs = self.trk.conf_state();
        let pr = self.trk.progress.get(&self.id);
        let ok = pr.is_some();
        self.is_learner = pr.is_some_and(|p| p.is_learner);
        if (!ok || self.is_learner) && self.state == StateType::StateLeader {
            if self.step_down_on_removal {
                self.become_follower(self.term, NONE);
            }
            return cs;
        }
        if self.state != StateType::StateLeader || cs.voters.is_empty() {
            return cs;
        }
        if self.maybe_commit() {
            self.bcast_append();
        } else {
            self.visit_ids(|r, id| {
                if id == r.id {
                    return;
                }
                r.maybe_send_append(id, false);
            });
        }
        if !self.trk.config.voters.ids().contains(&self.lead_transferee)
            && self.lead_transferee != 0
        {
            self.abort_leader_transfer();
        }
        cs
    }

    /// Loads a persisted HardState.
    pub fn load_state(&mut self, state: &HardState) {
        if state.commit < self.raft_log.committed || state.commit > self.raft_log.last_index() {
            panicf!(
                self.logger,
                "{:x} state.commit {} is out of range [{}, {}]",
                self.id,
                state.commit,
                self.raft_log.committed,
                self.raft_log.last_index()
            );
        }
        self.raft_log.committed = state.commit;
        self.term = state.term;
        self.vote = state.vote;
    }

    pub(crate) fn past_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    pub(crate) fn reset_randomized_election_timeout(&mut self) {
        self.randomized_election_timeout =
            self.election_timeout + self.rng.intn(self.election_timeout);
    }

    fn send_timeout_now(&mut self, to: u64) {
        self.send(Message {
            to,
            msg_type: MessageType::MsgTimeoutNow,
            ..Default::default()
        });
    }

    fn abort_leader_transfer(&mut self) {
        self.lead_transferee = NONE;
    }

    fn committed_entry_in_current_term(&self) -> bool {
        self.raft_log
            .zero_term_on_out_of_bounds(self.raft_log.term(self.raft_log.committed))
            == self.term
    }

    fn response_to_read_index_req(&mut self, req: Message, read_index: u64) -> Message {
        if req.from == NONE || req.from == self.id {
            let request_ctx = req
                .entries
                .into_iter()
                .next()
                .map(|e| e.data)
                .unwrap_or_default();
            self.read_states.push(ReadState {
                index: read_index,
                request_ctx,
            });
            return Message::default();
        }
        Message {
            msg_type: MessageType::MsgReadIndexResp,
            to: req.from,
            index: read_index,
            entries: req.entries,
            ..Default::default()
        }
    }

    pub(crate) fn increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        let s = payloads_size(ents);
        if self.uncommitted_size > 0
            && s > 0
            && self.uncommitted_size.saturating_add(s) > self.max_uncommitted_size
        {
            return false;
        }
        self.uncommitted_size += s;
        true
    }

    pub(crate) fn reduce_uncommitted_size(&mut self, s: u64) {
        if s > self.uncommitted_size {
            self.uncommitted_size = 0;
        } else {
            self.uncommitted_size -= s;
        }
    }

    fn release_pending_read_index_messages(&mut self) {
        if self.pending_read_index_messages.is_empty() {
            return;
        }
        if !self.committed_entry_in_current_term() {
            errorf!(
                self.logger,
                "pending MsgReadIndex should be released only after first commit in current term"
            );
            return;
        }
        let msgs = std::mem::take(&mut self.pending_read_index_messages);
        for m in msgs {
            self.send_msg_read_index_response(m);
        }
    }

    fn send_msg_read_index_response(&mut self, m: Message) {
        match self.read_only.option {
            ReadOnlyOption::ReadOnlySafe => {
                self.read_only.add_request(self.raft_log.committed, m);
                let ctx = self.read_only.heartbeat_ctx();
                self.read_only.recv_ack(self.id, &ctx);
                self.bcast_heartbeat();
            }
            ReadOnlyOption::ReadOnlyLeaseBased => {
                let committed = self.raft_log.committed;
                let resp = self.response_to_read_index_req(m, committed);
                if resp.to != NONE {
                    self.send(resp);
                }
            }
        }
    }

    /// Proposes a configuration change (used by RawNode).
    pub(crate) fn propose_conf_change(&mut self, cc: &ConfChangeI) -> Result<(), Error> {
        let m = conf_change_to_msg(Some(cc));
        self.step(m)
    }
}

pub(crate) fn step_leader<S: Storage>(r: &mut Raft<S>, mut m: Message) -> Result<(), Error> {
    match m.msg_type {
        MessageType::MsgBeat => {
            r.bcast_heartbeat();
            return Ok(());
        }
        MessageType::MsgCheckQuorum => {
            if !r.trk.quorum_active() {
                warningf!(
                    r.logger,
                    "{:x} stepped down to follower since quorum is not active",
                    r.id
                );
                r.become_follower(r.term, NONE);
            }
            let id = r.id;
            r.trk.visit(|pid, pr| {
                if pid != id {
                    pr.recent_active = false;
                }
            });
            return Ok(());
        }
        MessageType::MsgProp => {
            if m.entries.is_empty() {
                panicf!(r.logger, "{:x} stepped empty MsgProp", r.id);
            }
            if !r.trk.progress.contains_key(&r.id) {
                return Err(Error::ProposalDropped);
            }
            if r.lead_transferee != NONE {
                debugf!(
                    r.logger,
                    "{:x} [term {}] transfer leadership to {:x} is in progress; dropping proposal",
                    r.id,
                    r.term,
                    r.lead_transferee
                );
                return Err(Error::ProposalDropped);
            }
            let last_index = r.raft_log.last_index();
            for i in 0..m.entries.len() {
                let e = &m.entries[i];
                let cc = match e.entry_type {
                    EntryType::EntryConfChange => Some(ConfChangeI::V1(
                        ConfChange::decode(&e.data).unwrap_or_else(|e| panic!("{e}")),
                    )),
                    EntryType::EntryConfChangeV2 => Some(ConfChangeI::V2(
                        ConfChangeV2::decode(&e.data).unwrap_or_else(|e| panic!("{e}")),
                    )),
                    EntryType::EntryNormal => None,
                };
                if let Some(cc) = cc {
                    let already_pending = r.pending_conf_index > r.raft_log.applied;
                    let already_joint = !r.trk.config.voters.0[1].is_empty();
                    let wants_leave_joint = cc.as_v2().changes.is_empty();
                    let failed_check = if already_pending {
                        format!(
                            "possible unapplied conf change at index {} (applied to {})",
                            r.pending_conf_index, r.raft_log.applied
                        )
                    } else if already_joint && !wants_leave_joint {
                        "must transition out of joint config first".to_string()
                    } else if !already_joint && wants_leave_joint {
                        "not in joint state; refusing empty conf change".to_string()
                    } else {
                        String::new()
                    };
                    if !failed_check.is_empty() && !r.disable_conf_change_validation {
                        infof!(
                            r.logger,
                            "{:x} ignoring conf change {} at config {}: {}",
                            r.id,
                            describe_conf_change(&cc),
                            r.trk.config,
                            failed_check
                        );
                        m.entries[i] = Entry {
                            entry_type: EntryType::EntryNormal,
                            ..Default::default()
                        };
                    } else {
                        r.pending_conf_index = last_index + i as u64 + 1;
                    }
                }
            }
            if !r.append_entry(m.entries) {
                return Err(Error::ProposalDropped);
            }
            r.bcast_append();
            return Ok(());
        }
        MessageType::MsgReadIndex => {
            if r.trk.is_singleton() {
                let committed = r.raft_log.committed;
                let resp = r.response_to_read_index_req(m, committed);
                if resp.to != NONE {
                    r.send(resp);
                }
                return Ok(());
            }
            if !r.committed_entry_in_current_term() {
                r.pending_read_index_messages.push(m);
                return Ok(());
            }
            r.send_msg_read_index_response(m);
            return Ok(());
        }
        MessageType::MsgForgetLeader => return Ok(()),
        _ => {}
    }

    let from = m.from;
    if !r.trk.progress.contains_key(&from) {
        debugf!(r.logger, "{:x} no progress available for {:x}", r.id, from);
        return Ok(());
    }
    match m.msg_type {
        MessageType::MsgAppResp => {
            r.pr_mut(from).recent_active = true;
            if m.reject {
                debugf!(
                    r.logger,
                    "{:x} received MsgAppResp(rejected, hint: (index {}, term {})) from {:x} for index {}",
                    r.id,
                    m.reject_hint,
                    m.log_term,
                    from,
                    m.index
                );
                let mut next_probe_idx = m.reject_hint;
                if m.log_term > 0 {
                    next_probe_idx = r
                        .raft_log
                        .find_conflict_by_term(m.reject_hint, m.log_term)
                        .0;
                }
                if r.pr_mut(from).maybe_decr_to(m.index, next_probe_idx) {
                    debugf!(
                        r.logger,
                        "{:x} decreased progress of {:x} to [{}]",
                        r.id,
                        from,
                        r.trk.progress[&from]
                    );
                    let pr = r.pr_mut(from);
                    if pr.state == tracker::StateType::StateReplicate {
                        pr.become_probe();
                    }
                    r.send_append(from);
                }
            } else {
                let pr = r.pr_mut(from);
                if pr.maybe_update(m.index)
                    || (pr.match_ == m.index && pr.state == tracker::StateType::StateProbe)
                {
                    match pr.state {
                        tracker::StateType::StateProbe => pr.become_replicate(),
                        tracker::StateType::StateSnapshot => {
                            let match_ = pr.match_;
                            if match_ + 1 >= r.raft_log.first_index() {
                                debugf!(
                                    r.logger,
                                    "{:x} recovered from needing snapshot, resumed sending replication messages to {:x} [{}]",
                                    r.id,
                                    from,
                                    r.trk.progress[&from]
                                );
                                let pr = r.pr_mut(from);
                                pr.become_probe();
                                pr.become_replicate();
                            }
                        }
                        tracker::StateType::StateReplicate => pr.inflights.free_le(m.index),
                    }
                    if r.maybe_commit() {
                        r.release_pending_read_index_messages();
                        r.bcast_append();
                    } else if r.id != from
                        && r.trk.progress[&from].can_bump_commit(r.raft_log.committed)
                    {
                        r.send_append(from);
                    }
                    if r.id != from {
                        while r.maybe_send_append(from, false) {}
                    }
                    if from == r.lead_transferee
                        && r.trk.progress[&from].match_ == r.raft_log.last_index()
                    {
                        infof!(
                            r.logger,
                            "{:x} sent MsgTimeoutNow to {:x} after received MsgAppResp",
                            r.id,
                            from
                        );
                        r.send_timeout_now(from);
                    }
                }
            }
        }
        MessageType::MsgHeartbeatResp => {
            let pr = r.pr_mut(from);
            pr.recent_active = true;
            pr.msg_app_flow_paused = false;
            let (match_, state) = (pr.match_, pr.state);
            if match_ < r.raft_log.last_index() || state == tracker::StateType::StateProbe {
                r.send_append(from);
            }
            if r.read_only.option != ReadOnlyOption::ReadOnlySafe || m.context.is_empty() {
                return Ok(());
            }
            r.read_only.recv_ack(from, &m.context);
            let rss = r.read_only.maybe_advance(&r.trk.config.voters);
            for rs in rss {
                let resp = r.response_to_read_index_req(rs.req, rs.index);
                if resp.to != NONE {
                    r.send(resp);
                }
            }
        }
        MessageType::MsgSnapStatus => {
            if r.trk.progress[&from].state != tracker::StateType::StateSnapshot {
                return Ok(());
            }
            if !m.reject {
                r.pr_mut(from).become_probe();
                debugf!(
                    r.logger,
                    "{:x} snapshot succeeded, resumed sending replication messages to {:x} [{}]",
                    r.id,
                    from,
                    r.trk.progress[&from]
                );
            } else {
                let pr = r.pr_mut(from);
                pr.pending_snapshot = 0;
                pr.become_probe();
                debugf!(
                    r.logger,
                    "{:x} snapshot failed, resumed sending replication messages to {:x} [{}]",
                    r.id,
                    from,
                    r.trk.progress[&from]
                );
            }
            r.pr_mut(from).msg_app_flow_paused = true;
        }
        MessageType::MsgUnreachable => {
            let pr = r.pr_mut(from);
            if pr.state == tracker::StateType::StateReplicate {
                pr.become_probe();
            }
            debugf!(
                r.logger,
                "{:x} failed to send message to {:x} because it is unreachable [{}]",
                r.id,
                from,
                r.trk.progress[&from]
            );
        }
        MessageType::MsgTransferLeader => {
            if r.trk.progress[&from].is_learner {
                debugf!(
                    r.logger,
                    "{:x} is learner. Ignored transferring leadership",
                    r.id
                );
                return Ok(());
            }
            let lead_transferee = from;
            let last_lead_transferee = r.lead_transferee;
            if last_lead_transferee != NONE {
                if last_lead_transferee == lead_transferee {
                    infof!(
                        r.logger,
                        "{:x} [term {}] transfer leadership to {:x} is in progress, ignores request to same node {:x}",
                        r.id,
                        r.term,
                        lead_transferee,
                        lead_transferee
                    );
                    return Ok(());
                }
                r.abort_leader_transfer();
                infof!(
                    r.logger,
                    "{:x} [term {}] abort previous transferring leadership to {:x}",
                    r.id,
                    r.term,
                    last_lead_transferee
                );
            }
            if lead_transferee == r.id {
                debugf!(
                    r.logger,
                    "{:x} is already leader. Ignored transferring leadership to self",
                    r.id
                );
                return Ok(());
            }
            infof!(
                r.logger,
                "{:x} [term {}] starts to transfer leadership to {:x}",
                r.id,
                r.term,
                lead_transferee
            );
            r.election_elapsed = 0;
            r.lead_transferee = lead_transferee;
            if r.trk.progress[&from].match_ == r.raft_log.last_index() {
                r.send_timeout_now(lead_transferee);
                infof!(
                    r.logger,
                    "{:x} sends MsgTimeoutNow to {:x} immediately as {:x} already has up-to-date log",
                    r.id,
                    lead_transferee,
                    lead_transferee
                );
            } else {
                r.send_append(lead_transferee);
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn step_candidate<S: Storage>(r: &mut Raft<S>, m: Message) -> Result<(), Error> {
    let my_vote_resp_type = if r.state == StateType::StatePreCandidate {
        MessageType::MsgPreVoteResp
    } else {
        MessageType::MsgVoteResp
    };
    match m.msg_type {
        MessageType::MsgProp => {
            infof!(
                r.logger,
                "{:x} no leader at term {}; dropping proposal",
                r.id,
                r.term
            );
            return Err(Error::ProposalDropped);
        }
        MessageType::MsgApp => {
            r.become_follower(m.term, m.from);
            r.handle_append_entries(m);
        }
        MessageType::MsgHeartbeat => {
            r.become_follower(m.term, m.from);
            r.handle_heartbeat(m);
        }
        MessageType::MsgSnap => {
            r.become_follower(m.term, m.from);
            r.handle_snapshot(m);
        }
        t if t == my_vote_resp_type => {
            let (gr, rj, res) = r.poll(m.from, m.msg_type, !m.reject);
            infof!(
                r.logger,
                "{:x} has received {} {} votes and {} vote rejections",
                r.id,
                gr,
                m.msg_type,
                rj
            );
            match res {
                VoteResult::VoteWon => {
                    if r.state == StateType::StatePreCandidate {
                        r.campaign(CampaignType::Election);
                    } else {
                        r.become_leader();
                        r.bcast_append();
                    }
                }
                VoteResult::VoteLost => r.become_follower(r.term, NONE),
                VoteResult::VotePending => {}
            }
        }
        MessageType::MsgTimeoutNow => {
            debugf!(
                r.logger,
                "{:x} [term {} state {}] ignored MsgTimeoutNow from {:x}",
                r.id,
                r.term,
                r.state,
                m.from
            );
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn step_follower<S: Storage>(r: &mut Raft<S>, mut m: Message) -> Result<(), Error> {
    match m.msg_type {
        MessageType::MsgProp => {
            if r.lead == NONE {
                infof!(
                    r.logger,
                    "{:x} no leader at term {}; dropping proposal",
                    r.id,
                    r.term
                );
                return Err(Error::ProposalDropped);
            } else if r.disable_proposal_forwarding {
                infof!(
                    r.logger,
                    "{:x} not forwarding to leader {:x} at term {}; dropping proposal",
                    r.id,
                    r.lead,
                    r.term
                );
                return Err(Error::ProposalDropped);
            }
            m.to = r.lead;
            r.send(m);
        }
        MessageType::MsgApp => {
            r.election_elapsed = 0;
            r.lead = m.from;
            r.handle_append_entries(m);
        }
        MessageType::MsgHeartbeat => {
            r.election_elapsed = 0;
            r.lead = m.from;
            r.handle_heartbeat(m);
        }
        MessageType::MsgSnap => {
            r.election_elapsed = 0;
            r.lead = m.from;
            r.handle_snapshot(m);
        }
        MessageType::MsgTransferLeader => {
            if r.lead == NONE {
                infof!(
                    r.logger,
                    "{:x} no leader at term {}; dropping leader transfer msg",
                    r.id,
                    r.term
                );
                return Ok(());
            }
            m.to = r.lead;
            r.send(m);
        }
        MessageType::MsgForgetLeader => {
            if r.read_only.option == ReadOnlyOption::ReadOnlyLeaseBased {
                errorf!(
                    r.logger,
                    "ignoring MsgForgetLeader due to ReadOnlyLeaseBased"
                );
                return Ok(());
            }
            if r.lead != NONE {
                infof!(
                    r.logger,
                    "{:x} forgetting leader {:x} at term {}",
                    r.id,
                    r.lead,
                    r.term
                );
                r.lead = NONE;
            }
        }
        MessageType::MsgTimeoutNow => {
            infof!(
                r.logger,
                "{:x} [term {}] received MsgTimeoutNow from {:x} and starts an election to get leadership.",
                r.id,
                r.term,
                m.from
            );
            r.hup(CampaignType::Transfer);
        }
        MessageType::MsgReadIndex => {
            if r.lead == NONE {
                infof!(
                    r.logger,
                    "{:x} no leader at term {}; dropping index reading msg",
                    r.id,
                    r.term
                );
                return Ok(());
            }
            m.to = r.lead;
            r.send(m);
        }
        MessageType::MsgReadIndexResp => {
            if m.entries.len() != 1 {
                errorf!(
                    r.logger,
                    "{:x} invalid format of MsgReadIndexResp from {:x}, entries count: {}",
                    r.id,
                    m.from,
                    m.entries.len()
                );
                return Ok(());
            }
            let request_ctx = m
                .entries
                .into_iter()
                .next()
                .map(|e| e.data)
                .unwrap_or_default();
            r.read_states.push(ReadState {
                index: m.index,
                request_ctx,
            });
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
impl<S: Storage> Raft<S> {
    pub(crate) fn set_step(&mut self, f: StepFn<S>) {
        self.step = f;
    }
    pub(crate) fn election_timeout(&self) -> usize {
        self.election_timeout
    }
}
