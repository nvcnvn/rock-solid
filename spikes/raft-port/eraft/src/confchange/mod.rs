// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// confchange/confchange.go. Licensed under the Apache License 2.0.
//
// Deviation: Changer borrows the tracker instead of holding a shallow copy,
// and errors are strings.

//! Validated configuration changes.

mod restore;

use std::collections::BTreeSet;
use std::fmt::Write as _;

pub use restore::restore;

use crate::raftpb::{ConfChangeSingle, ConfChangeType};
use crate::tracker::{Config, Inflights, Progress, ProgressMap, ProgressTracker};

/// Result of a configuration change: the new config and progress map.
pub type ChangeResult = Result<(Config, ProgressMap), String>;

/// Applies simple and joint configuration changes with validation.
pub struct Changer<'a> {
    /// Tracker with the current configuration.
    pub tracker: &'a ProgressTracker,
    /// Last log index, used to initialize new progresses.
    pub last_index: u64,
}

impl Changer<'_> {
    /// Enters a joint configuration: copies incoming to outgoing, then applies `ccs`.
    pub fn enter_joint(&self, auto_leave: bool, ccs: &[ConfChangeSingle]) -> ChangeResult {
        let (mut cfg, mut trk) = self.check_and_copy()?;
        if joint(&cfg) {
            return Err("config is already joint".into());
        }
        if cfg.voters.0[0].is_empty() {
            return Err("can't make a zero-voter config joint".into());
        }
        cfg.voters.0[1] = cfg.voters.0[0].clone();
        self.apply(&mut cfg, &mut trk, ccs)?;
        cfg.auto_leave = auto_leave;
        check_and_return(cfg, trk)
    }

    /// Leaves a joint configuration.
    pub fn leave_joint(&self) -> ChangeResult {
        let (mut cfg, mut trk) = self.check_and_copy()?;
        if !joint(&cfg) {
            return Err("can't leave a non-joint config".into());
        }
        for id in std::mem::take(&mut cfg.learners_next) {
            cfg.learners.insert(id);
            if let Some(pr) = trk.get_mut(&id) {
                pr.is_learner = true;
            }
        }
        for id in cfg.voters.0[1].iter() {
            let is_voter = cfg.voters.0[0].contains(id);
            let is_learner = cfg.learners.contains(id);
            if !is_voter && !is_learner {
                trk.remove(id);
            }
        }
        cfg.voters.0[1].clear();
        cfg.auto_leave = false;
        check_and_return(cfg, trk)
    }

    /// Applies changes that alter the incoming voters by at most one.
    pub fn simple(&self, ccs: &[ConfChangeSingle]) -> ChangeResult {
        let (mut cfg, mut trk) = self.check_and_copy()?;
        if joint(&cfg) {
            return Err("can't apply simple config change in joint config".into());
        }
        self.apply(&mut cfg, &mut trk, ccs)?;
        if symdiff(&self.tracker.config.voters.0[0], &cfg.voters.0[0]) > 1 {
            return Err("more than one voter changed without entering joint config".into());
        }
        check_and_return(cfg, trk)
    }

    fn apply(
        &self,
        cfg: &mut Config,
        trk: &mut ProgressMap,
        ccs: &[ConfChangeSingle],
    ) -> Result<(), String> {
        for cc in ccs {
            if cc.node_id == 0 {
                continue;
            }
            match cc.change_type {
                ConfChangeType::ConfChangeAddNode => self.make_voter(cfg, trk, cc.node_id),
                ConfChangeType::ConfChangeAddLearnerNode => self.make_learner(cfg, trk, cc.node_id),
                ConfChangeType::ConfChangeRemoveNode => self.remove(cfg, trk, cc.node_id),
                ConfChangeType::ConfChangeUpdateNode => {}
            }
        }
        if cfg.voters.0[0].is_empty() {
            return Err("removed all voters".into());
        }
        Ok(())
    }

    fn make_voter(&self, cfg: &mut Config, trk: &mut ProgressMap, id: u64) {
        let Some(pr) = trk.get_mut(&id) else {
            self.init_progress(cfg, trk, id, false);
            return;
        };
        pr.is_learner = false;
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);
        cfg.voters.0[0].insert(id);
    }

    fn make_learner(&self, cfg: &mut Config, trk: &mut ProgressMap, id: u64) {
        let Some(pr) = trk.get(&id) else {
            self.init_progress(cfg, trk, id, true);
            return;
        };
        if pr.is_learner {
            return;
        }
        let pr = pr.clone();
        self.remove(cfg, trk, id);
        let on_right = cfg.voters.0[1].contains(&id);
        let pr = trk.entry(id).or_insert(pr);
        if on_right {
            cfg.learners_next.insert(id);
        } else {
            pr.is_learner = true;
            cfg.learners.insert(id);
        }
    }

    fn remove(&self, cfg: &mut Config, trk: &mut ProgressMap, id: u64) {
        if !trk.contains_key(&id) {
            return;
        }
        cfg.voters.0[0].remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);
        if !cfg.voters.0[1].contains(&id) {
            trk.remove(&id);
        }
    }

    fn init_progress(&self, cfg: &mut Config, trk: &mut ProgressMap, id: u64, is_learner: bool) {
        if !is_learner {
            cfg.voters.0[0].insert(id);
        } else {
            cfg.learners.insert(id);
        }
        trk.insert(
            id,
            Progress::new(
                0,
                self.last_index.max(1),
                Inflights::new(self.tracker.max_inflight, self.tracker.max_inflight_bytes),
                is_learner,
                true,
            ),
        );
    }

    fn check_and_copy(&self) -> ChangeResult {
        let cfg = self.tracker.config.clone_go();
        let trk = self.tracker.progress.clone();
        check_and_return(cfg, trk)
    }
}

fn check_invariants(cfg: &Config, trk: &ProgressMap) -> Result<(), String> {
    for ids in [&cfg.voters.ids(), &cfg.learners, &cfg.learners_next] {
        for id in ids {
            if !trk.contains_key(id) {
                return Err(format!("no progress for {id}"));
            }
        }
    }
    for id in &cfg.learners_next {
        if !cfg.voters.0[1].contains(id) {
            return Err(format!("{id} is in LearnersNext, but not Voters[1]"));
        }
        if trk[id].is_learner {
            return Err(format!(
                "{id} is in LearnersNext, but is already marked as learner"
            ));
        }
    }
    for id in &cfg.learners {
        if cfg.voters.0[1].contains(id) {
            return Err(format!("{id} is in Learners and Voters[1]"));
        }
        if cfg.voters.0[0].contains(id) {
            return Err(format!("{id} is in Learners and Voters[0]"));
        }
        if !trk[id].is_learner {
            return Err(format!("{id} is in Learners, but is not marked as learner"));
        }
    }
    if !joint(cfg) {
        if !cfg.learners_next.is_empty() {
            return Err("cfg.LearnersNext must be nil when not joint".into());
        }
        if cfg.auto_leave {
            return Err("AutoLeave must be false when not joint".into());
        }
    }
    Ok(())
}

fn check_and_return(cfg: Config, trk: ProgressMap) -> ChangeResult {
    check_invariants(&cfg, &trk)?;
    Ok((cfg, trk))
}

fn symdiff(l: &BTreeSet<u64>, r: &BTreeSet<u64>) -> usize {
    l.symmetric_difference(r).count()
}

fn joint(cfg: &Config) -> bool {
    !cfg.voters.0[1].is_empty()
}

/// Prints the type and node ID of each change, space-delimited.
pub fn describe(ccs: &[ConfChangeSingle]) -> String {
    let mut buf = String::new();
    for cc in ccs {
        if !buf.is_empty() {
            buf.push(' ');
        }
        let _ = write!(buf, "{}({})", cc.change_type, cc.node_id);
    }
    buf
}
