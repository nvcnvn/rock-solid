// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// confchange/restore.go. Licensed under the Apache License 2.0.

use super::{ChangeResult, Changer};
use crate::raftpb::{ConfChangeSingle, ConfChangeType, ConfState};

fn single(change_type: ConfChangeType, node_id: u64) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type,
        node_id,
    }
}

/// Translates a ConfState into the changes that create the outgoing config,
/// and the changes that then produce the ConfState from it.
fn to_conf_change_single(cs: &ConfState) -> (Vec<ConfChangeSingle>, Vec<ConfChangeSingle>) {
    let mut out = Vec::new();
    let mut incoming = Vec::new();
    for &id in &cs.voters_outgoing {
        out.push(single(ConfChangeType::ConfChangeAddNode, id));
    }
    for &id in &cs.voters_outgoing {
        incoming.push(single(ConfChangeType::ConfChangeRemoveNode, id));
    }
    for &id in &cs.voters {
        incoming.push(single(ConfChangeType::ConfChangeAddNode, id));
    }
    for &id in &cs.learners {
        incoming.push(single(ConfChangeType::ConfChangeAddLearnerNode, id));
    }
    for &id in &cs.learners_next {
        incoming.push(single(ConfChangeType::ConfChangeAddLearnerNode, id));
    }
    (out, incoming)
}

/// Runs the changes that enact `cs`, starting from the (empty) config of `chg`.
pub fn restore(chg: Changer<'_>, cs: &ConfState) -> ChangeResult {
    let (outgoing, incoming) = to_conf_change_single(cs);
    let mut trk = chg.tracker.clone();
    let last_index = chg.last_index;
    let mut step = |op: &dyn Fn(&Changer<'_>) -> ChangeResult| -> Result<(), String> {
        let (cfg, prs) = op(&Changer {
            tracker: &trk,
            last_index,
        })?;
        trk.config = cfg;
        trk.progress = prs;
        Ok(())
    };
    if outgoing.is_empty() {
        for cc in &incoming {
            step(&|c| c.simple(std::slice::from_ref(cc)))?;
        }
    } else {
        for cc in &outgoing {
            step(&|c| c.simple(std::slice::from_ref(cc)))?;
        }
        step(&|c| c.enter_joint(cs.auto_leave, &incoming))?;
    }
    Ok((trk.config, trk.progress))
}
