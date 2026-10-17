// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// quorum/majority.go. Licensed under the Apache License 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;

use super::{AckedIndexer, Index, VoteResult};

/// A set of IDs that uses majority quorums to make decisions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MajorityConfig(pub BTreeSet<u64>);

impl std::ops::Deref for MajorityConfig {
    type Target = BTreeSet<u64>;
    fn deref(&self) -> &BTreeSet<u64> {
        &self.0
    }
}

impl std::ops::DerefMut for MajorityConfig {
    fn deref_mut(&mut self) -> &mut BTreeSet<u64> {
        &mut self.0
    }
}

impl fmt::Display for MajorityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('(')?;
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_char(' ')?;
            }
            write!(f, "{id:x}")?;
        }
        f.write_char(')')
    }
}

impl MajorityConfig {
    /// Returns a multi-line representation of the commit indexes for the given lookuper.
    pub fn describe(&self, l: &dyn AckedIndexer) -> String {
        if self.0.is_empty() {
            return "<empty majority quorum>".to_string();
        }
        struct Tup {
            id: u64,
            idx: Index,
            ok: bool,
            bar: usize,
        }
        let n = self.0.len();
        let mut info: Vec<Tup> = self
            .0
            .iter()
            .map(|&id| {
                let a = l.acked_index(id);
                Tup {
                    id,
                    idx: a.unwrap_or(0),
                    ok: a.is_some(),
                    bar: 0,
                }
            })
            .collect();
        info.sort_by(|a, b| a.idx.cmp(&b.idx).then(a.id.cmp(&b.id)));
        for i in 1..info.len() {
            if info[i - 1].idx < info[i].idx {
                info[i].bar = i;
            } else {
                info[i].bar = info[i - 1].bar;
            }
        }
        info.sort_by_key(|t| t.id);
        let mut buf = String::new();
        buf.push_str(&" ".repeat(n));
        buf.push_str("    idx\n");
        for t in &info {
            if !t.ok {
                buf.push('?');
                buf.push_str(&" ".repeat(n));
            } else {
                buf.push_str(&"x".repeat(t.bar));
                buf.push('>');
                buf.push_str(&" ".repeat(n - t.bar));
            }
            let _ = writeln!(buf, " {:>5}    (id={:x})", t.idx, t.id);
        }
        buf
    }

    /// Returns the config as a sorted slice.
    pub fn slice(&self) -> Vec<u64> {
        self.0.iter().copied().collect()
    }

    /// Computes the committed index from the acked indexes of the voters.
    pub fn committed_index(&self, l: &dyn AckedIndexer) -> Index {
        let n = self.0.len();
        if n == 0 {
            return u64::MAX;
        }
        let mut stk = [0u64; 7];
        let mut heap;
        let srt: &mut [u64] = if n <= stk.len() {
            &mut stk[..n]
        } else {
            heap = vec![0u64; n];
            &mut heap[..]
        };
        let mut i = n;
        for &id in self.0.iter() {
            if let Some(idx) = l.acked_index(id) {
                i -= 1;
                srt[i] = idx;
            }
        }
        srt.sort_unstable();
        let pos = n - (n / 2 + 1);
        srt[pos]
    }

    /// Returns the vote result for the given votes.
    pub fn vote_result(&self, votes: &BTreeMap<u64, bool>) -> VoteResult {
        if self.0.is_empty() {
            return VoteResult::VoteWon;
        }
        let mut voted_cnt = 0;
        let mut missing = 0;
        for id in self.0.iter() {
            match votes.get(id) {
                None => missing += 1,
                Some(true) => voted_cnt += 1,
                Some(false) => {}
            }
        }
        let q = self.0.len() / 2 + 1;
        if voted_cnt >= q {
            return VoteResult::VoteWon;
        }
        if voted_cnt + missing >= q {
            return VoteResult::VotePending;
        }
        VoteResult::VoteLost
    }
}
