// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// read_only.go. Licensed under the Apache License 2.0.

use std::collections::{BTreeMap, VecDeque};

use crate::quorum::{AckedIndexer, Index, JointConfig};
use crate::raft::ReadOnlyOption;
use crate::raftpb::Message;

/// A pending read-only request.
#[derive(Clone, Debug)]
pub struct ReadIndexRequest {
    /// The original MsgReadIndex.
    pub req: Message,
    /// Commit index when the request was received.
    pub index: u64,
}

/// Tracks read-only requests awaiting heartbeat acknowledgement.
#[derive(Clone, Debug, Default)]
pub struct ReadOnly {
    /// How reads are served.
    pub option: ReadOnlyOption,
    acks: BTreeMap<u64, u64>,
    pub(crate) unconfirmed_reads: VecDeque<ReadIndexRequest>,
    confirmed_reads: u64,
}

impl ReadOnly {
    /// Creates an empty tracker.
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly {
            option,
            ..Default::default()
        }
    }

    /// Adds a request received at `commit_index`.
    pub fn add_request(&mut self, commit_index: u64, req: Message) {
        self.unconfirmed_reads.push_back(ReadIndexRequest {
            req,
            index: commit_index,
        });
    }

    /// Records a heartbeat acknowledgement with context `ctx`.
    pub fn recv_ack(&mut self, from: u64, ctx: &[u8]) {
        if !ctx.is_empty() {
            let mut b = [0u8; 8];
            let n = ctx.len().min(8);
            b[..n].copy_from_slice(&ctx[..n]);
            let v = u64::from_le_bytes(b);
            let e = self.acks.entry(from).or_insert(0);
            *e = (*e).max(v);
        }
    }

    /// Confirms and returns as many reads as the acks allow.
    pub fn maybe_advance(&mut self, c: &JointConfig) -> Vec<ReadIndexRequest> {
        let new_confirmed_reads = c.committed_index(self);
        if new_confirmed_reads <= self.confirmed_reads {
            return Vec::new();
        }
        let n = (new_confirmed_reads - self.confirmed_reads) as usize;
        let read_states = self.unconfirmed_reads.drain(..n).collect();
        self.confirmed_reads = new_confirmed_reads;
        read_states
    }

    /// Context to send to confirm all currently unconfirmed reads.
    pub fn heartbeat_ctx(&self) -> Vec<u8> {
        if self.unconfirmed_reads.is_empty() {
            return Vec::new();
        }
        let pos = self.confirmed_reads + self.unconfirmed_reads.len() as u64;
        pos.to_le_bytes().to_vec()
    }
}

impl AckedIndexer for ReadOnly {
    fn acked_index(&self, voter_id: u64) -> Option<Index> {
        self.acks.get(&voter_id).copied()
    }
}
