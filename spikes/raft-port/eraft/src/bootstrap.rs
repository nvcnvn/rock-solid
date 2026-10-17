// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// bootstrap.go. Licensed under the Apache License 2.0.

use crate::Error;
use crate::node::Peer;
use crate::raft::NONE;
use crate::raftpb::{ConfChange, ConfChangeType, Entry, EntryType, HardState};
use crate::rawnode::RawNode;
use crate::storage::Storage;

impl<S: Storage> RawNode<S> {
    /// Bootstraps an empty storage with the given peers.
    pub fn bootstrap(&mut self, peers: &[Peer]) -> Result<(), Error> {
        if peers.is_empty() {
            return Err(Error::Other(
                "must provide at least one peer to Bootstrap".into(),
            ));
        }
        let last_index = self.raft.raft_log.storage.last_index()?;
        if last_index != 0 {
            return Err(Error::Other("can't bootstrap a nonempty Storage".into()));
        }
        self.prev_hard_st = HardState::default();
        self.raft.become_follower(1, NONE);
        let ents: Vec<Entry> = peers
            .iter()
            .enumerate()
            .map(|(i, peer)| {
                let cc = ConfChange {
                    change_type: ConfChangeType::ConfChangeAddNode,
                    node_id: peer.id,
                    context: peer.context.clone(),
                    id: 0,
                };
                Entry {
                    entry_type: EntryType::EntryConfChange,
                    term: 1,
                    index: i as u64 + 1,
                    data: cc.encode(),
                }
            })
            .collect();
        let n = ents.len() as u64;
        self.raft.raft_log.append(ents);
        self.raft.raft_log.committed = n;
        for peer in peers {
            let cc = ConfChange {
                node_id: peer.id,
                change_type: ConfChangeType::ConfChangeAddNode,
                ..Default::default()
            };
            self.raft.apply_conf_change(&cc.as_v2());
        }
        Ok(())
    }
}
