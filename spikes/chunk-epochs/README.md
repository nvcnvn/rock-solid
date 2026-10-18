# Chunk epochs spike

**Question.** Two gaps in the [chunk index](../../docs/design.md#on-disk-format). (1) A write record carries no snapshot epoch, and a chunk's entry holds the epoch of its last freeze, so does a chunk first written after a snapshot cut read correctly at that snapshot, and if not, must every write entry carry its epoch (8 more header bytes), or can the index learn a chunk's first epoch some other way? (2) A replicated primary maps a write only once it commits, so its checkpoint's replay point sits before its oldest pending write, and a restart applies again records its runs already hold. Is that safe as the index stands, or must the index skip a write whose blocks hold a newer version, or a checkpoint wait for pending writes to commit?

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker, 4 vCPUs, shared with other agents' benchmarks; nothing here is timed). Go 1.27.0. The tests drive the real `internal/chunkstore` index at rock-solid commit `676095bf` (written against it; `results.txt` ran at `dbae461`, whose `internal/` is the same) through `go.mod`'s `replace`, plus [absent/index.go](absent/index.go), a copy of that index with one change ([absent/index.diff](absent/index.diff), 77 lines): a `freeze` of version 0, the *absent* chunk, which today is refused, is accepted for a chunk with no entry yet. It stores a frozen version 0 with an empty map and the freeze's epoch, so `LookupAt` at an epoch it covers finds nothing, and the chunk entry's epoch makes later writes in that epoch freeze nothing. The record codec already encodes a freeze of any version, so the change is to the index alone.

- [epochs_test.go](epochs_test.go): a primary of one volume writes with the volume's snapshot epoch and puts a freeze of the chunk's current version in the record of the first write after a cut, as [Versions share blocks](../../docs/design.md#on-disk-format) says. A model copies every chunk at each cut. E1 and E2 are the two shapes of the gap; E3 is random cuts, writes and punches over 4 chunks of 16 blocks, 400 steps, 50 seeds, checking every block of every snapshot after the run, and that a rebuild from the records, and the records applied twice, give the same index.
- [replay_test.go](replay_test.go): a replicated primary logs records of 1 to 3 writes or punches (with their freezes), commits 0 to 3 pending writes per round and maps each at commit, moves a live or frozen block as the cleaner does every third round, and checkpoints every 20 rounds with the replay point at its oldest pending write. After 300 rounds it restarts from the last checkpoint: the checkpoint's index plus every record from the replay point, in LSN order. That must read every block, live and at every snapshot epoch, by version and by the block a location is a copy of, as a rebuild that applies each record once does. R1 commits a chunk's writes in version order, R2 in any order; 200 seeds each. R3 is a write peering drops after the checkpoint, with and without the recovery copy that supersedes it in the same log.

```
./run.sh     # go test -v ./... -> results.txt, about 30 s
```

Raw output: [results.txt](results.txt).

## Results

| Test | Result |
| --- | --- |
| E1: chunk first written after cut 1, written again before cut 2 (index at HEAD) | **Snapshot 1 reads version 1**, written after its cut: the second write saw the entry's epoch 0 and froze it |
| E2: chunk first written after cut 1, then cut 2 and a write (index at HEAD) | **Snapshot 1 reads version 1** too: it reads the smallest freeze epoch at or above its own, 2, and nothing records that the chunk did not exist at cut 1 |
| E1, E2 with the absent freeze | Snapshot 1 finds nothing; snapshot 2 reads version 1 |
| E3 [property], index at HEAD | **35 of 50 seeds** read a snapshot wrong, 259 block reads, every one a chunk created after the cut it was read at |
| E3 [property], absent freeze | 0 of 50 seeds; 74 absent freezes against 5,267 freezes of a version; rebuild and double application equal the index |
| R1 [property]: restart past pending writes, a chunk's commits in version order | **0 of 200 seeds** differ from a rebuild. Replay applied 23,493 records, 3,219 of them (14%) already in the runs, across 7,917 cleaner moves and 3,064 checkpoints |
| R2 [property]: commits in any order | 200 of 200 differ, and so does the index that never crashed once its pending writes commit: a write mapped before an older write of its chunk is overwritten by it, and a freeze copies a map that already holds later writes |
| R3: a write peering dropped, a later write to other blocks of the chunk, restart from a checkpoint taken while the dropped write was pending | **Replay maps the dropped write** (block 0 at v2, LBA 201; the live index holds v1) |
| R3b: the primary also logs the recovery copy of the authoritative blocks under a new version | Replay ends at the recovered blocks (v3, a copy of v1's) |

## Conclusions

1. **A chunk first written after a snapshot cut needs a record that it did not exist.** Without one, its next write freezes a version written after the cut, and every snapshot at or before the cut reads it (E1, E2, E3). The primary puts a `freeze` of version 0 in the record of a write to a chunk that has no version yet when the volume's snapshot epoch is above 0. It costs one 37-byte entry per chunk created in a volume that has had a snapshot, and no format change; carrying the epoch in every write and punch entry would cost 8 bytes on each (33 to 41) and still need the index to record a chunk's first epoch. Ceph keeps the same fact per object: every write updates the object's `SnapSet.seq` ([PrimaryLogPG.cc:8670](https://github.com/ceph/ceph/blob/v19.2.2/src/osd/PrimaryLogPG.cc#L8670)), a clone is made only when the head exists ([:8554](https://github.com/ceph/ceph/blob/v19.2.2/src/osd/PrimaryLogPG.cc#L8554)), and a read at a snapshot older than `seq` with no clone is `ENOENT`.
2. **Replaying committed records again in LSN order is harmless, so neither a version check nor a waiting checkpoint is needed.** Every block ends at the last record in LSN order that wrote it, as when the records were first mapped, and a freeze applied twice changes nothing (R1). A cleaner move in the replayed range is undone and the block maps back to the record it was copied from, which is still on disk because records after the replay point stay live.
3. **The condition is that a primary maps a chunk's writes in version order.** Out of order, the index is wrong even without a crash (R2). Commits per chunk are in version order when every member persists a chunk's writes in the order the primary sent them, so this is a rule for the primary, not a cost.
4. **A dropped write must be superseded in the log of every member that logged it.** Replay and rebuild apply every logged record, so a write peering dropped comes back unless a later record of the same member covers its blocks (R3). Recovery's copy of the authoritative blocks under a new version is that record (R3b); a version check would not help, since the dropped write's version is newer than the blocks it replaced.
5. **The cost is replay length.** While a write is pending, every checkpoint keeps the same replay point, so replay grows by everything the disk logged while the oldest write waited, at most `node_down_after` (5 s) during a member failure, and those segments cannot be cleaned until the write resolves.

Not covered: the cleaner's own records in a rebuild (a copy of a block that a later write replaced must not apply), more than one primary per disk, and peering itself.
