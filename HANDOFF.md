# Handoff: Raft storage in the disk log

Branch `worktree-agent-a7bdcf5f49fef9af6`, based on main 6f8bb0e. Not ticked; not merged.

## Done (commits fffc20d, 64027f1, 402c930)

- `internal/raftshard/storage.go`: `Storage`, the shard's `raft.Storage`. Hard state, a base
  (index, term, ConfState of the last compaction or restored snapshot), and pointer-free entry
  slots whose payloads live in 64 KiB chunks (an entry larger than a chunk gets its own). `Entries`
  allocates one result slice per call, none per entry; `Append` truncates a conflicting tail;
  `Compact(i, cs)` takes the configuration at i (needed: a restart reads the base's ConfState) and
  drops chunks only compacted entries used. The chunks are on the Go heap: tech-debts entry
  "LSM pages, runs and retained Raft entries on the Go heap" extended.
- `internal/raftshard/round.go`: `Round` (HardState, Entries, Snapshot) now on `Output.Round`
  whenever `Output.Flush != 0`. `AppendRound` encodes it (flags; snapshot meta + data; first index,
  count, per-entry term/type/len/data; hard state; little-endian), `RoundItems` cuts the record into
  pool-buffer blob items of at most a record each, all `BlobRaftEntries` but the last
  `BlobRaftState`, `Storage.Replay(kind, payload)` buffers until the last blob and applies the round
  (later records win; a torn round, missing its last blob, changes nothing). `DecodeRound` is
  strict (re-encodes byte for byte), `FuzzDecodeRound` 45 s clean, two regression inputs in testdata.
- Shard: `Config.Storage *Storage`, `Config.SnapshotData func(index) []byte` (the seam for the
  Shard state machine's point-in-time view), `Shard.Applied() (index, ConfState)` for the state
  machine to keep with a memtable flush and pass to `Compact`. Snapshots are built on demand at
  the applied index; `raft.ErrSnapshotTemporarilyUnavailable` while `ApplyConfChange` runs (etcd-raft
  sends to an added learner inside it, before the applied index/conf hold it). A sent `MsgSnap` is
  reported `SnapshotFinish` a heartbeat later (in `tick`), else a leader whose log compacted past the
  snapshot stays in `StateSnapshot` forever (seen in `TestAReplacementNeverStallsProposals` seed 0);
  reporting at once livelocks with a follower that refuses it. Plant `staleSnapshot` +
  `TestALearnerCatchesUpFromASnapshotBuiltAtTheAppliedIndex`.
- Tests pass: `go test ./internal/raftshard`, lint.sh, seeds.sh for TestRaftRunsReplayFromTheirSeed
  and TestVoterReplacementRunsReplayFromTheirSeed (2000 seeds each). Bench: Append+Compact 0
  allocs/op, Entries 1 alloc per call.

## Remaining

1. **Driver: round -> IOID.** A small host per disk (in `raftshard`) that submits `RoundItems` to
   `disklog.Log.Submit(Hot, ...)`, maps the last ticket of each round to (owner, round), calls
   `Shard.Durable` on that ticket's `Durable` event, retries `RoundItems` on `ErrPoolExhausted`, and
   records each durable round's `BlobRef`s (segment, block, LSN, entry index from the event).
2. **Disk log liveness of Raft blobs.** `registerBlobs` tracks only checkpoint kinds. Proposed: a
   second `pieceSet` for `BlobRaftEntries`/`BlobRaftState` (not counted by `HoldsPiece`), registered at
   durability and at replay, added in `recount`, and a `RaftBlobsDead(refs)` that drops at once
   (`Cleanable` already guards records after replay LSNs). A round's blobs die once every entry in
   it is compacted AND a later durable round holds a newer hard state (else the vote is lost).
3. **Replay LSN per shard index.** min(LSN of the oldest live round, memtable start); an idle
   shard's newest hard-state round pins it, so a checkpoint re-logs the hard state of a shard whose
   pin falls behind (as etcd's WAL rotation does). Needs the multi-index checkpoint (see split).
4. **Crash between fsync and Durable**: a sim like `sim_test.go` whose nodes run a real
   `disklog.Log` on a simulated disk and rebuild `Storage` by `Replay` at restart (today the sim's
   `Storage` survives the crash and is written only at `Durable`, so it cannot show this). Register
   "a committed entry is never lost" with the simulator there. Add to seeds.sh + seed-runner table.
5. **Removed replica**: on `Output.Removed` the host reports all the shard's live round blobs dead.
6. **Record contents** test: one round with a chunk write and two shards' rounds -> one record, one
   `DiskFsync` (disklog level, no Store needed).
7. Docs: design.md "Log entries" bullet (round = blobs, last `BlobRaftState`; snapshot on demand;
   reported finished a heartbeat later; Compact takes the conf), glossary if needed, tick the item.

## Proposed split (decision for the user)

Items 3, the "Bound" property, retained entries joining the checkpoint-backpressure check, and
"a shard restores from its manifest's runs" need shard indexes checkpointed on the same disk as the
chunk index (the chunk store's `Store` owns the only checkpoint today) and the Shard state machine's
LSM tree. Proposal: finish 1, 2, 4, 5, 6, 7 here; move the multi-index checkpoint (per-shard
replay LSN, hard state carried across checkpoints, backpressure count, manifest restore, the
"index rebuild restarts the whole log" tech debt) into the Shard state machine item or a new
"Shard indexes in the disk owner's checkpoint" item after it.

## Pitfalls

- Restart applies `Config.Applied = Storage.Base().Index` and raft uses `InitialState`'s ConfState,
  so the base's ConfState must be the conf at the base index.
- Snapshot data in a round record is bounded only by splitting across blobs; a real shard's
  snapshot must become LSM runs instead (state machine item).
- Worktree hook refuses heredocs with python; write scripts to the scratchpad and run them.
