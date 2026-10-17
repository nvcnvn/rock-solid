# etcd-raft: with async storage writes, a snapshot can overtake queued conf changes

**Status:** reproduced on upstream etcd-raft v3.7.0 and `main` (638f427, 2026-10-09). Not reported upstream. A keyword search of etcd-io/raft issues and pull requests found no existing report.

**Found by:** the [Raft port spike](../../spikes/raft-port/README.md) (test P9, seed 237, 3 nodes), in the Rust port first and then in Go with the same panic.

## Summary

With `Config.AsyncStorageWrites`, etcd-raft hands log writes to a local append thread and committed entries to a local apply thread, as messages. The documentation says messages to *different* threads may be processed in any order (raft.go, the `AsyncStorageWrites` comment: "Messages to different targets can be processed in any order").

A follower can therefore restore a snapshot, and switch to the snapshot's configuration, while a committed conf change it handed to the apply thread earlier has not been applied yet. When the apply thread then calls `ApplyConfChange` with that older entry, raft applies it on top of the snapshot's configuration, which already includes it. In the reproduced case the entry leaves a joint configuration that is no longer joint, and raft panics:

```
panic: can't leave a non-joint config
```

## Mechanism

1. Node 3 appends entry 13 (leave the joint configuration) and learns it is committed. raft sends it to the apply thread in a `MsgStorageApply`, and the apply thread does not run yet.
2. Entry 14 commits on the other nodes, and the leader sends node 3 a snapshot at index 14 whose configuration is `voters=(1 2 3 4)`, not joint.
3. Node 3 steps the `MsgSnap`. `restore` (raft.go:1860) only checks that the snapshot index is above `raftLog.committed` (13). It doesn't check what is still queued on the apply thread (`raftLog.applying`). It restores, and calls `switchToConfig` with the snapshot's configuration.
4. The apply thread now runs entry 13. The application calls `ApplyConfChange` as documented, and the changer rejects leaving a non-joint configuration (confchange/confchange.go:99-100). `applyConfChange` turns every changer error into a panic (raft.go:1965-1967, `TODO(tbg): return the error to the caller`).

The application can't easily avoid it: `RawNode` reports the restored snapshot only in the next `Ready`, after `Step` has already switched the configuration, so the apply thread can't tell that entry 13 is now covered by the snapshot.

**Inferred, not reproduced:** for a stale simple change (add or remove a voter) instead of leaving a joint configuration, the changer would probably succeed, silently changing that node's configuration away from the cluster's. This comes from reading `confchange.go`; no scenario for it has been run.

## Reproduce

Needs git and Go; nothing else from this repository.

```
./repro.sh            # etcd-raft v3.7.0
./repro.sh main       # upstream main
```

The script clones etcd-raft, replaces its `testdata/` with [async_snapshot_overtakes_apply.txt](async_snapshot_overtakes_apply.txt), and runs etcd-raft's own `TestInteraction` datadriven test in rewrite mode, so the only possible failure is the panic. Output from both runs is in [results.txt](results.txt):

```
panic during testdata/async_snapshot_overtakes_apply.txt:94:
panic: can't leave a non-joint config
```

The scenario uses only upstream interaction-test directives (`add-nodes ... async-storage-writes=true`, `propose-conf-change`, `process-append-thread`, `send-snapshot`, `process-apply-thread`). Every directive before the last one also passes unchanged on Go, with expected output written by the Rust port (spike `run.sh p9`, last line of `results-p9.txt`).

## Possible fixes (for discussion)

- **In raft:** refuse a snapshot while committed entries are still queued for apply (`raftLog.applying > raftLog.applied`), answering the leader as for an ignored snapshot. The leader retries later.
- **In raft:** make `applyConfChange` ignore a conf change whose index is at or below the last restored snapshot's index, instead of applying it to the snapshot's configuration.
- **In the application:** drop apply-thread work covered by a restored snapshot. The spike's simulator does this, but it can only see the pending snapshot by reaching into `raftLog.unstable`; the public API gives no signal before the next `Ready`.
- **Documentation:** at least state the rule, since the current comment allows the ordering that triggers it.

## Related observation (not reproduced in Go)

Also with async writes, the apply thread can run ahead of the durable `HardState`. If the application takes a snapshot and compacts at the applied index while that index is above the durable `HardState.commit`, the next `newRaft` panics: `committed` starts at the snapshot index (log.go:93) and `loadState` rejects a lower `HardState.commit` (raft.go:2038-2039). Found while building the spike's simulator, by reading the source.

## Open before deciding what to do

- How CockroachDB, the main user of `AsyncStorageWrites`, avoids this (for example, by applying snapshots under the same lock as entry application) has not been checked.
- Whether a stale simple conf change really changes the configuration silently (above) needs its own scenario.
