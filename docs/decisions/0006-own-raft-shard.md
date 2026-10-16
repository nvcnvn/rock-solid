# 0006. Write our own Raft shard

Status: Accepted (2026-10-16)

## Context
The MetaStore runs about 1,000 Raft groups on each node. The [per-disk log](0003-one-log-per-disk.md) needs a Raft implementation that leaves persistence to its caller, and the sans-IO core needs one that takes time, randomness and I/O from outside: `on_event(now, ev) -> Vec<Action>`.

The [multi-raft spike](../design.md#appendix-multi-raft-spike) measured openraft 0.9.25 and raft-rs 0.7.0 with 1,000 groups × 3 replicas in one process:

- **openraft** preallocates a 4,096-command queue per replica, so its resident memory creeps toward about 670 KiB per replica. It runs 11 tokio tasks per group and costs about 3.5× raft-rs in idle CPU. Its leader calls storage `append` once per client write. It depends on tokio unconditionally, and a run replays only with a custom runtime, and even then not guaranteed. Version 0.10 batches writes and abstracts the runtime, but it has been in alpha for over two years and keeps per-group tasks and ticks.
- **raft-rs** has the right shape. Its `RawNode` returns a `Ready` of entries to persist, messages to send and entries to apply, and the caller owns ticks, messages and persistence. That gave 13 KiB per group, 4.5 µs of CPU per idle message, one fsync for every group's `Ready` and 0.07% of a core with idle groups quiesced. But its last release is from 2023 (TiKV uses git master), it fails our cargo-deny check on protobuf 2 and fxhash, and it draws election timeouts from `thread_rng()` unless a `#[doc(hidden)]` test hook overrides them.
- Both pull in `getrandom` through `rand`, which the sans-IO lint bans.

## Decision
The Raft shard is our own state machine in `core`, shaped like etcd-raft's and raft-rs's `RawNode`/`Ready`:

- the driver feeds it ticks and messages, and it returns entries to persist, messages to send and entries to apply;
- ticks, message batching and persistence belong to the driver;
- election timeouts come from a seeded RNG, state lives in `BTreeMap`s, and messages are plain structs, not protobuf;
- pre-vote and check-quorum are on from the start.

etcd-raft and raft-rs test cases are ported as fixtures. If the schedule needs consensus before our shard exists, the fallback is a vendored raft-rs fork with protobuf replaced by plain structs and the RNG injected.

## Consequences
- The Raft shard runs under the simulator like every other state machine, and one seed replays it exactly.
- The driver can share one flush across every shard on a disk, batch messages per node pair and stop ticking idle shards, because no shard runs its own tasks or timers.
- `core` takes no tokio, protobuf or `getrandom` dependency, and cargo-deny passes.
- We own a consensus implementation, the riskiest code in the system. Ported tests, a `stateright` model and deterministic simulation cover it. openraft's own fuzzer and Jepsen suite found quorum-safety bugs in 2026, so mature libraries are no guarantee either.
- Snapshots, log compaction, membership changes, leader transfer and read index are ours to write.
- openraft is worth re-measuring once 0.10 is stable, with its simulated runtime, `openraft-multi` and batched writes.
