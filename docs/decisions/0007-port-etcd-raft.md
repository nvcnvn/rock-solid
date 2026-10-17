# 0007. Port etcd-raft for the Raft shard

Status: Accepted (2026-10-17)

## Context
The MetaStore runs about 1,000 Raft groups on each node. The [per-disk log](0003-one-log-per-disk.md) needs a Raft implementation that leaves persistence to its caller, and the sans-IO core needs one that takes time, randomness and I/O from outside: `on_event(now, ev) -> Vec<Action>`.

The [multi-raft spike](../design.md#appendix-multi-raft-spike) ruled out both Rust libraries as they are. openraft 0.9 costs up to about 670 KiB and 11 tokio tasks per group and can't be replayed exactly. raft-rs has the right `RawNode`/`Ready` shape but fails cargo-deny and draws randomness from the OS. [ADR 0006](0006-own-raft-shard.md) therefore chose to write our own shard, shaped like etcd-raft's, and to port its tests.

The [Raft port spike](../design.md#appendix-raft-port-spike) ported etcd-raft v3.7.0 itself, minus its goroutine wrapper and protobuf codec, into that sans-IO shape:

- **Fidelity:** all 28 upstream interaction test files give byte-identical output, and 206 ported unit tests pass, including all of `raft_test.go` and `raft_paper_test.go`.
- **Safety:** 1,000 seeds each on 3 and 5 nodes, with drops, partitions, crashes, snapshots and joint membership changes, found no violation. A deliberately broken disk is caught.
- **Size:** 6,811 lines of Go became 6,173 lines of Rust.
- **Cost:** 7.7 KiB of heap per group and 22 to 33% less CPU per idle message than raft-rs on the same host. Writes are 8 to 13% slower.
- **Rules:** no dependency beyond `rand_chacha`, cargo-deny passes, clean under the sans-IO clippy rules, no `unsafe`, and one seed replays a run exactly.

A Raft written from scratch would have to find again the edge cases etcd-raft has fixed over years of production use in etcd and CockroachDB.

## Decision
The Raft shard in `core` is a port of etcd-raft, pinned to a release tag (v3.7.0, commit `b867cf1`), starting from `spikes/raft-port/eraft`:

- Keep etcd-raft's file layout, type and function names (in snake case), so upstream fixes can be followed file by file. Every ported file names its source and keeps the Apache License 2.0 attribution.
- Deviate only where the sans-IO shape needs it, and list each deviation in the file it affects: plain-struct messages, a seeded ChaCha8 stream per replica for election timeouts, `BTreeMap`/`BTreeSet` instead of Go maps, storage owned by `RawNode`, a minimal logger trait.
- Port etcd-raft's unit tests and datadriven testdata with the code. An upstream update moves the pinned tag, ports the changed code and reruns every testdata file byte for byte.
- Drive the shard with synchronous `Ready`/`Advance`. etcd-raft's asynchronous storage writes stay off: with them a restored snapshot can overtake committed configuration changes still queued for apply, which panics in upstream etcd-raft too ([study](../../studies/etcd-raft-async-snapshot-apply/README.md)).

## Consequences
- The shard starts with upstream's test suite passing and its years of fixes, and keeps the properties ADR 0006 wanted: the driver owns ticks, messages and persistence, so it shares flushes, batches per node pair and quiesces idle groups, and the simulator replays it from a seed.
- We follow upstream releases instead of owning the protocol alone. Each update is a port plus a testdata run, and the pinned tag is recorded in `docs/tech-debts.md` when the code lands in `core`.
- Go names and structure carry into Rust where idiomatic Rust might differ, and about 110 pedantic lint warnings need fixing when the code moves from the spike into `crates/`.
- Writes cost 9 to 11% more CPU per operation than raft-rs until the write path is profiled.
- Asynchronous storage writes need two guards before they can be turned on: drop apply work that a restored snapshot covers, and never compact past the durable commit index.
- The fallback ADR 0006 kept, a vendored raft-rs fork, is no longer needed.
