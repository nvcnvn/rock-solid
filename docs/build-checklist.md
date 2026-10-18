# Build checklist: phases 1 to 5

Phases 1 to 5 of the [roadmap](design.md#roadmap-and-open-questions) produce nothing a user can run; the first usable product is phase 6. This list breaks those phases into building blocks that can each be reviewed on their own. An item is a **component** (a state machine or driver), a **lib** (a dependency or a shared encoding) or a **test** (a suite, harness or measurement). Each one names its crate and what must hold before it is reviewed. The design lives in [design.md](design.md); items link to it rather than restate it.

Items are in build order within a phase. An item can start once the items it uses are reviewed.

**Every item, before review**

- `scripts/lint.sh` passes. Sans-IO crates (`core`, `meta`, `raft`, `sim`, `models`) pull in no I/O or OS randomness, even indirectly.
- Unit tests feed events and assert on actions; no mocks.
- New invariants are registered with the simulator's checker in the same change.
- Every failing seed found so far is a regression test.
- `docs/design.md` matches the code, and `docs/tech-debts.md` lists any pin or workaround.

## Phase 1: sans-IO core and simulator

**Foundations**

- [x] **Event and Action types** (component, `core`). `Tick`, `IoId`, `TimerId`, `NodeId`, `Event`, `Action` as in [Modularity](design.md#modularity-sans-io-core-pluggable-drivers). Review when the three driver-contract rules are expressible: data travels only as `IoBuf` handles, control messages of 256 bytes or less go inline, and a `PeerLost` fails a peer's outstanding requests.
- [x] **Buffer pool interface** (component, `core`; heap pool in `sim`). `IoBuf` handles from a pool of explicit size. Review when an exhausted pool is an error condition the caller handles, never a panic or a heap fallback.
- [x] **Seeded RNG** (lib, `core`). `rand_chacha` or `rand_pcg` with `default-features = false`. Review when the sans-IO dependency check passes and the RNG is the only source of randomness in `core`.
- [x] **IDs and hashing** (lib, `core`). 128-bit `ChunkId` packing and the PG hash (`xxh3_64`, seed 0, top bits) from [ADR 0004](decisions/0004-key-layout-and-ids.md) and [placement groups](design.md#data-layer-chunkstore). Both are frozen disk format. Review with golden vectors checked into the repo.
- [x] **CRC32C** (lib, `core`). Pick a crate that builds without `std` I/O; review with the RFC 3720 test vectors.

**Disk log** ([On-disk format](design.md#on-disk-format), [ADR 0003](decisions/0003-one-log-per-disk.md))

- [x] **Segments** (component, `core`). Segment size min(64 MiB, disk ÷ 4096) rounded down to a power of two; segment header carries the format version and the stream tag. The tag leaves room for a chunk-slot layout for whole chunks without a format break ([open question](design.md#roadmap-and-open-questions)).
- [x] **Record codec** (component, `core`). Header block with per-4K CRC32C and a header checksum, capped at `max_group_commit_bytes`. Review with a property test: for every torn prefix and every subset of landed sectors, decoding returns the whole record or rejects it.
- [x] **Group commit** (component, `core`). One write and one flush per round, accepting records from many producers (chunk writes and any number of shards), so either thread-per-core shape stays open. Two streams. Review when the action trace shows exactly one `DiskFsync` per round.
- [x] **Recovery** (component, `core`). Load the last manifest, replay the log after it; memtables flush once `max_replay_bytes` of records follow the manifest.
- [x] **LSM tree, stage 1** (component, `core`). Memtable plus periodic full snapshots and manifests in the log, behind the interface stage 2 will implement.
- [x] **Chunk index** (component, `core`). Extent map `(pool, h, chunk_id, version, first block) → (location, block count, record header)`; rebuild by scanning record headers, punches included.
- [x] **Streaming snapshots, compact memtable** (component, `core`). A snapshot reads a frozen view of the memtable and hands the log one piece at a time, each built at its final size; `MemTree` stores each entry as one allocation and restores by bulk build. Review against the [MetaStore memory spike](design.md#appendix-metastore-memory-spike): about 235 B per file, and a snapshot's peak is the tree plus one piece plus what the frozen view copies.
- [x] **Write-side decode check** (test, `core` and `sim`). In debug builds and the simulator, group commit decodes every record it just encoded and asserts the result equals what it submitted, and a round asserts it never builds a record with no entries. A property test checks that anything `submit` accepts decodes. Review when a deliberately broken encoder fails the simulator's seeds.
- [x] **Superblock copy** (component, `core`). A second superblock in the last block of segment 0, written and flushed after the first; recovery uses a valid copy and rewrites a damaged one, but never writes a block 0 of zeros or another format ([ADR 0008](decisions/0008-repair-superblock-only-from-own-block-0.md)). Changes the disk format, so it lands before the format is frozen at the first release.
- [x] **Quarantined segments** (component, `core`). A segment whose header fails to decode is free only if none of its blocks holds a record header of this log; otherwise recovery reports it as quarantined and group commit never reuses it ([Recovery](design.md#on-disk-format)). Review with the simulator's bit flips on segment headers: no acknowledged record is ever overwritten.
- [ ] **Streaming restore** (component, `core`). Recovery hands each index its snapshot pieces one at a time instead of returning them all in `Recovered`, and `Tree` restores from them in order, still by bulk build. Restoring every piece at once peaks at 1.81× the tree ([MetaStore memory spike](design.md#appendix-metastore-memory-spike), F5). Review when the spike's harness measures a restart's peak at the tree plus one piece plus a 16 B handle per entry for the bulk build's sort.

**ChunkStore and cleaner**

- [ ] **Chunk index overlap rule** (component, `core`). Decide in an ADR how the index orders extents of different versions that cover the same blocks: trim and split older entries on insert so the live version's map never overlaps (Ceph BlueStore, Btrfs), or keep every entry with its LSN and resolve at read time (RocksDB). Either way, frozen versions keep their entries, and the index counts live blocks per record for the cleaner. Review when replaying records in LSN order rebuilds the identical map and a read of the live version costs one lookup per block.
- [ ] **Single-node ChunkStore** (component, `core`). `write`, `punch`, `read`, `flush` with versions, `expect_ver` compare-and-set, `at_ver` reads and tombstones ([Data layer](design.md#data-layer-chunkstore)). A write larger than one record (`max_group_commit_bytes`, 1 MiB, so every whole 4 MiB chunk) goes to the log as several items of the same version, acknowledged once all are durable. It handles `Failed` and `CheckpointFailed` events from the log, and ends the index's snapshot when a checkpoint fails. It hands the log one snapshot piece at a time, each once the one before is durable. `seal` exists in the API only.
- [ ] **Segment usage table** (component, `core`). Each manifest carries state, live blocks and newest LSN per segment, with quarantined as one of the states so a quarantine outlives the start that found it; free segments come from it, and a rebuild reports a gap the table cannot explain as corruption ([Free space](design.md#on-disk-format)).
- [ ] **Segment reserve** (component, `core`). A few segments only checkpoints and the cleaner may use; ordinary writes get `NoSpace` while only the reserve is free ([Free space](design.md#on-disk-format)). Review when the simulator fills a disk to its reserve and a checkpoint and a cleaner pass still finish.
- [ ] **Cleaner** (component, `core`). Cost-benefit victim choice with the net-gain guard and the age cap, two streams, blocks regrouped by chunk, a TRIM action for freed segments. It takes live counts from the usage table and writes into the reserve when the disk is full. It is its own state machine, which the driver runs on a worker thread; its moves reach the index's owner as an event and apply only if the index still points at the copied block ([parallel cleaner spike](design.md#appendix-parallel-cleaner-spike)).

**Simulator** ([Testing strategy](design.md#testing-strategy))

- [ ] **Deterministic scheduler** (component, `sim`). One thread, simulated time, one seed. Review when two runs of a seed produce byte-identical action traces.
- [ ] **Simulated disk** (component, `sim`). Torn writes, loss of unflushed data on crash, bit flips, misdirected writes, fail-slow.
- [ ] **Invariant checker** (component, `sim`). Phase 1 invariants: acknowledged writes are readable; punched ranges read zeros; every cleaner pass frees more segments than it fills; a cleaner move never replaces a write that landed during cleaning; no segment is reused while it holds a live block; a disk full except for its reserve still finishes a checkpoint and a cleaner pass; a rebuilt chunk index equals the one recovered from its manifest.
- [ ] **History recorder** (component, `sim`). `(invoke_time, complete_time, op, result)` in a format Porcupine reads.
- [ ] **Seed runner in CI** (test). Thousands of seeds per commit; a failing seed prints a one-line replay command.
- [ ] **Planted-bug check** (test, `sim`). The simulator replays a failing seed exactly and finds a bug planted in a toy replication protocol ([done-when checkpoint](design.md#learning-project)).
- [ ] **Cleaner write amplification in simulation** (test, `sim`). Uniform overwrites at 80 / 90 / 95% full land near the [cleaner spike](design.md#appendix-cleaner-write-amplification-spike)'s 3.4 / 7.2 / 17.3. Then add Raft entries and LSM snapshots to the log and measure their cleaning cost, which answers that open question.

## Phase 2: protocol models

- [ ] **`stateright`** (lib, `models`). Review its dependency tree against `scripts/lint.sh` and cargo-deny; if its checker's threads or RNG break the sans-IO check, `models` keeps it as a dev-dependency.
- [ ] **Epoch change model** (test, `models`). Peering and per-node-pair read leases with bounded clock drift. Properties: no PG serves reads from two primaries at once; no write tagged `e` completes after `e + 1` activates; epochs never go backwards.
- [ ] **Sessions and fencing model** (test, `models`). Fence acknowledgments from every primary ([Sessions and fencing](design.md#sessions-and-fencing)). Properties: no fenced session's write is applied; no lock reaches a new holder while a primary would still accept the old holder's writes.
- [ ] **Cache-mode recall model** (test, `models`). Lost and delayed recalls, a client paused past its lease, owner failover mid-recall. Property: no stale read in WriteShared mode.
- [ ] **Purge model** (test, `models`). Crash mid-purge, unlink racing a write, truncate followed by extend. Properties: an interrupted purge reruns safely; a purge never deletes newer data.
- [ ] **Model-to-simulator map** (test). Each model states the depth it was checked to, and each property names the simulator invariant that checks it in the real code.

## Phase 3: replication and placement

The control-plane Raft group arrives in phase 4, so here a simulated single authority stands in for it.

- [ ] **Wire messages** (lib, `core`). Plain structs with a version; chunk writes carry the epoch and the session ID from the start.
- [ ] **Pools** (component, `core`). Settings and creation rules for 1 to 3 copies and `min_live` ([Pools and redundancy](design.md#pools-and-redundancy)); erasure coding waits for phase 10.
- [ ] **Placement computation** (component, `core`). Refill, then balance within the fill margin, under the failure-domain rule ([ADR 0002](decisions/0002-placement-table-computed-by-control-plane.md)). Review with property tests: after any add, remove and failure sequence, every PG keeps its width with at most `max_per_domain` per domain, no disk exceeds the fill margin, and moves stay near the minimum.
- [ ] **Replica primary** (component, `core`). Parallel fan-out, commit once every live member holds the version, reads of committed versions only, rejection of stale epochs, `min_live` and `maintenance`.
- [ ] **Peering and read leases** (component, `core`). As modeled in phase 2, leases renewed by node-pair heartbeats ([Epoch changes](design.md#epoch-changes-and-failure-detection)).
- [ ] **Failure and fail-slow detection** (component, `core`). Node heartbeats and `node_down_after`; `member_timeout` reports with a direct probe; fail-slow marking guarded by `min_live`, and rejoin only after a whole normal `slow_disk_window`.
- [ ] **Recovery per PG** (component, `core`). Most-degraded PGs first; resync after maintenance.
- [ ] **Simulated network** (component, `sim`). Drop, delay, reorder, duplicate, partition; node crash and restart; clock drift.
- [ ] **Placement faults and invariants** (test, `sim`). The placement faults listed in [Testing strategy](design.md#testing-strategy), checked against: one read-serving primary per PG; no write after a newer epoch; every chunk reaches its pool's copy count once recovery settles; too few copies return an error, never wrong data.
- [ ] **Consistency check of chunk histories** (test). Porcupine finds a linearizable order per chunk for recorded simulation histories.

## Phase 4: embedded MetaStore

- [ ] **Raft write-path profile** (test). Profile `spikes/raft-port/bench` writes against raft-rs and find the 8 to 13% gap ([open question](design.md#roadmap-and-open-questions)). Fix it inside the port only where the change stays local and is listed as a deviation; otherwise accept the gap.
- [ ] **Raft port crate** (lib, `raft`). The etcd-raft v3.7.0 port moved from `spikes/raft-port/eraft` into `crates/raft` (package `rock-raft`, sans-IO), with the pedantic lint fixes and the test harness off `std::fs`. Each ported file keeps a short header: the etcd copyright line and "ported from etcd-raft v3.7.0, Apache License 2.0"; the file map and deviation list move from the file headers into `crates/raft/UPSTREAM.md`. The pinned tag goes into `docs/tech-debts.md` as a version pin, and `crates/raft/check-upstream.sh` lists the Go files changed since the pin ([ADR 0007](decisions/0007-port-etcd-raft.md)).
- [ ] **Raft shard** (component, `core`). Wraps `rock-raft` in `on_event`: ticks from `ctx.now`, messages from `NetRecv`, payloads over 256 bytes as `IoBuf`s. Synchronous `Ready`/`Advance`, a 10 ms election tick under a 100 ms heartbeat ([Raft shard](design.md#metadata-layer-metastore)).
- [ ] **Ported Raft tests** (test, `core`). etcd-raft's unit tests and datadriven testdata, compared byte for byte and rerun on every upstream tag update.
- [ ] **Maelstrom harness** (test). A small I/O adapter that runs the Raft shard as a Maelstrom node; passes the linearizable key-value workload under partitions ([done-when checkpoint](design.md#learning-project)).
- [ ] **Raft persistence in the disk log** (component, `core`). Entries and hard state join the round's record; an entry is dead to the cleaner once the memtable that applied it is flushed.
- [ ] **Shard state machine** (component, `meta`). `txn` (compare-and-set, put, delete, all or nothing) and `scan(prefix, after, limit)` on the stage 1 LSM tree, with one log entry per leader per round.
- [ ] **Key encoding** (lib, `meta`). One-byte tags, fixed-width big-endian integers, a variable last component. Review with golden vectors and a property test that byte order equals logical order.
- [ ] **Sharded API, one shard** (component, `meta`). Routing through the shard map and the single-shard fast path; the API leaves room for cross-shard transactions.
- [ ] **Control-plane group** (component, `meta`). Pools, placement table, membership, fenced sessions and format level under `ctl/`, replacing phase 3's stand-in.
- [ ] **Filesystem schema** (component, `meta`). Inode ID allocation by slot and sequence, inodes, directory entries, xattrs, sessions and locks, orphans, purge lists, slot byte counters and replay records ([Key layout](design.md#metadata-layer-metastore)).
- [ ] **MetaStore invariants** (test, `sim`). At most one leader per shard per term; a committed entry is never lost; a shard recovered from manifest and log equals its state before the crash; at most one lock holder; a replayed request returns its first result and changes nothing.

## Phase 5: default drivers

- [ ] **Event loop** (component, `daemon`). One epoll loop per core over every driver's readiness fd; executes actions, feeds events.
- [ ] **Clock and timers** (component, `daemon`). Monotonic clock to `Tick`; `SetTimer` on timerfd or the io_uring timeout.
- [ ] **io_uring disk driver** (component, `drivers-uring`). File or raw device, `O_DIRECT`, write then `IORING_FSYNC_DATASYNC`, `durability: fua` as a setting, file-backed segments written in full at creation, discard for freed segments, an aligned heap buffer pool.
- [ ] **TCP network driver** (component, `drivers-tcp`). One connection per node pair, framing, messages batched per node pair, `PeerLost` on any transport error, reconnect.
- [ ] **Round driver** (component, `daemon`). Collects every shard's `Ready` and the chunk writes once per round, bounded by size and time, and decides how disk logs map to cores. Measure commit latency p50 and p99 with 32,000 proposals in flight; this answers two [open questions](design.md#roadmap-and-open-questions).
- [ ] **Shared ChunkStore suite** (test). One suite, run on the simulated disk and on io_uring ([done-when checkpoint](design.md#learning-project)).
- [ ] **Shared network suite** (test). One suite, run on the simulated network and on TCP, ready for RDMA in phase 9.
- [ ] **Durable write benchmark** (test). 4K at queue depth 1 with idle gaps, against the [NVMe flush spike](design.md#appendix-nvme-flush-spike)'s floor of about 0.95 ms (2 ms after idling), cross-checked with fio.
- [ ] **Crash tests on real disks** (test). `kill -9` mid-round, and device-mapper error and delay targets where root is available; every acknowledged write survives.
