# Build checklist: phases 1 to 5

Phases 1 to 5 of the [roadmap](design.md#roadmap-and-open-questions) produce nothing a user can run; the first usable product is phase 6. This list breaks those phases into building blocks that can each be reviewed on their own. An item is a **component** (a state machine or driver), a **lib** (a dependency or a shared encoding) or a **test** (a suite, harness or measurement). Each one names its package under `internal/` and what must hold before it is reviewed. The design lives in [design.md](design.md); items link to it rather than restate it. Where [test-scenarios.md](test-scenarios.md) has a section for an item, its tests cover every scenario in that section.

Items are in build order within a phase. An item can start once the items it uses are reviewed.

**Every item, before review**

- `scripts/lint.sh` passes, and sans-IO packages pass the sans-IO analyzer ([Modularity](design.md#modularity-sans-io-core-pluggable-drivers)).
- Hot-path benchmarks report zero allocations per operation.
- Unit tests feed events and assert on actions, with no mocks; every decoder has a fuzz test.
- New invariants are registered with the simulator's checker in the same change.
- Every failing seed found so far is a regression test.
- `docs/design.md` matches the code, and `docs/tech-debts.md` lists any pin or workaround.

## Phase 1: sans-IO core and simulator

**Tooling**

- [x] **Module, toolchain and lint** (lib). One Go module pinned with `toolchain go1.27.0`; `scripts/lint.sh` runs `gofmt`, `go vet`, `staticcheck`, `govulncheck` and a dependency license check.
- [x] **Sans-IO analyzer** (lib, `tools/sansio`). A `go/analysis` pass that `scripts/lint.sh` runs through `go vet -vettool` on every sans-IO package ([rules for state machines](design.md#modularity-sans-io-core-pluggable-drivers)). Review when it rejects a planted `range` over a map, `time.Now`, `go` statement, channel, `sync` import and global `math/rand` call in `internal/core`, and accepts the same code in a test file and in `internal/loop`.

**Foundations** (`internal/core`)

- [x] **Event and Action types** (component). `Tick`, `IOID`, `TimerID`, `NodeID`, `Context`, `Event`, `Action` as in [Modularity](design.md#modularity-sans-io-core-pluggable-drivers). `DiskWrite` takes a vector of buffers, so group commit writes producers' buffers in place without copying them into one image. Review when the three driver-contract rules are expressible: data travels only as `IOBuf` handles, control messages of 256 bytes or less go inline, and a `PeerLost` fails a peer's outstanding requests.
- [x] **Buffer pool interface** (component; heap pool in `internal/sim`). `IOBuf` handles of a slot and a generation, from a pool of explicit size. Review when an exhausted pool is an error the caller handles, never a panic or a heap fallback, and a stale handle is caught.
- [x] **Seeded RNG** (lib). `math/rand/v2` with a seeded PCG or ChaCha8 source per state machine, and a golden stream checked in.
- [x] **IDs and hashing** (lib). 128-bit `ChunkID` packing and the PG hash (`xxh3_64`, seed 0, top bits) from the [key layout](design.md#metadata-layer-metastore) and [placement groups](design.md#data-layer-chunkstore). Both are frozen disk format: the test reads `internal/core/testdata/chunk_id_golden.txt` unchanged.
- [x] **CRC32C** (lib). `hash/crc32` with the Castagnoli table; review with the RFC 3720 test vectors.
- [x] **Peer requests** (component). Outstanding requests per peer with their deadlines and buffers: a timeout keeps the buffer, `PeerLost` fails them all and returns their buffers, and a stale `Timer` is ignored.

**Disk log** (`internal/disklog`; [On-disk format](design.md#on-disk-format))

- [x] **Segments and superblock** (component). Segment size min(64 MiB, disk ÷ 4096) rounded down to a power of two; the segment header carries the format version and the stream tag, which leaves room for a chunk-slot layout for whole chunks without a format break ([open question](design.md#roadmap-and-open-questions)). Golden bytes are checked in as `internal/disklog/testdata/log_format_golden.txt`.
- [x] **Record codec** (component). Header blocks holding write, punch, freeze and blob entries, a CRC32C per data block and a header checksum, capped at `max_group_commit_bytes`. Review with a property test: for every torn prefix and every subset of landed sectors, decoding returns the whole record or rejects it.
- [x] **Group commit** (component). One write and one flush per round, accepting records from many producers (chunk writes and any number of shards), two streams, writing producers' buffers as one vectored write. A round's write and flush are emitted together so the driver can link them, and up to `commit_depth` rounds are in flight, a flush submitted only once its own and every earlier round's write have completed ([One flush per round](design.md#on-disk-format)). Review when the action trace shows exactly one `DiskFsync` per round, no block is copied, and a manifest larger than a record goes out in pieces.
- [ ] **Write-side decode check** (test). With the check on, as in the package's tests and the simulator, group commit decodes every record it just encoded and asserts it holds exactly what was submitted, and a round never builds a record with no entries. A property test checks that anything `submit` accepts decodes. Review when a deliberately broken encoder fails the simulator's seeds.
- [ ] **Segment usage table** (component). Each manifest carries state, live blocks and newest LSN per segment, with quarantined as one of the states so a quarantine outlives the start that found it. A segment is free only because the table says so, never because its header reads as zeros, and a rebuild reports a gap the table cannot explain as corruption ([Free space](design.md#on-disk-format)).
- [ ] **Recovery** (component). Load the last manifest and the runs it names, replay the log after each index's replay LSN; a checkpoint is due once `max_replay_bytes` of records follow the manifest. The tail rule accepts up to `commit_depth` torn or missing records and drops everything after the first. Segment headers are read with many reads in flight, since a disk has 4,096 to 262,144 of them.
- [ ] **Superblock copy** (component). A second superblock in the last block of segment 0, written and flushed after the first; recovery uses a valid copy and rewrites a damaged one, but never writes a block 0 of zeros or another format ([superblock repair](design.md#superblock-repair)).
- [ ] **Quarantined segments** (component). A segment whose header fails to decode is free only if the usage table says so and none of its blocks holds a record header of this log; otherwise recovery reports it as quarantined and group commit never reuses it. Review with the simulator's bit flips on segment headers: no acknowledged record is ever overwritten.

**LSM tree and chunk index**

- [ ] **LSM tree** (component, `internal/lsm`). A copy-on-write B+-tree memtable of 4 KiB pages in off-heap slabs ([Runtime in Go](design.md#runtime-in-go)); a flush that freezes it, starts an empty one and streams the frozen one into the log as a prefix-compressed sorted run, one piece at a time, each built at its final size; runs held in memory behind fence pointers; point-in-time views; a restore that loads runs and replays ([The LSM tree](design.md#on-disk-format)). A leaf redistributes into a neighbour before it splits. Review against the [Go memtable spike](design.md#appendix-go-memtable-spike) and [MetaStore memory spike](design.md#appendix-metastore-memory-spike): collection under 1 ms at 20 million entries, a flush's peak the tree plus one piece, a run near 135 B per file, and a get that touches the memtable and each run once. An entry larger than a piece is an error, not a panic.
- [ ] **Compaction** (component, `internal/lsm`). Size-tiered: runs of similar size merge into one on a worker goroutine, the owner swaps the result in, and the old runs are dead to the cleaner once a manifest naming the new one is durable. Review when a view opened before a compaction still reads the old runs, and a crash between the new run and its manifest recovers to the previous manifest plus replay.
- [ ] **Chunk index** (component, `internal/chunkstore`). In the LSM tree: one entry per chunk (version, snapshot epoch of its last write); a live map `(pool, h, chunk_id, 0, first block) → (version, location, block count, record header)` trimmed and split on insert so it never overlaps; a frozen copy of the map per `freeze` entry; live blocks counted per record for the cleaner ([chunk index](design.md#on-disk-format)). Rebuild by scanning record headers, punches and freezes included. Review when replaying records in LSN order rebuilds identical live and frozen maps, a read of the live version costs one lookup per block, and a read at a snapshot epoch finds the right frozen version.

**ChunkStore and cleaner** (`internal/chunkstore`, `internal/disklog`)

- [ ] **Single-node ChunkStore** (component). `Write`, `Punch`, `Read`, `Flush` with versions, `expectVer` compare-and-set, `atVer` reads and tombstones ([Data layer](design.md#data-layer-chunkstore)). Versions carry the epoch in their high 24 bits, fixed here; a write is mapped when it commits, which on one node is when its record is durable. A write larger than one record (`max_group_commit_bytes`, 1 MiB, so every whole 4 MiB chunk) goes to the log as several items of the same version, acknowledged once all are durable. It handles `Failed` and `CheckpointFailed` events from the log, and keeps the index's frozen memtable when a checkpoint fails. It hands the log one run piece at a time, each once the one before is durable. `Seal` exists in the API only.
- [ ] **Segment reserve** (component). A few segments only checkpoints and the cleaner may use; ordinary writes get `NoSpace` while only the reserve is free ([Free space](design.md#on-disk-format)). Review when the simulator fills a disk to its reserve and a checkpoint and a cleaner pass still finish.
- [ ] **Cleaner** (component). Cost-benefit victim choice with the net-gain guard and the age cap, two streams, blocks regrouped by chunk, a TRIM action for freed segments. It takes live counts from the usage table and writes into the reserve when the disk is full. It is its own state machine, which the daemon runs on a worker goroutine; its moves reach the index's owner as an event and apply only if the index still points at the copied block ([parallel cleaner spike](design.md#appendix-parallel-cleaner-spike)).

**Simulator** (`internal/sim`; [Testing strategy](design.md#testing-strategy))

- [ ] **Trace records** (lib, `internal/trace`). The fixed-field record of an event, an action or a syscall, its one-line text form `tick loop kind fields...`, and the checker library that scans a trace for a rule such as one `DiskFsync` per round ([Testing the design decisions](design.md#testing-the-design-decisions)). Review when the simulator and a planted event-loop trace use the same checker.
- [ ] **Deterministic scheduler** (component). One goroutine, simulated time, one seed. Review when two runs of a seed produce byte-identical action traces.
- [ ] **Simulated disk** (component). Torn writes, loss of unflushed data on crash, bit flips, misdirected writes, fail-slow.
- [ ] **Invariant checker** (component). Phase 1 invariants: acknowledged writes are readable; punched ranges read zeros; every cleaner pass frees more segments than it fills; a cleaner move never replaces a write that landed during cleaning; no segment is reused while it holds a live block; a disk full except for its reserve still finishes a checkpoint and a cleaner pass; a rebuilt chunk index equals the one recovered from its manifest; a chunk's live map never holds two entries for one block.
- [ ] **History recorder** (component). `(invoke_time, complete_time, op, result)` in a format Porcupine reads.
- [ ] **Seed runner in CI** (test). Thousands of seeds per commit; a failing seed prints a one-line replay command.
- [ ] **Planted-bug check** (test). The simulator replays a failing seed exactly and finds a bug planted in a toy replication protocol ([done-when checkpoint](design.md#learning-project)).
- [ ] **Cleaner write amplification in simulation** (test). Uniform overwrites at 80 / 90 / 95% full land near the [cleaner spike](design.md#appendix-cleaner-write-amplification-spike)'s 3.4 / 7.2 / 17.3. Then add Raft entries and LSM snapshots to the log and measure their cleaning cost, which answers that open question.

## Phase 2: protocol models

- [ ] **TLA+ and TLC** (lib, `models`). Specifications in TLA+, checked with TLC in CI; etcd-raft's own specification (`tla/` in its repository) is the template.
- [ ] **Epoch change model** (test, `models`). Peering and per-node-pair read leases with bounded clock drift. Properties: no PG serves reads from two primaries at once; no write tagged `e` completes after `e + 1` activates; epochs never go backwards.
- [ ] **Sessions and fencing model** (test, `models`). Fence acknowledgments from every primary ([Sessions and fencing](design.md#sessions-and-fencing)). Properties: no fenced session's write is applied; no lock reaches a new holder while a primary would still accept the old holder's writes.
- [ ] **Cache-mode recall model** (test, `models`). Lost and delayed recalls, a client paused past its lease, owner failover mid-recall. Property: no stale read in WriteShared mode.
- [ ] **Purge model** (test, `models`). Crash mid-purge, unlink racing a write, truncate followed by extend. Properties: an interrupted purge reruns safely; a purge never deletes newer data.
- [ ] **Model-to-simulator map** (test). Each model states the depth it was checked to, and each property names the simulator invariant that checks it in the real code.

## Phase 3: replication and placement

The control-plane Raft group arrives in phase 4, so here a simulated single authority stands in for it.

- [ ] **Wire messages** (lib, `internal/chunkstore`). Hand-written encodings with a version; chunk writes carry the epoch and the session ID from the start.
- [ ] **Pools** (component, `internal/chunkstore`). Settings and creation rules for 1 to 3 copies and `min_live` ([Pools and redundancy](design.md#pools-and-redundancy)); erasure coding waits for phase 10.
- [ ] **Placement computation** (component, `internal/chunkstore`). Refill, then balance within the fill margin, under the failure-domain rule ([placement table](design.md#data-layer-chunkstore)). Review with property tests: after any add, remove and failure sequence, every PG keeps its width with at most `max_per_domain` per domain, no disk exceeds the fill margin, and moves stay near the minimum.
- [ ] **Replica primary** (component, `internal/chunkstore`). Parallel fan-out, commit once every live member holds the version, a write mapped into the index only once it commits, reads of committed versions only, versions numbered from the epoch, rejection of stale epochs, `min_live` and `maintenance`.
- [ ] **Peering and read leases** (component, `internal/chunkstore`). As modeled in phase 2, leases renewed by node-pair heartbeats; a version is dropped only when a member live in the old epoch lacks it, and divergent members are recovered by copying from the authoritative one ([Epoch changes](design.md#epoch-changes-and-failure-detection)). A restarted primary peers before it serves.
- [ ] **Failure and fail-slow detection** (component, `internal/chunkstore`). Node heartbeats and `node_down_after`; `member_timeout` reports with a direct probe; fail-slow marking guarded by `min_live`, and rejoin only after a whole normal `slow_disk_window`.
- [ ] **Recovery per PG** (component, `internal/chunkstore`). Most-degraded PGs first; resync after maintenance.
- [ ] **Simulated network** (component, `internal/sim`). Drop, delay, reorder, duplicate, partition; node crash and restart; clock drift.
- [ ] **Placement faults and invariants** (test, `internal/sim`). The placement faults listed in [Testing strategy](design.md#testing-strategy), checked against: one read-serving primary per PG; no write after a newer epoch; every chunk reaches its pool's copy count once recovery settles; too few copies return an error, never wrong data.
- [ ] **Consistency check of chunk histories** (test). Porcupine finds a linearizable order per chunk for recorded simulation histories.

## Phase 4: embedded MetaStore

- [ ] **etcd-raft module** (lib). `go.etcd.io/raft/v3` v3.7.0 through a `replace` to a pinned fork whose one patch gives each replica a seeded election-timeout source in place of `crypto/rand` ([Raft shard](design.md#metadata-layer-metastore)). The pin and the patch go into `docs/tech-debts.md`. Review when a simulator seed replayed twice gives identical histories.
- [ ] **Upstream Raft tests** (test). etcd-raft's own unit tests and datadriven testdata pass on the patched fork, rerun on every upstream tag update.
- [ ] **Raft shard** (component, `internal/raftshard`). Wraps `raft.RawNode` in `OnEvent`: ticks from `ctx.Now`, messages from `NetRecv`, payloads over 256 bytes as `IOBuf`s. Synchronous `Ready`/`Advance`, a 10 ms election tick under a 100 ms heartbeat; a leader's appends go out with its own write, and a follower's acknowledgments wait for its round's flush ([Raft shard](design.md#metadata-layer-metastore)).
- [ ] **Raft storage in the disk log** (component, `internal/raftshard`). Entries and hard state join the round's record. `raft.Storage` serves every entry after the last compaction from the loop's off-heap slab; the log compacts, and its entries become dead to the cleaner, once the memtable that applied them is flushed.
- [ ] **Raft cost per group** (test). Heap per group, collection CPU and allocations per message at 1,000 groups per node with 32,000 proposals in flight, which answers that [open question](design.md#roadmap-and-open-questions).
- [ ] **Maelstrom harness** (test). A small I/O adapter that runs the Raft shard as a Maelstrom node; passes the linearizable key-value workload under partitions ([done-when checkpoint](design.md#learning-project)).
- [ ] **Shard state machine** (component, `internal/meta`). `Txn` (compare-and-set, put, delete, all or nothing) and `Scan(prefix, after, limit)` on the LSM tree, with one log entry per leader per round.
- [ ] **Key encoding** (lib, `internal/meta`). One-byte tags, fixed-width big-endian integers, a variable last component. Review with golden vectors and a property test that byte order equals logical order.
- [ ] **Sharded API, one shard** (component, `internal/meta`). Routing through the shard map and the single-shard fast path; the API leaves room for cross-shard transactions.
- [ ] **Control-plane group** (component, `internal/meta`). Pools, placement table, membership, fenced sessions and format level under `ctl/`, replacing phase 3's stand-in.
- [ ] **Filesystem schema** (component, `internal/meta`). Inode ID allocation by slot and sequence, inodes, directory entries, xattrs, sessions and locks, orphans, purge lists, slot byte counters and replay records ([Key layout](design.md#metadata-layer-metastore)).
- [ ] **MetaStore invariants** (test, `internal/sim`). At most one leader per shard per term; a committed entry is never lost; a shard recovered from manifest and log equals its state before the crash; at most one lock holder; a replayed request returns its first result and changes nothing.

## Phase 5: default drivers

- [ ] **Off-heap slabs** (component, `internal/loop`). Slabs mapped with `unix.Mmap`, page-aligned, handed out as slot and generation handles; the buffer pool and the memtable's page store on top ([Runtime in Go](design.md#runtime-in-go)). Review when a stale handle is caught and the Go heap stays flat while slabs fill.
- [ ] **Event loop** (component, `internal/loop`). One epoll loop goroutine per core over every driver's readiness fd, on raw non-blocking descriptors; executes actions, feeds events. An inbox of a lock-free queue and an eventfd, a timer heap on one timerfd, `GOMAXPROCS` above the number of loops, `RawSyscall` for non-blocking I/O, `writev` once per descriptor per pass, a read budget per descriptor per pass and capped replication queues ([Runtime in Go](design.md#runtime-in-go)). Every syscall the loop makes goes through a recording layer that emits a trace record. Review when the trace of a pass under 1,000 connections shows one `epoll_wait`, at most one `writev` per ready descriptor and no more than the read budget per descriptor; `runtime.NumGoroutine` is the same with 1 and 1,000 connections; `testing.AllocsPerRun` of a pass is 0; and with the [open-loop latency spike](design.md#appendix-open-loop-latency-spike)'s load generator no request is dropped up to the offered rate the spike's harness served ([Testing the design decisions](design.md#testing-the-design-decisions)).
- [ ] **Clock and timers** (component, `internal/loop`). Monotonic clock to `Tick`; `SetTimer` on the loop's timer heap and timerfd.
- [ ] **io_uring disk driver** (component, `internal/driver/uring`). Its own ring over raw syscalls, its completion eventfd in the loop's epoll set. File or raw device, `O_DIRECT`, vectored writes, a write and its `IORING_FSYNC_DATASYNC` linked in one submission, `durability: fua` as a setting, file-backed segments written in full at creation, discard for freed segments.
- [ ] **TCP network driver** (component, `internal/driver/tcp`). One connection per pair of event loops, opened on demand and chosen by the owner map ([Runtime in Go](design.md#runtime-in-go)), framing, messages batched per connection, `PeerLost` on any transport error, reconnect.
- [ ] **Round driver** (component, `internal/loop`). Collects every shard's `Ready` and the chunk writes once per round, bounded by size and time, and decides how disk logs map to cores. Measure commit latency p50 and p99 with 32,000 proposals in flight; this answers two [open questions](design.md#roadmap-and-open-questions).
- [ ] **Shared ChunkStore suite** (test). One suite, run on the simulated disk and on io_uring ([done-when checkpoint](design.md#learning-project)).
- [ ] **Shared network suite** (test). One suite, run on the simulated network and on TCP, ready for RDMA in phase 9.
- [ ] **Durable write benchmark** (test). 4K at queue depth 1 with idle gaps, against the [NVMe flush spike](design.md#appendix-nvme-flush-spike)'s floor of about 0.95 ms (2 ms after idling), cross-checked with fio. Report CPU per write and the recovery time of a full disk's segment headers.
- [ ] **Crash tests on real disks** (test). `kill -9` mid-round, and device-mapper error and delay targets where root is available; every acknowledged write survives.
