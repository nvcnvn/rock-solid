# Raft port spike (etcd-raft in sans-IO Rust)

**Question.** Can etcd-raft (Go, `go.etcd.io/raft`) be ported to Rust in our sans-IO shape (seeded random number generator (RNG), plain-struct messages instead of protobuf, `BTreeMap`/`BTreeSet` instead of hash maps, no threads, clocks or OS randomness) closely enough that its own test suite passes, at a per-group cost no worse than raft-rs? If so, [ADR 0006](../../docs/decisions/0006-own-raft-shard.md) becomes "port etcd-raft" instead of "write our own".

**Setup.**

- **Host and versions:** macOS 27.0 on an Apple M4 Pro, 14 cores, 64 GiB, rustc 1.99.1, Go 1.27.0.
  - etcd-raft **v3.7.0** (commit `b867cf13f6bc0dae21204302df97bc2355c3af55`, 2026-06-03), the latest release tag. [fetch.sh](fetch.sh) clones it into `vendor/raft`.
  - raft-rs **0.7.0** with `protobuf-codec`, for comparison. Its build needs a protoc 3.x; fetch.sh downloads protoc 21.12, which reports itself as 3.21.12.
  - The port depends only on rand_chacha 0.10.0 (`default-features = false`).
- **Shared host:** other agents ran on the machine. The 1-minute loadavg was 2.5–6 during the benchmarks, and each run header records it. Compare CPU numbers only within [results-p5-7.txt](results-p5-7.txt).
- **What was ported:** the whole library except `node.go` (the goroutine and channel wrapper) and the protobuf codec.
  - Go files: raft.go, rawnode.go, log.go, log_unstable.go, storage.go (MemoryStorage), read_only.go, status.go, util.go, bootstrap.go, types.go and logger.go, plus `tracker/`, `quorum/`, `confchange/` and the raftpb helpers. The raftpb messages and the types of node.go (Ready, SoftState, Peer) became plain Rust structs and enums.
  - The port lives in [eraft/](eraft/). Names, functions and file layout follow the Go code, in snake case, so upstream fixes can be followed. Every file carries an attribution header (Apache License 2.0).
  - Each deliberate deviation is listed under [Deviations](#deviations-from-upstream) and in the header of the file it affects.
- **Tests:** the Go test files were ported next to the code: `src/*_test.rs` and `eraft/tests/`.
  - The datadriven harness (`rafttest/interaction_env*.go`) is in [tests/rafttest](eraft/tests/rafttest/mod.rs).
  - A minimal port of `cockroachdb/datadriven` runs every file in rewrite mode and compares the rewritten file with the original byte for byte: [tests/datadriven](eraft/tests/datadriven/mod.rs).
- **Benchmark harness ([bench/](bench/)):** the single synchronous loop of [multi-raft's rr.rs](../multi-raft/src/rr.rs), made generic over a `Replica` trait ([harness.rs](bench/src/harness.rs)), so raft-rs ([rr.rs](bench/src/rr.rs)) and the port ([pr.rs](bench/src/pr.rs)) run in identical code.
  - Shape: 3 simulated nodes × 1,000 groups in one process, an in-memory network, MemoryStorage on each side, check_quorum and pre_vote on.
  - Each round the loop ticks every replica, delivers every queued message, persists every dirty group's Ready, does one simulated fsync (a 1 ms sleep) and advances.
  - One difference: when the port's `Advance` leaves more work (for example committed entries after the leader's self-ack), the replica goes back on the dirty list for the next pass. raft-rs hands that work back as a `LightReady` inside the same pass.
- **Measurement on macOS:**
  - CPU: `getrusage(RUSAGE_SELF)`; the loop runs on one thread.
  - Memory: live heap from a counting global allocator; RSS from `proc_pidinfo(PROC_PIDTASKINFO)`.
- **Stand-ins and limits:** the network, disks and fsync are simulated in memory, so these are protocol and CPU costs only. raft-rs draws election timeouts from its own `thread_rng`; the port uses its seeded ChaCha8.

```
./fetch.sh                 # etcd-raft v3.7.0 into vendor/raft, protoc into vendor/protoc
./run.sh                   # builds into ./target and writes every results-*.txt (about 6 min)
./run.sh p2 p5-7           # or selected tests: p1 p2 p3 p4 p5-7 p8 p9
cd eraft && cargo test --release                   # every ported test, P2 and P9
cargo test --release --test interaction -- ../vendor/raft/testdata   # P2 alone
cargo test --release --test sim -- 1000 1 10000                      # P9: seeds, first seed, ticks
B=target/release/raft-port-bench
$B raftrs|port --groups 1000 --hb 50 --elect 150-300 --idle 10
$B raftrs|port --groups 1000 --hb 100 --elect 1000-2000 --write 5 --inflight 32 --fsync-us 1000
$B sim-port --groups 30 --seed 11 [--rand-seed 99]
```

The expected outcomes were agreed with the user before any test ran:

- P1: the core is about 6–8k Go lines, and the Rust is similar.
- P2: at least 90% of the testdata files give byte-identical output.
- P3: all of raft_paper_test.go passes.
- P4: the same seed twice gives the same trace; a different seed gives a different trace.
- P5: live heap and RSS per group are at most raft-rs's on this host.
- P6: CPU per idle message is within ±30% of raft-rs.
- P7: write throughput is in the same order as raft-rs.
- P8: the library depends on nothing beyond rand_chacha and passes the sans-IO rules.
- P9: no safety violations in 1,000 seeds.

## Results: fidelity

| Test | Result |
| --- | --- |
| P1 size | **Go ported: 6,811 lines (3,908 non-blank, non-comment). Rust library: 6,173 lines (5,179 code)** ([results-p1.txt](results-p1.txt)). Rust has more code lines because rustfmt splits struct literals and format arguments; Go has more comment lines. The node.go types (about 100 of its 616 lines) and raft.proto's message definitions (205 lines) were ported as well. Left out: the `Node` goroutine wrapper in node.go, `raftpb/raft.pb.go` (1,233 lines of generated codec), `state_trace.go` (389 lines of TLA+ tracing behind the `with_tla` build tag) and doc.go. Rust tests and harnesses: 9,657 lines |
| P2 interaction tests | **28 of 28 testdata files byte-identical (100%, 6,724 lines)**, with no differences to explain ([results-p2.txt](results-p2.txt)). Negative control: two deliberately corrupted copies fail. The quorum (4 of 4) and confchange (9 of 9) datadriven files are byte-identical too ([results-p3.txt](results-p3.txt)). Cross-check in the other direction: a new 90-line scenario whose expected output the port wrote passes unchanged on upstream Go ([results-p9.txt](results-p9.txt), last line) |
| P3 paper tests | **raft_paper_test.go: 26 of 26 Go tests ported and passing.** Also ported and passing: raft_test.go 118/118, log_test.go 21/21, log_unstable_test.go 9/9, storage_test.go 8/8, tracker inflights + progress 12/12, rawnode_test.go 12/12, and quorum and confchange quick/restore tests on a seeded RNG. In total 206 unit tests plus the datadriven files. Two rawnode tests are partial ports because `readyWithoutAccept` merged into `Ready` (see deviations). One `test_slice` case (log_test.go:939) expects 3 entries where Go returns 2, from the entry-size deviation; the test asserts exactly that case. None of the ported tests needed a library change in raft.go's logic. Porting the tests found one Rust-only bug: `Progress::maybe_decr_to` computed `next - 1` with `next = 0` and panicked in debug builds, where Go's uint64 wraps (tracker/progress.go:245); it now uses `wrapping_sub`. Release builds already wrapped, so no result changed |
| P4 determinism | 30 groups × 3 nodes in virtual time, with a seeded message shuffle, 10% drops, and node 1 stopping at tick 150. **10 separate processes with seed 11 give 1 trace; 10 with seed 12 give 1 trace; the two traces differ.** P4b: the same network seed with a different election `rand_seed` gives a different trace, so the injected RNG really drives elections ([results-p4.txt](results-p4.txt)) |
| P8 dependencies | Runtime dependencies: rand_chacha 0.10.0, rand_core 0.10.1, ppv-lite86 0.2.21 and zerocopy 0.8.63. zerocopy-derive, syn, quote and proc-macro2 appear only as build-time proc macros. **The `scripts/lint.sh` I/O-crate check finds none, cargo-deny 0.20.2 with the repo's deny.toml passes (advisories, bans, licenses, sources), and clippy with the repo's root clippy.toml at `-D warnings` is clean: no HashMap/HashSet, Instant/SystemTime, threads, Mutex or getrandom, and no `unsafe`** ([results-p8.txt](results-p8.txt)). The repo's full pedantic lint set reports about 110 cosmetic warnings (41 missing doc backticks, 20 casts, 10 module docs) that would need fixing before the code moves into `crates/`. The test targets use `std::fs` and `process::exit`, which the sans-IO clippy.toml bans; that is acceptable for a spike harness but not inside `crates/` |

## Results: per-group cost (1,000 groups × 3 replicas, one thread)

Each cell shows two runs, alternating raft-rs and the port ([results-p5-7.txt](results-p5-7.txt)).

| Test | raft-rs 0.7.0 | Port (eraft) | Port vs raft-rs |
| --- | --- | --- | --- |
| P5 live heap per group, after election + 2 s and after 10 s idle | 8.7 / 8.7 KiB | **7.7 / 7.7 KiB** | −11% |
| P5 RSS per group | 14.1–16.0 KiB | 14.2–15.2 KiB | about equal; macOS RSS deltas are noisy |
| P5 struct size | `RawNode` 824 B, `Message` 192 B, `Entry` 104 B | `RawNode` 1,048 B, `Message` 152 B, `Entry` 48 B | |
| P6 idle, default timing (50 ms heartbeat, 150–300 ms election) | 14.2% / 13.4% of a core, 80,000 msgs/s, 1.78 / 1.69 µs per message | **10.8% / 10.3%**, 80,000 msgs/s, **1.35 / 1.29 µs per message** | −22% to −24% |
| P6 idle, tuned timing (100 ms / 1–2 s) | 7.8% / 8.1%, 40,000 msgs/s, 1.96 / 2.00 µs per message | **5.4% / 5.4%**, 40,000 msgs/s, **1.35 / 1.34 µs per message** | −31% to −33% |
| P7 writes, 100 B, 32 in flight per group, 1 ms fsync per round | 675,000 / 695,000 ops/s, 1.39 / 1.35 µs CPU per op | **607,000 / 641,000 ops/s, 1.55 / 1.47 µs per op** | −8% to −10% ops/s, +9% to +11% CPU per op |
| P7 writes, no fsync | 742,000 / 745,000 ops/s, 1.35 / 1.34 µs per op | 646,000 / 688,000 ops/s, 1.55 / 1.45 µs per op | −8% to −13% |
| P7 batching (both) | 16 entries persisted per Ready; one fsync covers 3,000 Readys and 48,000 entries | identical | |

- Message counts per heartbeat interval are identical: 4.0 per group in both, so the CPU comparison is per equal work.
- Neither CPU gap was profiled. The port clones entries into each Ready, and in synchronous mode it clones the committed entries once more for its self-addressed `MsgStorageApplyResp` (rawnode.go `acceptReady`). Those clones are candidates for the write gap, not established causes.

## Results: break attempt

| Test | Result |
| --- | --- |
| P9 safety, 1,000 seeds × 3 and 5 nodes, 10,000 ticks each | **0 violations of election safety, log matching, leader completeness, committed-entry stability or state machine safety** ([results-p9.txt](results-p9.txt)). The faults: 5% drops, 2% duplicates, 0–3 tick delays with reordering, random partitions for 20–100 ticks, and crash and restart from the persisted MemoryStorage (unstable entries lost). Also exercised: compaction with snapshot transfer, proposals, and simple and joint membership changes over voters and learners (2 spare node IDs). Per seed the RNG varies check_quorum, pre_vote, heartbeat (1–2 ticks), message size (64 B to unlimited), inflight limit (2–256) and step-down-on-removal; 29% of seeds use AsyncStorageWrites. Over 3-node runs: 1.62 M committed entries, 63,552 terms with a leader, 180,695 crashes, 54,780 snapshots applied, 452,782 conf-change applications |
| P9 negative control | The same runs with a disk that forgets its vote on crash: the checker finds two leaders in one term in 10 of 1,000 seeds (3 nodes) and 6 of 1,000 (5 nodes). The checker does catch real safety bugs |
| **P9 finding: with AsyncStorageWrites a snapshot can overtake queued conf changes** (seed 237, 3 nodes) | **Upstream behaviour, not a port bug. Go etcd-raft v3.7.0 panics at the same directive with the same message.** A follower steps `MsgSnap` and restores the snapshot, switching its configuration at once (raft.go:1860 `restore`). Restore only checks that the snapshot index is above `committed`, not that the entries handed to the apply thread (`applying`) have been applied. When the stalled apply thread then passes an older committed conf change to `ApplyConfChange`, it applies against the snapshot's configuration: here `LeaveJoint` on a non-joint config, which panics (confchange.go:100, raft.go:1967). A stale add or remove would instead change that node's configuration silently. The `RawNode` API does not report the restore until the next Ready. The regression case is [regress/async_snapshot_overtakes_apply.txt](regress/async_snapshot_overtakes_apply.txt), run on both implementations by `run.sh p9`. The simulator's application now skips apply-thread entries covered by a pending or installed snapshot; it can see the pending snapshot only because the port exposes `raft_log.unstable` (2,000 entries skipped per 1,000 seeds) |
| P9 finding: compaction past the durable commit index | Also an application contract, found while building the simulator. With AsyncStorageWrites the apply thread can run ahead of the `HardState` write. A snapshot taken at the applied index can then sit above `HardState.commit` on disk, and the next `newRaft` panics: `committed` starts at the snapshot index (log.go:93), and `loadState` rejects a lower `HardState.commit` (raft.go:2038-2039). Found by reading the source; not reproduced in Go. The simulator compacts only up to the durable commit |

## Deviations from upstream

1. **Messages are plain structs.** An unset proto2 field is a zero value, `Message.snapshot` is `Option<Box<Snapshot>>`, and `ConfChangeI` is an enum. `ConfChange` and `ConfChangeV2`, the only messages raft encodes into entries, use a hand-written, wire-compatible codec of 190 lines ([codec.rs](eraft/src/raftpb/codec.rs)).
2. **Entry size (`proto.Size`)** counts zero fields as unset. Go also counts a field explicitly set to zero, so a rejected conf change replaced by an explicitly typed `EntryNormal` entry, or an empty non-nil payload, is 2 bytes smaller here. That changes one `test_slice` case and no testdata.
3. **Election RNG.** Go draws timeouts from a process-wide `crypto/rand`. Each port raft uses ChaCha8, seeded by `Config::rand_seed`, on stream `id`. It stores only (seed, stream, draw count), 24 bytes, and replays the stream position on each draw.
4. **Storage** is a type parameter, `RawNode::new(&mut Config, S)`, instead of a `Config` field. `RawNode` owns it, and `store()`/`store_mut()` replace the shared pointer. MemoryStorage has no mutex and no call statistics.
5. **Logger:** one `log(level, fmt::Arguments)` method plus `enabled(level)` replace Go's twelve methods, and `SetLogger` is gone. The default logger discards everything; Go's writes to stderr.
6. **`Step` takes the message by value.** `MsgProp` entries move into the log (Go clones them), and `Ready` moves messages and read states out of raft instead of sharing slices. `readyWithoutAccept` and `acceptReady` became one function, so tests that call `readyWithoutAccept` were adapted.
7. **Containers:** `BTreeMap`/`BTreeSet` replace maps. Go keeps learner sets and the outgoing voters nil whenever they are empty (`nilAwareAdd`/`nilAwareDelete`), so an empty set stands for nil. `Visit` walks the `BTreeMap` in ID order, as Go sorts the keys. `unstable.stableTo` drains the front of a `Vec` and shrinks it when less than half is used; Go reslices and copies under the same condition.
8. Not ported: the TLA+ trace hooks (`state_trace.go`). `readOnly.recvAck` pads a context shorter than 8 bytes, where Go's `binary.LittleEndian.Uint64` panics. `Changer` borrows the tracker, and its errors are strings.
9. Go's uint64 arithmetic wraps. Rust release builds wrap the same way, debug builds panic. The test suite and 200 simulator seeds pass in debug builds after the `maybe_decr_to` fix.

## Conclusions

1. **Port etcd-raft instead of writing a Raft from scratch.**
   - The ported library passes every upstream test that was ported (P2, P3). That is 100% of the interaction testdata byte for byte, and 206 unit tests including all of raft_test.go and raft_paper_test.go.
   - It survived 2,000 adversarial seeds without a safety violation (P9).
   - It took about the same number of lines as the Go (P1).
   - The deviations needed for the sans-IO shape are small and local (the deviations list above).
2. **The port costs no more per group than raft-rs.** It uses 11% less live heap and the same RSS (P5), and 22–33% less CPU per idle message (P6). Its write throughput is 8–13% lower at 9–11% more CPU per op (P7), which is the same order. Profile the write path before tuning; entry clones in `Ready` are the first suspects.
3. **It meets the sans-IO rules as is** (P8, P4): no I/O crates, cargo-deny passes (raft-rs fails it), clean under the root clippy.toml, no `unsafe`, and one seed replays a run exactly. The pedantic lint set needs about 110 cosmetic fixes before the code moves into `crates/`.
4. **Keep the Go file layout and names so upstream fixes can be followed.** Track the pinned tag (v3.7.0, `b867cf1`) and port its testdata on every update; the datadriven files are the cheapest fidelity check.
5. **Use synchronous `Ready`/`Advance` in the shard first.** With AsyncStorageWrites the application must drop apply-thread entries that a restored snapshot covers, and must not compact past the durable `HardState.commit`. Upstream does not enforce either rule (P9 findings). If the shard adopts async writes, it needs both guards and the regression scenario in its tests.
6. **Run the P9 simulator under the repo's simulator before trusting changes.** The negative control shows it catches a forgotten vote within 1,000 seeds.

## New open questions

- Where does the 8–13% write-path gap come from (entry clones, `BTreeMap` lookups, the extra Ready pass)? Profile before deciding whether to deviate further from upstream.
- Should the shard expose "snapshot restored" to the application, so async apply can drop covered entries without reaching into `raft_log.unstable`? Report the async-apply hazard upstream?
- Small-N maps: a `BTreeMap` leaf allocates 11 slots of `Progress` (104 B each) for a 3-member group. A sorted `Vec` would cut the per-group heap further, at the cost of a deviation.

## Not tested

- Real disks, real fsync and a real network; latency percentiles.
- raft-rs under the P9 simulator.
- `Node` (goroutines) and its tests (node_test.go, rafttest/node*.go), the raft_flow_control_test.go and raft_snap_test.go suites, and the Go benchmarks.
