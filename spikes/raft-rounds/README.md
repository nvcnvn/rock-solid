# Raft rounds spike (etcd-raft round driver in Go)

**Question.** A MetaStore node's event loop collects every shard's `Ready` once per round and makes the round durable with one flush. What commit latency (p50 and p99) does that driver give at 1,000 groups per node with 32,000 proposals in flight, and must rounds be bounded by size, by time, or split across per-core loops? And what does etcd-raft cost in Go at that scale (heap per group, collection CPU, allocations per message), and do `raftpb` messages dominate, so that the driver should recycle them or encode straight from the shard's entries? This answers two open questions in [docs/design.md](../../docs/design.md#roadmap-and-open-questions) and the build-checklist items "Raft cost per group" and "Round driver". The [multi-raft spike](../multi-raft/) measured the Rust libraries; its naive loop had rounds of about 100 ms.

**Setup.**

- **Machine:** Linux 6.18.44 in a Firecracker VM, 4 vCPUs (Intel Xeon, 2.1 GHz), Go 1.27.0, **shared with six other agents' benchmarks** (loadavg 8 to 9). Written against rock-solid commit `676095bf56fdcfc9a0afce42ab45103137501d7f`.
- **Libraries:** `go.etcd.io/raft/v3` **v3.7.0** (the latest release and the design's pin) and **v3.6.0** for comparison; `bin/rounds36` is the same driver built against v3.6.0 (`build.sh`, `pb36.go`). v3.7.0 moved `raftpb` from gogoproto value structs to `google.golang.org/protobuf` with a pointer per scalar field: `Entry` ([raft.pb.go:343](https://github.com/etcd-io/raft/blob/v3.7.0/raftpb/raft.pb.go#L343)), `Message` with `Entries []*Entry` ([raft.pb.go:523](https://github.com/etcd-io/raft/blob/v3.7.0/raftpb/raft.pb.go#L523)), `Ready.Entries []*pb.Entry`, `Storage.Entries` returning `[]*pb.Entry` ([storage.go:77](https://github.com/etcd-io/raft/blob/v3.7.0/storage.go#L77)), and `raft.hardState()` building a `*HardState` from three `new(…)` on every `HasReady` and `Ready` ([raft.go:504](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L504), [rawnode.go:454](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L454)).
- **Cluster:** one process, 3 nodes × 1,000 groups (3,000 `RawNode`s with `MemoryStorage`), leaders spread 334/333/333. `CheckQuorum` and `PreVote` on, a 10 ms tick, heartbeat 10 ticks, election 100 ticks, `MaxSizePerMsg` 1 MiB, synchronous `Ready`/`Advance`. Storage compacts to 4 entries behind applied every 16 entries.
- **Driver (`main.go`):** each node runs 1, 2 or 4 loops; a group's replicas live on loop `g mod loops`, and each loop has its own flush stream (`commit_depth` 1). A pass ticks if due, decodes and `Step`s arrived message batches, queues arrived proposals, completes the round whose flush finished (sends the held messages, `Advance`), and, when no flush is in flight, starts a round: for every replica from a round-robin cursor, a leader with queued proposals proposes **one entry holding all of them** (the design's group commit; `-batch=false` proposes one entry per proposal, the multi-raft spike's shape), then `HasReady`/`Ready`, persist to `MemoryStorage`, apply committed entries (they are durable on a quorum), send `MsgApp`/heartbeats at once and hold `MsgAppResp`/`MsgVoteResp`/`MsgPreVoteResp` until the flush ([raft.go:546](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L546)). Entry bytes are copied into a record buffer. A round stops early at `-max-bytes` (`max_group_commit_bytes`, checked after each `Ready`, so it may overshoot by one) or `-max-cpu-us` of pass CPU (the time bound).
- **Virtual time with measured CPU:** the whole cluster runs on one locked OS thread as a discrete-event simulation. Each pass advances its loop's clock by the **thread CPU time it took** (`CLOCK_THREAD_CPUTIME_ID`, 0.5 µs per read, included), so other agents' load shifts CPU per operation by cache and hyperthread effects (about ±30% between runs of one configuration) but not by waiting for a CPU. Flush is a fixed 0.95 ms (the [NVMe flush spike](../nvme-flush/)'s consumer drive) or 0.1 ms (a drive with power-loss protection), or `-disk nvme`: the measured N3 curve (0.95 ms to 64 KiB, 1.9 ms at 256 KiB, 2.3 ms at 1 MiB, 4.3 ms at 4 MiB). Messages are encoded per destination loop into one batch per pass and arrive 50 µs later; `-wire=false` passes them in memory.
- **Load:** proposals of 128 B. Closed loop: 32 in flight per group, 32,000 in all, each completion re-proposed at once. Open loop: Poisson arrivals at a fixed cluster-wide rate over uniformly chosen groups. Latency runs from arrival to the leader applying it; 0.5 s of warm-up, 1.5 s measured.
- **Costs:** allocations from `runtime/metrics` over the measured window, per completed proposal and per message sent; allocation sites from a `pprof` allocs profile diffed against one taken at the window's start; live heap after two forced collections, idle (after elections plus 1 s) and loaded (after the run), per replica (a node holds one replica of each group). Collection CPU is the process CPU (`getrusage`) minus the loop thread's CPU, plus the runtime's assist estimate, with `GOGC` at 100.

```
./run.sh > results.txt 2>&1                 # builds, runs R, O and A (about 30 min on the shared VM)
./profile.sh > results-cpuprofile.txt 2>&1  # CPU profiles P1, P2
go test -bench ThreadCPUClock .             # 497 ns per CPU-clock read
./bin/rounds -mode open -rate 60000 -loops 4 [-flush-us 100] [-max-bytes 1048576] [-max-cpu-us 1000] [-disk nvme] [-batch=false] [-wire=false]
```

Expected before running: rounds bounded at 1 MiB cut p99 at 32,000 in flight; per-core loops cut it further; `raftpb` messages are the largest allocation site; under 20 KiB of heap per group.

## Results: closed loop, 32,000 proposals in flight (v3.7.0)

One loop per node unless noted. Round sizes are per loop.

| Test | Throughput | p50 / p99 | Rounds |
| --- | --- | --- | --- |
| R1 unbounded, flush 0.95 ms | 1.98 M/s | 15.2 / 28.2 ms | 188/s per loop, 1.3 MiB mean, 4.0 MiB max; loop 94% busy; 1.42 µs CPU per proposal |
| R2 `max_group_commit_bytes` 1 MiB | 1.98 M/s | 14.9 / 30.0 ms | 937 KiB mean |
| R3 256 KiB | **1.31 M/s** | 23.4 / 38.1 ms | Loop 63% busy: rounds wait on flushes |
| R4 collection CPU bounded at 1 ms per pass | 1.88 M/s | 16.0 / 29.7 ms | 484 KiB mean |
| R5 2 loops per node | 3.82 M/s | 7.7 / 20.1 ms | |
| R6 4 loops per node | **6.00 M/s** | **5.1 / 7.6 ms** | 339 KiB mean, 1.0 MiB max |
| R7 4 loops, 1 MiB | 5.48 M/s | 5.5 / 10.1 ms | |
| R8 unbounded, flush cost by size (N3 curve) | 1.76 M/s | 16.7 / 31.0 ms | |
| R9 1 MiB, flush cost by size | 1.50 M/s | 19.3 / 39.0 ms | |
| R10 **one entry per proposal** (multi-raft spike's shape) | 164 k/s | **178 / 308 ms** | 16.8 µs CPU per proposal |
| R11 one entry per proposal, 1 MiB | 175 k/s | 166 / 251 ms | |
| R12 flush 0.1 ms, unbounded | 1.95 M/s | 14.9 / 33.8 ms | |
| R13 flush 0.1 ms, 1 MiB | 1.72 M/s | 15.7 / 51.6 ms | |
| R14 flush 0.1 ms, 4 loops | 7.30 M/s | 4.0 / 10.7 ms | |

At a fixed 32,000 in flight, every row obeys Little's law: p50 ≈ 32,000 ÷ throughput. A bound cannot lower latency there; only throughput can.

## Results: open loop (v3.7.0)

p50 / p99 in ms, and the busiest loop's share of its core.

| Rate (cluster) | 1 loop | 1 loop, 256 KiB | 2 loops | 4 loops |
| --- | --- | --- | --- | --- |
| O flush 0.95 ms, 30,000/s | 4.6 / 10.6 (53%) | 6.0 / 32.8 (68%) | 3.2 / 5.4 (27%) | **2.9 / 4.6** (15%) |
| 60,000/s | 11.1 / 51.1 (90%) | 12.2 / 53.3 (92%) | 4.6 / 10.5 (54%) | **3.1 / 5.0** (26%) |
| 100,000/s | 34.6 / 84.1 (99%) | 23.4 / 77.1 (98%) | 4.9 / 7.9 (56%) | **3.1 / 4.8** (29%) |
| 130,000/s | 27.3 / 47.9 (99%) | 18.5 / 34.6 (98%) | 5.7 / 25.8 (68%) | **3.6 / 5.8** (41%) |
| O flush 0.1 ms, 30,000/s | 1.17 / 10.7 (79%) | 1.14 / 3.2 (78%) | 0.74 / 1.8 (56%) | **0.53 / 0.93** (38%) |
| 60,000/s | 1.84 / 7.8 (95%) | 3.1 / 30.7 (97%) | 0.83 / 1.9 (67%) | **0.58 / 0.98** (42%) |
| 100,000/s | 12.0 / 27.6 (99%) | 12.2 / 34.6 (99%) | 0.97 / 2.6 (79%) | **0.64 / 1.2** (52%) |
| 130,000/s | 21.5 / 37.3 (100%) | 22.4 / 44.4 (100%) | 1.37 / 11.1 (92%) | **0.66 / 3.6** (58%) |

| Test | Result |
| --- | --- |
| O round sizes | Unbounded rounds at 0.95 ms average 5 to 125 KiB and peak at 574 KiB; at 0.1 ms, 0 to 94 KiB. **A 1 MiB bound never triggers under open load** |
| O CPU per proposal (whole cluster, 3 replicas) | 23 to 53 µs at 0.95 ms flush, 8.4 messages per proposal. One loop per node saturates at about 60,000 to 100,000 proposals/s; run-to-run spread at 60,000/s on one loop was p99 10.7 to 51 ms as CPU per proposal moved 37 to 45 µs with the other agents' load |
| P1 CPU profile, 0.1 ms flush, 4 loops, 30,000/s | **`HasReady` takes 54% of loop CPU**, the round's scan of every group on the loop at 6,200 rounds/s per loop; CPU per proposal 142 µs, against 79 µs on one loop. Inside it, comparing `HardState` and `SoftState` through pointers (`isHardStateEqual` 39%, `SoftState.equal` 24%) and building the `*HardState` (19%) ([results-cpuprofile.txt](results-cpuprofile.txt)) |
| P2 CPU profile, 0.95 ms, 1 loop, 60,000/s | `collect` 37%, decode and `Step` of arrived messages 31%, background mark workers 16% of process CPU |

## Results: etcd-raft's cost (allocations, heap, collection)

| Test | v3.7.0 | v3.6.0 |
| --- | --- | --- |
| A1 open 60,000/s, encoded messages: allocations per proposal | **136.5 (8,049 B)** | 22.6 (3,790 B) |
| A1 allocations per message | **16.2** | 2.7 |
| A1 loop CPU per proposal | 43.8 µs | 26.7 µs (1.64× less) |
| A1 collection CPU, share of process CPU | **22.0%** (43 cycles in 1.5 s) | 13.8% (20 cycles) |
| A2 same, messages passed in memory without encoding | 97.5 per proposal, 11.5 per message, 30.2 µs | 18.8, 2.2, 22.2 µs |
| A3 closed 32,000 in flight, encoded | 4.8 per proposal, 16.1 per message; 1.58 M/s at 1.80 µs; collection 8.3% | 0.7, 2.7; 2.72 M/s at 1.01 µs; 8.1% |
| A heap per group (one replica per node) | **Idle 2.8 KiB** (2.7 MiB per node for 1,000 groups); 6.8 KiB at 60,000/s; 64 KiB at 32,000 in flight, mostly retained entries | Idle 2.8 KiB; 6.1 KiB; 68 KiB |
| A idle CPU, 1,000 groups per node, 10 ms tick, 100 ms heartbeat | 3.5 to 8% of a core per node across all runs, more with more loops per node (each pass reads the CPU clock) | 2.2 to 4.1% |
| A allocation sites, v3.7.0, open (objects) | **Protobuf pointer fields dominate:** decoding (`mDecode` cum. 30%, of it `consumeUint64Ptr` 16%), `raft.send` 14% (`new(r.id)`, `new(r.Term)`), `raft.hardState` 11% (every `HasReady`), `maybeSendAppend` 8%, `newStorageAppendRespMsg` 7%, `MessageType.Enum` 5%, `newStorageApplyRespMsg` 5%, `Propose` 2.5%. The driver's own buffers: under 1% | |
| A allocation sites, v3.6.0, open | | `raft.send` 41% (growing `r.msgs`), `readyWithoutAccept` 18%, `unstable.truncateAndAppend` 12%, `Message`/`Entry.Unmarshal` 17%, the proposal's entry slice and data 8% |

## Conclusions

1. **Batching each group's proposals into one entry per round is what keeps rounds short.** One entry per proposal reproduced the multi-raft spike's loop: 164,000 proposals/s and p50 178 ms at 32,000 in flight (R10). One entry per group per round gave 1.6 to 2.0 million/s on one loop per node, rounds of 5 ms and p50 15 ms (R1), because per-proposal Raft work becomes per-group work. At a fixed number in flight latency is in-flight ÷ throughput, which no bound changes.
2. **Bound rounds by size only, at `max_group_commit_bytes` (1 MiB); no time or CPU bound.** Under open load a 1 MiB bound never triggers (rounds of at most 574 KiB). Under closed overload it keeps the flush cost of 4 MiB rounds off the drive but costs 0 to 15% throughput (R2, R9, R13), and 256 KiB cost a third (R3), since a round left behind the cursor waits a whole flush. A CPU bound of 1 ms per pass changed nothing (R4). The 256 KiB bound on one loop sometimes lowered p50 near saturation and sometimes tripled p99 below it (O).
3. **Latency is set by the loop's utilization, so a node runs one loop per core and keeps each under about 50% busy.** A commit takes about three rounds: the leader's append, the follower's append and flush, and the leader's apply each wait for their loop's round in flight. Below half busy that is p50 2.9 to 3.6 ms and p99 4.6 to 5.8 ms with a 0.95 ms flush, and 0.53 to 0.66 ms and 0.9 to 1.2 ms with 0.1 ms (O, 4 loops). One loop holding 1,000 groups reaches 90% busy at 60,000 proposals/s and its p99 rises to 10 to 50 ms.
4. **Collect from the groups that changed, never by scanning all of them.** Calling `HasReady` on every group of the loop each round cost 54% of the loop's CPU when 0.1 ms flushes made rounds frequent (P1). The driver marks a group when it steps a message, proposes or ticks it, and collects only marked groups.
5. **On v3.7.0, `raftpb` dominates allocations, and most of it is inside etcd-raft, so the driver cannot recycle it away.** v3.7.0 makes 16.2 allocations per message and 136.5 per proposal against v3.6.0's 2.7 and 22.6, takes 1.64× the CPU per proposal, and spends 22% of process CPU in collection against 14% (A1). Decoding is only 30% of the objects; the rest comes from `new(…)` for pointer fields inside `raft.send`, `hardState` and `maybeSendAppend`. Recycling decoded messages is also unsafe: a follower's unstable log keeps the `*Entry` pointers of the `MsgApp` it stepped. With v3.6.0's value types, `Storage.Entries` can return `pb.Entry` values whose `Data` slices point into an off-heap slab; with v3.7.0 every entry read allocates a `*pb.Entry` and its fields.
6. **etcd-raft is cheap per group.** 2.8 KiB of heap per idle group (2.7 MiB per node for 1,000), 2 to 8% of a core per node idle with a 10 ms tick, and at load the live heap per group is set by the entries it retains, which the design moves off-heap.

## Not tested

- A real disk, real sockets and a real event loop: flushes and network are simulated, and all loops share one OS thread's caches.
- Several flush streams on one drive: each loop got its own stream at full speed. The NVMe flush spike saw two streams overlap at unchanged latency and four add latency.
- `commit_depth` 2, asynchronous storage writes, leader leases, snapshots and membership changes.
- The design's own entry storage; `MemoryStorage` stands in, with its locks and copies.
- A state machine: applying a proposal costs nothing here.
- Clean CPU numbers: the VM was shared, so CPU per operation moves about ±30% between runs of one configuration.
