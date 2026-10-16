# Multi-Raft spike (openraft vs raft-rs)

**Question.** The MetaStore runs about 1,000 Raft groups on each node. Is `openraft` mature and cheap enough for that, measured by memory per group and by CPU when the groups sit idle and only tick and heartbeat? Or should the MetaStore use another Raft library: `raft` (tikv/raft-rs), whose `Ready` API is close to our sans-IO core, or our own implementation? This answers the open question in [docs/design.md](../../docs/design.md#roadmap-and-open-questions) about `openraft` maturity for multi-Raft.

**Setup.**

- **Host and versions:** Linux 6.17, 14 cores, rustc 1.99.1.
  - openraft **0.9.25**, the latest stable release (2026-07-28). The latest pre-release is 0.10.0-alpha.37; it was read but not benchmarked.
  - raft (raft-rs) **0.7.0**, the latest crates.io release (2023-03-07), with `protobuf-codec`. `prost-codec` needs `protoc`.
  - tokio 1.47.1.
- **Shared host:** other agents were compiling and benchmarking at the same time. The 1-minute loadavg was between 11 and 19 during the runs, and every run header in [results.txt](results.txt) records it. Treat CPU numbers as ±30% and compare them only within this file.
- **Stand-ins:** one process holds 3 simulated nodes × 1,000 groups, so **3,000 replicas**. A real node hosting 1,000 groups holds 1,000 replicas, which is why the tables also give "per node" (total ÷ 3).
  - The network is in memory. raft-rs uses a `VecDeque` of messages. In openraft each RPC calls the target `Raft` handle directly.
  - Logs and state machines live in memory, with no compaction and no snapshots.
  - "Killing" node 1 means dropping every message to or from it.
  - fsync is simulated with a 1 ms sleep.
- **raft-rs harness ([src/rr.rs](src/rr.rs)):** one synchronous loop owns time, the network and storage. Each round it ticks every replica, delivers all queued messages, then collects every group's `Ready`, persists all of them, does one (simulated) fsync, and advances.
  - Settings: `check_quorum` and `pre_vote` on.
  - One tick equals one heartbeat unless `--tick` is given.
- **openraft harness ([src/or.rs](src/or.rs)):** each replica is a `Raft` with its own tasks on one tokio runtime: current-thread (`threads=1`) or 4 worker threads.
  - Storage implements the v2 `RaftLogStorage` and `RaftStateMachine` traits.
  - With `--fsync-us`, all 3,000 log stores send their `LogFlushed` callbacks to one shared WAL task, which fsyncs once for everything queued.
- **Measurement:**
  - Memory: RSS from `/proc/self/status`, plus live heap from a counting global allocator.
  - CPU: summed from `/proc/self/task/*/schedstat`.
  - tokio tasks: `RuntimeMetrics::num_alive_tasks`.
- **Timing presets:** "default" is openraft's default of a 50 ms heartbeat and a 150–300 ms election timeout. "Tuned" is a 100 ms heartbeat and a 1–2 s election timeout.

```
./run.sh > results.txt 2>&1      # builds into ./target, runs everything below (about 10 min)
B=target/release/multi-raft-spike
$B raftrs   --groups 1000 --hb 50 --elect 150-300 --idle 10 [--tick 10] [--quiesce]
$B openraft --groups 1000 --hb 50 --elect 150-300 --idle 10 --threads 1 [--quiesce]
$B raftrs   --groups 1000 --hb 100 --elect 1000-2000 --write 5 --inflight 32 [--fsync-us 1000]
$B openraft --groups 1000 --hb 100 --elect 1000-2000 --write 5 --inflight 32 --threads 4 [--fsync-us 1000]
$B raftrs-sim   --groups 30 --variant plain|seeded --seed 11
$B openraft-sim --groups 30 --variant plain|seeded --seed 11
```

Expected outcomes were written before the first run.

- M1: under 100 KiB per group for both.
- M2: under 5% of a core per node at the tuned settings; about 11 tokio tasks per openraft group.
- M3: re-election after a node loss within the maximum election timeout plus two heartbeats.
- M4: above 100k ops/s on one core.
- M7: both libraries replay exactly only once their randomness is seeded.

## Results: memory and idle cost (1,000 groups × 3 replicas)

| Test | Result |
| --- | --- |
| M1 raft-rs RSS | 13.0 KiB per group (about 4.3 KiB per replica), live heap 8.6 KiB. Flat after 10 s idle at 1,000 groups and after 120 s at 100 groups |
| M1 openraft RSS | 147–170 KiB per group after election, 249–298 KiB after 10 s idle. **At 100 groups it grows to 1,166 KiB after 120 s idle.** Live heap is **1,961–2,011 KiB per group**, of which 1,824 KiB is one 608 KiB allocation per replica |
| M1 cause | `Engine::new` preallocates `EngineOutput::new(4096)`, a `VecDeque` of 4,096 commands ([engine_impl.rs:110](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/engine/engine_impl.rs#L110)). The pages are touched only as the ring's head moves through them, so RSS creeps toward the full allocation (inferred from the growth; not traced). **Upper bound: about 670 KiB per replica, so about 650 MB on a node with 1,000 replicas.** 0.10.0-alpha.37 still uses 4096 (`engine_impl.rs:124`) |
| M2 tasks | openraft: 3 tokio tasks per replica (core, state-machine worker, tick) plus 1 replication task per follower on the leader. That is **11 per group, 11,000 tasks** for 1,000 groups. raft-rs: none, since the caller owns the loop |
| M2 idle, default timing (50 ms / 150–300 ms) | raft-rs: 35.8% of one core (11.9% per node), 80,000 msgs/s, about 4.5 µs CPU per message. **openraft, 1 thread: 94.0% (31.3% per node)** for only 44,800 msgs/s, about 21 µs per message: a single core is nearly saturated just keeping 3,000 idle replicas alive. openraft, 4 threads: 111.5%. openraft heartbeats every 75 ms, not 50 ms: its tick task runs every 1.5 × `heartbeat_interval` ([raft/mod.rs:249](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/raft/mod.rs#L249)) |
| M2 idle, tuned (100 ms / 1–2 s) | raft-rs: 13.0% (4.3% per node), 40,000 msgs/s. openraft: 46.5% (15.5% per node), 26,400 msgs/s. **Even tuned, openraft costs about 3.5× raft-rs in CPU, and about 5× per message** |
| M2 node-pair batching (raft-rs) | 80,000 Raft msgs/s fit in 240 node-pair batches/s (6 directed pairs × 40 rounds), because the driver sees every outgoing message of every group in a round |
| M5 quiescence (default timing) | raft-rs, driver stops ticking: **0.07%** of one core, 0 msgs. openraft `runtime_config().tick(false)` on every replica: 0 msgs but still 4.5%, because 3,000 tick tasks keep waking every 75 ms |

## Results: elections

| Test | Result |
| --- | --- |
| M3a bootstrap, 1,000 groups (one designated node per group campaigns or initializes, spread evenly) | raft-rs: 74–106 ms default, 89 ms tuned. openraft: 164–231 ms default, 172 ms tuned. Leaders end up 334/333/333 across the nodes |
| M3b default timing: node 1 stops, its 334 groups re-elect | raft-rs, 50 ms tick: 1,467 ms; 10 ms tick: 727 ms. openraft: 774 ms (1 thread), 645 ms (4 threads). **All are above the expected ≤ 400 ms** |
| M3b tuned | raft-rs, 100 ms tick: 3,124 ms; 10 ms tick: 2,777 ms. openraft: 3,995 ms. All above the expected ≤ 2,200 ms |
| M3b cause (raft-rs) | With the tick equal to the heartbeat, the randomized election timeout has only 3 distinct values (3–5 ticks, [raft.rs:2810](https://github.com/tikv/raft-rs/blob/v0.7.0/src/raft.rs#L2810)). So two followers often time out on the same tick, grant each other's pre-votes and split the real vote. At 1,000 groups a 10 ms tick halved it (1,467 → 727 ms); in a trial run at 100 groups before `run.sh` (not in results.txt) it went from 998 ms to 328 ms. With the tuned timing a 10 ms tick helped less (3,124 → 2,777 ms), so split votes are not the whole cause there; not investigated further. **The slowest of hundreds of groups decides failover time** |

## Results: writes and group commit

| Test | Result |
| --- | --- |
| M4, 100 B proposals, 4 in flight per group (4,000 total), tuned | raft-rs: 75,700 ops/s, 13.2 µs CPU per op. openraft, 1 thread: 25,800 ops/s, 38.6 µs per op |
| M4, 32 in flight per group (32,000 total) | raft-rs: 117,800 ops/s, 8.5 µs per op, one thread. openraft: 102,500 ops/s at 9.75 µs per op on 1 thread; 278,000 ops/s at 12.7 µs per op on 4 threads (354% CPU) |
| M4 batching | raft-rs puts every proposal since the last `Ready` into one append (16 entries persisted per `Ready` on average at 32 in flight). openraft sends 15 entries per AppendEntries RPC, but its storage gets **only 2.66 entries per `RaftLogStorage::append`**: the leader calls `append` once per `client_write` (`vec![entry]`, [raft_core.rs:497](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs#L497)), and followers append once per RPC. 0.10.0-alpha.37 batches client writes (`RaftMsg::ClientWrite { payloads, .. }`); that is read in the source, not measured |
| M5 group commit across groups, 1 ms simulated fsync, 32 in flight | raft-rs: 152,000 ops/s, 10 fsyncs/s, each covering 3,000 `Ready`s and 48,000 entries. That shows the driver can fsync once for every group, but it also shows this naive round-based driver has rounds of about 100 ms. openraft, shared WAL task: 53,200 ops/s, 66 fsyncs/s, **903 `append` calls (2,407 entries) per fsync** on 1 thread; 96,700 ops/s, 203 fsyncs/s, 534 appends per fsync on 4 threads. **Both libraries allow one fsync for many groups:** raft-rs because the caller persists `Ready`s, openraft because `append` completes through the `LogFlushed` callback ([storage/v2.rs](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/v2.rs)) |
| M5 heartbeat coalescing | raft-rs: possible in the driver. Ticks and messages are the caller's, so it can batch per node pair (measured above), coalesce heartbeats CockroachDB-style, or quiesce. openraft 0.9: no hook. Each group's tick task sends its own AppendEntries. `openraft-multi` (0.10 only) shares the network connection, not heartbeats |

## Results: maturity and sans-IO fit

| Test | Result |
| --- | --- |
| M6 releases | openraft: 0.9.x stable, 0.9.0 (2024-03-11) to 0.9.25 (2026-07-28). 0.10 has been in alpha for over two years (git tag `v0.10.0-alpha.6` dated 2024-08-06; alpha.37 2026-10-10). The README says "API is not stable yet", and 0.9 → 0.10 has many breaking changes (storage, `AsyncRuntime`, `Raft<C, SM>`). raft-rs: last crates.io release 2023-03-07, 6 commits on master in the last 12 months. TiKV pins raft-rs git master and a forked protobuf, not the crates.io release ([tikv Cargo.toml](https://github.com/tikv/tikv/blob/master/Cargo.toml)) |
| M6 users | openraft: Databend meta-service, CnosDB, RobustMQ, RocketMQ-rust, Hiqlite and others ([README](https://github.com/databendlabs/openraft#who-uses-it)). raft-rs: TiKV only ([README](https://github.com/tikv/raft-rs#projects-using-the-raft-crate)) |
| M6 bugs and testing | openraft: a turmoil deterministic-simulation fuzzer runs in CI since 2026-04, and a Jepsen suite runs on every push to main since 2026-07 ([tests-turmoil](https://github.com/databendlabs/openraft/tree/main/tests-turmoil), [jepsen](https://github.com/databendlabs/openraft/tree/main/jepsen)). In 2026 they found **quorum-safety bugs**: commit without a real quorum, [#1802](https://github.com/databendlabs/openraft/issues/1802) (fixed in 0.9.25); a stale membership after snapshot install, [#1808](https://github.com/databendlabs/openraft/issues/1808); follower log corruption, [#1795](https://github.com/databendlabs/openraft/issues/1795). A RaftCore panic that permanently loses quorum is open: [#2116](https://github.com/databendlabs/openraft/issues/2116). raft-rs: ported etcd tests plus failpoints, no fuzzing found; open issues include [#234](https://github.com/tikv/raft-rs/issues/234) (lease read after leader transfer) and [#595](https://github.com/tikv/raft-rs/issues/595) (a panic) |
| M6 footprint | openraft: 68 crates in its dependency tree, including tokio, clap, chrono and getrandom. raft-rs: 22 crates, including protobuf 2.28, fxhash and getrandom. Neither sets `rust-version` |
| M6 unsafe | openraft 0.9.25 `src/`: none. raft-rs 0.7.0: two (`quorum/majority.rs` uses a `MaybeUninit` stack array; `lib.rs` has one in the logger setup) |
| M6 cargo-deny with our `deny.toml` | **FAILED**: RUSTSEC-2024-0437 (protobuf 2, no fixed 2.x release) and RUSTSEC-2025-0057 (fxhash unmaintained), both from raft-rs; RUSTSEC-2026-0097 (rand 0.8 unsound), from both libraries |
| M7 sans-IO lint | Both libraries pull `getrandom` (via `rand`), which `scripts/lint.sh` bans in a sans-IO crate. openraft 0.9 also depends on tokio unconditionally (tokio channels and `select!` in `raft_core.rs`) |
| M7 replay, 10 runs in separate processes, same seed | raft-rs as shipped: 10 distinct traces (`rand::thread_rng()` election timeouts). **raft-rs with the driver overriding the timeout from a seeded RNG after every term or role change: 1 trace.** That override goes through `set_randomized_election_timeout`, which is `#[doc(hidden)]` "for testing" ([raft.rs:474](https://github.com/tikv/raft-rs/blob/v0.7.0/src/raft.rs#L474)). openraft with `TokioRuntime`: 10 distinct traces. **openraft with a custom `AsyncRuntime` whose `thread_rng()` is seeded, on a current-thread tokio runtime with a paused clock: 1 trace.** That is not guaranteed: tokio's unbiased `select!` in `replication/mod.rs:610` draws from tokio's own RNG, which is unseeded without `tokio_unstable`. The run is still a tokio executor running 11,000 tasks, not `on_event(now, ev) -> Vec<Action>`. 0.10 adds `openraft-rt-sim`, a seeded, virtual-time runtime ([rt-sim](https://github.com/databendlabs/openraft/tree/main/rt-sim)); not tried |

## Conclusions

1. **Don't use openraft 0.9 for the MetaStore's multi-Raft.**
   - Memory: it preallocates about 600 KiB per replica, and RSS creeps toward that (M1).
   - Tasks: each group runs 11 tasks (M2).
   - Idle CPU: at 1,000 groups it costs about 3.5× raft-rs, and close to a full core at default timing for 3,000 replicas (M2).
   - Group commit: the leader appends to storage once per write (M4).
   - Fit: it cannot run as a sans-IO state machine (M7).
   - openraft 0.10 fixes write batching and the runtime abstraction, but it is still alpha and keeps per-group tasks and ticks.
2. **The Raft shard must be a `Ready`-style state machine whose ticks, messages and persistence belong to the driver.** That shape gave 13 KiB per group, 4.5 µs CPU per idle message, one fsync for every group's `Ready` and quiescence at 0.07% CPU (M1, M2, M5). It is the only shape here that fits `on_event(now, ev) -> Vec<Action>`.
3. **Don't take raft-rs 0.7.0 as a dependency either.**
   - Supply chain: cargo-deny fails on protobuf 2 and fxhash, the crate pulls getrandom, and the last release is from 2023 (TiKV itself uses git master) (M6).
   - Determinism: it needs an undocumented test hook to stop drawing from `thread_rng()` (M7).
   - Instead, write our own Raft shard in `core` modelled on raft-rs and etcd-raft's `RawNode`/`Ready`, with a seeded RNG, `BTreeMap`s and no protobuf. Port etcd/raft-rs test cases as fixtures. A vendored fork of raft-rs (protobuf swapped for plain structs, RNG injected) is the fallback if the schedule needs consensus before our own shard exists.
4. **Plan for coalescing and quiescence from the start.** Plain per-group heartbeats cost 4 messages per group per heartbeat: 40,000 msgs/s per 1,000 groups at 100 ms (M2). Batch messages per node pair (240 batches/s instead of 80,000 messages/s), quiesce idle groups, and move failure detection for quiescent groups to node liveness (as TiKV hibernate regions and CockroachDB do).
5. **Budget failover as 1.5–5× the maximum election timeout, and tick the election timer finer than the heartbeat.** Failover time is set by the slowest of the hundreds of groups that lost their leader. Re-election of 334 groups took 1.4–5× the maximum election timeout in both libraries (M3b). With 3 distinct timeout values, a 10 ms tick halved it at default timing.
6. **Group commit across shards belongs in the driver's WAL, not in Raft.** One fsync covered 534–3,000 groups' appends in both harnesses (M5). The MetaStore's log store should be one per-core WAL shared by its shards, as TiKV's raft-engine is.

## New open questions

- How do quiescent groups keep leader leases for reads without heartbeats? Node-liveness epochs (CockroachDB) or a per-node-pair lease?
- Commit latency (p50 and p99) of a driver that collects every group's `Ready` per round. The naive loop here had rounds of about 100 ms at 32,000 proposals in flight, so the driver needs bounded rounds or per-core shards.
- Does the per-group cost really make 1,000 groups per node the right target, or should shards be fewer and larger?
- If openraft is reconsidered later: re-measure 0.10 with `openraft-rt-sim`, `openraft-multi` and its batched writes.

## Not tested

- Real disks, real fsync and a real network.
- Latency percentiles.
- Snapshots, log compaction and membership changes.
- openraft 0.10 (read only).
- Multi-process and multi-host runs.
- A real CockroachDB-style coalesced heartbeat (only per-pair batching was counted).
- Where openraft's RSS levels off over runs longer than 120 s.
- tokio `rng_seed` (needs `--cfg tokio_unstable`).
- Other libraries, such as little-raft (last release 2022-01) and async-raft (abandoned predecessor of openraft).
- Clean CPU numbers on an idle host.
