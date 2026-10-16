# Embedded KV engines spike (redb, fjall, plain log file)

> **Superseded in part.** Conclusions 3 and 4 chose redb for shard state and fjall for the chunk index, and conclusions 1 and 2 a separate Raft log per core. [ADR 0003](../../docs/decisions/0003-one-log-per-disk.md) instead keeps all three in one log per disk, with no embedded engine. The measurements stand.

**Question.** Which embedded key-value engine, `redb` (copy-on-write B-tree) or `fjall` (LSM tree), should hold (a) the Raft logs, (b) MetaStore shard state and (c) the per-disk ChunkStore [chunk index](../../docs/design.md#on-disk-format)? Or should some of these use something else, such as a plain append-only file for the Raft logs? This answers the "`redb` versus `fjall`" half of the Raft and storage-engine open question in [design.md](../../docs/design.md#roadmap-and-open-questions).

**Setup.** Linux 6.17 laptop: Intel Core Ultra 5 235U (14 cores), 15 GB RAM. One consumer NVMe, UMIS RPEYJ512MML1QWQ, which is also the root disk: ext4, volatile write cache. The engines are `redb` 4.4.0 (2026-10-13) and `fjall` 3.1.12 (2026-10-03) with `lsm-tree` 3.1.10, the latest stable releases on crates.io, pinned with `=`. Rust 1.99.1. The harness is one Rust binary ([src/main.rs](src/main.rs)) in a standalone cargo workspace, and every test runs in its own process.

Settings:

- Durable commits:
  - redb: `Durability::Immediate`. Its default is a 1-phase commit with checksums and one `fdatasync`.
  - fjall: a `WriteBatch` with `PersistMode::SyncData` (`fdatasync`).
  - Plain file: `write` + `fdatasync`.
- Caches: 64 MiB for each engine in K1, K2, K3 and K7. The defaults would be redb 1 GiB and fjall 32 MiB.
- Data: values are random, so incompressible, which makes fjall's LZ4 compression look worse than it would on real metadata.

Stand-ins and limits:

- **The machine was shared.** Other agents were compiling, running CPU-heavy simulations and measuring NVMe flush latency at the same time (loadavg 6 to 22, recorded on every test header).
- **Write amplification has two measures, and neither is exact.**
  - `wamp` is `/proc/self/io` `write_bytes` ÷ payload. It misses ext4 journal writes. It also over-counts writes into large page-cache folios: a 4 KiB rewrite of a file pre-zeroed with 1 MiB writes was counted as 1 MiB, so the harness pre-zeroes with 4 KiB writes.
  - `devwamp` is whole-disk sectors written (`/proc/diskstats`) ÷ payload. It includes the other processes: K0 measured 0.05 to 9 MB/s of background writes while the harness was idle.
- **Process crash, not power loss.** `kill -9` cannot lose the page cache, so K5 tests atomicity and recovery code, not what reaches the flash. Power-loss testing needs root (dm-log-writes or a VM).
- **Scaled down** from the suggested workloads:
  - K3 uses 10M index entries and 200k random updates per batch size.
  - K7 runs 100 commits.
  - K5c stops at a 64 MB journal: its first attempt kept writing and was killed after more than 2 GB.
  - Total bytes written by this spike are an estimate (about 15 to 20 GB), not a measurement.

Hypotheses were written before the first run, as the `[OK ]`/`[FAIL]` checks in the harness:

- redb within 3x of the file floor;
- redb write amplification above 10 at 256 B × 1 per commit;
- fjall at or below 3 at 256 B × 128;
- 100 separate commits at least 20x slower than one combined commit;
- fjall not coalescing concurrent `SyncData` batches;
- every acknowledged batch present after `kill -9`, and no partial batch.

```
./run.sh                      # all tests into results.txt; KV_QUICK=1 ./run.sh for a 5-minute smoke run
./run.sh k2:redb-qr k4        # selected tests; OUT=file.txt to redirect
./k6.sh > results-k6.txt      # dependency tree, unsafe count, compile time, binary size
target/release/kv-engines-spike k5child|k3child|open ...   # helpers spawned by the tests
```

Raw output (each file starts with kernel, versions, date and loadavg):

| File | Tests |
| --- | --- |
| [results-k1.txt](results-k1.txt) | K1, K1b, K1c |
| [results-k2.txt](results-k2.txt) | K2 redb, K2 fjall |
| [results-k3.txt](results-k3.txt) | K3, K5b |
| [results-k2qr-k4.txt](results-k2qr-k4.txt) | K0, K2 redb with quick-repair, K4, K4b, a first K5 run whose in-flight check had a harness bug (fixed), and the aborted first K5c |
| [results.txt](results.txt) | K0, K5, K5c, K7 |
| [results-k6.txt](results-k6.txt) | K6 |

## K1: Raft log

Each commit appends a batch of entries and makes it durable, as in the [group commit loop](../../docs/design.md#metadata-layer-metastore). There are four stores:

- **file-prealloc**: a segment file pre-zeroed ahead of time, overwritten in place, then `fdatasync`. This is the floor.
- **file-append**: a growing file, then `fdatasync`.
- **redb**: one `u64` → bytes table.
- **fjall**: one keyspace.

Latency is commit p50; `wamp` and `devwamp` are as defined in Setup.

| Test | Result |
| --- | --- |
| K1 256 B × 1 per commit | file-prealloc 1.10 ms, 835 entries/s. file-append 2.92 ms. redb 1.30 ms, **wamp 112** (devwamp 143). fjall 1.13 ms, wamp 17 (devwamp 43) |
| K1 256 B × 16 | file-prealloc 1.24 ms, 11.4k/s. file-append 3.04 ms. redb 3.63 ms, 4.3k/s, wamp 8.9. fjall 2.94 ms, 5.0k/s, wamp 2.1 |
| K1 256 B × 128 | file-prealloc 1.18 ms, 102k/s, wamp 1.12. file-append 3.19 ms, 38k/s. redb 3.98 ms, 29k/s, wamp 2.07. fjall 3.19 ms, 37k/s, wamp 1.24 |
| K1 4 KB × 1 | file-prealloc 1.07 ms. file-append 2.89 ms. redb 3.38 ms (p99 98 ms), wamp 9.0. fjall 2.77 ms, wamp 2.0 |
| K1 4 KB × 16 | file-prealloc 1.45 ms. file-append 3.47 ms. redb 4.51 ms, wamp 2.5. fjall 3.36 ms, wamp 1.07 |
| K1 4 KB × 128 | file-prealloc 4.2 ms, 28k/s. file-append 7.6 ms. redb 8.2 ms, 13.5k/s, wamp 2.1. fjall 8.2 ms, **p99 114 ms**, 5.6k/s overall: a memtable flush stalls commits |
| K1 why the file floor is 2.5x faster than appending | **An `fdatasync` that has to allocate blocks or extend the file costs an ext4 journal commit and a second device flush: about 2.9 ms instead of 1.1 ms.** fjall "preallocates" its journal with `set_len` ([journal/writer.rs:131](https://docs.rs/crate/fjall/3.1.12/source/src/journal/writer.rs)), which only makes a sparse file. That fits the numbers, though no block-layer trace confirmed it: fjall is at the floor only at 256 B × 1, where consecutive commits share an already-allocated 4 KB block, and at the append cost otherwise |
| K1 CPU per entry | 256 B × 128: file 1.6 µs, fjall 3.0 µs, redb 6.2 µs. 4 KB × 128: 16.4, 21.8 and 27.8 µs |
| K1b truncate the first 90% of 200k × 256 B entries in one durable step | File: unlink 11 segments, 25 ms, disk 60 → 16 MB. redb: `retain_in` in 41 ms, writes 0.15 MB. **The file stays at 53 MB until `compact()`** (62 ms) brings it to 5.3 MB. fjall with tombstones: 193 ms, writes 5 MB, and **disk grows** 106 → 112 MB, then to 118 MB after a major compaction. fjall `drop_range`: 14 ms, but **drops nothing**, because it removes only whole SSTs inside the range ([lsm-tree tree/mod.rs:243](https://docs.rs/crate/lsm-tree/3.1.10/source/src/tree/mod.rs)) and one SST spans the cut |
| K1c steady state: 4000 commits of 128 × 256 B, truncate to the last 20k entries every 100 commits (4.9 MB live) | File: p50 1.18 ms, p99 3.7 ms, truncate p50 4.7 ms, 16 MB on disk (the 144 MB maximum is all segments preallocated up front). redb: p50 1.57 ms, p99 6.0 ms, wamp 2.09, **9.1 MB on disk**. fjall: p50 3.1 ms, **p99 94 ms**, truncate up to 124 ms, **179 MB on disk** (fjall with `drop_range`: 193 MB) |
| K1 why fjall keeps space | A journal file is deleted only once every keyspace has flushed past it, and the active journal is never deleted ([journal/manager.rs:120-160](https://docs.rs/crate/fjall/3.1.12/source/src/journal/manager.rs)). By default up to 512 MiB of journal is kept (`max_journaling_size`). An obsolete SST is unlinked only when its last `Arc` drops ([lsm-tree table/inner.rs:78-85](https://docs.rs/crate/lsm-tree/3.1.10/source/src/table/inner.rs)), after old versions are garbage-collected past a seqno watermark ([version/super_version.rs:70](https://docs.rs/crate/lsm-tree/3.1.10/source/src/version/super_version.rs)) |

## K2: MetaStore shard

1M keys laid out as in ADR 0004: 500 directories × 1000 files, each with a dentry key and an attribute key, attributes 100 to 500 B, 174 MB logical. Each mixed round is 64 operations (70% `get`, 30% `put` of attributes) and ends in one commit. Two commit modes:

- **durable**: every round's commit is durable.
- **apply**: non-durable commits with a durable checkpoint every 100 rounds. This is what a Raft state machine can do, since its log is already durable.

`redb-qr` is redb with `set_quick_repair(true)` on every commit.

| Test | Result |
| --- | --- |
| K2 load | redb 7.6 s, disk 331 MB (**1.90x** logical). fjall 3.6 s, disk 228 MB (1.31x) |
| K2 durable rounds | redb 14.1k ops/s, get p50 7.2 µs, commit p50 3.6 ms / p99 11 ms, **wamp 39**. redb-qr 11.7k ops/s, commit p50 4.8 ms, wamp 52. fjall 9.5k ops/s, get p50 17.8 µs, commit p50 3.7 ms / **p99 96 ms**, wamp 1.75 (devwamp 12.7) |
| K2 apply mode | redb 28.7k ops/s, commit p50 0.88 ms, wamp 21. redb-qr 42.1k ops/s, wamp 22 (the redb/redb-qr gap is load noise; same code path). **fjall 47.7k ops/s**, commit p50 0.14 ms, wamp 3.7 |
| K2 `readdir` (prefix scan of 1000 entries), warm | redb p50 196 µs / p99 369 µs. fjall 374 µs / 1.8 ms |
| K2 space after the mixed phase | redb 345 MB (1.99x). fjall 203 MB (1.17x) including 17.5 MB of journal |
| K2 memory (cache 64 MiB) | **redb peak RSS 74 MB, fjall 239 MB** (memtable plus pinned index and filter blocks) |
| K2 open in a fresh process, page cache dropped | **redb 3 to 6 ms. fjall 306 ms with the journal unflushed, 454 ms after a memtable flush** |
| K2 cold reads after reopen | redb: 200 gets p50 233 µs / p99 1.4 ms, `readdir` 2.8 ms. fjall: gets p50 90 µs / p99 1.4 ms, `readdir` 0.6 ms |

## K3: per-disk chunk index

10M entries. The key is pool, h, chunk ID, version and first block (38 B, sorted by placement hash as in ADR 0004); the value is segment, offset, length and CRC (16 B); 540 MB logical. The index is bulk-loaded in key order, 100k entries per commit. Then come durable batches of random updates: each update inserts an entry and deletes one from the same chunk, 200k updates per batch size. Lookups are exact hits, misses at an existing chunk prefix, and "floor seeks" (last key ≤ the target block).

| Test | Result |
| --- | --- |
| K3 sorted bulk load | redb 17.4 s, 609 MB (1.18x). fjall 15.5 s, 416 MB (0.81x, smaller than the logical size; presumably key prefix truncation and LZ4, not checked) |
| K3 random updates, 1k per commit | **redb: commit p50 95 ms, 10k entries/s, wamp 160 (1.6 GB written for 200k updates)**. fjall: 14 ms, 60k/s, wamp 5.0 (51 MB) |
| K3 random updates, 10k per commit | redb 676 ms, 14.8k/s, wamp 114 (1.2 GB). fjall 103 ms, 89k/s, wamp 2.5 |
| K3 random updates, 100k per commit | redb 5.7 s, 17.9k/s, wamp 100 (1.0 GB). fjall 0.73 s, 131k/s, wamp 5.2. fjall's figures cover only compactions that ran during the test, so compaction debt is not included |
| K3 size after the updates | **redb 1095 MB (115 B per entry). fjall 466 MB (49 B per entry)**, including 26 MB of journal |
| K3 lookups, warm | redb: hit p50 10 µs, miss 0.8 µs, floor 1.0 µs. fjall: hit 14 µs, miss 1.8 µs (bloom filter), floor 14 µs |
| K3 lookups, cold (page cache dropped) | **redb: hit p50 147 µs / p99 236 µs. fjall: hit 1.4 ms / p99 5.0 ms, floor seek 2.3 ms / p99 7.9 ms** |
| K3 memory | redb RSS 125 MB. fjall 322 MB |
| K3 clean open, fresh process | redb 3 to 5 ms. fjall 1.6 to 2.1 s |
| K5b reopen after `kill -9` of a writer of 1k-entry durable commits on this index | **redb (default): 32.8, 34.2 and 21.7 s, reading 0.9 to 1.3 GB.** Without the allocator state from a 2-phase commit, an unclean shutdown makes redb walk the whole file ([db.rs:1574](https://docs.rs/crate/redb/4.4.0/source/src/db.rs)). **redb with quick-repair: 7, 13 and 9 ms.** fjall: 2.5, 3.4 and 3.2 s |
| K5c fjall open time against journal size (fresh process, page cache dropped) | 16 MB of journal: 0.53 s. 32 MB: 1.10 s. 64 MB: **2.8 s**, cold or warm, so CPU-bound. On open, fjall replays every journal entry into a memtable ([recovery.rs:153-180](https://docs.rs/crate/fjall/3.1.12/source/src/recovery.rs), [db.rs:700](https://docs.rs/crate/fjall/3.1.12/source/src/db.rs)). Extrapolated, not measured: the default 512 MiB journal limit would mean about 20 s |

## K4: durability semantics and group commit across shards

N = 100 shards. In each round every shard appends one 256 B entry, and the whole round has to be durable.

| Test | Result |
| --- | --- |
| K4 what a commit guarantees | redb `Immediate`: durable when `commit()` returns (1-phase: checksummed commit slot, then one `fdatasync`; the `None` level waits for a later `Immediate` commit). fjall: a batch is durable only with `PersistMode::SyncData`/`SyncAll` set on it or a later `persist()`; the default `Buffer` only reaches the OS. Both make a batch atomic across tables or keyspaces |
| K4 one combined commit per round, round p50 | Shared file + one `fdatasync`: **1.20 ms**. fjall, one batch over 100 keyspaces: 3.1 ms, 32 KB written. redb, one transaction over 100 tables: 5.4 ms, **816 KB written per round** (a CoW path per table) |
| K4 100 separate commits per round | redb, 100 transactions in one file: 120 ms. fjall, 100 batches: 114 ms. Both are about **22 to 36x the combined commit** |
| K4 100 threads committing at once | fjall, one database: 121 ms. **Concurrent `SyncData` batches are not coalesced**, because the journal lock is held across each `fdatasync` ([batch/mod.rs:106-125](https://docs.rs/crate/fjall/3.1.12/source/src/batch/mod.rs)). redb, 100 files: 20.6 ms. 100 plain files: 7.9 ms (the NVMe overlaps some flushes) |
| K4b 1000 shards in one process | redb, 1 file with 1000 tables: create 72 ms, reopen 14 ms, 4 MB. redb, 1000 files: create 7.9 s, 1000 file descriptors, +42 MB RSS, reopen 1.24 s. **fjall, 1 database with 1000 keyspaces: create 64 s** (64 ms per keyspace), reopen 160 ms, 2 file descriptors |

## K5: crash safety (process kill)

50 rounds per store: kill a writer of 64-entry durable batches at a random time, reopen, and verify.

| Test | Result |
| --- | --- |
| K5 redb (2815 batches acknowledged) | Every acknowledged batch present, no partial batch, values intact, nothing past the in-flight batch: all OK. Reopen p50 43 ms |
| K5 fjall (1191 batches) | All OK. Reopen p50 103 ms |
| K5 segmented file with CRC32C frames (2545 batches) | All OK. Recovery reads to the first bad frame; p50 73 ms |
| K5 power loss | Not tested: it needs root (dm-log-writes or a VM with a cache-dropping block device) |

## K6: maturity

Sources for the release, maintainer and bug figures are in the cited changelogs and issues; the footprint numbers are from [results-k6.txt](results-k6.txt).

| Test | Result |
| --- | --- |
| K6 format stability | redb: 1.0 (2023-06) promised a backward-compatible format, then 2.0 (2024-03) broke it with no upgrade tool, and 3.0 (2025-08) dropped the v2 format: you must upgrade through 2.6's `Database::upgrade()`. The README now promises "a reasonable effort" at an upgrade path ([CHANGELOG](https://github.com/cberner/redb/blob/master/CHANGELOG.md)). fjall: 1.x → 2.x and 2.x → 3.x both broke the disk format, and 3.x needs the `fjall_v2_v3_migrator` copy tool ([blog](https://fjall-rs.github.io/post/fjall-3/), [migrator](https://github.com/fjall-rs/migrate-v2-v3)) |
| K6 recent durability and corruption fixes, redb | 4.0.0 data loss through the `get_mut` guard. 4.2.0: a second crash during repair could roll back durable commits, and a crash while growing the file could leave it unopenable. 4.3.0: a commit could be rolled back after `check_integrity()` had repaired the database. Issue [#1334](https://github.com/cberner/redb/issues/1334): reads never verified page checksums (closed) |
| K6 recent durability and corruption fixes, fjall | 3.0.1/3.0.3 lost inserts ([lsm-tree#230](https://github.com/fjall-rs/lsm-tree/issues/230)). 3.1.7 "`SyncAll` can falsely imply durability" ([#304](https://github.com/fjall-rs/fjall/issues/304)). **3.1.11 (2026-10-02): a memtable flush did not sync the journal first, so after power loss only part of a batch survived ([#329](https://github.com/fjall-rs/fjall/issues/329))**. 3.1.12: a reused keyspace ID replayed the old keyspace's journal ([#328](https://github.com/fjall-rs/fjall/issues/328)). Still open: a snapshot can see part of a write batch ([#333](https://github.com/fjall-rs/fjall/issues/333)); mid-journal corruption cannot be told from a torn tail ([#311](https://github.com/fjall-rs/fjall/issues/311)) |
| K6 releases and maintainers | redb: 78 versions since 2018, one crates.io owner, 90% of commits by one person, 13.5M downloads, 521 reverse dependencies. fjall: 102 versions since 2023-12, one owner, 95% of commits by one person, 1.8M downloads, 96 reverse dependencies. Every fjall release from 3.0.0 to 3.1.5 is yanked, with no reason given ([crates.io](https://crates.io/crates/fjall/versions)) |
| K6 dependencies | **redb: one (`libc`)**. fjall: 48 unique crates, including `lsm-tree`, `crossbeam-skiplist`, `dashmap`, `flume`, `quick_cache`, `lz4_flex` and proc macros |
| K6 `unsafe` (blocks, fns and impls) | redb: 53 in 41k lines. fjall: 3 in 12.7k lines, each behind `#[expect(unsafe_code)]` despite its "100% safe" claim. lsm-tree: 33 in 30k lines |
| K6 clean release build (loadavg about 20) and stripped binary over a 337 KB hello world | redb 33.6 s, +1.0 MB. fjall 25.9 s, +1.3 MB |

## K7: a long-lived reader

A read transaction (redb) or snapshot (fjall) is held open over 100 durable commits of 1000 random overwrites each, on 100k keys × 200 B.

| Test | Result |
| --- | --- |
| K7 redb | **The file grows to 419 MB, against 26 MB with no reader. It stays at 423 MB after the reader closes and 50 more commits, until `compact()` (22 MB).** The writer is not blocked: commit p50 15.5 ms with the reader, 11.4 ms without. The reader still sees its old value |
| K7 fjall | The reader makes no difference here: 43.7 MB either way, and the snapshot still reads its old value. After a major compaction the store is 96 MB, of which 55 MB is journal. Commit p99 is 119 ms in the run without a reader (a flush stall) |

## Conclusions

1. **Raft logs (a) go in our own segmented log file, not in either engine.** Segments are recycled after being pre-zeroed (or written in full once), and each commit is `pwrite` + `fdatasync`, with CRC-framed records. It is the only store at the device floor for every batch shape: 1.1 to 1.5 ms p50, write amplification about 1.0 to 1.1. Truncation is an unlink. redb costs 2 to 112x write amplification and up to 8 ms. fjall's sparse journal pays the append cost (2.8 to 3.4 ms for batches above one 256 B entry); it kept 179 MB on disk for 4.9 MB of live log, stalled commits for about 100 ms during memtable flushes (K1, K1c), and replays its whole journal on open (K5c). Appending to a growing file is 2.5x slower than overwriting preallocated space (K1), so pre-zeroed or recycled segments are part of the design, not an optimisation.
2. **One log per core, shared by all of that core's Raft groups, with one `fdatasync` per round.** 100 shards in one shared-file flush take 1.2 ms, against 114 to 120 ms for 100 separate commits in either engine (K4). Neither engine merges concurrent commits by itself: fjall holds its journal lock across each `fdatasync`. Group commit across shards has to be built in the shard loop.
3. **MetaStore shard state (b) goes in redb, one file per core holding one table set per shard, with quick-repair on.**
   - redb gives faster point reads (7 µs against 18 µs p50) and scans (`readdir` 196 µs against 374 µs).
   - It opens in milliseconds, where fjall takes 0.3 to 2.8 s.
   - It uses a third of the memory (74 MB against 239 MB RSS).
   - It takes 72 ms to create 1000 tables in one file, where fjall needs 64 s for 1000 keyspaces (K2, K4b).
   - Default redb repairs by reading the whole file after a crash: 22 to 34 s for 1.1 GB. Quick-repair reopens in 7 to 13 ms and costs about 1.2 ms more per durable commit (K2, K5b).
   - The cost is write amplification of 21 (apply mode) to 52 (durable, quick-repair) and space amplification of about 2. Metadata write rates make that acceptable, but it counts against consumer SSD endurance. The state machine applies in non-durable commits with periodic durable checkpoints, because the Raft log is the durable copy: that doubles redb's throughput (K2).
4. **The chunk index (c) goes in fjall, as a lazily persisted checkpoint of the ChunkStore log.**
   - Random index updates in redb cost 100 to 160x write amplification: 1.0 to 1.6 GB written per 200k updates on a 10M-entry index, and 95 ms per 1k-entry commit. That would add a write stream to every data disk comparable to the user's own small writes (K3).
   - fjall does the same work at 2.5 to 5x, 6 to 7x faster, in less than half the space (49 B against 115 B per entry).
   - Two costs need managing:
     - Cold point lookups are slow (1.4 ms p50, 5 ms p99, against redb's 0.15 ms), so the hot part of the index has to stay in fjall's cache.
     - Open time grows with the journal at about 35 to 45 ms per MB, so the daemon persists and flushes at each checkpoint and sets `max_journaling_size` to its 64 MiB minimum.
5. **Whatever the engine, nothing long-lived holds a read view.** One open redb read transaction grew a 26 MB file to 419 MB in 100 commits, and the space came back only with `compact()` (K7). This confirms the design's rule that snapshot metadata is copied out. Raft snapshots of a redb shard must be streamed quickly or taken from a copy.
6. **Both engines are one-maintainer projects that have broken their disk format twice** and fixed durability bugs within the last month (K6). The engine therefore sits behind our own storage trait: the MetaStore's state can be rebuilt from Raft snapshots and the chunk index from the log. Pin exact versions. Each upgrade needs a [tech-debt entry](../../docs/tech-debts.md) and a crash test (K5) rerun. Do not adopt fjall below 3.1.11 (partial batches after power loss, #329).
7. **Measuring write amplification needs device-level tracing on a quiet machine.** `/proc/self/io` misses ext4 journal writes and over-counts large-folio writes, and `/proc/diskstats` includes other processes (K0: up to 9 MB/s here). Rerun `./run.sh` on a quiet machine, ideally with eBPF on the block layer, before tuning on these numbers.

### Per-use recommendation

| Use | Store | Main reason |
| --- | --- | --- |
| (a) Raft logs | Own segmented log, one per core, pre-zeroed segments, one `fdatasync` per round for all groups | At the device floor (1.1 ms), write amplification about 1, truncation by unlink. Neither engine coalesces commits across shards or reaches the floor |
| (b) MetaStore shard state | redb 4.x, one file per core, a table set per shard, quick-repair on, non-durable apply plus periodic durable checkpoint | Fast reads and scans, millisecond open, low RSS. Write amplification is the price |
| (c) Per-disk chunk index | fjall 3.1.12+, `Buffer` writes plus a persist at each checkpoint, small `max_journaling_size` | 19 to 46x less write amplification than redb on random updates, and less than half the size |

### New open questions

- Truncating a shared multi-group log: a segment can be deleted only when every group on it has snapshotted past it. Does a slow group force a snapshot, or are its live entries copied forward the way a cleaner would?
- redb quick-repair commit cost on a multi-GB file: it was measured only on a 345 MB shard (K2).
- fjall read latency once compaction debt accumulates over days, and how big the block cache must be to keep cold index lookups (1.4 ms here) off the I/O path.
- Should fjall's journal be truly preallocated (written once, or `fallocate` plus a write) to stop paying 2 flushes per commit? That needs a patch upstream or a fork.
- fjall issue [#333](https://github.com/fjall-rs/fjall/issues/333) (a snapshot sees part of a batch): does it affect index readers during a cleaner pass?
- Power-loss behaviour of all three stores on consumer NVMe with a volatile cache: needs a root-only dm-log-writes or VM test.

### Not tested

- Power loss.
- Multi-hour runs and steady-state compaction debt.
- More than 10M index entries, and indexes larger than RAM with a small page cache.
- fjall `SyncAll` and redb 2-phase commit on their own (quick-repair turns 2-phase on).
- fjall key-value separation and its compaction filters.
- redb `Durability::None` for the Raft log.
- An in-memory index with a checkpoint file as a third option for (c).
- openraft integration: the other half of the open question.
- Read-heavy concurrency across many threads.
- Any filesystem other than ext4.
