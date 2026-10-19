# LSM study: the read path

**Question.** What do Pebble and RocksDB put on an LSM tree's read path (filters, block cache, iterators, block format), which of it transfers to rock-solid's in-memory sorted runs, and does the MetaStore need per-run Bloom filters, or a cap on runs that merges early, before the block frontend brings filters for the chunk index ([open questions](../../docs/design.md#roadmap-and-open-questions))? The chunk index reads by floor lookups (the greatest key at or below a block's) inside one chunk's key prefix, which a whole-key filter cannot serve.

**Setup.** Linux 6.18.44 in a Firecracker VM, 4 vCPUs of an Intel Xeon at 2.8 GHz **shared with six other agents' benchmarks**, Go 1.27.0. Sources read: Pebble master at `a7b8321871e3` (2026-10-14) and v2.1.7, RocksDB v11.8.1. The harness ([main.go](main.go)) drives the real `internal/lsm` at rock-solid `86c47be` through the module's `replace` directive and Pebble master's own filter packages (`sstable/tablefilters/bloom`, `sstable/tablefilters/binaryfuse`) through a `replace` to the clone `fetch.sh` makes. It reports thread CPU time per operation (`CLOCK_THREAD_CPUTIME_ID` on a locked thread) and counts; on this machine absolute times moved by up to 2× between runs, so compare rows of one table and trust the counts (runs searched, probes) more than the nanoseconds.

- **F, filters alone.** Each of Pebble's policies over 200,000 distinct keys of two shapes: the chunk index's prefix `(pool u32, XXH3 of the chunk u64, chunk ID 16 B)`, 28 B; and MetaStore keys, half attribute keys `fs/{volume}/{inode}/a` (14 B) and half directory entries with 8 to 32-byte names. Bits per key, false positive rate (FPR) over 1,000,000 absent keys, build CPU per key, probe CPU absent and present, allocations. Three repetitions.
- **L, lookups over N runs.** One index of about 2 million entries, cut into N = 6, 12 or 24 runs in size tiering's steady shape (three runs per class, classes four times apart, oldest and largest first), so N = 24 is the [LSM tree](../../docs/design.md#on-disk-format)'s 16 GiB shard in miniature and N = 6 the same entries held in fewer, larger runs, as a run cap would leave them. Each index is loaded twice: as one real tree with N runs (`real tree`), and as N single-run trees probed newest first, with no filter (`none`) or with one filter per run over that run's keys (`bloom10`, `fuse8`), which is how a filtered tree would search. `runs` is the runs searched per lookup, `probes` the filter probes, `filter%` the filters' bytes against the runs', `fkeys/e` the keys fed to the filters per entry. Indexes:
  - **ci-whole**: chunks written whole, four 256-block extents each, every chunk in the run of the flush that wrote it. `read` is a floor of a random block of a written chunk within its prefix; `unwritten` the same for a chunk never written, as a thin volume's reads of unwritten space are. Filters hold each run's chunk prefixes (RocksDB's `prefix_extractor` with `whole_key_filtering = false`, Pebble's `Split`).
  - **ci-overwrite**: random one-block writes into a volume of 62,500 chunks, 32 entries per chunk overall, so every large run holds nearly every chunk. Same prefix filters.
  - **metastore**: the [MetaStore memory spike](../../docs/design.md#appendix-metastore-memory-spike)'s file model, an attribute record and a directory entry per file, 40 files per directory, each run holding the files created since the flush before. `get attr` reads an existing file's attributes; `get absent` a name that does not exist in an existing directory (a negative lookup, as every `create` and every `$PATH` search makes). Filters hold whole keys and each key's inode prefix, as RocksDB's `whole_key_filtering` with a prefix extractor does (`full_filter_block.cc:67-77`).
- **S, scans.** The real tree over the same MetaStore index in N runs: a seek, then 50 steps; CPU per seek and per step.

```
./fetch.sh   # shallow clones of Pebble master, Pebble v2.1.7 and RocksDB v11.8.1 into work/
./run.sh     # about 30 minutes on the shared VM; results-f.txt, results-ci.txt, results-ms.txt, results-s.txt
```

Expected before the run: every filter cuts a miss to the probes plus about FPR × N searches; a hit searches about one run; the prefix filter does little for ci-overwrite, where large runs hold every chunk; binary fuse at 8 bits beats Bloom at 10 bits on FPR at the same space but costs more to build; the merge's cost per step grows with N, since `internal/lsm` compares every source on every step.

## Results

Raw output: [results-f.txt](results-f.txt), [results-ci.txt](results-ci.txt), [results-ms.txt](results-ms.txt), [results-s.txt](results-s.txt). Every lookup and probe allocated nothing. The machine's load average reached 16 during L, so its times are about 1.5× those of a quieter trial run of the same harness; ratios within one table hold.

**F, filters alone** (200,000 keys; range over three repetitions)

| Filter | Bits/key | FPR | Build ns/key | Probe ns, absent |
| --- | --- | --- | --- | --- |
| Pebble Bloom, 10 bits per key | 10.0 | 1.16% (both shapes) | 30-36 | 41-67 |
| Pebble Bloom, 16 | 16.0 | 0.25% | 28-35 | 37-53 |
| **Pebble binary fuse, 8-bit fingerprints** | **9.34** | **0.39%** | 78-126 | **22-31** |
| Pebble binary fuse, 12 | 14.0 | 0.027% | 73-87 | 25-37 |
| XXH3 of the key alone | | | | 11-19 (28 B), 15-18 (14-45 B) |

**L, lookups over N runs** (about 2 million entries; µs per lookup, runs searched per lookup in brackets)

| Index, operation | N | Real tree | No filter | Bloom 10 | Binary fuse 8 | Filters' bytes |
| --- | --- | --- | --- | --- | --- | --- |
| ci-whole, read in a written chunk | 6 | 14.2 | 12.4 (6) | 4.1 (1.06) | 3.9 (1.02) | 0.76% of runs, 0.25 keys per entry |
| | 24 | 28.9 | 31.5 (24) | **5.1 (1.18)** | 4.2 (1.09) | 0.76% |
| ci-whole, read in an unwritten chunk | 6 | 12.5 | 13.8 (6) | 0.68 (0.07) | 0.30 (0.02) | |
| | 24 | 30.4 | 30.5 (24) | **1.35 (0.19)** | 1.08 (0.09) | |
| ci-overwrite, read | 6 | 13.9 | 14.2 (6) | 15.4 (5.65) | 14.5 (5.65) | 0.57% |
| | 12 | 25.5 | 27.3 (12) | 18.4 (7.18) | 17.4 (7.15) | 0.69% |
| | 24 | 28.9 | 33.2 (24) | **19.8 (7.19)** | 20.3 (7.12) | 0.74% |
| metastore, get attr (hit) | 6 | 2.7 | 3.0 (4.39) | 3.3 (1.04) | 3.3 (1.01) | 2.85% (Bloom), 1.51 keys per entry |
| | 24 | 3.4 | 4.6 (22.0) | 4.4 (1.19) | 3.9 (1.08) | 2.85% |
| metastore, get absent | 6 | 4.3 | 5.2 (6) | 0.94 (0.07) | 0.77 (0.02) | |
| | 12 | 5.9 | 5.9 (12) | 1.14 (0.14) | 0.88 (0.05) | |
| | 24 | 10.3 | 13.3 (24) | **1.89 (0.22)** | 1.35 (0.09) | |

**S, scans over N runs** (MetaStore index; ns)

| N | Seek | Step |
| --- | --- | --- |
| 6 | 4,174 | 95 |
| 12 | 5,228 | 157 |
| 24 | 6,617 | **360** |

## Findings per engine

Pebble at [`a7b8321871e3`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/) unless marked v2.1.7; RocksDB at [`v11.8.1`](https://github.com/facebook/rocksdb/blob/v11.8.1/).

| Practice | Pebble | RocksDB | For rock-solid |
| --- | --- | --- | --- |
| A filter per table for point lookups | `Get` is `SeekPrefixGE` (`get.go:42`); each table's iterator checks its filter first (`sstable/reader_iter_single_lvl.go:1027-1055`) | A full filter per SST file, checked by `Get` and prefix seeks; `format_version` 5 and later write the cache-local Bloom filter (`include/rocksdb/table.h:739`, `util/bloom_impl.h:144`) | **Adopt now**, one filter per in-memory run: L above |
| Filter kind and size | Bloom with every probe in one 64-byte line, 10 bits per key a "good value" at about 1% (`sstable/tablefilters/bloom/bloom.go:15-34, 130-133`), the RocksDB full-filter format (`bloom.go:118-128`); master adds binary fuse filters (`sstable/tablefilters/binaryfuse`, 8-bit: 9.0 to 9.5 bits per key at 0.39%, `binaryfuse/simulation.md`), which need every key's hash at once and about 24 B per hash to build (`binaryfuse/filter.go:21-30`); `AdaptivePolicy` caps a filter's size (`bloom/adaptive_policy.go:15-20`) | Bloom at 9.9 bits per key for about 1% (`include/rocksdb/filter_policy.h:136-167`); Ribbon saves about 30% of the space for 3 to 4× the build CPU and 3× its temporary memory, used below `bloom_before_level` (`filter_policy.h:169-210`) | **Bloom, 10 bits per key**: probes, not false positives, dominate a filtered lookup over 24 runs (F, L); our runs are not split into 64 MB tables, so a static filter's build memory is unbounded, while a Bloom filter sized from the known entry count is built as the run streams out |
| Bits per level (Monkey) | `DBTableFilterPolicyProgressive`: 16 bits per key on L0-L3, 14, 10, 8 on L6; binary fuse 16 to 4 (`options.go:1550-1623`), from Dayan et al., "Optimal Bloom Filters and Adaptive Merging for LSM-Trees" | Per-level choice through `bloom_before_level` | Not now: uniform 10 bits; at 1.2% a false positive adds 0.2 searches over 24 runs |
| No filter on the last level | Reads skip L6 filters unless `UseL6Filters` (`options.go:238-245`, `file_cache.go:696-703`), since L6 filter blocks are large and would be read into the cache | `optimize_filters_for_hits` writes none for the bottommost level (`include/rocksdb/advanced_options.h:802-816`, `db/version_set.cc:3147-3153`) | **Not for us**: our filters are in memory, the oldest run holds most keys, and misses are common (negative directory lookups, unwritten blocks) |
| Whole-key and prefix filters | Filters hold `Split(key)`, the key without its MVCC suffix (`internal/base/comparer.go:160-191`); only `SeekPrefixGE` and `Get` use them, never `SeekLT` | `prefix_extractor` (`include/rocksdb/options.h:289`) with `whole_key_filtering` (`table.h:662`) puts whole keys, prefixes or both in one filter (`table/block_based/full_filter_block.cc:67-77`); prefix seeks and `SeekForPrev` check it (`block_based_table_iterator.cc:86, 209-221`; `.h:438-458`) | **Adopt RocksDB's form**: an extractor per index; the chunk index holds only `(pool, h, chunk_id)`, the MetaStore whole keys and the inode prefix; a floor inside a prefix, as `SeekForPrev` in prefix mode, consults the prefix filter |
| Floors through a prefix | CockroachDB's MVCC timestamps sort newest first, so "newest version at or below t" is a `SeekPrefixGE` that the filter serves (`cockroachkvs/cockroachkvs.go:298, 354, 869-870`) | `SeekForPrev` checks the prefix filter | Floor inside a prefix, keys unchanged; no need to invert the block number |
| Partitioned filters and two-level indexes | Two-level index blocks (`sstable/reader_iter_two_lvl.go`); one filter per table | `partition_filters` with `kTwoLevelIndexSearch`, partitions of `metadata_block_size` 4 KiB (`table.h:310, 423, 519`) | Not while runs are in memory; per-piece partitions when runs come from the log |
| Block cache | Clock-PRO, 4 shards per CPU, blocks in memory from C `malloc` outside the Go heap, keyed by (handle, file, offset) (`internal/cache/cache.go:21-60`); one reader per missing block (`internal/cache/read_shard.go:17-30`) | `HyperClockCache`, lock-free, "generally recommended over LRUCache" (`include/rocksdb/cache.h:371-392`); index and filter blocks outside the cache by default (`table.h:238`), pinning options (`table.h:267, 290`) | Phase 8: one CLOCK cache per index owner, off-heap, no shards or locks since one loop owns the index; fence pointers and filters pinned outside it |
| Data block size, restart interval | 4 KiB blocks, a restart every 16 (`internal/base/options.go:11-12`) | 4 KiB, restarts 16, index restarts 1 (`table.h:400, 413, 416`); optional hash index inside data blocks for point gets (`table.h:362`) | **Confirm** restarts every 16. In memory one block per piece is fine; a run read from the log needs 4 to 32 KiB blocks so a miss reads one, not a 1 MiB piece |
| Columnar blocks | `TableFormatPebblev5` and later (`sstable/format.go:39-41`): columns per key part (`sstable/colblk/block.go:5-14`), a key schema that puts the MVCC suffix in its own column (`colblk/data_block.go:28-50`), prefix compression against a bundle's first key so iteration is "largely stateless" and reverse steps are cheap (`colblk/prefix_bytes.go:24-39, 180-191`) | Row blocks only | **Confirm** row blocks for now, with Pebble's actual reasons on record: faster seeks and steps, and CockroachDB's key schema. With filters a lookup searches about one run, so in-block search is no longer the main cost |
| Merging iterator | A heap of child iterators (`merging_iter.go:75-85`; master's `merging_iter_v2.go:21-35` adds range-deletion slabs) | `MergingIterator`, a heap | **Adopt a heap**: our linear merge costs 95 ns a step over 6 runs and 360 over 24 (S) |
| Forward seeks | `TrySeekUsingNext`: a seek above the last one may step instead of seek (`internal/base/iterator.go:272-300`), turned off after a filter excluded the table (`reader_iter_single_lvl.go:1030-1039`) | A seek inside the current data block skips the index seek (`block_based_table_iterator.cc:100-125`); `max_sequential_skip_in_iterations` (`advanced_options.h:749`) | **Adopt** for chunk reads: one reader per chunk, filters probed once with its prefix, ascending blocks served by stepping forward |
| Key range per table | `levelIter` positions by each file's bounds (`level_iter.go:21-70`) | `FilePicker` skips a file whose smallest or largest key excludes the key (`db/version_set.cc:360-395`) | **Adopt**: each run keeps its last key beside its first |
| Table/file cache | `fileCache` of open readers (`file_cache.go:60-75`) | `TableCache` | Not for us: no files |

## Conclusions

1. **Every run gets a filter now, not at the block frontend.** Over 24 runs a negative MetaStore lookup drops from 10 to 13 µs to 1.4 to 1.9 µs, searching 0.1 to 0.2 runs; a chunk-index read of a chunk written whole from 29 to 32 µs to 4 to 5 µs, 1.1 runs; a read of an unwritten chunk to about 1 µs. Filters cost 0.76% of the chunk index's run bytes (one key per chunk, not per extent) and 2.6 to 2.9% of the MetaStore's (whole keys plus inode prefixes, 1.5 keys per entry).
2. **No cap on runs.** Six runs without filters (4.3 µs for a MetaStore miss, 12.5 to 14 µs for a chunk read) cost more than 24 with them (1.9 µs and 1 to 5 µs). Holding 24 runs' entries in six takes merging into the largest run as leveled compaction does, which rewrites entries several times as often as size tiering at the same fanout; size tiering cannot go below about 14 runs at 16 GiB.
3. **Bloom at 10 bits per key, cache-local, in RocksDB's full-filter format as Pebble writes it.** Binary fuse at 8 bits is better on paper (9.3 bits per key at 0.39% against 10 at 1.16%, probes 22 to 31 ns against 41 to 67) but saves little in L (0.1 µs on a miss), costs 2 to 4× the build CPU and needs every hash and about 24 B per key at once; a run is one object of up to 58 GiB, not a 64 MB table, so the largest merge would hold gigabytes for it. A Bloom filter sized from the entry count the flush or merge knows can be filled as the run streams out.
4. **Filters hold what each index reads by.** The chunk index's reads are floors inside one chunk's keys, so its filter holds `(pool, h, chunk_id)` and a floor inside a prefix probes it (RocksDB's `SeekForPrev` in prefix mode); the MetaStore's holds whole keys and `fs/{volume}/{inode}`. A get that finds its key gains nothing in this file model (3.4 to 4.4 µs either way), since time-ordered inode numbers make a run that lacks the key cheap to search; misses and floors are where filters pay.
5. **A block volume's chunk index still searches about 7 of 24 runs** under random overwrites: every large run holds every hot chunk, so a chunk-prefix filter rules out only the small runs (19.8 against 33.2 µs). That is a new open question for phase 8.
6. **Keep filters on the oldest run**, unlike Pebble's default reads and RocksDB's `optimize_filters_for_hits`: those save reading large filter blocks from disk, a cost in-memory filters do not have.
7. **The merge needs a heap**: a scan step costs 95 ns over 6 runs and 360 ns over 24, since every step compares every source.
8. Probing costs more than false positives: a Bloom probe is 41 to 67 ns including the hash (XXH3 of the key alone 11 to 19 ns), against 0.2 extra searches from false positives at 24 runs. Hashing once per lookup and reusing it for every run's probe, which neither engine does, is the next saving.
