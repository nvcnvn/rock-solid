# Cleaner streams spike

**Question.** Three questions the [cleaner spike](../cleaner-wa/) left open about the per-disk log ([On-disk format](../../docs/design.md#on-disk-format)): what layout should the segments that hold whole chunks have, so that files deleted at random don't pay the write amplification (WA) of 64 MiB segments (Q13)? Does separating data by temperature into more than two streams, by age or by the number of times the cleaner moved it, close the gap to perfect hot/cold separation enough to be worth it now (Q14)? And what do the log's own writes cost, the Raft entries, sorted runs, compactions and manifests that share the log with chunk data, plus the chunk-index entries the cleaner causes when it moves blocks, and should they live in their own stream (Q15)?

**Setup.** The [cleaner-wa](../cleaner-wa/) simulator in Rust, extended ([src/main.rs](src/main.rs), no dependencies, its own `[workspace]`), driven by [run.py](run.py). Written against commit `676095bf` of this repository; it does not import the repo's code. Intel Xeon at 2.1 GHz, 4 vCPUs, 16 GiB, Linux 6.18.44 in a Firecracker VM, rustc 1.97.0, Python 3.13. The machine was shared with six other benchmarking agents; that changes only the `secs` field, since the simulation is deterministic.

What the simulator keeps from cleaner-wa: 4K blocks, 4 MiB chunks, cost-benefit victim choice with the net-gain guard and the age cap at one disk capacity of writes, 8 free segments kept, 4 victims per pass, copies grouped by chunk into 256-block records, 64 B record headers plus 32 B per extent and 4 B of CRC per block, header blocks dead to the cleaner. The default disk is 16 GiB of 4,096 segments of 4 MiB, the stand-in for 256 GiB of 64 MiB segments; where a 4 MiB chunk must be compared with the segment size, the disk is a real 64 GiB of 64 MiB segments, and the reserve stays 512 MiB at every segment size. Each run warms up for 4 disk capacities of writes and measures 2 windows of 1 capacity, repeated with 8 + 4 when the windows differ by more than 3%. Changes from cleaner-wa:

- **Records** hold at most 256 blocks (`max_group_commit_bytes` = 1 MiB), so a whole chunk is four records.
- **Net-gain guard** counts header blocks rounded up per record. With metadata in the log, cleaner-wa's estimate (exact bytes, not blocks) let the cleaner pick victims 98.9% live whose copy cost exactly what they freed, and it livelocked. Without metadata the guard never bound and the numbers match cleaner-wa (uniform 7.15 at 90%, hc90 4.57, whole chunks with greedy 4.47 against 4.44).
- **Streams (Q14).** Any number of open segments. `base`: user writes to stream 0, cleaner copies to 1 (the design). `moves` (F2FS multi-head logging): cleaner copies go to stream 1 + the times the block was already moved. `sepbit_gc`: cleaner copies split by age, under 4ℓ, under 16ℓ, older, where ℓ is the mean lifespan of the last 16 user segments reclaimed ([SepBIT, Wang et al., FAST 2022](https://www.usenix.org/conference/fast22/presentation/wang)). `sepbit_user`: user writes split by the lifespan of the block they overwrite (< ℓ is short-lived), copies to one stream. `sepbit`: both, 6 streams, as the paper. `oracle`: the workload's true hot set, user and cleaner streams for each temperature, with headers: the separation the others aim for, in the same model.
- **Chunk slots (Q13).** A slot segment holds whole chunks only, one chunk per slot, and is freed and TRIMmed when its last slot empties; a chunk goes to the fullest slot segment with a free slot. *slots15*: each slot holds its four records with their headers, 1,028 blocks, 15 slots after the segment header (93.75% of the segment holds data). *slots16*: a slot holds only the 1,024 data blocks, 16 per segment; the round's record headers go to the log, stay live while the chunk does and are moved by the cleaner like data.
- **Workloads.** `chunkrand`: whole chunks rewritten at random. `files`: files of N chunks on this disk deleted and rewritten whole at random, K files written at once with their chunks interleaved. `mixed`: half the chunks rewritten whole, half overwritten at random 4K, with a set share of the written bytes going to whole chunks. The others are cleaner-wa's: `uniform`, `hotcold` (hc80: 80% of writes to 20% of chunks; hc90: 90 to 10), `zipf` (θ 0.99 over blocks).
- **Metadata (Q15).** A *checkpoint* every `max_replay_bytes` of log (64 MiB, 4 MiB on the stand-in) flushes the chunk index as a sorted run of 40 B per entry and the MetaStore as a run of half the Raft bytes since the last one, writes a manifest of 17 B per used segment (`SegmentUsageSize`, [internal/disklog/manifest.go:17](../../internal/disklog/manifest.go)), scaled by 1/16 on the stand-in, then kills the previous manifest, the compaction inputs and every Raft entry written before it. Chunk-index entries: two per user write (its extent and its old record's live count) plus one per record; the cleaner adds one per moved extent, one per new record and one per victim record whose count it drops. Raft entries are a set share of the user bytes, packed into the user record when they share its stream. Segments holding live Raft entries are not victims, since records after the replay point must stay in place. Compaction is size-tiered by size class (factor 4, runs under 1 MiB, 64 KiB on the stand-in, in one class), 4 to 32 runs of a class at once, the output capped at the index's live size: one entry per live 4K-overwritten block or four per whole chunk. `meta_reserve` takes the metadata's space from the data, so that runs compare at about the same total fill (`u_total`, the time-averaged live share of the disk, metadata included).

Not modeled: the drive's own garbage collection, reads, a cleaner that runs beside the writer, erasure-coded shards and their slot sizes, snapshots, real traces.

```
cd spikes/cleaner-streams
cargo build --release
python3 run.py --jobs 4                    # Q13, Q14, Q15: 141 runs, about 45 min on the shared 4 vCPUs; writes results.txt
python3 run.py S --jobs 4 --out results2.txt   # supplementary runs S1-S4
python3 run.py --render results.txt Q13    # tables again from saved RUN lines
target/release/cleaner-streams wl=hotcold hot_w=0.9 hot_f=0.1 u=0.9 sep=sepbit_gc   # one run; keys in Cfg::parse
```

Raw output: [results.txt](results.txt) (Q13 to Q15), [results2.txt](results2.txt) (S1 to S4), [results3.txt](results3.txt) (S4 again, with the run count in its error). results.txt was produced before the slot path counted Raft entries; that changes only C3's slots-with-metadata row, which S3 replaces. Its A3 table shows ERR for "own" cells that were never run; `--render` prints them as "-".

## Results

### Q13: whole chunks

| Test | Result |
| --- | --- |
| A1: whole chunks rewritten at random, 64 GiB disk, 80 / 90 / 95% full | 64 MiB segments, cost-benefit 2.65 / **4.93** / 9.04 (greedy 2.48 / 4.47 / 7.93); 16 MiB 2.02 / 3.24 / 4.64; 4 MiB 1.17 / 1.52 / 1.98. **Slots: 1.004 at every fill, no cleaning.** slots15 runs out of space at 95% ([OK] as expected); slots16 does not |
| A2: whole files deleted at random, 90% full, 64 MiB segments | One writer: 3.33 / 1.67 / 1.08 for 4 / 16 / 64 chunks per file on the disk. **8 writers interleaved: 4.45 / 4.29 / 3.98**, close to random single chunks (4.93) |
| A3: half the data in whole chunks, half in random 4K overwrites, 64 MiB segments | Half the writes whole: log 3.02 / **6.10** / 13.2; whole chunks in their own stream 6.17 at 90%; **slots16 1.53 / 2.53 / 5.33**. A fifth of the writes whole: log 6.19, own stream 6.08, slots16 3.43 at 90% |
| A1, A3: slot fragmentation | Slot segments stay 99.9% full (`slot_occ`); all slots are one size, so any free slot fits |

Segment counts change what a manifest costs. Every manifest carries the whole usage table, 17 B per used segment, and one is written per `max_replay_bytes` (64 MiB): at 64 MiB segments that is 272 KiB per TiB of disk (0.4% of the log per TiB), at 16 MiB 1.1 MiB (1.7%), at 4 MiB 4.25 MiB (6.6%), and recovery reads 16× more segment headers.

### Q14: more streams

Cost-benefit with the age cap; WA and its ratio to the design's two streams. *Header-free oracle* is cleaner-wa's analytic two-pool separation.

| workload | u | base2 | moves3 | moves4 | sepbit-user3 | sepbit-gc4 | sepbit6 | oracle4 | header-free oracle |
|---|---|---|---|---|---|---|---|---|---|
| uniform | 0.80 | 3.35 | 3.54 (1.06) | 3.58 (1.07) | 3.68 (1.10) | 3.44 (1.03) | 3.89 (1.16) | - | - |
| uniform | 0.90 | 7.15 | 7.69 (1.07) | 8.14 (1.14) | 7.62 (1.07) | 9.09 (1.27) | 9.65 (1.35) | - | - |
| uniform | 0.95 | 17.29 | 18.34 (1.06) | 19.49 (1.13) | 17.69 (1.02) | **28.21 (1.63)** | 30.60 (1.77) | - | - |
| hc80 | 0.80 | 2.92 | 2.79 (0.96) | 2.75 (0.94) | 3.13 (1.07) | 2.74 (0.94) | 2.90 (0.99) | 2.69 (0.92) | 2.00 |
| hc80 | 0.90 | 6.15 | 5.99 (0.97) | 5.94 (0.97) | 6.56 (1.07) | 5.80 (0.94) | 6.29 (1.02) | 5.53 (0.90) | 3.57 |
| hc80 | 0.95 | 15.05 | 14.84 (0.99) | 14.81 (0.98) | 15.68 (1.04) | 17.72 (1.18) | 19.09 (1.27) | 13.12 (0.87) | 6.76 |
| hc90 | 0.80 | 2.18 | 1.92 (0.88) | 1.88 (0.86) | 2.35 (1.08) | 1.92 (0.88) | 1.98 (0.91) | 1.87 (0.86) | 1.48 |
| hc90 | 0.90 | 4.57 | 3.90 (0.85) | 3.58 (0.78) | 4.84 (1.06) | **3.32 (0.73)** | 3.55 (0.78) | 3.20 (0.70) | 2.34 |
| hc90 | 0.95 | 10.96 | 9.88 (0.90) | 9.23 (0.84) | 11.62 (1.06) | **7.64 (0.70)** | 8.33 (0.76) | 7.03 (0.64) | 4.11 |
| zipf | 0.80 | 2.45 | 2.44 (1.00) | 2.43 (0.99) | 2.37 (0.97) | 2.41 (0.98) | 2.33 (0.95) | - | - |
| zipf | 0.90 | 4.18 | 4.30 (1.03) | 4.38 (1.05) | 4.01 (0.96) | 4.32 (1.03) | 4.10 (0.98) | - | - |
| zipf | 0.95 | 8.61 | 8.52 (0.99) | 8.77 (1.02) | 8.46 (0.98) | 9.78 (1.14) | 8.53 (0.99) | - | - |

| Test | Result |
| --- | --- |
| B: gap to separation with headers | **Perfect separation in the same model is only 1.08 to 1.56× below two streams** (hc80 1.08 / 1.11 / 1.15, hc90 1.17 / 1.43 / 1.56), not cleaner-wa's 1.5 to 2.9×: headers and the reserve cost the same under any separation |
| B: best policy per workload | hc90: SepBIT-style age classes for cleaner output close 84 to 97% of that gap (−12 to −30%). hc80: 13 to 78%, under 6%. zipf: within ±5%, nothing helps |
| B: what it costs | **Uniform overwrites: +3 to +63%** (sepbit-gc4), +6 to +14% (moves). hc80 and zipf at 95%: +14 to +18% |
| B: user-side separation alone | sepbit-user3 is worse than two streams on every hot/cold and uniform row (+2 to +10%) |
| S1: greedy victim choice at 95% | Age classes are neutral: uniform 14.78 → 14.91, hc90 14.99 → 15.18. The uniform regression comes from cost-benefit's age term, which ranks the old classes' segments high however full they are; the hot/cold gain needs it too |

### Q15: the log's own writes

| Test | Result |
| --- | --- |
| C1: block volume, uniform 4K, chunk index in the log, about equal total fill, 80 / 90 / 95% | No metadata 3.35 / 7.15 / 17.3. **Runs in the cold stream: 4.27 / 11.0 / 49.6. Runs in their own stream: 3.86 / 8.04 / 26.4** |
| C1: hc90 | No metadata 2.18 / 4.57 / 11.0; runs cold 2.73 / 6.07 / 20.1; **runs own 2.37 / 4.71 / 12.5** |
| C1: where the 90% uniform WA goes, runs in their own stream | Chunk data 7.54 (7.15 without metadata: the runs' space and their transient compaction copies raise the effective fill), runs flushed 0.085, compacted 0.40, manifests 0.006, runs moved by the cleaner 0.004 blocks per user block |
| C1: chunk-index entries per user block, user / cleaner | Uniform 2.06 / 2.55, 6.51, 24.1 at 80 / 90 / 95% (own stream): **the cleaner causes 55%, 76% and 92% of the index's entries**; hc90 2.06 / 0.98, 2.68, 8.25 |
| C1: metadata not counted in the fill (data at 90%, total 91.8%) | 16.6 uniform, 9.3 hc90: the write limit must count the index's runs, as a disk-fill check does |
| S2: small-file stand-in, Raft entries 25% of the data bytes, MetaStore 2% of the disk, equal fill | 80%: Raft with chunk writes and runs in the cold stream 7.45; Raft own 6.91; runs own 6.40; **both own 6.03**. 90%: **only both own completes, 12.4**; the other three run out of space while a large MetaStore compaction holds its inputs and its output |
| C3, S3: file pool of whole chunks, Raft 1% of data, 90% | Log 4.93 → 5.53 with metadata; **slots 1.004 → 1.035** (Raft 0.010, runs 0.020 blocks per user block) |
| S4: the design's compaction rule, buckets of runs adjacent in age | **Ran out of space at 80% full: 13,025 chunk-index runs holding 18.5% of the disk** ([FAIL] as a check that the run count stays bounded). Checkpoint flushes alternate between small (no cleaning in the interval) and large (a cleaner pass's moves), so no four adjacent runs fall within 0.5 to 1.5× of their average, and none ever merge. Size classes that may take any runs of a class (C1) end with 6 to 16 runs |

## Conclusions

1. **Whole chunks go to chunk slots, not to the log** (Q13). Random deletes of whole chunks cost 4.9 at 90% full and 9.0 at 95% in 64 MiB segments, and slots cost nothing at any fill (1.004, the record headers). A pool that is half whole chunks also cleans its small writes for less, 2.5 instead of 6.1 at 90%, because the whole chunks no longer take spare space. Whole files are no cheaper than single chunks once writers interleave (4.0 to 4.5 with 8 writers), and placement groups spread each file over every disk, so a disk's log sees whole-file deletes as random chunk deletes.
2. **A slot holds data only, 16 per 64 MiB segment, and each round's record stays in the log** (Q13). Slots that carry their own record headers fit 15 per segment, and a pool of whole chunks then cannot pass about 93% full (A1 at 95%). Records in the log cost 0.4% of the chunk's size there, live while the chunk is, and the cleaner moves them like data. Smaller segments for the whole disk are not the answer: 4 MiB segments still cost 1.5 at 90%, and the usage table every manifest carries grows 16-fold, 6.6% more log writes per TiB of disk.
3. **No more temperature streams for chunk data now** (Q14). No policy simulated is a safe default: the one that closes most of the gap on hot/cold overwrites (SepBIT's age classes for cleaner output, −27 to −30% on hc90 at 90 to 95%) costs up to 63% more on uniform overwrites and 18% more on hc80 at 95%, and zipf gains nothing. The gap is also smaller than cleaner-wa's header-free oracle suggested: 1.08 to 1.56×. The stream tag can carry more streams later.
4. **Sorted runs, compaction output and manifests get their own stream** (Q15). Mixed with cleaner copies they raise block-volume WA by 25 to 54% at 80 to 90% full and nearly triple it at 95%; in their own stream by 3 to 15% at 80 to 90%. Runs die whole, so their segments empty without copies.
5. **Raft entries stay with new chunk copies** (Q15). A Raft stream of its own saved 6% more at 80% in the 25%-Raft stand-in, but a round writes one record to one stream, so it would split a commit's chunk write and Raft entries into two rounds and two flushes.
6. **The cleaner causes most of the chunk index's writes near full**: 55 / 76 / 92% of its entries at 80 / 90 / 95% for uniform overwrites. Index runs add 0.2 to 1.0 blocks per user block, about 6% of the total at 90%; the larger cost is the space they take and the copies compaction holds until a manifest frees its inputs.
7. **Compaction needs room**, two findings for the LSM tree: the design's rule of buckets over adjacent runs never merged when flush sizes alternated, and a MetaStore compaction writing 2% of the disk while its inputs stay live ran a 90% full disk out of free segments, where the 8-segment reserve was far smaller than the run.
