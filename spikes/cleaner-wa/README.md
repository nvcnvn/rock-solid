# Cleaner write-amplification spike

**Question.** What write amplification (WA) does the per-disk log ([ADR 0003](../../docs/decisions/0003-one-log-per-disk.md), [On-disk format](../../docs/design.md#on-disk-format)) and its cost-benefit cleaner have for block volumes with random overwrites as a disk fills toward the pool's 90% write limit? What segment size should it use, and does the write limit need a larger reserve?

**Setup.** A trace-free simulation in Rust ([src/main.rs](src/main.rs), no dependencies, its own `[workspace]`), driven by [run.py](run.py). Intel Core Ultra 5 235U, 14 cores, 15 GiB, Linux 6.17, rustc 1.99.1, Python 3.12.3. Other agents were running on the machine; this only affects the C7 micro-benchmark.

The model:

- A disk is S fixed-size segments of B 4K blocks. Live data is u × capacity, in 4 MiB chunks of 1024 blocks (u counts live-version blocks plus snapshot-pinned blocks). The disk is first filled sequentially, then overwritten for 6× its capacity (warm-up), then measured over 4 windows of 1× capacity each. A run is *steady* when the windows agree within 3%; an unsteady run is repeated with 3× the warm-up and 2× the windows.
- **Records.** A group commit appends one record: header blocks, then data. A header holds 64 B, plus 32 B per entry (chunk, version, block range), plus a 4 B CRC32C per block. Default record: 16 random 4K writes (64K of data, 1 header block). A record that does not fit the open segment is split, and each part gets its own header. Header blocks are dead for cleaning purposes: the cleaner writes new headers for the blocks it moves and never copies old ones.
- **Streams.** In two-stream mode, user writes go to stream 1 and cleaner copies to stream 2. In one-stream mode both share one open segment.
- **Cleaner.** It runs whenever fewer than 8 segments are free. Each pass picks 4 victims from the sealed segments by *greedy* (fewest live blocks) or *cost-benefit* ((1−u)·age/(1+u), Rosenblum & Ousterhout 1992, where age = now − youngest block's user-write time and moved blocks keep their write time). It copies their live blocks sorted by (chunk, version, block) into 256-block records, then frees and TRIMs the victims. *cb2cap* is cost-benefit with age capped at one disk capacity of user writes. Every policy skips victims whose free space is no larger than the header cost of copying them, because a cleaner without that guard livelocked (C2).
- **WA** = (user blocks + header blocks + cleaner-copied blocks) / user blocks.
- **Workloads.**
  - *uniform*: random aligned 4K overwrites.
  - *hc80* and *hc90*: hot/cold at chunk granularity. 80% of writes go to 20% of the chunks, or 90% to 10%; the hot chunks are scattered.
  - *zipf*: Zipf θ = 0.99 over blocks (YCSB's generator, Gray et al. 1994), with ranks shuffled so no chunk is hot as a whole.
  - *fifo*: whole 4 MiB chunks rewritten oldest-first.
  - *chunkrand*: whole chunks rewritten at random (file deletes and new files).
- Default disk: 16 GiB, 4096 × 4 MiB segments. This is a scaled stand-in for a 256 GiB disk with 64 MiB segments, or a 1 TiB disk with 256 MiB segments. C3 shows that the segment count, not the segment size, is what matters.
- **Analytic check.** Under greedy (or FIFO) cleaning with uniform random writes, the victim's live fraction x solves x = e^−(1−x)/u, and WA = 1/(1−x). This is the same as Desnoyers' closed form A = α/(α + W(−α·e^−α)) with α = 1/u ([SYSTOR 2012](https://www.ccs.neu.edu/home/pjd/papers/pjd-systor12.pdf), eq. 3–4). It predicts WA 1.26 at u = 0.50, 2.69 at 0.80, 5.18 at 0.90 and 10.2 at 0.95, and gives WA ≤ 2, 3 and 5 at u ≤ 0.721, 0.822 and 0.896.

Not modeled:

- the SSD's own flash translation layer (FTL);
- Raft entries, LSM sorted runs and manifests, which since ADR 0003 also live in the log and add writes and cleaning of their own;
- erasure-coded shards;
- a background or rate-limited cleaner (here cleaning is synchronous);
- reads and latency.

```
cd spikes/cleaner-wa
cargo build --release
python3 run.py                    # all tests, 276 runs, ~35 min on 11 threads, writes results.txt
python3 run.py V C1 --jobs 4      # a subset
target/release/cleaner-wa wl=zipf u=0.9 pol=cb streams=2 age_cap=1   # one run; keys in Cfg::parse
target/release/cleaner-wa bench   # copy + CRC32C + index-update micro-benchmark
```

Raw output: [results.txt](results.txt).

## Results

Expected outcomes were written into `run.py` as checks before the full run. A smoke test of four runs (V at 0.50 and 0.95, hc90 and zipf at 0.90) came first.

| Test | Result |
| --- | --- |
| V: greedy uniform, no headers, vs the analytic model | Expected within 3% at u = 0.50. **Got 1.256 against 1.257 (−0.1%). Every u from 0.50 to 0.95 is within 0.9%** ([OK]) |
| C1: uniform, greedy vs cost-benefit, 1 vs 2 streams | Expected cost-benefit within 15% of greedy, and WA about 10 at 0.95. **WA is 15.1 at 0.95 for greedy, 17.3 for cost-benefit with 2 streams** ([FAIL] on "about 10"). Cost-benefit is 3–14% *worse* than greedy, and 2 streams change nothing for uniform writes. The surplus over the model comes from headers (C4) |
| C2: skewed, greedy vs cost-benefit + 2 streams | Expected ≥ 20% lower WA for u ≥ 0.80. hc90: 23–40% lower; zipf: 37–45% lower ([OK]). **hc80: only 8–13% lower at 0.80–0.90 and 0.3% at 0.95** ([FAIL]). The best policy stays 1.5–2.9× above the two-pool oracle at 0.80–0.95, though the oracle has no headers, and headers alone add 16–52% at those fills (C4) |
| C2: uncapped cost-benefit + age-sorted copies | **Livelocked on hc90 at 0.90**: 1,000 cleaning passes without gaining a free segment (`ERR` in results.txt). So the net-gain guard alone is not enough. In an earlier run without that guard, this case and snapshot + hc80 at 0.95 were still running after 44 and 30 minutes when stopped; their siblings took 0.5–2 minutes. Capping age fixes it: 9.27 (unsteady) → 3.86 on hc90 |
| C2: age cap | Never hurts: cb2cap/cb2 ≤ 1.001 for every workload, u and copy order ([OK]) |
| C2: copy order (chunk / age / victim), u = 0.90 | With the cap: hc80 6.17 / 6.13 / 6.23; hc90 4.62 / 3.86 / 4.07; zipf 4.23 / 4.57 / 4.56. Chunk order is between 7% better (zipf) and 13% worse (hc90) than victim order; no order wins everywhere |
| C3a: segment size at a fixed 1024 segments | Expected ≤ 5% spread. **1, 4, 16, 64 and 256 MiB give the same WA: cost-benefit within 1.4% (u = 0.80/0.90) and 3.0% (u = 0.95)** ([OK]); greedy within 1% and 5.4% |
| C3b: segment count, with the 8-segment reserve | Expected > 10% higher WA at 256 segments than at 16384. **u = 0.95, cost-benefit: 70.2 at 256 segments (3.1% reserved), 20.3 at 1024, 17.3 at 4096, 16.7 at 16384** ([OK]) |
| C4: record size and headers | Expected 4K records to double WA. **4K records (QD1 sync): 10.98 against 5.64 without headers at u = 0.90 (1.95×)**. 64K records: +27% at 0.90 (7.15) and **+52% at 0.95 (17.30 against 11.40)**. 1M records: 15.40 at 0.95. 8-byte entries instead of 32 at 64K: 14.41 at 0.95 |
| C4: why headers cost more than their bytes | Header blocks are dead space spread evenly over every segment, so they act as lost capacity. The model evaluated at u_eff/(1−h), with h = `hdr_per_user`/`wa` (the header share of written blocks) taken from the greedy C1 lines in results.txt, gives 3.04 / 6.43 / 15.16 against the simulated 3.12 / 6.48 / 15.10 at 0.80 / 0.90 / 0.95. This was computed by hand from those fields; run.py does not print it. Cleaner records cost 0.0121 header blocks per copied block, and copies dominate near full |
| C5: 20% of the data pinned by a snapshot, then overwritten | Expected lower WA than without a snapshot at the same total u. Got 5–9% lower: 6.72 against 7.15 at 0.90 and 15.86 against 17.30 at 0.95 (cost-benefit, 2 streams) ([OK]). Frozen data behaves as cold data. The cost is that a snapshot raises u itself |
| C6: highest u for a target WA (64K records) | Uniform greedy: WA ≤ 2 / 3 / 5 at u ≤ 0.676 / 0.790 / 0.873. Uniform cost-benefit: 0.656 / 0.774 / 0.861. hc80 cost-benefit: 0.693 / 0.804 / 0.879. zipf cost-benefit: 0.710 / 0.847 / 0.915 |
| C6f: file pools, whole-chunk writes | Expected 1.00 for fifo. **fifo: 1.002 at every u** ([OK]). **Random whole-chunk rewrites with 64 MiB segments: 2.47 / 4.44 / 7.88 at 0.80 / 0.90 / 0.95 (greedy)**. With 16 MiB: 1.87 / 2.97 / 4.04. With 4 MiB: 1.08 / 1.40 / 1.70, above 1.0 because a 4 MiB chunk plus its header does not fit a 4 MiB segment |
| C7: cleaner cost | Cost-benefit, 2 streams, uniform: 2.26 / 6.01 / 16.04 copied blocks per user block at 0.80 / 0.90 / 0.95. Reading whole victim segments instead of only live blocks adds about 1 block read per user block. Copy plus CRC32C is 1.1 µs per 4K block and an in-memory index update 0.12 µs, so **about 2.8 / 7.5 / 20 µs of CPU per user 4K write** go to the cleaner (order of magnitude: shared machine, and the real index update goes through the LSM tree) |

### WA by utilization and policy

Uniform random 4K overwrites (C1). Every column except the first two includes 64K-record headers.

| u | model, greedy, no headers | sim, greedy, 1 stream, no headers | greedy, 1 stream | greedy, 2 streams | cost-benefit, 1 stream | cost-benefit, 2 streams |
|---|---|---|---|---|---|---|
| 0.50 | 1.26 | 1.26 | 1.37 | 1.37 | 1.37 | 1.41 |
| 0.70 | 1.88 | 1.88 | 2.11 | 2.11 | 2.14 | 2.23 |
| 0.80 | 2.69 | 2.71 | 3.12 | 3.11 | 3.23 | 3.35 |
| 0.85 | 3.52 | 3.54 | 4.18 | 4.17 | 4.41 | 4.54 |
| 0.90 | 5.18 | 5.24 | 6.48 | 6.46 | 6.95 | 7.15 |
| 0.95 | 10.17 | 10.45 | **15.10** | 14.78 | 16.43 | **17.30** |

Skewed workloads (C2). *Oracle* is two perfectly separated pools with greedy cleaning inside each and the spare space split optimally; Desnoyers 2012 analyses this separation.

| workload | u | greedy, 1 stream | greedy, 2 streams | cost-benefit, 1 stream | cost-benefit, 2 streams | cost-benefit, 2 streams, age cap | oracle |
|---|---|---|---|---|---|---|---|
| hc80 | 0.80 | 3.38 | 3.37 | 2.99 | 2.93 | 2.92 | 2.00 |
| hc80 | 0.90 | 6.73 | 6.70 | 6.34 | 6.16 | 6.17 | 3.57 |
| hc80 | 0.95 | 15.26 | 14.99 | 15.82 | **15.21** | 15.22 | 6.76 |
| hc90 | 0.80 | 3.72 | 3.67 | 2.38 | 2.25 | 2.21 | 1.48 |
| hc90 | 0.90 | 7.29 | 7.19 | 4.89 | 4.64 | 4.62 | 2.34 |
| hc90 | 0.95 | 15.57 | 15.91 | 14.12 | 12.03 | 12.02 | 4.11 |
| zipf | 0.80 | 3.92 | 3.82 | 2.64 | 2.49 | 2.47 | – |
| zipf | 0.90 | 7.73 | 6.29 | 4.75 | 4.24 | 4.23 | – |
| zipf | 0.95 | 16.88 | 11.99 | 12.07 | 9.26 | 9.25 | – |

At 0.50–0.85 (full grid in results.txt), cost-benefit with 2 streams stays below greedy for every skewed workload, by 3% (hc80, 0.50) up to 41% (zipf, 0.85).

Segment size and segment count (C3), uniform. Cells are greedy, 1 stream / cost-benefit, 2 streams.

| segment, fixed 1024 segments | u = 0.80 | u = 0.90 | u = 0.95 |
|---|---|---|---|
| 1 MiB | 3.18 / 3.39 | 6.93 / 7.56 | 18.28 / 20.10 |
| 4 MiB | 3.20 / 3.44 | 6.86 / 7.61 | 17.35 / 20.26 |
| 16 MiB | 3.20 / 3.44 | 6.90 / 7.64 | 17.83 / 20.71 |
| 64 MiB | 3.20 / 3.44 | 6.91 / 7.65 | 17.86 / 20.68 |
| 256 MiB | 3.20 / 3.43 | 6.91 / 7.64 | 17.89 / 20.70 |

| segments per disk (8 kept free) | reserve | u = 0.80 | u = 0.90 | u = 0.95 |
|---|---|---|---|---|
| 256 | 3.12% | 3.53 / 3.83 | 9.21 / 10.54 | **49.64 / 70.18** |
| 1024 | 0.78% | 3.20 / 3.44 | 6.86 / 7.61 | 17.35 / 20.26 |
| 4096 | 0.20% | 3.12 / 3.35 | 6.48 / 7.15 | 15.10 / 17.30 |
| 16384 | 0.05% | 3.10 / 3.33 | 6.39 / 7.04 | 14.57 / 16.65 |

Segments per disk for real sizes, and the metadata they need at 32 B per segment:

| segment | 256 GiB disk | 1 TiB disk | 4 TiB disk |
|---|---|---|---|
| 16 MiB | 16384 segments, 0.5 MiB | 65536, 2 MiB | 262144, 8 MiB |
| 64 MiB | 4096, 0.125 MiB | 16384, 0.5 MiB | 65536, 2 MiB |
| 256 MiB | 1024, 0.03 MiB | 4096, 0.125 MiB | 16384, 0.5 MiB |
| 1 GiB | 256 | 1024 | 4096 |

Record size (C4), uniform, cost-benefit, 2 streams. Header blocks per user block are in parentheses; they include the cleaner's own headers.

| record | u = 0.80 | u = 0.90 | u = 0.95 |
|---|---|---|---|
| no headers | 2.88 | 5.64 | 11.40 |
| 4K (QD1 sync) | 6.38 (1.05) | **10.98 (1.11)** | 22.12 (1.24) |
| 64K | 3.35 (0.09) | 7.15 (0.14) | **17.30 (0.26)** |
| 1M | 3.06 (0.04) | 6.46 (0.08) | 15.40 (0.18) |
| 64K, 8 B entries | 3.27 (0.07) | 6.66 (0.09) | 14.41 (0.13) |

Snapshots (C5). u is the total live data including pinned blocks, so the volume itself holds u/1.2. Cells are cost-benefit, 2 streams; the uniform column with no snapshot is from C1.

| u total | uniform, 20% pinned | uniform, no snapshot | hc80, 20% pinned | hc80, no snapshot |
|---|---|---|---|---|
| 0.80 | 3.18 | 3.35 | 2.76 | 2.93 |
| 0.90 | 6.72 | 7.15 | 5.73 | 6.16 |
| 0.95 | 15.86 | 17.30 | 13.85 | 15.21 |

File pools (C6f), whole 4 MiB chunk writes, 1024 segments. Cells are greedy, 1 stream / cost-benefit, 2 streams.

| workload | segment | u = 0.80 | u = 0.90 | u = 0.95 |
|---|---|---|---|---|
| fifo (oldest rewritten first) | 64 MiB | – / 1.002 | – / 1.002 | – / 1.002 |
| random whole-chunk rewrite | 4 MiB | 1.08 / 1.26 | 1.40 / 1.64 | 1.70 / 2.08 |
| random whole-chunk rewrite | 16 MiB | 1.87 / 2.08 | 2.97 / 3.28 | 4.04 / 4.71 |
| random whole-chunk rewrite | 64 MiB | 2.47 / 2.65 | **4.44 / 4.91** | **7.88 / 8.99** |

Notes on the raw output. 14 runs were unsteady at first and were repeated with a longer warm-up; they are marked `UNSTEADY-RETRIED` in results.txt. Two runs stayed unsteady:

- hc90, cost-benefit with age-sorted copies at 0.90: it livelocked on the retry, and the 9.27 above is from its unsteady first run.
- chunkrand, 4 MiB segments, cost-benefit at 0.95: a 4 GiB disk with only 1024 chunks, whose windows were 2.04–2.11.

## Conclusions

1. **The 95% write limit is too high for pools of block volumes.** With random 4K overwrites, the format as designed (64K records, 32 B header entries) reaches WA 15–17 at 95% full for uniform writes and 9–15 for skewed ones. That is 93–94% of the disk's write bandwidth, endurance and cleaner CPU (about 20 µs per user write) spent on cleaning. At 90% it is 6.5–7.2 for uniform and 4.2–6.2 for skewed with cost-benefit; at 80% it is 3.1–3.4 and 2.3–2.9 (C1, C2, C6). WA stays ≤ 5 only up to 86–87% full for uniform writes and 88–92% for skewed ones. Make the write limit a per-pool setting with defaults by content:
   - **block-volume pools:** warn at 80%, fail new writes with `ENOSPC` at 90%;
   - **file pools:** keep 85% / 95%.

   The limit applies per disk, not as a pool average, because placement leaves individual disks above the pool's fill.
2. **Two streams with cost-benefit are worth keeping, but only with two guards, and they are not enough on their own.**
   - **Benefit:** on skewed overwrites they cut WA by 36–45% against greedy for hc90 and zipf at u = 0.80–0.90, and by 8–13% for hc80 (C2). For uniform writes they cost 3–14% more than greedy (C1).
   - **Guard 1, net gain:** a victim must free more space than copying it costs, header blocks included.
   - **Guard 2, age cap:** age is capped, here at one disk capacity of user writes. LFS's uncapped age term keeps choosing old, nearly full segments such as frozen snapshot data. Without both guards, two configurations ran for 30+ minutes against 0.5–2 minutes for their siblings. With the net-gain guard but no cap, age-sorted copies on hc90 still livelocked. The cap never made WA worse (ratio ≤ 1.001).
   - **Not enough:** the best policy stays 1.5–2.9× above perfect hot/cold separation (the header-free oracle) at 80–95%, so better temperature separation is where the remaining gain is.
   - Grouping copied blocks by chunk is within −7% to +13% of victim order (no order wins everywhere), so keep it for defragmentation.
3. **Keep 64 MiB segments as the default, and require at least 4096 segments per disk.** At a fixed segment count, WA does not depend on segment size from 1 to 256 MiB (≤ 3% spread for cost-benefit, ≤ 5.4% for greedy, C3a). What matters is the free-segment reserve as a fraction of the disk. A reserve of 8 segments costs nothing at 4096+ segments (0.2%), but on a 256-segment disk it raises WA at 95% from 17 to 70 (C3b). So segment = min(64 MiB, disk / 4096), rounded down to a power of two: 64 MiB for disks of 256 GiB and more, 32 MiB for 128 GiB. Per-segment metadata is 0.5 MiB per TiB at 64 MiB. Larger segments align better with the SSD's internal garbage-collection unit:
   - stacked logs with mismatched segment sizes make the lower log clean more ([Yang et al., INFLOW 2014](https://www.usenix.org/conference/inflow14/workshop-program/presentation/yang));
   - F2FS groups segments into sections to match the FTL's GC unit (Lee et al., FAST 2015);
   - ZNS exposes that unit as zones (Bjørling et al., ATC 2021).

   This simulation can't put a number on the alignment benefit.
4. **Header blocks cost far more than their bytes near full, so the header format must be compact.** Header blocks are dead space in every segment, so they shrink the usable capacity. At 95%, 64K records raise WA from 11.4 to 17.3 (+52%). The model at u/(1−h) reproduces greedy within 3%. The cleaner's own records cost 1.2 header blocks per 100 copied blocks, because blocks moved under random overwrites are scattered and each needs its own 32 B entry. 8 B entries bring 17.3 down to 14.4 at 95% (C4). QD1 sync 4K writes pay one header block per data block, which doubles WA (10.98 against 5.64 at 90%) and fills half of every user segment with headers.
5. **Snapshots don't make cleaning worse at a given fill; they raise the fill.** Pinned frozen data is perfectly cold: at the same total u, WA is 5–9% lower than without a snapshot (C5). But snapshotting 20% of a 75% full disk and overwriting it brings the disk to 90%, where WA is 6.7. Count pinned blocks toward the write limit, as the design already does.
6. **File pools are cheap only when deletes follow write order, so random deletes need smaller segments.** Whole chunks rewritten oldest-first give WA 1.002 at any fill. Random whole-chunk rewrites (deleting old files at random) with 64 MiB segments give WA 4.4 at 90% and 7.9 at 95%: 16 chunks per segment already behave like random small writes. 16 MiB segments give 3.0 / 4.0 and 4 MiB give 1.4 / 1.7 (C6f). A segment that holds one chunk record must also fit that record's header blocks: a 4 MiB segment can't hold 4 MiB of data plus its header. The segment size for the shard/whole-chunk stream is a format question, listed below.
7. **Cleaner CPU is small at moderate fill and large near full.** 2.3 / 6.0 / 16 copied blocks per user block at 80 / 90 / 95%. At about 1.2 µs per copied block (copy plus CRC32C plus an in-memory index update), that is about 2.8 / 7.5 / 20 µs per user 4K write, before the index's own LSM writes (C7). Reading only live blocks saves one segment-block read per user block compared with reading whole segments.

### New open questions

- FTL write amplification of consumer SSDs under this log: 16, 64 and 256 MiB segments written sequentially and TRIMmed whole. Measure with a drive's host-writes and NAND-writes SMART counters. This decides whether larger segments lower the drive's own WA.
- Temperature separation beyond two streams. Separating cleaner output by age or by number of moves (as F2FS's multi-head logging or SepBIT, Wang et al. FAST 2022, do) could close part of the gap to the oracle.
- A compact header encoding for cleaner records and small records, and whether QD1 4K writes can avoid one header block per data block.
- The segment size, or chunk-aligned slots with per-chunk TRIM, for the stream that holds whole chunks and encoded shards, so that random file deletes don't pay 64 MiB-segment WA.
- Write amplification of the Raft entries, LSM sorted runs and compaction that now share the log, including the index updates the cleaner causes by moving blocks.
- When block and file pools share disks, which write limit applies to a disk.

### Limits

- No SSD FTL model: the WA here is host-side only, and the drive's own WA multiplies it.
- Synthetic workloads: no traces, no phase changes, no guest discards. Hot/cold is at chunk granularity and Zipf at block granularity.
- One disk, one static snapshot, no erasure-coded shards.
- The cleaner runs synchronously with a fixed 8-segment reserve and 4-victim batches.
- The header encoding is this spike's assumption.
- Index and Raft writes in the log are not counted.
- C7's CPU figures come from one micro-benchmark on a shared machine.
