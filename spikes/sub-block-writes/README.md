# Sub-block writes spike

**Question.** The ChunkStore API takes byte ranges, but records, checksums and the chunk index work in whole 4 KiB blocks. Where must a write that covers part of a block be merged into a whole block, and against which version, so that two writers of one block, pipelined writes and primary changes never lose or resurrect bytes? And what do small fsync'd appends and small files cost in the log under block read-modify-write, against byte-granular alternatives (patch entries, short blocks)? It feeds the [sub-block writes study](../../studies/sub-block-writes/README.md).

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker, 4 vCPUs, shared; nothing here is timed), Go 1.27.0. Three models, no I/O:

- [merge_test.go](merge_test.go): one chunk of 4 blocks, clients sending byte-range writes of 1 to 300 bytes (M9: up to 3 blocks), at random offsets, so many are unaligned and some cross a block boundary. The store keeps whole blocks only. A strategy turns each write into whole blocks: the primary merging against its newest ordered version, committed or pending (*overlay*); the primary merging against the committed version only (*committed*); the client reading the committed blocks and sending merged whole blocks (*client RMW*), optionally with compare-and-set on the chunk version it read. Commits happen in version order at random times; a failover ends the primary's tenure and peering keeps a prefix of its pending versions per chunk (or, in M7, any subset); in M8 the new primary serves writes before it holds a version peering kept, which recovery then writes as whole blocks. An oracle applies every acknowledged write's bytes in version order, and the committed image must equal it after every step. 500 seeds of 400 steps per test.
- [appends_test.go](appends_test.go): one disk log of 1,024 segments of 256 blocks (a scaled 1 GiB stand-in; the [cleaner spike](../cleaner-wa/README.md) showed the segment count, not size, is what matters), a hot and a cold stream, a greedy cleaner keeping 8 segments free, liveness counted in bytes, records split across segments as in that spike, and the record layout of `internal/disklog` (checked against `disklog.LayoutOf` in [layout_test.go](layout_test.go)). Background data is a block volume at fill u, overwritten at random 4 KiB. Append files grow by 100 (or 1,000) bytes per fsync, each fsync one flush, and are deleted at 1 MiB. Variants: *rmw* rewrites the touched block whole; *patch* logs a 35-byte patch entry plus the bytes packed after the record's blocks, and folds the block into a whole-block write when it completes (*patch8* also after 8 patches); *patchhdr* keeps the patch bytes in the header blocks; *short* rewrites the tail block as a short block holding only the bytes up to the end of file. A1: one append per round and no other writes (an fsync at queue depth 1 alone on the disk). A2: rounds of 16 items, 1 or 8 appends from different files plus 15 or 8 random overwrites, against a baseline of the overwrites alone, 3 seeds; *marginal* is the extra device bytes per round over the baseline, per append. 6 disk capacities of warm-up, 6 measured.
- [space_test.go](space_test.go): raw bytes a small file costs per pool, from the design's figures (a block per copy, a 33-byte entry and 4-byte CRC, the encoding rule of [When chunks are encoded](../../docs/design.md#when-chunks-are-encoded)), and MetaStore memory if file data were inline in the inode (133 B per file replica in runs).

Stand-ins: the models have no network, no latency and no disk errors; the append model's cleaner is greedy, not cost-benefit with an age cap, and the hot stream holds append and overwrite records together, as the design's does.

```
./run.sh     # go test -v ./... -> results.txt, about 80 s
```

Raw output: [results.txt](results.txt).

## Results

| Test | Result |
| --- | --- |
| L0: record layout against `disklog.LayoutOf` | Equal for a one-block write, 16 writes, a 100-byte blob, and 15 writes plus a blob |
| M1: primary merges against its newest ordered version, two clients | 0 of 500 seeds wrong, 70,689 writes |
| M2: primary merges against committed only, one client, 4 writes pipelined | **500 of 500 seeds lose bytes**: the second write's merged block carries the first one's old bytes |
| M3: same, two clients, one write each in flight | 500 of 500 lose bytes |
| M4: client reads committed blocks and sends merged whole blocks, two clients | 500 of 500 lose bytes |
| M5, M5b: client RMW with compare-and-set on the chunk version it read | 0 wrong; **0.64 retries per acknowledged write with two clients, 1.28 with four**, and half M1's throughput, on a 4-block chunk. The version a client read is the committed one, so its CAS fails whenever any write to the chunk is pending |
| M6: overlay, 8 pipelined writes per client, 3,670 failovers, peering drops a suffix of pending versions | 0 of 500 wrong |
| M7: same, peering drops any subset | **500 of 500 wrong**: a kept version carries bytes of a dropped one it was merged on |
| M8: new primary merges before it holds a version peering kept | **458 of 500 wrong**: recovery's whole blocks overwrite the new writes' bytes, or the new writes are merged on the stale block |
| M9: as M6, writes up to 3 blocks long | 0 of 500 wrong |
| A1: 100-byte append + fsync alone, u = 0.8 / 0.9 | **rmw 8,288 / 8,301 device bytes per append (83×)**; patch and patch8 the same, since 100 patch bytes still take a whole data block; **patchhdr 4,196 (42×)**; short the same as rmw. Cleaning 0 to 0.2% |
| A1: 1,000-byte appends, u = 0.8 | rmw 9,185 B (9.2×); patchhdr 5,096 (5.1×) |
| A2: 1 append + 15 overwrites per round | Baseline WA 3.29 / 7.42 at u = 0.8 / 0.9. **rmw, patch, patch8 and short identical: about 20,000 / 34,000 marginal device bytes per append (200× / 340×)**, since the append's block, short or patch, takes a block of its own; patchhdr 1,900 / 5,700 |
| A2: 8 appends + 8 overwrites per round, u = 0.8 | rmw 7,924 B per append (79×), short 5,770 (58×), patch 2,530 (25×), patch8 3,484 (35×), **patchhdr 581 (5.8×)** |
| A2: same, u = 0.9 | rmw 9,599 (96×), short 6,408 (64×), patch 5,216 (52×), patch8 5,965 (60×), patchhdr 2,098 (21×) |
| S1: rep3, raw bytes per file | 100 B: 12,399 (124×); 1 KiB: 12,399 (12.1×); 1.5 KiB: 12,399 (8.1×, 2.7× the pool's 3×); 2 KiB: 6.1×; 4 KiB: 3.03×; 6 KiB: 4.0×. Flushed alone, 24,576 B written for any file up to 4 KiB. A short block: 1.5 KiB → 4,725 (3.08×) |
| S1: ec4-2 (3 copies until encoded) | Files up to 8 KiB stay copies: **1.5 KiB costs 8.1×, 5.4× the pool's 1.5×**; 16 KiB encodes at 1.51×; 24 KiB at 2.0× (shards rounded to blocks) |
| S1: ec16-2 | Files up to 24 KiB stay copies, 2.7× the pool's 1.125× at 16 and 24 KiB |
| S2: index memory per small file replica | MetaStore 133 B in runs; chunk index 108 B raw (chunk entry 44 B, one extent 64 B), before prefix compression |
| S3: inline data in the inode | 60 B inline: 193 B per file replica (1.5×); 1.5 KiB: 1,669 B (12.5×), **so a 16 GiB budget holds 3.4 million files at three replicas instead of 43 million**; 2 KiB: 16.4× |

## Conclusions

1. **The merge belongs at the chunk primary, against the newest version it has ordered, pending ones included.** Merging against the committed version loses bytes as soon as two writes to one block are in flight, from one client (M2) or two (M3); a client-side merge loses bytes without compare-and-set (M4) and with it retries on every concurrent write to the chunk, since the version is per chunk (M5). The primary's overlay is correct under two writers, pipelining and failovers (M1, M6, M9).
2. **Peering may drop only a suffix of a chunk's pending versions.** A merged version contains the bytes of the version below it, so keeping it while dropping that one resurrects the dropped write's bytes (M7). It holds when every member logs a chunk's writes in version order, since durability is a prefix of each log; the rule must be stated and modelled.
3. **A new primary merges into a block only once it holds the version peering kept for it.** Otherwise recovery's whole blocks and the new writes overwrite each other (M8), the case Ceph covers by holding writes to missing or degraded objects.
4. **At queue depth 1, a fsync'd small append costs two blocks per copy (83× for 100 bytes) with any block-granular scheme, and one block only if its bytes travel in the header** (A1). Patch entries in the data area help only when several share a round (A2: 79× → 25× with 8 appenders per round, nothing with one).
5. **A short-lived block written into the shared log costs about the log's write amplification, not one block**: one 100-byte append per round of 15 overwrites added 4.9 blocks per append at u = 0.8 and 8.4 at 0.9 (A2), because its space is reclaimed by cleaning segments that also hold live data. With 8 appenders per round they dominate their segments and the cost falls to 1.9 to 2.3 blocks.
6. **Small files cost a block per copy, and in an erasure-coded pool they stay copies**: a 1.5 KiB file takes 8.1× its size in rep3 and in ec4-2 alike, 5.4× what ec4-2 promises (S1). Inline data in the inode would cost the MetaStore, whose runs live in memory, 12.5× per file at 1.5 KiB (S3). Short blocks bring a 1.5 KiB file to 3.1× and help appends somewhat (A2: 79× → 58×).

Not covered: append files that live on (deleting them at 1 MiB frees their completed blocks, so A1's cleaning is near zero; a kept log file leaves one live block per 41 tail rewrites in each segment), latency of the primary's base read, the network cost of sending merged blocks, real disks, the cost-benefit cleaner, cleaning of header-resident patch bytes beyond byte liveness, and read-side merging of patches.
