# Stream overlap spike

**Question.** Can the disk log's two streams commit concurrently, so that a hot 4K commit (user writes, Raft entries) no longer waits behind a large cold record (cleaner copies, encoded shards, sorted runs), without costing the cold stream its throughput? Today a disk runs one round at a time and alternates between the streams ([On-disk format](../../docs/design.md#on-disk-format)). The [NVMe flush spike](../nvme-flush/README.md) showed that a consumer drive overlaps two identical 4K write + flush streams at unchanged latency (N7). It did not test a 4K stream next to a 1 MiB one, and a flush empties the drive's whole write cache, so the hot flush may have to wait for the cold data anyway.

**Setup.** [stream.c](stream.c) is a C harness. Each stream appends records sequentially into its own half of a 1 GiB file that was `fallocate`d and then written in full once. Every commit is an `O_DIRECT` `pwrite` of the record, then `fdatasync`, as in the NVMe flush spike. The hot stream writes 4K records in a closed loop at queue depth 1. The cold stream writes records of 64K, 256K or 1M back to back, so it always has work, as a cleaner behind on its work would. Device flush counts come from `/sys/block/<dev>/stat`. CPU per hot commit is from `getrusage(RUSAGE_THREAD)`.

- **NVMe drive (the decision):** bare metal, Linux 6.17.0-1033-oem, Intel Core Ultra 5 235U, 14 CPUs. The drive the NVMe flush spike measured: UMIS RPEYJ512MML1QWQ, 512 GB, DRAM-less PCIe 4.0, volatile write cache, FUA supported, no power-loss protection, 14 hardware queues. It is also the root disk, under ext4. Load average 0.3 before the first run and under 2 throughout; no other process flushed while idle. Two runs: [results-nvme.txt](results-nvme.txt) and [results-nvme-rerun.txt](results-nvme-rerun.txt).
- **VM stand-in:** a cloud VM (Firecracker, Linux 6.18.44, 4 vCPUs, Intel Xeon at 2.1 GHz) with a virtio block device (`vda`, ext4, volatile write cache, no FUA, one hardware queue) backed by storage on the host. A flush costs about 60 µs and 1M records saturate the device at about 730 MB/s, so it is bandwidth-bound where the NVMe drive is flush-bound. Raw output: [results-vm.txt](results-vm.txt).

```
./build.sh                          # bin/stream
DEV=nvme0n1 ./run.sh nvme           # about 2 min, writes a 1 GiB file under work/, deletes it -> results-nvme.txt
DEV=nvme0n1 ./run.sh nvme-rerun     # the second run
DEV=vda ./run.sh vm                 # the stand-in
```

| Test | What runs |
| --- | --- |
| H1 | Hot stream alone |
| H2 | Cold stream alone, per record size |
| H3 | One commit loop, rounds alternate: a cold record and its flush, then a hot record and its flush. This is today's rule. A hot request arrives as the previous one is acknowledged, so it waits for the cold round in front of it |
| H3b | One commit loop, one round holds both records: cold write, hot write, one flush |
| H4 | Two concurrent commit loops, one per stream, unpinned |
| H4b | H4 with 1M cold records, cold loop busy half of the time (a cleaner that keeps up) |
| H5 | H4 with both loops pinned to one CPU, where the NVMe flush spike saw the kernel fold flushes (N7b) |

Expected outcomes were written into the harness's checks before the first run. Figures for cold records of 64K / 256K / 1M; a range spans the two NVMe runs.

## Results (NVMe drive)

| Test | Result |
| --- | --- |
| H1: hot alone | p50 951 µs, p99 2.3 to 2.4 ms, 1,090 to 1,100 commits/s (write 331 µs, flush 215 µs), as in N2 |
| H2: cold alone | 70 / 165 to 170 / 446 MB/s; a commit takes 0.97 / 1.60 to 1.66 / 2.33 to 2.35 ms. Flush-bound up to 64K |
| H3: alternating rounds (today) | Hot p50 1,819 to 1,874 / 2,147 to 2,189 / 3,036 to 3,053 µs: a cold commit, then its own. Cold 35 to 37 / 103 / 311 to 323 MB/s |
| H3b: one round, one flush | Hot p50 1,049 to 1,164 / 1,899 to 1,953 / 2,347 to 2,391 µs, 21 to 23% below H3 at 1M (OK). The one flush takes 1.41 ms at 1M. Cold 41 to 58 / 111 to 152 / 373 to 451 MB/s |
| H4: two concurrent loops | Expected hot p50 within 1.25× of H1 next to 1M records. **Got 2,291 to 2,327 µs, 2.4× H1 (FAIL), no better than H3b**: 1,065 to 1,118 / 1,874 to 1,882 / 2,291 to 2,327 µs, 1 to 4% below H3b at 256K and 1M, and on either side of it by up to 9% at 64K. The wait is in the hot flush: p50 370 / 1,170 / 1,600 µs against 215 µs alone. Cold 46 to 48 / 146 to 149 / 328 to 434 MB/s, 73% of H2 at 1M in the first run (FAIL) and 97% in the second (OK). Hot CPU per commit 11 to 17 µs, against 28 to 116 µs in H3's shared loop |
| H4b: cold busy half the time, 1M | Hot p50 938 to 1,021 µs, near H1 (OK); hot flush p50 215 to 219 µs, as alone |
| H5: both loops on one CPU | Expected flushes to fold as in N7b. **Not reproduced: 1.00 device flush per commit (FAIL).** N7b did not fold at two threads either (1.00); folding began at four. Latency matches H4 |
| p99, every test | A tail of 7.6 to 10.8 ms appears in a different set of tests in each run: H4 at 1M (10.8 ms), H4b and H5 at 64K in the first run, H3 at 1M and H3b at all sizes in the second. Without it, p99 is 2.4 to 3.3 ms in H3b, H4 and H5, and 3.6 to 5.2 ms in H3. The tail belongs to the drive, not the scheme (compare N6's 15.8 ms outlier), so p99 does not rank the schemes |

## Results (VM stand-in)

| Test | Result |
| --- | --- |
| H1: hot alone | p50 97 µs, p99 209 µs, 9,660 commits/s (write 37 µs, flush 59 µs) |
| H2: cold alone | 372 / 610 / 734 MB/s; a 1M commit takes 1,372 µs. Bandwidth-bound |
| H3: alternating rounds | Hot p50 266 / 490 / 1,553 µs, p99 551 / 1,044 / 3,216 µs. Cold 232 / 495 / 648 MB/s |
| H3b: one round, one flush | 64K: hot p50 219 µs, 18% below H3. 1M: 1,558 µs, the same as H3, because a flush costs little here and the bytes cost a lot (FAIL by 4 µs) |
| H4: two concurrent loops | Hot p50 181 / 421 / 1,356 µs, 14× H1 at 1M (FAIL) but 13 to 32% below H3; the hot flush p50 is 773 µs against 59 µs alone. p99 2,553 µs at 1M. Cold keeps 89 to 91% of H2 (OK) |
| H4b: cold busy half the time, 1M | Hot p50 105 µs (OK), p99 1,682 µs |
| H5: both loops on one CPU | 1.00 device flush per commit (FAIL); one hardware queue, so pinning changes nothing in blk-mq. Latency matches H4 |

## Conclusions

1. **On the consumer NVMe drive, a hot commit next to a stream of 1 MiB cold records does not stay near 0.95 ms: it takes 2.3 ms at best, a 1 MiB commit's own time.** A device flush drains the whole volatile write cache, so the hot flush completes only once the cold bytes written before it are durable: hot flush p50 1.6 ms against 215 µs alone (H4), hot commit p50 2.29 to 2.33 ms against 2.33 ms for a cold 1M commit alone (H2).
2. **A second commit loop gains nothing over one round with one flush on this drive.** H4 is 1 to 4% below H3b at 256K and 1M and on either side of it at 64K. Both save the same thing over today's alternating rounds: the hot round's own flush, and the slow write that follows a flush (N2c; hot write p50 331 to 555 µs in H3, 12 to 20 µs in H3b). Hot p50 falls from 3.04 ms to 2.3 ms at 1M (−24%) and from 1.85 ms to 1.1 ms at 64K. One loop that puts both streams' records under one flush gets that saving with one round in flight, so `commit_depth` 2 does not pay on a flush-bound drive. On a bandwidth-bound device (the VM, or a drive with power-loss protection where a flush is nearly free) H4 does beat H3b, by 13% at 1M, because there the flush is cheap and the cold bytes are not.
3. **The cold bytes under a flush set hot latency.** Next to 64K cold records, hot p50 is 1.05 to 1.16 ms, within 22% of H1; next to 256K, 1.9 ms; next to 1M, 2.3 ms (H3b, H4). That is N3's curve: 0.95 ms flat to 64K, then rising with the bytes. Capping the cold bytes that share a flush with a waiting hot commit keeps the hot commit near 1 ms, but costs the cold stream throughput whenever hot work is waiting, since small cold commits are flush-bound: 70 MB/s at 64K against 446 MB/s at 1M (H2). 128K, RocksDB's margin for a small-led group, was not measured; 64K and 256K bracket it.
4. **A cleaner that keeps up leaves hot p50 at the floor.** At half duty, hot p50 is 938 to 1,021 µs next to 1M records (H4b), with the hot flush at 215 to 219 µs, as alone: half the time no cold bytes are in the cache.
5. **Two loops pinned to one CPU fold no flushes** (H5), on the NVMe drive as on the VM, and as N7b showed at two threads. Folding needs more flushes waiting than two loops ever have.
6. **The drive's p99 is noisy at about 10 ms** in some tests of every run, whichever scheme they use, so these runs cannot rank the schemes by tail latency. With that tail left out, the schemes that flush once per hot commit (H3b, H4, H5) keep p99 at 3.1 to 3.3 ms next to 1M records, against 5.2 ms for alternating rounds.

Not covered: io_uring with both chains in flight from one thread, instead of two threads with `pwrite` and `fdatasync`. Raw block devices without ext4. A 128K cold cap. Drives with power-loss protection, where a flush is nearly free and the question becomes one of bandwidth only, as on the VM.
