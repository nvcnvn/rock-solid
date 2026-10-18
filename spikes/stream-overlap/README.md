# Stream overlap spike

**Question.** Can the disk log's two streams commit concurrently, so that a hot 4K commit (user writes, Raft entries) no longer waits behind a large cold record (cleaner copies, encoded shards, sorted runs), without costing the cold stream its throughput? Today a disk runs one round at a time and alternates between the streams ([On-disk format](../../docs/design.md#on-disk-format)). The [NVMe flush spike](../nvme-flush/README.md) showed that a consumer drive overlaps two identical 4K write + flush streams at unchanged latency (N7). It did not test a 4K stream next to a 1 MiB one, and a flush empties the drive's whole write cache, so the hot flush may have to wait for the cold data anyway.

**Setup.** [stream.c](stream.c) is a C harness. Each stream appends records sequentially into its own half of a 1 GiB file that was `fallocate`d and then written in full once. Every commit is an `O_DIRECT` `pwrite` of the record, then `fdatasync`, as in the NVMe flush spike. The hot stream writes 4K records in a closed loop at queue depth 1. The cold stream writes records of 64K, 256K or 1M back to back, so it always has work, as a cleaner behind on its work would. Device flush counts come from `/sys/block/<dev>/stat`. CPU per hot commit is from `getrusage(RUSAGE_THREAD)`.

**Stand-in.** The only results so far are from a cloud VM (Firecracker, Linux 6.18.44, 4 vCPUs, Intel Xeon at 2.1 GHz). Its disk is a virtio block device (`vda`, ext4, `write_cache` = write back, no FUA, one hardware queue) backed by storage on the host. There, a flush costs about 60 µs and 1M records saturate the device at about 730 to 810 MB/s, so the VM is bandwidth-bound where the consumer NVMe drive is flush-bound (0.95 ms per 4K commit). **The VM's numbers cannot settle the question.** They show the harness works and how a bandwidth-bound device behaves. The decision needs `results-nvme.txt` from the drive the NVMe flush spike measured.

```
./build.sh                     # bin/stream
DEV=nvme0n1 ./run.sh nvme      # about 2 min, writes a 1 GiB file under work/, deletes it -> results-nvme.txt
DEV=vda ./run.sh vm            # the stand-in run below
```

Raw output: [results-vm.txt](results-vm.txt).

| Test | What runs |
| --- | --- |
| H1 | Hot stream alone |
| H2 | Cold stream alone, per record size |
| H3 | One commit loop, rounds alternate: a cold record and its flush, then a hot record and its flush. This is today's rule. A hot request arrives as the previous one is acknowledged, so it waits for the cold round in front of it |
| H3b | One commit loop, one round holds both records: cold write, hot write, one flush |
| H4 | Two concurrent commit loops, one per stream, unpinned |
| H4b | H4 with 1M cold records, cold loop busy half of the time (a cleaner that keeps up) |
| H5 | H4 with both loops pinned to one CPU, where the NVMe flush spike saw the kernel fold flushes (N7b) |

## Results (VM stand-in)

Expected outcomes were written into the harness's checks before the first run.

| Test | Result |
| --- | --- |
| H1: hot alone | p50 97 µs, p99 209 µs, 9,660 commits/s (write 37 µs, flush 59 µs) |
| H2: cold alone | 64K 372 MB/s, 256K 610 MB/s, 1M 734 MB/s; a 1M commit takes 1,372 µs. The device is bandwidth-bound |
| H3: alternating rounds | Hot p50 266 / 490 / 1,553 µs and p99 551 / 1,044 / 3,216 µs next to 64K / 256K / 1M cold records. Cold 232 / 495 / 648 MB/s |
| H3b: one round, one flush | 64K: hot p50 219 µs, 18% below H3. 1M: 1,558 µs, the same as H3, because a flush costs little on this device and the bytes cost a lot (check FAIL by 4 µs) |
| H4: two concurrent loops | Expected hot p50 within 1.25× of H1 next to 1M records. **Got 1,356 µs, 14× H1 (FAIL).** The wait is in the hot flush: p50 773 µs against 59 µs alone. Still better than H3 at every size: hot p50 181 / 421 / 1,356 µs (−32 / −14 / −13%), p99 2,553 µs at 1M (−21%). Cold keeps 89 to 91% of H2's throughput (OK). Hot CPU per commit 20 to 38 µs, against 45 to 122 µs in H3's shared loop |
| H4b: cold busy half the time, 1M | Hot p50 105 µs, near H1 (OK), but **p99 1,682 µs**: a hot commit that lands during a cold record waits for most of it |
| H5: both loops on one CPU | Expected flushes to fold as in N7b. **Not reproduced: 1.00 device flush per commit (FAIL).** Latency matches H4. Cause not found; with one hardware queue all CPUs already share one flush queue on this VM, so pinning changes nothing in blk-mq here |

## Conclusions (stand-in only; rerun on NVMe before the design uses them)

1. **Two concurrent commit loops beat alternating rounds, but they do not shield the hot stream from the cold one.** On a bandwidth-bound device, a hot commit waits for the cold bytes queued ahead of it, whichever loop sent them: 1,356 µs next to 1M records in H4, against 1,553 µs in H3 and 97 µs alone.
2. **The size of the cold records matters more than concurrency.** Next to 64K cold records, hot p50 is 181 µs; next to 1M records it is 1,356 µs (H4). Capping cold records lower while hot commits are waiting, or pacing the cold stream, protects hot latency more than a second loop does. That would be a separate `max_group_commit_bytes` for the cold stream.
3. **A cleaner that keeps up still sets hot p99.** At half duty the hot p50 recovers, but p99 is 1.7 ms (H4b). The p99 cost is set by the largest record in flight, not by how busy the cold stream is.
4. **On the NVMe drive the question is still open.** There a flush costs 215 µs, and part of its cost is charged to the next write (N2c). H4 against H1 and H3 there decides whether a second commit loop per disk is worth the changes it needs in recovery: one torn record per stream, and explicit durability ordering between the streams.

Not covered: io_uring with both chains in flight from one thread, instead of two threads with `pwrite` and `fdatasync`. Raw block devices without ext4. Drives with power-loss protection, where a flush is nearly free and the question becomes one of bandwidth only, as on this VM.
