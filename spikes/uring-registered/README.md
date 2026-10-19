# io_uring registered buffers and files spike

**Question.** The default disk driver keeps its I/O buffers in an off-heap slab and drives io_uring with raw syscalls. Does registering the slab once (`IORING_REGISTER_BUFFERS`, then `IORING_OP_READ_FIXED`/`WRITE_FIXED`) and the disk descriptors (`IORING_REGISTER_FILES`, `IOSQE_FIXED_FILE`) cut the CPU per 4 KiB I/O at queue depths 1 and 32 enough to justify what registration costs: pinned memory, the memory-lock limit, registration time and size limits?

**Setup.** gcc 13.3, no liburing: [uring.c](uring.c) sets up the ring and calls `io_uring_setup`, `io_uring_enter` and `io_uring_register` directly, as the Go driver will. Written against commit `676095bf56fdcfc9a0afce42ab45103137501d7f`. One ring per process with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN`; every I/O is 4 KiB at a random aligned offset into a random 4 KiB slot of a 64 MiB slab (registered as one iovec when the buffer is fixed); QD I/Os stay in flight, and each `io_uring_enter` submits the refills and waits for one completion. CPU per I/O is the process's user + system time over the I/Os done (`getrusage`), which includes any io-wq worker threads. Neither kernel has `IRQ_TIME_ACCOUNTING`, so interrupt time lands on whichever task it interrupts, and on no one when the CPU is idle.

It ran on two hosts:

- **VM**: Linux 6.18.44 in a Firecracker VM with 4 vCPUs, shared with six other agents running benchmarks (load average 5 to 9). Wall-clock IOPS there are noise, and queue depth 1 on its virtio disk, where each I/O also costs a VM exit, is noisy in CPU too. Raw output: [results.txt](results.txt).
- **Bare metal**: Linux 6.17.0-1033-oem (Ubuntu) on a laptop, Intel Core Ultra 5 235U (12 cores, 14 threads, `powersave` governor, the process not pinned to a core). Its root disk is a DRAM-less consumer NVMe drive (UMIS AM6C1, sysfs model `RPEYJ512MML1QWQ`), ext4, 14 hardware queues, scheduler `none`, `nvme.poll_queues=0`. Raw output: [results-laptop.txt](results-laptop.txt).

The stand-ins:

- **N**: `IORING_OP_NOP` with `IORING_NOP_FILE` on a plain or fixed descriptor and `IORING_NOP_FIXED_BUFFER`: the submission path alone.
- **P**: buffered reads of a 256 MiB file held in the page cache.
- **Z**: `O_DIRECT` reads and writes on a 2 GiB zram device of zero pages (lz4), created with `hot_add` for the run and removed after it: a block device that does almost nothing per I/O. io_uring cannot issue to it inline (see below), so every Z I/O runs in an io-wq worker thread; it is not a stand-in for an NVMe drive's path through io_uring.
- **V**: `O_DIRECT` reads and writes on a 1 GiB file on the ext4 root disk: virtio-blk in the VM, the NVMe drive on bare metal. On the drive, a 4 KiB read at QD 1 takes 61 µs and a write 14 µs (the drive's write cache); QD 32 reaches 340,000 to 370,000 reads and 75,000 to 325,000 writes per second, the writes swinging from one 2 s run to the next.
- **R**: registration time and limits, as root (`CAP_IPC_LOCK`) and as `nobody` with an 8 MiB `RLIMIT_MEMLOCK`.

Each I/O row runs the four variants (plain, fixed file, fixed buffer, both) back to back in an order rotated every rep, 2 s each, 6 reps. Polled completions (`IORING_SETUP_IOPOLL`) were not tested: the drive has no poll queues (`nvme.poll_queues=0`, `/sys/block/nvme0n1/queue/io_poll` 0), and giving it some takes `nvme.poll_queues=N` on the kernel command line and a reboot, since the `nvme` module holds the root disk.

```
./build.sh                                # bin/uring
sudo modprobe zram                        # if /sys/class/zram-control is missing
sudo python3 run.py > results-laptop.txt  # about 10 minutes; needs root for zram; writes 1.3 GiB under work/
python3 summarize.py results-laptop.txt   # medians of all four variants as a markdown table
```

`run.py` writes the kernel, the CPU, whether it runs virtualized, the disk under `work/` and its polling setup into the first line of its output. Both results files predate that and carry a fixed description of the VM; for [results-laptop.txt](results-laptop.txt) it is wrong, and the bare-metal host above is the one it ran on.

Expected before the run: fixed buffers save the page pinning and unpinning of `O_DIRECT` (a few hundred ns per I/O) and nothing on buffered reads; fixed files save one `fget`/`fput` pair (tens of ns).

## Results

CPU µs per I/O, medians of 6 reps, VM / bare metal. A saving is the difference of medians; "k/6" counts the reps in which the variant cost less than plain in the same rep.

| Test | Plain | Fixed buffer against plain | Fixed buffer and file against plain |
| --- | --- | --- | --- |
| N, NOP, QD 1 | 0.222 / 0.134 | +0.01 / +0.00 | +0.00 / −0.003 (6/6) |
| N, NOP, QD 32 | 0.044 / 0.027 | +0.01 / +0.00 | −0.005 (6/6) / −0.001 (5/6) |
| P, buffered read from cache, QD 1 | 1.647 / 0.769 | −0.10 / +0.00 | −0.04 / −0.01 |
| P, buffered read from cache, QD 32 | 1.151 / 0.655 | +0.15 / +0.00 | +0.04 / −0.00 |
| Z, zram `O_DIRECT` read, QD 1 | 4.318 / 3.952 | −0.74 (−17%) / −0.05 (−1%) | −0.93 (−22%) / −0.04 (−1%) |
| Z, zram `O_DIRECT` read, QD 32 | 3.877 / 1.641 | **−1.12 (−29%), 6/6 / −0.27 (−17%), 6/6** | −0.98 (−25%), 6/6 / −0.27 (−17%), 6/6 |
| Z, zram `O_DIRECT` write, QD 1 | 5.291 / 4.026 | −1.69 (−32%) / −0.09 (−2%) | −1.41 (−27%) / −0.06 (−1%) |
| Z, zram `O_DIRECT` write, QD 32 | 4.301 / 1.897 | **−1.13 (−26%), 6/6 / −0.29 (−15%), 6/6** | −1.15 (−27%), 5/6 / −0.30 (−16%), 6/6 |
| V, disk `O_DIRECT` read, QD 1 | 18.86 / 3.708 | −0.78 (−4%), 3/6 / −0.21 (−6%), 4/6 | −1.11 (−6%), 3/6 / **−0.29 (−8%), 5/6** |
| V, disk `O_DIRECT` read, QD 32 | 3.375 / 1.883 | −0.52 (−15%), 6/6 / **−0.11 (−6%), 6/6** | −0.61 (−18%), 6/6 / **−0.10 (−5%), 6/6** |
| V, disk `O_DIRECT` write, QD 1 | 21.61 / 4.717 | −1.38 (−6%), 4/6 / −0.07 (−1%), 4/6 | −0.90 (−4%), 5/6 / **−0.24 (−5%), 6/6** |
| V, disk `O_DIRECT` write, QD 32 | 4.005 / 1.625 | −0.42 (−11%), 5/6 / +0.04 (+2%), 3/6 | −0.73 (−18%), 6/6 / **−0.09 (−6%), 5/6** |

A fixed file alone saves 4 to 9 ns on NOP, lower than plain in 5 or 6 of 6 reps on both hosts, and nothing measurable on any real I/O. On bare metal the spread of plain across reps is 0.46 µs for V reads and 0.70 µs for V writes at QD 1, and 0.05 and 0.27 µs at QD 32.

| Test | Result |
| --- | --- |
| R1, register a slab already touched | VM: 64 MiB 2.2 ms, 1 GiB 34 ms, 2 GiB (two 1 GiB iovecs) 139 ms. Bare metal: 1.5 ms, 17 ms, 75 ms. So 0.06 to 0.26 µs per 4 KiB page pinned. Unregistering 1 GiB: 7 ms / 5 ms |
| R2, register memory never touched | VM: 64 MiB 55 ms, 1 GiB 2.8 s. Bare metal: 12 ms, 180 ms. Registration faults every page in. A slab touched before use, as the design already requires, pays only R1 |
| R3, size limits | One iovec of 1025 MiB: `EFAULT` (at most 1 GiB per buffer, `io_uring/rsrc.c:90`). 16,384 iovecs of 4 KiB: registered in 8.6 ms / 4.9 ms; 16,385: `EINVAL` (`IORING_MAX_REG_BUFFERS` = 1 << 14, `rsrc.c:35`). So one ring can register at most 16 TiB, as up to 16,384 buffers of at most 1 GiB each |
| R4, memory-lock limit | **As `nobody` with an 8 MiB `RLIMIT_MEMLOCK`: 4 MiB registers, 64 MiB fails with `ENOMEM`**, on both hosts. Pinned pages are charged to the user's `locked_vm` against `RLIMIT_MEMLOCK` unless the process has `CAP_IPC_LOCK` (`io_uring/io_uring.c:3839`, `rsrc.c:39-56`). Each ring charges its registration again, except pages of a huge page already charged (`headpage_already_acct`, `rsrc.c:629`) |

Why, in the kernel source at [v6.18](https://github.com/torvalds/linux/tree/v6.18) and [v6.17](https://github.com/torvalds/linux/tree/v6.17):

- An `O_DIRECT` read or write from a user address builds a user iterator, and the block layer pins every page with `iov_iter_extract_pages` and unpins it at completion (`block/bio.c:1339-1354` in v6.18, `BIO_PAGE_PINNED`). A fixed buffer is a `bvec` iterator over pages pinned at registration, which `bio_iov_bvec_set` hands to the bio as is (`block/bio.c:1347-1348`). That pin and unpin of one page is what a fixed buffer saves: about 0.1 µs on bare metal.
- A buffered read copies into the user buffer either way, so a fixed buffer saves nothing there (P).
- A plain descriptor costs `fget` and `fput` per request (`io_uring/io_uring.c:2017-2019` in v6.18), a fixed one a table lookup and a reference on the table node (`io_uring.c:1998-2010`): a few ns, visible only in N.
- zram declares `BLK_FEAT_STABLE_WRITES | BLK_FEAT_SYNCHRONOUS` but not `BLK_FEAT_NOWAIT` (`drivers/block/zram/zram_drv.c:2685-2686` in v6.17, the same in v6.18), so its device file never gets `FMODE_NOWAIT` (`block/bdev.c:985-986`). io_uring then returns `-EAGAIN` before issuing (`io_uring/rw.c:929-931`, `__io_read`), cannot arm a poll on a block device, and queues the request to io-wq (`io_queue_async`, `io_uring/io_uring.c:2002-2009`). At QD 1 every zram I/O costs a handoff to a worker and a wakeup back (CPU ≈ wall ≈ 4 µs on bare metal); at QD 32 several workers run at once (1.6 CPU-seconds per second at a million I/Os per second). blk-mq devices such as NVMe get `BLK_FEAT_NOWAIT` (`block/blk-mq.c:4396`) and ext4 opens files with `FMODE_NOWAIT` (`fs/ext4/file.c:926`), so V is issued inline by the submitting task: a separate V read at QD 32 on bare metal, checked in `/proc/<pid>/task`, ran as a single thread.
- At QD 1 on the drive the task sleeps for the 14 to 61 µs the drive takes, so the completion interrupt mostly hits an idle CPU and is not counted; at QD 32 the task is running when it lands, and it is.

## Conclusions

1. **The disk driver registers its slab, for a 5 to 8% saving rather than a quarter.** On a real NVMe drive on bare metal, a fixed buffer saves about 0.1 µs of CPU per 4 KiB `O_DIRECT` I/O at QD 32 (−5 to −6% of 1.6 to 1.9 µs, lower in 5 or 6 of 6 reps with fixed files) and 0.2 to 0.3 µs at QD 1 (−5 to −8% of 3.7 to 4.7 µs). That is one page's pin and unpin. The 0.6 to 1.4 µs measured in the VM was inflated like every other cost there: plain zram at QD 32 cost 2.4 times as much in the VM as on bare metal. The saving is still worth taking, because it is paid per I/O and registration once per slab: the I/O buffer handle (slot and generation in the driver's slab) already names an address inside a registered region, so `READ_FIXED`/`WRITE_FIXED` take the same handle plus the region's buffer index.
2. **zram does not stand in for NVMe under io_uring.** zram cannot be issued without blocking, so io_uring hands every zram I/O to an io-wq worker. Its QD 1 cost is mostly the worker handoff, and at QD 32 it runs on several worker threads at once, where a fixed buffer saved 0.27 µs per I/O against 0.1 µs on the drive. A near-null device for this question needs `BLK_FEAT_NOWAIT`: `null_blk` or a real drive.
3. **What registration costs is a fixed slab, not a fixed ring.** Registration pins the slab: 17 to 70 ms per GiB already touched (R1), at most 1 GiB per region and 16,384 regions per ring (R3). The ring itself is laid out as before; a slab that grows registers its new region into a sparse table (`IORING_RSRC_REGISTER_SPARSE`, `IORING_REGISTER_BUFFERS_UPDATE`) without touching the old ones. Each loop's ring registers only that loop's slab, so the memory is charged once.
4. **The daemon pod needs `IPC_LOCK` or a memory-lock limit as large as its slabs**, as RDMA already does: with an 8 MiB limit, a 64 MiB registration fails (R4). Without either, the driver falls back to plain `READ`/`WRITE` on the same buffers and reports it, so registration is an optimization, never a condition for running.
5. **Fixed files are optional**: they save 4 to 9 ns per request, under the noise of every real I/O measured. The driver registers its disks' descriptors because it holds few of them for its whole life, but nothing depends on it.
6. **At queue depth 1 the saving is real but small.** The VM could not resolve it: each I/O cost 18 to 23 µs there, mostly the VM exit and interrupt, and the shared host added more noise than the saving. On the drive a QD 1 I/O costs 3.4 to 4.9 µs of CPU, and a fixed buffer with a fixed file is cheaper than plain in 5 of 6 reps for reads and 6 of 6 for writes, by 0.29 and 0.24 µs. A fixed buffer alone is cheaper in 4 of 6, by 0.21 and 0.07 µs, inside the 0.46 to 0.70 µs that plain varies across reps. So the direction holds at QD 1; this run sizes the saving only to "a few hundred ns".
7. **Polled completions remain unmeasured.** `IORING_SETUP_IOPOLL` needs NVMe poll queues, which neither host had. Polling replaces the interrupt and the wakeup, not the page pin, so a fixed buffer should save the same 0.1 to 0.3 µs, a larger share of a smaller total; a run on a host booted with `nvme.poll_queues` checks it.
