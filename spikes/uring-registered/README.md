# io_uring registered buffers and files spike

**Question.** The default disk driver keeps its I/O buffers in an off-heap slab and drives io_uring with raw syscalls. Does registering the slab once (`IORING_REGISTER_BUFFERS`, then `IORING_OP_READ_FIXED`/`WRITE_FIXED`) and the disk descriptors (`IORING_REGISTER_FILES`, `IOSQE_FIXED_FILE`) cut the CPU per 4 KiB I/O at queue depths 1 and 32 enough to justify what registration costs: pinned memory, the memory-lock limit, registration time and size limits?

**Setup.** Linux 6.18.44 in a Firecracker VM, 4 vCPUs, gcc 13.3, no liburing: [uring.c](uring.c) sets up the ring and calls `io_uring_setup`, `io_uring_enter` and `io_uring_register` directly, as the Go driver will. Written against commit `676095bf56fdcfc9a0afce42ab45103137501d7f`. One ring per process with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN`; every I/O is 4 KiB at a random aligned offset into a random 4 KiB slot of a 64 MiB slab (registered as one iovec when the buffer is fixed); QD I/Os stay in flight, and each `io_uring_enter` submits the refills and waits for one completion. CPU per I/O is the process's user + system time over the I/Os done (`getrusage`); the kernel has no `IRQ_TIME_ACCOUNTING`, so interrupt time lands on whichever task it interrupts. There is no NVMe drive, `null_blk` or `brd` in this kernel, so the stand-ins are:

- **N**: `IORING_OP_NOP` with `IORING_NOP_FILE` on a plain or fixed descriptor and `IORING_NOP_FIXED_BUFFER`: the submission path alone.
- **P**: buffered reads of a 256 MiB file held in the page cache.
- **Z**: `O_DIRECT` reads and writes on a 2 GiB zram device of zero pages (lz4), created with `hot_add` for the run and removed after it: a block device that does almost nothing per I/O, the closest stand-in here for an NVMe drive's kernel path.
- **V**: `O_DIRECT` reads and writes on a 1 GiB file on the VM's ext4 root disk (virtio-blk, shared with the other agents).
- **R**: registration time and limits, as root (`CAP_IPC_LOCK`) and as `nobody` with the default 8 MiB `RLIMIT_MEMLOCK`.

Each I/O row runs the four variants (plain, fixed file, fixed buffer, both) back to back in an order rotated every rep, 2 s each, 6 reps. The VM was shared with six other agents running benchmarks (load average 5 to 9), so wall-clock IOPS are noise and queue depth 1 on the virtio disk, where each I/O also costs a VM exit, is noisy in CPU too.

```
./build.sh                         # bin/uring
sudo python3 run.py > results.txt  # about 10 minutes; needs root for zram; writes 1.3 GiB under work/
python3 summarize.py               # medians as a markdown table
```

Expected before the run: fixed buffers save the page pinning and unpinning of `O_DIRECT` (a few hundred ns per I/O) and nothing on buffered reads; fixed files save one `fget`/`fput` pair (tens of ns).

## Results

Raw output: [results.txt](results.txt). CPU µs per I/O, medians of 6 reps.

| Test | Plain | Fixed file | Fixed buffer | Both | Both against plain |
| --- | --- | --- | --- | --- | --- |
| N, NOP, QD 1 | 0.222 | 0.214 | 0.233 | 0.226 | +0.004 µs |
| N, NOP, QD 32 | 0.044 | 0.037 | 0.050 | 0.038 | −0.005 µs; fixed file −0.007 µs, lower in 5 of 6 reps |
| P, buffered read from cache, QD 1 | 1.647 | 1.565 | 1.546 | 1.606 | −0.04 µs (noise) |
| P, buffered read from cache, QD 32 | 1.151 | 1.227 | 1.304 | 1.187 | +0.04 µs (noise) |
| Z, `O_DIRECT` read, QD 1 | 4.318 | 3.741 | 3.580 | 3.389 | **−0.93 µs (−22%)** |
| Z, `O_DIRECT` read, QD 32 | 3.877 | 4.152 | 2.752 | 2.899 | **−0.98 µs (−25%)**; fixed buffer lower in 6 of 6 reps |
| Z, `O_DIRECT` write, QD 1 | 5.291 | 4.130 | 3.603 | 3.883 | **−1.41 µs (−27%)** |
| Z, `O_DIRECT` write, QD 32 | 4.301 | 4.495 | 3.175 | 3.149 | **−1.15 µs (−27%)**; fixed buffer lower in 6 of 6 reps |
| V, `O_DIRECT` read, QD 1 | 18.86 | 23.23 | 18.08 | 17.75 | −1.1 µs (−6%, within this disk's noise) |
| V, `O_DIRECT` read, QD 32 | 3.375 | 3.107 | 2.855 | 2.766 | **−0.61 µs (−18%)**; fixed buffer lower in 6 of 6 reps |
| V, `O_DIRECT` write, QD 1 | 21.61 | 18.41 | 20.24 | 20.72 | −0.9 µs (−4%, within noise) |
| V, `O_DIRECT` write, QD 32 | 4.005 | 4.382 | 3.583 | 3.274 | **−0.73 µs (−18%)**; fixed buffer lower in 5 of 6 reps |

| Test | Result |
| --- | --- |
| R1, register a 64 MiB slab already touched | 2.2 ms; 1 GiB 34 ms, 2 GiB (two 1 GiB iovecs) 139 ms: 0.13 to 0.26 µs per 4 KiB page pinned. Unregistering 1 GiB 7 ms |
| R2, register memory never touched | 64 MiB 55 ms, 1 GiB 2.8 s: registration faults every page in, and first touches of guest memory are slow on this VM. A slab touched before use, as the design already requires, pays only R1 |
| R3, size limits | One iovec of 1025 MiB: `EFAULT` (at most 1 GiB per buffer, `io_uring/rsrc.c:90`). 16,384 iovecs of 4 KiB: registered in 8.6 ms; 16,385: `EINVAL` (`IORING_MAX_REG_BUFFERS` = 1 << 14, `rsrc.c:35`). So one ring can register at most 16 TiB, as up to 16,384 buffers of at most 1 GiB each |
| R4, memory-lock limit | **As `nobody` with the default 8 MiB `RLIMIT_MEMLOCK`: 4 MiB registers, 64 MiB fails with `ENOMEM`.** Pinned pages are charged to the user's `locked_vm` against `RLIMIT_MEMLOCK` unless the process has `CAP_IPC_LOCK` (`io_uring/io_uring.c:3839`, `rsrc.c:39-56`). Each ring charges its registration again, except pages of a huge page already charged (`headpage_already_acct`, `rsrc.c:629`) |

Why, in the kernel source at [v6.18](https://github.com/torvalds/linux/tree/v6.18):

- An `O_DIRECT` read or write from a user address builds a user iterator, and the block layer pins every page with `iov_iter_extract_pages` and unpins it at completion (`block/bio.c:1339-1354`, `BIO_PAGE_PINNED`). A fixed buffer is a `bvec` iterator over pages pinned at registration, which `bio_iov_bvec_set` hands to the bio as is (`block/bio.c:1347-1348`). That pin and unpin is the 0.6 to 1.4 µs measured in Z and V.
- A buffered read copies into the user buffer either way, so a fixed buffer saves nothing there (P).
- A plain descriptor costs `fget` and `fput` per request (`io_uring/io_uring.c:2017-2019`), a fixed one a table lookup and a reference on the table node (`io_uring.c:1998-2010`): 5 to 13 ns, visible only in N.

## Conclusions

1. **The disk driver registers its slab.** A fixed buffer cuts 0.6 to 1.4 µs of CPU from every 4 KiB `O_DIRECT` I/O, 18 to 27% at queue depth 32, consistently across reps (Z, V); if an NVMe drive's kernel path costs about what zram's does, that is about a fifth of the CPU per I/O. The I/O buffer handle (slot and generation in the driver's slab) already names an address inside a registered region, so `READ_FIXED`/`WRITE_FIXED` take the same handle plus the region's buffer index.
2. **What registration costs is a fixed slab, not a fixed ring.** Registration pins the slab: 34 to 70 ms per GiB already touched (R1), at most 1 GiB per region and 16,384 regions per ring (R3). The ring itself is laid out as before; a slab that grows registers its new region into a sparse table (`IORING_RSRC_REGISTER_SPARSE`, `IORING_REGISTER_BUFFERS_UPDATE`) without touching the old ones. Each loop's ring registers only that loop's slab, so the memory is charged once.
3. **The daemon pod needs `IPC_LOCK` or a memory-lock limit as large as its slabs**, as RDMA already does: with the default 8 MiB limit, a 64 MiB registration fails (R4). Without either, the driver falls back to plain `READ`/`WRITE` on the same buffers and reports it, so registration is an optimization, never a condition for running.
4. **Fixed files are optional**: they save 5 to 13 ns per request, under the noise of every real I/O measured. The driver registers its disks' descriptors because it holds few of them for its whole life, but nothing depends on it.
5. Queue depth 1 on the virtio disk could not resolve the difference: each I/O costs 18 to 23 µs there, mostly the VM exit and interrupt, and the shared host adds more noise than the 1 µs saved.
