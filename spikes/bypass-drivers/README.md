# Kernel-bypass drivers spike (SPDK, RDMA)

**Question.** Can SPDK (disk) and RDMA (network) be added later as opt-in drivers behind the sans-IO `Action`/`Event` interface, without changing the core, the default build or the "no hugepages, no pinned cores" install? Or does something in them have to shape the interface now?

**Setup.** Linux 6.17, Intel Core Ultra 5 235U laptop, consumer NVMe (root disk only). SPDK v26.09 with DPDK 26.03 and rdma-core v50.0, both shallow clones. Everything is built and run as a normal user: dev headers come from Ubuntu `.deb`s unpacked into a local sysroot. RDMA runs on Soft-RoCE (`rdma_rxe`) over Wi-Fi, which needs root once (`setup-root.sh`). Two queue pairs on one device stand in for two nodes.

```
./fetch.sh                      # shallow clones, user-local sysroot, SPDK build (no root)
./build.sh                      # bin/spdk_spike, bin/spdk_spike_nowa, bin/rdma_spike
bin/spdk_spike [--interrupt] [--reinit] work
sudo ./setup-root.sh            # once: rxe0 on the first real NIC; undo with teardown-root.sh
RS_SWEEP=1 bin/rdma_spike
```

Raw output: [results-spdk.txt](results-spdk.txt), [results-rdma.txt](results-rdma.txt).

## SPDK, embedded in our own loop

No `spdk_app_start` and no reactors: `spdk_env_init` with `no_huge` and `no_pci`, one `spdk_thread` that `main()` polls, or that sleeps in `epoll` on the thread's interrupt fd. Bdevs: malloc, aio and uring on a 64 MB file.

| Test | Result |
| --- | --- |
| S1: init as uid 1000, no hugepages | Works. **But stock SPDK fails here**: its default DPDK base address 0x200000000000 is above this laptop's 42-bit IOMMU DMA mask ("IOVA exceeding limits of current DMA mask"). Setting `base_virtaddr` to 0x10000000000 fixes it |
| S1: memory | Default pools need about 430 MB RSS and fail with 256 MB of DPDK memory. With bdev_io and iobuf pools shrunk: 63 MB at startup, 69 MB after the tests |
| S2: 4K QD1, poll mode | aio and uring: write p50 12 to 14 µs, read p50 17 µs, but **43 to 46 µs of CPU per I/O** because the loop spins while waiting. Heap and DMA buffers behave the same on these bdevs |
| S2: 4K QD1, interrupt mode | aio: same latency (+1 µs), **2.9 µs of CPU per I/O**. uring bdev: no interrupt support, so the thread fd stays ready and the core stays at 100%. The uring *socket* module refuses interrupt mode outright |
| S3: idle loop | Poll: 100% of a core. Interrupt: 0% |
| S4: another pthread submits through `spdk_thread_send_msg` and waits on an eventfd | 1.6 µs p50 (poll), 3.0 µs (interrupt). This is how a tokio task would hand work to the SPDK thread |
| S5: DMA-ability | With no hugepages, `spdk_vtophys` fails even for `spdk_dma_zmalloc` memory. Real NVMe under SPDK still needs root, `vfio-pci` and very likely hugepages. Not tested: the only NVMe drive here is the root disk |
| S6: `spdk_env_fini` then `spdk_env_init(NULL)` | Works; DMA allocation works after re-init |
| Interrupt mode + `spdk_thread_lib_init(NULL, 0)` | **Segfault on shutdown**: in interrupt mode, `thread_exit` calls the scheduler callback unconditionally (`thread.c:1215`). Embedders must use `spdk_thread_lib_init_ext` with a callback |
| Linking without `--whole-archive` | Links without error, then fails at start ("spdk_msg_mempool creation failed", then SIGSEGV). SPDK modules and DPDK drivers register from constructors, so the linker silently drops them |
| Binary | 11.8 MB with SPDK and DPDK static. It still needs these shared libraries at runtime: liburing, libaio, libnuma, libssl/libcrypto, libuuid |

## RDMA on Soft-RoCE

`libibverbs` is loaded with `dlopen` and the binary has no link-time dependency on it (`ldd`: libc only).

| Test | Result |
| --- | --- |
| R0: library missing / no device | Reports "transport = TCP" and exits cleanly in both cases |
| R1: device | rxe0, RoCE v2 GID is the IPv4-mapped one (index 1). `ibv_is_fork_initialized` = UNNEEDED on 6.17, so FUSE's fork/exec of `fusermount3` is safe |
| R2: `ibv_reg_mr` cost | 4 KB 2.3 µs, 64 KB 3.0 µs, 1 MB 12 µs, 16 MB 170 µs, 256 MB 3.9 ms (+ dereg 0.9 µs to 2.2 ms). Registering per I/O costs about as much as a small round trip |
| R3: `RLIMIT_MEMLOCK`, no `CAP_IPC_LOCK` | 64 KB limit: a 1 MB registration fails with ENOMEM. 8 MB limit: 4 MB ok, 16 MB fails. Pinned memory counts against the container's memlock limit |
| R4: SEND/RECV round trips, rxe vs kernel TCP loopback | 4 KB: rxe 12 µs (poll) / 16 µs (epoll), TCP 9 µs. 1 MB: rxe 1.1 to 1.3 GB/s, TCP 11.7 GB/s. **Soft-RoCE is slower than TCP at every size**; it is a functional stand-in, not a performance one |
| R4: CPU per round trip | Poll mode burns the waiting time (64 KB: 132 µs CPU per round trip). Completion channel in epoll: 5 to 10 µs at every size. rxe's own kernel work is not charged to the thread, so the CPU numbers understate its real cost |
| R5: 64 B inline send from unregistered heap memory | Works: small control messages need no registration |
| R5: 4 KB send from memory outside any MR | The QP goes to error. rxe reports only "Work Request Flushed", never the real cause, and **every later send on that QP fails** until it is rebuilt |
| R6: peer QP destroyed | `retry_cnt` 1 to 6: "transport retry counter exceeded" after about 4 ms (timeout 8) up to 540 ms (timeout 16), roughly (retry+1) × 4.096 µs × 2^timeout. **`retry_cnt=7`: no completion, ever** (rxe treats 7 as "retry forever", [rxe_comp.c:772](https://github.com/torvalds/linux/blob/v6.17/drivers/infiniband/sw/rxe/rxe_comp.c#L772); the InfiniBand spec and real NICs don't). `timeout=0` also never completes |
| R6: peer alive, no receive posted, `rnr_retry=3` | "RNR retry counter exceeded" after 5 ms. `rnr_retry=7` would wait forever |

## Conclusions

The drivers fit the slot, but four things belong in the driver interface **before the default drivers are written** (roadmap phase 5), not when SPDK and RDMA arrive in phase 9:

1. **Buffers come from the driver, not the core.** NVMe under SPDK can only DMA from SPDK-registered memory, and RDMA can only send from a registered MR; a stray buffer kills the whole connection. Registering per I/O costs as much as the I/O. So `Action`s carry an `IoBuf` handle from a driver-provided pool (a plain heap pool for io_uring and TCP, and the simulator). One pool registered with both SPDK and RDMA gives zero-copy disk to network. Small control messages (≤ 256 B) go inline and need no buffer.
2. **Drivers expose a readiness fd and default to event mode.** SPDK's thread interrupt fd and the ibverbs completion-channel fd both sit in epoll / tokio `AsyncFd`. Event mode costs up to 5 µs of latency and cuts CPU per operation (SPDK aio 43 → 2.9 µs; RDMA 4 KB 12 → 6 µs, 64 KB 132 → 7 µs). Busy polling is a separate opt-in for dedicated cores. Modules without interrupt support (SPDK uring bdev and socket) are excluded in event mode.
3. **Connections fail as a unit.** An RDMA error moves the QP to error and flushes everything queued on it. The driver turns that into one "peer connection lost" event, fails every outstanding `IoId` on it, and rebuilds the QP. Use `retry_cnt` ≤ 6 and `rnr_retry` < 7, and keep the core's own deadlines anyway: transport timeouts are not reliable (Soft-RoCE retries forever on 7).
4. **RDMA is an upgrade of a TCP session.** Connecting a QP needs an out-of-band exchange of queue pair number and GID, so every node pair opens TCP first and upgrades if both ends report a usable device. With `dlopen` the same binary runs on nodes without rdma-core.

Plug-and-play rules for the opt-in drivers:

- **SPDK**: compute `base_virtaddr` from the host's IOMMU address width; set pool sizes explicitly; use `spdk_thread_lib_init_ext`; link with `--whole-archive` (the Rust `build.rs` must do the same). Without root and hugepages it runs file-backed bdevs, which is enough for CI and the ChunkStore test suite. Real NVMe stays a privileged, dedicated-drive mode.
- **RDMA**: the daemon pod needs `IPC_LOCK` or a memlock limit above the buffer pool size. At startup, check the limit and fall back to TCP with a clear message rather than failing on the first large registration.
- **Soft-RoCE** is for correctness and fault injection only. All RDMA performance decisions need a real NIC.
