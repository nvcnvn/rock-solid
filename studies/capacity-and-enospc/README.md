# Pool capacity and ENOSPC: what a pool can promise, and when a full pool reaches the application

**Status:** read from source and documentation, plus a model spike ([spikes/capacity-model](../../spikes/capacity-model/README.md)) and a root-only spike written but not run ([spikes/block-enospc](../../spikes/block-enospc/README.md)). Nothing in `docs/` is changed yet; [Proposed design](#proposed-design) holds the replacement text.

**Question:** two related gaps in [Volume size and capacity](../../docs/design.md#volume-size-and-capacity).

1. A pool reports its capacity as free raw space divided by its overhead (`replicas`, or `(data + parity) ÷ data`). When failure domains differ in size this overstates: a `rep2` pool on two nodes with 0.7 and 2 GB free reports 1.35 GB, but every chunk needs a copy on each node, so about 0.63 GB (the small disk up to `write_limit`) can be written. Erasure-coded pools with unequal domains, and one-copy pools whose capacity-weighted placement meets a disk that another pool has filled, have the same problem. What should a pool report per pool, per locality and per node; what do `statfs` and the Container Storage Interface (CSI) capacity show; what does `overcommit: off` reserve against; what should the pool warn about; should placement refuse a layout that strands most of a disk?
2. When and how does a full pool reach the application? (a) The FUSE daemon acknowledges a buffered `write()` at once and flushes later, so `ENOSPC` from a chunk primary surfaces only at a later `write()`, `fsync` or `close`, and applications that ignore `close()` lose data. (b) A thin block volume that runs out of pool space returns an error to the filesystem inside it, which may shut down or remount read-only. (c) How do `write_limit`, the log's reserve and "writers slow down" fit, given that deletes must still work on a full pool?

## Summary

**Capacity.** The design's figure is what Ceph's `MAX AVAIL` and MinIO's usable capacity compute: room divided by overhead, as if every disk could take a share of new data proportional to its size. The [capacity model](../../spikes/capacity-model/README.md) shows it overstates by 1.36 to 3.3× on unequal failure domains (`ec4-2` at 2 per node on nodes of 4, 4 and 1 TB: 6.0 TB reported, 1.8 TB writable). Vitastor instead reports what its actual PG table lets through before the first disk fills: each disk's room divided by the share of new data the table sends it, the smallest over the pool. In the model that figure predicts the first refused write within 0.3% in every layout, so rock-solid should report it as **available**. Alongside it, a closed-form **balanced** bound over failure domains, min over j of S₍>ⱼ₎ ÷ (b − j·a), says what placement could reach; the design's figure is its j = 0 term, and the difference is **stranded** space. Volumes with `overcommit: off` are admitted against balanced, `statfs` and CSI show available, and the pool warns when space is stranded, unbalanced, near full, or too short to restore redundancy after losing its largest domain. Placement never refuses a layout, since another pool on the same disks can use stranded space, but its targets must be capped by the failure-domain rule: with capacity-weighted targets the balance loop never converges on unequal domains.

**ENOSPC under write-back.** ext4, XFS and Btrfs reserve space at `write()` before they buffer, so `write()` returns `ENOSPC` and writeback cannot fail for space. Every network client read reports late instead: the Linux NFS client at the next `write()`, `fsync` or `close`; the CephFS clients at `fsync` or `close`, after cancelling writes in flight and discarding cached data; JuiceFS at the next `write()` or flush of the file. CephFS narrows the window by syncing every write once its pool is nearly full, as ext4 stops delaying allocation when free space runs low. The proposal is the local filesystems' guarantee at a network client's cost: **space grants**. The daemon buffers a write only against a per-pool grant that the control-plane leader sets on the heartbeat each node already sends every second, and reuses the grant as its flushes complete; without a grant it writes through and gets `ENOSPC` from `write()`. Grants shrink as the pool fills, and together with one heartbeat of flushes they fit in the room every disk keeps above `write_limit`, so a granted flush is admitted past the limit and never refused for space. At 10 nodes they cost no message, about 16 bytes per pool per heartbeat, and at most 2.5 GiB of outstanding promises.

**Block volumes.** Ceph RBD and Vitastor hold writes while the cluster is full, for as long as it takes; dm-thin and LVM thin hold them for 60 s and then fail them with `BLK_STS_NOSPC`, and keep serving writes to blocks already provisioned. The proposal follows dm-thin: overwrites of blocks a volume already holds live and unshared pass `write_limit` (in the log they replace their old blocks, so live data does not grow), discards always pass, and writes that add data are held for `full_hold` (60 s, below the kernel's 120 s hung-task warning and capped below NVMe/TCP's 30 s I/O timeout for remote clients), then failed with `-ENOSPC`. The kernel turns that into `BLK_STS_NOSPC`, so the filesystem records `ENOSPC` for the file, and its journal, an overwrite, keeps working. What ext4 and XFS then do is read from source only; the block-enospc spike checks it but needs root.

**Deletes** pass `write_limit` (Ceph lets every operation that adds no bytes through a full pool). Whether punches and Raft entries should also draw on a slice of the disk log's reserve stays the [open question](../../docs/design.md#roadmap-and-open-questions) it is, now with ext4's, XFS's, Btrfs's and CockroachDB's reserves as precedent.

## What the design says now

- **Capacity:** "A pool reports it as free raw space divided by its encoded overhead ... data still held as copies is shown separately. Pools that share disks draw on the same free space" ([Volume size and capacity](../../docs/design.md#volume-size-and-capacity)). With `overcommit` off, "creating a volume reserves its full size", against nothing stated. `statfs` on a filesystem volume reports the PVC size as total and the slot counters' sum as used, so `df` shows the PVC's remainder as free even when the pool has less.
- **Placement:** "While a disk is outside a fill margin of its capacity-weighted share, move one member from the fullest disk to the emptiest eligible one" ([placement table](../../docs/design.md#data-layer-chunkstore)). No stopping rule when no move is eligible, and no target for disks whose failure domain cannot take a capacity-weighted share.
- **Write limit:** a write fails with `ENOSPC` once any member disk of its PG is past `write_limit` (90%), whatever it writes; the pool warns 10 points earlier. Below that, the log slows chunk writes when the cleaner lags and makes ordinary items wait while only the reserve is free ([Writers slow down](../../docs/design.md#on-disk-format)); `NoSpace` comes only when the cleaner has nothing to free. Deletes on a full disk are an [open question](../../docs/design.md#roadmap-and-open-questions).
- **Write-back:** "the daemon buffers writes in its own memory and acknowledges at once ... up to 256 MiB per node" ([Client design](../../docs/design.md#client-design)); a crash loses the buffer and "fails the next `fsync` or `close` on each [handle] with `EIO`" ([Daemon restart and upgrade](../../docs/design.md#daemon-restart-and-upgrade)). Nothing says what a flush that meets a full pool does.
- **Block:** ublk devices use user recovery with reissue because "a failed write makes ext4 abort its journal and XFS shut down" ([Daemon restart and upgrade](../../docs/design.md#daemon-restart-and-upgrade)); the [block frontend](../../docs/design.md#block-frontend) says nothing about a full pool. Blog posts [02](../../blogs/02-chunks-and-placement.md#volume-size-and-capacity), [05](../../blogs/05-the-cleaner-and-free-space.md), [07](../../blogs/07-pools-and-redundancy.md), [09](../../blogs/09-rwx-filesystem-and-cache-coherence.md) and [10](../../blogs/10-block-snapshots-and-clones.md) repeat the capacity formula and the per-disk `ENOSPC`.

## What other systems do

### Capacity reporting

| System | What it reports | How it accounts for placement | Fullness thresholds |
| --- | --- | --- | --- |
| Ceph | Per pool `MAX AVAIL`: the smallest, over the rule's OSDs, of (room to `full_ratio`) ÷ (the OSD's CRUSH weight ÷ the rule's total), divided by `size` or `(k + m) ÷ k` | CRUSH weights, normalized over the rule's root; the failure-domain rule is ignored | `nearfull` 85%, `backfillfull` 90%, `full` 95%, failsafe 97% |
| Vitastor | Per pool `max_available`: the smallest over OSDs of free × PG count ÷ PGs on that OSD, × data chunks for EC; "efficiency" = optimized space ÷ total | The actual PG table, built by a linear program that maximizes the space the PGs can hold | Out of space on an OSD |
| Longhorn | CSI `GetCapacity` per node: Σ over disks of ((maximum − reserved) × over-provisioning% − scheduled); `maximum_volume_size` the largest single disk's | Each replica is a whole volume on one disk; the replica count is not in the figure | A disk below 25% free is unschedulable |
| CockroachDB | Per store: capacity, available, used | Replicas are placed per store; no cluster figure divides by the replication factor | No new replicas on a store over 95% used, no rebalance onto one over 92.5% |
| MinIO | Usable capacity: Σ of the drives whose index in their erasure set is below the data count | Assumes the drives of a set are alike | A PUT refuses a pool whose drives lack size × 2 ÷ n each, or that would pass 99% |
| Kubernetes | `CSIStorageCapacity` per StorageClass and topology segment, from `GetCapacity` | The scheduler filters nodes for `WaitForFirstConsumer` volumes by `maximumVolumeSize`, else `capacity` | — |

**Ceph.** `PGMap::get_rule_avail` (`src/mon/PGMap.cc:1026-1067`) takes, for each OSD of a CRUSH rule, its free space minus `(1 − full_ratio)` of its size, divides by the OSD's share of the rule's weight, and keeps the smallest; `get_pool_free_space` (`:1006-1017`) divides that by `pool_raw_used_rate`, which is `size` or `(k + m) ÷ k` (`src/osd/OSDMap.cc:8467-8492`). The weight map comes from `CrushWrapper::get_rule_weight_osd_map`, which normalizes each OSD's CRUSH weight over the rule's `take` root, and its own comment admits the gap: "the ultimate object placement is subject to the pool size constraint, which might restrict the number of objects selected by the Crush rule" (`src/crush/CrushWrapper.cc:2556-2588`). So on two hosts of 0.7 and 2 TB, a `size=2` pool gets weights 0.26 and 0.74, while each host holds exactly one copy of every PG: `MAX AVAIL` is the same j = 0 term as rock-solid's figure (model C1, C2, C3, C6). The balancer documents the consequence: on mismatched devices an "excessively low value [of `upmap_max_deviation`] may result in the balancer shuffling data forever as it endeavors to meet an impossible expectation" (`doc/rados/operations/balancer.rst:89-115`). The troubleshooting page tells operators to "pay special attention to the most full OSDs", since `ceph df` "considers the ratio settings relative to the most full OSD" (`doc/rados/troubleshooting/troubleshooting-osd.rst:357-366`). Once any OSD of a pool is `full`, the monitor flags the whole pool `FULL` (`OSDMap::get_full_pools`, `src/osd/OSDMap.cc:1763-1798`; `src/mon/OSDMonitor.cc:1679-1742`); thresholds are `mon_osd_nearfull_ratio` 0.85, `mon_osd_backfillfull_ratio` 0.9, `mon_osd_full_ratio` 0.95 and `osd_failsafe_full_ratio` 0.97 (`src/common/options/global.yaml.in:1665-1686, 3495`). CephFS `statfs` on a single data pool reports used + `MAX AVAIL` as size (`PGMapDigest::get_statfs`, `src/mon/PGMap.cc:727-756`); with a quota, the kernel client reports quota − used and ignores the pool (`fs/ceph/quota.c:490-535`), so a CephFS PVC shows free space the pool may not have.

**Vitastor.** `vitastor-cli df` computes, for each OSD in a pool's PG targets, `free × reweight × real_pg_count ÷ PGs on the OSD`, keeps the smallest, and multiplies by `pg_size − parity_chunks` for erasure-coded pools (`src/cmd/cli_pool_ls.cpp:136-176`). That is "available from the table". The monitor builds PG tables with `lp_solve`: it maximizes the number of PGs subject to each OSD's weight-proportional cap (`mon/lp_optimizer/lp_optimizer.js:56-110`) and reports the result as `space_efficiency` = optimized space ÷ total, from the same per-OSD rule (`:526-545`), which `df` prints as EFFICIENCY.

**Longhorn.** `GetCapacity` sums, over a node's schedulable disks, `(StorageMaximum − StorageReserved) × over-provisioning% − StorageScheduled`, and returns the largest single disk's figure as `MaximumVolumeSize`, which the scheduler prefers (`csi/controller_server.go:764-903`). Over-provisioning defaults to 100% and a disk with less than 25% free is unschedulable (`types/setting.go:702-729`; `scheduler/replica_scheduler.go:1274-1310`). Scheduling reserves the volume's full size on each replica's disk, which is what Longhorn's "thick" accounting amounts to.

**CockroachDB** places replicas per store and never reports a capacity divided by replication: no new replicas go to a store over `kv.allocator.max_disk_utilization_threshold` (0.95), and none are rebalanced onto one over 0.925, "to provide a buffer between fully healthy stores and full stores" (`pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:50-58, 255-282`). Each store keeps an emergency ballast file of min(1 GiB, 1%), and a store with less than half the ballast free counts as full; deleting the ballast is how an operator gets a full node to start (`pkg/storage/ballast.go:25-98`).

**MinIO** counts as usable the drives whose index in their erasure set is below the data-drive count (`cmd/notification-summary.go:33-70`), so unequal drives in a set are not modelled. Admission is per object, which it can do because S3 PUTs carry their size: `hasSpaceFor` asks every drive of the set for size × 2 ÷ n and the set to stay under 99% (`cmd/object-api-utils.go:1242-1300`), assuming 1 GiB for an unknown size.

**Kubernetes.** The external-provisioner calls `GetCapacity` once per StorageClass and topology segment and publishes `CSIStorageCapacity` objects, refreshed every minute by default (`README.md`, `pkg/capacity/capacity.go:611-631`). The scheduler uses them only for `WaitForFirstConsumer` volumes of drivers that set `CSIDriver.spec.storageCapacity`: a node passes if some object for the class whose topology includes it has `maximumVolumeSize` (preferred, "more precise") or else `capacity` at least the PVC's request (`pkg/scheduler/framework/plugins/volumebinding/binder.go:974-1034`). A driver that sets the flag must publish an object for every such class, or no node passes. The check is advisory: "the scheduler has to decide based on potentially out-dated information", and a failed `CreateVolume` returns `RESOURCE_EXHAUSTED` and reschedules (`storage-capacity.md`; CSI `spec.md:1297`). The CSI spec's volume health API, which could mark a volume degraded, is alpha (`spec.md:1705-1727`).

### When the space runs out

| System | Where the check happens | What the writer sees | Deletes on a full pool |
| --- | --- | --- | --- |
| ext4 | `write()`: delayed allocation reserves clusters; below 150% of dirty clusters it stops delaying | `ENOSPC` from `write()` | min(2%, 4,096 clusters) reserved for punches and extent conversions |
| XFS | `write()`: delayed allocation takes free blocks from a per-CPU counter, serialized near zero; one flush-and-retry on `ENOSPC` | `ENOSPC` from `write()` | Reserve pool of min(5%, 8,192 blocks) for transactions at `ENOSPC` |
| XFS on zoned devices | `write()`: user capacity, then blocks instantly available; waits for garbage collection | Waits, then `ENOSPC` when nothing is reclaimable | — |
| Btrfs | `write()`: data and metadata reservation, flushing to make room | `ENOSPC` from `write()` | `unlink`/`rmdir` fall back to the global reserve |
| Linux NFS client | Server, at writeback | Error recorded on the file; the next `write()` (for `ENOSPC`, `EDQUOT`, `EFBIG`), `fsync` or `close` returns it; `sync` mounts at every `write()` | Server's |
| CephFS (kernel, `ceph-fuse`) | `write()` refuses once the pool is flagged `FULL`; `nearfull` makes every write synchronous | `ENOSPC` from `write()`, or from `fsync`/`close` for data buffered before; writes in flight cancelled and cached data discarded | Deletes and truncates work |
| Ceph RBD, librados | OSD | Writes pause until the pool is not full | Operations that add no bytes pass |
| Vitastor | OSD | Client retries `ENOSPC` until space appears (`client_retry_enospc`, default on) | — |
| JuiceFS | Volume capacity and quotas checked when a slice is committed, after its upload | `ENOSPC`/`EDQUOT` kept on the file, returned by its next `write()`, flush, `fsync` or `close`; the slice is deleted | — |
| dm-thin, LVM thin | Allocation of a new block | Held for `no_space_timeout` (60 s), then `BLK_STS_NOSPC`; or at once with `error_if_no_space`; provisioned, unshared blocks keep working | Discards processed |

**ext4.** `ext4_da_write_begin` reserves one cluster per new block through `ext4_da_reserve_space` → `ext4_claim_free_clusters`, which fails with `ENOSPC` (`fs/ext4/inode.c:1617-1643`), so `write()` sees it. The free-cluster check counts reserved but unallocated ("dirty") clusters and reads exact per-CPU sums once it gets within a watermark (`fs/ext4/balloc.c:620-666`). `ext4_nonda_switch` stops delaying allocation once free clusters fall below 150% of dirty ones, because "delalloc need an accurate free block accounting" (`fs/ext4/inode.c:3077-3107`, used at `:3127`). ext4 keeps min(2%, 4,096 clusters) for "situations where we can not afford to run out of space like for example punch hole" (`fs/ext4/super.c:4243-4271`). A failed data writeback only records the error on the file (`mapping_set_error`, `fs/ext4/page-io.c:365-375`); a failed journal write aborts the journal (`fs/jbd2/commit.c`), after which the filesystem takes no more changes, whatever `errors=` says.

**XFS.** Delayed allocation takes blocks from the free counter at `write()` (`xfs_bmapi_reserve_delalloc`, `fs/xfs/xfs_iomap.c:1536`); on `ENOSPC` the write flushes dirty inodes, trims speculative preallocation and retries once (`fs/xfs/xfs_file.c:978-1003`). `xfs_dec_freecounter` updates a per-CPU counter in batches of 1,024 blocks and serializes once within twice the batch of zero ("need to be more accurate the closer we are to zero", `fs/xfs/xfs_mount.c:1436-1490`, `xfs_mount.h:705`), the same shape as per-node grants that shrink near full. A reserve pool of min(5%, 8,192 blocks) covers "concurrent allocation transactions when we initially hit ENOSPC" (`xfs_mount.c:473-494`). XFS's zoned allocator, log-structured like rock-solid's disk log, keeps two counters: `XC_FREE_RTEXTENTS`, "the user available capacity", and `XC_FREE_RTAVAILABLE`, "the blocks instantly available for writes without waiting for GC" (`fs/xfs/xfs_zone_space_resv.c:36-55`); a write that gets the first but not the second waits for the collector, and gives up with `ENOSPC` only when no zone is reclaimable (`:119-185`). It truncates a buffered write to what it reserved (`xfs_file.c:1029-1050`). That is rock-solid's split between `write_limit` (an error) and the cleaner's backpressure (a wait). Metadata writeback retries `ENOSPC` and `EIO` forever by default, configurable per error under `/sys/fs/xfs/<dev>/error/metadata/` (`fs/xfs/xfs_sysfs.c:641-658`; `Documentation/admin-guide/xfs.rst`), which is what lets XFS ride out a thin pool's temporary exhaustion; a log write error shuts the filesystem down (`fs/xfs/xfs_log.c:1243-1245`).

**Btrfs** reserves data and metadata space before a buffered write copies anything (`reserve_space`, `fs/btrfs/file.c:1118`; `btrfs_delalloc_reserve_space`, `delalloc-space.c:500`). `unlink` and `rmdir` "do not always free space, so if we cannot make our reservations the normal way try and see if there is plenty of slack room in the global reserve" (`fs/btrfs/inode.c:4355-4368`).

**The Linux NFS client** buffers writes in the page cache and sends them asynchronously. A failed write records its error on the file through `errseq_t` (`fs/nfs/write.c`, `nfs_mapping_set_error`). Each `write()` samples that error before it copies and checks it after; for `EDQUOT`, `EFBIG` and `ENOSPC` it flushes everything and returns the error (`fs/nfs/file.c:730-777`). `close()` flushes and returns any error not yet reported (`nfs_file_flush`, `:140-156`). The `sync` mount option makes each `write()` wait for the server, and `write=eager|wait` sends or waits per call (`fs/nfs/fs_context.c:153-155`); `wsize` only sets the size of each WRITE. Everything else reaches the application only if it checks `fsync` or `close`.

**`errseq_t` and `close()`.** Since Linux 4.13 a writeback error is recorded once per file, in an `errseq_t` with a counter, and every open file description samples it at open (`fs/open.c:913`) and sees each new error once, at `fsync` (`file_check_and_advance_wb_err`, `mm/filemap.c:744-769`; `Documentation/core-api/errseq.rst`). `close()` returns whatever the filesystem's `->flush` returns (`filp_flush`, `fs/open.c:1538-1556`); ext4 and XFS have none, so `close()` never reports writeback errors there, while NFS's does. FUSE's `fuse_flush` sends `FUSE_FLUSH` on every `close()` of every descriptor, after writing back the page cache and checking the mapping's errors, and returns the daemon's reply (`fs/fuse/file.c:445-495`), so a daemon can report a late error at `close()`; an `exit()` that closes descriptors discards the result. A daemon's short reply to `FUSE_WRITE` is returned as a short `write()` (`fs/fuse/file.c:1286-1312`), so the next `write()` can return the error.

**CephFS.** The kernel client refuses a `write()` with `ENOSPC` once the map or the file's pool is `FULL`, after checking the quota (`EDQUOT`), and once either is `NEARFULL` it sets `IOCB_DSYNC` so every write goes to the OSDs before it returns (`fs/ceph/file.c:2383-2395, 2493-2495`). It mounts with `ABORT_ON_FULL` (`fs/ceph/super.c:823`), which fails writes in flight with `ENOSPC` when the pool fills (`net/ceph/osd_client.c:2643-2674`); RBD does not set it, so its writes pause (`:2418-2433`). The userspace client cancels writes in flight, discards the dirty cache and marks each inode's error, "rather than blocking, because otherwise when we fill up we potentially lock caps forever on files with dirty pages, and we need to be able to release those caps to the MDS so that it can delete files" (`Client::_handle_full_flag`, `src/client/Client.cc:3125-3158`). The documentation states the consequence: "the ENOSPC error may not be seen until the application calls fsync or fclose", and "buffered data may be discarded after an fclose if no space is available" (`doc/cephfs/full.rst:15-37`). Deletes and truncates keep working: the OSD rejects only operations that add bytes or objects to a `FULL` pool (`src/osd/PrimaryLogPG.cc:9203-9221`), and only the 97% failsafe drops everything without `FULL_TRY` (`:2175-2178`). Before Hammer, writes stalled instead, and the documentation lists why that was dangerous: a client with pending writes could not release a file for deletion, and metadata writes could fill the OSDs "such that no further deletions could be performed".

**JuiceFS** checks volume capacity and quotas when it commits a slice to the metadata engine, after uploading it (`pkg/meta/quota.go:280-306`, called from `doWrite`, `pkg/meta/redis.go:3166`). A refused commit deletes the uploaded objects and keeps `ENOSPC` or `EDQUOT` on the file writer, which every later `Write` and `Flush` returns (`pkg/vfs/writer.go:226-243, 350-393`). Its optional `--writeback` mode commits first and uploads later, and its documentation warns that data still in the local cache is lost with the disk (`docs/en/guide/cache.md:180-197`).

**dm-thin and LVM thin.** When a pool runs out of data space it goes to `out_of_data_space` mode: writes to blocks already provisioned and not shared with a snapshot proceed (`__process_bio_read_only`, `drivers/md/dm-thin.c:2026-2066`), discards are processed, and writes that need a new block are held until the pool grows or `no_space_timeout` (60 s, `:34`) expires, then failed with `BLK_STS_NOSPC` (`:2450-2460`), or failed at once with `error_if_no_space` (`:1592-1625`). A low-water mark raises an event for a daemon to extend the pool (`thin-provisioning.rst:118-122`). LVM documents the trade-off: queueing is the default "with the expectation that pool data space will be extended soon", failing "can result in file system damage that requires repair", and "disabling timeouts can result in the system running out of resources, memory exhaustion, hung tasks, and deadlocks" (`man/lvmthin.7_main:893-919`).

**ublk, NVMe and the block layer.** A ublk server's negative result goes through `errno_to_blk_status` (`drivers/block/ublk_drv.c:1159-1161`), so `-ENOSPC` becomes `BLK_STS_NOSPC`, logged as "critical space allocation" and seen by the filesystem as `-ENOSPC` (`block/blk-core.c:142`); anything unknown becomes `BLK_STS_IOERR`. A privileged ublk device never times a request out (`ublk_timeout` resets the timer, `ublk_drv.c:1394-1413`), so a server may hold a request as long as it likes, but the task waiting on it is in uninterruptible sleep and the hung-task detector warns after 120 s by default (`kernel/hung_task.c:52`, `lib/Kconfig.debug:1243-1246`). The NVMe host maps the Capacity Exceeded status to `BLK_STS_NOSPC` (`drivers/nvme/host/core.c:286-287`) but times out I/O after 30 s by default (`nvme_core.io_timeout`, `:56`) and then starts error recovery on TCP (`drivers/nvme/host/tcp.c:2674`).

**Vitastor** retries a write that got `ENOSPC` after `client_eio_retry_interval`, forever by default ("Retry writes on out of space errors to wait until some space is freed on OSDs", `docs/config/client.en.md:69-76`; `src/client/cluster_client.cpp:1700-1705`), as Ceph's RADOS clients pause.

### Not found or not read

- **Longhorn on a full disk during writes** (how a replica that fills behaves): not read; only its scheduling and `GetCapacity` were.
- **MinIO's documentation** on drives of unequal size in a set was not read; the usable figure is from code.
- **The `nfs(5)` man page** (nfs-utils) could not be fetched: `git.linux-nfs.org` is refused from this environment. NFS behaviour above is from the kernel source.
- **Ceph's tracker** was not searched for reports of `MAX AVAIL` on unequal hosts; the overstatement is reproduced from its formula, not observed in Ceph.

## Measurements

[spikes/capacity-model](../../spikes/capacity-model/README.md) builds integer placement tables the way the design describes, fills them with uniformly hashed 4 MiB chunks until the first refusal, and compares capacity figures (Python with numpy, no root, nothing timed).

| Layout | Design figure | Ceph rule | Balanced bound | Available from the table | First refusal, mean of 40 |
| --- | --- | --- | --- | --- | --- |
| C1 `rep2`, nodes of 0.7 and 2 GB | 1.35 GB | 1.22 GB | 0.63 GB | 0.63 GB | 0.63 GB |
| C2 `rep3`, nodes of 4, 4, 2, 1 TB | 3.67 TB | 3.30 TB | 2.70 TB | 2.68 TB | 2.68 TB |
| C3 `ec4-2` at 2 per node, nodes of 4, 4, 1 TB | 6.00 TB | 5.40 TB | 1.80 TB | 1.80 TB | 1.80 TB |
| C4 `none`, three 2 TB disks, one holding 1.2 TB of another pool | 4.80 TB | 1.80 TB | 4.20 TB | 1.79 TB by size, 4.19 TB capped | 1.79, 4.19 TB |
| C5 `rep3`, ten nodes of 0.5 to 4 TB | 7.17 TB | 6.45 TB | 6.45 TB | 6.42 TB | 6.42 TB |
| C6 `rep2`, nodes of 4, 1, 1 TB | 3.00 TB | 2.70 TB | 1.80 TB | 1.80 TB | 1.80 TB |

Available from the table matched the first refusal within 0.3% at the 5th percentile everywhere. With capacity-weighted targets the balance loop never reached its 5% margin in C1, C2, C3 and C6 (up to 200% off target) and ended only because the model stops when no move helps; with targets capped by the failure-domain rule it converged within 2% everywhere and reached the balanced bound within 1%. Weighting by size ignored the other pool in C4 and stranded 2.4 TB. One node's full 256 MiB buffer put up to 2.29× its share on one disk at p99 (3.43× at worst) on C5's ten disks, so a reserve sized by shares is overrun by ten full buffers in every trial (by up to 1.46×).

[spikes/block-enospc](../../spikes/block-enospc/README.md) runs ext4 and XFS on a dm-thin volume whose pool runs out, failing with `BLK_STS_NOSPC` at once or after a hold, and checks `fsync`'s error, overwrites and the mount state. It needs root and was not run; its expected outcomes come from the source above.

## What carries over

1. **Report what the table lets through, not room ÷ overhead.** Ceph's weight-based projection and MinIO's sum both assume a share per disk that the failure-domain rule may forbid. Vitastor's per-OSD rule from the actual table is exact up to hashing variance (model: 0.3%).
2. **Keep a separate figure for what placement could reach.** Vitastor's linear program maximizes it; a closed form over failure domains gives it for rock-solid's simpler rules, and the difference from the j = 0 term is stranded space an operator can act on.
3. **Balance toward a reachable target and stop when nothing helps.** Ceph's balancer can "shuffle data forever" toward weight-proportional targets on mismatched devices; the model's loop with capacity-weighted targets never converged on unequal domains.
4. **Reserve at `write()` to report at `write()`.** Every local filesystem read reserves before it buffers, with per-CPU batches that tighten near zero (XFS) or a switch to synchronous allocation when free space runs low (ext4). Every network client read reports late, at the next `write()`, `fsync` or `close`, and CephFS discards buffered data rather than hold it, because held dirty data blocks the deletes that would free space.
5. **Near full, stop buffering.** CephFS syncs every write once a pool is `nearfull`; ext4 stops delaying allocation below 150% of dirty space.
6. **Overwrites and deletes keep working on a full device.** dm-thin keeps provisioned, unshared blocks writable and processes discards when out of space; Ceph lets every operation that adds no bytes through a full pool; ext4, XFS, Btrfs and ZFS keep a slice for operations that free space.
7. **Hold block writes briefly, then fail with the space error.** dm-thin and LVM hold for 60 s and fail with `BLK_STS_NOSPC`; Ceph RBD and Vitastor hold forever, which LVM warns ends in hung tasks and deadlocks; failing at once turns a short full period into filesystem damage.

## Proposed design

### Mechanism

**Capacity figures.** A disk's *fill* is its live blocks, from the segment usage table, over its size; its *room* for a pool is `write_limit` × size minus every pool's data on it. The placement table gives each disk's *share* of the pool's new data: its member slots ÷ the PG count, a slot counting 1 for a copy and 1 ÷ `data` for a shard. The control-plane leader computes, per pool (and per node for `locality: node`, over that node's PGs), from the fills each node's heartbeat reports:

- **available** = min over the pool's disks of room ÷ share, less the space grants (below) outstanding on the pool: what uniformly hashed writes can add before the first disk reaches `write_limit`, for data in its encoded form. `statfs`, CSI and the CLI show it.
- **balanced** = min over j of S₍>ⱼ₎ ÷ (b − j·a), domains sorted by room, largest first, for every j with j·a < b, where S₍>ⱼ₎ is the room of all but the j largest domains, b the pool's overhead and a the most of a byte one domain holds (`max_per_domain` for copies, `max_per_domain ÷ data` for shards). It is the largest D with Σᵢ min(cᵢ, a·D) ≥ b·D, reached fractionally by laying the domains' loads end to end around a circle of length D (McNaughton's wrap-around rule), so placement can approach it to within PG granularity. Volumes with `overcommit` off are admitted against it.
- **stranded** = room ÷ b (the j = 0 term) − balanced.
- **reprotectable** = balanced over every domain but the largest: the data the pool can hold and still return to full redundancy after losing that domain.

Per node, the CLI lists each disk's fill, room, share and room ÷ share, so the disk that sets available is named.

**Placement targets.** Step 2 of the placement computation targets each disk's room for the pool, water-filled over failure domains so that no domain is asked for more than `max_per_domain` of a PG's width, and split within a domain by room, and it stops when no move brings the fullest disk nearer its target. Weighting by room rather than size keeps a disk that another pool filled from capping this one (model C4).

**Volume admission.** With `overcommit` off, the pool keeps a reservation, the sum of its volumes' sizes, at the copies' overhead for block volumes in an erasure-coded pool (they stay copies at least `seal_after`). `CreateVolume` and `ControllerExpandVolume` succeed only while reservations plus what snapshots hold stay within balanced, and otherwise fail with `RESOURCE_EXHAUSTED`. Balanced rather than available, because reservations are long-lived while available moves with every rebalance.

**Warnings,** as conditions on the `Pool` resource, events and metrics: `NearFull` once a member disk is within 10 points of `write_limit`, `Full` once one passes it; `Stranded` when stranded exceeds 10% of the pool's room, naming the domains that strand it; `Unbalanced` when available is more than 5% under balanced for longer than placement needs to move members; `CannotReprotect` when the pool holds more than reprotectable. Placement never refuses a layout, since another pool on the same disks (`none`, or a `locality: node` pool) can use the stranded room; `rock-solid pool create` and disk changes print the figures and ask for confirmation when a domain would strand more than half its room.

**Kubernetes.** `GetCapacity` returns, for the class's pool and the node's topology segment (`locality: node` pools per node, others the same figure everywhere): with `overcommit` off, balanced minus reservations as both `available_capacity` and `maximum_volume_size`; with it on, available as `available_capacity` and, as `maximum_volume_size`, 0 when available is 0 and otherwise the largest volume the pool accepts, so the scheduler keeps `WaitForFirstConsumer` pods off a full node-local pool without refusing thin volumes larger than what is free. `statfs` reports the PVC size, the slot counters' sum as used, the rest as free, and as available the smaller of the rest and the pool's available figure, so `df` shows what can be written.

**Space grants.** The daemon buffers a write only against a *space grant*: bytes of a pool it may hold unflushed. Each node's heartbeat to the control-plane leader, the one that renews its [session](../../docs/design.md#sessions-and-fencing), reports per pool what it holds buffered and what it flushed since the last one, and asks for more; the reply sets the grant, at most `writeback_buffer`. The daemon reuses its grant as each flush completes, so a node streaming faster than one buffer per heartbeat keeps buffering. The leader counts as used every grant plus what nodes flushed since the fills it holds were reported, and keeps the pool's grants at most half of available counted before them, so grants shrink as the pool fills and daemons turn to writing through before it is full, as CephFS does at `nearfull`. It also keeps them within the smallest *gap* any of the pool's disks has between `write_limit` and its log reserve, less what that disk can write in one heartbeat interval: a granted flush is admitted past `write_limit`, all of a node's buffer can land on one disk (B1: 3.4× its share at worst), and the leader learns of flushes up to a heartbeat late, so this is what keeps granted flushes off the reserve. A grant lasts as long as the session renewal that carried it; a new leader counts every live session as holding `writeback_buffer` per pool until it renews. A write the grant does not cover is written through: the daemon flushes that chunk's buffered writes, sends the write to the primary as for a shared file, and returns `ENOSPC` from `write()` if the pool refuses it. The PVC size is checked first, from the cached slot counters as now: it bounds the file's logical size, while the grant bounds new physical space, which an overwrite needs too. Mapped writes cannot be granted, since the kernel sends them at writeback; a refusal reaches `msync` or `fsync`.

**Errors after acknowledgment.** A flush can still fail: the log has no segment the cleaner can free, the session was fenced, or the daemon crashed. The daemon then drops the data, which a recall would otherwise wait on forever (CephFS's reason), and reports the error once to each handle open on the file at the time, at that handle's next `write()`, `fsync` or `close()`, which the kernel turns into `FUSE_FLUSH`: `ENOSPC` for space, `EIO` otherwise. That is `errseq_t`'s once-per-open-file rule, extended to `close()` as NFS does.

**Admission at the primary.** The primary refuses with `NoSpace` a write that adds live data once a member disk's fill, as the member last reported it with an acknowledgment or heartbeat, plus the grants outstanding on the pool times that disk's share, is past `write_limit`. Three kinds pass: a flush under a grant; a write that only replaces blocks the chunk holds live, written after its latest freeze, held as copies, and needing no new freeze, which in the log kills as many blocks as it writes (dm-thin's provisioned-and-unshared rule); and punches. A granted flush needs only its grant; the overwrite condition is checked only while a member is past `write_limit`, with one index lookup per run of blocks.

**Block volumes on a full pool.** A refused block write is held, not failed: the daemon retries it when the leader reports the pool's available figure has grown (a delete, a discard, a disk added, `write_limit` raised), for up to the StorageClass setting `full_hold` (default 60 s; 0 fails at once), then completes it with `-ENOSPC`, which the kernel turns into `BLK_STS_NOSPC`. Reads, discards and overwrites carry on meanwhile, since they pass `write_limit`. A held request keeps its ublk tag, so at most half of a queue's depth is held and the oldest held write beyond that fails, leaving tags for the discards that free space. An NVMe-oF target answers Capacity Exceeded, which the Linux host maps to `BLK_STS_NOSPC`, and holds at most 20 s, under the host's 30 s I/O timeout. While writes are held or failed, the node plugin posts a Warning event on the PVC, and the pool's condition is `Full`.

**The layers, top to bottom.** PVC size (file volumes, at `write()`: `ENOSPC`); space grant (buffered file writes: write through without one); `write_limit` at the primary (writes that add data: `ENOSPC` to `write()`, or held then `BLK_STS_NOSPC` on block); the log's admission when the cleaner lags (slower, then waiting); the log reserve (only checkpoints, the cleaner and merges; `NoSpace` only when nothing can be freed, which reaches the application as a late error). Punches pass every layer but the last, where they wait like any item; whether they should draw on part of the reserve stays the open question "Deletes on a full disk", rewritten below.

### Alternatives rejected

| Alternative | Who uses it | Why not |
| --- | --- | --- |
| Free raw ÷ overhead | rock-solid's current text, MinIO's usable capacity | Overstates by 1.36 to 3.3× on unequal domains (model C2, C3), and ignores other pools on a disk |
| Projection from CRUSH-like weights | Ceph `MAX AVAIL` | The same j = 0 term whenever the failure-domain rule binds (model C1 to C3, C6) |
| A linear program for placement | Vitastor's `lp_optimizer` | Capped water-filled targets with the existing greedy moves reached the bound within 1% (model), without a solver in the control plane |
| Refuse layouts that strand space | Nobody read | Another pool on the same disks can use it; a warning and a confirmation at creation suffice |
| Report late only: at the next `write()`, `fsync` or `close` | Linux NFS client, CephFS, JuiceFS | Applications that ignore `close()`, or exit, lose acknowledged data silently; local filesystems return `ENOSPC` from `write()`, and applications are written against them |
| Write through only once the pool is near full | CephFS's `nearfull` sync, ext4's switch from delayed allocation | Data buffered before the switch, which a node learns of up to a heartbeat late, can still meet a full pool while other writers fill it; grants cost the same heartbeat and bound it |
| Grants per disk, from each disk's owner | XFS's per-CPU counters, distributed | Exact per disk, but a grant per node and disk, requests to every storage node (compute-only nodes have no node-pair heartbeats), and a counter per member per `write()`; `write_limit` is soft with room above it, so a pool grant capped by that room is enough |
| A "near full" flag on primary replies | Suggested in the question | Arrives with the first flush that touches a full disk, after the bytes were acknowledged |
| Refuse every write past `write_limit`, overwrites included | rock-solid's current text | A full pool fails journal writes and overwrites of preallocated files; ext4 aborts its journal, XFS shuts down |
| Fail block writes at once | dm-thin `error_if_no_space` | A full period of seconds, while a purge or the cleaner frees space, becomes a filesystem error |
| Hold block writes forever | Ceph RBD, Vitastor `client_retry_enospc` | Tasks hang in uninterruptible sleep past the 120 s hung-task warning, pods cannot be killed or rescheduled, and held tags block discards; LVM warns of deadlocks |
| `EIO` for a full pool | Any server that maps unknown errors to `BLK_STS_IOERR` | The filesystem and `fsync` report an I/O error where the cause is space |

### Costs

- **Capacity figures:** per pool per heartbeat, O(disks) for available and O(domains × log domains) for balanced at the leader; shares change only with the placement epoch. Per-disk fills add about 8 bytes per disk to each storage node's heartbeat.
- **Space grants at 10 nodes:** no new message; about 16 bytes per pool on each heartbeat and its reply, 10 per second each way; one counter per node and pool at the leader, one per pool at each daemon, one subtraction per `write()`. Promised but unflushed data is at most 10 × 256 MiB = 2.5 GiB of the pool's available figure, held only while nodes buffer.
- **The gap above `write_limit`** must hold the grants plus a heartbeat of one disk's writes. A 100 GB disk at 90% has about 9.8 GB above the limit after its 8-segment reserve: room for 2.5 GiB of grants and a second of NVMe writes (1 to 3 GB) with 4 GB to spare. A 32 GB NVMe disk has about 3.1 GB, which a second of its writes nearly fills, so its pool's grants stay small and its daemons write through more often: small, fast disks pay in write latency for the guarantee.
- **Writing through near full:** a round trip plus a flush per `write()`, about 1 ms on the measured NVMe drive ([NVMe flush spike](../../docs/design.md#appendix-nvme-flush-spike)), against about 5 µs buffered, once a pool's available figure falls below twice what its nodes buffer.
- **Overwrite check:** one chunk-index lookup per run of blocks, 0.3 to 0.6 µs per run searched ([LSM runs spike](../../docs/design.md#appendix-lsm-runs-spike)), only while a member is past `write_limit`.
- **Block hold:** up to `full_hold` of stalled I/O in the pod, and up to half of each ublk queue's tags held.

### What the simulator and tests check

- **Invariants:** a flush under a grant is never refused for space; the grants outstanding on a pool plus a heartbeat of each disk's writes never exceed the gap between that disk's `write_limit` and its log reserve; a disk passes `write_limit` only through granted flushes, overwrites, punches and recovery; every acknowledged write that later fails reaches each handle open at the time, once, at its next `write()`, `fsync` or `close`; no held block write completes after `full_hold`, and held writes never take more than half a queue's tags.
- **Simulation faults:** the pool reaching `write_limit` while daemons buffer and other nodes write through; a heartbeat carrying a grant lost or delayed; a control-plane failover with grants out; a disk failure whose refill moves members onto the fullest disks while grants are out; a snapshot cut while a volume overwrites near full (its overwrites then need new space); a block write held while a discard, a purge or a new disk frees space; a write that crosses from a granted chunk into a refused one.
- **Property tests on placement:** for random layouts, capped targets converge, available ≤ balanced ≤ the j = 0 term, available is within 1% of balanced once converged, and uniformly hashed fills reach available within 1% (the model's C tests as seeds).
- **Real kernels:** the block-enospc spike's T1 to T6 in CI where root is available, and a FUSE test that a late error reaches `close()` and `fsync` on every handle once, and a short `write()` followed by `ENOSPC`.

### Replacement text for `docs/design.md`

#### "Placement table", the two numbered steps and the sentence after them

````markdown
1. Refill every missing member onto the eligible disk furthest below its target.
2. While a disk is outside a fill margin of its target, move one member from the disk furthest above its target to the eligible disk furthest below; stop when no move brings the fullest disk nearer its target.

A disk's target is its room for the pool, what `write_limit` leaves after every pool's data on it, water-filled over failure domains so that no domain is asked for more than `max_per_domain` of a PG's width, and split within a domain by room. A target proportional to size alone cannot be met when failure domains differ: two nodes of 0.7 and 2 TB under `rep2` each hold one copy of every PG, whatever their sizes, and in the [capacity model spike](#appendix-capacity-model-spike) the loop never converged on such layouts, while capped targets converged within 2% and reached the [balanced capacity](#volume-size-and-capacity) within 1%. Weighting by room also keeps a disk another pool filled from capping this one. Both steps keep the pool's failure-domain rule. This gives exact balance on small clusters with mismatched disks, where a placement function every node evaluates (Ceph CRUSH, Weka's algorithmic ownership) would leave disks unbalanced; Ceph patches the same problem with `pg-upmap` exception tables and a balancer that writes them, so in practice it runs a table too, and its balancer warns that on mismatched devices a tight deviation "may result in the balancer shuffling data forever as it endeavors to meet an impossible expectation".
````

(The rest of the paragraph, from "Each table change moves only the members the algorithm chose", stays.)

#### "Volume size and capacity", whole section

````markdown
### Volume size and capacity

A volume, block or file, can be larger than any single drive. Its chunks are spread across every disk of its pool through placement groups, so the limit is the pool's capacity, and a pool is full when its first disk is: every volume hashes over every PG, so once one disk refuses writes, some of every volume's writes fail.

**What a pool reports.** A disk's *fill* is its live blocks, from its segment usage table, over its size, and its *room* for a pool is what `write_limit` leaves after every pool's data on it. The placement table gives each disk's *share* of the pool's new data, its member slots over the PG count, a shard counting `1 ÷ data`. From the fills that heartbeats report, the control plane computes, per pool, and per node over the node's own PGs for `locality: node`:

| Figure | Definition | Used for |
| --- | --- | --- |
| Available | The smallest room ÷ share over the pool's disks, less the [space grants](#client-design) outstanding on the pool | `statfs`, CSI capacity, the CLI |
| Balanced | The most placement could make available: min over j of S₍>ⱼ₎ ÷ (b − j·a) | Admitting volumes with `overcommit` off |
| Stranded | Room ÷ b, minus balanced | The `Stranded` warning |
| Reprotectable | Balanced over every failure domain but the largest | The `CannotReprotect` warning |

Here b is the pool's overhead (`replicas`, or `(data + parity) ÷ data` for encoded data), a the most of each byte one failure domain may hold (`max_per_domain` copies, or `max_per_domain ÷ data` for shards), domains are sorted by room, largest first, S₍>ⱼ₎ is the room of all but the j largest, and j runs while j·a < b. Balanced is the largest D for which Σᵢ min(roomᵢ, a·D) ≥ b·D: no domain holds more than its room, nor more than `max_per_domain` members of any chunk. Room ÷ b is its j = 0 term, which is what Ceph's `MAX AVAIL` and MinIO's usable capacity amount to; it holds only while every domain can take a share proportional to its room. A `rep2` pool on two nodes with 0.7 and 2 GB disks has 1.215 GB of room ÷ 2, but every chunk needs a copy on the small disk, so balanced and available are 0.63 GB; `ec4-2` at 2 per node on nodes of 4, 4 and 1 TB has 8.1 TB of room, 5.4 TB ÷ 1.5, but holds 1.8 TB, since each node takes a third of every chunk. Available is what Vitastor reports: in the [capacity model spike](#appendix-capacity-model-spike), uniformly hashed fills reached it within 0.3% in every layout ([capacity and ENOSPC study](../studies/capacity-and-enospc/README.md)). Available and balanced assume new data is encoded, as encoding on write lands most file data; data held as copies counts as used at its size. Pools that share disks draw on the same room, so their figures overlap. The CLI also lists each disk's fill, room, share and room ÷ share, which names the disk that sets available.

**Warnings.** A pool raises conditions on its `Pool` resource, as events and as metrics: `NearFull` once a member disk is within 10 points of `write_limit`, `Full` once one passes it, `Stranded` when stranded exceeds 10% of the pool's room, naming the domains that strand it, `Unbalanced` when available stays more than 5% under balanced after placement has had time to move members, and `CannotReprotect` when the pool holds more than reprotectable, so that losing its largest domain would leave recovery no room to restore full redundancy. Placement never refuses a layout, because another pool on the same disks, such as `none` with `locality: node`, can use the room one pool strands; `rock-solid pool create` and disk changes print the figures and ask for confirmation when a domain would strand more than half its room.

- **Thin provisioning.** A chunk exists only after its first write, so creating a volume allocates nothing. With `overcommit` on, volume sizes may add up to more than the pool holds. With it off, the pool keeps a reservation, the sum of its volumes' sizes, at the copies' overhead for block volumes in an erasure-coded pool, since they stay copies for at least `seal_after`; `CreateVolume` and `ControllerExpandVolume` succeed only while reservations plus what snapshots hold stay within balanced, and otherwise fail with `RESOURCE_EXHAUSTED`. Balanced, not available, because a reservation outlives every rebalance.
- **Write limit.** A write that adds live data fails with `ENOSPC` once a disk holding one of its PG's members is past the pool's `write_limit` (default 90%), counting the grants outstanding on the pool by the disk's share; the primary decides from the fill each member reports with every acknowledgment and heartbeat. Three kinds of write pass: a flush of buffered data under a space grant, which was promised when the daemon acknowledged it; a write that only replaces blocks the chunk holds live, written after its latest freeze and held as copies, and that needs no new freeze, which kills in the log as many blocks as it writes, as dm-thin keeps writing provisioned, unshared blocks when its pool is out of space; and punches, which free space, as Ceph lets every operation that adds no bytes through a full pool. So a filesystem's journal and preallocated database files keep working on a full pool. The primary checks the overwrite only while a member is past the limit, one index lookup per run of blocks. The limit is checked per disk, not as a pool average, because one full disk stalls every PG on it; CockroachDB likewise stops placing replicas on a store over 95% full. The log's reserve and the cleaner's free segments sit below the limit, so a disk at its limit still cleans, checkpoints and merges ([Free space](#on-disk-format)). Pools that share a disk each check their own limit against the disk's total fill, so a pool with a higher limit can fill the disk past a lower one, and the lower one's writes stop there. The headroom serves recovery and the [cleaner](#on-disk-format): with random 4K overwrites, cleaning write amplification is about 3 at 80% full, 7 at 90% and 17 at 95%, where 94% of the disk's writes would be cleaning ([cleaner spike](#appendix-cleaner-write-amplification-spike)). Chunks written whole go to [chunk slots](#on-disk-format) and cost no cleaning whatever order they are deleted in, so a pool of large files, such as backups or footage archives, can raise its limit to 95%. Its small files and file tails still clean like block writes.
- **Snapshots use space.** Versions pinned by [snapshots](#snapshots-and-clones) count toward the pool, and an overwrite of a block a snapshot shares needs new space, so past `write_limit` it fails like a new write.
- **Wider blast radius.** A volume spread over every disk is hit by any failure, anywhere in its pool, that takes out more members of one PG than the pool survives. Recovery must restore a degraded PG before a second failure arrives, so it starts with the PGs that have lost the most members.
- **A filesystem volume's size is a quota.** The PVC size caps the sum of its files' sizes. Each inode slot keeps a byte counter (`u`, in the [key layout](#metadata-layer-metastore)), updated in the transaction that changes a file's size, so no single key is hot. `statfs` reports the PVC size as the total, the sum of the slot counters as used, the rest as free, and as available the smaller of the rest and the pool's available figure, so `df` shows what can still be written. The daemon caches the sum and the pool's figure for `statfs_cache` (default 5 s) and fails writes that would extend files past the PVC size with `ENOSPC`, before it looks at its space grant. Writers can overshoot by what they add in one cache period.
- **What Kubernetes sees.** `GetCapacity` answers for the class's pool and the node's topology segment, per node for `locality: node` pools and the same everywhere for others. With `overcommit` off, `available_capacity` and `maximum_volume_size` are balanced minus reservations. With it on, `available_capacity` is available and `maximum_volume_size` is 0 when available is 0 and otherwise the largest volume the pool accepts, so the scheduler keeps `WaitForFirstConsumer` pods off a node whose node-local pool is full but still places thin volumes larger than what is free. The check is advisory: a `CreateVolume` that the pool refuses returns `RESOURCE_EXHAUSTED`, and Kubernetes reschedules.
- **Largest file.** A file can't grow past the volume setting `max_file_size` (default 16 TiB, ext4's limit), which bounds a [purge](#freeing-space) to 4 million chunk IDs.

**How a full pool reaches a writer.** From the top:

| Check | Where | What the writer sees |
| --- | --- | --- |
| PVC size (file volumes) | Daemon, at `write()` | `ENOSPC` |
| Space grant (buffered file writes) | Daemon, at `write()` | The write goes through to the primary instead of the buffer |
| `write_limit`, for writes that add data | Primary | `ENOSPC` from `write()`; on a block volume the write is held for `full_hold`, then fails with `BLK_STS_NOSPC` ([Block frontend](#block-frontend)) |
| Cleaner behind | Disk log | Slower writes, then waiting ([Writers slow down](#on-disk-format)) |
| Only the reserve free, nothing to clean | Disk log | `NoSpace`, which reaches the application late, at its next `write()`, `fsync` or `close` ([Client design](#client-design)) |
````

#### "Pools and redundancy", the `write_limit` row

````markdown
| `write_limit` | 90% | Disk fill past which writes that add data fail with `ENOSPC`; see [Volume size and capacity](#volume-size-and-capacity) |
````

#### "Client design", rule 2, the write path, and two paragraphs after it

Rule 2:

````markdown
2. **Write-back buffering lives in the daemon.** Every `write()` reaches the daemon as a FUSE request (about 5 µs). Without `writeback_cache` the kernel sends each `write()` with exactly its offset and length (`fs/fuse/file.c`, `fuse_perform_write`), and direct I/O unaligned too, so the daemon takes any byte range. While the file is not shared, the daemon buffers writes in its own memory and acknowledges at once, as long as its [space grant](#client-design) covers them. It flushes a chunk as soon as it is full, and the rest on `fsync`, `close` or a recall. A flush covers whole blocks wherever the daemon holds a block's other bytes or they lie past the end of file, which no other node can change while the file is not shared, and bytes past the end are zero on disk ([Freeing space](#freeing-space)): a new small file goes out as one block padded with zeros, and the daemon keeps a file's last partial block after flushing it, until a recall, so the next append's flush is a whole block too. The chunk primary [merges](#data-layer-chunkstore) every other partial block, which is what write-shared files pay. The buffer holds up to 256 MiB per node (`writeback_buffer`); when it is full, the oldest partial chunk is flushed. The daemon therefore always knows when a file has been written, and can flush it on demand.
````

Write path:

````markdown
```
FUSE write(inode, off, data)
  past the PVC size (cached)  → ENOSPC
  file not shared and the grant covers it:
                   take it from the grant, buffer, acknowledge   // flushed later, in order
  otherwise:       flush the chunk's buffered writes first
                   chunk_id = (volume, inode, off / CHUNK_SIZE)
                   → ChunkStore.write via the chunk primary, which merges partial blocks
                   → acknowledge, or ENOSPC if the pool refuses it
                   → if the write extends the file: MetaStore size = max(size, end)
any chunk write    → first, if its index >= alloc_end: MetaStore.txn([raise alloc_end, set size written so far])
close() / fsync()  → flush buffered chunks, widened to whole blocks where the daemon holds the bytes
                   → MetaStore.txn([set size, mtime])
                   → an error this handle has not seen yet, if any
```
````

New paragraphs after "`alloc_end` bounds the chunks ...":

````markdown
**Space grants.** A buffered write is acknowledged before any disk takes it, so it must not fail later for lack of space: ext4, XFS and Btrfs give that guarantee by reserving blocks at `write()` (`ext4_da_reserve_space`, `xfs_dec_freecounter`, `btrfs_delalloc_reserve_space`), while the Linux NFS client, CephFS and JuiceFS report a full server only at a later `write()`, `fsync` or `close` ([capacity and ENOSPC study](../studies/capacity-and-enospc/README.md)). So the daemon buffers only against a *space grant*: bytes of a pool its node may hold unflushed. The heartbeat each node sends the control-plane leader every second, which renews its [session](#sessions-and-fencing), reports per pool what the node holds buffered and what it flushed since the last one, and asks for more; the reply sets the grant, at most `writeback_buffer`. The daemon reuses its grant as each flush completes, so a node streaming faster than one buffer per second keeps buffering. The leader counts as used every grant plus what nodes flushed since the fills it holds were reported, and keeps a pool's grants at most half its [available](#volume-size-and-capacity) figure counted before them, so grants shrink as the pool fills and daemons turn to writing through before it is full, as CephFS's client syncs every write once its pool is nearly full. It also keeps them within the smallest gap any of the pool's disks has between `write_limit` and its log reserve, less what that disk can write in one heartbeat interval: a granted flush passes `write_limit`, all of a node's buffer can land on one disk (one full buffer put up to 3.4 times its share on one disk in the [capacity model spike](#appendix-capacity-model-spike)), and the leader learns of flushes up to a heartbeat late. So a granted flush never reaches the reserve, and is never refused for space. A grant lasts as long as the renewal that carried it, and a new leader counts every live session as holding `writeback_buffer` per pool until it renews. A write the grant does not cover goes through: the daemon flushes that chunk's buffered writes and sends the write to the primary, as for a shared file, so a full pool returns `ENOSPC` from `write()`. Near full, writes then cost a round trip and a flush each instead of 5 µs. The PVC size is checked first; it bounds a file's logical size, while the grant bounds new physical space, which an overwrite needs too. Mapped writes cannot be granted: the kernel sends them at writeback, so a refusal reaches `msync` or `fsync`.

**Errors after acknowledgment.** A flush can still fail: the disk log has no segment the cleaner can free, the session was fenced, or the daemon crashed ([Daemon restart and upgrade](#daemon-restart-and-upgrade)). The daemon drops the data, which a recall would otherwise wait on forever, as CephFS's client discards its cache when a pool fills, and reports the error once to each handle that was open on the file, at that handle's next `write()`, `fsync` or `close()`, which the kernel sends as `FUSE_FLUSH` (`fs/fuse/file.c`, `fuse_flush`): `ENOSPC` for space, `EIO` otherwise. That is the rule `errseq_t` gives local filesystems, where every open file description sees a writeback error once at `fsync`, extended to `close()` as NFS does.
````

#### "Daemon restart and upgrade", the write-back bullet

````markdown
- **A crash loses the write-back buffer, and says so.** Unflushed buffered writes die with the process, as dirty pages die in a kernel crash, but here the application keeps running. The memfd lists the handles holding unflushed data, and the successor fails the next `write()`, `fsync` or `close` on each with `EIO` ([Errors after acknowledgment](#client-design)), so the loss is never silent.
````

#### "Block frontend", a new paragraph after "RWX block (Tier 0)"

````markdown
**A full pool holds writes, then fails them.** A block write the pool refuses at [`write_limit`](#volume-size-and-capacity) is held: the daemon retries it whenever the control plane reports the pool's available figure has grown (a delete, a discard, a disk added, `write_limit` raised), for up to the StorageClass setting `full_hold` (default 60 s, 0 to fail at once), then completes it with `-ENOSPC`, which the block layer turns into `BLK_STS_NOSPC`, "critical space allocation" ([`blk-core.c`](https://github.com/torvalds/linux/blob/v6.18/block/blk-core.c#L142)). Reads, discards and overwrites of blocks the volume already holds carry on meanwhile, since they pass `write_limit`, so the filesystem's journal and everything it wrote before keep working; what fails is new allocation, which ext4 and XFS record against the file, so `fsync` returns `ENOSPC` ([block ENOSPC spike](../spikes/block-enospc/README.md), not yet run). dm-thin and LVM thin pools behave the same way by default (`no_space_timeout`, 60 s). Failing at once would turn a few seconds at the limit, while a purge or the cleaner frees space, into a filesystem error; holding forever, as Ceph RBD and Vitastor do, keeps the pod's tasks in uninterruptible sleep past the kernel's 120 s hung-task warning, and a pod that cannot be killed cannot be rescheduled. A held request keeps its ublk tag, so the daemon holds at most half of a queue's depth and fails the oldest held write beyond that, leaving tags for the discards that free space. Remote clients get NVMe's Capacity Exceeded status, which Linux maps to `BLK_STS_NOSPC`, after at most 20 s, under the host's 30 s I/O timeout. While writes are held or failed, the node plugin posts a Warning event on the PVC, and the pool's condition is `Full`.
````

#### "Testing strategy"

Add to "Simulation faults for freeing space": "a pool reaching `write_limit` while daemons buffer and other nodes write through, a heartbeat carrying a space grant lost or delayed, a control-plane failover with grants out, a refill after a disk failure that moves members onto the fullest disks while grants are out, a snapshot cut while a volume overwrites near full, and a block write held while a discard, a purge or a new disk frees space".

Add to the invariants:

````markdown
- a flush under a space grant is never refused for space, and the grants outstanding on a pool plus a heartbeat of each disk's writes never exceed the gap between that disk's `write_limit` and its log reserve;
- a disk passes `write_limit` only through granted flushes, overwrites of blocks it holds live and unshared, punches and recovery;
- every failed write the daemon acknowledged reaches each handle open on its file at the time, once, at its next `write()`, `fsync` or `close`;
- no held block write completes after `full_hold`, and held writes never take more than half of a queue's tags;
````

Add to "Property tests on placement": "capped targets converge on any layout, available ≤ balanced ≤ room ÷ overhead, and once converged available is within 1% of balanced".

#### "Alternatives considered", three new rows

````markdown
| Pool capacity | Available from the placement table (the smallest room ÷ share), with a balanced bound over failure domains for admission and stranded space; placement targets each disk's room, capped by the failure-domain rule ([Volume size and capacity](#volume-size-and-capacity)) | Free raw space ÷ overhead (MinIO); Ceph's `MAX AVAIL` from CRUSH weights; Vitastor's linear program for placement; refusing layouts that strand space | Room ÷ overhead overstated by 1.36 to 3.3× on unequal domains and Ceph's rule by the same, while available matched the first refused write within 0.3%; capacity-weighted targets never converged on such layouts, capped ones reached the bound within 1% without a solver ([capacity model spike](#appendix-capacity-model-spike)). Stranded space is usable by another pool on the same disks |
| Full pool under write-back | Space grants per node and pool from the control-plane leader on the session heartbeat, at most half of available and, with a heartbeat of writes, within every disk's gap above `write_limit`; reused as flushes complete; write through without one; a later failure reported once per handle at its next `write()`, `fsync` or `close` ([Client design](#client-design)) | Late reports only (Linux NFS client, CephFS, JuiceFS); writing through only once near full (CephFS `nearfull`, ext4 leaving delayed allocation); grants per disk; a near-full flag on primary replies | Local filesystems return `ENOSPC` from `write()`, and applications that ignore `close()` would lose acknowledged data silently. A near-full switch or flag is learned after bytes were acknowledged. Per-disk grants are exact but need requests to every storage node; `write_limit` is soft, so a pool grant capped by the room above it is enough ([capacity and ENOSPC study](../studies/capacity-and-enospc/README.md)) |
| Block volumes on a full pool | Overwrites of live, unshared blocks and discards pass `write_limit`; writes that add data are held for `full_hold` (60 s), then fail with `BLK_STS_NOSPC` ([Block frontend](#block-frontend)) | Fail at once (dm-thin `error_if_no_space`); hold forever (Ceph RBD, Vitastor); `EIO`; refusing overwrites too | A few seconds at the limit should not abort a journal, and an endless hold hangs tasks past the hung-task warning and blocks pod deletion; dm-thin and LVM default to the same 60 s. `ENOSPC` names the cause in the filesystem and at `fsync` |
````

#### "Roadmap"

- Phase 3: "placement groups and the placement table computed by the control plane" → "placement groups and the placement table computed by the control plane, with targets capped by the failure-domain rule, pool capacity figures and `write_limit` admission that lets overwrites and punches through".
- Phase 4: "Sessions in the control-plane group;" → "Sessions and space grants in the control-plane group;".
- Phase 6: after "PVC size quotas," add "space grants with writing through and late errors per handle,".
- Phase 8: after "discard," add "holding writes on a full pool,".

#### "Open questions"

Replace "Deletes on a full disk" with:

````markdown
- [ ] Deletes on a full disk: punches pass a pool's `write_limit`, but with no free segment outside a disk's reserve they wait in its log like any item, and so do the MetaStore's Raft entries for `unlink`. Should records holding only punches, and Raft entries, draw on a slice of the reserve? Every system read keeps one: ext4 min(2%, 4,096 clusters) for punches, XFS a reserve pool of min(5%, 8,192 blocks), Btrfs its global reserve for `unlink`, ZFS its slop space, CockroachDB a ballast file of min(1 GiB, 1%) that an operator deletes ([capacity and ENOSPC study](../studies/capacity-and-enospc/README.md)). How many segments does purging a full disk's largest volume take, at 33 bytes per punch entry?
````

Add:

````markdown
- [ ] `full_hold` and the filesystem in a pod: with overwrites passing `write_limit`, do ext4 and XFS on a block volume keep their journals and report `ENOSPC` at `fsync` when new allocations fail, and does a 60 s hold end without hung-task warnings or I/O errors once space appears? `spikes/block-enospc/run-root.sh` measures it on dm-thin, which needs root.
- [ ] Space grants on small, fast disks: the gap above `write_limit` must hold every grant plus a heartbeat of the disk's writes, which on a 32 GB NVMe disk leaves almost nothing, so its pool's daemons write through. Would a shorter heartbeat for grants, or each disk's measured write rate in place of its peak, let such pools buffer, and at what cost in control-plane load?
````

#### New appendix

````markdown
## Appendix: capacity model spike

**Question.** What can a pool promise to `statfs`, CSI and the daemons' write-back buffers when its failure domains differ in size? See [spikes/capacity-model](../spikes/capacity-model/README.md).

**Setup.** A Python model with numpy, no I/O: integer placement tables built by the design's refill and balance steps, with capacity-weighted targets and with targets capped by the failure-domain rule, filled with uniformly hashed 4 MiB chunks until the first chunk that would put a member disk past `write_limit` (90%), 40 fills per table; then one node's full 256 MiB buffer and ten of them against each disk's share.

| Test | Result |
| --- | --- |
| C1 `rep2`, 0.7 and 2 GB | Room ÷ overhead 1.22 GB (1.35 GB on free raw space); balanced and first refusal 0.63 GB |
| C2 `rep3`, 4, 4, 2, 1 TB | 3.30 TB against 2.70 TB balanced, 2.68 TB first refusal |
| C3 `ec4-2` at 2 per node, 4, 4, 1 TB | **5.40 TB against 1.80 TB** |
| C4 `none`, a 2 TB disk shared with another pool | Weighted by size, available 1.79 TB; by room, 4.19 TB of a 4.20 TB bound |
| C5 `rep3`, ten mixed nodes | No stranding: 6.45 TB bound, 6.42 TB available |
| Convergence | Capacity-weighted targets never converged in C1, C2, C3 and C6; capped targets converged within 2% everywhere |
| B1, G1 | One full buffer puts up to 2.29× its share on one disk at p99 (3.43× at worst); a reserve of ten buffers × share is overrun in every trial |

**Conclusions.**

1. Room ÷ overhead is the j = 0 term of the balanced bound and overstates by 1.36 to 3.3× on unequal domains; available from the table predicts the first refusal within 0.3%.
2. Placement targets must be each disk's room, capped by the failure-domain rule, and the balance loop must stop when no move helps.
3. A pool-wide grant can land several times its share on one disk, so the grants' total must fit the gap above `write_limit` on every disk.
````

### Replacement text for `docs/glossary.md`

New entries, after **Thin provisioning and overcommit**:

````markdown
**Available, balanced and stranded capacity.** Available is what a pool can still take before its first disk reaches `write_limit`, given where its placement table sends new data; it is what `df` and Kubernetes see. Balanced is the most any placement could make available, given that copies of a chunk must sit in different failure domains; stranded is the room that rule leaves unusable, such as most of the larger disk when two nodes of different sizes each hold a copy of everything. See [Volume size and capacity](design.md#volume-size-and-capacity).

**Space grant.** Bytes of a pool a node may hold in its write-back buffer, granted by the control plane on the node's heartbeat. The daemon acknowledges a buffered write only against a grant, so a full pool is reported by `write()` itself, as a local filesystem reserves blocks before it buffers. See [Client design](design.md#client-design).
````

Rewrite **Thin provisioning and overcommit**:

````markdown
**Thin provisioning and overcommit.** A thin volume takes space only as it is written, so creating a 1 TiB volume allocates nothing. Overcommit lets the sizes of all volumes add up to more than the disks hold, betting they never all fill; with it off, the pool admits a volume only while every volume's size fits its balanced capacity. See [Volume size and capacity](design.md#volume-size-and-capacity).
````

Rewrite **Write-back and write-through**:

````markdown
**Write-back and write-through.** A write-back cache acknowledges a write before passing it on and sends it later; a write-through cache passes it on first. The daemon's **write-back buffer** holds a file's writes while no other node shares the file and the node's space grant covers them; otherwise the daemon writes through.
````

Add a row to "Words with more than one meaning":

````markdown
| Reserve, reservation | The disk log's reserve segments, kept for work that frees space. A pool's reservation of volume sizes with `overcommit` off. A space grant, which reserves pool space for a node's buffered writes |
````

### Replacement text for `docs/build-checklist.md`

- **Pools** (phase 3): append "Capacity figures from member fills and the table: available (the smallest room ÷ share), balanced (min over j of S₍>ⱼ₎ ÷ (b − j·a) over failure domains), stranded and reprotectable, and the `NearFull`, `Full`, `Stranded`, `Unbalanced` and `CannotReprotect` conditions; reservations for `overcommit` off against balanced. Review with the [capacity model spike](design.md#appendix-capacity-model-spike)'s C1 to C6 as data-driven cases: the figures match its results."
- **Placement computation** (phase 3): replace with "Refill, then balance within the fill margin, under the failure-domain rule, toward each disk's room for the pool, water-filled over domains with at most `max_per_domain` of a PG's width each, stopping when no move helps ([placement table](design.md#data-layer-chunkstore)). Review with property tests: after any add, remove and failure sequence, every PG keeps its width with at most `max_per_domain` per domain, the loop terminates, no disk exceeds the fill margin of a reachable target, available is within 1% of balanced, and moves stay near the minimum."
- **Replica primary** (phase 3): append "`write_limit` admission from the fill and outstanding grants each member reports: writes that add live data refused with `NoSpace` past it; granted flushes, overwrites of blocks held live, unshared and as copies with no freeze due, and punches admitted."
- **Control-plane group** (phase 4): after "sessions (...)", add "space grants per node and pool on the session heartbeat, at most half of available and, with a heartbeat of each disk's writes, within every disk's gap above `write_limit`, reused by the daemon as its flushes complete, every live session counted at `writeback_buffer` after a leader failover".
- **Placement faults and invariants** (phase 3): append "a disk passes `write_limit` only through granted flushes, overwrites, punches and recovery; uniformly hashed fills reach the reported available figure within 1%."

### Other changes that go with it

- **`docs/test-scenarios.md`:** nothing yet, since its sections cover phase 1 items. When **Pools**, **Placement computation** and the control-plane group's grants get sections, they start from the model's C1 to C6, a grant renewal across a leader failover, and a write crossing from a granted chunk into a refused one.
- **Blog posts:** [02](../../blogs/02-chunks-and-placement.md#volume-size-and-capacity) (the capacity formula and its worked example), [05](../../blogs/05-the-cleaner-and-free-space.md) (the table of three ways a full disk shows up, and the deletes direction), [07](../../blogs/07-pools-and-redundancy.md) ("it is also how a pool reports its capacity", and the headroom paragraph), [09](../../blogs/09-rwx-filesystem-and-cache-coherence.md) (rule 2 of the client and the restart table's write-back row) and [10](../../blogs/10-block-snapshots-and-clones.md) (a full pool on block volumes, and the trade-off about overwrites after a snapshot) state the current text.

## Unverified

- **What ext4 and XFS do on `BLK_STS_NOSPC`** is read from source: data writeback records the error on the file, a journal or log write failure takes the filesystem down, XFS retries metadata `ENOSPC` forever. Whether a new metadata block refused while overwrites pass still aborts ext4's journal is not known; [spikes/block-enospc](../../spikes/block-enospc/README.md) checks it but needs root and was not run.
- **FUSE error delivery** at `close()` and on a short `write()` is read from `fs/fuse/file.c` and `fs/open.c` at v6.18, not tested. `close()` errors are discarded when a process exits without closing.
- **Grant accounting under load** is reasoned, not modelled: the leader learns of flushes up to a heartbeat late, and the gap bound assumes a disk writes no faster than its measured peak during that heartbeat.
- **The balanced bound's exactness** rests on McNaughton's wrap-around construction and six model layouts with 128 to 512 PGs; small `pg_count` values add PG granularity the model did not vary.
- **CSI volume health** is alpha in the spec read, and whether Kubernetes consumes it was not checked, so the proposal reports through events and the `Pool` resource instead.
- **Not read:** Longhorn's behaviour when a replica's disk fills during writes, MinIO's documentation on unequal drives, the `nfs(5)` man page (the host is refused here), and Ceph's tracker on `MAX AVAIL`.
- **Branch heads, not releases.** Ceph, Vitastor, Longhorn, CockroachDB, MinIO, JuiceFS, Kubernetes, external-provisioner, the CSI spec and LVM2 were read at their default branches on 2026-10-19; Linux at v6.18. The commits are pinned in `fetch.sh`.

## Reproduce

`./fetch.sh [dir]` downloads every cited file at the pinned commits into `dir` (default `./work`) and prints the cited lines. It needs `curl`. The model runs with `spikes/capacity-model/run.sh` (Python 3 and numpy); the block spike with `sudo spikes/block-enospc/run-root.sh`.

## Sources

- Ceph at [`dcd6d90f`](https://github.com/ceph/ceph/tree/dcd6d90fabcee78f697a55151112d49f234668a3): [`src/mon/PGMap.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/mon/PGMap.cc), [`src/crush/CrushWrapper.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/crush/CrushWrapper.cc), [`src/osd/OSDMap.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/osd/OSDMap.cc), [`src/mon/OSDMonitor.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/mon/OSDMonitor.cc), [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/osd/PrimaryLogPG.cc), [`src/client/Client.cc`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/client/Client.cc), [`src/common/options/global.yaml.in`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/src/common/options/global.yaml.in), [`doc/cephfs/full.rst`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/doc/cephfs/full.rst), [`doc/rados/operations/balancer.rst`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/doc/rados/operations/balancer.rst), [`doc/rados/operations/monitoring.rst`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/doc/rados/operations/monitoring.rst), [`doc/rados/troubleshooting/troubleshooting-osd.rst`](https://github.com/ceph/ceph/blob/dcd6d90fabcee78f697a55151112d49f234668a3/doc/rados/troubleshooting/troubleshooting-osd.rst).
- Linux [v6.18](https://github.com/torvalds/linux/tree/v6.18): `fs/ext4/{inode,balloc,super,page-io}.c`, `fs/jbd2/commit.c`, `fs/xfs/{xfs_file,xfs_iomap,xfs_mount,xfs_zone_space_resv,xfs_sysfs,xfs_log}.c`, `fs/xfs/xfs_mount.h`, `Documentation/admin-guide/xfs.rst`, `fs/btrfs/{file,inode,delalloc-space}.c`, `fs/nfs/{file,write,fs_context}.c`, `fs/nfs/internal.h`, `fs/ceph/{file,super,quota}.c`, `net/ceph/osd_client.c`, `fs/fuse/file.c`, `fs/open.c`, `mm/filemap.c`, `include/linux/pagemap.h`, `Documentation/core-api/errseq.rst`, `drivers/md/dm-thin.c`, `Documentation/admin-guide/device-mapper/thin-provisioning.rst`, `drivers/block/ublk_drv.c`, `block/blk-core.c`, `kernel/hung_task.c`, `lib/Kconfig.debug`, `drivers/nvme/host/{core,tcp}.c`.
- Vitastor at [`df593bad`](https://github.com/vitalif/vitastor/tree/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff): `src/cmd/cli_pool_ls.cpp`, `mon/lp_optimizer/lp_optimizer.js`, `docs/usage/cli.en.md`, `docs/config/client.en.md`, `src/client/cluster_client.cpp`.
- Longhorn manager at [`d1968651`](https://github.com/longhorn/longhorn-manager/tree/d1968651ccc4bf60c97f55b7dcafdf7e228b5462): `csi/controller_server.go`, `types/setting.go`, `scheduler/replica_scheduler.go`.
- CockroachDB at [`d30c905f`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff): `pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go`, `pkg/storage/ballast.go`, `pkg/roachpb/metadata.go`.
- MinIO at [`7aac2a2c`](https://github.com/minio/minio/tree/7aac2a2c5b7c882e68c1ce017d8256be2feea27f): `cmd/notification-summary.go`, `cmd/object-api-utils.go`, `cmd/globals.go`.
- JuiceFS at [`506808b0`](https://github.com/juicedata/juicefs/tree/506808b0c521984e1780ff6ae8ca05cc0caddfaf): `pkg/vfs/writer.go`, `pkg/meta/quota.go`, `pkg/meta/redis.go`, `docs/en/guide/cache.md`.
- Kubernetes at [`7f9d9d6a`](https://github.com/kubernetes/kubernetes/tree/7f9d9d6a3d6e48be4d8d261c9474618d7f8d303d): `pkg/scheduler/framework/plugins/volumebinding/binder.go`; website at [`1fc565c2`](https://github.com/kubernetes/website/tree/1fc565c2f557823a911f073dc577945230f27dd7): `content/en/docs/concepts/storage/storage-capacity.md`; external-provisioner at [`0d12300b`](https://github.com/kubernetes-csi/external-provisioner/tree/0d12300b577bf0fd017c3ddea1f435669cc1cfdb): `README.md`, `pkg/capacity/capacity.go`, `pkg/controller/controller.go`; CSI spec at [`25b3f102`](https://github.com/container-storage-interface/spec/tree/25b3f10249abe6033887ed5177e856b9f73b1105): `spec.md`.
- LVM2 at [`0b58c33c`](https://github.com/lvmteam/lvm2/tree/0b58c33cdc51d26087a6e69c2f19ed660a43e989): `man/lvmthin.7_main`.
