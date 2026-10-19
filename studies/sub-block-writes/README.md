# Sub-block writes and small files: where bytes become blocks

**Status:** read from source and documentation, plus a model spike ([spikes/sub-block-writes](../../spikes/sub-block-writes/README.md)). Nothing here is adopted; the proposed design at the end is for review.

**Question:** the ChunkStore API takes byte ranges (`Write(c, off, data, expectVer)`, `Punch(c, off, n, …)` in [Data layer](../../docs/design.md#data-layer-chunkstore)), but everything below it works in whole 4 KiB blocks: a record's write and punch entries name block ranges, each data block carries a CRC32C, the chunk index's live map holds runs of whole blocks, freezes share whole blocks with snapshots, and erasure-coded chunks go back to copies on overwrite. The design does not say what happens to (a) files smaller than a block, 1 to 2 KiB, and (b) writes that cover part of a block, such as 100 bytes at offset 1000 or a 300-byte append. Is it a read-modify-write merge, where, against which version, and what does it cost? The same for punches, truncates and extends inside a block, small fsync'd appends, and the block frontend's logical block size.

## Summary

Every system read handles sub-block writes in one of three ways. **Merge into whole blocks where the order is decided**: BlueStore pads a small write with head and tail reads on each replica, Ceph's erasure-coded primary keeps an extent cache of pending writes so pipelined overwrites merge on each other, and VitastorFS merges on the client with compare-and-set on the object version. **Log the bytes and merge later**: the ZFS intent log copies small synchronous writes into log records and writes whole records at the next transaction group; JuiceFS and SeaweedFS keep byte-granular slices and resolve them at read time, compacting when they pile up. **Keep a floor**: XFS, BlueStore since Pacific and Vitastor allocate at least 4 KiB, and Vitastor exposes 4 KiB sectors to its block clients. Inline data for small files exists in ext4, Btrfs, F2FS, ZFS, WAFL and Lustre, but CephFS, the only distributed system read that put file data in its metadata servers' inodes, has deprecated it since Octopus because the kernel client's uninlining on write is racy.

The proposal: **the chunk primary merges every partial block against the newest version it has ordered, pending versions included, and sends the merged whole blocks to the replicas.** Records, checksums, the chunk index and snapshots stay block-granular, with no format change. Three rules make it safe, each checked by the spike's model: merge against pending versions, not just committed ones (otherwise pipelined or concurrent writes lose bytes); peering drops only a suffix of a chunk's pending versions; a new primary merges into a chunk only once it holds every version peering kept. The client daemon sends whole blocks whenever it holds the bytes, which in Exclusive mode covers appends and new small files, so only write-shared files and 512-byte block writes pay the primary's read. Truncates keep bytes past end of file zero on disk through a purge that starts at the byte offset. Small files keep the 4 KiB floor; inline data in the inode is rejected because the MetaStore's runs live in memory. Byte-granular patch entries and short blocks are measured and deferred. Block volumes expose 4 KiB logical blocks, with 512-byte emulation as a per-volume option served by the same merge.

## What the design has today

- **Byte ranges at the top.** `Write(c ChunkID, off uint32, data []byte, expectVer *uint64)`, `Punch(c, off, n, expectVer)` and `Read(c, off, n, atVer)` ([Data layer](../../docs/design.md#data-layer-chunkstore)). A client reaches them with whatever FUSE delivers: without `writeback_cache`, the kernel sends each buffered `write()` as a `FUSE_WRITE` of exactly its position and length (`fs/fuse/file.c:1261`, `fuse_perform_write`; the size is the caller's count, `:1044`), and direct I/O passes the caller's range through.
- **Blocks below.** A record's write and punch entries hold a first block and a block count, `uint16` each (`internal/disklog/record.go:71-77`, encoded at `:279-301`); the data area holds whole blocks, one CRC32C per block (`:246-262`), then packed blobs. The chunk index's extents are `First`, `Count` in blocks plus an LBA (`internal/chunkstore/index.go:86-92`), and `Apply` maps a write entry's blocks at `LBA + HeaderBlocks + payload/BlockSize` (`index.go:177-193`). `core.BlockSize` is 4096 (`internal/core/ids.go:16`).
- **Where bytes must become blocks:** in the code that builds a `disklog.WriteEntry` from an API `Write`, which is the single-node ChunkStore item of phase 1 and the replica primary of phase 3 ([build checklist](../../docs/build-checklist.md)). Neither exists yet, and no document says what they do with a partial block. `grep -i 'aligned\|partial block\|read-modify'` over `docs/` finds only the erasure-coding read-modify-write that the design avoids.
- **Related rules already in place:** a primary maps a write into its index only when it commits, and a chunk's writes in version order ([Reads see only acknowledged writes](../../docs/design.md#data-layer-chunkstore), [chunk epochs spike](../../spikes/chunk-epochs/README.md)); a write that peering drops is superseded by recovery's copy of the authoritative blocks ([Epoch changes](../../docs/design.md#epoch-changes-and-failure-detection)); the truncate path commits the size and a purge entry, then punches the new last chunk's tail, and an extension finishes a pending purge first ([Freeing space](../../docs/design.md#freeing-space)); partial chunks stay in the log ([Chunk slots](../../docs/design.md#on-disk-format)).

## What other systems do

| System | Sub-block write | Where merged, against what | Small files | Small fsync'd appends |
| --- | --- | --- | --- | --- |
| Ceph RADOS, BlueStore | Byte-range op replicated to every member | On **every replica**, in BlueStore: head and tail padding read from its own disk | 4 KiB `min_alloc_size` per replica since Pacific | Sub-allocation-unit overwrites go through the RocksDB WAL as deferred writes |
| Ceph erasure-coded pools | Byte-range op | At the **primary**, against an extent cache of in-flight and recent writes | k + m allocation units | Read-modify-write of the stripe |
| CephFS | As RADOS | — | Inline data in the inode (< 2 KiB), off by default, deprecated since Octopus | — |
| Vitastor | Rejected unless aligned to `bitmap_granularity` (4 KiB) | — | VitastorFS packs files into shared inodes, rounded to 4 KiB | Small writes go to the journal, then to the data area |
| VitastorFS | Byte range from NFS | On the **client** (the NFS proxy): read the 4 KiB block, merge, write with compare-and-set on the version, retry on conflict | As above | As writes |
| JuiceFS | A slice of exactly the written bytes | **Never merged on write**: reads take the newest slice per byte; compaction merges slices | An object of the file's size | One slice per flush |
| SeaweedFS (filer, mount) | A chunk of exactly the written bytes | Never merged on write: visible intervals by modification time | Needles packed at 8-byte alignment | One chunk per flush |
| ZFS | Copy-on-write of the whole record | At the DMU, in memory | Embedded block pointers (112 bytes); otherwise one block, at least 1 << ashift | Intent log record with the data (`WR_COPIED`), at least a 4 KiB log block; the record is written at the next transaction group |
| ext4, XFS | Page cache merges in memory; whole blocks written in place | In memory, against the page cache | ext4 `inline_data` (60 B in `i_block` plus in-inode xattr space, opt-in); XFS none | Data block in place plus a journal or log commit per fsync |
| Btrfs, F2FS | Copy-on-write of the whole block | In memory | Btrfs inline extents up to `max_inline` (2048 B default); F2FS inline data up to about 3.4 KiB in the inode block | Btrfs log tree; F2FS roll-forward node blocks |
| WAFL | Whole 4 KiB blocks at a consistency point | In memory, NVRAM logs the operation | Files of 64 bytes or less in the inode (secondary sources) | NVRAM log, then a consistency point |
| Lustre | Byte-range extent locks | On the OST or MDT backing filesystem | Data on MDT, up to 1 MiB by default, opt-in per layout | — |

### Ceph: BlueStore merges on every replica

- A replicated write carries its byte range to every member, and each member's BlueStore turns it into whole blocks. `_do_write_small` (`src/os/bluestore/BlueStore.cc:16879`) computes the head and tail padding to the chunk size (`:17008`), and when the write lands in an allocated blob it reads the missing head and tail from disk (`:17072-17120`), counting each as a `write_penalty_read_ops`. So a sub-block write costs a read on **every** replica.
- Writes smaller than `min_alloc_size` "must first pass through the BlueStore WAL" (`src/common/options/global.yaml.in:4786`); on HDDs, writes under `bluestore_prefer_deferred_size_hdd` (64 KiB, `:4863`) are deferred too, on SSDs none (`:4873`). A deferred write is written twice: into RocksDB, then in place.
- `min_alloc_size` fell from 64 KiB (HDD) and 16 KiB (SSD) to 4 KiB in Octopus (SSD) and Pacific (HDD), "driven by space amplification that was experienced by Ceph RADOS Gateway (RGW) deployments that hosted large numbers of small files" (`doc/rados/configuration/bluestore-config-ref.rst:452-459`). The same page counts what the floor costs: a 1 KB object "strands roughly 9 KB" at three copies, and "allocates 24 KB" in a k=4, m=2 pool (`:472-478`). Pre-Pacific HDD OSDs still raise a `BLUESTORE_LEGACY_MIN_ALLOC_SIZE` warning (`PendingReleaseNotes`).
- **Erasure-coded overwrites merge at the primary against pending writes.** `ECExtentCache` exists "to improve performance, particularly for small sequential writes, by caching the results of recent reads and writes"; it allows "a single read to be outstanding per PG at a time" and "will never re-order IO" (`src/osd/ECExtentCache.h:4-23`). The primary holds a write to an object it is missing until recovery brings it (`src/osd/PrimaryLogPG.cc:2225`, `wait_for_unreadable_object`) and a write to a degraded object until it is recovered (`:2255-2261`).

### CephFS inline data

- Inline data stores "small files (generally <2KB)" in the inode, served by the metadata server (`doc/cephfs/experimental-features.rst:16-22`). It "has been deprecated since the Octopus release. Enabling it triggers a health warning" (`:27-28`), and turning it on asks for `--yes-i-really-really-mean-it` (`src/mon/FSCommands.cc:475`).
- The kernel client reads inline data but never writes it: opening a file for write uninlines it first (`fs/ceph/file.c:242-243`), copying the data to the first RADOS object guarded by a compare on an `inline_version` xattr (`fs/ceph/addr.c:2197` onward). The deprecation proposal on the ceph-users list and patches titled "ceph: remove the inline data support" (LKML, July 2026) give the reason as race conditions in that uninlining that could corrupt data, which fixing would make complicated client code more complicated. Those two sources were seen only through a search engine's summary; the hosts did not resolve from here.

### Vitastor and VitastorFS

- Block writes must be aligned to `bitmap_granularity`, 4096 by default: "Required virtual disk write alignment ("sector size")" (`docs/config/layout-cluster.en.md:44-54`). So Vitastor's block clients see 4 KiB sectors and never send a sub-block write.
- VitastorFS, its NFS proxy, merges on the client. `nfs_do_rmw` (`src/nfs/nfs_kv_write.cpp:186`) reads the aligned block, overlays the write, and writes it with `version = read version + 1`; a compare-and-set failure re-reads and retries (`:250-256`). Two partial writes to one block within one request are sequenced by version (`:217-221`).
- Small files are packed into shared inodes, "one after another" (`docs/usage/nfs.en.md:297`), each allocated `align_up(24-byte header + size)` (`nfs_kv_write.cpp:57-73`), so still at a 4 KiB granularity; deleting or extending one leaves garbage that a `defrag` command reclaims.

### JuiceFS and SeaweedFS: byte slices, merged on read

- JuiceFS: "each slice represents a single continuous write" (`docs/en/introduction/architecture.md:40`); "the most recent written slice is read for each file position" (`:61`), and compaction merges "all slices within the same chunk into one" (`:67`). A read that sees 5 or more slices starts a background compaction (`pkg/meta/base.go:2178`); a write compacts at every 100th slice or past 350 (`:2234`), and synchronously at 2,500 (`maxSlices`, `:65`). Small files are objects of their own size, so there is no space floor, at the price of per-read merging and compaction.
- SeaweedFS packs needles at 8-byte alignment (`weed/storage/types/needle_types.go:63`). Its filer resolves a file's overlapping chunks into visible intervals by each chunk's modification timestamp (`weed/filer/filechunks.go:265-278`), which is a client clock, so two nodes writing one region with skewed clocks resolve by clock, not by arrival (inferred from the code, not tested).
- Both work because their objects are immutable and their metadata holds the slice list. rock-solid's chunk index is the slice list's equivalent, so the same design would mean a patch chain per block (below).

### ZFS: log the bytes, write the record later

- Small synchronous writes are `WR_COPIED`: "data is copied into lr_write_t" (`include/sys/zil.h:432-459`); writes of at least `zfs_immediate_write_sz` (32 KiB, `module/zfs/zil.c:2215`) are written in place and logged by pointer. An `lr_write_t` carries 192 bytes of header and block pointer before the data (`zil.h:342-353`), and a log block is at least `ZIL_MIN_BLKSZ`, 4 KiB (`zil.h:90`). The record itself is rewritten whole at the next transaction group. So a fsync'd 100-byte append costs at least one 4 KiB log block, then a share of the record's rewrite.
- Embedded block pointers keep up to `BPE_PAYLOAD_SIZE`, 14 × 8 = 112 bytes of compressed data in the pointer itself (`include/sys/spa.h:348-349`).

### Local filesystems

- **ext4** inline data holds 60 bytes in `i_block` (`EXT4_MIN_INLINE_DATA_SIZE`, `fs/ext4/inline.c:19`) plus whatever xattr space the inode has, and is an opt-in `mkfs` feature. **Btrfs** inlines extents up to `BTRFS_DEFAULT_MAX_INLINE`, 2048 bytes (`fs/btrfs/fs.h:309`), into its metadata leaves. **F2FS** keeps up to `MAX_INLINE_DATA` (`fs/f2fs/f2fs.h:546`) in the inode's 4 KiB node block, about 3.4 KiB. **XFS** has no inline file data. All four merge sub-block writes in the page cache and write whole blocks.
- What a fsync'd small append costs on ext4 (the data block in place plus a jbd2 commit of a descriptor, the inode block and a commit block; with `fast_commit`, a smaller commit) and on XFS (the data block plus a log write of the inode) is from general knowledge, not measured or read here; roughly 8 to 16 KiB per fsync.

### The stale-tail bug class

- fsx "checks for mmap last-page zero fill" (`ltp/fsx.c:13`): `check_eofpage` fails on "non-zero data past EOF" in a mapped page (`:879-899`), and with `-e`, `pollute_eofpage` writes garbage past end of file through a shared mapping before every size change, "If the upcoming operation does not correctly zero, incorrect file data will be detected" (`:1029-1094`). xfstests `generic/029` and `generic/030` test mapped writes against truncate down and up.
- FreeBSD's fusefs had exactly this bug: "During truncate, fusefs was discarding entire cached blocks, but it wasn't zeroing out the unused portion of a final partial block. This resulted in reads returning stale data" (commit `e312493b`, 2019, reported by fsx).
- XFS had its crash-ordering twin: on an extending truncate "the blocks beyond EOF get zeroed in memory, but we never force those changes to disk before we run the transaction that extends the file size", and "because in-memory behaviour is correct, tools like fsx don't pick up any coherency problems" (commit `5885ebda`, 2015). rock-solid's truncate commits the size before it punches the tail, so it has the same window unless the punch is recorded durably with the size (below).
- Linux FUSE protects the daemon from part of this: writeback of a mapped page is clamped to the inode's size (`fs/fuse/file.c:1858-1865`, "Got truncated off completely"), and a size change through `setattr` truncates and invalidates the page cache (`fs/fuse/dir.c:2085-2091`). Whether an extending `write()` leaves a polluted end-of-file page in a cached handle's page cache was not checked.

### Block devices

- ublk takes a logical block shift from 9 to `PAGE_SHIFT` (`drivers/block/ublk_drv.c:594`; `include/uapi/linux/ublk_cmd.h:534-537`), so 512 bytes or 4 KiB on x86.
- krbd sets `io_min` and `io_opt` but no logical block size (`drivers/block/rbd.c:4950-4960`), so it exposes the default 512 bytes, and BlueStore merges 512-byte writes on every replica.
- XFS refuses a device whose logical block size exceeds the filesystem's sector size (`fs/xfs/xfs_buf.c:1728-1733`, `bdev_validate_blocksize`), so a block volume's logical block size cannot change once a filesystem is on it.
- KubeVirt presents no block size unless the VM asks for one, a custom logical and physical size or `matchVolume` (`staging/src/kubevirt.io/api/core/v1/schema.go:811-822`; `pkg/virt-launcher/virtwrap/converter/storage/disk.go:89-118`), and QEMU, whose guests default to 512-byte sectors, pads a request smaller than the host's alignment with its own read-modify-write (`block/io.c:1521`, `bdrv_padding_rmw_read`). So a 4 KiB-logical device works for KubeVirt guests as is.
- SQLite's write-ahead log writes frames of a 24-byte header plus a page (`src/wal.c:477-480`), so every commit of 4 KiB pages has unaligned head and tail blocks.

## Findings

### 1. Sub-block write mechanics

The [merge model](../../spikes/sub-block-writes/README.md) applied each strategy to random byte-range writes (1 to 300 bytes, unaligned, some crossing blocks) and compared the stored blocks with every acknowledged write's bytes applied in version order:

| Strategy | Result |
| --- | --- |
| Primary merges against its newest ordered version, committed or pending | Correct with two clients (M1), 8 pipelined writes each, 3,670 failovers (M6), and writes up to 3 blocks long (M9) |
| Primary merges against the committed version | Loses bytes with one client pipelining 4 writes (M2) and with two clients one write each (M3): every seed |
| Client reads committed blocks, merges, sends whole blocks | Loses bytes with two clients (M4) |
| Client RMW with compare-and-set on the version it read (VitastorFS) | Correct, but 0.64 retries per write with two clients and 1.28 with four, half the throughput (M5). The version a client reads is the committed one, so its CAS fails whenever any write to the chunk is pending; the version is per chunk, so writes to other blocks conflict too |
| Overlay, but peering drops any subset of pending versions | Every seed wrong (M7): a kept version carries bytes of a dropped one it was merged on |
| Overlay, but a new primary merges before it holds a version peering kept | 458 of 500 seeds wrong (M8) |

**Where.** At the chunk primary, which already gives each chunk one order. Merging on the client needs compare-and-set and retries, and a lost update without it. Merging on every replica (Ceph's way) needs each member's base to be the same bytes: a member that recovered a block, or whose block failed its checksum, must fetch the base before it can apply a delta, on the write path, and the invariant "no two members hold different data under one version" then rests on every member's history instead of on the bytes sent.

**Against what.** The newest version the primary has ordered for each block: a pending version's merged block if one covers it, else the committed live map, else zeros for a block never written or punched. The primary already holds a pending write's blocks in pool buffers until every member acknowledges, so this costs no memory beyond keeping the merged buffer rather than the client's bytes.

**What goes to replicas.** The merged whole blocks. Against the delta:

| | Merged blocks (proposed) | Byte-range delta |
| --- | --- | --- |
| Disk reads per partial block | 1, at the primary (0 when the overlay or a cache holds it) | 1 per member |
| Network per replica, 100-byte write in one block | 4 KiB: 3.3 µs at 10 GbE, 33 µs at 1 GbE | About 150 bytes |
| Replica CPU | Log the block; CRC32C, 167 ns | Read, verify, merge, CRC32C |
| Replica disk write | One 4 KiB block | One 4 KiB block (records are block-granular either way) |
| Divergence | Impossible: every member logs the same bytes | Silent if any member's base differs |

A write's latency is dominated by the flush, 0.95 ms on the consumer drive and about 0.1 ms with power-loss protection ([NVMe flush spike](../../docs/design.md#appendix-nvme-flush-spike)), so the extra 4 KiB on the wire costs a few percent at most on 10 GbE. The base read is the real cost: one random 4 KiB read when nothing caches the block, not measured here.

**Ordering the read.** A partial write whose base is on disk must read it before it is sent and logged. Later writes of the same chunk wait behind it, so a chunk's writes keep their arrival order, as Ceph's extent cache never reorders and keeps one read outstanding per PG. Writes to other chunks are not held.

**The cases asked about:**

- *Two nodes writing different bytes of one block in WriteShared mode.* Both writes go to the chunk's primary; the second merges on the first, pending or committed (M1, M3 shows what the committed-only base loses). Appends from several nodes, which take their offsets from a MetaStore compare-and-set on the size, land in one tail block the same way.
- *Pipelined writes to one block.* The overlay (M2 is the failure without it). The daemon pipelines its flushes and the block layer issues many writes at once, so this is the common case, not a corner.
- *Primary change mid-flight.* Every member holds whole merged blocks, so a kept version needs nothing from the old primary's memory. Two rules carry the dependency between merged versions: peering drops only a suffix of each chunk's pending versions (M7), which holds when each member logs a chunk's writes in version order, since a disk's durability is a prefix of its log and a member that lacks version v therefore lacks every later one; and a new primary merges into a chunk only once it holds every version peering kept for it (M8), the rule Ceph applies to missing and degraded objects.
- *Snapshots and freezes.* The freeze entry goes into the merged write's record and copies the live map first, so the frozen version keeps the old block and the merged block is new space: the first sub-block write after a snapshot costs one 4 KiB block plus the base read.
- *Erasure-coded chunks.* A write to an encoded chunk already reads `data` shards and writes copies before applying ([When chunks are encoded](../../docs/design.md#when-chunks-are-encoded)); the decoded chunk is the merge base, so a partial write adds no read. Shards are byte ranges of the chunk rounded to blocks, so their CRCs are unchanged.
- *Crash and torn writes.* Nothing is overwritten in place: a merged block is new log space in a record whose CRCs and tail rule drop it whole if torn, and the previous version stays intact. In-place read-modify-write (BlueStore's deferred apply, a local filesystem's page writeback) needs sector atomicity or a journal for the bytes the write did not cover; the log needs neither.
- *O_DIRECT and checksums.* FUSE passes direct I/O through unaligned, so the daemon accepts any range. The merge writes into a 4 KiB-aligned pool buffer, so disk I/O stays aligned for `O_DIRECT`, io_uring fixed buffers and SPDK; whole middle blocks need no copy if the wire format puts block data at block-aligned offsets of the receive buffer, and each partial edge costs one copy of at most 4 KiB. The CRC is computed over the merged block. The base read verifies the base's CRC, and a base that fails it is fetched from another member before merging, so a merge never carries rot into a new version.

**The daemon sends whole blocks when it can.** In Exclusive mode no other node writes the file, and bytes past its end are zero on disk (below). So a flush can extend a partial block to a whole one whenever the daemon holds the block's other bytes or they lie at or past the end of file: a new small file is one whole block, padded with zeros. The daemon keeps each written file's last partial block after flushing it, so the next append flush sends that block whole too, and drops it on a recall. The primary then reads nothing for appends and small files; write-shared files, a buffer flushed under pressure into the middle of a file the daemon did not write, and 512-byte block writes pay the base read.

### 2. Punch, truncate, extend

- **Punch of part of a block** is a merged write of zeros through the same path: whole blocks inside the range become a punch entry, the edges become merged writes, all under one version in one record. A partial edge on a block that was never written needs nothing.
- **Truncate to an unaligned size, then extend.** POSIX extension reads as zeros, so the invariant to keep is: *once the purge for a file's size has run, every byte past that size is zero in the store.* The design's order commits the new size and a purge entry, then punches the last chunk's tail. A crash between the two leaves the old bytes in the tail of the size's block, and a later extension would expose them, exactly XFS's `5885ebda` bug. The fix is that the purge entry starts at the byte offset of the new size, not at a chunk boundary, so the purger punches `[size, end of its chunk)` again after a crash; the design already makes every extension finish a pending purge first. Then an extension writes nothing to the gap: unwritten blocks read as zeros and the tail of the old last block is zero.
- **Reads past end of file inside the last block** are clamped by the daemon to the size it knows, and the store would return zeros there anyway. The kernel clamps mapped writeback to the inode's size and drops cached pages on a `setattr` size change (above). Not checked: whether an extending `write()` on a cached handle keeps a page whose bytes past the old end of file were dirtied through a mapping (fsx `-e`); the daemon sees every extending write, so it can invalidate that one page with `notify_inval_inode` if a test shows it is needed.
- `fallocate` `PUNCH_HOLE` and `ZERO_RANGE` map to `Punch`.

### 3. Small files of 1 to 2 KiB

From the [space model](../../spikes/sub-block-writes/README.md) (S1 to S3), using the design's figures: a 4 KiB block per copy, a 33-byte write entry and a 4-byte CRC in a header shared with the round's other items, 133 B per file replica in the MetaStore's runs, and 108 B raw per file replica in the chunk index (a 44-byte chunk entry and a 64-byte extent):

| File | rep3 raw | ec4-2 raw | ec16-2 raw | Per replica, indexes |
| --- | --- | --- | --- | --- |
| 1 KiB | 12,399 B (12.1×) | 12,399 B as copies (12.1×, 8.1× the pool's 1.5×) | same, 10.8× the pool's 1.125× | 133 B MetaStore + 108 B chunk index |
| 1.5 KiB | 12,399 B (8.1×) | 12,399 B as copies (5.4× the pool's) | same, 7.2× the pool's | same |
| 2 KiB | 12,399 B (6.1×) | 12,399 B as copies (4.0× the pool's) | same | same |
| 16 KiB | 49,299 B (3.0×) | 24,798 B as shards (1.5×) | 49,299 B as copies (2.7× the pool's) | same |

A file flushed alone at queue depth 1 writes a header block per copy too, 24,576 B for any file up to 4 KiB, though only the data block stays live. Small files stay copies in erasure-coded pools: ec4-2 encodes from 12 KiB, ec16-2 from 28 KiB. The space a small file holds per replica, 4 KiB plus about 240 B of index, is still far below what its metadata round trips cost in time ([What is not free](../../docs/design.md#what-is-not-free)).

| Option | Space for a 1.5 KiB file, rep3 | What it costs the rest of the design |
| --- | --- | --- |
| **4 KiB floor** (XFS, BlueStore since Pacific, Vitastor) | 12,399 B | Nothing: today's design |
| Inline data in the inode (CephFS, ext4, Btrfs, F2FS, Lustre DoM) | About 4.7 KiB raw, and **1,669 B per file replica of MetaStore memory, 12.5×: a 16 GiB budget holds 3.4 million files at three replicas instead of 43 million** (S3) | The MetaStore's runs live in memory ([LSM tree](../../docs/design.md#on-disk-format)), so inline bytes are RAM. A value is at most 1,000 bytes, so a 1.5 KiB file needs two keys. Every data write becomes a Raft commit, written to the Raft log, then flushed into a run and rewritten by compaction (2.3 to 6.3× the flushed bytes). A second data path: write-shared writes of an inline file go to the owner, and inlined-to-chunk conversion must be ordered against chunk writes from other nodes, the race CephFS gave up on. A snapshot copies `fs/{volume}/` out of a view, so it would copy every inline file's data into MetaStore memory instead of sharing blocks |
| Tiny inline only, 60 to 112 B (ext4 `i_block`, ZFS embedded pointers, WAFL) | 1.5 KiB files unaffected; a 100-byte file: about 300 B instead of 12,399 | The same second data path for files nobody's space budget notices. Not worth it |
| Short blocks: store only a file's bytes up to the end of file in its last block, packed like blobs | 4,725 B (3.1×) | Write entries and extents gain a 2-byte stored length; the cleaner and the index's per-record counts count bytes, not blocks; a read of a short block reads the aligned blocks holding it. The live map keeps one entry per block, and freezes, merges and recovery are unchanged. Also helps appends a little (A2: 79× → 58×) |
| Smaller blocks (512 B or 1 KiB) | 1 KiB blocks: about 4.7 KiB | 4× to 8× the CRC bytes and index entries for random small writes, which already push a block volume toward 270 million entries per TB; 4Kn drives and SPDK cannot write below 4 KiB, so a lone record still costs 4 KiB per block written |
| Packing several small files into one chunk (VitastorFS, SeaweedFS volumes, Haystack) | VitastorFS still rounds to 4 KiB | Chunk IDs are computed from the inode; a shared chunk needs a lookup, garbage when files die or grow, and a defragmenter |

### 4. Small appends with fsync after each

From the [append model](../../spikes/sub-block-writes/README.md) (A1, A2), 100-byte appends, each followed by fsync:

| Variant | Alone on the disk, queue depth 1 | 8 appenders + 8 overwrites per round, u = 0.8 | Same, u = 0.9 |
| --- | --- | --- | --- |
| Block RMW (proposed) | 8,288 B per append per copy (83×) | 7,924 B (79×) | 9,599 B (96×) |
| Short block | 8,288 B (83×) | 5,770 B (58×) | 6,408 B (64×) |
| Patch entry, bytes in the data area | 8,288 B (83×) | 2,530 B (25×) | 5,216 B (52×) |
| Patch entry, folded after 8 patches | 8,288 B (83×) | 3,484 B (35×) | 5,965 B (60×) |
| Patch entry, bytes in the header blocks | **4,196 B (42×)** | 581 B (5.8×) | 2,098 B (21×) |

- **At queue depth 1 the floor is a block per flush.** A record is whole blocks, so a lone append is a header block plus a data block whatever the data block holds; only putting the bytes in the header halves it, which is what ZFS's intent log does with a 4 KiB minimum log block and 192 bytes of record header. Block RMW at 8 KiB per fsync per copy is about what ext4 and XFS write per fsync locally (inferred, not measured).
- **Patches pay only when several share a round.** With one append per round, a patch in the data area takes a block of its own and gains nothing (A2: identical to RMW).
- **A short-lived block costs about the log's write amplification.** One append per round among 15 overwrites added 4.9 blocks per append at u = 0.8 and 8.4 at 0.9, not one: its space is reclaimed by cleaning segments that also hold live data. With 8 appenders their blocks dominate their segments, and the cost falls to about 2 blocks.
- **What patches would cost.** A block's state becomes a base plus an ordered patch list, so the live map no longer holds one entry per block, a read merges up to the fold limit's worth of patches, a freeze copies patch lists, the cleaner must copy header-resident bytes (today header blocks are dead to it, and the record counts it keeps are in blocks), and something must fold: the primary on a later write, or the cleaner, deterministically on every member. That is the JuiceFS design inside the chunk index, with its compaction.

For 100 fsyncs a second, block RMW writes about 0.8 MB/s per copy, 2.5 MB/s across rep3, or 70 GB a day per copy if sustained: real wear for a chatty logger, comparable to a local filesystem's, and halved at best by header patches. Not enough to take on patch chains before measuring how often homelab workloads fsync small appends.

### 5. Block volumes

- **4 KiB logical blocks by default.** With `logical_bs_shift` 12 the block layer never sends a sub-block or unaligned request, so the block path never merges. Vitastor makes the same choice (`bitmap_granularity` is the "sector size"). Linux filesystems format at 4 KiB, and KubeVirt guests with their default 512-byte sectors are padded by QEMU's own read-modify-write on the node.
- **512e as a per-volume option**, for guests or tools that insist on 512-byte sectors (some older operating systems, databases that write 512-byte log blocks, `matchVolume` VMs that must see 512). It is served by the same primary merge, with no separate mechanism, and is safe for RWX block: two nodes writing different 512-byte sectors of one 4 KiB block merge at the primary (M1). Each such write pays the base read and writes a 4 KiB block.
- **Fixed at creation**, and inherited by clones and restores: XFS formatted with 512-byte sectors refuses a 4 KiB-logical device. Physical block 4 KiB in both cases, `io_min` 4 KiB, and `io_opt` the chunk size, 4 MiB, as krbd reports its object set size.

## Proposed design

### Mechanism

1. **Partial blocks merge at the chunk primary.** A write or punch names a byte range. The primary orders each chunk's operations in arrival order. For each block the operation covers only in part, it takes the newest version it has ordered for that block, from a pending write's merged block if one covers it, else from its live map (zeros if unwritten or punched), verifying the base's CRC32C and fetching it from another member if it fails, copies the base into a 4 KiB pool buffer and overlays the bytes. A base on disk is read before the operation is sent or logged, and the chunk's later operations wait behind it. The primary sends and logs only whole blocks; the record format, the CRCs, the chunk index and snapshots stay block-granular. Each loop keeps a small cache of recently merged blocks, as Ceph's extent cache does.
2. **The daemon widens flushes to whole blocks** when it holds the block's other bytes or they lie at or past the end of file, which in Exclusive mode it knows. It keeps each written file's last partial block after a flush, and drops it on a recall.
3. **Peering drops only a suffix** of a chunk's pending versions: every member logs a chunk's writes in version order, and resends after a reconnect keep that order.
4. **A new primary merges into a chunk only once it holds every version peering kept for it**; until recovery brings that chunk, its partial writes wait.
5. **Bytes past end of file are zero.** Truncate's purge entry starts at the new size's byte offset, and the purger punches the partial block first; every extension still finishes a pending purge before it writes. Reads are clamped to the size.
6. **Small files keep the 4 KiB floor**, and stay copies in erasure-coded pools. No inline data.
7. **Fsync'd small appends are block RMW**, with the daemon's kept tail block sparing the primary's read.
8. **Block volumes expose 4 KiB logical and physical blocks** (`logical_bs_shift` and `physical_bs_shift` 12, `io_min` 4 KiB, `io_opt` 4 MiB), with `block_size: 512` as a StorageClass parameter for 512e, fixed at creation.

### Alternatives rejected

| Alternative | Who uses it | Why not |
| --- | --- | --- |
| Merge on the client, compare-and-set on the version it read | VitastorFS | Without CAS, lost updates (M4); with it, retries on every concurrent write to the chunk, since the version is per chunk and reads see only committed versions (M5: 0.64 to 1.28 retries per write on a small chunk), and an extra round trip for the read |
| Send the byte range, merge on every member | Ceph BlueStore | A base read on every member instead of one, and members' bytes agree only if their histories do; a member with a failed or recovering base block must fetch it on the write path |
| Merge against the committed version only | — | Loses bytes with any two writes to one block in flight (M2, M3) |
| Byte-granular patch entries merged on read, folded later | JuiceFS slices, SeaweedFS chunks, ZFS intent log | No gain at queue depth 1 unless the bytes ride in the header (83× → 42×), where they make header blocks live; 3× to 14× less log under concurrent appenders. Costs one-entry-per-block, read merging, folding, patch lists in freezes and cleaner work. Deferred (open question) |
| Inline data in the inode | CephFS (deprecated), ext4, Btrfs, F2FS, Lustre DoM, WAFL | The MetaStore's runs are RAM: 12.5× per file at 1.5 KiB, 43 → 3.4 million files per 16 GiB; Raft and compaction write amplification on data; a second data path whose conversion raced in CephFS; snapshots would copy data |
| Short blocks for small files and file tails | ReiserFS tails, needle packing | 2.6× less space for a 1.5 KiB file and some gain for appends, for a 2-byte field in entries and extents and byte-counted liveness. Deferred (open question); old write entries stay valid, so it needs no migration |
| Smaller blocks | ext4 and XFS allow 1 KiB | 4 to 8× CRCs and index entries for random small writes; 4Kn drives and SPDK still write 4 KiB |
| 512-byte logical blocks by default | krbd, QEMU guests | Every 512-byte write pays a base read and a 4 KiB write; 4 KiB is what Linux filesystems write, and QEMU pads guests itself |

### Why it is safe

Write *apply(S, w)* for state S with write w's bytes overlaid. The claim is that the committed image of every chunk equals the acknowledged writes applied in version order, at every member.

1. **One order, one base.** The primary assigns versions v₁ < v₂ < … to a chunk's writes in the order it merges them, and merges w_k against the image it has ordered after w_{k−1}: the overlay holds every pending merged block, and the live map every committed one. So version v_k's blocks equal apply(image(v_{k−1}), w_k) restricted to the blocks w_k touches, and the image at v_k is apply(image(v_{k−1}), w_k). Concurrent writers and pipelined writes reach this primary in some order; that order is the version order, and no write's bytes are merged on an image that lacks an earlier version (M1, M6; M2 and M3 are the counterexamples without it).
2. **Members agree by construction.** Every member logs the same whole blocks for v_k, so it needs no base and cannot diverge; a member's maps differ only in which versions are durable, and each member logs a chunk's versions in order, so its durable set is a prefix (durability is a prefix of its log).
3. **Peering keeps a prefix.** Peering keeps a version unless some member live in the old epoch lacks it. By 2, a member lacking v_k lacks every later version, so the dropped versions are a suffix, and every kept version's merge base is itself kept (M7 is the counterexample when a subset could be dropped). Dropped writes were never acknowledged, so their bytes may vanish; recovery supersedes them in every log that holds them, as today.
4. **A new primary starts from the kept image.** Rule 4 makes its first merge in a chunk read the image of the newest kept version (M8 is the counterexample). From there, 1 applies again in its own epoch.
5. **Tails stay zero.** Every operation that lowers a file's size records a purge from the new size's byte offset in the same transaction, the purge's first punch zeroes the partial block, and every extension waits for pending purges. So no extension exposes a byte written past a size that a committed truncate set, after a crash too.

### Costs

- **Base read**: one random 4 KiB read at the primary for a partial block that neither the overlay, the cache nor the daemon supplies, which in practice means write-shared random writes and 512e block writes. It delays later writes to the same chunk only.
- **Network**: at most two whole edge blocks per write per replica instead of the bytes, 3.3 µs each at 10 GbE.
- **CPU**: one copy of at most 4 KiB per partial edge, and the CRC32C the log computes anyway (167 ns per block).
- **Memory**: a pending write's merged blocks are the buffers the primary holds for replication anyway; the merge cache is bounded per loop.
- **Space and writes**: a small file costs a block per copy, 12,399 B raw for 1 to 4 KiB in rep3 and in ec4-2 alike; a fsync'd 100-byte append costs 8.3 KiB per copy alone on the disk, about 2 to 2.3 blocks with 8 concurrent appenders.

### What the phase 2 models and the simulator should check

- **Epoch change model (TLA+):** writes carry a dependency on the chunk's previous version; property: no kept version follows a dropped one of its chunk, and no acknowledged read returns bytes of a dropped write. Model a member that logs one chunk's versions out of order to show the rule is needed, and a new primary that merges before recovery (M7, M8).
- **Simulator, a byte oracle:** besides the block-level invariants, an oracle of every acknowledged write's byte range in version order; every read must match it, live and at every snapshot. The workload mixes unaligned writes of 1 byte to 3 blocks, two clients on one block, pipelined depths up to 32, punches of partial blocks, and failovers during merges with base reads in flight.
- **New invariants:** a partial-block write's base is the newest version the primary has ordered; members hold equal bytes under each version (already listed); no byte past a file's size reads non-zero, including after a crash between a truncate's transaction and its punch, and after an extension that races a purge.
- **Faults:** a base read that fails its CRC; a primary crash with a base read in flight; a recall while the daemon holds a kept tail block; a 512e RWX block volume written by two nodes at 512-byte offsets in one block.
- **Conformance:** fsx with `-e` (end-of-file pollution) and mapped operations on the FUSE frontend, single and multi-client; xfstests `generic/029` and `generic/030`; `fio --verify` with 512-byte random writes on a 512e volume.

### Replacement text for `docs/design.md`

**Data layer: ChunkStore.** After "**Single order per chunk.**", add:

```markdown
**Byte ranges become whole blocks at the primary.** The API takes byte ranges; records, checksums and the chunk index hold whole 4 KiB blocks. For each block a write or punch covers only in part, the primary takes the newest version it has ordered for that block, from a pending write's merged block if one covers it, else from its live map (zeros if the block was never written or was punched), and overlays the bytes in a 4 KiB pool buffer. A base read from disk has its CRC32C checked, and one that fails is fetched from another member first, so a merge never carries rot into a new version; the chunk's later operations wait behind that read, so a chunk's writes keep their arrival order, as Ceph's erasure-coded extent cache never reorders and keeps one read outstanding per PG. The primary sends and logs the merged whole blocks, so members need no base and cannot diverge. Merging against the committed version alone would lose bytes as soon as two writes to one block are in flight, from one client pipelining or from two nodes; merging on the client needs a compare-and-set that fails on every concurrent write to the chunk; and merging on every member, as BlueStore does, costs a read per member and makes members agree only if their histories do ([sub-block writes study](../studies/sub-block-writes/README.md)). Two rules carry the dependency a merged version has on the one below it through a primary change: peering drops only a suffix of a chunk's pending versions, which holds because every member logs a chunk's writes in version order, and a new primary merges into a chunk only once it holds every version peering kept for it ([Epoch changes](#epoch-changes-and-failure-detection)). A partial block costs one random 4 KiB read at the primary when no pending write, the primary's small cache of recently merged blocks or the client supplies it, plus up to two whole edge blocks on the wire per replica.
```

Replace "**Punch.**" with:

```markdown
**Punch.** `punch` zeroes a byte range as a new version: whole blocks inside the range get a punch entry, and a block it covers in part is merged with zeros like a write, in the same record. Punched blocks are freed once no snapshot needs them, and later reads of the range return zeros. Punching a whole chunk deletes it and leaves a tombstone. File unlink and truncate, `fallocate` hole punches, block discard and volume deletion all free space this way (see [Freeing space](#freeing-space)).
```

**Epoch changes, "Writes" paragraph.** After "…as Ceph recovers divergent objects.", add:

```markdown
Each member logs a chunk's writes in the order of their versions, and a primary resends them in that order after a reconnect, so a member that lacks a version lacks every later one of its chunk, and peering drops a suffix of each chunk's pending versions: a version it keeps was never merged on one it drops. Until a chunk's kept versions are on the new primary, a write that covers part of one of its blocks waits, as Ceph's primary holds writes to a missing or degraded object.
```

**Snapshots and clones, "Data: copy-on-write per block".** "…so the first 4K write after a snapshot writes 4K, not a 4 MiB chunk copy" becomes "…so the first write after a snapshot writes only the 4K blocks it touches, merged whole, not a 4 MiB chunk copy".

**Freeing space, "Truncate" bullet:**

```markdown
- **Truncate.** Shrinking a file commits, in one transaction, the new size, `alloc_end` lowered to the new end, and a purge entry starting at the new size's byte offset and running to the old `alloc_end`. The purger's first punch zeroes the rest of the block holding the new end and the rest of its chunk; the daemon runs that punch itself before `truncate` returns, and the purger runs it again after a crash, so bytes past the size are zero on disk whatever crashes, the bug XFS fixed when its extending truncates exposed blocks zeroed only in memory. Reads never return data past the size, and anything that extends the file over a range with a pending purge, a write raising `alloc_end` or a truncate upward, first finishes that purge. So a file extended later reads zeros where it was cut, without writing them, and the purger never deletes newer data.
```

**Client design, rule 2.** Replace "It flushes a chunk as soon as it is full, and the rest on `fsync`, `close` or a recall." with:

```markdown
It flushes a chunk as soon as it is full, and the rest on `fsync`, `close` or a recall. A flush covers whole blocks wherever the daemon holds a block's other bytes or they lie past the end of file, which no other node can change while the file is not shared: a new small file goes out as one block padded with zeros, and the daemon keeps a file's last partial block after flushing it, so the next append's flush is a whole block too. The chunk primary merges every other partial block ([Data layer](#data-layer-chunkstore)), which is what write-shared files pay.
```

**Pools and redundancy, "When chunks are encoded".** After "…so a small file's only chunk in an `ec16-2` pool stays as copies.", add: "In `ec4-2` that is every file up to 8 KiB, so a 1.5 KiB file costs 8.1 times its size, 5.4 times what the pool promises."

**Block frontend.** After the RWO paragraph, add:

```markdown
**Logical block size.** A block volume exposes 4 KiB logical and physical blocks (`logical_bs_shift` 12), `io_min` 4 KiB and `io_opt` the chunk size, so the kernel never sends a write smaller than a block and the block path never merges. Vitastor exposes its 4 KiB `bitmap_granularity` the same way. KubeVirt guests, which see 512-byte sectors unless their disk asks otherwise, are padded by QEMU's own read-modify-write. The StorageClass parameter `block_size: 512` exposes 512-byte logical blocks for guests and tools that need them, merged at the primary like a file's partial blocks, which keeps RWX block correct when two nodes write sectors of one block; each such write pays a base read and a 4 KiB block. The size is fixed when the volume is created and inherited by its clones and restores, since XFS refuses a device whose logical block is larger than the sector size it was formatted with.
```

**Alternatives considered,** new rows:

```markdown
| Sub-block writes | Merged at the chunk primary against the newest version it has ordered, pending ones included; merged whole blocks sent to replicas; the daemon sends whole blocks when it holds the bytes ([Data layer](#data-layer-chunkstore)) | Client read-modify-write with compare-and-set (VitastorFS); a byte-range delta merged on every member (Ceph BlueStore); merging against the committed version; byte-granular patch entries merged on read (JuiceFS slices, the ZFS intent log) | In a model, merging on committed versions or on the client without CAS lost bytes on every seed; CAS retried 0.64 to 1.28 times per write; deltas cost a read per member and agree only if members' histories do. Patches halve a lone fsync'd append (83× → 42×) only if their bytes ride in header blocks, and cost one-entry-per-block, read merging and folding ([sub-block writes study](../studies/sub-block-writes/README.md)) |
| Small files | A 4 KiB block per copy, as XFS, BlueStore since Pacific and Vitastor | Inline data in the inode (CephFS, deprecated since Octopus; ext4, Btrfs, F2FS, Lustre DoM); short blocks; smaller blocks; packing files into shared chunks (VitastorFS, SeaweedFS) | The MetaStore's runs live in memory: 1.5 KiB inline costs 12.5× per file, 43 → 3.4 million files per 16 GiB. Short blocks would save 2.6× on a 1.5 KiB file and are an open question; smaller blocks multiply CRCs and index entries |
| Block volume logical block size | 4 KiB, 512e per volume through the primary merge | 512 bytes by default (krbd) | Linux filesystems write 4 KiB, QEMU pads its guests, and a 512-byte write would cost a base read and a 4 KiB block |
```

**Testing strategy.** Add to the invariants: "every read returns the bytes of the acknowledged writes applied in version order, checked against a byte-range oracle; no byte past a file's size reads non-zero; no version peering keeps follows one of its chunk that it drops". Add to the simulation faults for placement: "unaligned writes from two nodes into one block, pipelined, across a failover with a base read in flight; a base block that fails its checksum". Layer 6: "`fsx` with end-of-file pollution (`-e`) and mapped operations; xfstests `generic/029` and `generic/030`".

**Open questions,** new items:

```markdown
- [ ] Short blocks: storing only a file's bytes up to its end in its last block, packed in the record, would cut a 1.5 KiB file from 12,399 to 4,725 raw bytes in rep3 and an fsync'd 100-byte append among 8 concurrent appenders from 79× to 58×, for a 2-byte stored length in write entries and extents and liveness counted in bytes ([sub-block writes study](../studies/sub-block-writes/README.md)). How much of a homelab volume's space is small files and partial tail blocks? Sizes from `find -printf '%s'` over real volumes (media, source trees, `node_modules`, app data) decide it.
- [ ] Patch entries for fsync'd small appends: bytes kept in the record header would halve a lone append (83× → 42× for 100 bytes) and cut 8 concurrent appenders from 79× to 5.8×, at the price of live header blocks, a patch list per block on reads and freezes, and folding. How often do homelab workloads fsync appends smaller than a block (log files, MySQL's 512-byte redo blocks, SQLite's write-ahead log with its 24-byte frame headers)?
- [ ] An extending `write()` on a cached handle after the page past the old end of file was dirtied through a mapping: does the kernel keep showing those bytes (fsx `-e`)? If it does, the daemon invalidates that page when it sees the extension.
- [ ] The primary's base read: its latency on NVMe and SATA at queue depth 1, and how large a per-loop cache of merged blocks keeps write-shared appenders from reading.
```

**Roadmap.** Phase 1's single-node ChunkStore and phase 3's replica primary each gain "partial blocks merged against the newest ordered version"; phase 2's epoch change model gains "a chunk's pending versions dropped as a suffix"; phase 8 gains "4 KiB logical blocks, 512e per volume". The [build checklist](../../docs/build-checklist.md) items of the same names and [test scenarios](../../docs/test-scenarios.md) follow when those items are written.

### Replacement text for `docs/glossary.md`

Replace "**Read-modify-write and the write hole.**" with:

```markdown
**Read-modify-write, the write hole.** Storage writes whole blocks, so a write of a few bytes must read the rest of its block, overlay the bytes and write the block back: a read-modify-write. rock-solid's chunk primary does this for every block a write covers in part, against the newest version it has ordered, and sends the whole block to the replicas; see [Data layer](design.md#data-layer-chunkstore). An erasure-coded stripe needs the same for its parity, and if a crash lands between the writes to different shards, the parity no longer matches the data and a later rebuild produces garbage: the write hole. rock-solid avoids that one by never overwriting encoded data; a chunk is written as copies and encoded only once whole or idle. See [When chunks are encoded](design.md#when-chunks-are-encoded).
```

Rewrite "**Block, sector, logical block address (LBA).**":

```markdown
**Block, sector, logical block address (LBA), 512e.** A drive reads and writes fixed-size blocks, never single bytes; rock-solid works in 4 KiB blocks. An LBA is a block's number on the drive. A device's *logical* block size is the smallest write it accepts and its *physical* one what it writes internally; a *512e* drive accepts 512-byte writes on 4 KiB media by reading and rewriting the rest of the block itself. rock-solid's block volumes expose 4 KiB blocks, or 512e per volume; see [Block frontend](design.md#block-frontend).
```

## Unverified

- **CephFS's reasons for deprecating inline data** and the July 2026 kernel removal patches come from a search engine's summary of the ceph-users list and LKML; neither host resolved from here. The kernel client's uninline-at-open code and the deprecation message were read in source.
- **WAFL's 64-byte inline files** come from NetApp mailing-list posts, not from the 1994 paper or NetApp documentation.
- **ext4 and XFS bytes per fsync** are general knowledge, not measured; a loop device would need root.
- **The base read's latency** was not measured; the models have no I/O.
- **The append model** is a scaled 1 GiB disk with a greedy cleaner, three seeds; the marginal costs with one appender per round are differences of large totals and vary by several percent between seeds.
- **Whether an extending `write()` leaves a polluted end-of-file page** in a cached FUSE handle (fsx `-e`) was not tested.
- **SeaweedFS's resolution of concurrent writers by client clock** is read from `filechunks.go`, not tested.
- **Branch heads, not releases.** Ceph, OpenZFS, JuiceFS, SeaweedFS, Lustre, Vitastor, xfstests, QEMU, KubeVirt and SQLite were read at their default branches on 2026-10-19; Linux at v6.18. The commits are pinned in `fetch.sh`.

## Reproduce

`./fetch.sh [dir]` downloads every cited file at the pinned commits into `dir` (default `./work`), prints the cited lines and the two commit messages. Needs `curl` and `git`. The measurements: `spikes/sub-block-writes/run.sh`.

## Sources

- Ceph: [`src/mon/FSCommands.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/mon/FSCommands.cc), [`doc/cephfs/experimental-features.rst`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/doc/cephfs/experimental-features.rst), [`src/common/options/global.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/global.yaml.in), [`doc/rados/configuration/bluestore-config-ref.rst`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/doc/rados/configuration/bluestore-config-ref.rst), [`src/os/bluestore/BlueStore.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/os/bluestore/BlueStore.cc), [`src/osd/ECExtentCache.h`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/ECExtentCache.h), [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PrimaryLogPG.cc); [ceph-users thread on deprecating inline data](https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/message/SNSKVDAMA73OVBVD5PPITOBZB7TTPVD5/); ["ceph: remove the inline data support"](https://lkml.iu.edu/2607.3/09299.html).
- Linux v6.18: [`fs/ceph/file.c`](https://github.com/torvalds/linux/blob/v6.18/fs/ceph/file.c), [`fs/ceph/addr.c`](https://github.com/torvalds/linux/blob/v6.18/fs/ceph/addr.c), [`fs/fuse/file.c`](https://github.com/torvalds/linux/blob/v6.18/fs/fuse/file.c), [`fs/fuse/dir.c`](https://github.com/torvalds/linux/blob/v6.18/fs/fuse/dir.c), [`drivers/block/ublk_drv.c`](https://github.com/torvalds/linux/blob/v6.18/drivers/block/ublk_drv.c), [`include/uapi/linux/ublk_cmd.h`](https://github.com/torvalds/linux/blob/v6.18/include/uapi/linux/ublk_cmd.h), [`drivers/block/rbd.c`](https://github.com/torvalds/linux/blob/v6.18/drivers/block/rbd.c), [`fs/xfs/xfs_buf.c`](https://github.com/torvalds/linux/blob/v6.18/fs/xfs/xfs_buf.c), [`fs/btrfs/fs.h`](https://github.com/torvalds/linux/blob/v6.18/fs/btrfs/fs.h), [`fs/f2fs/f2fs.h`](https://github.com/torvalds/linux/blob/v6.18/fs/f2fs/f2fs.h), [`fs/ext4/inline.c`](https://github.com/torvalds/linux/blob/v6.18/fs/ext4/inline.c); commit [`5885ebda`](https://github.com/torvalds/linux/commit/5885ebda878b47c4b4602d4b0410cb4b282af024) (XFS, 2015).
- FreeBSD: commit [`e312493b`](https://github.com/freebsd/freebsd-src/commit/e312493b37949666334be3b6959e4dce62407c35) (fusefs, 2019).
- OpenZFS: [`include/sys/zil.h`](https://github.com/openzfs/zfs/blob/1e9b428dcad3716cf11005dd8970bc91c8c0100c/include/sys/zil.h), [`module/zfs/zil.c`](https://github.com/openzfs/zfs/blob/1e9b428dcad3716cf11005dd8970bc91c8c0100c/module/zfs/zil.c), [`include/sys/spa.h`](https://github.com/openzfs/zfs/blob/1e9b428dcad3716cf11005dd8970bc91c8c0100c/include/sys/spa.h).
- JuiceFS: [`pkg/meta/base.go`](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/meta/base.go), [`docs/en/introduction/architecture.md`](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/docs/en/introduction/architecture.md).
- SeaweedFS: [`weed/storage/types/needle_types.go`](https://github.com/seaweedfs/seaweedfs/blob/d3ee817b2e9ff1ee441019a90d2b778754afef4b/weed/storage/types/needle_types.go), [`weed/filer/filechunks.go`](https://github.com/seaweedfs/seaweedfs/blob/d3ee817b2e9ff1ee441019a90d2b778754afef4b/weed/filer/filechunks.go).
- Lustre: [`lustre/lod/lod_dev.c`](https://github.com/lustre/lustre-release/blob/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d/lustre/lod/lod_dev.c).
- Vitastor: [`docs/config/layout-cluster.en.md`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/docs/config/layout-cluster.en.md), [`docs/usage/nfs.en.md`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/docs/usage/nfs.en.md), [`src/nfs/nfs_kv_write.cpp`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/src/nfs/nfs_kv_write.cpp).
- xfstests: [`ltp/fsx.c`](https://github.com/kdave/xfstests/blob/22348afe338c0f6d540c0d7ef0db749eaa51b218/ltp/fsx.c), [`tests/generic/029`](https://github.com/kdave/xfstests/blob/22348afe338c0f6d540c0d7ef0db749eaa51b218/tests/generic/029).
- QEMU: [`block/io.c`](https://github.com/qemu/qemu/blob/a19f59a72d96056d9a8f48cf6c77f5787491e414/block/io.c). KubeVirt: [`schema.go`](https://github.com/kubevirt/kubevirt/blob/e2a5aa2d49019b63b7540ce8ecb9d9eaaad02421/staging/src/kubevirt.io/api/core/v1/schema.go), [`disk.go`](https://github.com/kubevirt/kubevirt/blob/e2a5aa2d49019b63b7540ce8ecb9d9eaaad02421/pkg/virt-launcher/virtwrap/converter/storage/disk.go). SQLite: [`src/wal.c`](https://github.com/sqlite/sqlite/blob/58626f682afbc2e3467e8c2e5b37f7f29d89ed99/src/wal.c).
- WAFL small files: NetApp "toasters" list posts, e.g. [this thread](https://lists.teaparty.net/hyperkitty/list/toasters@lists.teaparty.net/thread/BKFEZA6RKXTQFRS2W6ORFTHKR6D3YM53/).
