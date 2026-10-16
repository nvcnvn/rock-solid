# Rock Solid - a Rust Distributed Storage — Project Design

## Goals

Build a distributed storage system in Rust whose headline feature is **ReadWriteMany (RWX) shared filesystems** for a 3 to 10 node homelab Kubernetes cluster: many pods on many nodes mounting one directory, with no NFS gateway in the path, caching that stays correct when files are shared, and Longhorn-level ease of install. Block volumes come from the same core, at near-raw NVMe latency.

**Target user:** homelab and small-cluster Kubernetes operators. They have several NVMe or SATA solid-state drives per node, 10 to 25 Gigabit Ethernet (GbE), and some nodes with Remote Direct Memory Access (RDMA) capable cards. For RWX today they choose between a Network File System (NFS) server (single server, fragile), Longhorn RWX (an NFS server pod in front of each volume), JuiceFS (needs an external metadata database and object store) and CephFS (heavy on RAM, CPU and operator knowledge).

**Product goals, in priority order**

1. **RWX filesystem** (`volumeMode: Filesystem`, `ReadWriteMany`): a Filesystem in Userspace (FUSE) client on every node, talking straight to the storage nodes, with close-to-open consistency first and automatic per-file cache coherence next.
2. **Block volumes**: ReadWriteOnce (RWO) Persistent Volume Claims (PVCs), plus RWX block for KubeVirt live migration.
3. **S3** later, on the same core.
4. **Easy to run:** a single binary (one daemon per node, which also serves as the FUSE client) plus a Container Storage Interface (CSI) driver. No hugepages, pinned cores or external database to get started.
5. **Low CPU per operation**, so storage does not eat the cluster.
6. **Permissive license** (Apache 2.0).

**Performance targets.** Stated as round trips, because absolute numbers depend on the network and drives.

| Operation | Target cost |
| --- | --- |
| Read of a cached file (one writer, or readers only) | Page-cache speed: no FUSE or network trip (0.3 µs per 4K measured) |
| Read or write of a file being written from two or more nodes | One network round trip to the chunk primary, plus about 5 to 20 µs of FUSE overhead |
| `stat`, `lookup` within the cache timeout | Local |
| `stat`, `lookup` after a cache miss | One round trip to the MetaStore shard leader (lease read, no disk write) |
| `create`, `unlink`, same-directory `rename` | One Raft commit on one shard |
| `open` of a file | One round trip to the inode's owner (memory only, no disk write) |
| Block 4K write at queue depth 1 | Around 0.1 ms (Vitastor class) on drives with power-loss protection; Ceph is around 1 ms |

**Learning goals:** Rust, the Storage Performance Development Kit (SPDK), the Data Plane Development Kit (DPDK) and RDMA, extended Berkeley Packet Filter (eBPF) tracing, plus distributed-systems correctness testing.

**Non-goals for v1:** Server Message Block (SMB) file sharing for macOS and Windows clients, GPU-cluster scale, shared-writable `mmap` across nodes, cross-directory hard links, and a custom kernel module.

## Landscape and positioning

The open gap is **RWX without a gateway**: every client reads and writes the storage nodes directly, caches aggressively while it is safe, and stops caching automatically when it is not. It installs like Longhorn and is licensed like SeaweedFS.

| System | What it covers | Why it doesn't fill the gap |
| --- | --- | --- |
| Longhorn | Easy Kubernetes block; RWX through a share-manager pod running NFS-Ganesha | All RWX traffic goes through one pod per volume: a bottleneck and a failover pause |
| [JuiceFS](https://github.com/juicedata/juicefs) | FUSE file system, CSI driver, Apache 2.0 | Needs an external metadata engine (Redis, TiKV, SQL) and an object store; close-to-open consistency only |
| CephFS | POSIX file system with full client capabilities | Around 1 ms writes, heavy CPU and RAM, steep operations learning curve |
| [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | Single binary, S3, FUSE mount, CSI driver | Built around objects; no cache-coherence protocol for shared writes |
| [Vitastor](https://vitastor.io) | Ceph-like block, file, S3 at around 0.1 ms | Solo project; server under a GPL-based network-copyleft license that deters adopters |
| [OpenEBS Mayastor](https://github.com/openebs/mayastor) | Rust + SPDK + NVMe over Fabrics (NVMe-oF) block | Block only; requires hugepages and dedicated cores |
| [Weka](https://docs.weka.io/weka-system-overview/weka-client-and-mount-modes), DAOS, [DeepSeek 3FS](https://github.com/deepseek-ai/3fs) | AI-cluster file systems with kernel bypass | Built for GPU clusters; commercial or heavy to deploy |
| [MinIO](https://github.com/minio/minio), [RustFS](https://github.com/rustfs/rustfs), Garage | S3 | Object storage only |

**Takeaways**

- Homelab RWX today means NFS somewhere in the path. Removing it is the visible win.
- Weka shows that per-file cache-mode switching gives most of the benefit of full client capabilities with a much smaller protocol. Our spike shows it can be done on FUSE, without a kernel module (see [FUSE coherence spike](#appendix-fuse-coherence-spike)).
- Vitastor proves one person can get a 10x latency win over Ceph without DPDK, by shortening the code path. The same lean core serves block.
- Trust is the adoption gate. Storage people wait years before trusting a new system, so correctness work is the product.

## Strategy

Win on a short code path, correct caching and low CPU per operation. Kernel bypass is an opt-in fast path, never a requirement.

On homelab hardware the network caps throughput before the kernel does:

```
one NVMe Gen4 drive:  ~7 GB/s sequential, 4K write in ~0.04 ms (into its write cache)
10 GbE link:          ~1.2 GB/s
25 GbE link:          ~3 GB/s
kernel TCP stack:     saturates 10-25 GbE with one or two cores
```

So DPDK buys almost no throughput here. The latency gap between the disk and Ceph is almost all software path, which is what we attack. One caveat: a *durable* write is bounded by the drive's flush latency. Consumer NVMe drives without power-loss protection can take a millisecond or more to flush, so the 0.1 ms block target applies to drives with power-loss protection.

**Principles**

1. **RWX first:** the file frontend and its coherence protocol are the first usable product. Block and S3 reuse the same core.
2. **Lean data path:** client → chunk primary in one hop → log append in parallel with sends to replicas → acknowledge. Every frontend uses it.
3. **Cache where safe, never where unsafe:** a file is cached on a node only while no other node writes it. Correctness never depends on timeouts alone.
4. **Defaults that need no tuning:** `io_uring` for disk, kernel TCP for network, FUSE for the file client. No hugepages, pinned cores or kernel modules just to try it.
5. **Opt-in fast paths:** SPDK for dedicated drives, RDMA for nodes with capable cards. DPDK stays a learning experiment.
6. **Correctness before features:** deterministic simulation from day one, including the coherence protocol.
7. **Apache 2.0** to stand apart from Vitastor's network-copyleft license.

## Architecture overview

The system is a two-part core (ChunkStore for data, MetaStore for metadata) under three protocol frontends, over pluggable input/output drivers.

```mermaid
flowchart TB
    subgraph FE[" "]
        direction LR
        FILE["<b>File: RWX (first)</b><br/>FUSE client on every node<br/>MetaStore: namespace, opens, cache modes"]
        BLK["<b>Block volumes</b><br/>ublk + CSI; NVMe-oF/TCP remote<br/>MetaStore: attach only"]
        S3["<b>S3 gateway (later)</b><br/>PUT writes, then seals chunks<br/>MetaStore: commit, LIST"]
    end
    subgraph CORE["Core: sans-IO state machines (events in, actions out)"]
        direction LR
        CS["<b>ChunkStore (data)</b><br/>Versioned chunks, deterministic chunk IDs<br/>Redundancy per pool:<br/>1 to 3 copies or erasure coding"]
        MS["<b>MetaStore (metadata)</b><br/>Ordered transactional key-value, multi-Raft<br/>Control group: pools, placement table + epochs<br/>Inodes, leases, S3 index"]
    end
    subgraph DRV["Drivers: pluggable, chosen by flags"]
        direction LR
        DISK["<b>Disk</b><br/>Default: io_uring<br/>Opt-in: SPDK on dedicated NVMe"]
        NET["<b>Network</b><br/>Default: kernel TCP<br/>Opt-in: RDMA or Soft-RoCE"]
        SIM["<b>Simulator (tests)</b><br/>Simulated disk, network, time<br/>One seed replays a whole run"]
    end
    FILE --> CORE
    BLK --> CORE
    S3 --> CORE
    CORE -- "actions out, events in" --> DRV
```

*Three frontends share one core; they differ only in how much metadata they need.*

The same daemon runs on every node. On storage nodes it serves chunks and MetaStore shards; on every node, including compute-only ones, it is also the FUSE client for RWX mounts. The core never touches real I/O, so the simulator can stand in for every driver.

## Data layer: ChunkStore

The ChunkStore is dumb, fast and versioned. It knows nothing about files, buckets or volumes.

```rust
trait ChunkStore {
    async fn write(&self, c: ChunkId, off: u32, data: &[u8], expect_ver: Option<u64>) -> Result<u64>;
    async fn read(&self, c: ChunkId, off: u32, len: u32, at_ver: Option<u64>) -> Result<Bytes>;
    async fn seal(&self, c: ChunkId) -> Result<()>;      // now immutable -> eligible for erasure coding
    async fn flush(&self, cs: &[ChunkId]) -> Result<()>; // durability barrier
}
```

**Deterministic chunk IDs.** `chunk_id = (volume_id, inode_id, offset / CHUNK_SIZE)` is computed, never looked up; a block volume uses inode 0. The hot path needs no metadata. A chunk ID packs into 128 bits: a 32-bit volume, a 64-bit inode and a 32-bit chunk index, which allows files up to 16 PiB ([ADR 0004](decisions/0004-key-layout-and-ids.md)). `CHUNK_SIZE` is 4 MiB, Ceph RBD's default object size; the nightly benchmarks keep it honest. Inode IDs are scoped to their volume, so a [restored or cloned](#snapshots-and-clones) volume keeps every inode number and differs from its source only in `volume_id`.

**Versions.** Every write returns a new version. `expect_ver` gives compare-and-set on data. A primary keeps an old version only while a [snapshot](#snapshots-and-clones) pins it. A read at a frozen version (`at_ver`) needs no ordering against writes, so any PG member holding that version can serve it, and it never waits for a writer.

**Single order per chunk.** Each chunk has one primary that applies writes in one order. This is what makes RWX block and uncached shared files correct without any client-side protocol.

**Fencing.** Each write carries the client's session ID. When a client session expires (see [Sessions and fencing](#sessions-and-fencing)), the control plane publishes it as fenced and chunk primaries reject its writes. This stops a paused or partitioned client from writing after its locks and cache rights are gone.

**Integrity.** Each 4K block, and each 4K block of an erasure-coded shard, carries a CRC32C checksum in the [on-disk format](#on-disk-format), which catches bit rot, torn writes and misdirected writes.

**Placement groups.** Chunks are not placed one by one. Each chunk hashes into one of a fixed number of placement groups (PGs) in its volume's [pool](#pools-and-redundancy), as in Ceph, and placement is decided per PG:

```
chunk_id = (volume_id, inode_id, offset / CHUNK_SIZE)        // computed
pool     = volume record → pool                              // learned once, at attach or mount
h        = xxh3_64(chunk_id as 16 big-endian bytes, seed 0)  // computed
pg       = (pool, h >> (64 - log2(pool.pg_count)))           // top bits of the hash
members  = placement_table[pg]                               // in-memory; [primary, r1, r2] in rep3
```

The hash spreads a volume's consecutive chunks over many PGs, and so over every disk of its pool. A volume is therefore not bounded by any one drive; see [Volume size and capacity](#volume-size-and-capacity). The PG is also the unit of recovery and rebalancing: adding a disk moves whole PGs onto it, never individual chunks. The hash function and bit layout are part of the disk format and never change.

Each pool's `pg_count` is a power of two, sized for the largest expected cluster so that each disk holds around 100 PG members summed over all pools (`pg_count × width ÷ disks`): 2048 for a 3-replica pool alone on 60 disks. It can grow later by doubling. Because a PG is the top bits of the hash, it is one contiguous hash range, and doubling splits PG `p` into `2p` and `2p + 1` without rewriting the per-disk index.

**Placement table.** The table maps each PG to its members and carries an epoch number. Its size depends on the pools' PG counts and widths, not on how much data is stored: 2048 PGs × 3 replicas × a 4-byte disk ID is 24 KiB, and even a 4096-PG `ec16-2` pool is 288 KiB, whether the cluster holds 1 GB or 100 TB. So it is **not sharded**. It lives in the single control-plane Raft group, every node caches a full copy, and it changes only on pool, disk and node events, never on the I/O path. Any change (pool created, disk added, node failed, rebalance) bumps the epoch, and nodes reject requests carrying a stale epoch. Recovery and rebalancing while writes are in flight is where the hardest correctness bugs live.

The control-plane leader computes each new table from the previous one, and nodes never compute placement themselves ([ADR 0002](decisions/0002-placement-table-computed-by-control-plane.md)):

1. Refill every missing member onto the least-full eligible disk.
2. While a disk is outside a fill margin of its capacity-weighted share, move one member from the fullest disk to the emptiest eligible one.

Both steps keep the pool's failure-domain rule. This gives exact balance on small clusters with mismatched disks, where a CRUSH-style hash would leave disks unbalanced. Ceph patches the same problem with `pg-upmap` tables.

What does grow with data lives elsewhere. Each disk keeps its own [index](#on-disk-format) of the chunks and shards it holds, and file-to-inode metadata lives in the sharded MetaStore.

### On-disk format

Each disk is log-structured ([ADR 0003](decisions/0003-log-structured-chunkstore-format.md)). It is a sequence of fixed-size segments (64 MiB, to be tuned by benchmarks), and every write is appended:

```
group commit (all writes that arrived during the last flush):
  record = header block [(chunk_id, version, block range, crc32c per 4K block) ...]
         + data blocks
  append the record to the open segment of its stream, flush once, acknowledge every write
  later: commit the index entries for the record (lazily, about once a second)
```

- **Two streams.** New copies go to one open segment. Encoded shards and blocks moved by the cleaner, which are written whole and change rarely, go to another. Keeping them apart makes most segments either mostly live or mostly dead, which keeps cleaning cheap.
- **The log is the source of truth.** The per-disk index maps `(pool, h, chunk_id, version, block)` to a location and checksum. It is a checkpoint of the log, kept in the embedded engine in the daemon's state directory, including for disks under SPDK. After a crash, the daemon replays the records written since the last checkpoint. If the index is lost, a scan of the record headers rebuilds it. A delete appends a tombstone so that a rebuild cannot bring the chunk back.
- **Versions share blocks.** An overwrite appends only the blocks it writes. A version frozen by a [snapshot](#snapshots-and-clones) keeps its older index entries, so it shares every block it did not overwrite with the live version, without reference counts.
- **Cleaning.** A block is live while the live version, a snapshot or an unflattened clone still maps to it. The cleaner picks segments by cost-benefit (free space × age ÷ copy cost, as in the original log-structured file system), copies their live blocks grouped by chunk, which also defragments chunks overwritten at random, and discards (TRIMs) the freed segments. Writes stop at 95% full (see [Volume size and capacity](#volume-size-and-capacity)). Near that limit, block volumes with random overwrites pay more cleaning write amplification.
- **Upgrades.** Segment headers, record headers and index entries carry a format version. A daemon reads the current and the previous format, but writes the new one only after the operator raises the cluster's format level (a `ctl/` key) once every node runs the new release. Until then, rolling back is safe. The cleaner rewrites old segments over time.

### Pools and redundancy

Redundancy is set per **pool**: a named set of placement groups that share one redundancy scheme, one failure-domain rule and, optionally, one class of disk. Every block volume and RWX filesystem belongs to one pool, named by its StorageClass:

```yaml
kind: StorageClass
metadata: { name: footage }
parameters: { pool: ec4-2 }
```

Pools are cluster objects (a `Pool` custom resource, stored under `ctl/pool/` in the control-plane group). Installation creates `rep3` as the default pool. Every pool setting is the user's choice; the defaults suit a small team that wants its data protected but watches cost. They never lower how many failures data survives, and otherwise pick the option that uses less space.

| Setting | Default | Meaning |
| --- | --- | --- |
| `replicas` | 3; `parity + 1` in an erasure-coded pool | Copies of a mutable chunk, 1 to 3 |
| `sealed` | `replicated` | Layout of a chunk that is no longer being written: copies, or `ec` with `data` and `parity` shards |
| `encode_on_write` | On | Encode whole new chunks as they are written, skipping the copies stage |
| `seal_after` | 1 hour for files, 24 hours for block | Idle time before a chunk held as copies is encoded |
| `min_live` | `min(replicas, 2)` | Live PG members needed to accept writes |
| `failure_domain` | `node` | `node` or `disk` |
| `max_per_domain` | 1 | Shards of one PG allowed in one failure domain |
| `locality` | `node` with one copy, `spread` otherwise | Keep each volume on one node, or spread it over every disk |
| `overcommit` | On | Let volume sizes add up to more than the pool holds |
| `disk_class` | Any | Only disks of one class, such as NVMe |
| `pg_count` | Sized from the disk count | Power of two; see [placement groups](#data-layer-chunkstore) |

**Overwrites only ever hit copies.** A 4K overwrite in a 4 data + 2 parity stripe costs 3 reads plus 3 cross-node writes, and risks the write hole on crash:

```
stripe = [d0 d1 d2 d3 | p0 p1], overwrite d1:
  read old_d1, p0, p1
  delta = old_d1 ^ new_d1
  p0' = p0 ^ gf_mul(coef[0][1], delta)
  p1' = p1 ^ gf_mul(coef[1][1], delta)
  write new_d1, p0', p1'      // must land together on 3 nodes
```

Three-way replication costs 3 writes and 0 reads. So a chunk in an erasure-coded pool takes one of two forms, `replicas` copies or `data + parity` shards, and is only overwritten as copies. It is encoded when it is written whole or once it goes idle, as [Windows Azure Storage](https://www.usenix.org/system/files/conference/atc12/atc12-final181_0.pdf) does with its extents (see [When chunks are encoded](#when-chunks-are-encoded)).

**Presets.** Names follow `rep<copies>` and `ec<data>-<parity>`; any combination within the rules below is allowed.

| Pool | Copies | Shards | Survives | Failure domains needed | Usable / raw, encoded | Suits |
| --- | --- | --- | --- | --- | --- | --- |
| `none` | 1 | None | Nothing | 1 | 1 | Caches, CI scratch, apps that replicate themselves |
| `rep2` | 2 | None | 1 failure | 2 | 1/2 | Two-node clusters, re-creatable data |
| `rep3` (default) | 3 | None | 2 failures | 3 | 1/3 | Block volumes, databases |
| `ec2-1` | 2 | 2 + 1 | 1 failure | 3 | 2/3 | Media, photos and backups on a 3-node cluster |
| `ec4-2` | 3 | 4 + 2 | 2 failures | 6, or 3 at 2 per node (then 1 node) | 2/3 | Media, backups and S3 |
| `ec16-2` | 3 | 16 + 2 | 2 failures | 18, or 9 at 2 per node (then 1 node) | 8/9 | Archives on a large cluster |

An erasure-coded pool keeps `parity + 1` copies of a chunk by default, so a chunk survives the same number of failures in either form. "Usable / raw" counts encoded data; data held as copies (being edited, or idle for less than `seal_after`) costs `replicas` times its size.

**Rules checked when a pool is created:**

- **Width fits.** Copies always sit in distinct failure domains. Shards may share one up to `max_per_domain`, so `data + parity ≤ domains × max_per_domain`, counting only disks of the pool's class.
- **Node tolerance is printed.** With `max_per_domain` above 1, one node failure takes out several shards of a PG, and an encoded chunk survives `⌊parity ÷ max_per_domain⌋` node failures. `ec4-2` at 2 per node fits on 3 nodes and survives one node failure (Ceph's `crush-osds-per-failure-domain` does the same); `ec16-2` at 3 per node fits on 6 nodes and survives none. The CLI states, for every pool, how many node failures its copies and its shards survive.
- **Shape is fixed, the rest is not.** `data` and `parity` are fixed at creation, as in Ceph; to change them, [move the volumes](#moving-a-volume-to-another-pool) to another pool. Every other setting can change later; a change to `replicas` adds or drops copies per PG and bumps the epoch.

**Writes and `min_live`.** A write is acknowledged once every live member of the chunk's PG holds it. A PG with fewer than `min_live` live members rejects writes until recovery restores it, rather than accept writes onto too few copies. For `rep2` the default of 2 means a PG stops writing while either of its disks is down. A planned reboot avoids that with `maintenance <node>`: until the node returns, its PGs accept writes on one copy, and they resync first when it is back. Setting `min_live: 1` permanently does the same for unplanned failures, the setting Ceph users most often report losing data with.

**Encoded chunks need no write quorum.** An encoding commits only once all `data + parity` shards are durable, and only then are any copies dropped; a PG with a member down writes copies and defers encoding until recovery. An encoded chunk stays readable while any `data` of its shards survive. Ceph recommends `min_size = k + 1` for erasure-coded pools because it accepts writes to a stripe while only `min_size` shards are live; an encoding waits for every shard, so the rule does not arise.

#### When chunks are encoded

In an erasure-coded pool, a chunk's copies sit on the first `replicas` members of its PG and its shards on all `data + parity` members, so changing form moves no data between PGs. Shard `i` is the chunk's `i`-th contiguous byte range, so a 4K read of an encoded chunk touches one disk; a read whose shard is lost fetches `data` other shards and decodes. A chunk is encoded only if its shards, each rounded up to a 4 KiB block, take less space than its copies, so a small file's only chunk in an `ec16-2` pool stays as copies.

1. **On write** (`encode_on_write`). The daemon's [write-back buffer](#client-design) collects a file's writes chunk by chunk. When a chunk that held no data when the file was opened fills up, the daemon encodes it with Reed-Solomon (ISA-L) and sends each shard straight to its member; the primary commits the encoded version once every member holds its shard. At `close`, the file's last partial chunk is encoded the same way, and S3 objects take this path too. In `ec4-2` the data crosses the network once, at 1.5× its size, where copies-then-encode would cost 3× and then 1.5× more. This is the path for write-once data such as a studio's footage and photos.
2. **When idle** (`seal_after`). Everything else is written as copies: overwrites, files several nodes are writing, chunks flushed early by `fsync` or a full buffer, and block volumes. Each primary encodes chunks that have not been written for `seal_after`:

   ```
   seal(chunk), PG members m[0 .. data+parity]:
     s[0 .. data]   = chunk split into contiguous byte ranges
     p[0 .. parity] = reed_solomon_encode(s)
     send each shard to its member in parallel, wait until all are durable
     commit the encoded version; each member replaces its copy, if any, with its shard
   ```

3. **Back to copies on overwrite.** A write to an encoded chunk first turns it back into copies: the primary reads `data` shards, writes the copies, then applies the write, and the chunk is encoded again once idle. That costs a full chunk read and a replicated chunk write. Editors write new files rather than patch footage, so media rarely pays it. A database on a block volume pays it on the first write to each idle chunk, which is why block chunks wait 24 hours before encoding.

#### One-copy pools and locality

`none` keeps one copy of everything. With `locality: node`, its default, each node has its own PGs, and a volume is placed on one node (the node of its first pod, through CSI's `WaitForFirstConsumer`) with its chunks hashed over that node's PGs only. Losing a disk then loses part of that node's volumes only; reads of the lost chunks return `EIO`, and the pool lists the volumes affected. Pods on other nodes still reach the volume over the network. With `locality: spread`, a volume uses every disk, and any disk loss takes part of every volume in the pool.

**Metadata is not pooled.** Pools set the redundancy of data only. MetaStore shards are always 3-voter Raft groups, whatever pool their files use: metadata is small, and losing a directory loses every file under it.

#### Moving a volume to another pool

A volume moves to another pool in place, keeping its `volume_id`, PVC and inode numbers ([ADR 0005](decisions/0005-move-volumes-between-pools-in-place.md)). The move reuses the [snapshot](#snapshots-and-clones) machinery:

```
move(volume, to):
  cut at epoch E: pause every node using the volume, as for a snapshot
  txn: volume record pool = to, source = (old pool, E)
  resume: writes go to the new pool's PGs; a read of a block not yet
          written there falls through to the old pool at version E
  copy the remaining chunks in the background, as a flatten does
  drop the source pointer; sweep the volume's chunks out of the old pool
```

Chunk IDs are the same in both pools; only the PG changes. Snapshots taken before the move stay in the old pool, since each snapshot record names its pool. Until the copy finishes, the volume uses space in both pools, and reads of uncopied blocks take an extra hop.

In Kubernetes, a move starts when a `VolumeAttributesClass` with a different `pool` parameter is applied to the PVC: the driver advertises the CSI `MODIFY_VOLUME` capability and starts the move in `ControllerModifyVolume`. The CLI can start one too.

### Volume size and capacity

A volume, block or file, can be larger than any single drive. Its chunks are spread across every disk of its pool through placement groups, so the limit is the pool's capacity. A pool reports it as free raw space divided by its encoded overhead (`(data + parity) ÷ data`, or `replicas` in a replicated pool), since with encoding on write most data lands encoded; data still held as copies is shown separately. Pools that share disks draw on the same free space.

- **Thin provisioning.** A chunk exists only after its first write, so creating a volume allocates nothing. With `overcommit` on, volume sizes may add up to more than the pool holds: the pool warns at 85% raw full and fails new writes with `ENOSPC` at 95%, keeping the rest for recovery. With it off, creating a volume reserves its full size.
- **Snapshots use space.** Versions pinned by [snapshots](#snapshots-and-clones) count toward the pool, and the first write to a pinned block needs new space, so near full even an overwrite can fail with `ENOSPC`.
- **Wider blast radius.** A volume spread over every disk is hit by any failure, anywhere in its pool, that takes out more members of one PG than the pool survives. Recovery must restore a degraded PG before a second failure arrives, so it starts with the PGs that have lost the most members.

## Metadata layer: MetaStore

The MetaStore is an ordered, transactional key-value store, sharded into many small Raft groups, each with its own leader. Consensus is needed per key, not globally.

```rust
trait MetaStore {
    async fn txn(&self, ops: Vec<Op>) -> Result<()>;   // compare-and-set, put, delete, all-or-nothing
    async fn scan(&self, prefix: &[u8], after: &[u8], limit: usize) -> Result<Vec<(Key, Val)>>;
}
```

**Why consensus is unavoidable.** Leases, atomic rename and S3 `if-not-exists` are compare-and-set (CAS) operations, and agreeing on one CAS winner among replicas that can crash is consensus. Dynamo-style quorums and conflict-free replicated data types (CRDTs) scale without a leader but cannot do CAS: two racing lock grabs would both succeed.

**Sharding (multi-Raft).** The key space is split into ranges, each its own 3-replica Raft group, with leaders spread across nodes:

```rust
async fn txn(ops: Vec<Op>) -> Result<()> {
    let shards: HashSet<ShardId> = ops.iter().map(|o| shard_map.route(&o.key)).collect();
    if shards.len() == 1 { shard(shards[0]).propose(ops).await }  // fast path: one Raft round
    else { two_phase_commit(ops, shards).await }                 // slow path: rare by design
}
```

**Key layout: the design goal is single-shard for almost everything** ([ADR 0004](decisions/0004-key-layout-and-ids.md)).

Keys are written below as paths. On disk, each `word/` is a one-byte tag, and integers are fixed-width and big-endian, so numeric order is byte order. Only the last component may have variable length (a file name, xattr name or S3 key), so keys need no escaping and a prefix scan is a byte-prefix scan.

**Inode IDs** are 64 bits: a 24-bit *slot* and a 40-bit sequence number. Every key of an inode starts with `fs/{volume}/{inode}`, so a slot is one contiguous key range, and shards split only at slot boundaries.

- A new file takes the next sequence number in its parent's slot. Its entry and its inode are in one shard.
- A new directory takes a free slot from the same shard's range, so `mkdir` is single-shard too, and a later split can move whole subtrees. When the shard has no free slot left, the directory shares its parent's slot, and when a slot's 2^40 IDs run out, new files go to a fresh slot.
- IDs are never reused. The root directory is inode 1, and sequence number 0 of each slot holds the slot's own keys.

| Data | Key | Shard rule |
| --- | --- | --- |
| Pools, placement table, membership, fenced sessions, format level | `ctl/...` | One dedicated control-plane Raft group, not sharded; small because it maps placement groups, not chunks |
| Inode attributes | `fs/{volume}/{inode}/a` | The inode's slot. The volume prefix makes a filesystem one contiguous key range, which a snapshot copies and a deletion drops |
| Directory entries | `fs/{volume}/{dir}/d/{name}` → inode, type | The directory's slot, usually shared with its files, so `readdirplus` is one request to one shard |
| Extended attributes | `fs/{volume}/{inode}/x/{name}` | Next to the attributes, so one request returns both; the daemon caches small ones with the attributes |
| Case-folded names | `fs/{volume}/{dir}/c/{folded name}` → name | Only with the volume setting `case_index` (off by default) for SMB-shared volumes; turning it on later builds it in the background |
| flock and whole-file locks | `fs/{volume}/{inode}/l/{session}` | The inode's slot |
| Slot allocation counter, orphans (unlinked while open) | `fs/{volume}/{slot · 0}/n`, `fs/{volume}/{slot · 0}/o/{inode}` | The slot itself |
| Volume attach and mounts, snapshot records | `volume/{id}/attach`, `volume/{id}/snap/{epoch}` | Any shard; touched only at attach, detach, snapshot and move operations |
| S3 index | `s3/{volume}/{key}` → object ID, size, version | Range sharding, never hash, so `LIST prefix` is a sequential scan. A bucket is a volume, and object IDs come from a slot that each shard reserves, so a `PUT` stays single-shard |

**Inode owner.** The leader of an inode's shard is that inode's *owner*. Besides serving its keys, it keeps in-memory state for open files: who has the file open, who has written, and the current cache mode (see [Cache coherence](#cache-coherence-tiers)). This state is soft. It is not written to the Raft log, so `open` costs one round trip and no disk write. After a leader change, clients re-register their open files during a grace period of one session lease, as in NFS version 4. During it the new owner accepts only re-registrations: new opens and cache-mode changes for its inodes wait, while reads and writes on open handles carry on, since they go to chunk primaries.

**Cross-shard transactions (CockroachDB style, no global timestamp service):**

1. Coordinator writes a transaction record `{id: T, status: PENDING}` in shard X.
2. Each touched shard gets an "intent": a provisional value tagged T, written in parallel.
3. Flip T's record to COMMITTED. This single key is the atomic commit point.
4. Intents are resolved into real values asynchronously. A reader that meets an intent checks T's record, so anyone can finish or abort T after a coordinator crash.

Cross-directory `rename` is the main file operation that takes this path.

**Group commit.** Each leader batches whatever arrived during the last round into one log entry, one fsync and one network round:

```rust
loop {
    let batch = inbox.drain_up_to(1024);
    let entry = encode(&batch);
    tokio::join!(wal.append_and_fsync(&entry), replicate_to_followers(&entry));
    wait_for_majority().await;
    for t in batch { apply(&t); t.reply_ok(); }
}
```

**Thread-per-core.** Each core owns a set of shards outright: their Raft state, write-ahead log and storage engine, with no locks between them. Tables that every I/O reads and that rarely change (the placement table, fenced sessions) are copied to each core and updated by message, so the hot path takes no locks either.

**Reads.** Served by the leader under a time-based lease, so there is no extra round trip. Follower reads with the read-index check come later, if leaders get hot. Readers never wait for writers, except a reader that meets a cross-shard intent and must check its transaction record (below). Both candidate engines give readers a point-in-time view without blocking the writer, but a view held open holds back space reclamation, so anything long-lived, such as a [snapshot's metadata](#snapshots-and-clones), is copied out instead.

**Embedding.** The single-binary goal means Raft plus an ordered storage engine inside the daemon. Candidates are `openraft` for consensus and `redb` or `fjall` for the engine.

**Deferred:** leaderless consensus (Egalitarian Paxos, Accord). Leader-per-shard already removes the single-leader bottleneck.

## File frontend: RWX filesystem

This is the first product. Many pods on many nodes mount one POSIX directory: shared media, uploads, CI caches, datasets, app data. The data path is the shared ChunkStore path. The hard part is keeping each node's cache of pages, attributes and directory entries correct.

### Client design

Every node runs the daemon, which mounts RWX volumes through FUSE. Three rules come straight from the [FUSE spike](#appendix-fuse-coherence-spike):

1. **The kernel page cache is used for reads only.** We never enable FUSE's `writeback_cache`. It is a mount-wide switch, it hides writes from the daemon until the kernel flushes them, and it disables the kernel's automatic invalidation on attribute changes. With it on, the daemon could not stop a node from caching a shared file.
2. **Write-back buffering lives in the daemon.** Every `write()` reaches the daemon as a FUSE request (about 5 µs). While the file is not shared, the daemon buffers writes in its own memory and acknowledges at once. It flushes a chunk as soon as it is full, and the rest on `fsync`, `close` or a recall. The buffer holds up to 256 MiB per node (`writeback_buffer`); when it is full, the oldest partial chunk is flushed. The daemon therefore always knows when a file has been written, and can flush it on demand.
3. **Whether a handle is cached is decided at `open` and cannot change.** The kernel fixes a handle's direct-I/O flag when the file is opened. So the daemon chooses it per open, from the file's cache mode at that moment. Handles that are already open and cached are made coherent by other means (see Tier 2).

**Write path**

```
FUSE write(inode, off, data)
  file not shared: buffer in daemon, acknowledge         // flushed later, in order
  file shared:     chunk_id = (volume, inode, off / CHUNK_SIZE)
                   → ChunkStore.write via the chunk primary → acknowledge
                   → if the write extends the file: MetaStore size = max(size, end)
close() / fsync()  → flush buffered chunks → MetaStore.txn([set size, mtime])
```

**Read path**

```
cached handle:   kernel page cache → on a miss, FUSE read → daemon → ChunkStore.read
uncached handle: FUSE read every time → daemon → ChunkStore.read via the chunk primary
```

**Namespace operations** (`create`, `mkdir`, `unlink`, `rename`, `setattr`) are MetaStore transactions, single-shard except for cross-directory rename.

### Cache coherence tiers

Coherence ships in tiers. Each tier is correct for the workloads in its row, and each one is a usable release.

| Tier | What | Correct for | Effort |
| --- | --- | --- | --- |
| 0 | RWX block: multi-attach, no daemon-side cache, fencing | KubeVirt live migration, cluster-aware apps | Small |
| 1 | Close-to-open consistency, short attribute and directory-entry timeouts, flock, unlink and rename of open files, fencing | One writer per file at a time: media, configs, uploads, datasets, CI caches | Medium |
| 2 | Per-file cache mode switch: cache while one node holds the file or all are readers; uncached once two or more nodes share it and one has written | Concurrent writers on a file, automatically, with no tuning | Medium |
| 3 | Byte-range locks, directory caching with pushed invalidations | SQLite-style apps that lock byte ranges; large directory trees | Large |

Tier 1 already beats Longhorn RWX: there is no NFS pod per volume, so no single-server bottleneck or single point of failure.

**Tier 1: close-to-open**

- **On `open`:** the daemon registers the open with the inode's owner and gets current attributes. If the file changed since this node last cached it, the open is returned without `keep_cache`, and the kernel drops the stale pages.
- **On `close` and `fsync`:** the daemon flushes buffered writes and commits size and mtime. The next open on any node sees them.
- **Metadata:** attributes and directory entries are cached for 1 second by default. A `strict` mount option sets the timeouts to 0.
- **flock and whole-file `fcntl` locks:** MetaStore keys owned by the client's session.
- **Unlink and rename-over of open files:** if anyone still has the inode open, unlink records it in its slot's orphan list instead of freeing it. The owner frees an orphan once no node has it open, and only after any failover grace period. This replaces NFS's `.nfsXXXX` silly rename. It is in Tier 1 because temp files, log rotation and "write a temp file, then rename it over the original" are common.

**Tier 2: the per-file cache mode switch**

Weka caches file data in the page cache by default. When several servers access a file and at least one writes, it bypasses the page cache and sends all I/O to the backend. We do the same, and the inode's owner tracks the mode:

```rust
enum CacheMode {
    Exclusive { client: ClientId },          // one node: cache reads, daemon buffers writes
    ReadShared { readers: Set<ClientId> },   // several nodes, none has written: everyone caches reads
    WriteShared { members: Set<ClientId> },  // several nodes, one or more has written: nobody caches
}
```

The mode is computed from the open registry. A node counts as writing after its first write to the file, whatever flags it passed to `open()`. The daemon sees every write, because the kernel does no write-back.

```mermaid
stateDiagram-v2
    direction LR
    Exclusive: Exclusive<br/>One node has the file open<br/>Cached reads, buffered writes
    ReadShared: ReadShared<br/>Several nodes, no writer<br/>Everyone caches reads
    WriteShared: WriteShared<br/>Several nodes, 1+ has written<br/>Nobody caches
    Exclusive --> ReadShared: 2nd node opens, no writes yet
    Exclusive --> WriteShared: 2nd node opens after a write
    ReadShared --> WriteShared: first write (recall)
    ReadShared --> Exclusive: all but one node close
    WriteShared --> Exclusive: all but one node close
```

*Caching turns off only while a file is write-shared, and turns back on when only one node still has it open.*

**Entering WriteShared.** The owner sends a recall to every other member and waits for every acknowledgment, or for that member's session to expire and be fenced. The writing node's write waits for this too. A recall does four things on the receiving node:

1. **Flush:** write the daemon's buffered writes for the file to the ChunkStore.
2. **Drop cached pages:** `notify_inval_inode` (a few µs).
3. **Uncache new opens:** from now on, open the file with `direct_io`.
4. **Revalidate old handles:** a one-time drop is not enough, because an already-open cached handle refills the page cache on its next read (spike test T3). So while the file is WriteShared, the daemon answers every attribute request for it with a new change counter in the mtime field, with an attribute timeout of 0. With `auto_inval_data` on, the kernel then drops the cached pages before each `read()` on an old handle and fetches fresh data (spike T5c, about 15 µs per read locally). No extra network trip is needed.

```mermaid
sequenceDiagram
    participant A as Node A (writer)
    participant O as Inode owner
    participant B as Node B (reader, cached)
    A->>O: first write to file (mode ReadShared)
    O->>B: recall(inode)
    B->>B: flush buffer, drop pages, direct_io for new opens, revalidate old handles
    B-->>O: ack
    O-->>A: mode = WriteShared
    A->>A: write goes straight to the chunk primary
```

**While WriteShared:**

- **Data:** every read and write goes to the chunk primary, which orders them. That costs one network round trip per I/O, tens of microseconds on 10 GbE with kernel TCP. Write-sharing is rare in homelab workloads, so this is acceptable.
- **File size:** a write that extends the file updates the size in the MetaStore before it is acknowledged, so readers on other nodes can read past the old end of file. Writes inside the file don't touch the MetaStore.
- **Appends** (`O_APPEND`) from several nodes use a MetaStore compare-and-set on the size.
- **mmap:** FUSE does not tell the daemon when a file is mapped, and mapped pages are not revalidated on access (spike T5b). So after each write it commits, the writing node tells the other members' daemons, and each calls `notify_inval_inode` on its own kernel. Mapped readers then see the write within one message delay, not instantly. The `mmap_sync` mount option (off by default) makes each write wait for those invalidations instead. Shared-writable `mmap` across nodes stays a non-goal.

**Leaving WriteShared.** When all but one node have closed the file, the owner returns it to Exclusive. No recall is needed, since no other node is caching. New opens on the remaining node are cached again.

### Sessions and fencing

Each client daemon holds a session with a lease (10 s by default, `session_lease`), renewed with the inode owners it talks to. Its locks, open registrations and cache rights all belong to the session.

- A client stops writing at `lease_expiry - clock_skew_margin`.
- When a session expires, the control plane records it as fenced. Chunk primaries reject writes carrying a fenced session ID, and owners release its locks and open registrations.
- A fenced client must start a new session and reopen its files. Its applications get errors, never silently stale data.

This is the same machinery for flock (Tier 1), recalls (Tier 2) and RWX block (Tier 0).

### Kubernetes integration for RWX

- **CSI:** advertise `ReadWriteMany` for `volumeMode: Filesystem`. The node plugin asks the local daemon to mount the volume with `allow_other`, then bind-mounts it into each pod.
- **fsGroup:** advertise the CSI `VOLUME_MOUNT_GROUP` node capability. Kubelet then passes the pod's fsGroup to the driver instead of recursively changing ownership of every file, which would stall mounts of large volumes.
- **Compute-only nodes** run the daemon in client-only mode: no disks, no shards, just FUSE.
- **Daemon restart and upgrade:** if the FUSE daemon exits, every pod using its mounts gets "Transport endpoint is not connected". The plan is to pass the `/dev/fuse` file descriptors to the new daemon process across a restart, so mounts survive upgrades. The fallback is one mount process per volume, as JuiceFS does with mount pods. This is an open question to resolve before the first release.

### What is not free

- File metadata speed is bounded by MetaStore round trips, so small-file workloads never match a local disk. Every shared POSIX system pays this. Tier 3's pushed directory invalidations let caches live longer.
- While a file is write-shared, its reported mtime changes on every `stat` (a side effect of revalidation). Tools that compare mtimes on a file that another node is actively writing may notice.
- SMB for macOS and Windows clients would be a Samba gateway over the file frontend, and is out of scope for v1. The [Samba spike](#appendix-samba-gateway-spike) found that Samba's leases, share modes and lock state are per gateway, so a gateway on our filesystem needs a VFS module to see writes from other nodes.

## Block frontend

Block volumes use the same ChunkStore path and never touch the MetaStore on the hot path.

**Block 4K write**

```
pod write() → kernel block layer → userspace block device (ublk) in node daemon
  → chunk_id = (volume_id, 0, offset / CHUNK_SIZE) // computed
  → pg = (pool, top bits of xxh3_64(chunk_id))     // computed
  → placement_table[pg] → primary                  // in-memory, epoch-versioned
  → primary: log append ‖ send to the other replicas → acks → done
```

**RWO:** a single-writer attach record is taken in the MetaStore at attach. It prevents two nodes from mounting one volume and corrupting it. Remote clients use NVMe over Fabrics over TCP (NVMe-oF/TCP) instead of ublk.

**RWX block (Tier 0):** several nodes attach one raw device; the homelab user is KubeVirt live migration. The changes are a multi-writer attach record and no caching in the daemon, so every I/O goes to the chunk primary. The kernel's own cache for the block device is the application's choice: KubeVirt opens shared disks with `cache=none` (direct I/O), which is what makes live migration safe.

## Snapshots and clones

Volume snapshots are an optional CSI capability. Once the driver has it, backup tools such as Velero and Kasten work with no further integration: Velero's data mover creates a temporary PVC from the snapshot, mounts it and uploads it with Kopia. Without snapshots, Velero falls back to file-system backup from the live mount, which is not point-in-time.

**CSI side.**

- Advertise the `CREATE_DELETE_SNAPSHOT` controller capability (`CreateSnapshot`, `DeleteSnapshot`, `ListSnapshots`) and `CLONE_VOLUME`, and support `CreateVolume` from a snapshot. Backup needs restore too, since the data mover reads through a restored PVC.
- Ship the `csi-snapshotter` sidecar in the controller pod and a `VolumeSnapshotClass`. The snapshot CRDs and `snapshot-controller` are installed once per cluster and are missing from many distributions, so the Helm chart installs them optionally.
- `csi-sanity` already has snapshot tests.

**A snapshot is a crash-consistent cut.** It holds what the volume would hold had every node using it crashed at one instant: every write acknowledged before the cut and none issued after. Each chunk has its own primary, so no single chunk can define that instant. The cut is taken at the writers instead, with the same fan-out and fencing as a [Tier 2 recall](#cache-coherence-tiers):

```
snapshot(volume):
  E = next snapshot epoch of the volume
  to every node with the volume attached or mounted, in parallel:
    pause new writes and namespace operations on the volume
    wait until in-flight ones are acknowledged, then ack    // or its session expires and is fenced
  txn: put volume/{id}/snap/{E}, plus a marker key in each shard holding fs/{volume}/
  resume every node; its writes now carry epoch E
```

Writes pause for about one round trip to every node using the volume. Tagging writes without the pause would be cheaper but wrong: a node still on the old epoch could write data derived from a post-cut write it read from another node, and the snapshot would hold an effect without its cause. A block RWO volume has one node to pause; a volume nobody is using needs no pause.

The daemon's [write-back buffer](#client-design) is not flushed by default. Unflushed data is what a crash would lose too, and closed files are always included because `close` flushes. The `VolumeSnapshotClass` parameter `flush: true` makes each node flush its buffer for the volume before it acks. Application consistency (a database's own flush, `fsfreeze`) comes from backup-tool hooks.

**Data: copy-on-write per block.** A write carrying epoch E to a chunk last written before E freezes the chunk's current version for the snapshot, then applies. The frozen and live versions share every 4K block the write does not touch, so the first 4K write after a snapshot writes 4K, not a 4 MiB chunk copy; the [on-disk format](#on-disk-format) shares them without reference counts. Frozen versions never change, so in an erasure-coded pool they are encoded at once, without waiting for `seal_after`, and a [return to copies](#when-chunks-are-encoded) keeps the encoded version as the frozen one instead of dropping it. A backup reads the snapshot with `read(at_ver)`, which [any PG member](#data-layer-chunkstore) holding the version can serve, so backups don't load the primaries.

**Metadata.** On each shard, the marker key's Raft log index is the cut. When the shard applies the marker, it opens a point-in-time view of its storage engine, the same kind of view a Raft snapshot is built from, and copies the `fs/{volume}/` range out of it into `fs/{snapshot}/` in the background. The view is closed once the copy is done, so it never holds back space reclamation for the life of the snapshot.

**Restore and clone.** A restored volume gets a new `volume_id` and a parent pointer to `(source volume, E)`:

- **Metadata** is copied from `fs/{snapshot}/` into the new volume's range: one pass over its inodes and directory entries, no data. Orphaned inodes are dropped, since nothing has them open.
- **Data** is read through: a read of a block that chunk `(new, inode, k)` has not written falls through to `(source, inode, k)` at version E, at the cost of an extra hop. Writes land in the new volume, block by block. Inode numbers are unchanged, so `st_ino` survives a restore.
- **Clone from a PVC** (`CLONE_VOLUME`) is a snapshot followed by a restore.
- **Flatten.** With the StorageClass setting `flatten: true` (off by default), a background task copies every chunk the new volume has not written yet, then drops the parent pointer. Off saves space and suits Velero's temporary PVC, which is read once and deleted; a long-lived restored volume can turn it on. A restore into a different pool always flattens. To change a volume's pool without a new PVC, [move it](#moving-a-volume-to-another-pool).

**Deletion.** Deleting a snapshot removes its record at once. A sweep over the volume's PGs then drops each frozen version that no remaining snapshot and no unflattened clone needs. A deleted volume that still has snapshots or clones is hidden from Kubernetes and keeps those chunks until they are gone. Snapshot space counts toward the pool (see [Volume size and capacity](#volume-size-and-capacity)).

## S3 frontend (later)

```
stream body → fresh chunk ids → ChunkStore.write (once each; encoded on write in an erasure-coded pool) → seal
  → MetaStore.txn([put "bucket/key" → chunk list, if-not-exists or version check])
LIST → MetaStore.scan(prefix)
```

The object becomes visible atomically in that one transaction.

## Frontend comparison

|  | File (RWX) | Block | Object (S3) |
| --- | --- | --- | --- |
| Write pattern | Byte-range writes, appends | 4K overwrites in place | Whole object, written once |
| Writers | Many, coordinated per file by cache mode | One (RWO), or many with app coordination (RWX block) | Many, atomic per key |
| Metadata | Heavy: directories, rename, stat, opens | Almost none | Sorted key index |
| MetaStore on hot path | Open, close, fsync, namespace operations; extending writes while write-shared | Never (attach only) | Once per PUT commit, and on LIST |
| Redundancy, in an erasure-coded pool | Encoded as written; copies while shared or edited, encoded once idle | Copies; chunks idle for a day encoded | Encoded as written |
| Snapshot cut | Pause every node with the volume mounted | Pause the attached node (or nodes, for RWX block) | None |
| Hot cost | Metadata round trips | Latency at queue depth 1 | Throughput, listing |

## Modularity: sans-IO core, pluggable drivers

All protocol and replication logic, including the coherence protocol, is written as pure state machines that take events and return actions. They never call a syscall, tokio, SPDK or a clock. This one decision makes deterministic simulation possible and makes every input/output (I/O) backend swappable.

```rust
impl ChunkReplica {
    fn on_event(&mut self, now: Tick, ev: Event) -> Vec<Action> { ... }
}
enum Event  { NetRecv(NodeId, Msg), PeerLost(NodeId), DiskDone(IoId, Result<()>), Timer(TimerId) }
enum Action { NetSend(NodeId, Msg), DiskWrite(IoId, Lba, IoBuf), DiskFsync(IoId), SetTimer(TimerId, Tick) }
```

A driver executes `Action`s and feeds back `Event`s:

| Slot | Default driver | Opt-in fast driver | Test driver |
| --- | --- | --- | --- |
| Disk | `io_uring` on a file or raw device | SPDK on a dedicated NVMe drive | Simulated disk with fault model |
| Network | Kernel TCP | RDMA (real ConnectX or Soft-RoCE); DPDK as an experiment | Simulated network |
| Clock and timers | Monotonic clock | Same | Simulated time |
| Frontend | FUSE client; ublk + CSI driver | NVMe-oF/TCP for remote block clients | Simulated clients with a model page cache that record histories |

**Driver contract.** The [bypass-drivers spike](#appendix-kernel-bypass-drivers-spike) showed that SPDK and RDMA fit this slot, but only if three rules hold from the first (default) driver:

- **Data buffers belong to the driver.** `IoBuf` is a handle from a buffer pool the driver provides, never an arbitrary heap allocation. NVMe under SPDK can only DMA from SPDK-registered memory, and RDMA can only send from registered memory; one stray buffer breaks the whole RDMA connection, and registering per I/O costs as much as the I/O. The default drivers and the simulator use a plain heap pool, so nothing changes for them. Control messages up to 256 bytes go inline and need no pool buffer.
- **Every driver exposes a readiness fd and defaults to event mode.** SPDK's thread interrupt fd and the RDMA completion-channel fd both sit in the daemon's epoll loop. Busy polling is a separate opt-in for dedicated cores.
- **Network connections fail as a unit.** A transport error on a connection becomes one `PeerLost` event, every outstanding request on it fails, and the driver reconnects. The core keeps its own deadlines and never relies on transport timeouts.

**Crate layout (proposed)**

- `core`: state machines for ChunkStore replicas, placement, Raft shards, sessions and leases. No I/O dependencies.
- `meta`: MetaStore shard logic, key layout, cross-shard transactions, and the inode owner (open registry, cache modes, recalls).
- `drivers-uring`, `drivers-tcp`, `drivers-spdk`, `drivers-rdma`: one crate per backend, behind feature flags so the default build needs no SPDK.
- `sim`: deterministic simulator, fault models and invariant checkers.
- `frontend-file`, `frontend-block`, `frontend-s3`: protocol adapters onto `ChunkStore` and `MetaStore`.
- `daemon`: the single binary that wires the drivers and frontends picked by flags.
- `csi`: Kubernetes CSI driver.

**Transport negotiation.** Every node pair connects over TCP first, then upgrades to RDMA if both ends report a usable device. RDMA needs that TCP channel anyway, to exchange queue pair numbers and addresses. The daemon loads `libibverbs` at runtime, so one binary runs on nodes with and without rdma-core, and a missing library, device or memory-lock allowance means TCP, not an error. Mixed clusters work without configuration. Turning RDMA on at install (off by default) adds `IPC_LOCK` to the daemon pod so its buffer pool can be registered.

## Testing strategy

Most bugs live in interleavings that a real cluster hits rarely and can't reproduce. So most testing happens in deterministic simulation, and real hardware confirms what simulation can't model.

| Layer | What it catches | Tools |
| --- | --- | --- |
| 1. Deterministic simulation testing (DST) | Interleaving bugs across crashes, partitions, disk faults and lost recalls; reproducible from a seed | Own simulator over the sans-IO core |
| 2. Protocol model checking | Design bugs in the epoch protocol, sessions and fencing, cache-mode recalls, snapshot cuts, cross-shard commit | `stateright` (models in plain Rust) |
| 3. Consistency checking | Reads that no valid ordering could produce; close-to-open violations | Porcupine (Go), Jepsen Elle |
| 4. Real-cluster fault injection | Kernel, FUSE, firmware, fsync and RDMA behavior | device-mapper, SPDK error/delay bdevs, eBPF error injection (`bpf_override_return`), `tc netem`, `nftables`, `kill -9`, power cuts |
| 5. Heterogeneous matrix and property tests | Placement and rebalance bugs on mixed hardware | Generated matrix, `proptest` |
| 6. Conformance suites | POSIX and protocol compatibility gaps | `pjdfstest`, `fsx` (single and multi-client), `csi-sanity`, Kubernetes storage end-to-end tests, Ceph `s3-tests` |
| 7. Performance regression | Latency, CPU and recovery regressions | `fio` and metadata benchmarks nightly → VictoriaMetrics → Grafana; eBPF off-CPU and latency profiles to explain regressions |

**1. Deterministic simulation.** All nodes, disks, clients and the network run in one thread from one seeded random number generator. The simulator drops, delays, reorders and duplicates messages, and crashes and restarts nodes. A failing seed replays exactly. Run thousands of seeds per commit in continuous integration (CI), and millions overnight on idle homelab machines.

The simulated disk models:

- torn writes (only some 4K sectors of a larger write land);
- loss of all unflushed data on crash;
- silent bit flips and misdirected writes;
- fail-slow: a disk 100x slower that never errors.

Simulated FUSE clients model the kernel behavior the spike measured: a page cache that refills on reads, direct-I/O fixed at open, and mapped pages that are not revalidated.

Simulation faults for coherence: lost or delayed recalls, clients paused past their lease, clock skew, owner failover in the middle of a recall.

Simulation faults for redundancy: crashes and disk losses in the middle of an encoding (on write or when idle), a return to copies or a recovery, PGs dropping below `min_live`, and losses beyond what a pool survives.

Simulation faults for snapshots and pool moves: a node that never acks the pause, coordinator crash between pause and resume, shard leader change at the cut, snapshot deletion while a clone reads through or flattens, and a crash in the middle of a move's copy.

Simulation faults for the on-disk format: crashes during cleaning and between a record's flush and its index checkpoint, a torn record at the log's tail, and a lost index that must be rebuilt from the log.

Invariants checked after every step:

- every acknowledged write is readable;
- every chunk reaches its pool's redundancy (copies, or all `data + parity` shards) within the pool's failure-domain rule once recovery settles;
- an encoded version becomes readable, and copies are dropped, only after all its shards are durable;
- a chunk with fewer surviving copies or shards than it needs returns an error, never wrong data;
- at most one lock holder per lock; at most one writer per RWO volume;
- no client reads stale data from a file in WriteShared mode, and no fenced session's write is applied;
- an orphaned inode is never freed while a live session has it open;
- a snapshot holds every write acknowledged before its cut and none issued after, in both data and metadata;
- a frozen version is never dropped, and the cleaner never frees a block, while the live version, a snapshot or an unflattened clone needs it;
- an index rebuilt from the log equals the checkpointed one;
- epochs never go backwards on any node.

**3. Consistency checking.** Record every client operation as `(invoke_time, complete_time, op, result)`, from simulation and real clusters alike. For write-shared files the checker searches for a linearizable order. For other files it checks close-to-open: an open that starts after another node's close sees that close's data.

**4. Real cluster.** Run Jepsen-style nemesis schedules nightly: random kills, partitions, disk errors and slow disks during a multi-node file workload, checked at the end.

- **FUSE behavior** is pinned by the spike harness in [spikes/fuse-coherence](../spikes/fuse-coherence/), run in CI against each supported kernel.
- **Power cuts** via the Intelligent Platform Management Interface (IPMI) or a smart power strip are the only honest fsync test. Write checksummed patterns with `fio --verify=crc32c`, cut power, boot, verify. Some consumer NVMe drives acknowledge flushes they haven't done.
- **Soft-RoCE (`rdma_rxe`)** runs the RDMA code path on any ordinary network card, and `tc netem` can inject faults under it. It is a functional stand-in only: it is slower than kernel TCP at every message size, and it treats `retry_cnt=7` as "retry forever", unlike real NICs. RDMA performance decisions need a real card.
- device-mapper can't see SPDK I/O, so the SPDK path uses SPDK's own error and delay bdev modules.

**5. Test matrix.**

```
transport:  TCP-only | RDMA-only | mixed
disks/node: 1 | 4 | mixed sizes | mixed speed (NVMe + SATA SSD + HDD)
cluster:    3 nodes | 5 nodes | add node mid-run | remove disk mid-run
clients:    storage nodes only | plus compute-only nodes
backend:    io_uring | SPDK
pool:       none | rep2 | rep3 | ec2-1 | ec4-2 at 2 per node
```

Property tests on placement: after any random add, remove and failure sequence, every placement group keeps its pool's width with no more than `max_per_domain` members per failure domain, no disk exceeds a set fill margin above average, and chunks moved stay near the minimum.

**7. Performance.** Track metadata operations per second (create, stat, rename), cached and write-shared read latency, 4K block latency at queue depth 1, CPU per operation, and 99.9th-percentile latency during recovery. A 20% overnight regression is treated as a bug.

**Starting hardware.** One big machine is enough at first: a few virtual machines, QEMU's emulated NVMe devices (SPDK runs against them for correctness, not speed; this needs root, `vfio-pci` and hugepages inside the virtual machine). SPDK's file-backed aio bdev needs none of that and runs in CI as a normal user, and Soft-RoCE between the virtual machines.

## Alternatives considered

Each choice below favors a short hot path, a single binary and testable correctness over peak features. Choices settled after the first draft link their ADR in `docs/decisions/`.

| Decision | Chosen | Alternatives weighed | Why the chosen one |
| --- | --- | --- | --- |
| First product | RWX filesystem | Block first; S3 first | RWX is where homelab users have no good option; block alone competes with Longhorn v2 and Mayastor |
| Language | Rust | Go; C++ (Ceph, 3FS) | A learning goal; no garbage-collector pauses in tail latency; memory safety; SPDK and RDMA are C libraries, reached through bindgen without cgo overhead |
| File client | FUSE | Own kernel module (Weka, CephFS); NFS gateway pod (Longhorn RWX) | The spike shows FUSE can do per-file cache switching. FUSE is safe to iterate on, and a crash doesn't take down the kernel. A gateway funnels all traffic through one pod |
| Where write-back lives | In the daemon | FUSE `writeback_cache` (kernel page cache) | The kernel option is mount-wide, hides writes from the daemon and can't be revoked per file, so write-shared mode couldn't be enforced (spike T9) |
| Coherence | Tiers 1 → 2 → 3 | Close-to-open only (NFS, JuiceFS); full capabilities from the start (CephFS); caching left to the user (DAOS) | Ships useful RWX early; the Weka-style mode switch gets most of the benefit of capabilities with three states and one message type |
| Open-file state | Soft state at the inode owner, rebuilt after failover | Durable open records in Raft | Keeps `open` to one round trip with no fsync, which matters for small-file workloads |
| Kernel bypass | Opt-in flag | Mandatory DPDK and SPDK (Weka, Mayastor, Longhorn v2) | Homelab links are network-bound, so bypass buys latency, not throughput. Mandatory hugepages and pinned cores break "plug and play" |
| Core abstraction | Two narrow primitives: ChunkStore + MetaStore (Tectonic, Colossus, 3FS) | One rich object layer (Ceph RADOS) | Each frontend pays only for the metadata it needs |
| Metadata store | Embedded multi-Raft | External FoundationDB (3FS), TiKV, Redis (JuiceFS), etcd | The single-binary goal rules out a second system to operate. An external etcd remains an option for early prototypes |
| Consensus shape | Leader per shard, one shard at first | Single global Raft group; leaderless Egalitarian Paxos or Accord; quorum or CRDT with no consensus (Garage) | CAS is required for locks and rename, which rules out quorums and CRDTs. Per-shard leaders already remove the single-leader bottleneck |
| Cross-shard transactions | Transaction record plus intents (CockroachDB style) | Percolator with a central timestamp oracle (TiKV); a central sequencer (FoundationDB) | No extra central service; the commit point rides on one shard's Raft |
| Redundancy | Per pool, chosen by the user: 1 to 3 copies, optionally erasure coding; defaults favor capacity without lowering failure tolerance | One scheme for the whole cluster; a replica count per volume (Longhorn) | Placement groups spread every volume over every disk, so a redundancy scheme has to belong to a whole set of PGs, not one volume. Pools also let one cluster mix scratch, block and media data |
| Erasure coding of mutable data | Never in place: encode whole new chunks and idle chunks, overwrite only copies (Windows Azure Storage) | In-place overwrites of stripes (Ceph `allow_ec_overwrites`, Vitastor); erasure-code everything with redirect-on-write (Weka) | Avoids read-modify-write and the write hole without cluster-wide stripe garbage collection. Closing the write hole in place is hard: Vitastor's chaos test failed on every seed of its two-phase erasure-coded configurations before its [3.2.0 fixes](https://vitastor.io/en/blog/2026-08-31-v3.2.0.html) |
| Snapshot cut | Pause every writer of the volume for one round trip, then resume under a new epoch | Multi-version MetaStore and chunks with hybrid logical clock timestamps (CockroachDB); tagging writes with the new epoch without a pause | Reuses the recall fan-out and Raft's point-in-time view, with no timestamps in the key encoding. Tagging alone can capture an effect without its cause |
| Restore and clone | New volume with a parent pointer, flattened in the background when `flatten` is on (Ceph RBD) | Copy all data at restore | Restore is instant and costs no space; Velero's temporary PVCs never pay for a copy |
| Replication protocol | Primary fans out to replicas in parallel (Ceph, Vitastor) | Chain replication or CRAQ (3FS); leaderless quorum writes | One added hop instead of one per chain link; the primary also gives each chunk a single write order |
| Placement | Chunks hashed into placement groups; table of PG → members with epochs, computed by the control plane ([ADR 0002](decisions/0002-placement-table-computed-by-control-plane.md)) | Per-chunk table; placement every node computes (Ceph CRUSH, Weka's algorithmic ownership) | Exact balance on small clusters with mismatched disks, and explicit, throttled moves; Ceph adds `pg-upmap` tables to CRUSH for the same reason. Placement groups keep the table to a few hundred KiB, and epochs are easy to simulate |
| ChunkStore on-disk format | Log-structured segments with a cleaner; the per-disk index is a checkpoint of the log ([ADR 0003](decisions/0003-log-structured-chunkstore-format.md)) | Journal plus in-place data with a reference-counted extent map (Vitastor, BlueStore); chunk data inside the key-value engine | One write and one flush per durable write; snapshot block sharing, checksums and torn-write detection come free; sequential writes and TRIM suit consumer SSDs. Costs a cleaner |
| Metadata keys and inode IDs | Keys grouped by inode; inode IDs of a slot plus a sequence number, with shards split at slot boundaries ([ADR 0004](decisions/0004-key-layout-and-ids.md)) | One global transaction per create (3FS on FoundationDB, JuiceFS on TiKV); inodes inside directory entries (CephFS, HopsFS) | `create` and `mkdir` stay single-shard with no global transaction service, and `st_ino` stays stable |
| Changing a volume's pool | In place: cut, read-through to the old pool, background copy ([ADR 0005](decisions/0005-move-volumes-between-pools-in-place.md)) | Snapshot and restore into a new PVC only | Keeps the PVC, so applications are not rebound; reuses the snapshot machinery |
| Block frontend | ublk locally, NVMe-oF/TCP remotely | NBD (single-threaded overhead); iSCSI (old, heavy) | ublk rides io\_uring with little overhead; NVMe-oF is the native remote protocol for NVMe |
| Simulation | Own simulator over a sans-IO core | turmoil or madsim; Jepsen-only testing | We need a detailed disk and page-cache fault model, which those crates don't cover. Jepsen alone can't reproduce failures |
| License | Apache 2.0 ([ADR 0001](decisions/0001-name-rock-solid-apache-2.md)) | AGPL (MinIO); network copyleft (Vitastor) | Copyleft with a network clause deters adopters |

## Roadmap and open questions

Build order puts the simulator before any real driver and ships RWX file storage as the first usable product. Each phase ends with something that runs and is tested.

1. **Sans-IO core and simulator.** Single-node ChunkStore state machine with the log-structured on-disk format and its cleaner, simulated disk with the full fault model, invariant checker.
2. **Protocol models.** `stateright` models of placement epochs, sessions and fencing, and cache-mode recalls, before writing the real protocols.
3. **Replication and placement.** Pools with 1 to 3 replicas and `min_live`, primary-based replication, placement groups and the placement table computed by the control plane, with epochs, recovery per placement group. Fault-injected in simulation from the first commit.
4. **Embedded MetaStore.** Raft with group commit, one shard only, with the sharded API from day one. Inodes, directory entries, sessions and locks.
5. **Default drivers.** `io_uring` and kernel TCP behind the `Action` interface, with the driver contract (driver-owned buffer pools, readiness fd, `PeerLost`) in place from the start.
6. **File frontend with Tier 1: first usable product.** FUSE client with daemon-side write-back, close-to-open, flock, orphans, fencing. CSI driver with RWX and fsGroup delegation. Passes `pjdfstest`, single and multi-client `fsx`, and `csi-sanity`. Nightly nemesis runs and power-cut tests.
7. **Tier 2.** Inode owner with open registry and cache modes, recalls, revalidation of old handles, mmap invalidations. Simulated FUSE clients and the spike harness in CI.
8. **Block frontend.** ublk plus CSI for RWO, then RWX block (Tier 0).
9. **Opt-in fast drivers.** SPDK and RDMA behind the same `Action` interface; Soft-RoCE lab on virtual machines.
10. **Erasure-coded pools.** Encoding on write and when idle, return to copies on overwrite, Reed-Solomon with ISA-L, degraded reads, shard recovery, several shards per node.
11. **Snapshots, clones and pool moves.** Snapshot cut, per-block copy-on-write, restore with read-through and flatten, in-place pool moves through `ControllerModifyVolume`, CSI snapshot capabilities. Passes the `csi-sanity` snapshot tests and a Velero backup and restore.
12. **S3 frontend.** Seal plus MetaStore transactions; Ceph `s3-tests`.
13. **Scale-out and Tier 3.** Range splitting and leader balancing across nodes; byte-range locks; directory caching with pushed invalidations.

**Open questions**

Each of these needs a measurement or a spike, not a design choice.

- [ ] FUSE daemon restarts: pass `/dev/fuse` file descriptors to the new process, or run one mount process per volume? The spike must show what happens to requests the old daemon read but never answered (Linux 6.9 added `FUSE_NOTIFY_RESEND` to requeue them), and where the descriptors live while no daemon runs.
- [ ] Flush latency of common consumer NVMe drives, which bounds durable write latency on homelab hardware. Measure with eBPF on the `nvme_setup_cmd` and `nvme_complete_rq` tracepoints, cross-checked with `fio --fdatasync=1`, and record whether each drive has a DRAM cache and power-loss protection.
- [ ] Raft and storage engine: `openraft` maturity for multi-Raft (memory and tick cost of about 1,000 groups in one process), `redb` versus `fjall` for Raft logs, MetaStore shards and the per-disk [chunk index](#on-disk-format).
- [ ] Cleaner write amplification of the [on-disk format](#on-disk-format) for block volumes with random overwrites as a consumer SSD fills, which decides the segment size and whether the 95% write limit needs a larger reserve.
- [ ] SPDK NVMe in interrupt mode: does the PCIe NVMe driver support it with `vfio-pci`, and at what latency cost? A QEMU guest with an emulated NVMe and a virtual IOMMU can show whether it works; the latency cost needs a spare NVMe drive.

## Learning project

Each learning track starts with a small throwaway exercise, then lands as a real component, so learning time also builds the project. The order follows the roadmap: correctness tools first, kernel-bypass hardware last.

| Track | Warm-up exercise | Lands in the project as | Roadmap phase |
| --- | --- | --- | --- |
| Rust for systems code | Write a sans-IO echo state machine plus a tiny simulator that drops and reorders messages | `core` and `sim` crates | 1 |
| Correctness and consensus | Fly.io Gossip Glomers challenges on Maelstrom, through to the linearizable key-value one; then Raft from the paper | MetaStore shards, Raft with group commit | 2, 4 |
| Model checking | A `stateright` model of a 3-node lease: prove at most one holder per epoch | Models of placement epochs, sessions, cache-mode recalls | 2 |
| Consistency checking | Feed a recorded history from your simulator into Porcupine (Go) | History recorder plus checker in CI | 1, 3 |
| io\_uring | Raw-device read/write benchmark with the `io-uring` crate, compared against fio | Default disk driver | 5 |
| FUSE | Extend the coherence spike: add a second inode and directory-entry invalidation | `frontend-file` | 6, 7 |
| eBPF | bpftrace: a flush-latency histogram for your NVMe drives; count FUSE reads served by the daemon versus the page cache during spike tests T3 and T5 | Answer to the flush-latency open question; kernel-side checks in the spike CI; tracing and fault-injection tooling in Rust with `aya`. eBPF stays out of the data path: the verifier's limits make it the wrong home for storage logic | 1 (spike), 6 onward |
| Kubernetes storage | A toy CSI driver that provisions directories, run through `csi-sanity` | `csi` crate | 6 |
| SPDK | Run SPDK's NVMe perf tool and hello\_bdev against a QEMU emulated NVMe; then call one bdev from Rust via bindgen, studying Mayastor's Rust bindings | Opt-in SPDK disk driver | 9 |
| RDMA | rdma-core's ibv\_rc\_pingpong over Soft-RoCE between two virtual machines; then a Rust RDMA write-and-completion loop | Opt-in RDMA network driver, transport negotiation | 9 |
| Erasure coding | Reed-Solomon over GF(2^8) from scratch: encode, lose shards, decode; then benchmark it against ISA-L | Encoding in erasure-coded pools | 10 |
| DPDK | DPDK's l2fwd or testpmd on a spare port or virtual function; measure the busy-poll core cost. Compare with AF_XDP, which skips most of the kernel network stack without hugepages or a dedicated card | Side experiment only; informs the opt-in decision | Any |
| Performance work | Profile one hot path with perf and flame graphs; count CPU per operation | Nightly performance tracking | 6 onward |

**Done-when checkpoints**

- [ ] Simulator replays a failing seed exactly, and finds a bug planted in a toy replication protocol.
- [ ] Raft passes the Maelstrom linearizable key-value workload under partitions.
- [ ] Same `ChunkStore` test suite passes on the simulated disk, io\_uring and SPDK.
- [ ] Two pods on two nodes share an RWX PVC, one node is power-cut mid-write, and every file that was closed before the cut verifies after recovery.
- [ ] Two nodes write one file concurrently under `fsx` with Tier 2 on, and the consistency checker finds no stale read.
- [ ] RDMA and TCP drivers pass the same network test suite; a mixed 3-node cluster negotiates per pair.

## Appendix: FUSE coherence spike

**Question.** Can a FUSE daemon stop a node from serving stale cached data for a file that another node is writing, including through handles that were already open, without a kernel module?

**Setup.** Linux 6.17, libfuse 3.14. Two FUSE mounts (nodes A and B) serve one backing file, standing in for two clients over shared storage. A control channel triggers invalidations and changes open and attribute policy at runtime. Findings were checked against the kernel source (`fs/fuse/file.c`, `inode.c`). Code and raw output: [spikes/fuse-coherence](../spikes/fuse-coherence/).

| Test | Result |
| --- | --- |
| T1: B has the file cached, A writes | B reads stale data. A coherence protocol is needed |
| T2: daemon sends `notify_inval_inode` to B | B's next read is fresh. Cost: 2 to 25 µs |
| T3: B reads again, then A writes again | **Stale.** The old handle refills the page cache, so one recall at the mode switch is not enough |
| T4: new opens get `direct_io`; an old cached handle stays open | New handle fresh, **old handle stale**. The direct-I/O flag is fixed at open (`file.c`: `ff->open_flags`) |
| T5: `auto_inval_data` + attribute timeout 0 + data version as mtime | Old cached handle fresh on `read()`. One attribute request per read (about 5 to 6.5 µs locally) |
| T5b: same, but read through `mmap` | **Stale** until `notify_inval_inode`. Mapped pages are not revalidated |
| T5c: daemon reports a new change counter on every attribute request | Old cached handle always fresh, with no version lookup. About 15 to 17 µs per 4K read locally |
| T6: open without `keep_cache` | Stale pages dropped at open. Close-to-open works |
| T7: 4K read cost on B | Cached 0.3 µs; `direct_io` 5.4 to 6.7 µs; revalidated as in T5c, 15 to 17 µs (local daemon, no network) |
| T9: B mounted with `writeback_cache` | B's writes don't reach the daemon until flushed. `notify_inval_inode` forces them out (40 to 190 µs). `auto_inval_data` has no effect on regular files in this mode, so B stays stale |

**Conclusions** (applied in [File frontend](#file-frontend-rwx-filesystem))

- FUSE is enough for Tier 2; a kernel module is not needed.
- Never enable `writeback_cache`; buffer writes in the daemon.
- A recall must drop pages, switch new opens to `direct_io`, and put old handles into per-read revalidation.
- mmap readers need an invalidation after each write.

## Appendix: kernel-bypass drivers spike

**Question.** Can SPDK and RDMA be added later as opt-in drivers behind the `Action` interface, without changing the core, the default build or the no-hugepages install? Or does something in them have to shape the interface now?

**Setup.** Linux 6.17, laptop with one consumer NVMe drive (the root disk). SPDK v26.09 (DPDK 26.03) and rdma-core v50.0, both shallow clones, built and run as a normal user. SPDK runs inside the spike's own event loop (no `spdk_app_start`, no reactors) on malloc, aio and uring bdevs over a file. RDMA runs on Soft-RoCE over Wi-Fi; two queue pairs on one device stand in for two nodes. Code and raw output: [spikes/bypass-drivers](../spikes/bypass-drivers/).

| Test | Result |
| --- | --- |
| SPDK without hugepages, as a normal user | Works, but only after moving DPDK's base address below the host's IOMMU address width (42 bits here). The default fails at startup |
| SPDK memory | About 430 MB RSS with default pools; 63 MB with pools sized explicitly |
| SPDK 4K I/O, polling vs interrupt mode (aio bdev) | Same latency (write 13 µs, read 17 µs at p50). CPU per I/O 43 µs polling, 2.9 µs in interrupt mode; idle loop 100% vs 0% of a core. The uring bdev has no interrupt support and keeps a core busy |
| Handing I/O to the SPDK thread from another thread | 1.6 µs (poll) / 3 µs (interrupt) |
| SPDK embedding pitfalls | Interrupt mode crashes on shutdown unless the thread library gets a scheduler callback (`spdk_thread_lib_init_ext`). Linking without `--whole-archive` builds fine and fails at startup |
| SPDK DMA | Without hugepages, no memory is DMA-able. Real NVMe under SPDK still needs root, `vfio-pci` and very likely hugepages |
| RDMA library loading | Loaded at runtime; no library or no device means TCP, not a crash |
| RDMA memory registration | 2.3 µs for 4 KB, 12 µs for 1 MB, 170 µs for 16 MB. Fails with ENOMEM above the process's memory-lock limit (a 64 KB limit can't register 1 MB) |
| RDMA round trips on Soft-RoCE | 4 KB: 12 µs polling, 16 µs event-driven; kernel TCP loopback 9 µs. Event mode uses 5 to 10 µs of CPU per round trip at every size; polling burns the whole wait |
| RDMA bad buffer | Sending from memory outside any registration breaks the queue pair; every later send fails until it is rebuilt |
| RDMA dead peer | Detected in 4 to 540 ms depending on timeout settings. With `retry_cnt=7` Soft-RoCE retries forever |

**Conclusions** (applied in [Modularity](#modularity-sans-io-core-pluggable-drivers))

- Both drivers fit the driver slot; neither needs the core to change.
- The driver contract (driver-owned buffers, a readiness fd with event mode by default, connection-level failure) must exist from the default drivers on, or SPDK and RDMA would force a retrofit of every `Action`.
- SPDK's opt-in has two tiers: file-backed bdevs need no privileges; real NVMe needs root, `vfio-pci` and hugepages, on a dedicated drive.
- RDMA needs `IPC_LOCK` or a large enough memory-lock limit in the daemon pod, and TCP to bootstrap every connection.

## Appendix: Samba gateway spike

**Question.** If SMB is later served by Samba on top of the file frontend, how does Samba handle client caching, share modes and byte-range locks, what breaks with more than one gateway, and what does Samba need from the filesystem under it?

**Setup.** Linux 6.17, Samba 4.19.5 run unprivileged under `uid_wrapper`, a Python SMB 3.1.1 client that records and acknowledges lease breaks. Each gateway is one smbd with its own state, as on one node without CTDB. A counting passthrough FUSE filesystem stands in for the frontend; two mounts of one directory stand in for two nodes. Code and raw output: [spikes/samba](../spikes/samba/).

| Test | Result |
| --- | --- |
| Lease RWH held, a second client opens | Break to RH; the open waits for the ack (2 ms). Unacked: the open blocks 35 s |
| Read-caching lease held, another client writes | Break sent, but the write does not wait: SMB readers can be stale for one message delay |
| Share modes (deny write) | Enforced between SMB clients only; a local process writes freely |
| Byte-range locks | Mandatory for SMB clients; mirrored as OFD locks, which local processes see but may ignore |
| Two gateways, no CTDB | Both clients granted RWH on one file, share modes ignored. Locks cross only through cluster-wide fcntl locks |
| `kernel oplocks = yes` | Disables SMB2 leases. Crosses gateways on one kernel at 1 s per conflicting open; on FUSE, leases stay local to each mount |
| FUSE requests per SMB operation | create 22, open + read 10, stat 6, 100-entry listing 312 (lookup, `user.DOSATTRIB`, `listxattr` per entry) |
| Create in a 10,000-entry directory | 4.6 ms (Samba scans the directory for a case-insensitive match) vs 0.94 ms with `case sensitive = yes` |
| File identity across mounts | Same `st_ino`, different `st_dev` (CTDB needs `vfs_fileid`) |

**Conclusions** (applied in [Key layout](#metadata-layer-metastore) by [ADR 0004](decisions/0004-key-layout-and-ids.md))

- SMB leases map onto Tier 2 cache modes: a lease break is a recall. A Samba VFS module that registers the gateway's opens with the inode owner would make SMB clients coherent with pods on other nodes; without it, shares that pods also write need `oplocks = no`.
- Start with one gateway per volume; CTDB across nodes needs a cluster lock (a MetaStore-backed helper) and cluster-wide FUSE `lock`/`flock`.
- Shape the frontend now: inode IDs exposed as `st_ino`, small xattrs stored with the inode and cached by the daemon, readdirplus, and room for a case-folded name index.
