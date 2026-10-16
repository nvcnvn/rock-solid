# 0003. Store chunks in a log-structured format

Status: Accepted (2026-10-16)

## Context
Each disk's on-disk format has to support:

- 4K overwrites at queue depth 1 for block volumes, where a durable write costs one flush and consumer drives flush slowly;
- whole 4 MiB chunks and immutable erasure-coded shards for media;
- [snapshots](../design.md#snapshots-and-clones) in which a frozen version shares every 4K block it did not overwrite with the live version;
- a checksum on every 4K block, and recovery from torn writes;
- format upgrades with rollback;
- the SPDK driver, which has no filesystem under it.

There are two families:

- **Journal plus in-place data** (Vitastor; BlueStore's deferred writes). A small write goes to the journal, is acknowledged, and is copied to its place later. Big writes must skip the journal, or media bandwidth would double. Sharing blocks with snapshots needs a per-chunk extent map with reference counts, which is BlueStore's most complex part.
- **Log-structured** (LFS, Ceph SeaStore, most all-flash arrays). Every write appends to a segment, an index maps blocks to locations, and a cleaner reclaims segments.

A third option, keeping chunk data inside the embedded key-value engine, gives up control of flush ordering and direct I/O, and can't run on SPDK.

## Decision
The ChunkStore is log-structured. The disk is a sequence of fixed-size segments. Each group commit appends one record, a header block that describes every write in the batch (chunk, version, block range, CRC32C per block) followed by their data, and then flushes once. Writes go into two streams: new copies in one, encoded shards and blocks moved by the cleaner in the other.

The log is the source of truth. The per-disk index, which maps (chunk, version, block) to a location and checksum, is a checkpoint of the log. It is kept in the embedded engine in the daemon's state directory and committed lazily. After a crash, the daemon replays the records written since the last checkpoint. If the index is lost, a scan of the record headers rebuilds it. A delete appends a tombstone.

A frozen version keeps its older index entries, so snapshots need no reference counts. A block is live while the live version or a pinned version still maps to it. The cleaner picks segments by cost-benefit, copies their live blocks grouped by chunk, and discards (TRIMs) the freed segments.

Segment headers, record headers and index entries carry a format version. See [ChunkStore on-disk format](../design.md#on-disk-format) for upgrades.

## Consequences
- A durable write costs one sequential write and one flush, whatever its size and whether or not a snapshot pins the chunk.
- Snapshot block sharing, checksums and torn-write detection come with the format instead of being added to it.
- Writes are sequential and freed space is TRIMmed, which suits consumer SSDs and their own flash translation layer.
- The cleaner is new code and costs CPU and write amplification. The amplification grows as a disk fills, so block volumes with random overwrites pay most near the pool's 95% write limit.
- A block chunk overwritten at random is fragmented on disk until the cleaner regroups it. Sequential reads of such a chunk become random 4K reads, which NVMe handles well and SATA SSDs and HDDs handle less well.
- The index needs a filesystem. Disks under SPDK keep their index on the node's state directory, and it can always be rebuilt from the disk itself.
