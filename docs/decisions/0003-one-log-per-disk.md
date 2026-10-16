# 0003. Keep one log per disk for chunks, Raft logs and indexes

Status: Accepted (2026-10-16)

## Context
Each disk's on-disk format has to support:

- 4K overwrites at queue depth 1 for block volumes, where a durable write costs one flush and consumer drives flush slowly;
- whole 4 MiB chunks and immutable erasure-coded shards for media;
- [snapshots](../design.md#snapshots-and-clones) in which a frozen version shares every 4K block it did not overwrite with the live version;
- a checksum on every 4K block, and recovery from torn writes;
- the MetaStore: on the order of 1,000 Raft groups per node, each committing small entries at a high rate;
- format upgrades with rollback;
- the SPDK driver, which has no filesystem under it;
- deterministic simulation of every durable write, including crashes between a flush and a checkpoint.

For chunk data there are two families:

- **Journal plus in-place data** (Vitastor; BlueStore's deferred writes). A small write goes to the journal, is acknowledged, and is copied to its place later. Big writes must skip the journal, or media bandwidth would double. Sharing blocks with snapshots needs a per-chunk extent map with reference counts, which is BlueStore's most complex part.
- **Log-structured** (LFS, Ceph SeaStore, most all-flash arrays). Every write appends to a segment, an index maps blocks to locations, and a cleaner reclaims segments.

For the chunk index, MetaStore shards and Raft logs, the usual choice is an embedded ordered key-value engine such as `redb` or `fjall` in the daemon's state directory. Such an engine opens files and flushes them through the kernel itself, outside the disk driver. It can't run on a drive that SPDK owns, the simulator can't crash it, and its flushes, plus one per Raft group if each keeps its own log, come on top of the chunk log's. Keeping chunk data inside such an engine as well would also give up control of flush ordering and direct I/O.

## Decision
Each disk holds one log, and every byte of it is written through the disk driver's actions. The disk is a sequence of fixed-size segments. Each group commit appends one record holding everything that arrived during the last round, then flushes once:

- chunk writes: a header block that describes each write (chunk, version, block range, CRC32C per block), followed by the data;
- Raft log entries and Raft state (term, vote) of every MetaStore shard replica on the disk;
- sorted runs and manifests of the indexes below.

Writes go into two streams: new copies and Raft entries in one; encoded shards, blocks moved by the cleaner, and sorted runs in the other.

The log is the source of truth. The per-disk chunk index, which maps extents of (chunk, version, block range) to a location while the checksums stay in the record headers, and the state of each MetaStore shard are log-structured merge (LSM) trees inside the log. An in-memory memtable holds what was applied since the last flush. A flush writes it to the log as an immutable sorted run, and a manifest record lists the live runs. Compaction merges runs into new ones. After a crash, the daemon loads the last manifest and replays the log from that point. If the chunk index is lost, a scan of the record headers rebuilds it. A punch, including a whole-chunk delete, is recorded in a record header like a write.

The Raft library must leave persistence to its caller: it hands over the entries to persist and is told when they are durable. That is what lets every shard on a disk share the disk's one flush.

A frozen version keeps its older index entries, so snapshots need no reference counts. A block is live while the live version or a pinned version still maps to it. A Raft entry is dead once the memtable that applied it has been flushed, and a sorted run once compaction has replaced it. The cleaner picks segments by cost-benefit, copies their live contents, grouping blocks by chunk, and discards (TRIMs) the freed segments.

Segment headers, record headers, sorted runs and manifests carry a format version. See [On-disk format](../design.md#on-disk-format) for upgrades.

## Consequences
- A durable write costs one sequential write and one flush, whatever its size and whether or not a snapshot pins the chunk. The chunk writes and metadata commits of one round share that flush, however many shards the disk holds.
- The disk driver touches every stored byte. SPDK can own a whole drive, metadata included, and the simulator can crash any write, flush, compaction or checkpoint.
- The daemon embeds no key-value engine, and a disk needs no filesystem.
- Snapshot block sharing, checksums and torn-write detection come with the format instead of being added to it.
- Writes are sequential and freed space is TRIMmed, which suits consumer SSDs and their own flash translation layer.
- The cleaner and the LSM tree (memtable, sorted runs, manifest, compaction, Bloom filters, block cache) are new code. The LSM tree arrives in two stages behind one interface: first memtables only, saved by periodic full snapshots into the log; then sorted runs and compaction, once an index outgrows memory.
- Raft libraries that do their own storage I/O are ruled out.
- A large chunk write can delay a metadata commit in the same round. Capping the bytes in one record bounds that delay.
- The cleaner costs CPU and write amplification. The amplification grows as a disk fills, so block volumes with random overwrites pay most near the pool's 90% write limit.
- A block chunk overwritten at random is fragmented on disk until the cleaner regroups it. Sequential reads of such a chunk become random 4K reads, which NVMe handles well and SATA SSDs and HDDs handle less well.
