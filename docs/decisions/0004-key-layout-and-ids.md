# 0004. Encode keys by inode, allocate inode IDs in slots, pack chunk IDs in 128 bits

Status: Accepted (2026-10-16)

## Context
The MetaStore is range-sharded, and its design goal is that almost every operation touches one shard. A `create` writes a directory entry under the parent and a record for the new inode, so the two keys must fall into one shard without a global transaction service. Other systems handle this in different ways:

- **One global transaction across shards:** 3FS on FoundationDB, JuiceFS on TiKV.
- **Inodes stored inside directory entries,** with a separate path from inode number back to the entry: CephFS, HopsFS.
- **Ordered writes plus garbage collection** instead of one atomic write: Tectonic.

Inode numbers are visible as `st_ino` and must survive [restores](../design.md#snapshots-and-clones). The [Samba spike](../design.md#appendix-samba-gateway-spike) found that a gateway reads `user.DOSATTRIB` and lists xattrs on every directory entry, and scans the directory on every create to find a case-insensitive match. The key layout has to leave room for both before it is frozen.

Chunk IDs are computed on the hot path, hashed into placement groups and stored as the per-disk index key, so their packing is part of the disk format.

## Decision
- **Encoding.** A key is a one-byte tag followed by fixed-width big-endian integers, so numeric order is byte order. Only the last component may have variable length (a file name, xattr name or S3 key), so keys need no escaping and a prefix scan is a byte-prefix scan.
- **Inode IDs** are 64 bits: a 24-bit slot and a 40-bit sequence number. Every key of an inode starts with `fs/{volume}/{inode}`, so a slot is one contiguous key range, and shards split only at slot boundaries.
  - A new file takes the next sequence number in its parent's slot, so the create is single-shard.
  - A new directory takes a free slot from the same shard's range, so `mkdir` is single-shard too, and later splits can separate subtrees. When the shard has no free slot left, the directory shares its parent's slot.
  - Sequence number 0 of each slot is reserved for the slot's own keys: its allocation counter and its orphan list.
  - IDs are never reused.
- **Xattrs** are separate keys next to the inode's attributes, so one shard request returns both. The daemon caches small xattrs with the attributes.
- **Case-folded names.** A key range for an index of case-folded names is reserved. It is maintained only when the volume setting `case_index` is on, which it is not by default.
- **Chunk IDs** are 128 bits: 32-bit volume, 64-bit inode, 32-bit chunk index. That allows files up to 16 PiB. Volume IDs are never reused.
- **Placement-group hash.** A PG is the top `log2(pg_count)` bits of XXH3-64 (seed 0) over the big-endian chunk ID. A PG is then one contiguous hash range, and doubling `pg_count` splits each PG in two without rewriting the per-disk index, which is keyed by pool, hash and chunk ID.

## Consequences
- `create`, `mkdir`, `unlink` and `readdirplus` touch one shard, and a split never cuts a directory away from its files.
- A cross-directory `rename` keeps the inode in its old slot, so locality decays slowly over time. Correctness does not depend on it.
- A directory and all its files live in one slot, so one huge, busy directory is bounded by one shard. The same holds for CephFS without directory fragmentation.
- A slot holds 2^40 IDs, about 3.5 years of creates at 10,000 per second. A full slot sends new files to a fresh slot.
- 64-bit `st_ino` values can overflow 32-bit programs built without large-file support.
- The hash function and the bit layout are part of the disk format. Changing either moves every chunk.
- An SMB gateway can answer per-entry xattr reads from cache. A volume shared over SMB needs `case_index` on, which costs one extra key in each create's transaction.
