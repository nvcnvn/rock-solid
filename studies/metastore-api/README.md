# MetaStore API: transaction ops, key encoding, routing and space grants

**Status:** read from source, with an arithmetic model ([`model.py`](model.py), output in [`results.txt`](results.txt)). Reviewed independently and revised; nothing here is adopted yet; [Where it would apply](#where-it-would-apply) lists the sections each proposal touches.

**Question:** four parts, all about phase 4 (the embedded MetaStore).

1. **The shard API.** The design gives `Txn(ops)` as "compare-and-set, put, delete, all-or-nothing" and `Scan(prefix, after, limit)` ([Metadata layer](../../docs/design.md#metadata-layer-metastore)). Which operations do the filesystem's transactions actually need: a compare that a whole key range is empty (flock), a range removal (purge lists, volume deletion), an increment (slot counters), a size limit, an order rule inside one `Txn`? What do `after` and `limit` mean, and how does a scan resume? How do CockroachDB, etcd and TiKV do it?
2. **Key encoding.** Which tag byte values and field widths (session IDs, request IDs, snapshot epochs), and do `internal/lsm`'s limits (`MaxKey` 1,024 bytes, `MaxValue` 1,000) fit S3 keys (up to 1,024 bytes) and extended attributes (up to 64 KiB)?
3. **Routing.** Is a `Txn` sent to a follower forwarded or redirected? How do internal transactions (control-plane changes, the purger) avoid applying twice across an election? Do `ctl/` keys use the same routing? What does phase 4, with one data shard, do with a `Txn` that would span shards?
4. **Space grants.** How does the control-plane leader divide a cap that binds between nodes, where does each disk's per-heartbeat write rate come from, and does a lapsed session's grant still count until its fence is acknowledged? Lustre's grants and CephFS's quotas are the closest precedents.

## Summary

A few terms first. A *shard* is one Raft group holding a key range; its *leader* is the replica that proposes changes, and every replica *applies* them in log order to its copy of the data. A *compare-and-set* (CAS) writes only if a key still holds the value the writer read. *Group commit* means the leader puts every request that arrived during one round into one log entry, so a round of 1 to 3 ms carries hundreds of transactions.

**1. The op set.** Every transaction the design names fits six operations: `Put`, `Delete` (succeeds on an absent key), `DeleteRange(start, end)`, `Check(key, value or absent)`, `CheckEmpty(start, end)` and `Update(key, offset, Set | Add | Bump, n)`, which changes one 8-byte field of a fixed-layout value, `Bump` storing max(n, field + 1) so a time always moves forward. All checks are evaluated against the state before the transaction, then the writes apply in list order; a failed check returns its index and the key's current value. The one op that is not obvious is `Update`, and it is forced by concurrent writers to shared keys: every `create` in a directory must update the parent's `mtime` and `ctime` (POSIX, checked by `pjdfstest`), and creators whose transactions land in one round all built them from the same earlier read, so with compare-and-set only one of them commits per round. A single creator such as `tar x` is unaffected, but 64 parallel creators in one directory get 333 to 1,667 creates per second against tens of thousands with an update that never fails, bounded then by the shard's loop ([results](results.txt), part 1). FoundationDB's atomic `add` and `max`, including its "add at an offset in the value" technique, are the precedent; CockroachDB's `Increment` is the single-key form. `CheckEmpty` is forced by `rmdir` and rename over a directory (`ENOTEMPTY`), which the design does not list yet, by `flock`, and by the purger; it is etcd's `Compare` over `range_end`. A replay record stores a result only when it is final (a check the caller marks as its errno), so a daemon can retry a lost race under the same request ID. Every internal transaction carries a check on everything it read. Inode numbers are reserved in batches by a transaction of their own, prefetched as JuiceFS does. `Scan` takes a half-open range `[start, end)`, a key limit of at least 1 and a byte budget, and returns `more` when it stopped early; a caller resumes from the last key plus a zero byte. A `Txn` over 256 KiB is refused before proposing, never split.

**2. Key encoding.** One-byte tags with the inode's own keys first (`a` 0x01, `x` 0x02) so attributes and extended attributes are one range, slot keys from 0x10, and 64-bit session IDs, request IDs, snapshot and table epochs, plus a 32-bit mount ID in replay keys. CockroachDB escapes every variable-length key part because its SQL keys have several; rock-solid's rule that only the last part varies makes that unnecessary. The limits do not fit: an S3 index key is 1,029 bytes, over `MaxKey`, and a 64 KiB extended attribute value or a 4,095-byte symlink target (which `pjdfstest` creates) is over `MaxValue`. The page tree bounds key plus value at 2,033 bytes, so `MaxKey` can rise to 1,032 with `MaxValue` unchanged; range-removal bounds, which the tree stores two to a fragment, stay at most 1,000 bytes each; and larger values are split over continuation keys, as FoundationDB advises for values over 10 KB.

**3. Routing.** A follower answers "not leader" with the leader's node and term and never forwards (etcd-raft's `DisableProposalForwarding`), as CockroachDB and TiKV redirect; the inode owner is the leader, so a forwarded request would also bypass the owner's open registry. During an election a request fails retryably. Client requests are made safe to resend by their replay records; internal transactions by a `Check` or `CheckEmpty` on every key or range they read, since an unguarded delete, though idempotent, can remove an entry written after its first attempt. `ctl/` keys use the same `Txn` and `Scan`, routed by the same shard map with the `ctl/` range pinned to the control-plane group, which never splits, as CockroachDB's first-level addressing range never splits. In phase 4 a `Txn` whose keys fall in two groups returns an error naming them before anything is proposed.

**4. Space grants.** Divide a binding cap max-min fairly (water-filling), with what each node holds buffered as a floor its grant never drops below. A lapsed session's grant counts until its fence is acknowledged, which is when Lustre discards an evicted client's grant, "once we're sure no more interaction with the client is possible". Working this through found a gap in the design: a daemon may reuse its grant for its whole session lease (10 s) after its last answered heartbeat, so during a control-plane failover, or while it is cut off from the leader but not from storage, it can land flushes the leader never counted, 11 GB on each disk of a 3-node `rep3` pool against a 9.9 GB gap if the disk was near `write_limit` ([results](results.txt), part 3). The fix also answers where each disk's per-heartbeat write rate comes from: nowhere. A daemon may reuse at most two grants' worth of flushes between answered heartbeats, sending its heartbeat early once it has used half of that, then writes through; heartbeats carry a cumulative flush count and the grant the node holds, and a grant above the fair-share level is not reused. The leader keeps three times the grants, plus flushes not yet in fill reports, within every disk's gap, and needs no disk rate. It costs a few extra heartbeats per second from streaming nodes, a lower cap near full than buffering without a margin, and write-through (about 1 ms per write instead of 5 µs) for heavy writers during a control-plane failover.

**Design bugs found.** Besides the grant gap: two purge entries for one inode overwrite each other, so a truncate followed by an unlink leaks chunks; and the purger's range removal of the consumed part of the purge list, which the design prescribes, deletes entries added inside that range meanwhile, which leaks chunks too.

## What the design says now

- **API** ([Metadata layer](../../docs/design.md#metadata-layer-metastore)): `Txn(ops []Op) error // compare-and-set, put, delete, all-or-nothing` and `Scan(prefix, after []byte, limit int) ([]KV, error)`; routing sorts and deduplicates ops, takes one Raft round for one shard, and a two-phase commit otherwise. Group commit drains up to 1,024 requests into one entry per round.
- **Key layout:** each `word/` is a one-byte tag, integers fixed-width big-endian, only the last component variable. Inode IDs are a 24-bit slot over a 40-bit sequence. Slot keys sit at sequence 0: `n` (next sequence number), `u` (bytes used), `o/{inode}` (orphans), `p/{inode}` (purge list), `r/{session}/{request}` (replay records). Locks are `l/{session}`. `ctl/...` lives in "one dedicated control-plane Raft group, not sharded".
- **Uses that set requirements:** `create` compares the entry absent and the slot counter `n`; `u` is "updated in the transaction that changes a file's size, so no single key is hot" ([Volume size and capacity](../../docs/design.md#volume-size-and-capacity)); O_APPEND from several nodes is "a MetaStore compare-and-set on the size"; `mtime` is the later of the writer's time and the stored `mtime` plus 1 ns; a purge list consumed in key order drops its consumed prefix "with one range removal"; volume deletion removes `fs/{volume}/` "in one range removal"; a mutating request stores its result in a replay record "in the same transaction", keyed by session and request though the request carries a mount ID too.
- **Not mentioned:** `rmdir`, rename over a directory, `link`, symlinks, a directory's parent pointer (which a cross-directory rename's loop check needs), the parent directory's `mtime`, `ctime` and link count, a free-slot allocator for `mkdir`, what happens when two purge entries for one inode meet, and when replay records of an ended session go.
- **Space grants** ([Client design](../../docs/design.md#client-design)): the heartbeat reports per pool what a node holds buffered and flushed and asks for more; the reply sets the grant, at most `writeback_buffer`; "the daemon reuses its grant as each flush completes"; "a grant lasts as long as the renewal that carried it", which is `session_lease`. The leader keeps the pool's grants within half of available and within each disk's gap above `write_limit` "less what that disk can write in one heartbeat interval".
- **`internal/lsm`:** `MaxKey = 1024`, `MaxValue = 1000` (`internal/lsm/tree.go:16-19`), checked in `fits` (`tree.go:116-119`). A leaf entry is a 2-byte key length, a 2-byte value length, the key, a kind byte and the value (`internal/lsm/memtable.go:51-60`), with a 2-byte slot, in 4,096-byte pages with a 16-byte header (`internal/lsm/page.go:15-17`); a split always fits "since entries are at most half a page" (`memtable.go:159-162`).
- **Test scenarios** list 27 Open lines across the sections this study covers; [the last section](#the-other-open-lines) answers each.

## What other systems do

### CockroachDB (`d30c905`)

- **Conditional put.** `ConditionalPut` writes if the existing value equals `exp_bytes`; an empty `exp_bytes` means the key must not exist; otherwise it "returns a ConditionFailedError containing the actual value of the key" (`pkg/kv/kvpb/api.proto:321-338`). `allow_if_does_not_exist` also accepts an absent key (`api.proto:361`). `InitPut` (write if absent or equal) was removed from clients in January 2025 and from servers in May 2025 (commits `46787b33`, `71f21013`), leaving CPut as the one conditional write.
- **Increment.** "Increments the value for key, and returns the new value"; an absent key counts as 0 (`api.proto:402-417`); overflow fails the request with `IntegerOverflowError` (`pkg/storage/mvcc.go:2863-2870`).
- **Range removal.** `DeleteRange` over `[start, end)`, optionally as a constant-time range tombstone (`api.proto:453-474`). `IsSpanEmpty` asks whether a span holds any key (`api.proto:270-287`).
- **Scan.** `[start, end)` "and the maximum number of results (unbounded if zero)" (`api.proto:623-624`); the batch header's `max_span_request_keys` and `target_bytes` bound a reply by keys and bytes (`api.proto:2829`, `2850`), and a partial reply carries a `resume_span` naming what is left (`api.proto:136-149`).
- **Order inside a batch.** Requests sent to one range "are executed in-full according to their order in the batch" (`api.proto:2783-2786`; the loop in `pkg/kv/kvserver/replica_evaluate.go:295`), so a later request sees an earlier one's write.
- **Size.** A command over `kv.raft.command.max_size`, 64 MiB by default, is refused before proposing, "command is too large" (`pkg/kv/kvserver/kvserverbase/base.go:449-466`; `pkg/kv/kvserver/replica_raft.go:292`).
- **Where it evaluates.** The leaseholder evaluates a request and proposes the resulting writes, so replicas apply plain puts and need not reproduce the evaluation; the motivation was upgrades, where code downstream of Raft "needs to produce identical output even during migrations" (`docs/RFCS/20160420_proposer_evaluated_kv.md:21-34`).
- **Routing.** A replica that is not the leaseholder answers `NotLeaseHolderError` with the lease if known (`pkg/kv/kvpb/errors.proto:37-48`); the client's DistSender updates its range cache and retries at the leaseholder (`pkg/kv/kvclient/kvcoord/dist_sender.go:3142-3166`). Reproposals within one lease cannot apply twice, because a command fails if the range's lease applied index has reached its `max_lease_index` (`pkg/kv/kvserver/kvserverpb/proposer_kv.proto:335-350`); beyond that, a result the client never received is an `AmbiguousResultError` (`errors.proto:428-430`) the caller must resolve.
- **Keys.** One-byte prefixes for the system key space: local `0x01`, meta1 `0x02`, meta2 `0x03`, system `0x04` (`pkg/roachpb/data.go:49-51`; `pkg/keys/constants.go:20-25`). Tables follow under a varint table ID (`pkg/keys/sql.go:195-198`), whose one-byte form covers 0 to 109 (`pkg/util/encoding/encoding.go:155-160`, `417-420`). Every byte string in a key is escaped, `0x00` as `0x00 0xff`, and terminated with `0x00 0x01` (`encoding.go:616-621`, `640-645`), because a SQL key holds several variable-length columns in a row and each must sort on its own. Control data is ordinary keys in the same key space under the same API, and the first-level addressing range may not split: "meta1 addressing records cannot be split" (`pkg/kv/kvserver/addressing.go:81-85`).

### etcd (v3.6.9, `85651fa`) and etcd-raft (v3.6.0)

- **Txn.** A list of compares, then a list of success ops "processed in order" or failure ops (`api/etcdserverpb/rpc.proto:659-683`), after Google's PaxosDB MultiOp (`rpc.proto:643-658`). Compares are all evaluated before any op (`server/etcdserver/txn/txn.go:561-568`).
- **Range compares.** A `Compare` may cover `[key, range_end)` (`rpc.proto:638-640`). Over an empty range it compares a zero record, so `create_revision == 0` is true there, and a `VALUE` compare fails (`txn.go:572-595`): "no key exists in this range" is one compare.
- **Duplicates.** A put and a put, or a put and a delete range covering it, on one key in one `Txn` are rejected with `ErrGRPCDuplicateKey` (`server/etcdserver/api/v3rpc/key.go:189-265`). At most 128 ops (`server/embed/config.go:64`, checked at `key.go:155-166`), requests at most 1.5 MiB (`config.go:67`).
- **Range.** `[key, range_end)`; "when limit is set to 0, it is treated as no limit" (`rpc.proto:448-458`), and `more` says the range has more keys (`rpc.proto:508-509`).
- **Forwarding.** etcd-raft forwards a follower's proposal to the leader unless `DisableProposalForwarding` is set (`raft.go:248-256`, `1713-1723` at v3.6.0); with no leader it drops it with `ErrProposalDropped` (`raft.go:84-86`, `1716-1718`). The proposer of a forwarded proposal learns its result only by applying it.

### TiKV (`134cdb2`) and kvproto (`f8ad079`)

- **Mutations.** Prewrite ops include `Insert`, a put that requires the key to be absent, and `CheckNotExists`, the same check without a write (`proto/kvrpcpb.proto:1569-1583`).
- **Raw API.** `RawCAS` compares with a value or "not exist" and returns "the previous value regardless of whether the comparison is succeed" (`kvrpcpb.proto:1719-1738`); `RawScan` takes `start_key`, `end_key` and `limit` (`kvrpcpb.proto:696-707`); `RawDeleteRange` takes a half-open range (`kvrpcpb.proto:713-718`).
- **Routing and size.** A region error `NotLeader` names the region and its leader (`proto/errorpb.proto:19-24`), for the client to retry there (client-go not read). Raft entries are limited to 8 MiB (`components/raftstore/src/store/config.rs:546`).

### FoundationDB (`487620d`)

- **Atomic ops.** An atomic op reads a key, transforms it and writes it in one command, and "its use within a transaction will not cause the transaction to conflict", which "makes atomic operations ideal for operating on keys that are frequently modified" (`documentation/sphinx/source/api-common.rst.inc:87-97`). `add` works on a field "at a known offset in the value by prepending the appropriate number of zero bytes" and truncates on overflow (`api-common.rst.inc:99-103`); `max` keeps the larger value (`api-common.rst.inc:117-121`).
- **Limits.** Keys at most 10,000 bytes, values 100,000 (`known-limitations.rst:41`), transactions 10,000,000 bytes and a warning above 1 MB (`known-limitations.rst:28-30`); values above 10 KB, or above 1 KB when read in part, should be split across keys (`largeval.rst:16-27`).

### JuiceFS (`29c60bb`)

- Inode numbers come in batches: a client adds 1,024 to the `nextInode` counter in one transaction and hands them out from memory (`pkg/meta/base.go:51`, `1569-1620`). Used-space and inode counters are summed in client memory and added to their keys periodically, not in each operation's transaction (`pkg/meta/tkv.go:630-648`). The code gives no reason; the likely one, an inference, is that its increment is a read and a write inside an optimistic TiKV transaction (`tkv.go:1176-1184`) and so conflicts like any other. The next inode batch is fetched in the background when 10% of the current one is left (`base.go:66`, `1595-1601`).

### Lustre (`ee61b3f`)

- **The ledger sits with the data.** Each storage target (OST) keeps every client's grant (`ted_grant`) and the total (`tgd_tot_granted`); space left to grant is the cached free space, less a reserved percentage, less everything granted (`lustre/target/tgt_grant.c:472-533`). Each write RPC is checked against the client's grant at the OST that stores it (`tgt_grant.c:1308-1371`), so the target never learns of a write late.
- **How it divides.** A client gets some of what it asked for: per allocation, never more than one eighth of the space left and never more than two full RPCs' worth (`chunk`), with its total held to what it wants plus a chunk, and nothing while it already holds what it wants or more than its server-side grant plus a chunk ("either it just has lots of RPCs in flight, or it was evicted and its grants will soon be used up") (`tgt_grant.c:968-1013`). It is first come, not fair: whoever asks while space is plentiful keeps it.
- **Giving back.** Clients return grant themselves, a quarter of what they hold on a periodic shrink or down to one RPC's worth (`lustre/osc/osc_request.c:797-828`); the target accepts a shrink only once grants pass 25% of the space left (`tgt_grant.c:648-650`). A client with no grant falls back to synchronous writes (`lustre/osc/osc_cache.c:1850-1855`), as rock-solid's daemon writes through.
- **Evicted clients.** The target discards an export's grant when it disconnects and when the export is destroyed, "once we're sure no more interaction with the client is possible" (`lustre/ofd/ofd_obd.c:422`, `503-508`; `tgt_grant.c:1160-1210`).

### CephFS (`b7e6787`)

- Quotas are enforced by the client: a write past `max_bytes` fails with `EDQUOT` (`src/client/Client.cc:12846-12851`, `18385-18393`). A client whose unreported growth exceeds one sixteenth of the space left under the quota reports its size at once instead of on the next timer (`Client.cc:18395-18413`, `12389-12391`), a fraction-of-remaining rule like Lustre's. "Quotas are imprecise": writers stop "within tens of seconds", and the overshoot "depends primarily on the amount of time, not the amount of data" (`doc/cephfs/quota.rst:99-105`). rock-solid cannot accept that for grants, since a granted flush must never be refused; the overshoot by time is what the design's h_d term bounds and the reuse budget proposed here replaces.

### Limits from the interfaces

- **Linux:** `NAME_MAX` 255, `PATH_MAX` 4,096 including the terminating zero, `XATTR_NAME_MAX` 255, `XATTR_SIZE_MAX` 65,536 (`include/uapi/linux/limits.h:12-17`, v6.18). ext4 without its `ea_inode` feature fails with `ENOSPC` once an inode's attributes no longer fit its in-inode space and one block (`fs/ext4/xattr.c:1676-1686`).
- **`pjdfstest`:** creates a symlink whose target is `PATH_MAX` − 1 bytes and expects success (`tests/symlink/03.t:13-17`, `tests/misc.sh:145-151`), and checks that `mkdir` and `open(O_CREAT)` move the parent's `ctime` and `mtime` (`tests/mkdir/00.t:53-69`, `tests/open/00.t:56-`).
- **S3:** a key is "a sequence of Unicode characters whose UTF-8 encoding is at most 1,024 bytes long" (AWS developer guide, `UsingMetadata.md:11`); Ceph's RGW enforces the same (`src/rgw/rgw_rest.h:574`, `rgw_rest.cc:1815-1824`).
- **Samba:** `vfs_streams_xattr` stores alternate data streams, such as macOS resource forks, in xattrs, so "most file systems have severe limitations" for it, Linux's being 64 KiB per xattr (`docs-xml/manpages/vfs_streams_xattr.8.xml:45-48`, `77-87`).
- **FUSE:** request IDs come from a counter per connection (`fiq->reqctr` in `fs/fuse/dev.c:303-306`; one `fuse_iqueue` per `fuse_conn`, `fs/fuse/fuse_i.h:671-672`), so two mounts number their requests independently.

## Findings

1. **Compare-and-set on a shared key lets one of its concurrent writers commit per round.** The shard applies a round's transactions in log order, but each was built from a read taken before the round. If two compare the same key, the first applied changes it and the second fails, retries with the value its failure returned, and lands in a later round. The design names two keys that many transactions share, the slot counter `n` (every `create` in a slot) and the byte counter `u` (every size change in a slot), and it misses a third: the parent directory's attributes, since every `create`, `mkdir`, `unlink` and `rename` in a directory moves its `mtime` and `ctime`, and `mkdir` and `rmdir` its link count. A single creator, such as `tar x`, which waits for each create before the next, is unaffected: one create per commit either way, 6 to 30 s for 10,000 files. Concurrent creators in one directory (many pods writing into one upload directory, `make -j`, a parallel copy) are what it limits: with compare-and-set the directory still takes one per commit, 333 to 1,667 per second, where an update that cannot fail lets 64 creators in flight reach tens of thousands per second, until the shard's loop, about 60,000 proposals per second at 90% busy, is the bound (results, part 1). The design already accepts this limit for `n` ("Two racing `create`s" expects a retry). Evaluating the compare at apply time does not help by itself: its expected value still comes from the client's earlier read. Only a read-modify-write done at apply time avoids it. FoundationDB's atomic ops exist for this; JuiceFS avoids it by reserving inode batches and deferring counters.
2. **A range-emptiness check is needed by more than flock.** `rmdir` must fail with `ENOTEMPTY` if the directory has an entry, and so must a rename over a directory; neither is in the design or the schema scenarios. An exclusive flock needs "no other session's key under `l/`". Shared locks need "no exclusive holder", which a range check can express only if the mode is part of the key.
3. **Two purge entries for one inode collide.** The purge list is keyed `p/{inode}`. A truncate leaves `p/{i}` → from the new size to the old `alloc_end` and lowers `alloc_end`; an unlink before the purger runs writes `p/{i}` → `0 .. alloc_end`, now the lower value, and the truncated chunks above it are never deleted. Two truncates in a row lose the first range the same way. The design's rule that an extension first finishes a pending purge covers extensions only.
4. **The purger's removals lose entries added meanwhile.** The purge list is keyed by inode, so it is not a queue: a truncate, unlink, rename-over or an orphan's last close can add `p/{i}` for any inode of the slot at any time, below the purger's position too, or merge a range into a pending entry (the fix for finding 3). If the purger scans `p/`, finds K₁ … Kₘ, purges them and removes `[K₁, Kₘ]` with one range removal, as "Lists consumed in key order" in the design prescribes, any entry added inside that range between the scan and the removal goes unpurged, and its chunks leak. A blind `Delete(p/{i})` at the end of one entry's purge loses a range merged into it meanwhile. Being idempotent does not save either: a blind delete retried after an election can remove an entry written after the first attempt applied. The Purge model, which covers one file, cannot find this.
5. **Some values and keys do not fit the LSM tree.** A 64 KiB xattr (Samba streams, macOS resource forks), a 4,095-byte symlink target (`pjdfstest`), and a POSIX ACL past 124 entries all exceed `MaxValue`; the S3 index key `T(s3)`, volume, 1,024-byte key is 1,029 bytes, over `MaxKey` (results, part 5). The tree's real bound is on the pair: an entry plus its slot must fit half of a 4,080-byte page, so key plus value at most 2,033 bytes, of which 1,024 + 1,000 uses 2,024. A range-removal fragment holds two keys, a start and an end, in the second page tree, so it is bounded the same way: two keys of 1,024 bytes do not fit, and the design's statement that "two entries always fit a page" (`docs/design.md:229`) holds only for point entries.
6. **The replay key needs the mount ID.** FUSE numbers requests per connection, so two mounts of one volume by one session would collide on `r/{session}/{request}`. CSI mounts a volume once per node today, which hides it. Records are also dropped only in the slot a later request of the same mount touches, so records in other slots, and every record of an ended session, stay forever.
7. **A grant can be reused, unseen, for a whole session lease.** The leader learns of a node's flushes from that node's heartbeats, and its gap cap allows one heartbeat interval of writes it has not heard of (h_d). But a daemon reuses its grant for as long as the renewal lasts, `session_lease` (10 s), and a granted flush needs only its grant at the primary. If a node's heartbeats stop reaching the leader while its writes still reach storage (a control-plane failover, 3 to 10 s, or a partition between it and the leader only), it keeps buffering and flushing past `write_limit`. On a 3-node `rep3` pool of G4's disks every disk holds every byte, so a client streaming at 1.1 GB/s lands 11 GB on each disk in 10 s, on top of 0.81 GB of grants, against a gap of 9.87 GB: if the disk was within about 1.9 GB of `write_limit` when the outage began, the log reserve is overrun and a granted flush is refused, which the design promises never happens (results, part 3). A seed finds it only if it checks that no granted flush is refused, as the Control-plane group invariant does; the MetaStore invariants row checks only Σ + h_d ≤ g_d, which the leader satisfies while the bug happens.
8. **"Whether F lowers A directly, or each disk's room by its share" is the same thing.** Available is min over disks of room ÷ share, so lowering every disk's room by F × share lowers available by exactly F.

## Proposed answer

### 1. The shard API

```go
type MetaStore interface {
	Txn(t Txn) (Result, error)
	Scan(start, end []byte, limit int) (kvs []KV, more bool, err error)
}

type Txn struct {
	Ops    []Op
	Replay []byte // the replay record's key, or nil for internal transactions
	Oldest uint64 // the mount's oldest outstanding request; records below it are dropped
}

// Op kinds, the whole set:
//   Put(key, value)                       write
//   Delete(key)                           remove; succeeds if the key is absent
//   DeleteRange(start, end)               remove every key in [start, end)
//   Update(key, off, Set|Add|Bump, n)     change the 8-byte big-endian field at off; an absent key or
//                                         short value reads as zeros; Add wraps; Bump stores
//                                         max(n, field + 1), so the field always moves forward
//   Check(key, value | absent, final)     the key holds exactly value, or is absent; final marks a
//                                         failure the caller maps to an errno (EEXIST, ENOENT)
//   CheckEmpty(start, end, final)         no key in [start, end)

type Result struct {
	FailedCheck int      // index of the first failed check, or -1
	Actual      []byte   // that check's key's current value (nil if absent or CheckEmpty)
	Updated     []uint64 // each Update's new field value, in op order
}
```

A point read (`getattr`, `lookup`) is `Scan(k, k + 0x00, 1)`; a separate `Get` is only a shortcut.

**Rules.**

- **Evaluation.** The shard evaluates a `Txn` when it applies the log entry, on every replica, in log order: first every `Check` and `CheckEmpty` against the state before the `Txn`, then, if all pass, every write in list order, each seeing the ones before it. So `put a=1, delete a` leaves `a` absent, two updates of one key's fields compose, and nothing needs undoing, since no write can fail once the checks pass (`Add` wraps, as FoundationDB's does; the schema keeps its counters far from 2^63). Apply-time evaluation is what lets `Update` never conflict; it costs each replica the checks' reads, about 1 µs each from the memtable, and makes the ops' meaning disk format, since every release must apply an old entry the same way (CockroachDB moved evaluation to the proposer for that reason): a new op kind needs a raised format level.
- **Replay.** If `Replay` is set and that key exists, the shard returns the stored `Result` and applies nothing. Otherwise it applies the `Txn` and stores its `Result` under `Replay` only if the result is final for the FUSE request: every check passed, or the failed check is marked `final`. A failed check not marked final, a lost compare-and-set race such as `close`'s `Check(a)`, stores nothing, so the daemon re-reads and retries under the same replay key. The shard also removes the mount's records below `Oldest` with one `DeleteRange` in that slot, and when a session leaves the session table, each shard's leader removes `r/{session}/` in every slot in the background.
- **Limits, checked before proposing, as errors, never panics:** encoded size at most `max_txn_bytes` (256 KiB, a quarter of `max_group_commit_bytes`, three times the largest named transaction); keys at most `MaxKey`; values at most `MaxValue`; `DeleteRange` and `CheckEmpty` bounds at most 1,000 bytes each, so a fragment's two bounds fit half a page (every named range bound is under 40 bytes); at least one op. A `Txn` is never split, since splitting breaks all-or-nothing.
- **Scan.** `[start, end)`, at most `limit` keys (`limit` < 1 is an error, since an unbounded reply would allocate without bound) and at most `max_scan_bytes` (256 KiB) of keys and values, from one point-in-time view. `more` reports that it stopped at a limit; the caller resumes with `start` = last key + `0x00`, so `Scan` bounds may be `MaxKey` + 1 bytes, which is safe because they are never stored. A prefix scan is `Scan(p, PrefixEnd(p), n)`, and the design's exclusive `after` is `start = after + 0x00`, which is also S3's `start-after` and a `readdir` cookie. A range spanning shards is refused by routing.
- **Internal transactions are guarded.** Every `Txn` without a replay key carries a `Check` or `CheckEmpty` on each key or range it read, so a repeat after an election, or a write that raced it, fails instead of doing harm; an `Add` whose repetition would matter (`u`) rides only in a `Txn` with a replay key or with a `Check` a repeat would fail. Being idempotent is not enough (finding 4). The exceptions remove keys nobody can write again: replay records below a mount's oldest outstanding request, and a session's records once it has left the session table.

**Which ops each named transaction uses.** Field names (`a.mtime`, `a.nlink`) are 8-byte fields at fixed offsets of the attribute value, which the schema's hand-written encoding fixes. The rule that keeps contention away: keys many clients write (a directory's attributes, slot counters) change only through `Update`; a file's own attributes, which one node usually writes, change by `Check`-and-`Put`, so the size, `alloc_end` and the `u` delta stay consistent. "The parent's times" below is `Update(parent a.mtime, Bump, t)` and `Update(parent a.ctime, Bump, t)`, which keeps the design's rule that `mtime` moves forward by at least 1 ns on every change.

| Transaction | Ops |
| --- | --- |
| Inode reservation | Its own `Txn`, `Update(n, Add, 64)`, before the daemon's first create in a slot; the daemon hands out the 64 numbers from memory and reserves the next batch in the background when 10% are left, as JuiceFS prefetches its batches of 1,024 (`pkg/meta/base.go:1595-1620`). Cost: one extra round before a node's first create in a slot, hidden afterwards. A repeat after an election only skips numbers, which are never reused anyway |
| Slot reservation | `Update(the shard's next-slot key, Add, 16)` in its own `Txn`, reserved and prefetched the same way; the key is a slot key (`0x15`) at sequence 0 of the shard's lowest slot |
| `create` | `Check(d/name absent, final)`, `Put(d/name → inode)`, `Put(child a)`, the parent's times; with `case_index`, `Put(c/folded)` |
| `mkdir` | As `create`, plus `Update(parent a.nlink, Add, 1)`. In a fresh slot `s` from the daemon's reserved slots: `Put(s:1 a)`, `Put(s:1 P → parent)` and `Put(s:0 n = 66)`, so the directory takes sequence 1, as the root is inode 1, and the creating daemon owns sequences 2 … 65: "mkdir, then create in it" needs no reservation round |
| `link` | `Check(d/name absent, final)`, `Put(d/name)`, `Check(child a)` + `Put(child a)` with `nlink` + 1 and `ctime`, the parent's times |
| `symlink` | As `create`, plus `Put(child 0x03)` with the target, split into values of up to 1,000 bytes past that |
| `unlink`, last link, not open | `Check(d/name = inode, final)`, `Delete(d/name)`, `DeleteRange(child's keys)` (attributes, xattrs, locks, target), the purge entry merged (row below), `Update(u, Add, −size)`, the parent's times |
| `unlink`, other links | `Check(d/name, final)`, `Delete(d/name)`, `Check(child a)` + `Put(child a)` with `nlink` − 1 and `ctime`, the parent's times |
| `unlink`, still open | `Check(d/name, final)`, `Delete(d/name)`, `Put(o/inode)`, the child's `nlink` as above, the parent's times |
| `rmdir` | `Check(d/name = inode, final)`, `CheckEmpty(child's d/, final)`, `Delete(d/name)`, `DeleteRange(child's keys)`, `Update(parent a.nlink, Add, −1)`, the parent's times |
| `rename` in one directory | `Check(src entry, final)`, `Check(dst entry absent, or = what was read)`, `Delete(src)`, `Put(dst)`; over a directory also `CheckEmpty(its d/, final)`; a replaced inode as in `unlink` |
| `rename` across directories (single-shard in phase 4) | As above, plus both parents' times; for a directory, `Update(nlink, Add, ±1)` on both parents, `Put(its P → new parent)`, and the loop check: `Check(P = what was read)` on each ancestor of the new parent up to the root, so a directory never moves under itself. The design's key layout has no parent pointer; this needs one, `P` (`0x07`), in its own key so that creates in an ancestor do not fail the check |
| `setattr` on a directory (`chmod`, `utimensat`) | `Update(field, Set, v)` and `Update(a.ctime, Bump, now)`; never `Check` on a directory's attributes, which every create in it changes. `Set` has this one use; it is cheap once `Update` exists, and without it a `chmod` would retry behind a stream of creates |
| `alloc_end` raise, `close`/`fsync`, size change, O_APPEND | `Check(a = what was read)`, `Put(a)` with size, `alloc_end` and `mtime` = max(t, stored + 1 ns); `Update(u, Add, new size − old size)`. A failed `Check(a)` is not final: the daemon re-reads and retries |
| Truncate | `Check(a)`, `Put(a)`, `Update(u, Add, −delta)`, the purge entry merged |
| Purge entry (truncate, unlink, rename-over, orphan's last close) | `Check(p/inode absent, or = what was read)` + `Put(p/inode)` with the union of the pending range and the new one. Every pending range lies past the current size, since an extension first finishes it, so the union never covers live data (finding 3) |
| Purger | Punches in the ChunkStore, which are idempotent; then `Check(p/inode = what it read)` with `Delete(p/inode)`, or with a `Put` of its progress. A consumed run of entries K₁ … Kₘ goes with one `DeleteRange(K₁, Kₘ + 0x00)` carrying `Check(Kⱼ = vⱼ)` for each and `CheckEmpty` over each gap between them (finding 4), about 1 µs per check |
| flock `LOCK_EX` | The owner scans `l/` and gets keys K₁ … Kₘ. For each Kᵢ of a session gone from the session table, `Check(Kᵢ = v)`, `Delete(Kᵢ)`; any live holder fails the request. Then `CheckEmpty` over each gap between the Kᵢ, and `Put(l/x/{session})` |
| flock `LOCK_SH` | Locks keyed `l/{mode}/{session}`: as above over `l/x/` only, then `Put(l/s/{session})` |
| xattr set | `Put(x/name)`; values over 1,000 bytes split into pieces (below), in one `Txn` |
| Volume deletion | `Check(the volume's record says deleting)`, `DeleteRange(fs/{volume}/, PrefixEnd)` |
| S3 `PUT` | `Check(s3/v/key absent, or = version, final)`, `Put(s3/v/key)`; object IDs from the shard's reserved slot, reserved as inodes are |
| `ctl/`: a table, a session-table version | `Check(current epoch or version = e)`, `Put(e + 1)` and its contents |
| `ctl/`: a session starts | `Check(counter = k)`, `Put(counter = k + 1)`, `Put(session/k)`; sessions start rarely, so compare-and-set is enough |

**Costs.** Six op kinds, against three in the design. `Update` needs the attribute value to have a fixed layout (it already has a hand-written encoding). `CheckEmpty` is one seek plus a step over each removal still in the range: after `rm -rf` of a large directory, `rmdir` steps over one removal per deleted entry until a merge drops them, the pile-up the LSM deletions study measured at 10,820. Apply-time evaluation freezes op semantics as format. A final failure writes its replay record, so a failed client request is a memtable write as well as the log entry it already is. Inode and slot reservations add one round before a node's first create in a slot. Cross-directory rename of a directory reads every ancestor.

**Alternatives rejected.**

- *Compare-and-set only* (the design today): one create per directory per round under concurrent creators (finding 1).
- *Reservation inside the first create's `Txn`*: impossible, since the create's keys contain the new inode number and no op can use an earlier op's result.
- *Storing every failure in the replay record*: a daemon's own retry after a lost compare-and-set race would get the stale failure back forever.
- *etcd's compare/then/else.* The else branch's uses here are reading values on failure, which `Result.Actual` covers, and storing a final failure, which the replay rule covers.
- *Duplicate writes rejected* (etcd): two field updates of one key are common (`mtime` and `ctime`, size and `alloc_end`), and in-order application is no harder to apply.
- *Proposer-evaluated writes* (CockroachDB): the leader would compute each `Txn`'s writes before proposing, so replicas need not repeat its reads, but a write that depends on a key's value must then wait until the write before it on that key has applied, or the leader must track the effect of every proposal in flight; either is more than an apply-time `Update`.
- *`Max` for times*: a create from a node whose clock is behind would leave the directory's `mtime` unchanged, and a watcher would miss it; `Bump` always moves it.
- *Deferred counters* (JuiceFS): `statfs` and the PVC quota would drift by whatever a crashed daemon had not yet added.
- *Two transactions for a dead holder's lock* (clean up, then grant): one more round after every fence; the per-gap checks do it in one.

### 2. Key encoding

| Level | Tags |
| --- | --- |
| First byte | `0x01` ctl, `0x02` volume, `0x03` fs, `0x04` s3 |
| `ctl/` | `0x01` pool, `0x02` placement table, `0x03` membership, `0x04` session, `0x05` format level |
| `volume/{id}` | `0x01` attach, `0x02` snap |
| An inode's keys (sequence ≥ 1) | `0x01` a, `0x02` x, `0x03` symlink target, `0x04` l, `0x05` d, `0x06` c, `0x07` P (a directory's parent) |
| A slot's keys (sequence 0) | `0x10` n, `0x11` u, `0x12` o, `0x13` p, `0x14` r, `0x15` next free slot (at the shard's lowest slot only) |
| Pieces of a split value | The tag with its high bit set (`0x82` for x, `0x83` for a symlink target), then a 1-byte piece number, then the last component |

- **Widths, all big-endian:** volume 32 bits, inode 64 (24-bit slot, 40-bit sequence), session 64, mount 32, request 64 (FUSE's `unique` is 64 bits), table and snapshot epochs 64, lock mode 8. So `fs/5/2:1/a` is `03 00000005 0000020000000001 01`, and a replay key is `03 vvvvvvvv ssssss0000000000 14 <session 8> <mount 4> <request 8>`, 34 bytes.
- **Why these:** `a` and `x` adjacent make attributes plus xattrs one range; the inode's tags sort before the slot's, which sit at a sequence no inode uses, so a decoder can check that a slot tag appears only at sequence 0; gaps leave room; piece tags sort after everything an inode scan reads. A snapshot's metadata copy at `fs/{snapshot}/` puts a snapshot ID where a volume ID goes, so snapshot IDs come from the volume ID counter.
- **Against CockroachDB:** varint prefixes would save about 8 bytes per key, which the run blocks' prefix compression mostly recovers already; fixed widths keep decoding trivial and make slot boundaries plain prefixes. Escaping is unnecessary because only the last component varies.
- **Limits.** Raise `MaxKey` to 1,032 (an S3 key's 1,024 bytes plus its 5-byte prefix, rounded to 8) and keep `MaxValue` at 1,000: 2,032 stays within the 2,033 that a page split needs. Range-removal bounds stay at most 1,000 bytes each (2,007 bytes with both, within half a page), refused before proposing. `internal/lsm/block.go:238`, which checks `MaxKey` when it decodes run blocks, follows the constant; tests should assert both the point-entry and the fragment pair bound. Store any value over 1,000 bytes as pieces: the key's own value holds the first piece and the piece count, and pieces 1 … m follow under the piece tag. A 64 KiB xattr is about 66 values of up to 1,000 bytes, about 82 KiB in one `Txn`; a 4,095-byte symlink target is 5. A read of a split value is 1 + m point lookups, about 1 µs each. Name lengths are checked at the frontend: 255 bytes for names and xattr names, 4,095 for symlink targets, 64 KiB for xattr values, 1,024 for S3 keys.

### 3. Routing

- **Followers redirect.** The shard sets etcd-raft's `DisableProposalForwarding`; a follower or candidate answers `NotLeader{shard, leader node, term}`, and the daemon updates its route cache and resends. Forwarding would hide the result from the proposer, add a hop, and bypass the inode owner's open registry, which lives at the leader.
- **During an election** a `Txn` fails retryably (`ErrProposalDropped` underneath). The leader never re-proposes on its own, and a proposal from an old term is either in the new leader's log or gone, so the only ambiguity is the caller's: a client resends with the same replay key, which returns the first final result; an internal caller re-reads, and its guards (above) make a repeat fail harmlessly. CockroachDB needs `max_lease_index` because its leaseholder re-proposes; this shard does not.
- **`ctl/` uses the same `Txn` and `Scan`.** The shard map pins `[ctl/, PrefixEnd(ctl/))` to the control-plane group, which never splits. The control-plane leader's logic (placement, renewals in memory, fencing, grants) is code beside that group that proposes `Txn`s on `ctl/` keys like any client, so the shard state machine, its tests and its invariants are one piece of code.
- **Phase 4 refuses a `Txn` that spans groups** with `ErrCrossShard{groups}` before proposing anything. With the control-plane group and one data shard, that is a `Txn` mixing `ctl/` with other keys; no transaction named above does. Two-phase commit arrives with range splits (phase 13).

### 4. Space grants

- **Ask.** Each heartbeat asks, per pool, for min(`writeback_buffer`, what the node holds buffered + what it flushed since the last heartbeat): what it would buffer in the next interval at its current pace. An idle node asks for 0, so its grant comes back within a heartbeat, which is Lustre's shrink protocol for free.
- **Divide.** When the requests exceed the cap, max-min fair shares with what each node holds buffered as a floor: grantᵢ = min(askᵢ, max(bufferedᵢ, L)), with the water level L set so the grants fill the cap. Four nodes asking 256 MiB against 512 MiB get 128 each; asking 256, 256, 64 and 16 they get 216, 216, 64 and 16, also when the first already holds 200 MiB buffered; with 250 buffered, 250, 182, 64 and 16 (results, part 4). The part of a grant above L is a promise for data already buffered, not room to reuse: a node above the level buffers nothing new until its buffered bytes fall below L, and the leader counts it at buffered + k × L. Without that, a node held at its floor on a full disk (L = 0) would flush and refill its whole grant every window and fill G4's gap in about 9 windows (results, part 3). If the floors alone exceed the cap, as after a sharp drop in available, every grant is its floor, nothing new is buffered, and the excess drains as the nodes flush.
- **Reuse budget, in place of h_d.** A daemon may reuse its grant for at most k grants' worth of flushes, k = 2, counted from when it sent the last heartbeat whose reply it received; past that it writes through until the next reply, and data it already buffered still flushes. Once it has used half the budget it sends its heartbeat at once rather than waiting for the next second, as CephFS's client reports its size early once its unreported growth passes a fraction of what is left (`Client.cc:18395-18413`); the reply resets the window, so the budget bounds unseen bytes, not throughput. Everything a node can land since the window began is then at most (1 + k) × its grant, whatever happens to heartbeats, clocks or the control plane, so the cap becomes Σ (1 + k) × grantᵢ + F ≤ g_d for every disk, with F, the flushes reported but not yet in fill reports, counted in full as Σ is. No disk write rate is needed. The budget and the grant count physical blocks written, not `write()` bytes, so a 100-byte append is charged a whole block per copy.
- **What a heartbeat carries.** Per pool, a cumulative count of blocks flushed, which the leader differences, so a lost heartbeat loses no report; and the grant the node holds. The leader counts (1 + k) × max(held, new) for a node until a heartbeat echoes the new grant, so a shrink whose reply is lost is never counted before the node has it (without this, three nodes shrunk from 256 to 50 MiB with their replies lost could land 2.42 GB while the leader counts 0.47 GB). The cumulative count lives in the handoff memfd, so a successor after a daemon crash continues it; a successor that cannot read it buffers nothing until its first heartbeat is answered, and the leader keeps counting the old (1 + k) × grant until fill reports after that heartbeat arrive. A new leader, which knows no held grants, counts every live session as (1 + k) × `writeback_buffer` per pool until it renews.
- **The gap is measured from where the disk is.** g_d = the disk's size less its log reserve less max(fill, `write_limit`), the second reading of G13.
- **A lapsed session's grant counts until its fence is acknowledged,** with its reuse budget, the moment owners release its locks: until then a primary can still apply its granted flushes, and Lustre likewise discards an evicted client's grant only "once we're sure no more interaction with the client is possible". It holds (1 + k) × `writeback_buffer` of the pool per lapsed session for `session_lease` plus one fence round.

**Costs.** The cap near full is g_d ÷ (1 + k): with k = 2, 3.29 GB on G4's 100 GB disk at 90%, more than the 2.68 GB ten full grants need, and 1.06 GB on G5's 32 GB NVMe disk, where a 1 s h_d allows 0.17 GB (results, part 2). A streaming node sends extra heartbeats, about 4 per second at 10 GbE and 41 at 100 GbE with full grants; each costs the leader a memory update and no Raft entry. Without them the budget would cap buffered throughput even with no fault: at k = 2, 49% of each second at 10 GbE and 5% at 100 GbE (results, part 3). A lost reply costs nothing unless the next early heartbeat's reply is lost too. During a control-plane failover, a node that flushes more than k grants in the meantime writes through until a new leader answers, about 1 ms per write instead of 5 µs; a larger k buys a heavy writer less than a second more of buffering there and costs the cap near full in proportion (k = 4 gives 1.97 GB on G4), so k is small. The daemon keeps one counter and one snapshot of it per outstanding heartbeat.

**Alternatives rejected.** *Reuse for one heartbeat interval after the last answered heartbeat, with h_d the disk's peak rate × that window*: also closes finding 7, but it depends on clocks (the holder must stop on its faster clock, the leader assume the slower), one late reply sends even a light writer to write-through for about a second, and h_d needs a peak rate per disk, seeded per device class and never lowered, which leaves a small fast disk almost no grant (0.17 GB on G5). *A byte budget without early heartbeats*: it caps throughput on fast links with no fault at all, unless k is large, and a large k costs the cap near full. *Reuse for the whole session lease* (the design today): finding 7. *Lustre's one eighth of what is left, first come*: whoever asks first keeps it until it shrinks, and a periodic shrink protocol is needed to get it back. *Shares proportional to requests*: a node streaming hard crowds out many small writers, which water-filling serves in full. *Per-disk grants kept at each disk, as Lustre keeps them per target*: exact and with no lag, but a grant per node and disk and a request to every storage node, which the [capacity and ENOSPC study](../capacity-and-enospc/README.md) already rejected.

## The other Open lines

There are 27 Open lines in the sections this study covers; each is answered here.

**Shard state machine**

- Delete of an absent key: succeeds (CockroachDB, etcd, TiKV); the purger depends on it.
- Put then delete in one `Txn`: applied in order, so the key is absent; checks see the state before the `Txn`.
- `Txn` over 1 MiB: refused before proposing with a size error at 256 KiB; never split.
- Empty ops: an error.
- `after` exclusive: yes; with the range form it is `start = after + 0x00`.
- limit 0: an error; every reply is also bounded by bytes.
- Empty prefix: a range like any other; refused by routing when it spans groups, as it always does in phase 4.
- Range removal in a `Txn`: a `DeleteRange` op, not the purger calling the tree, so it replicates like every other write.

**Key encoding**

- Tag values and widths: the tables above.
- Key and value limits: `MaxKey` 1,032; range bounds at most 1,000 bytes; values over 1,000 bytes as pieces.

**Sharded API, one shard**

- A `Txn` across shards: `ErrCrossShard` naming the groups.
- At a follower: redirect with the leader hint.
- Internal `Txn`s re-proposed after a leader change: guarded by a `Check` or `CheckEmpty` on everything they read.
- `ctl/` keys: the same API and routing, with the range pinned to the control-plane group.

**Control-plane group**

- F lowering A, or each disk's room: equivalent (finding 8).
- G3, a binding cap: water-filling with buffered floors; no reuse above the level.
- G5, where h_d comes from: nowhere; a per-node reuse budget of k = 2 grants, with early heartbeats, replaces it.
- G10, a lapsed session's grant: counted, with its budget, until its fence is acknowledged.
- G13, a disk past `write_limit`: measured from its fill.
- Format level, a node on an older release: refuse to mark it up and raise a condition naming it. Ceph's monitor likewise ignores the boot of an OSD below a fixed minimum release (`src/mon/OSDMonitor.cc:3561-3567`); its operator-raised `require_osd_release` checks next to it refuse newer OSDs, not older ones. This study did not look further.

**Filesystem schema**

- `mkdir` in a fresh slot: sequence 1, with sequences 2 … 65 reserved for the creating daemon.
- flock "no other session's key": per-gap `CheckEmpty`, with the mode in the key.
- Slot byte counter: `Update(u, Add)`, never compare-and-set.
- A resend after its record was dropped: not answered by this evidence. It runs again and returns whatever the state now gives; an honest oldest-outstanding value rules it out.
- A failed request's result: stored only when final (a check marked `final`), so a `rename` that failed with `ENOENT` returns `ENOENT` when resent, and a lost race is retried. NFSv4.1's session reply cache is the usual precedent but was not read for this study.
- Replay key without a mount ID: add a 32-bit mount ID (finding 6).

**MetaStore invariants**

- The seed count for planted bugs: not answered by this evidence.

## What the phase 2 model, the simulator and the tests must check

- **Shard state machine:** 64 concurrent `create`s in one directory all commit in one round with distinct inodes (the design's "Two racing `create`s" expects a retry, which changes); 1,000 `create`s in one directory over 100 rounds commit without one failed check; a create from a node whose clock is behind still moves the parent's `mtime`; `rmdir` of a non-empty directory fails with `ENOTEMPTY`; a `Txn` with `Update`s, `Put` and `Delete` on one key applies in order; a `DeleteRange` bound of 1,001 bytes is refused before proposing.
- **Replay:** a `mkdir` that failed with `EEXIST`, resent, returns `EEXIST`; a `close` whose `Check(a)` loses a race retries under the same request ID and commits; a resent `Txn` returns its first final `Result`, including an `Update`'s value; a session leaving the session table has its records removed in every slot. Planted bug: a non-final check failure stored as the request's result.
- **Filesystem schema:** rows for `link`, `symlink` (a 4,095-byte target), cross-directory rename of a directory with its loop check, rename over a non-empty directory, an xattr of 64 KiB, inode and slot reservation (and `mkdir` then `create` with no reservation round).
- **Purge model:** two inodes, not one; add "truncate, then unlink before the purge runs", "two truncates" and "an unlink of a lower inode between the purger's scan and its range removal" to its scripted scenarios, and to its rules-removed table "a new purge entry for an inode with a pending one overwrites it" and "the consumed run is removed without checks", each with a leaked chunk as the counterexample.
- **Grants:** G3 with buffered floors and with floors above the cap; G11 rewritten (under early heartbeats a lost reply costs nothing unless the next one is lost too); a shrink whose reply is lost; a lost heartbeat request, then a fill report; a floor grant on a full disk with a writer streaming; a daemon crash mid-window; a 25 GbE stream with no faults, which stays buffered. The invariant, in both the Control-plane group and the MetaStore invariants sections: Σ (1 + k) × grants + F ≤ g_d on every disk, and no flush under a grant is refused for space, with the faults "a client partitioned from the control-plane leader but not from storage, streaming under its grant" and "a control-plane failover while daemons stream". Planted bugs: grants reusable for the whole session lease; a window counted from the reply's receipt instead of the request's send; flushes reported since the last sent heartbeat instead of cumulatively; a shrink counted before it is echoed; a floor grant that can be reused.
- **Golden vectors and fuzzing:** the tag values above, the 34-byte replay key, piece keys; a fuzz test of the piece decoder (a missing piece, a piece count past the limit).

## Where it would apply

- [Metadata layer](../../docs/design.md#metadata-layer-metastore): the interface block, its comment and the op table; the evaluation, replay and guard rules; the routing paragraph (redirects, `ctl/` pinned, phase 4's refusal); the key layout (tags, widths, pieces, the mount ID, `l/{mode}/`, the symlink target and parent keys, the next-slot key, inode and slot reservation); a sentence on why shared keys change only through `Update`; "Lists consumed in key order" rewritten so the purge list's removal carries its checks (finding 4).
- The LSM tree bullet in [On-disk format](../../docs/design.md#on-disk-format) (`docs/design.md:229`): the key and value limits, and the bound on range-removal fragments.
- [Freeing space](../../docs/design.md#freeing-space): unlink removes the inode's whole key range; a new purge entry merges with a pending one; the purger's writes are checked.
- [Client design](../../docs/design.md#client-design): "Space grants" (ask, water-filling with no reuse above the level, reuse budget and early heartbeats in place of h_d, the cumulative flush count and held grant on each heartbeat, g_d from fill and F, lapsed sessions, a new leader's count); namespace operations update the parent's times and link count.
- [Daemon restart and upgrade](../../docs/design.md#daemon-restart-and-upgrade) (`docs/design.md:719`): the mount ID in the replay key, which results are stored, records removed at session end.
- [Alternatives considered](../../docs/design.md#alternatives-considered): a row for the `Txn` op set (compare-and-set only; etcd's compare/then/else; proposer evaluation) and one for grants (reuse budget against a time window with h_d; water-filling against Lustre's rule).
- [Roadmap and open questions](../../docs/design.md#roadmap-and-open-questions): "Space grants on small, fast disks" answered by the reuse budget or rewritten around k.
- `docs/test-scenarios.md`: the Open lines above replaced by their answers; Shard state machine's "A key of 1,025 bytes" row becomes 1,033, plus a range-bound row; the `create` race row; Key encoding's Rules gain the session, mount and request widths; the Filesystem schema, replay and Purge model rows above; G3, G5, G10, G11 and G13 given their expected values; the MetaStore invariants grant row and its planted bug as above.
- `docs/build-checklist.md`: the Shard state machine item's op list; the Key encoding item's limits.
- `internal/lsm`: `MaxKey` 1,032, the range-bound check and tests of both pair bounds, in the change that adopts this.
- `docs/glossary.md`: "Atomic update (Add, Bump)", "Redirect (not leader)" and "Reuse budget", if adopted.

## Unverified

- **The create rate** is arithmetic from the design's measured commit latencies, assuming every concurrent creator in a round read the same parent value, and the loop bound is the design's 60,000 proposals per second; no MetaStore exists to measure it.
- **The page bound** of 2,033 bytes is read from `splitPoint` and its comment, not tested at the edge.
- **Finding 7** assumes a client can reach storage nodes while its heartbeats do not reach the control-plane leader, or that daemons keep flushing through a control-plane failover; both are listed simulator faults, but no seed has run.
- **The reuse budget's k** (2) trades the cap near full against buffering through a failover; it is reasoned, not measured, and early heartbeats at 100 GbE (about 41 per second per streaming node) are not measured against the leader's loop.
- **The fragment bound** assumes a range removal's end is stored as a value of the second page tree, as the design describes; `internal/lsm` has no range removals yet to check against.
- **Lustre's grant shrink interval** and its client-side accounting (`osc_cache.c`) were read only where cited.
- **Not read:** NFSv4.1's reply cache for failed requests, Ceph RGW's index transactions, TiKV's Percolator write path beyond its proto, HopsFS's inode ID batches.
- **Branch heads, not releases,** for CockroachDB, TiKV, kvproto, JuiceFS, Lustre, FoundationDB, pjdfstest, Samba and the AWS guide, read on 2026-10-19 and pinned in `fetch.sh`.

## Reproduce

`./fetch.sh [dir]` downloads every cited file at its pinned commit into `dir` (default `./work`) and prints the cited lines. Needs `curl`. `python3 model.py` prints [`results.txt`](results.txt).

## Sources

- CockroachDB `cockroachdb/cockroach` at [`d30c905`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff): `pkg/kv/kvpb/api.proto`, `pkg/kv/kvpb/errors.proto`, `pkg/storage/mvcc.go`, `pkg/kv/kvserver/replica_evaluate.go`, `pkg/kv/kvserver/replica_raft.go`, `pkg/kv/kvserver/kvserverbase/base.go`, `pkg/kv/kvserver/kvserverpb/proposer_kv.proto`, `pkg/kv/kvclient/kvcoord/dist_sender.go`, `pkg/kv/kvserver/addressing.go`, `pkg/keys/constants.go`, `pkg/keys/sql.go`, `pkg/roachpb/data.go`, `pkg/util/encoding/encoding.go`, `docs/RFCS/20160420_proposer_evaluated_kv.md`.
- etcd `etcd-io/etcd` v3.6.9 at [`85651fa`](https://github.com/etcd-io/etcd/tree/85651fa521731aaecad76ff81dee5450a766c874): `api/etcdserverpb/rpc.proto`, `server/etcdserver/txn/txn.go`, `server/etcdserver/api/v3rpc/key.go`, `server/embed/config.go`. etcd-raft `etcd-io/raft` v3.6.0 at [`f35a022`](https://github.com/etcd-io/raft/tree/f35a02212416d045c574e30aa59d4b07c4b8d732): `raft.go`.
- TiKV `tikv/tikv` at [`134cdb2`](https://github.com/tikv/tikv/tree/134cdb2ce9852aa30e878ade3849f877dd921018): `components/raftstore/src/store/config.rs`. kvproto `pingcap/kvproto` at [`f8ad079`](https://github.com/pingcap/kvproto/tree/f8ad079c8e9812ed5900b0c12b5f1d97171464c4): `proto/kvrpcpb.proto`, `proto/errorpb.proto`.
- FoundationDB `apple/foundationdb` at [`487620d`](https://github.com/apple/foundationdb/tree/487620d5aa41152b1f034f4e932005a87eb2947f): `documentation/sphinx/source/api-common.rst.inc`, `known-limitations.rst`, `largeval.rst`.
- JuiceFS `juicedata/juicefs` at [`29c60bb`](https://github.com/juicedata/juicefs/tree/29c60bbbeea410c6c0cb7a49540044e0c099dffb): `pkg/meta/base.go`, `pkg/meta/tkv.go`.
- Lustre `lustre/lustre-release` at [`ee61b3f`](https://github.com/lustre/lustre-release/tree/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d): `lustre/target/tgt_grant.c`, `lustre/ofd/ofd_obd.c`, `lustre/osc/osc_request.c`, `lustre/osc/osc_cache.c`.
- Ceph `ceph/ceph` at [`b7e6787`](https://github.com/ceph/ceph/tree/b7e6787889ed0783df44063506dcacaaf672758f): `doc/cephfs/quota.rst`, `src/client/Client.cc`, `src/rgw/rgw_rest.h`, `src/rgw/rgw_rest.cc`, `src/mon/OSDMonitor.cc`.
- Linux `torvalds/linux` v6.18 at [`7d0a66e`](https://github.com/torvalds/linux/tree/7d0a66e4bb9081d75c82ec4957c50034cb0ea449): `include/uapi/linux/limits.h`, `fs/fuse/dev.c`, `fs/fuse/fuse_i.h`, `fs/ext4/xattr.c`.
- pjdfstest `pjd/pjdfstest` at [`85a8aea`](https://github.com/pjd/pjdfstest/tree/85a8aea9e685999ef0540392fd80535f873d7ff7): `tests/symlink/03.t`, `tests/misc.sh`, `tests/mkdir/00.t`, `tests/open/00.t`.
- Samba `samba-team/samba` at [`1e3906f`](https://github.com/samba-team/samba/tree/1e3906f39e95bd283bb9a677327a5d5aac852845): `docs-xml/manpages/vfs_streams_xattr.8.xml`.
- AWS S3 developer guide `awsdocs/amazon-s3-developer-guide` at [`e5e6bbd`](https://github.com/awsdocs/amazon-s3-developer-guide/tree/e5e6bbd2771d39e8ce3736849548a94b98d5094b): `doc_source/UsingMetadata.md`.
- rock-solid: `internal/lsm/tree.go`, `internal/lsm/memtable.go`, `internal/lsm/page.go`.
