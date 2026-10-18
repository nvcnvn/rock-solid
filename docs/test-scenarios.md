# Test scenarios

The scenarios the phase 1 tests must cover, one section per [build checklist](build-checklist.md) item. They come from the tests of the first, Rust, implementation of the disk log, and from the RocksDB, etcd and Protocol-Aware Recovery (FAST 2018) tests those were ported from, named next to each scenario. They describe behaviour, not an API.

A block is 4 KiB. A "1 GiB disk" is 262,144 blocks: 4,096 segments of 64 blocks (256 KiB), with the superblock copy at block 63. "[property]" marks a randomized or exhaustive test, with its seeds so a run replays. Sizes that follow from the record layout (a 48-byte header prelude, 33 bytes per write or punch entry, 14 bytes per blob entry, 4 bytes of CRC per data block) move with the layout if the Go code changes it; the behaviour does not.

## Buffer pool

- Explicit capacity: a pool of 2 blocks hands out two 1-block buffers, reports used = capacity and
  available = 0, and refuses a third 1-byte request with PoolExhausted(requested 1, available 0).
- Freeing a buffer returns its bytes; the next allocation reuses the same slot with a new generation,
  so the old handle and the new one differ. Used and live-buffer counts return to 0 after all frees.
- A fresh (non-zeroed) allocation is filled with poison byte 0xA5, never with the previous owner's
  data and never with zeros; the zeroed allocation returns all zeros. Code that assumes zeros must fail.
- A pool limited to 1 live buffer (slot limit) refuses a second allocation with available = 0 even
  though bytes remain; after a free it allocates again.
- Using a freed handle, or a stale handle whose slot has been reallocated (generation mismatch), is a
  hard error (the simulator pool panics), never a silent read of someone else's buffer.
- Exhaustion is an ordinary outcome, not a crash: a writer machine with a 1-block pool issues its first
  write; a second write at t=105 cannot allocate and arms its retry timer for t=115 (delay 10); the
  timer firing while still exhausted re-arms for t=125; the first DiskDone frees the buffer and the
  queued write goes out reusing slot 0 at generation 2; after the last DiskDone no buffers are live.

## Seeded RNG

- Two generators with the same 64-bit seed (0xDEADBEEFCAFEBABE) yield identical streams across u64,
  u32, bounded draws (below 97), inclusive ranges (1000..2000) and byte fills, over 256 iterations.
- Golden stream: the first draws for seed 0 are checked in. A change to the generator breaks replay
  of every recorded regression seed, so it must fail this test first.
- Fork: a child drawn from a parent (seed 42) is deterministic (two parents with the same seed give
  equal children and equal subsequent parent draws) and its stream differs from the parent's.
- Bounds: below(0) and below(1) return 0; range(5..=5) returns 5; chance(0/10) is false and
  chance(10/10) true; 512 draws of range(10..=20) stay in range. Bounded draws are unbiased
  (rejection sampling).
- Shuffle (Fisher-Yates) of 0..16 with the same seed (99) gives the same permutation, and it is not
  the identity.

## IDs and hashing

- Inode IDs pack a 24-bit slot (high bits) and a 40-bit sequence: block-volume inode = 0 (slot 0, seq 0);
  root inode = 1 (slot 0, seq 1); slot-metadata ID for slot 0x123456 is 0x1234560000000000 (seq 0).
- Inode (slot 0xABCDEF, seq 0x9876543210) encodes big-endian as AB CD EF 98 76 54 32 10 and
  round-trips; slot > 2^24-1 or seq > 2^40-1 is rejected.
- Chunk IDs pack (volume u32, inode u64, index u32) into 128 bits, big-endian on disk:
  (0x01020304, 0x05060708090A0B0C, 0x0D0E0F10) encodes as bytes 01..10 and round-trips via bytes
  and via the 128-bit integer.
- Offset to chunk: chunks are 4 MiB (1,024 blocks). Offsets 0 and 4 MiB-1 give index 0, 4 MiB
  gives index 1; block volumes use inode 0. The largest offset (2^32 x 4 MiB - 1) gives index
  2^32-1; one byte more is an OffsetOverflow error carrying the offset (16 PiB limit).
- Placement-group count must be a non-zero power of two (0 and 3 rejected); with 1 PG every hash maps
  to PG 0. The PG index is the top log2(count) bits of the hash.
- [property] PG splitting keeps hash ranges contiguous: for 64 chunk IDs and every count 1..2^11,
  doubling the PG count maps PG p to 2p or 2p+1.
- Golden vectors (`internal/core/testdata/chunk_id_golden.txt`, 10 rows, frozen by the design's key layout): for each (volume, slot, seq,
  index) the packed 128-bit ID, its XXH3-64 (seed 0, over the 16 big-endian bytes), PG index at 64
  and at 2048 PGs, and the PG ID in pool 1 all match. Rows include all-zero
  (hash 0xd0a66a65c7528968, PGs 52/1669) and all-ones (0x722a202f07a4b313, 28/913). The fixture is
  frozen disk format and is never edited.

## CRC32C

- RFC 3720 appendix B.4 vectors (Castagnoli): "123456789" gives 0xE3069283; 32 zero bytes give
  0x8A9136AA; 32 x 0xFF give 0x62A8AB43; 32 incrementing bytes give 0x46DD794E; 32 decrementing
  bytes give 0x113FDB5C; the 48-byte iSCSI READ(10) PDU gives 0xD9963A56. The checksum is stored
  little-endian (e.g. AA 36 91 8A). Verification accepts the right value and rejects value+1.
- Incremental hashing and "append to a finished CRC" both equal the one-shot CRC of a 4,096-byte
  payload (byte i = i*31 mod 256) for split points 0, 1, 7, 16, 63, 64, 511, 2048, 4095, 4096.

## Event and Action types

- Inline control messages hold 0..256 bytes; 257 bytes is rejected with an error giving the length.
  Bulk data travels only as pool buffer handles attached to a message or a disk action, never inline.
- 1 tick = 1 µs: 2 s = 2,000,000 ticks = 2,000 ms; 15 ms = 15,000 ticks; 950 ticks truncates to 0 ms.

## Peer requests

- Send records the request and returns exactly two actions: send the message, then arm the
  caller-chosen timer at now + timeout (e.g. t=100, timeout 50 gives deadline 150). Sending with an
  ID or timer that is already in flight is a programming error.
- PeerLost for peer A fails every outstanding request to A, in request-ID order, each reported with
  its peer, timer, deadline and data buffer (buffer or none), reason PeerLost. Requests to peer B are
  untouched. A later timer for a request that PeerLost already failed reports nothing.
- Timeout keeps the buffer: when request 1's timer fires at its deadline, it is reported once with
  reason Timeout but stays tracked (in-flight count unchanged), because the driver may still hold its
  buffer. A repeat of that timer reports nothing. A reply ("complete") then removes it and hands back
  its buffer.
- A timed-out request that never gets a reply is failed by a later PeerLost together with the other
  requests to that peer, buffer included; afterwards nothing is in flight.
- A timer ID can be reused after its request timed out: a new request on that timer is tracked and
  PeerLost fails both the timed-out one and the new one.
- Completed request: completing returns it once (a second completion returns nothing); a later timer
  or PeerLost for it reports nothing; a timer that guards no request reports nothing.
- Stale timer: request 1 on timer T is completed, request 2 reuses T with deadline 180. The stale fire
  at 150 (request 1's deadline) is ignored because now < request 2's deadline; the fire at 180 times
  out request 2. Timers are matched to the current request and its deadline, not just the timer ID.

## Segments and superblock

- Segment size = min(64 MiB, disk / 4096) rounded down to a power of two, at least 4 blocks; the count
  is disk / segment size and must be at least 4. Expected (disk, segment size, count):
  16 TiB, 64 MiB, 262,144; 1 TiB, 64 MiB, 16,384; 256 GiB, 64 MiB, 4,096; 255 GiB, 32 MiB, 8,160;
  16 GiB, 4 MiB, 4,096; 1 GiB, 256 KiB, 4,096; 1 GiB + 3 blocks, 256 KiB, 4,096; 64 MiB, 16 KiB, 4,096;
  1 MiB, 16 KiB, 64; 64 KiB, 16 KiB, 4. A 15-block disk is DiskTooSmall.
- Addressing: block b of segment s is at LBA s x segment_blocks + b (1 GiB: segment 3 block 5 is
  LBA 197). Segment 0 holds the superblock in its first block and its copy in its last block.
- Superblock (magic, version, CRC, log ID, segment blocks, segment count) and segment header (magic,
  version, stream, CRC, log ID, open sequence, own index, newest manifest position or none)
  round-trip, hot and cold, with and without a manifest pointer. Segment 0 in the manifest fields
  means "no manifest".
- [property, exhaustive] Flipping any single bit of the segment header makes it fail to decode. An
  all-zero block decodes as BadMagic.
- An unknown stream tag (2) is rejected as Malformed even when the checksum was recomputed to match.
- Golden bytes: the superblock for log ID 0x0123456789abcdef on a 1 GiB disk, and the segment header
  (seq 7, segment 3, cold, manifest at segment 2 block 9 LSN 41), encode to a checked-in fixture.
  Changing it is a format break that needs a version bump.

## Record codec

- Layout: a record is header blocks then data blocks. The header holds a 48-byte prelude, 33 bytes
  per write/punch entry, 14 bytes per blob entry, and a 4-byte CRC32C per data block, zero padded to
  whole blocks. Data holds each write's blocks in entry order, then blob payloads packed back to back.
- Layout examples: blobs of 1 and 4,095 bytes pack into 1 data block (1 header block); a 4,097-byte
  blob needs 2 data blocks; a 1-block write is a 2-block record; a 1,003-block write still fits one
  header block (48 + 33 + 4 x 1003 = 4,093 bytes) and 1,004 blocks needs 2.
- A header with 200 one-block writes spills into 2 header blocks (200 data blocks) and still
  round-trips.
- [property] Round trip: seed 1, LSNs 1..499, random records of 1-5 entries (writes up to 3 blocks,
  punches up to 512 blocks, blobs under 8 KiB, at most 8 data blocks) with random payloads decode to
  the same header, entries and LSN, and each blob's extracted payload has its declared length.
- [property, exhaustive] Torn writes decode whole or not at all: seed 4, 40 records of at most 10
  blocks; for every subset of landed blocks (2^blocks) over three kinds of old content (zeros,
  random bytes, a different valid record at the same position), decoding returns either exactly the
  new record (only if every block landed) or exactly the old record (only if the image is still the
  old one), never a mixture, and never rejects a fully landed record. More than 1,000 cases.
- [property] Torn 512-byte sectors: seed 5, 300 records over random old content, each 512-byte
  sector landing with probability 1/2: decode returns the exact record or an error, and only errors
  when some sector did not land.
- Any single corrupted byte (XOR 0x10 at every offset of a 1-write, 2-data-block record) is rejected
  (RocksDB ChecksumMismatch).
- A freeze entry (pool 1, chunk (2,3,4), version 5, snapshot epoch 9) round-trips, adds no data block,
  and may share a record with writes, punches and blobs.
- An unknown entry tag (100) with a recomputed header CRC is Malformed (RocksDB BadRecordType).
- A header-block count that disagrees with the entries, CRC recomputed, is Malformed (BadLength).
- Truncated images (0, 4, 48, 4,096 bytes, and all but the last byte) are rejected (TruncatedTrailingRecord).
- A record longer than the blocks left in its segment is Malformed (BadLengthAtEnd).
- A record read back under a different expected segment sequence, log ID or offset is a Mismatch, so
  records of an earlier incarnation of a recycled segment are never accepted (RocksDB Recycle).
- Golden bytes: a record (log ID 0x0123456789abcdef, segment seq 9, offset 1, LSN 10) with a 1-block
  write (pool 1, chunk (2,3,4), version 5, block 6, data block filled with 0xAB), a whole-chunk punch
  (version 6) and a 3-byte manifest blob ("abc", owner 7) encodes to a checked-in fixture, followed by
  zeros to the end of the header block.

## Manifest codec

- A manifest (base LSN 77, base seq 5, hot start at segment 3 block 17, no cold start, two indexes:
  owner 1 with one run of one piece and replay LSN 70, owner 2 with no runs and replay LSN 77)
  round-trips. A trailing extra byte, or a truncation to 10 bytes, is Malformed. Each blob reference
  adds 18 bytes. A run of several pieces is one reference, to the piece that lists the others.

## Group commit

- Format writes the superblock to block 0, flushes, then writes the same buffer as the copy at the
  last block of segment 0 (LBA 63 on 1 GiB), flushes, and then is idle. The buffer is freed, and IO
  IDs count up from the caller's first ID.
- One write and one flush per round: the first submit starts round 1 at once; four items submitted
  while it is in flight (a 2-block write, blobs of 100 and 5,000 bytes, a whole-chunk punch) all go
  into round 2. Disk trace: write, flush, write, flush. Durable reports LSN 1 with the first ticket
  and LSN 2 with the four tickets in submit order. All pool buffers are released.
- Opening a segment: round 1 writes the segment header (segment 1, seq 1, hot) and the record
  together, starting at segment 1 block 0 (LBA 64); the record sits at block 1 and the data
  is the submitted bytes. Round 2 appends at block 3 (LBA 67) with LSN 2 and 4 entries.
- Streams alternate when both have work, and each opens its own segment: with hot items A, B and a
  cold item queued behind round 1 (A), round 2 is the cold item (segment 2, LBA 128) and round 3 is
  hot B plus a later hot item (LBA 67). LSNs are global across streams.
- Rounds stop at the maximum record size: with 3-block records a 3-block write is refused as
  TooLarge at submit, 1-block writes pack two per record (LSN 2 = two items, LSN 3 = the third),
  and the largest blob is 2 blocks.
- Full segment rolls, full disk fails: a 16-block disk has 3 usable 4-block segments; three 2-block
  writes fill one segment each (LBAs 4, 8, 12). The next write, even 1 block, fails at once with
  NoSpace naming its ticket, and its buffer is freed.
- A write or punch with zero blocks, or running past block 1,024 of its chunk, is refused (BadExtent)
  with no disk action.
- A disk error fails the log: an Io error on the round's write fails the in-flight round and every
  queued item in one Failed event (in-round tickets first), frees all buffers, and every later
  submit is refused with that error.
- Pool exhaustion retries: with a 2-block pool, a 1-block write needs a 3-block image (segment
  header, record header, data), so the round arms the retry timer at now + delay (TimerId 4, delay
  50) instead of writing. Once memory is back, the timer event starts the write and the item becomes
  durable at LSN 1.
- [property] Every accepted item reaches the disk as a record that decodes to it: 100 seeds; max
  record 2..64 blocks; disk 16 blocks (1 in 4) or 1 GiB; punch share 0-8 of 8; 1-1000 items to
  random streams, 1 in 8 sized near the record limit; completions interleaved at random. Before each
  write the log decodes its own bytes and checks they hold exactly the queued items. Every accepted
  ticket ends Durable or Failed(NoSpace); the only refusal is TooLarge; the pool ends with 0 live
  buffers.
- Checkpoint end to end (max replay 8 blocks): after a 3-block write (5 blocks with the segment
  header) a checkpoint is not due; after a second (9 blocks) it is. A third write is in flight when
  the checkpoint begins for owners 5 and 6; a second begin is Busy, and "due" turns false while one
  runs. Two pieces for owner 5 then end. Results: LSN 3 = the in-flight write; LSN 4 = both
  pieces in one cold record (segment 2 block 1, entries 0 and 1); LSN 5 = the manifest, reported as
  CheckpointDone and kept as the newest manifest. The manifest has base LSN 2 (durable at begin, not
  the in-flight 3), base seq 1, hot start segment 1 block 9, no cold start, trees
  [(5, [LSN4#0, LSN4#1]), (6, [])].
- The next segment opened after a checkpoint (segment 3, filled by 40-block writes) carries the
  manifest's position in its header.
- Manifest in pieces: with 3-block records (8 KiB max blob), a checkpoint of more runs than one blob
  can name writes the manifest as several pieces, the last naming the others, and the next segment
  header points at the last piece. Recovery from it equals recovery from a one-piece manifest, and
  a crash that lands all but the last piece recovers from the previous manifest.
- A checkpoint on a full disk: its piece fails with NoSpace, which also emits CheckpointFailed(NoSpace).
  End then says NoCheckpoint, and a new checkpoint can begin.

## Recovery

All recovery scenarios run against the simulated disk. "Open" = replay-mode recovery: load the chunk
index's runs, replay the records after its replay LSN, resume group commit. "Rebuild" = recovery that
ignores manifests and returns every record header.

- Empty log: a freshly formatted disk recovers with no records and no snapshots; the first new write
  gets LSN 1 (RocksDB Empty).
- An all-zero disk is NotFormatted.
- Reformat (recycle): a disk holding log 3's six records is formatted as log 4 with one write. Recovery
  returns only that record (log ID 4), and 4,094 of 4,095 segments are free. Old segments are
  never replayed (RocksDB Recycle).
- Torn tail: 3 records of 2-block writes, then a 4th round written but not flushed. A crash landing
  only its first 0, 1 or 2 of 3 blocks, or every block but the last, recovers LSNs 1-3, and the index
  equals the index before the 4th write (RocksDB TruncatedTrailingRecord, PartialLast).
- An unflushed but fully landed last record is kept (LSNs 1-4), although it was never acknowledged.
- Damage in a middle record is corruption, not truncation: XOR 0x5A into every byte of record 2's
  header gives Corrupt(LSN 2) (RocksDB ErrorJoinsRecords). One flipped bit in record 3's data
  or header, of 5 records, gives Corrupt(LSN 3) (PAR, FAST 2018).
- Damage in the last record (a data block of record 5 of 5) cannot be told from a torn write, so it
  recovers as a crash: LSNs 1-4.
- Overwriting a torn tail: 10 records, then an 11th 4-block record crashing with only its even blocks
  landed, recovers 10. Five more writes then reuse the space, and the next open gives LSNs 1-15 with
  an equal, readable index (etcd TestOpenOnTornWrite).
- Segment exactly full: 21 two-block writes on 1 GiB fill segment 1 to its last block (last record at
  block 61). After reopening, appends continue in segment 2 (record 22), and 30 records recover
  (etcd TestTailWriteNoSlackSpace).
- Crash in the round that opens a segment: write 22 opens segment 2. For each combination of the
  segment header and the record landing, recovery gives 22 records only if both landed, else 21. A
  following write and reopen give contiguous LSNs 1..n+1 (etcd TestRecoverAfterCut).
- Torn record left in the other stream: 2 hot records, then a cold 3-block blob whose first 2
  blocks (segment header, record header) land. Recovery gives LSNs 1-2. The next hot write reuses
  LSN 3 in segment 1. On reopen both streams hold an LSN-3 header, and recovery accepts the one
  whose data verifies (LSNs 1-3), reporting no corruption; rebuild agrees.
- Many segments, both streams: 64K-block disk (16-block segments), 4-block records, 200 writes of 1-3
  blocks plus a cold blob every 5th, using more than 20 segments. Reopen returns exactly the
  acknowledged records. The index from recovery and the index from rebuild both equal the live index,
  and every mapped extent reads back its written pattern (RocksDB ReadWrite/ManyBlocks/Fragmentation).
- Checkpoints bound replay: max replay 40 blocks, 120 writes of 1-4 blocks over 16 chunks with a
  whole-chunk punch every 7th, unlimited or 512-byte run pieces. At least 5 checkpoints complete.
  Reopen loads several pieces only with the 512-byte limit, and replays at most max replay + one
  segment + 1 MiB. Index, rebuild and read-back all match.
- Read errors: if reads of both block 0 and the superblock copy fail with Io, recovery ends with
  Disk(Io) and all its buffers are freed.
- [property] Random crashes keep acknowledged records: 50 seeds x 4 crash/reopen cycles; disk 1,024 or
  4,096 blocks; max record 2-32 blocks; max replay 4-200 blocks; random log ID. Each cycle runs up to 300
  steps, each either submitting 1-4 random items (60% writes of 1-3 blocks, 10% whole-chunk punches,
  20% hot blobs under 3,000 B, 10% cold blobs under 6,000 B) with 2/5 probability, or executing one
  pending disk action. The harness checkpoints whenever one is due, one piece per round. Then a crash
  lands each unflushed block with probability 1/2. Properties: recovery always succeeds; the
  recovered durable LSN is the pre-crash one or one more (then it is the last recovered record);
  the index from manifest+replay and the index from rebuild both equal the pre-crash index (plus
  that record); every mapped extent reads back; every blob has its declared length. Coverage checks:
  more than 5 kept in-flight rounds, more than 5 dropped, more than 20 recoveries from a manifest, and
  at least one NoSpace.

## Superblock copy

- A bit flip in block 0 or in the copy: recovery returns all records and rewrites the damaged copy
  from the good one, flushed before it reads further.
- An unreadable block 0 (read error) is restored from the copy, flushed.
- Block 0 of zeros, or of another format ("EFI PART" repeated), with a valid copy is CopyOnly(BadMagic,
  copy). The disk may have been reused, so recovery writes nothing and leaves restoring to the
  operator.
- Both copies damaged is NotFormatted.
- A reformat that crashes after the new block 0 is flushed but before the copy is written: block 0
  (new log) wins and the stale copy (old log) is rewritten to match.

## Quarantined segments

- [property] 10 seeds; 4,096-block disk (4-block segments), 4-block records, 300 writes of 1-2 blocks.
  Flip one random bit in the first 56 bytes of 3 random used segments' headers (not the newest)
  and of 1 free segment. Rebuild recovery reports exactly the 3 as quarantined, and the free one
  stays free. Filling the disk until NoSpace leaves every quarantined segment's blocks unchanged.
- Quarantine is detected by reading a damaged segment in full for valid record headers of this log
  (any segment sequence). A damaged header over a segment with no records counts as free.
- Replay through a quarantined segment is corruption: 64K-block disk, 60 one-block writes. Damaging
  the header of LSN 1's segment gives Corrupt(LSN 1), a gap at the start. Damaging the newest
  segment's header (several records) gives Corrupt(first LSN in it): more than `commit_depth` (1)
  lost records cannot be rounds in flight. Losing only one is allowed.

## LSM tree

- Get, replace and prefix scan: keys "ab", "a", "abc", "b", "aa": scan "a" returns a, aa, ab, abc in
  order; scan "ab" returns 2; inserting "b" again replaces its value and leaves 5 entries.
- [property] Flush to a run at any piece size: seed 11, 50 trees from up to 200 random inserts and
  removes (keys 1-40 B, values 0-59 B), max piece 106-4,105 B. Every piece is non-empty, within the
  limit and sized exactly. Loading the pieces as a run into an empty tree and reading every key
  equals the original.
- [property] A flush keeps its frozen view: seed 12, 200 trees from up to 300 mutations (keys 1-3 B,
  1/4 removes), max piece 70-569 B. Up to 20 random inserts/removes run between pieces, on keys before
  and after the cursor, into the new memtable. The run equals the tree as of the freeze, the frozen
  pages are freed after the last piece, and the tree still answers every key.
- Memtable over runs: after a flush, a key only in the run reads from the run; a key written after the
  flush reads the memtable's value; a key removed after the flush reads as absent; a scan merges the
  memtable and runs in key order with no duplicates. A get touches the memtable and each run at most once.
- Compaction: three runs with overlapping keys merge into one run equal to their merge, newest value
  winning and removed keys dropped. A view opened before the compaction still reads the three runs, and
  the old runs are released only after that view closes.
- Restore is runs plus replay: after two flushes and more writes, loading both runs and replaying the
  later records equals the live tree.
- Loading a truncated piece (last byte removed) is Malformed.

## Chunk index

- [property] Key byte order equals logical order: seed 21, 5,000 random key pairs. Comparing encoded
  keys (pool | XXH3 of chunk | chunk | frozen version, 0 for the live map | first block, all
  big-endian, 38 bytes) matches comparing the tuple. Keys round-trip, and a chunk's entry sorts
  before its live map, which sorts before its frozen maps.
- Applying a durable record at LBA 100 (one header block) with a 2-block write of chunk 7 v1 at
  block 0, a blob, a 1-block write of chunk 7 v2 at block 5, and a whole-chunk punch of chunk 8 v3
  maps: chunk 7's live map to data at LBA 101 (blocks 0-1, v1) and LBA 103 (block 5, v2; writes are
  packed before blobs), both with header LBA 100, and its chunk entry to version 2; chunk 8's live
  map to Zero(1,024 blocks, header 100) and its entry to version 3. Blobs are skipped. Listing a
  chunk returns its live extents by first block.
- Trim and split: a 1-block write of chunk 7 v3 at block 1 leaves the live map as block 0 (v1,
  LBA 101), block 1 (v3) and block 5 (v2); a 4-block write v4 at blocks 0-3 leaves blocks 0-3 (v4)
  and block 5 (v2). The live counts of the records that lost blocks fall by the blocks trimmed, and
  the live map never holds two entries for one block.
- Freeze: a record with freeze(chunk 7, v4, epoch 2) and a 1-block write v5 at block 2 copies the live
  map under key version 4 before applying the write. A read of block 2 at snapshot epoch 1 finds v4's
  block, the live read finds v5's, and a read at epoch 3 reads the live map. The chunk entry holds
  version 5 and epoch 2. A rebuild from headers in LSN order gives identical live and frozen maps.
- A cleaner move (same chunk, same blocks, new location LBA 200: data 201) applies only if the live
  or frozen entry still points at the location it copied; otherwise it is dropped.

## Simulated disk

- Writes stay volatile until a flush. Reads see unflushed writes. On a crash each unflushed block
  lands or not on its own: a flushed block of 1s at LBA 0, overwritten by an unflushed 3-block write
  of 2s, crashing with only block 1 landing leaves LBA 0 = 1s, LBA 1 = 2s, LBA 2 = zeros, and
  nothing unflushed.
- Unwritten blocks read as zeros. A read or write past the end of the disk completes with InvalidRange.
- Fault helpers used by the log tests: crash with a chosen landed-set or with p = 1/2 per block
  (from a seeded RNG); flip a bit (XOR a mask into one byte of a durable block); fail reads of
  chosen LBAs with Io.

## Seeded runs and the simulation harness

The seeded runs, each described in its section above:

- Group commit: 100 seeds (every accepted item decodes).
- Random crashes: 50 seeds x 4 cycles (acknowledged records kept, index = rebuild).
- Quarantine: 10 seeds (bit flips in segment headers).
- Record codec: seed 1 (round trip, 499 records), seed 4 (exhaustive torn subsets, 40 records x 3 old
  contents), seed 5 (torn 512-byte sectors, 300 records).

The simulation node harness: it applies every Durable record to the chunk
index. It starts a checkpoint whenever one is due, freezes the index's memtable, hands one run piece
at a time, and the next only after the previous piece is durable. It ends the checkpoint after the
last piece, and on CheckpointFailed keeps the frozen memtable for the next attempt and stops
checkpointing. Write payloads are a
pattern derived from (chunk hash, version, block), so any extent can be checked by reading it back.
