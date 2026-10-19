# Test scenarios

The scenarios the phase 1 tests must cover, one section per [build checklist](build-checklist.md) item. They come from the tests of the first, Rust, implementation of the disk log, and from the RocksDB, etcd and Protocol-Aware Recovery (FAST 2018) tests those were ported from, named next to each scenario. They describe behaviour, not an API.

A block is 4 KiB. A "1 GiB disk" is 262,144 blocks: 4,096 segments of 64 blocks (256 KiB), with the superblock copy at block 63. "[property]" marks a randomized or exhaustive test, with its seeds so a run replays. Sizes that follow from the record layout (a 48-byte header prelude, 33 bytes per write or punch entry, 37 per freeze, 14 per blob, 4 bytes of CRC per data block) move with the layout if the Go code changes it; the behaviour does not.

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
- An entry with a field its kind does not encode set (a freeze with an owner, a punch with an epoch,
  a write with a length, a blob with a version) is Malformed, since it would not decode to itself.
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

- A manifest (base LSN 77, two indexes: owner 1 with one run of one piece and replay LSN 70, owner 2
  with no runs and replay LSN 77) round-trips. A trailing extra byte, or a truncation to 10 bytes, is Malformed. Each blob reference
  adds 18 bytes. A run of several pieces is one reference, to the piece that lists the others.
- A manifest naming one owner twice, or a replay LSN past its base LSN, is Malformed.
- A manifest's last piece (the format version, a 2-byte count, an 18-byte reference to each earlier
  piece, then the rest of the manifest's bytes) round-trips; a one-piece manifest has no
  references; a truncated list is Malformed and another version BadVersion. Both decoders are fuzzed.
- The segment usage table round-trips with the manifest (segment 1 used, 40 live blocks, newest LSN
  77; segment 3 quarantined; segment 4 used and empty), and each row adds 17 bytes. A row for segment
  0, rows out of segment order or naming one segment twice, a free or unknown state, or a newest LSN
  past the base LSN is Malformed.

## Group commit

- Format takes each round slot's header and blob buffers from the pool for the log's lifetime
  (with 1 MiB rounds on 1 GiB: 64 + 63 blocks per slot), writes the superblock to block 0, flushes,
  then writes the same block as the copy at the last block of segment 0 (LBA 63), flushes, and is
  idle. IO IDs count up from the caller's first ID. A pool too small for the slots' buffers fails
  Format with PoolExhausted and leaves nothing allocated; a depth below 1 or rounds below 3 blocks
  are BadConfig. Close returns every buffer, and refuses while a write or flush is in flight.
- One write and one flush per round: the first submit starts round 1 at once; four items submitted
  while it is in flight (a 2-block write, blobs of 100 and 5,000 bytes, a whole-chunk punch) all go
  into round 2. Disk trace: write, linked flush, write, linked flush. Durable reports LSN 1 with the
  first ticket and LSN 2 with the four tickets in submit order and their payload offsets in the
  data area (0, 8,192, 8,292, 13,292). Every producer buffer is released.
- Opening a segment: round 1 writes the segment header (segment 1, seq 1, hot) and the record
  together, starting at segment 1 block 0 (LBA 64); the record sits at block 1 and the data
  is the submitted bytes. Round 2 appends at block 3 (LBA 67) with LSN 2 and 4 entries.
- Streams alternate when both have work, and each opens its own segment: with hot items A, B and a
  cold item queued behind round 1 (A), round 2 is the cold item (segment 2, LBA 128) and round 3 is
  hot B plus a later hot item (LBA 67). LSNs are global across streams.
- Rounds stop at the maximum round size, segment header included: with 3-block rounds an item's
  record may take 2 blocks, so a 3-block write and a blob over 1 block are refused as TooLarge at
  submit; 1-block writes pack two per record once no segment header is needed (LSN 2 = two items,
  LSN 3 = the third).
- Full segment rolls, full disk fails: a 16-block disk has 3 usable 4-block segments; three 2-block
  writes fill one segment each (LBAs 4, 8, 12). The next write, even 1 block, fails at once with
  NoSpace naming its ticket, and its buffer is freed. NoSpace fails the stream's whole queue, items
  behind its head included, so items are never written out of submit order.
- A full pool never stalls a round: with the pool holding only the log's buffers and eight 1-block
  writes, so that nothing is free, all eight become durable.
- A write or punch with zero blocks, or running past block 1,024 of its chunk, is refused (BadExtent)
  with no disk action.
- A refused item (a write buffer that is not exactly its blocks, a missing buffer, a blob longer than
  its buffer, a punch with a buffer, or a manifest blob kind, which only the log writes) leaves its
  buffer with the caller.
- A disk error fails the log: an Io error on round 1's write reports its ticket InDoubt and the
  queued items Failed, frees their buffers, and every later submit is refused with that error.
  With depth 2, an error on round 2's write while round 1's is in flight reports both rounds
  InDoubt (round 1 may still land) and the queued item Failed; round 1's buffers come back when its
  write completes.
- Driver faults fail the log: a flush reported before its write (ErrFlushBeforeWrite) leaves its
  round InDoubt and frees the write's buffers when the write completes; an error on an earlier
  round's flush that arrives after a later flush made both durable fails the log, and Close waits
  for it.
- Commit depth 2: a hot and a cold write are submitted. Round 1's write goes out with its linked
  flush; round 2's write goes out alone, since round 1's write has not completed. Round 2's write
  completing sends nothing; round 1's completing sends round 2's unlinked flush. That flush
  completing makes both rounds durable in LSN order, and round 1's own flush completing later
  changes nothing.
- Regression (seed 1 of the property below): with round 1's write completed first, round 2's flush
  waits for round 2's own write; an unlinked flush covers only writes that have completed.
- A round appends to a segment only once the round that opened it is durable (commit depth 2): while
  round 1 (hot, opening segment 1) is in flight a second hot write waits, and a cold write goes out
  at once into segment 2. Round 1 completing sends the waiting hot write.
- [property] Every accepted item ends durable in a record that decodes to it: 100 seeds; max round
  3..64 blocks; disk 16 blocks (1 in 4) or 1 GiB; commit depth 1-3; max replay 4-200 blocks; a pool
  of the log's buffers plus a round to 512 blocks; punch share 0-8 of 8; 1-1000 items to random
  streams, 1 in 8 sized near the round limit; a checkpoint of owners 1 and 2 with 0-3 random pieces
  whenever one is due; 1 seed in 8 fails one random disk action with Io; completions interleaved at
  random, a linked flush running only after its write. Before each write the log decodes its own
  bytes and checks they hold exactly the queued items. Every accepted ticket ends Durable, Failed
  (NoSpace or Io) or InDoubt (Io); a Durable record read back from the disk holds its tickets'
  entries and payloads at the reported offsets, and the log's own manifest pieces carry NoTicket;
  no record is reported durable before a flush that ran after its write completed; LSNs are
  reported in order; every finished checkpoint's manifest reads back with its base LSN and runs;
  without an error there is one flush per write; the only refusal is TooLarge; Close then finds
  every buffer back. Coverage: NoSpace, TooLarge, a full pool, InDoubt, deferred flushes, depth
  above 1, finished and failed checkpoints all occur.
- The decode check catches a broken encoder: an encoder that writes a wrong version into an entry
  and fixes the header checksum, flips a padding bit, or writes the segment header of the other
  stream with its checksum fixed panics in the decode check on every one of the property's seeds,
  as does a round that reports its last payload a byte late, or writes one block more than its
  record.
- [property] Anything Submit accepts decodes: 20 seeds; max round 3..64 blocks; commit depth 1-3;
  300 items each of a random kind tag (unknown ones included) and random field values, half with one
  field set beyond what its constructor sets, with a buffer of its size, a block off, or none. Each
  item is either refused with Malformed, BadExtent, TooLarge, BadBuffer or ReservedKind, all of which
  occur, or ends Durable in a record that reads back as exactly its entry and payload. Regression
  (seed 1): a freeze entry carrying an owner was accepted and decoded without it.
- Checkpoint end to end (max replay 8 blocks, counted in record blocks, segment headers left out):
  after a 3-block write (a 4-block record) a checkpoint is not due; after a second (8 blocks) it is.
  A third write is in flight when the checkpoint begins for owners 5 and 6; a second begin is Busy,
  and "due" turns false while one runs. Two pieces for owner 5 then end. Results: LSN 3 = the
  in-flight write; LSN 4 = both pieces in one cold record (segment 2 block 1, entries 0 and 1);
  LSN 5 = the manifest, reported as CheckpointDone and kept as the newest manifest. The manifest has
  base LSN 2 (durable at begin, not the in-flight 3), trees [(5, [LSN4#0, LSN4#1]), (6, [])] and a
  usage table of segment 1 alone: used, 6 live blocks (the two writes' data, not their headers),
  newest LSN 2. LSN 3 is not counted and segment 2, opened after begin, is not listed.
  The bytes left to replay are the 8 record blocks of LSNs 3 to 5.
- A checkpoint naming one owner twice, or a replay LSN past the durable LSN, is Malformed, as is
  one carrying a run in segment 0 or past the durable LSN. A checkpoint carrying a durable run names
  it first, then the run whose root it writes, and a change to the caller's slice after the begin
  does not reach the manifest.
- The next segment opened after a checkpoint (segment 3, filled by 40-block writes) carries the
  manifest's position in its header.
- Manifest in pieces: with 3-block rounds (4 KiB max blob), a checkpoint of 500 runs writes the
  manifest as several pieces, the last naming the others, and the next segment header points at
  the last piece.
- A checkpoint on a full disk: its piece fails with NoSpace, which also emits CheckpointFailed(NoSpace).
  End then says NoCheckpoint, and a new checkpoint can begin.

## Recovery

All recovery scenarios run against the simulated disk, through the chunk store stand-in described
under [Seeded runs](#seeded-runs-and-the-simulation-harness), whose "index" is a model of where each
live block of each chunk sits. "Open" = recovery in open mode: load the newest named manifest, read
the model's run through recovery, replay the records after its replay LSN, resume group commit.
"Rebuild" = recovery that ignores manifests and replays every record.

- Empty log: a freshly formatted disk recovers with no records and no manifest; the first new write
  gets LSN 1 (RocksDB Empty).
- An all-zero disk is NotFormatted.
- Reformat (recycle): a disk holding log 3's six writes and a cold blob is formatted as log 4 with one
  write. Recovery returns only that record (log ID 4), and all segments but segment 0 and the new
  log's are free, the old log's valid headers included (RocksDB Recycle).
- Torn tail: 3 records of 2-block writes, then a 4th round written but not flushed (LBAs 74-76). A
  crash landing none, {74}, {74, 75}, {75, 76} or {74, 76} recovers LSNs 1-3, and the index equals the
  index before the 4th write (RocksDB TruncatedTrailingRecord, PartialLast).
- An unflushed but fully landed last record is kept (LSNs 1-4), holding exactly the submitted write,
  although it was never acknowledged.
- Damage in a middle record is corruption, not truncation: XOR 0x5A into every byte of record 2's
  header gives Corrupt(LSN 2) (RocksDB ErrorJoinsRecords). One flipped bit in record 3's header or data,
  of 5 records, gives Corrupt(LSN 3) (PAR, FAST 2018), and in record 4's header, the last but one,
  Corrupt(LSN 4). Open and rebuild agree.
- Damage in the last record (a data block of record 5 of 5) cannot be told from a torn write, so it
  recovers as a crash: LSNs 1-4.
- Overwriting a torn tail: 10 records, then an 11th 4-block record crashing with only its even blocks
  landed (its header included), recovers 10 and zeroes the torn header. Five more writes then reuse the
  space, and the next open gives LSNs 1-15 with an equal, readable index (etcd TestOpenOnTornWrite).
- Segment exactly full: 21 two-block writes on 1 GiB fill segment 1 to its last block (last record at
  block 61). After reopening, appends continue in segment 2 (record 22), and 30 records recover
  (etcd TestTailWriteNoSlackSpace).
- Crash in the round that opens a segment: write 22 opens segment 2 (LBAs 128-131). For each
  combination of the segment header and the record landing, recovery gives 22 records only if both
  landed, else 21. A record that landed without its header is erased, and segment 2 is reopened with
  seq 3, past the seq that record carries; otherwise with seq 2. Segment 1 counts 42 live blocks
  and newest LSN 21; a segment 2 whose header landed is used, with 2 live blocks if its record did
  and none if not. A following write and reopen give contiguous LSNs 1..n+1 (etcd
  TestRecoverAfterCut).
- Torn record left in the other stream: 2 hot records, then a cold 3-block blob whose first 2 blocks
  (segment header, record header) land. Recovery gives LSNs 1-2 and zeroes the cold record's header.
  The next hot write reuses LSN 3 in segment 1, and open and rebuild then give LSNs 1-3.
- Commit depth 2, both rounds in flight landed whole: a hot and a cold write written but not
  flushed recover as LSNs 2 and 3, with their writes in the index.
- Commit depth 2, a round missing: of two hot rounds in flight the first's header does not land and
  the second lands whole. Both are dropped (LSN 1 recovers) and the second's header is zeroed, so a
  shorter write that reuses LSN 2 is not followed, on the next open, by the old LSN 3.
- A duplicate LSN is corruption: with the append rule switched off, a depth-2 round appending to a
  segment whose opening round is in flight lands alone; after recovery, segment 2 reopens with the
  same seq and the cold stream reuses the old record's LSN, and the next open gives Corrupt(LSN 23)
  instead of replaying the old record.
- A durable manifest is never dropped (depth 2): record 21's data is damaged, the manifest (LSN 22)
  is named by a segment header whose record never landed. Rebuild gives Corrupt(LSN 21), since the
  tail rule alone would drop the manifest; open, which replays from LSN 22, recovers it.
- Restart without a crash: a round written but not flushed when the process stops is replayed, and
  recovery's last action is a flush, so a crash right after it keeps LSN 4.
- A flipped bit in the header-block count of the last record on a 256 GiB disk (64 MiB segments)
  recovers as a torn tail (LSNs 1-2), not as an attempt to read a whole segment.
- Many reads in flight: with a read depth of 1, 4 or 64, that many segment-header reads are in flight
  at once.
- A failed superblock repair write, erase write or final flush fails recovery with Disk(Io) and frees
  every buffer.
- Manifest in pieces (3-block rounds): a checkpoint with 500 runs writes its manifest in several
  pieces; once a segment header names it, open loads the same manifest, all 500 runs, and replays from
  its replay LSN. A second such checkpoint crashing before the write of its last piece recovers from
  the first.
- Many segments, both streams: 64K-block disk (16-block segments), 4-block records, 200 writes of 1-2
  blocks plus a cold blob every 5th, reaching segment 20 or beyond. Open returns exactly the acknowledged
  records. The index from open and the index from rebuild both equal the live index, and every mapped
  block reads back its written pattern (RocksDB ReadWrite/ManyBlocks/Fragmentation).
- Checkpoints bound replay: 64K-block disk, max replay 40 blocks, 120 writes of 1-4 blocks over 16
  chunks with a whole-chunk punch every 7th, unlimited or 512-byte run pieces. At least 5 checkpoints
  complete. Open loads several pieces only with the 512-byte limit, replays at most max replay
  + 3 segments, and reads at most 4 segments more: a segment per stream before the replay point and
  the rest of each open segment. Index, rebuild and read-back all match.
- What is left to replay: with max replay 8 blocks, two 3-block writes leave a checkpoint due after
  recovery, with 8 record blocks to replay.
- Read errors: if reads of both block 0 and the superblock copy fail with Io, recovery ends with
  Disk(Io); a read error on a segment header fails it too. All its buffers are freed.
- Out-of-step calls: Recover refuses a read depth below 1 and an unknown mode; Replay before
  ManifestLoaded and Log before Recovered are refused; Close refuses while a read is in flight; a blob
  reference that does not name a blob of a durable record fails recovery with Corrupt at its LSN.
- [property] Random crashes keep acknowledged records: 50 seeds x 4 crash/reopen cycles; disk 1,024,
  4,096 or 65,536 blocks; max record 3-32 blocks; commit depth 1-2; max replay 4-200 blocks; run
  pieces unlimited or 512 bytes; random log ID. Each cycle runs 300 steps, each either submitting 1-4
  random items (60% writes of 1-3 blocks, 10% whole-chunk punches, 20% hot blobs under 3,000 B, 10%
  cold blobs under 6,000 B) when nothing is pending or with probability 2/5, or executing one random
  pending disk action. The stand-in checkpoints whenever one is due. Then, in 3 cycles of 4, a crash
  lands each unflushed block with probability 1/2; otherwise the process only restarts. Rebuild,
  with a crash before each of its writes and flushes with probability 1/4, each followed by a fresh
  start, then open; recovery's reads complete in random order. Properties: both succeed with the same
  durable LSN, between the one acknowledged before the crash and that plus the rounds in flight, and
  leave nothing unflushed; both give the same segment usage table, and with no round in flight at
  the crash the one the log held before it; every record holds only submitted entries and blobs; the index from open
  and from rebuild both equal the pre-crash index plus the records kept from rounds in flight; every
  mapped block reads back; both leave the same bytes to replay, and when nothing in flight was kept
  and the manifest is the newest, as many as before the crash. Coverage: more than 5 kept in-flight
  rounds, more than 5 dropped, more than 20 recoveries from a manifest, at least one NoSpace, depth 2,
  an erased header, a restart and a crash during recovery all occur.
- [fuzz] Bit flips anywhere in a log of 40 writes, cold blobs and checkpoints: rebuild and open either
  fail with Corrupt, CopyOnly or NotFormatted, or resume a log whose next write survives a restart.

## Superblock copy

- A bit flip in the log ID of block 0, or of the copy: recovery returns all records and rewrites the
  damaged one from the other, flushed before the first segment-header read (trace: read block 0, read
  the copy, write, linked flush, then the header reads).
- An unreadable block 0 (read error) is restored from the copy, flushed.
- Block 0 of zeros, or of another format ("EFI PART" repeated), or with a bit flipped in its magic or
  version, next to a valid copy is CopyOnly(BadMagic or BadVersion, copy). The disk may have been
  reused, so recovery reads nothing more, writes nothing and leaves restoring to the operator.
- Both copies damaged is NotFormatted.
- A reformat that crashes after the new block 0 is flushed but before the copy is written: block 0
  (new log) wins, the stale copy (old log) is rewritten to match, and every segment is free.

## Quarantined segments

- [property] 10 seeds; 4,096-block disk (4-block segments), 4-block records, max replay 40 blocks,
  300 writes of 1-2 blocks with checkpoints. Flip one random bit in the first 52 bytes of 3 random
  used segments' headers whose records all precede the replay point of the newest named manifest,
  and of 1 never-written segment. Open, its reads completing in random order, reports exactly the 3
  as quarantined, in segment order, and frees the other.
  Filling the disk until NoSpace leaves every quarantined segment's blocks unchanged and reuses the
  damaged free one; open then still equals the live index and reads back. Rebuild, which must
  replay the quarantined segments' records, gives Corrupt(their first LSN).
- Quarantine is detected by reading a damaged segment in full for valid record headers of this log
  (any segment sequence). A damaged header over a segment with no records counts as free.
- A damaged header over records past the replay point is corruption, since a torn write leaves a
  segment header whole or as it was: 64K-block disk, one-block writes. With 60 writes, damaging the
  header of LSN 1's segment gives Corrupt(LSN 1), and damaging the newest segment's header (several
  records) gives Corrupt(first LSN in it). With 64 writes the newest segment holds only record 64,
  which may have been acknowledged: Corrupt(LSN 64), and the segment is left unchanged.

## LSM tree

- Blocks (from Pebble's `sstable/rowblk` tests, a7b8321871e3): "apple", "apricot", "banana" with
  empty values encode to Pebble's golden bytes, and a seek to each of its 14 probe keys lands on the
  same entry; an empty block is one restart at 0. [property] Seed 31, 300 blocks of up to 100 keys
  across restarts: seek and floor, strict or not, agree with a sorted list. A truncated block,
  unsorted or repeated keys, an empty value, an unknown kind, or a restart past the entries is rejected.
- Get, replace and prefix scan: keys "ab", "a", "abc", "b", "aa": scan "a" returns a, aa, ab, abc in
  order; scan "ab" returns 2; inserting "b" again replaces its value and leaves 5 entries.
- [property] Seeds 0-11, 2,000-6,000 random puts and removes, keys and values up to the limits in
  every third seed so pages split at every level: gets, floors and a full scan read as a map, inner
  keys equal their child's smallest key, and every page in use is reachable.
- [property] Views: seeds 0-5, 15,000 writes with views opened and closed at random read as the map
  did when each opened; once all close no page is left copied away, and a write then copies nothing.
- Keys inserted in random order leave leaves at least 75% full (80% measured; 63% without
  redistribution in the Go memtable spike).
- [property] Flush to a run at any piece size: seed 11, 50 trees from up to 200 random inserts and
  removes (keys 1-40 B, values 0-59 B), max piece 123-4,105 B. Every piece is non-empty, within the
  limit and sized exactly, only the root lists references. Loading the pieces as a run into an empty
  tree reads as the original. A run needing more pieces than its root can list fails with TooLarge,
  checked to be so, in at most 15 trials.
- [property] A flush keeps its frozen view: seed 12, 200 trees from up to 300 mutations (keys 1-3 B
  over 5 letters, values 0-20 B, 1/4 removes), max piece 256-2,055 B. Up to 20 random inserts and
  removes run between pieces, on keys before and after the cursor, into the new memtable. The run
  equals the tree as of the freeze, the frozen pages are freed after the last piece, and the tree
  still reads as the map.
- A view opened during a flush keeps reading the frozen memtable after the flush ends, and its
  pages are freed only once the view closes.
- Memtable over runs: after a flush, a key only in the run reads from the run; a key written after the
  flush reads the memtable's value; a key removed after the flush reads as absent; a scan merges the
  memtable and runs in key order with no duplicates. With three runs and a flush running, a get of
  a key in every layer reads 5 layers: the memtable, the frozen one and each run once. A get of a
  key in no run searches only the memtables and the runs whose filter lets it through.
- Compaction: four runs with overlapping keys merge, on another goroutine while the memtable takes
  writes, into one run equal to their merge, newest value winning and removed keys dropped. A view
  opened before the compaction still reads the four runs; the next checkpoint names the merged run
  and reports every piece of the four dead.
- A compaction of four runs of 100 KB above a 10 MB run leaves that run out and keeps the
  four removals, which still hide its keys. A flush drops a removal no older run holds the key of,
  and keeps one that hides a key in a run.
- A compaction installed before it ran is Stale and the next is still picked; a bucket larger than
  a root of its pieces could list is never picked.
- A compaction installed while a flush writes its inputs: that checkpoint's manifest names the
  inputs, which restore to the tree, and the next names the merged run and reports the inputs dead.
- A checkpoint that fails after writing a compaction's result whole carries that run next time.
- Universal picker (ported from RocksDB's universal tests in `compaction_picker_test.cc`, run sizes
  newest first, trigger 4): 1,1,1,1 merges all four, size amplification firing first at 300%;
  1,10,100,1000 picks nothing, and 1,10,100,1000,10000 makes the run-limit fallback merge the newest
  two; 300,300,100 above 100 (newer runs 700% of the oldest) is a full size-amplification merge; a
  largest run of 64 MiB with a 1 MiB buffer gives an auto run limit of 8.
- A merge running over the oldest runs does not stop a merge of newer runs; installing both in
  either order leaves the runs in age order.
- Restore is runs plus replay: after six flushes, a compaction and more writes, loading the newest
  manifest's runs and replaying the later writes equals the live tree.
- Loading a run is Malformed when a piece or the root is truncated by a byte, a piece is missing or
  two are swapped, a piece names another run's sequence number, a key moves above the next one, or
  the run is loaded twice; a piece of another version is BadVersion. The loader is fuzzed.
- An entry larger than a piece, a key over 1,024 B or a value over 1,000 B is TooLarge, the largest
  entry fits; a piece size under 64 B is BadConfig.
- A failed checkpoint keeps the frozen memtable and reports the one piece it wrote of it as dead;
  writes go on into the new memtable, and the next flush writes the frozen one without freezing them.
- Run count: 2,000 flushes alternating 1.3 MB and 33 MB over a 44 MB index (and bursty and
  log-uniform sequences) never leave more than 2 + log2(largest run ÷ 1 MiB) + 1 runs, and the runs'
  bytes stay under 3× the live index after size amplification.
- Filters (from Pebble's `sstable/tablefilters/bloom` tests at a7b8321871e3): the filter of their
  test keys encodes to Pebble's golden bytes, and every added key may match. Over 1,000,000 absent
  keys, a 10 bits-per-key filter's false positives stay within 1.0 to 1.4%.
- [property] Seeds 0-11, random puts, removes, range removals, flushes and compactions, with filters
  on and off: gets, floors inside a prefix and prefix scans read alike and as the map. A run whose
  filter rules out a key is never searched for it.
- Over 24 runs, a get of a key no run holds searches on average fewer than 0.5 runs. A floor inside
  a chunk written whole searches only the run holding it, plus false positives. A restore loads each
  run's filter from its filter blocks, and lookups probe as before.
- [property] Range removals: seeds 0-59, 3,000 random puts, removals and range removals over 300 keys
  with flushes and compactions, with size tiering on and with every run under 1 MiB in one bucket:
  gets, floors and a full scan read as a map. A merge whose output drops its inputs' fragments, or a
  memtable that does not cut a later write's key out of a fragment, fails.
- A range removal over a run's keys, flushed: a get or a floor of any key in it probes each layer
  once; a write into the range after it reads back, before and after the flush; a fragment no older
  run has a key in is dropped by the flush.

## Chunk index

- [property] Key byte order equals logical order: seed 21, 5,000 random key pairs. Comparing encoded
  keys (pool | XXH3 of chunk | chunk | frozen version, 0 for the live map | first block, all
  big-endian, 38 bytes) matches comparing the tuple. Keys round-trip, a key whose hash names another
  chunk does not decode, and a chunk's entry sorts before its live map, which sorts before a frozen
  version's epoch and its map.
- Applying a durable record at LBA 100 (one header block) with a 2-block write of chunk 7 v1 at
  block 0, a blob, a 1-block write of chunk 7 v2 at block 5, and a whole-chunk punch of chunk 8 v3
  maps: chunk 7's live map to data at LBA 101 (blocks 0-1, v1) and LBA 103 (block 5, v2; writes are
  packed before blobs), both with header LBA 100, and its chunk entry to version 2; chunk 8's live
  map to Zero(1,024 blocks, header 100) and its entry to version 3. Blobs are skipped. Listing a
  chunk returns its live extents by first block; record 100 refers to 3 blocks.
- Trim and split: a 1-block write of chunk 7 v3 at block 1 leaves the live map as block 0 (v1,
  LBA 101), block 1 (v3) and block 5 (v2); a 4-block write v4 at blocks 0-3 leaves blocks 0-3 (v4)
  and block 5 (v2). The live counts of the records that lost blocks fall by the blocks trimmed, and
  the live map never holds two entries for one block.
- Freeze: a record with freeze(chunk 7, v4, epoch 2) and a 1-block write v5 at block 2 copies the live
  map under key version 4 before applying the write. A read of block 2 at snapshot epoch 1 finds v4's
  block, as does a read at epoch 2, whose cut the freezing write followed, the live read finds
  v5's, and a read at epoch 3 reads the live map. The chunk entry holds
  version 5 and epoch 2. The freeze applied again changes nothing. A rebuild from headers in LSN
  order gives identical live and frozen maps.
- A cleaner move (same chunk, same blocks, new location LBA 500: data 501) applies to every map, live
  or frozen, whose extent still points at the location it copied, splitting the extent around the
  moved blocks; once the maps point elsewhere it is dropped. A write that replaced the live block
  leaves only the frozen map to move.
- A record with an entry for pool 2^32 − 1, which the index keeps record counts under, or with a
  freeze of version 0 of a chunk that exists and holds no absent version, is refused whole.
- Removals: chunk 7 written as 1,024 one-block extents, checkpointed, then rewritten in one
  1,024-block write: the index holds one range removal for blocks 1 to 1,023 and no point removal, a
  lookup of every block takes at most two searches before the checkpoint, after it and after a
  compaction, and a 1-block write at block 700 splits the new extent.
- [property] Seeds 0-5, 1,500 random writes, punches, freezes and whole rewrites over 4 chunks, with
  checkpoints and compactions: every block reads as an index built with point removals does, and the
  invariants hold.
- Absent freeze: a record with freeze(chunk 9, v0, epoch 1) and a 1-block write v1 leaves chunk 9's
  entry at v1, epoch 1, and a read at epoch 1 finds nothing. A second write at epoch 1 carries no
  freeze. After cut 2, a write freezes v1 at epoch 2: a read at epoch 1 still finds nothing, and one
  at epoch 2 finds v1. The record applied again changes nothing.
- [property] Seed 22, 20 runs of 300 random writes, punches and freezes over 3 chunks of 64 blocks,
  with a cleaner move of a live or frozen block after one record in four: each move changes as many
  maps as the model holds the block at its location, every block, live and at every snapshot epoch,
  reads the version and location a model of whole-map copies holds, and the invariants hold after
  every step. The runs include snapshot cuts and chunks created after them, as the
  [chunk epochs spike](design.md#appendix-chunk-epochs-spike)'s E3 does.
- [property] Replay past pending writes (chunk epochs spike R1, 200 seeds): a primary that maps
  writes at commit, a chunk's in version order, with cleaner moves and checkpoints whose replay
  point is its oldest pending write, restarts from the checkpoint's runs plus every record from the
  replay point, and reads every block, live and at every snapshot epoch, as a rebuild does.
- [property] On a 1 GiB simulated disk (16-block records, a checkpoint every 96 blocks of records),
  seeds 0-3, three generations of 150 rounds of writes, punches and freezes, with checkpoints,
  multi-piece runs, carried runs and compactions: recovery from the manifest's runs plus replay, and
  a rebuild from every record header, both equal the live index.
- A crash after a checkpoint's runs are durable but before its manifest, with a compaction's result
  among them, recovers from the previous manifest plus replay to the live index, and the next
  checkpoint's restart does too.

## Segment usage table

The log below writes 60 one-block hot writes on a 64K-block disk (16-block segments), checkpointing
whenever 40 blocks follow the manifest.

- Zeroing the header of segment 1, whose records all precede the replay point, or all its 16
  blocks, or writing another log's segment header over it, quarantines it instead of freeing it: one segment fewer is free, open still equals the
  live index, and 16 more writes open new segments but leave segment 1's blocks unchanged. The next
  open quarantines it again.
- Zeroing the header of a segment the table lists that holds records past the replay point is
  Corrupt(the first of them).
- Zeroing the header of the newest hot segment, opened after the manifest and holding several
  records, is Corrupt(its first LSN), not a log that ends before it: its block 1 holds a record, so
  recovery reads it in full and the tail rule sees the records past the gap.
- A manifest whose table names a segment past the end of the disk, its record rewritten with valid
  checksums, is Corrupt(the manifest's LSN).
- A quarantine outlives the start that found it: a bit flip in segment 1's header is quarantined
  after reading the segment in full; once a checkpoint's table records it and a segment header
  names that manifest, open quarantines segment 1 again without reading past its header, and still
  does after its header is restored. Rebuild, which needs its records, gives Corrupt(LSN 1).

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

- Group commit: 100 seeds (every accepted item decodes), each also failing under five broken
  encoders, and 20 seeds of random entries (anything Submit accepts decodes).
- Random crashes: 50 seeds x 4 cycles (acknowledged records kept, index = rebuild), and the recovery
  fuzz test.
- Quarantine: 10 seeds (bit flips in segment headers).
- Record codec: seed 1 (round trip, 499 records), seed 4 (exhaustive torn subsets, 40 records x 3 old
  contents), seed 5 (torn 512-byte sectors, 300 records).

The chunk store stand-in of the recovery tests applies every Durable record to its model of the
chunk index. It starts a checkpoint whenever one is due, snapshots the model, hands one run piece at
a time, and the next only after the previous piece is durable, each piece naming the one before. It
ends the checkpoint after the last piece, and on CheckpointFailed stops checkpointing. Write payloads
are a pattern derived from (chunk, version, block), so any extent can be checked by reading it back.
