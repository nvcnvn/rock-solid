# Test scenarios

The scenarios the tests of phases 1 to 5 must cover, one section per [build checklist](build-checklist.md) item, in checklist order. A built item's section describes the tests as they are. An unbuilt item's section lists every case and requirement its tests must cover before it is reviewed, and where the item changes a built item's behaviour it quotes the built scenario and says what it becomes, for the change that builds the item to apply. The disk log's scenarios come from the tests of the first, Rust, implementation, and from the RocksDB, Pebble, etcd, Cassandra and Protocol-Aware Recovery (FAST 2018) tests those were ported from, named next to each scenario; the others come from [design.md](design.md), its appendices and the spike and study READMEs each section names. They describe behaviour, not an API.

A block is 4 KiB. A "1 GiB disk" is 262,144 blocks: 4,096 segments of 64 blocks (256 KiB), with the superblock copy at block 63. Sizes that follow from the record layout (a 48-byte header prelude, 33 bytes per write or punch entry, 37 per freeze, 14 per blob, 4 bytes of CRC per data block) move with the layout if the Go code changes it; the behaviour does not.

- **[property]** marks a randomized or exhaustive test, with its seeds so a run replays. Unless a line names its seeds, a new seeded test runs 10,000 seeds once and thousands per commit, as [Testing strategy](design.md#testing-strategy) asks.
- **Open:** marks a point the design does not settle yet. It is settled in design.md, and the scenario written, before the item is reviewed.
- **Benchmarks** lists a section's hot-path benchmarks, each of which must report zero allocations per operation unless the section names its exemption.
- Settings take their design.md defaults unless a scenario says otherwise: `primary_lease` 4 s, `lease_drift` 1% of the lease, `rate_window` 10 s, `rate_gap` 100 ms, `max_margin` 50%, `node_down_after` 5 s, `member_timeout` 1 s, a heartbeat every second, `session_lease` 10 s, `recall_timeout` 30 s, `writeback_buffer` 256 MiB per node, `write_limit` 90%, `raft_heartbeat` 100 ms, `raft_election_timeout` a random 1 to 2 s, `max_replay_bytes` 64 MiB, `max_group_commit_bytes` 1 MiB, `commit_depth` 1, `compaction_step_bytes` 64 MiB, `clean_free` 16, `shard_split_size` 512 MiB, `statfs_cache` 5 s, `max_file_size` 16 TiB.

## Module, toolchain and lint

- `go.mod` declares one module, `github.com/nvcnvn/rock-solid`, with `toolchain go1.27.0`, and
  `scripts/lint.sh` runs every step with `GOTOOLCHAIN` set from that line, so a newer local Go
  changes nothing. The toolchain and the three tool versions are pinned in
  [tech-debts.md](tech-debts.md#version-pin-go-toolchain-1270-and-lint-tools).
- The script fails, naming the cause, on each of these planted faults: a Go file outside `spikes/`
  and `studies/` that `gofmt` would change (the file is listed); a `go vet` finding; a sans-IO analyzer finding
  ([next section](#sans-io-analyzer)); a `staticcheck` (v0.8.2) finding; a vulnerability
  `govulncheck` (v1.9.0) finds in code the module calls (its exit status 3); a dependency whose
  license is not Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Zlib, CC0-1.0 or Unicode-3.0
  (`go-licenses` v2.0.1).
- `govulncheck` that cannot run at all (no network: any exit status but 0 and 3) prints a warning and
  skips; every other check still decides the result.
- Go files under `spikes/` and `studies/` are never linted, tracked or not; untracked Go files
  elsewhere are. With no Go file outside them, the script prints "nothing to lint" and passes.

## Sans-IO analyzer

`tools/sansio` runs under `analysistest` over planted packages in `tools/sansio/testdata`; every
finding is a `// want` line with its message, and a package with none must report nothing.
`scripts/lint.sh` runs it through `go vet -vettool` over `./...`.

| Planted code | In `internal/core` and its subpackages (sans-IO) | Accepted in |
| --- | --- | --- |
| `range` over a map, or over a named map type | Rejected: "iteration order is random; iterate sorted keys" | test files, `internal/loop`, `internal/driver/uring`, `internal/csi`, `cmd/rock-solid` |
| `time.Now` (also taken as a value), `Since`, `Until` | Rejected: "use ctx.Now" | the same |
| `time.Sleep`, `After`, `AfterFunc`, `NewTimer`, `NewTicker`, `Tick` | Rejected: "emit an Action with a TimerID" | the same |
| A `go` statement | Rejected | the same |
| A channel type (parameter or `make`), a send, a receive (also in a `select` case) | Rejected, each one | the same |
| A `select` statement | Rejected | the same |
| Import of `sync` or `sync/atomic` | Rejected: "a machine runs on one loop" | the same |
| Import of `os`, `os/exec`, `io/fs`, `net`, `net/http`, `syscall` or `golang.org/x/sys/unix` | Rejected: "emit an Action for I/O" | the same (a test file reads `testdata` with `os`) |
| Import of `unsafe`, `runtime`, `context`, `crypto/rand` or `math/rand` (v1) | Rejected, each with its reason | `unsafe` on the data path and in unchecked packages |
| A global `math/rand/v2` draw: `rand.Uint64`, `IntN`, `N`, `Float64`, `Shuffle` | Rejected: "draws from the global source; use a seeded core.Rand" | the same |
| Import of `log` or `log/slog`; a call of `fmt.Print*` or `fmt.Fprint*` | Rejected: "record a trace event" | test files, `internal/csi`, `cmd/rock-solid`; on the data path (`internal/loop`, `internal/driver/…`) rejected too: "the data path emits no free-form logs" |

- Accepted in a sans-IO package: `rand.New` over `rand.NewPCG` or `rand.NewChaCha8` seeded by the
  machine; ranging over `maps.Keys(m)`, `slices.Sorted(maps.Keys(m))`, a slice or an integer;
  `time.Duration` arithmetic; `fmt.Errorf` and `fmt.Sprint`; a `panic` on a broken invariant. On the
  data path `fmt.Sprintf` is accepted.
- Package classes, by import path: `internal/core`, its test variant (`internal/core
  [internal/core.test]`), `internal/sim/disk` and `internal/loopback` (a path component must match,
  not a string prefix) are sans-IO; `internal/loop`, `internal/driver/uring` and
  `internal/frontend/fuse` are data path; the generated `internal/core.test`, `internal/csi` and
  below, `cmd/…`, `tools/…` and another module's `internal/core` are unchecked.

## Event and Action types

- Inline control messages hold 0..256 bytes, an empty one included; 257 bytes is rejected with an
  error giving the length ("inline message too long: 257 bytes"). Bulk data travels only as pool
  buffer handles (`IOBuf`: a slot and a generation) attached to a message or a disk action, never
  inline.
- A `DiskWrite` gathers a vector of (pool buffer, length) pairs into one write, so group commit
  writes producers' buffers in place ([Group commit](#group-commit)); a `DiskFsync` marked linked
  runs only once the `DiskWrite` just before it completes. A `PeerLost` failing a peer's
  outstanding requests is under [Peer requests](#peer-requests).
- 1 tick = 1 µs: 2 s = 2,000,000 ticks = 2,000 ms; 15 ms = 15,000 ticks and back; 950 ticks
  truncates to 0 ms.
- Pool exhaustion is an error value that `errors.Is` matches, carrying the bytes requested and
  available: "buffer pool exhausted: requested 1 bytes, 0 available".
- Building an event and appending two actions to a reused slice allocates nothing (1,000 runs).

## Buffer pool interface

- Explicit capacity: a pool of 2 blocks hands out two 1-block buffers in different slots, reports
  used = capacity and available = 0, and refuses a third 1-byte request with PoolExhausted(requested
  1, available 0).
- Requests round up to whole blocks: in a 3-block pool, 4,097 bytes take 2 blocks; a 2-block request
  is then refused with PoolExhausted(requested 8,192, available 4,096), and a 1-byte request gets the
  last block (slot 2).
- Freeing a buffer returns its bytes; the next allocation reuses the same slot with a new generation
  (slot 0 generation 1, then slot 0 generation 2), so the old handle and the new one differ. Used and
  live-buffer counts return to 0 after all frees.
- A fresh (non-zeroed) allocation is filled with poison byte 0xA5, never with the previous owner's
  data and never with zeros; the zeroed allocation returns all zeros. Code that assumes zeros must fail.
- A pool limited to 1 live buffer (slot limit) refuses a second allocation with available = 0 even
  though bytes remain; after a free it allocates again.
- Reading or freeing a freed handle ("freed", slot 0 generation 1), or a stale handle whose slot has
  been reallocated ("stale", generation 2 against 3), is a hard error (the simulator pool panics),
  never a silent read of someone else's buffer, and the rejected free leaves the counts as they were.
  The zero handle and a slot past the pool panic too.
- Exhaustion is an ordinary outcome, not a crash: a writer machine with a 1-block pool issues its first
  write; a second write at t=105 cannot allocate and arms its retry timer for t=115 (delay 10); the
  timer firing while still exhausted re-arms for t=125; the first DiskDone frees the buffer and the
  queued write goes out reusing slot 0 at generation 2; after the last DiskDone no buffers are live.

## Seeded RNG

- Two generators with the same 64-bit seed (0xDEADBEEFCAFEBABE) yield identical streams across u64,
  u32, bounded draws (below 97), inclusive ranges (1000..2000) and 13-byte fills, over 256 iterations.
- Golden stream: the first four u64 draws for seed 0 are checked in. A change to the generator breaks
  replay of every recorded regression seed, so it must fail this test first.
- Fork: a child drawn from a parent (seed 42) is deterministic (two parents with the same seed give
  equal children and equal subsequent parent draws) and its stream differs from the parent's.
- Bounds: below(0) and below(1) return 0; range(5..=5) returns 5; chance(0/10) is false and
  chance(10/10) true; 512 draws of range(10..=20) stay in range. Bounded draws are unbiased: 30,000
  draws of below(3) (seed 3) give each value 9,000 to 11,000 times.
- Shuffle (Fisher-Yates) of 0..16 with the same seed (99) gives the same permutation, and it is not
  the identity.

## IDs and hashing

- Inode IDs pack a 24-bit slot (high bits) and a 40-bit sequence: block-volume inode = 0 (slot 0, seq 0);
  root inode = 1 (slot 0, seq 1); slot-metadata ID for slot 0x123456 is 0x1234560000000000 (seq 0).
- Inode (slot 0xABCDEF, seq 0x9876543210) encodes big-endian as AB CD EF 98 76 54 32 10 and
  round-trips; slot > 2^24-1 or seq > 2^40-1 is rejected.
- Chunk IDs pack (volume u32, inode u64, index u32) into 128 bits, big-endian on disk:
  (0x01020304, 0x05060708090A0B0C, 0x0D0E0F10) encodes as bytes 01..10 and round-trips via bytes;
  appending it to a buffer gives the same 16 bytes.
- Offset to chunk: chunks are 4 MiB (1,024 blocks). Offsets 0 and 4 MiB-1 give index 0, 4 MiB
  gives index 1. The largest offset (2^32 x 4 MiB - 1) gives index 2^32-1; one byte more is an
  OffsetOverflow error carrying the offset (16 PiB limit).
- Placement-group count must be a non-zero power of two (0, 3, 6 and 2^31+1 rejected); with 1 PG
  every hash maps to PG 0. The PG index is the top log2(count) bits of the hash: the all-ones hash
  is PG 63 of 64, and 2^63 is PG 1 of 2.
- [property] PG splitting keeps hash ranges contiguous: for 64 random chunk IDs (seed 7) and every
  count 1..2^10, doubling the PG count maps PG p to 2p or 2p+1.
- Golden vectors (`internal/core/testdata/chunk_id_golden.txt`, 10 rows, frozen by the design's key
  layout): for each (volume, slot, seq, index) the packed 128-bit ID, its XXH3-64 (seed 0, over the
  16 big-endian bytes), and its PG index at 64 and at 2048 PGs all match. Rows include all-zero
  (hash 0xd0a66a65c7528968, PGs 52/1669) and all-ones (0x722a202f07a4b313, 28/913). The fixture is
  frozen disk format and is never edited, and the test fails unless it reads exactly 10 rows.
- Benchmarks: hashing a chunk ID.

## CRC32C

- RFC 3720 appendix B.4 vectors (Castagnoli): "123456789" gives 0xE3069283; 32 zero bytes give
  0x8A9136AA; 32 x 0xFF give 0x62A8AB43; 32 incrementing bytes give 0x46DD794E; 32 decrementing
  bytes give 0x113FDB5C; the 48-byte iSCSI READ(10) PDU gives 0xD9963A56. The checksum is stored
  little-endian (e.g. AA 36 91 8A), and appending it writes those 4 bytes. The stored value reads
  back as the data's checksum, and a stored value+1 does not.
- Continuing a finished CRC over the rest equals the one-shot CRC of a 4,096-byte payload (byte i =
  i*31 mod 256) for split points 0, 1, 7, 16, 63, 64, 511, 2048, 4095, 4096.
- Benchmarks: the CRC32C of one 4 KiB block, whose bar is the 167 ns the design relies on on
  x86-64, and 359 ns on arm64 (an Apple M4 Pro).

## Lease clocks in the context

These tests add a clock and an event kind to the types of [Event and Action
types](#event-and-action-types), and this section is their only home.

- The context carries two clocks in ticks, `Now` (`CLOCK_BOOTTIME`) and `Raw`
  (`CLOCK_MONOTONIC_RAW`), set independently. A context with `Now` 1,000 and `Raw` 1,226 reads back
  both, since a slewed `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW` drift apart. So does one with
  `Now` 5,000,000 and `Raw` 4,000,000: neither clock is derived from the other, and neither has to
  be ahead.
- `Raw` 0 with `Now` non-zero is a valid context, and so is the reverse; neither is an error.
- `Resumed` is an event kind of its own and needs no other field: a `Resumed` event with peer,
  message, I/O ID, error and timer all zero is well formed.
- The sans-IO analyzer accepts reads of `ctx.Raw`, as it accepts `ctx.Now`.
- Every existing machine and harness sets both clocks, with `Raw` apart from `Now`: group commit
  (its tests and benchmark), recovery, peer requests, the chunk store's recovery stand-in, the
  buffer pool's test writer and the simulated disk's harness. Each test sets `Raw` to a value other
  than `Now` in at least one step. Its action trace is the same as with `Raw` = `Now`, and the same
  again with `Raw` = `Now` × 1.226 at every step, because these machines' deadlines are on `Now` and
  timers run on `CLOCK_BOOTTIME`.
  - Peer requests example: a send at `Now` 100 with timeout 50 has its deadline at 150 whatever
    `Raw` holds. A `Timer` at `Now` 150 with `Raw` 10 times the request out.
- A machine that holds no lease gets `Resumed`, emits no action and keeps its state: an outstanding
  peer request keeps its deadline, and group commit keeps its rounds in flight.
- Building a context with both clocks and a `Resumed` event, then appending two actions to a reused
  slice, allocates nothing (1,000 runs), and the hot-path benchmarks still report 0 allocations per
  operation.

## Peer requests

- Send records the request and returns exactly two actions: send the message, then arm the
  caller-chosen timer at now + timeout (e.g. t=100, timeout 50 gives deadline 150). Sending with an
  ID or timer that is already in flight is a programming error (a panic naming the request).
- PeerLost for peer A fails every outstanding request to A, in request-ID order, each reported with
  its peer, timer, deadline and data buffer (buffer or none), reason PeerLost. Requests to peer B are
  untouched, even one carrying the same buffer, which still times out at its own deadline (170). A
  later timer for a request that PeerLost already failed reports nothing.
- Timeout keeps the buffer: a fire before the deadline (149) reports nothing; when request 1's timer
  fires at its deadline (150), it is reported once with reason Timeout but stays tracked (in-flight
  count unchanged), because the driver may still hold its buffer. A repeat of that timer reports
  nothing. A reply ("complete") then removes it and hands back its buffer.
- A timed-out request that never gets a reply is failed by a later PeerLost together with the other
  requests to that peer, buffer included; afterwards nothing is in flight.
- A timer ID can be reused after its request timed out: a new request on that timer is tracked and
  PeerLost fails both the timed-out one and the new one.
- Completed request: completing returns it once (a second completion returns nothing); a later timer
  or PeerLost for it reports nothing; a timer that guards no request reports nothing.
- Stale timer: request 1 on timer T is completed, request 2 reuses T with deadline 180. The stale fire
  at 150 (request 1's deadline) is ignored because now < request 2's deadline; the fire at 180 times
  out request 2. Timers are matched to the current request and its deadline, not just the timer ID.
- A send and a completion allocate nothing (1,000 runs). Benchmarks: send then complete.

## Segments and superblock

- Segment size = min(64 MiB, disk / 4096) rounded down to a power of two, at least 4 blocks; the count
  is disk / segment size and must be at least 4. Expected (disk, segment size, count):
  16 TiB, 64 MiB, 262,144; 1 TiB, 64 MiB, 16,384; 256 GiB, 64 MiB, 4,096; 255 GiB, 32 MiB, 8,160;
  16 GiB, 4 MiB, 4,096; 1 GiB, 256 KiB, 4,096; 1 GiB + 3 blocks, 256 KiB, 4,096; 64 MiB, 16 KiB, 4,096;
  1 MiB, 16 KiB, 64; 64 KiB, 16 KiB, 4. A 15-block disk is DiskTooSmall, and an 8 EiB one
  DiskTooLarge.
- Addressing: block b of segment s is at LBA s x segment_blocks + b (1 GiB: segment 3 block 5 is
  LBA 197). Segment 0 holds the superblock in its first block and its copy in its last block (LBA 63).
- Superblock (magic, version, CRC, log ID, segment blocks, segment count) and segment header (magic,
  version, stream, CRC, log ID, open sequence, own index, newest manifest position or none)
  round-trip, hot and cold, with and without a manifest pointer, each as one 4 KiB block. Segment 0
  in the manifest fields means "no manifest". A superblock decodes from a longer buffer, ignoring
  what follows its block.
- [property, exhaustive] Flipping any single bit of the segment header makes it fail to decode.
- Damage, with the checksum recomputed where it says so:

  | Damage | Superblock | Segment header |
  | --- | --- | --- |
  | All zeros, or another magic | BadMagic | BadMagic |
  | Another version, checksum recomputed | BadVersion | BadVersion |
  | A bit flipped (superblock: in the log ID, the CRC or the last byte; segment header: in the sequence) | BadChecksum | BadChecksum |
  | Non-zero padding or tail, checksum recomputed | Malformed | Malformed |
  | Segment blocks 3 or 96 (not a power of two of at least 4), or a count of 3, checksum recomputed | Malformed | |
  | Stream 2, own index 0, manifest block 0, or an LSN with no manifest segment, checksum recomputed | | Malformed |
  | Shorter than a block | Truncated | Truncated |

  Encoding refuses what decoding would: a superblock of 6-block segments, a segment header of
  stream 2, index 0, a manifest block with no segment, or a manifest segment at block 0.
- Golden bytes: the superblock for log ID 0x0123456789abcdef on a 1 GiB disk, and the segment header
  (seq 7, segment 3, cold, manifest at segment 2 block 9 LSN 41), encode to the checked-in
  `internal/disklog/testdata/log_format_golden.txt`, followed by zeros to the end of the block.
  Until the first release a format change replaces the fixture in place; after it, a change needs a
  version bump.
- [fuzz] Both decoders: anything they accept re-encodes to the same block.

## Record codec

- Layout: a record is header blocks then data blocks. The header holds a 56-byte prelude, 33 bytes
  per write/punch entry, 37 per freeze, 14 per blob entry, and a 4-byte CRC32C per data block, zero
  padded to whole blocks. Data holds each write's blocks in entry order, then blob payloads packed
  back to back.

  | Entries | Header bytes | Header blocks | Data blocks |
  | --- | --- | --- | --- |
  | A 1-block write | 93 | 1 | 1 |
  | A punch | 89 | 1 | 0 |
  | A freeze | 93 | 1 | 0 |
  | An empty blob | 70 | 1 | 0 |
  | Blobs of 1 and 4,095 bytes | 88 | 1 | 1 |
  | A blob of 4,097 bytes | 78 | 1 | 2 |
  | A 1,001-block write | 4,093 | 1 | 1,001 |
  | A 1,002-block write | 4,097 | 2 | 1,002 |
  | A 1,003-block write | 4,101 | 2 | 1,003 |
  | A 1,004-block write | 4,105 | 2 | 1,004 |
  | 200 one-block writes | 7,456 | 2 | 200 |

  A 1-block write is a 2-block record. Payload offsets, for a blob of 5 bytes, a 2-block write, a
  punch, a blob of 7, a 1-block write and a freeze: 12,288 (after the 3 write blocks), 0, 12,293,
  12,293, 8,192, 12,300.
- An extent of 0 blocks, of 1,025, of 1,024 from block 1, or a punch at block 1,024 is BadExtent; a
  blob of 2^60 bytes, or blobs adding up past the data a record can hold, is TooLarge.
- An entry with a field its kind does not encode set (a freeze with an owner, a punch with an epoch,
  a write with a length, a blob with a version) is Malformed, since it would not decode to itself.
- A header with 200 one-block writes from 200 pools and chunks spills into 2 header blocks (200 data
  blocks) and still round-trips. A header encoded from the data blocks' CRCs equals the record's;
  one CRC short, or data shorter than the entries, is Malformed. Decoding into a slice that already
  holds entries appends after them.
- [property] Round trip: seed 1, LSNs 1..499, random log IDs, sequences and offsets, random records
  of 1-5 entries (writes up to 3 blocks, punches up to 512 blocks, freezes, blobs under 8 KiB, at most
  8 data blocks) with random payloads decode to the same header, entries and LSN, and each blob's
  extracted payload has its declared length.
- [property, exhaustive] Torn writes decode whole or not at all: seed 4, 40 records of at most 10
  blocks; for every subset of landed blocks (2^blocks) over three kinds of old content (zeros,
  random bytes, a different valid record at the same position), decoding returns either exactly the
  new record (only if every block landed) or exactly the old record (only if the image is still the
  old one), never a mixture, and never rejects a fully landed record. More than 1,000 cases.
- [property] Torn 512-byte sectors: seed 5, 300 records over random old content, each 512-byte
  sector landing with probability 1/2: decode returns the exact record if every sector landed and an
  error otherwise, and some record is torn.
- Any single corrupted byte (XOR 0x10 at every offset of a 1-write, 2-data-block record) is rejected
  (RocksDB ChecksumMismatch).
- A freeze entry (pool 1, chunk (2,3,4), version 5, snapshot epoch 9) round-trips, adds no data block,
  and may share a record with writes, punches and blobs.
- Damage to the golden record below, with the header checksum recomputed unless it says "a bit":

  | Damage | Error |
  | --- | --- |
  | Another magic, or all zeros | BadMagic |
  | Version 2 | BadVersion |
  | A bit of the header's LSN | BadChecksum |
  | A bit of a data block, or a non-zero byte in the blob padding of the last data block | BadChecksum |
  | Entry tag 100 or 0 | Malformed (RocksDB BadRecordType) |
  | Header-block count 0, or 2 for a 1-block header | Malformed (RocksDB BadLength) |
  | Data-block count 3 for 2, entry count 0, 2 or 4 for 3 | Malformed |
  | A durable LSN of 10 or 11 for LSN 10 | Malformed (encoding refuses it too) |
  | A non-zero byte in the header padding | Malformed |
  | A write of 0 blocks, a punch running past block 1,024 | BadExtent |
  | Another log ID, segment sequence or offset | Mismatch |

- Truncated images (0, 4, 56, 4,096 bytes, and all but the last byte) are rejected
  (RocksDB TruncatedTrailingRecord); the header block alone still decodes as a header.
- A record longer than the blocks left in its segment is BadLengthAtEnd (as RocksDB names it): the
  3-block golden record decodes with 3 blocks left and not with 2.
- A record read back under a different expected segment sequence, log ID or offset is a Mismatch, so
  records of an earlier incarnation of a recycled segment are never accepted (RocksDB Recycle).
- Golden bytes: a record (log ID 0x0123456789abcdef, segment seq 9, offset 1, LSN 10, durable LSN 9) with a 1-block
  write (pool 1, chunk (2,3,4), version 5, block 6, data block filled with 0xAB), a whole-chunk punch
  (version 6) and a 3-byte manifest blob ("abc", owner 7) encodes to a checked-in fixture, followed by
  zeros to the end of the header block. Its entries sit at bytes 56, 89 and 122, and a write's
  extent (first block, count) closes its entry.
- [fuzz] A record the decoder accepts re-encodes to the same bytes and spans its layout's blocks.
- Benchmarks: encoding and decoding a 16-entry record and its header alone.

## Group commit

- Format takes each round slot's header and blob buffers from the pool for the log's lifetime
  (with 1 MiB rounds on 1 GiB: 64 + 63 blocks per slot), writes the superblock to block 0, flushes,
  then writes the same block as the copy at the last block of segment 0 (LBA 63), flushes, and is
  idle. IO IDs count up from the caller's first ID. A pool too small for the slots' buffers (100
  blocks for 127) fails Format with PoolExhausted and leaves nothing allocated; a depth below 1 or
  rounds below 3 blocks are BadConfig. Close returns every buffer, and refuses while a write or flush
  is in flight.
- One write and one flush per round: the first submit starts round 1 at once; four items submitted
  while it is in flight (a 2-block write, blobs of 100 and 5,000 bytes, a whole-chunk punch) all go
  into round 2. Disk trace: write, linked flush, write, linked flush. Durable reports LSN 1 with the
  first ticket and LSN 2 with the four tickets in submit order and their payload offsets in the
  data area (0, 8,192, 8,292, 13,292). Every producer buffer is released.
- No block is copied: the 2-block write goes out from the buffer its producer submitted, as an
  element of the round's vectored write, beside the log's header buffer and the blob buffer the two
  blobs are packed into.
- Opening a segment: round 1 writes the segment header (segment 1, seq 1, hot, no manifest) and the
  record together, starting at segment 1 block 0 (LBA 64); the record sits at block 1 (LBA 65) and
  the data is the submitted bytes. Round 2 appends at block 3 (LBA 67) with LSN 2 and 4 entries.
- Streams alternate when both have work, and each opens its own segment: with hot items A, B and a
  cold item queued behind round 1 (A), round 2 is the cold item (segment 2, LBA 128, seq 2) and
  round 3 is hot B plus a later hot item (LBA 67). LSNs are global across streams.
- Rounds stop at the maximum round size, segment header included: with 3-block rounds an item's
  record may take 2 blocks, so a 3-block write and a blob over 1 block (4,097 bytes) are refused as
  TooLarge at submit; 1-block writes pack two per record once no segment header is needed (LSN 2 =
  two items, LSN 3 = the third).
- Full segment rolls, full disk fails: a 16-block disk has 3 usable 4-block segments and no reserve;
  three 2-block writes fill one segment each (LBAs 4, 8, 12). The next write, even 1 block, is held
  with no disk action, and fails with NoSpace naming its ticket, its buffer freed, once the owner
  reports that the cleaner has no victim (`FailStalled`), as the harness does whenever the log is
  stalled. NoSpace fails the stream's whole queue, items behind its head that would fit included,
  so items are never written out of submit order.
- A full pool never stalls a round: with the pool holding only the log's buffers and eight 1-block
  writes, so that nothing is free, all eight become durable in submit order.
- A write or punch with zero blocks, or running past block 1,024 of its chunk (a punch of 25 blocks
  at block 1,000), is refused (BadExtent) with no action and no event.
- A refused item leaves its buffer with the caller: BadBuffer for a write buffer that is not exactly
  its blocks, a missing write buffer, a blob longer than its buffer, or a punch with a buffer;
  ReservedKind for a manifest or manifest-piece blob, which only the log writes.
- A disk error fails the log: an Io error on round 1's write reports its ticket InDoubt and the
  queued items Failed, frees their buffers, and every later submit is refused with that error.
  With depth 2, an error on round 2's write while round 1's is in flight reports both rounds
  InDoubt (round 1 may still land) and the queued item Failed; Close is Busy until round 1's write
  completes and returns its buffers.
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
- [property] Every accepted item ends durable in a record that decodes to it: seeds 1-100; max round
  3..64 blocks; disk 16 blocks (1 in 4) or 1 GiB; commit depth 1-3; max replay 4-200 blocks; a pool
  of the log's buffers plus a round to 512 blocks; punch share 0-8 of 8; 1-1000 items to random
  streams, 1 in 8 sized near the round limit; a checkpoint of owners 1 and 2 with 0-3 random pieces
  whenever one is due; 1 seed in 8 fails one random disk action with Io; completions interleaved at
  random, a linked flush running only after its write. Every accepted ticket ends Durable, Failed
  (NoSpace or Io) or InDoubt (Io); a Durable record read back from the disk holds its tickets'
  entries and payloads at the reported offsets, and the log's own manifest pieces carry NoTicket;
  no record is reported durable before a flush that ran after its write completed; LSNs are
  reported in order; every finished checkpoint's manifest reads back with its base LSN and runs;
  without an error there is one flush per write; the only refusal is TooLarge; Close then finds
  every buffer back. Coverage: NoSpace, TooLarge, a full pool, InDoubt, deferred flushes, depth
  above 1, manifest pieces, finished and failed checkpoints all occur.
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
- The next segment opened after a checkpoint (segment 3, opened by a 60-block write) carries the
  manifest's position in its header.
- Manifest in pieces: with 3-block rounds (4 KiB max blob), a checkpoint of 500 runs writes the
  manifest as several pieces, the last naming the others, and the next segment header points at
  the last piece.
- A checkpoint on a full disk with no reserve: its piece waits, and the no-victim report fails it
  with NoSpace and emits CheckpointFailed(NoSpace). End then says NoCheckpoint, and a new
  checkpoint can begin.
- Manifest encoding: a manifest (base LSN 77, two indexes: owner 1 with one run of one piece and
  replay LSN 70, owner 2 with no runs and replay LSN 77) round-trips, at the size it reports. A
  trailing extra byte, or a truncation to 10 bytes, is Malformed, and another format version
  BadVersion. Each blob reference adds 18 bytes. A run of several pieces is one reference, to the
  piece that lists the others. A manifest naming one owner twice, or a replay LSN past its base LSN,
  is Malformed.
- A manifest's last piece (the format version, a 2-byte count, an 18-byte reference to each earlier
  piece, then the rest of the manifest's bytes) round-trips at the size it reports; a one-piece
  manifest has no references; a truncated list is Malformed and another version BadVersion.
- [fuzz] Both manifest decoders: anything they accept re-encodes to the same bytes.
- Benchmarks: a round of eight 1-block writes and a 300-byte blob, from submit to the flush's
  completion.

## Write-side decode check

- With `Check` on, as in the package's tests, the chunk index's tests and the simulator, group
  commit decodes every record it is about to write, segment header included, and panics with
  "decode check" unless the record holds exactly the queued items, entry by entry, each payload at
  the offset Durable will report. A round that would build a record with no entries panics. The
  default configuration leaves `Check` off.
- The decode check catches a broken encoder: an encoder that writes a wrong version into an entry
  and fixes the header checksum, flips a padding bit, or writes the segment header of the other
  stream with its checksum fixed panics in the decode check on every one of the group commit
  property's 100 seeds, as does a round that reports its last payload a byte late, or writes one
  block more than its record.
- [property] Anything Submit accepts decodes: seeds 1-20; max round 3..64 blocks; commit depth 1-3;
  300 items each of a random kind tag (unknown ones included) and random field values, half with one
  field set beyond what its constructor sets, with a buffer of its size, a block off, or none. Each
  item is either refused with Malformed, BadExtent, TooLarge, BadBuffer or ReservedKind, all of which
  occur, or ends Durable in a record that reads back as exactly its entry and payload; writes,
  punches, freezes and blobs are all accepted. Regression (seed 1): a freeze entry carrying an owner
  was accepted and decoded without it.

## Segment usage table

- The table round-trips with the manifest (segment 1 used, 40 live blocks, newest LSN 77; segment 3
  quarantined; segment 4 used and empty), and each row adds 17 bytes. A row for segment 0, rows out
  of segment order or naming one segment twice, a free or unknown state, or a newest LSN past the
  base LSN is Malformed.
- A row counts the data blocks of durable records as live, never their headers (6 for two 3-block
  writes in [Group commit](#group-commit)'s checkpoint), and a used segment left empty by rounds
  recovery dropped counts none (the segment-opening crash in [Recovery](#recovery)).

The log below writes 60 one-block hot writes on a 64K-block disk (16-block segments), checkpointing
whenever 40 blocks follow the manifest.

- Zeroing the header of segment 1, whose records all precede the replay point, or all its 16
  blocks, or writing another log's segment header over it, quarantines it instead of freeing it:
  one segment fewer is free, open still equals the live index, and 16 more writes open new segments
  but leave segment 1's blocks unchanged. The next open quarantines it again.
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
  does after its header is restored. Rebuild, which needs its records, reports the gap the table
  cannot explain: Corrupt(LSN 1).

## Recovery

All recovery scenarios run against the simulated disk, through the chunk store stand-in described
under [Seeded runs](#seeded-runs-and-the-simulation-harness), whose "index" is a model of where each
live block of each chunk sits. "Open" = recovery in open mode: load the newest named manifest, read
the model's run through recovery, replay the records after its replay LSN, resume group commit.
"Rebuild" = recovery that ignores manifests and replays every record. A recovery that fails leaves
no buffer allocated.

- Empty log: a freshly formatted disk recovers with no records and no manifest; the first new write
  gets LSN 1 (RocksDB Empty).
- An all-zero disk is NotFormatted.
- Reformat (recycle): a disk holding log 3's six writes and a cold blob is formatted as log 4 with one
  write. Recovery returns only that record (log ID 4), and all segments but segment 0 and the new
  log's are free, the old log's valid headers included (RocksDB Recycle).
- Torn tail: 3 records of 2-block writes, then a 4th round written but not flushed (LBAs 74-76). A
  crash landing none, {74}, {74, 75}, {75, 76} or {74, 76} recovers LSNs 1-3, and the index equals the
  index before the 4th write (RocksDB TruncatedTrailingRecord, PartialLast).
- An unflushed but fully landed last record ({74, 75, 76}) is kept (LSNs 1-4), holding exactly the
  submitted write, although it was never acknowledged.
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
  instead of replaying the old record. Within one stream too: a copy of record 2, its offset and
  checksum fixed, placed after record 4 gives Corrupt(LSN 2) instead of being skipped.
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
  reference that does not name a blob of a durable record fails recovery with Corrupt at its LSN, and
  a Replay after that is refused with the same error.
- [property] Random crashes keep acknowledged records: seeds 1-50 x 4 crash/reopen cycles; disk 1,024,
  4,096 or 65,536 blocks; max record 3-32 blocks; commit depth 1-2; max replay 4-200 blocks; run
  pieces unlimited or 512 bytes; random log ID. Each cycle runs 300 steps, each either submitting 1-4
  random items (60% writes of 1-3 blocks, 10% whole-chunk punches, 20% hot blobs under 3,000 B, 10%
  cold blobs under 6,000 B) when nothing is pending or with probability 2/5, or executing one random
  pending disk action. The stand-in checkpoints whenever one is due. Then each cycle ends in a crash
  with probability 3/4, landing each unflushed block with probability 1/2; otherwise the process
  only restarts. Rebuild runs first, with a crash before each of its writes and flushes with
  probability 1/4, each followed by a fresh start; then open, with no write between the two;
  recovery's reads complete in random order. Properties: both succeed with the same durable LSN,
  between the one acknowledged before the crash and that plus the rounds in flight, and leave
  nothing unflushed; both give the same segment usage table, and with no round in flight at the
  crash the one the log held before it; every record holds only submitted entries and blobs; the
  index from open and from rebuild both equal the pre-crash index plus the records kept from rounds
  in flight, so a second recovery after the first, or after a crash inside its writes, gives the
  same LSN and index (RocksDB "column family inconsistency" on second recovery, "SST file is ahead
  of WALs"); every mapped block reads back; both leave the same bytes to replay, and when nothing in
  flight was kept and the manifest is the newest, as many as before the crash. Coverage: more than 5
  kept in-flight rounds, more than 5 dropped, more than 20 recoveries from a manifest, at least one
  NoSpace, depth 2, an erased header, a restart and a crash during recovery all occur.
- [fuzz] Bit flips anywhere in a log of 40 writes, cold blobs and checkpoints on a 1,024-block disk:
  rebuild and open either fail with Corrupt, CopyOnly or NotFormatted, or resume a log whose next
  write survives a restart.
- A recovery that ends Corrupt writes nothing: the disk reads byte for byte as before it (RocksDB
  MANIFEST written before recovery succeeds).
- A failed write in a round fails the log: no item of that round or after is acknowledged; recovery
  after a restart gives an LSN no greater than the last record fully landed, the next record takes
  the next LSN, and no block of the failed round reads as a record (RocksDB sequence numbers going
  backwards, duplicate WAL entries after error recovery).
- A failed flush: the round's items are not acknowledged even though reads see the blocks; a crash
  then lands any subset, and recovery is a prefix.
- One checkpoint over two indexes and a shard replica, where one index has no new writes and one's
  flush fails: the manifest's replay LSN for each index is at most its oldest unflushed write, and a
  crash recovers all three to one LSN (RocksDB #11148, atomic flush leaving out a column family).

## Superblock copy

- A bit flip in the log ID of block 0, or of the copy: recovery returns all records and rewrites the
  damaged one from the other, flushed before the first segment-header read (trace: read block 0, read
  the copy, write, linked flush, then the header reads).
- An unreadable block 0 (read error) is restored from the copy, flushed, with the same trace.
- Block 0 of zeros, or of another format ("EFI PART" repeated), or with a bit flipped in its magic or
  version, next to a valid copy is CopyOnly(BadMagic or BadVersion, copy). The disk may have been
  reused, so recovery reads nothing more, writes nothing and leaves restoring to the operator.
- Both copies damaged is NotFormatted.
- A superblock whose geometry claims more blocks than the disk holds counts as damaged, and a copy
  that is not where its own geometry puts its copy is not used: next to a damaged block 0 either is
  NotFormatted ([superblock repair](design.md#superblock-repair)).
- A reformat that crashes after the new block 0 is flushed but before the copy is written: block 0
  (new log) wins, the stale copy (old log) is rewritten to match, and every segment is free.

## Quarantined segments

- [property] Seeds 1-10; 4,096-block disk (4-block segments), 4-block records, max replay 40 blocks,
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
  same entry; an empty block is one restart at 0. Floors of that block, strict or not, land where a
  sorted list does ("apricot" strict gives "apple", "b" gives "apricot", "apple" strict gives none).
  [property] Seed 31, 300 blocks of up to 100 keys across restarts: seek and floor, strict or not,
  agree with a sorted list, and the block validates with its entry count and last key.
- A block is rejected when truncated, with unsorted or repeated keys, an empty value, an unknown
  kind, a restart past the entries, no restart, a restart count larger than the block, or a first key
  not above the previous block's last.
- Get, replace and prefix scan: keys "ab", "a", "abc", "b", "aa": scan "a" returns a, aa, ab, abc in
  order; scan "ab" returns 2; inserting "b" again replaces its value and leaves 5 entries.
- [property] Seeds 0-11, 2,000-6,000 random puts and removes, keys and values up to the limits in
  every third seed so pages split at every level: gets, floors and a full scan read as a map, inner
  keys equal their child's smallest key, and every page in use is reachable.
- [property] Views: seeds 0-5, 15,000 writes with views opened and closed at random read as the map
  did when each opened; once all close no page is left copied away, and a write then copies nothing.
- 50,000 keys `fs/<random 16 hex digits>/a` with 40-byte values, inserted in random order, leave
  leaves at least 75% full (80% measured; 63% without redistribution in the Go memtable spike).
- [property] Flush to a run at any piece size: seed 11, 50 trees from up to 200 random inserts and
  removes (keys 1-40 B, values 0-59 B), max piece 123-4,105 B. Every piece is non-empty, within the
  limit and sized exactly, only the root lists references, and an empty tree writes no run. Loading
  the pieces as a run into an empty tree reads as the original. A run needing more pieces than its
  root can list fails with TooLarge, checked to be so, in at most 15 trials.
- [property] A flush keeps its frozen view: seed 12, 200 trees from up to 300 mutations (keys 1-3 B
  over 5 letters, values 0-20 B, 1/4 removes), max piece 256-2,055 B. Up to 20 random inserts and
  removes run between pieces, on keys before and after the cursor, into the new memtable. The run
  equals the tree as of the freeze, the frozen pages are freed after the last piece, and the tree
  still reads as the map.
- A flush hands out its next piece only once the previous one is durable; asking earlier is
  refused (NoFlush).
- A view opened during a flush keeps reading the frozen memtable after the flush ends, and its
  pages are freed only once the view closes.
- Memtable over runs: after a flush, a key only in the run reads from the run; a key written after the
  flush reads the memtable's value; a key removed after the flush reads as absent; a scan merges the
  memtable and runs in key order with no duplicates.
- A get touches each layer once: with three runs and a flush running, a get of a missing key and one
  of a key only in the oldest run each search 5 layers, the memtable, the frozen one and each run's
  fence pointers and one block.
- A run of the [MetaStore memory spike](design.md#appendix-metastore-memory-spike)'s file model
  (100,000 files, each a 92-byte attribute record and a directory entry, 20 to 50 files per
  directory, names of 8 to 32 bytes, in create order) takes at most 145 B per file (the spike's
  blocks of 16 entries took 135 B, F4).
- A flush's peak memory is the frozen tree, one piece and the run it becomes.
- A full collection with 20 million live entries, half in runs, takes under 1 ms (Go memtable
  spike; `BenchmarkCollection` with `-lsm.entries`).
- Compaction picks by RocksDB's universal rules; the picker cases are under "Compaction".
- Compaction: four runs with overlapping keys merge, on another goroutine while the memtable takes
  writes, into one run equal to their merge, newest value winning and removed keys dropped, since
  the oldest run is among them. A merge of newer runs may be picked while one over the oldest runs. A view opened before
  the compaction still reads the four runs; the next checkpoint names the merged run and reports
  every piece of the four dead.
- A merge of four small runs above a run over 1 MiB (30,000 entries) leaves that run out and keeps
  the four removals, which still hide its keys.
- A flush drops a removal no older run holds the key of, and keeps one that hides a key in a run:
  with "old" in a run, putting and removing "new" and removing "old" and "never" flushes a run of
  one entry, the removal of "old".
- A compaction installed before it ran is Stale and the next is still picked; a span whose output
  a root could not list is never picked (512-byte pieces, four runs of 400 entries): size
  amplification and size ratio both pick those four runs, the picker goes on to the next rule each
  time it refuses a span, and nothing is picked.
- A compaction installed while a flush writes its inputs: that checkpoint's manifest names the
  inputs, which restore to the tree, and the next names the merged run and reports the inputs dead.
- A checkpoint that fails after writing a compaction's result whole reports nothing dead and carries
  that run next time.
- Restore is runs plus replay: after six flushes, a compaction and more writes, loading the newest
  manifest's runs and replaying the later writes equals the live tree.
- Loading a run is Malformed when a piece or the root is truncated by a byte, a piece is missing or
  two are swapped, a piece names another run's sequence number, a key moves above the next one, or
  the run is loaded twice; a piece of another version is BadVersion. [fuzz] Any one-piece run the
  loader accepts iterates in strictly increasing key order, and gets and floors of its keys succeed.
- An entry larger than a piece is TooLarge, not a panic (a 120-byte entry with 128-byte pieces; a
  100-byte one fits), as are a key over `MaxKey` and a value over 1,000 B; the largest entry fits. A
  piece size under 64 B is BadConfig.
- A failed checkpoint keeps the frozen memtable and reports the one piece it wrote of it as dead;
  writes go on into the new memtable, and the next flush writes the frozen one without freezing them.
- Benchmarks: get, floor, put and an iterator step over 200,000 keys in a memtable and three runs.

## Compaction

The "LSM tree" section holds the merge, removal, staleness, install-during-flush, failed
checkpoint and restore scenarios; Merge steps rewrites the first two for steps.

Picker cases (a Go table until Data-driven files moves them to files; the universal cases of
RocksDB's `db/compaction/compaction_picker_test.cc` at v11.8.1 that exercise these three rules,
`NeedsCompactionUniversal` and `UniversalMaxReadAmp*`, are ported as they are, and the rows below
added; run sizes in MiB newest first; size
amplification 200%, size ratio 1% with at least 2 runs, auto run limit from a 1 MiB buffer;
nothing below 4 runs):

| Runs, newest first | Rule that fires | Merged |
| --- | --- | --- |
| 1, 1, 1 | none (3 runs) | nothing |
| 1, 1, 1, 1 | size amplification: newer 3 = 300% of the oldest, firing before size ratio | all four |
| 0.1, 0.1, 0.1, 0.1 above 10 | size ratio (amplification 4%) | the four small runs; their removals kept, since they hide keys in the 10 MiB run |
| 4, 4, 8, 20, 100 | size ratio: 4, 4 ≤ 4.04, 8 ≤ 8.08, 20 > 16.16 | the newest three (output 16) |
| 10, 1, 1, 100 | size ratio from the newest: 1 ≤ 10.1, 1 ≤ 11.11, 100 > 12.12 | the newest three |
| 1, 10, 100, 1000 | none: amplification 11%, no span of two, 4 runs ≤ auto limit | nothing |
| 1, 10, 9, 1000 | size ratio fails at 1 (10 > 1.01) and starts again at 10: 9 ≤ 10.1, 1000 > 19.19 | runs 2 and 3 (10 and 9) |
| 1, 1.5, 2.25, … , 38.4 (ten runs, each 1.5× the one before) | run limit: amplification 195% < 200%, no span, auto limit 8 for a 38.4 MiB largest run | the newest 10 − 8 + 1 = 3, leaving 8 runs |
| 300, 300, 100 above a 100 | size amplification (700%) | all |
| 1, 10, 100, 1000, 10000 with a fixed limit of 4 | run limit | the newest two |
| the oldest three held by a running merge, then 1, 1, 1, 1 | size ratio among the free runs; size amplification cannot fire while the oldest run is held | the newest four; the held runs untouched |

- Auto run limit, from the design's examples: largest run 44 MB → 8, 64 MiB → 8, 1.6 GB → 13,
  16 GiB → 16. A fixed limit set in an index's config replaces it.
- A merge running over the oldest runs does not stop a merge of newer runs; installing both in
  either order leaves the runs in age order.
- The limit is counted as RocksDB counts it: 1 MiB, then each next run 1% larger than all before,
  until one reaches the largest run; 38.4 MiB gives 8, as the ten-run case uses.
- Runs a running merge holds count toward the four: the oldest three held and one more run make
  four, and the free run alone picks nothing (no span of two).

Flush sequences (the [LSM compaction study](../spikes/lsm-study-compaction/)'s M1 to M3 through
the picker's size model, M1 over 2,000 flushes and M2 and M3 over the study's 4,000, measured over
the second half; the size model and the real tree gave identical counts in V1, so the bars below
are the model's):

| Index, flushes | Runs mean / max | Bytes rewritten per flushed byte | Runs' bytes ÷ live index |
| --- | --- | --- | --- |
| 44 MB, steady 1.3 MB | 4.0 / 7 | 3.65 | 1.29 to 1.70 mean |
| 44 MB, alternating 1.3 and 33 MB | 2.3 / 3 | 2.38 | same |
| 44 MB, bursty (33 MB one time in five) | 3.1 / 7 | 2.27 | same |
| 44 MB, log-uniform 0.13 to 33 MB | 3.6 / 8 | 2.37 | same |
| 1.6 GB, the same four sequences | 4.5 to 6.5 / at most 13 | 4.0 to 6.3 | |
| 30 MB whole-chunk chunk index, flushes of 40 and 1,000 entries | 6.1 to 6.7 / 7 | 9 to 27 | |
| 1.6 GB, steady, fixed limit 4 | | about 100 (the second-oldest run absorbs every flush) | |

- Run count through the real tree, at 1/33 of M1's sizes (3,300 keys of about 400 B, 64 KiB
  pieces, the buffer scaled to match) so it runs in about 2 s: 2,000 flushes alternating the two
  flush sizes, and the bursty and log-uniform sequences, never leave more than 2 + log2(largest run ÷ 1 MiB) + 1
  runs (the limit plus the flush not yet merged); the runs' bytes average 1.29 to 1.70× the live
  index, and the peak during a merge stays within 4.5× (the study's 3.1 to 4.4, design.md 3.1 to
  4.5).
- Max runs is a hard bound (the auto limit); mean runs and bytes rewritten land within 25% of the
  model's figures.

Scheduling and swapping:

- A merge's work is interleaved with the owner's writes and flushes, and the daemon runs it on a
  worker goroutine (`internal/lsm` is sans-IO); the owner installs the output in place of its
  inputs, in age order.
- The picker skips runs a running merge holds (the held-runs row above).
- One scheduler per disk, one merge worker: index A at 10 runs over a limit of 8 and index B at 14
  over 13 start A first by either measure. A at 6 over a limit of 4 (2 over, ratio 1.5) against B
  at 16 over 13 (3 over, ratio 1.23): A first, by the score of free runs ÷ limit (RocksDB's,
  `version_set.cc:3601`).
- `merge_workers` defaults to 1: with index A's merge running, index B's waits until it installs;
  with 2, both run.
- A merge's inputs die once a manifest that no longer names them is durable and a segment header
  names it; until then their pieces count live in the usage table.

Edge and negative cases:

- A crash while a merge runs, before any manifest names its output, recovers the previous runs
  plus replay, and the merge is picked again.
- A merge whose run check fails: see Run checks.
- Two merges never share an input run, under every picker case.
- Passes the Metamorphic index tests' sets that exercise it: Two merges, Fixed limit, G.

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
  map to Zero(1,024 blocks, header 100) and its entry to version 3. Blobs are skipped. Block 1 of
  chunk 7 reads LBA 102, and unwritten block 2 reads as zeros (the chunk has an entry); a chunk with no entry is not found. Listing a chunk returns its live
  extents by first block; record 100 refers to 3 blocks.
- Trim and split: a 1-block write of chunk 7 v3 at block 1 leaves the live map as block 0 (v1,
  LBA 101), block 1 (v3) and block 5 (v2); a 4-block write v4 at blocks 0-3 leaves blocks 0-3 (v4)
  and block 5 (v2). The live counts of the records that lost blocks fall by the blocks trimmed
  (records 100, 200, 300: 1, 0, 4), and the live map never holds two entries for one block.
- Freeze: a record with freeze(chunk 7, v4, epoch 2) and a 1-block write v5 at block 2 copies the live
  map under key version 4 before applying the write. A read of block 2 at snapshot epoch 1 finds v4's
  block, as does a read at epoch 2, whose cut the freezing write followed, the live read finds
  v5's, and a read at epoch 3 reads the live map. The chunk entry holds version 5 and epoch 2, and
  record 300 now refers to 7 blocks, 4 frozen and 3 live. The freeze applied again changes nothing.
  A rebuild from headers in LSN order gives identical live and frozen maps.
- A cleaner move names the chunk, its blocks, the location they were copied from and the new
  location and header, and applies to every map, live or frozen, that still points at that source.
  After the freeze above, moving block 5 from LBA 103 to 501 (header 500) changes the live map and
  v4's frozen map, which share it; a second move from LBA 103 changes nothing, and record 100 refers
  to no block.
- Moves split extents: a 4-block write v1 at LBA 101, frozen at epoch 1 by a record that writes
  block 3 v2 (LBA 201), then a move of blocks 1-2 from LBA 102 to 301 (header 300), changes 2 maps
  and leaves the live map as block 0 (v1, 101), blocks 1-2 (v1, 301), block 3 (v2, 201); record 100
  then refers to 3 blocks and record 300 to 4. A move from a location no map points at changes
  nothing, one of a block only the live map holds changes 1, and after a write replaced live block 0
  a move of it from LBA 101 changes only the frozen map.
- A record with an entry for pool 2^32 − 1, which the index keeps record counts under, or with a
  freeze of version 0 of a chunk that exists and holds no absent version, is refused whole and
  leaves the index as it was; a freeze of version 0 of a chunk with no entry is accepted (see
  "Absent freeze").
- The index's own check finds a planted second live entry over a block another entry holds.
- A read of a live block costs one lookup, the greatest key at or below its block's.
- [property] Seed 22, 20 runs of 300 records over 3 chunks of 64 blocks: a write of 1-8 blocks, or
  one time in five a punch, with a freeze of a written chunk at a new epoch one time in six, and a
  cleaner move of a live or (one time in three) frozen block after one record in four: each move
  changes as many maps as the model holds the block at its location, every block, live and at every
  snapshot epoch, reads the version and location a model of whole-map copies holds, and the
  invariants hold after every step.
- [property] Replay past pending writes (chunk epochs spike R1, 200 seeds): a primary that maps
  writes at commit, a chunk's in version order, with cleaner moves and checkpoints whose replay
  point is its oldest pending write, restarts from the checkpoint's runs plus every record from the
  replay point, and reads every block, live and at every snapshot epoch, as a rebuild does.
- [property] On a 1 GiB simulated disk (16-block records, a checkpoint every 96 blocks of records,
  the decode check on), seeds 0-3, three generations of 150 rounds of 1-4 items over 12 chunks
  (writes of 1-6 blocks, a whole-chunk punch one time in ten, a partial punch two in ten, a freeze
  of a written chunk one in eight), with checkpoints and compactions: after each generation a
  rebuild from every record header, and recovery from the manifest's runs plus replay, both equal
  the live index and pass its check. Every seed writes multi-piece runs, carries runs, compacts and
  freezes.
- A crash once a checkpoint's manifest record is durable but before any segment header names it,
  with a compaction's result among its runs, recovers from the previous manifest plus replay to the
  live index, and the next checkpoint's restart does too (seeds 0-3).
- Benchmarks: a lookup among 20,000 one-block extents over 64 chunks, and applying a one-block write.

## Range removals

Tree:

- [property] Seeds 0-59, 3,000 random puts, removals and range removals over 300 keys with flushes
  and compactions, under the five configurations of the [LSM runs spike](../spikes/lsm-runs/)'s
  `lsmx` test: gets, floors and a full scan read as a map. A planted merge whose output drops its
  inputs' fragments fails it (119 of 300 runs in the spike).
- [property] Seeds 0-11, random puts, removes, range removals, flushes and compactions, with
  filters on and off (once Run filters exists): gets, floors inside a prefix and prefix scans read
  alike and as the map. The filter case is Run filters' "A filter never holds fragments".
- A view opened before a range removal still reads the keys in it (RocksDB `Refresh` after
  DeleteRange, `HISTORY.md:1437` at v11.8.1).
- Size bounds, at `MaxKey` 1,032 and `MaxValue` 1,000: an entry with a 1,032-byte key and a
  1,000-byte value fits a page and splits; a 1,033-byte key is TooLarge. A range removal with both
  bounds of 1,000 bytes fits a fragment page and splits; a bound over 1,000 bytes is TooLarge,
  never a panic.
- A scan that starts inside a fragment and is bounded inside it returns nothing, and no layer is
  stepped past the bound to the fragment's end, checked with a search counter (RocksDB processing
  range tombstones past `iterate_upper_bound`, `HISTORY.md:1066`).
- A range removal over a run's keys, flushed: a get or a floor of any key in it probes each layer
  once; a write into the range after it reads back, before and after the flush; a fragment no older
  run has a key in is dropped by the flush.
- 262,144 one-chunk fragments in a memtable: a range removal and a cut allocate nothing and cost one
  search each.

Memtable fragments (keys are single letters, `k\0` the key right after `k`):

| Ops on an empty memtable | Fragments | Reads |
| --- | --- | --- |
| DeleteRange(b, e) | [b, e) | get c: absent |
| put c, DeleteRange(b, e) | [b, e); c now a removal | get c: absent; a flush keeps no entry for c: with no older key in [b, e) it drops both, and with an older run holding c it keeps [b, e) and drops the removal of c |
| DeleteRange(b, e), put c | [b, c), [c\0, e) | get c: the put; get d: absent |
| DeleteRange(b, d), DeleteRange(c, f) | [b, f) | |
| DeleteRange(b, c), DeleteRange(c, d) | [b, d) | |
| DeleteRange(b, e), DeleteRange(c, d) | [b, e) | |

- Fragments that only abut merge (the fifth row gives [b, d)), as overlapping ones do.
- DeleteRange(lo, hi) with lo ≥ hi returns an error and stores nothing; it never panics.

Layers and runs:

- A fragment hides every older layer and none of its own layer's entries: a run holding entry c
  and fragment [b, e) returns c (Pebble's ingested-table rule).
- A get stops at the newest layer holding the key or a fragment over it; a floor keeps each
  layer's candidate and moves every older layer below a covering fragment's start in one search;
  a scan moves older layers past the fragment's end, keeping each layer's current fragment until
  the scan passes its end (its search count is a Heap merge scenario).
- A run's fragments are stored after its entries in blocks of their own, `start → end` sorted by
  start, marked in the piece trailer, and searched in place. The run golden bytes are updated, and
  a run whose fragment blocks are truncated, unsorted or overlapping, or that the trailer does not
  mark, is Malformed when loaded (fuzzed with the run loader).
- A run's first and last keys include its fragments' bounds, so a run holding only fragments is
  never skipped by its bounds.
- A merge's output unites its inputs' fragments, clipped to what it keeps, and drops an older
  input's entry that a newer input's fragment covers: newer run A holds fragment [b, e), older run
  B holds entry c, and run C, older still and not merged, also holds c, so the fragment is kept.
  Merging A and B gives an output with [b, e) and no entry c, and a get of c is absent. A planted
  merge that keeps B's c fails it, since within a run entries win over fragments.

Dropping:

- A flush or merge keeps a fragment only while a run older than what it writes holds a key in it,
  and drops the removals a fragment it keeps covers. Removals arrive in key order, so each older
  run is checked with a forward cursor: a flush of 10,000 removals over three older runs searches
  each older run once and steps forward after, with no search per removal per run, and gives the
  same output as a search per removal.

Chunk index uses:

| Operation on chunk C | Removed | Kept |
| --- | --- | --- |
| A write covering two or more whole live extents | their keys, as one range removal | the extents it trims or splits, rewritten as points |
| A write covering exactly one whole extent | that key, as a point removal | |
| A whole-chunk punch | live map `(…, 0, 0) .. (…, 1)`, one range removal | chunk entry (tombstone), frozen maps |
| Releasing frozen version v ≥ 1 | `(…, v) .. (…, v + 1)` | the live map and other frozen versions |
| Releasing the absent version 0 | no keys (its map is empty and its key range is the live map's) | everything (see Absent freeze) |

- A whole-chunk punch leaves the chunk entry and one range removal over the live map, and no Zero
  extent; frozen maps read as before. A chunk with an entry and no extent at a block reads that
  block as zeros, and a chunk with no entry (or the absent version) as a miss, so a lookup tells
  the two apart. The built "Chunk index" scenario ("chunk 8's live map to Zero(1,024 blocks, header
  100)") becomes "chunk 8's live map empty, its entry at version 3", and Absent freeze's tombstone
  case and the Cleaner's whole-chunk punch row read zeros the same way.
- Chunk 7 written as 1,024 one-block extents, checkpointed, then rewritten in one 1,024-block
  write: the index holds one range removal and no point removal, a lookup of every block takes at
  most two searches before the checkpoint, after it and after a compaction, and a 1-block write at
  block 700 splits the new extent.
- [property] Seeds 0-5, 1,500 random writes, punches, freezes and whole rewrites over 4 chunks, with
  checkpoints and compactions: every block reads as an index built with point removals does, and
  the invariants hold. A planted memtable that does not cut a later put's key out of a fragment
  fails it (13 runs in the spike's `cix`).
- In `internal/lsm`, keys `fs/6/…`, `fs/7/…` and `fs/8/…` with `fs/7/` removed as one range: every
  `fs/7/` key reads absent, the others read as before, and a later put of `fs/7/1/a` reads back.
- Passes the Metamorphic index tests' Point removals set.

Performance (the LSM runs spike's case B and C, the real tree on the shared VM):

- The 1,024-extent rewrite above, timed: point removals took 255 / 201 µs per lookup in the
  memtable and flushed, and 731 µs after 192 GiB of further writes; range removals 0.4 to 1.8 µs.
- The mixed block volume (512 chunks, 100 flushes of 4,096 random 4 KiB writes and 12 whole-chunk
  rewrites): about 1.2 searches per lookup against 4.4 with point removals.
- The existing zero-allocation lookup benchmarks still report 0 allocations with fragments present.

## Run checks

Always on (release builds too), each failure an error, never a panic:

| Planted fault | Caught by | Result |
| --- | --- | --- |
| A flush's run holds one entry fewer than the frozen memtable | entry count on flush | the flush fails; the frozen memtable is kept, and the index is rebuilt from the log |
| A flush's run holds one range-removal fragment fewer | fragment count on flush | the flush fails, as above |
| A merge's output plus what it dropped is one entry short of its inputs | entry count on merge | the merge fails, nothing installed, the index rebuilt from the log |
| A merge's output plus what it dropped is one fragment short of its inputs | fragment count on merge | the merge fails, as above |
| One bit flipped in an in-memory piece of an input run, in an entry or a fragment block | CRC32C of the piece, checked before the merge reads it | the next merge reading it fails, as above |
| One bit flipped in a piece of an older run that a merge reads to decide whether to keep a removal | CRC32C of the piece, checked as the merge enters it | the merge fails, as above |
| One bit flipped in a merge's output before a flush writes it to the log | CRC32C of the piece, checked as the flush writes it | the flush fails, as above, and the log never holds the flipped block under a valid checksum |
| One bit flipped in an in-memory piece no merge reads | nothing until a merge reads it | reads go on; the flip is caught by the first merge that reads the piece |

- A failed check is `ErrCheck` from `NextPiece` or `Install`; the rebuild that follows is the
  store's (Single-node ChunkStore). A MetaStore shard replica catches up from a Raft snapshot
  instead (phase 4).
- A check that fails in step k of a merge whose earlier steps are installed rebuilds the index from
  the log like any failed check (with Merge steps).
- The CRC32C is kept per piece in memory, 4 bytes per piece, and checking a run's checksums
  allocates nothing. `BenchmarkMergeChecksums` merges four runs of the
  [metamorphic testing spike](../spikes/lsm-study-testing/)'s T6 workload (26,214 random 24-byte
  keys with 12-byte values each, 64 KiB pieces) with and without checking the inputs' checksums
  and computing the output's. On an arm64 Mac with Go 1.27, where CRC32C runs at 359 ns per 4 KiB,
  the two passes cost 12% (7.04 against 6.27 ms); the spike timed one pass at 4.8% of its own,
  slower merge.

Behind `Config.Check` (tests and simulator on, release off), each failure a panic in the flush or
merge that made it:

- A flush decodes every piece it built and compares it with the entries it took: a planted
  encoder that drops an entry panics in that flush.
- A merge checks its output's key order against its inputs: a planted merge that emits a key twice
  or out of order panics in that merge.
- The run list stays ordered by sequence number with no repeats: a planted install that puts the
  output as the oldest run (the metamorphic spike's P2) panics at install.
- The spike's other planted bugs (P1 and P5, which drop removals; P3, a restore bug; P4, a read
  bug) pass the counts and these checks and are caught by the package's unit and metamorphic tests
  (T2, T3).

## Metamorphic index tests

- [property] `internal/lsm`, seeds 0-199, 3,000 ops in a text form (`put aa v26x`, `del aa`,
  `flush`, `flushfail 3`, `floor aca`), drawn by weight: puts 35, removals 10, gets 15, floors 8,
  scans 5, views 11 (open 2, get 5, scan 2, close 2), flushes 5, failed flushes 1, compactions 4,
  restarts 1, plus range removals; keys of 1-6 letters a-f, a put's key new one time in four. Each
  op writes one history line, and the histories of every option set equal each other line by line
  and the map model's (Pebble's `internal/metamorphic`, a7b8321871e3).

  | Set | Pieces | Flushes | Compaction | Other |
  | --- | --- | --- | --- | --- |
  | F | 1 MiB | none: memtable only, a restart replays every write | none | |
  | A | 1 MiB | the generated ones | the generated ones, installed at once | |
  | B | 4 KiB | generated, and after every 40 writes | after every flush until none is due | |
  | C | 256 B | generated, and after every 10 writes | never | |
  | D | 512 B | generated, and after every 25 writes | picked at an op, installed between the pieces of the next flush | up to 4 writes between a flush's pieces |
  | E | 1 KiB | every flush fails after one piece first; also after every 30 writes | eager | a restart after every flush |
  | G | 4 KiB | after every 2 writes | only after every 40th flush, so a merge leaves older runs out | |
  | Filters off | | | | no run filters |
  | Filters "maybe" | | | | every filter probe answers "maybe" |
  | Two merges | | | two merges of disjoint adjacent runs at once | |
  | Fixed limit | | | a fixed run limit of 4, against the auto-tuned one | |
  | Point removals | | | | each range removal replaced by the point removals of the keys it covers (Pebble's `replace_single_delete`) |
  | Linear merge | | | | scans, flushes and merges compare every layer at every step instead of keeping the heap |

- The chunk index runs the same way with its own ops: writes, punches, freezes and cleaner moves, a
  chunk's writes in version order and freezes only after a cut; with range trims it reads as with
  point removals.
- Planted bugs, each one line, fail at least one seed of 20 under the set the
  [metamorphic testing spike](design.md#appendix-metamorphic-testing-spike) found it with, and every
  divergence from the model is also a divergence from set F:

  | Bug | Fails under |
  | --- | --- |
  | A flush drops every removal, even one hiding a key in an older run | every set with flushes |
  | A merge's output is installed as the oldest run | G only |
  | A restore loads runs in the order the manifest lists them, not by age | every set with restarts |
  | Reads skip the frozen memtable | the sets that read while a memtable is frozen: A, B, D, E |
  | A merge drops removals that hide keys in runs older than its inputs | G only |

- A failing seed prints its replay command and is reduced to the fewest ops that still fail, its
  cadence rewritten as explicit ops; the reduced ops file becomes a data-driven regression case
  under `testdata/` before the fix. Regression cases from the spike: `put aa v26x`, `flush`,
  `del aa`, `flush`, `floor aca` gives none; `put e v30xxx`, `flush`, `del e`, `flushfail 3`,
  `flush`, `restart`, `floor e` gives none.

## Run filters

Changed existing scenario ("LSM tree"):

- "A get touches each layer once: with three runs and a flush running, a get of a missing key and
  one of a key only in the oldest run each search 5 layers" becomes: the get of the key only in the
  oldest run searches the memtable, the frozen one and the oldest run, plus any run its filter
  lets through by a false positive; the get of a missing key searches only the memtables unless a
  filter lets it through. Every run is still probed once.

Lookups:

- A get of a key in no run searches only the memtables and the runs whose filter lets it through
  and whose first and last keys bracket it; each run keeps its last key.
- Filters are probed newest first: a key in the newest run probes no older run's filter, and a
  covering fragment in a newer run stops the get before any older filter is probed.
- A floor inside a chunk written whole searches only the run holding it, plus false positives.

Filter build and layout:

- The filter is sized from the entry count the flush or merge knows before it starts and filled
  as the run streams out. For 1,000,000 entries at 10 bits per key, Pebble's writer rounds
  10,000,000 bits up to 19,532 lines of 64 bytes, then to the odd 19,533: 1,250,112 bytes plus a
  5-byte trailer (checked against the ported code). The flush's peak is the frozen tree, one piece
  and the filter, and its allocations do not grow with the entry count.
- It follows the run's entries and range removals in blocks of its own, marked in the trailer;
  the run golden bytes are updated. A run whose filter blocks are truncated or not marked is
  Malformed on load (fuzzed). A restore loads each run's filter from its filter blocks without
  hashing a key, and lookups probe as before the restart.
- The port carries Pebble's BSD notice and its tests (`sstable/tablefilters/bloom` at
  a7b8321871e3): the filter of their test keys encodes to Pebble's golden bytes, and every added
  key may match. Over 1,000,000 absent keys, a 10 bits-per-key filter's false positives stay within
  1.0 to 1.4% (1.16% in the read path spike).
- Every run has a filter, the oldest one included.

What each index's filter holds (an extractor per index in `Config`):

| Index | Filter holds | Lookup that probes it | Probes with |
| --- | --- | --- | --- |
| Chunk index | each chunk's prefix `(pool, h, chunk_id)`, 28 bytes, one key per chunk | floor bounded to a chunk's prefix, chunk reader | the prefix, once per read |
| MetaStore | whole keys and each key's inode prefix `fs/{volume}/{inode}` | get; prefix scan of one inode (`readdir`) | the key; the prefix |
| An index with whole keys only | whole keys | get | the key |

- A floor bounded to a prefix equals a plain floor plus a prefix check, under the property tests.
- A read of blocks 3, 4 and 700 of one chunk takes one reader: it probes the filters once with the
  chunk's prefix and serves 4 by stepping forward from 3, as Pebble's `TrySeekUsingNext` does.
- A filter never holds fragments: with one run holding fragment [c, f) and its filter ruling out
  d, a get of d, which an older run holds, is absent (RocksDB returned such keys twice,
  `HISTORY.md:1451`, `:2783` at v11.8.1).

Targets (the [LSM read path spike](../spikes/lsm-study-reads/) over 24 runs, about 2 million
entries; counts are the bar, times on the shared VM):

| Lookup | Runs searched | Time (spike) |
| --- | --- | --- |
| MetaStore get of an absent name | at most 0.5 on average (0.22 measured) | 1.9 µs against 13.3 µs unfiltered |
| Chunk-index read of a chunk written whole | about 1.18 | 5.1 µs against 31.5 µs |
| Chunk-index read of an unwritten chunk | about 0.19 | 1.35 µs against 30.5 µs |
| Chunk-index read under random 4 KiB overwrites | about 7.2 (every large run holds every hot chunk) | 19.8 µs against 33.2 µs |
| Filter memory | 0.76% of the chunk index's run bytes, 2.6 to 2.9% of the MetaStore's | |

- Every probe and lookup allocates nothing.
- Passes the Metamorphic index tests' sets Filters off and Filters "maybe".

## Heap merge

- A scan merges the memtables and runs with a min-heap, as Pebble's `mergingIter` does. Review: a
  scan step over 24 runs costs less than twice the step over 6 (the linear merge took 95 ns over
  6 and 360 ns over 24), and a seek and a step allocate nothing.
- Ties: when several sources hold one key, the newest layer's entry is returned once and the
  others are stepped past; a removal in the newest layer hides the key.
- A scan of n steps that crosses k fragments of one layer does at most k + 1 fragment searches in
  that layer, checked with a search counter.
- A seek and the 20 steps after it allocate nothing over 24 runs with range removals.
- [property] The heap merge and the linear merge, kept for tests, return identical scans from the
  start and up to 8 steps from every probe, and bounded from every other one, every 300 ops of the
  range-removal property's seeds (0-59) under its five configurations. The metamorphic tests
  extend this to flushes, merges and reads during a flush with their Linear merge option set.
- A scan over one source (memtable only) and over zero runs works, and a scan that exhausts
  every source reports the end once.

## Absent freeze

- The "Chunk index" seed 22 property stays as it is: its epochs count freezes of chunks already written, with no
  volume cut, so reading a never-frozen chunk as live is right there. E3 below is the regression
  with volume cuts.

Scenarios (records fed to the index):

- A record with freeze(chunk 9, v0, epoch 1) and a 1-block write v1 leaves chunk 9's entry at v1,
  epoch 1, and a read at epoch 1 finds nothing. A second record at epoch 1, a write v2 with no
  freeze, follows. After cut 2, a record freezes v2 at epoch 2: a read at epoch 1 still finds
  nothing, and one at epoch 2 finds v2. Each record applied again changes nothing.
- A record with freeze(chunk 9, v0, epoch 1) and a punch v1 records the chunk absent at epoch 1,
  as a write would: a read at epoch 1 finds nothing, a live read zeros.

The chunk epochs spike's cases are regression tests. The index at 676095b refused a freeze of
version 0 outright; the column below is what it read when the writer left that freeze out:

| Case | Steps | Index at 676095b | With the absent freeze |
| --- | --- | --- | --- |
| E1 | cut 1; write v1 (block 0, with freeze v0 at epoch 1); write v2 (block 1); read snapshot 1 | snapshot 1 reads v1 | snapshot 1 reads nothing |
| E2 | cut 1; write v1 (freeze v0, epoch 1); cut 2; write v2 (freeze v1, epoch 2) | snapshot 1 reads v1 | snapshot 1 reads nothing; snapshot 2 reads v1 |
| E3 [property] | seeds 0-49 (PCG(seed, 10)), 400 steps: a cut one time in 10, otherwise a 1- to 4-block write or, one time in six, a punch, over 4 chunks of 16 blocks | 35 of 50 seeds read a snapshot wrong | 0 of 50; a rebuild from the records and the records applied twice equal the index |

- The absent version is an empty frozen map under `(…, 0)` holding its freeze epoch. `LookupAt`
  at an epoch it covers reads a miss, not zeros and not the live map.
- It is listed among the chunk's frozen versions, before any other.
- Cleaner moves skip it: a move of block 3 of chunk 9 changes the live map and real frozen
  versions only, and the absent version stays empty.
- A freeze of version 0 replayed for a chunk that now exists and holds the absent marker changes
  nothing (replay after a restart applies the same record again).
- Releasing a frozen version belongs to the snapshot deletion sweep ([Deletion](design.md#snapshots-and-clones)),
  which has no index operation yet. When it lands: releasing the absent version removes no keys,
  the live map reads as before, and a record with that freeze of version 0, replayed after the
  release, changes nothing and is never refused.
- A chunk with a tombstone has a version, so the first write after a cut freezes the tombstone's
  version, not version 0: a read at that snapshot returns zeros, not a miss.

## Single-node ChunkStore

The byte-range oracle applies every acknowledged write's and punch's bytes in version order;
after every step, every block, live and at every snapshot epoch, must equal it. A chunk is 1,024
blocks. "Pending" is a write whose record is not yet durable.

Byte ranges to whole blocks:

| Operation (chunk C, empty unless stated) | Logged | Read back |
| --- | --- | --- |
| Write(off 0, 4,096 B of 0x11) | one whole block, no base read | block 0 = 0x11 |
| Write(off 100, 1 B 0x22) | block 0 merged with zeros | zeros except byte 100 |
| Write(off 4,000, 200 B) | blocks 0 and 1, both merged | bytes 4,000-4,199 set, rest zeros |
| Write(off 3 × 4,096 + 10, 3 × 4,096 B) | 4 blocks: 2 merged edges, 2 whole | exact range |
| Punch(off 0, 8,192) | one punch entry, blocks 0-1 | zeros |
| Punch(off 100, 8,192) over written blocks 0-2 | one record, one version: block 1 a punch entry, blocks 0 and 2 merged with zeros as write entries | 100..8,291 zeros, the rest unchanged |
| Punch(off 0, 4 MiB) | whole-chunk punch, tombstone | zeros; the chunk entry stays with the new version |
| Punch of a chunk that does not exist | succeeds | zeros |
| Read(off 100, 10 B) of a written block | | those 10 bytes |
| Read of a chunk never written | | zeros |
| Write after a tombstone | the version continues past the tombstone's | the written bytes, other blocks zeros |

API edges (Open where noted):

- A write or punch running past 4 MiB is refused with no disk action. Open: its error, and
  whether a write is cut at the chunk's end by the caller or refused.
- Open: a zero-length write, punch or read (refused, or a no-op that returns the current
  version).
- Open: the version a punch returns (the new version, as a write does, by design.md's "Every
  write and punch returns a new version"), and whether a punch of a chunk that does not exist
  takes one.
- Open: `expectVer` = 0 on an absent chunk (create-if-absent) succeeds and on an existing chunk
  fails.

Merging against the newest ordered version (the [sub-block writes spike](../spikes/sub-block-writes/)):

- W1 = Write(off 0, 10 B 'a') pending, then W2 = Write(off 5, 10 B 'b'): W2's base is W1's merged
  block, so the block reads `aaaaabbbbbbbbbb` then zeros. Merging against the committed version
  (zeros) would lose W1's bytes: the spike's M2 lost bytes on 500 of 500 seeds.
- [property] M1 and M9 as a regression bar: seeds 0-499, 400 steps, two clients with 8 writes
  pipelined each, 1 byte to 3 blocks at random offsets: 0 seeds wrong.
- A base read from disk checks the block's CRC32C. A base whose CRC fails is never merged into a
  new version.
- A base read in flight holds the chunk's later operations: W1 partial (base on disk), then W2
  whole-block to the same block, then W3 to another chunk. W3 goes out at once; W2 is ordered and
  logged after W1, so the block ends as W2. The trace shows no write of C before W1's base read
  completes.
- Open: what a single-node store does when a base read fails, by CRC or with Io (the replicated
  design fetches it from another member), and whether the operations queued behind it fail or go
  on. It must never carry rot into a new version.
- Open: the per-loop cache of recently merged blocks (design, Data layer) is unmeasured; whether
  it is part of this item.

Versions (fixed here: 32-bit interval over a 32-bit counter):

| State | Next write's version |
| --- | --- |
| Chunk with no version, local interval 1 | (1, 1) = 0x0000000100000001 |
| Chunk at (1, 7), pending or committed | (1, 8) |
| Chunk at (1, 0xFFFFFFFF) | the local interval is raised: (2, 1) |
| Another chunk at (1, 5), after that raise | (2, 1), above (1, 5) |

- Version 0 is the absent chunk and is never assigned.
- No two writes of a chunk get one version (simulator invariant), across restarts too.
- Open: where the single-node interval number is kept so a restart never reuses a version that a
  lost, unacknowledged write held; the replicated design takes a new interval on restart.
- Open: whether the raise waits for the chunk's pending writes to become durable, as the
  replicated primary lets its PG's pending writes commit first.
- `expectVer`: Write with expectVer = the chunk's version succeeds; any other value fails with
  no disk action. Open: whether it compares with the newest ordered version or the newest
  committed one, and its error.
- `atVer`: a read at a frozen version returns that version's bytes and never waits for a writer;
  a read at an old version no snapshot pins is an error. Open: its error.
- A read without `atVer` while W is pending returns the previous version's blocks at once.

Commit and mapping:

- A write is mapped when its record is durable, and a chunk's writes are mapped in version order
  (the chunk epochs spike's R2 failed 200 of 200 seeds out of order). The log reports Durable in
  LSN order, so the case that matters is a write of several items: a whole-chunk W1 of 5 items,
  then a 1-block W2 to the same chunk whose record becomes durable before W1's last item. W2 is
  mapped after W1. Open: whether W2's acknowledgment waits for W1's; if it does not, a read after
  W2's acknowledgment misses W2 until W1 completes.
- Snapshot epochs on the writer side: the first write after a cut to a chunk at version v puts
  freeze(v, epoch) in its record; with a pending W1 at epoch 1 and a cut, W2 at epoch 2 freezes
  W1's version; a second write at epoch 2 carries no freeze; a chunk with no version gets a freeze
  of version 0 (Absent freeze), and so does a punch that gives an absent chunk its first version;
  in a volume whose snapshot epoch is 0, a chunk's first write carries no freeze.
- Open: whether a punch of a never-written chunk logs a record at all.
- Open: the `ChunkStore` interface carries no snapshot epoch, so how the single-node store learns
  a write's epoch is unstated.
- A write larger than one record goes as several items of one version. Group commit refuses an
  item whose record exceeds the round less one block, so with 1 MiB rounds an item holds 254 data
  blocks: a whole 4 MiB chunk goes as 5 items (254, 254, 254, 254 and 8), acknowledged once all 5
  are durable; the freeze, if any, is in the first. On the 1 GiB test disk (64-block rounds) an
  item holds 62 data blocks and a chunk takes 17 items (16 × 62 + 32).
- A checkpoint while such a write is partly durable keeps its replay point at the record of the
  write's first item (design.md: a replay point never passes a pending write's record).
- Open: a crash with 3 of the 5 items durable. Replay maps those blocks under the version, so an
  unacknowledged write is partly visible; the design does not say whether that is allowed or must
  be all or nothing.

Events from the log:

- `Failed(NoSpace)` fails the writes it names to their callers, in submit order.
- `InDoubt` items are never acknowledged, and the store refuses writes until recovery. Open: what
  the caller sees for an in-doubt write.
- `CheckpointFailed`: the index keeps its frozen memtable, and the next checkpoint writes it
  again from its first piece (the "LSM tree" scenario, through the store).
- A flush or merge that fails a run check (`lsm.ErrCheck`, see Run checks): the store rebuilds
  that index from the log, the rebuilt index equals the oracle's, and the disk's other indexes
  serve throughout.
- The store hands the log one run piece at a time, each once the one before is durable: the trace
  never shows two pieces of one run in flight.
- `Flush(cs)`: completes once every write to cs acknowledged before it is durable; with nothing
  pending it completes with no disk action. Open: the semantics, since writes are acknowledged
  only once durable.
- `Seal` exists in the API only and returns an error. Open: which.

Invariants and faults (simulator): acknowledged writes readable; reads equal the byte-range
oracle live and at every snapshot; punched ranges read zeros; no two writes of a chunk share a
version; a live map never holds two entries for one block (`Index.Check`); a rebuilt index equals
the recovered one. Faults: crash at every action of a workload with unaligned writes, a cut, a
whole-chunk write and a checkpoint; unaligned writes pipelined to one block; a merge base failing
its checksum; a failed base read.

Performance: `Write` of a whole block, `Write` of one byte with its base in a pending write, and
`Read` of one block report 0 allocations per operation; a merge uses one 4 KiB pool buffer.

## Segment reserve

The reserve, ⌈(2 × `max_replay_bytes` + merge workers × `compaction_step_bytes`) ÷ segment⌉ +
victims per pass (4, as in the cleaner spikes), at least 8, with both byte settings at 64 MiB
(`ReserveSegments`):

| Disk | Segment | Merge workers | Reserve |
| --- | --- | --- | --- |
| 256 GiB and up | 64 MiB | 1 | 3 + 4 = 7 by the formula; 8, the floor |
| 256 GiB and up | 64 MiB | 2 | 4 + 4 = 8 |
| 255 GiB | 32 MiB | 1 | 6 + 4 = 10 |
| 1 GiB | 256 KiB | 1 | 768 + 4 = 772 of 4,096 |

- A reserve of every usable segment or more (3 on a 16-block disk) is BadConfig: Format refuses it
  and leaves nothing allocated, and open of a formatted disk fails recovery with BadConfig before it
  reads a segment header. The small disks of the tests set the reserve directly.
- Full disk waits: a 16-block disk (3 usable 4-block segments) with a reserve of 1 and a cleaner
  that has a victim. Two 2-block writes fill segments 1 and 2 (LBAs 4 and 8); a third is held:
  `Stalled`, no disk action, its buffer kept, and Close is Busy. A checkpoint's 10-byte piece still
  opens the reserve segment (segment header and the 2-block record, LBAs 12-14); its manifest finds
  no segment free and waits. Once segments 1 and 2 are freed (a test hook standing in for the
  cleaner), the held write goes into segment 1 and the manifest into segment 2, the checkpoint
  completes, and nothing failed.
- A checkpoint finishes in the reserve: a 32-block disk (7 usable segments) with a reserve of 2.
  Five 2-block writes fill segments 1 to 5 and a sixth write is held; the checkpoint's piece and
  manifest go into segments 6 and 7 and it completes, while the held write stays held. A punch on
  the cold stream that would fit in the block the manifest left free in segment 7 is held too: an
  ordinary item never appends to a segment opened from the reserve. When the owner reports no
  victim (`FailStalled`), both fail with NoSpace, one Failed event per stream (hot, then cold),
  and their buffers are freed.
- After a restart with only the reserve free, the open segments count as taken from the reserve:
  the log above, after a checkpoint whose manifest went into segment 6 of 7 and a reopen, holds a
  cold punch that would fit in segment 6.
- A stall ends when a segment frees: on that disk with a reserve of 2, a hot write stalls with
  segments 6 and 7 free; a cold punch then takes the only round slot. Once segment 1 is freed the
  write is no longer `Stalled`, a no-victim report fails nothing, and the write goes into segment 1.
  `Stalled` is worked out when asked, never left over from the last round placed.
- A checkpoint waits until no victim: on the full-disk-waits disk, a checkpoint's first 2-block
  piece fills the reserve segment and its second finds no segment free; both it and the held write
  wait, with no disk action. `FailStalled` then reports Failed(NoSpace) for the held write (hot),
  CheckpointFailed(NoSpace) and Failed(NoSpace) for the waiting piece (cold); End then says
  NoCheckpoint.
- Settings: a `max_replay_bytes` past 2^62 is BadConfig, since the hold compares against twice
  it, and `ReserveSegments` saturates rather than wraps.

Who may use the reserve:

| Item while only the reserve is free | Result |
| --- | --- |
| Chunk write, punch, freeze, Raft entries | held in submit order |
| Checkpoint run pieces and manifest | written into the reserve |
| Merge step output | written into the reserve (with Merge steps) |
| Cleaner copy records | written into the reserve (with the Cleaner) |

- Review, for checkpoints: a disk filled to its reserve still finishes a checkpoint, and an
  ordinary item never opens or appends to a reserve segment (each fails under a planted bug). The
  merge step and the cleaner pass join the check with their items; the simulator invariant that the
  bytes held beyond what the manifests name never exceed the reserve arrives with the invariant
  checker.
- Crash with the disk full except the reserve, during a checkpoint in the reserve: recovery
  succeeds and the held items were never acknowledged (with Crash at every action).

## Merge steps

- Open: build order. In the checklist this item comes before Manifest edits and snapshots (which
  carries its "manifest edit holding the bounds") and Metadata stream (its merge lanes). Either the
  checklist moves Merge steps after Metadata stream, or until then the bounds go in the whole
  manifest and the output into a lane of its own in the cold stream, and the lane scenario below
  moves to Metadata stream. Whichever item first opens a second segment in a stream needs lanes:
  a second open cold segment breaks the per-stream replay rule as merges break the metadata
  stream's.
- Four runs merged in steps of two pieces: after the first step, a get at or below the bound reads
  the output and above it the inputs; a range removal across the bound is clipped to it; a view
  opened before the step still reads the four runs; the manifest edit names the bounded inputs and
  the partial output, and the input pieces wholly at or below the bound count dead once it is
  current; a crash after that manifest recovers both, resumes from the bound, and every key reads
  as before the merge.
- A step merges up to `compaction_step_bytes` (64 MiB) of output; the owner appends the step's
  pieces to the output run and sets each input's bound to the last key the merge has written; the
  next checkpoint writes the pieces, the output's root and a manifest edit holding the bounds, as a
  manifest alone when no checkpoint is due.
- Bounded runs: an input holds only its keys above its bound, so output and inputs never share a
  key. With inputs A (newer) and B over keys a..z and a step ending at f, a get of c reads the
  output, a get of q reads A then B, a get of f reads the output, a scan from a returns each key
  once, and a floor of g returns f from the output when neither input holds g.
- A lookup of a key at or below an input's bound never searches that input, even when its filter
  says maybe.
- An input piece wholly at or below the bound is dead in the usage table once the manifest naming
  that bound is current; the piece the bound falls in stays whole and live. Its blocks below the
  bound leave memory unless a view holds them, so a merge's memory peak is one step.
- Output fills its merge worker's lane of the metadata stream: with a merge running and checkpoint
  flushes going on, no output piece shares a segment with a flush piece or a manifest. When the
  merge ends, the worker's next merge continues the same open segment, whatever index it merges.
- Concurrent merges: a merge of the newest runs picked while a stepped merge holds the oldest runs
  installs whole or in its own steps, and the two merges' bounds can share one manifest edit; every
  key reads the same throughout.
- Restart between steps resumes the merge from the bound in the manifest: crash after step 2's
  manifest of a 5-step merge, recover, and the merge's next step starts at step 2's bound; every
  key reads the same before the merge, between steps, after the crash and after the merge.
- Crash after a step's pieces are durable but before its manifest: recovery uses the previous
  manifest's bound, and the step is merged again.
- A failed checkpoint during a merge keeps the step's pieces for the next checkpoint.
- Review (the [LSM space spike](../spikes/lsm-study-space/), C2 and C4): at most one step plus a
  checkpoint's flush held beyond the manifests, 2 segments of 64 MiB on a 256 GiB disk with a
  MetaStore index of 2 or 6.25% (whole: 65 and 109; with size amplification 88 and 258), 5 and 2
  segments of 4 MiB with 16 and 4 MiB steps on the 16 GiB stand-in; manifest bytes per flushed
  byte at most 0.0080 with full tables (0.0018 whole). Steps sharing the open segment left 30 to
  57 segments stranded against 8 to 10 in a segment of their own (C5).
- A merge holds at most one step beyond what the manifests name: checked after every step in the
  simulator together with the reserve invariant.

Changed existing scenarios ("LSM tree"):

- "Compaction: ... the next checkpoint names the merged run and reports every piece of the four
  dead" becomes: the four-run merge in 1 MiB steps writes one step per checkpoint, each checkpoint
  bounds the inputs and reports dead the pieces wholly at or below the bound, and the checkpoint
  after the last step names the merged run alone.
- "A compaction installed while a flush writes its inputs ... the next names the merged run and
  reports the inputs dead" becomes: the next names the output so far and the bounded inputs, and
  the inputs die step by step.
- "A checkpoint that fails after writing a compaction's result whole reports nothing dead and
  carries that run next time" becomes: a checkpoint that fails after writing a step reports nothing
  dead, keeps the inputs' previous bounds, and writes the step again next time.

## Write admission

Free segments outside the reserve against `clean_free` (16), read by the disk's owner every
round, time from `ctx.Now`:

| Free segments outside the reserve | Cleaner workers | Chunk write blocks | Ordinary items |
| --- | --- | --- | --- |
| 16 and more | 1, idle (no pass starts) | not charged | admitted |
| 15 | 1 | not charged | admitted |
| 14 | 2 | not charged | admitted |
| 9 | 2 | not charged | admitted |
| 8 | 2 | token bucket at 1.0 × the cleaner's net freeing rate | admitted as tokens allow |
| 4 | 2 | at 0.75 × (linear, assumed) | as tokens allow |
| 1 | 2 | at 0.5625 × (linear, assumed) | as tokens allow |
| 0 | 2 | | wait, in submit order |

- Open: whether a pass runs at 16 or more free segments; design.md says the cleaner keeps
  `clean_free` free.
- Open: the shape of the scaling between 8 and none; design.md says "scaled from 1 down to 0.5 as
  free segments fall from 8 to none", and the 4 and 1 rows assume linear steps.
- The rate is what the cleaner measurably frees, net of what it copies, averaged over one-second
  windows: a cleaner that freed 2 segments of 64 MiB and copied 1.5 segments' worth in the window
  [0, 1 s) refills the bucket from t = 1 s at 32 MiB/s at 8 free segments and at 24 MiB/s at 4.
- A cleaner that copies as much as it frees in a window gives a rate of 0, which stops chunk
  writes at any count from 8 down. Open: what the bucket does with a net rate of 0 or below.
- Raft entries, punches and freezes are never charged, at any count above 0.
- A chunk write the bucket refuses is held unsubmitted by the store and reported as refused by
  the bucket until it refills; it keeps its buffer. How the connection stops being read is the
  Event loop item's (phase 5).
- Open: the bucket's starting rate before the first one-second window is measured, and its
  burst size.
- Open: whether the second worker leaves again above 14 (hysteresis); the design only says when
  it joins.
- Review in the simulator, with the cleaner slower than the writers at 90 and 95% full: free
  segments outside the reserve stay above zero most of the time, the Cleaner's NoSpace invariant
  holds, and write latency stays bounded. Open: the bars for "most of the time" and "bounded";
  the roadmap lists the thresholds as unmeasured.
- An index over its run limit never slows or stops writes (no run-count stall).
- The same wait applies at twice `max_replay_bytes` (see Checkpoint backpressure); both
  conditions holding at once hold items until both clear.

## Durable LSN in record headers

Codec ("Record codec" holds the layout, golden and damage rows):

- The prelude carries the newest LSN durable when the round was built, 8 bytes at byte 48, 56
  bytes in all. The first record of a log carries 0. A durable LSN not below the record's own LSN is
  Malformed, and encoding refuses it.
- The exhaustive single-bit-flip, torn-subset and torn-sector properties cover the new field.

Tail rule. Let g be the first LSN replay does not find. The log ends at g only if no record header
found past g names a durable LSN at or past g, every header found past g lies within
g + `commit_depth` − 1, and no segment header names a manifest at or past g:

| Depth at write / open | Records | Damage or loss | Durable LSNs in later headers | Result |
| --- | --- | --- | --- | --- |
| 1 / 1 | 1-5 | record 5 torn | none past g | LSNs 1-4 |
| 1 / 1 | 1-5 | record 4 damaged | record 5 names 4 | Corrupt(4) |
| 1 / 1 | 1-5 | record 3 damaged | records 4 and 5 name 3 and 4 | Corrupt(3) |
| 2 / 2 | 1-5, round 5 begun after round 4 was durable | record 4 damaged | record 5 names 4 | Corrupt(4) (the window alone would recover 1-3) |
| 2 / 2 | 1-5, rounds 4 and 5 in flight together | record 4 missing, record 5 whole | record 5 names 3 | LSNs 1-3; record 5's header zeroed |
| 2 / 2 | 1-5, rounds 4 and 5 in flight | record 5 missing, record 4 whole | none past g | LSNs 1-4 |
| 1 / 2 | 1-5 | record 4 damaged | record 5 names 4 | Corrupt(4) |
| 3 / 3 | 1-5, rounds 3, 4, 5 in flight | record 3 torn, 4 and 5 whole | both name 2 | LSNs 1-2; headers 4 and 5 zeroed |
| 2 / 2 | 1-5, rounds 4 and 5 in flight | record 4 damaged, record 5's prelude torn | unreadable | LSNs 1-3 (a torn header names nothing; the window decides) |

These are Pebble's `readAheadForCorruption` cases (`record/record.go:466-588`, the test at 577,
a7b8321871e3): a later valid chunk whose synced offset passes the bad one proves corruption.

- After recovery the next record's durable LSN is the recovered LSN: recovering 1-3 above, the
  next round gets LSN 4 and names 3.
- Existing "Recovery" scenarios that stay as they are: the torn tail of 3 + 1 records, damage in a
  middle record, damage in the last record, "Commit depth 2, a round missing" (the second record
  names 1), the duplicate LSN, and the durable manifest never dropped.
- [property] "Random crashes keep acknowledged records": its commit depth widens from 1-2 to 1-3,
  and it adds: every record header's durable LSN is below its LSN and at most the log's durable
  LSN when its round was built, and a gap that a whole later header names durable is always
  Corrupt.

## Manifest edits and snapshots

- The first checkpoint of a log writes a snapshot: every index's runs and replay LSN and the whole
  usage table. Later checkpoints write an edit: runs added and removed, replay LSNs, merge bounds,
  and the usage rows that changed, a freed segment as a row in state free.
- Snapshot rule (Pebble's `record/rotation.go:40-68`): a checkpoint writes a snapshot instead of
  an edit when the edits since the last snapshot, this one included, would exceed the smaller of
  that snapshot and the next:

| Last snapshot | Edits since, before this one | This edit | Next snapshot | Writes |
| --- | --- | --- | --- | --- |
| 100 KiB | 40 KiB | 20 KiB | 110 KiB | edit (60 ≤ 100) |
| 100 KiB | 90 KiB | 20 KiB | 110 KiB | snapshot (110 > 100) |
| 100 KiB | 40 KiB | 15 KiB | 50 KiB | snapshot (55 > 50) |
| 100 KiB | 0 | 100 KiB | 120 KiB | edit (100 ≤ 100) |

- The rule's state survives a restart: after a snapshot of 100 KiB and two edits of 40 KiB, a
  crash and a recovery, the next checkpoint of a 30 KiB edit writes a snapshot (110 > 100), the
  sizes rebuilt from the last piece's list.
- The manifest's last piece lists the pieces of its snapshot and of every edit since; the segment
  header points at that last piece; recovery reads the snapshot and applies the edits in order.
- Review: open after any crash equals open from a snapshot taken at the same base LSN, and a usage
  table rebuilt from a snapshot plus edits equals the log's own, under the random-crash property.
- Changed bitmap: group commit marks each segment whose row changes. A checkpoint whose edit fails
  after segments 3 and 5 changed keeps the bitmap: the next edit holds 3, 5 and whatever changed
  since. A successful one clears it.
- Edit size: an edit holds at most one row per segment the records since the manifest before it
  changed, independent of the disk's size ([durability study](../spikes/lsm-study-durability/):
  3,071 rows at the median on 16,384 segments, 3,164 on 262,144).
- Space bars (the study's model, counts): on 1 TiB at 90% full with 4 KiB writes, edits plus
  snapshots 0.129% of the log against 0.415% for a full table, recovery reading at most 544 KiB;
  with 262,144 segments of 4 MiB, 0.159% against 6.64%, at most 8.5 MiB (the study's model; the
  tests check the bound below). Recovery reads at most twice a snapshot; the rule weighs every byte
  a checkpoint wrote for the manifest, piece lists included, and snapshots growing between
  rotations may exceed half of what manifests write.
- The cleaner never copies a manifest piece. A segment holding pieces the current manifest lists
  is not a victim; when the cleaner asks for it, the next checkpoint writes a snapshot whatever
  the rule says, and once a segment header naming that snapshot is durable the old pieces are
  dead.
- Quarantined rows are carried by every snapshot and never dropped by an edit.
- `NextSeq`: every snapshot and edit records it, 8 bytes, and both round-trip with it; recovery
  takes the loaded manifest's, the newest a durable segment header names. It is one past the open
  sequence of the newest segment a durable round had opened at the base LSN: with open sequences 1
  to 4 taken by durable rounds and a round in flight that has opened a segment at sequence 5 when
  the checkpoint begins, the manifest records 5, and a crash that keeps that round finds the
  segment at 5, at or past it, and used. Before the first manifest it is 1. Planted bug: taken from
  the log's next sequence number when the checkpoint begins (6 here), recovery frees that segment
  with its durable records, and the
  random-crash property fails (seed 1 in the [log segments study](../studies/log-segments/)).
- Changed existing scenario ("Segment usage table"): "A row for segment 0, rows out of segment
  order or naming one segment twice, a free or unknown state, or a newest LSN past the base LSN is
  Malformed" becomes: in a snapshot, a free row is Malformed; in an edit, a free row is valid; an
  unknown state is Malformed in both. New: an edit that removes a run its base does not hold, or
  names an index twice, is Malformed; an edit with no snapshot before it in the last piece's list
  is Malformed. Both decoders stay fuzzed.
- Changed existing scenario ("Group commit" property): "every finished checkpoint's manifest reads
  back with its base LSN and runs" reads the snapshot plus the edits the last piece lists.
- Crash cases: a crash before an edit's last piece recovers from the manifest before it; a crash
  after a snapshot's last piece but before any segment header names it recovers from the previous
  manifest (the existing "Manifest in pieces" recovery scenario, with edits).

## Checkpoint backpressure

With `max_replay_bytes` 8 blocks, counted in record blocks of ordinary items after the newest
manifest, written or in flight, segment headers and the checkpoint's own records left out:

| Ordinary record blocks after the newest manifest | Ordinary items | Checkpoint items |
| --- | --- | --- |
| 8 | admitted; a checkpoint is due | admitted |
| 15 | admitted | admitted |
| 17 (past twice `max_replay_bytes`) | held in submit order, buffers kept, not `Stalled` | admitted |
| a manifest becomes durable | the held items go out in submit order | |

- Three 3-block and a 2-block write leave 15 blocks; a 1-block write is admitted and leaves 17; a
  hot and a cold write are then held with no disk action. The checkpoint's 100-byte piece goes out
  at once and adds its 2 blocks to the bytes to replay (19), but not to the hold. Once the
  manifest is durable the held hot write, then the held cold write, become durable.
- A checkpoint's pieces and manifest wait in a queue of their own that leads the cold stream, so
  they never wait behind held items. A round may carry them and ordinary cold items together: with
  a hot write in flight, a cold 2-block write and a 100-byte piece queued behind it go out as one
  record, the piece first, which adds at least the write's 2 blocks to the hold and not
  the piece's block.
- A checkpoint larger than the bound releases the hold: with `max_replay_bytes` 4 blocks, two
  3-block writes hold a third; a checkpoint of four 2-block pieces and its manifest (14 blocks)
  completes and the held write goes out at once. Reopened, recovery replays from the start, since
  no segment header names the new manifest yet: 24 blocks to replay, of which the hold counts the
  10 of the three writes.
- A checkpoint that fails while items are held keeps them held, and the next checkpoint releases
  them. A failure for want of space, reported when the cleaner has no victim, fails the held items
  with NoSpace too, which bounds the wait: on a 16-block disk with `max_replay_bytes` 4 blocks,
  three 2-block writes fill it and hold a fourth; the checkpoint's piece finds no segment, and the
  no-victim report fails the piece, the checkpoint and the held write. A write submitted next is
  `Stalled` at once, until a new checkpoint begins.
- Merge-step pieces, written by the checkpoint, count as checkpoint items (with Merge steps).
- The chunk store stand-in of the recovery tests begins a new checkpoint after a failure once a
  record has become durable since, so held items are released and the random-crash property keeps
  "every accepted ticket ends ...".
- [property] A stalled checkpoint keeps the memtables bounded: seeds 1-10 on a 1 GiB disk, rounds
  of 3 to 16 blocks, commit depth 1-3, `max_replay_bytes` 8 to 40 blocks, 1,000 steps of 1- to
  8-block writes to random streams with completions interleaved, the stand-in withholding its pieces for runs of
  steps. After every event, the ordinary data durable after the newest manifest's base is at most
  twice `max_replay_bytes` plus one round; afterwards every write is durable and none failed.
  Coverage: a write held with nothing in flight. Without the hold it fails at seed 1. The Raft
  entries shards retain join the check with the Raft shard (phase 4).
- Replay after a crash while items were held reads at most twice `max_replay_bytes` plus a round,
  the records the checkpoint wrote and a segment per lane (the term for replicated primaries'
  pending writes arrives with phase 3).


## Held items: open points

- A round of a 100-byte checkpoint piece and two 1-block cold writes adds the writes' blocks and
  its header block to the hold, and nothing for the piece's block (today it adds all but the
  piece's 100 bytes).
- Open: whether `Close` with items held behind a checkpoint fails them with `ErrClosed`, freeing
  their buffers, or stays Busy.
- Open: whether ordinary cold items roll into a segment of their own while the checkpoint's queue
  fills a reserve segment and segments are free outside the reserve.
- Format and open refuse a zero reserve once the daemon sets it from `ReserveSegments`; the tests
  that need none say so.
## Metadata stream

- A third stream tag (2) for sorted runs, compaction output and manifests; the hot stream keeps
  chunk writes and Raft entries, the cold stream the cleaner's chunk copies and encoded shards.
- Changed existing scenarios, "Segments and superblock": the damage row "Stream 2, own index 0,
  manifest block 0, ..." and "Encoding refuses ... a segment header of stream 2" both change to
  stream 3; the segment header round trip covers metadata too, and the golden fixture gains a
  metadata segment header (Open: added beside the cold one, or replacing it) and a merge-lane
  header. The "Non-zero padding" damage row, which today covers byte 11, keeps covering the bytes
  still reserved; a lane past the disk's merge workers is not damage (see the lane whose worker is
  gone below). The format version stays 1 before the first release (see design.md "Upgrades").
- "Group commit": "Streams alternate when both have work" becomes: with hot items A and B, a cold
  item and a metadata item queued behind round 1 (A), the rounds that follow are the cold item
  (segment 2, LBA 128), the metadata item (segment 3, LBA 192, seq 3) and hot B plus a later hot
  item (LBA 67); LSNs are global. The order cold, metadata, hot is an assumption: design.md says
  only "taking the lanes that have work in turn".
- "Group commit", "Checkpoint end to end": the pieces' record (LSN 4) and the manifest (LSN 5) go to
  a metadata segment, not cold segment 2.
- "Group commit" property: its "items to random streams" include the metadata stream; and in
  "Write-side decode check", "writes the segment header of the other stream" covers each wrong
  stream of three.
- Which item goes where: a run piece submitted as cold is refused, or routed to the metadata
  stream (manifests are ReservedKind, written only by the log). Open: whether the cold stream
  still takes blobs at all; the existing property tests submit "cold blobs".
- Lanes: lane 0 of each stream is its shared segment, and merge worker *w* writes its output to
  lane *w* of the metadata stream. Two merge workers running give three open metadata segments, and
  no flush piece lands in a merge lane. The segment header names the lane in byte 11: a header of
  lane 0 encodes as today, a merge-lane header round-trips, and the decoder of the current format
  rejects it, as for a new format version.
- A merge's partly filled segment stays its worker's open segment when the merge ends, and the
  worker's next merge, of the same index or another, continues it: no merge segment is left with an
  unused tail while its worker runs.
- Group commit keeps a queue, a cursor and the append rule per lane, a round writes one record to
  one lane, and rounds take the lanes with work in turn. The append rule holds per lane: a round
  appends to a merge's segment only once the round that opened it is durable.
- Recovery reads each lane from the last of its segments whose first record is at or before the
  first record to replay, and merges every lane's records by LSN. Merge M opens segment 10 with its
  first record at LSN 100 and takes pieces at every checkpoint; shared metadata segment 11 opens at
  LSN 120; the replay point is 300. Per lane, replay reads M's segment from LSN 100 and every record
  past 300 in both segments. With the per-stream rule, as a regression test before the fix, it
  starts at segment 11 and misses M's records past 300: Corrupt(the first of them) while records
  follow, and, when M's are the newest durable records, a tail cut before them, which a later open
  that reads segment 10 reports Corrupt.
- Merge lanes' records count for the LSN chain and the tail rule and are delivered to no index:
  a crash after a step's pieces are durable and before their manifest recovers every acknowledged
  record, the index as of the previous manifest's bound; a crash after the manifest recovers the
  step. Either way each lane resumes appending in its own newest segment, and no flush piece lands
  in a merge lane afterwards. Skipping merge lanes in the same case reads as a gap: Corrupt.
- A torn merge record at the tail is found whole or torn by reading it in full, as other records
  are, never by its header blocks alone.
- A lane whose worker is gone (fewer merge workers configured after a restart): its last segment
  is read at every start until the cleaner frees it, and no record goes to it.
- Replay reads at most one segment per lane before the replay point: the trace check counts the
  segments read before the first record to replay.
- Recovery scenarios that carry over to the metadata stream: a torn record left in it at the tail
  (as "Torn record left in the other stream"), a crash in the round that opens a merge's segment
  (as "Crash in the round that opens a segment"), and the many-segments and random-crash
  properties with all three streams and 1 to 4 merge lanes.
- The write amplification bars with runs in this stream (3.86 / 8.04 / 26.4 at 80 / 90 / 95%
  under uniform overwrites) belong to the "Cleaner write amplification in simulation" item.

## Cleaner

The built "Chunk index" section already covers the owner's conditional `Move` and the move
property (seed 22).

Copy entry codec:

- A copy entry is a write entry's fields plus the location it copied from, 41 bytes. It
  round-trips, adds its blocks to the data area, joins the torn-write and bit-flip properties,
  and the golden record fixture gains a record of two copy entries. A copy with zero blocks or
  past block 1,024 is BadExtent; a copy entry with a field its kind does not encode set is
  Malformed.
- Open: a copy entry whose source is location 0 (a punched range): Malformed, or accepted and
  never applied.

Victim choice (score free space × age ÷ copy cost; the spikes use (1 − u) × age ÷ (1 + u), age in
user blocks written since the segment's youngest block was written, capped at one disk capacity;
on the 1 GiB disk one capacity is 262,144 blocks):

| Segment | Live share u | Age | Score | Chosen |
| --- | --- | --- | --- | --- |
| A | 0.50 | 10 | 3.33 | |
| B | 0.90 | 100 | 5.26 | B, of A and B |
| C, frozen snapshot data | 0.95 | 10,000 capacities, capped at 1 | 0.05 × 262,144 ÷ 1.95 = 6,722 | |
| D | 0.50 | 30,000 | 10,000 | D, of C and D; uncapped, C would score 6.7 × 10⁷ and always win |

- A pass takes the 4 best victims that pass the net-gain guard.
- Net-gain guard, header blocks rounded up per record (design: "a victim must free more space than
  copying it costs, header blocks included"). In 16,384-block (64 MiB) segments a copy record of
  single-block extents holds 253 data blocks and 3 header blocks (56 + 253 × 45 = 11,441 bytes; 256
  blocks fill the 1 MiB cap), so L live blocks cost L + 3⌈L ÷ 253⌉. A victim of 16,191 live blocks
  costs 16,383 and is picked; 16,192 costs 16,384 and is refused. Estimated in exact bytes, the
  guard let the cleaner pick 98.9% live victims and livelock (cleaner streams spike).
- Open: whether a copy record's own segment header is charged.
- Age cap regression, scripted in Go: a 1 GiB disk at 90% full with 20% of the data frozen by a
  snapshot; each of 1,000 passes gains at least one segment, and a planted uncapped score
  livelocks (cleaner spike C2: 1,000 passes without a free segment).
- A segment holding a piece the current manifest lists (Manifest edits and snapshots), or a run
  piece or record that the newest manifest named by a durable segment header needs (a record after
  its replay points), or moves not yet applied, is never a victim. A used segment
  with no live block (one that rounds recovery dropped left empty) is freed without copying
  anything.
- Live counts come from the usage table. Open: how the index's per-record counts reach the
  table's per-segment live counts.

A pass:

- The worker checks liveness, verifies each block's CRC32C (a failing block is never copied;
  Open: what the pass does with it, since a single node has no other copy), reads around the
  header-block cache, groups blocks by chunk, and writes records of copy entries into the cold
  stream, never a manifest piece. It sends its moves to the owner as an event.
- Grouping: a victim holding chunk 7 block 3, chunk 3 block 1, chunk 7 block 4 and chunk 3
  block 2 gives copy entries chunk 3 [1, 2] and chunk 7 [3, 4], each contiguous. Open: the order
  between chunks.
- Open: live run pieces in a victim, handled as the open question "Pieces shared between runs"
  settles (copy a piece and rewrite every root naming it, or have the next checkpoint write those
  runs again).
- The owner applies a move only where a map, live or frozen, still points at the source, then
  frees the victim once all its moves are applied. Interleavings (worker copies chunk 7 block 3
  from LBA 100 to LBA 5000):

| Between the copy and the apply | Live map after | Frozen maps after | Copy at 5000 |
| --- | --- | --- | --- |
| nothing | 5000 | | live |
| write of block 3 to LBA 900 | 900 | | dead at once |
| punch of block 3 | punched (location 0) | | dead |
| snapshot cut and a freeze of the version | 5000 | 5000 (counted twice) | live |
| freeze, then a write of block 3 to 900 | 900 | 5000 | live through the frozen map |
| whole-chunk punch | none (the punch leaves no extent) | frozen maps moved if they still point at 100 | live only through them |

- Copy entries in replay: chunk 7 v1 block 0 at LBA 101, a copy entry moving it from LBA 101 to
  LBA 601. Logged after a write v2 of block 0, the copy changes nothing, live or in a rebuild.
  Logged before it, the rebuild applies it and the write then replaces it. Either way the rebuilt
  live map and record counts equal the live index's, and applying the records twice changes
  nothing. A plain write entry in its place would map block 0 back to v1.
- A rebuild from record headers maps every copy entry exactly where the owner applied its move,
  record counts included (simulator invariant).
- Apply costs 42 to 72 ns per move on the owner, one index entry per moved block, 0 allocations
  (benchmark bar from the [parallel cleaner spike](../spikes/parallel-cleaner/), P4).
- Workers per disk: see Write admission.
- The cleaner writes into the reserve when only the reserve is free.

Freed segments and `NextSeq` (design.md, Recovery: a header of this log below the loaded
manifest's `NextSeq` is judged by the usage table, one at or past it was opened after the base).
The cases stage a cleaner pass on segment 1 of the "Segment usage table" log (60 one-block hot
writes, 16-block segments, a checkpoint whenever 40 blocks follow the manifest), whose records all
precede the replay point; the [log segments study](../studies/log-segments/)'s
`testdata/stale_header_test.go` stages them by hand against the built recovery, where all but the
control case fail, and they become regression tests here, staged by the cleaner itself:

| Crash order | Header on disk | Expected |
| --- | --- | --- |
| Freed in memory, the freeing manifest not durable | Old, below `NextSeq`, listed | Used, with the older manifest's row; replay of the copy records moves what its runs still map there |
| Freeing manifest durable, no segment header names it yet (control; passes today) | Old, below the loaded older manifest's `NextSeq`, listed | Used; the cleaner frees it again |
| A segment header naming the freeing manifest durable, not reused, TRIM not run (or a disk that is a file) | Old, below `NextSeq`, not listed | Free (today: used with 0 live blocks, one segment fewer free) |
| As above, TRIM run, reading back as zeros | Zeros, not listed | Free |
| As above, TRIM reading back as ones or garbage | Undecodable | Damaged: read in full, then free, since it holds no record of this log past the replay point; read in full again at every start until reused |
| Reused before any segment header names the freeing manifest, its new header and one record landed | New, at or past the loaded manifest's `NextSeq` | Used, live 1 and newest LSN the new record's, the old row dropped (today: live 8, the old row's 7 plus 1) |
| Reused after a header names the freeing manifest | New; at or past `NextSeq` if opened after its base, else listed | Used |
| Reused by a round in flight: its record at block 1 landed, its header did not; a header naming the freeing manifest durable, or not yet (two cases) | Old, below `NextSeq`; not listed, or listed | The record at block 1, at `NextSeq` or more, is erased and the next segment opened takes a sequence past it; the segment free, or used under the listing until the cleaner frees it again (today: not erased, and the next segment takes the sequence it carries, 11) |
| A round in flight at checkpoint begin opened a segment, and the crash kept the round | New, at or past `NextSeq`, since it counts durable rounds only | Used, its records replayed |

- A freed segment's own old record at block 1, below `NextSeq`, is ignored: recovery reads its
  header and block 1 and nothing more, at this start and every later one.
- Reuse at once: segment S holds a run piece that manifest M1 names; a cleaner pass ends and M2 is
  durable but no segment header names it yet, so allocation must not hand out S. Once a header
  naming M2 is durable and M2 no longer needs S, S may be reused at once, with no wait.
- `NextSeq` and the block-1 rule land together: freeing a segment under `NextSeq` while the block-1
  rule still counts every record of this log reads each freed segment in full at every start, and
  counting old records the other way round leaves a reused LSN that a later start can replay.
- Changed existing scenarios ("Recovery", "Segment usage table"), once the cleaner frees segments:
  the block-1 rule counts only a record at `NextSeq` or more; a table row for a segment whose
  header is at or past `NextSeq` is dropped and rebuilt from replay; a header of this log below
  `NextSeq` that the table leaves out is free, not used.
- Simulator, extending the random-crash property: with the cleaner freeing and reusing segments
  and crashes at every action, the usage table after every recovery equals the log's own, and the
  next open sequence number is at least the loaded manifest's `NextSeq`.
- Rebuild after cleaning: a rebuild from LSN 1 meets the LSNs freed segments held, gone for good;
  its rule is the design's open question, and its scenarios come with it.

TRIM (one `DiskTrim` per freed segment, `trim_rate` 128 MiB/s, time from `ctx.Now`):

| Case | Expected |
| --- | --- |
| 10 segments of 64 MiB freed at t = 0 | DiskTrims at 0, 0.5, 1.0, … 4.5 s |
| Low space (only the reserve free) | the same rate; never faster for space |
| 2,000 segments of 64 MiB queued at t = 0 | the base rate alone would end at t = 1,000 s; with the backlog rule every segment is trimmed within 10 minutes of being queued, so all by t = 600 s |
| A queued segment allocated again before its TRIM ran | its TRIM is dropped, no DiskTrim for it |
| A segment allocated again while its DiskTrim is in flight | its first write waits for the DiskTrim to complete |
| A segment freed, no segment header naming the freeing manifest durable yet | no DiskTrim until one is |
| A crash with TRIMs queued | they are lost, and the segments are free by the table all the same |
| A disk that is a file | no DiskTrim ever |

- Open: the exact rate for a backlog older than 5 minutes; design.md says "as Pebble's delete
  pacer does" (Pebble adds the backlog's bytes ÷ 5 minutes to its base rate).
- Open: a failed DiskTrim (fail the log, or ignore).

Invariants (simulator) and faults:

- Every cleaner pass frees more segments than it fills; a move never replaces a write that landed
  during cleaning; no segment is reused while it holds a live block, or a piece, run piece or
  record that the newest manifest named by a durable segment header needs (a record after its
  replay points); a disk full except its reserve still finishes a pass; no ordinary write fails
  with NoSpace while the cleaner has a victim; a frozen version is never dropped, and the cleaner
  never frees a block, while the live version, a snapshot or an unflattened clone needs it.
- Faults: a crash during cleaning, at every action of a pass (crash at every action); overwrites,
  punches and snapshot cuts landing between the worker's copy and the owner's apply; a crash
  after the copy record is durable and before the moves are applied (replay applies them with the
  same check); a crash after the victim is free in the table and before its TRIM.
- Performance bars (the parallel cleaner spike's P3, at 50,000 writes/s): the owner's work per
  write with a worker 1.6 to 1.8 µs at 90 to 95% full against 5.9 to 11 µs inline, its write p99
  about 6 µs. Open: whether these Rust figures are the Go benchmark's bar.
- Write amplification at 80 / 90 / 95% belongs to "Cleaner write amplification in simulation".

## Chunk slots

- Slots per segment: 16 in a 64 MiB segment; one in the 4 MiB segments of a 16 GiB disk; none on a
  disk whose segments are smaller than 4 MiB, where a whole chunk goes to the log as several items.
- A write entry naming a slot is 39 bytes, its slot's segment and index after the write's fields;
  golden bytes of a record with one such entry and one ordinary write are checked in. Its per-block
  CRCs cover the slot's blocks, so a block damaged in the slot fails the record as a damaged log
  block does.
- A whole-chunk write puts its data in a slot, its record in the hot stream; the index maps the
  chunk's 1,024 blocks to the slot. A partial write of the same chunk goes to the log and trims the
  slot's extent; the slot stays used while any block of it is live.
- A slot is free for reuse only once the record that rewrote or deleted its chunk is durable: a
  crash before that record is durable reads the old chunk from the slot.
- A slot segment has no segment header; the usage table lists it as `slot`. Replay marks a slot
  segment opened after the manifest from the records that name its slots, never from its first
  block (which holds chunk data that may look like anything, a valid segment header of this log
  included).
- The segment is TRIMmed and freed with its last slot; allocation fills the fullest slot segment
  first (two segments with 3 and 9 free slots: the next chunk goes to the one with 3).
- The tail rule applies to a slot write's record as to any other: a torn slot write under a
  record in flight is dropped, one under a durable record is corruption.
- Cost (the cleaner streams spike): whole chunks deleted at random cost no cleaning, write
  amplification near 1.004 at any fill; a pool holding half its data in whole chunks cleans its
  small writes for about 2.5 at 90% full.

## Trace records

- One 64-byte, pointer-free record type covers the four kinds the design names: an event a loop
  handles, an action it executes, a syscall it makes, and an output a machine hands its owner. Each has a fixed field set and order per kind, and one text line
  `tick loop kind fields...`. A loop's records carry its pass number, and a log's round actions carry
  their round, so a rule can tell which pass or round a line belongs to. For example:

  ```text
  1000250 2 syscall pass=81 epoll_wait ready=3
  1000251 2 syscall pass=81 read fd=17 bytes=4096
  1000252 2 event pass=81 NetRecv peer=5 msg=write
  1000253 2 action pass=81 DiskWrite round=7 io=41 lba=67 len=12288 vec=3
  1000253 2 action pass=81 DiskFsync round=7 io=42 linked=1
  1000254 2 syscall pass=81 writev fd=21 iov=3 bytes=12416
  1001207 2 event pass=82 DiskDone io=41 err=ok
  1001208 2 event pass=82 DiskDone io=42 err=ok
  ```

- An `output` line records what a machine hands its owner, after the flush that made it so:
  `1001209 2 output pass=82 Durable round=7 lsn=12`. An `Entry` action names each entry a round's
  record holds (`action pass=81 Entry round=7 type=freeze vol=3 inode=9 index=0`), and a snapshot
  cut is a message (`msg=cut`), so the freeze rule can read both. In the simulator `tick` is the scheduler's global time and `loop` is
  `node.loop` (`1000250 3.2 syscall ...`); records of three nodes whose `Now` differ merge into one
  file in tick order.
- Round trip: every kind's record encodes to its line and parses back to an equal record; for every
  kind, a line with a missing field, an extra field, fields out of order, an unknown kind or a
  non-numeric tick is rejected. [fuzz] The line parser is fuzzed: anything it accepts re-encodes to
  the same line.
- Two runs of one seed give byte-identical trace files, so `cmp` compares them; a field that holds a
  pointer, a wall-clock time or a map's iteration order would break this and is not allowed.
- Recording a record allocates nothing (`testing.AllocsPerRun` 0), since the event loop records
  every syscall on the data path.
- The checker library scans lines, or records, for a rule and reports the first line that breaks
  it, with the line number and the rule's name; a line that does not parse breaks the rule
  `syntax`. Rules keep state per loop. A round has at most one `DiskFsync`, after its write: a crash
  or a dump may end a trace between a round's write and its flush. The `epoll_wait` rule skips a
  loop's first pass, which a wrapped ring may hold only in part, and passes a trace with no
  syscalls, as the simulator's is. The peering and freeze rules read per loop, since the lines name
  no PG or volume beyond the entry's. Rounds and I/O IDs start over at a restart, so the round rules
  check a trace one incarnation at a time, split at each `Restart`. The rules the design names, each with a passing trace and a
  planted trace that must fail:

  | Rule | Passing trace that must pass | Planted failing trace |
  | --- | --- | --- |
  | Each round has exactly one `DiskFsync`, paired with its round's `DiskWrite`; format's two flushes and recovery's final flush are not rounds and are exempt | Depth 2: W1 F1 W2 W3 F2 F3 (as in `TestARoundAppendsOnlyOnceItsSegmentsOpeningRoundIsDurable`: "write 64+3 io104", "fsync io105", "write 128+3 io106", "write 67+2 io108", then the two flushes) | A round with two `DiskFsync` actions |
  | A round's write and flush are emitted together when every earlier round's write has completed | Depth 1: each `DiskWrite` and its linked `DiskFsync` in one pass | A `DiskFsync` emitted one pass after its round's `DiskWrite` with no earlier write in flight |
  | At most `commit_depth` rounds' writes in flight | Depth 1: round 2's `DiskWrite` after round 1's `DiskDone` | Depth 1: round 2's `DiskWrite` before round 1's `DiskDone` |
  | No round is reported durable before a flush that ran after its write completed | The seed-1 regression's fixed trace: `fsync io107` only after `DiskDone io=106` | The seed-1 trace: `DiskWrite io=106`, then `DiskFsync io=107` completing before `DiskDone io=106`, then a durable event for round 2 |
  | A chunk's first write after a snapshot cut carries a freeze entry in its record | A cut, then a write whose record holds `freeze` and `write` | A cut, then a write whose record holds no freeze |
  | No `NetSend` of a write before the peering reply (phase 3) | Peering replies, then the write's `NetSend` | A `NetSend` of a chunk write ahead of the `NetRecv` of the peering reply |
  | A PG primary serves no read before peering completes (phase 3) | Read replies after every peering reply | A read reply ahead of the peering replies |
  | At most one `writev` per descriptor per pass | One `writev fd=21` in pass 81 | Two `writev fd=21` with `pass=81` |
  | Bytes read per descriptor per pass at most the read budget (256 KiB in the spike) | One read of 256 KiB on fd 17 in a pass | Two reads of 200 KiB on fd 17 with one pass number |
  | Each pass number has exactly one `epoll_wait` | Pass 81 with one `epoll_wait` | Two `epoll_wait` records with `pass=81` |

  A level-triggered descriptor with data left past the read budget at the end of a pass is normal
  under bounded passes, and no rule flags it.
- Review criterion: the simulator's action trace of a seed and a planted event-loop trace (written by
  hand in the loop's syscall form) run through the same checker library, and each rule above passes
  its passing trace and fails its planted one in both.
- The daemon's `--trace` writes the same records to a ring in memory and dumps them as text on
  request: a dump after the ring has wrapped holds the newest records, whole, in tick order, and
  parses with the same parser. A ring of `trace_records` (default 65,536) records; 65,537 records
  dump the newest 65,536. The dump runs on the owning loop, so no record is half-written.
- On the data path no free-form log line exists: the sans-IO analyzer rejects `log`, `log/slog` and
  `fmt.Print*` in sans-IO and data-path packages (Sans-IO analyzer); operator logs go through
  `log/slog` off the data path.

## Deterministic scheduler

- One goroutine runs every simulated node, disk, client and network link from one seeded
  `core.Rand`. Review criterion: two runs of a seed produce byte-identical action traces, compared
  with `cmp`, for seeds 1-50 of the random crash test, 1-100 of the group commit property, and 0-99
  of the planted-bug protocol.
- Determinism holds across processes and settings: the same seed gives the same trace with
  `GOMAXPROCS` 1 and 8, under `-race`, in two separate processes (so map hash seeds and iteration
  starts differ), and run before or after other tests in one binary. A trace that changes is a failure
  naming the first differing line.
- Different seeds give different traces (a sanity check that the seed reaches the choices): over seeds
  0 to 99, no two traces are equal.
- The scheduler and the simulated drivers pass the sans-IO analyzer (`internal/sim` is sans-IO): no
  goroutine, channel, host clock, `range` over a map or global random draw.
- Simulated time: `ctx.Now` and `ctx.Raw` are ticks of 1 µs per node, set apart (a node may run `Now`
  1,000 and `Raw` 1,226), and only the scheduler advances them; nothing reads the host clock. Time
  jumps to the next pending completion, timer or delivery, so an idle stretch of an hour costs no
  wall time.
- Each step the generator chooses among everything pending: a disk action's completion (any pending
  one, not only the oldest, so completions reorder), a message delivery, a timer due, a client's next
  operation, a crash or a restart. A linked `DiskFsync` runs only after its `DiskWrite` completes.
- A write's blocks enter the disk's volatile state, which reads see, when its completion is chosen,
  not when it is issued, but a crash decides a write still in flight like an unflushed one: each
  of its blocks lands or not from the seed, as a real device may have written them. A linked
  `DiskFsync` whose write failed completes with Io without running, as io_uring cancels a link.
  A completion or delivery may be chosen at any time and moves the node's clocks to the later of
  now and its issue time plus its latency; a timer is offered only once due, and time jumps to the
  next one only when nothing else is pending; an unlinked `DiskFsync` makes durable only writes completed before it runs ("A flush makes
  durable every write that completed before it runs"). Case: depth 2, W1 and W2 issued, W1
  completes, an unlinked flush runs, crash with nothing landing: W2's blocks read as before. A
  scheduler that applies writes at issue lets the seed-1 bug (`TestAFlushSentLaterWaitsForItsOwnWrite`)
  pass, and this case must fail on it.
- Message delivery: for the planted-bug check in phase 1, the scheduler drops and reorders the toy
  protocol's messages itself, ahead of phase 3's simulated network, which adds duplicates and a
  `PeerLost` to a crashed node's peers. Crashes, restarts and drops are trace lines of their own
  (`event ... Crash`, `Restart`, `Drop peer=5 msg=write`).
- Timers: `SetTimer` on an armed `TimerID` replaces its deadline; the scheduler still delivers, at
  random, a `Timer` event for the replaced deadline or for one that no longer matters, because the
  driver contract allows it, and a machine must ignore it ("Stale timer" under Peer requests).
- Each machine draws from a child source forked from the run's seed (Seeded RNG, Fork), so adding a
  draw in one machine does not shift another's stream: the run's source forks the scheduler's
  child first, which makes every scheduling choice, then one child per node in node order, and each node's forks one per machine as the node creates it. A machine that
  draws once more leaves every other machine's draws unchanged.
- The scheduler drives the machines only through `OnEvent` and the actions they return: a machine
  that is handed an event outside its contract (a `DiskDone` for an unknown `IOID`) reports it as
  data, never panics on it.
- The disk log's seeded tests run on the scheduler with their seeds unchanged: the harness's
  seeded generator picks each step and the scheduler carries it out (`Take`), disk failures are
  `sim.Disk` faults, and crashes and restarts are the scheduler's. They keep their coverage
  thresholds. The group commit property's counts are unchanged; the random crash test's, now that a
  crash also decides writes in flight, are over seeds 1-50: 75 in-flight rounds kept, 79 dropped,
  199 recoveries from a manifest, 46 NoSpace, 15 at depth 2 and 22 at depth 3, 74 headers erased,
  49 restarts, 101 crashes inside recovery, 128 gaps named durable (12 inside the window). Each seed
  runs twice with byte-identical traces, and each incarnation's trace (split at `Restart`) passes
  the trace rules for rounds.

## Simulated disk

Crash model and fault helpers:

- Writes stay volatile until a flush. Reads see unflushed writes. On a crash each unflushed block
  lands or not on its own: a flushed block of 1s at LBA 0, overwritten by an unflushed 3-block write
  of 2s, crashing with only block 1 landing leaves LBA 0 = 1s, LBA 1 = 2s, LBA 2 = zeros, and
  nothing unflushed.
- Unwritten blocks read as zeros. A read or write past the end of the disk completes with InvalidRange.
- Fault helpers used by the log tests: crash with a chosen landed-set or with p = 1/2 per block
  (from a seeded RNG); flip a bit (XOR a mask into one byte of a durable block); fail reads of
  chosen LBAs with Io.
- Two unflushed writes to one block, the newer landing alone or the older alone, or both: the block
  reads as the one that landed last in execution order.

Injected errors (Pebble's `errorfs` predicates, RocksDB's fault injection):

- A failed write completes with Io and leaves each of its blocks undecided, as an unflushed write is:
  a crash lands each or not, since a device's error says nothing about which blocks it wrote. A
  failed flush completes with Io and leaves every unflushed block undecided: reads still see them,
  and a later crash lands each or not. A damaged read completes OK with one byte changed in the
  returned buffer. Errors are chosen by action kind, LBA range, the action's index and a probability
  from the disk's seeded source, so a seed replays them.
- Each predicate on its own and combined, on a scripted sequence of 10 actions (read LBA 0, write
  LBAs 64 to 66, flush, write 67, flush, read 64 to 67, write 128, flush, read 128, flush):

  | Predicate | Fails |
  | --- | --- |
  | kind = write | actions 2, 4 and 7 |
  | kind = flush | actions 3, 5, 8 and 10 |
  | kind = read, LBA range 64 to 65 | action 6 only (it touches 64) |
  | action index = 4 | action 4 only |
  | kind = write and LBA range 128 to 128 | action 7 only |
  | probability 1/2, seed 3 | the same subset on every run of seed 3 |
  | damaged read, kind = read, index = 9 | action 9 completes OK with exactly one byte changed |

- A failed read fills nothing the caller may trust and returns Io; a damaged read is
  indistinguishable from a good one to the caller, so only checksums catch it: a damaged read of a
  record header never decodes: damage in bytes 8 and up gives the checksum error, and in bytes 0-7
  (magic, version, header-block count) BadMagic, BadVersion, Malformed or Truncated.
- A failed flush is never followed by an acknowledgment from the same open log. Trace-check rule:
  after a `DiskDone` with an error for a log's `DiskFsync` (or `DiskWrite`), no durable event of that
  log instance appears until recovery has run again. Planted failing trace:

  ```text
  5000 0 action pass=12 DiskWrite round=7 io=41 lba=67 len=8192 vec=2
  5000 0 action pass=12 DiskFsync round=7 io=42 linked=1
  5950 0 event pass=13 DiskDone io=41 err=ok
  5951 0 event pass=14 DiskDone io=42 err=io
  5952 0 output pass=14 Durable round=7 lsn=7   <- breaks the rule
  ```

  The rule is `NoDurableAfterFailure`, and it resets at a `Restart`.

  The real group commit passes it on seeds 1-100 of the "every accepted item decodes" property, rerun
  with the first flush of each seed failed; a copy whose flush-error path is removed fails it.
- A failed write or flush is final for the open log: no item of that round or after is acknowledged,
  and the node runs recovery rather than retrying the flush.

Misdirected writes:

- A write lands whole at a different, in-range LBA chosen by the seed, and its intended blocks keep
  their old contents; reads after the write see the misdirected placement, as a device would show it.
- A record written to the wrong address is never replayed there: its header names its log ID,
  segment sequence and offset, so at the wrong place it is a Mismatch, never a record.
- At its own place the record is missing, and recovery treats it by the tail rule: within the last
  `commit_depth` LSNs with no later header naming it durable, it is dropped as in flight; otherwise
  recovery reports Corrupt at its LSN. Cases on a 1 GiB disk, 5 one-block records:

  | Misdirected | Depth | Expected recovery |
  | --- | --- | --- |
  | record 5 (last), into a free segment, not acknowledged (crash before its flush completed) | 1 | LSNs 1-4; nothing read at the wrong place counts as a record |
  | record 3, later rounds built after it was durable | 1 | Corrupt(LSN 3) |
  | record 5 over record 2's header block | 1 | Corrupt(LSN 2), and record 5 is not replayed at record 2's place |
  | record 4 of 5, both rounds in flight together | 2 | LSNs 1-3 |
  | record 4, round 5 begun after 4 was durable | 2 | Corrupt(LSN 4) (needs Durable LSN in record headers) |

- The blocks it overwrote at the wrong place are damage like a bit flip: a record's data block there
  fails its CRC32C wherever it is checked, a segment header there is handled as under Quarantined
  segments, and a free segment's blocks are harmless.
- A misdirected last record that was acknowledged is lost on a single disk, and the tail rule
  cannot tell. The simulated disk reports every write it misdirects, and the single-disk invariant
  "every acknowledged write is readable" excuses the writes of a misdirected record that no later
  durable header names; every other loss still trips it, and with replicas (phase 3) nothing is
  excused.

Torn writes, bit flips, fail-slow and discards:

- Torn writes: only some 4 KiB blocks of a multi-block write land at a crash; that is the per-block
  crash model above (Testing strategy: "only some 4K sectors of a larger write land"). The NVMe
  flush spike's drive is power-fail atomic for 512 B only, so the simulated disk also tears inside a
  4 KiB block: with tearing on, each 512 B sector of an unflushed block lands whole or not, and a
  segment header, which fits in one sector, lands whole or as it was.
- Silent bit flips by seeded probability over durable blocks, besides the chosen `FlipBit`: a flip in
  a record's data fails its CRC32C wherever the block is checked (recovery, a base read, a cleaner
  copy), and a flip in a header fails the header checksum. Open: whether every chunk read checks its
  block's CRC32C; if not, the byte-range oracle trips on a flipped block a read returns.
- Fail-slow: with no crash or other fault, every submitted item is acknowledged with a 100× fail-slow
  disk, no action completes with an error, and each completion takes 100 times its normal simulated
  latency. Base latencies are settings of the simulated disk: read 80 µs, write 20 µs, flush
  1,000 µs (a consumer drive's durable-write floor, from the NVMe flush spike), trim 20 µs.
- `DiskTrim` executes, since the cleaner's paced TRIM issues it. A trimmed block reads back as zeros
  or as its old contents, chosen per trim from the seed, as devices differ, and a trim is volatile
  until a flush like a write: a crash may undo it. A machine that relies on trimmed blocks reading
  as zeros fails under some seed. A failed trim leaves its blocks undecided, as a failed write does.
- Blocks a failed write or flush left undecided stay so until a later flush, which lands each from
  the seed; latency is one base time per action kind, whatever its size.

## Crash at every action

- [property] Disk log: 40 one- and two-block writes with two checkpoints (one with a compaction) on a
  1,024-block disk; for every k, crash at the k-th disk action with none, a seeded half and all of the
  unflushed blocks landing, and separately fail the k-th action with an Io error: each recovery holds
  every acknowledged write and is a prefix of the submitted ones (Pebble `TestErrors`,
  `TestDBCompactionCrash`).
- [property] Chunk store: a short, fixed workload on the single-node ChunkStore over a 65,536-block
  simulated disk (16-block segments): writes of 1 to 2 blocks to 4 chunks, unaligned writes of 1 byte
  to 3 blocks, a whole-chunk punch, a snapshot cut followed by a write (a freeze in its record), a
  checkpoint, a compaction, and a cleaner pass that moves at least one live and one frozen block.
  Between the cleaner's copy record and the owner applying the move, a write, a punch and a cut land
  on a chunk the pass is moving. Its disk actions are numbered 1..N in execution order. Open: N; it
  must stay small enough to run every k in every CI run.
- For every k in 1..N, three crash runs: crash at action k with no unflushed block landing, with a
  seeded half landing (seed k), and with all landing. Then recover (open) and, separately, rebuild.
- For every k, a failure run: action k fails (a write with Io, a flush with Io, a read with Io), the
  machine handles the error, then the node crashes with none, a seeded half and all unflushed and
  undecided blocks landing, as in the crash runs, and recovers.
- Expected for every run: Corrupt never occurs. The recovered state is a prefix of what was submitted
  (every record up to some LSN, none after); it holds every write acknowledged before the crash or
  failure; the chunk index, live and at every snapshot epoch, reads as a byte-range model of the
  kept records; open equals rebuild. A recovery whose own k-th action fails ends with Disk(Io) and
  writes nothing more, and the next recovery with no failure meets every check here.
- Rare actions are among the k: a manifest piece, the manifest's last piece, the superblock copy
  written at format, a segment header, a cleaner copy record, the record holding a compaction's
  output, and recovery's own writes. A second pass crashes recovery itself at each of its own actions
  (its header zeroing, its superblock repair, its final flush) and recovers again; both recoveries
  give the same LSN and index.
- After a failed flush or write, the trace shows no acknowledgment from that open log (the rule under
  Simulated disk).
- Coverage the test asserts it reached: a crash between a run and the manifest naming it, a crash
  inside a cleaner pass between its copy record and the owner applying the move, a write, a punch and
  a cut landing in that window, a crash in the round that opens a segment, and a failure of a
  manifest write.

## Crash oracle

- Per disk the oracle keeps every index's state at the last acknowledged LSN and the items submitted
  since, in LSN order (RocksDB's crash test: expected state plus trace). In phase 1 the index is the
  chunk index; from phase 4 each shard replica on the disk too.
- After a crash, recovery reports an LSN r. Required: acked ≤ r ≤ acked + in flight. The oracle
  replays its trace from acked to r onto the state at acked, and every index on the disk equals the
  result, with no hole. The window, with 7 acknowledged and 2 in flight:

  ```text
  LSN:    1   2   3   4   5   6   7 │  8   9  │ 10
          ──────────────────────────┼─────────┼────►
          acknowledged durable      │in flight│ 10: no record exists
  recovery may report 7, 8 or 9; never 6 (lost an acknowledged write);
  never 9 without 8 (a hole)
  ```

- Planted bugs, each in a copy of the package, each tripping the oracle on at least one seed of 50:
  recovery that stops one LSN early (reports 6); recovery that keeps a later in-flight record after
  dropping an earlier one (9 without 8); replay that applies a record twice where it is not
  idempotent (a cleaner move applied without its source check); a checkpoint whose replay LSN is
  past an index's oldest unflushed write (RocksDB #11148).
- The oracle's own state is not built from the code under test: it applies submitted items to a
  plain model, so a bug shared by the chunk index and the oracle cannot hide.
- The Recovery section's atomic-flush scenario (one checkpoint over two indexes and a shard replica)
  also passes through the oracle.
- Crash during recovery, then recovery again: the oracle's window is the same, and the second
  recovery reports the same r.
- The phase 5 real-disk crash test keeps the same oracle in its test driver, in a file outside the
  daemon it kills (Crash tests on real disks).

## Invariant checker

Each invariant is registered with the checker, checked after every step of every seed, and reports
its name, the seed and the step. Each has a planted violation, a one-line bug in a copy of the
package, that must trip it on at least one seed of 20; the fixed code trips none over the same
seeds. Open: the checker's cost per step and whether some invariants run every n steps.

| Invariant (phase 1) | Planted violation that must trip it |
| --- | --- |
| Acknowledged writes are readable, and no read returns a version later lost | Group commit reporting a round durable when an earlier round's write completes (the seed-1 bug), then a crash |
| No two writes of a chunk get one version | A restarted ChunkStore taking a chunk's next version from its runs, not its runs plus replay |
| Every read equals a byte-range oracle of the acknowledged writes and punches applied in version order, live and at every snapshot | A partial block merged against the committed version instead of the newest ordered one (wrong on every seed in the sub-block writes spike); a chunk's first write after a cut with no absent freeze (35 of 50 seeds in the chunk epochs spike) |
| Punched ranges read zeros | A punch whose partial first block is not merged, so the old bytes past the punch start stay |
| Every cleaner pass frees more segments than it fills | The net-gain guard removed, or age uncapped (the cleaner spike livelocked: 1,000 passes without gaining a free segment) |
| A cleaner move never replaces a write that landed during cleaning | A move applied without checking that the map still points at its source |
| No segment is reused while it holds a live block, or a piece, run piece or record that the newest manifest named by a durable segment header needs (a record after its replay points) | Allocation that takes a segment the usage table lists as used with live blocks; a cleaner that judges what is needed by the newest durable manifest, which no segment header names yet |
| After every recovery the usage table equals the log's own, and the next open sequence number is at least the loaded manifest's `NextSeq` | `NextSeq` taken from the log's next sequence number when the checkpoint begins, counting a segment a round in flight opened (the random-crash property fails at seed 1 in the [log segments study](../studies/log-segments/)) |
| A disk full except for its reserve still finishes a checkpoint, a merge step and a cleaner pass, and the bytes held beyond the manifests never exceed the reserve | A reserve sized without the merge-workers term, so a checkpoint during a merge fails with NoSpace; a merge step that installs its output only at the end, so the bytes held exceed the reserve |
| No ordinary write fails with `NoSpace` while the cleaner has a victim | Group commit failing held items with NoSpace as soon as only the reserve is free, without asking the owner whether the cleaner has a victim |
| A rebuilt chunk index equals the one recovered from its manifest | A rebuild that skips freeze entries |
| A chunk's live map never holds two entries for one block, with consistent record counts (`chunkstore.(*Index).Check`) | A trim that shortens the old extent but never splits it; a record count not lowered on trim |
| A rebuild from record headers maps every copy entry exactly where the owner applied its move, record counts included | A rebuild applying a copy entry as a plain write (maps block 0 back to v1, as in the Cleaner section's copy entries in replay) |

- The byte-range oracle keeps each chunk's bytes as the acknowledged writes and punches, applied in
  version order, left them, live and per snapshot epoch; it is a plain model, not the index.
- Open: the design's invariant "a frozen version is never dropped, and the cleaner never frees a
  block, while the live version, a snapshot or an unflattened clone needs it" applies in phase 1,
  since freezes and the cleaner exist, but is not in the checklist's phase 1 list. Planted violation:
  live counts that leave out frozen maps' blocks, so a segment holding only snapshot blocks is freed.
- Open: how the acknowledged-write invariant treats writes a misdirected-write fault destroyed (see
  Simulated disk).

## History recorder

- Every client operation is recorded as `(invoke_time, complete_time, op, result)`, from simulation
  and real clusters alike. In simulation the times are the scheduler's global time, not a node's
  `Now`, since a linearizability check needs one timeline and node clocks drift and are slewed by up
  to 22.6%. Open: the time base of real-cluster histories.
- The format is one Porcupine reads (Porcupine's `Operation`: client ID, input, call time, output,
  return time). A recorded history converts with no loss: op and result round-trip.
- Porcupine with a per-chunk register model: write A [10, 20], read A [15, 30], write B [25, 40],
  read B [45, 50] is accepted; write A [10, 20], write B [25, 40], read A [45, 50] is rejected, and
  the checker names read A.
- An operation that never completes (its node crashed) is recorded with no completion. Open: how an
  incomplete operation is written for Porcupine (an infinite return time is Porcupine's usual
  choice).
- Open: how pipelined chunk writes from one client are recorded, as one Porcupine client with
  overlapping operations or as one client per write in flight.
- Recording allocates nothing per operation on the data path. Open: where real-cluster histories are
  written.
- The consistency check of chunk histories (phase 3) and the file-level close-to-open check (phase 6)
  read the same format.

## Seed runner in CI

- CI runs thousands of seeds per commit; each failing seed prints a one-line replay command
  (`go test ./internal/... -run <Test> -seed <n>` or the runner's equivalent) that reproduces it
  exactly, and its ops file reduced to the fewest ops that still fail. Open: the seed count per commit
  and CI's time budget, the design's open question "Metamorphic cost in CI" (a 3,000-op seed costs
  about 99 ms per option set, and each set added costs a full run of every seed).
- Reduction drops chunks of ops while the failure persists. Targets from the metamorphic testing
  spike: the planted flush bug reduces from 3,000 ops to 5, and a second plant to 7, when flushes,
  failed flushes and restarts are ops. When an option sets the cadence (76 to 354 ops after
  reduction), the reduced case is rewritten with explicit ops before it is saved. Open: tolerance on
  the reduced size.
- The reduced case becomes a data-driven regression file under `testdata/` before the fix: the change
  shows the new case failing without the fix and passing with it. A fix that lands without its
  regression case fails review.
- A change that adds a seeded test runs it over 10,000 seeds once (as Pebble stresses every test a
  pull request adds); the run's result is recorded in the change.
- Millions of seeds overnight on idle machines: a failure found overnight produces the same one-line
  replay command and reduced file.
- A runner given a seed range shards it across processes, and the union of results equals a single
  process's run of the range (no seed skipped or run twice).

## Data-driven files

- `github.com/cockroachdb/datadriven` v1.0.2. A file holds commands, a `----` separator and the
  expected output; `go test -rewrite` regenerates the expected output for review, and a run without
  it fails on any difference, naming the file and line.
- The change that adds `datadriven` adds "Version pin: `github.com/cockroachdb/datadriven` v1.0.2" to
  `docs/tech-debts.md`, and the dependency passes the license check (Apache-2.0).
- Regression cases: the metamorphic spike's reduced plant as a file, which fails against the planted
  copy and passes against the real package:

  ```text
  # P1, seed 0 under A: a flush dropped a removal that hid a key in an older run.
  run set=A
  put aa v26x
  flush
  del aa
  flush
  floor aca
  ----
  floor aca: none
  ```

- Compaction picking: the universal picker cases ported from RocksDB's `compaction_picker_test.cc`
  (listed under Compaction) as data-driven files, each naming its source test and RocksDB version.
- Range-removal fragments: a memtable's fragments after a sequence of puts and range removals,
  printed as fragment lists, including a later write cutting its key out of a fragment.
- Scripted recovery: a disk image built by commands (format, write, flush, crash with a landed set,
  flip a bit) and the recovery result printed (LSN range, Corrupt LSN, quarantined segments); the
  Recovery section's torn-tail and depth-2 cases as the first files.
- An unknown command or a malformed argument fails the test with the file and line, never a silent
  skip.

## Planted-bug check

- The design's done-when checkpoint: "Simulator replays a failing seed exactly, and finds a bug
  planted in a toy replication protocol." The deterministic scheduler drops and reorders the toy
  protocol's messages, ahead of phase 3's simulated network. The learning project's warm-up is a
  sans-IO echo machine plus a tiny simulator that drops and reorders messages.
- Setup, for example: a toy primary-backup register whose messages the scheduler drops, delays and
  reorders, with crashes and restarts, and the invariant "every acknowledged write is readable after
  any crash". Planted bug, for example: the primary acknowledges before the backup's ack. Open: the
  design names neither the toy protocol nor the bug.
- Expected: a seed within the per-commit seed count trips the invariant (Open: that count); running
  that seed twice gives byte-identical traces up to the failure and the same failing step; the fixed
  protocol passes the same seeds and 10,000 more.
- The toy protocol passes the sans-IO analyzer and allocates nothing per event.

## Cleaner write amplification in simulation

- Setup, from the cleaner spike: a 16 GiB simulated disk (4,096 segments of 4 MiB, as min(64 MiB,
  disk ÷ 4096) gives), uniform random 4 KiB overwrites over a volume sized to the fill, warm-up of 6
  disk capacities of writes before measuring. Write amplification counts user, header and copied
  blocks over user blocks.
- Targets, cost-benefit with two streams:

  | Fill | Column 1: no index runs written or counted (cleaner spike) | Column 2: index runs in the metadata stream (cleaner streams spike) |
  | --- | --- | --- |
  | 80% | 3.4 | 3.9 (3.86) |
  | 90% | 7.2 | 8.0 (8.04) |
  | 95% | 17.3 | 26 (26.4) |

  The cleaner spike modelled no index runs; the streams spike's own baseline without them, at 1 MiB
  records, was 3.35 / 7.15 / 17.3. Column 2 needs the Metadata stream item. Open: how the Go log runs
  without index runs for column 1, since it always checkpoints. Open: tolerance ("land near" in the
  checklist). Open: the spikes' records were 64 KiB (C1) and 1 MiB (streams), with 64 B headers and
  32 B per entry, and they kept 8 segments free with 4 victims per pass; the Go log has a 48-byte
  prelude (56 with the durable LSN), 33 B per entry, `clean_free` 16 and its own reserve, so the setup
  must state which it matches.
- With the index's runs mixed into the cold stream, the streams spike measured 4.27 / 11.0 / 49.6;
  the real layout must stay near the metadata-stream figures, not these.
- The disk's exported write amplification metric equals the run's own count (user, header, copied
  and index-run blocks over user blocks) over the measured window, so the drive wear figures in
  design.md read the same number an operator does.
- The run reports cleaner CPU per user 4 KiB write against the spike's order of magnitude (about 2.8 /
  7.5 / 20 µs at 80 / 90 / 95%). Open: tolerance.
- Invariants hold throughout (Invariant checker), and no ordinary write fails with NoSpace while the
  cleaner has a victim.

## Seeded runs and the simulation harness

The seeded runs, each described in its section above:

- Group commit: seeds 1-100 (every accepted item decodes), each also failing under five broken
  encoders, and seeds 1-20 of random entries (anything Submit accepts decodes).
- Random crashes: seeds 1-50 x 4 cycles (acknowledged records kept, index = rebuild), and the recovery
  fuzz test.
- Quarantine: seeds 1-10 (bit flips in segment headers).
- Record codec: seed 1 (round trip, 499 records), seed 4 (exhaustive torn subsets, 40 records x 3 old
  contents), seed 5 (torn 512-byte sectors, 300 records).
- IDs and hashing: seed 7 (PG splitting, 64 chunk IDs).
- LSM tree: seed 31 (blocks), seeds 0-11 (puts and removes), seeds 0-5 (views), seed 11 (flush to a
  run), seed 12 (a flush keeps its frozen view).
- Chunk index: seed 21 (key order), seed 22 (cleaner moves over 3 chunks), 200 seeds (replay past
  pending writes, chunk epochs spike R1), seeds 0-3 (recovery on a 1 GiB simulated disk).
- Metamorphic index tests: seeds 0-199 x 12 option sets.
- Crash at every action: every k, with three landing sets and a failure each.
- Cleaner write amplification at 80 / 90 / 95% full.
- Planted-bug check: seeds 0-99 and the replay of its failing seed.

The chunk store stand-in of the recovery tests applies every Durable record to its model of the
chunk index. It starts a checkpoint whenever one is due, snapshots the model, hands one run piece at
a time, and the next only after the previous piece is durable, each piece naming the one before. It
ends the checkpoint after the last piece, and after a CheckpointFailed begins the next once a record
has become durable since. With no cleaner, the harness reports no victim whenever the log is
stalled. Write payloads
are a pattern derived from (chunk, version, block), so any extent can be checked by reading it back.

## TLA+ and TLC

- Every specification under `models/` has a TLC configuration checked in beside it that names its
  constants (nodes, PGs, values), its depth or state bound, and the properties and invariants it
  checks; CI runs TLC on each one and fails on any violation, deadlock the specification does not
  allow, or a run that stops short of its bound.
- Each model's checked depth and distinct-state count are printed by the CI job, so the
  [Model-to-simulator map](#model-to-simulator-map) can state the depth every model was checked to.
- A planted fault fails CI: one model run with a configuration that removes one of its rules (for
  the [Epoch change model](#epoch-change-model), any row of its counterexample table) reports the
  violated property and a trace, and that configuration is kept as a negative check that must keep
  failing.
- TLC is pinned to release 1.7.4, with its entry in [tech-debts.md](tech-debts.md), and no model
  uses a CommunityModules operator that needs the 1.8 pre-release. The template the models follow
  is etcd-raft's own specification (`tla/` in its repository).
- Every model has these runs ([Testing strategy](design.md#testing-strategy)):

  | Run | What it checks | When | Budget |
  | --- | --- | --- | --- |
  | `ci` | Exhaustive: safety properties, and liveness with a time bound written as an invariant over the model's clocks | Whenever `models/` or the TLC version changes, with the negative variants, each of which stops at its first counterexample | Under 5 minutes with 4 workers on the CI runner |
  | `deep` | Exhaustive, with one more node, chunk, crash or term, plus the unbounded liveness properties | Once per change to its spec, configuration or TLC version; the nightly job compares a hash of the three | 4 hours, after which the run fails |
  | Nightly simulation | `-simulate`, depth 100 or more, a fresh seed printed for replay | Every night, on machines not running the performance runs | About 1 hour per model |

- A `ci` run that passes its budget fails CI, and the fix is to move its largest constant to
  `deep`, not to raise the budget.
- A spec checked in with no `ci` or no `deep` configuration fails CI.

## Epoch change model

TLA+ in `models`, one spec checked with TLC in the runs of [TLA+ and TLC](#tla-and-tlc). Each
checked-in configuration states the depth or state bound it was checked to. The [Model-to-simulator map](build-checklist.md#phase-2-protocol-models) item writes which
simulator invariant checks each property; the invariants under [Placement faults and
invariants](#placement-faults-and-invariants) carry the number of the property they check. Lease
constants are as at the head of [Peering and read leases](#peering-and-read-leases), in abstract
ticks in the same ratios.

**Configurations.** Property 1 needs peering and leases together, so the spec is not split; its
`ci` runs are two configurations, each heavy on one part.

| Configuration | Nodes | What it holds |
| --- | --- | --- |
| `ci-leases` | 3 | 1 chunk of 1 block, no merges; both clocks per node in full; every interval's members and activation |
| `ci-peering` | 3 | 2 PGs and 2 chunks; lease expiry left open, except that a grantor that joins peering of an interval holds no live grant to that interval's old primary |
| `deep` | 4 | Both together, and the unbounded liveness properties |

| Constant | Value |
| --- | --- |
| Nodes | 3 in `ci-leases` and `ci-peering`, 4 in `deep` |
| PGs | 2, sharing nodes, so a table that changes one PG is seen by the other PG's members |
| Pools | `rep3` with `min_live` 2; and `rep2` with `min_live` 2 and `maintenance` |
| Chunks per PG | 1 or 2, each with 1 or 2 blocks |
| Counter limit | 3, so running out is reachable at small depth; the real limit is 2³² − 1 |
| Clocks per node | One for `CLOCK_BOOTTIME`, slewed by up to ±22.6% or running at the raw clock's rate (no daemon), and one for `CLOCK_MONOTONIC_RAW`, drifting by up to ±4% |

**State the model holds**

- **Tables.** A sequence of tables. Each PG entry holds the PG's interval number, its last activated
  interval and the members of the intervals since. Nodes adopt tables in order, may skip some, and
  never go back.
- **Messages.** Each is tagged with the sender's table epoch and the PG's interval number in that
  table. A member accepts a message only in the PG's current interval, and catches up to a table at
  least as new before it judges a higher number. A primary takes a client write whose number is at
  least the one its own term began in.
- **Peering.** It reaches a member of every interval since the last activated one that had at least
  `min_live` live members. It reads members' missing lists and keeps every version below a kept one.
- **Disks.** Each member is a disk ID bound to a log ID. A disk formatted again keeps its node but
  empties its log and draws a new log ID, which the authority treats as a new disk ID; the old disk
  ID stays down.
- **Activation.** Members persist their missing lists, the primary then activates and reports it on
  a heartbeat, and the leader folds the report into a later table that starts no interval.
- **Restarts and counters.** A restarted primary takes a new interval. A chunk whose counter runs
  out lets the PG's pending writes commit, then asks for a new interval.
- **Writes.** A chunk's pending writes are each merged on the version below them. Connections are
  per-process channels: a write reaches only the primary process its connection was opened to. A
  connection's hello and every chunk write and punch carry the session and incarnation. A primary
  holds client writes when an interval ends under it and orders them again once it has re-peered,
  only if no interval since had another primary; it checks incarnations when it orders a write.
- **Leases.** Leases are per node pair and counted on two clocks per node. Grants are delayed by any
  amount. The holder bounds each grantor's rate from any two grants of one grantor boot, asks
  again `rate_gap` after a window's first grant, and takes no lease from a grantor before it has a
  bound. Every grantor message carries its raw clock and boot ID, the step check trips at
  `1/(1 − m)` with `m` the margin of the lease from the last grant, and peer messages are handled
  before reads.
- **Clocks.** A suspend stops a node's raw clock while `CLOCK_BOOTTIME` gains the sleep, possibly up
  to 1 s short. A VM pause freezes every clock, with or without a marker change.
- **Leadership.** Raft leader fortification uses the same leases, with support epochs, the restart
  wait and leader transfer.

**Faults** ([PG epochs study](../studies/pg-epochs/README.md), [open
questions](design.md#roadmap-and-open-questions))

- A member removed and added back.
- Two interval changes before the first new primary peers.
- An interval with fewer than `min_live` live members.
- An interval short of `min_live` only by members in `maintenance`: it accepted writes and is not skipped.
- A replica replaced under a primary that stays.
- A primary restart.
- A counter running out.
- A split with writes in flight.
- An activation report lost or late.
- Recovery cut short by the next interval.
- An interval that never activated.
- A member live in the last activated interval that its peering never reached, returning after a
  later interval and holding versions the others dropped.
- A message duplicated or held for any time.
- A member's disk formatted again, or swapped for a new drive, while its node stays up.
- A suspend, or a VM pause with every clock frozen.
- A grantor that reboots within `rate_window`, its raw clock restarted near 0 and its rate changed
  by 2%.
- A grantor's raw rate stepping by up to `lease_drift / 2` after it was measured (an allowed fault).
- A dead daemon's chunk write delivered late to an old primary in interval `j`, or held by the
  primary when the successor's hello reaches it.
- A primary P holding a dead daemon's write W pauses past `node_down_after`. Interval k makes Q
  primary, the successor's reissue W′ and the pod's next write W2 to the block complete there. P
  resumes, fetches the whole table after the gap, and is primary again in interval m.
- Interval 41 is activated with primary A. Interval 42 replaces B with D and does not activate, and
  A serves reads under C's and D's grants. Interval 43 moves the primary to C, and A and B no longer
  share a PG.
- A grantor that has peered a new interval moving a PG's primary away, then grants the old primary
  under a fresh support epoch while the old primary still holds the older table.

**Properties.** TLC checks every property in `deep`, and every property but 10 in both `ci` configurations; property 10, lease liveness, needs lease expiry, which `ci-peering` leaves open, so it is checked in `ci-leases` and `deep`. A counterexample row whose
property is 1, 2 or 10 runs as a variant of `ci-leases`, every other row as a variant of
`ci-peering`.

1. No PG serves reads from two primaries at once.
2. No two replicas of a shard serve lease reads at once.
3. No write tagged with an interval completes after a later interval of its PG activates.
4. No acknowledged version, and no version an activated interval kept, is ever dropped.
5. The versions peering drops are a suffix of each chunk's pending versions, so no kept version was
   merged on a dropped one.
6. No two writes of a chunk get one version, and a chunk's versions grow.
7. A table that leaves a PG's interval alone never fails that PG's writes.
8. A new primary merges into a chunk only once it holds every version peering kept for it.
9. No node's table epoch or interval number goes backwards, and no PG's last activated interval goes
   down: a late report for 42, folded after 43 was recorded, changes nothing.
10. A node pair whose raw rates differ by up to 4%, with no pause, holds a lease within `rate_gap`
    plus two round trips of first contact, and keeps it with no step-check trip. The bound is in
    time, so `ci` checks it as an invariant over the model's clocks.
11. Once a dead daemon's reissued write W′ and a later write W2 to the same block are acknowledged,
    the block never again holds the dead daemon's W.

**Counterexamples.** Each row is a variant of the spec with one rule removed. TLC must find a
violation of the named property, which is never the property that only restates the removed rule. A
variant that passes means the model is too weak.

| Rule removed or broken | Property broken | Minimal trace |
| --- | --- | --- |
| A member logs one version of a chunk out of order (41.9 before 41.8) | 5 | Peering drops 41.8 but keeps 41.9, which was merged on it |
| A new primary merges before it holds a kept version | 5 or 4 | Recovery's whole block overwrites the new write's bytes, or the new write merges on the stale block (sub-block spike M8) |
| Peering stops only the previous interval | 3 | 41 → 42 → 43 before 42's primary peers; 43 reaches only a member live in 42 and not in 41; a write tagged 41 completes |
| Members compare member lists instead of interval numbers | 3 | [A,B,C] → [A,B] → [A,B,C]; C missed 42's writes, and the old primary's write tagged 41 collects C's acknowledgment |
| A disk ID kept across a new log ID | 4 | 41.7 acknowledged on A, B and C; C's disk is formatted again and reports under its old ID; A fails; B peers 42, and C, live in 41, lacks 41.7, so B drops an acknowledged write |
| A restarted primary peers in its old interval | 6 | A write from its previous run, still in a member's socket, lands after peering under a version the new run gives to other data |
| An older version is decided by its own interval's members after a later interval kept it | 4 | 42 keeps 41.7 and starts recovering it onto D; 42 ends; the next primary reaches only B and D, and 41's rule drops what 42 kept |
| A primary activates before missing lists are durable | 4 | A member crashes after activation, forgets that it lacks the kept version, and serves the older one |
| The holder ends its lease on the slower of its clocks | 1 | Holder `CLOCK_BOOTTIME` −22.6%: the lease outlives the grant by about 1.1 s |
| The grantor ends its grant on the faster of its clocks | 1 | Grantor `CLOCK_BOOTTIME` +22.6% |
| A lease before the first bound, on `lease_drift` alone | 1 | Holder's clocks both −1%, grantor's both +1%, a partition right after the first grant: the lease outlives it by 39 ms |
| That, with the step check at `1 + lease_drift / 2` restarting the window on each trip | 1 | Grantor's clocks both +1.2%: every heartbeat trips the check, the pair never gets a bound, a partition leaves 7 ms of overlap |
| Widening only past `1 + 2 lease_drift` | 1 | Grantor's clocks both +2%: the margin stays 1% where 1.96% is needed, and a partition follows a grant |
| The margin `max(lease_drift, 1 − 1/B)` | 1 | Pair measured at +1.5%, then the grantor's raw rate rises 0.4% and a partition follows |
| No new window at a grantor's new boot ID | 1 | Grantor reboots within `rate_window` and its new boot runs 2% fast; its first grant gives a negative bound and the step check sees `g − g_a < 0`; a partition leaves 38 ms of overlap |
| The step check at `1 + lease_drift / 2`, with no lease before a bound | 10 | Grantor's clocks both +2%: the check trips on every grant after a window's first, so no lease ever forms |
| No end of leases on `Resumed` | 1 | Suspend past `primary_lease`, then a read on resume |
| No step check on grantor messages | 1 | VM stop on a kvm-clock guest, messages queued, marker unchanged |
| Step check on grants only, not on every heartbeat | 1 | VM pause spike T6b: a grant in flight across the stop was stamped before it |
| Peer messages handled after reads in a pass | 1 | kvm-clock stop with queued grantor heartbeats and a client read queued behind them |
| The support epoch is not bumped on lapse | 1 | Partition, the grant lapses, the grantor helps peer, the partition heals, and the renewal continues the old grant |
| No restart wait | 1 | A grantor crashes right after granting, restarts and joins peering at once |
| Peering reaches only the last activated interval and the current one | 1 | The fault with intervals 41 to 43 above: B's grant to A lapses and B, live in 41, joins 43's peering at once; D, live in 42 and still bound by its grant to A, is never reached, and A serves reads while C accepts writes |
| Incarnations checked when a write arrives, not when it is ordered | 11 | P holds W; the successor's hello reaches P; P orders W after W′ and W2 |
| A held write ordered again although another primary led the PG since | 11 | The pause fault above: P has never seen the successor's incarnation, and orders W after W′ and W2, which completed on Q in interval k |
| A fresh grant covers the pair's PGs with no table check | 1 | B peers 42 (PG 17 moves to C); B's grant to A lapses, epoch 7 → 8; A, still on 41's table, asks under 8; B grants; A serves PG 17's reads while C accepts writes |
| Leader transfer without ending the old leader's lease at `MsgTimeoutNow` | 2 | Old and new leader both serve lease reads |

- `ci-leases` decides which grant rule passes: a grant that names the grantor's table epoch and is
  used only for PGs whose interval the holder holds in that table, or a grantor that refuses a grant
  until the holder reports a table at least as new (open question "Which PGs a node-pair grant
  covers"). The passing rule is named in the Grant row under [Wire messages](#wire-messages) before
  phase 3 builds it.
- Expected in `ci-peering`: with incarnations checked when a write is ordered and held writes
  ordered again only if no other primary led the PG since, property 11 holds with no table of
  incarnations that survives a primary change, and the two incarnation rows above each fail (open
  question "Incarnation fencing in the epoch-change model").
- Open: a VM stop whose queued messages the hypervisor drops or delays leaves up to a heartbeat of
  stale reads (open question "Stops in place on QEMU guests that run on kvm-clock"). The model
  either leaves that combination out with the reason stated, or expects property 1 to fail in it.
- Open: a grantor's raw rate rising past the headroom (`lease_drift / 2`), as on a clocksource
  switch or a live migration onto another host, followed by lost or delayed heartbeats, overlaps by
  up to `(Δρ − lease_drift / 2) · primary_lease`; so can a pause just under the step check's
  threshold seen only by a heartbeat late in a lease, by up to that heartbeat's one-way delay (open
  question "A grantor's raw rate rising past the lease margin's headroom"). The model excludes it
  with this reason, or expects property 1 to fail.

## Sessions and fencing model

A TLA+ specification, checked with TLC. It covers the [Sessions and fencing](design.md#sessions-and-fencing) protocol and the [session expiry study](../studies/session-expiry/README.md)'s safety argument, which so far is prose.

**What the model contains**

- Actors: client daemons, each with one session; a control plane with at least two leaders in turn (terms); storage nodes acting as primaries and replicas; and an inode owner holding one lock.
- Clocks. Each node's clocks advance at a rate within a bound of real time, and a node's clocks can also stop (a VM stopped with every clock frozen).
  - The holder (the client) counts from the send of its last answered renewal and ends `lease_drift` early.
  - The grantor (the leader) counts the whole `session_lease` from when it processed the renewal.
  - The holder ends its lease on `Resumed` and when a grantor's clock moved more than `1/(1 − m)` times its own since the last grant, `m` the lease's margin (design.md, Lease time).
  - The holder keeps its rate bound per leader node and boot: a leader failover starts a new window while the client keeps its old lease.
  - Whether the model carries both clocks per node with the 22.6% slew, or only the abstract rate bound, is the modeller's choice. The epoch change model checks the two-clock rule itself.
- Leader leases: only a leader holding its leader lease answers renewals, and a deposed leader's lease ends before its successor's starts.
- Session table: versions, each listing the live sessions and those fenced but not yet acknowledged, plus an acknowledged version.
- Storage nodes:
  - each applies table versions in order and acknowledges the version it holds;
  - each rejects writes from fenced or removed sessions, including writes already queued and writes replicated to it by a primary;
  - a node can be marked down in a committed table, and is marked up only once its heartbeat reports the current version.
- PG intervals:
  - a write completes only once every live member of its PG's interval holds it;
  - a committed down mark starts a new interval of every PG with a member on that node;
  - members of the new interval refuse writes tagged with the old one.
- Writes: a write from a client carries its session ID and can sit in the network for any time.
- Locks: an owner grants a lock to the next holder only once the old holder's session has left the table, that is, once its fence version is acknowledged.
- Faults:
  - message loss, delay and reordering;
  - client daemon pauses of any length with clocks running, and VM stops with clocks frozen;
  - a leader crash and failover, a deposed leader still answering renewals, and a leader stalled past `session_lease`;
  - a storage node partitioned.

**Properties.** Each names the simulator invariant that checks it in the real code, for the model-to-simulator map.

| Property | Simulator invariant (design.md Testing strategy) |
| --- | --- |
| No fence commits while the client's own lease still runs | "no session is fenced while its client's own lease still runs" |
| No fenced session's write is applied after the storage node applied the fence | "no fenced session's write is applied" |
| No lock reaches a new holder while a primary would still accept and complete the old holder's writes | "no lock is granted to a new holder while a primary would still accept the fenced holder's writes" |
| At most one lock holder | "at most one lock holder per lock" |
| A session whose client stops renewing is fenced, and its lock released, within `session_lease` plus one commit and one acknowledgment round, or once its unreachable storage node is marked down. `ci` checks it as an invariant over the model's clocks; `deep` also checks the unbounded form, that it is fenced eventually, under weak fairness | To register: "a dead client's locks are free within `session_lease` plus one commit and one acknowledgment round, or once an unreachable storage node is marked down" (design.md states this in Sessions and fencing, not in the invariant list) |

**A rule removed, and the counterexample the model must find.** Each row is a model variant that must fail. If TLC finds no counterexample, the model is too weak.

| Rule removed or changed | Counterexample the model must find | Property violated |
| --- | --- | --- |
| The client counts its lease from receiving the reply, not from sending the renewal | The reply is delayed by more than the margin. The client's lease then ends after the leader's count, and the leader fences while the client still writes | No fence while the client's lease runs |
| The client does not end its lease `lease_drift` early | Its clock runs fast within the rate bound and it overlaps the fence | Same |
| The client ignores `Resumed` | Its VM stops for 15 s with every clock frozen. It resumes believing 5 s of lease remain, and writes after the fence commits | Same |
| A leader answers renewals without holding its leader lease | A deposed leader renews C after the new leader's lease began. The new leader's timer for C started earlier, so it fences C while C's lease runs | Same |
| A new leader expires sessions from the last renewal time it can see, instead of giving every live session a full `session_lease` from the start of its own lease | It fences a client renewed by the old leader moments before the failover | Same |
| Owners release a fenced session's lock when the fence commits, not when its version is acknowledged | The next holder writes while a storage node that has not applied the fence still applies the old holder's queued write | No lock to a new holder while a primary would still accept the old one's writes |
| A storage node applies a version only to writes that arrive after it | A write queued before the version is applied after it | No fenced write applied |
| A replica does not check the fence on replicated writes | A primary that has not yet applied v1 replicates the fenced write, and the replica that applied v1 stores it | Same |
| A storage node marked down is marked up again with an older session table | The rejoined node accepts the fenced session's late write | Same |
| A down mark starts no new interval | The partitioned node, as a primary, completes the fenced client's write after the lock moved | No lock to a new holder while a primary would still accept the old one's writes |
| The acknowledged version counts only reachable nodes, without a committed down mark | Same as the owner-release row | Same |

**Scripted scenarios.** Each is a model trace to check and a simulator seed to keep. They start from the study's list.

1. **A renewal answered just before a control-plane failover.**
   - C sends a heartbeat at 0 s. Leader L1 processes it at 0.002 s and answers, and L1 crashes at 1 s.
   - C's heartbeats from 1 s on go unanswered. The voters' grants to L1 lapse at about 5 s (up to about 6.2 s with a grantor slewed 22.6%), and the election adds 1 to 2 s. L2's leader lease therefore starts at 6 to 7 s, or up to about 8.2 s with a slewed grantor, which is still before C's own lease ends at 9.9 s.
   - L2 gives every live session a full 10 s from that start and answers C's next heartbeat. L2's node is a new grantor for C, so C starts a new rate window, sends its next renewal `rate_gap` (100 ms) after that answer and takes its lease from L2 once it has a bound, still under its old lease.
   - Expected: no fence, and C keeps issuing writes throughout.
2. **The same, with a longer outage.** No leader answers until 12 s. Expected: C stops writing at 9.9 s and holds its FUSE and ublk requests, with no application error. It resumes when a renewal is answered, and no session is fenced.
3. **A client paused past its lease with a write already queued.** C's last answered renewal was sent at 0 s. C pauses at 1 s with a write W in the network to primary P. The leader's count runs out at about 10.0 s, and it commits fence version v1.
   - (a) W reaches P before P applies v1. W is applied, and the lock is released only after every storage node has acknowledged v1, so W is ordered before the next holder's writes.
   - (b) W reaches P after P applied v1. W is rejected.
   - (c) W reaches P after the acknowledged version was committed. W is rejected.
   - (d) W reaches P before P applies v1, but replica R applied v1 first. R rejects the replicated W, so W is never acknowledged, and the lock is still released only after every storage node acknowledges v1.
   - Expected in every case: the next holder's write is ordered after W, or W is refused.
4. **A client VM stopped with its clocks frozen.** C's VM stops at 2 s for 15 s. Its session is fenced at about 11 s. On resume, C ends its lease before serving any request (`Resumed`, or the step check on the leader's next reply: its raw clock moved more than `1/(1 − m)` times C's since the last answered renewal), and holds its requests until a renewal is answered. That renewal says it was fenced. Expected: no write from C is applied after the fence.
5. **A fence while storage node N3 is partitioned.**
   - N3 is partitioned at 0 s and marked down at 5 s, which starts a new interval of every PG with a member on it. C's session expires at 10 s.
   - v1 is committed as acknowledged as soon as every other node has acknowledged it, since N3's down mark is already committed.
   - The partition heals at 20 s, with N3 still holding the session table from before v1.
   - Expected: N3 is not marked up until its heartbeat reports v1. Until then it completes no write, and peering stops any write of C's still tagged with an old interval.
6. **Two sessions expiring in one batch.** A switch failure strands nodes X and Y at 0 s, and both sessions fall due within the same timer firing. Expected: one session-table version fences both, one acknowledgment from each storage node covers it, and one acknowledged commit follows. Variant: Y falls due 0.5 s after the timer fired for X. Expected: two versions. A node acknowledging the newer one covers both.
7. **A client that reaches the control plane but not the owner.** C renews normally but leaves a recall unanswered. After `recall_timeout` (30 s) the owner asks the leader to fence C. Once the fence is committed and acknowledged, C's recall counts as answered and its lock is free. C learns it was fenced from its next renewal, starts a new session and reopens its files. Its applications see errors, never stale data.
8. **A leader stalled past `session_lease`.**
   - The leader's loop stalls for 12 s while clients keep sending heartbeats. The stall also outlasts its 4 s leader lease, so the followers' grants lapse and they may elect L2.
   - Expected on waking: the old leader answers no renewal until it holds a leader lease again. If L2 was elected, L2 gives every session a full lease from the start of its own lease.
   - Whichever leader answers restarts every session's timer from now and fences no one. The study sets the threshold for a stall at more than a heartbeat interval (session expiry study, "Failover and stalls"); design.md gives none.
   - A client that truly died during the stall is fenced about 10 s after that restart.
9. **A new session's first write reaching a primary with an older table.** Session S7 starts at a table the primary has not yet received, and its write arrives first. Expected: the primary fetches the session table before judging the write, then accepts it, rather than rejecting it as an unknown session.
10. **A renewal answered by a deposed leader.** L1 is partitioned from the other voters but still holds its leader lease until t_L. It answers C's renewal at t_L − 0.1 s. L2's leader lease starts after t_L and gives C a full lease from that start. Expected: C's lease, counted from a send before t_L, ends before L2 could fence it.
11. **Fence latency.** A client node crashes at 0 s. It is marked down at 5 s, and its session falls due at about 10 s. Expected: its locks are free at 10 s plus one commit and one acknowledgment round, with no wait for the down node. Open: the design says "milliseconds on a LAN" without a bound; the simulator's assertion needs one.
12. **A leader whose raw clock runs 2% fast.** No time daemon on L1's node, so both its clocks run 2% fast. C bounds L1's rate from two renewals `rate_gap` apart and widens its margin to 2.5%. Expected: no step-check trip on any renewal, C's lease ends 9.75 s after each answered renewal's send, before L1's count, and C keeps its session and writes with no stall.

**Simulator faults covering the same ground** (design.md, simulation faults for coherence and placement):

- a fence that reaches some primaries late;
- a control-plane leader failover while sessions renew;
- a renewal answered by a deposed leader;
- a client that reaches the control plane but not an owner;
- a control-plane leader stalled past `session_lease`;
- clients paused past their lease;
- a holder whose VM stops for longer than a lease with every clock frozen.

**Configurations** (runs as in [TLA+ and TLC](#tla-and-tlc)): `ci` with 2 clients, 2 leader terms, 2 storage nodes, 1 owner, 1 lock, and up to 2 writes in flight per client; `deep` the same with 3 leader terms.

## Cache-mode recall model

A TLA+ specification of [Tier 2](design.md#cache-coherence-tiers) for one file. It uses the simulated FUSE client's kernel model from Testing strategy: a page cache per node that refills on reads, direct I/O fixed when a handle is opened, attribute timeouts, `auto_inval_data`, and mapped pages that are not revalidated.

**What the model contains**

- Two or three nodes. Each has a daemon with a write-back buffer, and a kernel with handles, each handle cached or direct I/O.
- An inode owner with the open registry (members, which have written, the mode). The owner can fail over, with a grace period of one session lease.
- Storage: one versioned value and the file's size, written through the chunk primary. A write that extends the file commits the size before it is acknowledged, and reads are clamped to the size.
- Recalls: sent, delivered, lost or delayed. Each is acknowledged, or ends when the member's session is fenced (by expiry, or by `recall_timeout`).
- Sessions, abstracted from the sessions model. A client daemon pauses past its lease while its node's kernel and applications keep running, then is fenced. No kernel timeout reaches past the session lease.

**Properties**

- No stale read in WriteShared mode: a `read()` on any unmapped handle, invoked after a write was acknowledged to any node, returns that write's bytes or newer ones. Simulator invariant: "no client reads stale data from a file in WriteShared mode".
- Close-to-open in every mode: an open that starts after another node's close sees that close's data. The consistency checker checks it in simulation.
- Open: mapped reads. The design promises them within one message delay unless `mmap_sync` is on, and its simulator models mapped pages, but whether this model checks "a mapped read issued one message delay after a write's acknowledgment returns it" or leaves mmap out is the modeller's to state.

**A rule removed, and the counterexample the model must find**

| Rule removed or changed | Counterexample (spike test where it was seen) | Property violated |
| --- | --- | --- |
| Step 4 dropped: no revalidation of old handles (attributes with timeout 0 and a new change counter in `mtime`) | B's cached handle, opened in ReadShared, refills its page cache after the recall and reads stale data after A's next write (T3) | No stale read in WriteShared |
| Step 3 dropped: new opens on B stay cached | A handle B opens after the switch reads cached pages (T4 shows the flag is fixed at open) | Same |
| Step 2 dropped: the recall does not drop cached pages | B's next read hits the old page (T1) | Same |
| Step 1 dropped on the writer's side: from Exclusive to WriteShared, the recalled writer A does not flush its buffer | B opens and reads storage before A's acknowledged buffered write reaches it | Same, and close-to-open |
| The owner answers the triggering open or write before every acknowledgment or fence | B reads before A has flushed or dropped its pages | Same |
| A lost recall counts as answered after a timeout, with no fence | B keeps its cached pages and reads stale data | Same |
| A kernel attribute timeout reaches past the client's session lease | B's daemon pauses, and B is fenced. Its kernel still trusts cached attributes and serves old pages to a running application | Same |
| After owner failover, the new owner accepts new opens before the grace period ends | It misses B's open and sees A alone. The file goes to Exclusive, A buffers and caches, and B reads stale data | Same |
| WriteShared returns to Exclusive while two nodes still have the file open | The returning node buffers writes the other node cannot see | Same |
| An extending write is acknowledged before its size commits | B's `read()` past the old end of file is clamped to the old size and misses acknowledged bytes | Same |
| Tier 1's open keeps `keep_cache` although the file changed since this node cached it (T6) | An open after A's close reads B's old pages | Close-to-open |

**Scripted scenarios**

1. **The two-pod example (design and post 09).**
   - `web` on node B opens `status.json` first: Exclusive. `uploader` on node A opens it: ReadShared.
   - A writes "BBBB". The owner recalls B, and B flushes, drops its pages, opens new handles with `direct_io` and revalidates old handles.
   - B acknowledges, and the mode becomes WriteShared. B's next `read()` on its old handle returns "BBBB".
   - With step 4 removed, B's read after the recall refills its cache with "BBBB", and it still returns "BBBB" after A writes "CCCC".
2. **Exclusive to WriteShared on a second open.** A has written and holds buffered data, and B opens the file. The owner recalls A, which flushes and drops its pages. The owner answers B's open, uncached, only after A's acknowledgment. B's first read returns A's buffered bytes.
3. **The writer unreachable.**
   - As in scenario 2, but A's node is partitioned. B's open waits up to A's `session_lease` (10 s), plus the time to fence A: one commit and one acknowledgment round.
   - Variant: A still renews its session but does not answer the owner. B waits `recall_timeout` (30 s), then A is fenced.
   - Expected in both: B's open completes only after the fence is acknowledged, and B reads storage without A's unflushed writes. Those writes are lost, and reported to A's handles as errors.
4. **Lost recall.** The recall to B is lost and no answer comes. Expected: the owner waits for an acknowledgment or a fence, never for a timeout alone.
5. **Delayed recall.** The recall reaches B after B closed and reopened the file. Expected: no stale read. Open: recalls carry no stated generation, so it is unspecified whether an acknowledgment of an older recall counts for a newer one.
6. **Owner failover mid-recall.** Owner O1 has sent a recall to B and crashes before B acknowledges. O2 becomes the owner and, for one session lease (10 s), accepts only re-registrations. A and B re-register. Expected: no stale read before or after, and no new open answered during the grace period. Open: whether a re-registration carries "has written", and whether O2 sends the recall again or waits for it.
7. **Leaving WriteShared.** B closes. The owner returns the file to Exclusive with no recall, and A's new opens are cached and its writes buffered again. A handle A opened with `direct_io` while the file was WriteShared stays uncached until A reopens it. Stored `mtime` 1,000 s; while WriteShared, B's last `stat` reported 1,000.000000040 s and A's 1,000.000000025 s. B's `close` stores 1,000.000000040 s, and A's next `stat`, no longer bumped, reports no less than 1,000.000000040 s. A planted variant that falls back to the stored `mtime` must fail "no file's `mtime` goes backwards".
8. **First write in ReadShared from a handle opened `O_RDWR` that has only read.** The file stays ReadShared until the first actual write.
9. **An extending write while WriteShared.** A writes 4 KiB past the end of a 10,000-byte file. The size is 14,096 in the MetaStore before A's write is acknowledged. B's `read()` of bytes 10,000 to 14,095 then returns them.

**Simulator faults** (design.md, simulation faults for coherence):

- lost or delayed recalls;
- clients paused past their lease;
- clock skew;
- owner failover in the middle of a recall;
- a planned leader transfer while opens arrive. The open registry moves with the transfer, so opens do not pause.

**Configurations** (runs as in [TLA+ and TLC](#tla-and-tlc)): `ci` with 3 nodes, 2 handles per node, 3 writes, 1 owner failover, 1 lost recall and 1 delayed recall; `deep` the same with 2 owner failovers.

Open: FUSE timeouts across a machine suspend. The kernel counts attribute timeouts in jiffies, which stop during suspend. The model covers paused daemons, not suspended machines, until the open question "FUSE timeouts across a suspend" is answered.

## Purge model

A TLA+ specification of [Freeing space](design.md#freeing-space) for two files in one slot, since the purge list is keyed by inode and the purger consumes runs of entries across inodes. It holds each file's size, `alloc_end`, the byte contents of its chunks (the model may use small chunks and blocks, such as a 4-block chunk of 4-byte blocks), the slot's purge list, writers on two nodes, the purger, sessions that can be fenced, and crashes.

**Rules the model contains**

- Before writing a chunk at or past `alloc_end`, a daemon raises `alloc_end`. The same transaction commits the size of what it has already written.
- Unlink. If no one has the file open, one transaction deletes the inode and adds a purge entry for chunks `0 .. alloc_end`. If it is still open, the inode goes on the orphan list instead. It moves to the purge list at its last close, and only after any failover grace period. A fenced session's opens count as closed only once its fence version is acknowledged.
- Truncate down. One transaction commits the new size, `alloc_end` lowered to the new end, and a purge entry from the new size's *byte offset* to the old `alloc_end`. The entry's first punch zeroes the rest of the block holding the new end (a merge) and the rest of that chunk. The daemon runs that punch before `truncate` returns, and the purger runs it again after a crash. The purger then deletes the whole chunks after it.
- Anything that extends the file over a range with a pending purge (a write raising `alloc_end`, a write or truncate that raises the size over the range) first finishes that purge.
- One purge entry per inode: a new purge range for an inode with a pending entry is merged into it as their union, with a check that the entry still holds what was read.
- The purger removes an entry it finished with a check that it still holds what the purger read, and a consumed run of entries with one range removal carrying a check on each entry and a check that each gap between them is empty.
- A punch to a chunk that does not exist succeeds.
- Reads are clamped to the size.
- Crashes can happen at any step, and the purger restarts from the purge list.

**Properties and their simulator invariants**

| Property | Simulator invariant |
| --- | --- |
| An interrupted purge reruns safely: running a purge entry again after any prefix of its punches leaves the same state as running it once | "once a purge settles, no chunk of a deleted inode or volume remains outside what snapshots hold"; "every read returns exactly the bytes of the acknowledged writes and punches applied in version order" |
| A purge never deletes newer data: no write acknowledged after the purge entry's transaction loses a byte to that purge | "every read returns exactly the bytes of the acknowledged writes and punches applied in version order" |
| Once a file's purge has run, no byte past its size is non-zero on disk, and an extension never exposes one | "no byte past a file's size is non-zero on disk once its purge has run"; "a truncated or punched range reads as zeros" |
| An orphaned inode is never freed while a live session has it open | "an orphaned inode is never freed while a live session has it open" |

**A rule removed, and the counterexample the model must find**

| Rule removed or changed | Counterexample | Property violated |
| --- | --- | --- |
| The truncate's purge entry starts at the next chunk boundary, not at the new size's byte offset | A truncate to an unaligned size, then an extension, reads the old tail bytes | No byte past size non-zero; extension exposes none |
| The tail is zeroed only in memory before the size commits (XFS commit `5885ebda`, 2015) | A crash between the size commit and the zeroing leaves old bytes past the size on disk, and a later extension shows them | Same |
| The first punch skips the partial block (FreeBSD fusefs `e312493b`, 2019, found by `fsx`) | The final partial block keeps its stale tail | Same |
| An extension does not finish the pending purge first | A write lands in the purge range, then the purger punches it | Purge never deletes newer data |
| `alloc_end` is raised after the chunk write instead of before | A crash after the chunk write leaves a chunk past `alloc_end`, and a later unlink's purge misses it | No chunk of a deleted inode remains |
| A punch to a missing chunk is an error | A rerun after a crash fails on the chunks already deleted | An interrupted purge reruns safely |
| Unlink frees an open file at once | Another node's open handle reads a purged chunk | Orphan never freed while open |
| An orphan moves to the purge list during an owner failover's grace period | A node that has not re-registered yet still holds the file open | Same |
| An orphan held by a fenced session is released when the fence commits, not when its version is acknowledged | The fenced holder's queued write lands after the purge | No chunk of a deleted inode remains |
| A new purge entry for an inode with a pending one overwrites it | A truncate, then an unlink before the purge runs: the unlink's `0 .. alloc_end` at the lowered `alloc_end` replaces the truncate's range, and the chunks above it are never deleted | No chunk of a deleted inode remains |
| The consumed run is removed without checks | The purger scans entries for inodes 1 and 3, an unlink adds inode 2's entry between them, and the range removal deletes it unpurged | No chunk of a deleted inode remains |

**Scripted scenarios.** Byte numbers are for a real 4 MiB chunk and 4 KiB block; scale them down for the model.

1. **Truncate to an unaligned size, then extend.** A 10,000-byte file of 0xAA has `alloc_end` 1, with its bytes in blocks 0 to 2 of chunk 0.
   - `truncate(5000)` commits size 5,000, `alloc_end` 1, and a purge entry from byte 5,000 to the end of chunk 0 (old `alloc_end` 1).
   - The first punch merges block 1, zeroing bytes 5,000 to 8,191, and punches blocks 2 to 1,023 of chunk 0.
   - `truncate(10000)` then extends the file.
   - Expected: bytes 5,000 to 9,999 read zero, and on disk bytes 5,000 to 8,191 of block 1 are zero.
2. **The same, crashing between the truncate's transaction and its punch.** The daemon crashes after committing size 5,000, before the punch. Until the purger runs it again, reads stop at 5,000, and the extension waits for the purge to finish. Expected: as in scenario 1.
3. **The same, crashing in the middle of the punch's record.** The record is torn and not durable. Expected: the purger runs the whole punch again, with the same result as scenario 1.
4. **Truncate to a block boundary.** Truncating a 10,000-byte file to 8,192 needs no merge: blocks 2 to 1,023 of chunk 0 are punched.
5. **Truncate across chunks.** A 16 MiB file (`alloc_end` 4) is truncated to 4 MiB + 100 bytes. The transaction commits `alloc_end` 2 and a purge entry from byte 4,194,404 to the end of chunk 3 (old `alloc_end` 4). The first punch zeroes bytes 100 to 4,095 of chunk 1's block 0 by a merge, and punches the rest of chunk 1. The purger then deletes chunks 2 and 3.
6. **Truncate to 0.** The entry covers chunks `0 .. alloc_end`. The first punch deletes chunk 0 whole, with no merge.
7. **Crash in the middle of a long purge.**
   - An unlinked file with `alloc_end` 1,000. The purger crashes after 400 chunks.
   - On restart it reruns the entry. The punches of the 400 deleted chunks succeed, and the purge ends with no chunk left.
   - The entry is removed, with a check that it still holds what the purger read, once the purge finishes. A consumed run of entries goes with one range removal carrying a check per entry and per gap (design: "Lists consumed in key order").
8. **Unlink racing a write from another node.**
   - Node B has the file open and is writing chunk 3 (`alloc_end` 4). Node A unlinks it.
   - Expected: the inode goes on the orphan list, with no purge while B has it open. B's writes keep working, and its `alloc_end` raises commit against the orphaned inode.
   - At B's last close, after any grace period, the inode moves to the purge list with chunks `0 .. alloc_end`, and the purge covers every chunk B wrote.
9. **An orphan whose holder is fenced.** As in scenario 8, but B's session is fenced while it holds the file open, with a write to chunk 3 still queued. Expected: the orphan moves to the purge list only after the fence's version is committed as acknowledged, so B's queued write is rejected before the purge runs.
10. **Extension racing a pending purge.**
    - A 16 MiB file (`alloc_end` 4) is truncated to 5,000. Before the purger deletes chunks 1 to 3, a write at 9 MiB on node B raises `alloc_end` from 1 to 3.
    - Expected: the purge finishes first, then the write lands, and a rerun of the entry leaves it alone.
    - Variant: the daemon crashed before the first punch. A write at 3 MiB extends the size over the pending range without raising `alloc_end`, and it also finishes the purge first.
11. **Truncate, then unlink before the purge runs.** A 16 MiB file (`alloc_end` 4) is truncated to 5,000, leaving a purge range from byte 5,000 to the end of chunk 3 and `alloc_end` 1; it is then unlinked. Expected: the entry becomes the union, `0 .. 4`, and once the purge settles no chunk of the inode remains.
12. **Two truncates.** The same file truncated to 8 MiB, then to 5,000, before the purger runs. Expected: the entry covers both ranges, and every chunk past the final size is deleted.
13. **An unlink of a lower inode between the purger's scan and its range removal.** The purger scans entries for inodes 1 and 3 and purges them; inode 2 is unlinked, adding its entry between them; the purger then removes the run. Expected: the removal's check over the gap fails, inode 2's entry survives, and a later pass purges it.

Open:

- The exact sequence of `alloc_end` raises. The design says "in steps that double up to 16 chunks" and "at most 64 MiB behind", but not whether the step sizes or the values double.
- Snapshots taken while a purge runs (punches carrying the snapshot epoch) are phase 11, outside this model.

**Configurations** (runs as in [TLA+ and TLC](#tla-and-tlc)): `ci` with 2 inodes of 2 chunks of 4 blocks of 4 bytes, 2 writers, 2 truncates, 1 unlink and up to 2 crashes; `deep` the same with up to 3 crashes.

## Model-to-simulator map

- Each configuration states its constants, the depth TLC reached, its distinct states and its wall
  time, as its run prints them, next to the property list; a model with no stated depth fails
  review. The depths each run must reach are those of [TLA+ and TLC](#tla-and-tlc).
- Each property names the simulator invariant that checks it in the real code and the checklist item
  that registers it; once that invariant exists, the row quotes its registered name. A property with
  none is listed as such and fails review unless the map says why.

| Model | Property | Simulator invariant (Testing strategy) | Registered by |
| --- | --- | --- | --- |
| Epoch change | No PG serves reads from two primaries at once | No PG serves reads from two primaries at once | Placement faults and invariants (phase 3) |
| Epoch change | No two replicas of a shard serve lease reads at once | No two replicas of a shard serve lease reads at overlapping times | MetaStore invariants (phase 4) |
| Epoch change | No write tagged with an interval completes after a later interval of its PG activates | No write tagged with an interval completes after a later interval of its PG has activated | Placement faults and invariants (phase 3) |
| Epoch change | No acknowledged version, and no version an activated interval kept, is ever dropped | No acknowledged version is ever dropped, and no version an activated interval kept is dropped later | Placement faults and invariants (phase 3) |
| Epoch change | The versions peering drops are a suffix of each chunk's pending versions, so no kept version was merged on a dropped one | No version peering keeps follows a dropped version of its chunk (with the byte-range oracle) | Placement faults and invariants (phase 3) |
| Epoch change | No two writes of a chunk get one version | No two writes of a chunk get one version | Invariant checker (phase 1) |
| Epoch change | A table that leaves a PG's interval alone never fails its writes | No write is refused for its interval while its PG's interval has not changed | Placement faults and invariants (phase 3) |
| Epoch change | A new primary merges into a chunk only once it holds every version peering kept | None named. Open: the byte-range oracle catches the effect (a merge on a missing base); the map must say so or the design must add one | Placement faults and invariants (phase 3) |
| Epoch change | Table epochs and interval numbers never go backwards | No node's table epoch or PG interval number goes backwards | Placement faults and invariants (phase 3) |
| Epoch change | Once a reissue and a later write to the same block are acknowledged, the block never again holds the dead daemon's write | Every read returns exactly the bytes of the acknowledged writes and punches applied in version order (the byte-range oracle), under the fault "a dead daemon's chunk write delivered after its successor's reissue and the pod's next write to the same block" | Invariant checker (phase 1) |
| Sessions and fencing | No fence commits while the client's own lease still runs | No session is fenced while its client's own lease still runs | Phase 6 file frontend (sessions and fencing) |
| Sessions and fencing | No fenced session's write is applied | No fenced session's write is applied | Phase 6 file frontend |
| Sessions and fencing | No lock reaches a new holder while a primary would still accept the old holder's writes | No lock is granted to a new holder while a primary would still accept the fenced holder's writes | Phase 6 file frontend |
| Cache-mode recall | No stale read in WriteShared mode | No client reads stale data from a file in WriteShared mode | Phase 7 (Tier 2) |
| Purge | An interrupted purge reruns safely | Open: no invariant checks a purge rerun directly; "once a purge settles, no chunk of a deleted inode or volume remains outside what snapshots hold" catches only leftovers | Phase 6 file frontend |
| Purge | A purge never deletes newer data | Every acknowledged write is readable (with the byte-range oracle) | Invariant checker (phase 1) |
| Purge | Once a file's purge has run, no byte past its size is non-zero, and an extension never exposes one | A truncated or punched range reads as zeros, and no byte past a file's size is non-zero on disk once its purge has run | Phase 6 file frontend |

- Open: the checklist has no phase 6 or 7 items yet, so the "Registered by" entries for sessions,
  recalls and purges name the roadmap phase.
- The epoch change model's counterexamples (a member logging one version out of order, a new primary
  merging before it holds a kept version, peering that stops only the previous interval, comparing
  member lists instead of interval numbers, a restarted primary peering in its old interval,
  deciding an older version by its own interval's members after a later interval kept it, activating
  before missing lists are durable) each also appear as a simulator fault or planted bug that trips
  the mapped invariant. Open: the checklist does not require this; it follows if every property has
  an invariant.
- Snapshot cuts and pool moves are modelled in phase 11, and cross-shard commit in phase 13, each
  before its code (design.md Roadmap), so this map has no rows for them yet.

## Wire messages

Hand-written encodings with a version byte, over `encoding/binary`'s `Append*` and `PutUint*` into
buffers the caller provides. No reflection. Every message about a PG carries its *PG fields*: the
sender's table epoch (`uint64`, per the PG epochs study) and the PG's interval number in that table
(`uint32`). k.n means version (interval k, counter n), packed as k << 32 | n.

| Message | Fields required by the design |
| --- | --- |
| Connection hello | The client's session ID and incarnation (the daemon's start time on `CLOCK_BOOTTIME`) |
| Chunk write, client to primary | PG fields, session ID, incarnation (8 bytes), chunk ID (16 bytes), byte range within the chunk, optional `expect_ver`, snapshot epoch, the data |
| Chunk write, primary to member | The above, plus the version and the merged whole blocks at block-aligned offsets of the frame |
| Write acknowledgment, member to primary | Version; the member's disk fill |
| Write reply to the client | Version; or `NoSpace`; or a refusal (the member's table epoch and the PG's interval); or an `expect_ver` mismatch |
| Punch | PG fields, session ID, incarnation, chunk ID, byte range, optional `expect_ver`, snapshot epoch; the version when sent to a member |
| Read | PG fields, chunk ID, byte range, optional `at_ver` |
| Read reply | Bytes and version; or a refusal (wrong interval, chunk on the missing list, unfound) |
| Grant | Support epoch, and the grantor's `CLOCK_MONOTONIC_RAW` reading `g`; the grantor's table epoch too if the epoch change model's grant rule needs it |
| Heartbeat between a node pair | The grantor's `CLOCK_MONOTONIC_RAW`, on every heartbeat |
| Heartbeat to the authority | Activation reports per PG; each member's acknowledgment latency; per disk its log ID, fill, oldest unfinished I/O age and whether its log has failed |
| `member_timeout` report, primary to authority | Member, PG |
| Direct probe, authority to a disk's owner loop | Disk ID |
| Probe reply | Log ID, oldest unfinished I/O age, whether the log has failed |
| Peering | The adopted interval; per chunk, the newest version; the member's missing list |

**Cases**

- Every kind round-trips, with each optional field present and absent.
- 1.1 packs to `0x0000000100000001`.
- In a primary-to-member write of 1 block and of 1,024 blocks, each block starts at a block-aligned
  offset of the frame. The receiver hands each block to a pool buffer without a copy; the receive
  path's trace records no copy of data bytes.
- An encoder whose frame exceeds 256 bytes attaches a pool buffer instead of failing. The 256-byte
  inline limit itself is under [Event and Action types](#event-and-action-types).
- Every request that carries data gets a reply, even a refusal. That reply is what lets the driver
  release the send buffer (driver contract).

**Rejects.** Each is an error returned to the caller, never a panic.

| Input | Result |
| --- | --- |
| Unknown version byte | Rejected as another version |
| Unknown message kind | Malformed |
| Truncated at every length from 0 to full − 1 | Malformed |
| A trailing extra byte | Malformed |
| Byte range with offset + length > 4 MiB | Malformed |
| Primary-to-member data length ≠ block count × 4,096 | Malformed |
| A version with interval 0 and counter ≠ 0, or counter 0 and interval ≠ 0 | Malformed (0 alone is the absent chunk) |
| Data that does not start at a block-aligned offset | Malformed |
| Interval number 0 in the PG fields | Malformed (intervals start at 1) |

- [fuzz] The seed corpus holds one valid frame of every kind. Random mutations never panic the
  decoder, and anything that decodes re-encodes to the same bytes.

**Open**

- The frame layout is not designed yet. A golden fixture under `testdata/` is due once it is, as for
  the log format.
- How a client's unaligned byte range is laid out in the frame, and so the data-length check for
  client writes. The checklist puts whole-block data at block-aligned offsets, but a client sends
  only some bytes of an edge block. A zero-length write's treatment is unstated too.
- Whether the phase 3 frame already carries the snapshot epoch. Chunk writes and punches carry one
  ([Snapshots and clones](design.md#snapshots-and-clones), [Freeing
  space](design.md#freeing-space)), but snapshots are phase 11.
- No phase 3 item names the primary's per-write session check. Every write carries the session ID,
  and the session table arrives with phase 4's control-plane group.

## Pools

Unit and data-driven tests for pool settings, creation rules and capacity figures, from [Pools and redundancy](design.md#pools-and-redundancy) and [Volume size and capacity](design.md#volume-size-and-capacity). Phase 3 covers pools of 1 to 3 copies. Erasure-coded settings are refused until phase 10. C3 still checks `ec4-2`'s capacity arithmetic, which is a pure function.

**Settings and defaults**

| Input | Expected |
| --- | --- |
| A pool created with no settings | `replicas` 3, `min_live` 2, `failure_domain` node, `max_per_domain` 1, `locality` spread, `overcommit` on, `write_limit` 90%, `disk_class` any, `pg_count` a power of two sized from the disk count |
| `replicas` 1 | `min_live` 1, `locality` node |
| `replicas` 2 | `min_live` 2, `locality` spread |
| `replicas` 0 or 4 | Rejected with an error naming the setting, never a panic |
| `sealed: ec`, `data`, `parity` or `encode_on_write` set (phase 3) | Rejected as not supported yet. Open: the design states only that erasure coding waits for phase 10, not the error |
| `failure_domain` other than `node` or `disk` | Rejected |
| `pg_count` 0, 3 or 1,000 | Rejected: it must be a non-zero power of two (as in IDs and hashing) |
| `pg_count` sized for 60 disks, `rep3` alone | 2048 (2048 × 3 ÷ 60 ≈ 102 members per disk). Open: the exact rounding rule ("sized for the largest expected cluster ... around 100") |
| `min_live` above `replicas`, or 0 | Rejected: it must be 1 to `replicas`, as Ceph's `min_size` must be 1 to `size` |
| `write_limit` of 10% or less | Rejected: `NearFull` starts 10 points below it |
| Installing a cluster | Creates one pool, `rep3`, as the default |
| `replicas` or `min_live` changed later | Accepted. The next table starts a new interval of every PG of the pool (asserted in Placement computation) |
| `failure_domain`, `write_limit`, `disk_class` or `overcommit` changed later | Accepted ("every other setting can change later") |
| `pg_count` doubled | Accepted. PG p splits into 2p and 2p + 1 |
| `pg_count` halved, or set to a value that is not a double of the current one | Open: rejected or not |

**Creation rules**

| Layout | Pool | Expected |
| --- | --- | --- |
| 3 nodes | `rep3`, `failure_domain` node | Accepted. The CLI states that copies survive 2 node failures, and that losing any one node leaves its PGs degraded until it returns; `rock-solid pool status` repeats it |
| 4 nodes | `rep3`, `failure_domain` node | Accepted, with no such loss stated: three nodes are left after any one fails |
| 2 nodes | `rep3`, `failure_domain` node | Rejected: width 3 > 2 domains |
| 2 nodes | `rep3`, `max_per_domain` 2 | Rejected: copies always sit in distinct failure domains |
| 2 nodes | `rep2` | Accepted. Survives 1 node failure |
| 1 node with 3 disks | `rep3`, `failure_domain` disk | Accepted, surviving 0 node failures. Open: how the CLI counts node tolerance for copies under `failure_domain: disk` |
| 3 nodes, 2 of them with NVMe disks | `rep3`, `disk_class` NVMe | Rejected: only domains with disks of the class count |
| 1 node | `none` | Accepted |
| Any | Presets `none`, `rep2`, `rep3` | Survive nothing, 1 and 2 failures; need 1, 2 and 3 failure domains; usable/raw 1, 1/2 and 1/3 |

**Capacity figures: the capacity model spike's cases as data-driven files.** Each case file carries:

- the layout;
- the spike's converged integer table with capped targets, as member slots per disk, exported from `spikes/capacity-model/model.py` with its seed;
- the fills, and the pool's own data on each disk.

The terms:

- *Capacity* cᵢ of a failure domain is the size × `write_limit` of the pool's disks in it, less other pools' data on them: its room plus this pool's own data there. In an empty pool, capacity and room are equal.
- *Room* is a disk's capacity less this pool's data on it, never below 0.
- *Held* is the pool's data, each chunk's raw bytes divided by its form's overhead.
- *b* is the overhead: `replicas`, or 1.5 for `ec4-2`.
- *a* is the most of each byte one failure domain may hold: 1 for copies, whatever `max_per_domain` is, or `max_per_domain ÷ data` for shards.
- *Balanced* B is min over j of S₍>ⱼ₎ ÷ (b − j·a), over capacities, rounded down to whole blocks.
- *f*, the stranded fraction, is Σᵢ max(0, cᵢ − a·B) ÷ Σᵢ cᵢ; each term is a domain's *stranded room*.
- *Reprotectable* is the least balanced over the survivors of each loss the design lists (Volume size and capacity), among losses whose survivors can hold a PG's width.
- *Available* is the smallest room ÷ share over the pool's disks, computed from the case's table. The expected value is the spike's figure, to within rounding.

| Case | Layout | Capacity (per domain) | Capacity ÷ b | Balanced | Available (spike) | f | Stranded room | Reprotectable (loss) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C1 | `rep2`, nodes of 0.7 and 2 GB | 0.63, 1.8 GB | 1.215 GB | 0.63 GB | 0.63 GB | 48.1% | B 1.17 GB (65%) | None: no spare domain, and no node has a second disk |
| C2 | `rep3`, nodes of 4, 4, 2, 1 TB | 3.6, 3.6, 1.8, 0.9 TB | 3.300 TB | 2.700 TB | 2.679 TB | 18.2% | A, B 0.9 TB each (25%) | 0.900 TB (node A) |
| C3 | `ec4-2` at 2 per node, nodes of 4, 4, 1 TB (2 disks each) | 3.6, 3.6, 0.9 TB | 5.400 TB | 1.800 TB | 1.800 TB | 66.7% | A, B 2.7 TB each (75%) | None: losing a node, or a disk of any node, leaves fewer than 6 members |
| C4 | `none`, spread, three 2 TB disks, A holding 1.2 TB of another pool | 0.6, 1.8, 1.8 TB | 4.200 TB | 4.200 TB | 4.189 TB (capped targets); 1.786 TB (size-weighted) | 0 | None | Not computed: one copy. It would be 2.400 TB (B or C), not the 3.6 TB of losing A |
| C5 | `rep3`, ten nodes of 4, 4, 4, 2, 2, 2, 1, 1, 1, 0.5 TB | 3.6 ×3, 1.8 ×3, 0.9 ×3, 0.45 TB | 6.450 TB | 6.450 TB | 6.422 TB | 0 | None | 5.250 TB (node A) |
| C6 | `rep2`, nodes of 4, 1, 1 TB | 3.6, 0.9, 0.9 TB | 2.700 TB | 1.800 TB | 1.800 TB | 33.3% | A 1.8 TB (50%; in whole blocks 439,453,126 of 878,906,250) | 0.900 TB (node A) |

Every case also checks available + held ≤ balanced ≤ capacity ÷ b. C4 with a size-weighted table is a case of its own: its available is 1.786 TB, 57% under balanced, which is the input for `Unbalanced`.

- C2 holding 0.8 TB and 1.0 TB at balanced proportions: reprotectable is 0.900 TB in both. Over room it would be 0.633 and 0.567 TB.
- C2 holding 2.0 TB: f is still 18.2% and balanced still 2.700 TB. Over room, balanced would be 0.70 TB.
- Without a spare domain, `rep3` on three nodes: of 2 × 2 TB each, reprotectable is 1.8 TB (losing any one disk); of 2 × 2, 2 × 2 and 4 + 1 TB, 0.9 TB (losing C's 4 TB disk); of one 4 TB disk each, none (no disk to lose within a node).
- A disk past its `write_limit` because of another pool has a capacity and a room of 0, and the pool's available becomes 0, never a negative figure.

The first refused write landing within 0.3% of available is a fill-based check. It belongs to Placement faults and invariants, since Pools does not fill.

**Conditions.** These are raised on the `Pool` resource, as events and as metrics. Fill rows are for the fullest member disk with `write_limit` 90% and no grants, unless stated; a disk's limit is ⌊size in blocks × 0.9⌋.

| Input | Expected |
| --- | --- |
| 79% | No condition |
| Live blocks exactly ⌊0.8 × size in blocks⌋ | `NearFull` |
| One block under the limit | `NearFull`. Available is one block ÷ share, and a one-block write that adds data passes |
| Exactly the limit | `Full`. Available is 0, and the next write that adds data is refused |
| Back to 89.5% after a discard, with block writes held | Still `Full`, which clears at 89%: a held block volume emits one `Full` event, not one per discard |
| `NearFull`, then back to 79.5% | Still `NearFull`; it clears at 79% |
| C2 with four 256 MiB grants, a disk 1 GiB × its share under its limit | `Full`. Available is 0, and admission refuses a write that adds data |
| Two pools on one disk, limits 90% and 95%, disk at 92% | The 90% pool `Full`, the 95% pool `NearFull` (each checks its own limit against the disk's total fill) |
| C1, C2, C3 and C6 created with `rock-solid pool create`, so confirmed at their f | No `Stranded` |
| C1, C2 or C3 as a `Pool` applied without the CLI, never confirmed (confirmed fraction 0) | `Stranded`, naming B 1.17 GB (C1), A and B 0.9 TB each (C2), A and B 2.7 TB each (C3) |
| C2 confirmed at 18.2%; node D's 1 TB disk marked out after a failure | f = 1 − 3 × 1.8 ÷ 9.0 = 40% > 18.2 + 5: `Stranded`, naming A and B |
| Then `rock-solid pool confirm` | `Stranded` clears; the confirmed fraction is 40% |
| C4, C5 | No `Stranded` (f = 0) |
| A pool whose disks have no capacity left (Σcᵢ = 0) | f = 0, no `Stranded` |
| C1 to C6 with their capped tables, placement proposing no move | No `Unbalanced`: available + held is 0.0 to 0.8% under balanced |
| C4 with a size-weighted table and every PG pinned, placement proposing no move | `Unbalanced` (57% under balanced), naming disk A with its share and target |
| C4 with a size-weighted table while placement still proposes moves, or a PG still lists past members | `Unbalanced` keeps its last value |
| C2 with a 4 TB disk added to D by `rock-solid` and confirmed, the pool 90% held | No `Unbalanced`: available + held is within 1.5% of balanced at any fill |
| C2 holding 0.8 TB | No `CannotReprotect` |
| C2 holding 1.0 TB | `CannotReprotect`, naming node A |
| C5 holding 5.3 TB; C6 holding 1.0 TB | `CannotReprotect`, naming node A in each |
| C1 or C3 holding any data | No `CannotReprotect`. `rock-solid pool status` says losing either node (C1), or any node (C3), leaves its PGs degraded until it returns |
| C4 (`none`) at any fill | No `CannotReprotect`: one copy |
| `rep3` on three nodes of 2 × 2 TB, holding 1.7 TB, then 2.0 TB | None, then `CannotReprotect` naming one disk (reprotectable 1.8 TB) |
| `rep3` on three nodes, one node rebooting | No figure or condition changes: its disks keep their shares at their last fills |
| A PG with a missing member that copies nothing for 60 s on `ctx.Now` | `Stuck`, naming the PG; it clears when the PG copies again or its past members are trimmed |

**Confirmation.** `rock-solid pool create` and disk changes print, per domain, cᵢ, its load at balance min(cᵢ, a·B) and its stranded room. They ask for confirmation when some domain has 2 × stranded room > cᵢ, and record f as the confirmed fraction.

| Input | Expected |
| --- | --- |
| C1 | B strands 1.17 of 1.8 GB (65%): asks; confirmed fraction 48.1% |
| C2 | A and B strand 0.9 of 3.6 TB each (25%): no prompt; confirmed fraction 18.2% |
| C3 | A and B strand 2.7 of 3.6 TB each (75%): asks |
| C6 | A strands 439,453,126 of 878,906,250 blocks, two blocks over half: asks |
| A disk added or removed | The same output and rule; the confirmed fraction becomes the new f |
| A `Pool` created without the CLI | Confirmed fraction 0 until `rock-solid pool confirm` |
| `rock-solid pool status` | Shows f, each domain's stranded room, the confirmed fraction, and the single losses after which the survivors cannot hold a PG's width |
| Any layout that strands space | Placement never refuses it |

**Available, grants and per-node figures**

- C2 with four nodes each holding a 256 MiB grant: available is 2.679 TB less 1 GiB.
- Disks outside the pool's `disk_class`, or holding no member of the pool, have no share and do not set available.
- A `none` pool with `locality: node`, on nodes A, B and C, sized as C4 with A's only disk holding 1.2 TB of another pool: per-node available is A 0.6 TB, B 1.8 TB and C 1.8 TB, each over the node's own PGs.
- Two pools on C2's disks, `rep3` and `none` spread: `rep3` is balanced at 2.700 TB and `none` at 9.900 TB, both computed from the same 9.9 TB of capacity. Their sum is more than that.
- The CLI lists each disk's fill, capacity, room, share and room ÷ share. In C1, it names the 0.7 GB node's disk as the one that sets available.

**Reservations with `overcommit` off** (C2: balanced 2.700 TB, available 2.679 TB). S is the space snapshots hold, 0 until phase 11.

| Steps | Expected |
| --- | --- |
| `CreateVolume` 1 TB, then 1 TB | Both succeed; reservations 2 TB |
| A third `CreateVolume` 1 TB | `RESOURCE_EXHAUSTED` (3 TB > 2.7 TB) |
| `ControllerExpandVolume` of one volume to 1.5 TB | Succeeds; reservations 2.5 TB |
| `CreateVolume` 0.19 TB, bringing reservations to 2.69 TB | Succeeds: within balanced (2.700 TB), though above available (2.679 TB) |
| Then an expand by 0.02 TB (2.71 TB) | `RESOURCE_EXHAUSTED` |
| Reservations of 2.5 TB with 2.0 TB written, then `CreateVolume` 0.1 TB | Succeeds: balanced over capacity is still 2.700 TB (over room it would be 0.70 TB) |
| The same, `CreateVolume` 0.3 TB instead | `RESOURCE_EXHAUSTED` (2.8 TB > 2.7 TB) |
| With S = 0.3 TB (phase 11), reservations of 2.5 TB and an expand by 0.1 TB | `RESOURCE_EXHAUSTED` (2.5 + 0.3 + 0.1 > 2.7) |
| The same steps with `overcommit` on | Succeed: volume sizes may add up to more than the pool holds |

Block volumes in an erasure-coded pool reserve at the copies' overhead. That case is phase 10.

**Several shards per domain (phase 10).** Two shards of a PG never share a disk, so a domain holds at most min(`max_per_domain`, its disks) members of a PG. `ec4-2` at 2 per node:

| Layout | Expected |
| --- | --- |
| Nodes of 2 × 2, 2 × 2 and 1 × 4 TB | Rejected at creation: 2 + 2 + 1 = 5 < 6 members fit |
| Nodes of 2 × 2, 2 × 2, 2 × 2 and 1 × 4 TB | Accepted; balanced 8.64 TB, not the closed form's 9.60 TB |
| C3 | Balanced 1.80 TB either way |
| Node A one 4 TB disk; B, C and D two 1.5 TB disks each | Each domain is checked in turn: losing A leaves 5.4 TB. Losing B, C or D leaves 1 + 2 + 2 < 6 members, a layout fact `pool status` states |

Every PG's first `replicas` members, which hold its copies, sit in distinct domains. [property] On random layouts of 4 to 6 nodes, reprotectable is the least over each domain in turn; the largest domain alone was not the worst in 367 of 3,000 (the study's test E2).

**Property tests.** [property] For random layouts of 3 to 10 nodes with 1 to 4 disks of 0.5 to 4 TB, some disks holding other pools' data, random valid tables and random amounts held at balanced proportions:

- available + held ≤ balanced ≤ Σcᵢ ÷ b;
- balanced equals the largest D with Σᵢ min(cᵢ, a·D) ≥ b·D, found by bisection to 0.1%;
- the domains' stranded rooms sum to Σcᵢ − b·B;
- f does not change with the amount held;
- for copies, balanced without the largest-capacity domain equals the least over single domains, and is at most balanced after any single disk loss;
- `CannotReprotect` never fires on a one-copy pool, nor for a loss whose survivors cannot hold the width.

"Available + held within the margin of balanced once converged" and "fills reach available within 1%" belong to Placement computation and to Placement faults and invariants. Pools only computes the figures from a given table and given fills.

## Placement computation

A pure function in the sans-IO core: the previous table plus an event gives a new table, with a
deterministic order of moves (no `range` over a map, ties broken in a fixed order).

**Which PGs get a new interval.** Each row is one placement table, and each raises the table epoch
by 1.

| Table change | PGs whose interval goes up by 1 |
| --- | --- |
| Pool B created | None in other pools; B's PGs start at interval 1 |
| Disk added, before balance moves anything | None |
| Balance moves one member of PG 7 | PG 7 only; the other 2,047 keep theirs |
| Disk D, one of a node's 3, marked down | Only the PGs with a member on D |
| Node marked down | Every PG with a member on the node: all 2,048 on 3 nodes; 615 of 2,048 on 10 nodes of 3 disks (PG epochs spike F1) |
| Node marked up again | The same PGs again |
| `maintenance <node>` set, then cleared | Every PG with a member on the node, at each change (a live mark) |
| PG 7's members reordered (same set, new primary) | PG 7 |
| Pool's `replicas` 2 → 3 | Every PG of that pool |
| Pool's `min_live` changed | Every PG of that pool |
| Pool's `write_limit` changed | None |
| Activation report folded in (last activated interval set, past members trimmed) | None |
| `pg_count` doubled | Every PG p of the pool becomes 2p and 2p + 1, each one interval above p: PG 5 at 9 gives PGs 10 and 11 at 10 |
| PG 7's primary asks because a counter ran out | PG 7 only |
| Node N restarts and asks | Exactly the PGs whose primary is on N |
| Member C removed (table e + 1), then added back (e + 2) | +2 over the two tables, though the member list equals table e's |
| Down mark and refill in one table | +1; in two tables, +2 |

- A change to the session table or the format level makes no placement table and leaves the table
  epoch alone.
- A delta's changed entries are exactly the PGs whose member list, order, live marks, interval, last
  activated interval or past members changed. Every other entry is byte for byte the previous
  table's.
- Open: whether a balance move swaps the member in one table (+1) or adds and then removes it (+2).
  The PG epochs spike ran both (F2: 1,202 against 2,402 tables per staged flap); the "Balance moves
  one member" row assumes one.

**Placement rules**

- Refill puts every missing member on the eligible disk furthest below its target. Balance then
  runs: while any disk is outside its fill margin, move one member from the disk furthest above its
  target, even one inside its own margin, to the eligible disk furthest below, and stop when no move
  brings that disk nearer.
- A disk's fill margin is max(5% of its target, 1 member). A 0.5 TB disk beside 8 TB disks, with a
  target of 10 to 20 members, has a margin of 1 member.
- Both steps keep the failure-domain rule. With `failure_domain: node`, no PG ever has two members
  on one node, and no domain holds more than `max_per_domain` of a PG's width.
- A disk's target is its capacity for the pool: `write_limit` less other pools' data on it, so this
  pool's own data counts, water-filled over domains capped at `max_per_domain` of the width, and
  split within a domain by capacity.
- Targets by capacity: after a disk is added to a pool holding data, the second computation, run
  once the first one's moves have copied, proposes no move.

  | Layout | Moves, over computations |
  | --- | --- |
  | `none`, one 2 TB node holding 1 TB, a 2 TB node added | 61, over 2 (124 over 4 with targets by room) |
  | C6 holding 0.8 TB, a 2 TB disk added to node B | 62, over 2 (98 over 3) |
  | C2 holding 1.5 TB, a 4 TB disk added to node D | 98, over 2 (153 over 5) |

**Convergence on the capacity model layouts.** C1 to C6 are the data-driven cases of the "Pools"
item, which holds their capacity figures ([capacity model
spike](../spikes/capacity-model/README.md)). Here they check only placement:

- With capped targets, the loop converges on all six, and the table's available figure comes
  within 1% of balanced.
- With size-proportional targets on C1, C2, C3 and C6, the loop never reaches its margin (worst disk
  23 to 200% off). It still terminates, because it stops when no move helps.
- In C4 (three 2 TB disks, one holding 1.2 TB of another pool), targets by capacity are what keep
  the table near the 4.2 TB bound.

**Table size and deltas**

| Table | Size |
| --- | --- |
| 2,048 PGs × 3 replicas × 4-byte disk ID + 8 bytes of interval numbers | 40 KiB |
| 4,096 PGs × 18 members, `ec16-2` | About 320 KiB |

Past intervals' member lists come on top of these sizes and are usually empty.

- A delta holds the changed PG entries and the CRC32C of the resulting table. Applied to the
  previous table, it reproduces the whole table and its CRC byte for byte.
- A delta applied to the wrong base, table e − 1 instead of e, fails the CRC, and the node fetches
  the whole table.
- A gap fetches the whole table: the node holds e and receives the delta for e + 2.
- A node that jumps from e to e + 5 by fetching a whole table loses nothing, and none of its
  interval numbers goes backwards.
- [fuzz] Delta and whole-table decoders. A truncated delta, an entry for a PG past `pg_count`, or a
  decreasing interval number is Malformed.
- PG epochs spike B1, a homelab year on 10 nodes: 104 to 105 MB of deltas against 399 MB to 2.25 GB
  of whole tables. F2, one flap with refill staged one member per table: 1,202 tables. A delta costs
  about 29 bytes per changed `rep3` PG entry plus its header (study's estimate).

**[property] tests.** 1,000 seeds of random add, remove and failure sequences and disk drains, over
the matrix in Testing strategy: 3 and 5 nodes, 1 or 4 disks per node, mixed sizes; `none`, `rep2`,
`rep3`.

After every table:

- Every PG keeps its pool's width, with at most `max_per_domain` per domain.
- No table raises an interval it need not, by the table above.
- A member removed and added back gets a new interval.
- A delta reproduces the whole table and its CRC.

Once the loop stops:

- It has converged: no disk is outside its fill margin.
- Available + held is at least balanced ÷ (1 + the largest margin of the pool's disks) and never
  above balanced.
- Members moved stay near the minimum.

`Unbalanced` [property], on the study's test U seeds: 200 random layouts each for `rep2` and `rep3`,
3 to 6 nodes of 1 or 2 disks of 0.5 to 8 TB, some partly used by other pools, one disk added to a
random node of a converged table:

- No addition raises `Unbalanced`, at any fill; the worst leaves available + held 9.4% under
  balanced, a disk of about ten members one member above its target.
- `Unbalanced` keeps its value while the computation proposes moves or a PG lists past members.

**Errors and open points**

- Refill that finds no eligible disk never panics, keeps the failure-domain rule, and leaves the
  member marked down. One example: `rep3` on 3 nodes after one node fails.
- A PG with no eligible disk for refill raises `Stuck` once it has copied nothing for 60 s, and
  `rock-solid pool status` states the layout fact (`rep3` on 3 nodes: losing a node leaves its PGs
  degraded until it returns). It never raises `CannotReprotect`.
- Open: the tolerance for "moves stay near the minimum".
- Open: the tie-break between equally placed disks.
- Open: how an operator's pin or drain is represented in the table, and that balance never moves a
  pinned PG.

## Replica primary

`internal/chunkstore`, with a simulated single authority standing in for the control plane. k.n is
version (interval k, counter n), as under [Wire messages](#wire-messages).

**Versions**

| Chunk state when a write arrives at a primary in interval k | Version given |
| --- | --- |
| Absent (version 0), k = 7 | 7.1 = `0x0000000700000001` |
| Newest ordered 7.5, still pending | 7.6 |
| Newest committed 6.9, nothing pending, k = 7 | 7.1, which orders above 6.9 |
| Parent PG split at interval 9, chunk at 9.4, child starts at 10 | 10.1 |
| Newest ordered 7.(2³² − 1) | The write waits: the PG's pending writes commit, the primary asks for interval 8 and re-peers in it (no lease wait, since the primary stays), then the write gets 8.1 |

- 1.(2³² − 1) orders below 2.1.
- A chunk at 7.(2³² − 3) takes 5 writes. Writes 1 and 2 get 7.(2³² − 2) and 7.(2³² − 1). Write 3
  waits until the PG's pending writes commit and interval 8 is peered, then gets 8.1. Writes 4 and 5
  get 8.2 and 8.3. Other PGs see no refusal. A 32-bit counter lasts 1.2 hours at 1 M writes/s to one
  chunk and 50 days at 1,000 (PG epochs spike C1).

**Interval check at a member whose table has PG 7 in interval 42**

| Tag | Sender's table epoch | Result |
| --- | --- | --- |
| 42 | Older or newer than the member's | Accepted |
| 41 | Any | Refused, with the member's table epoch and 42 |
| 43 | Newer | The member fetches a table at least as new as the sender's; if PG 7 is then at 43, accepted |
| 43 | Newer | As above, but PG 7 is at 44 after the fetch: refused with 44 |

- Open: a tag of 43 with a sender's table epoch older than the member's cannot come from a correct
  sender. Whether it is Malformed or refused is not stated.

**Client writes at primary B, whose term began in interval 42; PG 7 is now at 44 after two replica
swaps**

| Client tag | Result |
| --- | --- |
| 42, 43 or 44 | Accepted |
| 41 | Refused with 44; the client fetches the table |
| 45 | B fetches the table first |

- A client write that B held when interval 43 ended under it is ordered again, once B has re-peered
  in 44, with a version of interval 44.

**Commit, map and read**

- A write goes to every member in parallel while the primary appends it to its own log, and is
  acknowledged once every live member holds it.
- The primary maps the write into its index at commit, not when its own log entry is durable. A
  replica maps it when durable.
- A read without `at_ver` while 7.6 is pending returns 7.5's blocks without waiting. Once 7.6
  commits, the read returns 7.6.
- `at_ver` of a frozen version is served by any member holding it and never waits for a writer.
- `at_ver` of a version still pending at a replica never returns its bytes.
- Open: what `at_ver` of a version that is neither frozen nor current returns. There are no
  snapshots before phase 11.
- `expect_ver` equal to the chunk's version: the write succeeds. An older `expect_ver`: the write
  fails and changes nothing.
- Open: whether `expect_ver` is compared with the newest ordered version or the newest committed
  one. Case: 7.6 is pending, and two writes arrive, one with `expect_ver` 7.6 and one with 7.5.
- Commits that arrive out of version order are still mapped in version order. A planted variant that
  maps them in arrival order must fail, as chunk epochs spike R2 did; replay itself is covered by
  the [Chunk index](#chunk-index) section's R1 case.
- A chunk's writes go to each member in version order. After a `PeerLost` and reconnect they are
  sent again from the oldest unacknowledged one, in the same order. Trace check: no `NetSend` of
  k.(n + 1) to a member before k.n on that connection.
- A version peering dropped is superseded in every log that holds it; see [Recovery per
  PG](#recovery-per-pg).

**Partial blocks** (sub-block writes spike). Block 5 is committed at 7.7.

| Write | Base | Sent to members |
| --- | --- | --- |
| Bytes 0 to 99 of block 5, nothing pending | 7.7's block 5 from the live map, CRC32C checked | One merged whole block, 7.8 |
| Bytes 100 to 199 of block 5 while 7.8 is pending | 7.8's merged block | Whole block 7.9 holding both writes' bytes; 7.9 is acknowledged only after 7.8 commits |
| As above, but a member refuses 7.8 (its session fenced, or a new interval reached the member first) | 7.8 never commits | 7.9 is refused too, and no later version carries 7.8's bytes; the client retries against 7.7 |
| Bytes 0 to 99 of a block never written | Zeros | Whole block |
| Bytes 0 to 99 of a punched block | Zeros | Whole block |
| Bytes 100 to 8,291 of a chunk (blocks 0 to 2) | Edge blocks 0 and 2 only; block 1 is whole | Three whole blocks; at most two edge blocks are merged per write |
| Whole blocks 3 and 4 | None | Two blocks, no base read |
| Partial write whose base is in the loop's merged-block cache | The cache, no disk read | Whole block |
| Partial write whose base read fails its CRC32C | Fetched from another member before merging; the chunk's later operations wait behind it, in order | A block merged on the good copy, never on the rotten one |
| Punch of bytes 100 to 8,191 | Block 0 merged with zeros; block 1 gets a punch entry | One record |

- Sub-block spike M1, M6 and M9 as seeds: 2 clients, 8 writes pipelined each, writes of 1 to 300
  bytes (up to 3 blocks in M9) at random offsets, primary changes in about 2% of steps. No seed of
  500 differs from the byte oracle.
- M2 and M3 as planted bugs: merging on the committed version fails at least one seed of 500, with
  one client pipelining 4 writes or two clients with one write each. The spike's model failed every
  seed.

**`min_live` and `maintenance`**

| Pool, members down | Writes |
| --- | --- |
| `rep3`, `min_live` 2, one down | Accepted on the 2 live members |
| `rep3`, two down | Rejected until recovery restores a member |
| `rep2`, `min_live` 2, one down | Rejected |
| `rep2`, that node in `maintenance` | Accepted on one copy; resync on return is under [Recovery per PG](#recovery-per-pg) |

**`write_limit` admission.** The pool's `write_limit` is 90%. Member disk X reports 91% fill with
its acknowledgments and heartbeats.

| Write | Result |
| --- | --- |
| Adds live data to a PG with X as a member | Refused with `NoSpace` |
| The same, with every member under 90% (outstanding grants counted by each disk's share) | Admitted |
| Flush under a space grant | Admitted on the grant alone |
| Overwrite of blocks held live, written after the chunk's latest freeze, held as copies, no freeze due | Admitted; the check costs one index lookup per run of blocks |
| Overwrite of live blocks 3 and 4 plus never-written block 5, in one write | Refused with `NoSpace` (it adds live data) |
| Overwrite of a block a snapshot shares (a freeze is due) | Refused like a new write |
| Punch | Admitted |
| Recovery copy | Admitted |
| Any write while no member is past the limit | No overwrite lookup is made |
| Pool P1 (limit 95%) and pool P2 (limit 90%) on one disk at 92% total fill | P1 admitted, P2 refused: each checks its own limit against the disk's total fill |

- Tables applied as deltas and fetched whole after a gap or CRC mismatch: the cases under [Placement
  computation](#placement-computation), seen from the primary.
- Open: space grants arrive with phase 4's control-plane group; the checklist does not say how phase
  3's stand-in authority marks a flush as granted.

## Peering and read leases

`internal/lease` for the leases, `internal/chunkstore` for peering. Constants for every lease case
below and in the other sections: `primary_lease` 4 s (4,000,000 ticks), `lease_drift` 1%, a
heartbeat every 1 s, `rate_window` 10 s, `rate_gap` 100 ms, `max_margin` 50%, 1 tick = 1 µs. k.n is version (interval k, counter n).

**Serving reads**

- A `rep3` primary holding B's grant but not C's serves no reads, and still takes writes.
- C is marked down in a new interval. Once the primary has re-peered, C's grant is no longer needed.
- A grant that lapses during a pass: the read in that pass is refused.

**Lease time.** The holder counts from when it asked and ends the lease when either clock shows 3.96
s. The grantor counts from when it granted and keeps the grant until both clocks show 4 s. The
request is sent at real time 0 and granted at 0.5 ms. Rates are relative to real time. The
single-clock column is what counting on `CLOCK_BOOTTIME` alone would give (clock slew spike).

| Holder `BOOTTIME`, raw | Grantor `BOOTTIME`, raw | Holder ends (real s) | Grantor ends (real s) | Single-clock |
| --- | --- | --- | --- | --- |
| exact, exact | exact, exact | 3.960 | 4.0005 | Same |
| +22.6%, exact | exact, exact | 3.230 | 4.0005 | Same |
| −22.6%, exact | exact, exact | 3.960 | 4.0005 | Holder 5.116: 1.12 s overlap |
| exact, exact | +22.6%, exact | 3.960 | 4.0005 | Grantor 3.263: 0.70 s overlap |
| exact, exact | −22.6%, exact | 3.960 | 5.168 | Same |
| −22.6%, exact | +22.6%, exact | 3.960 | 4.0005 | 1.85 s overlap |
| −8.33% (chrony), exact | +8.33%, exact | 3.960 | 4.0005 | 0.63 s overlap |
| exact, −1% | exact, +1% | 3.960 | 4.0005 | |
| −100 ppm, −100 ppm (no daemon) | +100 ppm, +100 ppm | 3.9604 | 4.0001 | |
| −1%, −1% (no daemon), before the first bound | +1%, +1% | No lease taken | | |
| −1%, −1%, first lease, from grants `rate_gap` apart, 100 µs round trips | +1%, +1% | By 3.931 (margin up to 2.72%) | 3.961 | |
| −1%, −1%, after a full window | +1%, +1% | 3.939 (margin 2.52%) | 3.961 | |

- With a grantor's `CLOCK_BOOTTIME` 22.6% slow, a crash waits up to 5.2 s for the lease, past
  `node_down_after`.
- A grantor's wait is a timer on `ctx.Now`. If it fires while `ctx.Raw` shows less than the lease,
  the grantor sets another timer for the remainder and joins no peering before it.
- [property] 10,000 seeds with random rates per node (`BOOTTIME` ±22.6%, raw ±100 ppm) and random
  renewal timing: no holder's lease ever ends after its grantor's. With raw ±4%, and `BOOTTIME`
  slewed or at the raw rate (no daemon), the same holds from the first lease.

**Rate bound.** A grant carries the grantor's raw reading `g`; the holder records its own raw `s`
(request sent) and `r` (grant received).

```text
bound = (g_b − g_a) / (s_b − r_a)
```

The holder keeps a window per grantor node and boot ID. When a window's first grant arrives it
asks again `rate_gap` later, and it takes no lease from that grantor until it has a bound. It pairs
the shortest-round-trip grant of the first third of the window's span with that of its last third.
It widens its margin for that grantor by the excess of the bound over 1 + `lease_drift` / 2 = 1.005,
and reports the pair; a margin of `max_margin` or more gives no lease. Times are in µs, and G0 =
50,000,000.

| Case | s_a, r_a, g_a | s_b, r_b, g_b | Bound | Margin |
| --- | --- | --- | --- | --- |
| Exact rates, 1 ms round trips, `g` read at the worst ends | 0, 1,000, G0 | 3,333,333, 3,334,333, G0 + 3,334,333 | 1.000600 | 1% (600 ppm overshoot) |
| Exact, 100 µs round trips, worst ends | 0, 100, G0 | 3,333,333, 3,333,433, G0 + 3,333,433 | 1.000060 | 1% |
| Grantor raw 1% fast, 100 µs, `g` at midpoints | 0, 100, G0 + 50 | 3,333,333, 3,333,433, G0 + 50 + 3,366,666 | 1.010030 | 1.503%, reported |
| Grantor 0.6% fast | As above | g_b − g_a = 3,353,333 | 1.006030 | 1.103%, reported |
| Grantor 0.4% fast | As above | g_b − g_a = 3,346,666 | 1.004030 | 1%, nothing reported |
| Grantor 1% slow | As above | g_b − g_a = 3,300,000 | 0.99 | 1%: the margin never drops below `lease_drift` |
| Every grant in the first third took 50 ms, worst ends | 0, 50,000, G0 | 3,333,333, 3,333,433, G0 + 3,333,433 | 1.015259 | 2.026%: a false widening, in the safe direction |
| One 50 ms grant among 100 µs ones in the first third | The shortest is chosen | | 1.000060 | 1% |
| The pair started 6 s ago: thirds of the 6 s span, 100 µs, worst ends | 0, 100, G0 | 4,000,000, 4,000,100, G0 + 4,000,100 | 1.000025 | 1% |
| First grant of a window | 0, 100, G0 | | none | No lease from this grantor; a request is sent at r_a + 100,000 |
| Second grant, sent `rate_gap` after the first arrived, 100 µs, worst ends | 0, 100, G0 | 100,100, 100,200, G0 + 100,200 | 1.000999 | 1% |
| The same with 1 ms round trips | 0, 1,000, G0 | 101,000, 102,000, G0 + 102,000 | 1.020000 | 2.5%: the first lease ends 3.90 s after the ask |
| A grant with a new boot ID, g = 1,200 | | | none | A new window starts; no lease until its second grant |
| Grantor raw about 50% fast | | g_b − g_a = 1.505 × (s_b − r_a) | 1.505 | 51%, past `max_margin`: no lease, reported |

- Open (clock-skew study, recommendation 2): naming the miscalibrated node by majority at the
  control plane is not adopted.

**Pause detection.** The holder's last grant is `a`: s_a = 1,000,000, r_a = 1,001,000, g_a =
50,000,500. The lease taken from `a` has margin m. A heartbeat arriving at holder time r trips the check when
g − g_a > (r − s_a) / (1 − m). The rows use m = 1% unless they say otherwise.

| Event | Check | Result |
| --- | --- | --- |
| Heartbeat r = 2,001,000, g = 51,000,500 | 1,000,000 ≤ 1,001,000 / 0.99 = 1,011,111.1 | Nothing |
| The same with g = 51,011,611 | 1,011,111, not > 1,011,111.1 | Nothing |
| g = 51,011,612 | 1,011,112 > 1,011,111.1 | Ends every lease |
| Grantor raw 2% fast, m = 2.5%; g = 51,020,500 | 1,020,000 ≤ 1,001,000 / 0.975 = 1,026,666.7 | Nothing, on every heartbeat |
| The holder's VM stopped 10 s on kvm-clock, its clocks frozen; heartbeat r = 2,001,000, g = 61,000,500 | 11,000,000 > 1,011,111.1 | Ends every lease |
| `Resumed` after a suspend | | Ends every lease |
| `Resumed` after the VMClock marker changed | | Ends every lease |
| `Resumed` at a node that only grants | | Keeps its grants and its waits |

- After any detection the holder sends a grant request to every grantor at once and restarts its
  rate window, asking again `rate_gap` after the first grant arrives. About 100 ms of reads on
  each grantor's leases is lost, and no later bound counts the pause as a fast grantor.
- Peer messages come before reads. In one pass, a queued heartbeat that trips the step check and a
  client read that arrived after it: the trace shows the leases ended before the read, and the read
  is not served on the old lease.
- VM pause spike T6 as simulator seeds, 100 pause phases per length from 0.5 to 30 s. A planted bug
  must fail at least one seed:

| Pause handling | Expected |
| --- | --- |
| The rate bound alone, pauses of 3.5 s and longer (T6a) | Up to 0.99 s of stale reads: a planted bug |
| The rate bound alone, after a pause, with no window restart (T6a) | Every lease shortened for up to 4.0 s: a planted bug for the restart rule |
| Step check on grants only (T6b) | Up to 0.99 s of stale reads: a planted bug |
| Step check on every heartbeat, messages lost, no marker (T6c) | Up to 0.94 s of stale reads: a planted bug |
| Marker alone, messages queued (T6d) | 0.99 s of stale reads: a planted bug |
| Marker and step check, messages queued or lost (T6e) | 0 stale reads at every length; about 100 ms (`rate_gap` plus a round trip) blocked |

**Support epochs and the restart wait**

- A is primary of PG 17 and holds B's grant under support epoch 7.
  - A partition lasts 10 s.
  - A stops serving at 3.96 s on its own clocks.
  - B's grant lapses on both of B's clocks. B moves to support epoch 8 and helps peer interval 42,
    which moves PG 17 to C.
  - The partition heals before A sees that table.
  - A asks to renew under 7 and is refused; nothing A held under 7 survives. A asks afresh under 8.
- A grant under a lapsed support epoch is never renewed, whatever the message delay. A duplicated
  old renewal arriving after the bump is refused.
- A holds B's grant and reaches B but not the control plane. B adopts a table that moves PG 17's
  primary away from A; A's heartbeats still report the older table. B refuses A's renewals until A
  reports a table at least as new, so A's lease on B lapses within one `primary_lease` and A serves
  no read of PG 17 after it. Once A fetches the table, B renews again.
- A node that starts at t = 100 s grants no vote and joins no peering before 104 s. The wait is a
  grantor's, so it lasts until both clocks show 4 s: with its `BOOTTIME` 1% fast, not before its raw
  clock shows 4 s too.
- Open: whether the restart wait also delays a restarted primary's own peering, or only its grants
  and its part in other primaries' peering. A primary restarting at t = 100 s peers in its new
  interval at about 100 s under one reading and at 104 s under the other.
- Open: which PGs a fresh grant covers (open question "Which PGs a node-pair grant covers"). The
  epoch change model chooses the rule. Once it is chosen: B holds PG 17 in interval 42, A asks under
  8 with 41's table, and A gets no lease for PG 17.
- One grant covers every PG the pair shares and is renewed by the pair's heartbeat. With 2,048 PGs
  on 5 nodes, a pair sends one grant per heartbeat, not one per PG, and the count does not change
  when `pg_count` doubles.

**Leases and peering**

- A grantor joins no peering of a new interval that moves the primary off the holder until its grant
  expires on both clocks, or until the holder has adopted that interval (a planned move, no wait).
- A new interval that keeps the primary, such as a replica replaced or a counter bump, needs no
  lease wait. The primary keeps serving reads of committed versions and holds writes until it has
  re-peered.
- Peering across several intervals adds no wait: only the last activated interval's primary can have
  served reads, and peering reaches a member live in that interval.

**Stopping earlier intervals.** PG 7's last activated interval is 41.

| Intervals since 41 | Members peering must get to adopt k |
| --- | --- |
| 42 only, the usual case | One member live in 42 |
| 42 (never activated), now 43 | One live in 42 and one live in 41 |
| 42 had fewer than `min_live` live members | 42 skipped |
| 42 was short of `min_live` only by members in `maintenance` | 42 stopped and read like any interval that accepted writes |
| No member of an interval to stop is reachable | The PG waits (down) until one returns or an operator declares its disks lost; it accepts no writes, and this is not an error |

- Activation report lost. 42's primary activates and its heartbeat report is lost, then 43 starts.
  43's peering still reaches members of 41 and 42, since the last activated interval is still 41. A
  report for 42 that arrives after 43 was recorded does not lower it.

**Keep rule.** The last activated interval is L = 41 and the new primary peers in 43. A, B and C are
live in 41 unless a row says otherwise. "Lacks" means the member neither holds the version nor lists
it on its missing list.

| A | B | C | Decision |
| --- | --- | --- | --- |
| Unreachable | 41.8 | 41.7, lacks 41.8 | Drop 41.8; keep 41.7, with C authoritative |
| Unreachable | 41.8 | Down for all of 41 (41 = A, B) | Keep 41.8 |
| Unreachable | 41.8 | 41.8 | Keep 41.8: it may have been acknowledged |
| Unreachable | 41.8 | 41.7, lists 41.8 (written by a peering of 42 that never activated) | Keep 41.8 |
| Unreachable | 41.8, 41.9 | 41.8, lacks 41.9 | Drop 41.9, keep 41.8: a suffix |
| 41.7, lacks 41.8 | 41.8 | 41.8 | Drop 41.8 |
| Unreachable | 39.3, lists 39.4 | Lists 39.4 (L's peering kept 39.4) | The chunk is unfound; 39.3 is never served |
| Any | Any | Any; D, new in 43 and not live in 41, holds nothing | D does not affect the decision; recovery gives D the kept versions |

- A disk formatted again or replaced comes back as a new disk, not as the member it was: on
  41.8 acknowledged by A, B and C, C's disk is formatted again and its node reports a new log ID.
  The authority gives it a new disk ID, keeps C's old disk ID down, and peering keeps 41.8 from B.
  The planted variant that keeps C's old disk ID for the new log must drop 41.8 and fail "no
  acknowledged version is ever dropped".
- A second device reporting a log ID that is already up (a cloned drive) is refused, and the disk
  that reported it first keeps its ID.
- Before the primary activates, each member persists its missing list: for each chunk it lacks or
  holds a dropped version of, the kept version it must get. Trace check: no activation report before
  every member's missing-list write has its `DiskDone`. Reads and removal of entries are under
  [Recovery per PG](#recovery-per-pg).
- A member's log holds a chunk's versions in order, so a kept version never follows a dropped one;
  the simulator checks this after every step.
- A partial-block write to a chunk whose kept versions are not yet on the new primary waits. A
  whole-block write to the same chunk goes ahead, and a partial write to another chunk goes ahead.
- Open: whether recovery's copy of a block is skipped, or versioned below, a new whole-block write
  that went ahead to the same block (M8 shows the clobber). Case: kept 41.7 is on B only; new
  primary C takes a whole-block write to block 0, then recovers block 0 from B. The oracle must show
  the new write.
- Sub-block spike seeds:
  - M6 (a suffix dropped, 3,670 primary changes, 8 writes pipelined per client) and M9 (writes up to
    3 blocks long): no seed of 500 differs from the oracle.
  - M7 (any subset dropped) and M8 (merging before the kept versions are held) are planted bugs that
    fail at least one seed of 500. The spike's model failed 500 and 458 seeds.
- A restarted primary asks for a new interval of each PG it leads, peers in it, then serves. A
  planted variant that peers in its old interval must fail "no two members hold different data under
  one version" when a stale write from its previous run lands after peering.
- Activation is reported on the next heartbeat. The authority records the last activated interval in
  a later table that starts no interval, and trims the past intervals' member lists.

## Failure and fail-slow detection

A simulated single authority stands in for the control plane. `node_down_after` 5 s,
`member_timeout` 1 s, `slow_disk_factor` 10×, `slow_disk_window` 1 minute.

**Node failure**

| Events | Result |
| --- | --- |
| Heartbeats every 1 s, the last at t = 10.0 s | Marked down at t = 15.0 s, in a table that starts a new interval of every PG with a member on the node |
| A heartbeat at 14.9 s | Not marked; the 5 s count restarts from 14.9 s |
| A primary waits more than 1 s for member M's acknowledgment and reports it; the direct probe fails | M is marked down at once, not after 5 s |
| The same report; M's node and loop are up, but a write M's disk took 1.2 s ago has not completed | The probe answers from memory with age 1.2 s; M is marked down at once |
| The same report, and M's loop does not answer within 1 s | M is marked down |
| The same report, and the probe answers with no I/O older than 1 s | M is not marked |
| No report; a disk's oldest unfinished flush passes 1 s (the primary's own disk, or one holding only shard replicas) | Marked down from the next heartbeat |
| A disk's open log fails a write | Marked down from the next heartbeat, until recovery has run and it rejoins |
| A discard has been unfinished for 3 s; no read, write or flush is older than 1 s | Not marked |
| A stalled disk whose marking leaves a PG below `min_live` | Marked down anyway; the PG reports that it is below `min_live` |
| A stalled disk whose I/O completes; then normal for 50 s, then a whole minute | Stays down and probed for 50 s; rejoins after the whole `slow_disk_window` |
| Node A crashes at t = 0 with writes in flight | Writes to A's PGs wait until the down table (at most 5 s); writes to other PGs never wait |
| The same, for the PGs A led | The new primary then peers once its grantors' grants to A expire on both clocks: with a grantor's `CLOCK_BOOTTIME` −22.6%, 5.2 s after the grant, so up to about 0.2 s after the down table |
| A node flapping: down 5 s, up, every 10 s | Without refill, two intervals per flap for every PG with a member on the node (F1). With refill staged one member per table, 1,202 tables per flap and 3 intervals for the busiest PG (F2). Nothing damps it |

- Open: damping a flapping node, and delaying refill after a node goes down, are not decided (PG
  epochs study, "Not decided").
- Open: the link between a primary and member M is down while both reach the authority. The primary
  reports M, the probe answers healthy, and the PG's writes wait while the link stays down (open
  question "A link down between a primary and one member"). Today's expected result is that wait;
  the row changes with the rule the open question picks.
- Open: the 1 s stall threshold is a default, not a measurement (open question "The stall
  threshold").

**Fail-slow.** Disk X is in a pool of 6 disks, under 5 minutes of simulated load. The simulated disk
models a 100× fail-slow; each row sets X's slowdown so that its 99th percentile has the stated ratio
to the pool median.

| X's p99 against the pool median | Result |
| --- | --- |
| 12× for 60 s | Marked down as if failed |
| 12× for 59 s, then normal | Not marked |
| 9× for 10 minutes | Not marked |
| 12× for 60 s, but marking X down would leave a PG below `min_live` | Not marked; the pool reports those PGs as slow |
| Marked down, then normal for 59 s, then slow again | Stays down and keeps being probed |
| Marked down, then normal for a whole 60 s | Rejoins, with a new interval of its PGs |
| Alternating 50 s slow and 10 s normal | Never marked, and its PGs' intervals never churn |

- A latency report for an unknown disk, or from a node that is not the PG's primary, never panics.
  Open: how it is handled.
- Open: what the median is over a pool of 2 disks, whether it includes the disk being judged, and
  over what window the p99 is computed.

## Recovery per PG

- Most degraded first. In a `rep3` pool where PG 4 is missing 2 members and PGs 9 and 12 are missing
  1 each, recovery's first copies are for PG 4.
- Resync after maintenance comes first. A node returns from `maintenance` with `rep2` PGs that took
  writes on one copy; those PGs resync before any rebalance move, and the trace shows their recovery
  copies first.
- Open: the order between a maintenance resync and a more degraded PG. Case: `rep2` PG 3 resyncing
  while `rep3` PG 4 is missing 2 members.
- Recovery writes the authoritative member's blocks under a new version to every member that lacks
  the kept version or logged a dropped one, the new primary included.
  - Chunk epochs spike R3: without that record, replay from a checkpoint taken while the dropped
    write was pending brings it back.
  - R3b: with it, replay ends at the recovered blocks.
- A member's missing-list entry goes once the recovered blocks are durable on it.
- Recovery cut short by the next interval: the missing lists survive. The next primary reads them
  and keeps what the activated interval kept (the keep-rule table under [Peering and read
  leases](#peering-and-read-leases)).
- A member never serves a chunk while it is on its own missing list, and never returns an older
  version instead.
- Open: whether a primary whose own list holds the chunk waits for recovery, forwards the read, or
  returns an error.
- Recovery writes pass `write_limit`: a disk at 91% under a 90% limit still takes recovery copies.
- [property] 1,000 seeds of random failures and returns within what the pool survives: once recovery
  settles, every chunk reaches its pool's copy count within the failure-domain rule.
- Losses beyond what the pool survives, for example both members of `rep2` PG 4:
  - Before an operator declares the disks lost, the PG is down and writes wait. Open: whether reads
    wait or return an error.
  - After the declaration, reads return an error, never wrong data or zeros, and the PG is reported.
- Open: the order among equally degraded PGs, and recovery's rate limit or throttle.
- Open: how an empty refilled member is filled. The design says only that a PG is one key range of
  the chunk index; a scan of `(pool, h)` over that range would fit.

## Simulated network

`internal/sim`, run in one goroutine from one seeded source. A seed replays the run exactly: two
runs of one seed give byte-identical traces, compared with `cmp`.

**Message faults**

| Fault | What a test checks |
| --- | --- |
| Drop with probability p | The sender's deadline on `ctx.Now` fires; nothing hangs |
| Delay up to D | Delivery after the delay, in seeded order |
| Round-trip jitter, seeded per message | The rate bound's shortest-round-trip choice in each third is exercised |
| Asymmetric queueing up to 50 ms on grants | One direction only |
| Reorder | Two messages arrive swapped (see the Open below) |
| Duplicate | A duplicated replica write of one version with identical data changes nothing (see the Open below) |
| Partition, symmetric or one-way | Messages across it are lost. A partitioned connection fails after a seeded delay, or never, and both are run. On failure every request outstanding on it fails with one `PeerLost` |
| Node crash and restart | Every connection of the node gives its peers a `PeerLost`; unflushed disk state follows the simulated disk |

- Open: TCP keeps order within a connection, and the in-order send rule assumes it. Reorder should
  apply only across connections, or a reordered stream should count as a reconnect.
- Open: how a duplicated client write is recognized at the primary. The design gives a replay rule
  for MetaStore requests and incarnations for restarts, but nothing per chunk write.

**Driver contract** (Modularity), shared with the phase 5 network suite

- A reply is delivered only after its send's buffer has been released.
- A peer's `PeerLost` comes after every buffer of that peer has been released.
- A request that timed out keeps its buffer until the reply or `PeerLost`.

**Clocks per node**

| Clock fault | Model |
| --- | --- |
| Oscillator drift | Raw rate within ±1% of real time, fixed per node per seed |
| Time-daemon slew | `BOOTTIME` rate differs from raw by up to ±22.6%, changing at seeded times; chrony's 8.33% as a case of its own |
| Suspend | Raw and `MONOTONIC` stop; `BOOTTIME` gains the sleep rounded down to whole seconds; one `Resumed` when that gain reaches the threshold |
| Stop or pause in place, `tsc` guest or Firecracker | Every clock gains the pause; marker unchanged |
| Stop in place, QEMU kvm-clock guest | Every clock frozen; marker unchanged |
| Restore or live migration, any hypervisor or clocksource | Every clock frozen; marker changed |
| QEMU migration held at switchover | kvmclock counts the hold, the TSC does not; marker changed (VM pause spike T4b) |
| Messages to a paused node | Queued, then delivered on resume ahead of any request sent later; or lost |

- The simulated driver delivers `Resumed` when `core.Resumed` reports it from the model's readings,
  the loop's own rule: a suspend whose `BOOTTIME − MONOTONIC` gain reaches the threshold, or a marker
  change, and at no other time. A suspend the kernel rounded below the threshold gives none.
- The clock model can be imported alone, by `internal/loop/looptest`, which scripts the event
  loop's clocks from it.
- No message carries simulator time: a node sees only its own clocks.
- Open: a stop on a kvm-clock guest with its messages lost leaves up to a heartbeat of stale reads
  (open question). [Placement faults and invariants](#placement-faults-and-invariants) P15 must
  either exclude it or tolerate exactly that.

## Placement faults and invariants

Each scenario runs over 10,000 seeds once when it is added, and thousands per commit. A failing seed
prints its replay command, and its reduced ops become a data-driven regression case before the fix.
Lease constants are as at the head of [Peering and read leases](#peering-and-read-leases).

**Invariants checked after every step**, each with the number of the [Epoch change
model](#epoch-change-model) property it checks:

- One read-serving primary per PG (1).
- No read returns a version before it commits, and no read returns a version that is later lost.
- No write completes after a later interval of its PG activates (3).
- No write is refused for its interval while its PG's interval did not change. This is a counter,
  expected to be 0 (7).
- No acknowledged version, and no version an activated interval kept, is ever dropped (4).
- No kept version follows a dropped version of its chunk (5).
- No two members hold different data under one version, and no two writes of a chunk get one version
  (6).
- No node's table epoch or PG interval number goes backwards (9).
- No step check trips between two nodes neither paused nor rebooted, and each such pair holds a
  lease within `rate_gap` plus two round trips of first contact. This is a counter, expected to be
  0 (10).
- Every chunk reaches its pool's copy count once recovery settles.
- Too few copies return an error, never wrong data.
- A disk passes `write_limit` only through granted flushes, overwrites, punches and recovery.
- Uniformly hashed fills reach the reported available figure within 1%.
- Every read equals the byte-range oracle of acknowledged writes and punches applied in version
  order (11).

**Scripted scenarios.** Every one also holds every invariant above.

| # | Initial state, steps and faults | Expected |
| --- | --- | --- |
| P1 | 3 nodes, `rep3`. A is primary of PG 7. A partition cuts A and client X from B, C and the authority. The authority marks A down, interval 42, B primary. Client Y writes "dog"; X writes "cat" and reads | X's write never completes, and no read anywhere returns "cat". X's reads at A return the pre-partition value until A's lease ends (3.96 s on A's clocks), then are refused. Y's "dog" is acknowledged only after B's and C's grants to A expire on both of their clocks |
| P2 | PG 7 at 41, activated. Two tables bring it to 42 and then 43 before 42's primary peers | 43's primary gets a member of 42 and one of 41 to adopt 43. No write tagged 41 or 42 completes after 43 activates |
| P3 | Member C removed (42) and added back (43) within one recovery | C refuses tag 41; no write of 41 collects C's acknowledgment |
| P4 | 10 nodes, one node flapping every 10 s for 10 minutes, refill staged one member per table | About 1,202 tables per flap; the busiest PG gains 3 intervals per flap (F2) |
| P5 | Primary A of PG 7 crashes and restarts within 5 s, with a replicated write still in a member's socket | A takes interval 42 and peers (when, see the restart-wait Open under Peering and read leases). The stale write is refused or superseded; no two contents share one version |
| P6 | The counter case under [Replica primary](#replica-primary), under 4 clients | As stated there; the refusal counter of other PGs stays 0 |
| P7 | `pg_count` 1,024 → 2,048 with writes in flight to PG 567 | PGs 1,134 and 1,135 start one interval above 567; every chunk's versions keep growing |
| P8 | Disk X turns 100× slow for 90 s, then normal | X is marked down after 60 s and back after a whole normal minute; no PG drops below `min_live` because of it |
| P9 | Holder `BOOTTIME` −22.6%, grantor `BOOTTIME` +22.6%, holder raw −100 ppm, grantor raw +100 ppm | No overlap: the holder ends by 3.9604 s, the grantor at 4.0001 s or later |
| P9b | Slews up to ±22.6% changing at seeded times, raw ±1%, 10,000 seeds | No overlap from the first lease |
| P10 | No daemon: holder's clocks both −1%, grantor's both +1%, after the first bound, once the window is full | Margin 2.52% plus the bound's overshoot; the holder ends by 3.939 s, the grantor at 3.961 s |
| P10b | As P10, from either node's start, a partition right after the first grant | No lease before the second grant, `rate_gap` later; no overlap |
| P11 | Grants delayed by up to 50 ms in one direction | The bound widens only toward safety; no overlap |
| P12 | Holder suspended 30.6 s; `BOOTTIME` gains 30 s, raw and `MONOTONIC` gain 0 | `Resumed`, and every lease ends before the next read. A read at the old holder is refused until it adopts the new table; a read at the new primary returns the new write |
| P12b | Holder suspended for less than a second; the kernel rounds the sleep to 0 s, below the `Resumed` threshold | No `Resumed`; the holder keeps its leases, and no read overlaps a new primary's |
| P13 | Holder VM stopped 10 s, clocks frozen, messages queued, marker unchanged | The step check ends the leases before any read |
| P14 | Holder VM restored from a snapshot 10 s old, messages lost, marker changed | `Resumed` ends the leases |
| P15 | P13 with messages lost | Open: up to a heartbeat of stale reads (open question); tolerated or excluded |
| P16 | Node B crashes during peering of interval 42, after adopting it | Peering reaches another member of 41, or waits; no acknowledged version is lost |
| P17 | Node B restarts 1 s after granting A a lease | B joins no peering before 4 s after its start; A's reads stop at A's own end |
| P18 | Two nodes write 1 to 300 bytes into block 5, 8 pipelined each, with a primary change while a base read is in flight | The byte oracle holds |
| P19 | A merge's base block fails its checksum on the primary's disk | The base is fetched from another member; the oracle holds and no rotten byte reaches a new version |
| P20 | Pool at `write_limit` on one disk; a refill after a disk failure moves members onto the fullest disks | Writes adding data are refused with `NoSpace`; overwrites, punches and recovery pass; no disk passes the limit by any other path |
| P21 | Capacity layouts C1 to C6 filled with uniformly hashed chunks | The first refusal comes within 1% of the reported available figure |
| P22 | `rep3`, `min_live` 2, two members down with writes in flight | Writes are rejected until recovery restores a member; no write is acknowledged on one copy |
| P23 | `rep3` on 3 nodes of 2 disks; a disk fails, and the node's other disk, the only eligible one, fills to its log reserve during the refill, so copying stops | 60 s later on `ctx.Now` the pool raises `Stuck`, naming the PGs; adding a disk lets them copy, and `Stuck` clears |
| P24 | A node reboots, its disks down and not marked out | No capacity figure or pool condition changes: its disks keep their shares at their last reported fills |

- Open: P20's fault in Testing strategy happens "while grants are out". Grants arrive in phase 4,
  and the row gains them with the control-plane group.

## Consistency check of chunk histories

- The simulator records `(invoke_time, complete_time, op, result)` for every client chunk operation,
  in a format Porcupine reads (the phase 1 history recorder).
- The model is per chunk: a 4 MiB byte array and a version. Operations:
  - `write(off, bytes, expect_ver?) → version or error`
  - `punch(off, n, expect_ver?) → version or error`
  - `read(off, n) → bytes, version`
  - `read(off, n, at_ver) → bytes`
- Linearizable: each operation takes effect at one instant between its invoke and its complete.
- An operation with an unknown result is checked as maybe applied, with no completion.
- Open: which errors are definite (`NoSpace` before ordering, an `expect_ver` mismatch) and which
  leave the result unknown. Unknown results come from a timeout, a `PeerLost`, or an interval
  refusal after the write was sent to members, since peering can keep such a write.
- Porcupine finds a linearizable order for every chunk of every history from scenarios P1 to P22 and
  from 1,000 random seeds.
- Planted bugs, each found non-linearizable on at least one seed of 100:
  - A read served from a pending version.
  - An old primary serving reads with the lease check disabled (P1).
  - The holder ending its lease on the slower clock (P9's slews).
  - Merging on the committed version (sub-block M2).
  - Peering dropping a non-suffix (M7).
- Histories are split by chunk, so the check's cost is per chunk. Open: its CI time budget, for
  example for a history of 3 clients × 400 operations on one chunk.
- Close-to-open and the file-level checks of Testing strategy's layer 3 belong to the file frontend
  (phase 6).

## etcd-raft module

- The module graph resolves `go.etcd.io/raft/v3` v3.6.0 through a `replace` to the pinned fork, and `go mod verify` passes.
- **Seeded election timeouts.**
  - Two replicas built with the same seed draw the same sequence of randomized election timeouts. Seeds 1 and 2 differ within their first 100 draws.
  - Every draw lies in [`ElectionTick`, 2 × `ElectionTick` − 1]: 100 to 199 ticks of 10 ms, so 1.00 to 1.99 s.
  - No call reaches `crypto/rand` or `globalRand`: a test with a panicking `crypto/rand` reader installed passes.
- **Replay** (the review criterion). A simulator seed with 3 nodes, one shard, an election, proposals and a leader crash, replayed twice, gives byte-identical traces. A planted call to unseeded randomness inside the fork makes the two traces differ, and the test must catch it.
- **ReadIndex retry fix** (etcd-io/raft#397). The upstream test that came with the fix passes on the fork, and fails on plain v3.6.0. Open: the design names the fix but not its behaviour, so the reviewer checks the fork's diff against the upstream commit.
- **Tech-debt entries.** `docs/tech-debts.md` holds two entries, added in the same change:
  - the version pin, removed when an etcd-raft release keeps messages and entries without per-field allocations, at most 3 per message in `spikes/raft-rounds`;
  - the fork, removed when an upstream release takes a configurable random source, includes #397 and meets the pin's bar of 3 allocations per message.
- `scripts/lint.sh`'s license check passes for etcd-raft (Apache-2.0) and its dependencies. No CockroachDB-licensed code is in the fork or the shard.

## Upstream Raft tests

- Every test package of etcd-raft v3.6.0 passes on the fork: the unit tests and the datadriven interaction tests under `testdata/`, with `confchange`, `quorum` and `tracker` included.
- Tests that depend on randomized election timeouts pass with the seeded source. If one depended on `crypto/rand`'s distribution, that is a finding to report, not a test to skip.
- Each of the fork's two changes comes with a test: the seeded timeouts (etcd-raft module, above), and #397's own test.
- Whenever the fork moves to a new upstream tag, the whole suite must pass on that tag before the `replace` changes. A failing test blocks the move and is recorded by name.

## Raft shard

Trace tests that feed events to the shard (`OnEvent`) and assert on its actions.

**Configuration.** The shard builds its `raft.Config` with:

- synchronous `Ready`/`Advance`, with `AsyncStorageWrites` off;
- `CheckQuorum` off and `PreVote` on;
- an election tick of 10 ms, `HeartbeatTick` 10 (100 ms) and `ElectionTick` 100.

A test asserts each of these from the config. Raft storage in the disk log and Leader leases refer to this test rather than repeating it.

| Input | Expected actions |
| --- | --- |
| `ctx.Now` advances 10 ms | One `Tick` to `RawNode` |
| `ctx.Now` advances 100 ms on a leader of an active group | 10 ticks, and a heartbeat to each follower |
| `ctx.Now` advances 35 ms | Open: three ticks with the 5 ms remainder carried, or three ticks with it dropped. The design says only that ticks come from `ctx.Now` |
| `ctx.Now` advances 5 s in one event, as after a suspend or with `Resumed` | Open: 500 ticks at once, or a capped count |
| No event for 2 s on a follower that supports no leader | It campaigns, pre-vote first, at its randomized timeout, between 1.00 and 1.99 s |
| A peer message of 256 bytes | Delivered inline |
| A peer message of 257 bytes or more (an append with entries) | Carried as an `IOBuf` handle, never inline |
| An append over 256 bytes while the buffer pool is exhausted | The message is dropped as lost and counted, never a panic or a heap fallback. Raft resends it |
| A `NetRecv` that fails to decode (truncated, bad type) | Dropped and counted. No panic, and no step into `RawNode` |
| A message for a group the node does not host | Dropped. Open: whether it is answered |
| `PeerLost` for a follower's node | Open: whether the shard calls `ReportUnreachable` |
| A leader with proposals in a round | The `MsgApp` to followers goes out in the same pass as the leader's own `DiskWrite`, not after its flush. Batching into one entry per round is under Shard state machine |
| A follower receiving an append | `MsgAppResp` held until the durable notice for the round that holds the entry, then sent |
| A node granting a vote | `MsgVoteResp` held until the round holding the new term and vote is durable |
| A node answering a pre-vote | `MsgPreVoteResp` goes out with the `Ready`'s other messages, since a pre-vote changes no hard state |
| A `Ready` with committed entries | Applied to the state machine after persistence, then `Advance`. No second `Ready` is taken before `Advance` |
| A crash before the durable notice | On restart the follower's log lacks the entry, and the leader resends it. No acknowledgment was sent for it |

[property] Across seeds, a follower never acknowledges an entry, and no node grants a vote, before the round that holds it is durable.

## Leader leases

Unit and trace tests for fortification on the per-node-pair leases. Lease time follows the two-clock rule: the holder ends a grant `lease_drift` (40 ms of 4 s) early on its faster clock, counted from when it asked; the grantor waits the whole lease on its slower one.

**Votes and campaigns while supporting a leader**

| Follower state | Message or event | Expected |
| --- | --- | --- |
| Supports leader L under its node's grant to L's node, support epoch e, grant held | `MsgPreVote` from challenger C | Dropped before `RawNode`. No response |
| Same | `MsgVote` from C | Dropped |
| Same | `MsgVote` with the transfer context, from the target of a planned transfer | Passed to `RawNode`, which votes under Raft's rules |
| Same | The leader's Raft heartbeats are lost for 2 s while the node-pair grant holds | Its election timer is stopped: no campaign |
| Grant to L's node lapsed on the grantor's clocks, epoch bumped to e + 1 | `MsgPreVote`, then `MsgVote`, from C | Passed and answered under Raft's rules (the review criterion "votes once its grant lapses") |
| Supports no leader | `MsgVote` | Passed |
| Node restarted 2 s ago on both clocks (within `primary_lease`, 4 s) | Any `MsgVote` or `MsgPreVote` | Refused: no vote and no peering for one `primary_lease` after start |
| Node restarted 4.1 s ago on both clocks | `MsgVote` | Answered under Raft's rules |
| Node restarted 4.1 s ago on `CLOCK_BOOTTIME`, 3.9 s on `CLOCK_MONOTONIC_RAW` | `MsgVote` | Refused |
| Holder's renewal naming lapsed support epoch e, after the grantor moved to e + 1 | Renewal | Refused. A new grant comes under e + 1, and every fortification under e has ended |

**Expiry of one grant, on the holder**

| Input | Expected expiry |
| --- | --- |
| Grant asked at 0 ms, received at 50 ms | 3.96 s after the ask, on the faster of the holder's clocks, not 4.01 s |
| The holder's rate bound for this grantor is 1% fast: 0.5% over `lease_drift / 2` | The margin for this grantor widens by the 0.5% excess, to 60 ms: expiry 3.94 s after the ask |
| No rate bound for this grantor's boot yet, as after either node starts or the grantor reboots | No expiry from this voter: it does not count toward the quorum, though its support stands |

**Lease end: the quorum-th latest expiry over the fortified voters**

The leader counts itself as a fortified voter. Open: "itself included" gives the leader's own entry no expiry. The cases treat it as unbounded, so it never sets the lease.

| Voters | Fortified, with expiries | Lease ends |
| --- | --- | --- |
| 3: L, F1, F2 | L, F1 (t1) | t1 (the review criterion: lease to that follower's expiry) |
| 3 | L, F1 (t1 = 10 s), F2 (t2 = 12 s) | 12 s (2nd latest of ∞, 12, 10) |
| 3 | L only | No lease: reads go through ReadIndex |
| 5 | L, a (10), b (20), c (30) | 20 s (3rd latest) |
| 5 | L, a (10) | No lease |
| Joint, old {L, B, C}, new {L, D, E} | B (10), D (12) | min(10, 12) = 10 s: each half gives its own quorum-th expiry |
| Joint, as above | B (10) only | No lease: the new half has no quorum |

**Reads**

| State | Read | Expected |
| --- | --- | --- |
| New leader, fortified, no entry of its term committed yet | Lease read | Served through ReadIndex instead |
| Fortified, an entry of its term committed, lease unexpired | Lease read | Served from memory, with no round trip |
| `Ready` carrying `MsgTimeoutNow` to the transfer target has left | Lease read | No lease read from this point on. Served through ReadIndex or refused |
| Holder receives `Resumed` (a suspend, or a VMClock marker change) | Lease read | Stops serving lease reads at once, asks each grantor again, and restarts its rate window |
| A grantor's heartbeat shows its `CLOCK_MONOTONIC_RAW` moved further since the last grant than the holder's could have | Lease read | Ends its leases as on `Resumed`. Peer messages in the pass are handled before reads |

**Simulator faults** (MetaStore), with the invariant "no two replicas of a shard serve lease reads at overlapping times":

1. **A quiescent leader's node partitioned from one follower.** The other follower's grant keeps the lease. The partitioned follower's grant lapses and it stops supporting, but it cannot win without a majority. Expected: one lease-reading replica at any time.
2. **A leader's node suspended past its lease (5 s).** On resume it ends every lease (`Resumed`) before serving. A new leader may have been elected after the grants lapsed on the grantors' clocks. Expected: no overlap.
3. **A leader transfer while lease reads arrive.** Expected: the old leader serves none after `MsgTimeoutNow` leaves, and the new leader none before it is fortified and has committed an entry of its term.
4. **A configuration change while a lease is held.** Expected: the lease requires quorums in both halves throughout the joint configuration.
5. **Lease clocks.** A holder's or grantor's `CLOCK_BOOTTIME` slewed by up to ±22.6% while its `CLOCK_MONOTONIC_RAW` drifts by up to ±1%, and grants delayed by asymmetric queueing of up to 50 ms. Expected: no overlap. A crash fails over within 5.2 s plus an election, the most a 22.6% slew can stretch a grantor's wait.

## Raft storage in the disk log

- **Record contents.** A round with chunk writes and two shards' `Ready` writes one record to the hot stream. It holds the chunk blocks, and each shard's entries and hard state as blob payloads, with one write and one flush. The trace shows exactly one `DiskFsync` for the round.
- **Serving entries.** `raft.Storage.Entries(lo, hi, maxSize)` serves every entry after the last compaction from the loop's off-heap slab, with no disk read and no allocation per entry.
  - It returns `ErrCompacted` for lo at or below the compaction index.
  - It honours `maxSize`, returning at least one entry.
  - `Term(i)` returns `ErrCompacted` below the first index and `ErrUnavailable` past the last, as etcd-raft's `MemoryStorage` does.
  - A hi past last + 1 is a broken invariant of the caller.
  - `FirstIndex` and `LastIndex` are consistent with `Entries`.
- **Compaction.** Once the memtable that applied entries up to index i has been flushed, the shard compacts its log to i, and the segment usage table counts those entries' blocks dead to the cleaner. Before that flush, they stay live.
- **Bound.** [property] Retained entries never exceed twice `max_replay_bytes` per disk (128 MiB at the default 64 MiB), over seeds with bursts of proposals and delayed flushes.
- **Replay.** After a crash, a shard restores from its manifest's runs and replays the log after its replay LSN. Later records win: an entry at index 5 of term 3 replaces the earlier record's index 5 of term 2, the earlier record's entries 6 and on of term 2 are dropped, and `LastIndex` after replay is 5. The latest hard state in LSN order wins. The crash oracle compares the recovered shard with its state at the last acknowledged LSN.
- **Torn tail.** A torn record at the log's tail holding chunk writes and Raft entries together drops both (a simulation fault from the on-disk format list). No acknowledgment was sent for them, the leader resends, and no committed entry is lost.
- **Lagging follower.** A follower behind the compaction index gets a `MsgSnap` built from a point-in-time view of the shard's LSM tree, which never blocks the writer, and catches up from it. A view held open holds back the cleaner (asserted in the LSM tree tests).
- **Asynchronous storage writes stay off** (asserted under Raft shard). The two guards that would allow them get their own scenarios when they are built: drop apply work that a restored snapshot covers, and never compact past the durable commit index ([async snapshot apply study](../studies/etcd-raft-async-snapshot-apply/README.md)).

## Raft cost per group

A regression benchmark against the [raft rounds spike](design.md#appendix-raft-rounds-spike), run nightly. A 20% regression is a bug. This benchmark is exempt from the zero-allocation rule for "Benchmarks", by the etcd-raft version pin in `docs/tech-debts.md`, whose exit condition is at most 3 allocations per message.

| Measure | Setup | Bar | Spike figure |
| --- | --- | --- | --- |
| Allocations per Raft message | 3 nodes × 1,000 groups per node, 128 B proposals, open load at 60,000 proposals/s, 10 ms tick, 100 ms heartbeat, synchronous `Ready` | ≤ 3 | 2.7 (v3.6.0) |
| Heap per idle group | 1,000 groups per node, no proposals | ≤ 4 KiB | 2.8 KiB (2.7 MiB per node) |
| Allocations per proposal | As above | Tracked | 22.6 |
| CPU per proposal, across its three replicas | As above | Tracked | about 27 µs |
| Collection's share of CPU | As above | Tracked | 14% |
| Idle CPU per node, 10 ms tick, not quiesced | 1,000 groups | Tracked | 2 to 8% of a core |

The benchmark calls `b.ReportAllocs`. Quiesced groups (0.07% of a core in the multi-raft spike) are phase 13.

## Maelstrom harness

- **Adapter.** A small I/O adapter runs the Raft shard as a Maelstrom node: JSON messages on stdin and stdout, Maelstrom's network for peer messages, and the shard unchanged behind `OnEvent`. Open: whether the harness carries its own small map as the state machine, or waits for `internal/meta`'s `Txn`. Maelstrom harness comes before Shard state machine in the checklist.
- **Pass criterion.** It passes Maelstrom's `lin-kv` workload (`read`, `write`, `cas`) under `--nemesis partition`, checked linearizable by Maelstrom's checker (the [done-when checkpoint](design.md#learning-project)). Open: run parameters. A starting point is 3 and 5 nodes, `--time-limit 60 --rate 100 --concurrency 2n --nemesis-interval 5`.
- **Error codes.**
  - A `cas` on an absent key returns error 20 (key does not exist), and a `cas` whose `from` does not match returns 22 (precondition failed).
  - Error 11 (temporarily unavailable, a definite error) is returned only for an op that was never proposed.
  - An op that was proposed and then orphaned by a leader change returns 13 (crash, indefinite), or no reply.
- **Reads.** Reads go through ReadIndex or a lease read. Open: whether the harness carries the node-pair leases (`internal/lease`). Without them it must use ReadIndex for every read.
- **Planted bug.** A run where any node serves reads from its applied state must fail the checker.

## Shard state machine

**`Txn`: checks first, then writes in order, all or nothing**

| Ops | Expected |
| --- | --- |
| put a=1 | Committed; get a = 1 |
| check a == 1, put a=2 | Committed; a = 2 |
| check a == 1 (a is 2), put a=3, put b=1 | Fails with `FailedCheck` 0 and `Actual` 2. Neither a nor b changes |
| check a absent, put a=1, where a exists | Fails. This is `create`'s `EEXIST` and S3's `if-not-exists` |
| delete a, where a is absent | Committed: a delete of an absent key succeeds, as in CockroachDB, etcd and TiKV; the purger depends on it |
| put a=1, delete a, in one `Txn` | Committed; a is absent. Writes apply in list order |
| check a absent, put a=1, in one `Txn`, where a is absent | Committed: checks see the state before the `Txn`, not its own writes |
| `Update(a, 0, Add, 5)`, `Update(a, 8, Bump, 3)`, put b, delete c, on an absent a | Committed; a's two fields read 5 and 3; `Updated` is [5, 3] |
| `Update(a, 0, Bump, 10)` where the field holds 20 | Field 21: `Bump` stores max(n, field + 1) |
| `Update(a, 0, Add, 1)` where the field holds 2^64 − 1 | Field 0: `Add` wraps |
| `CheckEmpty(d/, PrefixEnd(d/))` over a directory with one entry, then after that entry's delete | Fails, then passes. `rmdir` of a non-empty directory fails with `ENOTEMPTY` |
| `DeleteRange(k1, k3)` over k1, k2, k3 | k1 and k2 removed, k3 kept: the range is half-open |
| A key of 1,033 bytes, or a value of 1,001 bytes | Refused with an error before proposing, never a panic (`MaxKey` 1,032, `MaxValue` 1,000) |
| A `DeleteRange` or `CheckEmpty` bound of 1,001 bytes | Refused before proposing; a bound of 1,000 bytes commits |
| A `Txn` encoding to more than 256 KiB (`max_txn_bytes`), as an 82 KiB xattr in pieces fits and 1 MiB does not | Refused before proposing with a size error, never split |
| Empty ops | An error |
| Two racing `Txn`s, each check a absent, put a=x | Exactly one commits (the design's lock-race example) |
| 64 concurrent `create`s in one directory, distinct names, inodes from each daemon's reservation | All commit in one round with distinct inodes; no check fails, since the parent's times move by `Update` |
| 1,000 `create`s in one directory over 100 rounds | All commit without one failed check |
| A `create` from a node whose clock is 10 s behind the directory's `mtime` | The parent's `mtime` still moves forward by 1 ns |

**`Scan(start, end, limit)`**

| Keys | Call | Expected |
| --- | --- | --- |
| a, aa, ab, abc, b | Scan(a, PrefixEnd(a), 10) | a, aa, ab, abc; `more` false |
| Same | Scan(ab, PrefixEnd(ab), 10) | ab, abc |
| Same | Scan(aa + 0x00, PrefixEnd(a), 10) | ab, abc: "after aa" is `start` = aa + 0x00 |
| Same | Scan(a, PrefixEnd(a), 2) | a, aa, `more` true; then Scan(aa + 0x00, PrefixEnd(a), 2) gives ab, abc, `more` false |
| Same | Scan(zz, PrefixEnd(a), 10) | Empty (start past end) |
| Same | Scan(c, PrefixEnd(c), 10) | Empty |
| Same | limit 0 | An error, since an unbounded reply would allocate without bound |
| Same | Scan(k, k + 0x00, 1) | The point read of k |
| Keys and values totalling 1 MiB | Scan with limit 10,000 | Stops at 256 KiB (`max_scan_bytes`) with `more` true; resuming from the last key + 0x00 returns the rest |
| A key of `MaxKey` bytes | Resume from it + 0x00 | Accepted: a scan bound of `MaxKey` + 1 bytes is never stored |
| Same | A scan over the whole key space, with the control-plane group and one data shard | Refused by routing, as every range spanning groups is |
| 1,000 directory entries under one directory | Scan of the directory's `d/` prefix | All 1,000 in name order, from one point-in-time view, while writes continue |

**Rounds and reads**

- **One log entry per leader per round.** 100 `Txn`s arriving at a leader during one round become one Raft entry. The trace shows one entry per round.
- **Reads never wait for writers.** A scan running while 1,000 puts commit returns the view as of its start.
- **Consumed lists.** A purge list consumed in key order has each consumed run removed by one `DeleteRange` op in a `Txn`, so it replicates like every other write and a seek to the queue's head steps over 1 removal, against 10,820 point removals in the LSM deletions study.
- **Benchmarks** (bars from the KV engines and MetaStore memory spikes): a get from a shard of 10 million files at about 1 µs (0.96 to 1.00 µs from the memtable in the spike), and no slower than redb's 7.2 µs; a 1,000-entry listing at about 3.5 µs from memory (redb 196 µs).

Recovery from manifest and log is under Raft storage in the disk log and MetaStore invariants. Bytes per file are under the LSM tree item.

## Key encoding

**Rules.** Each path component `word/` is a one-byte tag. Integers are fixed-width and big-endian:

- volume IDs, 32 bits;
- inode IDs, 64 bits: a 24-bit slot and a 40-bit sequence;
- session IDs and request IDs, 64 bits; mount IDs, 32 bits;
- table epochs and snapshot epochs, 64 bits; a lock mode, 8 bits.

Only the last component (a file name, an xattr name or an S3 key) has variable length, with no escaping and no terminator.

**Tags**, from the [key layout](design.md#metadata-layer-metastore), frozen by the golden file as disk format: first byte `0x01` ctl, `0x02` volume, `0x03` fs, `0x04` s3; under `ctl/` `0x01` pool, `0x02` placement table, `0x03` membership, `0x04` session, `0x05` format level; under `volume/{id}/` `0x01` attach, `0x02` snap; an inode's keys `0x01` a, `0x02` x, `0x03` t, `0x04` l, `0x05` d, `0x06` c, `0x07` P; a slot's keys `0x10` n, `0x11` u, `0x12` o, `0x13` p, `0x14` r, `0x15` f; a piece key is its value's tag with the high bit set (`0x82`, `0x83`), then a 1-byte piece number. The vectors below write some tags symbolically, as `T(fs)`, `T(a)` and so on, standing for these bytes.

**Golden vectors**, checked into `internal/meta/testdata/` and never edited once frozen:

| Key | Bytes |
| --- | --- |
| `fs/5/2:1/a` | `T(fs) 00000005 0000020000000001 T(a)` |
| `fs/5/2:1/d/beach.jpg` | `T(fs) 00000005 0000020000000001 T(d) 62 65 61 63 68 2e 6a 70 67` |
| `fs/5/2:0/n` (slot counters of slot 2) | `T(fs) 00000005 0000020000000000 T(n)` |
| `fs/5/2:0/p/{2:9}` | `T(fs) 00000005 0000020000000000 T(p) 0000020000000009` |
| Root directory's attributes in volume 1 | `T(fs) 00000001 0000000000000001 T(a)` |
| `volume/5/snap/{epoch 3}` | `T(volume) 00000005 T(snap) <epoch 3, fixed width>` |
| `ctl/session/{id}` | `T(ctl) T(session) <id, fixed width>` |
| `fs/5/2:1/a`, literally | `03 00000005 0000020000000001 01` |
| Replay key, volume 5, slot 2, session 7, mount 1, request 42 | `03 00000005 0000020000000000 14 0000000000000007 00000001 000000000000002a`, 34 bytes |
| `fs/5/2:3/l/x/{session 7}` | `T(fs) 00000005 0000020000000003 T(l) <mode x, 1 byte> 0000000000000007` |
| Piece 2 of xattr `user.big` on 2:3 | `T(fs) 00000005 0000020000000003 82 02 75 73 65 72 2e 62 69 67` |

**Order.**

- [property] For random pairs of logical keys, byte order equals logical order: components compared left to right, integers numerically, the last component bytewise.
- Fixed order, under the tags above: `fs/5/2:0/n` < `fs/5/2:1/a` < `fs/5/2:1/d/beach.jpg` < `fs/5/2:1/d/cat.jpg` < `fs/5/2:2/a` < `fs/5/3:0/n` < `fs/6/1:0/n`. Every key of slot 2 sorts before every key of slot 3, so a shard can split between slots.
- Volume 255 < volume 256: the 32-bit big-endian encoding keeps numeric order where a decimal string would not.
- A prefix scan of `fs/5/2:1/d/` returns directory entries only, never `fs/5/2:1/x/...`.
- A scan from `fs/5/2:3/a` through the end of `fs/5/2:3/x/` returns the attributes and the xattrs, and no other key kind.
- File names with 0x01 or 0xFF bytes, and S3 keys with 0x00, are still the last component and sort bytewise.

**Errors.** The encoder refuses a slot ≥ 2^24 or a sequence ≥ 2^40. The decoder refuses each of these, as an error, never a panic, and a fuzz test covers it:

- a truncated key;
- an unknown tag;
- a missing fixed-width field;
- trailing bytes after a fixed-width last component;
- a slot tag at a sequence other than 0.

**Limits and pieces.**

- An S3 key of 1,024 bytes gives an index key of 1,029 bytes, within `MaxKey` (1,032); a 1,025-byte S3 key is refused at the frontend.
- A 64 KiB xattr is stored as its first piece and piece count under `x/{name}` and about 65 more pieces, in one `Txn` of about 82 KiB, and reads back whole with 1 + m lookups; a 4,095-byte symlink target is 5 values.
- Names over 255 bytes, xattr names over 255, symlink targets over 4,095 and xattr values over 64 KiB are refused at the frontend, before any `Txn`.
- [fuzz] The piece decoder refuses a missing piece, a piece count past 66, and a piece past the count, as errors.

## Sharded API, one shard

- **Routing.** `Txn` routes its ops through the shard map, sorted and deduplicated. With one shard every key maps to it, and a trace shows one `Propose` to that shard's leader per `Txn`, never a cross-shard path.
- **Two shards.** A shard map with two ranges, used only in a unit test of the router, routes a `Txn` touching both to the slow path, which in phase 4 refuses it with `ErrCrossShard` naming both groups and proposes nothing. A `Txn` touching a `ctl/` key and a data key is refused the same way, since the control-plane group is a Raft group of its own; no designed transaction does that. Cross-shard transactions arrive in phase 13.
- **At a follower.** A `Txn` sent to a follower or a candidate is never forwarded: it is answered `NotLeader{shard, leader node, term}` and proposes nothing. The daemon updates its route cache and resends to that node, and the trace shows one `Propose`, at the leader. With no known leader the reply names none, and the daemon retries after the election.
- **During an election.** A client request arriving during an election fails retryably and is never applied twice: its resend carries the same replay key and returns the first final result (see Filesystem schema).
- **Internal `Txn`s across a leader change.** The purger's `Check(p/i = v)` with `Delete(p/i)` applies, its reply is lost, a new leader is elected, a truncate merges a new range into `p/i`, and the purger resends: the check fails and nothing is deleted. The same with a blind `Delete(p/i)` (planted bug) loses the merged range, and the purge invariant fires. A control-plane table version `Check(e)`, `Put(e + 1)` repeated after an election fails its check.
- **`ctl/` keys.** They go through the same `Txn` and `Scan`, and the shard map pins `[ctl/, PrefixEnd(ctl/))` to the control-plane group, which never splits: a split proposed inside it is refused.

## Control-plane group

**Pools**

| Input | Expected |
| --- | --- |
| A `Pool` create or update | Checked against the [Pools](#pools) creation rules, then committed under `ctl/pool/`. A refused one commits nothing |
| `replicas` changed | The next table starts a new interval of every PG of the pool |

**Membership**

| Input | Expected |
| --- | --- |
| A node misses heartbeats for `node_down_after` (5 s) | Marked down in one table, which starts a new interval of every PG with a member on it |
| A primary reports a member after `member_timeout` (1 s), and the member fails a direct probe (its disk stalled, its log failed, or no answer within 1 s) | Marked down at once |
| The same report, but the probe answers with no stall | Not marked down |
| A heartbeat shows a disk stalled, with no report | Marked down at once |
| A disk's 99th percentile acknowledgment latency above `slow_disk_factor` (10×) its pool's median for `slow_disk_window` (1 min) | Marked down as fail-slow, unless that would drop a PG below `min_live` |
| A fail-slow disk's latency back to normal for 50 s, then for a whole minute | It rejoins only after the whole `slow_disk_window` |
| A `role: meta` node | Holds control-plane and shard voters, and no PG member |

**Placement table**

| Input | Expected |
| --- | --- |
| A node holding table epoch e receives the delta for e + 1 | It applies the delta. The CRC32C of its resulting table equals the CRC in the entry |
| A delta whose resulting CRC does not match | The node fetches the whole table |
| A node at e receives e + 3 (a gap) | It fetches the whole table. It loses nothing, since each PG entry carries its interval number |
| A Raft snapshot of the group | Holds the whole table |
| A primary reports activation of interval k on its heartbeat | A later table records k as the PG's last activated interval and drops the member lists of the intervals before it. That table starts no interval |
| A disk added | Each table of the change raises the table epoch. Only PGs whose members change start a new interval |

**Sessions.** The [Sessions and fencing model](#sessions-and-fencing-model)'s scenarios run as simulator seeds against the real group. In addition:

| Input | Expected |
| --- | --- |
| A daemon starts | One commit of `ctl/session/{id}`, with an ID from a counter. IDs are never reused, even after a fence |
| 100 heartbeats renewing 10 sessions | No Raft entry: renewals change only the leader's memory |
| A session falls due | One commit fencing every session due at that moment, as one new session-table version |
| Every storage node acknowledges version v, or is marked down in a committed table | v is committed as acknowledged, and the fenced sessions leave the table |
| A storage node marked down sends a heartbeat reporting an older version | It is not marked up |
| A follower of the control-plane group, or a new leader before its leader lease starts, receives a heartbeat | It answers no renewal |
| A new leader's leader lease starts | Every live session gets a full `session_lease` from that start |
| The leader's loop falls behind by more than a heartbeat interval | Every session's timer restarts from now |
| An owner reports a recall unanswered for `recall_timeout` | The session is fenced in the next version |
| An operator evicts a session | The same |

**Space grants.** They are computed on each heartbeat, per node and pool ([Client design](design.md#client-design)). The inputs:

- A: the pool's available figure before grants;
- F: the bytes nodes reported flushed, from their cumulative counts, since the fills the leader holds were reported;
- askᵢ: min(`writeback_buffer`, what node i holds buffered + what it flushed since its last heartbeat);
- bufferedᵢ, heldᵢ: what node i holds buffered, and the grant its heartbeat says it holds;
- Σ: the sum over nodes of max(heldᵢ, grantᵢ) until a heartbeat echoes grantᵢ, then grantᵢ;
- k = 2: the reuse budget, in grants, between answered heartbeats;
- g_d: for each disk d of the pool, its size less its log reserve less the larger of its fill and `write_limit`.

The rules:

- each grant is at most `writeback_buffer` (256 MiB);
- Σ ≤ (A − F) ÷ 2; lowering A by F is the same as lowering every disk's room by F × its share, since A is the smallest room ÷ share;
- (1 + k) × Σ + F ≤ g_d for every disk d;
- a binding cap is divided by water-filling: grantᵢ = min(askᵢ, max(bufferedᵢ, L)), L the level that fills it, and a node above L buffers nothing new until its buffered bytes fall below L.

| Case | Inputs | Expected |
| --- | --- | --- |
| G1: room everywhere | 4 nodes ask 256 MiB each; A = 2.679 TB (C2); disks at 50% fill | Each node granted 256 MiB; Σ = 1 GiB |
| G2: a node asks for more than its buffer | One node holds 300 MiB buffered and flushed 1 GiB since its last heartbeat | Asks and is granted at most 256 MiB |
| G2b: an idle node | A node with nothing buffered and nothing flushed | Asks 0, and its grant returns to the pool within a heartbeat |
| G3: half-available cap | A = 1 GiB; 4 nodes ask 256 MiB each, nothing buffered | 128 MiB each; Σ = 512 MiB |
| G3b: unequal asks | Cap 512 MiB; asks 256, 256, 64 and 16 MiB | 216, 216, 64 and 16 MiB; the same when the first node holds 200 MiB buffered |
| G3c: a floor above the level | As G3b, the first node holding 250 MiB buffered | 250, 182, 64 and 16 MiB; the first node buffers nothing new until its buffered bytes fall below the level |
| G3d: floors above the cap | Cap 512 MiB; asks 256 each; buffered 200, 200, 200 and 0 MiB | Grants 200, 200, 200 and 0: every grant is its floor, nothing new is buffered, and the excess drains as the nodes flush |
| G4: gap cap on a large disk | A 100 GB disk at 90% fill: gap 9.87 GB after its 8-segment reserve (16 MiB segments); 10 nodes ask 256 MiB | Σ ≤ 9.87 ÷ 3 = 3.29 GB, so 2.68 GB of full grants fit |
| G5: gap cap on a small, fast disk | A 32 GB NVMe disk at 90%: gap 3.17 GB (4 MiB segments); 10 nodes ask 256 MiB | Σ ≤ 1.06 GB, about 101 MiB each by water-filling; no disk write rate enters |
| G6: no gap | g_d ≤ F | Σ = 0, and every daemon writes through |
| G7: available 0 | A = 0 | Σ = 0 |
| G8: flushes not yet in reported fills | A = 1 GiB; 100 MiB flushed and not yet reported | Σ ≤ 462 MiB until the next fill report. After it, the fills include the flush, A reflects it, and F is 0 again |
| G8b: a lost heartbeat request, then a fill report | A node's heartbeat with cumulative count c + 100 MiB is lost; the next carries c + 150 MiB | F counts all 150 MiB from the second heartbeat until a fill report covers it; nothing is lost with the first |
| G9: leader failover | 10 live sessions, 2 pools | The new leader counts each session at (1 + k) × 256 MiB per pool until it renews, and grants new ones only out of what is left; meanwhile a daemon past its budget writes through |
| G10: a node stops renewing | Its grant g was carried by a renewal at t | (1 + k) × g counts until its fence version is acknowledged, `session_lease` plus one fence round after t, then drops out of Σ |
| G11: heartbeat replies lost | One reply lost | Nothing changes: the daemon's early heartbeat at half its budget gets the next reply. With that reply lost too, it writes through once it has used k grants, until a reply comes. No flush is ever refused for space |
| G11b: a shrink whose reply is lost | 3 nodes shrunk from 256 to 50 MiB, the replies lost | The leader counts each at (1 + k) × 256 MiB until a heartbeat echoes 50 MiB; counting 50 at once would let them land 2.42 GB while it counted 0.47 GB |
| G12: grants reused | A node's 256 MiB flush completes | The daemon reuses its grant at once; once it has flushed 256 MiB of its 512 MiB budget it sends its heartbeat early, and the reply resets the window |
| G12b: a streaming node with no faults | A node writing at 25 GbE under a 256 MiB grant | It stays buffered throughout, sending about 11 extra heartbeats per second |
| G12c: a floor grant on a full disk | L = 0; a node holding 200 MiB buffered streams at 1.1 GB/s | Its grant stays at what it has buffered and it buffers nothing new: new writes go through, and its buffered bytes only drain |
| G12d: a daemon crash in the middle of a window | The successor reads the cumulative count from the handoff memfd | It continues the count. A successor that cannot read it buffers nothing until its first heartbeat is answered, and the leader counts the old (1 + k) × grant until fill reports after that heartbeat arrive |
| G13: a disk past `write_limit` | A disk at 92% after a refill moved members onto it | g_d is measured from its fill: 8% of the disk less its reserve |

The invariant, checked after every simulator step: for every disk d of the pool, (1 + k) × Σ + F ≤ g_d, and no flush under a grant is refused for space.

**Capacity figures.** From the fills that heartbeats report (about 8 bytes per disk) and the per-pool data on each disk, the leader computes per pool the available, balanced, stranded-fraction and reprotectable figures and the conditions, `Stuck` from each PG's copying progress, with the [Pools](#pools) cases as inputs, and per node for `locality: node` pools.

**Format level.** A `ctl/` key, raised by the operator. A node running a release older than the level that heartbeats after it was raised is not marked up, and a condition names it; Ceph's monitor likewise ignores the boot of an OSD below its minimum release.

**Replacing the phase 3 stand-in.** The phase 3 placement seeds run unchanged against the real group and pass the same invariants.

**Simulator faults:**

- a control-plane leader failover while sessions renew;
- a renewal answered by a deposed leader;
- the pool reaching `write_limit` while daemons buffer and other nodes write through;
- a heartbeat carrying a space grant lost or delayed, its request or its reply;
- a control-plane failover while daemons stream under their grants;
- a client partitioned from the control-plane leader but not from storage, streaming under its grant;
- a daemon crash in the middle of a reuse window;
- a refill after a disk failure that moves members onto the fullest disks while grants are out.

A write that crosses from a granted chunk into a refused one is a daemon case, covered with phase 6's file frontend.

## Filesystem schema

Transaction shapes, from the design's [transaction table](design.md#metadata-layer-metastore). Each is a single-shard `Txn` with one shard, asserted on the keys and ops it touches. "The parent's times" is `Update(parent a.mtime, Bump, t)` and `Update(parent a.ctime, Bump, t)`.

| Operation | Ops in one `Txn` | Expected, including errors |
| --- | --- | --- |
| Inode reservation, first create of a node in slot 2 | Its own `Txn`: `Update(fs/5/2:0/n, Add, 64)` | `Updated` gives the batch's end; the daemon hands out the 64 numbers from memory and reserves the next batch in the background when 6 are left. A repeat after an election only skips numbers |
| Slot reservation | Its own `Txn`: `Update` of the shard's `f` key, `Add`, 16 | 16 slots for the daemon's `mkdir`s, prefetched the same way |
| `create` of `beach.jpg` in /photos (slot 2), inode 2:k from the reservation | `Check(fs/5/2:1/d/beach.jpg absent, final)`; put the entry → 2:k, type file; put `fs/5/2:k/a`; the parent's times | A second `create` of the name fails with `EEXIST` |
| Two racing `create`s in slot 2 from two nodes, different names | Each with an inode from its own reservation | Both commit in one round, with distinct inodes; neither retries |
| `mkdir` with a free slot from the daemon's reservation | As `create`, plus `Update(parent a.nlink, Add, 1)`; `Put(fs/5/s:1/a)`, `Put(fs/5/s:1/P → 2:1)`, `Put(fs/5/s:0/n = 66)` | Single shard. The directory is s:1, and the creating daemon owns s:2 … s:65 |
| `mkdir`, then `create` in it from the same daemon | The create takes s:2 | No reservation round between them |
| `mkdir` with no free slot left | The new directory shares its parent's slot | Single shard |
| `n` = 2^40 − 1 in slot 2 | The next two `create`s | The first takes 2:(2^40 − 1), the last ID. The second goes to a fresh slot |
| `link` of 2:3 as `b` in /photos | `Check(d/b absent, final)`, `Put(d/b → 2:3)`, `Check(2:3 a)` and `Put(2:3 a)` with `nlink` + 1 and `ctime`, the parent's times | A second `link` to the name fails with `EEXIST` |
| `symlink` with a 4,095-byte target | As `create`, plus the target under `t` in 5 values | `readlink` returns the 4,095 bytes (`pjdfstest` `symlink/03.t`) |
| `unlink` of a file's last link, no open handles | `Check(d/name = inode, final)`; delete the entry; `DeleteRange` over the inode's keys; merge `fs/5/2:0/p/{inode}` with `0 .. alloc_end`; `Update(u, Add, −size)`; the parent's times | One transaction; no attribute, xattr, lock or target key of the inode remains |
| `unlink` of a file with a second link | Delete only the entry; `nlink` − 1 by `Check(a)` and `Put(a)` | No purge entry |
| `unlink` of a file still open | Delete the entry; put `fs/5/2:0/o/{inode}`; `nlink` − 1 | At last close, and after any grace period, the orphan moves to `p/` |
| `rmdir` of an empty directory | `Check(d/name = inode, final)`, `CheckEmpty(child's d/, final)`, delete the entry, `DeleteRange` over its keys, `Update(parent a.nlink, Add, −1)`, the parent's times | Committed |
| `rmdir` of a directory with one entry | Same | `ENOTEMPTY`, stored as the request's final result |
| `rename` within one directory | `Check(src, final)`, `Check(dst absent)`, delete src, put dst | `rename` moves the entry exactly once |
| Rename over a non-empty directory | As above, with `CheckEmpty(dst's d/, final)` | `ENOTEMPTY`; nothing changes |
| Rename-over of a file someone has open | Move the entry; the replaced inode goes to `o/` | As for unlink of an open file |
| Rename-over of a file no one has open | Move the entry; remove the replaced inode's keys; merge its purge entry `0 .. alloc_end` | One transaction |
| Cross-directory rename of directory /a/b into /c, with /c's ancestors /c and / | Both parents' times; `Update(nlink, Add, −1)` on /a and `+1` on /c; `Put(b's P → /c)`; `Check(P = what was read)` on /c | Committed; a create in /c meanwhile does not fail it, since `P` is its own key |
| Cross-directory rename of /a into /a/b/c | The loop check reads c's ancestors up to /a | Refused with `EINVAL`: a directory never moves under itself |
| The same, while another node moves /a/b elsewhere | The moved directory's `Check(P)` fails | The rename retries from a fresh read; no loop forms |
| `chmod` of a directory while 64 creates land in it | `Update(mode, Set, v)`, `Update(a.ctime, Bump, now)` | Commits in the same round as the creates; no `Check` on the directory's attributes |
| Truncate from 10,000 bytes to 5,000 | `Check(a)`, size 5,000, `alloc_end` 1, `Update(u, Add, −5,000)`, and a purge range from byte 5,000 to the old `alloc_end` merged into `p/` | The range starts at the byte offset, not at a chunk boundary |
| Truncate, then unlink before the purger runs | A 16 MiB file (`alloc_end` 4) truncated to 5,000, then unlinked | `p/{inode}` holds the union, `0 .. 4`, not the unlink's `0 .. 1` alone: no chunk leaks |
| Two truncates before the purger runs | 16 MiB to 8 MiB, then to 5,000 | `p/{inode}` holds the union of both ranges |
| `alloc_end` raise | `Check(a)`, `alloc_end` raised, plus the size written so far | One `Txn` |
| `close` whose `Check(a)` loses a race with another node's size change | The check fails, not final | The daemon reads again and retries under the same request ID, and commits |
| `close` or `fsync`, writer's clock behind | Stored `mtime` 1,000.000000005 s; the writer's wall clock reads 999.9 s | Stored `mtime` becomes 1,000.000000006 s |
| `close` or `fsync`, writer's clock ahead | Stored `mtime` 1,000 s; the writer's last write at 1,001 s | Stored `mtime` becomes 1,001 s |
| `close` of a write-shared file, writer's clock behind | Stored `mtime` 1,000 s; the node last reported 1,000.000000040 s; its last write at 999.9 s | Stored `mtime` becomes 1,000.000000040 s |
| `utimensat(500 s)` | Same stored `mtime` | Stored `mtime` becomes 500 s |
| flock `LOCK_EX` | Scan `fs/5/2:3/l/`; `CheckEmpty` over each gap between the keys found; put `fs/5/2:3/l/x/{s}` | Granted when no live session holds a key |
| flock `LOCK_EX` while a session gone from the session table holds `l/x/{t}` | `Check(l/x/{t} = v)`, `Delete` it, the gap checks, `Put(l/x/{s})` | Granted in one `Txn`, the dead holder's key gone |
| flock `LOCK_SH` from two sessions, then `LOCK_EX` from a third | `l/s/{s1}`, `l/s/{s2}`; each shared lock checks `l/x/` only | Both shared locks granted; the exclusive one refused |
| A lock key added inside a gap after the scan | The `CheckEmpty` over that gap fails | The request reads again; at most one exclusive holder |
| Size change, slot byte counter | Size from 0 to 3 bytes | `Update(fs/5/2:0/u, Add, 3)` in the same transaction, never compare-and-set, and `statfs` used is the sum of the slot counters |
| xattr `user.DOSATTRIB` on 2:3 | Put `fs/5/2:3/x/user.DOSATTRIB` | One scan returns it with the attributes |
| xattr of 64 KiB on 2:3 | The first piece and count under `x/{name}`, 65 pieces under the piece tag | One `Txn` of about 82 KiB; `getxattr` returns the 64 KiB |
| `case_index` on, `create` of `Photo.JPG` | Also put `fs/5/2:1/c/{folded}` → name | One extra key per create |

**Replay records**

| Input | Expected |
| --- | --- |
| `mkdir` with (session 7, mount 1, request 42) applied, then resent with the same IDs | The second returns the stored result (success), not `EEXIST`, and changes nothing |
| A `mkdir` that failed with `EEXIST`, resent | Returns `EEXIST`: a final failure is stored |
| A `rename` that failed with `ENOENT`, resent after the source appears | Returns `ENOENT`, and nothing moves |
| A resent `Txn` with an `Update` | Returns its first `Result`, including the `Update`'s value |
| Two mounts of volume 5 by session 7, each sending request 42 | Two records, `r/7/1/42` and `r/7/2/42`; neither answers the other |
| A request carrying "oldest outstanding" 50 | The shard removes that session and mount's records below 50 in that slot, with one range removal |
| Session 7 leaves the session table | Each shard's leader removes `r/7/` in every slot, in the background |
| A resend after its record was dropped | It runs again and returns whatever the state now gives. An honest oldest-outstanding value rules it out |
| [property] Every mutating op, resent 0 to 3 times at random | Each returns its first final result, and the final state equals the state without resends |

Replay records are kept as `fs/{volume}/{slot · 0}/r/{session}/{mount}/{request}`, in the slot the request changed.

**Invariants:**

- An inode ID is never reused; a counter reset is caught.
- No file's `mtime` goes backwards, except by `utimensat`.
- A file's keys and its directory's entry are in one slot, unless a cross-directory rename moved the entry, or the parent's slot had run out of IDs when the file was created.
- No directory is its own ancestor.

## MetaStore invariants

Simulator invariants checked after every step, with the faults that must exercise each.

| Invariant | Faults that exercise it |
| --- | --- |
| At most one leader per shard per term | Partitions; leader crashes; a node restarted within `primary_lease` |
| No two replicas of a shard serve lease reads at overlapping times | A quiescent leader's node partitioned from one follower; a leader's node suspended past its lease; a leader transfer while lease reads arrive; a configuration change while a lease is held; slewed clocks; VM pauses |
| A committed entry is never lost | Crashes before and after a round's flush; a torn tail record holding chunk writes and Raft entries |
| A shard recovered from manifest and log equals its state before the crash | Crashes during memtable flushes, compactions, checkpoints and cleaner passes (crash oracle) |
| At most one lock holder | Racing flock `Txn`s; owner failover; a session fence |
| No session is fenced while its client's own lease still runs | Control-plane leader failover while sessions renew; a renewal answered by a deposed leader; a leader stalled past `session_lease`; clients paused past their lease; slewed clocks; a client VM stopped with its clocks frozen |
| No lock reaches a new holder while a primary would still accept the fenced holder's writes | A fence that reaches some primaries late; a storage node partitioned during a fence |
| On every disk of a pool, (1 + k) times the grants outstanding plus the flushes not yet in fill reports never exceed the disk's gap above the larger of its fill and `write_limit`, and no flush under a grant is refused for space | The pool reaching `write_limit` while daemons buffer and other nodes write through; grant heartbeats lost or delayed, requests and replies; a control-plane failover while daemons stream; a client partitioned from the control-plane leader but not from storage, streaming under its grant; a daemon crash in the middle of a reuse window; a refill moving members onto the fullest disks while grants are out |
| A replayed request returns its first result and changes nothing | A daemon crash with requests read but unanswered, followed by their resend; an election while requests are in flight |
| An internal `Txn` repeated after an election changes nothing it did not read | An election while the purger and the control-plane leader have transactions in flight; a truncate merging into a purge entry the purger is consuming |

Each invariant is shown to fire: a planted bug for each must fail at least one seed. Open: the seed count; 20 is the suggestion, as the metamorphic planted bugs use. The bugs:

- two leaders allowed in one term;
- lease reads served by a follower;
- an entry acknowledged before its flush;
- replay skipping the last record;
- a flock `Txn` without its compare;
- a lock granted while the old holder's session is still listed;
- a fence committed at the holder's lease end without its margin;
- grants not capped by the gap;
- grants reusable for the whole session lease;
- a reuse window counted from the reply's receipt instead of the request's send;
- flushes reported since the last sent heartbeat instead of cumulatively;
- a shrink counted before a heartbeat echoes it;
- a floor grant that can be reused;
- a replay record ignored;
- a check failure that is not final stored as the request's result;
- a purger's `Delete` without its `Check`.

## Off-heap slabs

- Slabs are mapped with `unix.Mmap`, outside the Go heap, and every slot's address is 4 KiB-aligned
  (`O_DIRECT` and io_uring need it). A page is viewed as `*[4096]byte` by slice conversion, with no
  `unsafe`.
- Handles are a `uint32` slot plus a generation, as the simulator pool's. The Buffer pool section's
  scenarios (explicit capacity, PoolExhausted with requested and available, a reused slot with a new
  generation, the live-buffer limit, exhaustion as an ordinary outcome) pass on the slab pool too.
  `AllocZeroed` returns zeros; poison on `Alloc` is the simulator pool's only.
- Review criterion: a stale handle is caught. A freed handle, and a handle whose slot was reallocated
  (generation mismatch), are refused before any byte is read or written. Open: whether the slab pool
  panics, as the simulator pool does, or returns an error in release builds.
- Review criterion: the Go heap stays flat while slabs fill. Fill slabs to 1 GiB of buffers and
  memtable pages: `runtime/metrics` heap object bytes rise by no more than the handle tables, and a
  forced collection takes no longer than with empty slabs. Open: tolerance.
- The memtable's page store on slabs: every LSM tree and chunk index scenario passes with it. A full
  collection with 20 million entries in slabs takes under 1 ms (as in the LSM tree section), and
  collection takes 0.0% of CPU under request garbage (Go memtable spike).
- With slabs in place the tech debt "LSM pages and runs on the Go heap" meets its exit condition: the
  page store takes its slabs from them, runs are released when the last view holding them closes, and
  the entry is deleted in the same change.
- Slabs never hold a Go pointer: the types stored in them are byte arrays and integers (checked by a
  test over the slab's element types, or by review). Open: no tool enforces it.
- Growth: a slab that grows adds regions of at most 1 GiB to a sparse table, so an io_uring
  registration made earlier stays valid; a handle into an old region still reads the same bytes after
  growth.
- `GOMEMLIMIT` is set to the daemon's memory budget minus its mapped slabs, with `GOGC` at its
  default: a daemon with a 4 GiB budget and 3 GiB of slabs runs with a 1 GiB limit.
- Hot-path allocation and free allocate nothing (`b.ReportAllocs` 0).
- Open: what unmapping a slab with a live handle does; the design does not say.

## Event loop

Pass order, the read budget, `writev`, the inbox burst and the block decision run stepped, on
`looptest` (design.md, Testing strategy, Driver tests); 1,000 connections, goroutines, allocations, cgroup
sizing (in `docker run --cpus=2.5`) and the open-loop load run on the real kernel.

Review criteria, each a test:

- Stepped, seed 1-50: a run of 8 loops over socketpairs, run twice, gives the same results and the
  same event order per loop, and so does a run of 1 loop. Traces name connections by the IDs the
  loop assigns, not by descriptor number; once the loop and driver packages pass the loop rule set,
  the two runs' traces are `cmp`-equal.
- Stepped: a loop that calls `Wait` with `block` set while `Pending` reports work (a queued inbox
  item, a deferred submission, unspent read budget) fails the test at once.
- Real kernel: under 1,000 connections, the recorded trace of every pass shows one `epoll_wait`, at most one
  `writev` per ready descriptor, and no more than the read budget (256 KiB in the spike) read per
  descriptor. Example of a passing pass:

  ```text
  2000000 1 syscall pass=40 epoll_wait ready=2
  2000001 1 syscall pass=40 read fd=40 bytes=262144
  2000002 1 syscall pass=40 read fd=41 bytes=8192
  2000003 1 event pass=40 NetRecv peer=3 msg=write
  2000009 1 syscall pass=40 writev fd=40 iov=12 bytes=49536
  2000010 1 syscall pass=40 writev fd=41 iov=1 bytes=64
  2000011 1 syscall pass=41 epoll_wait ready=0
  ```

  A pass reading fd 40 past 256 KiB, or writing it twice, fails.
- `runtime.NumGoroutine` is the same with 1 and with 1,000 connections.
- Under a 2.5-CPU cgroup the daemon runs 2 loops with `GOMAXPROCS` 3, and `cpu.stat` shows no
  throttled periods at idle.
- `testing.AllocsPerRun` of a pass is 0, on the real kernel and on the stepped kernel.
- With the open-loop latency spike's load generator (16 connections, writes timed from their
  scheduled send), no request is dropped up to 100,000 writes/s, the rate the spike's bounded harness
  served (the generator's own limit), and output buffers stay under 20 MB.

Sizing from the cgroup (Go's default `GOMAXPROCS` is `min(CPUs, max(2, ceil(limit)))`):

| CPUs | Limit | Loops | `GOMAXPROCS` |
| --- | --- | --- | --- |
| 4 | none | 4 | 5, set by the daemon |
| 4 | 2.5 | 2 | 3, left to the runtime |
| 4 | 2 | 1 | 2, left to the runtime |
| 4 | 3 | 2 | 3, left to the runtime |
| 4 | 2.5, cgroup v1 `cpu.cfs_quota_us` 250000 over 100000 | 2 | 3 |
| 4 | 4 (equal to the CPU count) | 3 | 4, left to the runtime |
| 2 | 3 | 1 | 2, left to the runtime |

- The limit is read from `cpu.max` (cgroup v2) or `cpu.cfs_quota_us` (v1), never inferred from
  `runtime.GOMAXPROCS(0)` against `runtime.NumCPU()`, which misses a limit equal to the CPU count.
- A limit lowered while the daemon runs leaves it with no spare P, and it reports that until it
  restarts. Open: what it does when the limit is raised.
- The chart asks for an integer limit of at least 3 CPUs or none; a 2-CPU limit runs one loop, and a
  fractional limit is accepted. Open: whether the daemon reports a fractional limit.

Inbox, timers and syscalls:

- Other loops and worker goroutines hand work through a lock-free multi-producer queue and write its
  eventfd only when the queue was empty: a burst of 64 items from one producer costs one eventfd write
  and one wakeup (trace: one `write fd=<eventfd>`). Real kernel, under `-race`: producer goroutines
  post to a consumer loop that really blocks in `epoll_wait`, and every item is handled within a
  bound. Targets from the log per core spike: an inbox hop
  of one item about 13.6 µs of CPU, 8 items 0.73 µs and 64 items 0.07 µs per item. Open: tolerance.
- Timers live in a heap per loop with one timerfd armed for the earliest deadline: after any sequence
  of `SetTimer` actions, the timerfd holds the earliest deadline in the heap, armed with at most one
  `timerfd_settime` per pass and none when the earliest deadline did not change (Clock and timers).
- Non-blocking reads, writes and sends use `unix.RawSyscall`; `epoll_wait` uses `unix.Syscall`
  (checked by the recording layer, which tags each call). No `io_uring_enter` waits for completions.
- Loops are not locked to OS threads, except a loop driving a C library that requires its own thread.
- Every syscall the loop makes goes through the recording layer and appears in the trace; a test with
  the recording layer counting calls finds no syscall missing from the trace.
- Output buffers are rings or compacted in place, never grown by `append`; the bytes queued for
  replication are capped, and over the cap the loop stops reading clients. Open: the cap's value.
- A pass handles peer messages before client reads, so a lease holder that lost time ends its leases
  before it serves a read that waited behind the grantor's messages (VM pause spike).
- `tools/sansio`'s rule set for `internal/loop` and `internal/driver/…` finds no `time.Now`,
  `time.Since`, `time.Sleep`, `time.Timer`, `time.AfterFunc`, channel operation, `select` or `range`
  over a map; only the real kernel behind the seam reads a clock. A planted `time.Now` in a driver
  fails the lint.
- CPU per replicated 4 KiB write is benchmarked nightly against the codegen spike's tuned figure; a
  20% regression is a bug. Open: the figure on the CI host.

## Clock and timers

Each case runs in one tier (design.md, Testing strategy, Driver tests):

- **Stepped**, on `looptest`'s scripted clocks with the jump scripted inside a chosen pass's `Wait`:
  the slew, `Resumed`, VMClock-marker (each row of the table below as a scripted reading),
  never-decreasing and timer cases.
- **Real kernel**: read cost and allocations, the wake-up `Now`, no false `Resumed` under
  throttling, and the timerfd's clock and `timerfd_settime` record.
- **VM, nightly** (`spikes/vm-pause`'s QEMU and Firecracker drivers): the hypervisor rows of the
  table with the real marker, and the `date -s` step.

Reading the clocks:

- Each pass reads its clocks after its wait returns, together: `CLOCK_BOOTTIME` into `ctx.Now` and
  `CLOCK_MONOTONIC_RAW` into `ctx.Raw`, in ticks of 1 µs (nanoseconds ÷ 1,000), with the loop's own
  raw syscalls, between two `CLOCK_MONOTONIC` reads (Go's monotonic clock is `CLOCK_MONOTONIC`,
  neither of the lease clocks). Benchmark on the real kernel: each raw read costs about 140 ns and
  the clock read allocates nothing.
- The real `Clocks` reads `CLOCK_MONOTONIC`, `CLOCK_BOOTTIME`, `CLOCK_MONOTONIC_RAW`,
  `CLOCK_MONOTONIC`, and reads again when the two `CLOCK_MONOTONIC` readings lie more than ε apart.
  Real kernel: a million passes under `docker run --cpus=0.5` beside a CPU hog deliver no
  `Resumed`. Open: ε (design.md, open questions).
- Real kernel: a loop blocked 200 ms in `epoll_wait` and woken by a socket write hands that
  `NetRecv` a `Now` at least 200 ms after the previous pass's.
- Trace records carry the pass's clock reading as their tick; no record has a tick of its own.
- `ctx.Now` and `ctx.Raw` handed to machines never decrease within a run of the loop, even when the
  script feeds a reading below the last one. Open: what the loop does then (design.md, open
  questions). A wall-clock step of 1 s or 2 s (`date -s`, VM nightly) moves neither.
- A time daemon slewing `CLOCK_BOOTTIME` (tick 9000, −10%) moves `Now` against `Raw` while `Raw` keeps
  its rate; a 1 s timer then fires after 1.111 s of `Raw`, as the clock slew spike measured, and
  machines see both values.

`Resumed`:

- `core.Resumed(prev, cur, threshold)` reports a suspend when `CLOCK_BOOTTIME − CLOCK_MONOTONIC`
  grew by at least the threshold between two readings, which the kernel does only when it adds sleep
  time, and reports a marker change. The threshold lies above ε and at most 2 s. Open: its value
  within those bounds.
- **[property]** Over the clock model: every seeded slew of up to ±22.6% and drift of up to ±1%
  gives no `Resumed`; every suspend whose gain reaches the threshold gives exactly one; every marker
  change gives exactly one; a suspend the kernel rounded below the threshold gives none, in the loop
  and in the simulator alike.
- A 2 s suspend scripted inside a pass's `Wait` delivers one `Resumed` in that same pass, after its
  clock read and before any other event, its `NetRecv`s and its client reads included; every timer
  that fell due during the suspend fires in that pass, in deadline order.
- VMClock: where `/dev/vmclock0` exists the loop maps it and reads the marker each pass (0.8 ns in a
  guest, 2.8 ns from Go natively); where it does not (bare metal, or a host whose kernel lacks the
  driver), the marker reads 0 and the loop runs without it and reports so, never fails. Cases from
  the VM pause spike, with the expected loop behaviour:

  | Event | Marker | Expected |
  | --- | --- | --- |
  | QEMU `loadvm`, or an incoming live migration (1 ms downtime) | Changed | `Resumed` in the first pass after the guest runs |
  | QEMU live migration held 10 s at switchover | Changed; kvmclock counted the hold, the TSC did not | `Resumed` |
  | Firecracker snapshot restore | Changed | `Resumed` |
  | One snapshot restored twice | Changed in each restored guest | `Resumed` in each |
  | QEMU `stop` 10 s and `cont`, guest on `kvm-clock` | Unchanged, every clock frozen | No `Resumed`; the lease's step check on grantor messages must catch it (not this item) |
  | QEMU migration cancelled after 10 s, guest on `kvm-clock` | Unchanged, every clock frozen | No `Resumed`; as above |
  | QEMU `stop`/`cont`, guest on `tsc` (`+invtsc`), or a Firecracker pause | Unchanged; every clock gained 10 s | No `Resumed` from the marker; `Now` and `Raw` both jump |
  | `SIGSTOP` of QEMU 10 s | Unchanged; clocks kept time | No `Resumed` |
  | Suspend to RAM | n/a | `Resumed`; `Now` gained the sleep, `Raw` did not |

Timers:

- `SetTimer` arms the loop's timer heap, a pure function in `internal/core`; the timerfd is on
  `CLOCK_BOOTTIME`, so a timer that came due during a suspend fires after resume, and every overdue
  timer fires in deadline order. Open: tie-break between equal deadlines.
- `SetTimer` on an armed `TimerID` replaces its deadline; there is no cancel; a `Timer` event for a
  replaced deadline may still arrive and the loop does not filter it (machines check their own
  deadline).
- Timers fire from the heap, not from the timerfd's readiness: a pass whose `Wait` reported no
  timerfd still fires every timer due by `Now`, and `Wait` drains the timerfd's expiration count.
- A client read and a lease-expiry timer that become ready in the same `Wait` are handled after the
  expiry.
- `Arm` only when the earliest deadline changes: at most one `timerfd_settime` per pass, and none in
  a pass whose earliest deadline did not change.
- Real kernel: the timerfd is created on `CLOCK_BOOTTIME`, and each `timerfd_settime` record has
  `TFD_TIMER_ABSTIME` and an `it_value` equal to the heap's minimum. `timerfd_gettime` returns only
  the time remaining (`fs/timerfd.c`), so it is used as a bound: remaining ≤ deadline − a
  `CLOCK_BOOTTIME` reading taken just before. A real 10 ms timer wakes a blocked loop no earlier
  than its deadline.

## io_uring disk driver

- The driver maps its own ring per loop and drives it with raw syscalls (no liburing); the ring's
  completion eventfd sits in the loop's epoll set, and the driver runs in event mode only: it never
  waits inside `io_uring_enter` (trace: no `io_uring_enter` with `min_complete` above 0), drains a
  full completion queue within the pass and defers what a full submission queue cannot take.
- The ring is tested on the real kernel with a file-backed disk, natively or in a container whose
  seccomp profile allows `io_uring_*`: Docker's default profile fails `io_uring_setup` with
  `EPERM`, so those runs use `--security-opt seccomp=unconfined` or a profile that allows it.
- File or raw device, opened `O_DIRECT`; every read and write is block-aligned in offset, length and
  buffer address.
- Vectored writes: a `DiskWrite` with a `Vec` of pool buffers writes them in order as one contiguous
  write; with `Vec` empty it writes `Buf`. A vector whose lengths do not sum to `Len` is refused.
- A round's write and its `IORING_FSYNC_DATASYNC` emitted together are submitted linked
  (`IOSQE_IO_LINK`) in one `io_uring_enter`, with no wakeup between them (trace: one `io_uring_enter`
  submitting 2). A flush emitted later, unlinked, covers only writes that completed before it.
- `durability: fua` as a per-disk setting: writes carry FUA and no flush is issued; default `flush`.
  On the measured consumer drive FUA cost 2.2 ms per 4 KiB write against 0.95 ms for write plus
  flush, so `flush` stays the default.
- File-backed disks: segments are written in full when the file is created, so later writes are
  overwrites: 1,000 durable 4 KiB writes into the file have a p50 near 0.95 ms on the NVMe flush
  spike's drive, not the 2.5 ms of writes into never-written `fallocate`d extents, and the ext4
  journal's commit count does not rise across them.
- Discard for freed segments, three cases: on Linux 6.12 and later a `DiskTrim` is one
  `BLOCK_URING_CMD_DISCARD` ring command; on older kernels it is `BLKDISCARD` on a worker goroutine,
  whose completion reaches the loop through the inbox; on a file-backed disk the `DiskTrim` completes
  with no discard, no hole is punched, and the file stays written in full.
- Fixed buffers: the loop's slab is registered once (`IORING_REGISTER_BUFFERS`) in regions of at most
  1 GiB, at most 16,384 buffers per ring, and reads and writes use `READ_FIXED`/`WRITE_FIXED`; a slab
  that grows registers its new region in a sparse table without re-registering the old. The disk
  descriptors are registered as fixed files.
- Fallback: without `CAP_IPC_LOCK` and with the default 8 MiB memory-lock limit, registering 64 MiB
  fails with `ENOMEM`; the driver then issues plain `READ`/`WRITE` on the same buffers, every scenario
  of this section still passes, and the driver's status reports the fallback.
- Benchmarks, 4 KiB `O_DIRECT` on the NVMe flush spike's drive (registered buffers spike):

  | Case | Target |
  | --- | --- |
  | Fixed buffer, QD 32, read | −0.1 µs per I/O (1.88 → 1.77 µs), every rep |
  | Fixed buffer and file, QD 32, write | −0.1 µs (1.63 → 1.53 µs) |
  | Fixed buffer and file, QD 1 | −0.24 to −0.29 µs of 3.7 to 4.7 µs |
  | Registration | 17 to 70 ms per GiB already touched, once |

  Open: tolerance. Open: polled completions (`IORING_SETUP_IOPOLL`) are an open question, not built.
- Building submission entries from actions and decoding completions into `DiskDone` are pure
  functions, table-tested and fuzzed:
  - a completion with an error result becomes a `DiskDone` with that error, and the log fails;
  - a flush linked behind a failed write completes with `-ECANCELED`, and the log fails on the
    write's error, not the flush's;
  - a short count (fewer bytes than asked) is decoded as such. Open: how it is reported to the
    core; the design does not say.
- A flush error reported late, for a round a later flush already made durable, still reaches the
  log as an error.
- The driver contract: a buffer named in a `DiskRead` or `DiskWrite` stays with the driver until its
  `DiskDone`; a test frees nothing early and the slab's generation check finds no reuse in flight.
- Many reads in flight: recovery's segment-header reads (4,096 to 262,144 per disk) go out at the read
  depth recovery asks for, and the ring never holds more than its submission queue; a full queue
  defers submissions to the next pass, never drops them. Open: ring depth.
- Formatting a raw device discards every segment, or zeroes each segment header where a discard does
  not read back as zeros. A freed segment's discard does the same. Test: a device prefilled with random bytes and with another log's valid
  segment headers is formatted; the next recovery's trace reads only segment headers and block 1 of
  each, and reads no segment in full, at that start and every later one. Open: how the driver learns
  whether a discard reads back as zeros.

## TCP network driver

- One connection per pair of event loops, opened on demand and chosen by the owner map: a chunk
  write from loop A on node 1 to a disk loop B owns on node 2 travels A → B and its reply B → A, with
  no inbox hop on either side (trace: no inbox message for that request).
- A request that reaches the wrong loop, after a shard moved, is forwarded through the inbox and its
  reply names the new owner; the sender's next request goes to the new owner directly. The owner map
  is a hint: with a stale map every request still completes.
- A 10-node cluster of 8-loop nodes holds at most 576 sockets per node.
- Framing: hand-written encodings with a version; every frame decoder has a native fuzz test.
  Messages up to 256 bytes travel inline; bulk data travels in pool buffers. Open: whether frames
  carry a checksum, and whether a frame that fails to decode is a `PeerLost` or a corrupt message
  delivered as data.
- A received chunk write's blocks are read straight into 4 KiB-aligned pool buffers (Wire messages:
  whole-block data at block-aligned offsets of the frame): no copy and no allocation per message
  (`b.ReportAllocs` 0).
- Messages are batched per connection: everything ready for one connection in a pass goes in one
  `writev` (as under Event loop).
- Dial and listen are functions the driver is given, so stepped tests hand it AF_UNIX socketpairs
  and skip the TCP-only socket options; the TCP-specific cases below use real loopback.
- Any transport error (reset, EOF, a write error, a peer that stops reading past the driver's limit)
  becomes exactly one `PeerLost` for that peer; every outstanding request on the connection fails;
  the driver reconnects, and requests after the reconnect succeed. Each error is made on loopback: a
  reset by closing the peer with `SO_LINGER` 0, an EOF by `shutdown`, a peer that stops reading with
  a small receive buffer.
- Buffers (driver contract): a `NetSend`'s buffer is released before the peer's reply is delivered,
  and all of a peer's buffers are released before its `PeerLost` is emitted; a timeout releases
  nothing; one buffer sent to several peers is freed once every one is done. A test checks the slab's
  live count is zero after a fan-out to three peers where one is lost.
- A peer answers every request that carries data, even to reject it.
- The core keeps its own deadlines: the driver sets no transport timeout that fails a request on its
  own. Open: whether the driver uses keepalive or `TCP_USER_TIMEOUT`.
- The read budget per connection per pass and the replication queue cap apply as under Event loop.
- TCP first: RDMA upgrades the same connection's peers in phase 9; nodes without rdma-core stay on TCP
  with no error.

## Round driver

Review criteria, as stepped trace checks on `looptest`, each run with 1, 2, 4 and 8 producer loops;
the latency measurement below runs on real loop goroutines:

- At most one inbox message per producer loop per disk per round in each direction: once per pass a
  loop hands each disk's owner one message with its shards' `Ready` records and its chunk writes for
  that disk, and the owner hands back one durable notice per round. Example for producer loop 0 and
  owner loop 3:

  ```text
  3000100 0 inbox pass=210 send to=3 disk=1 records=12 chunk_writes=3
  3000100 0 syscall pass=210 write fd=<eventfd of loop 3>
  3000105 3 action pass=88 DiskWrite round=51 io=88 lba=4160 len=49152
  3000105 3 action pass=88 DiskFsync round=51 io=89 linked=1
  3000990 3 event pass=89 DiskDone io=88 err=ok
  3001060 3 event pass=90 DiskDone io=89 err=ok
  3001061 3 inbox pass=90 send to=0 disk=1 durable_lsn=812
  ```

  A second `inbox send to=3 disk=1` from loop 0 in the same round fails.
- One `DiskFsync` per round per disk, whatever the number of producer loops (1, 2, 4, 8).
- Records from 8 producer loops land in one record (the log per core spike wrote 39 items per flush
  with 8 producers, 8 with a log per loop). Chunk writes from producer loops ride the same inbox
  message as their loop's `Ready` records and land in the same round's record.
- A shard's replayed entries reach the shard's loop after recovery, which runs on the owner; the
  cleaner's moves for a shard's runs reach the shard's loop; a checkpoint asks each index's loop for
  its runs.
- A shard on the disk's own loop skips both inbox hops.

Collection:

- A round with 1,000 idle shards and one active shard calls `HasReady` on one shard; a pass's CPU
  with 1,000 idle shards does not depend on the shard count (compare 10, 100 and 1,000 idle shards).
  Scanning every shard cost 54% of loop CPU at 0.1 ms flushes in the raft rounds spike.
- One entry per shard per round, holding every proposal queued for it.
- A round is bounded by `max_group_commit_bytes` (1 MiB) only, with no time bound: a round that
  reaches it leaves the rest for the next round in submit order, and every shard is collected within
  two rounds. Under open load rounds stay far below the cap (at most 574 KiB at 130,000 proposals/s on
  one loop).

Latency, against the raft rounds spike (3 nodes × 1,000 groups, 128 B proposals, 4 loops per node):

| Flush | Spike p50 | Spike p99 | Checklist target |
| --- | --- | --- | --- |
| 0.95 ms | 2.9 to 3.6 ms | 4.6 to 5.8 ms | 3 ms / 5 ms |
| 0.1 ms | 0.5 to 0.7 ms | 0.9 to 3.6 ms | (not stated) |

Open: tolerance; the offered load (the spike ran 30,000 to 130,000 proposals/s); and whether the
measurement needs the Raft shard of phase 4 or a stand-in shard.

## Shared ChunkStore suite

- One suite, run unchanged on the simulated disk and on io_uring (a file-backed disk always; a raw
  device where available); the driver is the suite's only parameter. Done-when checkpoint: the same
  suite later passes on SPDK (phase 9).
- Its cases are the Single-node ChunkStore item's behaviours, checked against a byte-range model:
  `Write`, `Punch`, `Read`, `Flush` with versions; `expectVer` compare-and-set that succeeds at the
  current version and fails at a stale one; `atVer` reads of an older version; tombstones after a
  whole-chunk punch; unaligned writes of 1 byte to 3 blocks, pipelined to one block, merged at the
  newest ordered version; a whole 4 MiB chunk write split into several items of one version and
  acknowledged once all are durable; a snapshot cut, then reads at that epoch; a chunk's first write
  after a cut with its absent freeze.
- Restart: a clean close and reopen recovers every acknowledged write, on both drivers; a checkpoint
  then replay gives the same reads as before.
- The suite runs stepped on `looptest`. On io_uring the results equal the model and the counts per
  pass stay within their bounds, but whole traces are not compared: pass boundaries differ, so
  interleavings may.
- Fault injection (crash landing sets, injected errors, bit flips) runs only on the simulated disk;
  the suite marks those cases simulator-only rather than skipping them silently.
- Open: the design names the suite but not its case list; the list above is the Single-node
  ChunkStore item's.

## Shared network suite

- One suite, run on the simulated network and on TCP, structured so that RDMA joins it in phase 9
  (done-when: RDMA and TCP pass the same suite, and a mixed 3-node cluster negotiates per pair).
- Cases, the driver contract's network rules: a request and its reply between two loops on two
  nodes; a fan-out of one buffer to three peers, freed once all are done; a peer lost mid-request
  produces one `PeerLost`, fails every outstanding request to it, and releases every buffer before
  the `PeerLost`; reconnect, then success; a timeout keeps the buffer until the reply or `PeerLost`;
  a 256-byte message inline and a 257-byte payload in a pool buffer; a request to the wrong loop
  forwarded and answered with the new owner.
- On TCP the suite runs stepped on `looptest`, with unsent bytes (`SIOCOUTQ` above 0) counted as in
  flight so no deadline fires while a segment crosses loopback. It compares results with the model
  and checks counts, not whole traces.
- Open: whether the suite may assume per-connection order. The simulated network reorders, and the
  checklist's Replica primary item sends "a chunk's writes … to every member in version order and
  again in that order after a reconnect", which relies on per-connection order.

## Durable write benchmark

- 4 KiB writes at queue depth 1, each durable (write plus flush) before the next, through the full
  path: event loop, disk log, io_uring, on a raw device or a file-backed disk whose segments were
  written in full.
- Targets on the NVMe flush spike's drive (UMIS RPEYJ512MML1QWQ, no power-loss protection):

  | Case | Target |
  | --- | --- |
  | Back to back | p50 about 0.95 ms, p99 about 2.4 ms, about 1,090 durable writes/s |
  | After 10 ms idle | about 1.2 ms |
  | After 100 ms to 3 s idle | 1.9 to 2.1 ms |
  | Software share | about 20 µs above the device's time (0.9 ms of 0.95 ms is the device) |

  Open: tolerance.
- Cross-check with fio on the same drive and the same file or device: `fio --rw=randwrite --bs=4k
  --direct=1 --iodepth=1 --fdatasync=1` gave 1,038 IOPS (write p50 338 µs + sync p50 221 µs) in the
  spike. On another drive, the benchmark is compared with fio on that drive, not with the numbers
  above. Open: tolerance against fio.
- Idle gaps are part of every run, since a drive idle for 100 ms adds about 1 ms to the next durable
  write.
- Reports CPU per write. Open: no target is given.
- Reports the recovery time of a full disk's segment headers (4,096 headers at 256 GiB, 262,144 at
  16 TiB), with many reads in flight. Open: no target is given.
- Runs nightly; a 20% regression is a bug. The hot path reports zero allocations per operation.

## Crash tests on real disks

- `kill -9` of the daemon mid-round: the test driver, a separate process, sends writes and records
  in its own file each acknowledgment and every write in flight (the crash oracle, as RocksDB's crash
  test keeps its expected state and trace outside the process it kills), then kills the daemon at a
  random point while a round is in flight, restarts it and checks the recovery against the file.
- Every restart recovers a prefix of what was submitted that holds every acknowledged write; the
  checks are the Crash oracle's.
- A `kill -9` leaves the drive's write cache intact, so it tests in-flight rounds, not lost cached
  writes; device-mapper targets cover the rest, where root is available: an error target failing
  writes or flushes at a chosen point (the log fails, no later acknowledgment, recovery is a prefix),
  and a delay target slowing the device (nothing fails, latency grows). Open: which targets
  (`dm-flakey` with `drop_writes`, `dm-error`, `dm-delay`) and settings the test uses.
- Runs on a file-backed disk without root in CI; device-mapper cases only where root is available.
- Repeated: many kill cycles per run, each with a new kill point drawn from a seed the test prints,
  so a failing cycle is repeated with the same kill point (the kill's timing still varies).
- Power cuts are not this item: they come with the nightly real-cluster tests of phase 6.
