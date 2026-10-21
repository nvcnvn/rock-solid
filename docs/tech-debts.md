# Tech debts

## Version pin: Go toolchain 1.27.0 and lint tools
- **Where:** `toolchain` in `go.mod`; the `staticcheck`, `govulncheck` and `go-licenses` versions in `scripts/lint.sh`
- **Why:** the runtime behaviour the [Runtime in Go](design.md#runtime-in-go) rules are measured against (scheduler, collector) changes between releases, and each linter release adds checks, so unpinned versions would change performance or break the build without any code change.
- **Remove when:** never removed, only bumped. Bump the toolchain and the tools together, fix new findings and rerun the hot-path benchmarks in the same change.

## Version pin: `github.com/zeebo/xxh3` v1.1.0
- **Where:** `go.mod`; `ChunkID.Hash` in `internal/core/ids.go`
- **Why:** the PG hash is disk format, and XXH3-64 is not in the standard library. The module is pure Go with assembly for amd64 and matches the reference XXH3 output; its one indirect dependency, `klauspost/cpuid`, selects the assembly path.
- **Remove when:** never removed, only bumped, and only after `TestChunkIDGoldenVectors` passes on the new version, since a hash change would move every chunk.

## Version pin: `github.com/cockroachdb/datadriven` v1.0.2
- **Where:** `go.mod`; the data-driven tests and their `testdata/` files in `internal/lsm`, `internal/chunkstore` and `internal/disklog`
- **Why:** the file format and the `-rewrite` output are what the checked-in expected outputs are compared against, so an unpinned release could change how a file parses or prints and fail every case without a code change. It is the format Pebble and etcd-raft use ([Testing strategy](design.md#testing-strategy)).
- **Remove when:** never removed, only bumped. A bump reruns every data-driven test without `-rewrite`, and any changed expected output is reviewed as a format change, not regenerated.

## Workaround: LSM pages and runs on the Go heap
- **Where:** `pages` in `internal/lsm/page.go`; the blocks of `run` in `internal/lsm/run.go`
- **Why:** memtable pages and runs belong in slabs mapped outside the Go heap ([Runtime in Go](design.md#runtime-in-go)), which the event loop's off-heap slabs provide, and those do not exist yet. Until then they are pointer-free Go slices: the collector does not scan them (0.24 ms per collection at 20 million entries) but counts them toward its heap goal, and runs are freed by the collector rather than by hand once no view holds them.
- **Remove when:** the off-heap slabs of `internal/loop` exist; the page store takes its slabs from them and runs are released when the last view holding them closes.

## Version pin: TLC 1.7.4
- **Where:** the TLC jar the `models/` CI job downloads, by checksum
- **Why:** a TLC release can change what a configuration explores or how fast, so an unpinned checker would move every model's state counts and `ci` times without a spec change ([Testing strategy](design.md#testing-strategy)). The 1.8 line is a pre-release whose jar is rebuilt in place, so the models use no CommunityModules operator that needs it ([protocol models study](../studies/protocol-models/README.md)).
- **Remove when:** never removed, only bumped. A bump reruns every `deep` configuration, which the nightly job does once the TLC version in its hash changes, and records the new state counts in the model-to-simulator map.

## Workaround: writes larger than one record copied
- **Where:** `writeItems` in `internal/chunkstore/store.go`
- **Why:** a write larger than one record goes to the log as several items, each needing a pool buffer of exactly its blocks, and a pool buffer cannot be split, so the store copies such a write into one buffer per item although [chunk blocks are never copied](design.md#on-disk-format) otherwise. A whole chunk on a disk with [chunk slots](design.md#on-disk-format) is one item and is not copied; a larger partial write is, and so is a whole chunk on a disk whose segments are smaller than a chunk.
- **Remove when:** the frontends' receive path hands a larger write as item-sized buffers.

## Workaround: writes waiting for room fail at once
- **Where:** `housekeep` in `internal/chunkstore/store.go`
- **Why:** with no cleaner, nothing frees a segment, so the store calls `FailStalled` as soon as group commit holds items for room, where the design fails them only once the cleaner has no victim ([Writers slow down](design.md#on-disk-format)).
- **Remove when:** the Cleaner item lands and the store calls `FailStalled` only when the cleaner reports no victim.

## Workaround: an index rebuild restarts the whole log
- **Where:** `indexBroken` and `drain` in `internal/chunkstore/store.go`
- **Why:** a rebuild replays the log through recovery, which only runs on a log that is not appending, so the store stops, closes its log and recovers it in rebuild mode, where the design rebuilds one index while the disk's other indexes serve ([The LSM tree](design.md#on-disk-format)). The chunk index is a disk's only index until MetaStore shards share it.
- **Remove when:** a second index shares a disk (Raft storage in the disk log) and the log can replay its records to one index while it keeps appending.
