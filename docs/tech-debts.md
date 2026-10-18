# Tech debts

## Version pin: Go toolchain 1.27.0 and lint tools
- **Where:** `toolchain` in `go.mod`; the `staticcheck`, `govulncheck` and `go-licenses` versions in `scripts/lint.sh`
- **Why:** the runtime behaviour the [Runtime in Go](design.md#runtime-in-go) rules are measured against (scheduler, collector) changes between releases, and each linter release adds checks, so unpinned versions would change performance or break the build without any code change.
- **Remove when:** never removed, only bumped. Bump the toolchain and the tools together, fix new findings and rerun the hot-path benchmarks in the same change.

## Version pin: `github.com/zeebo/xxh3` v1.1.0
- **Where:** `go.mod`; `ChunkID.Hash` in `internal/core/ids.go`
- **Why:** the PG hash is disk format, and XXH3-64 is not in the standard library. The module is pure Go with assembly for amd64 and matches the reference XXH3 output; its one indirect dependency, `klauspost/cpuid`, selects the assembly path.
- **Remove when:** never removed, only bumped, and only after `TestChunkIDGoldenVectors` passes on the new version, since a hash change would move every chunk.

## Workaround: free segments found from their headers
- **Where:** `classify` and `endReplay` in `internal/disklog/recovery.go`
- **Why:** manifests carry no segment usage table yet, so recovery calls a segment free when its header reads as zeros or as another log's, or when a damaged header covers no record of this log, which [Free space](design.md#on-disk-format) says a segment must never be freed for. It is safe while nothing frees a used segment, since no cleaner runs yet.
- **Remove when:** the Segment usage table item of the [build checklist](build-checklist.md) lands: recovery takes free segments from the newest manifest's table and checks only the segments opened after it.
