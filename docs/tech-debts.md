# Tech debts

## Version pin: Go toolchain 1.27.0 and lint tools
- **Where:** `toolchain` in `go.mod`; the `staticcheck`, `govulncheck` and `go-licenses` versions in `scripts/lint.sh`
- **Why:** the runtime behaviour the [Runtime in Go](design.md#runtime-in-go) rules are measured against (scheduler, collector) changes between releases, and each linter release adds checks, so unpinned versions would change performance or break the build without any code change.
- **Remove when:** never removed, only bumped. Bump the toolchain and the tools together, fix new findings and rerun the hot-path benchmarks in the same change.

## Version pin: `github.com/zeebo/xxh3` v1.1.0
- **Where:** `go.mod`; `ChunkID.Hash` in `internal/core/ids.go`
- **Why:** the PG hash is disk format, and XXH3-64 is not in the standard library. The module is pure Go with assembly for amd64 and matches the reference XXH3 output; its one indirect dependency, `klauspost/cpuid`, selects the assembly path.
- **Remove when:** never removed, only bumped, and only after `TestChunkIDGoldenVectors` passes on the new version, since a hash change would move every chunk.

## Workaround: LSM pages and runs on the Go heap
- **Where:** `pages` in `internal/lsm/page.go`; the blocks of `run` in `internal/lsm/run.go`
- **Why:** memtable pages and runs belong in slabs mapped outside the Go heap ([Runtime in Go](design.md#runtime-in-go)), which the event loop's off-heap slabs provide, and those do not exist yet. Until then they are pointer-free Go slices: the collector does not scan them (0.24 ms per collection at 20 million entries) but counts them toward its heap goal, and runs are freed by the collector rather than by hand once no view holds them.
- **Remove when:** the off-heap slabs of `internal/loop` exist; the page store takes its slabs from them and runs are released when the last view holding them closes.

## Version pin: TLC 1.7.4
- **Where:** the TLC jar the `models/` CI job downloads, by checksum
- **Why:** a TLC release can change what a configuration explores or how fast, so an unpinned checker would move every model's state counts and `ci` times without a spec change ([Testing strategy](design.md#testing-strategy)). The 1.8 line is a pre-release whose jar is rebuilt in place, so the models use no CommunityModules operator that needs it ([protocol models study](../studies/protocol-models/README.md)).
- **Remove when:** never removed, only bumped. A bump reruns every `deep` configuration, which the nightly job does once the TLC version in its hash changes, and records the new state counts in the model-to-simulator map.
