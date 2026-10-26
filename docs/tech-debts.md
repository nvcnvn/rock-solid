# Tech debts

## Version pin: Go toolchain 1.27.0 and lint tools
- **Where:** `toolchain` in `go.mod`, which CI's `actions/setup-go` also installs; the `staticcheck`, `govulncheck` and `go-licenses` versions in `scripts/lint.sh`
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
- **Where:** `version` and `sha256` in `scripts/models.sh`, which downloads the TLC jar into `~/.cache/rock-solid` (`$XDG_CACHE_HOME` when set) and checks it against the checksum
- **Why:** a TLC release can change what a configuration explores or how fast, so an unpinned checker would move every model's state counts and `ci` times without a spec change ([Testing strategy](design.md#testing-strategy)). The 1.8 line is a pre-release whose jar is rebuilt in place, so the models use no CommunityModules operator that needs it ([protocol models study](../studies/protocol-models/README.md)).
- **Remove when:** never removed, only bumped. A bump reruns every `ci` and negative configuration (`scripts/models.sh ci`, which CI runs on the change) and every `deep` one (the next nightly CI run, since the pin is part of each configuration's hash), and records the new state counts in the [model-to-simulator map](test-scenarios.md#model-to-simulator-map).

## Workaround: writes larger than one record copied
- **Where:** `writeItems` in `internal/chunkstore/store.go`
- **Why:** a write larger than one record goes to the log as several items, each needing a pool buffer of exactly its blocks, and a pool buffer cannot be split, so the store copies such a write into one buffer per item although [chunk blocks are never copied](design.md#on-disk-format) otherwise. A whole chunk on a disk with [chunk slots](design.md#on-disk-format) is one item and is not copied; a larger partial write is, and so is a whole chunk on a disk whose segments are smaller than a chunk.
- **Remove when:** the frontends' receive path hands a larger write as item-sized buffers.

## Workaround: an index rebuild restarts the whole log
- **Where:** `indexBroken` and `drain` in `internal/chunkstore/store.go`
- **Why:** a rebuild reloads the runs the newest named manifest lists and replays the log after them through recovery, which only runs on a log that is not appending, so the store stops, closes its log and recovers it again, where the design rebuilds one index while the disk's other indexes serve ([The LSM tree](design.md#on-disk-format)). The chunk index is a disk's only index until MetaStore shards share it.
- **Remove when:** a second index shares a disk (Raft storage in the disk log) and the log can replay its records to one index while it keeps appending.

## Version pin: GitHub Actions and Java 17 in CI
- **Where:** each `uses:` in `.github/workflows/go.yml` and `models.yml`, pinned to a commit with its release in a trailing comment; `java-version` of `actions/setup-java` (Temurin 17) in `models.yml`
- **Why:** a release tag can be moved to other code, so a tag would let an action's owner change what runs with this repository's token; a commit cannot change. TLC's speed depends on the JVM, and the 300 s `ci` budget and the [model-to-simulator map](test-scenarios.md#model-to-simulator-map)'s wall times were measured on Java 17, which TLC 1.7.4 runs on.
- **Remove when:** never removed, only bumped. Bump an action to a release at least two weeks old, and Java only with a `ci` run of every model whose times are compared with the map's.

## Workaround: `go test -p 3` in CI
- **Where:** the `test` job in `.github/workflows/go.yml`
- **Why:** `internal/chunkstore`'s tests peak near 4.3 GB resident ([invariant checker](test-scenarios.md#invariant-checker)), and Go's default runs as many packages at once as the runner has CPUs, 4, so three at a time keeps a runner's 16 GB clear of the heaviest packages running together.
- **Remove when:** the heaviest test package peaks under 3 GB, or the runner has 32 GB.

## Workaround: a primary's whole-chunk writes skip chunk slots
- **Where:** `writeItems` in `internal/chunkstore/store.go`, for gated writes
- **Why:** a primary's write stays unmapped until it commits and may be dropped by peering; a dropped write in a chunk slot would leave the slot held by the log while no map refers to it, and `CheckSlots` requires the two to match. So a primary logs a whole chunk as ordinary items, as many records as `max_group_commit_bytes` takes, while its members, which map as they log, still write it to a slot.
- **Remove when:** the store frees the slot of a dropped write, as it frees a slot a later write replaces; then gated whole-chunk writes take a slot like any other.

## Workaround: a store keeps every client session's newest incarnation
- **Where:** `sessions` and `SeeIncarnation` in `internal/chunkstore/store.go`
- **Why:** a store fences an older incarnation of a client session by the newest it has seen of that session, and phase 3 has no record of when a session ends, so the map keeps one entry for every session that ever wrote to the disk and grows without bound.
- **Remove when:** phase 4's session table tells a replica that a session ended, and the store drops that session's entry.

## Workaround: messages for a newer table parked without bound
- **Where:** `park` and `parked` in `internal/chunkstore/replica.go`
- **Why:** a replica keeps a message tagged with a newer table epoch than its own, with its pool buffer, until it adopts a table at least that new, and nothing bounds how many it keeps, so a client or peer naming an epoch the control plane never publishes holds pool buffers until a table that new arrives.
- **Remove when:** a table fetch from the control plane answers with its newest epoch; the replica then refuses a parked message once the newest table is older than the epoch it names, and refuses messages past a bound.

## Workaround: a primary trusts a flush's granted flag
- **Where:** `clientUpdate` in `internal/chunkstore/replica.go`
- **Why:** a client marks a flush under a space grant in the update's flags, and `write_limit` admission lets it through on that mark alone, since phase 3 has no record of which client holds which grant; a client that sets the flag wrongly can fill a disk past its `write_limit` ([Admission against `write_limit`](design.md#data-layer-chunkstore)).
- **Remove when:** phase 4's control-plane group gives the primary each session's outstanding grants, and the primary admits a granted flush only within its session's grant.

## Workaround: leases per loop pair
- **Where:** `internal/chunkstore/replica_lease.go`
- **Why:** each disk's replica holds and grants leases on its own loop, keyed by the peer loop, while the design grants them per node pair so one grant covers every PG two nodes share ([Reads](design.md#epoch-changes-and-failure-detection)); phase 3's simulator runs one disk loop per node, where the two are the same, and no node loop exists yet to own the pair's leases.
- **Remove when:** a node runs several disk loops (the phase 5 drivers); its leases then move to one per node pair, shared by its loops.

## Workaround: partial writes held on a lacking chunk without bound
- **Where:** `lead.held` in `internal/chunkstore/replica.go`
- **Why:** a primary holds a partial-block write to a chunk whose kept version it lacks ([Which versions survive](design.md#epoch-changes-and-failure-detection)) until it holds that version, and only recovery brings it; with no recovery yet, every write a client resends after its timeout joins the list and pins a pool buffer.
- **Remove when:** Recovery per PG releases the held writes as it recovers the chunk and the primary refuses a partial write to a lacking chunk once a bound of held writes is reached.

## Workaround: fail-slow judged on reported latencies, not in a simulated cluster
- **Where:** `TestAFailSlowDiskIsMarkedDown` in `internal/chunkstore/detector_test.go`, which feeds the detector the latencies primaries would report; `TestTheDetectorMarksAFailedDiskDown` leaves fail-slow out
- **Why:** the simulator's scheduler keeps one clock and moves it to an action's end when it takes the completion, so a disk slowed 100× delays the completions in flight on the other disks as much as its own, and no disk looks slow beside its pool.
- **Remove when:** the scheduler times each disk's completions on that disk alone; then a seeded cluster run slows one disk 100× and expects it marked down after `slow_disk_window`.
