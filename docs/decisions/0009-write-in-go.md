# 0009. Write rock-solid in Go

Status: Accepted (2026-10-18)

## Context
rock-solid was designed in Rust: a sans-IO core of state machines, drivers that execute their actions, and etcd-raft ported to Rust for the Raft shard ([ADR 0007](0007-port-etcd-raft.md)). Phase 1 had produced about 6,100 lines of Rust (the disk log, group commit, recovery, the stage 1 LSM tree and the chunk index). The project is easier to read, review and extend in Go, which is also the language of etcd-raft, the CSI tooling, go-fuse, Porcupine and cilium/ebpf. A series of spikes on one 4-vCPU cloud VM, with Go 1.27.0 and Rust 1.99.1, measured what Go costs:

| Question | Measurement |
| --- | --- |
| CPU per replicated 4K write, same epoll design ([tuned Go spike](../design.md#appendix-tuned-go-spike)) | Go 1.21× Rust; 1.5 to 1.7× with a goroutine per connection, whatever its synchronization |
| Where that comes from ([codegen spike](../design.md#appendix-go-and-rust-codegen-spike)) | Not the compiler: the generated code costs about the same per write. About 61% is runtime scheduling (`sysmon` retaking the P of a loop blocked in `epoll_wait`), 16% syscall wrappers |
| After tuning the runtime (codegen spike) | **Go 1.07× Rust** with `GOMAXPROCS` above the blocking loops, raw syscalls for non-blocking I/O, and memory touched before use |
| Compute-bound code ([Go CPU spike](../design.md#appendix-go-cpu-per-operation-spike), [tuned Go spike](../design.md#appendix-tuned-go-spike)) | Go 1.1 to 1.25× Rust; PGO, `GOAMD64=v3` and bounds checks each under 3.5% |
| Rising open-loop load ([open-loop latency spike](../design.md#appendix-open-loop-latency-spike)) | Same throughput up to 100,000 writes/s; Go's p99 knee 1.2× earlier (measured before the runtime tuning above) |
| Garbage collector ([Go memtable spike](../design.md#appendix-go-memtable-spike), [tuned Go spike](../design.md#appendix-tuned-go-spike)) | Pauses under 1 ms. A memtable with a pointer per entry costs 23 to 35% of CPU in collection; in a Go arena 9%; as pointer-free 4 KiB pages 0% (0.3 ms per cycle at 20 million entries) |
| CRC32C (Go CPU spike) | Go `hash/crc32` 167 ns per 4 KiB, against 1.2 µs for the table-driven Rust crate the phase 1 code used |

The rules below are what make those numbers hold; without them the measured cost is 1.5 to 1.7× the CPU and a quarter of it in collection.

## Decision
rock-solid is written in Go, starting again from the design rather than porting the Rust code. The toolchain is pinned in `go.mod` (`go 1.27`, `toolchain go1.27.0`). The sans-IO design, the one log per disk and every other decision stay.

The rules apply to the data path: the sans-IO packages, the event loops, the drivers and the frontends' request handling. Code off the data path (the CSI driver's gRPC server, metrics, the admin API, the FUSE descriptor holder) is ordinary Go with goroutines, `net/http` and `context`. Each rule names the measurement or constraint behind it.

**Event loops**
1. **One event loop per core**, a goroutine running `epoll` through `golang.org/x/sys/unix` on raw non-blocking descriptors. It owns its shards, the indexes of its disks and its peer connections outright. Never a goroutine per connection, and never `net.Conn` on the data path: 1.21× against 1.5 to 1.7× Rust's CPU.
2. **`GOMAXPROCS` above the number of loops that block in `epoll_wait`.** With as many busy loops as Ps, `sysmon` retakes a blocked loop's P (`runtime/proc.go`), costing futex wakeups and CPU migrations: −2.9 µs per write, futex calls from 0.21 to 0.02 per write. The daemon sets it at start from its loop count and the container's CPU limit.
3. **`unix.RawSyscall` for reads, writes and sends on non-blocking descriptors**, which never block; `unix.Syscall` for `epoll_wait`, `io_uring_enter` with a wait, and anything else that can block: −1.4 µs per write.
4. **Loops are not locked to OS threads** (35 against 29 µs per write), except where a C library requires its own thread, as SPDK's `spdk_thread` does.
5. **A loop waits only on its epoll set.** A goroutine blocked in `epoll_wait` cannot also wait on a channel, so everything that can wake a loop is a descriptor: its sockets, its io_uring completion eventfd, one timerfd armed for the earliest deadline in the loop's timer heap, and one eventfd for its inbox. Other loops and worker goroutines hand it work through a lock-free multi-producer queue and write the eventfd only when the queue was empty, so a burst costs one wakeup. No channel, `select`, `time.Timer` or `time.AfterFunc` on the data path.
6. **Every pass does bounded work:** a read budget per descriptor per pass (256 KiB), a cap on queued replication bytes that stops reading clients while over it, and output buffers that are rings or compacted in place. A slice grown by `append` copies itself (`runtime/slice.go`), and a loop that drains every socket collapsed into multi-second stalls from 30,000 writes/s.
7. **One write per descriptor per pass**, gathering every frame ready for it with `writev`: syscalls set the cost of a write, and batching halved Rust's and cut Go's by 29%.
8. **Disk I/O goes through io_uring**, on a ring the driver maps and drives with raw syscalls, since Go's standard library has none. The ring's completion eventfd sits in the loop's epoll set.

**State machines**
9. **`Event` and `Action` are tagged structs, not interfaces**, and `OnEvent` appends its actions to a slice the caller owns and reuses, so a call allocates nothing. Hot-path structs are filled in place, not built as temporaries and copied: a frame literal copied into a slice cost 534 ns, filled in place 177 ns.
10. **Determinism in the sans-IO packages:** no `range` over a map (Go randomizes iteration on purpose, `internal/runtime/maps/table.go`; iterate an ordered structure or `slices.Sorted(maps.Keys(m))`); randomness only from a seeded `math/rand/v2` source (PCG or ChaCha8) owned by the machine; no clock, `go` statement, channel, `select`, `sync` or `sync/atomic`, and no `os`, `net`, `syscall` or `unsafe`. A `go/analysis` pass run by `go vet` enforces this. Driver tests use `testing/synctest` for fake time.
11. **Encodings are hand-written** over `encoding/binary`'s `Append*` and `PutUint*` functions into buffers the caller provides, with no reflection (`encoding/gob`, `encoding/json`, `binary.Read`/`Write`) on the data path. Every decoder has a native fuzz test (`go test -fuzz`).

**Memory**
12. **Long-lived bulk structures hold no pointers.** The memtable is a copy-on-write B+-tree of 4 KiB pages, pages referring to each other by `uint32` number; the chunk index and similar tables are slices of integers. Collection then costs 0.3 ms at 20 million entries instead of 0.66 CPU-seconds per cycle, and a snapshot is the root page number at the freeze. Restored pages are 98% full (175 B per file); pages filled in create order are 63% full, so the tree redistributes into a neighbour before splitting.
13. **Bulk memory lives outside the Go heap.** Memtable pages, index tables, retained Raft entries and I/O buffers are slabs mapped with `unix.Mmap` and held as `[]byte`; a page is viewed as `*[4096]byte` by slice conversion, without `unsafe`. Three reasons: the collector's heap goal is `GOGC` percent above the live heap, pointer-free memory included, so 16 GiB of memtable on the heap would let the heap grow toward 32 GiB before a cycle; `O_DIRECT` and io_uring need page-aligned buffers, which the Go heap does not promise; and a mapped slab is registered once with io_uring (`IORING_REGISTER_BUFFERS`), SPDK or RDMA. Off-heap memory never holds a Go pointer, since the collector does not scan it. SPDK and RDMA slabs are allocated by their C libraries instead, which cgo's pointer rules allow.
14. **I/O buffers are handles**, a `uint32` slot plus a generation into the driver's slab, never `sync.Pool`, which empties at collections, has no bound and cannot be registered for DMA. `sync.Pool` is for scratch off the data path.
15. **No Go arenas.** `GOEXPERIMENT=arenas` is on hold upstream, and the layouts above already cost nothing to collect: pages took 0.0% of CPU in collection against 9.3% for a memtable in an arena.
16. **`GOMEMLIMIT` budgets the Go heap**, which is the daemon's memory budget minus its mapped slabs, with `GOGC` left at its default. Hot-path benchmarks report allocations (`b.ReportAllocs`), and CI fails one that allocates per operation.

**Concurrency outside the loops**
17. **Read-mostly tables** (the placement table, fenced sessions) are immutable values published through `atomic.Pointer`; readers load the current one without locks, and the collector frees old ones.
18. **Latency paths use typed atomics and fixed slot tables** instead of a mutex, a map and a channel: queue-depth-1 p50 67 against 84 µs. Counters written by different cores are padded to a cache line.
19. **CPU-heavy background work runs on worker goroutines** that hand results to the owning loop through its inbox (rule 5), as the cleaner does: its copy work costs Go 1.12 to 1.24× Rust.

**Libraries and build**
20. **CRC32C from `hash/crc32`** (Castagnoli table), which interleaves three SSE4.2 streams on amd64 and uses the CRC32 instructions on arm64.
21. **etcd-raft used directly** (`go.etcd.io/raft/v3`, v3.7.0), driven by synchronous `Ready`/`Advance` with asynchronous storage writes off, as ADR 0007 decided for the port. Upstream draws election timeouts from `crypto/rand` behind a mutex (`raft.go`, `globalRand`), so one patch gives each replica a seeded source; it is carried as a `replace` to a pinned fork with a `docs/tech-debts.md` entry until upstream takes a configurable source. Its `raft.Storage` is ours: etcd-raft reads entries synchronously while it builds appends, so the shard serves every entry after its last compaction from the loop's off-heap slab, and compacts once the memtable that applied them is flushed. A simulator test replays a seed twice and compares histories, which catches any other source of nondeterminism.
22. **cgo only in the SPDK and RDMA drivers**, behind build tags so the default build is pure Go, and every crossing carries a batch: many submissions per call, many completions per poll. `unsafe` only in driver packages, for rings, cgo and registered memory.
23. **Release builds use PGO** (a committed `default.pgo` from a representative profile) and ship for `amd64` at the default `GOAMD64=v1` and for `arm64`. `GOAMD64=v3` gained under 3.5% and would exclude homelab CPUs without AVX2, such as Goldmont Plus Celerons, and `hash/crc32` selects its instructions at run time either way.

## Consequences
- The CPU cost is about 1.07 to 1.1× Rust's on the data path with these rules, and 1.1 to 1.25× on compute-bound code. Each serial owner (a hot PG primary, a disk's commit loop, a busy shard) reaches its throughput ceiling and its latency knee correspondingly earlier, so headroom is planned per owner. The project accepts this for code that is easier to follow.
- The rules are what keep the cost there. A memtable or index with pointers per entry brings back 23 to 35% of CPU in collection, and goroutine-per-connection networking 1.5 to 1.7×, so the vet pass, the allocation checks in CI and code review guard them.
- The Rust crates are deleted, not ported. Their tests' scenarios are kept, language-neutral, in [docs/test-scenarios.md](../test-scenarios.md), and the frozen ID and hash vectors of [ADR 0004](0004-key-layout-and-ids.md) in `internal/core/testdata`. The disk log's byte layout is not frozen until the first release, so the Go code designs its own. The Raft port (`spikes/raft-port`) is not carried over: ADR 0007 is superseded, and its findings on asynchronous storage writes still apply.
- Off-heap memory is managed by hand. A page used after it is freed reads another page's data instead of crashing, so slab handles carry generations, and the simulator poisons freed and freshly allocated memory.
- etcd-raft's cost per group in Go is not yet measured: the 7.7 KiB per group was the Rust port's, and upstream builds its `raftpb` messages on the heap. The Raft shard measures heap and collection cost at 1,000 groups per node before phase 4 is reviewed.
- Protocol models move from `stateright`, which is Rust-only, to TLA+ checked with TLC; etcd-raft keeps a TLA+ specification of its own in its repository (`tla/`).
- The tooling moves with the code: `scripts/lint.sh` runs `gofmt`, `go vet` with the sans-IO analyzer, `staticcheck`, `govulncheck` and a dependency license check.
- The cost of cgo crossings into SPDK and RDMA is not yet measured from Go; the SPDK and RDMA drivers measure it before they commit to a batch size.
