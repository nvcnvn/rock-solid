# 0009. Write rock-solid in Go

Status: Accepted (2026-10-18)

## Context
rock-solid was designed in Rust: a sans-IO core of state machines, drivers that execute their actions, and etcd-raft ported to Rust for the Raft shard ([ADR 0007](0007-port-etcd-raft.md)). The project is easier to read, review and extend in Go, which is also the language of etcd-raft, the CSI tooling, go-fuse, Porcupine and cilium/ebpf. A series of spikes on one 4-vCPU cloud VM, with Go 1.27.0 and Rust 1.99.1, measured what Go costs:

| Question | Measurement |
| --- | --- |
| CPU per replicated 4K write, same epoll design ([tuned Go spike](../design.md#appendix-tuned-go-spike)) | Go 1.21× Rust; 1.5 to 1.7× with a goroutine per connection, whatever its synchronization |
| Where that comes from ([codegen spike](../design.md#appendix-go-and-rust-codegen-spike)) | Not the compiler: the generated code costs about the same per write. About 61% is runtime scheduling (`sysmon` retaking the P of a loop blocked in `epoll_wait`), 16% syscall wrappers |
| After tuning the runtime (codegen spike) | **Go 1.07× Rust** with `GOMAXPROCS` above the blocking loops, raw syscalls for non-blocking I/O, and memory touched before use |
| Compute-bound code ([Go CPU spike](../design.md#appendix-go-cpu-per-operation-spike), [tuned Go spike](../design.md#appendix-tuned-go-spike)) | Go 1.1 to 1.25× Rust; PGO, `GOAMD64=v3` and bounds checks each under 3.5% |
| Rising open-loop load ([open-loop latency spike](../design.md#appendix-open-loop-latency-spike)) | Same throughput up to 100,000 writes/s; Go's p99 knee 1.2× earlier (measured before the runtime tuning above) |
| Garbage collector ([Go memtable spike](../design.md#appendix-go-memtable-spike), [tuned Go spike](../design.md#appendix-tuned-go-spike)) | Pauses under 1 ms. A memtable with a pointer per entry costs 23 to 35% of CPU in collection; in a Go arena 9%; as pointer-free 4 KiB pages 0% (0.3 ms per cycle at 20 million entries) |
| CRC32C (Go CPU spike) | Go `hash/crc32` 167 ns per 4 KiB, against 1.2 µs for the table-driven crate in `crates/core` |

The rules below are what make those numbers hold; without them the measured cost is 1.5 to 1.7× the CPU and a quarter of it in collection.

## Decision
rock-solid is written in Go, pinned to a toolchain release in `go.mod` (`go 1.27`, `toolchain go1.27.0`). The sans-IO design, the one log per disk and every other decision stay. Code follows these rules, each backed by the measurement in its spike.

**Event loops**
1. **One event loop per node role** (each disk's commit loop, each core's set of shards, each network peer set) as a goroutine running `epoll` through the `syscall` package on raw non-blocking descriptors. Never a goroutine per connection, and never `net.Conn` on the data path: 1.21× against 1.5 to 1.7× Rust's CPU.
2. **`GOMAXPROCS` above the number of loops that block in `epoll_wait`.** With as many busy loops as Ps, `sysmon` retakes a blocked loop's P (`runtime/proc.go`), costing futex wakeups and CPU migrations: −2.9 µs per write, futex calls from 0.21 to 0.02 per write. The daemon sets it at start from its loop count and the container's CPU limit.
3. **`syscall.RawSyscall` for reads, writes and sends on non-blocking descriptors**, which never block; `syscall.Syscall` for `epoll_wait` and anything that can: −1.4 µs per write.
4. **Don't lock loops to OS threads** (35 against 29 µs per write), except where a C library requires its own thread, as SPDK's `spdk_thread` does.
5. **Every pass does bounded work:** a read budget per descriptor per pass (256 KiB), a cap on queued replication bytes that stops reading clients while over it, and output buffers that are rings or compacted in place. A slice grown by `append` copies itself (`runtime/slice.go`), and a loop that drains every socket collapsed into multi-second stalls from 30,000 writes/s.
6. **One write per descriptor per pass**, gathering every frame ready for it: syscalls set the cost of a write, and batching halved Rust's and cut Go's by 29%.

**State machines**
7. **`Event` and `Action` are tagged structs, not interfaces**, and `OnEvent` appends its actions to a slice the caller owns and reuses, so a call allocates nothing. Hot-path structs are filled in place, not built as temporaries and copied: a frame literal copied into a slice cost 534 ns, filled in place 177 ns.
8. **Determinism in the core:** no `range` over a map (Go randomizes iteration on purpose, `internal/runtime/maps/table.go`; iterate ordered structures or `slices.Sorted(maps.Keys(m))`); randomness only from a seeded `math/rand/v2` source (PCG or ChaCha8) per replica; no `time.Now`, `go` statement, channel, `select` or `sync` primitive. A `go/analysis` pass in `go vet` enforces these as `clippy.toml` did. Driver tests use `testing/synctest` for fake time.

**Memory**
9. **Long-lived bulk structures hold no pointers.** The memtable is a copy-on-write B+-tree of 4 KiB pages (`[4096]byte`) in slabs, pages referring to each other by `uint32` number; the chunk index and similar tables are slices of integers. Collection then costs 0.3 ms at 20 million entries instead of 0.66 CPU-seconds per cycle, and a snapshot is the root page number at the freeze. Restored pages are 98% full (175 B per file); pages filled in create order are 63% full, so the tree redistributes into a neighbour before splitting.
10. **I/O buffers come from the driver's slab of fixed-size blocks**, handed out as a `uint32` slot plus generation, never from `sync.Pool`, which empties at collections, has no bound and can't be registered for DMA. SPDK and RDMA slabs are C-allocated, registered memory, which the collector never scans and cgo's pointer rules allow. `sync.Pool` is for request scratch in the frontends only.
11. **Go arenas (`GOEXPERIMENT=arenas`) only for memory freed whole**: a frozen memtable while it is written out as a sorted run, a snapshot's pieces, per-request scratch. They cut collection 2.1 to 2.5× against the heap, but keep every overwritten entry until freed. They are an experiment, so each use gets a `docs/tech-debts.md` entry.
12. **`GOMEMLIMIT` is the daemon's memory budget**, and hot-path benchmarks report allocations (`b.ReportAllocs`); CI fails one that allocates per operation.

**Concurrency outside the loops**
13. **Read-mostly tables** (the placement table, fenced sessions) are immutable values published through `atomic.Pointer`; readers load the current one without locks, and the collector frees old ones.
14. **Latency paths use typed atomics and fixed slot tables** instead of a mutex, a map and a channel: queue-depth-1 p50 67 against 84 µs. Counters written by different cores are padded to a cache line.
15. **CPU-heavy background work runs on worker goroutines** that hand results to the owning loop as events, as the cleaner does: its copy work costs Go 1.12 to 1.24× Rust.

**Libraries and build**
16. **CRC32C from `hash/crc32`** (Castagnoli table), which interleaves three SSE4.2 streams.
17. **etcd-raft used directly** (`go.etcd.io/raft/v3`, v3.7.0), driven by synchronous `Ready`/`Advance` with asynchronous storage writes off, as ADR 0007 decided for the port. Upstream draws election timeouts from `crypto/rand` (`raft.go`), so one patch gives each replica a seeded source; it is carried as a `replace` to a pinned fork with a `docs/tech-debts.md` entry until upstream takes a configurable source. A simulator test replays a seed twice and compares histories, which catches any other source of nondeterminism.
18. **cgo only in the SPDK and RDMA drivers**, behind build tags so the default build is pure Go, and every crossing carries a batch: many submissions per call, many completions per poll.
19. **Release builds use PGO** (a committed `default.pgo` from a representative profile) **and `GOAMD64=v3`**. They cost nothing; the measured gain is under 3.5%, so no budget counts on it.

## Consequences
- The CPU cost is about 1.07 to 1.1× Rust's on the data path with these rules, and 1.1 to 1.25× on compute-bound code. Each serial owner (a hot PG primary, a disk's commit loop, a busy shard) reaches its throughput ceiling and its latency knee correspondingly earlier, so headroom is planned per owner. The project accepts this for code that is easier to follow.
- The rules are what keep the cost there. A memtable or index with pointers per entry brings back 23 to 35% of CPU in collection, and goroutine-per-connection networking 1.5 to 1.7×, so the vet pass, the allocation checks in CI and code review guard them.
- `crates/core` and `crates/sim`, about 6,100 lines, are rewritten as Go packages; until then the Rust crates stay and `docs/tech-debts.md` records them. The Raft port (`spikes/raft-port`, the planned `rock-raft` crate) is not carried over: ADR 0007 is superseded, and its findings on asynchronous storage writes still apply.
- Protocol models move from `stateright`, which is Rust-only, to TLA+ checked with TLC; etcd-raft keeps a TLA+ specification of its own in its repository (`tla/`).
- The tooling moves with the code: `scripts/lint.sh` runs `gofmt`, `go vet` with the sans-IO analyzer, `staticcheck` and `govulncheck`, and the project instructions, the sans-IO skill and the dependency license check get Go counterparts.
- There is no io_uring in Go's standard library; the default disk driver drives its own ring through raw syscalls.
- The cost of cgo crossings into SPDK and RDMA is not yet measured from Go; the SPDK and RDMA drivers measure it before they commit to a batch size.
