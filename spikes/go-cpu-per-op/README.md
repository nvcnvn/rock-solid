# Go against Rust: CPU per operation

**Question.** On drives with power-loss protection, where a flush costs microseconds instead of a millisecond, the data path is bound by CPU per operation. How much more CPU per operation, and how much more latency, would the same design cost in Go than in Rust? This informs the [Language](../../docs/design.md#alternatives-considered) choice. The estimate before measuring was 1.2 to 1.5× the CPU per small write, and +5 to +20 µs at queue depth 1.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker), 4 vCPUs of an Intel Xeon at 2.1 GHz. Go 1.27.0, Rust 1.99.1. Three comparisons, each run by [run.py](run.py), with the two languages interleaved so drift on the shared host hits both:

- **C0, CRC32C of a 4 KiB block** ([crc-rust](crc-rust/), [crc-go](crc-go/)). Rust: the `crc` 3.4 crate with one table and with the 16 tables `crates/core` uses; a single-stream SSE4.2 loop; the `crc32c` 0.6.8 crate. Each is built for the default x86-64 target and for x86-64-v2. Go: `hash/crc32`.
- **G1, the parallel cleaner harness** ([cleaner-go](cleaner-go/) is a line-by-line port of [../parallel-cleaner](../parallel-cleaner/)). Same tests, with `crc=0`, because the first run (below) measured the two CRC implementations rather than the languages. The owner thread is locked to an OS thread in Go so its CPU can be read; the cleaner's figure in Go is process CPU minus the owner's, which includes the runtime and the collector.
- **G2, a replicated 4K write** ([replicate-rust](replicate-rust/), [replicate-go](replicate-go/)). A client sends a 4K write to a primary, which verifies its CRC32C, appends it to an in-memory log and sends it to two replicas that do the same and acknowledge. The primary acknowledges once both have. Everything runs over loopback TCP in one process, with no disk, as on a drive whose flush is nearly free. Rust runs the design's shape: one epoll event loop per node, non-blocking sockets, no locks or channels, CRC32C from the `crc32c` crate built for x86-64-v2. Go runs idiomatic goroutines: one per connection, a mutex per replica connection and a channel per request. Both run with one connection (queue depth 1) and with 16, in two write modes:
  - `coalesce=0`: every frame is written as soon as it is produced;
  - `coalesce=1`: frames ready together go out in one write. In Rust, once per pass of the event loop. In Go, a writer goroutine per replica connection drains its queue into one buffered write, and a replica flushes its acknowledgments only when no further frame is already buffered.

  The harnesses count their own socket reads and writes; `/proc/self/io` counts only `read` and `write`, and Rust's standard library sends with `send` and `recv`.

```
python3 run.py      # about 12 minutes -> results.txt
CPUPROFILE=cpu.prof bin/replicate-go conns=16 coalesce=1 secs=8 && go tool pprof -top bin/replicate-go cpu.prof
```

Raw output: [results.txt](results.txt), and the first G1 run with CRC32C on: [results-g1-crc.txt](results-g1-crc.txt).

## Results

| Test | Result |
| --- | --- |
| C0: CRC32C per 4 KiB | Go `hash/crc32` **167 ns**. Rust `crc32c` crate **215 ns built for x86-64-v2, 798 ns for the default target**. Single-stream SSE4.2 loop 415 / 759 ns. `crc` crate with 16 tables, as in `crates/core`, **1,174 to 1,213 ns**; with one table 10.5 µs. Go uses three interleaved streams (`castagnoliSSE42Triple`, `hash/crc32/crc32_amd64.go`). The `crc32c` crate does too, but its entry point lacks `#[target_feature(enable = "sse4.2")]` (`src/hw_x86_64.rs:8` against `:52`), so on the default target every 8-byte step is a function call that cannot inline |
| G1, first run with CRC32C | Go needed **0.71 to 0.79×** Rust's owner work per write at 90 and 95% full. The harnesses differ in CRC32C (Rust's single-stream loop, 4.5× slower than Go's per C0) and do 4.9 to 9.1 checksums per user write |
| G1, without CRC32C: owner work per write | Inline at 90 / 95% full: Rust 2,862 / 5,093 ns, Go 3,330 / 5,768 ns, **1.16× / 1.13×**. Paced at 50k writes/s: inline 1.14×, shared mode 1.14×, owner mode 0.92× (noise at about 1.4 µs). Write p99 inline 133 / 271 µs against 150 / 301 µs |
| G1, without CRC32C: cleaner CPU per user write | Owner mode 6,620 against 8,233 ns (1.24×); shared mode 6,248 against 7,000 ns (1.12×). Go's figure includes the runtime and the collector. Write amplification identical (9.11 to 9.13) |
| G2, one connection | Rust 12,300 writes/s at p50 75 µs and 91 µs of CPU per write; Go 10,700 to 11,500 writes/s at **p50 85 to 87 µs (+10 to +11 µs)** and 124 to 141 µs of CPU per write (**1.35 to 1.55×**). Most of a write's CPU here is waking each idle node in turn, so this ratio says more about idle wakeups than about the data path. Go uses 1.42 to 1.50 cores against 1.12, from the scheduler looking for work before it sleeps |
| G2, 16 connections, every frame written at once | Rust 55.5 µs of CPU per write, 31,500 writes/s; Go 55.2 µs, 56,600 writes/s: **the same CPU per write**, 6.00 socket writes per operation in both. Go's throughput is higher because its goroutines spread over all 4 vCPUs (3.13 cores), while each Rust node is one thread |
| G2, 16 connections, frames batched | **Rust 25.4 µs of CPU per write, Go 39.2 µs (1.54×)**, both about 74,000 to 75,000 writes/s, Go on 2.95 cores against Rust's 1.88. p50 204 against 196 µs, p99 442 against 522 µs. Socket writes per operation: Rust 2.78, Go 3.69 |
| G2, Go CPU profile, 16 connections, batched | 62% inside syscalls, 7% `memmove`, about 9% in the scheduler (`schedule`, `findRunnable`) and about 5% in channels. Most of the 1.54× is the extra socket write per operation, each of which also wakes the receiving goroutine; the runtime adds the rest |

## Conclusions

1. **On CPU-bound code Go costs 1.1 to 1.25× Rust.** That covers block copies, index updates, atomics and channel or batch handoffs (G1), and is at the low end of the estimate.
2. **On the replicated write path, the number of syscalls matters more than the language.** With one write per frame, Go and Rust cost the same CPU per write (G2, 55 µs). Batching what is ready together halves Rust's cost. It cuts Go's by 29%, leaving Go at 1.54×, mostly because the goroutine design still makes a third more writes per operation. On a node with power-loss-protection drives that is CPU-bound at small writes, expect about 35% fewer 4K writes per core in Go with each design batching as above.
3. **Queue-depth-1 latency is about +10 µs in Go** (G2), at the top of the error this VM's 75 to 90 µs loopback round trip allows. Against the 0.1 ms target for drives with power-loss protection, that is about 10%.
4. **Use hardware CRC32C with three streams, and build for x86-64-v2 or later.** `crates/core` checksums each 4K block with the `crc` crate's 16 tables at about 1.2 µs, 5.5 to 7.3× slower than `crc32c` built for x86-64-v2 or Go's `hash/crc32` (C0). The difference is about 1 µs per block: a tenth of a core at 100,000 blocks per second, a whole core at 1 million (4 GB/s). Built for the default x86-64 target, `crc32c` is no faster than a single-stream loop.

Not covered: disk I/O and io_uring, RDMA and SPDK (so cgo's cost), more than 4 cores, and a Go design with one goroutine per node driving its own event loop, which Go's standard library does not expose.

## What a Go port would rely on

Checked against the Go 1.27.0 toolchain's source:

- **Map iteration order cannot be made deterministic.** Every iterator starts at a random offset (`internal/runtime/maps/table.go:757-758`), and no `GODEBUG` or `GOEXPERIMENT` setting changes it. The sans-IO core's rule becomes: never range over a map. Use ordered structures, or `slices.Sorted(maps.Keys(m))` (iterators, Go 1.23). This would be enforced by a `go/analysis` vet pass, the counterpart of `clippy.toml`'s `HashMap` ban, pinned with a `tool` directive in `go.mod` (Go 1.24).
- **Seeded randomness**: `math/rand/v2` with `rand.NewPCG` or `rand.NewChaCha8`, never the global functions. The same vet pass bans `time.Now`, the global `rand` functions, the `go` statement and `select` in the core.
- **Fake time for driver tests**: `testing/synctest` (stable since Go 1.25) runs goroutines in a bubble with a fake clock. Scheduling order inside the bubble is still not seeded, so the simulator stays single-threaded and sans-IO, as etcd-raft's `Ready` loop already is.
- **Garbage collector**: Green Tea is on by default (`internal/buildcfg/exp.go`; `GOEXPERIMENT=nogreenteagc` still opts out). Size-specialized malloc is also on. Memory-heavy structures still need pointer-free layouts ([go-memtable](../go-memtable/README.md)).
- **Goroutine leak profile**: `goroutineleak` in `runtime/pprof` is no longer an experiment in 1.27.
- **SIMD**: the `simd` and `simd/archsimd` packages exist behind `GOEXPERIMENT=simd`. `hash/crc32` needs none of it.
- **Generic methods** compile in Go 1.27.
- **Observability**: `runtime/trace.FlightRecorder` (Go 1.25) keeps the last seconds of execution trace for a p99 spike. `GOMAXPROCS` follows the container's CPU limit and updates while running (Go 1.25).
- **Not available**: array lengths as type parameters, goroutine priorities or core affinity (only `runtime.LockOSThread`), and an epoll event loop in the standard library.
