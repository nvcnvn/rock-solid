# Tuned Go spike

**Question.** How much of Go's measured gap to Rust ([Go CPU spike](../go-cpu-per-op/README.md): 1.54× the CPU per replicated 4K write) closes with Go-specific work: fewer syscalls, atomics instead of mutexes and channels, the same event-loop design as Rust, and the compiler's options (profile-guided optimization, `GOAMD64=v3`, bounds checks)? And do Go arenas (`GOEXPERIMENT=arenas`) cut the collector cost of a memtable with one pointer per entry ([Go memtable spike](../go-memtable/README.md))?

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker), 4 vCPUs of an Intel Xeon at 2.1 GHz with AVX2 and AVX-512, Go 1.27.0, Rust 1.99.1. [run.py](run.py) builds everything, collects the profiles and runs:

- **T1, replicated 4K write** (the protocol, client and checks of [go-cpu-per-op](../go-cpu-per-op/)), every design batching the frames that are ready together:
  - **Rust**: one epoll loop per node, as before.
  - **Go v1**: goroutine per connection, as before.
  - **Go v2** ([replicate-go2](replicate-go2/)): requests in flight live in a fixed table of slots with atomic acknowledgment counts, replacing the map and its mutex. The goroutine reading a replica's acknowledgments replies to the client itself, so no request is handed back through a channel. Each replica gets a lock-free bounded ring (Vyukov's) drained into one buffered write, and the writer yields once before it flushes.
  - **Go v3** ([replicate-go3](replicate-go3/)): replicate-rust's design ported line for line. One goroutine per node runs its own epoll loop on raw non-blocking sockets through the `syscall` package, bypassing Go's network poller, with no locks or channels.
- **T2, compiler options**: Go v3 and the Go cleaner harness (inline cleaning at 95% full, without CRC32C) built five ways:
  - the default (`GOAMD64=v1`);
  - `GOAMD64=v3`;
  - profile-guided optimization from a 10 s profile of the default build;
  - both;
  - `-gcflags=all=-B`, which drops bounds checks and is a measurement only, never a build to ship.

  Rust's replicate (default x86-64-v2) and cleaner (default x86-64) are built again with `target-cpu=x86-64-v3`.
- **T3, arenas**: the memtable spike's B-tree of `[]byte` entries at 10 million files, on the Go heap and with every entry, node and item array allocated from one Go arena, beside the page tree. One binary built with `GOEXPERIMENT=arenas` runs all three.

```
python3 run.py      # about 15 minutes; writes a 1.6 GB data file under work/ for T3 and deletes it -> results.txt
```

Raw output: [results.txt](results.txt). Expected outcomes, written before the run: v2 at 30 to 34 µs and at most 3.2 socket writes per operation at 16 connections; PGO 2 to 7%, `GOAMD64=v3` 0 to 3%, bounds checks 1 to 5%; arenas changing the collector's CPU by less than 20%, since an arena chunk holding pointers is scanned like any other object (`runtime/arena.go`). v3 was added after a first look at v2.

## Results

| Test | Result |
| --- | --- |
| T1, 16 connections | Rust 22.3 µs of CPU per write, 79,900 writes/s. **Go v3 27.1 µs (1.21×)**, 70,700 writes/s, with Rust's syscall counts (5.18 reads and 2.66 writes per operation). Go v2 36.3 µs (1.63×), 3.47 writes per operation (FAIL against 30 to 34 µs and 3.2). Go v1 37.8 µs (1.70×), 3.69 writes |
| T1, one connection | Rust p50 74.9 µs, 91 µs of CPU per write. **Go v2 p50 67.3 µs**, 110 µs of CPU (1.20×): replying from the acknowledgment reader removes a goroutine wakeup from every write. Go v3 p50 77.5 µs (1.24×). Go v1 p50 84.2 µs (1.51×) |
| T1, goroutines pinned to threads | A smoke run of v3 with each loop locked to its own OS thread cost 35 µs per write against 29 µs unlocked: blocking in `epoll_wait` on a locked thread makes the runtime hand its processor to another thread and back |
| T2, Go compiler options | Against the default: `GOAMD64=v3` −1.1% (replicate) and −1.0% (cleaner); PGO +3.4% and −1.2%; PGO with `GOAMD64=v3` −1.9% and −3.2%; no bounds checks −1.2% and −2.5%. **All within 3.5%**, about the run-to-run spread of these runs. PGO checks FAIL against 2 to 7% |
| T2, Rust compiler options | `target-cpu=x86-64-v3`: replicate +0.6%, **cleaner −6.3%** |
| T2, Go against Rust after tuning | Replicate: Go's best build 27.4 against Rust's 24.0 µs (1.14×; Rust varied 22.3 to 24.0 between T1 and T2). Cleaner: Go's best 5,632 against Rust's best 5,050 ns (1.12×) |
| T3, memory per file | Go heap 256 B (RSS 268 B), **arena 243 B (RSS 247 B)**, pages 276 B |
| T3, one full collection with the tree live | Go heap 281 to 288 ms wall, 659 to 683 ms CPU. **Arena 77 to 83 ms wall, 307 to 330 ms CPU: 2.1× less CPU, 3.6× less wall time.** Pages 0.2 to 0.4 ms |
| T3, under request garbage | Go heap: collection 23.5% of process CPU, garbage reached 153 of 256 MB/s. **Arena: 9.3%**, 200 MB/s. Pages: 0.0%, 235 MB/s. Arena gets p50 2.96 µs against 2.83 on the heap and 1.69 for pages |

**Why arenas help more than expected.** The collector still visits every pointer in an arena, but an arena chunk is one large heap object (`runtime/arena.go`): marking any pointer into it marks the chunk once, and the chunk is scanned as one linear run of pointer slots. On the heap, each of the 20 million entries is its own object to find, mark and queue. The pointers are the same; the bookkeeping per pointer is far less.

## Conclusions

1. **Design closes most of the gap; compiler options do not.** With Rust's design, Go costs 1.14 to 1.21× Rust on the replicated write path and 1.12× on the cleaner, against 1.5 to 1.7× with goroutine per connection. PGO, `GOAMD64=v3` and bounds checks are each worth under 3.5% on code whose cost is syscalls, block copies and atomics. Build releases with PGO and `GOAMD64=v3` anyway, since they cost nothing, but don't count on them.
2. **A Go data path should be an event loop per node on raw sockets**, the sans-IO driver's shape, not goroutine per connection. Atomics instead of mutexes, maps and channels helped latency at queue depth 1 (p50 67 against 84 µs) but not CPU at 16 connections, where the number of syscalls decides. Don't pin the loops to OS threads.
3. **Arenas cut the collector's cost of a pointer-heavy memtable by 2.1 to 2.5×, but pointer-free pages cut it by 1,000× or more.** Arenas fit memory that is built, used and dropped whole: per-request scratch, a frozen memtable being written out as a sorted run, a snapshot's pieces. They don't fit a long-lived memtable, whose overwritten entries stay until the whole arena is freed. Arenas are still an experiment in Go 1.27, with the same API as in Go 1.20.
4. **Rust gains from `target-cpu=x86-64-v3` where Go does not** (−6.3% on the cleaner). Together with the CRC32C finding in the [Go CPU spike](../go-cpu-per-op/README.md), that makes x86-64-v2 or v3 the right Rust release target.

Not covered: io_uring from Go, cgo calls into SPDK or ibverbs, PGO for Rust (it needs `llvm-profdata`), more than 4 cores, and arenas for the page tree's own slabs, which are already pointer-free.
