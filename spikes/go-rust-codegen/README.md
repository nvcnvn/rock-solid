# Go against Rust: where the extra cycles go

**Question.** With the same event-loop design and the same syscall counts, Go v3 costs 27.1 µs of CPU per replicated 4K write against Rust's 22.3 µs (1.21×, [go-tuned](../go-tuned/README.md)), and Go's cleaner harness costs 1.12 to 1.16× Rust on pure computation ([go-cpu-per-op](../go-cpu-per-op/README.md)). The collector is not the cause. Where do the extra cycles go: the syscall wrappers, the runtime scheduler, or the code Go's compiler generates? And don't the two compilers share a backend?

**Short answer.** They share nothing below the parser. Most of the gap is Go's runtime scheduler and syscall wrappers, not generated code. Part of it was the benchmarks themselves.

## Toolchains

| Compiler | Backend | State |
| --- | --- | --- |
| **gc** (`cmd/compile`, what `go build` runs) | Go's own SSA backend, no LLVM or GCC: a fixed list of passes (`cmd/compile/internal/ssa/compile.go:464-518`: `opt`, `generic cse`, `nilcheckelim`, `prove` for bounds checks, `dse`, `lower` from rule files such as `ssa/_gen/AMD64.rules`, `tighten`, `regalloc`), and a linear-scan register allocator that treats the function as one long block (`ssa/regalloc.go:7-15`). Inlining budget 80 nodes (`inline/inl.go:50`). A struct with more than 4 fields or more than 32 bytes is never held in registers (`ssa/decompose.go:412` `MaxStruct = 4`, `ssa/value.go:614` `CanSSA`) | The production compiler. It is built for compile speed and does little alias analysis, no auto-vectorization and no SROA of large structs |
| **rustc** 1.99.1 | LLVM 23.1.1 (`rustc -vV`): LLVM's full optimization pipeline, including SROA, alias analysis from `&`/`&mut` (`noalias`), dead store elimination across allocations, and the loop vectorizer | Production |
| **gccgo** 14.2 (Ubuntu 24.04 `gccgo-14`) | GCC's backend. Front end `gofrontend` in C++, runtime `libgo` | Its library is **Go 1.18** (`strings libgo.so.23`: `go1.18 gccgo (Ubuntu 14.2.0-4ubuntu2~24.04.1) 14.2.0`) and it **does not parse generics** (a `func Max[T int\|float64]` fails with `expected '('`), as the gccgo page says for GCC 12 and 13 ([go.dev/doc/install/gccgo](https://go.dev/doc/install/gccgo)). replicate-go3 doesn't build with it (generics in `slices`, Go 1.19+ APIs, range over int) |
| **gollvm** | The same `gofrontend`, bridged to LLVM IR, with `libgo` as its runtime ([go.googlesource.com/gollvm](https://go.googlesource.com/gollvm/)) | Never released, Linux x86-64 and arm64 only, and it says it is not meant to replace gc. Its master branch was last moved to LLVM mainline about two years ago. It shares gccgo's Go 1.18-era front end and runtime. Not tried: go.googlesource.com cannot be reached from this container |

So "the same backend" does not hold for gc. The only LLVM or GCC route for Go is gofrontend with libgo, and that is stuck at Go 1.18 and loses gc's assembly fast paths (M below).

## Setup

Linux 6.18.44 in a Firecracker VM, 4 vCPUs, Go 1.27.0, Rust 1.99.1 (LLVM 23.1.1), gccgo 14.2.0, perf 6.8.12. **The VM moved to a different host during the spike.** `/proc/cpuinfo` now says Xeon @ 2.80 GHz (2.10 GHz in go-tuned), and the replicate runs cost about 1.6× go-tuned's absolute µs, with run-to-run spread up to ±20%. So the tables below compare variants within one run, not against go-tuned's numbers. Another agent benchmarks on the same vCPUs, and every timed command runs under `flock` on a shared lock. perf works without root here, but only with software events (`cpu-clock`): the VM has no PMU, so there are no cycle or cache-miss counters.

- **M, microbenchmarks** ([micro-go](micro-go/), [micro-rust](micro-rust/)): each piece of the write path in both languages, with the parse, header and ring code copied from the two programs. micro-go sticks to Go 1.18 so gccgo builds the same source (`-O2 -march=x86-64-v2`). Pinned to CPU 3 with `taskset`, so Go runs with `GOMAXPROCS=1` and sysmon shares the CPU. 3 rounds alternating Go, gccgo and Rust; the median of 7 repetitions per round, then the median over rounds.
- **V, replicate variants** (16 connections, 5 s, 5 rounds interleaved):
  - `rust`: [go-cpu-per-op/replicate-rust](../go-cpu-per-op/replicate-rust/) as published.
  - `rust-kept` ([replicate-rust-kept](replicate-rust-kept/)): the same with `black_box` on each ring, so LLVM keeps the replicas' copies into their rings (A1). With `touch=1`, every ring page is written before the measured window.
  - `go3`: [go-tuned/replicate-go3](../go-tuned/replicate-go3/) as published (`lock=0`).
  - `go4` ([replicate-go4](replicate-go4/)): go3 with `touch=1`, and `raw=1` for socket reads and writes through `syscall.RawSyscall` (the sockets are non-blocking, so these calls never block; `epoll_wait` still goes through `syscall.EpollWait`). Its `raw=0` path calls `syscall.Syscall` and compares `Errno` values instead of the `error` interface.
  - `P8`: the same binary with `GOMAXPROCS=8`.
- **P1, counters**: `perf stat` with syscall tracepoints, context switches, migrations and page faults over a whole run.
- **P2, profiles**: `perf record -g -e cpu-clock -F 4000` from the start of the measured window (`-D`), split by [classify.py](classify.py). A kernel sample goes to the syscall named by its `__x64_sys_*` frame (or to syscall entry/exit, page faults or interrupts); a user sample goes by its leaf symbol. Mean of 2 runs. Also Go's own pprof (`CPUPROFILE`).
- **A, generated code**: `go tool objdump` and `objdump` of the parse loop, `-gcflags=-d=ssa/check_bce/debug=1` and `-m=2` on replicate-go3.

```
./fetch.sh          # perf and gccgo from Ubuntu .debs into vendor/, no root
python3 run.py      # about 30 minutes with the lock shared; BENCH_LOCK=<file> to share a lock -> results.txt, asm/
```

Raw output: [results.txt](results.txt) (one header line per stage, because the run was resumed stage by stage after a container restart). Assembly: [asm/](asm/).

## Expected outcomes (written before running)

- P1: the same reads, writes and `epoll_wait` calls; Go adds 0.3 to 1 `futex`, `nanosleep` or `epoll_pwait` calls per write, and more context switches.
- P2: kernel time per write equal within 1 µs; Go's runtime (entersyscall/exitsyscall, `schedule`, `findRunnable`, spinning) 2 to 3 µs per write; user code about 1 µs more in Go.
- M: Go's syscall wrapper adds 20 to 40 ns per call (about 0.3 µs per write at 10 calls); map, parse and copy each within 30 ns of Rust; together under 1.5 µs per write. So **most of the 4.8 µs is the scheduler around a blocking `epoll_wait`, not generated code**.
- A: Go emits 2 to 4 bounds checks in the parse loop that LLVM removes, inlines less, and does not vectorize byte loops; `_ = b[n]` hints remove the checks but save under 1%.
- V: Go source changes (raw syscalls for non-blocking I/O, fewer pointers) recover 1 to 2 µs of the 4.8.

## Results

### A1. The published Rust program skips two of its three ring copies

| Test | Result |
| --- | --- |
| A1, dead stores | The rings in replicate-rust are written but never read, so LLVM deletes the replicas' `copy_from_slice` into them as dead stores. In the replica thread's code, the CRC32C call is followed directly by the ring offset update and the acknowledgment header, with no `memcpy` (`objdump` of `__rust_begin_short_backtrace::<replicate_rust::main::{closure#0}>`, 0x17349 to 0x17431). The primary keeps its copy. **The published Rust figure does one 4K ring copy per write where Go does three.** Evidence: 0.078 page faults per write for rust against 0.24 to 0.28 for rust-kept and go3 (P1 counters in results.txt); in an earlier run, 66 MB was resident in the 192 MB that holds the three rings (`/proc/<pid>/smaps`) |
| A1, the cost | Keeping the copies (`rust-kept`) adds **1.07 µs of `memmove` per write** in P2 (1.39 → 2.46 µs). V's totals do not resolve the difference (36.02 against 36.83 µs, inside the spread of 29 to 51 µs) |
| A1b, first-touch faults | Each ring page faults once, when it is first written. Inside this VM a 4K first-touch fault can cost about 11 µs in `clear_page_erms`: one go3 profile on the old host showed 2,243 samples in it for 51,700 faults, while a Rust run had 30 samples for 50,000. The likely cause is that the VM's balloon has free page reporting on (`/sys/bus/virtio/devices/virtio0/features` bit 5, `VIRTIO_BALLOON_F_REPORTING`), so a freed guest page is returned to the host and costs a host fault when touched again. This is unconfirmed without host access. In this run, go3 pays **0.71 µs per write** in page faults and `touch=1` brings it to 0.06. It is an artifact of the VM and of the 5 s run length, not of the language |

### M. Microbenchmarks (ns per operation)

| Benchmark | Go (gc) | gccgo | Rust | gc − Rust |
| --- | --- | --- | --- | --- |
| `getppid`: gc `Syscall`, gc `RawSyscall`, Rust libc | 181.6, raw 105.2 | 426.9, raw 112.9 | 100.0 | **+82 (wrapper +76)** |
| `read` returning EAGAIN: gc `syscall.Read`, raw; Rust std `recv`, libc `read` | 356.6, raw 237.5 | 746.8, raw 236.1 | recv 179.5, read 215.5 | +177 (wrapper +119; `read` costs 36 more than `recv` in the kernel) |
| `epoll_wait`, timeout 0, nothing ready | 191.4, raw 127.7 | 446.8 | 105.0 | +86 (wrapper +64) |
| `epoll_wait`, timeout 0, one ready | 383.0, raw 259.6 | 646.6 | 237.4 | +146 (wrapper +123) |
| loopback write + read, 24 B / 4,120 B | 6,352 / 6,905, raw 4,857 / 4,784 | 7,570 / 7,349 | 4,388 / 4,542 (std) | kernel-bound; the gc wrapper gap here (1.5 µs) is sysmon, which shares the pinned CPU |
| `time.Now` / `Instant::now` | 44.7 | 63.4 | 21.4 | +23 (two vDSO reads: wall and monotonic) |
| parse 64 ack frames | **534.2** | 2,206.6 | **156.2** | **+378 (3.4×)** |
| the same with `_ = b[n]`-style reslicing (BCE) | 493.0 | 2,054.5 | | −8% |
| the same, filling each frame in place (`f := &frames[len-1]`) | **177.1** | 2,186.6 | | **3.0× faster, within 13% of Rust** |
| parse 16 replicate frames | 134.1 | 547.9 | 39.9 | +94 |
| 16 headers | 71.2 | 719.3 | 49.9 | +21 |
| header + 4 KiB append | 45.9 | 90.5 | 46.2 | 0 |
| CRC32C, 4 KiB | **182.9** | 2,019.5 | 221.2 | −38 (C0's 167 against 215 ns, reproduced) |
| 4 KiB into a 64 KiB / 64 MiB ring | 78.5 / 569.0 | 79.1 / 560.2 | 74.6 / 599.0 | 0 (memory-bound) |
| pending map: insert, 2 lookups, delete | `*pend` 84.5, value 87.9 | 281.3, 233.9 | SipHash 52.0, Fx 16.4 | +33 |
| sum 4 KiB of bytes, indexed | 2,757.6 | 1,982.7 | **650.6** | +2,107: LLVM vectorizes (`pmovzxbq` + `paddq`), gc emits no vector adds |
| gather 1,024 bytes by index | 689.8 | 849.3 | 693.6 | 0: both check every index |

**Summed per write** (counts per write from P1: 8.9 syscalls, 6 frames parsed, 6 headers, 3 CRCs, 1 map cycle, 1 clock read): the syscall wrapper is **+0.6 to +1.1 µs** (64 to 123 ns × 8.9). The `read`-versus-`recv` kernel path is +0.19 µs (36 ns × 5.2). Generated code totals **about 0**: parse +0.04, headers +0.01, map +0.03 and clock +0.02, against −0.11 for Go's faster CRC32C. Where the 4.8 µs goes is the runtime, not the compiler.

### P1. Syscalls and scheduler events per write

| Variant | Syscalls | `epoll_wait` | `futex` | `nanosleep` | Context switches | CPU migrations |
| --- | --- | --- | --- | --- | --- | --- |
| rust-kept touch | 9.04 | 0.945 | 0 | 0 | 0.505 | 0.003 |
| go3 | 9.17 | 0.834 | **0.210** | **0.226** | **0.838** | **0.137** |
| go4 touch P8 | 8.86 | 0.840 | 0.024 | 0.092 | 0.590 | 0.076 |
| go4 raw touch P8 | 9.07 | 0.925 | 0.018 | 0.064 | 0.570 | 0.056 |

Reads (5.2 to 5.4) and writes (2.7 to 2.8) per write are the same in all variants, so the design is identical. Go adds 0.44 runtime syscalls per write, 66% more context switches and 46× the migrations.

### P2. Profile split, µs per write (perf on the new host, so totals run about 2× go-tuned's)

| Bucket | rust | rust-kept touch | go3 | go4 touch | go4 raw touch P8 |
| --- | --- | --- | --- | --- | --- |
| kernel: write (incl. loopback receive and wakeups) | 27.53 | 26.55 | 25.81 | 23.00 | 25.31 |
| kernel: `epoll_wait` (incl. sleeping and waking) | 10.25 | 9.80 | 8.10 | 7.20 | 8.64 |
| kernel: read | 5.68 | 5.48 | 6.81 | 6.11 | 6.43 |
| kernel: syscall entry/exit | 1.13 | 1.00 | 1.66 | 1.51 | 1.38 |
| **kernel: `futex`** | 0 | 0 | **3.23** | 2.88 | 0.21 |
| **kernel: `nanosleep`** (sysmon) | 0 | 0 | **4.22** | 3.74 | 1.20 |
| kernel: page faults | 0.21 | 0.02 | 0.71 | 0.06 | 0.06 |
| user: syscall wrappers | 1.22 | 1.19 | 2.42 | 2.11 | 1.44 |
| **user: scheduler/sysmon** | 0 | 0 | **1.28** | 1.21 | 0.32 |
| user: memmove | 1.39 | 2.46 | 2.51 | 2.36 | 2.35 |
| user: program code | 1.80 | 1.87 | 2.15 | 2.01 | 2.06 |
| user: CRC32C | 0.35 | 0.29 | 0.97 | 0.97 | 0.95 |
| user: map, alloc/GC, clock, other runtime | 0.18 | 0.18 | 0.94 | 0.86 | 0.77 |
| **total** | 49.79 | 48.97 | **60.89** | 54.10 | **51.34** |

**The gap decomposed.** go3 against rust-kept touch is 11.9 µs here (1.24×, close to go-tuned's 1.21×). The last column is the 4.8 µs scaled by each part's share:

| Part | µs here | Share | Of the published 4.8 µs |
| --- | --- | --- | --- |
| Scheduler and sysmon: `futex` + `nanosleep` + user scheduler + other runtime, minus the `epoll_wait` sleeping Go no longer does in the kernel | 8.92 − 1.70 = **7.22** | 61% | **~2.9 µs** |
| Syscall wrappers (entersyscall/exitsyscall) and their kernel entry/exit | 1.23 + 0.66 = **1.89** | 16% | **~0.8 µs** |
| User code: CRC32C +0.68, program +0.28, map +0.24, alloc/GC +0.23, clock +0.11, memmove +0.05 | **1.59** | 13% | ~0.6 µs (0.4 without CRC32C, see below) |
| First-touch page faults (A1b) | **0.69** | 6% | ~0.3 µs |
| `read`/`write` instead of `recv`/`send` in the kernel (net of write −0.74) | **0.59** | 5% | ~0.2 µs |

The published 4.8 µs also understates the full gap, because the Rust side skipped two 4K copies (A1: +1.07 µs of `memmove` once kept).

Two attribution limits. First, the CRC32C bucket disagrees with M: Rust's in-program share is about 100 ns per checksum, below what M measures for the same call (221 ns) and below what 512 serial `crc32q` steps can take at any clock this VM reports. Without a PMU, `cpu-clock` samples can land on neighbouring code, so take CRC32C from M (Go is faster) and not from P2. Second, the map, alloc and program buckets are 0.75 µs above M's 0.1 µs. This is consistent with Go's 0.137 migrations per write leaving caches cold, which can't be measured here.

**Go's pprof misses the runtime cost.** pprof saw 1.55 cores against 1.90 by `getrusage`, with `runtime.futex` at 1.3% and `schedule` at 1.8%. Go arms its per-thread profiling timer only when an M runs a goroutine (`runtime/proc.go:3388`, in `execute`), so sysmon and Ms parking in `futex` are never sampled. Use `perf` to see the runtime's share.

**Where the futex and nanosleep calls come from** (Go frame-pointer stacks under the kernel samples, from the earlier profile on the old host):

- 57% `notewakeup < startm < handoffp < retake < sysmon`;
- 43% `notesleep < stopm < findRunnable/exitsyscallNoP`;
- all `nanosleep` is `usleep < sysmon`.

With four goroutines on four Ps there is no idle P. sysmon then retakes the P of any goroutine blocked in `epoll_wait` for more than one 20 µs tick (`proc.go:6754` skips the retake only if a P is idle or spinning). `handoffp` starts a spinning M to run it (`proc.go:3167`), which finds no work and sleeps. The goroutine returns from `epoll_wait` without a P and parks its own M (`exitsyscallNoP`). The goroutine then resumes on another thread, often on another CPU. Each successful retake resets sysmon to its 20 µs sleep (`proc.go:6550`).

### V. Go changes, CPU µs per write (median of 5; spread 29 to 53 µs on this host)

| Variant | Median | min to max | vs rust-kept touch |
| --- | --- | --- | --- |
| rust | 36.02 | 29.37 to 38.66 | −0.04 |
| rust-kept | 36.83 | 35.08 to 50.98 | +0.77 |
| rust-kept touch | 36.06 | 34.24 to 41.25 | 0 |
| go3 | 45.44 | 42.02 to 53.04 | **+9.38 (1.26×)** |
| go4 touch | 42.84 | 40.99 to 48.41 | +6.78 |
| go4 touch P8 | 39.99 | 38.26 to 44.81 | +3.93 |
| go4 raw touch | 44.43 | 39.48 to 47.81 | +8.37 |
| **go4 raw touch P8** | **38.57** | 31.33 to 43.25 | **+2.51 (1.07×)** |

Under `perf stat` (P1's runs), task-clock per write was 45.75 µs for rust-kept touch, 52.49 for go3, 44.44 for go4 touch P8 and 45.20 for go4 raw touch P8.

### A. Generated code of the frame parse loop

| Test | Result |
| --- | --- |
| A2, instructions per frame | **Rust 30, 1 bounds check** ([asm/micro_rust-parse.s](asm/micro_rust-parse.s)). **Go 46, 3 bounds checks** ([asm/main-parse.s](asm/main-parse.s)). Go also reloads `c.rbuf` and `c.rlen` from memory on every frame, because it does no alias analysis. Rust reloads the `Vec`'s length and capacity, which LLVM can't prove the stores don't alias |
| A3, the real cost: a stack temporary | `frame` has 5 fields and 40 bytes, so it is not SSA-able (`ssa/decompose.go:412`, `ssa/value.go:614`). `append(frames, frame{...})` builds the literal in a stack temporary (`walk/builtin.go:60`, `cheapExpr`): three 16-byte zero stores (`MOVUPS X15`), then five narrow stores (`MOVB`, `MOVQ`, `MOVL`), then three 16-byte loads (`MOVUPS 0(R15)`) copying it into the slice. Each wide load spans several narrow stores, which x86 can't forward from the store buffer, so it waits for them to reach L1. That is why Go takes 3.4× Rust's time on 1.5× the instructions. Filling the element in place removes the temporary and the copy: 534 → 177 ns, within 13% of Rust (M). LLVM's SROA never makes the temporary |
| A4, bounds checks | The prove pass leaves 21 bounds checks in replicate-go3's own `main.go`, 2 of them in the parse loop. Reslicing to remove them (`parseBCE`, 1 check left) changes the time by −8% here, and by +8 to +15% in trial runs before the final one. `-gcflags=all=-B` was worth −1.2% on go3 (go-tuned T2) |
| A5, inlining | gc does not inline `(*ring).store` (cost 104), `(*loop).read` (224), `flush` (226) or `arm` (109), all over the 80 budget. It does inline `header` (34). LLVM with fat LTO also keeps `Loop::read` and `Loop::flush` out of line, but inlines the per-node closure into `run`, which gc can't do through a func value. That is a few ns per loop pass |
| A6, gccgo | Slower than gc everywhere except the byte sum: CRC32C 11× (libgo's `hash/crc32` has only `slicingUpdate`, no SSE4.2 path, because gccgo can't assemble gc's Plan 9 `.s` files); parse 4× and headers 10× (`binary.LittleEndian.Uint32` is a PLT call into `libgo.so`, `encoding_1binary.littleEndian.Uint32@plt`, never inlined); `Syscall` 2.4× (libgo's entersyscall). GCC's backend doesn't help when the hot calls are into a shared runtime it can't inline |

## Conclusions

1. **gc and rustc share no backend.** gc is Go's own SSA compiler with a linear-scan allocator, an 80-node inlining budget, no auto-vectorization, little alias analysis and no SROA past 4 fields or 32 bytes. rustc hands LLVM IR with `noalias` facts to LLVM 23. The LLVM and GCC routes for Go (gollvm, gccgo) run a Go 1.18 front end and runtime without generics and lose gc's assembly fast paths: gccgo 14 is 2 to 11× slower than gc on this path (A6). Neither is an option for a Go data path.
2. **Generated code is not why Go costs more on the write path.** Summed over one replicated write, the compiler differences net out to about zero (M): parse, headers, map and clock add about 0.1 µs, and Go's CRC32C is 38 ns faster per block. Codegen does matter in tight loops. The parse loop takes 3.4× as long, from a stack temporary and store-forwarding stalls (A3, fixed in source by filling the slice element in place). A byte-summing loop takes 4.2× as long, because LLVM vectorizes it and gc doesn't. So for the cleaner and other compute-bound code, the 1.12 to 1.16× in go-cpu-per-op is plausible and source-fixable in hot spots.
3. **About 60% of the gap (~2.9 of 4.8 µs) is the runtime scheduler around a blocking `epoll_wait`**: sysmon retaking Ps, handing them to spinning Ms that find nothing, threads parking and waking in `futex`, and sysmon's 20 µs `nanosleep` loop (P1, P2). **Run more Ps than goroutines that block in syscalls** (`GOMAXPROCS=8` for 4 event loops on 4 vCPUs). That cut `futex` calls per write from 0.21 to 0.02, `nanosleep` from 0.23 to 0.06 to 0.09, migrations by half, and 2.9 µs per write (V). It costs nothing in code. It does give the collector a larger worker pool (25% of Ps) and lets more goroutines run at once, so check it on the real program before adopting it.
4. **About 16% (~0.8 µs) is `entersyscall`/`exitsyscall`**, 64 to 123 ns per call on this VM (M, three times the expected cost). For non-blocking sockets, `syscall.RawSyscall` is safe and removes it (the gc raw call costs the same as libc). Together with P8 it took go3's gap from 9.4 to 2.5 µs (1.26× → 1.07×, V). A blocking call such as `epoll_wait` with a timeout must keep going through `Syscall`, or the P and garbage collection stall behind it.
5. **The published 1.21× was partly the benchmarks.** LLVM deleted two of Rust's three 4K ring copies as dead stores (A1, +1.07 µs once kept). Go paid first-touch page faults on three rings to Rust's one, which in this VM cost up to 11 µs each (A1b). Any benchmark whose output is never read needs `black_box` in Rust (or the equivalent sink in Go). Pre-touch long-lived buffers, or measure past the first pass, before comparing languages in a VM.
6. **With these changes Go is at 1.07× Rust's CPU per replicated write** (go4 raw touch P8). The rest is `read`/`write` instead of `recv`/`send` (36 ns per read call in the kernel, M), the remaining cache effects of goroutines moving between CPUs, and run-to-run noise of the same size. Measure Go runtime costs with `perf`, not pprof: pprof never samples sysmon or parked Ms (`runtime/proc.go:3388`).

Not covered: hardware counters (no PMU in the VM, so store-forwarding stalls and cache misses are inferred from code and timings), gollvm (not reachable), gccgo on the replicate programs (they need Go 1.21+), and the cleaner harness's own assembly.
