# Open-loop latency: Rust against Go on the replicated write path

**Question.** At the same offered load, how do p50, p99 and p99.9 latency and server CPU differ between the Rust and the Go (v3, same design) replicated 4K write paths as load rises toward saturation, and where is each one's knee? The [tuned Go spike](../go-tuned/README.md) measured them closed-loop at 16 connections: Rust 22.3 µs of CPU per write at 79,900 writes/s, Go v3 27.1 µs (1.21×) at 70,700 writes/s. A closed loop cannot show a knee, because it lowers its offered load as latency rises.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker) with 4 vCPUs, shared with another agent's benchmarks (every measured run holds a shared `flock`). Go 1.27.0, Rust 1.99.1, gcc 13.3. The container restarted between the two sweeps and came back on a different host: **host A** (first sweep, [results-host-a.txt](results-host-a.txt)) had an Intel Xeon at 2.1 GHz, **host B** (second sweep, [results.txt](results.txt)) an Intel Xeon at 2.8 GHz with three times as much vCPU preemption and about twice the latency at every rate. Numbers are compared only within a host.

- **Servers.** [server-rust](server-rust/) and [server-go](server-go/) are copies of [replicate-rust](../go-cpu-per-op/replicate-rust/) and [replicate-go3](../go-tuned/replicate-go3/): one epoll loop per node, the same protocol, CRC32C check, in-memory log and batching. The primary and both replicas still run in one process over loopback TCP, but the primary serves 16 external client connections on a TCP port instead of an in-process client. Each server prints its op count, process CPU and the largest write buffer it flushed (`max_wbuf`) when the clients close.
  - `drain=1`, the original harnesses: a loop pass reads each readable connection until it would block, growing the 256 KiB read buffer as needed.
  - `drain=0`, added after the first sweep: one read of at most the 256 KiB buffer per connection per pass; level-triggered epoll reports the rest on the next pass. Named `rust-d0` and `go-d0` in the results.
- **Load generator.** [loadgen.c](loadgen/loadgen.c), one C process for both servers, so client cost is identical. Poisson arrivals at a fixed rate, fixed before the run, go out round-robin over 16 pipelined connections whenever they are due, whatever the responses. Latency runs from the scheduled send time, so a late send counts against the request (no coordinated omission). A request due while 16,384 are in flight is dropped and counted. Each point runs 1 s of warm-up and 10 s measured; server CPU is read from `/proc/<pid>/stat` over the measured window, so it excludes the client. It also reports how late its own sends were (`lag_p99_us`) and machine, softirq and steal CPU from `/proc/stat`.
- **Sweeps.** Host A: Rust and Go with drain=1, 10k to 110k writes/s in 14 steps. Host B: all four servers, 10k to 140k in 12 steps. Each point twice, servers interleaved in rotating order. Nothing is pinned: the comparison is of the designs as they would run. A point is rerun if other processes used more than 0.3 cores during it (two reruns on host A, none on host B). TCP retransmissions, zero-window and prune counters from `/proc/net/netstat` are recorded per point.
- **Definitions.** *Full service*: achieved within 2% of offered and nothing dropped. *Knee*: the lowest offered rate whose p99 (or p50) is more than twice its value at 10k.
- **Stand-in limits.** Loopback TCP stands in for the network. The whole cluster shares 4 vCPUs with the load generator, which reaches 0.9 cores of its one thread from 90k up and sends late by milliseconds at 120k and above, so results at 120k and 140k measure the load generator as much as the servers. The hypervisor preempts this VM's vCPUs for 0.5 to 5 ms several times a second even when it is idle ([vcpu-gaps.c](loadgen/vcpu-gaps.c), P0), which puts a floor of about 1 to 2 ms under p99 at every rate.

```
python3 run.py | tee results.txt               # about 25 minutes without lock waits
python3 run.py point go-d0 70                  # one point at 70k writes/s
CPUPROFILE=go.pprof bin/server-go conns=16     # then bin/loadgen port=<printed> pid=<pid> rate=60000
go tool pprof -top bin/server-go go.pprof
```

[results-host-a.txt](results-host-a.txt) was produced by this run.py before the drain option and the 120k/140k rates existed; its servers are today's drain=1.

**Expected outcomes, written before the sweep.**

- E1: at 10k, Go's p50 is 0 to 15 µs above Rust's (queue depth 1 measured +2.6 µs for v3).
- E2: Go's p99 knee comes at a 1.1 to 1.35× lower rate than Rust's, from its primary loop being about 1.2× busier per write.
- E3: Rust serves in full up to 80 to 100k writes/s and Go up to 65 to 85k, a ratio of 1.1 to 1.35. Open loop lets batching grow with load, so both may exceed their closed-loop rates.
- E4: at 30k, Go's server CPU per write is 1.1 to 1.4× Rust's.
- E5: CPU per write falls as load rises in both, since a loop pass batches more frames. At low rates it is well above the closed-loop 22 to 27 µs, which was measured at saturation and included the client.

Smoke runs before the sweep, while building the harness, showed two things the sweep must account for: p99 of 1 to 2 ms already at 30k in both, coming in episodes of 1 to 10 ms (vCPU preemption, see P0), and in 4 of about 10 Go runs at 30k a 2 to 3.5 s stall that dropped requests, together with TCP retransmissions and zero-window probes. Whether those stalls are Go's or came from other load on the machine is not yet known; the sweep records retransmissions and other processes' CPU per point.

**Expected outcomes for the second sweep, written before it** (on the second host, after the first sweep found Go's collapse and its cause): E6, with drain=0 Go no longer collapses below its CPU limit, and its p99 knee and full-service rate come at a 1.1 to 1.35× lower rate than Rust's with drain=0. E1 to E5 are checked again for the original drain=1 servers.

## Results

| Test | Result |
| --- | --- |
| P0: vCPU preemption, one spinning thread on an idle machine for 10 s | Host A: 26 gaps of 0.5 to 5 ms (longest 2.1 ms). Host B: 51 (longest 4.9 ms). Slow requests come in episodes of 1 to 10 ms, about 10 a second, in both servers |
| E1: p50 at 10k, Go minus Rust | Host A **+8 µs** (130 against 122 µs), OK. Host B +40 µs (265 against 225), FAIL. The host B gap was not investigated |
| E2: p99 knee, drain=1 | Host A: Rust 65k, **Go 30k**; host B: Rust 40k, Go 20k. FAIL (2.0 to 2.2×, want 1.1 to 1.35). Go's knee is its collapse (C1), not queueing |
| E3: full service, drain=1 | Host A: **Rust to at least 110k with nothing dropped, Go only to 20k**: Go dropped requests in 14 of 29 runs from 30k up. Host B: Rust to 80k, Go to 40k. FAIL |
| C1: Go's collapse with drain=1 | In a dropping run the Go server goes quiet for 1 to 4 s: clients' sockets to the primary sit in zero-window probing, the netstat counters show zero windows, prunes and 39 to 846 retransmissions, and on host B the primary's buffered writes to a replica reach **81 to 196 MB** (`max_wbuf`; at most 6.6 MB in Go runs without drops). In a separate traced run at 60k the primary goroutine ran **1.08 s without a syscall**, and the CPU profile of another: **61% `runtime.memmove` and 13% `memclrNoHeapPointers`, 85 to 90% of them under `runtime.growslice`**, from `append` onto the replica write buffer. Cause below |
| C2: Rust with drain=1 | The same instability, later and milder: on host B it dropped requests in 7 of 24 runs, all at 90k and above, with `max_wbuf` 42 to 109 MB. Never on host A |
| C3: what C1 is not | Scheduler latency over a whole 11 s trace of a collapsing run was 66 ms in total. GC ran only at start-up. Collapses continued with `GOMAXPROCS=8`, with `GODEBUG=multipathtcp=0` (Go 1.24+ listens with MPTCP by default) and on host B with `GODEBUG=asyncpreemptoff=1` |
| E6: drain=0 (host B) | **Neither collapses**: no drops at all up to 100k, `max_wbuf` at most 20 MB. p99 knee **Rust 60k, Go 50k (1.20×), OK**. Full service: both to 100k (1.00×, FAIL); above that the load generator limits both |
| Latency at the same rate, drain=0 (host B) | 30k: Rust p50 249 / p99 1,386 / p99.9 4,142 µs, Go 303 / 1,742 / 14,844. 50k: Rust 294 / 2,958 / 8,839, Go 363 / 7,446 / 34,092. 70k: Rust 327 / 10,783 / 24,250, Go 485 / 6,222 / 25,370. 100k: **Rust p50 440 / p99 10.6 ms, Go 875 µs / 50 ms**. Go's p50 gap grows from +34 µs at 10k to +435 µs at 100k; p50 knee Rust 120k, Go 80k (1.5×) |
| E4: server CPU per write at 30k | Host A Go 43.8 against Rust 37.9 µs (1.16×), host B 49.5 against 45.0 (1.10×). OK |
| E5: CPU per write against load | Falls in all four, OK. drain=0 on host B: Rust 88 µs at 10k, 45 at 30k, 17.3 at 80k, 13.8 at 100k; Go 110, 49, 18.2 and 14.7 (**1.05 to 1.25×**). Server cores barely move: Rust 1.34 to 1.39 and Go 1.47 to 1.50 from 30k to 100k |
| T1: Rust primary thread's CPU, drain=0 (host B) | **0.90 cores at 30k, 0.97 at 60k, 0.98 at 100k**; each replica 0.19 to 0.22. Go's loops move between threads (six threads at 0.16 to 0.30 cores each), so the same split cannot be read for Go |

**Per offered rate, host B, drain=0** (mean of 2 runs; cores = server CPU / wall time; dropped is the mean of the 2 runs):

| Offered | Rust achieved | Rust p50 / p99 / p99.9 µs | Rust cores | Go achieved | Go p50 / p99 / p99.9 µs | Go cores |
| --- | --- | --- | --- | --- | --- | --- |
| 10k | 9,936 | 236 / 1,594 / 6,830 | 0.88 | 9,936 | 270 / 1,713 / 5,825 | 1.09 |
| 20k | 19,952 | 244 / 1,551 / 3,890 | 1.19 | 19,952 | 277 / 2,706 / 14,978 | 1.35 |
| 30k | 29,966 | 249 / 1,386 / 4,142 | 1.34 | 29,966 | 303 / 1,742 / 14,844 | 1.47 |
| 40k | 39,936 | 278 / 2,406 / 6,188 | 1.38 | 39,936 | 340 / 2,112 / 4,605 | 1.50 |
| 50k | 49,960 | 294 / 2,958 / 8,839 | 1.39 | 49,958 | 363 / 7,446 / 34,092 | 1.49 |
| 60k | 59,942 | 321 / 3,202 / 8,496 | 1.39 | 59,944 | 427 / 3,084 / 11,961 | 1.48 |
| 70k | 69,944 | 327 / 10,783 / 24,250 | 1.38 | 69,982 | 485 / 6,222 / 25,370 | 1.47 |
| 80k | 80,000 | 391 / 39,322 / 62,216 | 1.38 | 80,002 | 649 / 21,430 / 40,962 | 1.46 |
| 90k | 90,014 | 422 / 9,038 / 26,645 | 1.37 | 90,021 | 724 / 31,280 / 44,927 | 1.46 |
| 100k | 99,972 | 440 / 10,635 / 28,421 | 1.38 | 99,878 | 875 / 49,985 / 72,626 | 1.47 |
| 120k | 118,124 (19,618 dropped) | 61,519 / 166,434 / 198,859 | 1.27 | 115,100 (54,052 dropped) | 60,580 / 209,307 / 246,265 | 1.44 |
| 140k | 117,200 (220,686 dropped) | 131,924 / 203,588 / 224,565 | 1.22 | 110,374 (295,964 dropped) | 134,979 / 228,506 / 255,354 | 1.42 |

**Per offered rate, drain=1 (the original harnesses), host A** (same columns):

| Offered | Rust achieved | Rust p50 / p99 / p99.9 µs | Rust cores | Go achieved | Go p50 / p99 / p99.9 µs | Go cores |
| --- | --- | --- | --- | --- | --- | --- |
| 10k | 9,936 | 122 / 1,142 / 13,767 | 0.59 | 9,936 | 130 / 789 / 2,947 | 0.74 |
| 20k | 19,952 | 127 / 955 / 7,359 | 0.93 | 19,952 | 140 / 1,052 / 4,385 | 1.08 |
| 30k | 29,966 | 140 / 1,926 / 5,734 | 1.13 | 29,026 (17,588 dropped) | 155 / 6,404 / 245,166 | 1.27 |
| 40k | 39,936 | 145 / 1,229 / 4,180 | 1.26 | 39,936 | 176 / 3,823 / 35,753 | 1.38 |
| 50k | 49,959 | 162 / 1,829 / 5,161 | 1.32 | 49,958 | 181 / 1,413 / 4,225 | 1.41 |
| 60k | 59,946 | 161 / 1,941 / 12,533 | 1.34 | 48,948 (109,947 dropped) | 192 / 1,944,399 / 1,985,300 | 1.43 |
| 70k | 69,942 | 166 / 1,954 / 7,358 | 1.37 | 62,455 (83,088 dropped) | 203 / 762,477 / 1,241,268 | 1.46 |
| 80k | 80,000 | 185 / 2,172 / 12,273 | 1.35 | 70,333 (112,161 dropped) | 247 / 49,120 / 1,300,820 | 1.49 |
| 90k | 90,013 | 185 / 2,332 / 12,095 | 1.36 | 83,661 (79,878 dropped) | 269 / 331,038 / 385,784 | 1.40 |
| 100k | 99,966 | 211 / 3,077 / 20,793 | 1.35 | 83,766 (178,355 dropped) | 308 / 766,123 / 1,359,076 | 1.43 |
| 110k | 109,942 | 229 / 6,643 / 36,643 | 1.34 | 95,640 (159,340 dropped) | 353 / 587,958 / 947,904 | 1.44 |

Host B's drain=1 table, with the same pattern (Go collapsing from 50k, Rust from 90k), is in [results.txt](results.txt).

**Why Go collapses with drain=1 (C1).** The loop processes every frame it has read before it flushes anything. When a replica falls behind for a moment (a preempted vCPU is enough), the primary's write buffer to it stops draining fully; it is only reset to empty when completely sent (`flush` in [server-go/main.go](server-go/main.go)), so it keeps growing with every write appended. Go grows a slice by allocating a new one, zeroing its tail and copying everything (`runtime/slice.go:178` `growslice`, `:270` `memclrNoHeapPointers`, `:284` `memmove`), by only 1.25× once it is large (`nextslicecap`, `:341`). At tens of MB each growth is a multi-millisecond copy, so a pass gets slower the more it has buffered; meanwhile drain=1 lets it read every client's whole backlog into the same pass. Past a point the pass never catches up: it ran 1.08 s in one trace, the clients' and replicas' receive buffers filled, the kernel pruned and dropped segments, and the retransmission backoff (200 ms doubling) stretched the stall to seconds. Rust's design is the same, and it hit the same runaway on host B above 90k; its growth is cheaper (`Vec` doubles, and glibc `realloc` moves a large mmapped block with `mremap` instead of copying, `malloc/malloc.c` `mremap_chunk`; not measured here), which only moves its threshold. Bounding each pass's reads (drain=0) bounds the buffer, and the instability is gone in both languages (E6).

## Conclusions

1. **Bound the work of an event-loop pass.** Reading each socket until it would block makes a pass's cost grow with the backlog, and a loop whose per-frame cost rises with the batch has no stable overload point: the Go harness collapsed into multi-second stalls from 30k (host A) and 50k (host B), the Rust one above 90k (host B), at 1.4 to 1.6 cores of 4 (C1, C2). With one read of at most 256 KiB per connection per pass, neither dropped a request below the load generator's limit (E6). The production loop needs that bound, plus a cap on queued replication bytes that stops reading clients (backpressure), and write buffers that are compacted or ring-shaped rather than grown.
2. **With the same bounded design, Go's latency knee comes about 1.2× earlier than Rust's, as predicted, but throughput up to 100k is the same** (E6: p99 knee 50k against 60k; both served 100k in full). Above about 60k Go's latency grows faster: p50 2.0× and p99 4.7× Rust's at 100k (875 µs / 50 ms against 440 µs / 10.6 ms). On a shared 4-vCPU VM, p99 below 1 to 2 ms is not measurable for either (P0).
3. **The event loop is work-conserving, so "cores" is not utilization.** Rust's primary thread is 0.90 to 0.98 cores busy from 30k to 100k while CPU per write falls from 45 to 13.8 µs, because batches grow with load (T1, E5). Server cores stay at 1.34 to 1.39 (Rust) and 1.47 to 1.50 (Go) across that range. Capacity planning must use latency against offered rate, not CPU headroom.
4. **Go's CPU cost per write is 1.05 to 1.25× Rust's under open-loop load** (E4, E5), at or below the 1.21× the closed loop measured, and about 0.1 cores more in total.
5. **Do not compare latency across hosts.** The same binaries ran about twice as slow at every rate after the container moved to host B (p50 at 10k 225 against 122 µs for Rust), and host B's Go-minus-Rust p50 at 10k was +40 µs against host A's +8 µs (E1, not explained).

Not covered: per-thread CPU of Go's loops (they move between threads; it would need `runtime.LockOSThread`, which costs more per [go-tuned](../go-tuned/README.md)), servers above about 115k writes/s, which needs a faster or multi-threaded load generator or more cores, a real network, and why host B widened Go's low-load p50 gap.
