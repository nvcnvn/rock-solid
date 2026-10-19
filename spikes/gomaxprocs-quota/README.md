# GOMAXPROCS under a CPU quota spike

**Question.** The daemon runs one event loop per core and sets `GOMAXPROCS` above the number of loops, because with as many busy loops as Ps `sysmon` retakes the P of a loop blocked in `epoll_wait` ([codegen spike](../go-rust-codegen/)). That spike ran without a CFS quota, where an extra P costs nothing. Under a Kubernetes CPU limit, does the extra P let the process run more threads than its quota and get throttled, and should the daemon size its loops to the limit minus one instead? And what does Go 1.27 already do with `GOMAXPROCS` inside a cgroup?

**Setup.** Linux 6.18.44 in a Firecracker VM, 4 vCPUs, Go 1.27.0, cgroup v1 `cpu` controller, `cpu.cfs_period_us` 100,000 (the kubelet's default). Written against commit `676095bf56fdcfc9a0afce42ab45103137501d7f`. The VM was shared with six other agents running benchmarks (load average 5 to 9): the cgroup under test and the load generator each get `cpu.shares` 262144, the maximum, so the others cannot take CPU the quota allows (as on a node whose pods' requests fit its cores), but wall-clock latency still carries their noise. Throttling counts, CPU per operation, the Go scheduler's own latency and the background work done are the reliable columns; p50 and p99 compare configurations only roughly.

- [gen.c](gen.c), outside the quota: creates one socketpair per loop, forks the server into the cgroup, and sends 16-byte messages stamped with `CLOCK_MONOTONIC` round-robin over the loops at a fixed rate (open loop; a full socket is a drop, never a wait).
- [server/main.go](server/main.go), in the cgroup: N loops shaped like the daemon's (a goroutine blocking in `epoll_wait` on its socket, raw `read`, not locked to an OS thread, as the design says), each spending 20 µs of CPU per message and recording latency from the send stamp. Background work stands in for what runs off the loops: a goroutine allocating 64 MB/s of 64-byte pointerful objects over a 32 MB live heap (about one collection every 2 s), and a worker computing in 10 ms bursts every 100 ms (the cleaner). The server reports the CPU it used (`getrusage`), collector cycles and pauses, the background work actually done, and the Go scheduler's runnable-to-running latency (`/sched/latencies:seconds`).
- [run.py](run.py): T0 and T1 read `runtime.GOMAXPROCS(0)` inside the cgroup for a range of quotas. M runs every combination of loops ∈ {limit, limit − 1} and `GOMAXPROCS` ∈ {loops, loops + 1} (one of which is always Go's default), under quotas of 2 and 3 CPUs at loads of 40% and 65% of the quota offered to the loops (25 µs per message including the loop's own cost), and with no quota at 40% of 4 CPUs; 6 s per run, 3 reps, and `cpu.stat` read before and after each.

```
./build.sh                       # bin/gen, bin/server
sudo python3 run.py > results.txt   # about 9 minutes; creates and removes /sys/fs/cgroup/cpu/gomaxprocs-spike{,-gen}
python3 summarize.py             # medians of the M rows as a markdown table
```

Expected before the run: T0 gives `ceil(quota)` with a floor of 2; under a quota, `GOMAXPROCS` = loops + 1 gets throttled in some periods and `GOMAXPROCS` ≤ the quota never does; limit − 1 loops costs a loop's worth of capacity.

## Results

Raw output: [results.txt](results.txt). M rows are medians of 3 runs (`summarize.py`); "background" is the garbage goroutine's allocation rate out of 64 MB/s asked (about 50 MB/s is what it reaches with a P to itself).

| Test | Result |
| --- | --- |
| T0, default `GOMAXPROCS` by quota | 0.5, 1, 1.5, 2 CPUs → 2; 2.01, 2.5, 3 → 3; 3.5, 6, none → 4. **`min(CPUs in the affinity mask, max(2, ceil(quota/period)))`**, as `runtime/cgroup_linux.go` (`defaultGOMAXPROCS`, `adjustCgroupGOMAXPROCS`) says. Go reads only the process's own cgroup, not its ancestors (`internal/runtime/cgroup/cgroup_linux.go`, `OpenCPU`) |
| T1, quota changed while running | Default: 3 → 2 within 1 s of the quota dropping to 1.5 CPUs, → 4 within 1 s of its removal (`sysmon` rereads the limit at most once per second, `runtime/proc.go`, `sysmonUpdateGOMAXPROCS`). With `GOMAXPROCS=3` in the environment it stays 3: an explicit value, from the environment or `runtime.GOMAXPROCS(n)`, turns updates off (`sched.customGOMAXPROCS`) until `runtime.SetDefaultGOMAXPROCS()`; `GODEBUG=updatemaxprocs=0` turns them off too |
| M1, quota 2, 2 loops, `GOMAXPROCS` 3 (above the quota) | **Throttled in 3.3% of periods at 40% load (7 ms per 6 s) and 6.5% at 65% (174 ms per 6 s)**. Background fully served (50 and 48 MB/s, 3 collections), scheduler p99 2.1 and 0.4 ms |
| M2, quota 2, 2 loops, `GOMAXPROCS` 2 (Go's default) | Never throttled, but **background starved: 12 and 6 MB/s, 1 collection instead of 3**, and the loops wait for a P: scheduler p99 12.6 to 14.7 ms, p50 188 µs and 21 ms against 53 µs and 1.4 ms in M1. The lower CPU per message (36.0 against 42.8 µs) is the background work not done |
| M3, quota 2, 1 loop, `GOMAXPROCS` 2 (Go's default) | Never throttled, background served (49 and 50 MB/s), scheduler p99 0.2 to 0.5 ms. **Half the loop capacity: at 65% load one loop dropped 30,055 of 312,000 messages** and p50 was 243 ms |
| M4, quota 2, 1 loop, `GOMAXPROCS` 1 | Worst: background starved (4 to 5 MB/s), scheduler p99 21 to 25 ms, and at 65% load 54,491 drops |
| M5, quota 3, 3 loops, `GOMAXPROCS` 4 | Throttled in 0% of periods at 40% load and 1.6% at 65% (2 ms per 6 s); background served (52 and 46 MB/s); p50 64 µs at 40% |
| M6, quota 3, 3 loops, `GOMAXPROCS` 3 (default) | Never throttled; background starved (21 and 8 MB/s); scheduler p99 7.3 and 14.7 ms |
| M7, quota 3, 2 loops, `GOMAXPROCS` 3 (default) | Never throttled; background served (47 MB/s); scheduler p99 0.8 to 2.1 ms. p50 951 µs at 40% and 70 ms at 65%, where each of the 2 loops runs near 100% |
| M8, no quota, 4 loops, `GOMAXPROCS` 5 against 4 | CPU per message 34.5 against 35.0 µs; scheduler p99 0.46 against 5.2 ms; background 50 against 28 MB/s; p50 137 against 268 µs. The extra P costs nothing without a quota, as in the codegen spike |
| M9, no quota, 3 loops, `GOMAXPROCS` 4 (default) | CPU per message 31.1 µs, scheduler p99 0.46 ms, background 49 MB/s, p50 94 µs |
| All M rows | **No run with `GOMAXPROCS` ≤ the quota was ever throttled (36 runs); runs with `GOMAXPROCS` above it were throttled in 7 of 12.** Collector pauses stayed under 3.2 ms everywhere |

## Conclusions

1. **An extra P is free without a quota and is not free under one.** `GOMAXPROCS` above `ceil(quota)` lets loops, collector workers and background goroutines run on more threads than the quota pays for, and CFS then stalls the whole process until the next 100 ms period: 1.6 to 6.5% of periods at 2 CPUs (M1), up to 1.6% at 3 (M5). With `GOMAXPROCS` at or below the quota it never happened (all M rows).
2. **`GOMAXPROCS` must stay above the loop count anyway**, under a quota or not. With as many Ps as loops, a loop returning from `epoll_wait` waits for a P held by a background goroutine until `sysmon` preempts it (scheduler p99 7 to 25 ms, M2, M4, M6), and background goroutines and the collector starve (4 to 21 MB/s of 50). This is the codegen spike's rule seen from the other side: the spare P serves the loops' wakeups and the work off the loops.
3. **So under a quota, loops = `GOMAXPROCS` − 1 with `GOMAXPROCS` at Go's default, `max(2, ceil(limit))`.** It is the only shape that kept the spare P and was never throttled (M3, M7). The daemon leaves `GOMAXPROCS` to the runtime, so it follows an in-place resize of the limit within a second (T1), while the loop count is fixed at start: a raised limit leaves a second spare P until restart, which costs nothing, and a lowered one can leave no spare P, which the daemon reports until it is restarted. Go does not export the limit, so the daemon reads `cpu.max` (or `cpu.cfs_quota_us`) itself to choose between this rule and the next: comparing `runtime.GOMAXPROCS(0)` with `runtime.NumCPU()` misses a limit equal to the CPU count.
4. **The cost is one core of loop capacity**: half at a 2-CPU limit (M3 dropped messages at 65% load), a third at 3. A daemon that needs the loop capacity of its whole limit is better off with a higher limit, or none, than with an extra P that the quota throttles.
5. **Without a quota the codegen spike's shape stands**: loops = CPUs in the affinity mask and `GOMAXPROCS` = loops + 1, set explicitly. The extra P cost nothing (M8: 34.5 against 35.0 µs of CPU per message) and kept the loops' scheduler p99 at 0.46 ms against 5.2 ms. CPUs − 1 loops (M9) measured the same within this VM's noise, so there is no reason to give up a loop's capacity on a node without a limit.
6. Fractional limits round up: at 2.5 CPUs Go picks 3 Ps and the daemon 2 loops, so the process can still exceed its quota by half a core when everything is busy at once. Integer limits avoid that.
