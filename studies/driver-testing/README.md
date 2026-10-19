# Driver testing: why `testing/synctest` cannot drive the event loop, and what can

**Status:** read from source, plus a Go experiment run in a Linux container ([bubble/](bubble/), [results](bubble/results.txt)). Not adopted; the proposal is in [Proposed answer](#proposed-answer), and [Where it would apply](#where-it-would-apply) lists the exact changes to the docs.

**Question:** [Testing strategy](../../docs/design.md#testing-strategy) says drivers are tested with Go's `testing/synctest` for fake time. But the phase 5 event loop reads `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW` with raw `clock_gettime` syscalls, sleeps in `epoll_wait` and arms a `timerfd` ([Runtime in Go](../../docs/design.md#runtime-in-go)). Does synctest fake any of that? If not, how should the phase 5 drivers (event loop, clock and timers, io_uring, TCP, round driver; [build checklist](../../docs/build-checklist.md#phase-5-default-drivers)) be tested deterministically, how can `Resumed` and timers on `CLOCK_BOOTTIME` be tested without suspending a machine, and which tests must run against the real kernel?

## Summary

`testing/synctest` runs a test's goroutines in a *bubble* with a fake clock that jumps ahead whenever every goroutine in the bubble is *durably blocked*, meaning blocked on something only another goroutine in the bubble can release: a channel made in the bubble, `time.Sleep`, `sync.Cond`, a bubbled `WaitGroup`. It fakes only Go's `time` package. A raw `clock_gettime` reads the kernel, a `timerfd` counts kernel time, and a goroutine inside a system call is not durably blocked, so the bubble waits for the kernel. The experiment shows all three: inside a bubble an hour of `time.Sleep` passes while `CLOCK_BOOTTIME` moves 18 µs; a goroutine in `epoll_wait` holds a 1 ms fake sleep, and `synctest.Wait`, for the 300 ms of real time until another goroutine wakes it (forever, with no waker); a 1 s `timerfd` reads "not expired" after 2 s of fake time. So synctest cannot drive any of the five phase 5 components. It fits only goroutines off the data path that wait on channels and timers, like a worker's retry loop.

What works is a seam plus a stepper. TigerBeetle and FoundationDB put time behind one interface (`Time`, `src/stdx/time.zig:17-24`; `INetwork`, `flow/include/flow/network.h:184-200`) and step every node from one thread over a simulated network and simulated storage (`src/testing/cluster.zig:571-575`; `fdbrpc/sim2.cpp:1588-1606`). FoundationDB's production loop also shows the order a pass needs: it sleeps, collects what is ready, and only then reads the clock and fires due timers (`flow/Net2.cpp:1616-1638`). For rock-solid the seam is a small kernel interface in `internal/loop` with three calls: wait in `epoll_wait`, read every clock together (`CLOCK_BOOTTIME`, `CLOCK_MONOTONIC_RAW`, `CLOCK_MONOTONIC` and the VMClock marker), and arm the timerfd. The loop exposes `Pass()`, which waits first and reads the clocks after the wait returns, so no event is handled at a time earlier than when it became ready. Two pieces of logic move into the sans-IO core as pure functions: the `Resumed` detector and the timer heap.

The test implementation of the seam (`internal/loop/looptest`) scripts the clocks from the clock model the simulated network specifies. It applies scripted jumps inside `Wait`, as if the loop had slept through them, and polls the real epoll set without blocking. The simulator's scheduler then steps many loops, and their worker jobs, from one goroutine in a seeded order. Unlike TigerBeetle and FoundationDB, this runs over real kernel descriptors. Its determinism rests on AF_UNIX delivering data inside the writer's own call (E5, `net/unix/af_unix.c:2506-2510`), and for a seed it promises the same results and the same event order per loop, not byte-identical traces.

With the seam, a suspend is a scripted jump of `CLOCK_BOOTTIME` against `CLOCK_MONOTONIC`. The kernel changes the gap between those two clocks only when it adds sleep time (`kernel/time/timekeeping.c:267-270`, called only from `:1867`). A test can therefore feed the readings a suspend, a 22.6% slew or a marker change produces, inside a chosen pass's wait, and check that this pass delivers `Resumed` before its reads and fires its overdue timers in deadline order.

The real kernel is still needed for what scripted readings cannot show:
- syscall counts per pass under 1,000 connections, zero allocations and goroutine counts;
- a loop that really blocks and wakes, to catch lost wake-ups;
- false `Resumed` from a stall between two clock reads;
- cgroup sizing, io_uring and TCP behaviour, and the timerfd's clock and absolute deadline.

Those tests assert counts, orderings and lower bounds, never exact timings, as TigerBeetle's IO tests do (`src/io/test.zig:333-336`). Docker covers the cgroup cases: `--cpus=2.5` sets `cpu.max` to `250000 100000` (E6), which needs a Docker daemon rather than root on the host. But Docker's default seccomp profile refuses `io_uring_setup` (E6), so io_uring tests run natively or with `--security-opt seccomp=unconfined`. The VM pause harness runs the hypervisor cases nightly.

## What the design says now

- [Modularity](../../docs/design.md#modularity-sans-io-core-pluggable-drivers), *Deterministic* rule: "Driver tests use `testing/synctest` for fake time."
- [Testing strategy](../../docs/design.md#testing-strategy), layer 1 tools: "Own simulator over the sans-IO core; `testing/synctest` for drivers".
- [Runtime in Go](../../docs/design.md#runtime-in-go): one epoll loop goroutine per core; `epoll_wait` through `unix.Syscall`, non-blocking I/O through `unix.RawSyscall`; a timerfd armed for the earliest deadline in the loop's timer heap; an inbox of a lock-free queue plus an eventfd; no channel, `select` or `time.Timer` on the data path; every syscall through a recording layer that emits a trace record.
- [Lease time](../../docs/design.md#epoch-changes-and-failure-detection): the loop reads `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW` itself, about 140 ns each per pass, and "compares `CLOCK_BOOTTIME` with Go's clock to see a suspend"; it reads the VMClock disruption marker from a mapped page.
- [test-scenarios.md, Clock and timers](../../docs/test-scenarios.md#clock-and-timers) already flags the conflict as an Open line: "the loop reads its clocks with raw syscalls, which synctest does not fake; the loop needs an injectable clock source (and marker page) for its tests, or these cases run only in a VM."
- The [Go CPU per operation spike](../../spikes/go-cpu-per-op/README.md) is where synctest entered the design. It also noted that "scheduling order inside the bubble is still not seeded".

## What synctest fakes, from Go 1.27.0's source

Terms first. A *bubble* is the set of goroutines started inside `synctest.Test`. A goroutine is *durably blocked* when only another goroutine of the same bubble can unblock it. A *syscall* is a call into the Linux kernel. Go makes a syscall either through `syscall.Syscall`, which tells the scheduler the goroutine is in the kernel (its status becomes `_Gsyscall`), or through `RawSyscall`, which tells it nothing (the goroutine stays `_Grunning`). `epoll_wait` puts a thread to sleep until one of a set of file descriptors is ready. A *timerfd* is a file descriptor that becomes readable when a kernel timer expires. An *eventfd* is a counter descriptor that one thread writes to wake another.

| Fact | Source (go1.27.0, `8af2175`) |
| --- | --- |
| Only the `time` package is faked. `time.Now` returns the bubble's fake time, starting at midnight UTC on 2000-01-01, with no monotonic reading. `time.Since` and the timer heap use the same fake nanoseconds | `src/testing/synctest/synctest.go:20-22`; `src/runtime/synctest.go:182`; `src/runtime/time.go:17-29` (`time_runtimeNow`), `:39-44` (`time_runtimeNano`) |
| Time advances only when every goroutine in the bubble is durably blocked | `synctest.go:24-25`; `runtime/synctest.go:132-158` (`maybeWakeLocked` returns nothing while `running > 0`) |
| A goroutine counts as not running only in `_Gwaiting` with a reason from a fixed list: a nil-channel or bubbled-channel operation, `select`, `time.Sleep`, `sync.Cond`, the bubble's `WaitGroup`, `synctest.Wait` | `runtime/synctest.go:43-75` (`changegstatus`); `src/runtime/runtime2.go:1386-1399` (`isIdleInSynctest`) |
| A goroutine in `syscall.Syscall` is `_Gsyscall`, so it still counts as running. One in `RawSyscall` stays `_Grunning`. A goroutine parked on Go's own netpoller (`net.Conn` reads) waits with `waitReasonIOWait`, which is not on the list | `src/runtime/proc.go:4735` (a bubbled goroutine entering a syscall goes through `casgstatus`); `src/runtime/netpoll.go:575`; `runtime2.go:1226` |
| The documentation says the same: mutexes, "blocking on I/O, such as reading from a network socket", and "system calls" are not durably blocking | `synctest.go:86-93` |
| A channel, `time.Timer` or `time.Ticker` made in a bubble may not be used from outside it | `synctest.go:97-99` |

So none of the loop's three time-related operations is faked, and a loop goroutine waiting in `epoll_wait` keeps the whole bubble from ever advancing.

## The experiment

[bubble/bubble_linux_test.go](bubble/bubble_linux_test.go), stdlib only, run by [bubble/run.sh](bubble/run.sh) in `alpine:3` containers on Docker Desktop 29.1.3 (linux/arm64, kernel 6.12.54-linuxkit), Go 1.27.0. Timings vary from run to run. [results.txt](bubble/results.txt):

| Test | What it does | Result |
| --- | --- | --- |
| E1 | In a bubble, `time.Sleep(time.Hour)`, reading `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW` with `RawSyscall(SYS_CLOCK_GETTIME)` before and after, as [spikes/clock-slew/gocost](../../spikes/clock-slew/gocost/) does | Fake time moved 1h0m0s from 2000-01-01T00:00:00Z. `CLOCK_BOOTTIME` moved 18.4 µs and `CLOCK_MONOTONIC_RAW` 18.0 µs |
| E2 | A bubbled goroutine waits in `epoll_pwait` with no timeout on an eventfd. A goroutine outside the bubble writes the eventfd after 300 ms of real time. Meanwhile the test goroutine does `time.Sleep(1ms)`, or `synctest.Wait()` | The 1 ms fake sleep took 306 ms of `CLOCK_BOOTTIME` with `Syscall6`, and 300 ms with `RawSyscall6`; `synctest.Wait` took 307 ms. Without the outside writer they would wait forever (`runtime/synctest.go:133`; not run) |
| E2, side finding | The `RawSyscall6` variant counts how often `epoll_pwait` returns `EINTR` | 62 times in 300 ms: Go's preemption signals (`SIGURG`) interrupt a goroutine that the scheduler thinks is running; what sent this many was not traced. The `Syscall6` variant saw none. This bears out the design's rule that anything that can block goes through `unix.Syscall` |
| E3 | In a bubble, a timerfd on `CLOCK_BOOTTIME` armed for 1 s, then `time.Sleep(2 * time.Second)` and a non-blocking read | `expirations=0`, `EAGAIN`: the kernel timer never saw the fake 2 s |
| E4 | What a bubble is good for: a worker goroutine backs off with `time.Sleep` (10, 20, 40… ms) and after each attempt writes an eventfd with a non-blocking `RawSyscall`, as a worker hands results to a loop's inbox | Attempts at exactly 10, 30, 70, 150, 310 and 630 ms of fake time, the eventfd holds 6, and the test takes 0.00 s of real time. A syscall that returns at once does not hold the bubble |
| E5 | Not a bubble: after a 64-byte write, is the reader ready to an `epoll_wait` with timeout 0, 10,000 times over? | AF_UNIX socketpair 10,000 of 10,000; TCP over loopback 10,000 of 10,000 |
| E6 | Not a bubble: in a default container, with `--cpus=2.5`, and with `--cpus=2.5 --security-opt seccomp=unconfined`, read `/sys/fs/cgroup/cpu.max` and call `io_uring_setup` | `cpu.max` is `max 100000`, then `250000 100000` twice; uid 0 in every container. `io_uring_setup` fails with `EPERM` under Docker's default seccomp profile and succeeds with `seccomp=unconfined` |

E5 is the property the stepped harness in [Proposed answer](#proposed-answer) relies on. For AF_UNIX the kernel guarantees it: `unix_stream_sendmsg` queues the data on the peer and calls its `sk_data_ready` inside the writer's own call (`net/unix/af_unix.c:2393`, `:2506-2510`). For TCP over loopback it held here but is not a guarantee (see [Unverified](#unverified)).

## How other systems test event loops and clocks

### TigerBeetle: a `Time` interface, an `IO` per platform, a simulator that ticks

TigerBeetle (`0450c32`) is a single-threaded replicated database whose replica is written against interfaces for time, storage and the network, and tested in a deterministic simulator called the VOPR.

- **Time is a vtable of two functions.** `Time` holds `monotonic` and `realtime` function pointers (`src/stdx/time.zig:17-24`). `TimeOS` reads `CLOCK_BOOTTIME` on Linux, the same clock rock-solid uses, because `CLOCK_MONOTONIC` excludes suspended time (`:128-142`). It panics if the clock ever goes backwards (`:72-73`). `TimeSim` advances by a fixed duration per `tick()` and adds a modelled offset (linear drift, periodic, step jumps, or noisy) to its real-time clock (`:197-325`).
- **The replica never reads a clock on its own.** It receives a `Time` (`src/vsr/replica.zig:650`), is generic over its `MessageBus` and `Storage` types (`:144-149`), and is driven by `tick()`. The production main loop is two lines: `replica.tick()`, then `io.run_for_ns(tick_ms)` (`src/tigerbeetle/main.zig:582-585`). The simulator ticks every replica's `TimeSim` from one thread, "even if a replica is down or paused" (`src/testing/cluster.zig:571-575`).
- **The real `IO` is tested against the real kernel, with loose timing.** Linux's `IO` is io_uring (`src/io.zig:10-15`), and it carries its own `TimeOS` for `run_for_ns` (`src/io/linux.zig:42`, `:95-124`). Its tests open real sockets and files. The timeout test asserts only a lower bound, `elapsed >= delay`, because "we did observed delays as large as 200ms on CI", and marks the upper bound `maybe` (`src/io/test.zig:333-336`). A separate "very simple mock IO" exists only for storage fuzzing (`src/testing/io.zig:13`). The VOPR replaces storage and the message bus altogether; TigerBeetle never runs real kernel I/O under simulated time.

### FoundationDB: one timer queue, two sources of `now()`

FoundationDB (`487620d`) runs its whole cluster in one simulated process. Every component reaches the network and clock through `INetwork`: `now()`, `timer()` and `delay()` are virtual (`flow/include/flow/network.h:184-200`). Two implementations share one `TaskQueue`, a heap of timers plus a ready queue (`flow/include/flow/TaskQueue.h:32-34`, `:69`, `:77`):

- `Net2`, the production loop, updates `now` from the monotonic clock each pass and calls `taskQueue.processReadyTimers(now)` (`flow/Net2.cpp:216`, `:298`, `:1638`).
- The order matters: `Net2::run` sleeps in `reactor.sleep`, collects ready I/O with `reactor.react()`, and only then calls `updateNow()` and `processReadyTimers(now)` (`flow/Net2.cpp:1616-1638`), and `now()` "only updates in the run loop" (`network.h:186-187`). Time is read after the wait.
- `Sim2`'s `now()` returns a variable (`fdbrpc/sim2.cpp:1240`). Its run loop, when nothing is ready, jumps that variable to the next timer and processes the same queue (`:1588-1606`), as synctest does for goroutines. Even `timer()`, the "real" clock, creeps by seeded random steps toward 0.1 s past the simulated time (`:1243-1245`).

So the timer logic is one piece of code that runs in both, and only the time source and the decision to sleep differ. `Sim2` simulates the network too; FoundationDB never runs real sockets under simulated time.

### gnet: a Go epoll library tested only on the real kernel

gnet (`441457f`) is a Go event-loop library on raw epoll, the closest public analogue to rock-solid's loop. Its poller blocks in `unix.EpollWait` with `msec := -1` (`pkg/netpoll/poller_epoll_default.go:119-121`). Its tick callback runs on a separate goroutine with a `time.Timer` (`eventloop_unix.go:416-438`). Its tests dial real loopback sockets on fixed ports and time with the wall clock. The tick test's check, `dur < 250&time.Millisecond || dur > time.Second`, only logs, and `250&time.Millisecond` is a bitwise AND equal to 64 ns, so the lower bound can never fail (`gnet_test.go:930-938`). Wall-clock assertions on a real loop are weak, and this one is wrong without failing anything.

### Pebble: manual time for logic, synctest only over in-memory files

Pebble (`a7b8321`) uses both techniques, and keeps each to what it can do:

- **A small time interface.** The WAL failover manager reads time through `timeSource` (`now`, `newTicker`), "extracted from CockroachDB's timeutil" (`wal/failover_manager.go:1022-1038`), and its tests drive a `manualTime` that they `advance` (`wal/failover_manager_test.go:54`, `:100`).
- **synctest where goroutines sleep but no I/O blocks.** `TestFailoverManager_Quiesce` runs in a bubble over `vfs.NewMem()` wrapped in `errorfs.RandomLatency`, whose injected latencies are `time.Sleep`s and so become instant fake time (`wal/failover_manager_test.go:576-582`; `vfs/errorfs/latency.go:26`). Six Pebble packages use synctest, none of them over real files or sockets. The delete pacer adds 1 ns to every wait it computes, because a zero wait "will be problematic with synctest (which has exact sleeps)" (`internal/deletepacer/rate_calc.go:174`).
- **Faults through a wrapper, not a mock.** `errorfs.Wrap` takes an `Injector` that picks operations to fail (`vfs/errorfs/errorfs.go:171`, `:291`), and `MemFS.CrashClone` keeps a random subset of unsynced writes (`vfs/mem_fs.go:61`, `:145`). rock-solid's simulated disk already plays this role for the sans-IO core.

### CockroachDB: `TimeSource`, `ManualTime`, and what synctest cost it

CockroachDB (`d30c905`) defines `timeutil.TimeSource` (`Now`, `Since`, `Until`, `NewTimer`, `NewTicker`, `pkg/util/timeutil/time_source.go:12-18`) with a `ManualTime` that tests `Advance` and `AdvanceTo` (`manual_time.go:19`, `:84`, `:110`). Its hybrid logical clock takes a `WallClock` (`pkg/util/hlc/hlc.go:96-99`), and `HybridManualClock` can be moved forward or paused (`:133`, `:195`). It has started using synctest, and every use must go through a wrapper. CockroachDB keeps a pool of stopped `time.Timer`s, and a pooled timer made outside a bubble "would prevent the bubble's fake clock from advancing", so the wrapper turns the pool off while any bubble is active (`pkg/util/timeutil/timer.go:16-28`, `synctest.go:17-24`). Mixing bubbled and unbubbled time has a cost even in ordinary Go code.

### madsim: replacing the C library's clock

madsim, a deterministic simulator for Rust, makes even direct clock reads deterministic by defining its own `clock_gettime` and `gettimeofday` symbols, which take the place of libc's (`madsim/src/sim/time/system_time.rs:40-44`, `:81`). That works only for code that calls libc. rock-solid's loop calls the kernel directly with `RawSyscall`, so there is no symbol to replace.

## Findings

1. **synctest fakes none of the loop's time and waiting.** Raw clock reads see real time (E1), a timerfd counts real time (E3), and a loop goroutine in `epoll_wait` holds the bubble's clock and `synctest.Wait` until the kernel returns, forever if nothing wakes it (E2). That is the runtime's rule (`runtime/synctest.go:132-133`, `proc.go:4735`) and the documented one (`synctest.go:86-93`); the Go issue tracker was not checked for plans to change it.
2. **Mixing bubble time with kernel time is worse than either.** The design has the loop compare `CLOCK_BOOTTIME` with Go's clock (`CLOCK_MONOTONIC`) to detect a suspend. In a bubble Go's clock stands still while code runs and jumps while it sleeps, and `CLOCK_BOOTTIME` does the opposite, so the gap between them moves by however long the test computes or sleeps: real time spent looks like a suspend (a spurious `Resumed`), and a fake sleep looks like a clock running backwards. CockroachDB's timer-pool workaround shows the same kind of leak in ordinary code. The loop should read all its clocks through one place.
3. **What synctest is good for in rock-solid** is goroutines off the data path that block only on channels, `time.Sleep`, `time.Timer`, `sync.Cond` or `context` deadlines, with in-memory I/O such as `net.Pipe`, and that make only syscalls which return at once (E4): worker goroutines' retry and backoff, the CSI server's timeouts, a periodic cgroup-limit check, the daemon's restart and handoff deadlines. A worker whose body makes a blocking syscall (`BLKDISCARD` on older kernels) cannot run in a bubble. Interleavings inside a bubble are not seeded, so those tests assert on outcomes after `synctest.Wait`, not on order.
4. **The loop's deterministic part is most of it, but where the clock is read decides safety.** Of the phase 5 scenarios, the pass order (peer messages before client reads), bounded reads, one `writev` per descriptor, inbox batching, the timer heap and its earliest deadline, `Resumed` detection, round collection and per-round inbox messages are all decisions made from what a pass reads. Only the clock readings, the timerfd and the blocking wait bring in time. TigerBeetle and FoundationDB put exactly that boundary behind an interface (`Time`, `INetwork`), and FoundationDB shares its timer code between production and simulation. Its production loop reads the clock after its wait (`Net2.cpp:1616-1638`), and rock-solid's lease rules need the same: a suspend, a lease expiry or a grant's arrival that happens while the loop sleeps must be seen at a time no earlier than when the loop woke. A loop also has more than a clock to hide: the timerfd's readiness, the trace's ticks, and worker goroutines posting to its inbox at real times all bring in time, so the seam has to cover them (see [Proposed answer](#the-seam-and-the-pass)).
5. **`Resumed` is testable from readings, except for stalls between the reads.** `CLOCK_BOOTTIME − CLOCK_MONOTONIC` changes only when the kernel adds sleep time (`timekeeping.c:267-270`, `:1867`). A slew moves both clocks together, so a scripted pair of readings decides the outcome: a 2 s gain gives one `Resumed`, a 22.6% slew gives none. On a real kernel the two clocks are read at different instants, so a stall between the reads (descheduling, CFS throttling) moves their difference too; scripted readings cannot show that, and a real-kernel test must. The clock model that produces such readings exists as a specification (test-scenarios.md, *Clocks per node* under [Simulated network](../../docs/test-scenarios.md#simulated-network), a phase 3 item); `internal/sim` holds only the disk and the pool today.
6. **The kernel side of timers needs only a static check.** A timerfd made with `CLOCK_BOOTTIME` (`fs/timerfd.c:404-410`) counts suspended time by the kernel's definition, so a timer that fell due during a suspend is due at resume. What the loop must get right is that it creates the timerfd on that clock, arms it with the earliest deadline in absolute ticks, and fires every overdue timer in order once `Now` jumps. The first two are visible on the real kernel without any time passing, in the recording layer's `timerfd_create` and `timerfd_settime` records; `timerfd_gettime` returns only the time remaining (`fs/timerfd.c:553`), so it can bound the deadline but not show it. The third is a stepped test.
7. **Two ways to fake the kernel's clocks were ruled out.** A time namespace (`unshare --time`) can offset `CLOCK_BOOTTIME` against `CLOCK_MONOTONIC`, but its offsets freeze once the first process enters (`kernel/time/namespace.c:237`, `:432-433`), so it cannot simulate a suspend between two passes. Replacing the C library's `clock_gettime`, as madsim does, has no effect on raw syscalls.
8. **Real-kernel tests should assert counts, orderings and lower bounds.** TigerBeetle asserts only `elapsed >= delay` (`src/io/test.zig:333-336`); gnet's two-sided check is wrong and only logs (`gnet_test.go:937-938`). The design's real-kernel checks are already of the right kind: syscalls per pass from the trace, `AllocsPerRun`, `NumGoroutine`.

## Proposed answer

**A kernel seam in `internal/loop` with the clocks read after the wait, two pure functions in the core, a stepped harness on the simulator's scheduler, and three tiers of driver tests, with synctest kept to code off the data path.**

### The seam and the pass

```go
// Ready is one descriptor that epoll_wait reported, with its events.
type Ready struct {
	FD     int32
	Events uint32
}

// Kernel is everything a loop asks of the kernel that involves time or waiting.
type Kernel interface {
	Wait(ready []Ready, block bool) []Ready // epoll_wait; block is the loop's own decision
	Clocks() core.Clocks                    // every clock, read together
	Arm(deadline core.Tick)                 // the loop's timerfd: CLOCK_BOOTTIME, absolute
}

// Pending reports what a loop still has to do, for the stepper and the block decision.
func (l *Loop) Pending() (work bool, next core.Tick, inFlight int)

// Pass waits, reads the clocks, delivers Resumed if due, fires due timers, handles events
// (peer messages before client reads), executes actions and re-arms the timerfd if needed.
func (l *Loop) Pass()

// Run is for { l.Pass() }.
func (l *Loop) Run()
```

`core.Clocks` holds `Boot`, `Raw` and `Mono` ticks (`CLOCK_BOOTTIME`, `CLOCK_MONOTONIC_RAW`, `CLOCK_MONOTONIC`) and the VMClock `Marker` (0 without `/dev/vmclock0`).

These are the rules the loop keeps:

- **The clocks are read after the wait.** A pass waits, then reads its clocks, then delivers `Resumed` if the detector says so, fires every timer due by `Boot`, handles events with peer messages before client reads, executes actions, and arms the timerfd. Every event of a pass is therefore handled at a time no earlier than when it became ready. FoundationDB's `Net2::run` keeps the same order: `reactor.sleep`, `reactor.react()`, then `updateNow()` and `processReadyTimers(now)` (`flow/Net2.cpp:1616-1638`). Reading the clocks before the wait would be unsafe for leases in four ways:
  - A suspend during the wait would surface a pass late, after that pass had served the reads that woke it on the old lease.
  - A lease-expiry timer and a read that became ready together would be handled at a `Now` from before the expiry.
  - A grant's arrival time `r` would be recorded early by the length of the wait. That makes the grantor-rate bound too low, which is the unsafe direction ([Lease time](../../docs/design.md#epoch-changes-and-failure-detection)).
  - A timerfd wake-up would find nothing due and cost an extra pass.
- **Trace records take their tick from the pass's `Clocks`**, never from a clock read of their own.
- **The loop decides when to block.** It passes `block = true` only when `Pending` reports no queued inbox items, no deferred submissions and no unspent budget, and both kernels see that decision.
- **Timers do not depend on the timerfd being ready.** `realKernel.Wait` removes the timerfd from the ready list and drains its expiration count. Every pass fires the heap's timers that are due by `Boot`.
- **`Arm` is called only when the heap's earliest deadline changed** since the last arm, so a pass makes no `timerfd_settime` otherwise.
- **The loop never waits inside `io_uring_enter`.** The io_uring driver runs in event mode, and its completions arrive through its eventfd. A full completion queue is drained within the pass, and a full submission queue defers to the next pass. If the driver ever has to wait in the kernel, that wait joins `Kernel`.
- **`realKernel.Clocks` brackets its reads.** It reads `MONOTONIC`, `BOOTTIME`, `RAW`, `MONOTONIC`, and reads again if the two `MONOTONIC` readings are more than ε apart. Without the bracket, a stall between the reads (descheduling, or the CFS throttling the [GOMAXPROCS quota spike](../../docs/design.md#appendix-gomaxprocs-under-a-cpu-quota-spike) measured) would look like a suspend and end every lease the node holds. The gain threshold must lie above ε and at most 2 s: a pause that matters outlasts 3 s, and the kernel may round a suspend down to whole seconds. `realKernel`'s constructor opens and maps `/dev/vmclock0`.
- **No clock is read in `internal/loop` or `internal/driver/…` outside `realKernel`.** `tools/sansio` gets a rule set for those packages: no `time.Now`, `time.Since`, `time.Sleep`, `time.Timer` or `time.AfterFunc`, no channel operation or `select`, and no `range` over a map. This also settles the existing Open line "no tool enforces this outside sans-IO packages".

Cost: three interface calls per pass, a few ns (an estimate). The clock reads are the two raw syscalls the design already pays for (about 280 ns), plus two cheap `CLOCK_MONOTONIC` reads through the vDSO, the kernel's syscall-free fast path. The timerfd is armed only when the earliest deadline changes, and nothing allocates.

### Two pure functions in the core

The `Resumed` detector, `core.Resumed(prev, cur Clocks, threshold) bool`, and the timer heap (deadlines in, due timers out in deadline order) are sans-IO code in `internal/core`. That gives three things:
- Both get property tests over the clock model directly.
- The simulated network's driver uses the same detector. The simulator then delivers `Resumed` only when the model's `BOOTTIME − MONOTONIC` gain reaches the threshold or the marker changes, which is the loop's rule. Today the simulator's spec delivers it "exactly when a suspend or marker-change fault fires", and that disagrees with a real loop on a suspend shorter than the threshold, or one the kernel rounded down to zero.
- The seam is still needed for the wiring and for the order of wait, clock read and reads, which these functions cannot see.

### The stepped kernel and the stepper

- **`looptest.Kernel`:**
  - `Wait` polls the loop's real epoll set with timeout 0. The set holds no timerfd in this mode.
  - `Clocks` returns scripted readings from the per-node clock model the *Simulated network* item specifies (phase 3), built so that `looptest` can import it alone.
  - `Arm` records the deadline.
  - Scripted events (a time jump, a suspend, a VM pause, a marker change, and the advance to the next deadline) are applied *inside* `Wait`, as if the loop had slept through them. They therefore land between the wait and the clock reading, as in production.
  - When the loop calls `Wait` with `block = true` while `Pending` reports work, the test fails at once. A loop that would sleep with queued inbox items or deferred submissions has lost a wake-up.
- **The stepper is `internal/sim`'s deterministic scheduler** (phase 1), with each loop as an actor, not a second scheduler.
  - Each step is a seeded choice among loops with work and worker jobs.
  - When nothing has work and nothing is in flight, scripted time advances to the earliest `next`, as `Sim2` does (`fdbrpc/sim2.cpp:1593-1606`).
  - Kernel I/O counts as in flight: io_uring submissions not yet completed, and sockets with unsent bytes (`SIOCOUTQ` above 0 on TCP). The stepper waits for it in a real `epoll_wait` with a real-time limit and does not advance scripted time. Kernel I/O therefore takes no scripted time, and a `PeerLost` deadline cannot fire while a segment is still on its way through loopback.
- **Workers run inline.** Cleaner copies, erasure coding and the `BLKDISCARD` worker are stepper actors in this mode. A submitted job is a step, its result is posted to the inbox, and a job not yet run counts as in flight. The real-kernel tier keeps them on goroutines.
- **Connections come from a factory.** The TCP driver takes dial and listen as functions. `looptest` hands out AF_UNIX socketpairs and skips TCP-only socket options; TCP-specific tests use real loopback.
- **Traces name connections by a loop-assigned ID**, not by fd, since fd numbers depend on what else the test process has open.
- **The stepped kernel is a test driver backed by a model**, as the simulated disk is, not a mock with expectations, so CLAUDE.md's "no mocks" holds.

What the stepper guarantees: over socketpairs and eventfds, a seed gives the same results and the same event order per loop. Byte-identical traces, checked with `cmp`, come once the loop and driver packages pass the rule set above and traces carry IDs, not fds. Over io_uring and TCP it gives the same results, bounded counts per pass, and the same order per connection, but pass boundaries can differ.

The stepper differs from production in three ways, and each is covered elsewhere:
- **One goroutine**, so it cannot find races between loops. The inbox gets a stress test under `-race` with real producer goroutines and a consumer loop that really blocks on `realKernel`; every item must be handled within a bound.
- **Scripted clocks**, so it cannot show what the kernel's clocks do. The clock slew and VM pause spikes measured that, the VM tier keeps checking it, and the stall false-positive test below covers the reads.
- **It never sleeps**, so it cannot show a real lost wake-up. The block assertion above and the real-blocking stress cover that.

### Three tiers for the phase 5 items

| Tier | Runs | Covers |
| --- | --- | --- |
| Stepped | Every commit, seeded, in the package tests and the shared suites; io_uring cases need a host or container that allows `io_uring_setup` | Event loop: peer messages before client reads, read budget, one `writev` per descriptor, inbox burst = one eventfd write, output rings, replication cap, the block decision. Clock and timers: every `Resumed` case (suspend, slew, marker change, each row of the VMClock table as a scripted reading) scripted inside a pass's wait, clocks that never go backwards, overdue timers in deadline order after a suspend, equal-deadline tie-break, replaced deadlines, arming only on change. Round driver: every trace check with 1, 2, 4 and 8 producer loops, `HasReady` on marked shards only, the `max_group_commit_bytes` split. TCP: routing by owner map, forwarding, reconnect. Shared ChunkStore and network suites on io_uring and TCP, compared with the model and on counts, not whole traces |
| Real kernel | Every commit in CI, on a Linux host natively, or in a container whose seccomp profile allows `io_uring_*`; the cgroup case needs a Docker daemon (root-equivalent), not root on the host | Trace counts per pass under 1,000 connections. `NumGoroutine` with 1 and 1,000 connections. `AllocsPerRun` of a pass = 0 on `realKernel`. Clock reads about 140 ns each with 0 allocations (benchmark). A loop blocked 200 ms in `epoll_wait` and woken by a socket write hands that `NetRecv` a `Now` at least 200 ms after the previous pass's. A million passes under `docker run --cpus=0.5` beside a CPU hog give no `Resumed`. The timerfd is created on `CLOCK_BOOTTIME`, and each `timerfd_settime` record has `TFD_TIMER_ABSTIME` and an `it_value` equal to the heap's minimum; `timerfd_gettime` returns time remaining (`fs/timerfd.c:553`), so it is used only as a bound, remaining ≤ deadline − a `CLOCK_BOOTTIME` reading taken just before. A real 10 ms timer wakes a blocked loop no earlier than its deadline. `Syscall`/`RawSyscall` tags. Inbox stress under `-race`. Cgroup sizing in `docker run --cpus=2.5`, with the sizing itself a pure function tested on `cpu.max` and `cpu.cfs_quota_us` contents from `testdata`. io_uring on a file-backed disk: a linked write and flush in one `io_uring_enter`, fixed buffers and the `ENOMEM` fallback. TCP errors made on loopback: a reset by closing with `SO_LINGER` 0, an EOF by `shutdown`, a reader that stops reading with a small receive buffer |
| VM, nightly | The [VM pause](../../spikes/vm-pause/) harness (QEMU, Firecracker) | Snapshot restore, live migration and `stop`/`cont` rows of the VMClock table with the real marker; a `date -s` wall-clock step that moves neither clock; device-mapper error and delay targets (root in the guest). The open-loop load test and the durable-write benchmark run on their own hosts, as today |

The drivers' pure pieces get table tests and fuzzing instead of a fake. For io_uring that is encoding an action into submission entries and decoding a completion into a `DiskDone`: an error, a short count, and `-ECANCELED` on the flush linked behind a failed write. For TCP it is the frame codec, which is already required.

One note for the design owner, outside this study: Kubernetes' `RuntimeDefault` seccomp profile may refuse io_uring to the daemon pod as Docker's does. That was not checked.

### Alternatives rejected

| Alternative | Why not |
| --- | --- |
| synctest for drivers, as the design says | It fakes only the `time` package (E1, E3), and a goroutine in `epoll_wait` holds the bubble (E2). It would also need the loop to use Go's timers and channels, which the data-path rules forbid for cost |
| Read time through Go's `time` package in the loop so a bubble fakes it | Go's clock is `CLOCK_MONOTONIC`, neither of the two the leases need; and the loop still waits in `epoll_wait`, so the bubble still could not advance |
| Interpose the syscalls (madsim's symbol replacement, `LD_PRELOAD`, `ptrace` or seccomp user notification) | Raw syscalls bypass libc, so there is nothing to replace; tracing every syscall is slow and its timing is not deterministic |
| Time namespaces to fake `CLOCK_BOOTTIME` | Offsets freeze when the first process enters (namespace.c:432-433), so they cannot model a suspend between passes |
| Run the clock cases only in VMs | Minutes per case, no seed, and no way to put a suspend inside a chosen pass. Kept for the cases that need the real hypervisor |
| Make `internal/loop` itself sans-IO, emitting syscalls as actions | The loop is the driver. A second layer of actions duplicates the recording layer and costs per operation, while only the time and waiting calls need replacing |
| Extract the `Resumed` detector and timer heap as pure functions, with no seam | Adopted alongside the seam, not instead of it. The functions test the decisions, but the order of wait, clock read and reads that the lease rules need, and the wiring around them, can only be tested through the seam |
| Simulate the network and disks fully, as TigerBeetle and FoundationDB do, with no real descriptors | The sans-IO simulator already does that for the protocols. The drivers' own code (the epoll handling, the recording layer, the io_uring ring, socket errors) would then go untested; the stepper exists to run that code |

## What the tests must check

- A stepped run of a seed over socketpairs, run twice, gives the same results and the same event order per loop, with 1 and with 8 loops; once traces carry connection IDs and the loop packages pass the determinism rules, the traces are `cmp`-equal.
- `core.Resumed`, as a property over the clock model: across every seeded slew of up to ±22.6% and drift of up to ±1%, no `Resumed`; across every suspend whose gain reaches the threshold, exactly one; across every marker change, exactly one; a suspend the kernel rounded below the threshold gives none, in the loop and in the simulator alike.
- A suspend scripted inside a pass's `Wait` gives `Resumed` in that same pass, before its `NetRecv` and its client reads, and every timer that fell due fires in that pass in deadline order.
- A client read and a lease-expiry timer that become ready in the same `Wait` are handled after the expiry.
- A loop that calls `Wait` with `block = true` while `Pending` reports work fails the test.
- `ctx.Now` and `ctx.Raw` handed to machines never decrease, even when the script feeds a reading lower than the last. What the loop does then is not in the design yet; TigerBeetle panics (`time.zig:72-73`).
- No `timerfd_settime` in a pass whose earliest deadline did not change.
- `tools/sansio`'s loop rule set finds no clock read, channel, `select` or map `range` in `internal/loop` and `internal/driver/…`.
- `AllocsPerRun` of a pass is 0 on `realKernel` and on the stepped kernel.
- The real-kernel checks of the table: the 200 ms wake-up `Now`, no `Resumed` under throttling, the timerfd's `settime` record and `gettime` bound, inbox stress with a really blocking loop.

## Where it would apply

**design.md**

- [Modularity](../../docs/design.md#modularity-sans-io-core-pluggable-drivers), *Deterministic* rule: delete "Driver tests use `testing/synctest` for fake time." The rule is about sans-IO machines; driver testing moves to Testing strategy.
- [Modularity](../../docs/design.md#modularity-sans-io-core-pluggable-drivers), driver slot table, *Clock and timers* row, *Test driver* column: add that the loop's own tests take the same simulated clocks through its kernel seam.
- [Runtime in Go](../../docs/design.md#runtime-in-go), *Event loops*: a new bullet after "A loop waits only on its epoll set", **Time and waiting go through one seam.** It states five things: the three-method interface and `Pass`; the order (wait, then clocks, then `Resumed`, due timers, peer messages before client reads, actions, arm); that trace ticks come from the pass's clocks; arming only on change; and the bracketed clock read. It ends with the cost. The *Raw syscalls* bullet adds that the loop never waits inside `io_uring_enter`, and *Concurrency outside the loops* adds that workers are stepper actors in tests.
- [Lease time](../../docs/design.md#epoch-changes-and-failure-detection): "compares `CLOCK_BOOTTIME` with Go's clock" becomes "compares `CLOCK_BOOTTIME` with `CLOCK_MONOTONIC`, read together after the pass's wait", with the fact that their difference changes only when the kernel adds sleep time, and the threshold's bounds (above the read bracket's ε, at most 2 s).
- [Package layout](../../docs/design.md#package-layout): the `internal/core` row adds the `Resumed` detector and the timer heap; the `internal/loop` row adds the kernel seam; a new `internal/loop/looptest` row reads "Stepped kernel on scripted clocks, actors for `internal/sim`'s scheduler | No". The `tools/sansio` row adds its rule set for loop and driver packages.
- [Testing strategy](../../docs/design.md#testing-strategy): layer 1's tools become "Own simulator over the sans-IO core; event loops and drivers stepped on scripted clocks by the same scheduler". A short **Driver tests** paragraph after the simulation paragraphs states three things: the three tiers; the stepper's three differences from production and what covers each; and that `testing/synctest` is for goroutines off the data path that wait only on channels and Go timers, never for a loop or a goroutine that blocks in a syscall. The *Simulation faults for placement* sentence on suspends adds a suspend below the `Resumed` threshold.
- [Testing the design decisions](../../docs/design.md#testing-the-design-decisions), *What the runtime does* row: add "a stepped run of a seed gives the same results and per-loop event order", and the loop rule set of `tools/sansio`.
- [Alternatives considered](../../docs/design.md#alternatives-considered): a *Driver test time* row with the choice and the rejected alternatives above, linking this study.
- [Roadmap and open questions](../../docs/design.md#roadmap-and-open-questions): add what the loop does when a clock reading goes backwards.

**build-checklist.md**

- Phase 1, *Deterministic scheduler*: add that it schedules external actors (phase 5's loops and worker jobs) as well as simulated nodes.
- Phase 3, *Simulated network*: add "its per-node clock model can be imported alone, by `internal/loop/looptest`, and it delivers `Resumed` through `core.Resumed`".
- Phase 5, *Event loop*: add "Waiting, the clock reads and timer arming go through the kernel seam, with the clocks read after the wait; `Pass` and `Pending` are exposed, and `looptest` steps many loops from `internal/sim`'s scheduler on scripted clocks; `tools/sansio`'s loop rule set passes". Add to its review criteria: "a seeded stepped run of 8 loops over socketpairs gives the same results and per-loop event order twice; a loop that would block with work pending fails the stepped run".
- Phase 5, *Clock and timers*: add "Tested on scripted clocks through the kernel seam; the `Resumed` detector and timer heap are pure functions in `internal/core`. Review when every Clock and timers scenario passes stepped, the real-kernel checks pass in CI, and the VMClock rows pass nightly in the VM pause harness."
- Phase 5, *io_uring disk driver*: add "Event mode only, never waiting in `io_uring_enter`. Building submission entries from actions and decoding completions into `DiskDone` are pure functions with table tests and fuzzing; the ring itself is tested on the real kernel with a file-backed disk, natively or in a container that allows `io_uring_*`."
- Phase 5, *TCP network driver*: add "Dial and listen are passed in, so tests can use socketpairs; transport errors are produced on loopback (a reset, an EOF, a reader that stops)."
- Phase 5, *Round driver*: "Review with a trace check" becomes "Review with a stepped trace check, with 1, 2, 4 and 8 producer loops"; the latency measurement stays on real loop goroutines.

**test-scenarios.md**

- *Clock and timers*: delete the synctest Open line. Open the section with which tier runs each case: stepped for the slew, `Resumed`, VMClock-marker, never-decreasing and timer cases; real kernel for read cost and allocations, the wake-up `Now`, throttling false positives, and the timerfd's clock and settime record; VM nightly for the hypervisor rows and the `date -s` step. Add the scenarios in [What the tests must check](#what-the-tests-must-check). Replace "Open: the gain threshold" with its bounds, still open within them. Add an Open line: what the loop does when a clock reading goes backwards. "Open: the order of `Resumed` within its pass" is answered: first, after the clock read and before any event.
- *Event loop*: say which review criteria run stepped (pass order, budget, `writev`, inbox burst, block decision) and which on the real kernel (1,000 connections, goroutines, allocations, cgroup sizing in `docker run --cpus=2.5`, open-loop load). Add the stepped scenarios, and inbox stress under `-race` with a really blocking consumer. Replace "Open: no tool enforces this outside sans-IO packages" with the loop rule set. "Open: whether arming many timers in one pass is limited to one `timerfd_settime`" is answered: at most one per pass, and none if the earliest deadline did not change.
- *io_uring disk driver*: add the completion-decoding cases: an error result becomes a `DiskDone` with that error; a flush linked behind a failed write completes with `-ECANCELED`, and the log fails on the write's error; a short count is decoded, so the Open line on its policy has a test to land in. Add "never waits in `io_uring_enter`", and the seccomp requirement for containers.
- *TCP network driver*: add the connection factory and the loopback recipe for each transport error.
- *Round driver*, *Shared ChunkStore suite*, *Shared network suite*: their trace checks and suites run stepped. On io_uring and TCP they compare results with the model and check counts, not whole traces.
- *Simulated network* (phase 3): "delivers `Resumed` exactly when a suspend or marker-change fault fires" becomes "delivers `Resumed` when `core.Resumed` reports it from the model's readings". Add a suspend shorter than the threshold to *Placement faults and invariants*.

**glossary.md**: entries for *kernel seam*, *stepped loop* and *synctest bubble*.

**Stale once the design changes, not edited here:** `blogs/11-runtime-in-go.md:412` and `blogs/12-testing-for-trust.md:122` say drivers are tested with synctest.

## Unverified

- **The harness is a design, not code.** Nothing in `looptest` was built. The rules above (clocks after the wait, the block assertion, workers as actors) come from reading the design and the precedents, not from running a loop.
- **One platform.** The experiment ran on linux/arm64 in Docker Desktop's VM (kernel 6.12.54-linuxkit, Docker 29.1.3), not on amd64 or bare metal. The synctest behaviour is in architecture-independent runtime code. Timings vary between runs: a rerun by the reviewer saw E1 at 24 µs, 67 `EINTR`s, and waits of 301 to 311 ms.
- **TCP loopback readiness.** E5 saw a TCP loopback write ready to a zero-timeout `epoll_wait` 10,000 times in 10,000, on an idle VM. Loopback delivery runs in a softirq, the kernel's deferred-work context, which the kernel can push to its `ksoftirqd` thread under load, so this is not guaranteed; the kernel source for it was not read. That is why the stepper treats unsent socket bytes as in flight.
- **The `EINTR` source.** Go's preemption signals (`SIGURG`) interrupt a blocking `RawSyscall`, but about 62 in 300 ms is more than the 10 ms preemption period (`proc.go:6679`) explains, and what sent the rest was not traced.
- **Seam cost** (a few ns per pass) and **ε** for the clock bracket were not measured.
- **synctest over gRPC.** That an in-memory gRPC listener runs in a bubble was not tried; gRPC starts goroutines and timers of its own.
- **Go's plans.** That synctest will keep treating syscalls as not durably blocking is the documented rule (`synctest.go:86-93`); the Go issue tracker was not checked.
- **Seccomp elsewhere.** Docker's default profile refusing `io_uring_setup` was measured (E6); containerd's and Kubernetes' `RuntimeDefault` were not. Docker with cgroup v1 would expose `cpu.cfs_quota_us` instead of `cpu.max`.
- **Suspend to RAM in the VM harness.** Whether QEMU's q35 machine can suspend a guest and wake it from QMP (its S3 state) in the existing harness was not tried; the stepped tier covers the logic either way.
- **Pebble and CockroachDB** were read, not run.
- **io_uring link cancellation.** That a flush linked behind a failed write completes with `-ECANCELED` is from the `io_uring_enter(2)` manual page, not from kernel source.

## Reproduce

- `./fetch.sh [dir]` downloads every cited file at its pinned commit into `dir` (default `./work`) and prints the cited lines. Needs `curl`.
- `bubble/run.sh > bubble/results.txt` builds the experiment for Linux with Go 1.27 and runs it, natively on Linux, or elsewhere in an `alpine:3` container followed by two more runs of E6 with `--cpus=2.5` and with `--cpus=2.5 --security-opt seccomp=unconfined` (needs `docker`). Natively, E6 reports the host's own cgroup and seccomp state. It takes a few seconds. The experiment is its own module (`bubble/go.mod`), so the repository's `go vet ./...` does not build it; `scripts/lint.sh` does run `gofmt -l` on it, since it checks every Go file outside `spikes/`.

## Sources

- Go 1.27.0, `golang/go` at [`8af2175`](https://github.com/golang/go/tree/8af21751f066eced273ca3ce49506b366847c623): [`src/testing/synctest/synctest.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/testing/synctest/synctest.go), [`src/runtime/synctest.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/runtime/synctest.go), [`src/runtime/time.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/runtime/time.go), [`src/runtime/runtime2.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/runtime/runtime2.go), [`src/runtime/proc.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/runtime/proc.go), [`src/runtime/netpoll.go`](https://github.com/golang/go/blob/8af21751f066eced273ca3ce49506b366847c623/src/runtime/netpoll.go). The local toolchain's GOROOT matches these files byte for byte.
- TigerBeetle, `tigerbeetle/tigerbeetle` at [`0450c32`](https://github.com/tigerbeetle/tigerbeetle/tree/0450c32b2c3c3ab6114f3402cc601341cc8192aa): [`src/stdx/time.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/stdx/time.zig), [`src/io.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/io.zig), [`src/io/linux.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/io/linux.zig), [`src/io/test.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/io/test.zig), [`src/testing/io.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/testing/io.zig), [`src/testing/cluster.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/testing/cluster.zig), [`src/tigerbeetle/main.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/tigerbeetle/main.zig), [`src/vsr/replica.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/replica.zig).
- FoundationDB, `apple/foundationdb` at [`487620d`](https://github.com/apple/foundationdb/tree/487620d5aa41152b1f034f4e932005a87eb2947f): [`flow/include/flow/network.h`](https://github.com/apple/foundationdb/blob/487620d5aa41152b1f034f4e932005a87eb2947f/flow/include/flow/network.h), [`flow/include/flow/TaskQueue.h`](https://github.com/apple/foundationdb/blob/487620d5aa41152b1f034f4e932005a87eb2947f/flow/include/flow/TaskQueue.h), [`flow/Net2.cpp`](https://github.com/apple/foundationdb/blob/487620d5aa41152b1f034f4e932005a87eb2947f/flow/Net2.cpp), [`fdbrpc/sim2.cpp`](https://github.com/apple/foundationdb/blob/487620d5aa41152b1f034f4e932005a87eb2947f/fdbrpc/sim2.cpp).
- gnet, `panjf2000/gnet` at [`441457f`](https://github.com/panjf2000/gnet/tree/441457f07983e2e2fbc12a33c05fa68ffdc5ee47): [`pkg/netpoll/poller_epoll_default.go`](https://github.com/panjf2000/gnet/blob/441457f07983e2e2fbc12a33c05fa68ffdc5ee47/pkg/netpoll/poller_epoll_default.go), [`eventloop_unix.go`](https://github.com/panjf2000/gnet/blob/441457f07983e2e2fbc12a33c05fa68ffdc5ee47/eventloop_unix.go), [`gnet_test.go`](https://github.com/panjf2000/gnet/blob/441457f07983e2e2fbc12a33c05fa68ffdc5ee47/gnet_test.go).
- Pebble, `cockroachdb/pebble` at [`a7b8321`](https://github.com/cockroachdb/pebble/tree/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23): [`wal/failover_manager.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/wal/failover_manager.go), [`wal/failover_manager_test.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/wal/failover_manager_test.go), [`internal/deletepacer/rate_calc.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/internal/deletepacer/rate_calc.go), [`vfs/errorfs/errorfs.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/vfs/errorfs/errorfs.go), [`vfs/errorfs/latency.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/vfs/errorfs/latency.go), [`vfs/mem_fs.go`](https://github.com/cockroachdb/pebble/blob/a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23/vfs/mem_fs.go).
- CockroachDB, `cockroachdb/cockroach` at [`d30c905`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff): [`pkg/util/timeutil/time_source.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/util/timeutil/time_source.go), [`pkg/util/timeutil/manual_time.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/util/timeutil/manual_time.go), [`pkg/util/timeutil/timer.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/util/timeutil/timer.go), [`pkg/util/timeutil/synctest.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/util/timeutil/synctest.go), [`pkg/util/hlc/hlc.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/util/hlc/hlc.go).
- madsim, `madsim-rs/madsim` at [`519950e`](https://github.com/madsim-rs/madsim/tree/519950efb4711464f300ed7edf2967ed62d5f502): [`madsim/src/sim/time/system_time.rs`](https://github.com/madsim-rs/madsim/blob/519950efb4711464f300ed7edf2967ed62d5f502/madsim/src/sim/time/system_time.rs).
- Linux v6.18, `torvalds/linux` at [`7d0a66e`](https://github.com/torvalds/linux/tree/7d0a66e4bb9081d75c82ec4957c50034cb0ea449): [`kernel/time/namespace.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/kernel/time/namespace.c), [`kernel/time/timekeeping.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/kernel/time/timekeeping.c), [`fs/timerfd.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/fs/timerfd.c), [`net/unix/af_unix.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/net/unix/af_unix.c).
