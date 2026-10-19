# Lease margins: the startup window and the margin-widening rule

**Status:** read from source (Linux v6.18, CockroachDB `d30c905`) plus a model script ([margins.py](margins.py), output in [results.txt](results.txt)); no experiment on hardware. Revised after an independent review (grantor boot key, sessions, Raft wording, model variants). The [proposal](#proposal) is adopted in the design ([Lease time](../../docs/design.md#epoch-changes-and-failure-detection), [Sessions and fencing](../../docs/design.md#sessions-and-fencing), [leader leases](../../docs/design.md#metadata-layer-metastore)), with the rate-rise residual as an open question; the alternative of a grantor recounting its grants after a VMClock marker change is left in that question, not adopted.

**Question:** A lease holder ends its lease `lease_drift` (1%) early on the faster of its two clocks, the grantor keeps it on the slower of its two, and a holder widens its margin for a grantor whose clock it has measured to run fast ([Lease time](../../docs/design.md#epoch-changes-and-failure-detection)). Two things are unsettled:

1. **The startup window.** Until a pair of nodes has grants a whole `rate_window` (10 s) apart, the holder has no rate bound and its margin is `lease_drift` alone. If both clocks of each node are miscalibrated the same way, the holder's slow and the grantor's fast, a 4 s lease can outlast its grant: about 39 ms with ±1% on each side ([test-scenarios.md](../../docs/test-scenarios.md#epoch-change-model), "Open (startup window)"). Is that real, what does Linux promise about boot calibration, how does CockroachDB handle startup, and how should the design close it?
2. **The widening rule.** A holder whose bound for a grantor exceeds `1 + lease_drift / 2` widens its margin "by the excess": 1.1% for a grantor 0.6% fast, where the [clock slew spike](../../spikes/clock-slew/README.md) expected 1.6%. What margin is actually needed, and is the rule safe, too conservative, or unsafe?

## Summary

Terms first. A *lease* here is a promise between two nodes: the *grantor* promises not to help another node take over for a while, so the *holder* can serve reads alone. Each side counts the while on its own clocks. `CLOCK_BOOTTIME` is the clock a time daemon (chrony, ntpd, systemd-timesyncd) may speed up or slow down by up to 22.6%; `CLOCK_MONOTONIC_RAW` (*raw* below) ignores the daemon and runs at whatever rate the kernel worked out for the CPU's timestamp counter at boot (its *calibration*). The *margin* is how much earlier than the grantor the holder stops. The *rate bound* `B` is an upper limit, measured from grant round trips, on how fast the grantor's raw clock runs compared with the holder's.

**The widening rule is safe, and its extra half percent is useful headroom.** Since the holder stops on its faster clock and the grantor on its slower one, the worst case is when both of the holder's clocks are as slow as its raw clock and both of the grantor's as fast as its raw one, so only the ratio of the raw clocks, ρ, matters. The lease ends first exactly when `1 − m ≤ 1/ρ`, so the least safe margin is `1 − 1/ρ`, and with `B ≥ ρ` the margin `1 − 1/B` is enough. The design's rule gives `lease_drift + (B − 1.005)`, which is always at least `lease_drift / 2` (0.5%, 20 ms of a 4 s lease) more than needed: for a grantor 0.6% fast, 0.596% is needed, 1.1% is given, and lease_drift alone already covered it. That half percent is not waste. It is the only thing covering a change in the pair's rate after it was measured, such as the kernel switching clocksource. The spike README's 1.6% adds `lease_drift` to the whole ratio and so counts it twice. Keep the design's rule and write down why.

**But the step check undoes it.** The check that catches a hidden VM pause ends every lease when the grantor's raw clock moved more than `1 + lease_drift / 2` times the holder's since the last grant, then restarts the rate window. That is the same threshold the widening rule starts at. So any grantor more than about 0.5% fast trips the check on every heartbeat, the window never fills, the bound never forms, and the margin stays at `lease_drift` alone, not for 10 s but for as long as the pair runs. In the model, a grantor 2% fast trips the check on 59 of 59 heartbeats and a partition leaves the lease 38 ms past its grant; at 4%, 114 ms ([results.txt](results.txt), part C). The test-scenario rows for a 1% fast grantor (margin 1.503%) and the full-window row (margin 2.52%) are unreachable as the design is written. Client sessions count their leases by the same rules, so a client whose control-plane leader runs more than about 0.5% fast loses its session lease at every renewal and stalls its writes about once a second.

**The startup window is real, and calibration gives no bound to rely on.** On Intel CPUs that report their crystal frequency, Linux trusts the reported number outright and skips the check against another timer (`arch/x86/kernel/tsc.c:693-699`, `:1424-1437`). A wrong entry in the kernel's own crystal table ran Skylake-X workstations 4.0% slow until a fix in December 2017 (later in distribution kernels), and the same fix notes servers whose spread-spectrum circuit lowers the crystal by about 0.25% (Linux commit `b511203093`). Without the reported number, the kernel measures against other timers and accepts the result within 1% (`tsc.c:1381-1383`); an operator's `tsc_early_khz=` more than 1% off stays (`tsc.c:70-74`, `:1454-1461`). Virtual machines inherit their host's calibration exactly (`arch/x86/kernel/kvmclock.c:117-121`). Both of a node's clocks erring together needs no missing daemon: ntpd and timesyncd correct at most 500 ppm of frequency and step the rest, which leaves `CLOCK_BOOTTIME` at the raw clock's rate. Errors over 1% are rare, but nothing rules them out, and a margin cannot be sized for an unbounded error.

**CockroachDB checks its clock assumption before a connection is ever used.** Its Store Liveness needs no rate at all, since support expirations are hybrid-logical-clock timestamps from the requester's clock (`pkg/kv/kvserver/storeliveness/doc.go:43`, `:127-133`). Its one clock bound, the maximum offset, is checked on the first heartbeat of every connection, before the connection resolves, and a server dies on a violation (`pkg/rpc/connection.go:77-79`, `pkg/rpc/peer.go:296-297`, `:517-525`, `pkg/server/server.go:355`). One round trip measures an offset; a rate needs two, separated in time.

**Proposal.** (1) Keep the widening rule. (2) Tie the step check to the margin in force: trip when `g − g_a > (r − s_a) / (1 − m)`, so it fires exactly when the grantor's clock ran faster than this lease's margin covers. (3) Keep the rate window per grantor node and boot (grants carry the grantor's boot ID, since the raw clock restarts at every boot), and take no lease from a grantor without a bound for its current boot. After the first grant of a window arrives, ask again `rate_gap` (a constant, 100 ms) later, and bound from the window's grants once they span `rate_gap`. Cost: about 100 ms without reads on that grantor's lease after the holder starts, after the grantor reboots, and after every pause detection (today one round trip); a shorter first lease while the window fills (3.90 s instead of 3.96 s at 1 ms round trips, 3.18 s at 10 ms); no visible cost to sessions or to Raft leader failover. In the model this makes every pair safe from 1.0 to 1.04 with no step-check trips and at least 20 ms of headroom, including across a grantor reboot.

## What the design says now

- **Counting** ([Lease time](../../docs/design.md#epoch-changes-and-failure-detection)). The holder ends the lease when either clock shows `primary_lease × (1 − lease_drift)` since it asked; the grantor keeps it until both show `primary_lease` since it granted. `primary_lease` is 4 s, `lease_drift` 1%.
- **The rate bound.** A grant carries the grantor's raw reading `g`; the holder notes its own raw time `s` when it sent the request and `r` when the grant arrived. Between grants `a` and `b`, `B = (g_b − g_a) / (s_b − r_a)` bounds the grantor's raw rate over the holder's from above, whatever the network delays. Over `rate_window` (10 s) the holder pairs the shortest round trip of the first third with that of the last third.
- **Widening.** A holder whose bound exceeds `1 + lease_drift / 2` widens its margin for that grantor by the excess and reports the pair. Until a pair has grants a whole window apart, the margin is `lease_drift` alone.
- **Step check.** Every grantor heartbeat carries `g`. If `g − g_a > (r − s_a)(1 + lease_drift / 2)`, the holder lost time (its VM was stopped) and ends its leases. After that, or after `Resumed`, it asks every grantor again at once and restarts its rate window, "so a pause costs one round trip of reads".
- **Restart wait.** A node that restarts grants no vote and joins no peering for one `primary_lease`.
- **Test scenarios.** [Peering and read leases](../../docs/test-scenarios.md#peering-and-read-leases) expects 1.503% for a grantor 1% fast and 1.103% for 0.6% fast, 2.52% for the ±1% pair after a full window, and a 39 ms overlap within the first window (an Open line). The [Epoch change model](../../docs/test-scenarios.md#epoch-change-model) gives each node a raw clock drifting by up to ±1% and says TLC will find the startup overlap unless the design closes it. The clock slew spike's README expected 1.6% for 0.6% fast (`spikes/clock-slew/README.md:176`).

## What Linux promises about the raw clock's rate

`CLOCK_MONOTONIC_RAW` counts the active clocksource's cycles times the multiplier set when that clocksource was installed (`kernel/time/timekeeping.c:367`), and nothing in `adjtimex` touches it. On x86 the clocksource is the TSC (timestamp counter, a register the CPU increments at a fixed rate), and the multiplier comes from `tsc_khz`, the TSC frequency the kernel decides at boot. How it decides, at v6.18:

| Path | When it is taken | What the kernel checks |
| --- | --- | --- |
| CPUID leaf 0x15 reports the crystal frequency | Intel CPUs that report it (`tsc.c:682`); Denverton hardcoded to 25 MHz (`:685-691`) | Nothing. "The most accurate one so far we have" (`:693-699`) sets `TSC_KNOWN_FREQ`, which skips the refined calibration (`:1424-1437`). A deviation over 500 ppm is only printed, and only with `tsc=recalibrate` (`:1362-1376`) |
| CPUID 0x15 ratio, crystal derived from leaf 0x16's base MHz | Skylake and Kaby Lake clients, which report no crystal (`:702-712`) | Not marked known, so it is refined, below |
| MSR tables (Atom SoCs) | `tsc_msr.c` | Marked known and reliable. The kernel's comment: the SDM's rounded frequencies "lead to clock drift" (`tsc_msr.c:21-35`), about 400 ppm for 83.3 against 83.333 MHz, since replaced by exact ratios |
| Quick calibration against the PIT, then refined over about a second against the HPET or ACPI PM timer | AMD (the CPUID path returns 0 for non-Intel, `tsc.c:668`) and Intel CPUs without CPUID frequencies | The quick one aims for 500 ppm (`:559-565`); the refined result replaces it only if within 1%, otherwise the quick one stays (`:1303-1314`, `:1381-1383`) |
| `tsc_early_khz=` boot parameter | Set by the operator | Used instead of the platform calibration (`tsc.c:70-74`, `:1454-1461`), so not marked known; the refined calibration then replaces it only if within 1%, so a value more than 1% off stays (`:1381-1383`) |
| Clocksource watchdog | Systems not exempt: the TSC is not watched where it is constant, non-stop, has `TSC_ADJUST` and there are at most four sockets (`:1254-1265`), nor where it is marked reliable | Every 0.5 s compares the TSC with the HPET or ACPI PM timer, both converted by their own calibrated multipliers, and marks the TSC unstable past a skew of at least 500 ppm (`clocksource.c:113`, `:127-150`, `:516-522`, `:1177-1196`). The kernel then switches clocksource, which installs a new raw multiplier (`timekeeping.c:1606`): the raw clock's rate changes while the node runs |

What has gone wrong in practice:

- **4%.** Linux commit `b511203093` (Len Brown, 22 December 2017, "x86/tsc: Fix erroneous TSC rate on Skylake Xeon"): the hardcoded 25 MHz crystal for `INTEL_FAM6_SKYLAKE_X` gave "a -4.0% time drift rate on SKX workstations", which use a 24 MHz crystal. Every kernel from the commit it fixes, `6baf3d61821f`, to this fix ran those machines' raw clocks 4% slow. The clocksource watchdog did not catch it: its threshold was then `NSEC_PER_SEC >> 4`, 62.5 ms per 0.5 s or 12.5% (`kernel/time/clocksource.c:129` at `b511203093`).
- **0.25%.** The same commit: "SKX servers subject the crystal to an EMI reduction circuit that reduces its actual frequency by (approximately) -0.25%", about 1 s per 10 minutes. A spread-spectrum clock (one whose frequency is swept slightly to spread electromagnetic noise) lowers the average rate, and a frequency the CPU reports does not include it.
- **Measured here.** One laptop's CPUID frequency was within tens of ppm of NTP time on two boots ([clock slew spike](../../spikes/clock-slew/README.md), L1-L4).

**Virtual machines inherit the host's calibration.** KVM guests take the TSC frequency from the paravirtual clock page the host fills from its own `tsc_khz` and mark it known, so they never refine it (`kvmclock.c:117-121`, `pvclock.c:27-37`, `arch/x86/kvm/x86.c:3332`, `:3374`); kvm-clock itself counts "with the frequency of the raw clock" of the host (`x86.c:2389`). Firecracker is KVM. Hyper-V guests read the frequency from a hypervisor MSR and mark it known (`arch/x86/kernel/cpu/mshyperv.c:355`, `:480-482`), and VMware guests from the hypervisor's CPUID leaf (`arch/x86/kernel/cpu/vmware.c:391-392`). In the [VM pause spike](../../spikes/vm-pause/README.md)'s guests (QEMU with KVM, with and without `+invtsc`, and Firecracker), the guest's TSC ran within 0.5 ppm of the host's 3,187.2 MHz against the guest's raw clock (`results-*.txt`, line `tsc: ... Hz against CLOCK_MONOTONIC_RAW`). That shows the guest uses the host's figure, not that the figure is right. So two VMs on one host agree, and VMs on different hosts differ by their hosts' errors. A live migration moves a guest onto another host's calibration.

**The time daemon does not save the startup case either.** A node's `CLOCK_BOOTTIME` differs from its raw clock only by what a daemon adds. chrony steers rate through the tick (±10%) and would correct a 4% error; ntpd and systemd-timesyncd steer only the frequency word, which `MAXFREQ` caps at 500 ppm, and step the clock when the offset grows past 128 ms or 0.4 s (clock slew spike, S4, S11, S12). A step leaves `CLOCK_BOOTTIME`'s rate alone (S9). So under ntpd or timesyncd, an error of a few percent shows in both clocks, and "both clocks err the same way" needs no missing daemon.

**Verdict.** The kernel bounds the raw clock's rate only on paths that check against another timer (1%, or about 500 ppm under the watchdog), and on the paths that trust a reported frequency, which most current Intel machines (CPUID) and every KVM, Hyper-V and VMware guest (the hypervisor's figure) take, it bounds nothing. Errors over 1% are rare but have shipped, so the design cannot assume them away. A margin sized for some error (5%? 10%?) only moves the line.

## CockroachDB

Read at `cockroachdb/cockroach` master `d30c905` (2026-09-23).

- **No rate, so no startup window for one.** Store Liveness, under its leader leases, takes support expirations "from the requester's clock" as hybrid logical clock timestamps (`pkg/kv/kvserver/storeliveness/doc.go:43`), and "clock skew and drift do not affect the correctness of store liveness" as long as each message forwards the receiver's clock (`doc.go:127-133`). A store heartbeats every 1 s for 3 s of support (`pkg/base/config.go:254`, `:259`). The [clock skew study](../clock-skew/README.md) covers why rock-solid counts durations instead.
- **Its clock bound is checked before first use.** Each RPC connection runs one heartbeat before it can carry anything: the connection's future "is signaled with success ... once the initial heartbeat succeeds" (`pkg/rpc/connection.go:77-79`; `pkg/rpc/peer.go:296-297`). That heartbeat measures the peer's offset with Cristian's method, uncertainty half the round trip, ignoring pings slower than twice the tolerated offset (`pkg/rpc/clock_offset.go:535-548`), and checks it against the majority rule (`clock_offset.go:467`), counting ambiguous samples as healthy (`:442`). On a violation the node dies, since servers set `FatalOnOffsetViolation` (`peer.go:517-525`; `pkg/server/server.go:355`). A node with no peers yet has nothing to compare (`numClocks > 0`, `:467`).
- **Restart.** A restarting store forwards its clock past the largest support it ever withdrew, sleeps until the largest expiration it ever requested, both persisted (`pkg/kv/kvserver/storeliveness/support_manager.go:244-251`), then waits a grace period before withdrawing anyone's support, "to prevent support churn until the first heartbeats are delivered" (`config.go:40-43`; `support_manager.go:257`, `:381`). The server also waits up to `MaxOffset` before serving, so that its timestamps stay monotonic (`pkg/server/clock_monotonicity.go:123-130`).

What carries over: CockroachDB verifies its clock premise per connection, before the first use, at the cost of one round trip, and does not let a connection run on an unchecked premise for a window. rock-solid's premise is a rate, and the cheapest check of a rate is two round trips separated by a gap. That is proposal 3.

## Findings

### 1. The margin a lease needs

Real time `t`. The holder's raw clock runs at `h` times real time, its `CLOCK_BOOTTIME` at `h'`; the grantor's at `g` and `g'`. The holder asks at 0 and stops when its faster clock shows `L(1 − m)`, at `L(1 − m) / max(h, h')`. The grantor grants at some `d ≥ 0` and stops when its slower clock shows `L`, at `d + L / min(g, g')`. The lease ends first when

```text
(1 − m) / max(h, h')  ≤  1 / min(g, g')        (worst case d = 0)
```

Since `max(h, h') ≥ h` and `min(g, g') ≤ g`, it is enough that `1 − m ≤ h / g = 1/ρ`, with ρ the grantor's raw rate over the holder's; and it is also necessary, because a holder whose `CLOCK_BOOTTIME` is no faster than its raw clock and a grantor whose `CLOCK_BOOTTIME` is no slower than its raw one, as with no daemon, reach it. No slew of either `CLOCK_BOOTTIME` can make it worse. So:

```text
least safe margin = 1 − 1/ρ        safe for every ρ ≤ B when  m ≥ 1 − 1/B
```

`1 − 1/ρ` is a little less than `ρ − 1` (0.596% for 1.006, 3.85% for 1.04). `lease_drift` alone covers ρ up to `1 / (1 − lease_drift)` = 1.0101.

| Rule, with `B` the bound | Margin at B = 1.006 | Least headroom for ρ in 1 to 1.2 | Verdict |
| --- | --- | --- | --- |
| Needed: `1 − 1/B` | 0.596% | 0 | |
| Design: `lease_drift + max(0, B − (1 + lease_drift / 2))` | 1.1% | **0.50%** at every B: `0.005 + (B − 1)² / B` above 1.005, `lease_drift − (1 − 1/B)` (0.5025% to 1%) below | Safe, with uniform headroom |
| Spike README: `lease_drift + (B − 1)` once `B > 1.005` | 1.6% | 0.50%, jumping to 1% at the threshold | Safe, counts `lease_drift` twice |
| Threshold `1 + lease_drift`, widen by the excess | 1.0% | 0.01% | Safe, no headroom |
| Threshold `1 + 2 lease_drift`, widen by the excess | 1.0% | −0.96% | Unsafe (planted variant) |
| `max(lease_drift, 1 − 1/B)` | 1.0% | 0 | Safe, no headroom |

*Headroom* is the margin given less the margin needed: how far ρ may rise after `B` was measured before the lease could outlast its grant. `B` is an average over a window in the past, while the lease runs in the future, so headroom is what covers a rate that changes in between: an oscillator warming up (parts per million), a clocksource switch by the watchdog (at least 500 ppm, possibly much more), or a migrated VM landing on another host's calibration. Part B of the script checks every rule against 20,000 random and 81 corner pairs (raw calibration within ±1% or ±4%, `CLOCK_BOOTTIME` slewed by up to ±22.6% or not at all, round trips from 50 µs to 1 ms, the grantor's reading anywhere inside them):

| Worst overlap of a 4 s lease (ms, positive is unsafe) | Design | Spike README | Excess over 1 + ld | `max(ld, 1 − 1/B)` | ld alone |
| --- | --- | --- | --- | --- | --- |
| Raw ±1% | −20.1 | −20.3 | −0.4 | 0.0 | **+39.6** |
| Raw ±4% (the Skylake-X error) | −19.5 | −20.0 | −0.5 | 0.0 | **+278.8** |
| ±1%, then a 0.4% rate step after the window | −4.3 | −4.7 | **+15.6** | **+16.0** | **+55.5** |
| ±1%, then a 1% rate step after the window | **+19.7** | **+19.5** | **+39.6** | **+40.0** | **+79.4** |

The design's rule is safe for any calibration error once `B` is measured, and survives a rate change of up to `lease_drift / 2` after it; the zero-headroom rules do not. A change bigger than the headroom is the step check's job, below. So the threshold of `lease_drift / 2` and widening "by the excess" are safe and not too conservative: they buy 20 ms of headroom on a 4 s lease. The 1.6% the spike README expected is the other reading of "widens by the excess"; it is safe, but gives a 4 s lease up for a margin counted twice.

### 2. The step check undoes the widening

The step check compares the grantor's raw clock with the holder's over the time since the last grant `a`: `(g − g_a) / (r − s_a)` is a *lower* bound on ρ (the holder's span `r − s_a` is the longest the real time can have been). The check trips when that exceeds `1 + lease_drift / 2`. A grantor whose raw clock is genuinely 0.6% fast shows `(g − g_a) / (r − s_a)` ≈ 1.006 on every heartbeat, which trips it: with a 1 s heartbeat and 100 µs round trips the check fires above about ρ = 1.0051, with 1 ms round trips above about 1.006. Each trip ends every lease and restarts the rate window, so the window never spans 10 s and the bound never forms.

Part C of the script runs a pair for 60 s (holder exact, grantor ρ fast, no daemon, 100 µs round trips) and partitions the network at 2,000 random times:

| ρ | Design as written: trips, margin, worst overlap | Step threshold `1/(1 − m)` only | Proposal (all three changes) |
| --- | --- | --- | --- |
| 1.000 | 0, 1.0%, −40.1 ms | 0, 1.0%, −40.1 ms | 0, 1.0%, −40.1 ms |
| 1.006 | **59**, 1.0%, −16.2 ms | 0, 1.1%, −16.2 ms | 0, 1.1%, −20.2 ms |
| 1.0101 | **59**, 1.0%, −0.1 ms | 0, 1.51%, −0.1 ms | 0, 1.51%, −20.5 ms |
| 1.012 | **59**, 1.0%, **+7.4 ms** | **59**, 1.0%, **+7.4 ms** | 0, 1.7%, −20.6 ms |
| 1.02 | **59**, 1.0%, **+38.4 ms** | **59**, 1.0%, **+38.4 ms** | 0, 2.5%, −21.6 ms |
| 1.04 | **59**, 1.0%, **+113.8 ms** | **59**, 1.0%, **+113.8 ms** | 0, 4.5%, −26.2 ms |

Under the design as written a grantor between 0.5% and 1% fast is still safe, only because `lease_drift` alone covers it, but trips the check on every heartbeat, a needless detection each second. Above 1.0101 the pair is unsafe for as long as it runs, not only for its first 10 s. The fault model's counterexample "no rate bound ... after a full window" and the scenarios that expect a full window for a fast grantor (the 1.503% and 2.52% rows) describe a state the pair never reaches.

Tying the threshold to the margin, `1/(1 − m)`, alone fixes the grantors the margin already covers (middle column), but leaves those above 1.0101 stuck: before a bound exists, `m` is `lease_drift` and they trip. That is the startup window, which needs its own change.

### 3. The startup window

The 39 ms is real under its premise: holder's raw clock 1% slow, grantor's 1% fast, both `CLOCK_BOOTTIME`s at their raw rates, a partition (or the grantor peering a new interval) right after a grant. The holder stops at 3.96 / 0.99 = 4.000 s of real time, the grantor at 4 / 1.01 = 3.960 s. With the 4% Skylake-X error on both sides it is 279 ms. The premise needs a pair whose raw clocks differ by more than 1%, rare but possible ([above](#what-linux-promises-about-the-raw-clocks-rate)), and finding 2 makes the window permanent for exactly those pairs.

The bound does not need a whole window. Any two grants give `B = (g_b − g_a) / (s_b − r_a)`, which is an upper bound however close together they are; a short gap only loosens it, by about the two round trips over the gap (`2 RTT / gap`): 0.2% with 100 µs round trips 100 ms apart, 2% with 1 ms round trips, 0.2% again once the grants span 1 s. A loose bound only widens the margin. So a holder can have a sound bound 100 ms after first contact, and a margin that tightens to its steady value as the window fills.

### 4. What else follows

- **Pause detection still works with the tied threshold.** A pause makes `(g − g_a) / (r − s_a)` jump. Write `x = 1 − ρ(1 − m)`: over a span `T` since grant `a`, the check misses a frozen-clock pause of at most `T·x`, and the lease tolerates `(L/ρ)·x`, so a missed pause fits inside the lease whenever `T ≤ L/ρ`, for any margin. With a 1 s heartbeat the check misses 5 to 10 ms, against at least 20 ms of headroom (part D). The design's "a pause matters once it outlasts 3 s" assumes the network keeps delivering; with a partition right after the resume, this headroom argument is the one that holds.
- **One caveat: the heartbeat's one-way delay.** `g` is read when the grantor sends the heartbeat and `r` when it arrives, so the grantor's lease advanced a further one-way delay `δ` that the check cannot see. A pause just under the threshold, seen only by a heartbeat in the last `δ/x` of a lease (about 20 ms at 100 µs; the case where renewals fail but heartbeats arrive), can leave an overlap of up to `δ`, under a millisecond. A model with unbounded delays will find it; it belongs with the existing Open on dropped messages, phrased as "dropped or delayed".
- **A rate change after measurement is caught at the next heartbeat, unless heartbeats are lost.** With the tied threshold, the step check trips exactly when the grantor's clock ran faster over the last heartbeat than the margin covers, whether from a pause or from a rate change past the headroom. Two causes are known: the watchdog switching a grantor's clocksource, which installs a new raw multiplier (`timekeeping.c:1606`), and a grantor live-migrated onto a host with another calibration, which keeps its grants on `Resumed` and runs at the new host's rate afterwards. A rise past `lease_drift / 2` followed by lost heartbeats overlaps by up to `(Δρ − lease_drift / 2) · primary_lease`. The study proposes an Open line for it ([below](#what-the-phase-2-model-simulator-and-tests-must-check)) and lists one way to close the migration case among the alternatives.
- **A grantor's identity is part of its clock.** The raw clock restarts near 0 at every boot, and a session's grantor is whichever node leads the control plane. A window that mixes two boots or two nodes gives a meaningless bound and a step check that either never trips (the new reading is lower) or trips for nothing (higher). Hence proposal 3's boot key (part E).
- **The restart wait is a grantor's wait,** so the derivation above settles that it must last until *both* clocks show `primary_lease`. It begins after the crash, so after every grant of the previous run, and needs nothing more. The [Leader leases](../../docs/test-scenarios.md#leader-leases) scenarios already count it on both clocks ("restarted 4.1 s ago on `CLOCK_BOOTTIME`, 3.9 s on `CLOCK_MONOTONIC_RAW`: refused").

## Proposal

1. **Keep the widening rule, and state its reason.** Margin for a grantor = `lease_drift + max(0, B − (1 + lease_drift / 2))`, reported above `1 + lease_drift / 2`. The lease needs `1 − 1/B`. The rule leaves at least `lease_drift / 2` more, the headroom for the pair's rate to rise after it was measured. A margin of `max_margin` (50%) or more, a bound of about 1.5, gives no lease and reports the pair.
2. **Tie the step check to the margin in force.** A heartbeat from a grantor ends the holder's leases when `g − g_a > (r − s_a) / (1 − m)`, where `m` is the margin of the lease the holder took from grant `a` (`1/0.99` = 1.0101 at `lease_drift` alone). It then trips exactly when the grantor's clock ran faster than that lease allows, and never on a grantor whose rate the margin already covers.
3. **A bound per grantor boot, and no lease without one.**
   - A holder keeps its rate window, its last grant `a` and its bound per grantor node and boot. Every grant and heartbeat carries the grantor's boot ID (`/proc/sys/kernel/random/boot_id`, read once at start), because `CLOCK_MONOTONIC_RAW` starts again near 0 at every boot and a new boot may calibrate differently, or run on another host.
   - A new window starts at first contact with a grantor, at a new boot ID, at a raw reading below the last one, after any detection (`Resumed`, the VMClock marker, the step check), and when the last grant ages out of `rate_window`.
   - After a window's first grant arrives, the holder asks again `rate_gap` (a constant, 100 ms) later, so `s_b − r_a ≥ rate_gap`. While the window spans less than `rate_window` but at least `rate_gap`, it bounds from the span's first and last thirds, as over a full window.
   - The holder uses a grantor's grant only once it has a bound for that grantor's boot. This replaces "until a pair has grants a whole window apart, its margin is `lease_drift` alone".

Without the boot key, a grantor that reboots in less than `rate_window`, a VM or microVM reboot or a kexec, leaves its last pre-reboot grant in the holder's window. The first grant of the new boot then gives a negative "bound", the rule turns it into `lease_drift`, the step check sees `g − g_a < 0` and nothing trips: with the new boot 2% fast, a partition right after leaves the lease 38 ms past its grant. Keyed by boot, the holder starts a new window and leaves itself 26 ms of headroom ([results.txt](results.txt), part E).

**Costs.** The first margin after a window starts depends on the round trip and on ρ: the worst bound from two grants is `ρ (rate_gap + 2 RTT) / rate_gap` (part F).

| Round trip | First lease at ρ = 1 | At ρ = 1.04 | After 1 s of window |
| --- | --- | --- | --- |
| 100 µs | 3.960 s (1.0%) | 3.812 s (4.7%) | 3.960 s and 3.819 s |
| 1 ms | 3.900 s (2.5%) | 3.737 s (6.6%) | 3.960 s and 3.812 s |
| 10 ms, an event loop at 90% load (design.md:524) | 3.180 s (20.5%) | 2.988 s (25.3%) | 3.900 s and 3.737 s |

| Cost | Size | When |
| --- | --- | --- |
| Reads blocked: every PG the grantor is a live member of, at every primary that needs its grant; Raft groups whose leader then lacks a quorum of expiries read through ReadIndex, a round trip per read | `rate_gap` + one round trip, about 100 ms | A holder's start, which overlaps the peering a restarted primary does anyway. A grantor's reboot, after which it is usually marked down and up again first. A grantor's daemon restart in the same boot costs nothing: its raw clock and boot ID are unchanged, so the window and bound carry on |
| Reads after a detection | About 100 ms, against one round trip today | After a pause, a suspend, a snapshot or a migration. VM pause spike T6e's "no more than 1 ms blocked" becomes about 100 ms |
| Shorter leases while the window fills | The table above | The first second after a start or detection |
| Messages | One extra request per grantor per window start; 16 bytes of boot ID per grant and heartbeat | |

**Raft leader leases.** A follower's support, and so its refusal to vote, rests on its node's grant to the leader's node, which the grantor keeps whether or not the holder has a bound; that is unchanged. What changes is the leader's lease: a voter whose node-pair grant the leader's node cannot use yet, for want of a bound, contributes no expiry. A leader without a quorum of expiries has no lease and reads through ReadIndex, as before it is fortified. So a leader failover is unaffected, and a restarted node's groups use ReadIndex for their first 100 ms or so.

**Sessions** count their leases by the same rules (design.md *Sessions and fencing*): the client daemon is the holder, the control-plane leader's node the grantor.
- Under the design as written, a client whose leader runs more than about 0.5% fast trips the step check on every renewal. Its session lease ends each time, and its daemon "holds its requests until a renewal is answered": writes stall about once a second, for as long as that node leads. Proposal 2 removes that.
- With proposal 3, a client daemon's start holds writes for about 100 ms more, on top of opening its session.
- A leader failover changes the grantor's node, so the bound is keyed by the leader's node and boot, and a new leader starts a new window. The client keeps its lease, counted from its last answered renewal, while it measures. The new leader's second renewal comes 100 ms after its first, well inside `session_lease` (10 s), so a failover adds nothing a client sees.
- A client node holds no node-pair leases, so it has no window to reuse. It has one grantor at a time, so one window.
- A client daemon handed off to its successor in the same boot (design.md *Daemon restart and upgrade*) can pass its window with the session, since the raw clock is the same; otherwise the successor pays 100 ms.

**Why 100 ms and not the next heartbeat.** Taking the bound from the first two heartbeat grants, 1 s apart, needs no constant and no extra request, and its first margin is tighter (1% at 1 ms round trips, 2.5% at 10 ms). Part C shows it is equally safe. It costs 1 s of reads at every start and detection instead of 100 ms. A QEMU internal snapshot of a running guest stops it for 0.07 to 0.8 s and, on kvm-clock, freezes its clocks with the marker unchanged (VM pause spike, T2c); the step check is what sees it, so under the heartbeat rule each snapshot would cost about 1 s of reads on top. That is the reason for 100 ms. If the user prefers fewer moving parts, the heartbeat rule is a sound fallback.

### Alternatives rejected

| Alternative | Why not |
| --- | --- |
| A wider margin until the window fills | No width is safe for an error the kernel does not bound (4% has shipped, and a `tsc_early_khz=` more than 1% off stays); 5% would cost 200 ms of every lease until the window fills, and with finding 2 the window never fills for the pairs that need it |
| Shorter leases until the window fills | The overlap scales with the lease but never reaches zero: a 1 s lease still outlasts its grant by 10 ms at ±1% |
| Refusing leases until a whole `rate_window` | Safe, but 10 s without reads at every start and after every pause, where 100 ms gives the same safety |
| The bound from the next heartbeat's grant | Sound, with no new constant; costs 1 s of reads per start and per detection, including every QEMU snapshot (above) |
| Lengthening the restart wait to cover the window | Covers only a restarted grantor, not a restarted holder or a new node; adds 6 s to every node's rejoin |
| A startup rate check against a majority of peers (CockroachDB's first-heartbeat check, turned to rates) | Judges a node, while safety is per pair; needs a window against several peers before serving; the [clock skew study](../clock-skew/README.md)'s recommendation 2 keeps it as an operator alert, which this proposal does not need |
| Accepting the risk with a stated assumption (relative calibration within 1%) | The kernel does not guarantee it on the CPUID path, every VM inherits its host's error, and ntpd and timesyncd do not correct it; with finding 2 the exposure would be permanent for the affected pairs |
| `max(lease_drift, 1 − 1/B)` as the margin | Exact, but no headroom once `B` passes 1.0101: a 0.4% rate change after measurement already overlaps (part B) |
| A grantor that resumes from a VMClock marker change counting its outstanding grants from the resume, a full `primary_lease` on both clocks | Would close the migration case of the residual [below](#4-what-else-follows) at a cost of up to 4 s of peering delay after a migration or restore. Left as an option for the Open line, not adopted: it adds a grantor rule for a case the headroom and the next heartbeat already cover unless a partition coincides |

## What the phase 2 model, simulator and tests must check

- **Epoch change model.**
  - Clocks: raw drifting by up to ±1%, with `CLOCK_BOOTTIME` both equal to raw (no daemon) and slewed. A grantor may reboot within `rate_window` with its raw rate changed by 2% and its raw clock restarted near 0.
  - Rules: the bound from any two grants of one grantor boot, no lease without a bound, and the step threshold `1/(1 − m)`. Property 1 must hold.
  - Planted variants that must fail property 1: a lease on `lease_drift` alone before the first bound (39 ms at ±1%); that together with the step threshold `1 + lease_drift / 2` and a window restart on each trip, the design as written (a 1.2% fast pair never leaves its startup state; 7 ms); widening only past `1 + 2 lease_drift`; `max(lease_drift, 1 − 1/B)` with a raw rate step of 0.4% after measurement; and a window not restarted on a grantor's new boot (part E, 38 ms).
  - The step threshold `1 + lease_drift / 2` *with* proposal 3 does not break safety: such a pair never gets a lease (part C, last block). It fails a liveness property instead, which the model should state: a pair with ρ ≤ 1.04 holds a lease within `rate_gap` plus two round trips of first contact and keeps it with no detections.
  - A raw rate step of up to `lease_drift / 2` is an allowed fault. A bigger one followed by lost or delayed heartbeats is the residual Open below.
- **Simulator.** Pairs with ρ from 0.96 to 1.04, heartbeat jitter, round trips from 50 µs to 10 ms, grantor reboots and session-leader failovers. Checks: no overlap in any seed; no step-check trip in steady state for any ρ; a first lease within `rate_gap` plus a round trip of first contact; and the margin within 0.2% of `lease_drift + max(0, ρ − 1.005)` once the window is full.
- **Unit tables.** The rate-bound table keeps its 1.503% and 1.103% rows, now reachable. It adds rows for: the first grant of a window, which gives no lease; the second grant, sent 100 ms after the first arrived, which gives a bound with its overshoot; a grant with a new boot ID, which starts a new window; and a margin of 50%, which gives no lease. The pause-detection rows move to the threshold `1/(1 − m)`: with s_a = 1,000,000, r_a = 1,001,000, r = 2,001,000 and `m` = 1%, the limit is 1,001,000 / 0.99 = 1,011,111.1, so g − g_a = 1,011,111 ends nothing and 1,011,112 ends every lease.
- **A new Open line**, in the shape of the existing one for kvm-clock stops with dropped messages: "a grantor's raw rate rising past the headroom, as on a clocksource switch or a live migration onto another host, followed by lost or delayed heartbeats, overlaps by up to `(Δρ − lease_drift / 2) · primary_lease`; the model excludes it with this reason, or expects property 1 to fail". The one-way delay of the heartbeat that would see a pause belongs to the same Open (finding 4).

## Where it would apply

- **[docs/design.md](../../docs/design.md)**
  - The *Lease time* paragraph of [Epoch changes and failure detection](../../docs/design.md#epoch-changes-and-failure-detection):
    - say why the widening rule is enough: `1 − 1/B` is needed, and `lease_drift / 2` is headroom for a rate that changes after measurement;
    - replace "until a pair has grants a whole window apart ... its margin is `lease_drift` alone" with proposal 3, `rate_gap`, the boot ID and `max_margin`;
    - change the step check's threshold to `1/(1 − m)`;
    - change "so a pause costs one round trip of reads" to about `rate_gap`;
    - let the sentence on calibration ("which nothing bounds") cite the CPUID path and the 4% Skylake-X error.
  - [Sessions and fencing](../../docs/design.md#sessions-and-fencing): one sentence saying that a client's bound is per leader node and boot, and that a failover starts a new window inside the old lease.
  - [Metadata layer](../../docs/design.md#metadata-layer-metastore), leader leases: a voter whose grant the leader's node has no bound for gives no expiry.
  - *Alternatives considered*: the *Lease clock* row gains the startup alternatives with their reasons, and the *Lease across a VM pause* row ("moved further than the holder's own could") changes to the tied threshold.
  - *Roadmap and open questions*: the residual (rate rise past the headroom with lost heartbeats).
- **[docs/build-checklist.md](../../docs/build-checklist.md)**
  - *Peering and read leases*: no lease without a bound, `rate_gap`, the boot key and the step threshold.
  - *Epoch change model*: the planted variants and the liveness property.
  - *Sessions and fencing model*: the client's bound per leader node and boot, and the tied step check.
  - *Leader leases*: "Reads go through ReadIndex until the leader is fortified, holds a quorum of expiries and…", where an expiry now needs a bound.
- **[docs/test-scenarios.md](../../docs/test-scenarios.md)**
  - The settings line (line 10) gains `rate_gap` as a constant.
  - *Peering and read leases*:
    - delete the startup Open and the restart-wait-on-both-clocks Open;
    - replace the within-the-first-window row of the lease-time table with the no-lease case;
    - change the `[property]` line "With raw ±1%, the same holds once each pair has a full window" to "from the first lease, with raw up to ±4%";
    - turn the rate-bound row "The pair started 6 s ago, no grants a whole window apart: none, 1%" into a bound from the 6 s span, and add the rows above;
    - recompute the pause-detection rows;
    - change "One round trip of reads is lost" after a detection to about 100 ms, and T6e to about 100 ms blocked.
  - *Epoch change model*: delete the startup Open; add the counterexample rows, the liveness property and the new residual Open. The counterexample "No rate bound: the margin is `lease_drift` alone forever ... after a full window" becomes "a lease before the first bound".
  - *Placement faults and invariants*: P9b ("once the pair has a full window") becomes "from the first lease"; P10 ("after a full window") becomes "after the first bound"; P10b (the 39 ms Open) becomes "no lease before the second grant; no overlap".
  - *Sessions and fencing model*: its clock bullet ("when a grantor's clock moved further than its own could have") and scripted scenario 4 use `1/(1 − m)` and the per-leader bound. Add a scenario: a leader whose raw clock runs 2% fast, where the client keeps its session with no stall.
  - *Leader leases*, "Expiry of one grant": "No grants a whole `rate_window` apart yet: margin `lease_drift` alone" becomes "no bound yet: no expiry from this voter".
- **Stale texts, listed only.** `spikes/clock-slew/README.md:176` holds a 1.6% expectation in a spike record, superseded by test-scenarios.md's 1.103%. `blogs/03-epochs-leases-and-failures.md:525-531` states the window rule and would need the same change; this study does not edit it.

## Other Open lines in these sections

*Lease clocks in the context* has none.

| Section | Open line | Answered by this study? |
| --- | --- | --- |
| Epoch change model | Startup window | Yes: proposal 3, with 2 |
| Epoch change model | Whether the per-primary table of newest incarnations must survive a primary change | No; nothing here bears on it |
| Epoch change model | A VM stop whose queued messages are dropped leaves up to a heartbeat of stale reads | No; the tied step threshold neither widens nor closes it. It should read "dropped or delayed" (finding 4) |
| Epoch change model | Which grant rule passes (grant names the grantor's table epoch, or grantor waits for the holder's table); not marked Open | No |
| Peering and read leases | Startup window (39 ms row) | Yes, as above |
| Peering and read leases | Naming the miscalibrated node by majority (clock skew study, recommendation 2) | Partly: safety does not need it, since each pair's own bound suffices; whether to alert stays open |
| Peering and read leases | Whether the restart wait is counted on both clocks | Yes: it is a grantor's wait, so both (finding 4), as *Leader leases* already expects |
| Peering and read leases | Whether the restart wait also delays a restarted primary's own peering | No; with proposal 3 a restarted holder serves no lease reads for its first 100 ms either way |
| Peering and read leases | Which PGs a fresh grant covers | No; the epoch change model decides |
| Peering and read leases | Whether recovery's copy of a block is skipped, or versioned below, a newer whole-block write | No; unrelated to leases |

## Unverified

- **How common errors over 1% are.** One fixed bug (4%) and one spread-spectrum figure (0.25%) come from the kernel's history; no fleet measurement was found or made. Kernels between `6baf3d61821f` and `b511203093` were not checked for which releases shipped the Skylake-X entry.
- **Other architectures.** arm64 reads its counter frequency from `CNTFRQ_EL0`, which firmware sets and the kernel does not calibrate; not read.
- **AMD.** Read only as far as the CPUID path returning 0 for non-Intel CPUs, so AMD takes the PIT/HPET calibration; newer AMD frequency enumeration, if any, was not checked.
- **Hyper-V and VMware rates** were read from the guest side only; what the hypervisors put in their frequency MSR or leaf, and how accurate it is, was not.
- **The clocksource watchdog threshold** is from reading `clocksource.c`; the TSC's and HPET's registered uncertainty margins were not computed per machine, and no switch was provoked.
- **The model** has constant raw rates within a run (apart from part B's single step and part E's reboot), symmetric jitter-free 100 µs round trips in parts C and E, and grants on every heartbeat; asymmetric paths, lost grants and the heartbeat's one-way delay are left to the simulator. Sessions were analysed on paper, not run in the script.
- **The boot ID.** Reading `/proc/sys/kernel/random/boot_id` once at start was not checked inside every container runtime the CSI node plugin might run under; it is a host-wide, read-only file.
- **CockroachDB** was read at a master commit, not a release; how often its first-heartbeat check fires in practice was not looked up.

## Reproduce

`./fetch.sh [dir]` downloads every cited file at the pinned commits into `dir` (default `./work`) and prints the cited lines; needs `curl`. `python3 -I margins.py > results.txt` reruns the model (standard library only, deterministic, a few seconds).

## Sources

Linux at [v6.18](https://github.com/torvalds/linux/tree/7d0a66e4bb9081d75c82ec4957c50034cb0ea449) (`7d0a66e`): [`arch/x86/kernel/tsc.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/tsc.c), [`arch/x86/kernel/tsc_msr.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/tsc_msr.c), [`kernel/time/clocksource.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/kernel/time/clocksource.c), [`kernel/time/timekeeping.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/kernel/time/timekeeping.c), [`arch/x86/kernel/kvmclock.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/kvmclock.c), [`arch/x86/kernel/pvclock.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/pvclock.c), [`arch/x86/kvm/x86.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kvm/x86.c), [`arch/x86/kernel/cpu/mshyperv.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/cpu/mshyperv.c), [`arch/x86/kernel/cpu/vmware.c`](https://github.com/torvalds/linux/blob/7d0a66e4bb9081d75c82ec4957c50034cb0ea449/arch/x86/kernel/cpu/vmware.c); commit [`b511203093`](https://github.com/torvalds/linux/commit/b511203093489eb1829cb4de86e8214752205ac6) ("x86/tsc: Fix erroneous TSC rate on Skylake Xeon").

CockroachDB, `cockroachdb/cockroach` at [`d30c905`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff): [`pkg/kv/kvserver/storeliveness/doc.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/kv/kvserver/storeliveness/doc.go), [`config.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/kv/kvserver/storeliveness/config.go), [`support_manager.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/kv/kvserver/storeliveness/support_manager.go), [`pkg/rpc/clock_offset.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/rpc/clock_offset.go), [`pkg/rpc/peer.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/rpc/peer.go), [`pkg/rpc/connection.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/rpc/connection.go), [`pkg/server/server.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/server/server.go), [`pkg/server/clock_monotonicity.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/server/clock_monotonicity.go), [`pkg/base/config.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/base/config.go).

rock-solid: the [clock slew spike](../../spikes/clock-slew/README.md) (S4, S9, S11-S13, L1-L4) and the [VM pause spike](../../spikes/vm-pause/README.md)'s guest results.
