# Clock slew spike

**Question.** Read leases, Raft leader leases and session leases are counted on each side's own `CLOCK_BOOTTIME`, and the holder ends its lease `lease_drift` (1% of the lease) early on the grounds that "the kernel slews a clock by at most 500 ppm (`MAXFREQ`)". Is that bound true: how fast or slow can a time daemon make `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` run against real time, what do chrony, systemd-timesyncd and ntpd actually do, and which clock should leases be counted on so that a holder's lease always ends before its grantor's?

**Setup.** Host: Linux 6.18.44 in a Firecracker VM (`clocksource=tsc`, `tsc_known_freq`, no time daemon running), 4 vCPUs, Go 1.27.0. Written against commit `311362b`. Changing this machine's clock was ruled out, so every adjustment runs inside a disposable kernel: Linux v6.18 built as User-Mode Linux (UML), booted with [slew_guest.c](slew_guest.c) as init. UML's host side reads the host's `CLOCK_MONOTONIC` and never calls `adjtimex`, `clock_adjtime` or `clock_settime` (`arch/um/os-Linux/time.c`), so an `adjtimex` inside the guest changes only the guest kernel's timekeeper; [results-uml.txt](results-uml.txt) prints the host's NTP state before and after to show it unchanged. The guest runs the kernel's own `kernel/time/ntp.c` and `timekeeping.c`, so it shows what any Linux 6.18 does with each adjustment; it cannot show what a real daemon decides to do.

The real daemons ran on a second host, a bare-metal laptop: Intel Core Ultra 5 235U, Linux 6.17.0-1033-oem (Ubuntu 24.04), `clocksource=tsc` with `tsc_known_freq`, systemd-timesyncd syncing it over a 290-390 ms internet path. [run-vm.sh](run-vm.sh) boots, as an ordinary user, a disposable KVM guest: QEMU 8.2.2 (unpacked from this release's .debs) with `-accel kvm -cpu host`, the Ubuntu 26.04 minimal cloud image (Linux 7.0.0-38, chrony 4.8, systemd 259.5) on a throwaway snapshot, cloud-init seeded over HTTP from the host's loopback; it runs [run-vm-root.sh](run-vm-root.sh) as root in the guest and prints the host's NTP state before and after. The guest's clocksource is kvm-clock (the guest sees no invariant TSC), so its `CLOCK_MONOTONIC_RAW` runs at the host's TSC rate (S15) and its steps and slews stay in the guest kernel. Its time sources are Ubuntu's default NTS pools (`*.ntp.ubuntu.com`, 150-200 ms away) through QEMU's user-mode network, or, as a clean source, the host's `CLOCK_REALTIME` through `ptp_kvm` (`/dev/ptp0`); clockrate also reads `/dev/ptp0` each window to print the guest's offset from the host (`real-phc_ms`), which shows what the daemon did to the guest's time, not only its rate. chrony 4.8 and 4.9 slew alike (`maxslewrate`'s default and `update_slew()` are unchanged; 4.9 drops a tick hysteresis for `HZ` ≤ 250 from `sys_linux.c` and lowers `maxdrift`), and timesyncd 259's `manager_adjust_clock()` is v258's.

- [adjtimex_read.c](adjtimex_read.c): prints the kernel's NTP state with `adjtimex(modes=0)` and the outstanding `adjtime()` offset with `ADJ_OFFSET_SS_READ`, both allowed unprivileged.
- [clockrate.c](clockrate.c): samples `CLOCK_MONOTONIC`, `CLOCK_BOOTTIME` and `CLOCK_REALTIME` between two `CLOCK_MONOTONIC_RAW` reads (the tightest of 7 brackets) and prints each clock's rate against `CLOCK_MONOTONIC_RAW` per window, next to the rate the current tick and frequency predict, the PLL offset left to slew and, given a PTP clock, `CLOCK_REALTIME`'s offset from it. Built static so the VM guest runs it too.
- [slew_guest.c](slew_guest.c): applies each adjustment the kernel accepts and checks the rates; slews of an offset are measured from one `second_overflow()` to the next, seen as a change in the outstanding offset. It refuses to run unless it is PID 1.
- [gocost/](gocost/): what reading `CLOCK_MONOTONIC_RAW` costs a Go event loop.

```
./fetch.sh            # Linux v6.18 (shallow) and flex into vendor/
./build.sh uml        # bin/: host tools, static guest, UML kernel (2.5 min on 4 cores)
./run-host.sh > results-host.txt     # read-only, unprivileged
./run-uml.sh  > results-uml.txt      # about 2.5 minutes, no root
(cd gocost && go test -bench . -benchtime 2s -count 3) > results-gocost.txt

# On the laptop: real daemons in a KVM guest, no root on the host (only /dev/kvm)
./fetch.sh vm         # QEMU and its libraries, the Ubuntu 26.04 cloud image (420 MB), timesyncd's .deb
./build.sh
./run-vm.sh chrony    > results-vm-chrony.txt     # V0-V2, about 8 minutes
./run-vm.sh timesyncd > results-vm-timesyncd.txt  # V3, about 3 minutes
{ ./run-host.sh; timedatectl timesync-status; } > results-host-laptop.txt
# results-host-laptop-boot2.txt: the same after a reboot, headed by `journalctl -k -b 0` and `-b -1`'s tsc lines
```

Expected before the run: tick changes by ±10% move `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` by ±10% against `CLOCK_MONOTONIC_RAW`, the PLL and `adjtime()` offsets add to that, and `MAXFREQ` clamps only the frequency word. With the real daemons: chrony slews a 1 s offset at `maxslewrate` (83,333 ppm) for about 12 s; timesyncd hands a 0.3 s offset to the kernel PLL at constant 1, slewing offset/8 per second; `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` follow, `CLOCK_MONOTONIC_RAW` does not.

## Results

### What the sources say

Linux at [v6.18](https://github.com/torvalds/linux/tree/v6.18), chrony [4.9](https://gitlab.com/chrony/chrony/-/tree/4.9), systemd [v258](https://github.com/systemd/systemd/tree/v258), ntpd from [ntp-project/ntp](https://github.com/ntp-project/ntp) master.

| Test | Result |
| --- | --- |
| S1 what slews `CLOCK_MONOTONIC` | `ntp_update_frequency()` sets the nanoseconds per second the timekeeper adds to `tkr_mono` from three terms: `tick_usec * 1000 * USER_HZ + ntp_tick_adj + time_freq` ([`ntp.c:251`](https://github.com/torvalds/linux/blob/v6.18/kernel/time/ntp.c#L251)). Each second `second_overflow()` adds a chunk of the PLL phase offset and up to 500 µs of the `adjtime()` offset (`ntp.c:461-487`). `timekeeping_adjust()` turns that tick length into `tkr_mono.mult` ([`timekeeping.c:2158`](https://github.com/torvalds/linux/blob/v6.18/kernel/time/timekeeping.c#L2158)); it prints a warning past 11% but clamps nothing |
| S2 `CLOCK_BOOTTIME` | `tkr_mono` plus `offs_boot` (`ktime_get_with_offset`, `timekeeping.c:857`; vDSO `vsyscall.c:53-57`), and `offs_boot` changes only at resume (`tk_update_sleep_time`, `timekeeping.c:267`). **So `CLOCK_BOOTTIME` runs at exactly `CLOCK_MONOTONIC`'s slewed rate** |
| S3 `CLOCK_MONOTONIC_RAW` | `tkr_raw.mult` is set only when a clocksource is installed (`tk_setup_internals`, `timekeeping.c:366-367`); nothing in `adjtimex` touches it. It stops in suspend, like `CLOCK_MONOTONIC`. `timerfd_create` refuses it (`fs/timerfd.c:405-409`) |
| S4 `MAXFREQ` | 500,000 ns/s ([`include/linux/timex.h:136`](https://github.com/torvalds/linux/blob/v6.18/include/linux/timex.h#L136)) clamps only `time_freq`: on `ADJ_FREQUENCY` (`ntp.c:732-735`) and in the PLL/FLL (`ntp.c:327-329`). **It does not bound the tick, the phase offset or `adjtime()`** |
| S5 tick | `ADJ_TICK` accepts 900,000/`USER_HZ` to 1,100,000/`USER_HZ` ([`timekeeping.c:2598-2601`](https://github.com/torvalds/linux/blob/v6.18/kernel/time/timekeeping.c#L2598)): **±10%**, applied at once (`timekeeping.c:2728`) |
| S6 PLL phase offset (`ADJ_OFFSET` with `STA_PLL`) | Clamped to `MAXPHASE` 0.5 s (`ntp.c:300`); each second the kernel slews `offset >> (SHIFT_PLL + time_constant)` of what remains (`ntp_offset_chunk`, `ntp.c:217-220`, `SHIFT_PLL` 2 in `timex.h:120`), with `time_constant` from 0 to `MAXTC` 10 (`ntp.c:747-750`). **At constant 0 the first second slews 0.125 s: 12.5%** |
| S7 `adjtime()` (`ADJ_OFFSET_SINGLESHOT`) | `MAX_TICKADJ` 500 µs per second (`ntp.c:103`, `470-487`): 500 ppm, **on top of** the frequency |
| S8 `ntp_tick_adj=` boot parameter | Added to the second length with no limit (`ntp.c:257`, `1092-1101`) |
| S9 combined bound | ±(100,000 + 500 + 125,000 + 500) ppm = **±22.6%**, plus whatever `ntp_tick_adj` adds. Steps (`settimeofday`, `ADJ_SETOFFSET`) move only `wall_to_monotonic` and leave `CLOCK_MONOTONIC` alone (`__timekeeping_inject_offset`, `timekeeping.c:1480`) |
| S10 chrony on Linux | Turns the kernel PLL off (`sys_timex.c:199-214`) and runs the clock at any rate up to 100,000 ppm by splitting it between `ADJ_TICK` and `ADJ_FREQUENCY` ([`sys_linux.c:132-153`](https://gitlab.com/chrony/chrony/-/blob/4.9/sys_linux.c#L132), maximum `1e6 * (nominal_tick / 10) / nominal_tick` at `sys_linux.c:274-275, 338`). It slews an offset at up to `maxslewrate`, **default 83,333 ppm (1/12, `conf.c:112`)**, a rate that grows with the offset (`sys_generic.c:203-223`). `makestep` is off unless configured (`conf.c:165-166`); the shipped example steps only in the first 3 updates (`makestep 1.0 3`), so later offsets of any size are slewed. `leapsecmode slew` corrects a leap second in 12 s at that rate (`doc/chrony.conf.adoc:1309-1315`) |
| S11 systemd-timesyncd | Offsets under 0.4 s go to the kernel PLL with `constant = log2(poll) - 4` (`timesyncd-manager.c:52, 247-253`); at its 32 s minimum poll (`timesyncd-manager.h:14`) that is constant 1, **offset/8 in the first second: up to 5%**. Larger offsets are stepped |
| S12 ntpd | Kernel PLL with `constant = sys_poll` (`ntpd/ntp_loopfilter.c:766-774`), stepping above 0.128 s (`:38`). At the default 64 s minimum poll (`NTP_MINDPOLL` 6) that slews at most 0.128 s >> 8 per second, 500 ppm, plus up to 500 ppm of frequency. With `-x` it slews with `adjtime()` at 500 ppm |
| S13 `CLOCK_MONOTONIC_RAW`'s own error | The TSC's calibrated frequency. The kernel trusts a CPUID-reported crystal frequency outright and keeps the boot's fast calibration if the refined one differs by more than 1% (`arch/x86/kernel/tsc.c:1312`, `1355-1374`); only a time daemon corrects the difference, through tick and frequency, so it shows in `CLOCK_BOOTTIME` but never in `CLOCK_MONOTONIC_RAW` |
| S14 sleep time | At resume the kernel adds the sleep to `offs_boot`, measured by a clocksource that runs in suspend or else by the persistent clock (`timekeeping_resume`, `timekeeping.c:1937-1975`), which on x86 is the CMOS clock in whole seconds (`arch/x86/kernel/rtc.c:76`) |
| S15 virtual machines | KVM's kvmclock counts "from boot time, but with the frequency of the raw clock" (`arch/x86/kvm/x86.c:2387-2391`), so a host's slews do not reach its guests. QEMU saves kvmclock when a VM stops and restores the same value when it continues ([`hw/i386/kvm/clock.c:163-219`](https://github.com/qemu/qemu/blob/v10.1.0/hw/i386/kvm/clock.c#L163)), and Firecracker restores snapshots the same way unless `clock_realtime` is set (`docs/snapshotting/snapshot-support.md`): **every guest clock, `CLOCK_MONOTONIC_RAW` included, stands still across a pause** |

### Measured on the Firecracker host (unprivileged, read-only)

Raw output: [results-host.txt](results-host.txt), [results-gocost.txt](results-gocost.txt).

| Test | Result |
| --- | --- |
| H1 `adjtimex(modes=0)` | Works unprivileged: `tick=10000 freq=0 offset=0 status=UNSYNC constant=2 tolerance=500 ppm`, no `adjtime()` offset outstanding. No time daemon runs here |
| H2 rates over 60 × 1 s | `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW` −0.014 to +0.014 ppm, `CLOCK_BOOTTIME` against `CLOCK_MONOTONIC` ≤ 0.013 ppm: with tick 10000 and freq 0 the clocks run together, so the harness's noise floor is far below anything that matters |
| H3 rates over 100 × 100 ms | −0.19 to +0.11 ppm |
| C1 cost in Go | `time.Now()` 44 ns (vDSO, `CLOCK_MONOTONIC`); `clock_gettime(CLOCK_MONOTONIC_RAW)` and `clock_gettime(CLOCK_BOOTTIME)` through `syscall.RawSyscall` 140 to 151 ns each, 0 allocations. Go reaches neither through the vDSO, so counting leases on `CLOCK_MONOTONIC_RAW` as well costs one more ~140 ns syscall per loop pass |

### Measured in the UML guest (every adjustment the kernel accepts)

Raw output: [results-uml.txt](results-uml.txt). Rates are `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW`; `CLOCK_BOOTTIME` matched `CLOCK_MONOTONIC` within 20 ppm in every window. 87 checks, 0 failures.

| Test | Result |
| --- | --- |
| G0 no adjustment | −8 to 0 ppm |
| G1, G2 `tick` 9000, 11000 | **−99,987 to −100,009 ppm; +99,992 to +100,017 ppm** |
| G3 `tick` 8999, 11001 | `EINVAL`, as S5 says |
| G4 `freq` +1000 ppm | Reads back 500 ppm (clamped by `MAXFREQ`); runs at +489 to +505 ppm |
| G5 `tick` 9000 + `freq` −500 | −100,491 to −100,503 ppm |
| G6 `adjtime(±0.5 s)` | ±494 to ±506 ppm, 500 µs consumed per second |
| G7 PLL, offset ±0.5 s, constant 0 | Per second: **±124,909 to ±124,969**, ±93,686 to ±93,737, ±70,269 to ±70,290, ±52,691 to ±52,723 ppm (predicted 125,000, 93,750, 70,312, 52,734) |
| G8 timesyncd's call, −0.399 s, constant 1 | −49,870, −43,635, −38,179, −33,412 ppm (predicted −49,875, −43,641, −38,186, −33,412): 0.165 s slewed in 4 s |
| G9 chrony's call for `maxslewrate` 83,333 ppm (`tick` 10000 ∓ 833, `freq` ∓33.3 ppm) | **−83,322 to −83,340 and +83,325 to +83,338 ppm**, steady |
| G10 all at once (`tick` ∓10%, `freq` ∓500, `adjtime` ∓0.5 s, PLL ∓0.5 s at constant 0) | **First second −225,968 and +225,947 ppm** (predicted ±226,000), second ±194,730 |
| G11 1 s timerfd at `tick` 9000 | `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC` both fire after 1.1114 to 1.1115 s of `CLOCK_MONOTONIC_RAW` (predicted 1.1111 s): timers follow the slew. `timerfd_create(CLOCK_MONOTONIC_RAW)` → `EINVAL` |
| G12 steps of +1 s (`clock_settime`) and +2 s (`ADJ_SETOFFSET`) | `CLOCK_REALTIME` jumped 3.0 s; `CLOCK_MONOTONIC`'s rate over the window +5 ppm: steps are harmless to leases |
| G13 detecting a slew | 100 ms windows of `CLOCK_BOOTTIME` against `CLOCK_MONOTONIC_RAW`: worst 38 ppm with no adjustment; −83,309 to −83,356 ppm during G9's slew. One heartbeat interval is enough to see a slew |

### Measured with the real daemons in a KVM guest

Raw output: [results-vm-chrony.txt](results-vm-chrony.txt), [results-vm-timesyncd.txt](results-vm-timesyncd.txt). Each step is `date -s`; rates are `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW` per 1 s window, offsets are the guest's `CLOCK_REALTIME` against the host's. `CLOCK_BOOTTIME` matched `CLOCK_MONOTONIC` within 0.21 ppm in every window, and the host's tick and frequency word were the same before and after each run (only its own timesyncd's PLL offset moved).

| Test | Result |
| --- | --- |
| V0 guest clocks | kvm-clock (available: kvm-clock, tsc, hpet, acpi_pm). Locked to the host through `ptp_kvm`, chrony found the guest clock 20 ppm fast against the host's `CLOCK_REALTIME`. The 1 s slewed on `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW` brought the guest back to within 10 µs of the host (V2), so `CLOCK_MONOTONIC_RAW` kept the host's rate throughout |
| V1 chrony 4.8, Ubuntu's NTS pools, +1 s, `burst 4/4` | **−83,333 ppm** from the first window for 13 s, slewing 1.132 s: 0.13 s past the offset, to −94 ms. At its next update, about 60 s later, it slewed the same way again, −281 ms over 8 s at −65,108 then a steady −30,644 ppm, to −373 ms; 64 s after that +399 ms at **+83,333 ppm** for 6 s, to +28 ms, where it stayed. 142 s from step to steady, 29 s of it slewing, and the guest's time off by up to 0.37 s after its first correction. Meanwhile chronyd logged the clock "wrong by −3.19 seconds", then "+2.91 seconds", one second apart. The frequency word was left at +130 ppm, 150 ppm from V2's |
| V2 chrony 4.8, the host's clock (`ptp_kvm`, `poll 0`) its only source, +1 s, then −1 s | **−83,352 ppm** for 13.0 s and **+83,312 ppm** for 13.1 s: `maxslewrate` around the −20 ppm base frequency, starting within a second of the step. Within 10 µs of the host after each, no overshoot |
| V3 timesyncd 259, Ubuntu's default server, +0.3 s, then restarted | It measured −0.249 s (the path's asymmetry hid 50 ms) and handed it to the kernel PLL at constant 1: −29,827 ppm in the first whole window, then 7/8 of the rate before each second (−25,902, −22,980, −20,179 ppm), under 1,000 ppm after 30 s. The same call moved the PLL's frequency word from +336 to −394 ppm. Its next two polls, 32 and 64 s apart, slewed +13 and +19 ms at under 500 ppm |

V1's overshoot and swings come from how chrony estimates offset: a least-squares fit over each source's samples (`SST_DoNewRegression`, [`sourcestats.c:556-574`](https://gitlab.com/chrony/chrony/-/blob/4.8/sourcestats.c#L556) at 4.8), and a step chrony did not make lands in the middle of the samples it fits; its estimates swing until the samples before the step age out, the "wrong by" lines being logged past `logchange`'s 1 s (`reference.c:492`). With V2's one sample a second the old samples go within seconds and the slew ends on the offset.

### Measured on the laptop host (unprivileged, read-only)

Raw output: [results-host-laptop.txt](results-host-laptop.txt), [results-host-laptop-boot2.txt](results-host-laptop-boot2.txt) after a reboot (with both boots' TSC lines from the kernel log), and the host's `adjtimex` state at the head and foot of each `results-vm-*.txt`.

| Test | Result |
| --- | --- |
| L1 TSC | `tsc: Detected 3187.200 MHz TSC`, the frequency CPUID reports, on both boots measured; with `tsc_known_freq` the kernel skips the refined calibration ([`tsc.c:1424-1434`](https://github.com/torvalds/linux/blob/v6.18/arch/x86/kernel/tsc.c#L1424)), so there is no "Refined TSC clocksource calibration" line and every boot's `CLOCK_MONOTONIC_RAW` runs at the same rate |
| L2 rates, timesyncd at a 34 min poll | `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW` −18.3 to −18.0 ppm: the frequency word, −15.9 ppm, plus the PLL slewing its remaining −1.2 ms at constant 7 (`offset >> 9`, −2.3 ppm) |
| L3 timesyncd's state through the day | Frequency word +29.4, +21.1, +186.7, −15.9 ppm; PLL offsets up to −24.2 ms at constant 3, which slews `offset >> 5`, 756 ppm, in its first second. timesyncd reported offsets from −54 to +19 ms on this path, with 12 to 26 ms of jitter, and a 60 ms offset at constant 3 slews 1,875 ppm in its first second |
| L4 after a reboot | Same TSC frequency, no refinement. Frequency word +5.0 ppm, tick 10000, PLL slewing −3.4 ms at constant 6 (`offset >> 8`, −13 ppm): `CLOCK_MONOTONIC` against `CLOCK_MONOTONIC_RAW` −8.2 to −5.1 ppm. **The frequency word timesyncd settled on stayed between −16 and +29 ppm on both boots (+187 once, while chasing a 24 ms offset), so the crystal's error is tens of ppm and the same each boot; rates of hundreds to thousands of ppm are timesyncd slewing phase, and come and go with each poll's offset** |

## Conclusions

1. **The 500 ppm bound in the design is wrong.** `MAXFREQ` limits only the frequency word. The tick (±10%), the PLL phase offset (up to 12.5% in its first second) and `adjtime()` (500 ppm) add to it, so the kernel lets a time daemon run `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` up to 22.6% fast or slow (S1-S9, G1-G10), and the `ntp_tick_adj` boot parameter is unbounded.
2. **Ordinary daemons use that range, and the real ones do exactly what their source says.** chrony, the default on RHEL, Fedora, SUSE and Ubuntu 25.10 and later, slews any offset after its first updates at its `maxslewrate`, 8.33% by default (S10, G9, and chronyd itself in V1, V2); systemd-timesyncd, the default on Debian and earlier Ubuntu, at up to about 5% through the kernel PLL (S11, G8, V3). ntpd's defaults stay near 0.1% (S12). Offsets big enough to trigger such slews follow a VM pause or migration (S15), a resume whose sleep was measured in whole seconds (S14), a leap second under `leapsecmode slew`, or a bad time source. Even with no event, timesyncd on a noisy path slews hundreds to about 1,900 ppm at each poll (L3), past the 500 ppm the design once assumed.
3. **A 1% margin then fails by a wide margin.** With `primary_lease` 4 s and `lease_drift` 1%, a holder whose clock chrony slows by 8.33% keeps its lease 4.32 s while its grantor's ends at 4 s: 0.32 s of overlap. If the grantor's clock is also slewed fast, 0.63 s; at the kernel's limits on both sides, 1.85 s, about half the lease. Covering every slew with a wider margin would take 15.4% for chrony on both sides and 37% for the kernel's limits, and still miss `ntp_tick_adj`.
4. **A slew lasts longer than its offset implies.** chrony with a clean source took 13 s per second of offset (V2); with internet sources it took 142 s to settle a 1 s step, slewing at the full rate in both directions three times (V1); timesyncd's PLL slews for about 30 s at 32 s polls (V3). Rate, not duration, is what a lease cares about, and no daemon exceeded its rate; but anything that stops leases while a slew lasts stops them for minutes.
5. **`CLOCK_MONOTONIC_RAW` is the clock no daemon slews** (S3, G0-G13, and the real daemons in V0-V3), and KVM counts guest time on it for the same reason (S15). But its rate is the boot's TSC calibration, which the kernel never checks against a bound (it keeps the quick boot calibration when the refined one disagrees by more than 1%, and trusts a CPUID-reported frequency outright) and which only a daemon corrects, in `CLOCK_BOOTTIME` (S13). On the one laptop measured, the CPUID frequency was within tens of ppm of NTP time on both boots (L1-L4). So neither clock alone is bounded by physics plus nothing else; together they are: a holder that ends its lease on whichever clock runs faster, and a grantor that keeps it on whichever runs slower, are wrong only if both of a node's clocks are wrong in the same direction.
6. **Timers follow the slewed clock** (G11) and no timerfd counts on `CLOCK_MONOTONIC_RAW`, so a timer can only wake the loop; the lease decision compares clock readings.
7. **Suspend needs its own rule.** `CLOCK_MONOTONIC_RAW` stops in suspend, and `CLOCK_BOOTTIME` may count the sleep in whole seconds (S14), so neither tells a holder how long it slept. A holder must end its leases on resume; a grantor needs nothing, since a clock that stopped only makes it wait longer.
8. **Steps are harmless** (G12): only slews matter, so a daemon configured to step (`makestep` with no limit) would not hurt leases, but rock-solid cannot rely on how operators configure their daemons.
9. **A VM pause freezes every guest clock** (S15), so no clock inside the guest can tell a holder that a pause outlived its lease. This is a separate gap that the clock choice does not close.
10. Reading `CLOCK_MONOTONIC_RAW` costs a Go loop one ~140 ns syscall per pass, as `CLOCK_BOOTTIME` already does (C1).

## What is unverified

- ntpd and PTP daemons (`phc2sys`, which also adjusts frequency) were read, not run. chrony and timesyncd ran once each against one step size and one network path (V1-V3); a datacenter path with sub-millisecond jitter should behave like V2, which was not checked with a real NTP server.
- Other architectures. Everything here is x86-64; arm64's generic timer and its calibration, and the RTC resolution there, were not checked.
- Oscillator error. No measurement here of how far `CLOCK_MONOTONIC_RAW` drifts between real machines (typical crystals are within ±100 ppm, but S13 lets calibration be worse), nor of TSC calibration on hardware without CPUID frequency reporting. The heartbeat rate check measures it in production.
- VM pause and live migration were read from QEMU and Firecracker source and docs, not run; whether the VMClock device's disruption marker (`drivers/ptp/ptp_vmclock.c`) is available and usable by an unprivileged daemon was not checked.
- Hypervisors other than KVM (Xen, Hyper-V, VMware) and their paravirtual clocks were not checked.

## Design change

### Options weighed

| Option | Safe against | Cost | Verdict |
| --- | --- | --- | --- |
| Keep `CLOCK_BOOTTIME`, `lease_drift` 1% (today) | ntpd at its defaults | None | Unsafe under chrony or timesyncd slews (conclusion 3) |
| `CLOCK_BOOTTIME`, `lease_drift` widened to 37% | Every slew the kernel allows, if no `ntp_tick_adj` | A 4 s lease held for 2.5 s; correctness rests on a reading of every kernel knob, and a future kernel may add one | Rejected |
| Read `adjtimex` each renewal and shorten by the worst rate it shows | Slews in progress when the lease is taken | A daemon changes tick and frequency mid-lease (chrony re-plans each slew, S10); the safe bound is again the kernel's whole range | Rejected |
| Require `maxslewrate 500` in chrony, steps elsewhere | Configured nodes | rock-solid does not own the host's time daemon; timesyncd has no such setting; nothing checks it | A recommendation at most |
| Stop granting and holding while `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC_RAW` disagree (G13) | Slews | Leases, and so reads, stop for the whole slew: 13 s per second of offset under chrony with a clean source, 142 s for a 1 s step with internet sources (V1, V2) | Rejected: an availability cost for what counting on both clocks gives for free |
| `CLOCK_MONOTONIC_RAW` alone, holder ends leases on resume | Every slew | One more syscall per pass. Exposed to a TSC calibration the kernel never bounds (S13) | Close, but trades a daemon's slews for the kernel's calibration |
| **Both clocks: the holder ends on the faster, the grantor on the slower; holder ends leases on resume; node pairs measure their oscillators' relative rate** | Slews and calibration error, unless both of a node's clocks err the same way; the pair check catches that | One more ~140 ns syscall per loop pass; two readings per lease; a rate estimate per node pair | **Chosen** |
| Synchronized clocks with a maximum offset (CockroachDB) | Slews (an offset bound is indifferent to rate) | Needs a clock-sync service and self-fencing when offset exceeds the bound; the design chose bounded drift so it needs neither | Rejected |

How others handle it:

- **CockroachDB** bounds skew, not drift: lease expirations are hybrid logical clock timestamps from the requester's clock, propagated on every message (`pkg/kv/kvserver/storeliveness/doc.go:43, 127-133` at v25.4.0), and a node whose clock is more than 80% of `--max-offset` (500 ms) from a majority of others shuts itself down. A fast slew corrects offset, so it never hurts them; they pay with a synchronized-clock requirement.
- **etcd/raft** says outright that `ReadOnlyLeaseBased` "can be affected by clock drift. If the clock drift is unbounded, leader might keep the lease longer than it should" (`raft.go:62-66` at v3.6.0); it counts ticks from Go timers, which run on `CLOCK_MONOTONIC`, so it carries this exact exposure. The design already keeps etcd-raft's lease off for other reasons.
- **Ceph** PG read leases use `CLOCK_MONOTONIC` (`src/common/ceph_time.h:294` at v19.2.3), and its design note says monotonic clocks are "unaffected by time adjustments" (`doc/dev/osd_internals/stale_read.rst:44-50`). That holds for steps, not slews. Its lease is `osd_pool_default_read_lease_ratio` 0.8 × `osd_heartbeat_grace` 20 s = 16 s; the note describes heartbeats bounding the offset between OSD clocks, not their rates.
- **KVM** counts kvmclock on `CLOCK_MONOTONIC_RAW` plus the boot offset (S15) precisely so that host slews do not leak into guests.

### Where it is applied

- [docs/design.md](../../docs/design.md): the "Lease time" paragraph in [Epoch changes and failure detection](../../docs/design.md#epoch-changes-and-failure-detection) owns the rule; the Raft leader leases, sessions and the fencing safety argument point to it. Also the `Context` block, the `Resumed` event and the "Clock and timers" driver row in [Runtime in Go](../../docs/design.md#runtime-in-go), the placement simulation faults, the "Lease clock" row in [Alternatives considered](../../docs/design.md#alternatives-considered), the open question on leases across a VM pause, and [Appendix: clock slew spike](../../docs/design.md#appendix-clock-slew-spike).
- [docs/glossary.md](../../docs/glossary.md): "Monotonic clock, clock skew, clock drift, slewing".
- [docs/build-checklist.md](../../docs/build-checklist.md): "Epoch change model", "Peering and read leases", "Simulated network", "Leader leases", "Clock and timers".

### Test scenarios

`docs/test-scenarios.md` covers phase 1 only, so these wait here for the sections of "Clock and timers" and "Peering and read leases":

**Clock and timers**

- `ctx.Now` and `ctx.Raw` come from one pass's reads; with no adjustment they advance together within 1 ppm over 1 s.
- `CLOCK_BOOTTIME` gaining 2 s on `CLOCK_MONOTONIC` between passes delivers one `Resumed`; a slew of 22.6% delivers none.

**Peering and read leases (clock cases)**

- Holder's `CLOCK_BOOTTIME` 22.6% slow, its `CLOCK_MONOTONIC_RAW` exact: the lease ends at `primary_lease × 0.99` of real time. Holder's `CLOCK_MONOTONIC_RAW` 1% slow, its `CLOCK_BOOTTIME` exact: the same.
- Grantor's `CLOCK_BOOTTIME` 22.6% fast, its `CLOCK_MONOTONIC_RAW` exact: it joins peering only after `primary_lease` of real time. Grantor's `CLOCK_MONOTONIC_RAW` 1% fast, its `CLOCK_BOOTTIME` exact: the same.
- A grantor's wait, set as a timer on `ctx.Now`, that fires while `ctx.Raw` shows less than the lease sets another timer for the remainder and joins no peering before it.
- [property] Random rates per node within those bounds and random renewal timing: no holder's lease ever ends after its grantor's.
- A holder that receives `Resumed` ends every lease it holds before it serves another read; a grantor that receives `Resumed` keeps its grants and its waits.
- A peer whose heartbeat timestamps show its `CLOCK_MONOTONIC_RAW` 0.6% fast: the holder's margin for that grantor grows to 1.6% and the pair is reported; at 0.4% nothing changes.
