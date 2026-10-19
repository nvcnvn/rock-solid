# VM pause spike

**Question.** A lease holder trusts its own clocks, and a hypervisor that stops a virtual machine can stop every one of them: QEMU restores kvmclock to its value at the stop, and QEMU and Firecracker restore a snapshot's clock as it was saved ([clock slew spike](../clock-slew/), S15). Can a storage node in a VM learn that it was paused, snapshot-restored or live-migrated before it serves its next lease read, and what does checking cost per event-loop pass? If it can't, lease reads on VMs need a different rule. The [clock-skew study](../../studies/clock-skew/README.md) (recommendation 3) points at a clock the hypervisor provides, the VMClock device.

**Setup.** Host: Linux 6.18.44 in a Firecracker VM, 4 vCPUs, no `/dev/kvm`, no `/dev/ptp*`, Go 1.27.0. Written against commit `45978eb`. Nothing installed: [fetch.sh](fetch.sh) unpacks QEMU 10.2.1 (the first release with the `vmclock` device is 10.0.0), its libraries and glibc 2.43, the Ubuntu 26.04 kernel 7.0.0-14-generic (`CONFIG_PTP_1588_CLOCK_VMCLOCK=m`) and a static busybox (used only as `cpio`) from the Ubuntu archive into `vendor/sysroot`, and [env.sh](env.sh) runs QEMU through the unpacked loader. Stand-in: QEMU in TCG (software emulation, no KVM), q35, 1 vCPU, 256 MiB, `-device vmclock`, the kernel booted with a 1 MiB initramfs whose init is [vmpause_guest.c](vmpause_guest.c). The init loads `ptp_vmclock.ko.zst` itself, maps `/dev/vmclock0` read-only and prints, every 100 ms of guest time, the four clocks, the CMOS RTC and every VMClock field; [run.py](run.py) stamps each line with the host's `CLOCK_MONOTONIC` on arrival and drives QEMU over QMP. A clock's gain across an event is its change from the last sample before to the first after, set against the host's gap over the same two lines; a frozen clock gains one sample interval. TCG has no kvmclock: its guest TSC is QEMU's tick counter, which stops with the VM and migrates with it (S5), so TCG freezes guest time across a stop for a different reason than KVM does, and the KVM and Firecracker behaviour below is read from source, not run. [model.py](model.py) models the holder's lease rules (T6); [gocost/](gocost/) measures the marker read from Go.

```
./fetch.sh                                  # sysroot and cited sources into vendor/ (30 s)
./build.sh                                  # bin/: static init, initramfs, kernel
./run-host.sh            > results-host.txt # does this Firecracker host expose VMClock?
./run.py bench           > results-bench.txt  # T5, about 15 s
(cd gocost && go test -bench . -benchtime 2s -count 3) > results-gocost.txt
./run.py vm              > results-vm.txt     # T0-T4, about 3 minutes
python3 -I model.py      > results-model.txt  # T6, about 7 minutes
```

Expected before the run: the marker changes on a migration and a snapshot restore and not on a stop and continue (S1); every guest clock stands still across a stop (T1); reading the marker costs under 50 ns natively, less than one clock read (T5); the design's rate bound sees a pause only at the first grant after resume, so stale reads stay possible until then (T6). Tests that try to break it: a stop that is not a QMP stop (T2b), a snapshot taken of a running guest (T2c), a snapshot restored twice (T3c), a migration held or cancelled at switchover (T4b, T4c).

## Results

### What the sources say

QEMU [v10.2.1](https://github.com/qemu/qemu/tree/v10.2.1), Linux [v7.0](https://github.com/torvalds/linux/tree/v7.0), Firecracker [v1.17.0](https://github.com/firecracker-microvm/firecracker/tree/v1.17.0).

| Test | Result |
| --- | --- |
| S1 QEMU's `vmclock` | Opt-in `-device vmclock` on x86 PC machines, absent before 10.0.0. Its page is guest RAM ([`hw/acpi/vmclock.c:138`](https://github.com/qemu/qemu/blob/v10.2.1/hw/acpi/vmclock.c#L138)), so a guest reads it with a plain load. QEMU fills only the header: `counter_id` invalid, `clock_status` unknown, `flags` 0 (`vmclock.c:144-150`), so no time, no PTP clock, no notification and no `vm_generation_counter`. `vmclock_update_guest` adds one to `disruption_marker` under `seq_count` (`vmclock.c:64-85`); it runs at realize and from the device's `post_load` (`vmclock.c:91-97`), that is on every incoming migration, migration from a file and `loadvm`, and never on `stop`, `cont` or `savevm`. `post_load` runs inside `qemu_loadvm_state` ([`migration/migration.c:885`](https://github.com/qemu/qemu/blob/v10.2.1/migration/migration.c#L885)) before the bottom half that calls `vm_start` (`migration.c:911`, `847`, `854`): **no guest instruction after a restore sees the old marker** |
| S2 Firecracker | VMClock since 1.15.0 (`CHANGELOG.md`), always present, with `VM_GEN_COUNTER_PRESENT` and `NOTIFICATION_PRESENT` set and no time (`src/vmm/src/devices/acpi/vmclock.rs:102`). On snapshot load `do_post_restore` adds one to `disruption_marker` and `vm_generation_counter` and raises the device's interrupt (`vmclock.rs:122-150`, called from `device_manager/persist.rs:212-213`) before the vCPUs resume. Pause and resume only stop and start the vCPUs (`src/vmm/src/lib.rs:463-483`): no clock is saved or set, so kvmclock keeps counting and **a paused guest's clocks show the pause**. A restore sets kvmclock to its saved value unless `clock_realtime` is set (`arch/x86_64/vm.rs:137-155`). No live migration; no RTC on x86-64 (`devices/legacy/mod.rs:10-11`) |
| S3 Linux `ptp_vmclock` | Registers `/dev/vmclock0` when the region is a page or larger ([`drivers/ptp/ptp_vmclock.c:740`](https://github.com/torvalds/linux/blob/v7.0/drivers/ptp/ptp_vmclock.c#L740)), with the default mode 0600, so a daemon needs root or a udev rule. `mmap` gives a read-only, cacheable mapping of the page (`ptp_vmclock.c:372-389`); `read()` copies it under the seqlock (`:391-435`); `poll()` returns `POLLHUP` at once unless the hypervisor sets `NOTIFICATION_PRESENT` (`:447-448`). A PTP clock appears only when the device gives a TSC or arch-counter time (`:337-365`, `:748`), which neither QEMU nor Firecracker does. The ABI promises only that the marker "changes to another non-repeating value" on a disruption ([`vmclock-abi.h:83-88`](https://github.com/torvalds/linux/blob/v7.0/include/uapi/linux/vmclock-abi.h#L83)) |
| S4 KVM under QEMU | `kvmclock_vm_state_change` saves `KVM_GET_CLOCK` at `stop` and sets the same value at `cont` ([`hw/i386/kvm/clock.c:163-221`](https://github.com/qemu/qemu/blob/v10.2.1/hw/i386/kvm/clock.c#L163)): **a guest on kvm-clock does not see a stop**. The guest TSC is written back only at `KVM_PUT_RESET_STATE` or above ([`target/i386/kvm/kvm.c:4019-4020`](https://github.com/qemu/qemu/blob/v10.2.1/target/i386/kvm/kvm.c#L4019)), on reset and restore, not at `cont`, so it keeps counting through a stop; a guest prefers the `tsc` clocksource when the host exposes invariant TSC ([`arch/x86/kernel/kvmclock.c:343-346`](https://github.com/torvalds/linux/blob/v7.0/arch/x86/kernel/kvmclock.c#L343)), which QEMU does only with `+invtsc`, and that blocks migration unless `tsc-frequency` is set (`kvm.c:2332-2339`) |
| S5 TCG, the stand-in | The guest TSC is `cpus_get_elapsed_ticks()` (`hw/i386/x86-cpu.c:35-38`), which stops at every VM stop (`cpu_disable_ticks`, `system/cpus.c:302`) and restarts at start (`cpus.c:794`), and migrates as the `timer` section (`system/cpu-timers.c:220-228`). It never returns less than its last value (`cpu-timers.c:49-53`) |
| S6 CMOS RTC | QEMU's RTC counts `QEMU_CLOCK_HOST` by default (`system/rtc.c:145`), so it keeps host time through every event, in whole seconds |
| S7 network while stopped | virtio-net refuses packets while the VM is stopped (`hw/net/virtio-net.c:1635`, `net/net.c:633-637`), so they wait in the backend, for a tap device the host kernel's queue, which drops what overflows it, and reach the guest in arrival order at `cont`. Packets sent during a migration's downtime or to a restored VM go wherever the network delivered them then |
| S8 `ptp_kvm` | A PTP clock whose reading is the host's `CLOCK_REALTIME`, fetched by the `KVM_HC_CLOCK_PAIRING` hypercall on every read ([`drivers/ptp/ptp_kvm_x86.c:77-86`](https://github.com/torvalds/linux/blob/v7.0/drivers/ptp/ptp_kvm_x86.c#L77)): a syscall plus a VM exit. It needs a guest on kvm-clock (`ptp_kvm_x86.c:48`) and a host whose clocksource is the TSC (`arch/x86/kvm/x86.c:3066-3074`, `:10254`), and works under any KVM-based hypervisor, since KVM answers the hypercall. Host time runs on through a stop, so comparing it with `CLOCK_MONOTONIC_RAW` would show one, subject to the host's own slews and steps |

### Measured on this host (unprivileged, read-only)

Raw output: [results-host.txt](results-host.txt).

| Test | Result |
| --- | --- |
| H1 this Firecracker VM | Exposes the VMClock ACPI device (`AMZNC10C`, `\_SB_.VCLK`), but its kernel is built without `CONFIG_PTP_1588_CLOCK_VMCLOCK` and without `/dev/mem`, so nothing in it can read the page: **a hypervisor offering the device is not enough; the guest kernel must have the driver** |

### Measured in the TCG guest

Raw output: [results-vm.txt](results-vm.txt), 57 checks passed, 4 failed (T3b's clocks, explained below). Gains are for the four guest clocks, which moved together within 4 ms in every row.

| Test | Result |
| --- | --- |
| T0 device | `/dev/vmclock0` maps; mode 0600, `counter_id` 0xff, `flags` 0, marker 1 at boot; `poll()` returns `POLLHUP` |
| T1 QMP `stop` 10 s, `cont` | Host gap 10.103 s; every clock gained 0.102 s (one sample): **frozen**. RTC +10 s |
| T2 marker across T1 | 1 → 1, `seq_count` unchanged: **a stop leaves no trace in VMClock** |
| T2b `SIGSTOP` of the QEMU process 10 s | Every clock gained 10.003 s over a 10.003 s gap; marker unchanged. A stall that bypasses QEMU's run state keeps the clocks counting |
| T2c `savevm` of the running guest (0.81 s) | Clocks gained 0.100 s over a 0.911 s gap; marker unchanged: an internal snapshot is a stop |
| T3 migrate to a file, restore in a new process 10 s later | Clocks gained 0.108 s over 11.216 s; RTC +11 s; **marker 2 → 3** |
| T3b `loadvm` into the same process 10 s later | **Marker 1 → 2**. The clocks gained 11.17 s over 12.24 s, the time the VM ran after the snapshot: a TCG artifact, since QEMU's tick counter never goes back within one process (S5); kvmclock restores the snapshot's value |
| T3c the same file restored a second time | Marker 3 again, the first restore's value, and not the saved guest's 2: markers repeat across clones, so only a change means anything |
| T4 live migration on localhost (2 ms downtime) | **Marker 3 → 4**; clocks gained 0.201 s over 0.304 s |
| T4b live migration held 10 s at switchover (`pause-before-switchover`, 10,322 ms downtime) | **Marker 4 → 5**; clocks gained 0.102 s over 10.454 s; RTC +11 s |
| T4c migration cancelled after 10 s at switchover | The source resumed: clocks gained 0.100 s over 10.408 s, **marker unchanged**. A migration that fails at switchover is a stop |

`vm_generation_counter` stayed 0 and `flags` 0 throughout, as S1 says.

### Cost per read (T5)

Raw output: [results-bench.txt](results-bench.txt), [results-gocost.txt](results-gocost.txt). p50 over 201 batches. Under KVM the page is guest RAM, so the host's read of an ordinary mapped page is the native cost; TCG inflates everything.

| Test | Result |
| --- | --- |
| T5a host, native, C | **Marker 0.7 ns**; `clock_gettime` through the vDSO 27 to 28 ns; `CLOCK_MONOTONIC_RAW` by raw syscall 157 ns |
| T5b host, Go | **Marker 2.8 ns** with `atomic.LoadUint32`/`LoadUint64` on an `mmap`'d page, 0 allocations; `CLOCK_MONOTONIC_RAW` by `syscall.RawSyscall` 196 to 205 ns |
| T5c TCG guest | Marker 13.6 ns; vDSO clocks 194 to 199 ns; clock syscalls 943 to 1,103 ns; `pread` of `/dev/vmclock0` 1,745 ns; `ioctl(RTC_RD_TIME)` 5.5 µs, p99 51 µs |

### Modelled lease rules (T6)

Raw output: [results-model.txt](results-model.txt). `primary_lease` 4 s, `lease_drift` 1%, a grant round trip every 1 s heartbeat, `rate_window` 10 s, round trips 0.2 to 1 ms; 100 pause phases per pause length P from 0.5 to 30 s. Stale exposure is the real time after resume in which the holder trusts a lease its grantor no longer keeps; blocked is the time in which the grantor keeps one the holder will not use. Grants in flight during the pause are queued (a stop: S7) or lost (a migration or restore). On a detection the holder asks for a grant at once and restarts its rate window.

| Test | Result |
| --- | --- |
| T6a `rate`: the rate bound alone (the design) | No exposure up to P = 2 s: the grantor's lease still runs at resume, since it was renewed within a heartbeat. From P = 3.5 s, **up to 0.99 s of stale exposure** (p50 0.50 s), until the first grant round trip after resume; afterwards the bound counts the pause as a fast grantor and **shortens every lease for up to 4.0 s** |
| T6b `step`: plus a step check on each grant: `g_b − g_a > (r_b − s_a)(1 + lease_drift / 2)` ends every lease | Same exposure (0.99 s): a grant in flight across the pause was stamped before it; blocked ≤ 1 ms |
| T6c `hb`: the step check on every message from the grantor stamped with its `CLOCK_MONOTONIC_RAW`, its own heartbeats included, handled before reads | **0 with messages queued**, at every P; up to 0.94 s with them lost |
| T6d `marker`: the rate bound plus the VMClock marker read each pass | **0 with messages lost** (the restore and migration cases); with them queued the marker does not change and T6a's 0.99 s remains |
| T6e `all`: T6c and T6d together | **0 in every row**; blocked ≤ 1 ms |

## Conclusions

1. **A restore or a migration is visible before the guest runs again.** QEMU 10.0 and later with `-device vmclock`, and Firecracker 1.15 and later, change the disruption marker before the vCPUs start (S1, S2); every restore and migration measured changed it, a 2 ms-downtime migration included (T3, T3b, T4, T4b). A holder that compares the marker with the value it last saw, each pass, and ends its leases on a change has no stale exposure in these cases (T6d).
2. **Checking costs nothing a loop would notice:** 0.7 ns in C and 2.8 ns from Go natively, against 157 to 205 ns for each of the two clock syscalls the loop already makes (T5). It must be the mapped page: `read()` is a syscall, and `poll()` cannot wake a loop on QEMU (S3, T0). The device is opt-in on QEMU, needs a guest kernel with the driver (H1) and root or a udev rule for `/dev/vmclock0`.
3. **A stop that is not a restore leaves no trace inside the guest.** A QMP `stop`, an internal snapshot of a running VM and a migration cancelled at switchover froze every guest clock and left the marker alone (T1, T2, T2c, T4c), and KVM's kvmclock does the same at `cont` (S4). Only QEMU's RTC saw them, in whole seconds and microseconds per read (S6, T5), and Firecracker has none. Stalls that bypass the hypervisor's run state, and Firecracker's pause, keep the clocks counting, so leases expire on time (T2b, S2).
4. **The design's rate bound neither catches a pause in time nor survives one.** Stale reads need a pause longer than `primary_lease` minus a heartbeat, 3 s; after one the holder serves on its old lease until its next grant round trip, up to 0.99 s, and then the bound shortens every lease for up to 4 s (T6a).
5. **The grantors' clocks catch a stop, if their messages are read first.** Stamping every message a grantor sends with its `CLOCK_MONOTONIC_RAW` and applying the step check to each, before the reads of the same pass, ends the old leases at resume whenever messages sent during the stop were queued, as QEMU's virtio-net leaves them (S7, T6c). Restarting the rate window on a detection keeps the pause from also counting as a fast grantor, so the cost is one round trip (T6b, T6e).
6. **Marker and step check together leave no exposure in the model** (T6e). The marker covers lost messages, the step check covers stops.
7. **Compare markers only for change:** two restores of one snapshot show the same value (T3c).
8. **What remains open:** a stop whose queued messages the backend dropped, or during which the network was also cut, followed by a client request that reaches the holder before any grantor message: up to one heartbeat of stale reads. Hosts that never stop storage VMs with frozen clocks (Firecracker's pause, or QEMU with invariant TSC and the `tsc` clocksource, S4) close it; so would reading `ptp_kvm`'s host clock each pass that serves lease reads, at the cost of a syscall and a VM exit (S8, not measured), or a read rule that confirms with each grantor per batch of reads instead of a lease. Neither KVM nor Firecracker was run.

## What is unverified

- KVM and Firecracker. There is no `/dev/kvm` here, so kvmclock across `stop`, `cont`, `loadvm` and migration (S4), Firecracker's pause, restore and VMClock interrupt (S2), `ptp_kvm` and its cost (S8), and the cost of the marker read in a KVM guest were read or inferred, not measured. `spikes/vm-pause` runs unchanged with `-accel kvm` on a host that has it.
- Networking. The guest had no NIC, so S7's queueing, the order in which a stopped VM's backlog reaches its sockets, and how much a tap queue holds before it drops were not measured; T6c rests on them.
- Several event loops. The model has one holder that reads a grantor's messages before its reads; with one loop per core, a step seen on one loop's connection must reach the loops serving reads first.
- Other hypervisors and clouds: whether Xen, Hyper-V, VMware or public clouds expose VMClock, and whether arm64 guests behave the same, was not checked.
