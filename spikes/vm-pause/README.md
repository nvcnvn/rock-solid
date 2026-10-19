# VM pause spike

**Question.** A lease holder trusts its own clocks, and a hypervisor that stops a virtual machine can stop every one of them: QEMU restores kvmclock to its value at the stop, and QEMU and Firecracker restore a snapshot's clock as it was saved ([clock slew spike](../clock-slew/), S15). Can a storage node in a VM learn that it was paused, snapshot-restored or live-migrated before it serves its next lease read, and what does checking cost per event-loop pass? If it can't, lease reads on VMs need a different rule. The [clock-skew study](../../studies/clock-skew/README.md) (recommendation 3) points at a clock the hypervisor provides, the VMClock device.

**Setup.** Two hosts, the same guest. Nothing is installed on either: [fetch.sh](fetch.sh) unpacks QEMU 10.2.1 (the first release with the `vmclock` device is 10.0.0), its libraries and glibc 2.43, the Ubuntu 26.04 kernel 7.0.0-14-generic (`CONFIG_PTP_1588_CLOCK_VMCLOCK=m`, `CONFIG_PTP_1588_CLOCK_KVM=m`) and a static busybox (used only as `cpio`) from the Ubuntu archive into `vendor/sysroot`, downloads the Firecracker v1.17.0 static release binary (the newest release) and checks its SHA-256, and [env.sh](env.sh) runs QEMU through the unpacked loader. [build.sh](build.sh) builds a 1 MiB initramfs whose init is [vmpause_guest.c](vmpause_guest.c), and decompresses the kernel to an ELF `vmlinux` for Firecracker with Linux v7.0's `scripts/extract-vmlinux`. The init loads `ptp_vmclock` and `ptp_kvm` itself, maps `/dev/vmclock0` read-only, opens the `KVM virtual PTP` clock if one registers, and prints, every 100 ms of guest time, the four clocks, the guest TSC (scaled by a 200 ms calibration against `CLOCK_MONOTONIC_RAW` at boot), the `ptp_kvm` clock, the CMOS RTC and every VMClock field. When the device sets `NOTIFICATION_PRESENT` it waits between samples in `poll()` on `/dev/vmclock0` and prints a `W` line on each wakeup. [run.py](run.py) stamps each line with the host's `CLOCK_MONOTONIC` on arrival and drives QEMU over QMP or Firecracker over its API socket. A clock's gain across an event is its change from the last sample before to the first after, set against the host's gap over the same two lines; a frozen clock gains one sample interval. Every expectation in run.py was written from the sources before the run. Guests have 1 vCPU, 256 MiB and no NIC.

- **Cloud host** (TCG, T0 to T6): Linux 6.18.44 in a Firecracker VM, 4 vCPUs, no `/dev/kvm`, Go 1.27.0, harness at commit `45978eb`. QEMU in TCG (software emulation), q35, `-device vmclock`. TCG has no kvmclock: its guest TSC is QEMU's tick counter, which stops with the VM and migrates with it (S5), so TCG freezes guest time across a stop for a different reason than KVM does. [model.py](model.py) models the holder's lease rules (T6); [gocost/](gocost/) measures the marker read from Go.
- **Bare-metal host** (KVM and Firecracker, T1 to T5, F0 to F3): Linux 6.17.0-1033-oem, Ubuntu 24.04, Intel Core Ultra 5 235U, host clocksource `tsc` (`constant_tsc`, `nonstop_tsc`), `/dev/kvm` by ACL, no root, no tap device, so the network tests (S7) were not run. QEMU with `-accel kvm -cpu host`, whose guest picks `kvm-clock`, and again with `-cpu host,+invtsc,tsc-frequency=3187200000` (the host's KVM TSC rate, read with QMP `qom-get` of the CPU's `tsc-frequency`; QEMU refuses to migrate an `invtsc` guest without it), whose guest picks `tsc`. Firecracker v1.17.0 without a CPU template, run without the jailer: it passes the host's invariant TSC through, so its guest picks `tsc`; it was run again with `clocksource=kvm-clock` on the kernel command line. Everything ran pinned to cores 8 to 13, with snapshots and sockets on tmpfs.

```
./fetch.sh                                  # sysroot, Firecracker and cited sources into vendor/
./build.sh                                  # bin/: static init, initramfs, vmlinuz, vmlinux
# cloud host, no /dev/kvm
./run-host.sh            > results-host.txt # does this Firecracker host expose VMClock?
./run.py bench           > results-bench.txt  # T5 in TCG, about 15 s
(cd gocost && go test -bench . -benchtime 2s -count 3) > results-gocost.txt
./run.py vm              > results-vm.txt     # T0-T4 in TCG, about 3 minutes
python3 -I model.py      > results-model.txt  # T6, about 7 minutes
# bare-metal host with /dev/kvm
export WORK=/dev/shm/vmpause
ACCEL=kvm ./run.py vm    > results-vm-kvm.txt         # T1-T4 on kvm-clock, about 3 minutes
INV=host,+invtsc,tsc-frequency=3187200000
ACCEL=kvm CPU=$INV ./run.py vm    > results-vm-kvm-invtsc.txt
ACCEL=kvm ./run.py bench          > results-bench-kvm.txt
ACCEL=kvm CPU=$INV ./run.py bench > results-bench-kvm-invtsc.txt
./run.py fc              > results-fc-firecracker.txt # F0-F3, about 2 minutes
APPEND=clocksource=kvm-clock ./run.py fc > results-fc-firecracker-kvmclock.txt
./run.py fc-bench        > results-bench-firecracker.txt
./run.py vm              > results-vm-tcg-baremetal.txt
```

`results-vm.txt` and `results-bench.txt` come from the harness before it sampled the TSC and `ptp_kvm`. The current harness, run in TCG on the bare-metal host (`./run.py vm`), gave the same results with the TSC moving with the other clocks: [results-vm-tcg-baremetal.txt](results-vm-tcg-baremetal.txt), 64 checks passed, 5 failed (T3b, as below).

Expected before the run: the marker changes on a migration and a snapshot restore and not on a stop and continue (S1, S2); a stop freezes kvmclock while the guest TSC runs on, a restore or migration freezes both (S4); a Firecracker pause freezes nothing and its restore freezes both unless `clock_realtime` is set (S2); `ptp_kvm` keeps host time through every event (S8); reading the marker costs under 50 ns, less than one clock read (T5); the design's rate bound sees a pause only at the first grant after resume (T6). Tests that try to break it: a stop that is not a QMP stop (T2b), a snapshot taken of a running guest (T2c), a snapshot restored twice (T3c, F3b), a migration held or cancelled at switchover (T4b, T4c), a Firecracker snapshot taken while paused and then resumed (F2).

## Results

### What the sources say

QEMU [v10.2.1](https://github.com/qemu/qemu/tree/v10.2.1), Linux [v7.0](https://github.com/torvalds/linux/tree/v7.0), Firecracker [v1.17.0](https://github.com/firecracker-microvm/firecracker/tree/v1.17.0). The last column gives what the run showed.

| Test | Result | Run |
| --- | --- | --- |
| S1 QEMU's `vmclock` | Opt-in `-device vmclock` on x86 PC machines, absent before 10.0.0. Its page is guest RAM ([`hw/acpi/vmclock.c:138`](https://github.com/qemu/qemu/blob/v10.2.1/hw/acpi/vmclock.c#L138)), so a guest reads it with a plain load. QEMU fills only the header: `counter_id` invalid, `clock_status` unknown, `flags` 0 (`vmclock.c:144-150`), so no time, no PTP clock, no notification and no `vm_generation_counter`. `vmclock_update_guest` adds one to `disruption_marker` under `seq_count` (`vmclock.c:64-85`); it runs at realize and from the device's `post_load` (`vmclock.c:91-97`), that is on every incoming migration, migration from a file and `loadvm`, and never on `stop`, `cont` or `savevm`. `post_load` runs inside `qemu_loadvm_state` ([`migration/migration.c:885`](https://github.com/qemu/qemu/blob/v10.2.1/migration/migration.c#L885)) before the bottom half that calls `vm_start` (`migration.c:911`, `847`, `854`): no guest instruction after a restore sees the old marker | Confirmed in TCG and KVM |
| S2 Firecracker | VMClock since 1.15.0 (`CHANGELOG.md`), always present, with `VM_GEN_COUNTER_PRESENT` and `NOTIFICATION_PRESENT` set and no time (`src/vmm/src/devices/acpi/vmclock.rs:102`). On snapshot load `do_post_restore` adds one to `disruption_marker` and `vm_generation_counter` and raises the device's interrupt (`vmclock.rs:122-150`, called from `device_manager/persist.rs:212-213`) before the vCPUs resume. Pause and resume only stop and start the vCPUs (`src/vmm/src/lib.rs:463-483`): no clock is saved or set, so kvmclock and the TSC keep counting. A snapshot saves `KVM_GET_CLOCK` (`arch/x86_64/vm.rs:190`) and the vCPU MSRs, the TSC among them (`arch/x86_64/vcpu.rs:40-46`); a restore sets both back, and with `clock_realtime` moves kvmclock on by the host's real time since the save (`vm.rs:137-155`; KVM: [`arch/x86/kvm/x86.c:7222-7228`](https://github.com/torvalds/linux/blob/v7.0/arch/x86/kvm/x86.c#L7222)), not the TSC. No live migration; no RTC on x86-64 (`devices/legacy/mod.rs:10-11`) | Confirmed (F0-F3) |
| S3 Linux `ptp_vmclock` | Registers `/dev/vmclock0` when the region is a page or larger ([`drivers/ptp/ptp_vmclock.c:740`](https://github.com/torvalds/linux/blob/v7.0/drivers/ptp/ptp_vmclock.c#L740)), with the default mode 0600, so a daemon needs root or a udev rule. `mmap` gives a read-only, cacheable mapping of the page (`ptp_vmclock.c:372-389`); `read()` copies it under the seqlock and records the `seq_count` it saw for the file (`:391-435`); `poll()` returns `POLLHUP` at once unless the hypervisor sets `NOTIFICATION_PRESENT`, and otherwise `POLLIN` while `seq_count` differs from what the file last read (`:437-455`). A PTP clock appears only when the device gives a TSC or arch-counter time (`:337-365`, `:748`), which neither QEMU nor Firecracker does. The ABI promises only that the marker "changes to another non-repeating value" on a disruption ([`vmclock-abi.h:83-88`](https://github.com/torvalds/linux/blob/v7.0/include/uapi/linux/vmclock-abi.h#L83)) | Confirmed |
| S4 KVM under QEMU | `kvmclock_vm_state_change` saves `KVM_GET_CLOCK` at a stop and sets it at the next start ([`hw/i386/kvm/clock.c:163-221`](https://github.com/qemu/qemu/blob/v10.2.1/hw/i386/kvm/clock.c#L163)), so kvmclock stands still across a stop. But `kvmclock_pre_save` reads the clock again when the device state is saved unless the VM was stopped by the user (`clock.c:266-287`), so a migration counts the time between its stop and its device-state save, including a hold at switchover. The guest TSC is read at the stop (`kvm_synchronize_all_tsc`, `clock.c:209`) and written back only at `KVM_PUT_RESET_STATE` or above ([`target/i386/kvm/kvm.c:4019-4020`](https://github.com/qemu/qemu/blob/v10.2.1/target/i386/kvm/kvm.c#L4019)), on reset and restore, not at `cont`, so it runs on through a stop and stands still across a restore or migration. A guest prefers the `tsc` clocksource when the host exposes invariant TSC ([`arch/x86/kernel/kvmclock.c:343-346`](https://github.com/torvalds/linux/blob/v7.0/arch/x86/kernel/kvmclock.c#L343)), which QEMU does only with `+invtsc`, and that blocks migration unless `tsc-frequency` is set (`kvm.c:2332-2339`) | **The read of `pre_save` was missed before the run; T4b found it.** The rest confirmed |
| S5 TCG, the stand-in | The guest TSC is `cpus_get_elapsed_ticks()` (`hw/i386/x86-cpu.c:35-38`), which stops at every VM stop (`cpu_disable_ticks`, `system/cpus.c:302`) and restarts at start (`cpus.c:794`), and migrates as the `timer` section (`system/cpu-timers.c:220-228`). It never returns less than its last value (`cpu-timers.c:49-53`) | Confirmed |
| S6 CMOS RTC | QEMU's RTC counts `QEMU_CLOCK_HOST` by default (`system/rtc.c:145`), so it keeps host time through every event, in whole seconds | Confirmed |
| S7 network while stopped | virtio-net refuses packets while the VM is stopped (`hw/net/virtio-net.c:1635`, `net/net.c:633-637`), so they wait in the backend, for a tap device the host kernel's queue, which drops what overflows it, and reach the guest in arrival order at `cont`. Packets sent during a migration's downtime or to a restored VM go wherever the network delivered them then | Not run: no NIC in the guests, no tap device on either host |
| S8 `ptp_kvm` | A PTP clock whose reading is the host's `CLOCK_REALTIME`, fetched by the `KVM_HC_CLOCK_PAIRING` hypercall on every read ([`drivers/ptp/ptp_kvm_x86.c:77-86`](https://github.com/torvalds/linux/blob/v7.0/drivers/ptp/ptp_kvm_x86.c#L77)): a syscall plus a VM exit. It needs kvmclock set up (`ptp_kvm_x86.c:48`), which `kvmclock_init` does whenever the hypervisor advertises it, whichever clocksource then wins (`kvmclock.c:316`), and a host whose clocksource is the TSC (`arch/x86/kvm/x86.c:3066-3074`, `:10254`). KVM answers the hypercall, so it works under any KVM-based hypervisor | Confirmed, including in guests on the `tsc` clocksource (QEMU `+invtsc`, Firecracker) |

### Measured on the cloud host (unprivileged, read-only)

Raw output: [results-host.txt](results-host.txt).

| Test | Result |
| --- | --- |
| H1 the cloud Firecracker VM | Exposes the VMClock ACPI device (`AMZNC10C`, `\_SB_.VCLK`), but its kernel is built without `CONFIG_PTP_1588_CLOCK_VMCLOCK` and without `/dev/mem`, so nothing in it can read the page: **a hypervisor offering the device is not enough; the guest kernel must have the driver** |

### Measured in the TCG guest

Raw output: [results-vm.txt](results-vm.txt), 57 checks passed, 4 failed (T3b's clocks, explained below). Gains are for the four guest clocks, which moved together within 4 ms in every row.

| Test | Result |
| --- | --- |
| T0 device | `/dev/vmclock0` maps; mode 0600, `counter_id` 0xff, `flags` 0, marker 1 at boot; `poll()` returns `POLLHUP` |
| T1 QMP `stop` 10 s, `cont` | Host gap 10.103 s; every clock gained 0.102 s (one sample): frozen. RTC +10 s |
| T2 marker across T1 | 1 → 1, `seq_count` unchanged: a stop leaves no trace in VMClock |
| T2b `SIGSTOP` of the QEMU process 10 s | Every clock gained 10.003 s over a 10.003 s gap; marker unchanged |
| T2c `savevm` of the running guest (0.81 s) | Clocks gained 0.100 s over a 0.911 s gap; marker unchanged: an internal snapshot is a stop |
| T3 migrate to a file, restore in a new process 10 s later | Clocks gained 0.108 s over 11.216 s; RTC +11 s; marker 2 → 3 |
| T3b `loadvm` into the same process 10 s later | Marker 1 → 2. The clocks gained 11.17 s over 12.24 s, the time the VM ran after the snapshot: a TCG artifact, since QEMU's tick counter never goes back within one process (S5); under KVM they stand still (below) |
| T3c the same file restored a second time | Marker 3 again, the first restore's value, and not the saved guest's 2: markers repeat across clones, so only a change means anything |
| T4 live migration on localhost (2 ms downtime) | Marker 3 → 4; clocks gained 0.201 s over 0.304 s |
| T4b live migration held 10 s at switchover (`pause-before-switchover`, 10,322 ms downtime) | Marker 4 → 5; clocks gained 0.102 s over 10.454 s; RTC +11 s |
| T4c migration cancelled after 10 s at switchover | The source resumed: clocks gained 0.100 s over 10.408 s, marker unchanged |

`vm_generation_counter` stayed 0 and `flags` 0 throughout, as S1 says.

### Measured in KVM guests under QEMU

Raw output: [results-vm-kvm.txt](results-vm-kvm.txt) (guest on `kvm-clock`, 73 checks passed, 4 failed: T4b's clocks, which S4's `pre_save` explains) and [results-vm-kvm-invtsc.txt](results-vm-kvm-invtsc.txt) (`+invtsc`, guest on `tsc`, 77 passed). Gains in seconds over the host gap; "clocks" are `CLOCK_MONOTONIC_RAW`, `BOOTTIME`, `MONOTONIC` and `REALTIME`, which moved together in every row. The `ptp_kvm` clock gained the host gap within 1 ms in every row of both runs, and the RTC within a second.

| Test | `kvm-clock` guest: clocks / TSC / host | `+invtsc`, `tsc` guest: clocks / host | Marker |
| --- | --- | --- | --- |
| T0 device | `ptp_kvm` registers as `ptp0` in both; VMClock as in TCG | | 1 at boot |
| T1 QMP `stop` 10 s, `cont` | **0.100 / 10.101 / 10.101: kvmclock frozen, TSC ran on** | **10.004 / 10.004: the stop shows** | unchanged |
| T2b `SIGSTOP` 10 s | 10.002 / 10.002 / 10.003 | 10.003 / 10.003 | unchanged |
| T2c `savevm` of the running guest | 0.100 / 0.102 / 0.102 (0.10 s save, under one sample) | 0.100 / 0.100 (0.07 s save) | unchanged |
| T3b `loadvm` 10 s later | 0.120 / 0.027 / 11.202 | 0.100 / 11.265 | changed |
| T3 migrate to a file, restore 10 s later in a new process | 0.100 / 0.100 / 10.598 | 0.100 / 10.568 | changed |
| T3c the same file restored again | first restore's marker again | | |
| T4 live migration, 1 ms downtime | 0.100 / 0.100 / 0.104 | 0.100 / 0.108 | changed |
| T4b live migration held 10 s at switchover (10,338 and 10,350 ms downtime) | **10.409 / 0.075 / 10.415: kvmclock counted the hold, the TSC did not** | 0.100 / 10.453 | changed |
| T4c migration cancelled after 10 s at switchover | 0.100 / 10.446 / 10.449 | **10.414 / 10.414: the stop shows** | unchanged |

The `tsc` guest kept that clocksource through every event: a 10 s forward jump of the TSC at `cont` did not make the kernel's watchdog reject it.

### Measured in Firecracker guests

Raw output: [results-fc-firecracker.txt](results-fc-firecracker.txt) (default, guest on `tsc`) and [results-fc-firecracker-kvmclock.txt](results-fc-firecracker-kvmclock.txt) (`clocksource=kvm-clock`), 48 checks passed in each. The restores load the snapshot F2 took, so their host gap runs from the last sample before F2's pause.

| Test | `tsc` guest: clocks / TSC / host | `kvm-clock` guest: clocks / TSC / host | Marker, generation |
| --- | --- | --- | --- |
| F0 device | `flags` 0x300 (`VM_GEN_COUNTER_PRESENT`, `NOTIFICATION_PRESENT`); `seq_count`, marker and generation 0 at boot; `poll()` blocks instead of returning `POLLHUP`; `ptp_kvm` registers; no `/dev/rtc0` | same | 0, 0 |
| F1 pause 10 s, resume | **10.003 / 10.003 / 10.002: the pause shows** | **10.003 / 10.003 / 10.003** | unchanged; no wakeup |
| F2 pause, full snapshot (0.11 s), resume 10 s later | 10.108 / 10.108 / 10.108 | 10.111 / 10.111 / 10.111 | unchanged |
| F3 snapshot loaded in a new process (2 ms) | 0.100 / 0.100 / 20.795 | 0.101 / 0.101 / 20.799 | **0 → 1, 0 → 1; `poll()` woke at once with the new marker**, 96 ms before the next sample was due |
| F3b the same snapshot loaded again | 0.100 / 0.100 / 22.965 | 0.101 / 0.101 / 22.978 | 1 again: the first restore's values |
| F3c loaded with `clock_realtime: true` | 0.100 / 0.100 / 25.138 | **25.050 / 0.006 / 25.051: kvmclock moved on, the TSC did not** | 0 → 1, 0 → 1, woke |

The `ptp_kvm` clock gained the host gap within 2 ms in every row.

### Cost per read (T5)

Raw output: [results-bench.txt](results-bench.txt), [results-gocost.txt](results-gocost.txt) (cloud host), [results-bench-kvm.txt](results-bench-kvm.txt), [results-bench-kvm-invtsc.txt](results-bench-kvm-invtsc.txt), [results-bench-firecracker.txt](results-bench-firecracker.txt) (bare-metal host). p50 over 201 batches, in ns.

| Read | Cloud host, native | TCG guest | Bare-metal host, native | KVM, `kvm-clock` | KVM, `+invtsc` | Firecracker |
| --- | --- | --- | --- | --- | --- | --- |
| **Marker, mapped page, seqlock** | **0.7** (Go: **2.8**, 0 allocations) | 13.6 | 0.8 | **0.8** | 0.8 | **0.8** |
| vDSO clock | 27 to 28 | 194 to 199 | 16.8 to 16.9 | 25.8 to 26.4 | 20.9 to 21.8 | 21.4 to 21.8 |
| Clock by raw syscall | 157 to 159 (Go: 196 to 205) | 943 to 1,103 | 121.5 to 124.7 | 136.8 to 140.0 | 129.6 to 132.4 | 132.2 to 132.8 |
| `rdtsc` | | | 7.8 | 7.7 | 7.8 | 7.7 |
| **`ptp_kvm` clock, `clock_gettime`** | | | | **1,607** (p99 1,689) | 1,624 | 1,817 |
| `pread` of `/dev/vmclock0` | | 1,745 | | 178 | 175 | 171 |
| `ioctl(RTC_RD_TIME)` | | 5,471 | | 98,486 | 98,872 | no RTC |

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

1. **A restore or a migration is visible before the guest runs again, on real hypervisors too.** QEMU 10.0 and later with `-device vmclock`, and Firecracker 1.15 and later, change the disruption marker before the vCPUs start (S1, S2). Every restore and migration measured changed it, in TCG, under KVM on either clocksource and in Firecracker, a 1 ms-downtime migration included (T3, T3b, T4, T4b, F3). A holder that compares the marker with the value it last saw, each pass, and ends its leases on a change has no stale exposure in these cases (T6d). Firecracker's interrupt also woke a `poll()` on `/dev/vmclock0` as soon as the restored guest ran (F3), but the mapped page already says the same at no cost.
2. **Checking costs nothing a loop would notice:** 0.8 ns in a KVM or Firecracker guest, the same as natively, and 2.8 ns from Go natively, against 130 to 140 ns for each raw clock syscall in those guests (T5). It must be the mapped page: `read()` costs 170 to 180 ns, and `poll()` cannot wake a loop on QEMU (S3, T0). The device is opt-in on QEMU, needs a guest kernel with the driver (H1) and root or a udev rule for `/dev/vmclock0`.
3. **Whether a stop in place shows depends on the hypervisor and the guest clocksource.**
   - QEMU, guest on `kvm-clock` (QEMU's CPU models without `invtsc`): a `stop`, an internal snapshot and a migration cancelled at switchover freeze every guest clock and leave the marker alone (T1, T2c, T4c), while the TSC runs on underneath. **Only this combination hides a stop.**
   - QEMU with `+invtsc` and `tsc-frequency`, guest on `tsc`: every stop shows in every guest clock (T1, T4c), so the holder's own lease clocks run out across it; restores and migrations freeze the clocks but change the marker (T3, T4, T4b).
   - Firecracker on either clocksource: a pause, and a snapshot taken while paused, show in every clock (F1, F2); a restore freezes them but changes the marker and the generation (F3). `clock_realtime` moves kvmclock on by the time since the snapshot but leaves the TSC, so it does nothing for Firecracker's default `tsc` guest (F3c).
   - A QEMU migration counts its own downtime in kvmclock, including a hold at switchover, but not in the TSC (T4b, S4's `pre_save`); the marker changes either way.
4. **The design's rate bound neither catches a pause in time nor survives one.** Stale reads need a pause longer than `primary_lease` minus a heartbeat, 3 s; after one the holder serves on its old lease until its next grant round trip, up to 0.99 s, and then the bound shortens every lease for up to 4 s (T6a).
5. **The grantors' clocks catch a stop, if their messages are read first.** Stamping every message a grantor sends with its `CLOCK_MONOTONIC_RAW` and applying the step check to each, before the reads of the same pass, ends the old leases at resume whenever messages sent during the stop were queued, as QEMU's virtio-net leaves them (S7, T6c). Restarting the rate window on a detection keeps the pause from also counting as a fast grantor, so the cost is one round trip (T6b, T6e).
6. **Marker and step check together leave no exposure in the model** (T6e). The marker covers lost messages, the step check covers stops.
7. **Compare markers only for change:** two restores of one snapshot show the same value (T3c, F3b), and the starting value differs by hypervisor (1 on QEMU, 0 on Firecracker).
8. **`ptp_kvm` sees every pause, at 1.6 to 1.8 µs a read.** It registered in every KVM guest, on `kvm-clock` and on `tsc`, under QEMU and Firecracker, and kept host time through every stop, restore and migration (T1 to T4c, F1 to F3c), but each read is a syscall and a hypercall, 12 to 14 times a raw clock syscall (T5). It is the host's `CLOCK_REALTIME`, so it also moves with the host's time daemon.
9. **What remains open:** on QEMU with a guest on `kvm-clock`, a stop whose queued messages the backend dropped, or during which the network was also cut, followed by a client request that reaches the holder before any grantor message: up to one heartbeat of stale reads. Running storage VMs with `+invtsc` (S4, T1, T4c) or on Firecracker (F1) closes it with no per-pass cost; reading `ptp_kvm` on each pass that serves lease reads closes it at about 1.7 µs a pass (conclusion 8); so would a read rule that confirms with each grantor per batch of reads instead of a lease.

## What is unverified

- Networking. No guest had a NIC and neither host had a tap device, so S7's queueing, the order in which a stopped VM's backlog reaches its sockets, and how much a tap queue holds before it drops were not measured; T6c rests on them.
- Several event loops. The model has one holder that reads a grantor's messages before its reads; with one loop per core, a step seen on one loop's connection must reach the loops serving reads first.
- Migration between hosts. Every migration ran on one host, so whether `tsc-frequency` and TSC scaling keep an `invtsc` guest's clocks right across hosts with different TSC rates was not tested.
- Firecracker's diff snapshots, its userfaultfd memory backend and the jailer, and the marker read from Go inside a guest (the C read costs the same in a guest as natively, so Go's should too).
- Other hypervisors and clouds: whether Xen, Hyper-V, VMware or public clouds expose VMClock, and whether arm64 guests behave the same, was not checked.
