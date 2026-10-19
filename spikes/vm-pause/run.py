#!/usr/bin/env python3
"""Drives QEMU guests running vmpause-guest through pauses, snapshots and
migrations over QMP, and checks each guest clock and VMClock field against the
host's clock.

    ./run.py vm     T0-T4: one guest carried through every event in order
    ./run.py bench  T5: guest cost of the marker and the clocks, then the host's

Each guest prints a sample every 100 ms of guest time; the host stamps every
line with its own CLOCK_MONOTONIC on arrival. For an event, a clock's gain is
its change from the last sample before the event to the first one after, and
the host's gap is the change in arrival stamps over the same two lines. A
frozen clock gains one sample interval (0.1 s) whatever the host's gap.
"""
import json
import os
import platform
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(HERE, "work")
SAMPLE = 0.1
PAUSE = 10.0


def env():
    out = subprocess.run(["bash", "-c", ". ./env.sh; echo \"$QEMU\"; echo \"$QEMU_IMG\""],
                         cwd=HERE, capture_output=True, text=True, check=True).stdout
    qemu, qemu_img = out.strip().split("\n")
    return shlex.split(qemu), shlex.split(qemu_img)


QEMU, QEMU_IMG = env()


def check(ok, test, what, got, want):
    print(f"{'[OK ]' if ok else '[FAIL]'} {test} {what} got={got} want={want}", flush=True)
    return ok


class VM:
    """One QEMU process: serial lines stamped on arrival, and a QMP socket."""

    def __init__(self, name, mode, incoming=None):
        self.name = name
        self.lines = []
        self.samples = []
        self.cond = threading.Condition()
        qmp_path = os.path.join(WORK, f"qmp-{name}.sock")
        if os.path.exists(qmp_path):
            os.unlink(qmp_path)
        args = QEMU + [
            "-machine", "q35", "-accel", "tcg", "-cpu", "max", "-m", "256", "-smp", "1",
            "-nodefaults", "-no-user-config", "-display", "none", "-serial", "stdio",
            "-device", "vmclock",
            "-drive", f"if=none,id=snap0,format=qcow2,file.filename={WORK}/snap.qcow2,"
                      "file.driver=file,file.locking=off",
            "-kernel", os.path.join(HERE, "bin/vmlinuz"),
            "-initrd", os.path.join(HERE, "bin/initrd.cpio"),
            "-append", f"console=ttyS0 quiet panic=-1 vp.mode={mode}",
            "-qmp", f"unix:{qmp_path},server=on,wait=off", "-no-reboot",
        ]
        if incoming:
            args += ["-incoming", incoming]
        self.proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1)
        threading.Thread(target=self._read, daemon=True).start()
        for _ in range(100):
            if os.path.exists(qmp_path):
                break
            time.sleep(0.05)
        self.qmp = socket.socket(socket.AF_UNIX)
        self.qmp.connect(qmp_path)
        self.qf = self.qmp.makefile("rw")
        self.qf.readline()
        self.cmd("qmp_capabilities")

    def _read(self):
        for line in self.proc.stdout:
            t = time.monotonic()
            line = line.rstrip("\r\n")
            with self.cond:
                self.lines.append((t, line))
                if line.startswith("S "):
                    s = {k: int(v, 0) for k, v in re.findall(r"(\w+)=(\S+)", line)}
                    s["host"] = t
                    s["vm"] = self.name
                    s["line"] = line
                    self.samples.append(s)
                self.cond.notify_all()
            if not line.startswith("S "):
                print(f"  {self.name}| {line}", flush=True)

    def cmd(self, name, **arguments):
        msg = {"execute": name}
        if arguments:
            msg["arguments"] = arguments
        self.qf.write(json.dumps(msg) + "\n")
        self.qf.flush()
        while True:
            r = json.loads(self.qf.readline())
            if "return" in r:
                return r["return"]
            if "error" in r:
                raise RuntimeError(f"{self.name} {name}: {r['error']}")

    def hmp(self, line):
        return self.cmd("human-monitor-command", **{"command-line": line})

    def wait_line(self, prefix, timeout=240):
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                for _, l in self.lines:
                    if l.startswith(prefix):
                        return l
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"{self.name}: no '{prefix}'")
                self.cond.wait(left)

    def wait_samples(self, n, timeout=60):
        end = time.monotonic() + timeout
        with self.cond:
            start = len(self.samples)
            while len(self.samples) < start + n:
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"{self.name}: samples stopped")
                self.cond.wait(left)

    def last(self):
        with self.cond:
            return self.samples[-1]

    def first_after(self, t, timeout=60):
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                for s in self.samples:
                    if s["host"] > t:
                        return s
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"{self.name}: no sample after event")
                self.cond.wait(left)

    def status(self):
        return self.cmd("query-status")["status"]

    def wait_migration(self, states, timeout=300):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            r = self.cmd("query-migrate")
            if r.get("status") in states:
                return r
            if r.get("status") == "failed":
                raise RuntimeError(f"{self.name}: migration failed: {r}")
            time.sleep(0.05)
        raise TimeoutError(f"{self.name}: migration stuck in {r.get('status')}")

    def quit(self):
        with open(os.path.join(WORK, f"serial-{self.name}.txt"), "w") as f:
            for t, l in self.lines:
                f.write(f"{t:.3f} {l}\n")
        try:
            self.cmd("quit")
        except Exception:
            pass
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def gains(before, after):
    host = after["host"] - before["host"]
    g = {"host": host}
    for k in ("raw", "boot", "mono", "real"):
        g[k] = (after[k] - before[k]) / 1e9
    g["rtc"] = after["rtc"] - before["rtc"]
    return g


def report(test, event, before, after, frozen, marker_changes, gen_changes=False):
    """Prints the gains across an event and checks them against expectations."""
    g = gains(before, after)
    print(f"G {test} {event}: host {g['host']:.3f} s | raw {g['raw']:.3f} boot {g['boot']:.3f} "
          f"mono {g['mono']:.3f} real {g['real']:.3f} s | rtc {g['rtc']} s | "
          f"marker {before['marker']}->{after['marker']} seq {before['seq']}->{after['seq']} "
          f"gen {before['gen']}->{after['gen']} flags {before['flags']:#x}->{after['flags']:#x} "
          f"| n {before['n']}->{after['n']} on {before['vm']}->{after['vm']}", flush=True)
    print(f"  {before['vm']} {before['host']:.3f} {before['line']}")
    print(f"  {after['vm']} {after['host']:.3f} {after['line']}", flush=True)
    for k in ("raw", "boot", "mono", "real"):
        if frozen:
            check(g[k] < 1.0, test, f"{k} stood still", f"{g[k]:.3f}s/host {g['host']:.3f}s",
                  "<1s")
        else:
            check(abs(g[k] - g["host"]) < 1.0, test, f"{k} kept host time",
                  f"{g[k]:.3f}s/host {g['host']:.3f}s", "host±1s")
    check(abs(g["rtc"] - g["host"]) <= 2, test, "rtc kept host time",
          f"{g['rtc']}s/host {g['host']:.3f}s", "host±2s")
    changed = after["marker"] != before["marker"]
    check(changed == marker_changes, test, "marker changed", changed, marker_changes)
    gchanged = after["gen"] != before["gen"]
    check(gchanged == gen_changes, test, "vm_generation_counter changed", gchanged, gen_changes)
    return g


def stop_cont(vm, test, seconds, how):
    vm.wait_samples(10)
    before = vm.last()
    if how == "qmp":
        vm.cmd("stop")
        check(vm.status() == "paused", test, "status after stop", vm.status(), "paused")
    else:
        os.kill(vm.proc.pid, signal.SIGSTOP)
    time.sleep(seconds)
    t = time.monotonic()
    if how == "qmp":
        vm.cmd("cont")
    else:
        os.kill(vm.proc.pid, signal.SIGCONT)
    after = vm.first_after(t)
    return before, after


def run_vm():
    subprocess.run(QEMU_IMG + ["create", "-q", "-f", "qcow2", f"{WORK}/snap.qcow2", "64M"],
                   check=True)
    port = 47000 + os.getpid() % 1000
    a = VM("A", "monitor")
    a.wait_line("READY", 300)
    vms = [a]
    try:
        # T1, T2: QMP stop and cont.
        before, after = stop_cont(a, "T1/T2", PAUSE, "qmp")
        report("T1/T2", f"QMP stop {PAUSE:.0f} s, cont", before, after, frozen=True,
               marker_changes=False)

        # T2b: a host stall that is not a QMP stop: SIGSTOP the whole QEMU process.
        before, after = stop_cont(a, "T2b", PAUSE, "signal")
        report("T2b", f"SIGSTOP {PAUSE:.0f} s, SIGCONT", before, after, frozen=False,
               marker_changes=False)

        # T2c: an internal snapshot of the running guest (savevm stops it while saving).
        a.wait_samples(10)
        before = a.last()
        t0 = time.monotonic()
        out = a.hmp("savevm s1")
        t1 = time.monotonic()
        print(f"  savevm s1 took {t1 - t0:.3f} s {out.strip()!r}", flush=True)
        after = a.first_after(t1)
        report("T2c", f"savevm on the running guest ({t1 - t0:.2f} s)", before, after,
               frozen=True, marker_changes=False)
        snap_sample = before

        # T3b: loadvm of that snapshot into the same process, PAUSE s later.
        a.wait_samples(10)
        time.sleep(PAUSE)
        t = time.monotonic()
        out = a.hmp("loadvm s1")
        print(f"  loadvm s1 {out.strip()!r}", flush=True)
        after = a.first_after(t)
        with a.cond:
            prior = [s for s in a.samples if s["n"] == after["n"] - 1 and s["host"] <= t1]
        base = prior[-1] if prior else snap_sample
        report("T3b", "loadvm into the same process", base, after, frozen=True,
               marker_changes=True)
        check(after["host"] - base["host"] > PAUSE, "T3b", "host gap covers the wait",
              f"{after['host'] - base['host']:.3f}s", f">{PAUSE:.0f}s")

        # T3: migrate to a file, quit, restore in a new process PAUSE s later.
        a.wait_samples(10)
        snapfile = f"{WORK}/state.mig"
        if os.path.exists(snapfile):
            os.unlink(snapfile)
        before = a.last()
        a.cmd("migrate", uri=f"file:{snapfile}")
        a.wait_migration({"completed"})
        time.sleep(0.3)
        before = a.last()
        check(a.status() == "postmigrate", "T3", "source status", a.status(), "postmigrate")
        a.quit()
        time.sleep(PAUSE)
        b = VM("B", "monitor", incoming=f"file:{snapfile}")
        vms.append(b)
        after = b.first_after(0, 120)
        report("T3", f"migrate to file, restore in a new process {PAUSE:.0f} s later",
               before, after, frozen=True, marker_changes=True)

        # T3c: restore the same file again: does the restored guest see the same marker?
        b.wait_samples(10)
        first_restore = after
        c = VM("C", "monitor", incoming=f"file:{snapfile}")
        vms.append(c)
        after_c = c.first_after(0, 120)
        check(after_c["marker"] == first_restore["marker"], "T3c",
              "second restore of one file shows the first restore's marker",
              after_c["marker"], first_restore["marker"])
        check(after_c["marker"] != before["marker"], "T3c",
              "and differs from the saved guest's", after_c["marker"], f"!={before['marker']}")
        b.quit()

        # T4: live migration C -> D over TCP on localhost.
        c.wait_samples(10)
        d = VM("D", "monitor", incoming=f"tcp:127.0.0.1:{port}")
        vms.append(d)
        before = c.last()
        t0 = time.monotonic()
        c.cmd("migrate", uri=f"tcp:127.0.0.1:{port}")
        r = c.wait_migration({"completed"})
        time.sleep(0.3)
        before = c.last()
        print(f"  migration: total {r.get('total-time')} ms downtime {r.get('downtime')} ms "
              f"setup {r.get('setup-time')} ms", flush=True)
        after = d.first_after(t0)
        report("T4", f"live migration (downtime {r.get('downtime')} ms)", before, after,
               frozen=True, marker_changes=True)
        c.quit()

        # T4b: live migration D -> E held PAUSE s at switchover (source stopped).
        d.wait_samples(10)
        port += 1
        e = VM("E", "monitor", incoming=f"tcp:127.0.0.1:{port}")
        vms.append(e)
        d.cmd("migrate-set-capabilities",
              capabilities=[{"capability": "pause-before-switchover", "state": True}])
        before = d.last()
        d.cmd("migrate", uri=f"tcp:127.0.0.1:{port}")
        d.wait_migration({"pre-switchover"})
        time.sleep(0.3)
        before = d.last()
        time.sleep(PAUSE)
        t = time.monotonic()
        d.cmd("migrate-continue", state="pre-switchover")
        r = d.wait_migration({"completed"})
        print(f"  migration: total {r.get('total-time')} ms downtime {r.get('downtime')} ms",
              flush=True)
        after = e.first_after(t)
        report("T4b", f"live migration held {PAUSE:.0f} s at switchover", before, after,
               frozen=True, marker_changes=True)
        d.quit()

        # T4c: a migration cancelled at switchover: the source stopped, then resumes.
        e.wait_samples(10)
        port += 1
        f = VM("F", "monitor", incoming=f"tcp:127.0.0.1:{port}")
        vms.append(f)
        e.cmd("migrate-set-capabilities",
              capabilities=[{"capability": "pause-before-switchover", "state": True}])
        before = e.last()
        e.cmd("migrate", uri=f"tcp:127.0.0.1:{port}")
        e.wait_migration({"pre-switchover"})
        time.sleep(0.3)
        before = e.last()
        time.sleep(PAUSE)
        t = time.monotonic()
        e.cmd("migrate_cancel")
        r = e.wait_migration({"cancelled"})
        if e.status() != "running":
            e.cmd("cont")
        after = e.first_after(t)
        report("T4c", f"migration cancelled after {PAUSE:.0f} s at switchover; source resumes",
               before, after, frozen=True, marker_changes=False)
        f.quit()
        e.wait_samples(5)
    finally:
        for vm in vms:
            if vm.proc.poll() is None:
                vm.quit()


def p50s(lines):
    return {m.group(1).strip(): float(m.group(2))
            for m in (re.match(r"C (.+?)\s+p50=\s*([\d.]+) ns", l) for l in lines) if m}


def run_bench():
    g = VM("G", "bench")
    g.wait_line("DONE", 300)
    g.proc.wait(30)
    guest = p50s(l for _, l in g.lines)
    print("# host (native, anonymous page standing in for the VMClock page):", flush=True)
    out = subprocess.run([os.path.join(HERE, "bin/vmpause-guest"), "bench"],
                         capture_output=True, text=True, check=True).stdout
    print(out, end="", flush=True)
    host = p50s(out.split("\n"))
    m, c = "marker (mmap seqlock)", "vdso CLOCK_MONOTONIC_RAW"
    check(host[m] < 50, "T5", "host marker read p50", f"{host[m]}ns", "<50ns")
    check(host[m] < host[c], "T5", "host marker read below one clock read",
          f"{host[m]}ns vs {host[c]}ns", "marker<clock")
    check(guest[m] < guest[c], "T5", "guest (TCG) marker read below one clock read",
          f"{guest[m]}ns vs {guest[c]}ns", "marker<clock")
    r = "pread /dev/vmclock0 (24 B)"
    check(guest[r] > guest[m], "T5", "guest read() of the device costs more than the mmap",
          f"{guest[r]}ns vs {guest[m]}ns", "read>mmap")


def main():
    os.makedirs(WORK, exist_ok=True)
    qv = subprocess.run(QEMU + ["--version"], capture_output=True, text=True).stdout.split("\n")[0]
    print(f"# host {platform.system()} {platform.release()} | {qv} | guest Linux 7.0.0-14-generic "
          f"(Ubuntu 26.04) | TCG, 1 vCPU, 256 MiB | "
          f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
    {"vm": run_vm, "bench": run_bench}[sys.argv[1]]()


if __name__ == "__main__":
    main()
