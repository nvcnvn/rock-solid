"""Splits a `perf record -g -e cpu-clock` profile into kernel and user buckets.

Kernel samples go to the syscall named by their `__x64_sys_*` frame, to syscall entry and exit
when they are in the kernel's entry code without one (the wrapper does not name the syscall in Go),
to page faults, or to interrupts. User samples go by their leaf symbol.
"""

import re
import subprocess
import sys
from collections import defaultdict

SYSCALL_GROUPS = {
    "read": "read", "recvfrom": "read", "write": "write", "sendto": "write",
    "epoll_wait": "epoll_wait", "epoll_pwait": "epoll_wait", "epoll_ctl": "epoll_ctl",
    "futex": "futex", "nanosleep": "nanosleep", "clock_nanosleep": "nanosleep",
}

USER_RULES = [
    (r"memmove|memcpy|memset|memclr", "memmove/memset"),
    (r"crc32|castagnoli", "crc32c"),
    (r"entersyscall|exitsyscall|reentersyscall|internal/runtime/syscall|^syscall\.|casgstatus|"
     r"^runtime\.(save|getcallerfp|getfp|wirep|acquirep|exitsyscallTryGetP)$|"
     r"__libc_recv|__send|__libc_read|__libc_write|asynccancel|^epoll_wait$|^read$|^write$|"
     r"__errno_location|^getppid$", "syscall wrappers"),
    (r"maps\.|mapaccess|mapassign|mapdelete|hashbrown|RawTable|sip|Hasher|hash::", "hash map"),
    (r"mallocgc|nextFree|mcache|mcentral|mheap|gcBgMark|gcDrain|scanobject|markroot|sweep|"
     r"greyobject|findObject|wbBuf|bulkBarrier|gcWork|^runtime\.gc|^malloc|^free|_int_", "alloc/GC"),
    (r"schedule|findRunnable|stealWork|runqgrab|runqsteal|park_m|mcall|gopark|goready|wakep|"
     r"startm|stopm|handoffp|notesleep|notewakeup|futexsleep|futexwakeup|^runtime\.futex|"
     r"sysmon|usleep|retake|checkTimers|netpoll|resetspinning|pidle|lock2|unlock2|runqput|"
     r"execute|gogo|goexit|mPark|osyield|procyield|nanotime|setBlockOnExitSyscall|Gscanstatus|"
     r"releaseP|incidlelocked|takeP|timers", "scheduler/sysmon"),
    (r"vdso|clock_gettime|time\.Now|walltime|Instant", "clock"),
    (r"^main\.|replicate_rust|<replicate|sort|^slices\.|ifaceeq|memequal|efaceeq", "program code"),
]


def bucket_user(sym):
    for pat, name in USER_RULES:
        if re.search(pat, sym):
            return name
    return "other user (" + ("runtime" if sym.startswith("runtime.") else "lib") + ")"


def classify_sample(frames):
    """frames: list of (sym, dso) from leaf outward."""
    leaf_sym, leaf_dso = frames[0]
    if leaf_dso == "[vdso]":
        return "user: clock"
    if "kernel.kallsyms" not in leaf_dso:
        return "user: " + bucket_user(leaf_sym)
    syms = [s for s, _ in frames]
    if any(s in ("asm_exc_page_fault", "exc_page_fault") for s in syms):
        return "kernel: page faults"
    for s in syms:
        m = re.match(r"__(?:x64|do)_sys_(\w+)", s)
        if m:
            return "kernel: " + SYSCALL_GROUPS.get(m.group(1), "other syscalls")
    if "entry_SYSCALL_64_after_hwframe" in syms:
        return "kernel: syscall entry/exit"
    return "kernel: interrupts and other"


def parse(path, env=None):
    out = subprocess.run(["perf", "script", "-i", path, "-F", "period,ip,sym,dso"],
                         capture_output=True, text=True, check=True, env=env).stdout
    totals = defaultdict(int)
    period, frames = 0, []
    for line in out.splitlines() + [""]:
        if not line.strip():
            if frames:
                totals[classify_sample(frames)] += period
            period, frames = 0, []
            continue
        if not line.startswith("\t"):
            period = int(line.split()[-1])
            continue
        m = re.match(r"\s*[0-9a-f]+ (.*) \((.*)\)$", line)
        if m:
            sym = re.sub(r"\+0x[0-9a-f]+$", "", m.group(1))
            frames.append((sym, m.group(2)))
    return totals


def main():
    path, ops = sys.argv[1], float(sys.argv[2])
    totals = parse(path)
    total = sum(totals.values())
    for k in sorted(totals, key=lambda k: (not k.startswith("kernel"), -totals[k])):
        print(f"PROFILE {k:34s} us_per_op={totals[k] / 1000 / ops:6.2f} share={100 * totals[k] / total:5.1f}%")
    print(f"PROFILE {'total':34s} us_per_op={total / 1000 / ops:6.2f}")


if __name__ == "__main__":
    main()
