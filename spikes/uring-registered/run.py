#!/usr/bin/env python3
"""Drive the io_uring registered buffers and files spike. Needs root for the zram device.

R*  registration limits and cost
N*  IORING_OP_NOP with a plain or fixed file and a fixed buffer: submission cost alone
P*  buffered 4 KiB reads served from the page cache
Z*  O_DIRECT 4 KiB reads and writes on a zram device holding zero pages: a near-null device
V*  O_DIRECT 4 KiB reads and writes on a file in work/, on whatever disk holds it
Every I/O row runs the four variants (plain, fixed file, fixed buffer, both) back to back, in an order
rotated each rep, REPS times. P reads its own file, so the O_DIRECT writes of V never evict it.
"""
import os
import platform
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "bin", "uring")
WORK = os.path.join(HERE, "work")
DATA = os.path.join(WORK, "data")
CACHED = os.path.join(WORK, "cached")
SECS = os.environ.get("SECS", "2")
REPS = int(os.environ.get("REPS", "6"))
VARIANTS = [(0, 0), (0, 1), (1, 0), (1, 1)]  # (fbuf, ffile)


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def read(path, default="?"):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return default


def host():
    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "?")
    virt = sh("systemd-detect-virt").stdout.strip() or ("vm" if " hypervisor" in read("/proc/cpuinfo") else "none")
    src, fstype = (sh(f"findmnt -no SOURCE,FSTYPE --target {WORK}").stdout.split() + ["?", "?"])[:2]
    disk = sh(f"lsblk -no PKNAME {src}").stdout.strip() or os.path.basename(src)
    model = read(f"/sys/block/{disk}/device/model", "")
    return (f"cpu='{cpu}' virt={virt} load1={os.getloadavg()[0]:.1f} disk='{disk} {model} {fstype}' "
            f"io_poll={read(f'/sys/block/{disk}/queue/io_poll')} nvme_poll_queues={read('/sys/module/nvme/parameters/poll_queues', '-')}")


def run(args, env=None):
    r = subprocess.run([BIN] + args, capture_output=True, text=True, env=env)
    line = (r.stdout.strip() or r.stderr.strip()).replace("\n", " | ")
    return line


def main():
    os.makedirs(WORK, exist_ok=True)
    zid = open("/sys/class/zram-control/hot_add").read().strip()
    zdev = f"/dev/zram{zid}"
    try:
        with open(f"/sys/block/zram{zid}/comp_algorithm", "w") as f:
            f.write("lz4")
        with open(f"/sys/block/zram{zid}/disksize", "w") as f:
            f.write("2G")
        if not os.path.exists(DATA) or os.path.getsize(DATA) != 1 << 30:
            sh(f"dd if=/dev/zero of={DATA} bs=1M count=1024 oflag=direct status=none")
        ver = sh("cc --version | head -1").stdout.strip()
        print(f"# kernel={platform.release()} cc='{ver}' nproc={os.cpu_count()} date={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
              f"secs={SECS} reps={REPS} ring=SINGLE_ISSUER|DEFER_TASKRUN slab=64MiB zram=lz4 {host()}")
        print("## R registration (as root CAP_IPC_LOCK skips RLIMIT_MEMLOCK accounting)")
        pop = dict(os.environ, POPULATE="1")
        for env, args in [(None, ["64"]), (None, ["1024"]), (pop, ["64"]), (pop, ["1024"]), (pop, ["2048"]),
                          (pop, ["1025", "0", "single"]), (pop, ["64", "16384"]), (pop, ["64", "16385"])]:
            print("R", "populated" if env else "untouched", run(["reg"] + args, env))
        for mb in ["4", "64"]:
            r = sh(f"ulimit -l 8192 && setpriv --reuid=65534 --regid=65534 --clear-groups {BIN} reg {mb}")
            print("R nonroot", (r.stdout + r.stderr).strip())
        sys.stdout.flush()

        if not os.path.exists(CACHED):
            sh(f"dd if=/dev/zero of={CACHED} bs=1M count=256 status=none")
        rows = []
        for qd in ["1", "32"]:
            rows.append(("N", "/dev/null", "nop", qd, []))
        for qd in ["1", "32"]:
            rows.append(("P", CACHED, "read", qd, ["direct=0", "size_mb=256"]))
        for op in ["read", "write"]:
            for qd in ["1", "32"]:
                rows.append(("Z", zdev, op, qd, ["direct=1", "size_mb=2048"]))
        for op in ["read", "write"]:
            for qd in ["1", "32"]:
                rows.append(("V", DATA, op, qd, ["direct=1", "size_mb=1024"]))
        for rep in range(REPS):
            for tag, path, op, qd, extra in rows:
                if tag == "P":
                    sh(f"cat {CACHED} > /dev/null")
                for i in range(len(VARIANTS)):
                    fbuf, ffile = VARIANTS[(i + rep) % len(VARIANTS)]
                    out = run(["io", path, op, qd, SECS, f"fbuf={fbuf}", f"ffile={ffile}"] + extra)
                    print(f"{tag} rep={rep} {out}")
                    sys.stdout.flush()
    finally:
        with open("/sys/class/zram-control/hot_remove", "w") as f:
            f.write(zid)


if __name__ == "__main__":
    main()
