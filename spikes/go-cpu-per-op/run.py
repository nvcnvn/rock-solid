#!/usr/bin/env python3
"""Run the Go and Rust CRC32C, cleaner harnesses and replicated-write paths side by side; print raw
lines, tables and checks to results.txt.

python3 run.py [crc] [cleaner] [replicate]
"""
import datetime
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUST_CLEANER = os.path.join(HERE, "..", "parallel-cleaner", "target", "release", "parallel-cleaner")
GO_CLEANER = os.path.join(HERE, "bin", "cleaner-go")
RUST_REPL = os.path.join(HERE, "replicate-rust", "target", "release", "replicate-rust")
GO_REPL = os.path.join(HERE, "bin", "replicate-go")
CRC_RUST = os.path.join(HERE, "crc-rust", "target", "release", "crc-rust")
CRC_RUST_V2 = os.path.join(HERE, "crc-rust", "target-v2", "release", "crc-rust")
CRC_GO = os.path.join(HERE, "bin", "crc-go")
GO = {"GOTOOLCHAIN": os.environ.get("GOTOOLCHAIN", "go1.27.0")}
FAILS = []


def run(binary, **kw):
    line = subprocess.run([binary] + [f"{k}={v}" for k, v in kw.items()], check=True, capture_output=True,
                          text=True).stdout.strip().splitlines()[-1]
    print(line, flush=True)
    return {k: float(v) for k, v in (p.split("=") for p in line.split("|")[1].split())}


def check(tid, what, ok, got, want):
    print(f"[{'OK ' if ok else 'FAIL'}] {tid} {what} got={got} want={want}", flush=True)
    if not ok:
        FAILS.append(tid)


def table(title, cols, rows):
    print(f"\n| {title} | " + " | ".join(cols) + " |")
    print("|---" * (len(cols) + 1) + "|")
    for name, vals in rows:
        print(f"| {name} | " + " | ".join(vals) + " |")
    print(flush=True)


def crc():
    print("=== C0: CRC32C of one 4 KiB block", flush=True)
    rows = []
    for label, binary in (("Rust, default target", CRC_RUST), ("Rust, x86-64-v2", CRC_RUST_V2), ("Go", CRC_GO)):
        out = subprocess.run([binary], check=True, capture_output=True, text=True).stdout.strip().splitlines()
        for line in out:
            print(line, flush=True)
            impl = line.split("impl=")[1].split()[0]
            rows.append((f"{label}: {impl}", [line.split("ns_per_4k=")[1]]))
    table("implementation", ["ns per 4 KiB"], rows)


def cleaner():
    print("=== G1: cleaner harness without CRC32C (crc=0), Rust vs Go; inline unthrottled, then paced at 50k "
          "writes/s", flush=True)
    cases = [("inline u=0.90", dict(mode="inline", u=0.9)), ("inline u=0.95", dict(mode="inline", u=0.95)),
             ("paced inline u=0.95", dict(mode="inline", u=0.95, rate=50000)),
             ("paced owner/1 u=0.95", dict(mode="owner", workers=1, u=0.95, rate=50000)),
             ("paced shared/1 u=0.95", dict(mode="shared", workers=1, u=0.95, rate=50000))]
    rows = []
    for name, kw in cases:
        kw = dict(kw, crc=0)
        r = run(RUST_CLEANER, tag=f"G1/rust/{name.replace(' ', '_')}", **kw)
        g = run(GO_CLEANER, tag=f"G1/go/{name.replace(' ', '_')}", **kw)
        rows.append((name, [f"{r['owner_work_ns']:.0f}", f"{g['owner_work_ns']:.0f}",
                            f"{g['owner_work_ns'] / r['owner_work_ns']:.2f}",
                            f"{r['worker_cpu_ns']:.0f}", f"{g['worker_cpu_ns']:.0f}",
                            f"{r['lat_p99_us']:.1f}", f"{g['lat_p99_us']:.1f}", f"{r['wa']:.2f}", f"{g['wa']:.2f}"]))
        if "inline" in name and "paced" not in name:
            ratio = g["owner_work_ns"] / r["owner_work_ns"]
            check("G1", f"Go / Rust owner work per write, {name}", 1.2 <= ratio <= 1.5, f"{ratio:.2f}x",
                  "1.2-1.5x (estimate before the run)")
    table("case", ["Rust owner ns/write", "Go owner ns/write", "Go/Rust", "Rust cleaner cpu ns/write",
                   "Go cleaner cpu ns/write", "Rust p99 us", "Go p99 us", "Rust WA", "Go WA"], rows)


def replicate():
    print("=== G2: replicated 4K write over loopback TCP, client -> primary -> 2 replicas, no disk; each "
          "language writing every frame at once (coalesce=0) and batching frames that are ready together "
          "(coalesce=1)", flush=True)
    configs = [("rust", RUST_REPL, 1), ("rust", RUST_REPL, 0), ("go", GO_REPL, 0), ("go", GO_REPL, 1)]
    res = {}
    for conns in (1, 16):
        for _ in range(2):
            for lang, binary, co in configs:
                res.setdefault((lang, co, conns), []).append(run(binary, conns=conns, secs=10, coalesce=co))
    rows = []
    for conns in (1, 16):
        for lang, _, co in configs:
            rs = res[(lang, co, conns)]
            avg = {k: sum(r[k] for r in rs) / len(rs) for k in rs[0]}
            res[(lang, co, conns)] = avg
            rows.append((f"{lang}, coalesce={co}, {conns} conn",
                         [f"{avg['ops_per_s']:.0f}", f"{avg['cpu_us_per_op']:.1f}", f"{avg['cores']:.2f}",
                          f"{avg['reads_per_op']:.2f}", f"{avg['writes_per_op']:.2f}", f"{avg['lat_p50_us']:.1f}",
                          f"{avg['lat_p99_us']:.1f}", f"{avg['lat_p999_us']:.1f}"]))
    table("language, mode, connections", ["ops/s", "CPU us/op (all 4 nodes)", "cores", "socket reads/op",
                                          "socket writes/op", "p50 us", "p99 us", "p99.9 us"], rows)
    for conns in (1, 16):
        for co in (1, 0):
            ratio = res[("go", co, conns)]["cpu_us_per_op"] / res[("rust", co, conns)]["cpu_us_per_op"]
            check("G2", f"Go / Rust CPU per replicated write, coalesce={co}, {conns} conn", 1.2 <= ratio <= 1.5,
                  f"{ratio:.2f}x", "1.2-1.5x (estimate before the run)")
    d = res[("go", 1, 1)]["lat_p50_us"] - res[("rust", 1, 1)]["lat_p50_us"]
    check("G2", "Go - Rust p50 latency at queue depth 1", 5 <= d <= 20, f"{d:+.1f}us", "+5 to +20us (estimate)")


def main():
    want = set(sys.argv[1:]) or {"crc", "cleaner", "replicate"}
    subprocess.run(["go", "build", "-o", "bin/crc-go", "./crc-go"], cwd=HERE, check=True, env={**os.environ, **GO})
    crc_dir = os.path.join(HERE, "crc-rust")
    subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=crc_dir, check=True)
    subprocess.run(["cargo", "build", "--release", "--quiet", "--target-dir", "target-v2"], cwd=crc_dir, check=True,
                   env={**os.environ, "RUSTFLAGS": "-C target-cpu=x86-64-v2"})
    subprocess.run(["go", "build", "-o", "bin/cleaner-go", "./cleaner-go"], cwd=HERE, check=True,
                   env={**os.environ, **GO})
    subprocess.run(["go", "build", "-o", "bin/replicate-go", "./replicate-go"], cwd=HERE, check=True,
                   env={**os.environ, **GO})
    subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=os.path.join(HERE, "replicate-rust"), check=True)
    subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=os.path.join(HERE, "..", "parallel-cleaner"),
                   check=True)
    sys.stdout = Tee(os.path.join(HERE, "results.txt"))
    gov = subprocess.run(["go", "version"], capture_output=True, text=True, env={**os.environ, **GO}).stdout.split()[2]
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    cpu = next((ln.split(":", 1)[1].strip() for ln in open("/proc/cpuinfo") if ln.startswith("model name")), "?")
    print(f"# {platform.release()}, {cpu}, {os.cpu_count()} CPUs, {gov}, {rustc}, "
          f"{datetime.datetime.now().isoformat(timespec='seconds')}")
    if "crc" in want:
        crc()
    if "cleaner" in want:
        cleaner()
    if "replicate" in want:
        replicate()
    print(f"\n# checks failed: {', '.join(FAILS) or 'none'}")


class Tee:
    def __init__(self, path):
        self.f = open(path, "w")

    def write(self, s):
        self.f.write(s)
        sys.__stdout__.write(s)

    def flush(self):
        self.f.flush()
        sys.__stdout__.flush()


if __name__ == "__main__":
    main()
