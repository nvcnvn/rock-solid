#!/usr/bin/env python3
"""Drive the parallel-cleaner harness through tests P0-P4 and P0b; print raw RUN lines, tables and checks.

python3 run.py [GROUP ...] [--out results.txt]
"""
import datetime
import math
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "target", "release", "parallel-cleaner")
U = [0.80, 0.90, 0.95]
RATE = 50000


def greedy_uniform_wa(u):
    """Mean-field greedy cleaning under uniform random overwrites: x = exp(-(1 - x) / u), WA = 1 / (1 - x)."""
    lo, hi = 0.0, 1.0 - 1e-12
    for _ in range(200):
        mid = (lo + hi) / 2
        if math.exp(-(1 - mid) / u) - mid > 0:
            lo = mid
        else:
            hi = mid
    return 1.0 / (1.0 - lo)


def run(**kw):
    args = [BIN] + [f"{k}={v}" for k, v in kw.items()]
    line = subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()
    print(line, flush=True)
    res = {}
    for part in line.split("|")[1].split():
        k, v = part.split("=")
        res[k] = float(v)
    return res


FAILS = []


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


def p0():
    print("=== P0: inline cleaner, unthrottled; WA against the greedy model", flush=True)
    out = {}
    for u in U:
        r = run(tag=f"P0/{u}", mode="inline", u=u)
        out[u] = r
        want = greedy_uniform_wa(u)
        check("P0", f"WA at u={u} within 10% of greedy model", abs(r["wa"] / want - 1) < 0.10,
              f"{r['wa']:.2f}", f"{want:.2f}+-10%")
    table("u", ["writes/s", "owner ns/write", "copies/user", "p99 us", "p99.9 us", "max us"],
          [(f"{u}", [f"{r['writes_per_s']:.0f}", f"{r['owner_work_ns']:.0f}", f"{r['copies_per_user']:.2f}",
                     f"{r['lat_p99_us']:.1f}", f"{r['lat_p999_us']:.1f}", f"{r['lat_max_us']:.0f}"])
           for u, r in out.items()])
    return out


def p0b():
    print("=== P0b: inline cleaner at u=0.95, 4096 segments of 16, 64 or 256 blocks, and 64 blocks with 12x warm-up",
          flush=True)
    rows = []
    for name, kw in (("16", dict(seg_blocks=16)), ("64", dict(seg_blocks=64)), ("256", dict(seg_blocks=256)),
                     ("64, warm 12x", dict(seg_blocks=64, warm=12))):
        r = run(tag=f"P0b/{name}", mode="inline", u=0.95, **kw)
        rows.append((name, [f"{r['wa']:.2f}", f"{greedy_uniform_wa(0.95):.2f}"]))
    table("blocks per segment", ["WA", "greedy model"], rows)


def scale(group, mode, inline):
    print(f"=== {group}: {mode} mode, unthrottled at u=0.95, 1-3 workers", flush=True)
    out = {}
    for w in (1, 2, 3):
        out[w] = run(tag=f"{group}/{w}", mode=mode, workers=w, u=0.95)
    base = inline[0.95]["writes_per_s"]
    table("workers", ["writes/s", "x inline", "owner work ns", "owner stall ns", "worker cpu ns/user", "wasted/user"],
          [(f"{w}", [f"{r['writes_per_s']:.0f}", f"{r['writes_per_s'] / base:.2f}", f"{r['owner_work_ns']:.0f}",
                     f"{r['stall_ns']:.0f}", f"{r['worker_cpu_ns']:.0f}", f"{r['wasted_per_user']:.4f}"])
           for w, r in out.items()])
    check(group, "max write rate with 2 workers vs inline", out[2]["writes_per_s"] > 1.5 * base,
          f"{out[2]['writes_per_s'] / base:.2f}x", ">1.5x")
    check(group, "max write rate grows from 2 to 3 workers", out[3]["writes_per_s"] > 1.1 * out[2]["writes_per_s"],
          f"{out[3]['writes_per_s'] / out[2]['writes_per_s']:.2f}x", ">1.1x")
    return out


def p3():
    print(f"=== P3: owner paced at {RATE} user writes/s; inline vs owner vs shared", flush=True)
    out = {}
    for u in (0.90, 0.95):
        for name, kw in (("inline", dict(mode="inline")), ("owner/1", dict(mode="owner", workers=1)),
                         ("owner/2", dict(mode="owner", workers=2)), ("shared/1", dict(mode="shared", workers=1)),
                         ("shared/2", dict(mode="shared", workers=2))):
            out[(u, name)] = run(tag=f"P3/{u}/{name}", u=u, rate=RATE, **kw)
        rows = []
        for name in ("inline", "owner/1", "owner/2", "shared/1", "shared/2"):
            r = out[(u, name)]
            total = r["owner_work_ns"] + r["worker_cpu_ns"]
            rows.append((name, [f"{r['owner_work_ns']:.0f}", f"{r['worker_cpu_ns']:.0f}", f"{total:.0f}",
                                f"{r['stall_ns']:.0f}", f"{r['lat_p50_us']:.2f}", f"{r['lat_p99_us']:.1f}",
                                f"{r['lat_p999_us']:.1f}", f"{r['lat_max_us']:.0f}", f"{r['wa']:.2f}"]))
        table(f"u={u}", ["owner work ns/write", "cleaner cpu ns/write", "total ns/write", "stall ns", "p50 us",
                         "p99 us", "p99.9 us", "max us", "WA"], rows)
        inl, own, sh = out[(u, "inline")], out[(u, "owner/1")], out[(u, "shared/1")]
        check("P3", f"owner work per write, owner mode vs inline, u={u}",
              own["owner_work_ns"] < 0.4 * inl["owner_work_ns"],
              f"{own['owner_work_ns']:.0f}ns vs {inl['owner_work_ns']:.0f}ns", "<0.4x")
        check("P3", f"write p99 owner mode vs inline, u={u}", own["lat_p99_us"] < 0.1 * inl["lat_p99_us"],
              f"{own['lat_p99_us']:.1f}us vs {inl['lat_p99_us']:.1f}us", "<0.1x")
        o_tot = own["owner_work_ns"] + own["worker_cpu_ns"]
        check("P3", f"total CPU per write, owner mode vs inline, u={u}", o_tot < 1.25 * inl["owner_work_ns"],
              f"{o_tot / inl['owner_work_ns']:.2f}x", "<1.25x")
        check("P4", f"owner work per write, owner mode (applies moves) vs shared, u={u}",
              own["owner_work_ns"] < 1.5 * sh["owner_work_ns"],
              f"{own['owner_work_ns']:.0f}ns vs {sh['owner_work_ns']:.0f}ns", "<1.5x")
    return out


def main():
    groups = [a for a in sys.argv[1:] if not a.startswith("--")]
    out_path = os.path.join(HERE, "results.txt")
    if "--out" in sys.argv:
        out_path = sys.argv[sys.argv.index("--out") + 1]
        groups = [g for g in groups if g != out_path]
    subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=HERE, check=True)
    sys.stdout = Tee(out_path)
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    cpu = next((ln.split(":", 1)[1].strip() for ln in open("/proc/cpuinfo") if ln.startswith("model name")), "?")
    print(f"# {platform.release()}, {cpu}, {os.cpu_count()} CPUs, {rustc}, Python {platform.python_version()}, "
          f"{datetime.datetime.now().isoformat(timespec='seconds')}")
    print("# 1 GiB in-memory disk = 4096 segments x 64 blocks of 4K; uniform random 4K overwrites; greedy cleaner; "
          "free_low=8 segments; warm-up 4x capacity, measure 2x capacity")
    want = set(groups) or {"P0", "P0b", "P1", "P2", "P3"}
    inline = p0() if want & {"P0", "P1", "P2"} else None
    if "P0b" in want:
        p0b()
    if "P1" in want:
        scale("P1", "owner", inline)
    if "P2" in want:
        scale("P2", "shared", inline)
    if "P3" in want:
        p3()
    print(f"\n# checks failed: {', '.join(FAILS) or 'none'}")


class Tee:
    def __init__(self, path):
        self.f = open(path, "w")
        self.out = sys.__stdout__

    def write(self, s):
        self.f.write(s)
        self.out.write(s)

    def flush(self):
        self.f.flush()
        self.out.flush()


if __name__ == "__main__":
    main()
