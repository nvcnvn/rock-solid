#!/usr/bin/env python3
"""Drive the cleaner-wa simulator through tests V, C1-C7; print raw RUN lines, tables and checks.

python3 run.py [GROUP ...] [--jobs N] [--out results.txt]
"""
import concurrent.futures as cf
import datetime
import math
import os
import platform
import subprocess
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "target", "release", "cleaner-wa")
U_GRID = [0.50, 0.70, 0.80, 0.85, 0.90, 0.95]
U_FINE = [0.60, 0.75, 0.875, 0.92, 0.94, 0.97]
POLICIES = {"greedy1": ("greedy", 1), "greedy2": ("greedy", 2), "cb1": ("cb", 1), "cb2": ("cb", 2)}
# cost-benefit with age capped at one disk capacity of user writes (added after cb livelocked on frozen data)
CAP = dict(pol="cb", streams=2, age_cap=1)
SKEW = {
    "hc80": dict(wl="hotcold", hot_w=0.8, hot_f=0.2),
    "hc90": dict(wl="hotcold", hot_w=0.9, hot_f=0.1),
    "zipf": dict(wl="zipf", theta=0.99),
}
HEAVY_BLOCKS = 32 << 20  # disks of 128 GB or more use ~0.4-0.8 GB each; run at most 2 at once


def greedy_uniform_wa(u):
    """Mean-field greedy/FIFO cleaning under uniform random overwrites (Desnoyers 2012, Hu 2009).

    The victim's live fraction x solves x = exp(-(1 - x) / u); WA = 1 / (1 - x).
    """
    lo, hi = 0.0, 1.0 - 1e-12
    for _ in range(200):
        mid = (lo + hi) / 2
        if math.exp(-(1 - mid) / u) - mid > 0:
            lo = mid
        else:
            hi = mid
    return 1.0 / (1.0 - lo)


def u_for_wa(target):
    x = 1.0 - 1.0 / target
    return -(1.0 - x) / math.log(x)


def hotcold_oracle(u, hot_w, hot_f):
    """Two perfectly separated pools, greedy-uniform inside each, spare space split optimally."""
    hot, cold = u * hot_f, u * (1 - hot_f)
    spare = 1.0 - u
    best = float("inf")
    for i in range(1, 2000):
        sh = spare * i / 2000
        wa = hot_w * greedy_uniform_wa(hot / (hot + sh)) + (1 - hot_w) * greedy_uniform_wa(cold / (cold + spare - sh))
        best = min(best, wa)
    return best


def args_of(kw):
    return [f"{k}={v}" for k, v in kw.items() if k != "retry"]


def blocks_of(kw):
    seg_mb = int(kw.get("seg_mb", 4))
    segs = int(kw["segs"]) if "segs" in kw else int(float(kw.get("disk_gb", 16)) * 1024 / seg_mb)
    return seg_mb * 256 * segs


def parse(line):
    head, tail = line[4:].split(" | ")
    d = {}
    for part in (head + " " + tail).split():
        k, _, v = part.partition("=")
        d[k] = v
    for k in list(d):
        try:
            d[k] = float(d[k])
        except ValueError:
            pass
    return d


def jobs_for(groups):
    jobs = []

    def add(group, key, **kw):
        kw.setdefault("tag", f"{group}:" + "/".join(map(str, key if isinstance(key, tuple) else (key,))))
        jobs.append((group, key, kw))

    if "V" in groups:
        for u in U_GRID:
            add("V", u, wl="uniform", u=u, pol="greedy", streams=1, headers=0)
    if "C1" in groups or "C6" in groups:
        for u in U_GRID + (U_FINE if "C6" in groups else []):
            for p, (pol, st) in POLICIES.items():
                if u in U_FINE and p not in ("greedy1", "cb2"):
                    continue
                add("C1", (p, u), wl="uniform", u=u, pol=pol, streams=st)
    if "C2" in groups or "C6" in groups:
        for w, wkw in SKEW.items():
            for u in U_GRID + (U_FINE if "C6" in groups and w != "hc90" else []):
                for p, (pol, st) in POLICIES.items():
                    if u in U_FINE and p not in ("greedy1", "cb2"):
                        continue
                    add("C2", (w, p, u), u=u, pol=pol, streams=st, **wkw)
            for u in U_GRID:
                add("C2", (w, "cb2cap", u), u=u, **CAP, **wkw)
            for order in ("age", "victim"):
                add("C2", (w, "cb2-" + order, 0.9), u=0.9, pol="cb", streams=2, order=order, **wkw)
                add("C2", (w, "cb2cap-" + order, 0.9), u=0.9, order=order, **CAP, **wkw)
    if "C3" in groups:
        for seg in (1, 4, 16, 64, 256):
            for u in (0.8, 0.9, 0.95):
                for p in ("greedy1", "cb2"):
                    pol, st = POLICIES[p]
                    extra = dict(warm=4, windows=2) if seg >= 64 else {}
                    add("C3a", (seg, p, u), wl="uniform", u=u, pol=pol, streams=st, seg_mb=seg, segs=1024, **extra)
        for segs in (256, 1024, 4096, 16384):
            for u in (0.8, 0.9, 0.95):
                for p in ("greedy1", "cb2"):
                    pol, st = POLICIES[p]
                    add("C3b", (segs, p, u), wl="uniform", u=u, pol=pol, streams=st, seg_mb=4, segs=segs)
    if "C4" in groups:
        for rec in (4, 64, 1024):
            for u in (0.8, 0.9, 0.95):
                add("C4", (rec, u), wl="uniform", u=u, pol="cb", streams=2, rec_kb=rec)
        for u in (0.8, 0.9, 0.95):
            add("C4", ("none", u), wl="uniform", u=u, pol="cb", streams=2, headers=0)
            add("C4", ("compact64", u), wl="uniform", u=u, pol="cb", streams=2, entry_b=8, crc_b=4)
    if "C5" in groups:
        for u in (0.8, 0.9, 0.95):
            for p in ("greedy1", "cb2"):
                pol, st = POLICIES[p]
                add("C5", ("snap", p, u), wl="uniform", u=u, pol=pol, streams=st, snap=0.2, warm=24)
                add("C5", ("snap-hc80", p, u), u=u, pol=pol, streams=st, snap=0.2, warm=24, **SKEW["hc80"])
            add("C5", ("snap", "cb2cap", u), wl="uniform", u=u, snap=0.2, warm=24, **CAP)
            add("C5", ("snap-hc80", "cb2cap", u), u=u, snap=0.2, warm=24, **CAP, **SKEW["hc80"])
    if "C6" in groups:
        for u in (0.8, 0.9, 0.95):
            add("C6f", ("fifo", 64, u), wl="fifo", u=u, pol="cb", streams=2, io_kb=4096, rec_kb=4096, seg_mb=64, segs=1024)
            for seg in (4, 16, 64):
                for p in ("greedy1", "cb2"):
                    pol, st = POLICIES[p]
                    add("C6f", ("chunkrand", seg, p, u), wl="chunkrand", u=u, pol=pol, streams=st,
                        io_kb=4096, rec_kb=4096, seg_mb=seg, segs=1024)
    return jobs


class Out:
    def __init__(self, path):
        self.f = open(path, "w") if path else None

    def __call__(self, s=""):
        print(s, flush=True)
        if self.f:
            self.f.write(s + "\n")
            self.f.flush()


def check(out, name, ok, got, want):
    out(f"[{'OK ' if ok else 'FAIL'}] {name}: got={got} want={want}")


def machine_line():
    cpu = "?"
    with open("/proc/cpuinfo") as f:
        for line in f:
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    with open("/proc/meminfo") as f:
        mem_kb = int(f.readline().split()[1])
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True, cwd=HERE).stdout.strip()
    return (f"# {cpu}, {os.cpu_count()} cores, {mem_kb / 2**20:.0f} GiB RAM; kernel {platform.release()}; "
            f"{rustc}; Python {platform.python_version()}; {datetime.date.today().isoformat()}")


def main():
    argv = sys.argv[1:]
    njobs = 11
    out_path = os.path.join(HERE, "results.txt")
    groups = []
    i = 0
    while i < len(argv):
        if argv[i] == "--jobs":
            njobs = int(argv[i + 1])
            i += 2
        elif argv[i] == "--out":
            out_path = argv[i + 1]
            i += 2
        else:
            groups.append(argv[i])
            i += 1
    groups = groups or ["V", "C1", "C2", "C3", "C4", "C5", "C6", "C7"]
    out = Out(out_path)
    out(machine_line())
    out(f"# groups {' '.join(groups)}; 4K blocks, 4 MiB chunks; default disk 16 GiB = 4096 x 4 MiB segments, "
        f"free_low=8 segments, batch=4 victims, 64K group-commit records, warm-up 6x capacity, 4 windows of 1x capacity")

    jobs = jobs_for(groups)
    heavy = threading.Semaphore(2)
    res = {}

    def run_one(job):
        group, key, kw = job
        is_heavy = blocks_of(kw) >= HEAVY_BLOCKS
        if is_heavy:
            heavy.acquire()
        try:
            p = subprocess.run([BIN] + args_of(kw), capture_output=True, text=True)
        finally:
            if is_heavy:
                heavy.release()
        if p.returncode != 0:
            return job, [], p.stderr.strip().splitlines()[-2:] or ["?"]
        line = [x for x in p.stdout.splitlines() if x.startswith("RUN ")][0]
        lines = [line]
        if parse(line)["steady"] != "OK" and "retry" not in kw:
            longer = dict(kw, warm=3 * float(kw.get("warm", 6)), win=2, retry=1)
            _, more, err = run_one((group, key, longer))
            return job, lines + more, err
        return job, lines, None

    with cf.ThreadPoolExecutor(njobs) as ex:
        futs = [ex.submit(run_one, j) for j in jobs]
        for f in cf.as_completed(futs):
            (group, key, kw), lines, err = f.result()
            for i, line in enumerate(lines):
                out(line if i == len(lines) - 1 else "UNSTEADY-RETRIED " + line)
            if lines:
                res[(group, key)] = parse(lines[-1])
            if err:
                out(f"ERR {group} {key} {kw}: {err}")

    def wa(group, key, field="wa"):
        r = res.get((group, key))
        return r[field] if r else float("nan")

    steady_bad = [r["tag"] for r in res.values() if r["steady"] != "OK"]
    check(out, "every run reached steady state (4 windows within 3% of each other)", not steady_bad,
          f"{len(res) - len(steady_bad)}/{len(res)} steady, not: {','.join(steady_bad) or '-'}", "all")

    if "V" in groups:
        out("\n=== V: greedy, one stream, no headers, uniform 4K overwrites vs mean-field model")
        out("| u | u_eff | sim WA | model WA(u_eff) | error |")
        out("|---|---|---|---|---|")
        for u in U_GRID:
            r = res[("V", u)]
            m = greedy_uniform_wa(r["u_eff"])
            out(f"| {u:.2f} | {r['u_eff']:.4f} | {r['wa']:.3f} | {m:.3f} | {100 * (r['wa'] / m - 1):+.1f}% |")
        r = res[("V", 0.5)]
        m = greedy_uniform_wa(r["u_eff"])
        check(out, "V1 u=0.50 greedy uniform matches the mean-field model within 3%",
              abs(r["wa"] / m - 1) <= 0.03, f"{r['wa']:.4f}", f"{m:.4f} +-3%")
        errs = [abs(res[("V", u)]["wa"] / greedy_uniform_wa(res[("V", u)]["u_eff"]) - 1) for u in U_GRID]
        check(out, "V2 all u within 5% of the model", max(errs) <= 0.05, f"max err {100 * max(errs):.1f}%", "<=5%")

    if "C1" in groups or "C6" in groups:
        out("\n=== C1: uniform 4K random overwrites, WA incl. headers (64K records)")
        out("| u | model greedy, no headers | sim greedy 1 stream, no headers (V) | greedy 1 stream | greedy 2 streams | cost-benefit 1 stream | cost-benefit 2 streams |")
        out("|---|---|---|---|---|---|---|")
        for u in U_GRID:
            row = [f"{wa('C1', (p, u)):.2f}" for p in POLICIES]
            out(f"| {u:.2f} | {greedy_uniform_wa(u):.2f} | {wa('V', u):.2f} | " + " | ".join(row) + " |")
        ratios = [wa("C1", ("cb2", u)) / wa("C1", ("greedy1", u)) for u in U_GRID]
        check(out, "C1 uniform: cost-benefit+2 streams within 15% of greedy+1 stream (no locality to exploit)",
              all(0.85 <= x <= 1.15 for x in ratios), "ratios " + ",".join(f"{x:.3f}" for x in ratios), "0.85..1.15")
        r95 = res[("C1", ("greedy1", 0.95))]
        check(out, "C1 greedy uniform at u=0.95 is about 10 (model 10.2 + headers)", 9 <= r95["wa"] <= 12.5,
              f"{r95['wa']:.2f}", "9..12.5")

    if "C2" in groups or "C6" in groups:
        for w in SKEW:
            out(f"\n=== C2: {w} ({SKEW[w]}), WA incl. headers (64K records)")
            hdr = "| u | greedy 1 stream | greedy 2 streams | cost-benefit 1 stream | cost-benefit 2 streams | cost-benefit 2 streams, age cap | uniform greedy 1 |"
            if w.startswith("hc"):
                hdr += " oracle 2 pools |"
            out(hdr)
            out("|---" * (hdr.count("|") - 1) + "|")
            for u in U_GRID:
                row = [f"{wa('C2', (w, p, u)):.2f}" for p in list(POLICIES) + ["cb2cap"]]
                line = f"| {u:.2f} | " + " | ".join(row) + f" | {wa('C1', ('greedy1', u)):.2f} |"
                if w.startswith("hc"):
                    line += f" {hotcold_oracle(u, SKEW[w]['hot_w'], SKEW[w]['hot_f']):.2f} |"
                out(line)
            for p in ("cb2", "cb2cap"):
                out(f"copy order at u=0.90, {p}: chunk {wa('C2', (w, p, 0.9)):.2f}, "
                    f"age {wa('C2', (w, p + '-age', 0.9)):.2f}, victim order {wa('C2', (w, p + '-victim', 0.9)):.2f}")
        for w in SKEW:
            ratios = {u: wa("C2", (w, "cb2", u)) / wa("C2", (w, "greedy1", u)) for u in (0.8, 0.85, 0.9, 0.95)}
            check(out, f"C2 {w}: cost-benefit+2 streams at least 20% below greedy+1 stream for u>=0.8",
                  all(x <= 0.8 for x in ratios.values()),
                  ",".join(f"u{u}:{x:.3f}" for u, x in ratios.items()), "<=0.80")
        for w in SKEW:
            keys = [(p, u) for u in U_GRID for p in ("cb2",)] + [(p + "-" + o, 0.9) for p in ("cb2",) for o in ("age", "victim")]
            worst = max(wa("C2", (w, "cb2cap" + p[3:], u)) / wa("C2", (w, p, u)) for p, u in keys)
            check(out, f"C2 {w}: age cap never hurts (cb2cap <= cb2 * 1.02 at every u and copy order)", worst <= 1.02,
                  f"max ratio {worst:.3f}", "<=1.02")
        for w in ("hc80", "hc90"):
            g = wa("C2", (w, "greedy1", 0.9))
            un = wa("C1", ("greedy1", 0.9))
            check(out, f"C2 {w}: greedy is worse with locality than without (LFS paper, Fig. 4)", g > un,
                  f"{g:.2f} vs uniform {un:.2f}", "greater")

    if "C3" in groups:
        out("\n=== C3a: segment size at a fixed 1024 segments (reserve fraction fixed at 8/1024), uniform")
        out("| segment | disk | u=0.80 greedy1 | u=0.80 cb2 | u=0.90 greedy1 | u=0.90 cb2 | u=0.95 greedy1 | u=0.95 cb2 |")
        out("|---|---|---|---|---|---|---|---|")
        for seg in (1, 4, 16, 64, 256):
            row = [f"{wa('C3a', (seg, p, u)):.2f}" for u in (0.8, 0.9, 0.95) for p in ("greedy1", "cb2")]
            out(f"| {seg} MiB | {seg} GiB | " + " | ".join(row) + " |")
        for u in (0.8, 0.9, 0.95):
            vals = [wa("C3a", (seg, "cb2", u)) for seg in (1, 4, 16, 64, 256)]
            spread = (max(vals) - min(vals)) / min(vals)
            check(out, f"C3a u={u}: WA varies <=5% across 1-256 MiB segments when the segment count is fixed",
                  spread <= 0.05, f"spread {100 * spread:.1f}% ({','.join(f'{v:.2f}' for v in vals)})", "<=5%")
        out("\n=== C3b: segment count (reserve of 8 free segments = 8/S of the disk), 4 MiB stand-in segments, uniform")
        out("| segments S | reserve | u=0.80 greedy1 | u=0.80 cb2 | u=0.90 greedy1 | u=0.90 cb2 | u=0.95 greedy1 | u=0.95 cb2 |")
        out("|---|---|---|---|---|---|---|---|")
        for segs in (256, 1024, 4096, 16384):
            row = [f"{wa('C3b', (segs, p, u)):.2f}" for u in (0.8, 0.9, 0.95) for p in ("greedy1", "cb2")]
            out(f"| {segs} | {100 * 8 / segs:.2f}% | " + " | ".join(row) + " |")
        a, b = wa("C3b", (256, "cb2", 0.95)), wa("C3b", (16384, "cb2", 0.95))
        check(out, "C3b u=0.95: 8 reserve segments out of 256 raise WA by >10% vs out of 16384",
              a > 1.1 * b, f"{a:.2f} vs {b:.2f}", ">10% higher")
        out("\nper-segment metadata and reserve for real disks (32 B/segment: live count, youngest mtime, state, seq)")
        out("| disk | segment | segments | metadata | 8-segment reserve |")
        out("|---|---|---|---|---|")
        for disk_gb in (256, 1024, 4096):
            for seg in (1, 4, 16, 64, 256, 1024):
                s = disk_gb * 1024 // seg
                out(f"| {disk_gb} GiB | {seg} MiB | {s} | {s * 32 / 2**20:.2f} MiB | {100 * 8 / s:.3f}% |")

    if "C4" in groups:
        out("\n=== C4: group-commit record size (uniform, cost-benefit, 2 streams)")
        out("| record | u=0.80 WA | u=0.80 hdr/user | u=0.90 WA | u=0.90 hdr/user | u=0.95 WA | u=0.95 hdr/user |")
        out("|---|---|---|---|---|---|---|")
        for rec in ("none", 4, 64, 1024, "compact64"):
            row = []
            for u in (0.8, 0.9, 0.95):
                r = res[("C4", (rec, u))]
                row += [f"{r['wa']:.2f}", f"{r['hdr_per_user']:.3f}"]
            name = {"none": "no headers", "compact64": "64K, 8-byte entries"}.get(rec) or f"{rec}K ({max(1, rec // 4)} x 4K writes)"
            out(f"| {name} | " + " | ".join(row) + " |")
        for rec, want in ((4, 1.0), (64, 1 / 16), (1024, 3 / 256)):
            r = res[("C4", (rec, 0.9))]
            check(out, f"C4 {rec}K records: user-record header blocks per user block", abs(r["uhdr_per_user"] - want) <= 0.05 * want,
                  f"{r['uhdr_per_user']:.4f} (cleaner records add {r['chdr_per_copy']:.4f} per copied block)", f"{want:.4f} +-5%")
        r4, r0 = res[("C4", (4, 0.9))], res[("C4", ("none", 0.9))]
        check(out, "C4 4K records at u=0.9: total WA about 2x the header-free WA", 1.8 <= r4["wa"] / r0["wa"] <= 2.2,
              f"{r4['wa']:.2f} / {r0['wa']:.2f} = {r4['wa'] / r0['wa']:.2f}", "1.8..2.2")

    if "C5" in groups:
        out("\n=== C5: 20% of the data snapshotted then overwritten; u = total live incl. pinned blocks")
        out("| u total | uniform greedy1 | uniform cb2 | uniform cb2cap | no snapshot greedy1 | no snapshot cb2 | hc80 greedy1 | hc80 cb2 | hc80 cb2cap | hc80 no snapshot cb2 |")
        out("|---|---|---|---|---|---|---|---|---|---|")
        for u in (0.8, 0.9, 0.95):
            row = [wa("C5", ("snap", "greedy1", u)), wa("C5", ("snap", "cb2", u)), wa("C5", ("snap", "cb2cap", u)),
                   wa("C1", ("greedy1", u)), wa("C1", ("cb2", u)),
                   wa("C5", ("snap-hc80", "greedy1", u)), wa("C5", ("snap-hc80", "cb2", u)), wa("C5", ("snap-hc80", "cb2cap", u)),
                   wa("C2", ("hc80", "cb2", u))]
            out(f"| {u:.2f} | " + " | ".join(f"{x:.2f}" for x in row) + " |")
        for u in (0.9, 0.95):
            s, n = wa("C5", ("snap", "cb2", u)), wa("C1", ("cb2", u))
            check(out, f"C5 u={u}: frozen (cold) pinned data makes cb2 WA lower than uniform at the same total u",
                  s < n, f"{s:.2f} vs {n:.2f}", "lower")

    if "C6" in groups:
        out("\n=== C6: highest u that keeps WA (incl. 64K-record headers) at or below a target, linear interpolation")
        out("| workload | policy | WA<=2 | WA<=3 | WA<=5 | WA at 0.85 | WA at 0.95 |")
        out("|---|---|---|---|---|---|---|")
        grid = sorted(U_GRID + U_FINE)
        out(f"| model, greedy uniform, no headers | - | {u_for_wa(2):.3f} | {u_for_wa(3):.3f} | {u_for_wa(5):.3f} | "
            f"{greedy_uniform_wa(0.85):.2f} | {greedy_uniform_wa(0.95):.2f} |")
        for w in ("uniform", "hc80", "zipf"):
            for p in ("greedy1", "cb2"):
                pts = [(u, wa("C1", (p, u)) if w == "uniform" else wa("C2", (w, p, u))) for u in grid]
                pts = [(u, v) for u, v in pts if not math.isnan(v)]
                cells = []
                for t in (2, 3, 5):
                    cross = "-"
                    for (u0, v0), (u1, v1) in zip(pts, pts[1:]):
                        if v0 <= t < v1:
                            cross = f"{u0 + (t - v0) / (v1 - v0) * (u1 - u0):.3f}"
                            break
                    cells.append(cross)
                d = dict(pts)
                out(f"| {w} | {p} | " + " | ".join(cells) + f" | {d.get(0.85, float('nan')):.2f} | {d.get(0.95, float('nan')):.2f} |")
        out("\n=== C6f: file-pool baselines, whole 4 MiB chunk writes (1 write per record), 1024 segments")
        out("| workload | segment | u=0.80 greedy1 | u=0.80 cb2 | u=0.90 greedy1 | u=0.90 cb2 | u=0.95 greedy1 | u=0.95 cb2 |")
        out("|---|---|---|---|---|---|---|---|")
        out("| fifo (delete oldest) | 64 MiB | - | " + " | - | ".join(f"{wa('C6f', ('fifo', 64, u)):.3f}" for u in (0.8, 0.9, 0.95)) + " |")
        for seg in (4, 16, 64):
            row = [f"{wa('C6f', ('chunkrand', seg, p, u)):.3f}" for u in (0.8, 0.9, 0.95) for p in ("greedy1", "cb2")]
            out(f"| random whole-chunk rewrite | {seg} MiB | " + " | ".join(row) + " |")
        f95 = wa("C6f", ("fifo", 64, 0.95))
        check(out, "C6f fifo whole-chunk writes: WA = 1 + headers", f95 < 1.01, f"{f95:.4f}", "<1.01")
        c64, b64 = wa("C6f", ("chunkrand", 64, "greedy1", 0.9)), wa("C1", ("greedy1", 0.9))
        check(out, "C6f random whole-chunk rewrite, 64 MiB segments, u=0.9: 1.1 < WA < 4K-random WA",
              1.1 < c64 < b64, f"{c64:.3f} (4K random {b64:.2f})", f"1.1..{b64:.2f}")

    if "C7" in groups:
        out("\n=== C7: cleaner work per user block and copy-loop CPU")
        p = subprocess.run([BIN, "bench"], capture_output=True, text=True)
        out(p.stdout.strip())
        bench = parse("RUN x=1 | " + p.stdout.split("BENCH ", 1)[1].split(" (")[0])
        per_copy = bench["copy+crc4k_ns"] + bench["hashmap_index_update_ns"]
        out("| case | copies per user block | segment-block reads per user block (whole-segment read) | cleaner CPU per user 4K, us (copy+crc+in-memory index) |")
        out("|---|---|---|---|")
        cases = [("uniform cb2 u=0.80", ("C1", ("cb2", 0.8))), ("uniform cb2 u=0.90", ("C1", ("cb2", 0.9))),
                 ("uniform cb2 u=0.95", ("C1", ("cb2", 0.95))), ("zipf cb2 u=0.90", ("C2", ("zipf", "cb2", 0.9))),
                 ("hc80 cb2 u=0.90", ("C2", ("hc80", "cb2", 0.9)))]
        for name, k in cases:
            r = res.get(k)
            if r:
                out(f"| {name} | {r['copies_per_user']:.2f} | {r['segread_per_user']:.2f} | {r['copies_per_user'] * per_copy / 1000:.2f} |")


if __name__ == "__main__":
    main()
