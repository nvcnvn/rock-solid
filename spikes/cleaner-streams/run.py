#!/usr/bin/env python3
"""Drive the cleaner-streams simulator through groups Q13, Q14 and Q15; print raw RUN lines, tables and checks.

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
BIN = os.path.join(HERE, "target", "release", "cleaner-streams")
U3 = [0.80, 0.90, 0.95]
FAST = dict(warm=4, windows=2)
SKEW = {
    "uniform": dict(wl="uniform"),
    "hc80": dict(wl="hotcold", hot_w=0.8, hot_f=0.2),
    "hc90": dict(wl="hotcold", hot_w=0.9, hot_f=0.1),
    "zipf": dict(wl="zipf", theta=0.99),
}
# Q14 separation policies on top of cost-benefit with the age cap (the design's cleaner)
SEPS = {
    "base2": dict(sep="base", streams=2),
    "moves3": dict(sep="moves", streams=3),
    "moves4": dict(sep="moves", streams=4),
    "sepbit-user3": dict(sep="sepbit_user"),
    "sepbit-gc4": dict(sep="sepbit_gc"),
    "sepbit6": dict(sep="sepbit"),
    "oracle4": dict(sep="oracle"),
}
# 64 GiB disk; the reserve is 512 MiB whatever the segment size
DISK64 = {64: dict(seg_mb=64, segs=1024, free_low=8), 16: dict(seg_mb=16, segs=4096, free_low=32),
          4: dict(seg_mb=4, segs=16384, free_low=128)}
WHOLE = dict(io_kb=4096, rec_kb=4096)
# a real 64 MiB-segment disk: checkpoint every 64 MiB, 17 B manifest rows, 1 MiB small-run bucket
REAL_META = dict(ckpt_mb=64, man_row_b=17, small_run_kb=1024)
HEAVY_BLOCKS = 16 << 20


def greedy_uniform_wa(u):
    lo, hi = 0.0, 1.0 - 1e-12
    for _ in range(200):
        mid = (lo + hi) / 2
        if math.exp(-(1 - mid) / u) - mid > 0:
            lo = mid
        else:
            hi = mid
    return 1.0 / (1.0 - lo)


def hotcold_oracle(u, hot_w, hot_f):
    """Two perfectly separated pools, greedy-uniform inside each, spare space split optimally, no headers."""
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
    segs = int(kw.get("segs", 16 * 1024 // seg_mb))
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
        kw.setdefault("tag", f"{group}:" + "/".join(map(str, key)))
        jobs.append((group, key, kw))

    if "Q13" in groups:
        for u in U3:
            add("A1", ("log64-greedy1", u), wl="chunkrand", u=u, pol="greedy", streams=1, **WHOLE, **DISK64[64], **FAST)
            for seg in (64, 16, 4):
                add("A1", (f"log{seg}", u), wl="chunkrand", u=u, **WHOLE, **DISK64[seg], **FAST)
            add("A1", ("slots15", u), wl="chunkrand", u=u, slots=1, **WHOLE, **DISK64[64], **FAST)
            add("A1", ("slots16", u), wl="chunkrand", u=u, slots=1, slot_hdr_in_log=1, **WHOLE, **DISK64[64], **FAST)
        for fc in (4, 16, 64):
            for w in (1, 8):
                add("A2", (fc, w), wl="files", file_chunks=fc, writers=w, u=0.9, **WHOLE, **DISK64[64], **FAST)
        for wf, ww in ((0.5, 0.5), (0.5, 0.2)):
            for u in (U3 if ww == 0.5 else [0.9]):
                mix = dict(wl="mixed", whole_f=wf, whole_w=ww, u=u, **DISK64[64], **FAST)
                add("A3", (ww, "log", u), **mix)
                if u == 0.9:
                    add("A3", (ww, "own", u), whole_stream=2, **mix)
                add("A3", (ww, "slots16", u), slots=1, slot_hdr_in_log=1, **mix)
    if "Q14" in groups:
        for w, wkw in SKEW.items():
            for u in U3:
                for p, pkw in SEPS.items():
                    if p == "oracle4" and not w.startswith("hc"):
                        continue
                    add("B", (w, p, u), u=u, **wkw, **pkw, **FAST)
    if "S" in groups:
        for w in ("uniform", "hc90"):
            for p in ("base2", "sepbit-gc4"):
                add("S1", (w, p, 0.95), u=0.95, pol="greedy", **SKEW[w], **SEPS[p], **FAST)
        small = dict(wl="uniform", meta=1, raft_ratio=0.25, meta_live=0.02, meta_reserve=0.07, **FAST)
        for u in (0.8, 0.9):
            add("S2", ("raft-hot,runs-cold", u), u=u, **small)
            add("S2", ("raft-own,runs-cold", u), u=u, raft_stream=2, **small)
            add("S2", ("raft-hot,runs-own", u), u=u, run_stream=2, **small)
            add("S2", ("raft-own,runs-own", u), u=u, raft_stream=2, run_stream=3, **small)
        fp = dict(wl="chunkrand", u=0.9, **WHOLE, **DISK64[64], **REAL_META, **FAST)
        add("S3", ("slots16", "meta"), slots=1, slot_hdr_in_log=1, meta=1, raft_ratio=0.01, meta_live=0.005, meta_reserve=0.01, **fp)
        add("S4", ("stcs-adjacent", 0.8), wl="uniform", u=0.8, meta=1, compaction="stcs", **FAST)
    if "Q15" in groups:
        for w in ("uniform", "hc90"):
            for u in U3:
                add("C1", (w, "none", u), u=u, **SKEW[w], **FAST)
                add("C1", (w, "runs-cold", u), u=u, meta=1, meta_reserve=0.02, **SKEW[w], **FAST)
                add("C1", (w, "runs-own", u), u=u, meta=1, meta_reserve=0.02, run_stream=2, **SKEW[w], **FAST)
                if u == 0.9:
                    add("C1", (w, "runs-cold-unreserved", u), u=u, meta=1, **SKEW[w], **FAST)
        small = dict(wl="uniform", meta=1, raft_ratio=0.25, meta_live=0.02, meta_reserve=0.04, **FAST)
        for u in (0.8, 0.9):
            add("C2", ("none", u), wl="uniform", u=u, **FAST)
            add("C2", ("raft-hot,runs-cold", u), u=u, **small)
            add("C2", ("raft-own,runs-cold", u), u=u, raft_stream=2, **small)
            add("C2", ("raft-hot,runs-own", u), u=u, run_stream=2, **small)
            add("C2", ("raft-own,runs-own", u), u=u, raft_stream=2, run_stream=3, **small)
        fp = dict(wl="chunkrand", u=0.9, **WHOLE, **DISK64[64], **REAL_META, **FAST)
        add("C3", ("log", "none"), **fp)
        add("C3", ("log", "meta"), meta=1, raft_ratio=0.01, meta_live=0.005, meta_reserve=0.01, **fp)
        add("C3", ("slots16", "none"), slots=1, slot_hdr_in_log=1, **fp)
        add("C3", ("slots16", "meta"), slots=1, slot_hdr_in_log=1, meta=1, raft_ratio=0.01, meta_live=0.005, meta_reserve=0.01, **fp)
        add("C4", ("stcs-adjacent", 0.8), wl="uniform", u=0.8, meta=1, compaction="stcs", **FAST)
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
    return (f"# {cpu}, {os.cpu_count()} vCPUs (shared with other benchmarks), {mem_kb / 2**20:.0f} GiB RAM; "
            f"kernel {platform.release()}; {rustc}; Python {platform.python_version()}; {datetime.date.today().isoformat()}")


def main():
    argv = sys.argv[1:]
    njobs = 3
    out_path = os.path.join(HERE, "results.txt")
    groups = []
    render = []
    i = 0
    while i < len(argv):
        if argv[i] == "--jobs":
            njobs = int(argv[i + 1])
            i += 2
        elif argv[i] == "--render":
            render.append(argv[i + 1])
            i += 2
        elif argv[i] == "--out":
            out_path = argv[i + 1]
            i += 2
        else:
            groups.append(argv[i])
            i += 1
    groups = groups or ["Q13", "Q14", "Q15"]
    if render:
        out = Out(None)
        res = {}
        tags = {kw["tag"]: (grp, key) for grp, key, kw in jobs_for(groups)}
        for path in render:
            for line in open(path):
                if line.startswith("RUN "):
                    r = parse(line.strip())
                    if r["tag"] in tags:
                        res[tags[r["tag"]]] = r
        return tables(out, groups, res)
    out = Out(out_path)
    out(machine_line())
    out(f"# groups {' '.join(groups)}; 4K blocks, 4 MiB chunks, 1 MiB max record; default disk 16 GiB = 4096 x 4 MiB "
        f"segments standing in for 256 GiB of 64 MiB ones; cost-benefit with age cap, free_low=8, batch=4; "
        f"warm-up 4x capacity, 2 windows of 1x capacity")

    jobs = jobs_for(groups)
    heavy = threading.Semaphore(2)
    res = {}

    def run_one(job):
        group, key, kw = job
        is_heavy = blocks_of(kw) >= HEAVY_BLOCKS and kw.get("wl") == "mixed"
        if is_heavy:
            heavy.acquire()
        try:
            p = subprocess.run([BIN] + args_of(kw), capture_output=True, text=True)
        finally:
            if is_heavy:
                heavy.release()
        if p.returncode != 0:
            lines = p.stderr.splitlines()
            err = [lines[i + 1] for i, x in enumerate(lines[:-1]) if "panicked" in x]
            return job, [], err or lines[-2:] or ["?"]
        line = [x for x in p.stdout.splitlines() if x.startswith("RUN ")][0]
        lines = [line]
        if parse(line)["steady"] != "OK" and "retry" not in kw:
            longer = dict(kw, warm=8, windows=4, retry=1)
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

    tables(out, groups, res)


def tables(out, groups, res):
    def g(group, key, field="wa"):
        r = res.get((group, key))
        return r[field] if r else float("nan")

    def keys(group):
        return [jkey for (jg, jkey, _) in jobs if jg == group]

    def f(x, d=2):
        return "ERR" if x != x else f"{x:.{d}f}"

    steady_bad = [r["tag"] for r in res.values() if r["steady"] != "OK"]
    jobs = jobs_for(groups)
    check(out, "every run reached steady state (windows within 3%)", not steady_bad,
          f"{len(res) - len(steady_bad)}/{len(res)} steady, not: {','.join(steady_bad) or '-'}", "all")

    if "Q13" in groups:
        out("\n=== A1: whole 4 MiB chunks rewritten at random, 64 GiB disk, WA at u = 0.80 / 0.90 / 0.95")
        out("| layout | u = 0.80 | u = 0.90 | u = 0.95 | slot segments in use at 0.90 |")
        out("|---|---|---|---|---|")
        for lay in ("log64-greedy1", "log64", "log16", "log4", "slots15", "slots16"):
            out(f"| {lay} | " + " | ".join(f(g("A1", (lay, u)), 3) for u in U3) + f" | {f(g('A1', (lay, 0.9), 'slot_segs'), 0)} |")
        check(out, "A1 slots cost no cleaning (WA <= 1.01 at 0.90)", g("A1", ("slots16", 0.9)) <= 1.01,
              f(g("A1", ("slots16", 0.9)), 4), "<= 1.01")
        check(out, "A1 15 slots per 64 MiB segment cannot hold 95% (15/16 = 93.75%)", ("A1", ("slots15", 0.95)) not in res,
              "ERR" if ("A1", ("slots15", 0.95)) not in res else f(g("A1", ("slots15", 0.95))), "ERR (out of space)")
        out("\n=== A2: whole files of N chunks deleted at random, K writers interleaved, 64 MiB segments, u = 0.90")
        out("| chunks per file on this disk | 1 writer | 8 writers |")
        out("|---|---|---|")
        for fc in (4, 16, 64):
            out(f"| {fc} | {f(g('A2', (fc, 1)), 3)} | {f(g('A2', (fc, 8)), 3)} |")
        out("\n=== A3: half the data in whole chunks, half overwritten at random 4K, 64 MiB segments")
        out("| whole-chunk share of writes | layout | u = 0.80 | u = 0.90 | u = 0.95 |")
        out("|---|---|---|---|---|")
        for ww in (0.5, 0.2):
            for lay in ("log", "own", "slots16"):
                us = U3 if ww == 0.5 else [0.9]
                cells = [f(g("A3", (ww, lay, u)), 3) if (ww, lay, u) in keys("A3") else "-" for u in U3]
                out(f"| {ww} | {lay} | " + " | ".join(cells) + " |")

    if "Q14" in groups:
        out("\n=== B: separation policies, cost-benefit with age cap, 16 GiB stand-in; WA, and ratio to base2")
        out("| workload | u | " + " | ".join(SEPS) + " | header-free oracle |")
        out("|---|---|" + "---|" * (len(SEPS) + 1))
        for w, wkw in SKEW.items():
            for u in U3:
                base = g("B", (w, "base2", u))
                cells = []
                for p in SEPS:
                    x = g("B", (w, p, u))
                    cells.append("-" if (w, p, u) not in [(k[1][0], k[1][1], k[1][2]) for k in res if k[0] == "B"]
                                 else f"{x:.2f} ({x / base:.2f})")
                orc = f"{hotcold_oracle(u, wkw['hot_w'], wkw['hot_f']):.2f}" if w.startswith("hc") else "-"
                out(f"| {w} | {u:.2f} | " + " | ".join(cells) + f" | {orc} |")
        out("\nGap closed toward the oracle with headers: (base2 - best) / (base2 - oracle4)")
        for w in ("hc80", "hc90"):
            for u in U3:
                base, orc = g("B", (w, "base2", u)), g("B", (w, "oracle4", u))
                best_p = min((p for p in SEPS if p not in ("base2", "oracle4")), key=lambda p: g("B", (w, p, u)))
                best = g("B", (w, best_p, u))
                out(f"- {w} u={u:.2f}: base2 {base:.2f}, oracle4 {orc:.2f} ({base / orc:.2f}x), best {best_p} {best:.2f}: "
                    f"{100 * (base - best) / (base - orc):.0f}% of the gap")

    if "S" in groups:
        out("\n=== S1: greedy victim choice with and without age classes, u = 0.95")
        for w in ("uniform", "hc90"):
            out(f"- {w}: base2 {f(g('S1', (w, 'base2', 0.95)))}, sepbit-gc4 {f(g('S1', (w, 'sepbit-gc4', 0.95)))}")
        out("\n=== S2: C2 again with the MetaStore's space taken from the data (meta_reserve 0.07)")
        out("| layout | u | WA total | chunk WA | Raft | runs flushed | runs compacted | moved runs | u_total |")
        out("|---|---|---|---|---|---|---|---|---|")
        for u in (0.8, 0.9):
            for lay in ("raft-hot,runs-cold", "raft-own,runs-cold", "raft-hot,runs-own", "raft-own,runs-own"):
                r = res.get(("S2", (lay, u)))
                if not r:
                    out(f"| {lay} | {u:.2f} | ERR |")
                    continue
                out(f"| {lay} | {u:.2f} | {r['wa']:.3f} | {r['chunk_wa']:.3f} | {r['raft_per_user']:.3f} | "
                    f"{r['runflush_per_user']:.3f} | {r['runcompact_per_user']:.3f} | {r['blobmove_per_user']:.3f} | {r['u_total']:.3f} |")
        r = res.get(("S3", ("slots16", "meta")))
        out("\n=== S3: C3's slot layout with Raft entries counted")
        if r:
            out(f"- slots16 + meta: WA {r['wa']:.3f}, chunk WA {r['chunk_wa']:.3f}, Raft {r['raft_per_user']:.4f}, runs "
                f"{r['runflush_per_user']:.4f} + {r['runcompact_per_user']:.4f}, u_total {r['u_total']:.3f}")
        else:
            out("- ERR")
        check(out, "S4 the design's adjacent-only size-tiered buckets keep the run count bounded",
              ("S4", ("stcs-adjacent", 0.8)) in res, "completed" if ("S4", ("stcs-adjacent", 0.8)) in res else "ERR", "completed")

    if "Q15" in groups:
        out("\n=== C1: block volumes with the chunk index's runs, compactions, manifests and cleaner index updates")
        out("| workload | u | layout | WA total | chunk WA | runs flushed | runs compacted | manifests | moved runs | "
            "index entries per user block: user / cleaner | u_total |")
        out("|---|---|---|---|---|---|---|---|---|---|---|")
        for w in ("uniform", "hc90"):
            for u in U3:
                for lay in ("none", "runs-cold", "runs-own", "runs-cold-unreserved"):
                    if lay == "runs-cold-unreserved" and u != 0.9:
                        continue
                    r = res.get(("C1", (w, lay, u)))
                    if not r:
                        out(f"| {w} | {u:.2f} | {lay} | ERR |")
                        continue
                    out(f"| {w} | {u:.2f} | {lay} | {r['wa']:.3f} | {r['chunk_wa']:.3f} | {r['runflush_per_user']:.3f} | "
                        f"{r['runcompact_per_user']:.3f} | {r['manifest_per_user']:.4f} | {r['blobmove_per_user']:.3f} | "
                        f"{r['idx_user_entries_per_write'] :.2f} / {r['idx_clean_entries_per_user']:.2f} | {r['u_total']:.3f} |")
        out("\n=== C2: small-file pool stand-in: uniform 4K plus Raft entries of 25% of the data bytes, MetaStore 2% of the disk")
        out("| layout | u | WA total | chunk WA | Raft | runs flushed | runs compacted | moved runs | u_total |")
        out("|---|---|---|---|---|---|---|---|---|")
        for u in (0.8, 0.9):
            for lay in ("none", "raft-hot,runs-cold", "raft-own,runs-cold", "raft-hot,runs-own", "raft-own,runs-own"):
                r = res.get(("C2", (lay, u)))
                if not r:
                    out(f"| {lay} | {u:.2f} | ERR |")
                    continue
                out(f"| {lay} | {u:.2f} | {r['wa']:.3f} | {r['chunk_wa']:.3f} | {r['raft_per_user']:.3f} | "
                    f"{r['runflush_per_user']:.3f} | {r['runcompact_per_user']:.3f} | {r['blobmove_per_user']:.3f} | {r['u_total']:.3f} |")
        out("\n=== C3: file pool of whole chunks deleted at random, 64 MiB segments, Raft 1% of data, u = 0.90")
        out("| layout | metadata | WA total | chunk WA | Raft | runs flushed | runs compacted | u_total |")
        out("|---|---|---|---|---|---|---|---|")
        for lay in ("log", "slots16"):
            for m in ("none", "meta"):
                r = res.get(("C3", (lay, m)))
                if not r:
                    out(f"| {lay} | {m} | ERR |")
                    continue
                out(f"| {lay} | {m} | {r['wa']:.3f} | {r['chunk_wa']:.3f} | {r['raft_per_user']:.4f} | "
                    f"{r['runflush_per_user']:.4f} | {r['runcompact_per_user']:.4f} | {r['u_total']:.3f} |")
        check(out, "C4 the design's adjacent-only size-tiered buckets keep the run count bounded",
              ("C4", ("stcs-adjacent", 0.8)) in res, "completed" if ("C4", ("stcs-adjacent", 0.8)) in res else "ERR", "completed")


if __name__ == "__main__":
    main()
