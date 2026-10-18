#!/usr/bin/env python3
"""Median of each M configuration across reps in results.txt, as a markdown table."""
import re
import statistics
import sys
from collections import OrderedDict

path = sys.argv[1] if len(sys.argv) > 1 else "results.txt"
groups = OrderedDict()
for line in open(path):
    if not line.startswith("M "):
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    key = (kv["limit"], kv["load"], kv["loops"], kv["gomaxprocs"], kv["procs_set"])
    groups.setdefault(key, []).append(kv)

cols = [("throttled_pct", "throttled periods %"), ("throttled_ms", "throttled ms"), ("cores", "cores"),
        ("cpu_us_per_op", "CPU µs/op"), ("lat_p50_us", "p50 µs"), ("lat_p99_us", "p99 µs"),
        ("lat_p999_us", "p99.9 µs"), ("gc_pause_max_us", "GC pause max µs"), ("sched_lat_p99_us", "sched p99 µs"),
        ("gc_cycles", "GC cycles"), ("garbage_mb_per_s", "background MB/s"), ("worker_miters_per_s", "worker Mi/s"),
        ("drops", "drops")]
print("| limit | load | loops | GOMAXPROCS | n | " + " | ".join(c[1] for c in cols) + " |")
print("|" + " --- |" * (5 + len(cols)))
for (limit, load, loops, procs, procs_set), rows in groups.items():
    cells = []
    for c, _ in cols:
        v = statistics.median(float(r[c]) for r in rows)
        cells.append(f"{v:.2f}" if c in ("cores", "cpu_us_per_op") else f"{v:.1f}" if c == "throttled_pct" else f"{v:.0f}")
    p = procs + (" (default)" if procs_set == "default" else "")
    print(f"| {limit} | {load} | {loops} | {p} | {len(rows)} | " + " | ".join(cells) + " |")
