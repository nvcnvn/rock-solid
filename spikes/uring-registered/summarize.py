#!/usr/bin/env python3
"""Median CPU per I/O and IOPS of each row and variant across reps in results.txt, as a markdown table."""
import re
import statistics
import sys
from collections import OrderedDict

path = sys.argv[1] if len(sys.argv) > 1 else "results.txt"
groups = OrderedDict()
for line in open(path):
    m = re.match(r"([NPZV]) rep=\d+ IO (.*)", line)
    if not m:
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", m.group(2)))
    key = (m.group(1), kv["op"], kv["qd"])
    groups.setdefault(key, OrderedDict()).setdefault((kv["fbuf"], kv["ffile"]), []).append(kv)

names = {("0", "0"): "plain", ("0", "1"): "fixed file", ("1", "0"): "fixed buffer", ("1", "1"): "both"}
print("| test | op | QD | " + " | ".join(f"{n} CPU µs/IO" for n in names.values()) + " | both vs plain | " +
      " | ".join(f"{n} kIOPS" for n in names.values()) + " |")
print("|" + " --- |" * (3 + 2 * len(names) + 1))
for (tag, op, qd), variants in groups.items():
    cpu = {k: statistics.median(float(r["cpu_us_per_io"]) for r in v) for k, v in variants.items()}
    iops = {k: statistics.median(float(r["iops"]) for r in v) / 1000 for k, v in variants.items()}
    base, both = cpu[("0", "0")], cpu[("1", "1")]
    delta = f"{both - base:+.3f} µs ({100 * (both - base) / base:+.0f}%)"
    print(f"| {tag} | {op} | {qd} | " + " | ".join(f"{cpu[k]:.3f}" for k in names) + f" | {delta} | " +
          " | ".join(f"{iops[k]:.0f}" for k in names) + " |")
