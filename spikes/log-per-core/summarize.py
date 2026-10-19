"""Medians over repetitions of logpercore's raw lines, and the spike's checks."""
import re
import statistics
import sys

lines = sys.stdin.read().splitlines()
print(lines[0])
runs = {}
for ln in lines[1:]:
    f = ln.split()
    if not f or f[0] not in ("hop", "perloop", "owner"):
        continue
    kv = dict(x.split("=", 1) for x in f[1:])
    key = (f[0], kv.get("batch") or kv.get("k"))
    runs.setdefault(key, []).append({k: float(re.sub(r"[^0-9.]", "", v)) for k, v in kv.items()})

def med(key, field):
    return statistics.median(r[field] for r in runs[key])

print("\nmedians over repetitions")
for key in sorted(runs):
    fields = list(runs[key][0])
    print(key[0], " ".join(f"{f}={med(key, f):g}" for f in fields), f"(n={len(runs[key])})")

def check(name, got, want, ok):
    print(f"[{'OK ' if ok else 'FAIL'}] {name}: got={got} want={want}")

print("\nchecks (expected outcomes written before the full run)")
p1, p2, p4 = (med(("perloop", k), "items/s") for k in ("1", "2", "4"))
check("C1 one log per loop, 2 loops vs 1: items/s ratio (N7 on NVMe: about 2x)", round(p2 / p1, 2), ">= 1.6", p2 / p1 >= 1.6)
f = max(med(("owner", k), "dev_flushes/record") for k in ("1", "2", "4", "8"))
check("C2 owner: device flushes per record", round(f, 2), "<= 1.00", f <= 1.005)
for k in ("4", "8"):
    o, p = med(("owner", k), "items/s"), med(("perloop", k), "items/s")
    check(f"C3 k={k}: owner items/s / per-loop items/s", round(o / p, 2), ">= 1.0", o >= p)
for k in ("1", "2", "4", "8"):
    o, p = med(("owner", k), "cpu/item"), med(("perloop", k), "cpu/item")
    check(f"C4 k={k}: owner CPU per item / per-loop CPU per item", round(o / p, 2), "<= 1.25", o / p <= 1.25)
h1, h64 = med(("hop", "1"), "cpu/item/hop"), med(("hop", "64"), "cpu/item/hop")
check("C5 hop, 1 item: CPU per hop (us)", h1, "<= 20", h1 <= 20)
check("C5 hop, 64 items: CPU per item per hop (us)", h64, "<= 1", h64 <= 1)
print("\nraw output")
