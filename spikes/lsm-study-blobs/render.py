"""Prints the README's tables from results.txt."""
import re, sys

runs = []
cur = None
for line in open(sys.argv[1] if len(sys.argv) > 1 else "results.txt"):
    kv = dict(re.findall(r"([\w/]+)=([^\s]+)", line))
    if line.startswith("RUN"):
        cur = {"run": kv}
        runs.append(cur)
    elif "distinct_count_keys" in line:
        cur["pass"] = kv
    elif "model=" in line:
        cur[kv["model"]] = kv

def f(x, n=2):
    return f"{float(x):.{n}f}"

print("| workload | fill | order | WA | moves per user write | D: puts per move | D: cleaner's share of extent puts | DC: extents per live block | F per run: puts per move | F per run: forwarding entries per live block | F per record: puts per move | F per record: forwarding entries per live block | reads through F |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in sorted(runs, key=lambda r: (r["run"]["wl"] != "uniform", r["run"]["u"], r["run"]["order"])):
    R, P, D, DC, F = r["run"], r["pass"], r["D"], r["DC"], r["F"]
    print(f"| {R['wl']} | {R['u']} | {R['order']} | {f(R['WA'])} | {f(R['moved_per_user'])} | {f(D['move_puts/moved'],3)} | {float(D['cleaner_share_extent'])*100:.0f}% "
          f"| {f(DC['extents/live_block'],4)} (D {f(D['extents/live_block'],4)}) | {f(F['fwd_puts/moved'],3)} | {f(F['fwd_runs/live_block'],3)} "
          f"| {f(P['fwd_rec_puts/moved'],3)} | {f(P['live_fwd_rec_entries/live_block'],3)} | {float(F['reads_via_fwd'])*100:.0f}% |")
