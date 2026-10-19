#!/usr/bin/env python3
"""Arithmetic behind the metastore-api study. No dependencies: python3 model.py > results.txt"""

GB, MiB, GiB = 1e9, 2**20, 2**30

print("== 1. Creates per second in one directory, by how shared keys are updated")
print("   (commit latency from the design's raft rounds spike; every creator waits for its commit before the next)")
for creators in (1, 64):
    for lat_ms in (0.6, 3.0):
        rounds = 1000 / lat_ms
        cas = rounds  # creators in one round read the same parent value; one wins, the rest retry next round
        upd = rounds * creators  # Update never fails, so every creator's Txn in the round commits
        print(f"   {creators} creator{'s' if creators > 1 else ''} in flight, latency {lat_ms} ms: compare-and-set {cas:,.0f}/s, "
              f"Update {upd:,.0f}/s; 10,000 files {10000 / cas:.1f} s against {10000 / upd:.2f} s")
print("   One loop applies about 60,000 proposals/s at 90% busy (design, Event loop per core), so the Update")
print("   figures above about 50,000/s are capped by the shard's loop, not by conflicts.")

print("\n== 2. Largest sum of grants Σ a disk's gap g_d allows, under two rules for unseen reuse")
print("   time window: Σ + h_d <= g_d, h_d = peak rate x window;  byte budget: Σ x (1 + k) <= g_d, no h_d")
cases = {
    "G4: 100 GB disk at 90%, 16 MiB segments, 3 GB/s": (100 * GB * 0.10 - 8 * 16 * MiB, 3 * GB),
    "G5: 32 GB NVMe at 90%, 4 MiB segments, 3 GB/s": (32 * GB * 0.10 - 8 * 4 * MiB, 3 * GB),
}
for name, (gap, rate) in cases.items():
    tw = ", ".join(f"{w}s {max(0, gap - rate * w) / GB:.2f}" for w in (1, 0.25))
    bb = ", ".join(f"k={k} {gap / (1 + k) / GB:.2f}" for k in (1, 2, 4))
    print(f"   {name}, gap {gap / GB:.2f} GB: time window {tw}; byte budget {bb} GB"
          f"  (10 nodes x 256 MiB = {10 * 256 * MiB / GB:.2f} GB)")

print("\n== 3. Granted bytes the leader has not heard of, on one disk of a 3-node rep3 pool of G4's disks")
print("   (every disk holds a copy of every chunk, so every granted byte can land on one disk)")
gap4 = cases["G4: 100 GB disk at 90%, 16 MiB segments, 3 GB/s"][0]
g = 256 * MiB
for unseen, held, why in ((1.1 * GB * 10, 3 * g, "design today, one client streams 10 s at 1.1 GB/s unseen"),
                          (3 * 2 * g, 3 * g, "budget k = 2, all three nodes at their budget (worst case)"),
                          (3 * 4 * g, 3 * g, "budget k = 4, all three nodes at their budget (worst case)")):
    total = unseen + held
    print(f"   {why}: {unseen / GB:.2f} GB unseen + {held / GB:.2f} GB granted = {total / GB:.2f} GB "
          f"against {gap4 / GB:.2f} GB -> {'overruns the log reserve' if total > gap4 else 'fits'}")
for k in (1, 2, 4):
    sigma = gap4 / (1 + k)
    print(f"   cap binding, G4 with 10 nodes, k = {k}: grants {sigma / GB:.2f} GB, worst landing {sigma * (1 + k) / GB:.2f} GB = g_d")
print("   Without the fixes the review found: a shrink from 256 to 50 MiB on three nodes whose replies are lost lands")
print(f"   up to {3 * 3 * g / GB:.2f} GB (k = 2) while the leader counts {3 * 3 * 50 * MiB / GB:.2f} GB; a floor grant reused at a full disk")
print(f"   lands (1 + k) x g per window, so a 1.1 GB/s writer fills G4's gap in about {gap4 / (1.1 * GB):.0f} windows.")
print("\n   Share of each second a node keeps buffering with no fault, if the window resets only once a second:")
for name, rate in (("10 GbE", 1.1 * GB), ("25 GbE", 2.9 * GB), ("100 GbE", 11 * GB)):
    row = ", ".join(f"k={k}: {min(1, k * g / rate):.0%}" for k in (1, 2, 4))
    print(f"   {name}: {row}  (grant 256 MiB)")
print("   With an early heartbeat at half the budget, the window resets every k x g / 2 bytes, so a node at rate R")
for name, rate in (("10 GbE", 1.1 * GB), ("25 GbE", 2.9 * GB), ("100 GbE", 11 * GB)):
    print(f"   {name}: {rate / (2 * g / 2):.1f} extra heartbeats per second at k = 2, and buffering never stops")
print("   (one more heartbeat costs the leader a memory update, no Raft entry)")

print("\n== 4. Dividing a binding cap: grant_i = min(ask_i, max(floor_i, L)), L the water level that fills the cap;")
print("   floor_i is what node i holds buffered. If the floors alone exceed the cap, every grant is its floor.")
def waterfill(cap, asks, floors):
    if sum(floors) >= cap:
        return list(floors)
    lo, hi = 0.0, float(max(asks))
    for _ in range(100):
        mid = (lo + hi) / 2
        if sum(min(a, max(f, mid)) for a, f in zip(asks, floors)) > cap:
            hi = mid
        else:
            lo = mid
    return [min(a, max(f, lo)) for a, f in zip(asks, floors)]
for asks, floors in (([256] * 4, [0] * 4), ([256, 256, 64, 16], [0] * 4),
                     ([256, 256, 64, 16], [200, 0, 0, 0]), ([256, 256, 64, 16], [250, 0, 0, 0]),
                     ([256] * 4, [200, 200, 200, 0])):
    g = waterfill(512, asks, floors)
    print(f"   cap 512 MiB, asks {asks}, buffered {floors} -> grants {[round(x) for x in g]} MiB, sum {round(sum(g))}")

print("\n== 5. Key sizes against internal/lsm (MaxKey 1,024, MaxValue 1,000). A leaf entry is k + v + 5 bytes plus a")
print("   2-byte slot, and splitPoint needs every entry within half of a 4,080-byte page: k + v <= 2,033.")
fs = 1 + 4 + 8  # T(fs) volume inode
keys = {
    "directory entry, 255-byte name": fs + 1 + 255,
    "xattr, 255-byte name": fs + 1 + 255,
    "xattr piece, 255-byte name": fs + 1 + 1 + 255,
    "lock (mode, session)": fs + 1 + 1 + 8,
    "replay record (session, mount, request)": fs + 1 + 8 + 4 + 8,
    "purge entry": fs + 1 + 8,
    "S3 index, 1,024-byte key": 1 + 4 + 1024,
}
for k, n in keys.items():
    print(f"   {k}: {n} B{'  > MaxKey' if n > 1024 else ''}")
print(f"   MaxKey 1,032 with MaxValue 1,000: k + v = {1032 + 1000} <= 2,033")
print(f"   A range-removal fragment holds two bounds: with both at MaxKey 1,032 it needs {2 * 1032 + 7} B > 2,040;")
print(f"   with bounds of at most 1,000 B each, {2 * 1000 + 7} B fits. Named range bounds: inode keys {fs + 1} B,")
print(f"   a volume 5 B, replay records {fs + 1 + 8 + 4 + 8} B.")
print(f"   64 KiB xattr in values of up to 1,000 B: {-(-65536 // 1000)} values, "
      f"about {(-(-65536 // 1000)) * (1000 + 271) / 1024:.0f} KiB of Txn")
print(f"   4,095-byte symlink target: {-(-4095 // 1000)} values")
