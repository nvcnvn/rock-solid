#!/usr/bin/env python3
"""Capacity model: what a pool can write before its first disk reaches write_limit.

Builds integer placement tables the way docs/design.md describes (refill onto the
least-full eligible disk, then move one member at a time from the fullest disk to the
emptiest eligible one), once with the design's capacity-weighted targets and once with
targets capped by the failure-domain rule. For each table it compares four capacity
figures with what uniformly hashed 4 MiB chunks actually write before the first refusal,
then measures how a node's write-back buffer spreads over disks, which sizes space grants.

Usage: python3 model.py [test ...]   (C1..C6, B1, G1; default all)
"""

import math
import sys

import numpy as np

GB = 1e9
MIB = 1 << 20
CHUNK = 4 * MIB
LIMIT = 0.90
MARGIN = 0.05


class Pool:
    def __init__(self, name, width, copies_per_slot, max_per_domain, overhead):
        self.name = name
        self.width = width
        self.slot_weight = copies_per_slot
        self.q = max_per_domain
        self.b = overhead


def rep(r):
    return Pool(f"rep{r}", r, 1.0, 1, float(r))


def ec(k, m, q):
    return Pool(f"ec{k}-{m} at {q} per node", k + m, 1.0 / k, q, (k + m) / k)


def none():
    return Pool("none", 1, 1.0, 1, 1.0)


class Layout:
    """disks: list of (domain, size, used by other pools)."""

    def __init__(self, disks):
        self.domain = np.array([d for d, _, _ in disks])
        self.size = np.array([s for _, s, _ in disks], dtype=float)
        self.used = np.array([u for _, _, u in disks], dtype=float)
        self.headroom = np.maximum(0.0, LIMIT * self.size - self.used)
        self.domains = sorted(set(self.domain.tolist()))


def water_fill(weights, cap):
    """Fractions f_i = min(cap, lam * w_i) summing to 1; None if the caps cannot reach 1."""
    w = np.asarray(weights, dtype=float)
    if cap * len(w) < 1 - 1e-12:
        return None
    capped = np.zeros(len(w), dtype=bool)
    while True:
        rest = w[~capped].sum()
        lam = (1 - cap * capped.sum()) / rest if rest > 0 else 0.0
        f = np.where(capped, cap, lam * w)
        over = (~capped) & (f > cap + 1e-12)
        if not over.any():
            return f
        capped |= over


def targets(layout, pool, mode):
    """Per-disk share of the pool's member slots."""
    if mode == "weighted":
        return layout.size / layout.size.sum()
    w_disk = layout.headroom
    dom_w = np.array([w_disk[layout.domain == d].sum() for d in layout.domains])
    f = water_fill(dom_w, pool.q / pool.width)
    share = np.zeros(len(layout.size))
    for i, d in enumerate(layout.domains):
        idx = layout.domain == d
        share[idx] = f[i] * w_disk[idx] / w_disk[idx].sum()
    return share


def place(layout, pool, n_pgs, mode, rng):
    """Refill, then balance: the design's two steps. Returns (members, converged, worst ratio)."""
    n = len(layout.size)
    tgt = targets(layout, pool, mode) * n_pgs * pool.width
    load = np.zeros(n)
    members = [[] for _ in range(n_pgs)]

    def eligible(pg, d, leaving=None):
        if d in members[pg]:
            return False
        dom = layout.domain[d]
        count = sum(1 for x in members[pg] if x != leaving and layout.domain[x] == dom)
        return count < pool.q

    for pg in range(n_pgs):
        for _ in range(pool.width):
            cands = [d for d in range(n) if tgt[d] > 0 and eligible(pg, d)]
            if not cands:
                raise ValueError("width does not fit the failure domains")
            d = min(cands, key=lambda d: ((load[d] + 1) / tgt[d], rng.random()))
            members[pg].append(d)
            load[d] += 1

    converged = False
    for _ in range(n_pgs * pool.width * 4):
        ratio = np.where(tgt > 0, load / np.maximum(tgt, 1e-12), 0)
        if np.all(np.abs(ratio[tgt > 0] - 1) <= MARGIN):
            converged = True
            break
        moved = False
        for src in np.argsort(-ratio):
            if ratio[src] <= 1 + MARGIN:
                break
            best = None
            for pg in range(n_pgs):
                if src not in members[pg]:
                    continue
                for d in range(n):
                    if tgt[d] <= 0 or not eligible(pg, d, leaving=src):
                        continue
                    after = (load[d] + 1) / tgt[d]
                    if after < ratio[src] - 1 / tgt[src] + 1e-9 and (best is None or after < best[0]):
                        best = (after, pg, d)
            if best:
                _, pg, d = best
                members[pg][members[pg].index(src)] = d
                load[src] -= 1
                load[d] += 1
                moved = True
                break
        if not moved:
            break
    ratio = np.where(tgt > 0, load / np.maximum(tgt, 1e-12), 0)
    return members, converged, float(np.max(np.abs(ratio[tgt > 0] - 1)))


def shares(layout, pool, members):
    s = np.zeros(len(layout.size))
    for m in members:
        for d in m:
            s[d] += pool.slot_weight
    return s / len(members)


def naive_design(layout, pool):
    return (layout.size - layout.used).sum() / pool.b


def naive_limit(layout, pool):
    return layout.headroom.sum() / pool.b


def ceph_max_avail(layout, pool):
    w = layout.size / layout.size.sum()
    return np.min(layout.headroom / w) / pool.b


def available_now(layout, s):
    use = s > 0
    return float(np.min(layout.headroom[use] / s[use]))


def balanced(layout, pool, without=None):
    """Largest D with sum_i min(c_i, a D) >= b D over failure domains: min over j of S_{>j}/(b - j a)."""
    c = sorted((layout.headroom[layout.domain == d].sum() for d in layout.domains if d != without),
               reverse=True)
    if pool.q * len(c) < pool.width:
        return 0.0
    a = pool.q * pool.slot_weight
    best = math.inf
    for j in range(len(c) + 1):
        if pool.b - j * a <= 1e-12:
            break
        best = min(best, sum(c[j:]) / (pool.b - j * a))
    return best


def incidence(layout, pool, members):
    inc = np.zeros((len(members), len(layout.size)))
    for pg, m in enumerate(members):
        for d in m:
            inc[pg, d] += pool.slot_weight
    return inc


def fill(layout, pool, members, rng, batch=65536):
    """Write uniformly hashed chunks until the first refusal; returns logical bytes written."""
    inc = incidence(layout, pool, members) * CHUNK
    level = layout.used.copy()
    limit = LIMIT * layout.size
    written = 0
    while True:
        pgs = rng.integers(len(members), size=batch)
        cum = level + np.cumsum(inc[pgs], axis=0)
        bad = np.nonzero((cum > limit).any(axis=1))[0]
        if len(bad):
            return (written + bad[0]) * CHUNK
        level = cum[-1]
        written += batch


SCENARIOS = {
    "C1": ("rep2 on two nodes, 0.7 and 2 GB disks", rep(2),
           [("A", 0.7 * GB, 0), ("B", 2 * GB, 0)], 128),
    "C2": ("rep3 on four nodes, 4, 4, 2 and 1 TB", rep(3),
           [("A", 4e12, 0), ("B", 4e12, 0), ("C", 2e12, 0), ("D", 1e12, 0)], 128),
    "C3": ("ec4-2 at 2 per node on three nodes, 2x2, 2x2 and 2x0.5 TB", ec(4, 2, 2),
           [("A", 2e12, 0), ("A", 2e12, 0), ("B", 2e12, 0), ("B", 2e12, 0),
            ("C", 0.5e12, 0), ("C", 0.5e12, 0)], 128),
    "C4": ("none, spread, three 2 TB disks, one holding 1.2 TB of another pool", none(),
           [("A", 2e12, 1.2e12), ("B", 2e12, 0), ("C", 2e12, 0)], 256),
    "C6": ("rep2 on three nodes, 4, 1 and 1 TB", rep(2),
           [("A", 4e12, 0), ("B", 1e12, 0), ("C", 1e12, 0)], 128),
    "C5": ("rep3 on ten nodes, 4,4,4,2,2,2,1,1,1,0.5 TB", rep(3),
           [(chr(65 + i), s * 1e12, 0) for i, s in enumerate([4, 4, 4, 2, 2, 2, 1, 1, 1, 0.5])], 512),
}


def fmt(x):
    return f"{x / GB:,.2f} GB" if x < 1e12 else f"{x / 1e12:,.3f} TB"


def run_capacity(key, trials=40):
    title, pool, disks, n_pgs = SCENARIOS[key]
    layout = Layout(disks)
    print(f"== {key}: {title}, {pool.name}, {n_pgs} PGs, write_limit {LIMIT:.0%}")
    print(f"   naive, free raw / overhead (design text)  {fmt(naive_design(layout, pool))}")
    print(f"   naive, headroom to write_limit / overhead {fmt(naive_limit(layout, pool))}")
    print(f"   Ceph MAX AVAIL rule (CRUSH weight = size)  {fmt(ceph_max_avail(layout, pool))}")
    print(f"   balanced bound over failure domains       {fmt(balanced(layout, pool))}")
    big = max(layout.domains, key=lambda d: layout.size[layout.domain == d].sum())
    print(f"   balanced bound after losing domain {big}       {fmt(balanced(layout, pool, without=big))}")
    for mode in ("weighted", "capped"):
        rng = np.random.default_rng(1)
        members, conv, worst = place(layout, pool, n_pgs, mode, rng)
        s = shares(layout, pool, members)
        avail = available_now(layout, s)
        got = np.array([fill(layout, pool, members, np.random.default_rng(100 + t)) for t in range(trials)])
        print(f"   [{mode:8s}] converged={conv} worst |load/target-1|={worst:.2f} "
              f"available-from-table {fmt(avail)}; first refusal over {trials} fills: "
              f"mean {fmt(got.mean())} ({got.mean() / avail:.2f}x), p5 {fmt(np.percentile(got, 5))}")


def run_buffer_skew(trials=20000):
    """B1: how a full 256 MiB buffer (64 chunks, random PGs) lands on each disk, against its share."""
    print("== B1: per-disk bytes of one node's full 256 MiB buffer, relative to buffer x share")
    for key in ("C2", "C5"):
        title, pool, disks, n_pgs = SCENARIOS[key]
        layout = Layout(disks)
        members, _, _ = place(layout, pool, n_pgs, "capped", np.random.default_rng(1))
        s = shares(layout, pool, members)
        inc = incidence(layout, pool, members)
        rng = np.random.default_rng(7)
        pgs = rng.integers(n_pgs, size=(trials, 64))
        per_disk = inc[pgs].sum(axis=1)
        ratio = per_disk / (64 * s)
        worst = ratio.max(axis=1)
        print(f"   {key} ({pool.name}, {len(s)} disks, {n_pgs} PGs): disk share {s.min():.3f}..{s.max():.3f}; "
              f"worst disk per buffer p50 {np.percentile(worst, 50):.2f}x p99 {np.percentile(worst, 99):.2f}x "
              f"max {worst.max():.2f}x; any one disk p99 {np.percentile(ratio, 99):.2f}x")


def run_grants(trials=2000):
    """G1: one pool-wide grant per node, at C5's table, 10 nodes."""
    title, pool, disks, n_pgs = SCENARIOS["C5"]
    layout = Layout(disks)
    members, _, _ = place(layout, pool, n_pgs, "capped", np.random.default_rng(1))
    s = shares(layout, pool, members)
    inc = incidence(layout, pool, members)
    nodes, buf = 10, 64
    rng = np.random.default_rng(11)
    print("== G1: pool-wide grant of 256 MiB per node, each disk keeping 10 x 256 MiB x share in reserve")
    over = 0
    worst = 0.0
    for _ in range(trials):
        pgs = rng.integers(n_pgs, size=(nodes, buf))
        per_disk = inc[pgs.ravel()].sum(axis=0)
        reserve = nodes * buf * s
        r = (per_disk / reserve).max()
        worst = max(worst, r)
        over += r > 1
    print(f"   all 10 buffers full at once: some disk past its reserve in {over}/{trials} trials, worst {worst:.2f}x")


def main(argv):
    tests = argv or ["C1", "C2", "C3", "C4", "C5", "C6", "B1", "G1"]
    print(f"capacity model; python {sys.version.split()[0]}, numpy {np.__version__}")
    for t in tests:
        if t in SCENARIOS:
            run_capacity(t)
        elif t == "B1":
            run_buffer_skew()
        elif t == "G1":
            run_grants()
        else:
            raise SystemExit(f"unknown test {t}")


if __name__ == "__main__":
    main(sys.argv[1:])
