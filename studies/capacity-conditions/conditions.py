#!/usr/bin/env python3
"""Pool conditions model: where Stranded, CannotReprotect and Unbalanced should fire.

Pure Python, no dependencies. Ports the capped-target placement of
spikes/capacity-model/model.py (refill, then balance within a fill margin) and adds:

  S  the C1..C6 figures under the design's reading and under the proposed rules
  R  reprotectable after removing each domain in turn, by size or by capacity, and
     the room-based reading against the capacity-based one as the pool fills (C2)
  O  stranded fraction on ordinary homelab layouts
  E  shards with max_per_domain above a domain's disk count
  E2 whether the largest-capacity domain is still the worst loss once each disk is capped
  D  layouts without a spare domain: the loss of one disk instead
  T  member moves after a disk addition with data held, targets by room against by capacity
  U  how far available sits under balanced once capped placement stops, on random
     layouts with pg_count sized by the design's rule, which sets the Unbalanced threshold

Usage: python3 conditions.py [S R O E E2 D T U ...]   (default all, about 2 minutes)
"""

import math
import random
import sys

TB = 1e12
LIMIT = 0.90
MARGIN = 0.05


class Pool:
    def __init__(self, name, width, slot_weight, q, b, copies):
        self.name, self.width, self.slot_weight, self.q, self.b = name, width, slot_weight, q, b
        self.copies = copies
        self.a = q * slot_weight


def rep(r):
    return Pool(f"rep{r}", r, 1.0, 1, float(r), r)


def ec(k, m, q):
    return Pool(f"ec{k}-{m}@{q}", k + m, 1.0 / k, q, (k + m) / k, m + 1)


def none():
    return Pool("none", 1, 1.0, 1, 1.0, 1)


def balanced(c, pool):
    """Largest D with sum_i min(c_i, a D) >= b D: min over j of S_{>j} / (b - j a)."""
    c = sorted(c, reverse=True)
    if pool.q * len(c) < pool.width:
        return 0.0
    best = math.inf
    for j in range(len(c) + 1):
        if pool.b - j * pool.a <= 1e-12:
            break
        best = min(best, sum(c[j:]) / (pool.b - j * pool.a))
    return best


def check_balanced(c, pool):
    """Bisection on the definition, to check the closed form."""
    if pool.q * len(c) < pool.width:
        return 0.0
    lo, hi = 0.0, sum(c)
    for _ in range(200):
        mid = (lo + hi) / 2
        if sum(min(x, pool.a * mid) for x in c) >= pool.b * mid:
            lo = mid
        else:
            hi = mid
    return lo


def domain_rooms(disks, extra=None):
    """Room per domain: write_limit x size less every pool's data (others, then this pool's)."""
    out = {}
    for i, (d, size, other) in enumerate(disks):
        own = extra[i] if extra else 0.0
        out[d] = out.get(d, 0.0) + max(0.0, LIMIT * size - other - own)
    return out


def figures(disks, pool, own=None):
    """The figures, with capacity = room + this pool's own data, per domain."""
    room = domain_rooms(disks, own)
    cap = dict(room)
    if own:
        for i, (d, _, _) in enumerate(disks):
            cap[d] += own[i]
    held = sum(own) / pool.b if own else 0.0
    B = balanced(list(cap.values()), pool)
    total = sum(cap.values())
    stranded_raw = total - pool.b * B
    dom_stranded = {d: max(0.0, c - pool.a * B) for d, c in cap.items()}
    by_cap = {d: balanced([c for e, c in cap.items() if e != d], pool) for d in cap}
    return dict(room=room, cap=cap, held=held, B=B, total=total, stranded=stranded_raw / pool.b,
                f_raw=stranded_raw / total if total else 0.0, dom_stranded=dom_stranded, by_cap=by_cap)


# The spike's layouts: (domain, size, bytes of other pools).
SCENARIOS = {
    "C1": ("rep2, nodes of 0.7 and 2 GB", rep(2), [("A", 0.7e9, 0), ("B", 2e9, 0)], 128),
    "C2": ("rep3, nodes of 4, 4, 2, 1 TB", rep(3),
           [("A", 4 * TB, 0), ("B", 4 * TB, 0), ("C", 2 * TB, 0), ("D", 1 * TB, 0)], 128),
    "C3": ("ec4-2 at 2 per node, nodes of 4, 4, 1 TB", ec(4, 2, 2),
           [("A", 2 * TB, 0), ("A", 2 * TB, 0), ("B", 2 * TB, 0), ("B", 2 * TB, 0),
            ("C", 0.5 * TB, 0), ("C", 0.5 * TB, 0)], 128),
    "C4": ("none, three 2 TB disks, A holding 1.2 TB of another pool", none(),
           [("A", 2 * TB, 1.2 * TB), ("B", 2 * TB, 0), ("C", 2 * TB, 0)], 256),
    "C5": ("rep3, ten nodes of 4,4,4,2,2,2,1,1,1,0.5 TB", rep(3),
           [(chr(65 + i), s * TB, 0) for i, s in enumerate([4, 4, 4, 2, 2, 2, 1, 1, 1, 0.5])], 512),
    "C6": ("rep2, nodes of 4, 1, 1 TB", rep(2), [("A", 4 * TB, 0), ("B", 1 * TB, 0), ("C", 1 * TB, 0)], 128),
}


def fmt(x):
    return f"{x / TB:.3f} TB" if x >= 1e11 else f"{x / 1e9:.3f} GB"


def pct(x):
    return f"{100 * x:.1f}%"


def run_S():
    print("== S: C1..C6 under the design's reading and the proposed rules (empty pools)")
    print("   Stranded: design reading = stranded / raw room; proposed = b x stranded / capacity "
          "(the same as stranded / (capacity / b)); fires above 10%")
    for key, (title, pool, disks, _) in SCENARIOS.items():
        f = figures(disks, pool)
        mixed = f["stranded"] / f["total"] if f["total"] else 0.0
        size = {}
        for d, s, _ in disks:
            size[d] = size.get(d, 0.0) + s
        first_by_size = max(size, key=lambda d: size[d])  # the spike's choice: ties go to the first
        largest = max(f["cap"], key=lambda d: f["cap"][d])
        worst = min(f["by_cap"].values())
        assert abs(f["by_cap"][largest] - worst) < 1e-6, "removing the largest capacity is the worst case"
        fits = pool.q * (len(f["cap"]) - 1) >= pool.width
        ask = [d for d, s in f["dom_stranded"].items() if s > 0.5 * f["cap"][d]]
        print(f"-- {key}: {title} ({pool.name}), capacity {fmt(f['total'])}, room / b {fmt(f['total'] / pool.b)}, "
              f"balanced {fmt(f['B'])}, stranded {fmt(f['stranded'])}")
        print(f"   stranded / raw room (design text, mixed units) {pct(mixed)}; b x stranded / capacity {pct(f['f_raw'])}"
              f" -> Stranded {'yes' if f['f_raw'] > 0.10 else 'no'} (design reading: {'yes' if mixed > 0.10 else 'no'})")
        print("   domain stranded room max(0, c_i - a B): " + ", ".join(
            f"{d} {fmt(s)} of {fmt(f['cap'][d])} ({pct(s / f['cap'][d] if f['cap'][d] else 0)})"
            for d, s in f["dom_stranded"].items()) + f"; create asks for confirmation: {ask or 'no'}")
        print(f"   reprotectable: without the first largest by size ({first_by_size}) {fmt(f['by_cap'][first_by_size])}; "
              f"without the largest by capacity ({largest}) {fmt(worst)} = the minimum over domains; "
              f"survivors fit the width: {fits}; copies {pool.copies}")


def c6_in_blocks():
    """The create prompt on C6 in whole 4 KiB blocks, balanced floored."""
    blk = lambda size: int(size) // 4096 * 9 // 10
    cA, cB, cC = blk(4e12), blk(1e12), blk(1e12)
    B = (cB + cC) // (2 - 1)
    stranded = cA - B
    print(f"   C6 in blocks: c_A {cA:,}, balanced {B:,}, A strands {stranded:,}; "
          f"2 x stranded > c_A: {2 * stranded > cA} (2 x stranded - c_A = {2 * stranded - cA} blocks)")


def held_layout(disks, pool, H):
    """Own raw data per disk once the pool holds H at its balanced proportions."""
    f = figures(disks, pool)
    own = []
    for d, size, other in disks:
        dom_load = min(f["cap"][d], pool.a * f["B"]) * H / f["B"]
        disk_room = max(0.0, LIMIT * size - other)
        own.append(dom_load * disk_room / f["room"][d] if f["room"][d] else 0.0)
    return own


def run_S_tail():
    c6_in_blocks()


def run_R():
    print("== R: balanced, reprotectable and stranded as the pool fills at balanced proportions, C2 (rep3, 4, 4, 2, 1 TB)")
    _, pool, disks, _ = SCENARIOS["C2"]
    for H in (0.0, 0.5 * TB, 0.8 * TB, 0.9 * TB, 1.0 * TB, 2.0 * TB, 2.5 * TB):
        own = held_layout(disks, pool, H)
        f = figures(disks, pool, own)
        room = f["room"]
        big = max(room, key=lambda d: room[d])
        room_based = balanced([r for d, r in room.items() if d != big], pool)
        cap_based = min(f["by_cap"].values())
        print(f"   holding {fmt(H)}: balanced over free room {fmt(balanced(list(room.values()), pool))}, "
              f"over capacity {fmt(f['B'])}")
        print(f"      reprotect: room-based (balanced over free room without {big}) {fmt(room_based)} -> "
              f"{'CannotReprotect' if H > room_based else 'ok'}; capacity-based {fmt(cap_based)} -> "
              f"{'CannotReprotect' if H > cap_based else 'ok'}; stranded b x s / capacity {pct(f['f_raw'])}, "
              f"b x s / remaining room {pct(pool.b * f['stranded'] / sum(room.values()) if sum(room.values()) else 1)}")


ORDINARY = [
    ("rep3, three equal 2 TB nodes", rep(3), [2, 2, 2]),
    ("rep3, 2, 2 and 1.92 TB (vendor sizes)", rep(3), [2, 2, 1.92]),
    ("rep3, 2, 2 and 1.8 TB", rep(3), [2, 2, 1.8]),
    ("rep3, one node upgraded: 2, 2, 4 TB", rep(3), [2, 2, 4]),
    ("rep3, one node smaller: 2, 2, 1 TB", rep(3), [2, 2, 1]),
    ("rep3, four nodes 2, 2, 2, 1 TB", rep(3), [2, 2, 2, 1]),
    ("rep3, five nodes 4, 2, 2, 2, 1 TB", rep(3), [4, 2, 2, 2, 1]),
    ("rep2, two nodes 2 and 1.92 TB", rep(2), [2, 1.92]),
    ("rep2, two nodes 4 and 2 TB", rep(2), [4, 2]),
    ("rep2, three nodes 2, 2, 1 TB", rep(2), [2, 2, 1]),
    ("rep2, four nodes 4, 1, 1, 1 TB", rep(2), [4, 1, 1, 1]),
    ("ec2-1, three nodes 4, 4, 2 TB", ec(2, 1, 1), [4, 4, 2]),
]


def run_O():
    print("== O: stranded fraction (b x stranded / capacity) on ordinary layouts; Stranded fires above 10%")
    for title, pool, sizes in ORDINARY:
        disks = [(chr(65 + i), s * TB, 0) for i, s in enumerate(sizes)]
        f = figures(disks, pool)
        worst_dom = max(f["dom_stranded"], key=lambda d: f["dom_stranded"][d] / f["cap"][d])
        frac = f["dom_stranded"][worst_dom] / f["cap"][worst_dom]
        print(f"   {title:42s} stranded {pct(f['f_raw']):>6s} -> {'Stranded' if f['f_raw'] > 0.10 else 'quiet':8s}; "
              f"worst domain {worst_dom} strands {pct(frac)}{' -> create asks' if frac > 0.5 else ''}")


def place(disks, pool, n_pgs, rng, start=None, margin=MARGIN, any_above=False, floor=0, own=None,
          by_capacity=True):
    """Capped-target placement, ported from spikes/capacity-model/model.py (mode 'capped').

    start: a previous table (member disk indices per PG) to balance from instead of refilling.
    any_above: while some disk is outside the margin, move from the disk furthest above its
    target even if that disk is inside the margin (the spike moves only from disks above it).
    floor: a disk's margin is max(margin, floor members / its target).
    own: this pool's raw bytes per disk. by_capacity: targets from write_limit less other pools'
    data (capacity) rather than less every pool's data, this one's included (room)."""
    n = len(disks)
    own = own or [0.0] * n
    dom = [d for d, _, _ in disks]
    cap = [max(0.0, LIMIT * s - o) for _, s, o in disks]
    room = [max(0.0, c - w) for c, w in zip(cap, own)]
    weight = cap if by_capacity else room
    names = sorted(set(dom))
    dom_w = [sum(weight[i] for i in range(n) if dom[i] == d) for d in names]
    f = water_fill(dom_w, pool.q / pool.width)
    tgt = [0.0] * n
    for k, d in enumerate(names):
        idx = [i for i in range(n) if dom[i] == d]
        tot = sum(weight[i] for i in idx)
        for i in idx:
            tgt[i] = f[k] * weight[i] / tot * n_pgs * pool.width if tot else 0.0
    mg = [max(margin, floor / t) if t > 0 else 0.0 for t in tgt]
    members = [list(m) for m in start] if start else [[] for _ in range(n_pgs)]
    load = [0] * n
    for m in members:
        for d in m:
            load[d] += 1

    def eligible(pg, d, leaving=None):
        if d in members[pg]:
            return False
        return sum(1 for x in members[pg] if x != leaving and dom[x] == dom[d]) < pool.q

    for pg in range(n_pgs):
        while len(members[pg]) < pool.width:
            cands = [d for d in range(n) if tgt[d] > 0 and eligible(pg, d)]
            d = min(cands, key=lambda d: ((load[d] + 1) / tgt[d], rng.random()))
            members[pg].append(d)
            load[d] += 1
    on = [set() for _ in range(n)]
    for pg, m in enumerate(members):
        for d in m:
            on[d].add(pg)
    converged = False
    moves = 0
    for _ in range(n_pgs * pool.width * 4):
        ratio = [load[i] / tgt[i] if tgt[i] > 0 else 0.0 for i in range(n)]
        if all(abs(ratio[i] - 1) <= mg[i] + 1e-12 for i in range(n) if tgt[i] > 0):
            converged = True
            break
        moved = False
        for src in sorted(range(n), key=lambda i: -ratio[i]):
            if ratio[src] <= (1 if any_above else 1 + mg[src]):
                break
            best = None
            for pg in sorted(on[src]):
                for d in range(n):
                    if tgt[d] <= 0 or not eligible(pg, d, leaving=src):
                        continue
                    after = (load[d] + 1) / tgt[d]
                    if after < ratio[src] - 1 / tgt[src] + 1e-9 and (best is None or after < best[0]):
                        best = (after, pg, d)
            if best:
                _, pg, d = best
                members[pg][members[pg].index(src)] = d
                on[src].discard(pg)
                on[d].add(pg)
                load[src] -= 1
                load[d] += 1
                moved = True
                moves += 1
                break
        if not moved:
            break
    share = [load[i] * pool.slot_weight / n_pgs for i in range(n)]
    avail = min(room[i] / share[i] for i in range(n) if share[i] > 0)
    bound = max(mg[i] for i in range(n) if tgt[i] > 0)
    return avail, converged, members, moves, bound


def water_fill(w, cap):
    capped = [False] * len(w)
    while True:
        rest = sum(x for x, c in zip(w, capped) if not c)
        lam = (1 - cap * sum(capped)) / rest if rest > 0 else 0.0
        f = [cap if c else lam * x for x, c in zip(w, capped)]
        over = [not c and v > cap + 1e-12 for v, c in zip(f, capped)]
        if not any(over):
            return f
        capped = [c or o for c, o in zip(capped, over)]


def pg_count_for(n_disks, width):
    """The design's rule: around 100 members per disk, a power of two."""
    p = 1
    while p * width / n_disks < 100:
        p *= 2
    return p


def random_layout(rng, pool):
    sizes = [0.5, 1, 2, 4, 8]
    while True:
        nodes = rng.randint(3, 6)
        disks = []
        for k in range(nodes):
            for _ in range(rng.randint(1, 2)):
                s = rng.choice(sizes) * TB
                disks.append((chr(65 + k), s, rng.choice([0, 0, 0, 0.2, 0.5]) * s))
        if pool.q * nodes >= pool.width:
            return disks


def shortfall(disks, pool, avail):
    return 1 - avail / balanced(list(domain_rooms(disks).values()), pool)


def summary(name, short, stuck):
    short = sorted(short)
    q = lambda p: short[min(len(short) - 1, int(p * len(short)))]
    return (f"   {name}: shortfall median {pct(q(0.5))}, p90 {pct(q(0.9))}, p99 {pct(q(0.99))}, max {pct(short[-1])}; "
            f"over 2%: {sum(s > 0.02 for s in short)}, over 5%: {sum(s > 0.05 for s in short)} of {len(short)}; "
            f"stopped outside the margin: {stuck}")


def run_U(layouts=200):
    print("== U: available + held under balanced once capped placement stops, pg_count by the design's rule "
          "(around 100 members per disk); data follows the table, so available + held does not depend on held")
    for key, (title, pool, disks, n_pgs) in SCENARIOS.items():
        avail, conv, _, _, _ = place(disks, pool, n_pgs, random.Random(1))
        print(f"   {key} at {n_pgs} PGs, margin 5%: available {fmt(avail)}, "
              f"shortfall {pct(shortfall(disks, pool, avail))}, converged {conv}")
    variants = [(0.05, False, 0), (0.05, True, 0), (0.02, True, 0), (0.05, True, 1), (0.05, True, 2)]
    for pool in (rep(2), rep(3)):
        rng = random.Random(42)
        cases = []
        while len(cases) < layouts:
            disks = random_layout(rng, pool)
            n_pgs = pg_count_for(len(disks), pool.width)
            try:
                avail, conv, members, _, _ = place(disks, pool, n_pgs, random.Random(len(cases)))
            except ValueError:
                continue
            node = rng.choice(sorted({d for d, _, _ in disks}))
            more = disks + [(node, rng.choice([1, 2, 4, 8]) * TB, 0)]
            cases.append((disks, n_pgs, members, more, avail, conv))
        fresh = [shortfall(d, pool, a) for d, _, _, _, a, _ in cases]
        print(summary(f"{pool.name}, placed from scratch, margin 5%", fresh, sum(not c[5] for c in cases)))
        for margin, any_above, floor in variants:
            short, stuck, moves, broken = [], 0, 0, 0
            for k, (_, n_pgs, members, more, _, _) in enumerate(cases):
                avail, conv, _, mv, bound = place(more, pool, n_pgs, random.Random(k), start=members,
                                                  margin=margin, any_above=any_above, floor=floor)
                sf = shortfall(more, pool, avail)
                short.append(sf)
                stuck += not conv
                moves += mv
                broken += conv and (1 - sf) < 1 / (1 + bound) - 1e-9
            rule = "from any disk above target" if any_above else "only from disks above the margin"
            fl = f"max({margin:.0%}, {floor} member{'s' if floor > 1 else ''})" if floor else f"{margin:.0%}"
            print(summary(f"{pool.name}, a disk added, margin {fl}, {rule}, {moves / layouts:.0f} moves per add",
                          short, stuck) + f"; converged below balanced / (1 + margin): {broken}")
            if any_above and margin == 0.05 and floor in (0, 1):
                for h in (0.5, 0.7, 0.9):
                    old = sum((1 - s) - h < 0.9 * (1 - h) for s in short)
                    new = sum(s > 0.10 for s in short)
                    print(f"      held at {h:.0%} of balanced: Unbalanced by available < 0.9 (balanced - held) "
                          f"{old} of {layouts}; by available + held < 0.9 balanced {new} of {layouts}")


def run_T():
    print("== T: member moves after a disk is added to a pool holding data, targets by room against by capacity "
          "(recomputed after each round's data has moved, until no move)")
    cases = [
        ("none, one 2 TB node holding 1 TB, a 2 TB node added", none(), [("A", 2 * TB, 0)], 1 * TB, ("B", 2 * TB)),
        ("C6 holding 0.8 TB, a 2 TB disk added to B", rep(2), SCENARIOS["C6"][2], 0.8 * TB, ("B", 2 * TB)),
        ("C2 holding 1.5 TB, a 4 TB disk added to D", rep(3), SCENARIOS["C2"][2], 1.5 * TB, ("D", 4 * TB)),
    ]
    for title, pool, disks, H, (node, size) in cases:
        n_pgs = 128
        _, _, members, _, _ = place(disks, pool, n_pgs, random.Random(1))
        more = list(disks) + [(node, size, 0)]
        out = []
        for by_capacity in (False, True):
            m = [list(x) for x in members]
            total, rounds = 0, 0
            for rounds in range(1, 11):
                load = [0] * len(more)
                for pg in m:
                    for d in pg:
                        load[d] += 1
                own = [load[i] * pool.slot_weight / n_pgs * H for i in range(len(more))]
                _, _, m2, mv, _ = place(more, pool, n_pgs, random.Random(rounds), start=m, any_above=True,
                                        own=own, by_capacity=by_capacity)
                m = m2
                total += mv
                if mv == 0:
                    break
            out.append(f"{'capacity' if by_capacity else 'room'}: {total} moves over {rounds} recomputations")
        print(f"   {title}: " + "; ".join(out))
    _, pool, disks, n = SCENARIOS["C2"]
    _, _, m, _, _ = place(disks, pool, n, random.Random(1))
    more = list(disks) + [("D", 4 * TB, 0)]
    avail, conv, _, _, _ = place(more, pool, n, random.Random(2), start=m, any_above=True, floor=1)
    print(f"   C2 with a 4 TB disk added to D, margin max(5%, 1 member): converged {conv}, "
          f"1 - (available + held) / balanced = {pct(shortfall(more, pool, avail))} at any held")


def balanced_disks(disks, pool):
    """Balanced with each disk also capped at one member per PG: the largest D with
    sum over domains of min(a D, sum over its disks of min(room, slot_weight D)) >= b D."""
    per = {}
    for d, size, other in disks:
        per.setdefault(d, []).append(max(0.0, LIMIT * size - other))
    if sum(min(pool.q, len(r)) for r in per.values()) < pool.width:
        return 0.0
    g = lambda D: sum(min(pool.a * D, sum(min(x, pool.slot_weight * D) for x in r)) for r in per.values()) - pool.b * D
    lo, hi = 0.0, sum(sum(r) for r in per.values())
    for _ in range(200):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if g(mid) >= 0 else (lo, mid)
    return lo


def run_E():
    print("== E: shards with max_per_domain 2 on nodes with fewer than 2 disks")
    pool = ec(4, 2, 2)
    for title, sizes in (("three nodes, 2x2, 2x2 and 1x4 TB", [[2, 2], [2, 2], [4]]),
                         ("four nodes, 2x2, 2x2, 2x2 and 1x4 TB", [[2, 2], [2, 2], [2, 2], [4]]),
                         ("C3: three nodes, 2x2, 2x2 and 2x0.5 TB", [[2, 2], [2, 2], [0.5, 0.5]])):
        disks = [(chr(65 + k), s * TB, 0) for k, node in enumerate(sizes) for s in node]
        nodes = len(sizes)
        print(f"   {pool.name}, {title}: width fits by the design's rule (6 <= {nodes} x 2): {6 <= nodes * 2}; "
              f"counting min(max_per_domain, disks): {sum(min(2, len(n)) for n in sizes) >= 6}; "
              f"balanced by the design's formula {fmt(balanced(list(domain_rooms(disks).values()), pool))}, "
              f"with each disk capped {fmt(balanced_disks(disks, pool))}")


def without(disks, dom):
    return [x for x in disks if x[0] != dom]


def domain_caps(disks):
    c = {}
    for d, size, other in disks:
        c[d] = c.get(d, 0.0) + max(0.0, LIMIT * size - other)
    return c


def run_E2(trials=3000):
    print("== E2: with each disk capped, is removing the largest-capacity domain still the worst case? (ec4-2 at 2 per node)")
    pool = ec(4, 2, 2)
    disks = [("A", 4 * TB, 0)] + [(d, 1.5 * TB, 0) for d in "BCD" for _ in range(2)]
    c = domain_caps(disks)
    print("   A one 4 TB disk, B, C, D two 1.5 TB each: " + ", ".join(
        f"without {d} (capacity {fmt(c[d])}) {fmt(balanced_disks(without(disks, d), pool))}" for d in sorted(c)))
    rng = random.Random(5)
    bad = 0
    for _ in range(trials):
        disks = [(chr(65 + k), rng.choice([0.5, 1, 1.5, 2, 4, 8]) * TB, 0)
                 for k in range(rng.randint(4, 6)) for _ in range(rng.randint(1, 3))]
        c = domain_caps(disks)
        largest = max(sorted(c), key=lambda d: c[d])
        worst = min(balanced_disks(without(disks, d), pool) for d in c)
        bad += balanced_disks(without(disks, largest), pool) > worst + 1e-6 * TB
    print(f"   random layouts of 4 to 6 nodes with 1 to 3 disks: largest-capacity domain not the worst in {bad} of {trials}")


def run_D():
    print("== D: layouts without a spare domain, losing one disk (rep3)")
    pool = rep(3)
    for title, sizes in (("three nodes of 2 x 2 TB", [[2, 2], [2, 2], [2, 2]]),
                         ("three nodes of 2 x 2, 2 x 2 and 4 + 1 TB", [[2, 2], [2, 2], [4, 1]]),
                         ("three nodes of one 4 TB disk", [[4], [4], [4]])):
        caps = {chr(65 + k): [LIMIT * s * TB for s in node] for k, node in enumerate(sizes)}
        c = {d: sum(v) for d, v in caps.items()}
        B = balanced(list(c.values()), pool)
        worst, which = math.inf, None
        for d, v in caps.items():
            if len(v) < 2:
                continue
            for k, x in enumerate(v):
                r = balanced([c[e] - (x if e == d else 0) for e in c], pool)
                if r < worst:
                    worst, which = r, f"{d}'s {fmt(x / LIMIT)} disk"
        print(f"   {title}: balanced {fmt(B)}; spare domain: {len(c) - 1 >= pool.width}; "
              + (f"reprotectable after losing {which}: {fmt(worst)}" if which else "no node has a second disk: nothing to check"))


def self_check():
    rng = random.Random(3)
    for _ in range(300):
        pool = rng.choice([rep(2), rep(3), ec(4, 2, 2), ec(2, 1, 1), none()])
        c = [rng.uniform(0, 4) for _ in range(rng.randint(2, 8))]
        x, y = balanced(c, pool), check_balanced(c, pool)
        assert abs(x - y) <= 1e-6 * max(1, x), (pool.name, c, x, y)
        g = sum(min(v, pool.a * x) for v in c) - pool.b * x
        assert x == 0 or abs(g) < 1e-6 * sum(c), "at balance the domain loads sum to b x balanced"
        worst = min(balanced([v for j, v in enumerate(c) if j != i], pool) for i in range(len(c)))
        largest = c.index(max(c))
        assert abs(balanced([v for j, v in enumerate(c) if j != largest], pool) - worst) < 1e-9
        if pool.q == 1 and len(c) - 1 >= pool.width:
            k = rng.randrange(len(c))
            part = rng.uniform(0, c[k])
            lost_disk = balanced([v - (part if j == k else 0) for j, v in enumerate(c)], pool)
            assert worst <= lost_disk + 1e-9, "losing a domain dominates losing one of its disks"


def main(argv):
    self_check()
    print("conditions model; python", sys.version.split()[0], "; self-check passed")
    for t in argv or ["S", "R", "O", "E", "E2", "D", "T", "U"]:
        {"S": lambda: (run_S(), run_S_tail()), "R": run_R, "O": run_O, "E": run_E, "E2": run_E2, "D": run_D, "T": run_T,
         "U": run_U}[t]()


if __name__ == "__main__":
    main(sys.argv[1:])
