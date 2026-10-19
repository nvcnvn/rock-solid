"""Epoch and interval budget model for the pg-epochs study.

Builds a placement table the way docs/design.md describes it (refill each missing member onto the
least-full eligible disk, then move one member at a time from the fullest disk to the emptiest
eligible one while a disk is outside a 5% margin of its share), applies a year of cluster events
or one cycle of a flapping fault, and counts:

  tables     global table epochs committed
  changes    PG interval changes: a PG's ordered member list or a member's live mark changed
  max/PG     the most interval changes any one PG saw
  retry      mean fraction of PGs whose interval changed per table, which is the fraction of
             in-flight writes a table change makes retry under per-PG checks (1.0 under the
             global check)

Run: python3 model.py   (no I/O, no root, about 2 minutes)
"""

import random
import sys

WIDTH = 3
MARGIN = 0.05
SECONDS_PER_YEAR = 365.25 * 86400


class Cluster:
    def __init__(self, nodes, disks_per_node, pg_count, seed, stage, move_tables):
        self.rng = random.Random(seed)
        self.stage = stage  # members moved per table; 0 means all of a step in one table
        self.move_tables = move_tables  # 1: replace in place; 2: add, then remove once recovered
        self.disks = []  # (node, capacity)
        sizes = [1, 2, 4][:disks_per_node]
        for n in range(nodes):
            for d in range(disks_per_node):
                self.disks.append((n, sizes[(n + d) % len(sizes)]))
        self.up = set(range(len(self.disks)))
        self.down_marked = set()
        self.pgs = [[] for _ in range(pg_count)]
        self.interval = [1] * pg_count
        self.reset_stats()
        for p in range(pg_count):
            while len(self.pgs[p]) < WIDTH:
                self.pgs[p].append(self.least_full_eligible(p))
        self.balance(count=False)
        self.reset_stats()

    def reset_stats(self):
        self.tables = 0
        self.changed_per_table = []
        self.changes = [0] * len(self.pgs)

    def counts(self):
        c = [0] * len(self.disks)
        for members in self.pgs:
            for d in members:
                c[d] += 1
        return c

    def shares(self):
        """Each live disk's target member count: capacity-weighted, with each node capped at one
        member per PG and the excess water-filled over the other nodes (the capacity model
        spike's capped targets)."""
        live = [d for d in self.up if d not in self.down_marked]
        node_cap = {}
        for d in live:
            node_cap[self.disks[d][0]] = node_cap.get(self.disks[d][0], 0) + self.disks[d][1]
        total = len(self.pgs) * WIDTH
        target, free = {}, dict(node_cap)
        while free:
            cap = sum(free.values())
            over = [n for n in free if total * free[n] / cap > len(self.pgs)]
            if not over:
                for n in free:
                    target[n] = total * free[n] / cap
                break
            for n in over:
                target[n] = len(self.pgs)
                total -= len(self.pgs)
                del free[n]
        return {d: max(target[self.disks[d][0]] * self.disks[d][1] / node_cap[self.disks[d][0]], 1e-9)
                for d in live}

    def eligible(self, p, d, leaving=None):
        if d not in self.up or d in self.down_marked:
            return False
        nodes = {self.disks[m][0] for m in self.pgs[p] if m != leaving}
        return self.disks[d][0] not in nodes and d not in self.pgs[p]

    def least_full_eligible(self, p, leaving=None):
        c = self.counts()
        sh = self.shares()
        best = None
        for d in sh:
            if not self.eligible(p, d, leaving):
                continue
            f = c[d] / sh[d]
            if best is None or f < best[0]:
                best = (f, d)
        return None if best is None else best[1]

    def commit(self, pgs_changed):
        """One table: every PG in pgs_changed gets a new interval."""
        self.tables += 1
        for p in pgs_changed:
            self.interval[p] += 1
            self.changes[p] += 1
        self.changed_per_table.append(len(pgs_changed))

    def commit_moves(self, moves):
        """moves: list of PGs each moving one member; staged as the cluster's policy says."""
        if not moves:
            return
        step = len(moves) if self.stage == 0 else self.stage
        for i in range(0, len(moves), step):
            batch = moves[i:i + step]
            for _ in range(self.move_tables):
                self.commit(batch)

    def mark(self, disk_ids, down):
        """One table marking disks down or up; every PG with a member on them changes interval."""
        if down:
            self.down_marked |= set(disk_ids)
        else:
            self.down_marked -= set(disk_ids)
        affected = [p for p, m in enumerate(self.pgs) if any(d in disk_ids for d in m)]
        self.commit(affected)

    def refill(self):
        moves = []
        for p, members in enumerate(self.pgs):
            for i, d in enumerate(members):
                if d in self.down_marked or d not in self.up:
                    to = self.least_full_eligible(p, leaving=d)
                    if to is not None:  # no other failure domain: the PG stays degraded
                        members[i] = to
                        moves.append(p)
        self.commit_moves(moves)

    def balance(self, count=True):
        moves = []
        for _ in range(20000):
            c = self.counts()
            sh = self.shares()
            fill = {d: c[d] / sh[d] for d in sh}
            hi = max(fill, key=fill.get)
            lo = min(fill, key=fill.get)
            if fill[hi] <= 1 + MARGIN and fill[lo] >= 1 - MARGIN:
                break
            moved = False
            cands = [p for p, m in enumerate(self.pgs) if hi in m]
            self.rng.shuffle(cands)
            for p in cands:
                if self.eligible(p, lo, leaving=hi):
                    self.pgs[p][self.pgs[p].index(hi)] = lo
                    moves.append(p)
                    moved = True
                    break
            if not moved:
                break
        if count:
            self.commit_moves(moves)
        return len(moves)

    def node_disks(self, n):
        return [i for i, (node, _) in enumerate(self.disks) if node == n]

    def add_disk(self, node, cap):
        self.disks.append((node, cap))
        self.up.add(len(self.disks) - 1)
        self.balance()

    def remove_disk(self, d):
        self.up.discard(d)
        self.down_marked.discard(d)

    def pgs_led_by(self, n):
        return [p for p, m in enumerate(self.pgs) if self.disks[m[0]][0] == n]

    def summary(self):
        retry = (sum(self.changed_per_table) / len(self.pgs) / self.tables) if self.tables else 0.0
        return self.tables, sum(self.changes), max(self.changes), retry


def year(c, nodes, refill_delay_hits, restart_repeer):
    """Applies one year of a homelab's events in a random order."""
    events = []
    events += ["reboot"] * (2 * 12 * nodes)  # kernel and k3s updates, twice a month per node
    events += ["outage_short"] * 6  # a node down for minutes: crash, power blip, reboot hang
    events += ["outage_long"] * refill_delay_hits  # down past the refill delay, then back
    events += ["disk_fail"] * 2  # disk dies, refilled, replaced a week later
    events += ["disk_add"] * 2
    events += ["slow_mark"] * 6  # fail-slow mark, back after a quiet slow_disk_window
    events += ["pool_change"] * 6  # pool created or a setting changed; no member moves
    events += ["exhaust"] * 4  # a chunk's counter runs out: its PG takes a new interval
    c.rng.shuffle(events)
    for e in events:
        n = c.rng.randrange(nodes)
        if e in ("reboot", "outage_short"):
            c.mark(c.node_disks(n), True)
            if restart_repeer:
                c.commit(c.pgs_led_by(n))
            c.mark(c.node_disks(n), False)
        elif e == "outage_long":
            c.mark(c.node_disks(n), True)
            c.refill()
            c.down_marked -= set(c.node_disks(n))
            c.commit([])  # node marked up: its disks are empty, no PG names them
            c.balance()
        elif e == "disk_fail":
            d = c.rng.choice(sorted(c.up))
            c.mark([d], True)
            c.refill()
            c.remove_disk(d)
            c.commit([])
            c.add_disk(c.disks[d][0], c.disks[d][1])
        elif e == "disk_add":
            c.add_disk(n, c.rng.choice([1, 2, 4]))
        elif e == "slow_mark":
            d = c.rng.choice(sorted(c.up))
            c.mark([d], True)
            c.mark([d], False)
        elif e == "pool_change":
            c.commit([])
        elif e == "exhaust":
            c.commit([c.rng.randrange(len(c.pgs))])


def flap_cycle(c, node, refill):
    """One down and up of a node; with refill, its members move away and back."""
    c.mark(c.node_disks(node), True)
    if refill:
        c.refill()
        c.down_marked -= set(c.node_disks(node))
        c.commit([])
        c.balance()
    else:
        c.mark(c.node_disks(node), False)


def years_to(bits, per_year):
    if per_year == 0:
        return float("inf")
    return (2 ** bits - 1) / per_year


def fmt_years(y):
    if y == float("inf"):
        return "never"
    if y >= 1e6:
        return f"{y:.1e} y"
    if y >= 10:
        return f"{y:,.0f} y"
    if y >= 1:
        return f"{y:.1f} y"
    if y * 365.25 >= 1:
        return f"{y * 365.25:.1f} d"
    return f"{y * 365.25 * 24:.1f} h"


def main():
    print("pg-epochs model; python", sys.version.split()[0])
    print()
    print("== Y: one year of homelab events (24 reboots per node, 6 short and 2 long outages,")
    print("   2 disk failures, 2 disk adds, 6 fail-slow marks, 6 pool changes, 4 counter bumps)")
    print("   stage: members moved per table (all = one table per step); move: tables per moved member")
    print(f"{'cluster':<22}{'stage':>6}{'move':>5}{'seed':>5}{'tables':>8}{'changes':>9}{'max/PG':>8}{'retry':>8}")
    yearly = {}
    for name, nodes, dpn, pgs in [("3 nodes x 2 disks", 3, 2, 2048), ("10 nodes x 3 disks", 10, 3, 2048)]:
        for stage, mt in [(0, 1), (1, 1), (1, 2)]:
            for seed in (1, 2, 3):
                c = Cluster(nodes, dpn, pgs, seed, stage, mt)
                year(c, nodes, refill_delay_hits=2, restart_repeer=True)
                t, ch, mx, r = c.summary()
                yearly.setdefault((name, stage, mt), []).append((t, ch, mx, r))
                print(f"{name:<22}{'all' if stage == 0 else stage:>6}{mt:>5}{seed:>5}{t:>8}{ch:>9}{mx:>8}{r:>8.3f}")
    print()

    print("== F: one flap cycle of one node (down, then up); per cycle")
    print(f"{'cluster':<22}{'refill':>7}{'stage':>6}{'move':>5}{'tables':>8}{'changes':>9}{'max/PG':>8}{'retry':>8}")
    flap = {}
    for name, nodes, dpn, pgs in [("3 nodes x 2 disks", 3, 2, 2048), ("10 nodes x 3 disks", 10, 3, 2048)]:
        for refill, stage, mt in [(False, 0, 1), (True, 0, 1), (True, 1, 1), (True, 1, 2)]:
            if nodes == 3 and refill:
                continue  # rep3 on 3 nodes has nowhere to refill
            c = Cluster(nodes, dpn, pgs, 7, stage, mt)
            flap_cycle(c, 0, refill)
            t, ch, mx, r = c.summary()
            flap[(name, refill, stage, mt)] = (t, ch, mx, r)
            print(f"{name:<22}{'yes' if refill else 'no':>7}{'all' if stage == 0 else stage:>6}{mt:>5}{t:>8}{ch:>9}{mx:>8}{r:>8.3f}")
    print()

    print("== X: years until a field of the given width runs out")
    print("   global field = table epochs; per-PG field = interval changes of the busiest PG")
    rows = []
    for (name, stage, mt), runs in yearly.items():
        t = max(r[0] for r in runs)
        mx = max(r[2] for r in runs)
        rows.append((f"Y {name}, stage {'all' if stage == 0 else stage}, move {mt}", t, mx))
    # sustained flapping: cycles per year
    undamped = SECONDS_PER_YEAR / 10  # down after 5 s without heartbeats, up on the next one
    damped = 5 * 6 * 24 * 365.25  # Ceph osd_max_markdown_count 5 per osd_max_markdown_period 10 min
    slow = SECONDS_PER_YEAR / 120  # fail-slow: marked after a slow window, back after a quiet one
    for (name, refill, stage, mt), (t, ch, mx, r) in flap.items():
        tag = f"{name}, refill {'yes' if refill else 'no'}, stage {'all' if stage == 0 else stage}, move {mt}"
        rows.append((f"F undamped 10 s, {tag}", t * undamped, mx * undamped))
        rows.append((f"F damped 5/10 min, {tag}", t * damped, mx * damped))
    rows.append(("fail-slow disk flapping, 1 cycle / 2 min, no refill", 2 * slow, 2 * slow))
    print(f"{'scenario':<70}{'tables/y':>12}{'PG/y':>12}{'g24':>11}{'g32':>11}{'pg24':>11}{'pg28':>11}{'pg32':>11}")
    for label, t, mx in rows:
        print(f"{label:<70}{t:>12,.0f}{mx:>12,.0f}{fmt_years(years_to(24, t)):>11}{fmt_years(years_to(32, t)):>11}"
              f"{fmt_years(years_to(24, mx)):>11}{fmt_years(years_to(28, mx)):>11}{fmt_years(years_to(32, mx)):>11}")
    print()

    print("== C: time until a counter of the given width runs out at a sustained write rate")
    print(f"{'writes/s':>10}  {'scope':<44}{'c32':>10}{'c36':>10}{'c40':>10}{'c48':>10}")
    for rate, scope in [
        (1_000, "one hot chunk, homelab database"),
        (20_000, "one chunk, fsync-bound at queue depth 20"),
        (150_000, "one primary disk at 10 GbE, rep3 (2 copies out)"),
        (300_000, "one primary disk, CPU-bound loop"),
        (1_000_000, "one chunk or disk, upper bound of the loops"),
    ]:
        out = [fmt_years((2 ** b - 1) / rate / SECONDS_PER_YEAR) for b in (32, 36, 40, 48)]
        print(f"{rate:>10,}  {scope:<44}" + "".join(f"{o:>10}" for o in out))
    print()

    print("== B: table bytes pushed per table change (rep3, 2048 PGs)")
    whole = 2048 * (WIDTH * 4 + 4 + 8 + 1) + 32
    delta_pg = 4 + 4 + 8 + WIDTH * 4 + 1
    print(f"   whole table: {whole:,} B (members 4 B each, interval u32, since u64, live bits)")
    print(f"   delta: 32 B header + CRC, plus {delta_pg} B per PG whose entry changed")
    for name, nodes in [("3 nodes x 2 disks", 3), ("10 nodes x 3 disks", 10)]:
        for stage, mt in [(0, 1), (1, 1)]:
            runs = yearly[(name, stage, mt)]
            t = sum(r[0] for r in runs) / len(runs)
            ch = sum(r[1] for r in runs) / len(runs)
            whole_y = t * whole * nodes
            delta_y = (t * 32 + ch * delta_pg) * nodes
            print(f"   {name}, stage {'all' if stage == 0 else stage}: {t:,.0f} tables/y,"
                  f" whole {whole_y / 1e6:,.1f} MB/y, deltas {delta_y / 1e6:,.2f} MB/y to {nodes} nodes")


if __name__ == "__main__":
    main()
