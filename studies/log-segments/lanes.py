#!/usr/bin/env python3
"""Which segments must recovery read when merges fill metadata segments of their own?

A model of the log, not of the code: rounds take global LSNs; the hot, cold and shared metadata
lanes each fill one segment at a time, and each merge worker fills a lane of its own, written only
while a checkpoint streams its step. The run stops at a random round, as at a crash with nothing
in flight. Each rule picks the segments recovery reads; a rule misses if some record past the
newest manifest's replay point lies in a segment it does not read. Reads are counted in blocks,
from where the rule starts in a segment to the segment's end, since replay scans the unused tail
of every segment it reads.

    python3 -I lanes.py > results.txt
"""
import random

SEG = 64                 # blocks per segment
MAX_REPLAY = 256         # record blocks after a manifest before a checkpoint is due
STEP = 96                # blocks of merge output per step
LAG = 40                 # how many records an index's replay point may trail the base


class Log:
    def __init__(self, workers, rng):
        self.rng = rng
        self.lanes = ["hot", "cold", "meta"] + [f"merge{w}" for w in range(1, workers + 1)]
        self.segs = []            # dicts: lane, seq, records [(lsn, start, blocks)], used
        self.open = {}
        self.lsn = 0
        self.seq = 0
        self.manifest = None      # (lsn, base, replay)
        self.since = 0
        self.merging = set()

    def append(self, lane, blocks):
        s = self.open.get(lane)
        if s is None or s["used"] + blocks > SEG - 1:
            self.seq += 1
            s = {"lane": lane, "seq": self.seq, "records": [], "used": 0}
            self.segs.append(s)
            self.open[lane] = s
        self.lsn += 1
        s["records"].append((self.lsn, 1 + s["used"], blocks))
        s["used"] += blocks
        return self.lsn

    def traffic(self):
        lane = "hot" if self.rng.random() < 0.8 else "cold"
        self.append(lane, self.rng.randint(1, 8))
        self.since += 1

    def checkpoint(self, crash_at):
        base = self.lsn
        cut = {"next_seq": self.seq + 1, "open": {id(s): 1 + s["used"] for s in self.open.values()},
               "newest": {id(s): s["records"][-1][0] for s in self.segs}}
        replay = max(0, base - self.rng.randint(0, LAG))
        for lane in self.lanes[3:]:
            if lane in self.merging and self.rng.random() < 0.3:
                self.merging.discard(lane)
            elif lane not in self.merging and self.rng.random() < 0.5:
                self.merging.add(lane)
        work = [("meta", self.rng.randint(1, 8)) for _ in range(self.rng.randint(2, 12))]
        for lane in sorted(self.merging):
            left = STEP
            while left > 0:
                b = min(left, 8)
                work.append((lane, b))
                left -= b
        self.rng.shuffle(work)
        for lane, b in work:
            if self.lsn >= crash_at:
                return
            self.append(lane, b)
            if self.rng.random() < 0.5:
                self.traffic()
        if self.lsn >= crash_at:
            return
        self.manifest = (self.append("meta", 1), base, replay, cut)
        self.since = 0

    def run(self, rounds):
        while self.lsn < rounds:
            if self.since * 4 >= MAX_REPLAY:
                self.checkpoint(rounds)
            else:
                self.traffic()


def first(s):
    return s["records"][0][0]


def chain_rule(segs, replay, key):
    """design.md today: per group, read from the last segment whose first record is at or before
    the first record to replay, and every later segment of the group."""
    picks = {}
    groups = {}
    for s in segs:
        groups.setdefault(key(s), []).append(s)
    for g in groups.values():
        g.sort(key=lambda s: s["seq"])
        start = 0
        for i, s in enumerate(g):
            if first(s) <= replay + 1:
                start = i
        for s in g[start:]:
            picks[id(s)] = 1
    return picks


def table_rule(segs, replay, cut):
    """Segments opened after the base in full; listed segments whose newest record is past the
    replay point from block 1; segments open at the base from where the base left them."""
    picks = {}
    for s in segs:
        k = id(s)
        if s["seq"] >= cut["next_seq"]:
            picks[k] = 1
        elif cut["newest"][k] > replay:
            picks[k] = 1
        elif k in cut["open"]:
            picks[k] = cut["open"][k]
    return picks


def stream_of(s):
    return "meta" if s["lane"].startswith("merge") else s["lane"]


def evaluate(log):
    if log.manifest is None:
        return None
    _, base, replay, cut = log.manifest
    segs = log.segs
    oracle = {}
    for s in segs:
        past = [r for r in s["records"] if r[0] > replay]
        if past:
            oracle[id(s)] = past[0][1]
    rules = {
        "per stream (design.md now)": chain_rule(segs, replay, stream_of),
        "per lane (lane byte in header)": chain_rule(segs, replay, lambda s: s["lane"]),
        "per lane, merge lanes skipped": {k: v for k, v in chain_rule(segs, replay, lambda s: s["lane"]).items()
                                         if not next(s for s in segs if id(s) == k)["lane"].startswith("merge")},
        "table, open list and N in manifest": table_rule(segs, replay, cut),
        "fewest blocks (oracle)": oracle,
    }
    out = {}
    for name, picks in rules.items():
        missed = sum(1 for k in oracle if k not in picks)
        blocks = sum(SEG - start for start in picks.values())
        out[name] = (missed, blocks)
    return out


def main():
    print(__doc__.strip().splitlines()[0])
    print(f"segment {SEG} blocks, checkpoint every {MAX_REPLAY} record blocks, step {STEP} blocks, replay lag up to {LAG} records")
    for workers in (0, 1, 2, 4):
        totals = {}
        runs = 0
        for seed in range(1, 2001):
            rng = random.Random(seed * 10 + workers)
            log = Log(workers, rng)
            log.run(rng.randint(300, 3000))
            res = evaluate(log)
            if res is None:
                continue
            runs += 1
            oracle_blocks = res["fewest blocks (oracle)"][1]
            for name, (missed, blocks) in res.items():
                t = totals.setdefault(name, [0, 0, 0.0])
                t[0] += missed > 0
                t[1] = max(t[1], (blocks - oracle_blocks) / SEG)
                t[2] += (blocks - oracle_blocks) / SEG
        print(f"\nmerge workers {workers}, {runs} crashes after a manifest")
        print(f"  {'rule':34} {'runs missing a record':>22} {'extra segments read, mean':>26} {'max':>5}")
        for name, (miss, worst, total) in totals.items():
            extra = f"{total / runs:>26.2f} {worst:>5.2f}" if miss == 0 else f"{'n/a':>26} {'n/a':>5}"
            print(f"  {name:34} {miss:>22} {extra}")


if __name__ == "__main__":
    main()
