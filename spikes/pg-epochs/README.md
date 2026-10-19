# PG epochs spike

**Question.** How fast do placement-table epochs and per-PG intervals advance on a homelab cluster, under realistic churn and under a flapping node or disk, how many in-flight writes does each table change disturb when a member checks a request's epoch globally rather than per PG, and how long does each width of the version's two fields last? The [pg-epochs study](../../studies/pg-epochs/README.md) uses the answer to choose the version's bit layout.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker, shared; nothing is timed), Python 3.13.16, no root, no I/O. One model, [model.py](model.py):

- **Placement** builds a `rep3` table of 2,048 PGs as [the placement table](../../docs/design.md#data-layer-chunkstore) describes it: refill each missing member onto the least-full eligible disk, then, while a disk is outside a 5% margin of its target, move one member from the fullest disk to the emptiest eligible one, one member of a PG per node. Targets are capped by the failure-domain rule, as the [capacity model spike](../capacity-model/README.md) found they must be. Two clusters: 3 nodes of 2 disks and 10 nodes of 3 disks, disks of 1, 2 and 4 TB, each node the same total.
- **A table change** is one global epoch. A PG's **interval** changes when its ordered member list or a member's live mark changes. Two staging policies bracket what the design allows ("moves can be staged and throttled"): every member a step moves in one table (`stage all`), or one member per table (`stage 1`). A moved member costs one interval change (replaced in place, the new member recovering while it serves) or two (added, then the old one removed once the new one has the data, as Ceph's `pg_temp`).
- **Y, a year of events**, in random order, three seeds: 24 reboots per node (kernel and k3s updates), 6 short node outages, 2 outages long enough to refill and rebalance back, 2 disk failures refilled and replaced, 2 disk adds, 6 fail-slow marks and returns, 6 pool changes that move nothing, 4 counter-exhaustion bumps of one PG, and a re-peer bump of the PGs a node leads each time it restarts (the study proposes it).
- **F, one flap cycle** of one node: marked down, then up; with refill, its members move away and are balanced back.
- **X** turns rates into years until a field of 24, 28 or 32 bits runs out, for a field that counts table epochs (`g`) and one that counts the busiest PG's intervals (`pg`). Flapping is sustained: undamped, a cycle every 10 s (down after `node_down_after`, 5 s, up on the next heartbeat); damped, 5 cycles per 10 minutes (Ceph's `osd_max_markdown_count` in `osd_max_markdown_period`); a fail-slow disk, a cycle every 2 minutes (`slow_disk_window` slow, then a window normal).
- **C** is the arithmetic for the counter field: time until 32, 36, 40 or 48 bits run out at a sustained rate per chunk or per primary.
- **B** is the bytes a year of tables costs pushed whole to every node, against deltas of the PG entries that changed.

**retry** is the mean fraction of PGs whose interval a table changed, which is the fraction of in-flight writes (spread evenly over PGs) that a table change makes retry under a per-PG check; under the global check it is 1.

```
./run.sh     # python3 model.py -> results.txt, about 2 minutes
```

Raw output: [results.txt](results.txt).

## Results

| Test | Result |
| --- | --- |
| Y1: 3 nodes, a year, `stage all` | 270 tables, the busiest PG 208 interval changes; **retry 0.71 to 0.74**: on 3 nodes every PG has a member on every node, so each node event changes every PG |
| Y2: 3 nodes, a year, `stage 1` | 4,030 to 6,506 tables (7,796 to 12,748 with two tables per move), busiest PG 196 to 213; retry 0.016 to 0.049 |
| Y3: 10 nodes, a year, `stage all` | 778 tables, busiest PG 228 to 233; **retry 0.225** |
| Y4: 10 nodes, a year, `stage 1` | 4,277 to 4,497 tables (7,786 to 8,226 with two per move), busiest PG 228 to 237; **retry 0.022 to 0.041** |
| Y, writes disturbed | Under a per-PG check a year disturbs Σ changes ÷ PGs, 174 to 177 times all writes in flight on 10 nodes and 191 to 203 on 3 nodes, whatever the staging; under the global check it is the table count: **4.4× more on 10 nodes with tables per step (1.35× on 3 nodes), 24 to 46× more with tables per move (20 to 63× on 3 nodes)** |
| F1: node flap, no refill | 2 tables; every PG with a member on the node changes twice: all 2,048 on 3 nodes (retry 1.0), 615 of 2,048 on 10 nodes (retry 0.30) |
| F2: node flap, refill and balance back, 10 nodes | `stage all`: 4 tables, busiest PG 3 changes. **`stage 1`: 1,202 tables per cycle** (2,402 with two per move), busiest PG 3 (5) |
| X1: realistic year | Global field: g24 lasts 1,316 to 62,138 years, depending on staging. Per-PG field: pg24 70,790 years or more |
| X2: undamped node flap, no refill | 6.3 million a year on both fields: **g24 and pg24 run out in 2.7 years**, pg28 in 43, pg32 in 680 |
| X3: undamped node flap, staged refill and balance back | **Global: 7.6 billion tables a year, g24 runs out in 19 hours, g32 in 207 days.** Busiest PG: 15.8 million a year, pg24 in 1.1 years, pg28 in 17, pg32 in 272 |
| X4: damped flap (5 per 10 min), staged refill | Global: 316 to 632 million a year, g24 in 9.7 to 19 days, g32 in 6.8 to 14 years. Busiest PG: 0.8 to 1.3 million, pg24 in 13 to 21 years, pg32 in 3,266 or more |
| X5: fail-slow disk flapping at its limit | 526,000 a year, 24 bits in 32 years on either field |
| C1: counter field | 32 bits: **1.2 h at 1 M writes/s**, 4.0 h at 300,000, 8.0 h at 150,000, 2.5 days at 20,000, 50 days at 1,000. 40 bits: 12.7 days, 42 days, 85 days, 1.7 years, 35 years |
| B1: table bytes, 10 nodes | Whole tables of 51 KB: 399 MB a year with `stage all`, 2.25 GB with `stage 1`; deltas: 104 to 105 MB either way. On 3 nodes 41.5 MB against 34.9 MB, since node events change every PG |

## Conclusions

1. **A field that holds global table epochs can run out within the cluster's life.** Realistic churn burns 778 to 12,748 epochs a year, which 24 bits survive for over a thousand years, but one node flapping undamped with its members refilled one per table burns 7.6 billion a year: 24 bits last 19 hours and 32 bits 207 days (X3). The burn rate depends on staging, which the design leaves open.
2. **A per-PG interval count grows only with that PG's own changes**: 196 to 237 a year for the busiest PG whatever the staging (Y), and at most a few per flap cycle (F2). 32 bits outlast an undamped flapping node for 272 years in the worst staging (X3); 24 bits only 1.1 to 2.7 years undamped and 13 to 32 years damped (X2 to X5).
3. **Per-PG checks disturb far fewer in-flight writes**, 4.4× fewer on 10 nodes with one table per step and 24 to 46× with one per move, and make staging free for writes outside the moved PGs (Y). **On 3 nodes they save almost nothing for node events**, which change every PG (Y1, F1); they still isolate disk events, counter bumps, restarts and moves.
4. **A 32-bit counter per chunk runs out at most every 1.2 hours** at the loop's upper bound of 1 M writes/s to one chunk, 4 hours at a CPU-bound loop's 300,000, and every 50 days at a busy database's 1,000 (C1). A per-PG interval bump costs one table entry and one re-peer of that PG, so that rate is harmless; under the global check each bump would disturb every write in the cluster.
5. **Deltas cut the table bytes pushed by 4 to 21× on 10 nodes** (B1), the larger factor when moves are staged; on 3 nodes they save 16%, since node events change every PG. Either way the volume is small at homelab scale (2.25 GB a year at worst); the gain is the burst, 61 MB per node per staged flap cycle against about 35 KB.

Not covered: erasure-coded pools (a wider PG has a member on more nodes, so node events change more of its PGs), real recovery times (the model counts tables, not seconds), and the activation reports that the study folds into tables, which add table epochs but change no interval.
