# Capacity model spike

**Question.** What can a pool promise to `statfs`, CSI and the daemons' write-back buffers when its failure domains differ in size: is the design's figure, free raw space divided by the pool's overhead, what the pool can write before a disk reaches `write_limit`, and which figure is? The [capacity and ENOSPC study](../../studies/capacity-and-enospc/README.md) uses the answer.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker, shared; nothing is timed), Python 3.13.16 with numpy 2.5.3, no root. One model, [model.py](model.py), no I/O:

- A layout is a list of disks, each with a failure domain (its node), a size and the bytes other pools already hold on it; `write_limit` is 90%.
- Placement builds an integer table of PGs the way [the placement table](../../docs/design.md#data-layer-chunkstore) describes it: refill each member onto the least-full eligible disk, then, while a disk is outside a 5% margin of its target, move one member from the fullest disk to the emptiest eligible one, keeping at most `max_per_domain` members of a PG in one domain. The loop stops when no move brings the fullest disk closer to its target, which the design text does not say. Two targets: **weighted**, each disk's size over the total (the design's "capacity-weighted share"), and **capped**, each disk's room under `write_limit` once other pools' data is subtracted, water-filled over domains so that no domain is asked for more than `max_per_domain` of a PG's width.
- Capacity figures: the design's (free raw ÷ overhead), the same to `write_limit`, Ceph's MAX AVAIL rule (`PGMap::get_rule_avail`: each disk's room ÷ its CRUSH weight share, the smallest, ÷ overhead, with weights equal to sizes), the **balanced bound** over failure domains (below), and **available from the table**: each disk's room ÷ the share of new data the table sends it (its member slots ÷ PG count, a shard counting 1/k), the smallest over the pool's disks. This is Vitastor's `max_available` (`src/cmd/cli_pool_ls.cpp`).
- **Fill:** 4 MiB chunks hashed uniformly into PGs, each member taking its copy or shard, until the first chunk one of whose member disks would pass `write_limit`; 40 fills per table.
- **B1:** one node's full 256 MiB buffer, 64 chunks in random PGs, against each disk's share. **G1:** ten such buffers against a reserve of 10 × 256 MiB × share on every disk, as one pool-wide grant per node would set aside.

The balanced bound is the largest D with Σᵢ min(cᵢ, a·D) ≥ b·D, where cᵢ is domain i's room under `write_limit`, b the pool's overhead (`replicas`, or `(data + parity) ÷ data`) and a = `max_per_domain` × the bytes one member slot holds per byte of data (1 for a copy, 1/`data` for a shard): no domain can hold more than its own room, nor more than `max_per_domain` slots of any chunk. It equals min over j of S₍>ⱼ₎ ÷ (b − j·a), with domains sorted by room, largest first, S₍>ⱼ₎ the room of all but the j largest, and j over the values with j·a < b. j = 0 is the design's figure; a larger j wins whenever the largest domains cannot take a share proportional to their room. Fractionally the bound is reached by laying the domains' loads end to end around a circle of circumference D (McNaughton's wrap-around rule), so it is exact up to PG granularity.

```
./run.sh     # python3 model.py -> results.txt, about 40 s
```

Raw output: [results.txt](results.txt).

## Results

| Test | Layout | Design figure | Ceph rule | Balanced bound | Available from table | First refusal (mean, p5) |
| --- | --- | --- | --- | --- | --- | --- |
| C1 | `rep2`, two nodes, 0.7 and 2 GB | 1.35 GB | 1.22 GB | 0.63 GB | 0.63 GB | **0.63, 0.63 GB** |
| C2 | `rep3`, four nodes, 4, 4, 2, 1 TB | 3.667 TB | 3.300 TB | 2.700 TB | 2.679 TB | 2.680, 2.671 TB |
| C3 | `ec4-2` at 2 per node, three nodes, 2×2, 2×2, 2×0.5 TB | 6.000 TB | 5.400 TB | 1.800 TB | 1.800 TB | **1.800, 1.800 TB: the design figure is 3.3× too high** |
| C4 | `none`, three 2 TB disks, one holding 1.2 TB of another pool | 4.800 TB | 1.800 TB | 4.200 TB | weighted 1.786 TB, capped 4.189 TB | weighted 1.787 TB, **capped 4.185 TB** |
| C5 | `rep3`, ten nodes, 4, 4, 4, 2, 2, 2, 1, 1, 1, 0.5 TB | 7.167 TB | 6.450 TB | 6.450 TB | 6.422 TB | 6.423, 6.416 TB |
| C6 | `rep2`, three nodes, 4, 1, 1 TB | 3.000 TB | 2.700 TB | 1.800 TB | 1.800 TB | 1.798, 1.796 TB |

| Test | Result |
| --- | --- |
| Convergence | **With weighted targets the balance loop never reaches its margin in C1, C2, C3 and C6** (worst disk 23 to 200% off target): a large domain cannot take more than one member of each PG, so the loop ends only because the model stops when no move helps. With capped targets it converges in every layout, within 2%. |
| Losing the largest domain | The balanced bound without it: C2 0.9 TB (against 2.7 TB with it), C5 5.25 TB, C6 0.9 TB; C1 and C3 0, since too few domains remain for the width. |
| B1 | One full buffer puts on its worst disk 1.08× its share at the median and 1.40× at p99 on C2's four disks; 1.37× and 2.29× (max 3.43×) on C5's ten. |
| G1 | With ten full buffers, some disk exceeds a reserve of 10 × 256 MiB × its share in 2,000 of 2,000 trials, by up to 1.46×. |

## Conclusions

1. **The design's figure, taken to `write_limit`, is the j = 0 term of the balanced bound, so it overstates whenever domains are unequal**; as the design states it, on free raw space, by 2.1× in C1, 1.36× in C2, 3.3× in C3 and 1.67× in C6. Ceph's MAX AVAIL rule computes the same j = 0 term in those four layouts, since CRUSH weights ignore the failure-domain rule, and is right only where the rule does not bind (C5) or for a table weighted by size (C4).
2. **Available from the table predicts the first refusal within 0.3%** at p5 over 40 fills in every layout, so it is the number to report: what can be written before the first disk of the pool reaches `write_limit`, given placement as it is.
3. **The balanced bound is what placement can reach**, and the table reached it within 1% in every layout once targets were capped. It is the number to admit volumes against with `overcommit` off and to compute stranded space from (the j = 0 term minus the bound).
4. **Targets must be each disk's room for this pool, capped by the failure-domain rule.** Capacity-weighted targets never converge on unequal domains, and weighting by size ignores other pools on the disk: in C4 the table then strands 2.4 TB (1.786 against 4.2 TB).
5. **A pool-wide space grant does not bound what lands on one disk**: a node's buffer lands unevenly, up to 2.29× its share on one disk at p99 and 3.43× at worst (B1), so a reserve sized by shares is overrun in every trial (G1). Pool-wide grants need a separate bound on what all of them can put on one disk, such as keeping their total within every disk's room above `write_limit`; grants per disk would bound it exactly.
