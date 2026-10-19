# Pool conditions: where `Stranded`, `CannotReprotect`, `Unbalanced`, `NearFull` and `Full` fire

**Status:** read from source (Ceph, Vitastor, CockroachDB) plus a model ([conditions.py](conditions.py), output in [results.txt](results.txt)) that ports the [capacity model spike](../../spikes/capacity-model/README.md)'s placement to plain Python and extends it. Revised after an independent review. Adopted in the design: capacity, the figures, the conditions with `Stuck` and the confirmed fraction in [Volume size and capacity](../../docs/design.md#volume-size-and-capacity), targets by capacity and the floored margin in the [placement table](../../docs/design.md#data-layer-chunkstore), and the creation rules in [Pools and redundancy](../../docs/design.md#pools-and-redundancy); the thresholds' wider calibration is an [open question](../../docs/design.md#roadmap-and-open-questions).

**Question:** the [capacity and ENOSPC study](../capacity-and-enospc/README.md) gave each pool five *conditions*: statuses on its `Pool` resource, also sent as Kubernetes events and metrics, that tell an operator something about its space ([Volume size and capacity](../../docs/design.md#volume-size-and-capacity), "Warnings"). Writing their tests ([test-scenarios.md](../../docs/test-scenarios.md), sections "Pools" and "Placement computation") showed that their boundaries are not fixed:

1. `Stranded` fires "when stranded exceeds 10% of the pool's room", but stranded is in data bytes (what applications can write) and room in raw bytes (disk space, every copy counted), so C2 reads as 6.1% one way and 18.2% the other. What should the rule compare, at what threshold, so it fires when an operator should act and not on ordinary layouts? What is "a domain's stranded room", which the confirmation at `rock-solid pool create` needs?
2. `CannotReprotect` compares the pool's data with *reprotectable*, "balanced over every failure domain but the largest". The spike's C4 removed the disk holding another pool (3.6 TB), the design's text the largest domain (2.4 TB). Which domain is right: the largest, the worst single one for this pool, or each in turn? Does it apply to a one-copy `none` pool?
3. Exactly where `NearFull` and `Full` start (strict or not, per disk), how long "after placement has had time to move members" is before `Unbalanced`, and what `a` is for copies, which the design calls "`max_per_domain` copies" while copies must sit in distinct failure domains.

## Summary

Some words first. A *failure domain* is the unit a pool keeps copies apart across, by default a node. A disk's *room* for a pool is the space `write_limit` (90%) leaves on it after every pool's data. *Balanced* is the most the pool could hold if placement spread it perfectly under its failure-domain rule. *Stranded* space is space that rule stops the pool from ever using, such as most of a 4 TB node next to 1 TB nodes under `rep3`, which must put one copy of every chunk on each of three nodes.

**One idea runs through every answer: measure against what the pool could hold, not against what is still free.** Call a domain's *capacity* for the pool its room plus the pool's own data on it, that is `write_limit` × size less *other* pools' data. Figures measured against free room drift as the pool fills, even when nothing is wrong. Free room shrinks with every byte written, so `Stranded` and `Unbalanced` as written would rise to 100% on healthy pools near full, and reprotectable and the `overcommit: off` admission check would shrink. Placement targets have the same problem: on free room, each finished move shifts the targets and proposes moves back, which costs 1.5 to 2 times the moves (test T). Capacity does not move as the pool fills.

**1. `Stranded`.** The consistent ratio is raw stranded room ÷ capacity, both in raw bytes. That is the same number as stranded data bytes ÷ (capacity ÷ b), so C2 is **18.2%**; the 6.1% reading mixes units. A domain's stranded room is **max(0, cᵢ − a × balanced)**, what is left on it at balance, and these add up to the pool's stranded room.

On a mismatched three-node homelab (2, 2, 4 TB: 25%) stranding is permanent; only new hardware clears it. So it belongs where the operator sees and accepts it: `pool create` and disk changes print it and ask to confirm when a domain strands more than half its capacity, and `pool status` always shows it. The *condition* is raised only when stranding exceeds 10% **and** is more than 5 points above what the operator last confirmed. That happens when a disk fails out or another pool fills the small disks, not because the layout was always so. Both numbers are judgment calls.

**2. `CannotReprotect`.**
- **Which domain.** For copies, and for shards one per domain, take the domain with the most capacity: removing it is provably the worst single loss. The spike picked by disk size, where C4's disks tie at 2 TB, and so got the wrong one. For shards with `max_per_domain` above 1 that proof fails (367 of 3,000 random layouts, test E2), so phase 10 checks each domain in turn.
- **How it is computed.** Reprotectable is computed over capacity: over free room, C2 would fire at 0.8 TB held instead of 0.9 TB.
- **When it fires.** Never for one-copy pools. It is raised only when data is the problem, meaning the pool holds more than the surviving disks could re-protect. A loss that leaves too few domains or disks for a PG's width, such as `rep3` on exactly three nodes losing a node, is a fact of the layout: stated at creation and in `pool status`, never a standing warning.
- **No spare domain.** Such layouts still get the check that matters to them: losing one disk of a node with two. Three nodes of 2 × 2 TB under `rep3` raise it above 1.8 TB held.

**3. The other boundaries.**
- **`Full`** is the primary's admission rule, grants included: a member disk's live blocks plus outstanding grants × its share reach ⌊size × `write_limit`⌋, in whole blocks. So `Full`, available = 0 and the first refused write coincide.
- **`NearFull`** uses the same sum from 10 points below the limit. The two are exclusive per pool, as Ceph's flags are.
- **Clearing.** Both clear only 1 point below where they were raised, a judgment call. Without that gap, a full pool holding block writes would emit an event pair at every discard.
- **`Unbalanced`** needs no clock. It is evaluated when placement proposes no move and nothing is still copying, and keeps its value otherwise. It is raised when available + held is more than **10%** under balanced. That is twice the 5% margin, a judgment call: the worst ordinary disk addition in the model leaves 9.4%.
- **a = 1 for copies.**

**Placement changes the rules depend on:**
- targets by capacity;
- the source disk is the one furthest above its target even inside the margin, as the design's text and Ceph say, not as the spike's loop did;
- the margin per disk is max(5% of target, 1 member), because 5% is less than one member on a small disk and the loop could never meet it;
- the property "available within 1% of balanced once the loop stops" becomes "available + held ≥ balanced ÷ (1 + margin) once converged". It held in every converged run of the model.

One more problem turned up outside the questions: `overcommit: off` admission must compare reservations with balanced over capacity.

## What the design says now

- **Figures** ([Volume size and capacity](../../docs/design.md#volume-size-and-capacity)). Available is the smallest room ÷ share over the pool's disks, less grants. Balanced is min over j of S₍>ⱼ₎ ÷ (b − j·a) over failure domains sorted by room. Stranded is room ÷ b − balanced, and reprotectable is balanced over every domain but the largest. a is "the most of each byte one failure domain may hold (`max_per_domain` copies, or `max_per_domain ÷ data` for shards)".
- **Warnings** (same section): "`NearFull` once a member disk is within 10 points of `write_limit`, `Full` once one passes it, `Stranded` when stranded exceeds 10% of the pool's room, naming the domains that strand it, `Unbalanced` when available stays more than 5% under balanced after placement has had time to move members, and `CannotReprotect` when the pool holds more than reprotectable, so that losing its largest domain would leave recovery no room to restore full redundancy." `rock-solid pool create` and disk changes "ask for confirmation when a domain would strand more than half its room". [Block frontend](../../docs/design.md#block-frontend): while block writes are held, "the pool's condition is `Full`".
- **Placement** ([placement table](../../docs/design.md#data-layer-chunkstore)). "A disk's target is its room for the pool, what `write_limit` leaves after every pool's data on it". "While a disk is outside a fill margin of its target, move one member from the disk furthest above its target to the eligible disk furthest below; stop when no move brings that disk nearer its target." The margin has no value, and both spikes used 5%. [Testing strategy](../../docs/design.md#testing-strategy) and the build checklist ask that once the loop stops, "no disk exceeds the fill margin of its capped target" and "available is within 1% of balanced".
- **Creation rule** ([Pools and redundancy](../../docs/design.md#pools-and-redundancy)): "Copies always sit in distinct failure domains. Shards may share one up to `max_per_domain`, so `data + parity ≤ domains × max_per_domain`."
- **Reservations**: with `overcommit` off, "reservations plus what snapshots hold stay within balanced", reservations being the sum of the pool's volume sizes.
- **Tests and blogs.**
  - [test-scenarios.md](../../docs/test-scenarios.md) "Pools" marks each of the three questions Open. It reads a domain's stranded room as min(roomᵢ, a × balanced), which is the domain's load at balance, not what it leaves unused.
  - [Blog 02](../../blogs/02-chunks-and-placement.md) says `Unbalanced` fires "because members are still moving", the opposite of the design's "after placement has had time".

## What other systems do

### Ceph

Read at `ceph/ceph` `b7e67878` (2026-10-18).

- **Per-device states, strict.** Each OSD (Ceph's per-disk daemon) checks its used ÷ total against the ratios in order, with strict comparisons: `pratio > failsafe_ratio`, `ratio > full_ratio`, `ratio > backfillfull_ratio`, `pratio > nearfull_ratio` (`src/osd/OSD.cc:836-843`).
  - `pratio` is the physical ratio, and `ratio` adds pending backfill (`:1089-1114`). So `FULL` and `BACKFILLFULL` count data still to arrive, while `NEARFULL` and failsafe do not.
  - The defaults are 0.85, 0.90, 0.95 and 0.97 (`src/common/options/global.yaml.in:1665-1689, 3495-3498`). An OSD at exactly 95% is not full.
- **Pool flags from devices, exclusive, no hysteresis.** The monitor marks a pool `FULL`, `BACKFILLFULL` or `NEARFULL` when any up OSD its rule uses is in that state (`OSDMap::get_full_pools`, `src/osd/OSDMap.cc:1763-1795`). Setting `FULL` clears the other two (`src/mon/OSDMonitor.cc:1740-1742`). Health reports `OSD_FULL` as an error and `OSD_BACKFILLFULL` and `OSD_NEARFULL` as warnings, per OSD (`OSDMap.cc:8081-8118`). A state clears as soon as the ratio drops.
- **The available figure clamps at the boundary the state does not.** `get_rule_avail` takes, per OSD, `max(0, kb_avail − kb × (1 − full_ratio))` (`src/mon/PGMap.cc:1053`). That is 0 once used ≥ `full_ratio`, so `MAX AVAIL` reads 0 at exactly 95% while the OSD is not yet `FULL`. Pool quotas use `≥` (`PGMap.cc:3113, 3120`). Ceph uses both conventions.
- **Raw against raw.** The PG autoscaler's overcommit warning multiplies each pool's `target_size_bytes` by its raw-used rate (`size`, or `(k + m) ÷ k`), then compares the sum with the root's raw capacity (`src/pybind/mgr/pg_autoscaler/module.py:464, 494-503, 1111-1137`).
- **No predictive check for losing a domain.** The documentation asks operators to plan by hand:
  - to plan "for a series of Ceph OSD Daemon failures where the cluster can recover to an `active+clean` state";
  - to read `nearfull` as "a failure of some OSDs could result in a temporary service disruption";
  - to derive the full ratios from the capacity of the OSDs, or the rack, expected to fail (`doc/rados/configuration/mon-config-ref.rst:305-386`).

  Ceph reports the problem only once it happens: `PG_BACKFILL_FULL`, "Low space hindering backfill (add storage if this doesn't resolve itself)", and `PG_RECOVERY_FULL` (`PGMap.cc:2679-2692`).
- **One-copy pools are a separate warning.** `POOL_NO_REDUNDANCY` fires for every pool with `size == 1` (`OSDMap.cc:8326-8344`). It can be turned off with `mon_warn_on_pool_no_redundancy` (`src/common/options/mon.yaml.in:599-603`).
- **The balancer has no imbalance warning, and gates on state, not time.**
  - It wakes every `sleep_interval`, 60 s (`src/pybind/mgr/balancer/module.py:310-314, 734-778`).
  - It skips a round while any PG is unknown, degraded or inactive, or while more than `target_max_misplaced_ratio` (5%) of objects are misplaced. When PGs are `backfill_toofull` it proceeds anyway, so that its moves can unblock recovery (`:1043-1075`, `override_toofull` at `:1056`).
  - It makes at most 10 changes per round (`:315-319`).
  - When nothing helps it records "Unable to find further optimization ..." in `balancer status`, and no health check reads it (`:1244-1247`).
- **Its margin is in PGs, and 1 is recommended for mismatched disks.** A disk within `upmap_max_deviation` (default 5 PGs) of its target counts as balanced (`module.py:320-327`). The documentation warns that "an excessively low value ... may result in the balancer shuffling data forever". It recommends 1, "reasonable and safe for most clusters", for those "with multiple CRUSH device classes and/or OSDs that differ in capacity" (`doc/rados/operations/balancer.rst:88-127`), which is the homelab case.
- **It moves from disks inside the margin when only underfull disks remain.** `calc_pg_upmaps` stops once every OSD is within the deviation (`OSDMap.cc:6095`). If no OSD is over the deviation but some are under it, it logs "Using more_overfull since we still have underfull" and takes sources from every OSD above its target (`:6117-6128`; `fill_overfull_underfull`, `:6557-6597`). That is the design's text, not the spike's loop.
- **Time thresholds where time is the point.** PG states such as inactive or undersized count as *stuck* after `mon_pg_stuck_threshold`, 60 s (`src/common/options/mgr.yaml.in:325-332`; `PGMap.cc:2583`).
- **`min_size` bounds.** 1 to `size` for replicated pools, k to `size` for erasure-coded ones (`OSDMonitor.cc:8862, 8878`).

### Vitastor

Read at `vitalif/vitastor` `df593bad` (2026-09-27).

- **What it stores.** Per pool, the monitor stores `total_raw_tb`, computed from the actual integer PG table as pg size × the smallest over OSDs of size × PG count ÷ PGs on the OSD (`mon/pg_gen.js:236-258`; `mon/lp_optimizer/lp_optimizer.js:106, 526-548`). It also stores `space_efficiency`, which is that figure ÷ the sum of the OSDs' sizes (`mon/etcd_schema.js:366-372`).
- **What the ratio measures.** It is raw over raw. As it comes from the actual table, 1 − efficiency covers stranding *and* table imbalance together, against total size. It is the analogue of 1 − (available + held) ÷ (capacity ÷ b) here, not of stranding alone.
- **How it is shown.** As a Prometheus gauge (`mon/prometheus.js:47-50, 164`) and the `EFFICIENCY` column of `vitastor-cli df` (`docs/usage/cli.en.md:84-97`), with no threshold and no warning.
- **Recovery timing.** Recovery waits a fixed `osd_out_time`, 600 s, after an OSD fails (`docs/config/monitor.en.md:152-159`).

### CockroachDB

Read at `cockroachdb/cockroach` `d30c905f` (2026-09-23).

- **Capacity is defined as here.** "The disk's capacity is really just the available space plus cockroach's usage", so a store's fill is used ÷ (available + used) (`pkg/roachpb/metadata.go:563-573`).
- **Full at or above the threshold.** A store accepts new replicas while `FractionUsed() < 0.95` and rebalanced ones while `< 0.925` (`pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:53-58, 772-784`). The setting comments say "greater than" (`:252-282`). The 2.5-point gap keeps replicas from "ping pong[ing] back and forth between two nodes" (`:265-273`).
- **A rebalance margin with a floor.** Range counts are balanced to within max(5% of the mean, `minRangeRebalanceThreshold` = 2 replicas) (`:60-70, 139-145, 2526-2532`). The floor exists because "with few enough replicas per node (<<30)", 5% alone "could lead to a condition that would always fire".

### Not found

- **No system read warns about stranded space.** Vitastor reports it as a number, mixed with imbalance; Ceph does not report it at all.
- **No system read predicts whether losing a domain leaves room to recover.** Ceph documents it as manual planning and reports the failure when it happens.
- Longhorn and MinIO were not read again: the earlier study found nothing on these boundaries.

## Findings

All figures are from [results.txt](results.txt). Tests S, R, O, E, E2 and D compute closed forms; T and U build placement tables.

### 1. Capacity, and the consistent stranded ratio

Let cᵢ be a domain's capacity for the pool (`write_limit` × size less other pools' data, which is room plus this pool's own data) and B balanced over the cᵢ.

At balance, domain i holds min(cᵢ, a·B), and these loads add up to exactly b·B. The reason: g(D) = Σᵢ min(cᵢ, a·D) − b·D is continuous and concave, 0 at D = 0, and finally decreasing, so the largest D with g ≥ 0 has g = 0 (checked on 300 random layouts). Hence:

- raw stranded room = Σᵢ cᵢ − b·B = Σᵢ max(0, cᵢ − a·B), the sum of the **domains' stranded rooms**;
- raw stranded ÷ capacity = stranded data ÷ (capacity ÷ b). The units do not matter as long as both sides use the same one.

| Case | Stranded (data) | ÷ raw room, the mixed reading | b × stranded ÷ capacity | Domains' stranded room (share of their capacity) |
| --- | --- | --- | --- | --- |
| C1 `rep2`, 0.7 and 2 GB | 0.585 GB | 24.1% | **48.1%** | B 1.17 GB (65%) |
| C2 `rep3`, 4, 4, 2, 1 TB | 0.600 TB | 6.1% | **18.2%** | A, B 0.9 TB each (25%) |
| C3 `ec4-2` at 2 per node, 4, 4, 1 TB | 3.600 TB | 44.4% | **66.7%** | A, B 2.7 TB each (75%) |
| C4 `none`, A holding 1.2 TB of another pool | 0 | 0 | **0** | none |
| C5 `rep3`, ten nodes | 0 | 0 | **0** | none |
| C6 `rep2`, 4, 1, 1 TB | 0.900 TB | 16.7% | **33.3%** | A 1.8 TB (50%, one block over in whole blocks) |

As the pool fills at balanced proportions, its stranded figure stays the same and its free room shrinks. Divided by free room, C2's ratio goes 18.2% → 21.4% → 26.1% → 46.2% → 75% at 0, 0.5, 1, 2 and 2.5 TB held (test R). Divided by capacity it stays 18.2%.

### 2. Stranding on ordinary layouts is permanent, so it is a layout fact

Test O, empty pools, b × stranded ÷ capacity:

| Layout | Stranded | Worst domain's share stranded |
| --- | --- | --- |
| `rep3`, three equal nodes | 0 | 0 |
| `rep3`, 2, 2 and 1.92 TB (vendors' "2 TB") | 2.7% | 4% |
| `rep3`, 2, 2 and 1.8 TB | 6.9% | 10% |
| `rep3`, 2, 2, 4 TB (one node upgraded) | 25% | 50% |
| `rep3`, 2, 2, 1 TB | 40% | 50% |
| `rep3`, four nodes 2, 2, 2, 1 TB | 0 | 0 |
| `rep3`, five nodes 4, 2, 2, 2, 1 TB | 4.5% | 12.5% |
| `rep2`, 2 and 1.92 TB | 2.0% | 4% |
| `rep2`, 4 and 2 TB | 33% | 50% |
| `rep2`, three nodes 2, 2, 1 TB | 0 | 0 |
| `rep2`, four nodes 4, 1, 1, 1 TB | 14% | 25% |
| `ec2-1`, three nodes 4, 4, 2 TB | 40% | 50% |

For a pool exactly as wide as its node count, the ratio is 1 − n × smallest ÷ total. A three-node `rep3` therefore passes 10% once one node is more than 14% smaller than two equal peers, or more than a third bigger. That describes many homelabs, and nothing but new hardware changes it.

A condition raised from the first day on such a layout says nothing new after the creation prompt, and it trains the operator to ignore conditions. That is also why the no-spare-domain case of `CannotReprotect` is not a condition (finding 4). Stranding the operator has not seen is a different matter: a disk marked out after a failure, or another pool filling the small disks. That is what the condition should report.

### 3. Removing the largest domain is the worst case, for copies; not for shards with the per-disk cap

**Copies, and shards with `max_per_domain` 1.** Balanced depends only on the domains' capacities, symmetrically, and never decreases when one grows. Sorted c₁ ≥ … ≥ cₙ, removing c₁ leaves (c₂ … cₙ), and removing any cₖ leaves something elementwise at least as large. So the largest-capacity domain is the worst single loss. This is checked on the C cases and 300 random layouts.

In C4 the disks tie at 2 TB, but A has 0.6 TB of capacity and B and C 1.8 TB. Removing B or C leaves 2.4 TB, while removing A, the first by size, leaves 3.6 TB.

Over free room, C2's reprotectable falls as the pool fills: 0.900 TB empty, 0.633 TB at 0.8 TB held (where the condition would fire although the test cases expect it not to) and 0.567 TB at 1 TB. Over capacity it stays at 0.900 TB.

**Shards with `max_per_domain` above 1.** Here balanced also depends on how many disks a domain has (finding 8), and the theorem fails. Take `ec4-2` at 2 per node, with node A one 4 TB disk and nodes B, C and D two 1.5 TB disks each:

- removing A, the largest, leaves 5.4 TB;
- removing B, C or D leaves 0, since 1 + 2 + 2 < 6 members fit.

In random layouts of 4 to 6 nodes the largest domain was not the worst in 367 of 3,000 (test E2). Phase 10 must check each domain in turn, with the width test for each.

### 4. `CannotReprotect` on layouts without a spare domain

A layout has a *spare domain* when losing any one domain still leaves room for a PG's width. C1 (`rep2` on two nodes), C3 and the default `rep3` on three nodes have none, so their domain-loss reprotectable is 0. Read literally, the condition is raised from the first byte written and only a new node clears it.

The disk is still the common failure on such layouts, and recovery then refills within the node. Where a spare domain exists, losing a domain dominates losing any one of its disks. Taking disk capacity d out of domain k and then dropping the reduced cₖ − d leaves exactly what removing domain k leaves. That multiset is elementwise at least what removing the largest domain leaves, and adding cₖ − d back cannot lower balanced (`self_check`).

So the disk check is needed only where no spare domain exists (test D):

| `rep3`, three nodes | Spare domain | Worst single disk loss | Reprotectable |
| --- | --- | --- | --- |
| 2 × 2 TB each | no | any 2 TB disk | **1.8 TB** |
| 2 × 2, 2 × 2 and 4 + 1 TB | no | C's 4 TB disk | **0.9 TB** |
| one 4 TB disk each | no | none to lose within a node | layout fact only |

On the first of these, the default pool raises `CannotReprotect` above half full. That is true and actionable: past that point a single failed disk leaves PGs degraded until it is replaced.

### 5. `Unbalanced`: what placement leaves once it stops, and why the comparison is available + held

**Available + held does not depend on held.** Data follows the table, so disk i holds shareᵢ × held of this pool, roomᵢ = cᵢ − shareᵢ × held, and available = min(cᵢ ÷ shareᵢ) − held. The study's first rule, available < 0.9 × (balanced − held), therefore divides a fixed absolute gap by a shrinking amount. At 70% held it would raise `Unbalanced` after 21 to 24 of 200 ordinary disk additions, and at 90% held after 88 to 122. Compared as available + held < 0.9 × balanced, it raises none at any fill (test U).

Test U builds capped tables for 200 random layouts per pool: 3 to 6 nodes, 1 or 2 disks of 0.5 to 8 TB each, some partly used by other pools, and `pg_count` by the design's rule of about 100 members per disk. It then adds one disk to a random node and balances from the old table. Shortfall is 1 − (available + held) ÷ balanced.

| Table | Rule for the source disk | Margin | Moves per add | Median | p90 | p99 | Max | Stopped outside the margin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `rep2`, from scratch | | 5% | | 0.4% | 0.6% | 0.9% | 1.0% | 18 / 200 |
| `rep2`, disk added | above the margin (spike) | 5% | 127 | 4.6% | 4.7% | 9.4% | 12.2% | 188 |
| `rep2`, disk added | above its target (design, Ceph) | 5% | 149 | 1.2% | 3.2% | 4.5% | 9.4% | 15 |
| `rep2`, disk added | above its target | max(5%, 1 member) | 149 | 1.3% | 3.2% | 4.5% | 9.4% | **0** |
| `rep2`, disk added | above its target | max(5%, 2 members) | 149 | 1.3% | 3.2% | 5.4% | 9.4% | 0 |
| `rep3`, from scratch | | 5% | | 0.3% | 0.7% | 1.0% | 1.1% | 7 / 200 |
| `rep3`, disk added | above the margin (spike) | 5% | 132 | 4.3% | 4.7% | 4.8% | 4.8% | 161 |
| `rep3`, disk added | above its target (design, Ceph) | 5% | 147 | 0.8% | 3.0% | 4.6% | 4.7% | 8 |
| `rep3`, disk added | above its target | max(5%, 1 member) | 147 | 0.8% | 3.0% | 4.6% | 4.7% | **0** |
| `rep3`, disk added | above its target | max(5%, 2 members) | 146 | 0.9% | 3.0% | 4.6% | 5.6% | 0 |

- **A converged table is within the margin of balanced, and no closer is promised.** For a disk in an uncapped domain, capped targets give cᵢ ÷ target share = balanced exactly. (Water-filling caps the same domains the min over j does, and width × slot weight = b.) A capped disk sits above balanced. So if every disk is within (1 + mᵢ) of its target, available + held ≥ balanced ÷ (1 + max mᵢ). Every converged run of the model's 2,000 disk additions met this.
- **5% is less than one member on a small disk.** A 0.5 TB disk next to 8 TB disks has a target of 10 to 20 slots. The loop can never bring it within 5%, so it stops "outside the margin" (18 and 15 of 200 above), and the design's property "no disk exceeds the fill margin" fails.
  - With a floor of one member, every run converged.
  - CockroachDB floors its 5% at 2 replicas. Here a floor of 2 widened the tail (max 5.4 to 5.6%) for no gain.
  - Ceph recommends a deviation of 1 PG for mismatched disks.
- **The design's text does better than the spike.** Moving from the disk furthest above its target, even inside the margin, cuts the median shortfall after an addition from 4.3 to 4.6% to 0.8 to 1.2%. The cost is 11 to 17% more moves. The spike's rule leaves available about 4.7% short of balanced after every addition, and so does the design's own property "within 1% once the loop stops".
- **The 9.4% case is granularity.** It is the same `rep2` layout under every rule. It converges under the floored margin, so a disk with a target of about ten members sits one member above it. Raising `pg_count` is the fix, which is a reason `Unbalanced` can report.
- **Time is the wrong measure.** The leader knows when placement is done: the computation proposes no move, and no PG lists past members. How long that takes depends on how much data moves. Ceph's balancer gates on states, not time. At 10%, no ordinary disk addition raises `Unbalanced` in the model (0 of 400 under the design's rule). The condition then means placement stopped well short of what it could reach: pinned PGs, a drain, `pg_count` too small for a disk, or a stuck algorithm.
- **A stuck copy needs its own condition.** A PG whose backfill is blocked keeps its past members indefinitely, so `Unbalanced` is never evaluated and nothing reports it. No state says "copying stopped making progress". That is the one place a timer fits, as in Ceph's 60 s `mon_pg_stuck_threshold`.

### 6. Placement targets by capacity

When a PG moves, its data moves with it. With targets on free room (design.md: "after every pool's data on it", this pool's included), each finished move changes the targets, and the next computation proposes moves back. Test T adds a disk to a pool holding data and recomputes after each round's data has moved, until no move is proposed:

| Layout | Moves, targets by room | Moves, targets by capacity |
| --- | --- | --- |
| `none`, one 2 TB node holding 1 TB, a 2 TB node added | 124, over 4 recomputations | 61, over 2 |
| C6 holding 0.8 TB, a 2 TB disk added to node B | 98, over 3 | 62, over 2 |
| C2 holding 1.5 TB, a 4 TB disk added to node D | 153, over 5 | 98, over 2 |

With targets by capacity, the second computation always confirms the first. The `Unbalanced` gate, "the computation proposes no move", then does not flip after each batch of moves.

### 7. Admission with `overcommit` off has the same denominator problem

Reservations are the sum of volume sizes, which includes data already written; balanced over free room excludes that data.

In C2 with 2.0 TB of the pool's own data, balanced over free room is 0.70 TB and balanced over capacity 2.70 TB (test R). With reservations of 2.5 TB, a 0.1 TB `CreateVolume` would fail against 0.7 TB, although 2.5 + 0.1 is within the 2.7 TB the pool can hold. Every reservation case in test-scenarios.md starts from an empty pool, which hides this.

### 8. Shards with `max_per_domain` above a domain's disk count (phase 10)

Two shards of one PG cannot share a disk, so a domain holds at most min(`max_per_domain`, its disks) members of a PG (test E):

| `ec4-2` at 2 per node | Width fits, design | Width fits, per disk | Balanced, design | Balanced, each disk capped |
| --- | --- | --- | --- | --- |
| Nodes of 2×2, 2×2 and 1×4 TB | yes (6 ≤ 3 × 2) | **no (5 < 6)** | 7.20 TB | **0** |
| Nodes of 2×2, 2×2, 2×2 and 1×4 TB | yes | yes | 9.60 TB | **8.64 TB** |
| C3: 2×2, 2×2 and 2×0.5 TB | yes | yes | 1.80 TB | 1.80 TB |

The general bound is the largest D with Σ over domains of min(a·D, Σ over its disks of min(cₖ, D ÷ data)) ≥ b·D. It reduces to the design's formula when `max_per_domain` is 1. Placement targets need the same per-disk cap.

In an erasure-coded pool the copies sit on a PG's first `replicas` members, so placement must also keep those members in distinct domains. That follows from "copies always sit in distinct failure domains" but is not stated among the placement rules.

## Proposed answer

### Terms

- **Capacity** cᵢ of a domain for a pool: `write_limit` × size less other pools' data, summed over the pool's disks in the domain. That is its room plus the pool's own data there.
- **Held**: the pool's data, each chunk's raw bytes divided by the overhead of its form (`replicas` for copies, `(data + parity) ÷ data` for shards).
- **Balanced** B: the closed form over the cᵢ, so it is the most the pool can hold in total. a is **1 for copies**, whatever `max_per_domain` says, and `max_per_domain ÷ data` for shards.
- **Available**: as now, over free room, less grants.
- **Stranded fraction** f: Σᵢ max(0, cᵢ − a·B) ÷ Σᵢ cᵢ, and 0 when Σᵢ cᵢ = 0.
- **The figures' scope.** They are computed over every disk with a share in the current table, a down disk at its last reported fill. A disk leaves the figures only when it is removed or marked out, so a node reboot changes nothing.
- **Arithmetic.** Fill comparisons are in whole 4 KiB blocks, with the limit at ⌊size_blocks × `write_limit`⌋ and B rounded down to whole blocks. `write_limit` must exceed 10 points.

### The rules

| Condition | Raised when | Cleared when | Threshold |
| --- | --- | --- | --- |
| `Full` | Some member disk's live blocks + ⌈grants × share⌉ ≥ its limit in blocks. This is the primary's admission rule, so it coincides with the first refused write and with available = 0 | Every member disk is at least 1 point below its limit, grants counted | Limit: the design's. Clearing gap of 1 point: judgment |
| `NearFull` | Not `Full`, and some member disk's live blocks + ⌈grants × share⌉ ≥ (`write_limit` − 10 points) in blocks | 1 point below that | 10 points: the design's. 1 point: judgment |
| `Stranded` | f > 10%, and f > the confirmed fraction + 5 points. It names each domain with cᵢ > a·B and its stranded room cᵢ − a·B | f falls 1 point below the level that raised it, or the operator confirms the current layout | 10% and 5 points: judgment, from hand-picked layouts (test O) |
| `Unbalanced` | Evaluated only when placement proposes no move for the pool and no PG of the pool lists past members; otherwise it keeps its last value. Raised when available + held < 0.9 × B. It names the disk that sets available, with its share and target | The same evaluation finds available + held ≥ 0.91 × B | 10% (twice the margin): judgment. The worst ordinary addition in test U leaves 9.4% |
| `CannotReprotect` | Pools with `replicas` ≥ 2 only. Raised when held > reprotectable, where reprotectable is the least balanced over the survivors of any loss considered, among losses whose survivors can still hold a PG's width (below). It names the domain or disk | held ≤ reprotectable | None |
| `Stuck` | A PG of the pool has past or missing members and has copied nothing for `stuck_after` on `ctx.Now`. It names the PGs | It copies again, or its past members are trimmed | `stuck_after` 60 s, Ceph's `mon_pg_stuck_threshold`: judgment. The one timer, because no state shows that copying stopped |

Losses that `CannotReprotect` considers:

- **Spare domain, copies or one shard per domain.** The domain with the largest cᵢ.
- **No spare domain.** Each disk whose domain has another disk, with that disk's capacity taken out of its domain.
- **Shards with `max_per_domain` > 1 (phase 10).** Each domain in turn, and failing those each disk, with each disk capped at one member per PG.
- **A loss whose survivors cannot hold the width** is a fact of the layout. The creation output states it ("losing node A leaves its PGs degraded until it returns"), and so does `rock-solid pool status`. It is never a condition.

Two things around the conditions:

- **Confirmation.** `rock-solid pool create` and disk changes print, per domain, cᵢ, its load at balance min(cᵢ, a·B) and its stranded room max(0, cᵢ − a·B). They ask for confirmation when some domain has 2 × (cᵢ − a·B) > cᵢ, and record f as the pool's confirmed fraction. So does `rock-solid pool confirm`, which also suits a `Pool` created without the CLI (confirmed fraction 0 until then). `pool status` always shows f, the domains and the confirmed fraction. The half-capacity prompt is the design's number and a judgment call.
- **Admission.** The primary refuses writes that add data under the same predicate as `Full`. With `overcommit` off, reservations plus snapshots are checked against balanced over capacity.

Placement, which the rules depend on:

- A disk's target is its capacity for the pool, water-filled over domains and split within a domain as now.
- The source of a move is the disk furthest above its target, even inside the margin, while any disk is outside it.
- The fill margin of a disk is max(5% of its target, 1 member). Ceph's 5 PGs at about 100 per disk and CockroachDB's 5% set the 5%; Ceph's recommendation of 1 PG for mismatched disks sets the floor.
- Properties once the loop stops: it has converged. Then available + held ≥ B ÷ (1 + the largest margin of the pool's disks), and from scratch available + held is within 1.1% of B (model).

### Outcomes on C1 to C6

The pools are created with `rock-solid pool create` and are empty except where a row gives an amount held. Tables are capped and built from scratch.

| Case | f | Create prompt | `Stranded` | Reprotectable (loss considered) | `CannotReprotect` | `Unbalanced` (shortfall) |
| --- | --- | --- | --- | --- | --- | --- |
| C1 `rep2`, 0.7 and 2 GB | 48.1% | **Yes**, B strands 65% | No: confirmed at creation. **Yes**, naming B 1.17 GB, if the pool was never confirmed | None: no spare domain, and no node has a second disk | Never; status says losing either node leaves PGs degraded | No (0.0%) |
| C2 `rep3`, 4, 4, 2, 1 TB | 18.2% | No, A and B 25% | No once confirmed; **yes**, naming A and B 0.9 TB each, if not | 0.900 TB (node A) | **Above 0.9 TB held**: 1.0 TB yes, 0.8 TB no, at any fill | No (0.8%) |
| C3 `ec4-2` at 2 per node, 4, 4, 1 TB | 66.7% | **Yes**, A and B 75% | As C1: A and B 2.7 TB each if never confirmed | None: losing a node, or a disk of any node, leaves fewer than 6 members | Never; status says so | No (0.0%) |
| C4 `none`, three 2 TB, A holding 1.2 TB elsewhere | 0 | No | No | Would be 2.400 TB (B or C) | Never: one copy | No (0.3%). A size-weighted table, 57% short, only once placement cannot move it, e.g. every PG pinned |
| C5 `rep3`, ten nodes | 0 | No | No | 5.250 TB (node A) | Above 5.25 TB held | No (0.4%) |
| C6 `rep2`, 4, 1, 1 TB | 33.3% | **Yes**: in whole blocks A strands 439,453,126 of 878,906,250, two blocks over half | No once confirmed | 0.900 TB (node A) | Above 0.9 TB held | No (0.0%) |

Later events on these pools:

| Event | Condition |
| --- | --- |
| C2 confirmed at 18.2%; node D's 1 TB disk is marked out after a failure | f rises to 1 − 3 × 1.8 ÷ 9.0 = 40% > 18.2 + 5: **`Stranded`**, naming A and B |
| C2 with a 4 TB disk added to D by `rock-solid` and confirmed, the pool 90% held | No `Unbalanced`: available + held is within 1.5% of balanced at any fill (test T) |
| `rep3` on three nodes, one node rebooting | No change: its disks keep their shares at their last fills |
| `rep3` on three nodes of 2 × 2 TB, holding 2.0 TB | `CannotReprotect`, naming any one disk (reprotectable 1.8 TB) |

Fill of the fullest member disk, with `write_limit` 90% and no grants unless stated:

| Fill | Condition |
| --- | --- |
| 79% | None |
| Exactly 80% (live blocks = ⌊0.8 × size⌋) | `NearFull` |
| One block under 90% | `NearFull`; available is one block ÷ share |
| Exactly 90% | `Full`; available is 0; the next write that adds data is refused |
| Back to 89.5% after a discard, writes held | Still `Full` (clears at 89%), so a held block volume emits one `Full` event, not one per discard |
| C2 with four 256 MiB grants, a disk 1 GiB × share under its limit | `Full`; available is 0; admission refuses |
| Two pools, limits 90% and 95%, disk at 92% | The 90% pool `Full`, the 95% pool `NearFull` |

### Costs

- One more term, capacity, which needs each pool's own data per disk. The leader already receives it from the per-disk fills and per-pool used counts that heartbeats carry.
- `CannotReprotect` evaluates balanced once per pool per heartbeat. Without a spare domain it evaluates it once per disk; in phase 10 with `max_per_domain` > 1, once per domain. That is at most tens of evaluations.
- The design's balance rule costs 11 to 17% more member moves per disk addition than the spike's. Targets by capacity save 36 to 51% of the moves on a pool that holds data (test T).
- `Stranded` needs one stored number per pool (the confirmed fraction) and one CLI verb (`pool confirm`).
- `Stuck` adds one timer per PG that is copying.
- The 1-point clearing gap means `Full` and `NearFull` can still show for a short while after space is freed.

### Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Stranded data bytes ÷ raw room (the 6.1% reading) | Divides data bytes by raw bytes, so under `rep3` the same stranding reads a third of what it is |
| Stranded ÷ free room, available ÷ (balanced − held), reprotectable or admission over free room | All drift with the pool's own fill: `Stranded` and `Unbalanced` reach 100% on healthy pools near full, and reprotectable and admission shrink (tests R, U) |
| Targets by free room (the design's text) | Each finished move shifts the targets; 1.5 to 2 times the moves and 3 to 5 recomputations (test T) |
| `Stranded` raised whenever f > 10% | Raised forever on mismatched three-node homelabs (25 to 57%), which only new hardware changes |
| `Stranded` as a status-only figure, never a condition | Misses stranding that arrives without the operator's knowledge, such as a disk marked out after a failure (C2: 18% → 40%) |
| Exempting room another pool on the domain could use | Hides room nobody uses. Room another pool actually fills leaves this pool's capacity, which lowers f by itself |
| The largest domain by size, or for shards with `max_per_domain` > 1 the largest by capacity | By size picks the wrong domain (C4). With the per-disk cap the largest is not the worst in 367 of 3,000 layouts (test E2) |
| `CannotReprotect` on one-copy pools | No redundancy to restore. Ceph's equivalent is the separate `POOL_NO_REDUNDANCY` |
| `CannotReprotect` for losses that leave too few domains or disks for the width | Raised from the first byte on the default `rep3` with three nodes and on the `rep2` preset's two nodes |
| No disk-loss check without a spare domain | The default layout would get no reprotection warning at all, although one failed disk is its most likely loss |
| A timer for `Unbalanced` (Ceph's 60 s, Vitastor's 600 s) | A move takes as long as its data takes to copy. The leader knows when placement is done; the timer belongs to `Stuck` |
| `Unbalanced` at 5% | Within 0.2 points of what the spike's loop leaves after every addition (4.7 to 4.8%), and exceeded by 1 to 3 of 200 ordinary `rep2` additions |
| A 5% margin with no floor | Below one member on small disks, so the loop cannot converge and the property fails (18 of 200 `rep2` tables) |
| A floor of 2 members (CockroachDB's) | Widened the tail to 5.4 to 5.6% in the model for no gain over 1 |
| `Full` from live blocks alone, or strict `>` (Ceph's states) | Leaves windows where admission refuses and available is 0 but the pool is not `Full`, which the block frontend relies on |
| No hysteresis (Ceph) | A full pool holding block writes sits at the limit by design, and each discard would clear and re-raise `Full` |

## What the phase 2 model, the simulator and the tests must check

- **Pools, data-driven:**
  - the C1 to C6 outcomes and the "Later events" rows above;
  - C2 with 0.8 and 1.0 TB held at balanced proportions: no `CannotReprotect`, then `CannotReprotect`, with reprotectable 0.900 TB in both;
  - C2 with 2.0 TB held: f still 18.2%, balanced 2.7 TB.
- **Boundaries in blocks:**
  - live blocks exactly ⌊0.8 × size⌋ raise `NearFull`;
  - exactly the limit raises `Full`, available is 0, and a write that adds data is refused;
  - one block below gives `NearFull`, available one block ÷ share, and the write passes;
  - the grants row;
  - clearing at 1 point below;
  - `write_limit` of 10% or less is rejected;
  - Σcᵢ = 0 raises no `Stranded`;
  - C6's create prompt fires by two blocks.
- **Reservations:** C2 with reservations of 2.5 TB and 2.0 TB written: `CreateVolume` 0.1 TB succeeds, 0.3 TB fails.
- **Property, Pools:**
  - available + held ≤ B ≤ Σcᵢ ÷ b;
  - the domains' stranded rooms sum to Σcᵢ − b·B;
  - f does not change as a pool fills at balanced proportions;
  - for copies, balanced without the largest-capacity domain equals the minimum over single domains, and is at most balanced after any single disk loss;
  - `CannotReprotect` never fires on a one-copy pool or for a loss whose survivors cannot hold the width.
- **Placement computation:**
  - targets by capacity;
  - after a disk addition to a pool holding data, the second computation proposes no move;
  - the loop always converges with the floored margin;
  - once converged, available + held ≥ B ÷ (1 + largest margin);
  - `Unbalanced` keeps its value while moves are proposed or past members are listed, and on the test U seeds no disk addition raises it at any fill;
  - a size-weighted C4 table with every PG pinned raises it.
- **Simulator:** a backfill blocked by a full disk raises `Stuck` after 60 s; a node reboot changes no figure.
- **Phase 10:**
  - width fits and balanced with min(`max_per_domain`, disks) per domain (test E);
  - each domain in turn for `CannotReprotect`, with test E2's counterexample;
  - every PG's first `replicas` members in distinct domains.

## Where it would apply

If approved:

- **`docs/design.md`**:
  - [Volume size and capacity](../../docs/design.md#volume-size-and-capacity):
    - define capacity and held;
    - rewrite the figures table: balanced over capacity, the stranded fraction with the domains' stranded rooms, reprotectable by the losses above, a = 1 for copies;
    - rewrite "Warnings" as the rules table, with the confirmed fraction, `pool confirm`, the status lines for layout facts and `Stuck`;
    - in "Write limit", the admission predicate with grants and `≥`;
    - in "Thin provisioning", reservations against balanced over capacity.
  - [Data layer](../../docs/design.md#data-layer-chunkstore), placement table, around line 169: targets by capacity, the source rule and the floored margin.
  - [Pools and redundancy](../../docs/design.md#pools-and-redundancy):
    - the creation output for losses that leave too few domains or disks;
    - `write_limit` above 10 points;
    - for phase 10, min(`max_per_domain`, disks) in the width rule.
  - [Block frontend](../../docs/design.md#block-frontend), "the pool's condition is `Full`" while writes are held: holds under the new definition, which counts grants and clears 1 point down.
  - [Testing strategy](../../docs/design.md#testing-strategy), around line 1103: the converged bound with the floored margin, instead of "within 1%" and "no disk exceeds the fill margin".
  - [Alternatives considered](../../docs/design.md#alternatives-considered), the "Pool capacity" row (around line 1152): balanced over capacity, targets by capacity.
- **`docs/build-checklist.md`**:
  - Pools item: the conditions, `Stuck`, capacity;
  - Placement computation item, around line 97: "toward each disk's room" becomes "capacity", plus the floored margin and the converged bound.
- **`docs/test-scenarios.md`**:
  - "Pools":
    - the terms (capacity, held, f);
    - the capacity cases table: stranded and reprotectable as above;
    - the conditions table, replacing the rows "C1 or C3 holding any data → `CannotReprotect`" (now never) and "C4 with a size-weighted table held past 'time to move members'" (now pinned);
    - the property, which becomes available + held ≤ balanced ≤ capacity ÷ b;
    - "Available, grants and per-node figures": the `Full`-with-grants row;
    - the reservation cases with data written;
    - close the Open lines listed below.
  - "Placement computation":
    - "Placement rules": targets by capacity and the source rule;
    - "Once the loop stops": converged, and the bound;
    - "Errors and open points": refill with no eligible disk is reported by `Stuck` once copying stops, and the layout fact by status.
- **`docs/glossary.md`**: "Available, balanced and stranded capacity" gains capacity, held and a domain's stranded room.
- **Blogs** [02](../../blogs/02-chunks-and-placement.md), whose `Unbalanced` reading "members are still moving" is the opposite of the rule, and [07](../../blogs/07-pools-and-redundancy.md) (`Stranded`, `CannotReprotect`).

### The other Open lines in the two sections

"Pools":

- `sealed: ec` refused in phase 3, with which error: **not answered**.
- The `pg_count` rounding rule: **not answered**. The model used the smallest power of two with `pg_count × width ÷ disks ≥ 100`, which gives the design's 2,048 for 60 disks. Ceph's autoscaler rounds to the nearest power of two.
- `min_live` above `replicas`, or 0: **answered by precedent**. Ceph rejects `min_size` outside 1 to `size` for replicated pools and k to `size` for erasure-coded ones (`src/mon/OSDMonitor.cc:8862, 8878`).
- `pg_count` halved or not doubled: **not answered**.
- How the CLI counts node tolerance for copies under `failure_domain: disk`: **not answered**. With several disks per node, "the largest domain" is then a disk, while the realistic loss is a node; that needs its own rule.
- a for copies: **answered**, 1.
- C4's reprotectable, 3.6 or 2.4 TB: **answered**, 2.4 TB, and it never applies to `none`.
- Exactly 80% or 90%: **answered**, `≥` in blocks, grants counted.
- C2 and `Stranded`: **answered**. Its f is 18.2%; raised only if never confirmed, or if it later rises 5 points.
- How long before `Unbalanced`: **answered**. No time: a state predicate that holds its value while placement moves.
- `CannotReprotect` on `none`: **answered**, never.
- A domain's stranded room: **answered**, max(0, cᵢ − a·B). It replaces the scenario's min(roomᵢ, a·B), which is the domain's load.
- Room clamped at 0: **answered**. Room is max(0, …), as Ceph's `get_rule_avail` clamps (`src/mon/PGMap.cc:1053`).

"Placement computation":

- A balance move as one table or two: **not answered**.
- How a PG with no eligible disk for refill is reported: **answered**. It is `Stuck` once copying stops, and the status line for the layout fact, not `CannotReprotect`.
- The fill margin's value: **answered**, max(5% of target, 1 member) (test U, Ceph, CockroachDB).
- The tolerance for "moves stay near the minimum": **not answered**. The model counts 127 to 155 moves per disk addition, but does not compute the minimum.
- The tie-break between equal disks: **not answered**.
- Pins and drains in the table: **not answered**. A pinned PG is one of the causes `Unbalanced` reports.

## Unverified

- **The thresholds are judgments** checked against a model, not against operators: `Stranded`'s 10% and 5 points, `Unbalanced`'s 10%, the 1-point clearing gap, `Stuck`'s 60 s, and the half-capacity prompt. No system read warns about stranded space, and the ordinary layouts are chosen by hand.
- **The `Unbalanced` calibration covers one disk addition from a converged table**: 200 layouts each for `rep2` and `rep3`, 3 to 6 nodes. Removals, refills after failures, several additions in a row and erasure-coded pools were not run. Test T has three layouts.
- **"Data follows the table"** is assumed for the claim that available + held and f do not depend on held. Data still being copied is held twice, which the `Unbalanced` gate excludes, but `Stranded` does not.
- **Two pools reprotecting at once** compete for the same room; each pool's `CannotReprotect` is computed alone, as the figures are.
- **The per-disk cap for shards** is reasoned (laminar capacity constraints) and computed by bisection; placement with those caps was not built.
- **Branch heads, not releases.** Ceph at `b7e67878` (2026-10-18), Vitastor at `df593bad` (2026-09-27), CockroachDB at `d30c905f` (2026-09-23).
- **The model is a port.** numpy was not available here, so [conditions.py](conditions.py) reimplements the spike's capped placement in plain Python. It reproduces the spike's available figures for C1 to C6 to the digit (test U, first lines).

## Reproduce

`./run.sh` runs the model into `results.txt`. It needs Python 3 and nothing else, and takes about 4 minutes; `python3 conditions.py S R O E E2 D T` alone takes a few seconds.

`./fetch.sh [dir]` downloads every cited file at the pinned commits into `dir` (default `./work`) and prints the cited lines. It needs `curl`.

## Sources

- Ceph at [`b7e67878`](https://github.com/ceph/ceph/tree/b7e6787889ed0783df44063506dcacaaf672758f): `src/osd/OSD.cc`, `src/osd/OSDMap.cc`, `src/mon/OSDMonitor.cc`, `src/mon/PGMap.cc`, `src/common/options/{global,mon,mgr}.yaml.in`, `src/pybind/mgr/balancer/module.py`, `src/pybind/mgr/pg_autoscaler/module.py`, `doc/rados/configuration/mon-config-ref.rst`, `doc/rados/operations/balancer.rst`.
- Vitastor at [`df593bad`](https://github.com/vitalif/vitastor/tree/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff): `mon/pg_gen.js`, `mon/lp_optimizer/lp_optimizer.js`, `mon/etcd_schema.js`, `mon/prometheus.js`, `docs/usage/cli.en.md`, `docs/config/monitor.en.md`.
- CockroachDB at [`d30c905f`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff): `pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go`, `pkg/roachpb/metadata.go`.
- rock-solid: [spikes/capacity-model](../../spikes/capacity-model/README.md) (layouts C1 to C6, and the placement this model ports), [studies/capacity-and-enospc](../capacity-and-enospc/README.md).
