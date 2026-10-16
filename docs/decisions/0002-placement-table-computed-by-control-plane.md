# 0002. Place placement groups with a table the control plane computes

Status: Accepted (2026-10-16)

## Context
Chunks hash into placement groups (PGs), and something must map each PG to its member disks. Because there are PGs, a full table is small: 24 KiB for 2048 PGs × 3 replicas, 288 KiB for a 4096-PG `ec16-2` pool, whatever the amount of data. There are two ways to build it:

- **A table** computed by the control-plane Raft leader, versioned by an epoch and cached by every node.
- **A function** every node evaluates (Ceph CRUSH, Weka's algorithmic ownership), so no table needs to be distributed.

The target is 3 to 10 nodes with mixed disk sizes. On clusters that small, pseudo-random placement leaves disks noticeably unbalanced. Ceph ended up adding `pg-upmap` exception tables and a balancer that writes them on top of CRUSH, so in practice it runs a table too. Placement changes during recovery are where the hardest bugs live, so the mechanism has to be easy to simulate and model-check.

## Decision
The control-plane leader computes the placement table and commits each new version as one Raft entry with a new epoch. Nodes never compute placement. They read the table and reject requests that carry a stale epoch.

The new table is computed from the previous one, never from scratch. It is a pure function in the sans-IO core:

1. Refill every missing member onto the least-full eligible disk.
2. While a disk is outside a fill margin of its capacity-weighted share, move one member from the fullest disk to the emptiest eligible one.

Both steps keep the pool's failure-domain rule.

## Consequences
- Balance is exact even on three nodes with mismatched disks, and each table change moves only the members the algorithm chose.
- Moves can be staged and throttled, and operators can drain a disk or pin a PG by editing the table.
- The epoch is simply the table's version, which keeps simulation and `stateright` models simple.
- Every node must receive the table. It is a few hundred KiB at most and changes only on pool, disk and node events.
- One implementation decides all placement, so it carries the placement property tests.
- At thousands of disks, the table and its recomputation would grow enough to revisit computed placement in a new ADR. That is outside the target.
