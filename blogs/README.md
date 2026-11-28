# rock-solid, explained

A series of deep-dive posts on the design in [docs/design.md](../docs/design.md), written for backend, DevOps and SRE engineers who have run storage but never built it. Each post explains every idea twice: first in plain words, then how rock-solid actually does it. Each one covers the problem, the alternatives other systems chose, what rock-solid picked and why, what it costs, and what is still open.

Start with the first post; the others can be read in any order.

| # | Post | What it covers |
| --- | --- | --- |
| 01 | [The big picture](01-the-big-picture.md) | Goals, the gap in homelab RWX storage, the strategy, the architecture and the build order |
| 02 | [Chunks and placement](02-chunks-and-placement.md) | 4 MiB chunks, computed chunk IDs, versions, resends, placement groups and the placement table |
| 03 | [Epochs, leases and failures](03-epochs-leases-and-failures.md) | Changing a placement group's primary safely: intervals, peering, which writes survive, recovery |
| 03b | [Leases, failure detection and fencing](03b-leases-failure-detection-and-fencing.md) | Read leases on two clocks, suspends and VM pauses, failure and fail-slow detection, fencing paused clients |
| 04 | [One log per disk](04-one-log-per-disk.md) | The on-disk format: one append-only log, group commit, flushes, streams and crash recovery |
| 05 | [The cleaner and free space](05-the-cleaner-and-free-space.md) | Garbage collection in the log, write amplification, backpressure, TRIM and freeing space |
| 06 | [The LSM tree](06-the-lsm-tree.md) | The index inside the log: memtable, sorted runs, Bloom filters and compaction |
| 07 | [Pools and redundancy](07-pools-and-redundancy.md) | Copies versus erasure coding, failure domains, sealing, one-copy pools and rebuilds |
| 08 | [The MetaStore](08-the-metastore.md) | Filesystem metadata in many small Raft groups, transactions and leader leases |
| 09 | [RWX filesystem and cache coherence](09-rwx-filesystem-and-cache-coherence.md) | The FUSE client, close-to-open, per-file cache modes, sessions and restarts |
| 10 | [Block volumes, snapshots and clones](10-block-snapshots-and-clones.md) | ublk block volumes, crash-consistent snapshots, clones and S3 later |
| 11 | [Runtime in Go](11-runtime-in-go.md) | The sans-IO core, drivers, one event loop per core and zero allocations per operation |
| 12 | [Testing for trust](12-testing-for-trust.md) | Deterministic simulation, conformance to model-checked specs, crash oracles and fuzzing |

Terms are explained in each post as they come up; [docs/glossary.md](../docs/glossary.md) collects them in one place.

## Where the build stands

The posts describe the design, whether or not a part is built yet. The [roadmap](../docs/design.md#roadmap-and-open-questions) builds it in phases, and the [build checklist](../docs/build-checklist.md) tracks phases 1 to 6 item by item; the checklist, not this table, is the current record. As of 2026-11-28:

| Phase | What it builds | Posts | State |
| --- | --- | --- | --- |
| 1 | Sans-IO core, the disk log, the LSM index, the single-node ChunkStore, the simulator | 04, 05, 06, 11, 12 | Done |
| 2 | TLA+ models of epoch changes, sessions, recalls and purges | 03, 03b, 12 | Nearly done |
| 3 | Replication, placement, peering, read leases, failure detection | 02, 03, 03b | Done |
| 4 | The embedded MetaStore, sessions and space grants | 08 | In progress |
| 5 | Event loops, `io_uring` and kernel TCP | 11 | Nearly done |
| 6 | The FUSE client with close-to-open, the CSI driver, the Samba gateway: the first usable product | 09 | In progress |
| 7 to 14 | Cache modes, block volumes, SPDK and RDMA, erasure coding, snapshots, S3, scale-out, SQL | 07, 09, 10 | Not started |
