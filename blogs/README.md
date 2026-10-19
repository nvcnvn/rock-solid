# rock-solid, explained

A series of deep-dive posts on the design in [docs/design.md](../docs/design.md), written for backend, DevOps and SRE engineers who have run storage but never built it. Each post explains every idea twice: first in plain words, then how rock-solid actually does it. Each one covers the problem, the alternatives other systems chose, what rock-solid picked and why, the measurements behind it, what it costs, and what is still open.

rock-solid is in early development and nothing runs yet. The posts say which parts exist in code today and which are planned.

Start with the first post; the others can be read in any order.

| # | Post | What it covers |
| --- | --- | --- |
| 01 | [The big picture](01-the-big-picture.md) | Goals, the gap in homelab RWX storage, the strategy, the architecture and the roadmap |
| 02 | [Chunks and placement](02-chunks-and-placement.md) | 4 MiB chunks, computed chunk IDs, versions, placement groups and the placement table |
| 03 | [Epochs, leases and failures](03-epochs-leases-and-failures.md) | Changing a placement group's primary safely: peering, read leases, failure detection and fencing |
| 04 | [One log per disk](04-one-log-per-disk.md) | The on-disk format: one append-only log, group commit, flushes, streams and crash recovery |
| 05 | [The cleaner and free space](05-the-cleaner-and-free-space.md) | Garbage collection in the log, write amplification, backpressure, TRIM and freeing space |
| 06 | [The LSM tree](06-the-lsm-tree.md) | The index inside the log: memtable, sorted runs, Bloom filters and compaction |
| 07 | [Pools and redundancy](07-pools-and-redundancy.md) | Copies versus erasure coding, failure domains, sealing, one-copy pools and rebuilds |
| 08 | [The MetaStore](08-the-metastore.md) | Filesystem metadata in many small Raft groups, transactions and leader leases |
| 09 | [RWX filesystem and cache coherence](09-rwx-filesystem-and-cache-coherence.md) | The FUSE client, close-to-open, per-file cache modes, sessions and restarts |
| 10 | [Block volumes, snapshots and clones](10-block-snapshots-and-clones.md) | ublk block volumes, crash-consistent snapshots, clones and S3 later |
| 11 | [Runtime in Go](11-runtime-in-go.md) | The sans-IO core, drivers, one event loop per core and zero allocations per operation |
| 12 | [Testing for trust](12-testing-for-trust.md) | Deterministic simulation, crash oracles, fuzzing and metamorphic tests |

Terms are explained in each post as they come up; [docs/glossary.md](../docs/glossary.md) collects them in one place.
