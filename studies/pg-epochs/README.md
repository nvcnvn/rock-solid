# PG epochs: checking requests per placement group, and the version's bit layout

**Status:** read from source plus a model spike ([spikes/pg-epochs](../../spikes/pg-epochs/README.md)). The proposed design at the end is adopted; [Where it is applied](#where-it-is-applied) lists the sections that hold the current text.

**Question:** two parts.

1. Any change to the placement table bumps one global epoch, and a member rejects a request tagged with an older one ([Placement table](../../docs/design.md#data-layer-chunkstore)), so every write in flight anywhere retries on every change, also in PGs whose membership did not change. When should a member that holds table `e'` still accept a request tagged `e < e'`, what should a version's epoch field then hold, and do peering, read leases, recovery of divergent versions, the suffix rule for merged blocks, fencing and snapshot freezes still hold? Should nodes get table deltas rather than whole tables?
2. A version is 64 bits: a 24-bit epoch over a 40-bit counter ([Versions](../../docs/design.md#data-layer-chunkstore)). Is that split right, given how fast epochs and counters are used, what happens when either runs out, and how does the answer change with per-PG checks? The layout is disk format.

## Summary

Ceph never rejects a request just because the cluster map moved on. A replica drops a replicated operation only if its PG started a new *interval* after the operation's map epoch (`can_discard_replica_op`, keyed on `last_peering_reset`), and a primary drops a client operation only if the PG's primary changed since (`same_primary_since`); a request from an older map is otherwise served, after the receiver has caught up to the sender's map. An interval is a run of maps in which the PG's acting and up sets, its primary, its pool's `size` and `min_size` and its PG count stay the same. Ceph's daemons work intervals out by walking every map in order, which is why they need every incremental map. Vitastor goes further and puts a per-PG epoch in the high 48 bits of every object version, over a 16-bit per-object counter whose overflow simply bumps that PG's epoch.

The proposal: **the placement table carries an interval number per PG, and a member accepts a request about a PG only when it carries that PG's current interval number.** A table change disturbs only the PGs whose members, member order, live marks, pool size or split it changed; everything else in it raises the epoch and leaves intervals alone. **A version becomes the PG's 32-bit interval number over a 32-bit counter per chunk**, reset in each interval. Per-PG interval numbers grow 3 to 35 times slower than table epochs in a homelab year and 400 to 480 times slower under a flapping node, and 32 bits of them outlast a node flapping every 10 seconds for 272 years, where 32 bits of table epochs ran out in 207 days and 24 bits in 19 hours in the spike's model. A 32-bit counter runs out at most once an hour per chunk at a million writes a second, which costs that one PG a new interval. Checking the safety argument turned up three things the current text needs whatever the check: peering must reach a member of *every* interval since the PG last activated, not only the previous one; a restarted primary must take a new interval, not peer in its old one; and members need a persisted *missing* list (Ceph's missing set) so a later peering knows what an interval that activated decided. Tables should be committed and pushed as deltas with a CRC of the result, but correctness does not depend on it.

## What the design says now

- **Versions** ([Data layer](../../docs/design.md#data-layer-chunkstore), "Versions"): "a 64-bit number whose high 24 bits are the epoch of the primary that assigned it and whose low 40 bits that primary counts up; a primary that runs out asks the control plane for a new epoch".
- **The table and its epoch** ("Placement table"): "Any change (pool created, disk added, node failed, rebalance) bumps the epoch, and nodes reject requests carrying a stale epoch"; "The epoch is simply the table's version". The leader commits each new table as one Raft entry, and every node caches the whole table, 24 KiB for 2,048 `rep3` PGs and 288 KiB for a 4,096-PG `ec16-2` pool.
- **Epoch changes** ("Writes"): a write tagged `e` completes only when every member live in `e` acknowledges; a member that adopted `e + 1` rejects anything tagged `e`; the new primary gets "at least one member that was live in `e` to adopt `e + 1`" and keeps a version "unless some member that was live in `e` lacks it". The versions it drops are a suffix of each chunk's pending versions, and partial-block writes wait until the new primary holds every kept version. "A primary that restarts peers again, in its own epoch or a new one, before it serves anything" ("Reads see only acknowledged writes").
- **Read leases** are per node pair with a *support epoch*, a different number; a grantor does not take part in "peering a new epoch that moves the primary away" until the lease lapses.
- **Fencing** releases a session's locks once every storage node acknowledged the session table "or has been marked down in a committed epoch, after which peering stops any write still tagged with the old epoch". The session table is versioned on its own: "not a placement epoch, which would make members reject every write in flight".
- **Snapshots**: freeze entries carry a volume's *snapshot epoch*, again a different number, and name a chunk version.
- **On disk** a version is an opaque `uint64` in every write, punch and freeze entry (`internal/disklog/record.go:284-294`; a write entry is 33 bytes) and in the design's copy entries, in every chunk index key, live or frozen (`internal/chunkstore/index.go:54-58`, 38 bytes), in every extent value (`index.go:113-118`, 26 bytes) and in each chunk's entry (`index.go:134-137`).
- **No code fixes the layout yet.** The 24/40 split in `internal/core/ids.go:22-27` is the *inode ID's* slot and sequence, not a version's. Nothing allocates versions: the build checklist's single-node ChunkStore item, which says "Versions carry the epoch in their high 24 bits, fixed here", is open. So the layout can still change without a migration.
- **Is the counter per chunk or per primary?** The design text and blog 03 ("A primary that runs out of its 40-bit counter (about 1.1 trillion writes)") read as one counter per primary. The examples read as one per chunk that restarts in a new epoch: chunk 7 at 41.7, the next write 41.8, the new primary's first 42.1, and "Version 41.9 of a block may therefore hold 41.8's bytes". Neither the code nor the checklist decides. The arithmetic below covers both.

## What other systems do

| System | When an older request is still valid | What a version holds | Epoch width | Counter |
| --- | --- | --- | --- | --- |
| Ceph RADOS | Replica: its PG has not started a new interval since the request's map epoch, after waiting for a map at least as new as the interval's start. Primary: the PG's primary has not changed since the client's map epoch | `eversion_t`: the primary's current map epoch and a per-PG counter | 32-bit map epoch, cluster-wide | 64-bit per PG, never reset; 12 bytes on disk |
| Vitastor | Not read here (its PG state lives in etcd under leases) | A per-PG epoch over a per-object counter | 48 bits, per PG | 16 bits per object, reset in each epoch; a full counter bumps the PG's epoch |
| rock-solid today | Never: any older table epoch is rejected | The assigning primary's table epoch over a counter | 24 bits, cluster-wide | 40 bits, per primary or per chunk (not decided) |

### Ceph

- **Intervals.** "A sequence of OSD map epochs during which the acting set and the up set for particular PG do not change" (`doc/dev/peering.rst:37-40`). `PastIntervals::is_new_interval` (`src/osd/osd_types.cc:4665-4721`) also counts the acting and up primaries, the pool's `size` and `min_size`, a split or merge of the PG and a few cluster flags. Each PG keeps `same_interval_since`, `same_primary_since` and `last_epoch_started` in its history (`src/osd/osd_types.h:2827-2844`), and `start_peering_interval` sets `same_interval_since` and the PG's `last_peering_reset` to the map epoch at which the interval changed (`src/osd/PeeringState.cc:628-632`, `:790-814`).
- **Replica operations.** A replicated write carries two epochs: `map_epoch`, the primary's current map, and `min_epoch`, the start of its interval (`src/osd/ReplicatedBackend.cc:1168-1174`). The receiving OSD queues it until its own map is at least `min_epoch` (`src/osd/OSD.cc:7684`; `PG::requeue_map_waiters`, `src/osd/PG.cc:1274-1293`), then drops it if the sender is down in its next map (`PG.cc:2043`, `:2052`) or "if pg changes _at all_" after `map_epoch` (`PG.cc:2058`, `old_peering_msg` at `:1868-1877`, which compares `last_peering_reset`). Taken together, a replica accepts exactly the operations of its current interval.
- **Client operations.** A primary drops a client's operation whose map epoch is older than `same_primary_since` (`PG.cc:1973`), a split, or the pool's `last_force_op_resend` (`PG.cc:1995-2020`); otherwise it serves it, and when its interval changes while it stays primary, it cancels its in-flight replicated writes and requeues their client operations instead of failing them (`PrimaryLogPG::on_change` calls `apply_and_flush_repops(is_primary())`, `src/osd/PrimaryLogPG.cc:13475`, `:13165-13185`). The client resends exactly the operations whose PG started a new interval in its map (`src/osdc/Objecter.cc:3246-3279`). The `OSD::require_same_or_newer_map` that Ceph's developer notes describe (`doc/dev/osd_internals/map_message_handling.rst:54`) survives at this commit as that `min_epoch` wait; the function itself is gone.
- **Peering across several intervals.** "If we can communicate with at least one member of each acting set since the last successful peering, someone will have a record of every (acknowledged) operation" (`peering.rst:113-121`). A new primary lists the past intervals since `last_epoch_started` and must reach one OSD of each that *may have gone read-write* (`peering.rst:139-160`). An interval whose acting set was smaller than `min_size` is never counted (`osd_types.cc:4871`). To skip intervals whose primary never activated, a primary has the monitors record `up_thru` in the map before it activates (`peering.rst:91-104`, `PeeringState.cc:1499-1510`), which costs a map epoch per peering; the comment above `check_new_interval` (`osd_types.cc:4791-4825`) explains the case it covers, an acting set `[A B]`, then `[B]`, then nothing, then `[A]`.
- **Versions.** `eversion_t` is a 64-bit `version` and a 32-bit `epoch` (`osd_types.h:838-841`), ordered by epoch, then version (`:890-892`), and encoded as 12 bytes (`osd_types.cc:1413-1415`). `get_next_version` takes the current map epoch and the PG's last version plus one (`src/osd/PG.h:1338-1345`): the counter is per PG and never resets. `epoch_t` is 32 bits, "13 epochs/second for 10 years" (`src/include/types.h:83`).
- **Incremental maps.** An OSD applies each incremental map to the one before it (`OSD.cc:8412-8440`), compares the result's CRC with the incremental's `full_crc` and asks for a full map on a mismatch (`OSD.cc:8454-8463`), and then advances every PG through every epoch in turn (`OSD::advance_pg`, `OSD.cc:9057-9064`), since intervals are computed map by map. Monitors keep at least 500 epochs (`mon_min_osdmap_epochs`, `src/common/options/global.yaml.in:1804`) and send up to 40 per message (`osd_map_message_max`, `:2961`).
- **Damping.** An OSD marked down 5 times within 10 minutes shuts itself down (`osd_max_markdown_count`, `osd_max_markdown_period`, `src/common/options/osd.yaml.in:927-937`), and a down OSD is marked out, which moves its PGs, only after 10 minutes (`mon_osd_down_out_interval`, `src/common/options/mon.yaml.in:1079`).

### Vitastor

- `PG_EPOCH_BITS` is 48 (`src/osd/osd_peering_pg.h:15`); the epoch "should increase with each non-clean activation of the PG" (`:94`). Peering sets it to the largest epoch found in any object version and adds one unless the PG is active (`src/osd/osd_peering_pg.cpp:426-449`).
- A write takes `(epoch << 16) | 1` when the object's version is from an older epoch, and the version plus one otherwise; when the 16-bit counter is full it bumps the PG's epoch (`src/osd/osd_primary_write.cpp:202-213`), and it reports a new epoch to etcd before writing with it (`:215-240`, `src/osd/osd_cluster.cpp:1135-1146`). So a counter overflow every 65,535 writes to one object is a routine, PG-local event.

## Findings

### 1. When a request from an older table is still valid

**The rule.** For a request about PG `p`, tagged with table `e`, reaching a member that holds `e' > e`: it is valid exactly when no table in `(e, e']` started a new interval of `p`, where a new interval starts when a table changes `p`'s ordered member list (so its primary), a member's live mark (down, up, `maintenance`), its pool's `replicas` or `min_live`, or splits `p`. Pool creation, other PGs' entries, `write_limit`, the session table and the format level change nothing a member of `p` checks, so they must not end `p`'s interval.

**Endpoints are not enough.** Comparing `p`'s members in `e` and `e'` fails when a member is removed and added back: the lists match, but writes of the interval between were never sent to it, and an old primary's write tagged `e` would collect its acknowledgment. Ceph avoids this by walking every map. The control plane here already computes each table from the previous one, so it can number `p`'s intervals and put the number in the table. Then a member compares two numbers, a node that skipped tables loses nothing, and the rule needs no history on the node: accept when the request's interval number equals `p`'s in the member's table, refuse a lower one, and catch up to the sender's table before judging a higher one.

**Client writes.** A daemon's write carries no state that depends on `p`'s replicas, only on its primary. A primary can therefore take a client's write whose interval number is at least the one in which its own term as primary began, as Ceph's `same_primary_since` rule does, and order again after re-peering the writes it held when an interval ended under it. A client sending to the wrong primary is refused with the current number and fetches the table.

**What it saves** ([spike](../../spikes/pg-epochs/README.md), Y, F):

| Cluster, staging | Tables in a year | Mean share of PGs a table changes | Writes disturbed, global check ÷ per-PG |
| --- | --- | --- | --- |
| 10 nodes, one table per step | 778 | 22% | 4.4× |
| 10 nodes, one table per moved member | 4,277 to 8,226 | 2 to 4% | 24 to 46× |
| 3 nodes, one table per step | 270 | 71 to 74% | 1.35× |
| 3 nodes, one table per moved member | 4,030 to 12,748 | 2 to 5% | 20 to 63× |

On 3 nodes every PG has a member on every node, so a reboot changes every PG and per-PG checks save nothing for node events; they still isolate disk events, rebalancing, counter bumps and restarts. Under per-PG checks, staging moves one at a time costs no extra disturbed writes, since the PGs' interval changes are the same either way, about 175 per PG a year on 10 nodes; under the global check each staged table disturbs every write.

### 2. What the version's high field should hold

Three candidates, all of which keep versions unique (one primary per interval) and growing within a chunk:

- **The epoch the request was tagged with** (Ceph's choice). Two versions from one interval then carry different high fields whenever unrelated PGs change, the field burns at the cluster-wide table rate, and finding the interval that assigned a version, which the keep rule needs, takes a lookup by epoch range. Ceph can afford it because its 64-bit per-PG counter alone already orders a PG's writes.
- **The table epoch at which the interval began** (`same_interval_since`). One value per interval, but still a table epoch, so it burns at the table rate too.
- **The PG's interval number.** One value per interval, and it grows only when the PG itself changes.

The interval number wins on the bit budget (section 6). It needs one rule the epochs gave for free: when PG `p` splits into `2p` and `2p + 1`, both children start at `p`'s interval plus one, so a chunk's versions keep growing as it changes PG. A [pool move](../../docs/design.md#moving-a-volume-to-another-pool) gives a chunk a new PG in another pool with versions that start afresh; nothing compares versions across pools (reads fall through at a snapshot epoch, and an `expect_ver` from the old pool fails, which is correct).

### 3. Peering and its safety argument

The argument still goes "a write tagged `i` completes only when every member live in `i` acknowledges it; a member that adopted a later interval refuses it". Three gaps showed up, two of which the current text has with global epochs as well.

**Every interval since the last activation, not only the previous one.** Tables can be committed faster than PGs peer. If `p` changes interval at `e + 1` and again at `e + 2` before the primary of `e + 1` peers, the primary of `e + 2` that gets a member live in `e + 1` to adopt has not stopped writes tagged `e` unless that member was also live in `e`. Ceph's rule is the general one: one member of each interval since `last_epoch_started` that may have accepted writes. Here an interval may have accepted writes only if its primary activated, and only with at least `min_live` live members. Without Ceph's `up_thru`, which costs a table per peering, the primary cannot know whether a past interval's primary activated, so it treats every interval since the last one it knows activated as possibly written, skipping those with fewer than `min_live` live members, and waits if it cannot reach a member of one. To know that list, each PG's table entry carries its last activated interval, which primaries report on their heartbeats and the leader folds into a later table, and the member lists of the intervals since. That change raises the epoch and starts no interval. Normally the list is just the previous interval.

**A restarted primary needs a new interval.** Peering "in its own epoch" after a restart is unsafe: a replicated write the previous run sent can sit in a member's socket buffer, land after the member answered peering, and carry a version the restarted primary then gives to other data, two contents under one version. Taking a new interval makes members refuse the old run's messages once they answer peering. With per-PG intervals this costs one table entry touching only the PGs the node leads; with the global check it would disturb the cluster at every restart.

**What an activated interval decided must survive it.** The keep rule ("keep a version unless some member live in `e` lacks it") assumes the members reached hold everything the previous interval had decided. They need not. Interval `L` peers, keeps a pending version `v` of chunk `X` and starts recovering it onto a new member; `L` then ends before recovery finishes, and the next primary reaches only members live in `L`, some of which lack `v`. If `L` had already acknowledged a write merged on `v`, dropping `v` would lose acknowledged bytes; if an older member that `L` never reached still lacks `v`, the rule applied to `v`'s own interval would drop what `L` kept. Ceph keeps this decision in each member's PG log and missing set: activation sends every member the authoritative log, and a member records the objects it still lacks. The equivalent here is a persisted *missing list*: before a primary activates, every member records, for each chunk it lacks or holds a dropped version of, the kept version it must get, and recovery removes entries. Then the next peering starts from the newest activated interval `L`: members live in `L` know, from their logs and missing lists, the kept version of every chunk; a version assigned in `L` is kept unless some member live in `L` neither holds it nor lists it; a version below a kept one is kept; a chunk whose kept version no reachable member holds is reported unfound, never replaced by an older one. This is a candidate rule; the phase 2 model has to confirm it.

**The suffix rule is unchanged.** One primary per interval sends a chunk's writes in version order and each member logs them in that order, so the versions of one interval that peering drops are a suffix of that interval's versions of the chunk; versions of a later interval exist only on top of what that interval's peering kept, and "a version below a kept one is kept" makes the whole dropped set a suffix. A new primary still merges a partial block only once it holds every kept version.

### 4. Read leases, fencing and snapshots

- **Read leases.** Leases stay per node pair. A grantor does not take part in peering a new interval of a PG that moves its primary away from the holder until the lease lapses or the holder adopted that interval. A new interval that keeps the primary (a replica replaced, a counter bump) needs no lease wait; the primary keeps serving reads of committed versions, since no other node can commit to the PG, and holds writes until it has re-peered. Only the newest activated interval's primary can have served reads, and peering reaches a member live in that interval, which granted it a lease, so the multi-interval rule changes nothing here.
- **Fencing.** Marking a storage node down changes, in that same table, the interval of every PG with a member on it, so "marked down in a committed epoch" becomes "marked down in a committed table" with the same effect: every such PG peers again and its old-interval writes stop. PGs without a member on that node have acknowledged the fence on every member, and the fence is checked per write by every storage node, replicas included, independently of intervals. The session table can stay its own version; the reason "a placement epoch would make members reject every write in flight" no longer holds, but a separate version still keeps the acknowledged watermark about fences alone.
- **Snapshots.** Freeze entries carry a volume's snapshot epoch and name a version as an opaque number; neither depends on the table epoch. Interval numbers and counters start at 1, so version 0 stays the absent chunk and the live map's key.

### 5. Deltas instead of whole tables

Ceph needs every incremental map because its daemons compute intervals map by map. Here each PG entry carries its interval number, so a node can jump from any table to the newest, and deltas are only a saving: the PG entries a change touched (about 29 bytes each for `rep3`), plus a header with the epoch and the CRC32C of the resulting table, as Ceph's `full_crc`. In the spike, a year on 10 nodes pushed 399 MB of whole tables with one table per step and 2.25 GB with one per moved member, against 105 MB of deltas; on 3 nodes 41.5 against 34.9 MB, since node events change every PG. The volume is small either way; the burst is not: one flap cycle with staged refills commits 1,202 tables, 61 MB of whole tables to each node and to the control plane's Raft log on each voter.

Costs: a delta codec with a fuzz test, a node that holds `e − 1` applies the delta and checks the CRC and fetches the whole table on a gap or mismatch, and the leader keeps recent deltas or just sends the whole table to anyone further behind. The Raft entries should hold deltas in any case, with the whole table in Raft snapshots, since the log is written three times and replayed.

### 6. Bit budget

**Epoch field, if it holds table epochs** ([spike](../../spikes/pg-epochs/README.md), X):

| Scenario | Table epochs a year | 24 bits last | 32 bits last |
| --- | --- | --- | --- |
| Homelab year, 10 nodes, one table per step | 778 | 21,565 years | millions |
| Same, one table per moved member, two per move | 8,226 | 2,040 years | 522,121 years |
| One node flapping every 10 s, no refill | 6.3 million | 2.7 years | 680 years |
| Flapping every 10 s, refill and balance back staged per member | 7.6 billion | **19 hours** | **207 days** |
| Same, damped to 5 flaps per 10 minutes | 316 to 632 million | 10 to 19 days | 6.8 to 14 years |
| A fail-slow disk flapping at its limit | 526,000 | 32 years | 8,166 years |

**Epoch field, if it holds the PG's interval number** (the busiest PG):

| Scenario | Intervals a year | 24 bits | 28 bits | 32 bits |
| --- | --- | --- | --- | --- |
| Homelab year, any staging | 196 to 237 | 70,000 years | 1.1 million | 18 million |
| Flapping every 10 s, no refill | 6.3 million | 2.7 years | 43 years | 680 years |
| Flapping every 10 s, staged refill, two tables per move | 15.8 million | **1.1 years** | 17 years | **272 years** |
| Damped, staged refill | 0.8 to 1.3 million | 13 to 21 years | 204 to 340 years | 3,266 years or more |

The design has neither a refill delay nor flap damping (only fail-slow disks are damped), so the undamped rows are reachable today by a node whose network drops heartbeats for 5 seconds at a time.

**Counter field.** The rate that matters depends on what the counter counts:

| Writes/s | Where | 32 bits | 36 bits | 40 bits |
| --- | --- | --- | --- | --- |
| 1,000 | One hot chunk of a homelab database | 50 days | 2.2 years | 35 years |
| 20,000 | One chunk, fsync-bound, 20 in flight | 2.5 days | 40 days | 1.7 years |
| 150,000 | One primary disk at 10 GbE, rep3 | 8 hours | 5.3 days | 85 days |
| 300,000 | One primary disk, CPU-bound loop | 4 hours | 2.7 days | 42 days |
| 1,000,000 | Upper bound of the loops, one chunk or one disk | 1.2 hours | 19 hours | 12.7 days |

A counter **per primary** at 40 bits runs out every 6 to 12 weeks on a busy NVMe disk; with 30 such disks and the global check, that is a cluster-wide retry every one to three days from counters alone. A counter **per chunk**, reset in each interval, needs nothing beyond what the primary already keeps (the chunk's newest ordered version, which merges and freezes already read), and runs out only for a single chunk written millions of times in one interval.

**At exhaustion.**

- *Counter:* the primary holds that chunk's writes, lets the PG's pending writes commit, asks for a new interval of the PG and re-peers, dropping nothing, then continues at `(i + 1, 1)`. Under per-PG intervals this pauses one PG's writes for about a control-plane commit and a peering round trip; the number of such bumps is at most a node's writes ÷ 2³², one an hour per node at a million writes a second. Under the global check each one disturbs every write in the cluster.
- *Interval or epoch field:* nothing can assign a larger version, so the PG (per-PG field) or the whole cluster (table epoch field) can no longer change membership or recover. The only way out is to renumber every version of every chunk on every member while the PG is clean, rewriting index keys, extents and frozen maps and invalidating every `at_ver` a backup holds: an offline format migration. So that field must be sized never to run out, and the control plane should warn at half.

**128-bit versions.** A write or punch entry grows from 33 to 41 bytes and a freeze from 37 to 45. Records are whole blocks, so header blocks grow only for rounds of 90 to 109 or 199 to 220 one-block writes, 0.2% more blocks over all round sizes, but headers are capacity near full, and 22% more header bytes per entry is the wrong direction. Every extent value grows from 26 to 34 bytes and each chunk entry by 8; for a block volume at 270 million extents per TB that is about 2.2 GB more in memory per TB. Ceph pays 12 bytes because its counter is per PG and never resets; with a counter that resets per interval and per chunk, 64 bits suffice.

**Options:**

| Layout | Field holds | Interval or epoch field lasts, undamped flapping, worst staging | Counter runs out at 1 M writes/s per chunk | Verdict |
| --- | --- | --- | --- | --- |
| 24 + 40 (today) | Table epoch | 19 hours | 12.7 days | Rejected: the field burns at the cluster's rate |
| 32 + 32 | Table epoch at the interval's start | 207 days | 1.2 hours | Rejected, same reason |
| 24 + 40 | PG interval number | 1.1 years (13 years damped) | 12.7 days | Rejected: survives only with damping that does not exist |
| 28 + 36 | PG interval number | 17 years | 19 hours | Viable; less headroom where running out is fatal, for fewer bumps where a bump is cheap |
| **32 + 32** | **PG interval number** | **272 years** | **1.2 hours** | **Proposed** |
| 64 + 64 | Either | Never | Never | Rejected: 8 more bytes per entry and extent for no gain |

## Proposed design

### Mechanism

1. **Intervals.** Every PG entry in the placement table carries an interval number, starting at 1. The leader gives a PG a new interval, its number plus one, in a table that changes the PG's ordered member list, a member's live mark (down, up, `maintenance`), its pool's `replicas` or `min_live`, or splits it (both children take the parent's number plus one), and when the PG's primary asks because a chunk's counter ran out or it restarted. Nothing else starts an interval. Every table still has an epoch, now a `uint64` that lives only in the control plane and on the wire.
2. **The check.** Every message about a PG carries the sender's table epoch and the PG's interval number in it. A member accepts a message when the interval number equals the PG's in its own table, refuses a lower one with its own epoch and number, and fetches a table at least as new as the sender's before it judges a higher one. A primary accepts a client's write when the client's number is at least the one in which its term as primary began, and orders again, after it re-peers, the client writes it held when an interval ended under it.
3. **Versions.** `version = interval << 32 | counter`. A primary gives a chunk's write the chunk's newest ordered version plus one, pending versions included, if that version is of the current interval, and `(interval, 1)` otherwise. At `2³² − 1` it holds the chunk's writes and asks for a new interval of the PG, letting the PG's pending writes commit first so the re-peer drops nothing.
4. **Peering a new interval `k`.** The primary gets, for every interval of the PG since its last activated interval that had at least `min_live` live members, one member live in it to adopt a table with interval `k`, and waits if it cannot. It collects every member's newest versions and missing list, keeps a version of the last activated interval `L` unless a member live in `L` neither holds nor lists it, takes earlier versions as `L`'s peering decided them, keeps every version below a kept one, and reports a chunk whose kept version no reachable member holds as unfound. Before it activates, every member persists its missing list, the kept version of each chunk it lacks or holds a dropped version of; recovery removes entries. The primary reports activation on its heartbeat, and the leader records it as the PG's last activated interval, trimming the past intervals the entry lists, in a later table that starts no interval.
5. **Restarts.** A primary that restarts asks for a new interval of each PG it leads and peers in it before serving.
6. **Tables as deltas.** The leader commits each table as a Raft entry holding the PG entries it changed and the CRC32C of the resulting table; Raft snapshots hold the whole table. A node holding the previous table applies the delta and checks the CRC; on a gap or mismatch it fetches the whole table.

### Bit layout

```text
 63                                32 31                                 0
+------------------------------------+------------------------------------+
| interval number of the chunk's PG  | the chunk's writes in that         |
| when its primary ordered the write | interval, counted from 1           |
+------------------------------------+------------------------------------+
  0 = the absent chunk and the live map's key; (1, 1) = 0x0000000100000001
```

### Why it is safe

1. **Old intervals stop.** A write tagged with interval `j` of PG `p` completes only when every member live in `j` acknowledges it, and a member acknowledges only while `p`'s interval in its table is `j`. Tables never go backwards on a node (a restarted node fetches the newest before it answers), and interval numbers never go backwards within a table sequence, so a member that adopted interval `k > j` never acknowledges `j` again. Peering `k` gets such a member for every interval since the last activated one that could have accepted writes; the intervals before that were stopped by the activated interval's own peering, by induction. So once `k` activates, no write tagged with an earlier interval completes.
2. **Nothing acknowledged is lost.** An acknowledged write of interval `j` is on every member live in `j`, including the one peering reached for `j`, or, for intervals before the last activated one `L`, it was kept by `L`'s peering and is held or listed as missing by every member live in `L`. Dropped versions were never acknowledged, are a suffix of their chunk, and are superseded by recovery in every log that holds them, as today.
3. **One version, one content.** Each interval has one primary, which numbers a chunk's writes from what peering left plus one; a restarted primary works in a new interval, so it never reuses a number its previous run sent. Interval numbers grow across primaries and across splits, so a stale member's write is told apart by its version.
4. **Unchanged PGs are untouched.** A table that does not start a new interval of `p` changes nothing a member of `p` checks, so its writes keep completing; this is what makes the check cheaper, not what makes it safe.

### Costs

- **Table:** 4 bytes per PG for the interval number and 4 for the last activated interval, plus the member lists of unsettled past intervals, usually none: 24 KiB grows to about 40 KiB for 2,048 `rep3` PGs.
- **Wire:** a `uint64` epoch and a `uint32` interval per message; the check is one comparison against an array indexed by PG, as today's epoch check.
- **Control plane:** a table entry per primary restart (touching that node's PGs), per counter bump (at most hourly per node at the loops' limit), and per batch of activation reports. None changes another PG's interval.
- **Peering:** one more member to reach per interval that ended before it activated, normally none; one persisted missing list per member before activation.
- **Counter bumps:** one PG's writes paused for a control-plane commit and a peering round trip, at most once an hour per hot chunk.

### Alternatives rejected

| Alternative | Who uses it | Why not |
| --- | --- | --- |
| Keep the global check | rock-solid today | Every table disturbs every write in flight: 4.4 to 46 times more disturbed writes in a homelab year on 10 nodes, and counter bumps and restarts become cluster-wide events |
| Compare the PG's members in the two tables | — | A member removed and added back looks unchanged, and its acknowledgment would complete an old interval's write |
| Derive intervals on each node by walking every table | Ceph | Makes every intermediate table mandatory on every node; the control plane already computes each table from the last |
| Record each primary's activation in the table before it activates, to skip intervals that never activated | Ceph `up_thru` | A table commit on every peering's critical path; skipping intervals with fewer than `min_live` live members and waiting for the rest is enough at 3 to 10 nodes |
| The tag's table epoch in the version, with a never-reset counter per PG | Ceph `eversion_t` (12 bytes) | Burns at the cluster's table rate and needs a wider counter; 32 bits of table epochs ran out in 207 days in the worst modeled case |
| The table epoch at the interval's start in the version | — | One value per interval, but still a table epoch, with the same burn rate |
| Keep 24 + 40 with interval numbers | — | 24 bits survive undamped flapping for 1.1 to 2.7 years, and the design does not damp nodes |
| 28 + 36 | — | Viable; gives up 16 times the headroom on the field whose exhaustion is fatal for fewer bumps of the field whose exhaustion is cheap |
| 128-bit versions | Ceph pays 96 bits | 8 more bytes per record entry (33 to 41) and per extent (26 to 34), about 2.2 GB more index memory per TB of block volume, for headroom 64 bits already give |
| A counter per primary | The design's wording | Runs out every 6 to 12 weeks on a busy disk; a counter per chunk needs no extra state |
| Interval numbers with a 16-bit counter | Vitastor | A hot chunk would bump its PG every few seconds to a minute; a bump here is a table entry and a re-peer, not one etcd write |
| Peer a restarted primary in its old interval | The design's "in its own epoch" | A write its previous run left in a socket can land after peering under a version given to other data |

### What the phase 2 model and the simulator must check

**Epoch change model (TLA+):** two PGs sharing three or four nodes, so a table that changes one is seen by the other's members; nodes that adopt tables monotonically and may skip some; messages tagged with an epoch and an interval number, and the acceptance rule with its catch-up. Faults: a member removed and added back; two interval changes before the first new primary peers; an interval with fewer than `min_live` live members; a replica replaced under a primary that stays; a primary restart; a counter running out; a split with writes in flight; an activation report lost or late; recovery cut short by the next interval. Properties: no write tagged with an interval completes after a later interval of its PG activated; no acknowledged version is dropped, and no version an activated interval kept is dropped later; the versions dropped are a suffix of each chunk's pending versions; no two writes of a chunk get one version, and a chunk's versions grow; a table that leaves a PG's interval alone never fails that PG's writes; no PG serves reads from two primaries; no node's table epoch or interval number goes backwards. Counterexamples it should find: blocking only the previous interval; comparing member lists; a restarted primary peering in its old interval; deciding an older version by its own interval's members after a later interval kept it; activating before the missing lists are durable.

**Simulator:** the same invariants after every step, plus a count of writes refused for their tag while their PG's interval did not change, which must be zero. Faults: interval changes in bursts faster than peering, a node flapping every few seconds, a primary killed and restarted within `node_down_after`, a counter set close to its limit, a PG split under load, and a member removed and added back within one recovery.

### Where it is applied

The proposal is adopted; the design documents hold the current text, and this list says where.

- **`docs/design.md`**, [Data layer](../../docs/design.md#data-layer-chunkstore): "Versions" (the 32 + 32 layout, the counter per chunk, why not table epochs or 128 bits), "Reads see only acknowledged writes" (a restarted primary takes a new interval), the merge paragraph (every version below a kept one is kept), and "Placement table" (per-PG intervals in the table, tables committed and pushed as deltas with the resulting table's CRC32C).
- [Epoch changes and failure detection](../../docs/design.md#epoch-changes-and-failure-detection): the opening (what starts an interval; the table, support and snapshot epochs named apart), "Requests are checked against their PG's interval", "Writes" (every interval since the last activated one), "Which versions survive" (missing lists, the candidate keep rule), "Recovery", "Restarts", "Reads" (leases per interval; support epochs), "Failure detection" and "Fail-slow".
- [On-disk format](../../docs/design.md#on-disk-format), "Headers are capacity": the version's 8 bytes in each entry. [Pools](../../docs/design.md#pools-and-redundancy): a `replicas` change starts new intervals. [Moving a volume](../../docs/design.md#moving-a-volume-to-another-pool): versions follow the new PG; the cut and read-through are by snapshot epoch.
- [Sessions and fencing](../../docs/design.md#sessions-and-fencing): "marked down in a committed table", fences checked on every write in PGs whose interval did not change, and why the session table keeps its own version.
- [Block frontend](../../docs/design.md#block-frontend) and [Snapshots and clones](../../docs/design.md#snapshots-and-clones): writes tagged with the PG's interval; snapshot epochs named as such.
- [Testing strategy](../../docs/design.md#testing-strategy): the per-PG interval faults and invariants.
- [Alternatives considered](../../docs/design.md#alternatives-considered): rows "Stale requests" and "Version layout" (replacing "Chunk versions"), and "Placement", "Snapshot cut" and "Snapshot epoch of a chunk's first write" reworded.
- [Roadmap and open questions](../../docs/design.md#roadmap-and-open-questions): phases 2 and 3, the incarnation question reworded, and a new open question on the keep rule across several intervals.
- [Appendix: PG epochs spike](../../docs/design.md#appendix-pg-epochs-spike).
- **`docs/glossary.md`**: "Version", "Snapshot" (snapshot epoch), "Epoch, table epoch", "Interval" (new), "Failure detection, marked down", "Peering" (missing list), "Support epoch", and the "Epoch" and "Interval" rows of "Words with more than one meaning".
- **`docs/build-checklist.md`**: single-node ChunkStore (the layout), epoch change model, wire messages, placement computation, replica primary, peering and read leases, placement faults and invariants, control-plane group.
- **Blog posts** [02](../../blogs/02-chunks-and-placement.md) (the table, its intervals and deltas, the version layout and why a plain counter fails), [03](../../blogs/03-epochs-leases-and-failures.md) (table epochs and intervals, the per-PG check, peering across intervals, missing lists, restarts, the lease and fencing races), [07](../../blogs/07-pools-and-redundancy.md), [08](../../blogs/08-the-metastore.md), [09](../../blogs/09-rwx-filesystem-and-cache-coherence.md), [10](../../blogs/10-block-snapshots-and-clones.md) (snapshot epochs named) and [12](../../blogs/12-testing-for-trust.md).
- `docs/test-scenarios.md` is unchanged: no section covers versions yet. When the version helper lands, the section of its checklist item starts from: `(1, 1)` packs to `0x0000000100000001` and orders below `(2, 1)` whatever the counters; interval 0 and counter 0 are refused except as version 0.
- **Not decided:** damping a flapping node (Ceph stops an OSD after 5 markdowns in 10 minutes) and delaying refill after a node goes down (Ceph waits 10 minutes). The design states the cost of their absence in "Failure detection"; with 32-bit intervals neither is needed for the bit budget.

## Unverified

- **The keep rule across intervals and the missing list are prose.** Section 3's rule is a candidate built on Ceph's missing sets; the phase 2 model has to check it, including recovery cut short by the next interval and a member that `L`'s peering never reached.
- **Write rates** per chunk and per disk are estimates from the design's measurements (one loop's throughput flat up to 100,000 writes/s, about 30 µs of CPU per write) and from network limits, not measurements of one chunk.
- **The model's event rates are assumptions** (24 reboots per node a year, 2 long outages, 2 disk failures); erasure-coded pools, real recovery times and refill delays are not modeled, and the design does not say whether refill waits.
- **Vitastor's stale-request check** was not read; only its version and epoch code.
- **Ceph was read at a branch head**, `b7e6787`, as in the other studies; Vitastor at `df593ba`. `OSD::require_same_or_newer_map`, which the Ceph design notes cite, no longer exists at that commit; its role is the `min_epoch` wait cited above.
- **No public figure** was found for how fast real Ceph clusters' OSD map epochs grow.

## Reproduce

`./fetch.sh [dir]` downloads every cited Ceph and Vitastor file at the pinned commits into `dir` (default `./work`) and prints the cited lines. Needs `curl`. The model: `spikes/pg-epochs/run.sh`, about 2 minutes, no root.

## Sources

- Ceph at [`b7e6787`](https://github.com/ceph/ceph/tree/b7e6787889ed0783df44063506dcacaaf672758f): [`doc/dev/peering.rst`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/doc/dev/peering.rst), [`doc/dev/osd_internals/map_message_handling.rst`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/doc/dev/osd_internals/map_message_handling.rst), [`src/include/types.h`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/include/types.h), [`src/osd/osd_types.h`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/osd_types.h), [`src/osd/osd_types.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/osd_types.cc), [`src/osd/PG.h`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PG.h), [`src/osd/PG.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PG.cc), [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PrimaryLogPG.cc), [`src/osd/PeeringState.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PeeringState.cc), [`src/osd/ReplicatedBackend.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/ReplicatedBackend.cc), [`src/osd/OSD.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/OSD.cc), [`src/osdc/Objecter.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osdc/Objecter.cc), [`src/common/options/global.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/global.yaml.in), [`src/common/options/osd.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/osd.yaml.in), [`src/common/options/mon.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/mon.yaml.in).
- Vitastor at [`df593ba`](https://github.com/vitalif/vitastor/tree/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff): [`src/osd/osd_peering_pg.h`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/src/osd/osd_peering_pg.h), [`src/osd/osd_peering_pg.cpp`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/src/osd/osd_peering_pg.cpp), [`src/osd/osd_primary_write.cpp`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/src/osd/osd_primary_write.cpp), [`src/osd/osd_cluster.cpp`](https://github.com/vitalif/vitastor/blob/df593badb5dfdbfbacac97b0c310f6ea3d5c3dff/src/osd/osd_cluster.cpp).
- rock-solid: [`internal/core/ids.go`](../../internal/core/ids.go), [`internal/disklog/record.go`](../../internal/disklog/record.go), [`internal/chunkstore/index.go`](../../internal/chunkstore/index.go), [chunk epochs spike](../../spikes/chunk-epochs/README.md), [pg-epochs spike](../../spikes/pg-epochs/README.md).
