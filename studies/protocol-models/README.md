# Protocol models: which designs to model, when, and how deep

**Status:** read from source and papers, plus TLC runs of CockroachDB's and etcd-raft's published models on one machine ([results.txt](results.txt)). Adopted in the design: the `ci`, `deep` and nightly tiers in Testing strategy, the epoch change model as one spec with `ci-leases` and `ci-peering` configurations, the incarnation rules in Epoch changes and Daemon restart and upgrade (still for the model to confirm, under open questions), the phase 11 and phase 13 models in the roadmap, the refusal of a `Txn` spanning two Raft groups until phase 13, and the TLC 1.7.4 pin in tech-debts.md. The paused node's resume rule went into Snapshots and clones as a constraint, leaving the conditional commit to the phase 11 model.

**Question:** design.md's Testing strategy, layer 2, lists TLA+ models of epoch changes, sessions and fencing, cache-mode recalls, purges, snapshot cuts and cross-shard commit, but the roadmap's phase 2 and the build checklist build only the first four, and no phase schedules the last two. test-scenarios.md leaves open each model's depth, the CI time budget and whether deeper runs go nightly. Which of rock-solid's protocols are riskiest to get wrong, judged by how comparable systems found their bugs? What do CockroachDB and etcd-raft model, and what did modelling find for them, for AWS, MongoDB and Azure Cosmos DB, and what does Ceph do without models? What sizes and run times are practical with TLC? When should the snapshot-cut and cross-shard-commit models be written, and do the open questions "Incarnation fencing" and "Which PGs a node-pair grant covers" belong in the phase 2 epoch change model?

## Summary

A *TLA+ model* is a short, precise description of a protocol as a state machine: the variables each node holds, and every step any node, message or fault can take. *TLC* is the model checker that comes with TLA+: it walks every reachable state of a small instance of the model (say 3 nodes, 2 placement groups, at most 2 crashes) and reports a step-by-step trace if any state breaks a stated property. It checks the design, not the Go code; the simulator checks the code.

**Riskiest designs.** Epoch changes (peering, intervals, read leases) are the riskiest by a wide margin: Ceph, which has no models, still fixes peering bugs every year (68 commits since 2019 that touch peering and close a tracker issue), and AWS's worst model-found bug was a 35-step data loss in DynamoDB's replication and membership protocol. Sessions and fencing come next. The order follows what a bug costs and how deep its trace is, not fix counts. Cross-shard commit is high-risk but not yet designed: CockroachDB's TPC-C runs tripped two assertions in its transaction-record protocol that its TLA+ model then reproduced, and MongoDB's model found a 27-step isolation violation with two transactions. The snapshot cut and pool moves are medium-high: CephFS kept snapshots behind a flag for five years, and RBD shipped a flatten that corrupted clones after an interrupted flatten for six years before it was found. Recalls and purges are smaller protocols with sharp edges and are cheap to model.

**Schedule.** Keep the four phase 2 models. Write the snapshot-cut model, which also covers pool moves, in phase 11 before the cut's code, by extending the phase 2 sessions and recall models. Write the cross-shard commit model in phase 13, where range splitting first makes two data shards, as the first precise form of a design that is four lines long today. Drop neither. What phases 1 to 5 hold for snapshots (freeze entries, built; the snapshot epoch in every write, designed) is the data side a cut model would add to, not change. Both protocols also depend on designs (sessions, recalls, multiple shards) that the phase 2 models and later phases will still change. The two open questions do belong in phase 2, because each changes a phase 3 message or primary rule. Keep the epoch change model as one spec, because its central property needs peering and leases together, and give it two `ci` configurations, one heavy on leases and one on peering, to fit the budget. Incarnation fencing's expected answer: the incarnation table need not survive a primary change, if a primary checks incarnations when it orders a write, re-orders a held write only if no other primary led the PG since, and every chunk write carries its incarnation.

**Depth and budget.** Each model gets a `ci` configuration that TLC checks exhaustively for safety in under 5 minutes with 4 workers, run whenever `models/` or the TLC pin changes. Each also gets a `deep` configuration with one more node, chunk, crash or term, plus its unbounded liveness properties, checked once per change to it, capped at 4 hours. Each night, TLC's random simulation runs for about an hour per model with a fresh seed. At the throughput measured here (30,000 to 430,000 distinct states a second on 4 workers), 5 minutes covers roughly 10 to 130 million states. That is what MongoDB's two-shard transaction model and CockroachDB's two published models need. A Raft-sized spec needs much more: etcd-raft's, cut down to 3 fixed servers, did not finish in 15 minutes.

## What the design says now

- Testing strategy, layer 2: "Design bugs in epoch changes (per-PG intervals, peering, read leases), sessions and fencing, cache-mode recalls, purges, snapshot cuts, cross-shard commit", checked "as etcd-raft checks its own" ([design.md](../../docs/design.md#testing-strategy)). Unlike layers 1, 3, 4, 5 and 7, layer 2 has no paragraph of its own, so the table row is the only place that says what gets modelled.
- Testing the design decisions: properties that must hold across a run are held by "simulator invariants checked after every step, plus a TLA+ model for each protocol".
- Roadmap, phase 2: models of epoch changes, sessions and fencing, cache-mode recalls and purges, "before writing the real protocols". Phase 11 builds the snapshot cut, copy-on-write, restore, flatten and in-place pool moves; no phase names cross-shard transactions, and phase 13 builds range splitting.
- Build checklist, phase 2: five items (TLA+ and TLC, the four models) plus the model-to-simulator map. The map's note says that layer 2 lists snapshot cuts and cross-shard commit, "but no phase 2 item models them".
- test-scenarios.md: the "TLA+ and TLC" section leaves the per-model CI budget and nightly runs open. The sessions, recall and purge sections leave their depths open, each with a suggested first configuration. The epoch change model already lists both open questions as faults, plus a counterexample row for grant coverage.
- The Learning project table lists models of "placement intervals, sessions, cache-mode recalls" for the model-checking track, without purges.
- Snapshots and clones: the cut pauses every node using the volume, waits for in-flight writes "or its session expires and is fenced", commits `volume/{id}/snap/{E}` plus a marker key in each shard holding `fs/{volume}/`, and resumes. The design does not say who resumes the nodes if the coordinator crashes; the simulator faults list "coordinator crash between pause and resume".
- Metadata layer, cross-shard transactions: four steps (a PENDING record in one shard, intents in the others, a flip to COMMITTED, asynchronous resolution); "anyone can finish or abort T after a coordinator crash". Phase 4 runs one shard, and the "Sharded API, one shard" scenarios leave open what a `Txn` touching two shards returns.

## What other systems do

### CockroachDB: two specs, written after the design, checked by hand

`docs/tla-plus/` at `d30c905` holds two specs, and neither is run by CI. No build file or workflow outside the directory names `tla2tools` or `.tla`, and the README only points to TLA+ resources.

- **ParallelCommits** (919 lines of PlusCal and its translation) models one transaction committing its writes and its record in parallel, while "preventers" try to recover it (`ParallelCommits.tla:8-47`). It checks `AckImpliesCommit` (an acknowledged commit is committed, `:534`), and, as liveness, that an implicit commit becomes explicit (`:531`). Its configuration is 2 keys, 2 preventers and 3 attempts (`ParallelCommits.cfg:5-7`), and the Toolbox launch file uses 8 workers (`ParallelCommits___Model_1.launch:50`).
  - It was written in May 2019 (`af78fdcf89a`), a year after the RFC (`docs/RFCS/20180324_parallel_commit.md`), while the code was being rolled out.
  - Adding asynchronous intent resolution "reveals an overly strict assertion in the `RecoverRecord` step, which fires immediately" (`50e1037`). The same assertion had fired in TPC-C runs, and PR #37784 removed it.
  - Adding pipelined writes made the model trip "the hazard discussed in #37866" (`74c36e5`). That hazard was first seen in TPC-C as a transaction coordinator holding a record "unexpectedly already committed". The next commit designed the fix in the model (`8dc06b6`): query the transaction record when a pre-commit check finds an intent missing.
  - The spec's own comment names the root of both bugs: intent resolution loses information, so a missing intent can mean "never written" or "resolved after commit" (`ParallelCommits.tla:66-80`).
  - An outside reader later found the model itself wrong: a loop that never shrank its set, found "just by chance" when making the committer fail-free produced a violation they did not expect (issue #54130, fixed in `abc7f04`). A model can hide its own bugs until someone runs a variant of it.
- **StoreLiveness** (517 lines) models the node-pair support that CockroachDB's leader leases ride on. rock-solid's node-pair leases copy this design (design.md, Raft shard). It checks durable support and three other invariants, with 2 nodes, clocks up to 3 ticks and 1 restart (`StoreLiveness.cfg:5-8`). It was written in May 2024 (`ef95359`) and is cited by the package documentation (`pkg/kv/kvserver/storeliveness/doc.go:103`). When it was updated to the shipped algorithm, the new Support Disjointness invariant showed that clock propagation was needed: "Earlier we thought clock propagation is not needed for correctness but with the addition of the Support Disjointness Invariant, now it is" (`c2df2db`).

What carries over: CockroachDB models two protocols, transaction commit and node-pair liveness, as separate specs at small sizes. The transaction spec's bugs came from steps it first left out, intent resolution and pipelining. rock-solid's cross-shard model should include asynchronous resolution and record cleanup from its first version.

### etcd-raft: a spec, trace validation, and no CI

`tla/` at v3.6.0 holds `etcdraft.tla` (926 lines), a model-checking wrapper `MCetcdraft.tla` with its configuration, and `Traceetcdraft.tla`, which validates traces logged by the real library (built with `-tags with_tla`) against the spec. The README asks that model checking "run enough time (at least hours, depending on how confident we need) without failure" (`tla/README.md:69`). Its script downloads TLC's nightly build (`validate-model.sh:15`). Its configuration is 3 initial servers out of 4, 2 reconfigurations, terms below 10 and 5 requests (`MCetcdraft.cfg:24-29`). Nothing in `.github/workflows` runs it. PR #113, which added it, reports no bug found ("No. I did not find bug so far") and lists CI integration as future work.

So etcd-raft is a template for spec style and for trace validation, not for CI practice. rock-solid's "TLA+ and TLC" item already asks for more than the template does: CI runs, stated bounds and negative checks.

### AWS: models find design bugs that testing misses

Newcombe et al., *Use of Formal Methods at Amazon Web Services* (2014; CACM April 2015):

- Applied to 10 large systems. The table lists S3's fault-tolerant network algorithm (2 bugs, plus bugs in proposed optimizations), S3's background redistribution of data (1 bug, plus a bug in the first proposed fix), DynamoDB's replication and group membership (3 bugs, "some requiring traces of 35 steps"), EBS volume management (3 bugs), a lock-free structure (missed a liveness bug "as we did not check liveness"), and a replication and reconfiguration algorithm (1 bug).
- DynamoDB's bug "could lead to losing data if a particular sequence of failures and recovery steps was interleaved with other processing". It "had passed unnoticed through extensive design reviews, code reviews, and testing", which included fault injection on a simulated network that lost, duplicated and reordered messages, which is rock-solid's layer 1. They checked it with distributed TLC on ten 8-core EC2 instances.
- One known bug that had surfaced only after months of testing was found by TLC "in a few seconds". The team's reviewed fix was then shown to fail by another trace.
- When a new feature was added to DynamoDB, adding it to the existing spec showed at once that "the initial design would have introduced a subtle bug".
- Engineers learned TLA+ and got "useful results in 2 to 3 weeks"; each spec took "a couple of weeks".

What carries over: the bugs models find are in fault and recovery paths, at depths a simulator rarely reaches. A model is worth most when it exists before the code. Extending an existing spec for a new feature is cheap, which argues for writing later protocols' models as extensions of the phase 2 ones.

### MongoDB: a two-shard transaction model in minutes

Schultz and Demirbas, *Design and Modular Verification of Distributed Transactions in MongoDB* (PVLDB 18, 2025; artifacts in `mongodb-labs/vldb25-dist-txns` at `74526c1`):

- It models MongoDB's cross-shard transactions (two-phase commit across shards, with prepare) and, as a separate module, the storage engine interface under them. It checks snapshot isolation and read committed with TLC for "finite instantiations".
- Table 1: 2 shards, 2 keys, 2 transactions, timestamps 1 to 3, at most 2 operations per transaction. The runs are 2,132,765 states at depth 35 in 46 s, 1,967,907 in 42 s, and 8,454,961 in 3 min 20 s, on an 8-vCPU EC2 m6g.2xlarge. The repository's `check_models.sh` runs each configuration with 6 workers.
- "Although we check small models, we still found these to be effective": a detail of prepared-transaction reads missing from an early version of the model gave "a 27-step counterexample leading to a snapshot isolation violation, with just 2 transactions and 2 operations per transaction, in a few minutes".
- The storage module's state graph (490,360 states) generated 87,143 tests that ran against WiredTiger, and found where the authors' model of the engine was wrong.
- Their reconfiguration protocol (Schultz et al., OPODIS 2021) took about 8 hours for 345 million distinct states and 19.5 hours for 812 million, on 20 TLC workers. That is the scale of a deep run, not a CI run.
- Davis et al., *eXtreme Modelling in Practice* (VLDB 2020), checked traces from the MongoDB server against its Raft spec and gave up after 10 weeks of work, while generating tests from a model for Realm Sync worked.

### Azure Cosmos DB: a model written after the fact

Hackett, Rowe and Kuppe, *Understanding Inconsistency in Azure Cosmos DB with TLA+* (arXiv 2210.13661, 2022), wrote a client-side model of the five consistency levels, about 390 lines, years after the service shipped. They checked it exhaustively for logs up to 6 and by random simulation to 100 steps. It found documentation errors, and it explained a 28-day outage: two servers used session tokens arbitrarily, so reads were stale. It found no implementation bug, and the paper says that without a link to the code (trace validation), errors can be caught only by review. What carries over: exhaustive checks at a small bound plus random simulation deeper, which is the `ci` and `deep` split proposed below.

### PingCAP (TiKV): transaction protocols are what gets modelled

`pingcap/tla-plus` at `8ae629d` models Raft, Raft region merges, Percolator, a concurrent Percolator, two-phase commit, TiKV's distributed transactions (optimistic and pessimistic, with snapshot isolation among nine invariants, `DistributedTransaction/Test1.cfg`), commit-timestamp optimizations and resolved timestamps. Besides consensus, the protocols they model are the ones that commit across shards and the ones that change shard boundaries.

### Ceph: no models, bugs found by thrashing and by users

Ceph at `b7e6787` has no TLA+ or other model in the tree. Its tests are teuthology suites that kill and restart daemons under load (`qa/tasks/thrashosds.py`, `mds_thrash.py`, `mon_thrash.py`, `thrash_pool_snaps.py`), plus users' reports. What that leaves:

- **Peering.** Since 2019, 68 commits under `src/osd` that mention peering or past intervals also carry a `Fixes:` tracker link. Examples:
  - required past-interval bounds taken from an OSD's own `oldest_map`, which "can lag for a while" (`0c611b3`, 2023);
  - the PG log trimmed past `last_update_ondisk`, so a later interval change could not find every divergent entry (`eef1928`, 2020);
  - lease renewals sent before the PG was active, "crashing the replicas" (`71b5b39`, 2020).
- **CephFS snapshots.** Emperor (2013) disallowed new snapshots unless a flag was set (`doc/releases/emperor.rst:324`). Mimic (2018) declared them "now stable when combined with multiple MDS daemons" and enabled them by default (`doc/releases/mimic.rst:1399,1736`). Since 2019, 81 commits under `src/mds` and `src/client` mention snapshots and close a tracker issue.
- **RBD snapshots, clones and flatten.** About 53 snapshot fixes in `src/librbd` since 2019, including:
  - snapshot creation unblocking I/O before the snapshot's object map existed (`1e87440`, 2018), the same ordering as rock-solid's resume-after-commit;
  - copy-up from the parent not restarting writes delayed behind it (`31ed673`, 2019);
  - flatten, from 2018 (`40af4f8`), trusting an object map that an interrupted flatten or a crashed client had left inconsistent, which "would corrupt the clone". The fix landed in December 2024 (`97ed3fc`).

The counts come from `git log --grep` and are rough: they include feature commits that close a tracker issue. They show where the bugs keep coming, not how many there are.

## Findings

### 1. How risky each design is

The ranking goes by what a bug costs and how deep its trace is, not by how many fixes another system logged. A bug that loses an acknowledged write or serves a stale read, and needs crashes, restarts and table changes interleaved to show, ranks above one that fails a single operation.

| Protocol (design.md section) | How comparable systems found their bugs | Risk | Model |
| --- | --- | --- | --- |
| Epoch changes: intervals, peering, missing lists, read leases, fortification | Ceph: a steady stream of peering fixes from thrashing and users, no model. AWS: 35-step data loss in DynamoDB replication and membership, past fault-injection testing. CockroachDB: the Store Liveness model changed the algorithm (clock propagation) | Highest: a lost acknowledged write or a stale read, after crashes, restarts and table changes interleave | Phase 2, one spec with two `ci` configurations (finding 3) |
| Sessions and fencing, incarnations | Ceph: 17 or 18 blocklist fixes since 2019 (over `src/osd`, `src/mds`, `src/client`, `src/librbd`) | High: a fenced client's write applied, or a lock held twice; the safety argument is prose today (session expiry study) | Phase 2 |
| Cross-shard commit: transaction record, intents | CockroachDB: two TPC-C assertion failures, reproduced and fixed in the model. MongoDB: 27-step snapshot isolation violation with 2 transactions | High, and the design leaves out what the hazards come from: conflicting intents, record cleanup, rename loops | When designed, before code (finding 4) |
| Snapshot cut and pool moves | CephFS: snapshots experimental 2013 to 2018. RBD: I/O resumed before the snapshot's map existed; a flatten that corrupted clones after an interrupted flatten, live for six years | Medium-high: one coordinator, one commit point, but crash and membership races the design does not yet cover. Ceph's snapshot fix counts (81 CephFS, about 53 RBD) exceed its peering count, but much of CephFS's trouble comes from per-directory snapshot realms, which rock-solid does not have: it snapshots whole volumes | Phase 11, before the cut's code (finding 4) |
| Cache-mode recalls | Ceph: 27 cap revoke and recall fixes since 2019 | Medium, kept to one file | Phase 2 |
| Purges | XFS `5885ebda` and FreeBSD fusefs `e312493b` (the purge model's own rows) | Medium, small state | Phase 2 |

### 2. Practical TLC sizes and run times

From [results.txt](results.txt), 4 workers on an Apple M4 Pro:

| Model | Constants | Distinct states | Depth | Wall time |
| --- | --- | --- | --- | --- |
| CockroachDB StoreLiveness, as shipped | 2 nodes, clock ≤ 3, 1 restart, symmetry | 25,383,879 | 45 | 1 min 28 s |
| CockroachDB ParallelCommits, safety only | 2 keys, 2 preventers, 3 attempts | 8,216,557 | 63 | 19 s |
| CockroachDB ParallelCommits, as shipped (with liveness) | same | 8,216,557 (49,299,342 in the behaviour graph its liveness check walks) | 63 | 13 min 2 s |
| etcd-raft MCetcdraft, cut down | 3 fixed servers, no reconfiguration, terms below 3, 2 requests | 27,582,788 when stopped, about 19 million still queued | 15 | did not finish in 15 min |
| etcd-raft MCetcdraft, as shipped | 3 of 4 servers, 2 reconfigurations, terms below 10, 5 requests | 17,669,559 when stopped | 15 | stopped by hand at 13.5 min, still growing |

Published figures: MongoDB's 2-shard transaction model, 2.1 to 8.5 million states in 42 s to 3 min 20 s on 8 vCPUs; its reconfiguration model, 345 to 812 million states in 8 to 19.5 hours on 20 workers; DynamoDB's, distributed TLC on 80 cores.

Three things follow:

- **A `ci` configuration of 10 to 50 million states fits 5 minutes** on 4 workers for a spec whose states are small records, as all three published transaction and lease models do. GitHub's hosted runner for a public repository has 4 CPUs (2 for a private one), and is likely slower per core than this machine; that factor is unmeasured.
- **Liveness is the expensive part.** ParallelCommits' safety check took 19 s, and the same state space with its temporal properties took 13 minutes, 41 times as long. AWS's lock-free structure missed a liveness bug because liveness was not checked. So unbounded liveness properties belong in the `deep` tier. A liveness property with a time bound, such as the sessions model's "fenced within `session_lease` plus one commit and one acknowledgment round", can instead be written as a safety invariant over the model's clocks, which is cheap enough for `ci`.
- **Message bags explode.** etcd-raft's spec keeps every message in flight as a bag of records, so even 3 fixed servers and 2 requests grow without end in practice. rock-solid's epoch model has messages "held for any time" and duplicated. It needs the counters etcd-raft's wrapper uses (one append in flight per pair, `MCetcdraft.tla:87-105`) and bounds on interval changes, restarts and writes, or it will not fit any budget.

### 3. One epoch spec with two `ci` configurations, and both open questions in it

The phase 2 epoch change item models tables, intervals, peering, missing lists, pending writes, restarts and counters. It also models two clocks per node, rate bounds, suspends, VM pauses, support epochs and fortification, on 3 or 4 nodes with 2 PGs. StoreLiveness alone, with 2 nodes and clocks up to 3, is 25 million states, and peering with 2 PGs, 2 chunks and messages held for any time is likely at least as large as the cut-down etcd-raft spec. At its full constants, the whole model would likely not fit the `ci` budget. That was not measured.

It cannot be cut into two specs along "peering" and "leases", though, because property 1 ("no PG serves reads from two primaries at once") needs three rules together:

- peering reaches a member of every interval since the last activated one;
- a grantor joins no peering that moves a primary away while its grant to that primary runs;
- an interval that keeps its primary goes on serving reads before it activates.

Here is a trace that needs all three. Interval 41 is activated with primary A. Interval 42 replaces B with D and is not yet activated, and A goes on serving reads under leases from C and D. Interval 43 moves the primary to C. If A and B no longer share a PG, B's grant to A can lapse, and B, live in 41, could join 43's peering at once. Only D, reached for interval 42, still waits on its grant to A. A lease spec that kept only each PG's interval number and primary would have no member sets for 42, and a peering spec with leases as a bare predicate would have no grants, so neither would notice if a later change narrowed the reach rule.

So keep **one spec, as etcd-raft's `MCetcdraft` wraps one spec with model-checking limits, with two `ci` configurations**:

- `ci-leases`: 1 chunk, no merges, full clocks, and member sets and activation per interval.
- `ci-peering`: 2 PGs and 2 chunks, with lease expiry left to nondeterminism within the ordering that grants impose on peering: a grantor that joins peering of an interval has no live grant to that interval's old primary.
- `deep`: both together, at 4 nodes.

CockroachDB's precedent is the same shape: StoreLiveness proves durable support as the base "to build lease disjointness" on (`StoreLiveness.tla:14-20`), a narrow assume-guarantee interface. What it does not do is split one property across two specs. Also add the trace above as a fault, and as the minimal trace of a new counterexample row in `ci-leases`: peering reaches only the last activated interval and the current one.

**Incarnation fencing.** The question is whether a primary's table of newest incarnations must survive a primary change. Two parts are clear:

- A dead daemon's write W is bound to the connection it was sent on, so it can reach only the primary process it was addressed to, P in interval `j`.
- If the reissue W′ completed on a different primary, interval `j` was stopped before that primary activated, and W can never complete there. If P restarted, W's connection died with it.

That leaves writes P already *holds*, which the design has a primary that stays primary across an interval change order again after it re-peers (Epoch changes: "orders again, once it has re-peered, the client writes it held when an interval ended under it"). Two traces matter:

1. P holds W, D2's hello reaches P, and P orders W after W′ and the pod's next write W2. A check at arrival only misses W; a check when P orders a write catches it.
2. P holds W, pauses past `node_down_after`, and interval k makes Q primary. D2 reissues W′ to Q, and W2 completes there. P resumes, fetches the whole table after the gap, and finds itself primary again in interval m. From P's view an interval "ended under it" while it stayed primary. P has never seen D2's incarnation, so checking at ordering does not help, and W lands after W′ and W2.

Expected answer: the table need not survive a primary change, *provided that*:

- (i) a primary checks incarnations when it orders a write, not only when it receives one, as storage nodes already reject fenced sessions' "writes already queued";
- (ii) a primary orders a held write again only if no interval since the one it held it in had another primary, which peering shows. Otherwise it refuses the write, and the client retries with a fresh table.

Checking at ordering also needs the incarnation where the write is ordered. Today only the connection hello carries it (test-scenarios Wire messages), and with one event loop per core the old connection, the new connection and the chunk's loop can be three different loops. So each client chunk write and punch should carry the incarnation (8 bytes), and the chunk's loop compares it directly. The alternative, that a hello reaches every loop's inbox before any write from that connection is forwarded, is a cross-loop ordering rule the runtime would have to keep forever, so it is rejected.

The model needs:

- connections as per-process channels;
- held writes;
- the pause-and-skip fault above;
- a variant with the check at arrival only, and one without rule (ii), each of which must fail.

The property they break is an effect on data, not a restatement of the rule: once W′ and a later W2 to the same block are acknowledged, the block never again holds W. The simulator's byte-range oracle already checks that, under the fault "a dead daemon's chunk write delivered after its successor's reissue and the pod's next write".

**Grant coverage.** Which PGs a grant covers is the interaction of a node-pair lease with two nodes' different tables. It is property 1, checked in `ci-leases`, and the existing counterexample row ("A fresh grant covers the pair's PGs with no table check") already states the trace. Both candidate rules change the phase 3 Grant message (whether it carries the grantor's table epoch) or the grantor's state, so it must be settled before phase 3 builds peering and read leases.

### 4. When to model snapshot cuts and cross-shard commit

**Snapshot cut and pool moves: phase 11, before the cut's code.** What phases 1 to 5 hold for snapshots is the data side:

- `freeze` entries, absent freezes, frozen versions in the chunk index, and reads at an epoch, all built in phase 1 (`internal/chunkstore`) and tested by E1 to E3 and the metamorphic runs;
- the snapshot epoch in every chunk write and punch, fixed so far only in the design of the phase 3 wire frame.

A model of the cut would add to these, not change them. A primary-side rule it might find reads fields that are already there; one example is refusing a write whose snapshot epoch is below its chunk's last freeze epoch, which the chunk index stores. Such a refusal is a new reply, which the wire's version byte allows to be added. What the model would check is the cut itself, and the design leaves the hard parts open:

- **The coordinator's crash.** The cut is a two-phase commit: pausing is preparing, and `volume/{id}/snap/{E}` is the commit point. A node paused by a coordinator that died cannot resume on a timeout alone. If E committed and the node resumes on the old epoch, its next writes do not freeze, and the snapshot holds a post-cut write. So a paused node must learn from the MetaStore whether E exists, and the coordinator's commit must be conditional, so that a cut a node gave up on can never commit later. That is the transaction-record pattern (PENDING, then COMMITTED or ABORTED).
- **Membership.** A node that attaches or mounts the volume while the cut runs is not in the coordinator's list.
- **Fencing.** A node acked "by its session expiring" must be fenced *and acknowledged* (the sessions model's owner-release row) before E commits, or its queued write lands on both sides of the cut.
- **Ordering.** Resume only after the commit (RBD `1e87440`).
- **Pool moves** add read-through to the old pool and a background copy racing client writes, where an interrupted copy must rerun safely (RBD `31ed673`, `97ed3fc`).

The cut reuses the Tier 2 recall's fan-out and the sessions model's fencing. Phase 2 will check those and may change them, and phases 6 and 7 build them. A cut model written now would have to follow those designs as they change; in phase 11 it is an extension of two checked specs, which AWS found cheap and quick to find bugs in.

Testing strategy already lists cut faults that phases 6 to 10 would meet first: "a snapshot taken while a purge runs" and "a snapshot cut while a volume overwrites near full". They wait for phase 11 too, as the purge model already says of the first.

**Shards before phase 13.** From phase 4 there are two Raft groups, the control-plane group (`ctl/`) and one data shard. `volume/{id}/snap/{E}` and the marker are on "any shard", and no designed operation in phases 4 to 12 writes `ctl/` and a data shard in one `Txn` (phase 12's `PUT` "stays single-shard"). So the cut's transaction is single-group until range splitting, and the cut model needs no cross-shard commit.

**Cross-shard commit: in the phase that first runs two data shards (phase 13), before the transaction code.** Every operation, cross-directory rename included, is single-shard until range splitting exists. The design's four steps leave out what CockroachDB's and MongoDB's bugs came from:

- what a reader or writer that meets another transaction's PENDING intent does (wait, or abort the transaction by changing its record, and how the coordinator learns it lost);
- when a record may be removed (only after every intent is resolved; CockroachDB's ambiguity comes from resolution losing information, `ParallelCommits.tla:66-80`);
- how a cross-directory rename refuses to make a directory its own ancestor when the ancestors are on other shards;
- how the snapshot cut's markers in several shards commit with the snapshot record.

That is a design to write, and the model should be its first precise form, as AWS now refines prose designs into PlusCal. Until then, a `Txn` whose keys span two groups should return an error naming them, the control-plane group included, as test-scenarios suggests, so that nothing comes to depend on a cross-shard path before it is modelled.

**Dropping them** from the layer 2 table would leave the two protocols with a coordinator and a commit point unmodelled. That is the class in which every system read here found its subtle bugs: CockroachDB's recovery, MongoDB's prepare, DynamoDB's recovery steps, RBD's interrupted flatten.

## Proposed answer

1. Keep the four phase 2 models. Keep epoch changes as one spec, with two `ci` configurations, one heavy on leases and one on peering, and a combined `deep` one.
   - It takes incarnation fencing, with rules (i) and (ii) and the incarnation carried on every chunk write.
   - It takes grant coverage, and decides between the two grant rules.
2. Schedule the snapshot-cut model, covering pool moves and flatten, in phase 11 before the cut's code, extending the sessions and recall models. Schedule the cross-shard commit model with the cross-shard transaction design, in phase 13 when range splitting first makes two data shards, before its code. State in the layer 2 row which phase models what.
3. Give every model `ci` and `deep` configurations:
   - **`ci`**: exhaustive, safety properties and time-bounded liveness written as invariants, under 5 minutes with 4 workers on the CI runner. It runs whenever `models/` or the TLC pin changes, together with the negative variants, which stop at their first counterexample.
   - **`deep`**: larger constants plus the unbounded liveness properties, run exhaustively once per change to its spec, configuration or TLC version, capped at 4 hours. The nightly job compares a hash of the three.
   - **Nightly**: TLC's simulation mode (`-simulate`, depth 100 or more) with a fresh seed, for a fixed budget of about 1 hour per model, on machines not running layer 7's performance runs.
   - **Outgrowing `ci`**: when a `ci` configuration outgrows 5 minutes, its largest constant moves to `deep`. Each configuration's sizes, depth and state count go in the model-to-simulator map.
   - **TLC version**: pin release 1.7.4, and avoid CommunityModules operators that need the 1.8 pre-release, whose jar is rebuilt in place (run-tlc.sh needed it for etcd-raft's spec).

**Costs.**

- The epoch spec's two `ci` configurations must between them still exercise every rule. The `deep` run is the check that they do.
- Up to 4 machine-hours per changed `deep` configuration, about 20 for a change to all five phase 2 specs. On a normal night only about 5 hours of simulation run.
- Eight bytes more in every client chunk write and punch.
- The snapshot and cross-shard models arrive late, so phases 6 to 10 run without them. That is safe because what those phases build for snapshots is only the data side the cut model would add to, and the cut faults they would meet wait for phase 11.

**Alternatives rejected.**

- *All six models in phase 2.* The cut depends on sessions and recalls, whose phase 2 models are still to be checked, and cross-shard commit is not designed. Both would be written twice. Phase 2, already four specs of two to three weeks each by AWS's experience, would double.
- *Drop them from the table.* See above.
- *Two epoch specs with a refinement check between them.* A lease spec would hold an abstract peering: intervals with member sets, a primary and an activated flag, and the rule that an interval activates only once every earlier one since the last activated has a member that adopted it and holds no live grant to that interval's primary. The peering spec would check that its concrete peering implies that rule. It is sound, but it costs a second spec and a refinement mapping to keep in step for the same coverage that two configurations of one spec give.
- *No CI runs, models checked by hand, as CockroachDB and etcd-raft do.* Their models went stale against code and against themselves (CockroachDB #54130). rock-solid's map from models to the simulator assumes the models still pass.
- *Exhaustive `deep` runs every night.* An unchanged spec, configuration and TLC version give the same answer every night; only simulation finds anything new. Running them would also take the homelab machines from the performance runs.
- *Trace validation, as etcd-raft has.* MongoDB abandoned it after 10 weeks. The model-to-simulator map and the simulator's invariants cover the same gap more cheaply. Reconsider once the trace record format is stable.

## What the phase 2 models, the simulator and tests must check

- **Epoch model, `ci-leases`:**
  - the trace of finding 3 (a replica replaced, an unactivated interval that keeps its primary, then a primary move) as a fault, with a counterexample row in which peering reaches only the last activated interval and the current one;
  - the grant-coverage row, and whichever grant rule passes, named in the Grant message's scenario under Wire messages.
- **Epoch model, `ci-peering`:**
  - held writes, and the fault in which a primary holding a write skips a table whose interval had another primary, then leads the PG again;
  - the variants "incarnation checked at arrival only" and "held writes re-ordered without rule (ii)", each of which must fail;
  - the property they break: once a reissue W′ and a later write W2 to the same block are acknowledged, the block never again holds the dead daemon's W, mapped to the byte-range oracle.
- **Every model:** each `ci` configuration finishes in budget, and the run prints states, depth and wall time.
- **Phase 11** (for its checklist when one exists), a cut model with:
  - faults: a coordinator crash at each step, a node that never acks, an attach racing the cut, a fence acked but not acknowledged, and a move's copy crashing and rerunning;
  - property: "a snapshot holds every write acknowledged before its cut and none issued after".
- **Phase 13**, a cross-shard commit model with:
  - faults: coordinator crash at each step, conflicting intents, readers aborting a live transaction, asynchronous resolution and record cleanup, and concurrent cross-directory renames;
  - properties: atomic visibility, no directory cycle, no record removed while an intent names it.

## Where it would apply

**design.md**

- **Testing strategy**, layer 2 row: "Design bugs in epoch changes (per-PG intervals, peering, read leases, Raft fortification), sessions and fencing, cache-mode recalls and purges (phase 2); snapshot cuts and pool moves (phase 11); cross-shard commit (phase 13)".
- **Testing strategy**, a new "**2. Protocol models.**" paragraph after layer 1. It holds:
  - the `ci`, `deep` and nightly simulation tiers and their budgets;
  - negative variants in CI;
  - depths stated in the model-to-simulator map;
  - that each protocol after phase 2 gets its model in the phase that builds it, before its code.
- **Testing strategy**, the freeing-space and full-pool faults that involve a cut: "registered in phase 11".
- **Epoch changes**: the sentence on held writes gains rules (i) and (ii). The Daemon restart paragraph on incarnations says each chunk write carries one.
- **Metadata layer, cross-shard transactions**: they are designed and modelled in phase 13, and until then a `Txn` spanning two groups, the control-plane group included, is refused with an error naming them.
- **Roadmap**:
  - Phase 2's sentence mentions the epoch model's two configurations and says later models come with their phases.
  - Phase 11 adds "a TLA+ model of the cut and the move, built on the sessions and recall models, before the cut's code".
  - Phase 13 adds "cross-shard transactions (transaction record and intents) for cross-directory rename and snapshot markers, designed and modelled before their code".
- **Open questions**:
  - "Incarnation fencing in the epoch-change model" gets its expected answer, (i), (ii) and the incarnation per write, for the model to confirm.
  - "Peering across several intervals" and "Which PGs a node-pair grant covers" name the configuration that checks them (`ci-peering`, `ci-leases`).
- **Snapshots and clones**: one sentence that a paused node resumes only once it knows whether `volume/{id}/snap/{E}` committed, left for the phase 11 model to settle. This is optional now.
- **Learning project**: the model-checking row adds purges.

**build-checklist.md**

- Phase 2, "TLA+ and TLC": the tiers, the 5-minute and 4-hour budgets and the TLC 1.7.4 pin.
- Phase 2, "Epoch change model": its two `ci` configurations and the new faults (finding 3's trace; held writes across a skipped interval).
- Phase 2, "Model-to-simulator map": records every configuration's depth.
- Phase 3, "Peering and read leases" ("As modeled in phase 2"): names the epoch model's configurations.
- Phase 3, "Wire messages": the incarnation in client chunk writes and punches.
- Phase 4, "Sharded API, one shard": the error for a `Txn` spanning two groups.

**test-scenarios.md**

- "TLA+ and TLC": the Open line is replaced by the tiers and budgets.
- "Epoch change model":
  - "TLC checks every property in every configuration" and "Nodes | 3, and separately 4" become the two `ci` configurations at 3 nodes and `deep` at 4.
  - The incarnation Open line becomes the two counterexample rows and the data property above, numbered 10. The numbering stays global, so the invariants under Placement faults keep their numbers.
- "Sessions and fencing model": the time-bounded liveness row becomes a `ci` invariant over the model's clocks, with the unbounded form in `deep`. The sessions, recall and purge sections turn their suggested first configurations into `ci` ones, each with one larger `deep` one.
- "Wire messages": the incarnation in chunk writes and punches.
- "Sharded API, one shard": its Open line is answered with the error.
- "Model-to-simulator map":
  - a row for property 10 naming the byte-range oracle and the reissue fault;
  - its "Open: the design gives no target depth" is answered by the tiers;
  - its closing note says snapshot cuts, pool moves and cross-shard commit are modelled in phases 11 and 13.

**tech-debts.md**: the TLC 1.7.4 pin entry, which "TLA+ and TLC" already requires.

## Unverified

- **CI runner speed.** The throughput figures are from an Apple M4 Pro with 4 TLC workers. A GitHub-hosted 4-CPU runner is likely slower per core, so the 5-minute budget may need constants a step smaller there.
- **Whether one epoch model at full constants fits `ci`** was not measured. The two-configuration split assumes it does not.
- **etcd-raft as shipped** was stopped by hand at 13.5 minutes with 17.7 million distinct states, still at depth 15; how long an exhaustive run would take was not measured. Its README's "at least hours" suggests it is not meant to finish.
- **The incarnation argument** and the trace of finding 3 are reasoning from design.md's text, not model runs. They are the model's to confirm.
- **The Ceph counts** are `git log --grep` matches, which include feature commits that close tracker issues, and they shift by one with the cut-off date: a reviewer's recount gave 53 RBD snapshot fixes and 17 blocklist fixes. The recall and blocklist counts used the patterns `caps? (revoke|recall)|revoke` and `blocklist|blacklist` over `src/osd src/mds src/client src/librbd`.
- **The AWS figures** are from the September 2014 technical report hosted by MIT 6.826. The CACM version (April 2015) was not compared.
- **When CockroachDB's leader leases shipped** relative to the May 2024 Store Liveness spec was not checked. Nor was its README's silence on CI checked against every build file.
- **MongoDB's reconfiguration paper**: the run times are from the ar5iv HTML. Whether model checking found bugs in its early designs is not said. The acknowledgments credit a person with finding "subtle bugs in early design proposals".

## Reproduce

- `./fetch.sh [dir]` downloads every cited file, commit and paper at the pinned commits into `dir` (default `./work`) and prints the cited lines. It needs `curl` and `python3`; `pdftext.py`, standard library only, extracts the two PDFs' text.
- `./run-tlc.sh [dir] [workers] [cap-seconds]` downloads TLC 1.7.4, TLC 1.8.0 and CommunityModules by checksum, plus the CockroachDB and etcd-raft specs at their pinned commits.
  - It checks five configurations, each capped at `cap-seconds` (default 900), and prints one line per run, as in [results.txt](results.txt).
  - It needs `curl`, Java 11 or later, and `perl`, used for the time cap. A full run takes about 50 minutes.
  - results.txt's last row was stopped by hand at 809 s, so the script reports it at the cap instead.

## Sources

- CockroachDB, `cockroachdb/cockroach` at [`d30c905`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff):
  - [`docs/tla-plus/`](https://github.com/cockroachdb/cockroach/tree/d30c905fff79ef825adc96bcc647f1872a90f2ff/docs/tla-plus) (ParallelCommits, StoreLiveness);
  - [`pkg/kv/kvserver/storeliveness/doc.go`](https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/kv/kvserver/storeliveness/doc.go);
  - commits [`af78fdc`](https://github.com/cockroachdb/cockroach/commit/af78fdcf89a076a4b4da0695660cace67ccd3aed), [`50e1037`](https://github.com/cockroachdb/cockroach/commit/50e10374283e37525610eda141082871bf2a5554), [`74c36e5`](https://github.com/cockroachdb/cockroach/commit/74c36e51a5b3825076c378834558de234b13feae), [`8dc06b6`](https://github.com/cockroachdb/cockroach/commit/8dc06b6a49fbf43b6e81baa2693e5c890a36e21b), [`abc7f04`](https://github.com/cockroachdb/cockroach/commit/abc7f04791315c949bcd4fc13248ef41630eada3), [`ef95359`](https://github.com/cockroachdb/cockroach/commit/ef953596d5a91a90a927182938a7363e27671ad1), [`c2df2db`](https://github.com/cockroachdb/cockroach/commit/c2df2db47df74ed5790187b5179fb1995ed98222);
  - issues and PRs [#37866](https://github.com/cockroachdb/cockroach/issues/37866), [#37784](https://github.com/cockroachdb/cockroach/pull/37784), [#54130](https://github.com/cockroachdb/cockroach/issues/54130);
  - the blog post [*Parallel Commits*](https://www.cockroachlabs.com/blog/parallel-commits/).
- etcd-raft, `etcd-io/raft` at v3.6.0 [`f35a022`](https://github.com/etcd-io/raft/tree/f35a02212416d045c574e30aa59d4b07c4b8d732/tla): `tla/README.md`, `tla/MCetcdraft.cfg`, `tla/MCetcdraft.tla`, `tla/validate-model.sh`, and [PR #113](https://github.com/etcd-io/raft/pull/113).
- Ceph, `ceph/ceph` at [`b7e6787`](https://github.com/ceph/ceph/tree/b7e6787889ed0783df44063506dcacaaf672758f):
  - `doc/releases/emperor.rst`, `doc/releases/mimic.rst`, `qa/tasks/`;
  - commits [`97ed3fc`](https://github.com/ceph/ceph/commit/97ed3fced13dd48520ec9c165537ff0bbc7cbb64), [`40af4f8`](https://github.com/ceph/ceph/commit/40af4f87b64f944de786f6fcb6cc18eddd3633ef), [`1e87440`](https://github.com/ceph/ceph/commit/1e874403bf861cb8b74261308d8b73434cf90341), [`31ed673`](https://github.com/ceph/ceph/commit/31ed673a7484c8067dd928cabd5bcb923a35dbaf), [`0c611b3`](https://github.com/ceph/ceph/commit/0c611b362fb9cc4225f18283f74299551c2c5953), [`eef1928`](https://github.com/ceph/ceph/commit/eef1928e0153f09dc2522bb885b0ad9f5c65f4bd), [`71b5b39`](https://github.com/ceph/ceph/commit/71b5b39220b5f69995faffad1ef6992db6a337df).
- PingCAP, [`pingcap/tla-plus` at `8ae629d`](https://github.com/pingcap/tla-plus/tree/8ae629ddf9f5ad8efc763bff93e3e43ce0a3585d).
- MongoDB:
  - W. Schultz, M. Demirbas, [*Design and Modular Verification of Distributed Transactions in MongoDB*](https://www.vldb.org/pvldb/vol18/p5045-schultz.pdf), PVLDB 18 (2025), with artifacts at [`mongodb-labs/vldb25-dist-txns` `74526c1`](https://github.com/mongodb-labs/vldb25-dist-txns/tree/74526c1201109405172eb845413154f547a815ee);
  - W. Schultz et al., [*Design and Analysis of a Logless Dynamic Reconfiguration Protocol*](https://arxiv.org/abs/2102.11960), OPODIS 2021;
  - A. J. J. Davis et al., [*eXtreme Modelling in Practice*](https://arxiv.org/abs/2006.00915), VLDB 2020.
- AWS: C. Newcombe et al., [*Use of Formal Methods at Amazon Web Services*](https://6826.csail.mit.edu/2019/papers/formal-methods-amazon.pdf) (technical report, 29 September 2014), published as "How Amazon Web Services Uses Formal Methods", CACM 58(4), April 2015.
- Azure Cosmos DB: A. F. Hackett, J. Rowe, M. A. Kuppe, [*Understanding Inconsistency in Azure Cosmos DB with TLA+*](https://arxiv.org/abs/2210.13661), 2022.
- GitHub-hosted runner sizes: [GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
- TLC: [v1.7.4](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4), [v1.8.0](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0) (pre-release), [CommunityModules 202505152026](https://github.com/tlaplus/CommunityModules/releases/tag/202505152026).
