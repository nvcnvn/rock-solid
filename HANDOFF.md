# Handoff: voter replacement model

Brief: /Users/nvcnvn/.claude/projects/-Volumes-T5-Codes-rock-solid/handoff/voter-model-brief.md
Branch voter-model. Nothing merged or pushed. The checklist item stays UNTICKED.

## Done, each from a trace's states
Every negative below fails for its rule, on the spec before `InitLearners`, which changes no configuration but
neg-one-half. Those marked * ran before the Init `mark` fix: rerun, and re-read if the depth moves.

| Negative | Property | Distinct, depth | Read in the states |
| --- | --- | --- | --- |
| neg-no-first-commit* | ReadsAcked | 93,060, 18 | Leader 2 holds index 3 in its log, state machine at 2, reads on 3's lease; 1 acks index 3 |
| neg-one-half | OneReader | 339,895, 27, 48 s | Both joint: 1 reads on 4 alone, 2 won on the old half and reads on 4 |
| neg-one-half-commit | CommittedSurvive | 503,074, 31, 60 s | Index 5 commits with match <5,2,4,5>, one of the old half; 3 wins on 3+2 without it |
| neg-stale-snapshot | `Stops` | 35,139 | Snapshot at the base lacks the learner, refused, probe rejected, sent again: the code's retry, a real livelock |
| neg-no-transfer | deadlock | 2, 2 | reconcile returns after target(): a dropped leader with no transfer sits, as in the code |
| neg-double-vote* | OneLeaderPerTerm | 5,709, 10 | Early: 1 votes 2, crash forgets it, votes 3 in term 2 |
| neg-lost-copy* | HeldAfterDeletion | 525, 8 | NoGuard: 2 and 3 delete on a fresh replica's notice, one copy left |
| neg-early* | CommittedSurvive | 10,519, 11 | 3 answers a vote with its round in flight, crash forgets, index 3 commits behind leader 2. NOT suspect |
| neg-disloyal* | OneReader | 123,985, 18 | 2 supports 1 and campaigns: the plant covers stopped as well as refuses (lines 379, 392) |
| neg-no-restart-wait* | OneReader | 262,037, 18 | 2 supports 1, restarts, campaigns at once, 1 still holds its lease |
| neg-transfer-keeps-lease* | OneReader | 940,878, 19, 44 s | 1 reads after MsgTimeoutNow; 2 won by the transfer's vote |
| neg-no-guard* | NoWrongfulDeletion | 118, 6 | Fresh 4 answers at 0, voter at applied 2 believes it |
| neg-boot-at-zero* | NoWrongfulDeletion | 979, 7 | Restart at applied 0, fresh 4 answers at 0: shard.go:433's case |
| neg-stale-answer* | NoWrongfulDeletion | 15,782, 14 | 4 answers at commit 3 with applied 0 |
| neg-commit-below-base*, neg-voter-first*, neg-eager*, neg-apply-past-halt*, neg-compact-past-halt* | | 222; 23; 96; 769; 2,606 | Failed, states NOT read. compact-past-halt now expects CompactStops and has not run since |

Liveness (step 5) is decided: no ENABLED and no CONSTRAINT in a live configuration, so nothing is pruned. `Stops` shows
Protocol has no cycle, the deadlock check that every end has SwapDone. ci-live passes: 81,102, depth 37, 12 s.
ci-live-handoff passes: 430,905, depth 50, 71 s. So Protocol has no cycles in both plans. The check found three spec
errors the ENABLED form hid: no StateSnapshot pause, Reprobe outside Protocol, and Init's `mark` at 0, which made every
learner caught up from Init on, so PromotedHolds checked nothing there.

The HaltStops race (step 6) can happen. A copy of the spec with a temporary invariant reached it at 1,401,037 states,
depth 20: a snapshot stepped in the round that carries the poison entry, Durable halts at index 3 with sm 2, the install
moves the base to 4. The code agrees: done calls ApplySnapshot before meta.Shard.apply returns on halted. Harmless: a
restart restores that snapshot, whole, and runs on. So HaltStops is now only sm < halt, and CompactStops, an action
property, covers compaction. Say so in docs/test-scenarios.md.

## Not done
1. Run every `ci` on this spec; nothing below has passed since its last edit. One TLC at a time, and another session
   runs `trace_epoch` TLC at times: `pgrep -f 'tlc2.TL[C]'`. Do not kill it.
   - ci-halt (now both 1 and 3 halt), ci-halt-swap (no compaction now), ci-eager (no write, MaxMsgs 5), ci-notice
     (MaxMsgs 4), ci-crash (no compaction), ci-restart (new, no election): shrunk, not rerun.
   - ci-transfer: 3,036,956 at depth 24 when stopped, near 200 s: likely over. ci-lease, ci-joint: never run.
   - Past 300 s, try `InitLearners = {4}`, which removes the learner's catch-up, for what does not need it.
2. Step 4, shard.go:433: needs ci-notice to pass. `neg-boot-at-zero` still shows what protects it (see table).
3. Step 7: ci-live and ci-live-handoff carry every safety invariant and no bound on messages, so a pair reorders there.
   The numbers above predate those invariants: rerun.
4. deep-live is ci-live on 3+1. With the mark fix it may fit `ci`: try it. Add `deep-joint`, `deep-lease` and it last.
5. Read the unread traces in the table. Then run `scripts/models.sh ci voters` whole, `scripts/model-gate.sh`.
6. Docs: insert the draft below. Change it: `Stops` and the deadlock check replace "state property"; HaltStops and
   CompactStops as above; C9; aims in the configurations; `ci-swap`, `ci-halt-leader` are gone; `neg-double-vote`,
   `neg-lost-copy`. design.md's Testing strategy says liveness is an invariant over clocks: this one has none.
7. No negative: the removal of a dropped learner, the voter-removal order, and CanCampaign, whose plant was dropped
   since nothing ran to show it can fail. Plant `current` has none, as bounded writes cannot leave a learner behind.

Runners are in the scratchpad's v2 directory: t.sh cfg secs, batch.sh, tr.py out (per-step state diff), rd.sh, hd.sh.

## Section draft
## Voter replacement model

A TLA+ specification of what rock-solid adds to etcd-raft in one group, `models/voters/voters.tla`: [Replica placement and replacement](design.md#metadata-layer-metastore)'s swap, [leader leases](design.md#metadata-layer-metastore) and lease reads, the removal notice, snapshots at the applied index, durable rounds and a replica halted at an entry it cannot decode. It holds, per replica, the term, vote, log of normal and configuration entries, commit and applied indexes, the configuration etcd-raft's tracker holds and the shard's applied one, the storage's base, what is durable and the round in flight, the state machine's applied index, fortification on both sides, and the halt; between replicas, the messages and the node-pair grants. Raft's safety is assumed where it can be and its log, terms and votes kept, since the rules here rest on them. It starts from a settled group of three in term 1, the [Voter replacement](#voter-replacement) tests' A, B and C, with D fresh on empty storage, and a plan from the first step.

**Faults.** Messages delayed, reordered across replica pairs and lost; a crash at any point, keeping what was reported durable or the round in flight whole, so a round can be durable with its shard never told; leases ending with expiry left open, as the [Epoch change model](#epoch-change-model)'s `Clocks = FALSE`; a campaign at any time a shard's election timer runs; compaction to the state machine's applied index; an entry one replica cannot decode.

**Rules the model contains**

- A configuration takes effect when applied, per replica, as etcd-raft does, and a replica with an unapplied configuration entry in its committed log does not campaign. A restart applies again from the base, with the base's configuration.
- Each action is one call into a shard, as `Shard.ready` ends each: a Ready that must be durable is the round, a leader's appends leave with it, and everything else it carries waits for `Durable`. A leader acknowledges its own entries once durable.
- The leader adds a learner, swaps one that holds every entry committed at its mark for a dropped voter in one joint change, leaves on its own, removes a dropped learner, and removes a voter alone once every planned replica votes, one change at a time. It never removes itself: it hands off to a planned, caught-up voter it holds a lease from.
- A follower supports a leader while its node's grant to the leader's node holds in the support epoch it recorded. It drops votes while it supports one, except a transfer's, and during the restart wait, and stops supporting a leader that asks for a later term.
- A lease read needs a leader, no `MsgTimeoutNow` sent in its term, an entry of its term applied, and a fortified quorum in each half of its applied configuration.
- A member answers `kindMember` and a sender outside its applied configuration with a notice at its applied index. A replica believes a notice from at or past its own applied index, and only while its applied configuration holds it. A replica whose own log takes it out reports itself removed.
- A snapshot carries the applied index, its term and the configuration there. A replica outside its configuration refuses it.
- A halted replica stops applying at the entry, drops Raft messages but its followers' answers while it leads, votes and campaigns no more, still learns of its removal and hands leadership on, and compacts no further.

**Properties and their simulator invariants**

| Property | Model invariant | Simulator invariant |
| --- | --- | --- |
| At most one leader per term | `OneLeaderPerTerm` | "at most one leader per shard per term" |
| Committed entries survive every leader change and every membership change | `CommittedSurvive` | "a committed entry is never lost" |
| A lease read returns every acknowledged entry | `ReadsAcked` | "a lease read sees every applied entry" |
| No two replicas serve lease reads at once, in joint configurations too | `OneReader` | "no two replicas of a shard serve lease reads at overlapping times" |
| A replica deletes its state only once a committed configuration lacks it | `NoWrongfulDeletion` | "only a replica the group's configuration removed deletes its state" |
| After any deletion, every committed entry is durable on a quorum of each half of the newest committed configuration | `HeldAfterDeletion` | None: it follows from "a committed entry is never lost" over the replicas left, which the simulator's replacement run checks |
| A learner promoted holds every entry committed at its mark | `PromotedHolds` | "no learner is promoted to voter before it has caught up with its leader's log" |
| A halted replica applies nothing past its halt, and its log compacts no further than its state applied | `HaltStops` | None yet: `internal/meta`'s tests assert it per trace, and a simulator invariant comes with the checklist's [plants](build-checklist.md#phase-4-embedded-metastore) |
| A restart's hard state loads: its commit index is at or above the base | `Restarts` | "a committed entry is never lost", whose restarts panic otherwise |
| A swap completes and a dropped leader hands off, on small plans | `SwapCompletes` | "a replacement never stalls proposals while two voters of the old configuration are up" |

`PromotedHolds` is availability, not safety. A joint quorum still commits on the old half, so an empty voter loses nothing: `ci-eager` removes the rule and every safety property above holds. What the rule buys is that commits never wait on a newcomer.

`SwapCompletes` is a state property: with no fault, no campaign and no write, no step of a run repeats, so a fair run ends in a state with no step left, and each such state has a leader on the plan. The assumption is that every step is eventually taken, with no time in it. TLC's own fairness cost more than a thousand times the states per minute here.

**A rule removed, and the counterexample the model finds**

Each row is a negative configuration in `models/voters/` that TLC must stop at its property. Each one's trace was read.

@@NEGATIVES@@

**The notice from a non-member.** `Shard.recv` answers `kindMember` with no check that the answerer is a member, unlike `sender()`. `ci-notice` passes with that answer in the model, and a fresh replica does answer: it answers at applied 0, which no asker believes, since every replica its own configuration holds is past index 0, a boot configuration's base being index 1. `neg-boot-at-zero` shows that this is what the guard rests on: with the boot base at 0 a restarted voter asks at applied 0, a fresh replica answers, and the voter deletes its state. A check that the answerer is a member would change nothing while boots start at index 1.

**Not modelled.** Pre-vote, which changes no term or vote, and its answer. Election timers, clocks and lease expiry times: the [Epoch change model](#epoch-change-model) holds the lease-time rule, and here a lease ends when it may. The node-pair lease protocol, folded into one step. Message retries beyond a reprobe, duplicate messages, flow control. `Peers` and a node a shard does not know yet: any replica can reach any other. Which peers the plan names changing mid-run: `SetPlan` is in the constants. `refill_after` and placement, which are pure functions tested by table. Voter counts other than 3 and a replacement of more than one voter.

**Configurations** (runs as in [TLA+ and TLC](#tla-and-tlc); results in the [Model-to-simulator map](#model-to-simulator-map)). Bounds are constants and a state constraint, `Bounds`, on terms, messages in flight, messages of one kind per replica pair, crashes and support epochs.

@@CONFIGS@@

**For trace validation.** Each action is one call into a shard, so a [Trace validation pilot](build-checklist.md#phase-4-embedded-metastore) record of a message handled, a tick or a `Durable` maps to one action. The spec's header lists its compressions, each with its code steps, and each costs the pilot a mapping: `C1`, a lease grant in one step; `C2`, a grant's lapse and the restart wait's end reading the holders' state; `C3`, a crash and restart in one step; `C4`, heartbeats as empty appends; `C5`, compaction; `C6`, a configuration entry carrying its result; `C7`, the state machine's apply inside `Durable`; `C8`, no answer to a refused vote.
