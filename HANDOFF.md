# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table, the sweep and the plant table are in `docs/test-scenarios.md`,
"The epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `JOBS=6 KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. `JOBS=6` at most, nothing heavy beside it.
- `JOBS=6 KEEP=/some/dir PLANTS="a b" FIRST=0 COUNT=40 scripts/conform.sh plants` for traces can fail.
  `CONFORM_RUNON=1` runs on past a broken invariant, one seed at a time.
- The check copies `models/epoch/*` per trace as it runs: edit no spec during a run.
- To see why a line fails, add `PrintT(ToString(...))` in a copy of `trace_epoch.tla` in the check's directory
  and filter TLC's output for it; prints come from every branch, so read the last ones.
- `rm` of a shell-variable target is refused by the safety check, and the whole command with it; name the
  directory literally or use `"${x:?}"`. A check keeps 28 MB per passing trace.
- A negative's cfg names its one invariant under `INVARIANTS`: copying one, change that line too.

State at the head of this branch: 39 of 40 seeds pass, failing 2 (no verdict). Seeds 40-339: 268 of 300 pass, 15 no
verdict, 8 `peer`, 3 `ask`, 1 `plan`, 5 `tracedata` stops, no `order` left. `JOBS=2 scripts/seeds.sh` passed 85,400
seeds at abb8966. `epoch.tla` has not changed since `scripts/models.sh ci epoch` passed 64 of 64 at aa05106.

Open, in order:
1. `neg-recovery-read-old-disk` still has no Go plant: `recoveryAsked` refuses the formatted node's read, so a plant
   must also make the primary take that answer for an empty image, and needs a seed that breaks an invariant.
   `RecoverOrder` is what would catch it, unproven by a plant.
2. Seed 2: 26 `mayfail` lines at one crash. Whether a crash loses only a suffix of the log, so the lines form a
   chain, would cut the branching; read the log's durability order before assuming it.
3. Discrepancies 13 and 15 to 18, and the sweep's unread `peer` and `ask` seeds. Seed 191 stops at its `peer` with
   `ListsMatch` and the `TornSets` check removed too, so the list check did not cause it.
4. From the review: S8, `settleSubset`, `TestReplicatedWritesSurviveFaults`, the coverage report.
