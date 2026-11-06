# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `JOBS=6 KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. `JOBS=6` at most, 4 beside anything busy, nothing heavy beside it.
- `JOBS=6 KEEP=/some/dir PLANTS="a b" FIRST=0 COUNT=40 scripts/conform.sh plants` for traces can fail.
  `CONFORM_RUNON=1` runs on past a broken invariant, one seed at a time.
- To see why a line fails, add `PrintT(ToString(...))` in a copy of `trace_epoch.tla` in the check's directory
  and filter TLC's output for it; prints come from every branch, so read the last ones.
- `rm` of a shell-variable target is refused by the safety check, and the whole command with it; name the
  directory literally or use `"${x:?}"`. A check keeps 28 MB per passing trace.
- A negative's cfg names its one invariant under `INVARIANTS`: copying one, change that line too.

State: 38 of 40 seeds pass, failing 2 (no verdict) and 25 (discrepancy 14).

NOT confirmed when this was written: `scripts/models.sh ci epoch` on the final spec (head of this branch, which
puts `Recover`'s guards before its choice of source), and the sweep of seeds 40-339. Both were running from
/private/tmp/claude-501/-Volumes-T5-Codes-rock-solid/7f074d57-9fac-4a02-a560-0408c534bfcf/scratchpad/chain2.sh, logging to /private/tmp/claude-501/-Volumes-T5-Codes-rock-solid/7f074d57-9fac-4a02-a560-0408c534bfcf/scratchpad/ci3.log and /private/tmp/claude-501/-Volumes-T5-Codes-rock-solid/7f074d57-9fac-4a02-a560-0408c534bfcf/scratchpad/sweep.log, verdict lines only. Read those, or rerun them.
The ci run before the guard fix failed only by time: `ci-restarts-moved` past 300 s, with the same state counts.
The run before the split passed all 64 configurations, slowest 275 s.

Next:
1. Confirm `ci epoch` on the final spec, then fill the results table beside `ci-restarts` in the
   "Model-to-simulator map" with `ci-edits`, `ci-restarts-moved`, `ci-tears` and its new `ci-restarts` row.
2. `unlistTorn` at seed 24 passes where the `recovered` line stopped it at 86b32d1. Removing `mayrecover` or the
   `RecoveryOver` stutter does not stop it. Compare with the trace and line rejected at 86b32d1.
3. Discrepancy 14, seed 25: add a record at the primary's `fail` and hold the member's refusal line until it.
4. Seed 2: 26 `mayfail` lines at one crash. Whether a crash loses only a suffix of the log, so the lines form a
   chain, would cut the branching; read the log's durability order before assuming it.
5. `JOBS=2 scripts/seeds.sh` passed 85,400 seeds at cdf49bd, the last Go change.

Open from the review: S8, `settleSubset`, discrepancy 13, `TestReplicatedWritesSurviveFaults`, the coverage report.
