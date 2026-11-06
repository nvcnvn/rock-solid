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
