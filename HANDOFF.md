# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `JOBS=6 KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. `JOBS=6` at most, 4 beside anything busy, nothing heavy beside it: the 40
  baselines and 99 plant traces took 812 s, against about 25 minutes for the baselines alone serially. Lines print
  when all are done.
- `JOBS=6 KEEP=/some/dir PLANTS="a b" FIRST=0 COUNT=40 scripts/conform.sh plants` for traces can fail.
  `CONFORM_RUNON=1` runs on past a broken invariant, one seed at a time.
- Generating 300 seeds of traces takes under 30 s and 500 MB; an `.stop` scan over them finds plant seeds fast:
  `CONFORM_DIR=x CONFORM_PLANTS=p go test ./internal/chunkstore -run '^TestPlantsKeepTraces$' -seeds 300`.
- A check keeps 28 MB of `out` per passing trace: delete kept directories as you go.
- To see why a line fails, add `PrintT(ToString(...))` in a disjunct of `Line`, in a copy of
  `trace_epoch.tla` in the check's directory, and filter TLC's output for it.
- `rm -rf` of a `$(...)` target is refused by the safety check; name the directory literally.
- A negative's cfg names its one invariant under `INVARIANTS`: copying one, change that line too.

State: 33 of 40 seeds pass, failing 2, 3, 7, 25, 31, 34, 38. 16 of 26 rules caught, 9 clean, mixed seed sets:
see the paragraph under the plant table.

NOT RUN on the final spec: `scripts/models.sh ci epoch`. Since its last pass `epoch.tla` gained `TornLists`,
`T` in `PeerOn`, three `Bug` branches, and a second conjunct in `UnchangedIntervalAccepts`. `ci-tears` passed
(1.2 million states, 158 s) before the last two; the three new negatives pass after them. Run `ci epoch` next,
about 35 minutes on 4 workers now, then fill the "Models in CI" table: it lacks `ci-edits`, `ci-restarts-moved`
and `ci-tears`, and its `ci-restarts` row predates the split. A failure of `UnchangedIntervalAccepts` on a plan
with `ownfail` or a format means the new conjunct needs its premise reviewed, not that the protocol broke.

Next, in order:
1. `scripts/models.sh ci epoch`, as above.
2. Discrepancy 9: seed 7, place `peer` before a format of a disk that answered, in `decide` in
   `tools/tracedata`. Seeds 31 and 34: `PrintT` of `Reply` in `ReplyMatches` to see the field, then read.
3. Discrepancy 11: seeds 3 and 38, a recovery write landing after its primary moved on. See its row. Moving the
   line before the primary's leaving step in `tracedata` is the small route.
4. Discrepancy 14, seed 25: unread beyond its row. A record at the primary's `fail` would place the refusal.
5. Seed 2: no verdict in 400 s and 2 GB, likewise seed 148: branching at about line 400.
6. Seeds 0-39 and the whole plant table, `JOBS=6`, with the 33 seeds. Its rows but the three new ones are still
   the 30 seeds' counts.
7. The sweep of seeds 40-339, `JOBS=4`. Generation needs `-timeout` no longer than the 10-minute default.
8. `JOBS=2 scripts/seeds.sh` once after the last non-test Go change. Not run since `recoverEmpty`.

Open from the review:
- S8: `committed` is not tied to a request, and is checked one way, so `settleSubset` passes.
- `Recovered` names no source, so `recoverFromListed` passes 4 traces. Naming the source closes it.
- Unread: the `recovered` lines of `coverAtRead` at 12 and `peerInOldInterval` at 16, and the `peer` lines of
  `neverAsk` at 27, `recoverSelfOnly` at 11 and `recoverFromListed`. The first two may be discrepancy 11's shape.
- `conform.sh plants` prints "fired at" from the first differing line, but `tracedata` reads the plan from
  the whole trace, so a rejection can come before it, as `keepID` at 35.
- Discrepancy 13, low priority: `Send` to a primary with an empty `pr`.
- `TestReplicatedWritesSurviveFaults`, the coverage report per action, and the observations that would close
  traces can fail: blocks a recovery write leaves out, what `Settle` answered per version.

Left as they are, for a decision on the way:
- `ReplicaOutput.Trace` holds the records and no loop reads them yet; the harness forwards them only while
  `conforming`, since forwarding them always added one allocation to the zero-allocation test's window, by
  the simulator's trace growing, not by `note`, which is alloc-free with capacity.
- A primary's `recoveryOf` lookup runs only on recovery writes.
