# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. A failure prints the line with no step, or the invariant a line
  broke. `no verdict` is neither, and is not kept.
- `KEEP=/some/dir FIRST=0 COUNT=40 scripts/conform.sh plants` for traces can fail; `PLANTS="a b"` picks
  plants. `CONFORM_RUNON=1` runs on past a broken invariant: run it one seed at a time, since over 40 seeds
  it passes Go's 10-minute test limit.
- To see why a line fails, add `PrintT(ToString(...))` in a disjunct of `Line`, in a copy of
  `trace_epoch.tla` in the check's directory, and filter TLC's output for it.
- `scripts/models.sh ci epoch` on 4 workers took 32 minutes here, not 8: don't run it beside trace checks.

State: 5 seeds of 40 pass (6, 11, 18, 23, 36). No unplanted seed broke a safety invariant. 11 of 21 rules
caught, 4 clean.

Open from the review:
- S8: `committed` is not tied to a request, and is checked one way, so `settleSubset` passes.
- The four invariants checked at the end run only on a trace that reaches it, on the first branch there:
  a failing seed has evaluated none of them.
- `neg-edit-keeps-interval` has no Go plant: its row is open. `slices.Equal` at `placement.go:223` would
  compare as sets; `TestPlantedPeeringBugsBreakTheOracle` should catch it by `invOneVersion`.

Needs a decision:
- The `activate` line needs every up member's `Persisted`. That reads as an observation, not a rule, but it
  sits in the wrapper: say if it restates `Activate`.

Next, in order:
1. Settle discrepancy 6, the spec `Order` guard, 17 seeds. Code: `lacks` in `replica_peer.go` `tryActivate`,
   false once a settled primary maps the kept image though the decision lists it. Spec: `Order` needs
   `miss[own][c] = NoMiss` and takes `on` from `Newest(dlog)`, a dropped version. `Peer` knows gated from
   mapped, from `pend` and `refd`, so `dec` could record per chunk whether the primary's image is the kept
   version. Add a negative with its rule row. This changes `epoch.tla`: `scripts/models.sh ci epoch` must pass.
2. Discrepancy 7, 8 seeds: read why a member refuses with no disk fault. `MemberFail` has no budget in a
   trace, and that is what catches `sendEpoch`, so settling 7 must keep a refusal for a table epoch rejected:
   give the refusal its own reason in the record, rather than restoring the budget.
3. Discrepancy 12, seeds 1, 5, 24, new: a member's `logged` above its `whole`. Read what such a version is;
   `keep.go` lists that member and the spec's `Entry` would not.
4. Discrepancy 10, seeds 27, 38: a repeat `Persisted` after activation. `activateEarly` rests on this guard,
   so tell a repeat from a first persist rather than dropping it.
5. Discrepancy 9, seeds 7, 31, and 11, seed 22; discrepancy 8, seeds 0, 26.
6. Unread lines in the plant table: `neverAsk` at seed 23, `recoverSelfOnly` at 11, 18, 36.
7. Discrepancy 13, low priority: `Send` to a primary with an empty `pr`.
8. Observations that would close traces can fail: blocks a recovery write leaves out (`coverAtRead`), what
   `Settle` answered per version (`settleSubset`). `fenceOnArrival`, `reorderAfterOther`, `settleByNumber`,
   `readBelowMinLive` need a workload that keeps a trace.
9. `TestReplicatedWritesSurviveFaults`: call `conformBegin` and `conformWrite` in `runFaults`, and map
   `EvDrop`, `EvPause`, disk faults (`OwnFail`, `MemberFail`). `belowProtocol` lists those events today:
   move them out when they map.
10. Coverage report per action, from line kinds, in `conform.sh`.

Left as they are, for a decision on the way:
- `ReplicaOutput.Trace` holds the records and no loop reads them yet; the harness forwards them only while
  `conforming`, since forwarding them always added one allocation to the zero-allocation test's window, by
  the simulator's trace growing, not by `note`, which is alloc-free with capacity.
- A primary's `recoveryOf` lookup runs only on recovery writes.
