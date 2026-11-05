# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. A failure prints the line with no step, or the invariant a line
  broke. `no verdict` is neither, and is not kept. The 40 seeds now take about 25 minutes.
- `KEEP=/some/dir FIRST=6 COUNT=1 scripts/conform.sh plants` for traces can fail; `PLANTS="a b"` picks
  plants. `CONFORM_RUNON=1` runs on past a broken invariant: run it one seed at a time.
- To see why a line fails, add `PrintT(ToString(...))` in a disjunct of `Line`, in a copy of
  `trace_epoch.tla` in the check's directory, and filter TLC's output for it.
- `scripts/models.sh ci epoch` on 4 workers takes about 32 minutes: don't run it beside trace checks.
  `ci-restarts` took 293 s of its 300 here, so a state more may need its largest plan moved.

State: 30 seeds of 40 pass. No unplanted seed broke a safety invariant.

Needs a decision:
- Discrepancy 12, seeds 1, 5, 24: a crash leaves part of a version, and `keep` lists that member where
  `Entry` does not. The kept version is the same. See its row.

Open from the review:
- S8: `committed` is not tied to a request, and is checked one way, so `settleSubset` passes.
- `neg-edit-keeps-interval` and `neg-merge-on-dropped` have no Go plant: their rows are open.
  `slices.Equal` at `placement.go:223` would compare as sets, and `TestPlantedPeeringBugsBreakTheOracle`
  should catch it by `invOneVersion`. Dropping `x.Version != k.kept` from `lacking` is the other.

Next, in order:
1. Discrepancy 9: seed 7 needs `Peer` taken before a format that follows the reply; read seeds 31 and 34.
2. Discrepancy 11: seeds 3 and 38 stop at `Recovered`; at 38 the primary restarted first, so it may be a
   recovery write landing after its primary's crash.
3. Discrepancy 14, seed 25: a record at the primary's `fail` would place the refusal when it hears.
4. Seed 2: no verdict in 400 s and 2 GB.
5. Unread lines in the plant table, and the plants that fire on none of the seeds used: the plant table
   can now use the 30 passing seeds, not 5.
6. Discrepancy 13, low priority: `Send` to a primary with an empty `pr`.
7. Observations that would close traces can fail: blocks a recovery write leaves out (`coverAtRead`), what
   `Settle` answered per version (`settleSubset`). `fenceOnArrival`, `reorderAfterOther`, `settleByNumber`,
   `readBelowMinLive` need a workload that keeps a trace.
8. `TestReplicatedWritesSurviveFaults`: call `conformBegin` and `conformWrite` in `runFaults`, and map
   `EvDrop`, `EvPause`, disk faults (`OwnFail`, `MemberFail`). `belowProtocol` lists those events today:
   move them out when they map.
9. Coverage report per action, from line kinds, in `conform.sh`.

Left as they are, for a decision on the way:
- `ReplicaOutput.Trace` holds the records and no loop reads them yet; the harness forwards them only while
  `conforming`, since forwarding them always added one allocation to the zero-allocation test's window, by
  the simulator's trace growing, not by `note`, which is alloc-free with capacity.
- A primary's `recoveryOf` lookup runs only on recovery writes.
