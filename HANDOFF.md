# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries scripts/conform.sh`, or kept
  `.trace` files as arguments. A failure prints the line with no step.
- `KEEP=/some/dir FIRST=0 COUNT=40 scripts/conform.sh plants` for traces can fail; `PLANTS="a b"` picks
  plants and `CONFORM_RUNON=1` runs on past a broken invariant. All plants over 6 passing seeds takes
  about 40 minutes, in turn, mostly seeds 1 and 11. `KEEP` skips a check with spec and data unchanged.
- To see why a line fails, add `PrintT(ToString(...))` in a disjunct of `TraceNext` guarded on that line,
  in a copy of `trace_epoch.tla` in the check's directory.

Next, in order:
1. Settle discrepancy 6, the spec `Order` guard, printed at all 19 seeds. Code: `lacks` in
   `replica_peer.go` `tryActivate`, false once a settled primary maps the kept image though the decision
   lists it. Spec: `Order` needs `miss[own][c] = NoMiss` and takes `on` from `Newest(dlog)`, a dropped
   version. `Peer` knows gated from mapped, from `pend` and `refd`, so `dec` could record per chunk
   whether the primary's image is the kept version. Add a negative with its rule row. This changes
   `epoch.tla`: `scripts/models.sh ci epoch` must pass.
2. Discrepancy 7, 8 seeds: read why a member refuses with no disk fault. `MemberFail` has no budget in a
   trace now, and that is what catches `sendEpoch`, so settling 7 must keep a refusal for a table epoch
   rejected: give the refusal its own reason in the record, rather than restoring the budget.
3. Discrepancy 10, seeds 27, 38: a repeat `Persisted` after activation. `activateEarly` rests on this
   guard, so tell a repeat from a first persist rather than dropping it.
4. Discrepancy 9, seeds 7, 31, and 11, seed 22; discrepancy 8, seeds 0, 26.
5. `Send` to a primary whose `pr` is empty after a restart loses the write in the spec, and the code
   holds it. `neverAsk` shows it; no unplanted seed of 40 stops there. Not in the table: decide whether
   it is a discrepancy.
6. Observations that would close traces can fail: blocks a recovery write leaves out (`coverAtRead`),
   what `Settle` answered per version (`settleSubset`). `fenceOnArrival`, `reorderAfterOther`,
   `settleByNumber`, `readBelowMinLive` need a workload that keeps a trace.
7. `primaries-1` and `-11` take 141 s and 222 s: `maydeliver` and `mayfail` branches stay alive. Try
   `-dfid`, and drop a branch where the next line shows it died.
8. `TestReplicatedWritesSurviveFaults`: call `conformBegin` and `conformWrite` in `runFaults`, and map
   `EvDrop`, `EvPause`, disk faults (`OwnFail`, `MemberFail`).
9. Coverage report per action, from line kinds, in `conform.sh`.

Left as they are, for a decision on the way:
- `ReplicaOutput.Trace` holds the records and no loop reads them yet; the harness forwards them only
  while `conforming`, since forwarding them always added one allocation to the zero-allocation test's window, by the
  simulator's trace growing, not by `note`, which is alloc-free with capacity.
- A primary's `recoveryOf` lookup runs only on recovery writes.
