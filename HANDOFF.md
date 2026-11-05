# Handoff: spec conformance check, epoch model

Results, the mapping, the discrepancy table and the plant table are in `docs/test-scenarios.md`, "The
epoch model's check" and "Traces can fail in the epoch model's check". The checklist item stays unticked.

Run:
- `KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries FIRST=0 COUNT=40 scripts/conform.sh`,
  or kept `.trace` files as arguments. A failure prints the line with no step, or the invariant a line
  broke. `no verdict` is neither, and is not kept. The 40 seeds now take about 25 minutes.
- `KEEP=/some/dir FIRST=6 COUNT=1 scripts/conform.sh plants` for traces can fail, one seed at a time;
  `PLANTS="a b"` picks plants. `CONFORM_RUNON=1` runs on past a broken invariant: run it one seed at a time.
- To see why a line fails, add `PrintT(ToString(...))` in a disjunct of `Line`, in a copy of
  `trace_epoch.tla` in the check's directory, and filter TLC's output for it.
- `scripts/models.sh ci epoch` on 4 workers takes about 32 minutes: don't run it beside trace checks.
  It passes, 60 of 60. `ci-restarts-moved` takes 259 s of its 300.

State: 30 seeds of 40 pass: 0, 4, 6, 8 to 23, 26 to 30, 32, 33, 35, 36, 37, 39. No unplanted seed broke a
safety invariant. Traces can fail holds for 13 of 23 rules, 7 clean, over all 30 seeds; the table has the
counts per plant. Those 30 use the relaxed paths: `img` in `Order` 19, `img` in `Recover` 4, `Recover` at
K = 0 only seed 22, `GoneRefuses` 8.

To measure a path: copy `epoch.tla` beside the kept `trace_data.tla`, remove the path, run TLC on
`trace_epoch.tla`. A seed that stops needs it. `plants` over the 30 takes about 3 hours one seed at a time.

Needs a decision:
- Discrepancy 12, seeds 1, 5, 24: a crash leaves part of a version, and `keep` lists that member where
  `Entry` does not. Listing is required, so the spec needs a torn version in `Entry` or a choice in `Peer`.
  See its row.

Open from the review:
- S8: `committed` is not tied to a request, and is checked one way, so `settleSubset` passes.
- No plant targets `img` as a recovery source, `Recover` at K = 0 or `GoneRefuses`: nothing yet shows those
  paths reject a wrong trace.
- Unread: 3 plant traces pass only by a relaxed path their baseline does not need: `neverAsk` at 6 and 22
  (`img` in `Order`), `freshGrantNoCheck` at 37 (`img` in `Recover`). Also the `recovered` lines of
  `coverAtRead` at 12 and `peerInOldInterval` at 16, and the `peer` lines of `neverAsk` at 27 and
  `recoverSelfOnly` at 11. The first two may be discrepancy 11's shape.
- The results table in "Models in CI" has no row for `ci-edits` or `ci-restarts-moved`, and its `ci-restarts`
  row predates the split: it needs a `scripts/models.sh ci epoch` run.
- `conform.sh plants` prints "fired at" from the first differing line, but `tracedata` reads the plan from
  the whole trace, so a rejection can come before it, as `keepID` at 35.

Next, in order:
1. Discrepancy 9: seed 7 needs `Peer` taken before a format that follows the reply; read seeds 31 and 34.
2. Discrepancy 11: seeds 3 and 38 stop at `Recovered`; at 38 the primary restarted first, so it may be a
   recovery write landing after its primary's crash.
3. Discrepancy 14, seed 25: a record at the primary's `fail` would place the refusal when it hears.
4. Seed 2: no verdict in 400 s and 2 GB.
5. Plants for the relaxed paths above, and the four plants that fire on none of the 30.
6. Discrepancy 13, low priority: `Send` to a primary with an empty `pr`. `order` rejections of
   `peerInOldInterval` and `neverAsk` stop there.
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
