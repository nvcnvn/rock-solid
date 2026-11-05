# Handoff: spec conformance check, epoch model

Stopped at the brief's token limit. Results, the mapping and the discrepancy table are in
`docs/test-scenarios.md`, "The epoch model's check". The checklist item stays unticked.

Run: `KEEP=/some/dir RUN=TestPeeringKeepsAcknowledgedWritesAcrossPrimaries scripts/conform.sh`, or give
kept `.trace` files as arguments. A failure prints the line with no step, numbered as `Trace[line]`.
To see why, add a `PrintT` beside that line kind in a copy of `trace_epoch.tla` in the check's directory.

Next, in order:
1. Settle discrepancy 6, the spec `Order` guard: `seeds 3, 4, 5, 9`. Code: `lacks` in `replica_peer.go`
   `tryActivate`, false once a settled primary maps the kept image though the decision lists it.
   Spec: `Order` needs `miss[own][c] = NoMiss` and takes `on` from `Newest(dlog)`, a dropped version.
   The spec cannot tell a gated, dropped version from a mapped one; `Peer` knows them, from `pend` and
   `refd`, so `dec` could record per chunk whether the primary's image is the kept version. Add a
   negative with its rule row. This changes `epoch.tla`: `scripts/models.sh ci epoch` must pass.
2. Discrepancy 7 (seeds 2, 8, `mfail`): read which guard of `MemberFail` fails. Likely `m.v` in
   `dlog` after a member restart, where the code refuses a version it may hold in part.
3. Discrepancy 9 (seed 7, first `Peer`): print `Reply` beside `l.rep`.
4. Discrepancy 8 (seed 0): the test's last table asks with no primary asking. End the trace before it.
5. `primaries-1` takes 147 s: `maydeliver` and `mayfail` branches stay alive. Try `-dfid`, and drop
   a branch where the next line shows it died.
6. Traces can fail, not started. Plan: a `conform.sh` mode over a table of plant name to setter, with a
   `peerPlant` that checks every invariant and stops at a broken one, so a kept trace is a seed whose
   invariants pass. Plants for this scope: the `chunkstore.*` rows of `scripts/model-rules.tsv` with
   `TestPlantedPeeringBugsBreakTheOracle`. The `Persisted` guard is what catches `activateEarly`, and
   `Accepted` what catches `sameMembers` and `sendEpoch`; each is untested.
7. `TestReplicatedWritesSurviveFaults`: call `conformBegin` and `conformWrite` in `runFaults`, and map
   `EvDrop`, `EvPause`, disk faults (`OwnFail`, `MemberFail`).
8. Coverage report per action, from line kinds, in `conform.sh`.

Check first: `primaries-1` has 84 `order` lines but 73 `logged` and 18 `committed`, with 62 `mayfail`.
Three restarts and 16 writes in flight at most do not obviously explain 62, so a path may lack its
`Logged` or `Committed` record, which would also explain the 147 s and would leave `mayfail` too loose.

Left as they are, for a decision on the way:
- `ReplicaOutput.Trace` holds the records and no loop reads them yet; the harness forwards them only
  while `conforming`, since forwarding them always added one allocation to the zero-allocation test's window, by the
  simulator's trace growing, not by `note`, which is alloc-free with capacity.
- A primary's `recoveryOf` lookup runs only on recovery writes.
