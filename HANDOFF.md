# Handoff: phase 2 epoch change model (remove before merge)

Branch `worktree-agent-a805bd9688ebb962d`. Spec `models/epoch/epoch.tla`; configs `ci-peering.cfg`,
`ci-leases.cfg`, `deep-peering.cfg`, `deep-leases.cfg`, 32 `neg-*.cfg`.

## Done and verified (`scripts/models.sh ci epoch`, 4 workers, M4 Pro)

| Config | Constants | Depth | Distinct states | Time |
| --- | --- | --- | --- | --- |
| ci-peering | 3 nodes; PG 1 rep3 [1,2,3], PG 2 rep2+maint [2,3]; 2 chunks; 11 plans (PeerPlans); MaxTable 5, Spare 1, counter 2, 1 fault, 1 format; lease expiry open | 31 | 981,217 | 107 s |
| ci-leases | 3 nodes; 1 chunk; 10 plans (LeasePlans); node 3 raw 960/1040, BOOTTIME slew ±22.6% on nodes 2/3; MaxTime 1100 (plans 300–600); MaxFault 2; MaxTerm 2 | 73 | 786,018 | 62 s |

Every neg-*.cfg stops at its expected property (last full run all ok; reboot variants re-run after the
heal change, both ok):
- NothingCommittedDropped: activate-before-durable, decide-by-own-interval, keep-disk-id, merge-before-kept
- NoLateCompletion: compare-member-lists, stop-previous-only
- DropsAreSuffix: log-out-of-order, peering-clears-missing
- NoResurrectedWrite: fence-on-arrival, reorder-after-other-primary
- OneWritePerVersion: restart-same-interval
- OneReader: fresh-grant-no-table-check, grantor-faster-clock, holder-slower-clock, instant-reboot,
  lease-before-bound, lease-before-bound-half-drift, margin-exact, no-boot-window, no-end-on-resume,
  no-restart-wait, no-step-check, reach-last-activated-only, read-below-min-live,
  reads-before-peer-messages, step-on-grants-only, widen-past-two-drift
- OneLeaseholder: no-epoch-bump, transfer-keeps-lease
- LeaseForms: check-before-bound, step-half-drift

## Deep results (coordinator, 2026-10-22)

The delayed-message plan moved to `deep-slow.cfg` (3 nodes, ci-leases' constants): with it,
deep-leases passed 108M states in 4 h. All three passed, recorded in test-scenarios:
deep-peering 32 / 14,838,803 / 2 h 30 min; deep-leases 132 / 5,312,889 / 9 min; deep-slow
84 / 137,228,807 / 3 h 59 min, too close to its 4 h cap: shrink it a little (e.g. MaxFault 2 → 1
or a shorter MaxTime) and rerun.

## Review, lease half (done)

1. Grantor pause made the rate bound too low (reviewer's TLC trace). Fix: a grantor's run ID, drawn at
   process start and at every Resumed (model: Wake, Thaw of a marker pause, Restart, Reboot), on every
   grant and heartbeat; a holder starts a new rate window when it changes. design.md Lease time,
   Alternatives row, kvm-clock open question (a grantor stopped in place draws none), glossary,
   build-checklist updated. Plans HandoverPause and HandoverSuspendCut (with a partition, node 3 fast)
   in deep-leases; neg-no-run-window (NoRunWindow, PausePlans) violates OneReader, 156 s.
   neg-no-boot-window now sets NoBootWindow and NoRunWindow (a reboot draws a new run ID too).
2. ci plans with suspend/pause, stop, restart and both Raft plans run node 3 4% fast;
   HandoverRebootFast in deep-leases. A step-check trip on a faulted pair taints the holder's other
   pairs (Detected), since it ends all the holder's leases.
3. Property 10 stays checked at zero round trips; build-checklist now promises only that.
4. deep-slow: MaxTime 1100 -> 500.
5. design.md, glossary, test-scenarios: a PG below min_live serves neither reads nor writes.
6. Wire rows: Lease request (holder's table epoch), Grant and node-pair heartbeat (boot ID, run ID).

Results (2026-10-23, 4 workers, 4 GB heap), recorded in test-scenarios: `scripts/models.sh ci epoch`
all ok (ci-leases 81 / 797,699 / 66 s; ci-peering 31 / 981,217 / 110 s); deep-leases 132 /
27,402,416 / 50 min; deep-slow 72 / 21,073,400 / 33 min. deep-peering unchanged, not rerun.
For the user: the run ID makes the boot ID redundant for rate windows; both are kept.

## Remaining (peering half of the review, in progress)

Done and committed: items 1-8 of the review (model b576b75, docs f4a050d) and item 9, the boot ID
dropped (786ac21; neg-no-boot-window folded into neg-no-run-window). First full ci run (before
CanStall's fault budget and item 9) all ok: ci-leases 81 / 1,215,222 / 100 s, ci-peering 30 /
2,000,812 / 218 s, ci-restarts 25 / 743,289 / 73 s. Peering decides with a subset P of members
persisting; Activate persists the rest; CanStall limits that to where a later interval, or a
restart, reboot or format still in the fault budget, can stop activation. Recover takes its read
with its write and picks the read point "at" (matters only under RecoveryCoversAtRead); separate
read and write steps multiplied ci-peering's states by more than 5. Quick checks (not committed):
NoLateCompletion fails with CompareMemberLists, MergesOnKept with MergeBeforeKept, TablesForward with
ReportBeforeActivate and with Fold's guard removed under LateReport.

In progress: sizing deep-peering, whose free plan grew past 7M states at depth 12 at 4 nodes (it was
14.8M in all before); split or shrink it to fit 4 h with liveness. Then rerun deep-peering,
deep-leases, deep-slow and full ci, record results in test-scenarios' Configuration results table.
Then: fresh review, final ci rerun, tick the build-checklist "Epoch change model" item, delete
HANDOFF.md before merge.

## Design bugs found and design.md changes

1. Writes paragraph claimed no earlier-interval write completes after k activates; false (a reached
   member acked before adopting k). Restated: completes only if k's peering kept it. Property 3
   restated in test-scenarios, build-checklist, map.
2. A kept primary alone in an interval below min_live read with no lease, and peering skips such
   intervals: reads now need min_live live members (Reads paragraph). neg-read-below-min-live.
3. A later peering must never clear a missing-list entry (torn bytes under a newer write); stated in
   "Which versions survive". neg-peering-clears-missing.
4. Keep rule made precise: start from the newest decision a reached member persisted (members persist
   the interval with their list), versions of that interval or later kept unless a reached member live
   in their interval neither holds nor lists them. Open question "Peering across several intervals"
   removed.
5. Step check only against a grant that gave a lease (else no bound ever forms for a >1% fast
   grantor); stated in Lease time. neg-check-before-bound.
6. Restart wait after a reboot relies on the reboot taking longer than (1 − 1/ρ)·primary_lease;
   stated in Reads. neg-instant-reboot.
7. Support epoch matters for Raft leader leases (property 2), not primary read leases; stated.
8. Recovery as designed could lose an acknowledged write: whole-block writes go ahead while a member
   lacks the kept version, and a recovery write copying the old block over one ordered after its read
   lost it. Recovery now leaves out the blocks covered by writes of the interval ordered before the
   recovery write, decided when it is ordered (Recovery paragraph); neg-recovery-covers-at-read.
   Also stated why an activation is reported only once done (neg-report-before-activate).
Open questions settled and removed: grant coverage (no table epoch needed; Wire messages Grant row
updated), incarnation fencing (model confirms). kvm-clock stop and rate-rise-past-headroom questions
stay open; the model leaves those cases out (stated in both).

## For the user

- ci uses membership *plans* (scripted change sequences from the fault list) instead of any change at
  any time, which did not fit 5 min; deep-peering has the free plan. Documented in design.md Testing
  strategy and test-scenarios.
- Not modelled: PG split with writes in flight (stated in test-scenarios).
- Boot ID dropped (item 9). The sessions model and its checklist item still key the client's rate
  bound "per leader node and boot"; under the run-ID rule that is per node and run. Its model's
  reboot is a superset of a new run for that purpose, but it does not model a leader's daemon
  restart or pause as a new window. Left unchanged; say if it should follow.
- The step check now compares only messages of the same run (it compared the same boot). Across a
  grantor's daemon restart the holder's lease from that grantor ends anyway at the new window, and
  its other grantors' heartbeats check its own lost time, so nothing found needs the boot ID.
