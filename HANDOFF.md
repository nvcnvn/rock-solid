# Handoff: phase 2 epoch change model (remove before merge)

Branch `worktree-agent-a805bd9688ebb962d`. Spec `models/epoch/epoch.tla`; configs `ci-peering`,
`ci-restarts`, `ci-leases`, `deep-peering`, `deep-leases`, `deep-slow`, 34 `neg-*.cfg`.

## Results (2026-10-24, 4 workers, 4 GB heap, M4 Pro; recorded in test-scenarios)

`scripts/models.sh ci epoch` all ok: ci-leases 81 / 1,212,947 / 94 s; ci-peering 30 / 1,789,778 /
193 s; ci-restarts 24 / 552,713 / 51 s; slowest negative neg-no-run-window 212 s. Deep:
deep-leases 132 / 37,736,534 / 66 min; deep-slow 72 / 21,444,840 / 33 min; deep-peering
31 / 11,847,313 / 89 min.

Every negative violates its expected property:
- NothingCommittedDropped: activate-before-durable, compare-member-lists, decide-by-own-interval,
  keep-disk-id, merge-before-kept, recovery-covers-at-read, report-before-activate,
  stop-previous-only
- UnchangedIntervalAccepts: compare-table-epochs
- DropsAreSuffix: log-out-of-order, peering-clears-missing
- NoResurrectedWrite: fence-on-arrival, reorder-after-other-primary
- OneWritePerVersion: restart-same-interval
- OneReader: fresh-grant-no-table-check, grantor-faster-clock, holder-slower-clock, instant-reboot,
  lease-before-bound, lease-before-bound-half-drift, margin-exact, no-end-on-resume,
  no-restart-wait, no-run-window, no-step-check, reach-last-activated-only, read-below-min-live,
  reads-before-peer-messages, step-on-grants-only, widen-past-two-drift
- OneLeaseholder: no-epoch-bump, transfer-keeps-lease
- LeaseForms: check-before-bound, step-half-drift

Quick checks, not committed: NoLateCompletion (property 3) fails with CompareMemberLists,
MergesOnKept (8) with MergeBeforeKept, TablesForward (9) with ReportBeforeActivate and with Fold's
guard removed under the LateReport plan.

## Review fixes (both halves done)

Lease half: grantor run ID on grants and heartbeats, faults paired with a 4%-fast grantor, property
10 at zero round trips, deep-slow shortened, reads stop below min_live, wire rows. Then the boot ID
was dropped (the run ID changes at every start, so at every reboot).
Peering half: incarnation table per PG, forgotten on a primary change; delivered writes stay in the
network; the two incarnation counterexample rows restored; peering decides with any subset of
members persisting and activates later where a later interval, restart, reboot or format can stop
it; recovery leaves out blocks as of its write (neg-recovery-covers-at-read); property 3 restated
(the old one is part of 4), 7 with the table epoch in messages (neg-compare-table-epochs), 8 by the
merge chain, 9 with "the recorded activation happened" (neg-report-before-activate) and a plan that
folds a late report; chunk 2 has two blocks and a maintenance-with-merges plan; ci-restarts holds
the restart and format plans; docs moved (results rows to the map's results table, property rows
to the epoch section's property table), duplicated constants removed, the free plan and
build-checklist wording fixed, the 12,000/s figure moved, the two-bug variant noted.

## Final review (findings in the session scratchpad, review-epoch2/findings.md)

Done and committed: F1 run ID ends the old run's lease (35a0197), F5 arrived per message (863964e),
F2 Settled escape reqIv + neg-restart-no-ask (3f27f44), F8 no-up intervals skipped (e2b2b6f), F7
recovery version (98d1f53), F3+F4 sessions boot -> run (103dd9f), F9 checklist pointer (332f19f).
In progress: F6 (move a plan from ci-peering to ci-restarts; per-plan state counts being measured),
then ci epoch, ci sessions, deep-leases, deep-peering, deep-slow, results into test-scenarios
(all wall times in seconds).

## Remaining

1. Finish the final review items above.
2. Tick the build-checklist "Epoch change model" item.
3. Delete HANDOFF.md before merge.

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
- Recovery is modelled as one step that chooses when its read happened (any point since
  activation), not two steps: what the read returns never changes before the write, and separate
  steps multiplied ci-peering's states by more than 5. Likewise a member persisting its missing list
  is folded into the decision or the activation. Both reductions are argued in epoch.tla's comments.
- ci-peering takes 193 s of its 300 s on the M4 Pro. If CI's runner is slower, split its plans by
  write family (partial writes, session writes) rather than raise the budget.
- The free plan has no fault now: with a restart it passed 20M states without finishing.
