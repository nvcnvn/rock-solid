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

## Running now

`scripts/models.sh deep epoch > scratchpad/deep-run.txt` (started about 11:37 on 2026-10-22, in the
background). Runs deep-leases.cfg then deep-peering.cfg, each capped at 4 h.
- Both `ok`: put depth/states/time into docs/test-scenarios.md "Epoch change model configurations"
  (placeholders DEEPPEER*/DEEPLEASE*), tick the build-checklist "Epoch change model" item, commit.
- A property violated: read the trace; if a real design gap, fix design.md and the spec, keep a
  neg variant; if a model artifact, fix the spec and rerun ci and the negs.
- Over 4 h: shrink (deep-peering: Free plan n 3→2 or drop its write; deep-leases: drop HandoverSlow)
  and say so in the map row.

## Remaining

1. Deep results into the map (above); tick the checklist item.
2. Rerun `scripts/models.sh ci epoch` once at the end; confirm all ok; commit.
3. Delete HANDOFF.md before merge; report to the coordinator.

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
Open questions settled and removed: grant coverage (no table epoch needed; Wire messages Grant row
updated), incarnation fencing (model confirms). kvm-clock stop and rate-rise-past-headroom questions
stay open; the model leaves those cases out (stated in both).

## For the user

- ci uses membership *plans* (scripted change sequences from the fault list) instead of any change at
  any time, which did not fit 5 min; deep-peering has the free plan. Documented in design.md Testing
  strategy and test-scenarios.
- Not modelled: PG split with writes in flight (stated in test-scenarios).
