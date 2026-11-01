# Handoff: Leader leases, finding 1 (restarted node asks no lease during its restart wait)

Branch `worktree-agent-a66751dd8cedc0593`. Done and committed: the Leader leases item (9594dae,
bdc0ecd, 397026e, f2590e6) and the review's findings 2 to 6 (e7f0cc6). Lint, `go test -p 4 ./...`,
`scripts/raft-tests.sh` and `JOBS=2 scripts/seeds.sh TestRaftRunsReplayFromTheirSeed` (2,000 seeds,
15 s) pass at e7f0cc6.

## Remaining: the user's decision on finding 1

A restarted node's lease driver asks for no node-pair lease while its own grantor waits out the
restart wait (`lease.Grantor.Waiting`). The grants its old run held then lapse as after a crash,
their support epochs bump, and Raft followers run their lapse campaign instead of waiting for the
restarted leader's election timeout. Reviewer's prototype measured crash to first post-crash apply
p50 4.06 s / p90 5.28 s (was 5.56 / 6.23), 2,000 seeds clean. Cost: a restarted chunkstore primary
activates about `rate_gap` (100 ms) later. Do it as its own commit.

1. **chunkstore** (`internal/chunkstore/replica_lease.go`): `leaseTick` skips asks while
   `r.grantor.Waiting(t)`; `armLease` arms no earlier than `r.grantor.WaitEnd(t)` during the wait.
   The patch is in the session scratchpad,
   `/private/tmp/claude-501/-Volumes-T5-Codes-rock-solid/b875c9c1-668c-48fe-bf5b-4496753598e7/scratchpad/restart-no-ask.diff`
   (about 15 lines). With it, these fail and need work:
   - `TestPlantedPeeringBugsBreakTheOracle`, `TestPlantedPeeringBugsBreakTheFaultRun`,
     `TestPlantedReadBugsBreakTheReadRun`, `TestPlantedBugsBreakChunkHistories`: each pins the seed
     where a plant breaks its invariant; the timing shift moves them. Re-find each seed (the
     comments and test-scenarios.md give "k of 500" counts too; update both).
   - `TestTheMostDegradedPGRecoversFirst` (`replica_recover_test.go:335`): recovery order came out
     `[9 9 4 4 4 4 12 12]`; probably the PGs' primaries now hold leases later after the cluster
     starts, so check whether the test must run past the restart wait plus `rate_gap` before it
     publishes, rather than the order rule breaking.
   - Then run chunkstore's seeded tests in `scripts/seeds.sh` (one at a time, `JOBS=2`) and
     `TestNoLeaseOutlastsItsGrant`.
2. **raftshard simulator** (`internal/raftshard/sim_test.go`, `leaseTick`): skip asks while
   `r.grantor.Waiting(t)`. Keep the liveness fix in `shard.go` `refuses` (support ends at the
   supported leader's own later-term (pre-)vote) unless the reviewer says otherwise; it still
   covers a leader that stepped down without restarting.
3. **Docs** (use the `design-doc` skill):
   - design.md "Leases" restart-wait text (Epoch changes, about line 215: "until both its clocks show
     one `primary_lease` since it started it grants no vote and joins no peering"): add that it asks
     for no lease either, and why (the old run's grants lapse and their support epochs bump).
   - design.md lapse paragraph (about line 737, "That is how a group learns that its leader's node
     restarted in place..."): rewrite so a restarted leader's groups fail over like a crash, after
     the grants lapse plus `raft_lapse_campaign`; the own-campaign rule becomes the case of a leader
     that stepped down.
   - design.md Timing bullet (about line 711): the 7.1 s budget must hold again; restate the
     restart case within it.
   - test-scenarios.md Leader leases: the "Supports L, and L itself asks..." row and Lease clocks
     fault 5's "a leader restarted in place" clause; Peering and read leases if a chunkstore
     scenario names the restart wait.
4. Rerun lint, `go test -p 4 ./...`, the seeds touched, then report to the coordinator.

## Pitfalls

- Shell heredocs and `python3 - <<EOF` are refused by the worktree guard; write scripts to the
  scratchpad and run them, or use Edit.
- Resource rules: `go test -p 4`, `GOMEMLIMIT=4GiB`, one heavy command at a time.
