# Handoff: review of the sessions, recall and purge models

Branch `worktree-agent-a6296739ec2d05dec`. Delete this file before merging.

## Done (committed)

- Item 1 (purge gap punch): the reviewer's purge-list gap fix fails too (a write that read the size
  before a truncate lands in the gap after it is zeroed). Replaced gap zeroing with a drain: the
  purger runs a truncate's entry only after the owner drains the file (`DrainBeforePurge`,
  `neg-no-drain`). New `ci-extend` (two extensions at different offsets). Found: a size commit whose
  check fails must raise `alloc_end` again before rewriting.
- Items 2-4 (sessions): leaders stretch a session's lease by their measured rate over the client's,
  `dead_wait` before measuring (`CountOnHolder`, `DeadWait`; `neg-count-on-own-clock`,
  `neg-dead-wait-lease`). `MarginOverVoters`, `RestartWait` and `rb` removed. Rates 16/20, drift 1/8.
  Faults enabled in `ci-evict`, `ci-faults`, `ci-rates`; every neg differs from its base by one
  constant; `neg-window-per-boot` (violates only NoSpuriousTrip). `deep-lease` (8-tick lease),
  `deep-terms` (3 terms).
- Item 5, 10, 11, 12: docs; purger progress records (`ProgressAfterPunch`,
  `neg-progress-before-punch`).
- Items 6, 7: map rewritten with a configuration-results table (ci rows filled); `ZeroGapOnExtend`
  gone so `neg-no-rewrite` changes one rule.
- Items 8, 9 (recall): Exclusive first writes don't register; a second open recalls the holder
  (`OpenRecallsHolder`, `neg-open-skips-holder`); recall numbers dropped (`AckNamesTerm`).
- Purge `FairSpec` gained weak fairness on `Land` (deep-write's PurgeSettles failed without it).

## Deep results so far

recall/deep: depth 49, 138,142,376, 1719 s. purge/deep-list: 74, 38,193,992, 402 s.
purge/deep-orphan (rerun with Land fairness): 38, 935,148, 44 s. purge/deep-write: 43, 6,337,400,
430 s. sessions/deep-down: 50,
151,663,068, 1676 s. sessions/deep-lease: 71, 803,978,256, 4904 s. sessions/deep-terms: 50,
81,717,036, 415 s. sessions/deep-fence: ran out of 4 GB heap in liveness at 64M states; shrunk to one
write in flight per client (uncommitted until rerun).

## Remaining

1. Rerun `sessions` deep-fence (shrunk, committed) alone.
2. Add the deep rows to the map's configuration-results table in `docs/test-scenarios.md`
   (after the `ci-orphan` row), and update `deep-fence`'s line in the sessions Configurations list.
3. Tick the three checklist items once their deep runs pass; delete this file; report.

## Decisions for the user

- Item 4: the measured-count rule assumes no voter's raw clock runs twice as fast as a client's,
  only for sessions a new leader has not heard from (`dead_wait`). Alternatives: client margin over
  measured bounds of every voter (no lease while any voter is unmeasured; a dead client blocks a
  fully restarted control plane), or a time-only restart wait (unsafe for a fast new boot).
- Item 1: the drain costs a round trip to other openers before a truncate's purge; extensions past
  EOF cost nothing.
- Item 8: a second open of an Exclusive file always waits for the holder.
