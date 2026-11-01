# Voter replacement: review verdicts

- S1 agree: a partitioned old leader never asks and its peers never answer; a peer will answer any Raft message from a replica outside its applied configuration with the removal notice (same index check), the simpler fix.
- S2 agree: transfer only to a planned voter that is recently active and holds the leader's last index; with none, wait. A plan keeping no current voter adds learners first, then transfers.
- S3 agree, with the reviewer's call: a voter whose disk now reports from another node is placed again under a new ID; nodes counted from the disk record.
- S4 agree: trace test for the heartbeat-earlier rule on a slower link; unit test for the asker-side membership check.
- N1 agree: Durable returns early once removed.
- N2 agree: reconcile evaluated once per heartbeat; a plan added to BenchmarkProposal.
- N3 agree: planted lease read before an entry of the term commits, pinned seeds; design states a leader acknowledges a write on apply.
- N4 agree if a one-liner: learn a member's node from the sender's address.
- N5, N6, N7 agree: checklist and doc wording.

## Done
(updated as fixes land)

## Remaining
All of the above.
