-------------------------------- MODULE voters --------------------------------
\* Voter replacement (docs/design.md, "Replica placement and replacement"): one Raft group's
\* log, terms and elections, its configurations as etcd-raft applies them, the replacement
\* that joins as a learner, catches up and is swapped in for a dropped voter by one joint
\* change that the leader then leaves, a leader the plan drops keeping itself out of its
\* own swap, and the removal notice that lets a replica which missed its removal delete
\* its state.
\*
\* What the Control-plane group keeps is taken as given: the plan (Plan) is committed
\* before any replica sees it and never changes, and a replica ID is never handed out
\* twice, so the replacement is a replica no configuration has named before.
\*
\* Messages are not modelled: an election, an append and a membership question each act
\* in one step on the state of the replicas they involve, as etcd-raft's own TLA+ spec
\* checks the message-level protocol beneath. An election that fails changes nothing,
\* which is what pre-vote gives: a candidate that could not win raises no term. A
\* configuration takes effect on a replica when it applies the entry, as in etcd-raft;
\* entries hold the configuration they result in, which equals etcd-raft's delta while
\* the leader proposes one change at a time. A crash keeps the replica's log, term,
\* commit and applied indexes (its restart from a base is not modelled here). Each rule
\* the design states is a BOOLEAN constant: the ci and deep configurations set every one
\* TRUE, and each negative configuration turns one off.

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Replicas,       \* every replica ID the group's plans name
    BaseVoters,     \* the voters of the configuration the group starts with
    Plan,           \* the voters the Control-plane group's plan names for the group
    Stopped,        \* replicas down from the start, for good
    MaxTerm,        \* elections stop at this term
    MaxCrashes      \* crashes of replicas, in all

CONSTANTS
    LearnerFirst,       \* a planned replica joins as a learner (FALSE: as a voter)
    CatchUpFirst,       \* a learner is promoted only once it holds what the leader had
                        \* committed a heartbeat earlier (FALSE: at once)
    OwnTermFirst,       \* a leader proposes a change only once it applied an entry of its term
    OneAtATime,         \* a leader proposes a change only once it applied the previous one
    JointQuorum,        \* a joint configuration needs a quorum of each half (FALSE: the incoming)
    CampaignApplied,    \* a replica campaigns only once it applied every configuration change
                        \* it knows committed, etcd-raft's hasUnappliedConfChanges: one still
                        \* counting votes by an older configuration could win on a quorum
                        \* that misses the newer one's
    DeleteOnCommitted,  \* a replica deletes its state on the group's configuration
                        \* (FALSE: also when the plan drops it)
    NoticeIndex,        \* a removal notice counts only at or past the asker's applied index
    NoticeMember        \* a removal notice counts only while the asker's applied
                        \* configuration holds it

ASSUME BaseVoters \subseteq Replicas /\ Plan \subseteq Replicas /\ Stopped \subseteq Replicas

Max(S) == CHOOSE x \in S : \A y \in S : x >= y

\* A configuration: incoming voters, outgoing voters (empty unless joint) and learners.
Conf(i, o, l) == [in |-> i, out |-> o, lrn |-> l]
Base == Conf(BaseVoters, {}, {})
NoConf == Conf({}, {}, {})
Voters(c) == c.in \cup c.out
Members(c) == c.in \cup c.out \cup c.lrn
Majority(S, Q) == 2 * Cardinality(S \cap Q) > Cardinality(S)
\* A joint configuration's two halves each need a quorum, so a decision of the old
\* voters and one of the new always share a voter.
Quorum(c, Q) == Majority(c.in, Q) /\ (JointQuorum /\ c.out # {} => Majority(c.out, Q))

\* A log entry carries its term and, for a configuration change, the configuration it
\* results in (NoConf for the leader's empty entry).
IsConf(e) == e.cfg.in # {}
ConfIndex(s) == LET ks == {k \in 1 .. Len(s) : IsConf(s[k])} IN IF ks = {} THEN 0 ELSE Max(ks)
ConfOf(s) == IF ConfIndex(s) = 0 THEN Base ELSE s[ConfIndex(s)].cfg
LastTerm(s) == IF s = <<>> THEN 0 ELSE s[Len(s)].t
IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s

VARIABLES
    term,       \* per replica: its current term
    leader,     \* per replica: it leads in its term
    log,        \* per replica: its log
    commit,     \* per replica: its commit index
    applied,    \* per replica: its applied index
    match,      \* per leader: the index each replica's log is known to match its own up to,
                \* cleared when it stops leading, as is hbCommit
    hbCommit,   \* per leader: its commit index a heartbeat earlier, the one before its
                \* latest advance (the code's mark can lag further, a weaker bar)
    up,         \* per replica: it runs
    gone,       \* per replica: it deleted its state and sends nothing more
    crashes,    \* crashes so far
    elected,    \* history: <<term, replica>> of every election won
    chosen,     \* history: the committed log, each entry [e, t] with the term it first committed in
    early       \* history: replicas made voters before they held what they had to

vars == <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
          elected, chosen, early>>

AppliedConf(r) == ConfOf(SubSeq(log[r], 1, applied[r]))
LastConf(r) == ConfOf(log[r])
Live(r) == up[r] /\ ~gone[r]
Leads(r) == Live(r) /\ leader[r]
\* A replica holds what its leader had committed a heartbeat earlier.
CaughtUp(i, r) == IsPrefix(SubSeq(log[i], 1, hbCommit[i]), log[r])

NoMatch == [r \in Replicas |-> 0]

Init ==
    /\ term = [r \in Replicas |-> 0]
    /\ leader = [r \in Replicas |-> FALSE]
    /\ log = [r \in Replicas |-> <<>>]
    /\ commit = [r \in Replicas |-> 0]
    /\ applied = [r \in Replicas |-> 0]
    /\ match = [r \in Replicas |-> NoMatch]
    /\ hbCommit = [r \in Replicas |-> 0]
    /\ up = [r \in Replicas |-> r \notin Stopped]
    /\ gone = [r \in Replicas |-> FALSE]
    /\ crashes = 0
    /\ elected = {}
    /\ chosen = <<>>
    /\ early = {}

\* A voter of its own applied configuration wins the votes of a quorum of it, each voter's
\* log no newer than its own, in a term past every one of theirs. A replica votes whatever
\* its own configuration, as an etcd-raft learner does when a candidate counts it, so
\* the voters are drawn from the candidate's configuration.
Elect(i, Q) ==
    LET nt == 1 + Max({term[j] : j \in Q}) IN
    /\ Live(i) /\ i \in Q
    /\ CampaignApplied => ConfIndex(SubSeq(log[i], 1, commit[i])) <= applied[i]
    /\ Q \subseteq Voters(AppliedConf(i))
    /\ \A j \in Q : Live(j)
    /\ Quorum(AppliedConf(i), Q)
    /\ nt <= MaxTerm
    /\ \A j \in Q : \/ LastTerm(log[i]) > LastTerm(log[j])
                    \/ LastTerm(log[i]) = LastTerm(log[j]) /\ Len(log[i]) >= Len(log[j])
    /\ term' = [j \in Replicas |-> IF j \in Q THEN nt ELSE term[j]]
    /\ leader' = [j \in Replicas |-> IF j \in Q THEN j = i ELSE leader[j]]
    /\ log' = [log EXCEPT ![i] = Append(@, [t |-> nt, cfg |-> NoConf])]
    /\ match' = [j \in Replicas |->
                    IF j = i THEN [k \in Replicas |-> IF k = i THEN Len(log[i]) + 1 ELSE 0]
                    ELSE IF j \in Q THEN NoMatch ELSE match[j]]
    /\ hbCommit' = [j \in Replicas |-> IF j = i THEN commit[i] ELSE IF j \in Q THEN 0 ELSE hbCommit[j]]
    /\ elected' = elected \cup {<<nt, i>>}
    /\ UNCHANGED <<commit, applied, up, gone, crashes, chosen, early>>

\* The highest index a leader may commit given what each replica is known to hold: an entry
\* of its term that a quorum of its applied configuration holds.
Committable(i, m) ==
    LET ok(n) == log[i][n].t = term[i] /\ Quorum(AppliedConf(i), {j \in Replicas : m[j] >= n})
    IN Max({commit[i]} \cup {n \in commit[i] + 1 .. Len(log[i]) : ok(n)})

\* A leader that commits up to n keeps its commit before the advance as hbCommit; CommitTo
\* records the newly committed entries.
HbCommit(i, n) == IF n > commit[i] THEN commit[i] ELSE hbCommit[i]

CommitTo(i, n) ==
    /\ chosen' = IF n <= Len(chosen) THEN chosen
                 ELSE chosen \o [k \in 1 .. n - Len(chosen) |->
                                    [e |-> log[i][Len(chosen) + k], t |-> term[i]]]

\* The leader sends its log to a member of its applied configuration, learners included;
\* the replica keeps a longer log that agrees with it, and takes the leader's otherwise.
Replicate(i, j) ==
    /\ Leads(i) /\ Live(j) /\ j # i
    /\ j \in Members(AppliedConf(i))
    /\ term[j] <= term[i]
    /\ \/ ~IsPrefix(log[i], log[j]) \/ term[j] < term[i]
       \/ commit[j] < commit[i] \/ match[i][j] < Len(log[i])
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ leader' = [leader EXCEPT ![j] = FALSE]
    /\ log' = [log EXCEPT ![j] = IF IsPrefix(log[i], @) THEN @ ELSE log[i]]
    /\ match' = [match EXCEPT ![i][j] = Len(log[i]), ![j] = NoMatch]
    /\ LET n == Committable(i, match'[i])
       IN /\ commit' = [commit EXCEPT ![j] = IF commit[i] > @ THEN commit[i] ELSE @, ![i] = n]
          /\ hbCommit' = [hbCommit EXCEPT ![i] = HbCommit(i, n), ![j] = 0]
          /\ CommitTo(i, n)
    /\ UNCHANGED <<applied, up, gone, crashes, elected, early>>

\* The leader commits on an answer, and again when it applies a configuration that a
\* quorum of already holds (etcd-raft's maybeCommit on a configuration switch).
Commit(i) ==
    LET n == Committable(i, match[i]) IN
    /\ Leads(i) /\ n > commit[i]
    /\ commit' = [commit EXCEPT ![i] = n]
    /\ hbCommit' = [hbCommit EXCEPT ![i] = HbCommit(i, n)]
    /\ CommitTo(i, n)
    /\ UNCHANGED <<term, leader, log, applied, match, up, gone, crashes, elected, early>>

\* A replica applies every entry it knows committed. One that applies its own removal knows
\* from its log that the removal committed, and deletes its state; a leader that does keeps
\* its flag, so LeadersStay sees it.
Apply(r) ==
    /\ Live(r) /\ applied[r] < commit[r]
    /\ applied' = [applied EXCEPT ![r] = commit[r]]
    /\ gone' = [gone EXCEPT ![r] = r \in Members(AppliedConf(r))
                                   /\ r \notin Members(ConfOf(SubSeq(log[r], 1, commit[r])))]
    /\ UNCHANGED <<term, leader, log, commit, match, hbCommit, up, crashes, elected, chosen, early>>

\* etcd-raft refuses a change while an earlier one is unapplied, and a new leader counts
\* every entry it holds as such, so it proposes one only once it applied an entry of its term.
CanChange(i) ==
    /\ Leads(i)
    /\ OwnTermFirst => applied[i] > 0 /\ log[i][applied[i]].t = term[i]
    /\ OneAtATime => ConfIndex(log[i]) <= applied[i]

Propose(i, c, promoted) ==
    /\ log' = [log EXCEPT ![i] = Append(@, [t |-> term[i], cfg |-> c])]
    /\ match' = [match EXCEPT ![i][i] = Len(log[i]) + 1]
    /\ early' = early \cup {p \in promoted : ~CaughtUp(i, p)}
    /\ UNCHANGED <<term, leader, commit, applied, hbCommit, up, gone, crashes, elected, chosen>>

\* A planned replica joins as a learner: as a voter it would join every quorum while it
\* holds nothing, so with a voter down every commit would wait for it to catch up.
AddLearner(i, p) ==
    LET c == LastConf(i) IN
    /\ CanChange(i) /\ c.out = {} /\ p \in Plan \ Members(c)
    /\ IF LearnerFirst THEN Propose(i, Conf(c.in, {}, c.lrn \cup {p}), {})
       ELSE Propose(i, Conf(c.in \cup {p}, {}, c.lrn), {p})

\* One change removes a voter the plan drops and promotes a caught-up planned learner,
\* through a joint configuration. A leader never swaps itself out: one the plan drops hands
\* its leadership on (the transfer is an election here). That keeps the proposer from
\* removing itself, not every leader: LeadersStay.
Swap(i, l, d) ==
    LET c == LastConf(i) IN
    /\ CanChange(i) /\ c.out = {}
    /\ l \in c.lrn \cap Plan /\ d \in c.in \ Plan /\ d # i
    /\ CatchUpFirst => match[i][l] >= hbCommit[i]
    /\ Propose(i, Conf((c.in \ {d}) \cup {l}, c.in, c.lrn \ {l}), {l})

\* etcd-raft leaves a joint configuration on its own once the leader applied it, even a
\* leader elected while joint from the outgoing half, which then removes itself.
Leave(i) ==
    LET c == LastConf(i) IN
    /\ CanChange(i) /\ c.out # {}
    /\ Propose(i, Conf(c.in, {}, c.lrn), {})

\* A replica asks a peer whether it is still a member, or sends it a Raft message, and a
\* peer whose applied configuration lacks it answers with its applied index. An answer at
\* or past the asker's applied index, while the asker's own applied configuration holds
\* it, shows the removal committed after the asker's last applied entry; a peer behind
\* the asker may not have applied its addition yet.
Notice(r, p) ==
    /\ Live(r) /\ Live(p) /\ r # p
    /\ r \notin Members(AppliedConf(p))
    /\ NoticeIndex => applied[p] >= applied[r]
    /\ NoticeMember => r \in Members(AppliedConf(r))
    /\ gone' = [gone EXCEPT ![r] = TRUE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early>>

\* The shard map runs ahead of the group: the plan dropping a replica does not show that
\* the group removed it. Enabled only with DeleteOnCommitted off.
DeleteOnPlan(r) ==
    /\ ~DeleteOnCommitted /\ Live(r) /\ r \notin Plan
    /\ gone' = [gone EXCEPT ![r] = TRUE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early>>

Crash(r) ==
    /\ up[r] /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![r] = FALSE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ match' = [match EXCEPT ![r] = NoMatch]
    /\ hbCommit' = [hbCommit EXCEPT ![r] = 0]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<term, log, commit, applied, gone, elected, chosen, early>>

Restart(r) ==
    /\ ~up[r] /\ r \notin Stopped
    /\ up' = [up EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, gone, crashes, elected, chosen, early>>

Next ==
    \/ \E i \in Replicas, Q \in SUBSET Replicas : Elect(i, Q)
    \/ \E i, j \in Replicas : Replicate(i, j)
    \/ \E i \in Replicas : Commit(i) \/ Leave(i)
    \/ \E r \in Replicas : Apply(r) \/ DeleteOnPlan(r) \/ Crash(r) \/ Restart(r)
    \/ \E i, p \in Replicas : AddLearner(i, p)
    \/ \E i, l, d \in Replicas : Swap(i, l, d)
    \/ \E r, p \in Replicas : Notice(r, p)

Spec == Init /\ [][Next]_vars

\* Liveness with a voter down for good: every step but elections, crashes and deletions
\* on the plan's word is weakly fair, and elections stop at MaxTerm, so the last leader
\* keeps its term.
FairSpec ==
    /\ Spec
    /\ WF_vars(\E i \in Replicas, Q \in SUBSET Replicas : Elect(i, Q))
    /\ \A i, j \in Replicas : WF_vars(Replicate(i, j))
    /\ \A i \in Replicas : WF_vars(Commit(i)) /\ WF_vars(Leave(i))
    /\ \A r \in Replicas : WF_vars(Apply(r))
    /\ \A i, p \in Replicas : WF_vars(AddLearner(i, p))
    /\ \A i \in Replicas : WF_vars(\E l, d \in Replicas : Swap(i, l, d))

\* The voters the plan keeps are interchangeable (safety configurations only).
Symmetry == Permutations(BaseVoters \cap Plan)

TypeOK ==
    /\ term \in [Replicas -> 0 .. MaxTerm]
    /\ leader \in [Replicas -> BOOLEAN]
    /\ commit \in [Replicas -> Nat] /\ applied \in [Replicas -> Nat]
    /\ \A r \in Replicas : applied[r] <= commit[r] /\ commit[r] <= Len(log[r])

\* At most one leader per term.
AtMostOneLeader == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

\* Committed entries are never lost: a leader of a later term holds every one.
CommittedNeverLost ==
    \A r \in Replicas, k \in 1 .. Len(chosen) :
        Leads(r) /\ chosen[k].t < term[r] => k <= Len(log[r]) /\ log[r][k] = chosen[k].e

\* Every replica's committed entries are the group's.
CommittedAgree ==
    \A r \in Replicas : \A k \in 1 .. commit[r] : k <= Len(chosen) /\ log[r][k] = chosen[k].e

\* No replica becomes a voter before it holds what the leader had committed a heartbeat earlier.
VotersCaughtUp == early = {}

\* A replica deletes its state only once a committed configuration lacks it.
ChosenConf == ConfOf([k \in 1 .. Len(chosen) |-> chosen[k].e])
DeletesOnlyRemoved == \A r \in Replicas : gone[r] => r \notin Members(ChosenConf)

\* A leader never applies its own removal. Not checked by ci or deep: etcd-raft's auto-leave
\* breaks it (neg-outgoing-leader-leaves), and the group then waits for its followers to stop
\* supporting the removed leader's term, which this spec does not model.
LeadersStay == \A r \in Replicas : gone[r] => ~leader[r]

\* With a voter down for good, the swap commits on the survivors.
SwapCommits == <>(ChosenConf = Conf(Plan, {}, {}))
=============================================================================
