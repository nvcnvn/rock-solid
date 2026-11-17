-------------------------------- MODULE voters --------------------------------
\* Voter replacement (docs/design.md, "Replica placement and replacement"): one Raft group's
\* log, terms and elections, its configurations as etcd-raft applies them, the replacement
\* that joins as a learner, catches up and is swapped in for a dropped voter by one joint
\* change that the leader then leaves, a leader the plan drops handing its leadership on,
\* fortified leader leases, the removal notice that tells a replica which missed its
\* removal, and the plan's completion, on which a removed replica deletes its state.
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
\* the leader proposes one change at a time.
\*
\* A crash keeps the replica's log and term. It restarts from its base, the index of its last
\* compaction, at or below what its state machine applied: its applied index and
\* configuration are the base's, and its commit index is the one its hard state was last
\* written with, or the base's if higher. etcd-raft hands out a hard state only when it
\* changed, and a Ready that only moves the commit index writes it only when it commits a
\* configuration change, so a Ready that appends writes it only when it changes the term or
\* vote or moves the commit index too: the commit index an earlier commit-only Ready carried
\* may be missing from the write. The code writes the newest hard state on each durable round,
\* at least as often, and needs no more than this. The log is kept whole, since no
\* step reads an entry at or below the base except as the base's configuration and last
\* term, both kept.
\*
\* A replica halts at a committed entry its state machine cannot decode. Raft goes on
\* applying configuration changes, but the replica acts as a down one: it takes no append,
\* votes and campaigns in no election and fortifies no leader. While it leads it still takes
\* its followers' answers, commits, counts the support it had, and its node renews the
\* grants, so its followers neither campaign nor learn from its node that it is gone. Its
\* base stays below the entry, so after a restart it halts again on reaching it. A replica
\* that dies is down for good from then on.
\*
\* A replica learns its removal by applying it or from a notice. It never applies it to its
\* configuration, but stays a voter of the joint one, campaigning, voting, leading, taking
\* appends and fortifying, and serves no lease read; while it leads it hands its leadership
\* to a planned voter. A voter that holds the leave but applied only the joint
\* configuration can still win an election and needs the outgoing half's quorum to commit,
\* and the dropped voter can hold that half's newest entry. Each voter of the plan reports
\* once it applied the leave, and the plan is complete once all have; only then does the
\* node of a replica that learned its removal delete it. The authority that
\* collects the reports is whichever leader runs the control plane, the group's own for the
\* Control-plane group, and a leader that applied only the joint configuration never
\* commits an entry of its term, so never runs it.
\*
\* Fortification is held as two sides of one grant: a follower's support (it campaigns in
\* no election and votes in none but its leader's own or a transfer target's) and the
\* leader's count of it (its lease). A grant lapse ends the leader's count first by the
\* lease-time rule, then the follower's support, taken here as one Lapse step; a crash
\* lapses every grant to and from the node at once, its restart wait counted in. Each rule
\* the design states is a BOOLEAN constant: the ci and deep configurations set every one
\* TRUE, and each negative configuration turns one off.

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Replicas,       \* every replica ID the group's plans name
    BaseVoters,     \* the voters of the configuration the group starts with
    Plan,           \* the voters the Control-plane group's plan names for the group
    Stopped,        \* replicas down from the start, for good
    MaxTerm,        \* elections stop at this term
    MaxCrashes,     \* crashes of replicas, in all
    MaxHalts,       \* replicas that halt, in all
    MaxDeaths       \* replicas that die, in all

CONSTANTS
    LearnerFirst,       \* a planned replica joins as a learner (FALSE: as a voter)
    CatchUpFirst,       \* a learner is promoted only once it holds what the leader had
                        \* committed a heartbeat earlier (FALSE: at once)
    OneAtATime,         \* a leader proposes a change only once it applied the previous one
    JointQuorum,        \* a joint configuration needs a quorum of each half (FALSE: the incoming)
    CampaignApplied,    \* a replica campaigns only once it applied every configuration change
                        \* it knows committed, etcd-raft's hasUnappliedConfChanges: one still
                        \* counting votes by an older configuration could win on a quorum
                        \* that misses the newer one's
    DeleteOnCommitted,  \* a replica deletes its state on the group's configuration
                        \* (FALSE: also when the plan drops it)
    NoticeIndex,        \* a removal notice counts only at or past the asker's applied index
    NoticeMember,       \* a removal notice counts only while the asker's applied
                        \* configuration holds it
    AskLeaderNode,      \* a follower that hears nothing from the leader it supports asks the
                        \* leader's node, and stops supporting it once it deleted its state
    RemovedStopsLease,  \* a leader serves no lease read once it applied its own removal
    HaltHandsOff,       \* a halted leader hands its leadership to a voter it replicates to
    DurableCommit,      \* a restart takes the commit index its hard state was written with
                        \* (FALSE: the base's alone)
    RemovedWaitsForPlan,\* a removed replica keeps its state until the plan is complete
                        \* (FALSE: it deletes it once it learns its removal)
    DurableConfCommit,  \* a Ready that commits a configuration change writes the hard state,
                        \* and is durable before the change applies or the commit leaves, so no
                        \* restart takes a replica back across a change it knew committed
                        \* (FALSE: a Ready that only moves the commit index writes nothing)
    RemovedStaysVoter   \* a replica never applies its own removal to its configuration: it
                        \* stays a voter of the joint one, campaigning and leading, until its
                        \* node deletes it (FALSE: it applies it and campaigns no more)

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
\* The index of the last configuration entry among the first n of s naming a member of R
\* (Replicas: any), or 0; ConfAt(s, n) is the configuration they give.
RECURSIVE ConfIndexIn(_, _, _)
ConfIndexIn(s, n, R) ==
    IF n = 0 THEN 0
    ELSE IF IsConf(s[n]) /\ Members(s[n].cfg) \cap R # {} THEN n ELSE ConfIndexIn(s, n - 1, R)
ConfIndex(s) == ConfIndexIn(s, Len(s), Replicas)
ConfAt(s, n) == LET k == ConfIndexIn(s, n, Replicas) IN IF k = 0 THEN Base ELSE s[k].cfg
ConfOf(s) == ConfAt(s, Len(s))
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
    early,      \* history: replicas made voters before they held what they had to
    support,    \* per replica: {the leader of its term it supports}, or {}
    fort,       \* per leader: the replicas whose support of its term it counts
    hsCommit,   \* per replica: the commit index its hard state was last written with
    haltAt,     \* per replica: the committed entry it cannot decode, or 0
    out,        \* per replica: it learned its removal; set wherever gone is
    dead,       \* the replicas that died
    reported    \* the voters of the plan that reported the leave applied

vars == <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
          elected, chosen, early, support, fort, hsCommit, haltAt, out, dead, reported>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* With RemovedStaysVoter, a replica past its own removal keeps the last configuration it
\* applied that names it.
AppliedConf(r) ==
    LET raw == ConfAt(log[r], applied[r])
        k == ConfIndexIn(log[r], applied[r], {r})
    IN IF RemovedStaysVoter /\ r \notin Members(raw) /\ k # 0 THEN log[r][k].cfg ELSE raw
LastConf(r) == ConfOf(log[r])
Live(r) == up[r] /\ ~gone[r]
Leads(r) == Live(r) /\ leader[r] /\ (RemovedStaysVoter \/ ~out[r])
Halted(r) == haltAt[r] # 0 /\ applied[r] >= haltAt[r]

\* The Ready of each replica in S writes its hard state with commit index c. Once no crash is left nothing reads hsCommit, so it stays.
\* PassesConf(S, c) adds the replicas whose commit index c passes a configuration change.
PassesConf(S, c) ==
    S \cup {r \in Replicas : DurableConfCommit /\ \E k \in commit[r] + 1 .. c[r] : IsConf(log'[r][k])}

Persist(S, c) ==
    hsCommit' = IF crashes < MaxCrashes
                THEN [r \in Replicas |-> IF r \in S THEN c[r] ELSE hsCommit[r]]
                ELSE hsCommit
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
    /\ support = [r \in Replicas |-> {}]
    /\ fort = [r \in Replicas |-> {}]
    /\ hsCommit = [r \in Replicas |-> 0]
    /\ haltAt = [r \in Replicas |-> 0]
    /\ out = [r \in Replicas |-> FALSE]
    /\ dead = {}
    /\ reported = {}

\* Two states Spec reaches with no crash, every replica having applied the swap's joint
\* configuration. In JointInit a voter the plan keeps leads term 1, having proposed the
\* learner and the swap and committed each on every replica. OutgoingInit follows it by one
\* step: the voter the plan drops wins term 2 on every replica, so it leads the joint
\* configuration and goes on to leave it.
JointLog ==
    <<[t |-> 1, cfg |-> NoConf], [t |-> 1, cfg |-> Conf(BaseVoters, {}, Plan \ BaseVoters)],
      [t |-> 1, cfg |-> Conf(Plan, BaseVoters, {})]>>
Kept == CHOOSE x \in BaseVoters \cap Plan : TRUE
Dropped == CHOOSE x \in BaseVoters \ Plan : TRUE

\* l leads term t: Kept term 1, which knows every log matches its own and last advanced its
\* commit from 2, or Dropped term 2, which appended its empty entry and knows nothing yet.
\* Each hard state was last written with the commit index entry 3 came with, 2, unless the
\* Ready that committed entry 3, a configuration change, wrote it (DurableConfCommit), or, in
\* term 2, with the vote, 3.
JointState(l, t) ==
    /\ term = [r \in Replicas |-> t]
    /\ leader = [r \in Replicas |-> r = l]
    /\ log = [r \in Replicas |->
                IF r = l /\ t = 2 THEN Append(JointLog, [t |-> 2, cfg |-> NoConf]) ELSE JointLog]
    /\ commit = [r \in Replicas |-> 3]
    /\ applied = [r \in Replicas |-> 3]
    /\ match = [r \in Replicas |->
                  IF r # l THEN NoMatch
                  ELSE IF t = 1 THEN [k \in Replicas |-> 3]
                  ELSE [k \in Replicas |-> IF k = l THEN 4 ELSE 0]]
    /\ hbCommit = [r \in Replicas |-> IF r # l THEN 0 ELSE IF t = 1 THEN 2 ELSE 3]
    /\ up = [r \in Replicas |-> r \notin Stopped]
    /\ gone = [r \in Replicas |-> FALSE]
    /\ crashes = 0
    /\ elected = {<<1, Kept>>, <<t, l>>}
    /\ chosen = [k \in 1 .. 3 |-> [e |-> JointLog[k], t |-> 1]]
    /\ early = {}
    /\ support = [r \in Replicas |-> {}]
    /\ fort = [r \in Replicas |-> {}]
    /\ hsCommit = [r \in Replicas |-> IF t = 1 /\ ~DurableConfCommit THEN 2 ELSE 3]
    /\ haltAt = [r \in Replicas |-> 0]
    /\ out = [r \in Replicas |-> FALSE]
    /\ dead = {}
    /\ reported = {}

JointInit == JointState(Kept, 1)
OutgoingInit == JointState(Dropped, 2)

\* LeaveInit follows OutgoingInit by three steps: the dropped voter proposes the leave, sends
\* it with its empty entry to the voters other than Kept, which lags, and commits both.
LeaveLog == JointLog \o <<[t |-> 2, cfg |-> NoConf], [t |-> 2, cfg |-> Conf(Plan, {}, {})]>>

LeaveInit ==
    /\ term = [r \in Replicas |-> 2]
    /\ leader = [r \in Replicas |-> r = Dropped]
    /\ log = [r \in Replicas |-> IF r = Kept THEN JointLog ELSE LeaveLog]
    /\ commit = [r \in Replicas |-> IF r = Dropped THEN 5 ELSE 3]
    /\ applied = [r \in Replicas |-> 3]
    /\ match = [r \in Replicas |->
                  IF r # Dropped THEN NoMatch ELSE [k \in Replicas |-> IF k = Kept THEN 0 ELSE 5]]
    /\ hbCommit = [r \in Replicas |-> IF r = Dropped THEN 3 ELSE 0]
    /\ up = [r \in Replicas |-> r \notin Stopped]
    /\ gone = [r \in Replicas |-> FALSE]
    /\ crashes = 0
    /\ elected = {<<1, Kept>>, <<2, Dropped>>}
    /\ chosen = [k \in 1 .. 5 |-> [e |-> LeaveLog[k], t |-> IF k <= 3 THEN 1 ELSE 2]]
    /\ early = {}
    /\ support = [r \in Replicas |-> {}]
    /\ fort = [r \in Replicas |-> {}]
    /\ hsCommit = [r \in Replicas |-> IF r = Dropped /\ DurableConfCommit THEN 5 ELSE 3]
    /\ haltAt = [r \in Replicas |-> 0]
    /\ out = [r \in Replicas |-> FALSE]
    /\ dead = {}
    /\ reported = {}

\* A voter of its own applied configuration wins the votes of a quorum of it, each voter's
\* log no newer than its own, in a term past every one of theirs. A replica votes whatever
\* its own configuration, as an etcd-raft learner does when a candidate counts it, so
\* the voters are drawn from the candidate's configuration. A voter that supports a leader
\* votes only for that leader itself, which asks only once it no longer leads, or, on a
\* transfer, for the target of the leader in from, which steps down. A halted replica votes
\* and campaigns in none.
UpToDate(i, j) ==
    \/ LastTerm(log[i]) > LastTerm(log[j])
    \/ LastTerm(log[i]) = LastTerm(log[j]) /\ Len(log[i]) >= Len(log[j])

Run(i, Q, from) ==
    LET nt == 1 + Max({term[j] : j \in Q})
        ac == AppliedConf(i)
    IN
    /\ Live(i) /\ (RemovedStaysVoter \/ ~out[i]) /\ i \in Q
    /\ \A j \in Q : support[j] \subseteq {i} \cup from /\ ~Halted(j)
    /\ CampaignApplied => ConfIndexIn(log[i], commit[i], Replicas) <= applied[i]
    /\ Q \subseteq Voters(ac)
    /\ \A j \in Q : Live(j)
    /\ Quorum(ac, Q)
    /\ nt <= MaxTerm
    /\ \A j \in Q : UpToDate(i, j)
    /\ term' = [j \in Replicas |-> IF j \in Q THEN nt ELSE term[j]]
    /\ leader' = [j \in Replicas |-> IF j \in Q THEN j = i ELSE leader[j] /\ j \notin from]
    /\ log' = [log EXCEPT ![i] = Append(@, [t |-> nt, cfg |-> NoConf])]
    /\ match' = [j \in Replicas |->
                    IF j = i THEN [k \in Replicas |-> IF k = i THEN Len(log[i]) + 1 ELSE 0]
                    ELSE IF j \in Q \cup from THEN NoMatch ELSE match[j]]
    /\ hbCommit' = [j \in Replicas |->
                    IF j = i THEN commit[i] ELSE IF j \in Q \cup from THEN 0 ELSE hbCommit[j]]
    /\ support' = [j \in Replicas |-> IF j \in Q THEN {} ELSE support[j]]
    /\ fort' = [j \in Replicas |-> IF j \in Q \cup from THEN {} ELSE fort[j]]
    /\ elected' = elected \cup {<<nt, i>>}
    /\ Persist(Q, commit)
    /\ UNCHANGED <<commit, applied, up, gone, crashes, chosen, early, haltAt, out, dead, reported>>

Elect(i, Q) == Run(i, Q, {})

\* A leader the plan drops, out of any joint configuration or past its own removal, hands
\* its leadership to a planned voter, and a halted leader to any voter it replicates to
\* (handOff), joint or not; either serves no lease read from then on. The target is a voter
\* that took an append of the leader's term, which the code's bar, a voter replicating at or
\* past the mark, implies; the dropped leader hands off only once it committed an entry of
\* its term (reconcile).
Transfer(l, i, Q) ==
    /\ Leads(l) /\ i # l /\ match[l][i] > 0
    /\ \/ l \notin Plan /\ i \in Plan /\ (AppliedConf(l).out = {} \/ (RemovedStaysVoter /\ out[l]))
          /\ commit[l] > 0 /\ log[l][commit[l]].t = term[l]
       \/ HaltHandsOff /\ Halted(l) /\ i \in Voters(AppliedConf(l))
    /\ Run(i, Q, {l})

\* A leader asks the voters of its term for support, and each gives it: a grant to the
\* leader's node that holds is taken as given, and Lapse ends one, so any subset of them
\* supporting the leader is reached. A halted replica drops the ask and its answer.
Fortify(i) ==
    LET F == {j \in Voters(AppliedConf(i)) \ {i} : Live(j) /\ ~Halted(j) /\ term[j] = term[i]} IN
    /\ Leads(i) /\ ~Halted(i)
    /\ \E j \in F : support[j] # {i} \/ j \notin fort[i]
    /\ support' = [j \in Replicas |-> IF j \in F THEN {i} ELSE support[j]]
    /\ fort' = [fort EXCEPT ![i] = @ \cup F]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
                   elected, chosen, early, hsCommit, haltAt, out, dead, reported>>

\* j's node's grant to i's node lapses.
Lapse(j, i) ==
    /\ support[j] = {i} \/ j \in fort[i]
    /\ support' = [support EXCEPT ![j] = @ \ {i}]
    /\ fort' = [fort EXCEPT ![i] = @ \ {j}]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
                   elected, chosen, early, hsCommit, haltAt, out, dead, reported>>

\* The highest index a leader may commit given what each replica is known to hold: an entry
\* of its term that a quorum of its applied configuration holds.
Committable(i, m) ==
    LET ac == AppliedConf(i)
        ok(n) == log[i][n].t = term[i] /\ Quorum(ac, {j \in Replicas : m[j] >= n})
    IN Max({commit[i]} \cup {n \in commit[i] + 1 .. Len(log[i]) : ok(n)})

\* A leader that commits up to n keeps its commit before the advance as hbCommit; CommitTo
\* records the newly committed entries.
HbCommit(i, n) == IF n > commit[i] THEN commit[i] ELSE hbCommit[i]

CommitTo(i, n) ==
    /\ chosen' = IF n <= Len(chosen) THEN chosen
                 ELSE chosen \o [k \in 1 .. n - Len(chosen) |->
                                    [e |-> log[i][Len(chosen) + k], t |-> term[i]]]

\* The leader sends its log to a member of its applied configuration, learners included;
\* the replica keeps a longer log that agrees with it, and takes the leader's otherwise; a
\* halted replica drops it. The replica writes its hard state when its term changes, or when
\* it appends and its commit index moves in the same Ready.
Replicate(i, j) ==
    /\ Leads(i) /\ Live(j) /\ j # i /\ ~Halted(j)
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
    /\ support' = [support EXCEPT ![j] = IF term[j] < term[i] THEN {} ELSE @]
    /\ fort' = [fort EXCEPT ![j] = {}]
    /\ Persist(PassesConf(IF term[j] < term[i] \/ (log'[j] # log[j] /\ commit'[j] # commit[j]) THEN {j} ELSE {}, commit'), commit')
    /\ UNCHANGED <<applied, up, gone, crashes, elected, early, haltAt, out, dead, reported>>

\* The leader commits on an answer, and again when it applies a configuration that a
\* quorum of already holds (etcd-raft's maybeCommit on a configuration switch).
Commit(i) ==
    LET n == Committable(i, match[i]) IN
    /\ Leads(i) /\ n > commit[i]
    /\ commit' = [commit EXCEPT ![i] = n]
    /\ hbCommit' = [hbCommit EXCEPT ![i] = HbCommit(i, n)]
    /\ CommitTo(i, n)
    /\ log' = log
    /\ Persist(PassesConf({}, commit'), commit')
    /\ UNCHANGED <<term, leader, applied, match, up, gone, crashes, elected, early, support, fort,
                   haltAt, out, dead, reported>>

\* A replica applies every entry it knows committed. One that applies its own removal learns
\* from its log that the removal committed, and with RemovedStaysVoter keeps the joint
\* configuration (AppliedConf). Without it, the replica leaves that configuration, and a
\* leader that does keeps its flag and its count of support, which RemovedStopsLease alone
\* keeps from a lease read: etcd-raft leaves a joint configuration on its own, so a leader
\* elected from the outgoing half applies its own removal, in the Ready that sends the
\* commit, every follower may miss that commit, and none hears from the leader again. A
\* halted replica applies them too.
\*
\* A voter of the plan that applies the configuration leaving the joint one reports it, at
\* once here, the earliest a deletion can follow; with DurableConfCommit its hard state
\* already holds the leave's commit, so no restart rewinds what it reported.
Left(c) == c.in = Plan /\ c.out = {}

ApplyEntries(r) ==
    LET c == ConfAt(log[r], commit[r])
        removal == r \in Members(AppliedConf(r)) /\ r \notin Members(c)
    IN
    /\ Live(r) /\ applied[r] < commit[r]
    /\ applied' = [applied EXCEPT ![r] = commit[r]]
    /\ out' = [out EXCEPT ![r] = @ \/ removal]
    /\ gone' = [gone EXCEPT ![r] = removal /\ ~RemovedWaitsForPlan]
    /\ reported' = IF RemovedWaitsForPlan /\ r \in Plan /\ Left(c) THEN reported \cup {r} ELSE reported
    /\ UNCHANGED <<term, leader, log, commit, match, hbCommit, up, crashes, elected, chosen, early,
                   support, fort, hsCommit, dead>>

Apply(r) == ApplyEntries(r) /\ UNCHANGED haltAt

\* One of the entries r applies is one its state machine cannot decode.
Halt(r) ==
    /\ haltAt[r] = 0 /\ Cardinality({x \in Replicas : haltAt[x] # 0}) < MaxHalts
    /\ ApplyEntries(r)
    /\ \E h \in applied[r] + 1 .. commit[r] : haltAt' = [haltAt EXCEPT ![r] = h]

\* etcd-raft refuses a change while an earlier one is unapplied, and a new leader counts
\* every entry before its empty one, those of earlier terms, as such (raft.go,
\* pendingConfIndex in becomeLeader).
EtcdMayChange(i) ==
    /\ Leads(i)
    /\ \A k \in 1 .. Len(log[i]) : log[i][k].t < term[i] => applied[i] >= k
    /\ OneAtATime => ConfIndex(log[i]) <= applied[i]

\* The code proposes a change only once it applied an entry of its term, too.
CanChange(i) == EtcdMayChange(i) /\ applied[i] > 0 /\ log[i][applied[i]].t = term[i]

Propose(i, c, promoted) ==
    /\ log' = [log EXCEPT ![i] = Append(@, [t |-> term[i], cfg |-> c])]
    /\ match' = [match EXCEPT ![i][i] = Len(log[i]) + 1]
    /\ early' = early \cup {p \in promoted : ~CaughtUp(i, p)}
    /\ Persist({}, commit)
    /\ UNCHANGED <<term, leader, commit, applied, hbCommit, up, gone, crashes, elected, chosen,
                   support, fort, haltAt, out, dead, reported>>

\* A planned replica joins as a learner: as a voter it would join every quorum while it
\* holds nothing, so with a voter down every commit would wait for it to catch up.
AddLearner(i, p) ==
    LET c == LastConf(i) IN
    /\ CanChange(i) /\ c.out = {} /\ p \in Plan \ Members(c)
    /\ IF LearnerFirst THEN Propose(i, Conf(c.in, {}, c.lrn \cup {p}), {})
       ELSE Propose(i, Conf(c.in \cup {p}, {}, c.lrn), {p})

\* One change removes a voter the plan drops and promotes a caught-up planned learner,
\* through a joint configuration. A leader never swaps itself out: one the plan drops hands
\* its leadership on (Transfer).
Swap(i, l, d) ==
    LET c == LastConf(i) IN
    /\ CanChange(i) /\ c.out = {}
    /\ l \in c.lrn \cap Plan /\ d \in c.in \ Plan /\ d # i
    /\ CatchUpFirst => match[i][l] >= hbCommit[i]
    /\ Propose(i, Conf((c.in \ {d}) \cup {l}, c.in, c.lrn \ {l}), {l})

\* etcd-raft leaves a joint configuration on its own once the leader applied it (appliedTo),
\* with no entry of the leader's term yet committed, and even a leader elected while joint from
\* the outgoing half, which then removes itself. A leader past its own removal, still joint,
\* proposes the leave again; that entry changes nothing, so it is left out here.
Leave(i) ==
    LET c == LastConf(i) IN
    /\ EtcdMayChange(i) /\ c.out # {}
    /\ Propose(i, Conf(c.in, {}, c.lrn), {})

\* A replica asks a peer whether it is still a member, or sends it a Raft message, and a
\* peer whose applied configuration lacks it answers with its applied index. An answer at
\* or past the asker's applied index, while the asker's own applied configuration holds
\* it, shows the removal committed after the asker's last applied entry; a peer behind
\* the asker may not have applied its addition yet. With RemovedStaysVoter a leader that
\* learns its removal so goes on leading, as one that applies it does, and hands off.
Notice(r, p) ==
    /\ Live(r) /\ Live(p) /\ r # p /\ ~out[r]
    /\ r \notin Members(AppliedConf(p))
    /\ NoticeIndex => applied[p] >= applied[r]
    /\ NoticeMember => r \in Members(AppliedConf(r))
    /\ out' = [out EXCEPT ![r] = TRUE]
    /\ gone' = [gone EXCEPT ![r] = ~RemovedWaitsForPlan]
    /\ leader' = [leader EXCEPT ![r] = @ /\ RemovedStaysVoter]
    /\ fort' = [fort EXCEPT ![r] = IF RemovedStaysVoter THEN @ ELSE {}]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early, support,
                   hsCommit, haltAt, dead, reported>>

\* A follower that heard nothing from the leader it supports for an election timeout asks
\* the leader's node, as a replica asks its peers whether it is still a member. Once the
\* leader's replica learned its removal, which it does only once the removal committed, the
\* node answers that the replica left the group, and the follower stops supporting it.
AskLeader(r, l) ==
    /\ AskLeaderNode /\ Live(r) /\ support[r] = {l} /\ up[l] /\ out[l]
    /\ support' = [support EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
                   elected, chosen, early, fort, hsCommit, haltAt, out, dead, reported>>

\* Once every voter of the plan reported, the leader that runs the control plane tells the
\* node of a replica that learned its removal to delete it: no voter can lead the joint
\* configuration any more.
PlanDone == Plan \subseteq reported

Delete(r) ==
    /\ RemovedWaitsForPlan /\ Live(r) /\ out[r] /\ PlanDone
    /\ gone' = [gone EXCEPT ![r] = TRUE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ fort' = [fort EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early, support,
                   hsCommit, haltAt, out, dead, reported>>

\* The shard map runs ahead of the group: the plan dropping a replica does not show that
\* the group removed it. Enabled only with DeleteOnCommitted off.
DeleteOnPlan(r) ==
    /\ ~DeleteOnCommitted /\ Live(r) /\ r \notin Plan
    /\ gone' = [gone EXCEPT ![r] = TRUE]
    /\ out' = [out EXCEPT ![r] = TRUE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ fort' = [fort EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early, support,
                   hsCommit, haltAt, dead, reported>>

\* The node stops: its grants lapse at once, its replica's leadership with them.
Down(r) ==
    /\ up' = [up EXCEPT ![r] = FALSE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ match' = [match EXCEPT ![r] = NoMatch]
    /\ hbCommit' = [hbCommit EXCEPT ![r] = 0]
    /\ support' = [j \in Replicas |-> IF j = r THEN {} ELSE support[j] \ {r}]
    /\ fort' = [j \in Replicas |-> IF j = r THEN {} ELSE fort[j] \ {r}]
    /\ UNCHANGED <<term, log, commit, applied, gone, elected, chosen, early, hsCommit, haltAt, out,
                   reported>>

Crash(r) ==
    /\ up[r] /\ crashes < MaxCrashes
    /\ Down(r)
    /\ crashes' = crashes + 1
    /\ UNCHANGED dead

Die(r) ==
    /\ up[r] /\ Cardinality(dead) < MaxDeaths
    /\ Down(r)
    /\ dead' = dead \cup {r}
    /\ UNCHANGED crashes

\* The replica restarts from a base at or below what its state machine applied, which a
\* halted one never applied past the entry before its halt. It still knows its removal.
Restart(r) ==
    LET top == IF haltAt[r] = 0 THEN applied[r] ELSE Min({applied[r], haltAt[r] - 1}) IN
    /\ ~up[r] /\ r \notin Stopped \cup dead
    /\ up' = [up EXCEPT ![r] = TRUE]
    /\ \E b \in 0 .. top :
          /\ applied' = [applied EXCEPT ![r] = b]
          /\ commit' = [commit EXCEPT ![r] = IF DurableCommit THEN Max({b, hsCommit[r]}) ELSE b]
    /\ UNCHANGED <<term, leader, log, match, hbCommit, gone, crashes, elected, chosen, early,
                   support, fort, hsCommit, haltAt, out, dead, reported>>

LiveSteps ==
    \/ \E i, j \in Replicas : Replicate(i, j)
    \/ \E i \in Replicas : Fortify(i)
    \/ \E i \in Replicas : Commit(i) \/ Leave(i)
    \/ \E r \in Replicas : Apply(r) \/ Restart(r)
    \/ \E r, l \in Replicas : AskLeader(r, l)
    \/ \E i, p \in Replicas : AddLearner(i, p)
    \/ \E i, l, d \in Replicas : Swap(i, l, d)
    \/ \E r \in Replicas : Delete(r)

Next ==
    \/ LiveSteps
    \/ \E r \in Replicas : DeleteOnPlan(r) \/ Crash(r) \/ Halt(r) \/ Die(r)
    \/ \E r, p \in Replicas : Notice(r, p)
    \/ \E i \in Replicas, Q \in SUBSET Replicas : Elect(i, Q)
    \/ \E l, i \in Replicas, Q \in SUBSET Replicas : Transfer(l, i, Q)
    \/ \E i, j \in Replicas : Lapse(i, j)

Spec == Init /\ [][Next]_vars

\* Liveness: no grant lapses but by a crash, since a running node renews its grants, and
\* nobody campaigns while a replica leads, since its followers hear from it well within
\* an election timeout, and a candidate wins every vote it can. Only a leader crashes,
\* and it restarts, only a leader halts, and any replica may die. No replica learns its
\* removal but by applying it. Every step but crashes, halts, deaths, fortification
\* and deletions is weakly fair, and elections stop at MaxTerm.
Leaderless == \A r \in Replicas : ~Leads(r)
Votes(i, from) ==
    {j \in Voters(AppliedConf(i)) :
        Live(j) /\ ~Halted(j) /\ support[j] \subseteq {i} \cup from /\ UpToDate(i, j)}
LiveElect == Leaderless /\ \E i \in Replicas : Elect(i, Votes(i, {}))
LiveTransfer(l) == \E i \in Replicas : Transfer(l, i, Votes(i, {l}))
FairCrash(r) == Leads(r) /\ Crash(r)
FairHalt(r) == Leads(r) /\ Halt(r)

FairSpec ==
    /\ Init
    /\ [][LiveSteps \/ LiveElect \/
          \E r \in Replicas : LiveTransfer(r) \/ FairCrash(r) \/ FairHalt(r) \/ Die(r)]_vars
    /\ WF_vars(LiveElect)
    /\ \A l \in Replicas : WF_vars(LiveTransfer(l))
    /\ \A i, j \in Replicas : WF_vars(Replicate(i, j))
    /\ \A i \in Replicas : WF_vars(Commit(i)) /\ WF_vars(Leave(i))
    /\ \A r \in Replicas : WF_vars(Apply(r)) /\ WF_vars(Restart(r))
    /\ \A r \in Replicas : WF_vars(\E l \in Replicas : AskLeader(r, l))
    /\ \A i, p \in Replicas : WF_vars(AddLearner(i, p))
    /\ \A i \in Replicas : WF_vars(\E l, d \in Replicas : Swap(i, l, d))

\* No leader asks for support (a state constraint): support only narrows whom a voter
\* votes for, so every election and commit stays reachable, and lease reads go unchecked.
Unfortified == \A r \in Replicas : fort[r] = {}

\* Only the follower of JointInit that both configurations keep crashes (a state
\* constraint): its restart from a base can rewind it past both changes.
KeptFollowerCrashes == \A r \in Replicas : ~up[r] => r \in (BaseVoters \cap Plan) \ {Kept}

\* From LeaveInit, only the dropped voter and the voter that took the leave with it go down
\* (a state constraint): the faults on the voters that hold the leave and the outgoing half's
\* newest entries; a death of a or d is ci-leave-die-live's.
LeaveHoldersDown == \A r \in Replicas : ~up[r] => r \in {Dropped} \cup ((BaseVoters \cap Plan) \ {Kept})

\* The voters the plan keeps are interchangeable (safety configurations only).
Symmetry == Permutations(BaseVoters \cap Plan)

TypeOK ==
    /\ term \in [Replicas -> 0 .. MaxTerm]
    /\ leader \in [Replicas -> BOOLEAN]
    /\ commit \in [Replicas -> Nat] /\ applied \in [Replicas -> Nat]
    /\ support \in [Replicas -> SUBSET Replicas] /\ fort \in [Replicas -> SUBSET Replicas]
    /\ \A r \in Replicas : applied[r] <= commit[r] /\ commit[r] <= Len(log[r])
    /\ \A r \in Replicas : haltAt[r] <= Len(log[r])
    /\ out \in [Replicas -> BOOLEAN] /\ dead \subseteq Replicas /\ reported \subseteq Plan
    /\ \A r \in Replicas : gone[r] => out[r]

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

\* A voter's report that it applied the leave holds across a restart: its commit index
\* never falls back below the leave.
ReportsHold == \A r \in reported : Left(ConfAt(log[r], commit[r]))

\* A replica deletes its state only once a committed configuration lacks it.
ChosenConf == ConfOf([k \in 1 .. Len(chosen) |-> chosen[k].e])
DeletesOnlyRemoved == \A r \in Replicas : gone[r] => r \notin Members(ChosenConf)

\* A leader serves a lease read while it runs, has not learned its removal, has
\* committed an entry of its term, and counts the support of a quorum of each half of its
\* applied configuration, itself included.
LeaseRead(i) ==
    /\ up[i] /\ leader[i]
    /\ RemovedStopsLease => ~out[i]
    /\ commit[i] > 0 /\ log[i][commit[i]].t = term[i]
    /\ Quorum(AppliedConf(i), fort[i] \cup {i})

\* No two replicas serve lease reads at overlapping times.
OneLeaseReader == \A i, j \in Replicas : LeaseRead(i) /\ LeaseRead(j) => i = j

\* A lease read sees every committed entry.
LeaseReadsCurrent == \A i \in Replicas : LeaseRead(i) => Len(chosen) <= commit[i]

\* With a voter down for good, the swap commits on the survivors.
SwapCommits == <>(ChosenConf = Conf(Plan, {}, {}))

\* After the swap, the group has a leader in the new configuration that has not halted, even
\* when the leader that commits the leave is the dropped voter or halts.
HasNewLeader == \E r \in Plan : Leads(r) /\ ~Halted(r) /\ AppliedConf(r) = Conf(Plan, {}, {})
NewLeader == <>[]HasNewLeader
=============================================================================
