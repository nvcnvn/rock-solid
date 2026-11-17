-------------------------------- MODULE voters --------------------------------
\* Voter replacement (docs/design.md, "Replica placement and replacement"): one Raft group's
\* log, terms and elections, its configurations as etcd-raft applies them, the replacement
\* that joins as a learner, catches up and is swapped in for a dropped voter by one joint
\* change that the leader then leaves, a leader the plan drops handing its leadership on,
\* fortified leader leases, and the removal notice that lets a replica which missed its
\* removal delete its state.
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
\* written with, which a Ready that only moves the commit index never writes, or the base's
\* if higher. The log is kept whole, since no step reads an entry at or below the base
\* except as the base's configuration and last term, both kept.
\*
\* A replica halts at a committed entry its state machine cannot decode. Raft goes on
\* applying configuration changes, but the replica acts as a down one: it takes no append,
\* votes and campaigns in no election and fortifies no leader. While it leads it still takes
\* its followers' answers, commits, counts the support it had, and its node renews the
\* grants, so its followers neither campaign nor learn from its node that it is gone. Its
\* base stays below the entry, so after a restart it halts again on reaching it.
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
    MaxHalts        \* replicas that halt, in all

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
    DurableCommit       \* a restart takes the commit index its hard state was written with
                        \* (FALSE: the base's alone)

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
    early,      \* history: replicas made voters before they held what they had to
    support,    \* per replica: {the leader of its term it supports}, or {}
    fort,       \* per leader: the replicas whose support of its term it counts
    hsCommit,   \* per replica: the commit index its hard state was last written with
    haltAt      \* per replica: the committed entry it cannot decode, or 0

vars == <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
          elected, chosen, early, support, fort, hsCommit, haltAt>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

AppliedConf(r) == ConfOf(SubSeq(log[r], 1, applied[r]))
LastConf(r) == ConfOf(log[r])
Live(r) == up[r] /\ ~gone[r]
Leads(r) == Live(r) /\ leader[r]
Halted(r) == haltAt[r] # 0 /\ applied[r] >= haltAt[r]

\* A Ready that appends or changes the term or vote of the replicas in S writes their hard
\* state with commit index c. Once no crash is left nothing reads hsCommit, so it stays.
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
\* Each hard state was last written with the commit index entry 3 came with, 2, or, in term
\* 2, with the vote, 3.
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
    /\ hsCommit = [r \in Replicas |-> IF t = 1 THEN 2 ELSE 3]
    /\ haltAt = [r \in Replicas |-> 0]

JointInit == JointState(Kept, 1)
OutgoingInit == JointState(Dropped, 2)

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
    LET nt == 1 + Max({term[j] : j \in Q}) IN
    /\ Live(i) /\ i \in Q
    /\ \A j \in Q : support[j] \subseteq {i} \cup from /\ ~Halted(j)
    /\ CampaignApplied => ConfIndex(SubSeq(log[i], 1, commit[i])) <= applied[i]
    /\ Q \subseteq Voters(AppliedConf(i))
    /\ \A j \in Q : Live(j)
    /\ Quorum(AppliedConf(i), Q)
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
    /\ UNCHANGED <<commit, applied, up, gone, crashes, chosen, early, haltAt>>

Elect(i, Q) == Run(i, Q, {})

\* A leader the plan drops, out of any joint configuration, hands its leadership to a
\* planned voter, and a halted leader to any voter it replicates to (handOff), joint or
\* not; either serves no lease read from then on.
Transfer(l, i, Q) ==
    /\ Leads(l) /\ i # l
    /\ \/ l \notin Plan /\ i \in Plan /\ AppliedConf(l).out = {}
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
                   elected, chosen, early, hsCommit, haltAt>>

\* j's node's grant to i's node lapses.
Lapse(j, i) ==
    /\ support[j] = {i} \/ j \in fort[i]
    /\ support' = [support EXCEPT ![j] = @ \ {i}]
    /\ fort' = [fort EXCEPT ![i] = @ \ {j}]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
                   elected, chosen, early, hsCommit, haltAt>>

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
\* the replica keeps a longer log that agrees with it, and takes the leader's otherwise; a
\* halted replica drops it. The replica writes its hard state when its log or term changes.
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
    /\ Persist(IF log'[j] # log[j] \/ term[j] < term[i] THEN {j} ELSE {}, commit')
    /\ UNCHANGED <<applied, up, gone, crashes, elected, early, haltAt>>

\* The leader commits on an answer, and again when it applies a configuration that a
\* quorum of already holds (etcd-raft's maybeCommit on a configuration switch).
Commit(i) ==
    LET n == Committable(i, match[i]) IN
    /\ Leads(i) /\ n > commit[i]
    /\ commit' = [commit EXCEPT ![i] = n]
    /\ hbCommit' = [hbCommit EXCEPT ![i] = HbCommit(i, n)]
    /\ CommitTo(i, n)
    /\ UNCHANGED <<term, leader, log, applied, match, up, gone, crashes, elected, early, support, fort,
                   hsCommit, haltAt>>

\* A replica applies every entry it knows committed. One that applies its own removal knows
\* from its log that the removal committed, and deletes its state; a leader that does keeps
\* its flag and its count of support, which RemovedStopsLease alone keeps from a lease read.
\* etcd-raft leaves a joint configuration on its own, so a leader elected from the outgoing
\* half applies its own removal, in the Ready that sends the commit: every follower may
\* miss that commit, and none hears from the leader again. A halted replica applies them too.
ApplyEntries(r) ==
    LET c == ConfOf(SubSeq(log[r], 1, commit[r])) IN
    /\ Live(r) /\ applied[r] < commit[r]
    /\ applied' = [applied EXCEPT ![r] = commit[r]]
    /\ gone' = [gone EXCEPT ![r] = r \in Members(AppliedConf(r)) /\ r \notin Members(c)]
    /\ UNCHANGED <<term, leader, log, commit, match, hbCommit, up, crashes, elected, chosen, early,
                   support, fort, hsCommit>>

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
    /\ Persist({i}, commit)
    /\ UNCHANGED <<term, leader, commit, applied, hbCommit, up, gone, crashes, elected, chosen,
                   support, fort, haltAt>>

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
\* the outgoing half, which then removes itself.
Leave(i) ==
    LET c == LastConf(i) IN
    /\ EtcdMayChange(i) /\ c.out # {}
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
    /\ fort' = [fort EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early, support,
                   hsCommit, haltAt>>

\* A follower that heard nothing from the leader it supports for an election timeout asks
\* the leader's node, as a replica asks its peers whether it is still a member. Once the
\* leader's replica deleted its state, which it does only on its committed removal, the
\* node answers that it no longer holds it, and the follower stops supporting the leader.
AskLeader(r, l) ==
    /\ AskLeaderNode /\ Live(r) /\ support[r] = {l} /\ up[l] /\ gone[l]
    /\ support' = [support EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, leader, log, commit, applied, match, hbCommit, up, gone, crashes,
                   elected, chosen, early, fort, hsCommit, haltAt>>

\* The shard map runs ahead of the group: the plan dropping a replica does not show that
\* the group removed it. Enabled only with DeleteOnCommitted off.
DeleteOnPlan(r) ==
    /\ ~DeleteOnCommitted /\ Live(r) /\ r \notin Plan
    /\ gone' = [gone EXCEPT ![r] = TRUE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ fort' = [fort EXCEPT ![r] = {}]
    /\ UNCHANGED <<term, log, commit, applied, match, hbCommit, up, crashes, elected, chosen, early, support,
                   hsCommit, haltAt>>

Crash(r) ==
    /\ up[r] /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![r] = FALSE]
    /\ leader' = [leader EXCEPT ![r] = FALSE]
    /\ match' = [match EXCEPT ![r] = NoMatch]
    /\ hbCommit' = [hbCommit EXCEPT ![r] = 0]
    /\ crashes' = crashes + 1
    /\ support' = [j \in Replicas |-> IF j = r THEN {} ELSE support[j] \ {r}]
    /\ fort' = [j \in Replicas |-> IF j = r THEN {} ELSE fort[j] \ {r}]
    /\ UNCHANGED <<term, log, commit, applied, gone, elected, chosen, early, hsCommit, haltAt>>

\* The replica restarts from a base at or below what its state machine applied, which a
\* halted one never applied past the entry before its halt.
Restart(r) ==
    LET top == IF haltAt[r] = 0 THEN applied[r] ELSE Min({applied[r], haltAt[r] - 1}) IN
    /\ ~up[r] /\ r \notin Stopped
    /\ up' = [up EXCEPT ![r] = TRUE]
    /\ \E b \in 0 .. top :
          /\ applied' = [applied EXCEPT ![r] = b]
          /\ commit' = [commit EXCEPT ![r] = IF DurableCommit THEN Max({b, hsCommit[r]}) ELSE b]
    /\ UNCHANGED <<term, leader, log, match, hbCommit, gone, crashes, elected, chosen, early,
                   support, fort, hsCommit, haltAt>>

LiveSteps ==
    \/ \E i, j \in Replicas : Replicate(i, j)
    \/ \E i \in Replicas : Fortify(i)
    \/ \E i \in Replicas : Commit(i) \/ Leave(i)
    \/ \E r \in Replicas : Apply(r) \/ Restart(r)
    \/ \E r, l \in Replicas : AskLeader(r, l)
    \/ \E i, p \in Replicas : AddLearner(i, p)
    \/ \E i, l, d \in Replicas : Swap(i, l, d)

Next ==
    \/ LiveSteps
    \/ \E r \in Replicas : DeleteOnPlan(r) \/ Crash(r) \/ Halt(r)
    \/ \E r, p \in Replicas : Notice(r, p)
    \/ \E i \in Replicas, Q \in SUBSET Replicas : Elect(i, Q)
    \/ \E l, i \in Replicas, Q \in SUBSET Replicas : Transfer(l, i, Q)
    \/ \E i, j \in Replicas : Lapse(i, j)

Spec == Init /\ [][Next]_vars

\* Liveness: no grant lapses but by a crash, since a running node renews its grants, and
\* nobody campaigns while a replica leads, since its followers hear from it well within
\* an election timeout, and a candidate wins every vote it can. Only a leader crashes,
\* and it restarts, and only a leader halts. No replica deletes its state but by applying
\* its removal. Every step but crashes, halts and fortification is weakly fair, and
\* elections stop at MaxTerm.
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
    /\ [][LiveSteps \/ LiveElect \/ \E r \in Replicas : LiveTransfer(r) \/ FairCrash(r) \/ FairHalt(r)]_vars
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

\* The voters the plan keeps are interchangeable (safety configurations only).
Symmetry == Permutations(BaseVoters \cap Plan)

TypeOK ==
    /\ term \in [Replicas -> 0 .. MaxTerm]
    /\ leader \in [Replicas -> BOOLEAN]
    /\ commit \in [Replicas -> Nat] /\ applied \in [Replicas -> Nat]
    /\ support \in [Replicas -> SUBSET Replicas] /\ fort \in [Replicas -> SUBSET Replicas]
    /\ \A r \in Replicas : applied[r] <= commit[r] /\ commit[r] <= Len(log[r])
    /\ \A r \in Replicas : haltAt[r] <= Len(log[r])

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

\* A leader serves a lease read while it runs, has not applied its own removal, has
\* committed an entry of its term, and counts the support of a quorum of each half of its
\* applied configuration, itself included.
LeaseRead(i) ==
    /\ up[i] /\ leader[i]
    /\ RemovedStopsLease => ~gone[i]
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
NewLeader ==
    <>[](\E r \in Plan : Leads(r) /\ ~Halted(r) /\ AppliedConf(r) = Conf(Plan, {}, {}))
=============================================================================
