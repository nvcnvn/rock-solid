------------------------------- MODULE voters -------------------------------
\* What rock-solid adds to etcd-raft in one group: the plan-driven voter swap,
\* fortified leases and lease reads, the removal notice, snapshots at the
\* applied index, durable rounds, and a replica halted at an entry it cannot
\* decode. Raft is here only as far as those rules rest on it: a log with
\* terms, votes, commit and apply. Pre-vote, ticks and the message-level
\* details that decide nothing here are left out. See "Voter replacement
\* model" in docs/test-scenarios.md.
\*
\* Each action is one call into a shard, named for the code: a message handled,
\* a tick, a Durable report. Emit ends each as Shard.ready ends the call, so a
\* Ready, its round and its sends are in the action that caused them, and an
\* apply is in the Durable or the Ready that hands the entry over. The action's
\* comment names its function. The steps an action folds beyond that are listed
\* here, and a trace pays for each.
\*
\* Compressions, each a cost for trace validation:
\*   C1 Grant folds the node-pair lease's Holder.Ask, Grantor.Grant and
\*      Holder.OnGrant into one step reading both nodes: epoch.tla models that
\*      protocol, and its messages here would multiply every plan.
\*   C2 Lapse and EndWait read the holders' state: that is how a model with
\*      open expiry says the grantor waits for the slower clock.
\*   C3 Restart folds a crash and the restart. A node that stays down is one
\*      whose events do not happen.
\*   C4 A heartbeat and its answer are an empty append and its answer, sent
\*      when the commit index moved, so a run has no idle cycle of them.
\*   C5 Compact folds the memtable flush, Shard.Applied and Host.Compact.
\*   C6 A conf entry carries the configuration it produces, not the changes:
\*      the leader proposes one change at a time on the applied configuration,
\*      so applying the changes gives the same result.
\*   C7 The state machine's apply is inside Durable and ApplyReady, as Apply
\*      is called inside done.
\*   C8 A vote refused carries no answer, and a replica told of a later term
\*      by a vote answer takes it in RecvHigher.

EXTENDS Integers, Sequences, FiniteSets, TLC

Range(f) == {f[i] : i \in DOMAIN f}

CONSTANTS
    Replica,      \* Raft replica IDs, each on a node of its own
    InitVoters,   \* the group's boot configuration, replica 1 leading term 1
    Plan,         \* the voters of the shard map, {} for none (Shard.SetPlan)
    Values,       \* the client writes
    Poison,       \* the value CannotDecode cannot decode, 0 for none
    CannotDecode,
    Events,       \* the optional events a run may take
    Candidates,   \* the replicas a timer may start a campaign on
    MaxTerm,      \* bounds, checked in Bounds and nowhere else
    MaxMsgs,
    MaxLink,      \* messages of one kind in flight from one replica to another
    MaxCrash,
    MaxEpoch,
    Bugs

VARIABLES
    rep,    \* per replica: a Replica record, see Fresh
    msgs,   \* the network: a message can be delayed, reordered or lost
    grant,  \* grant[g][h]: g's grant to h as g holds it
    hold,   \* hold[h][g]: g's lease as h holds it
    \* History, which no replica reads.
    elected, \* every [t, r] that won
    cmt,     \* every [i, e] a leader committed
    acked,   \* every [i, e] a client was told of
    lagged,  \* a voter was added not holding every entry committed at the mark
    crashes

vars == <<rep, msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

Bug(x) == x \in Bugs
On(x) == x \in Events
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : y >= x

NoConf == [v |-> {}, o |-> {}, l |-> {}]
Conf(v) == [v |-> v, o |-> {}, l |-> {}]
Members(c) == c.v \cup c.o \cup c.l
Voters(c) == c.v \cup c.o
Maj(q, vs) == vs = {} \/ 2 * Cardinality(q \cap vs) > Cardinality(vs)
Quorum(q, c) == Maj(q, c.v) /\ (Bug("OneHalfCommit") \/ Maj(q, c.o))

Entry(t, k, v, c) == [t |-> t, k |-> k, v |-> v, c |-> c]
Boot == Entry(0, "n", 0, NoConf)
NoRound == [hs |-> FALSE]
NoSnap == [i |-> 0, t |-> 0, c |-> NoConf]
NoFort == [t |-> 0, run |-> 0, ep |-> 0]
NoSupport == [t |-> 0, ep |-> 0, lead |-> 0]
NoIndex == [p \in Replica |-> 0]
NoFortifies == [p \in Replica |-> NoFort]

Committed(s) == {[i |-> i, e |-> s.log[i]] : i \in 1..s.commit}
LastTerm(log) == IF log = <<>> THEN 0 ELSE log[Len(log)].t
TermAt(log, i) == IF i = 0 THEN 0 ELSE IF i > Len(log) THEN -1 ELSE log[i].t
HasConf(log, lo, hi) == \E i \in lo..hi : log[i].k = "c"

\* A replica's state. term to xfer are the RawNode's; stable what it knows
\* durable and round what it wrote and was not told of; base the storage's
\* last compaction or restored snapshot; conf, applied, committed, confTerm,
\* mark, transferred, support and fort the shard's; sm and halt the state
\* machine's. log runs from index 1 and keeps the entries below the base as
\* the state machine's contents, which only a snapshot reads.
Fresh(log, cm, base, term, run) ==
    [term |-> term, vote |-> 0, role |-> "F", lead |-> 0, log |-> log, commit |-> cm,
     votes |-> {}, next |-> NoIndex, match |-> NoIndex, sent |-> NoIndex, xfer |-> 0, pci |-> 0,
     psnap |-> NoSnap,
     rconf |-> base.c, seen |-> cm, pend |-> {}, round |-> NoRound,
     stable |-> [term |-> term, vote |-> 0, log |-> log, commit |-> cm], base |-> base,
     conf |-> base.c, applied |-> base.i, sm |-> base.i, committed |-> 0, confTerm |-> 0, mark |-> 0,
     transferred |-> 0, support |-> NoSupport, fort |-> NoFortifies,
     halt |-> 0, removed |-> FALSE, wait |-> FALSE, run |-> run]

BootBase == [i |-> IF Bug("BootAtZero") THEN 0 ELSE 1, t |-> 0, c |-> Conf(InitVoters)]
BootLog == <<Boot, Entry(1, "n", 0, NoConf)>>

\* The group settled in term 1: replica 1 leads and every voter applied its
\* first entry. Replica IDs outside the boot voters are fresh, as a replacement
\* starts on empty storage.
Init ==
    LET voter == [Fresh(BootLog, 2, BootBase, 1, 1) EXCEPT
                    !.vote = 1, !.lead = 1, !.stable.vote = 1, !.applied = 2, !.sm = 2,
                    !.committed = 1]
    IN
    /\ rep = [r \in Replica |->
                IF r = 1 THEN [voter EXCEPT !.role = "L",
                                 !.next = [p \in Replica |-> 3],
                                 !.match = [p \in Replica |-> IF p \in InitVoters THEN 2 ELSE 0],
                                 !.sent = [p \in Replica |-> 2], !.pci = 2]
                ELSE IF r \in InitVoters THEN voter
                ELSE Fresh(<<>>, 0, [i |-> 0, t |-> 0, c |-> NoConf], 0, 1)]
    /\ msgs = {}
    /\ grant = [g \in Replica |-> [h \in Replica |->
                  [live |-> g # h /\ g \in InitVoters /\ h = 1, ep |-> 1]]]
    /\ hold = [h \in Replica |-> [g \in Replica |->
                  [has |-> g # h /\ g \in InitVoters /\ h = 1, ep |-> 1, run |-> 1]]]
    /\ elected = {[t |-> 1, r |-> 1]}
    /\ cmt = Committed(rep[1])
    /\ acked = {}
    /\ lagged = FALSE
    /\ crashes = 0

-----------------------------------------------------------------------------
\* Raft's steps, as pure functions of a replica's state

\* raft.becomeFollower
Adopt(s, t, l) ==
    [s EXCEPT !.term = IF t > @ THEN t ELSE @, !.vote = IF t > s.term THEN 0 ELSE @,
              !.role = "F", !.lead = l, !.votes = {}, !.next = NoIndex, !.match = NoIndex,
              !.sent = NoIndex, !.xfer = 0]

\* raft.maybeCommit
MaybeCommit(s) ==
    LET cs == {i \in s.commit + 1..Len(s.log) :
                 s.log[i].t = s.term /\ Quorum({p \in Replica : s.match[p] >= i}, s.rconf)}
    IN IF s.role = "L" /\ cs # {} THEN [s EXCEPT !.commit = Max(cs)] ELSE s

\* raft.hup
CanCampaign(s, r) ==
    /\ r \in Voters(s.rconf)
    /\ s.psnap = NoSnap
    /\ Bug("CampaignUnapplied") \/ ~HasConf(s.log, s.applied + 1, s.commit)

\* raft.campaign
Campaign1(s, r, x) ==
    [s EXCEPT !.term = @ + 1, !.vote = r, !.role = "C", !.lead = 0, !.votes = {r},
              !.next = NoIndex, !.match = NoIndex, !.sent = NoIndex, !.xfer = 0,
              !.pend = @ \cup {[y |-> "vote", f |-> r, o |-> p, t |-> s.term + 1, x |-> x,
                                li |-> Len(s.log), lt |-> LastTerm(s.log)] : p \in Voters(s.rconf) \ {r}}]

\* raft.handleAppendEntries' appending.
Merge(log, pi, es) ==
    LET conflicts == {k \in 1..Len(es) : pi + k > Len(log) \/ log[pi + k].t # es[k].t}
    IN IF conflicts = {} THEN log
       ELSE SubSeq(log, 1, pi + Min(conflicts) - 1) \o SubSeq(es, Min(conflicts), Len(es))

\* done's handing over of one committed entry: a configuration change takes
\* effect here, on each replica, and the state machine applies or halts.
ApplyOne(s, r) ==
    LET i == s.applied + 1
        e == s.log[i]
        bad == e.v # 0 /\ e.v = Poison /\ r \in CannotDecode
        stopped == s.halt # 0 /\ ~Bug("ApplyPastHalt")
    IN [s EXCEPT !.applied = i, !.committed = IF e.t > @ THEN e.t ELSE @,
                 !.conf = IF e.k = "c" THEN e.c ELSE @,
                 !.rconf = IF e.k = "c" THEN e.c ELSE @,
                 !.confTerm = IF e.k = "c" THEN 0 ELSE @,
                 !.halt = IF s.halt = 0 /\ bad THEN i ELSE @,
                 !.sm = IF stopped \/ bad THEN @ ELSE i]

RECURSIVE ApplyTo(_, _, _)
ApplyTo(s, r, hi) == IF s.applied >= hi THEN s ELSE ApplyTo(ApplyOne(s, r), r, hi)

\* done ends: a replica whose own log takes it out reports itself removed.
Leaves(was, s, r) ==
    IF r \in Members(was.conf) /\ r \notin Members(s.conf) THEN [s EXCEPT !.removed = TRUE] ELSE s

\* The leader answers its own entries as it applies them, by their term.
Acks(was, s, r) ==
    {[i |-> i, e |-> s.log[i]] : i \in {j \in was.applied + 1..s.sm :
        s.log[j].v # 0 /\ [t |-> s.log[j].t, r |-> r] \in elected}}

\* Shard.sender, and the same notice from a replica halted: a member tells one
\* outside its applied configuration, at its applied index.
Notice(s, r, id) ==
    [y |-> "removed", f |-> r, o |-> id, i |-> IF Bug("StaleAnswer") THEN s.commit ELSE s.applied]
Sender(s, r, id) ==
    IF id \notin Members(s.conf) /\ r \in Members(s.conf) THEN {Notice(s, r, id)} ELSE {}

\* A message the shard takes: recv's halted case.
Takes(s, m) == ~s.removed /\ (s.halt = 0 \/ (m.y = "appR" /\ s.role = "L"))

Supports(s, r) ==
    /\ s.support.t # 0
    /\ s.support.t = s.term
    /\ grant[r][s.support.lead].live
    /\ grant[r][s.support.lead].ep = s.support.ep

\* Shard.fortifies
Fortifies(s, l, p) ==
    /\ s.fort[p].t = s.term
    /\ hold[l][p].has
    /\ hold[l][p].run = s.fort[p].run
    /\ hold[l][p].ep = s.fort[p].ep

\* Shard.quorum
Fortified(s, l, vs) == Maj({p \in vs : p = l \/ Fortifies(s, l, p)}, vs)

\* Shard.LeaseRead, and meta.Shard.Scan's refusal once halted.
Reads(r) ==
    LET s == rep[r] IN
    /\ ~s.removed
    /\ s.role = "L"
    /\ s.halt = 0
    /\ s.transferred # s.term
    /\ s.committed = s.term \/ Bug("Uncommitted")
    /\ Fortified(s, r, s.conf.v)
    /\ Bug("OneHalf") \/ Fortified(s, r, s.conf.o)

Settled(s) == s.conf.o = {} /\ s.conf.l = {} /\ s.conf.v = Plan

\* Shard.change, with mark as commit.
Change(s, l) ==
    LET c == s.conf
        strayL == c.l \ Plan
        absent == Plan \ Members(c)
        strayV == (c.v \ Plan) \ {l}
        ready == {id \in c.l : Bug("Eager") \/ s.match[id] >= s.mark}
    IN IF strayL # {} THEN [c EXCEPT !.l = @ \ {Min(strayL)}]
       ELSE IF absent # {} THEN
         IF Bug("VoterFirst") THEN [c EXCEPT !.v = @ \cup {Min(absent)}]
         ELSE [c EXCEPT !.l = @ \cup {Min(absent)}]
       ELSE IF ready # {} /\ strayV # {} THEN
         [v |-> (c.v \ {Min(strayV)}) \cup {Min(ready)}, o |-> c.v, l |-> c.l \ {Min(ready)}]
       ELSE IF ready # {} THEN [c EXCEPT !.v = @ \cup {Min(ready)}, !.l = @ \ {Min(ready)}]
       ELSE IF strayV # {} /\ c.l = {} THEN [c EXCEPT !.v = @ \ {Min(strayV)}]
       ELSE c

\* A voter the change adds holds every entry committed at the mark, as far as
\* it told anyone: history, for PromotedHolds.
Lags(s, nc) ==
    \E id \in nc.v \ Voters(s.conf) : \E c \in cmt :
        c.i <= s.mark /\ ~(c.i <= Len(rep[id].stable.log) /\ rep[id].stable.log[c.i] = c.e)

\* Shard.replicating
Caught(s, l, p) == p # l /\ p \in s.conf.v /\ s.next[p] > s.base.i /\ s.match[p] >= s.mark

\* raft.becomeLeader's and raft.bcastAppend's sends: Shard.ready sends every
\* peer its append or snapshot in the Ready that needs it. next moves on at once,
\* as a follower in StateReplicate has it, and an empty append tells a new commit
\* index (C4). A halted leader's snapshot has no data and a follower refuses it,
\* so none is sent.
Sends(s, r) ==
    {p \in IF s.role = "L" THEN Members(s.rconf) \ {r} ELSE {} :
        IF s.next[p] > s.base.i THEN s.next[p] <= Len(s.log) \/ s.sent[p] < s.commit
        ELSE s.halt = 0}

SnapMeta(s) ==
    IF Bug("StaleSnapshot") THEN s.base
    ELSE [i |-> s.applied, t |-> TermAt(s.log, s.applied), c |-> s.conf]

SendTo(s, r, p) ==
    LET n == s.next[p] IN
    IF n > s.base.i
    THEN [y |-> "app", f |-> r, o |-> p, t |-> s.term, pi |-> n - 1, pt |-> TermAt(s.log, n - 1),
          es |-> SubSeq(s.log, n, Len(s.log)), c |-> s.commit]
    ELSE [y |-> "snap", f |-> r, o |-> p, t |-> s.term, s |-> SnapMeta(s),
          d |-> SubSeq(s.log, 1, SnapMeta(s).i)]

AfterSends(s, ps) ==
    [s EXCEPT !.next = [p \in Replica |->
                 IF p \notin ps THEN @[p]
                 ELSE IF s.next[p] > s.base.i THEN Len(s.log) + 1 ELSE SnapMeta(s).i + 1],
              !.sent = [p \in Replica |-> IF p \in ps THEN s.commit ELSE @[p]]]

MustSync(s) ==
    s.term # s.stable.term \/ s.vote # s.stable.vote \/ s.log # s.stable.log \/ s.psnap # NoSnap

\* Shard.ready: with no round in flight, a Ready that need not be durable is
\* done at once, and its commit index reaches no disk; one that must be becomes
\* the round, whose appends leave with it and whose other messages wait for
\* Durable. Bug("Early") sends those now, as plants.early does.
Ready(s, r) ==
    IF s.round # NoRound \/ s.removed THEN [s |-> s, out |-> {}, acks |-> {}]
    ELSE
    LET sync == MustSync(s)
        a == IF sync THEN s
             ELSE ApplyTo([s EXCEPT !.pend = {}, !.seen = s.commit], r, s.commit)
        d == Leaves(s, a, r)
        ps == IF d.removed THEN {} ELSE Sends(d, r)
        o == {SendTo(d, r, p) : p \in ps}
        e == AfterSends(d, ps)
        hs == s.term # s.stable.term \/ s.vote # s.stable.vote \/ s.commit # s.seen
        t == [e EXCEPT !.round = [hs |-> hs, term |-> s.term, vote |-> s.vote, log |-> s.log,
                                  commit |-> s.commit, snap |-> s.psnap,
                                  held |-> IF Bug("Early") THEN {} ELSE s.pend],
                       !.psnap = NoSnap, !.seen = s.commit, !.pend = {}]
    IN IF sync
       THEN [s |-> t, out |-> o \cup (IF Bug("Early") THEN s.pend ELSE {}), acks |-> {}]
       ELSE [s |-> e, out |-> o \cup s.pend, acks |-> Acks(s, a, r)]

\* Storage.InitialState: a Ready that moves only the commit index reaches no
\* disk, so the base can pass the last durable commit, and etcd-raft refuses a
\* hard state below its first index.
Loads(cm, base) == IF cm > base.i \/ Bug("CommitBelowBase") THEN cm ELSE base.i

\* The end of each event: what the event left, then Shard.ready.
Emit(r, s, gone, extra, acks) ==
    LET f == Ready(s, r) IN
    /\ rep' = [rep EXCEPT ![r] = f.s]
    /\ msgs' = (msgs \ gone) \cup extra \cup f.out
    /\ acked' = acked \cup acks \cup f.acks

-----------------------------------------------------------------------------
\* Events. Each is one call into the shard, named for what the code calls it.

\* Shard.tick, whose election timer stands still in Shard.stopped, RawNode.Tick
\* and raft.hup; or Shard.fortify's campaign raft_lapse_campaign after a lapse.
Campaign(r) ==
    LET s == rep[r] IN
    /\ r \in Candidates
    /\ ~s.removed
    /\ s.role # "L"
    /\ s.halt = 0
    /\ s.role = "C" \/ ~(s.wait \/ (Supports(s, r) /\ ~Bug("Disloyal")))
    /\ CanCampaign(s, r)
    /\ Emit(r, Campaign1(s, r, FALSE), {}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* A tick with nothing but a Ready to take, as after a restart: Shard.tick.
Tick(r) ==
    LET s == rep[r] IN
    /\ ~s.removed
    /\ s.round = NoRound
    /\ MustSync(s) \/ s.commit > s.applied \/ s.pend # {} \/ Sends(s, r) # {}
    /\ Emit(r, s, {}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Shard.recv: Shard.refuses, then raft.Step of MsgVote.
RecvVote(r, m) ==
    LET s == rep[r]
        c == IF ~s.wait /\ m.f = s.support.lead /\ m.t > s.support.t
             THEN [s EXCEPT !.support = NoSupport] ELSE s
        refused == s.wait \/ (Supports(c, r) /\ ~m.x /\ ~Bug("Disloyal"))
        a == IF m.t > c.term THEN Adopt(c, m.t, 0) ELSE c
        can == a.vote = m.f \/ (a.vote = 0 /\ a.lead = 0)
        fresh == LastTerm(a.log) < m.lt \/ (LastTerm(a.log) = m.lt /\ Len(a.log) <= m.li)
        n == IF ~refused /\ m.t >= c.term /\ can /\ fresh
             THEN [a EXCEPT !.vote = m.f,
                            !.pend = @ \cup {[y |-> "voteR", f |-> r, o |-> m.f, t |-> m.t]}]
             ELSE IF ~refused /\ m.t >= c.term THEN a ELSE c
    IN
    /\ m.y = "vote"
    /\ m.o = r
    /\ Takes(s, m)
    /\ Emit(r, n, {m}, Sender(n, r, m.f), {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* raft.Step of MsgVoteResp; raft.becomeLeader on a quorum.
RecvVoteResp(r, m) ==
    LET s == rep[r]
        vs == s.votes \cup {m.f}
        won == Quorum(vs, s.rconf)
        last == Len(s.log)
    IN
    /\ m.y = "voteR"
    /\ m.o = r
    /\ m.t = s.term
    /\ s.role = "C"
    /\ Takes(s, m)
    /\ Emit(r, IF won
         THEN [s EXCEPT !.votes = vs, !.role = "L", !.lead = r, !.pci = last, !.mark = s.commit,
                        !.log = @ \o <<Entry(s.term, "n", 0, NoConf)>>,
                        !.next = [p \in Replica |-> last + 1],
                        !.match = [NoIndex EXCEPT ![r] = last], !.sent = NoIndex]
         ELSE [s EXCEPT !.votes = vs], {m}, {}, {})
    /\ elected' = IF won THEN elected \cup {[t |-> s.term, r |-> r]} ELSE elected
    /\ UNCHANGED <<grant, hold, cmt, lagged, crashes>>

\* raft.Step's first case: a later term in an answer (C8).
RecvHigher(r, m) ==
    LET s == rep[r] IN
    /\ m.y \in {"voteR", "appR"}
    /\ m.o = r
    /\ m.t > s.term
    /\ Takes(s, m)
    /\ Emit(r, Adopt(s, m.t, 0), {m}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Shard.Propose
Propose(l, v) ==
    LET s == rep[l] IN
    /\ v \in Values
    /\ ~s.removed
    /\ s.role = "L"
    /\ s.halt = 0
    /\ s.xfer = 0
    /\ ~\E r \in Replica : \E e \in Range(rep[r].log) : e.v = v
    /\ Emit(l, [s EXCEPT !.log = @ \o <<Entry(s.term, "n", v, NoConf)>>], {}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* A leader sends again from the follower's match once an append or its answer
\* was lost: raft.ReportUnreachable and the probe after it.
Reprobe(l, p) ==
    LET s == rep[l] IN
    /\ On("lose")
    /\ ~s.removed
    /\ s.role = "L"
    /\ p \in Members(s.rconf) \ {l}
    /\ s.next[p] > s.match[p] + 1
    /\ Emit(l, [s EXCEPT !.next[p] = s.match[p] + 1, !.sent[p] = 0], {}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* raft.Step of MsgApp: raft.handleAppendEntries. A replica of a later term
\* answers with it, which is how an old leader steps down.
RecvApp(r, m) ==
    LET s == rep[r]
        a == Adopt(s, m.t, m.f)
        ok == m.pi <= Len(a.log) /\ TermAt(a.log, m.pi) = m.pt
        top == m.pi + Len(m.es)
        cm == IF m.c < top THEN m.c ELSE top
        reply(y, i) == [y |-> "appR", f |-> r, o |-> m.f, t |-> a.term, ok |-> y, i |-> i]
        n == CASE m.t < s.term ->
                    [s EXCEPT !.pend = @ \cup {[y |-> "appR", f |-> r, o |-> m.f, t |-> s.term,
                                                ok |-> FALSE, i |-> 0]}]
             [] m.pi < a.commit -> [a EXCEPT !.pend = @ \cup {reply(TRUE, a.commit)}]
             [] ok -> [a EXCEPT !.log = Merge(a.log, m.pi, m.es), !.commit = IF cm > @ THEN cm ELSE @,
                                !.pend = @ \cup {reply(TRUE, top)}]
             [] OTHER -> [a EXCEPT !.pend = @ \cup {reply(FALSE, IF m.pi < Len(a.log) THEN m.pi ELSE Len(a.log))}]
    IN
    /\ m.y = "app"
    /\ m.o = r
    /\ Takes(s, m)
    /\ Emit(r, n, {m}, Sender(n, r, m.f), {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* A halted replica drops a Raft message, and still tells a sender outside its
\* configuration: Shard.recv's halted case, then Shard.sender.
RecvHalted(r, m) ==
    LET s == rep[r] IN
    /\ m.y \in {"app", "vote", "snap"}
    /\ m.o = r
    /\ ~s.removed
    /\ s.halt # 0
    /\ Emit(r, s, {m}, Sender(s, r, m.f), {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* raft.Step of MsgSnap: raft.restore. The log, commit and tracker change at
\* once, the shard's applied index and configuration in Durable.
RecvSnap(r, m) ==
    LET s == rep[r]
        a == Adopt(s, m.t, m.f)
        sn == m.s
        reply(i) == [y |-> "appR", f |-> r, o |-> m.f, t |-> a.term, ok |-> TRUE, i |-> i]
        n == CASE sn.i <= a.commit \/ r \notin Members(sn.c) ->
                    [a EXCEPT !.pend = @ \cup {reply(a.commit)}]
             [] sn.i <= Len(a.log) /\ a.log[sn.i].t = sn.t ->
                    [a EXCEPT !.commit = sn.i, !.pend = @ \cup {reply(sn.i)}]
             [] OTHER -> [a EXCEPT !.log = m.d, !.commit = sn.i, !.rconf = sn.c, !.psnap = sn,
                                   !.pend = @ \cup {reply(sn.i)}]
    IN
    /\ m.y = "snap"
    /\ m.o = r
    /\ m.t >= s.term
    /\ Takes(s, m)
    /\ Emit(r, n, {m}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* raft.Step of MsgAppResp: stepLeader, then raft.maybeCommit.
RecvAppResp(l, m) ==
    LET s == rep[l]
        p == m.f
        lo == IF s.next[p] - 1 < m.i + 1 THEN s.next[p] - 1 ELSE m.i + 1
        n == IF m.ok
             THEN MaybeCommit([s EXCEPT !.match[p] = IF m.i > @ THEN m.i ELSE @,
                                        !.next[p] = IF m.i + 1 > @ THEN m.i + 1 ELSE @])
             ELSE [s EXCEPT !.next[p] = IF lo > s.match[p] + 1 THEN lo ELSE s.match[p] + 1]
    IN
    /\ m.y = "appR"
    /\ m.o = l
    /\ m.t = s.term
    /\ s.role = "L"
    /\ Takes(s, m)
    /\ Emit(l, n, {m}, {}, {})
    /\ cmt' = cmt \cup Committed(n)
    /\ UNCHANGED <<grant, hold, elected, lagged, crashes>>

\* Shard.Durable: the round's write is done and the shard told. Shard.done
\* installs a snapshot, sends what it held and hands over what the Ready
\* committed; Advance acknowledges the leader's own entries.
Durable(r) ==
    LET s == rep[r]
        R == s.round
        in == IF R.snap = NoSnap THEN s
              ELSE [s EXCEPT !.base = R.snap, !.conf = R.snap.c, !.applied = R.snap.i,
                             !.sm = IF s.halt # 0 THEN @ ELSE R.snap.i]
        st == [in EXCEPT !.round = NoRound,
                         !.stable = [term |-> R.term, vote |-> R.vote, log |-> R.log,
                                     commit |-> IF R.hs THEN R.commit ELSE @.commit]]
        a == ApplyTo(st, r, R.commit)
        n == IF a.role = "L" /\ a.term = R.term
             THEN MaybeCommit([a EXCEPT !.match[r] = IF Len(R.log) > @ THEN Len(R.log) ELSE @])
             ELSE a
    IN
    /\ ~s.removed
    /\ R # NoRound
    /\ Emit(r, Leaves(in, n, r), {}, R.held, Acks(st, a, r))
    /\ cmt' = cmt \cup (IF n.role = "L" THEN Committed(n) ELSE {})
    /\ UNCHANGED <<grant, hold, elected, lagged, crashes>>

\* newShard after a crash (C3). The crash keeps what was reported durable, or
\* the round in flight whole, as a round torn at the log's tail is dropped: so
\* a round can be durable with its shard never told. The state machine and the
\* configuration restart from the base and apply again.
Restart(r) ==
    LET s == rep[r]
        kept == {s.stable} \cup (IF s.round = NoRound THEN {} ELSE
                  {[term |-> s.round.term, vote |-> s.round.vote, log |-> s.round.log,
                    commit |-> IF s.round.hs THEN s.round.commit ELSE s.stable.commit]})
    IN
    /\ On("crash")
    /\ ~s.removed
    /\ \E d \in kept :
         LET base == IF s.round # NoRound /\ d # s.stable /\ s.round.snap # NoSnap
                     THEN s.round.snap ELSE s.base
             cm == Loads(d.commit, base)
             f == Fresh(d.log, cm, base, d.term, s.run + 1)
         IN rep' = [rep EXCEPT ![r] = [f EXCEPT !.vote = d.vote, !.stable.vote = d.vote,
                                                !.stable.commit = d.commit, !.seen = d.commit,
                                                !.wait = ~Bug("NoRestartWait")]]
    /\ grant' = [grant EXCEPT ![r] = [h \in Replica |-> [live |-> FALSE, ep |-> 0]]]
    /\ hold' = [hold EXCEPT ![r] = [g \in Replica |-> [has |-> FALSE, ep |-> 0, run |-> 0]]]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<msgs, elected, cmt, acked, lagged>>

\* Shard.reconcile's first lines: the commit index of a heartbeat ago.
Mark(r) ==
    LET s == rep[r] IN
    /\ ~s.removed
    /\ s.role = "L"
    /\ Plan # {}
    /\ ~Settled(s)
    /\ s.mark # s.commit
    /\ rep' = [rep EXCEPT ![r].mark = s.commit]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.reconcile's change: the next step toward the plan, one at a time.
Reconcile(l) ==
    LET s == rep[l]
        nc == Change(s, l)
    IN
    /\ ~s.removed
    /\ s.round = NoRound
    /\ Plan # {}
    /\ ~Settled(s)
    /\ s.confTerm # s.term
    /\ s.role = "L"
    /\ s.committed = s.term
    /\ s.conf.o = {}
    /\ s.xfer = 0
    /\ s.pci <= s.applied
    /\ l \in Plan \/ s.conf.v \cap Plan = {}
    /\ nc # s.conf
    /\ Emit(l, [s EXCEPT !.log = @ \o <<Entry(s.term, "c", 0, nc)>>, !.confTerm = s.term,
                         !.pci = Len(s.log) + 1], {}, {}, {})
    /\ lagged' = (lagged \/ Lags(s, nc))
    /\ UNCHANGED <<grant, hold, elected, cmt, crashes>>

\* raft.appliedTo: etcd-raft leaves a joint configuration on its own.
AutoLeave(l) ==
    LET s == rep[l] IN
    /\ ~s.removed
    /\ s.round = NoRound
    /\ s.role = "L"
    /\ s.rconf.o # {}
    /\ s.applied >= s.pci
    /\ s.xfer = 0
    /\ Emit(l, [s EXCEPT !.log = @ \o <<Entry(s.term, "c", 0, [s.rconf EXCEPT !.o = {}])>>,
                         !.pci = Len(s.log) + 1], {}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Shard.reconcile's RawNode.TransferLeader: a leader the plan drops, once a
\* planned voter is ready, from Shard.target; proposals stop while it waits.
Transfer(l, t) ==
    LET s == rep[l] IN
    /\ ~Bug("NoTransfer")
    /\ ~s.removed
    /\ s.round = NoRound
    /\ Plan # {}
    /\ ~Settled(s)
    /\ s.role = "L"
    /\ s.xfer = 0
    /\ s.committed = s.term
    /\ s.conf.o = {}
    /\ s.confTerm # s.term
    /\ l \notin Plan
    /\ s.conf.v \cap Plan # {}
    /\ t \in Plan
    /\ Caught(s, l, t)
    /\ hold[l][t].has
    /\ rep' = [rep EXCEPT ![l].xfer = t]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.handOff: a halted leader hands leadership to a replicating voter.
HandOff(l, t) ==
    LET s == rep[l] IN
    /\ ~s.removed
    /\ s.round = NoRound
    /\ s.halt # 0
    /\ s.role = "L"
    /\ s.xfer = 0
    /\ Caught(s, l, t)
    /\ rep' = [rep EXCEPT ![l].xfer = t]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* raft.sendTimeoutNow once the target holds the log; Shard.ready marks the
\* term transferred on taking the Ready that carries it.
SendTimeoutNow(l) ==
    LET s == rep[l] IN
    /\ ~s.removed
    /\ s.round = NoRound
    /\ s.role = "L"
    /\ s.xfer # 0
    /\ s.match[s.xfer] = Len(s.log)
    /\ s.transferred # s.term \/ Bug("TransferKeepsLease")
    /\ rep' = [rep EXCEPT ![l].transferred = IF Bug("TransferKeepsLease") THEN @ ELSE s.term]
    /\ msgs' = msgs \cup {[y |-> "now", f |-> l, o |-> s.xfer, t |-> s.term]}
    /\ UNCHANGED <<grant, hold, elected, cmt, acked, lagged, crashes>>

\* raft.tickHeartbeat: a transfer not done in an election timeout is given up.
AbortTransfer(l) ==
    LET s == rep[l] IN
    /\ On("abort")
    /\ ~s.removed
    /\ s.role = "L"
    /\ s.xfer # 0
    /\ rep' = [rep EXCEPT ![l].xfer = 0]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* raft.Step of MsgTimeoutNow: a term it has passed drops it, and a follower
\* that may campaigns at once, on etcd-raft's transfer context.
RecvTimeoutNow(r, m) ==
    LET s == rep[r]
        a == IF m.t > s.term THEN Adopt(s, m.t, 0) ELSE s
    IN
    /\ m.y = "now"
    /\ m.o = r
    /\ m.t >= s.term
    /\ Takes(s, m)
    /\ Emit(r, IF a.role = "F" /\ CanCampaign(a, r) THEN Campaign1(a, r, TRUE) ELSE a, {m}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Shard.fortify: a leader asks every peer it holds a lease from.
Fortify(l) ==
    LET s == rep[l]
        ps == {p \in Members(s.conf) \ {l} : hold[l][p].has /\ ~Fortifies(s, l, p)}
    IN
    /\ On("fortify")
    /\ ~s.removed
    /\ s.role = "L"
    /\ ps # {}
    /\ \E p \in ps : [y |-> "fort", f |-> l, o |-> p, t |-> s.term] \notin msgs
    /\ msgs' = msgs \cup {[y |-> "fort", f |-> l, o |-> p, t |-> s.term] : p \in ps}
    /\ UNCHANGED <<rep, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.fortifyAsked
RecvFortify(v, m) ==
    LET s == rep[v]
        ok == m.t = s.term /\ m.f = s.lead /\ grant[v][m.f].live
    IN
    /\ m.y = "fort"
    /\ m.o = v
    /\ Takes(s, m)
    /\ Emit(v, IF ok THEN [s EXCEPT !.support = [t |-> m.t, ep |-> grant[v][m.f].ep, lead |-> m.f]]
               ELSE s, {m},
            IF ok THEN {[y |-> "forted", f |-> v, o |-> m.f, t |-> m.t, run |-> s.run,
                         ep |-> grant[v][m.f].ep]} ELSE {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Shard.fortifyAnswered
RecvFortified(l, m) ==
    LET s == rep[l] IN
    /\ m.y = "forted"
    /\ m.o = l
    /\ Takes(s, m)
    /\ s.role = "L"
    /\ m.t = s.term
    /\ Emit(l, [s EXCEPT !.fort[m.f] = [t |-> m.t, run |-> m.run, ep |-> m.ep]], {m}, {}, {})
    /\ UNCHANGED <<grant, hold, elected, cmt, lagged, crashes>>

\* Grantor.Grant and the holder's lease from it (C1). A grant that lapsed comes
\* back under a new support epoch. Only a leader's leases decide anything, so
\* only a leader takes one.
Grant(g, h) ==
    LET ep == IF grant[g][h].live THEN grant[g][h].ep ELSE grant[g][h].ep + 1 IN
    /\ g # h
    /\ rep[h].role = "L"
    /\ ~rep[g].removed /\ ~rep[h].removed
    /\ ~rep[g].wait
    /\ ~(grant[g][h].live /\ hold[h][g].has /\ hold[h][g].run = rep[g].run)
    /\ grant' = [grant EXCEPT ![g][h] = [live |-> TRUE, ep |-> ep]]
    /\ hold' = [hold EXCEPT ![h][g] = [has |-> TRUE, ep |-> ep, run |-> rep[g].run]]
    /\ UNCHANGED <<rep, msgs, elected, cmt, acked, lagged, crashes>>

\* The holder's lease ends, on its faster clock.
Expire(h, g) ==
    /\ On("lapse")
    /\ hold[h][g].has
    /\ rep[h].role = "L"
    /\ hold' = [hold EXCEPT ![h][g].has = FALSE]
    /\ UNCHANGED <<rep, msgs, grant, elected, cmt, acked, lagged, crashes>>

\* The grant lapses on the grantor's clocks, never before the holder's lease
\* in it ended (C2).
Lapse(g, h) ==
    /\ On("lapse")
    /\ grant[g][h].live
    /\ ~(hold[h][g].has /\ hold[h][g].run = rep[g].run /\ hold[h][g].ep = grant[g][h].ep)
    /\ grant' = [grant EXCEPT ![g][h].live = FALSE]
    /\ UNCHANGED <<rep, msgs, hold, elected, cmt, acked, lagged, crashes>>

\* The restart wait ends once no lease of the old run can run (C2).
EndWait(r) ==
    LET s == rep[r] IN
    /\ s.wait
    /\ \A h \in Replica : ~(hold[h][r].has /\ hold[h][r].run # s.run)
    /\ rep' = [rep EXCEPT ![r].wait = FALSE]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.fortify: a replica that has not heard its leader asks its peers whether
\* it is a member, while its applied configuration holds it.
AskMember(r) ==
    LET s == rep[r] IN
    /\ On("ask")
    /\ ~s.removed
    /\ s.role # "L"
    /\ r \in Members(s.conf)
    /\ msgs' = msgs \cup {[y |-> "member", f |-> r, o |-> p] : p \in Replica \ {r}}
    /\ msgs' # msgs
    /\ UNCHANGED <<rep, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.recv, kindMember. It holds no check that the answerer is a member.
RecvMember(r, m) ==
    LET s == rep[r] IN
    /\ m.y = "member"
    /\ m.o = r
    /\ ~s.removed
    /\ msgs' = (msgs \ {m}) \cup (IF m.f \notin Members(s.conf) THEN {Notice(s, r, m.f)} ELSE {})
    /\ UNCHANGED <<rep, grant, hold, elected, cmt, acked, lagged, crashes>>

\* Shard.recv, kindRemoved: an answer from past this replica's applied index
\* proves a removal, to a replica its applied configuration holds.
RecvRemoved(r, m) ==
    LET s == rep[r] IN
    /\ m.y = "removed"
    /\ m.o = r
    /\ ~s.removed
    /\ rep' = [rep EXCEPT ![r].removed = (m.i >= s.applied \/ Bug("NoGuard")) /\ r \in Members(s.conf)]
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<grant, hold, elected, cmt, acked, lagged, crashes>>

\* Host.Compact to meta.Shard.Applied (C5): a halted replica's is its base.
Compact(r) ==
    LET s == rep[r]
        i == IF Bug("CompactPastHalt") THEN s.applied ELSE s.sm
    IN
    /\ On("compact")
    /\ ~s.removed
    /\ s.halt = 0 \/ Bug("CompactPastHalt")
    /\ i > s.base.i
    /\ i = s.applied
    /\ rep' = [rep EXCEPT ![r].base = [i |-> i, t |-> s.log[i].t, c |-> s.conf]]
    /\ UNCHANGED <<msgs, grant, hold, elected, cmt, acked, lagged, crashes>>

\* A message lost, or one its receiver drops unread: to a removed replica, or
\* of a term it has passed.
Drop(m) ==
    /\ \/ On("lose")
       \/ rep[m.o].removed
       \/ "t" \in DOMAIN m /\ m.t < rep[m.o].term
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<rep, grant, hold, elected, cmt, acked, lagged, crashes>>

Recv ==
    \E m \in msgs :
        \/ RecvVote(m.o, m) \/ RecvVoteResp(m.o, m) \/ RecvHigher(m.o, m) \/ RecvApp(m.o, m)
        \/ RecvSnap(m.o, m) \/ RecvAppResp(m.o, m) \/ RecvTimeoutNow(m.o, m) \/ RecvFortify(m.o, m)
        \/ RecvFortified(m.o, m) \/ RecvMember(m.o, m) \/ RecvRemoved(m.o, m) \/ RecvHalted(m.o, m)

\* The steps a fair run takes. A campaign, a lost message, a crash, a lease
\* ending and a compaction are the choices left out.
Protocol ==
    \/ Recv
    \/ \E r \in Replica :
         \/ Tick(r) \/ Durable(r) \/ Mark(r) \/ Reconcile(r) \/ AutoLeave(r)
         \/ SendTimeoutNow(r) \/ EndWait(r) \/ Fortify(r)
    \/ \E l, p \in Replica : Transfer(l, p) \/ Grant(p, l)

Next ==
    \/ Protocol
    \/ \E m \in msgs : Drop(m)
    \/ \E r \in Replica :
         \/ Campaign(r) \/ Restart(r) \/ Compact(r) \/ AbortTransfer(r) \/ AskMember(r)
         \/ \E v \in Values : Propose(r, v)
    \/ \E l, p \in Replica :
         \/ HandOff(l, p) \/ Expire(l, p) \/ Lapse(l, p) \/ Reprobe(l, p)

Spec == Init /\ [][Next]_vars

Bounds ==
    /\ Cardinality(msgs) <= MaxMsgs
    /\ \A m \in msgs : Cardinality({x \in msgs : x.f = m.f /\ x.o = m.o /\ x.y = m.y}) <= MaxLink
    /\ crashes <= MaxCrash
    /\ \A r \in Replica : rep[r].term <= MaxTerm
    /\ \A g, h \in Replica : grant[g][h].ep <= MaxEpoch

-----------------------------------------------------------------------------
\* Properties

\* 1. At most one leader per term.
OneLeaderPerTerm == \A a, b \in elected : a.t = b.t => a.r = b.r

\* 2. Every leader of a later term holds every committed entry, and no two
\* replicas' state machines differ at an index: committed entries survive every
\* leader change and every membership change.
Holds(log, c) == c.i <= Len(log) /\ log[c.i] = c.e
CommittedSurvive ==
    /\ \A r \in Replica : LET s == rep[r] IN
         (~s.removed /\ s.role = "L") => \A c \in cmt : c.e.t < s.term => Holds(s.log, c)
    /\ \A r, q \in Replica : \A i \in 1..rep[r].sm :
         i <= rep[q].sm => rep[r].log[i] = rep[q].log[i]

\* 3. A lease read returns every acknowledged entry.
ReadsAcked ==
    \A r \in Replica : Reads(r) =>
       \A c \in acked : c.i <= rep[r].sm /\ rep[r].log[c.i] = c.e

\* 4. No two replicas serve lease reads at once.
OneReader == \A a, b \in Replica : a # b => ~(Reads(a) /\ Reads(b))

\* 5. A replica deletes its state only once a committed configuration lacks it.
NoWrongfulDeletion ==
    \A r \in Replica : rep[r].removed =>
       \E c \in cmt : c.e.k = "c" /\ r \notin Members(c.e.c)

\* The configurations no later committed configuration replaced.
CurrentConfs ==
    LET cs == {c \in cmt : c.e.k = "c"}
    IN IF cs = {} THEN {Conf(InitVoters)}
       ELSE {c.e.c : c \in {x \in cs : \A y \in cs : y.i <= x.i}}

\* 6. After any deletion, every committed entry is durable on a quorum of every
\* current configuration, in each half.
HeldAfterDeletion ==
    (\E r \in Replica : rep[r].removed) =>
       \A c \in cmt : \A k \in CurrentConfs :
          Quorum({r \in Replica : ~rep[r].removed /\ Holds(rep[r].stable.log, c)}, k)

\* 7. A learner promoted holds every entry committed at its mark. Raft's own
\* safety does not rest on this: a joint quorum still commits on the old half,
\* so ci-eager runs every other safety property with the rule removed. It is
\* availability, what keeps the swap from stalling commits on an empty voter.
PromotedHolds == ~lagged

\* 8. A halted replica applies nothing past its halt, and its log compacts no
\* further than its state applied.
HaltStops ==
    \A r \in Replica : LET s == rep[r] IN
       s.halt # 0 => s.sm < s.halt /\ s.base.i <= s.sm

\* 9. A restart's hard state loads.
Restarts ==
    \A r \in Replica : LET s == rep[r] IN
       ~s.removed => Loads(s.stable.commit, s.base) >= s.base.i

\* 10. A swap completes, and a dropped leader hands off: with no fault, no
\* campaign and no writes, Protocol has no cycle, so every fair run ends in a
\* state with no step left, and each such state has a leader on the plan. That
\* is the time-free assumption, every step eventually taken, without TLC's
\* fairness, which costs a thousand times the states here.
SwapCompletes ==
    \/ ENABLED Protocol
    \/ \E r \in Replica : rep[r].role = "L" /\ ~rep[r].removed /\ Settled(rep[r])

=============================================================================
