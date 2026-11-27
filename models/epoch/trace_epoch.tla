---------------------------- MODULE trace_epoch ----------------------------
\* Trace validation of the epoch model: TLC checks that a simulator run, as
\* tools/tracedata writes it into module trace_data, is a behavior of epoch.tla.
\* Each line names the spec action that takes it; what the record observed is
\* checked against the state the action leaves. No protocol rule lives here.
\* A record the spec compresses into another action's step is a stutter, whose
\* guard says which step holds it: see "Spec conformance" in
\* docs/test-scenarios.md. Variables no line observes stay free.
\*
\* TraceIncomplete is an invariant whose violation is the pass: the line counter
\* passed the end. A run that finishes with no violation failed, and its search
\* depth names the line no state could take.

EXTENDS epoch, trace_data

VARIABLES line, healed, stayed

\* healed: the [c, v] a recovered line took Recover for. A primary records its
\* own copy of a recovery write, on a disk that lists nothing, so that line is a
\* stutter while a member lists the chunk or once one recovered that version.
\* stayed: the mayfail lines whose version the crash kept.

\* A version in a trace needs more than the 3 bits of epoch.tla's counter.
TBase == 1048576
TVer(k, n) == k * TBase + n
TIvOfV(v) == v \div TBase
TCnt(v) == v % TBase

\* No traced run injects a disk fault, so the one MemberFail a trace takes is an
\* unready line, a refusal while the member's store replays its log after a
\* restart: a refusal of a version of its interval by a store that serves, an
\* mfail line, has no step. That no step, not UnchangedIntervalAccepts, which
\* a budget for "mfail" leaves silent on refusals, is what rejects one.
TPlanSet == {[free |-> FALSE, m |-> TPlan, w |-> TWrites, f |-> {"restart", "format", "ownfail", "lateask", "mfail", "lose", "trim"},
              t |-> 0, k |-> 100000, s |-> 100000]}
TMaint == [p \in TPG |-> FALSE]
TRates == [n \in TNode |-> {1000}]
TSlews == [n \in TNode |-> {0}]

\* A persisted interval below 1 reads as interval 1: the spec's Init persisted
\* it on every disk of Node, and the keep rule reads at least the table's.
Decided1(x) == Max({x, 1})

\* A crash loses a suffix of the log in LSN order, so an own record it kept in
\* part, logged above the newest whole version, is the oldest one it lost: the
\* mayfail line after its own kept its version.
KeptBelowTorn(r) ==
    LET at == {i \in 1..line - 1 : /\ Trace[i].k = "mayfail" /\ Trace[i].n = dnode[r.d]
                                   /\ Trace[i].c = r.c /\ Trace[i].v = r.logged}
        i == Max(at)
    IN at # {} /\ Trace[i + 1].k = "mayfail" /\ Trace[i + 1].n = Trace[i].n => i + 1 \in stayed

ReplyMatches(l) ==
    \A r \in l.rep :
        /\ Reply(r.d, r.c).whole = r.whole
        /\ \/ r.logged = r.whole
           \/ /\ r.logged > r.whole
              /\ \E x \in vers : x.c = r.c /\ x.v = r.logged
              /\ KeptBelowTorn(r)
        /\ Reply(r.d, r.c).list = r.list
        /\ Decided1(Reply(r.d, r.c).decided) = Decided1(r.decided)

\* The decision's U: the up members whose reply shows a version logged above the
\* newest whole one, listing nothing.
TornIn(l) ==
    {[d |-> r.d, c |-> r.c] :
       r \in {y \in l.rep : y.logged > y.whole /\ y.list = NoMiss /\ y.d \in UpIn(l.p, l.iv)}}

\* The code's lists are the spec's under T, the members of U Entry leaves off.
ListsMatch(l) ==
    LET T == {x \in TornIn(l) : Entry(x.d, x.c, l.kept[x.c]) = NoMiss}
        up == [d : UpIn(l.p, l.iv), c : ChunksOf(l.p)]
    IN l.listed = {[d |-> x.d, c |-> x.c, v |-> ListOf(x.d, x.c, l.kept[x.c], T)] :
                     x \in {y \in up : ListOf(y.d, y.c, l.kept[y.c], T) # NoMiss}}

KeptMatches(l) ==
    \E x \in dec' :
        /\ x.p = l.p /\ x.k = l.iv /\ x.n = l.n /\ x.r = run[l.n]
        /\ \A c \in Chunk : x.kept[c] = l.kept[c]

\* The disks whose lists the trace records durable for a peer line's interval:
\* the spec has them persist at the decision, the code after it.
PersistsAfter(l) ==
    {Trace[i].d : i \in {j \in line + 1..Len(Trace) :
        Trace[j].k = "persisted" /\ Trace[j].p = l.p /\ Trace[j].iv = l.iv}}

\* The code's table against the spec's, after the lines that made its changes.
RowMatches(r) ==
    LET x == ivs[r.p][r.iv]
    IN /\ Cur(r.p) = r.iv
       /\ tbl[Len(tbl)][r.p].la = Decided1(r.la)
       /\ x.mem = r.mem
       /\ \A j \in 1..Len(r.mem) : x.st[r.mem[j]] = r.marks[j]
       /\ x.lv = r.lv
       /\ x.ml = r.ml
       /\ {y.iv : y \in Range(r.past)} = Decided1(r.la)..r.iv - 1
       /\ \A y \in Range(r.past) :
            /\ ivs[r.p][y.iv].mem = y.mem
            /\ ivs[r.p][y.iv].ml = y.ml
            /\ \A j \in 1..Len(y.mem) : ivs[r.p][y.iv].st[y.mem[j]] = y.marks[j]

\* A recovered line a stutter holds, the node's own copy of a recovery write it
\* ordered as primary of l.iv: the version is that decision's kept one or one of
\* the interval, and the source an up member of the interval, none only for a
\* chunk nothing was kept of.
OwnRecovery(l) ==
    \E x \in dec :
        /\ x.p = ChunkPG[l.c] /\ x.k = l.iv /\ x.n = l.n
        /\ \/ l.v = x.kept[l.c]
           \/ IvOfV(l.v) = l.iv /\ \E r \in vers : r.c = l.c /\ r.v = l.v
        /\ IF x.kept[l.c] = 0 THEN l.s = 0 ELSE l.s \in UpIn(x.p, l.iv)

\* A primary's gated recovery write settled by a later interval's peering of the
\* same primary, which listed the chunk again: its entry stays, and Recover did not
\* happen for it.
RecoveryOver(l) ==
    /\ OwnRecovery(l)
    /\ l.l = l.n
    /\ \E a \in act, y \in dec :
         /\ a.p = ChunkPG[l.c] /\ a.k > l.iv
         /\ y.p = a.p /\ y.k = a.k /\ y.r = a.r /\ y.n = l.n

\* The primary ordered this recovery write at an earlier line.
RecoveryOrdered(l) ==
    \E i \in 1..line - 1 :
        /\ Trace[i].k = "recoverorder"
        /\ Trace[i].c = l.c /\ Trace[i].iv = l.iv /\ Trace[i].s = l.s

\* A primary's record of its own copy of a recovery write that landed after it
\* took its own reply for a later interval and before that interval activated:
\* the decision, made or to come, reads the reply and lists the chunk again, so
\* the line is a stutter once the primary left the interval.
RecoveredLate(l) ==
    /\ RecoveryOrdered(l)
    /\ OwnRecovery(l)
    /\ IvAt(l.n, ChunkPG[l.c]) > l.iv
    /\ miss[ddisk[l.n]][l.c] # NoMiss

\* A crash loses a suffix of a disk's log in LSN order, and a primary submits its
\* own records in the order it orders them, so the mayfail lines, newest first,
\* lose a prefix of themselves: a line loses its version only if the one before
\* lost its own.
LostAbove(l) ==
    IF line > 1 /\ Trace[line - 1].k = "mayfail" /\ Trace[line - 1].n = l.n
    THEN Trace[line - 1].v \notin dlog[ddisk[l.n]][Trace[line - 1].c]
    ELSE TRUE

RecoverLine(l) ==
    /\ (l.s = 0) = (miss[ddisk[l.n]][l.c] = 0)
    /\ RecoverFrom(l.l, ChunkPG[l.c], l.c, ddisk[l.n], IF l.s = 0 THEN ddisk[l.n] ELSE l.s, l.v)

\* The entries of chunk c the resend table of disk d answers from. The code
\* drops a version's entry at the decision marker that drops it, earlier than
\* the spec's Persist, so a disk listing c is not compared (Known in epoch.tla).
KnownIds(d, c) == {g \in ids[d] : g.c = c /\ g.v \in Known(d, c)}

\* The entry of write w in n's pin that an order, reanswer or stale line takes.
\* A write's entries differ only in hold, and a step's guards see hold only in
\* Fenced. The step takes the earliest-held entry it allows, which a later
\* hold, or none, is fenced no more often than, so the trace does not double
\* its states at each line of a write sent again while held. A later Send of w
\* merges into the [w, 0] this leaves, where taking [w, 0] would have kept two
\* entries, so the choice may reject a trace the spec accepts, never accepts
\* one it rejects, and hides no bug of the code. Fenced reads n's decision of
\* its interval, which Order and Reanswer require before they read it.
Took(n, w, fenceable) ==
    LET p == ChunkPG[Writes[w].c]
        S == {y \in pin[n] : y.w = w /\ ~(fenceable /\ Decided(n, p, IvAt(n, p)) /\ Fenced(n, y))}
        H == {y \in S : y.hold # 0}
    IN IF H = {} THEN S ELSE {y \in H : \A z \in H : y.hold <= z.hold}

Line(l) ==
    \/ /\ l.k = "table"
       /\ \A r \in Range(l.rows) : RowMatches(r)
       /\ UNCHANGED vars
    \/ /\ l.k = "plan"
       /\ Membership
    \/ /\ l.k = "fold"
       /\ Reactive
       /\ tbl'[Len(tbl')][l.p].la = l.iv
    \/ /\ l.k = "renew"
       /\ Reactive
       /\ Cur(l.p) < tbl'[Len(tbl')][l.p].iv
    \/ /\ l.k = "adopt"
       /\ IF l.e = ep[l.n] THEN UNCHANGED vars ELSE Adopt(l.n, l.e)
    \/ /\ l.k = "send"
       /\ IF ENABLED Send(l.w) THEN Send(l.w) ELSE UNCHANGED vars
    \/ /\ l.k = "order"
       /\ \E x \in Took(l.n, l.w, TRUE) : Order(l.n, x)
       /\ pv'[l.n][l.c] = l.v
    \/ /\ l.k = "logged"
       /\ IF l.v \in dlog[ddisk[l.n]][l.c]
          THEN UNCHANGED vars
          ELSE \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : Deliver(m)
    \/ /\ l.k = "ownlogged"
       /\ \E r \in {x \in ownw : x.n = l.n /\ x.c = l.c /\ x.v = l.v} : OwnLogged(r)
    \/ /\ l.k = "maydeliver"
       /\ \/ UNCHANGED vars
          \/ \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : Deliver(m)
    \/ /\ l.k = "unready"
       /\ \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : MemberFail(m)
    \/ /\ l.k = "gone"
       /\ \E m \in {x \in rm : dhost[x.to] = l.n /\ x.c = l.c /\ x.v = l.v} :
            IF [c |-> l.c, v |-> l.v] \in failed[m.to] THEN UNCHANGED vars ELSE GoneRefuses(m)
    \/ /\ l.k = "gonerecover"
       /\ LET p == ChunkPG[l.c]
          IN IF \E n \in Node : Running(n) /\ Leads(n, p) /\ pr[n][p].act /\ IvAt(n, p) = IvAt(l.n, p)
             THEN \E n \in Node, d \in Disk :
                    /\ dhost[d] = l.n
                    /\ GoneRefusesRecovery(n, l.c, d)
                    /\ pend' = pend \ {x \in pend : x.n = n /\ x.r = run[n] /\ x.c = l.c /\ x.v > l.v}
             ELSE /\ ~\E r \in pend : r.c = l.c /\ r.v > l.v
                  /\ UNCHANGED vars
    \/ /\ l.k = "mayfail"
       /\ \/ UNCHANGED vars
          \/ /\ LostAbove(l)
             /\ \E r \in {x \in ownw : x.c = l.c /\ x.v = l.v} : OwnFail(l.n, r)
    \/ /\ l.k = "restart"
       /\ Restart(l.n)
    \/ /\ l.k = "format"
       /\ Format(l.n)
    \/ /\ l.k = "lost"
       /\ Lost(l.d)
       /\ UNCHANGED step
    \/ /\ l.k = "recoverorder"
       /\ Leads(l.n, ChunkPG[l.c])
       /\ pr[l.n][ChunkPG[l.c]].act
       /\ IvAt(l.n, ChunkPG[l.c]) = l.iv
       /\ \E d \in Disk : miss[d][l.c] # NoMiss
       /\ UNCHANGED vars
    \/ /\ l.k = "recovered"
       /\ RecoveryOrdered(l)
       /\ IF RecoveryOver(l)
          THEN UNCHANGED vars
          ELSE IF miss[ddisk[l.n]][l.c] = NoMiss
          THEN /\ OwnRecovery(l)
               /\ \E d \in Disk : miss[d][l.c] # NoMiss \/ [c |-> l.c, v |-> l.v] \in healed
               /\ UNCHANGED vars
          ELSE /\ l.iv = IvAt(l.l, ChunkPG[l.c])
               /\ \/ l.v = miss[ddisk[l.n]][l.c]
                  \/ IvOfV(l.v) = IvAt(l.l, ChunkPG[l.c]) /\ l.v <= pv[l.l][l.c]
               /\ RecoverLine(l)
    \/ /\ l.k = "recoveredlate"
       /\ RecoveredLate(l)
       /\ UNCHANGED vars
    \/ /\ l.k = "mayrecover"
       /\ RecoveryOrdered(l)
       /\ \/ UNCHANGED vars
          \/ RecoverLine(l)
    \/ /\ l.k = "reported"
       /\ IF ivs[l.p][l.iv].lv = {} \/ \E r \in recd : r.p = l.p /\ r.k = l.iv
          THEN UNCHANGED vars
          ELSE IvAt(l.n, l.p) = l.iv /\ ReportRecovered(l.n, l.p)
    \/ /\ l.k = "ask"
       /\ IF reqIv[l.p] = l.iv
          THEN UNCHANGED vars
          ELSE IvAt(l.n, l.p) = l.iv /\ (AskNext(l.n, l.p) \/ CounterOut(l.n, l.p))
    \/ /\ l.k = "restartask"
       /\ IF reqIv[l.p] = l.iv
          THEN UNCHANGED vars
          ELSE IvAt(l.n, l.p) = l.iv /\ RestartAsk(l.n, l.p)
    \/ /\ l.k = "committed"
       /\ IF [c |-> l.c, v |-> l.v] \in ackd
          THEN UNCHANGED vars
          ELSE \E r \in {x \in pend : x.c = l.c /\ x.v = l.v} : IF l.heard THEN Answer(r) ELSE LoseAnswer(r)
    \/ /\ l.k = "reanswer"
       /\ \E x \in Took(l.n, l.w, TRUE) :
            /\ KnownOf(l.n, x) # {}
            /\ Max(KnownOf(l.n, x)) = l.v
            /\ IF l.heard THEN Reanswer(l.n, x) ELSE LoseReanswer(l.n, x)
    \/ /\ l.k = "reanswered"
       /\ [c |-> l.c, v |-> l.v] \in ackd
       /\ UNCHANGED vars
    \/ /\ l.k = "stale"
       /\ \E x \in Took(l.n, l.w, FALSE) : Stale(l.n, x)
    \/ /\ l.k = "staled"
       /\ l.w \in wm[ddisk[l.n]]
       /\ UNCHANGED vars
    \/ /\ l.k = "trim"
       /\ IF l.w \in wm[l.d] THEN UNCHANGED vars ELSE Trim(l.d, l.w)
    \/ /\ l.k = "resends"
       /\ \A c \in Chunk : miss[l.d][c] = NoMiss => {g \in l.e : g.c = c} = KnownIds(l.d, c)
       /\ UNCHANGED vars
    \/ /\ l.k = "peer"
       /\ pr[l.n][l.p].iv = l.iv
       /\ ReplyMatches(l)
       /\ PeerOpen(l.n, l.p)
       /\ l.j \subseteq PeerElig(l.n, l.p)
       /\ PersistsAfter(l) \in PeerPersists(l.n, l.p)
       /\ TornIn(l) \in TornSets(l.n, l.p)
       /\ ListsMatch(l)
       /\ PeerOn(l.n, l.p, l.j, PersistsAfter(l), TornIn(l))
       /\ KeptMatches(l)
    \/ /\ l.k = "persisted"
       /\ \E x \in dec : x.p = l.p /\ x.k = l.iv
       /\ l.again \/ ~\E a \in act : a.p = l.p /\ a.k = l.iv
       /\ persIv[l.d][l.p] = l.iv
       /\ UNCHANGED vars
    \/ /\ l.k = "activate"
       /\ pr[l.n][l.p].iv = l.iv
       /\ \A d \in UpIn(l.p, l.iv) : persIv[d][l.p] = l.iv
       /\ Activate(l.n, l.p)

TraceInit == Init /\ line = 1 /\ healed = {} /\ stayed = {}

TraceNext ==
    /\ line <= Len(Trace)
    /\ Line(Trace[line])
    /\ line' = line + 1
    /\ healed' = IF Trace[line].k = "recovered" /\ miss[ddisk[Trace[line].n]][Trace[line].c] # NoMiss
                    /\ ~RecoveryOver(Trace[line])
                 THEN healed \cup {[c |-> Trace[line].c, v |-> Trace[line].v]}
                 ELSE healed
    /\ stayed' = IF Trace[line].k = "mayfail" /\ Trace[line].v \in dlog'[ddisk[Trace[line].n]][Trace[line].c]
                 THEN stayed \cup {line}
                 ELSE stayed

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, line, healed, stayed>>

TraceIncomplete == line <= Len(Trace)

\* Vers, ackd, dec and tbl only grow and what these read of them never changes,
\* so a break stays: each is checked once, at the end of the trace, since on
\* every state a long trace takes hours. A trace stopped by a line evaluates
\* none of them.
AtEndCommitted == line <= Len(Trace) \/ NothingCommittedDropped
AtEndOneWrite == line <= Len(Trace) \/ OneWritePerVersion
AtEndMerges == line <= Len(Trace) \/ MergesOnKept
AtEndTables == line <= Len(Trace) \/ TablesForward
AtEndAppliedOnce == line <= Len(Trace) \/ AppliedOnce
AtEndStaleOnlyHeard == line <= Len(Trace) \/ StaleOnlyHeard
=============================================================================
