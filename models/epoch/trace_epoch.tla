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

VARIABLES line, healed

\* healed: the [c, v] a recovered line took Recover for. A primary records its
\* own copy of a recovery write, on a disk that lists nothing, so that line is a
\* stutter while a member lists the chunk or once one recovered that version.

\* A version in a trace needs more than the 3 bits of epoch.tla's counter.
TBase == 1048576
TVer(k, n) == k * TBase + n
TIvOfV(v) == v \div TBase
TCnt(v) == v % TBase

\* No traced run injects a disk fault, so MemberFail has no budget and a member's
\* refusal of a version of its interval has no step.
TPlanSet == {[free |-> FALSE, m |-> TPlan, w |-> TWrites, f |-> {"restart", "format", "ownfail"},
              t |-> 0, k |-> 100000, s |-> 100000]}
TMaint == [p \in TPG |-> FALSE]
TRates == [n \in TNode |-> {1000}]
TSlews == [n \in TNode |-> {0}]

\* A persisted interval below 1 reads as interval 1: the spec's Init persisted
\* it on every disk of Node, and the keep rule reads at least the table's.
Decided1(x) == Max({x, 1})

ReplyMatches(l) ==
    \A r \in l.rep :
        /\ Reply(r.d, r.c).whole = r.whole
        /\ \/ r.logged = r.whole
           \/ r.logged > r.whole /\ \E x \in vers : x.c = r.c /\ x.v = r.logged
        /\ Reply(r.d, r.c).list = r.list
        /\ Decided1(Reply(r.d, r.c).decided) = Decided1(r.decided)

\* The members a reply says logged more than they hold whole: Entry leaves such
\* a member off for the version it holds, and the code lists it (ListOf).
TornIn(l) == {[d |-> r.d, c |-> r.c] : r \in {x \in l.rep : x.logged > x.whole}}

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

RecoverLine(l) ==
    /\ (l.s = 0) = (miss[ddisk[l.n]][l.c] = 0)
    /\ RecoverFrom(l.l, ChunkPG[l.c], l.c, ddisk[l.n], IF l.s = 0 THEN ddisk[l.n] ELSE l.s)

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
       /\ \E x \in {y \in pin[l.n] : y.w = l.w} : Order(l.n, x)
       /\ pv'[l.n][l.c] = l.v
    \/ /\ l.k = "logged"
       /\ IF l.v \in dlog[ddisk[l.n]][l.c]
          THEN UNCHANGED vars
          ELSE \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : Deliver(m)
    \/ /\ l.k = "maydeliver"
       /\ \/ UNCHANGED vars
          \/ \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : Deliver(m)
    \/ /\ l.k = "mfail"
       /\ \E m \in {x \in rm : x.to = ddisk[l.n] /\ x.c = l.c /\ x.v = l.v} : MemberFail(m)
    \/ /\ l.k = "gone"
       /\ \E m \in {x \in rm : dhost[x.to] = l.n /\ x.c = l.c /\ x.v = l.v} :
            IF [c |-> l.c, v |-> l.v] \in failed[m.to] THEN UNCHANGED vars ELSE GoneRefuses(m)
    \/ /\ l.k = "mayfail"
       /\ \/ UNCHANGED vars
          \/ \E r \in {x \in pend : x.c = l.c /\ x.v = l.v} : OwnFail(l.n, r)
    \/ /\ l.k = "restart"
       /\ Restart(l.n)
    \/ /\ l.k = "format"
       /\ Format(l.n)
    \/ /\ l.k = "recovered"
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
    \/ /\ l.k = "mayrecover"
       /\ \/ UNCHANGED vars
          \/ RecoverLine(l)
    \/ /\ l.k = "reported"
       /\ IF ivs[l.p][l.iv].lv = 0 \/ [p |-> l.p, k |-> l.iv] \in recd
          THEN UNCHANGED vars
          ELSE IvAt(l.n, l.p) = l.iv /\ ReportRecovered(l.n, l.p)
    \/ /\ l.k = "ask"
       /\ IF reqIv[l.p] = l.iv
          THEN UNCHANGED vars
          ELSE IvAt(l.n, l.p) = l.iv /\ (AskNext(l.n, l.p) \/ CounterOut(l.n, l.p))
    \/ /\ l.k = "committed"
       /\ [c |-> l.c, v |-> l.v] \in ackd
       /\ UNCHANGED vars
    \/ /\ l.k = "peer"
       /\ pr[l.n][l.p].iv = l.iv
       /\ ReplyMatches(l)
       /\ PeerOpen(l.n, l.p)
       /\ l.j \subseteq PeerElig(l.n, l.p)
       /\ PersistsAfter(l) \in PeerPersists(l.n, l.p)
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

TraceInit == Init /\ line = 1 /\ healed = {}

TraceNext ==
    /\ line <= Len(Trace)
    /\ Line(Trace[line])
    /\ line' = line + 1
    /\ healed' = IF Trace[line].k = "recovered" /\ miss[ddisk[Trace[line].n]][Trace[line].c] # NoMiss
                    /\ ~RecoveryOver(Trace[line])
                 THEN healed \cup {[c |-> Trace[line].c, v |-> Trace[line].v]}
                 ELSE healed

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, line, healed>>

TraceIncomplete == line <= Len(Trace)

\* Vers, ackd, dec and tbl only grow and what these read of them never changes,
\* so a break stays: each is checked once, at the end of the trace, since on
\* every state a long trace takes hours. A trace stopped by a line evaluates
\* none of them.
AtEndCommitted == line <= Len(Trace) \/ NothingCommittedDropped
AtEndOneWrite == line <= Len(Trace) \/ OneWritePerVersion
AtEndMerges == line <= Len(Trace) \/ MergesOnKept
AtEndTables == line <= Len(Trace) \/ TablesForward
=============================================================================
