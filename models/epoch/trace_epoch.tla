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

VARIABLE line

\* A version in a trace needs more than the 3 bits of epoch.tla's counter.
TBase == 1048576
TVer(k, n) == k * TBase + n
TIvOfV(v) == v \div TBase
TCnt(v) == v % TBase

TPlanSet == {[free |-> FALSE, m |-> TPlan, w |-> TWrites, f |-> {"restart", "format"},
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
        /\ Reply(r.d, r.c).list = r.list
        /\ Decided1(Reply(r.d, r.c).decided) = Decided1(r.decided)

KeptMatches(l) ==
    \E x \in dec' :
        /\ x.p = l.p /\ x.k = l.iv /\ x.n = l.n /\ x.r = run[l.n]
        /\ \A c \in Chunk : x.kept[c] = l.kept[c]

Line(l) ==
    \/ /\ l.k = "plan"
       /\ Membership
    \/ /\ l.k = "fold"
       /\ Reactive
       /\ tbl'[Len(tbl')][l.p].la = l.iv
    \/ /\ l.k = "renew"
       /\ Reactive
       /\ Cur(l.p) < tbl'[Len(tbl')][l.p].iv
    \/ /\ l.k = "adopt"
       /\ IF l.e <= ep[l.n] THEN UNCHANGED vars ELSE Adopt(l.n, l.e)
    \/ /\ l.k = "send"
       /\ IF ENABLED Send(l.w) THEN Send(l.w) ELSE UNCHANGED vars
    \/ /\ l.k = "order"
       /\ \E x \in {y \in pin[l.n] : y.w = l.w} : Order(l.n, x)
       /\ pv'[l.n][l.c] = l.v
    \/ /\ l.k = "logged"
       /\ IF l.v \in dlog[l.d][l.c]
          THEN UNCHANGED vars
          ELSE \E m \in {x \in rm : x.to = l.d /\ x.c = l.c /\ x.v = l.v} : Deliver(m)
    \/ /\ l.k = "committed"
       /\ [c |-> l.c, v |-> l.v] \in ackd
       /\ UNCHANGED vars
    \/ /\ l.k = "peer"
       /\ pr[l.n][l.p].iv = l.iv
       /\ \A d \in l.j : CanAnswer(d, l.p, l.iv)
       /\ ReplyMatches(l)
       /\ Peer(l.n, l.p)
       /\ KeptMatches(l)
    \/ /\ l.k = "persisted"
       /\ \E x \in dec : x.p = l.p /\ x.k = l.iv
       /\ ~\E a \in act : a.p = l.p /\ a.k = l.iv
       /\ UNCHANGED vars
    \/ /\ l.k = "activate"
       /\ pr[l.n][l.p].iv = l.iv
       /\ Activate(l.n, l.p)

TraceInit == Init /\ line = 1

TraceNext ==
    /\ line <= Len(Trace)
    /\ Line(Trace[line])
    /\ line' = line + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, line>>

TraceIncomplete == line <= Len(Trace)
=============================================================================
