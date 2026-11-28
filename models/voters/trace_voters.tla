---------------------------- MODULE trace_voters ----------------------------
\* Trace validation of the voter replacement model: TLC checks that a simulator run of
\* internal/raftshard, as tools/votersdata writes it into module trace_data, is a behavior of
\* voters.tla. Each line names the spec action that takes it; what the record observed is
\* checked against the state the action leaves. No protocol rule lives here. A record the spec
\* compresses into another action's step is a stutter: see "From spec to code" in
\* docs/design.md's Testing strategy. Variables no line observes stay free.
\*
\* Replicate hands the follower the leader's whole log and its commit index, or its log up to
\* that index and the commit index before its latest advance, as it takes the append, where the
\* code's follower takes the entries and the index the append left with, as etcd-raft batches a
\* long log: the spec terms of a follower's log a line observes are a prefix of the spec's, and
\* a commit or applied index at most the spec's. The terms pin each entry an index counts.
\*
\* TraceIncomplete is an invariant whose violation is the pass: the line counter passed the
\* end. A run that finishes with no violation failed, and its search depth names the line no
\* state could take.

EXTENDS voters, trace_data

VARIABLE line

Terms(s) == [k \in 1 .. Len(s) |-> s[k].t]

Line(l) ==
    \/ /\ l.k = "elect"
       /\ \E Q \in SUBSET Replicas : Elect(l.i, Q)
       /\ term'[l.i] = l.t
    \/ /\ l.k = "transfer"
       /\ \E f \in Replicas, Q \in SUBSET Replicas : Transfer(f, l.i, Q)
       /\ term'[l.i] = l.t
    \/ /\ l.k = "term"
       /\ UNCHANGED vars
    \/ /\ l.k = "addlearner"
       /\ \E p \in Replicas : AddLearner(l.i, p)
       /\ log'[l.i][Len(log'[l.i])].cfg = l.cfg
    \/ /\ l.k = "swap"
       /\ \E a, d \in Replicas : Swap(l.i, a, d)
       /\ log'[l.i][Len(log'[l.i])].cfg = l.cfg
    \/ /\ l.k = "leave"
       /\ Leave(l.i)
       /\ log'[l.i][Len(log'[l.i])].cfg = l.cfg
    \/ /\ l.k = "replicate"
       /\ Len(l.lg) >= l.c
       /\ \/ /\ IsPrefix(l.lg, Terms(log[l.j])) /\ commit[l.j] >= l.c /\ term[l.j] = term[l.i]
             /\ UNCHANGED vars
          \/ /\ Replicate(l.i, l.j)
             /\ IsPrefix(l.lg, Terms(log'[l.j])) /\ commit'[l.j] >= l.c
    \/ /\ l.k = "commit"
       /\ IF commit[l.i] >= l.c THEN UNCHANGED vars ELSE Commit(l.i) /\ commit'[l.i] >= l.c
    \/ /\ l.k = "apply"
       /\ IF applied[l.r] >= l.a THEN UNCHANGED vars ELSE Apply(l.r) /\ applied'[l.r] >= l.a
    \/ /\ l.k = "removed"
       /\ IF out[l.r] THEN UNCHANGED vars ELSE Apply(l.r) /\ out'[l.r]
    \/ /\ l.k = "handofffails"
       /\ \E i \in Replicas : HandOffFails(l.l, i)
       /\ term'[l.l] = l.t
    \/ /\ l.k = "fortify"
       /\ IF ENABLED Fortify(l.i) THEN Fortify(l.i) /\ l.f \subseteq fort'[l.i] ELSE UNCHANGED vars
    \/ /\ l.k = "lapse"
       /\ IF support[l.j] = {l.i} \/ l.j \in fort[l.i] THEN Lapse(l.j, l.i) ELSE UNCHANGED vars
    \/ /\ l.k = "askleader"
       /\ \/ AskLeader(l.r, l.l)
          \/ support[l.r] # {l.l} /\ UNCHANGED vars
    \/ /\ l.k = "notice"
       /\ IF out[l.r] THEN UNCHANGED vars ELSE Notice(l.r, l.p)
    \/ /\ l.k = "delete"
       /\ Delete(l.r)
    \/ /\ l.k = "crash"
       /\ Crash(l.r)
    \/ /\ l.k = "die"
       /\ Die(l.r)
    \/ /\ l.k = "halt"
       /\ Halt(l.r)
       /\ applied'[l.r] >= l.a
    \/ /\ l.k = "restart"
       /\ Restart(l.r)
       /\ commit'[l.r] >= l.c /\ applied'[l.r] = l.a

TraceInit == Init /\ line = 1

TraceNext ==
    /\ line <= Len(Trace)
    /\ Line(Trace[line])
    /\ line' = line + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, line>>

TraceIncomplete == line <= Len(Trace)
=============================================================================
