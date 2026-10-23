------------------------------- MODULE epoch -------------------------------
\* The epoch change protocol of docs/design.md, "Epoch changes and failure
\* detection": placement tables numbering each PG's intervals, requests checked
\* against their PG's interval, peering across every interval since the last
\* activated one, missing lists, versions merged on the version below them,
\* incarnation fencing of client writes, per-node-pair read leases counted on
\* two clocks per node, and Raft leader fortification on the same leases.
\*
\* Each element of Bugs removes or breaks one rule; a negative configuration
\* sets one and names the property it must violate.

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Node,       \* storage nodes 1..N; node n starts with disk n
    PG,         \* placement groups
    Chunk,      \* chunks
    ChunkPG,    \* [Chunk -> PG]
    Blocks,     \* [Chunk -> Nat]: blocks per chunk
    MinLive,    \* [PG -> Nat]: the pool's min_live
    Maint,      \* [PG -> BOOLEAN]: the pool lets a member be marked maintenance
    InitMem,    \* [PG -> Seq(Node)]: interval 1's members, primary first
    MaxSend,    \* times a client sends one write
    MaxTable,   \* tables the control plane commits
    Spare,      \* tables beyond a plan's changes: renewals and folds of activation reports
    MaxCounter, \* a chunk's counter limit within one interval (2^32 - 1 in the code)
    MaxFormat,  \* disks formatted again
    MaxRestart, \* process restarts per node
    MaxTerm,    \* Raft terms
    MaxFault,   \* clock and network faults
    Moves,      \* control-plane changes enabled with no plans, and "fold"
    Plans,      \* set of [free, m, w, f, t, k, s (, n)]: with free, any change in
                \* Moves at any time, up to n tables, otherwise the membership changes
                \* m in order; w the client writes, Seq([c, blk, whole, inc, re,
                \* after]); f the faults and flags enabled; t the time bound; k the
                \* faults; s the spare tables, Spare unless the plan says
    Clocks,     \* TRUE: two clocks per node; FALSE: lease expiry left open
    RawRates,   \* [Node -> SUBSET Nat]: CLOCK_MONOTONIC_RAW's rate, per mille of real time
    Slews,      \* [Node -> SUBSET Int]: CLOCK_BOOTTIME's slew, per mille of the raw rate
    MaxTime,    \* real time bound, in units of 10 ms
    Bugs

\* Lease constants of design.md "Lease time", in units of 10 ms and per mille.
Lease == 400          \* primary_lease, 4 s
Drift == 10           \* lease_drift, 1%
RateGap == 10         \* rate_gap, 100 ms
HB == 100             \* heartbeat period, 1 s
MaxMargin == 500      \* max_margin, 50%
L1000 == Lease * 1000 \* a lease in clock readings (rate per mille times units)

Bug(b) == b \in Bugs
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Newest(S) == IF S = {} THEN 0 ELSE Max(S)
Ceil(a, b) == (a + b - 1) \div b

\* A version k.n packs interval k and counter n; 0 is the absent chunk.
Ver(k, n) == k * 8 + n
IvOfV(v) == v \div 8
Cnt(v) == v % 8

MaxDisk == Cardinality(Node) + MaxFormat
Disk == 1..MaxDisk
NoMiss == -1
NoWin == [w |-> FALSE, w1s |-> 0, w1r |-> 0, w1g |-> 0, B |-> 0,
          lg |-> FALSE, a |-> FALSE, as |-> 0, asB |-> 0, ag |-> 0, am |-> Drift,
          ep |-> 0, lastg |-> 0, has |-> FALSE, rn |-> 0]
NoPr == [iv |-> 0, act |-> FALSE, srv |-> FALSE, since |-> 0]
Off == [on |-> FALSE, x |-> 0, rn |-> 0, ep |-> 0, s |-> 0, sB |-> 0]
NoFort == [l |-> 0, ep |-> 0]
Quorum == Cardinality(Node) \div 2 + 1
Infinity == MaxTime + 1

VARIABLES
    \* Control plane and tables
    tbl,      \* Seq([PG -> [iv, la]]): every committed table
    ivs,      \* [PG -> Seq([mem: Seq(Disk), st: [Disk -> mark]])]: each interval's members
    reqIv,    \* [PG -> Nat]: the interval whose primary asked for a new one
    reports,  \* set of [p, k]: activation reports on heartbeats, possibly late or repeated
    plan,     \* the planned membership changes of this behavior
    step,     \* the next planned change
    \* Nodes and disks
    ep,       \* [Node -> Nat]: the table each node holds
    dnode,    \* [Disk -> Node \cup {0}]: the node hosting a disk, 0 once its log is gone
    ddisk,    \* [Node -> Disk]: each node's disk
    fmts,     \* disks formatted again so far
    stale,    \* nodes cut off from the control plane, which adopt no new table
    run,      \* [Node -> Nat]: process incarnation
    rsIv,     \* [Node -> [PG -> Nat]]: the interval a restarted primary may not peer in
    dlog,     \* [Disk -> [Chunk -> SUBSET Nat]]: versions in each disk's log
    miss,     \* [Disk -> [Chunk -> Int]]: persisted missing list
    persIv,   \* [Disk -> [PG -> Nat]]: the interval whose missing list a disk persisted
    mrecv,    \* [Disk -> SUBSET [p, k, ent]]: with ActivateBeforeDurable, lists not yet durable
    \* Primaries
    pr,       \* [Node -> [PG -> [iv, act, srv, since]]]
    pv,       \* [Node -> [Chunk -> Nat]]: newest version a primary ordered
    pin,      \* [Node -> SUBSET [w, hold]]: client writes a primary holds unordered
    known,    \* [Node -> [PG -> Nat]]: newest incarnation of the session a primary has seen
    dec,      \* set of [p, k, n, r, kept]: peering decisions
    act,      \* set of [p, k, r]: activated intervals
    \* Data path
    rm,       \* set of replicated writes in flight or delivered (kept, so duplicated)
    vers,     \* set of [c, v, w, on, blk, n, r]: every version ordered (on: merged on)
    ackd,     \* set of [c, v]: versions acknowledged to the client
    wst,      \* [1..Len(Writes) -> 0..2]: unsent, sent, acknowledged
    sends,    \* [1..Len(Writes) -> Nat]
    dmn,      \* 1 while the client daemon's first incarnation runs, 2 after its successor starts
    bad,      \* set of names: completions and keeps the properties forbid, seen when they happen
    \* Leases
    gr,       \* [Node -> [Node -> [ep, live, fg, on, tR, tB]]]: grantor side
    hl,       \* [Node -> [Node -> NoWin]]: holder side
    rw,       \* [Node -> BOOLEAN]: in the restart wait
    \* Clocks (constant when Clocks is FALSE)
    now, raw, boot, rawR, bootR, slew, runId, frz, suspAt, rstAt,
    rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty, hs, faults,
    \* Raft
    rterm, rlead, fort, fby, xfer, xto

ctlVars == <<tbl, ivs, reqIv, reports, plan, step>>
nodeVars == <<ep, dnode, ddisk, fmts, stale, run, rsIv, dlog, miss, persIv, mrecv>>
priVars == <<pr, pv, pin, known, dec, act>>
dataVars == <<rm, vers, ackd, wst, sends, dmn, bad>>
leaseVars == <<gr, hl, rw>>
clockVars == <<now, raw, boot, rawR, bootR, slew, runId, frz, suspAt, rstAt,
               rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty, hs, faults>>
raftVars == <<rterm, rlead, fort, fby, xfer, xto>>
vars == <<ctlVars, nodeVars, priVars, dataVars, leaseVars, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* Views of tables and intervals

Writes == plan.w
\* The plan's Raft group, with a voter on every node.
Raft == "raft" \in plan.f

Mem(p, i) == {ivs[p][i].mem[j] : j \in 1..Len(ivs[p][i].mem)}
St(p, i, d) == ivs[p][i].st[d]
UpIn(p, i) == {d \in Mem(p, i) : St(p, i, d) = "up"}
LiveCnt(p, i) == Cardinality({d \in Mem(p, i) : St(p, i, d) # "down"})
UpSeq(p, i) == SelectSeq(ivs[p][i].mem, LAMBDA d : St(p, i, d) = "up")
PrimDisk(p, i) ==
    LET m == ivs[p][i].mem
        J == {j \in 1..Len(m) : St(p, i, m[j]) = "up"}
    IN IF J = {} THEN 0 ELSE m[Min(J)]
PrimNode(p, i) == IF PrimDisk(p, i) = 0 THEN 0 ELSE dnode[PrimDisk(p, i)]
IvAt(n, p) == tbl[ep[n]][p].iv
LaAt(n, p) == tbl[ep[n]][p].la
Cur(p) == tbl[Len(tbl)][p].iv
ChunksOf(p) == {c \in Chunk : ChunkPG[c] = p}
Running(n) == frz[n] = "run"
\* Every interval in (from, to] had n as its primary.
KeptPrimary(n, p, from, to) == \A i \in from + 1..to : PrimNode(p, i) = n

Leads(n, p) ==
    /\ pr[n][p].iv # 0
    /\ pr[n][p].iv = IvAt(n, p)
    /\ PrimNode(p, IvAt(n, p)) = n

-----------------------------------------------------------------------------
\* Lease time

RateRaw(n) == IF frz[n] = "run" THEN rawR[n] ELSE 0
RateBoot(n) == IF frz[n] \in {"run", "susp"} THEN bootR[n] ELSE 0
\* The first time at which a clock running at rate rt gains need more.
Until(need, rt) == IF need <= 0 THEN now ELSE IF rt = 0 THEN Infinity ELSE now + Ceil(need, rt)

\* The holder ends a lease when either clock shows the lease less its margin.
HolderEnd(h, g) ==
    LET x == hl[h][g]
        left == Lease * (1000 - x.am)
        tr == Until(left - (raw[h] - x.as), RateRaw(h))
        tb == Until(left - (boot[h] - x.asB), RateBoot(h))
    IN IF Bug("HolderSlowerClock") THEN Max({tr, tb}) ELSE Min({tr, tb})

\* The grantor keeps a grant until both clocks show the whole lease.
GrantEnd(g, h) ==
    LET y == gr[g][h]
        tr == Until(L1000 - (raw[g] - y.tR), RateRaw(g))
        tb == Until(L1000 - (boot[g] - y.tB), RateBoot(g))
    IN IF Bug("GrantorFasterClock") THEN Min({tr, tb}) ELSE Max({tr, tb})

LeaseOK(h, g) ==
    IF Clocks THEN hl[h][g].a /\ HolderEnd(h, g) > now
    ELSE hl[h][g].has

GrantLive(g, h) ==
    IF Clocks THEN gr[g][h].on /\ GrantEnd(g, h) > now
    ELSE gr[g][h].live

\* A grant the grantor still knows of: a restart forgets them.
GrantKnown(g, h) == GrantLive(g, h) /\ ~gr[g][h].fg

\* Whether the holder needs the grantor's lease: it leads a PG the grantor is
\* a live member of, or, in a plan that checks the Raft group alone, it leads
\* the group.
Needs(h, g) ==
    /\ h # g
    /\ IF Raft THEN rlead[h]
       ELSE \E p \in PG : Leads(h, p) /\ g \in {dnode[d] : d \in UpIn(p, IvAt(h, p))}

-----------------------------------------------------------------------------
\* Properties' views

\* A primary serves a PG's reads: it activated, or an interval that kept it
\* as primary followed one it activated; the interval has at least min_live
\* live members, since peering skips one with fewer; and it holds a lease from
\* each other live member, having handled every peer message that waited for it.
Reading(n, p) ==
    /\ Leads(n, p)
    /\ pr[n][p].srv
    /\ LiveCnt(p, IvAt(n, p)) >= MinLive[p] \/ Bug("ReadBelowMinLive")
    /\ Running(n)
    /\ arrived[n] = {} \/ Bug("ReadsBeforePeerMessages")
    /\ \A d \in UpIn(p, IvAt(n, p)) :
         /\ dnode[d] # 0
         /\ dnode[d] = n \/ LeaseOK(n, dnode[d])

\* The Raft leader serves lease reads while a quorum of the voters that
\* fortified it, itself included, give unexpired leases in the same support
\* epoch, and it has not handed leadership over.
RaftReads(n) ==
    /\ Raft
    /\ rlead[n]
    /\ Running(n)
    /\ ~xfer[n] \/ Bug("TransferKeepsLease")
    /\ Cardinality({n} \cup {v \in Node \ {n} :
                     fby[n][v] >= 0 /\ LeaseOK(n, v) /\ hl[n][v].ep = fby[n][v]}) >= Quorum

ActDecs == {x \in dec : [p |-> x.p, k |-> x.k, r |-> x.r] \in act}
AckedV(c) == {y.v : y \in {z \in ackd : z.c = c}}

RECURSIVE Chain(_, _)
\* The versions a version's bytes rest on, following each merge to its base.
Chain(c, v) ==
    IF v = 0 THEN {}
    ELSE LET R == {r \in vers : r.c = c /\ r.v = v}
         IN IF R = {} THEN {v} ELSE {v} \cup Chain(c, (CHOOSE r \in R : TRUE).on)

BlkOf(c, v) == (CHOOSE r \in vers : r.c = c /\ r.v = v).blk
WOf(c, v) == (CHOOSE r \in vers : r.c = c /\ r.v = v).w
\* Per block, the newest version that wrote it.
Content(c, S) == [b \in 1..Blocks[c] |-> Newest({v \in S : BlkOf(c, v) = b})]

\* Versions committed: acknowledged in their interval, or kept by an activated
\* interval's peering.
CommitAt(c) ==
    {[v |-> u, at |-> IvOfV(u)] : u \in AckedV(c)}
    \cup {[v |-> x.kept[c], at |-> x.k] : x \in {y \in ActDecs : y.p = ChunkPG[c] /\ y.kept[c] # 0}}

-----------------------------------------------------------------------------
\* Initial state

\* A plan's raw rates: with "exact" every clock runs at real time, with
\* "fast" node 3 alone runs at the fastest rate of RawRates.
PlanRates(n) ==
    IF "exact" \in plan.f THEN {1000}
    ELSE IF "fast" \in plan.f THEN IF n = 3 THEN {Max(RawRates[3])} ELSE {1000}
    ELSE RawRates[n]

\* With lease expiry left open, interval 1's primaries start with their grants.
InitGrant(g, h) ==
    ~Clocks /\ g # h /\ \E p \in PG : InitMem[p][1] = h /\ \E j \in 1..Len(InitMem[p]) : InitMem[p][j] = g

InitIv(p) ==[mem |-> InitMem[p], st |-> [d \in Disk |-> IF d \in Node THEN "up" ELSE "down"]]

Init ==
    /\ tbl = <<[p \in PG |-> [iv |-> 1, la |-> 1]]>>
    /\ ivs = [p \in PG |-> <<InitIv(p)>>]
    /\ reqIv = [p \in PG |-> 0]
    /\ plan \in Plans
    /\ step = 1
    /\ reports = {}
    /\ ep = [n \in Node |-> 1]
    /\ dnode = [d \in Disk |-> IF d \in Node THEN d ELSE 0]
    /\ ddisk = [n \in Node |-> n]
    /\ fmts = 0
    /\ stale = IF "stale2" \in plan.f THEN {2} ELSE {}
    /\ run = [n \in Node |-> 1]
    /\ rsIv = [n \in Node |-> [p \in PG |-> 0]]
    /\ dlog = [d \in Disk |-> [c \in Chunk |-> {}]]
    /\ miss = [d \in Disk |-> [c \in Chunk |-> NoMiss]]
    /\ persIv = [d \in Disk |-> [p \in PG |-> IF d \in Node THEN 1 ELSE 0]]
    /\ mrecv = [d \in Disk |-> {}]
    /\ pr = [n \in Node |-> [p \in PG |->
              IF InitMem[p][1] = n THEN [iv |-> 1, act |-> TRUE, srv |-> TRUE, since |-> 1]
              ELSE NoPr]]
    /\ pv = [n \in Node |-> [c \in Chunk |-> 0]]
    /\ pin = [n \in Node |-> {}]
    /\ known = [n \in Node |-> [p \in PG |-> 0]]
    /\ dec = {[p |-> p, k |-> 1, n |-> InitMem[p][1], r |-> 1, kept |-> [c \in Chunk |-> 0]] : p \in PG}
    /\ act = {[p |-> p, k |-> 1, r |-> 1] : p \in PG}
    /\ rm = {}
    /\ vers = {}
    /\ ackd = {}
    /\ wst = [i \in 1..Len(Writes) |-> 0]
    /\ sends = [i \in 1..Len(Writes) |-> 0]
    /\ dmn = 1
    /\ bad = {}
    /\ gr = [g \in Node |-> [h \in Node |->
              [ep |-> 1, live |-> InitGrant(g, h), fg |-> FALSE, on |-> FALSE, tR |-> 0, tB |-> 0]]]
    /\ hl = [h \in Node |-> [g \in Node |-> [NoWin EXCEPT !.has = InitGrant(g, h), !.ep = 1]]]
    /\ rw = [n \in Node |-> FALSE]
    /\ now = 0
    /\ raw = [n \in Node |-> 0]
    /\ boot = [n \in Node |-> 0]
    /\ rawR \in [Node -> UNION {RawRates[n] : n \in Node} \cup {1000}]
    /\ \A n \in Node : rawR[n] \in PlanRates(n)
    /\ slew \in [Node -> UNION {Slews[n] : n \in Node} \cup {0}]
    /\ \A n \in Node : slew[n] \in IF "exact" \in plan.f \/ "fast" \in plan.f THEN {0} ELSE Slews[n]
    /\ bootR = [n \in Node |-> (rawR[n] * (1000 + slew[n])) \div 1000]
    /\ runId = [n \in Node |-> 1]
    /\ frz = [n \in Node |-> "run"]
    /\ suspAt = [n \in Node |-> 0]
    /\ rstAt = [n \in Node |-> [r |-> 0, b |-> 0]]
    /\ rq = [h \in Node |-> [g \in Node |-> Off]]
    /\ gm = [g \in Node |-> [h \in Node |-> Off]]
    /\ hbm = [g \in Node |-> [h \in Node |-> Off]]
    /\ lastAsk = [h \in Node |-> [g \in Node |-> -1]]
    /\ lastHb = [g \in Node |-> [h \in Node |-> -1]]
    /\ net = [h \in Node |-> [g \in Node |-> "ok"]]
    /\ arrived = [n \in Node |-> {}]
    /\ dirty = [h \in Node |-> [g \in Node |-> FALSE]]
    /\ hs = [h \in Node |-> [g \in Node |-> -1]]
    /\ faults = 0
    /\ rterm = [n \in Node |-> IF Raft THEN 1 ELSE 0]
    /\ rlead = [n \in Node |-> Raft /\ n = 1]
    /\ fort = [n \in Node |-> NoFort]
    /\ fby = [n \in Node |-> [v \in Node |-> -1]]
    /\ xfer = [n \in Node |-> FALSE]
    /\ xto = 0

-----------------------------------------------------------------------------
\* Control plane: each change commits one table.

Commit(t) == /\ Len(tbl) < IF plan.free THEN plan.n ELSE MaxTable
             /\ tbl' = Append(tbl, t)

\* A new interval of each PG in ps, with members from f[p].
NewIntervals(ps, f) ==
    /\ Commit([p \in PG |-> IF p \in ps THEN [iv |-> Cur(p) + 1, la |-> tbl[Len(tbl)][p].la]
                            ELSE tbl[Len(tbl)][p]])
    /\ ivs' = [p \in PG |-> IF p \in ps THEN Append(ivs[p], f[p]) ELSE ivs[p]]

Allowed(p, d, from, to) ==
    \/ from = "up" /\ to = "down"
    \/ from = "down" /\ to = "up" /\ dnode[d] # 0
    \/ from = "up" /\ to = "maint" /\ Maint[p]
    \/ from = "maint" /\ to = "up" /\ dnode[d] # 0

\* Marking a disk down, up or in maintenance starts a new interval of every PG
\* it belongs to.
MarkDisk(d, to) ==
    /\ LET ps == {p \in PG : d \in Mem(p, Cur(p)) /\ Allowed(p, d, St(p, Cur(p), d), to)}
       IN /\ ps # {}
          /\ NewIntervals(ps, [p \in PG |->
               [ivs[p][Cur(p)] EXCEPT !.st[d] = to]])
    /\ UNCHANGED <<reqIv, reports>>

\* A refill or move replaces a member with another disk.
Replace(p, old, new) ==
    /\ old \in Mem(p, Cur(p))
    /\ new \notin Mem(p, Cur(p))
    /\ dnode[new] # 0
    /\ LET m == ivs[p][Cur(p)].mem
           j == CHOOSE j \in 1..Len(m) : m[j] = old
       IN NewIntervals({p}, [q \in PG |->
            [mem |-> [m EXCEPT ![j] = new], st |-> [ivs[p][Cur(p)].st EXCEPT ![new] = "up"]]])
    /\ UNCHANGED <<reqIv, reports>>

\* Moving the primary: the first two members swap.
Rotate(p) ==
    /\ LET m == ivs[p][Cur(p)].mem
       IN /\ Len(m) >= 2
          /\ NewIntervals({p}, [q \in PG |->
               [ivs[p][Cur(p)] EXCEPT !.mem = <<m[2], m[1]>> \o SubSeq(m, 3, Len(m))]])
    /\ UNCHANGED <<reqIv, reports>>

\* A primary that restarted, or whose chunk counter ran out, gets a new interval.
Renew(p) ==
    /\ reqIv[p] = Cur(p)
    /\ NewIntervals({p}, [q \in PG |-> ivs[p][Cur(p)]])
    /\ reqIv' = [reqIv EXCEPT ![p] = 0]
    /\ UNCHANGED reports

\* The leader folds an activation report, possibly late, into a table that
\* starts no interval.
Fold(p, k) ==
    /\ "fold" \in Moves
    /\ [p |-> p, k |-> k] \in reports
    /\ k > tbl[Len(tbl)][p].la
    /\ Commit([tbl[Len(tbl)] EXCEPT ![p].la = Max({k, tbl[Len(tbl)][p].la})])
    /\ UNCHANGED <<ivs, reqIv, reports>>

\* A planned membership change: [k |-> "mark", d, to], [k |-> "replace", p,
\* old, new] or [k |-> "rotate", p].
Planned(m) ==
    \/ m.k = "mark" /\ MarkDisk(m.d, m.to)
    \/ m.k = "replace" /\ Replace(m.p, m.old, m.new)
    \/ m.k = "rotate" /\ Rotate(m.p)

\* A free plan makes any change in Moves at any time; another makes its
\* changes in order.
Membership ==
    /\ \/ /\ plan.free
          /\ \/ \E d \in Disk, to \in {"up", "down", "maint"} : to \in Moves /\ MarkDisk(d, to)
             \/ "replace" \in Moves /\ \E p \in PG, old, new \in Disk : Replace(p, old, new)
             \/ "rotate" \in Moves /\ \E p \in PG : Rotate(p)
          /\ UNCHANGED <<plan, step>>
       \/ /\ step <= Len(plan.m)
          /\ Planned(plan.m[step])
          /\ step' = step + 1
          /\ UNCHANGED plan
    /\ UNCHANGED <<nodeVars, priVars, dataVars, leaseVars, clockVars, raftVars>>

\* Renewals a primary asks for and folds of activation reports.
Reactive ==
    /\ plan.free \/ Len(tbl) - step < plan.s
    /\ \E p \in PG : Renew(p) \/ \E k \in 1..MaxTable : Fold(p, k)
    /\ UNCHANGED <<plan, step, nodeVars, priVars, dataVars, leaseVars, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* A node adopts a newer table, possibly skipping some. Where a PG's interval
\* changed under its primary, the primary holds its unordered client writes
\* and re-peers; if the new interval kept it as primary since the one it
\* served in, it goes on serving reads of committed versions and keeps its
\* table of incarnations, which any other change of primary forgets.

AdoptPr(n, p, e) ==
    LET old == pr[n][p]
        k == tbl[e][p].iv
    IN IF k = IvAt(n, p) THEN old
       ELSE IF PrimNode(p, k) = n /\ (k > rsIv[n][p] \/ Bug("RestartSameInterval"))
            THEN IF old.iv # 0 /\ KeptPrimary(n, p, old.iv, k)
                 THEN [iv |-> k, act |-> FALSE, srv |-> old.srv, since |-> old.since]
                 ELSE [iv |-> k, act |-> FALSE, srv |-> FALSE, since |-> k]
            ELSE NoPr

Adopt(n, e) ==
    /\ Running(n)
    /\ e \in ep[n] + 1..Len(tbl)
    /\ ep' = [ep EXCEPT ![n] = e]
    /\ pr' = [pr EXCEPT ![n] = [p \in PG |-> AdoptPr(n, p, e)]]
    /\ known' = [known EXCEPT ![n] = [p \in PG |->
          IF AdoptPr(n, p, e).iv # 0 /\ AdoptPr(n, p, e).since = pr[n][p].since THEN known[n][p] ELSE 0]]
    /\ pin' = [pin EXCEPT ![n] =
          {IF tbl[e][ChunkPG[Writes[x.w].c]].iv = IvAt(n, ChunkPG[Writes[x.w].c])
           THEN x
           ELSE [w |-> x.w, hold |-> IF x.hold # 0 THEN x.hold ELSE IvAt(n, ChunkPG[Writes[x.w].c])] :
             x \in {y \in pin[n] : LET p == ChunkPG[Writes[y.w].c]
                                   IN tbl[e][p].iv = IvAt(n, p) \/ AdoptPr(n, p, e).iv # 0}}]
    /\ UNCHANGED <<ctlVars, dnode, ddisk, fmts, stale, run, rsIv, dlog, miss, persIv, mrecv,
                   pv, dec, act, dataVars, leaseVars, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* Peering

\* The intervals a new primary of k must stop, from the last activated one L.
NeedIv(p, k, L) ==
    LET from == IF Bug("StopPreviousOnly") THEN k - 1 ELSE L
        ivl == IF Bug("ReachLastActivatedOnly") THEN {L} ELSE from..k - 1
    IN {i \in ivl : i >= 1 /\ LiveCnt(p, i) >= MinLive[p]}

\* The nodes a grantor n must not grant while it answers peering of interval
\* k of p: the primaries of the intervals since the last activated one that k
\* moves the primary away from, unless they adopted k themselves.
OldPrimaries(n, p, k) ==
    {X \in {PrimNode(p, i) : i \in LaAt(n, p)..k - 1} :
        X \notin {0, n, PrimNode(p, k)} /\ IvAt(X, p) < k}

\* A disk answers peering of interval k once its node holds a table with k. A
\* grantor answers no peering of an interval that moves a PG's primary away
\* from a node it still grants, unless that node adopted k itself; nor during
\* its restart wait. A member's state for the PG stays as it answered until
\* the primary decides, so the model takes the answers at the decision.
\* With lease expiry left open, the grants a member knows of to such nodes
\* lapse when it answers, as if it waited them out; a grant it forgot in a
\* restart blocks it until it lapses on its own.
CanAnswer(d, p, k) ==
    LET n == dnode[d]
    IN /\ n # 0
       /\ Running(n)
       /\ ~rw[n]
       /\ IvAt(n, p) = k
       /\ \A X \in OldPrimaries(n, p, k) :
            IF Clocks THEN ~GrantKnown(n, X) ELSE ~(GrantLive(n, X) /\ gr[n][X].fg)

\* Open lease expiry: the grants of J's nodes to the old primaries lapse.
LapseFor(J, p, k) ==
    LET lapse(g, h) == /\ \E d \in J : dnode[d] = g /\ h \in OldPrimaries(g, p, k)
                       /\ gr[g][h].live
    IN /\ gr' = [g \in Node |-> [h \in Node |->
                   IF ~Clocks /\ lapse(g, h) THEN [gr[g][h] EXCEPT !.live = FALSE] ELSE gr[g][h]]]
       /\ hl' = [h \in Node |-> [g \in Node |->
                   IF ~Clocks /\ lapse(g, h) THEN [hl[h][g] EXCEPT !.has = FALSE] ELSE hl[h][g]]]

Holds(d, c, v) == Newest(dlog[d][c]) >= v \/ miss[d][c] >= v

\* The keep rule. LS is the newest interval whose peering decision a reached
\* member persisted (at least the table's last activated interval L); members
\* that persisted it know, held or listed, the version it kept below LS.
\* A version of LS or later is kept unless a reached member live in its
\* interval neither holds nor lists it; every version below a kept one is kept.
Kept(p, L, J, c) ==
    LET LS == Max({persIv[d][p] : d \in J} \cup {L})
        RS == {d \in J : persIv[d][p] = LS}
        base == Newest({IF miss[d][c] # NoMiss THEN miss[d][c]
                        ELSE Newest({v \in dlog[d][c] : IvOfV(v) < LS}) : d \in RS})
        held == UNION {dlog[d][c] : d \in J}
        listed == {miss[d][c] : d \in J} \ {NoMiss, 0}
        keep(v) == \A d \in J \cap UpIn(p, IvOfV(v)) : Holds(d, c, v)
    IN IF Bug("DecideByOwnInterval")
       THEN Newest({v \in held \cup listed : keep(v)})
       ELSE Newest({base} \cup {v \in held : IvOfV(v) >= LS /\ keep(v)})

\* A member's missing-list entry: the kept version, unless it holds it as its
\* newest and lists nothing. Only recovery removes an entry: a member that
\* still lists a chunk may hold a dropped version's bytes under a newer write.
Entry(d, c, K) ==
    IF Newest(dlog[d][c]) = K /\ (miss[d][c] = NoMiss \/ Bug("PeeringClearsMissing"))
    THEN NoMiss ELSE K

\* The primary activates and reports it on its next heartbeat.
Activated(n, p, k) ==
    /\ pr' = [pr EXCEPT ![n][p] = [@ EXCEPT !.act = TRUE, !.srv = TRUE]]
    /\ act' = act \cup {[p |-> p, k |-> k, r |-> run[n]]}
    /\ reports' = reports \cup {[p |-> p, k |-> k]}

\* A decision of the primary's current run for interval k of p.
Decided(n, p, k) == \E x \in dec : x.p = p /\ x.k = k /\ x.n = n /\ x.r = run[n]
TheDec(n, p, k) == CHOOSE x \in dec : x.p = p /\ x.k = k /\ x.n = n /\ x.r = run[n]

\* The members in P persist, with interval k, their lists from ls.
PersistAll(P, p, k, ls) ==
    /\ miss' = [d \in Disk |-> IF d \in P
                   THEN [c \in Chunk |-> IF c \in ChunksOf(p) THEN ls[d][c] ELSE miss[d][c]]
                   ELSE miss[d]]
    /\ persIv' = [d \in Disk |-> IF d \in P THEN [persIv[d] EXCEPT ![p] = k] ELSE persIv[d]]

\* Whether something can stop interval k of p from activating once its
\* primary n decided: a later interval its members or primary may adopt
\* first, or a restart or reboot of n or a format of a member, while the plan
\* still allows one.
FaultLeft(kind) == kind \in plan.f /\ faults < Min({MaxFault, plan.k})
CanStall(n, p, k) ==
    \/ Cur(p) > k
    \/ (FaultLeft("restart") \/ FaultLeft("reboot")) /\ run[n] < MaxRestart
    \/ FaultLeft("format") /\ fmts < MaxFormat

\* The new primary of k peers: it reaches every live member of k and a live
\* member of every interval since the last activated one, decides what
\* survives and sends every live member its missing list, then activates once
\* all have persisted theirs. Nothing reads a member's list before the next
\* peering, which the member answers only once it left k, so a member that
\* persists its list while still in k may as well persist it at the decision:
\* the members in P do, and the rest at activation (Activate), which a primary
\* that restarts or moves on never reaches. Where nothing can stop the
\* interval, every member persists and the primary activates at the decision.
Peer(n, p) ==
    LET k == IvAt(n, p)
        L == LaAt(n, p)
        Elig == {d \in Mem(p, k) \cup UNION {Mem(p, i) : i \in L..k - 1} : CanAnswer(d, p, k)}
    IN /\ Running(n)
       /\ pr[n][p].iv = k
       /\ ~pr[n][p].act
       /\ PrimNode(p, k) = n
       /\ ~Decided(n, p, k)
       /\ LiveCnt(p, k) >= MinLive[p]
       /\ UpIn(p, k) \subseteq Elig
       /\ \E J \in SUBSET Elig :
          LET K == [c \in Chunk |-> IF c \in ChunksOf(p) THEN Kept(p, L, J, c) ELSE 0]
              \* The members the decision counts as holding the kept version:
              \* the recovery sources it reached, and the live members it
              \* leaves off the missing list.
              src(c) == {d \in J : Newest(dlog[d][c]) = K[c] /\ miss[d][c] = NoMiss}
                        \cup {d \in UpIn(p, k) : Entry(d, c, K[c]) = NoMiss}
              torn == \E c \in ChunksOf(p) : K[c] # 0 /\
                        \E s \in src(c) : Content(c, dlog[s][c]) # Content(c, Chain(c, K[c]))
              lists == [d \in UpIn(p, k) |-> [c \in ChunksOf(p) |-> Entry(d, c, K[c])]]
          IN /\ UpIn(p, k) \subseteq J
             /\ \A i \in NeedIv(p, k, L) : J \cap UpIn(p, i) # {}
             /\ LapseFor(J, p, k)
             /\ dec' = dec \cup {[p |-> p, k |-> k, n |-> n, r |-> run[n], kept |-> K]}
             /\ bad' = IF torn THEN bad \cup {"torn"} ELSE bad
             /\ pv' = [pv EXCEPT ![n] = [c \in Chunk |-> IF c \in ChunksOf(p) THEN K[c] ELSE pv[n][c]]]
             /\ \E P \in IF CanStall(n, p, k) THEN SUBSET UpIn(p, k) ELSE {UpIn(p, k)} :
                  IF Bug("ActivateBeforeDurable")
                  THEN /\ mrecv' = [d \in Disk |-> IF d \in UpIn(p, k)
                                       THEN mrecv[d] \cup {[p |-> p, k |-> k, ent |-> lists[d]]}
                                       ELSE mrecv[d]]
                       /\ Activated(n, p, k)
                       /\ UNCHANGED <<miss, persIv>>
                  ELSE IF CanStall(n, p, k)
                  THEN /\ PersistAll(P, p, k, lists)
                       /\ reports' = IF Bug("ReportBeforeActivate") THEN reports \cup {[p |-> p, k |-> k]}
                                     ELSE reports
                       /\ UNCHANGED <<pr, act, mrecv>>
                  ELSE /\ PersistAll(P, p, k, lists)
                       /\ Activated(n, p, k)
                       /\ UNCHANGED mrecv
       /\ UNCHANGED <<tbl, ivs, plan, step, reqIv, ep, dnode, ddisk, fmts, stale, run, rsIv, dlog,
                      pin, known, rm, vers, ackd, wst, sends, dmn,
                      rw, clockVars, raftVars>>

\* The members still to persist do so, as they could have at the decision:
\* their logs and lists have not changed since.
Activate(n, p) ==
    LET k == IvAt(n, p)
        R == {d \in UpIn(p, k) : persIv[d][p] # k}
        K == TheDec(n, p, k).kept
    IN /\ Running(n)
       /\ pr[n][p].iv = k
       /\ ~pr[n][p].act
       /\ PrimNode(p, k) = n
       /\ Decided(n, p, k)
       /\ \A d \in R : dnode[d] # 0 /\ Running(dnode[d]) /\ IvAt(dnode[d], p) = k
       /\ PersistAll(R, p, k, [d \in R |-> [c \in ChunksOf(p) |-> Entry(d, c, K[c])]])
       /\ Activated(n, p, k)
       /\ UNCHANGED <<tbl, ivs, plan, step, reqIv, ep, dnode, ddisk, fmts, stale, run, rsIv, dlog, mrecv,
                      pv, pin, known, dec, dataVars, leaseVars, clockVars, raftVars>>

\* With ActivateBeforeDurable, a member persists its list after activation,
\* and a restart loses a list it has not persisted yet.
Persist(d) ==
    \E x \in mrecv[d] :
       /\ dnode[d] # 0
       /\ Running(dnode[d])
       /\ IvAt(dnode[d], x.p) = x.k
       /\ miss' = [miss EXCEPT ![d] = [c \in Chunk |-> IF c \in ChunksOf(x.p) THEN x.ent[c] ELSE miss[d][c]]]
       /\ persIv' = [persIv EXCEPT ![d][x.p] = x.k]
       /\ mrecv' = [mrecv EXCEPT ![d] = mrecv[d] \ {x}]
       /\ UNCHANGED <<ctlVars, ep, dnode, ddisk, fmts, stale, run, rsIv, dlog, priVars, dataVars,
                      leaseVars, clockVars, raftVars>>


\* Recovery is a write the primary orders: it reads the kept version's blocks
\* from a member that holds it, then orders a write of them to a member that
\* lists the chunk. The real write takes a new version and carries only the
\* blocks no write of the interval ordered before it covers, so the member's
\* own writes of the interval stay on top; the model keeps the source's
\* versions up to the kept one under their own numbers, which Content reads
\* the same way. What the read returns never changes, since the source lists
\* nothing and a new interval stops the write, so the model takes the read
\* with the write and lets "at", the newest version the primary had ordered
\* when it read, be any since it activated. With RecoveryCoversAtRead, the
\* primary picks the blocks at the read, so the writes ordered between the
\* read and the write are overwritten.
Recover(n, p, c, d) ==
    LET k == IvAt(n, p)
        K == miss[d][c]
    IN /\ Running(n)
       /\ Leads(n, p)
       /\ pr[n][p].act
       /\ c \in ChunksOf(p)
       /\ d \in UpIn(p, k)
       /\ dnode[d] # 0
       /\ Running(dnode[d])
       /\ IvAt(dnode[d], p) = k
       /\ K # NoMiss
       /\ \E s \in UpIn(p, k), at \in {Ver(k, 0)} \cup {v \in Ver(k, 1)..pv[n][c] : IvOfV(v) = k} :
            /\ dnode[s] # 0
            /\ Running(dnode[s])
            /\ miss[s][c] = NoMiss
            /\ K = 0 \/ K \in dlog[s][c]
            /\ at = Ver(k, 0) \/ Bug("RecoveryCoversAtRead")
            /\ dlog' = [dlog EXCEPT ![d][c] =
                          {v \in dlog[s][c] : v <= K}
                          \cup {v \in dlog[d][c] : IvOfV(v) = k /\ (v <= at \/ ~Bug("RecoveryCoversAtRead"))}]
       /\ miss' = [miss EXCEPT ![d][c] = NoMiss]
       /\ UNCHANGED <<ctlVars, ep, dnode, ddisk, fmts, stale, run, rsIv, persIv, mrecv, priVars, dataVars,
                      leaseVars, clockVars, raftVars>>

\* A primary whose chunk counter ran out lets its pending writes complete,
\* then asks for a new interval.
CounterOut(n, p) ==
    LET k == IvAt(n, p)
    IN /\ Running(n)
       /\ Leads(n, p)
       /\ pr[n][p].act
       /\ reqIv[p] # k
       /\ \E x \in pin[n] : LET c == Writes[x.w].c IN
            ChunkPG[c] = p /\ IvOfV(pv[n][c]) = k /\ Cnt(pv[n][c]) = MaxCounter
       /\ \A r \in vers : r.n = n /\ r.r = run[n] /\ ChunkPG[r.c] = p
                            => [c |-> r.c, v |-> r.v] \in ackd
       /\ reqIv' = [reqIv EXCEPT ![p] = k]
       /\ UNCHANGED <<tbl, ivs, plan, step, reports, nodeVars, priVars, dataVars, leaseVars, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* Client writes. The client sends each write to the primary of its newest
\* table, on a connection to that process; a daemon's successor reissues its
\* writes, and its connection's hello names its incarnation. A write that
\* reaches the process late is the same as one the process holds unordered,
\* so a write reaches its process when sent.

Pre(i) ==
    LET w == Writes[i]
    IN /\ w.after # 0 => wst[w.after] = 2
       /\ w.inc = 1 => dmn = 1
       /\ w.inc = 2 => dmn = 2
       /\ w.re # 0 => wst[w.re] # 2

\* The primary takes a client write tagged with an interval at least the one
\* its term began in. Once the hello of incarnation 2 reached it, it refuses
\* every write of incarnation 1.
Send(i) ==
    LET w == Writes[i]
        p == ChunkPG[w.c]
        n == PrimNode(p, Cur(p))
    IN /\ wst[i] # 2
       /\ sends[i] < MaxSend
       /\ Pre(i)
       /\ n # 0
       /\ Running(n)
       /\ wst' = [wst EXCEPT ![i] = 1]
       /\ sends' = [sends EXCEPT ![i] = @ + 1]
       /\ IF /\ pr[n][p].iv # 0
             /\ Cur(p) >= pr[n][p].since
             /\ w.inc = 0 \/ w.inc >= known[n][p]
          THEN /\ pin' = [pin EXCEPT ![n] = @ \cup {[w |-> i, hold |-> 0]}]
               /\ known' = [known EXCEPT ![n][p] = Max({@, w.inc})]
          ELSE UNCHANGED <<pin, known>>
       /\ UNCHANGED <<ctlVars, nodeVars, pr, pv, dec, act, rm, vers, ackd, dmn, bad,
                      leaseVars, clockVars, raftVars>>

\* The first daemon dies; its successor starts.
Die ==
    /\ "die" \in plan.f
    /\ dmn = 1
    /\ dmn' = 2
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, rm, vers, ackd, wst, sends, bad,
                   leaseVars, clockVars, raftVars>>

\* Whether a held or fenced write must be refused when it is ordered.
Fenced(n, x) ==
    LET w == Writes[x.w]
        p == ChunkPG[w.c]
    IN \/ ~Bug("FenceOnArrivalOnly") /\ w.inc # 0 /\ w.inc < known[n][p]
       \/ ~Bug("ReorderAfterOtherPrimary") /\ x.hold # 0 /\ ~KeptPrimary(n, p, x.hold, IvAt(n, p))

\* The primary acknowledges a version to the client once every member live
\* in its interval logged it, if the process that ordered it still runs.
Acked(r, logs) ==
    /\ run[r.n] = r.r
    /\ Running(r.n)
    /\ \A d \in UpIn(ChunkPG[r.c], IvOfV(r.v)) : r.v \in logs[d][r.c]

Ack(r) ==
    /\ ackd' = ackd \cup {[c |-> r.c, v |-> r.v]}
    /\ wst' = [wst EXCEPT ![r.w] = 2]

\* The active primary orders a client write: the chunk's next version, merged
\* on the newest version it holds. A write covering part of a block waits
\* until the primary holds every version peering kept.
Order(n, x) ==
    LET w == Writes[x.w]
        c == w.c
        p == ChunkPG[c]
        k == IvAt(n, p)
        own == ddisk[n]
        cur == pv[n][c]
        v == IF IvOfV(cur) = k THEN cur + 1 ELSE Ver(k, 1)
        on == IF w.whole THEN cur ELSE Newest(dlog[own][c])
        r == [c |-> c, v |-> v, w |-> x.w, on |-> on, blk |-> w.blk, n |-> n, r |-> run[n]]
        logs == [dlog EXCEPT ![own][c] = @ \cup {v}]
    IN /\ Running(n)
       /\ x \in pin[n]
       /\ Leads(n, p)
       /\ pr[n][p].act
       /\ own = PrimDisk(p, k)
       /\ ~Fenced(n, x)
       /\ w.whole \/ miss[own][c] = NoMiss \/ Bug("MergeBeforeKept")
       /\ Cnt(v) <= MaxCounter
       /\ pin' = [pin EXCEPT ![n] = @ \ {x}]
       /\ pv' = [pv EXCEPT ![n][c] = v]
       /\ vers' = vers \cup {r}
       /\ dlog' = logs
       /\ rm' = rm \cup {[p |-> p, c |-> c, v |-> v, e |-> ep[n], iv |-> k, from |-> n, r |-> run[n], to |-> d] :
                          d \in UpIn(p, k) \ {own}}
       /\ IF Acked(r, logs) THEN Ack(r) ELSE UNCHANGED <<ackd, wst>>
       /\ UNCHANGED <<ctlVars, ep, dnode, ddisk, fmts, stale, run, rsIv, miss, persIv, mrecv,
                      pr, known, dec, act, sends, dmn, bad, leaseVars, clockVars, raftVars>>

\* The primary refuses a write it may not order; the client retries.
Refuse(n, x) ==
    /\ Running(n)
    /\ x \in pin[n]
    /\ Leads(n, ChunkPG[Writes[x.w].c])
    /\ pr[n][ChunkPG[Writes[x.w].c]].act
    /\ Fenced(n, x)
    /\ pin' = [pin EXCEPT ![n] = @ \ {x}]
    /\ UNCHANGED <<ctlVars, nodeVars, pr, pv, known, dec, act, dataVars,
                   leaseVars, clockVars, raftVars>>

\* A member accepts a write tagged with its PG's current interval.
Accepts(d, m) ==
    LET k == IvAt(dnode[d], m.p)
    IN IF Bug("CompareMemberLists") THEN UpSeq(m.p, k) = UpSeq(m.p, m.iv)
       ELSE IF Bug("CompareTableEpochs") THEN ep[dnode[d]] = m.e
       ELSE k = m.iv

\* A member logs a chunk's writes in the order its primary sent them,
\* skipping those it refuses; the last acknowledgment completes the write. A
\* delivered write stays in the network, so it may arrive again.
Deliver(m) ==
    LET d == m.to
        logs == [dlog EXCEPT ![d][m.c] = @ \cup {m.v}]
        r == CHOOSE x \in vers : x.c = m.c /\ x.v = m.v /\ x.n = m.from /\ x.r = m.r
    IN /\ dnode[d] # 0
       /\ Running(dnode[d])
       /\ m.v \notin dlog[d][m.c]
       /\ Accepts(d, m)
       /\ \/ Bug("LogOutOfOrder")
          \/ \A m2 \in rm : (/\ m2.to = d /\ m2.c = m.c /\ m2.from = m.from /\ m2.r = m.r
                              /\ m2.v < m.v /\ Accepts(d, m2))
                              => m2.v \in dlog[d][m.c]
       /\ dlog' = logs
       /\ IF [c |-> m.c, v |-> m.v] \notin ackd /\ Acked(r, logs) THEN Ack(r)
          ELSE UNCHANGED <<ackd, wst>>
       /\ bad' = IF IvAt(dnode[d], m.p) > m.iv THEN bad \cup {"late"} ELSE bad
       /\ UNCHANGED <<ctlVars, ep, dnode, ddisk, fmts, stale, run, rsIv, miss, persIv, mrecv, priVars,
                      rm, vers, sends, dmn, leaseVars, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* Leases with expiry left open (Clocks = FALSE). A grant sets the holder's
\* lease, and a lapse ends both, so a holder never outlives its grant.

CanGrant(g, h) ==
    /\ ~rw[g]
    /\ \/ ep[h] >= ep[g]
       \/ Bug("FreshGrantNoTableCheck") /\ ~GrantLive(g, h)

NewEp(g, h) ==
    IF GrantLive(g, h) /\ ~gr[g][h].fg THEN gr[g][h].ep
    ELSE IF Bug("NoEpochBump") /\ ~gr[g][h].fg THEN gr[g][h].ep
    ELSE gr[g][h].ep + 1

OpenGrant(g, h) ==
    /\ ~Clocks
    /\ Needs(h, g)
    /\ CanGrant(g, h)
    /\ ~(GrantLive(g, h) /\ hl[h][g].has /\ ~gr[g][h].fg)
    /\ gr' = [gr EXCEPT ![g][h] = [@ EXCEPT !.ep = NewEp(g, h), !.live = TRUE, !.fg = FALSE]]
    /\ hl' = [hl EXCEPT ![h][g] = [@ EXCEPT !.has = TRUE, !.ep = NewEp(g, h)]]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, rw, clockVars, raftVars>>

\* The restart wait outlasts every grant the node forgot, and the node
\* answers no peering during it, so with lease expiry left open a restart
\* lapses them at once (Restart). Without the wait they lapse on their own.
OpenLapse(g, h) ==
    /\ ~Clocks
    /\ gr[g][h].live
    /\ gr[g][h].fg
    /\ gr' = [gr EXCEPT ![g][h].live = FALSE]
    /\ hl' = [hl EXCEPT ![h][g].has = FALSE]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, rw, clockVars, raftVars>>

-----------------------------------------------------------------------------
\* Leases with two clocks per node (Clocks = TRUE). Messages on a pair whose
\* network is "ok" are delivered when sent, so time passes only when no
\* message or due request waits; on a "slow" pair they wait any time, and on
\* a "cut" pair they are lost.

PairUp(h, g) == net[h][g] # "cut"
PairOK(h, g) == net[h][g] = "ok" /\ Running(h) /\ Running(g)

AskDue(h, g) ==
    LET x == hl[h][g] IN
    /\ Needs(h, g)
    /\ ~rq[h][g].on
    /\ ~gm[g][h].on
    /\ lastAsk[h][g] < now
    /\ \/ ~x.w
       \/ x.B = 0 /\ raw[h] - x.w1r >= RateGap * 1000
       \/ x.B # 0 /\ ~LeaseOK(h, g)
       \/ now >= lastAsk[h][g] + HB

HbDue(g, h) == gr[g][h].on /\ ~gr[g][h].fg /\ now >= lastHb[g][h] + HB

WaitOver(n) ==
    /\ raw[n] - rstAt[n].r >= L1000
    /\ boot[n] - rstAt[n].b >= L1000

\* On an "ok" pair, a message is due as soon as its receiver runs, and a
\* request or heartbeat as soon as its sender does.
Urgent ==
    \E h, g \in Node :
       \/ net[h][g] = "ok" /\
          \/ rq[h][g].on /\ Running(g)
          \/ gm[g][h].on /\ Running(h)
          \/ hbm[g][h].on /\ Running(h)
          \/ AskDue(h, g) /\ Running(h)
          \/ HbDue(g, h) /\ Running(g)
       \/ rw[h] /\ Running(h) /\ WaitOver(h)

\* The bound on the grantor's raw rate over the holder's, from the window's
\* first grant and this one, per mille, rounded up.
Bound(x, s, gx) ==
    IF s - x.w1r <= 0 THEN x.B
    ELSE IF gx - x.w1g <= 0 THEN 1
    ELSE Ceil(1000 * (gx - x.w1g), s - x.w1r)

Margin(B) ==
    IF Bug("MarginExact") THEN Max({Drift, 1000 - 1000000 \div B})
    ELSE IF Bug("WidenPastTwoDrift") THEN Drift + Max({0, B - (1000 + 2 * Drift)})
    ELSE Drift + Max({0, B - (1000 + Drift \div 2)})

\* The step check: the grantor's raw clock moved further since the last grant
\* than the margin of the lease taken from it allows. A grant that gave no
\* lease, before the first bound, has nothing to check.
Trips(x, gx, r) ==
    /\ x.lg
    /\ x.a \/ Bug("CheckBeforeBound")
    /\ IF Bug("StepHalfDrift") THEN 1000 * (gx - x.ag) > (1000 + Drift \div 2) * (r - x.as)
       ELSE (gx - x.ag) * (1000 - x.am) > 1000 * (r - x.as)

\* A window restarts at a new run ID, which every process start and reboot
\* draws, or a raw reading below the last.
NewWindow(x, m) ==
    \/ ~x.w
    \/ m.rn # x.rn /\ ~Bug("NoRunWindow")
    \/ m.x < x.lastg

\* Ending every lease and window: on Resumed, or a step-check trip.
EndAll(h) == [hl EXCEPT ![h] = [g \in Node |-> [NoWin EXCEPT !.ep = hl[h][g].ep]]]

\* Whether grant or heartbeat m from g trips the holder's step check.
GrantTrips(h, g, m) ==
    ~Bug("NoStepCheck") /\ (hl[h][g].rn = m.rn \/ Bug("NoRunWindow")) /\ Trips(hl[h][g], m.x, raw[h])
HbTrips(h, g, m) == ~Bug("StepOnGrantsOnly") /\ GrantTrips(h, g, m)

\* A trip on a pair a fault touched ends the holder's other leases too, so
\* property 10 no longer holds for its other pairs.
Detected(h, g, trip) ==
    IF trip /\ dirty[h][g] THEN [dirty EXCEPT ![h] = [x \in Node |-> TRUE]] ELSE dirty

\* The holder's leases after grant m from g: the step check, then the window,
\* the bound, the margin and the lease, taken only once there is a bound.
AfterGrant(h, g, m) ==
    LET x == hl[h][g]
        r == raw[h]
        trip == GrantTrips(h, g, m)
        nw == NewWindow(x, m)
        B == IF nw THEN 0 ELSE Bound(x, m.s, m.x)
        mg == IF B = 0 THEN Drift ELSE Margin(B)
        lease == (B # 0 \/ Bug("LeaseBeforeBound")) /\ mg < MaxMargin
    IN IF trip THEN EndAll(h)
       ELSE [hl EXCEPT ![h][g] =
              [w |-> TRUE,
               w1s |-> IF nw THEN m.s ELSE x.w1s, w1r |-> IF nw THEN r ELSE x.w1r,
               w1g |-> IF nw THEN m.x ELSE x.w1g, B |-> B,
               lg |-> TRUE, a |-> lease, as |-> m.s, asB |-> m.sB, ag |-> m.x, am |-> mg,
               ep |-> m.ep, lastg |-> m.x, has |-> FALSE, rn |-> m.rn]]

\* The holder's leases after a heartbeat m from g, which carries g's raw clock
\* and run ID.
AfterHb(h, g, m) ==
    LET x == hl[h][g]
    IN IF HbTrips(h, g, m) THEN EndAll(h)
       ELSE IF NewWindow(x, m) /\ x.w
       THEN [hl EXCEPT ![h][g] = [@ EXCEPT !.w = FALSE, !.B = 0, !.lastg = m.x]]
       ELSE [hl EXCEPT ![h][g].lastg = Max({m.x, x.lastg})]

\* A grant the grantor gives now, answering a request sent at holder clock
\* readings s and sB.
GrantMsg(g, h, s, sB) == [on |-> TRUE, x |-> raw[g], rn |-> runId[g], ep |-> NewEp(g, h),
                          s |-> s, sB |-> sB]
Granted(g, h) == [gr EXCEPT ![g][h] = [ep |-> NewEp(g, h), live |-> FALSE, fg |-> FALSE, on |-> TRUE,
                                       tR |-> raw[g], tB |-> boot[g]]]

HbMsg(g) == [Off EXCEPT !.on = TRUE, !.x = raw[g], !.rn = runId[g]]

\* The round trips and heartbeats due at one instant on "ok" pairs all happen
\* before time passes; they run in the order of their pairs (holder, grantor).
Due(h, g) == PairOK(h, g) /\ (AskDue(h, g) \/ HbDue(g, h))
First(h, g) == \A h2, g2 \in Node : (h2 * 16 + g2 < h * 16 + g) => ~Due(h2, g2)

\* On an "ok" pair a request, its grant and its arrival take no time, so the
\* round trip is one step.
RoundTrip(h, g) ==
    /\ Clocks
    /\ PairOK(h, g)
    /\ AskDue(h, g)
    /\ First(h, g)
    /\ lastAsk' = [lastAsk EXCEPT ![h][g] = now]
    /\ hs' = IF hs[h][g] = -1 /\ ~dirty[h][g] THEN [hs EXCEPT ![h][g] = now] ELSE hs
    /\ IF CanGrant(g, h)
       THEN /\ gr' = Granted(g, h)
            /\ hl' = AfterGrant(h, g, GrantMsg(g, h, raw[h], boot[h]))
            /\ dirty' = Detected(h, g, GrantTrips(h, g, GrantMsg(g, h, raw[h], boot[h])))
            /\ lastHb' = [lastHb EXCEPT ![g][h] = now]
       ELSE /\ dirty' = [dirty EXCEPT ![h][g] = TRUE]
            /\ UNCHANGED <<gr, hl, lastHb>>
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, rw, now, raw, boot, rawR, bootR, slew,
                   runId, frz, suspAt, rstAt, rq, gm, hbm, net, arrived, faults, raftVars>>

\* On an "ok" pair a heartbeat arrives as it is sent.
Beat(g, h) ==
    /\ Clocks
    /\ PairOK(h, g)
    /\ HbDue(g, h)
    /\ First(h, g)
    /\ hl' = AfterHb(h, g, HbMsg(g))
    /\ dirty' = Detected(h, g, HbTrips(h, g, HbMsg(g)))
    /\ lastHb' = [lastHb EXCEPT ![g][h] = now]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, rw, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, rq, gm, hbm, lastAsk, net, arrived, hs,
                   faults, raftVars>>

\* On a "slow" pair, or to or from a stopped node, each message is its own
\* step and may wait any time.
Ask(h, g) ==
    /\ Clocks
    /\ Running(h)
    /\ PairUp(h, g)
    /\ ~PairOK(h, g)
    /\ AskDue(h, g)
    /\ rq' = [rq EXCEPT ![h][g] = [Off EXCEPT !.on = TRUE, !.s = raw[h], !.sB = boot[h]]]
    /\ lastAsk' = [lastAsk EXCEPT ![h][g] = now]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, gm, hbm, lastHb, net, arrived, dirty, hs, faults,
                   raftVars>>

\* The grantor grants once the holder reported a table at least as new as its
\* own; a grant after a lapse or a restart takes a new support epoch.
Grant(g, h) ==
    /\ Clocks
    /\ rq[h][g].on
    /\ Running(g)
    /\ PairUp(h, g)
    /\ CanGrant(g, h)
    /\ gr' = Granted(g, h)
    /\ gm' = [gm EXCEPT ![g][h] = GrantMsg(g, h, rq[h][g].s, rq[h][g].sB)]
    /\ rq' = [rq EXCEPT ![h][g] = Off]
    /\ lastHb' = [lastHb EXCEPT ![g][h] = now]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, hl, rw, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, hbm, lastAsk, net, arrived, dirty, hs, faults,
                   raftVars>>

GrantRefused(g, h) ==
    /\ Clocks
    /\ rq[h][g].on
    /\ Running(g)
    /\ ~CanGrant(g, h)
    /\ rq' = [rq EXCEPT ![h][g] = Off]
    /\ dirty' = [dirty EXCEPT ![h][g] = TRUE]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, gm, hbm, lastAsk, lastHb, net, arrived, hs,
                   faults, raftVars>>

SendHb(g, h) ==
    /\ Clocks
    /\ Running(g)
    /\ PairUp(h, g)
    /\ ~PairOK(h, g)
    /\ HbDue(g, h)
    /\ hbm' = [hbm EXCEPT ![g][h] = HbMsg(g)]
    /\ lastHb' = [lastHb EXCEPT ![g][h] = now]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, rq, gm, lastAsk, net, arrived, dirty, hs,
                   faults, raftVars>>

RecvGrant(h, g) ==
    /\ Clocks
    /\ gm[g][h].on
    /\ Running(h)
    /\ PairUp(h, g)
    /\ gm' = [gm EXCEPT ![g][h] = Off]
    /\ arrived' = [arrived EXCEPT ![h] = @ \ {g}]
    /\ hl' = AfterGrant(h, g, gm[g][h])
    /\ dirty' = Detected(h, g, GrantTrips(h, g, gm[g][h]))
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, rw, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, rq, hbm, lastAsk, lastHb, net, hs,
                   faults, raftVars>>

RecvHb(h, g) ==
    /\ Clocks
    /\ hbm[g][h].on
    /\ Running(h)
    /\ PairUp(h, g)
    /\ hbm' = [hbm EXCEPT ![g][h] = Off]
    /\ arrived' = [arrived EXCEPT ![h] = @ \ {g}]
    /\ hl' = AfterHb(h, g, hbm[g][h])
    /\ dirty' = Detected(h, g, HbTrips(h, g, hbm[g][h]))
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, rw, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, rq, gm, lastAsk, lastHb, net, hs,
                   faults, raftVars>>

EndWait(n) ==
    /\ Clocks
    /\ rw[n]
    /\ Running(n)
    /\ WaitOver(n)
    /\ rw' = [rw EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, hl, clockVars, raftVars>>

\* Real time advances to the next time something is due.
Deadlines ==
    {HB * (now \div HB + 1)}
    \cup {IF hl[h][g].a THEN HolderEnd(h, g) ELSE Infinity : h, g \in Node}
    \cup {IF gr[g][h].on THEN GrantEnd(g, h) ELSE Infinity : g, h \in Node}
    \cup {IF rw[n] THEN Max({Until(L1000 - (raw[n] - rstAt[n].r), RateRaw(n)),
                             Until(L1000 - (boot[n] - rstAt[n].b), RateBoot(n))})
          ELSE Infinity : n \in Node}
    \cup {IF hl[h][g].w /\ hl[h][g].B = 0
          THEN Until(RateGap * 1000 - (raw[h] - hl[h][g].w1r), RateRaw(h)) ELSE Infinity : h, g \in Node}
    \cup {IF lastAsk[h][g] >= 0 THEN lastAsk[h][g] + HB ELSE Infinity : h, g \in Node}
    \cup {IF lastHb[g][h] >= 0 THEN lastHb[g][h] + HB ELSE Infinity : g, h \in Node}



-----------------------------------------------------------------------------
\* Faults

\* The nodes faults hit: in a handover of PG 2, its primary (node 2) and its
\* grantor (node 3). In a Raft plan, partitions cut the first leader's links.
Target(n) == "stale2" \in plan.f => n \in {2, 3}

Fault(kind) == /\ FaultLeft(kind)
               /\ faults' = faults + 1

\* A node cut off from the control plane still reaches its peers but adopts
\* no new table: node 2 from the start with "stale2", or from any time with
\* "isolate2".
Isolate(n) ==
    /\ n = 2
    /\ "isolate2" \in plan.f
    /\ stale = {}
    /\ stale' = {n}
    /\ UNCHANGED <<ctlVars, ep, dnode, ddisk, fmts, run, rsIv, dlog, miss, persIv, mrecv, priVars,
                   dataVars, leaseVars, clockVars, raftVars>>

Taint(n) ==[h \in Node |-> [g \in Node |-> dirty[h][g] \/ h = n \/ g = n]]

\* A suspend stops the raw clock while CLOCK_BOOTTIME gains the sleep.
Suspend(n) ==
    /\ Clocks
    /\ Running(n)
    /\ Fault("susp")
    /\ frz' = [frz EXCEPT ![n] = "susp"]
    /\ suspAt' = [suspAt EXCEPT ![n] = boot[n]]
    /\ dirty' = Taint(n)
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, hs, raftVars>>

\* On resume CLOCK_BOOTTIME has gained the sleep, possibly up to 1 s short,
\* and the loop delivers Resumed, which ends every lease the node holds and
\* draws a new run ID.
Wake(n) ==
    /\ Clocks
    /\ frz[n] = "susp"
    /\ boot[n] - suspAt[n] > 1
    /\ \E cut \in {0, Min({boot[n] - suspAt[n] - 1, HB * 1000})} :
         boot' = [boot EXCEPT ![n] = @ - cut]
    /\ frz' = [frz EXCEPT ![n] = "run"]
    /\ hl' = IF Bug("NoEndOnResume") THEN hl ELSE EndAll(n)
    /\ runId' = [runId EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, rw, now, raw, rawR, bootR, slew,
                   suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty, hs,
                   faults, raftVars>>

\* A VM pause freezes every clock; "pause" changes the VMClock marker, "stop"
\* (a kvm-clock guest stopped in place) does not.
Freeze(n, kind) ==
    /\ Clocks
    /\ Running(n)
    /\ Fault(kind)
    /\ frz' = [frz EXCEPT ![n] = kind]
    /\ dirty' = Taint(n)
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, hs,
                   raftVars>>

\* A paused VM continues. After a marker change the loop delivers Resumed,
\* which ends the node's leases and draws a new run ID.
\* After a stop in place, the messages queued for it arrive ahead of any
\* request sent after them. The model leaves out a stop in place that no
\* queued grantor message spans (design.md, open question on kvm-clock stops).
Thaw(n) ==
    /\ Clocks
    /\ frz[n] \in {"pause", "stop"}
    /\ frz[n] = "stop" =>
         \A g \in Node : LeaseOK(n, g) => (hbm[g][n].on \/ gm[g][n].on)
    /\ frz' = [frz EXCEPT ![n] = "run"]
    /\ hl' = IF frz[n] = "pause" /\ ~Bug("NoEndOnResume") THEN EndAll(n) ELSE hl
    /\ runId' = IF frz[n] = "pause" THEN [runId EXCEPT ![n] = @ + 1] ELSE runId
    /\ arrived' = [arrived EXCEPT ![n] =
                     IF frz[n] = "stop" THEN {g \in Node : hbm[g][n].on \/ gm[g][n].on} ELSE {}]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, gr, rw, now, raw, boot, rawR, bootR,
                   slew, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, dirty, hs,
                   faults, raftVars>>

\* A grantor's raw rate rises by up to lease_drift / 2 after it was measured.
RateStep(n) ==
    /\ Clocks
    /\ Fault("step")
    /\ rawR' = [rawR EXCEPT ![n] = @ + Drift \div 2 - 1]
    /\ bootR' = [bootR EXCEPT ![n] = @ + Drift \div 2 - 1]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, slew,
                   runId, frz, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty,
                   hs, raftVars>>

NetFault(h, g, to) ==
    /\ Clocks
    /\ h < g
    /\ net[h][g] # to
    /\ IF to = "ok" THEN UNCHANGED faults ELSE Fault(to)
    /\ net' = [net EXCEPT ![h][g] = to, ![g][h] = to]
    /\ IF to = "cut"
       THEN /\ rq' = [rq EXCEPT ![h][g] = Off, ![g][h] = Off]
            /\ gm' = [gm EXCEPT ![h][g] = Off, ![g][h] = Off]
            /\ hbm' = [hbm EXCEPT ![h][g] = Off, ![g][h] = Off]
       ELSE UNCHANGED <<rq, gm, hbm>>
    /\ dirty' = [dirty EXCEPT ![h][g] = TRUE, ![g][h] = TRUE]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, lastAsk, lastHb, arrived, hs, raftVars>>

\* A process restart forgets everything but the disk, the table and the Raft
\* term, and draws a new run ID. A restarted primary asks for a new interval
\* of each PG it led.
RestartState(n) ==
    /\ run' = [run EXCEPT ![n] = @ + 1]
    /\ pr' = [pr EXCEPT ![n] = [p \in PG |->
               IF Bug("RestartSameInterval") /\ PrimNode(p, IvAt(n, p)) = n
               THEN [iv |-> IvAt(n, p), act |-> FALSE, srv |-> FALSE, since |-> IvAt(n, p)]
               ELSE NoPr]]
    /\ rsIv' = [rsIv EXCEPT ![n] = [p \in PG |->
                 IF PrimNode(p, IvAt(n, p)) = n THEN IvAt(n, p) ELSE rsIv[n][p]]]
    /\ reqIv' = [p \in PG |->
                  IF PrimNode(p, IvAt(n, p)) = n /\ IvAt(n, p) = Cur(p) /\ ~Bug("RestartSameInterval")
                  THEN Cur(p) ELSE reqIv[p]]
    /\ pin' = [pin EXCEPT ![n] = {}]
    /\ known' = [known EXCEPT ![n] = [p \in PG |-> 0]]
    /\ mrecv' = [mrecv EXCEPT ![ddisk[n]] = {}]
    /\ rw' = [rw EXCEPT ![n] = Clocks /\ ~Bug("NoRestartWait")]
    /\ rlead' = [rlead EXCEPT ![n] = FALSE]
    /\ fort' = [fort EXCEPT ![n] = NoFort]
    /\ fby' = [fby EXCEPT ![n] = [v \in Node |-> -1]]
    /\ xfer' = [xfer EXCEPT ![n] = FALSE]
    /\ runId' = [runId EXCEPT ![n] = @ + 1]

Restart(n) ==
    /\ Fault("restart")
    /\ Running(n)
    /\ run[n] < MaxRestart
    /\ RestartState(n)
    /\ LET waited == ~Clocks /\ ~Bug("NoRestartWait") IN
       /\ gr' = [gr EXCEPT ![n] = [h \in Node |->
                  [gr[n][h] EXCEPT !.fg = TRUE, !.live = @ /\ ~waited]]]
       /\ hl' = [h \in Node |->
                  IF h = n THEN [g \in Node |-> NoWin]
                  ELSE [hl[h] EXCEPT ![n].has = @ /\ ~waited]]
    /\ rstAt' = [rstAt EXCEPT ![n] = [r |-> raw[n], b |-> boot[n]]]
    /\ dirty' = Taint(n)
    /\ UNCHANGED <<tbl, ivs, plan, step, reports, ep, dnode, ddisk, fmts, stale, dlog, miss, persIv, pv, dec, act,
                   dataVars, now, raw, boot, rawR, bootR, slew, frz, suspAt, rq, gm, hbm,
                   lastAsk, lastHb, net, arrived, hs, rterm, xto>>

\* A reboot restarts the process and both clocks near 0, with a raw rate 2%
\* off its old one. The node is down for at least a
\* heartbeat first (BootUp): a reboot takes seconds, which the restart wait
\* relies on, since the new boot's clocks may run faster than the holders
\* measured the old one.
Reboot(n) ==
    /\ Clocks
    /\ Running(n)
    /\ run[n] < MaxRestart
    /\ Fault("reboot")
    /\ RestartState(n)
    /\ gr' = [gr EXCEPT ![n] = [h \in Node |-> [gr[n][h] EXCEPT !.fg = TRUE, !.on = FALSE]]]
    /\ hl' = [hl EXCEPT ![n] = [g \in Node |-> NoWin]]
    /\ \E dr \in {-20, 20} :
         /\ rawR' = [rawR EXCEPT ![n] = @ + dr]
         /\ bootR' = [bootR EXCEPT ![n] = ((rawR[n] + dr) * (1000 + slew[n])) \div 1000]
    /\ raw' = [raw EXCEPT ![n] = 0]
    /\ boot' = [boot EXCEPT ![n] = 0]
    /\ rstAt' = [rstAt EXCEPT ![n] = [r |-> 0, b |-> 0]]
    /\ dirty' = Taint(n)
    /\ frz' = [frz EXCEPT ![n] = "boot"]
    /\ suspAt' = [suspAt EXCEPT ![n] = now]
    /\ UNCHANGED <<tbl, ivs, plan, step, reports, ep, dnode, ddisk, fmts, stale, dlog, miss, persIv, pv, dec, act,
                   dataVars, now, slew, rq, gm, hbm, lastAsk, lastHb, net, arrived,
                   hs, rterm, xto>>

BootUp(n) ==
    /\ frz[n] = "boot"
    /\ now >= suspAt[n] + HB \/ Bug("InstantReboot")
    /\ frz' = [frz EXCEPT ![n] = "run"]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty, hs,
                   faults, raftVars>>

\* A disk formatted again draws a new log ID, which the control plane treats
\* as a new, empty disk; the old disk ID stays down.
Format(n) ==
    /\ Fault("format")
    /\ Running(n)
    /\ fmts < MaxFormat
    /\ fmts' = fmts + 1
    /\ IF Bug("KeepDiskId")
       THEN LET d == ddisk[n] IN
            /\ dlog' = [dlog EXCEPT ![d] = [c \in Chunk |-> {}]]
            /\ miss' = [miss EXCEPT ![d] = [c \in Chunk |-> NoMiss]]
            /\ persIv' = [persIv EXCEPT ![d] = [p \in PG |-> 0]]
            /\ mrecv' = [mrecv EXCEPT ![d] = {}]
            /\ UNCHANGED <<dnode, ddisk>>
       ELSE LET x == Cardinality(Node) + fmts + 1 IN
            /\ dnode' = [dnode EXCEPT ![x] = n, ![ddisk[n]] = 0]
            /\ ddisk' = [ddisk EXCEPT ![n] = x]
            /\ UNCHANGED <<dlog, miss, persIv, mrecv>>
    /\ UNCHANGED <<ctlVars, ep, stale, run, rsIv, priVars, dataVars, leaseVars, now, raw, boot, rawR, bootR,
                   slew, runId, frz, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived,
                   dirty, hs, raftVars>>

-----------------------------------------------------------------------------
\* Raft leader fortification on the node-pair leases

\* A follower supports a leader while its node's grant to the leader's node
\* holds in the support epoch it recorded.
Supports(v, c) ==
    /\ fort[v].l \notin {0, c, v}
    /\ GrantLive(v, fort[v].l)
    /\ gr[v][fort[v].l].ep = fort[v].ep

\* A candidate wins a quorum's votes; a voter supporting another leader
\* refuses, unless the candidate is a transfer target. With lease expiry left
\* open, such a voter instead waits out its grant, which lapses.
Elect(n, Q) ==
    LET t == Max({rterm[v] : v \in Q}) + 1
        lapse(g, h) == ~Clocks /\ xto # n /\ g \in Q /\ Supports(g, n) /\ fort[g].l = h
    IN
    /\ Raft
    /\ n \in Q
    /\ Cardinality(Q) = Quorum
    /\ ~rlead[n]
    /\ t <= MaxTerm
    /\ \A v \in Q : Running(v) /\ ~rw[v] /\ (~Supports(v, n) \/ xto = n \/ ~Clocks)
    /\ gr' = [g \in Node |-> [h \in Node |->
               IF lapse(g, h) THEN [gr[g][h] EXCEPT !.live = FALSE] ELSE gr[g][h]]]
    /\ hl' = [h \in Node |-> [g \in Node |->
               IF lapse(g, h) THEN [hl[h][g] EXCEPT !.has = FALSE] ELSE hl[h][g]]]
    /\ rterm' = [v \in Node |-> IF v \in Q THEN t ELSE rterm[v]]
    /\ rlead' = [v \in Node |-> IF v = n THEN TRUE ELSE IF v \in Q THEN FALSE ELSE rlead[v]]
    /\ fort' = [v \in Node |-> IF v \in Q THEN NoFort ELSE fort[v]]
    /\ fby' = [fby EXCEPT ![n] = [v \in Node |-> -1]]
    /\ xfer' = [xfer EXCEPT ![n] = FALSE]
    /\ xto' = IF xto = n THEN 0 ELSE xto
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, rw, clockVars>>

\* A follower fortifies the leader under its node's grant to the leader's node.
Fortify(n, v) ==
    /\ Raft
    /\ rlead[n]
    /\ v # n
    /\ Running(n) /\ Running(v)
    /\ rterm[v] <= rterm[n]
    /\ GrantLive(v, n)
    /\ ~Supports(v, n)
    /\ fort[v] # [l |-> n, ep |-> gr[v][n].ep]
    /\ fort' = [fort EXCEPT ![v] = [l |-> n, ep |-> gr[v][n].ep]]
    /\ fby' = [fby EXCEPT ![n][v] = gr[v][n].ep]
    /\ rterm' = [rterm EXCEPT ![v] = rterm[n]]
    /\ rlead' = [rlead EXCEPT ![v] = FALSE]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, clockVars, xfer, xto>>

StepDown(n) ==
    /\ Raft
    /\ rlead[n]
    /\ \E v \in Node : rterm[v] > rterm[n]
    /\ rlead' = [rlead EXCEPT ![n] = FALSE]
    /\ rterm' = [rterm EXCEPT ![n] = Max({rterm[v] : v \in Node})]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, clockVars, fort, fby, xfer, xto>>

\* Leader transfer: the leader sends MsgTimeoutNow and ends its own lease.
Transfer(n, t) ==
    /\ "transfer" \in plan.f
    /\ rlead[n]
    /\ t # n
    /\ xto = 0
    /\ Running(n)
    /\ xfer' = [xfer EXCEPT ![n] = TRUE]
    /\ xto' = t
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, clockVars, rterm, rlead, fort, fby>>

-----------------------------------------------------------------------------

\* Steps that follow from the state: with two clocks per node they happen
\* before time passes, so that time multiplies only the choices below.
Protocol ==
    \/ Reactive
    \/ \E n \in Node \ stale : \E e \in {ep[n] + 1, Len(tbl)} : Adopt(n, e)
    \/ \E n \in Node, p \in PG : Peer(n, p) \/ Activate(n, p) \/ CounterOut(n, p)
    \/ \E d \in Disk : Persist(d)
    \/ \E n \in Node, p \in PG, c \in Chunk, d \in Disk : Recover(n, p, c, d)
    \/ \E i \in 1..Len(Writes) : Send(i)
    \/ \E n \in Node : \E x \in pin[n] : Order(n, x) \/ Refuse(n, x)
    \/ \E m \in rm : Deliver(m)
    \/ \E n, v \in Node : ~rlead[v] /\ Fortify(n, v)
    \/ \E n \in Node : BootUp(n)

\* Choices: membership changes, faults, elections and transfers, and an old
\* leader hearing of a newer term, which a message may bring late. With two
\* clocks per node they come on heartbeat boundaries: what a lease overlap
\* turns on is the last renewal before them, which keeps its own timing.
Choices ==
    \/ Membership
    \/ Die
    \/ \E n \in Node : Isolate(n)
    \/ \E g, h \in Node : OpenGrant(g, h) \/ OpenLapse(g, h)
    \/ \E n \in Node : Target(n) /\
         \/ Suspend(n) \/ Wake(n) \/ Freeze(n, "pause") \/ Freeze(n, "stop") \/ Thaw(n)
         \/ RateStep(n) \/ Restart(n) \/ Reboot(n) \/ Format(n)
    \/ \E h, g \in Node, to \in {"ok", "slow", "cut"} :
         to \in plan.f \cup (IF "heal" \in plan.f THEN {"ok"} ELSE {})
         /\ Target(h) /\ Target(g) /\ (Raft => 1 \in {h, g})
         /\ NetFault(h, g, to)
    \/ \E n \in Node, Q \in SUBSET Node : Elect(n, Q)
    \/ \E n, v \in Node : Transfer(n, v)
    \/ \E n \in Node : StepDown(n)
    \/ \E n, v \in Node : rlead[v] /\ Fortify(n, v)

Choice ==
    /\ ~Clocks \/ now % HB = 0
    /\ Choices

LeaseSteps ==
    \/ \E h, g \in Node : Ask(h, g) \/ Grant(g, h) \/ GrantRefused(g, h) \/ SendHb(g, h)
                          \/ RecvGrant(h, g) \/ RecvHb(h, g) \/ RoundTrip(h, g) \/ Beat(h, g)
    \/ \E n \in Node : EndWait(n)

\* Real time advances to the next time something is due, once nothing waits
\* to happen now.
Tick ==
    LET t == Min({x \in Deadlines : x > now} \cup {Infinity}) IN
    /\ Clocks
    /\ ~Urgent
    /\ ~ENABLED Protocol
    /\ t <= Min({MaxTime, plan.t})
    /\ now' = t
    /\ raw' = [n \in Node |-> raw[n] + RateRaw(n) * (t - now)]
    /\ boot' = [n \in Node |-> boot[n] + RateBoot(n) * (t - now)]
    /\ UNCHANGED <<ctlVars, nodeVars, priVars, dataVars, leaseVars, rawR, bootR, slew,
                   runId, frz, suspAt, rstAt, rq, gm, hbm, lastAsk, lastHb, net, arrived, dirty, hs,
                   faults, stale, raftVars>>

Next == Protocol \/ Choice \/ LeaseSteps \/ Tick

Spec == Init /\ [][Next]_vars

\* The unbounded liveness property 12 assumes every step that follows from the
\* state is eventually taken, and grants a restart forgot eventually lapse.
FairSpec == Spec /\ WF_vars(Protocol) /\ WF_vars(\E g, h \in Node : OpenLapse(g, h))

-----------------------------------------------------------------------------
\* Properties

\* 1. No PG serves reads from two primaries at once: while a primary serves a
\* PG's reads, no later interval of that PG has activated under another primary.
OneReader ==
    \A p \in PG, n \in Node :
       Reading(n, p) => \A x \in act : (x.p = p /\ x.k > IvAt(n, p)) => PrimNode(p, x.k) = n

\* 2. No two replicas of a shard serve lease reads at once.
OneLeaseholder ==
    \A n, m \in Node : n # m => ~(RaftReads(n) /\ RaftReads(m))

\* 3. A member that adopted a later interval of a PG logs no write tagged
\* with an earlier one, so the members peering reached stop the intervals
\* they were live in.
NoLateCompletion == "late" \notin bad

\* 4. No acknowledged version, and no version an activated interval kept, is
\* ever dropped: every later decision and acknowledged version rests on it.
\* That includes a write tagged with an interval that completes after a later
\* one activated, which peering must have kept.
NothingCommittedDropped ==
    \A c \in Chunk : \A u \in CommitAt(c) :
       /\ \A x \in ActDecs : (x.p = ChunkPG[c] /\ x.k > u.at) => u.v \in Chain(c, x.kept[c])
       /\ \A w \in AckedV(c) : w > u.v => u.v \in Chain(c, w)

\* 5. The versions peering drops are a suffix of each chunk's pending
\* versions: every member peering counts as holding a kept version holds the
\* bytes of every version it was merged on.
DropsAreSuffix == "torn" \notin bad

\* 6. No two writes of a chunk get one version, and each version is above the
\* version it was merged on.
OneWritePerVersion ==
    \A x, y \in vers : (x.c = y.c /\ x.v = y.v) => x.w = y.w /\ x.on = y.on
VersionsGrow == \A x \in vers : x.on < x.v

\* 7. A table that leaves a PG's interval alone never fails that PG's writes:
\* a write tagged with table epoch e is accepted by a member holding any table
\* in which the PG's interval is still the one it had in e.
UnchangedIntervalAccepts ==
    \A m \in rm : dnode[m.to] > 0 /\ IvAt(dnode[m.to], m.p) = m.iv => Accepts(m.to, m)

\* 8. A new primary merges into a chunk only once it holds every version
\* peering kept for it: the version it merges on rests on the kept one.
MergesOnKept ==
    \A x \in vers : ~Writes[x.w].whole =>
       \A y \in dec : (y.p = ChunkPG[x.c] /\ y.k = IvOfV(x.v) /\ y.n = x.n /\ y.r = x.r)
                        => y.kept[x.c] = 0 \/ y.kept[x.c] \in Chain(x.c, x.on)

\* 9. Table epochs, interval numbers and last activated intervals never go
\* backwards (the node half is the action property Monotonic), and a table's
\* last activated interval of a PG did activate.
TablesForward ==
    \A e \in 1..Len(tbl), p \in PG :
       /\ e < Len(tbl) => tbl[e + 1][p].iv >= tbl[e][p].iv /\ tbl[e + 1][p].la >= tbl[e][p].la
       /\ \E x \in act : x.p = p /\ x.k = tbl[e][p].la
Monotonic == [][\A n \in Node : ep'[n] >= ep[n]]_vars

\* 10. A pair whose raw rates differ by up to 4%, with no pause, reboot,
\* network fault or refusal since first contact, and no detection by its
\* holder on a pair a fault touched, holds a lease within rate_gap (plus
\* rounding, at zero round trips) of first contact and keeps it with no
\* step-check trip.
LeaseForms ==
    Clocks /\ ~Urgent =>
      \A h, g \in Node :
        (/\ Needs(h, g)
         /\ hs[h][g] >= 0
         /\ ~dirty[h][g]
         /\ 1000 * rawR[g] <= 1040 * rawR[h]
         /\ 1000 * rawR[h] <= 1040 * rawR[g]
         /\ now >= hs[h][g] + RateGap + 2)
        => LeaseOK(h, g)

\* 11. Once a dead daemon's reissued write and a later write to the same block
\* are acknowledged, the block never again holds the dead daemon's write.
NoResurrectedWrite ==
    \A r \in 1..Len(Writes) : \A j \in 1..Len(Writes) :
       (Writes[r].re # 0 /\ Writes[j].after = r /\ wst[r] = 2 /\ wst[j] = 2) =>
         LET c == Writes[r].c
             b == Writes[r].blk
             top == Newest({u.v : u \in {x \in CommitAt(c) : BlkOf(c, x.v) = b}})
         IN top # 0 => WOf(c, top) # Writes[r].re

\* 12. Peering settles: every PG eventually activates its current interval,
\* unless the interval has fewer than min_live live members, a live member's
\* disk or every live member of an interval peering needs is gone, or the
\* table bound left a restarted primary without the interval it asked for.
Settled(p) ==
    LET k == Cur(p)
        n == PrimNode(p, k)
    IN \/ \E x \in act : x.p = p /\ x.k = k
       \/ LiveCnt(p, k) < MinLive[p]
       \/ n = 0
       \/ \E d \in UpIn(p, k) : dnode[d] = 0
       \/ \E i \in NeedIv(p, k, tbl[Len(tbl)][p].la) : \A d \in UpIn(p, i) : dnode[d] = 0
       \/ rsIv[n][p] >= k
PeeringSettles == <>[](\A p \in PG : Settled(p))

-----------------------------------------------------------------------------
\* Configurations: the .cfg files substitute these for the constants. PG 1 is
\* in a rep3 pool with min_live 2; PG 2 in a rep2 pool with min_live 2 that
\* allows maintenance.

PGs == {1, 2}
MinLive2 == [p \in PGs |-> 2]
MaintPG2 == [p \in PGs |-> p = 2]
Mem3 == [p \in PGs |-> IF p = 1 THEN <<1, 2, 3>> ELSE <<2, 3>>]
Mem4 == [p \in PGs |-> IF p = 1 THEN <<1, 2, 3>> ELSE <<2, 4>>]
Nodes3 == {1, 2, 3}
Nodes4 == {1, 2, 3, 4}
Exact == [n \in Nodes4 |-> {1000}]
NoSlew == [n \in Nodes4 |-> {0}]

\* Chunk 1 is in PG 1, chunk 2 in PG 2, each with two blocks.
Chunks2 == {1, 2}
ChunkInPG == [c \in Chunks2 |-> c]
Blocks22 == [c \in Chunks2 |-> 2]
Chunks1 == {1}
ChunkInPG1 == [c \in Chunks1 |-> 1]
Blocks1 == [c \in Chunks1 |-> 1]

\* Two writes covering part of each of chunk 1's blocks.
Partial ==
    <<[c |-> 1, blk |-> 1, whole |-> FALSE, inc |-> 0, re |-> 0, after |-> 0],
      [c |-> 1, blk |-> 2, whole |-> FALSE, inc |-> 0, re |-> 0, after |-> 0]>>
\* The same to chunk 2.
Partial2 == [i \in 1..2 |-> [Partial[i] EXCEPT !.c = 2]]
\* To chunk 2's first block: a daemon's write W (incarnation 1), its successor's
\* reissue W' once it died, and the successor's next write W2 once W' is
\* acknowledged.
Session ==
    <<[c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 1, re |-> 0, after |-> 0],
      [c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 2, re |-> 1, after |-> 0],
      [c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 2, re |-> 0, after |-> 2]>>
\* Both, for deep.
Both ==
    Partial \o
    <<[c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 1, re |-> 0, after |-> 0],
      [c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 2, re |-> 3, after |-> 0],
      [c |-> 2, blk |-> 1, whole |-> TRUE, inc |-> 2, re |-> 0, after |-> 4]>>
\* One whole-block write to chunk 1, for ci-leases.
OneWhole == <<[c |-> 1, blk |-> 1, whole |-> TRUE, inc |-> 0, re |-> 0, after |-> 0]>>

Mark(d, to) == [k |-> "mark", d |-> d, to |-> to]
Repl(p, old, new) == [k |-> "replace", p |-> p, old |-> old, new |-> new]
Rot(p) == [k |-> "rotate", p |-> p]
Plan(m, w, f) == [free |-> FALSE, m |-> m, w |-> w, f |-> f, t |-> 600, k |-> 1, s |-> Spare]

\* The membership faults of test-scenarios.md "Epoch change model" on 3 nodes,
\* each with the writes whose rules it tests.
\* A member removed and added back; PG 2 is below min_live in between.
RemovedAndBack == Plan(<<Mark(3, "down"), Mark(3, "up")>>, Partial, {})
\* PG 1's primary down and back: a member live in the last activated interval
\* that its peering never reached returns.
PrimaryBack == Plan(<<Mark(1, "down"), Mark(1, "up")>>, Partial, {})
\* A member down and back, then the primary moved to it.
DownBackMoved == Plan(<<Mark(2, "down"), Mark(2, "up"), Rot(1)>>, Partial, {})
\* A disk formatted again, replaced by its new disk.
Reformatted == Plan(<<Repl(1, 3, 4)>>, Partial, {"format"})
\* PG 2 short of min_live only by a member in maintenance; its primary stays.
MaintKeepsPrimary == Plan(<<Mark(3, "maint"), Mark(3, "up")>>, Session, {"die"})
\* PG 2's primary in maintenance: the primary moves to 3 and back.
PrimaryAway == Plan(<<Mark(2, "maint"), Mark(2, "up")>>, Session, {"die"})
\* A replica replaced under a primary that stays, then the primary replaced.
ReplicaThenPrimary == Plan(<<Repl(2, 3, 1), Repl(2, 2, 3)>>, Session, {})
\* The primary replaced, then the other member in maintenance.
PrimaryThenMaint == Plan(<<Repl(2, 2, 1), Mark(3, "maint")>>, Session, {})
\* A replica replaced, then the primary in maintenance.
ReplicaThenMaint == Plan(<<Repl(2, 3, 1), Mark(2, "maint")>>, Session, {"restart"})
\* A primary restart with no membership change, and the daemon's successor.
Restarted == Plan(<<>>, Session, {"restart", "die"})
\* PG 2 below min_live while its primary stays, then the primary moved away.
BelowMinLive == Plan(<<Mark(3, "down"), Mark(3, "up"), Mark(2, "maint")>>, Session, {})
\* The same with maintenance instead of down, and partial writes: the new
\* primary merges only once recovery brought it every kept version.
MaintMerges == Plan(<<Mark(3, "maint"), Mark(3, "up"), Mark(2, "maint")>>, Partial2, {})
\* Two activations and two spare tables, so a report can be folded late.
LateReport == [Plan(<<Mark(3, "down"), Mark(3, "up")>>, <<>>, {}) EXCEPT !.s = 2]

\* ci-peering checks the plans with membership changes alone; ci-restarts
\* those with a restart or a format, under which a decision can stop short of
\* activation (CanStall), and, to keep ci-peering within 5 minutes, the
\* maintenance plan with merges.
PeerPlans ==
    {RemovedAndBack, PrimaryBack, DownBackMoved, MaintKeepsPrimary, PrimaryAway,
     ReplicaThenPrimary, PrimaryThenMaint, BelowMinLive, LateReport}
RestartPlans == {Reformatted, ReplicaThenMaint, Restarted, MaintMerges}

\* ci-leases: PG 2's primary is node 2, whose CLOCK_BOOTTIME runs at its raw
\* rate or 22.6% slow; node 3, its only grantor, runs 4% slow or fast, with
\* CLOCK_BOOTTIME at that rate or 22.6% fast; node 1 is exact. A plan with
\* "fast" runs node 3 4% fast and the rest exact, one with "exact" every clock
\* exact (PlanRates).
LeaseRates == [n \in Nodes4 |-> IF n = 3 THEN {960, 1040} ELSE {1000}]
LeaseSlews == [n \in Nodes4 |-> CASE n = 2 -> {0, -226} [] n = 3 -> {0, 226} [] OTHER -> {0}]

\* Leases form and hold with no change of membership.
Steady == Plan(<<>>, OneWhole, {})
\* PG 2's primary, node 2, is put in maintenance while it still runs, cut off
\* from the control plane: its grantor, node 3, refuses to renew it, waits out
\* its grant and peers the new interval as primary.
Handover == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2"})
\* The handover with node 3 fast, then with each fault: its raw rate rising
\* by 0.4% after it was measured, and a partition;
HandoverStep == [Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "step", "cut", "fast"}) EXCEPT !.k = 2]
\* a suspend, or a VM pause that changes the VMClock marker;
HandoverSuspend == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "susp", "pause", "fast"})
\* a kvm-clock guest stopped in place, with no partition (design.md, open
\* question on kvm-clock stops);
HandoverStop == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "stop", "fast"})
\* a process restart;
HandoverRestart == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "restart", "fast"})
\* a VM pause and a partition, so that a holder's rate window may span the
\* grantor's pause;
HandoverPause == [Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "pause", "cut", "fast"}) EXCEPT !.t = 500, !.k = 2]
\* a suspend and a partition;
HandoverSuspendCut == [Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "susp", "cut", "fast"}) EXCEPT !.t = 500, !.k = 2]
\* a reboot within rate_window, the new boot 2% faster or slower, and a
\* partition that may heal.
HandoverRebootFast == [Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "reboot", "cut", "heal", "fast"}) EXCEPT !.t = 1100, !.k = 2]
\* The handover with exact clocks and the same reboot;
HandoverReboot == [Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "reboot", "cut", "heal", "exact"}) EXCEPT !.t = 1100, !.k = 2]
\* a partition that may heal;
HandoverCut == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "cut", "heal", "exact"})
\* messages delayed any time.
HandoverSlow == Plan(<<Mark(2, "maint")>>, OneWhole, {"stale2", "slow", "exact"})
\* Interval 2 of PG 2 replaces 3 with 1 under primary 2, which serves reads
\* under 1's grant; interval 3 puts 1 in maintenance; interval 4 moves the
\* primary to 3, which was live in interval 1 but not in 2 or 3.
Maintenance == Plan(<<Repl(2, 3, 1), Mark(1, "maint"), Repl(2, 2, 3)>>, <<>>, {"isolate2", "exact"})
\* The Raft group, node 1 its leader in term 1 at the start and node 3 fast,
\* with leader transfer;
RaftTransfer == [Plan(<<>>, <<>>, {"raft", "transfer", "fast"}) EXCEPT !.t = 300]
\* with partitions that heal.
RaftPartition == [Plan(<<>>, <<>>, {"raft", "cut", "heal", "fast"}) EXCEPT !.t = 500, !.k = 2]

\* ci-leases checks LeasePlans. deep-leases adds the plans that pair a
\* partition with a pause, a suspend or a reboot, at 1.6 to 7.6 million states
\* each on 3 nodes; the negative variants that need one run it alone. The
\* plan with delayed messages, at over 3 million states, runs alone in
\* deep-slow at 3 nodes, since with the others at 4 nodes it passed 108 million
\* states in 4 hours.
LeasePlans == {Steady, Handover, HandoverStep, HandoverSuspend, HandoverStop,
               HandoverRestart, HandoverCut, Maintenance, RaftTransfer, RaftPartition}
RebootPlans == {HandoverReboot}
PausePlans == {HandoverPause}
AllLeasePlans == LeasePlans \cup RebootPlans \cup PausePlans \cup {HandoverSuspendCut, HandoverRebootFast}
SlowPlans == {HandoverSlow}

\* deep-peering: every peering plan; deep-free: any two changes at any time
\* with one partial write to chunk 1 and a restart, about 10 million states
\* at 4 nodes, so with liveness each fits 4 hours only alone. Node 4 starts
\* outside both PGs, a spare a change can move a member to.
Free == [free |-> TRUE, m |-> <<>>, w |-> SubSeq(Partial, 1, 1), f |-> {"restart"}, t |-> 0, k |-> 1, s |-> 0, n |-> 3]
DeepPeerPlans == PeerPlans \cup RestartPlans
FreePlans == {Free}

=============================================================================
