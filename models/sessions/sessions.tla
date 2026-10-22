------------------------------- MODULE sessions -------------------------------
\* Sessions and fencing (docs/design.md): client sessions renewed on heartbeats to the
\* control-plane leader and expired on its clock, leader leases and failover, the client's
\* rate bound per leader node and boot and its step check, the leader's measurement of its
\* own rate over each client's, fences batched into session table versions that storage
\* nodes apply and acknowledge, the acknowledged version that releases owners, down marks
\* that start new intervals of a placement group (PG), writes through the PG's primary to
\* its members, and a lock an inode owner grants.
\*
\* Time advances in ticks. Each node's raw clock runs at a whole number of units per tick
\* (its rate), fixed per boot except for one rise within the lease margin's headroom; the
\* model carries this one clock per node, the rate bound the design's two clocks give a
\* lease (the epoch change model checks the two-clock rule itself). A client's virtual
\* machine can stop with its clock frozen. Durations are kept as units since an event and
\* saturate at Cap, the leader's count at its largest threshold. Each rule the design
\* states is a BOOLEAN constant: the ci and deep configurations set every one TRUE, and
\* each negative configuration turns one off.

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Clients,        \* client daemons, each with one session
    Leaders,        \* control-plane nodes that can lead
    Storage,        \* storage nodes, numbered: the PG's members in order
    Rates,          \* the raw clock rates a node can boot with, in units per tick
    SessionLease,   \* session_lease, in units
    DriftNum, DriftDen,  \* lease_drift = DriftNum / DriftDen of the lease
    Cap,            \* durations saturate here; it exceeds every lease
    DeadWait,       \* a leader's wait for a session it has not measured, in its units
    DownAfter,      \* node_down_after, in ticks
    Windows,        \* renewals are messages and clients measure leaders' rates (FALSE: one step)
    WithLock,       \* clients contend for one lock
    MaxTerms, MaxReboots, MaxRises, MaxStops, MaxStalls, MaxEvicts, MaxCrashes,
    MaxPartitions, MaxWrites, MaxInFlight, MaxLost

CONSTANTS
    CountFromSend,          \* the client counts its lease from the renewal's send
    EndDriftEarly,          \* the client ends its lease lease_drift early
    EndOnResumed,           \* the client ends its leases on Resumed
    AnswerWithLease,        \* only a leader holding its leader lease answers renewals
    FullLeaseOnFailover,    \* a new leader gives every session a full lease from its start
    WindowPerBoot,          \* the client keeps its rate bound per leader node and boot
    StepAtMargin,           \* the step check trips at 1 / (1 - m), not at 1 + lease_drift / 2
    EvictWaitsLease,        \* an eviction stops renewals and fences once the lease ran out
    ReleaseOnAck,           \* owners release a fenced session's lock at its acknowledgment
    CheckWhenOrdered,       \* a primary checks the fence when it orders a write, not on arrival
    ReplicaChecks,          \* a replica checks the fence on replicated writes
    UpNeedsCurrent,         \* a node is marked up only once it holds the current version
    PeeringStops,           \* a new interval activates only once its earlier ones are stopped
    AckNeedsEveryNode,      \* the acknowledged version waits for every node not marked down
    DownCountsAsAck,        \* a node marked down counts as acknowledged
    CountOnHolder           \* a leader stretches a session's lease by its measured rate over
                            \* the client's, and waits DeadWait for one it has not measured

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
Sat(x) == Min(x, Cap)
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y
None == <<>>
MaxIntv == 1 + 2 * MaxPartitions
Intervals == 1 .. MaxIntv

\* The margin m of a lease taken under a bound B = bg / bh on the leader's rate over the
\* client's: lease_drift plus B's excess over 1 + lease_drift / 2. One minus it is
\* Keep(bg, bh)[1] / Keep(bg, bh)[2]; at 1/2 or less the client takes no lease.
Keep(bg, bh) ==
    LET excess == Max(0, bg * 2 * DriftDen - bh * (2 * DriftDen + DriftNum))
        base == IF EndDriftEarly THEN DriftNum ELSE 0
    IN <<2 * bh * (DriftDen - base) - excess, 2 * DriftDen * bh>>
LeaseLen(B) == LET k == Keep(B[1], B[2]) IN IF 2 * k[1] <= k[2] THEN 0 ELSE (SessionLease * k[1]) \div k[2]
\* The step check: the leader's clock moved g units while the client's moved h since the
\* last grant; it trips past 1 / (1 - m), or past a fixed 1 + lease_drift / 2.
Trips(g, h, B) ==
    IF StepAtMargin THEN LET k == Keep(B[1], B[2]) IN g * k[1] > h * k[2]
    ELSE g * 2 * DriftDen > h * (2 * DriftDen + DriftNum)

VARIABLES
    fage,       \* per client: ticks since its fence committed in which the leader held its
                \* lease and every storage node was reachable or marked down, up to 2
    cl,         \* per client: lease, outstanding renewal, rate window, VM state, lock
    rate, boot, rose,   \* per control-plane node: raw rate, boot number, rate risen
    term, lnode, holding,   \* current term, each term's leader node, the term holding the lease
    stalled,    \* the leader's loop is stalled
    timer,      \* the leader's per-session count, in its units, since it last renewed it
    meas,       \* per client, the leader's measurement of its rate over the client's: -1 none,
                \* 0 after one renewal, and the rate measured after a second
    evicting,   \* per client: the owner asked for an eviction
    req, rep,   \* renewals and their answers in flight
    tver, fencedAt, acked,  \* the session table: version, fence versions, acknowledged
    nver, nint, reach, down, unreach,  \* storage nodes: versions held, reachability, down marks
    intv, liveAt, activated, ctr,      \* the PG's intervals, live members, last activated, counters
    actd,       \* per storage node, the interval it last activated as primary
    app,        \* per storage node, the writes it has applied
    writes,     \* client writes in flight, queued, or replicating
    lockHolder, lockGen,
    budget,
    hist        \* history: completed writes and violation flags

vars == <<fage, cl, rate, boot, rose, term, lnode, holding, stalled, timer, meas, evicting,
          req, rep, tver, fencedAt, acked, nver, nint, reach, down, unreach, intv, liveAt,
          activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

clientVars == <<cl>>
leaderVars == <<rate, boot, rose, term, lnode, holding, stalled, timer, meas, evicting>>
tableVars == <<tver, fencedAt, acked, fage>>
storageVars == <<nver, nint, reach, down, unreach, intv, liveAt, activated, actd, ctr, app>>

FirstClient == SetMin(Clients)
ClientRate == SetMin(Rates)
\* Ticks a client's rate window lasts: past it the window starts again (rate_window).
CapT == Cap \div ClientRate + 1
\* The leader's count after which a session is due, in its units: the lease, stretched by
\* the leader's rate over the client's once it has measured it from two renewals, and
\* DeadWait before that. The leader's timer saturates there.
Threshold(c) ==
    IF ~CountOnHolder THEN SessionLease
    ELSE IF meas[c] > 0 THEN Max(SessionLease, (SessionLease * meas[c] + ClientRate - 1) \div ClientRate)
    ELSE DeadWait
TCap == IF CountOnHolder THEN Max(SessionLease, DeadWait) ELSE SessionLease
\* The leader's measurement after it processes a renewal from the client.
Measured(c, n) == IF meas[c] < 0 THEN 0 ELSE rate[n]

Init ==
    /\ fage = [c \in Clients |-> 0]
    /\ cl = [c \in Clients |->
               [on |-> FALSE, age |-> 0, len |-> 0, out |-> -1, seq |-> 0,
                wkey |-> None, wn |-> 0, B |-> None, hs |-> 0, ra |-> 0, gt |-> 0,
                stopped |-> FALSE, crashed |-> FALSE, told |-> FALSE,
                holds |-> IF WithLock /\ c = FirstClient THEN 1 ELSE 0, inflight |-> 0]]
    /\ rate \in [Leaders -> Rates]
    /\ boot = [n \in Leaders |-> 1]
    /\ rose = [n \in Leaders |-> FALSE]
    /\ term = 1
    /\ \E n \in Leaders : lnode = [t \in 1 .. MaxTerms |-> n]
    /\ holding = 1
    /\ stalled = FALSE
    /\ timer = [c \in Clients |-> 0]
    \* (not Windows) the first leader starts out having measured every client.
    /\ meas = [c \in Clients |-> IF Windows THEN -1 ELSE rate[lnode[1]]]
    /\ evicting = [c \in Clients |-> FALSE]
    /\ req = {} /\ rep = {}
    /\ tver = 0 /\ fencedAt = [c \in Clients |-> 0] /\ acked = 0
    /\ nver = [s \in Storage |-> 0] /\ nint = [s \in Storage |-> 1]
    /\ reach = [s \in Storage |-> TRUE] /\ down = {} /\ unreach = [s \in Storage |-> 0]
    /\ intv = 1 /\ liveAt = [k \in Intervals |-> IF k = 1 THEN Storage ELSE {}]
    /\ activated = 1 /\ actd = [s \in Storage |-> IF s = SetMin(Storage) THEN 1 ELSE 0]
    /\ ctr = [k \in Intervals |-> 0]
    /\ app = [s \in Storage |-> {}]
    /\ writes = {}
    /\ lockHolder = IF WithLock THEN FirstClient ELSE 0
    /\ lockGen = IF WithLock THEN 1 ELSE 0
    /\ budget = [reboot |-> MaxReboots, rise |-> MaxRises, stop |-> MaxStops,
                 stall |-> MaxStalls, evict |-> MaxEvicts, crash |-> MaxCrashes,
                 part |-> MaxPartitions, w |-> MaxWrites, lost |-> MaxLost]
    /\ hist = [done |-> {}, fenceInLease |-> FALSE, fencedApplied |-> FALSE,
               misordered |-> FALSE, trip |-> FALSE]

Leased(c) == cl[c].on /\ cl[c].age < cl[c].len
\* The client can act: its VM runs, its daemon is alive, and it has not learned of a fence.
Acting(c) == ~cl[c].stopped /\ ~cl[c].crashed /\ ~cl[c].told
Live(c) == fencedAt[c] = 0
LeftTable(c) == fencedAt[c] # 0 /\ fencedAt[c] <= acked
FencedIn(v, c) == fencedAt[c] # 0 /\ fencedAt[c] <= v
Leader == lnode[term]
Active == holding = term /\ ~stalled
Primary(k) == IF liveAt[k] = {} THEN 0 ELSE SetMin(liveAt[k])

-----------------------------------------------------------------------------
\* Time.

\* One tick: every running clock advances by its node's rate. Time does not pass while the
\* leader owes a fence it can commit, an acknowledgment it can commit, or a down mark, or
\* while a reachable storage node lags the session table: those take well under a tick.
Due(c) == Live(c) /\ timer[c] >= Threshold(c)
Urgent ==
    \/ Active /\ \E c \in Clients : Due(c)
    \/ Active /\ \E s \in Storage : reach[s] /\ nver[s] < tver
    \/ Active /\ tver > acked /\ \A s \in Storage : nver[s] >= tver \/ (s \in down /\ DownCountsAsAck)
    \/ Active /\ \E s \in Storage : ~reach[s] /\ s \notin down /\ unreach[s] >= DownAfter

Tick ==
    /\ ~Urgent
    /\ fage' = [c \in Clients |->
                  IF fencedAt[c] # 0 /\ Active /\ \A s \in Storage : reach[s] \/ s \in down
                  THEN Min(fage[c] + 1, 2) ELSE fage[c]]
    /\ cl' = [c \in Clients |->
                IF cl[c].stopped THEN cl[c]
                ELSE LET r == ClientRate IN
                     [cl[c] EXCEPT !.age = Sat(@ + r), !.out = IF @ < 0 THEN @ ELSE Sat(@ + r),
                                   !.hs = IF Windows THEN Sat(@ + r) ELSE @,
                                   !.ra = IF Windows THEN Sat(@ + r) ELSE @,
                                   !.gt = IF Windows THEN Min(@ + 1, CapT) ELSE @]]
    /\ rep' = IF Windows THEN {[m EXCEPT !.age = Min(@ + 1, CapT)] : m \in rep} ELSE rep
    /\ timer' = [c \in Clients |-> Min(timer[c] + rate[Leader], TCap)]
    /\ unreach' = [s \in Storage |-> IF reach[s] THEN 0 ELSE Min(unreach[s] + 1, DownAfter)]
    /\ UNCHANGED <<rate, boot, rose, term, lnode, holding, stalled, meas, evicting, req,
                   tver, fencedAt, acked, nver, nint, reach, down, intv, liveAt, activated, actd, ctr, app, writes,
                   lockHolder, lockGen, budget, hist>>

-----------------------------------------------------------------------------
\* The client's side of its session.

\* (not Windows) a renewal in one step, at a leader holding its lease: configurations that
\* check fencing rather than lease timing leave out the messages and the rate windows, so
\* the lease is counted from the leader's processing on equal clocks, and the leader has
\* measured the client.
RenewNow(c) ==
    /\ ~Windows /\ Acting(c) /\ Active /\ ~(evicting[c] /\ EvictWaitsLease)
    /\ IF Live(c)
       THEN /\ timer' = [timer EXCEPT ![c] = 0]
            /\ meas' = [meas EXCEPT ![c] = rate[Leader]]
            /\ cl' = [cl EXCEPT ![c].on = TRUE, ![c].age = 0,
                                ![c].len = LeaseLen(<<rate[Leader], ClientRate>>)]
       ELSE /\ cl' = [cl EXCEPT ![c].told = TRUE, ![c].on = FALSE, ![c].holds = 0]
            /\ UNCHANGED <<timer, meas>>
    /\ UNCHANGED <<rate, boot, rose, term, lnode, holding, stalled, evicting, req, rep,
                   tableVars, storageVars, writes, lockHolder, lockGen, budget, hist>>

\* The client sends a heartbeat naming its session; it keeps one renewal outstanding,
\* sending it again once both it and its answer are lost, and sends the next once a tick
\* has passed since its last grant arrived (rate_gap).
SendRenewal(c) ==
    /\ Windows /\ Acting(c)
    /\ cl[c].out < 0 \/ ~\E m \in req \cup rep : m.c = c
    /\ cl[c].wn = 0 \/ cl[c].ra >= ClientRate
    /\ cl' = [cl EXCEPT ![c].out = 0, ![c].seq = 1 - @]
    \* Requests carry unique numbers, so the client ignores any answer to an older one; the
    \* model drops those answers here and numbers requests with one bit.
    /\ req' = {m \in req : m.c # c} \cup {[c |-> c, seq |-> 1 - cl[c].seq]}
    /\ rep' = {m \in rep : m.c # c}
    /\ UNCHANGED <<leaderVars, tableVars, storageVars, writes, lockHolder, lockGen,
                   budget, hist>>

\* An answer to the outstanding renewal arrives. A fenced session learns it and starts
\* over. Otherwise the answer is a grant from one leader node and boot: the first grant of
\* a window gives no lease; later ones bound the leader's rate, pass the step check, and
\* give a lease counted from the renewal's send, lease_drift and the bound's excess early.
Answer(c, m) ==
    /\ m \in rep /\ m.c = c /\ Acting(c)
    /\ rep' = rep \ {m}
    /\ IF m.seq # cl[c].seq \/ cl[c].out < 0
       THEN UNCHANGED <<cl, hist>>
       ELSE IF m.fenced
       THEN cl' = [cl EXCEPT ![c].told = TRUE, ![c].on = FALSE, ![c].holds = 0]
            /\ UNCHANGED hist
       ELSE IF ~m.ok
       THEN cl' = [cl EXCEPT ![c].out = -1] /\ UNCHANGED hist
       ELSE LET key == IF WindowPerBoot THEN <<m.node, m.boot>> ELSE <<"any">>
                from == IF CountFromSend THEN cl[c].out ELSE 0
                first == [cl[c] EXCEPT !.out = -1, !.wkey = key, !.wn = 1, !.B = None,
                                       !.hs = cl[c].out, !.ra = 0, !.gt = m.age]
                \* Units the leader's clock ran between the last grant taken and this one.
                g == Max(0, cl[c].gt - m.age) * m.rate
                bound == IF ~Windows THEN <<m.rate, ClientRate>>
                         ELSE IF cl[c].B # None THEN cl[c].B
                         ELSE <<g, cl[c].ra - cl[c].out>>
                tripped == Windows /\ cl[c].B # None /\ Trips(g, cl[c].hs, cl[c].B)
            IN IF Windows /\ (cl[c].wkey # key \/ cl[c].wn = 0 \/ cl[c].hs >= Cap
                              \/ cl[c].gt >= CapT
                              \/ (cl[c].B = None /\ cl[c].ra <= cl[c].out))
               THEN cl' = [cl EXCEPT ![c] = first] /\ UNCHANGED hist
               ELSE IF tripped
               THEN /\ cl' = [cl EXCEPT ![c] = [first EXCEPT !.on = FALSE]]
                    /\ hist' = [hist EXCEPT !.trip = TRUE]
               ELSE /\ cl' = [cl EXCEPT ![c].out = -1, ![c].wn = 2, ![c].B = bound,
                                        ![c].hs = cl[c].out, ![c].ra = 0, ![c].gt = m.age,
                                        ![c].on = LeaseLen(bound) > 0,
                                        ![c].age = from, ![c].len = LeaseLen(bound)]
                    /\ UNCHANGED hist
    /\ UNCHANGED <<leaderVars, req, tableVars, storageVars, writes, lockHolder, lockGen,
                   budget>>

\* The client's virtual machine stops with every clock frozen, or continues. On Resumed
\* the client ends its leases and restarts its rate windows.
Stop(c) ==
    /\ ~cl[c].stopped /\ ~cl[c].crashed /\ budget.stop > 0
    /\ cl' = [cl EXCEPT ![c].stopped = TRUE]
    /\ budget' = [budget EXCEPT !.stop = @ - 1]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, writes, lockHolder,
                   lockGen, hist>>

Resume(c) ==
    /\ cl[c].stopped
    /\ cl' = [cl EXCEPT ![c].stopped = FALSE,
                        ![c].on = IF EndOnResumed THEN FALSE ELSE @,
                        ![c].out = IF EndOnResumed THEN -1 ELSE @,
                        ![c].wn = IF EndOnResumed THEN 0 ELSE @,
                        ![c].B = IF EndOnResumed THEN None ELSE @]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, writes, lockHolder,
                   lockGen, budget, hist>>

\* The client daemon dies for good and stops renewing.
CrashClient(c) ==
    /\ ~cl[c].crashed /\ budget.crash > 0
    /\ cl' = [cl EXCEPT ![c].crashed = TRUE, ![c].on = FALSE]
    /\ budget' = [budget EXCEPT !.crash = @ - 1]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, writes, lockHolder,
                   lockGen, hist>>

-----------------------------------------------------------------------------
\* The control plane.

\* The leader of term t processes a renewal: a fenced session hears so; a session being
\* evicted is not renewed; any other has its timer restarted. The answer carries the
\* node's boot; its age in flight stands for the leader's raw clock reading it carries.
Renew(t, m) ==
    /\ m \in req /\ t <= term
    /\ ~stalled \/ t # term
    /\ IF AnswerWithLease THEN holding = t /\ t = term ELSE TRUE
    /\ LET n == lnode[t] IN
       /\ req' = req \ {m}
       /\ rep' = rep \cup {[c |-> m.c, seq |-> m.seq, node |-> n, boot |-> boot[n],
                            age |-> 0, rate |-> rate[n], fenced |-> ~Live(m.c),
                            ok |-> Live(m.c) /\ ~(evicting[m.c] /\ EvictWaitsLease)]}
       /\ IF Live(m.c) /\ ~(evicting[m.c] /\ EvictWaitsLease) /\ t = term
          THEN /\ timer' = [timer EXCEPT ![m.c] = 0]
               /\ meas' = [meas EXCEPT ![m.c] = Measured(m.c, n)]
          ELSE UNCHANGED <<timer, meas>>
    /\ UNCHANGED <<cl, rate, boot, rose, term, lnode, holding, stalled, evicting, tableVars,
                   storageVars, writes, lockHolder, lockGen, budget, hist>>

\* The leader commits every session due at this moment as one new session-table version.
\* No client may still hold its lease when its fence commits.
FenceDue ==
    /\ Active
    /\ \E c \in Clients : Due(c)
    /\ LET due == {c \in Clients : Due(c)} IN
       /\ tver' = tver + 1
       /\ fencedAt' = [c \in Clients |-> IF c \in due THEN tver + 1 ELSE fencedAt[c]]
       /\ hist' = [hist EXCEPT !.fenceInLease = @ \/ \E c \in due : Leased(c) /\ ~cl[c].stopped]
    /\ UNCHANGED <<cl, leaderVars, req, rep, acked, fage, storageVars, writes, lockHolder,
                   lockGen, budget>>

\* An owner asks the control plane to evict a session that left a recall unanswered. The
\* leader stops renewing it and fences it once its lease has run out; with the rule off,
\* it fences it at once.
Evict(c) ==
    /\ Active /\ Live(c) /\ ~evicting[c] /\ budget.evict > 0
    /\ budget' = [budget EXCEPT !.evict = @ - 1]
    /\ evicting' = [evicting EXCEPT ![c] = TRUE]
    /\ IF EvictWaitsLease
       THEN UNCHANGED <<tver, fencedAt, hist>>
       ELSE /\ tver' = tver + 1
            /\ fencedAt' = [fencedAt EXCEPT ![c] = tver + 1]
            /\ hist' = [hist EXCEPT !.fenceInLease = @ \/ (Leased(c) /\ ~cl[c].stopped)]
    /\ UNCHANGED <<cl, rate, boot, rose, term, lnode, holding, stalled, timer, meas,
                   req, rep, acked, fage, storageVars, writes, lockHolder, lockGen>>

\* Once every storage node holds the version, or is marked down, it is committed as
\* acknowledged and its fenced sessions leave the table.
AckCommit ==
    /\ Active /\ tver > acked
    /\ \A s \in Storage :
         \/ nver[s] >= tver
         \/ s \in down /\ DownCountsAsAck
         \/ ~AckNeedsEveryNode /\ ~reach[s]
    /\ acked' = tver
    /\ UNCHANGED <<cl, leaderVars, req, rep, tver, fencedAt, fage, storageVars, writes,
                   lockHolder, lockGen, budget, hist>>

\* The leader's lease ends: it crashed, was partitioned from the voters, or stalled.
LoseLease ==
    /\ holding = term /\ term < MaxTerms
    /\ holding' = 0
    /\ UNCHANGED <<cl, rate, boot, rose, term, lnode, stalled, timer, meas, evicting,
                   req, rep, tableVars, storageVars, writes, lockHolder, lockGen, budget, hist>>

\* A new leader is elected once no leader lease runs, and holds its lease. It gives every
\* live session a full lease from now, with no measurement yet; with the rule off, it counts
\* each from the last renewal it can see, which is none, since renewals live only in the
\* old leader's memory.
Elect(n) ==
    /\ holding = 0 /\ term < MaxTerms
    /\ term' = term + 1
    /\ lnode' = [lnode EXCEPT ![term + 1] = n]
    /\ holding' = term + 1
    /\ stalled' = FALSE
    /\ timer' = IF FullLeaseOnFailover THEN [c \in Clients |-> 0]
                ELSE [c \in Clients |-> TCap]
    /\ meas' = [c \in Clients |-> -1]
    /\ evicting' = [c \in Clients |-> FALSE]
    /\ UNCHANGED <<cl, rate, boot, rose, req, rep, tableVars, storageVars, writes,
                   lockHolder, lockGen, budget, hist>>

\* A control-plane node reboots: a new boot, possibly at another raw rate. If it led, its
\* leader lease is gone with it.
Reboot(n) ==
    /\ budget.reboot > 0 /\ (Leader = n => term < MaxTerms)
    /\ budget' = [budget EXCEPT !.reboot = @ - 1]
    /\ boot' = [boot EXCEPT ![n] = @ + 1]
    /\ \E r \in Rates : rate' = [rate EXCEPT ![n] = r]
    /\ rose' = [rose EXCEPT ![n] = FALSE]
    /\ holding' = IF Leader = n THEN 0 ELSE holding
    /\ UNCHANGED <<cl, term, lnode, stalled, timer, meas, evicting, req, rep, tableVars,
                   storageVars, writes, lockHolder, lockGen, hist>>

\* A node's raw rate rises within the lease margin's headroom, as a clocksource switch or
\* a migration does, once per boot.
Rise(n) ==
    /\ budget.rise > 0 /\ ~rose[n]
    /\ (rate[n] + 1) * 2 * DriftDen <= rate[n] * (2 * DriftDen + DriftNum)
    /\ budget' = [budget EXCEPT !.rise = @ - 1]
    /\ rate' = [rate EXCEPT ![n] = @ + 1]
    /\ rose' = [rose EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<cl, boot, term, lnode, holding, stalled, timer, meas, evicting,
                   req, rep, tableVars, storageVars, writes, lockHolder, lockGen, hist>>

\* The leader's loop stalls; when it runs again it restarts every session's timer rather
\* than expire sessions it could not hear.
Stall ==
    /\ ~stalled /\ budget.stall > 0
    /\ stalled' = TRUE
    /\ budget' = [budget EXCEPT !.stall = @ - 1]
    /\ UNCHANGED <<cl, rate, boot, rose, term, lnode, holding, timer, meas, evicting,
                   req, rep, tableVars, storageVars, writes, lockHolder, lockGen, hist>>

Wake ==
    /\ stalled
    /\ stalled' = FALSE
    /\ timer' = [c \in Clients |-> 0]
    /\ UNCHANGED <<cl, rate, boot, rose, term, lnode, holding, meas, evicting, req, rep,
                   tableVars, storageVars, writes, lockHolder, lockGen, budget, hist>>

\* A renewal or an answer is lost.
Lose ==
    /\ budget.lost > 0
    /\ budget' = [budget EXCEPT !.lost = @ - 1]
    /\ \/ \E m \in req : req' = req \ {m} /\ UNCHANGED rep
       \/ \E m \in rep : rep' = rep \ {m} /\ UNCHANGED req
    /\ UNCHANGED <<cl, leaderVars, tableVars, storageVars, writes, lockHolder, lockGen, hist>>

-----------------------------------------------------------------------------
\* Storage nodes and the PG.

\* A storage node the control plane reaches applies the newest session-table version, and
\* from then on rejects the fenced sessions' writes; its heartbeat acknowledges it.
ApplyTable(s) ==
    /\ reach[s] /\ nver[s] < tver
    /\ nver' = [nver EXCEPT ![s] = tver]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nint, reach, down, unreach, intv,
                   liveAt, activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

\* A storage node the control plane reaches adopts the PG's current interval.
AdoptInterval(s) ==
    /\ reach[s] /\ nint[s] < intv
    /\ nint' = [nint EXCEPT ![s] = intv]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, reach, down, unreach, intv,
                   liveAt, activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

\* A storage node is partitioned from the control plane, but not from clients or the
\* other storage nodes; or the partition heals.
Partition(s) ==
    /\ reach[s] /\ budget.part > 0
    /\ reach' = [reach EXCEPT ![s] = FALSE]
    /\ budget' = [budget EXCEPT !.part = @ - 1]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, down, unreach, intv,
                   liveAt, activated, actd, ctr, app, writes, lockHolder, lockGen, hist>>

Heal(s) ==
    /\ ~reach[s]
    /\ reach' = [reach EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, down, unreach, intv,
                   liveAt, activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

\* After node_down_after without a heartbeat, the leader commits a down mark, which starts
\* a new interval of the PG.
MarkDown(s) ==
    /\ Active /\ ~reach[s] /\ s \notin down /\ unreach[s] >= DownAfter /\ intv < MaxIntv
    /\ down' = down \cup {s}
    /\ intv' = intv + 1
    /\ liveAt' = [liveAt EXCEPT ![intv + 1] = Storage \ (down \cup {s})]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, reach, unreach,
                   activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

\* A down node whose heartbeat reports the current session-table version is marked up, in
\* a new interval.
MarkUp(s) ==
    /\ Active /\ reach[s] /\ s \in down /\ intv < MaxIntv
    /\ UpNeedsCurrent => nver[s] = tver
    /\ down' = down \ {s}
    /\ intv' = intv + 1
    /\ liveAt' = [liveAt EXCEPT ![intv + 1] = Storage \ (down \ {s})]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, reach, unreach,
                   activated, actd, ctr, app, writes, lockHolder, lockGen, budget, hist>>

\* The interval's primary activates it once it has peered: every interval since the last
\* activated one had a live member adopt this one, so none of their writes can complete.
Activate ==
    /\ activated < intv /\ Primary(intv) # 0 /\ nint[Primary(intv)] = intv
    /\ PeeringStops =>
         \A j \in activated .. intv - 1 : liveAt[j] = {} \/ \E s \in liveAt[j] : nint[s] > j
    /\ activated' = intv
    /\ actd' = [actd EXCEPT ![Primary(intv)] = intv]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, reach, down, unreach,
                   intv, liveAt, ctr, app, writes, lockHolder, lockGen, budget, hist>>

-----------------------------------------------------------------------------
\* Writes and the lock.

\* A client holding the lock, under its lease, writes through the primary of the PG's
\* current interval, tagged with that interval.
Write(c) ==
    /\ Acting(c) /\ Leased(c) /\ cl[c].holds # 0 /\ cl[c].inflight < MaxInFlight
    /\ budget.w > 0 /\ Primary(intv) # 0
    /\ budget' = [budget EXCEPT !.w = @ - 1]
    /\ writes' = writes \cup {[id |-> budget.w, c |-> c, gen |-> cl[c].holds, tag |-> intv,
                               at |-> Primary(intv), st |-> "net", v |-> <<0, 0>>, acks |-> {}, base |-> {}]}
    /\ cl' = [cl EXCEPT ![c].inflight = @ + 1]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, lockHolder, lockGen, hist>>

\* A write reaches its primary, which refuses one tagged with an older interval, fetches a
\* newer table first, and otherwise queues it (checking the fence here too).
Arrive(w) ==
    /\ w \in writes /\ w.st = "net" /\ w.tag <= nint[w.at]
    /\ writes' = (writes \ {w}) \cup
                 {[w EXCEPT !.st = IF w.tag < nint[w.at] \/ FencedIn(nver[w.at], w.c)
                                   THEN "ended" ELSE "queued"]}
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, storageVars, lockHolder, lockGen,
                   budget, hist>>

\* A write every live member of its interval holds is complete, whenever its answer then
\* reaches the client. A completed write of an older lock generation must be ordered before
\* every completed write of a newer one, in its version and in the history (base) its
\* primary held when it ordered the newer one.
Completes(w) ==
    [hist EXCEPT
       !.done = @ \cup {[gen |-> w.gen, v |-> w.v, id |-> w.id, base |-> w.base]},
       !.misordered = @ \/ \E d \in hist.done :
            \/ d.gen > w.gen /\ w.id \notin d.base
            \/ d.gen < w.gen /\ d.id \notin w.base
            \/ d.gen < w.gen /\ (d.v[1] > w.v[1] \/ (d.v[1] = w.v[1] /\ d.v[2] > w.v[2]))]
Held(w, acks) == liveAt[w.tag] \ {w.at} \subseteq acks

\* The primary orders a queued write in the interval it itself activated, checking the fence
\* then, and applies it. A write it held across an interval change is ordered again only if
\* no other primary led the PG since; otherwise it is refused.
Order(w) ==
    /\ w \in writes /\ w.st = "queued"
    /\ LET p == w.at
           k == nint[p]
           other == \E j \in w.tag + 1 .. k : Primary(j) \notin {0, p}
           fenced == CheckWhenOrdered /\ FencedIn(nver[p], w.c)
           \* Peering brought every write a reached member holds into p's history; the
           \* model takes every write any member has applied, which only weakens the check.
           w2 == [w EXCEPT !.tag = k, !.v = <<k, ctr[k] + 1>>,
                           !.base = UNION {app[s] : s \in Storage}]
       IN /\ Primary(k) = p /\ actd[p] = k
          /\ IF other \/ fenced
             THEN writes' = (writes \ {w}) \cup {[w EXCEPT !.st = "ended"]}
                  /\ UNCHANGED <<ctr, app, hist>>
             ELSE /\ ctr' = [ctr EXCEPT ![k] = @ + 1]
                  /\ writes' = (writes \ {w}) \cup
                               {[w2 EXCEPT !.st = IF Held(w2, {}) THEN "ended" ELSE "replicating"]}
                  /\ app' = [app EXCEPT ![p] = @ \cup {w.id}]
                  /\ LET h == IF Held(w2, {}) THEN Completes(w2) ELSE hist
                     IN hist' = [h EXCEPT !.fencedApplied = @ \/ FencedIn(nver[p], w.c)]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, reach, down, unreach,
                   intv, liveAt, activated, actd, lockHolder, lockGen, budget>>

\* A member receives the replicated write: it refuses an older interval, fetches a newer
\* table first, checks the fence, and stores and acknowledges it.
Replicate(w, s) ==
    /\ w \in writes /\ w.st = "replicating"
    /\ s \in liveAt[w.tag] \ ({w.at} \cup w.acks) /\ w.tag <= nint[s]
    /\ IF w.tag < nint[s] \/ (ReplicaChecks /\ FencedIn(nver[s], w.c))
       THEN writes' = (writes \ {w}) \cup {[w EXCEPT !.st = "ended"]} /\ UNCHANGED <<app, hist>>
       ELSE LET acks == w.acks \cup {s}
                h == IF Held(w, acks) THEN Completes(w) ELSE hist
            IN /\ writes' = (writes \ {w}) \cup
                            {[w EXCEPT !.acks = acks, !.st = IF Held(w, acks) THEN "ended" ELSE @]}
               /\ app' = [app EXCEPT ![s] = @ \cup {w.id}]
               /\ hist' = [h EXCEPT !.fencedApplied = @ \/ FencedIn(nver[s], w.c)]
    /\ UNCHANGED <<cl, leaderVars, req, rep, tableVars, nver, nint, reach, down, unreach,
                   intv, liveAt, activated, actd, ctr, lockHolder, lockGen, budget>>

\* The answer to a completed or refused write returns to its client.
Answered(w) ==
    /\ w \in writes /\ w.st = "ended"
    /\ writes' = writes \ {w}
    /\ cl' = [cl EXCEPT ![w.c].inflight = @ - 1]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, lockHolder, lockGen,
                   budget, hist>>

\* The owner grants the lock to the next client once the holder's session has left the
\* table (with the rule off, once its fence commits); the next holder deletes the old key.
Grant(c) ==
    /\ WithLock /\ lockHolder # c /\ Acting(c) /\ Live(c) /\ lockGen < 2
    /\ lockHolder = 0 \/ (IF ReleaseOnAck THEN LeftTable(lockHolder) ELSE ~Live(lockHolder))
    /\ lockHolder' = c
    /\ lockGen' = lockGen + 1
    /\ cl' = [cl EXCEPT ![c].holds = lockGen + 1]
    /\ UNCHANGED <<leaderVars, req, rep, tableVars, storageVars, writes, budget, hist>>

-----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E c \in Clients : \/ SendRenewal(c) \/ RenewNow(c) \/ Stop(c) \/ Resume(c) \/ CrashClient(c)
                          \/ Evict(c) \/ Write(c) \/ Grant(c)
    \/ \E c \in Clients, m \in rep : Answer(c, m)
    \/ \E t \in 1 .. MaxTerms, m \in req : Renew(t, m)
    \/ FenceDue \/ AckCommit \/ LoseLease \/ Stall \/ Wake \/ Lose \/ Activate
    \/ \E n \in Leaders : Elect(n) \/ Reboot(n) \/ Rise(n)
    \/ \E s \in Storage : \/ ApplyTable(s) \/ AdoptInterval(s) \/ Partition(s) \/ Heal(s)
                          \/ MarkDown(s) \/ MarkUp(s)
    \/ \E w \in writes : \/ Arrive(w) \/ Order(w) \/ Answered(w)
                         \/ \E s \in Storage : Replicate(w, s)

Spec == Init /\ [][Next]_vars

\* Liveness: time passes, the leader commits and acknowledges fences, storage nodes apply
\* tables, down marks are committed and leaders get elected.
FairSpec ==
    /\ Spec
    /\ WF_vars(Tick) /\ WF_vars(FenceDue) /\ WF_vars(AckCommit) /\ WF_vars(Wake)
    /\ \A n \in Leaders : WF_vars(Elect(n))
    /\ \A s \in Storage : WF_vars(ApplyTable(s)) /\ WF_vars(MarkDown(s)) /\ WF_vars(Heal(s))

ClientSymmetry == Permutations(Clients)

-----------------------------------------------------------------------------
\* Properties.

TypeOK ==
    /\ term \in 1 .. MaxTerms /\ holding \in 0 .. MaxTerms
    /\ fencedAt \in [Clients -> Nat] /\ acked <= tver
    /\ \A c \in Clients : cl[c].inflight \in 0 .. MaxInFlight

\* No fence commits while the client's own lease still runs.
NoFenceWhileLeased ==
    /\ ~hist.fenceInLease
    /\ \A c \in Clients : ~Live(c) => ~(Leased(c) /\ ~cl[c].stopped /\ ~cl[c].told)

\* No fenced session's write is applied after the storage node applied the fence.
NoFencedWriteApplied == ~hist.fencedApplied

\* No lock reaches a new holder before the old holder's writes are ordered: every completed
\* write of an older lock generation completed before, and is ordered before, every
\* completed write of a newer one.
LockOrder == ~hist.misordered

\* At most one client holds the lock and may act on it.
AtMostOneHolder ==
    Cardinality({c \in Clients : cl[c].holds # 0 /\ Leased(c) /\ Acting(c)}) <= 1

\* The step check never trips between a client never stopped and a leader boot whose rate
\* stays within the margin's headroom.
NoSpuriousTrip == ~hist.trip

\* A fenced session leaves the table, and its lock is free, within one tick of its fence
\* commit (the commit, the push and an acknowledgment), not counting ticks in which the
\* leader lacked its lease or a storage node was unreachable and not yet marked down; time
\* cannot pass a down mark that is due. The fence itself commits in the tick the session's
\* lease runs out on the leader's clock, since time cannot pass a fence the leader owes.
FenceReleasedInTime == \A c \in Clients : fage[c] >= 1 => LeftTable(c)

\* A client that crashed is eventually fenced and leaves the table (deep only).
CrashedClientReleased == \A c \in Clients : cl[c].crashed ~> LeftTable(c)

=============================================================================
