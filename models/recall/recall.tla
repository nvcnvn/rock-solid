-------------------------------- MODULE recall --------------------------------
\* Tier 2 cache coherence for one file (docs/design.md, Cache coherence tiers): the
\* inode owner's open registry and cache modes, recalls and their acknowledgments, each
\* node's daemon (write-back buffer, what it last learned of the mode) and kernel (page
\* cache, attribute cache, cached and direct-I/O handles), the file's stored bytes and
\* size, session leases and fences abstracted from the sessions model, owner failover
\* with its grace period, and paused daemons whose kernels keep serving.
\*
\* The file has one region per element of Regions; region 0 lies inside the starting
\* size and a higher one past it, so writing it extends the file. Each write gets the
\* next number, and a region holds the number of the write it last stored (0 at first).
\* Mapped pages are left out. Each rule the design states is a BOOLEAN constant: the ci
\* and deep configurations set every one TRUE, and each negative configuration turns one off.

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,          \* client nodes, each a daemon and a kernel
    Regions,        \* {0} or {0, 1}
    MaxHandles,     \* handles one node can hold open on the file
    MaxWrites, MaxFailovers, MaxLost,
    MaxPauses, MaxLapses,  \* daemon pauses, and session leases that run out
    MaxRecalls             \* recalls numbered while earlier ones are still in flight

CONSTANTS
    RecallFlush,            \* step 1: the recalled daemon flushes its buffered writes
    RecallDrop,             \* step 2: it drops the kernel's pages and attributes
    RecallDirect,           \* step 3: its new opens use direct I/O
    RecallReval,            \* step 4: it revalidates old handles (attribute timeout 0)
    AnswerAfterRecall,      \* the owner answers the open or write only after the recall
    NoTimeoutAnswer,        \* a recall ends only by an acknowledgment or a fence
    RecallIds,              \* an acknowledgment counts only for the recall and term it names
    AttrWithinLease,        \* no attribute timeout reaches past the session lease
    EvictWaitsLease,        \* an eviction fences only once the session's lease ran out
    GraceBlocksOpens,       \* the new owner takes no new opens or writes in its grace period
    GraceUntilReregistered, \* the grace period lasts until every live session re-registered
    ReregCarriesWrote,      \* a re-registration says whether the node has written
    StayWriteShared,        \* WriteShared ends only when one node still has the file open
    SizeBeforeAck,          \* an extending write commits the size before it is acknowledged
    KeepCacheCheck          \* Tier 1: an open drops pages if the file changed since cached

NoPage == -1
Max(a, b) == IF a >= b THEN a ELSE b
SetMax(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : x >= y
Flags(m) == CASE m = "Ex" -> [buffer |-> TRUE, direct |-> FALSE, reval |-> FALSE]
              [] m = "RS" -> [buffer |-> FALSE, direct |-> FALSE, reval |-> FALSE]
              [] m = "WS" -> [buffer |-> FALSE, direct |-> TRUE, reval |-> TRUE]
ModeOf(M, W) == IF M = {} THEN "none" ELSE IF Cardinality(M) = 1 THEN "Ex"
                ELSE IF W \cap M # {} THEN "WS" ELSE "RS"

VARIABLES
    store, size,        \* storage: per region the stored write, and the committed size
    sizeq,              \* (not SizeBeforeAck) extensions acknowledged, size not yet committed
    members, wrote,     \* the owner's open registry: nodes with the file open, and writers
    mode,               \* the owner's cache mode: "none", "Ex", "RS" or "WS"
    pend,               \* the open or first write waiting on a recall, or <<>>
    epoch, grace,       \* the owner's term, and whether it is in its grace period
    nextId,             \* the next recall number
    rmsg, amsg,         \* recalls and acknowledgments in flight
    hc, hd,             \* per node, cached and direct-I/O handles open
    pc, attr,           \* per node, the kernel's pages and whether it trusts its attributes
    dflags,             \* per node, what the daemon learned: buffer, direct, reval
    buf,                \* per node and region, the daemon's newest buffered write, or 0
    iw,                 \* per node, the daemon has written since it opened the file
    oq, wq,             \* per node, an open, or a first write to a region, waiting
    lease, paused,      \* per node, the session lease and whether the daemon is paused
    evicted, fenced,    \* per node, eviction requested, and fenced and acknowledged
    rereg,              \* per node, it still owes the new owner a re-registration
    budget,             \* writes, failovers and lost messages left
    hist                \* history: acked and failed writes, close and open floors, flags

vars == <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg, amsg,
          hc, hd, pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg,
          budget, hist>>

Init ==
    /\ store = [r \in Regions |-> 0]
    /\ size = 1
    /\ sizeq = {}
    /\ members = {} /\ wrote = {} /\ mode = "none" /\ pend = <<>>
    /\ epoch = 1 /\ grace = FALSE /\ nextId = 1
    /\ rmsg = {} /\ amsg = {}
    /\ hc = [n \in Nodes |-> 0] /\ hd = [n \in Nodes |-> 0]
    /\ pc = [n \in Nodes |-> [r \in Regions |-> NoPage]]
    /\ attr = [n \in Nodes |-> FALSE]
    /\ dflags = [n \in Nodes |-> Flags("RS")]
    /\ buf = [n \in Nodes |-> [r \in Regions |-> 0]]
    /\ iw = [n \in Nodes |-> FALSE]
    /\ oq = [n \in Nodes |-> FALSE] /\ wq = [n \in Nodes |-> NoPage]
    /\ lease = [n \in Nodes |-> TRUE] /\ paused = [n \in Nodes |-> FALSE]
    /\ evicted = [n \in Nodes |-> FALSE] /\ fenced = [n \in Nodes |-> FALSE]
    /\ rereg = [n \in Nodes |-> FALSE]
    /\ budget = [w |-> MaxWrites, o |-> MaxFailovers, l |-> MaxLost, p |-> MaxPauses,
                 x |-> MaxLapses]
    /\ hist = [acked |-> {}, failed |-> {}, closed |-> [n \in Nodes |-> [r \in Regions |-> 0]],
               floor |-> [n \in Nodes |-> [r \in Regions |-> 0]], wr |-> [n \in Nodes |-> FALSE],
               bufd |-> [n \in Nodes |-> {}], staleWS |-> FALSE, staleCTO |-> FALSE]

\* The node's kernel holds a handle; a fenced node's applications still do.
Open(n) == hc[n] + hd[n] > 0
\* The daemon serves a request only while running, under a lease, and not fenced.
Serving(n) == ~paused[n] /\ lease[n] /\ ~fenced[n]
\* What the daemon reads for region r: its buffer, else storage clamped to the size.
DV(n, r) == IF buf[n][r] # 0 THEN buf[n][r] ELSE IF r < size THEN store[r] ELSE 0
\* Every write acknowledged and not lost to a fence, per region.
Newest(r) == SetMax({w.id : w \in {x \in hist.acked : x.r = r /\ x.id \notin hist.failed}})
\* Two nodes have the file open and one of them has written to it since it opened it.
SharedWritten ==
    /\ Cardinality({n \in Nodes : Open(n)}) >= 2
    /\ \E n \in Nodes : Open(n) /\ hist.wr[n]
\* Pages the kernel drops when attributes show the file changed (auto_inval_data).
Fresh(n) == [r \in Regions |-> IF pc[n][r] = DV(n, r) THEN pc[n][r] ELSE NoPage]
\* The owner's view of nodes other than n that may cache or buffer the file.
Others(n) == members \ {n}
\* The mode once a node leaves the registry: WriteShared stays until one node is left,
\* or, with the rule off, ends once no more than one remaining member has written.
AfterLeave(M, W) ==
    IF mode = "none" THEN "none"
    ELSE IF mode = "WS" /\ Cardinality(M) >= 2
         THEN (IF ~StayWriteShared /\ Cardinality(W \cap M) <= 1 THEN "Ex" ELSE "WS")
         ELSE ModeOf(M, W)

\* A read on node n of region r returning v: checks both properties.
Check(n, r, v) ==
    hist' = [hist EXCEPT !.staleWS = @ \/ (SharedWritten /\ v < Newest(r)),
                         !.staleCTO = @ \/ v < hist.floor[n][r]]

-----------------------------------------------------------------------------
\* Reads, opens, writes and closes on a node.

\* read() on a cached handle: the kernel serves its page while it trusts the attributes;
\* otherwise it asks the daemon for attributes, drops changed pages, and reads through
\* the daemon, keeping the page. Under revalidation the attribute timeout is 0, so every
\* read asks.
ReadCached(n, r) ==
    /\ hc[n] > 0
    /\ IF attr[n] /\ pc[n][r] # NoPage
       THEN /\ Check(n, r, pc[n][r])
            /\ UNCHANGED <<pc, attr>>
       ELSE /\ Serving(n)
            /\ Check(n, r, DV(n, r))
            /\ pc' = [pc EXCEPT ![n] = [Fresh(n) EXCEPT ![r] = DV(n, r)]]
            /\ attr' = [attr EXCEPT ![n] = ~dflags[n].reval]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg,
                   budget>>

\* read() on a direct-I/O handle goes to the daemon every time.
ReadDirect(n, r) ==
    /\ hd[n] > 0 /\ Serving(n)
    /\ Check(n, r, DV(n, r))
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted,
                   fenced, rereg, budget>>

OwnerFree == ~grace \/ ~GraceBlocksOpens
\* The next recall's number. Numbering starts again once no recall or acknowledgment is in
\* flight, which keeps the state finite and changes nothing a node or the owner can see.
Rid == IF rmsg = {} /\ amsg = {} THEN 1 ELSE nextId

\* A handle opened in mode m: cached unless m is WriteShared; a cached open drops the
\* pages of a file that changed since this node cached it (Tier 1). Attributes come back
\* with the open.
Opened(n, m, firstOpen) ==
    /\ dflags' = [dflags EXCEPT ![n] = Flags(m)]
    /\ IF m = "WS"
       THEN /\ hd' = [hd EXCEPT ![n] = @ + 1] /\ UNCHANGED <<hc, pc>>
            /\ attr' = [attr EXCEPT ![n] = FALSE]
       ELSE /\ hc' = [hc EXCEPT ![n] = @ + 1] /\ UNCHANGED hd
            /\ pc' = IF KeepCacheCheck THEN [pc EXCEPT ![n] = Fresh(n)] ELSE pc
            /\ attr' = [attr EXCEPT ![n] = TRUE]
    /\ hist' = IF firstOpen
               THEN [hist EXCEPT !.floor[n] = [r \in Regions |->
                                   SetMax({hist.closed[m2][r] : m2 \in Nodes \ {n}})]]
               ELSE hist

\* open() of the file's first handle on n registers with the owner. A second node's open
\* of a file another node has written waits on a recall; otherwise the owner answers with
\* the mode at once.
OpenFirst(n) ==
    /\ hc[n] + hd[n] = 0 /\ Serving(n) /\ OwnerFree /\ pend = <<>> /\ ~rereg[n]
    /\ LET M == members \cup {n}
           holders == {m \in Others(n) : m \in wrote}
       IN IF holders = {} \/ mode = "WS"
          THEN /\ members' = M
               /\ mode' = ModeOf(M, wrote)
               /\ Opened(n, ModeOf(M, wrote), TRUE)
               /\ oq' = [oq EXCEPT ![n] = FALSE]
               /\ UNCHANGED <<pend, nextId, rmsg>>
          ELSE /\ pend' = [kind |-> "open", n |-> n, r |-> 0,
                           ids |-> [m \in Others(n) |-> Rid]]
               /\ rmsg' = rmsg \cup {[to |-> m, ep |-> epoch, id |-> Rid] : m \in Others(n)}
               /\ nextId' = Rid + 1 /\ Rid <= MaxRecalls
               /\ oq' = [oq EXCEPT ![n] = TRUE]
               /\ IF AnswerAfterRecall
                  THEN UNCHANGED <<members, mode, hc, hd, pc, attr, dflags, hist>>
                  ELSE /\ members' = M /\ mode' = "WS"
                       /\ Opened(n, "WS", TRUE)
    /\ UNCHANGED <<store, size, sizeq, wrote, epoch, grace, amsg, buf, iw, wq, lease, paused,
                   evicted, fenced, rereg, budget>>

\* Another handle on a node that has the file open follows what the daemon learned.
OpenMore(n) ==
    /\ hc[n] + hd[n] > 0 /\ hc[n] + hd[n] < MaxHandles /\ Serving(n)
    /\ IF dflags[n].direct
       THEN hd' = [hd EXCEPT ![n] = @ + 1] /\ UNCHANGED hc
       ELSE hc' = [hc EXCEPT ![n] = @ + 1] /\ UNCHANGED hd
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced,
                   rereg, budget, hist>>

\* A write the daemon has registered: buffered and acknowledged at once in Exclusive,
\* otherwise sent to the chunk primary; an extension commits the size before it is
\* acknowledged. A write through a cached handle fills the kernel's page.
DoWrite(n, r, cachedHandle, buffered) ==
    LET id == MaxWrites - budget.w + 1
    IN /\ budget' = [budget EXCEPT !.w = @ - 1]
       /\ IF buffered
          THEN /\ buf' = [buf EXCEPT ![n][r] = id]
               /\ UNCHANGED <<store, size, sizeq>>
          ELSE /\ store' = [store EXCEPT ![r] = id]
               /\ IF r < size \/ SizeBeforeAck
                  THEN size' = Max(size, r + 1) /\ UNCHANGED sizeq
                  ELSE sizeq' = sizeq \cup {r} /\ UNCHANGED size
               /\ UNCHANGED buf
       /\ pc' = [pc EXCEPT ![n][r] = IF cachedHandle THEN id ELSE NoPage]
       /\ hist' = [hist EXCEPT !.acked = @ \cup {[id |-> id, r |-> r, n |-> n]},
                               !.wr[n] = TRUE,
                               !.bufd[n] = IF buffered THEN @ \cup {id} ELSE @]

\* write(): a node's first write to the file registers with the owner, which in
\* ReadShared first recalls every other member; later writes go straight on.
Write(n, r) ==
    /\ hc[n] + hd[n] > 0 /\ Serving(n) /\ budget.w > 0 /\ wq[n] = NoPage
    /\ \E cachedHandle \in {b \in BOOLEAN : IF b THEN hc[n] > 0 ELSE hd[n] > 0} :
       IF iw[n]
       THEN /\ DoWrite(n, r, cachedHandle, dflags[n].buffer)
            /\ UNCHANGED <<members, wrote, mode, pend, nextId, rmsg, dflags, iw, wq, attr>>
       ELSE /\ OwnerFree /\ pend = <<>> /\ ~rereg[n]
            /\ IF mode = "RS" /\ Others(n) # {}
               THEN /\ pend' = [kind |-> "write", n |-> n, r |-> r,
                                ids |-> [m \in Others(n) |-> Rid]]
                    /\ rmsg' = rmsg \cup {[to |-> m, ep |-> epoch, id |-> Rid] : m \in Others(n)}
                    /\ nextId' = Rid + 1 /\ Rid <= MaxRecalls
                    /\ IF AnswerAfterRecall
                       THEN /\ wq' = [wq EXCEPT ![n] = r]
                            /\ UNCHANGED <<members, wrote, mode, dflags, iw, pc, attr, buf,
                                           store, size, sizeq, budget, hist>>
                       ELSE /\ wrote' = wrote \cup {n} /\ mode' = "WS"
                            /\ dflags' = [dflags EXCEPT ![n] = Flags("WS")]
                            /\ attr' = [attr EXCEPT ![n] = FALSE]
                            /\ iw' = [iw EXCEPT ![n] = TRUE]
                            /\ DoWrite(n, r, cachedHandle, FALSE)
                            /\ UNCHANGED <<members, wq>>
               ELSE LET m == IF mode \in {"Ex", "WS"} THEN mode ELSE "Ex"
                    IN /\ wrote' = wrote \cup {n}
                       /\ dflags' = [dflags EXCEPT ![n] = Flags(m)]
                       /\ attr' = IF m = "WS" THEN [attr EXCEPT ![n] = FALSE] ELSE attr
                       /\ iw' = [iw EXCEPT ![n] = TRUE]
                       /\ DoWrite(n, r, cachedHandle, m = "Ex")
                    /\ UNCHANGED <<members, mode, pend, nextId, rmsg, wq>>
    /\ UNCHANGED <<epoch, grace, amsg, hc, hd, oq, lease, paused, evicted, fenced, rereg>>

\* (not SizeBeforeAck) the size of an acknowledged extension is committed.
CommitSize(r) ==
    /\ r \in sizeq
    /\ size' = Max(size, r + 1)
    /\ sizeq' = sizeq \ {r}
    /\ UNCHANGED <<store, members, wrote, mode, pend, epoch, grace, nextId, rmsg, amsg, hc, hd,
                   pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg,
                   budget, hist>>

\* The daemon writes its buffer to storage and commits the size.
FlushBuf(n) ==
    /\ store' = [r \in Regions |-> IF buf[n][r] # 0 THEN buf[n][r] ELSE store[r]]
    /\ size' = Max(size, SetMax({r + 1 : r \in {x \in Regions : buf[n][x] # 0}}))
    /\ buf' = [buf EXCEPT ![n] = [r \in Regions |-> 0]]

\* close() of a handle; the last one flushes the buffer and leaves the registry, and a
\* file left open on one node returns to Exclusive.
Close(n) ==
    /\ hc[n] + hd[n] > 0 /\ Serving(n)
    /\ \E cachedHandle \in {b \in BOOLEAN : IF b THEN hc[n] > 0 ELSE hd[n] > 0} :
         IF cachedHandle THEN hc' = [hc EXCEPT ![n] = @ - 1] /\ UNCHANGED hd
                         ELSE hd' = [hd EXCEPT ![n] = @ - 1] /\ UNCHANGED hc
    /\ IF hc[n] + hd[n] = 1
       THEN /\ FlushBuf(n)
            /\ LET M == members \ {n}
                   W == wrote \ {n}
               IN /\ members' = M /\ wrote' = W
                  /\ mode' = AfterLeave(M, W)
            /\ iw' = [iw EXCEPT ![n] = FALSE]
            /\ hist' = [hist EXCEPT !.closed[n] = [r \in Regions |->
                           Max(@[r], SetMax({w.id : w \in {x \in hist.acked : x.n = n /\ x.r = r}}))],
                                    !.wr[n] = FALSE, !.bufd[n] = {}]
       ELSE UNCHANGED <<store, size, buf, members, wrote, mode, iw, hist>>
    /\ UNCHANGED <<sizeq, pend, epoch, grace, nextId, rmsg, amsg, pc, attr, dflags, oq, wq,
                   lease, paused, evicted, fenced, rereg, budget>>

-----------------------------------------------------------------------------
\* Recalls.

\* The recalled daemon flushes its buffer, drops the kernel's pages and attributes, opens
\* the file with direct I/O from now on and revalidates its old handles, then acknowledges.
Recalled(n, msg) ==
    /\ msg \in rmsg /\ msg.to = n /\ Serving(n)
    /\ rmsg' = rmsg \ {msg}
    /\ IF RecallFlush THEN FlushBuf(n) ELSE UNCHANGED <<store, size, buf>>
    /\ pc' = IF RecallDrop THEN [pc EXCEPT ![n] = [r \in Regions |-> NoPage]] ELSE pc
    /\ attr' = IF RecallDrop THEN [attr EXCEPT ![n] = FALSE] ELSE attr
    /\ dflags' = [dflags EXCEPT ![n] = [buffer |-> FALSE,
                                        direct |-> RecallDirect \/ @.direct,
                                        reval |-> RecallReval \/ @.reval]]
    /\ amsg' = amsg \cup {[from |-> n, ep |-> msg.ep, id |-> msg.id]}
    /\ hist' = IF RecallFlush THEN [hist EXCEPT !.bufd[n] = {}] ELSE hist
    /\ UNCHANGED <<sizeq, members, wrote, mode, pend, epoch, grace, nextId, hc, hd, iw, oq, wq,
                   lease, paused, evicted, fenced, rereg, budget>>

\* The owner takes an acknowledgment for the recall it names, from its own term.
AckRecv(a) ==
    /\ a \in amsg
    /\ amsg' = amsg \ {a}
    /\ pend' = IF /\ pend # <<>> /\ a.from \in DOMAIN pend.ids
                  /\ (~RecallIds \/ (a.ep = epoch /\ pend.ids[a.from] = a.id))
               THEN [pend EXCEPT !.ids = [m \in DOMAIN @ \ {a.from} |-> @[m]]]
               ELSE pend
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, epoch, grace, nextId, rmsg, hc, hd,
                   pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg,
                   budget, hist>>

\* (not NoTimeoutAnswer) the owner counts a recall as answered after a timeout alone.
TimeoutAnswer(m) ==
    /\ ~NoTimeoutAnswer /\ pend # <<>> /\ m \in DOMAIN pend.ids
    /\ pend' = [pend EXCEPT !.ids = [x \in DOMAIN @ \ {m} |-> @[x]]]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, epoch, grace, nextId, rmsg, amsg,
                   hc, hd, pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced,
                   rereg, budget, hist>>

\* Every recall answered: the file is WriteShared, and the open or write goes on.
Complete ==
    /\ pend # <<>> /\ DOMAIN pend.ids = {}
    /\ pend' = <<>>
    /\ LET n == pend.n IN
       IF pend.kind = "open"
       THEN /\ oq' = [oq EXCEPT ![n] = FALSE]
            /\ IF AnswerAfterRecall /\ ~fenced[n]
               THEN /\ Serving(n)
                    /\ members' = members \cup {n} /\ mode' = "WS"
                    /\ Opened(n, "WS", TRUE)
               ELSE UNCHANGED <<members, mode, hc, hd, pc, attr, dflags, hist>>
            /\ UNCHANGED <<wrote, iw, wq, store, size, sizeq, buf, budget>>
       ELSE /\ wq' = [wq EXCEPT ![n] = NoPage]
            /\ IF AnswerAfterRecall /\ Open(n) /\ ~fenced[n]
               THEN /\ Serving(n)
                    /\ wrote' = wrote \cup {n} /\ mode' = "WS"
                    /\ dflags' = [dflags EXCEPT ![n] = Flags("WS")]
                    /\ attr' = [attr EXCEPT ![n] = FALSE]
                    /\ iw' = [iw EXCEPT ![n] = TRUE]
                    /\ \E cachedHandle \in {b \in BOOLEAN : IF b THEN hc[n] > 0 ELSE hd[n] > 0} :
                         DoWrite(n, pend.r, cachedHandle, FALSE)
               ELSE UNCHANGED <<wrote, mode, dflags, attr, iw, store, size, sizeq, buf, pc, budget, hist>>
            /\ UNCHANGED <<members, oq, hc, hd>>
    /\ UNCHANGED <<epoch, grace, nextId, rmsg, amsg, lease, paused, evicted, fenced, rereg>>

\* A recall or an acknowledgment is lost.
Lose ==
    /\ budget.l > 0
    /\ \/ \E m \in rmsg : rmsg' = rmsg \ {m} /\ UNCHANGED amsg
       \/ \E a \in amsg : amsg' = amsg \ {a} /\ UNCHANGED rmsg
    /\ budget' = [budget EXCEPT !.l = @ - 1]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, hc, hd,
                   pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg, hist>>

-----------------------------------------------------------------------------
\* Sessions, pauses and the owner's failover.

\* A daemon pauses while its kernel and applications keep running, or runs again.
Pause(n) ==
    /\ IF paused[n] THEN UNCHANGED budget
       ELSE budget.p > 0 /\ budget' = [budget EXCEPT !.p = @ - 1]
    /\ paused' = [paused EXCEPT ![n] = ~@]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, pc, attr, dflags, buf, iw, oq, wq, lease, evicted, fenced,
                   rereg, hist>>

\* The session lease ends on the client (a pause, a partition, a control-plane outage, or
\* renewals stopped by an eviction). The attribute timeouts the daemon gave the kernel
\* end with it.
LeaseEnd(n) ==
    /\ lease[n] /\ ~fenced[n] /\ budget.x > 0
    /\ budget' = [budget EXCEPT !.x = @ - 1]
    /\ lease' = [lease EXCEPT ![n] = FALSE]
    /\ attr' = IF AttrWithinLease THEN [attr EXCEPT ![n] = FALSE] ELSE attr
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, pc, dflags, buf, iw, oq, wq, paused, evicted, fenced, rereg,
                   hist>>

\* A renewal is answered: the daemon drops the file's pages before it serves again.
Renew(n) ==
    /\ ~lease[n] /\ ~paused[n] /\ ~evicted[n] /\ ~fenced[n]
    /\ lease' = [lease EXCEPT ![n] = TRUE]
    /\ pc' = [pc EXCEPT ![n] = [r \in Regions |-> NoPage]]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, attr, dflags, buf, iw, oq, wq, paused, evicted, fenced, rereg,
                   budget, hist>>

\* The owner asks the control plane to evict a member that left a recall unanswered for
\* recall_timeout; the control plane stops renewing its session.
Evict(n) ==
    /\ pend # <<>> /\ n \in DOMAIN pend.ids /\ ~evicted[n]
    /\ evicted' = [evicted EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, pc, attr, dflags, buf, iw, oq, wq, lease, paused, fenced,
                   rereg, budget, hist>>

\* The fence is committed and acknowledged once the session's lease has run out (or, with
\* the rule off, at once on an eviction). Owners drop its opens and count its recalls as
\* answered; its buffered writes are lost, and reported to its handles as errors.
Fence(n) ==
    /\ ~fenced[n]
    /\ ~lease[n] \/ (~EvictWaitsLease /\ evicted[n])
    /\ fenced' = [fenced EXCEPT ![n] = TRUE]
    /\ LET M == members \ {n}
           W == wrote \ {n}
       IN /\ members' = M /\ wrote' = W
          /\ mode' = AfterLeave(M, W)
    /\ pend' = IF pend # <<>> /\ n \in DOMAIN pend.ids
               THEN [pend EXCEPT !.ids = [m \in DOMAIN @ \ {n} |-> @[m]]] ELSE pend
    /\ hist' = [hist EXCEPT !.failed = @ \cup hist.bufd[n], !.bufd[n] = {}]
    /\ UNCHANGED <<store, size, sizeq, epoch, grace, nextId, rmsg, amsg, hc, hd, pc, attr,
                   dflags, buf, iw, oq, wq, lease, paused, evicted, rereg, budget>>

\* The owner fails over: its registry and any recall in progress are lost, and the new
\* owner, in a newer term, starts a grace period. Waiting opens and writes are sent again.
OwnerFailover ==
    /\ budget.o > 0
    /\ budget' = [budget EXCEPT !.o = @ - 1]
    /\ epoch' = epoch + 1
    /\ members' = {} /\ wrote' = {} /\ mode' = "none" /\ pend' = <<>>
    /\ grace' = TRUE
    /\ amsg' = {}
    /\ rereg' = [n \in Nodes |-> Open(n) /\ ~fenced[n]]
    /\ oq' = [n \in Nodes |-> FALSE]
    /\ wq' = [n \in Nodes |-> NoPage]
    /\ UNCHANGED <<store, size, sizeq, nextId, rmsg, hc, hd, pc, attr, dflags, buf, iw,
                   lease, paused, evicted, fenced, hist>>

\* A node re-registers its open file with the new owner, with whether it has written.
Reregister(n) ==
    /\ rereg[n] /\ Serving(n)
    /\ rereg' = [rereg EXCEPT ![n] = FALSE]
    /\ members' = members \cup {n}
    /\ wrote' = IF iw[n] /\ ReregCarriesWrote THEN wrote \cup {n} ELSE wrote
    /\ mode' = IF grace THEN mode ELSE ModeOf(members', wrote')
    /\ UNCHANGED <<store, size, sizeq, pend, epoch, grace, nextId, rmsg, amsg, hc, hd, pc, attr,
                   dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, budget, hist>>

\* The grace period ends once every session that had the file open re-registered or was
\* fenced; the owner computes the mode from the registry.
EndGrace ==
    /\ grace
    /\ GraceUntilReregistered => \A n \in Nodes : ~rereg[n] \/ fenced[n]
    /\ grace' = FALSE
    /\ mode' = ModeOf(members, wrote)
    /\ UNCHANGED <<store, size, sizeq, members, wrote, pend, epoch, nextId, rmsg, amsg, hc, hd,
                   pc, attr, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced, rereg,
                   budget, hist>>

\* The kernel's attribute timeout runs out.
AttrExpire(n) ==
    /\ attr[n]
    /\ attr' = [attr EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<store, size, sizeq, members, wrote, mode, pend, epoch, grace, nextId, rmsg,
                   amsg, hc, hd, pc, dflags, buf, iw, oq, wq, lease, paused, evicted, fenced,
                   rereg, budget, hist>>

-----------------------------------------------------------------------------

Next ==
    \/ \E n \in Nodes, r \in Regions : ReadCached(n, r) \/ ReadDirect(n, r) \/ Write(n, r)
    \/ \E n \in Nodes : \/ OpenFirst(n) \/ OpenMore(n) \/ Close(n)
                        \/ Pause(n) \/ LeaseEnd(n) \/ Renew(n) \/ Evict(n) \/ Fence(n)
                        \/ Reregister(n) \/ AttrExpire(n) \/ TimeoutAnswer(n)
    \/ \E n \in Nodes, msg \in rmsg : Recalled(n, msg)
    \/ \E a \in amsg : AckRecv(a)
    \/ \E r \in Regions : CommitSize(r)
    \/ Complete \/ Lose \/ OwnerFailover \/ EndGrace

Spec == Init /\ [][Next]_vars

\* Nodes are interchangeable; configurations check up to their permutations.
NodeSymmetry == Permutations(Nodes)

-----------------------------------------------------------------------------
\* Properties.

TypeOK ==
    /\ mode \in {"none", "Ex", "RS", "WS"}
    /\ hc \in [Nodes -> 0 .. MaxHandles] /\ hd \in [Nodes -> 0 .. MaxHandles]
    /\ store \in [Regions -> 0 .. MaxWrites]

\* No stale read in WriteShared mode: while two nodes have the file open and one of them
\* has written, a read() on any handle returns the bytes of every write acknowledged
\* before it, or newer ones.
NoStaleReadWriteShared == ~hist.staleWS

\* Close-to-open: an open that starts after another node's close sees that close's data.
CloseToOpen == ~hist.staleCTO

=============================================================================
