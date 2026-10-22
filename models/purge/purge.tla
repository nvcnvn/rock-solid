-------------------------------- MODULE purge --------------------------------
\* Freeing space (docs/design.md): the inodes of one slot, their sizes, alloc_end and
\* chunk bytes, the slot's purge list and its purger, writes from client daemons on
\* several nodes, truncates, unlinks and orphans, an inode owner that fails over, sessions
\* that are fenced, and crashes of daemons and of the purger.
\*
\* Bytes hold 0 or 1 (non-zero). A chunk is ChunkBlocks blocks of BlockBytes bytes, and a
\* missing chunk reads as zeros. Chunk writes and the purger's punches travel as messages
\* that can arrive late; MetaStore transactions are single steps. Each rule the design
\* states is a BOOLEAN constant: the ci and deep configurations set every rule TRUE (and
\* DaemonPunch and MemoryTail, which are not rules, FALSE), and each negative configuration
\* changes one.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Inodes,         \* inode numbers of one slot, which order the purge list's keys
    Nodes,          \* client daemons
    BlockBytes, ChunkBlocks, Chunks,
    InitSize,       \* every file starts this many bytes long, its bytes written
    WriteOffsets,   \* the bytes a write may target
    TruncSizes,     \* the sizes a truncate may set
    MaxWrites, MaxTruncates, MaxUnlinks, MaxCrashes, MaxFences, MaxFailovers

CONSTANTS
    TruncEntryAtByte,       \* a truncate's entry starts at the new size, not the next chunk
    MergePartialBlock,      \* the first punch zeroes the rest of the partial block too
    MemoryTail,             \* (not a rule) the partial block's tail is zeroed only in memory
    DaemonPunch,            \* (not a rule) the truncating daemon sends the first punch itself
    ExtendFinishesPurge,    \* an extension waits for a pending purge of its inode to finish
    RewriteAfterCheck,      \* a size commit whose check fails writes the chunk again
    ZeroGapOnExtend,        \* an extension zeroes the gap between the old size and its start
    RaiseBeforeWrite,       \* alloc_end is raised before the chunk write, not after
    MissingPunchSucceeds,   \* a punch to a missing chunk succeeds
    PurgerTermFence,        \* a primary rejects punches from an older term of the purger
    IncarnationFence,       \* a crashed daemon's writes are fenced before its files are released
    OrphanWhileOpen,        \* unlink of an open file makes an orphan
    OrphanAfterGrace,       \* an orphan moves to the purge list only after the grace period
    GraceUntilReregistered, \* the grace period lasts until every live session re-registered
    ReleaseOnAck,           \* a fenced session's opens count as closed at its acknowledgment
    MergeEntries,           \* a new purge range for an inode is merged with its pending one
    CheckedRemoval          \* the purger's range removal checks each entry and each gap

ChunkBytes == BlockBytes * ChunkBlocks
FileBytes == ChunkBytes * Chunks
Bytes == 0 .. FileBytes - 1
ChunkIds == 0 .. Chunks - 1
ChunkOf(b) == b \div ChunkBytes
CeilChunk(s) == (s + ChunkBytes - 1) \div ChunkBytes
NextBlock(b) == ((b + BlockBytes - 1) \div BlockBytes) * BlockBytes
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y
SetMax(S) == CHOOSE x \in S : \A y \in S : x >= y

None == <<>>
\* A purge entry <<s, e>> covers byte s up to the end of chunk e - 1.
Overlaps(en, lo, hi) == en # None /\ en[1] < hi /\ lo < en[2] * ChunkBytes
NPunches(en) == IF en[1] >= en[2] * ChunkBytes THEN 0 ELSE en[2] - ChunkOf(en[1])
\* Punch k of entry en zeroes chunk ChunkOf(en[1]) + k from byte PunchFrom(en, k) to its end:
\* the first merges the partial block holding the entry's start, the rest delete whole chunks.
PunchFrom(en, k) ==
    IF k = 0 THEN (IF MergePartialBlock THEN en[1] ELSE NextBlock(en[1]))
    ELSE (ChunkOf(en[1]) + k) * ChunkBytes

VARIABLES
    size, allocEnd,     \* inode attributes in the MetaStore
    aver,               \* changes with every write of the attributes, as mtime does
    linked,             \* the inode has a directory entry
    orphan,             \* unlinked while open, not yet on the purge list
    gone,               \* the inode's keys are removed: unlinked and on the purge list
    data, exists,       \* chunk bytes on disk, and which chunks exist
    seen,               \* per chunk, the newest purger term its primary has seen
    entry,              \* the slot's purge list: inode -> <<start byte, end chunk>> or None
    tailPunch,          \* (DaemonPunch) a first punch its daemon still owes: <<s>> or None
    memTail,            \* (MemoryTail) a partial block's tail zeroed only in memory
    writes,             \* writes in progress
    punches,            \* the purger's punches in flight
    opens,              \* node -> inodes it has open
    reg,                \* the owner's open registry: inode -> nodes
    grace, rereg,       \* the owner's grace period, and who re-registered in it
    fence,              \* node -> "live", "committed", "applied" (by storage) or "acked"
    inc,                \* node -> "ok", or "fencing" while a crashed daemon's writes may land
    purger,             \* the purger: [on, term, read, run, done, step, wait]
    budget,             \* operations and faults left
    ghost,              \* history: per byte, cut by a truncate or unlink, junk, acknowledged
    flags               \* history: rerunFailed, lost, exposed

vars == <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry, tailPunch,
          memTail, writes, punches, opens, reg, grace, rereg, fence, inc, purger, budget,
          ghost, flags>>

Init ==
    /\ size = [i \in Inodes |-> InitSize]
    /\ allocEnd = [i \in Inodes |-> CeilChunk(InitSize)]
    /\ aver = [i \in Inodes |-> 0]
    /\ linked = [i \in Inodes |-> TRUE]
    /\ orphan = [i \in Inodes |-> FALSE]
    /\ gone = [i \in Inodes |-> FALSE]
    /\ data = [i \in Inodes |-> [b \in Bytes |-> IF b < InitSize THEN 1 ELSE 0]]
    /\ exists = [i \in Inodes |-> [c \in ChunkIds |-> c < CeilChunk(InitSize)]]
    /\ seen = [i \in Inodes |-> [c \in ChunkIds |-> 0]]
    /\ entry = [i \in Inodes |-> None]
    /\ tailPunch = [i \in Inodes |-> None]
    /\ memTail = [i \in Inodes |-> None]
    /\ writes = {}
    /\ punches = {}
    /\ opens = [n \in Nodes |-> {}]
    /\ reg = [i \in Inodes |-> {}]
    /\ grace = FALSE
    /\ rereg = {}
    /\ fence = [n \in Nodes |-> "live"]
    /\ inc = [n \in Nodes |-> "ok"]
    /\ purger = [on |-> FALSE, term |-> 1, read |-> [i \in Inodes |-> None], run |-> {},
                 done |-> {}, step |-> 0, wait |-> FALSE]
    /\ budget = [w |-> MaxWrites, t |-> MaxTruncates, u |-> MaxUnlinks, c |-> MaxCrashes,
                 f |-> MaxFences, o |-> MaxFailovers]
    /\ ghost = [cut |-> [i \in Inodes |-> [b \in Bytes |-> FALSE]],
                junk |-> [i \in Inodes |-> [b \in Bytes |-> FALSE]],
                acked |-> [i \in Inodes |-> [b \in Bytes |-> b < InitSize]]]
    /\ flags = [rerunFailed |-> FALSE, lost |-> FALSE, exposed |-> FALSE]

Merge(old, s, e) ==
    IF old = None \/ ~MergeEntries THEN <<s, e>> ELSE <<Min(old[1], s), Max(old[2], e)>>

InRange(b, lo, hi) == lo <= b /\ b < hi
\* Bytes lo .. hi - 1 of inode i zeroed on disk; zeroing an acknowledged byte loses a write.
ZeroData(i, lo, hi) == [data EXCEPT ![i] = [b \in Bytes |-> IF InRange(b, lo, hi) THEN 0 ELSE @[b]]]
LosesAcked(i, lo, hi) == \E b \in Bytes : InRange(b, lo, hi) /\ ghost.acked[i][b] /\ data[i][b] = 1

\* A punch reaching chunk c of inode i's primary: it zeroes the chunk from byte lo, and
\* deletes it when lo is the chunk's first byte. A missing chunk takes it as done.
ApplyPunch(i, c, lo) ==
    IF ~exists[i][c]
    THEN /\ flags' = [flags EXCEPT !.rerunFailed = @ \/ ~MissingPunchSucceeds]
         /\ UNCHANGED <<data, exists>>
    ELSE /\ data' = ZeroData(i, lo, (c + 1) * ChunkBytes)
         /\ exists' = [exists EXCEPT ![i][c] = lo > c * ChunkBytes]
         /\ flags' = [flags EXCEPT !.lost = @ \/ LosesAcked(i, lo, (c + 1) * ChunkBytes)]

\* The gap an extension opens, zeroed by a punch below alloc_end.
GapData(i, lo, hi) == IF ZeroGapOnExtend THEN ZeroData(i, lo, hi) ELSE data
Exposes(d, i, lo, hi) ==
    \E b \in Bytes : InRange(b, lo, hi) /\ d[i][b] = 1 /\ (ghost.cut[i][b] \/ ghost.junk[i][b])

Live(n) == fence[n] = "live"
Ready(n) == Live(n) /\ inc[n] = "ok"

-----------------------------------------------------------------------------
\* Client operations.

\* open() registers with the inode owner, which takes no new opens in its grace period.
Open(n, i) ==
    /\ linked[i] /\ Ready(n) /\ ~grace /\ i \notin opens[n]
    /\ opens' = [opens EXCEPT ![n] = @ \cup {i}]
    /\ reg' = [reg EXCEPT ![i] = @ \cup {n}]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, grace, rereg, fence, inc, purger,
                   budget, ghost, flags>>

\* The last close of a file on a node. The kernel sends it only once the node's writes to
\* it have returned, and a successor sends none while its predecessor's writes may land.
Close(n, i) ==
    /\ i \in opens[n] /\ inc[n] = "ok"
    /\ ~\E w \in writes : w.n = n /\ w.i = i /\ ~w.dead
    /\ opens' = [opens EXCEPT ![n] = @ \ {i}]
    /\ reg' = [reg EXCEPT ![i] = @ \ {n}]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, grace, rereg, fence, inc, purger,
                   budget, ghost, flags>>

\* A write of byte b starts by reading the inode. An extension (past the size, or a chunk
\* at or past alloc_end) waits while a purge of the inode is pending.
StartWrite(n, i, b) ==
    /\ i \in opens[n] /\ Ready(n) /\ budget.w > 0 /\ ~gone[i]
    /\ ~(ExtendFinishesPurge /\ entry[i] # None /\ (b >= size[i] \/ ChunkOf(b) >= allocEnd[i]))
    /\ writes' = writes \cup {[id |-> budget.w, n |-> n, i |-> i, b |-> b, ext |-> b >= size[i],
                               av |-> aver[i], dead |-> FALSE,
                               st |-> IF RaiseBeforeWrite THEN "raise" ELSE "land"]}
    /\ budget' = [budget EXCEPT !.w = @ - 1]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, punches, opens, reg, grace, rereg, fence, inc, purger,
                   ghost, flags>>

AfterRaise(w) == IF RaiseBeforeWrite THEN "land" ELSE IF w.ext THEN "size" ELSE "done"
AfterLand(w) == IF w.dead THEN "done" ELSE IF ~RaiseBeforeWrite THEN "raise" ELSE IF w.ext THEN "size" ELSE "done"
Advance(w, st) == IF st = "done" THEN writes \ {w} ELSE (writes \ {w}) \cup {[w EXCEPT !.st = st]}

\* Before writing a chunk at or past alloc_end, the daemon raises it in a MetaStore
\* transaction, to any later chunk (the design's doubling steps are one such sequence).
Raise(w) ==
    /\ w \in writes /\ w.st = "raise"
    /\ IF gone[w.i]
       THEN writes' = writes \ {w} /\ UNCHANGED <<allocEnd, aver>>
       ELSE IF ChunkOf(w.b) < allocEnd[w.i]
       THEN writes' = Advance(w, AfterRaise(w)) /\ UNCHANGED <<allocEnd, aver>>
       ELSE /\ ~(ExtendFinishesPurge /\ entry[w.i] # None)
            /\ \E e \in ChunkOf(w.b) + 1 .. Chunks : allocEnd' = [allocEnd EXCEPT ![w.i] = e]
            /\ aver' = [aver EXCEPT ![w.i] = @ + 1]
            /\ writes' = Advance(w, AfterRaise(w))
    /\ UNCHANGED <<size, linked, orphan, gone, data, exists, seen, entry, tailPunch, memTail,
                   punches, opens, reg, grace, rereg, fence, inc, purger, budget, ghost, flags>>

\* The chunk write reaches its primary, which rejects it once its storage node applied
\* the session's fence. A write already sent outlives its daemon's crash. A write inside
\* the size is acknowledged here; one that finds the file cut below it lands past the size.
Land(w) ==
    /\ w \in writes /\ w.st = "land"
    /\ IF fence[w.n] \in {"applied", "acked"}
       THEN writes' = writes \ {w} /\ UNCHANGED <<data, exists, ghost>>
       ELSE /\ data' = [data EXCEPT ![w.i][w.b] = 1]
            /\ exists' = [exists EXCEPT ![w.i][ChunkOf(w.b)] = TRUE]
            /\ ghost' = [ghost EXCEPT !.cut[w.i][w.b] = FALSE,
                                      !.junk[w.i][w.b] = ~w.ext /\ w.b >= size[w.i],
                                      !.acked[w.i][w.b] = ~w.ext /\ ~w.dead /\ w.b < size[w.i]]
            /\ writes' = Advance(w, AfterLand(w))
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, seen, entry, tailPunch, memTail,
                   punches, opens, reg, grace, rereg, fence, inc, purger, budget, flags>>

\* An extending write commits the new size with a check on the attributes it read, then
\* is acknowledged. It waits for a pending purge over its range. If the check fails, it
\* reads again and sends its chunk write again, since a truncate or another extension
\* may have zeroed its byte meanwhile. The gap below its byte is zeroed first.
CommitSize(w) ==
    /\ w \in writes /\ w.st = "size"
    /\ IF gone[w.i]
       THEN writes' = writes \ {w} /\ UNCHANGED <<size, aver, data, ghost, flags>>
       ELSE /\ ~(ExtendFinishesPurge /\ Overlaps(entry[w.i], size[w.i], w.b + 1))
            /\ IF RewriteAfterCheck /\ aver[w.i] # w.av
               THEN /\ writes' = (writes \ {w}) \cup {[w EXCEPT !.st = "land", !.av = aver[w.i]]}
                    /\ UNCHANGED <<size, aver, data, ghost, flags>>
               ELSE LET d == GapData(w.i, size[w.i], w.b)
                    IN /\ data' = d
                       /\ size' = [size EXCEPT ![w.i] = Max(@, w.b + 1)]
                       /\ aver' = [aver EXCEPT ![w.i] = @ + 1]
                       /\ ghost' = [ghost EXCEPT !.acked[w.i][w.b] = TRUE]
                       /\ flags' = [flags EXCEPT !.lost = @ \/ data[w.i][w.b] = 0
                                                            \/ LosesAcked(w.i, size[w.i], w.b),
                                                 !.exposed = @ \/ Exposes(d, w.i, size[w.i], w.b)]
                       /\ writes' = writes \ {w}
    /\ UNCHANGED <<allocEnd, linked, orphan, gone, exists, seen, entry, tailPunch, memTail,
                   punches, opens, reg, grace, rereg, fence, inc, purger, budget>>

\* truncate() down: one transaction commits the new size, alloc_end lowered to the new
\* end and a purge range from the new size to the old alloc_end, merged into a pending one.
TruncateDown(i, s) ==
    /\ ~gone[i] /\ budget.t > 0 /\ s < size[i]
    /\ LET start == IF TruncEntryAtByte THEN s ELSE CeilChunk(s) * ChunkBytes
       IN /\ entry' = [entry EXCEPT ![i] = Merge(@, start, allocEnd[i])]
          /\ tailPunch' = [tailPunch EXCEPT ![i] =
                             IF DaemonPunch /\ start < FileBytes THEN <<start>> ELSE None]
    /\ memTail' = [memTail EXCEPT ![i] =
                     IF MemoryTail /\ s # NextBlock(s) THEN <<s, NextBlock(s)>> ELSE None]
    /\ size' = [size EXCEPT ![i] = s]
    /\ allocEnd' = [allocEnd EXCEPT ![i] = CeilChunk(s)]
    /\ aver' = [aver EXCEPT ![i] = @ + 1]
    /\ ghost' = [ghost EXCEPT
                   !.cut[i] = [b \in Bytes |-> @[b] \/ (b >= s /\ data[i][b] = 1)],
                   !.acked[i] = [b \in Bytes |-> @[b] /\ b < s]]
    /\ budget' = [budget EXCEPT !.t = @ - 1]
    /\ UNCHANGED <<linked, orphan, gone, data, exists, seen, writes, punches, opens, reg, grace,
                   rereg, fence, inc, purger, flags>>

\* truncate() up: waits for a pending purge, zeroes the gap and commits the size.
TruncateUp(i, s) ==
    /\ ~gone[i] /\ budget.t > 0 /\ s > size[i]
    /\ ~(ExtendFinishesPurge /\ entry[i] # None)
    /\ LET d == GapData(i, size[i], s)
       IN /\ data' = d
          /\ flags' = [flags EXCEPT !.exposed = @ \/ Exposes(d, i, size[i], s),
                                    !.lost = @ \/ LosesAcked(i, size[i], s)]
    /\ size' = [size EXCEPT ![i] = s]
    /\ aver' = [aver EXCEPT ![i] = @ + 1]
    /\ budget' = [budget EXCEPT !.t = @ - 1]
    /\ UNCHANGED <<allocEnd, linked, orphan, gone, exists, seen, entry, tailPunch, memTail,
                   writes, punches, opens, reg, grace, rereg, fence, inc, purger, ghost>>

\* (DaemonPunch) the truncating daemon's own first punch reaches the primary, possibly
\* after the purger finished the entry.
TailPunch(i) ==
    /\ tailPunch[i] # None
    /\ tailPunch' = [tailPunch EXCEPT ![i] = None]
    /\ ApplyPunch(i, ChunkOf(tailPunch[i][1]), PunchFrom(<<tailPunch[i][1], 0>>, 0))
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, seen, entry, memTail, writes,
                   punches, opens, reg, grace, rereg, fence, inc, purger, budget, ghost>>

\* (MemoryTail) the tail zeroed in memory reaches the disk.
MemTailFlush(i) ==
    /\ memTail[i] # None
    /\ data' = ZeroData(i, memTail[i][1], memTail[i][2])
    /\ memTail' = [memTail EXCEPT ![i] = None]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, exists, seen, entry, tailPunch,
                   writes, punches, opens, reg, grace, rereg, fence, inc, purger, budget,
                   ghost, flags>>

CutAll(i) == [ghost EXCEPT !.cut[i] = [b \in Bytes |-> @[b] \/ data[i][b] = 1],
                           !.acked[i] = [b \in Bytes |-> FALSE]]

\* Removing the last link: if the owner sees the file open (or is in its grace period),
\* the inode becomes an orphan; otherwise one transaction removes its keys and adds a
\* purge entry for chunks 0 .. alloc_end.
Unlink(i) ==
    /\ linked[i] /\ budget.u > 0
    /\ linked' = [linked EXCEPT ![i] = FALSE]
    /\ budget' = [budget EXCEPT !.u = @ - 1]
    /\ IF OrphanWhileOpen /\ (reg[i] # {} \/ grace)
       THEN /\ orphan' = [orphan EXCEPT ![i] = TRUE]
            /\ UNCHANGED <<gone, entry, ghost>>
       ELSE /\ gone' = [gone EXCEPT ![i] = TRUE]
            /\ entry' = [entry EXCEPT ![i] = Merge(@, 0, allocEnd[i])]
            /\ ghost' = CutAll(i)
            /\ UNCHANGED orphan
    /\ UNCHANGED <<size, allocEnd, aver, data, exists, seen, tailPunch, memTail, writes, punches,
                   opens, reg, grace, rereg, fence, inc, purger, flags>>

\* The owner moves an orphan to the purge list once no node has it open, and only after
\* its grace period.
FreeOrphan(i) ==
    /\ orphan[i] /\ reg[i] = {}
    /\ ~(OrphanAfterGrace /\ grace)
    /\ orphan' = [orphan EXCEPT ![i] = FALSE]
    /\ gone' = [gone EXCEPT ![i] = TRUE]
    /\ entry' = [entry EXCEPT ![i] = Merge(@, 0, allocEnd[i])]
    /\ ghost' = CutAll(i)
    /\ UNCHANGED <<size, allocEnd, aver, linked, data, exists, seen, tailPunch, memTail, writes,
                   punches, opens, reg, grace, rereg, fence, inc, purger, budget, flags>>

-----------------------------------------------------------------------------
\* Sessions, the owner and crashes.

\* The control plane commits a fence of n's session (its lease has run out, so n issues
\* nothing new), storage nodes apply it, and it is committed as acknowledged.
FenceCommit(n) ==
    /\ fence[n] = "live" /\ budget.f > 0
    /\ fence' = [fence EXCEPT ![n] = "committed"]
    /\ budget' = [budget EXCEPT !.f = @ - 1]
    /\ reg' = IF ReleaseOnAck THEN reg ELSE [i \in Inodes |-> reg[i] \ {n}]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, grace, rereg, inc, purger,
                   ghost, flags>>

FenceApply(n) ==
    /\ fence[n] = "committed"
    /\ fence' = [fence EXCEPT ![n] = "applied"]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, reg, grace, rereg, inc, purger,
                   budget, ghost, flags>>

\* Once acknowledged, the session leaves the table, and owners drop its opens.
FenceAck(n) ==
    /\ fence[n] = "applied"
    /\ fence' = [fence EXCEPT ![n] = "acked"]
    /\ reg' = [i \in Inodes |-> reg[i] \ {n}]
    /\ opens' = [opens EXCEPT ![n] = {}]
    /\ writes' = {w \in writes : w.n # n}
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, punches, grace, rereg, inc, purger, budget, ghost, flags>>

\* The inode owner fails over: its open registry is lost and a grace period starts.
OwnerFailover ==
    /\ budget.o > 0
    /\ reg' = [i \in Inodes |-> {}]
    /\ grace' = TRUE
    /\ rereg' = {}
    /\ budget' = [budget EXCEPT !.o = @ - 1]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, fence, inc, purger, ghost, flags>>

\* A live session re-registers its open files with the new owner.
Reregister(n) ==
    /\ grace /\ n \notin rereg /\ Live(n)
    /\ reg' = [i \in Inodes |-> IF i \in opens[n] THEN reg[i] \cup {n} ELSE reg[i]]
    /\ rereg' = rereg \cup {n}
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, grace, fence, inc, purger,
                   budget, ghost, flags>>

\* The grace period ends once every session in the table re-registered or left it.
EndGrace ==
    /\ grace
    /\ GraceUntilReregistered => \A n \in Nodes : n \in rereg \/ fence[n] = "acked"
    /\ grace' = FALSE
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, reg, rereg, fence, inc, purger,
                   budget, ghost, flags>>

\* A daemon crash loses what it held in memory: writes not yet sent and a tail zeroed in
\* memory. Its chunk writes already sent can still land. Its successor resumes the session
\* and the open handles under a new incarnation.
CrashDaemon(n) ==
    /\ budget.c > 0 /\ fence[n] # "acked"
    /\ writes' = {w \in writes : w.n # n} \cup
                 {[w EXCEPT !.dead = TRUE] : w \in {x \in writes : x.n = n /\ x.st = "land"}}
    /\ tailPunch' = [i \in Inodes |-> None]
    /\ memTail' = [i \in Inodes |-> None]
    /\ inc' = [inc EXCEPT ![n] = IF IncarnationFence THEN "fencing" ELSE "ok"]
    /\ budget' = [budget EXCEPT !.c = @ - 1]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   punches, opens, reg, grace, rereg, fence, purger, ghost, flags>>

\* The successor's incarnation is committed in the session table, every storage node
\* applies it and rejects the old incarnation's writes from then on, queued ones included,
\* and the version is acknowledged.
FenceIncarnation(n) ==
    /\ inc[n] = "fencing"
    /\ inc' = [inc EXCEPT ![n] = "ok"]
    /\ writes' = {w \in writes : w.n # n \/ ~w.dead}
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, punches, opens, reg, grace, rereg, fence, purger, budget,
                   ghost, flags>>

-----------------------------------------------------------------------------
\* The purger: the slot's shard leader works through the purge list, one punch at a time.

\* It reads the list and takes the run of every pending entry, in key order.
Scan ==
    /\ ~purger.on
    /\ \E i \in Inodes : entry[i] # None
    /\ purger' = [purger EXCEPT !.on = TRUE, !.read = entry,
                                !.run = {i \in Inodes : entry[i] # None},
                                !.done = {}, !.step = 0, !.wait = FALSE]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, reg, grace, rereg, fence, inc,
                   budget, ghost, flags>>

\* It sends the next punch of the lowest entry of the run it has not finished, tagged with
\* its term, and waits for it to complete.
PurgeSend ==
    /\ purger.on /\ ~purger.wait /\ purger.run # purger.done
    /\ LET i == SetMin(purger.run \ purger.done)
           en == purger.read[i]
       IN IF purger.step < NPunches(en)
          THEN /\ punches' = punches \cup {[i |-> i, c |-> ChunkOf(en[1]) + purger.step,
                                            lo |-> PunchFrom(en, purger.step), term |-> purger.term]}
               /\ purger' = [purger EXCEPT !.step = @ + 1, !.wait = TRUE]
          ELSE /\ purger' = [purger EXCEPT !.done = @ \cup {i}, !.step = 0]
               /\ UNCHANGED punches
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, opens, reg, grace, rereg, fence, inc, budget,
                   ghost, flags>>

\* A punch reaches its primary, which rejects it if it has seen a newer purger term.
PunchLand(p) ==
    /\ p \in punches
    /\ punches' = punches \ {p}
    /\ IF PurgerTermFence /\ p.term < seen[p.i][p.c]
       THEN UNCHANGED <<data, exists, seen, flags>>
       ELSE /\ ApplyPunch(p.i, p.c, p.lo)
            /\ seen' = [seen EXCEPT ![p.i][p.c] = Max(@, p.term)]
    /\ purger' = IF purger.on /\ p.term = purger.term THEN [purger EXCEPT !.wait = FALSE] ELSE purger
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, entry, tailPunch, memTail, writes,
                   opens, reg, grace, rereg, fence, inc, budget, ghost>>

\* It removes the consumed run with one range removal, checking that each entry still holds
\* what it read and that each gap between them is empty. A failed check removes nothing,
\* and the purger reads the list again.
RemoveRun ==
    /\ purger.on /\ ~purger.wait /\ purger.run = purger.done
    /\ LET lo == SetMin(purger.run)
           hi == SetMax(purger.run)
           inRun == {i \in Inodes : lo <= i /\ i <= hi}
           ok == /\ \A i \in purger.run : entry[i] = purger.read[i]
                 /\ \A i \in inRun \ purger.run : entry[i] = None
       IN entry' = IF ~CheckedRemoval \/ ok
                   THEN [i \in Inodes |-> IF i \in inRun THEN None ELSE entry[i]]
                   ELSE entry
    /\ purger' = [purger EXCEPT !.on = FALSE]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, tailPunch,
                   memTail, writes, punches, opens, reg, grace, rereg, fence, inc, budget,
                   ghost, flags>>

\* The purger's shard leader crashes or loses its term; the next leader, in a newer term,
\* starts again from the list. A punch the old one sent can still arrive.
CrashPurger ==
    /\ purger.on /\ budget.c > 0
    /\ purger' = [purger EXCEPT !.on = FALSE, !.term = @ + 1, !.wait = FALSE]
    /\ budget' = [budget EXCEPT !.c = @ - 1]
    /\ UNCHANGED <<size, allocEnd, aver, linked, orphan, gone, data, exists, seen, entry,
                   tailPunch, memTail, writes, punches, opens, reg, grace, rereg, fence, inc,
                   ghost, flags>>

-----------------------------------------------------------------------------

Next ==
    \/ \E n \in Nodes, i \in Inodes : Open(n, i) \/ Close(n, i)
    \/ \E n \in Nodes, i \in Inodes, b \in WriteOffsets : StartWrite(n, i, b)
    \/ \E w \in writes : Raise(w) \/ Land(w) \/ CommitSize(w)
    \/ \E i \in Inodes, s \in TruncSizes : TruncateDown(i, s) \/ TruncateUp(i, s)
    \/ \E i \in Inodes : TailPunch(i) \/ MemTailFlush(i) \/ Unlink(i) \/ FreeOrphan(i)
    \/ \E n \in Nodes : \/ FenceCommit(n) \/ FenceApply(n) \/ FenceAck(n)
                        \/ Reregister(n) \/ CrashDaemon(n) \/ FenceIncarnation(n)
    \/ OwnerFailover \/ EndGrace
    \/ Scan \/ PurgeSend \/ RemoveRun \/ CrashPurger
    \/ \E p \in punches : PunchLand(p)

Spec == Init /\ [][Next]_vars

\* Liveness needs the purger to keep running and its punches to arrive.
FairSpec ==
    /\ Spec
    /\ WF_vars(Scan) /\ WF_vars(PurgeSend) /\ WF_vars(RemoveRun)
    /\ WF_vars(\E p \in punches : PunchLand(p))

\* Daemons are interchangeable; ci configurations check up to their permutations.
NodeSymmetry == Permutations(Nodes)

-----------------------------------------------------------------------------
\* Properties.

TypeOK ==
    /\ size \in [Inodes -> 0 .. FileBytes]
    /\ allocEnd \in [Inodes -> 0 .. Chunks]
    /\ data \in [Inodes -> [Bytes -> {0, 1}]]
    /\ fence \in [Nodes -> {"live", "committed", "applied", "acked"}]
    /\ inc \in [Nodes -> {"ok", "fencing"}]

\* An interrupted purge reruns safely: running an entry again after any prefix of its
\* punches never fails, so it ends as a single run would.
RerunSafe == ~flags.rerunFailed

\* A purge never deletes newer data: no acknowledged byte is zeroed except by a truncate
\* or unlink that cut it, and an extending write's byte is on disk when it is acknowledged.
NeverDeletesNewer == ~flags.lost

\* Once a file's purge has run, no byte a truncate or unlink cut off is non-zero on disk.
TruncatedBytesZero ==
    \A i \in Inodes :
        (entry[i] = None /\ tailPunch[i] = None /\ memTail[i] = None)
            => \A b \in Bytes : ~(ghost.cut[i][b] /\ data[i][b] = 1)

\* An extension never exposes a byte a truncate cut off, nor one a write that read an
\* older size landed past the size.
ExtensionNeverExposes == ~flags.exposed

\* Once a deleted inode's purge has settled, none of its chunks remains.
NoChunkOfDeletedInode ==
    \A i \in Inodes : (gone[i] /\ entry[i] = None) => \A c \in ChunkIds : ~exists[i][c]

\* An orphaned inode is never freed while a live session has it open.
OrphanNotFreedWhileOpen == \A i \in Inodes : gone[i] => \A n \in Nodes : i \notin opens[n]

\* Every purge entry is eventually removed (deep only).
PurgeSettles == \A i \in Inodes : [](entry[i] # None => <>(entry[i] = None))

=============================================================================
