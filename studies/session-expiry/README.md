# Session expiry: who decides, and how the fence is published

**Status:** read from documentation and source, no experiment. The proposed design at the end is adopted: [Sessions and fencing](../../docs/design.md#sessions-and-fencing) holds the current text, which refers to `lease_drift` by name instead of restating its bound.

**Question:** a client daemon renews its session with the inode owners it talks to ([Sessions and fencing](../../docs/design.md#sessions-and-fencing)). When the session expires, the control plane fences it, every chunk primary acknowledges, and only then do owners release its locks. The design does not say where the session's authoritative lease lives when the client renews with owners on several shards, who declares it expired, how that reaches the control plane, what happens when owners disagree or fail over, how a client that reaches one owner but not another is treated, or what renewal costs. Blog posts [03](../../blogs/03-epochs-leases-and-failures.md#6-fencing-client-sessions) and [09](../../blogs/09-rwx-filesystem-and-cache-coherence.md#sessions-and-fencing) draw that hop as unspecified.

## Summary

Every system below that fences clients cluster-wide has **one authority declare the expiry**, even where several servers notice it. CephFS lets each metadata server (MDS) time out its own sessions, but the eviction is a blocklist entry committed by the monitors into the OSD map, so the first server to give up evicts the client everywhere. Chubby, ZooKeeper, etcd and HDFS keep each session's lease in their leader's memory, write only creation and expiry to their replicated log, and give every session a fresh full lease when a new leader takes over. Systems whose servers evict independently (Lustre targets, an NFS server) hold the client's locks and the data those locks protect on the same server, so a local eviction is a complete fence there. rock-solid separates the two (locks at inode owners, data at chunk primaries), so it is in CephFS's position and needs a cluster-wide verdict.

The proposal: **the control plane owns every session.** The record is a key in the control-plane group; its lease is renewed by the heartbeat each node already sends the control-plane leader every second; the leader declares expiry on its own clock and commits the fence in the same group; storage nodes acknowledge a versioned session table; owners act only on a committed acknowledgment watermark and never decide expiry themselves. Renewal then costs one message per node per second, whatever the shard count, and no Raft write.

## What each system does

| System | Where the lease lives | Who declares expiry | How the fence is published and acknowledged | Failover | Per-client renewal cost |
| --- | --- | --- | --- | --- | --- |
| CephFS | One session per client per MDS rank, timers in each MDS's memory | Each rank, on its own timer; first the session goes stale, then it is evicted | MDS asks the monitors to blocklist the client's address in the OSD map, waits until it has that map, then sets an OSD epoch barrier sent on every capability message; no per-OSD acknowledgment | New MDS waits 45 s for clients to reconnect, evicts the rest | One renew message per MDS rank every `session_timeout / 3` (20 s) |
| NFSv4.1 (Linux nfsd) | One lease per client ID per server, covering all its state there | The server | No fence beyond the server itself: locks and data are on it | Server restart: grace period for reclaims | Any request renews; idle clients send one per lease period |
| Chubby | Session in the master's memory, recorded in its database | Master | Sequencers checked by servers, or a lock-delay of up to a minute where servers cannot check | New master extends every lease to the maximum the old master may have granted; client in jeopardy waits a 45 s grace period | One KeepAlive per lease (12 s default, up to 60 s under load); 90,000 clients on one master |
| ZooKeeper | Leader's session tracker, in memory; creation and close in the log | Leader, which proposes `closeSession` | None for outside resources | New leader tracks every session from the snapshot with a full timeout | Client pings any server; followers forward touched sessions to the leader on their own ping |
| etcd | Leader's lessor, in memory; remaining TTL checkpointed every 5 min | Leader, via a revoke proposal | None: its own lock example says a lease alone cannot give mutual exclusion and needs a token check at the resource | New leader extends every lease by an election timeout | Keepalives per lease, forwarded to the leader |
| HDFS | One lease per client at the NameNode, covering every file it writes | NameNode | Lease recovery bumps the last block's generation stamp at the DataNodes | New active NameNode renews every lease afresh | One renew per client per 30 s |
| Kubernetes nodes | One `Lease` object per node in `kube-node-lease` | Node controller marks the node `Unknown` | None: volumes are force-detached after 6 min with a documented corruption risk, or an operator taints the node out of service after checking it is off | Leases are API objects, so they survive | One update per node per renewal |
| Lustre | Each server target tracks each client export | Each target, on its own: ping evictor or lock callback timeout | None beyond the target, which holds both the locks and the objects | Targets recover with client replay | One ping per target per 25 s; `suppress_pings` exists because this grows with targets |

### CephFS

- **Each MDS rank runs its own timer.** The client sends a renew to every MDS session it holds whenever `session_timeout / 3` has passed (`src/client/Client.cc:7545`, `renew_caps` loops over `mds_sessions`). The client counts its capabilities' lifetime from when it *sent* the renew (`Client.cc:2757`: `cap_ttl = last_cap_renew_request + session_timeout`). Defaults are `session_timeout` 60 s and `session_autoclose` 300 s (`src/mds/MDSMap.h:605-606`).
- **Stale, then evicted.** `Server::find_idle_sessions` (`src/mds/Server.cc:1211`) marks a session stale after `session_timeout` and revokes its capabilities, then evicts at `session_autoclose`. It adds the age of its own dispatch queue to the cutoff, and does nothing at all if the MDS itself was laggy recently (`Server.cc:1223`, "don't kick clients if we've been laggy"), so a stalled server does not evict clients it could not hear.
- **Eviction is cluster-wide through the monitors.** With `mds_session_blocklist_on_timeout` (default true, `src/common/options/mds.yaml.in:270`), `MDSRank::evict_client` sends an `osd blocklist add` command to the monitors (`src/mds/MDSRank.cc:3983`), waits until it has the latest OSD map, sets the OSD epoch barrier to that epoch, and only then kills the session (`MDSRank.cc:4016-4031`). Every other rank applies the same blocklist from the OSD map and kills its own sessions for that address (`MDSRank::apply_blocklist`, `MDSRank.cc:1996`). The documentation says so directly: "sending an eviction command to just a single MDS is sufficient, because the blocklist propagates it to the others" (`doc/cephfs/eviction.rst:144`).
- **A barrier instead of acknowledgments.** The MDS does not wait for every OSD. The barrier epoch travels on every capability message, and a client may not send operations to OSDs until it has that map (`eviction.rst:185`); an OSD holds an operation whose minimum epoch is newer than its own map until it catches up (`src/osd/OSD.cc:7684`), and rejects blocklisted senders (`src/osd/PrimaryLogPG.cc:2147`). So the next capability holder's writes reach each OSD only after that OSD knows the blocklist. Blocklist entries expire after an hour (`mon_osd_blocklist_default_expire`, `src/common/options/mon.yaml.in:1178`).
- **Alive but unresponsive.** Eviction of a client that does not answer a capability revoke (`mds_cap_revoke_eviction_timeout`) is off by default (`mds.yaml.in:1573`); the writer waits and the cluster raises a health warning. After an MDS failover, clients that do not reconnect within `mds_reconnect_timeout` (45 s) are evicted.

### NFSv4.1 (Linux nfsd)

- One lease per client ID covers all of that client's state on the server. nfsd renews it whenever a request from the client completes (`fs/nfsd/nfs4state.c:205`, `renew_client_locked`, on `CLOCK_BOOTTIME` seconds) and its laundromat expires clients older than one lease (`nfs4state.c:6896`).
- nfsd keeps an expired client as a *courtesy* client (`nfs4state.c:6794`): its state survives until another client conflicts with it, which is safe only because the server holding the locks is the one that serves the data.
- RFC 8881 itself could not be fetched from this environment; the protocol's lease, grace and revocation rules are not quoted here.

### Chubby

From the OSDI 2006 paper (§2.4, §2.8, §2.9, §3):

- "The master advances the lease timeout in three circumstances: on creation of the session, when a master fail-over occurs, and when it responds to a KeepAlive." The default extension is 12 s; an overloaded master raises it to about 60 s, since "KeepAlives are by far the dominant type of request". One master served 90,000 clients.
- "The client maintains a local lease timeout that is a conservative approximation of the master's lease timeout", allowing for the reply's time in flight and the master's clock running faster than the client's by a bounded factor.
- When the local lease runs out the session is in *jeopardy*: the client empties its cache and blocks application calls for a grace period (45 s). If a new master answers in time, the application sees only a delay.
- A new master picks a new client epoch, rebuilds sessions from its database, and extends "session leases ... to the maximum that the previous master may have been using".
- Fencing toward other servers uses sequencers (lock name, mode and generation), which servers check; for servers that cannot, a lock lost by a failed holder stays unavailable for a lock-delay of up to a minute.

### ZooKeeper

- A client may connect to any server. Followers remember which sessions they heard from and pass them to the leader with each ping (`LearnerSessionTracker.java:48`, `LearnerHandler.java:691`), so many servers *observe* a session but only the leader's `SessionTrackerImpl` tracks its expiry, and the leader expires it by proposing a close (`SessionTrackerImpl.java:170`).
- A new leader starts tracking every session it recovers with a full timeout (`SessionTrackerImpl.java:117`). This is the forwarding model: renew wherever you are, one authority decides.

### etcd

- The Raft leader is the *primary lessor*; renewals at a follower are forwarded (`server/lease/lessor.go:401`, `ErrNotPrimary`), live only in the leader's memory, and expiry becomes a revoke proposal, rate-limited to 1,000 leases per second (`lessor.go:46`).
- A new leader extends every lease by one election timeout (`lessor.go:480-488`, called from `server/etcdserver/server.go:1960`), because "there might be two primary leaders at the same time ... for at most a leader election timeout" (`lessor.go:253`). Remaining TTLs are checkpointed every 5 minutes (`lessor.go:52`) so that long leases do not restart from full at every election.
- etcd fences nothing: `contrib/lock/README.md` reproduces the paused-holder race and says a lease-based lock "cannot provide mutual exclusion" without a token the resource checks.

### HDFS

- "A Lease governs all the locks held by a single client" (`LeaseManager.java:441`): one lease per client at the NameNode, renewed every 30 s (`LeaseRenewer.java:184`, half the soft limit) whatever the number of files.
- A NameNode that becomes active renews every lease, "because, while we were in standby mode, the leases weren't getting renewed on this NN. Give them all a fresh start here" (`FSNamesystem.java:1479-1482`).
- Lease recovery fences the old writer at the DataNodes by giving the last block a new generation stamp (`LeaseManager.java`, class comment).

### Kubernetes node leases

- Every kubelet heartbeat updates the node's `Lease` in `kube-node-lease` (`content/en/docs/concepts/architecture/leases.md:22-26`). The node controller marks an unreachable node `Unknown` and waits 5 minutes before evicting its pods (`nodes.md:224`).
- Nothing fences the node. Volumes are force-detached after 6 minutes, and the documentation warns that "volumes on the node in question might encounter data corruption" (`node-shutdown.md:298-305`); the safe path is an operator adding the `out-of-service` taint after checking that the node is off (`node-shutdown.md:274-290`). This is the gap a storage system's own fence fills.

### Lustre

- Each server target evicts on its own: the ping evictor drops a client not heard from for `PING_INTERVAL + PING_EVICT_TIMEOUT`, 7 × 25 s = 175 s by default (`lustre/include/obd_support.h:92`, `lustre/obdclass/class_obd.c:58`, `lustre/ptlrpc/pinger.c:516`), and a lock callback that times out evicts the client from that target (`lustre/ldlm/ldlm_lockd.c:252`).
- A client pings every target it is connected to, so pings grow with clients × targets; the `suppress_pings` module parameter (`pinger.c:25`) exists to turn them off on large systems.
- Eviction is local, which is enough because each target holds both the extent locks and the objects they cover.

### Not found

- **Weka:** no public documentation of client eviction or fencing was reachable.
- **GFS and Colossus:** GFS's master grants 60 s chunk leases to *primaries*, extended on heartbeats; that is the primary-lease problem rock-solid already solves with read leases, not client sessions. The paper was not re-read for this study.

## What carries over

1. **One verdict per session.** Wherever locks and data live on different servers (CephFS, HDFS, rock-solid), expiry is declared or propagated by one authority. Independent per-server expiry works only when the server holding the lock also holds the data (Lustre, NFS).
2. **Renewal touches memory, not the log.** Every leader-owned design logs creation and expiry only. etcd's checkpoints are an optimization for leases of minutes or hours, not needed at 10 s.
3. **A new authority grants a full lease.** ZooKeeper, etcd (plus an election timeout), HDFS and Chubby (the old master's maximum) all err toward keeping sessions alive across a failover; the client sees a pause (Chubby's jeopardy), never a spurious eviction.
4. **The holder counts from its send.** CephFS and Chubby both count the client's lease from the request, not the reply, which with a bounded clock rate makes the client's lease end first.
5. **The authority does not trust its own stalls.** CephFS skips expiries after it was laggy; etcd rate-limits revokes after a failover.
6. **Renewal cost is per client, not per server.** One authority served 90,000 clients for Chubby; per-server renewal is what Lustre and CephFS pay for (`suppress_pings`, renew to every rank).
7. **Barrier or acknowledgment.** CephFS orders the next holder after the fence with an epoch barrier on its operations; rock-solid's design waits for every storage node's acknowledgment instead. Both are sound; the acknowledgment keeps the stronger invariant (no fenced write is applied anywhere once the lock moves) at the cost of one round to every storage node.

## Proposed design

### Mechanism

- **Record.** A session is a key in the control-plane group, `ctl/session/{id}` → node and state (`live`, or `fenced at v`). IDs come from a counter, `ctl/session/next`, and are never reused. The daemon creates its session at start with one control-plane commit; a [restarted daemon](../../docs/design.md#daemon-restart-and-upgrade) resumes it.
- **Renewal.** The heartbeat each node already sends the control-plane leader every second ([failure detection](../../docs/design.md#epoch-changes-and-failure-detection)) names the node's session; the leader's reply grants the renewal and names the current session-table version. Compute-only nodes send the same heartbeat. Renewals update the leader's memory only. Only a leader holding its [leader lease](../../docs/design.md#metadata-layer-metastore) answers them.
- **Clocks.** The client's lease ends at (send time of its last *answered* renewal) + `session_lease` − `lease_drift`, on its `CLOCK_BOOTTIME`. The leader's runs out at (time it processed the last renewal) + `session_lease`, on its own `CLOCK_BOOTTIME`. `lease_drift` (1%, 100 ms of 10 s) replaces `clock_skew_margin`: no wall clocks are compared, so the margin covers rate, not skew, exactly as for node-pair leases.
- **Who declares expiry.** Only the control-plane leader, for three reasons: the lease ran out on its clock; an owner asks, because the session left a recall unanswered for `recall_timeout` (30 s); or an operator evicts it. Owners never decide. A leader keeps one timer at the earliest expiry, and when it fires commits every session then due in **one** entry, a new session-table version.
- **Failover and stalls.** A new leader, once its leader lease starts, gives every live session a full `session_lease` from that moment before it expires anything (ZooKeeper, etcd, HDFS). A leader whose loop fell behind by more than a heartbeat interval restarts every timer from now instead of expiring sessions it could not hear (CephFS's laggy rule).
- **Publication.** The session table (version, next ID, live sessions, sessions fenced but not yet acknowledged, acknowledged watermark) is published to every node like the placement table, as an immutable value behind an `atomic.Pointer`. A storage node applies a version, from then on rejects every chunk write whose session is fenced or is below the next ID and absent, including writes already queued, and acknowledges; its heartbeats repeat the version it holds, so a lost acknowledgment costs at most a second. A write naming a session at or above its table's next ID makes the primary fetch the table first.
- **Watermark.** When every storage node has acknowledged version v, or has been marked down in a committed epoch, the leader commits `ctl/session/acked = v`, which drops the sessions fenced at or before v from the table. A node marked down is marked up again only once its heartbeat reports the current version, so it never serves under an older table.
- **Owners.** An owner holds no session timers. It rejects requests from sessions its table does not list as live, waits on sessions fenced but not acknowledged, and treats a session that has left the table as gone: a lock key under that session is free (deleted in the transaction that grants the lock to the next holder, and by a background sweep), its open registrations are dropped, and its outstanding recalls count as answered. An owner's failover changes nothing about sessions; the new owner's re-registration grace stays as it is.
- **Client.** When its lease lapses without word of a fence, the daemon stops issuing writes and trusting caches, and holds its FUSE and ublk requests until a renewal is answered: live (it carries on, as after a control-plane failover) or fenced (requests fail, it starts a new session and reopens its files). A client that reaches the control plane but not an owner stays live; its requests to that owner time out, and if the owner needs a recall from it, `recall_timeout` ends the wait with an eviction.

### Why it is safe

Write *s* for the client's send time of its last answered renewal and *p* for when the leader processed it, both in real time, so *s* < *p*. With every clock's rate within ±ρ of real time (the design's bound: the kernel slews by at most 500 ppm, plus the oscillator's own error, so ρ is far below 0.5%), the client's lease ends in real time no later than *s* + 0.99 · `session_lease` / (1 − ρ), and the leader's no earlier than *p* + `session_lease` / (1 + ρ). The first is earlier whenever 0.99 (1 + ρ) < 1 − ρ, that is ρ < 0.5%. A renewal processed by a deposed leader was processed under its leader lease, which ended before the new leader's began, and the new leader's expiry starts no earlier than its own lease, so the bound holds across failovers. Hence:

1. The fence is committed after the client's lease has ended, so the client issues no write after the fence; any write it issued before is either applied at its primary before that primary applies the version, or rejected after.
2. Owners release only on the watermark, after every storage node has applied the version or been excluded by a committed epoch, in which case peering stops every write tagged with the old epoch ([Epoch changes](../../docs/design.md#epoch-changes-and-failure-detection)). Every write of the fenced client that was applied is therefore ordered before every write of the next holder, at every primary.
3. The client's kernel caches expire with its lease, which ended before the fence, so a recall counted as answered by fencing never leaves a reader trusting stale pages, except for mapped pages (as the design already says) and the suspend case under *Unverified* below.

Liveness is the only thing the control plane's own state can cost: a stalled or new leader delays expiries, never brings them forward.

### Costs

- **Renewal:** one heartbeat and reply per node per second, which failure detection already sends, plus about 24 bytes (session ID, incarnation, table version held). At 10 nodes, 10 per second; at 100, 100 per second. Independent of shard count: with tens of shards per node, renewal "with the owners it talks to" would mean every client node renewing with nearly every node that leads a shard, N × (N − 1) messages per interval, 90 at 10 nodes and 9,900 at 100, each owner keeping a timer per session.
- **Raft writes:** one per session start, two per fence batch (the fence and the watermark). None per renewal.
- **Fence latency:** `session_lease` from the last renewal, plus a control-plane commit, one push and one acknowledgment from each storage node: milliseconds on a LAN. If a storage node is unreachable, its down mark (`node_down_after`, 5 s) bounds the wait, and when the client's own node crashed, that mark has already happened, since 5 s is shorter than the 10 s lease.
- **Control-plane outage:** clients keep writing for up to `session_lease` after their last renewal; a longer outage pauses every client's writes until a new leader answers, and fences none of them. A control-plane failover (a 4 s leader lease plus a 1 to 2 s election) fits inside the lease.

### Batching

Fences are batched: one entry fences every session due when the leader's timer fires, and a storage node acknowledges the latest version it holds, which covers every earlier one. Expiries cluster exactly when batching matters: a switch or power failure strands many nodes at once, and they all fall due within one heartbeat interval. Per-session versions would also work at 10 nodes, but would serialize N commits and N acknowledgment rounds in that case.

Fences stay a separate version, not a placement epoch, although Ceph keeps its blocklist in the OSD map: a placement epoch makes members reject every write tagged with the previous one and spends the 24-bit epoch field of chunk versions, so a fence would bounce every write in flight.

### Alternatives rejected

| Alternative | Who uses it | Why not |
| --- | --- | --- |
| Renew with every owner the client talks to, each owner expiring on its own and the first one asking the control plane to fence (the current text, read literally) | CephFS with several active MDS ranks; Lustre targets | A client cut off from one owner is fenced everywhere while other owners still renew it; renewal grows with the owners each client talks to; every owner failover restarts its timers; a block-only client talks to no owner at all |
| One session key in a MetaStore shard, whose leader expires it and asks the control plane to fence | Nobody among the systems read | Adds a hop and a second group whose failover pauses renewal, for load the control-plane leader already carries |
| The node's liveness is the session: fence at `node_down_after` | Kubernetes node leases (without the fence) | Couples a 5 s storage timeout to application-visible errors: a short stall would cost every application on the node a reopen, where it now costs a pause |
| Ceph's epoch barrier on the next holder's operations instead of acknowledgments | CephFS | Saves one acknowledgment round that costs milliseconds at 10 nodes, but every write would carry and check a table version, and a fenced write could still be applied at a primary the next holder has not touched |
| Keep an expired session until someone conflicts with it (courtesy clients) | Linux nfsd | Safe only when the lock server also serves the data; here the conflict would have to wait for a fence round anyway |
| A time-only fence (lock-delay) or none | Chubby's lock-delay; etcd and ZooKeeper leave it to the application | The design already rejects timeouts alone: a paused client's write can arrive at any time |

### Replacement text for `docs/design.md`, "Sessions and fencing"

````markdown
### Sessions and fencing

Each client daemon holds one session, for its FUSE mounts and block devices alike, and every chunk write carries its ID. The session's locks, open registrations and cache rights all belong to it.

**The control plane owns every session.** A session is a key in the control-plane group, `ctl/session/{id}` → node and state, with IDs from a counter that never reuses one. Its lease (`session_lease`, 10 s by default) is renewed by the heartbeat each node already sends the control-plane leader every second for [failure detection](#epoch-changes-and-failure-detection): the heartbeat names the session and the reply grants the renewal. Renewals touch only the leader's memory; the Raft log changes only when a session starts or is fenced. Renewal therefore costs one message per node per second whatever the number of shards, a block-only node renews like any other, and owners never decide that a session expired, so they cannot disagree and their failovers leave sessions alone. Chubby, ZooKeeper, etcd and HDFS keep sessions at their leader in the same way. CephFS lets each metadata server expire its own sessions but turns the first expiry into a cluster-wide blocklist, and Lustre's servers evict independently, which works only because each holds both the locks and the data they protect ([session expiry study](../studies/session-expiry/README.md)).

**Each side counts the lease on its own clock.** The client counts from when it sent its last answered renewal and ends its lease `lease_drift` early, as every lease holder does; the leader counts from when it processed that renewal, on `CLOCK_BOOTTIME`. Only a leader holding its [leader lease](#metadata-layer-metastore) answers renewals. A new leader gives every live session a full `session_lease` from the start of its own lease before it expires anything, and a leader whose loop fell behind restarts every timer instead of expiring sessions it could not hear. Both only delay expiries, never bring them forward.

- **The client limits itself.** It issues no write after its lease ends, and no attribute or directory-entry timeout it gives the kernel reaches past that instant. A paused client's kernel therefore stops trusting its cached pages when the lease ends: the next `read()` asks the daemon for attributes, and the daemon, seeing its lease has lapsed, drops the file's pages first. Mapped pages are the exception, since the kernel does not revalidate them (spike T5b), so a paused node's `mmap` readers can see stale pages until its daemon runs again and invalidates every cached inode. A daemon whose lease lapsed without word of a fence holds its requests until a renewal is answered, so a control-plane failover shows as a pause, as in Chubby's grace period.
- **The control plane fences.** The leader fences a session when its lease runs out, when an owner asks because the session left a recall unanswered for `recall_timeout` (30 s), or when an operator evicts it. It commits every session due at that moment in one new version of the *session table*, which lists live and fenced sessions and is published like the placement table. Every storage node applies the version, from then on rejects every write from a fenced or removed session, including writes already queued, and acknowledges; its heartbeats repeat the version it holds. A write from a session newer than a primary's table makes the primary fetch the table first.
- **Owners release last.** Once every storage node has acknowledged the version, or has been marked down in a committed epoch, the leader commits it as acknowledged, and the fenced sessions leave the table. A down node is marked up again only once its heartbeat reports the current version. Owners act only on that: a lock key whose session has left the table is free, and the owner deletes it in the transaction that grants the lock to the next holder; the session's open registrations go, and its recalls count as answered. A write the fenced client got applied before that is therefore ordered before every write of the next holder.
- **A fenced client starts over.** It learns from its next renewal or a rejected write, starts a new session and reopens its files. Its applications get errors, never silently stale data.

```mermaid
sequenceDiagram
    participant C as Client daemon
    participant L as Control-plane leader
    participant P as Storage nodes
    participant O as Inode owner
    C->>L: heartbeat, session S
    L-->>C: renewed
    Note over C: paused; its lease ends lease_drift early
    Note over L: no renewal for session_lease
    L->>L: commit S fenced, table version v
    L->>P: table v
    P-->>L: applied v (or node marked down)
    L->>L: commit v acknowledged
    L->>O: table: S gone
    O->>O: S's locks free, opens dropped, recalls answered
    C->>P: late write, session S
    P-->>C: rejected
```

*The client's lease ends before the leader's, the fence commits after both, and owners release only after every storage node has applied it.*

This is safe because the client's lease ends first: it counts from a send that came before the leader processed the renewal, and ends `lease_drift` early, which covers the rate difference between the two clocks. So the client issues no write after the fence commits, every write it issued earlier is applied before its primary applies the fence or rejected after, and the next holder writes only after every primary has applied it. A dead client's locks wait `session_lease` plus a commit and one acknowledgment round, milliseconds on a LAN, or until an unreachable storage node is marked down, which for a crashed client node has already happened, since `node_down_after` is shorter than `session_lease`. Fences are batched, so a switch failure that strands many nodes costs one version and one round. They are a separate version, not a placement epoch, which would make members reject every write in flight. A control-plane outage longer than `session_lease` pauses every client's writes until a new leader answers, and fences none of them.

This is the same machinery for flock (Tier 1), recalls (Tier 2), snapshot cuts and RWX block (Tier 0).
````

### Other changes that go with it

- **Roadmap, phase 2:** "sessions and fencing with fence acknowledgments" → "sessions (renewal on heartbeats to the control-plane leader, expiry on its clock, leader failover) and fencing with fence acknowledgments".
- **Roadmap, phase 4:** "Inodes, directory entries, sessions and locks" → "Inodes, directory entries, locks", since sessions move to the control-plane group in the same phase.
- **Key layout, `ctl/...` row:** "fenced sessions" → "sessions and the session table".
- **ChunkStore "Fencing" paragraph:** "the control plane publishes it as fenced and chunk primaries reject its writes. The session's locks and cache rights are released only after every node acting as a primary has applied the fence." → "the control plane publishes it as fenced in its session table and every storage node rejects its writes. The session's locks and cache rights are released only after every storage node has applied the fence or been marked down."
- **Runtime in Go:** "Read-mostly tables (the placement table, fenced sessions)" → "(the placement table, the session table)".
- **Entering WriteShared:** after "plus the time to fence its session", add "; a node that answers the control plane but not the owner is fenced after `recall_timeout`".
- **Alternatives considered,** a new row: *Session expiry* | Sessions owned by the control plane: renewed on each node's heartbeat to its leader, expired on the leader's clock, fenced in one versioned session table acknowledged by every storage node | Renewal with every inode owner, each expiring on its own (CephFS with several metadata servers, Lustre targets); a session key in one MetaStore shard; node liveness as the session; CephFS's epoch barrier instead of acknowledgments | One verdict per session and renewal cost per node, not per owner or shard; per-owner expiry fences a client cut off from one owner everywhere, node liveness turns 5 s stalls into application errors, and the barrier saves a round that costs milliseconds at 10 nodes.
- **Testing strategy, simulation faults for coherence:** add "a control-plane leader failover while sessions renew, a deposed leader answering renewals, a client that reaches the control plane but not an owner, and a leader stalled past `session_lease`".
- **Build checklist:** "Sessions and fencing model": add renewal on heartbeats, expiry on the leader's clock, failover with a full lease, batched fences and the acknowledged watermark, and the property "no fence commits while the client's own lease runs". "Control-plane group": "fenced sessions" → "sessions, the session table and its acknowledged watermark". "Filesystem schema": "sessions and locks" → "locks".
- **Glossary:** *Session*: "kept alive by a lease the control-plane leader renews on the node's heartbeat". *Control plane*: "fenced sessions" → "sessions". *Fencing*: "publishes an expired session as fenced in its session table, every storage node acknowledges it".
- **Blogs 03 and 09:** the "mechanism unspecified" hop becomes the leader's own expiry, the open-directions bullet goes, and `clock_skew_margin` becomes `lease_drift`.
- **`docs/test-scenarios.md`:** nothing yet, since its sections cover phase 1 components; when the sessions and fencing model gets its section, it starts from: a renewal answered just before a control-plane failover; a client paused past its lease with a write already queued at a primary; a fence while a storage node is partitioned, which then rejoins with an older table; two sessions expiring in one batch, one acknowledgment covering both; a client that reaches the control plane but not an owner, evicted by `recall_timeout`; a leader stalled past `session_lease` fencing no one on waking; a new session's first write reaching a primary with an older table.

## Unverified

- **NFSv4.1, pNFS and NFS delegation recall:** RFC 8881 and RFC 8435 could not be fetched (rfc-editor.org and the IETF datatracker do not resolve from this environment); the NFS row rests on Linux nfsd's source alone.
- **Weka** publishes nothing reachable on client eviction; **GFS** was not re-read.
- **Kubernetes `node-monitor-grace-period`:** its current default is not stated in the pages read.
- **Suspend and the kernel's FUSE timeouts.** FUSE stores attribute and entry timeouts in jiffies (`fs/fuse/dir.c:103`), which do not advance while the machine is suspended, while the daemon counts its lease on `CLOCK_BOOTTIME`. A client machine that suspends and resumes would then keep trusting cached attributes for the rest of their timeout (1 s by default) after its lease has ended, until its daemon invalidates them. Read from source, not tested; it affects point 3 of the safety argument and the design's claim that the kernel stops trusting cached pages when the lease ends.
- **The safety argument is prose.** The phase 2 sessions and fencing model should check it, including the deposed-leader case and the watermark.
- **`recall_timeout` of 30 s is a guess:** long enough for a full 256 MiB write-back flush on 1 GbE (about 2 s) with a wide margin; CephFS ships the equivalent off.
- **Incarnations.** The heartbeat could carry the daemon's incarnation, putting the newest one in the session record; whether that settles the open question on incarnation fencing across primary changes is not argued here.
- **Branch heads, not releases.** Ceph, ZooKeeper, etcd, Hadoop, Kubernetes website and Lustre were read at their default branches on 2026-10-19; Linux at v6.18. The commits are pinned in `fetch.sh`.

## Reproduce

`./fetch.sh [dir]` downloads every cited file at the pinned commits into `dir` (default `./work`) and prints the cited lines. Needs `curl`, and `pdftotext` for the Chubby paper (from the papers-we-love repository).

## Sources

- CephFS client eviction: [`doc/cephfs/eviction.rst`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/doc/cephfs/eviction.rst); [`src/mds/Server.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/mds/Server.cc), [`src/mds/MDSRank.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/mds/MDSRank.cc), [`src/mds/MDSMap.h`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/mds/MDSMap.h), [`src/client/Client.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/client/Client.cc), [`src/osd/OSD.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/OSD.cc), [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/osd/PrimaryLogPG.cc), [`src/common/options/mds.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/mds.yaml.in), [`src/common/options/mon.yaml.in`](https://github.com/ceph/ceph/blob/b7e6787889ed0783df44063506dcacaaf672758f/src/common/options/mon.yaml.in).
- Linux v6.18: [`fs/nfsd/nfs4state.c`](https://github.com/torvalds/linux/blob/v6.18/fs/nfsd/nfs4state.c), [`fs/fuse/dir.c`](https://github.com/torvalds/linux/blob/v6.18/fs/fuse/dir.c).
- Mike Burrows, "The Chubby lock service for loosely-coupled distributed systems", OSDI 2006, §2.4, §2.8, §2.9, §3 ([copy](https://github.com/papers-we-love/papers-we-love/blob/9b92363eac4d4fbea0f5fdc7766ef68d22037ca8/distributed_systems/the-chubby-lock-service-for-loosely-coupled-distributed-systems.pdf)).
- ZooKeeper: [`SessionTrackerImpl.java`](https://github.com/apache/zookeeper/blob/48f7cb00e2d48d4563e45bebcd0f4665c30772a2/zookeeper-server/src/main/java/org/apache/zookeeper/server/SessionTrackerImpl.java), [`LearnerSessionTracker.java`](https://github.com/apache/zookeeper/blob/48f7cb00e2d48d4563e45bebcd0f4665c30772a2/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/LearnerSessionTracker.java), [`LearnerHandler.java`](https://github.com/apache/zookeeper/blob/48f7cb00e2d48d4563e45bebcd0f4665c30772a2/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/LearnerHandler.java).
- etcd: [`server/lease/lessor.go`](https://github.com/etcd-io/etcd/blob/22cc5d9acd4a5953dfdfc2594dbeb53da89bb0d4/server/lease/lessor.go), [`server/etcdserver/server.go`](https://github.com/etcd-io/etcd/blob/22cc5d9acd4a5953dfdfc2594dbeb53da89bb0d4/server/etcdserver/server.go), [`contrib/lock/README.md`](https://github.com/etcd-io/etcd/blob/22cc5d9acd4a5953dfdfc2594dbeb53da89bb0d4/contrib/lock/README.md).
- HDFS: [`LeaseManager.java`](https://github.com/apache/hadoop/blob/1097744bc490b65cad5ab2fbeafff35687971357/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java), [`FSNamesystem.java`](https://github.com/apache/hadoop/blob/1097744bc490b65cad5ab2fbeafff35687971357/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java), [`LeaseRenewer.java`](https://github.com/apache/hadoop/blob/1097744bc490b65cad5ab2fbeafff35687971357/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java).
- Kubernetes documentation sources: [`leases.md`](https://github.com/kubernetes/website/blob/1fc565c2f557823a911f073dc577945230f27dd7/content/en/docs/concepts/architecture/leases.md), [`nodes.md`](https://github.com/kubernetes/website/blob/1fc565c2f557823a911f073dc577945230f27dd7/content/en/docs/concepts/architecture/nodes.md), [`node-shutdown.md`](https://github.com/kubernetes/website/blob/1fc565c2f557823a911f073dc577945230f27dd7/content/en/docs/concepts/cluster-administration/node-shutdown.md).
- Lustre: [`lustre/ldlm/ldlm_lockd.c`](https://github.com/lustre/lustre-release/blob/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d/lustre/ldlm/ldlm_lockd.c), [`lustre/ptlrpc/pinger.c`](https://github.com/lustre/lustre-release/blob/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d/lustre/ptlrpc/pinger.c), [`lustre/include/obd_support.h`](https://github.com/lustre/lustre-release/blob/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d/lustre/include/obd_support.h), [`lustre/obdclass/class_obd.c`](https://github.com/lustre/lustre-release/blob/ee61b3f6dd57dd642c14469fcb9593cd20e7a81d/lustre/obdclass/class_obd.c).
