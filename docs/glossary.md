# Glossary

The storage and distributed-systems terms that [design.md](design.md) and the other docs use, each explained one level past a dictionary definition: what the idea is, why storage systems need it, and where rock-solid uses it. Numbers, defaults and the reasons behind decisions live in design.md; entries link to the section that owns them.

Read [one write, end to end](#one-write-end-to-end) first: it introduces most of the core words in the order a request meets them. Several words mean different things in different places; [Words with more than one meaning](#words-with-more-than-one-meaning) lists them.

## One write, end to end

A pod on node A writes 4 KiB into a file that a pod on node B also writes.

1. The pod's `write()` goes to the kernel, which hands it to the **FUSE** client inside node A's **daemon**, the one rock-solid process that runs on every node.
2. The daemon computes the **chunk ID** from the volume, the file's **inode** number and the offset, hashes the chunk ID to a **placement group (PG)**, and looks the PG up in the **placement table**. The table lists the PG's **members**: say three disks on three different nodes.
3. The first member is the PG's **primary**. The daemon sends the write there.
4. The primary stamps the write with a new **version**, turns any block it covers only in part into a whole block by a **read-modify-write**, appends it to its disk's **log**, and at the same time sends it to the other members, the **replicas**, which append it to their own logs.
5. Each member **flushes** its drive so the write is **durable**: it survives a power cut. Once every live member holds the write durably, the version is **committed**, and the primary **acknowledges** the write to node A, which returns from `write()`.

Had no other node been using the file, the daemon would instead have kept the write in its **write-back buffer** and sent it on later, on `close`, `fsync` or when a chunk filled.

The file's name, size and directory live elsewhere, in the **MetaStore**: a set of small replicated databases (**shards**), each a **Raft** group with one **leader**. The leader is the metadata counterpart of a primary. Which disks belong to which PG, and which nodes are up, is decided by one more Raft group, the **control plane**.

## Roles: who does what

**Primary.** The one member of a placement group that orders and coordinates every write to the PG's chunks. Clients send reads and writes to it. It gives each write a version, writes it to its own disk and forwards it to the other members in parallel, and acknowledges once every live member has it. One primary per chunk means one order of writes per chunk: two nodes writing the same bytes can never have their writes applied in one order on one copy and the other order on another. Primary is a role, not a kind of machine: every disk is primary for some PGs and a plain replica for others, which spreads the work. When the primary's node fails, the control plane publishes a new placement table naming another member as primary, and that member [peers](#coordination-and-failure) before it serves anything. This scheme is called primary-backup replication; Ceph uses the same word ("primary OSD"). See [Data layer](design.md#data-layer-chunkstore) and [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Replica.** A member of a PG other than the primary. It stores the writes the primary forwards, acknowledges them once they are durable, and serves reads of frozen snapshot versions. In Raft, "replica" means any copy of a shard, leader included.

**Member.** A disk listed for a PG in the placement table: the primary plus its replicas, or, for an erasure-coded chunk, the disks holding its shards. A member is **live** while the control plane has not marked it down.

**Leader and follower.** The Raft words for primary and replica. A shard's leader takes every write, replicates it to the followers and commits it once a majority has it. Unlike a primary, a leader is **elected** by the group itself, not appointed from outside. rock-solid says "primary" for chunk data and "leader" for metadata. See [Raft](#consensus-and-metadata).

**Why two schemes.** A primary waits for every live member, so a write survives as long as one copy does, but it needs an outside authority (the control plane) to decide who is live and who is primary; otherwise two nodes could both believe they are primary. A Raft group waits only for a majority and elects its own leader, so it needs no outside authority, at the cost of tolerating fewer failures for the same number of copies (one of three). The control plane itself is a Raft group, which breaks the circle.

**Control plane.** The single Raft group that holds cluster-wide state: pools, the placement table, node membership, client sessions. It changes rarely and never sits on the I/O path; every node caches its tables. "Control plane" is the usual name for the part of a system that decides configuration, as opposed to the **data plane** that moves user data.

**Inode owner.** The leader of the MetaStore shard holding an inode. Besides serving the inode's keys, it tracks in memory who has the file open and which cache mode the file is in. See [Inode owner](design.md#metadata-layer-metastore).

**Coordinator.** Whoever drives a multi-step operation that touches several parties, such as a cross-shard transaction or a snapshot cut. If the coordinator crashes partway, the protocol must let someone else finish or undo its work.

**Daemon, node, client.** The daemon is the single rock-solid binary, one per node. A storage node has disks and serves chunks and shards; a compute-only node runs the daemon only as a FUSE client. "Client" means whoever sends a request: usually a daemon acting for a pod.

## Kubernetes and volumes

**Volume.** A disk-like resource a pod uses. In rock-solid a volume is either a **block volume**, a raw device the pod formats itself, or a **filesystem volume**, a shared directory tree. Each has a `volume_id` that is never reused.

**PersistentVolumeClaim (PVC) and PersistentVolume (PV).** A PVC is a pod's request for storage ("10 GiB, shared, from class X"). The storage driver creates a PV to satisfy it, and Kubernetes binds the two. A PVC cannot change its StorageClass later, which is why [moving a volume between pools](design.md#moving-a-volume-to-another-pool) happens in place.

**StorageClass.** A named template for PVCs: which driver creates them and with which parameters. In rock-solid its `pool` parameter picks the pool.

**Access modes: RWO, RWX.** ReadWriteOnce (RWO): one node at a time mounts the volume read-write. ReadWriteMany (RWX): many nodes mount it read-write at once. RWX is rock-solid's headline feature because it is hard: every node caches, and the caches must agree.

**`volumeMode`.** `Filesystem` gives the pod a mounted directory; `Block` gives it a raw device.

**Container Storage Interface (CSI).** The standard plug-in interface between Kubernetes and a storage system. The **controller plugin** runs once per cluster and creates, deletes and snapshots volumes; the **node plugin** runs on every node and mounts volumes into pods. **Sidecars** are helper containers Kubernetes ships that turn its objects into CSI calls.

**Attach and mount.** Attach makes a volume available on a node; mount makes it appear at a path. For RWX, rock-solid mounts each volume once per node and **bind-mounts** that mount into each pod: one directory made visible at a second path.

**Thin provisioning and overcommit.** A thin volume takes space only as it is written, so creating a 1 TiB volume allocates nothing. Overcommit lets the sizes of all volumes add up to more than the disks hold, betting they never all fill; with it off, the pool admits a volume only while every volume's size fits its balanced capacity. When a thin pool does fill, a block volume's writes that need new space wait until it has room: failing them would corrupt the filesystem inside. See [Volume size and capacity](design.md#volume-size-and-capacity) and [Block frontend](design.md#block-frontend).

**Capacity, held, available, balanced and stranded.** A disk's *capacity* for a pool is the space the pool's `write_limit` leaves on it after other pools' data, so it includes what this pool already stores there; *held* is how much data applications have stored in the pool. Available is what a pool can still take before its first disk reaches `write_limit`, given where its placement table sends new data; it is what `df` and Kubernetes see. Balanced is the most the pool could hold in all under any placement, given that copies of a chunk must sit in different failure domains; stranded space is the capacity that rule leaves unusable, such as most of the larger disk when two nodes of different sizes each hold a copy of everything. A failure domain's *stranded room* is the part of its capacity it would still have free with the pool perfectly balanced and full. These figures count capacity rather than free space so that they stay put as the pool fills. See [Volume size and capacity](design.md#volume-size-and-capacity).

**Pool conditions.** Statuses a pool raises on its `Pool` resource, also sent as Kubernetes events and metrics, to tell an operator about its space: `NearFull` and `Full` as its fullest disk nears or reaches `write_limit`; `Stranded` when more of its capacity is stranded than the operator accepted; `Unbalanced` when placement has stopped well short of balanced; `CannotReprotect` when losing a disk or node would leave too little room to restore every copy; and `Stuck` when a placement group's copying has stalled for a minute. See [Volume size and capacity](design.md#volume-size-and-capacity).

**Fill margin.** How far from its target a disk may sit before placement moves members off or onto it: 5% of the target, but at least one member, since on a small disk one member is more than 5%. See [placement table](design.md#data-layer-chunkstore).

**Space grant.** Bytes of a pool a node may hold in its write-back buffer, granted by the control plane on the node's heartbeat. The daemon acknowledges a buffered write only against a grant, so a full pool is reported by `write()` itself, as a local filesystem reserves blocks before it buffers. When nodes ask for more than the pool can give, the control plane shares it out by **water-filling**: every node gets what it asked for up to a common level, raised until the grants fill what is available, so a heavy writer cannot crowd out small ones. See [Client design](design.md#client-design).

**Reuse budget.** How much a daemon may flush under its space grant before the control plane hears from it again: two grants' worth, counted from its last answered heartbeat. Past it the daemon writes straight to storage until the next reply, and it sends its heartbeat early once it has used half, so a busy node rarely reaches the limit. It bounds what a node can write unseen to three grants, whatever happens to heartbeats or the control plane, so the control plane can keep every disk's free room above the bytes it promised. See [Client design](design.md#client-design).

**KubeVirt live migration.** KubeVirt runs virtual machines in pods. Moving a running VM to another node needs both nodes to open its disk at once for a moment, which is why RWX block volumes exist.

**VolumeSnapshot, Velero.** A VolumeSnapshot is the Kubernetes object for a point-in-time copy of a PVC. Velero is a backup tool that uses them. See [Snapshots and clones](design.md#snapshots-and-clones).

## Drives and durability

**Block, sector, logical block address (LBA), 512e.** A drive reads and writes fixed-size blocks, never single bytes; rock-solid works in 4 KiB blocks, so even a 100-byte file takes a whole block per copy. An LBA is a block's number on the drive. A device's *logical* block size is the smallest write it accepts and its *physical* one what it writes internally; a *512e* device accepts 512-byte writes on 4 KiB blocks by reading and rewriting the rest of each block itself. rock-solid's block volumes expose 4 KiB blocks, or 512e per volume; see [Block frontend](design.md#block-frontend).

**Write cache.** Drives acknowledge a write once it sits in their own RAM, before it reaches flash or platters, because that is much faster. A power cut loses whatever is still in that cache.

**Flush, `fsync`, `fdatasync`.** A flush asks the drive to move its write cache onto stable media and answers only once it has. `fsync` and `fdatasync` are the system calls that end in a flush. Flushes are the dominant cost of a durable write on consumer drives, which is why rock-solid shares one flush among many writes ([group commit](#on-disk-structures)).

**Durable.** Survives a crash or power cut. A write is durable only after a flush that covers it, or a FUA write. "Acknowledged", "durable" and "committed" are three different moments; see [Words with more than one meaning](#words-with-more-than-one-meaning).

**Force Unit Access (FUA).** A flag on one write that makes it skip the write cache, so it is durable when it completes. On the drive measured it was slower than a write plus a flush.

**Power-loss protection (PLP).** Capacitors on enterprise drives that hold enough energy to save the write cache when power fails. Such a drive can treat its cache as durable, so flushes are nearly free. Most consumer drives lack it.

**Torn write.** A write of several blocks of which only some landed when power failed. Logs detect torn writes with checksums and treat the torn record as never written.

**Bit rot and misdirected writes.** Data that changed on the media after it was written, and a write that the drive put at the wrong address. Both return wrong data without an error, so rock-solid checksums every 4 KiB block with **CRC32C**, a fast checksum with CPU instructions for it.

**TRIM, discard.** Telling an SSD that blocks no longer hold data, so it can erase them ahead of time and needs less internal copying. A pod's filesystem sends discards when files are deleted, since the CSI driver mounts it with `discard`; rock-solid turns them into [punches](#data-model), which give the space back to the pool.

**Queue depth (QD).** How many I/Os are in flight at once. Queue depth 1, one I/O waiting for the previous one, measures pure latency; deeper queues measure throughput.

**Latency percentiles: p50, p99, p99.9.** p99 is the latency that 99% of operations beat. Averages hide the slow outliers, which are what applications notice, so targets are given as percentiles. "Tail latency" means the high percentiles.

**Round trip.** One message to another node and its reply. Performance targets are stated as round trips because their duration depends on the network.

**Fail-slow.** A disk or node that keeps working but becomes far slower instead of failing. It is harder to handle than a crash because nothing reports an error. See [Fail-slow](design.md#epoch-changes-and-failure-detection).

**NVMe, SATA, HDD.** NVMe drives are SSDs on the PCIe bus, with deep hardware queues and microsecond latency. SATA SSDs use an older, slower interface. HDDs are spinning disks, slow at random access; rock-solid supports them only later, behind flash ([Addresses belong to the log](design.md#on-disk-format)). Shingled magnetic recording (SMR) HDDs overlap their tracks to hold more, and drive-managed ones stall for seconds while they rewrite them.

## Data model

**Chunk.** A 4 MiB piece of a file or block volume, and the unit that is placed, replicated, versioned and erasure-coded. A file of 10 MiB is three chunks. Chunks exist only once written.

**Chunk ID.** `(volume_id, inode_id, offset / chunk size)`, computed rather than looked up, so finding a chunk never needs a metadata query. A block volume uses inode 0.

**Inode.** A file's identity: a number plus its attributes (size, owner, permissions, times). Names are separate: a directory entry maps a name to an inode number, so one inode can have several names (hard links). The term comes from Unix filesystems.

**Version.** A number stamped on each write to a chunk, larger than every earlier one. Versions let a read ask for a past state (`at_ver`), let a write succeed only if the chunk is still at the version the writer last saw (compare-and-set), and let a new primary tell which copy is newest. Its high 32 bits are the number of the chunk's PG [interval](#redundancy-and-placement) in which it was written, and its low 32 bits count the chunk's writes in that interval, from 1; see [Versions](design.md#data-layer-chunkstore).

**Committed version.** A version that every live member of the chunk's PG holds. Reads return only committed versions, so a reader never sees a write that a crash could still undo.

**Punch, tombstone, purge.** A punch zeroes a range of a chunk and frees the blocks it covers whole, zeroing the edges of blocks it covers in part; punching a whole chunk deletes it and leaves a tombstone, a marker that says "deleted" so an older copy cannot bring the chunk back. A purge is the background job that punches every chunk of a deleted or truncated file. See [Freeing space](design.md#freeing-space).

**Extent.** A run of consecutive blocks described by one entry: "blocks 8 to 15 of this chunk are at disk location X". Indexing extents rather than single blocks keeps indexes small.

**Snapshot.** A read-only copy of a volume as it was at one instant. Making one copies nothing: the snapshot keeps the old blocks, and only blocks written afterwards get new space. A **crash-consistent** snapshot holds exactly what a crash at that instant would have left on disk. Each snapshot of a volume gets the next **snapshot epoch** of that volume, and writes after its cut carry it, so the first write to each chunk keeps the old version for the snapshot. See [Snapshots and clones](design.md#snapshots-and-clones).

**Copy-on-write (COW).** Never modifying shared data in place: a write puts new data elsewhere and repoints the writer, so whoever still points at the old data (a snapshot, a reader) keeps seeing it.

**Freeze.** The step that preserves a chunk's current version for a snapshot when the first write after the snapshot arrives. The frozen version keeps pointing at the old blocks. A chunk first written after a snapshot freezes version 0, the absent chunk, so the snapshot reads it as never written.

**Clone, restore, read-through, flatten.** A restore creates a new volume from a snapshot, and a clone creates one from a live volume. Neither copies data at first: a read of a block the new volume has not written **reads through** to the source. **Flatten** copies the remaining blocks in the background and then drops the link to the source.

**Seal.** Marking a chunk immutable, which makes it eligible for erasure coding. A sealed chunk that is written again first returns to copies.

## Redundancy and placement

**Replication.** Keeping several full copies of data on different disks; three copies survive two failures and cost three times the space.

**Erasure coding (EC).** Splitting data into `data` pieces and computing `parity` extra pieces from them, so that any `data` of the pieces rebuild the whole. `ec4-2` writes 4 data and 2 parity **shards**, survives 2 failures and costs 1.5× the space instead of 3×. **Reed-Solomon** is the standard code for this. The catch is updates: changing one data shard means reading and rewriting the parity shards too.

**Stripe.** One set of data shards together with the parity shards computed from them.

**Read-modify-write, the write hole.** Storage writes whole blocks, so a write of a few bytes must read the rest of its block, overlay the bytes and write the block back: a read-modify-write, or *merge*. rock-solid's chunk primary merges every block a write covers in part, against the newest version it has ordered, and sends the whole block to the replicas; see [Data layer](design.md#data-layer-chunkstore). An erasure-coded stripe needs the same for its parity, and if a crash lands between the writes to different shards, the parity no longer matches the data and a later rebuild produces garbage: the write hole. rock-solid avoids that one by never overwriting encoded data; a chunk is written as copies and encoded only once whole or idle. See [When chunks are encoded](design.md#when-chunks-are-encoded).

**Degraded.** Running with fewer copies or shards than configured, after a failure and before recovery. A **degraded read** of an encoded chunk rebuilds a lost shard from the others.

**Pool.** A set of placement groups that share one redundancy scheme (`rep3`, `ec4-2`, …), one failure-domain rule and optionally one class of disk. Every volume belongs to one pool. See [Pools and redundancy](design.md#pools-and-redundancy).

**Failure domain.** A group of things that can fail together: a disk, a node, a rack. Copies of one chunk go to different failure domains, so two copies never sit on one node.

**Placement group (PG).** A bucket of chunks that are placed together. Each chunk hashes into one of a fixed number of PGs, and the cluster decides placement per PG, never per chunk, so the placement table stays small whatever the amount of data. A PG is also the unit of recovery and rebalancing: adding a disk moves whole PGs onto it. The idea and the name come from Ceph.

**Placement table.** The map from each PG to its members. The control plane computes it and every node caches a full copy. See [Placement table](design.md#data-layer-chunkstore).

**Epoch, table epoch.** The placement table's version number. Every change (disk added, node down, a rebalancing step) produces a new table with a higher epoch, and a node uses it to tell which of two tables is newer and to fetch the one a message's sender holds. Whether a request is still valid depends on its PG's interval, not on the table epoch. Ceph calls the same thing an OSD map epoch. A *support epoch* and a *snapshot epoch* are different numbers; see [Words with more than one meaning](#words-with-more-than-one-meaning).

**Interval.** A run of placement tables during which one PG's members, their order and which of them are up stay the same, so the PG keeps one primary and one set of members that must acknowledge its writes. The table numbers each PG's intervals, and a member accepts a request about a PG only if it carries the PG's current number, so a table change disturbs only the writes of the PGs whose members it changed. The interval number is also the high half of every chunk version written in it. The term and the idea come from Ceph. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Recovery.** Copying data back onto new or returning members until every PG again has its full redundancy. Not to be confused with a disk log's crash recovery.

**Refill, `refill_after`.** Giving a PG a new member on another disk in place of a missing one, after which recovery copies the data onto it. A member that is only down is not missing until it has been down for `refill_after` (10 minutes by default), Ceph's "marked out", or for `degraded_refill_after` (1 minute) when its PG has fallen below `min_live` and so takes no writes; its disk losing its log makes it missing at once. A member peering must wait for anyway is never refilled. The new member counts toward `min_live` while recovery is still copying onto it. See [Failure detection](design.md#epoch-changes-and-failure-detection).

**Rebalancing.** Moving PGs between disks to even out how full they are, for example after a disk is added.

**Blast radius.** How much data one failure affects. Spreading a volume over every disk makes it fast and lets it outgrow any one drive, but any failure in the pool touches it.

## Coordination and failure

**Heartbeat.** A small periodic "I am alive" message. Missing several of them is how a node is detected as down.

**Failure detection, marked down.** The control plane decides that a node or disk has failed and publishes a new table that marks it down, which starts a new interval of every PG that has a member there. The member stays in its PGs while down, so a reboot costs only the writes it missed, unless it leaves a PG below `min_live` for longer than `degraded_refill_after`. Detection is a guess: a slow node looks the same as a dead one, which is why protocols must stay safe when the guess is wrong. See [Failure detection](design.md#epoch-changes-and-failure-detection).

**Flap damping, `flap_count`, `flap_window`.** Keeping a node or disk that keeps going down and coming back marked down until it has stayed healthy for a while, so its PGs do not start new intervals over and over. rock-solid holds one marked down `flap_count` times (5) within `flap_window` (10 minutes) down until it has been normal for a whole `flap_window`, unless that would keep a PG below `min_live` with nowhere to refill; Ceph stops an OSD at the same count. See [Failure detection](design.md#epoch-changes-and-failure-detection).

**Split brain.** Two nodes both believing they are in charge, for example after a network partition, and both accepting writes. Most of the coordination machinery below exists to prevent it.

**Lease.** A right that expires on its own after a fixed time unless renewed. The holder must stop using the right before its lease ends, and the grantor must not hand it to anyone else until then, so the two never overlap even if they cannot talk. This needs clocks that run at about the same rate (**bounded drift**), not clocks that show the same time.

**Read lease (primary lease).** The lease a primary needs from the other members of its PG before it serves reads, so that an old primary stops serving before a new one starts writing. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Peering.** What a new primary does before serving a PG: it contacts members of every interval since the PG last activated, makes sure none of those intervals can complete any more writes, and agrees with the members on the newest version of every chunk; the versions it drops, never acknowledged, are always a chunk's newest. Each member keeps a **missing list**, the kept version of every chunk it still has to receive, so the next peering knows what this one decided. The term comes from Ceph. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Session.** A client daemon's registration with the cluster, kept alive by a lease that the control plane renews on the node's heartbeat and alone declares expired. Its locks, open files and cache rights belong to the session, so they all end together when it expires.

**Fencing.** Making sure a client that lost its rights can no longer act on them, even if it does not know yet: a paused or partitioned client may wake up and keep writing. rock-solid publishes an expired session as **fenced** in its **session table**, the versioned list of live sessions that every node caches, every storage node applies the table and rejects the session's writes, and only once all have acknowledged are its locks given to someone else. Timeouts alone cannot do this, because the client's own clock and scheduling cannot be trusted. See [Sessions and fencing](design.md#sessions-and-fencing).

**Monotonic clock, clock skew, clock drift, slewing.** A monotonic clock only moves forward, unlike wall-clock time, which can jump. Skew is the difference between two clocks' readings; drift is the difference in their rates. A time daemon such as chrony corrects a clock's reading by **slewing** it: running it faster or slower for a while instead of jumping it, by up to 22.6% on Linux. Linux's `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` follow slews; `CLOCK_MONOTONIC_RAW` does not, so it drifts only as far as the machine's oscillator and its calibration at boot do. rock-solid counts every lease on both, each side taking the one that is safe for it. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Lease margin, rate bound, step check.** The **margin** is how much earlier than its grantor a lease holder stops counting: `lease_drift`, 1% of the lease, for clocks that agree. Both clocks of a machine can run fast or slow together, by an error in the kernel's boot calibration that no daemon fixes, so a holder measures each grantor: from the grantor's clock readings on two grants and its own send and receive times, it gets an upper limit, the **rate bound**, on how fast the grantor's clock runs against its own, and widens its margin to match. It needs two grants at least `rate_gap` (100 ms) apart, takes no lease from a grantor before then, and starts again when the grantor reboots, which it sees from the **boot ID**, a random number Linux picks at every boot. The **step check** compares the grantor's clock on every heartbeat with its own: a grantor's clock that moved further than the margin allows means the holder lost time, as in a VM pause, and it ends its leases. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**VM pause, VMClock.** A hypervisor can stop a virtual machine and later continue it, save it as a snapshot and restore it, or move it to another host while it runs (live migration). QEMU and Firecracker then give the guest its clocks back as they were, so to every clock inside the VM no time passed. **VMClock** is a page of memory the hypervisor shares with the guest; its **disruption marker** changes whenever the guest's clock was disrupted, which QEMU and Firecracker do on every restore and migration, but not on a stop and continue in place. rock-solid's lease holders end their leases when the marker changes, or when a peer's clock shows they lost time. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

## Consensus and metadata

**Metadata.** Data about data: file names, directories, sizes, permissions, locks, which disks hold which PGs. It is small but every operation needs it, and losing it loses the data it describes.

**Key-value store, ordered.** A database of keys mapped to values. Ordered means keys are kept sorted, so all keys sharing a prefix ("every entry of this directory") can be read with one **scan**. The MetaStore is one; every piece of metadata is a key. See [Key layout](design.md#metadata-layer-metastore).

**Compare-and-set (CAS).** "Set this key to X only if it is still Y." Locks, `rename` and S3's create-if-absent are all CAS, since two racing clients must not both win. In a MetaStore transaction the compare is a `Check` op, and `CheckEmpty` asks that no key lies in a range, as `rmdir` needs.

**Atomic update (Add, Bump).** A change to a number stored in a key that the store computes itself, from whatever the key holds when the change applies, instead of the client sending the new value: `Add` adds to it, `Bump` sets it to the larger of a given time and the stored time plus one, so it always moves forward. Many clients can update one key in the same round without any of them failing, which compare-and-set cannot do; a directory's modification time and a slot's counters change this way. FoundationDB calls these atomic operations. See [Metadata layer](design.md#metadata-layer-metastore).

**Consensus.** Getting several machines, some of which may crash, to agree on one value or one order of operations. CAS across replicas needs it: they must agree on which of two racing requests won.

**Quorum, majority.** A quorum is the number of replicas whose agreement is enough to decide. A majority quorum (2 of 3, 3 of 5) has the useful property that any two quorums overlap in at least one replica, so a later decision always sees an earlier one.

**Raft.** A widely used consensus algorithm. A group of replicas, usually three, elects a **leader**; the leader appends each operation to its **Raft log** and copies it to the **followers**; an entry is **committed** once a majority has it durably, and then every replica **applies** it to its own copy of the data, in log order. If the leader fails, followers wait out a random **election timeout** and one is elected for a new **term**. rock-solid uses etcd's implementation, etcd-raft. See [Raft shard](design.md#metadata-layer-metastore).

**Voter.** A replica that counts toward Raft's majority.

**Learner.** A Raft replica that receives the log but does not vote. A new replica joins as a learner and becomes a voter only once it has caught up, so a group never waits on a member with an empty log. See [Replica placement and replacement](design.md#metadata-layer-metastore).

**Joint configuration, `ConfChangeV2`.** Raft's safe way to change a group's voters: for a while every decision needs a majority of the old voters and a majority of the new ones, so the two sets can never decide apart. etcd-raft's `ConfChangeV2` enters one and, by default, leaves it on its own.

**Log compaction, Raft snapshot.** A Raft log cannot grow forever. Once entries are applied and the result saved, they are dropped; a replica too far behind to catch up from the log is sent a Raft snapshot, a copy of the applied state, instead.

**Shard, Raft group, multi-Raft.** The metadata key space is cut into ranges, and each range is its own Raft group, called a shard. Running many groups (multi-Raft) spreads leaders, and so load, over every node, instead of sending every metadata write through one leader. A shard that grows too large **splits** in two.

**Redirect (not leader).** What a shard replica that is not the leader does with a request: it answers "not leader", naming the leader it knows, and the client resends there, rather than forwarding the request itself. The client then hears its result directly, and the request reaches the replica that keeps the shard's in-memory state. CockroachDB and TiKV redirect the same way. See [Metadata layer](design.md#metadata-layer-metastore).

**Slot.** A range of inode numbers that always stays in one shard. A directory and its files share a slot, so creating a file touches one shard only. See [Inode IDs](design.md#metadata-layer-metastore).

**Range sharding and hash sharding.** Range sharding gives each shard a contiguous span of keys, so a prefix scan reads one or a few shards. Hash sharding spreads keys randomly, which balances load but makes every scan touch every shard.

**Transaction, atomic.** A group of changes that happen all together or not at all. A **single-shard** transaction commits in one Raft round. A **cross-shard** transaction needs more, because no single Raft log covers it.

**Two-phase commit, transaction record, intent.** The general way to commit across shards: first write every change provisionally, then flip one switch that makes them all real. rock-solid writes provisional values called **intents**, and a single **transaction record** key whose flip to COMMITTED is the switch. A reader that meets an intent checks the record. See [Cross-shard transactions](design.md#metadata-layer-metastore).

**Lease read, leader lease.** A Raft leader normally must check with a majority before answering a read, to be sure it is still the leader. Holding a lease lets it answer from memory instead, as long as the lease lasts. rock-solid's leader lease comes from **fortification**: the leader asks each follower to promise that, while its node's lease to the leader's node holds, it will not help elect anyone else in that group. With a quorum of promises the leader holds a lease without sending heartbeats, so an idle group can go quiet and still serve reads. See [Reads and leader leases](design.md#metadata-layer-metastore).

**Support epoch.** The number of one unbroken run of a node-pair lease. The grantor bumps it when the lease lapses, so nothing granted under the old number comes back after a partition heals. It belongs to the node pair and has nothing to do with the placement table's epoch. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Leader transfer, leader balancing.** Handing leadership to another replica on purpose, without an election: for an upgrade, to drain a node, or to spread leaders evenly.

**Quiescence.** Letting an idle Raft group stop its timers and heartbeats, so thousands of idle groups cost almost nothing. The per-node-pair leases keep an idle group's leader lease and its followers' loyalty.

**Soft state.** State kept only in memory and rebuilt after a crash rather than written to disk. The inode owner's record of open files is soft state, which keeps `open` free of disk writes.

**Linearizable.** The strongest usual consistency guarantee: every operation appears to take effect at one instant between its call and its return, so the system behaves like a single copy. Checking a recorded history for it is how tests catch stale reads.

## On-disk structures

**Log-structured, append-only.** Writing only at the end of a log and never overwriting in place. Every write becomes one sequential append, which is fast and makes crashes easy to reason about; the price is that old data must be cleaned up later. rock-solid keeps one log per disk for everything on it. See [On-disk format](design.md#on-disk-format).

**Segment.** A fixed-size piece of the disk log, written front to back once and later cleaned and reused as a whole.

**Open sequence number, `NextSeq`.** A counter the log raises every time it starts filling a segment, written into the segment's header and every record header in it, so a block left from an earlier use of the segment never reads as a current record. Every manifest records `NextSeq`, the number the next segment will take, so recovery can tell a segment opened after the manifest from one the cleaner freed before it, whose old header is still on disk. See [Recovery](design.md#on-disk-format).

**Log ID.** A random number drawn when a disk is formatted, written into its superblock and every header, so a block left by an earlier format is never read as this one's. The control plane binds each disk ID to its log ID: a disk formatted again or replaced comes back as a new disk with a new ID, never as the member it was, whose writes it no longer holds. See [Epoch changes](design.md#epoch-changes-and-failure-detection).

**Record.** One append to the log: header blocks describing what follows (which chunk, which version, checksums) and the data blocks themselves.

**Log sequence number (LSN).** A record's position in the log's order. "Everything after LSN 5000" names exactly which records a recovery must replay.

**Stream.** One of three sequences of segments the log appends to: hot (new writes), cold (cleaner copies and shards) and metadata (sorted runs, compactions, manifests). Each segment holds one kind of data, so it tends to empty all at once, which keeps cleaning cheap.

**Lane.** A sequence of segments of one stream that fill one after another, at most one open at a time. Each stream has a shared lane, and each merge worker writes its output to a lane of its own in the metadata stream, so merge output does not mix with other data. Recovery reads each lane from just before the replay point and merges the lanes in LSN order. See [On-disk format](design.md#on-disk-format).

**Chunk slot.** A chunk-sized place in a slot segment holding one whole chunk's data. It is freed when the chunk is rewritten or deleted, so whole chunks never need cleaning. See [On-disk format](design.md#on-disk-format).

**Superblock.** The block at the start of the disk that says how the rest is laid out. rock-solid keeps a second copy so one bad block cannot make a disk unreadable; see [Superblock repair](design.md#superblock-repair).

**Group commit, round.** Collecting every write that arrived while the previous flush was running, and making them all durable with one write and one flush. Each such batch is a round. Since a flush costs the same whether it covers one write or a hundred, this multiplies throughput. In the MetaStore, each leader also folds a round's proposals into one Raft entry.

**Write-ahead log (WAL), replay, recovery.** A WAL records each change before it is applied elsewhere, so after a crash the change can be redone by **replaying** the log. Crash recovery means reading the log back and rebuilding the in-memory state from it.

**Torn tail, tail rule.** After a crash, the last few records of a log may be torn or missing because their rounds were still being written. The tail rule tells such a torn tail from damage: a missing record ends the log only if no record after it says the missing one was already durable (each record header names the newest durable LSN when it was written) and the records found after it are few enough to have been in flight together (`commit_depth` rounds). Otherwise something already durable was lost, and recovery reports corruption instead of quietly cutting the log short. See [Recovery](design.md#on-disk-format).

**Checkpoint, manifest.** Replaying the whole log after every crash would take ever longer, so a checkpoint periodically saves the in-memory state to disk and writes a **manifest**, a small record that says where that state is and from which LSN replay must start. A manifest is a **snapshot** of that state or an **edit** listing only what changed since the manifest before it; a snapshot is written once the edits since the last one would outgrow it.

**Index.** A structure that finds data by key without scanning everything. The **chunk index** on each disk maps each chunk's blocks to their place in the log; its **live map** lists the current extents of each chunk.

**LSM tree (log-structured merge tree).** The index structure used by most modern key-value engines (RocksDB, Cassandra) and by rock-solid. New entries go into an in-memory **memtable**; when it grows, it is written out whole as an immutable, sorted **sorted run** (often called an SSTable); **compaction** merges runs in the background, dropping overwritten and deleted entries. Writes are cheap; a lookup checks the memtable, then the runs. **Size-tiered** compaction merges runs of similar size; **universal** compaction, RocksDB's size-tiered variant that rock-solid uses, merges only runs adjacent in age, when a run is no larger than the newer runs before it or the newer runs together outgrow the oldest. **Leveled** compaction instead splits runs into key ranges and merges each range into the level below.

**Read amplification, space amplification.** The runs a lookup searches, and the bytes an index takes divided by its live data. Compaction trades them against write amplification. See [The LSM tree](design.md#on-disk-format).

**Memtable flush.** Writing a memtable out as a sorted run. Unrelated to a drive flush, though both make data durable.

**Removal marker.** What an LSM tree writes to delete a key: an entry saying "deleted", since the older runs holding the key are immutable. It hides the key in every older run until a compaction that includes the oldest run drops it with them. Other engines call it a tombstone, the word rock-solid keeps for a deleted chunk. A **range removal** hides every key from one key up to another, as RocksDB's and Pebble's `DeleteRange` do, kept as non-overlapping fragments per memtable and run that hide older layers' keys but not their own run's; the chunk index uses one when a write covers several old extents and to remove a punched chunk's live map, and the MetaStore to drop a volume or a list's consumed entries. See [The LSM tree](design.md#on-disk-format).

**Value separation.** Keeping large values out of an index, in a log of their own, so the index holds only their locations and merging it rewrites small entries instead of the data; WiscKey introduced it, and RocksDB's BlobDB, TiKV's Titan and Pebble ship it. The price is a cleaner that copies live values out of old log space and updates the index. rock-solid's chunk data and chunk index are laid out this way; see [On-disk format](design.md#on-disk-format).

**Copy entry.** The record entry a cleaner writes for blocks it moves: the chunk, version and blocks, plus the location it copied them from. Replay applies it only where the index still holds that location, so a write that replaced the blocks first is never undone. See [On-disk format](design.md#on-disk-format).

**Metamorphic test.** A test that runs the same operations under configurations that must not change the results (piece sizes, flush and compaction timing, restarts) and compares what each run read. It needs no model of the right answer, only the rule that the configurations agree. Pebble's main correctness test. See [Testing strategy](design.md#testing-strategy).

**Data-driven test.** A test whose cases are text files: a command, its input lines, a separator and the expected output, which the test rewrites on request for review. Used by Pebble and etcd-raft (`github.com/cockroachdb/datadriven`). See [Testing strategy](design.md#testing-strategy).

**Test-case reduction.** Shrinking a failing generated test to the fewest operations that still fail, by removing chunks while the failure persists. See [Testing strategy](design.md#testing-strategy).

**Prefix recovery.** Recovery that returns exactly the writes up to some point and none after it: no recovered write is newer than a lost one (no hole). RocksDB's crash test and our crash oracle check it. See [Testing strategy](design.md#testing-strategy).

**Delete-only compaction.** Pebble's name for dropping whole tables that a newer range deletion covers, by editing the manifest instead of merging. See [The LSM tree](design.md#on-disk-format).

**Run piece.** A sorted run is written to the log in pieces of at most one record each; the last, the root, lists the others, so a manifest names a run by its root. See [The LSM tree](design.md#on-disk-format).

**Fence pointer, prefix compression.** A run is cut into blocks, and the first key of each block is a fence pointer: binary search over them finds the one block that can hold a key. Inside a block, each key is stored as the bytes it shares with the key before it plus the rest, which shrinks sorted keys with long common prefixes; every 16th key, a restart point, is stored whole so a search can start there. This is LevelDB's block format, which Pebble and RocksDB keep.

**Bloom filter, prefix filter.** A small structure that answers "is this key possibly in this run?" with no false negatives and a few false positives (about 1% at 10 bits per key), so a lookup can skip runs that cannot hold its key. A prefix filter holds key prefixes instead, such as a chunk's `(pool, h, chunk_id)`, so a floor or a scan inside one prefix can skip runs too. See [The LSM tree](design.md#on-disk-format).

**Block cache.** Memory holding recently read blocks of runs that live on disk, so repeated lookups skip the disk. rock-solid needs one only when an index outgrows memory. See [The LSM tree](design.md#on-disk-format).

**B+-tree.** A sorted tree whose nodes are fixed-size pages, the classic structure for indexes. rock-solid's memtable is one, built **copy-on-write** so a reader can keep a consistent view while the writer goes on.

**Point-in-time view.** A frozen view of an LSM tree at one moment, which a reader or a Raft snapshot uses without blocking writers.

**Cleaner, garbage collection.** In a log, overwritten and deleted data leaves dead blocks scattered through old segments. The cleaner picks a **victim** segment, copies its still-live blocks to the head of the log and frees the whole segment for reuse. Also called segment cleaning or log garbage collection, and unrelated to Go's garbage collector.

**Write amplification.** Bytes actually written to the drive divided by bytes the user wrote. The cleaner's copying raises it, more so the fuller the disk is, which is why pools stop writes before disks are full. SSDs have their own internal write amplification on top. See [Volume size and capacity](design.md#volume-size-and-capacity).

**Reserve.** Segments held back for the work that frees space, checkpoints, the cleaner and merge steps, since freeing space itself needs some free space. Ordinary writes wait while only the reserve is free.

**Write stall, admission control.** Slowing or pausing new writes when background work (cleaning, checkpoints) falls behind, so the disk never runs out in the middle of it. RocksDB and Pebble call a pause a write stall; CockroachDB's admission control admits writes at the rate background work measurably keeps up, which is what rock-solid does. See [On-disk format](design.md#on-disk-format).

**Merge step, bound.** A merge installs its output a step at a time; each input run then holds only the keys above a bound key, the last key the step reached, so the pieces it no longer needs are freed before the merge ends. ScyllaDB calls this incremental compaction. See [The LSM tree](design.md#on-disk-format).

**Quarantine.** Setting aside a segment that looks damaged: never reused, and reported for repair from another replica.

**Segment usage table.** The list of which segments are in use or quarantined, how many live blocks each holds and its newest LSN, kept in the manifests: whole in a snapshot, the changed rows in an edit. Recovery takes free space from it instead of guessing from segment headers, and the cleaner picks victims by it. See [On-disk format](design.md#on-disk-format).

## Files and caching

**POSIX.** The standard Unix filesystem interface (`open`, `read`, `write`, `rename`, `stat`, locks) that applications expect.

**Namespace operations.** Operations on names rather than data: `create`, `mkdir`, `unlink`, `rename`, `setattr`. They are MetaStore transactions.

**Directory entry, attributes, xattr.** A directory entry maps a name to an inode. Attributes are what `stat` returns: size, mode, owner, times. Extended attributes (xattrs) are extra name-value pairs on a file.

**`mtime`.** A file's last-modification time. rock-solid also uses it to force kernels to drop stale cached pages; see Tier 2 in [Cache coherence tiers](design.md#cache-coherence-tiers).

**Filesystem in Userspace (FUSE).** A Linux mechanism that forwards a filesystem's operations from the kernel to an ordinary process, here the daemon, through `/dev/fuse`. It avoids writing a kernel module, at a cost of a few microseconds per request.

**Descriptor handoff.** Passing the open `/dev/fuse` file descriptor that serves a mount from one process to another over a Unix socket (`SCM_RIGHTS`). The kernel keeps the mount's whole state with the descriptor, so a restarted daemon that takes it back serves the mount without remounting, and applications see a pause instead of errors. See [Daemon restart and upgrade](design.md#daemon-restart-and-upgrade).

**FUSE over io_uring.** A FUSE transport (Linux 6.14+) in which the daemon takes requests and returns replies through io_uring commands on per-CPU queues instead of reading and writing `/dev/fuse`. The daemon does not use it, because a crash strands or fails requests ([Daemon restart and upgrade](design.md#daemon-restart-and-upgrade)).

**Page cache.** The kernel's in-memory cache of file contents. A read served from it never reaches the daemon, which is why caching is fast and also why it must be turned off when another node is writing.

**Direct I/O.** Bypassing the page cache, so every read and write goes to the daemon. rock-solid opens a file with direct I/O while it is write-shared.

**Write-back and write-through.** A write-back cache acknowledges a write before passing it on and sends it later; a write-through cache passes it on first. The daemon's **write-back buffer** holds a file's writes while no other node shares the file and the node's space grant covers them; otherwise the daemon writes through.

**Cache coherence.** Making every node's cache agree with the latest writes, so no node reads stale data. See [Cache coherence tiers](design.md#cache-coherence-tiers).

**Close-to-open consistency.** The guarantee NFS gives: an `open` that starts after another node's `close` sees everything written before that close. Simple and enough for one writer at a time, but nodes that keep a file open together can see stale data.

**Cache mode: Exclusive, ReadShared, WriteShared.** The per-file state the inode owner tracks: one node has the file open (it may cache everything), several nodes only read it (all may cache reads), or several nodes have it open and one has written (nobody caches).

**Recall, invalidation.** A recall is the inode owner telling other nodes to give up their cache rights for a file: flush buffered writes and drop cached pages. The write or open that makes the file write-shared waits until each node has acknowledged or been fenced. Invalidation is the kernel side of dropping pages.

**`mmap`.** Mapping a file into a process's memory, so reads and writes are plain memory accesses the filesystem never sees one by one. That makes coherence for mapped files much harder.

**flock, `fcntl` locks, byte-range locks.** Advisory locks applications take on a whole file (`flock`) or on ranges of bytes (`fcntl`), which only cooperating applications respect.

**Orphan.** A file that was unlinked while still open. POSIX keeps it readable until the last close; rock-solid records it on an orphan list and frees it only then. NFS fakes this with "silly rename" to a hidden `.nfsXXXX` name.

**Grace period.** After a metadata leader crashes, a window in which clients re-register their open files before the new leader accepts new opens. The idea comes from NFS version 4.

**NFS, SMB, Samba.** Network File System and Server Message Block are network file-sharing protocols. Both put a server between clients and storage, which is the bottleneck rock-solid's RWX avoids. Samba is the open-source SMB server.

**ublk, NVMe-oF, NBD, iSCSI.** Ways to present a block device. ublk lets a user-space process (the daemon) serve a local block device through io_uring. NVMe over Fabrics (NVMe-oF) and iSCSI expose a block device over the network; NBD is Linux's simple network block device. ublk's **user recovery** keeps the device and holds its I/O while the daemon restarts, and with **reissue** hands the requests the dead daemon held to its successor.

**Incarnation.** One run of a daemon process, named by its start time. Every connection's hello and every chunk write and punch carry it. A primary that has seen a newer incarnation of a client's session rejects the older one's writes, checking when it orders each write, so bytes a dead daemon left on the wire, or a write the primary already held, cannot land after its successor's. See [Daemon restart and upgrade](design.md#daemon-restart-and-upgrade).

**S3, object, bucket.** S3 is Amazon's object-storage protocol, which many systems implement. An object is a whole blob written at once under a key; a bucket holds objects.

## Runtime and drivers

**Sans-IO, state machine, event, action.** rock-solid's protocol code never does input or output itself. Each component is a state machine with one function, `OnEvent`: it takes an event ("message arrived", "disk write finished", "timer fired") and returns actions ("send this", "write that", "wake me at T"). A **driver** carries out the actions and turns what happens into events. Because nothing real happens inside the machine, a simulator can feed it every possible ordering. See [Modularity](design.md#modularity-sans-io-core-pluggable-drivers).

**Data path, hot path.** The code every read and write runs through, where a microsecond per operation matters. The [runtime rules](design.md#runtime-in-go) apply there and not elsewhere.

**Event loop, epoll.** One thread of control that waits for any of many things to happen (a socket readable, a disk completion, a timer) and handles each in turn. epoll is the Linux system call that does the waiting. rock-solid runs one event loop per CPU core instead of one goroutine per connection.

**io_uring.** The modern Linux interface for asynchronous I/O: the program puts requests on a ring shared with the kernel and picks up completions from another, with few system calls.

**Kernel seam.** The one small interface through which an event loop does everything that involves time or waiting: it waits in `epoll_wait`, reads all its clocks together, and arms its timer. Production plugs in the real kernel; tests plug in a stepped kernel with scripted clocks. A pass waits first and reads the clocks only after the wait, so nothing that became ready while the loop slept is handled at an earlier time. See [Runtime in Go](design.md#runtime-in-go).

**Garbage collector (GC), heap, off-heap.** Go frees unused memory automatically by scanning the heap for pointers. Memory mapped outside the Go heap is never scanned, so large pointer-free structures there cost the collector nothing; rock-solid keeps its bulk data there.

**Buffer pool, slab, handle.** A fixed area of memory, carved into equal slots (a slab), from which I/O buffers are taken and returned. Code passes a small handle (slot number plus generation) instead of a pointer, so a stale handle can be detected.

**Kernel bypass.** Talking to hardware from user space without going through the kernel, to save its overhead. **SPDK** does it for NVMe drives, **DPDK** for network cards, and **RDMA** (Remote Direct Memory Access) lets one machine's network card read and write another machine's memory directly. **Soft-RoCE** is a software RDMA that runs on any network card, for testing. These need **hugepages** (large memory pages) or **registered memory** the hardware may access by **DMA** (direct memory access), and often **polling**: spinning on a core instead of sleeping until an interrupt. io_uring registers memory too (*fixed buffers*), so the kernel pins it once instead of on every request ([Runtime in Go](design.md#runtime-in-go)).

**CPU limit, CFS quota, throttling.** A Kubernetes CPU limit becomes a CFS bandwidth quota on the container's cgroup: so much CPU time per 100 ms period across all its threads. A process that uses its quota early in a period is throttled, with every thread stopped, until the next period. This is why the daemon never runs more Go Ps than its limit ([Runtime in Go](design.md#runtime-in-go)).

**cgo.** Go's mechanism for calling C libraries, used only for SPDK and RDMA.

## Testing

**Deterministic simulation testing (DST), seed.** Running a whole cluster (nodes, disks, network, clients) inside one process, driven by one random number generator. The simulator injects crashes, lost messages and disk faults. The same **seed** produces exactly the same run, so any bug it finds can be replayed. See [Testing strategy](design.md#testing-strategy).

**Invariant.** A property that must hold at every step, such as "every acknowledged write is readable". The simulator checks them all after each step.

**Fault injection, nemesis.** Deliberately causing failures (kills, partitions, slow disks) to see whether the system survives them. Jepsen calls the component that does this the nemesis.

**TLA+, TLC, model checking.** TLA+ is a language for describing a protocol precisely; the TLC model checker explores every reachable state of a small instance and reports any that breaks an invariant, catching design bugs before code exists.

**Jepsen, Elle, Porcupine.** Tools that record what clients saw and check whether any valid order of operations could explain it; one that cannot reveals a consistency bug.

**Fuzzing.** Feeding a function large numbers of generated inputs to find ones that crash it. Every decoder of on-disk or network bytes is fuzzed, because those bytes can be corrupt.

**Golden file.** A checked-in file of expected bytes that the code must reproduce exactly, so an accidental format change fails a test.

**Spike.** A throwaway experiment that answers one design question by measurement. Kept under `spikes/`, summarized in the appendices of design.md.

**Stepped loop.** An event loop run in a test on a stepped kernel: its clocks are scripted, its waits never block, and the simulator's scheduler decides, from a seed, which loop or worker job runs next. Many loops then run in one goroutine over real sockets, a suspend can be scripted inside a chosen pass, and a seed replays the same event order. See [Testing strategy](design.md#testing-strategy).

**synctest bubble.** Go's `testing/synctest` runs a test's goroutines in a bubble whose fake clock jumps ahead whenever every goroutine in it is blocked on something only another one can release, such as a channel or `time.Sleep`. It fakes only Go's `time` package, so it suits background goroutines that wait on channels and timers, but not an event loop, whose clock reads and `epoll_wait` go to the kernel. See [Testing strategy](design.md#testing-strategy).

## Words with more than one meaning

| Word | Meanings |
| --- | --- |
| Primary | The PG member that orders a chunk's writes (almost always). In [Superblock repair](design.md#superblock-repair), the first copy of a filesystem's superblock, as ext4 and XFS use it |
| Replica | A PG member other than the primary, holding a copy of chunk data. Any copy of a Raft shard, leader included. `rep3` means three copies |
| Member | A disk in a PG. In a cache mode, a node that has the file open |
| Leader | A Raft group's elected coordinator; never used for chunk data, which has primaries |
| Owner | The inode owner, the leader of an inode's shard. The event loop that owns a disk's log and index, its connections or its shards and is the only code to change them |
| Epoch | The placement table's version, the table epoch (almost always). A volume's snapshot epoch, which counts its snapshots. A node-pair lease's support epoch. A chunk version holds none of them: its high half is its PG's interval number |
| Interval | A PG's run of placement tables with the same members (almost always). Any span of time, as in a heartbeat interval |
| Shard | A MetaStore Raft group over a range of keys. One piece of an erasure-coded chunk |
| Commit | A chunk version is committed once every live member holds it. A Raft entry, once a majority holds it. A transaction, once its changes take effect. Group commit is batching many writes into one flush |
| Acknowledged, durable, committed | Acknowledged: the caller was told the write succeeded. Durable: it survives a power cut on one disk. Committed: it is on every live member (data) or a majority (Raft). A primary acknowledges a write only once it is committed, and so durable. The daemon's write-back buffer acknowledges the application before any of that, which is why only `fsync` and `close` promise durability |
| Flush | A drive flush, which makes cached writes durable. A memtable flush, which writes the in-memory index out as a sorted run. Flushing the write-back buffer, which sends buffered writes to the chunk primaries |
| Snapshot | A volume snapshot, a point-in-time copy users see. A Raft snapshot, a copy of a shard's state sent to a lagging replica. A point-in-time view, a frozen read view of an LSM tree |
| Log | The disk log holding all of a disk's data. A Raft log, the ordered entries of a shard, stored inside the disk log. Operator logs, ordinary text messages |
| Lease | A session lease, held by a client daemon. A primary's read lease, held per PG. A Raft leader lease, for reads without a round trip; both of the last two ride on per-node-pair leases. In Samba, an SMB lease is a client cache right |
| Pool | A storage pool: PGs sharing one redundancy scheme. A buffer pool: memory that I/O buffers come from |
| Reserve, reservation | The disk log's reserve segments, kept for work that frees space. A pool's reservation of volume sizes with `overcommit` off. A space grant, which reserves pool space for a node's buffered writes |
| Recovery | Crash recovery: rebuilding a disk's state from its log at start. PG recovery: restoring full redundancy after a failure |
| Index | The chunk index of a disk. A MetaStore shard's state. The S3 index of keys to objects |
| Round | A group-commit round: one write and one flush. A round trip: a message and its reply |
| Seal, freeze | Seal makes a chunk immutable so it can be encoded. Freeze keeps one version of a chunk for a snapshot |
| Write amplification | Extra writes caused by the cleaner. Extra writes inside an SSD by its own firmware |
| Garbage collection | The cleaner reclaiming log segments. Go's collector reclaiming heap memory |
