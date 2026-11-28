package chunkstore

import (
	"cmp"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lease"
	"github.com/nvcnvn/rock-solid/internal/raftshard"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// maxMembers is the widest PG a replica primary fans out to, leaving members included.
const maxMembers = 8

// ResendTimer is the timer on which a primary sends its members again, in order, the writes a lost connection left unacknowledged.
const ResendTimer core.TimerID = 0x72736e64

// ResendAfter is how long after a PeerLost the primary sends again: 10 ms.
const ResendAfter = core.Tick(10_000)

// RetryTimer is the timer on which a replica asks the control plane again for the table and the intervals no table has answered yet.
const RetryTimer core.TimerID = 0x72747279

// RetryAfter is how often a replica asks again: 1 s, a heartbeat.
const RetryAfter = core.Tick(1_000_000)

// LeaseTimer is the timer on which a replica asks its grantors for leases and heartbeats the holders of its grants.
const LeaseTimer core.TimerID = 0x6c736574

// ReplicaConfig is a disk's place in the cluster.
type ReplicaConfig struct {
	// Disk is the disk whose store the replica runs, by its ID in the placement table.
	Disk uint32
	// Seed seeds the source the replica draws its run IDs from; Start is when its loop started, from which the restart wait counts; Lease is the lease settings, lease.DefaultConfig when zero, and never with a Resume below its.
	Seed  uint64
	Start lease.Time
	Lease lease.Config
	// Holder and Grantor are its loop's leases, which the other shards on the loop read and the replica renews, or nil for leases of its own.
	Holder  *lease.Holder
	Grantor *lease.Grantor
	// Log is the log ID of the disk's store, which its heartbeats and probe answers carry.
	Log uint64
	// Restarted is set when the store was opened rather than formatted: an earlier run may have writes in flight, so the replica asks for a new interval of each PG it is a member of in its first table, and as primary peers in it before it serves.
	Restarted bool
}

// ReplicaOutput is what a call into a Replica asks of its loop: actions, and of the control plane a table at least as new as Fetch, whole if Whole, a new interval of each PG in Asks, whose interval a spent version counter, a failed version or an index rebuild ended, and the record of each interval in Activations and in Recovered, which the loop sends the control plane in a Report beside its heartbeat. The replica asks again, and reports again, every RetryAfter until a table answers. Trace is the protocol steps the call took, for the loop's trace, and Mail the store's mail to its shards on the round driver.
type ReplicaOutput struct {
	Actions []core.Action
	Fetch   uint64
	Whole   bool
	// FetchSessions, when not 0, asks for the newest session table, which SetSessions hands the
	// replica naming this number.
	FetchSessions uint64
	Asks          []PGFields
	Activations   []PGReport
	Recovered     []PGReport
	Trace         []trace.Record
	Mail          []raftshard.Mail
}

// Reset empties the output for reuse.
func (o *ReplicaOutput) Reset() {
	o.Actions, o.Fetch, o.Whole, o.FetchSessions, o.Asks, o.Activations, o.Recovered, o.Trace, o.Mail = o.Actions[:0], 0, false, 0, o.Asks[:0], o.Activations[:0], o.Recovered[:0], o.Trace[:0], o.Mail[:0]
}

func (o *ReplicaOutput) note(name trace.Name, a, b, c, d, e uint64) {
	o.Trace = append(o.Trace, trace.Record{Name: name, Fields: [trace.MaxFields]uint64{a, b, c, d, e}})
}

func pgField(pg core.PGID) uint64 { return uint64(pg.Pool)<<32 | uint64(pg.Index) }

func (o *ReplicaOutput) fetch(epoch uint64, whole bool) {
	o.Fetch = max(o.Fetch, epoch)
	o.Whole = o.Whole || whole
}

// fetch asks for a table at least as new as epoch, whole if whole, until the replica holds one.
func (r *Replica) fetch(epoch uint64, whole bool, out *ReplicaOutput) {
	r.want, r.wantWhole = max(r.want, epoch), r.wantWhole || whole
	out.fetch(epoch, whole)
}

// retry asks again for what no table has answered: the table wanted and each lead's next interval; a stale firing asks nothing.
func (r *Replica) retry(ctx *core.Context, out *ReplicaOutput) {
	if r.retryAt == 0 || ctx.Now < r.retryAt {
		return
	}
	r.retryAt = 0
	if r.want != 0 {
		out.fetch(r.want, r.wantWhole)
	}
	if r.wantSessions {
		out.FetchSessions = r.sessionAsk
	}
	r.peerRetry(ctx, out)
	r.reportRecovered(true, out)
	r.retryReads()
	for i := range r.leads {
		if l := &r.leads[i]; l.asked {
			out.Asks = append(out.Asks, PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval})
		}
	}
	for _, a := range r.asks {
		out.Asks = append(out.Asks, PGFields{Epoch: r.table.Epoch, PG: a.PG, Interval: a.Interval})
	}
}

// armRetry arms RetryTimer while a table or an interval is asked for.
func (r *Replica) armRetry(ctx *core.Context, out *ReplicaOutput) {
	if r.retryAt != 0 || r.want == 0 && !r.wantSessions && len(r.asks) == 0 && !slices.ContainsFunc(r.leads, func(l lead) bool { return l.asked }) && !r.peering() && !r.unrecorded() && !r.waits {
		return
	}
	r.retryAt = ctx.Now + RetryAfter
	out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: RetryTimer, At: ctx.Now + RetryAfter})
}

// lead is a PG this disk is primary of: its interval in the replica's table, the interval its term began in, whether it has peered in the interval, its peering until it has, the decision of its last peering, and the client writes it holds until it has peered, or until it holds the versions peering kept for their chunk.
type lead struct {
	pg       core.PGID
	interval uint32
	since    uint32
	active   bool
	// serving is set once the lead activated, and kept while it re-peers in a later interval that kept this disk as primary: no other node can have committed to the PG since.
	serving bool
	peer    *peering
	kept    []keptChunk
	held    []heldMsg
	// asking is set once a chunk of the PG spent its counter or a version failed, which ends the interval: the primary orders nothing more in it and asks for the next once none of its versions is pending, setting asked. spent counts the held writes the store refused for a spent counter, which go ahead of those held since.
	asking bool
	asked  bool
	spent  int
	// todo is the chunks the decision left a member lacking, in chunk order, done before cursor, and
	// scan where a pass of recoverWork goes on looking; listed counts, by place in the interval's
	// members, the chunks each still lacks; reported is set once the interval was reported recovered;
	// urgent holds the places in todo of the chunks client writes wait on, which recovery takes first.
	todo     []recoverChunk
	cursor   int
	scan     int
	listed   [maxMembers]int
	reported bool
	urgent   []int
	// roomless is set once a chunk's copy found a target without room for it, and holds the lead's
	// chunks no client write waits on until something that can free room happens; full is the
	// places of the leaving members such a copy found without room, which those copies leave out
	// meanwhile rather than wait for, since the recovered report waits for no leaving member.
	roomless bool
	full     uint8
}

type heldMsg struct {
	peer core.NodeID
	msg  core.Msg
	hold uint32
}

// parkedMsg is a message waiting for a table: epoch is the placement table's it waits for, kind and
// id the request's, or ask the FetchSessions it first waited on for a session table; chunk and
// version name a member's write, whose resend replaces its copy.
type parkedMsg struct {
	epoch   uint64
	kind    Kind
	id      core.RequestID
	ask     uint64
	chunk   core.ChunkID
	version uint64
	peer    core.NodeID
	msg     core.Msg
}

type reqKind uint8

const (
	reqClient reqKind = iota + 1
	reqMember
	reqRead
	reqRecover
	reqRecoverRead
)

// storeReq is a store request in flight: who asked, and for a client's write or punch its version's state.
type storeReq struct {
	id    uint64
	kind  reqKind
	peer  core.NodeID
	msgID core.RequestID
	pg    core.PGID
	chunk core.ChunkID
	// version is the version a member's write or punch was handed, and recover marks a recovery
	// write, whose version is the chunk's write before it, or a recovery read; source is the disk a recovery write's
	// image came from, and interval the interval it was ordered in.
	version  uint64
	recover  bool
	source   uint32
	interval uint32
}

// version is a client write or punch the primary took in interval and fans out once its store orders it: the members that must hold it, those that do, and those whose latest send, numbered in sends, has neither a reply nor a PeerLost yet; done once its client is answered.
type version struct {
	req      uint64
	client   core.NodeID
	msgID    core.RequestID
	epoch    uint64
	interval uint32
	u        Update
	buf      core.IOBuf
	members  [maxMembers]uint32
	sends    [maxMembers]uint8
	n        int
	sentAt   core.Tick
	// timed is the members sent the version when it was ordered, whose first acknowledgment of that send is a latency.
	timed  uint8
	acked  uint8
	out    uint8
	logged bool
	commit bool
	done   bool
	// recover marks a recovery write, which answers no client and goes only to the members that
	// lack its chunk; ordered is set once the store ordered the version, whose number a recovery
	// write shares with the chunk's write before it and which is 0 for one that leaves it absent.
	recover bool
	ordered bool
}

// Replica runs a disk's store as a member of the PGs the placement table puts on it: primary of those it leads, ordering client writes, fanning them out and committing them once every live member holds them, and a replica of the rest.
type Replica struct {
	cfg    ReplicaConfig
	st     *Store
	table  Table
	addrs  []core.NodeID
	known  []bool
	sout   Output
	leads  []lead
	images []pgImage
	reqs   []storeReq
	vers   []version
	parked []parkedMsg
	dups   []dup
	failed []failedVer
	bases  []baseFetch
	// lost is the peers a PeerLost left with writes to send again, which new writes to them wait behind.
	lost      []core.NodeID
	unsettled []core.ChunkID
	// leftDue is set while the store may keep gated a version of a PG the table names this disk in no interval of;
	// leftDropped counts the chunks whose gated versions dropLeft dropped, which the seeded runs report.
	leftDue     bool
	leftDropped int
	// unlogged is the store request and chunk of each own version released before its records were durable, kept only to trace when they become so.
	unlogged [][2]uint64
	nextID   uint64
	frame    []byte
	armed    bool
	// want is the newest table epoch asked for and not yet held, whole if wantWhole; retryAt is when RetryTimer is armed for, 0 when it is not.
	// asks is the new intervals this disk asked for, as a restarted member, of PGs no table has moved on since.
	asks      []PGFields
	want      uint64
	wantWhole bool
	retryAt   core.Tick
	// wantSessions is set while a write waits for a session table newer than the store's; sessionAsk
	// numbers the FetchSessions, answered is the newest a SetSessions answered, and unparked is the
	// ask of the parked write being taken again.
	wantSessions                   bool
	sessionAsk, answered, unparked uint64
	// holder is the leases this disk holds as a primary, from the loops in grantors, which also holds the loops in shared; grantor the grants it gives; leaseAt when LeaseTimer is armed for, 0 when it is not; beatAt when the next heartbeat to its holders is due.
	holder   *lease.Holder
	grantor  *lease.Grantor
	lcfg     lease.Config
	rng      *core.Rand
	grantors []core.NodeID
	shared   []core.NodeID
	holders  []core.NodeID
	leaseAt  core.Tick
	leaseID  uint64
	beatAt   core.Tick
	// answers is the peering requests this disk has yet to answer as a member, peerAt when PeerTimer is armed for, seen the table epoch each holder last reported, persists the missing lists its store persists, drops the chunks it drops for give-ups; peerID numbers peering's requests, persists and drops.
	answers    []peerAsk
	peerAt     core.Tick
	seen       []seenEpoch
	persists   []persistReq
	drops      []giveUpDrop
	peerID     uint64
	peerDisks  []uint32
	peerChunks []ChunkPeer
	peerMsg    Peering
	// recs is the recoveries under way, released the held writes a recovery lets go.
	recs     []recovery
	released []heldMsg
	// replaying is set while held writes are taken again, for the fenceOnArrival plant.
	replaying bool
	waits     bool
	plant     replicaPlants
	// disks is each disk's size and the live blocks it last reported, by disk ID; pools the pools'
	// settings and outstanding grants; slots each pool's member slots on each disk in the table;
	// live and poolLive this disk's live blocks, in all and by pool, as summed at liveAt.
	disks    []diskInfo
	pools    []poolInfo
	slots    []diskSlots
	live     uint64
	poolLive []PoolLive
	liveAt   core.Tick
	// auth is the control plane's address once reporting is set; rep the heartbeat reused, and waitAt when WaitTimer is armed for, 0 when it is not.
	auth      core.NodeID
	reporting bool
	reportID  uint64
	rep       Report
	waitAt    core.Tick
}

// diskInfo is what a replica knows of a disk: its size and log reserve, its live blocks as it last reported them, and when it last acknowledged a write with them on both clocks, and, as a member of PGs this disk leads, its acknowledgment latencies since the last heartbeat and when it was last reported for a wait past member_timeout.
type diskInfo struct {
	blocks   uint64
	reserve  uint64
	live     uint64
	ackedAt  core.Tick
	ackedRaw core.Tick
	// copied is the live blocks the disk summed afresh for the last recovery write it acknowledged, at copiedAt.
	copied   uint64
	copiedAt core.Tick
	lat      latHist
	waited   core.Tick
}

// ReuseBudget is k, the grants' worth of flushes a daemon reuses its grant for between answered
// heartbeats, so that a disk must keep room for (1 + k) × the grants outstanding.
const ReuseBudget = 2

type poolInfo struct {
	id     core.PoolID
	pool   Pool
	grants uint64
}

type diskSlots struct {
	pool core.PoolID
	disk uint32
	n    uint64
}

// fillEvery is how often a replica sums its disk's live blocks for the fill it reports: 100 ms.
const fillEvery = core.Tick(100_000)

func (r *Replica) disk(d uint32) *diskInfo {
	if int(d) >= len(r.disks) {
		r.disks = append(r.disks, make([]diskInfo, int(d)+1-len(r.disks))...)
	}
	return &r.disks[d]
}

func (r *Replica) pool(id core.PoolID) *poolInfo {
	for i := range r.pools {
		if r.pools[i].id == id {
			return &r.pools[i]
		}
	}
	r.pools = append(r.pools, poolInfo{id: id})
	return &r.pools[len(r.pools)-1]
}

// SetDisk records a disk's size and its log's reserve in blocks, as the leader last heard them from the disk's node; a size of 0, not yet reported to that leader, changes nothing.
func (r *Replica) SetDisk(disk uint32, blocks, reserve uint64) {
	d := r.disk(disk)
	if blocks == 0 || d.blocks == blocks && d.reserve == reserve {
		return
	}
	d.blocks, d.reserve = blocks, reserve
	r.roomFreed()
}

// noteFill records the live blocks a member acknowledged a write with, and when, for a recovery write, which the member sums afresh.
func (r *Replica) noteFill(ctx *core.Context, disk uint32, fill uint64, recover bool) {
	d := r.disk(disk)
	if fill < d.live {
		r.roomFreed()
	}
	d.live, d.ackedAt, d.ackedRaw = fill, max(ctx.Now, 1), ctx.Raw
	if recover {
		d.copied, d.copiedAt = fill, max(ctx.Now, 1)
	}
}

// fillHeld is how long the live blocks a member acknowledged a write with outrank a lower fill its
// heartbeat relays, which can be older: longer than the oldest fill the leader passes on
// (meta.FillFresh), with the sum's 100 ms and the messages' delays. The leader cuts at FillFresh on
// the faster of its clocks and the replica holds until both of its own show fillHeld, so a slew
// only shortens the one or lengthens the other, and with either raw clock up to 4% off, 1.6 s is
// left for the heartbeat's and the answer's transit. A fill relayed lower within it, if taken,
// would restart copies it stopped, every answer.
const fillHeld = core.Tick(4_000_000)

// MemberFill records the live blocks a member disk reported on its heartbeat, as the leader relays
// them, but no lower than it acknowledged a write with until both clocks show fillHeld since.
func (r *Replica) MemberFill(ctx *core.Context, disk uint32, live uint64) {
	d := r.disk(disk)
	if d.ackedAt != 0 && (ctx.Now-min(d.ackedAt, ctx.Now) < fillHeld || ctx.Raw-min(d.ackedRaw, ctx.Raw) < fillHeld) {
		live = max(live, d.live)
	}
	if live < d.live {
		r.roomFreed()
	}
	d.live = live
}

// SetPool records a pool's settings; a pool never set takes each disk's default write_limit.
func (r *Replica) SetPool(id core.PoolID, p Pool) { r.pool(id).pool = p }

// SetGrants records the space grants outstanding on a pool, in blocks, as the leader's answer to a node's report carries them.
func (r *Replica) SetGrants(id core.PoolID, blocks uint64) {
	p := r.pool(id)
	if blocks < p.grants {
		r.roomFreed()
	}
	p.grants = blocks
}

// fill is this disk's live blocks, summed again, with each pool's, once fillEvery has passed while the store serves.
func (r *Replica) fill(ctx *core.Context) uint64 {
	if r.st.Serving() && (r.liveAt == 0 || ctx.Now-r.liveAt >= fillEvery) {
		r.live, r.liveAt = r.st.Log().LiveBlocks(), max(ctx.Now, 1)
		r.poolLive = r.st.Index().PoolsLive(r.poolLive[:0])
	}
	return r.live
}

// freshFill is this disk's live blocks summed now. It leaves admission's 100 ms sum as it is, which
// the write-limit run's invariant reads after each event as the fill the event's writes were admitted on.
func (r *Replica) freshFill(ctx *core.Context) uint64 {
	if !r.st.Serving() {
		return r.fill(ctx)
	}
	return r.st.Log().LiveBlocks()
}

// full reports whether a disk holding one of the PG's members has reached its pool's write_limit: its live blocks plus the pool's outstanding grants times the disk's share, rounded up, against ⌊its size in blocks × write_limit⌋.
func (r *Replica) full(ctx *core.Context, pg core.PGID, p *PoolTable, e *PGEntry) bool {
	pi := r.pool(pg.Pool)
	for _, m := range e.Members {
		d := r.disk(m.Disk)
		if d.blocks == 0 {
			continue
		}
		live := d.live
		if m.Disk == r.cfg.Disk {
			live = r.fill(ctx)
		}
		var slots uint64
		if j, ok := slices.BinarySearchFunc(r.slots, diskSlots{pool: pg.Pool, disk: m.Disk}, compareSlots); ok {
			slots = r.slots[j].n
		}
		if !r.plant.grantsUncounted {
			live += mulDivUp(pi.grants, slots, uint64(len(p.PGs)))
		}
		if live >= d.blocks*uint64(pi.pool.limitPercent(d.blocks))/100 {
			return true
		}
	}
	return false
}

// countSlots counts each pool's member slots on each disk in the table, in (pool, disk) order.
func (r *Replica) countSlots() {
	r.slots = r.slots[:0]
	for pi := range r.table.Pools {
		p := &r.table.Pools[pi]
		for i := range p.PGs {
			for _, m := range p.PGs[i].Members {
				key := diskSlots{pool: p.ID, disk: m.Disk}
				j, ok := slices.BinarySearchFunc(r.slots, key, compareSlots)
				if !ok {
					r.slots = slices.Insert(r.slots, j, key)
				}
				r.slots[j].n++
			}
		}
	}
}

func compareSlots(a, b diskSlots) int {
	return cmp.Or(cmp.Compare(a.pool, b.pool), cmp.Compare(a.disk, b.disk))
}

type replicaPlants struct {
	// resendNewestFirst sends a member a lost connection's writes again newest first.
	resendNewestFirst bool
	// peerInOldInterval has a restarted primary peer in the interval it led before, asking for none; mergeWhileLacking, with listerOrders, merges a partial-block write on a chunk before the primary holds the version peering kept (sub-block spike M8); mergeOnDropped merges one on a version the primary mapped and peering dropped.
	peerInOldInterval bool
	mergeWhileLacking bool
	mergeOnDropped    bool
	// noRecovery recovers nothing, as a primary without recovery would; coverAtRead picks the
	// blocks a recovery write leaves out when it reads the source rather than when it is ordered
	// (the epoch change model's RecoveryCoversAtRead).
	noRecovery  bool
	coverAtRead bool
	// copyPastGrants copies onto a disk into the room the grants outstanding on it were promised;
	// grantsUncounted admits a plain write against a member's fill alone, its pool's grants left out.
	copyPastGrants  bool
	grantsUncounted bool
	// neverAsk has a restarted primary ask for no new interval, memberNoAsk a restarted member; sendEpoch has a member accept a
	// write only under the table epoch it was sent in; sameMembers has a member accept a write
	// tagged with an earlier interval whose members' disks are those of its current one;
	// leftNeverSettles keeps the gated versions of a PG the table names this disk in no interval
	// of, and settleLeftEarly drops them once the current interval leaves it out.
	neverAsk         bool
	memberNoAsk      bool
	leftNeverSettles bool
	settleLeftEarly  bool
	sendEpoch        bool
	sameMembers      bool
	// stopPreviousOnly has peering reach only the interval just before its own, and lastActivatedOnly
	// only the last activated one; ownInterval decides
	// each version by the members of its own interval alone; activateEarly activates before every
	// member persisted its list; reportDecided reports an activation once the decision is made;
	// relistNever leaves off the missing list a member that lists a chunk but holds the kept
	// version as its newest, which with the store's clearListed has a later peering clear the
	// entry before recovery did; unlistTorn leaves off a member that holds the kept version as its
	// newest whole one with part of a later version logged.
	stopPreviousOnly  bool
	lastActivatedOnly bool
	ownInterval       bool
	activateEarly     bool
	reportDecided     bool
	relistNever       bool
	unlistTorn        bool
	// listerVetoes has a member that lists a chunk veto a version it lacks; tornVetoes has a member
	// up in the interval holding a later version in part veto it; keepPastMember has a
	// reached member outside the interval veto a version every up member holds; writeOnUnfound
	// orders a write to an unfound chunk; neverUnfound leaves a chunk with no source neither recovering nor unfound;
	// forgetImage has a decision forget that the lead's last one found the primary's image at the
	// version both keep; forgetImageDown drops the image once an interval has the disk a member
	// not up; ignoreImageFlag has a restarted disk carry no image until it decides again; mapOnDemote
	// maps a demoted primary's gated versions as a member's; imageOverDroppedRecovery takes the
	// primary's copy for its image though the version its last recovery was logged under is dropped.
	listerVetoes   bool
	tornVetoes     bool
	keepPastMember bool
	writeOnUnfound bool
	neverUnfound   bool
	// skipPastGiveUp has peering of a PG given up skip every past interval, not only those up to the give-up.
	skipPastGiveUp           bool
	forgetImage              bool
	forgetImageDown          bool
	ignoreImageFlag          bool
	mapOnDemote              bool
	imageOverDroppedRecovery bool
	// fenceOnArrival refuses an older incarnation's or fenced session's write when it reaches the
	// primary, not when the store orders it; reorderAfterOther keeps a lead's term across intervals
	// another primary led, so its held writes are ordered again; memberNoFence has a member take a
	// replicated write without checking its session.
	fenceOnArrival    bool
	reorderAfterOther bool
	memberNoFence     bool
	// admitPastLimit orders a plain write that adds live data past a member's write_limit.
	admitPastLimit bool
	// commitOnOwnLog commits a version once the primary's own log holds it, before its members do;
	// ackOnLoneCopy answers a kept version OK at activation however few copies its chunk has;
	// noDegradedHold orders a client write to a chunk with fewer than min_live members that lack
	// nothing of it; listerOrders orders one while the primary's own copy lacks something of it, but a
	// partial-block write while it lacks the kept version, unless mergeWhileLacking too.
	commitOnOwnLog bool
	ackOnLoneCopy  bool
	noDegradedHold bool
	listerOrders   bool
	// recoverSelfOnly has the primary recover only its own copies, never a member's;
	// recoverFromListed has a primary that lacks a chunk serve its recovery; clearNoSource clears a
	// list with an empty image when no member can serve the chunk; goneWhileUp refuses one version
	// in 16 at an up member as if its disk had left; readOldDisk has a disk formatted again answer a
	// recovery read for its node's old disk ID from its own, empty image.
	recoverSelfOnly   bool
	recoverFromListed bool
	clearNoSource     bool
	goneWhileUp       bool
	readOldDisk       bool
	// logPastFailed has a member log a chunk's later version of an interval past one it failed;
	// orderPastFailed has a primary go on ordering in an interval a failed version ended, and
	// neverAskOnFail never ask for the next; serveRefused maps the versions it refuses; noDemote leaves
	// the versions of a PG it no longer leads unanswered; serveAfterOther keeps serving across
	// intervals another primary led; noListAbove leaves off the missing list a member that holds a
	// version above the kept one; answerForOldDisk has a disk formatted again answer peering for its
	// old disk ID; currentMinLive judges a past interval by the current min_live.
	logPastFailed    bool
	orderPastFailed  bool
	neverAskOnFail   bool
	serveRefused     bool
	noDemote         bool
	serveAfterOther  bool
	noListAbove      bool
	answerForOldDisk bool
	currentMinLive   bool
	// readBelowMinLive serves reads in an interval with fewer than min_live live members;
	// freshGrantNoCheck grants a lease afresh to a holder that reported an older table; noLeaseCheck
	// serves reads with no lease from the other up members.
	readBelowMinLive  bool
	freshGrantNoCheck bool
	noLeaseCheck      bool
	// noResendTable orders a client write sent again as a new one; resendAtOnce answers one a
	// pending version took with that version at once, before it commits.
	noResendTable bool
	resendAtOnce  bool
}

// NewReplica runs st, a serving store, as disk cfg.Disk, drawing its first run ID; it grants no lease until its restart wait is over.
func NewReplica(cfg ReplicaConfig, st *Store) *Replica {
	lc := cfg.Lease
	if lc == (lease.Config{}) {
		lc = lease.DefaultConfig
	}
	lc.Resume = max(lc.Resume, lease.DefaultConfig.Resume)
	r := &Replica{cfg: cfg, st: st, lcfg: lc, rng: core.NewRand(cfg.Seed), holder: cfg.Holder, grantor: cfg.Grantor}
	if r.holder == nil {
		r.holder = lease.NewHolder(lc)
	}
	if r.grantor == nil {
		r.grantor = lease.NewGrantor(lc, r.rng.Uint64(), cfg.Start)
	}
	return r
}

// Store is the replica's store.
func (r *Replica) Store() *Store { return r.st }

// Table is the placement table the replica holds.
func (r *Replica) Table() *Table { return &r.table }

// SetAddr records the loop that owns disk, as the membership record publishes it.
func (r *Replica) SetAddr(disk uint32, addr core.NodeID) {
	if int(disk) >= len(r.addrs) {
		r.addrs = append(r.addrs, make([]core.NodeID, int(disk)+1-len(r.addrs))...)
		r.known = append(r.known, make([]bool, int(disk)+1-len(r.known))...)
	}
	r.addrs[disk], r.known[disk] = addr, true
	r.noteGrantors()
}

// reachable reports whether the membership record names the loop that owns disk.
func (r *Replica) reachable(disk uint32) bool { return int(disk) < len(r.known) && r.known[disk] }

// SetTable adopts t if it is newer than the table the replica holds.
func (r *Replica) SetTable(ctx *core.Context, t Table, out *ReplicaOutput) {
	if t.Epoch > r.table.Epoch {
		r.adopt(ctx, t, out)
	}
}

// ApplyDelta adopts the table d makes of the one the replica holds, or asks for the whole table after a gap or a CRC mismatch.
func (r *Replica) ApplyDelta(ctx *core.Context, d *Delta, out *ReplicaOutput) {
	if d.Epoch <= r.table.Epoch {
		return
	}
	t, err := r.table.Apply(d)
	if err != nil {
		r.fetch(d.Epoch, true, out)
		r.armRetry(ctx, out)
		return
	}
	r.adopt(ctx, t, out)
}

func (t *Table) entry(pg core.PGID) (*PoolTable, *PGEntry) {
	p := t.pool(pg.Pool)
	if p == nil || int(pg.Index) >= len(p.PGs) {
		return nil, nil
	}
	return p, &p.PGs[pg.Index]
}

// named is the pool and entry of the PG f names, or nils when f is of a table older than the pool's
// last split: its number then names a PG of the numbering before the split, which a half of another
// PG may now hold at the same interval.
func (t *Table) named(f PGFields) (*PoolTable, *PGEntry) {
	if p, e := t.entry(f.PG); p == nil || f.Epoch >= p.Split {
		return p, e
	}
	return nil, nil
}

// namedFor is named for a message about chunk c, with no entry when c is not in the PG f names under
// the table's pg_count: f is then of a table that numbers the pool otherwise, and wait reports one
// newer than the replica's, of a split it has not adopted.
func (r *Replica) namedFor(f PGFields, c core.ChunkID) (p *PoolTable, e *PGEntry, wait bool) {
	if pt := r.table.pool(f.PG.Pool); pt != nil && core.PGIndexOf(c.Hash(), uint32(len(pt.PGs))) != f.PG.Index {
		return nil, nil, f.Epoch > r.table.Epoch
	}
	p, e = r.table.named(f)
	return p, e, false
}

// primary is the PG's first up member, which orders its writes; an interval whose live members are all in maintenance has none.
func (e *PGEntry) primary() (uint32, bool) {
	for _, m := range e.Members {
		if m.Mark == MarkUp {
			return m.Disk, true
		}
	}
	return 0, false
}

func (e *PGEntry) live() int {
	n := 0
	for _, m := range e.Members {
		if m.Mark != MarkDown {
			n++
		}
	}
	return n
}

func (r *Replica) lead(pg core.PGID) int {
	i, ok := slices.BinarySearchFunc(r.leads, pg, func(l lead, pg core.PGID) int { return comparePG(l.pg, pg) })
	if !ok {
		return -1
	}
	return i
}

func comparePG(a, b core.PGID) int {
	return cmp.Or(cmp.Compare(a.Pool, b.Pool), cmp.Compare(a.Index, b.Index))
}

// adopt takes t as the replica's table. In the first table after a restart, this disk asks for a new interval of every PG it is a member of, so that a decision settles the versions its replay gated. A PG this disk leads in t keeps its term if no other primary led it in between, as the PG's listed past intervals show, and holds its client writes until it has peered in its new interval; a PG it no longer leads refuses the writes it held.
func (r *Replica) adopt(ctx *core.Context, t Table, out *ReplicaOutput) {
	out.note(trace.OutAdopt, t.Epoch, 0, 0, 0, 0)
	first := r.table.Epoch == 0
	r.noteDrops(&r.table, &t)
	prev := r.table
	r.table = t
	r.splitOff(ctx, &prev, &r.table, out)
	if t.Epoch >= r.want {
		r.want, r.wantWhole = 0, false
	}
	r.countSlots()
	r.keepImages()
	r.failed = slices.DeleteFunc(r.failed, func(f failedVer) bool {
		_, e := r.table.entry(f.pg)
		return e == nil || e.Interval != uint32(f.version>>32)
	})
	r.asks = slices.DeleteFunc(r.asks, func(a PGFields) bool {
		_, e := t.entry(a.PG)
		return e == nil || e.Interval != a.Interval
	})
	kept := r.leads[:0:0]
	for pi := range t.Pools {
		p := &t.Pools[pi]
		for i := range p.PGs {
			e := &p.PGs[i]
			pg := core.PGID{Pool: p.ID, Index: uint32(i)}
			if d, ok := e.primary(); !ok || d != r.cfg.Disk {
				if first && r.cfg.Restarted && !r.plant.memberNoAsk && slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk }) {
					a := PGFields{Epoch: t.Epoch, PG: pg, Interval: e.Interval}
					r.asks = append(r.asks, a)
					out.note(trace.OutAsk, pgField(pg), uint64(e.Interval), 0, 0, 0)
					out.Asks = append(out.Asks, a)
				}
				continue
			}
			l := lead{pg: pg, interval: e.Interval, since: e.Interval, asking: first && r.cfg.Restarted && !r.plant.peerInOldInterval}
			if j := r.lead(pg); j >= 0 {
				old := r.leads[j]
				l.held = old.held
				r.leads[j].held = nil
				switch {
				case old.interval == e.Interval:
					l = old
				case r.keptPrimary(e, old.interval) || r.plant.serveAfterOther:
					l.since, l.serving, l.kept = old.since, old.serving, old.kept
				case r.plant.reorderAfterOther:
					l.since = old.since
				}
			}
			kept = append(kept, l)
		}
	}
	for i := range r.leads {
		for _, h := range r.leads[i].held {
			r.refuseHeld(ctx, h, out)
		}
	}
	old := r.leads
	r.leads = kept
	r.dropRecoveries(old)
	r.noteGrantors()
	r.armLease(ctx, out)
	r.dropWaiting(ctx, out)
	r.demote(ctx, out)
	r.leftDue = true
	r.unpark(ctx, out)
	r.drain(ctx, out)
}

// splitOff drops this disk's leads, asks, failed versions and peering images of each pool t, the
// replica's table now, splits from prev, refusing the leads' held writes under t: they name their PG by number, and
// a split gives each number to a half of another PG, so no half may take its parent's state by number.
func (r *Replica) splitOff(ctx *core.Context, prev, t *Table, out *ReplicaOutput) {
	split := func(id core.PoolID) bool {
		p, n := prev.pool(id), t.pool(id)
		return p != nil && n != nil && len(p.PGs) != len(n.PGs)
	}
	r.failed = slices.DeleteFunc(r.failed, func(f failedVer) bool { return split(f.pg.Pool) })
	r.asks = slices.DeleteFunc(r.asks, func(a PGFields) bool { return split(a.PG.Pool) })
	r.images = slices.DeleteFunc(r.images, func(m pgImage) bool { return split(m.pg.Pool) })
	if !slices.ContainsFunc(r.leads, func(l lead) bool { return split(l.pg.Pool) }) {
		return
	}
	old := r.leads
	r.leads = slices.DeleteFunc(slices.Clone(old), func(l lead) bool { return split(l.pg.Pool) })
	for _, l := range old {
		if split(l.pg.Pool) {
			for _, h := range l.held {
				r.refuseHeld(ctx, h, out)
			}
		}
	}
	r.dropRecoveries(old)
}

// unpark takes again each parked message the replica's tables are now new enough for; one that
// waited on the session table and still does parks again.
func (r *Replica) unpark(ctx *core.Context, out *ReplicaOutput) {
	parked := r.parked
	r.parked = nil
	for _, p := range parked {
		if p.epoch > r.table.Epoch {
			r.parked = append(r.parked, p)
			continue
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: p.peer, Msg: p.msg}
		r.unparked = p.ask
		r.recv(ctx, &ev, out)
	}
	r.unparked = 0
}

// SetSessions hands the replica session table t, which its store applies if newer, and takes again
// the writes that waited for it. answers is the FetchSessions t was read after, 0 for a table sent
// unasked: a write that waited since that ask or before is refused if its session is still newer than
// the store's table, since it was never handed out.
func (r *Replica) SetSessions(ctx *core.Context, t SessionTable, answers uint64, out *ReplicaOutput) {
	r.st.SetSessions(t)
	r.answered = max(r.answered, answers)
	r.wantSessions = false
	r.unpark(ctx, out)
	r.armRetry(ctx, out)
	r.drain(ctx, out)
}

// parkSessions keeps a write from a session newer than the store's session table until the replica
// holds a table that knows it, a member's resent write replacing its copy. It reports false, for the
// caller to refuse the write, once a table read since the write first waited has answered, or when
// maxSessionWaits writes already wait.
func (r *Replica) parkSessions(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) bool {
	if r.unparked != 0 && r.unparked <= r.answered {
		return false
	}
	i := -1
	if u.Version != 0 {
		i = slices.IndexFunc(r.parked, func(p parkedMsg) bool { return p.ask != 0 && p.chunk == u.Chunk && p.version == u.Version })
	}
	p := parkedMsg{ask: r.unparked, chunk: u.Chunk, version: u.Version, peer: ev.Peer, msg: ev.Msg}
	switch {
	case i >= 0:
		p.ask = r.parked[i].ask
		freeBuf(ctx, r.parked[i].msg.Buf)
		r.parked[i] = p
	case r.sessionWaits() >= maxSessionWaits:
		return false
	default:
		if p.ask == 0 {
			r.sessionAsk++
			p.ask = r.sessionAsk
		}
		r.parked = append(r.parked, p)
	}
	r.wantSessions, out.FetchSessions = true, r.sessionAsk
	return true
}

func (r *Replica) sessionWaits() int {
	n := 0
	for _, p := range r.parked {
		if p.ask != 0 {
			n++
		}
	}
	return n
}

// pgImage is the image of PG pg that this process's last decision of it found: the chunks whose copy on this disk is the
// version it kept. A later decision that keeps the same version finds it still, though the store lists the chunk for
// versions of its own the decision dropped. It holds while every interval of the PG since, up to checked, had this disk
// as primary or as a member not up, so a lost lead keeps it, and is emptied once one did not.
type pgImage struct {
	pg      core.PGID
	checked uint32
	chunks  []ChunkVersion
}

// image is pg's entry in the images, created empty and checked up to interval k if this process has not decided pg.
func (r *Replica) image(pg core.PGID, k uint32) *pgImage {
	i, ok := slices.BinarySearchFunc(r.images, pg, func(m pgImage, pg core.PGID) int { return comparePG(m.pg, pg) })
	if !ok {
		r.images = slices.Insert(r.images, i, pgImage{pg: pg, checked: k})
	}
	return &r.images[i]
}

// imaged reports the version of chunk c in the image this disk carries for pg: its last decision's or, until this process
// decides pg, the version its store lists c for under the image flag in the PG's newest persisted decision.
func (r *Replica) imaged(pg core.PGID, c core.ChunkID) (uint64, bool) {
	if i, ok := slices.BinarySearchFunc(r.images, pg, func(m pgImage, pg core.PGID) int { return comparePG(m.pg, pg) }); ok {
		j := slices.IndexFunc(r.images[i].chunks, func(cv ChunkVersion) bool { return cv.Chunk == c })
		if j < 0 {
			return 0, false
		}
		return r.images[i].chunks[j].Version, true
	}
	p := r.table.pool(pg.Pool)
	if p == nil || r.plant.ignoreImageFlag {
		return 0, false
	}
	x, _ := r.st.Index().Chunk(pg.Pool, c)
	return x.Listed, x.Image && x.Listed != 0 && x.ListedIn == r.st.Index().Decided(pg.Pool, pg.Index, uint32(len(p.PGs)))
}

// keepImages empties each image whose PG, in an interval the table brings, has this disk neither as primary nor as a
// member not up, or no longer shows that interval.
func (r *Replica) keepImages() {
	for i := range r.images {
		m := &r.images[i]
		_, e := r.table.entry(m.pg)
		if e == nil {
			m.chunks = m.chunks[:0]
			continue
		}
		for k := m.checked + 1; k <= e.Interval && len(m.chunks) > 0; k++ {
			if !r.carries(e, k) {
				m.chunks = m.chunks[:0]
			}
		}
		m.checked = max(m.checked, e.Interval)
	}
}

// carries reports whether interval k of e had this disk as primary or as a member not up.
func (r *Replica) carries(e *PGEntry, k uint32) bool {
	ms := e.Members
	if k != e.Interval {
		j := slices.IndexFunc(e.Past, func(p PastInterval) bool { return p.Interval == k })
		if j < 0 {
			return false
		}
		ms = e.Past[j].Members
	}
	if d, ok := (&PGEntry{Members: ms}).primary(); ok && d == r.cfg.Disk {
		return true
	}
	return slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == r.cfg.Disk && m.Mark != MarkUp && !r.plant.forgetImageDown })
}

// dropLeft drops the store's gated versions of each PG the table names this disk in no interval of since the last activated one, as a decision keeping none of them would: peering reaches only those intervals' members and recovery reads only the current one's, so no decision of the PG comes here again, and gated they would hold the replay point and their segments for good. A chunk whose records are not yet resolved, or a store still replaying, waits for a later drain.
func (r *Replica) dropLeft(ctx *core.Context) {
	r.leftDue = !r.st.Serving()
	if r.leftDue || r.plant.leftNeverSettles {
		return
	}
	r.unsettled = r.st.Unsettled(r.unsettled[:0])
	for _, c := range r.unsettled {
		if pg, ok := r.pgOf(c); !ok || r.names(pg) {
			continue
		}
		if _, err := r.st.Settle(ctx, c, 0, &r.sout); err != nil {
			r.leftDue = true
		} else {
			r.leftDropped++
		}
	}
}

// names reports whether the table names this disk in PG pg's current interval or in one since its last activated; a PG it lacks counts as naming it.
func (r *Replica) names(pg core.PGID) bool {
	_, e := r.table.entry(pg)
	if e == nil {
		return true
	}
	in := func(ms []Member) bool {
		return slices.ContainsFunc(ms, func(m Member) bool { return m.Disk == r.cfg.Disk })
	}
	if in(e.Members) {
		return true
	}
	return !r.plant.settleLeftEarly && slices.ContainsFunc(e.Past, func(p PastInterval) bool { return p.Interval >= e.Activated && in(p.Members) })
}

// demote refuses the clients of the versions this disk ordered as the primary of a PG it no longer leads, since peering decides whether those versions stay. The store keeps them gated: the next decision this disk persists for the PG settles them.
func (r *Replica) demote(ctx *core.Context, out *ReplicaOutput) {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		if v.done || !v.ordered || r.lead(v.u.PG.PG) >= 0 || r.plant.noDemote {
			continue
		}
		v.done = true
		if !v.recover {
			r.takeReq(v.req)
		}
		r.answerClient(ctx, v, r.refusal(v.u.PG.PG), out)
		r.release(ctx, i)
	}
	if r.plant.mapOnDemote {
		r.unsettled = r.st.Unsettled(r.unsettled[:0])
		for _, c := range r.unsettled {
			if pg, ok := r.pgOf(c); !ok || r.lead(pg) < 0 {
				r.st.Ungate(ctx, c, &r.sout)
			}
		}
	}
}

// reportOf names interval k of pg, a PG of the replica's table, for an activation or recovered report.
func (r *Replica) reportOf(pg core.PGID, k uint32) PGReport {
	p, _ := r.table.entry(pg)
	return PGReport{PGFields: PGFields{Epoch: r.table.Epoch, PG: pg, Interval: k}, PGCount: uint32(len(p.PGs))}
}

// pgOf is the PG chunk c belongs to in the replica's table.
func (r *Replica) pgOf(c core.ChunkID) (core.PGID, bool) {
	p := r.table.pool(r.st.cfg.Pool)
	if p == nil {
		return core.PGID{}, false
	}
	return core.PGID{Pool: p.ID, Index: core.PGIndexOf(c.Hash(), uint32(len(p.PGs)))}, true
}

// keptPrimary reports whether this disk was the primary of every interval of e after from: e's past intervals list them back to its last activated one.
func (r *Replica) keptPrimary(e *PGEntry, from uint32) bool {
	for k := from + 1; k < e.Interval; k++ {
		i := slices.IndexFunc(e.Past, func(p PastInterval) bool { return p.Interval == k })
		if i < 0 {
			return false
		}
		past := PGEntry{Members: e.Past[i].Members}
		if d, ok := past.primary(); !ok || d != r.cfg.Disk {
			return false
		}
	}
	return from < e.Interval
}

// Activate is peering's end, which the replica reaches itself once every up member persisted its list: the PG this disk leads is now active in interval, and the client writes it held are ordered again, unless another primary led the PG since the interval they were held in. It returns ErrUnresolved, activating nothing, while a chunk of the PG has versions peering has not settled.
func (r *Replica) Activate(ctx *core.Context, pg core.PGID, interval uint32, out *ReplicaOutput) error {
	i := r.lead(pg)
	if i < 0 || r.leads[i].interval != interval || r.leads[i].asking && !r.leads[i].active {
		return nil
	}
	if r.unsettled = r.Unsettled(pg, r.unsettled[:0]); len(r.unsettled) > 0 {
		return ErrUnresolved
	}
	r.leads[i].active, r.leads[i].serving, r.leads[i].peer = true, true, nil
	out.note(trace.OutActivate, pgField(pg), uint64(interval), 0, 0, 0)
	held := r.leads[i].held
	r.leads[i].held = nil
	was := r.replaying
	r.replaying = true
	for _, h := range held {
		if h.hold < r.leads[i].since {
			r.refuseHeld(ctx, h, out)
			continue
		}
		ev := core.Event{Kind: core.EvNetRecv, Peer: h.peer, Msg: h.msg}
		r.recv(ctx, &ev, out)
	}
	r.replaying = was
	r.drain(ctx, out)
	return nil
}

// Unsettled appends to dst the chunks of pg with versions pending in the store, which, before the PG activates, are an earlier interval's for peering to settle: no member may hold them, so only this disk can name them.
func (r *Replica) Unsettled(pg core.PGID, dst []core.ChunkID) []core.ChunkID {
	n := len(dst)
	all := r.st.Unsettled(dst)
	dst = all[:n]
	for _, c := range all[n:] {
		if p, ok := r.pgOf(c); ok && p == pg {
			dst = append(dst, c)
		}
	}
	return dst
}

// storeFailed refuses every client write held or not yet answered once the store has failed, which then answers nothing more: the PGs will peer on another primary, which decides whether their versions stay.
func (r *Replica) storeFailed(ctx *core.Context, out *ReplicaOutput) {
	for i := range r.leads {
		held := r.leads[i].held
		r.leads[i].held, r.leads[i].spent = nil, 0
		for _, h := range held {
			r.refuseHeld(ctx, h, out)
		}
	}
	for i := len(r.vers) - 1; i >= 0; i-- {
		if v := &r.vers[i]; !v.done {
			v.done = true
			r.takeReq(v.req)
			r.answerClient(ctx, v, r.refusal(v.u.PG.PG), out)
			r.release(ctx, i)
		}
	}
	r.unlogged = r.unlogged[:0]
}

func (r *Replica) refuseHeld(ctx *core.Context, h heldMsg, out *ReplicaOutput) {
	frame, data := Frame(ctx.Pool, &h.msg)
	var u Update
	if DecodeUpdate(frame, data, &u) == nil {
		r.reply(ctx, h.peer, u.ID, r.refusal(u.PG.PG), out)
	}
	if h.msg.Buf != core.NoBuf {
		ctx.Pool.Free(h.msg.Buf)
	}
}

// OnEvent takes a message, a lost peer, a disk completion or one of the store's timers.
func (r *Replica) OnEvent(ctx *core.Context, ev *core.Event, out *ReplicaOutput) {
	switch ev.Kind {
	case core.EvNetRecv:
		r.recv(ctx, ev, out)
	case core.EvPeerLost:
		r.peerLost(ctx, ev.Peer, out)
	case core.EvTimer:
		if ev.Timer == ResendTimer {
			r.resend(out)
			break
		}
		if ev.Timer == RetryTimer {
			r.retry(ctx, out)
			break
		}
		if ev.Timer == LeaseTimer {
			r.leaseTick(ctx, out)
			break
		}
		if ev.Timer == PeerTimer {
			r.peerTick(ctx)
			break
		}
		if ev.Timer == ReportTimer {
			r.report(ctx, out)
			break
		}
		if ev.Timer == WaitTimer {
			r.waited(ctx, out)
			break
		}
		r.st.OnEvent(ctx, ev, &r.sout)
	case core.EvDiskDone:
		r.st.OnEvent(ctx, ev, &r.sout)
	case core.EvResumed:
		r.resumed(ctx, out)
	}
	r.drain(ctx, out)
}

// OnLog takes an event of the store's log, or the nil that follows each disk completion, from the
// loop that owns the log.
func (r *Replica) OnLog(ctx *core.Context, ev *disklog.Event, out *ReplicaOutput) {
	r.st.OnLog(ctx, ev, &r.sout)
	r.drain(ctx, out)
}

func (r *Replica) recv(ctx *core.Context, ev *core.Event, out *ReplicaOutput) {
	frame, data := Frame(ctx.Pool, &ev.Msg)
	buf := ev.Msg.Buf
	kind, err := Peek(frame)
	if err != nil {
		freeBuf(ctx, buf)
		return
	}
	switch kind {
	case KindHello:
		var h Hello
		if DecodeHello(frame, data, &h) == nil {
			r.st.SeeIncarnation(h.Session, h.Incarnation)
		}
	case KindWrite, KindPunch:
		var u Update
		if DecodeUpdate(frame, data, &u) != nil {
			break
		}
		r.clientUpdate(ctx, ev, &u, out)
		return
	case KindReplicaWrite, KindReplicaPunch, KindRecover:
		var u Update
		if DecodeUpdate(frame, data, &u) != nil {
			break
		}
		r.memberUpdate(ctx, ev, &u, out)
		return
	case KindRead, KindRecoverRead:
		var rd Read
		if DecodeRead(frame, data, &rd) == nil && r.read(ctx, ev, &rd, out) {
			return
		}
	case KindAck:
		var rp Response
		if DecodeResponse(frame, data, &rp) == nil {
			r.ack(ctx, ev.Peer, &rp, out)
		}
	case KindReadReply:
		var rp Response
		if DecodeResponse(frame, data, &rp) == nil && (r.baseReply(ctx, &rp, buf, out) || r.recoverReply(ctx, ev.Peer, &rp, buf, out)) {
			return
		}
	case KindLeaseRequest:
		var lr LeaseRequest
		if DecodeLeaseRequest(frame, data, &lr) == nil {
			r.leaseRequest(ctx, ev.Peer, &lr, out)
		}
	case KindGrant:
		var g Grant
		if DecodeGrant(frame, data, &g) == nil {
			r.grant(ctx, ev.Peer, &g, out)
		}
	case KindHeartbeat:
		var hb Heartbeat
		if DecodeHeartbeat(frame, data, &hb) == nil {
			r.heartbeat(ctx, ev.Peer, &hb, out)
		}
	case KindProbe:
		var p Probe
		if DecodeProbe(frame, data, &p) == nil {
			r.probe(ctx, ev.Peer, &p, out)
		}
	case KindPeer, KindPeerReply, KindPersist, KindPersistAck:
		p := &r.peerMsg
		if DecodePeering(frame, data, p) != nil {
			break
		}
		switch {
		case kind == KindPeerReply:
			r.peerReplied(ctx, ev.Peer, p, out)
		case kind == KindPersistAck:
			r.persistAck(ev.Peer, p, out)
		case kind == KindPeer && r.peerAsked(ctx, ev, p, out), kind == KindPersist && r.persistAsked(ctx, ev, p, out):
			return
		}
	}
	freeBuf(ctx, buf)
}

// askBase sends the next up member of the PG a read of the base block at its version, or fails the write once none is left to ask: after a member's refusal or a lost connection to it.
func (r *Replica) askBase(ctx *core.Context, i int, out *ReplicaOutput) {
	f := &r.bases[i]
	_, e := r.table.entry(f.pg)
	for e != nil && f.tried < len(e.Members) {
		m := e.Members[f.tried]
		f.tried++
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk {
			continue
		}
		rd := Read{ID: f.rid, PG: PGFields{Epoch: r.table.Epoch, PG: f.pg, Interval: e.Interval}, Chunk: f.Chunk,
			Off: uint32(f.Block) * core.BlockSize, Len: core.BlockSize, HasAt: true, At: f.Version}
		r.frame = AppendRead(r.frame[:0], &rd)
		a := core.Action{Kind: core.ActNetSend, Peer: r.addr(m.Disk)}
		a.Msg.Len = uint16(copy(a.Msg.Body[:], r.frame))
		out.Actions = append(out.Actions, a)
		f.asked = a.Peer
		return
	}
	id, b := f.ID, f.Block
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, b, core.NoBuf, disklog.ErrBadChecksum, &r.sout)
}

// baseReply takes a member's answer to a base fetch, reporting whether it was one; it takes buf.
func (r *Replica) baseReply(ctx *core.Context, rp *Response, buf core.IOBuf, out *ReplicaOutput) bool {
	i := slices.IndexFunc(r.bases, func(f baseFetch) bool { return f.rid == rp.ID })
	if i < 0 {
		return false
	}
	if rp.Status != StatusOK {
		freeBuf(ctx, buf)
		r.askBase(ctx, i, out)
		return true
	}
	id, b := r.bases[i].ID, r.bases[i].Block
	r.bases = slices.Delete(r.bases, i, i+1)
	r.st.SupplyBase(ctx, id, b, buf, nil, &r.sout)
	return true
}

func freeBuf(ctx *core.Context, b core.IOBuf) {
	if b != core.NoBuf {
		ctx.Pool.Free(b)
	}
}

// refusal is the refusal of a request about pg: this replica's table epoch and pg's interval in it.
func (r *Replica) refusal(pg core.PGID) Response {
	rp := Response{Status: StatusRefused, Epoch: r.table.Epoch, Interval: 1}
	if _, e := r.table.entry(pg); e != nil {
		rp.Interval = e.Interval
	}
	return rp
}

// park keeps a message tagged with a newer table than the replica holds until it has one at least as new as the
// sender's; a retry of a request parked from the same peer replaces its copy, so retries hold one buffer.
func (r *Replica) park(ctx *core.Context, ev *core.Event, epoch uint64, out *ReplicaOutput) {
	frame, _ := Frame(ctx.Pool, &ev.Msg)
	p := parkedMsg{epoch: epoch, kind: Kind(frame[1]), id: core.RequestID(le.Uint64(frame[2:])), peer: ev.Peer, msg: ev.Msg}
	if i := slices.IndexFunc(r.parked, func(q parkedMsg) bool { return q.ask == 0 && q.peer == p.peer && q.kind == p.kind && q.id == p.id }); i >= 0 {
		freeBuf(ctx, r.parked[i].msg.Buf)
		r.parked[i] = p
	} else {
		r.parked = append(r.parked, p)
	}
	r.fetch(epoch, false, out)
}

// clientUpdate takes a client's write or punch at the PG's primary: it is refused unless this disk leads the PG with at least min_live members live and the client's interval falls within the primary's term, held while the PG peers, and otherwise ordered. Any write to a chunk the primary's own copy lacks something of is held until recovery clears it, since the one copy a write surely lands on is the primary's: one logged by members that list the chunk alone, its clean copies cut off, would be kept with no clean copy. Any write to a chunk with fewer than min_live members that lack nothing of it is held until recovery clears enough, since its OK would otherwise rest on members that cannot serve the chunk, up to maxHeld writes held; recovery takes a chunk a write waits on first. Any write is answered unfound when no member can be the chunk's source: the primary lists such a chunk, so the write would wait for ever for a recovery with nothing to copy.
func (r *Replica) clientUpdate(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) {
	if r.plant.fenceOnArrival && !r.replaying && u.Session != 0 && u.Incarnation < r.st.sessions[u.Session] {
		freeBuf(ctx, ev.Msg.Buf)
		r.reply(ctx, ev.Peer, u.ID, r.refusal(u.PG.PG), out)
		return
	}
	if f := r.st.Sessions(); f.Newer(u.Session) && r.parkSessions(ctx, ev, u, out) {
		return
	} else if f.Newer(u.Session) || f.Fenced(u.Session) {
		freeBuf(ctx, ev.Msg.Buf)
		r.reply(ctx, ev.Peer, u.ID, r.refusal(u.PG.PG), out)
		return
	}
	r.st.SeeIncarnation(u.Session, u.Incarnation)
	p, e, wait := r.namedFor(u.PG, u.Chunk)
	i := r.lead(u.PG.PG)
	switch {
	case wait || e != nil && i < 0 && u.PG.Interval > e.Interval && u.PG.Epoch > r.table.Epoch:
		r.park(ctx, ev, u.PG.Epoch, out)
		return
	case e == nil || i < 0 || u.PG.Interval < r.leads[i].since || e.live() < int(p.MinLive) || r.st.Err() != nil:
	case u.PG.Interval > r.leads[i].interval:
		if u.PG.Epoch > r.table.Epoch {
			r.park(ctx, ev, u.PG.Epoch, out)
			return
		}
	case u.Kind == KindWrite && !u.Granted && !r.plant.admitPastLimit && r.full(ctx, u.PG.PG, p, e) && !r.st.Overwrites(u.Chunk, u.Off, u.Len, u.Snapshot):
		freeBuf(ctx, ev.Msg.Buf)
		r.reply(ctx, ev.Peer, u.ID, Response{Status: StatusNoSpace}, out)
		return
	case !r.leads[i].active || r.leads[i].asking && !r.plant.orderPastFailed || r.leads[i].isUnfound(u.Chunk) && !r.plant.writeOnUnfound || r.leads[i].short(e, int(p.MinLive), u.Chunk) && !r.plant.noDegradedHold ||
		r.leads[i].listsOwn(e, r.cfg.Disk, u.Chunk) && !(r.plant.listerOrders && (edgeBytes(u.Off, u.Len) == 0 || !r.leads[i].lacks(u.Chunk) || r.plant.mergeWhileLacking)):
		l := &r.leads[i]
		if l.active && !l.asking && l.isUnfound(u.Chunk) && !r.plant.writeOnUnfound {
			freeBuf(ctx, ev.Msg.Buf)
			r.reply(ctx, ev.Peer, u.ID, Response{Status: StatusUnfound}, out)
			return
		}
		if l.active && !l.asking && len(l.held) >= maxHeld {
			break
		}
		if l.active && !l.asking {
			l.urge(u.Chunk)
		}
		l.held = append(l.held, heldMsg{peer: ev.Peer, msg: ev.Msg, hold: l.interval})
		return
	default:
		if !r.resent(ctx, ev, u, uint32(len(p.PGs)), out) {
			r.order(ctx, ev, u, r.leads[i].interval, uint32(len(p.PGs)))
		}
		return
	}
	freeBuf(ctx, ev.Msg.Buf)
	r.reply(ctx, ev.Peer, u.ID, r.refusal(u.PG.PG), out)
}

// resent answers a client write sent again from the resend table, after the holds every write
// waits on, rather than ordering it again: one at or below its session's watermark Stale, since its
// client heard an earlier attempt's answer; one a version this primary has pending took, with that
// version's outcome; and one a version its disk has mapped took, OK with that version.
func (r *Replica) resent(ctx *core.Context, ev *core.Event, u *Update, pgs uint32, out *ReplicaOutput) bool {
	if r.plant.noResendTable {
		return false
	}
	ver, stale := r.st.Index().Resent(r.st.cfg.Pool, u.Chunk, pgs, u.Session, u.Incarnation, uint64(u.ID))
	j := slices.IndexFunc(r.vers, func(v version) bool {
		return !v.recover && !v.done && v.msgID == u.ID && v.u.Chunk == u.Chunk && v.u.Session == u.Session && v.u.Incarnation == u.Incarnation
	})
	switch {
	case stale:
		out.note(trace.OutStale, uint64(u.ID), u.Chunk.Hash(), u.Session<<32|u.Incarnation, 0, 0)
		r.reply(ctx, ev.Peer, u.ID, Response{Status: StatusStale}, out)
	case j >= 0 && r.plant.resendAtOnce && r.vers[j].ordered:
		out.note(trace.OutReanswer, u.Chunk.Hash(), r.vers[j].u.Version, uint64(u.ID), 0, 0)
		r.reply(ctx, ev.Peer, u.ID, Response{Version: r.vers[j].u.Version}, out)
	case j >= 0:
		r.dups = append(r.dups, dup{orig: r.vers[j].req, peer: ev.Peer, msgID: u.ID, chunk: u.Chunk})
	case ver != 0:
		out.note(trace.OutReanswer, u.Chunk.Hash(), ver, uint64(u.ID), 0, 0)
		r.reply(ctx, ev.Peer, u.ID, Response{Version: ver}, out)
	default:
		return false
	}
	freeBuf(ctx, ev.Msg.Buf)
	return true
}

func (r *Replica) order(ctx *core.Context, ev *core.Event, u *Update, interval, pgs uint32) {
	id := r.newReq(storeReq{kind: reqClient, peer: ev.Peer, msgID: u.ID, pg: u.PG.PG, chunk: u.Chunk})
	req := Request{ID: id, Chunk: u.Chunk, Off: u.Off, Len: u.Len, Buf: ev.Msg.Buf, HasVer: u.HasExpect, Ver: u.Expect,
		Epoch: u.Snapshot, Interval: interval, Gated: true, Session: u.Session, Incarnation: u.Incarnation,
		Request: uint64(u.ID), Oldest: u.Oldest, PGs: pgs}
	if l := r.lead(u.PG.PG); l >= 0 {
		req.Lacking = r.leads[l].lacks(u.Chunk)
	}
	if r.plant.fenceOnArrival {
		req.Session = 0
	}
	r.vers = append(r.vers, version{req: id, client: ev.Peer, msgID: u.ID, epoch: r.table.Epoch, interval: interval, u: *u})
	if u.Kind == KindWrite {
		r.st.Write(ctx, req, &r.sout)
	} else {
		freeBuf(ctx, ev.Msg.Buf)
		r.st.Punch(ctx, req, &r.sout)
	}
}

// memberUpdate takes a write or punch the PG's primary ordered: accepted only in the PG's current interval, by an up member, and refused once an earlier version of its chunk in the interval failed here, so the member logs a chunk's versions without a gap. A version sent again while its first copy is in flight is answered with that copy's outcome; one whose copy is done is refused, since after a failure or a restart the member may hold only some of its records.
func (r *Replica) memberUpdate(ctx *core.Context, ev *core.Event, u *Update, out *ReplicaOutput) {
	p, e, wait := r.namedFor(u.PG, u.Chunk)
	if wait || e != nil && u.PG.Interval > e.Interval && u.PG.Epoch > r.table.Epoch {
		r.park(ctx, ev, u.PG.Epoch, out)
		return
	}
	newer := r.st.Sessions().Newer(u.Session)
	if newer && r.parkSessions(ctx, ev, u, out) {
		return
	}
	recover := u.Kind == KindRecover
	gone := e != nil && (!slices.ContainsFunc(e.Members, func(m Member) bool { return m.Disk == r.cfg.Disk && m.Mark == MarkUp }) || r.plant.goneWhileUp && u.Version&15 == 7)
	failed := slices.IndexFunc(r.failed, func(f failedVer) bool {
		return f.chunk == u.Chunk && uint32(f.version>>32) == u.PG.Interval && (f.version == u.Version || f.version < u.Version && !r.plant.logPastFailed || recover)
	})
	unready := r.unready()
	if failed >= 0 {
		unready |= r.failed[failed].unready
	}
	refuse := e == nil || newer || r.st.Sessions().Fenced(u.Session) && !r.plant.memberNoFence || u.PG.Interval != e.Interval && !r.sameMembersAt(e, u.PG.Interval) || r.plant.sendEpoch && u.PG.Epoch != r.table.Epoch || gone || failed >= 0
	if !refuse && (recover || u.Version <= r.st.Newest(u.Chunk)) {
		if j := slices.IndexFunc(r.reqs, func(q storeReq) bool {
			return q.kind == reqMember && q.chunk == u.Chunk && q.version == u.Version && q.recover == recover
		}); j >= 0 {
			freeBuf(ctx, ev.Msg.Buf)
			r.dups = append(r.dups, dup{orig: r.reqs[j].id, peer: ev.Peer, msgID: u.ID})
			return
		}
		if !recover || r.listedIn(u.Chunk) == 0 {
			r.noteFailed(failedVer{pg: u.PG.PG, chunk: u.Chunk, version: u.Version, unready: unready})
			refuse = true
		}
	}
	if refuse {
		if e != nil && u.PG.Interval == e.Interval {
			var reason, rec uint64
			if gone {
				reason = 1
			}
			if recover {
				rec = 1
			}
			out.note(trace.OutMemberFail, u.Chunk.Hash(), u.Version, reason, rec, unready)
			r.noteFailed(failedVer{pg: u.PG.PG, chunk: u.Chunk, version: u.Version, unready: unready})
		}
		freeBuf(ctx, ev.Msg.Buf)
		rp := r.refusal(u.PG.PG)
		rp.Kind, rp.Fill = KindAck, r.fill(ctx)
		r.answer(ctx, ev.Peer, u.ID, &rp, core.NoBuf, out)
		return
	}
	if recover {
		out.note(trace.OutRecovering, u.Chunk.Hash(), uint64(u.Source), uint64(u.PG.Interval), u.Version, 0)
	} else {
		out.note(trace.OutAccepted, u.Chunk.Hash(), u.Version, 0, 0, 0)
	}
	id := r.newReq(storeReq{kind: reqMember, peer: ev.Peer, msgID: u.ID, pg: u.PG.PG, chunk: u.Chunk, version: u.Version, recover: recover, source: u.Source, interval: u.PG.Interval})
	req := Request{ID: id, Chunk: u.Chunk, Off: u.Off, Len: u.Len, Buf: ev.Msg.Buf, Epoch: u.Snapshot, Version: u.Version, Session: u.Session,
		Incarnation: u.Incarnation, Request: u.Request, Oldest: u.Oldest, PGs: uint32(len(p.PGs))}
	if r.plant.memberNoFence {
		req.Session = 0
	}
	if recover {
		req.Version, req.HasVer, req.Ver, req.Interval, req.Recover = 0, true, u.Version, u.PG.Interval, true
	}
	if u.Kind == KindReplicaWrite || recover {
		r.st.Write(ctx, req, &r.sout)
	} else {
		r.st.Punch(ctx, req, &r.sout)
	}
}

// sameMembersAt reports, under the sameMembers plant, whether e's past interval k had the disks of its current members, in order.
func (r *Replica) sameMembersAt(e *PGEntry, k uint32) bool {
	if !r.plant.sameMembers {
		return false
	}
	i := slices.IndexFunc(e.Past, func(p PastInterval) bool { return p.Interval == k })
	return i >= 0 && slices.EqualFunc(e.Past[i].Members, e.Members, func(a, b Member) bool { return a.Disk == b.Disk })
}

// baseFetch is a base block the primary asks another member for, tried from member tried of the PG, under read ID rid, of the loop at asked.
type baseFetch struct {
	BaseFetch
	pg    core.PGID
	tried int
	rid   core.RequestID
	asked core.NodeID
}

// dup is a write sent again to a member, or by a client to the primary, while its first copy, store request orig, is in flight; it is answered as that copy is.
type dup struct {
	orig  uint64
	peer  core.NodeID
	msgID core.RequestID
	chunk core.ChunkID
}

// unready is a member refusal's trace field: 1 while the store is not serving, as while it replays its log after a
// restart, rebuilds its index or is stopped by a disk fault.
func (r *Replica) unready() uint64 {
	if r.st.Serving() {
		return 0
	}
	return 1
}

// failedVer is a version of a chunk a member failed to log, after which it refuses the chunk's later versions in that interval.
type failedVer struct {
	pg      core.PGID
	chunk   core.ChunkID
	version uint64
	// unready is the trace field of the refusal that failed the version, which the later refusals it causes carry.
	unready uint64
}

func (r *Replica) noteFailed(f failedVer) {
	if !slices.ContainsFunc(r.failed, func(g failedVer) bool { return g.pg == f.pg && g.chunk == f.chunk && g.version == f.version }) {
		r.failed = append(r.failed, f)
	}
}

// read serves a client's read at the PG's primary while it may serve reads, a read of a frozen version at any member, and at a member a base its primary fetches, the blocks at the version the primary's index names; it reports whether it parked the read for a newer table.
func (r *Replica) read(ctx *core.Context, ev *core.Event, rd *Read, out *ReplicaOutput) (parked bool) {
	if rd.Recover {
		return r.recoveryAsked(ctx, ev, rd, out)
	}
	p, e, wait := r.namedFor(rd.PG, rd.Chunk)
	if wait {
		r.park(ctx, ev, rd.PG.Epoch, out)
		return true
	}
	i := r.lead(rd.PG.PG)
	primary := e != nil && i >= 0 && r.leads[i].serving && rd.PG.Interval >= r.leads[i].since && rd.PG.Interval <= r.leads[i].interval && r.readable(ctx, p, e)
	base := false
	if e != nil && i < 0 && rd.HasAt && rd.PG.Interval == e.Interval {
		d, ok := e.primary()
		base = ok && r.addr(d) == ev.Peer
	}
	if e == nil || i >= 0 && !primary {
		rp := r.refusal(rd.PG.PG)
		rp.Kind = KindReadReply
		r.answer(ctx, ev.Peer, rd.ID, &rp, core.NoBuf, out)
		return false
	}
	if primary && r.leads[i].isUnfound(rd.Chunk) {
		rp := Response{Kind: KindReadReply, Status: StatusUnfound}
		r.answer(ctx, ev.Peer, rd.ID, &rp, core.NoBuf, out)
		return false
	}
	id := r.newReq(storeReq{kind: reqRead, peer: ev.Peer, msgID: rd.ID, pg: rd.PG.PG, chunk: rd.Chunk})
	r.st.Read(ctx, Request{ID: id, Chunk: rd.Chunk, Off: rd.Off, Len: rd.Len, HasVer: rd.HasAt, Ver: rd.At, FrozenOnly: !primary && !base, Base: base}, &r.sout)
	return false
}

func (r *Replica) newReq(q storeReq) uint64 {
	r.nextID++
	q.id = r.nextID
	r.reqs = append(r.reqs, q)
	return q.id
}

func (r *Replica) reqIndex(id uint64) (int, bool) {
	return slices.BinarySearchFunc(r.reqs, id, func(q storeReq, id uint64) int { return cmp.Compare(q.id, id) })
}

func (r *Replica) takeReq(id uint64) (storeReq, bool) {
	i, ok := r.reqIndex(id)
	if !ok {
		return storeReq{}, false
	}
	q := r.reqs[i]
	r.reqs = slices.Delete(r.reqs, i, i+1)
	return q, true
}

// versionOf finds a version by its store request, which grows in the order r.vers holds them and numbers its sends to members.
func (r *Replica) versionOf(req uint64) int {
	i, ok := slices.BinarySearchFunc(r.vers, req, func(v version, req uint64) int { return cmp.Compare(v.req, req) })
	if !ok {
		return -1
	}
	return i
}

// drain handles what the store answered until it has nothing more to say: the versions it ordered, those durable on its disk and the requests it finished. A rebuild of the store's index ends the interval of every PG this disk leads, as a restart would: the rebuilt index maps what the log holds, versions peering has yet to decide included.
func (r *Replica) drain(ctx *core.Context, out *ReplicaOutput) {
	if r.leftDue {
		r.dropLeft(ctx)
	}
	r.dropGivenUp(ctx)
	r.drainStore(ctx, out)
	r.peerWork(ctx, out)
	r.recoverWork(ctx, out)
	for len(r.sout.Ordered)+len(r.sout.Logged)+len(r.sout.Replies)+len(r.sout.Bases) > 0 || r.sout.Rebuilt {
		r.drainStore(ctx, out)
		r.peerWork(ctx, out)
		r.recoverWork(ctx, out)
	}
	if x := r.st.Index(); x != nil {
		for _, e := range x.trims {
			out.note(trace.OutTrim, e.Chunk.Hash(), e.Session<<32|e.Incarnation, e.Oldest, 0, 0)
		}
		x.trims = x.trims[:0]
	}
	r.reportRecovered(false, out)
	if r.st.Err() != nil {
		r.storeFailed(ctx, out)
	}
	out.Actions = append(out.Actions, r.sout.Actions...)
	r.sout.Actions = r.sout.Actions[:0]
	r.ask(out)
	r.armRetry(ctx, out)
}

// drainStore handles what the store has said so far, which it then forgets but for its actions.
func (r *Replica) drainStore(ctx *core.Context, out *ReplicaOutput) {
	for o, l, p, b := 0, 0, 0, 0; o < len(r.sout.Ordered) || l < len(r.sout.Logged) || p < len(r.sout.Replies) || b < len(r.sout.Bases); {
		for ; b < len(r.sout.Bases); b++ {
			r.nextID++
			f := baseFetch{BaseFetch: r.sout.Bases[b], rid: core.RequestID(r.nextID)}
			if i, ok := r.reqIndex(f.ID); ok {
				f.pg = r.reqs[i].pg
			}
			r.bases = append(r.bases, f)
			r.askBase(ctx, len(r.bases)-1, out)
		}
		for ; o < len(r.sout.Ordered); o++ {
			r.ordered(ctx, r.sout.Ordered[o], out)
		}
		for ; l < len(r.sout.Logged); l++ {
			r.logged(ctx, r.sout.Logged[l], out)
		}
		for ; p < len(r.sout.Replies); p++ {
			r.finished(ctx, r.sout.Replies[p], out)
		}
	}
	if r.sout.Rebuilt {
		r.leftDue = true
		for i := range r.leads {
			r.leads[i].active, r.leads[i].serving, r.leads[i].asking, r.leads[i].peer = false, false, true, nil
		}
		r.recs = r.recs[:0]
	}
	out.Mail = append(out.Mail, r.sout.Mail...)
	acts := r.sout.Actions
	r.sout.Reset()
	r.sout.Actions = acts
}

// ordered fans a version out to every up member of its interval but this disk, in parallel with the store logging it, unless the interval has ended since the client's write was taken or the PG is wider than maxMembers: then the version fails at once. A member a resend is due to gets it with the resend.
func (r *Replica) ordered(ctx *core.Context, o Reply, out *ReplicaOutput) {
	i := r.versionOf(o.ID)
	v := &r.vers[i]
	v.u.Version, v.buf, v.ordered = o.Version, o.Buf, true
	if !v.recover {
		out.note(trace.OutOrder, uint64(v.msgID), v.u.Chunk.Hash(), o.Version, v.u.Session<<32|v.u.Incarnation, 0)
	} else if r.recoveryTarget(v, r.cfg.Disk) {
		out.note(trace.OutRecovering, v.u.Chunk.Hash(), uint64(v.u.Source), uint64(v.interval), o.Version, 0)
	}
	l := r.lead(v.u.PG.PG)
	if l < 0 || r.leads[l].asking && !r.plant.orderPastFailed || r.leads[l].interval != v.interval {
		r.fail(ctx, i, Response{}, out)
		return
	}
	_, e := r.table.entry(v.u.PG.PG)
	for _, m := range e.Members {
		if m.Mark != MarkUp || m.Disk == r.cfg.Disk || v.recover && !r.recoveryTarget(v, m.Disk) {
			continue
		}
		if v.n == maxMembers {
			r.fail(ctx, i, Response{}, out)
			return
		}
		v.members[v.n] = m.Disk
		v.n++
	}
	v.sentAt = ctx.Now
	for k := range v.n {
		if !slices.Contains(r.lost, r.addr(v.members[k])) {
			r.send(v, k, out)
			v.timed |= 1 << k
		}
	}
	if v.n > 0 {
		r.armWait(ctx, out)
	}
}

// sendShift places a send's number in the ID of the update it carries, above the version's store request, so an answer to a send a lost connection left behind is told from the answer to the send after it, whose buffer is still in the network.
// The 8-bit number wraps after 256 sends of one version to one member, which ResendAfter spaces over at least 2.56 s: an answer that late waits on a member write stalled past member_timeout (1 s), whose disk failure detection marks down, ending the interval and its resends.
const sendShift = 56

func (r *Replica) send(v *version, k int, out *ReplicaOutput) {
	v.sends[k]++
	u := v.u
	if !v.recover {
		u.Request = uint64(v.msgID)
	}
	u.ID, u.HasExpect, u.Expect, u.Granted = core.RequestID(v.req)|core.RequestID(v.sends[k])<<sendShift, false, 0, false
	u.PG.Epoch, u.PG.Interval = v.epoch, v.interval
	switch u.Kind {
	case KindWrite:
		first, last := spanOf(u.Off, u.Len)
		u.Kind, u.Off, u.Len = KindReplicaWrite, uint32(first)*core.BlockSize, uint32(last-first+1)*core.BlockSize
	case KindPunch:
		u.Kind = KindReplicaPunch
	}
	r.frame = AppendUpdate(r.frame[:0], &u)
	a := core.Action{Kind: core.ActNetSend, Peer: r.addr(v.members[k])}
	a.Msg.Len, a.Msg.Buf = uint16(copy(a.Msg.Body[:], r.frame)), v.buf
	out.Actions = append(out.Actions, a)
	v.out |= 1 << k
}

func (r *Replica) addr(disk uint32) core.NodeID {
	if int(disk) < len(r.addrs) {
		return r.addrs[disk]
	}
	return core.NodeID{}
}

func (r *Replica) logged(ctx *core.Context, l Reply, out *ReplicaOutput) {
	i := r.versionOf(l.ID)
	if i < 0 {
		for j, u := range r.unlogged {
			if u[0] == l.ID {
				if l.Err == nil {
					out.note(trace.OutLogged, u[1], l.Version, 0, 0, 0)
				}
				r.unlogged = slices.Delete(r.unlogged, j, j+1)
				break
			}
		}
		return
	}
	if !r.vers[i].ordered {
		r.vers[i].u.Version, r.vers[i].ordered = l.Version, true
	}
	if l.Err != nil {
		r.fail(ctx, i, Response{}, out)
		return
	}
	r.vers[i].logged = true
	if !r.vers[i].recover {
		out.note(trace.OutLogged, r.vers[i].u.Chunk.Hash(), r.vers[i].u.Version, 0, 0, 0)
	}
	r.tryCommit(ctx, i)
}

func statusOf(err error) Status {
	switch {
	case err == nil:
		return StatusOK
	case errors.Is(err, disklog.ErrNoSpace):
		return StatusNoSpace
	case errors.Is(err, ErrVersion):
		return StatusMismatch
	}
	return StatusRefused
}

// ack takes a member's acknowledgment of a version, which ends its send only if it answers the latest.
func (r *Replica) ack(ctx *core.Context, peer core.NodeID, rp *Response, out *ReplicaOutput) {
	req := uint64(rp.ID & (1<<sendShift - 1))
	i := r.versionOf(req)
	if i < 0 {
		return
	}
	v := &r.vers[i]
	k := -1
	for j := range v.n {
		if r.addr(v.members[j]) == peer {
			k = j
		}
	}
	if k < 0 {
		return
	}
	if uint8(rp.ID>>sendShift) == v.sends[k] {
		v.out &^= 1 << k
	}
	r.noteFill(ctx, v.members[k], rp.Fill, v.recover)
	switch {
	case rp.Status == StatusOK && rp.Version == v.u.Version:
		if v.acked&(1<<k) == 0 && v.timed&(1<<k) != 0 && uint8(rp.ID>>sendShift) == 1 {
			r.noteAck(v.members[k], ctx.Now-v.sentAt)
		}
		v.acked |= 1 << k
		r.tryCommit(ctx, i)
	case rp.Status != StatusOK:
		if rp.Status == StatusRefused && rp.Epoch > r.table.Epoch {
			r.fetch(rp.Epoch, false, out)
		}
		r.fail(ctx, i, *rp, out)
	}
	if i = r.versionOf(req); i >= 0 {
		r.release(ctx, i)
	}
}

func (r *Replica) tryCommit(ctx *core.Context, i int) {
	v := &r.vers[i]
	if v.done || !v.logged || v.commit || v.acked != 1<<v.n-1 && !r.plant.commitOnOwnLog {
		return
	}
	v.commit = true
	r.st.Commit(ctx, v.u.Chunk, v.req, &r.sout)
}

// fail ends a version that failed and every later one of its chunk, which may hold its bytes, so none of them commits: their clients are refused, with ref's table if it is newer, since peering decides whether they stay, and the PG's interval ends, as for a spent counter. The store keeps them pending until Settle.
func (r *Replica) fail(ctx *core.Context, i int, ref Response, out *ReplicaOutput) {
	v := &r.vers[i]
	if v.done || v.commit {
		return
	}
	c, from, pg := v.u.Chunk, v.u.Version, v.u.PG.PG
	out.note(trace.OutFail, c.Hash(), from, 0, 0, 0)
	if l := r.lead(pg); l >= 0 && r.leads[l].interval == v.interval {
		r.leads[l].asking = true
	}
	if own := r.refusal(pg); ref.Epoch <= own.Epoch {
		ref = own
	}
	ref.Kind, ref.Status, ref.Version, ref.Fill = KindReply, StatusRefused, 0, 0
	for j := len(r.vers) - 1; j >= 0; j-- {
		w := &r.vers[j]
		if w.u.Chunk != c || w.u.Version < from || w.u.Version == from && j < i || w.done || w.commit {
			continue
		}
		w.done = true
		if r.plant.serveRefused && w.ordered && !w.recover {
			r.st.Commit(ctx, w.u.Chunk, w.req, &r.sout)
		}
		if !w.recover {
			r.takeReq(w.req)
		}
		r.answerClient(ctx, w, ref, out)
		r.release(ctx, j)
	}
	r.dropWaiting(ctx, out)
}

// refuseWaiting refuses the clients of chunk c's ordered versions that have not committed; the store keeps them for peering to settle. A version answered or committed by every member it was sent to is left alone, and a recovery write has no client.
func (r *Replica) refuseWaiting(ctx *core.Context, c core.ChunkID, ref Response, out *ReplicaOutput) {
	for j := len(r.vers) - 1; j >= 0; j-- {
		v := &r.vers[j]
		if v.u.Chunk != c || !v.ordered || v.recover || v.done || v.commit {
			continue
		}
		v.done = true
		r.takeReq(v.req)
		r.answerClient(ctx, v, ref, out)
		r.release(ctx, j)
	}
}

// dropWaiting refuses the client writes and punches still waiting in the store, for a base read or behind one, whose PG's interval has ended since they were taken, and has the store drop them, so none is ordered in an interval that ended. One the store has already ordered or answered is left to that answer.
func (r *Replica) dropWaiting(ctx *core.Context, out *ReplicaOutput) {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		if v.done || v.ordered {
			continue
		}
		if l := r.lead(v.u.PG.PG); l >= 0 && (!r.leads[l].asking || r.plant.orderPastFailed) && r.leads[l].interval == v.interval || !r.st.Cancel(ctx, v.u.Chunk, v.req, &r.sout) {
			continue
		}
		v.done = true
		r.takeReq(v.req)
		for j := len(r.bases) - 1; j >= 0; j-- {
			if f := r.bases[j]; f.ID == v.req {
				r.bases = slices.Delete(r.bases, j, j+1)
				r.st.SupplyBase(ctx, f.ID, f.Block, core.NoBuf, ErrDropped, &r.sout)
			}
		}
		r.answerClient(ctx, v, r.refusal(v.u.PG.PG), out)
		r.release(ctx, i)
	}
}

// finished answers a request the store completed: a client's write or punch once its version is mapped or failed, a member's once durable, a read with its blocks.
func (r *Replica) finished(ctx *core.Context, rp Reply, out *ReplicaOutput) {
	if rp.ID&persistBit != 0 {
		r.persisted(ctx, rp, out)
		return
	}
	if rp.ID&dropBit != 0 {
		r.dropped(rp)
		return
	}
	q, ok := r.takeReq(rp.ID)
	if !ok {
		return
	}
	switch q.kind {
	case reqRecover:
		r.recoveryWritten(ctx, q, rp, out)
	case reqRecoverRead:
		if k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.read == rp.ID }); k >= 0 {
			r.recoveryRead(ctx, k, rp.Buf, rp.Err == nil, errors.Is(rp.Err, disklog.ErrBadChecksum), out)
		} else {
			freeBuf(ctx, rp.Buf)
		}
	case reqClient:
		i := r.versionOf(rp.ID)
		if i >= 0 && errors.Is(rp.Err, ErrCounter) {
			r.counterSpent(ctx, i, rp.Buf, out)
			return
		}
		resp := Response{Kind: KindReply, Status: statusOf(rp.Err), Version: rp.Version}
		if resp.Status != StatusOK {
			resp.Version = 0
		}
		if resp.Status == StatusRefused {
			ref := r.refusal(q.pg)
			resp.Epoch, resp.Interval = ref.Epoch, ref.Interval
		}
		if resp.Status == StatusOK {
			out.note(trace.OutCommitted, q.chunk.Hash(), rp.Version, 0, 0, 0)
		}
		if i >= 0 {
			r.vers[i].done = true
			r.release(ctx, i)
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
		r.answerDups(ctx, q.id, &resp, out)
	case reqMember:
		fill := r.fill(ctx)
		if q.recover {
			fill = r.freshFill(ctx)
		}
		resp := Response{Kind: KindAck, Status: statusOf(rp.Err), Version: rp.Version, Fill: fill}
		if resp.Status == StatusMismatch {
			resp.Status = StatusRefused
		}
		if resp.Status != StatusOK {
			ref := r.refusal(q.pg)
			resp.Version, resp.Epoch, resp.Interval = 0, ref.Epoch, ref.Interval
			var rec uint64
			if q.recover {
				rec = 1
			}
			out.note(trace.OutMemberFail, q.chunk.Hash(), q.version, 0, rec, r.unready())
			r.noteFailed(failedVer{pg: q.pg, chunk: q.chunk, version: q.version, unready: r.unready()})
		} else if q.recover {
			out.note(trace.OutRecovered, q.chunk.Hash(), rp.Version, uint64(q.source), uint64(q.interval), 0)
		} else {
			out.note(trace.OutLogged, q.chunk.Hash(), rp.Version, 0, 0, 0)
		}
		r.answer(ctx, q.peer, q.msgID, &resp, core.NoBuf, out)
		r.answerDups(ctx, q.id, &resp, out)
	case reqRead:
		resp := Response{Kind: KindReadReply, Version: rp.Version}
		switch {
		case errors.Is(rp.Err, ErrMissing):
			resp.Status, resp.Version = StatusMissing, 0
		case q.recover && errors.Is(rp.Err, disklog.ErrBadChecksum):
			resp.Status, resp.Version = StatusUnfound, 0
		case rp.Err != nil:
			ref := r.refusal(q.pg)
			resp.Status, resp.Version, resp.Epoch, resp.Interval = StatusRefused, 0, ref.Epoch, ref.Interval
		}
		r.answer(ctx, q.peer, q.msgID, &resp, rp.Buf, out)
	}
}

// release drops a finished version once no member's send of its buffer is outstanding.
func (r *Replica) release(ctx *core.Context, i int) {
	if v := &r.vers[i]; v.done && v.out == 0 {
		if v.ordered && !v.recover && !v.logged {
			r.unlogged = append(r.unlogged, [2]uint64{v.req, v.u.Chunk.Hash()})
		}
		freeBuf(ctx, v.buf)
		r.vers = slices.Delete(r.vers, i, i+1)
	}
}

// counterSpent holds a client write whose chunk spent its counter in the PG's interval, its buffer handed back, until the PG has a new interval: the PG's next writes are held behind it, and once its pending writes are done the primary asks for the interval. A write of a PG this disk no longer leads is refused.
func (r *Replica) counterSpent(ctx *core.Context, i int, buf core.IOBuf, out *ReplicaOutput) {
	v := r.vers[i]
	r.vers = slices.Delete(r.vers, i, i+1)
	ref := r.refusal(v.u.PG.PG)
	ref.Kind = KindReply
	r.answerDups(ctx, v.req, &ref, out)
	l := r.lead(v.u.PG.PG)
	if l < 0 {
		freeBuf(ctx, buf)
		r.reply(ctx, v.client, v.msgID, r.refusal(v.u.PG.PG), out)
		return
	}
	h := heldMsg{peer: v.client, hold: r.leads[l].interval}
	h.msg.Len, h.msg.Buf = uint16(copy(h.msg.Body[:], AppendUpdate(r.frame[:0], &v.u))), buf
	ld := &r.leads[l]
	ld.held = slices.Insert(ld.held, ld.spent, h)
	ld.spent++
	ld.asking = true
	r.dropWaiting(ctx, out)
}

// ask asks for the next interval of each PG whose interval ended, once none of its versions waits for a member or its own log: a committed version may wait behind a failed one, which only Settle resolves.
func (r *Replica) ask(out *ReplicaOutput) {
	for i := range r.leads {
		l := &r.leads[i]
		if !l.asking || l.asked || r.plant.neverAsk && r.cfg.Restarted || r.plant.neverAskOnFail || slices.ContainsFunc(r.vers, func(v version) bool { return v.u.PG.PG == l.pg && !v.done && !v.commit }) {
			continue
		}
		l.asked = true
		out.note(trace.OutAsk, pgField(l.pg), uint64(l.interval), 0, 0, 0)
		out.Asks = append(out.Asks, PGFields{Epoch: r.table.Epoch, PG: l.pg, Interval: l.interval})
	}
}

// peerLost ends every send to the peer and has the writes it left unacknowledged sent again after ResendAfter, new writes to it waiting behind them; a base asked of it is asked of the next member.
func (r *Replica) peerLost(ctx *core.Context, peer core.NodeID, out *ReplicaOutput) {
	r.recoverLost(ctx, peer, out)
	for i := len(r.bases) - 1; i >= 0; i-- {
		if r.bases[i].asked == peer {
			r.askBase(ctx, i, out)
		}
	}
	lost := false
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := &r.vers[i]
		for k := range v.n {
			if r.addr(v.members[k]) == peer {
				v.out &^= 1 << k
				lost = lost || v.acked&(1<<k) == 0 && !v.done
			}
		}
		r.release(ctx, i)
	}
	if lost && !slices.Contains(r.lost, peer) {
		r.lost = append(r.lost, peer)
	}
	if lost && !r.armed {
		r.armed = true
		out.Actions = append(out.Actions, core.Action{Kind: core.ActSetTimer, Timer: ResendTimer, At: ctx.Now + ResendAfter})
	}
}

// resend sends each member, in the order they were ordered, so a chunk's in version order, the writes of the PGs' current intervals it has neither acknowledged nor has a send of outstanding; a write of an interval that ended is left to the next interval's peering.
func (r *Replica) resend(out *ReplicaOutput) {
	r.armed, r.lost = false, r.lost[:0]
	for n := range r.vers {
		i := n
		if r.plant.resendNewestFirst {
			i = len(r.vers) - 1 - n
		}
		v := &r.vers[i]
		_, e := r.table.named(PGFields{Epoch: v.epoch, PG: v.u.PG.PG, Interval: v.interval})
		if v.done || !v.ordered || e == nil || v.interval != e.Interval {
			continue
		}
		for k := range v.n {
			if v.acked&(1<<k) == 0 && v.out&(1<<k) == 0 {
				r.send(v, k, out)
			}
		}
	}
}

// ErrActive reports a Settle of a chunk whose PG this disk leads and has activated in its current interval, where the chunk's next versions could take the numbers of those it drops.
var ErrActive = errors.New("chunkstore: settle of a chunk its primary has activated")

// Settle is peering's decision on chunk c, made before this disk activates as primary in a new interval: the chunk's pending versions on the chain of keep, the version peering kept, are mapped and their clients answered, and the others are dropped and their clients refused, as the store settles them. It settles nothing and returns ErrActive once the PG is active, or ErrUnresolved while one of the chunk's writes waits to be ordered or its records here are neither durable nor failed, which peering waits for, since a version whose local write then failed would be kept torn.
func (r *Replica) Settle(ctx *core.Context, c core.ChunkID, keep uint64, out *ReplicaOutput) error {
	if pg, ok := r.pgOf(c); ok {
		if l := r.lead(pg); l >= 0 && r.leads[l].active {
			return ErrActive
		}
	}
	if err := r.settle(ctx, c, keep); err != nil {
		return err
	}
	r.drain(ctx, out)
	return nil
}

// settle hands the store peering's decision on chunk c and marks the versions it keeps committed.
func (r *Replica) settle(ctx *core.Context, c core.ChunkID, keep uint64) error {
	top, err := r.st.Settle(ctx, c, keep, &r.sout)
	if err != nil {
		return err
	}
	for i := range r.vers {
		if v := &r.vers[i]; v.u.Chunk == c && v.ordered && v.u.Version <= top {
			v.commit = true
		}
	}
	return nil
}

// answerClient answers the client of version v with rp, a refusal; a recovery write has none, and
// its chunk is recovered again unless its interval ended.
func (r *Replica) answerClient(ctx *core.Context, v *version, rp Response, out *ReplicaOutput) {
	if v.recover {
		if k := slices.IndexFunc(r.recs, func(rc recovery) bool { return rc.req == v.req }); k >= 0 {
			r.endRecovery(k, false)
		}
		return
	}
	rp.Kind = KindReply
	r.answer(ctx, v.client, v.msgID, &rp, core.NoBuf, out)
	r.answerDups(ctx, v.req, &rp, out)
}

// answerDups answers each update sent again while store request orig was in flight as orig was answered.
func (r *Replica) answerDups(ctx *core.Context, orig uint64, rp *Response, out *ReplicaOutput) {
	for j := 0; j < len(r.dups); {
		if d := r.dups[j]; d.orig == orig {
			if rp.Kind == KindReply && rp.Status == StatusOK {
				out.note(trace.OutReanswer, d.chunk.Hash(), rp.Version, uint64(d.msgID), 0, 0)
			}
			r.answer(ctx, d.peer, d.msgID, rp, core.NoBuf, out)
			r.dups = slices.Delete(r.dups, j, j+1)
			continue
		}
		j++
	}
}

// reply answers a client's write or punch.
func (r *Replica) reply(ctx *core.Context, peer core.NodeID, id core.RequestID, rp Response, out *ReplicaOutput) {
	rp.Kind = KindReply
	r.answer(ctx, peer, id, &rp, core.NoBuf, out)
}

func (r *Replica) answer(ctx *core.Context, peer core.NodeID, id core.RequestID, rp *Response, buf core.IOBuf, out *ReplicaOutput) {
	rp.ID = id
	r.frame = AppendResponse(r.frame[:0], rp)
	a := core.Action{Kind: core.ActNetSend, Peer: peer, Reply: rp.Kind != KindAck}
	a.Msg.Len, a.Msg.Buf = uint16(copy(a.Msg.Body[:], r.frame)), buf
	out.Actions = append(out.Actions, a)
}
