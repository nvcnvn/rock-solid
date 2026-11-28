package looptest

import (
	"encoding/binary"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/meta"
	"github.com/nvcnvn/rock-solid/internal/node"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// metaTrio runs three storage nodes' device loops over simulated disks, as their processes do,
// each node.Device hosting the voter of trioGroup its disk's plan places, the three booting the
// group together; the loops' messages cross to one another, and the devices' answers to a client
// are kept. A follower whose entries the trio corrupts gets each entry its leader appends to it
// with a format byte no release decodes.
type metaTrio struct {
	t       *testing.T
	s       *sim.Scheduler
	ds      [3]*trioDevice
	reads   []meta.ReadAnswer
	answers map[uint64]error
	corrupt uint32
	// beatAt is when the devices next take their membership, 0 before the first.
	beatAt core.Tick
}

const trioGroup = 20

var trioClient = core.NodeID{Node: 90}

type trioDevice struct {
	deviceOnLoop
	tr       *metaTrio
	node     uint32
	l        *loop.Loop
	taking   node.Output
	reported node.DeviceOutput
}

func (m *trioDevice) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	if ev.Kind == core.EvTimer && ev.Timer == nodeMetaTake {
		m.out.Reset()
		m.d.Take(ctx, &m.taking, &m.out)
		m.reported, _ = m.out.Clone()
		return m.done(out)
	}
	return m.deviceOnLoop.OnEvent(ctx, ev, out)
}

func (m *trioDevice) OnReply(_ *loop.Log, r loop.Reply) { m.d.OnReply(r) }

// trioDriver hands a device's disk actions to its disk and its messages to the loop they name.
type trioDriver struct {
	diskDriver
	m *trioDevice
}

func (d trioDriver) Execute(a *core.Action) {
	if a.Kind != core.ActNetSend {
		d.diskDriver.Execute(a)
		return
	}
	tr, from := d.m.tr, d.m.l.Context().Pool
	if a.Peer == trioClient {
		var ra meta.ReadAnswer
		if meta.DecodeReadAnswer(from, &a.Msg, &ra) == nil {
			tr.reads = append(tr.reads, ra)
		}
		var an meta.Answer
		if meta.DecodeAnswer(from, &a.Msg, &an) == nil {
			tr.answers[an.Token] = an.Err
		}
	}
	i := slices.IndexFunc(tr.ds[:], func(m *trioDevice) bool { return a.Peer == core.NodeID{Node: m.node, Loop: 1} })
	if i < 0 {
		if a.Msg.Buf != core.NoBuf {
			from.Free(a.Msg.Buf)
		}
		return
	}
	if a.Peer.Node == tr.corrupt {
		tr.corruptEntries(&a.Msg, from)
	}
	dst := tr.ds[i].l
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: d.m.node, Loop: 1}, Msg: a.Msg}
	if b := a.Msg.Buf; b != core.NoBuf {
		to := dst.Context().Pool
		rb, err := to.Alloc(len(from.Bytes(b)))
		if err != nil {
			tr.t.Fatal(err)
		}
		copy(to.Bytes(rb), from.Bytes(b))
		from.Free(b)
		ev.Msg.Buf = rb
	}
	dst.Inbox().Post(&ev)
}

// corruptEntries sets the format byte of each entry with data a Raft append of trioGroup in msg
// carries to one past any release's.
func (tr *metaTrio) corruptEntries(msg *core.Msg, pool core.BufPool) {
	in := msg.Inline()
	if len(in) < 10 || in[0] != core.ProtoGroup || binary.LittleEndian.Uint64(in[1:]) != trioGroup || in[9] != 0 {
		return
	}
	body := in[10:]
	if msg.Buf != core.NoBuf {
		body = pool.Bytes(msg.Buf)[:binary.LittleEndian.Uint32(body)]
	}
	var m raftpb.Message
	if m.Unmarshal(body) != nil || m.Type != raftpb.MsgApp {
		return
	}
	for i := range m.Entries {
		if e := &m.Entries[i]; e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
			e.Data[0] = 0xff
		}
	}
	if _, err := m.MarshalTo(body); err != nil {
		tr.t.Fatal(err)
	}
}

// newMetaTrio starts the three devices on new disks and hands each the membership naming the three
// disks and the plan placing trioGroup's voter i+1 on disk i+1.
func newMetaTrio(t *testing.T) *metaTrio {
	tr := &metaTrio{t: t, s: sim.New(1, sim.Settings{PoolBytes: 1 << 20}), answers: map[uint64]error{}}
	for i := range tr.ds {
		sd := storetest.NewSimDiskOf(uint64(i)+1, storetest.DiskBytes)
		n := tr.s.AddNode(sim.DefaultClock, nil, func(*sim.Node) {})
		k := NewKernel(tr.s, n.ID())
		dr, err := NewDisk(k, sd.D, sd.Pool())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(dr.Close)
		m := &trioDevice{tr: tr, node: n.ID(), deviceOnLoop: deviceOnLoop{cfg: node.DeviceConfig{Device: 7, Blocks: sd.Blocks(), Loop: 1, Store: shardReplay(), Seed: uint64(i) + 1, Meta: metaTemplate()}}}
		m.l = loop.New(loop.Config{Node: n.ID(), ID: 1, Pool: sd.Pool()}, k, m, trioDriver{diskDriver{dr}, m})
		if err := m.l.AttachDisk(dr); err != nil {
			t.Fatal(err)
		}
		m.lg = m.l.AttachLog(nil, m.cfg.Store.Log, chunkstore.FirstLogIO, chunkstore.EndLogIO, 2)
		m.cfg.Host, m.cfg.Rounds = m.l, m.lg
		AddWithDisk(n, k, m.l, dr)
		m.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
		tr.ds[i] = m
	}
	tr.until(func() bool { return tr.ds[0].opened+tr.ds[1].opened+tr.ds[2].opened == 3 })
	g := meta.Group{ID: trioGroup, LastID: 3, Boot: 3}
	var ds []meta.DiskAddr
	for i, m := range tr.ds {
		g.Voters = append(g.Voters, meta.Replica{ID: uint64(i) + 1, Disk: uint32(i) + 1})
		ds = append(ds, meta.DiskAddr{Disk: uint32(i) + 1, Log: m.report.Log, At: core.NodeID{Node: m.node, Loop: 1}})
	}
	for _, m := range tr.ds {
		m.taking = node.Output{Disks: ds, Plans: []meta.Group{g}, Member: true}
	}
	tr.take()
	return tr
}

// take has every device take its membership now and every trioBeat from then on, as a node hands
// its devices each answer to its heartbeat.
func (tr *metaTrio) take() {
	tr.beatAt = tr.s.Time()
	tr.run(10_000)
}

const trioBeat core.Tick = 1_000_000

// step runs the trio's next step, first handing the devices their membership once it is due.
func (tr *metaTrio) step() bool {
	if tr.beatAt != 0 && tr.s.Time() >= tr.beatAt {
		tr.beatAt += trioBeat
		for _, m := range tr.ds {
			m.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: nodeMetaTake})
		}
	}
	return tr.s.Step()
}

// until runs the trio until done holds, failing it past a minute of its time.
func (tr *metaTrio) until(done func() bool) {
	tr.t.Helper()
	end := tr.s.Time() + 60_000_000
	for !done() {
		if tr.s.Time() > end || !tr.step() {
			tr.t.Fatal("the trio never got there")
		}
	}
}

// run runs the trio for d of its time.
func (tr *metaTrio) run(d core.Tick) {
	end := tr.s.Time() + d
	for tr.s.Time() < end && tr.step() {
	}
}

// shard is device i's replica of trioGroup, nil before it starts.
func (tr *metaTrio) shard(i int) *meta.Shard {
	if a := tr.ds[i].d.Meta(trioGroup); a != nil {
		return a.Shard()
	}
	return nil
}

// leader is the device whose replica leads trioGroup, once one does.
func (tr *metaTrio) leader() int {
	var l int
	tr.until(func() bool {
		l = slices.IndexFunc(tr.ds[:], func(m *trioDevice) bool {
			a := m.d.Meta(trioGroup)
			return a != nil && a.Shard() != nil && a.Shard().Raft().Leader() == a.ReplicaID()
		})
		return l >= 0
	})
	return l
}

// post sends device i what the client encodes, from the client.
func (tr *metaTrio) post(i int, encode func(pool core.BufPool, msg *core.Msg) error) {
	ev := core.Event{Kind: core.EvNetRecv, Peer: trioClient}
	if err := encode(tr.ds[i].l.Context().Pool, &ev.Msg); err != nil {
		tr.t.Fatal(err)
	}
	tr.ds[i].l.Inbox().Post(&ev)
}

// A group whose three voters are on three storage nodes' disks serves reads: each device's
// replicas share their loop's leases with its chunk replica, which renews them with every
// loop hosting a peer of theirs, so the leader is fortified and holds its leader lease.
func TestAGroupOfThreeVotersOnStorageNodesServesReads(t *testing.T) {
	tr := newMetaTrio(t)
	l := tr.leader()
	tr.run(10_000_000)
	for i := range 5 {
		tr.post(l, func(pool core.BufPool, msg *core.Msg) error {
			return meta.EncodeRead(pool, trioGroup, uint64(i)+1, &meta.Read{Start: []byte{1}, End: []byte{2}, Limit: 1}, msg)
		})
		tr.run(1_000_000)
	}
	if len(tr.reads) != 5 || slices.ContainsFunc(tr.reads, func(ra meta.ReadAnswer) bool { return ra.Err != nil }) {
		t.Fatalf("the leader answered the reads %+v", tr.reads)
	}
}

// A voter on a storage node halted at a committed entry it cannot decode is in its device's
// report, with the entry's index and since when, the same in the next report.
func TestADevicesReportNamesItsHaltedReplica(t *testing.T) {
	tr := newMetaTrio(t)
	l := tr.leader()
	f := (l + 1) % 3
	tr.corrupt = tr.ds[f].node
	tr.post(l, func(pool core.BufPool, msg *core.Msg) error {
		return meta.EncodeTxn(pool, trioGroup, 1, &meta.Txn{Ops: []meta.Op{meta.Put([]byte("\x01k"), []byte("v"))}}, msg)
	})
	tr.until(func() bool { s := tr.shard(f); return s != nil && s.Halted() != 0 })
	at := tr.shard(f).Halted()
	tr.run(1_000_000)
	if err, ok := tr.answers[1]; !ok || err != nil {
		t.Fatalf("the leader answered the Txn %v, %v", err, ok)
	}
	tr.take()
	want := meta.Halt{Group: trioGroup, ID: uint64(f) + 1, Index: at}
	hs := tr.ds[f].reported.Halts
	if len(hs) != 1 || hs[0].Group != want.Group || hs[0].ID != want.ID || hs[0].Index != want.Index {
		t.Fatalf("the device reported halts %+v, want %+v", hs, want)
	}
	first := hs[0].Since
	tr.run(1_000_000)
	tr.take()
	if hs := tr.ds[f].reported.Halts; len(hs) != 1 || hs[0].Since != first {
		t.Fatalf("the next report has halts %+v, the halt seen since %d", hs, first)
	}
	if hs := tr.ds[l].reported.Halts; len(hs) != 0 {
		t.Fatalf("the leader's device reported halts %+v", hs)
	}
}
