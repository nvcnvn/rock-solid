package meta

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

func TestNodeMessagesCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	var many []uint64
	for i := range 100 {
		many = append(many, uint64(i)*1000+1)
	}
	for _, nr := range []NodeReport{
		{Node: 3},
		{Node: 1 << 31, Release: 2, Meta: true, MetaOnly: true, Epoch: 9, Sessions: 4,
			Opened: []OpenedDisk{{Device: 0xdead, Log: 1 << 63, Blocks: 1 << 30, Reserve: 64, Class: "nvme", Loop: 1 << 31}, {Device: 2, Log: 3}},
			Failed: many,
			Halts:  []HaltReport{{Group: 1, ID: 2, Index: 3, Age: 4}},
			Replicas: []ReplicaReport{{Group: 2, ID: 4, ConfIndex: 1 << 40, Joint: true, Voters: []uint64{1, 2, 4}},
				{Group: 1 << 63, ID: 3, Removed: true}, {Group: 2, ID: 5, Joint: true, Removed: true, Voters: many}}},
	} {
		var m core.Msg
		if err := EncodeNodeReport(pool, 1, &nr, &m); err != nil {
			t.Fatal(err)
		}
		got := NodeReport{Replicas: []ReplicaReport{{Voters: []uint64{9}}}}
		if g, k, _, _ := Peek(&m); g != 1 || k != KindNodeReport {
			t.Fatalf("report framed as group %d kind %#x", g, k)
		}
		if err := DecodeNodeReport(pool, &m, &got); err != nil || !bytes.Equal(appendNodeReport(nil, &got), appendNodeReport(nil, &nr)) {
			t.Fatalf("decoded %v: %+v, sent %+v", err, got, nr)
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	}
	for _, st := range []chunkstore.SessionTable{{}, {Version: 3, Next: 9, Live: []uint64{1, 5, 8}}, {Version: 1, Next: many[99] + 1, Live: many}} {
		var m core.Msg
		if err := EncodeSessions(pool, 1, 7, &st, &m); err != nil {
			t.Fatal(err)
		}
		got := chunkstore.SessionTable{Live: []uint64{42}}
		if _, _, tok, _ := Peek(&m); tok != 7 {
			t.Fatalf("token %d", tok)
		}
		if err := DecodeSessions(pool, &m, &got); err != nil || got.Version != st.Version || got.Next != st.Next || !slices.Equal(got.Live, st.Live) {
			t.Fatalf("decoded %v: %+v, sent %+v", err, got, st)
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers left", pool.Live())
	}
}

func TestASessionTableWhoseLiveIDsDoNotRiseBelowNextIsMalformed(t *testing.T) {
	pool := sim.NewPool(1<<16, 0)
	for _, live := range [][]uint64{{0}, {3, 3}, {4, 2}, {9}} {
		var m core.Msg
		st := chunkstore.SessionTable{Version: 1, Next: 9, Live: live}
		_ = EncodeSessions(pool, 1, 0, &st, &m)
		if err := DecodeSessions(pool, &m, &st); err != ErrMalformed {
			t.Errorf("live %v decodes: %v", live, err)
		}
	}
}

// Group 257's ID begins with the bytes 1, 1, which a chunkstore frame of version 1 and kind 1
// would also begin with, before the protocol byte.
func TestAGroupMessageIsNeverAChunkstoreFrame(t *testing.T) {
	pool := sim.NewPool(1<<16, 0)
	var msgs [3]core.Msg
	_ = EncodeTxn(pool, 257, 1, &Txn{Ops: []Op{Put(attr(1, 2, 1), b("v"))}}, &msgs[0])
	_ = EncodeNodeReport(pool, 257, &NodeReport{Node: 1}, &msgs[1])
	_ = EncodeSessions(pool, 257, 0, &chunkstore.SessionTable{Version: 1, Next: 1}, &msgs[2])
	for i := range msgs {
		frame, _ := chunkstore.Frame(pool, &msgs[i])
		if _, err := chunkstore.Peek(frame); err != chunkstore.ErrWireVersion || !msgs[i].Group() {
			t.Errorf("message %d of group 257 peeks as a chunkstore frame: %v, group %v", i, err, msgs[i].Group())
		}
	}
	var m core.Msg
	if err := chunkstore.Attach(pool, &m, chunkstore.AppendReport(nil, &chunkstore.Report{ID: 1}), core.NoBuf); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := Peek(&m); ok || m.Group() {
		t.Error("a chunkstore frame peeks as a group message")
	}
}

func fuzzNodeWire(f *testing.F, seed func(pool core.BufPool, m *core.Msg), decode func(pool core.BufPool, m *core.Msg) (core.Msg, bool)) {
	addWire(f, seed)
	f.Fuzz(func(t *testing.T, inline, body []byte) {
		pool := sim.NewPool(1<<20, 0)
		m, ok := wireMsg(pool, inline, body)
		if !ok {
			return
		}
		if again, ok := decode(pool, &m); ok {
			if out, in := wireBytes(pool, &again), wireBytes(pool, &m); !bytes.Equal(out, in) {
				t.Fatalf("decoded and encoded again:\n%x\nwas\n%x", out, in)
			}
			if again.Buf != core.NoBuf {
				pool.Free(again.Buf)
			}
		}
		if m.Buf != core.NoBuf {
			pool.Free(m.Buf)
		}
	})
}

// addWire adds the message seed encodes to f's corpus.
func addWire(f *testing.F, seed func(pool core.BufPool, m *core.Msg)) {
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	seed(pool, &m)
	all := wireBytes(pool, &m)
	f.Add(all[:m.Len], all[m.Len:])
}

func FuzzDecodeNodeReport(f *testing.F) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) {
		nr := NodeReport{Node: 2, Release: 1, Meta: true, Sessions: 3, Opened: []OpenedDisk{{Device: 5, Log: 6, Blocks: 7, Class: "ssd", Loop: 2}},
			Failed: []uint64{9}, Halts: []HaltReport{{Group: 1, ID: 1, Index: 4, Age: 5}},
			Replicas: []ReplicaReport{{Group: 2, ID: 3, ConfIndex: 7, Joint: true, Removed: true, Voters: []uint64{1, 2, 4}}}}
		_ = EncodeNodeReport(pool, 1, &nr, m)
	}, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var nr NodeReport
		var again core.Msg
		g, _, _, _ := Peek(m)
		if DecodeNodeReport(pool, m, &nr) != nil {
			return again, false
		}
		if err := EncodeNodeReport(pool, g, &nr, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeSessions(f *testing.F) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) {
		_ = EncodeSessions(pool, 1, 4, &chunkstore.SessionTable{Version: 2, Next: 9, Live: []uint64{3, 8}}, m)
	}, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var st chunkstore.SessionTable
		var again core.Msg
		g, _, tok, _ := Peek(m)
		if DecodeSessions(pool, m, &st) != nil {
			return again, false
		}
		if err := EncodeSessions(pool, g, tok, &st, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func TestPlacementAndLeaderMessagesCrossTheWireWhole(t *testing.T) {
	r, _ := poolRun(t)
	tbl := r.ctl.Table()
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	if err := EncodeTable(pool, 1, tbl, &m); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTable(pool, &m)
	if err != nil || !bytes.Equal(chunkstore.AppendTable(nil, &got), chunkstore.AppendTable(nil, tbl)) || len(tbl.Pools) == 0 {
		t.Fatalf("decoded %v: %+v, sent %+v", err, got, tbl)
	}
	free(&core.Context{Pool: pool}, &m)
	d := chunkstore.MakeDelta(&chunkstore.Table{}, tbl)
	if err := EncodeDelta(pool, 1, &d, &m); err != nil {
		t.Fatal(err)
	}
	gd, err := DecodeDelta(pool, &m)
	if err != nil || !bytes.Equal(chunkstore.AppendDelta(nil, &gd), chunkstore.AppendDelta(nil, &d)) {
		t.Fatalf("decoded %v: %+v, sent %+v", err, gd, d)
	}
	free(&core.Context{Pool: pool}, &m)
	for _, known := range []bool{false, true} {
		if err := EncodeLeader(pool, 1, core.NodeID{Node: 1 << 31, Loop: 7}, known, &m); err != nil {
			t.Fatal(err)
		}
		l, k, err := DecodeLeader(pool, &m)
		if err != nil || k != known || known && l != (core.NodeID{Node: 1 << 31, Loop: 7}) {
			t.Fatalf("decoded %v: %v %v", err, l, k)
		}
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers left", pool.Live())
	}
}

// fuzzRecode fuzzes a decoder of a message with no token through fuzzNodeWire: decode, then
// encode again into the same group.
func fuzzRecode[T any](f *testing.F, seed T, enc func(core.BufPool, uint64, *T, *core.Msg) error, dec func(core.BufPool, *core.Msg) (T, error)) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) { _ = enc(pool, 1, &seed, m) }, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var again core.Msg
		g, _, _, _ := Peek(m)
		v, err := dec(pool, m)
		if err != nil {
			return again, false
		}
		if err := enc(pool, g, &v, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

// seedTable is a table with a pool of two PGs, one with a past interval, so a fuzzer's seed reaches
// the body decoder.
func seedTable() chunkstore.Table {
	ms := []chunkstore.Member{{Disk: 1}, {Disk: 2, Leaving: true}}
	return chunkstore.Table{Epoch: 3, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 2, MinLive: 1, PGs: []chunkstore.PGEntry{
		{Members: ms, Interval: 3, Activated: 1, Past: []chunkstore.PastInterval{{Interval: 2, MinLive: 1, Members: ms[:1]}}},
		{Members: ms[:1], Interval: 1, Activated: 1, Pinned: true, Recovered: true},
	}}}}
}

func FuzzDecodeTableMsg(f *testing.F) {
	fuzzRecode(f, seedTable(), EncodeTable, DecodeTable)
}

func FuzzDecodeDeltaMsg(f *testing.F) {
	t := seedTable()
	fuzzRecode(f, chunkstore.MakeDelta(&chunkstore.Table{Epoch: 2}, &t), EncodeDelta, DecodeDelta)
}

type leaderMsg struct {
	at    core.NodeID
	known bool
}

func FuzzDecodeLeader(f *testing.F) {
	fuzzRecode(f, leaderMsg{core.NodeID{Node: 2, Loop: 3}, true},
		func(pool core.BufPool, g uint64, l *leaderMsg, m *core.Msg) error {
			return EncodeLeader(pool, g, l.at, l.known, m)
		},
		func(pool core.BufPool, m *core.Msg) (leaderMsg, error) {
			at, known, err := DecodeLeader(pool, m)
			return leaderMsg{at, known}, err
		})
}

func TestSessionStartAndDiskMessagesCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	if err := EncodeSessionStart(pool, 1, 77, &m); err != nil {
		t.Fatal(err)
	}
	if g, k, tok, _ := Peek(&m); g != 1 || k != KindSessionStart || tok != 77 {
		t.Fatalf("start framed as group %d kind %#x token %d", g, k, tok)
	}
	if err := EncodeStarted(pool, 1, 77, 1<<40, &m); err != nil {
		t.Fatal(err)
	}
	if id, err := DecodeStarted(pool, &m); err != nil || id != 1<<40 {
		t.Fatalf("started decoded %v: %d", err, id)
	}
	var ds []DiskAddr
	for i := range 300 {
		ds = append(ds, DiskAddr{Disk: uint32(i)*7 + 1, Log: uint64(i) << 40, At: core.NodeID{Node: uint32(i), Loop: 1 << 31}})
	}
	for _, sent := range [][]DiskAddr{nil, ds[:1], ds} {
		if err := EncodeDisks(pool, 1, sent, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeDisks(pool, &m, []DiskAddr{{Disk: 9}})
		if err != nil || len(got) != len(sent) || len(sent) > 0 && !slices.Equal(got, sent) {
			t.Fatalf("decoded %v: %d disks, sent %d", err, len(got), len(sent))
		}
		free(&core.Context{Pool: pool}, &m)
	}
	for _, bad := range [][]DiskAddr{{{Disk: 0}}, {{Disk: 3}, {Disk: 3}}, {{Disk: 4}, {Disk: 2}}} {
		_ = EncodeDisks(pool, 1, bad, &m)
		if _, err := DecodeDisks(pool, &m, nil); err != ErrMalformed {
			t.Errorf("disks %v decode: %v", bad, err)
		}
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers left", pool.Live())
	}
}

func FuzzDecodeStarted(f *testing.F) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) { _ = EncodeStarted(pool, 1, 4, 9, m) }, func(pool core.BufPool, m *core.Msg) (core.Msg, bool) {
		var again core.Msg
		g, _, tok, _ := Peek(m)
		id, err := DecodeStarted(pool, m)
		if err != nil {
			return again, false
		}
		if err := EncodeStarted(pool, g, tok, id, &again); err != nil {
			panic(err)
		}
		return again, true
	})
}

func FuzzDecodeDisks(f *testing.F) {
	fuzzRecode(f, []DiskAddr{{Disk: 1, Log: 5, At: core.NodeID{Node: 2, Loop: 1}}, {Disk: 4, Log: 6, At: core.NodeID{Node: 3}}},
		func(pool core.BufPool, g uint64, ds *[]DiskAddr, m *core.Msg) error {
			return EncodeDisks(pool, g, *ds, m)
		},
		func(pool core.BufPool, m *core.Msg) ([]DiskAddr, error) { return DecodeDisks(pool, m, nil) })
}

func TestAsksCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	var fs []chunkstore.PGFields
	for i := range 200 {
		fs = append(fs, chunkstore.PGFields{Epoch: uint64(i) << 40, PG: core.PGID{Pool: core.PoolID(i), Index: 1 << 31}, Interval: uint32(i) + 1})
	}
	for _, sent := range [][]chunkstore.PGFields{nil, fs[:1], fs} {
		if err := EncodeAsks(pool, 1, sent, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAsks(pool, &m, fs[:1:1])
		if err != nil || !slices.Equal(got, sent) && len(sent) > 0 || len(got) != len(sent) {
			t.Fatalf("decoded %v: %d asks, sent %d", err, len(got), len(sent))
		}
		free(&core.Context{Pool: pool}, &m)
	}
	if err := encodeBody(pool, 1, KindAsks, 0, []byte{1, 0, 0x80, 0x80, 0x80, 0x80, 0x10, 0, 1}, &m); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAsks(pool, &m, nil); err != ErrMalformed {
		t.Fatalf("a pool over 32 bits decodes: %v", err)
	}
	if pool.Live() != 0 {
		t.Fatalf("%d buffers left", pool.Live())
	}
}

func FuzzDecodeAsks(f *testing.F) {
	fuzzRecode(f, []chunkstore.PGFields{{Epoch: 3, PG: core.PGID{Pool: 1, Index: 2}, Interval: 4}, {Epoch: 3, PG: core.PGID{Pool: 1}, Interval: 9}},
		func(pool core.BufPool, g uint64, fs *[]chunkstore.PGFields, m *core.Msg) error {
			return EncodeAsks(pool, g, *fs, m)
		},
		func(pool core.BufPool, m *core.Msg) ([]chunkstore.PGFields, error) { return DecodeAsks(pool, m, nil) })
}

func TestDeletesCrossTheWireWhole(t *testing.T) {
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	var rs []ReplicaRef
	for i := range 200 {
		rs = append(rs, ReplicaRef{Group: uint64(i) << 50, ID: uint64(i) + 1})
	}
	for _, sent := range [][]ReplicaRef{nil, rs[:1], rs} {
		if err := EncodeDelete(pool, 1, sent, &m); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeDelete(pool, &m, rs[:1:1])
		if err != nil || len(got) != len(sent) || len(sent) > 0 && !slices.Equal(got, sent) {
			t.Fatalf("decoded %v: %v, sent %v", err, got, sent)
		}
		free(&core.Context{Pool: pool}, &m)
	}
	if err := encodeBody(pool, 1, KindDelete, 0, []byte{2, 1, 1}, &m); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeDelete(pool, &m, nil); err != ErrMalformed {
		t.Fatalf("a delete one replica short decodes: %v", err)
	}
	free(&core.Context{Pool: pool}, &m)
	if pool.Live() != 0 {
		t.Fatalf("%d buffers left", pool.Live())
	}
}

func FuzzDecodeDelete(f *testing.F) {
	fuzzRecode(f, []ReplicaRef{{Group: 3, ID: 4}, {Group: 1 << 40, ID: 9}},
		func(pool core.BufPool, g uint64, rs *[]ReplicaRef, m *core.Msg) error {
			return EncodeDelete(pool, g, *rs, m)
		},
		func(pool core.BufPool, m *core.Msg) ([]ReplicaRef, error) { return DecodeDelete(pool, m, nil) })
}
