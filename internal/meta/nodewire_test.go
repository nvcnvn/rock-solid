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
			Opened: []OpenedDisk{{Device: 0xdead, Log: 1 << 63, Blocks: 1 << 30, Reserve: 64, Class: "nvme"}, {Device: 2, Log: 3}},
			Failed: many,
			Halts:  []HaltReport{{Group: 1, ID: 2, Index: 3, Age: 4}}},
	} {
		var m core.Msg
		if err := EncodeNodeReport(pool, 1, &nr, &m); err != nil {
			t.Fatal(err)
		}
		var got NodeReport
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
	pool := sim.NewPool(1<<20, 0)
	var m core.Msg
	seed(pool, &m)
	all := wireBytes(pool, &m)
	f.Add(all[:m.Len], all[m.Len:])
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

func FuzzDecodeNodeReport(f *testing.F) {
	fuzzNodeWire(f, func(pool core.BufPool, m *core.Msg) {
		nr := NodeReport{Node: 2, Release: 1, Meta: true, Sessions: 3, Opened: []OpenedDisk{{Device: 5, Log: 6, Blocks: 7, Class: "ssd"}},
			Failed: []uint64{9}, Halts: []HaltReport{{Group: 1, ID: 1, Index: 4, Age: 5}}}
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
