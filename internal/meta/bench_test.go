package meta

import (
	"fmt"
	"encoding/binary"
	"flag"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// BenchmarkRouteCreate routes a create through the shard map and the route cache to its leader
// and takes its answer.
func BenchmarkRouteCreate(b *testing.B) {
	m := oneShard(0)
	r, err := NewRouter(&m, 1)
	if err != nil {
		b.Fatal(err)
	}
	var ctx core.Context
	var out RouterOutput
	t := Txn{Ops: []Op{CheckAbsent(attr(1, 2, 3), true), Put(attr(1, 2, 3), be(1)), Update(attr(1, 2, 1), 0, Bump, 1)}, Replay: replayKey(1, 1)}
	a := Answer{Result: Result{FailedCheck: -1}}
	if err := r.Txn(&ctx, &t, 0, &out); err != nil {
		b.Fatal(err)
	}
	r.Answer(&ctx, &Answer{Err: NotLeader{Group: dataGroup, Leader: 2, Node: 1, Term: 2}}, &out)
	r.Answer(&ctx, &a, &out)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		out.Reset()
		a.Token = uint64(i) + 1
		if err := r.Txn(&ctx, &t, a.Token, &out); err != nil || out.Sends[0].Node != 1 {
			b.Fatal(err)
		}
		r.Answer(&ctx, &a, &out)
	}
}

var (
	benchFiles = flag.Int("meta.files", 1_000_000, "files in the shard BenchmarkPointRead reads from")
	checkTree  = flag.Bool("meta.check", true, "run the tests' trees with lsm's checks, which a benchmark turns off to measure")
)

// BenchmarkApplyCreate decodes and applies a create, as every replica applies it from its log
// entry: a check, two puts and the parent's two times by Bump.
func BenchmarkApplyCreate(b *testing.B) {
	s := newTestState(b)
	entries := make([][]byte, 1<<16)
	for i := range entries {
		t := create("dir", i, uint64(i), 7)
		entries[i] = appendTxn(nil, &t)
	}
	var d Txn
	var r Result
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if i%len(entries) == 0 && i > 0 {
			b.StopTimer()
			s = newTestState(b)
			b.StartTimer()
		}
		if _, err := decodeTxn(entries[i%len(entries)], &d); err != nil {
			b.Fatal(err)
		}
		_ = s.Apply(&d, &r)
	}
}

// BenchmarkScanListing lists a 1,000-entry directory in one Scan.
func BenchmarkScanListing(b *testing.B) {
	s := newTestState(b)
	for i := range 1000 {
		must(s.tree.Put(binary.BigEndian.AppendUint32([]byte("dir/d/"), uint32(i)), be(uint64(i))))
	}
	start := []byte("dir/d/")
	end := PrefixEnd(start)
	var r ScanResult
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := s.Scan(start, end, 1000, &r); err != nil || len(r.KVs) != 1000 {
			b.Fatal(err, len(r.KVs))
		}
	}
}

// BenchmarkPointRead reads one file's attributes, Scan(k, k + 0x00, 1), from a shard of -meta.files files.
func BenchmarkPointRead(b *testing.B) {
	s := newTestState(b)
	key := func(i uint32) []byte {
		k := binary.BigEndian.AppendUint64([]byte{3, 0, 0, 0, 5}, uint64(2)<<40|uint64(i))
		return append(k, 1)
	}
	attrs := make([]byte, 80)
	for i := range uint32(*benchFiles) {
		must(s.tree.Put(key(i), attrs))
	}
	keys := make([][]byte, 1<<16)
	for i := range keys {
		keys[i] = append(key(uint32(i*7919)%uint32(*benchFiles)), 0)
	}
	var r ScanResult
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		end := keys[i%len(keys)]
		if err := s.Scan(end[:len(end)-1], end, 1, &r); err != nil || len(r.KVs) != 1 {
			b.Fatal(err, len(r.KVs))
		}
	}
}

// BenchmarkServeRead serves a point read of one file's attributes over the wire at a leader under
// its lease: decode, lease check, Scan and answer.
func BenchmarkServeRead(b *testing.B) {
	c := newGroup(b, 3, false)
	c.renewLeases()
	l := c.elect()
	c.heartbeat()
	k := attr(1, 2, 1)
	c.txn(l, Txn{Ops: []Op{Put(k, make([]byte, 80))}}, 1)
	c.settle()
	ctx := c.ctx(l)
	sv := NewServer()
	ev := core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3}}
	if err := EncodeRead(ctx.Pool, dataGroup, 9, &Read{Start: k, End: append(slices.Clone(k), 0), Limit: 1}, &ev.Msg); err != nil {
		b.Fatal(err)
	}
	acts := make([]core.Action, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		acts = sv.ServeRead(&ctx, &ev, c.n[l].s, acts[:0])
		if len(acts) != 1 || acts[0].Msg.Buf != core.NoBuf || sv.ra.Err != nil || len(sv.ra.KVs) != 1 {
			b.Fatalf("answered %+v", sv.ra)
		}
	}
}

func BenchmarkReviewServeOpenRelease(b *testing.B) {
	for _, held := range []int{0, 100_000} {
		b.Run(fmt.Sprint(held), func(b *testing.B) {
			c := newGroup(b, 3, false)
			c.renewLeases()
			l := c.elect()
			c.heartbeat()
			a := Attr{Mode: ModeFile | 0o644, Nlink: 1}
			k := mustKey(&Key{Space: SpaceFS, Volume: 1, Inode: 5, Tag: InodeAttr})
			c.txn(l, Txn{Ops: []Op{Put(k, AppendAttr(nil, &a))}}, 1)
			c.settle()
			ctx := c.ctx(l)
			sv := NewServer()
			for i := range held {
				sv.reg.rows = append(sv.reg.rows, openRow{at: inodeAt{dataGroup, 1, uint64(i)}, who: opener{1, 1}})
			}
			var evs [2]core.Event
			for i, reg := range []byte{RegOpen, RegRelease} {
				evs[i] = core.Event{Kind: core.EvNetRecv, Peer: core.NodeID{Node: 3}}
				if err := EncodeRead(ctx.Pool, dataGroup, 9, &Read{Start: k, End: append(slices.Clone(k), 0), Limit: 1, Reg: reg, Session: 2, Mount: 1}, &evs[i].Msg); err != nil {
					b.Fatal(err)
				}
			}
			acts := make([]core.Action, 0, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				acts = sv.ServeRead(&ctx, &evs[i&1], c.n[l].s, acts[:0])
				if len(acts) != 1 || sv.ra.Err != nil || len(sv.ra.KVs) != 1 {
					b.Fatalf("answered %+v", sv.ra)
				}
			}
		})
	}
}
