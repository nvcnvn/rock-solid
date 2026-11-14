package meta

import (
	"encoding/binary"
	"flag"
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
