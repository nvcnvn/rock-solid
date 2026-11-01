package meta

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// A Txn that decodes is within the limits, encodes back to the same bytes and applies without a panic.
func FuzzDecodeTxn(f *testing.F) {
	for _, t := range []Txn{
		ops(Put(b("a"), b("1")), Delete(b("b")), DeleteRange(b("c"), b("d"))),
		ops(Check(b("a"), b("1"), true), CheckAbsent(b("b"), false), CheckEmpty(b("c"), b("d"), true), Update(b("e"), 8, Bump, 1<<40)),
		{Ops: []Op{Put(b("a"), nil)}, Replay: replayKey(1, 7), Oldest: 3},
	} {
		f.Add(appendTxn(nil, &t))
	}
	s := newTestState(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		var d Txn
		rest, err := decodeTxn(data, &d)
		if err != nil {
			return
		}
		if _, err := d.Check(); err != nil {
			t.Fatalf("decoded %+v fails Check: %v", d, err)
		}
		enc := appendTxn(nil, &d)
		if !bytes.Equal(enc, data[:len(data)-len(rest)]) {
			t.Fatalf("decoded %+v encodes to %x, not %x", d, enc, data[:len(data)-len(rest)])
		}
		var r Result
		_ = s.Apply(&d, &r)
	})
}

// Snapshot data CheckSnapshot accepts restores to the same keys, and data it refuses leaves the state as it was.
func FuzzSnapshot(f *testing.F) {
	src := newTestState(f)
	set(src, "a", "1", "b", "", "c", "xyz")
	f.Add(src.SnapshotData(0))
	f.Add([]byte{snapshotFormat, 1, 'a', 0, 1, 'a', 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		s := newTestState(t)
		set(s, "z", "old")
		err := s.Restore(9, data)
		if (err == nil) != CheckSnapshot(data) {
			t.Fatalf("Restore: %v, CheckSnapshot %v", err, CheckSnapshot(data))
		}
		if err != nil {
			if v, _ := get(s, "z"); v != "old" || s.Applied() != 0 {
				t.Fatal("a refused snapshot changed the state")
			}
			return
		}
		if got := s.SnapshotData(9); !bytes.Equal(got, data) {
			t.Fatalf("restored %x, which snapshots as %x", data, got)
		}
	})
}

// A replay record decodes without a panic, and one that decodes encodes back to the same bytes.
func FuzzDecodeRecord(f *testing.F) {
	f.Add(appendRecord(nil, &Result{FailedCheck: -1, Updated: []uint64{1, 2}}))
	f.Add(appendRecord(nil, &Result{FailedCheck: 3}))
	f.Fuzz(func(t *testing.T, data []byte) {
		var r Result
		if decodeRecord(data, &r) != nil {
			return
		}
		if got := appendRecord(nil, &r); !bytes.Equal(got, data) {
			t.Fatalf("record %x decodes to %+v, which encodes to %x", data, r, got)
		}
	})
}

// Any Txn that passes Check, its ops' fields drawn at random whatever their kind, decodes from its
// encoding to itself, so no Txn a leader takes makes an entry the replicas cannot apply.
func TestCheckedTxnsDecodeToThemselves(t *testing.T) {
	seeds.Run(t, 1, 2000, func(seed uint64) {
		rng := rand.New(rand.NewPCG(seed, 2))
		bs := func() []byte { return [][]byte{nil, {}, b("a"), b("b"), b("ab")}[rng.IntN(5)] }
		var tx Txn
		for range 1 + rng.IntN(3) {
			tx.Ops = append(tx.Ops, Op{Kind: OpKind(rng.IntN(8)), Key: bs(), End: bs(), Value: bs(), Absent: rng.IntN(2) == 0, Final: rng.IntN(2) == 0,
				Off: uint16(rng.IntN(2)), Mode: Mode(rng.IntN(5)), N: uint64(rng.IntN(2))})
		}
		if rng.IntN(2) == 0 {
			tx.Replay, tx.Oldest = replayKey(1, 5), uint64(rng.IntN(6))
		}
		if _, err := tx.Check(); err != nil {
			return
		}
		var d Txn
		if rest, err := decodeTxn(appendTxn(nil, &tx), &d); err != nil || len(rest) != 0 || !sameTxn(&tx, &d) {
			t.Fatalf("%+v passed Check but decodes to %+v (%v)", tx, d, err)
		}
	})
}

func sameTxn(a, b *Txn) bool {
	if !bytes.Equal(a.Replay, b.Replay) || a.Oldest != b.Oldest || len(a.Ops) != len(b.Ops) {
		return false
	}
	for i := range a.Ops {
		x, y := a.Ops[i], b.Ops[i]
		if !bytes.Equal(x.Key, y.Key) || !bytes.Equal(x.End, y.End) || !bytes.Equal(x.Value, y.Value) {
			return false
		}
		if x.Kind != y.Kind || x.Absent != y.Absent || x.Final != y.Final || x.Off != y.Off || x.Mode != y.Mode || x.N != y.N {
			return false
		}
	}
	return true
}
