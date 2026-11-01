package meta

import (
	"bytes"
	"testing"
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
