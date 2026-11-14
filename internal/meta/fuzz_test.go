package meta

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
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

// Bytes that decode as a key encode back to the same bytes, and anything else is refused without a panic.
func FuzzDecodeKey(f *testing.F) {
	for _, k := range goldenKeys {
		f.Add(mustKey(&k))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var k Key
		if DecodeKey(data, &k) != nil {
			return
		}
		if enc, err := AppendKey(nil, &k); err != nil || !bytes.Equal(enc, data) {
			t.Fatalf("%x decodes to %+v, which encodes to %x (%v)", data, k, enc, err)
		}
	})
}

// A head and pieces that join are the split of the value they join to, and a value splits and
// joins back to itself.
func FuzzPieces(f *testing.F) {
	f.Add([]byte{0}, []byte{}, uint16(0))
	f.Add(append([]byte{2}, make([]byte, MaxValue-1)...), make([]byte, MaxValue), uint16(7))
	f.Add(append([]byte{maxPieces}, make([]byte, MaxValue-1)...), make([]byte, MaxValue), uint16(MaxValue-1))
	f.Fuzz(func(t *testing.T, head, piece []byte, last uint16) {
		vals := [][]byte{head}
		if m, _, err := DecodeHead(head); err == nil && m > 0 {
			for i := 1; i <= m; i++ {
				vals = append(vals, piece)
			}
			vals[m] = piece[:min(len(piece), int(last)+1)]
		}
		if v, err := join(vals); err == nil {
			for i, p := range split(t, v) {
				if !bytes.Equal(p, vals[i]) {
					t.Fatalf("value %d of a %d-byte split is %x, joined from %x", i, len(v), p, vals[i])
				}
			}
		}
		if (len(head)+len(piece))*(1+int(last)) > MaxSplit {
			return
		}
		v := bytes.Repeat(append(head, piece...), 1+int(last))
		if got, err := join(split(t, v)); err != nil || !bytes.Equal(got, v) {
			t.Fatalf("a %d-byte value joins to %d bytes (%v)", len(v), len(got), err)
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

// A pool's settings decode without a panic, and those that decode pass the pool rules and encode
// back to the same bytes.
func FuzzDecodePool(f *testing.F) {
	for _, s := range []chunkstore.PoolSpec{spec(3, 8), spec(1, 1024), {DiskClass: "ssd", FailureDomain: chunkstore.DomainDisk}} {
		p, err := chunkstore.NewPool(s, 12)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(appendPool(nil, &p))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := decodePool(data)
		if err != nil {
			return
		}
		if p.Check() != nil || !bytes.Equal(appendPool(nil, &p), data) {
			t.Fatalf("%x decodes to %+v, which fails the rules or encodes to %x", data, p, appendPool(nil, &p))
		}
	})
}

// A shard-map group decodes without a panic, and one that decodes has distinct voter IDs no
// higher than its last, and encodes back to the same bytes.
func FuzzDecodeGroup(f *testing.F) {
	f.Add(uint64(1), appendGroup(nil, &Group{ID: 1}))
	f.Add(uint64(2), appendGroup(nil, &Group{ID: 2, Start: b("m"), LastID: 4, Voters: []Replica{{2, 0, 1}, {3, 1, 7}, {4, 2, 300}}}))
	f.Fuzz(func(t *testing.T, id uint64, data []byte) {
		g, err := decodeGroup(id, data)
		if err != nil {
			return
		}
		seen := map[uint64]bool{}
		for _, v := range g.Voters {
			if v.ID == 0 || v.ID > g.LastID || seen[v.ID] {
				t.Fatalf("%x decodes to %+v, a voter ID out of place", data, g)
			}
			seen[v.ID] = true
		}
		if g.ID != id || len(g.Start) > MaxKey || !bytes.Equal(appendGroup(nil, &g), data) {
			t.Fatalf("%x decodes to %+v, which encodes to %x", data, g, appendGroup(nil, &g))
		}
	})
}

// fuzzCtlKVs reads data as ctl/ keys and values, each a key length mod 16, a value length, the
// key after its ctl/ byte and the value, and returns them in key order, each key once.
func fuzzCtlKVs(data []byte) []KV {
	var kvs []KV
	for len(data) >= 2 {
		kl, vl := int(data[0])%16, int(data[1])
		data = data[2:]
		if kl+vl > len(data) {
			break
		}
		kvs = append(kvs, KV{append([]byte{SpaceCtl}, data[:kl]...), data[kl : kl+vl]})
		data = data[kl+vl:]
	}
	slices.SortStableFunc(kvs, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
	return slices.CompactFunc(kvs, func(a, b KV) bool { return bytes.Equal(a.Key, b.Key) })
}

// The control plane's ctl/ keys decode without a panic, and a state that decodes holds a table
// whose CRC32C is its head's, a shard map whose ranges CheckRanges accepts, disks only under a
// record version, and sessions only under the session table's head, below its next ID and fenced
// at most at its version, which its acknowledged version does not pass.
func FuzzDecodeCtl(f *testing.F) {
	var seed []byte
	add := func(k, v []byte) { seed = append(append(append(seed, byte(len(k)-1), byte(len(v))), k[1:]...), v...) }
	p, err := chunkstore.NewPool(spec(3, 2), 4)
	if err != nil {
		f.Fatal(err)
	}
	m := func(d ...uint32) []chunkstore.Member {
		var ms []chunkstore.Member
		for _, x := range d {
			ms = append(ms, chunkstore.Member{Disk: x})
		}
		return ms
	}
	tb := chunkstore.Table{Epoch: 1, Pools: []chunkstore.PoolTable{{ID: 1, Replicas: 3, MinLive: 2, PGs: []chunkstore.PGEntry{{Interval: 1, Members: m(1, 2, 3)}, {Interval: 1, Members: m(2, 3, 4)}}}}}
	add(poolKey(1), appendPool(nil, &p))
	add(ctlKey(CtlPlacement, nil), tableHead(1, tb.CRC()))
	add(placementKey(1), poolHead(&tb.Pools[0]))
	for i := range tb.Pools[0].PGs {
		h, _ := AppendHead(nil, chunkstore.AppendEntry(nil, &tb.Pools[0].PGs[i]))
		add(entryKey(core.PGID{Pool: 1, Index: uint32(i)}), h)
	}
	add(ctlKey(CtlMembership, nil), bigEndian.AppendUint64(nil, 3))
	for _, e := range []DiskEntry{
		{RecordedDisk: chunkstore.RecordedDisk{ID: 1, Log: 0x11, Device: 0xd1, Up: true}},
		{RecordedDisk: chunkstore.RecordedDisk{ID: 2, Log: 0x12, Device: 0xd2, Node: 1, Gone: true}},
		{RecordedDisk: chunkstore.RecordedDisk{ID: 3, Log: 0x13, Device: 0xd2, Node: 1}, Age: AgeStep, Drain: true},
	} {
		add(diskKey(e.ID), appendDisk(nil, &e))
	}
	add(ctlKey(CtlFormat, nil), binary.AppendUvarint(nil, 2))
	add(sessionKey(0), appendSessionHead(nil, &SessionTable{Next: 3, Version: 1}))
	for _, e := range []SessionEntry{{ID: 1, Node: 4, Incarnation: 1, Fenced: 1}, {ID: 2, Node: 300, Incarnation: 1}} {
		add(sessionKey(e.ID), appendSession(nil, &e))
	}
	add(ctlKey(CtlShardMap, nil), bigEndian.AppendUint64(nil, 1))
	for _, g := range []Group{{ID: 1, LastID: 3}, {ID: 2, Start: PrefixEnd([]byte{SpaceCtl}), LastID: 3}} {
		g.Voters = []Replica{{1, 0, 1}, {2, 1, 2}, {3, 2, 3}}
		h, _ := AppendHead(nil, appendGroup(nil, &g))
		add(groupKey(g.ID), h)
	}
	if _, err := decodeCtl(fuzzCtlKVs(seed)); err != nil {
		f.Fatalf("the seed state: %v", err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := decodeCtl(fuzzCtlKVs(data))
		if err != nil {
			return
		}
		if s.table.Epoch != 0 && s.table.CRC() != s.crc || len(s.smap.Groups) > 0 && s.smap.CheckRanges() != nil || len(s.disks) > 0 && s.diskV == 0 {
			t.Fatalf("decoded %+v", s)
		}
		ss := &s.sess
		if ss.Acked > ss.Version || slices.ContainsFunc(ss.Sessions, func(e SessionEntry) bool { return e.ID == 0 || e.ID >= ss.Next || e.Fenced > ss.Version }) {
			t.Fatalf("decoded the session table %+v", ss)
		}
	})
}

// A disk entry that decodes is never up and gone, or drained once gone, and encodes back to the same bytes.
func FuzzDecodeDisk(f *testing.F) {
	for _, e := range []DiskEntry{
		{RecordedDisk: chunkstore.RecordedDisk{ID: 1, Log: 0x11, Device: 0xd1, Up: true}},
		{RecordedDisk: chunkstore.RecordedDisk{ID: 2, Log: 0x12, Device: 0xd2, Node: 300}, Age: 7 * AgeStep, Drain: true},
		{RecordedDisk: chunkstore.RecordedDisk{ID: 3, Log: 0x13, Device: 0xd2, Node: 1, Gone: true}},
	} {
		f.Add(e.ID, appendDisk(nil, &e))
	}
	f.Fuzz(func(t *testing.T, id uint32, data []byte) {
		e, err := decodeDisk(id, data)
		if err != nil {
			return
		}
		if e.ID != id || e.Up && (e.Gone || e.Age != 0) || e.Gone && e.Drain || !bytes.Equal(appendDisk(nil, &e), data) {
			t.Fatalf("%x decodes to %+v, which encodes to %x", data, e, appendDisk(nil, &e))
		}
	})
}

// A session entry that decodes has an incarnation and encodes back to the same bytes.
func FuzzDecodeSession(f *testing.F) {
	for _, e := range []SessionEntry{{ID: 1, Node: 4, Incarnation: 1}, {ID: 2, Node: 300, Incarnation: 7, Fenced: 9}} {
		f.Add(e.ID, appendSession(nil, &e))
	}
	f.Fuzz(func(t *testing.T, id uint64, data []byte) {
		e, err := decodeSession(id, data)
		if err != nil {
			return
		}
		if e.ID != id || e.Incarnation == 0 || !bytes.Equal(appendSession(nil, &e), data) {
			t.Fatalf("%x decodes to %+v, which encodes to %x", data, e, appendSession(nil, &e))
		}
	})
}
