package chunkstore

import (
	"cmp"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var (
	// ErrTableGap reports a delta that does not follow the table it is applied to.
	ErrTableGap = errors.New("chunkstore: placement delta does not follow the table")
	// ErrTableCRC reports a delta whose result does not match the CRC32C it carries.
	ErrTableCRC = errors.New("chunkstore: placement delta's result fails its CRC32C")
)

// DeltaPool is a pool's settings in a delta: every pool of the resulting table is listed.
type DeltaPool struct {
	ID       core.PoolID
	Replicas uint8
	MinLive  uint8
	PGCount  uint32
}

// DeltaEntry is a PG entry a delta changes.
type DeltaEntry struct {
	PG    core.PGID
	Entry PGEntry
}

// Delta is one table as a change to the one before it: its epoch, its pools, the PG entries it changes and the CRC32C of the whole table it gives.
type Delta struct {
	Epoch   uint64
	CRC     uint32
	Pools   []DeltaPool
	Entries []DeltaEntry
}

// CRC is the CRC32C of t's encoding.
func (t *Table) CRC() uint32 { return core.CRC32C(AppendTable(nil, t)) }

// MakeDelta is next as a delta from prev: every entry of a pool that is new or split, and otherwise the entries that differ.
func MakeDelta(prev, next *Table) Delta {
	d := Delta{Epoch: next.Epoch, CRC: next.CRC()}
	for _, p := range next.Pools {
		d.Pools = append(d.Pools, DeltaPool{p.ID, p.Replicas, p.MinLive, uint32(len(p.PGs))})
		old := prev.pool(p.ID)
		for i := range p.PGs {
			if old == nil || len(old.PGs) != len(p.PGs) || !old.PGs[i].equal(&p.PGs[i]) {
				d.Entries = append(d.Entries, DeltaEntry{core.PGID{Pool: p.ID, Index: uint32(i)}, p.PGs[i]})
			}
		}
	}
	return d
}

func (t *Table) pool(id core.PoolID) *PoolTable {
	if i, ok := slices.BinarySearchFunc(t.Pools, id, func(p PoolTable, id core.PoolID) int { return cmp.Compare(p.ID, id) }); ok {
		return &t.Pools[i]
	}
	return nil
}

func (e *PGEntry) equal(o *PGEntry) bool {
	return e.Interval == o.Interval && e.Activated == o.Activated && e.Pinned == o.Pinned && e.Recovered == o.Recovered && slices.Equal(e.Members, o.Members) &&
		slices.EqualFunc(e.Past, o.Past, func(a, b PastInterval) bool {
			return a.Interval == b.Interval && a.MinLive == b.MinLive && slices.Equal(a.Members, b.Members)
		})
}

// Apply gives the table d makes of t: ErrTableGap unless d follows t, ErrMalformed for an entry whose interval goes backwards or a new or resized pool without exactly its PG count of entries, ErrTableCRC if the result fails d's CRC.
func (t *Table) Apply(d *Delta) (Table, error) {
	if d.Epoch != t.Epoch+1 {
		return Table{}, ErrTableGap
	}
	next := Table{Epoch: d.Epoch, Pools: make([]PoolTable, len(d.Pools))}
	for i, dp := range d.Pools {
		old := t.pool(dp.ID)
		if old != nil && len(old.PGs) != int(dp.PGCount) {
			old = nil
		}
		if old == nil && d.entries(dp.ID) != int(dp.PGCount) {
			return Table{}, ErrMalformed
		}
		p := PoolTable{ID: dp.ID, Replicas: dp.Replicas, MinLive: dp.MinLive, PGs: make([]PGEntry, dp.PGCount)}
		if old != nil {
			copy(p.PGs, old.PGs)
		}
		for _, e := range d.Entries {
			if e.PG.Pool != dp.ID {
				continue
			}
			if old != nil && e.Entry.Interval < old.PGs[e.PG.Index].Interval {
				return Table{}, ErrMalformed
			}
			p.PGs[e.PG.Index] = e.Entry
		}
		if slices.ContainsFunc(p.PGs, func(e PGEntry) bool { return e.Interval == 0 }) {
			return Table{}, ErrMalformed
		}
		next.Pools[i] = p
	}
	if next.CRC() != d.CRC {
		return Table{}, ErrTableCRC
	}
	return next, nil
}

func (d *Delta) entries(pool core.PoolID) int {
	n := 0
	for _, e := range d.Entries {
		if e.PG.Pool == pool {
			n++
		}
	}
	return n
}

// AppendTable appends t's encoding: its epoch, then each pool's settings and entries.
func AppendTable(dst []byte, t *Table) []byte {
	dst = le.AppendUint32(le.AppendUint64(dst, t.Epoch), uint32(len(t.Pools)))
	for i := range t.Pools {
		p := &t.Pools[i]
		dst = appendDeltaPool(dst, DeltaPool{p.ID, p.Replicas, p.MinLive, uint32(len(p.PGs))})
		for j := range p.PGs {
			dst = appendEntry(dst, &p.PGs[j])
		}
	}
	return dst
}

// AppendDelta appends d's encoding: its epoch, CRC and pools, then each entry after its pool and PG.
func AppendDelta(dst []byte, d *Delta) []byte {
	dst = le.AppendUint32(le.AppendUint32(le.AppendUint64(dst, d.Epoch), d.CRC), uint32(len(d.Pools)))
	for _, p := range d.Pools {
		dst = appendDeltaPool(dst, p)
	}
	dst = le.AppendUint32(dst, uint32(len(d.Entries)))
	for i := range d.Entries {
		e := &d.Entries[i]
		dst = appendEntry(le.AppendUint32(le.AppendUint32(dst, uint32(e.PG.Pool)), e.PG.Index), &e.Entry)
	}
	return dst
}

func appendDeltaPool(dst []byte, p DeltaPool) []byte {
	return le.AppendUint32(append(le.AppendUint32(dst, uint32(p.ID)), p.Replicas, p.MinLive), p.PGCount)
}

func appendEntry(dst []byte, e *PGEntry) []byte {
	dst = le.AppendUint32(le.AppendUint32(dst, e.Interval), e.Activated)
	dst = append(dst, flagIf(e.Pinned, pinnedFlag)|flagIf(e.Recovered, recoveredFlag))
	dst = appendMembers(dst, e.Members)
	dst = binary.AppendUvarint(dst, uint64(len(e.Past)))
	for _, pi := range e.Past {
		dst = appendMembers(append(le.AppendUint32(dst, pi.Interval), pi.MinLive), pi.Members)
	}
	return dst
}

// The flags byte of a PG entry.
const (
	pinnedFlag    = 0x01
	recoveredFlag = 0x02
)

func flagIf(on bool, flag uint8) uint8 {
	if on {
		return flag
	}
	return 0
}

// leavingBit marks a leaving member in its mark byte.
const leavingBit = 0x80

func appendMembers(dst []byte, ms []Member) []byte {
	dst = append(dst, uint8(len(ms)))
	for _, m := range ms {
		mark := uint8(m.Mark)
		if m.Leaving {
			mark |= leavingBit
		}
		dst = append(le.AppendUint32(dst, m.Disk), mark)
	}
	return dst
}

// DecodeTable decodes a whole table; anything AppendTable would not write is ErrMalformed.
func DecodeTable(b []byte) (Table, error) {
	r := reader{b: b, n: len(b)}
	t := Table{Epoch: r.u64()}
	n := r.count(10)
	for i := range n {
		dp := r.deltaPool()
		if i > 0 && dp.ID <= t.Pools[i-1].ID {
			r.bad = true
		}
		p := PoolTable{ID: dp.ID, Replicas: dp.Replicas, MinLive: dp.MinLive}
		if !r.bad && uint64(dp.PGCount)*11 > uint64(len(r.b)) {
			r.bad = true
		}
		for range dp.PGCount {
			if r.bad {
				break
			}
			p.PGs = append(p.PGs, r.entry())
		}
		t.Pools = append(t.Pools, p)
	}
	return t, r.end()
}

// DecodeDelta decodes a delta; an entry for a pool it does not list or a PG past its pool's count is ErrMalformed, as is anything AppendDelta would not write.
func DecodeDelta(b []byte) (Delta, error) {
	r := reader{b: b, n: len(b)}
	d := Delta{Epoch: r.u64(), CRC: r.u32()}
	n := r.count(10)
	for i := range n {
		d.Pools = append(d.Pools, r.deltaPool())
		if i > 0 && d.Pools[i].ID <= d.Pools[i-1].ID {
			r.bad = true
		}
	}
	n = r.count(19)
	for range n {
		pg := core.PGID{Pool: core.PoolID(r.u32()), Index: r.u32()}
		j, ok := slices.BinarySearchFunc(d.Pools, pg.Pool, func(p DeltaPool, id core.PoolID) int { return cmp.Compare(p.ID, id) })
		if !ok || pg.Index >= d.Pools[j].PGCount {
			r.bad = true
		}
		if k := len(d.Entries); k > 0 {
			last := d.Entries[k-1].PG
			if pg.Pool < last.Pool || pg.Pool == last.Pool && pg.Index <= last.Index {
				r.bad = true
			}
		}
		d.Entries = append(d.Entries, DeltaEntry{pg, r.entry()})
		if r.bad {
			break
		}
	}
	return d, r.end()
}

func (r *reader) deltaPool() DeltaPool {
	p := DeltaPool{ID: core.PoolID(r.u32()), Replicas: r.u8(), MinLive: r.u8(), PGCount: r.u32()}
	if p.Replicas == 0 || p.MinLive == 0 || p.MinLive > p.Replicas || core.CheckPGCount(p.PGCount) != nil {
		r.bad = true
	}
	return p
}

// entry reads a PG entry: an interval of at least 1, a last activated interval not above it, at most one leaving member and none in a recovered interval, and past intervals rising from the last activated one to below it, each with a min_live of at least 1.
func (r *reader) entry() PGEntry {
	e := PGEntry{Interval: r.u32(), Activated: r.u32()}
	flags := r.u8()
	e.Pinned, e.Recovered, e.Members = flags&pinnedFlag != 0, flags&recoveredFlag != 0, r.members()
	if e.Interval == 0 || e.Activated > e.Interval || flags&^(pinnedFlag|recoveredFlag) != 0 {
		r.bad = true
	}
	if n := leavingIn(e.Members); n > 1 || n == 1 && e.Recovered {
		r.bad = true
	}
	n := r.uvarint()
	low := e.Activated
	for i := range n {
		pi := PastInterval{Interval: r.u32(), MinLive: r.u8(), Members: r.members()}
		if pi.MinLive == 0 || pi.Interval < low || i > 0 && pi.Interval == low || pi.Interval >= e.Interval {
			r.bad = true
		}
		low = pi.Interval
		if r.bad {
			break
		}
		e.Past = append(e.Past, pi)
	}
	return e
}

// uvarint reads a count in its shortest varint encoding.
func (r *reader) uvarint() int {
	v, n := binary.Uvarint(r.b)
	if r.bad || n <= 0 || n != len(binary.AppendUvarint(nil, v)) || v > uint64(len(r.b)) {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return int(v)
}

func (r *reader) members() []Member {
	n := int(r.u8())
	if r.bad || n*5 > len(r.b) {
		r.bad = true
		return nil
	}
	var ms []Member
	for range n {
		m := Member{Disk: r.u32()}
		mark := r.u8()
		m.Mark, m.Leaving = Mark(mark&^leavingBit), mark&leavingBit != 0
		if m.Mark > MarkMaint || slices.ContainsFunc(ms, func(o Member) bool { return o.Disk == m.Disk }) {
			r.bad = true
		}
		ms = append(ms, m)
	}
	return ms
}

func (r *reader) end() error {
	if r.bad || len(r.b) != 0 {
		return ErrMalformed
	}
	return nil
}
