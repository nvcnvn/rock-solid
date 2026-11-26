package meta

import (
	"encoding/binary"
	"math"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// An operator's command travels to the control-plane group in the same framing as a node's
// messages, its token the request number the command line chose, and its body an op byte and the
// op's arguments: a disk ID for a declaration, a removal, a drain or an undrain, a pool ID and the pool's spec for a
// pool, nothing for the failed devices, a pool ID, a PG number and the interval for a give-up, a pool ID and a PG number
// for a PG's state, and a pool ID, a PG number, a pin byte, a count and each disk ID for an edit. A spec is a uvarint whose bit i is set when the i-th of its
// optional settings is (replicas, min_live, data, parity, encode_on_write, max_per_domain,
// overcommit, write_limit, pg_count), each set one as a uvarint in that order, then sealed,
// failure domain, locality and disk class as strings. The answer carries the command's request as
// its token, and its body is the refusal as a string, empty once the command took effect, then a
// count and each failed device's node and device, then for a give-up or a PG's state the interval, a byte whose bit 0
// says every chunk and bit 1 dropped, a count and each chunk's ID, and the count of chunks unfound. A voter that does not lead answers with the
// leader it knows (KindLeader).
const (
	KindAdmin       byte = 0x4d
	KindAdminAnswer byte = 0x4e
)

// AdminOp is an operator's command.
type AdminOp byte

// The operator's commands: declare a disk lost, remove a drained disk, create or change a pool,
// list the devices nodes left failed, give up a PG's current interval, read a PG's state,
// drain a disk or end its drain, edit, pin or unpin a PG, and read the membership a node's
// report gets, the shard map with the loop of each disk.
const (
	AdminLost AdminOp = 1 + iota
	AdminRemove
	AdminPool
	AdminFailed
	AdminGiveUp
	AdminPG
	AdminDrain
	AdminUndrain
	AdminEdit
	AdminMembership
)

// Admin is an operator's command: Disk for AdminLost, AdminRemove, AdminDrain and AdminUndrain, Pool and Spec for AdminPool,
// Pool, PG and the PG's current Interval for AdminGiveUp, Pool and PG for AdminPG, and for AdminEdit Pool, PG, the
// member Disks in order, none to keep the PG's members, and whether to Pin it.
type Admin struct {
	Op       AdminOp
	Disk     uint32
	Pool     core.PoolID
	PG       uint32
	Interval uint32
	Spec     chunkstore.PoolSpec
	Disks    []uint32
	Pin      bool
}

// AdminAnswer is the answer to an operator's command: why the leader refused it, empty once it
// took effect, the failed devices AdminFailed asked for, the give-up AdminGiveUp committed or, for
// AdminPG, the PG's current interval alone, its Interval 0 for none, and for AdminPG the chunks the
// PG's primary last reported unfound in that interval.
type AdminAnswer struct {
	Refused string
	Failed  []FailedDevice
	GiveUp  chunkstore.GiveUp
	Unfound uint32
}

// EncodeAdmin writes command a, numbered request, into msg, or returns the pool's error.
func EncodeAdmin(pool core.BufPool, group, request uint64, a *Admin, msg *core.Msg) error {
	body := []byte{byte(a.Op)}
	switch a.Op {
	case AdminLost, AdminRemove, AdminDrain, AdminUndrain:
		body = binary.AppendUvarint(body, uint64(a.Disk))
	case AdminPool:
		body = appendSpec(binary.AppendUvarint(body, uint64(a.Pool)), &a.Spec)
	case AdminGiveUp:
		body = binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(body, uint64(a.Pool)), uint64(a.PG)), uint64(a.Interval))
	case AdminPG:
		body = binary.AppendUvarint(binary.AppendUvarint(body, uint64(a.Pool)), uint64(a.PG))
	case AdminEdit:
		var pin byte
		if a.Pin {
			pin = 1
		}
		body = binary.AppendUvarint(append(binary.AppendUvarint(binary.AppendUvarint(body, uint64(a.Pool)), uint64(a.PG)), pin), uint64(len(a.Disks)))
		for _, d := range a.Disks {
			body = binary.AppendUvarint(body, uint64(d))
		}
	}
	return encodeBody(pool, group, KindAdmin, request, body, msg)
}

// DecodeAdmin decodes an operator's command; one that does not decode is ErrMalformed. Peek gives
// its request.
func DecodeAdmin(pool core.BufPool, msg *core.Msg) (Admin, error) {
	b, err := msgBody(pool, KindAdmin, msg)
	if err != nil {
		return Admin{}, err
	}
	d := decoder{b: b}
	a := Admin{Op: AdminOp(d.byte())}
	switch a.Op {
	case AdminLost, AdminRemove, AdminDrain, AdminUndrain:
		n := d.uvarint()
		d.bad = d.bad || n > math.MaxUint32
		a.Disk = uint32(n)
	case AdminPool:
		n := d.uvarint()
		d.bad = d.bad || n > math.MaxUint32
		a.Pool = core.PoolID(n)
		d.spec(&a.Spec)
	case AdminGiveUp, AdminPG, AdminEdit:
		pool, pg := d.uvarint(), d.uvarint()
		d.bad = d.bad || pool > math.MaxUint32 || pg > math.MaxUint32
		a.Pool, a.PG = core.PoolID(pool), uint32(pg)
		switch a.Op {
		case AdminGiveUp:
			k := d.uvarint()
			d.bad = d.bad || k == 0 || k > math.MaxUint32
			a.Interval = uint32(k)
		case AdminEdit:
			pin := d.byte()
			d.bad = d.bad || pin > 1
			a.Pin = pin == 1
			for n := d.count(); n > 0 && !d.bad; n-- {
				disk := d.uvarint()
				d.bad = d.bad || disk > math.MaxUint32
				a.Disks = append(a.Disks, uint32(disk))
			}
		}
	case AdminFailed, AdminMembership:
	default:
		d.bad = true
	}
	if d.bad || len(d.b) != 0 {
		return Admin{}, ErrMalformed
	}
	return a, nil
}

type opt struct {
	v  uint64
	ok bool
}

func optInt[T int | uint8 | uint32](p *T) opt {
	if p == nil {
		return opt{}
	}
	return opt{uint64(*p), true}
}

func optBool(p *bool) opt {
	if p == nil {
		return opt{}
	}
	if *p {
		return opt{1, true}
	}
	return opt{0, true}
}

func appendSpec(dst []byte, s *chunkstore.PoolSpec) []byte {
	opts := [...]opt{optInt(s.Replicas), optInt(s.MinLive), optInt(s.Data), optInt(s.Parity), optBool(s.EncodeOnWrite),
		optInt(s.MaxPerDomain), optBool(s.Overcommit), optInt(s.WriteLimit), optInt(s.PGCount)}
	var set uint64
	var vals []byte
	for i, o := range opts {
		if o.ok {
			set |= 1 << i
			vals = binary.AppendUvarint(vals, o.v)
		}
	}
	dst = append(binary.AppendUvarint(dst, set), vals...)
	for _, str := range [...]string{s.Sealed, s.FailureDomain, s.Locality, s.DiskClass} {
		dst = appendBytes(dst, []byte(str))
	}
	return dst
}

// spec decodes a spec into s; a setting past its range, such as a bool other than 0 or 1, is bad.
func (d *decoder) spec(s *chunkstore.PoolSpec) {
	set := d.uvarint()
	d.bad = d.bad || set >= 1<<9
	next := func(i int, max uint64) (uint64, bool) {
		if d.bad || set&(1<<i) == 0 {
			return 0, false
		}
		v := d.uvarint()
		d.bad = d.bad || v > max
		return v, !d.bad
	}
	if v, ok := next(0, math.MaxInt32); ok {
		s.Replicas = new(int(v))
	}
	if v, ok := next(1, math.MaxInt32); ok {
		s.MinLive = new(int(v))
	}
	if v, ok := next(2, math.MaxInt32); ok {
		s.Data = new(int(v))
	}
	if v, ok := next(3, math.MaxInt32); ok {
		s.Parity = new(int(v))
	}
	if v, ok := next(4, 1); ok {
		s.EncodeOnWrite = new(v == 1)
	}
	if v, ok := next(5, math.MaxInt32); ok {
		s.MaxPerDomain = new(int(v))
	}
	if v, ok := next(6, 1); ok {
		s.Overcommit = new(v == 1)
	}
	if v, ok := next(7, math.MaxUint8); ok {
		s.WriteLimit = new(uint8(v))
	}
	if v, ok := next(8, math.MaxUint32); ok {
		s.PGCount = new(uint32(v))
	}
	s.Sealed, s.FailureDomain, s.Locality, s.DiskClass = string(d.bytes()), string(d.bytes()), string(d.bytes()), string(d.bytes())
}

// EncodeAdminAnswer writes the answer to request into msg, or returns the pool's error.
func EncodeAdminAnswer(pool core.BufPool, group, request uint64, a *AdminAnswer, msg *core.Msg) error {
	body := binary.AppendUvarint(appendBytes(nil, []byte(a.Refused)), uint64(len(a.Failed)))
	for _, f := range a.Failed {
		body = binary.AppendUvarint(binary.AppendUvarint(body, uint64(f.Node)), f.Device)
	}
	if g := &a.GiveUp; g.Interval != 0 {
		var flags byte
		if g.All {
			flags |= 1
		}
		if g.Drop {
			flags |= 2
		}
		body = binary.AppendUvarint(append(binary.AppendUvarint(body, uint64(g.Interval)), flags), uint64(len(g.Chunks)))
		for _, c := range g.Chunks {
			body = c.AppendTo(body)
		}
		body = binary.AppendUvarint(body, uint64(a.Unfound))
	}
	return encodeBody(pool, group, KindAdminAnswer, request, body, msg)
}

// DecodeAdminAnswer decodes the answer to an operator's command; one that does not decode is
// ErrMalformed. Peek gives the request it answers.
func DecodeAdminAnswer(pool core.BufPool, msg *core.Msg) (AdminAnswer, error) {
	b, err := msgBody(pool, KindAdminAnswer, msg)
	if err != nil {
		return AdminAnswer{}, err
	}
	d := decoder{b: b}
	a := AdminAnswer{Refused: string(d.bytes())}
	for n := d.count(); n > 0 && !d.bad; n-- {
		node := d.uvarint()
		d.bad = d.bad || node > math.MaxUint32
		a.Failed = append(a.Failed, FailedDevice{Node: uint32(node), Device: d.uvarint()})
	}
	if len(d.b) != 0 && !d.bad {
		g := &a.GiveUp
		k := d.uvarint()
		flags := d.byte()
		d.bad = d.bad || k == 0 || k > math.MaxUint32 || flags > 3
		g.Interval, g.All, g.Drop = uint32(k), flags&1 != 0, flags&2 != 0
		for n := d.count(); n > 0 && !d.bad; n-- {
			if len(d.b) < core.ChunkIDSize {
				d.bad = true
				break
			}
			g.Chunks = append(g.Chunks, core.ChunkIDFromBytes([core.ChunkIDSize]byte(d.b)))
			d.b = d.b[core.ChunkIDSize:]
		}
		n := d.uvarint()
		d.bad = d.bad || n > math.MaxUint32
		a.Unfound = uint32(n)
	}
	if d.bad || len(d.b) != 0 {
		return AdminAnswer{}, ErrMalformed
	}
	return a, nil
}
