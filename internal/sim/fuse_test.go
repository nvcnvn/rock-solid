package sim_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/fuseproto"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

const (
	oRDONLY = 0
	oRDWR   = 2
)

type sfile struct {
	ino     uint64
	data    []byte
	version uint64
}

// store is the files every node's daemon serves, standing in for the mounts's storage.
type store struct {
	names map[string]*sfile
	inos  map[uint64]*sfile
}

type keepRule int

const (
	keepAlways keepRule = iota
	keepNever
	// keepUnchanged answers keep_cache unless the file changed since this node's last open of it or
	// write to it saw the version before.
	keepUnchanged
)

// daemon is a scripted FUSE daemon of one node, answering its mount's requests through frames.
type daemon struct {
	t     testing.TB
	m     *sim.Mount
	st    *store
	ops   map[fuseproto.Opcode]int
	seen  map[uint64]uint64
	queue [][]byte

	attrTTL, entryTTL uint64
	direct            bool
	keep              keepRule
	// invalOnChange sends inval_inode for the attributes before an open of a changed file.
	invalOnChange bool
	// staleAttrs writes an attribute reply with the attributes it was answered with, even after the
	// daemon read a later request for the inode.
	staleAttrs bool
	// negative answers a missing name with an entry of node ID 0 for a second instead of ENOENT.
	negative bool
	// lag lets runFiles hold replies to write later.
	lag bool
	// churn reports a new mtime on every attribute request, the spike's change counter.
	churn     bool
	churned   uint32
	maxWrite  uint32
	bigWrites bool

	// read counts the requests read for each node ID, held the replies not yet written.
	read map[uint64]uint64
	held []reply
}

// reply is a request's answer, as of the count of requests read for its node ID.
type reply struct {
	r    fuseproto.Request
	out  []byte
	read uint64
}

// tier1 sets Tier 1's daemon: a regular file's entry has a timeout of 0 and its attributes one of
// a second, and an open of a file changed since the node saw it is answered without keep_cache.
// deliver fetches a reply's attributes again, and reply answers a miss with ENOENT.
func tier1(d *daemon) { d.attrTTL, d.entryTTL, d.keep = 1, 0, keepUnchanged }

func (d *daemon) attr(f *sfile) fuseproto.Attr {
	nsec := uint32(f.version % 1_000_000_000)
	if d.churn {
		d.churned++
		nsec = 500_000_000 + d.churned
	}
	return fuseproto.Attr{Ino: f.ino, Size: uint64(len(f.data)), Mode: 0100644, Nlink: 1, Mtime: 1_700_000_000, MtimeNsec: nsec}
}

// target is the node ID a request is for: a LOOKUP's is the name's.
func (d *daemon) target(r *fuseproto.Request) uint64 {
	if r.Opcode != fuseproto.OpLookup {
		return r.NodeID
	}
	if f := d.st.names[string(r.Name)]; f != nil {
		return f.ino
	}
	return 0
}

func (d *daemon) serve(frame []byte) { d.deliver(d.answer(frame)) }

// hold answers a request and keeps the reply for deliver, as a daemon thread does that is slow to
// write it.
func (d *daemon) hold(frame []byte) { d.held = append(d.held, d.answer(frame)) }

func (d *daemon) answer(frame []byte) reply {
	d.t.Helper()
	var r fuseproto.Request
	if err := fuseproto.Decode(frame, &r); err != nil {
		d.t.Fatal(err)
	}
	d.ops[r.Opcode]++
	return reply{r: r, out: d.reply(&r), read: d.read[d.target(&r)]}
}

// deliver writes a reply, the attributes of a LOOKUP or GETATTR fetched again if the daemon read
// another request for the inode since (Tier 1), unless staleAttrs is set.
func (d *daemon) deliver(a reply) {
	d.t.Helper()
	if (a.r.Opcode == fuseproto.OpLookup || a.r.Opcode == fuseproto.OpGetattr) && !d.staleAttrs && a.read != d.read[d.target(&a.r)] {
		a.out = d.reply(&a.r)
	}
	if err := d.m.Write(a.out); err != nil {
		d.t.Fatal(err)
	}
}

func (d *daemon) reply(r *fuseproto.Request) []byte {
	f := d.st.inos[r.NodeID]
	switch r.Opcode {
	case fuseproto.OpInit:
		flags := uint64(fuseproto.FlagAutoInvalData | fuseproto.FlagMaxPages)
		if d.bigWrites {
			flags |= fuseproto.FlagBigWrites
		}
		return fuseproto.AppendInit(nil, r.Unique, &fuseproto.InitOut{Major: 7, Minor: 45, Flags: flags, MaxWrite: d.maxWrite, MaxPages: 32})
	case fuseproto.OpLookup:
		if f = d.st.names[string(r.Name)]; f == nil && d.negative {
			return fuseproto.AppendEntry(nil, r.Unique, &fuseproto.Entry{EntryValid: 1})
		} else if f == nil {
			return fuseproto.AppendError(nil, r.Unique, 2)
		}
		return fuseproto.AppendEntry(nil, r.Unique, &fuseproto.Entry{NodeID: f.ino, EntryValid: d.entryTTL, AttrValid: d.attrTTL, Attr: d.attr(f)})
	case fuseproto.OpGetattr:
		a := d.attr(f)
		return fuseproto.AppendAttr(nil, r.Unique, d.attrTTL, 0, &a)
	case fuseproto.OpOpen:
		changed := d.seen[f.ino] != f.version
		if changed && d.invalOnChange {
			if err := d.m.Write(fuseproto.AppendInvalInode(nil, f.ino, -1, 0)); err != nil {
				d.t.Fatal(err)
			}
		}
		d.seen[f.ino] = f.version
		var flags uint32
		if d.keep == keepAlways || d.keep == keepUnchanged && !changed {
			flags |= fuseproto.OpenKeepCache
		}
		if d.direct {
			flags |= fuseproto.OpenDirectIO
		}
		return fuseproto.AppendOpen(nil, r.Unique, fuseproto.Open{Fh: uint64(d.ops[r.Opcode]), Flags: flags})
	case fuseproto.OpRead:
		lo := min(r.Offset, uint64(len(f.data)))
		out := append(make([]byte, fuseproto.OutHeaderSize), f.data[lo:min(r.Offset+uint64(r.Size), uint64(len(f.data)))]...)
		fuseproto.PutDataReply(out, r.Unique)
		return out
	case fuseproto.OpWrite:
		if d.seen[f.ino] == f.version {
			d.seen[f.ino]++
		}
		f.write(int(r.Offset), r.Data)
		return fuseproto.AppendWrite(nil, r.Unique, r.Size)
	}
	return fuseproto.AppendEmpty(nil, r.Unique)
}

func (f *sfile) write(off int, data []byte) {
	if end := off + len(data); end > len(f.data) {
		f.data = append(f.data, make([]byte, end-len(f.data))...)
	}
	copy(f.data[off:], data)
	f.version++
}

func (d *daemon) inval(off, n int64) {
	if err := d.m.Write(fuseproto.AppendInvalInode(nil, 2, off, n)); err != nil {
		d.t.Fatal(err)
	}
}

// take moves the requests the mount has sent to the daemon's own queue, from which it answers
// them in any order, as a daemon with several threads does.
func (d *daemon) take() {
	for f, ok := d.m.Read(); ok; f, ok = d.m.Read() {
		var r fuseproto.Request
		if err := fuseproto.Decode(f, &r); err != nil {
			d.t.Fatal(err)
		}
		d.read[d.target(&r)]++
		d.queue = append(d.queue, f)
	}
}

type mounts struct {
	t     testing.TB
	s     *sim.Scheduler
	h     *sim.History
	st    *store
	d     []*daemon
	procs uint32
}

func newMounts(t testing.TB, seed uint64, nodes int, set func(*daemon)) *mounts {
	s := sim.New(seed, sim.DefaultSettings())
	c := &mounts{t: t, s: s, h: sim.NewHistory(s, 1024), st: &store{names: map[string]*sfile{}, inos: map[uint64]*sfile{}}}
	for range nodes {
		d := &daemon{t: t, st: c.st, ops: map[fuseproto.Opcode]int{}, seen: map[uint64]uint64{}, read: map[uint64]uint64{},
			m: sim.NewMount(s.Time, c.h), maxWrite: 128 << 10, bigWrites: true}
		set(d)
		c.d = append(c.d, d)
	}
	c.pump()
	return c
}

func (c *mounts) file(name string, data []byte) *sfile {
	f := &sfile{ino: uint64(len(c.st.names) + 2), data: data}
	c.st.names[name], c.st.inos[f.ino] = f, f
	return f
}

func (c *mounts) proc(node int) *sim.Process {
	c.procs++
	return c.d[node].m.Process(c.procs - 1)
}

// holdWrites answers every request of node but its WRITEs, which stay queued.
func (c *mounts) holdWrites(node int) {
	d := c.d[node]
	var held [][]byte
	for d.take(); len(d.queue) > 0; d.take() {
		f := d.queue[0]
		d.queue = d.queue[1:]
		if fuseproto.Opcode(binary.LittleEndian.Uint32(f[4:])) == fuseproto.OpWrite {
			held = append(held, f)
		} else {
			d.serve(f)
		}
	}
	d.queue = held
}

// pump answers every request in order until none is left.
func (c *mounts) pump() {
	for more := true; more; {
		more = false
		for _, d := range c.d {
			d.take()
			for len(d.queue) > 0 {
				f := d.queue[0]
				d.queue = d.queue[1:]
				d.serve(f)
				more = true
			}
		}
	}
}

func (c *mounts) do(p *sim.Process, call func()) string {
	c.t.Helper()
	c.s.Advance(c.s.Time() + 1)
	call()
	c.pump()
	if p.Busy() || p.Errno != 0 {
		c.t.Fatalf("system call busy %v, errno %d", p.Busy(), p.Errno)
	}
	return string(p.Data)
}

// session opens f, calls do and closes it, each call answered in order.
func (c *mounts) session(p *sim.Process, flags uint32, do func()) {
	c.t.Helper()
	c.do(p, func() { p.Open("f", flags) })
	c.do(p, do)
	c.do(p, p.Close)
}

// The FUSE coherence spike's T1 to T6 in its order, on its file of 64 KiB of A, with its
// daemon's settings: attribute and entry timeouts of an hour, keep_cache, auto_inval_data and the
// data version as the mtime's nanoseconds.
func TestTheCoherenceSpikeReplays(t *testing.T) {
	c := newMounts(t, 1, 2, func(d *daemon) { d.attrTTL, d.entryTTL = 3600, 3600 })
	f := c.file("f", bytes.Repeat([]byte("A"), 65536))
	A, B := c.d[0], c.d[1]
	a, b1 := c.proc(0), c.proc(1)
	c.do(a, func() { a.Open("f", oRDWR) })
	c.do(b1, func() { b1.Open("f", oRDWR) })
	read := func(p *sim.Process) string { return c.do(p, func() { p.Read(0, 4) }) }
	mapRead := func(p *sim.Process) string { return c.do(p, func() { p.MapRead(0, 4) }) }
	write := func(p *sim.Process, s string) { c.do(p, func() { p.Write(0, []byte(s)) }) }
	check := func(step, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s: got %q, want %q", step, got, want)
		}
	}

	check("T1 B's first read", read(b1), "AAAA")
	write(a, "BBBB")
	check("T1 B's read after A's write is stale", read(b1), "AAAA")

	B.inval(0, 0)
	check("T2 B's read after inval_inode", read(b1), "BBBB")

	check("T3 B's read refills the cache", read(b1), "BBBB")
	write(a, "CCCC")
	check("T3 B's old handle is stale again", read(b1), "BBBB")

	B.direct = true
	b2 := c.proc(1)
	c.do(b2, func() { b2.Open("f", oRDWR) })
	write(a, "DDDD")
	check("T4 B's new direct handle", read(b2), "DDDD")
	check("T4 B's old cached handle is stale", read(b1), "BBBB")
	write(b2, "EEEE")
	check("T4 B's old handle after B's own direct write", read(b1), "EEEE")
	c.do(b2, b2.Close)
	B.direct = false

	B.attrTTL = 0
	B.inval(0, 0)
	read(b1)
	clear(B.ops)
	write(a, "FFFF")
	check("T5 B's old cached handle revalidated", read(b1), "FFFF")
	check("T5 B's read again", read(b1), "FFFF")
	if B.ops[fuseproto.OpRead] != 1 || B.ops[fuseproto.OpGetattr] != 2 {
		t.Errorf("T5 B's daemon: %d READ, %d GETATTR for 2 reads, want 1 and 2", B.ops[fuseproto.OpRead], B.ops[fuseproto.OpGetattr])
	}

	check("T5b B's mapped read", mapRead(b1), "FFFF")
	write(a, "GGGG")
	check("T5b B's mapped read after A's write is stale", mapRead(b1), "FFFF")
	B.inval(0, 0)
	check("T5b B's mapped read after inval_inode", mapRead(b1), "GGGG")
	B.attrTTL = 3600

	B.attrTTL, B.churn = 0, true
	B.inval(0, 0)
	read(b1)
	copy(f.data, "QQQQ") // behind B's back, the version unchanged
	check("T5c B's old cached handle with a churned change counter", read(b1), "QQQQ")
	B.attrTTL, B.churn = 3600, false
	B.inval(0, 0)

	read(b1)
	write(a, "HHHH")
	B.keep = keepNever
	b3 := c.proc(1)
	c.do(b3, func() { b3.Open("f", oRDONLY) })
	check("T6 B's new handle without keep_cache", read(b3), "HHHH")
	c.do(b3, b3.Close)
	if A.ops[fuseproto.OpWrite] == 0 {
		t.Error("A's writes never reached its daemon")
	}
}

// words is n bytes at off as a writing handle of id writes them, for sim.CloseToOpen.
func words(id uint64, off int64, n int) []byte {
	b := make([]byte, n)
	for i := 0; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(b[i:], id<<32|uint64(off+int64(i))/8)
	}
	return b
}

// The kernel takes a file's size only from LOOKUP and GETATTR replies and keeps it for the
// attribute timeout. A read after another node's close that grew the file stops at the old size
// when the name's entry was cached, whether or not the daemon answered the open without keep_cache
// and sent inval_inode for the attributes: a write on the node past the old size, still out, makes
// the kernel drop the attributes a reader fetches (FUSE_I_SIZE_UNSTABLE). Only an entry timeout of
// 0, so the open's LOOKUP brings the size, lets the read reach it.
func TestAReadAfterAnotherNodesCloseSeesItsSizeOnlyWhenTheOpenLooksItUp(t *testing.T) {
	for _, c := range []struct {
		name     string
		entryTTL uint64
		inval    bool
		want     int
	}{
		{"entries cached", 1, false, 0},
		{"entries cached, inval_inode at open", 1, true, 0},
		{"entries not cached", 0, false, sim.PageSize},
	} {
		m := newMounts(t, 1, 2, func(d *daemon) {
			d.attrTTL, d.entryTTL, d.keep, d.invalOnChange = 1, c.entryTTL, keepUnchanged, c.inval
		})
		m.file("f", nil)
		a, w, r := m.proc(0), m.proc(1), m.proc(1)
		m.session(a, oRDWR, func() { a.Write(0, words(1, 0, sim.PageSize)) })
		m.session(r, oRDONLY, func() { r.Read(0, 8) })
		m.session(a, oRDWR, func() { a.Write(0, words(2, 0, 3*sim.PageSize)) })
		m.do(w, func() { w.Open("f", oRDWR) })
		w.Write(0, words(3, 0, 2*sim.PageSize))
		m.holdWrites(1)
		r.Open("f", oRDONLY)
		m.holdWrites(1)
		r.Read(2*sim.PageSize, sim.PageSize)
		m.holdWrites(1)
		got := len(r.Data)
		_, broken := sim.CloseToOpen(m.h.Ops())
		if r.Busy() || got != c.want || broken != (c.want == 0) {
			t.Errorf("%s: read %d bytes, want %d; close-to-open broken %v", c.name, got, c.want, broken)
		}
	}
}

// An open that skips the path walk, a reopen through /proc/self/fd or open_by_handle_at, sends no
// LOOKUP, so after another node's close a reader so opened keeps the size the node held until its
// attributes time out, then reads the current one: Tier 1's known limit. A writer so opened writes
// past the size it holds, and while that write is out the kernel drops the attributes a reader on
// its node fetches (FUSE_I_SIZE_UNSTABLE), so that reader, opened by path after the close, reads
// the held size too, and the current one once the write returns.
func TestAReopenWithoutLookupHoldsTheNodesSizeForAWhile(t *testing.T) {
	for _, writer := range []bool{false, true} {
		m := newMounts(t, 1, 2, tier1)
		f := m.file("f", nil)
		a, w, r := m.proc(0), m.proc(1), m.proc(1)
		m.session(a, oRDWR, func() { a.Write(0, words(1, 0, sim.PageSize)) })
		m.session(r, oRDONLY, func() { r.Read(0, 8) })
		m.session(a, oRDWR, func() { a.Write(0, words(2, 0, 3*sim.PageSize)) })
		m.s.Advance(m.s.Time() + 1)
		want := words(2, 0, 3*sim.PageSize)
		if writer {
			m.do(w, func() { w.OpenInode(f.ino, oRDWR) })
			w.Write(0, words(3, 0, 4*sim.PageSize))
			m.holdWrites(1)
			r.Open("f", oRDONLY)
			want = words(3, 0, 3*sim.PageSize)
		} else {
			r.OpenInode(f.ino, oRDONLY)
		}
		m.holdWrites(1)
		r.Read(0, 3*sim.PageSize)
		m.holdWrites(1)
		held := len(r.Data)
		_, broken := sim.CloseToOpen(m.h.Ops())
		m.pump()
		if !writer {
			m.s.Advance(m.s.Time() + 1_000_001)
		}
		if got := m.do(r, func() { r.Read(0, 3*sim.PageSize) }); held != sim.PageSize || !broken || got != string(want) {
			t.Errorf("writer %v: read %d bytes, want %d, close-to-open broken %v; then %d bytes, want %d of the close",
				writer, held, sim.PageSize, broken, len(got), len(want))
		}
	}
}

// The kernel drops a LOOKUP or GETATTR reply when the attributes changed after the request was
// sent (attr_version, inode.c fuse_change_attributes_i). So an inval_inode for the attributes at
// one open of a changed file would drop the fresh attributes of a writer's LOOKUP already sent,
// and the writer, writing past the old size, would stop a reader on the node at the old size
// (FUSE_I_SIZE_UNSTABLE). Tier 1 sends none, and the reader reads the close's size.
func TestAnInvalInodeAtOpenWouldDropTheSizeAnotherOpenFetches(t *testing.T) {
	for _, inval := range []bool{false, true} {
		m := newMounts(t, 1, 2, func(d *daemon) {
			tier1(d)
			if inval {
				d.invalOnChange = true
			}
		})
		m.file("f", nil)
		a, o, w, r := m.proc(0), m.proc(1), m.proc(1), m.proc(1)
		m.session(a, oRDWR, func() { a.Write(0, words(1, 0, sim.PageSize)) })
		m.session(r, oRDONLY, func() { r.Read(0, 8) })
		m.do(a, func() { a.Open("f", oRDWR) })
		d := m.d[1]
		o.Open("f", oRDONLY)
		d.take()
		d.serve(d.queue[0])
		d.take()
		open := d.queue[1] // o's OPEN, answered once the file has changed
		d.queue = nil
		m.do(a, func() { a.Write(0, words(2, 0, 3*sim.PageSize)) })
		m.do(a, a.Close)
		m.s.Advance(m.s.Time() + 1)
		w.Open("f", oRDWR)
		d.take()
		d.serve(open)
		d.serve(d.queue[0]) // w's LOOKUP
		d.queue = nil
		m.holdWrites(1)
		w.Write(0, words(3, 0, 4*sim.PageSize))
		m.holdWrites(1)
		r.Open("f", oRDONLY)
		m.holdWrites(1)
		r.Read(0, 3*sim.PageSize)
		m.holdWrites(1)
		want := 3 * sim.PageSize
		if inval {
			want = sim.PageSize
		}
		if _, broken := sim.CloseToOpen(m.h.Ops()); r.Busy() || len(r.Data) != want || broken != inval {
			t.Errorf("inval_inode at open %v: read %d bytes, want %d; close-to-open broken %v", inval, len(r.Data), want, broken)
		}
	}
}

// A reply the daemon answers and writes late, a LOOKUP sent before another node's close answered
// with the old size, lands before the LOOKUP of a later open and makes the kernel drop that one's
// fresh size (attr_version). Tier 1's daemon fetches a reply's attributes again when it has read a
// later request for the inode, which keeps the old size from landing.
func TestALateAttributeReplyHidesAnotherNodesClose(t *testing.T) {
	for _, stale := range []bool{false, true} {
		m := newMounts(t, 1, 2, func(d *daemon) { tier1(d); d.staleAttrs = stale })
		m.file("f", nil)
		a, r1, r2 := m.proc(0), m.proc(1), m.proc(1)
		m.session(a, oRDWR, func() { a.Write(0, words(1, 0, sim.PageSize)) })
		m.session(r1, oRDONLY, func() { r1.Read(0, 8) })
		m.do(a, func() { a.Open("f", oRDWR) })
		r1.Open("f", oRDONLY)
		d := m.d[1]
		d.take()
		d.hold(d.queue[0]) // r1's LOOKUP, answered with the old size
		d.queue = nil
		m.do(a, func() { a.Write(0, words(2, 0, 3*sim.PageSize)) })
		m.do(a, a.Close)
		m.s.Advance(m.s.Time() + 1)
		r2.Open("f", oRDONLY)
		d.take()
		d.deliver(d.held[0])
		d.held = nil
		m.pump()
		m.s.Advance(m.s.Time() + 1)
		r2.Read(0, 3*sim.PageSize)
		m.pump()
		if _, broken := sim.CloseToOpen(m.h.Ops()); broken != stale {
			t.Errorf("attributes written as fetched %v: read %d bytes; close-to-open broken %v", stale, len(r2.Data), broken)
		}
	}
}

// A missing name answered with ENOENT is looked up again at the next open, which finds the file
// another node created since; one answered with an entry of node ID 0 is kept as a negative entry
// for its timeout, and the open fails (dir.c fuse_lookup, fuse_dentry_revalidate).
func TestAMissIsNotCached(t *testing.T) {
	for _, negative := range []bool{false, true} {
		m := newMounts(t, 1, 1, func(d *daemon) { tier1(d); d.negative = negative })
		p := m.proc(0)
		p.Open("f", oRDONLY)
		m.pump()
		m.file("f", words(1, 0, 8))
		m.s.Advance(m.s.Time() + 1)
		p.Open("f", oRDONLY)
		m.pump()
		if want := map[bool]int32{false: 0, true: 2}[negative]; p.Busy() || p.Errno != want {
			t.Errorf("node ID 0 for a miss %v: open errno %d, want %d", negative, p.Errno, want)
		}
	}
}

// A name is looked up again once its entry timeout passes, and attributes fetched again once
// theirs does.
func TestEntryAndAttributeTimeouts(t *testing.T) {
	c := newMounts(t, 1, 1, func(d *daemon) { d.attrTTL, d.entryTTL = 1, 2 })
	c.file("f", words(1, 0, 8))
	d, p := c.d[0], c.proc(0)
	cycle := func() {
		c.do(p, func() { p.Open("f", oRDONLY) })
		c.do(p, func() { p.Read(0, 8) })
		c.do(p, p.Close)
	}
	for _, step := range []struct {
		after             core.Tick
		lookups, getattrs int
	}{{0, 1, 0}, {500_000, 1, 0}, {1_000_001, 1, 1}, {2_000_001, 2, 1}} {
		c.s.Advance(step.after)
		cycle()
		if d.ops[fuseproto.OpLookup] != step.lookups || d.ops[fuseproto.OpGetattr] != step.getattrs {
			t.Errorf("at %d: %d LOOKUP, %d GETATTR, want %d and %d", step.after, d.ops[fuseproto.OpLookup], d.ops[fuseproto.OpGetattr], step.lookups, step.getattrs)
		}
	}
}

// A write goes through the page cache: a whole page it writes is cached, a part of a page not
// cached is read from the daemon later, the attributes are fetched again at the next read, and a
// direct write drops the pages it writes over, before its WRITEs and after them, so a page a cached
// read filled while they were out goes too. Mapped reads show the cache as it is.
func TestWritesGoThroughThePageCache(t *testing.T) {
	c := newMounts(t, 1, 1, func(d *daemon) { d.attrTTL, d.entryTTL = 3600, 3600 })
	c.file("f", words(1, 0, 3*sim.PageSize))
	d, p, q := c.d[0], c.proc(0), c.proc(0)
	c.do(p, func() { p.Open("f", oRDWR) })
	check := func(step string, off int64, want uint64, reads int) {
		t.Helper()
		got := c.do(p, func() { p.MapRead(off, 8) })
		if got != string(words(want, off, 8)) || d.ops[fuseproto.OpRead] != reads {
			t.Errorf("%s: read %x after %d READs, want %x after %d", step, got, d.ops[fuseproto.OpRead], words(want, off, 8), reads)
		}
	}
	check("mapped", 0, 1, 1)
	c.do(p, func() { p.Write(sim.PageSize, words(2, sim.PageSize, sim.PageSize)) })
	check("a whole page written", sim.PageSize, 2, 1)
	c.do(p, func() { p.Write(2*sim.PageSize, words(3, 2*sim.PageSize, 8)) })
	check("part of a page not cached", 2*sim.PageSize, 3, 2)
	c.do(p, func() { p.Read(0, 8) })
	if d.ops[fuseproto.OpGetattr] != 1 {
		t.Errorf("%d GETATTR for the read after writes, want 1", d.ops[fuseproto.OpGetattr])
	}
	check("after the read", 0, 1, 3)
	d.direct = true
	c.do(q, func() { q.Open("f", oRDWR) })
	c.do(q, func() { q.Write(0, words(4, 0, 8)) })
	check("a direct write over a cached page", 0, 4, 4)
	q.Write(0, words(5, 0, 8))
	d.take()
	write := d.queue[0]
	d.queue = nil
	p.MapRead(0, 8)
	c.pump() // the READ, answered before the WRITE
	d.serve(write)
	c.pump()
	check("a cached read racing a direct write", 0, 5, 6)
}

// A size that changes drops every page, even with the mtime unchanged.
func TestASizeChangeDropsThePages(t *testing.T) {
	c := newMounts(t, 1, 1, func(d *daemon) { d.attrTTL, d.entryTTL = 0, 3600 })
	f := c.file("f", words(1, 0, 8))
	p := c.proc(0)
	c.do(p, func() { p.Open("f", oRDONLY) })
	c.do(p, func() { p.Read(0, 16) })
	f.data = words(2, 0, 16) // behind the kernel's back, the version unchanged
	if got := c.do(p, func() { p.Read(0, 16) }); got != string(words(2, 0, 16)) {
		t.Errorf("read %x, want %x", got, words(2, 0, 16))
	}
}

// A read records the least ID among its words that are not zero, a word holding another offset
// counting as ID 0, and the offset of its first zero word, or of its end if none is.
func TestAReadRecordsWhatItsWordsSay(t *testing.T) {
	c := newMounts(t, 1, 1, func(d *daemon) { d.attrTTL, d.entryTTL = 3600, 3600 })
	c.file("f", slices.Concat(words(3, 0, 16), make([]byte, 8), words(2, 24, 8), words(5, 0, 8)))
	p := c.proc(0)
	c.do(p, func() { p.Open("f", oRDONLY) })
	for _, r := range []struct {
		off         int64
		n           int
		least, zero uint64
	}{{0, 16, 3, 16}, {0, 32, 2, 16}, {24, 8, 2, 32}, {0, 40, 0, 16}} {
		c.do(p, func() { p.Read(r.off, r.n) })
		ops := c.h.Ops()
		if out := ops[len(ops)-1].Output; out[2] != r.least || out[3] != r.zero {
			t.Errorf("read of %d at %d: least ID %d, first zero at %d, want %d and %d", r.n, r.off, out[2], out[3], r.least, r.zero)
		}
	}
}

func TestTheCloseToOpenCheck(t *testing.T) {
	op := func(client uint32, at, done core.Tick, in, out [sim.OpWords]uint64) sim.Op {
		return sim.Op{Client: client, Invoke: at, Complete: done, Done: true, Input: in, Output: out}
	}
	const file = 2 << 8
	writer := []sim.Op{
		op(0, 0, 1, [4]uint64{sim.FileOpen, oRDWR}, [4]uint64{0, 2}),
		op(0, 2, 3, [4]uint64{sim.FileWrite | file, 0, 16, 7 << 32}, [4]uint64{0, 16}),
		op(0, 4, 5, [4]uint64{sim.FileClose | file}, [4]uint64{}),
	}
	for _, c := range []struct {
		name   string
		open   core.Tick
		out    [sim.OpWords]uint64
		broken bool
	}{
		{"the close's data", 6, [4]uint64{0, 16, 7, 16}, false},
		{"a word older than the close", 6, [4]uint64{0, 16, 6, 16}, true},
		{"short of the close's size", 6, [4]uint64{0, 8, 7, 8}, true},
		{"zeros past the close's end", 6, [4]uint64{0, 24, 7, 16}, false},
		{"a zero word before the close's end", 6, [4]uint64{0, 24, 7, 8}, true},
		{"a word at another offset", 6, [4]uint64{0, 16, 0, 16}, true},
		{"older data, opened before the close returned", 4, [4]uint64{0, 16, 6, 16}, false},
		{"short, opened before the close returned", 4, [4]uint64{0, 8, 6, 8}, false},
		{"a word at another offset, opened before the close returned", 4, [4]uint64{0, 16, 0, 16}, true},
	} {
		ops := append(append([]sim.Op{}, writer...),
			op(1, c.open, c.open+1, [4]uint64{sim.FileOpen, oRDONLY}, [4]uint64{0, 2}),
			op(1, 7, 8, [4]uint64{sim.FileRead | file, 0, 24}, c.out))
		if i, broken := sim.CloseToOpen(ops); broken != c.broken || broken && i != 4 {
			t.Errorf("%s: broken %v at %d, want %v", c.name, broken, i, c.broken)
		}
	}
}

const invCloseToOpen = "every open after a close sees that close's data"

// runFiles runs two nodes' processes against one daemon policy, set over Tier 1's: two
// processes a node, two files, sessions that open by name, read, read through a mapping, write
// the whole file and fsync, one writer of a file at a time, daemons that write a quarter of their
// replies late and in any order, and time moving up to 1.5 s at once past the 1 s attribute
// timeout, against Tier 1's daemon (tier1). It returns the run's history and its close-to-open
// violation.
func runFiles(t testing.TB, seed uint64, set func(*daemon)) (*sim.History, error) {
	r := core.NewRand(seed)
	c := newMounts(t, seed, 2, func(d *daemon) {
		tier1(d)
		d.lag = true
		d.maxWrite, d.bigWrites = uint32(4096*r.Range(1, 3)), r.Chance(3, 4)
		set(d)
	})
	names := []string{"a", "b"}
	for _, n := range names {
		c.file(n, nil)
	}
	type actor struct {
		p             *sim.Process
		open, closing bool
		file          int
		id            uint64
		written, size int64
		left          int
	}
	var actors []*actor
	for node := range 2 {
		for range 2 {
			actors = append(actors, &actor{p: c.proc(node)})
		}
	}
	writer := []*actor{nil, nil}
	sizes := []int64{0, 0}
	var ids uint64
	next := func(a *actor) {
		p := a.p
		switch {
		case !a.open:
			a.file, a.open, a.left = int(r.Below(2)), true, int(r.Range(1, 4))
			flags := uint32(oRDONLY)
			if writer[a.file] == nil && r.Chance(1, 2) {
				ids++
				writer[a.file], a.id, a.written, flags = a, ids, 0, oRDWR
				a.size = max(sizes[a.file], 8*int64(r.Range(1, 1536)))
			}
			p.Open(names[a.file], flags)
		case p.Errno != 0:
			c.t.Fatalf("errno %d", p.Errno)
		case a.id != 0 && a.written < a.size && (a.left == 0 || r.Chance(3, 4)):
			n := min(a.size-a.written, 8*int64(r.Range(1, 1024)))
			p.Write(a.written, words(a.id, a.written, int(n)))
			a.written += n
			sizes[a.file] = max(sizes[a.file], a.written)
		case a.left > 0:
			a.left--
			off, n := 8*int64(r.Below(uint64(sizes[a.file]+sim.PageSize)/8)), 8*int(r.Range(1, 1024))
			switch k := r.Below(5); {
			case k == 0 && a.id != 0:
				p.Fsync()
			case k < 2:
				p.MapRead(off, n)
			default:
				p.Read(off, n)
			}
		default:
			p.Close()
			a.closing = true
		}
	}
	for range 1000 {
		for _, a := range actors {
			if a.closing && !a.p.Busy() {
				if writer[a.file] == a {
					writer[a.file] = nil
				}
				a.open, a.closing, a.id = false, false, 0
			}
		}
		c.s.Advance(c.s.Time() + 1)
		switch k := r.Below(100); {
		case k < 50:
			if a := actors[r.Below(uint64(len(actors)))]; !a.p.Busy() {
				next(a)
			}
		case k < 98:
			d := c.d[r.Below(2)]
			d.take()
			if len(d.held) > 0 && r.Chance(1, 3) {
				i := r.Below(uint64(len(d.held)))
				a := d.held[i]
				d.held = append(d.held[:i], d.held[i+1:]...)
				d.deliver(a)
			} else if len(d.queue) > 0 {
				i := r.Below(uint64(len(d.queue)))
				f := d.queue[i]
				d.queue = append(d.queue[:i], d.queue[i+1:]...)
				if d.lag && r.Chance(1, 4) {
					d.hold(f)
				} else {
					d.serve(f)
				}
			}
		default:
			c.s.Advance(c.s.Time() + core.Tick(r.Below(1_500_000)))
		}
	}
	var inv sim.Invariants
	inv.Register(invCloseToOpen, func() error {
		if i, broken := sim.CloseToOpen(c.h.Ops()); broken {
			return fmt.Errorf("operation %d: %+v", i, c.h.Ops()[i])
		}
		return nil
	})
	return c.h, inv.Check()
}

func TestFileHistoriesKeepCloseToOpen(t *testing.T) {
	seeds.Run(t, 1, 200, func(seed uint64) {
		h, err := runFiles(t, seed, func(*daemon) {})
		if err != nil {
			t.Fatal(err)
		}
		if len(h.Ops()) < 100 {
			t.Fatalf("%d operations", len(h.Ops()))
		}
	})
}

// Daemons that cache a file's entry for a second fail the close-to-open check on the run's seeds:
// one answering every open with keep_cache, as if another node's close changed nothing, Tier 1's
// otherwise, whose open of a changed file leaves the kernel the size it holds, and that sending
// inval_inode for the attributes at the open too, which a write on the node past that size defeats.
func TestPlantedDaemonsBreakCloseToOpen(t *testing.T) {
	for _, c := range []struct {
		name string
		set  func(*daemon)
	}{
		{"keep_cache after another node's close", func(d *daemon) { d.entryTTL, d.keep = 1, keepAlways }},
		{"no keep_cache after a change", func(d *daemon) { d.entryTTL = 1 }},
		{"no keep_cache and inval_inode after a change", func(d *daemon) { d.entryTTL, d.invalOnChange = 1, true }},
	} {
		failed, first := 0, uint64(0)
		for seed := uint64(1); seed <= 1000; seed++ {
			if _, err := runFiles(t, seed, c.set); err != nil {
				if failed++; first == 0 {
					first = seed
				}
			}
		}
		t.Logf("%s: %d of 1,000 seeds fail, the first %d", c.name, failed, first)
		if failed == 0 {
			t.Errorf("%s: no seed of 1,000 fails close-to-open", c.name)
		}
	}
}
