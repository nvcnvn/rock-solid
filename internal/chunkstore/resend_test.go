package chunkstore

import (
	"bytes"
	"cmp"
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// dropRefusals drops every refusal the client would hear for request id.
func (c *rcluster) dropRefusals(id core.RequestID) {
	c.drop = func(_ int, a *core.Action) bool {
		var rp Response
		return a.Reply && DecodeResponse(a.Msg.Inline(), 0, &rp) == nil && rp.Kind == KindReply && rp.ID == id && rp.Status == StatusRefused
	}
}

// heldX checks that a client answered OK for X, written whole over the chunk's first 100 bytes, reads X back.
func (c *rcluster) heldX(id core.RequestID, ch core.ChunkID, xs []byte) error {
	if rp := c.reply(id); rp.Status != StatusOK {
		return fmt.Errorf("X answered %+v", rp)
	}
	if got := c.readNow(pg0, ch, 0, 100); !bytes.Equal(got, xs) {
		return fmt.Errorf("X acknowledged, read %q", got[:1])
	}
	return nil
}

// X is sent and at once sent again, and disk 3 refuses 1.1, which ends the interval: the resend,
// arriving while 1.1 is pending, waits for its outcome and is refused with it, the refusals lost.
// Interval 2's peering drops 1.1, and X sent once more is ordered afresh and read back. Answered
// OK with 1.1 at once (resendAtOnce, the epoch change model's ResendAtOnce), the client holds an
// acknowledgment of a version peering dropped.
func TestAResendWaitsForItsPendingVersion(t *testing.T) {
	if err := resendWhilePending(t, false); err != nil {
		t.Fatal(err)
	}
	if err := resendWhilePending(t, true); err == nil {
		t.Fatal("answered at once, the resend passed")
	}
}

func resendWhilePending(t *testing.T, atOnce bool) error {
	c := newRCluster(t, rtable(4, 2))
	for _, r := range c.reps {
		r.plant.resendAtOnce = atOnce
	}
	ch := chunkIn(pg0, 4, 0)
	xs := bytes.Repeat([]byte{'x'}, 100)
	restore := c.refuse()
	id := c.write(pg0, ch, 0, xs)
	c.resend(id, pg0, ch, 0, xs)
	c.dropRefusals(id)
	c.settle()
	restore()
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), true)
	c.settle()
	if _, ok := c.cl.replies[id]; !ok {
		c.resend(id, pg0, ch, 0, xs)
		c.settle()
	}
	return c.heldX(id, ch, xs)
}

// Disk 3 refuses X's 1.1, and interval 2's peering drops it. X sent again is ordered afresh as 2.1:
// the primary put no entry in its resend table for 1.1, which it never mapped. With the entry put
// once 1.1 is logged (writerAtLog, the epoch change model's KeepDroppedIds) the resend is answered
// OK with the dropped 1.1.
func TestAResendOfADroppedVersionIsOrderedAgain(t *testing.T) {
	if err := resendDropped(t, false); err != nil {
		t.Fatal(err)
	}
	if err := resendDropped(t, true); err == nil {
		t.Fatal("with the dropped version's entry kept, the resend passed")
	}
}

func resendDropped(t *testing.T, atLog bool) error {
	c := newRCluster(t, rtable(4, 2))
	for _, r := range c.reps {
		r.st.plant.writerAtLog = atLog
	}
	ch := chunkIn(pg0, 4, 0)
	xs := bytes.Repeat([]byte{'x'}, 100)
	restore := c.refuse()
	id := c.write(pg0, ch, 0, xs)
	c.settle()
	if rp := c.reply(id); rp.Status != StatusRefused {
		return fmt.Errorf("1.1 answered %+v", rp)
	}
	restore()
	c.publish(c.next(func(t *Table) { setInterval(t, pg0, 2) }), true)
	c.settle()
	c.resend(id, pg0, ch, 0, xs)
	c.settle()
	if rp := c.reply(id); rp.Version != MakeVersion(2, 1) {
		return fmt.Errorf("the resend answered %+v, want 2.1", rp)
	}
	return c.heldX(id, ch, xs)
}

// sendX sends write id, or a new one for 0, of data at the chunk's start, naming oldest as its
// session's oldest outstanding request ID, or the write's own for 0.
func (c *rcluster) sendX(id core.RequestID, ch core.ChunkID, data []byte, oldest core.RequestID) core.RequestID {
	delete(c.cl.replies, id)
	return c.update(c.pgFields(pg0), ch, 0, data, 0, func(u *Update) {
		if id != 0 {
			u.ID = id
		}
		u.Oldest = uint64(cmp.Or(oldest, u.ID))
	})
}

// X commits as 1.1 and its client hears it; W, naming W as its session's oldest outstanding
// request, commits as 1.2 and trims X. An attempt of X sent before its answer arrived then reaches
// the primary: it is at or below the session's watermark, so it is answered Stale and ordered
// nowhere, and a read sees W. With X's entry trimmed and no watermark (noWatermark, the epoch change
// model's NoWatermark) it is ordered again as 1.3: X, W, X.
func TestATrimmedWriteSentAgainIsStale(t *testing.T) {
	if err := trimmedResend(t, false); err != nil {
		t.Fatal(err)
	}
	if err := trimmedResend(t, true); err == nil {
		t.Fatal("with no watermark, the late attempt passed")
	}
}

func trimmedResend(t *testing.T, noWatermark bool) error {
	c := newRCluster(t, rtable(4, 2))
	for _, r := range c.reps {
		r.st.plant.noWatermark = noWatermark
	}
	ch := chunkIn(pg0, 4, 0)
	xs, ws := bytes.Repeat([]byte{'x'}, 100), bytes.Repeat([]byte{'w'}, 100)
	x := c.sendX(0, ch, xs, 0)
	c.settle()
	w := c.sendX(0, ch, ws, 0)
	c.settle()
	if rx, rw := c.reply(x), c.reply(w); rx.Version != MakeVersion(1, 1) || rw.Version != MakeVersion(1, 2) {
		return fmt.Errorf("X answered %+v, W %+v", rx, rw)
	}
	c.sendX(x, ch, xs, x)
	c.settle()
	if rp := c.reply(x); rp.Status != StatusStale {
		return fmt.Errorf("the late attempt of X answered %+v, want stale", rp)
	}
	if got := c.readNow(pg0, ch, 0, 100); !bytes.Equal(got, ws) {
		return fmt.Errorf("read %q, want W", got[:1])
	}
	return nil
}

// X commits as 1.1 and its answer is lost; X sent again is answered 1.1 from the resend table. A
// disk that trims X once it maps 1.1 (trimOnAnswer, the epoch change model's TrimOnAnswer) answers
// the resend Stale, which leaves the client with no outcome for X.
func TestAWriteIsStaleOnlyOnceItsClientHeard(t *testing.T) {
	if err := staleUnheard(t, false); err != nil {
		t.Fatal(err)
	}
	if err := staleUnheard(t, true); err == nil {
		t.Fatal("trimmed on its answer, the resend passed")
	}
}

func staleUnheard(t *testing.T, onAnswer bool) error {
	c := newRCluster(t, rtable(4, 2))
	for _, r := range c.reps {
		r.st.plant.trimOnAnswer = onAnswer
	}
	ch := chunkIn(pg0, 4, 0)
	xs := bytes.Repeat([]byte{'x'}, 100)
	lost := c.nextID + 1
	c.drop = func(_ int, a *core.Action) bool {
		var rp Response
		return a.Reply && DecodeResponse(a.Msg.Inline(), 0, &rp) == nil && rp.Kind == KindReply && rp.ID == lost && rp.Status == StatusOK
	}
	c.sendX(0, ch, xs, 0)
	c.settle()
	c.drop = nil
	c.sendX(lost, ch, xs, lost)
	c.settle()
	if rp := c.reply(lost); rp.Status != StatusOK || rp.Version != MakeVersion(1, 1) {
		return fmt.Errorf("X sent again answered %+v, never having heard 1.1", rp)
	}
	return nil
}

// X commits as 1.1 on every member and its answer is lost; interval 2 marks disk 1 down, so disk 2
// leads, and X sent to it is answered 1.1 from the entry disk 2 logged as a member.
func TestANewPrimaryAnswersAResendFromItsTable(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	xs := bytes.Repeat([]byte{'x'}, 100)
	x := c.sendX(0, ch, xs, 0)
	c.settle()
	c.publish(c.next(func(t *Table) { mark(t, pg0, 1, MarkDown) }), true)
	c.settle()
	c.sendX(x, ch, xs, x)
	c.settle()
	if rp := c.reply(x); rp.Status != StatusOK || rp.Version != MakeVersion(1, 1) {
		t.Fatalf("X sent to the new primary answered %+v, want 1.1", rp)
	}
}

// A session writes chunk B of the pool's one PG as requests 3 and 5, naming 4 as its oldest
// outstanding request with 5, so 3 is trimmed. The pool splits in two, B falling in half 1, and
// the session writes chunk A of half 0 naming 12 as its oldest. Its trim reaches the shared PG
// below and must leave a watermark there, so late attempts of 3 and 5 at half 1 are stale.
func TestASplitHalfsTrimKeepsItsSiblingsWritesStale(t *testing.T) {
	x := newIndex(t)
	a, b := chunkIn(core.PGID{Index: 0}, 2, 0), chunkIn(core.PGID{Index: 1}, 2, 0)
	whole := func(e disklog.Entry) disklog.Entry { e.Last = true; return e }
	apply(t, x, record(1, whole(disklog.WriterEntry(1, b, 1, 1, 7, 1, 3, 3))))
	apply(t, x, record(2, whole(disklog.WriterEntry(1, b, 2, 1, 7, 1, 5, 4))))
	apply(t, x, record(3, whole(disklog.WriterEntry(1, a, 1, 2, 7, 1, 12, 12))))
	for _, req := range []uint64{3, 5} {
		if v, stale := x.Resent(1, b, 2, 7, 1, req); !stale {
			t.Errorf("request %d at half 1: version %d, not stale", req, v)
		}
	}
}

// A client write of four records, its writer entry in the first, crashed at every disk action
// with no unflushed block landing, a seeded half of them, and all of them: after recovery, the
// resend table names its version only if the disk holds the version whole.
func TestAWriterEntryMapsOnlyWithItsWholeVersion(t *testing.T) {
	ch := crashChunks[0]
	workload := func(h *harness) {
		h.writeReq(ch, 0, fill(core.ChunkSize-core.BlockSize, 1), Request{Request: 5, Oldest: 5, PGs: 1, Session: 7, Incarnation: 1})
		h.settle()
	}
	full := newHarness(t, crashConfig())
	workload(full)
	if v, _ := full.s.Index().Resent(testPool, ch, 1, 7, 1, 5); v == 0 {
		t.Fatal("the write left no entry")
	}
	torn := 0
	for k := 0; k <= full.executed; k++ {
		for landing := range 3 {
			h := newHarness(t, crashConfig())
			h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
			h.crashAt = k
			func() {
				defer func() {
					if r := recover(); r != nil && r != errCrash {
						panic(r)
					}
				}()
				workload(h)
			}()
			switch landing {
			case 0:
				h.disk.Crash(func(int, core.LBA) bool { return false })
			case 1:
				h.disk.CrashRandom(core.NewRand(uint64(k)))
			default:
				h.disk.Crash(func(int, core.LBA) bool { return true })
			}
			h.crashAt = -1
			h.recoverStore(disklog.ModeOpen)
			c, _ := h.s.Index().Chunk(testPool, ch)
			if c.Version > c.Whole {
				torn++
			}
			if v, _ := h.s.Index().Resent(testPool, ch, 1, 7, 1, 5); v != 0 && c.Whole < v {
				t.Fatalf("crash at action %d, landing %d: the table names %#x, the disk holds %+v", k, landing, v, c)
			}
		}
	}
	if torn == 0 {
		t.Fatal("no crash tore the version")
	}
}

// A client write whose writer entry is durable and whose data item failed is answered with the
// failure and leaves no entry in the resend table.
func TestAFailedWriteLeavesNoResendEntry(t *testing.T) {
	h := newHarness(t, testConfig())
	ci := h.s.chunkSlot(chunkC)
	oi := h.s.newOp(opWrite, Request{Chunk: chunkC, Request: 5, Oldest: 5, PGs: 1, Session: 7, Incarnation: 1})
	o := &h.s.ops[oi]
	v := MakeVersion(0, 1)
	o.chunk, o.version, o.left = ci, v, 1
	o.items = append(o.items, item{entry: disklog.WriterEntry(testPool, chunkC, v, 1, 7, 1, 5, 5), state: itemDurable},
		item{entry: disklog.WriteEntry(testPool, chunkC, v, 0, 1)})
	o.items[1].entry.Last = true
	h.s.chunks[ci].pending = append(h.s.chunks[ci].pending, oi)
	if h.s.itemFailed(&h.ctx, oi, 1, disklog.ErrNoSpace, &h.out) {
		h.s.mapResolved(&h.ctx, ci, &h.out)
	}
	h.take()
	if got, _ := h.s.Index().Resent(testPool, chunkC, 1, 7, 1, 5); got != 0 {
		t.Fatalf("the failed write's request names %#x", got)
	}
}

// A disk's resend table names a version only while the disk holds it: a client write's entry
// goes when a decision marker lists the chunk at a version below it, when the marker drops a
// recovery that brought the chunk, along with every version of an earlier interval, and when a
// give-up drops the chunk. A resend answered from the entry would acknowledge a version no disk
// may hold, so it must find none and be ordered again.
func TestAResendEntryGoesWithItsVersion(t *testing.T) {
	c := chunk(1)
	writer := func(v, req uint64) disklog.Entry {
		e := disklog.WriterEntry(1, c, v, 1, 7, 1, req, 1)
		e.Last = true
		return e
	}
	resent := func(x *Index, req uint64) uint64 {
		v, _ := x.Resent(1, c, 1, 7, 1, req)
		return v
	}
	t.Run("decision marker", func(t *testing.T) {
		x := newIndex(t)
		apply(t, x, record(1, writer(1<<32|1, 3), writer(1<<32|2, 4)))
		apply(t, x, record(2, disklog.MissingEntry(1, c, 1<<32|1, 2), disklog.DecisionEntry(1, 0, 1, 2)))
		if got := resent(x, 4); got != 0 {
			t.Fatalf("the dropped 1.2's request names %#x", got)
		}
		if got := resent(x, 3); got != 1<<32|1 {
			t.Fatalf("the kept 1.1's request names %#x", got)
		}
	})
	t.Run("dropped recovery", func(t *testing.T) {
		x := newIndex(t)
		apply(t, x, record(1, writer(1<<32|1, 3)))
		apply(t, x, record(2, disklog.MissingEntry(1, c, 1<<32|1, 2), disklog.DecisionEntry(1, 0, 1, 2)))
		apply(t, x, record(3, disklog.RecoveredEntry(1, c, 2<<32|1, 2)))
		apply(t, x, record(4, disklog.MissingEntry(1, c, 1<<32|1, 3), disklog.DecisionEntry(1, 0, 1, 3)))
		if got := resent(x, 3); got != 0 {
			t.Fatalf("1.1's request names %#x after its recovery was dropped", got)
		}
	})
	t.Run("give-up", func(t *testing.T) {
		x := newIndex(t)
		apply(t, x, record(1, writer(1<<32|1, 3)))
		apply(t, x, record(2, disklog.DroppedEntry(1, c, 1)))
		if got := resent(x, 3); got != 0 {
			t.Fatalf("the dropped chunk's request names %#x", got)
		}
	})
}

// A session the session table fences loses its entries and watermark, and a live one keeps its
// own, both on the live index and after a rebuild replays the writes: a fenced session's writes
// are refused before any lookup, so its table only takes room, and with no entry a retry would be
// ordered again.
func TestAFencedSessionLeavesTheResendTable(t *testing.T) {
	h := newHarness(t, testConfig())
	h.s.SetSessions(SessionTable{Version: 1, Next: 10, Live: []uint64{7, 8}})
	for _, r := range []Request{{Request: 5, Oldest: 5}, {Request: 6, Oldest: 6}, {Session: 8, Request: 5, Oldest: 5}} {
		r.Session, r.Incarnation, r.PGs = cmp.Or(r.Session, 7), 1, 1
		h.sync(h.writeReq(chunkC, 0, []byte{byte(r.Session)}, r))
	}
	check := func(when string) {
		t.Helper()
		for _, req := range []uint64{5, 6} {
			if v, stale := h.s.Index().Resent(testPool, chunkC, 1, 7, 1, req); v != 0 || stale {
				t.Fatalf("%s: fenced session 7's request %d names %#x, stale %v", when, req, v, stale)
			}
		}
		if v, _ := h.s.Index().Resent(testPool, chunkC, 1, 8, 1, 5); v == 0 {
			t.Fatalf("%s: live session 8 lost its entry", when)
		}
	}
	if v, stale := h.s.Index().Resent(testPool, chunkC, 1, 7, 1, 5); v != 0 || !stale {
		t.Fatalf("session 7's trimmed request 5 names %#x, stale %v", v, stale)
	}
	h.s.SetSessions(SessionTable{Version: 2, Next: 10, Live: []uint64{8}})
	check("fenced")
	h.rebuild()
	check("rebuilt")
}

// Disk 2 is down while a session writes chunk A: X, whose answer it hears, Z, and then n more, each
// naming Z as the session's oldest outstanding request, which trims X. Disk 2 comes back and
// recovers A from disk 1, its write carrying the PG's watermarks and A's entries, the overwritten
// Z's included; then disk 1 goes down and disk 2 leads. X sent again to it is Stale and Z sent again
// is answered with its first version, with a few entries and with more than 16 blocks of recovery
// head hold. A recovery carrying no entry or watermark (recoveryForgetsIds, the epoch change model's
// RecoveryForgetsIds), or only the entries of the versions A's live map reads (recoveryLiveOnly,
// RecoveryLiveOnly), orders Z again.
func TestARecoveredMemberAnswersResendsFromItsCopy(t *testing.T) {
	for _, n := range []int{3, 1500} {
		if err := recoveredResends(t, n, func(*plants) {}); err != nil {
			t.Fatalf("%d writes after Z: %v", n, err)
		}
	}
	for _, tc := range []struct {
		name  string
		plant func(*plants)
	}{
		{"no entries", func(p *plants) { p.recoveryForgetsIds = true }},
		{"the live map's own", func(p *plants) { p.recoveryLiveOnly = true }},
	} {
		if err := recoveredResends(t, 3, tc.plant); err == nil {
			t.Errorf("copying %s, the resends passed", tc.name)
		}
	}
}

func recoveredResends(t *testing.T, n int, plant func(*plants)) error {
	c := newRCluster(t, rtable(4, 2))
	for _, r := range c.reps {
		plant(&r.st.plant)
	}
	ch := chunkIn(pg0, 4, 0)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 2, MarkDown) }), true)
	c.settle()
	x := c.sendX(0, ch, bytes.Repeat([]byte{'x'}, 100), 0)
	z := c.sendX(0, ch, bytes.Repeat([]byte{'z'}, 100), 0)
	c.settle()
	zv := c.reply(z).Version
	for i := range n {
		c.sendX(0, ch, bytes.Repeat([]byte{'a' + byte(i%20)}, 100), z)
		if i%50 == 49 {
			c.settle()
		}
	}
	c.settle()
	last := c.readNow(pg0, ch, 0, 100)
	c.publish(c.next(func(t *Table) { mark(t, pg0, 2, MarkUp) }), true)
	c.settle()
	if l := c.listed(1, ch); l.ListedIn != 0 {
		return fmt.Errorf("disk 2 still lists A: %+v", l)
	}
	c.publish(c.next(func(t *Table) { mark(t, pg0, 1, MarkDown) }), true)
	c.settle()
	c.sendX(x, ch, bytes.Repeat([]byte{'x'}, 100), x)
	c.sendX(z, ch, bytes.Repeat([]byte{'z'}, 100), z)
	c.settle()
	if rp := c.reply(x); rp.Status != StatusStale {
		return fmt.Errorf("X sent again answered %+v, want stale", rp)
	}
	if rp := c.reply(z); rp.Status != StatusOK || rp.Version != zv {
		return fmt.Errorf("Z sent again answered %+v, want %#x", rp, zv)
	}
	if got := c.readNow(pg0, ch, 0, 100); !bytes.Equal(got, last) {
		return fmt.Errorf("read %q, want %q", got[:1], last[:1])
	}
	return nil
}

// A recovery write in interval 2 copies session 7's watermark 4 and its entry for request 5 at
// 1.1. Once the recovered entry is applied the target answers request 5 with 1.1 and request 3 as
// stale, and it drops its own entry for request 9 at 1.3, an earlier interval's version the write
// does not leave on it, while keeping request 20's at 2.1, of the write's own interval. Copies whose
// recovered entry never came, as when the write tore, are dropped at the chunk's next recovery.
func TestARecoveryWriteReplacesTheTargetsEntriesForItsChunk(t *testing.T) {
	c := chunk(1)
	writer := func(v, req uint64) disklog.Entry {
		e := disklog.WriterEntry(1, c, v, 1, 7, 1, req, 1)
		e.Last = true
		return e
	}
	resent := func(x *Index, req uint64) (uint64, bool) { return x.Resent(1, c, 1, 7, 1, req) }
	copies := func(rv uint64) []disklog.Entry {
		return []disklog.Entry{disklog.ResendEntry(1, c, rv, 1, 7, 1, 0, 4), disklog.ResendEntry(1, c, rv, 1, 7, 1, 5, MakeVersion(1, 1))}
	}
	x := newIndex(t)
	apply(t, x, record(1, writer(MakeVersion(1, 3), 9), writer(MakeVersion(2, 1), 20)))
	apply(t, x, record(2, copies(MakeVersion(1, 2))...))
	apply(t, x, record(3, disklog.RecoveredEntry(1, c, MakeVersion(1, 2), 2)))
	for _, tc := range []struct {
		req     uint64
		version uint64
		stale   bool
	}{{5, MakeVersion(1, 1), false}, {3, 0, true}, {9, 0, false}, {20, MakeVersion(2, 1), false}} {
		if v, stale := resent(x, tc.req); v != tc.version || stale != tc.stale {
			t.Errorf("request %d names %#x, stale %v; want %#x, %v", tc.req, v, stale, tc.version, tc.stale)
		}
	}
	torn := newIndex(t)
	apply(t, torn, record(1, copies(MakeVersion(1, 2))...))
	apply(t, torn, record(2, disklog.RecoveredEntry(1, c, MakeVersion(1, 3), 2)))
	if v, stale := resent(torn, 5); v != 0 || stale {
		t.Errorf("a torn recovery's copy names %#x, stale %v", v, stale)
	}
}
