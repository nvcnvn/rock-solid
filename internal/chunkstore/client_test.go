package chunkstore

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var testFile = File{Pool: testPool, Volume: 1, Inode: 7}

// tclient is a client on its own, the test as every replica: disk d is node d − 1.
type tclient struct {
	t    testing.TB
	c    *Client
	ctx  core.Context
	out  ClientOutput
	sent []core.Action
	done []Completion
	// ids, when set, sees every send and every answer the test gives.
	ids *idRules
}

func newTClient(t testing.TB, tbl Table) *tclient {
	h := &tclient{t: t, c: NewClient(ClientConfig{Session: 3, Incarnation: 2}), ctx: core.Context{Now: 1, Raw: 1, Pool: sim.NewPool(64<<20, 0)}}
	for d := uint32(1); d <= 3; d++ {
		h.c.SetAddr(d, core.NodeID{Node: d - 1})
	}
	h.c.SetTable(&h.ctx, tbl, &h.out)
	h.take()
	return h
}

// take moves the client's output to sent and done.
func (h *tclient) take() {
	for _, a := range h.out.Actions {
		if a.Kind == core.ActNetSend {
			h.sent = append(h.sent, a)
			if h.ids != nil {
				h.ids.send(&a)
			}
		}
	}
	h.done = append(h.done, h.out.Done...)
	h.out.Reset()
}

func (h *tclient) write(token uint64, off uint64, data []byte) {
	h.t.Helper()
	if err := h.c.Write(&h.ctx, token, testFile, off, data, false, &h.out); err != nil {
		h.t.Fatal(err)
	}
	h.take()
}

// updates decodes the writes and punches among the sends, and takes the sends.
func (h *tclient) updates() ([]Update, []core.Action) {
	h.t.Helper()
	var us []Update
	var as []core.Action
	for _, a := range h.sent {
		if k := Kind(a.Msg.Body[1]); k == KindWrite || k == KindPunch {
			var u Update
			if err := DecodeUpdate(a.Msg.Inline(), dataLen(h.ctx.Pool, a.Msg.Buf), &u); err != nil {
				h.t.Fatal(err)
			}
			us, as = append(us, u), append(as, a)
		}
	}
	h.sent = h.sent[:0]
	return us, as
}

func dataLen(p core.BufPool, b core.IOBuf) int {
	if b == core.NoBuf {
		return 0
	}
	return len(p.Bytes(b))
}

// answer has the replica a was sent to answer it with rp, its ID a's.
func (h *tclient) answer(a core.Action, rp Response) {
	rp.ID = core.RequestID(le.Uint64(a.Msg.Body[2:]))
	if h.ids != nil {
		h.ids.answered(rp)
	}
	ev := core.Event{Kind: core.EvNetRecv, Peer: a.Peer}
	ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendResponse(nil, &rp)))
	if rp.Kind == KindReadReply && rp.Status == StatusOK {
		var rd Read
		if err := DecodeRead(a.Msg.Inline(), 0, &rd); err != nil {
			h.t.Fatal(err)
		}
		b, err := h.ctx.Pool.AllocZeroed(dataBytes(rd.Off, rd.Len))
		if err != nil {
			h.t.Fatal(err)
		}
		copy(h.ctx.Pool.Bytes(b)[rd.Off%core.BlockSize:][:rd.Len], bytes.Repeat([]byte{byte(rd.Chunk.Index + 1)}, int(rd.Len)))
		ev.Msg.Buf = b
	}
	h.c.OnEvent(&h.ctx, &ev, &h.out)
	if h.ids != nil {
		h.ids.completed(rp, h.out.Done)
	}
	h.take()
}

func (h *tclient) event(ev core.Event) {
	h.c.OnEvent(&h.ctx, &ev, &h.out)
	h.take()
}

// at moves the clock to now and fires ClientTimer.
func (h *tclient) at(now core.Tick) {
	h.ctx.Now, h.ctx.Raw = now, now
	h.event(core.Event{Kind: core.EvTimer, Timer: ClientTimer})
}

func refused(epoch uint64) Response {
	return Response{Kind: KindReply, Status: StatusRefused, Epoch: epoch, Interval: 1}
}

// A chunk's PG is the top bits of xxh3_64 of its ID, the golden vectors of core's
// chunk_id_golden.txt for a pool of 64 PGs, and its write goes to that PG's primary tagged with the
// table's epoch and the PG's interval.
func TestAClientPlacesAChunkByItsHash(t *testing.T) {
	tbl := rtable(64, 2)
	tbl.Pools[0].PGs[40].Interval = 4
	h := newTClient(t, tbl)
	for i, g := range []struct {
		inode core.InodeID
		index uint32
		pg    uint32
	}{{0, 0, 35}, {0, 1, 33}, {1, 0, 40}, {1, 1, 20}} {
		f := File{Pool: testPool, Volume: 1, Inode: g.inode}
		if err := h.c.Write(&h.ctx, uint64(i), f, uint64(g.index)*core.ChunkSize, []byte("x"), false, &h.out); err != nil {
			t.Fatal(err)
		}
		h.take()
		us, as := h.updates()
		if len(us) != 1 || us[0].PG.PG.Index != g.pg || us[0].Chunk != (core.ChunkID{Volume: 1, Inode: g.inode, Index: g.index}) {
			t.Fatalf("%+v: sent %+v", g, us)
		}
		_, e := tbl.entry(us[0].PG.PG)
		if lead, _ := e.primary(); as[0].Peer.Node != lead-1 || us[0].PG.Epoch != 1 || us[0].PG.Interval != e.Interval {
			t.Fatalf("%+v sent to node %d as %+v, its PG led by disk %d in interval %d", g, as[0].Peer.Node, us[0].PG, lead, e.Interval)
		}
	}
}

// A connection's first send is a hello naming the session and incarnation, as is the first after
// the connection is lost; every write names them too.
func TestAClientSaysHelloOnEveryConnection(t *testing.T) {
	h := newTClient(t, rtable(1, 2))
	hellos := func() int {
		n := 0
		for _, a := range h.sent {
			var hl Hello
			if Kind(a.Msg.Body[1]) == KindHello {
				if err := DecodeHello(a.Msg.Inline(), 0, &hl); err != nil || hl.Session != 3 || hl.Incarnation != 2 || a.Peer.Node != 0 {
					t.Fatalf("hello %+v to %v: %v", hl, a.Peer, err)
				}
				n++
			}
		}
		return n
	}
	h.write(1, 0, []byte("a"))
	if n := hellos(); n != 1 || Kind(h.sent[0].Msg.Body[1]) != KindHello {
		t.Fatalf("%d hellos, first send kind %d", n, h.sent[0].Msg.Body[1])
	}
	us, _ := h.updates()
	h.write(2, 0, []byte("b"))
	n := hellos()
	more, _ := h.updates()
	us = append(us, more...)
	if n != 0 {
		t.Fatalf("%d hellos on an open connection", n)
	}
	h.event(core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: 0}})
	h.at(h.ctx.Now + ResendAfter)
	if n := hellos(); n != 1 || Kind(h.sent[0].Msg.Body[1]) != KindHello {
		t.Fatalf("%d hellos after the connection was lost", n)
	}
	more, _ = h.updates()
	us = append(us, more...)
	if len(us) != 4 {
		t.Fatalf("sent %d writes, want 2 and their 2 resends", len(us))
	}
	for _, u := range us {
		if u.Session != 3 || u.Incarnation != 2 {
			t.Fatalf("write %+v", u)
		}
	}
}

// A write across a chunk's end goes as one write per chunk, each with its own bytes, and completes
// per chunk.
func TestAWriteAcrossAChunksEndIsSplit(t *testing.T) {
	h := newTClient(t, rtable(4, 2))
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i)
	}
	off := uint64(3*core.ChunkSize - 100)
	h.write(9, off, data)
	us, as := h.updates()
	if len(us) != 2 || us[0].Chunk.Index != 2 || us[0].Off != core.ChunkSize-100 || us[0].Len != 100 || us[1].Chunk.Index != 3 || us[1].Off != 0 || us[1].Len != 200 {
		t.Fatalf("sent %+v", us)
	}
	for i, u := range us {
		got := h.ctx.Pool.Bytes(as[i].Msg.Buf)[u.Off%core.BlockSize:][:u.Len]
		if want := data[100*i : 100*i+int(u.Len)]; !bytes.Equal(got, want) {
			t.Fatalf("part %d carries % x", i, got[:8])
		}
	}
	for i := range as {
		h.answer(as[i], Response{Kind: KindReply, Version: MakeVersion(1, 1)})
	}
	if len(h.done) != 2 || h.done[0].Token != 9 || h.done[0].Off != off || h.done[0].Len != 100 || h.done[1].Off != off+100 || h.done[1].Len != 200 {
		t.Fatalf("completed %+v", h.done)
	}
}

// A refusal naming a newer table holds the write until the client holds that table, then sends it
// under its request ID to the primary the table names; a refusal naming none sends it again to the
// same primary after ClientRetryAfter.
func TestARefusalNamingANewerTableWaitsForIt(t *testing.T) {
	tbl := rtable(1, 2)
	h := newTClient(t, tbl)
	h.write(1, 0, []byte("a"))
	us, as := h.updates()
	h.answer(as[0], refused(1))
	h.at(h.ctx.Now + ClientRetryAfter)
	again, as := h.updates()
	if len(again) != 1 || again[0].ID != us[0].ID || as[0].Peer.Node != 0 {
		t.Fatalf("after a refusal at the client's own epoch sent %+v", again)
	}
	h.answer(as[0], refused(5))
	if h.out.Fetch != 0 || h.c.Epoch() != 1 {
		t.Fatal("output not taken")
	}
	for now, end := h.ctx.Now, h.ctx.Now+3*ClientResendAfter; now < end; now += ClientRetryAfter {
		h.at(now)
	}
	if us, _ := h.updates(); len(us) != 0 {
		t.Fatalf("sent %+v before the table the refusal named", us)
	}
	next := cloneTable(tbl)
	next.Epoch = 5
	next.Pools[0].PGs[0].Members[0], next.Pools[0].PGs[0].Members[1] = next.Pools[0].PGs[0].Members[1], next.Pools[0].PGs[0].Members[0]
	next.Pools[0].PGs[0].Interval = 2
	h.c.SetTable(&h.ctx, next, &h.out)
	h.take()
	sent, as := h.updates()
	if len(sent) != 1 || sent[0].ID != us[0].ID || as[0].Peer.Node != 1 || sent[0].PG.Epoch != 5 || sent[0].PG.Interval != 2 {
		t.Fatalf("with the table sent %+v to %v", sent, as)
	}
}

// A write refused without a newer table, or answered unfound, goes again after an interval that
// doubles per attempt from ClientRetryAfter up to ClientRetryMax.
func TestARefusedWriteBacksOff(t *testing.T) {
	h := newTClient(t, rtable(1, 2))
	h.write(1, 0, []byte("a"))
	_, as := h.updates()
	for i, wait := range []core.Tick{1, 2, 4, 8, 16, 32, 32} {
		rp := refused(1)
		if i%2 == 1 {
			rp = Response{Kind: KindReply, Status: StatusUnfound}
		}
		h.answer(as[0], rp)
		then := h.ctx.Now
		h.at(then + wait*ClientRetryAfter - 1)
		if us, _ := h.updates(); len(us) != 0 {
			t.Fatalf("attempt %d sent again before %d ms", i+2, wait*100)
		}
		h.at(then + wait*ClientRetryAfter)
		if _, as = h.updates(); len(as) != 1 {
			t.Fatalf("attempt %d not sent after %d ms", i+2, wait*100)
		}
	}
}

// The client asks for the table a refusal names.
func TestAClientAsksForTheTableARefusalNames(t *testing.T) {
	h := newTClient(t, rtable(1, 2))
	h.write(1, 0, []byte("a"))
	_, as := h.updates()
	rp := refused(7)
	rp.ID = core.RequestID(le.Uint64(as[0].Msg.Body[2:]))
	ev := core.Event{Kind: core.EvNetRecv, Peer: as[0].Peer}
	ev.Msg.Len = uint16(copy(ev.Msg.Body[:], AppendResponse(nil, &rp)))
	h.c.OnEvent(&h.ctx, &ev, &h.out)
	if h.out.Fetch != 7 {
		t.Fatalf("asked for table %d", h.out.Fetch)
	}
}

// A write the pool refuses for space completes as StatusNoSpace, ENOSPC, and is never sent again.
func TestAWriteRefusedForSpaceIsENOSPC(t *testing.T) {
	h := newTClient(t, rtable(1, 2))
	h.write(4, 0, []byte("a"))
	_, as := h.updates()
	h.answer(as[0], Response{Kind: KindReply, Status: StatusNoSpace})
	h.at(h.ctx.Now + 2*ClientResendAfter)
	if us, _ := h.updates(); len(us) != 0 || len(h.done) != 1 || h.done[0].Status != StatusNoSpace || h.c.Pending() != 0 {
		t.Fatalf("sent %+v, completed %+v", us, h.done)
	}
}

// A read goes to the primary, again after a refusal, and hands the caller the bytes it answered.
func TestAClientReadsCommittedBytesFromThePrimary(t *testing.T) {
	h := newTClient(t, rtable(4, 2))
	if err := h.c.Read(&h.ctx, 5, testFile, core.ChunkSize+5000, 100, &h.out); err != nil {
		t.Fatal(err)
	}
	h.take()
	var rd Read
	if len(h.sent) != 2 || Kind(h.sent[1].Msg.Body[1]) != KindRead || DecodeRead(h.sent[1].Msg.Inline(), 0, &rd) != nil || rd.Chunk.Index != 1 || rd.Off != 5000 || rd.Len != 100 || rd.HasAt {
		t.Fatalf("sent %d, read %+v", len(h.sent), rd)
	}
	a := h.sent[1]
	h.sent = h.sent[:0]
	h.answer(a, Response{Kind: KindReadReply, Status: StatusRefused, Epoch: 1, Interval: 1})
	h.at(h.ctx.Now + ClientRetryAfter)
	if len(h.sent) != 1 || le.Uint64(h.sent[0].Msg.Body[2:]) != uint64(rd.ID) {
		t.Fatalf("after a refusal sent %d", len(h.sent))
	}
	h.answer(h.sent[0], Response{Kind: KindReadReply, Version: MakeVersion(1, 3)})
	if len(h.done) != 1 || h.done[0].Token != 5 || h.done[0].Version != MakeVersion(1, 3) || h.done[0].Buf == core.NoBuf {
		t.Fatalf("completed %+v", h.done)
	}
	if got := h.ctx.Pool.Bytes(h.done[0].Buf)[5000%core.BlockSize:][:100]; !bytes.Equal(got, bytes.Repeat([]byte{2}, 100)) {
		t.Fatalf("read % x", got[:4])
	}
	if err := h.c.Read(&h.ctx, 6, testFile, 0, 1, &h.out); err != nil {
		t.Fatal(err)
	}
	h.take()
	h.answer(h.sent[len(h.sent)-1], Response{Kind: KindReadReply, Status: StatusUnfound})
	if len(h.done) != 2 || h.done[1].Status != StatusUnfound || h.c.Pending() != 0 {
		t.Fatalf("a read answered unfound completed %+v", h.done)
	}
}

// idRules holds a client's writes to the rules of request IDs: every send names its own ID at most
// RequestWindow past an oldest at or below it, no ID below the oldest lacks a final answer, a write
// goes again only under its request ID, and never once it has a final answer. It also holds what the
// client tells its caller: a write completes once, on its first final answer, as answered.
type idRules struct {
	pool  core.BufPool
	final map[core.RequestID]bool
	low   core.RequestID
	idOf  map[core.IOBuf]core.RequestID
	bufOf map[core.RequestID]core.IOBuf
	// due is whether the answer being given is its write's first final answer.
	due bool
	err error
}

func newIDRules(pool core.BufPool) *idRules {
	return &idRules{pool: pool, final: map[core.RequestID]bool{}, low: 1, idOf: map[core.IOBuf]core.RequestID{}, bufOf: map[core.RequestID]core.IOBuf{}}
}

func (r *idRules) send(a *core.Action) {
	if Kind(a.Msg.Body[1]) != KindWrite || r.err != nil {
		return
	}
	var u Update
	if err := DecodeUpdate(a.Msg.Inline(), dataLen(r.pool, a.Msg.Buf), &u); err != nil {
		r.err = fmt.Errorf("write %d names oldest outside its window: %v", le.Uint64(a.Msg.Body[2:]), err)
		return
	}
	for r.final[r.low] {
		r.low++
	}
	id, buf := u.ID, a.Msg.Buf
	switch {
	case u.Oldest > uint64(id) || uint64(id)-u.Oldest > RequestWindow:
		r.err = fmt.Errorf("write %d names oldest %d", id, u.Oldest)
	case r.final[id]:
		r.err = fmt.Errorf("write %d sent again after its final answer", id)
	case r.idOf[buf] != 0 && r.idOf[buf] != id:
		r.err = fmt.Errorf("write %d sent again as %d", r.idOf[buf], id)
	case u.Oldest > uint64(r.low):
		r.err = fmt.Errorf("write %d names oldest %d past %d, which has no final answer", id, u.Oldest, r.low)
	case r.bufOf[id] != core.NoBuf && r.bufOf[id] != buf:
		r.err = fmt.Errorf("request %d names two writes", id)
	}
	r.idOf[buf], r.bufOf[id] = id, buf
}

func (r *idRules) answered(rp Response) {
	r.due = rp.Kind == KindReply && final(rp.Status) && !r.final[rp.ID]
	if rp.Kind == KindReply && final(rp.Status) {
		r.final[rp.ID] = true
		delete(r.idOf, r.bufOf[rp.ID])
	}
}

func (r *idRules) completed(rp Response, done []Completion) {
	if r.err == nil && rp.Kind == KindReply && (len(done) != b2i(r.due) || r.due && (done[0].Status != rp.Status || done[0].Version != rp.Version)) {
		r.err = fmt.Errorf("write %d answered %d at %#x completed %+v", rp.ID, rp.Status, rp.Version, done)
	}
}

// clientRun drives a client against replicas the run plays: writes of up to 3 blocks to 8 chunks
// of 4 PGs, answered OK, refused for space, refused (naming a newer table, given later, half the
// time), answered unfound or left unanswered; lost connections; and tables that move a PG's primary
// or leave it with none. It then answers everything OK and checks that every byte written completes.
func clientRun(t *testing.T, seed uint64, plant clientPlants) error {
	rng := core.NewRand(seed ^ 0xc11e)
	tbl := rtable(4, 2)
	h := newTClient(t, tbl)
	h.ids, h.c.plant = newIDRules(h.ctx.Pool), plant
	var inflight []core.Action
	written := map[uint64]uint64{}
	var token uint64
	want := uint64(0)
	settle := func() {
		for _, a := range h.sent {
			if k := Kind(a.Msg.Body[1]); k == KindWrite {
				inflight = append(inflight, a)
			}
		}
		h.sent = h.sent[:0]
	}
	edit := func() {
		tbl = cloneTable(tbl)
		tbl.Epoch++
		e := &tbl.Pools[0].PGs[rng.Below(4)]
		e.Interval++
		switch rng.Below(3) {
		case 0:
			e.Members = append(e.Members[1:], e.Members[0])
		case 1:
			for i := range e.Members {
				e.Members[i].Mark = MarkDown
			}
		default:
			for i := range e.Members {
				e.Members[i].Mark = MarkUp
			}
		}
	}
	var later bool
	for step := 0; step < 3000; step++ {
		switch k := rng.Below(10); {
		case k < 3 && h.c.Pending() < 40:
			token++
			n := 1 + rng.Below(3*core.BlockSize)
			off := uint64(rng.Below(8))*core.ChunkSize + rng.Below(core.ChunkSize-1)
			h.write(token, off, make([]byte, n))
			written[token] = n
			want += n
		case k < 7 && len(inflight) > 0:
			i := int(rng.Below(uint64(len(inflight))))
			a := inflight[i]
			inflight = slices.Delete(inflight, i, i+1)
			switch rng.Below(8) {
			case 0, 1, 2:
				h.answer(a, Response{Kind: KindReply, Version: MakeVersion(1, 1)})
			case 3:
				h.answer(a, Response{Kind: KindReply, Status: StatusNoSpace})
			case 4:
				h.answer(a, refused(tbl.Epoch))
			case 5:
				h.answer(a, refused(tbl.Epoch+1))
				later = later || rng.Chance(1, 2)
			case 6:
				h.answer(a, Response{Kind: KindReply, Status: StatusUnfound})
			}
		case k == 7:
			h.event(core.Event{Kind: core.EvPeerLost, Peer: core.NodeID{Node: uint32(rng.Below(3))}})
		case k == 8 || later:
			later = false
			edit()
			h.c.SetTable(&h.ctx, tbl, &h.out)
			h.take()
		default:
			h.at(h.ctx.Now + core.Tick(rng.Below(uint64(ClientResendAfter))))
		}
		settle()
		if h.ids.err != nil {
			return h.ids.err
		}
	}
	for i := range tbl.Pools[0].PGs {
		for j := range tbl.Pools[0].PGs[i].Members {
			tbl.Pools[0].PGs[i].Members[j].Mark = MarkUp
		}
	}
	tbl.Epoch += 10
	h.c.SetTable(&h.ctx, tbl, &h.out)
	h.take()
	for range 100_000 {
		settle()
		if h.c.Pending() == 0 {
			break
		}
		for _, a := range inflight {
			h.answer(a, Response{Kind: KindReply, Version: MakeVersion(1, 1)})
		}
		inflight = inflight[:0]
		h.at(h.ctx.Now + ClientResendAfter)
		if h.ids.err != nil {
			return h.ids.err
		}
	}
	got := uint64(0)
	for _, d := range h.done {
		got += uint64(d.Len)
		written[d.Token] -= uint64(d.Len)
	}
	if got != want || h.c.Pending() != 0 {
		return fmt.Errorf("%d of %d bytes completed, %d parts pending", got, want, h.c.Pending())
	}
	return nil
}

// The client's request IDs keep the resend table's rules over seeded runs of writes against
// replicas that answer, refuse, lose connections and move primaries, and every write completes.
func TestAClientKeepsItsRequestIDRules(t *testing.T) {
	seeds.Run(t, 0, 20, func(seed uint64) {
		if err := clientRun(t, seed, clientPlants{}); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	})
}

// windowRun writes to a PG with no primary, its write left with an ID and unsent, and then 2
// windows of writes to another PG, each answered OK as it is sent: the client takes no ID past
// RequestWindow over the first, and sends the first once the PG has a primary again.
func windowRun(t *testing.T, plant clientPlants) error {
	tbl := rtable(2, 2)
	for i := range tbl.Pools[0].PGs[0].Members {
		tbl.Pools[0].PGs[0].Members[i].Mark = MarkDown
	}
	h := newTClient(t, tbl)
	h.ids, h.c.plant = newIDRules(h.ctx.Pool), plant
	chunk := func(pg uint32) uint64 {
		return uint64(chunkIn(core.PGID{Pool: testPool, Index: pg}, 2, 0).Index) * core.ChunkSize
	}
	h.write(1, chunk(0), []byte("stuck"))
	for i := range 2 * RequestWindow {
		h.write(uint64(i+2), chunk(1), []byte("x"))
		if h.ids.err != nil {
			return h.ids.err
		}
		_, as := h.updates()
		for _, a := range as {
			h.answer(a, Response{Kind: KindReply, Version: MakeVersion(1, 1)})
		}
		if h.ids.err != nil {
			return h.ids.err
		}
	}
	if n := len(h.done); n != RequestWindow {
		return fmt.Errorf("%d writes completed past a write with no primary, want %d", n, RequestWindow)
	}
	tbl = cloneTable(tbl)
	tbl.Epoch++
	tbl.Pools[0].PGs[0] = rtable(2, 2).Pools[0].PGs[0]
	h.c.SetTable(&h.ctx, tbl, &h.out)
	h.take()
	for h.c.Pending() > 0 && h.ids.err == nil {
		us, as := h.updates()
		if len(us) == 0 {
			return fmt.Errorf("%d writes pending, none sent", h.c.Pending())
		}
		for _, a := range as {
			h.answer(a, Response{Kind: KindReply, Version: MakeVersion(1, 1)})
		}
	}
	return h.ids.err
}

func TestAClientTakesNoIDPastTheWindow(t *testing.T) {
	if err := windowRun(t, clientPlants{}); err != nil {
		t.Fatal(err)
	}
}

// Each planted break of a request ID rule fails the client's runs.
func TestPlantedClientBugsBreakItsRequestIDRules(t *testing.T) {
	for _, p := range []struct {
		name  string
		plant clientPlants
		want  string
	}{
		{"a write sent again under a new ID", clientPlants{resendNewID: true}, "sent again as"},
		{"a write naming its own ID as the oldest", clientPlants{oldestOwn: true}, "which has no final answer"},
		{"the oldest past an ID assigned and not sent", clientPlants{oldestSent: true}, "which has no final answer"},
		{"a refusal taken as final", clientPlants{refusedFinal: true}, "answered 2 at 0x0 completed"},
		{"a write sent again after its final answer", clientPlants{keepAnswered: true}, "after its final answer"},
		{"IDs taken past the window", clientPlants{pastWindow: true}, "names oldest"},
		{"a refused write completed OK", clientPlants{refusedOK: true}, "answered 2 at 0x0 completed"},
	} {
		var err error
		for seed := uint64(0); seed < 20 && err == nil; seed++ {
			err = clientRun(t, seed, p.plant)
		}
		if err == nil {
			err = windowRun(t, p.plant)
		}
		if err == nil || !bytes.Contains([]byte(err.Error()), []byte(p.want)) {
			t.Errorf("%s: %v", p.name, err)
		}
	}
}

// oneClientOp sends one write, or with read one read, and answers it OK, the way a client on a
// node's loop does: its output reused, and the answer decoded from an event the loop reuses.
func oneClientOp(h *tclient, read bool, data []byte, ev *core.Event) {
	h.out.Reset()
	var err error
	if read {
		err = h.c.Read(&h.ctx, 1, testFile, 100, uint32(len(data)), &h.out)
	} else {
		err = h.c.Write(&h.ctx, 1, testFile, 100, data, false, &h.out)
	}
	if err != nil {
		h.t.Fatal(err)
	}
	i := len(h.out.Actions) - 1
	for h.out.Actions[i].Kind != core.ActNetSend {
		i--
	}
	a := &h.out.Actions[i]
	rp := Response{Kind: KindReply, ID: core.RequestID(le.Uint64(a.Msg.Body[2:])), Version: MakeVersion(1, 1)}
	*ev = core.Event{Kind: core.EvNetRecv, Peer: a.Peer}
	if read {
		rp.Kind = KindReadReply
		if ev.Msg.Buf, err = h.ctx.Pool.Alloc(core.BlockSize); err != nil {
			h.t.Fatal(err)
		}
	}
	ev.Msg.Len = uint16(len(AppendResponse(ev.Msg.Body[:0], &rp)))
	h.out.Reset()
	h.c.OnEvent(&h.ctx, ev, &h.out)
	if len(h.out.Done) != 1 || h.out.Done[0].Status != StatusOK {
		h.t.Fatalf("completed %+v", h.out.Done)
	}
	if read {
		h.ctx.Pool.Free(h.out.Done[0].Buf)
	}
}

func TestAClientsWritesAndReadsAllocateNothing(t *testing.T) {
	h := newTClient(t, rtable(4, 2))
	data := make([]byte, 3000)
	var ev core.Event
	for _, read := range []bool{false, true} {
		oneClientOp(h, read, data, &ev)
		if n := testing.AllocsPerRun(100, func() { oneClientOp(h, read, data, &ev) }); n != 0 {
			t.Fatalf("read %v: %v allocations per operation", read, n)
		}
	}
}

func BenchmarkClientWrite(b *testing.B) { benchClient(b, false) }

func BenchmarkClientRead(b *testing.B) { benchClient(b, true) }

func benchClient(b *testing.B, read bool) {
	h := newTClient(b, rtable(4, 2))
	data := make([]byte, 3000)
	var ev core.Event
	oneClientOp(h, read, data, &ev)
	b.ReportAllocs()
	for b.Loop() {
		oneClientOp(h, read, data, &ev)
	}
}
