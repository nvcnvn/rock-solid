package disklog_test

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// diskOracle is the crash oracle over the disk's one index, kept as the plain model.
type diskOracle = sim.Oracle[disklog.Event, model]

// oracleFeed is a crash oracle over this log's records, whatever model it keeps.
type oracleFeed interface {
	Written(lsn uint64, ev disklog.Event) error
	Acked(lsn uint64) error
}

// watch starts a crash oracle at the log's durable LSN, whose model there is m: from now on the
// harness hands it every round the log writes and every record it reports durable, and each
// recovery's result carries what it expects.
func (h *harness) watch(m model) *diskOracle {
	if h.log.InFlight() != 0 {
		h.t.Fatalf("watch with %d rounds in flight", h.log.InFlight())
	}
	o := sim.NewOracle[disklog.Event](h.log.DurableLSN(), m.Clone())
	h.feedOracle(o)
	return o
}

// feedOracle hands o every round the log writes and every LSN it reports durable, and at the end
// of the test checks that every ticket submitted through the harness is answered or held.
func (h *harness) feedOracle(o oracleFeed) {
	if h.oracle == nil {
		b := h.tickets
		h.t.Cleanup(func() {
			if b.log != nil && !b.t.Failed() {
				b.mismatch(b.unheld())
			}
		})
	}
	h.oracle = o
}

// notedRound is a round in flight as the oracle was handed it: the entries decoded from its
// write and the tickets the log built it from.
type notedRound struct {
	entries []disklog.Entry
	tickets []disklog.Ticket
}

// crashedRounds is a dropped log's rounds in flight and the items submitted under their tickets.
type crashedRounds struct {
	rounds    map[uint64]notedRound
	submitted map[disklog.Ticket]disklog.Entry
}

// ticketBook is the items submitted through a harness and the log they went to, kept apart from
// the harness so the end-of-test check holds no disk or pool.
type ticketBook struct {
	t   testing.TB
	log *disklog.Log
	// submitted is the entry of each item, by ticket.
	submitted map[disklog.Ticket]disklog.Entry
	// unanswered is the tickets no event has answered yet.
	unanswered map[disklog.Ticket]bool
	// onMismatch, when set, takes each ticket check that fails instead of failing the test.
	onMismatch func(error)
}

// track records an item submitted under ticket t, which an event must answer.
func (b *ticketBook) track(t disklog.Ticket, e disklog.Entry) {
	b.submitted[t] = e
	b.unanswered[t] = true
}

// answer marks the tickets an event answers, and when watched fails a ticket answered twice; a
// failed item was never written, so its entry is forgotten, while an item in doubt keeps it for
// the recovery that decides.
func (b *ticketBook) answer(ev disklog.Event, watched bool) {
	b.t.Helper()
	switch ev.Kind {
	case disklog.Durable, disklog.InDoubt, disklog.Failed:
		for _, t := range ev.Tickets {
			if watched && t != disklog.NoTicket && !b.unanswered[t] {
				b.mismatch(fmt.Errorf("ticket %d answered %v after it was answered or was never submitted through the harness", t, ev.Kind))
			}
			delete(b.unanswered, t)
			if ev.Kind == disklog.Failed {
				delete(b.submitted, t)
			}
		}
	}
}

// mismatch fails the test with a failed ticket check, or hands it to onMismatch.
func (b *ticketBook) mismatch(err error) {
	b.t.Helper()
	switch {
	case err == nil:
	case b.onMismatch != nil:
		b.onMismatch(err)
	default:
		b.t.Fatalf("oracle: %v", err)
	}
}

// unheld reports a ticket that no event answered and the log neither queues nor carries in a
// round in flight: an item it lost without a word.
func (b *ticketBook) unheld() error {
	held := map[disklog.Ticket]bool{}
	for _, t := range disklog.QueuedTickets(b.log) {
		held[t] = true
	}
	for _, t := range disklog.InFlightTickets(b.log) {
		held[t] = true
	}
	for _, t := range slices.Sorted(maps.Keys(b.unanswered)) {
		if !held[t] {
			return fmt.Errorf("ticket %d (%+v) has no answer and is neither queued nor in a round in flight", t, b.submitted[t])
		}
	}
	return nil
}

// settle checks each round in flight when the log was dropped that a recovery to LSN durable
// kept against the items submitted under its tickets, then forgets them.
func (h *harness) settle(durable uint64) {
	h.t.Helper()
	c := h.crashed
	h.crashed = crashedRounds{}
	if h.oracle == nil {
		return
	}
	for _, lsn := range slices.Sorted(maps.Keys(c.rounds)) {
		if lsn <= durable {
			h.tickets.mismatch(checkTickets(lsn, c.rounds[lsn].entries, c.rounds[lsn].tickets, c.submitted))
		}
	}
}

// noteRound hands the oracle the record a round's write a carries, decoded from the bytes the
// write sends with the open sequence of the write that opened its segment, as a Durable event
// for it would report it. Writes to segment 0 are the superblock's.
func (h *harness) noteRound(a core.Action) {
	h.t.Helper()
	seg, block := uint32(uint64(a.LBA)/uint64(h.geo.SegmentBlocks)), uint32(uint64(a.LBA)%uint64(h.geo.SegmentBlocks))
	if seg == 0 {
		return
	}
	img := h.writeImage(a)
	lba := a.LBA
	if block == 0 {
		img, block, lba = img[core.BlockSize:], 1, lba+1
	}
	seq, ok := h.opened[seg]
	if !ok {
		h.t.Fatalf("a round is written at LBA %d of segment %d, which no write opened", lba, seg)
	}
	hdr, err := disklog.DecodeHeader(img, disklog.Place{LogID: h.logID, Seq: seq, Offset: block}, h.geo.SegmentBlocks-block, nil)
	if err != nil {
		h.t.Fatalf("round written at LBA %d: %v", lba, err)
	}
	ev := disklog.Event{Kind: disklog.Durable, LSN: hdr.Record.LSN, Segment: seg, Block: block, LBA: lba, HeaderBlocks: hdr.Layout.HeaderBlocks, Entries: hdr.Entries}
	for i := range ev.Entries {
		ev.Payloads = append(ev.Payloads, disklog.PayloadOffset(ev.Entries, i))
	}
	if err := h.oracle.Written(ev.LSN, ev); err != nil {
		h.t.Fatalf("oracle: %v", err)
	}
	h.noted[ev.LSN] = notedRound{entries: ev.Entries, tickets: disklog.RoundTickets(h.log, ev.LSN)}
}

// noteOpen records the open sequence of the segment a write of its block 0 opens; a block 0
// that is not a segment header of this log leaves the segment unopened.
func (h *harness) noteOpen(a core.Action) {
	seg := uint32(uint64(a.LBA) / uint64(h.geo.SegmentBlocks))
	if seg == 0 || uint64(a.LBA)%uint64(h.geo.SegmentBlocks) != 0 {
		return
	}
	sh, err := disklog.DecodeSegmentHeader(h.writeImage(a)[:core.BlockSize])
	if err != nil || sh.LogID != h.logID {
		delete(h.opened, seg)
		return
	}
	h.opened[seg] = sh.Seq
}

// writeImage is the bytes a write sends.
func (h *harness) writeImage(a core.Action) []byte {
	var img []byte
	for _, v := range a.Vec {
		img = append(img, h.pool.Bytes(v.Buf)[:v.Len]...)
	}
	return img
}

// checkTickets reports how the entries decoded from the round at lsn differ from the items
// submitted under its tickets, and forgets each item it matches; only the log's own manifest
// pieces carry no ticket.
func checkTickets(lsn uint64, entries []disklog.Entry, tickets []disklog.Ticket, submitted map[disklog.Ticket]disklog.Entry) error {
	if len(entries) != len(tickets) {
		return fmt.Errorf("LSN %d: %d entries decoded from its round, %d tickets", lsn, len(entries), len(tickets))
	}
	for i, t := range tickets {
		e := entries[i]
		if t == disklog.NoTicket {
			if e.Kind != disklog.EntryBlob || e.BlobKind != disklog.BlobManifest && e.BlobKind != disklog.BlobManifestPiece {
				return fmt.Errorf("LSN %d entry %d is %+v under no ticket", lsn, i, e)
			}
			continue
		}
		want, ok := submitted[t]
		if !ok {
			return fmt.Errorf("LSN %d entry %d: ticket %d was not submitted through the harness, or was already answered", lsn, i, t)
		}
		if e != want {
			return fmt.Errorf("LSN %d entry %d is %+v, ticket %d submitted %+v", lsn, i, e, t, want)
		}
		delete(submitted, t)
	}
	return nil
}

// oracleCheck reports how a recovery fails the crash oracle: an LSN outside its window, or an
// index other than its model at that LSN.
func oracleCheck(res *recovered) error {
	if res.oracleErr != nil {
		return res.oracleErr
	}
	if !maps.Equal(res.index, res.expect) {
		return fmt.Errorf("index at LSN %d differs from the oracle's: %s", res.durable, modelDiff(res.index, res.expect))
	}
	return nil
}

// modelDiff names the first block where got and want differ, and how many do.
func modelDiff(got, want model) string {
	all := want.Clone()
	maps.Copy(all, got)
	first, n := "", 0
	for _, k := range all.keys() {
		g, inGot := got[k]
		w, inWant := want[k]
		if inGot == inWant && g == w {
			continue
		}
		n++
		if first != "" {
			continue
		}
		switch {
		case !inGot:
			first = fmt.Sprintf("chunk %d block %d missing, want v%d at %d", k.chunk.Index, k.block, w.version, w.lba)
		case !inWant:
			first = fmt.Sprintf("chunk %d block %d v%d at %d, want none", k.chunk.Index, k.block, g.version, g.lba)
		default:
			first = fmt.Sprintf("chunk %d block %d v%d pool %d at %d, want v%d pool %d at %d", k.chunk.Index, k.block, g.version, g.pool, g.lba, w.version, w.pool, w.lba)
		}
	}
	if n > 1 {
		first += fmt.Sprintf(" (%d blocks differ)", n)
	}
	return first
}

// tornMiddle writes one to six acknowledged rounds, then commit depth rounds in flight together,
// each a chunk of its own, and crashes with every block of their writes landing but one or more
// of an earlier in-flight round's: a gap with a whole round after it. It returns the recovery
// and the LSN recovery must stop at.
func tornMiddle(t *testing.T, seed uint64, plant func(*disklog.Recovery)) (*recovered, uint64) {
	r := core.NewRand(seed)
	cfg := testConfig(0)
	cfg.CommitDepth = int(r.Range(2, 3))
	h := formatted(t, gib, cfg)
	h.plant = plant
	h.watch(model{})
	chunk := uint32(0)
	write := func() disklog.Item {
		chunk++
		return h.chunkWrite(chunk, 0, uint16(r.Range(1, 3)), uint64(chunk))
	}
	for range r.Range(1, 6) {
		h.put(disklog.Hot, write())
	}
	acked := h.log.DurableLSN()
	for range cfg.CommitDepth {
		h.submit(disklog.Hot, write())
	}
	var rounds []core.Action
	for i := 0; i < len(h.pending); {
		if a := h.pending[i].act; a.Kind == core.ActDiskWrite {
			rounds = append(rounds, a)
			h.step(i)
			continue
		}
		i++
	}
	if len(rounds) != cfg.CommitDepth {
		t.Fatalf("seed %d: %d rounds in flight, want %d", seed, len(rounds), cfg.CommitDepth)
	}
	i := int(r.Below(uint64(len(rounds) - 1)))
	torn := rounds[i]
	blocks := core.LBA(torn.Len / core.BlockSize)
	lost := map[core.LBA]bool{torn.LBA + core.LBA(r.Below(uint64(blocks))): true}
	for b := range blocks {
		if r.Chance(1, 2) {
			lost[torn.LBA+b] = true
		}
	}
	h.disk.Crash(func(_ int, lba core.LBA) bool { return !lost[lba] })
	return h.recover(disklog.ModeOpen), acked + uint64(i)
}

// A round torn between acknowledged rounds and a whole one in flight ends the log there, and
// replaying past it trips the crash oracle on every seed.
func TestOracleCatchesReplayPastATornRound(t *testing.T) {
	seeds.Run(t, 1, 50, func(seed uint64) {
		res, stop := tornMiddle(t, seed, nil)
		if res.err != nil || res.durable != stop {
			t.Fatalf("seed %d: recovered to LSN %d, %v; want %d", seed, res.durable, res.err, stop)
		}
		if err := oracleCheck(res); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if res, _ = tornMiddle(t, seed, disklog.ReplayPastGaps); res.err != nil || oracleCheck(res) == nil {
			t.Fatalf("seed %d: planted, recovery ended %v and the oracle passed it", seed, res.err)
		}
	})
}

// A round that hands its items' entries out under one another's tickets fails the check of
// each durable record's entries against the items submitted.
func TestTicketCheckCatchesSwappedTickets(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.watch(model{})
	var mismatch error
	h.tickets.onMismatch = func(err error) { mismatch = cmp.Or(mismatch, err) }
	h.put(disklog.Hot, h.chunkWrite(1, 0, 1, 1))
	if mismatch != nil {
		t.Fatalf("unplanted: %v", mismatch)
	}
	disklog.SwapTickets(h.log)
	h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 2))
	h.submit(disklog.Hot, h.chunkWrite(3, 0, 1, 3))
	h.submit(disklog.Hot, h.chunkWrite(4, 0, 1, 4))
	h.run()
	if mismatch == nil || !strings.Contains(mismatch.Error(), "LSN 3 entry 0 is") || !strings.Contains(mismatch.Error(), "ticket 4 submitted") {
		t.Fatalf("swapped tickets: %v, want LSN 3's first entry named against ticket 4", mismatch)
	}
}

// An item the log drops without an event, from its queue or with a round in flight, fails the
// ticket check when the log is dropped at a crash, and at the end of the test.
func TestTicketCheckCatchesALostItem(t *testing.T) {
	losses := []struct {
		name string
		lose func(h *harness) bool
	}{
		{"queued", func(h *harness) bool {
			h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 2))
			return disklog.LoseNewest(h.log, &h.ctx, disklog.Hot)
		}},
		{"in flight", func(h *harness) bool {
			h.step(0)
			return disklog.LoseOldestRound(h.log)
		}},
	}
	for _, loss := range losses {
		var mismatch error
		start := func(t *testing.T) *harness {
			h := formatted(t, gib, testConfig(0))
			h.tickets.onMismatch = func(err error) { mismatch = cmp.Or(mismatch, err) }
			h.watch(model{})
			h.submit(disklog.Hot, h.chunkWrite(1, 0, 1, 1))
			if !loss.lose(h) {
				t.Fatalf("%s: nothing to lose", loss.name)
			}
			return h
		}
		h := start(t)
		if mismatch != nil {
			t.Fatalf("%s, before the crash: %v", loss.name, mismatch)
		}
		h.disk.Crash(func(int, core.LBA) bool { return false })
		h.reopen()
		want := map[string]string{"queued": "ticket 2 ", "in flight": "ticket 1 "}[loss.name]
		if mismatch == nil || !strings.Contains(mismatch.Error(), want) {
			t.Fatalf("%s, at the crash: %v, want %q named", loss.name, mismatch, want)
		}
		mismatch = nil
		t.Run(loss.name, func(t *testing.T) { start(t) })
		if mismatch == nil || !strings.Contains(mismatch.Error(), want) {
			t.Fatalf("%s, at the end of the test: %v, want %q named", loss.name, mismatch, want)
		}
	}
}

// A round in flight at a crash that recovery keeps is checked against the items submitted under
// its tickets, as a round reported durable is.
func TestTicketCheckCoversKeptRounds(t *testing.T) {
	for _, swap := range []bool{false, true} {
		h := formatted(t, gib, testConfig(0))
		h.watch(model{})
		var mismatch error
		h.tickets.onMismatch = func(err error) { mismatch = cmp.Or(mismatch, err) }
		h.put(disklog.Hot, h.chunkWrite(1, 0, 1, 1))
		if swap {
			disklog.SwapTickets(h.log)
		}
		h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 2))
		h.submit(disklog.Hot, h.chunkWrite(3, 0, 1, 3))
		h.submit(disklog.Hot, h.chunkWrite(4, 0, 1, 4))
		h.round()
		h.step(0)
		if h.log.InFlight() != 1 || len(h.pending) != 1 || h.pending[0].act.Kind != core.ActDiskFsync {
			t.Fatalf("swap %v: want LSN 3's flush the only action left, have %+v", swap, h.pending)
		}
		h.disk.Crash(func(int, core.LBA) bool { return true })
		if res := h.reopen(); res.durable != 3 {
			t.Fatalf("swap %v: recovered to LSN %d, want 3", swap, res.durable)
		}
		switch {
		case !swap && mismatch != nil:
			t.Fatalf("unplanted: %v", mismatch)
		case swap && (mismatch == nil || !strings.Contains(mismatch.Error(), "LSN 3 entry 0 is") || !strings.Contains(mismatch.Error(), "ticket 4 submitted")):
			t.Fatalf("swapped tickets in a kept round: %v, want LSN 3's first entry named against ticket 4", mismatch)
		}
	}
}

// failRecorder takes a test's first Fatalf or Fatal and stops the code that made it.
type failRecorder struct {
	testing.TB
	msg string
}

type recordedFailure struct{}

func (f *failRecorder) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	panic(recordedFailure{})
}

func (f *failRecorder) Fatal(args ...any) {
	f.msg = fmt.Sprint(args...)
	panic(recordedFailure{})
}

func (f *failRecorder) Cleanup(func()) {}

// A ticket check that fails inside a scripted recovery case names the case's testdata line and
// the command that tripped it.
func TestScriptedHarnessFailuresNameTheirLine(t *testing.T) {
	rec := &failRecorder{TB: t}
	d := &datadriven.TestData{
		Pos:     "testdata/recovery:7",
		Cmd:     "run",
		CmdArgs: []datadriven.CmdArg{{Key: "plant", Vals: []string{"swap-tickets"}}},
		Input:   "write hot 1 0 1 1\nsubmit hot 2 0 1 2\nsubmit hot 3 0 1 3\nsubmit hot 4 0 1 4\nwrite hot 5 0 1 5",
	}
	func() {
		defer func() {
			if r := recover(); r != nil && r != (recordedFailure{}) {
				panic(r)
			}
		}()
		scriptedRecovery(rec, d)
	}()
	if want := `testdata/recovery:7: "write hot 5 0 1 5": oracle: LSN 3 entry 0 is`; !strings.HasPrefix(rec.msg, want) {
		t.Fatalf("failure %q, want it to start %q", rec.msg, want)
	}
}
