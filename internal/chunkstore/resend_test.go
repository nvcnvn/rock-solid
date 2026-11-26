package chunkstore

import (
	"bytes"
	"cmp"
	"fmt"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
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
