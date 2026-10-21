package trace

import "testing"

// slotRound is a round with a chunk slot's write, a 4 MiB DiskWrite of one buffer, beside its
// record's, flushed once both completed, then a round of a record alone.
const slotRound = `100 0 action pass=1 DiskWrite round=1 io=7 lba=8192 len=4194304 vec=1
100 0 action pass=1 DiskWrite round=1 io=5 lba=4099 len=8192 vec=2
110 0 event pass=2 DiskDone io=7 err=ok
111 0 event pass=3 DiskDone io=5 err=ok
111 0 action pass=3 DiskFsync round=1 io=6 linked=0
120 0 event pass=4 DiskDone io=6 err=ok
120 0 output pass=4 Durable round=1 lsn=1
120 0 action pass=4 DiskWrite round=2 io=8 lba=4101 len=8192 vec=2
120 0 action pass=4 DiskFsync round=2 io=9 linked=1
130 0 event pass=5 DiskDone io=8 err=ok
131 0 event pass=6 DiskDone io=9 err=ok
131 0 output pass=6 Durable round=2 lsn=2
`

// A round may carry one slot write before its record's, under its one flush, which follows both
// writes; a second slot write, or a flush or Durable before the slot's write completed, fails.
func TestARoundWithASlotWrite(t *testing.T) {
	rules := []Rule{OneFsyncPerRound(), WriteAndFlushTogether(), CommitDepth(1), DurableAfterFlush()}
	if err := Check([]byte(slotRound), rules...); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		trace string
		rule  Rule
		line  int
	}{
		{"second slot write", `100 0 action pass=1 DiskWrite round=1 io=7 lba=8192 len=4194304 vec=1
100 0 action pass=1 DiskWrite round=1 io=8 lba=9216 len=4194304 vec=1
`, OneFsyncPerRound(), 2},
		{"flush before the slot's write completed", `100 0 action pass=1 DiskWrite round=1 io=7 lba=8192 len=4194304 vec=1
100 0 action pass=1 DiskWrite round=1 io=5 lba=4099 len=8192 vec=2
111 0 event pass=2 DiskDone io=5 err=ok
111 0 action pass=2 DiskFsync round=1 io=6 linked=0
`, OneFsyncPerRound(), 4},
		{"durable over a slot write the flush did not cover", `100 0 action pass=1 DiskWrite round=1 io=7 lba=8192 len=4194304 vec=1
100 0 action pass=1 DiskWrite round=1 io=5 lba=4099 len=8192 vec=2
111 0 event pass=2 DiskDone io=5 err=ok
111 0 action pass=2 DiskFsync round=1 io=6 linked=0
120 0 event pass=3 DiskDone io=6 err=ok
120 0 output pass=3 Durable round=1 lsn=1
`, DurableAfterFlush(), 6},
	} {
		v := wantViolation(t, Check([]byte(c.trace), c.rule))
		if v.Line != c.line {
			t.Fatalf("%s: %v, want line %d", c.name, v, c.line)
		}
	}
}
