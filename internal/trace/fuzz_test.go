package trace

import (
	"bytes"
	"strings"
	"testing"
)

func FuzzParse(f *testing.F) {
	for _, line := range everyName {
		f.Add([]byte(line))
	}
	for _, line := range strings.Split(scenarioExample, "\n") {
		f.Add([]byte(line))
	}
	f.Add([]byte("1 2 event pass=1 NetRecv peer=5.0 msg=write"))
	f.Fuzz(func(t *testing.T, line []byte) {
		r, err := Parse(line)
		if err != nil {
			return
		}
		if got := r.Append(nil); !bytes.Equal(got, line) {
			t.Fatalf("accepted %q but re-encoded it as %q", line, got)
		}
	})
}

func FuzzCheck(f *testing.F) {
	f.Add([]byte(scenarioExample))
	for _, c := range ruleCases {
		f.Add([]byte(c.pass))
		f.Add([]byte(c.fail))
	}
	rules := []Rule{
		OneFsyncPerRound(), WriteAndFlushTogether(), CommitDepth(2), DurableAfterFlush(), FreezeAfterCut(),
		PeeringBeforeWrite(), PeeringBeforeRead(), OneWritevPerFD(), ReadBudget(256 << 10), OneEpollWaitPerPass(),
	}
	f.Fuzz(func(t *testing.T, text []byte) {
		err := Check(text, rules...)
		if err == nil {
			return
		}
		v, ok := err.(*Violation)
		if !ok || v.Line < 0 || v.Line > bytes.Count(text, []byte("\n"))+1 {
			t.Fatalf("%v", err)
		}
	})
}
