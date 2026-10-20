package disklog_test

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// diskLogRules are the trace rules a disk log at commit depth depth keeps.
func diskLogRules(depth int) []trace.Rule {
	return []trace.Rule{trace.OneFsyncPerRound(), trace.WriteAndFlushTogether(), trace.CommitDepth(depth), trace.DurableAfterFlush(), trace.NoDurableAfterFailure()}
}

// checkIncarnations checks each incarnation of a scheduler trace, from one Restart to the
// next, on its own, since a restarted log numbers its IOs and rounds again.
func checkIncarnations(text []byte, rules ...trace.Rule) error {
	start, line, first := 0, 0, 1
	for off := 0; off < len(text); {
		end := off + bytes.IndexByte(text[off:], '\n') + 1
		line++
		r, err := trace.Parse(text[off : end-1])
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if r.Name == trace.EvRestart {
			if err := checkFrom(text[start:off], first, rules); err != nil {
				return err
			}
			start, first = off, line
		}
		off = end
	}
	return checkFrom(text[start:], first, rules)
}

func checkFrom(text []byte, first int, rules []trace.Rule) error {
	err := trace.Check(text, rules...)
	var v *trace.Violation
	if errors.As(err, &v) {
		v.Line += first - 1
	}
	return err
}
