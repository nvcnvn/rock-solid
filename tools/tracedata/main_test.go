package main

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

func TestPersistedUnrecordedOnlyForAnIntervalNeverActivated(t *testing.T) {
	const node, pg, iv, d = 3, 7, 16, 5
	peerReply := trace.Record{Name: trace.OutReply, Fields: [trace.MaxFields]uint64{pg, 25, d, iv}}
	activate := trace.Record{Name: trace.OutActivate, Fields: [trace.MaxFields]uint64{pg, iv}}
	for _, c := range []struct {
		name string
		rest []trace.Record
		want int
	}{
		{"never activated", []trace.Record{peerReply}, 1},
		{"activated later", []trace.Record{peerReply, activate}, 0},
	} {
		g := &gen{pgs: []uint64{pg}, lists: map[[3]uint64]bool{}, replies: map[[3]uint64]map[uint64]*reply{
			{node, pg, iv}: {d: {decided: iv}},
		}}
		decide := trace.Record{Name: trace.OutDecide, Fields: [trace.MaxFields]uint64{pg, iv}}
		g.persistedUnrecorded(node, &decide, c.rest)
		if len(g.lines) != c.want {
			t.Errorf("%s: lines %q, want %d", c.name, g.lines, c.want)
		}
	}
}
