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

func TestRecoveryLateOnlyForAReplyItsDecisionReads(t *testing.T) {
	const node, pg, chunk, iv = 1, 7, 9, 3
	decide := func(k uint64) trace.Record {
		return trace.Record{Name: trace.OutDecide, Fields: [trace.MaxFields]uint64{pg, k}}
	}
	for _, c := range []struct {
		name string
		rest []trace.Record
		want bool
	}{
		{"its interval decides next", []trace.Record{decide(iv + 1)}, true},
		{"no decision follows", nil, true},
		{"a later interval decides next", []trace.Record{decide(iv + 2)}, false},
	} {
		g := &gen{decisions: map[[3]uint64]bool{{node, pg, iv}: true}, replies: map[[3]uint64]map[uint64]*reply{
			{node, pg, iv + 1}: {node: {chunks: map[uint64][3]uint64{chunk: {0, 0, 1}}}},
		}}
		if got := g.repliedLater(node, pg, chunk, iv, c.rest); got != c.want {
			t.Errorf("%s: repliedLater %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPastRefusalOnlyAtTheFailOfItsVersion(t *testing.T) {
	const chunk, v = 9, 5
	g := &gen{past: map[[2]uint64][]string{{chunk, v}: {"a"}, {chunk, v + 1}: {"b"}}}
	g.heardPast(chunk, v)
	g.heardPast(chunk, v+2)
	if len(g.lines) != 1 || g.lines[0] != "[a]" {
		t.Errorf("lines %q, want [a] alone", g.lines)
	}
}
