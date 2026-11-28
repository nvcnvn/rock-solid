package main

import (
	"regexp"
	"slices"
	"strings"
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

func TestRecoveryLateForAReplyListingNothingThatItsDecisionListed(t *testing.T) {
	const node, pg, chunk, iv = 1, 7, 9, 3
	for _, listed := range []bool{false, true} {
		g := &gen{decisions: map[[3]uint64]bool{{node, pg, iv}: true, {node + 1, pg, iv + 1}: true}, listedBy: map[[4]uint64]bool{},
			replies: map[[3]uint64]map[uint64]*reply{{node + 1, pg, iv + 1}: {node: {chunks: map[uint64][3]uint64{chunk: {5, 5, 0}}}}}}
		if listed {
			g.listedBy[[4]uint64{pg, iv + 1, node, chunk}] = true
		}
		if got := g.repliedLater(node, pg, chunk, iv, nil); got != listed {
			t.Errorf("decision listed %v: repliedLater %v", listed, got)
		}
	}
}

func TestMergedSendMovesAfterTheTakeItMergedWith(t *testing.T) {
	const w = 4
	for _, c := range []struct {
		name       string
		unanswered int
		want       []string
	}{
		{"no send since the take", 0, []string{"[take]", "[send]", "[x]"}},
		{"a send since the take", 1, []string{"[send]", "[take]", "[x]"}},
	} {
		g := &gen{lines: []string{"[send]", "[take]", "[x]"}, mergedAt: map[int]int{w: 0}, takenAt: map[int]int{w: 1}, unanswered: map[int]int{w: c.unanswered}}
		g.takeMerged(w)
		if !slices.Equal(g.lines, c.want) {
			t.Errorf("%s: lines %q, want %q", c.name, g.lines, c.want)
		}
	}
}

func rec(node uint32, name trace.Name, f ...uint64) trace.Record {
	r := trace.Record{Node: node, Name: name}
	copy(r.Fields[:], f)
	return r
}

// runOf runs tracedata over a first table naming PG pg alone, then recs.
func runOf(pg uint64, recs ...trace.Record) *gen {
	g := newGen()
	g.run(append([]trace.Record{rec(0, trace.OutTable, 1), rec(0, trace.OutEntry, pg, 1, 1, 2)}, recs...))
	return g
}

func TestRunWritesAPastRefusalAtTheFailOfItsVersion(t *testing.T) {
	const pg, c, v = 7, 9, 1<<32 | 4
	for _, failed := range []bool{false, true} {
		recs := []trace.Record{rec(0, trace.OutSend, 1, c, pg, 4096, 1), rec(2, trace.OutPastRefused, c, v)}
		if failed {
			recs = append(recs, rec(0, trace.OutFail, c, v))
		}
		g := runOf(pg, recs...)
		if got := slices.ContainsFunc(g.lines, func(l string) bool { return strings.Contains(l, `"pastrefused"`) }); got != failed {
			t.Errorf("failed %v: lines %q", failed, g.lines)
		}
	}
}

func TestRunMovesAMergedSendAfterTheOrderItMergedWith(t *testing.T) {
	const pg, c = 7, 9
	send := rec(0, trace.OutSend, 1, c, pg, 4096, 1)
	g := runOf(pg, send, send, rec(0, trace.OutOrder, 1, c, 1<<32|1, 1), rec(0, trace.OutOrder, 1, c, 1<<32|2, 1))
	kind := regexp.MustCompile(`k \|-> "([a-z]+)"`)
	var kinds []string
	for _, l := range g.lines {
		kinds = append(kinds, kind.FindStringSubmatch(l)[1])
	}
	if want := []string{"send", "order", "send", "order"}; !slices.Equal(kinds, want) {
		t.Errorf("kinds %q, want %q", kinds, want)
	}
}

func TestRunCountsADecisionsListingInRepliedLater(t *testing.T) {
	const pg, c, iv = 7, 9, 3
	for _, listed := range []bool{false, true} {
		recs := []trace.Record{rec(0, trace.OutSend, 1, c, pg, 4096, 1),
			rec(1, trace.OutReply, pg, iv+1, 1, 0), rec(1, trace.OutReplyChunk, 1, c, 1<<32|1, 1<<32|1, 0), rec(1, trace.OutDecide, pg, iv+1)}
		if listed {
			recs = append(recs, rec(1, trace.OutListed, 1, c, 1<<32|1))
		}
		if got := runOf(pg, recs...).repliedLater(1, pg, c, iv, nil); got != listed {
			t.Errorf("decision listed %v: repliedLater %v", listed, got)
		}
	}
}
