package main

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// steps is the Trace lines votersdata writes for the trace records recs, one per string, each in
// its own pass.
func steps(t *testing.T, recs ...string) []string {
	t.Helper()
	var full []string
	for i, s := range recs {
		f := strings.SplitN(s, " ", 4)
		full = append(full, fmt.Sprintf("%s %s %s pass=%d %s", f[0], f[1], f[2], i+1, f[3]))
	}
	return data(t, full...)
}

// data is the Trace lines votersdata writes for the trace records recs, one per string.
func data(t *testing.T, recs ...string) []string {
	t.Helper()
	var rs []trace.Record
	for _, s := range recs {
		r, err := trace.Parse([]byte(s))
		if err != nil {
			t.Fatalf("record %q: %v", s, err)
		}
		rs = append(rs, r)
	}
	var b bytes.Buffer
	write(&b, "trace_data", rs)
	out := b.String()
	out = out[strings.Index(out, "Trace == <<\n")+len("Trace == <<\n") : strings.LastIndex(out, "\n>>")]
	return strings.Split(strings.ReplaceAll(out, ",\n", "\n"), "\n")
}

// boot has replica 1 lead code term 1 with its empty entry and a learner's addition, committed
// to spec index 1, which replicas 2 and 3 hold, 3 with commit 1.
var boot = []string{
	"0 0.0 output RaftEntry index=2 term=1 kind=1 change=0",
	"0 0.0 output RaftState term=1 lead=1 commit=1 applied=1 flags=1",
	"0 0.0 output RaftEntry index=3 term=1 kind=2 change=50177",
	"0 0.0 output RaftState term=1 lead=1 commit=1 applied=1 flags=1",
	"0 1.0 output RaftEntry index=2 term=1 kind=1 change=0",
	"0 1.0 output RaftEntry index=3 term=1 kind=2 change=50177",
	"0 1.0 output RaftState term=1 lead=1 commit=1 applied=1 flags=0",
	"0 2.0 output RaftEntry index=2 term=1 kind=1 change=0",
	"0 2.0 output RaftEntry index=3 term=1 kind=2 change=50177",
	"0 2.0 output RaftState term=1 lead=1 commit=1 applied=1 flags=0",
	"0 0.0 output RaftState term=1 lead=1 commit=2 applied=2 flags=1",
	"0 2.0 output RaftState term=1 lead=1 commit=2 applied=1 flags=0",
}

func before(ls []string, a, b string) bool {
	i, j := slices.Index(ls, a), slices.Index(ls, b)
	return i >= 0 && j >= 0 && i < j
}

// An append is placed past a later line of the follower's only when that line is an apply or the
// same leader's append of a prefix of its log at no higher commit.
func TestAnAppendPassesOnlyAppliesAndPrefixesOfItsLeader(t *testing.T) {
	for _, c := range []struct {
		name string
		l    line
		want bool
	}{
		{"apply", line{s: `[k |-> "apply", r |-> 3, a |-> 1]`}, true},
		{"prefix", line{s: `[k |-> "replicate"`, lead: 1, lg: []int{1}, c: 1}, true},
		{"higher commit", line{s: `[k |-> "replicate"`, lead: 1, lg: []int{1}, c: 2}, false},
		{"longer log", line{s: `[k |-> "replicate"`, lead: 1, lg: []int{1, 1, 1}, c: 1}, false},
		{"other leader", line{s: `[k |-> "replicate"`, lead: 2, lg: []int{1}, c: 1}, false},
	} {
		c.l.k, c.l.id = key{2, 0}, 3
		g := &gen{lines: []line{c.l}}
		if got := g.passes(3, key{1, 0}, 1, []int{1, 1}, 1); got != c.want {
			t.Errorf("%s: passes is %v, want %v", c.name, got, c.want)
		}
	}
}

// An append the leader sent before it crashed, taken after the follower applied, is placed
// before the crash: the follower's apply still holds with the commit the append moves.
func TestAnAppendSentBeforeItsLeaderStoppedPassesTheFollowersApply(t *testing.T) {
	ls := steps(t, append(slices.Clone(boot),
		"0 0.0 output RaftState term=1 lead=1 commit=3 applied=2 flags=1",
		"0 0.0 event Crash",
		"0 2.0 output RaftState term=1 lead=1 commit=2 applied=2 flags=0",
		"0 2.0 output RaftState term=1 lead=1 commit=3 applied=2 flags=0",
	)...)
	app := `[k |-> "replicate", i |-> 1, j |-> 3, lg |-> <<1, 1>>, c |-> 2]`
	if !before(ls, app, `[k |-> "crash", r |-> 1]`) || !before(ls, `[k |-> "commit", i |-> 1, c |-> 2]`, app) {
		t.Fatalf("the append is not placed between the leader's commit and its crash:\n%s", strings.Join(ls, "\n"))
	}
}

// Of two terms a leaderless follower's log moved across whose leaders both led by now, the
// append is the later term's leader's.
func TestAnAppendIsTheLatestLedTermsLeaders(t *testing.T) {
	g := &gen{
		elected: []uint64{1, 2},
		leaders: map[uint64]uint64{1: 1, 2: 2},
		reps:    []*replica{{hist: []sent{{leads: true, term: 1}}}, {hist: []sent{{leads: true, term: 2}}}},
	}
	if l, term := g.sender(1, 2); l != 2 || term != 2 {
		t.Fatalf("sender is replica %d of term %d, want replica 2 of term 2", l, term)
	}
}

// A follower that took an append and then a vote of a later term in one pass names no leader:
// the append is the leader's of the term it left, as the later term's candidate has not won yet.
func TestAnAppendTakenWithALaterTermsVoteIsTheEarlierLeaders(t *testing.T) {
	ls := steps(t, append(slices.Clone(boot),
		"0 0.0 output RaftEntry index=4 term=1 kind=2 change=50177",
		"0 0.0 output RaftState term=1 lead=1 commit=2 applied=2 flags=1",
		"0 2.0 output RaftEntry index=4 term=1 kind=2 change=50177",
		"0 2.0 output RaftState term=2 lead=0 commit=2 applied=1 flags=0",
		"0 1.0 output RaftEntry index=4 term=2 kind=1 change=0",
		"0 1.0 output RaftState term=2 lead=2 commit=1 applied=1 flags=1",
	)...)
	if !slices.Contains(ls, `[k |-> "replicate", i |-> 1, j |-> 3, lg |-> <<1, 1, 1>>, c |-> 1]`) {
		t.Fatalf("the append is not leader 1's:\n%s", strings.Join(ls, "\n"))
	}
}

// A test's halt with nothing to apply after a crash and restart takes the place of the last apply
// before the crash, and the restart goes below the halt, an apply halting the replica again.
func TestAHaltAfterARestartIsTheApplyBeforeTheCrash(t *testing.T) {
	ls := steps(t, append(slices.Clone(boot),
		"0 0.0 event Crash",
		"0 0.0 event Restart",
		"0 0.0 output RaftState term=1 lead=0 commit=2 applied=2 flags=0",
		"0 0.0 output RaftState term=1 lead=0 commit=2 applied=2 flags=4",
	)...)
	want := []string{
		`[k |-> "halt", r |-> 1, a |-> 1]`,
		`[k |-> "crash", r |-> 1]`,
		`[k |-> "restart", r |-> 1, c |-> 1, a |-> 0]`,
		`[k |-> "apply", r |-> 1, a |-> 1]`,
	}
	got := slices.DeleteFunc(slices.Clone(ls), func(s string) bool {
		return !strings.Contains(s, `r |-> 1,`) && !strings.HasSuffix(s, `r |-> 1]`) || strings.Contains(s, `"term"`)
	})
	if !slices.Equal(got, want) {
		t.Fatalf("replica 1's lines are\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// handOff is boot with no learner, then replica 2 taking replica 1's MsgTimeoutNow and winning
// term 2, and replica 1 stepping down after applying spec index 1.
var handOff = []string{
	"0 0.0 output pass=1 RaftEntry index=2 term=1 kind=1 change=0",
	"0 0.0 output pass=2 RaftState term=1 lead=1 commit=1 applied=1 flags=1",
	"0 1.0 output pass=5 RaftEntry index=2 term=1 kind=1 change=0",
	"0 1.0 output pass=7 RaftState term=1 lead=1 commit=1 applied=1 flags=0",
	"0 2.0 output pass=8 RaftEntry index=2 term=1 kind=1 change=0",
	"0 2.0 output pass=10 RaftState term=1 lead=1 commit=1 applied=1 flags=0",
	"0 0.0 output pass=11 RaftState term=1 lead=1 commit=2 applied=2 flags=1",
	"0 2.0 output pass=12 RaftState term=1 lead=1 commit=2 applied=1 flags=0",
	"0 1.0 event pass=13 NetRecv peer=0 msg=14",
	"0 1.0 output pass=13 RaftEntry index=3 term=2 kind=1 change=0",
	"0 1.0 output pass=13 RaftState term=2 lead=2 commit=1 applied=1 flags=1",
	"0 0.0 output pass=16 RaftState term=2 lead=2 commit=2 applied=2 flags=0",
}

// A test's halt with nothing to apply keeps its place at the end when a hand-off, which the spec's
// halted leader enables, a line of the replica's other than one crash and restart, or a restart
// below the apply comes between it and the replica's last apply.
func TestAHaltKeepsItsPlacePastAHandOffOrALineOfItsOwn(t *testing.T) {
	for _, c := range []struct {
		name, halt string
		recs       []string
	}{
		{"hand-off", `[k |-> "halt", r |-> 1, a |-> 1]`, append(slices.Clone(handOff),
			"0 0.0 output pass=17 RaftState term=2 lead=2 commit=2 applied=2 flags=4",
		)},
		{"hand-off and restart", `[k |-> "halt", r |-> 1, a |-> 1]`, append(slices.Clone(handOff),
			"0 0.0 event pass=17 Crash",
			"0 0.0 event pass=18 Restart",
			"0 0.0 output pass=19 RaftState term=2 lead=0 commit=2 applied=2 flags=0",
			"0 0.0 output pass=20 RaftState term=2 lead=0 commit=2 applied=2 flags=4",
		)},
		{"append before the crash", `[k |-> "halt", r |-> 3, a |-> 1]`, append(slices.Clone(handOff[:8]),
			"0 2.0 output pass=13 RaftState term=1 lead=1 commit=2 applied=2 flags=0",
			"0 0.0 output pass=14 RaftEntry index=3 term=1 kind=1 change=0",
			"0 0.0 output pass=14 RaftState term=1 lead=1 commit=2 applied=2 flags=1",
			"0 2.0 output pass=15 RaftEntry index=3 term=1 kind=1 change=0",
			"0 2.0 output pass=15 RaftState term=1 lead=1 commit=2 applied=2 flags=0",
			"0 2.0 event pass=16 Crash",
			"0 2.0 event pass=17 Restart",
			"0 2.0 output pass=18 RaftState term=1 lead=0 commit=2 applied=2 flags=0",
			"0 2.0 output pass=19 RaftState term=1 lead=0 commit=2 applied=2 flags=4",
		)},
		{"two restarts", `[k |-> "halt", r |-> 3, a |-> 1]`, append(slices.Clone(handOff[:8]),
			"0 2.0 output pass=13 RaftState term=1 lead=1 commit=2 applied=2 flags=0",
			"0 2.0 event pass=14 Crash",
			"0 2.0 event pass=15 Restart",
			"0 2.0 output pass=16 RaftState term=1 lead=0 commit=2 applied=2 flags=0",
			"0 2.0 event pass=17 Crash",
			"0 2.0 event pass=18 Restart",
			"0 2.0 output pass=19 RaftState term=1 lead=0 commit=2 applied=2 flags=0",
			"0 2.0 output pass=20 RaftState term=1 lead=0 commit=2 applied=2 flags=4",
		)},
		{"restart below the apply", `[k |-> "halt", r |-> 3, a |-> 0]`, append(slices.Clone(handOff[:8]),
			"0 2.0 output pass=13 RaftState term=1 lead=1 commit=2 applied=2 flags=0",
			"0 2.0 event pass=16 Crash",
			"0 2.0 event pass=17 Restart",
			"0 2.0 output pass=18 RaftState term=1 lead=0 commit=2 applied=1 flags=0",
			"0 2.0 output pass=19 RaftState term=1 lead=0 commit=2 applied=1 flags=4",
		)},
	} {
		ls := data(t, c.recs...)
		if slices.Index(ls, c.halt) != len(ls)-1 {
			t.Errorf("%s: the halt is not the last line:\n%s", c.name, strings.Join(ls, "\n"))
		}
	}
}
