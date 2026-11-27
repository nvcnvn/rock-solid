package main

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// steps is the Trace lines votersdata writes for the trace records recs, one per string.
func steps(t *testing.T, recs ...string) []string {
	t.Helper()
	var rs []trace.Record
	for i, s := range recs {
		f := strings.SplitN(s, " ", 4)
		r, err := trace.Parse(fmt.Appendf(nil, "%s %s %s pass=%d %s", f[0], f[1], f[2], i+1, f[3]))
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
