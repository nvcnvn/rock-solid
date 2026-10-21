package metamorphic

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// recorder is an engine that records the calls Run makes, and panics at an op named "boom".
type recorder struct {
	calls []string
	views int
}

func (r *recorder) Do(o Op) string {
	if o.Kind == "boom" {
		panic("boom")
	}
	r.calls = append(r.calls, o.String())
	if o.Kind == "get" {
		return "=" + o.Args[0]
	}
	return "ok"
}

func (r *recorder) Writes(o Op) bool { return o.Kind == "put" }
func (r *recorder) Views() int       { return r.views }

func (r *recorder) Flush(between func()) error {
	r.calls = append(r.calls, "flush{")
	between()
	between()
	r.calls = append(r.calls, "}")
	return nil
}

func (r *recorder) FailFlush(n int) error {
	r.calls = append(r.calls, fmt.Sprintf("fail %d", n))
	return nil
}

func (r *recorder) Compact() (bool, error) {
	r.calls = append(r.calls, "compact")
	return false, nil
}

func (r *recorder) CompactStart() { r.calls = append(r.calls, "start") }

func (r *recorder) CompactFinish() error {
	r.calls = append(r.calls, "finish")
	return nil
}

func (r *recorder) Restart() error {
	r.calls = append(r.calls, "restart")
	return nil
}

func ops(t *testing.T, text string) []Op {
	t.Helper()
	o, err := Parse(text, func(Op) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// The cadence options decide the structural calls: without FlushOps the flush, flushfail and
// compact ops do nothing; FlushEvery flushes after every n writes; Absorb runs the writes after a
// flush op between its pieces; Deferred holds a merge picked at a compact op until the next
// flush's first piece; FailFirst fails each flush once first; RestartEach restarts after each
// flush while no view is open; Eager compacts after every flush.
func TestRunCadence(t *testing.T) {
	text := "put a 1\nflush\nput b 2\nput c 3\nput d 4\ncompact\nflushfail 2\nget a\nflush\nrestart\n"
	for _, c := range []struct {
		name  string
		o     Options
		views int
		want  string
	}{
		{"memtable only", Options{}, 0,
			"put a 1|put b 2|put c 3|put d 4|get a|restart"},
		{"flush every 2 writes", Options{FlushOps: true, FlushEvery: 2}, 0,
			"put a 1|flush{|}|put b 2|flush{|}|put c 3|put d 4|flush{|}|fail 2|get a|flush{|}|restart"},
		{"absorb", Options{FlushOps: true, Absorb: 2}, 0,
			"put a 1|flush{|put b 2|put c 3|}|put d 4|fail 2|get a|flush{|}|restart"},
		{"deferred", Options{FlushOps: true, Compact: CompactDeferred}, 0,
			"put a 1|flush{|}|put b 2|put c 3|put d 4|start|fail 2|get a|flush{|finish|}|restart"},
		{"fail first, restart each, eager", Options{FlushOps: true, FailFirst: 1, RestartEach: true, Compact: CompactEager}, 0,
			"put a 1|fail 1|flush{|}|restart|compact|put b 2|put c 3|put d 4|fail 2|get a|fail 1|flush{|}|restart|compact|restart"},
		{"no restart with a view open", Options{FlushOps: true, RestartEach: true}, 1,
			"put a 1|flush{|}|put b 2|put c 3|put d 4|fail 2|get a|flush{|}|restart"},
		{"compact at ops", Options{FlushOps: true, Compact: CompactAtOps}, 0,
			"put a 1|flush{|}|put b 2|put c 3|put d 4|compact|fail 2|get a|flush{|}|restart"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &recorder{views: c.views}
			hist := Run(ops(t, text), c.o, r)
			if got := strings.Join(r.calls, "|"); got != c.want {
				t.Fatalf("calls\n got %s\nwant %s", got, c.want)
			}
			if hist[7] != "=a" {
				t.Fatalf("get a: %q", hist[7])
			}
		})
	}
}

// Absorb runs reads as well as writes between a flush's pieces, and stops at a structural op.
func TestRunAbsorbsReads(t *testing.T) {
	r := &recorder{}
	hist := Run(ops(t, "flush\nget a\nput b 2\nrestart\nget c\n"), Options{FlushOps: true, Absorb: 4}, r)
	if got, want := strings.Join(r.calls, "|"), "flush{|get a|put b 2|}|restart|get c"; got != want {
		t.Fatalf("calls\n got %s\nwant %s", got, want)
	}
	if hist[1] != "=a" || hist[4] != "=c" {
		t.Fatalf("history %q", hist)
	}
}

// A panic ends the history: the op that panicked holds its message and every later op "not run".
func TestRunRecordsAPanic(t *testing.T) {
	hist := Run(ops(t, "put a 1\nboom\nget a\n"), Options{}, &recorder{})
	if want := []string{"ok", "PANIC boom", "not run"}; !slices.Equal(hist, want) {
		t.Fatalf("history %q, want %q", hist, want)
	}
}

func TestParse(t *testing.T) {
	bad := errors.New("bad")
	check := func(o Op) error {
		if o.Kind == "frob" {
			return bad
		}
		return nil
	}
	got, err := Parse("# a comment\n\n  put a 1  \nflushfail 3\n", check)
	if err != nil || len(got) != 2 || got[0].String() != "put a 1" || got[1].Int(0) != 3 {
		t.Fatalf("%v %v", got, err)
	}
	for _, text := range []string{"flush 1", "compact x", "restart now", "flushfail", "flushfail 0", "flushfail x", "flushfail 1 2", "put a\nfrob"} {
		_, err := Parse(text, check)
		if err == nil {
			t.Fatalf("%q parsed", text)
		}
		line := strings.Count(text, "\n") + 1
		if !strings.Contains(err.Error(), fmt.Sprintf("line %d ", line)) {
			t.Fatalf("%q: %v does not name line %d", text, err, line)
		}
	}
	if _, err := Parse("frob", check); !errors.Is(err, bad) {
		t.Fatalf("the check's error is lost: %v", err)
	}
	if err := Ints(Op{Kind: "x", Args: []string{"1", "-2"}}, 2); err == nil {
		t.Fatal("a negative count passed")
	}
}

// Reduce keeps removing ops while the failure persists: the fewest ops holding both "a" and "b"
// in order is the two of them.
func TestReduce(t *testing.T) {
	in := ops(t, "x\na\ny\nz\nb\nw\n")
	fails := func(c []Op) bool {
		i := slices.IndexFunc(c, func(o Op) bool { return o.Kind == "a" })
		return i >= 0 && slices.ContainsFunc(c[i:], func(o Op) bool { return o.Kind == "b" })
	}
	got := Reduce(in, fails)
	if len(got) != 2 || got[0].Kind != "a" || got[1].Kind != "b" {
		t.Fatalf("reduced to %v", got)
	}
}

func TestDiffAndFormat(t *testing.T) {
	if Diff([]string{"a", "b"}, []string{"a", "b"}) != -1 || Diff([]string{"a", "b"}, []string{"a", "c"}) != 1 || Diff([]string{"a"}, []string{"a", "b"}) != 1 {
		t.Fatal("Diff")
	}
	got := Format(ops(t, "put a 1\nget a\n"), []string{"ok", "=1"})
	if got != "get a: =1\n" {
		t.Fatalf("Format: %q", got)
	}
}

// fakeTree is a tree whose flush writes pieces pieces, the last a root, and whose CheckpointDone
// reports the pieces of the previous checkpoint dead.
type fakeTree struct {
	pieces, next int
	frozen       bool
	failed       int
	carried      []disklog.BlobRef
	written      []disklog.BlobRef
	dead         []disklog.BlobRef
}

func (f *fakeTree) BeginFlush() (bool, error) {
	froze := !f.frozen
	f.frozen, f.next, f.written = true, 0, nil
	return froze, nil
}

func (f *fakeTree) Frozen() bool                   { return f.frozen }
func (f *fakeTree) CarriedRuns() []disklog.BlobRef { return f.carried }

func (f *fakeTree) NextPiece(buf []byte) (int, bool, error) {
	if f.next == f.pieces {
		return 0, false, nil
	}
	f.next++
	buf[0] = byte(f.next)
	return 1, f.next == f.pieces, nil
}

func (f *fakeTree) PieceDurable(ref disklog.BlobRef) error {
	f.written = append(f.written, ref)
	return nil
}

func (f *fakeTree) CheckpointDone() ([]disklog.BlobRef, error) {
	dead := f.dead
	f.dead, f.frozen = f.written, false
	return dead, nil
}

func (f *fakeTree) FlushFailed() []disklog.BlobRef {
	f.failed++
	return f.written
}

// A checkpoint names its roots and its replay point; a failed one keeps the previous manifest and
// replays from the freeze it left; Restore hands over each named run's pieces, and a manifest
// naming a dead piece is an error.
func TestLogCheckpoints(t *testing.T) {
	l := NewLog(16)
	tr := &fakeTree{pieces: 2}
	if err := l.Checkpoint(tr, 5, 0, nil); err != nil {
		t.Fatal(err)
	}
	if l.Replay() != 5 || l.Flushes != 1 {
		t.Fatalf("replay %d, flushes %d", l.Replay(), l.Flushes)
	}
	if err := l.Checkpoint(tr, 9, 1, nil); err != nil {
		t.Fatal(err)
	}
	if l.Replay() != 5 || l.Failed != 1 || tr.failed != 1 {
		t.Fatalf("a failed checkpoint moved the replay point to %d", l.Replay())
	}
	between := 0
	if err := l.Checkpoint(tr, 12, 0, func() { between++ }); err != nil {
		t.Fatal(err)
	}
	if l.Replay() != 9 || between != 2 {
		t.Fatalf("replay %d after a freeze at 9, %d calls between pieces", l.Replay(), between)
	}
	var loaded [][]byte
	refs := func(root []byte, _ []disklog.BlobRef) ([]disklog.BlobRef, error) {
		return tr.written[:1], nil
	}
	err := l.Restore(refs, func(root disklog.BlobRef, pieces [][]byte) error {
		loaded = pieces
		return nil
	})
	if err != nil || len(loaded) != 2 || loaded[0][0] != 1 || loaded[1][0] != 2 {
		t.Fatalf("restored %v, %v", loaded, err)
	}
	stale := func(root []byte, _ []disklog.BlobRef) ([]disklog.BlobRef, error) {
		return []disklog.BlobRef{{Segment: 9}}, nil
	}
	if err := l.Restore(stale, func(disklog.BlobRef, [][]byte) error { return nil }); err == nil {
		t.Fatal("a manifest naming a dead piece restored")
	}
}
