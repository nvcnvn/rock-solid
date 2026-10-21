// Package metamorphic runs an index's metamorphic test, as Pebble's internal/metamorphic does: one
// ops file runs under option sets that push each internal choice to an extreme, and the history of
// what every op returned must be the same under each set and under a model.
package metamorphic

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Op is one line of an ops file: a kind and its arguments.
type Op struct {
	Kind string
	Args []string
}

// String is the op's line in an ops file.
func (o Op) String() string {
	if len(o.Args) == 0 {
		return o.Kind
	}
	return o.Kind + " " + strings.Join(o.Args, " ")
}

// Int is argument i as an integer.
func (o Op) Int(i int) int {
	n, _ := strconv.Atoi(o.Args[i])
	return n
}

// Uint is argument i as an unsigned integer.
func (o Op) Uint(i int) uint64 {
	n, _ := strconv.ParseUint(o.Args[i], 10, 64)
	return n
}

// The structural ops, which an engine must run without changing what any later op returns.
const (
	Flush     = "flush"
	FlushFail = "flushfail"
	Compact   = "compact"
	Restart   = "restart"
)

// Parse reads ops one per line, skipping blank lines and lines starting with #; check validates
// every op that is not structural, and a failure names the line.
func Parse(text string, check func(Op) error) ([]Op, error) {
	var ops []Op
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		f := strings.Fields(line)
		o := Op{Kind: f[0], Args: f[1:]}
		var err error
		switch o.Kind {
		case Flush, Compact, Restart:
			if len(o.Args) != 0 {
				err = fmt.Errorf("%s takes no arguments", o.Kind)
			}
		case FlushFail:
			if n, e := strconv.Atoi(strings.Join(o.Args, " ")); e != nil || n < 1 {
				err = fmt.Errorf("%s takes a piece count of at least 1", o.Kind)
			}
		default:
			err = check(o)
		}
		if err != nil {
			return nil, fmt.Errorf("line %d %q: %w", i+1, line, err)
		}
		ops = append(ops, o)
	}
	return ops, nil
}

// Ints checks that o has exactly n arguments, each a non-negative integer.
func Ints(o Op, n int) error {
	if len(o.Args) != n {
		return fmt.Errorf("%s takes %d arguments", o.Kind, n)
	}
	for _, a := range o.Args {
		if _, err := strconv.ParseUint(a, 10, 64); err != nil {
			return fmt.Errorf("%s: %q is not a count", o.Kind, a)
		}
	}
	return nil
}

// Engine is an index under test, or its model, for which every structural op does nothing.
type Engine interface {
	// Do runs an op that is not structural and returns its history line.
	Do(op Op) string
	// Writes reports whether op changes the index, so that it may run between a flush's pieces.
	Writes(op Op) bool
	// Views is how many point-in-time views are open, which a restart would close.
	Views() int
	// Flush runs a whole checkpoint, calling between once each piece is built.
	Flush(between func()) error
	// FailFlush begins a checkpoint that fails once pieces pieces are durable.
	FailFlush(pieces int) error
	// Compact runs and installs the merges due now, reporting whether there were any.
	Compact() (bool, error)
	// CompactStart picks a merge and holds it; CompactFinish runs and installs it.
	CompactStart()
	CompactFinish() error
	// Restart drops everything not durable and recovers from the newest manifest plus replay.
	Restart() error
}

// Compaction is when an option set compacts.
type Compaction uint8

// When an option set compacts.
const (
	// CompactNever ignores compact ops.
	CompactNever Compaction = iota
	// CompactAtOps compacts at each compact op.
	CompactAtOps
	// CompactEager compacts after every flush until no merge is due.
	CompactEager
	// CompactDeferred picks a merge at each compact op and installs it between the pieces of the next flush, or at the next compact op if that comes first.
	CompactDeferred
)

// Options is the cadence an option set runs the structural ops at.
type Options struct {
	// FlushOps runs the generated flush, flushfail and compact ops; without it the index stays in its memtable.
	FlushOps bool
	// FlushEvery also flushes after every n writes.
	FlushEvery int
	// Compact is when merges run.
	Compact Compaction
	// CompactEvery, with CompactNever, compacts until no merge is due after every n flushes.
	CompactEvery int
	// Absorb runs up to this many ops that follow a flush op, writes and reads, between its pieces.
	Absorb int
	// FailFirst makes every flush first fail after this many pieces, then run again.
	FailFirst int
	// RestartEach restarts after every flush that finds no view open.
	RestartEach bool
}

// Run executes ops on e under o and returns one history line per op; a panic ends the history
// with its message.
func Run(ops []Op, o Options, e Engine) (hist []string) {
	r := runner{ops: ops, o: o, e: e, hist: make([]string, len(ops)), done: make([]bool, len(ops))}
	defer func() {
		if p := recover(); p != nil {
			i := min(r.at, len(ops)-1)
			r.hist[i] = fmt.Sprintf("PANIC %v", p)
			for j := i + 1; j < len(ops); j++ {
				r.hist[j] = "not run"
			}
			hist = r.hist
		}
	}()
	r.run()
	return r.hist
}

type runner struct {
	ops     []Op
	o       Options
	e       Engine
	hist    []string
	done    []bool
	at      int
	writes  int
	flushes int
	pending bool
}

func errLine(err error) string {
	if err != nil {
		return "ERR " + err.Error()
	}
	return "ok"
}

func (r *runner) run() {
	for r.at = 0; r.at < len(r.ops); r.at++ {
		if r.done[r.at] {
			continue
		}
		op := r.ops[r.at]
		switch op.Kind {
		case Flush:
			r.hist[r.at] = "ok"
			if r.o.FlushOps {
				r.hist[r.at] = r.flush(r.at, r.o.Absorb > 0)
			}
		case FlushFail:
			r.hist[r.at] = "ok"
			if r.o.FlushOps {
				r.hist[r.at] = errLine(r.e.FailFlush(op.Int(0)))
			}
		case Compact:
			r.hist[r.at] = "ok"
			if r.o.FlushOps {
				r.compactOp()
			}
		case Restart:
			r.pending = false
			r.hist[r.at] = errLine(r.e.Restart())
		default:
			if !r.e.Writes(op) {
				r.hist[r.at] = r.e.Do(op)
				continue
			}
			r.write(r.at)
			if r.o.FlushEvery > 0 && r.writes%r.o.FlushEvery == 0 {
				if l := r.flush(r.at, false); l != "ok" {
					r.hist[r.at] = l
				}
			}
		}
	}
}

func (r *runner) write(i int) {
	r.hist[i], r.done[i] = r.e.Do(r.ops[i]), true
	r.writes++
}

func (r *runner) compactOp() {
	switch r.o.Compact {
	case CompactAtOps:
		_, err := r.e.Compact()
		r.hist[r.at] = errLine(err)
	case CompactDeferred:
		if r.pending {
			r.hist[r.at] = errLine(r.e.CompactFinish())
		}
		r.e.CompactStart()
		r.pending = true
	}
}

// finishPending installs a merge held across a flush, reporting a failure on line.
func (r *runner) finishPending(line *string) {
	if !r.pending {
		return
	}
	r.pending = false
	if err := r.e.CompactFinish(); err != nil {
		*line = errLine(err)
	}
}

// flush runs a checkpoint for the op at at; with absorb, the ops that follow it run between its
// pieces until a structural op.
func (r *runner) flush(at int, absorb bool) string {
	line := "ok"
	if r.o.FailFirst > 0 {
		if err := r.e.FailFlush(r.o.FailFirst); err != nil {
			return errLine(err)
		}
	}
	next := at + 1
	err := r.e.Flush(func() {
		r.finishPending(&line)
		if absorb && next < len(r.ops) && next-at <= r.o.Absorb && !structural(r.ops[next]) {
			if r.e.Writes(r.ops[next]) {
				r.write(next)
			} else {
				r.hist[next], r.done[next] = r.e.Do(r.ops[next]), true
			}
			next++
		}
	})
	r.finishPending(&line)
	if err != nil {
		return errLine(err)
	}
	if r.o.RestartEach && r.e.Views() == 0 {
		if err := r.e.Restart(); err != nil {
			return errLine(err)
		}
	}
	r.flushes++
	if r.o.Compact == CompactEager || r.o.CompactEvery > 0 && r.flushes%r.o.CompactEvery == 0 {
		for {
			ok, err := r.e.Compact()
			if err != nil {
				return errLine(err)
			}
			if !ok {
				break
			}
		}
	}
	return line
}

func structural(o Op) bool {
	switch o.Kind {
	case Flush, FlushFail, Compact, Restart:
		return true
	}
	return false
}

// Diff returns the first op whose history lines differ, or -1.
func Diff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return min(len(a), len(b))
	}
	return -1
}

// Reduce removes ops while fails still reports a failure, halving the chunk it tries to remove
// down to single ops and repeating until nothing more goes, as Pebble's --try-to-reduce does.
func Reduce(ops []Op, fails func([]Op) bool) []Op {
	cur := slices.Clone(ops)
	for {
		before := len(cur)
		for chunk := max(len(cur)/2, 1); chunk >= 1; chunk /= 2 {
			for start := 0; start < len(cur); {
				end := min(start+chunk, len(cur))
				cand := slices.Concat(cur[:start], cur[end:])
				if len(cand) > 0 && fails(cand) {
					cur = cand
					continue
				}
				start = end
			}
		}
		if len(cur) == before {
			return cur
		}
	}
}

// Format writes each op whose history line is not "ok" followed by that line, one per line.
func Format(ops []Op, hist []string) string {
	var b strings.Builder
	for i, op := range ops {
		if hist[i] != "ok" {
			b.WriteString(op.String() + ": " + hist[i] + "\n")
		}
	}
	return b.String()
}
