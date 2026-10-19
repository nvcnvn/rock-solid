// Package meta is a small metamorphic test harness in the style of Pebble's
// metamorphic package: a seed generates one sequence of operations, every
// option set runs the same sequence, and the histories of what each read
// returned must be identical. Flushes, compactions, failed checkpoints and
// restarts are operations that must never change a read's result.
package meta

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
)

// Op is one generated operation; its text form is the line in an ops file.
type Op struct {
	Kind string
	Key  string
	Val  string
	N    int
}

func (o Op) String() string {
	switch o.Kind {
	case "put":
		return "put " + o.Key + " " + o.Val
	case "del", "get", "floor":
		return o.Kind + " " + o.Key
	case "scan":
		return fmt.Sprintf("scan %s %d", o.Key, o.N)
	case "vopen", "vclose":
		return fmt.Sprintf("%s %d", o.Kind, o.N)
	case "vget":
		return fmt.Sprintf("vget %d %s", o.N, o.Key)
	case "vscan":
		return fmt.Sprintf("vscan %d %s", o.N, o.Key)
	case "flushfail":
		return fmt.Sprintf("flushfail %d", o.N)
	}
	return o.Kind
}

// Parse reads ops in the text form String writes, one per line; # starts a comment.
func Parse(text string) ([]Op, error) {
	var ops []Op
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		f := strings.Fields(line)
		o := Op{Kind: f[0]}
		atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
		switch {
		case o.Kind == "put" && len(f) == 3:
			o.Key, o.Val = f[1], f[2]
		case (o.Kind == "del" || o.Kind == "get" || o.Kind == "floor") && len(f) == 2:
			o.Key = f[1]
		case o.Kind == "scan" && len(f) == 3:
			o.Key, o.N = f[1], atoi(f[2])
		case (o.Kind == "vopen" || o.Kind == "vclose" || o.Kind == "flushfail") && len(f) == 2:
			o.N = atoi(f[1])
		case (o.Kind == "vget" || o.Kind == "vscan") && len(f) == 3:
			o.N, o.Key = atoi(f[1]), f[2]
		case (o.Kind == "flush" || o.Kind == "compact" || o.Kind == "restart") && len(f) == 1:
		default:
			return nil, fmt.Errorf("bad op %q", line)
		}
		ops = append(ops, o)
	}
	return ops, nil
}

func (o Op) write() bool { return o.Kind == "put" || o.Kind == "del" }

// Generate makes n ops from seed. Like Pebble's key manager, a key is new a
// quarter of the time and otherwise one already used, so ops collide.
func Generate(seed uint64, n int) []Op {
	r := rand.New(rand.NewPCG(seed, 0x6d657461))
	var keys []string
	newKey := func() string {
		b := make([]byte, 1+r.IntN(6))
		for i := range b {
			b[i] = 'a' + byte(r.IntN(6))
		}
		return string(b)
	}
	key := func(newP int) string {
		if len(keys) == 0 || r.IntN(100) < newP {
			k := newKey()
			keys = append(keys, k)
			return k
		}
		return keys[r.IntN(len(keys))]
	}
	probe := func() string {
		if len(keys) == 0 || r.IntN(10) < 3 {
			return newKey()
		}
		return keys[r.IntN(len(keys))]
	}
	type w struct {
		kind   string
		weight int
	}
	deck := []w{{"put", 35}, {"del", 10}, {"get", 15}, {"floor", 8}, {"scan", 5}, {"vopen", 2},
		{"vget", 5}, {"vscan", 2}, {"vclose", 2}, {"flush", 5}, {"flushfail", 1}, {"compact", 4}, {"restart", 1}}
	total := 0
	for _, d := range deck {
		total += d.weight
	}
	var open []int
	nextView := 1
	var ops []Op
	for len(ops) < n {
		x := r.IntN(total)
		var kind string
		for _, d := range deck {
			if x < d.weight {
				kind = d.kind
				break
			}
			x -= d.weight
		}
		o := Op{Kind: kind}
		switch kind {
		case "put":
			o.Key, o.Val = key(25), "v"+strconv.Itoa(len(ops))+strings.Repeat("x", r.IntN(8))
		case "del":
			o.Key = key(5)
		case "get", "floor":
			o.Key = probe()
		case "scan":
			o.Key, o.N = probe(), 1+r.IntN(8)
		case "vopen":
			if len(open) >= 4 {
				continue
			}
			o.N = nextView
			open = append(open, nextView)
			nextView++
		case "vget", "vscan", "vclose":
			if len(open) == 0 {
				continue
			}
			i := r.IntN(len(open))
			o.N, o.Key = open[i], probe()
			if kind == "vclose" {
				open = slices.Delete(open, i, i+1)
			}
		case "flushfail":
			o.N = 1 + r.IntN(3)
		case "restart":
			open = nil
		}
		ops = append(ops, o)
	}
	return ops
}

// Engine is a tree under test, or the model.
type Engine interface {
	Put(k, v string) error
	Delete(k string) error
	Get(k string) (string, bool)
	Floor(k string) (string, string, bool)
	Scan(lo string, n int) []string
	ViewOpen(id int)
	ViewGet(id int, k string) (string, bool, bool)
	ViewScan(id int, lo string, n int) ([]string, bool)
	ViewClose(id int)
	// Flush runs a whole checkpoint, calling between after each durable piece.
	Flush(between func()) error
	// FailFlush begins a checkpoint that fails after pieces durable pieces.
	FailFlush(pieces int) error
	// CompactNow picks, runs and installs one compaction, reporting whether one was due.
	CompactNow() (bool, error)
	// CompactStart picks a compaction and holds it; CompactFinish runs and installs it.
	CompactStart()
	CompactFinish() error
	// Restart drops everything not durable and recovers from the newest manifest plus replay.
	Restart() error
	Stats() string
}

// Options is one configuration the ops run under.
type Options struct {
	Name         string
	Piece        int
	FlushOps     bool   // run the generated flush, flushfail and compact ops
	FlushEvery   int    // also flush after every n writes
	Compact      string // none, ops, eager (after every flush, until none is due), deferred (installed during the next flush)
	Absorb       int    // up to this many writes following a flush op run between its pieces
	FailFirst    int    // every flush first fails after this many pieces, then runs again
	RestartEach  bool   // restart after every flush
	CompactEvery int    // with Compact none: compact until none is due after every n flushes
}

// Absorbed counts writes run between a flush's pieces, over every Run.
var Absorbed int

// Run executes ops under o and returns one history line per op.
func Run(ops []Op, o Options, e Engine) (hist []string) {
	hist = make([]string, len(ops))
	done := make([]bool, len(ops))
	i := 0
	defer func() {
		if p := recover(); p != nil {
			hist[min(i, len(ops)-1)] = fmt.Sprintf("PANIC %v", p)
			for j := i + 1; j < len(ops); j++ {
				hist[j] = "not run"
			}
		}
	}()
	errLine := func(err error) string {
		if err != nil {
			return "ERR " + err.Error()
		}
		return "ok"
	}
	writes := 0
	pending := false
	views := map[int]bool{}
	flushes := 0
	write := func(j int) {
		op := ops[j]
		var err error
		if op.Kind == "put" {
			err = e.Put(op.Key, op.Val)
		} else {
			err = e.Delete(op.Key)
		}
		hist[j], done[j] = errLine(err), true
		writes++
	}
	var flush func(at int, absorb bool) string
	flush = func(at int, absorb bool) string {
		var line string
		if o.FailFirst > 0 {
			if err := e.FailFlush(o.FailFirst); err != nil {
				return "ERR " + err.Error()
			}
		}
		next := at + 1
		err := e.Flush(func() {
			if pending {
				pending = false
				if err := e.CompactFinish(); err != nil {
					line = "ERR " + err.Error()
				}
			}
			if absorb && next < len(ops) && next-at <= o.Absorb && ops[next].write() {
				write(next)
				next++
				Absorbed++
			}
		})
		if pending {
			pending = false
			if err := e.CompactFinish(); err != nil {
				line = "ERR " + err.Error()
			}
		}
		if err != nil {
			return "ERR " + err.Error()
		}
		if o.RestartEach && len(views) == 0 {
			if err := e.Restart(); err != nil {
				return "ERR " + err.Error()
			}
		}
		flushes++
		if o.Compact == "eager" || o.CompactEvery > 0 && flushes%o.CompactEvery == 0 {
			for {
				ok, err := e.CompactNow()
				if err != nil {
					return "ERR " + err.Error()
				}
				if !ok {
					break
				}
			}
		}
		if line == "" {
			line = "ok"
		}
		return line
	}
	for i = 0; i < len(ops); i++ {
		if done[i] {
			continue
		}
		op := ops[i]
		switch op.Kind {
		case "put", "del":
			write(i)
			if o.FlushEvery > 0 && writes%o.FlushEvery == 0 {
				if l := flush(i, false); l != "ok" {
					hist[i] = l
				}
			}
		case "get":
			v, ok := e.Get(op.Key)
			hist[i] = fmtGet(v, ok)
		case "floor":
			k, v, ok := e.Floor(op.Key)
			if ok {
				hist[i] = k + "=" + v
			} else {
				hist[i] = "none"
			}
		case "scan":
			hist[i] = "[" + strings.Join(e.Scan(op.Key, op.N), " ") + "]"
		case "vopen":
			e.ViewOpen(op.N)
			views[op.N] = true
			hist[i] = "ok"
		case "vget":
			v, ok, exists := e.ViewGet(op.N, op.Key)
			if !exists {
				hist[i] = "no view"
			} else {
				hist[i] = fmtGet(v, ok)
			}
		case "vscan":
			s, exists := e.ViewScan(op.N, op.Key, 4)
			if !exists {
				hist[i] = "no view"
			} else {
				hist[i] = "[" + strings.Join(s, " ") + "]"
			}
		case "vclose":
			e.ViewClose(op.N)
			delete(views, op.N)
			hist[i] = "ok"
		case "flush":
			hist[i] = "ok"
			if o.FlushOps {
				hist[i] = flush(i, o.Absorb > 0)
			}
		case "flushfail":
			hist[i] = "ok"
			if o.FlushOps {
				hist[i] = errLine(e.FailFlush(op.N))
			}
		case "compact":
			hist[i] = "ok"
			if !o.FlushOps {
				break
			}
			switch o.Compact {
			case "ops":
				_, err := e.CompactNow()
				hist[i] = errLine(err)
			case "deferred":
				if pending {
					if err := e.CompactFinish(); err != nil {
						hist[i] = "ERR " + err.Error()
					}
				}
				e.CompactStart()
				pending = true
			}
		case "restart":
			pending = false
			clear(views)
			hist[i] = errLine(e.Restart())
		}
	}
	return hist
}

func fmtGet(v string, ok bool) string {
	if ok {
		return "=" + v
	}
	return "miss"
}

// Diff returns the first op whose history line differs, or -1.
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

// Reduce removes ops while fails still reports a failure, halving the chunk it
// tries to remove down to single ops and repeating until nothing more goes, as
// Pebble's --try-to-reduce does.
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

// Model is the reference: a map, where flushes, compactions and restarts do nothing.
type Model struct {
	m     map[string]string
	views map[int]map[string]string
}

// NewModel makes an empty model.
func NewModel() *Model { return &Model{m: map[string]string{}, views: map[int]map[string]string{}} }

func (m *Model) Put(k, v string) error       { m.m[k] = v; return nil }
func (m *Model) Delete(k string) error       { delete(m.m, k); return nil }
func (m *Model) Get(k string) (string, bool) { v, ok := m.m[k]; return v, ok }

func (m *Model) Floor(k string) (string, string, bool) { return floor(m.m, k) }

func floor(m map[string]string, k string) (string, string, bool) {
	best, found := "", false
	for key := range m {
		if key <= k && (!found || key > best) {
			best, found = key, true
		}
	}
	return best, m[best], found
}

func scan(m map[string]string, lo string, n int) []string {
	var keys []string
	for k := range m {
		if k >= lo {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := []string{}
	for _, k := range keys[:min(n, len(keys))] {
		out = append(out, k+"="+m[k])
	}
	return out
}

func (m *Model) Scan(lo string, n int) []string { return scan(m.m, lo, n) }
func (m *Model) ViewOpen(id int) {
	c := make(map[string]string, len(m.m))
	for k, v := range m.m {
		c[k] = v
	}
	m.views[id] = c
}
func (m *Model) ViewGet(id int, k string) (string, bool, bool) {
	vm, ok := m.views[id]
	if !ok {
		return "", false, false
	}
	v, ok := vm[k]
	return v, ok, true
}
func (m *Model) ViewScan(id int, lo string, n int) ([]string, bool) {
	vm, ok := m.views[id]
	if !ok {
		return nil, false
	}
	return scan(vm, lo, n), true
}
func (m *Model) ViewClose(id int)           { delete(m.views, id) }
func (m *Model) Flush(between func()) error { return nil }
func (m *Model) FailFlush(int) error        { return nil }
func (m *Model) CompactNow() (bool, error)  { return false, nil }
func (m *Model) CompactStart()              {}
func (m *Model) CompactFinish() error       { return nil }
func (m *Model) Restart() error             { clear(m.views); return nil }
func (m *Model) Stats() string              { return "model" }
