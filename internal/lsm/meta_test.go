package lsm

import (
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/nvcnvn/rock-solid/internal/sim/metamorphic"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var metaOps = flag.Int("meta.ops", 3000, "ops per metamorphic seed")

// metaSet is an option set: the tree's settings and the cadence of its structural ops.
type metaSet struct {
	name     string
	piece    int
	runLimit int
	step     int
	merges   int
	linear   bool
	points   bool
	probe    FilterProbe
	run      metamorphic.Options
}

// metaSets push one internal choice each to its extreme (docs/test-scenarios.md, Metamorphic
// index tests); F, the memtable alone, comes first.
var metaSets = []metaSet{
	{name: "F", piece: 1 << 20},
	{name: "A", piece: 1 << 20, run: metamorphic.Options{FlushOps: true, Compact: metamorphic.CompactAtOps}},
	{name: "B", piece: 4096, run: metamorphic.Options{FlushOps: true, FlushEvery: 40, Compact: metamorphic.CompactEager}},
	{name: "C", piece: 256, run: metamorphic.Options{FlushOps: true, FlushEvery: 10}},
	{name: "D", piece: 512, run: metamorphic.Options{FlushOps: true, FlushEvery: 25, Compact: metamorphic.CompactDeferred, Absorb: 4}},
	{name: "E", piece: 1024, run: metamorphic.Options{FlushOps: true, FlushEvery: 30, Compact: metamorphic.CompactEager, FailFirst: 1, RestartEach: true}},
	{name: "G", piece: 4096, run: metamorphic.Options{FlushOps: true, FlushEvery: 2, CompactEvery: 40}},
	{name: "two-merges", piece: 1024, merges: 2, run: metamorphic.Options{FlushOps: true, FlushEvery: 3, CompactEvery: 8}},
	{name: "fixed-limit", piece: 1024, runLimit: 4, run: metamorphic.Options{FlushOps: true, FlushEvery: 5, Compact: metamorphic.CompactEager}},
	{name: "point-removals", piece: 4096, points: true, run: metamorphic.Options{FlushOps: true, FlushEvery: 20, Compact: metamorphic.CompactEager}},
	{name: "linear-merge", piece: 1024, linear: true, run: metamorphic.Options{FlushOps: true, FlushEvery: 15, Compact: metamorphic.CompactEager, Absorb: 4}},
	{name: "steps", piece: 1024, step: 1024, run: metamorphic.Options{FlushOps: true, FlushEvery: 10, Compact: metamorphic.CompactEager}},
	{name: "steps-restart", piece: 768, step: 768, run: metamorphic.Options{FlushOps: true, FlushEvery: 20, Compact: metamorphic.CompactEager, FailFirst: 1, RestartEach: true}},
	{name: "steps-deferred", piece: 1024, step: 1024, merges: 2, run: metamorphic.Options{FlushOps: true, FlushEvery: 8, Compact: metamorphic.CompactDeferred, Absorb: 4, CompactEvery: 3}},
	{name: "filters-off", piece: 256, probe: FiltersOff, run: metamorphic.Options{FlushOps: true, FlushEvery: 10}},
	{name: "filters-maybe", piece: 256, probe: FiltersMaybe, run: metamorphic.Options{FlushOps: true, FlushEvery: 10}},
}

func metaSetNamed(name string) (metaSet, bool) {
	i := slices.IndexFunc(metaSets, func(s metaSet) bool { return s.name == name })
	if i < 0 {
		return metaSet{}, false
	}
	return metaSets[i], true
}

func checkMetaOp(o metamorphic.Op) error {
	want := map[string]int{"put": 2, "del": 1, "delrange": 2, "get": 1, "floor": 1, "scan": 2, "vopen": 1, "vget": 2, "vscan": 2, "vclose": 1}
	n, ok := want[o.Kind]
	if !ok {
		return fmt.Errorf("unknown op %q", o.Kind)
	}
	if len(o.Args) != n {
		return fmt.Errorf("%s takes %d arguments", o.Kind, n)
	}
	switch o.Kind {
	case "delrange":
		if o.Args[0] >= o.Args[1] {
			return fmt.Errorf("delrange needs its start below its end")
		}
	case "scan", "vopen", "vclose", "vget", "vscan":
		at := 0
		if o.Kind == "scan" {
			at = 1
		}
		if _, err := strconv.Atoi(o.Args[at]); err != nil {
			return fmt.Errorf("%s: %q is not a number", o.Kind, o.Args[at])
		}
	}
	return nil
}

// genMetaOps draws n ops from seed by weight, as the metamorphic testing spike's generator; a
// key is new a quarter of the time and otherwise one already used, so ops collide.
func genMetaOps(seed uint64, n int) []metamorphic.Op {
	r := rand.New(rand.NewPCG(seed, 0x6d657461))
	var keys []string
	newKey := func() string {
		b := make([]byte, 1+r.IntN(6))
		for i := range b {
			b[i] = 'a' + byte(r.IntN(6))
		}
		return string(b)
	}
	key := func(newPercent int) string {
		if len(keys) == 0 || r.IntN(100) < newPercent {
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
	deck := []struct {
		kind   string
		weight int
	}{{"put", 35}, {"del", 10}, {"delrange", 3}, {"get", 15}, {"floor", 8}, {"scan", 5}, {"vopen", 2},
		{"vget", 5}, {"vscan", 2}, {"vclose", 2}, {"flush", 5}, {"flushfail", 1}, {"compact", 4}, {"restart", 1}}
	total := 0
	for _, d := range deck {
		total += d.weight
	}
	var open []int
	nextView := 1
	ops := make([]metamorphic.Op, 0, n)
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
		o := metamorphic.Op{Kind: kind}
		switch kind {
		case "put":
			o.Args = []string{key(25), "v" + strconv.Itoa(len(ops)) + strings.Repeat("x", r.IntN(8))}
		case "del":
			o.Args = []string{key(5)}
		case "delrange":
			lo, hi := probe(), probe()
			if r.IntN(2) == 0 {
				hi = lo[:len(lo)-1] + string(lo[len(lo)-1]+1)
			}
			if lo == hi {
				continue
			}
			o.Args = []string{min(lo, hi), max(lo, hi)}
		case "get", "floor":
			o.Args = []string{probe()}
		case "scan":
			o.Args = []string{probe(), strconv.Itoa(1 + r.IntN(8))}
		case "vopen":
			if len(open) >= 4 {
				continue
			}
			o.Args = []string{strconv.Itoa(nextView)}
			open = append(open, nextView)
			nextView++
		case "vget", "vscan", "vclose":
			if len(open) == 0 {
				continue
			}
			i := r.IntN(len(open))
			o.Args = []string{strconv.Itoa(open[i])}
			if kind == "vclose" {
				open = slices.Delete(open, i, i+1)
			} else {
				o.Args = append(o.Args, probe())
			}
		case "flushfail":
			o.Args = []string{strconv.Itoa(1 + r.IntN(3))}
		case "restart":
			open = nil
		}
		ops = append(ops, o)
	}
	return ops
}

func getLine(v string, ok bool) string {
	if ok {
		return "=" + v
	}
	return "miss"
}

func scanLine(kvs []string) string { return "[" + strings.Join(kvs, " ") + "]" }

// metaModel is the map the tree must read as; the structural ops do nothing to it.
type metaModel struct {
	m     map[string]string
	views map[string]map[string]string
}

func newMetaModel() *metaModel {
	return &metaModel{m: map[string]string{}, views: map[string]map[string]string{}}
}

func modelScan(m map[string]string, lo string, n int) []string {
	out := []string{}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if k >= lo && len(out) < n {
			out = append(out, k+"="+m[k])
		}
	}
	return out
}

func (m *metaModel) Do(o metamorphic.Op) string {
	switch o.Kind {
	case "put":
		m.m[o.Args[0]] = o.Args[1]
	case "del":
		delete(m.m, o.Args[0])
	case "delrange":
		maps.DeleteFunc(m.m, func(k, _ string) bool { return k >= o.Args[0] && k < o.Args[1] })
	case "get":
		v, ok := m.m[o.Args[0]]
		return getLine(v, ok)
	case "floor":
		best, found := "", false
		for k := range m.m {
			if k <= o.Args[0] && (!found || k > best) {
				best, found = k, true
			}
		}
		if !found {
			return "none"
		}
		return best + "=" + m.m[best]
	case "scan":
		return scanLine(modelScan(m.m, o.Args[0], o.Int(1)))
	case "vopen":
		m.views[o.Args[0]] = maps.Clone(m.m)
	case "vget":
		vm, ok := m.views[o.Args[0]]
		if !ok {
			return "no view"
		}
		v, ok := vm[o.Args[1]]
		return getLine(v, ok)
	case "vscan":
		vm, ok := m.views[o.Args[0]]
		if !ok {
			return "no view"
		}
		return scanLine(modelScan(vm, o.Args[1], 4))
	case "vclose":
		delete(m.views, o.Args[0])
	}
	return "ok"
}

func metaWrites(o metamorphic.Op) bool {
	return o.Kind == "put" || o.Kind == "del" || o.Kind == "delrange"
}

func (m *metaModel) Writes(o metamorphic.Op) bool { return metaWrites(o) }
func (m *metaModel) Views() int                   { return len(m.views) }
func (m *metaModel) Flush(func()) error           { return nil }
func (m *metaModel) FailFlush(int) error          { return nil }
func (m *metaModel) Compact() (bool, error)       { return false, nil }
func (m *metaModel) CompactStart()                {}
func (m *metaModel) CompactFinish() error         { return nil }
func (m *metaModel) Restart() error               { clear(m.views); return nil }

// metaWrite is a write in the log a restart replays: a put, a removal or a range removal.
type metaWrite struct {
	op     string
	k, arg string
}

// metaStats counts what an option set made the tree do, to show the set reaches its extreme.
type metaStats struct {
	flushes, failed, compactions, together, deferred, restarts, maxRuns, searches, filterProbes, steps, resumed int
}

// metaTree runs a Tree over a log that keeps every live piece. Its writes stand in for the disk
// log's records: a restart loads the newest manifest's runs and replays the writes after the
// manifest's replay point, the freeze of the memtable it wrote.
type metaTree struct {
	set     metaSet
	check   bool
	plant   plants
	tr      *Tree
	log     *metamorphic.Log
	writes  []metaWrite
	keys    []string
	views   map[string]*View
	pending *Compaction
	st      metaStats
}

func newMetaTree(s metaSet, check bool, plant plants) *metaTree {
	m := &metaTree{set: s, check: check, plant: plant, log: metamorphic.NewLog(s.piece), views: map[string]*View{}}
	m.tr = m.newTree()
	return m
}

func (m *metaTree) newTree() *Tree {
	tr, err := New(Config{MaxPiece: m.set.piece, RunLimit: m.set.runLimit, StepBytes: m.set.step, Check: m.check, Probe: m.set.probe})
	if err != nil {
		panic(err)
	}
	tr.linear, tr.plant = m.set.linear, m.plant
	return tr
}

func (m *metaTree) apply(tr *Tree, w metaWrite) error {
	switch w.op {
	case "put":
		return tr.Put([]byte(w.k), []byte(w.arg))
	case "del":
		return tr.Delete([]byte(w.k))
	}
	return tr.DeleteRange([]byte(w.k), []byte(w.arg))
}

func (m *metaTree) write(w metaWrite) string {
	m.writes = append(m.writes, w)
	if err := m.apply(m.tr, w); err != nil {
		return "ERR " + err.Error()
	}
	return "ok"
}

func iterScan(it *Iter, lo string, n int) []string {
	out := []string{}
	for ok := it.SeekGE([]byte(lo)); ok && len(out) < n; ok = it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	return out
}

func (m *metaTree) Do(o metamorphic.Op) string {
	switch o.Kind {
	case "put":
		if i, found := slices.BinarySearch(m.keys, o.Args[0]); !found {
			m.keys = slices.Insert(m.keys, i, o.Args[0])
		}
		return m.write(metaWrite{op: "put", k: o.Args[0], arg: o.Args[1]})
	case "del":
		return m.write(metaWrite{op: "del", k: o.Args[0]})
	case "delrange":
		if !m.set.points {
			return m.write(metaWrite{op: "delrange", k: o.Args[0], arg: o.Args[1]})
		}
		lo, _ := slices.BinarySearch(m.keys, o.Args[0])
		hi, _ := slices.BinarySearch(m.keys, o.Args[1])
		for _, k := range m.keys[lo:hi] {
			if l := m.write(metaWrite{op: "del", k: k}); l != "ok" {
				return l
			}
		}
		return "ok"
	case "get":
		v, ok := m.tr.Get([]byte(o.Args[0]))
		return getLine(string(v), ok)
	case "floor":
		k, v, ok := m.tr.Floor([]byte(o.Args[0]))
		if !ok {
			return "none"
		}
		return string(k) + "=" + string(v)
	case "scan":
		var it Iter
		m.tr.Iter(&it)
		return scanLine(iterScan(&it, o.Args[0], o.Int(1)))
	case "vopen":
		m.views[o.Args[0]] = m.tr.View()
	case "vget":
		v, ok := m.views[o.Args[0]]
		if !ok {
			return "no view"
		}
		val, found := v.Get([]byte(o.Args[1]))
		return getLine(string(val), found)
	case "vscan":
		v, ok := m.views[o.Args[0]]
		if !ok {
			return "no view"
		}
		var it Iter
		v.Iter(&it)
		return scanLine(iterScan(&it, o.Args[1], 4))
	case "vclose":
		if v, ok := m.views[o.Args[0]]; ok {
			v.Close()
			delete(m.views, o.Args[0])
		}
	}
	return "ok"
}

func (m *metaTree) Writes(o metamorphic.Op) bool { return metaWrites(o) }
func (m *metaTree) Views() int                   { return len(m.views) }

func (m *metaTree) Flush(between func()) error {
	return m.checkpoint(0, between)
}

func (m *metaTree) FailFlush(pieces int) error { return m.checkpoint(pieces, nil) }

func (m *metaTree) checkpoint(limit int, between func()) error {
	if err := m.log.Checkpoint(m.tr, len(m.writes), limit, between); err != nil {
		return err
	}
	m.st.maxRuns = max(m.st.maxRuns, m.tr.Runs())
	return nil
}

func (m *metaTree) install(c *Compaction) error {
	if err := m.tr.Install(c); err != nil {
		return err
	}
	if !c.final {
		m.st.steps++
	}
	m.st.compactions++
	return nil
}

// run runs a merge's next step, counting a merge a restart left between steps.
func (m *metaTree) run(c *Compaction) {
	if !c.started && c.bound != nil {
		m.st.resumed++
	}
	c.Run()
}

// Compact picks as many merges as the set runs at once, runs them all and installs them newest
// last.
func (m *metaTree) Compact() (bool, error) {
	var cs []*Compaction
	for len(cs) < max(m.set.merges, 1) {
		c := m.tr.PickCompaction()
		if c == nil {
			break
		}
		cs = append(cs, c)
	}
	if len(cs) == 0 {
		return false, nil
	}
	if len(cs) > 1 {
		m.st.together++
	}
	for _, c := range cs {
		m.run(c)
	}
	for i := len(cs) - 1; i >= 0; i-- {
		if err := m.install(cs[i]); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (m *metaTree) CompactStart() {
	if m.pending == nil {
		m.pending = m.tr.PickCompaction()
	}
}

// CompactFinish runs the held merge only now, after later writes and flushes, since a merge's Run
// must not depend on the tree it came from.
func (m *metaTree) CompactFinish() error {
	c := m.pending
	m.pending = nil
	if c == nil {
		return nil
	}
	m.run(c)
	if m.tr.Frozen() {
		m.st.deferred++
	}
	return m.install(c)
}

func (m *metaTree) Restart() error {
	for id, v := range m.views {
		v.Close()
		delete(m.views, id)
	}
	m.pending = nil
	tr := m.newTree()
	if err := m.log.Restore(RootRefs, tr.LoadRun, tr.LoadBoundedRun); err != nil {
		return err
	}
	for _, w := range m.writes[m.log.Replay():] {
		if err := m.apply(tr, w); err != nil {
			return err
		}
	}
	m.countSearches()
	m.tr = tr
	m.st.restarts++
	return nil
}

func (m *metaTree) countSearches() {
	m.st.searches += m.tr.Searches()
	m.st.filterProbes += m.tr.FilterProbes()
}

// metaFailure is the first option set whose history differs from the model's.
type metaFailure struct {
	set  metaSet
	at   int
	got  []string
	want []string
}

// metaRun runs ops under every set in sets and the model, and returns the first set that differs
// from the model, the stats of each set, and whether some set differs from F when F is first.
func metaRun(ops []metamorphic.Op, sets []metaSet, check bool, plant plants) (*metaFailure, []metaStats, bool) {
	want := metamorphic.Run(ops, metamorphic.Options{}, newMetaModel())
	var fail *metaFailure
	var first []string
	stats := make([]metaStats, len(sets))
	fromF := false
	for i, s := range sets {
		e := newMetaTree(s, check, plant)
		got := metamorphic.Run(ops, s.run, e)
		e.countSearches()
		stats[i] = e.st
		stats[i].flushes, stats[i].failed = e.log.Flushes, e.log.Failed
		if i == 0 {
			first = got
		} else if sets[0].name == "F" && metamorphic.Diff(first, got) >= 0 {
			fromF = true
		}
		if d := metamorphic.Diff(want, got); d >= 0 && fail == nil {
			fail = &metaFailure{set: s, at: d, got: got, want: want}
		}
	}
	return fail, stats, fromF
}

// reduceMeta reduces ops, whose last op is the first to differ from the model under s, to the
// fewest whose first difference is still that op.
func reduceMeta(ops []metamorphic.Op, s metaSet, check bool, plant plants) []metamorphic.Op {
	last := ops[len(ops)-1].String()
	return metamorphic.Reduce(ops, func(c []metamorphic.Op) bool {
		f, _, _ := metaRun(c, []metaSet{s}, check, plant)
		return f != nil && f.at == len(c)-1 && c[f.at].String() == last
	})
}

// metaCase writes ops as a data-driven regression case under s with the model's history.
func metaCase(ops []metamorphic.Op, s metaSet) string {
	want := metamorphic.Run(ops, metamorphic.Options{}, newMetaModel())
	var b strings.Builder
	fmt.Fprintf(&b, "run set=%s\n", s.name)
	for _, o := range ops {
		b.WriteString(o.String() + "\n")
	}
	b.WriteString("----\n" + metamorphic.Format(ops, want))
	return b.String()
}

// [property] Every seed's ops read the same under every option set and the map model. A failing
// seed prints its replay command and its ops reduced to a regression case for testdata/regressions.
func TestMetamorphic(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 3
	}
	seeds.Run(t, 0, n, func(seed uint64) {
		ops := genMetaOps(seed, *metaOps)
		fail, stats, _ := metaRun(ops, metaSets, true, plants{})
		if seed == 0 {
			for i, s := range metaSets {
				t.Logf("seed 0 under %s: %+v", s.name, stats[i])
			}
		}
		if fail == nil {
			return
		}
		small := reduceMeta(ops[:fail.at+1], fail.set, true, plants{})
		t.Fatalf("seed %d under %s, op %d %q: got %q, the model %q\nreduced to %d ops, for testdata/regressions:\n%s",
			seed, fail.set.name, fail.at, ops[fail.at], fail.got[fail.at], fail.want[fail.at], len(small), metaCase(small, fail.set))
	})
}

// Each option set reaches the extreme it is there for on seed 0: F never flushes, C and G keep
// many runs, E fails flushes and restarts, D installs merges during flushes and two-merges runs
// two merges at once.
func TestMetamorphicSetsReachTheirExtremes(t *testing.T) {
	_, stats, _ := metaRun(genMetaOps(0, 3000), metaSets, true, plants{})
	of := func(name string) metaStats {
		return stats[slices.IndexFunc(metaSets, func(s metaSet) bool { return s.name == name })]
	}
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"F never flushes", of("F").flushes == 0},
		{"C keeps over 100 runs", of("C").maxRuns > 100},
		{"G keeps 40 runs", of("G").maxRuns >= 40},
		{"E fails flushes and restarts", of("E").failed > 0 && of("E").restarts > of("A").restarts},
		{"D installs merges during flushes", of("D").deferred > 0},
		{"two-merges runs two at once", of("two-merges").together > 0},
		{"fixed-limit compacts", of("fixed-limit").compactions > 0},
		{"steps installs merges in steps", of("steps").steps > 0},
		{"steps-restart resumes merges after restarts", of("steps-restart").resumed > 0},
		{"steps-deferred installs steps during flushes", of("steps-deferred").steps > 0 && of("steps-deferred").deferred > 0},
		{"filters-off probes no filter and searches more than C", of("filters-off").filterProbes == 0 && of("filters-off").searches > of("C").searches},
		{"filters-maybe probes as C does and searches as filters-off does", of("filters-maybe").filterProbes == of("C").filterProbes && of("filters-maybe").searches == of("filters-off").searches},
	} {
		if !c.ok {
			t.Errorf("%s: %+v", c.name, stats)
		}
	}
}

// Each planted bug, with Check off, makes the set it is checked under differ from the model within
// 20 seeds, and that seed differs from F too.
func TestMetamorphicFindsPlantedBugs(t *testing.T) {
	for _, c := range []struct {
		name  string
		set   string
		plant plants
	}{
		{"a flush drops every removal", "A", plants{dropRemovals: true}},
		{"a merge's output is installed as the oldest run", "B", plants{installOldest: true}},
		{"a restore loads runs in the manifest's order", "E", plants{loadUnordered: true}},
		{"reads skip the frozen memtable", "D", plants{skipFrozen: true}},
		{"a merge drops removals that hide keys in older runs", "B", plants{mergeNoOlder: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := metaSetNamed(c.set)
			f, _ := metaSetNamed("F")
			for seed := range uint64(20) {
				fail, _, fromF := metaRun(genMetaOps(seed, 3000), []metaSet{f, s}, false, c.plant)
				if fail == nil {
					continue
				}
				if fail.set.name != c.set {
					t.Fatalf("seed %d: F differs from the model", seed)
				}
				if !fromF {
					t.Fatalf("seed %d: %s differs from the model but not from F", seed, c.set)
				}
				return
			}
			t.Fatalf("no seed of 20 differs from the model under %s", c.set)
		})
	}
}

// metaPlants names the planted bugs a regression case can name with plant=, the one that found it.
var metaPlants = map[string]plants{
	"drop-removals":  {dropRemovals: true},
	"load-unordered": {loadUnordered: true},
	"merge-no-older": {mergeNoOlder: true},
	"clobber-bound":  {clobberBound: true},
}

// metaRegression reads a regression case: its option set, its ops and the plant that found it.
func metaRegression(t *testing.T, d *datadriven.TestData) (metaSet, []metamorphic.Op, plants) {
	if d.Cmd != "run" {
		d.Fatalf(t, "unknown command %q", d.Cmd)
	}
	var name string
	d.ScanArgs(t, "set", &name)
	s, ok := metaSetNamed(name)
	if !ok {
		d.Fatalf(t, "unknown option set %q", name)
	}
	var plant plants
	if d.HasArg("plant") {
		var p string
		d.ScanArgs(t, "plant", &p)
		if plant, ok = metaPlants[p]; !ok {
			d.Fatalf(t, "unknown plant %q", p)
		}
	}
	ops, err := metamorphic.Parse(d.Input, checkMetaOp)
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	return s, ops, plant
}

// The reduced failures of the metamorphic test, each run under its option set, must read as the
// model does; an unknown command, set, plant or op fails the test at its line.
func TestMetamorphicRegressions(t *testing.T) {
	datadriven.RunTest(t, "testdata/regressions", func(t *testing.T, d *datadriven.TestData) string {
		s, ops, _ := metaRegression(t, d)
		if fail, _, _ := metaRun(ops, []metaSet{s}, true, plants{}); fail != nil {
			t.Errorf("%s: op %d %q: got %q, the model %q", d.Pos, fail.at, ops[fail.at], fail.got[fail.at], fail.want[fail.at])
		}
		return metamorphic.Format(ops, metamorphic.Run(ops, s.run, newMetaTree(s, true, plants{})))
	})
}

// A regression case that names the planted bug that found it differs from the model with that
// bug on.
func TestMetamorphicRegressionsCatchTheirPlants(t *testing.T) {
	datadriven.RunTest(t, "testdata/regressions", func(t *testing.T, d *datadriven.TestData) string {
		s, ops, plant := metaRegression(t, d)
		if plant != (plants{}) {
			if fail, _, _ := metaRun(ops, []metaSet{s}, false, plant); fail == nil {
				t.Errorf("%s: reads as the model with %+v", d.Pos, plant)
			}
		}
		return d.Expected
	})
}

func TestMetamorphicParseRejects(t *testing.T) {
	for _, text := range []string{"put a", "frob a", "delrange b a", "scan a x", "flush now", "flushfail", "flushfail 0"} {
		if _, err := metamorphic.Parse(text, checkMetaOp); err == nil {
			t.Errorf("%q parsed", text)
		} else if !strings.Contains(err.Error(), "line 1") {
			t.Errorf("%q: %v names no line", text, err)
		}
	}
}

// A failure from a planted flush bug reduces to under 10 ops, written as a regression case that
// fails with the plant on.
func TestMetamorphicReducesAFailure(t *testing.T) {
	s, _ := metaSetNamed("A")
	plant := plants{dropRemovals: true}
	ops := genMetaOps(0, 3000)
	fail, _, _ := metaRun(ops, []metaSet{s}, false, plant)
	if fail == nil {
		t.Fatal("the plant reads as the model")
	}
	small := reduceMeta(ops[:fail.at+1], s, false, plant)
	t.Logf("reduced from %d ops to %d:\n%s", fail.at+1, len(small), metaCase(small, s))
	if len(small) >= 10 {
		t.Fatalf("reduced to %d ops", len(small))
	}
}
