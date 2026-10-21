package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// A fenced partition is the filter's length, the filter, a fence flag, the fence's length and the
// fence; an unfenced one ends in a zero flag and length. Decoding refuses a partition holding other
// keys than the tree's, an unfenced partition before the last, fences out of order, a bad flag, an
// unfenced partition with a fence and any truncation.
func TestFilterPartitionFormat(t *testing.T) {
	var f filterWriter
	f.reset(WholeKeys, nil, 1)
	f.add([]byte("a"))
	bits := f.finish()
	p := newPart(bits, []byte("k5"), true)
	want := append(binary.LittleEndian.AppendUint32(nil, uint32(len(bits))), bits...)
	want = append(want, "\x01"+"\x02\x00"+"k5"...)
	if !bytes.Equal(p.enc, want) || !bytes.Equal(p.bits, bits) || string(p.fence) != "k5" {
		t.Fatalf("partition\n%x\nwant\n%x", p.enc, want)
	}
	last := newPart(bits, []byte("ignored"), false)
	if got := last.enc[len(last.enc)-partTrailer:]; !bytes.Equal(got, []byte{0, 0, 0}) || len(last.fence) != 0 {
		t.Fatalf("unfenced trailer %x, fence %q", got, last.fence)
	}
	two := slices.Concat(p.enc, last.enc)
	parts, ok := parseParts(two, WholeKeys)
	if !ok || len(parts) != 2 || string(parts[0].fence) != "k5" || parts[1].fenced {
		t.Fatalf("parsed %+v, %v", parts, ok)
	}
	if _, ok := parseParts(slices.Concat(newPart(bits, []byte("a"), true).enc), WholeKeys); !ok {
		t.Fatal("a run between merge steps, every partition fenced, does not parse")
	}
	bad := map[string][]byte{
		"other keys":       newPart(bits, nil, false).enc,
		"unfenced first":   slices.Concat(last.enc, p.enc),
		"fences unordered": slices.Concat(p.enc, newPart(bits, []byte("k4"), true).enc),
		"fences equal":     slices.Concat(p.enc, p.enc),
		"bad flag":         append(slices.Clone(p.enc[:len(p.enc)-5]), "\x02\x02\x00k5"...),
		"fence unfenced":   append(slices.Clone(last.enc[:len(last.enc)-3]), "\x00\x01\x00k"...),
		"empty":            nil,
	}
	for n := range len(two) - 1 {
		if n != len(p.enc) {
			bad[fmt.Sprint("truncated to ", n)] = two[:n]
		}
	}
	for name, b := range bad {
		what := WholeKeys
		if name == "other keys" {
			what = Prefixes
		}
		if _, ok := parseParts(b, what); ok {
			t.Errorf("%s: parsed", name)
		}
	}
}

// prefixTree is a tree whose filters hold keys and their first n bytes, merging in steps of a piece.
func prefixTree(t testing.TB, piece, n int) *Tree {
	t.Helper()
	tr, err := New(Config{MaxPiece: piece, StepBytes: piece, Check: true, Filter: KeysAndPrefixes, Prefix: func(k []byte) int {
		if len(k) < n {
			return 0
		}
		return n
	}})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// mergeAll merges every run of tr in steps to one, a checkpoint after each step, checking reads
// after each.
func mergeAll(t testing.TB, tr *Tree, f *fakeLog, m model, probes []string) int {
	t.Helper()
	steps := 0
	for c := tr.pickSpan(0, len(tr.runs), ruleNone); c != nil; c = tr.PickCompaction() {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		checkReads(t, tr, m, probes)
		_, dead := f.checkpoint(t, tr, nil)
		f.drop(dead)
		steps++
	}
	return steps
}

// prefixKeys is how a partition test's keys take prefixes: a fixed two bytes, or the bytes up to
// and including the first slash, one or two letters before it.
type prefixKeys struct {
	name   string
	prefix func([]byte) int
	key    func(r *rand.Rand) string
	probes []string
}

func prefixCases() []prefixKeys {
	letter := func(r *rand.Rand) byte { return 'a' + byte(r.IntN(6)) }
	var fixed, slashed []string
	for c := 'a'; c <= 'g'; c++ {
		for d := 'a'; d <= 'g'; d++ {
			fixed = append(fixed, string(c)+string(d), string(c)+string(d)+"2", string(c)+string(d)+"9")
			slashed = append(slashed, string(c)+string(d)+"/", string(c)+string(d)+"/2", string(c)+string(d)+"/9")
		}
		slashed = append(slashed, string(c)+"/", string(c)+"/2", string(c)+"/9", string(c))
	}
	return []prefixKeys{
		{"fixed", func(k []byte) int {
			if len(k) < 2 {
				return 0
			}
			return 2
		}, func(r *rand.Rand) string { return fmt.Sprintf("%c%c%d", letter(r), letter(r), r.IntN(4)) }, fixed},
		{"slash", func(k []byte) int { return bytes.IndexByte(k, '/') + 1 }, func(r *rand.Rand) string {
			p := string(letter(r))
			if r.IntN(2) == 0 {
				p += string(letter(r))
			}
			return fmt.Sprintf("%s/%d", p, r.IntN(4))
		}, slashed},
	}
}

// partitionReadsDiffer merges four runs of seed's keys in steps, under filters holding what and
// pk's prefixes, and describes the first read that differs from the map between steps, after them
// or after a restore.
func partitionReadsDiffer(t testing.TB, seed uint64, what FilterKeys, pk prefixKeys, plant plants) string {
	r := rand.New(rand.NewPCG(seed, 7))
	tr, err := New(Config{MaxPiece: 256, StepBytes: 256, Check: true, Filter: what, Prefix: pk.prefix})
	if err != nil {
		t.Fatal(err)
	}
	tr.plant = plant
	f := newFakeLog()
	m := model{}
	for run := range 4 {
		for range 150 {
			k := pk.key(r)
			if r.IntN(3) == 0 {
				del(t, tr, m, k)
			} else {
				put(t, tr, m, k, fmt.Sprint(run))
			}
		}
		if run == 2 {
			delRange(t, tr, m, "bb", "cc")
		}
		f.checkpoint(t, tr, nil)
	}
	steps := 0
	for c := tr.pickSpan(0, 4, ruleNone); c != nil; c = tr.PickCompaction() {
		c.Run()
		if err := tr.Install(c); err != nil {
			t.Fatal(err)
		}
		if d := readsDiffer(tr, m, pk.probes); d != "" {
			return d
		}
		_, dead := f.checkpoint(t, tr, nil)
		f.drop(dead)
		steps++
	}
	if parts := tr.runs[0].parts; steps < 3 || len(parts) < 3 || parts[len(parts)-1].fenced || !parts[0].fenced {
		t.Fatalf("seed %d: %d steps, %d partitions", seed, steps, len(parts))
	}
	return readsDiffer(f.restoreManifest(t, tr.cfg), m, pk.probes)
}

// [property] A run merged in steps has a filter partition per step, fenced by the step's bound, and
// reads like the map under whole-key, prefix and both filters, with fixed and variable-length
// prefixes: gets, floors, prefix floors and prefix scans of prefixes whose keys fall in several
// partitions, between steps and after, restored or not. Removals the merge drops can end a step,
// and with the fence's prefix left out of its partition a lookup by prefix, a prefix filter's get
// among them, then misses keys of later partitions.
func TestStepPartitionsReadLikeTheMap(t *testing.T) {
	for _, what := range []FilterKeys{WholeKeys, Prefixes, KeysAndPrefixes} {
		for _, pk := range prefixCases() {
			t.Run([]string{"keys", "prefixes", "both"}[what]+"/"+pk.name, func(t *testing.T) {
				planted := false
				for seed := range uint64(16) {
					if d := partitionReadsDiffer(t, seed, what, pk, plants{}); d != "" {
						t.Fatalf("seed %d: %s", seed, d)
					}
					planted = planted || partitionReadsDiffer(t, seed, what, pk, plants{noFencePrefix: true}) != ""
				}
				if planted != (what != WholeKeys) {
					t.Fatalf("with the fence's prefix left out a seed misses a key: %v", planted)
				}
			})
		}
	}
}

// Each lookup probes one partition of each run however many partitions it has, and allocates
// nothing: a get, a prefix floor and a prefix seek over a run of many partitions.
func TestPartitionedLookupsProbeOnce(t *testing.T) {
	tr := prefixTree(t, 256, 1)
	f := newFakeLog()
	m := model{}
	for run := range 4 {
		for i := range 80 {
			put(t, tr, m, fmt.Sprintf("%c%03d", 'a'+i%4, i), fmt.Sprint(run))
		}
		f.checkpoint(t, tr, nil)
	}
	mergeAll(t, tr, f, m, nil)
	if tr.Runs() != 1 || len(tr.runs[0].parts) < 3 {
		t.Fatalf("%d runs, %d partitions", tr.Runs(), len(tr.runs[0].parts))
	}
	var it Iter
	for _, k := range []string{"a000", "b041", "c999", "d079", "d0"} {
		kb := []byte(k)
		for name, lookup := range map[string]func(){
			"get":          func() { tr.Get(kb) },
			"prefix floor": func() { tr.FloorPrefix(kb) },
			"prefix seek":  func() { tr.Iter(&it); it.SeekPrefixGE(kb) },
		} {
			before := tr.it.filterProbes + it.s.filterProbes
			lookup()
			if n := tr.it.filterProbes + it.s.filterProbes - before; n != 1 {
				t.Errorf("%s %s probed %d partitions", name, k, n)
			}
			if a := testing.AllocsPerRun(100, lookup); a != 0 {
				t.Errorf("%s %s allocates %v", name, k, a)
			}
		}
	}
}

// FuzzFilterParts feeds arbitrary bytes as a run's filter area: decoding never panics, and the
// partitions it accepts encode back to the same bytes and answer probes of any key.
func FuzzFilterParts(f *testing.F) {
	var w filterWriter
	w.reset(Prefixes, func(k []byte) int { return min(len(k), 1) }, 2)
	w.add([]byte("ab"))
	bits := w.finish()
	f.Add(slices.Concat(newPart(bits, []byte("ab"), true).enc, newPart(bits, nil, false).enc), []byte("a"))
	f.Add(newPart(bits, []byte("k"), true).enc, []byte("z"))
	f.Fuzz(func(t *testing.T, b, k []byte) {
		parts, ok := parseParts(b, Prefixes)
		if !ok {
			return
		}
		var again []byte
		r := &run{}
		for _, p := range parts {
			again = append(again, newPart(p.bits, p.fence, p.fenced).enc...)
			r.addPart(p)
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("re-encoding %x gave %x", b, again)
		}
		r.mayContain(k, bloomHash(k))
	})
}

// BenchmarkPartitionedLookups gets, floors within a prefix and seeks a prefix over one run of 20
// or more filter partitions, a merge's output in steps.
func BenchmarkPartitionedLookups(b *testing.B) {
	tr := prefixTree(b, 4096, 2)
	tr.cfg.StepBytes = 4096
	f := newFakeLog()
	var keys [][]byte
	for run := range 4 {
		for i := range 3000 {
			k := fmt.Sprintf("%02x%04d%d", i%256, i, run)
			put(b, tr, nil, k, "v")
			keys = append(keys, []byte(k))
		}
		f.checkpoint(b, tr, nil)
	}
	for c := tr.pickSpan(0, 4, ruleNone); c != nil; c = tr.PickCompaction() {
		c.Run()
		if err := tr.Install(c); err != nil {
			b.Fatal(err)
		}
		f.checkpoint(b, tr, nil)
	}
	if tr.Runs() != 1 || len(tr.runs[0].parts) < 20 {
		b.Fatalf("%d runs, %d partitions", tr.Runs(), len(tr.runs[0].parts))
	}
	var it Iter
	for name, lookup := range map[string]func(k []byte){
		"get":          func(k []byte) { tr.Get(k) },
		"prefix floor": func(k []byte) { tr.FloorPrefix(k) },
		"prefix seek":  func(k []byte) { tr.Iter(&it); it.SeekPrefixGE(k) },
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				lookup(keys[i%len(keys)])
				i++
			}
		})
	}
}
