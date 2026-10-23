// The tests marked as from Pebble are ported from sstable/tablefilters/bloom/bloom_test.go
// (a7b8321871e3), under the notice in filter.go.

package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func newFilter(keys ...[]byte) []byte {
	var f filterWriter
	f.reset(WholeKeys, nil, len(keys))
	for _, k := range keys {
		f.add(k)
	}
	return f.finish()
}

func filterStr(f []byte) string {
	var buf strings.Builder
	for i, x := range f {
		if i > 0 {
			if i%8 == 0 {
				buf.WriteString("\n")
			} else {
				buf.WriteString("  ")
			}
		}
		for j := range uint(8) {
			if x&(1<<(7-j)) != 0 {
				buf.WriteString("1")
			} else {
				buf.WriteString(".")
			}
		}
	}
	buf.WriteString("\n")
	return buf.String()
}

// From Pebble's TestSmallBloomFilter; the bits come from RocksDB's FullBloomTest.FullSmall.
func TestSmallBloomFilter(t *testing.T) {
	f := newFilter([]byte("hello"), []byte("world"))
	want := strings.TrimLeft(`
........  ........  ........  .......1  ........  ........  ........  ........
........  .1......  ........  .1......  ........  ........  ........  ........
...1....  ........  ........  ........  ........  ........  ........  ........
........  ........  ........  ........  ........  ........  ........  ...1....
........  ........  ........  ........  .....1..  ........  ........  ........
.......1  ........  ........  ........  ........  ........  .1......  ........
........  ........  ........  ........  ........  ...1....  ........  ........
.......1  ........  ........  ........  .1...1..  ........  ........  ........
.....11.  .......1  ........  ........  ........
`, "\n")
	if got := filterStr(f[filterKeyHdr:]); got != want {
		t.Fatalf("filter\n%s\nwant\n%s", got, want)
	}
	if !validFilter(f, WholeKeys) || filterKeyCount(f) != 2 {
		t.Fatalf("filter header %x", f[:filterKeyHdr])
	}
	for k, want := range map[string]bool{"hello": true, "world": true, "x": false, "foo": false} {
		if got := mayContain(f, bloomHash([]byte(k))); got != want {
			t.Errorf("may contain %q: %v, want %v", k, got, want)
		}
	}
}

// From Pebble's TestBloomFilter: every added key matches, filters stay within their size, and
// false positives stay under 2%, mostly under 1.25%.
func TestBloomFilter(t *testing.T) {
	nextLength := func(x int) int {
		switch {
		case x < 10:
			return x + 1
		case x < 100:
			return x + 10
		case x < 1000:
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte { return binary.LittleEndian.AppendUint32(nil, uint32(i)) }
	mediocre, good := 0, 0
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := range length {
			keys = append(keys, le32(i))
		}
		f := newFilter(keys...)
		if maxLen := filterKeyHdr + 5 + ((length*10)/cacheLineBits+2)*cacheLineSize; len(f) > maxLen {
			t.Errorf("length %d: filter of %d bytes, max %d", length, len(f), maxLen)
			continue
		}
		for _, k := range keys {
			if !mayContain(f, bloomHash(k)) {
				t.Fatalf("length %d: did not contain key %q", length, k)
			}
		}
		fp := 0
		for i := range 10000 {
			if mayContain(f, bloomHash(le32(1e9+i))) {
				fp++
			}
		}
		if fp > 0.02*10000 {
			t.Errorf("length %d: %d false positives in 10000", length, fp)
		}
		if fp > 0.0125*10000 {
			mediocre++
		} else {
			good++
		}
	}
	if mediocre > good/5 {
		t.Errorf("%d mediocre filters but only %d good ones", mediocre, good)
	}
}

// From Pebble's TestHash; the values come from RocksDB's util/hash_test.cc TestHash.
func TestBloomHash(t *testing.T) {
	for _, c := range []struct {
		s    string
		want uint32
	}{
		{"", 3164544308}, {"\x08", 422599524}, {"\x17", 3168152998}, {"\x9a", 3195034349},
		{"\x1c", 2651681383}, {"\x4d\x76", 2447836956}, {"\x52\xd5", 3854228105}, {"\x91\xf7", 31066776},
		{"\xd6\x27", 1806091603}, {"\x30\x46\x0b", 3808221797}, {"\x56\xdc\xd6", 2157698265},
		{"\xd4\x52\x33", 1721992661}, {"\x6a\xb5\xf4", 2469105222}, {"\x67\x53\x81\x1c", 118283265},
		{"\x69\xb8\xc0\x88", 3416318611}, {"\x1e\x84\xaf\x2d", 3315003572}, {"\x46\xdc\x54\xbe", 447346355},
		{"\xd0\x7a\x6e\xea\x56", 4255445370}, {"\x86\x83\xd5\xa4\xd8", 2390603402},
		{"\xb7\x46\xbb\x77\xce", 2048907743}, {"\x6c\xa8\xbc\xe5\x99", 2177978500},
		{"\x5c\x5e\xe1\xa0\x73\x81", 1036846008}, {"\x08\x5d\x73\x1c\xe5\x2e", 229980482},
		{"\x42\xfb\xf2\x52\xb4\x10", 3655585422}, {"\x73\xe1\xff\x56\x9c\xce", 3502708029},
		{"\x5c\xbe\x97\x75\x54\x9a\x52", 815120748}, {"\x16\x82\x39\x49\x88\x2b\x36", 3056033698},
		{"\x59\x77\xf0\xa7\x24\xf4\x78", 587205227}, {"\xd3\xa5\x7c\x0e\xc0\x02\x07", 2030937252},
		{"\x31\x1b\x98\x75\x96\x22\xd3\x9a", 469635402}, {"\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 3530274698},
		{"\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 1974545809}, {"\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 3563570120},
		{"\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 2706087434}, {"\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 1534654151},
		{"\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 2355554696}, {"\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 1400800912},
		{"\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 3420325137}, {"\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 3427803584},
		{"\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 1152407945}, {"\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 3382479516},
	} {
		if got := bloomHash([]byte(c.s)); got != c.want {
			t.Errorf("hash %q = %d, want %d", c.s, got, c.want)
		}
	}
}

// A filter for 1,000,000 random 16-byte keys rounds 10,000,000 bits up to 19,532 lines, then to
// the odd 19,533, and over 1,000,000 absent keys lets 1.0 to 1.4% through; a probe allocates
// nothing.
func TestFilterSizeAndFalsePositives(t *testing.T) {
	const n = 1_000_000
	var f filterWriter
	f.reset(WholeKeys, nil, n)
	r := rand.New(rand.NewPCG(1, 1))
	k := make([]byte, 16)
	for range n {
		binary.BigEndian.PutUint64(k, r.Uint64())
		binary.BigEndian.PutUint64(k[8:], r.Uint64())
		f.add(k)
	}
	b := f.finish()
	if lines := binary.LittleEndian.Uint32(b[len(b)-4:]); lines != 19_533 || len(b)-filterKeyHdr != 1_250_112+5 {
		t.Fatalf("%d lines in %d bytes", lines, len(b)-filterKeyHdr)
	}
	fp := 0
	for range n {
		binary.BigEndian.PutUint64(k, r.Uint64())
		binary.BigEndian.PutUint64(k[8:], r.Uint64())
		if mayContain(b, bloomHash(k)) {
			fp++
		}
	}
	rate := float64(fp) / n * 100
	t.Logf("%.3f%% false positives", rate)
	if rate < 1.0 || rate > 1.4 {
		t.Fatalf("%.3f%% false positives, want 1.0 to 1.4%%", rate)
	}
	if a := testing.AllocsPerRun(100, func() { mayContain(b, bloomHash(k)) }); a != 0 {
		t.Fatalf("a probe allocates %v", a)
	}
}

// A filter of prefixes holds each distinct prefix once, and one of keys and prefixes each key and
// each prefix that is not a whole key; a key with no prefix adds no prefix.
func TestFilterPrefixKeys(t *testing.T) {
	two := func(k []byte) int { return min(len(k), 2) }
	keys := []string{"a", "ab", "ab1", "ab2", "ac1", "b"}
	for _, c := range []struct {
		what FilterKeys
		want int
		in   []string
	}{
		{WholeKeys, 6, []string{"a", "ab", "ab1", "ab2", "ac1", "b"}},
		{Prefixes, 4, []string{"a", "ab", "ac", "b"}},
		{KeysAndPrefixes, 7, []string{"a", "ab", "ab1", "ab2", "ac", "ac1", "b"}},
	} {
		var f filterWriter
		f.reset(c.what, two, 2*len(keys))
		for _, k := range keys {
			f.add([]byte(k))
		}
		b := f.finish()
		if filterKeyCount(b) != c.want {
			t.Errorf("%d: %d keys, want %d", c.what, filterKeyCount(b), c.want)
		}
		for _, k := range c.in {
			if !mayContain(b, bloomHash([]byte(k))) {
				t.Errorf("%d: %q ruled out", c.what, k)
			}
		}
	}
}

func BenchmarkFilterProbe(b *testing.B) {
	keys := make([][]byte, 100_000)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key%09d", i)
	}
	f := newFilter(keys...)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		mayContain(f, bloomHash(keys[i%len(keys)]))
		i++
	}
}

// noFalsePositives fails the test if a run's filter lets through one of keys the run does not
// hold, so that counts of searches depend on the filters' design and not on this hash's luck.
func noFalsePositives(t *testing.T, tr *Tree, keys ...string) {
	t.Helper()
	var it blockIter
	for i, r := range tr.runs {
		for _, k := range keys {
			if _, held := r.blocks.get(&it, []byte(k)); !held && r.mayContain([]byte(k), bloomHash([]byte(k))) {
				t.Fatalf("run %d's filter lets %q through; pick another key", i, k)
			}
		}
	}
}

// A get probes the filters newest first: a key in the newest run probes no older run's filter,
// and a covering fragment in a newer run stops the get before any older filter is probed.
func TestFilterProbeOrder(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	put(t, tr, nil, "y", "old")
	put(t, tr, nil, "z", "old")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, "x", "new")
	put(t, tr, nil, "zz", "new")
	delRange(t, tr, nil, "y", "yy")
	f.checkpoint(t, tr, nil)
	noFalsePositives(t, tr, "x", "y", "z")
	for _, c := range []struct {
		k      string
		found  bool
		probes int
	}{{"x", true, 1}, {"y", false, 1}, {"z", true, 2}} {
		before := tr.it.filterProbes
		if _, ok := tr.Get([]byte(c.k)); ok != c.found {
			t.Fatalf("get %q found %v", c.k, ok)
		}
		if n := tr.it.filterProbes - before; n != c.probes {
			t.Fatalf("get %q probed %d filters, want %d", c.k, n, c.probes)
		}
	}
}

// A filter holds no fragment: a run whose fragment covers d and whose filter rules d out still
// hides the d an older run holds.
func TestFilterHoldsNoFragments(t *testing.T) {
	tr := newTree(t, 1<<20)
	f := newFakeLog()
	put(t, tr, nil, "d", "old")
	f.checkpoint(t, tr, nil)
	put(t, tr, nil, "a", "new")
	delRange(t, tr, nil, "c", "f")
	f.checkpoint(t, tr, nil)
	if tr.runs[0].mayContain([]byte("d"), bloomHash([]byte("d"))) {
		t.Fatal("the newer run's filter lets d through")
	}
	if v, ok := tr.Get([]byte("d")); ok {
		t.Fatalf("get d = %q under the fragment", v)
	}
	if k, _, ok := tr.Floor([]byte("e")); !ok || string(k) != "a" {
		t.Fatalf("floor e = %q %v, want a", k, ok)
	}
}

// filterRuns is a tree of 24 runs and a memtable, each holding the even blocks 0 to 18 of 200
// chunks of its own, each key an 8-digit chunk prefix then a block.
func filterRuns(t testing.TB, cfg Config) (*Tree, *fakeLog, []disklog.BlobRef) {
	tr, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeLog()
	var roots []disklog.BlobRef
	for layer := range 25 {
		for c := range 200 {
			for b := range 10 {
				put(t, tr, nil, string(chunkKey(nil, c, layer, 2*b)), "v")
			}
		}
		if layer < 24 {
			roots, _ = f.checkpoint(t, tr, nil)
		}
	}
	return tr, f, roots
}

func chunkKey(dst []byte, c, layer, block int) []byte {
	return fmt.Appendf(dst, "%08d/%02d", (c*25+layer)*7919%100_000_007, block)
}

func chunkPrefix(k []byte) int { return min(len(k), 8) }

// Over 24 runs, a get of an absent key searches at most 0.5 runs on average and allocates nothing,
// and a floor inside a chunk searches the run holding it plus at most 0.3 more; a restored tree's
// filters read as before, and a tree whose filters hold other keys refuses the runs.
func TestFilterMissesOver24Runs(t *testing.T) {
	cfg := Config{MaxPiece: 1 << 20, Filter: KeysAndPrefixes, Prefix: chunkPrefix}
	tr, f, roots := filterRuns(t, cfg)
	if tr.Runs() != 24 {
		t.Fatalf("%d runs", tr.Runs())
	}
	const n = 10_000
	var key []byte
	searches := func(r *Tree) (float64, float64) {
		before := r.Searches()
		for i := range n {
			key = chunkKey(key[:0], i%200, i%24, 2*(i%10)+1)
			if _, ok := r.Get(key); ok {
				t.Fatalf("get %q found an absent key", key)
			}
		}
		mid := r.Searches()
		for i := range n {
			key = chunkKey(key[:0], i%200, i%24, 2*(i%10)+1)
			if k, _, ok := r.FloorPrefix(key); !ok || len(k) != len(key) || k[len(k)-1] != key[len(key)-1]-1 {
				t.Fatalf("prefix floor %q = %q %v", key, k, ok)
			}
		}
		return float64(mid-before)/n - 1, float64(r.Searches()-mid)/n - 1
	}
	miss, floor := searches(tr)
	t.Logf("runs searched: %.3f per miss, %.3f per prefix floor", miss, floor)
	if miss > 0.5 || floor > 1.3 {
		t.Fatalf("runs searched: %.3f per miss, %.3f per prefix floor", miss, floor)
	}
	if a := testing.AllocsPerRun(100, func() { tr.Get(key) }); a != 0 {
		t.Fatalf("a filtered get allocates %v", a)
	}
	if a := testing.AllocsPerRun(100, func() { tr.FloorPrefix(key) }); a != 0 {
		t.Fatalf("a prefix floor allocates %v", a)
	}
	restored, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.load(restored, roots); err != nil {
		t.Fatal(err)
	}
	put(t, restored, nil, "~", "v")
	if m, fl := searches(restored); m != miss || fl != floor {
		t.Fatalf("restored: %.3f per miss, %.3f per prefix floor, want %.3f and %.3f", m, fl, miss, floor)
	}
	whole, err := New(Config{MaxPiece: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.load(whole, roots); !errors.Is(err, ErrMalformed) {
		t.Fatalf("whole-key tree loading prefix filters: %v", err)
	}
}
