package lsm

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
)

func buildBlock(keys, values []string) []byte {
	var w blockWriter
	w.reset(make([]byte, 0, 1<<16))
	for i, k := range keys {
		var v []byte
		if values != nil {
			v = []byte(values[i])
		}
		w.add([]byte(k), v)
	}
	return w.finish()
}

// From TestBlockWriter in Pebble's sstable/rowblk/rowblk_writer_test.go (a7b8321871e3).
func TestBlockWriterGolden(t *testing.T) {
	got := buildBlock([]string{"apple", "apricot", "banana"}, nil)
	want := []byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00")
	if !bytes.Equal(got, want) {
		t.Fatalf("block\n%q\nwant\n%q", got, want)
	}
	if empty := buildBlock(nil, nil); !bytes.Equal(empty, emptyBlock[:]) {
		t.Fatalf("empty block %q, want %q", empty, emptyBlock[:])
	}
}

// From TestBlockIter in Pebble's sstable/rowblk/rowblk_iter_test.go (a7b8321871e3): a seek to
// each key lands on the entry at index, and iteration runs to the end from there.
func TestBlockIterSeekGE(t *testing.T) {
	k := []byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00")
	cases := []struct {
		index int
		key   string
	}{
		{0, ""}, {0, "a"}, {0, "aaaaaaaaaaaaaaa"}, {0, "app"}, {0, "apple"},
		{1, "appliance"}, {1, "apricos"}, {1, "apricot"},
		{2, "azzzzzzzzzzzzzz"}, {2, "b"}, {2, "banan"}, {2, "banana"},
		{3, "banana\x00"}, {3, "c"},
	}
	keys := []string{"apple", "apricot", "banana"}
	for _, tc := range cases {
		var it blockIter
		it.init(k)
		var got []string
		for ok := it.seekGE([]byte(tc.key)); ok; ok = it.advance() {
			got = append(got, string(it.key))
		}
		if !slices.Equal(got, keys[tc.index:]) {
			t.Errorf("seek %q: %q, want %q", tc.key, got, keys[tc.index:])
		}
	}
}

func TestBlockFloor(t *testing.T) {
	b := buildBlock([]string{"apple", "apricot", "banana"}, []string{"1", "2", "3"})
	cases := []struct {
		key    string
		strict bool
		want   string
	}{
		{"", false, ""}, {"apple", false, "apple"}, {"apple", true, ""}, {"apples", false, "apple"},
		{"apricot", false, "apricot"}, {"apricot", true, "apple"}, {"b", false, "apricot"},
		{"banana", true, "apricot"}, {"banana", false, "banana"}, {"c", true, "banana"},
	}
	for _, tc := range cases {
		var it blockIter
		it.init(b)
		got := ""
		if it.floor([]byte(tc.key), tc.strict) {
			got = string(it.key)
			if want := map[string]string{"apple": "1", "apricot": "2", "banana": "3"}[got]; string(it.val) != want {
				t.Errorf("floor %q: value %q, want %q", tc.key, it.val, want)
			}
		}
		if got != tc.want {
			t.Errorf("floor(%q, strict %v) = %q, want %q", tc.key, tc.strict, got, tc.want)
		}
	}
}

// Seeks and floors across many restarts agree with a sorted list, and the block validates.
func TestBlockRestartsProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(31, 0))
	for trial := range 300 {
		set := map[string]bool{}
		for range r.IntN(100) {
			set[randKey(r, 1+r.IntN(6), 3)] = true
		}
		keys := slices.Sorted(func(yield func(string) bool) {
			for k := range set {
				if !yield(k) {
					return
				}
			}
		})
		values := make([]string, len(keys))
		for i := range values {
			values[i] = string([]byte{kindSet}) + fmt.Sprint(i)
		}
		b := buildBlock(keys, values)
		if n, last, ok := validateBlock(b, nil, false); !ok || n != len(keys) || (n > 0 && string(last) != keys[n-1]) {
			t.Fatalf("trial %d: validate %d %q %v", trial, n, last, ok)
		}
		for range 50 {
			probe := randKey(r, r.IntN(7), 3)
			var it blockIter
			it.init(b)
			i := sort.SearchStrings(keys, probe)
			if ok := it.seekGE([]byte(probe)); ok != (i < len(keys)) || ok && string(it.key) != keys[i] {
				t.Fatalf("trial %d: seek %q at %q, want index %d of %q", trial, probe, it.key, i, keys)
			}
			for _, strict := range []bool{false, true} {
				j := sort.Search(len(keys), func(j int) bool {
					return keys[j] > probe || (strict && keys[j] == probe)
				}) - 1
				it.init(b)
				if ok := it.floor([]byte(probe), strict); ok != (j >= 0) || ok && (string(it.key) != keys[j] || string(it.val) != values[j]) {
					t.Fatalf("trial %d: floor(%q, %v) at %q, want index %d of %q", trial, probe, strict, it.key, j, keys)
				}
			}
		}
	}
}

func randKey(r *rand.Rand, n, alphabet int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(r.IntN(alphabet))
	}
	return string(b)
}

func TestValidateBlockRejects(t *testing.T) {
	good := buildBlock([]string{"a", "b"}, []string{"\x01x", "\x00"})
	if _, _, ok := validateBlock(good, nil, false); !ok {
		t.Fatal("good block rejected")
	}
	bad := map[string][]byte{
		"truncated":       good[:len(good)-1],
		"unsorted":        buildBlock([]string{"b", "a"}, []string{"\x01", "\x01"}),
		"duplicate":       buildBlock([]string{"a", "a"}, []string{"\x01", "\x01"}),
		"empty value":     buildBlock([]string{"a"}, []string{""}),
		"unknown kind":    buildBlock([]string{"a"}, []string{"\x07"}),
		"restart past":    append(slices.Clone(good[:len(good)-8]), 9, 0, 0, 0, 1, 0, 0, 0),
		"no restarts":     append(slices.Clone(good[:len(good)-8]), 0, 0, 0, 0),
		"too many counts": append(slices.Clone(good[:len(good)-4]), 0xff, 0, 0, 0),
	}
	for name, b := range bad {
		if _, _, ok := validateBlock(b, nil, false); ok {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, _, ok := validateBlock(good, []byte("b"), true); ok {
		t.Error("block not above the previous key accepted")
	}
}
