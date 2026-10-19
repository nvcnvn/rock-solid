package lsmd

import (
	"bytes"
	"slices"
)

// frag is a range removal [lo, hi): it hides the keys in that range held by older sources. A run's
// own entries in the range win over its own fragments. A memtable turns its entries in a removed
// range into removals and cuts a key it later sets out of its fragments, so its fragments hide
// its own entries too: a floor or a seek skips them in one search.
type frag struct{ lo, hi []byte }

// rangeSet is a sorted list of non-overlapping fragments. It is never changed in place, so a
// state that captured it keeps reading the set it saw.
type rangeSet []frag

// cover returns the fragment holding k.
func (s rangeSet) cover(k []byte) (frag, bool) {
	if len(s) == 0 {
		return frag{}, false
	}
	i, _ := slices.BinarySearchFunc(s, k, func(f frag, k []byte) int {
		if bytes.Compare(f.lo, k) <= 0 {
			return -1
		}
		return 1
	})
	if i == 0 {
		return frag{}, false
	}
	f := s[i-1]
	if bytes.Compare(k, f.hi) < 0 {
		return f, true
	}
	return frag{}, false
}

// with returns the set plus [lo, hi), merged with the fragments it touches.
func (s rangeSet) with(lo, hi []byte) rangeSet {
	lo, hi = bytes.Clone(lo), bytes.Clone(hi)
	out := make(rangeSet, 0, len(s)+1)
	placed := false
	for _, f := range s {
		switch {
		case bytes.Compare(f.hi, lo) < 0:
			out = append(out, f)
		case bytes.Compare(hi, f.lo) < 0:
			if !placed {
				out = append(out, frag{lo, hi})
				placed = true
			}
			out = append(out, f)
		default:
			if bytes.Compare(f.lo, lo) < 0 {
				lo = f.lo
			}
			if bytes.Compare(f.hi, hi) > 0 {
				hi = f.hi
			}
		}
	}
	if !placed {
		out = append(out, frag{lo, hi})
	}
	return out
}

func (s rangeSet) bytes() int {
	n := 0
	for _, f := range s {
		n += len(f.lo) + len(f.hi) + 4
	}
	return n
}

// heldIn reports whether any run holds a key in [lo, hi).
func heldIn(runs []*run, lo, hi []byte) bool {
	var ri runIter
	for _, r := range runs {
		ri.init(r)
		if ri.seekGE(lo) && bytes.Compare(ri.it.key, hi) < 0 {
			return true
		}
	}
	return false
}

// keepHeld returns the fragments of s that some run in older still has keys in.
func keepHeld(s rangeSet, older []*run) rangeSet {
	var out rangeSet
	for _, f := range s {
		if heldIn(older, f.lo, f.hi) {
			out = append(out, f)
		}
	}
	return out
}

// without returns the set with k cut out of the fragment holding it.
func (s rangeSet) without(k []byte) rangeSet {
	f, ok := s.cover(k)
	if !ok {
		return s
	}
	out := make(rangeSet, 0, len(s)+1)
	after := append(bytes.Clone(k), 0)
	for _, g := range s {
		if !bytes.Equal(g.lo, f.lo) {
			out = append(out, g)
			continue
		}
		if bytes.Compare(g.lo, k) < 0 {
			out = append(out, frag{g.lo, bytes.Clone(k)})
		}
		if bytes.Compare(after, g.hi) < 0 {
			out = append(out, frag{after, g.hi})
		}
	}
	return out
}
