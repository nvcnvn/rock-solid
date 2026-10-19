package lsmx

import "bytes"

// source is one layer of a tree: a memtable at a root, or a run.
type source struct {
	rdels rangeSet
	ck    []byte
	cv    []byte
	cok   bool
	mem   *memtable
	root  uint32
	run   *run
	mc    memCursor
	ri    runIter
	ok    bool
}

func (s *source) seekGE(k []byte) {
	if s.run == nil {
		s.ok = s.mc.seekGE(k)
	} else {
		s.ok = s.ri.seekGE(k)
	}
}

func (s *source) next() {
	if s.run == nil {
		s.ok = s.mc.next()
	} else {
		s.ok = s.ri.next()
	}
}

func (s *source) key() []byte {
	if s.run == nil {
		return s.mc.key()
	}
	return s.ri.it.key
}

func (s *source) value() []byte {
	if s.run == nil {
		return s.mc.value()
	}
	return s.ri.it.val
}

func (s *source) get(k []byte) ([]byte, bool) {
	if s.run == nil {
		return s.mem.get(s.root, k)
	}
	return s.run.get(&s.ri.it, k)
}

func (s *source) floor(k []byte, strict bool) ([]byte, []byte, bool) {
	if s.run == nil {
		return s.mem.floor(s.root, k, strict)
	}
	if !s.ri.floor(k, strict) {
		return nil, nil, false
	}
	return s.ri.it.key, s.ri.it.val, true
}

// state is what a read sees: the memtable, the one being flushed and the runs, newest first.
type state struct {
	memRdels    rangeSet
	frozenRdels rangeSet
	mem         *memtable
	memRoot     uint32
	frozen      *memtable
	frozenRoot  uint32
	runs        []*run
}

// sources lays the state's layers out newest first in srcs, reusing its buffers.
func (st *state) sources(srcs []source) []source {
	n := 1 + len(st.runs)
	if st.frozen != nil {
		n++
	}
	for len(srcs) < n {
		if len(srcs) < cap(srcs) {
			srcs = srcs[:len(srcs)+1]
		} else {
			srcs = append(srcs, source{})
		}
	}
	srcs = srcs[:n]
	i := 0
	set := func(m *memtable, root uint32, rd rangeSet) {
		s := &srcs[i]
		s.mem, s.root, s.run, s.ok, s.rdels = m, root, nil, false, rd
		s.mc.init(m, root)
		i++
	}
	set(st.mem, st.memRoot, st.memRdels)
	if st.frozen != nil {
		set(st.frozen, st.frozenRoot, st.frozenRdels)
	}
	for _, r := range st.runs {
		s := &srcs[i]
		s.mem, s.run, s.ok, s.rdels = nil, r, false, r.rdels
		s.ri.init(r)
		i++
	}
	return srcs
}

// iterState merges sources in key order; among equal keys the newest source's entry wins.
type iterState struct {
	srcs    []source
	cur     int
	keepDel bool
	prev    []byte
	probes  int
	steps   int
	ranges  bool
}

func (st *state) hasRanges() bool {
	if len(st.memRdels) > 0 || len(st.frozenRdels) > 0 {
		return true
	}
	for _, r := range st.runs {
		if len(r.rdels) > 0 {
			return true
		}
	}
	return false
}

func (it *iterState) reset(st *state, keepDel bool) {
	it.srcs = st.sources(it.srcs)
	it.cur, it.keepDel, it.steps = -1, keepDel, 0
	it.ranges = st.hasRanges()
}

func (it *iterState) valid() bool   { return it.cur >= 0 }
func (it *iterState) key() []byte   { return it.srcs[it.cur].key() }
func (it *iterState) value() []byte { return it.srcs[it.cur].value() }

func (it *iterState) seek(k []byte) {
	for i := range it.srcs {
		it.srcs[i].seekGE(k)
	}
	it.settle()
}

func (it *iterState) next() {
	it.skip()
	it.settle()
}

func (it *iterState) skip() {
	it.prev = append(it.prev[:0], it.key()...)
	for i := range it.srcs {
		if s := &it.srcs[i]; s.ok && bytes.Equal(s.key(), it.prev) {
			s.next()
		}
	}
}

func (it *iterState) settle() {
	for {
		it.cur = -1
		for i := range it.srcs {
			if s := &it.srcs[i]; s.ok && (it.cur < 0 || bytes.Compare(s.key(), it.srcs[it.cur].key()) < 0) {
				it.cur = i
			}
		}
		if it.cur < 0 {
			return
		}
		if it.ranges && it.hideRange() {
			continue
		}
		if it.keepDel || it.value()[0] != kindDel {
			return
		}
		it.skip()
	}
}

func (it *iterState) get(k []byte) ([]byte, bool) {
	for i := range it.srcs {
		it.probes++
		if v, ok := it.srcs[i].get(k); ok {
			if v[0] == kindDel {
				return nil, false
			}
			return v[1:], true
		}
		if _, ok := it.srcs[i].rdels.cover(k); ok {
			return nil, false
		}
	}
	return nil, false
}

// hideRange moves every source older than a range removal covering the current key past the
// range, and reports whether it did.
func (it *iterState) hideRange() bool {
	k := it.key()
	for j := 0; j <= it.cur; j++ {
		if j == it.cur && it.srcs[j].mem == nil {
			break
		}
		f, ok := it.srcs[j].rdels.cover(k)
		if !ok {
			continue
		}
		it.steps++
		first := j + 1
		if it.srcs[j].mem != nil {
			first = j
		}
		for s := first; s < len(it.srcs); s++ {
			if src := &it.srcs[s]; src.ok && bytes.Compare(src.key(), f.hi) < 0 {
				src.seekGE(f.hi)
			}
		}
		return true
	}
	return false
}

func (s *source) requery(bound []byte, strict bool) {
	s.ck, s.cv, s.cok = s.floor(bound, strict)
}

// floorCursor is floor keeping each source's candidate: a removal moves below it only the sources
// holding its key, and a range removal moves the sources older than it below the range's start.
func (it *iterState) floorCursor(k []byte) ([]byte, []byte, bool) {
	n := len(it.srcs)
	for i := range it.srcs {
		it.probes++
		it.srcs[i].requery(k, false)
	}
	for {
		it.steps++
		best := -1
		for i := range it.srcs {
			if s := &it.srcs[i]; s.cok && (best < 0 || bytes.Compare(s.ck, it.srcs[best].ck) > 0) {
				best = i
			}
		}
		if best < 0 {
			return nil, nil, false
		}
		bk, bv := it.srcs[best].ck, it.srcs[best].cv
		hidden := false
		if it.ranges {
			for j := 0; j <= best; j++ {
				if j == best && it.srcs[j].mem == nil {
					break
				}
				f, ok := it.srcs[j].rdels.cover(bk)
				if !ok {
					continue
				}
				first := j + 1
				if it.srcs[j].mem != nil {
					first = j
				}
				for s := first; s < n; s++ {
					if src := &it.srcs[s]; src.cok && bytes.Compare(src.ck, f.lo) >= 0 {
						it.probes++
						src.requery(f.lo, true)
					}
				}
				hidden = true
				break
			}
		}
		if hidden {
			continue
		}
		if bv[0] != kindDel {
			return bk, bv[1:], true
		}
		it.prev = append(it.prev[:0], bk...)
		for s := best; s < n; s++ {
			if src := &it.srcs[s]; src.cok && bytes.Equal(src.ck, it.prev) {
				it.probes++
				src.requery(it.prev, true)
			}
		}
	}
}

// floor finds the greatest key <= k that is not removed; a removal hides the keys it shadows,
// so the search steps below it.
func (it *iterState) floor(k []byte) ([]byte, []byte, bool) {
	bound, strict := k, false
	for {
		best := -1
		var bk, bv []byte
		for i := range it.srcs {
			it.probes++
			if fk, fv, ok := it.srcs[i].floor(bound, strict); ok && (best < 0 || bytes.Compare(fk, bk) > 0) {
				best, bk, bv = i, fk, fv
			}
		}
		if best < 0 {
			return nil, nil, false
		}
		it.steps++
		if bv[0] != kindDel {
			return bk, bv[1:], true
		}
		it.prev = append(it.prev[:0], bk...)
		bound, strict = it.prev, true
	}
}

// Iter walks a tree or view in key order, skipping removed keys. It is valid until the tree's next
// write when opened on a tree, and until the view closes when opened on a view.
type Iter struct {
	s iterState
}

// SeekGE moves to the first key >= k.
func (it *Iter) SeekGE(k []byte) bool {
	it.s.seek(k)
	return it.s.valid()
}

// Next moves to the next key.
func (it *Iter) Next() bool {
	if it.s.valid() {
		it.s.next()
	}
	return it.s.valid()
}

// Valid reports whether the iterator is at a key.
func (it *Iter) Valid() bool { return it.s.valid() }

// Key is the current key; it changes when the iterator moves.
func (it *Iter) Key() []byte { return it.s.key() }

// Value is the current value; it changes when the iterator moves.
func (it *Iter) Value() []byte { return it.s.value()[1:] }
