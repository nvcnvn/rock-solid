package lsm

import "bytes"

// source is one layer of a tree: a memtable at a root, or a run, with its range removals'
// fragments. A fragment hides every older layer, and its own layer's entries only when selfHide:
// a memtable whose entries under its fragments are all removals.
type source struct {
	mem      *memtable
	root     uint32
	froot    uint32
	run      *run
	mc       memCursor
	ri       runIter
	ok       bool
	empty    bool
	frags    bool
	selfHide bool
	out      bool
	fc       memCursor
	fr       runIter
	fpos     bool
	fok      bool
	ck       []byte
	cv       []byte
	cok      bool
}

func (s *source) setMem(m *memtable, root, froot uint32) {
	s.mem, s.root, s.froot, s.run, s.ok, s.fpos, s.out = m, root, froot, nil, false, false, false
	s.frags, s.selfHide = m.hasFrags(froot), !m.mixed
	p := m.pg.get(root)
	s.empty = p.kind() == leafPage && p.n() == 0
	s.mc.init(m, root)
}

func (s *source) setRun(r *run) {
	s.mem, s.run, s.ok, s.fpos, s.empty, s.out = nil, r, false, false, false, false
	s.frags, s.selfHide = len(r.frags) > 0, false
	s.ri.init(r.blocks)
	s.fr.init(r.frags)
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
	return s.run.blocks.get(&s.ri.it, k)
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

// fragFloor is the fragment with the greatest start <= b, or < b when strict, found by a search.
func (s *source) fragFloor(b []byte, strict bool) ([]byte, []byte, bool) {
	if s.run == nil {
		return s.mem.floor(s.froot, b, strict)
	}
	s.fpos = false
	if !s.fr.floor(b, strict) {
		return nil, nil, false
	}
	return s.fr.it.key, s.fr.it.val, true
}

// fragSeek moves the forward fragment cursor to the first fragment ending above k.
func (s *source) fragSeek(k []byte) {
	s.fpos = true
	if s.run == nil {
		s.fc.init(s.mem, s.froot)
		if s.fc.seekFloor(k) {
			s.fok = bytes.Compare(k, s.fc.value()) < 0 || s.fc.next()
		} else {
			s.fok = s.fc.valid
		}
		return
	}
	if s.fr.floor(k, false) {
		s.fok = bytes.Compare(k, s.fr.it.val) < 0 || s.fr.next()
	} else {
		s.fok = s.fr.seekGE(k)
	}
}

func (s *source) fragNext() {
	if s.run == nil {
		s.fok = s.fc.next()
	} else {
		s.fok = s.fr.next()
	}
}

func (s *source) fragStart() []byte {
	if s.run == nil {
		return s.fc.key()
	}
	return s.fr.it.key
}

func (s *source) fragEnd() []byte {
	if s.run == nil {
		return s.fc.value()
	}
	return s.fr.it.val
}

// fragCover returns the end of the fragment holding k, keeping the fragment until k passes its
// end; k must not fall below a key it was given since the last seek.
func (s *source) fragCover(k []byte) ([]byte, bool) {
	if !s.fpos {
		s.fragSeek(k)
	}
	for s.fok && bytes.Compare(s.fragEnd(), k) <= 0 {
		s.fragNext()
	}
	if s.fok && bytes.Compare(s.fragStart(), k) <= 0 {
		return s.fragEnd(), true
	}
	return nil, false
}

// state is what a read sees: the memtable, the one being flushed and the runs, newest first, and
// what the runs' filters hold.
type state struct {
	mem         *memtable
	memRoot     uint32
	memFRoot    uint32
	frozen      *memtable
	frozenRoot  uint32
	frozenFRoot uint32
	runs        []*run
	filter      FilterKeys
	prefix      func([]byte) int
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
	srcs[i].setMem(st.mem, st.memRoot, st.memFRoot)
	i++
	if st.frozen != nil {
		srcs[i].setMem(st.frozen, st.frozenRoot, st.frozenFRoot)
		i++
	}
	for _, r := range st.runs {
		srcs[i].setRun(r)
		i++
	}
	return srcs
}

// iterState merges sources in key order with a min-heap of the sources at a key, ordered by key
// and then age, as Pebble's mergingIter does; among equal keys the newest source's entry wins, and
// a fragment hides the older sources' entries in it. probes counts the searches of a layer's
// entries, filterProbes the probes of run filters, steps a floor's rounds. With keepDel, as a flush
// or merge reads, it leaves hidden entries for the caller to drop one by one, so that dropped, with
// the older entries each key shadows, and fragsDropped account for every entry and fragment of the
// sources. A prefix lookup leaves out the sources marked out, the runs whose filter rules its prefix
// out. With linear, a reference for tests, it compares every source at every step instead of
// keeping the heap.
type iterState struct {
	srcs         []source
	filter       FilterKeys
	prefix       func([]byte) int
	heap         []int
	cur          int
	linear       bool
	keepDel      bool
	ranges       bool
	bounded      bool
	upper        []byte
	inPrefix     bool
	pfx          []byte
	prev         []byte
	lo           []byte
	ulo          []byte
	uhi          []byte
	uok          bool
	united       int
	dropped      int
	fragsDropped int
	probes       int
	filterProbes int
	steps        int
}

func (it *iterState) reset(st *state, keepDel bool) {
	it.srcs, it.heap = st.sources(it.srcs), it.heap[:0]
	it.filter, it.prefix, it.inPrefix = st.filter, st.prefix, false
	it.cur, it.keepDel, it.bounded, it.uok = -1, keepDel, false, false
	it.dropped, it.fragsDropped = 0, 0
	it.ranges = false
	for i := range it.srcs {
		it.ranges = it.ranges || it.srcs[i].frags
	}
}

func (it *iterState) valid() bool   { return it.cur >= 0 }
func (it *iterState) key() []byte   { return it.srcs[it.cur].key() }
func (it *iterState) value() []byte { return it.srcs[it.cur].value() }

func (it *iterState) seek(k []byte) {
	for i := range it.srcs {
		s := &it.srcs[i]
		s.fpos = false
		if s.empty || s.out || s.run != nil && s.run.beyond(k) {
			s.ok = false
			continue
		}
		it.probes++
		s.seekGE(k)
	}
	it.order()
	it.settle()
}

func (it *iterState) less(a, b int) bool {
	c := bytes.Compare(it.srcs[a].key(), it.srcs[b].key())
	return c < 0 || c == 0 && a < b
}

// order rebuilds the heap from the sources positioned at a key.
func (it *iterState) order() {
	if it.linear {
		return
	}
	it.heap = it.heap[:0]
	for i := range it.srcs {
		if it.srcs[i].ok {
			it.heap = append(it.heap, i)
		}
	}
	for i := len(it.heap)/2 - 1; i >= 0; i-- {
		it.down(i)
	}
}

func (it *iterState) down(i int) {
	h := it.heap
	for {
		c := 2*i + 1
		if c >= len(h) {
			return
		}
		if r := c + 1; r < len(h) && it.less(h[r], h[c]) {
			c = r
		}
		if !it.less(h[c], h[i]) {
			return
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

// nextTop moves the source at the top of the heap to its next key.
func (it *iterState) nextTop() {
	s := &it.srcs[it.heap[0]]
	s.next()
	if !s.ok {
		last := len(it.heap) - 1
		it.heap[0] = it.heap[last]
		it.heap = it.heap[:last]
	}
	if len(it.heap) > 0 {
		it.down(0)
	}
}

// top sets cur to the source at the smallest key, the newest among equal keys, or -1.
func (it *iterState) top() {
	it.cur = -1
	if !it.linear {
		if len(it.heap) > 0 {
			it.cur = it.heap[0]
		}
		return
	}
	for i := range it.srcs {
		if s := &it.srcs[i]; s.ok && (it.cur < 0 || bytes.Compare(s.key(), it.srcs[it.cur].key()) < 0) {
			it.cur = i
		}
	}
}

func (it *iterState) next() {
	if n := it.skip(); it.keepDel {
		it.dropped += n - 1
	}
	it.settle()
}

// skip moves every source at the current key past it and returns how many it moved.
func (it *iterState) skip() int {
	it.prev = append(it.prev[:0], it.key()...)
	n := 0
	if !it.linear {
		for len(it.heap) > 0 && (n == 0 || bytes.Equal(it.srcs[it.heap[0]].key(), it.prev)) {
			it.nextTop()
			n++
		}
		return n
	}
	for i := range it.srcs {
		if s := &it.srcs[i]; s.ok && bytes.Equal(s.key(), it.prev) {
			s.next()
			n++
		}
	}
	return n
}

func (it *iterState) settle() {
	for {
		it.top()
		if it.cur < 0 {
			return
		}
		if it.bounded && bytes.Compare(it.key(), it.upper) >= 0 || it.inPrefix && !bytes.HasPrefix(it.key(), it.pfx) {
			it.cur = -1
			return
		}
		if it.ranges && !it.keepDel && it.hideRange() {
			continue
		}
		if it.keepDel || it.value()[0] != kindDel {
			return
		}
		it.skip()
	}
}

// hideRange moves the sources a fragment over the current key hides past its end, or out of
// the scan when the end is at or past the upper bound, and reports whether it did.
func (it *iterState) hideRange() bool {
	k := it.key()
	for j := 0; j <= it.cur; j++ {
		s := &it.srcs[j]
		if !s.frags || j == it.cur && !s.selfHide {
			continue
		}
		end, ok := s.fragCover(k)
		if !ok {
			continue
		}
		first := j + 1
		if s.selfHide {
			first = j
		}
		past := it.bounded && bytes.Compare(end, it.upper) >= 0
		for o := first; o < len(it.srcs); o++ {
			if src := &it.srcs[o]; src.ok && bytes.Compare(src.key(), end) < 0 {
				if past {
					src.ok = false
				} else {
					it.probes++
					src.seekGE(end)
				}
			}
		}
		it.order()
		return true
	}
	return false
}

// hidden reports whether a fragment of a source newer than the current one, or of the current
// one when it hides its own entries, holds the current key.
func (it *iterState) hidden() bool {
	if !it.ranges {
		return false
	}
	k := it.key()
	for j := 0; j <= it.cur; j++ {
		if s := &it.srcs[j]; s.frags && (j < it.cur || s.selfHide) {
			if _, ok := s.fragCover(k); ok {
				return true
			}
		}
	}
	return false
}

// covered reports whether a fragment of any source holds k; k must not fall below a key it was
// given since the last seek.
func (it *iterState) covered(k []byte) bool {
	if !it.ranges {
		return false
	}
	for i := range it.srcs {
		if s := &it.srcs[i]; s.frags {
			if _, ok := s.fragCover(k); ok {
				return true
			}
		}
	}
	return false
}

// fragsFromStart moves every source's fragments to their first, for nextFrag.
func (it *iterState) fragsFromStart() {
	it.uok = false
	for i := range it.srcs {
		if s := &it.srcs[i]; s.frags {
			s.fragSeek(nil)
		} else {
			s.fok = false
		}
	}
}

func (it *iterState) firstFrag() int {
	best := -1
	for i := range it.srcs {
		if s := &it.srcs[i]; s.fok && (best < 0 || bytes.Compare(s.fragStart(), it.srcs[best].fragStart()) < 0) {
			best = i
		}
	}
	return best
}

// nextFrag sets ulo and uhi to the next fragment of the union of the sources' fragments, those
// that overlap or abut merged into one, and united to how many it merged.
func (it *iterState) nextFrag() bool {
	b := it.firstFrag()
	if b < 0 {
		return false
	}
	it.ulo = append(it.ulo[:0], it.srcs[b].fragStart()...)
	it.uhi = append(it.uhi[:0], it.srcs[b].fragEnd()...)
	it.srcs[b].fragNext()
	for it.united = 1; ; it.united++ {
		b = it.firstFrag()
		if b < 0 || bytes.Compare(it.srcs[b].fragStart(), it.uhi) > 0 {
			return true
		}
		it.uhi = maxKey(it.uhi, it.srcs[b].fragEnd())
		it.srcs[b].fragNext()
	}
}

// pendingFrag reports whether a fragment of the union that a run in older holds a key in is
// left, setting ulo and uhi to it and counting the fragments it passes over in fragsDropped.
func (it *iterState) pendingFrag(older []runIter) bool {
	for !it.uok {
		if !it.nextFrag() {
			return false
		}
		if it.uok = heldIn(older, it.ulo, it.uhi); !it.uok {
			it.fragsDropped += it.united
		}
	}
	return true
}

// filterKey is what a lookup of k probes the filters with: k itself when they hold whole keys
// and whole is set, otherwise k's prefix when they hold prefixes, or nothing.
func (it *iterState) filterKey(k []byte, whole bool) ([]byte, bool) {
	if whole && it.filter != Prefixes {
		return k, true
	}
	if it.filter == WholeKeys {
		return nil, false
	}
	p := prefixOf(it.prefix, k)
	return p, len(p) > 0
}

// filterOut marks the runs whose filter rules out fk, the filter key of a prefix lookup.
func (it *iterState) filterOut(fk []byte, on bool) {
	h := bloomHash(fk)
	for i := range it.srcs {
		s := &it.srcs[i]
		s.out = on && s.run != nil && !it.mayHold(s.run, h)
	}
}

func (it *iterState) mayHold(r *run, h uint32) bool {
	it.filterProbes++
	return mayContain(r.filter, h)
}

func (it *iterState) get(k []byte) ([]byte, bool) {
	fk, on := it.filterKey(k, true)
	var h uint32
	if on {
		h = bloomHash(fk)
	}
	for i := range it.srcs {
		s := &it.srcs[i]
		if s.run != nil && s.run.outside(k) {
			continue
		}
		if !s.empty && !(on && s.run != nil && !it.mayHold(s.run, h)) {
			it.probes++
			if v, ok := s.get(k); ok {
				if v[0] == kindDel {
					return nil, false
				}
				return v[1:], true
			}
		}
		if s.frags {
			if _, end, ok := s.fragFloor(k, false); ok && bytes.Compare(k, end) < 0 {
				return nil, false
			}
		}
	}
	return nil, false
}

func (it *iterState) candidate(s *source, b []byte, strict bool) {
	if s.empty || s.out || s.run != nil && s.run.below(b, strict) {
		s.cok = false
		return
	}
	it.probes++
	s.ck, s.cv, s.cok = s.floor(b, strict)
}

// lower returns the bound below which older layers are searched: the start of s's fragment that
// covers the top of the keys at or below b, or below b when strict, if it has one.
func (it *iterState) lower(s *source, b []byte, strict bool) ([]byte, bool) {
	if !s.frags {
		return b, strict
	}
	lo, end, ok := s.fragFloor(b, strict)
	if !ok {
		return b, strict
	}
	if c := bytes.Compare(end, b); c > 0 || c == 0 && strict {
		return lo, true
	}
	return b, strict
}

// hider returns the newest layer whose fragment hides key k of layer best, and its start.
func (it *iterState) hider(best int, k []byte) (int, []byte) {
	for j := 0; j <= best; j++ {
		s := &it.srcs[j]
		if !s.frags || j == best && !s.selfHide {
			continue
		}
		if lo, end, ok := s.fragFloor(k, false); ok && bytes.Compare(k, end) < 0 {
			return j, lo
		}
	}
	return -1, nil
}

// floor finds the greatest key <= k that is not removed. Each layer keeps its candidate: a layer's
// fragment over the bound lowers the bound of the layers older than it before they are searched,
// a fragment over the best candidate moves the layers it hides below its start in one search
// each, and a removal moves the layers holding its key below it.
func (it *iterState) floor(k []byte) ([]byte, []byte, bool) {
	it.steps = 0
	bound, strict := k, false
	for i := range it.srcs {
		s := &it.srcs[i]
		if s.selfHide {
			bound, strict = it.lower(s, bound, strict)
		}
		it.candidate(s, bound, strict)
		if !s.selfHide {
			bound, strict = it.lower(s, bound, strict)
		}
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
		if it.inPrefix && !bytes.HasPrefix(bk, it.pfx) {
			return nil, nil, false
		}
		if j, lo := it.hider(best, bk); j >= 0 {
			it.lo = append(it.lo[:0], lo...)
			first := j + 1
			if it.srcs[j].selfHide {
				first = j
			}
			for o := first; o < len(it.srcs); o++ {
				if s := &it.srcs[o]; s.cok && bytes.Compare(s.ck, it.lo) >= 0 {
					it.candidate(s, it.lo, true)
				}
			}
			continue
		}
		if bv[0] != kindDel {
			return bk, bv[1:], true
		}
		it.prev = append(it.prev[:0], bk...)
		for o := best; o < len(it.srcs); o++ {
			if s := &it.srcs[o]; s.cok && bytes.Equal(s.ck, it.prev) {
				it.candidate(s, it.prev, true)
			}
		}
	}
}

// floorPrefix is floor bounded to k's prefix, leaving out the runs whose filter rules it out.
func (it *iterState) floorPrefix(k []byte) ([]byte, []byte, bool) {
	it.setPrefix(k)
	return it.floor(k)
}

// setPrefix bounds the next floor or scan to k's prefix and marks the runs whose filter rules it out.
func (it *iterState) setPrefix(k []byte) {
	it.pfx = append(it.pfx[:0], prefixOf(it.prefix, k)...)
	it.inPrefix = true
	fk, on := it.filterKey(k, false)
	it.filterOut(fk, on)
}

// Iter walks a tree or view in key order, skipping removed keys, until the tree's next write or the view closes.
type Iter struct {
	s iterState
}

// SetUpperBound makes the iterator stop before hi, nil for no bound, until it is opened again.
func (it *Iter) SetUpperBound(hi []byte) {
	it.s.bounded = hi != nil
	it.s.upper = append(it.s.upper[:0], hi...)
}

// SeekGE moves to the first key >= k.
func (it *Iter) SeekGE(k []byte) bool {
	if it.s.inPrefix {
		it.s.inPrefix = false
		it.s.filterOut(nil, false)
	}
	it.s.seek(k)
	return it.s.valid()
}

// SeekPrefixGE moves to the first key >= k with k's prefix, and the scan stops at the prefix's last key; with no prefix it is SeekGE.
func (it *Iter) SeekPrefixGE(k []byte) bool {
	it.s.setPrefix(k)
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
