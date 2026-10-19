package lsmd

import "encoding/binary"

// A stored value starts with its kind, so a removal is an entry that shadows older runs.
const (
	kindDel byte = 0
	kindSet byte = 1
)

type stalePage struct{ id, born, at uint32 }

// memtable is a copy-on-write B+-tree of pages. A view pins the root it saw, and once one is
// open a write copies each page it changes that a view can reach. A page copied away is freed
// once no open view could reach it.
type memtable struct {
	rdels   rangeSet
	pg      *pages
	root    uint32
	count   int
	gen     uint32
	views   []uint32
	stale   []stalePage
	refs    int
	scratch [3]page
	ebuf    []byte
	ibuf    []byte
	sep     []byte
	elist   [][]byte
	slist   [][]byte
}

func newMemtable(pg *pages) *memtable {
	m := &memtable{pg: pg, gen: 1, refs: 1}
	m.root = pg.alloc(leafPage, m.gen)
	return m
}

func (m *memtable) writable(id uint32) uint32 {
	p := m.pg.get(id)
	if len(m.views) == 0 || p.gen() > m.views[len(m.views)-1] {
		return id
	}
	nid := m.pg.alloc(p.kind(), m.gen)
	np := m.pg.get(nid)
	*np = *p
	np.setGen(m.gen)
	m.stale = append(m.stale, stalePage{id: id, born: p.gen(), at: m.gen})
	return nid
}

func (m *memtable) leafEntry(k []byte, kind byte, v []byte) []byte {
	e := m.ebuf[:0]
	e = binary.LittleEndian.AppendUint16(e, uint16(len(k)))
	e = binary.LittleEndian.AppendUint16(e, uint16(len(v)+1))
	e = append(e, k...)
	e = append(e, kind)
	e = append(e, v...)
	m.ebuf = e
	return e
}

func (m *memtable) innerEntry(k []byte, child uint32) []byte {
	e := m.ibuf[:0]
	e = binary.LittleEndian.AppendUint16(e, uint16(len(k)))
	e = binary.LittleEndian.AppendUint32(e, child)
	e = append(e, k...)
	m.ibuf = e
	return e
}

func entryKey(leafEntry []byte) []byte {
	return leafEntry[4 : 4+int(binary.LittleEndian.Uint16(leafEntry))]
}

// put stores k with value v of the given kind, replacing any entry for k.
func (m *memtable) put(k []byte, kind byte, v []byte) {
	e := m.leafEntry(k, kind, v)
	m.root = m.writable(m.root)
	sep, right, split := m.insert(m.root, k, e)
	if split {
		r := m.pg.alloc(innerPage, m.gen)
		p := m.pg.get(r)
		p.setLeft(m.root)
		p.add(0, m.innerEntry(sep, right))
		m.root = r
	}
}

// insert places leaf entry e for k into the writable subtree id; a split returns the new right
// page and its smallest key.
func (m *memtable) insert(id uint32, k, e []byte) ([]byte, uint32, bool) {
	p := m.pg.get(id)
	if p.kind() == leafPage {
		return m.insertLeaf(id, false, 0, id, k, e)
	}
	ci := p.childFor(k)
	child := m.writable(p.childAt(ci))
	p.setChildAt(ci, child)
	var sep []byte
	var right uint32
	var split bool
	if m.pg.get(child).kind() == leafPage {
		sep, right, split = m.insertLeaf(id, true, ci, child, k, e)
	} else {
		sep, right, split = m.insert(child, k, e)
	}
	if !split {
		return nil, 0, false
	}
	return m.place(id, ci+1, m.innerEntry(sep, right))
}

func (m *memtable) insertLeaf(parent uint32, hasParent bool, ci int, id uint32, k, e []byte) ([]byte, uint32, bool) {
	p := m.pg.get(id)
	i, found := p.lowerBound(k)
	if found {
		if old := p.entry(i); len(old) == len(e) {
			copy(old, e)
			return nil, 0, false
		}
		p.remove(i)
	} else {
		m.count++
	}
	if p.room() >= len(e)+2 {
		p.add(i, e)
		return nil, 0, false
	}
	m.scratch[0] = *p
	all := insertAt(m.scratch[0].entries(m.elist[:0]), i, e)
	m.elist = all
	if entriesSize(all) <= pageCap {
		p.rebuild(all)
		return nil, 0, false
	}
	if hasParent && m.redistribute(parent, ci, id, all) {
		return nil, 0, false
	}
	return m.split(id, all, i == len(all)-1)
}

// place puts inner entry e at slot i of page id, compacting or splitting it.
func (m *memtable) place(id uint32, i int, e []byte) ([]byte, uint32, bool) {
	p := m.pg.get(id)
	if p.room() >= len(e)+2 {
		p.add(i, e)
		return nil, 0, false
	}
	m.scratch[0] = *p
	all := insertAt(m.scratch[0].entries(m.elist[:0]), i, e)
	m.elist = all
	if entriesSize(all) <= pageCap {
		p.rebuild(all)
		return nil, 0, false
	}
	return m.split(id, all, false)
}

// splitPoint is where to cut list so that both halves fit a page, as near the middle as it can,
// with list[cut:] the right half, or list[cut] moving up and list[cut+1:] the right half when
// skipMid. It reports false when no cut fits; one always does when list is one full page plus an
// entry, since entries are at most half a page.
func splitPoint(list [][]byte, skipMid bool) (int, bool) {
	total := entriesSize(list)
	cut, left := 0, 0
	for cut < len(list)-1 && left+len(list[cut])+2 <= total/2 {
		left += len(list[cut]) + 2
		cut++
	}
	right := func() int {
		r := total - left
		if skipMid {
			r -= len(list[cut]) + 2
		}
		return r
	}
	for cut < len(list)-1 && (cut == 0 || right() > pageCap) {
		left += len(list[cut]) + 2
		cut++
	}
	return cut, cut > 0 && left <= pageCap && right() <= pageCap
}

func (m *memtable) split(id uint32, all [][]byte, appending bool) ([]byte, uint32, bool) {
	p := m.pg.get(id)
	leaf := p.kind() == leafPage
	cut, ok := splitPoint(all, !leaf)
	if !ok {
		panic("lsm: no split of an overfull page fits")
	}
	if leaf && appending {
		// Appending past a leaf's last key, as a restore in key order does: keep the leaf full
		// and start the next one, instead of leaving every page half empty.
		cut = len(all) - 1
	}
	rid := m.pg.alloc(p.kind(), m.gen)
	r := m.pg.get(rid)
	if leaf {
		m.sep = append(m.sep[:0], entryKey(all[cut])...)
		p.rebuild(all[:cut])
		r.rebuild(all[cut:])
	} else {
		mid := all[cut]
		m.sep = append(m.sep[:0], mid[6:6+int(binary.LittleEndian.Uint16(mid))]...)
		r.setLeft(binary.LittleEndian.Uint32(mid[2:]))
		p.rebuild(all[:cut])
		r.rebuild(all[cut+1:])
	}
	return m.sep, rid, true
}

// redistribute moves entries between the full leaf at child ci of parent and a neighbour with
// room, so a leaf filled in create order does not split half empty.
func (m *memtable) redistribute(parent uint32, ci int, id uint32, all [][]byte) bool {
	pp := m.pg.get(parent)
	leftOK, rightOK := ci >= 0, ci+1 < pp.n()
	if leftOK && rightOK {
		if m.pg.get(pp.childAt(ci-1)).used() <= m.pg.get(pp.child(ci+1)).used() {
			rightOK = false
		} else {
			leftOK = false
		}
	}
	var sibIdx, sepIdx int
	switch {
	case leftOK:
		sibIdx, sepIdx = ci-1, ci
	case rightOK:
		sibIdx, sepIdx = ci+1, ci+1
	default:
		return false
	}
	sib := pp.childAt(sibIdx)
	m.scratch[1] = *m.pg.get(sib)
	comb := m.slist[:0]
	if leftOK {
		comb = m.scratch[1].entries(comb)
		comb = append(comb, all...)
	} else {
		comb = append(comb, all...)
		comb = m.scratch[1].entries(comb)
	}
	m.slist = comb
	if entriesSize(comb) > 2*pageCap*7/8 {
		return false
	}
	cut, ok := splitPoint(comb, false)
	if !ok {
		return false
	}
	m.sep = append(m.sep[:0], entryKey(comb[cut])...)
	if !pp.canSetKey(sepIdx, len(m.sep)) {
		return false
	}
	sid := m.writable(sib)
	pp.setChildAt(sibIdx, sid)
	leftID, rightID := sid, id
	if rightOK {
		leftID, rightID = id, sid
	}
	m.pg.get(leftID).rebuild(comb[:cut])
	m.pg.get(rightID).rebuild(comb[cut:])
	m.setKey(parent, sepIdx, m.sep, rightID)
	return true
}

func (p *page) canSetKey(i, keyLen int) bool {
	old, ne := len(p.entry(i)), 6+keyLen
	return ne <= old || p.room() >= ne || p.used()-old+ne <= pageCap
}

// setKey replaces inner entry i of page id with key k and child; canSetKey has checked it fits.
func (m *memtable) setKey(id uint32, i int, k []byte, child uint32) {
	p := m.pg.get(id)
	ne := m.innerEntry(k, child)
	old := p.entry(i)
	switch {
	case len(ne) <= len(old):
		copy(p[p.slot(i):], ne)
		p.setDead(p.dead() + len(old) - len(ne))
	case p.room() >= len(ne):
		p.remove(i)
		p.add(i, ne)
	default:
		m.scratch[2] = *p
		list := m.scratch[2].entries(m.elist[:0])
		list[i] = ne
		m.elist = list
		p.rebuild(list)
	}
}

// get returns k's stored value, its kind byte first.
func (m *memtable) get(root uint32, k []byte) ([]byte, bool) {
	p := m.pg.get(root)
	for p.kind() == innerPage {
		p = m.pg.get(p.childAt(p.childFor(k)))
	}
	i, found := p.lowerBound(k)
	if !found {
		return nil, false
	}
	return p.value(i), true
}

// floor returns the greatest entry with key <= k, or < k when strict.
func (m *memtable) floor(root uint32, k []byte, strict bool) ([]byte, []byte, bool) {
	id, alt, hasAlt := root, uint32(0), false
	p := m.pg.get(id)
	for p.kind() == innerPage {
		ci := p.childFor(k)
		if ci >= 0 {
			alt, hasAlt = p.childAt(ci-1), true
		}
		id = p.childAt(ci)
		p = m.pg.get(id)
	}
	i, found := p.lowerBound(k)
	if !found || strict {
		i--
	}
	if i < 0 {
		if !hasAlt {
			return nil, nil, false
		}
		p = m.pg.get(alt)
		for p.kind() == innerPage {
			p = m.pg.get(p.childAt(p.n() - 1))
		}
		i = p.n() - 1
	}
	return p.key(i), p.value(i), true
}

func (m *memtable) openView() uint32 {
	g := m.gen
	m.views = append(m.views, g)
	m.gen++
	m.refs++
	return g
}

func (m *memtable) closeView(g uint32) {
	for i, v := range m.views {
		if v == g {
			m.views = append(m.views[:i], m.views[i+1:]...)
			break
		}
	}
	keep := m.stale[:0]
	for _, s := range m.stale {
		if m.seen(s) {
			keep = append(keep, s)
		} else {
			m.pg.release(s.id)
		}
	}
	m.stale = keep
	m.unref()
}

func (m *memtable) seen(s stalePage) bool {
	for _, g := range m.views {
		if g >= s.born && g < s.at {
			return true
		}
	}
	return false
}

func (m *memtable) unref() {
	m.refs--
	if m.refs > 0 {
		return
	}
	m.releaseTree(m.root)
	for _, s := range m.stale {
		m.pg.release(s.id)
	}
	m.stale = nil
}

func (m *memtable) releaseTree(id uint32) {
	p := m.pg.get(id)
	if p.kind() == innerPage {
		m.releaseTree(p.left())
		for i := range p.n() {
			m.releaseTree(p.child(i))
		}
	}
	m.pg.release(id)
}

// memCursor walks a memtable's entries in key order from the root it was opened on, which must
// not change under it: a frozen memtable's, or a view's.
type memCursor struct {
	pg    *pages
	root  uint32
	depth int
	ids   [maxDepth]uint32
	idx   [maxDepth]int
	valid bool
}

func (c *memCursor) init(m *memtable, root uint32) { *c = memCursor{pg: m.pg, root: root} }

func (c *memCursor) seekGE(k []byte) bool {
	c.depth = 0
	id := c.root
	for {
		if c.depth == maxDepth {
			panic("lsm: memtable deeper than maxDepth")
		}
		p := c.pg.get(id)
		c.ids[c.depth] = id
		if p.kind() == leafPage {
			i, _ := p.lowerBound(k)
			c.idx[c.depth] = i
			c.valid = true
			if i >= p.n() {
				return c.advance()
			}
			return true
		}
		ci := p.childFor(k)
		c.idx[c.depth] = ci
		c.depth++
		id = p.childAt(ci)
	}
}

func (c *memCursor) next() bool {
	if !c.valid {
		return false
	}
	c.idx[c.depth]++
	if c.idx[c.depth] < c.pg.get(c.ids[c.depth]).n() {
		return true
	}
	return c.advance()
}

// advance moves past the end of the current leaf to the first entry of the next one.
func (c *memCursor) advance() bool {
	for d := c.depth - 1; d >= 0; d-- {
		p := c.pg.get(c.ids[d])
		if c.idx[d]+1 >= p.n() {
			continue
		}
		c.idx[d]++
		id := p.child(c.idx[d])
		c.depth = d + 1
		for {
			q := c.pg.get(id)
			c.ids[c.depth] = id
			if q.kind() == leafPage {
				c.idx[c.depth] = 0
				return true
			}
			c.idx[c.depth] = -1
			c.depth++
			id = q.left()
		}
	}
	c.valid = false
	return false
}

func (c *memCursor) key() []byte   { return c.pg.get(c.ids[c.depth]).key(c.idx[c.depth]) }
func (c *memCursor) value() []byte { return c.pg.get(c.ids[c.depth]).value(c.idx[c.depth]) }
