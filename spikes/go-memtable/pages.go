package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// pageTree is variant 3: a copy-on-write B+-tree of 4 KiB pages held in pointer-free slabs, as
// LMDB and redb lay out theirs. Pages refer to each other by page number, never by Go pointer, so
// the collector sees one pointer per 64 MiB slab.
//
// Page layout: a 16-byte header, then a slot array of u16 entry offsets growing up, and entries
// growing down from the end. A leaf entry is `key_len u16 | value_len u16 | key | value`; an inner
// entry is `key_len u16 | child u32 | key`, the child holding keys >= key. The header's `left` is
// an inner page's child for keys below its first entry.
const (
	pageSize  = 4096
	hdrSize   = 16
	slabPages = 16384
	kindLeaf  = 1
	kindInner = 2
)

type page [pageSize]byte

func (p *page) kind() byte         { return p[0] }
func (p *page) n() int             { return int(binary.LittleEndian.Uint16(p[2:])) }
func (p *page) setN(n int)         { binary.LittleEndian.PutUint16(p[2:], uint16(n)) }
func (p *page) free() int          { return int(binary.LittleEndian.Uint16(p[4:])) }
func (p *page) setFree(f int)      { binary.LittleEndian.PutUint16(p[4:], uint16(f)) }
func (p *page) dead() int          { return int(binary.LittleEndian.Uint16(p[6:])) }
func (p *page) setDead(d int)      { binary.LittleEndian.PutUint16(p[6:], uint16(d)) }
func (p *page) gen() uint32        { return binary.LittleEndian.Uint32(p[8:]) }
func (p *page) setGen(g uint32)    { binary.LittleEndian.PutUint32(p[8:], g) }
func (p *page) left() uint32       { return binary.LittleEndian.Uint32(p[12:]) }
func (p *page) setLeft(c uint32)   { binary.LittleEndian.PutUint32(p[12:], c) }
func (p *page) slot(i int) int     { return int(binary.LittleEndian.Uint16(p[hdrSize+2*i:])) }
func (p *page) setSlot(i, off int) { binary.LittleEndian.PutUint16(p[hdrSize+2*i:], uint16(off)) }
func (p *page) room() int          { return p.free() - hdrSize - 2*p.n() }
func (p *page) keyLen(off int) int { return int(binary.LittleEndian.Uint16(p[off:])) }
func (p *page) child(i int) uint32 { return binary.LittleEndian.Uint32(p[p.slot(i)+2:]) }
func (p *page) setChild(i int, c uint32) {
	binary.LittleEndian.PutUint32(p[p.slot(i)+2:], c)
}

func (p *page) key(i int) []byte {
	off := p.slot(i)
	if p.kind() == kindLeaf {
		return p[off+4 : off+4+p.keyLen(off)]
	}
	return p[off+6 : off+6+p.keyLen(off)]
}

func (p *page) value(i int) []byte {
	off := p.slot(i)
	kl, vl := p.keyLen(off), int(binary.LittleEndian.Uint16(p[off+2:]))
	return p[off+4+kl : off+4+kl+vl]
}

func (p *page) entry(i int) []byte {
	off := p.slot(i)
	if p.kind() == kindLeaf {
		return p[off : off+4+p.keyLen(off)+int(binary.LittleEndian.Uint16(p[off+2:]))]
	}
	return p[off : off+6+p.keyLen(off)]
}

// lowerBound is the first slot whose key is >= k.
func (p *page) lowerBound(k []byte) (int, bool) {
	lo, hi := 0, p.n()
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(p.key(m), k) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < p.n() && bytes.Equal(p.key(lo), k)
}

// childFor is the index of the inner entry whose child holds k, or -1 for left.
func (p *page) childFor(k []byte) int {
	lo, hi := 0, p.n()
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(p.key(m), k) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}

func (p *page) childAt(i int) uint32 {
	if i < 0 {
		return p.left()
	}
	return p.child(i)
}

func (p *page) setChildAt(i int, c uint32) {
	if i < 0 {
		p.setLeft(c)
	} else {
		p.setChild(i, c)
	}
}

func (p *page) reset(kind byte, gen uint32) {
	clear(p[:hdrSize])
	p[0] = kind
	p.setFree(pageSize)
	p.setGen(gen)
}

// add writes raw entry e at slot i; the caller has checked room.
func (p *page) add(i int, e []byte) {
	off := p.free() - len(e)
	copy(p[off:], e)
	p.setFree(off)
	n := p.n()
	copy(p[hdrSize+2*(i+1):hdrSize+2*(n+1)], p[hdrSize+2*i:hdrSize+2*n])
	p.setSlot(i, off)
	p.setN(n + 1)
}

func (p *page) remove(i int) {
	p.setDead(p.dead() + len(p.entry(i)))
	n := p.n()
	copy(p[hdrSize+2*i:], p[hdrSize+2*(i+1):hdrSize+2*n])
	p.setN(n - 1)
}

// rebuild rewrites p from entries (which must not point into p) and keeps its header fields.
func (p *page) rebuild(entries [][]byte) {
	left := p.left()
	p.reset(p.kind(), p.gen())
	p.setLeft(left)
	for i, e := range entries {
		p.add(i, e)
	}
}

func (p *page) entries(dst [][]byte) [][]byte {
	for i := range p.n() {
		dst = append(dst, p.entry(i))
	}
	return dst
}

type pageTree struct {
	slabs      [][]page
	next       uint32
	freeList   []uint32
	root       uint32
	count      int
	gen        uint32
	snapshot   bool
	frozenRoot uint32
	superseded []uint32
	inUse      int
	copies     int
	scratch    page
	ebuf       []byte
	elist      [][]byte
}

func newPageTree() *pageTree {
	t := &pageTree{gen: 1}
	t.root = t.alloc(kindLeaf)
	return t
}

func (t *pageTree) pg(id uint32) *page { return &t.slabs[id/slabPages][id%slabPages] }

func (t *pageTree) alloc(kind byte) uint32 {
	var id uint32
	if n := len(t.freeList); n > 0 {
		id = t.freeList[n-1]
		t.freeList = t.freeList[:n-1]
	} else {
		if int(t.next/slabPages) == len(t.slabs) {
			t.slabs = append(t.slabs, make([]page, slabPages))
		}
		id = t.next
		t.next++
	}
	t.inUse++
	t.pg(id).reset(kind, t.gen)
	return id
}

// writable returns id, or a copy of it if it belongs to the snapshot's frozen view.
func (t *pageTree) writable(id uint32) uint32 {
	if !t.snapshot || t.pg(id).gen() == t.gen {
		return id
	}
	nid := t.alloc(0)
	*t.pg(nid) = *t.pg(id)
	t.pg(nid).setGen(t.gen)
	t.superseded = append(t.superseded, id)
	t.copies++
	return nid
}

func (t *pageTree) put(k, v []byte) {
	nid, sep, right, split := t.insert(t.root, k, v)
	t.root = nid
	if split {
		r := t.alloc(kindInner)
		p := t.pg(r)
		p.setLeft(nid)
		p.add(0, t.innerEntry(sep, right))
		t.root = r
	}
}

func (t *pageTree) leafEntry(k, v []byte) []byte {
	e := t.ebuf[:0]
	e = binary.LittleEndian.AppendUint16(e, uint16(len(k)))
	e = binary.LittleEndian.AppendUint16(e, uint16(len(v)))
	e = append(append(e, k...), v...)
	t.ebuf = e
	return e
}

func (t *pageTree) innerEntry(k []byte, child uint32) []byte {
	e := make([]byte, 0, 6+len(k))
	e = binary.LittleEndian.AppendUint16(e, uint16(len(k)))
	e = binary.LittleEndian.AppendUint32(e, child)
	return append(e, k...)
}

// place puts e at slot i of page id, compacting or splitting it. A split returns the new right
// page and the first key it holds.
func (t *pageTree) place(id uint32, i int, e []byte) ([]byte, uint32, bool) {
	p := t.pg(id)
	if p.room() >= len(e)+2 {
		p.add(i, e)
		return nil, 0, false
	}
	t.scratch = *p
	all := t.scratch.entries(t.elist[:0])
	all = insertAt(all, i, e)
	t.elist = all
	total := 0
	for _, x := range all {
		total += len(x) + 2
	}
	if p.dead() > 0 && hdrSize+total <= pageSize {
		p.rebuild(all)
		return nil, 0, false
	}
	m, acc := 0, 0
	for acc < total/2 {
		acc += len(all[m]) + 2
		m++
	}
	if p.kind() == kindLeaf && i == len(all)-1 {
		// Appending past a leaf's last key, as a restore in key order does: keep the leaf full
		// and start the next one, instead of leaving every page half empty.
		m = i
	}
	rid := t.alloc(p.kind())
	r := t.pg(rid)
	var sep []byte
	if p.kind() == kindLeaf {
		p.rebuild(all[:m])
		r.rebuild(all[m:])
		sep = bytes.Clone(r.key(0))
	} else {
		mid := all[m]
		kl := int(binary.LittleEndian.Uint16(mid))
		sep = bytes.Clone(mid[6 : 6+kl])
		r.setLeft(binary.LittleEndian.Uint32(mid[2:]))
		p.rebuild(all[:m])
		r.rebuild(all[m+1:])
	}
	return sep, rid, true
}

func (t *pageTree) insert(id uint32, k, v []byte) (uint32, []byte, uint32, bool) {
	id = t.writable(id)
	p := t.pg(id)
	if p.kind() == kindLeaf {
		i, found := p.lowerBound(k)
		if found {
			if old := p.value(i); len(old) == len(v) {
				copy(old, v)
				return id, nil, 0, false
			}
			p.remove(i)
		} else {
			t.count++
		}
		sep, r, split := t.place(id, i, t.leafEntry(k, v))
		return id, sep, r, split
	}
	ci := p.childFor(k)
	child := p.childAt(ci)
	nc, sep, r, split := t.insert(child, k, v)
	p = t.pg(id)
	if nc != child {
		p.setChildAt(ci, nc)
	}
	if !split {
		return id, nil, 0, false
	}
	s2, r2, split2 := t.place(id, ci+1, t.innerEntry(sep, r))
	return id, s2, r2, split2
}

func (t *pageTree) get(k []byte) ([]byte, bool) {
	p := t.pg(t.root)
	for p.kind() == kindInner {
		p = t.pg(p.childAt(p.childFor(k)))
	}
	i, found := p.lowerBound(k)
	if !found {
		return nil, false
	}
	return p.value(i), true
}

func (t *pageTree) scan(prefix []byte, fn func(k, v []byte) bool) { t.ascend(t.root, prefix, fn) }

func (t *pageTree) ascend(id uint32, prefix []byte, fn func(k, v []byte) bool) bool {
	p := t.pg(id)
	if p.kind() == kindLeaf {
		i, _ := p.lowerBound(prefix)
		for ; i < p.n(); i++ {
			if !bytes.HasPrefix(p.key(i), prefix) || !fn(p.key(i), p.value(i)) {
				return false
			}
		}
		return true
	}
	for c := p.childFor(prefix); c < p.n(); c++ {
		if !t.ascend(p.childAt(c), prefix, fn) {
			return false
		}
	}
	return true
}

// beginSnapshot freezes the current tree; every later write copies the pages it changes.
func (t *pageTree) beginSnapshot() {
	t.snapshot = true
	t.frozenRoot = t.root
	t.gen++
}

func (t *pageTree) scanFrozen(fn func(k, v []byte) bool) { t.ascend(t.frozenRoot, nil, fn) }

// endSnapshot frees the pages only the frozen view still used.
func (t *pageTree) endSnapshot() {
	for _, id := range t.superseded {
		t.pg(id)[0] = 0
		t.freeList = append(t.freeList, id)
		t.inUse--
	}
	t.superseded = t.superseded[:0]
	t.snapshot = false
}

func (t *pageTree) len() int { return t.count }

func (t *pageTree) stats() string {
	used := 0
	for id := range t.next {
		p := t.pg(id)
		if p.kind() != 0 {
			used += pageSize - p.room() - p.dead()
		}
	}
	return fmt.Sprintf("pages=%d fill=%.3f copies=%d", t.inUse, float64(used)/float64(t.inUse*pageSize), t.copies)
}
