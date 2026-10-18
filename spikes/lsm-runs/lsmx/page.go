package lsmx

import (
	"bytes"
	"encoding/binary"
)

// A page is 4 KiB: a 16-byte header, then a slot array of u16 entry offsets growing up, and
// entries growing down from the end. A leaf entry is `key_len u16 | value_len u16 | key | value`,
// the value starting with its kind byte; an inner entry is `key_len u16 | child u32 | key`, the
// child holding keys >= key. The header's left is an inner page's child for keys below its first
// entry. Every inner key equals the smallest key of its child's subtree, since memtables never
// drop a key: a removal is an entry too.
const (
	pageSize  = 4096
	pageHdr   = 16
	pageCap   = pageSize - pageHdr
	slabPages = 256
	leafPage  = 1
	innerPage = 2
	maxDepth  = 40
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
func (p *page) slot(i int) int     { return int(binary.LittleEndian.Uint16(p[pageHdr+2*i:])) }
func (p *page) setSlot(i, off int) { binary.LittleEndian.PutUint16(p[pageHdr+2*i:], uint16(off)) }
func (p *page) room() int          { return p.free() - pageHdr - 2*p.n() }
func (p *page) used() int          { return pageSize - p.free() - p.dead() + 2*p.n() }
func (p *page) keyLen(off int) int { return int(binary.LittleEndian.Uint16(p[off:])) }
func (p *page) child(i int) uint32 { return binary.LittleEndian.Uint32(p[p.slot(i)+2:]) }
func (p *page) setChild(i int, c uint32) {
	binary.LittleEndian.PutUint32(p[p.slot(i)+2:], c)
}

func (p *page) key(i int) []byte {
	off := p.slot(i)
	if p.kind() == leafPage {
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
	if p.kind() == leafPage {
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
	clear(p[:pageHdr])
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
	copy(p[pageHdr+2*(i+1):pageHdr+2*(n+1)], p[pageHdr+2*i:pageHdr+2*n])
	p.setSlot(i, off)
	p.setN(n + 1)
}

func (p *page) remove(i int) {
	p.setDead(p.dead() + len(p.entry(i)))
	n := p.n()
	copy(p[pageHdr+2*i:], p[pageHdr+2*(i+1):pageHdr+2*n])
	p.setN(n - 1)
}

// rebuild rewrites p from entries, which must not point into p, keeping its kind, gen and left.
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

// pages is a store of pages in pointer-free slabs, addressed by page number.
type pages struct {
	slabs    [][]page
	next     uint32
	freeList []uint32
	inUse    int
}

func (s *pages) get(id uint32) *page { return &s.slabs[id/slabPages][id%slabPages] }

func (s *pages) alloc(kind byte, gen uint32) uint32 {
	var id uint32
	if n := len(s.freeList); n > 0 {
		id = s.freeList[n-1]
		s.freeList = s.freeList[:n-1]
	} else {
		if int(s.next/slabPages) == len(s.slabs) {
			s.slabs = append(s.slabs, make([]page, slabPages))
		}
		id = s.next
		s.next++
	}
	s.inUse++
	s.get(id).reset(kind, gen)
	return id
}

func (s *pages) release(id uint32) {
	p := s.get(id)
	if p.kind() == 0 {
		panic("lsm: page freed twice")
	}
	p[0] = 0
	s.freeList = append(s.freeList, id)
	s.inUse--
}

func insertAt(list [][]byte, i int, e []byte) [][]byte {
	list = append(list, nil)
	copy(list[i+1:], list[i:])
	list[i] = e
	return list
}

func entriesSize(list [][]byte) int {
	n := 0
	for _, e := range list {
		n += len(e) + 2
	}
	return n
}
