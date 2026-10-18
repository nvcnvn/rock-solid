package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type memtable interface {
	put(k, v []byte)
	get(k []byte) ([]byte, bool)
	scan(prefix []byte, fn func(k, v []byte) bool)
	len() int
	stats() string
}

// sliceTree is variant 1: a B-tree of []byte entries, `key_len u16 | key | value` in one
// allocation each, as the Rust MemTree's Entry is. One pointer per entry for the collector.
type sliceTree struct{ t btree[[]byte] }

func entryKey(e []byte) []byte { return e[2 : 2+binary.LittleEndian.Uint16(e)] }

func newSliceTree() *sliceTree {
	s := &sliceTree{}
	s.t.keyOf = entryKey
	s.t.cmp = func(e []byte, k []byte) int { return bytes.Compare(entryKey(e), k) }
	return s
}

func (s *sliceTree) put(k, v []byte) {
	e := make([]byte, 2+len(k)+len(v))
	binary.LittleEndian.PutUint16(e, uint16(len(k)))
	copy(e[2:], k)
	copy(e[2+len(k):], v)
	s.t.put(e)
}

func (s *sliceTree) get(k []byte) ([]byte, bool) {
	e, ok := s.t.get(k)
	if !ok {
		return nil, false
	}
	return e[2+len(k):], true
}

func (s *sliceTree) scan(prefix []byte, fn func(k, v []byte) bool) {
	s.t.scan(prefix, func(e []byte) bool {
		k := entryKey(e)
		return fn(k, e[2+len(k):])
	})
}

func (s *sliceTree) len() int      { return s.t.len }
func (s *sliceTree) stats() string { return fmt.Sprintf("nodes=%d", s.t.nodes) }

// arenaTree is variant 2: the same B-tree of uint64 handles (slab << 32 | offset) into 64 MiB
// []byte slabs holding `key_len u16 | value_len u16 | key | value`. Only the nodes and slabs are
// pointers. An overwrite appends the new entry and leaves the old one as a hole; the spike does not
// reclaim holes and reports their bytes.
const slabSize = 64 << 20

type arenaTree struct {
	t     btree[uint64]
	slabs [][]byte
	used  int
	holes int
}

func newArenaTree() *arenaTree {
	a := &arenaTree{}
	a.t.keyOf = a.key
	a.t.cmp = func(h uint64, k []byte) int { return bytes.Compare(a.key(h), k) }
	return a
}

func (a *arenaTree) entry(h uint64) []byte { return a.slabs[h>>32][uint32(h):] }

func (a *arenaTree) key(h uint64) []byte {
	e := a.entry(h)
	return e[4 : 4+binary.LittleEndian.Uint16(e)]
}

func (a *arenaTree) value(h uint64) []byte {
	e := a.entry(h)
	kl, vl := binary.LittleEndian.Uint16(e), binary.LittleEndian.Uint16(e[2:])
	return e[4+uint32(kl) : 4+uint32(kl)+uint32(vl)]
}

func (a *arenaTree) put(k, v []byte) {
	n := 4 + len(k) + len(v)
	if len(a.slabs) == 0 || a.used+n > slabSize {
		a.slabs = append(a.slabs, make([]byte, slabSize))
		a.used = 0
	}
	s := a.slabs[len(a.slabs)-1]
	binary.LittleEndian.PutUint16(s[a.used:], uint16(len(k)))
	binary.LittleEndian.PutUint16(s[a.used+2:], uint16(len(v)))
	copy(s[a.used+4:], k)
	copy(s[a.used+4+len(k):], v)
	h := uint64(len(a.slabs)-1)<<32 | uint64(a.used)
	a.used += n
	if old, replaced := a.t.put(h); replaced {
		a.holes += 4 + len(a.key(old)) + len(a.value(old))
	}
}

func (a *arenaTree) get(k []byte) ([]byte, bool) {
	h, ok := a.t.get(k)
	if !ok {
		return nil, false
	}
	return a.value(h), true
}

func (a *arenaTree) scan(prefix []byte, fn func(k, v []byte) bool) {
	a.t.scan(prefix, func(h uint64) bool { return fn(a.key(h), a.value(h)) })
}

func (a *arenaTree) len() int { return a.t.len }
func (a *arenaTree) stats() string {
	return fmt.Sprintf("nodes=%d slabs=%d holes=%d", a.t.nodes, len(a.slabs), a.holes)
}
