//go:build goexperiment.arenas

package main

import (
	"arena"
	"encoding/binary"
)

// goArenaTree is variant 1 with every entry, node and item array allocated from one Go arena
// (GOEXPERIMENT=arenas). An arena chunk that holds pointers is scanned like any other object
// (runtime/arena.go), so this tests whether arenas cut the collector's marking or only freeing.
// Overwritten entries stay in the arena until it is freed; the spike never frees it.
type goArenaTree struct {
	sliceTree
	a *arena.Arena
}

func init() {
	extraVariants["slices-goarena"] = func() memtable { return newGoArenaTree() }
}

func newGoArenaTree() *goArenaTree {
	g := &goArenaTree{sliceTree: *newSliceTree(), a: arena.NewArena()}
	g.t.alloc = func(leaf bool) *node[[]byte] {
		n := arena.New[node[[]byte]](g.a)
		n.items = arena.MakeSlice[[]byte](g.a, 0, maxItems)
		if !leaf {
			n.children = arena.MakeSlice[*node[[]byte]](g.a, 0, maxItems+1)
		}
		return n
	}
	return g
}

func (g *goArenaTree) put(k, v []byte) {
	e := arena.MakeSlice[byte](g.a, 2+len(k)+len(v), 2+len(k)+len(v))
	binary.LittleEndian.PutUint16(e, uint16(len(k)))
	copy(e[2:], k)
	copy(e[2+len(k):], v)
	g.t.put(e)
}
