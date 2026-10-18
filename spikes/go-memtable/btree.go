package main

import "bytes"

// A B-tree of items of type T, as github.com/google/btree builds it (degree 32, so 31 to 63 items
// per node). The two variants that use it differ only in what an item is: a []byte holding the
// entry (one pointer per entry) or a uint64 handle into an arena (no pointer per entry).
const (
	degree   = 32
	maxItems = 2*degree - 1
)

type node[T any] struct {
	items    []T
	children []*node[T]
}

type btree[T any] struct {
	root  *node[T]
	cmp   func(item T, key []byte) int
	keyOf func(item T) []byte
	len   int
	nodes int
}

func (t *btree[T]) newNode(leaf bool) *node[T] {
	t.nodes++
	n := &node[T]{items: make([]T, 0, maxItems)}
	if !leaf {
		n.children = make([]*node[T], 0, maxItems+1)
	}
	return n
}

func (t *btree[T]) search(n *node[T], k []byte) (int, bool) {
	lo, hi := 0, len(n.items)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if t.cmp(n.items[m], k) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(n.items) && t.cmp(n.items[lo], k) == 0
}

func (t *btree[T]) get(k []byte) (T, bool) {
	for n := t.root; n != nil; {
		i, found := t.search(n, k)
		if found {
			return n.items[i], true
		}
		if n.children == nil {
			break
		}
		n = n.children[i]
	}
	var zero T
	return zero, false
}

// put stores item, returning the item it replaced.
func (t *btree[T]) put(item T) (T, bool) {
	if t.root == nil {
		t.root = t.newNode(true)
	}
	if len(t.root.items) == maxItems {
		old := t.root
		t.root = t.newNode(false)
		t.root.children = append(t.root.children, old)
		t.split(t.root, 0)
	}
	old, replaced := t.insert(t.root, item, t.keyOf(item))
	if !replaced {
		t.len++
	}
	return old, replaced
}

func (t *btree[T]) split(parent *node[T], i int) {
	c := parent.children[i]
	mid := degree - 1
	right := t.newNode(c.children == nil)
	right.items = append(right.items, c.items[mid+1:]...)
	median := c.items[mid]
	clear(c.items[mid:])
	c.items = c.items[:mid]
	if c.children != nil {
		right.children = append(right.children, c.children[mid+1:]...)
		clear(c.children[mid+1:])
		c.children = c.children[:mid+1]
	}
	parent.items = insertAt(parent.items, i, median)
	parent.children = insertAt(parent.children, i+1, right)
}

func (t *btree[T]) insert(n *node[T], item T, k []byte) (T, bool) {
	for {
		i, found := t.search(n, k)
		if found {
			old := n.items[i]
			n.items[i] = item
			return old, true
		}
		if n.children == nil {
			n.items = insertAt(n.items, i, item)
			var zero T
			return zero, false
		}
		if len(n.children[i].items) == maxItems {
			t.split(n, i)
			switch c := t.cmp(n.items[i], k); {
			case c == 0:
				old := n.items[i]
				n.items[i] = item
				return old, true
			case c < 0:
				i++
			}
		}
		n = n.children[i]
	}
}

// scan calls fn for each item whose key starts with prefix, in key order, until fn returns false.
func (t *btree[T]) scan(prefix []byte, fn func(T) bool) {
	if t.root != nil {
		t.ascend(t.root, prefix, fn)
	}
}

func (t *btree[T]) ascend(n *node[T], prefix []byte, fn func(T) bool) bool {
	i, _ := t.search(n, prefix)
	for ; i <= len(n.items); i++ {
		if n.children != nil && !t.ascend(n.children[i], prefix, fn) {
			return false
		}
		if i == len(n.items) {
			break
		}
		if !bytes.HasPrefix(t.keyOf(n.items[i]), prefix) || !fn(n.items[i]) {
			return false
		}
	}
	return true
}

func insertAt[S ~[]E, E any](s S, i int, v E) S {
	var zero E
	s = append(s, zero)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}
