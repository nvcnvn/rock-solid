package lsmd

import "bytes"

// plantFirstKeyOnly is a planted bug for the model test: a block whose first key a fragment covers is
// dropped whole.
var plantFirstKeyOnly bool

// DropStats counts what DropCovered did.
type DropStats struct {
	Calls        int
	RunsChanged  int
	BlocksDrop   int
	BytesDropped int
	BytesKept    int
}

// lastKey is the last key of a block.
func lastKey(b []byte, it *blockIter) []byte {
	it.init(b)
	var last []byte
	for ok := it.first(); ok; ok = it.advance() {
		last = append(last[:0], it.key...)
	}
	return last
}

// DropCovered drops from every run the blocks that a newer layer's fragments cover whole, as
// Pebble's delete-only compactions drop the tables a range deletion covers. A run keeps its place
// in age order; one that lost blocks is written again by the next checkpoint, and one left with
// neither blocks nor fragments leaves the tree. It runs only while no flush or compaction does.
func (t *Tree) DropCovered() {
	if t.fl != nil || t.compact != nil {
		return
	}
	t.Drop.Calls++
	union := t.mem.rdels
	if t.frozen != nil {
		for _, f := range t.frozen.rdels {
			union = union.with(f.lo, f.hi)
		}
	}
	var it blockIter
	out := t.runs[:0:0]
	for _, r := range t.runs {
		if len(union) == 0 {
			out = append(out, r)
			union = r.rdels
			continue
		}
		var kept [][]byte
		dropped := 0
		for _, b := range r.blocks {
			f, ok := union.cover(firstKey(b))
			if ok && (plantFirstKeyOnly || bytes.Compare(lastKey(b, &it), f.hi) < 0) {
				dropped += len(b)
				t.Drop.BlocksDrop++
				continue
			}
			kept = append(kept, b)
		}
		if dropped > 0 {
			t.Drop.RunsChanged++
			t.Drop.BytesDropped += dropped
			nr := &run{seq: r.seq, rdels: r.rdels}
			nr.bytes = nr.rdels.bytes()
			for _, b := range kept {
				nr.addBlock(b)
			}
			t.Drop.BytesKept += nr.bytes
			if len(nr.blocks) > 0 || len(nr.rdels) > 0 {
				out = append(out, nr)
			}
		} else {
			out = append(out, r)
		}
		for _, f := range r.rdels {
			union = union.with(f.lo, f.hi)
		}
	}
	t.runs = out
}
