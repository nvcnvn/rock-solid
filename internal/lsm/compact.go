package lsm

import (
	"bytes"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Compaction is size-tiered by the bucket rule of Cassandra's SizeTieredCompactionStrategy
// (getBuckets): a run joins a bucket when its size is within half to one and a half times the
// bucket's average, or when both are below minRunBytes, and the bucket of at least minMerge runs
// with the smallest average merges first. Entries carry no sequence number, so a merge takes only
// runs adjacent in age, as RocksDB's universal compaction does: buckets form along the run list,
// newest first, never over runs sorted by size.
const (
	minMerge    = 4
	maxMerge    = 32
	minRunBytes = 1 << 20
)

// Compaction merges adjacent runs into one. Run touches nothing the tree's owner changes, so it
// may run on a worker goroutine; the owner then installs the result.
type Compaction struct {
	inputs []*run
	blocks [][][]byte
	older  []*run
	seq    uint64
	limit  int
	out    *run
}

func similar(size, avg, small int) bool {
	return (2*size > avg && 2*size < 3*avg) || (size < small && avg < small)
}

// buckets cuts sizes, newest first, into buckets of adjacent runs, each [start, end); runs below
// small bucket together.
func buckets(sizes []int, small int) [][2]int {
	var out [][2]int
	for i := 0; i < len(sizes); {
		j, sum := i+1, sizes[i]
		for j < len(sizes) && j-i < maxMerge && similar(sizes[j], sum/(j-i), small) {
			sum += sizes[j]
			j++
		}
		out = append(out, [2]int{i, j})
		i = j
	}
	return out
}

// PickCompaction returns the next compaction due, or nil when none is or one is running.
func (t *Tree) PickCompaction() *Compaction {
	if t.compact != nil {
		return nil
	}
	sizes := make([]int, len(t.runs))
	for i, r := range t.runs {
		sizes[i] = r.bytes
	}
	best, bestAvg := [2]int{}, -1
	for _, b := range buckets(sizes, minRunBytes) {
		n := b[1] - b[0]
		if n < minMerge {
			continue
		}
		sum := 0
		for _, s := range sizes[b[0]:b[1]] {
			sum += s
		}
		if sum > t.maxRunBytes() {
			continue
		}
		if avg := sum / n; bestAvg < 0 || avg < bestAvg {
			best, bestAvg = b, avg
		}
	}
	if bestAvg < 0 {
		return nil
	}
	c := &Compaction{
		inputs: slices.Clone(t.runs[best[0]:best[1]]),
		seq:    t.runs[best[0]].seq,
		limit:  t.cfg.MaxPiece - pieceOverhead,
	}
	for _, r := range c.inputs {
		c.blocks = append(c.blocks, slices.Clone(r.blocks))
	}
	for _, r := range t.runs[best[1]:] {
		c.older = append(c.older, &run{blocks: slices.Clone(r.blocks)})
	}
	t.compact = c
	return c
}

// maxRunBytes is the most a run may hold for its root to list every piece, a piece spare.
func (t *Tree) maxRunBytes() int {
	pieces := (t.cfg.MaxPiece - pieceOverhead - emptyBlockSize) / disklog.BlobRefSize
	return (pieces - 1) * (t.cfg.MaxPiece - pieceOverhead) / 2
}

// Run merges the inputs, the newest entry of each key winning; a removal is dropped unless a run
// older than the inputs holds its key.
func (c *Compaction) Run() {
	it := iterState{srcs: make([]source, len(c.inputs)), cur: -1, keepDel: true}
	for i, blocks := range c.blocks {
		it.srcs[i].run = &run{blocks: blocks}
		it.srcs[i].ri.init(it.srcs[i].run)
	}
	it.seek(nil)
	out := &run{seq: c.seq}
	buf := make([]byte, 0, c.limit)
	var w blockWriter
	var probe blockIter
	for it.valid() {
		w.reset(buf)
		fillBlock(&w, &it, c.limit, c.older, &probe)
		if w.n > 0 {
			out.addBlock(bytes.Clone(w.finish()))
		}
	}
	c.out = out
}

// Install swaps the compaction's result in for its inputs. A view opened before keeps reading the
// inputs; the next flush writes the result, and the inputs die once a manifest no longer names them.
func (t *Tree) Install(c *Compaction) error {
	if c == nil || t.compact != c {
		return ErrStale
	}
	t.compact = nil
	if c.out == nil {
		return ErrStale
	}
	i := slices.Index(t.runs, c.inputs[0])
	if i < 0 || i+len(c.inputs) > len(t.runs) || !slices.Equal(t.runs[i:i+len(c.inputs)], c.inputs) {
		return ErrStale
	}
	runs := make([]*run, 0, len(t.runs)-len(c.inputs)+1)
	runs = append(runs, t.runs[:i]...)
	if len(c.out.blocks) > 0 {
		runs = append(runs, c.out)
	}
	t.runs = append(runs, t.runs[i+len(c.inputs):]...)
	return nil
}
