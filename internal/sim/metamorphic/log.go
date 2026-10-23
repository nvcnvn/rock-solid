package metamorphic

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// Tree is the part of an lsm.Tree a checkpoint drives.
type Tree interface {
	BeginFlush() (bool, error)
	Frozen() bool
	CarriedRuns() []disklog.BlobRef
	Bounds() []disklog.MergeBound
	NextPiece(buf []byte) (int, bool, error)
	PieceDurable(ref disklog.BlobRef) error
	CheckpointDone() ([]disklog.BlobRef, error)
	FlushFailed() []disklog.BlobRef
}

// Log stands in for the disk log under one tree: it keeps every live piece a checkpoint writes,
// and the newest manifest's runs, merge bounds and replay point, counted in the index's writes
// before it. A restore reading a piece reported dead fails.
type Log struct {
	piece    int
	blobs    map[disklog.BlobRef][]byte
	lsn      uint64
	roots    []disklog.BlobRef
	bounds   []disklog.MergeBound
	replay   int
	frozenAt int
	// Flushes counts the checkpoints whose manifest is durable, Failed those that failed.
	Flushes, Failed int
}

// NewLog makes an empty log for a tree of pieces of at most piece bytes.
func NewLog(piece int) *Log {
	return &Log{piece: piece, blobs: map[disklog.BlobRef][]byte{}}
}

// Checkpoint flushes t, whose index has taken writes writes, calling between once each piece is
// built. With limit above 0 the checkpoint fails once limit pieces are durable, unless it finished
// with fewer.
func (l *Log) Checkpoint(t Tree, writes, limit int, between func()) error {
	froze, err := t.BeginFlush()
	if err != nil {
		return err
	}
	replay := writes
	if froze {
		l.frozenAt = writes
	} else if t.Frozen() {
		replay = l.frozenAt
	}
	roots, bounds := slices.Clone(t.CarriedRuns()), slices.Clone(t.Bounds())
	buf := make([]byte, l.piece)
	for n := 0; ; n++ {
		if limit > 0 && n == limit {
			for _, r := range t.FlushFailed() {
				delete(l.blobs, r)
			}
			l.Failed++
			return nil
		}
		size, root, err := t.NextPiece(buf)
		if err != nil {
			return err
		}
		if size == 0 {
			break
		}
		l.lsn++
		ref := disklog.BlobRef{Segment: 1, Block: uint32(l.lsn), LSN: l.lsn}
		l.blobs[ref] = bytes.Clone(buf[:size])
		if between != nil {
			between()
		}
		if err := t.PieceDurable(ref); err != nil {
			return err
		}
		if root {
			roots = append(roots, ref)
		}
	}
	dead, err := t.CheckpointDone()
	if err != nil {
		return err
	}
	for _, r := range dead {
		delete(l.blobs, r)
	}
	l.roots, l.bounds, l.replay = roots, bounds, replay
	l.Flushes++
	return nil
}

// Replay is how many of the index's writes the newest manifest holds; a restart replays the rest.
func (l *Log) Replay() int { return l.replay }

// Restore hands load each run the newest manifest names, its pieces in order and its root last,
// finding a root's earlier pieces with rootRefs, or loadBounded a run a merge bound names, its
// pieces from the first the bound leaves live.
func (l *Log) Restore(rootRefs func([]byte, []disklog.BlobRef) ([]disklog.BlobRef, error), load func(disklog.BlobRef, [][]byte) error, loadBounded func(disklog.MergeBound, [][]byte) error) error {
	for _, root := range l.roots {
		refs, err := rootRefs(l.blobs[root], nil)
		if err != nil {
			return fmt.Errorf("root refs: %w", err)
		}
		b := slices.IndexFunc(l.bounds, func(b disklog.MergeBound) bool { return b.Run == root })
		if b >= 0 {
			refs = refs[min(int(l.bounds[b].Dead), len(refs)):]
		}
		var pieces [][]byte
		for _, r := range append(refs, root) {
			p, ok := l.blobs[r]
			if !ok {
				return fmt.Errorf("the manifest names dead piece %+v", r)
			}
			pieces = append(pieces, p)
		}
		if b >= 0 {
			err = loadBounded(l.bounds[b], pieces)
		} else {
			err = load(root, pieces)
		}
		if err != nil {
			return fmt.Errorf("load run: %w", err)
		}
	}
	return nil
}
