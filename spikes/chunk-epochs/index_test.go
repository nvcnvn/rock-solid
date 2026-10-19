package chunkepochs

import (
	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/spikes/chunk-epochs/absent"
)

// index is what the tests need of the chunk index, so the same scenario runs against
// internal/chunkstore at HEAD and against the absent-freeze prototype.
type index interface {
	Apply(ev *disklog.Event) error
	Chunk(c core.ChunkID) (version, epoch uint64, ok bool)
	Lookup(c core.ChunkID, b uint16) (block, bool)
	LookupAt(c core.ChunkID, b uint16, epoch uint64) (block, bool)
	Move(c core.ChunkID, first uint16, from, to, header core.LBA) int
	Check() error
	Tree() *lsm.Tree
}

type block struct {
	Version uint64
	LBA     core.LBA
}

const pool = 1

func newTree() *lsm.Tree {
	t, err := lsm.New(lsm.Config{MaxPiece: 1 << 20})
	if err != nil {
		panic(err)
	}
	return t
}

type current struct{ x *chunkstore.Index }

func newCurrent(t *lsm.Tree) index {
	if t == nil {
		t = newTree()
	}
	return current{chunkstore.NewIndex(t)}
}

func (i current) Apply(ev *disklog.Event) error { return i.x.Apply(ev) }
func (i current) Chunk(c core.ChunkID) (uint64, uint64, bool) {
	ch, ok := i.x.Chunk(pool, c)
	return ch.Version, ch.Epoch, ok
}
func (i current) Lookup(c core.ChunkID, b uint16) (block, bool) {
	e, ok := i.x.Lookup(pool, c, b)
	return block{e.Version, e.BlockLBA(b)}, ok
}
func (i current) LookupAt(c core.ChunkID, b uint16, epoch uint64) (block, bool) {
	e, ok := i.x.LookupAt(pool, c, b, epoch)
	return block{e.Version, e.BlockLBA(b)}, ok
}
func (i current) Move(c core.ChunkID, first uint16, from, to, header core.LBA) int {
	return i.x.Move(pool, c, first, 1, from, to, header)
}
func (i current) Check() error    { return i.x.Check() }
func (i current) Tree() *lsm.Tree { return i.x.Tree() }

type proto struct{ x *absent.Index }

func newProto(t *lsm.Tree) index {
	if t == nil {
		t = newTree()
	}
	return proto{absent.NewIndex(t)}
}

func (i proto) Apply(ev *disklog.Event) error { return i.x.Apply(ev) }
func (i proto) Chunk(c core.ChunkID) (uint64, uint64, bool) {
	ch, ok := i.x.Chunk(pool, c)
	return ch.Version, ch.Epoch, ok
}
func (i proto) Lookup(c core.ChunkID, b uint16) (block, bool) {
	e, ok := i.x.Lookup(pool, c, b)
	return block{e.Version, e.BlockLBA(b)}, ok
}
func (i proto) LookupAt(c core.ChunkID, b uint16, epoch uint64) (block, bool) {
	e, ok := i.x.LookupAt(pool, c, b, epoch)
	return block{e.Version, e.BlockLBA(b)}, ok
}
func (i proto) Move(c core.ChunkID, first uint16, from, to, header core.LBA) int {
	return i.x.Move(pool, c, first, 1, from, to, header)
}
func (i proto) Check() error    { return i.x.Check() }
func (i proto) Tree() *lsm.Tree { return i.x.Tree() }

// event is a durable record at lba with one header block; payloads are the entries' offsets in
// the whole record, so an event can carry a subset of a record's entries.
func event(lba core.LBA, entries []disklog.Entry, payloads []uint64) *disklog.Event {
	return &disklog.Event{Kind: disklog.Durable, LBA: lba, HeaderBlocks: 1, Entries: entries, Payloads: payloads}
}

func record(lba core.LBA, entries ...disklog.Entry) *disklog.Event {
	var p []uint64
	for i := range entries {
		p = append(p, disklog.PayloadOffset(entries, i))
	}
	return event(lba, entries, p)
}

func dump(t *lsm.Tree) [][2]string {
	var it lsm.Iter
	t.Iter(&it)
	var out [][2]string
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	return out
}

// cloneTree copies every entry of t into a fresh tree: the runs a restart loads.
func cloneTree(t *lsm.Tree) *lsm.Tree {
	n := newTree()
	for _, kv := range dump(t) {
		if err := n.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			panic(err)
		}
	}
	return n
}

func chunk(i int) core.ChunkID { return core.ChunkID{Volume: 1, Inode: 0, Index: uint32(i)} }
