package chunkstore

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

// FuzzDecodeKey feeds arbitrary bytes as an extent's key: decoding never panics, and a key it
// accepts encodes back to the same bytes.
func FuzzDecodeKey(f *testing.F) {
	f.Add(AppendKey(nil, Key{Pool: 1, Chunk: chunk(7), Version: 3, First: 9}))
	f.Add(AppendKey(nil, Key{Pool: RecordPool, Chunk: chunk(7)}))
	f.Add(AppendKey(nil, Key{Pool: DecisionPool, Chunk: chunk(7)}))
	f.Add(make([]byte, KeySize))
	f.Fuzz(func(t *testing.T, b []byte) {
		k, err := DecodeKey(b)
		if err != nil {
			return
		}
		if reserved(k.Pool) {
			t.Fatalf("accepted a key in a reserved pool: %x", b)
		}
		if got := AppendKey(nil, k); !bytes.Equal(got, b) {
			t.Fatalf("key %x decodes to %+v, which encodes to %x", b, k, got)
		}
	})
}

// FuzzDecodeExtent feeds arbitrary bytes as an extent's key and value: decoding never panics, and
// an extent it accepts encodes back to the same value.
func FuzzDecodeExtent(f *testing.F) {
	key := AppendKey(nil, Key{Pool: 1, Chunk: chunk(7), First: 9})
	f.Add(key, appendExtent(nil, Extent{First: 9, Count: 2, Version: 3, LBA: 100, Header: 99}))
	f.Add(key, appendExtent(nil, Extent{First: 9, Count: 1024})[:extentSize-1])
	f.Fuzz(func(t *testing.T, k, v []byte) {
		e, ok := decodeExtent(k, v)
		if !ok {
			return
		}
		if got := appendExtent(nil, e); !bytes.Equal(got, v) {
			t.Fatalf("value %x decodes to %+v, which encodes to %x", v, e, got)
		}
		if len(k) != KeySize {
			t.Fatalf("accepted a %d-byte key", len(k))
		}
	})
}

// FuzzDecodeChunk feeds arbitrary bytes as a chunk's entry: a value of the entry's size decodes
// and encodes back to the same bytes.
func FuzzDecodeChunk(f *testing.F) {
	f.Add(appendChunk(nil, Chunk{Version: 1<<32 | 2, Epoch: 3, Whole: 1<<32 | 1, Listed: 2<<32 | 5, ListedIn: 4}))
	f.Fuzz(func(t *testing.T, v []byte) {
		if len(v) != chunkValueSize {
			return
		}
		if got := appendChunk(nil, decodeChunk(v)); !bytes.Equal(got, v) {
			t.Fatalf("entry %x decodes to %+v, which encodes to %x", v, decodeChunk(v), got)
		}
	})
}

// FuzzCheck fills a tree with arbitrary entries, each a length byte and a key then a length byte
// and a value: Check returns an error or nil and never panics, and when it passes a Reader reads
// blocks, ascending and back, live and at an epoch, as Lookup and LookupAt do.
func FuzzCheck(f *testing.F) {
	x := slotIndex(f)
	if err := x.Apply(record(100,
		disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(1), Version: 1, First: 0, Count: 2},
		disklog.Entry{Kind: disklog.EntryFreeze, Pool: 1, Chunk: chunk(1), Version: 1, Epoch: 5},
		disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(1), Version: 2, First: 1, Count: 1},
		disklog.Entry{Kind: disklog.EntryFreeze, Pool: 1, Chunk: chunk(2), Version: 0, Epoch: 5},
		disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(2), Version: 1, First: 0, Count: 1},
		disklog.SlotEntry(1, chunk(3), 1, 2, 1),
		disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(3), Version: 2, First: 7, Count: 1, Last: true},
		disklog.MissingEntry(1, chunk(2), 1<<32|1, 2),
		disklog.MissingEntry(1, chunk(4), 0, 2),
		disklog.DecisionEntry(1, 0, 1, 2),
	)); err != nil {
		f.Fatal(err)
	}
	var seed, past []byte
	for _, kv := range dumpIndex(x) {
		k, v := []byte(kv[0]), []byte(kv[1])
		seed = append(append(append(append(seed, byte(len(k))), k...), byte(len(v))), v...)
		if e, ok := decodeExtent(k, v); ok {
			if _, slot := x.slotOf(e); slot {
				v = binary.LittleEndian.AppendUint64(slices.Clone(v[:extentSize-8]), 1<<62)
			}
		}
		past = append(append(append(append(past, byte(len(k))), k...), byte(len(v))), v...)
	}
	f.Add(seed)
	f.Add(past)
	f.Fuzz(func(t *testing.T, b []byte) {
		tree, err := lsm.New(TreeConfig(lsm.Config{MaxPiece: 1 << 20, Check: true}))
		if err != nil {
			t.Fatal(err)
		}
		for len(b) > 0 {
			k, rest, ok := field(b)
			if !ok {
				break
			}
			v, rest, ok := field(rest)
			if !ok {
				break
			}
			b = rest
			if len(k) > 0 {
				if err := tree.Put(k, v); err != nil {
					t.Fatal(err)
				}
			}
		}
		x := NewIndex(tree, slotSegmentBlocks, slotSegments)
		if x.Check() == nil {
			var r Reader
			for _, c := range []uint32{1, 2} {
				x.Open(&r, 1, chunk(c))
				for _, b := range []uint16{0, 1, 0} {
					e, ok := x.Lookup(1, chunk(c), b)
					if re, rok := r.Lookup(b); re != e || rok != ok {
						t.Fatalf("chunk %d block %d: reader %+v %v, lookup %+v %v", c, b, re, rok, e, ok)
					}
				}
				x.OpenAt(&r, 1, chunk(c), 5)
				for _, b := range []uint16{0, 1} {
					e, ok := x.LookupAt(1, chunk(c), b, 5)
					if re, rok := r.Lookup(b); re != e || rok != ok {
						t.Fatalf("chunk %d block %d at epoch 5: reader %+v %v, lookup %+v %v", c, b, re, rok, e, ok)
					}
				}
			}
		}
	})
}

func field(b []byte) (f, rest []byte, ok bool) {
	if len(b) == 0 || int(b[0]) > len(b)-1 {
		return nil, nil, false
	}
	n := int(b[0])
	return b[1 : 1+n], b[1+n:], true
}

// FuzzDecodeBlockMap feeds arbitrary bytes as a recovery buffer: decoding never panics, and a map it
// accepts writes no block it punches, puts back to the same first block and counts the buffer's
// blocks.
func FuzzDecodeBlockMap(f *testing.F) {
	var m blockMap
	m.data.setRange(2, 4)
	m.holes.set(9)
	buf := make([]byte, 4*4096)
	m.put(buf)
	f.Add(buf)
	f.Add(buf[:4096])
	f.Add(make([]byte, 4096))
	f.Fuzz(func(t *testing.T, b []byte) {
		var m blockMap
		if decodeBlockMap(b, &m) != nil {
			return
		}
		for w := range blockWords {
			if m.data[w]&m.holes[w] != 0 {
				t.Fatalf("block map %+v writes and punches a block", m)
			}
		}
		again := make([]byte, 4096)
		m.put(again)
		if !bytes.Equal(again, b[:4096]) || len(b) != (1+m.data.count())*4096 {
			t.Fatalf("block map %+v of %d bytes does not put back", m, len(b))
		}
	})
}
