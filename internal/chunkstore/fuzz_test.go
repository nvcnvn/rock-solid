package chunkstore

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

// FuzzDecodeKey feeds arbitrary bytes as an extent's key: decoding never panics, and a key it
// accepts encodes back to the same bytes.
func FuzzDecodeKey(f *testing.F) {
	f.Add(AppendKey(nil, Key{Pool: 1, Chunk: chunk(7), Version: 3, First: 9}))
	f.Add(AppendKey(nil, Key{Pool: RecordPool, Chunk: chunk(7)}))
	f.Add(make([]byte, KeySize))
	f.Fuzz(func(t *testing.T, b []byte) {
		k, err := DecodeKey(b)
		if err != nil {
			return
		}
		if k.Pool == RecordPool {
			t.Fatalf("accepted a key in the reserved pool: %x", b)
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
		disklog.Entry{Kind: disklog.EntryWrite, Pool: 1, Chunk: chunk(3), Version: 2, First: 7, Count: 1},
	)); err != nil {
		f.Fatal(err)
	}
	var seed []byte
	for _, kv := range dumpIndex(x) {
		seed = append(seed, byte(len(kv[0])))
		seed = append(seed, kv[0]...)
		seed = append(seed, byte(len(kv[1])))
		seed = append(seed, kv[1]...)
	}
	f.Add(seed)
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
		x := NewIndex(tree, slotSegmentBlocks)
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
