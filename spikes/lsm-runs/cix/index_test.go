package cix

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	lsmx "github.com/nvcnvn/rock-solid/spikes/lsm-runs/lsmx"
)

type flushTree interface {
	BeginFlush() (bool, error)
	NextPiece(buf []byte) (int, bool, error)
	PieceDurable(ref disklog.BlobRef) error
	CheckpointDone() ([]disklog.BlobRef, error)
}

func checkpoint(t *testing.T, tr flushTree, next *uint64) {
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1<<16)
	for {
		n, _, err := tr.NextPiece(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		*next++
		if err := tr.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(*next), LSN: *next}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.CheckpointDone(); err != nil {
		t.Fatal(err)
	}
}

// The spike's index with range removals maps every block as the real index does, and passes its
// invariant check, under random writes, punches, freezes, whole rewrites, flushes and compactions.
func TestSameAsChunkstore(t *testing.T) {
	for _, cfg := range []lsmx.Config{
		{MaxPiece: 1 << 16, CursorFloor: true},
		{MaxPiece: 1 << 16, CursorFloor: true, SmallRun: 1, DensityMerge: 0.5, DensityMinRemovals: 8},
	} {
		for seed := range uint64(6) {
			t.Run(fmt.Sprintf("%+v/%d", cfg, seed), func(t *testing.T) {
				rt, err := lsm.New(lsm.Config{MaxPiece: 1 << 16})
				if err != nil {
					t.Fatal(err)
				}
				xt, err := lsmx.New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				real, x := chunkstore.NewIndex(rt), NewIndex(xt)
				x.RangeTrim = 2
				r := rand.New(rand.NewPCG(seed, 3))
				lba := core.LBA(1)
				var rn, xn uint64
				ver := uint64(1)
				for step := range 1500 {
					c := core.ChunkID{Volume: 1, Inode: 2, Index: uint32(r.IntN(4))}
					var e disklog.Entry
					switch k := r.IntN(100); {
					case k < 60:
						first := r.IntN(core.BlocksPerChunk)
						e = disklog.WriteEntry(1, c, ver, uint16(first), uint16(1+r.IntN(min(64, core.BlocksPerChunk-first))))
					case k < 70:
						first := r.IntN(core.BlocksPerChunk)
						e = disklog.PunchEntry(1, c, ver, uint16(first), uint16(1+r.IntN(core.BlocksPerChunk-first)))
					case k < 75:
						e = disklog.FreezeEntry(1, c, ver, uint64(step))
					default:
						first := r.IntN(4) * 256
						e = disklog.WriteEntry(1, c, ver, uint16(first), uint16(256*(1+r.IntN(4-first/256))))
					}
					ver++
					ev := &disklog.Event{Kind: disklog.Durable, LBA: lba, HeaderBlocks: 1, Entries: []disklog.Entry{e}}
					ev.Payloads = []uint64{disklog.PayloadOffset(ev.Entries, 0)}
					lba += 1 + core.LBA(e.Count)
					if err := real.Apply(ev); err != nil {
						t.Fatal(err)
					}
					if err := x.Apply(ev); err != nil {
						t.Fatal(err)
					}
					if r.IntN(40) == 0 {
						checkpoint(t, rt, &rn)
						checkpoint(t, xt, &xn)
						for c := rt.PickCompaction(); c != nil; c = rt.PickCompaction() {
							c.Run()
							if err := rt.Install(c); err != nil {
								t.Fatal(err)
							}
						}
						for c := xt.PickCompaction(); c != nil; c = xt.PickCompaction() {
							c.Run()
							if err := xt.Install(c); err != nil {
								t.Fatal(err)
							}
						}
					}
					if step%50 == 49 {
						if err := x.Check(); err != nil {
							t.Fatal(err)
						}
						for i := range 4 {
							c := core.ChunkID{Volume: 1, Inode: 2, Index: uint32(i)}
							for b := range core.BlocksPerChunk {
								re, rok := real.Lookup(1, c, uint16(b))
								xe, xok := x.Lookup(1, c, uint16(b))
								if rok != xok || re.First != xe.First || re.Count != xe.Count || re.LBA != xe.LBA || re.Version != xe.Version {
									t.Fatalf("step %d chunk %d block %d: real %+v %v, spike %+v %v", step, i, b, re, rok, xe, xok)
								}
							}
						}
					}
				}
			})
		}
	}
}
