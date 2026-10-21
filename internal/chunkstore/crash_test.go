package chunkstore

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

type blockKey struct {
	c core.ChunkID
	b uint16
}

type frozenBlocks struct {
	version uint64
	epoch   uint64
	blocks  map[uint16][]byte
}

// storeModel is the crash oracle's plain model of a disk's chunks: each written block's bytes,
// live and in each frozen version, applying records' entries in LSN order as replay does.
type storeModel struct {
	live   map[blockKey][]byte
	frozen map[core.ChunkID][]frozenBlocks
}

func newStoreModel() *storeModel {
	return &storeModel{live: map[blockKey][]byte{}, frozen: map[core.ChunkID][]frozenBlocks{}}
}

func (m *storeModel) Apply(r writtenRecord) {
	for i, e := range r.entries {
		switch e.Kind {
		case disklog.EntryWrite, disklog.EntrySlot:
			for k := range e.Count {
				m.live[blockKey{e.Chunk, e.First + k}] = r.payloads[i][int(k)*core.BlockSize:][:core.BlockSize]
			}
		case disklog.EntryPunch:
			for k := range e.Count {
				delete(m.live, blockKey{e.Chunk, e.First + k})
			}
		case disklog.EntryFreeze:
			if m.isFrozen(e.Chunk, e.Version) {
				continue
			}
			f := frozenBlocks{version: e.Version, epoch: e.Epoch, blocks: map[uint16][]byte{}}
			for k, b := range m.live {
				if k.c == e.Chunk && e.Version != 0 {
					f.blocks[k.b] = b
				}
			}
			m.frozen[e.Chunk] = append(m.frozen[e.Chunk], f)
		}
	}
}

func (m *storeModel) isFrozen(c core.ChunkID, v uint64) bool {
	for _, f := range m.frozen[c] {
		if f.version == v {
			return true
		}
	}
	return false
}

func (m *storeModel) Clone() *storeModel {
	c := &storeModel{live: maps.Clone(m.live), frozen: map[core.ChunkID][]frozenBlocks{}}
	for k, fs := range m.frozen {
		c.frozen[k] = append([]frozenBlocks(nil), fs...)
	}
	return c
}

// at is block b of the chunk as the snapshot of epoch reads it, or live for epoch 0.
func (m *storeModel) at(c core.ChunkID, b uint16, epoch uint64) []byte {
	zero := make([]byte, core.BlockSize)
	if epoch != 0 {
		var best *frozenBlocks
		for i, f := range m.frozen[c] {
			if f.epoch >= epoch && (best == nil || f.epoch < best.epoch) {
				best = &m.frozen[c][i]
			}
		}
		if best != nil {
			if blk, ok := best.blocks[b]; ok {
				return blk
			}
			return zero
		}
	}
	if blk, ok := m.live[blockKey{c, b}]; ok {
		return blk
	}
	return zero
}

// checkModel compares every block the model or the index holds of the chunks, live and at each
// epoch up to maxEpoch, with the store.
func checkModel(t testing.TB, h *harness, m *storeModel, chunks []core.ChunkID, maxEpoch uint64) {
	t.Helper()
	ix := h.s.Index()
	for _, c := range chunks {
		blocks := map[uint16]bool{}
		for k := range m.live {
			if k.c == c {
				blocks[k.b] = true
			}
		}
		for _, f := range m.frozen[c] {
			for b := range f.blocks {
				blocks[b] = true
			}
		}
		vers := append(ix.frozen(nil, testPool, c), 0, 0)
		for i := 0; i < len(vers); i += 2 {
			for _, e := range ix.Extents(nil, testPool, c, vers[i]) {
				for b := e.First; int(b) < e.end(); b++ {
					blocks[b] = true
				}
			}
		}
		for epoch := range maxEpoch + 1 {
			for b := range blocks {
				if got, want := h.block(c, b, epoch), m.at(c, b, epoch); !bytes.Equal(got, want) {
					i := firstDiff(got, want)
					t.Fatalf("chunk %v block %d at epoch %d: byte %d is %#x, the model holds %#x", c, b, epoch, i, got[i], want[i])
				}
			}
		}
	}
}

// crashChunks are the chunks of the crash workload.
var crashChunks = []core.ChunkID{chunkC, {Volume: 1, Inode: 7, Index: 4}}

// crashWorkload writes unaligned bytes, a cut, a whole chunk and enough records for a checkpoint,
// settling only now and then so that rounds pipeline.
func crashWorkload(h *harness) {
	c, d := crashChunks[0], crashChunks[1]
	h.write(c, 100, fill(300, 0xa1), 0)
	h.write(c, 4000, fill(5000, 0xa2), 0)
	h.step()
	h.write(c, 150, fill(10, 0xa3), 0)
	h.punch(c, 2*core.BlockSize+7, core.BlockSize, 0)
	h.settle()
	h.write(c, 50, fill(20, 0xa4), 1)
	h.write(d, 0, fill(core.ChunkSize, 0xa5), 1)
	h.write(c, 3*core.BlockSize, fill(core.BlockSize, 0xa6), 1)
	h.step()
	h.step()
	h.write(d, 10, fill(3, 0xa7), 2)
	h.settle()
	h.punch(d, 0, core.ChunkSize, 2)
	h.write(c, 0, fill(core.BlockSize+1, 0xa8), 2)
	h.settle()
}

func crashConfig() Config {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	return cfg
}

// recoverHarness reopens the store on the harness's disk after a crash and checks the recovered
// state against the crash oracle.
func (h *harness) recoverStore(mode disklog.Mode) {
	h.t.Helper()
	h.queue, h.out = nil, Output{}
	var err error
	h.s = newStore(h.cfg)
	if err = h.s.recover(&h.ctx, h.disk.Blocks(), mode); err != nil {
		h.t.Fatal(err)
	}
	h.s.pump(&h.ctx, &h.out)
	h.take()
	h.settle()
	if !h.s.Serving() {
		h.t.Fatalf("recovery stopped: %v", h.s.Err())
	}
}

// [property] A workload crashed at its k-th disk action, for every k, with no unflushed block
// landing, a seeded half of them, and all of them, recovers to a prefix of what was written that holds
// every acknowledged record, live and at every snapshot, and the rebuilt index equals it.
func TestCrashAtEveryAction(t *testing.T) {
	crashEveryAction(t, crashConfig(), nil, crashWorkload)
}

// crashEveryAction runs the crash property over workload on a disk cut as geo, or the default
// disk when geo is nil, checking after every recovery that the log's chunk slots in use are
// those the index refers to. On a disk with chunk slots, a crash also lands every write but
// those of slots, so a record lands without its slot data.
func crashEveryAction(t *testing.T, cfg Config, geo *disklog.Geometry, workload func(*harness)) {
	start := func() *harness {
		if geo == nil {
			return newHarness(t, cfg)
		}
		return newHarnessOn(t, cfg, *geo)
	}
	recoverStore := func(h *harness, k int, mode disklog.Mode) {
		h.recoverStore(mode)
		if err := h.s.CheckSlots(); err != nil {
			t.Fatalf("crash at action %d, recovery in mode %d: %v", k, mode, err)
		}
	}
	full := start()
	workload(full)
	total := full.executed
	if full.s.Log().Manifest().IsNone() {
		t.Fatal("the workload wrote no manifest")
	}
	if err := full.s.CheckSlots(); err != nil {
		t.Fatal(err)
	}
	landings := []string{"none", "half", "all"}
	if geo != nil {
		landings = append(landings, "all but slots")
	}
	for k := 0; k < total; k++ {
		for _, landing := range landings {
			h := start()
			h.oracle = sim.NewOracle[writtenRecord](0, newStoreModel())
			h.crashAt = k
			func() {
				defer func() {
					if r := recover(); r != nil && r != errCrash {
						panic(r)
					}
				}()
				workload(h)
			}()
			switch landing {
			case "none":
				h.disk.Crash(func(int, core.LBA) bool { return false })
			case "half":
				h.disk.CrashRandom(core.NewRand(uint64(k)))
			case "all":
				h.disk.Crash(func(int, core.LBA) bool { return true })
			case "all but slots":
				h.disk.Crash(func(_ int, lba core.LBA) bool {
					return !slices.ContainsFunc(slices.Collect(maps.Keys(h.slotData)), func(s core.LBA) bool { return lba >= s && lba < s+core.BlocksPerChunk })
				})
			}
			h.crashAt = -1
			recoverStore(h, k, disklog.ModeOpen)
			m := h.recovered
			checkModel(t, h, m, crashChunks, 2)
			opened := dumpIndex(h.s.Index())
			recoverStore(h, k, disklog.ModeRebuild)
			if got := dumpIndex(h.s.Index()); !slices.Equal(got, opened) {
				t.Fatalf("crash at action %d: rebuilt index has %d entries, recovered %d", k, len(got), len(opened))
			}
			checkModel(t, h, m, crashChunks, 2)
		}
	}
}
