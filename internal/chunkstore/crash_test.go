package chunkstore

import (
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

// storeModel is the crash oracle's plain model of a disk's chunks: each chunk's entry and each
// written block's bytes, live and as each cut's snapshot reads them, applying records' entries in
// LSN order. A freeze at cut e keeps the chunk as it then stands for the snapshot of e, and of
// every earlier cut no earlier freeze kept, whatever version it names. A write or punch of an
// interval at or past its PG's newest decision is held, durable and not visible, until the next
// decision marker of the PG maps or drops it. A drop removes the chunk whole unless it holds or lists
// something of a later interval.
type storeModel struct {
	chunks map[core.ChunkID]Chunk
	live   map[blockKey][]byte
	frozen map[core.ChunkID][]frozenBlocks
	// unmarked is the missing-list entries no decision marker of theirs has followed yet.
	unmarked []disklog.Entry
	decided  map[pgKey]uint32
	held     []heldEntry
}

type pgKey struct {
	pool       core.PoolID
	owner, pgs uint32
}

type heldEntry struct {
	entry   disklog.Entry
	payload []byte
}

func newStoreModel() *storeModel {
	return &storeModel{chunks: map[core.ChunkID]Chunk{}, live: map[blockKey][]byte{}, frozen: map[core.ChunkID][]frozenBlocks{}, decided: map[pgKey]uint32{}}
}

func (m *storeModel) Apply(r writtenRecord) {
	for i, e := range r.entries {
		switch e.Kind {
		case disklog.EntryWrite, disklog.EntrySlot, disklog.EntryPunch:
			if d := m.decidedFor(e.Pool, e.Chunk); d != 0 && intervalOf(e.Version) >= d {
				m.held = append(m.held, heldEntry{entry: e, payload: r.payloads[i]})
				continue
			}
			m.mapEntry(e, r.payloads[i])
		case disklog.EntryFreeze:
			ch, ok := m.chunks[e.Chunk]
			if !ok {
				ch.Version = e.Version
			}
			ch.Epoch = max(ch.Epoch, e.Epoch)
			m.chunks[e.Chunk] = ch
			f := frozenBlocks{version: e.Version, epoch: e.Epoch, blocks: map[uint16][]byte{}}
			for k, b := range m.live {
				if k.c == e.Chunk {
					f.blocks[k.b] = b
				}
			}
			m.frozen[e.Chunk] = append(m.frozen[e.Chunk], f)
		case disklog.EntryMissing:
			m.unmarked = append(m.unmarked, e)
		case disklog.EntryDecision:
			m.unmarked = slices.DeleteFunc(m.unmarked, func(u disklog.Entry) bool {
				if u.Interval != e.Interval || core.PGIndexOf(u.Chunk.Hash(), uint32(e.Length)) != e.Owner {
					return false
				}
				ch := m.chunks[u.Chunk]
				ch.Listed, ch.ListedIn = u.Version, u.Interval
				m.chunks[u.Chunk] = ch
				return true
			})
			k := pgKey{e.Pool, e.Owner, uint32(e.Length)}
			m.decided[k] = max(m.decided[k], e.Interval)
			m.settleHeld(e)
		case disklog.EntryRecovered:
			for j := len(m.held) - 1; j >= 0; j-- {
				if h := &m.held[j].entry; h.Chunk == e.Chunk && h.Version == e.Version {
					h.Last = true
					break
				}
			}
			ch := m.chunks[e.Chunk]
			ch.Version, ch.Whole, ch.Recovered = e.Version, e.Version, e.Version
			if ch.ListedIn <= e.Interval {
				ch.Listed, ch.ListedIn = 0, 0
			}
			m.chunks[e.Chunk] = ch
		case disklog.EntryDropped:
			m.held = slices.DeleteFunc(m.held, func(h heldEntry) bool { return h.entry.Chunk == e.Chunk })
			if m.chunks[e.Chunk].after(e.Interval) {
				break
			}
			delete(m.chunks, e.Chunk)
			delete(m.frozen, e.Chunk)
			maps.DeleteFunc(m.live, func(k blockKey, _ []byte) bool { return k.c == e.Chunk })
		}
	}
}

func (m *storeModel) mapEntry(e disklog.Entry, payload []byte) {
	m.version(e)
	for k := range e.Count {
		if e.Kind == disklog.EntryPunch {
			delete(m.live, blockKey{e.Chunk, e.First + k})
		} else {
			m.live[blockKey{e.Chunk, e.First + k}] = payload[int(k)*core.BlockSize:][:core.BlockSize]
		}
	}
}

// decidedFor is the newest decision interval of a PG holding chunk c, at any PG count.
func (m *storeModel) decidedFor(pool core.PoolID, c core.ChunkID) uint32 {
	var d uint32
	for k, iv := range m.decided {
		if k.pool == pool && core.PGIndexOf(c.Hash(), k.pgs) == k.owner {
			d = max(d, iv)
		}
	}
	return d
}

// settleHeld settles each chunk of decision d's PG that holds versions back by the version d's
// list keeps if it lists the chunk, else its newest held: the held versions up to the newest held
// one of that version's interval at or below it are mapped, the rest dropped.
func (m *storeModel) settleHeld(d disklog.Entry) {
	for {
		j := slices.IndexFunc(m.held, func(h heldEntry) bool {
			return h.entry.Pool == d.Pool && core.PGIndexOf(h.entry.Chunk.Hash(), uint32(d.Length)) == d.Owner
		})
		if j < 0 {
			return
		}
		c := m.held[j].entry.Chunk
		same := func(h heldEntry) bool { return h.entry.Pool == d.Pool && h.entry.Chunk == c }
		vers := make([]uint64, 0, len(m.held))
		for _, h := range m.held {
			if same(h) {
				vers = append(vers, h.entry.Version)
			}
		}
		keep := slices.Max(vers)
		if ch := m.chunks[c]; ch.ListedIn == d.Interval {
			keep = ch.Listed
		}
		top := uint64(0)
		for _, v := range vers {
			if v <= keep && intervalOf(v) == intervalOf(keep) {
				top = max(top, v)
			}
		}
		m.held = slices.DeleteFunc(m.held, func(h heldEntry) bool {
			if !same(h) {
				return false
			}
			if h.entry.Version <= top {
				m.mapEntry(h.entry, h.payload)
			} else if ch := m.chunks[c]; ch.Recovered == h.entry.Version {
				ch.Recovered = 0
				if ch.Whole == h.entry.Version {
					ch.Whole = 0
				}
				if ch.Version == h.entry.Version {
					ch.Version = 0
				}
				m.chunks[c] = ch
			}
			return true
		})
	}
}

// version moves the chunk to e's version, which the disk holds whole once e is its last entry.
func (m *storeModel) version(e disklog.Entry) {
	ch := m.chunks[e.Chunk]
	ch.Version = e.Version
	if e.Last {
		ch.Whole = e.Version
	}
	m.chunks[e.Chunk] = ch
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
	c := &storeModel{chunks: maps.Clone(m.chunks), live: maps.Clone(m.live), frozen: map[core.ChunkID][]frozenBlocks{}, unmarked: slices.Clone(m.unmarked),
		decided: maps.Clone(m.decided), held: slices.Clone(m.held)}
	for k, fs := range m.frozen {
		c.frozen[k] = append([]frozenBlocks(nil), fs...)
	}
	return c
}

// frozenFor is the freeze the snapshot of epoch reads chunk c from, or nil when it reads live.
func (m *storeModel) frozenFor(c core.ChunkID, epoch uint64) *frozenBlocks {
	var best *frozenBlocks
	for i, f := range m.frozen[c] {
		if f.epoch >= epoch && (best == nil || f.epoch < best.epoch) {
			best = &m.frozen[c][i]
		}
	}
	return best
}

// at is block b of the chunk as the snapshot of epoch reads it, or live for epoch 0.
func (m *storeModel) at(c core.ChunkID, b uint16, epoch uint64) []byte {
	if epoch != 0 {
		if f := m.frozenFor(c, epoch); f != nil {
			if blk, ok := f.blocks[b]; ok {
				return blk
			}
			return zeroBlock
		}
	}
	if blk, ok := m.live[blockKey{c, b}]; ok {
		return blk
	}
	return zeroBlock
}

// crashChunks are the chunks of the crash workload.
var crashChunks = []core.ChunkID{chunkC, {Volume: 1, Inode: 7, Index: 4}}

// crashWorkload writes unaligned bytes, a cut, a whole chunk, a drop, a later write a second drop leaves alone and enough
// records for a checkpoint, settling only now and then so that rounds pipeline.
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
	h.drop(d, 1)
	h.step()
	h.write(c, 20, fill(5, 0xa9), 2)
	h.settle()
	h.writeReq(d, 0, fill(core.BlockSize, 0xaa), Request{Version: MakeVersion(2, 1)})
	h.settle()
	h.s.Drop(&h.ctx, h.id(), d, 1, &h.out)
	h.take()
	h.step()
	h.punch(d, 0, core.ChunkSize, 2)
	h.write(c, 0, fill(core.BlockSize+1, 0xa8), 2)
	h.settle()
}

func crashConfig() Config {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	return cfg
}

// recoverStore reopens the store on the harness's disk after a crash and checks the recovered
// state against the crash oracle.
func (h *harness) recoverStore(mode disklog.Mode) {
	h.t.Helper()
	if !h.reopen(mode) {
		h.t.Fatalf("recovery stopped: %v", h.s.Err())
	}
}

// reopen recovers the store on the harness's disk after a crash, settles it, and reports whether
// it serves. After a crash of a settled store, nothing queued, the recovery must finish with the
// index the store held.
func (h *harness) reopen(mode disklog.Mode) bool {
	h.t.Helper()
	h.unsettled = len(h.queue) > 0 || len(h.held) > 0 || !h.s.Serving()
	if !h.unsettled {
		if mode == disklog.ModeRebuild {
			h.expectIndex(invRebuild)
		} else {
			h.expectIndex(invReplay)
		}
	}
	h.queue, h.out = nil, Output{}
	h.abandoned, h.held = h.pool.Live(), nil
	closeStore(h.t, h.s, h.pages)
	h.s = newStore(h.cfg)
	h.s.plant, h.s.metaVictim = h.plant, h.noteMetaVictim
	if err := h.s.recover(&h.ctx, h.disk.Blocks(), mode); err != nil {
		h.t.Fatal(err)
	}
	h.s.pump(&h.ctx, &h.out)
	h.take()
	h.settle()
	return h.s.Serving()
}

// [property] A workload crashed at its k-th disk action, for every k, with no unflushed block
// landing, a seeded half of them, and all of them, recovers to a prefix of what was written that holds
// every acknowledged record, live and at every snapshot, and the rebuilt index equals it.
func TestCrashAtEveryAction(t *testing.T) {
	crashEveryAction(t, crashConfig(), nil, crashWorkload, true)
}

// crashEveryAction runs the crash property over workload on a disk cut as geo, or the default
// disk when geo is nil, checking after every recovery that the log's chunk slots in use are
// those the index refers to. On a disk with chunk slots, a crash also lands every write but
// those of slots, so a record lands without its slot data. With checkpoints, the workload must
// write a manifest.
func crashEveryAction(t *testing.T, cfg Config, geo *disklog.Geometry, workload func(*harness), checkpoints bool) {
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
	if checkpoints && full.s.Log().Manifest().IsNone() {
		t.Fatal("the workload wrote no manifest")
	}
	if err := full.s.CheckSlots(); err != nil {
		t.Fatal(err)
	}
	cleaned := full.s.CleanStats().Freed > 0
	landings := []string{"none", "half", "all"}
	if geo != nil {
		landings = append(landings, "all but slots")
	}
	for k := 0; k <= total; k++ {
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
			h.rebuild()
			if err := h.s.CheckSlots(); err != nil {
				t.Fatalf("crash at action %d, rebuild: %v", k, err)
			}
			if !cleaned {
				recoverStore(h, k, disklog.ModeRebuild)
			}
		}
	}
}

// [property] A primary's own records survive a crash as a prefix of the order its store ordered
// them in, across chunks: gated writes of two chunks, three rounds in flight, crashed at every action
// with no unflushed block landing, each of four seeded halves, and all of them, keep no version
// ordered after one that was lost. The epoch model's trace mapping rests on it: a crash's mayfail
// lines lose only a prefix of themselves, newest first.
func TestACrashKeepsAPrefixOfTheOrderedVersions(t *testing.T) {
	const writes = 12
	cfg := crashConfig()
	cfg.Log.CommitDepth = 3
	var ids []uint64
	workload := func(h *harness) {
		for i := range writes {
			if id := h.gated(crashChunks[i%2], 0, fill(core.BlockSize, byte(i+1)), 4); len(ids) < writes {
				ids = append(ids, id)
			}
			if i%3 == 2 {
				h.step()
			}
		}
		h.settle()
	}
	full := newHarness(t, cfg)
	workload(full)
	if len(full.orderedIDs) != writes {
		t.Fatalf("%d of %d writes ordered", len(full.orderedIDs), writes)
	}
	var split int
	for k := 0; k <= full.executed; k++ {
		for landing := range 6 {
			h := newHarness(t, cfg)
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
			case 0:
				h.disk.Crash(func(int, core.LBA) bool { return false })
			case 5:
				h.disk.Crash(func(int, core.LBA) bool { return true })
			default:
				h.disk.CrashRandom(core.NewRand(uint64(k*8 + landing)))
			}
			h.crashAt = -1
			ordered := h.orderedIDs
			h.recoverStore(disklog.ModeOpen)
			newest := [2]byte{h.readNow(crashChunks[0], 0, 1)[0], h.readNow(crashChunks[1], 0, 1)[0]}
			kept, lost := 0, 0
			for j, id := range ordered {
				i := slices.Index(ids, id)
				if byte(i+1) > newest[i%2] {
					lost++
					continue
				}
				if lost > 0 {
					t.Fatalf("crash at action %d, landing %d: write %d, ordered %d of %v, survived after one ordered before it was lost", k, landing, i, j, ordered)
				}
				kept++
			}
			if kept > 0 && lost > 0 {
				split++
			}
		}
	}
	if split == 0 {
		t.Fatal("no crash kept some ordered writes and lost others")
	}
}
