package chunkstore

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// blockRead is what reading one block returns: whether the chunk reads, and the version,
// location (0 for zeros) and record of the extent holding the block.
type blockRead struct {
	ok          bool
	version     uint64
	lba, header core.LBA
}

type modelFrozen struct {
	version, epoch uint64
	blocks         map[uint16]blockRead
}

type modelChunk struct {
	entry          bool
	version, epoch uint64
	live           map[uint16]blockRead
	frozen         []modelFrozen
}

type chunkRef struct {
	pool  core.PoolID
	chunk core.ChunkID
}

// blockModel is the crash oracle's plain model of the chunk index: per chunk its entry, and
// block by block its live map and frozen maps.
type blockModel map[chunkRef]*modelChunk

func (m blockModel) chunk(pool core.PoolID, c core.ChunkID) *modelChunk {
	k := chunkRef{pool, c}
	if m[k] == nil {
		m[k] = &modelChunk{live: map[uint16]blockRead{}}
	}
	return m[k]
}

// Apply maps a record's entries as Index.Apply's contract says, block by block.
func (m blockModel) Apply(ev disklog.Event) {
	for i, e := range ev.Entries {
		switch e.Kind {
		case disklog.EntryWrite, disklog.EntryPunch:
			ch := m.chunk(e.Pool, e.Chunk)
			ch.entry, ch.version = true, e.Version
			if e.Kind == disklog.EntryPunch && e.First == 0 && e.Count == core.BlocksPerChunk {
				clear(ch.live)
				continue
			}
			for b := range e.Count {
				r := blockRead{ok: true, version: e.Version, header: ev.LBA}
				if e.Kind == disklog.EntryWrite {
					r.lba = ev.LBA + core.LBA(ev.HeaderBlocks) + core.LBA(ev.Payloads[i]/core.BlockSize) + core.LBA(b)
				}
				ch.live[e.First+b] = r
			}
		case disklog.EntryFreeze:
			ch := m.chunk(e.Pool, e.Chunk)
			if ch.frozenIndex(e.Version) >= 0 {
				continue
			}
			f := modelFrozen{version: e.Version, epoch: e.Epoch, blocks: map[uint16]blockRead{}}
			if e.Version != 0 {
				f.blocks = maps.Clone(ch.live)
			}
			ch.frozen = append(ch.frozen, f)
			if !ch.entry {
				ch.entry, ch.version = true, e.Version
			}
			ch.epoch = max(ch.epoch, e.Epoch)
		}
	}
}

// Clone copies the model.
func (m blockModel) Clone() blockModel {
	out := blockModel{}
	for k, ch := range m {
		c := *ch
		c.live = maps.Clone(ch.live)
		c.frozen = slices.Clone(ch.frozen)
		for i := range c.frozen {
			c.frozen[i].blocks = maps.Clone(c.frozen[i].blocks)
		}
		out[k] = &c
	}
	return out
}

func (ch *modelChunk) frozenIndex(version uint64) int {
	return slices.IndexFunc(ch.frozen, func(f modelFrozen) bool { return f.version == version })
}

// frozenAt is the freeze a snapshot at epoch reads, as Index.FrozenAt finds it.
func (ch *modelChunk) frozenAt(epoch uint64) (*modelFrozen, bool) {
	var best *modelFrozen
	for i := range ch.frozen {
		if f := &ch.frozen[i]; f.epoch >= epoch && (best == nil || f.epoch < best.epoch) {
			best = f
		}
	}
	return best, best != nil
}

// read is block b as a snapshot reading f, or the live map when f is nil, reads it: zeros where
// no extent holds b, nothing where the chunk has no entry or is absent.
func (ch *modelChunk) read(f *modelFrozen, b uint16) blockRead {
	blocks := ch.live
	switch {
	case f != nil && f.version == 0, f == nil && (!ch.entry || ch.version == 0):
		return blockRead{}
	case f != nil:
		blocks = f.blocks
	}
	if r, ok := blocks[b]; ok {
		return r
	}
	return blockRead{ok: true}
}

func extentRead(e Extent, ok bool, b uint16) blockRead {
	if !ok {
		return blockRead{}
	}
	return blockRead{ok: true, version: e.Version, lba: e.BlockLBA(b), header: e.Header}
}

// workloadChunks is every chunk the recovery workload writes or freezes.
func workloadChunks() []core.ChunkID {
	var cs []core.ChunkID
	for inode := range 3 {
		for i := range 4 {
			cs = append(cs, core.ChunkID{Volume: 1, Inode: core.InodeID(1 + inode), Index: uint32(i)})
		}
	}
	return append(cs, core.ChunkID{Volume: 1, Inode: 4, Index: 0}, core.ChunkID{Volume: 1, Inode: 4, Index: 1})
}

// outsideWorkload names a chunk that x or m holds and the workload neither writes nor freezes.
func outsideWorkload(x *Index, m blockModel) error {
	known := map[chunkRef]bool{}
	for _, c := range workloadChunks() {
		known[chunkRef{1, c}] = true
	}
	for k := range m {
		if !known[k] {
			return fmt.Errorf("the model holds chunk %v of pool %d, which the workload does not touch", k.chunk, k.pool)
		}
	}
	for _, kv := range dumpIndex(x) {
		k := []byte(kv[0])
		pool := core.PoolID(binary.BigEndian.Uint32(k))
		if pool == RecordPool {
			continue
		}
		if c := core.ChunkIDFromBytes([core.ChunkIDSize]byte(k[12:chunkKeySize])); !known[chunkRef{pool, c}] {
			return fmt.Errorf("the index holds chunk %v of pool %d, which the workload does not touch", c, pool)
		}
	}
	return nil
}

// oracleDiff names the first read where x differs from the model m: a chunk's entry, the version
// a snapshot reads at an epoch, or a block live or in a frozen version, which is LookupAt's read
// at that version's epochs once the versions match, without its FrozenAt per block.
func oracleDiff(x *Index, m blockModel) error {
	if err := x.Check(); err != nil {
		return err
	}
	if err := outsideWorkload(x, m); err != nil {
		return err
	}
	for _, c := range workloadChunks() {
		ch := m.chunk(1, c)
		got, ok := x.Chunk(1, c)
		if want := (Chunk{Version: ch.version, Epoch: ch.epoch}); ok != ch.entry || ok && got != want {
			return fmt.Errorf("chunk %v entry %+v %v, want %+v %v", c, got, ok, want, ch.entry)
		}
		newest := uint64(0)
		for _, f := range ch.frozen {
			newest = max(newest, f.epoch)
		}
		for e := uint64(1); e <= newest+1; e++ {
			v, ok := x.FrozenAt(1, c, e)
			f, fok := ch.frozenAt(e)
			if ok != fok || ok && v != f.version {
				return fmt.Errorf("chunk %v at epoch %d reads frozen version %d %v, want %+v", c, e, v, ok, f)
			}
		}
		for b := range uint16(core.BlocksPerChunk) {
			e, ok := x.Lookup(1, c, b)
			if got, want := extentRead(e, ok, b), ch.read(nil, b); got != want {
				return fmt.Errorf("chunk %v block %d reads %+v live, want %+v", c, b, got, want)
			}
			for i := range ch.frozen {
				f := &ch.frozen[i]
				e, ok := Extent{}, false
				if f.version != 0 {
					e, ok = x.lookup(1, c, f.version, b)
				}
				if got, want := extentRead(e, ok, b), ch.read(f, b); got != want {
					return fmt.Errorf("chunk %v block %d reads %+v at epoch %d, want %+v", c, b, got, f.epoch, want)
				}
			}
		}
	}
	return nil
}

// mapsBlocks reports whether a record holds an entry the index maps.
func mapsBlocks(ev *disklog.Event) bool {
	return slices.ContainsFunc(ev.Entries, func(e disklog.Entry) bool { return e.Kind != disklog.EntryBlob })
}

// noteWrite records the open sequence of a segment whose header the write a carries, and, while
// the log runs, hands the oracle the record a round's write carries, decoded from the bytes it
// sends as a Durable event for it would report it. Segment 0 is the superblock's.
func (n *node) noteWrite(a core.Action) {
	seg, block := uint32(uint64(a.LBA)/uint64(n.geo.SegmentBlocks)), uint32(uint64(a.LBA)%uint64(n.geo.SegmentBlocks))
	if seg == 0 {
		return
	}
	var img []byte
	for _, v := range a.Vec {
		img = append(img, n.pool.Bytes(v.Buf)[:v.Len]...)
	}
	if len(a.Vec) == 0 {
		img = n.pool.Bytes(a.Buf)[:a.Len]
	}
	lba := a.LBA
	if block == 0 {
		sh, err := disklog.DecodeSegmentHeader(img[:core.BlockSize])
		if err != nil || sh.LogID != nodeLogID {
			delete(n.opened, seg)
			return
		}
		n.opened[seg] = sh.Seq
		img, block, lba = img[core.BlockSize:], 1, lba+1
	}
	if n.oracle == nil || n.rec != nil {
		return
	}
	seq, ok := n.opened[seg]
	if !ok {
		n.t.Fatalf("a round is written at LBA %d of segment %d, which no write opened", lba, seg)
	}
	hdr, err := disklog.DecodeHeader(img, disklog.Place{LogID: nodeLogID, Seq: seq, Offset: block}, n.geo.SegmentBlocks-block, nil)
	if err != nil {
		n.t.Fatalf("round written at LBA %d: %v", lba, err)
	}
	ev := disklog.Event{Kind: disklog.Durable, LSN: hdr.Record.LSN, Segment: seg, Block: block, LBA: lba, HeaderBlocks: hdr.Layout.HeaderBlocks, Entries: hdr.Entries}
	for i := range ev.Entries {
		ev.Payloads = append(ev.Payloads, disklog.PayloadOffset(ev.Entries, i))
	}
	if err := n.oracle.Written(ev.LSN, ev); err != nil {
		n.t.Fatalf("oracle: %v", err)
	}
}

// oracleStats counts what the crash cycles reached.
type oracleStats struct {
	recoveries, kept, crashedRecoveries, erasedThenCrashed, withRuns, withFreezes int
	// absent counts recoveries with a chunk frozen absent, writtenAfterAbsent those with one
	// written after it.
	absent, writtenAfterAbsent int
}

// crashCycles runs the workload on a node crashed at random actions six times, each unflushed
// block landing at random, and recovers after each crash; every other recovery is first crashed
// at its final flush and run again. It returns the first recovered index the oracle rejects.
func crashCycles(t *testing.T, seed uint64, replayLate bool, st *oracleStats) error {
	t.Helper()
	n := newNode(t)
	w := &workload{r: rand.New(rand.NewPCG(seed, 9))}
	r := core.NewRand(seed)
	n.landing, n.replayLate = r, replayLate
	var mismatch error
	if replayLate {
		n.mismatch = func(err error) { mismatch = cmp.Or(mismatch, err) }
	}
	for cycle := range 6 {
		n.crashAct = func(a core.Action) bool { return a.Kind != core.ActDiskRead && r.Chance(1, 300) }
		w.run(n, 120)
		if !n.crashed {
			n.crash()
		}
		acked, newest := n.oracle.Window()
		if cycle%2 == 1 {
			var found uint64
			if n.recoverUntil(disklog.ModeOpen, func(a core.Action) bool {
				if n.rec == nil || a.Kind != core.ActDiskFsync || a.Linked {
					return false
				}
				found = n.rec.DurableLSN()
				if n.rec.Erased() > 0 {
					st.erasedThenCrashed++
				}
				return true
			}) {
				t.Fatalf("seed %d cycle %d: recovery issued no final flush to crash at", seed, cycle)
			}
			st.crashedRecoveries++
			if a, b := n.oracle.Window(); a != acked || b != newest {
				t.Fatalf("seed %d cycle %d: a crashed recovery moved the oracle's window from [%d, %d] to [%d, %d]", seed, cycle, acked, newest, a, b)
			}
			n.recover(disklog.ModeOpen)
			if n.recoveredLSN != found {
				t.Fatalf("seed %d cycle %d: recovered to LSN %d, the crashed recovery found %d", seed, cycle, n.recoveredLSN, found)
			}
		} else {
			n.recover(disklog.ModeOpen)
		}
		if mismatch != nil {
			return mismatch
		}
		st.recoveries++
		if n.recoveredLSN > acked {
			st.kept++
		}
		if n.ix.Tree().Runs() > 0 {
			st.withRuns++
		}
		chunks := slices.Collect(maps.Values(n.want))
		if slices.ContainsFunc(chunks, func(ch *modelChunk) bool { return len(ch.frozen) > 0 }) {
			st.withFreezes++
		}
		absent := func(ch *modelChunk) bool { return ch.frozenIndex(0) >= 0 }
		if slices.ContainsFunc(chunks, absent) {
			st.absent++
		}
		if slices.ContainsFunc(chunks, func(ch *modelChunk) bool { return absent(ch) && ch.version != 0 }) {
			st.writtenAfterAbsent++
		}
	}
	return nil
}

// [property] Seeds 0-7, six crashes each at random actions, each unflushed block landing at
// random: every recovery reports an LSN in the crash oracle's window and recovers an index that
// reads, live and at every snapshot epoch, as the oracle's block model at that LSN. A recovery
// crashed at its final flush leaves the window as it was, and the recovery run again reports
// the LSN it had found.
func TestRecoveriesMatchTheCrashOracle(t *testing.T) {
	var st oracleStats
	for seed := range uint64(8) {
		if err := crashCycles(t, seed, false, &st); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	t.Logf("%+v", st)
	if st.kept == 0 || st.erasedThenCrashed == 0 || st.withRuns < st.recoveries/2 || st.withFreezes == 0 || st.absent == 0 || st.writtenAfterAbsent == 0 {
		t.Fatalf("the crashes missed a case: %+v", st)
	}
}

// Replay that leaves out the first record past the replay LSN that maps blocks, as a replay LSN
// one record too new would (RocksDB #11148), trips the crash oracle on some seed of 8.
func TestCrashOracleCatchesLateReplay(t *testing.T) {
	for seed := range uint64(8) {
		if err := crashCycles(t, seed, true, &oracleStats{}); err != nil {
			if !strings.Contains(err.Error(), "want") {
				t.Fatalf("seed %d: %v names no expected read", seed, err)
			}
			t.Logf("seed %d: %v", seed, err)
			return
		}
	}
	t.Fatal("no seed of 8 tripped the oracle")
}
