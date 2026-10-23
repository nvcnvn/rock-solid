package chunkstore

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// The store's phase 1 data invariants, as the harness registers them, most specific first so
// that a bug breaking several is named by the one closest to it.
const (
	invVersions = "no two writes of a chunk get one version"
	invRebuild  = "a rebuilt chunk index equals the recovered one"
	invReplay   = "replay rebuilds the index the store held"
	invIndex    = "the index passes its own check"
	invAcked    = "acknowledged writes are readable"
	invPunched  = "punched ranges read zeros"
	invReads    = "every read equals the byte-range oracle"
)

// register registers the store's invariants with h's registry. The harness checks them at the
// end of every settle, so after every recovery and rebuild, and a test may check them at any
// step. A settle compares the reads only of the chunks changed or moved since the last check; the
// first check after a recovery, and one a test asks for, compare every chunk's.
func (h *harness) register() {
	h.inv.Register(invVersions, func() error { return h.versionErr })
	h.inv.Register(invRebuild, func() error { return h.indexErr[invRebuild] })
	h.inv.Register(invReplay, func() error { return h.indexErr[invReplay] })
	h.inv.Register(invIndex, func() error {
		if !h.s.Serving() {
			return nil
		}
		return h.s.Index().Check()
	})
	h.inv.Register(invAcked, h.ackedReadable)
	h.inv.Register(invPunched, h.punchesReadZeros)
	h.inv.Register(invReads, h.readsMatchOracle)
}

// holds checks every invariant, comparing every chunk's reads with the byte-range oracle, and
// stops the test at the first broken one, or, with h.catch set, panics with it for violated to
// return.
func (h *harness) holds() {
	h.t.Helper()
	h.check(true)
}

// check checks every invariant, comparing the reads of every chunk with the byte-range oracle
// when all is set and otherwise those of the chunks toCheck lists.
func (h *harness) check(all bool) {
	h.t.Helper()
	h.checking = h.toCheck(all || h.full)
	h.full = false
	if err := h.inv.Check(); err != nil {
		err = fmt.Errorf("after action %d: %w", h.executed, err)
		if h.catch {
			panic(err)
		}
		h.t.Fatal(err)
	}
}

// violated runs f and returns the violation a check under h.catch panicked with, if any.
func violated(f func()) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		e, ok := r.(error)
		var v *sim.Violation
		if !ok || !errors.As(e, &v) {
			panic(r)
		}
		err = e
	}()
	f()
	return nil
}

// violationOf is the name of the invariant err reports broken, or empty.
func violationOf(err error) string {
	var v *sim.Violation
	if errors.As(err, &v) {
		return v.Name
	}
	return ""
}

// expectIndex has the next recovery to finish compare its index with the one the store holds
// now, under invariant name.
func (h *harness) expectIndex(name string) {
	h.indexBefore, h.indexFor = dumpIndex(h.s.Index()), name
}

// recoveredIndex compares the index a recovery finished with the one expectIndex kept.
func (h *harness) recoveredIndex() {
	if h.indexBefore == nil {
		return
	}
	if got := dumpIndex(h.s.Index()); !slices.Equal(got, h.indexBefore) && h.indexErr[h.indexFor] == nil {
		h.indexErr[h.indexFor] = fmt.Errorf("the index holds %d entries, %s", len(got), indexDiff(got, h.indexBefore))
	}
	h.indexBefore = nil
}

// indexDiff names the first entry where got and want differ.
func indexDiff(got, want [][2]string) string {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return fmt.Sprintf("entry %d is %x = %x, the store had %x = %x", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}
	return fmt.Sprintf("the store had %d", len(want))
}

// ackedReadable checks a recovered store, until its next write or punch, against the crash
// oracle: the recovered LSN lies in the oracle's window, so no acknowledged record is lost, and
// the store reads as the oracle's model at that LSN, each chunk's entry and every block the model
// or the index holds, live and at every snapshot epoch. The index holds no chunk the model does not.
func (h *harness) ackedReadable() error {
	if h.recoverErr != nil {
		return h.recoverErr
	}
	m := h.recovered
	if m == nil || h.changed || !h.s.Serving() {
		return nil
	}
	ix := h.s.Index()
	for _, c := range h.knownChunks() {
		got, ok := ix.Chunk(testPool, c)
		if want, wok := m.chunks[c]; ok != wok || got != want {
			return fmt.Errorf("chunk %v entry %+v %v, the model holds %+v %v", c, got, ok, want, wok)
		}
		var held []uint16
		for k := range m.live {
			if k.c == c {
				held = append(held, k.b)
			}
		}
		for _, f := range m.frozen[c] {
			held = slices.AppendSeq(held, maps.Keys(f.blocks))
		}
		v := h.viewOf(c)
		for epoch := range h.maxEpoch + 2 {
			view := v.read(h, epoch)
			for _, b := range sortedBlocks(view, held) {
				if got, want := orZeros(view[b]), m.at(c, b, epoch); !bytes.Equal(got, want) {
					i := firstDiff(got, want)
					return fmt.Errorf("chunk %v block %d at epoch %d: byte %d is %#x, the model holds %#x", c, b, epoch, i, got[i], want[i])
				}
			}
		}
	}
	for _, kv := range dumpIndex(ix) {
		k := []byte(kv[0])
		if pool := core.PoolID(binary.BigEndian.Uint32(k)); pool == RecordPool {
			continue
		}
		if c := core.ChunkIDFromBytes([core.ChunkIDSize]byte(k[12:chunkKeySize])); !hasChunk(m, c) {
			return fmt.Errorf("the index holds chunk %v, which the model does not", c)
		}
	}
	return nil
}

func hasChunk(m *storeModel, c core.ChunkID) bool {
	_, ok := m.chunks[c]
	return ok
}

// knownChunks lists, in order, every chunk the harness wrote or punched and every chunk the
// recovered model holds.
func (h *harness) knownChunks() []core.ChunkID {
	set := map[core.ChunkID]bool{}
	for _, c := range h.changes {
		set[c] = true
	}
	if h.recovered != nil {
		for c := range h.recovered.chunks {
			set[c] = true
		}
	}
	return slices.SortedFunc(maps.Keys(set), chunkOrder)
}

func chunkOrder(a, b core.ChunkID) int {
	x, y := a.Bytes(), b.Bytes()
	return bytes.Compare(x[:], y[:])
}

// sortedBlocks is the blocks view holds and those of more, in order, once each.
func sortedBlocks(view map[uint16][]byte, more []uint16) []uint16 {
	bs := slices.AppendSeq(slices.Clone(more), maps.Keys(view))
	slices.Sort(bs)
	return slices.Compact(bs)
}

func orZeros(b []byte) []byte {
	if b == nil {
		return zeroBlock
	}
	return b
}

// punchesReadZeros checks that every byte an acknowledged punch cleared, and no later
// acknowledged write has written, reads as zero live.
func (h *harness) punchesReadZeros() error {
	for _, c := range h.checking {
		live := h.viewOf(c).read(h, 0)
		ch := h.bytes.chunks[c]
		for _, b := range slices.Sorted(maps.Keys(ch.punched)) {
			got, mask := live[b], ch.punched[b]
			for i := range got {
				if mask[i] != 0 && got[i] != 0 {
					return fmt.Errorf("chunk %v block %d byte %d is %#x, punched", c, b, i, got[i])
				}
			}
		}
	}
	return nil
}

// readsMatchOracle checks that every block of the chunks checked reads, live and as the snapshot
// of every epoch up to one past the newest cut, as the byte-range oracle of the acknowledged
// writes and punches holds it.
func (h *harness) readsMatchOracle() error {
	for _, c := range h.checking {
		ch, v := h.bytes.chunks[c], h.viewOf(c)
		for epoch := range h.maxEpoch + 2 {
			got, want := v.read(h, epoch), ch.at(epoch)
			for _, b := range sortedBlocks(got, slices.Collect(maps.Keys(want))) {
				if g, w := orZeros(got[b]), orZeros(want[b]); !bytes.Equal(g, w) {
					i := firstDiff(g, w)
					return fmt.Errorf("chunk %v block %d at epoch %d: byte %d is %#x, the oracle holds %#x", c, b, epoch, i, g[i], w[i])
				}
			}
		}
	}
	return nil
}

// storeView is what a check reads of one chunk through the index: its frozen versions with their
// epochs, as Index.frozen lists them, each map's extents, live under version 0, and the blocks
// each map stores, once read from the disk.
type storeView struct {
	c      core.ChunkID
	vers   []uint64
	maps   map[uint64][]Extent
	blocks map[uint64]map[uint16][]byte
}

// viewOf is chunk c's view, which a check keeps until the next.
func (h *harness) viewOf(c core.ChunkID) *storeView {
	if v, ok := h.views[c]; ok {
		return v
	}
	ix := h.s.Index()
	v := &storeView{c: c, vers: ix.frozen(nil, testPool, c), maps: map[uint64][]Extent{}, blocks: map[uint64]map[uint16][]byte{}}
	for i := 0; i < len(v.vers); i += 2 {
		if ver := v.vers[i]; ver != 0 {
			v.maps[ver] = ix.Extents(nil, testPool, c, ver)
		}
	}
	v.maps[0] = ix.Extents(nil, testPool, c, 0)
	h.views[c] = v
	return v
}

// same reports whether v and o hold the same versions and maps.
func (v *storeView) same(o *storeView) bool {
	if o == nil || !slices.Equal(v.vers, o.vers) || len(v.maps) != len(o.maps) {
		return false
	}
	for ver, m := range v.maps {
		if !slices.Equal(m, o.maps[ver]) {
			return false
		}
	}
	return true
}

// read is the chunk as the snapshot of epoch reads it, live for epoch 0: each block a map stores
// on disk, read from the disk, a block absent reading zeros.
func (v *storeView) read(h *harness, epoch uint64) map[uint16][]byte {
	var ver uint64
	if epoch != 0 {
		if fv, ok := frozenAt(v.vers, epoch); ok {
			if fv == 0 {
				return nil
			}
			ver = fv
		}
	}
	if blocks, ok := v.blocks[ver]; ok {
		return blocks
	}
	blocks := map[uint16][]byte{}
	for _, e := range v.maps[ver] {
		if e.LBA == 0 {
			continue
		}
		data := h.disk.Read(e.LBA, uint64(e.Count))
		for b := e.First; int(b) < e.end(); b++ {
			blocks[b] = data[int(b-e.First)*core.BlockSize:][:core.BlockSize]
		}
	}
	v.blocks[ver] = blocks
	return blocks
}

// toCheck lists the chunks of the byte-range oracle whose reads the next check compares: each one
// a change was acknowledged on since the last check, whose maps have moved since, or whose maps
// overlap blocks written or trimmed since, or, with all set, every one. It keeps the maps a check
// saw.
func (h *harness) toCheck(all bool) []core.ChunkID {
	clear(h.views)
	touched := mergeRanges(h.touched)
	h.touched = h.touched[:0]
	if h.bytes == nil || !h.s.Serving() {
		return nil
	}
	var cs []core.ChunkID
	for _, c := range h.bytes.sorted() {
		v := h.viewOf(c)
		if all || h.dirty[c] || !v.same(h.seen[c]) || v.overlaps(touched) {
			cs = append(cs, c)
		}
		h.seen[c] = v
	}
	clear(h.dirty)
	return cs
}

// lbaRange is the blocks from lo up to hi.
type lbaRange struct{ lo, hi core.LBA }

// touch notes the blocks from lo up to hi written or trimmed, joining them to the last range
// noted when they follow it.
func (h *harness) touch(lo, hi core.LBA) {
	if n := len(h.touched); n > 0 && h.touched[n-1].hi == lo {
		h.touched[n-1].hi = hi
		return
	}
	h.touched = append(h.touched, lbaRange{lo, hi})
}

// mergeRanges sorts rs and joins those that overlap or touch, in place.
func mergeRanges(rs []lbaRange) []lbaRange {
	slices.SortFunc(rs, func(a, b lbaRange) int { return cmp.Compare(a.lo, b.lo) })
	out := rs[:0]
	for _, r := range rs {
		if n := len(out); n > 0 && r.lo <= out[n-1].hi {
			out[n-1].hi = max(out[n-1].hi, r.hi)
			continue
		}
		out = append(out, r)
	}
	return out
}

// overlaps reports whether a block one of v's maps stores lies in one of the sorted, disjoint
// ranges rs.
func (v *storeView) overlaps(rs []lbaRange) bool {
	if len(rs) == 0 {
		return false
	}
	for _, m := range v.maps {
		for _, e := range m {
			if e.LBA == 0 {
				continue
			}
			end := e.LBA + core.LBA(e.Count)
			i, _ := slices.BinarySearchFunc(rs, e.LBA, func(r lbaRange, lba core.LBA) int { return cmp.Compare(r.hi, lba+1) })
			if i < len(rs) && rs[i].lo < end {
				return true
			}
		}
	}
	return false
}

var zeroBlock = make([]byte, core.BlockSize)

// change is a write (data set) or punch (data nil) the harness sent and the store has not answered.
type change struct {
	c      core.ChunkID
	off, n uint32
	data   []byte
	epoch  uint64
}

// ackChange applies the change answered OK at version to the byte-range oracle; a version at or
// below one the chunk already acknowledged breaks invVersions.
func (h *harness) ackChange(ch change, version uint64) {
	o := h.bytes.chunk(ch.c)
	if version == 0 && ch.data == nil {
		return
	}
	h.dirty[ch.c] = true
	if version <= o.version && h.versionErr == nil {
		h.versionErr = fmt.Errorf("chunk %v acknowledged version %#x after %#x", ch.c, version, o.version)
	}
	o.apply(ch, version)
}

// byteOracle is each chunk's bytes as the acknowledged writes and punches, applied in version
// order, left them: live, and for each snapshot epoch as the last change before its cut left
// them. Blocks are kept whole and never changed in place, so a snapshot shares them.
type byteOracle struct {
	chunks map[core.ChunkID]*oracleChunk
}

type oracleChunk struct {
	version, epoch uint64
	// live and each snapshot's blocks, a block absent reading zeros.
	live  map[uint16][]byte
	snaps map[uint64]map[uint16][]byte
	// punched marks, per block, the bytes a punch cleared that no write has written since.
	punched map[uint16][]byte
}

func newByteOracle() *byteOracle { return &byteOracle{chunks: map[core.ChunkID]*oracleChunk{}} }

func (o *byteOracle) chunk(c core.ChunkID) *oracleChunk {
	ch, ok := o.chunks[c]
	if !ok {
		ch = &oracleChunk{live: map[uint16][]byte{}, snaps: map[uint64]map[uint16][]byte{}, punched: map[uint16][]byte{}}
		o.chunks[c] = ch
	}
	return ch
}

func (o *byteOracle) sorted() []core.ChunkID {
	return slices.SortedFunc(maps.Keys(o.chunks), chunkOrder)
}

func (ch *oracleChunk) apply(c change, version uint64) {
	for e := ch.epoch + 1; e <= c.epoch; e++ {
		ch.snaps[e] = maps.Clone(ch.live)
	}
	ch.version, ch.epoch = max(ch.version, version), max(ch.epoch, c.epoch)
	first, last := spanOf(c.off, c.n)
	for b := first; b <= last; b++ {
		lo, hi, _ := partial(c.off, c.n, b)
		blk := slices.Clone(ch.live[b])
		if blk == nil {
			blk = make([]byte, core.BlockSize)
		}
		mask := slices.Clone(ch.punched[b])
		if mask == nil {
			mask = make([]byte, core.BlockSize)
		}
		if c.data == nil {
			clear(blk[lo:hi])
			for i := lo; i < hi; i++ {
				mask[i] = 1
			}
		} else {
			start := int(b-first)*core.BlockSize + int(lo) - int(c.off%core.BlockSize)
			copy(blk[lo:hi], c.data[start:])
			clear(mask[lo:hi])
		}
		if bytes.Equal(blk, zeroBlock) {
			delete(ch.live, b)
		} else {
			ch.live[b] = blk
		}
		if bytes.Equal(mask, zeroBlock) {
			delete(ch.punched, b)
		} else {
			ch.punched[b] = mask
		}
	}
}

// at is the chunk's blocks as the snapshot of epoch reads them, or live for epoch 0.
func (ch *oracleChunk) at(epoch uint64) map[uint16][]byte {
	if epoch == 0 || epoch > ch.epoch {
		return ch.live
	}
	return ch.snaps[epoch]
}

// rebase makes the oracle the crash oracle's model of a recovered store, which holds every
// acknowledged change and those in flight that the recovery kept.
func (o *byteOracle) rebase(m *storeModel) {
	clear(o.chunks)
	for c, e := range m.chunks {
		ch := o.chunk(c)
		ch.version, ch.epoch = e.Version, e.Epoch
	}
	for k, blk := range m.live {
		if !bytes.Equal(blk, zeroBlock) {
			o.chunk(k.c).live[k.b] = blk
		}
	}
	for c, ch := range o.chunks {
		for e := uint64(1); e <= ch.epoch; e++ {
			f := m.frozenFor(c, e)
			if f == nil {
				ch.snaps[e] = maps.Clone(ch.live)
				continue
			}
			ch.snaps[e] = map[uint16][]byte{}
			for b, blk := range f.blocks {
				if !bytes.Equal(blk, zeroBlock) {
					ch.snaps[e][b] = blk
				}
			}
		}
	}
}

// A bug planted in the store breaks the invariant it violates, which the registry names.
func TestPlantedBugsBreakTheirInvariants(t *testing.T) {
	for _, c := range []struct {
		want  string
		plant plants
		run   func(h *harness)
	}{
		{invVersions, plants{forgetVersion: true}, func(h *harness) {
			h.sync(h.write(chunkC, 0, fill(10, 1), 0))
			h.sync(h.write(chunkC, 0, fill(10, 2), 0))
		}},
		{invRebuild, plants{rebuildSkipsFreezes: true}, func(h *harness) {
			h.sync(h.write(chunkC, 0, fill(10, 1), 0))
			h.sync(h.write(chunkC, 0, fill(10, 2), 1))
			h.rebuild()
		}},
		{invReplay, plants{}, func(h *harness) {
			h.sync(h.write(chunkC, 0, fill(3*core.BlockSize, 1), 0))
			h.sync(h.punch(chunkC, 0, core.BlockSize, 0))
			h.disk.Crash(func(int, core.LBA) bool { return false })
			h.plant = plants{replayDropsPunches: true}
			h.recoverStore(disklog.ModeOpen)
		}},
		{invPunched, plants{punchKeepsHead: true}, func(h *harness) {
			h.sync(h.write(chunkC, 0, fill(3*core.BlockSize, 1), 0))
			h.sync(h.punch(chunkC, 100, 2*core.BlockSize, 0))
		}},
	} {
		t.Run(c.want, func(t *testing.T) {
			h := newHarness(t, testConfig())
			h.s.plant, h.catch = c.plant, true
			err := violated(func() { c.run(h) })
			if violationOf(err) != c.want {
				t.Fatalf("broke %v", err)
			}
			t.Log(err)
		})
	}
}
