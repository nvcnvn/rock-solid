package chunkstore

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/internal/sim/metamorphic"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var metaOps = flag.Int("meta.ops", 1000, "ops per metamorphic seed")

// metaBlocks is how many blocks of a chunk ops touch; a whole-chunk punch covers all of
// core.BlocksPerChunk.
const metaBlocks = 16

// metaSet is an option set: the index's tree settings, its removals and the cadence of the
// structural ops.
type metaSet struct {
	name     string
	piece    int
	runLimit int
	merges   int
	points   bool
	run      metamorphic.Options
}

// metaSets are the lsm metamorphic test's option sets, with point removals for the range trims;
// the linear merge is internal/lsm's own. C's pieces are 512 bytes, not 256: a root of 256 bytes
// lists 13 pieces, fewer than a flush of 10 index writes takes.
var metaSets = []metaSet{
	{name: "F", piece: 1 << 20},
	{name: "A", piece: 1 << 20, run: metamorphic.Options{FlushOps: true, Compact: metamorphic.CompactAtOps}},
	{name: "B", piece: 4096, run: metamorphic.Options{FlushOps: true, FlushEvery: 40, Compact: metamorphic.CompactEager}},
	{name: "C", piece: 512, run: metamorphic.Options{FlushOps: true, FlushEvery: 10}},
	{name: "D", piece: 512, run: metamorphic.Options{FlushOps: true, FlushEvery: 25, Compact: metamorphic.CompactDeferred, Absorb: 4}},
	{name: "E", piece: 1024, run: metamorphic.Options{FlushOps: true, FlushEvery: 30, Compact: metamorphic.CompactEager, FailFirst: 1, RestartEach: true}},
	{name: "G", piece: 4096, run: metamorphic.Options{FlushOps: true, FlushEvery: 2, CompactEvery: 40}},
	{name: "two-merges", piece: 1024, merges: 2, run: metamorphic.Options{FlushOps: true, FlushEvery: 3, CompactEvery: 8}},
	{name: "fixed-limit", piece: 1024, runLimit: 4, run: metamorphic.Options{FlushOps: true, FlushEvery: 5, Compact: metamorphic.CompactEager}},
	{name: "point-removals", piece: 4096, points: true, run: metamorphic.Options{FlushOps: true, FlushEvery: 20, Compact: metamorphic.CompactEager}},
}

func metaSetNamed(name string) (metaSet, bool) {
	i := slices.IndexFunc(metaSets, func(s metaSet) bool { return s.name == name })
	if i < 0 {
		return metaSet{}, false
	}
	return metaSets[i], true
}

// metaArgs is how many arguments each op that is not structural takes.
var metaArgs = map[string]int{
	"write": 5, "punch": 5, "freeze": 4, "move": 5, "release": 2,
	"lookup": 3, "at": 4, "chunk": 1, "frozenat": 2, "live": 1, "check": 0,
}

func checkMetaOp(o metamorphic.Op) error {
	n, ok := metaArgs[o.Kind]
	if !ok {
		return fmt.Errorf("unknown op %q", o.Kind)
	}
	if err := metamorphic.Ints(o, n); err != nil {
		return err
	}
	switch o.Kind {
	case "write", "punch":
		first, count := o.Int(2), o.Int(3)
		whole := o.Kind == "punch" && first == 0 && count == core.BlocksPerChunk
		if !whole && (count == 0 || first+count > metaBlocks) {
			return fmt.Errorf("%s of blocks [%d, %d) outside blocks 0-%d or a whole chunk", o.Kind, first, first+count, metaBlocks-1)
		}
	case "lookup", "at":
		if b, n := o.Int(1), o.Int(2); b+n > metaBlocks {
			return fmt.Errorf("%s of blocks [%d, %d) outside blocks 0-%d", o.Kind, b, b+n, metaBlocks-1)
		}
	case "move":
		if o.Int(1) >= metaBlocks {
			return fmt.Errorf("move of block %d outside blocks 0-%d", o.Int(1), metaBlocks-1)
		}
	}
	return nil
}

func metaWrites(o metamorphic.Op) bool {
	switch o.Kind {
	case "write", "punch", "freeze", "move", "release":
		return true
	}
	return false
}

// metaBlock is one block of a map: unset, or the version, location and record of its extent.
type metaBlock struct {
	set                  bool
	version, lba, header uint64
}

func (b metaBlock) String() string {
	if !b.set {
		return "0:0:0"
	}
	return fmt.Sprintf("%d:%d:%d", b.version, b.lba, b.header)
}

type metaFrozen struct {
	version, epoch uint64
	blocks         [metaBlocks]metaBlock
}

type metaChunk struct {
	entry          bool
	version, epoch uint64
	live           [metaBlocks]metaBlock
	frozen         []metaFrozen
}

// chunkModel is what the index must read as, block by block: per chunk its entry, live map and
// frozen maps; the structural ops do nothing to it.
type chunkModel struct {
	chunks map[uint64]*metaChunk
}

func newChunkModel() *chunkModel { return &chunkModel{chunks: map[uint64]*metaChunk{}} }

func (m *chunkModel) chunk(c uint64) *metaChunk {
	if m.chunks[c] == nil {
		m.chunks[c] = &metaChunk{}
	}
	return m.chunks[c]
}

func (ch *metaChunk) frozenAt(epoch uint64) (*metaFrozen, bool) {
	var best *metaFrozen
	for i := range ch.frozen {
		if f := &ch.frozen[i]; f.epoch >= epoch && (best == nil || f.epoch < best.epoch) {
			best = f
		}
	}
	return best, best != nil
}

func (ch *metaChunk) frozenIndex(v uint64) int {
	return slices.IndexFunc(ch.frozen, func(f metaFrozen) bool { return f.version == v })
}

func lookupLine(blocks []string) string { return strings.Join(blocks, " ") }

func (m *chunkModel) read(ch *metaChunk, f *metaFrozen, b, n int) string {
	var out []string
	for i := b; i < b+n; i++ {
		switch {
		case f != nil && f.version == 0, f == nil && (!ch.entry || ch.version == 0):
			out = append(out, "miss")
		case f != nil:
			out = append(out, f.blocks[i].String())
		default:
			out = append(out, ch.live[i].String())
		}
	}
	return lookupLine(out)
}

func (m *chunkModel) Do(o metamorphic.Op) string {
	switch o.Kind {
	case "write", "punch":
		ch := m.chunk(o.Uint(0))
		v, first, count, lba := o.Uint(1), o.Int(2), o.Int(3), o.Uint(4)
		ch.entry, ch.version = true, v
		if o.Kind == "punch" && first == 0 && count == core.BlocksPerChunk {
			ch.live = [metaBlocks]metaBlock{}
			return "ok"
		}
		for b := first; b < first+count; b++ {
			if o.Kind == "punch" {
				ch.live[b] = metaBlock{set: true, version: v, header: lba}
			} else {
				ch.live[b] = metaBlock{set: true, version: v, lba: lba + 1 + uint64(b-first), header: lba}
			}
		}
	case "freeze":
		ch := m.chunk(o.Uint(0))
		v, epoch := o.Uint(1), o.Uint(2)
		if ch.frozenIndex(v) >= 0 {
			return "ok"
		}
		f := metaFrozen{version: v, epoch: epoch}
		if v != 0 {
			f.blocks = ch.live
		}
		ch.frozen = append(ch.frozen, f)
		if !ch.entry {
			ch.entry, ch.version = true, v
		}
		ch.epoch = max(ch.epoch, epoch)
	case "release":
		ch := m.chunk(o.Uint(0))
		if v := o.Uint(1); v != 0 {
			if i := ch.frozenIndex(v); i >= 0 {
				ch.frozen = slices.Delete(ch.frozen, i, i+1)
			}
		}
	case "move":
		ch := m.chunk(o.Uint(0))
		b, from, to, header := o.Int(1), o.Uint(2), o.Uint(3), o.Uint(4)
		maps := []*[metaBlocks]metaBlock{}
		for i := range ch.frozen {
			if ch.frozen[i].version != 0 {
				maps = append(maps, &ch.frozen[i].blocks)
			}
		}
		if ch.entry && ch.version != 0 {
			maps = append(maps, &ch.live)
		}
		moved := 0
		for _, mp := range maps {
			if blk := &mp[b]; blk.set && blk.lba != 0 && blk.lba == from {
				blk.lba, blk.header = to, header
				moved++
			}
		}
		return "moved " + strconv.Itoa(moved)
	case "lookup":
		return m.read(m.chunk(o.Uint(0)), nil, o.Int(1), o.Int(2))
	case "at":
		ch := m.chunk(o.Uint(0))
		f, _ := ch.frozenAt(o.Uint(3))
		return m.read(ch, f, o.Int(1), o.Int(2))
	case "chunk":
		ch := m.chunk(o.Uint(0))
		if !ch.entry {
			return "none"
		}
		return fmt.Sprintf("%d:%d", ch.version, ch.epoch)
	case "frozenat":
		if f, ok := m.chunk(o.Uint(0)).frozenAt(o.Uint(1)); ok {
			return strconv.FormatUint(f.version, 10)
		}
		return "none"
	case "live":
		h, n := o.Uint(0), 0
		count := func(blocks *[metaBlocks]metaBlock) {
			for _, b := range blocks {
				if b.set && b.lba != 0 && b.header == h {
					n++
				}
			}
		}
		for _, ch := range m.chunks {
			count(&ch.live)
			for i := range ch.frozen {
				count(&ch.frozen[i].blocks)
			}
		}
		return strconv.Itoa(n)
	}
	return "ok"
}

func (m *chunkModel) Writes(o metamorphic.Op) bool { return metaWrites(o) }
func (m *chunkModel) Views() int                   { return 0 }
func (m *chunkModel) Flush(func()) error           { return nil }
func (m *chunkModel) FailFlush(int) error          { return nil }
func (m *chunkModel) Compact() (bool, error)       { return false, nil }
func (m *chunkModel) CompactStart()                {}
func (m *chunkModel) CompactFinish() error         { return nil }
func (m *chunkModel) Restart() error               { return nil }

// genMetaOps draws n ops from seed: a chunk's writes and punches in version order, a freeze of
// its newest version, or of version 0 if it has none, before its first write after each cut,
// cleaner moves of blocks a live or frozen map holds, releases of frozen versions, and reads.
// It keeps a model to choose them.
func genMetaOps(seed uint64, n int) []metamorphic.Op {
	const chunks = 4
	r := rand.New(rand.NewPCG(seed, 0x6378))
	m := newChunkModel()
	var next [chunks]uint64
	var frozeAt [chunks]uint64
	var headers []uint64
	epoch, lba := uint64(0), uint64(100)
	u := func(v uint64) string { return strconv.FormatUint(v, 10) }
	i := func(v int) string { return strconv.Itoa(v) }
	deck := []struct {
		kind   string
		weight int
	}{{"write", 30}, {"punch", 6}, {"wholepunch", 1}, {"cut", 2}, {"release", 2}, {"move", 8},
		{"lookup", 12}, {"at", 10}, {"chunk", 3}, {"frozenat", 3}, {"live", 4}, {"check", 2},
		{"flush", 5}, {"flushfail", 1}, {"compact", 4}, {"restart", 1}}
	total := 0
	for _, d := range deck {
		total += d.weight
	}
	ops := make([]metamorphic.Op, 0, n)
	add := func(o metamorphic.Op) {
		ops = append(ops, o)
		if metaWrites(o) {
			m.Do(o)
		}
	}
	for len(ops) < n {
		x := r.IntN(total)
		var kind string
		for _, d := range deck {
			if x < d.weight {
				kind = d.kind
				break
			}
			x -= d.weight
		}
		c := r.IntN(chunks)
		ch := m.chunk(uint64(c))
		first := r.IntN(metaBlocks)
		count := 1 + r.IntN(min(4, metaBlocks-first))
		if r.IntN(8) == 0 {
			count = 1 + r.IntN(metaBlocks-first)
		}
		switch kind {
		case "write", "punch", "wholepunch":
			if frozeAt[c] < epoch {
				frozeAt[c] = epoch
				v := uint64(0)
				if ch.entry {
					v = ch.version
				}
				add(metamorphic.Op{Kind: "freeze", Args: []string{i(c), u(v), u(epoch), u(lba)}})
				lba++
			}
			next[c]++
			if kind == "wholepunch" {
				kind, first, count = "punch", 0, core.BlocksPerChunk
			}
			add(metamorphic.Op{Kind: kind, Args: []string{i(c), u(next[c]), i(first), i(count), u(lba)}})
			headers = append(headers, lba)
			lba++
			if kind == "write" {
				lba += uint64(count)
			}
		case "cut":
			epoch++
		case "release":
			var vs []uint64
			for _, f := range ch.frozen {
				vs = append(vs, f.version)
			}
			if len(vs) == 0 {
				continue
			}
			add(metamorphic.Op{Kind: "release", Args: []string{i(c), u(vs[r.IntN(len(vs))])}})
		case "move":
			from := ch.live[first].lba
			if len(ch.frozen) > 0 && r.IntN(3) == 0 {
				from = ch.frozen[r.IntN(len(ch.frozen))].blocks[first].lba
			}
			if from == 0 {
				continue
			}
			add(metamorphic.Op{Kind: "move", Args: []string{i(c), i(first), u(from), u(lba + 1), u(lba)}})
			headers = append(headers, lba)
			lba += 2
		case "lookup":
			add(metamorphic.Op{Kind: "lookup", Args: []string{i(c), i(first), i(count)}})
		case "at":
			add(metamorphic.Op{Kind: "at", Args: []string{i(c), i(first), i(count), u(r.Uint64N(epoch + 2))}})
		case "chunk":
			add(metamorphic.Op{Kind: "chunk", Args: []string{i(c)}})
		case "frozenat":
			add(metamorphic.Op{Kind: "frozenat", Args: []string{i(c), u(r.Uint64N(epoch + 2))}})
		case "live":
			if len(headers) == 0 {
				continue
			}
			add(metamorphic.Op{Kind: "live", Args: []string{u(headers[r.IntN(len(headers))])}})
		case "flushfail":
			add(metamorphic.Op{Kind: kind, Args: []string{i(1 + r.IntN(3))}})
		default:
			add(metamorphic.Op{Kind: kind})
		}
	}
	return ops
}

// metaStats counts what an option set made the index's tree do.
type metaStats struct {
	flushes, failed, compactions, together, deferred, restarts, maxRuns int
}

// metaIndex runs an Index over a log that keeps every live piece. Its write ops stand in for the
// disk log's records, a cleaner move's copy record among them: a restart loads the newest
// manifest's runs and replays the ops after the manifest's replay point.
type metaIndex struct {
	set     metaSet
	x       *Index
	log     *metamorphic.Log
	writes  []metamorphic.Op
	pending *lsm.Compaction
	st      metaStats
}

func newMetaIndex(s metaSet) *metaIndex {
	m := &metaIndex{set: s, log: metamorphic.NewLog(s.piece)}
	m.x = m.newIndex(m.newTree())
	return m
}

func (m *metaIndex) newTree() *lsm.Tree {
	tr, err := lsm.New(lsm.Config{MaxPiece: m.set.piece, RunLimit: m.set.runLimit, Check: true})
	if err != nil {
		panic(err)
	}
	return tr
}

func (m *metaIndex) newIndex(tr *lsm.Tree) *Index {
	x := NewIndex(tr)
	x.pointRemovals = m.set.points
	return x
}

func metaChunkID(o metamorphic.Op) core.ChunkID { return chunk(uint32(o.Uint(0))) }

// applyMetaOp runs write op o on x and returns its history line.
func applyMetaOp(x *Index, o metamorphic.Op) string {
	var err error
	switch o.Kind {
	case "write":
		err = x.Apply(record(core.LBA(o.Uint(4)), disklog.WriteEntry(1, metaChunkID(o), o.Uint(1), uint16(o.Int(2)), uint16(o.Int(3)))))
	case "punch":
		err = x.Apply(record(core.LBA(o.Uint(4)), disklog.PunchEntry(1, metaChunkID(o), o.Uint(1), uint16(o.Int(2)), uint16(o.Int(3)))))
	case "freeze":
		err = x.Apply(record(core.LBA(o.Uint(3)), disklog.FreezeEntry(1, metaChunkID(o), o.Uint(1), o.Uint(2))))
	case "release":
		x.Release(1, metaChunkID(o), o.Uint(1))
	case "move":
		n := x.Move(1, metaChunkID(o), uint16(o.Int(1)), 1, core.LBA(o.Uint(2)), core.LBA(o.Uint(3)), core.LBA(o.Uint(4)))
		return "moved " + strconv.Itoa(n)
	}
	if err != nil {
		return "ERR " + err.Error()
	}
	return "ok"
}

func blockLine(e Extent, ok bool, b uint16) string {
	if !ok {
		return "miss"
	}
	return fmt.Sprintf("%d:%d:%d", e.Version, e.BlockLBA(b), e.Header)
}

func (m *metaIndex) Do(o metamorphic.Op) string {
	if metaWrites(o) {
		m.writes = append(m.writes, o)
		return applyMetaOp(m.x, o)
	}
	switch o.Kind {
	case "lookup", "at":
		c := metaChunkID(o)
		var out []string
		for b := o.Int(1); b < o.Int(1)+o.Int(2); b++ {
			var e Extent
			var ok bool
			if o.Kind == "lookup" {
				e, ok = m.x.Lookup(1, c, uint16(b))
			} else {
				e, ok = m.x.LookupAt(1, c, uint16(b), o.Uint(3))
			}
			out = append(out, blockLine(e, ok, uint16(b)))
		}
		return lookupLine(out)
	case "chunk":
		ch, ok := m.x.Chunk(1, metaChunkID(o))
		if !ok {
			return "none"
		}
		return fmt.Sprintf("%d:%d", ch.Version, ch.Epoch)
	case "frozenat":
		if v, ok := m.x.FrozenAt(1, metaChunkID(o), o.Uint(1)); ok {
			return strconv.FormatUint(v, 10)
		}
		return "none"
	case "live":
		return strconv.FormatUint(uint64(m.x.Live(core.LBA(o.Uint(0)))), 10)
	case "check":
		if err := m.x.Check(); err != nil {
			return "ERR " + err.Error()
		}
	}
	return "ok"
}

func (m *metaIndex) Writes(o metamorphic.Op) bool { return metaWrites(o) }
func (m *metaIndex) Views() int                   { return 0 }

func (m *metaIndex) checkpoint(limit int, between func()) error {
	if err := m.log.Checkpoint(m.x.Tree(), len(m.writes), limit, between); err != nil {
		return err
	}
	m.st.maxRuns = max(m.st.maxRuns, m.x.Tree().Runs())
	return nil
}

func (m *metaIndex) Flush(between func()) error { return m.checkpoint(0, between) }
func (m *metaIndex) FailFlush(pieces int) error { return m.checkpoint(pieces, nil) }

// Compact picks as many merges as the set runs at once, runs them all and installs them newest
// last.
func (m *metaIndex) Compact() (bool, error) {
	tr := m.x.Tree()
	var cs []*lsm.Compaction
	for len(cs) < max(m.set.merges, 1) {
		c := tr.PickCompaction()
		if c == nil {
			break
		}
		cs = append(cs, c)
	}
	if len(cs) == 0 {
		return false, nil
	}
	if len(cs) > 1 {
		m.st.together++
	}
	for _, c := range cs {
		c.Run()
	}
	for i := len(cs) - 1; i >= 0; i-- {
		if err := tr.Install(cs[i]); err != nil {
			return true, err
		}
		m.st.compactions++
	}
	return true, nil
}

func (m *metaIndex) CompactStart() {
	if m.pending == nil {
		m.pending = m.x.Tree().PickCompaction()
	}
}

func (m *metaIndex) CompactFinish() error {
	c := m.pending
	m.pending = nil
	if c == nil {
		return nil
	}
	c.Run()
	m.st.compactions++
	if m.x.Tree().Frozen() {
		m.st.deferred++
	}
	return m.x.Tree().Install(c)
}

func (m *metaIndex) Restart() error {
	m.pending = nil
	tr := m.newTree()
	if err := m.log.Restore(lsm.RootRefs, tr.LoadRun); err != nil {
		return err
	}
	x := m.newIndex(tr)
	for _, o := range m.writes[m.log.Replay():] {
		if l := applyMetaOp(x, o); strings.HasPrefix(l, "ERR") {
			return errors.New(l)
		}
	}
	m.x = x
	m.st.restarts++
	return nil
}

// metaFailure is the first option set whose history differs from the model's.
type metaFailure struct {
	set  metaSet
	at   int
	got  []string
	want []string
}

// metaRun runs ops under every set in sets and the model, and returns the first set that differs
// from the model and the stats of each set.
func metaRun(ops []metamorphic.Op, sets []metaSet) (*metaFailure, []metaStats) {
	want := metamorphic.Run(ops, metamorphic.Options{}, newChunkModel())
	var fail *metaFailure
	stats := make([]metaStats, len(sets))
	for i, s := range sets {
		e := newMetaIndex(s)
		got := metamorphic.Run(ops, s.run, e)
		stats[i] = e.st
		stats[i].flushes, stats[i].failed = e.log.Flushes, e.log.Failed
		if d := metamorphic.Diff(want, got); d >= 0 && fail == nil {
			fail = &metaFailure{set: s, at: d, got: got, want: want}
		}
	}
	return fail, stats
}

// metaCase writes ops as a data-driven regression case under s with the model's history.
func metaCase(ops []metamorphic.Op, s metaSet) string {
	want := metamorphic.Run(ops, metamorphic.Options{}, newChunkModel())
	var b strings.Builder
	fmt.Fprintf(&b, "run set=%s\n", s.name)
	for _, o := range ops {
		b.WriteString(o.String() + "\n")
	}
	b.WriteString("----\n" + metamorphic.Format(ops, want))
	return b.String()
}

// [property] Every seed's ops read the same, block by block, live and at snapshot epochs, under
// every option set and the model; a failing seed prints its replay command and its ops reduced to
// a regression case for testdata/regressions.
func TestMetamorphic(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 3
	}
	seeds.Run(t, 0, n, func(seed uint64) {
		ops := genMetaOps(seed, *metaOps)
		fail, stats := metaRun(ops, metaSets)
		if seed == 0 {
			for i, s := range metaSets {
				t.Logf("seed 0 under %s: %+v", s.name, stats[i])
			}
		}
		if fail == nil {
			return
		}
		last := ops[fail.at].String()
		small := metamorphic.Reduce(ops[:fail.at+1], func(c []metamorphic.Op) bool {
			f, _ := metaRun(c, []metaSet{fail.set})
			return f != nil && f.at == len(c)-1 && c[f.at].String() == last
		})
		t.Fatalf("seed %d under %s, op %d %q: got %q, the model %q\nreduced to %d ops, for testdata/regressions:\n%s",
			seed, fail.set.name, fail.at, ops[fail.at], fail.got[fail.at], fail.want[fail.at], len(small), metaCase(small, fail.set))
	})
}

// The generated ops exercise what the index does on seed 0: absent freezes, whole-chunk punches,
// moves that change a map, releases, and reads at snapshot epochs that find a frozen version; and
// each option set reaches the extreme it is there for.
func TestMetamorphicOpsCoverTheIndex(t *testing.T) {
	ops := genMetaOps(0, 3000)
	hist := metamorphic.Run(ops, metamorphic.Options{}, newChunkModel())
	seen := map[string]bool{}
	for i, o := range ops {
		switch {
		case o.Kind == "freeze" && o.Args[1] == "0":
			seen["absent freeze"] = true
		case o.Kind == "punch" && o.Args[3] == strconv.Itoa(core.BlocksPerChunk):
			seen["whole-chunk punch"] = true
		case o.Kind == "move" && hist[i] != "moved 0":
			seen["move"] = true
		case o.Kind == "release":
			seen["release"] = true
		case o.Kind == "frozenat" && hist[i] != "none":
			seen["frozen version read"] = true
		}
	}
	for _, want := range []string{"absent freeze", "whole-chunk punch", "move", "release", "frozen version read"} {
		if !seen[want] {
			t.Errorf("seed 0 has no %s", want)
		}
	}
	_, stats := metaRun(ops, metaSets)
	of := func(name string) metaStats {
		return stats[slices.IndexFunc(metaSets, func(s metaSet) bool { return s.name == name })]
	}
	for i, s := range metaSets {
		if s.run.FlushOps && stats[i].flushes == 0 {
			t.Errorf("%s never flushed: %+v", s.name, stats[i])
		}
	}
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"G keeps 40 runs", of("G").maxRuns >= 40},
		{"E fails flushes and restarts", of("E").failed > 0 && of("E").restarts > of("A").restarts},
		{"D installs merges during flushes", of("D").deferred > 0},
		{"two-merges runs two at once", of("two-merges").together > 0},
		{"fixed-limit compacts", of("fixed-limit").compactions > 0},
	} {
		if !c.ok {
			t.Errorf("%s: %+v", c.name, stats)
		}
	}
}

// The chunk index's reduced failures, each run under its option set, must read as the model does.
func TestMetamorphicRegressions(t *testing.T) {
	datadriven.RunTest(t, "testdata/regressions", func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "run" {
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		var name string
		d.ScanArgs(t, "set", &name)
		s, ok := metaSetNamed(name)
		if !ok {
			d.Fatalf(t, "unknown option set %q", name)
		}
		ops, err := metamorphic.Parse(d.Input, checkMetaOp)
		if err != nil {
			d.Fatalf(t, "%v", err)
		}
		if fail, _ := metaRun(ops, []metaSet{s}); fail != nil {
			t.Errorf("%s: op %d %q: got %q, the model %q", d.Pos, fail.at, ops[fail.at], fail.got[fail.at], fail.want[fail.at])
		}
		return metamorphic.Format(ops, metamorphic.Run(ops, s.run, newMetaIndex(s)))
	})
}
