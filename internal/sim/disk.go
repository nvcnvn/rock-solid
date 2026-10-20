package sim

import (
	"fmt"
	"maps"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// SectorSize is the unit the simulated disk tears a block into when tearing is on.
const SectorSize = 512

const sectorsPerBlock = core.BlockSize / SectorSize

const allSectors = 1<<sectorsPerBlock - 1

var zeroBlock = make([]byte, core.BlockSize)

// DiskLatency is the simulated time each kind of action takes on a healthy disk.
type DiskLatency struct {
	Read  core.Tick
	Write core.Tick
	Flush core.Tick
	Trim  core.Tick
}

// DefaultDiskLatency is read 80 µs, write 20 µs, flush 1,000 µs (a consumer drive's durable-write floor) and trim 20 µs.
var DefaultDiskLatency = DiskLatency{Read: 80, Write: 20, Flush: 1000, Trim: 20}

// DiskSettings are the simulated disk's tunables.
type DiskSettings struct {
	Seed        uint64
	Latency     DiskLatency
	TearSectors bool
}

// DefaultDiskSettings are seed 0, DefaultDiskLatency and no tearing inside a block.
func DefaultDiskSettings() DiskSettings { return DiskSettings{Latency: DefaultDiskLatency} }

// Disk is the simulator's disk: writes stay volatile until a flush, reads see them at once, and a crash lands each unflushed block or not.
type Disk struct {
	blocks      uint64
	latency     DiskLatency
	slow        core.Tick
	tear        bool
	predRand    *core.Rand
	rand        *core.Rand
	executed    uint64
	durable     map[core.LBA][]byte
	view        map[core.LBA][]byte
	unflushed   []blockWrite
	faults      []Fault
	misdirected []MisdirectedWrite
}

type blockWrite struct {
	lba       core.LBA
	data      []byte
	undecided bool
}

// DiskOp is one executed action as a fault predicate sees it; Index counts the disk's actions from 1 in execution order.
type DiskOp struct {
	Index  uint64
	Kind   core.ActionKind
	IO     core.IOID
	LBA    core.LBA
	Blocks uint64
}

// Pred chooses the actions a fault applies to, drawing any chance from r, the disk's seeded predicate source.
type Pred func(op DiskOp, r *core.Rand) bool

// OnKind matches actions of kind k.
func OnKind(k core.ActionKind) Pred {
	return func(op DiskOp, _ *core.Rand) bool { return op.Kind == k }
}

// OnBlocks matches actions touching any block from first to last inclusive.
func OnBlocks(first, last core.LBA) Pred {
	return func(op DiskOp, _ *core.Rand) bool {
		return op.Blocks > 0 && op.LBA <= last && uint64(first) < uint64(op.LBA)+op.Blocks
	}
}

// OnIndex matches the i-th action executed, counting from 1.
func OnIndex(i uint64) Pred {
	return func(op DiskOp, _ *core.Rand) bool { return op.Index == i }
}

// Randomly matches each action with probability num/den from the disk's seeded source.
func Randomly(num, den uint64) Pred {
	return func(_ DiskOp, r *core.Rand) bool { return r.Chance(num, den) }
}

// And matches when every predicate does, testing them in order and stopping at the first miss.
func And(ps ...Pred) Pred {
	return func(op DiskOp, r *core.Rand) bool {
		for _, p := range ps {
			if !p(op, r) {
				return false
			}
		}
		return true
	}
}

// Or matches when any predicate does, testing them in order and stopping at the first match.
func Or(ps ...Pred) Pred {
	return func(op DiskOp, r *core.Rand) bool {
		for _, p := range ps {
			if p(op, r) {
				return true
			}
		}
		return false
	}
}

// Not matches when p does not.
func Not(p Pred) Pred {
	return func(op DiskOp, r *core.Rand) bool { return !p(op, r) }
}

// Once matches only the first action p matches.
func Once(p Pred) Pred {
	done := false
	return func(op DiskOp, r *core.Rand) bool {
		if done || !p(op, r) {
			return false
		}
		done = true
		return true
	}
}

// FaultEffect is what an injected fault does to an action it applies to.
type FaultEffect uint8

// Fault effects.
const (
	// FailIO completes the action with DiskIO, leaving a write's or trim's blocks, or a flush's unflushed ones, undecided.
	FailIO FaultEffect = iota + 1
	// DamageRead completes a read OK with one byte of the returned buffer changed.
	DamageRead
	// Misdirect lands a write whole at another LBA and completes it OK.
	Misdirect
)

// Fault is an injected fault: Effect on every action When matches, with To choosing a misdirected write's LBA, or the seed when To is nil.
type Fault struct {
	Effect FaultEffect
	When   Pred
	To     func(op DiskOp) core.LBA
}

func (f *Fault) appliesTo(k core.ActionKind) bool {
	switch f.Effect {
	case FailIO:
		return k == core.ActDiskRead || k == core.ActDiskWrite || k == core.ActDiskFsync || k == core.ActDiskTrim
	case DamageRead:
		return k == core.ActDiskRead
	case Misdirect:
		return k == core.ActDiskWrite
	}
	return false
}

// MisdirectedWrite records a write the disk landed away from its intended LBA.
type MisdirectedWrite struct {
	Index    uint64
	IO       core.IOID
	Intended core.LBA
	Landed   core.LBA
	Blocks   uint64
}

// NewDisk makes an all-zero disk of the given number of blocks with DefaultDiskSettings.
func NewDisk(blocks uint64) *Disk { return NewDiskWith(blocks, DefaultDiskSettings()) }

// NewDiskWith makes an all-zero disk of the given number of blocks with settings s.
func NewDiskWith(blocks uint64, s DiskSettings) *Disk {
	root := core.NewRand(s.Seed)
	return &Disk{
		blocks:   blocks,
		latency:  s.Latency,
		slow:     1,
		tear:     s.TearSectors,
		predRand: root.Fork(),
		rand:     root.Fork(),
		durable:  map[core.LBA][]byte{},
		view:     map[core.LBA][]byte{},
	}
}

// Blocks is the disk's size in blocks.
func (d *Disk) Blocks() uint64 { return d.blocks }

// SetSlow makes every later action take factor times its base latency, never failing because of it; 1 is a healthy disk.
func (d *Disk) SetSlow(factor uint64) {
	if factor == 0 {
		panic("sim: fail-slow factor 0")
	}
	d.slow = core.Tick(factor)
}

// Inject adds a fault on later actions; for each action the first fault added whose effect fits its kind and whose predicate matches applies.
func (d *Disk) Inject(f Fault) {
	if f.When == nil || f.Effect < FailIO || f.Effect > Misdirect {
		panic(fmt.Sprintf("sim: fault %+v", f))
	}
	d.faults = append(d.faults, f)
}

// ClearFaults removes every injected fault.
func (d *Disk) ClearFaults() { d.faults = nil }

// Misdirected lists every write the disk misdirected, in execution order.
func (d *Disk) Misdirected() []MisdirectedWrite { return d.misdirected }

// Execute runs a read, write, flush or trim at once and returns its completion.
func (d *Disk) Execute(pool core.BufPool, a *core.Action) core.Event {
	ev, _ := d.ExecuteTimed(pool, a)
	return ev
}

// ExecuteTimed runs a read, write, flush or trim at once and returns its completion and the simulated time it took.
func (d *Disk) ExecuteTimed(pool core.BufPool, a *core.Action) (core.Event, core.Tick) {
	d.executed++
	op := DiskOp{Index: d.executed, Kind: a.Kind, IO: a.IO, LBA: a.LBA, Blocks: a.Len / core.BlockSize}
	if a.Kind == core.ActDiskFsync {
		op.LBA, op.Blocks = 0, 0
	}
	ev := core.Event{Kind: core.EvDiskDone, IO: a.IO}
	var base core.Tick
	switch a.Kind {
	case core.ActDiskRead:
		base = d.latency.Read
		ev.Err = d.read(pool, a, op)
	case core.ActDiskWrite:
		base = d.latency.Write
		ev.Err = d.write(pool, a, op)
	case core.ActDiskFsync:
		base = d.latency.Flush
		ev.Err = d.fsync(op)
	case core.ActDiskTrim:
		base = d.latency.Trim
		ev.Err = d.trim(a, op)
	default:
		panic(fmt.Sprintf("sim: disk cannot execute %+v", *a))
	}
	return ev, base * d.slow
}

func (d *Disk) fault(op DiskOp) *Fault {
	for i := range d.faults {
		f := &d.faults[i]
		if f.appliesTo(op.Kind) && f.When(op, d.predRand) {
			return f
		}
	}
	return nil
}

func (d *Disk) inRange(lba core.LBA, n uint64) bool {
	return n > 0 && uint64(lba) < d.blocks && n <= d.blocks-uint64(lba)
}

func (d *Disk) validRange(a *core.Action) bool {
	return a.Len%core.BlockSize == 0 && d.inRange(a.LBA, a.Len/core.BlockSize)
}

func (d *Disk) read(pool core.BufPool, a *core.Action, op DiskOp) core.DiskError {
	if !d.validRange(a) {
		return core.DiskInvalidRange
	}
	dst := pool.Bytes(a.Buf)[:a.Len]
	f := d.fault(op)
	if f != nil && f.Effect == FailIO {
		d.rand.Fill(dst)
		return core.DiskIO
	}
	for i := range op.Blocks {
		blk := dst[i*core.BlockSize : (i+1)*core.BlockSize]
		if src, ok := d.view[a.LBA+core.LBA(i)]; ok {
			copy(blk, src)
		} else {
			clear(blk)
		}
	}
	if f != nil && f.Effect == DamageRead {
		dst[d.rand.Below(a.Len)] ^= byte(d.rand.Range(1, 255))
	}
	return core.DiskOK
}

func (d *Disk) write(pool core.BufPool, a *core.Action, op DiskOp) core.DiskError {
	if !d.validRange(a) {
		return core.DiskInvalidRange
	}
	img := gather(pool, a)
	at := a.LBA
	f := d.fault(op)
	if f != nil && f.Effect == Misdirect {
		at = d.misdirect(f, op)
	}
	failed := f != nil && f.Effect == FailIO
	for i := 0; i < len(img); i += core.BlockSize {
		d.stage(at+core.LBA(i/core.BlockSize), img[i:i+core.BlockSize:i+core.BlockSize], failed)
	}
	if failed {
		return core.DiskIO
	}
	return core.DiskOK
}

func gather(pool core.BufPool, a *core.Action) []byte {
	img := make([]byte, 0, a.Len)
	if len(a.Vec) == 0 {
		img = append(img, pool.Bytes(a.Buf)[:a.Len]...)
	}
	for _, v := range a.Vec {
		img = append(img, pool.Bytes(v.Buf)[:v.Len]...)
	}
	if uint64(len(img)) != a.Len {
		panic(fmt.Sprintf("sim: write io%d gathers %d bytes, says %d", a.IO, len(img), a.Len))
	}
	return img
}

// inFlight adds a write issued but not completed to the block writes the next crash decides, leaving what reads see unchanged.
func (d *Disk) inFlight(pool core.BufPool, a *core.Action) {
	if !d.validRange(a) {
		return
	}
	img := gather(pool, a)
	for i := 0; i < len(img); i += core.BlockSize {
		d.unflushed = append(d.unflushed, blockWrite{lba: a.LBA + core.LBA(i/core.BlockSize), data: img[i : i+core.BlockSize : i+core.BlockSize]})
	}
}

func (d *Disk) misdirect(f *Fault, op DiskOp) core.LBA {
	last := d.blocks - op.Blocks
	if last == 0 {
		return op.LBA
	}
	var to core.LBA
	if f.To != nil {
		to = f.To(op)
		if to == op.LBA || !d.inRange(to, op.Blocks) {
			panic(fmt.Sprintf("sim: write io%d of %d blocks at LBA %d misdirected to LBA %d", op.IO, op.Blocks, op.LBA, to))
		}
	} else {
		to = core.LBA(d.rand.Below(last))
		if to >= op.LBA {
			to++
		}
	}
	d.misdirected = append(d.misdirected, MisdirectedWrite{Index: op.Index, IO: op.IO, Intended: op.LBA, Landed: to, Blocks: op.Blocks})
	return to
}

func (d *Disk) stage(lba core.LBA, data []byte, undecided bool) {
	d.unflushed = append(d.unflushed, blockWrite{lba: lba, data: data, undecided: undecided})
	d.view[lba] = data
}

func (d *Disk) fsync(op DiskOp) core.DiskError {
	if d.fault(op) != nil {
		for i := range d.unflushed {
			d.unflushed[i].undecided = true
		}
		return core.DiskIO
	}
	d.Flush()
	return core.DiskOK
}

func (d *Disk) trim(a *core.Action, op DiskOp) core.DiskError {
	if !d.validRange(a) {
		return core.DiskInvalidRange
	}
	failed := d.fault(op) != nil
	if d.rand.Chance(1, 2) {
		for i := range op.Blocks {
			d.stage(a.LBA+core.LBA(i), zeroBlock, failed)
		}
	}
	if failed {
		return core.DiskIO
	}
	return core.DiskOK
}

// Flush makes every write executed so far durable; a block left undecided by a failed write or flush lands or not from the seed.
func (d *Disk) Flush() {
	var reverted []core.LBA
	for _, w := range d.unflushed {
		if !w.undecided {
			d.land(w, allSectors)
			continue
		}
		reverted = append(reverted, w.lba)
		d.land(w, d.seededLanding(d.rand))
	}
	d.unflushed = nil
	for _, lba := range reverted {
		d.showDurable(lba)
	}
}

func (d *Disk) showDurable(lba core.LBA) {
	if blk, ok := d.durable[lba]; ok {
		d.view[lba] = blk
	} else {
		delete(d.view, lba)
	}
}

func (d *Disk) seededLanding(r *core.Rand) uint8 {
	if d.tear {
		return uint8(r.Below(allSectors + 1))
	}
	if r.Chance(1, 2) {
		return allSectors
	}
	return 0
}

func (d *Disk) land(w blockWrite, sectors uint8) {
	switch sectors {
	case 0:
	case allSectors:
		d.durable[w.lba] = w.data
	default:
		blk := make([]byte, core.BlockSize)
		copy(blk, d.durable[w.lba])
		for s := range sectorsPerBlock {
			if sectors&(1<<s) != 0 {
				copy(blk[s*SectorSize:(s+1)*SectorSize], w.data[s*SectorSize:])
			}
		}
		d.durable[w.lba] = blk
	}
}

// Unflushed is the number of block writes a crash would decide.
func (d *Disk) Unflushed() int { return len(d.unflushed) }

// Crash drops volatile state, landing the i-th unflushed block write, in execution order, only if landed says so.
func (d *Disk) Crash(landed func(i int, lba core.LBA) bool) {
	d.crash(func(i int, w blockWrite) uint8 {
		if landed(i, w.lba) {
			return allSectors
		}
		return 0
	})
}

// CrashSectors drops volatile state, landing sector s of the i-th unflushed block write, in execution order, only if landed says so.
func (d *Disk) CrashSectors(landed func(i int, lba core.LBA, s int) bool) {
	d.crash(func(i int, w blockWrite) uint8 {
		var sectors uint8
		for s := range sectorsPerBlock {
			if landed(i, w.lba, s) {
				sectors |= 1 << s
			}
		}
		return sectors
	})
}

// CrashRandom crashes with each unflushed block write, or each of its sectors when tearing is on, landing with probability 1/2.
func (d *Disk) CrashRandom(r *core.Rand) {
	d.crash(func(int, blockWrite) uint8 { return d.seededLanding(r) })
}

func (d *Disk) crash(sectors func(i int, w blockWrite) uint8) {
	for i, w := range d.unflushed {
		d.land(w, sectors(i, w))
	}
	d.unflushed = nil
	d.view = maps.Clone(d.durable)
}

// Read returns n blocks from lba as reads see them now.
func (d *Disk) Read(lba core.LBA, n uint64) []byte {
	b := make([]byte, n*core.BlockSize)
	for i := range n {
		if src, ok := d.view[lba+core.LBA(i)]; ok {
			copy(b[i*core.BlockSize:], src)
		}
	}
	return b
}

// Put writes whole blocks at lba durably, as content left on the disk before a test starts.
func (d *Disk) Put(lba core.LBA, data []byte) {
	if len(data)%core.BlockSize != 0 || !d.inRange(lba, uint64(len(data)/core.BlockSize)) {
		panic(fmt.Sprintf("sim: put of %d bytes at LBA %d", len(data), lba))
	}
	for i := 0; i < len(data); i += core.BlockSize {
		blk := append([]byte(nil), data[i:i+core.BlockSize]...)
		at := lba + core.LBA(i/core.BlockSize)
		d.durable[at] = blk
		d.view[at] = blk
	}
}

// FlipBit XORs mask into byte off of the durable block at lba, as silent corruption does.
func (d *Disk) FlipBit(lba core.LBA, off int, mask byte) {
	if !d.inRange(lba, 1) {
		panic(fmt.Sprintf("sim: bit flip at LBA %d", lba))
	}
	blk := make([]byte, core.BlockSize)
	copy(blk, d.durable[lba])
	blk[off] ^= mask
	d.durable[lba] = blk
	if !slices.ContainsFunc(d.unflushed, func(w blockWrite) bool { return w.lba == lba }) {
		d.view[lba] = blk
	}
}

// FlipRandomBits flips one seeded bit in each written durable block with probability num/den, in LBA order, and returns the blocks flipped.
func (d *Disk) FlipRandomBits(num, den uint64) []core.LBA {
	var flipped []core.LBA
	for _, lba := range slices.Sorted(maps.Keys(d.durable)) {
		if d.rand.Chance(num, den) {
			d.FlipBit(lba, int(d.rand.Below(core.BlockSize)), 1<<d.rand.Below(8))
			flipped = append(flipped, lba)
		}
	}
	return flipped
}

// FailReads makes every later read touching one of the blocks complete with DiskIO.
func (d *Disk) FailReads(lbas ...core.LBA) {
	ps := make([]Pred, len(lbas))
	for i, lba := range lbas {
		ps[i] = OnBlocks(lba, lba)
	}
	d.Inject(Fault{Effect: FailIO, When: And(OnKind(core.ActDiskRead), Or(ps...))})
}
