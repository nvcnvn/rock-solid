package sim

import (
	"fmt"
	"maps"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// Disk is the simulator's disk: writes stay volatile until a flush, reads see them at once, and a crash lands each unflushed block or not.
type Disk struct {
	blocks    uint64
	durable   map[core.LBA][]byte
	view      map[core.LBA][]byte
	unflushed []blockWrite
	failRead  map[core.LBA]bool
}

type blockWrite struct {
	lba  core.LBA
	data []byte
}

// NewDisk makes an all-zero disk of the given number of blocks.
func NewDisk(blocks uint64) *Disk {
	return &Disk{
		blocks:   blocks,
		durable:  map[core.LBA][]byte{},
		view:     map[core.LBA][]byte{},
		failRead: map[core.LBA]bool{},
	}
}

// Blocks is the disk's size in blocks.
func (d *Disk) Blocks() uint64 { return d.blocks }

// Execute runs a read, write or flush at once and returns its completion.
func (d *Disk) Execute(pool core.BufPool, a *core.Action) core.Event {
	ev := core.Event{Kind: core.EvDiskDone, IO: a.IO}
	switch a.Kind {
	case core.ActDiskRead:
		ev.Err = d.read(pool, a)
	case core.ActDiskWrite:
		ev.Err = d.write(pool, a)
	case core.ActDiskFsync:
		d.Flush()
	default:
		panic(fmt.Sprintf("sim: disk cannot execute %+v", *a))
	}
	return ev
}

func (d *Disk) inRange(lba core.LBA, n uint64) bool {
	return n > 0 && uint64(lba) < d.blocks && n <= d.blocks-uint64(lba)
}

func (d *Disk) read(pool core.BufPool, a *core.Action) core.DiskError {
	if a.Len%core.BlockSize != 0 || !d.inRange(a.LBA, a.Len/core.BlockSize) {
		return core.DiskInvalidRange
	}
	n := a.Len / core.BlockSize
	for i := range n {
		if d.failRead[a.LBA+core.LBA(i)] {
			return core.DiskIO
		}
	}
	dst := pool.Bytes(a.Buf)[:a.Len]
	for i := range n {
		blk := dst[i*core.BlockSize : (i+1)*core.BlockSize]
		if src, ok := d.view[a.LBA+core.LBA(i)]; ok {
			copy(blk, src)
		} else {
			clear(blk)
		}
	}
	return core.DiskOK
}

func (d *Disk) write(pool core.BufPool, a *core.Action) core.DiskError {
	if a.Len%core.BlockSize != 0 || !d.inRange(a.LBA, a.Len/core.BlockSize) {
		return core.DiskInvalidRange
	}
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
	for i := 0; i < len(img); i += core.BlockSize {
		w := blockWrite{lba: a.LBA + core.LBA(i/core.BlockSize), data: img[i : i+core.BlockSize : i+core.BlockSize]}
		d.unflushed = append(d.unflushed, w)
		d.view[w.lba] = w.data
	}
	return core.DiskOK
}

// Flush makes every write executed so far durable.
func (d *Disk) Flush() {
	for _, w := range d.unflushed {
		d.durable[w.lba] = w.data
	}
	d.unflushed = nil
}

// Unflushed is the number of block writes a crash would decide.
func (d *Disk) Unflushed() int { return len(d.unflushed) }

// Crash drops volatile state, landing the i-th unflushed block write, in execution order, only if landed says so.
func (d *Disk) Crash(landed func(i int, lba core.LBA) bool) {
	for i, w := range d.unflushed {
		if landed(i, w.lba) {
			d.durable[w.lba] = w.data
		}
	}
	d.unflushed = nil
	d.view = maps.Clone(d.durable)
}

// CrashRandom crashes with each unflushed block write landing with probability 1/2.
func (d *Disk) CrashRandom(r *core.Rand) {
	d.Crash(func(int, core.LBA) bool { return r.Chance(1, 2) })
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

// FlipBit XORs mask into byte off of the block at lba, durably, as silent corruption does.
func (d *Disk) FlipBit(lba core.LBA, off int, mask byte) {
	blk := d.Read(lba, 1)
	blk[off] ^= mask
	d.Put(lba, blk)
}

// FailReads makes every later read touching one of the blocks complete with DiskIO.
func (d *Disk) FailReads(lbas ...core.LBA) {
	for _, lba := range lbas {
		d.failRead[lba] = true
	}
}
