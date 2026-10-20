package trace

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// RecordSize is the bytes a record takes in a ring.
const RecordSize = 64

// DefaultRecords is the default trace_records, the records a loop's ring holds.
const DefaultRecords = 65536

// ErrRingSize reports ring memory that is empty or not a whole number of records.
var ErrRingSize = errors.New("ring memory is not a positive multiple of RecordSize")

// Ring keeps the newest records a loop wrote in memory the caller provides, overwriting the oldest.
type Ring struct {
	mem   []byte
	slots uint64
	added uint64
}

// NewRing makes a ring over mem, which holds len(mem)/RecordSize records.
func NewRing(mem []byte) (*Ring, error) {
	if len(mem) == 0 || len(mem)%RecordSize != 0 {
		return nil, fmt.Errorf("%w: %d bytes", ErrRingSize, len(mem))
	}
	return &Ring{mem: mem, slots: uint64(len(mem) / RecordSize)}, nil
}

// Add records r, overwriting the oldest record once the ring is full.
func (g *Ring) Add(r *Record) {
	i := g.added % g.slots
	g.added++
	b := g.mem[i*RecordSize : i*RecordSize+RecordSize]
	binary.LittleEndian.PutUint64(b[0:], uint64(r.Tick))
	binary.LittleEndian.PutUint64(b[8:], r.Pass)
	binary.LittleEndian.PutUint32(b[16:], r.Node)
	binary.LittleEndian.PutUint16(b[20:], r.Loop)
	b[22] = byte(r.Name)
	b[23] = 0
	if r.Sim {
		b[23] = 1
	}
	for j, v := range r.Fields {
		binary.LittleEndian.PutUint64(b[24+8*j:], v)
	}
}

// Len is the number of records the ring holds.
func (g *Ring) Len() int { return int(min(g.added, g.slots)) }

// Added is the number of records ever added, including those overwritten.
func (g *Ring) Added() uint64 { return g.added }

// At returns the i-th record held, oldest first.
func (g *Ring) At(i int) Record {
	n := g.Len()
	if i < 0 || i >= n {
		panic(fmt.Sprintf("trace: record %d of %d", i, n))
	}
	slot := (g.added - uint64(n) + uint64(i)) % g.slots
	b := g.mem[slot*RecordSize : slot*RecordSize+RecordSize]
	r := Record{
		Tick: core.Tick(binary.LittleEndian.Uint64(b[0:])),
		Pass: binary.LittleEndian.Uint64(b[8:]),
		Node: binary.LittleEndian.Uint32(b[16:]),
		Loop: binary.LittleEndian.Uint16(b[20:]),
		Name: Name(b[22]),
		Sim:  b[23] == 1,
	}
	for j := range r.Fields {
		r.Fields[j] = binary.LittleEndian.Uint64(b[24+8*j:])
	}
	return r
}

// Dump appends the records held as text lines, oldest first, each ending in a newline.
func (g *Ring) Dump(b []byte) []byte {
	for i := range g.Len() {
		r := g.At(i)
		b = r.Append(b)
		b = append(b, '\n')
	}
	return b
}
