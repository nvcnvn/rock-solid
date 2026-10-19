package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// pebbleRun replays a workload through Pebble's leveled compaction: one batch and one Flush per
// checkpoint, no WAL, no compression, no Bloom filters, one compaction at a time, and a wait for
// compactions to settle before the next flush, as the size model assumes.
func pebbleRun(w workload, valueB int, lbase int64, seed uint64) result {
	opts := &pebble.Options{
		FS:                    vfs.NewMem(),
		DisableWAL:            true,
		MemTableSize:          512 << 20,
		LBaseMaxBytes:         lbase,
		L0CompactionThreshold: 4,
		FormatMajorVersion:    pebble.FormatNewest,
		Logger:                quiet{},
	}
	opts.CompactionConcurrencyRange = func() (int, int) { return 1, 1 }
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return pebble.DBCompressionNone })
	db, err := pebble.Open("db", opts)
	must(err)
	defer db.Close()
	rng := rand.New(rand.NewPCG(seed, 2))
	kb, vb := make([]byte, 16), make([]byte, valueB)
	var ver uint64
	settle := func(peak *float64, live float64) {
		quiet := 0
		last := int64(-1)
		for quiet < 3 {
			time.Sleep(3 * time.Millisecond)
			m := db.Metrics()
			if live > 0 {
				*peak = max(*peak, float64(tableBytes(m)+m.Compact.InProgressBytes)/live)
			}
			if m.Compact.NumInProgress == 0 && m.Compact.Count == last {
				quiet++
			} else {
				quiet = 0
			}
			last = m.Compact.Count
		}
	}
	batchOf := func(ids func(i int) uint64, n int) {
		b := db.NewBatch()
		for i := range n {
			ver++
			must(b.Set(keyOf(kb, ids(i)), valueOf(vb, valueB, ver), nil))
		}
		must(b.Commit(pebble.NoSync))
		must(b.Close())
		must(db.Flush())
	}
	batchOf(func(i int) uint64 { return uint64(i) }, w.keys)
	var dummy float64
	settle(&dummy, 0)
	must(db.Compact(context.Background(), []byte{0}, []byte{0xff, 0xff}, false))
	live := float64(tableBytes(db.Metrics()))
	res := result{policy: fmt.Sprintf("pebble leveled (LBase %d MiB)", lbase>>20), workload: w.name, liveB: live}
	half := len(w.flushes) / 2
	var flushed0, compacted0 uint64
	samples := 0
	for fi, n := range w.flushes {
		if fi == half {
			flushed0, compacted0 = flushCompactBytes(db.Metrics())
		}
		measure := fi >= half
		batchOf(func(int) uint64 { return rng.Uint64N(uint64(w.keys)) }, n)
		var peak float64
		settle(&peak, live)
		m := db.Metrics()
		res.runsMaxEver = max(res.runsMaxEver, m.ReadAmp())
		if measure {
			res.flushes++
			samples++
			ra := m.ReadAmp()
			res.runsMean += float64(ra)
			res.runsMax = max(res.runsMax, ra)
			sp := float64(tableBytes(m)) / live
			res.spaceMean += sp
			res.spaceMax = max(res.spaceMax, sp)
			res.peakSpace = max(res.peakSpace, peak)
		}
	}
	f1, c1 := flushCompactBytes(db.Metrics())
	res.flushedB, res.compactedB = float64(f1-flushed0), float64(c1-compacted0)
	res.compactions = int(db.Metrics().Compact.Count)
	res.runsMean /= float64(samples)
	res.spaceMean /= float64(samples)
	return res
}

func tableBytes(m *pebble.Metrics) int64 {
	var n int64
	for i := range m.Levels {
		n += m.Levels[i].AggregateSize()
	}
	return n
}

func flushCompactBytes(m *pebble.Metrics) (flushed, compacted uint64) {
	for i := range m.Levels {
		flushed += m.Levels[i].TableBytesFlushed
		compacted += m.Levels[i].TableBytesCompacted
	}
	return flushed, compacted
}

type quiet struct{}

func (quiet) Infof(string, ...interface{})  {}
func (quiet) Errorf(string, ...interface{}) {}
func (quiet) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
