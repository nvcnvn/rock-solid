package subblock

import "testing"

// Space a small file costs, per the design's figures: a 4 KiB block per copy plus its 33-byte
// write entry and 4-byte CRC in a header shared with the round's other items, or a header block
// of its own when the file is flushed alone; an erasure-coded pool encodes a chunk only when its
// shards, each rounded up to a block, take less than its copies (docs/design.md, "When chunks
// are encoded"). A short block stores only the file's bytes, packed, with a 35-byte entry.

func blocksFor(n int) int { return (n + blockSize - 1) / blockSize }

func perCopy(size int, alone, short bool) int {
	b := blocksFor(size)
	data := b * blockSize
	entry := writeEntry
	if short {
		data, entry = size, writeEntry+2
	}
	hdr := entry + crcSize*b
	if alone {
		var r recLayout
		r.headerBytes, r.dataBytes = entry, data
		r.finish()
		return r.blocks() * blockSize
	}
	return data + hdr
}

type pool struct {
	name         string
	copies, k, m int
}

func pooled(p pool, size int, alone, short bool) (int, string) {
	copies := p.copies * perCopy(size, alone, short)
	if p.k == 0 {
		return copies, "copies"
	}
	shard := blocksFor((size+p.k-1)/p.k) * blockSize
	encoded := (p.k + p.m) * (shard + writeEntry + crcSize*blocksFor(shard))
	if alone {
		encoded = (p.k + p.m) * (shard + blockSize)
	}
	if encoded < copies {
		return encoded, "shards"
	}
	return copies, "copies"
}

func TestSmallFileSpace(t *testing.T) {
	pools := []pool{{"rep3", 3, 0, 0}, {"ec2-1", 2, 2, 1}, {"ec4-2", 3, 4, 2}, {"ec16-2", 3, 16, 2}}
	sizes := []int{100, 1024, 1536, 2048, 4096, 6144, 8192, 16384, 24576}
	for _, p := range pools {
		nominal := float64(p.copies)
		if p.k > 0 {
			nominal = float64(p.k+p.m) / float64(p.k)
		}
		for _, size := range sizes {
			shared, form := pooled(p, size, false, false)
			alone, _ := pooled(p, size, true, false)
			short, sform := pooled(p, size, false, true)
			t.Logf("S1 %-6s %6d B file: %6d B raw as %-6s (%5.2fx, %4.1fx the pool's %.2fx) in a shared round, %6d B flushed alone; short block %6d B as %s (%5.2fx)",
				p.name, size, shared, form, float64(shared)/float64(size), float64(shared)/float64(size)/nominal, nominal, alone, short, sform, float64(short)/float64(size))
		}
	}
	const (
		metaRun    = 133 // MetaStore run bytes per file (design, LSM tree)
		chunkEntry = 28 + 16
		extent     = 38 + 26
	)
	t.Logf("S2 index memory per file replica: MetaStore %d B in runs, chunk index %d B raw (chunk entry %d + one extent %d) before prefix compression",
		metaRun, chunkEntry+extent, chunkEntry, extent)
	for _, size := range []int{60, 112, 512, 1024, 1536, 2048} {
		inline := metaRun + size
		t.Logf("S3 inline %4d B in the inode: %5d B per file replica of MetaStore memory (%4.1fx), a 16 GiB budget holds %5.1f million files at 3 replicas (%.1f million without inline data)",
			size, inline, float64(inline)/metaRun, 16*float64(1<<30)/float64(inline)/3/1e6, 16*float64(1<<30)/metaRun/3/1e6)
	}
}
