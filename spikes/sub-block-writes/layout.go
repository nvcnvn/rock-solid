package subblock

// Record layout as internal/disklog sizes it (record.go, layoutAcc): a 48-byte prelude, the
// entries, a CRC32C per data block, rounded up to whole header blocks; the data area holds whole
// written blocks, then packed payloads, rounded up to whole blocks. layout_test.go checks it
// against disklog.LayoutOf for the entry kinds the format has.
const (
	blockSize   = 4096
	prelude     = 48
	crcSize     = 4
	writeEntry  = 33
	blobEntry   = 14
	copyEntry   = writeEntry + 8 // a cleaner's copy names its source
	patchEntry  = 35             // proposed: kind, pool, chunk, version, block, byte offset, length
	patchCopy   = patchEntry + 8
	maxRecBytes = 1 << 20
)

type recLayout struct {
	headerBytes  int // entries and any payload kept in the header
	dataBytes    int // whole blocks, then packed payloads
	headerBlocks int
	dataBlocks   int
}

func (r *recLayout) finish() {
	r.dataBlocks = (r.dataBytes + blockSize - 1) / blockSize
	r.headerBlocks = (prelude + r.headerBytes + crcSize*r.dataBlocks + blockSize - 1) / blockSize
}

func (r *recLayout) blocks() int { return r.headerBlocks + r.dataBlocks }
