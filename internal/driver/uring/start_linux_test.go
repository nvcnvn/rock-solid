package uring

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// TestStartChild is a storage node starting its device, the disk file RS_START_CHILD, with log IDs
// drawn from RS_START_SEED and no trims if RS_START_NOTRIM is true: it formats the device again under RS_START_FORMAT if set, else does
// what chunkstore.StartDevice says, and sends itself SIGKILL after RS_START_KILL rounds of format's
// completions if set. To the file RS_START_OUT it appends 'f' and the log ID before it formats,
// 'n' and the rounds a format took, 'o', the log ID and chunk 1 once it serves, or 'x' when it
// leaves the device to the operator.
func TestStartChild(t *testing.T) {
	path := os.Getenv("RS_START_CHILD")
	if path == "" {
		t.Skip("the start test's child process")
	}
	out, err := os.OpenFile(os.Getenv("RS_START_OUT"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	put := func(kind byte, v uint64, rest []byte) {
		b := binary.LittleEndian.AppendUint64([]byte{kind}, v)
		if _, err := out.Write(append(b, rest...)); err != nil {
			t.Fatal(err)
		}
	}
	sd := &suiteDisk{t: t, path: path}
	sd.start()
	r, act, log := storetest.StartRig(t, sd, startConfig(os.Getenv("RS_START_NOTRIM") == "true"), core.NewRand(envUint(t, "RS_START_SEED", 0)))
	if v := envUint(t, "RS_START_FORMAT", 0); v != 0 {
		act, log = chunkstore.StartFormat, v
	}
	switch act {
	case chunkstore.StartFailed:
		put('x', 0, nil)
		return
	case chunkstore.StartFormat:
		put('f', log, nil)
		r.Format(log)
		kill := envUint(t, "RS_START_KILL", 0)
		n := uint64(0)
		for ; ; n++ {
			if n == kill && kill > 0 {
				_ = unix.Kill(unix.Getpid(), unix.SIGKILL)
				select {}
			}
			if !r.Step() {
				break
			}
		}
		put('n', n, nil)
	}
	put('o', r.S.Log().Superblock().LogID, r.Chunk(storetest.Chunk(1)))
}

func startConfig(noTrim bool) chunkstore.Config {
	cfg := storetest.Config()
	cfg.Log.NoTrim = noTrim
	return cfg
}

func kinds(recs []startRecord) []byte {
	var ks []byte
	for _, r := range recs {
		ks = append(ks, r.kind)
	}
	return ks
}

// startRecord is one record a start child appended.
type startRecord struct {
	kind  byte
	v     uint64
	chunk []byte
}

// startChild runs a start child on the disk file path and returns its records; kill > 0 lets it
// end killed.
func startChild(t *testing.T, path string, noTrim bool, seed, format, kill uint64) []startRecord {
	t.Helper()
	outPath := path + ".out"
	_ = os.Remove(outPath)
	c := exec.Command(os.Args[0], "-test.run=^TestStartChild$", "-test.count=1", "-test.timeout=0")
	c.Env = append(os.Environ(), "RS_START_CHILD="+path, "RS_START_OUT="+outPath, fmt.Sprint("RS_START_SEED=", seed),
		fmt.Sprint("RS_START_FORMAT=", format), fmt.Sprint("RS_START_KILL=", kill), fmt.Sprint("RS_START_NOTRIM=", noTrim))
	var stderr bytes.Buffer
	c.Stdout, c.Stderr = &stderr, &stderr
	err := c.Run()
	var ee *exec.ExitError
	if err != nil && !(kill > 0 && errors.As(err, &ee) && ee.ProcessState.Sys().(syscall.WaitStatus).Signal() == syscall.SIGKILL) {
		t.Fatalf("start child (seed %d, format %#x, kill %d): %v\n%s", seed, format, kill, err, stderr.String())
	}
	b, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	var recs []startRecord
	for len(b) > 0 {
		rec := startRecord{kind: b[0], v: binary.LittleEndian.Uint64(b[1:])}
		b = b[9:]
		if rec.kind == 'o' {
			rec.chunk, b = b[:core.ChunkSize], b[core.ChunkSize:]
		}
		recs = append(recs, rec)
	}
	return recs
}

// The review of "Storage node disk startup": a node's process formatting its device again over a
// log and killed after each of the first and last rounds of format's completions and one between,
// then started and killed after as many rounds of the format that start runs, comes back at each
// start with its old log whole, or formatted under a log ID no format drew before, which the record
// takes as a new disk; with trims, and with none, as a node formats a disk that is a file.
func TestAStartKilledTwiceMidFormatComesBackWholeOrNew(t *testing.T) {
	for _, noTrim := range []bool{false, true} {
		t.Run(fmt.Sprint("NoTrim=", noTrim), func(t *testing.T) { killFormatsTwice(t, noTrim) })
	}
}

func killFormatsTwice(t *testing.T, noTrim bool) {
	path := diskFile(t, storetest.DiskBytes/bs)
	data := storetest.Fill(3*core.BlockSize, 7)
	oldLog := func() {
		sd := &suiteDisk{t: t, path: path}
		sd.start()
		r := storetest.NewRig(t, sd, startConfig(noTrim))
		r.OK(r.Write(storetest.Chunk(1), 0, data, 0))
		sd.stop()
	}
	oldLog()
	recs := startChild(t, path, noTrim, 1, 0xc0, 0)
	if len(recs) != 3 || recs[1].kind != 'n' {
		t.Fatalf("an unkilled format: records %q", kinds(recs))
	}
	total := int(recs[1].v)
	ks := []int{1, 2, 3, 4, 5, 6, 7, 8, total / 2}
	for k := max(9, total-8); k <= total; k++ {
		ks = append(ks, k)
	}
	ks = slices.DeleteFunc(ks, func(k int) bool { return k < 1 || k > total })
	slices.Sort(ks)
	ks = slices.Compact(ks)
	var old, fresh int
	for _, k := range ks {
		oldLog()
		var rec chunkstore.DiskRecord
		id, _ := rec.Report(0, 1, 0x5eed, "")
		cut := core.NewRand(uint64(k)).Uint64()
		logs := []uint64{0x5eed, cut}
		startChild(t, path, noTrim, 0, cut, uint64(k))
		opened := false
		for start := uint64(0); start < 2 && !opened; start++ {
			for _, rc := range startChild(t, path, noTrim, uint64(k)<<8|start, 0, uint64(k)*(1-start)) {
				switch rc.kind {
				case 'f':
					if slices.Contains(logs, rc.v) {
						t.Fatalf("kill after %d rounds: start %d formats under log %#x again", k, start, rc.v)
					}
					logs = append(logs, rc.v)
				case 'o':
					opened = true
					nid, err := rec.Report(0, 1, rc.v, "")
					switch {
					case err != nil:
						t.Fatalf("kill after %d rounds: start %d opened log %#x, refused: %v", k, start, rc.v, err)
					case rc.v == 0x5eed && nid == id && bytes.Equal(rc.chunk[:len(data)], data):
						old++
					case rc.v != 0x5eed && slices.Contains(logs, rc.v) && nid > id && !slices.ContainsFunc(rc.chunk, func(b byte) bool { return b != 0 }):
						fresh++
					default:
						t.Fatalf("kill after %d rounds: start %d opened log %#x as disk %d, old log disk %d, formats drew %#x", k, start, rc.v, nid, id, logs)
					}
				case 'x':
					t.Fatalf("kill after %d rounds: start %d left the device to the operator", k, start)
				}
			}
		}
		if !opened {
			t.Fatalf("kill after %d rounds: the unkilled start did not open the device", k)
		}
	}
	t.Logf("format took %d rounds; %d starts found the old log, %d a new one", total, old, fresh)
	// With no trims a format's first write is the new block 0, and a kill lets every write that
	// completed land, so no kill after a round finds the old log.
	if old == 0 && !noTrim || fresh == 0 {
		t.Fatalf("old log %d, new log %d", old, fresh)
	}
}

// The review of "Storage node disk startup": a device whose superblock and copy both fail their
// checksums is reported failed by a node's process and left byte for byte as it was.
func TestAStartLeavesADeviceWithNoValidSuperblockAsItWas(t *testing.T) {
	path := diskFile(t, storetest.DiskBytes/bs)
	sd := &suiteDisk{t: t, path: path}
	sd.start()
	r := storetest.NewRig(t, sd, storetest.Config())
	r.OK(r.Write(storetest.Chunk(1), 0, storetest.Fill(core.BlockSize, 7), 0))
	geo := r.S.Log().Superblock().Geometry
	sd.stop()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, lba := range []core.LBA{geo.SuperblockLBA(), geo.SuperblockCopyLBA()} {
		var b [1]byte
		off := int64(lba)*core.BlockSize + 16
		if _, err := f.ReadAt(b[:], off); err != nil {
			t.Fatal(err)
		}
		b[0] ^= 1
		if _, err := f.WriteAt(b[:], off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileSum(t, path)
	if recs := startChild(t, path, false, 1, 0, 0); len(recs) != 1 || recs[0].kind != 'x' {
		t.Fatalf("a device with no valid superblock: records %q", kinds(recs))
	}
	if fileSum(t, path) != before {
		t.Fatal("starting a device with no valid superblock changed it")
	}
}

func fileSum(t *testing.T, path string) [sha256.Size]byte {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(b)
}
