package uring

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
)

// The crash test runs a store on a file-backed disk in a child process (this test binary, with
// RS_CRASH_CHILD set), sends it a seeded workload, kills it with SIGKILL while changes are in
// flight, restarts it over the same file and checks what it recovered against the crash oracle,
// a file the driver keeps outside the child. The oracle file has one record per line:
//
//	write <seq> <chunk> <off> <len> <data seed>   sent: a write of len bytes from core.NewRand(data seed).Fill
//	punch <seq> <chunk> <off> <len>                sent: a punch
//	ack <seq>                                      acknowledged; fsynced before the driver counts it
//	kill <cycle>                                   the child was killed
//	kept <chunk> <seq>                             the restart recovered the chunk's changes up to seq (0: none since the last kept)
//	torn <chunk> <seq> <blocks>                    and the first blocks of the next change, seq, unacknowledged
const (
	crashChunks = 4
	crashWindow = 8
	cmdBytes    = 21
	crashSpan   = 4
)

func TestCrashChild(t *testing.T) {
	path := os.Getenv("RS_CRASH_CHILD")
	if path == "" {
		t.Skip("the crash test's child process")
	}
	sd := &suiteDisk{t: t, path: path}
	sd.start()
	cfg := storetest.Config()
	cfg.Log.MaxReplayBytes = 256 << 10
	var r *storetest.Rig
	if os.Getenv("RS_CRASH_FORMAT") != "" {
		r = storetest.NewRig(t, sd, cfg)
	} else {
		r = storetest.OpenRig(t, sd, cfg)
	}
	if mw := r.S.Log().MaxWriteBlocks(); mw < crashSpan {
		t.Fatalf("a write of %d blocks takes several log items (at most %d blocks each); the driver checks a write as one", crashSpan, mw)
	}
	cmds := bufio.NewReaderSize(os.NewFile(3, "commands"), 1<<20)
	acks := os.NewFile(4, "acks")
	for c := range crashChunks {
		if _, err := acks.Write(r.Chunk(crashChunk(c))); err != nil {
			t.Fatal(err)
		}
	}
	type sent struct{ id, seq uint64 }
	var out []sent
	var hdr [cmdBytes]byte
	var ack [8]byte
	for {
		for len(out) < crashWindow {
			if _, err := io.ReadFull(cmds, hdr[:]); err != nil {
				t.Fatal(err)
			}
			seq := binary.LittleEndian.Uint64(hdr[1:])
			c := crashChunk(int(binary.LittleEndian.Uint32(hdr[9:])))
			off, n := binary.LittleEndian.Uint32(hdr[13:]), binary.LittleEndian.Uint32(hdr[17:])
			if hdr[0] == 'p' {
				out = append(out, sent{r.Punch(c, off, n, 0), seq})
				continue
			}
			data := make([]byte, n)
			if _, err := io.ReadFull(cmds, data); err != nil {
				t.Fatal(err)
			}
			out = append(out, sent{r.Write(c, off, data, 0), seq})
		}
		if !r.Step() {
			t.Fatalf("%d changes unanswered with nothing in flight", len(out))
		}
		kept := out[:0]
		for _, s := range out {
			p, ok := r.Reply(s.id)
			if !ok {
				kept = append(kept, s)
				continue
			}
			if p.Err != nil {
				t.Fatalf("change %d: %v", s.seq, p.Err)
			}
			binary.LittleEndian.PutUint64(ack[:], s.seq)
			if _, err := acks.Write(ack[:]); err != nil {
				t.Fatal(err)
			}
		}
		out = kept
	}
}

func crashChunk(i int) core.ChunkID { return storetest.Chunk(1 + uint32(i)) }

// TestCrashOnARealDisk kills the store's process RS_CRASH_CYCLES times (default 150) on seed
// RS_CRASH_SEED (default 1), keeping the disk file and the oracle in RS_CRASH_DIR if it is set.
func TestCrashOnARealDisk(t *testing.T) {
	newRing(t, 1<<20, 8)
	seed, cycles := envUint(t, "RS_CRASH_SEED", 1), envUint(t, "RS_CRASH_CYCLES", 150)
	dir := os.Getenv("RS_CRASH_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	ct := &crashTest{t: t, seed: seed, rnd: core.NewRand(seed), changes: core.NewRand(seed + 1<<32), pace: core.NewRand(seed + 2<<32), disk: filepath.Join(dir, "disk"), oracle: filepath.Join(dir, "oracle")}
	if err := CreateFile(ct.disk, storetest.DiskBytes); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(ct.oracle)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ct.file, ct.w = f, bufio.NewWriter(f)
	if ct.read, err = os.Open(ct.oracle); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ct.read.Close() }()
	ct.lines = bufio.NewScanner(ct.read)
	for i := range ct.base {
		ct.base[i] = make([]byte, core.ChunkSize)
	}
	for cycle := range cycles {
		ct.cycle(cycle)
	}
	ct.start(false)
	ct.check()
	ct.kill()
	t.Logf("seed %d: %d cycles, %d changes sent, %d acknowledged; restarts that kept a change never acknowledged: %d, lost one: %d, kept the first rounds of one: %d",
		seed, cycles, ct.seq, ct.acked, ct.keptCycles, ct.lostCycles, ct.tornCycles)
}

type crashTest struct {
	t    *testing.T
	seed uint64
	rnd  *core.Rand
	// changes draws the changes sent, pace the pauses between them, and rnd the kills, so the
	// timing of a run leaves the changes and the kill points as the seed draws them.
	changes, pace *core.Rand
	disk          string
	oracle        string
	file          *os.File
	w             *bufio.Writer
	read          *os.File
	lines         *bufio.Scanner
	base          [crashChunks][]byte

	child   *exec.Cmd
	cmds    *os.File
	ackPipe *os.File
	acks    *bufio.Reader
	stderr  bytes.Buffer

	seq, acked                         uint64
	keptCycles, lostCycles, tornCycles int
}

func (ct *crashTest) cycle(cycle uint64) {
	ct.start(cycle == 0)
	ct.check()
	killAt := ct.acked + ct.rnd.Range(1, 400)
	for range crashWindow {
		ct.send()
	}
	var ack [8]byte
	for ct.acked < killAt {
		ct.read8(ack[:])
		n := 1
		ct.record("ack %d", binary.LittleEndian.Uint64(ack[:]))
		for ct.acks.Buffered() >= 8 {
			ct.read8(ack[:])
			ct.record("ack %d", binary.LittleEndian.Uint64(ack[:]))
			n++
		}
		ct.sync()
		ct.acked += uint64(n)
		for range n {
			ct.send()
		}
		if ct.pace.Chance(1, 50) {
			time.Sleep(time.Duration(ct.pace.Below(300)) * time.Microsecond)
		}
	}
	time.Sleep(time.Duration(ct.rnd.Below(500)) * time.Microsecond)
	ct.kill()
	ct.drain()
	ct.record("kill %d", cycle)
	ct.sync()
}

func (ct *crashTest) send() {
	ct.seq++
	c := int(ct.changes.Below(crashChunks))
	off := uint32(ct.changes.Below(core.ChunkSize - 1))
	n := uint32(min(ct.changes.Range(1, 3*core.BlockSize), core.ChunkSize-uint64(off)))
	var hdr [cmdBytes]byte
	binary.LittleEndian.PutUint64(hdr[1:], ct.seq)
	binary.LittleEndian.PutUint32(hdr[9:], uint32(c))
	binary.LittleEndian.PutUint32(hdr[13:], off)
	binary.LittleEndian.PutUint32(hdr[17:], n)
	msg := hdr[:]
	if ct.changes.Chance(1, 6) {
		hdr[0] = 'p'
		ct.record("punch %d %d %d %d", ct.seq, c, off, n)
	} else {
		hdr[0] = 'w'
		ds := ct.changes.Uint64()
		ct.record("write %d %d %d %d %d", ct.seq, c, off, n, ds)
		msg = append(msg, writeData(ds, n)...)
	}
	if err := ct.w.Flush(); err != nil {
		ct.t.Fatal(err)
	}
	if _, err := ct.cmds.Write(msg); err != nil {
		ct.fail("sending change %d: %v", ct.seq, err)
	}
}

func writeData(seed uint64, n uint32) []byte {
	b := make([]byte, n)
	core.NewRand(seed).Fill(b)
	return b
}

func (ct *crashTest) start(format bool) {
	cmdR, cmdW, err := os.Pipe()
	if err != nil {
		ct.t.Fatal(err)
	}
	ackR, ackW, err := os.Pipe()
	if err != nil {
		ct.t.Fatal(err)
	}
	ct.stderr.Reset()
	ct.child = exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1", "-test.timeout=0")
	ct.child.Env = append(os.Environ(), "RS_CRASH_CHILD="+ct.disk)
	if format {
		ct.child.Env = append(ct.child.Env, "RS_CRASH_FORMAT=1")
	}
	ct.child.ExtraFiles = []*os.File{cmdR, ackW}
	ct.child.Stdout, ct.child.Stderr = &ct.stderr, &ct.stderr
	if err := ct.child.Start(); err != nil {
		ct.t.Fatal(err)
	}
	_ = cmdR.Close()
	_ = ackW.Close()
	ct.cmds, ct.ackPipe, ct.acks = cmdW, ackR, bufio.NewReaderSize(ackR, 1<<20)
}

func (ct *crashTest) kill() {
	_ = ct.child.Process.Signal(syscall.SIGKILL)
	_ = ct.child.Wait()
	_ = ct.cmds.Close()
}

// drain counts the acknowledgments the killed child sent that the driver had not read yet: a
// client would have seen them.
func (ct *crashTest) drain() {
	var ack [8]byte
	for {
		_, err := io.ReadFull(ct.acks, ack[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			ct.fail("draining acknowledgments: %v", err)
		}
		ct.record("ack %d", binary.LittleEndian.Uint64(ack[:]))
		ct.acked++
	}
	_ = ct.ackPipe.Close()
}

func (ct *crashTest) read8(b []byte) {
	if _, err := io.ReadFull(ct.acks, b); err != nil {
		ct.fail("reading an acknowledgment: %v", err)
	}
}

func (ct *crashTest) record(format string, args ...any) {
	if _, err := fmt.Fprintf(ct.w, format+"\n", args...); err != nil {
		ct.t.Fatal(err)
	}
}

func (ct *crashTest) sync() {
	if err := ct.w.Flush(); err != nil {
		ct.t.Fatal(err)
	}
	if err := ct.file.Sync(); err != nil {
		ct.t.Fatal(err)
	}
}

type oracleChange struct {
	seq, off, n, ds uint64
	punch, acked    bool
}

// check reads what the restarted child recovered and checks it against the oracle file's records
// since the last check: each chunk must hold its changes, in the order sent, up to one at or past
// the last acknowledged, and none after it.
func (ct *crashTest) check() {
	var pending [crashChunks][]oracleChange
	for ct.lines.Scan() {
		f := strings.Fields(ct.lines.Text())
		v := make([]uint64, len(f)-1)
		for i := range v {
			var err error
			if v[i], err = strconv.ParseUint(f[i+1], 10, 64); err != nil {
				ct.t.Fatalf("oracle line %q: %v", ct.lines.Text(), err)
			}
		}
		switch f[0] {
		case "write", "punch":
			ch := oracleChange{seq: v[0], off: v[2], n: v[3], punch: f[0] == "punch"}
			if !ch.punch {
				ch.ds = v[4]
			}
			pending[v[1]] = append(pending[v[1]], ch)
		case "ack":
			ct.markAcked(&pending, v[0])
		}
	}
	if err := ct.lines.Err(); err != nil {
		ct.t.Fatal(err)
	}
	ct.lines = bufio.NewScanner(ct.read)
	got := make([]byte, core.ChunkSize)
	kept, lost, cut := false, false, false
	for c := range crashChunks {
		if _, err := io.ReadFull(ct.acks, got); err != nil {
			ct.fail("reading recovered chunk %d: %v", c, err)
		}
		chs := pending[c]
		last := -1
		for i, ch := range chs {
			if ch.acked {
				last = i
			}
		}
		want := ct.base[c]
		for i := range last + 1 {
			apply(want, chs[i])
		}
		k, torn := last, 0
		for !bytes.Equal(got, want) {
			if k+1 == len(chs) {
				saved := filepath.Join(filepath.Dir(ct.oracle), fmt.Sprintf("recovered-%d", c))
				_ = os.WriteFile(saved, got, 0o644)
				ct.fail("chunk %d recovered as no prefix of its changes holding every acknowledged one (%d sent since the last restart, the last acknowledged #%d; kept in %s): %v",
					c, len(chs), last, saved, storetest.Diff(got, ct.base[c]))
			}
			next := bytes.Clone(want)
			apply(next, chs[k+1])
			if torn = tornBlocks(got, want, next, chs[k+1]); torn > 0 {
				copy(want, got)
				break
			}
			copy(want, next)
			k++
		}
		kept = kept || k > last || torn > 0
		lost = lost || k < len(chs)-1
		var seq uint64
		if k >= 0 {
			seq = chs[k].seq
		}
		ct.record("kept %d %d", c, seq)
		if torn > 0 {
			ct.record("torn %d %d %d", c, chs[k+1].seq, torn)
			cut = true
		}
	}
	ct.sync()
	if cut {
		ct.tornCycles++
	}
	if kept {
		ct.keptCycles++
	}
	if lost {
		ct.lostCycles++
	}
}

// tornBlocks is how many of ch's blocks, counted from its first, got holds as next has them, the
// rest as before holds them, when that count ends one of ch's log items short of its last: an
// unaligned punch logs its edge blocks as writes apart from the punch of its middle, so a crash
// may keep the first items alone. A write here is one item, and an item comes back whole or not.
func tornBlocks(got, before, next []byte, ch oracleChange) int {
	if !ch.punch || bytes.Equal(got, next) {
		return 0
	}
	first, last := ch.off/core.BlockSize, (ch.off+ch.n-1)/core.BlockSize
	if first == last {
		return 0
	}
	var cuts []uint64
	if ch.off%core.BlockSize != 0 {
		cuts = append(cuts, 1)
	}
	if (ch.off+ch.n)%core.BlockSize != 0 {
		cuts = append(cuts, last-first)
	}
	for _, j := range cuts {
		b := (first + j) * core.BlockSize
		if bytes.Equal(got[:b], next[:b]) && bytes.Equal(got[b:], before[b:]) {
			return int(j)
		}
	}
	return 0
}

func TestTornBlocksCutOnlyBetweenLogItems(t *testing.T) {
	ones := bytes.Repeat([]byte{1}, core.ChunkSize)
	for _, tc := range []struct {
		name string
		ch   oracleChange
		kept uint64
		want int
	}{
		{"a 3-block write, one item, kept its first block", oracleChange{off: 10 * core.BlockSize, n: 3 * core.BlockSize, ds: 7}, 1, 0},
		{"an aligned 4-block punch, one item, kept half", oracleChange{off: 20 * core.BlockSize, n: 4 * core.BlockSize, punch: true}, 2, 0},
		{"an unaligned punch kept its head edge", oracleChange{off: 20*core.BlockSize + 9, n: 3 * core.BlockSize, punch: true}, 1, 1},
		{"an unaligned punch kept its head edge and middle", oracleChange{off: 20*core.BlockSize + 9, n: 3 * core.BlockSize, punch: true}, 3, 3},
		{"an unaligned punch kept part of its middle", oracleChange{off: 20*core.BlockSize + 9, n: 3 * core.BlockSize, punch: true}, 2, 0},
	} {
		next := bytes.Clone(ones)
		apply(next, tc.ch)
		b := (tc.ch.off/core.BlockSize + tc.kept) * core.BlockSize
		got := append(bytes.Clone(next[:b]), ones[b:]...)
		if n := tornBlocks(got, ones, next, tc.ch); n != tc.want {
			t.Errorf("%s: tornBlocks %d, want %d", tc.name, n, tc.want)
		}
	}
}

func (ct *crashTest) markAcked(pending *[crashChunks][]oracleChange, seq uint64) {
	for c := range pending {
		for i := range pending[c] {
			if pending[c][i].seq == seq {
				pending[c][i].acked = true
				return
			}
		}
	}
	ct.t.Fatalf("oracle acknowledges change %d, never sent since the last restart", seq)
}

func apply(b []byte, ch oracleChange) {
	if ch.punch {
		clear(b[ch.off : ch.off+ch.n])
		return
	}
	copy(b[ch.off:], writeData(ch.ds, uint32(ch.n)))
}

func (ct *crashTest) fail(format string, args ...any) {
	ct.t.Helper()
	ct.kill()
	ct.t.Fatalf("seed %d: %s\nreplay: RS_CRASH_SEED=%d go test -run TestCrashOnARealDisk ./internal/driver/uring/\noracle: %s\nchild output:\n%s",
		ct.seed, fmt.Sprintf(format, args...), ct.seed, ct.oracle, ct.stderr.String())
}

func envUint(t *testing.T, name string, def uint64) uint64 {
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}
