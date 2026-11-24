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

	"github.com/anishathalye/porcupine"
	"golang.org/x/sys/unix"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/lincheck"
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
//
// Beside the driver, histClients client processes per child (this test binary, with
// RS_HIST_CLIENT set) write and read 8-byte slots, one block each, in chunks of their own, one
// operation at a time. The driver and the clients record every operation they send in the run's
// history file, and the run ends with a Porcupine check of the slots' reads and writes. Commands
// share one pipe into the child, each written whole as pipes keep writes of at most PIPE_BUF:
// kind, sender (0 the driver, else the client's reply pipe), seq, chunk, offset, length and the
// data seed or, for a slot, the value.
const (
	crashChunks = 4
	crashWindow = 8
	cmdBytes    = 30
	crashSpan   = 4
	histClients = 2
	slotChunks  = 2
	// slotGroup is how many slots a cycle's clients add; they use those and the previous cycle's,
	// so each slot's history spans a kill and stays short.
	slotGroup      = 8
	blocksPerChunk = core.ChunkSize / core.BlockSize
)

func putCmd(b *[cmdBytes]byte, kind, sender byte, seq uint64, chunk int, off, n uint32, ds uint64) {
	b[0], b[1] = kind, sender
	binary.LittleEndian.PutUint64(b[2:], seq)
	binary.LittleEndian.PutUint32(b[10:], uint32(chunk))
	binary.LittleEndian.PutUint32(b[14:], off)
	binary.LittleEndian.PutUint32(b[18:], n)
	binary.LittleEndian.PutUint64(b[22:], ds)
}

func TestCrashChild(t *testing.T) {
	path := os.Getenv("RS_CRASH_CHILD")
	if path == "" {
		t.Skip("the crash test's child process")
	}
	replies := make([]*os.File, envUint(t, "RS_CRASH_CLIENTS", 0))
	for i := range replies {
		replies[i] = os.NewFile(uintptr(5+i), "replies")
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
	type sent struct {
		id, seq uint64
		sender  byte
		read    bool
		off     uint32
	}
	var out []sent
	var hdr [cmdBytes]byte
	var ack [8]byte
	var reply [16]byte
	for {
		for len(out) < crashWindow {
			if _, err := io.ReadFull(cmds, hdr[:]); err != nil {
				t.Fatal(err)
			}
			s := sent{seq: binary.LittleEndian.Uint64(hdr[2:]), sender: hdr[1], off: binary.LittleEndian.Uint32(hdr[14:])}
			c := crashChunk(int(binary.LittleEndian.Uint32(hdr[10:])))
			n, ds := binary.LittleEndian.Uint32(hdr[18:]), binary.LittleEndian.Uint64(hdr[22:])
			switch hdr[0] {
			case 'p':
				s.id = r.Punch(c, s.off, n, 0)
			case 'w':
				s.id = r.Write(c, s.off, writeData(ds, n), 0)
			case 's':
				s.id = r.Write(c, s.off, binary.LittleEndian.AppendUint64(nil, ds), 0)
			case 'r':
				s.id, s.read = r.Read(chunkstore.Request{Chunk: c, Off: s.off, Len: 8}), true
			default:
				t.Fatalf("command %q", hdr[0])
			}
			out = append(out, s)
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
				t.Fatalf("sender %d's command %d: %v", s.sender, s.seq, p.Err)
			}
			if s.sender == 0 {
				binary.LittleEndian.PutUint64(ack[:], s.seq)
				if _, err := acks.Write(ack[:]); err != nil {
					t.Fatal(err)
				}
				continue
			}
			binary.LittleEndian.PutUint64(reply[:], s.seq)
			clear(reply[8:])
			if s.read {
				copy(reply[8:], r.Pool().Bytes(p.Buf)[s.off%core.BlockSize:])
				r.Pool().Free(p.Buf)
			}
			if _, err := replies[s.sender-1].Write(reply[:]); err != nil {
				t.Fatal(err)
			}
		}
		out = kept
	}
}

func crashChunk(i int) core.ChunkID { return storetest.Chunk(1 + uint32(i)) }

// TestHistClient is a history client of the crash test: fd 3 the child's command pipe, fd 4 its
// replies. It stops, its operation in flight never completed, when the child is killed.
func TestHistClient(t *testing.T) {
	path := os.Getenv("RS_HIST_CLIENT")
	if path == "" {
		t.Skip("the crash test's history clients")
	}
	id, cycle := envUint(t, "RS_HIST_ID", 0), envUint(t, "RS_HIST_CYCLE", 0)
	h, err := openHist(path)
	if err != nil {
		t.Fatal(err)
	}
	cmds, replies := os.NewFile(3, "commands"), os.NewFile(4, "replies")
	rnd := core.NewRand(envUint(t, "RS_CRASH_SEED", 1) + id<<40)
	var cmd [cmdBytes]byte
	var reply [16]byte
	for seq := uint64(1); ; seq++ {
		b := ((cycle+rnd.Below(2))*slotGroup + rnd.Below(slotGroup)) % (slotChunks * blocksPerChunk)
		kind, v := byte('r'), uint64(0)
		if rnd.Chance(1, 2) {
			kind, v = 's', id<<32|seq
		}
		putCmd(&cmd, kind, byte(1+(id-1)%histClients), seq, crashChunks+int(b/blocksPerChunk), uint32(b%blocksPerChunk*core.BlockSize), 8, v)
		if err := h.invoke(uint32(id), seq, [sim.OpWords]uint64{uint64(kind), b, v}); err != nil {
			t.Fatal(err)
		}
		if _, err := cmds.Write(cmd[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(replies, reply[:]); err != nil {
			return
		}
		if got := binary.LittleEndian.Uint64(reply[:]); got != seq {
			t.Fatalf("reply to %d, waiting for %d", got, seq)
		}
		if err := h.complete(uint32(id), seq, [sim.OpWords]uint64{binary.LittleEndian.Uint64(reply[8:])}); err != nil {
			t.Fatal(err)
		}
	}
}

// histFile appends one process's events to a run's history file, each in one write: on a local Linux
// filesystem an O_APPEND write to a regular file lands whole, so several processes' events
// interleave without tearing, and a torn one makes sim.ReadHistory refuse the file.
type histFile struct {
	f  *os.File
	ts unix.Timespec
	e  [sim.EventBytes]byte
}

func openHist(path string) (*histFile, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	return &histFile{f: f}, err
}

// nanos reads CLOCK_MONOTONIC_RAW, one clock for every process on the host ("Consistency
// checking" in docs/design.md says why this one).
func (h *histFile) nanos() int64 {
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &h.ts); err != nil {
		panic(err)
	}
	return h.ts.Nano()
}

// invoke stamps the operation's start rounded down and complete its end rounded up, so its
// interval covers what the clock saw.
func (h *histFile) invoke(client uint32, op uint64, in [sim.OpWords]uint64) error {
	sim.PutEvent(&h.e, false, client, op, core.Tick(h.nanos()/1000), in)
	_, err := h.f.Write(h.e[:])
	return err
}

func (h *histFile) complete(client uint32, op uint64, out [sim.OpWords]uint64) error {
	sim.PutEvent(&h.e, true, client, op, core.Tick((h.nanos()+999)/1000), out)
	_, err := h.f.Write(h.e[:])
	return err
}

func BenchmarkHistFile(b *testing.B) {
	b.ReportAllocs()
	h, err := openHist(filepath.Join(b.TempDir(), "history"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = h.f.Close() }()
	in := [sim.OpWords]uint64{'s', 3, 7}
	for i := range uint64(b.N) {
		if err := h.invoke(1, i, in); err != nil {
			b.Fatal(err)
		}
		if err := h.complete(1, i, in); err != nil {
			b.Fatal(err)
		}
	}
}

// slots is a register per slot, starting at 0: a slot write's input {'s', slot, value}, a read's
// {'r', slot} with output {value}. The driver's changes are checked by the oracle, not here.
var slots = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		bySlot := map[uint64][]porcupine.Operation{}
		for _, o := range h {
			if in := o.Input.([sim.OpWords]uint64); in[0] == 's' || in[0] == 'r' {
				bySlot[in[1]] = append(bySlot[in[1]], o)
			}
		}
		var parts [][]porcupine.Operation
		for _, p := range bySlot {
			parts = append(parts, p)
		}
		return parts
	},
	Init: func() any { return uint64(0) },
	Step: func(state, input, output any) (bool, any) {
		in, out, v := input.([sim.OpWords]uint64), output.(lincheck.Result), state.(uint64)
		if in[0] == 's' {
			return true, in[2]
		}
		return !out.Done || out.Output[0] == v, v
	},
}

// TestCrashOnARealDisk kills the store's process RS_CRASH_CYCLES times (default 150) on seed
// RS_CRASH_SEED (default 1), keeping the disk file, the oracle and the history in RS_CRASH_DIR if
// it is set. With RS_CRASH_TARGET set, the disk is RS_RAW_DEVICE's first blocks behind that
// device-mapper target (dmRig), and each kill comes with the target's fault: error fails every
// I/O from then on, flakey drops every write, and log-writes replays only the flushed writes, a
// power cut; delay slows every I/O by 5 ms, and linear is the raw device alone.
func TestCrashOnARealDisk(t *testing.T) {
	newRing(t, 1<<20, 8)
	seed, cycles := envUint(t, "RS_CRASH_SEED", 1), envUint(t, "RS_CRASH_CYCLES", 150)
	dir := os.Getenv("RS_CRASH_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	ct := &crashTest{t: t, seed: seed, rnd: core.NewRand(seed), changes: core.NewRand(seed + 1<<32), pace: core.NewRand(seed + 2<<32),
		disk: filepath.Join(dir, "disk"), oracle: filepath.Join(dir, "oracle"), history: filepath.Join(dir, "history")}
	if ct.target = os.Getenv("RS_CRASH_TARGET"); ct.target != "" {
		ct.dm, ct.disk = newDMRig(t, rawDevice(t), ct.target, storetest.DiskBytes), dmPath
	} else if err := CreateFile(ct.disk, storetest.DiskBytes); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(ct.history)
	var err error
	if ct.hist, err = openHist(ct.history); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ct.hist.f.Close() }()
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
	ct.start(false, cycles)
	ct.check()
	ct.kill()
	if ct.clientErr != "" {
		t.Fatalf("seed %d: %s", seed, ct.clientErr)
	}
	t.Logf("seed %d: %d cycles, %d changes sent, %d acknowledged; restarts that kept a change never acknowledged: %d, lost one: %d, kept the first rounds of one: %d",
		seed, cycles, ct.seq, ct.acked, ct.keptCycles, ct.lostCycles, ct.tornCycles)
	if ct.target == "error" {
		t.Logf("error: %d of %d children answered a change with an error before their kill", ct.failed, cycles)
	}
	if ct.target == "log-writes" {
		t.Logf("log-writes: %d writes replayed behind %d flushes, %d completed writes never flushed dropped", ct.dm.replayed, ct.dm.flushes, ct.dm.dropped)
	}
	ct.checkHistory()
}

// checkHistory runs the Porcupine check over the slots' reads and writes the history file holds.
func (ct *crashTest) checkHistory() {
	b, err := os.ReadFile(ct.history)
	if err != nil {
		ct.t.Fatal(err)
	}
	h, err := sim.ReadHistory(b)
	if err != nil {
		ct.t.Fatal(err)
	}
	var reads, writes, open int
	for _, op := range h.Ops() {
		switch {
		case op.Input[0] == 'r':
			reads++
		case op.Input[0] == 's':
			writes++
		default:
			continue
		}
		if !op.Done {
			open++
		}
	}
	i, res := lincheck.Illegal(slots, h, 5*time.Minute)
	switch res {
	case porcupine.Ok:
		ct.t.Logf("history: %d slot reads and %d slot writes from %d client processes, %d never completed, linearizable", reads, writes, ct.clientRuns, open)
	case porcupine.Illegal:
		op, t0 := h.Ops()[i], h.Ops()[0].Invoke
		ct.t.Fatalf("seed %d: the slots' history has no linearizable order; it leaves out client %d's %c of slot %d (written %#x, read %#x, done %v), invoked %dµs into the run and completed at %dµs\nhistory: %s",
			ct.seed, op.Client, op.Input[0], op.Input[1], op.Input[2], op.Output[0], op.Done, op.Invoke-t0, int64(op.Complete)-int64(t0), ct.history)
	default:
		ct.t.Fatalf("seed %d: the check of the slots' history ran out of time; history: %s", ct.seed, ct.history)
	}
}

type crashTest struct {
	t    *testing.T
	seed uint64
	rnd  *core.Rand
	// target is RS_CRASH_TARGET, dm the device-mapper rig it puts the disk behind, or nil.
	target string
	dm     *dmRig
	// failed counts the children the error target failed before their kill, and failFrom is the
	// first change sent once it did, which must never be acknowledged, into a child that may have
	// stopped.
	failed   int
	failFrom uint64
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

	history    string
	hist       *histFile
	clients    []*exec.Cmd
	clientOut  []bytes.Buffer
	clientErr  string
	clientRuns int

	seq, acked                         uint64
	keptCycles, lostCycles, tornCycles int
}

func (ct *crashTest) cycle(cycle uint64) {
	ct.start(cycle == 0, cycle)
	ct.check()
	killAt := ct.acked + ct.rnd.Range(1, 400)
	for range crashWindow {
		ct.send()
	}
	var ack [8]byte
	for ct.acked < killAt {
		ct.read8(ack[:])
		n := 1
		ct.ack(ack[:])
		for ct.acks.Buffered() >= 8 {
			ct.read8(ack[:])
			ct.ack(ack[:])
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
	switch ct.target {
	case "error":
		ct.dm.swap("error")
		ct.failFrom = ct.seq + 1
		for range crashWindow {
			ct.send()
		}
		time.Sleep(50 * time.Millisecond)
		ct.kill()
		if strings.Contains(ct.stderr.String(), "--- FAIL: TestCrashChild") {
			ct.failed++
		}
		ct.drain()
		ct.dm.swap("linear")
	case "flakey":
		// A client's write acknowledged once flakey drops writes would be lost, so they stop first.
		for _, c := range ct.clients {
			_ = c.Process.Signal(syscall.SIGKILL)
			_ = c.Wait()
		}
		ct.clients = nil
		ct.dm.swap("flakey")
		time.Sleep(20 * time.Millisecond)
		ct.kill()
		_ = ct.ackPipe.Close()
		ct.dm.swap("linear")
	default:
		ct.kill()
		ct.drain()
		if ct.dm != nil {
			ct.dm.cut()
		}
	}
	ct.record("kill %d", cycle)
	ct.sync()
	if ct.clientErr != "" {
		ct.t.Fatalf("seed %d: %s", ct.seed, ct.clientErr)
	}
}

func (ct *crashTest) ack(b []byte) {
	seq := binary.LittleEndian.Uint64(b)
	ct.record("ack %d", seq)
	if err := ct.hist.complete(0, seq, [sim.OpWords]uint64{}); err != nil {
		ct.t.Fatal(err)
	}
}

func (ct *crashTest) send() {
	ct.seq++
	c := int(ct.changes.Below(crashChunks))
	off := uint32(ct.changes.Below(core.ChunkSize - 1))
	n := uint32(min(ct.changes.Range(1, 3*core.BlockSize), core.ChunkSize-uint64(off)))
	kind, ds := byte('p'), uint64(0)
	if ct.changes.Chance(1, 6) {
		ct.record("punch %d %d %d %d", ct.seq, c, off, n)
	} else {
		kind, ds = 'w', ct.changes.Uint64()
		ct.record("write %d %d %d %d %d", ct.seq, c, off, n, ds)
	}
	if err := ct.w.Flush(); err != nil {
		ct.t.Fatal(err)
	}
	var cmd [cmdBytes]byte
	putCmd(&cmd, kind, 0, ct.seq, c, off, n, ds)
	if err := ct.hist.invoke(0, ct.seq, [sim.OpWords]uint64{uint64(kind), uint64(c), uint64(off)<<32 | uint64(n), ds}); err != nil {
		ct.t.Fatal(err)
	}
	if _, err := ct.cmds.Write(cmd[:]); err != nil && ct.failFrom == 0 {
		ct.fail("sending change %d: %v", ct.seq, err)
	}
}

func writeData(seed uint64, n uint32) []byte {
	b := make([]byte, n)
	core.NewRand(seed).Fill(b)
	return b
}

func (ct *crashTest) start(format bool, cycle uint64) {
	cmdR, cmdW := ct.pipe()
	ackR, ackW := ct.pipe()
	var replyR, replyW [histClients]*os.File
	for i := range replyR {
		replyR[i], replyW[i] = ct.pipe()
	}
	ct.stderr.Reset()
	if ct.dm != nil {
		ct.dm.begin()
	}
	ct.child = exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1", "-test.timeout=0")
	ct.child.Env = append(os.Environ(), "RS_CRASH_CHILD="+ct.disk, fmt.Sprint("RS_CRASH_CLIENTS=", histClients))
	if format {
		ct.child.Env = append(ct.child.Env, "RS_CRASH_FORMAT=1")
	}
	ct.child.ExtraFiles = append([]*os.File{cmdR, ackW}, replyW[:]...)
	ct.child.Stdout, ct.child.Stderr = &ct.stderr, &ct.stderr
	if err := ct.child.Start(); err != nil {
		ct.t.Fatal(err)
	}
	for _, f := range ct.child.ExtraFiles {
		_ = f.Close()
	}
	ct.clientOut = make([]bytes.Buffer, histClients)
	for i := range histClients {
		ct.clientRuns++
		c := exec.Command(os.Args[0], "-test.run=^TestHistClient$", "-test.count=1", "-test.timeout=0")
		c.Env = append(os.Environ(), "RS_HIST_CLIENT="+ct.history, fmt.Sprint("RS_HIST_ID=", 1+cycle*histClients+uint64(i)),
			fmt.Sprint("RS_HIST_CYCLE=", cycle), fmt.Sprint("RS_CRASH_SEED=", ct.seed))
		c.ExtraFiles = []*os.File{cmdW, replyR[i]}
		c.Stdout, c.Stderr = &ct.clientOut[i], &ct.clientOut[i]
		if err := c.Start(); err != nil {
			ct.t.Fatal(err)
		}
		_ = replyR[i].Close()
		ct.clients = append(ct.clients, c)
	}
	ct.cmds, ct.ackPipe, ct.acks = cmdW, ackR, bufio.NewReaderSize(ackR, 1<<20)
}

func (ct *crashTest) pipe() (r, w *os.File) {
	r, w, err := os.Pipe()
	if err != nil {
		ct.t.Fatal(err)
	}
	return r, w
}

// kill kills the child and waits for its clients, which stop on losing it.
func (ct *crashTest) kill() {
	_ = ct.child.Process.Signal(syscall.SIGKILL)
	_ = ct.child.Wait()
	_ = ct.cmds.Close()
	for i, c := range ct.clients {
		if err := c.Wait(); err != nil && ct.clientErr == "" {
			ct.clientErr = fmt.Sprintf("history client %d: %v\n%s", i, err, ct.clientOut[i].String())
		}
	}
	ct.clients = nil
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
		ct.ack(ack[:])
		ct.acked++
		if seq := binary.LittleEndian.Uint64(ack[:]); ct.failFrom != 0 && seq >= ct.failFrom {
			ct.fail("change %d, sent once every I/O failed, was acknowledged", seq)
		}
	}
	ct.failFrom = 0
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
	ct.t.Fatalf("seed %d: %s\nreplay: RS_CRASH_SEED=%d go test -run TestCrashOnARealDisk ./internal/driver/uring/\noracle: %s\nchild output:\n%s%s",
		ct.seed, fmt.Sprintf(format, args...), ct.seed, ct.oracle, ct.stderr.String(), ct.clientErr)
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
