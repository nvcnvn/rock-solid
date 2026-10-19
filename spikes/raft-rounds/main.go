// Command raft-rounds drives 3 simulated nodes x N etcd-raft groups with a round driver that
// collects every group's Ready once per round, in virtual time: each loop pass costs the thread
// CPU time it measurably took, a flush costs a fixed (or measured-curve) latency, and messages
// cross the in-process network as encoded batches after a fixed delay.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
)

type config struct {
	nodes, groups, loops int
	flushUs              float64
	disk                 string
	netUs                float64
	maxBytes             int
	maxCPUUs             float64
	mode                 string
	inflight             int
	rate                 float64
	payload              int
	batch                bool
	wire                 bool
	warmup, measure      float64
	idle                 float64
	tickMs               float64
	seed                 uint64
	memprof, cpuprof     string
	label                string
}

func cpuNow() int64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &ts)
	return ts.Nano()
}

func growTo(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	nb := make([]byte, n, 2*n)
	copy(nb, b)
	return nb
}

const (
	evPass = iota
	evMsgs
	evFlush
	evTick
	evArrival
)

type envelope struct {
	g int32
	m msgT
}

type event struct {
	t    int64
	seq  uint64
	kind uint8
	loop int32
	buf  []byte
	envs []envelope
}

type evHeap []event

func (h evHeap) less(i, j int) bool {
	if h[i].t != h[j].t {
		return h[i].t < h[j].t
	}
	return h[i].seq < h[j].seq
}

func (h *evHeap) push(e event) {
	*h = append(*h, e)
	a := *h
	i := len(a) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !a.less(i, p) {
			break
		}
		a[i], a[p] = a[p], a[i]
		i = p
	}
}

func (h *evHeap) pop() event {
	a := *h
	top := a[0]
	n := len(a) - 1
	a[0] = a[n]
	a[n] = event{}
	a = a[:n]
	i := 0
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		m := l
		if r := l + 1; r < n && a.less(r, l) {
			m = r
		}
		if !a.less(m, i) {
			break
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
	*h = a
	return top
}

type prop struct {
	g int32
	t int64
}

type replica struct {
	g       int32
	rn      *raft.RawNode
	st      *raft.MemoryStorage
	lp      *loop
	rd      raft.Ready
	inRound bool
	leader  bool
	pending []int64
	waiting []int64
	whead   int
	late    []msgT
	applied uint64
	compact uint64
}

type loop struct {
	id, node    int
	reps        []*replica
	byGroup     map[int32]*replica
	busyUntil   int64
	passPending bool
	props       []prop
	batches     [][]byte
	envBatches  [][]envelope
	tickDue     bool
	flushing    bool
	flushDone   bool
	submitBytes int
	submitNow   bool
	round       []*replica
	cursor      int
	out         [][]byte
	outEnv      [][]envelope
	dirty       []int
	rec         []byte
	done        []int64
	doneG       []int32

	cpuNs, collectNs, cpuAll        int64
	rounds, roundBytes, roundReadys int64
	maxRoundBytes                   int64
	passes                          int64
}

type sim struct {
	cfg        config
	h          evHeap
	seq        uint64
	loops      []*loop
	leaderLoop []int32
	rng        *rand.Rand
	measuring  bool
	mStart     int64

	bufPool [][]byte
	envPool [][]envelope

	hist         []int64
	completed    int64
	dropped      int64
	msgsSent     int64
	batchesSent  int64
	msgBytes     int64
	snapMsgs     int64
	entriesProps int64
}

func (s *sim) push(e event) {
	s.seq++
	e.seq = s.seq
	s.h.push(e)
}

func (s *sim) schedulePass(l *loop, t int64) {
	if l.passPending {
		return
	}
	l.passPending = true
	if l.busyUntil > t {
		t = l.busyUntil
	}
	s.push(event{t: t, kind: evPass, loop: int32(l.id)})
}

func (s *sim) loopOf(node int, g int32) *loop {
	return s.loops[node*s.cfg.loops+int(g)%s.cfg.loops]
}

func (s *sim) flushLatency(bytes int) int64 {
	us := s.cfg.flushUs
	if s.cfg.disk == "nvme" {
		kib := float64(bytes) / 1024
		pts := [][2]float64{{0, 950}, {64, 950}, {256, 1900}, {1024, 2300}, {4096, 4300}}
		us = pts[len(pts)-1][1] + (kib-4096)*(2000.0/3072)
		for i := 1; i < len(pts); i++ {
			if kib <= pts[i][0] {
				a, b := pts[i-1], pts[i]
				us = a[1] + (kib-a[0])/(b[0]-a[0])*(b[1]-a[1])
				break
			}
		}
	}
	return int64(us * 1000)
}

func needsDurable(t pb.MessageType) bool {
	return t == pb.MsgAppResp || t == pb.MsgVoteResp || t == pb.MsgPreVoteResp
}

func (s *sim) send(l *loop, g int32, m msgT) {
	if mType(m) == pb.MsgSnap {
		s.snapMsgs++
	}
	dst := s.loopOf(int(mTo(m))-1, g)
	if s.cfg.wire {
		b := l.out[dst.id]
		if b == nil {
			b = s.getBuf()
			l.dirty = append(l.dirty, dst.id)
		}
		o := len(b)
		b = growTo(b, o+8)
		binary.LittleEndian.PutUint32(b[o:], uint32(g))
		b = mAppend(b, m)
		binary.LittleEndian.PutUint32(b[o+4:], uint32(len(b)-o-8))
		s.msgBytes += int64(len(b) - o)
		l.out[dst.id] = b
	} else {
		e := l.outEnv[dst.id]
		if e == nil {
			e = s.getEnv()
			l.dirty = append(l.dirty, dst.id)
		}
		l.outEnv[dst.id] = append(e, envelope{g: g, m: m})
	}
	s.msgsSent++
}

func (s *sim) getBuf() []byte {
	if n := len(s.bufPool); n > 0 {
		b := s.bufPool[n-1]
		s.bufPool = s.bufPool[:n-1]
		return b[:0]
	}
	return make([]byte, 0, 64<<10)
}

func (s *sim) getEnv() []envelope {
	if n := len(s.envPool); n > 0 {
		e := s.envPool[n-1]
		s.envPool = s.envPool[:n-1]
		return e[:0]
	}
	return make([]envelope, 0, 256)
}

func (s *sim) flushOut(l *loop, t int64) {
	at := t + int64(s.cfg.netUs*1000)
	for _, d := range l.dirty {
		if s.cfg.wire {
			s.push(event{t: at, kind: evMsgs, loop: int32(d), buf: l.out[d]})
			l.out[d] = nil
		} else {
			s.push(event{t: at, kind: evMsgs, loop: int32(d), envs: l.outEnv[d]})
			l.outEnv[d] = nil
		}
		s.batchesSent++
	}
	l.dirty = l.dirty[:0]
}

func (s *sim) deliver(l *loop) {
	for _, b := range l.batches {
		for o := 0; o < len(b); {
			g := int32(binary.LittleEndian.Uint32(b[o:]))
			n := int(binary.LittleEndian.Uint32(b[o+4:]))
			m, err := mDecode(b[o+8 : o+8+n])
			if err != nil {
				log.Fatalf("decode: %v", err)
			}
			o += 8 + n
			_ = step(l.byGroup[g].rn, m)
		}
		s.bufPool = append(s.bufPool, b[:0])
	}
	l.batches = l.batches[:0]
	for _, es := range l.envBatches {
		for i := range es {
			_ = step(l.byGroup[es[i].g].rn, es[i].m)
			es[i] = envelope{}
		}
		s.envPool = append(s.envPool, es[:0])
	}
	l.envBatches = l.envBatches[:0]
}

func (s *sim) proposeBatch(r *replica) {
	n := len(r.pending)
	data := make([]byte, 8+n*s.cfg.payload)
	binary.LittleEndian.PutUint32(data, uint32(n))
	if err := r.rn.Propose(data); err != nil {
		s.dropped += int64(n)
	} else {
		r.waiting = append(r.waiting, r.pending...)
		s.entriesProps++
	}
	r.pending = r.pending[:0]
}

func (s *sim) apply(l *loop, r *replica, ents []entryT) {
	for i := range ents {
		e := ents[i]
		r.applied = eIndex(e)
		d := eData(e)
		if !eNormal(e) || len(d) < 8 || !r.leader {
			continue
		}
		n := int(binary.LittleEndian.Uint32(d))
		for k := 0; k < n && r.whead < len(r.waiting); k++ {
			l.done = append(l.done, r.waiting[r.whead])
			l.doneG = append(l.doneG, r.g)
			r.whead++
		}
		if r.whead > 1024 && r.whead*2 > len(r.waiting) {
			r.waiting = r.waiting[:copy(r.waiting, r.waiting[r.whead:])]
			r.whead = 0
		}
	}
	if r.applied >= r.compact+16 {
		c := r.applied - 4
		snapshotAndCompact(r.st, c)
		r.compact = c
	}
}

func (s *sim) completeRound(l *loop) {
	for _, r := range l.round {
		for _, m := range r.late {
			s.send(l, r.g, m)
		}
		clear(r.late)
		r.late = r.late[:0]
		r.rn.Advance(r.rd)
		r.rd = raft.Ready{}
		r.inRound = false
	}
	clear(l.round)
	l.round = l.round[:0]
	l.flushing = false
	l.flushDone = false
}

func (s *sim) collect(l *loop, passStart int64) {
	c0 := cpuNow()
	bytes, readys := 0, 0
	l.rec = l.rec[:0]
	n := len(l.reps)
	next := l.cursor
	for i := 0; i < n; i++ {
		idx := (l.cursor + i) % n
		next = (idx + 1) % n
		r := l.reps[idx]
		if r.inRound {
			continue
		}
		if s.cfg.batch && len(r.pending) > 0 && r.leader {
			s.proposeBatch(r)
		}
		if !r.rn.HasReady() {
			continue
		}
		rd := r.rn.Ready()
		if rd.SoftState != nil {
			r.leader = rd.SoftState.RaftState == raft.StateLeader
			if r.leader {
				s.leaderLoop[r.g] = int32(l.id)
			}
		}
		persist(r.st, &rd)
		s.apply(l, r, rd.CommittedEntries)
		for _, m := range rd.Messages {
			if needsDurable(mType(m)) {
				r.late = append(r.late, m)
			} else {
				s.send(l, r.g, m)
			}
		}
		if !needsPersist(&rd) && len(r.late) == 0 {
			r.rn.Advance(rd)
			continue
		}
		for j := range rd.Entries {
			d := eData(rd.Entries[j])
			o := len(l.rec)
			l.rec = growTo(l.rec, o+24+len(d))
			copy(l.rec[o+24:], d)
		}
		if hardStateSet(&rd) {
			l.rec = growTo(l.rec, len(l.rec)+24)
		}
		r.rd = rd
		r.inRound = true
		l.round = append(l.round, r)
		readys++
		bytes = len(l.rec)
		if s.cfg.maxBytes > 0 && bytes >= s.cfg.maxBytes {
			break
		}
		if s.cfg.maxCPUUs > 0 && readys&7 == 0 && float64(cpuNow()-passStart)/1000 >= s.cfg.maxCPUUs {
			break
		}
	}
	l.cursor = next
	if readys > 0 {
		l.flushing = true
		l.submitBytes = bytes
		l.submitNow = true
		if s.measuring {
			l.rounds++
			l.roundBytes += int64(bytes)
			l.roundReadys += int64(readys)
			l.maxRoundBytes = max(l.maxRoundBytes, int64(bytes))
		}
	}
	if s.measuring {
		l.collectNs += cpuNow() - c0
	}
}

func (s *sim) pass(l *loop, t int64) {
	c0 := cpuNow()
	if l.tickDue {
		for _, r := range l.reps {
			r.rn.Tick()
		}
		l.tickDue = false
	}
	s.deliver(l)
	for _, p := range l.props {
		r := l.byGroup[p.g]
		if !r.leader {
			s.dropped++
			continue
		}
		if s.cfg.batch {
			r.pending = append(r.pending, p.t)
			continue
		}
		data := make([]byte, 8+s.cfg.payload)
		binary.LittleEndian.PutUint32(data, 1)
		if err := r.rn.Propose(data); err != nil {
			s.dropped++
			continue
		}
		r.waiting = append(r.waiting, p.t)
		s.entriesProps++
	}
	l.props = l.props[:0]
	if l.flushDone {
		s.completeRound(l)
	}
	if !l.flushing {
		s.collect(l, c0)
	}
	d := cpuNow() - c0
	end := t + d
	l.busyUntil = end
	s.flushOut(l, end)
	if l.submitNow {
		l.submitNow = false
		s.push(event{t: end + s.flushLatency(l.submitBytes), kind: evFlush, loop: int32(l.id)})
	}
	for i, a := range l.done {
		if s.measuring && a >= s.mStart {
			s.record(end - a)
		}
		if s.cfg.mode == "closed" {
			l.props = append(l.props, prop{g: l.doneG[i], t: end})
		}
	}
	if len(l.done) > 0 && s.cfg.mode == "closed" {
		s.schedulePass(l, end)
	}
	l.done = l.done[:0]
	l.doneG = l.doneG[:0]
	l.cpuAll += d
	if s.measuring {
		l.cpuNs += d
		l.passes++
	}
}

const histSub = 64

func (s *sim) record(ns int64) {
	s.completed++
	us := float64(ns)/1000 + 1
	b := int(math.Log2(us) * histSub)
	if b >= len(s.hist) {
		b = len(s.hist) - 1
	}
	s.hist[b]++
}

func (s *sim) pct(p float64) float64 {
	var tot int64
	for _, c := range s.hist {
		tot += c
	}
	want := int64(math.Ceil(p * float64(tot)))
	var acc int64
	for i, c := range s.hist {
		acc += c
		if acc >= want && c > 0 {
			return math.Exp2(float64(i)/histSub) - 1
		}
	}
	return 0
}

func (s *sim) run(until int64) {
	for len(s.h) > 0 && s.h[0].t <= until {
		e := s.h.pop()
		switch e.kind {
		case evPass:
			l := s.loops[e.loop]
			l.passPending = false
			s.pass(l, e.t)
		case evMsgs:
			l := s.loops[e.loop]
			if e.buf != nil {
				l.batches = append(l.batches, e.buf)
			} else {
				l.envBatches = append(l.envBatches, e.envs)
			}
			s.schedulePass(l, e.t)
		case evFlush:
			l := s.loops[e.loop]
			l.flushDone = true
			s.schedulePass(l, e.t)
		case evTick:
			for _, l := range s.loops {
				l.tickDue = true
				s.schedulePass(l, e.t)
			}
			s.push(event{t: e.t + int64(s.cfg.tickMs*1e6), kind: evTick})
		case evArrival:
			g := int32(s.rng.IntN(s.cfg.groups))
			if li := s.leaderLoop[g]; li >= 0 {
				l := s.loops[li]
				l.props = append(l.props, prop{g: g, t: e.t})
				s.schedulePass(l, e.t)
			} else {
				s.dropped++
			}
			gap := -math.Log(1-s.rng.Float64()) / s.cfg.rate * 1e9
			s.push(event{t: e.t + int64(gap), kind: evArrival})
		}
	}
}

func readMetrics(names ...string) []float64 {
	ss := make([]metrics.Sample, len(names))
	for i, n := range names {
		ss[i].Name = n
	}
	metrics.Read(ss)
	out := make([]float64, len(names))
	for i, s := range ss {
		switch s.Value.Kind() {
		case metrics.KindUint64:
			out[i] = float64(s.Value.Uint64())
		case metrics.KindFloat64:
			out[i] = s.Value.Float64()
		}
	}
	return out
}

var metricNames = []string{
	"/gc/heap/allocs:objects",
	"/gc/heap/allocs:bytes",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/user:cpu-seconds",
	"/gc/cycles/total:gc-cycles",
	"/gc/heap/live:bytes",
	"/cpu/classes/gc/mark/assist:cpu-seconds",
}

func procCPU() float64 {
	var ru unix.Rusage
	_ = unix.Getrusage(unix.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
}

func liveHeap() float64 {
	runtime.GC()
	runtime.GC()
	return readMetrics("/gc/heap/live:bytes")[0]
}

func main() {
	var c config
	flag.IntVar(&c.nodes, "nodes", 3, "nodes")
	flag.IntVar(&c.groups, "groups", 1000, "Raft groups")
	flag.IntVar(&c.loops, "loops", 1, "event loops (cores) per node, each with its own flush stream")
	flag.Float64Var(&c.flushUs, "flush-us", 950, "fixed write+flush latency in µs")
	flag.StringVar(&c.disk, "disk", "fixed", "fixed, or nvme: the NVMe flush spike's N3 latency-by-size curve")
	flag.Float64Var(&c.netUs, "net-us", 50, "one-way network delay in µs")
	flag.IntVar(&c.maxBytes, "max-bytes", 0, "max_group_commit_bytes; 0 = unbounded")
	flag.Float64Var(&c.maxCPUUs, "max-cpu-us", 0, "stop collecting Readys once the pass used this much CPU; 0 = unbounded")
	flag.StringVar(&c.mode, "mode", "closed", "closed (inflight per group) or open (Poisson at -rate)")
	flag.IntVar(&c.inflight, "inflight", 32, "proposals in flight per group, closed mode")
	flag.Float64Var(&c.rate, "rate", 100000, "proposals per second, open mode")
	flag.IntVar(&c.payload, "payload", 128, "bytes per proposal")
	flag.BoolVar(&c.batch, "batch", true, "one entry per group per round (design) instead of one per proposal")
	flag.BoolVar(&c.wire, "wire", true, "encode messages into batches and decode them on delivery")
	flag.Float64Var(&c.warmup, "warmup", 0.5, "virtual seconds of load before measuring")
	flag.Float64Var(&c.measure, "measure", 2, "virtual seconds measured")
	flag.Float64Var(&c.idle, "idle", 1, "virtual seconds idle after elections, before the idle heap reading")
	flag.Float64Var(&c.tickMs, "tick-ms", 10, "tick interval")
	flag.Uint64Var(&c.seed, "seed", 1, "seed")
	flag.StringVar(&c.memprof, "memprof", "", "write allocs profiles <p>.base and <p>.end around the measurement")
	flag.StringVar(&c.cpuprof, "cpuprof", "", "CPU profile of the measurement")
	flag.StringVar(&c.label, "label", "", "label")
	flag.Parse()
	if c.memprof != "" {
		runtime.MemProfileRate = 16 << 10
	}
	runtime.LockOSThread()

	s := &sim{cfg: c, rng: rand.New(rand.NewPCG(c.seed, 7)), hist: make([]int64, 40*histSub)}
	s.leaderLoop = make([]int32, c.groups)
	for i := range s.leaderLoop {
		s.leaderLoop[i] = -1
	}
	nl := c.nodes * c.loops
	for i := 0; i < nl; i++ {
		s.loops = append(s.loops, &loop{id: i, node: i / c.loops, byGroup: map[int32]*replica{},
			out: make([][]byte, nl), outEnv: make([][]envelope, nl)})
	}
	h0 := liveHeap()
	voters := make([]uint64, c.nodes)
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	logger := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	for g := int32(0); g < int32(c.groups); g++ {
		for n := 0; n < c.nodes; n++ {
			st := raft.NewMemoryStorage()
			if err := bootstrap(st, voters); err != nil {
				log.Fatal(err)
			}
			rn, err := raft.NewRawNode(&raft.Config{
				ID: uint64(n + 1), ElectionTick: 100, HeartbeatTick: 10, Storage: st,
				MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, CheckQuorum: true, PreVote: true,
				Logger: logger,
			})
			if err != nil {
				log.Fatal(err)
			}
			l := s.loopOf(n, g)
			r := &replica{g: g, rn: rn, st: st, lp: l, compact: 1}
			l.reps = append(l.reps, r)
			l.byGroup[g] = r
			if int(g)%c.nodes == n {
				_ = rn.Campaign()
			}
		}
	}
	for _, l := range s.loops {
		s.schedulePass(l, 0)
	}
	s.push(event{t: int64(c.tickMs * 1e6), kind: evTick})
	elect := int64(0)
	for elect < 10e9 {
		elect += 10e6
		s.run(elect)
		ok := true
		for _, li := range s.leaderLoop {
			if li < 0 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
	}
	perNode := make([]int, c.nodes)
	for _, li := range s.leaderLoop {
		if li >= 0 {
			perNode[s.loops[li].node]++
		}
	}
	idleEnd := elect + int64(c.idle*1e9)
	var idle0 int64
	for _, l := range s.loops {
		idle0 += l.cpuAll
	}
	s.run(idleEnd)
	var idleCPU int64
	for _, l := range s.loops {
		idleCPU += l.cpuAll
	}
	idleCPU -= idle0
	hIdle := liveHeap()
	reps := float64(c.nodes * c.groups)

	t := idleEnd
	if c.mode == "closed" {
		for g := int32(0); g < int32(c.groups); g++ {
			l := s.loops[s.leaderLoop[g]]
			for k := 0; k < c.inflight; k++ {
				l.props = append(l.props, prop{g: g, t: t})
			}
			s.schedulePass(l, t)
		}
	} else {
		s.push(event{t: t, kind: evArrival})
	}
	s.run(t + int64(c.warmup*1e9))
	s.mStart = t + int64(c.warmup*1e9)
	s.measuring = true
	s.msgsSent, s.batchesSent, s.msgBytes, s.entriesProps, s.dropped = 0, 0, 0, 0, 0
	debug.FreeOSMemory()
	if c.memprof != "" {
		writeProf(c.memprof + ".base")
	}
	if c.cpuprof != "" {
		f, _ := os.Create(c.cpuprof)
		_ = pprof.StartCPUProfile(f)
	}
	m0 := readMetrics(metricNames...)
	p0 := procCPU()
	s.run(s.mStart + int64(c.measure*1e9))
	p1 := procCPU()
	m1 := readMetrics(metricNames...)
	if c.cpuprof != "" {
		pprof.StopCPUProfile()
	}
	if c.memprof != "" {
		writeProf(c.memprof + ".end")
	}
	hLoad := liveHeap()

	var cpu, coll, rounds, rbytes, rreadys, maxr, passes int64
	maxUtil := 0.0
	for _, l := range s.loops {
		cpu += l.cpuNs
		coll += l.collectNs
		rounds += l.rounds
		rbytes += l.roundBytes
		rreadys += l.roundReadys
		maxr = max(maxr, l.maxRoundBytes)
		passes += l.passes
		maxUtil = max(maxUtil, float64(l.cpuNs)/(c.measure*1e9))
	}
	ops := float64(s.completed)
	allocs := m1[0] - m0[0]
	abytes := m1[1] - m0[1]
	assist := m1[6] - m0[6]
	other := (p1 - p0) - float64(cpu)/1e9
	label := c.label
	if label == "" {
		label = strings.Join(os.Args[1:], " ")
	}
	fmt.Printf("== %s\n", label)
	fmt.Printf("   raft %s, %d nodes x %d groups, %d loop(s)/node, flush %s %.0fus, net %.0fus, mode %s",
		raftVersion, c.nodes, c.groups, c.loops, c.disk, c.flushUs, c.netUs, c.mode)
	if c.mode == "closed" {
		fmt.Printf(" %d/group (%d in flight)", c.inflight, c.inflight*c.groups)
	} else {
		fmt.Printf(" %.0f/s", c.rate)
	}
	fmt.Printf(", max-bytes %d, max-cpu-us %.0f, batch %v, wire %v\n", c.maxBytes, c.maxCPUUs, c.batch, c.wire)
	fmt.Printf("   elected in %.2fs, leaders per node %v; heap before groups %.1f MiB\n", float64(elect)/1e9, perNode, h0/(1<<20))
	fmt.Printf("   idle: %.2f%% of a core per node (ticks and heartbeats)\n", 100*float64(idleCPU)/(c.idle*1e9)/float64(c.nodes))
	fmt.Printf("   heap per replica: idle %.1f KiB, loaded %.1f KiB (per node: idle %.1f MiB, loaded %.1f MiB)\n",
		(hIdle-h0)/reps/1024, (hLoad-h0)/reps/1024, (hIdle-h0)/float64(c.nodes)/(1<<20), (hLoad-h0)/float64(c.nodes)/(1<<20))
	fmt.Printf("   throughput %.0f ops/s, latency p50 %.0f us, p99 %.0f us, p99.9 %.0f us, max %.0f us, dropped %d\n",
		ops/c.measure, s.pct(0.5), s.pct(0.99), s.pct(0.999), s.pct(1), s.dropped)
	fmt.Printf("   loop CPU per op %.2f us (collect %.0f%%), busiest loop %.0f%% busy, passes/s/loop %.0f\n",
		float64(cpu)/1000/ops, 100*float64(coll)/float64(cpu), 100*maxUtil, float64(passes)/c.measure/float64(len(s.loops)))
	if rounds > 0 {
		fmt.Printf("   rounds/s/loop %.0f, per round: %.0f KiB mean, %.0f KiB max, %.0f Readys; entries proposed per op %.3f\n",
			float64(rounds)/c.measure/float64(len(s.loops)), float64(rbytes)/float64(rounds)/1024, float64(maxr)/1024,
			float64(rreadys)/float64(rounds), float64(s.entriesProps)/ops)
	}
	fmt.Printf("   msgs %.0f/s (%.2f per op, %.0f B avg), batches %.0f/s, MsgSnap %d\n",
		float64(s.msgsSent)/c.measure, float64(s.msgsSent)/ops, float64(s.msgBytes)/math.Max(1, float64(s.msgsSent)),
		float64(s.batchesSent)/c.measure, s.snapMsgs)
	fmt.Printf("   allocs per op %.1f (%.0f B), per msg %.1f; GC cycles %.0f; process CPU %.0f ms = loop threads %.0f (incl. GC assist ~%.0f) + other threads %.0f (GC workers, sysmon): GC ~%.1f%% of process CPU\n",
		allocs/ops, abytes/ops, allocs/math.Max(1, float64(s.msgsSent)), m1[4]-m0[4], (p1-p0)*1000,
		float64(cpu)/1e6, assist*1000, other*1000, 100*(other+assist)/(p1-p0))
}

func writeProf(path string) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	_ = pprof.Lookup("allocs").WriteTo(f, 0)
}
