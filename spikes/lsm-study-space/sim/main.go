// Command sim models the metadata stream of one disk log: checkpoint flushes, size-tiered
// compactions under the bucket rule of internal/lsm/compact.go, manifests and a greedy
// cleaner for metadata segments. It measures how much space a compaction holds beyond what
// the manifests name, with compaction output installed whole (the design today) or in steps
// that each end with a manifest and free the input pieces wholly below the step's bound.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

const MiB = 1 << 20

type disk struct {
	segBytes   int64
	nSegs      int
	pieceBytes int64
	fill       float64 // share of the disk's segments in use, for the manifest's usage table
}

type cfg struct {
	name    string
	disk    disk
	indexes []idxCfg
	mode    string // "whole" or "step"
	step    int64  // output bytes per step in step mode
	ownLane bool   // compaction output fills segments of its own
	stepMin int64  // compactions with at most this much output install whole
	workers int    // concurrent compactions per disk
	rate    int64  // output bytes a worker produces per checkpoint interval
	cps     int    // checkpoints to run
	warmup  int    // checkpoints before measuring
	slack   int    // metadata segments the cleaner lets fragmentation strand
}

type idxCfg struct {
	name    string
	keys    float64
	entry   float64
	updates []float64 // updates per checkpoint, cycled
	amp     float64   // RocksDB universal's size amplification trigger, 0 for none
	initial bool      // start with one run holding every key
}

type piece struct {
	hi     float64
	bytes  int64
	seg    int
	dead   bool
	doomed bool
	inRun  bool
	busy   bool
}

type run struct {
	keys   float64
	bytes  int64
	pieces []*piece
	bound  float64
	root   *piece
	next   int
}

type compaction struct {
	inputs   []*run
	out      *run
	outKeys  float64
	outBytes int64
	done     int64
	progress float64
}

type index struct {
	cfg    idxCfg
	runs   []*run // newest first
	comp   *compaction
	worker int
	cp     int
}

type sim struct {
	c         cfg
	live      []int64
	used      []bool
	pieces    [][]*piece
	opens     []int
	fills     []int64
	lane      int
	steps     int
	busy      []int
	free      []int
	idx       []*index
	manifest  *piece
	pending   []*piece // pieces that die once the next manifest is durable
	named     int64
	held      int64
	usedCount int

	flushed, compWritten, manWritten, rootWritten, copied int64
	peakSegs, peakBytes                                   int64
	sumSegs, sumBytes, samples                            int64
	peakUsed                                              int
	maxRuns                                               []int
	sumNamed                                              float64
	exhausted                                             int64
	reserveSegs                                           int
}

func newSim(c cfg) *sim {
	s := &sim{c: c, live: make([]int64, c.disk.nSegs), used: make([]bool, c.disk.nSegs), busy: make([]int, c.disk.nSegs),
		pieces: make([][]*piece, c.disk.nSegs)}
	for range len(c.indexes) + 1 {
		s.opens = append(s.opens, -1)
		s.fills = append(s.fills, 0)
	}
	for i := c.disk.nSegs - 1; i >= 0; i-- {
		s.free = append(s.free, i)
	}
	for _, ic := range c.indexes {
		s.idx = append(s.idx, &index{cfg: ic})
	}
	s.maxRuns = make([]int, len(c.indexes))
	return s
}

func (s *sim) isOpen(g int) bool {
	for _, o := range s.opens {
		if o == g {
			return true
		}
	}
	return false
}

func (s *sim) openSeg() {
	old := s.opens[s.lane]
	s.opens[s.lane] = -1
	if old >= 0 && s.live[old] == 0 {
		s.freeSeg(old)
	}
	n := len(s.free)
	if n == 0 {
		panic("metadata stream ran out of segments")
	}
	g := s.free[n-1]
	s.free = s.free[:n-1]
	s.used[g] = true
	s.usedCount++
	s.opens[s.lane] = g
	s.fills[s.lane] = 0
}

func (s *sim) freeSeg(g int) {
	s.used[g] = false
	s.usedCount--
	s.pieces[g] = s.pieces[g][:0]
	s.free = append(s.free, g)
}

func (s *sim) write(p *piece) {
	if s.opens[s.lane] < 0 || s.fills[s.lane]+p.bytes > s.c.disk.segBytes {
		s.openSeg()
	}
	g := s.opens[s.lane]
	p.seg = g
	p.dead = false
	s.held += p.bytes
	s.live[g] += p.bytes
	s.fills[s.lane] += p.bytes
	s.pieces[g] = append(s.pieces[g], p)
}

func (s *sim) kill(p *piece) {
	if p == nil || p.dead {
		return
	}
	p.dead = true
	if p.busy {
		s.busy[p.seg]--
	}
	s.held -= p.bytes
	s.live[p.seg] -= p.bytes
	if s.live[p.seg] == 0 && !s.isOpen(p.seg) {
		s.freeSeg(p.seg)
	}
}

// newRun lays out keys distinct keys evenly over the key space [0, 1) in pieces.
func (s *sim) newRun(keys, entry float64) *run {
	bytes := int64(keys * entry)
	n := (bytes + s.c.disk.pieceBytes - 1) / s.c.disk.pieceBytes
	if n == 0 {
		n = 1
	}
	r := &run{keys: keys, bytes: bytes}
	for j := int64(0); j < n; j++ {
		b := bytes / n
		if j == n-1 {
			b = bytes - b*(n-1)
		}
		r.pieces = append(r.pieces, &piece{hi: float64(j+1) / float64(n), bytes: b})
	}
	return r
}

func (s *sim) writeRun(r *run) {
	for _, p := range r.pieces {
		s.write(p)
		s.name(p)
	}
}

func (s *sim) name(p *piece) {
	p.inRun = true
	s.named += p.bytes
}

func distinct(k, u float64) float64 { return k * (1 - math.Exp(-u/k)) }

func similar(size, avg, small int64) bool {
	return (2*size > avg && 2*size < 3*avg) || (size < small && avg < small)
}

const (
	minMerge    = 4
	maxMerge    = 32
	minRunBytes = 1 << 20
)

func (s *sim) maxRunBytes() int64 {
	pieces := (s.c.disk.pieceBytes - 32) / 18
	return (pieces - 1) * (s.c.disk.pieceBytes - 32) / 2
}

// pick is internal/lsm's PickCompaction over modelled run sizes.
func (s *sim) pick(x *index) *compaction {
	sizes := make([]int64, len(x.runs))
	for i, r := range x.runs {
		sizes[i] = r.bytes
	}
	best, bestAvg := [2]int{}, int64(-1)
	for i := 0; i < len(sizes); {
		j, sum := i+1, sizes[i]
		for j < len(sizes) && j-i < maxMerge && similar(sizes[j], sum/int64(j-i), minRunBytes) {
			sum += sizes[j]
			j++
		}
		if n := j - i; n >= minMerge && sum <= s.maxRunBytes() {
			if avg := sum / int64(n); bestAvg < 0 || avg < bestAvg {
				best, bestAvg = [2]int{i, j}, avg
			}
		}
		i = j
	}
	if x.cfg.amp > 0 && len(x.runs) > 1 {
		var rest int64
		for _, r := range x.runs[:len(x.runs)-1] {
			rest += r.bytes
		}
		if float64(rest) >= x.cfg.amp*float64(x.runs[len(x.runs)-1].bytes) {
			best, bestAvg = [2]int{0, len(x.runs)}, 0
		}
	}
	if bestAvg < 0 {
		return nil
	}
	c := &compaction{inputs: append([]*run(nil), x.runs[best[0]:best[1]]...)}
	miss := 1.0
	for _, r := range c.inputs {
		miss *= 1 - r.keys/x.cfg.keys
	}
	c.outKeys = x.cfg.keys * (1 - miss)
	c.outBytes = int64(c.outKeys * x.cfg.entry)
	return c
}

func (s *sim) manifestBytes() int64 {
	runs := 0
	for _, x := range s.idx {
		runs += len(x.runs) + 1
	}
	return int64(float64(s.c.disk.nSegs)*s.c.disk.fill)*17 + int64(runs)*(18+16)
}

func (s *sim) namedBytes() int64 { return s.named }

// manifest writes a manifest, measures what the disk holds while the previous one is still
// the durable one, then retires everything the new manifest no longer names.
func (s *sim) writeManifest(cp int, namedOld int64) int64 {
	m := &piece{bytes: s.manifestBytes()}
	s.write(m)
	s.manWritten += m.bytes
	namedNew := s.namedBytes()
	if cp >= s.c.warmup {
		held, usedSegs := s.held, s.usedCount
		base := max(namedOld, namedNew)
		tb := held - base
		ts := int64(usedSegs) - (held+s.c.disk.segBytes-1)/s.c.disk.segBytes
		s.peakBytes = max(s.peakBytes, tb)
		s.peakSegs = max(s.peakSegs, ts)
		s.peakUsed = max(s.peakUsed, usedSegs)
		s.sumBytes += tb
		s.sumSegs += ts
		s.samples++
		if int((tb+s.c.disk.segBytes-1)/s.c.disk.segBytes) > s.reserveSegs {
			s.exhausted++
		}
	}
	for _, p := range s.pending {
		s.kill(p)
	}
	s.pending = s.pending[:0]
	s.kill(s.manifest)
	s.manifest = m
	return namedNew
}

func (s *sim) doom(p *piece) {
	if !p.dead && !p.doomed {
		p.doomed = true
		if p.inRun {
			s.named -= p.bytes
		}
		s.pending = append(s.pending, p)
	}
}

// clean copies the emptiest metadata segment's live pieces to the head while fragmentation
// strands more than slack segments and that segment is at most half live. A segment holding
// inputs of a running compaction is no victim, since they die when it ends.
func (s *sim) clean() {
	for {
		held, usedSegs := s.held, s.usedCount
		if int64(usedSegs)-(held+s.c.disk.segBytes-1)/s.c.disk.segBytes <= int64(s.c.slack) {
			return
		}
		victim, least := -1, int64(math.MaxInt64)
		for g := range s.live {
			if s.used[g] && !s.isOpen(g) && s.busy[g] == 0 && s.live[g] < least {
				victim, least = g, s.live[g]
			}
		}
		if victim < 0 || least > s.c.disk.segBytes/2 {
			return
		}
		moving := append([]*piece(nil), s.pieces[victim]...)
		for _, p := range moving {
			if p.dead || p.seg != victim {
				continue
			}
			s.live[victim] -= p.bytes
			s.held -= p.bytes
			s.copied += p.bytes
			s.write(p)
		}
		if s.live[victim] == 0 {
			s.freeSeg(victim)
		}
	}
}

func (s *sim) checkpoint(cp int) {
	namedOld := s.namedBytes()
	for _, x := range s.idx {
		u := x.cfg.updates[x.cp%len(x.cfg.updates)]
		x.cp++
		r := s.newRun(distinct(x.cfg.keys, u), x.cfg.entry)
		s.writeRun(r)
		s.flushed += r.bytes
		x.runs = append([]*run{r}, x.runs...)
	}
	stepsBefore := s.steps
	budget := make([]int64, s.c.workers)
	for w := range budget {
		budget[w] = s.c.rate
	}
	for w := range budget {
		for budget[w] > 0 {
			xi, x := s.assigned(w)
			if x == nil {
				xi, x = s.start(cp, w)
			}
			if x == nil {
				break
			}
			if s.c.mode == "whole" {
				c := x.comp
				n := min(budget[w], c.outBytes-c.done)
				budget[w] -= n
				c.done += n
				if c.done < c.outBytes {
					continue
				}
				c.out = s.newRun(c.outKeys, x.cfg.entry)
				if s.c.ownLane {
					s.lane = xi + 1
				}
				s.writeRun(c.out)
				s.lane = 0
				s.compWritten += c.out.bytes
				s.install(x, c)
				for _, r := range c.inputs {
					for _, p := range r.pieces {
						s.doom(p)
					}
				}
				x.comp = nil
				continue
			}
			budget[w] -= s.stepOnce(cp, xi, x, budget[w], &namedOld)
		}
	}
	for i, x := range s.idx {
		s.maxRuns[i] = max(s.maxRuns[i], len(x.runs))
	}
	if s.c.mode == "whole" || s.steps == stepsBefore {
		s.writeManifest(cp, namedOld)
	}
	s.clean()
}

func (s *sim) assigned(w int) (int, *index) {
	for i, x := range s.idx {
		if x.comp != nil && x.worker == w {
			return i, x
		}
	}
	return -1, nil
}

func (s *sim) start(cp, w int) (int, *index) {
	for k := range s.idx {
		i := (cp + k) % len(s.idx)
		x := s.idx[i]
		if x.comp != nil {
			continue
		}
		c := s.pick(x)
		if c == nil {
			continue
		}
		x.comp, x.worker = c, w
		for _, r := range c.inputs {
			for _, p := range r.pieces {
				if !p.dead {
					p.busy = true
					s.busy[p.seg]++
				}
			}
		}
		return i, x
	}
	return -1, nil
}

// stepOnce writes one step of a compaction's output, ending with a manifest, and returns the
// bytes it wrote.
func (s *sim) stepOnce(cp, xi int, x *index, budget int64, namedOld *int64) int64 {
	c := x.comp
	if c.out == nil {
		c.out = &run{keys: c.outKeys, bytes: c.outBytes}
		s.install(x, c)
	}
	step := s.c.step
	if c.outBytes <= s.c.stepMin {
		step = c.outBytes
	}
	n := min(step, budget, c.outBytes-c.done)
	for left := n; left > 0; {
		b := min(left, s.c.disk.pieceBytes)
		left -= b
		c.done += b
		p := &piece{hi: float64(c.done) / float64(c.outBytes), bytes: b}
		if s.c.ownLane {
			s.lane = xi + 1
		}
		s.write(p)
		s.lane = 0
		s.name(p)
		c.out.pieces = append(c.out.pieces, p)
		s.compWritten += b
	}
	if c.done >= c.outBytes {
		c.progress = 1
	} else {
		c.progress = float64(c.done) / float64(c.outBytes)
	}
	root := &piece{bytes: int64(18*len(c.out.pieces) + 32)}
	s.write(root)
	s.name(root)
	s.rootWritten += root.bytes
	if c.out.root != nil {
		s.doom(c.out.root)
	}
	c.out.root = root
	for _, r := range c.inputs {
		r.bound = c.progress
		for r.next < len(r.pieces) && r.pieces[r.next].hi <= c.progress {
			s.doom(r.pieces[r.next])
			r.next++
		}
	}
	*namedOld = s.writeManifest(cp, *namedOld)
	s.steps++
	if c.progress >= 1 {
		s.dropInputs(x, c)
		x.comp = nil
	}
	return max(n, 1)
}

// install puts the output in place of the inputs; in step mode the inputs stay listed, each
// bounded below by the compaction's progress, until the last step.
func (s *sim) install(x *index, c *compaction) {
	i := -1
	for k, r := range x.runs {
		if r == c.inputs[0] {
			i = k
			break
		}
	}
	if i < 0 {
		panic("inputs not found")
	}
	runs := append([]*run(nil), x.runs[:i]...)
	if s.c.mode == "step" {
		runs = append(runs, c.inputs...)
	}
	runs = append(runs, c.out)
	runs = append(runs, x.runs[i+len(c.inputs):]...)
	x.runs = runs
}

func (s *sim) dropInputs(x *index, c *compaction) {
	in := map[*run]bool{}
	for _, r := range c.inputs {
		in[r] = true
	}
	runs := x.runs[:0]
	for _, r := range x.runs {
		if !in[r] {
			runs = append(runs, r)
		}
	}
	x.runs = runs
}

func (s *sim) runAll() {
	for _, x := range s.idx {
		if x.cfg.initial {
			r := s.newRun(x.cfg.keys, x.cfg.entry)
			s.writeRun(r)
			x.runs = []*run{r}
		}
	}
	s.writeManifest(-1, 0)
	for cp := 0; cp < s.c.cps; cp++ {
		s.checkpoint(cp)
		if cp >= s.c.warmup {
			s.sumNamed += float64(s.namedBytes())
		}
	}
}

func (s *sim) report(out *strings.Builder) {
	seg := float64(s.c.disk.segBytes) / MiB
	var live float64
	for _, x := range s.idx {
		live += x.cfg.keys * x.cfg.entry
	}
	runs := make([]string, len(s.maxRuns))
	for i, n := range s.maxRuns {
		runs[i] = fmt.Sprintf("%s:%d", s.idx[i].cfg.name, n)
	}
	sort.Strings(runs)
	mode := s.c.mode
	if mode == "step" {
		mode = fmt.Sprintf("step %d MiB", s.c.step/MiB)
	}
	if s.c.ownLane {
		mode += "+own"
	}
	if s.c.stepMin > 0 {
		mode += fmt.Sprintf(">%dG", s.c.stepMin>>30)
	}
	if *verbose {
		for _, x := range s.idx {
			var sizes []string
			var sum int64
			for _, r := range x.runs {
				sizes = append(sizes, fmt.Sprintf("%.0f", float64(r.bytes)/MiB))
				sum += r.bytes
			}
			fmt.Fprintf(out, "  %s live %.0f MiB, runs %.0f MiB: %s\n", x.cfg.name, x.cfg.keys*x.cfg.entry/MiB, float64(sum)/MiB, strings.Join(sizes, " "))
		}
	}
	fmt.Fprintf(out, "%-34s %-19s held beyond manifests: peak %6.0f MiB = %4d segs, mean %6.1f MiB; over %d-seg reserve at %5.1f%% of manifests | fragmentation peak %3d mean %5.1f segs | compaction %5.2f, roots %.4f, manifests %.4f, meta cleaner %.3f per flushed byte | named/live %.2f | max runs %s\n",
		s.c.name, mode, float64(s.peakBytes)/MiB, (s.peakBytes+s.c.disk.segBytes-1)/s.c.disk.segBytes, float64(s.sumBytes)/float64(s.samples)/MiB,
		s.reserveSegs, 100*float64(s.exhausted)/float64(s.samples), s.peakSegs, float64(s.sumSegs)/float64(s.samples),
		float64(s.compWritten)/float64(s.flushed), float64(s.rootWritten)/float64(s.flushed),
		float64(s.manWritten)/float64(s.flushed), float64(s.copied)/float64(s.flushed),
		s.sumNamed/float64(s.c.cps-s.c.warmup)/live, strings.Join(runs, " "))
	_ = seg
}

var verbose = flag.Bool("v", false, "print each index's runs at the end")

func main() {
	only := flag.String("only", "", "run scenarios whose name contains this")
	flag.Parse()
	real := disk{segBytes: 64 * MiB, nSegs: 4096, pieceBytes: MiB, fill: 0.9}
	stand := disk{segBytes: 4 * MiB, nSegs: 4096, pieceBytes: MiB, fill: 0.9}
	chunk := idxCfg{name: "chunk", keys: 25e6, entry: 40, updates: []float64{50000}, initial: true}
	ms := func(name string, d disk, share float64, flushBytes float64) idxCfg {
		bytes := share * float64(d.segBytes) * float64(d.nSegs)
		return idxCfg{name: name, keys: bytes / 100, entry: 100, updates: []float64{flushBytes / 100}, initial: true}
	}
	type scen struct {
		name    string
		d       disk
		idx     []idxCfg
		workers int
		rate    int64
		cps     int
		reserve int
	}
	scens := []scen{
		{"S1 stand-in 16GiB, MS 2%", stand, []idxCfg{ms("ms", stand, 0.02, 2*MiB), {name: "chunk", keys: 1.5e6, entry: 40, updates: []float64{100, 6000}, initial: true}}, 1, 32 * MiB, 4000, 8},
		{"R1 256GiB, MS 2%", real, []idxCfg{ms("ms", real, 0.02, 32*MiB), chunk}, 1, 512 * MiB, 3000, 8},
		{"R2 256GiB, MS 6.25%", real, []idxCfg{ms("ms", real, 0.0625, 32*MiB), chunk}, 1, 512 * MiB, 4000, 8},
		{"R3 256GiB, 8 shards x 1%", real, func() []idxCfg {
			var v []idxCfg
			for i := 0; i < 8; i++ {
				v = append(v, ms(fmt.Sprintf("ms%d", i), real, 0.01, 4*MiB))
			}
			return append(v, chunk)
		}(), 2, 512 * MiB, 3000, 8},
	}
	type mode struct {
		mode string
		step int64
		own  bool
		min  int64
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# held beyond manifests: metadata bytes the log holds beyond max(bytes the durable manifest names, bytes the next one names), sampled as each manifest is written\n")
	fmt.Fprintf(&out, "# fragmentation: metadata segments in use beyond ceil(held bytes / segment); a greedy cleaner copies segments at most half live while it exceeds 4\n")
	fmt.Fprintf(&out, "# compaction, roots, manifests, meta cleaner: bytes written per byte flushed from memtables; named/live: bytes the manifest names per live index byte\n")
	fmt.Fprintf(&out, "# whole: output installed once complete (design today); step N: output installed every N MiB, each step ending with a manifest; +own: compaction output fills metadata segments of its own\n")
	fmt.Fprintf(&out, "# picker adjacent: internal/lsm's buckets of runs adjacent in age; +amp: also RocksDB universal's size amplification trigger, all runs when the newer runs exceed 2x the oldest\n")
	for _, sc := range scens {
		if *only != "" && !strings.Contains(sc.name, *only) {
			continue
		}
		modes := []mode{{"whole", 0, false, 0}, {"whole", 0, true, 0}, {"step", 64 * MiB, false, 0}, {"step", 64 * MiB, true, 0}, {"step", 256 * MiB, true, 0}}
		if sc.d.segBytes == 4*MiB {
			modes = []mode{{"whole", 0, false, 0}, {"whole", 0, true, 0}, {"step", 16 * MiB, false, 0}, {"step", 16 * MiB, true, 0}, {"step", 4 * MiB, true, 0}}
		}
		for _, amp := range []float64{0, 2} {
			idx := make([]idxCfg, len(sc.idx))
			copy(idx, sc.idx)
			name := sc.name
			if amp > 0 {
				name += " +amp"
			}
			for i := range idx {
				idx[i].amp = amp
			}
			for _, m := range modes {
				c := cfg{name: name, disk: sc.d, indexes: idx, mode: m.mode, step: m.step, ownLane: m.own, stepMin: m.min,
					workers: sc.workers, rate: sc.rate, cps: sc.cps, warmup: sc.cps / 5, slack: 4}
				s := newSim(c)
				s.reserveSegs = sc.reserve
				s.runAll()
				s.report(&out)
			}
		}
	}
	os.Stdout.WriteString(out.String())
}
