// Command tracedata turns a simulator trace into the data module the epoch model's trace spec reads: the run's constants and one line per protocol step. It decides no protocol step, and holds the mapping's judgement in two places: classify names the spec's changes for a table, and landOpen and crash name the versions a node may have made durable unrecorded.
//
//	tracedata [-module trace_data] run.trace > trace_data.tla
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// verBase is the spec's version packing, TVer in models/epoch/trace_epoch.tla.
const verBase = 1 << 20

type member struct {
	disk, mark uint64
	leaving    bool
}

type past struct {
	interval, minLive uint64
	members           []member
}

type entry struct {
	interval, activated, minLive uint64
	members                      []member
	past                         []past
}

type write struct {
	chunk, block, inc int
	whole             bool
}

type reply struct {
	decided uint64
	chunks  map[uint64][3]uint64
}

type gen struct {
	pgs     []uint64
	chunks  []uint64
	chunkPG []int
	blocks  []int
	entries map[uint64]*entry
	initial map[uint64]entry
	nodes   int
	writes  []write
	wid     map[uint64]int
	plan    []string
	tables  int
	epochs  map[uint64]int
	lines   []string
	replies map[[3]uint64]map[uint64]*reply
	cur     *reply
	// disks maps a disk ID to the spec's: one formatted again takes the next past the nodes.
	diskIDs map[uint64]uint64
	fmts    int
	// primary is each PG's newest activated node; ownOpen a node's versions it ordered and has not logged, memberOpen those a member has not.
	primary    map[uint64]int
	ownOpen    map[int][][2]uint64
	memberOpen map[int][][2]uint64
	// lists holds each [disk, pg, interval] a Persisted record named.
	lists map[[3]uint64]bool
	// operatorAsk holds each PG the harness, no primary, asked a new interval of: its next table is an Edit that changes nothing.
	operatorAsk map[uint64]bool
	// activated is each PG's newest activated interval, tableIv the interval of each PG at each epoch.
	activated map[uint64]uint64
	tableIv   map[uint64]map[uint64]uint64
	// left is the line where a [node, pg] stopped being in the PG's activated interval, formatAt the line where a spec disk's node formatted it, and nodeDisk a node's spec disk once formatted.
	left     map[[2]uint64]int
	formatAt map[uint64]int
	nodeDisk map[int]uint64
	// recovering holds each node's recovery writes its store took and has not reported: chunk, spec source, interval.
	recovering map[int][][3]uint64
}

func ver(v uint64) string { return strconv.FormatUint((v>>32)*verBase+v&(1<<32-1), 10) }

func (g *gen) pg(f uint64) int {
	i := slices.Index(g.pgs, f)
	if i < 0 {
		fatal("unknown PG %d", f)
	}
	return i + 1
}

func (g *gen) chunk(hash uint64) int {
	i := slices.Index(g.chunks, hash)
	if i < 0 {
		fatal("chunk %d was never written", hash)
	}
	return i + 1
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tracedata: "+format+"\n", args...)
	os.Exit(1)
}

// chunkField is the PG of a chunk, by its trace field.
func (g *gen) chunkField(hash uint64) uint64 { return g.pgs[g.chunkPG[g.chunk(hash)-1]-1] }

func (g *gen) disk(id uint64) uint64 {
	if d, ok := g.diskIDs[id]; ok {
		return d
	}
	return id
}

// source is the spec's disk for a Recovered record's source, 0 for none.
func (g *gen) source(id uint64) uint64 {
	if id == 0 {
		return 0
	}
	return g.disk(id)
}

func dropVersion(l [][2]uint64, c, v uint64) [][2]uint64 {
	return slices.DeleteFunc(l, func(x [2]uint64) bool { return x[0] == c && x[1] == v })
}

// landOpen lets the versions node n's store took and has not reported durable land now: the spec logs a version as a member takes it, and a table or a crash may come before the record.
func (g *gen) landOpen(n int) {
	for _, x := range g.memberOpen[n] {
		g.line(`k |-> "maydeliver", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(x[0]), ver(x[1]))
	}
}

// crash writes a process crash: a write in flight may have reached the disk, so the lines before the restart let the versions a member had not reported durable land, and those the node's own log had not lose their record.
func (g *gen) crash(n int) {
	g.landOpen(n)
	for _, x := range g.recovering[n] {
		f := g.chunkField(x[0])
		if x[2] == g.activated[f] {
			g.lineAt(g.recoveredAt(f, n, x[1]), `k |-> "mayrecover", n |-> %d, l |-> %d, c |-> %d, s |-> %d, iv |-> %d`, n, g.primary[f], g.chunk(x[0]), x[1], x[2])
		}
	}
	g.recovering[n] = nil
	own := g.ownOpen[n]
	for j := len(own) - 1; j >= 0; j-- {
		g.line(`k |-> "mayfail", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(own[j][0]), ver(own[j][1]))
	}
	g.memberOpen[n], g.ownOpen[n] = nil, nil
	g.line(`k |-> "restart", n |-> %d`, n)
}

func (g *gen) line(format string, args ...any) {
	g.lines = append(g.lines, "["+fmt.Sprintf(format, args...)+"]")
}

// lineAt writes a line at position at, before the lines already there, and keeps the marked positions on the lines they name.
func (g *gen) lineAt(at int, format string, args ...any) {
	if at == len(g.lines) {
		g.line(format, args...)
		return
	}
	g.lines = slices.Insert(g.lines, at, "["+fmt.Sprintf(format, args...)+"]")
	for k, p := range g.left {
		if p >= at {
			g.left[k] = p + 1
		}
	}
	for k, p := range g.formatAt {
		if p >= at {
			g.formatAt[k] = p + 1
		}
	}
}

// leave marks that node n leaves PG f's activated interval at the next line.
func (g *gen) leave(n int, f uint64) {
	k := [2]uint64{uint64(n), f}
	if _, ok := g.left[k]; !ok {
		g.left[k] = len(g.lines)
	}
}

// recoveredAt is where a recovery write of PG f to node n read from spec disk s lands: the spec takes its read and its write as one step while its primary leads, so a record from after the primary, the target or the source left the interval goes before the first of those steps, across lines that keep what Recover reads. The store took the write in time, as maydeliver lets an open version land.
func (g *gen) recoveredAt(f uint64, n int, s uint64) int {
	at := len(g.lines)
	for _, x := range []int{g.primary[f], n} {
		if p, ok := g.left[[2]uint64{uint64(x), f}]; ok {
			at = min(at, p)
		}
	}
	if p, ok := g.formatAt[s]; ok {
		at = min(at, p)
	}
	return at
}

func marks(m uint64) string { return [...]string{"up", "down", "maint"}[m] }

func disks(ms []member) []uint64 {
	var d []uint64
	for _, m := range ms {
		d = append(d, m.disk)
	}
	return d
}

// classify appends the spec's table changes for one committed table: its folds, then its membership changes.
func (g *gen) classify(epoch uint64, changed []uint64, next map[uint64]*entry) {
	for _, f := range changed {
		old, e := g.entries[f], next[f]
		if e.activated > old.activated && e.activated > 1 {
			g.tables++
			g.line(`k |-> "fold", p |-> %d, iv |-> %d`, g.pg(f), e.activated)
		}
	}
	marked := map[[2]uint64]bool{}
	var rest []uint64
	for _, f := range changed {
		old, e := g.entries[f], next[f]
		if e.interval == old.interval {
			continue
		}
		d, to, ok := oneMark(old, e)
		if ok {
			marked[[2]uint64{d, to}] = true
			continue
		}
		rest = append(rest, f)
	}
	for _, f := range rest {
		old, e := g.entries[f], next[f]
		p := g.pg(f)
		a, b := disks(old.members), disks(e.members)
		asked := g.operatorAsk[f]
		delete(g.operatorAsk, f)
		switch {
		case slices.Equal(a, b) && sameMembers(old, e) && e.minLive == old.minLive && !asked:
			g.tables++
			g.line(`k |-> "renew", p |-> %d`, p)
		case len(b) == len(a)+1 && slices.Equal(a, b[:len(a)]) && leavingOf(e) != 0 && sameMarks(old, &entry{members: e.members[:len(a)]}):
			g.planned(fmt.Sprintf(`[k |-> "add", p |-> %d, old |-> %d, new |-> %d]`, p, g.disk(leavingOf(e)), g.disk(b[len(a)])))
		case len(b) == len(a)-1 && leavingOf(old) != 0 && slices.Equal(b, slices.DeleteFunc(slices.Clone(a), func(d uint64) bool { return d == leavingOf(old) })):
			g.planned(fmt.Sprintf(`[k |-> "remove", p |-> %d]`, p))
		case len(a) == len(b) && differs(a, b) == 1:
			j := slices.IndexFunc(a, func(d uint64) bool { return !slices.Contains(b, d) })
			g.planned(fmt.Sprintf(`[k |-> "replace", p |-> %d, old |-> %d, new |-> %d]`, p, g.disk(a[j]), g.disk(b[j])))
		case len(a) >= 2 && len(a) == len(b) && a[0] == b[1] && a[1] == b[0] && slices.Equal(a[2:], b[2:]) && sameMarks(old, e):
			g.planned(fmt.Sprintf(`[k |-> "rotate", p |-> %d]`, p))
		case len(a) == len(b) && differs(sortedCopy(a), sortedCopy(b)) == 0:
			var mem, ms []string
			for _, m := range e.members {
				mem = append(mem, strconv.FormatUint(g.disk(m.disk), 10))
				ms = append(ms, `"`+marks(m.mark)+`"`)
			}
			g.planned(fmt.Sprintf(`[k |-> "edit", p |-> %d, mem |-> <<%s>>, marks |-> <<%s>>]`, p, strings.Join(mem, ", "), strings.Join(ms, ", ")))
		default:
			fatal("table %d changes PG %d from %v to %v: no spec change maps it", epoch, p, old.members, e.members)
		}
	}
	for _, k := range sortedPairs(marked) {
		g.planned(fmt.Sprintf(`[k |-> "mark", d |-> %d, to |-> "%s"]`, g.disk(k[0]), marks(k[1])))
	}
	for _, f := range changed {
		g.entries[f] = next[f]
	}
	g.tableIv[epoch] = map[uint64]uint64{}
	for f, e := range g.entries {
		g.tableIv[epoch][f] = e.interval
	}
	g.epochs[epoch] = g.tables
	g.tableLine(changed)
}

func (g *gen) memberList(ms []member) (mem, ms2 string) {
	var d, k []string
	for _, m := range ms {
		d = append(d, strconv.FormatUint(g.disk(m.disk), 10))
		k = append(k, `"`+marks(m.mark)+`"`)
	}
	return "<<" + strings.Join(d, ", ") + ">>", "<<" + strings.Join(k, ", ") + ">>"
}

// tableLine has the check compare the entries of the code's table with the spec's after the changes classify named.
func (g *gen) tableLine(changed []uint64) {
	var rows []string
	for _, f := range changed {
		e := g.entries[f]
		mem, ms := g.memberList(e.members)
		var ps []string
		for _, x := range e.past {
			pm, pk := g.memberList(x.members)
			ps = append(ps, fmt.Sprintf("[iv |-> %d, ml |-> %d, mem |-> %s, marks |-> %s]", x.interval, x.minLive, pm, pk))
		}
		rows = append(rows, fmt.Sprintf("[p |-> %d, iv |-> %d, la |-> %d, ml |-> %d, lv |-> %d, mem |-> %s, marks |-> %s, past |-> <<%s>>]",
			g.pg(f), e.interval, e.activated, e.minLive, g.disk(leavingOf(e)), mem, ms, strings.Join(ps, ", ")))
	}
	g.line(`k |-> "table", rows |-> <<%s>>`, strings.Join(rows, ", "))
}

func (g *gen) planned(item string) {
	g.plan = append(g.plan, item)
	g.tables++
	g.line(`k |-> "plan"`)
}

func leavingOf(e *entry) uint64 {
	for _, m := range e.members {
		if m.leaving {
			return m.disk
		}
	}
	return 0
}

func sortedCopy(a []uint64) []uint64 {
	a = slices.Clone(a)
	slices.Sort(a)
	return a
}

func differs(a, b []uint64) int {
	n := 0
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func sortedPairs(m map[[2]uint64]bool) [][2]uint64 {
	var out [][2]uint64
	for k := range m {
		out = append(out, k)
	}
	slices.SortFunc(out, func(a, b [2]uint64) int {
		if a[0] != b[0] {
			return int(a[0]) - int(b[0])
		}
		return int(a[1]) - int(b[1])
	})
	return out
}

func sameMembers(a, b *entry) bool { return slices.Equal(a.members, b.members) }

func sameMarks(a, b *entry) bool {
	mark := func(e *entry, d uint64) uint64 {
		return e.members[slices.IndexFunc(e.members, func(m member) bool { return m.disk == d })].mark
	}
	for _, m := range a.members {
		if mark(b, m.disk) != m.mark {
			return false
		}
	}
	return true
}

// oneMark reports the one disk whose mark alone changes between a and b.
func oneMark(a, b *entry) (disk, to uint64, ok bool) {
	if !slices.Equal(disks(a.members), disks(b.members)) {
		return 0, 0, false
	}
	n := 0
	for i := range a.members {
		if a.members[i].mark != b.members[i].mark {
			disk, to = a.members[i].disk, b.members[i].mark
			n++
		}
	}
	return disk, to, n == 1
}

func (g *gen) load(path string) []trace.Record {
	f, err := os.Open(path)
	if err != nil {
		fatal("%v", err)
	}
	defer f.Close()
	var recs []trace.Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	started := false
	for sc.Scan() {
		r, err := trace.Parse(sc.Bytes())
		if err != nil {
			fatal("%v", err)
		}
		started = started || r.Name == trace.OutTable
		if started {
			recs = append(recs, r)
		}
		if r.Sim && int(r.Node) > g.nodes {
			g.nodes = int(r.Node)
		}
	}
	if err := sc.Err(); err != nil {
		fatal("%v", err)
	}
	return recs
}

func (g *gen) tableAt(recs []trace.Record, i int) (epoch uint64, changed []uint64, next map[uint64]*entry, end int) {
	epoch = recs[i].Fields[0]
	next = map[uint64]*entry{}
	var cur *entry
	var members *[]member
	for i++; i < len(recs); i++ {
		r := &recs[i]
		switch r.Name {
		case trace.OutEntry:
			cur = &entry{interval: r.Fields[1], activated: r.Fields[2], minLive: r.Fields[3]}
			next[r.Fields[0]] = cur
			changed = append(changed, r.Fields[0])
			members = &cur.members
		case trace.OutPast:
			cur.past = append(cur.past, past{interval: r.Fields[1], minLive: r.Fields[2]})
			members = &cur.past[len(cur.past)-1].members
		case trace.OutMember:
			*members = append(*members, member{r.Fields[1], r.Fields[2], r.Fields[3] != 0})
		default:
			return epoch, changed, next, i
		}
	}
	return epoch, changed, next, i
}

func (g *gen) scan(recs []trace.Record) {
	for i := range recs {
		r := &recs[i]
		if r.Name != trace.OutSend {
			continue
		}
		c := r.Fields[1]
		j := slices.Index(g.chunks, c)
		if j < 0 {
			g.chunks = append(g.chunks, c)
			g.chunkPG = append(g.chunkPG, g.pg(r.Fields[2]))
			g.blocks = append(g.blocks, 1)
			j = len(g.chunks) - 1
		}
		off, n := r.Fields[3]>>32, r.Fields[3]&(1<<32-1)
		blk := int(off/4096) + 1
		g.blocks[j] = max(g.blocks[j], blk)
		g.writes = append(g.writes, write{chunk: j + 1, block: blk, inc: int(r.Fields[4]), whole: off%4096 == 0 && n%4096 == 0})
		g.wid[r.Fields[0]] = len(g.writes)
	}
}

func (g *gen) run(recs []trace.Record) {
	epoch, changed, first, i := g.tableAt(recs, 0)
	g.pgs = slices.Clone(changed)
	for f, e := range first {
		g.entries[f], g.initial[f] = e, *e
	}
	g.tables = 1
	g.epochs[epoch] = 1
	g.scan(recs)
	for i < len(recs) {
		r := &recs[i]
		n := int(r.Node) + 1
		i++
		switch r.Name {
		case trace.OutTable:
			epoch, changed, next, end := g.tableAt(recs, i-1)
			g.classify(epoch, changed, next)
			i = end
		case trace.OutAdopt:
			for _, f := range g.pgs {
				if iv := g.tableIv[r.Fields[0]][f]; iv > g.activated[f] && g.activated[f] != 0 {
					g.leave(n, f)
				}
			}
			g.landOpen(n)
			e, ok := g.epochs[r.Fields[0]]
			if !ok {
				fatal("node %d adopts epoch %d, which no table named", n, r.Fields[0])
			}
			g.line(`k |-> "adopt", n |-> %d, e |-> %d`, n, e)
		case trace.OutSend:
			g.line(`k |-> "send", w |-> %d`, g.wid[r.Fields[0]])
		case trace.OutOrder:
			g.line(`k |-> "order", n |-> %d, w |-> %d, c |-> %d, v |-> %s`, n, g.wid[r.Fields[0]], g.chunk(r.Fields[1]), ver(r.Fields[2]))
			cv := [2]uint64{r.Fields[1], r.Fields[2]}
			g.ownOpen[n] = append(g.ownOpen[n], cv)
		case trace.OutAccepted:
			g.memberOpen[n] = append(g.memberOpen[n], [2]uint64{r.Fields[0], r.Fields[1]})
		case trace.OutMemberFail:
			kind := "mfail"
			if r.Fields[2] != 0 {
				kind = "gone"
			}
			g.line(`k |-> "%s", n |-> %d, c |-> %d, v |-> %s`, kind, n, g.chunk(r.Fields[0]), ver(r.Fields[1]))
		case trace.OutLogged:
			g.ownOpen[n] = dropVersion(g.ownOpen[n], r.Fields[0], r.Fields[1])
			g.memberOpen[n] = dropVersion(g.memberOpen[n], r.Fields[0], r.Fields[1])
			g.line(`k |-> "logged", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(r.Fields[0]), ver(r.Fields[1]))
		case trace.OutRecovering:
			g.recovering[n] = append(g.recovering[n], [3]uint64{r.Fields[0], g.source(r.Fields[1]), r.Fields[2]})
		case trace.OutRecovered:
			g.recovering[n] = slices.DeleteFunc(g.recovering[n], func(x [3]uint64) bool { return x[0] == r.Fields[0] && x[2] == r.Fields[3] })
			f, s := g.chunkField(r.Fields[0]), g.source(r.Fields[2])
			at := len(g.lines)
			if r.Fields[3] == g.activated[f] {
				at = g.recoveredAt(f, n, s)
			}
			g.lineAt(at, `k |-> "recovered", n |-> %d, l |-> %d, c |-> %d, v |-> %s, s |-> %d, iv |-> %d`, n, g.primary[f], g.chunk(r.Fields[0]), ver(r.Fields[1]), s, r.Fields[3])
		case trace.OutReported:
			g.line(`k |-> "reported", n |-> %d, p |-> %d, iv |-> %d`, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.OutAsk:
			if n > g.nodes {
				g.operatorAsk[r.Fields[0]] = true
				break
			}
			g.line(`k |-> "ask", n |-> %d, p |-> %d, iv |-> %d`, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.OutFormat:
			old, ok := g.nodeDisk[n]
			if !ok {
				old = uint64(n)
			}
			g.formatAt[old] = len(g.lines)
			g.fmts++
			g.diskIDs[r.Fields[0]] = uint64(g.nodes + g.fmts)
			g.nodeDisk[n] = g.diskIDs[r.Fields[0]]
			g.line(`k |-> "format", n |-> %d`, n)
		case trace.OutCommitted:
			g.line(`k |-> "committed", c |-> %d, v |-> %s`, g.chunk(r.Fields[0]), ver(r.Fields[1]))
		case trace.OutReply:
			key := [3]uint64{uint64(n), r.Fields[0], r.Fields[1]}
			if g.replies[key] == nil {
				g.replies[key] = map[uint64]*reply{}
			}
			g.cur = &reply{decided: r.Fields[3], chunks: map[uint64][3]uint64{}}
			g.replies[key][r.Fields[2]] = g.cur
		case trace.OutReplyChunk:
			g.cur.chunks[r.Fields[1]] = [3]uint64{r.Fields[2], r.Fields[3], r.Fields[4]}
		case trace.OutDecide:
			kept := make([]string, len(g.chunks))
			for j := range kept {
				kept[j] = "0"
			}
			for ; i < len(recs) && recs[i].Name == trace.OutKept && recs[i].Node == r.Node; i++ {
				kept[g.chunk(recs[i].Fields[0])-1] = ver(recs[i].Fields[1])
			}
			g.decide(n, r, kept)
		case trace.OutPersisted:
			key := [3]uint64{r.Fields[2], r.Fields[0], r.Fields[1]}
			g.line(`k |-> "persisted", d |-> %d, p |-> %d, iv |-> %d, again |-> %s`, g.disk(r.Fields[2]), g.pg(r.Fields[0]), r.Fields[1], strings.ToUpper(strconv.FormatBool(g.lists[key])))
			g.lists[key] = true
		case trace.OutActivate:
			g.primary[r.Fields[0]] = n
			g.activated[r.Fields[0]] = r.Fields[1]
			for k := range g.left {
				if k[1] == r.Fields[0] {
					delete(g.left, k)
				}
			}
			g.line(`k |-> "activate", n |-> %d, p |-> %d, iv |-> %d`, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.EvCrash:
			for _, f := range g.pgs {
				if g.primary[f] == n {
					g.leave(n, f)
				}
			}
			g.crash(n)
		default:
			if !slices.Contains(belowProtocol, r.Name) {
				fatal("record %v at node %d has no mapping", r.Name, n)
			}
		}
	}
}

// belowProtocol are the records with no spec action, the machine's steps below the protocol's and the other records of a run.
var belowProtocol = []trace.Name{
	trace.EvNetRecv, trace.EvPeerLost, trace.EvDiskDone, trace.EvTimer, trace.EvResumed, trace.EvRestart, trace.EvDrop, trace.EvPause, trace.EvResume,
	trace.ActNetSend, trace.ActDiskRead, trace.ActDiskWrite, trace.ActDiskFsync, trace.ActDiskTrim, trace.ActSetTimer, trace.ActEntry,
	trace.SysEpollWait, trace.SysRead, trace.SysWritev, trace.SysIOUringEnter, trace.SysTimerfdSettime, trace.SysEventfdWrite,
	trace.OutDurable, trace.OutFailed, trace.OutCheckpointDone,
}

func (g *gen) decide(n int, r *trace.Record, kept []string) {
	got := g.replies[[3]uint64{uint64(n), r.Fields[0], r.Fields[1]}]
	var ds []uint64
	for d := range got {
		ds = append(ds, d)
	}
	slices.Sort(ds)
	var rep, j []string
	at := len(g.lines)
	for _, d := range ds {
		j = append(j, strconv.FormatUint(g.disk(d), 10))
		if p, ok := g.formatAt[g.disk(d)]; ok {
			at = min(at, p)
		}
		x := got[d]
		for _, h := range g.chunks {
			cv, ok := x.chunks[h]
			if !ok && g.chunkPG[g.chunk(h)-1] != g.pg(r.Fields[0]) {
				continue
			}
			list := "-1"
			if cv[2] != 0 {
				list = ver(cv[2] - 1)
			}
			rep = append(rep, fmt.Sprintf("[d |-> %d, c |-> %d, whole |-> %s, logged |-> %s, list |-> %s, decided |-> %d]", g.disk(d), g.chunk(h), ver(cv[0]), ver(cv[1]), list, x.decided))
		}
	}
	g.lineAt(at, `k |-> "peer", n |-> %d, p |-> %d, iv |-> %d, j |-> {%s}, kept |-> <<%s>>, rep |-> {%s}`,
		n, g.pg(r.Fields[0]), r.Fields[1], strings.Join(j, ", "), strings.Join(kept, ", "), strings.Join(rep, ", "))
}

func (g *gen) write(w *bufio.Writer, module string) {
	fmt.Fprintf(w, "---- MODULE %s ----\n", module)
	fmt.Fprintln(w, "EXTENDS Integers")
	nodes := g.nodes // the highest sim node is the client, numbered from 0
	fmt.Fprintf(w, "TNode == 1..%d\n", nodes)
	fmt.Fprintf(w, "TPG == 1..%d\n", len(g.pgs))
	fmt.Fprintf(w, "TChunk == 1..%d\n", max(len(g.chunks), 1))
	var a []string
	if len(g.chunks) == 0 {
		a = []string{"1"}
	}
	for _, p := range g.chunkPG {
		a = append(a, strconv.Itoa(p))
	}
	fmt.Fprintf(w, "TChunkPG == <<%s>>\n", strings.Join(a, ", "))
	a = nil
	if len(g.chunks) == 0 {
		a = []string{"1"}
	}
	for _, b := range g.blocks {
		a = append(a, strconv.Itoa(b))
	}
	fmt.Fprintf(w, "TBlocks == <<%s>>\n", strings.Join(a, ", "))
	var mem, ml []string
	for _, f := range g.pgs {
		e := g.initial[f]
		var d []string
		for _, m := range e.members {
			d = append(d, strconv.FormatUint(m.disk, 10))
		}
		mem = append(mem, "<<"+strings.Join(d, ", ")+">>")
		ml = append(ml, strconv.FormatUint(e.minLive, 10))
	}
	fmt.Fprintf(w, "TInitMem == <<%s>>\n", strings.Join(mem, ", "))
	fmt.Fprintf(w, "TMinLive == <<%s>>\n", strings.Join(ml, ", "))
	fmt.Fprintf(w, "TPlan == <<%s>>\n", strings.Join(g.plan, ",\n  "))
	var ws []string
	for _, x := range g.writes {
		ws = append(ws, fmt.Sprintf("[c |-> %d, blk |-> %d, whole |-> %s, inc |-> %d, re |-> 0, after |-> 0]", x.chunk, x.block, strings.ToUpper(strconv.FormatBool(x.whole)), x.inc))
	}
	fmt.Fprintf(w, "TWrites == <<%s>>\n", strings.Join(ws, ",\n  "))
	fmt.Fprintf(w, "TTables == %d\n", g.tables)
	fmt.Fprintf(w, "Trace == <<\n  %s\n>>\n", strings.Join(g.lines, ",\n  "))
	fmt.Fprintln(w, "====")
}

func main() {
	module := flag.String("module", "trace_data", "module name")
	flag.Parse()
	if flag.NArg() != 1 {
		fatal("usage: tracedata [-module name] run.trace")
	}
	g := &gen{entries: map[uint64]*entry{}, initial: map[uint64]entry{}, wid: map[uint64]int{}, epochs: map[uint64]int{}, replies: map[[3]uint64]map[uint64]*reply{},
		lists: map[[3]uint64]bool{}, operatorAsk: map[uint64]bool{}, diskIDs: map[uint64]uint64{}, primary: map[uint64]int{}, ownOpen: map[int][][2]uint64{}, memberOpen: map[int][][2]uint64{},
		activated: map[uint64]uint64{}, tableIv: map[uint64]map[uint64]uint64{}, left: map[[2]uint64]int{}, formatAt: map[uint64]int{}, nodeDisk: map[int]uint64{}, recovering: map[int][][3]uint64{}}
	recs := g.load(flag.Arg(0))
	if len(recs) == 0 {
		fatal("no table in the trace")
	}
	g.run(recs)
	if !slices.ContainsFunc(g.lines, func(l string) bool {
		return !strings.HasPrefix(l, `[k |-> "table"`) && !strings.HasPrefix(l, `[k |-> "plan"`) && !strings.HasPrefix(l, `[k |-> "fold"`) && !strings.HasPrefix(l, `[k |-> "renew"`)
	}) {
		fatal("the trace has no protocol step")
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	g.write(w, *module)
}
