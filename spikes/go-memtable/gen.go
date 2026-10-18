package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
)

// The file model of the MetaStore memory spike (ADR 0004 keys): an attribute record per inode, a
// directory entry per name and a slot counter per directory; 20 to 50 files per directory, up to 4
// subdirectories each, and directory 1 holding the 996 files and 4 subdirectories that M4 lists.
const (
	volume      = 1
	tagFS       = 0x02
	tagAttr     = 'a'
	tagDirent   = 'd'
	tagSlot     = 'n'
	typeReg     = 1
	typeDir     = 2
	attrLen     = 92
	bigDir      = 1
	bigDirFiles = 996
	nameChars   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._"
	probeEvery  = 7
)

func ino(slot int, seq uint64) uint64 { return uint64(slot)<<40 | seq }

func key(inode uint64, tag byte, last []byte) []byte {
	k := make([]byte, 0, 14+len(last))
	k = append(k, tagFS)
	k = binary.BigEndian.AppendUint32(k, volume)
	k = binary.BigEndian.AppendUint64(k, inode)
	k = append(k, tag)
	return append(k, last...)
}

func direntPrefix(slot int) []byte { return key(ino(slot, 1), tagDirent, nil) }

func attrValue(r *rand.Rand, dir bool, nlink uint32) []byte {
	v := make([]byte, 0, attrLen)
	mode, size := uint32(0o100644), uint64(r.IntN(1<<24))
	if dir {
		mode, size = 0o040755, 0
	}
	for _, x := range []uint32{mode, 1000 + uint32(r.IntN(4)), 1000, nlink, 0} {
		v = binary.BigEndian.AppendUint32(v, x)
	}
	v = binary.BigEndian.AppendUint64(v, size)
	v = binary.BigEndian.AppendUint64(v, (size+4095)&^4095)
	for range 4 {
		v = binary.BigEndian.AppendUint64(v, uint64(1_700_000_000+r.IntN(1<<26)))
		v = binary.BigEndian.AppendUint32(v, uint32(r.IntN(1_000_000_000)))
	}
	return binary.BigEndian.AppendUint64(v, uint64(r.IntN(1<<20)))
}

type nameHist struct {
	lens []int
	cum  []uint64
}

func loadNames(path string) nameHist {
	data, err := os.ReadFile(path)
	check(err)
	var h nameHist
	var acc uint64
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 || strings.HasPrefix(line, "#") {
			continue
		}
		l, _ := strconv.Atoi(f[0])
		c, _ := strconv.ParseUint(f[1], 10, 64)
		acc += c
		h.lens = append(h.lens, l)
		h.cum = append(h.cum, acc)
	}
	return h
}

func (h nameHist) draw(r *rand.Rand) int {
	x := r.Uint64N(h.cum[len(h.cum)-1])
	return h.lens[sort.Search(len(h.cum), func(i int) bool { return h.cum[i] > x })]
}

// generate writes every entry in create order as `key_len u16 | value_len u16 | key | value`, and
// every probeEvery-th key to the probe file.
func generate(files int, names nameHist, dataPath, probePath string) {
	r := rand.New(rand.NewPCG(1, 2))
	plan := []int{}
	for total := 0; total < files; {
		n := 20 + r.IntN(31)
		if len(plan) == bigDir {
			n = bigDirFiles
		}
		n = min(n, files-total)
		plan = append(plan, n)
		total += n
	}
	df, err := os.Create(dataPath)
	check(err)
	pf, err := os.Create(probePath)
	check(err)
	dw, pw := bufio.NewWriterSize(df, 1<<20), bufio.NewWriterSize(pf, 1<<20)
	var entries, bytes int
	emit := func(k, v []byte) {
		var hdr [4]byte
		binary.LittleEndian.PutUint16(hdr[:2], uint16(len(k)))
		binary.LittleEndian.PutUint16(hdr[2:], uint16(len(v)))
		dw.Write(hdr[:])
		dw.Write(k)
		dw.Write(v)
		if entries%probeEvery == 0 {
			pw.Write(hdr[:2])
			pw.Write(k)
		}
		entries++
		bytes += len(k) + len(v)
	}
	d := len(plan)
	subdirs := func(j int) (int, int) { return min(4*j+1, d), min(4*j+5, d) }
	lo, hi := subdirs(0)
	emit(key(ino(0, 1), tagAttr, nil), attrValue(r, true, uint32(2+hi-lo)))
	seen := map[string]struct{}{}
	name := func() []byte {
		n := max(names.draw(r), 1)
		for attempt := 1; ; attempt++ {
			b := make([]byte, n)
			for i := range b {
				b[i] = nameChars[r.IntN(64)]
			}
			if _, dup := seen[string(b)]; !dup {
				seen[string(b)] = struct{}{}
				return b
			}
			if attempt%64 == 0 {
				n++
			}
		}
	}
	for j := range d {
		dir := ino(j, 1)
		clear(seen)
		for k := range plan[j] {
			f := ino(j, uint64(2+k))
			v := binary.BigEndian.AppendUint64(make([]byte, 0, 9), f)
			emit(key(dir, tagDirent, name()), append(v, typeReg))
			emit(key(f, tagAttr, nil), attrValue(r, false, 1))
		}
		lo, hi := subdirs(j)
		for c := lo; c < hi; c++ {
			ci := ino(c, 1)
			v := binary.BigEndian.AppendUint64(make([]byte, 0, 9), ci)
			emit(key(dir, tagDirent, name()), append(v, typeDir))
			clo, chi := subdirs(c)
			emit(key(ci, tagAttr, nil), attrValue(r, true, uint32(2+chi-clo)))
		}
		v := binary.BigEndian.AppendUint64(make([]byte, 0, 16), uint64(2+plan[j]))
		emit(key(ino(j, 0), tagSlot, nil), binary.BigEndian.AppendUint64(v, r.Uint64N(1<<40)))
	}
	check(dw.Flush())
	check(pw.Flush())
	check(df.Close())
	check(pf.Close())
	fmt.Printf("generated files=%d dirs=%d entries=%d key+value bytes=%d (%.1f per file)\n", files, d, entries, bytes,
		float64(bytes)/float64(files))
}

// forEach streams the data file's entries; k and v are only valid during the call.
func forEach(path string, fn func(k, v []byte)) {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	buf := make([]byte, 1<<16)
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		kl, vl := int(binary.LittleEndian.Uint16(hdr[:2])), int(binary.LittleEndian.Uint16(hdr[2:]))
		_, err := io.ReadFull(r, buf[:kl+vl])
		check(err)
		fn(buf[:kl], buf[kl:kl+vl])
	}
}

// probes holds sampled keys in one pointer-free buffer, so they add nothing for the collector to scan.
type probes struct {
	data []byte
	offs []uint32
}

func (p *probes) len() int { return len(p.offs) }

func (p *probes) key(i int) []byte {
	o := p.offs[i]
	n := uint32(binary.LittleEndian.Uint16(p.data[o:]))
	return p.data[o+2 : o+2+n]
}

func loadProbes(path string) *probes {
	data, err := os.ReadFile(path)
	check(err)
	p := &probes{data: data}
	for o := 0; o < len(data); o += 2 + int(binary.LittleEndian.Uint16(data[o:])) {
		p.offs = append(p.offs, uint32(o))
	}
	return p
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
