package main

import (
	"fmt"
	"math"
)

// mrun is a sorted run in the size model: the fraction of the key space it covers. Under uniform
// updates a flush of n entries over K keys covers 1-exp(-n/K), and a merge covers
// 1-prod(1-c_i), the union of its inputs.
type mrun struct {
	cover   float64
	pending bool
}

// picker returns the next merge over runs (newest first) as [i, j), or ok=false.
type picker interface {
	name() string
	pick(sizes []float64) (i, j int, ok bool)
}

// bucketPicker is internal/lsm's rule (compact.go): Cassandra's buckets over runs adjacent in
// age, 4 to 32 runs, runs under 1 MiB bucket together, smallest average first. With fallback set,
// when no bucket is due and the run count passes the cap, it takes universal's size-ratio pick
// and then universal's read-amp fallback (merge the newest runs-cap+1).
type bucketPicker struct {
	fallback bool
	capFixed int
}

func (p bucketPicker) name() string {
	if !p.fallback {
		return "bucket"
	}
	if p.capFixed > 0 {
		return fmt.Sprintf("bucket+fallback(cap %d)", p.capFixed)
	}
	return "bucket+fallback(cap formula)"
}

const (
	minMerge    = 4
	maxMerge    = 32
	minRunBytes = 1 << 20
)

func similar(size, avg, small float64) bool {
	return (2*size > avg && 2*size < 3*avg) || (size < small && avg < small)
}

func (p bucketPicker) pick(sizes []float64) (int, int, bool) {
	best, bestAvg := [2]int{}, -1.0
	for i := 0; i < len(sizes); {
		j, sum := i+1, sizes[i]
		for j < len(sizes) && j-i < maxMerge && similar(sizes[j], sum/float64(j-i), minRunBytes) {
			sum += sizes[j]
			j++
		}
		if n := j - i; n >= minMerge {
			if avg := sum / float64(n); bestAvg < 0 || avg < bestAvg {
				best, bestAvg = [2]int{i, j}, avg
			}
		}
		i = j
	}
	if bestAvg >= 0 {
		return best[0], best[1], true
	}
	if !p.fallback {
		return 0, 0, false
	}
	limit := p.capFixed
	if limit == 0 {
		total := 0.0
		for _, s := range sizes {
			total += s
		}
		limit = 3*int(math.Ceil(math.Log(math.Max(total/minRunBytes, 1))/math.Log(4))) + 3
	}
	if len(sizes) <= limit {
		return 0, 0, false
	}
	if i, j, ok := sizeRatio(sizes, 1, 2, math.MaxInt); ok {
		return i, j, true
	}
	return 0, len(sizes) - limit + 1, true
}

// universalPicker ports RocksDB's UniversalCompactionBuilder::PickCompaction
// (db/compaction/compaction_picker_universal.cc): nothing below trigger sorted runs; then size
// amplification (all runs but the oldest against the oldest), then the size-ratio rule from the
// newest run, then the read-amp fallback that merges the newest runs-maxRuns+1 whatever their
// sizes. maxReadAmp -1 makes maxRuns the trigger; 0 auto-tunes it from the largest run, the size
// ratio and writeBuffer.
type universalPicker struct {
	trigger     int
	ratio       float64
	minWidth    int
	maxWidth    int
	maxSizeAmp  float64
	maxReadAmp  int
	writeBuffer float64
}

func (p universalPicker) name() string {
	s := fmt.Sprintf("universal(trigger %d", p.trigger)
	if p.maxReadAmp == 0 {
		s += fmt.Sprintf(", max_read_amp auto, buffer %.3g MiB", p.writeBuffer/(1<<20))
	}
	if p.maxSizeAmp > 1e9 {
		s += ", no size amp"
	}
	return s + ")"
}

func sizeRatio(sizes []float64, ratio float64, minWidth, maxFiles int) (int, int, bool) {
	minWidth = max(minWidth, 2)
	for loop := range sizes {
		cnt, sz := 1, sizes[loop]
		for i := loop + 1; cnt < maxFiles && i < len(sizes); i++ {
			if sz*(100+ratio)/100 < sizes[i] {
				break
			}
			sz += sizes[i]
			cnt++
		}
		if cnt >= minWidth {
			return loop, loop + cnt, true
		}
	}
	return 0, 0, false
}

func (p universalPicker) pick(sizes []float64) (int, int, bool) {
	n := len(sizes)
	if n == 0 || n < p.trigger {
		return 0, 0, false
	}
	base, cand := sizes[n-1], 0.0
	for _, s := range sizes[:n-1] {
		cand += s
	}
	if n > 1 && cand*100 >= p.maxSizeAmp*base {
		return 0, n, true
	}
	if i, j, ok := sizeRatio(sizes, p.ratio, p.minWidth, p.maxWidth); ok {
		return i, j, true
	}
	maxRuns := p.trigger
	if p.maxReadAmp == 0 {
		largest := 0.0
		for _, s := range sizes {
			largest = max(largest, s)
		}
		maxRuns = 1
		cur, total := p.writeBuffer, 0.0
		for cur < largest {
			total += cur
			cur = (100 + p.ratio) / 100 * total
			maxRuns++
		}
	}
	if n > maxRuns {
		if i, j, ok := sizeRatio(sizes, math.Inf(1), p.minWidth, min(p.maxWidth, n-maxRuns+1)); ok {
			return i, j, true
		}
	}
	return 0, 0, false
}

// result is what one policy did on one workload; run counts and space are sampled once a
// flush's compactions settle, over the second half of the sequence.
type result struct {
	policy, workload     string
	flushes              int
	runsMean             float64
	runsMax, runsMaxEver int
	flushedB, compactedB float64
	mergedB              float64
	spaceMean, spaceMax  float64
	peakSpace, maxOutput float64
	compactions, inputs  int
	liveB                float64
}

func (r result) wa() float64 { return (r.flushedB + r.compactedB) / r.flushedB }

func simulate(w workload, p picker) result {
	K := float64(w.keys)
	live := K * w.entryB
	size := func(r mrun) float64 { return r.cover * live }
	runs := []mrun{{cover: 1}}
	res := result{policy: p.name(), workload: w.name, liveB: live}
	half := len(w.flushes) / 2
	samples := 0
	for fi, n := range w.flushes {
		measure := fi >= half
		r := mrun{cover: 1 - math.Exp(-float64(n)/K)}
		runs = append([]mrun{r}, runs...)
		if measure {
			res.flushedB += size(r)
			res.flushes++
		}
		res.runsMaxEver = max(res.runsMaxEver, len(runs))
		for {
			sizes := make([]float64, len(runs))
			total := 0.0
			for i, r := range runs {
				sizes[i] = size(r)
				total += sizes[i]
			}
			i, j, ok := p.pick(sizes)
			if !ok || j-i < 2 {
				break
			}
			keep := 1.0
			for _, r := range runs[i:j] {
				keep *= 1 - r.cover
			}
			out := mrun{cover: 1 - keep, pending: true}
			if measure {
				for _, r := range runs[i:j] {
					if r.pending {
						res.compactedB -= size(r)
					}
				}
				res.compactedB += size(out)
				res.mergedB += size(out)
				res.compactions++
				res.inputs += j - i
				res.peakSpace = max(res.peakSpace, (total+size(out))/live)
				res.maxOutput = max(res.maxOutput, size(out)/live)
			}
			runs = append(append(append([]mrun{}, runs[:i]...), out), runs[j:]...)
		}
		for k := range runs {
			runs[k].pending = false
		}
		if measure {
			total := 0.0
			for _, r := range runs {
				total += size(r)
			}
			samples++
			res.runsMean += float64(len(runs))
			res.runsMax = max(res.runsMax, len(runs))
			res.spaceMean += total / live
			res.spaceMax = max(res.spaceMax, total/live)
		}
		if len(runs) > 100000 {
			break
		}
	}
	res.runsMean /= float64(samples)
	res.spaceMean /= float64(samples)
	return res
}

func policies(smallB float64) []picker {
	ps := []picker{
		bucketPicker{},
		bucketPicker{fallback: true},
		bucketPicker{fallback: true, capFixed: 8},
		universalPicker{trigger: 4, ratio: 1, minWidth: 2, maxWidth: math.MaxInt, maxSizeAmp: 200, maxReadAmp: -1},
		universalPicker{trigger: 8, ratio: 1, minWidth: 2, maxWidth: math.MaxInt, maxSizeAmp: 200, maxReadAmp: -1},
		universalPicker{trigger: 4, ratio: 1, minWidth: 2, maxWidth: math.MaxInt, maxSizeAmp: 200, maxReadAmp: 0, writeBuffer: smallB},
		universalPicker{trigger: 4, ratio: 1, minWidth: 2, maxWidth: math.MaxInt, maxSizeAmp: math.Inf(1), maxReadAmp: -1},
	}
	return append(ps, universalPicker{trigger: 4, ratio: 1, minWidth: 2, maxWidth: math.MaxInt, maxSizeAmp: 200, maxReadAmp: 0, writeBuffer: minRunBytes})
}

func printHeader() {
	fmt.Printf("%-12s %-52s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s\n", "workload", "policy",
		"runsMean", "runsMax", "WA", "cmp/fl", "spaceAvg", "spaceMax", "peak", "maxOut", "inputs", "merge/fl")
}

func (r result) print() {
	in := 0.0
	if r.compactions > 0 {
		in = float64(r.inputs) / float64(r.compactions)
	}
	fmt.Printf("%-12s %-52s %8.2f %8d %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.1f %8.2f\n", r.workload, r.policy,
		r.runsMean, r.runsMax, r.wa(), r.compactedB/r.flushedB, r.spaceMean, r.spaceMax, r.peakSpace, r.maxOutput, in, r.mergedB/r.flushedB)
}
