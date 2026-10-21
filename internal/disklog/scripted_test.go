package disklog_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// scriptedRecovery runs one case of testdata/recovery on a freshly formatted 1 GiB disk and
// returns what its commands print.
func scriptedRecovery(t *testing.T, d *datadriven.TestData) string {
	cfg := testConfig(0)
	for _, a := range d.CmdArgs {
		switch {
		case a.Key == "depth" && len(a.Vals) == 1:
			n, err := strconv.Atoi(a.Vals[0])
			if err != nil {
				d.Fatalf(t, "depth: %v", err)
			}
			cfg.CommitDepth = n
		default:
			d.Fatalf(t, "unknown argument %q", a.String())
		}
	}
	h := formatted(t, gib, cfg)
	var b strings.Builder
	var last *recovered
	for _, line := range strings.Split(strings.TrimSpace(d.Input), "\n") {
		f := strings.Fields(line)
		nums := func(from, n int) []uint64 {
			if n >= 0 && len(f) != from+n {
				d.Fatalf(t, "%q: %s takes %d arguments", line, f[0], from-1+n)
			}
			var out []uint64
			for _, s := range f[from:] {
				v, err := strconv.ParseUint(s, 0, 64)
				if err != nil {
					d.Fatalf(t, "%q: %v", line, err)
				}
				out = append(out, v)
			}
			return out
		}
		stream := func() disklog.Stream {
			if len(f) < 2 {
				d.Fatalf(t, "%q: %s needs a stream", line, f[0])
			}
			switch f[1] {
			case "hot":
				return disklog.Hot
			case "cold":
				return disklog.Cold
			}
			d.Fatalf(t, "%q: unknown stream %q", line, f[1])
			return 0
		}
		switch f[0] {
		case "write", "submit":
			s := stream()
			v := nums(2, 4)
			h.trace = nil
			h.submit(s, h.chunkWrite(uint32(v[0]), uint16(v[1]), uint16(v[2]), v[3]))
			if f[0] == "write" {
				h.run()
			} else {
				fmt.Fprintf(&b, "%s: %s\n", line, strings.Join(h.trace, ", "))
			}
			h.trace = nil
		case "exec":
			if len(f) != 2 {
				d.Fatalf(t, "%q: exec takes an action's IO", line)
			}
			io, err := strconv.ParseUint(strings.TrimPrefix(f[1], "io"), 10, 64)
			i := slices.IndexFunc(h.pending, func(p pendingIO) bool { return uint64(p.act.IO) == io })
			if err != nil || i < 0 {
				d.Fatalf(t, "%q: no pending action %s", line, f[1])
			}
			h.step(i)
			h.trace = nil
		case "crash":
			lbas := nums(1, -1)
			h.disk.Crash(func(_ int, lba core.LBA) bool { return slices.Contains(lbas, uint64(lba)) })
		case "xor":
			v := nums(1, 2)
			xorBlock(h, core.LBA(v[0]), byte(v[1]))
		case "flip":
			v := nums(1, 3)
			h.disk.FlipBit(core.LBA(v[0]), int(v[1]), byte(v[2]))
		case "zeros":
			v := nums(1, 1)
			fmt.Fprintf(&b, "%s: %v\n", line, isZeroBlock(h.read(core.LBA(v[0]), 1)))
		case "recover":
			mode := disklog.ModeOpen
			switch {
			case len(f) == 2 && f[1] == "open":
			case len(f) == 2 && f[1] == "rebuild":
				mode = disklog.ModeRebuild
			default:
				d.Fatalf(t, "%q: recover open or rebuild", line)
			}
			last = h.recover(mode)
			fmt.Fprintf(&b, "%s: %s\n", line, recoveryResult(last))
		case "index":
			if len(f) != 1 || last == nil || last.err != nil {
				d.Fatalf(t, "%q: index takes no arguments and follows a successful recover", line)
			}
			b.WriteString(indexResult(h, last.index))
		default:
			d.Fatalf(t, "unknown command %q", line)
		}
	}
	return b.String()
}

// recoveryResult prints a recovery's LSNs, as a range when they are contiguous, its error, and the
// headers it erased and segments it quarantined.
func recoveryResult(res *recovered) string {
	var ce *disklog.CorruptError
	switch {
	case errors.As(res.err, &ce):
		return fmt.Sprintf("Corrupt(LSN %d)", ce.LSN)
	case res.err != nil:
		return res.err.Error()
	}
	lsns := res.lsns()
	var out string
	switch {
	case len(lsns) == 0:
		out = "no records"
	case slices.Equal(lsns, lsnRange(lsns[0], lsns[len(lsns)-1])):
		out = fmt.Sprintf("LSNs %d-%d", lsns[0], lsns[len(lsns)-1])
	default:
		out = fmt.Sprintf("LSNs %v", lsns)
	}
	out += fmt.Sprintf(", durable %d", res.durable)
	if res.erased > 0 {
		out += fmt.Sprintf(", headers erased: %d", res.erased)
	}
	if len(res.quarantined) > 0 {
		out += fmt.Sprintf(", segments %v quarantined", res.quarantined)
	}
	return out
}

// indexResult prints a recovered index as extents, each a run of blocks of one chunk version at
// consecutive LBAs, and whether every block reads back the data its version wrote.
func indexResult(h *harness, m model) string {
	var lines []string
	status := ""
	keys := m.keys()
	for i := 0; i < len(keys); {
		k, v := keys[i], m[keys[i]]
		n := 1
		for ; i+n < len(keys); n++ {
			nk, nv := keys[i+n], m[keys[i+n]]
			if nk.chunk != k.chunk || nk.block != k.block+uint16(n) || nv.version != v.version || nv.lba != v.lba+core.LBA(n) {
				break
			}
		}
		for j := range n {
			kj, vj := keys[i+j], m[keys[i+j]]
			if status == "" && !bytes.Equal(h.read(vj.lba, 1), blockData(kj.chunk, vj.version, kj.block)) {
				status = fmt.Sprintf("chunk %d block %d does not read back", kj.chunk.Index, kj.block)
			}
		}
		blocks, lbas := fmt.Sprintf("block %d", k.block), fmt.Sprintf("%d", v.lba)
		if n > 1 {
			blocks = fmt.Sprintf("blocks %d-%d", k.block, k.block+uint16(n-1))
			lbas = fmt.Sprintf("%d-%d", v.lba, v.lba+core.LBA(n-1))
		}
		lines = append(lines, fmt.Sprintf("  chunk %d %s v%d at %s\n", k.chunk.Index, blocks, v.version, lbas))
		i += n
	}
	if status == "" {
		status = "every block reads back"
	}
	return fmt.Sprintf("index: %d blocks, %s\n", len(keys), status) + strings.Join(lines, "")
}

// Recovery from disk images that testdata/recovery's commands build: write (submit and complete),
// submit (only queue, printing the actions), exec ioN, crash landing the listed unflushed blocks,
// xor a block with a mask, flip bits of a byte, zeros, recover open or rebuild, and index, the
// last recovery's index read back.
func TestScriptedRecovery(t *testing.T) {
	datadriven.RunTest(t, "testdata/recovery", func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "run" {
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return scriptedRecovery(t, d)
	})
}
