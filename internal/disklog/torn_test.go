package disklog

import (
	"bytes"
	"math/bits"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func TestTornBlocksSeed4(t *testing.T) {
	r := core.NewRand(4)
	kinds := []string{"zeros", "random", "record"}
	cases := 0
	for i := range 40 {
		rec := Record{Place: testPlace, LSN: uint64(i + 1), Durable: r.Below(uint64(i + 1))}
		tr := randomRecord(t, r, rec, 9)
		n := int(tr.layout.Blocks())
		if tr.layout.HeaderBlocks != 1 || n > 10 {
			t.Fatalf("record %d spans %d blocks", i, n)
		}
		for kind, name := range kinds {
			var old testRecord
			switch name {
			case "zeros":
				old.image = make([]byte, len(tr.image))
			case "random":
				old.image = randomBytes(r, len(tr.image))
			case "record":
				oldRec := Record{Place: testPlace, LSN: rec.LSN + 1000, Durable: rec.LSN + 999}
				if n == 1 {
					old = encodeRecord(t, oldRec, []Entry{PunchEntry(1, testChunk, 1, 0, 1)}, [][]byte{nil})
				} else {
					old = encodeRecord(t, oldRec, []Entry{WriteEntry(1, testChunk, 1, 0, uint16(n-1))}, [][]byte{randomBytes(r, (n-1)*core.BlockSize)})
				}
			}
			if len(old.image) != len(tr.image) {
				t.Fatalf("old content of %d bytes for a %d-byte record", len(old.image), len(tr.image))
			}
			block := func(img []byte, b int) []byte { return img[b*core.BlockSize : (b+1)*core.BlockSize] }
			same := make([]bool, n)
			for b := range n {
				same[b] = bytes.Equal(block(old.image, b), block(tr.image, b))
			}
			img := bytes.Clone(old.image)
			landed := make([]bool, n)
			check := func() {
				isNew, isOld := true, true
				for b := range n {
					isNew = isNew && (landed[b] || same[b])
					isOld = isOld && (!landed[b] || same[b])
				}
				d, err := DecodeRecord(img, testPlace, uint32(n), nil)
				switch {
				case isNew:
					if err != nil || !sameRecord(d, tr) {
						t.Fatalf("record %d, %s old, landed %v: fully landed record gives %v", i, name, landed, err)
					}
				case isOld && kind == 2:
					if err != nil || !sameRecord(d, old) {
						t.Fatalf("record %d, %s old, landed %v: untouched old record gives %v", i, name, landed, err)
					}
				default:
					if err == nil {
						t.Fatalf("record %d, %s old, landed %v: torn image decodes as LSN %d", i, name, landed, d.Record.LSN)
					}
				}
				cases++
			}
			check()
			for mask := uint(1); mask < 1<<n; mask++ {
				b := bits.TrailingZeros(mask)
				landed[b] = !landed[b]
				src := old.image
				if landed[b] {
					src = tr.image
				}
				copy(block(img, b), block(src, b))
				check()
			}
		}
	}
	if cases <= 1000 {
		t.Fatalf("only %d cases", cases)
	}
}

func TestTornSectorsSeed5(t *testing.T) {
	const sector = 512
	r := core.NewRand(5)
	errs := 0
	for i := range 300 {
		rec := Record{Place: testPlace, LSN: uint64(i + 1), Durable: r.Below(uint64(i + 1))}
		tr := randomRecord(t, r, rec, 8)
		img := randomBytes(r, len(tr.image))
		all := true
		for s := 0; s < len(img); s += sector {
			if r.Chance(1, 2) {
				copy(img[s:s+sector], tr.image[s:s+sector])
			} else {
				all = false
			}
		}
		d, err := DecodeRecord(img, testPlace, manyLeft, nil)
		switch {
		case all && (err != nil || !sameRecord(d, tr)):
			t.Fatalf("record %d: every sector landed but decode gives %v", i, err)
		case !all && err == nil:
			t.Fatalf("record %d: a sector did not land but decode gives LSN %d", i, d.Record.LSN)
		}
		if err != nil {
			errs++
		}
	}
	if errs == 0 {
		t.Fatal("no record was torn")
	}
}
