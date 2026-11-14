package meta

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

func ino(slot uint32, seq uint64) uint64 {
	i, err := MakeInode(slot, seq)
	if err != nil {
		panic(err)
	}
	return i
}

func fsKey(vol uint32, inode uint64, tag byte) Key {
	return Key{Space: SpaceFS, Volume: vol, Inode: inode, Tag: tag}
}

func with(k Key, f func(*Key)) Key { f(&k); return k }

// goldenKeys are the keys testdata/key_golden.txt freezes, by the name it gives them.
var goldenKeys = map[string]Key{
	"ctl/pool/fast":         {Space: SpaceCtl, Tag: CtlPool, Name: b("fast")},
	"ctl/placement":         {Space: SpaceCtl, Tag: CtlPlacement},
	"ctl/membership":        {Space: SpaceCtl, Tag: CtlMembership},
	"ctl/session/7":         {Space: SpaceCtl, Tag: CtlSession, ID: 7},
	"ctl/format":            {Space: SpaceCtl, Tag: CtlFormat},
	"ctl/shardmap":          {Space: SpaceCtl, Tag: CtlShardMap},
	"ctl/pool/5":            {Space: SpaceCtl, Tag: CtlPool, Name: []byte{0, 0, 0, 5}},
	"ctl/placement/5":       {Space: SpaceCtl, Tag: CtlPlacement, Name: []byte{0, 0, 0, 5}},
	"ctl/placement/5/9":     {Space: SpaceCtl, Tag: CtlPlacement, Name: []byte{0, 0, 0, 5, 0, 0, 0, 9}},
	"ctl/placement/5/9#1":   {Space: SpaceCtl, Tag: CtlPlacement, Name: []byte{0, 0, 0, 5, 0, 0, 0, 9, 1}},
	"ctl/shardmap/3":        {Space: SpaceCtl, Tag: CtlShardMap, Name: []byte{0, 0, 0, 0, 0, 0, 0, 3}},
	"ctl/shardmap/3#1":      {Space: SpaceCtl, Tag: CtlShardMap, Name: []byte{0, 0, 0, 0, 0, 0, 0, 3, 1}},
	"ctl/membership/4":      {Space: SpaceCtl, Tag: CtlMembership, Name: []byte{0, 0, 0, 4}},
	"volume/5/attach":       {Space: SpaceVolume, Volume: 5, Tag: VolumeAttach},
	"volume/5/snap/3":       {Space: SpaceVolume, Volume: 5, Tag: VolumeSnap, ID: 3},
	"fs/1/0:1/a":            fsKey(1, ino(0, 1), InodeAttr),
	"fs/5/2:1/a":            fsKey(5, ino(2, 1), InodeAttr),
	"fs/5/2:3/x/user.big":   with(fsKey(5, ino(2, 3), InodeXattr), func(k *Key) { k.Name = b("user.big") }),
	"fs/5/2:3/t":            fsKey(5, ino(2, 3), InodeTarget),
	"fs/5/2:3/l/s/7":        with(fsKey(5, ino(2, 3), InodeLock), func(k *Key) { k.Mode, k.Session = LockShared, 7 }),
	"fs/5/2:3/l/x/7":        with(fsKey(5, ino(2, 3), InodeLock), func(k *Key) { k.Mode, k.Session = LockExclusive, 7 }),
	"fs/5/2:1/d/beach.jpg":  with(fsKey(5, ino(2, 1), InodeEntry), func(k *Key) { k.Name = b("beach.jpg") }),
	"fs/5/2:1/c/beach.jpg":  with(fsKey(5, ino(2, 1), InodeFolded), func(k *Key) { k.Name = b("beach.jpg") }),
	"fs/5/2:1/P":            fsKey(5, ino(2, 1), InodeParent),
	"fs/5/2:0/n":            fsKey(5, ino(2, 0), SlotNext),
	"fs/5/2:0/u":            fsKey(5, ino(2, 0), SlotUsed),
	"fs/5/2:0/o/2:9":        with(fsKey(5, ino(2, 0), SlotOrphan), func(k *Key) { k.ID = ino(2, 9) }),
	"fs/5/2:0/p/2:9":        with(fsKey(5, ino(2, 0), SlotPurge), func(k *Key) { k.ID = ino(2, 9) }),
	"fs/5/2:0/r/7/1/42":     with(fsKey(5, ino(2, 0), SlotReplay), func(k *Key) { k.Session, k.Mount, k.ID = 7, 1, 42 }),
	"fs/5/2:0/f":            fsKey(5, ino(2, 0), SlotFree),
	"fs/5/2:3/x#2/user.big": with(fsKey(5, ino(2, 3), InodeXattr|PieceBit), func(k *Key) { k.Piece, k.Name = 2, b("user.big") }),
	"fs/5/2:3/t#1":          with(fsKey(5, ino(2, 3), InodeTarget|PieceBit), func(k *Key) { k.Piece = 1 }),
	"s3/5/photos/cat.jpg":   {Space: SpaceS3, Volume: 5, Name: b("photos/cat.jpg")},
	"fs/max/max:max/a":      fsKey(0xffffffff, ino(1<<SlotBits-1, 1<<SeqBits-1), InodeAttr),
}

// testdata/key_golden.txt is frozen disk format: the test reads it, never the reverse.
func TestKeyGoldenVectors(t *testing.T) {
	f, err := os.Open("testdata/key_golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, hx, ok := strings.Cut(line, " ")
		want, err := hex.DecodeString(strings.ReplaceAll(hx, " ", ""))
		k, known := goldenKeys[name]
		if !ok || err != nil || !known {
			t.Fatalf("golden line %q: no key of that name, or no hex", line)
		}
		seen[name] = true
		got, err := AppendKey(nil, &k)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s encodes to %x (%v), golden %x", name, got, err, want)
		}
		var d Key
		if err := DecodeKey(want, &d); err != nil || !reflect.DeepEqual(d, k) {
			t.Errorf("%s: golden %x decodes to %+v (%v)", name, want, d, err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	for name := range goldenKeys {
		if !seen[name] {
			t.Errorf("%s has no golden line", name)
		}
	}
	replay := goldenKeys["fs/5/2:0/r/7/1/42"]
	if k := mustKey(&replay); len(k) != 34 {
		t.Errorf("a replay key is %d bytes, want 34", len(k))
	}
}

// randKey draws a key of any kind, from few values per field, so pairs share prefixes and
// fields sit at their bounds.
func randKey(rng *rand.Rand) Key {
	pick := func(xs ...uint64) uint64 { return xs[rng.IntN(len(xs))] }
	n64 := func() uint64 { return pick(0, 1, 2, 255, 256, 1<<32-1, 1<<63, 1<<64-1) }
	str := func() []byte {
		s := make([]byte, 1+rng.IntN(3))
		for i := range s {
			s[i] = byte(pick(0x00, 0x01, 'a', 'b', 0x7f, 0x80, 0xff))
		}
		return s
	}
	var k Key
	k.Space = byte(1 + rng.IntN(4))
	switch k.Space {
	case SpaceCtl:
		k.Tag = byte(pick(uint64(CtlPool), uint64(CtlPlacement), uint64(CtlMembership), uint64(CtlSession), uint64(CtlFormat), uint64(CtlShardMap)))
	case SpaceVolume:
		k.Volume, k.Tag = uint32(n64()), byte(pick(uint64(VolumeAttach), uint64(VolumeSnap)))
	case SpaceFS:
		k.Volume = uint32(n64())
		k.Inode = ino(uint32(pick(0, 1, 2, 1<<SlotBits-1)), pick(0, 1, 2, 1<<SeqBits-1))
		if k.Inode&seqMask == 0 {
			k.Tag = byte(pick(0x10, 0x11, 0x12, 0x13, 0x14, 0x15))
		} else {
			k.Tag = byte(pick(1, 2, 3, 4, 5, 6, 7, 0x82, 0x83))
		}
		if k.Tag&PieceBit != 0 {
			k.Piece = byte(pick(1, 2, 255))
		}
	case SpaceS3:
		k.Volume = uint32(n64())
	}
	switch layoutOf(k.Space, k.Tag, k.Inode) {
	case name:
		k.Name = str()
	case rest:
		if rng.IntN(2) == 0 {
			k.Name = str()
		}
	case id:
		k.ID = n64()
	case lock:
		k.Mode, k.Session = byte(pick(uint64(LockShared), uint64(LockExclusive))), n64()
	case replay:
		k.Session, k.Mount, k.ID = n64(), uint32(n64()), n64()
	}
	return k
}

// compareLogical compares keys component by component, left to right: integers numerically,
// the last component bytewise.
func compareLogical(a, b *Key) int {
	parts := func(k *Key) []uint64 {
		p := []uint64{uint64(k.Space)}
		switch k.Space {
		case SpaceCtl:
			p = append(p, uint64(k.Tag))
		case SpaceVolume:
			p = append(p, uint64(k.Volume), uint64(k.Tag))
		case SpaceFS:
			p = append(p, uint64(k.Volume), k.Inode>>SeqBits, k.Inode&seqMask, uint64(k.Tag), uint64(k.Piece))
		case SpaceS3:
			p = append(p, uint64(k.Volume))
		}
		return append(p, uint64(k.Mode), k.Session, uint64(k.Mount), k.ID)
	}
	if c := slices.Compare(parts(a), parts(b)); c != 0 {
		return c
	}
	return bytes.Compare(a.Name, b.Name)
}

func TestKeyByteOrderIsLogicalOrder(t *testing.T) {
	seeds.Run(t, 1, 200, func(seed uint64) {
		rng := rand.New(rand.NewPCG(seed, 0))
		for range 200 {
			a, b := randKey(rng), randKey(rng)
			ea, err := AppendKey(nil, &a)
			if err != nil {
				t.Fatalf("%+v: %v", a, err)
			}
			eb := mustKey(&b)
			if got, want := bytes.Compare(ea, eb), compareLogical(&a, &b); got != want {
				t.Fatalf("%+v vs %+v: bytes compare %d, logical %d", a, b, got, want)
			}
			var d Key
			if err := DecodeKey(ea, &d); err != nil || !reflect.DeepEqual(d, a) {
				t.Fatalf("%+v encodes to %x, which decodes to %+v (%v)", a, ea, d, err)
			}
		}
	})
}

func TestKeyFixedOrder(t *testing.T) {
	entry := func(vol uint32, inode uint64, n string) Key {
		return with(fsKey(vol, inode, InodeEntry), func(k *Key) { k.Name = b(n) })
	}
	ordered := []Key{
		{Space: SpaceVolume, Volume: 255, Tag: VolumeAttach},
		{Space: SpaceVolume, Volume: 256, Tag: VolumeAttach},
		fsKey(5, ino(2, 0), SlotNext),
		fsKey(5, ino(2, 1), InodeAttr),
		entry(5, ino(2, 1), "\x01"),
		entry(5, ino(2, 1), "beach.jpg"),
		entry(5, ino(2, 1), "cat.jpg"),
		entry(5, ino(2, 1), "cat.jpg\xff"),
		fsKey(5, ino(2, 2), InodeAttr),
		fsKey(5, ino(3, 0), SlotNext),
		fsKey(6, ino(1, 0), SlotNext),
		{Space: SpaceS3, Volume: 5, Name: b("a")},
		{Space: SpaceS3, Volume: 5, Name: b("a\x00")},
		{Space: SpaceS3, Volume: 5, Name: b("a/")},
	}
	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(mustKey(&ordered[i-1]), mustKey(&ordered[i])) >= 0 {
			t.Errorf("%+v does not sort before %+v", ordered[i-1], ordered[i])
		}
	}
}

// A prefix scan of an inode's d/ returns its directory entries only, and a scan from its a
// through its x/ its attributes and xattrs only.
func TestKeyScansReturnOneKind(t *testing.T) {
	s := newTestState(t)
	i := ino(2, 3)
	named := func(tag byte, n string) Key { return with(fsKey(5, i, tag), func(k *Key) { k.Name = b(n) }) }
	keys := []Key{
		fsKey(5, i, InodeAttr), named(InodeXattr, "user.a"), named(InodeXattr, "user.big"), fsKey(5, i, InodeTarget),
		with(fsKey(5, i, InodeLock), func(k *Key) { k.Mode, k.Session = LockExclusive, 7 }),
		named(InodeEntry, "a"), named(InodeEntry, "b"), named(InodeFolded, "a"), fsKey(5, i, InodeParent),
		with(fsKey(5, i, InodeXattr|PieceBit), func(k *Key) { k.Piece, k.Name = 1, b("user.big") }),
		fsKey(5, ino(2, 2), InodeParent), fsKey(5, ino(2, 4), InodeAttr),
	}
	for _, k := range keys {
		run(t, s, ops(Put(mustKey(&k), nil)))
	}
	scan := func(lo, hi []byte, want ...Key) {
		t.Helper()
		got, _ := scanKeys(t, s, lo, hi, 100)
		var w []string
		for _, k := range want {
			w = append(w, string(mustKey(&k)))
		}
		if !slices.Equal(got, w) {
			t.Errorf("scan [%x, %x) got %x, want %x", lo, hi, got, w)
		}
	}
	d := mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: i, Tag: InodeEntry, Name: b("a")})
	d = d[:len(d)-1]
	scan(d, PrefixEnd(d), keys[5], keys[6])
	x := d[: len(d)-1 : len(d)-1]
	x = append(x, InodeXattr)
	scan(mustKey(&keys[0]), PrefixEnd(x), keys[0], keys[1], keys[2])
}

func TestKeyErrors(t *testing.T) {
	good := mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: ino(2, 0), Tag: SlotReplay, Session: 7, Mount: 1, ID: 42})
	attr := mustKey(&Key{Space: SpaceFS, Volume: 5, Inode: ino(2, 1), Tag: InodeAttr})
	at := func(k []byte, i int, c byte) []byte { k = slices.Clone(k); k[i] = c; return k }
	for name, k := range map[string][]byte{
		"empty":                      nil,
		"unknown space":              {0x05, 1},
		"space alone":                {SpaceFS},
		"truncated inode":            attr[:10],
		"no tag":                     attr[:13],
		"unknown inode tag":          at(attr, 13, 0x08),
		"unknown ctl tag":            {SpaceCtl, 0x07},
		"unknown volume tag":         {SpaceVolume, 0, 0, 0, 5, 0x03},
		"piece of an attribute":      at(attr, 13, InodeAttr|PieceBit),
		"replay missing its ID":      good[:33],
		"trailing bytes":             append(slices.Clone(good), 0),
		"bytes after a":              append(slices.Clone(attr), 0),
		"slot tag at sequence 1":     at(attr, 13, SlotNext),
		"inode tag at sequence 0":    at(good, 13, InodeAttr),
		"piece 0":                    append(at(attr, 13, InodeTarget|PieceBit), 0),
		"piece with no number":       at(attr, 13, InodeTarget|PieceBit),
		"entry with no name":         at(attr, 13, InodeEntry),
		"s3 with no key":             {SpaceS3, 0, 0, 0, 5},
		"unknown lock mode":          append(at(attr, 13, InodeLock), 3, 0, 0, 0, 0, 0, 0, 0, 7),
		"ctl session short of an ID": {SpaceCtl, CtlSession, 0, 7},
		"over MaxKey":                append([]byte{SpaceS3, 0, 0, 0, 5}, make([]byte, MaxKey-4)...),
	} {
		var d Key
		if err := DecodeKey(k, &d); !errors.Is(err, ErrBadKey) {
			t.Errorf("%s: %x decodes to %+v (%v)", name, k, d, err)
		}
	}
	if _, err := MakeInode(1<<SlotBits, 1); !errors.Is(err, ErrBadKey) {
		t.Errorf("slot 2^24: %v", err)
	}
	if _, err := MakeInode(1, 1<<SeqBits); !errors.Is(err, ErrBadKey) {
		t.Errorf("sequence 2^40: %v", err)
	}
	dst := b("kept")
	if got, err := AppendKey(dst, &Key{Space: SpaceFS, Volume: 5, Inode: ino(2, 0), Tag: InodeAttr}); !errors.Is(err, ErrBadKey) || string(got) != "kept" {
		t.Errorf("an inode tag at sequence 0 appends %q (%v)", got, err)
	}
	s3 := Key{Space: SpaceS3, Volume: 5, Name: make([]byte, 1024)}
	if k, err := AppendKey(nil, &s3); err != nil || len(k) != 1029 {
		t.Errorf("a 1,024-byte S3 key: %d bytes (%v), want 1,029", len(k), err)
	}
	s3.Name = make([]byte, MaxKey-4)
	if _, err := AppendKey(nil, &s3); !errors.Is(err, ErrBadKey) {
		t.Errorf("an S3 index key over MaxKey: %v", err)
	}
}

func split(t *testing.T, v []byte) [][]byte {
	t.Helper()
	h, err := AppendHead(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	vals := [][]byte{h}
	for i := 1; i <= Pieces(len(v)); i++ {
		vals = append(vals, Piece(v, i))
	}
	return vals
}

func join(vals [][]byte) ([]byte, error) {
	m, v, err := DecodeHead(vals[0])
	if err != nil || m != len(vals)-1 {
		return nil, ErrBadPiece
	}
	v = slices.Clone(v)
	for i := 1; i <= m; i++ {
		if v, err = AppendPiece(v, vals[i], i, m); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func TestPieces(t *testing.T) {
	for _, c := range []struct{ n, values int }{{0, 1}, {999, 1}, {1000, 2}, {1999, 2}, {2000, 3}, {4095, 5}, {MaxSplit, 66}} {
		v := make([]byte, c.n)
		for i := range v {
			v[i] = byte(i * 7)
		}
		vals := split(t, v)
		for _, p := range vals {
			if len(p) > MaxValue {
				t.Fatalf("%d bytes: a value of %d", c.n, len(p))
			}
		}
		got, err := join(vals)
		if len(vals) != c.values || err != nil || !bytes.Equal(got, v) {
			t.Errorf("%d bytes: %d values, joined %d bytes (%v); want %d values", c.n, len(vals), len(got), err, c.values)
		}
	}
	if _, err := AppendHead(nil, make([]byte, MaxSplit+1)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a value over MaxSplit: %v", err)
	}
	vals := split(t, make([]byte, 2500))
	for name, bad := range map[string][][]byte{
		"short head":        {vals[0][:999], vals[1], vals[2]},
		"short inner piece": {vals[0], vals[1][:999], vals[2]},
		"long last piece":   {vals[0], vals[1], append(slices.Clone(vals[1]), 0)},
		"missing piece":     {vals[0], vals[1], nil},
		"empty head":        {nil},
		"count past 65":     {append([]byte{66}, vals[0][1:]...)},
	} {
		if _, err := join(bad); !errors.Is(err, ErrBadPiece) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := AppendPiece(nil, vals[2], 3, 2); !errors.Is(err, ErrBadPiece) {
		t.Errorf("a piece past the count: %v", err)
	}
	full := [][]byte{append([]byte{maxPieces}, make([]byte, MaxValue-1)...)}
	for range maxPieces {
		full = append(full, make([]byte, MaxValue))
	}
	if v, err := join(full); !errors.Is(err, ErrBadPiece) {
		t.Errorf("a head of 65 and 65 full pieces joined to %d bytes (%v), over MaxSplit", len(v), err)
	}
}

// Encoding and decoding keys and split values, and checking a Txn's replay key, allocate nothing.
func TestKeysAllocateNothing(t *testing.T) {
	k := Key{Space: SpaceFS, Volume: 5, Inode: ino(2, 1), Tag: InodeEntry, Name: b("beach.jpg")}
	buf := make([]byte, 0, MaxKey)
	tx := Txn{Ops: []Op{Put(b("a"), nil)}, Replay: replayKey(1, 9), Oldest: 3}
	var d Key
	v := make([]byte, 2500)
	head, joined := make([]byte, 0, MaxValue), make([]byte, 0, len(v))
	if n := testing.AllocsPerRun(100, func() {
		buf, _ = AppendKey(buf[:0], &k)
		_ = DecodeKey(buf, &d)
		_, _ = tx.Check()
		head, _ = AppendHead(head[:0], v)
		m, first, _ := DecodeHead(head)
		joined = append(joined[:0], first...)
		for i := 1; i <= m; i++ {
			joined, _ = AppendPiece(joined, Piece(v, i), i, m)
		}
	}); n != 0 {
		t.Fatalf("%v allocations per run", n)
	}
}
