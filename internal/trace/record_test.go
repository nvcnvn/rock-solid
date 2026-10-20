package trace

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var everyName = []string{
	"1000252 2 event pass=81 NetRecv peer=5 msg=write",
	"1000252 2 event pass=81 PeerLost peer=5.1",
	"1001207 2 event pass=82 DiskDone io=41 err=ok",
	"1001207 2 event pass=82 Timer timer=9",
	"1001207 2 event pass=82 Resumed",
	"1000253 2 action pass=81 NetSend peer=7.3 msg=peering_reply",
	"1000253 2 action pass=81 DiskRead io=40 lba=67 len=4096",
	"1000253 2 action pass=81 DiskWrite round=7 io=41 lba=67 len=12288 vec=3",
	"1000253 2 action pass=81 DiskFsync round=7 io=42 linked=1",
	"1000253 2 action pass=81 DiskTrim io=43 lba=1024 len=1048576",
	"1000253 2 action pass=81 SetTimer timer=9 at=2000000",
	"1000253 2 action pass=81 Entry round=7 type=freeze vol=1 inode=2 index=3",
	"1000250 2 syscall pass=81 epoll_wait ready=3",
	"1000251 2 syscall pass=81 read fd=17 bytes=4096",
	"1000254 2 syscall pass=81 writev fd=21 iov=3 bytes=12416",
	"1000254 2 syscall pass=81 io_uring_enter submit=2 wait=0",
	"1000254 2 syscall pass=81 timerfd_settime at=2000000",
	"1000254 2 syscall pass=81 eventfd_write fd=9",
	"1000255 2 output pass=81 Durable round=7 lsn=12",
	"1000255 2 output pass=81 Failed err=nospace",
	"1000255 2 output pass=81 CheckpointDone lsn=12",
	"1001207 2 event pass=82 Crash",
	"1001207 2 event pass=82 Restart",
	"1000253 2 event pass=81 Drop peer=7.3 msg=write_reply",
}

const scenarioExample = `1000250 2 syscall pass=81 epoll_wait ready=3
1000251 2 syscall pass=81 read fd=17 bytes=4096
1000252 2 event pass=81 NetRecv peer=5 msg=write
1000253 2 action pass=81 DiskWrite round=7 io=41 lba=67 len=12288 vec=3
1000253 2 action pass=81 DiskFsync round=7 io=42 linked=1
1000254 2 syscall pass=81 writev fd=21 iov=3 bytes=12416
1001207 2 event pass=82 DiskDone io=41 err=ok
1001208 2 event pass=82 DiskDone io=42 err=ok
1001208 2 output pass=82 Durable round=7 lsn=12
1001209 3.2 syscall pass=81 epoll_wait ready=3
`

func TestRecordIs64Bytes(t *testing.T) {
	if got := reflect.TypeFor[Record]().Size(); got != RecordSize {
		t.Fatalf("Record is %d bytes, want %d", got, RecordSize)
	}
}

func TestEveryNameHasALine(t *testing.T) {
	seen := make([]bool, numNames)
	for _, line := range everyName {
		r, err := Parse([]byte(line))
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		seen[r.Name] = true
	}
	for n := Name(1); n < numNames; n++ {
		if !seen[n] {
			t.Errorf("no line for %s", n)
		}
	}
}

func TestEveryKindRoundTrips(t *testing.T) {
	for _, line := range append(everyName, strings.Split(strings.TrimSuffix(scenarioExample, "\n"), "\n")...) {
		r, err := Parse([]byte(line))
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if got := string(r.Append(nil)); got != line {
			t.Fatalf("re-encoded %q as %q", line, got)
		}
		again, err := Parse(r.Append(nil))
		if err != nil || again != r {
			t.Fatalf("%q parsed back to %+v, %v; want %+v", line, again, err, r)
		}
	}
}

func TestParseFillsFieldsInSchemaOrder(t *testing.T) {
	r, err := Parse([]byte("1000253 3.2 action pass=81 DiskWrite round=7 io=41 lba=67 len=12288 vec=3"))
	if err != nil {
		t.Fatal(err)
	}
	want := Record{Tick: 1000253, Pass: 81, Node: 3, Loop: 2, Sim: true, Name: ActDiskWrite, Fields: [MaxFields]uint64{7, 41, 67, 12288, 3}}
	if r != want {
		t.Fatalf("got %+v, want %+v", r, want)
	}
	r, err = Parse([]byte("7 1.0 event pass=3 Drop peer=2 msg=write"))
	if err != nil {
		t.Fatal(err)
	}
	want = Record{Tick: 7, Pass: 3, Node: 1, Sim: true, Name: EvDrop, Fields: [MaxFields]uint64{Peer(core.NodeID{Node: 2}), uint64(MsgWrite)}}
	if r != want {
		t.Fatalf("got %+v, want %+v", r, want)
	}
	r, err = Parse([]byte("5 0 event pass=1 NetRecv peer=5.2 msg=cut"))
	if err != nil {
		t.Fatal(err)
	}
	if PeerID(r.Fields[0]) != (core.NodeID{Node: 5, Loop: 2}) || Msg(r.Fields[1]) != MsgCut || r.Kind() != KindEvent {
		t.Fatalf("got %+v", r)
	}
}

func TestRandomRecordsRoundTrip(t *testing.T) {
	rng := core.NewRand(1)
	for range 10000 {
		r := randomRecord(rng)
		line := r.Append(nil)
		got, err := Parse(line)
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if got != r {
			t.Fatalf("%q parsed to %+v, want %+v", line, got, r)
		}
	}
}

func TestEveryKindRejectsBrokenLines(t *testing.T) {
	for _, line := range everyName {
		toks := strings.Split(line, " ")
		fields := toks[5:]
		bad := [][2]string{
			{"extra field", line + " x=1"},
			{"repeated field", line + " " + toks[len(toks)-1]},
			{"unknown kind", strings.Join(append([]string{toks[0], toks[1], "evnt"}, toks[3:]...), " ")},
			{"non-numeric tick", "12a " + strings.Join(toks[1:], " ")},
			{"negative tick", "-1 " + strings.Join(toks[1:], " ")},
			{"overflowed tick", "18446744073709551616 " + strings.Join(toks[1:], " ")},
			{"leading zero", "0" + line},
			{"empty", ""},
			{"trailing space", line + " "},
			{"double space", strings.Replace(line, " ", "  ", 1)},
			{"no pass", strings.Join(append(toks[:3:3], toks[4:]...), " ")},
			{"unknown name", strings.Join(append(toks[:4:4], append([]string{"Nope"}, fields...)...), " ")},
		}
		if len(fields) > 0 {
			bad = append(bad,
				[2]string{"missing field", strings.Join(toks[:len(toks)-1], " ")},
				[2]string{"unknown field", strings.Join(toks[:5], " ") + " bogus=1" + strings.TrimPrefix(strings.Join(fields, " "), fields[0])})
		}
		if len(fields) > 1 {
			swapped := append([]string{}, fields...)
			swapped[0], swapped[1] = swapped[1], swapped[0]
			bad = append(bad, [2]string{"out of order", strings.Join(append(toks[:5:5], swapped...), " ")})
		}
		for _, c := range bad {
			if _, err := Parse([]byte(c[1])); !errors.Is(err, ErrSyntax) {
				t.Errorf("%s: %q gave %v, want ErrSyntax", c[0], c[1], err)
			}
		}
	}
}

func TestParseRejectsNonCanonicalValues(t *testing.T) {
	for _, line := range []string{
		"1 2 action pass=1 NetRecv peer=5 msg=write",
		"1 2 event pass=1 NetRecv peer=5.0 msg=write",
		"1 2 event pass=1 NetRecv peer=05 msg=write",
		"1 2 event pass=1 NetRecv peer=5 msg=gossip",
		"1 2 event pass=1 NetRecv peer=4294967296 msg=write",
		"1 2 event pass=1 DiskDone io=1 err=OK",
		"1 2 action pass=1 DiskFsync round=1 io=2 linked=2",
		"1 2 action pass=1 Entry round=1 type=delete vol=1 inode=2 index=3",
		"1 65536 event pass=1 Resumed",
		"1 4294967296.1 event pass=1 Resumed",
		"1 .1 event pass=1 Resumed",
		"1 1. event pass=1 Resumed",
		"1 2 event pass=01 Resumed",
		"1 2 event pass= Resumed",
		"1 2 event pass=1 Resumed\n",
		"1 2 event pass=1 Timer timer",
		"1 2 syscall pass=1 read bytes=1 fd=17",
	} {
		if _, err := Parse([]byte(line)); !errors.Is(err, ErrSyntax) {
			t.Errorf("%q gave %v, want ErrSyntax", line, err)
		}
	}
}

func TestTwoEncodingsOfOneInputAreByteIdentical(t *testing.T) {
	encode := func() []byte {
		rng := core.NewRand(7)
		var b []byte
		for range 1000 {
			r := randomRecord(rng)
			b = r.Append(b)
			b = append(b, '\n')
		}
		return b
	}
	if a, b := encode(), encode(); !bytes.Equal(a, b) {
		t.Fatal("one seed gave two traces")
	}
}

func TestAppendAllocatesNothing(t *testing.T) {
	r, err := Parse([]byte(everyName[7]))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 0, 256)
	if n := testing.AllocsPerRun(1000, func() { buf = r.Append(buf[:0]) }); n != 0 {
		t.Fatalf("Append allocates %v times", n)
	}
}

func BenchmarkAppend(b *testing.B) {
	r, err := Parse([]byte(everyName[7]))
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf = r.Append(buf[:0])
	}
}

func randomRecord(rng *core.Rand) Record {
	r := Record{
		Tick: core.Tick(rng.Uint64() >> rng.Below(64)),
		Pass: rng.Uint64() >> rng.Below(64),
		Loop: uint16(rng.Uint32()),
		Name: Name(rng.Range(1, uint64(numNames)-1)),
	}
	if rng.Chance(1, 2) {
		r.Node, r.Sim = rng.Uint32(), true
	}
	for i, f := range schemas[r.Name].fields {
		switch f.typ {
		case typeNum:
			r.Fields[i] = rng.Uint64() >> rng.Below(64)
		case typeBool:
			r.Fields[i] = rng.Below(2)
		case typePeer:
			r.Fields[i] = Peer(core.NodeID{Node: rng.Uint32(), Loop: uint32(rng.Below(4))})
		case typeMsg:
			r.Fields[i] = rng.Below(uint64(len(msgText)))
		case typeResult:
			r.Fields[i] = rng.Below(uint64(len(resultText)))
		case typeEntry:
			r.Fields[i] = rng.Below(uint64(len(entryText)))
		}
	}
	return r
}
