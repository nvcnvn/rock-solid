package trace

import (
	"bytes"
	"errors"
	"testing"
)

type ruleCase struct {
	rule     Rule
	pass     string
	fail     string
	failLine int
}

var ruleCases = []ruleCase{
	{
		rule: OneFsyncPerRound(),
		pass: `1000 0 action pass=1 DiskWrite round=0 io=100 lba=0 len=4096 vec=1
1000 0 action pass=1 DiskFsync round=0 io=101 linked=1
1010 0 event pass=2 DiskDone io=100 err=ok
1011 0 event pass=3 DiskDone io=101 err=ok
1011 0 action pass=3 DiskWrite round=0 io=102 lba=8 len=4096 vec=1
1011 0 action pass=3 DiskFsync round=0 io=103 linked=1
1020 0 event pass=4 DiskDone io=102 err=ok
1021 0 event pass=5 DiskDone io=103 err=ok
1030 0 action pass=6 DiskWrite round=1 io=104 lba=64 len=12288 vec=3
1030 0 action pass=6 DiskFsync round=1 io=105 linked=1
1031 0 action pass=7 DiskWrite round=2 io=106 lba=128 len=12288 vec=3
1040 0 event pass=8 DiskDone io=104 err=ok
1041 0 event pass=9 DiskDone io=105 err=ok
1041 0 output pass=9 Durable round=1 lsn=1
1041 0 action pass=9 DiskWrite round=3 io=108 lba=67 len=8192 vec=2
1050 0 event pass=10 DiskDone io=106 err=ok
1050 0 action pass=10 DiskFsync round=2 io=107 linked=0
1051 0 event pass=11 DiskDone io=108 err=ok
1051 0 action pass=11 DiskFsync round=3 io=109 linked=0
1060 0 event pass=12 DiskDone io=107 err=ok
1060 0 output pass=12 Durable round=2 lsn=2
1061 0 event pass=13 DiskDone io=109 err=ok
1061 0 output pass=13 Durable round=3 lsn=3
`,
		fail: `1030 0 action pass=6 DiskWrite round=1 io=104 lba=64 len=12288 vec=3
1030 0 action pass=6 DiskFsync round=1 io=105 linked=1
1040 0 event pass=7 DiskDone io=104 err=ok
1040 0 action pass=7 DiskFsync round=1 io=106 linked=0
`,
		failLine: 4,
	},
	{
		rule: WriteAndFlushTogether(),
		pass: depthOne,
		fail: `100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=4096 vec=1
101 0 action pass=2 DiskFsync round=1 io=2 linked=0
`,
		failLine: 2,
	},
	{
		rule: CommitDepth(1),
		pass: depthOne,
		fail: `100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=4096 vec=1
100 0 action pass=1 DiskFsync round=1 io=2 linked=1
101 0 event pass=2 Timer timer=1
101 0 action pass=2 DiskWrite round=2 io=3 lba=65 len=4096 vec=1
`,
		failLine: 4,
	},
	{
		rule: DurableAfterFlush(),
		pass: `100 0 action pass=1 DiskWrite round=1 io=104 lba=64 len=4096 vec=1
100 0 action pass=1 DiskFsync round=1 io=105 linked=1
101 0 action pass=2 DiskWrite round=2 io=106 lba=128 len=4096 vec=1
110 0 event pass=3 DiskDone io=104 err=ok
111 0 event pass=4 DiskDone io=105 err=ok
111 0 output pass=4 Durable round=1 lsn=1
120 0 event pass=5 DiskDone io=106 err=ok
120 0 action pass=5 DiskFsync round=2 io=107 linked=0
130 0 event pass=6 DiskDone io=107 err=ok
130 0 output pass=6 Durable round=2 lsn=2
`,
		fail: `100 0 action pass=1 DiskWrite round=1 io=104 lba=64 len=4096 vec=1
100 0 action pass=1 DiskFsync round=1 io=105 linked=1
101 0 action pass=2 DiskWrite round=2 io=106 lba=128 len=4096 vec=1
110 0 event pass=3 DiskDone io=104 err=ok
110 0 action pass=3 DiskFsync round=2 io=107 linked=0
111 0 event pass=4 DiskDone io=105 err=ok
111 0 output pass=4 Durable round=1 lsn=1
115 0 event pass=5 DiskDone io=107 err=ok
115 0 output pass=5 Durable round=2 lsn=2
120 0 event pass=6 DiskDone io=106 err=ok
`,
		failLine: 9,
	},
	{
		rule: NoDurableAfterFailure(),
		pass: `5000 0 action pass=12 DiskWrite round=7 io=41 lba=67 len=8192 vec=2
5000 0 action pass=12 DiskFsync round=7 io=42 linked=1
5950 0 event pass=13 DiskDone io=41 err=ok
5951 0 event pass=14 DiskDone io=42 err=io
6000 0 event pass=15 Crash
7000 0 event pass=16 Restart
7100 0 action pass=17 DiskWrite round=7 io=41 lba=67 len=8192 vec=2
7100 0 action pass=17 DiskFsync round=7 io=42 linked=1
7150 0 event pass=18 DiskDone io=41 err=ok
7151 0 event pass=19 DiskDone io=42 err=ok
7151 0 output pass=19 Durable round=7 lsn=7
`,
		fail: `5000 0 action pass=12 DiskWrite round=7 io=41 lba=67 len=8192 vec=2
5000 0 action pass=12 DiskFsync round=7 io=42 linked=1
5950 0 event pass=13 DiskDone io=41 err=ok
5951 0 event pass=14 DiskDone io=42 err=io
5952 0 output pass=14 Durable round=7 lsn=7
`,
		failLine: 5,
	},
	{
		rule: FreezeAfterCut(),
		pass: `100 0 action pass=1 Entry round=1 type=write vol=1 inode=2 index=0
100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=8192 vec=2
100 0 action pass=1 DiskFsync round=1 io=2 linked=1
200 0 event pass=2 NetRecv peer=1 msg=cut
300 0 action pass=3 Entry round=2 type=freeze vol=1 inode=2 index=0
300 0 action pass=3 Entry round=2 type=write vol=1 inode=2 index=0
300 0 action pass=3 DiskWrite round=2 io=3 lba=66 len=8192 vec=2
300 0 action pass=3 DiskFsync round=2 io=4 linked=1
400 0 action pass=4 Entry round=3 type=write vol=1 inode=2 index=0
400 0 action pass=4 DiskWrite round=3 io=5 lba=68 len=8192 vec=2
400 0 action pass=4 DiskFsync round=3 io=6 linked=1
`,
		fail: `100 0 action pass=1 Entry round=1 type=write vol=1 inode=2 index=0
100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=8192 vec=2
100 0 action pass=1 DiskFsync round=1 io=2 linked=1
200 0 event pass=2 NetRecv peer=1 msg=cut
300 0 action pass=3 Entry round=2 type=write vol=1 inode=2 index=0
300 0 action pass=3 DiskWrite round=2 io=3 lba=66 len=8192 vec=2
300 0 action pass=3 DiskFsync round=2 io=4 linked=1
`,
		failLine: 5,
	},
	{
		rule: PeeringBeforeWrite(),
		pass: `100 0 action pass=1 NetSend peer=1 msg=peering
100 0 action pass=1 NetSend peer=2 msg=peering
110 0 event pass=2 NetRecv peer=1 msg=peering_reply
111 0 event pass=3 NetRecv peer=2 msg=peering_reply
120 0 event pass=4 NetRecv peer=9 msg=write
120 0 action pass=4 NetSend peer=1 msg=write
120 0 action pass=4 NetSend peer=2 msg=write
`,
		fail: `100 0 action pass=1 NetSend peer=1 msg=peering
100 0 action pass=1 NetSend peer=2 msg=peering
110 0 event pass=2 NetRecv peer=1 msg=peering_reply
110 0 event pass=2 NetRecv peer=9 msg=write
110 0 action pass=2 NetSend peer=1 msg=write
111 0 event pass=3 NetRecv peer=2 msg=peering_reply
`,
		failLine: 5,
	},
	{
		rule: PeeringBeforeRead(),
		pass: `100 0 action pass=1 NetSend peer=1 msg=peering
100 0 action pass=1 NetSend peer=2 msg=peering
110 0 event pass=2 NetRecv peer=1 msg=peering_reply
111 0 event pass=3 NetRecv peer=2 msg=peering_reply
120 0 event pass=4 NetRecv peer=9 msg=read
120 0 action pass=4 DiskRead io=1 lba=64 len=4096
130 0 event pass=5 DiskDone io=1 err=ok
130 0 action pass=5 NetSend peer=9 msg=read_reply
`,
		fail: `100 0 action pass=1 NetSend peer=1 msg=peering
100 0 action pass=1 NetSend peer=2 msg=peering
105 0 event pass=2 NetRecv peer=9 msg=read
105 0 action pass=2 DiskRead io=1 lba=64 len=4096
107 0 event pass=3 DiskDone io=1 err=ok
107 0 action pass=3 NetSend peer=9 msg=read_reply
110 0 event pass=4 NetRecv peer=1 msg=peering_reply
111 0 event pass=5 NetRecv peer=2 msg=peering_reply
`,
		failLine: 6,
	},
	{
		rule: OneWritevPerFD(),
		pass: `1000250 2 syscall pass=81 epoll_wait ready=2
1000251 2 syscall pass=81 writev fd=21 iov=3 bytes=12416
1000251 2 syscall pass=81 writev fd=22 iov=1 bytes=4160
1000260 2 syscall pass=82 epoll_wait ready=1
1000261 2 syscall pass=82 writev fd=21 iov=1 bytes=64
`,
		fail: `1000250 2 syscall pass=81 epoll_wait ready=2
1000251 2 syscall pass=81 writev fd=21 iov=3 bytes=12416
1000252 2 syscall pass=81 writev fd=21 iov=1 bytes=64
`,
		failLine: 3,
	},
	{
		rule: ReadBudget(256 << 10),
		pass: `1000250 2 syscall pass=81 epoll_wait ready=1
1000251 2 syscall pass=81 read fd=17 bytes=262144
1000260 2 syscall pass=82 epoll_wait ready=1
1000261 2 syscall pass=82 read fd=17 bytes=262144
`,
		fail: `1000250 2 syscall pass=81 epoll_wait ready=1
1000251 2 syscall pass=81 read fd=17 bytes=204800
1000252 2 syscall pass=81 read fd=17 bytes=204800
`,
		failLine: 3,
	},
	{
		rule: OneEpollWaitPerPass(),
		pass: `1000250 2 syscall pass=81 epoll_wait ready=1
1000251 2 syscall pass=81 read fd=17 bytes=4096
1000252 2 event pass=81 NetRecv peer=5 msg=write
1000260 2 syscall pass=82 epoll_wait ready=0
`,
		fail: `1000250 2 syscall pass=81 epoll_wait ready=1
1000251 2 syscall pass=81 read fd=17 bytes=4096
1000252 2 syscall pass=81 epoll_wait ready=0
`,
		failLine: 3,
	},
}

const depthOne = `100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=4096 vec=1
100 0 action pass=1 DiskFsync round=1 io=2 linked=1
110 0 event pass=2 DiskDone io=1 err=ok
111 0 event pass=3 DiskDone io=2 err=ok
111 0 output pass=3 Durable round=1 lsn=1
111 0 action pass=3 DiskWrite round=2 io=3 lba=65 len=4096 vec=1
111 0 action pass=3 DiskFsync round=2 io=4 linked=1
120 0 event pass=4 DiskDone io=3 err=ok
121 0 event pass=5 DiskDone io=4 err=ok
121 0 output pass=5 Durable round=2 lsn=2
`

func TestEveryRulePassesItsTraceAndFailsItsPlantedOne(t *testing.T) {
	for _, c := range ruleCases {
		t.Run(c.rule.Name, func(t *testing.T) {
			if err := Check([]byte(c.pass), c.rule); err != nil {
				t.Fatalf("passing trace: %v", err)
			}
			v := wantViolation(t, Check([]byte(c.fail), c.rule))
			if v.Line != c.failLine || v.Rule != c.rule.Name {
				t.Fatalf("planted trace gave %v, want line %d", v, c.failLine)
			}
			recs := parseAll(t, c.fail)
			if again := wantViolation(t, CheckRecords(recs, c.rule)); *again != *v {
				t.Fatalf("records gave %v, lines gave %v", again, v)
			}
		})
	}
}

func TestOneLoopsRecordsDoNotBreakAnothersRules(t *testing.T) {
	for _, c := range ruleCases {
		t.Run(c.rule.Name, func(t *testing.T) {
			fail, pass := parseAll(t, c.fail), parseAll(t, c.pass)
			var merged []Record
			want := 0
			for i := range max(len(fail), len(pass)) {
				if i < len(pass) {
					r := pass[i]
					r.Sim, r.Node = true, 9
					merged = append(merged, r)
				}
				if i < len(fail) {
					merged = append(merged, fail[i])
					if i+1 == c.failLine {
						want = len(merged)
					}
				}
			}
			v := wantViolation(t, CheckRecords(merged, c.rule))
			if v.Line != want {
				t.Fatalf("merged trace gave %v, want line %d", v, want)
			}
		})
	}
}

func TestTheScenarioExamplePassesEveryLoopRule(t *testing.T) {
	err := Check([]byte(scenarioExample), OneFsyncPerRound(), WriteAndFlushTogether(), CommitDepth(1),
		DurableAfterFlush(), OneWritevPerFD(), ReadBudget(256<<10), OneEpollWaitPerPass())
	if err != nil {
		t.Fatal(err)
	}
}

func TestAPassWithSyscallsAndNoEpollWaitFails(t *testing.T) {
	partialFirst := `1000251 2 syscall pass=80 writev fd=21 iov=1 bytes=64
1000252 2 syscall pass=81 epoll_wait ready=1
1000253 2 syscall pass=81 read fd=17 bytes=4096
`
	if err := Check([]byte(partialFirst), OneEpollWaitPerPass()); err != nil {
		t.Fatalf("a first pass held in part: %v", err)
	}
	for _, c := range []struct {
		trace string
		line  int
	}{
		{partialFirst + "1000260 2 syscall pass=82 read fd=17 bytes=4096\n1000261 2 event pass=83 Resumed\n", 5},
		{partialFirst + "1000260 2 syscall pass=82 read fd=17 bytes=4096\n", 4},
	} {
		v := wantViolation(t, Check([]byte(c.trace), OneEpollWaitPerPass()))
		if v.Line != c.line {
			t.Fatalf("got %v, want line %d", v, c.line)
		}
	}
}

func TestAnUnlinkedFlushCoversOnlyWritesDoneBeforeIt(t *testing.T) {
	trace := `100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=4096 vec=1
101 0 event pass=2 Timer timer=1
101 0 action pass=2 DiskFsync round=1 io=2 linked=1
110 0 event pass=3 DiskDone io=1 err=ok
111 0 event pass=4 DiskDone io=2 err=ok
111 0 output pass=4 Durable round=1 lsn=1
`
	v := wantViolation(t, Check([]byte(trace), DurableAfterFlush()))
	if v.Line != 6 {
		t.Fatalf("a linked flush in a later pass than its write: %v", v)
	}
	failedWrite := `100 0 action pass=1 DiskWrite round=1 io=1 lba=64 len=4096 vec=1
110 0 event pass=2 DiskDone io=1 err=io
110 0 action pass=2 DiskFsync round=1 io=2 linked=0
111 0 event pass=3 DiskDone io=2 err=ok
111 0 output pass=3 Durable round=1 lsn=1
`
	v = wantViolation(t, Check([]byte(failedWrite), DurableAfterFlush()))
	if v.Line != 5 {
		t.Fatalf("a failed write: %v", v)
	}
}

func TestCheckReportsTheFirstLineThatDoesNotParse(t *testing.T) {
	for _, c := range []struct {
		text string
		line int
	}{
		{"1 0 event pass=1 Resumed\n1 0 event pass=1 Resumed extra=1\n", 2},
		{"1 0 event pass=1 Resumed\n\n", 2},
		{"1 0 event pass=1 Resumed", 1},
	} {
		v := wantViolation(t, Check([]byte(c.text), OneEpollWaitPerPass()))
		if v.Line != c.line || v.Rule != SyntaxRule {
			t.Fatalf("%q gave %v", c.text, v)
		}
	}
	if err := Check(nil, OneEpollWaitPerPass()); err != nil {
		t.Fatal(err)
	}
}

func TestTheFirstRuleBrokenOnALineIsReported(t *testing.T) {
	trace := `1000250 2 syscall pass=81 epoll_wait ready=1
1000251 2 syscall pass=81 read fd=17 bytes=300000
1000252 2 syscall pass=81 writev fd=21 iov=1 bytes=64
1000253 2 syscall pass=81 writev fd=21 iov=1 bytes=64
`
	v := wantViolation(t, Check([]byte(trace), OneWritevPerFD(), ReadBudget(256<<10)))
	if v.Line != 2 || v.Rule != "read-budget" {
		t.Fatalf("got %v", v)
	}
	if !bytes.Contains([]byte(v.Error()), []byte("line 2 breaks read-budget")) {
		t.Fatalf("error %q", v.Error())
	}
}

func wantViolation(t *testing.T, err error) *Violation {
	t.Helper()
	var v *Violation
	if !errors.As(err, &v) {
		t.Fatalf("got %v, want a Violation", err)
	}
	return v
}

func parseAll(t *testing.T, text string) []Record {
	t.Helper()
	var recs []Record
	for _, l := range bytes.Split(bytes.TrimSuffix([]byte(text), []byte("\n")), []byte("\n")) {
		r, err := Parse(l)
		if err != nil {
			t.Fatalf("%q: %v", l, err)
		}
		recs = append(recs, r)
	}
	return recs
}
