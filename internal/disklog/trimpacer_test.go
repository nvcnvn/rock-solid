package disklog

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const second = core.Tick(1000 * 1000)

// drain runs the pacer from now on, as its timer would, and returns when each TRIM went out.
func drain(p *trimPacer, now core.Tick) []core.Tick {
	var at []core.Tick
	for {
		_, ok, wake := p.next(now)
		switch {
		case ok:
			at = append(at, now)
		case wake == 0:
			return at
		default:
			now = wake
		}
	}
}

// 10 segments of 64 MiB freed at once go out at 128 MiB/s: one every half second.
func TestTrimsArePaced(t *testing.T) {
	p := trimPacer{rate: DefaultTrimRate, segBytes: 64 << 20}
	for s := range uint32(10) {
		p.push(s+1, 0, false)
	}
	at := drain(&p, 0)
	for i, a := range at {
		if want := core.Tick(i) * second / 2; a != want {
			t.Fatalf("TRIM %d at %v, want %v", i, a.Duration(), want.Duration())
		}
	}
	if len(at) != 10 {
		t.Fatalf("%d TRIMs", len(at))
	}
}

// 2,000 segments of 64 MiB queued at once would take 1,000 s at the base rate; once they have
// waited 5 minutes the pacer adds the rate that clears them in 5 more, so every one is trimmed
// within 10 minutes of being queued.
func TestATrimBacklogClearsWithinTenMinutes(t *testing.T) {
	p := trimPacer{rate: DefaultTrimRate, segBytes: 64 << 20}
	for s := range uint32(2000) {
		p.push(s+1, 0, false)
	}
	at := drain(&p, 0)
	if last := at[len(at)-1]; len(at) != 2000 || last > 600*second || last < 400*second {
		t.Fatalf("%d TRIMs, the last at %v", len(at), last.Duration())
	}
	if at[599] >= 300*second || at[600] < 299*second {
		t.Fatalf("TRIM 600 at %v: the base rate holds for the first 5 minutes", at[600].Duration())
	}
}
