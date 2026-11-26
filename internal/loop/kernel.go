package loop

import "github.com/nvcnvn/rock-solid/internal/core"

// Kernel is everything a loop asks of the kernel that involves time or waiting.
type Kernel interface {
	// Watch adds fd to the set Wait waits on, reported under token when readable.
	Watch(fd int, token uint64) error
	// Modify sets whether Wait reports fd when it is readable and when it is writable; a hang-up
	// or error is reported either way.
	Modify(fd int, token uint64, in, out bool) error
	// Unwatch removes fd from the set.
	Unwatch(fd int) error
	// Wait returns what is ready, appended to ready; it blocks until something is only when block
	// is set, which is the loop's own decision. A wake or the armed deadline passing comes back
	// as TokenWake or TokenTimer.
	Wait(ready []Ready, block bool) []Ready
	// Wake makes the loop's next Wait return TokenWake; any goroutine may call it.
	Wake()
	// Clocks reads CLOCK_BOOTTIME, CLOCK_MONOTONIC_RAW, CLOCK_MONOTONIC and the VMClock marker together.
	Clocks() core.Clocks
	// Arm sets the loop's one timer to an absolute deadline on CLOCK_BOOTTIME.
	Arm(deadline core.Tick)
}

// Ready is a token Wait reports and what its descriptor is ready for.
type Ready struct {
	Token uint64
	In    bool
	Out   bool
	Hup   bool
}

// Tokens Wait reports for the kernel's own descriptors and the disk ring's; a loop's connections use their ConnIDs, whose slots stay below the low 32 bits of these.
const (
	TokenWake  = ^uint64(0)
	TokenTimer = ^uint64(0) - 1
	TokenDisk  = ^uint64(0) - 2
)
