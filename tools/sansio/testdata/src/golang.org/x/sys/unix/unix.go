package unix

// Getpid stands in for the real package in the analyzer's test data.
func Getpid() int { return 0 }

// Timespec stands in for the real type.
type Timespec struct{}

// ClockGettime stands in for the real function.
func ClockGettime(int32, *Timespec) error { return nil }

// Gettimeofday stands in for the real function.
func Gettimeofday(*Timeval) error { return nil }

// Timeval stands in for the real type.
type Timeval struct{}

// Time stands in for the real function.
func Time(*int64) (int64, error) { return 0, nil }
