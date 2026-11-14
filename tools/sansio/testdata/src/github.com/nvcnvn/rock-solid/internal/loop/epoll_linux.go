package loop

import "golang.org/x/sys/unix"

func clock() {
	_ = unix.ClockGettime(1, nil)
}
