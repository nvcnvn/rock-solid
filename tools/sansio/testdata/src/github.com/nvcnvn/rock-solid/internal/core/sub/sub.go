package sub

import (
	_ "sync" // want `sans-IO package imports "sync": a machine runs on one loop and needs no synchronization`
	"time"
)

func clock() time.Time { return time.Now() } // want `sans-IO package calls time.Now: use ctx.Now`
