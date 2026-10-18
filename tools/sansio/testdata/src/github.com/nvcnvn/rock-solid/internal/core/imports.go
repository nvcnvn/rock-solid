package core

import (
	_ "context"     // want `sans-IO package imports "context": cancellation and deadlines arrive as events`
	_ "crypto/rand" // want `sans-IO package imports "crypto/rand": use a seeded core.Rand`
	_ "io/fs"       // want `sans-IO package imports "io/fs": emit an Action for I/O`
	_ "log"         // want `sans-IO package imports "log": record a trace event instead of a free-form log`
	_ "log/slog"    // want `sans-IO package imports "log/slog": record a trace event instead of a free-form log`
	_ "math/rand"   // want `sans-IO package imports "math/rand": use a seeded core.Rand`
	_ "net"         // want `sans-IO package imports "net": emit an Action for I/O`
	_ "net/http"    // want `sans-IO package imports "net/http": emit an Action for I/O`
	_ "os"          // want `sans-IO package imports "os": emit an Action for I/O`
	_ "os/exec"     // want `sans-IO package imports "os/exec": emit an Action for I/O`
	_ "runtime"     // want `sans-IO package imports "runtime": the loop owns scheduling; emit an Action`
	_ "sync"        // want `sans-IO package imports "sync": a machine runs on one loop and needs no synchronization`
	_ "sync/atomic" // want `sans-IO package imports "sync/atomic": a machine runs on one loop and needs no synchronization`
	_ "syscall"     // want `sans-IO package imports "syscall": emit an Action for I/O`
	_ "unsafe"      // want `sans-IO package imports "unsafe": keep state in plain Go values`

	_ "golang.org/x/sys/unix" // want `sans-IO package imports "golang.org/x/sys/unix": emit an Action for I/O`
)
