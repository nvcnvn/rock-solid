package loop

// Sizing is how many loops a process runs and the GOMAXPROCS it sets, if any.
type Sizing struct {
	Loops int
	// Procs is the GOMAXPROCS to set, 0 to leave it to the runtime, which follows the CPU limit.
	Procs int
}

// Size sizes the loops from the runtime's default GOMAXPROCS, procs, and the CPU count, cpus.
// Since Go 1.25 the default follows the cgroup CPU limit, so procs below cpus means a limit: one
// loop fewer than procs, and GOMAXPROCS left to the runtime. Otherwise a loop per CPU and one P
// more. A limit equal to the CPU count looks like none.
func Size(procs, cpus int) Sizing {
	if procs < cpus {
		return Sizing{Loops: max(1, procs-1)}
	}
	return Sizing{Loops: cpus, Procs: cpus + 1}
}
