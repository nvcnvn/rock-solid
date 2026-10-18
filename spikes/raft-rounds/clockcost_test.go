package main

import "testing"

func BenchmarkThreadCPUClock(b *testing.B) {
	for b.Loop() {
		cpuNow()
	}
}
