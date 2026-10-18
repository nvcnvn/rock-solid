// CRC32C of a 4 KiB block with Go's hash/crc32, which uses SSE4.2 with three interleaved streams.
package main

import (
	"fmt"
	"hash/crc32"
	"time"
)

func main() {
	t := crc32.MakeTable(crc32.Castagnoli)
	b := make([]byte, 4096)
	for i := range b {
		b[i] = 0x5a
	}
	const n = 2_000_000
	var x uint32
	s := time.Now()
	for i := range n {
		b[0] = byte(i)
		x ^= crc32.Checksum(b, t)
	}
	fmt.Printf("CRC lang=go impl=hash/crc32 | ns_per_4k=%.0f\n", float64(time.Since(s).Nanoseconds())/n)
	if x == 1 {
		fmt.Println()
	}
}
