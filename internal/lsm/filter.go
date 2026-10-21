// The Bloom filter is ported from Pebble's sstable/tablefilters/bloom (a7b8321871e3), whose notice
// follows; it writes RocksDB's full-filter format.
//
// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this list of
//     conditions and the following disclaimer.
//   - Redistributions in binary form must reproduce the above copyright notice, this list of
//     conditions and the following disclaimer in the documentation and/or other materials provided
//     with the distribution.
//   - Neither the name of Google Inc. nor the names of its contributors may be used to endorse or
//     promote products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
// IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
// OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package lsm

import (
	"bytes"
	"encoding/binary"
)

// FilterKeys is what a tree's run filters hold.
type FilterKeys uint8

const (
	// WholeKeys filters hold every key.
	WholeKeys FilterKeys = iota
	// Prefixes filters hold every key's prefix that Config.Prefix names.
	Prefixes
	// KeysAndPrefixes filters hold every key and its prefix.
	KeysAndPrefixes
)

// A run's filter is what it holds u8 (a FilterKeys) and the key count u64, then RocksDB's full
// filter: lines of 64 bytes of bits, the probe count u8 and the line count u32, little-endian.
// Every probe of a key falls in one line.
const (
	bitsPerKey    = 10
	filterProbes  = 6
	cacheLineSize = 64
	cacheLineBits = cacheLineSize * 8
	filterKeyHdr  = 9
	filterTrailer = 5
)

// filterLines is the line count for keys, odd so that more bits pick the line.
func filterLines(keys int) uint32 {
	return uint32((uint64(keys)*bitsPerKey+cacheLineBits-1)/cacheLineBits) | 1
}

func bloomHash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ (uint32(len(b)) * m)
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	// RocksDB sign-extends the trailing bytes.
	switch len(b) {
	case 3:
		h += uint32(int8(b[2])) << 16
		fallthrough
	case 2:
		h += uint32(int8(b[1])) << 8
		fallthrough
	case 1:
		h += uint32(int8(b[0]))
		h *= m
		h ^= h >> 24
	}
	return h
}

// filterWriter fills a filter sized in advance as a run's keys stream out in order.
type filterWriter struct {
	what       FilterKeys
	prefix     func([]byte) int
	blob       []byte
	lines      uint32
	keys       int
	last       []byte
	hasPfx     bool
	skipSecond bool
}

// reset starts a filter sized for at most keys distinct filter keys.
func (f *filterWriter) reset(what FilterKeys, prefix func([]byte) int, keys int) {
	f.what, f.prefix, f.keys, f.hasPfx = what, prefix, 0, false
	f.lines = filterLines(keys)
	f.blob = make([]byte, filterKeyHdr+int(f.lines)*cacheLineSize+filterTrailer)
}

func (f *filterWriter) addHash(h uint32) {
	bits := f.blob[filterKeyHdr:]
	delta := h>>17 | h<<15
	line := bits[(h%f.lines)*cacheLineSize:][:cacheLineSize]
	for range filterProbes {
		line[(h>>3)&(cacheLineSize-1)] |= 1 << (h & 7)
		h += delta
	}
	f.keys++
}

// add adds what the filter holds of k, which follows every key added before.
func (f *filterWriter) add(k []byte) {
	if f.skipSecond && f.keys == 1 {
		f.skipSecond = false
		return
	}
	if f.what != Prefixes {
		f.addHash(bloomHash(k))
	}
	if f.what == WholeKeys {
		return
	}
	p := prefixOf(f.prefix, k)
	if len(p) == 0 || f.hasPfx && bytes.Equal(p, f.last) {
		return
	}
	f.last, f.hasPfx = append(f.last[:0], p...), true
	if f.what == KeysAndPrefixes && len(p) == len(k) {
		return
	}
	f.addHash(bloomHash(p))
}

func prefixOf(prefix func([]byte) int, k []byte) []byte {
	if prefix == nil {
		return nil
	}
	return k[:min(max(prefix(k), 0), len(k))]
}

// checkHolds panics unless the filter holds what it must of each key in taken, a uvarint-prefixed
// key then value each.
func (f *filterWriter) checkHolds(taken []byte) {
	for len(taken) > 0 {
		var k []byte
		k, taken = cutUvarint(taken)
		_, taken = cutUvarint(taken)
		fk := k
		if f.what == Prefixes {
			if fk = prefixOf(f.prefix, k); len(fk) == 0 {
				continue
			}
		}
		if !f.holds(bloomHash(fk)) {
			panic("lsm: a flush left a key out of its filter")
		}
	}
}

func (f *filterWriter) holds(h uint32) bool {
	line := f.blob[filterKeyHdr+int(h%f.lines)*cacheLineSize:][:cacheLineSize]
	delta := h>>17 | h<<15
	for range filterProbes {
		if line[(h>>3)&(cacheLineSize-1)]&(1<<(h&7)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// finish returns the filter, holding its key count.
func (f *filterWriter) finish() []byte {
	n := len(f.blob) - filterTrailer
	f.blob[n] = filterProbes
	binary.LittleEndian.PutUint32(f.blob[n+1:], f.lines)
	f.blob[0] = byte(f.what)
	binary.LittleEndian.PutUint64(f.blob[1:], uint64(f.keys))
	b := f.blob
	f.blob = nil
	return b
}

// validFilter reports whether b, a run's filter with its header, is well formed and holds what.
func validFilter(b []byte, what FilterKeys) bool {
	if len(b) < filterKeyHdr+cacheLineSize+filterTrailer {
		return false
	}
	n := len(b) - filterTrailer
	lines := binary.LittleEndian.Uint32(b[n+1:])
	return FilterKeys(b[0]) == what && b[n] >= 1 && b[n] <= 30 && lines > 0 &&
		uint64(n-filterKeyHdr) == uint64(lines)*cacheLineSize && binary.LittleEndian.Uint64(b[1:]) <= uint64(lines)*cacheLineBits
}

// filterKeyCount is how many keys the filter holds.
func filterKeyCount(b []byte) int { return int(binary.LittleEndian.Uint64(b[1:])) }

// mayContain reports whether the filter b, with its header, may hold the key hashed to h.
func mayContain(b []byte, h uint32) bool {
	n := len(b) - filterTrailer
	probes := b[n]
	lines := binary.LittleEndian.Uint32(b[n+1:])
	line := b[filterKeyHdr+int(h%lines)*cacheLineSize:][:cacheLineSize]
	delta := h>>17 | h<<15
	for range probes {
		if line[(h>>3)&(cacheLineSize-1)]&(1<<(h&7)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
