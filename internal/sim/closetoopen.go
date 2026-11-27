package sim

import "github.com/nvcnvn/rock-solid/internal/core"

// CloseToOpen checks file operations recorded as a Mount's processes record them for close-to-open
// consistency: a read in a handle opened after a close of a handle that wrote the file returns
// the bytes that close covers, none older. A close that failed promises nothing. It returns the index of the first read that breaks the
// rule, and false if none does. The histories it checks are written so that a read's words name
// what it saw: every 8-byte word a handle writes holds that handle's ID, which rises from one
// writing handle of a file to the next, above its offset/8 (id<<32 | offset/8); one handle writes
// a file at a time, over every byte the file holds, and reads are of whole words. So every word of
// a read below the end of the last close before its open is that close's or a later writer's, at
// its own offset, and a word past that end is a later writer's or still zero.
func CloseToOpen(ops []Op) (int, bool) {
	type closed struct {
		at      core.Tick
		id, end uint64
	}
	type session struct {
		ino     uint64
		open    core.Tick
		id, end uint64
	}
	var sessions []session
	held := map[uint32]int{}
	closes := map[uint64][]closed{}
	reads := map[int]int{}
	for i, op := range ops {
		s, ok := held[op.Client]
		switch op.Input[0] & 0xff {
		case FileOpen:
			if op.Done && op.Output[0] == 0 {
				held[op.Client] = len(sessions)
				sessions = append(sessions, session{ino: op.Output[1], open: op.Invoke})
			}
		case FileWrite:
			if ok && op.Done && op.Output[0] == 0 {
				sessions[s].id = op.Input[3] >> 32
				sessions[s].end = max(sessions[s].end, op.Input[1]+op.Output[1])
			}
		case FileClose:
			if ok && op.Done && op.Output[0] == 0 && sessions[s].id != 0 {
				closes[sessions[s].ino] = append(closes[sessions[s].ino], closed{op.Complete, sessions[s].id, sessions[s].end})
			}
			delete(held, op.Client)
		case FileRead, FileMapRead:
			if ok {
				reads[i] = s
			}
		}
	}
	for i, op := range ops {
		s, ok := reads[i]
		if !ok || !op.Done || op.Output[0] != 0 {
			continue
		}
		var id, end uint64
		for _, c := range closes[sessions[s].ino] {
			if c.at < sessions[s].open {
				id, end = max(id, c.id), max(end, c.end)
			}
		}
		off, n, got, least, zero := op.Input[1], op.Input[2], op.Output[1], op.Output[2], op.Output[3]
		if end > off && got < min(n, end-off) || got > 0 && (least < max(id, 1) || zero < min(off+got, end)) {
			return i, true
		}
	}
	return 0, false
}
