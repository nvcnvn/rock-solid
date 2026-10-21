package disklog

import "testing"

func TestPieceSetFreesABlockWithItsLastPiece(t *testing.T) {
	p := newPieceSet(4)
	a := BlobRef{Segment: 2, Block: 1, LSN: 9, Entry: 0}
	b := BlobRef{Segment: 2, Block: 1, LSN: 9, Entry: 1}
	p.add(a, 2, 5)
	p.add(b, 4, 6)
	live := make([]uint32, 4)
	p.coverage(live)
	if live[2] != 4 || p.in[2] != 2 {
		t.Fatalf("coverage %v, %d pieces in segment 2; want 4 blocks and 2", live, p.in[2])
	}
	if n := p.drop(a); n != 2 {
		t.Fatalf("dropping a frees %d blocks, want blocks 2 and 3 but not 4, which b touches", n)
	}
	if n := p.drop(a); n != 0 || !p.holds(b) {
		t.Fatalf("dropping a again freed %d blocks", n)
	}
	if n := p.drop(b); n != 2 || p.in[2] != 0 || len(p.recs) != 0 {
		t.Fatalf("dropping b frees %d blocks, leaving %d pieces", n, p.in[2])
	}
}
