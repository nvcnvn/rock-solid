package loop

// MarkerStuck is the marker read when the VMClock sequence count never settles.
const MarkerStuck = markerStuck

// MarkerWithin reads the disruption marker in at most tries reads.
func (v *VMClock) MarkerWithin(tries int) uint64 { return v.marker(tries) }
