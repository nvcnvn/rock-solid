package chunkstore

import (
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// ErrLogRefused reports a disk whose log ID is already up on another node or device, as a cloned drive's is, or belongs to a disk whose log was replaced.
var ErrLogRefused = errors.New("chunkstore: log ID refused")

// RecordedDisk is a disk as the control plane records it: its ID, the log ID it reported, where it was last reported, whether it is up or else since when it is down, and whether its log is gone, a new log having been reported in its place.
type RecordedDisk struct {
	ID        uint32
	Log       uint64
	Node      uint32
	Device    uint32
	Up        bool
	DownSince core.Tick
	Gone      bool
}

// DiskRecord is the control plane's record of disks, each known by the log ID it reports: a disk ID never names another log, and is never reused.
type DiskRecord struct {
	disks []RecordedDisk
}

// Report takes a node's report that its device opened the log log, returning the disk's ID. A log ID the record knows is the disk it names, marked up, unless it is up elsewhere or its log was replaced, which is refused. A log ID it does not know joins as a new disk under the next disk ID, and a disk last reported on that device stays down with its log gone, so its members refill elsewhere at once.
func (r *DiskRecord) Report(node, device uint32, log uint64) (uint32, error) {
	if i := slices.IndexFunc(r.disks, func(d RecordedDisk) bool { return d.Log == log }); i >= 0 {
		d := &r.disks[i]
		if d.Gone || d.Up && (d.Node != node || d.Device != device) {
			return 0, ErrLogRefused
		}
		d.Node, d.Device, d.Up = node, device, true
		return d.ID, nil
	}
	for i := range r.disks {
		if d := &r.disks[i]; d.Node == node && d.Device == device && !d.Gone {
			d.Up, d.Gone = false, true
		}
	}
	id := uint32(len(r.disks) + 1)
	r.disks = append(r.disks, RecordedDisk{ID: id, Log: log, Node: node, Device: device, Up: true})
	return id, nil
}

// Down marks disk id down since since; it stays known by its log ID.
func (r *DiskRecord) Down(id uint32, since core.Tick) {
	if id >= 1 && int(id) <= len(r.disks) {
		r.disks[id-1].Up, r.disks[id-1].DownSince = false, since
	}
}

// Up marks disk id up.
func (r *DiskRecord) Up(id uint32) {
	if id >= 1 && int(id) <= len(r.disks) {
		r.disks[id-1].Up = true
	}
}

// Disks is every disk recorded, by ID; the caller must not change it.
func (r *DiskRecord) Disks() []RecordedDisk { return r.disks }
