package chunkstore

import (
	"errors"
	"slices"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// ErrLogRefused reports a disk whose log ID is already up on another node or device, as a cloned drive's is, or belongs to a disk whose log was replaced.
var ErrLogRefused = errors.New("chunkstore: log ID refused")

// ErrLostRefused reports a declaration that a disk is lost for a disk the record does not know, or one that is up.
var ErrLostRefused = errors.New("chunkstore: disk not down")

// DeviceStart is what a storage node does at start with one of its devices.
type DeviceStart uint8

const (
	// StartOpened reports the device's log opened.
	StartOpened DeviceStart = iota
	// StartFormat formats the device under a new log ID, then reports it opened.
	StartFormat
	// StartFailed reports the device failed and leaves it to the operator, never formatting it; recovery's own repairs are the only writes it has had.
	StartFailed
)

// StartDevice is what a node does with a device whose store's recovery, or the format StartFormat began, ended with err, nil once it serves, and for StartFormat the log ID to format it under; rng must be seeded from the operating system at each process start, never from a fixed or configured seed, or a format cut short draws the same log ID again.
func StartDevice(rng *core.Rand, err error) (DeviceStart, uint64) {
	switch {
	case err == nil:
		return StartOpened, 0
	case errors.Is(err, disklog.ErrNotFormatted):
		// A format cut short leaves both superblocks zeros, so a new log ID per start keeps a
		// crash from bringing one back; two formats share an ID with odds about n²/2⁶⁵.
		return StartFormat, rng.Uint64()
	}
	return StartFailed, 0
}

// RecordedDisk is a disk as the control plane records it: its ID, the log ID it reported, the node and the device it was last reported on, whether it is up or else since when it is down, and whether its log is gone, a new log having been reported in its place, and its class, set when it joins. Device is the drive's stable identity, such as its WWN, never its slot on the node, which a restart may renumber.
type RecordedDisk struct {
	ID        uint32
	Log       uint64
	Node      uint32
	Device    uint64
	Up        bool
	DownSince core.Tick
	Gone      bool
	Class     string
}

// plantCloneJoins is a bug the clone run plants in every record, the control plane's own among them:
// a log up on another device joins as a new disk.
var plantCloneJoins bool

// DiskRecord is the control plane's record of disks, each known by the log ID it reports: a disk ID never names another log, and is never reused.
type DiskRecord struct {
	disks []RecordedDisk
}

// Report takes a node's report that its device opened the log log, of class class, returning the disk's ID. A log ID the record knows is the disk it names, marked up on node, its class unchanged, unless its log was replaced, it is up on another node, or device is not its own, which is refused: a disk moved to another node keeps its device, so its log on another device is a copy, which may miss acknowledged writes. A log ID it does not know joins as a new disk of that class under the next disk ID, and a disk last reported on that device stays down with its log gone, so its members refill elsewhere at once.
func (r *DiskRecord) Report(node uint32, device, log uint64, class string) (uint32, error) {
	if i := slices.IndexFunc(r.disks, func(d RecordedDisk) bool { return d.Log == log }); i >= 0 {
		d := &r.disks[i]
		elsewhere := d.Device != device || d.Up && d.Node != node
		if d.Gone || elsewhere && !plantCloneJoins {
			return 0, ErrLogRefused
		}
		if !elsewhere {
			d.Node, d.Device, d.Up = node, device, true
			return d.ID, nil
		}
	}
	for i := range r.disks {
		if d := &r.disks[i]; d.Node == node && d.Device == device && !d.Gone {
			d.Up, d.Gone = false, true
		}
	}
	id := uint32(len(r.disks) + 1)
	r.disks = append(r.disks, RecordedDisk{ID: id, Log: log, Node: node, Device: device, Up: true, Class: class})
	return id, nil
}

// NewDiskRecord is the record of disks, which hold IDs 1 to len(disks) in order, as a record committed them.
func NewDiskRecord(disks []RecordedDisk) DiskRecord { return DiskRecord{disks: disks} }

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

// Lost takes an operator's declaration that disk id, down, will not return: its log is gone as if replaced, so its members refill at once and a later report of it is refused, and what only it holds stays unfound.
func (r *DiskRecord) Lost(id uint32) error {
	if id < 1 || int(id) > len(r.disks) || r.disks[id-1].Up {
		return ErrLostRefused
	}
	r.disks[id-1].Gone = true
	return nil
}

// Remove takes disk id, up or down, out of the pool for good, as Lost does a down one: a later report of its log is refused. ErrLostRefused for a disk the record does not know.
func (r *DiskRecord) Remove(id uint32) error {
	if id < 1 || int(id) > len(r.disks) {
		return ErrLostRefused
	}
	r.disks[id-1].Up, r.disks[id-1].Gone = false, true
	return nil
}

// Disks is every disk recorded, by ID; the caller must not change it.
func (r *DiskRecord) Disks() []RecordedDisk { return r.disks }
