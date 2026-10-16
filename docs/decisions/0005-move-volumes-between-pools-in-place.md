# 0005. Move volumes between pools in place

Status: Accepted (2026-10-16)

## Context
A pool's `data` and `parity` are fixed, so changing them means moving each volume to another pool. Snapshot and restore can already do that, but the result is a new PVC. Kubernetes does not let a PVC change its StorageClass, so the application must be stopped and its claim rebound. A small team that starts on `rep3` and later wants its media on `ec4-2` should not have to do that.

Snapshots already provide the pieces a move needs: a cut that pauses every writer for one round trip, reads that fall through to a source version, and a background flatten that copies chunks.

## Decision
A volume moves in place and keeps its `volume_id`, PVC and inode numbers:

1. Take a cut at epoch E, as a snapshot does.
2. Point the volume record at the new pool, with the old pool at E as its source.
3. Resume. Writes go to the new pool's placement groups, and a read of a block not yet written there falls through to the old pool at E.
4. Copy the remaining chunks in the background, as a flatten does.
5. Drop the source pointer and sweep the old pool.

Snapshots taken before the move stay in the old pool. Each snapshot record names its pool.

In Kubernetes, a move starts when a `VolumeAttributesClass` with a different `pool` parameter is applied to the PVC (CSI `ControllerModifyVolume`). The CLI can start one too.

## Consequences
- Changing a volume's redundancy shape no longer touches the application, its PVC or its inode numbers.
- Moves reuse the cut, read-through and flatten code and their simulation coverage. They ship with snapshots, not before.
- Until the copy finishes, the volume uses space in both pools, and reads of uncopied blocks take an extra hop.
- A volume's old snapshots keep space in the old pool until they are deleted.
