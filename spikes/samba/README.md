# Samba gateway spike (leases, share modes, locks)

**Question.** If SMB is later served by Samba on top of the RWX file frontend, how does Samba handle client caching (oplocks and leases), share modes and byte-range locks? What breaks when Samba runs on more than one node? And what does Samba need from the filesystem under it that should shape the FUSE frontend now?

**Setup.** Linux 6.17, Samba 4.19.5 from Ubuntu 24.04 `.deb`s unpacked into a local sysroot. smbd runs as a normal user: `uid_wrapper` makes it believe it is root, and the kernel still sees uid 1000. The SMB client is `smbprotocol` (pure Python, SMB 3.1.1), patched in [smbspike.py](smbspike.py) to receive and acknowledge lease and oplock breaks, or to leave them unacknowledged. A *gateway* is one smbd with its own state directory, so its own `locking.tdb` and `brlock.tdb`, as on one node without CTDB. [passfs.c](passfs.c) is a counting passthrough FUSE filesystem standing in for our frontend: two mounts of one backing directory stand in for two nodes. Everything runs on one host and one kernel.

```
./fetch.sh                  # debs into vendor/root, smbprotocol into vendor/py (no root)
./build.sh                  # passfs
. ./env.sh && python3 run.py [L] [G] [F] [--fast]
```

Raw output: [results.txt](results.txt).

## How Samba keeps SMB clients coherent (one gateway, local directory)

| Test | Result |
| --- | --- |
| L1: c1 asks for an RWH lease (read, write, handle caching) | Granted RWH. A second open by c1 with the same lease key shares the lease: no break |
| L1: c2 opens the same file | c1 gets a break RWH → RH that requires an ack. c2's open waits for it: 2.2 ms here. c2 gets RH, not RWH |
| L1: c2 writes | c1 gets RH → none. After c1 closes, c2's next open gets RWH again |
| L2: a third client writes while two hold RH | Both get RH → none, but **the write does not wait**: 0.3 ms. Read-caching breaks are asynchronous |
| L3: the RWH holder never acks | **c2's open blocks for 35.0 s**, then succeeds and the lease is downgraded anyway |
| L3: an RH holder never acks, another client writes | The write completes at once. The holder can keep serving stale cached reads until it processes the break |
| L4: c1 opens with share = read only (deny write) | c2 open for write or delete: SHARING_VIOLATION; for read: OK. **A local process opens O_RDWR and writes freely**: share modes exist only in Samba's `locking.tdb` (Linux no longer supports kernel share modes, `LOCK_MAND`) |
| L5: c1 locks [0, 100) exclusively | c2 lock, read and write inside the range: LOCK_NOT_GRANTED / FILE_LOCK_CONFLICT. SMB locks are mandatory, enforced by smbd |
| L5: the same lock seen by a local process | `F_OFD_GETLK` shows a write lock [0, 100) (`posix locking = yes` mirrors it as an OFD lock). A local write into the range still succeeds: POSIX locks are advisory |
| L5: c2 waits on a blocking lock, c1 unlocks | Granted 1.2 ms after the unlock |
| L6: local (non-SMB) write while c1 holds RWH, `kernel oplocks = no` (default) | **No break.** c1's cache is stale and nobody tells it |
| L6: `kernel oplocks = yes` | SMB2 leases are no longer granted (RWH request → none). A batch oplock is granted and backed by `F_SETLEASE`; a local `open()` breaks it (0.35 ms with an ack). If the client never acks, the local `open()` blocks 35.0 s |
| L7: change notify, local process creates a file | Delivered in 0.7 ms (inotify) |

## Two gateways on one directory, no CTDB

What happens if every node simply runs its own smbd over the shared filesystem.

| Test | Result |
| --- | --- |
| G1: lease on gateway A, open and write through gateway B | **Both clients are granted RWH**, and A gets no break when B writes. Each believes it has exclusive write caching |
| G2: deny-write open on A, open for write on B | OK: share modes are not enforced across gateways |
| G3: exclusive byte-range lock on A, conflicting lock, read or write on B | LOCK_NOT_GRANTED / FILE_LOCK_CONFLICT: **locks do cross**, because both gateways mirror them into fcntl locks on the same filesystem and B checks the kernel |
| G4: `kernel oplocks = yes` on both | The batch oplock on A is broken by B's open, because both smbd share one kernel. But **B's open takes 1.0 s**: `open()` returns EWOULDBLOCK, smbd defers the open and retries on a 1 s timer |

## Samba on top of FUSE

| Test | Result |
| --- | --- |
| F1: FUSE requests per SMB operation, attribute timeout 1 s | create + 4K write + close: 22. open + 4K read + close: 10. open + close (stat): 6. **Listing a 100-entry directory: 312**: per entry, one lookup, one `getxattr user.DOSATTRIB` and one `listxattr` (for the EA size) |
| F1: timeout 0 (strict) | 33 / 16 / 11 / 352 |
| F1: `case sensitive = yes`, `ea support = no`, `store dos attributes = no` | 17 / 8 / 4 / 107: the listing drops to one lookup per entry |
| F1: the create sequence | Before creating `new`, smbd stats it, then **opens and reads the whole parent directory** to look for a case-insensitive match. Then it reads `system.posix_acl_*` on the parent, creates, writes `user.DOSATTRIB`, and reads it back after the write. (The `security.capability` lookup on write comes from the kernel, not Samba) |
| F1b: new names in a 10,000-entry directory | Default (`case sensitive = auto`): create 4.6 ms, open of a missing name 3.4 ms. `case sensitive = yes`: 0.94 ms and 0.24 ms. The scan costs O(directory size) per create and per miss |
| F1c: local ext4 share, same operations | open + read + close 0.75 ms, create + write + close 1.0 ms; on passfs 0.93 and 1.26 ms. Mostly Python client overhead; only the request counts are meaningful |
| F2: xattrs Samba touches | `user.DOSATTRIB`, `system.posix_acl_access`, `system.posix_acl_default`. On a filesystem without xattrs Samba still works, without DOS attributes. FUSE never caches xattrs, so each one is a daemon request |
| F3: `kernel oplocks = yes`, gateway A on mount A, gateway B on mount B | **No break**, on open or on write through B. A plain `F_SETLEASE` on mount A is broken by an open on mount A but not on mount B. FUSE accepts leases but keeps them in the local kernel: the daemon never hears of them |
| F4: change notify through gateway A | A file created through mount B is not reported; through mount A it is. inotify sees only local changes |
| F5: one file through both mounts | Same `st_ino` (passfs passes the backing inode through), **different `st_dev`** (0x36, 0x4d). Samba keys `locking.tdb` and `brlock.tdb` by (dev, ino) |
| F6: lock through gateway A, conflicting lock through gateway B, passfs without a `lock` op | **Granted**: the kernel keeps POSIX locks per mount. Both clients hold an exclusive lock on the same range |
| F6: passfs forwards `lock` to OFD locks on the backing file | LOCK_NOT_GRANTED: correct across nodes |

Pitfalls met along the way: in `--foreground` mode smbd exits when stdin reaches EOF; on exit it sends SIGTERM to its whole process group, so the harness starts it in its own session.

## Conclusions

**SMB leases have the same shape as our Tier 2 cache modes.** A lease is a per-client cache right; a break is our recall. Breaks that take away write or handle caching block the conflicting open until the holder acks, or for 35 s. Read-caching breaks don't block anyone. So an SMB reader may see stale data for one message delay after another client's write, much like our mmap compromise. Our recall waits for the ack or for the session to be fenced, which is stricter.

**All of Samba's coherence state is per gateway.** Leases, share modes, open handles and SMB lock state live in smbd's own tdb files. A second smbd on another node, or a pod writing through our FUSE mount, is invisible to it (G1, G2, L6, F3). Samba's two escape hatches don't help on FUSE: kernel oplocks stay local to one kernel and cost 1 s per conflicting open even when they work, and inotify sees only local changes. Only byte-range locks cross over, and only when the filesystem's fcntl locks are cluster-wide (G3, F6).

That leaves two ways to deploy Samba later:

1. **One gateway per volume** (the Longhorn share-manager shape; acceptable here because SMB is a side door, not the RWX path). Correct among SMB clients. The gap is mixed access: a pod on another node writing a file that an SMB client has cached. Closing it needs a small **Samba VFS module** that registers the gateway's opens with the inode owner and turns a recall into a lease break. That is how GPFS integrates: `vfs_gpfs` maps leases and share modes onto GPFS's own cluster-wide ones. Until that module exists, shares that pods also write should run with `oplocks = no` (no client caching).
2. **CTDB on several nodes.** CTDB distributes `locking.tdb` and `brlock.tdb` across the cluster. It needs a cluster lock: a helper program (the `!` syntax, as `ctdb_etcd_lock` does for etcd) that could take a MetaStore lock. It also needs `vfs_fileid`, because `st_dev` differs per mount (F5), and cluster-wide fcntl locks from the filesystem. Samba 4.22 turns directory leases off when clustered. Samba 4.25's persistent handles are experimental and require `posix locking = no` and `kernel oplocks = no`.

Either way, SMB stays out of v1. But four things belong in the FUSE frontend and MetaStore design **now**, because adding them later would change on-disk keys or the FUSE contract:

1. **Stable inode numbers across nodes** (`use_ino`, inode ID as `st_ino`). Samba, CTDB and NFS all key open state on it. The design already allocates inode IDs; it should state that they are exposed as `st_ino`.
2. **xattrs are part of the inode, cached like attributes.** smbd reads `user.DOSATTRIB` and the POSIX ACL xattrs on every open and, for a listing, on every entry. FUSE never caches xattrs, so the daemon must, with the same coherence as attributes, and return them without a MetaStore round trip. Store small xattrs in the inode record, not as separate keys.
3. **Directory listing and lookup must be cheap.** A Samba listing costs three FUSE requests per entry, plus a full directory scan for every create and every missing name (case-insensitive matching). Support readdirplus. Reserve a case-folded name index in the key layout (`dir/{parent}/{casefold(name)}` beside the exact name) that a VFS module can query, as `vfs_glusterfs` does through a special xattr. Without it, Samba admins must set `case sensitive = yes`, which Windows applications don't expect.
4. **FUSE `lock` / `flock` go to MetaStore locks once more than one node can serve SMB.** Without them two gateways both grant the same exclusive lock (F6). This moves byte-range locks from Tier 3 earlier only if multi-gateway SMB is wanted; a single gateway per volume doesn't need it.

Not tested here: Windows and macOS clients, durable handles across reconnect, CTDB itself, Samba 4.22+ directory leases, and anything at real network latency.
