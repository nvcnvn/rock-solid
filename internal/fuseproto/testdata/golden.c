/*
 * Writes golden.txt: one FUSE frame per line, "name hex", built from the kernel's own struct
 * definitions so the Go codec's offsets are checked against linux/fuse.h, not against themselves.
 * The values follow the exchanges of spikes/fuse-restart/rawfs.c and spikes/fuse-handoff/hofs.c.
 *
 *   mkdir -p /tmp/h/linux && curl -fsSL -o /tmp/h/linux/fuse.h \
 *     https://raw.githubusercontent.com/gregkh/linux/v6.18.44/include/uapi/linux/fuse.h
 *   cc -I/tmp/h -o /tmp/golden golden.c && /tmp/golden > golden.txt
 */
#include <linux/fuse.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static unsigned char buf[4096];
static size_t n;

static void put(const void *p, size_t len)
{
	memcpy(buf + n, p, len);
	n += len;
}

static void str(const char *s) { put(s, strlen(s) + 1); }

static void in(uint32_t opcode, uint64_t unique, uint64_t nodeid)
{
	struct fuse_in_header h = {.opcode = opcode, .unique = unique, .nodeid = nodeid,
				   .uid = 1000, .gid = 1001, .pid = 4242};
	n = 0;
	put(&h, sizeof(h));
}

static void out(int32_t error, uint64_t unique)
{
	struct fuse_out_header h = {.error = error, .unique = unique};
	n = 0;
	put(&h, sizeof(h));
}

static void emit(const char *name)
{
	uint32_t len = n;
	memcpy(buf, &len, sizeof(len));
	printf("%s ", name);
	for (size_t i = 0; i < n; i++)
		printf("%02x", buf[i]);
	printf("\n");
}

static struct fuse_attr attr(uint64_t ino)
{
	return (struct fuse_attr){.ino = ino, .size = 5000, .blocks = 16, .atime = 1700000001,
				  .mtime = 1700000002, .ctime = 1700000003, .atimensec = 11,
				  .mtimensec = 12, .ctimensec = 13, .mode = 0100644, .nlink = 1,
				  .uid = 1000, .gid = 1001, .rdev = 7, .blksize = 4096, .flags = 0};
}

static struct fuse_entry_out entry(uint64_t ino)
{
	return (struct fuse_entry_out){.nodeid = ino, .generation = 3, .entry_valid = 1,
				       .attr_valid = 2, .entry_valid_nsec = 500,
				       .attr_valid_nsec = 600, .attr = attr(ino)};
}

int main(void)
{
	struct fuse_init_in ii = {.major = 7, .minor = 45, .max_readahead = 131072,
				  .flags = FUSE_ASYNC_READ | FUSE_PARALLEL_DIROPS | FUSE_MAX_PAGES |
					   FUSE_INIT_EXT | FUSE_AUTO_INVAL_DATA,
				  .flags2 = (uint32_t)((FUSE_HAS_RESEND | FUSE_REQUEST_TIMEOUT) >> 32)};
	in(FUSE_INIT, 2, 0);
	put(&ii, sizeof(ii));
	emit("init");

	in(FUSE_LOOKUP, FUSE_UNIQUE_RESEND | 0x14a, 1);
	str("hang1");
	emit("lookup");

	struct fuse_forget_in fi = {.nlookup = 3};
	in(FUSE_FORGET, 0x150, 7);
	put(&fi, sizeof(fi));
	emit("forget");

	struct fuse_batch_forget_in bfi = {.count = 2};
	struct fuse_forget_one f1 = {.nodeid = 7, .nlookup = 3}, f2 = {.nodeid = 9, .nlookup = 1};
	in(FUSE_BATCH_FORGET, 0x152, 0);
	put(&bfi, sizeof(bfi));
	put(&f1, sizeof(f1));
	put(&f2, sizeof(f2));
	emit("batch_forget");

	struct fuse_getattr_in gi = {.getattr_flags = FUSE_GETATTR_FH, .fh = 77};
	in(FUSE_GETATTR, 0x154, 7);
	put(&gi, sizeof(gi));
	emit("getattr");

	struct fuse_setattr_in si = {.valid = FATTR_SIZE | FATTR_MTIME | FATTR_FH, .fh = 77,
				     .size = 4096, .lock_owner = 0x1234, .atime = 1700000001,
				     .mtime = 1700000002, .ctime = 1700000003, .atimensec = 21,
				     .mtimensec = 22, .ctimensec = 23, .mode = 0100600, .uid = 1002,
				     .gid = 1003};
	in(FUSE_SETATTR, 0x156, 7);
	put(&si, sizeof(si));
	emit("setattr");

	struct fuse_create_in ci = {.flags = 0101102, .mode = 0100644, .umask = 022,
				    .open_flags = 0};
	in(FUSE_CREATE, 0x158, 1);
	put(&ci, sizeof(ci));
	str("f0");
	emit("create");

	/* The supplementary groups extension, its own argument after the name, as dir.c adds it. */
	struct fuse_ext_header eh = {.size = 16, .type = FUSE_EXT_GROUPS};
	uint32_t groups[2] = {1, 1005};
	in(FUSE_CREATE, 0x159, 1);
	put(&ci, sizeof(ci));
	str("f0");
	put(&eh, sizeof(eh));
	put(groups, sizeof(groups));
	((struct fuse_in_header *)buf)->total_extlen = 2;
	emit("create_groups");

	struct fuse_open_in oi = {.flags = 0100002};
	in(FUSE_OPEN, 0x15a, 7);
	put(&oi, sizeof(oi));
	emit("open");

	struct fuse_read_in ri = {.fh = 7, .offset = 8192, .size = 4096,
				  .read_flags = FUSE_READ_LOCKOWNER, .lock_owner = 0x99,
				  .flags = 0100002};
	in(FUSE_READ, 0x15c, 7);
	put(&ri, sizeof(ri));
	emit("read");

	struct fuse_write_in wi = {.fh = 7, .offset = 4096, .size = 8,
				   .write_flags = FUSE_WRITE_LOCKOWNER, .lock_owner = 0x99,
				   .flags = 0100002};
	in(FUSE_WRITE, 0x15e, 7);
	put(&wi, sizeof(wi));
	put("w0:00001", 8);
	emit("write");

	struct fuse_flush_in fli = {.fh = 7, .lock_owner = 0x99};
	in(FUSE_FLUSH, 0x160, 7);
	put(&fli, sizeof(fli));
	emit("flush");

	struct fuse_release_in rli = {.fh = 7, .flags = 0100002,
				      .release_flags = FUSE_RELEASE_FLUSH, .lock_owner = 0x99};
	in(FUSE_RELEASE, 0x162, 7);
	put(&rli, sizeof(rli));
	emit("release");

	struct fuse_fsync_in fsi = {.fh = 7, .fsync_flags = FUSE_FSYNC_FDATASYNC};
	in(FUSE_FSYNC, 0x164, 7);
	put(&fsi, sizeof(fsi));
	emit("fsync");

	in(FUSE_UNLINK, 0x166, 1);
	str("f0");
	emit("unlink");

	struct fuse_mkdir_in mi = {.mode = 040755, .umask = 022};
	in(FUSE_MKDIR, FUSE_UNIQUE_RESEND | 0x14c, 1);
	put(&mi, sizeof(mi));
	str("mkhangdir");
	emit("mkdir");

	in(FUSE_RMDIR, 0x168, 1);
	str("mkhangdir");
	emit("rmdir");

	struct fuse_rename_in rni = {.newdir = 8};
	in(FUSE_RENAME, 0x16a, 1);
	put(&rni, sizeof(rni));
	str("old");
	str("new");
	emit("rename");

	struct fuse_open_in odi = {.flags = 0200000};
	in(FUSE_OPENDIR, 0x16c, 1);
	put(&odi, sizeof(odi));
	emit("opendir");

	struct fuse_read_in rdi = {.fh = 1, .offset = 2, .size = 4096};
	in(FUSE_READDIRPLUS, 0x16e, 1);
	put(&rdi, sizeof(rdi));
	emit("readdirplus");

	struct fuse_release_in rldi = {.fh = 1, .flags = 0200000};
	in(FUSE_RELEASEDIR, 0x170, 1);
	put(&rldi, sizeof(rldi));
	emit("releasedir");

	in(FUSE_STATFS, 0x172, 1);
	emit("statfs");

	in(FUSE_INTERRUPT, 0x174, 0);
	put(&(struct fuse_interrupt_in){.unique = 0x15c}, sizeof(struct fuse_interrupt_in));
	emit("unknown");

	struct fuse_link_in li = {.oldnodeid = 7};
	in(FUSE_LINK, 0x176, 8);
	put(&li, sizeof(li));
	str("f0-link");
	emit("link");

	struct fuse_access_in ai = {.mask = 6};
	in(FUSE_ACCESS, 0x178, 7);
	put(&ai, sizeof(ai));
	emit("access");

	struct fuse_fsync_in fsdi = {.fh = 3, .fsync_flags = FUSE_FSYNC_FDATASYNC};
	in(FUSE_FSYNCDIR, 0x17a, 1);
	put(&fsdi, sizeof(fsdi));
	emit("fsyncdir");

	struct fuse_rename2_in rn2i = {.newdir = 8, .flags = 1 /* RENAME_NOREPLACE */};
	in(FUSE_RENAME2, 0x17c, 1);
	put(&rn2i, sizeof(rn2i));
	str("old");
	str("new");
	emit("rename2");

	/* vfs_tmpfile names the unlinked dentry "/". */
	struct fuse_create_in tci = {.flags = 020200002 /* O_TMPFILE | O_RDWR */, .mode = 0100600,
				     .umask = 022, .open_flags = FUSE_OPEN_KILL_SUIDGID};
	in(FUSE_TMPFILE, 0x17e, 1);
	put(&tci, sizeof(tci));
	str("/");
	emit("tmpfile");

	struct fuse_statx_in sxi = {.getattr_flags = FUSE_GETATTR_FH, .fh = 77,
				    .sx_flags = 0x2000 /* AT_STATX_FORCE_SYNC */,
				    .sx_mask = 0xfff /* STATX_BASIC_STATS | STATX_BTIME */};
	in(FUSE_STATX, 0x180, 7);
	put(&sxi, sizeof(sxi));
	emit("statx");

	/* The compat body: the mount does not negotiate FUSE_SETXATTR_EXT. */
	struct fuse_setxattr_in sxai = {.size = 4, .flags = 1 /* XATTR_CREATE */};
	in(FUSE_SETXATTR, 0x182, 7);
	put(&sxai, FUSE_COMPAT_SETXATTR_IN_SIZE);
	str("user.DOSATTRIB");
	put("\0\4\0\x20", 4);
	emit("setxattr");

	struct fuse_getxattr_in gxi = {.size = 256};
	in(FUSE_GETXATTR, 0x184, 7);
	put(&gxi, sizeof(gxi));
	str("user.DOSATTRIB");
	emit("getxattr");

	struct fuse_getxattr_in lxi = {.size = 4096};
	in(FUSE_LISTXATTR, 0x186, 7);
	put(&lxi, sizeof(lxi));
	emit("listxattr");

	in(FUSE_REMOVEXATTR, 0x188, 7);
	str("user.DOSATTRIB");
	emit("removexattr");

	/* Replies and notifications. */
	struct fuse_init_out io = {.major = 7, .minor = 45, .max_readahead = 131072,
				   .flags = FUSE_ASYNC_READ | FUSE_PARALLEL_DIROPS | FUSE_MAX_PAGES |
					    FUSE_INIT_EXT | FUSE_AUTO_INVAL_DATA,
				   .flags2 = 0, .max_background = 16, .congestion_threshold = 12,
				   .max_write = 128 * 1024, .time_gran = 1, .max_pages = 32,
				   .map_alignment = 0, .max_stack_depth = 0, .request_timeout = 0};
	out(0, 2);
	put(&io, sizeof(io));
	emit("init_reply");

	struct fuse_entry_out eo = entry(7);
	out(0, FUSE_UNIQUE_RESEND | 0x14a);
	put(&eo, sizeof(eo));
	emit("entry_reply");

	struct fuse_attr_out ao = {.attr_valid = 2, .attr_valid_nsec = 600, .attr = attr(7)};
	out(0, 0x154);
	put(&ao, sizeof(ao));
	emit("attr_reply");

	struct fuse_open_out oo = {.fh = 7, .open_flags = FOPEN_DIRECT_IO};
	out(0, 0x15a);
	put(&oo, sizeof(oo));
	emit("open_reply");

	out(0, 0x158);
	eo = entry(7);
	put(&eo, sizeof(eo));
	oo = (struct fuse_open_out){.fh = 7, .open_flags = FOPEN_KEEP_CACHE};
	put(&oo, sizeof(oo));
	emit("create_reply");

	out(0, 0x15c);
	put("w0:00001", 8);
	emit("read_reply");

	struct fuse_write_out wo = {.size = 8};
	out(0, 0x15e);
	put(&wo, sizeof(wo));
	emit("write_reply");

	struct fuse_getxattr_out gxo = {.size = 23};
	out(0, 0x184);
	put(&gxo, sizeof(gxo));
	emit("xattr_size_reply");

	struct fuse_statfs_out so = {.st = {.blocks = 1000, .bfree = 900, .bavail = 800,
					    .files = 100, .ffree = 90, .bsize = 4096,
					    .namelen = 255, .frsize = 4096}};
	out(0, 0x172);
	put(&so, sizeof(so));
	emit("statfs_reply");

	out(0, 0x162);
	emit("empty_reply");

	out(-17, FUSE_UNIQUE_RESEND | 0x14c);
	emit("error_reply");

	out(0, 0x16e);
	/* Names of 8 and 16 bytes need no padding: a record is FUSE_DIRENTPLUS_SIZE, never 8 more. */
	const char *names[] = {"a", "longer-name", "eight-ch", "sixteen-chars-ab"};
	uint64_t inos[] = {7, 9, 11, 13};
	for (int i = 0; i < 4; i++) {
		struct fuse_direntplus d = {.entry_out = entry(inos[i])};
		d.dirent.ino = inos[i];
		d.dirent.off = 3 + i;
		d.dirent.namelen = strlen(names[i]);
		d.dirent.type = 8;
		size_t start = n;
		put(&d, FUSE_NAME_OFFSET_DIRENTPLUS);
		put(names[i], strlen(names[i]));
		size_t rec = FUSE_DIRENTPLUS_SIZE(&d);
		memset(buf + n, 0, start + rec - n);
		n = start + rec;
	}
	emit("readdirplus_reply");

	struct fuse_notify_inval_inode_out ni = {.ino = 7, .off = 0, .len = -1};
	out(FUSE_NOTIFY_INVAL_INODE, 0);
	put(&ni, sizeof(ni));
	emit("inval_inode");

	struct fuse_notify_inval_entry_out ne = {.parent = 1, .namelen = 2, .flags = FUSE_EXPIRE_ONLY};
	out(FUSE_NOTIFY_INVAL_ENTRY, 0);
	put(&ne, sizeof(ne));
	str("f0");
	emit("inval_entry");

	out(FUSE_NOTIFY_RESEND, 0);
	emit("resend");
	return 0;
}
