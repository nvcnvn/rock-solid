// Raw FUSE daemon for the handoff spike, run as root: mounts with mount(2), puts its /dev/fuse fd
// (and, with --shm, the memfd holding its io_uring buffers) into a separate holder process at
// mount time, and serves either over read/write on /dev/fuse or over FUSE-over-io_uring.
// Derived from spikes/fuse-restart/rawfs.c: a flat passthrough of one backing directory,
// nodeid = backing st_ino, fh = nodeid, so a successor rebuilds every handle from the backing
// directory alone.
//
//   hofs hold <sock>
//   hofs mount <mnt> <backing> --holder <sock> [--allow-other] [--uring] [--shm] [--hang]
//                                              [--batch N --delay-us U]
//   hofs get <holder-sock> <backing> [--resend] [--uring] [--shm] [--no-register] [--kick <mnt>] [--hang]
//
// stdin commands: resend | register | kick <mnt> | ping | exit
// stdout: one event per line, "EV <CLOCK_MONOTONIC us> <what> ...".
// --hang: LOOKUP of "hang*" is read and never answered.
// --uring: negotiate FUSE_OVER_IO_URING and serve every queue (one per possible CPU) from this
//   single thread. --shm: ring buffers live in a memfd mapped at a fixed address and handed to the
//   holder, so a successor maps the same memory at the same address and can answer requests that
//   the dead daemon had fetched.
#define _GNU_SOURCE
#include <linux/fuse.h>
#include <linux/io_uring.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_WRITE (128 * 1024)
#define BUFSZ (MAX_WRITE + 8192)
#define MAXFD 8
#define MAXBATCH 16
#define STATE_MAGIC 0x484f4653u
#define NQ_MAX 16
#define DEPTH 2
#define NENT (NQ_MAX * DEPTH)
#define SHM_ADDR ((void *)0x7e0000000000ULL)
#define PRIV_ADDR ((void *)0x7d0000000000ULL)

struct state {
	uint32_t magic;
	uint32_t nfds;
	uint32_t has_shm;
	uint32_t uring;
	struct fuse_init_in init_in;
	struct fuse_init_out init_out;
};

static struct state st;
static int fds[MAXFD];
static int nfds;
static int shmfd = -1;
static int backing = -1;
static const char *backing_path;
static int opt_hang, opt_resend, opt_uring, opt_shm, opt_allow_other, opt_batch = 1, opt_delay_us;
static uint64_t n_req, n_init, n_uring_req, n_read_req, n_resent;

static uint64_t now_us(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return (uint64_t)t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

static void ev(const char *fmt, ...)
{
	va_list ap;
	printf("EV %" PRIu64 " ", now_us());
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
	putchar('\n');
	fflush(stdout);
}

static void die(const char *what)
{
	ev("FATAL %s errno=%d %s", what, errno, strerror(errno));
	exit(1);
}

/* ---- fd passing ---- */

static int send_fds(int sock, const void *data, size_t len, const int *f, int n)
{
	char cbuf[CMSG_SPACE(sizeof(int) * MAXFD)];
	struct iovec iov = {(void *)data, len};
	struct msghdr m = {.msg_iov = &iov, .msg_iovlen = 1};
	if (n) {
		memset(cbuf, 0, sizeof(cbuf));
		m.msg_control = cbuf;
		m.msg_controllen = CMSG_SPACE(sizeof(int) * n);
		struct cmsghdr *c = CMSG_FIRSTHDR(&m);
		c->cmsg_level = SOL_SOCKET;
		c->cmsg_type = SCM_RIGHTS;
		c->cmsg_len = CMSG_LEN(sizeof(int) * n);
		memcpy(CMSG_DATA(c), f, sizeof(int) * n);
	}
	return sendmsg(sock, &m, 0) == (ssize_t)len ? 0 : -1;
}

static int recv_fds(int sock, void *data, size_t len, int *f, int *n)
{
	char cbuf[CMSG_SPACE(sizeof(int) * MAXFD)];
	struct iovec iov = {data, len};
	struct msghdr m = {.msg_iov = &iov, .msg_iovlen = 1, .msg_control = cbuf, .msg_controllen = sizeof(cbuf)};
	ssize_t r = recvmsg(sock, &m, MSG_WAITALL);
	if (r != (ssize_t)len)
		return -1;
	*n = 0;
	for (struct cmsghdr *c = CMSG_FIRSTHDR(&m); c; c = CMSG_NXTHDR(&m, c))
		if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS) {
			*n = (c->cmsg_len - CMSG_LEN(0)) / sizeof(int);
			memcpy(f, CMSG_DATA(c), sizeof(int) * *n);
		}
	return 0;
}

static int unix_connect(const char *path)
{
	int s = socket(AF_UNIX, SOCK_STREAM, 0);
	struct sockaddr_un a = {.sun_family = AF_UNIX};
	strncpy(a.sun_path, path, sizeof(a.sun_path) - 1);
	if (connect(s, (struct sockaddr *)&a, sizeof(a)) < 0)
		die("connect");
	return s;
}

static int unix_listen(const char *path)
{
	int s = socket(AF_UNIX, SOCK_STREAM, 0);
	struct sockaddr_un a = {.sun_family = AF_UNIX};
	strncpy(a.sun_path, path, sizeof(a.sun_path) - 1);
	unlink(path);
	if (bind(s, (struct sockaddr *)&a, sizeof(a)) < 0 || listen(s, 4) < 0)
		die("listen");
	return s;
}

/* ---- inode table: nodeid = backing st_ino ---- */

struct node {
	uint64_t ino;
	char name[256];
	int fd;
};
static struct node nodes[4096];
static int nnodes;

static struct node *node_add(uint64_t ino, const char *name)
{
	for (int i = 0; i < nnodes; i++)
		if (nodes[i].ino == ino)
			return &nodes[i];
	struct node *n = &nodes[nnodes++];
	n->ino = ino;
	snprintf(n->name, sizeof(n->name), "%s", name);
	n->fd = -1;
	return n;
}

static struct node *node_find(uint64_t ino)
{
	for (int i = 0; i < nnodes; i++)
		if (nodes[i].ino == ino)
			return &nodes[i];
	ev("RESCAN nodeid=%" PRIu64, ino);
	DIR *d = opendir(backing_path);
	struct dirent *e;
	while ((e = readdir(d)))
		if (e->d_name[0] != '.')
			node_add(e->d_ino, e->d_name);
	closedir(d);
	for (int i = 0; i < nnodes; i++)
		if (nodes[i].ino == ino)
			return &nodes[i];
	return NULL;
}

static void fill_attr(struct fuse_attr *a, const struct stat *s, uint64_t nodeid)
{
	memset(a, 0, sizeof(*a));
	a->ino = nodeid;
	a->size = s->st_size;
	a->blocks = s->st_blocks;
	a->atime = s->st_atime;
	a->mtime = s->st_mtime;
	a->ctime = s->st_ctime;
	a->mode = s->st_mode;
	a->nlink = s->st_nlink;
	a->uid = s->st_uid;
	a->gid = s->st_gid;
	a->blksize = 4096;
}

static int stat_node(uint64_t nodeid, struct stat *s)
{
	if (nodeid == FUSE_ROOT_ID)
		return fstat(backing, s) ? -errno : 0;
	struct node *n = node_find(nodeid);
	if (!n)
		return -ESTALE;
	return fstatat(backing, n->name, s, AT_SYMLINK_NOFOLLOW) ? -errno : 0;
}

static int node_fd(uint64_t nodeid)
{
	struct node *n = node_find(nodeid);
	if (!n)
		return -1;
	if (n->fd < 0)
		n->fd = openat(backing, n->name, O_RDWR | O_CLOEXEC);
	return n->fd;
}

/* ---- io_uring transport ---- */

struct stage {
	struct fuse_out_header oh;
	uint32_t payload_sz;
	uint32_t pad;
	char payload[MAX_WRITE];
};

// Per entry, in the memfd with --shm: 0 = the kernel owns it, 1 = a request was fetched and is
// being handled, 2 = its reply is complete in the stage and is being copied and committed.
struct ent_shared {
	uint32_t state;
	uint32_t pad;
	struct iovec iov[2];
};

struct shm_layout {
	struct fuse_uring_req_header hdr[NENT];
	struct ent_shared es[NENT];
	char payload[NENT][MAX_WRITE];
	struct stage stage[NENT];
};

static struct shm_layout *shm;
static int nq;
static int ring_fd = -1;
static unsigned *sq_tail, *sq_mask, *sq_array, *cq_head, *cq_tail, *cq_mask;
static char *sqes;
static struct io_uring_cqe *cqes;
static int cur_ent = -1;

static void uring_setup(void)
{
	struct io_uring_params p = {.flags = IORING_SETUP_SQE128};
	ring_fd = syscall(__NR_io_uring_setup, 256, &p);
	if (ring_fd < 0)
		die("io_uring_setup");
	size_t sqsz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
	size_t cqsz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
	size_t sz = sqsz > cqsz ? sqsz : cqsz;
	char *r = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_SQ_RING);
	if (r == MAP_FAILED)
		die("mmap ring");
	sq_tail = (unsigned *)(r + p.sq_off.tail);
	sq_mask = (unsigned *)(r + p.sq_off.ring_mask);
	sq_array = (unsigned *)(r + p.sq_off.array);
	cq_head = (unsigned *)(r + p.cq_off.head);
	cq_tail = (unsigned *)(r + p.cq_off.tail);
	cq_mask = (unsigned *)(r + p.cq_off.ring_mask);
	cqes = (struct io_uring_cqe *)(r + p.cq_off.cqes);
	sqes = mmap(NULL, p.sq_entries * 128, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,
		    IORING_OFF_SQES);
	if (sqes == MAP_FAILED)
		die("mmap sqes");
}

static void uring_submit(int e, uint32_t op, uint64_t commit_id)
{
	unsigned tail = *sq_tail, idx = tail & *sq_mask;
	struct io_uring_sqe *sqe = (void *)(sqes + idx * 128);
	memset(sqe, 0, 128);
	sqe->opcode = IORING_OP_URING_CMD;
	sqe->fd = fds[0];
	sqe->cmd_op = op;
	sqe->addr = (uint64_t)(uintptr_t)shm->es[e].iov;
	sqe->len = 2;
	sqe->user_data = e;
	struct fuse_uring_cmd_req *c = (void *)sqe->cmd;
	c->qid = e / DEPTH;
	c->commit_id = commit_id;
	sq_array[idx] = idx;
	atomic_store_explicit((_Atomic unsigned *)sq_tail, tail + 1, memory_order_release);
	if (syscall(__NR_io_uring_enter, ring_fd, 1, 0, 0, NULL, 0) != 1)
		die("io_uring_enter submit");
}

static void shm_map(int create)
{
	size_t sz = sizeof(struct shm_layout);
	void *addr = opt_shm ? SHM_ADDR : PRIV_ADDR;
	if (create) {
		shmfd = memfd_create("hofs-ring", MFD_CLOEXEC);
		if (shmfd < 0 || ftruncate(shmfd, sz) < 0)
			die("memfd");
	}
	shm = mmap(addr, sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED_NOREPLACE, shmfd, 0);
	if (shm != addr)
		die("mmap shm at fixed address");
}

static void ent_register(int e)
{
	struct ent_shared *s = &shm->es[e];
	memset(&shm->hdr[e], 0, sizeof(shm->hdr[e]));
	memset(&shm->stage[e].oh, 0, sizeof(shm->stage[e].oh));
	s->iov[0] = (struct iovec){&shm->hdr[e], sizeof(shm->hdr[e])};
	s->iov[1] = (struct iovec){shm->payload[e], MAX_WRITE};
	s->state = 0;
	uring_submit(e, FUSE_IO_URING_CMD_REGISTER, 0);
}

static void ent_commit_stage(int e)
{
	struct stage *g = &shm->stage[e];
	memcpy(shm->hdr[e].in_out, &g->oh, sizeof(g->oh));
	shm->hdr[e].ring_ent_in_out.payload_sz = g->payload_sz;
	memcpy(shm->payload[e], g->payload, g->payload_sz);
	uring_submit(e, FUSE_IO_URING_CMD_COMMIT_AND_FETCH, g->oh.unique);
	shm->es[e].state = 0;
}

static void reply(int fd, uint64_t unique, int err, const void *data, size_t len)
{
	if (err && err != -ENOENT)
		ev("REPLY_ERR unique=%#" PRIx64 " err=%d ent=%d", unique, err, cur_ent);
	if (cur_ent >= 0) {
		struct stage *g = &shm->stage[cur_ent];
		g->oh = (struct fuse_out_header){.len = sizeof(g->oh) + (err ? 0 : len), .error = err, .unique = unique};
		g->payload_sz = err ? 0 : len;
		memcpy(g->payload, data, g->payload_sz);
		shm->es[cur_ent].state = 2;
		ent_commit_stage(cur_ent);
		return;
	}
	struct fuse_out_header oh = {.len = sizeof(oh) + (err ? 0 : len), .error = err, .unique = unique};
	struct iovec iov[2] = {{&oh, sizeof(oh)}, {(void *)data, err ? 0 : len}};
	if (writev(fd, iov, 2) < 0)
		ev("REPLY_FAIL unique=%#" PRIx64 " errno=%d %s", unique, errno, strerror(errno));
}

static void send_resend(void)
{
	struct fuse_out_header oh = {.len = sizeof(oh), .error = FUSE_NOTIFY_RESEND, .unique = 0};
	ssize_t r = write(fds[0], &oh, sizeof(oh));
	ev("RESEND_SENT ret=%zd errno=%d", r, r < 0 ? errno : 0);
}

/* ---- protocol ---- */

static int entry_for(const char *name, struct fuse_entry_out *eo)
{
	struct stat s;
	if (fstatat(backing, name, &s, AT_SYMLINK_NOFOLLOW))
		return -errno;
	node_add(s.st_ino, name);
	memset(eo, 0, sizeof(*eo));
	eo->nodeid = s.st_ino;
	fill_attr(&eo->attr, &s, s.st_ino);
	return 0;
}

static void do_init(int fd, struct fuse_in_header *h, void *arg)
{
	size_t inlen = h->len - sizeof(*h);
	memset(&st.init_in, 0, sizeof(st.init_in));
	memcpy(&st.init_in, arg, inlen < sizeof(st.init_in) ? inlen : sizeof(st.init_in));
	uint64_t kflags = st.init_in.flags;
	if (kflags & FUSE_INIT_EXT)
		kflags |= (uint64_t)st.init_in.flags2 << 32;
	struct fuse_init_out *o = &st.init_out;
	memset(o, 0, sizeof(*o));
	o->major = FUSE_KERNEL_VERSION;
	o->minor = FUSE_KERNEL_MINOR_VERSION;
	o->max_readahead = st.init_in.max_readahead;
	o->max_background = 16;
	o->congestion_threshold = 12;
	o->max_write = MAX_WRITE;
	o->time_gran = 1;
	o->max_pages = MAX_WRITE / 4096;
	uint64_t f = FUSE_MAX_PAGES | FUSE_INIT_EXT | FUSE_PARALLEL_DIROPS;
	if (opt_uring)
		f |= FUSE_OVER_IO_URING;
	o->flags = (uint32_t)f;
	o->flags2 = (uint32_t)(f >> 32);
	st.magic = STATE_MAGIC;
	st.uring = opt_uring;
	n_init++;
	ev("INIT kernel=%u.%u flags=%#" PRIx64 " over_io_uring_offered=%d has_resend=%d reply_flags=%#" PRIx64
	   " max_write=%u",
	   st.init_in.major, st.init_in.minor, kflags, !!(kflags & FUSE_OVER_IO_URING), !!(kflags & FUSE_HAS_RESEND), f,
	   o->max_write);
	reply(fd, h->unique, 0, o, sizeof(*o));
}

static void handle(int fd, char *buf, ssize_t len)
{
	struct fuse_in_header *h = (void *)buf;
	void *arg = buf + sizeof(*h);
	uint64_t u = h->unique;
	int resent = !!(u & FUSE_UNIQUE_RESEND);
	n_req++;
	if (resent) {
		n_resent++;
		ev("GOT_RESENT unique=%#" PRIx64 " op=%u nodeid=%" PRIu64, u, h->opcode, h->nodeid);
	}
	(void)len;
	switch (h->opcode) {
	case FUSE_INIT:
		do_init(fd, h, arg);
		return;
	case FUSE_LOOKUP: {
		const char *name = arg;
		if (opt_hang && !strncmp(name, "hang", 4)) {
			ev("HUNG unique=%#" PRIx64 " op=LOOKUP name=%s ent=%d", u, name, cur_ent);
			return;
		}
		struct fuse_entry_out eo;
		int e = entry_for(name, &eo);
		reply(fd, u, e, &eo, sizeof(eo));
		return;
	}
	case FUSE_FORGET:
	case FUSE_BATCH_FORGET:
		return;
	case FUSE_INTERRUPT:
		ev("INTERRUPT for=%#" PRIx64, ((struct fuse_interrupt_in *)arg)->unique);
		return;
	case FUSE_GETATTR:
	case FUSE_SETATTR: {
		struct stat s;
		int e = stat_node(h->nodeid, &s);
		if (!e && h->opcode == FUSE_SETATTR) {
			struct fuse_setattr_in *sa = arg;
			if (sa->valid & FATTR_SIZE) {
				int nf = node_fd(h->nodeid);
				e = nf < 0 || ftruncate(nf, sa->size) ? -EIO : 0;
				if (!e)
					e = stat_node(h->nodeid, &s);
			}
		}
		struct fuse_attr_out ao = {0};
		if (!e)
			fill_attr(&ao.attr, &s, h->nodeid);
		reply(fd, u, e, &ao, sizeof(ao));
		return;
	}
	case FUSE_OPEN:
	case FUSE_OPENDIR: {
		struct fuse_open_out oo = {.fh = h->nodeid, .open_flags = h->opcode == FUSE_OPEN ? FOPEN_DIRECT_IO : 0};
		struct stat s;
		int e = stat_node(h->nodeid, &s);
		reply(fd, u, e, &oo, sizeof(oo));
		return;
	}
	case FUSE_READ: {
		struct fuse_read_in *ri = arg;
		static char data[MAX_WRITE];
		size_t sz = ri->size > sizeof(data) ? sizeof(data) : ri->size;
		int nf = node_fd(ri->fh);
		ssize_t r = nf < 0 ? -1 : pread(nf, data, sz, ri->offset);
		reply(fd, u, r < 0 ? -EIO : 0, data, r < 0 ? 0 : r);
		return;
	}
	case FUSE_WRITE: {
		struct fuse_write_in *wi = arg;
		int nf = node_fd(wi->fh);
		ssize_t r = nf < 0 ? -1 : pwrite(nf, wi + 1, wi->size, wi->offset);
		struct fuse_write_out wo = {.size = r < 0 ? 0 : r};
		reply(fd, u, r < 0 ? -EIO : 0, &wo, sizeof(wo));
		return;
	}
	case FUSE_READDIR: {
		struct fuse_read_in *ri = arg;
		static char out[65536];
		size_t pos = 0, max = ri->size < sizeof(out) ? ri->size : sizeof(out);
		DIR *d = opendir(backing_path);
		struct dirent *e;
		uint64_t off = 0;
		while ((e = readdir(d))) {
			off++;
			if (off <= ri->offset)
				continue;
			size_t nl = strlen(e->d_name);
			size_t rec = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + nl);
			if (pos + rec > max)
				break;
			struct fuse_dirent *de = (void *)(out + pos);
			memset(de, 0, rec);
			de->ino = e->d_ino;
			de->off = off;
			de->namelen = nl;
			de->type = e->d_type;
			memcpy(de->name, e->d_name, nl);
			pos += rec;
			if (e->d_name[0] != '.')
				node_add(e->d_ino, e->d_name);
		}
		closedir(d);
		reply(fd, u, 0, out, pos);
		return;
	}
	case FUSE_STATFS: {
		struct fuse_statfs_out so = {.st = {.bsize = 4096, .frsize = 4096, .namelen = 255}};
		reply(fd, u, 0, &so, sizeof(so));
		return;
	}
	case FUSE_RELEASE:
	case FUSE_RELEASEDIR:
	case FUSE_FLUSH:
	case FUSE_FSYNC:
	case FUSE_FSYNCDIR:
	case FUSE_DESTROY:
		reply(fd, u, 0, NULL, 0);
		return;
	default:
		reply(fd, u, -ENOSYS, NULL, 0);
	}
}

// Size of the per-opcode header the kernel puts in op_in; everything after it is in the payload.
static size_t op_size(uint32_t op)
{
	switch (op) {
	case FUSE_GETATTR:
		return sizeof(struct fuse_getattr_in);
	case FUSE_SETATTR:
		return sizeof(struct fuse_setattr_in);
	case FUSE_OPEN:
	case FUSE_OPENDIR:
		return sizeof(struct fuse_open_in);
	case FUSE_READ:
	case FUSE_READDIR:
		return sizeof(struct fuse_read_in);
	case FUSE_WRITE:
		return sizeof(struct fuse_write_in);
	case FUSE_RELEASE:
	case FUSE_RELEASEDIR:
		return sizeof(struct fuse_release_in);
	case FUSE_FLUSH:
		return sizeof(struct fuse_flush_in);
	case FUSE_FSYNC:
	case FUSE_FSYNCDIR:
		return sizeof(struct fuse_fsync_in);
	case FUSE_MKDIR:
		return sizeof(struct fuse_mkdir_in);
	case FUSE_ACCESS:
		return sizeof(struct fuse_access_in);
	default:
		return 0;
	}
}

static void handle_ent(int e)
{
	static char buf[BUFSZ];
	struct fuse_in_header *h = (void *)shm->hdr[e].in_out;
	size_t osz = op_size(h->opcode);
	uint32_t psz = shm->hdr[e].ring_ent_in_out.payload_sz;
	if (psz > MAX_WRITE)
		psz = MAX_WRITE;
	memcpy(buf, h, sizeof(*h));
	memcpy(buf + sizeof(*h), shm->hdr[e].op_in, osz);
	memcpy(buf + sizeof(*h) + osz, shm->payload[e], psz);
	buf[sizeof(*h) + osz + psz] = 0;
	shm->es[e].state = 1;
	n_uring_req++;
	if (opt_delay_us)
		usleep(opt_delay_us);
	cur_ent = e;
	handle(fds[0], buf, sizeof(*h) + osz + psz);
	cur_ent = -1;
}

static void uring_reap(void)
{
	for (;;) {
		unsigned head = *cq_head;
		if (head == atomic_load_explicit((_Atomic unsigned *)cq_tail, memory_order_acquire))
			return;
		struct io_uring_cqe *c = &cqes[head & *cq_mask];
		int e = (int)c->user_data, res = c->res;
		atomic_store_explicit((_Atomic unsigned *)cq_head, head + 1, memory_order_release);
		if (res < 0) {
			ev("URING_CQE_ERR ent=%d res=%d %s", e, res, strerror(-res));
			if (res == -ENOENT)
				ent_register(e);
			continue;
		}
		handle_ent(e);
	}
}

static void uring_register_all(void)
{
	for (int e = 0; e < nq * DEPTH; e++)
		ent_register(e);
	ev("URING_REGISTERED queues=%d depth=%d", nq, DEPTH);
}

// A successor adopts the dead daemon's entries. An entry whose header holds a request other than
// the one last replied to was fetched (or delivered with its completion lost in the old ring) and
// is handled again, as RESEND would; an entry whose reply was staged is committed from its stage
// (an ENOENT then means the old daemon's commit had gone through). Every other entry is
// registered afresh at the same buffers.
static void uring_adopt(void)
{
	int fetched = 0, unreaped = 0, staged = 0, fresh = 0;
	for (int e = 0; e < nq * DEPTH; e++) {
		struct fuse_in_header *h = (void *)shm->hdr[e].in_out;
		uint32_t s = shm->es[e].state;
		if (h->unique && h->unique != shm->stage[e].oh.unique) {
			s == 1 ? fetched++ : unreaped++;
			ev("ADOPT_FETCHED ent=%d state=%u unique=%#" PRIx64 " op=%u", e, s, h->unique, h->opcode);
			handle_ent(e);
		} else if (s == 2) {
			staged++;
			ev("ADOPT_STAGED ent=%d unique=%#" PRIx64, e, shm->stage[e].oh.unique);
			ent_commit_stage(e);
		} else {
			fresh++;
			ent_register(e);
		}
	}
	ev("URING_ADOPTED fetched=%d unreaped=%d staged=%d fresh=%d", fetched, unreaped, staged, fresh);
}

// A request queued on a ring queue while it had no available entry is only dispatched after a
// later commit on that queue, so the successor makes one request from each CPU.
static void kick(const char *mnt)
{
	for (int q = 0; q < nq; q++) {
		pid_t p = fork();
		if (p == 0) {
			cpu_set_t cs;
			CPU_ZERO(&cs);
			CPU_SET(q, &cs);
			sched_setaffinity(0, sizeof(cs), &cs);
			struct stat s;
			_exit(stat(mnt, &s) ? 1 : 0);
		}
	}
	ev("KICK_STARTED queues=%d", nq);
}

/* ---- holder and commands ---- */

static void put_to_holder(const char *path)
{
	int f[MAXFD], n = 0;
	for (int i = 0; i < nfds; i++)
		f[n++] = fds[i];
	st.nfds = nfds;
	st.has_shm = opt_shm;
	if (opt_shm)
		f[n++] = shmfd;
	int s = unix_connect(path);
	if (write(s, "P", 1) != 1 || send_fds(s, &st, sizeof(st), f, n) < 0)
		die("put");
	char ack;
	if (read(s, &ack, 1) != 1)
		die("no ack");
	close(s);
	ev("PUT_TO_HOLDER nfds=%d", n);
}

static void cmd(char *line)
{
	char *a = strtok(line, " \n"), *b = strtok(NULL, " \n");
	if (!a)
		return;
	if (!strcmp(a, "ping"))
		ev("PONG reqs=%" PRIu64 " read_reqs=%" PRIu64 " uring_reqs=%" PRIu64 " resent=%" PRIu64 " inits=%" PRIu64,
		   n_req, n_read_req, n_uring_req, n_resent, n_init);
	else if (!strcmp(a, "resend"))
		send_resend();
	else if (!strcmp(a, "register"))
		opt_shm ? uring_adopt() : uring_register_all();
	else if (!strcmp(a, "kick") && b)
		kick(b);
	else if (!strcmp(a, "exit")) {
		ev("EXIT");
		exit(0);
	}
}

static void serve(void)
{
	static char bufs[MAXBATCH][BUFSZ];
	static ssize_t lens[MAXBATCH];
	char line[512];
	size_t lp = 0;
	int stdin_closed = 0;
	fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK);
	for (;;) {
		struct pollfd p[3] = {{.fd = stdin_closed ? -1 : 0, .events = POLLIN},
				      {.fd = fds[0], .events = POLLIN},
				      {.fd = ring_fd, .events = POLLIN}};
		if (ring_fd >= 0)
			syscall(__NR_io_uring_enter, ring_fd, 0, 0, IORING_ENTER_GETEVENTS, NULL, 0);
		if (ring_fd >= 0)
			uring_reap();
		int r = poll(p, 3, ring_fd >= 0 ? 20 : -1);
		if (r < 0 && errno != EINTR)
			die("poll");
		if (p[0].revents) {
			ssize_t k = read(0, line + lp, sizeof(line) - lp - 1);
			if (k <= 0)
				stdin_closed = 1;
			else {
				lp += k;
				line[lp] = 0;
				char *nl;
				while ((nl = strchr(line, '\n'))) {
					*nl = 0;
					cmd(line);
					memmove(line, nl + 1, strlen(nl + 1) + 1);
					lp = strlen(line);
				}
			}
		}
		if (p[1].revents & (POLLERR | POLLHUP)) {
			ev("DEV_GONE revents=%#x", p[1].revents);
			exit(0);
		}
		if (p[1].revents & POLLIN) {
			int nb = 0;
			while (nb < opt_batch) {
				ssize_t k = read(fds[0], bufs[nb], BUFSZ);
				if (k < 0) {
					if (errno == EAGAIN || errno == EINTR || errno == ENOENT)
						break;
					ev("DEV_READ_ERR errno=%d %s", errno, strerror(errno));
					exit(2);
				}
				lens[nb++] = k;
			}
			if (nb && opt_delay_us)
				usleep(opt_delay_us);
			for (int i = 0; i < nb; i++) {
				n_read_req++;
				handle(fds[0], bufs[i], lens[i]);
			}
		}
	}
}

static void hold(const char *path)
{
	int l = unix_listen(path), hf[MAXFD], hn = 0;
	struct state hs;
	ev("HOLDER_LISTENING pid=%d", getpid());
	for (;;) {
		struct pollfd p[2] = {{.fd = l, .events = POLLIN}, {.fd = hn ? hf[0] : -1, .events = 0}};
		poll(p, 2, -1);
		if (p[1].revents & (POLLERR | POLLHUP)) {
			ev("HOLDER_CONN_DEAD revents=%#x dropping %d fds", p[1].revents, hn);
			for (int i = 0; i < hn; i++)
				close(hf[i]);
			hn = 0;
		}
		if (!(p[0].revents & POLLIN))
			continue;
		int s = accept(l, NULL, NULL);
		char op;
		if (read(s, &op, 1) != 1) {
			close(s);
			continue;
		}
		if (op == 'P') {
			for (int i = 0; i < hn; i++)
				close(hf[i]);
			if (recv_fds(s, &hs, sizeof(hs), hf, &hn) < 0)
				die("holder recv");
			ev("HOLDER_PUT nfds=%d", hn);
		} else {
			send_fds(s, &hs, sizeof(hs), hf, hn);
			ev("HOLDER_GET nfds=%d", hn);
		}
		if (write(s, "k", 1) != 1)
			ev("HOLDER_ACK_FAIL");
		close(s);
	}
}

static const char *opt_val(int argc, char **argv, const char *o)
{
	for (int i = 1; i < argc - 1; i++)
		if (!strcmp(argv[i], o))
			return argv[i + 1];
	return NULL;
}

static int has_opt(int argc, char **argv, const char *o)
{
	for (int i = 1; i < argc; i++)
		if (!strcmp(argv[i], o))
			return 1;
	return 0;
}

static void open_backing(const char *p)
{
	backing_path = p;
	backing = open(p, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (backing < 0)
		die("backing");
}

static int count_possible_cpus(void)
{
	int lo, hi;
	FILE *f = fopen("/sys/devices/system/cpu/possible", "r");
	if (!f || fscanf(f, "%d-%d", &lo, &hi) != 2)
		die("cpu/possible");
	fclose(f);
	return hi + 1;
}

int main(int argc, char **argv)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	signal(SIGCHLD, SIG_IGN);
	if (argc < 3) {
		fprintf(stderr, "see header of hofs.c for usage\n");
		return 2;
	}
	const char *mode = argv[1];
	ev("START mode=%s pid=%d uid=%d", mode, getpid(), getuid());
	if (!strcmp(mode, "hold")) {
		hold(argv[2]);
		return 0;
	}
	opt_hang = has_opt(argc, argv, "--hang");
	opt_resend = has_opt(argc, argv, "--resend");
	opt_uring = has_opt(argc, argv, "--uring");
	opt_shm = has_opt(argc, argv, "--shm");
	opt_allow_other = has_opt(argc, argv, "--allow-other");
	if (opt_val(argc, argv, "--batch"))
		opt_batch = atoi(opt_val(argc, argv, "--batch"));
	if (opt_val(argc, argv, "--delay-us"))
		opt_delay_us = atoi(opt_val(argc, argv, "--delay-us"));
	nq = count_possible_cpus();
	if (nq > NQ_MAX)
		die("too many cpus");
	if (!strcmp(mode, "mount")) {
		open_backing(argv[3]);
		int fd = open("/dev/fuse", O_RDWR | O_CLOEXEC);
		if (fd < 0)
			die("open /dev/fuse");
		char opts[256];
		snprintf(opts, sizeof(opts), "fd=%d,rootmode=40000,user_id=0,group_id=0%s", fd,
			 opt_allow_other ? ",allow_other" : "");
		if (mount("hofs", argv[2], "fuse.hofs", MS_NOSUID | MS_NODEV, opts) < 0)
			die("mount");
		fds[nfds++] = fd;
		ev("MOUNTED fd=%d opts=%s", fd, opts);
		static char buf[BUFSZ];
		ssize_t r = read(fd, buf, sizeof(buf));
		if (r <= 0)
			die("read INIT");
		handle(fd, buf, r);
		if (opt_uring) {
			shm_map(1);
			uring_setup();
			uring_register_all();
		}
		put_to_holder(opt_val(argc, argv, "--holder"));
		serve();
	}
	if (!strcmp(mode, "get")) {
		open_backing(argv[3]);
		int f[MAXFD], n = 0;
		int s = unix_connect(argv[2]);
		if (write(s, "G", 1) != 1 || recv_fds(s, &st, sizeof(st), f, &n) < 0)
			die("get");
		close(s);
		if (st.magic != STATE_MAGIC)
			die("bad state");
		nfds = st.nfds;
		for (int i = 0; i < nfds; i++)
			fds[i] = f[i];
		if (st.has_shm)
			shmfd = f[nfds];
		ev("TAKEOVER nfds=%d shm=%d uring=%u kernel=%u.%u max_write=%u", nfds, shmfd >= 0, st.uring,
		   st.init_in.major, st.init_in.minor, st.init_out.max_write);
		if (opt_uring) {
			if (opt_shm && shmfd < 0)
				die("no shm from holder");
			shm_map(!opt_shm);
			uring_setup();
			if (!has_opt(argc, argv, "--no-register"))
				opt_shm ? uring_adopt() : uring_register_all();
		}
		if (opt_resend)
			send_resend();
		if (opt_val(argc, argv, "--kick"))
			kick(opt_val(argc, argv, "--kick"));
		ev("SERVING");
		serve();
	}
	return 2;
}
