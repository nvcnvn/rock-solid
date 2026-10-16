// Raw /dev/fuse daemon for the restart spike: mounts through fusermount3, speaks the protocol by hand,
// and hands its /dev/fuse fds to a successor (SCM_RIGHTS, execve, an fd-holder or the systemd fdstore).
// A flat passthrough of one backing directory; nodeid = backing st_ino, fh = nodeid, so a successor
// rebuilds every handle from the backing directory alone.
//
//   rawfs mount <mnt> <backing> [--hang] [--auto-unmount] [--fdstore] [--timeout S]
//   rawfs takeover <listen-sock> <backing> [--resend] [--hang]
//   rawfs get <holder-sock> <backing> [--resend] [--hang]
//   rawfs adopt <backing> [--resend]            (state and fds from RAWFS_STATE / RAWFS_FDS, after execve)
//   rawfs hold <listen-sock>
//
// stdin commands: handoff <sock> | put <sock> | reexec | exec <prog> [args] | clone | handoff-first <sock>
//                 resend | exit | ping
// stdout: one event per line, "EV <CLOCK_MONOTONIC us> <what> ...".
// --hang: LOOKUP of "hang*" and MKDIR of "mkhang*" (applied first) are read and never answered.
// env: RAWFS_TIMEOUT=S negotiates FUSE_REQUEST_TIMEOUT, RAWFS_NO_PARALLEL_DIROPS=1 leaves out
//      FUSE_PARALLEL_DIROPS, RAWFS_STRIP_RESEND=1 clears bit 63 in replies, RAWFS_LOGPID=1 logs requesters.
#define _GNU_SOURCE
#include <linux/fuse.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_WRITE (128 * 1024)
#define BUFSZ (MAX_WRITE + 8192)
#define MAXFD 8
#define STATE_MAGIC 0x52535446u

struct state {
	uint32_t magic;
	uint32_t nfds;
	uint32_t has_commfd;
	uint32_t pad;
	struct fuse_init_in init_in;
	struct fuse_init_out init_out;
};

static struct state st;
static int fds[MAXFD];
static int nfds;
static int commfd = -1;
static int read_from = -1;
static int backing = -1;
static const char *backing_path;
static int opt_hang, opt_resend;
static uint64_t n_req, n_init;
static char *self_argv0;

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

static void pack_fds(int *out, int *n)
{
	*n = 0;
	for (int i = 0; i < nfds; i++)
		out[(*n)++] = fds[i];
	st.has_commfd = commfd >= 0;
	if (commfd >= 0)
		out[(*n)++] = commfd;
	st.nfds = nfds;
}

static void unpack_fds(const int *in, int n)
{
	if (st.magic != STATE_MAGIC)
		die("bad state");
	nfds = st.nfds;
	for (int i = 0; i < nfds; i++)
		fds[i] = in[i];
	commfd = st.has_commfd && n > nfds ? in[nfds] : -1;
}

/* ---- mounting ---- */

static int do_mount(const char *mnt, int auto_unmount)
{
	int sv[2];
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0)
		die("socketpair");
	char opts[256];
	snprintf(opts, sizeof(opts), "fsname=rawfs,subtype=rawfs%s", auto_unmount ? ",auto_unmount" : "");
	pid_t p = fork();
	if (p == 0) {
		char env[32];
		snprintf(env, sizeof(env), "%d", sv[0]);
		setenv("_FUSE_COMMFD", env, 1);
		close(sv[1]);
		execlp("fusermount3", "fusermount3", "-o", opts, "--", mnt, (char *)NULL);
		_exit(127);
	}
	close(sv[0]);
	ev("FUSERMOUNT pid=%d", p);
	int f[MAXFD], n = 0;
	char c;
	if (recv_fds(sv[1], &c, 1, f, &n) < 0 || n != 1)
		die("fusermount3 sent no fd");
	if (auto_unmount)
		commfd = sv[1];
	else {
		close(sv[1]);
		waitpid(p, NULL, 0);
	}
	return f[0];
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

/* ---- protocol ---- */

static void reply(int fd, uint64_t unique, int err, const void *data, size_t len)
{
	if (getenv("RAWFS_STRIP_RESEND"))
		unique &= ~FUSE_UNIQUE_RESEND;
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
	o->flags = FUSE_MAX_PAGES | FUSE_INIT_EXT;
	if (!getenv("RAWFS_NO_PARALLEL_DIROPS"))
		o->flags |= FUSE_PARALLEL_DIROPS;
	char *t = getenv("RAWFS_TIMEOUT");
	if (t && (kflags & FUSE_REQUEST_TIMEOUT)) {
		o->flags2 |= FUSE_REQUEST_TIMEOUT >> 32;
		o->request_timeout = atoi(t);
	}
	st.magic = STATE_MAGIC;
	n_init++;
	ev("INIT kernel=%u.%u flags=%#" PRIx64 " has_resend=%d request_timeout_offered=%d reply_flags=%#" PRIx64
	   " max_write=%u timeout=%u",
	   st.init_in.major, st.init_in.minor, kflags, !!(kflags & FUSE_HAS_RESEND), !!(kflags & FUSE_REQUEST_TIMEOUT),
	   o->flags | (uint64_t)o->flags2 << 32, o->max_write, o->request_timeout);
	reply(fd, h->unique, 0, o, sizeof(*o));
}

static int is_hang(const char *name, const char *prefix)
{
	return opt_hang && strncmp(name, prefix, strlen(prefix)) == 0;
}

static void handle(int fd, char *buf, ssize_t len)
{
	struct fuse_in_header *h = (void *)buf;
	void *arg = buf + sizeof(*h);
	uint64_t u = h->unique;
	int resent = !!(u & FUSE_UNIQUE_RESEND);
	n_req++;
	if (getenv("RAWFS_LOGPID"))
		ev("REQ op=%u pid=%u", h->opcode, h->pid);
	if (resent)
		ev("GOT_RESENT unique=%#" PRIx64 " op=%u nodeid=%" PRIu64, u, h->opcode, h->nodeid);
	(void)len;
	switch (h->opcode) {
	case FUSE_INIT:
		do_init(fd, h, arg);
		return;
	case FUSE_LOOKUP: {
		const char *name = arg;
		if (is_hang(name, "hang")) {
			ev("HUNG unique=%#" PRIx64 " op=LOOKUP name=%s", u, name);
			return;
		}
		struct fuse_entry_out eo;
		int e = entry_for(name, &eo);
		reply(fd, u, e, &eo, sizeof(eo));
		return;
	}
	case FUSE_MKDIR: {
		struct fuse_mkdir_in *mi = arg;
		const char *name = (char *)(mi + 1);
		int e = mkdirat(backing, name, mi->mode & 07777) ? -errno : 0;
		if (is_hang(name, "mkhang")) {
			ev("HUNG unique=%#" PRIx64 " op=MKDIR name=%s applied=%d", u, name, e);
			return;
		}
		struct fuse_entry_out eo;
		if (!e)
			e = entry_for(name, &eo);
		if (e)
			ev("MKDIR_ERR unique=%#" PRIx64 " name=%s err=%d resent=%d", u, name, e, resent);
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

/* ---- handoff ---- */

static void state_hex(char *out)
{
	const unsigned char *p = (const unsigned char *)&st;
	for (size_t i = 0; i < sizeof(st); i++)
		sprintf(out + 2 * i, "%02x", p[i]);
}

static void state_unhex(const char *in)
{
	unsigned char *p = (unsigned char *)&st;
	for (size_t i = 0; i < sizeof(st); i++)
		sscanf(in + 2 * i, "%2hhx", &p[i]);
}

static void env_fds(void)
{
	char list[64] = "";
	for (int i = 0; i < nfds; i++)
		sprintf(list + strlen(list), "%s%d", i ? "," : "", fds[i]);
	if (commfd >= 0)
		sprintf(list + strlen(list), ",c%d", commfd);
	setenv("RAWFS_FDS", list, 1);
	static char hex[2 * sizeof(struct state) + 1];
	st.nfds = nfds;
	st.has_commfd = commfd >= 0;
	state_hex(hex);
	setenv("RAWFS_STATE", hex, 1);
}

static void cmd(char *line)
{
	char *a = strtok(line, " \n"), *b = strtok(NULL, " \n");
	if (!a)
		return;
	if (!strcmp(a, "ping")) {
		ev("PONG reqs=%" PRIu64 " inits=%" PRIu64 " nfds=%d", n_req, n_init, nfds);
	} else if (!strcmp(a, "handoff") || !strcmp(a, "handoff-first") || !strcmp(a, "put")) {
		int f[MAXFD], n;
		pack_fds(f, &n);
		if (!strcmp(a, "handoff-first")) {
			n = 1;
			st.nfds = 1;
			st.has_commfd = 0;
		}
		ev("HANDOFF_START to=%s nfds=%d", b, n);
		int s = unix_connect(b);
		if (!strcmp(a, "put") && write(s, "P", 1) != 1)
			die("put");
		if (send_fds(s, &st, sizeof(st), f, n) < 0)
			die("send_fds");
		char ack;
		if (read(s, &ack, 1) != 1)
			die("no ack");
		close(s);
		ev("HANDOFF_SENT");
		if (strcmp(a, "put")) {
			ev("EXIT");
			exit(0);
		}
	} else if (!strcmp(a, "reexec") || !strcmp(a, "exec")) {
		env_fds();
		ev("EXEC_START");
		if (!strcmp(a, "reexec"))
			execl("/proc/self/exe", self_argv0, "adopt", backing_path, opt_resend ? "--resend" : "", (char *)NULL);
		else {
			char *argv[16] = {b};
			int i = 1;
			char *x;
			while (i < 15 && (x = strtok(NULL, " \n")))
				argv[i++] = x;
			execv(b, argv);
		}
		die("exec");
	} else if (!strcmp(a, "clone")) {
		int c = open("/dev/fuse", O_RDWR);
		uint32_t master = fds[0];
		if (c < 0 || ioctl(c, FUSE_DEV_IOC_CLONE, &master) < 0)
			die("clone");
		fds[nfds++] = c;
		read_from = nfds - 1;
		ev("CLONED fd=%d reading_only_from_clone", c);
	} else if (!strcmp(a, "resend")) {
		send_resend();
	} else if (!strcmp(a, "exit")) {
		ev("EXIT");
		exit(0);
	}
}

static int stdin_closed;

static void serve(void)
{
	static char buf[BUFSZ];
	char line[512];
	size_t lp = 0;
	if (opt_resend)
		send_resend();
	for (;;) {
		struct pollfd p[MAXFD + 1];
		int map[MAXFD + 1], np = 0;
		p[np] = (struct pollfd){.fd = stdin_closed ? -1 : 0, .events = POLLIN};
		map[np++] = -1;
		for (int i = 0; i < nfds; i++)
			if (read_from < 0 || read_from == i) {
				p[np] = (struct pollfd){.fd = fds[i], .events = POLLIN};
				map[np++] = i;
			}
		if (poll(p, np, -1) < 0) {
			if (errno == EINTR)
				continue;
			die("poll");
		}
		for (int k = 0; k < np; k++) {
			if (!p[k].revents)
				continue;
			if (map[k] < 0) {
				ssize_t r = read(0, line + lp, sizeof(line) - lp - 1);
				if (r <= 0) {
					stdin_closed = 1;
					continue;
				}
				lp += r;
				line[lp] = 0;
				char *nl;
				while ((nl = strchr(line, '\n'))) {
					*nl = 0;
					cmd(line);
					memmove(line, nl + 1, strlen(nl + 1) + 1);
					lp = strlen(line);
				}
				continue;
			}
			int fd = fds[map[k]];
			ssize_t r = read(fd, buf, sizeof(buf));
			if (r < 0) {
				if (errno == EINTR || errno == EAGAIN || errno == ENOENT)
					continue;
				ev("DEV_READ_ERR errno=%d %s", errno, strerror(errno));
				exit(2);
			}
			handle(fd, buf, r);
		}
	}
}

/* ---- systemd fdstore (sd_pid_notify_with_fds by hand) ---- */

static void sd_notify_fds(const char *msg, const int *f, int n)
{
	const char *p = getenv("NOTIFY_SOCKET");
	if (!p)
		return;
	int s = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
	struct sockaddr_un a = {.sun_family = AF_UNIX};
	strncpy(a.sun_path, p, sizeof(a.sun_path) - 1);
	if (a.sun_path[0] == '@')
		a.sun_path[0] = 0;
	socklen_t al = offsetof(struct sockaddr_un, sun_path) + strlen(p);
	char cbuf[CMSG_SPACE(sizeof(int) * MAXFD)] = {0};
	struct iovec iov = {(void *)msg, strlen(msg)};
	struct msghdr m = {.msg_name = &a, .msg_namelen = al, .msg_iov = &iov, .msg_iovlen = 1};
	if (n) {
		m.msg_control = cbuf;
		m.msg_controllen = CMSG_SPACE(sizeof(int) * n);
		struct cmsghdr *c = CMSG_FIRSTHDR(&m);
		c->cmsg_level = SOL_SOCKET;
		c->cmsg_type = SCM_RIGHTS;
		c->cmsg_len = CMSG_LEN(sizeof(int) * n);
		memcpy(CMSG_DATA(c), f, sizeof(int) * n);
	}
	ssize_t r = sendmsg(s, &m, 0);
	ev("SD_NOTIFY %s ret=%zd", strchr(msg, '\n') ? "fdstore" : msg, r);
	close(s);
}

static void fdstore_put(void)
{
	int mfd = memfd_create("rawfs-state", MFD_CLOEXEC);
	st.nfds = nfds;
	if (write(mfd, &st, sizeof(st)) != sizeof(st))
		die("memfd write");
	sd_notify_fds("FDSTORE=1\nFDNAME=fuse", fds, nfds);
	sd_notify_fds("FDSTORE=1\nFDNAME=state", &mfd, 1);
	close(mfd);
}

static int fdstore_adopt(void)
{
	char *n = getenv("LISTEN_FDS"), *names = getenv("LISTEN_FDNAMES");
	if (!n || !names)
		return 0;
	ev("LISTEN_FDS=%s LISTEN_FDNAMES=%s", n, names);
	char *save, *tok = strtok_r(names, ":", &save);
	int fd = 3, sfd = -1;
	nfds = 0;
	for (; tok; tok = strtok_r(NULL, ":", &save), fd++) {
		if (!strcmp(tok, "fuse"))
			fds[nfds++] = fd;
		else if (!strcmp(tok, "state"))
			sfd = fd;
	}
	if (!nfds || sfd < 0 || pread(sfd, &st, sizeof(st), 0) != sizeof(st))
		die("fdstore adopt");
	close(sfd);
	return 1;
}

/* ---- fd holder ---- */

static void hold(const char *path)
{
	int l = unix_listen(path), hf[MAXFD], hn = 0;
	struct state hs;
	ev("HOLDER_LISTENING");
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

static void parse_opts(int argc, char **argv, int from)
{
	for (int i = from; i < argc; i++) {
		if (!strcmp(argv[i], "--hang"))
			opt_hang = 1;
		else if (!strcmp(argv[i], "--resend"))
			opt_resend = 1;
	}
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

int main(int argc, char **argv)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	self_argv0 = argv[0];
	if (argc < 3) {
		fprintf(stderr, "see header of rawfs.c for usage\n");
		return 2;
	}
	const char *mode = argv[1];
	ev("START mode=%s pid=%d", mode, getpid());
	if (!strcmp(mode, "hold")) {
		hold(argv[2]);
		return 0;
	}
	if (!strcmp(mode, "mount")) {
		open_backing(argv[3]);
		parse_opts(argc, argv, 4);
		if (fdstore_adopt()) {
			ev("ADOPTED_FROM_FDSTORE nfds=%d", nfds);
		} else {
			fds[nfds++] = do_mount(argv[2], has_opt(argc, argv, "--auto-unmount"));
			ev("MOUNTED fd=%d", fds[0]);
			static char buf[BUFSZ];
			ssize_t r = read(fds[0], buf, sizeof(buf));
			if (r <= 0)
				die("read INIT");
			handle(fds[0], buf, r);
			if (has_opt(argc, argv, "--fdstore"))
				fdstore_put();
		}
		sd_notify_fds("READY=1", NULL, 0);
		serve();
	}
	int f[MAXFD], n = 0;
	if (!strcmp(mode, "takeover")) {
		open_backing(argv[3]);
		parse_opts(argc, argv, 4);
		int l = unix_listen(argv[2]);
		ev("LISTENING");
		int s = accept(l, NULL, NULL);
		if (recv_fds(s, &st, sizeof(st), f, &n) < 0)
			die("recv");
		if (write(s, "k", 1) != 1)
			die("ack");
		close(s);
		close(l);
	} else if (!strcmp(mode, "get")) {
		open_backing(argv[3]);
		parse_opts(argc, argv, 4);
		int s = unix_connect(argv[2]);
		if (write(s, "G", 1) != 1 || recv_fds(s, &st, sizeof(st), f, &n) < 0)
			die("get");
		close(s);
	} else if (!strcmp(mode, "adopt")) {
		open_backing(argv[2]);
		parse_opts(argc, argv, 3);
		state_unhex(getenv("RAWFS_STATE"));
		char *list = getenv("RAWFS_FDS"), *tok, *save;
		for (tok = strtok_r(list, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
			f[n++] = atoi(tok[0] == 'c' ? tok + 1 : tok);
	} else {
		return 2;
	}
	unpack_fds(f, n);
	ev("TAKEOVER nfds=%d commfd=%d kernel=%u.%u max_write=%u", nfds, commfd, st.init_in.major, st.init_in.minor,
	   st.init_out.max_write);
	serve();
}
