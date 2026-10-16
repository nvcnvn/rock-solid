// libfuse 3.18 low-level daemon for the restart spike: the same flat passthrough as rawfs.c
// (nodeid = backing st_ino, fh = nodeid), used to check what libfuse allows on an fd that the
// kernel already INITed, and as the one-process-per-mount baseline for memory and threads.
//
//   llfs mount <mnt> <backing>         own fusermount3 mount + custom io that records INIT; a LOOKUP of
//                                      "handoff_reexec" leaves the loop and re-execs into "llfs adopt"
//   llfs adopt <backing>               custom io on RAWFS_FDS, replays a synthetic INIT from RAWFS_STATE
//   llfs devfd <backing>               fuse_session_mount("/dev/fd/N") on RAWFS_FDS, no INIT replay
//   llfs libmount <mnt> <backing> [--mt]   plain fuse_session_mount; --mt runs fuse_session_loop_mt
//   llfs multi <k> <mnt-prefix> <backing>  k sessions in one process, one thread each
#define _GNU_SOURCE
#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 18)
#include <fuse_lowlevel.h>
#include <fuse_kernel.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define STATE_MAGIC 0x52535446u
#define FAKE_UNIQUE 0x7ffffffffffff000ull

struct state {
	uint32_t magic;
	uint32_t nfds;
	uint32_t has_commfd;
	uint32_t pad;
	struct fuse_init_in init_in;
	struct fuse_init_out init_out;
};

static struct state st;
static int backing = -1;
static const char *backing_path;
static int replay_pending, handoff_requested;
static struct fuse_session *main_se;

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

/* ---- filesystem: nodeid = backing st_ino ---- */

struct node {
	uint64_t ino;
	char name[256];
};
static struct node nodes[4096];
static int nnodes;
static pthread_mutex_t nodes_mu = PTHREAD_MUTEX_INITIALIZER;

static const char *node_name(uint64_t ino, const char *add)
{
	pthread_mutex_lock(&nodes_mu);
	const char *r = NULL;
	for (int i = 0; i < nnodes && !r; i++)
		if (nodes[i].ino == ino)
			r = nodes[i].name;
	if (!r && add) {
		nodes[nnodes].ino = ino;
		snprintf(nodes[nnodes].name, 256, "%s", add);
		r = nodes[nnodes++].name;
	}
	if (!r) {
		DIR *d = opendir(backing_path);
		struct dirent *e;
		while ((e = readdir(d)))
			if (e->d_ino == ino) {
				nodes[nnodes].ino = ino;
				snprintf(nodes[nnodes].name, 256, "%s", e->d_name);
				r = nodes[nnodes++].name;
			}
		closedir(d);
	}
	pthread_mutex_unlock(&nodes_mu);
	return r;
}

static int stat_ino(fuse_ino_t ino, struct stat *s)
{
	if (ino == FUSE_ROOT_ID) {
		if (fstat(backing, s))
			return errno;
		s->st_ino = FUSE_ROOT_ID;
		return 0;
	}
	const char *n = node_name(ino, NULL);
	if (!n)
		return ESTALE;
	return fstatat(backing, n, s, AT_SYMLINK_NOFOLLOW) ? errno : 0;
}

static void ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	(void)parent;
	if (!strcmp(name, "handoff_reexec")) {
		handoff_requested = 1;
		fuse_reply_err(req, ENOENT);
		fuse_session_exit(main_se);
		return;
	}
	struct fuse_entry_param e = {0};
	if (fstatat(backing, name, &e.attr, AT_SYMLINK_NOFOLLOW)) {
		fuse_reply_err(req, errno);
		return;
	}
	node_name(e.attr.st_ino, name);
	e.ino = e.attr.st_ino;
	fuse_reply_entry(req, &e);
}

static void ll_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	(void)fi;
	struct stat s;
	int e = stat_ino(ino, &s);
	if (e)
		fuse_reply_err(req, e);
	else
		fuse_reply_attr(req, &s, 0);
}

static void ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct stat s;
	int e = stat_ino(ino, &s);
	if (e) {
		fuse_reply_err(req, e);
		return;
	}
	fi->fh = ino;
	fi->direct_io = 1;
	fuse_reply_open(req, fi);
}

static void ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
	(void)fi;
	const char *n = node_name(ino, NULL);
	int fd = n ? openat(backing, n, O_RDONLY | O_CLOEXEC) : -1;
	if (fd < 0) {
		fuse_reply_err(req, EIO);
		return;
	}
	char *buf = malloc(size);
	ssize_t r = pread(fd, buf, size, off);
	close(fd);
	if (r < 0)
		fuse_reply_err(req, EIO);
	else
		fuse_reply_buf(req, buf, r);
	free(buf);
}

static void ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
	(void)ino;
	(void)fi;
	char *buf = calloc(1, size);
	size_t pos = 0;
	DIR *d = opendir(backing_path);
	struct dirent *e;
	off_t i = 0;
	while ((e = readdir(d))) {
		if (++i <= off)
			continue;
		struct stat s = {.st_ino = e->d_ino, .st_mode = e->d_type << 12};
		size_t need = fuse_add_direntry(req, buf + pos, size - pos, e->d_name, &s, i);
		if (need > size - pos)
			break;
		pos += need;
	}
	closedir(d);
	fuse_reply_buf(req, buf, pos);
	free(buf);
}

static const struct fuse_lowlevel_ops ops = {
	.lookup = ll_lookup,
	.getattr = ll_getattr,
	.open = ll_open,
	.read = ll_read,
	.readdir = ll_readdir,
};

/* ---- custom io: record INIT on first mount, replay it on adopt ---- */

static ssize_t io_read(int fd, void *buf, size_t len, void *ud)
{
	(void)ud;
	if (replay_pending) {
		replay_pending = 0;
		struct fuse_in_header *h = buf;
		memset(h, 0, sizeof(*h));
		h->len = sizeof(*h) + sizeof(st.init_in);
		h->opcode = FUSE_INIT;
		h->unique = FAKE_UNIQUE;
		memcpy(h + 1, &st.init_in, sizeof(st.init_in));
		ev("INIT_SYNTHETIC kernel=%u.%u", st.init_in.major, st.init_in.minor);
		return h->len;
	}
	ssize_t r = read(fd, buf, len);
	struct fuse_in_header *h = buf;
	if (r > 0 && h->opcode == FUSE_INIT) {
		memset(&st.init_in, 0, sizeof(st.init_in));
		memcpy(&st.init_in, h + 1, r - sizeof(*h) < sizeof(st.init_in) ? r - sizeof(*h) : sizeof(st.init_in));
		ev("INIT_FROM_KERNEL kernel=%u.%u", st.init_in.major, st.init_in.minor);
	}
	return r;
}

static ssize_t io_writev(int fd, struct iovec *iov, int count, void *ud)
{
	(void)ud;
	struct fuse_out_header *oh = iov[0].iov_base;
	size_t total = 0;
	for (int i = 0; i < count; i++)
		total += iov[i].iov_len;
	if (oh->unique == FAKE_UNIQUE) {
		struct fuse_init_out out = {0};
		size_t off = 0;
		for (int i = 0; i < count; i++) {
			size_t from = i == 0 ? sizeof(*oh) : 0;
			size_t n = iov[i].iov_len - from;
			if (off + n > sizeof(out))
				n = sizeof(out) - off;
			memcpy((char *)&out + off, (char *)iov[i].iov_base + from, n);
			off += n;
		}
		int same = !memcmp(&out, &st.init_out, sizeof(out));
		ev("INIT_REPLY_SWALLOWED matches_original=%d flags=%#x/%#x flags2=%#x/%#x max_write=%u/%u", same, out.flags,
		   st.init_out.flags, out.flags2, st.init_out.flags2, out.max_write, st.init_out.max_write);
		return total;
	}
	ssize_t r = writev(fd, iov, count);
	if (r > 0 && st.magic != STATE_MAGIC && total >= sizeof(*oh) + 8 && iov[0].iov_len >= sizeof(*oh)) {
		/* First reply on a fresh mount is the INIT reply. */
		size_t off = 0;
		for (int i = 0; i < count; i++) {
			size_t from = i == 0 ? sizeof(*oh) : 0;
			size_t n = iov[i].iov_len - from;
			if (off + n > sizeof(st.init_out))
				n = sizeof(st.init_out) - off;
			memcpy((char *)&st.init_out + off, (char *)iov[i].iov_base + from, n);
			off += n;
		}
		st.magic = STATE_MAGIC;
		ev("INIT_REPLY_RECORDED flags=%#x flags2=%#x max_write=%u", st.init_out.flags, st.init_out.flags2,
		   st.init_out.max_write);
	}
	return r;
}

static const struct fuse_custom_io cio = {.read = io_read, .writev = io_writev};

/* ---- helpers ---- */

static int fusermount(const char *mnt)
{
	int sv[2];
	socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
	pid_t p = fork();
	if (p == 0) {
		char env[32];
		snprintf(env, sizeof(env), "%d", sv[0]);
		setenv("_FUSE_COMMFD", env, 1);
		execlp("fusermount3", "fusermount3", "-o", "fsname=llfs,subtype=llfs", "--", mnt, (char *)NULL);
		_exit(127);
	}
	close(sv[0]);
	char c, cbuf[CMSG_SPACE(sizeof(int))];
	struct iovec iov = {&c, 1};
	struct msghdr m = {.msg_iov = &iov, .msg_iovlen = 1, .msg_control = cbuf, .msg_controllen = sizeof(cbuf)};
	if (recvmsg(sv[1], &m, 0) != 1) {
		ev("FATAL fusermount3");
		exit(1);
	}
	int fd;
	memcpy(&fd, CMSG_DATA(CMSG_FIRSTHDR(&m)), sizeof(int));
	close(sv[1]);
	waitpid(p, NULL, 0);
	return fd;
}

static struct fuse_session *new_session(void)
{
	char *argv[] = {"llfs", NULL};
	struct fuse_args args = FUSE_ARGS_INIT(1, argv);
	struct fuse_session *se = fuse_session_new(&args, &ops, sizeof(ops), NULL);
	if (!se) {
		ev("FATAL fuse_session_new");
		exit(1);
	}
	return se;
}

static void state_hex(char *out)
{
	const unsigned char *p = (const unsigned char *)&st;
	for (size_t i = 0; i < sizeof(st); i++)
		sprintf(out + 2 * i, "%02x", p[i]);
}

static int env_fd(void)
{
	const char *hex = getenv("RAWFS_STATE"), *fds = getenv("RAWFS_FDS");
	if (!hex || !fds) {
		ev("FATAL no RAWFS_STATE/RAWFS_FDS");
		exit(1);
	}
	unsigned char *p = (unsigned char *)&st;
	for (size_t i = 0; i < sizeof(st); i++)
		sscanf(hex + 2 * i, "%2hhx", &p[i]);
	return atoi(fds);
}

static void open_backing(const char *p)
{
	backing_path = p;
	backing = open(p, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (backing < 0) {
		ev("FATAL backing");
		exit(1);
	}
}

static void *multi_loop(void *se)
{
	fuse_session_loop(se);
	return NULL;
}

int main(int argc, char **argv)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	if (argc < 3)
		return 2;
	const char *mode = argv[1];
	ev("START mode=%s pid=%d", mode, getpid());
	if (!strcmp(mode, "mount")) {
		open_backing(argv[3]);
		int fd = fusermount(argv[2]);
		main_se = new_session();
		int r = fuse_session_custom_io(main_se, &cio, sizeof(cio), fd);
		ev("CUSTOM_IO ret=%d", r);
		if (r)
			return 1;
		ev("MOUNTED fd=%d", fd);
		fuse_session_loop(main_se);
		if (!handoff_requested)
			return 0;
		static char hex[2 * sizeof(struct state) + 1];
		char fdenv[16];
		st.nfds = 1;
		state_hex(hex);
		snprintf(fdenv, sizeof(fdenv), "%d", fd);
		setenv("RAWFS_STATE", hex, 1);
		setenv("RAWFS_FDS", fdenv, 1);
		ev("EXEC_START");
		execl("/proc/self/exe", argv[0], "adopt", argv[3], (char *)NULL);
		return 1;
	}
	if (!strcmp(mode, "adopt") || !strcmp(mode, "devfd")) {
		open_backing(argv[2]);
		int fd = env_fd();
		main_se = new_session();
		if (!strcmp(mode, "adopt")) {
			replay_pending = 1;
			int r = fuse_session_custom_io(main_se, &cio, sizeof(cio), fd);
			ev("CUSTOM_IO ret=%d", r);
			if (r)
				return 1;
		} else {
			char mp[32];
			snprintf(mp, sizeof(mp), "/dev/fd/%d", fd);
			int r = fuse_session_mount(main_se, mp);
			ev("DEVFD_MOUNT %s ret=%d", mp, r);
			if (r)
				return 1;
		}
		ev("TAKEOVER fd=%d", fd);
		fuse_session_loop(main_se);
		return 0;
	}
	if (!strcmp(mode, "libmount")) {
		open_backing(argv[3]);
		main_se = new_session();
		if (fuse_session_mount(main_se, argv[2]))
			return 1;
		ev("MOUNTED");
		if (argc > 4 && !strcmp(argv[4], "--mt")) {
			struct fuse_loop_config *cfg = fuse_loop_cfg_create();
			fuse_session_loop_mt(main_se, cfg);
		} else {
			fuse_session_loop(main_se);
		}
		return 0;
	}
	if (!strcmp(mode, "multi")) {
		int k = atoi(argv[2]);
		open_backing(argv[4]);
		pthread_t t[64];
		for (int i = 0; i < k; i++) {
			char mp[512];
			snprintf(mp, sizeof(mp), "%s%d", argv[3], i);
			struct fuse_session *se = new_session();
			if (fuse_session_mount(se, mp))
				return 1;
			pthread_create(&t[i], NULL, multi_loop, se);
		}
		ev("MOUNTED k=%d", k);
		for (int i = 0; i < k; i++)
			pthread_join(t[i], NULL);
		return 0;
	}
	return 2;
}
