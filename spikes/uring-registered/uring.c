// uring: CPU per I/O of io_uring with plain or registered buffers and files.
//
// usage:
//   uring io <path> <nop|read|write> <qd> <secs> direct=0|1 fbuf=0|1 ffile=0|1 [size_mb=N] [slab_mb=N]
//   uring reg <slab_mb> [iovecs]      time IORING_REGISTER_BUFFERS of an anonymous slab
//
// One ring per process, set up as the daemon's driver would (SINGLE_ISSUER, DEFER_TASKRUN),
// driven with raw syscalls. Every I/O is 4 KiB at a random aligned offset, into a random 4 KiB
// slot of one slab (registered as whole iovecs of at most 1 GiB when fbuf=1). qd I/Os stay in
// flight: each io_uring_enter submits the refills and waits for at least one completion.
// For nop, fbuf and ffile set IORING_NOP_FIXED_BUFFER and IORING_NOP_FILE|IORING_NOP_FIXED_FILE.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#ifndef IORING_NOP_FILE
#define IORING_NOP_FILE (1U << 1)
#define IORING_NOP_FIXED_FILE (1U << 2)
#define IORING_NOP_FIXED_BUFFER (1U << 3)
#endif

struct ring {
	int fd;
	unsigned *sq_head, *sq_tail, *sq_mask, *sq_array;
	unsigned *cq_head, *cq_tail, *cq_mask;
	struct io_uring_sqe *sqes;
	struct io_uring_cqe *cqes;
	unsigned pending;
	unsigned long enters;
};

static double now(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static double cpu_s(void) {
	struct rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	return ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6 + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
}

static uint64_t rng = 88172645463325252ull;
static uint64_t xorshift(void) {
	rng ^= rng << 13;
	rng ^= rng >> 7;
	rng ^= rng << 17;
	return rng;
}

static void ring_init(struct ring *r, unsigned entries) {
	struct io_uring_params p;
	memset(&p, 0, sizeof p);
	p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
	r->fd = syscall(__NR_io_uring_setup, entries, &p);
	if (r->fd < 0) { perror("io_uring_setup"); exit(1); }
	size_t sqsz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
	size_t cqsz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
	if (cqsz > sqsz) sqsz = cqsz;
	char *sq = mmap(0, sqsz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQ_RING);
	if (sq == MAP_FAILED) { perror("mmap sq"); exit(1); }
	char *cq = sq;
	r->sq_head = (unsigned *)(sq + p.sq_off.head);
	r->sq_tail = (unsigned *)(sq + p.sq_off.tail);
	r->sq_mask = (unsigned *)(sq + p.sq_off.ring_mask);
	r->sq_array = (unsigned *)(sq + p.sq_off.array);
	r->cq_head = (unsigned *)(cq + p.cq_off.head);
	r->cq_tail = (unsigned *)(cq + p.cq_off.tail);
	r->cq_mask = (unsigned *)(cq + p.cq_off.ring_mask);
	r->cqes = (struct io_uring_cqe *)(cq + p.cq_off.cqes);
	r->sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe), PROT_READ | PROT_WRITE,
		       MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQES);
	if (r->sqes == MAP_FAILED) { perror("mmap sqes"); exit(1); }
	for (unsigned i = 0; i < p.sq_entries; i++) r->sq_array[i] = i;
}

static struct io_uring_sqe *get_sqe(struct ring *r) {
	unsigned tail = *r->sq_tail;
	struct io_uring_sqe *s = &r->sqes[tail & *r->sq_mask];
	memset(s, 0, sizeof *s);
	__atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
	r->pending++;
	return s;
}

static int enter(struct ring *r, unsigned min_complete) {
	int ret = syscall(__NR_io_uring_enter, r->fd, r->pending, min_complete, IORING_ENTER_GETEVENTS, NULL, 0);
	if (ret < 0) { perror("io_uring_enter"); exit(1); }
	r->pending = 0;
	r->enters++;
	return ret;
}

static int reg_buffers(int ringfd, char *slab, size_t len, int *niov_out) {
	size_t maxio = 1ul << 30;
	int n = (len + maxio - 1) / maxio;
	struct iovec *iov = calloc(n, sizeof *iov);
	for (int i = 0; i < n; i++) {
		iov[i].iov_base = slab + i * maxio;
		iov[i].iov_len = len - i * maxio < maxio ? len - i * maxio : maxio;
	}
	*niov_out = n;
	int ret = syscall(__NR_io_uring_register, ringfd, IORING_REGISTER_BUFFERS, iov, n);
	free(iov);
	return ret;
}

static int cmd_reg(int argc, char **argv) {
	size_t mb = atol(argv[2]);
	int iovecs = argc > 3 ? atoi(argv[3]) : 0;
	struct ring r;
	ring_init(&r, 8);
	size_t len = mb << 20;
	char *slab = mmap(0, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | (getenv("POPULATE") ? MAP_POPULATE : 0), -1, 0);
	if (slab == MAP_FAILED) { perror("mmap slab"); return 1; }
	struct rlimit rl;
	getrlimit(RLIMIT_MEMLOCK, &rl);
	double t0 = now();
	int ret, niov;
	if (iovecs > 0) {
		struct iovec *iov = calloc(iovecs, sizeof *iov);
		size_t each = len / iovecs;
		for (int i = 0; i < iovecs; i++) { iov[i].iov_base = slab + i * each; iov[i].iov_len = each; }
		niov = iovecs;
		ret = syscall(__NR_io_uring_register, r.fd, IORING_REGISTER_BUFFERS, iov, iovecs);
	} else if (argc > 4) {
		struct iovec iov = {slab, len};
		niov = 1;
		ret = syscall(__NR_io_uring_register, r.fd, IORING_REGISTER_BUFFERS, &iov, 1);
	} else {
		ret = reg_buffers(r.fd, slab, len, &niov);
	}
	double t1 = now();
	int err = ret < 0 ? errno : 0;
	if (ret == 0) syscall(__NR_io_uring_register, r.fd, IORING_UNREGISTER_BUFFERS, NULL, 0);
	double t2 = now();
	printf("REG slab_mb=%zu iovecs=%d uid=%d memlock_kb=%lld ret=%d errno=%s register_ms=%.2f unregister_ms=%.2f\n",
	       mb, niov, getuid(), rl.rlim_cur == RLIM_INFINITY ? -1LL : (long long)rl.rlim_cur / 1024, ret,
	       err ? strerror(err) : "0", (t1 - t0) * 1000, (t2 - t1) * 1000);
	return 0;
}

int main(int argc, char **argv) {
	if (argc >= 3 && strcmp(argv[1], "reg") == 0) return cmd_reg(argc, argv);
	if (argc < 6 || strcmp(argv[1], "io") != 0) {
		fprintf(stderr, "usage: uring io path nop|read|write qd secs direct= fbuf= ffile= | uring reg slab_mb\n");
		return 2;
	}
	const char *path = argv[2];
	const char *op = argv[3];
	unsigned qd = atoi(argv[4]);
	double secs = atof(argv[5]);
	int direct = 0, fbuf = 0, ffile = 0;
	size_t size_mb = 1024, slab_mb = 64;
	for (int i = 6; i < argc; i++) {
		sscanf(argv[i], "direct=%d", &direct);
		sscanf(argv[i], "fbuf=%d", &fbuf);
		sscanf(argv[i], "ffile=%d", &ffile);
		sscanf(argv[i], "size_mb=%zu", &size_mb);
		sscanf(argv[i], "slab_mb=%zu", &slab_mb);
	}
	int nop = strcmp(op, "nop") == 0;
	int wr = strcmp(op, "write") == 0;
	int fd = open(path, (wr ? O_RDWR : O_RDONLY) | (direct ? O_DIRECT : 0));
	if (fd < 0) { perror(path); return 1; }

	struct ring r;
	ring_init(&r, qd < 8 ? 8 : qd);
	size_t slab_len = slab_mb << 20;
	char *slab = mmap(0, slab_len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
	if (slab == MAP_FAILED) { perror("mmap slab"); return 1; }
	int niov = 0;
	if (fbuf && reg_buffers(r.fd, slab, slab_len, &niov) < 0) { perror("register buffers"); return 1; }
	if (ffile && syscall(__NR_io_uring_register, r.fd, IORING_REGISTER_FILES, &fd, 1) < 0) {
		perror("register files");
		return 1;
	}
	uint64_t blocks = (size_mb << 20) / 4096;
	uint64_t slots = slab_len / 4096;

#define PREP()                                                                        \
	do {                                                                              \
		struct io_uring_sqe *s = get_sqe(&r);                                         \
		char *buf = slab + (xorshift() % slots) * 4096;                               \
		if (nop) {                                                                    \
			s->opcode = IORING_OP_NOP;                                                \
			unsigned fl = 0;                                                          \
			if (ffile) { fl |= IORING_NOP_FILE | IORING_NOP_FIXED_FILE; s->fd = 0; }  \
			else { fl |= IORING_NOP_FILE; s->fd = fd; }                               \
			if (fbuf) { fl |= IORING_NOP_FIXED_BUFFER; s->buf_index = 0; }            \
			s->rw_flags = fl;                                                         \
		} else {                                                                      \
			s->opcode = fbuf ? (wr ? IORING_OP_WRITE_FIXED : IORING_OP_READ_FIXED)    \
					 : (wr ? IORING_OP_WRITE : IORING_OP_READ);           \
			s->fd = ffile ? 0 : fd;                                                   \
			if (ffile) s->flags |= IOSQE_FIXED_FILE;                                  \
			s->addr = (uint64_t)(uintptr_t)buf;                                       \
			s->len = 4096;                                                            \
			s->off = (xorshift() % blocks) * 4096;                                    \
			s->buf_index = (buf - slab) >> 30;                                        \
		}                                                                             \
	} while (0)

	for (unsigned i = 0; i < qd; i++) PREP();
	double t0 = now(), c0 = cpu_s(), end = t0 + secs;
	uint64_t ops = 0, errs = 0;
	int stopping = 0;
	unsigned inflight = qd;
	while (inflight > 0) {
		enter(&r, 1);
		unsigned head = *r.cq_head;
		unsigned tail = __atomic_load_n(r.cq_tail, __ATOMIC_ACQUIRE);
		while (head != tail) {
			struct io_uring_cqe *c = &r.cqes[head & *r.cq_mask];
			if (c->res < 0 || (!nop && c->res != 4096)) {
				if (errs++ == 0) fprintf(stderr, "cqe res=%d (%s)\n", c->res, c->res < 0 ? strerror(-c->res) : "short");
			}
			ops++;
			inflight--;
			head++;
			if (!stopping) { PREP(); inflight++; }
		}
		__atomic_store_n(r.cq_head, head, __ATOMIC_RELEASE);
		if (!stopping && now() >= end) stopping = 1;
	}
	double t1 = now(), c1 = cpu_s();
	printf("IO path=%s op=%s qd=%u direct=%d fbuf=%d ffile=%d iovecs=%d ops=%llu errs=%llu secs=%.2f iops=%.0f cpu_us_per_io=%.3f "
	       "wall_us_per_io=%.2f enters_per_io=%.3f\n",
	       path, op, qd, direct, fbuf, ffile, niov, (unsigned long long)ops, (unsigned long long)errs, t1 - t0,
	       ops / (t1 - t0), (c1 - c0) * 1e6 / ops, (t1 - t0) * 1e6 / ops * qd, (double)r.enters / ops);
	return errs ? 1 : 0;
}
