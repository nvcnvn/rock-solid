/* SPDK embedding spike: can SPDK run as one "disk driver" inside our daemon, driven by our own
 * event loop, as a normal user, with no hugepages, no pinned cores and no spdk_app_start()?
 *
 * usage: spdk_spike [--interrupt] [--mem MB] [--reinit] <workdir>
 *   S1  env init (no_huge, no_pci) + our own spdk_thread polled from main()
 *   S2  4K QD1 write/read latency and CPU per op on malloc, aio and uring bdevs,
 *       with SPDK DMA buffers and with plain heap buffers
 *   S3  idle CPU of the loop (poll vs --interrupt, where we block in epoll on the thread fd)
 *   S4  cross-thread submission: another pthread hands I/O to the SPDK thread via
 *       spdk_thread_send_msg and waits on an eventfd (how a tokio worker would talk to it)
 *   S5  which buffers an NVMe/PCIe bdev could DMA from (spdk_vtophys / spdk_mem_register)
 *   S6  --reinit: spdk_env_fini() then spdk_env_init(NULL) in the same process
 */
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/init.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "malloc/bdev_malloc.h"
#include "aio/bdev_aio.h"
#include "uring/bdev_uring.h"

#define BS 4096
#define N_OPS 20000

static struct spdk_thread *g_thread;
static bool g_intr;
static int g_epfd = -1;
static volatile bool g_flag;
static int g_rc;

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static uint64_t cpu_ns(void)
{
	struct rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	return (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000000ull +
	       (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) * 1000ull;
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

/* The daemon's event loop, reduced to one thread: either busy-poll the SPDK thread, or sleep in
 * epoll on its interrupt fd (which we could equally register with tokio/mio). */
static void loop_once(int timeout_ms)
{
	if (g_intr) {
		struct epoll_event ev;
		epoll_wait(g_epfd, &ev, 1, timeout_ms);
	}
	spdk_thread_poll(g_thread, 0, 0);
}

static void run_until_flag(void)
{
	while (!g_flag) {
		loop_once(100);
	}
	g_flag = false;
}

static void fini_done(void *arg)
{
	g_flag = true;
}

static void set_flag(int rc, void *arg)
{
	g_rc = rc;
	g_flag = true;
}

/* ---- S2: single-thread QD1 I/O ---- */

static int g_io_ok;

static void io_done(struct spdk_bdev_io *io, bool ok, void *arg)
{
	g_io_ok = ok;
	spdk_bdev_free_io(io);
	g_flag = true;
}

static void bdev_event(enum spdk_bdev_event_type t, struct spdk_bdev *b, void *ctx) {}

static void bench(const char *bdev, const char *buftype, void *buf)
{
	struct spdk_bdev_desc *desc;
	int rc = spdk_bdev_open_ext(bdev, true, bdev_event, NULL, &desc);
	if (rc) {
		printf("  %-8s %-5s open failed: %s\n", bdev, buftype, strerror(-rc));
		return;
	}
	struct spdk_io_channel *ch = spdk_bdev_get_io_channel(desc);
	if (!ch) {
		printf("  %-8s %-5s no io channel (module refused this thread mode)\n", bdev, buftype);
		spdk_bdev_close(desc);
		return;
	}
	static uint64_t lat[2][N_OPS];
	uint64_t cpu0 = cpu_ns(), t0 = now_ns();
	int fails = 0;
	for (int i = 0; i < N_OPS; i++) {
		for (int w = 0; w < 2; w++) {
			uint64_t s = now_ns();
			uint64_t off = (uint64_t)(i % 256) * BS;
			rc = w == 0 ? spdk_bdev_write(desc, ch, buf, off, BS, io_done, NULL)
			     : spdk_bdev_read(desc, ch, buf, off, BS, io_done, NULL);
			if (rc) {
				printf("  submit failed: %s\n", strerror(-rc));
				goto out;
			}
			run_until_flag();
			fails += !g_io_ok;
			lat[w][i] = now_ns() - s;
		}
	}
	uint64_t wall = now_ns() - t0, cpu = cpu_ns() - cpu0;
	qsort(lat[0], N_OPS, 8, cmp_u64);
	qsort(lat[1], N_OPS, 8, cmp_u64);
	printf("  %-8s %-5s write p50 %6.1f p99 %6.1f us | read p50 %6.1f p99 %6.1f us | CPU %5.1f%% of wall, %5.1f us/op | failed %d\n",
	       bdev, buftype, lat[0][N_OPS / 2] / 1e3, lat[0][N_OPS * 99 / 100] / 1e3,
	       lat[1][N_OPS / 2] / 1e3, lat[1][N_OPS * 99 / 100] / 1e3,
	       100.0 * cpu / wall, cpu / 1e3 / (2 * N_OPS), fails);
out:
	spdk_put_io_channel(ch);
	spdk_bdev_close(desc);
	spdk_thread_poll(g_thread, 0, 0);
}

/* ---- S4: cross-thread submission ---- */

struct xreq {
	struct spdk_bdev_desc *desc;
	struct spdk_io_channel *ch;
	void *buf;
	int efd;
};

static void xio_done(struct spdk_bdev_io *io, bool ok, void *arg)
{
	struct xreq *r = arg;
	uint64_t one = 1;
	spdk_bdev_free_io(io);
	if (write(r->efd, &one, 8) != 8) {
		abort();
	}
}

static void xsubmit(void *arg)
{
	struct xreq *r = arg;
	if (spdk_bdev_write(r->desc, r->ch, r->buf, 0, BS, xio_done, r)) {
		abort();
	}
}

static atomic_bool g_xstop;

static void *xclient(void *arg)
{
	struct xreq *r = arg;
	static uint64_t lat[N_OPS];
	uint64_t v;
	for (int i = 0; i < N_OPS; i++) {
		uint64_t s = now_ns();
		spdk_thread_send_msg(g_thread, xsubmit, r);
		if (read(r->efd, &v, 8) != 8) {
			abort();
		}
		lat[i] = now_ns() - s;
	}
	qsort(lat, N_OPS, 8, cmp_u64);
	printf("  malloc write via spdk_thread_send_msg from another pthread: p50 %.1f p99 %.1f us\n",
	       lat[N_OPS / 2] / 1e3, lat[N_OPS * 99 / 100] / 1e3);
	atomic_store(&g_xstop, true);
	return NULL;
}

static void cross_thread(void *buf)
{
	struct xreq r = { .buf = buf, .efd = eventfd(0, 0) };
	spdk_bdev_open_ext("Malloc0", true, bdev_event, NULL, &r.desc);
	r.ch = spdk_bdev_get_io_channel(r.desc);
	pthread_t t;
	pthread_create(&t, NULL, xclient, &r);
	while (!atomic_load(&g_xstop)) {
		loop_once(100);
	}
	pthread_join(t, NULL);
	spdk_put_io_channel(r.ch);
	spdk_bdev_close(r.desc);
	close(r.efd);
}

/* ---- S3: idle CPU ---- */

static void idle_cpu(void)
{
	uint64_t c0 = cpu_ns(), t0 = now_ns();
	while (now_ns() - t0 < 1000000000ull) {
		loop_once(100);
	}
	printf("  idle loop for 1 s: CPU %.1f%%\n", 100.0 * (cpu_ns() - c0) / (now_ns() - t0));
}

/* ---- S5: which memory an NVMe/PCIe device could DMA from ---- */

static void dma_check(void *dma, void *heap)
{
	uint64_t sz = BS;
	uint64_t p1 = spdk_vtophys(dma, &sz);
	sz = BS;
	uint64_t p2 = spdk_vtophys(heap, &sz);
	printf("  spdk_vtophys(spdk_dma_zmalloc buf) = %s\n", p1 == SPDK_VTOPHYS_ERROR ? "ERROR" : "ok");
	printf("  spdk_vtophys(heap buf)             = %s\n", p2 == SPDK_VTOPHYS_ERROR ? "ERROR (NVMe/PCIe would reject it)" : "ok");
	void *big = aligned_alloc(2 << 20, 4 << 20);
	memset(big, 0, 4 << 20);
	int rc = spdk_mem_register(big, 4 << 20);
	sz = BS;
	uint64_t p3 = rc ? SPDK_VTOPHYS_ERROR : spdk_vtophys(big, &sz);
	printf("  spdk_mem_register(4 MB heap, 2 MB aligned) = %d (%s); vtophys after = %s\n", rc,
	       rc ? strerror(-rc) : "ok", p3 == SPDK_VTOPHYS_ERROR ? "ERROR" : "ok");
	if (!rc) {
		spdk_mem_unregister(big, 4 << 20);
	}
	free(big);
}

/* We own scheduling: there is one SPDK thread and main() polls it, so every op is a no-op.
 * Must not be NULL: in interrupt mode SPDK calls it on thread exit. */
static int thread_op(struct spdk_thread *t, enum spdk_thread_op op)
{
	return 0;
}

static bool thread_op_supported(enum spdk_thread_op op)
{
	return true;
}

static long rss_kb(void)
{
	FILE *f = fopen("/proc/self/status", "r");
	char line[256];
	long kb = -1;
	while (fgets(line, sizeof line, f)) {
		if (!strncmp(line, "VmRSS:", 6)) {
			kb = atol(line + 6);
		}
	}
	fclose(f);
	return kb;
}

int main(int argc, char **argv)
{
	int mem = 128;
	bool reinit = false;
	const char *dir = NULL;
	setvbuf(stdout, NULL, _IOLBF, 0);
	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "--interrupt")) {
			g_intr = true;
		} else if (!strcmp(argv[i], "--mem")) {
			mem = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "--reinit")) {
			reinit = true;
		} else {
			dir = argv[i];
		}
	}
	if (!dir) {
		fprintf(stderr, "usage: %s [--interrupt] [--mem MB] [--reinit] <workdir>\n", argv[0]);
		return 2;
	}

	printf("S1 env init: uid=%d, no_huge, no_pci, mem %d MB, mode %s\n", getuid(), mem,
	       g_intr ? "interrupt" : "poll");
	struct spdk_env_opts opts = { .opts_size = sizeof(opts) };
	spdk_env_opts_init(&opts);
	opts.name = "rs-spike";
	opts.no_huge = true;
	opts.no_pci = true;
	opts.mem_size = mem;
	opts.core_mask = "0x1";
	/* DPDK's default base (0x200000000000, bit 45) is above a 42-bit IOMMU DMA mask, and
	 * no-huge init then fails with "IOVA exceeding limits of current DMA mask". */
	opts.base_virtaddr = 0x10000000000ull;
	uint64_t t0 = now_ns();
	if (spdk_env_init(&opts)) {
		printf("  spdk_env_init FAILED\n");
		return 1;
	}
	printf("  spdk_env_init ok in %.1f ms, RSS %ld MB\n", (now_ns() - t0) / 1e6, rss_kb() / 1024);
	spdk_log_set_print_level(SPDK_LOG_WARN);
	if (g_intr && spdk_interrupt_mode_enable()) {
		printf("  spdk_interrupt_mode_enable FAILED\n");
		return 1;
	}
	spdk_thread_lib_init_ext(thread_op, thread_op_supported, 0, SPDK_DEFAULT_MSG_MEMPOOL_SIZE);
	g_thread = spdk_thread_create("rs-disk", NULL);
	spdk_set_thread(g_thread);
	if (g_intr) {
		g_epfd = epoll_create1(0);
		struct epoll_event ev = { .events = EPOLLIN };
		epoll_ctl(g_epfd, EPOLL_CTL_ADD, spdk_thread_get_interrupt_fd(g_thread), &ev);
	}

	/* Shrink SPDK's preallocated pools: the defaults need ~400 MB of DPDK memory. */
	struct spdk_bdev_opts bo;
	spdk_bdev_get_opts(&bo, sizeof(bo));
	bo.bdev_io_pool_size = 1024;
	bo.bdev_io_cache_size = 16;
	spdk_bdev_set_opts(&bo);
	struct spdk_iobuf_opts io;
	spdk_iobuf_get_opts(&io, sizeof(io));
	io.small_pool_count = 1024;
	io.large_pool_count = 128;
	spdk_iobuf_set_opts(&io);

	spdk_subsystem_init(set_flag, NULL);
	run_until_flag();
	if (g_rc) {
		printf("  spdk_subsystem_init FAILED rc=%d\n", g_rc);
		return 1;
	}
	printf("  subsystems up, RSS %ld MB\n", rss_kb() / 1024);

	char path[512];
	snprintf(path, sizeof path, "%s/disk.img", dir);
	int fd = open(path, O_CREAT | O_RDWR, 0644);
	if (fd < 0 || ftruncate(fd, 64 << 20)) {
		perror(path);
		return 1;
	}
	close(fd);

	struct spdk_bdev *b;
	struct malloc_bdev_opts mo = { .name = "Malloc0", .num_blocks = 4096, .block_size = BS };
	int rc = create_malloc_disk(&b, &mo);
	printf("  create malloc bdev: %s\n", rc ? strerror(-rc) : "ok");
	rc = create_aio_bdev("Aio0", path, BS, false, false, NULL, false);
	printf("  create aio bdev on %s: %s\n", path, rc ? strerror(-rc) : "ok");
	struct bdev_uring_opts uo = { .name = "Uring0", .filename = path, .block_size = BS };
	b = create_uring_bdev(&uo);
	printf("  create uring bdev on same file: %s\n", b ? "ok" : "FAILED");

	void *dma = spdk_dma_zmalloc(BS, BS, NULL);
	void *heap = aligned_alloc(BS, BS);
	memset(heap, 0xab, BS);

	printf("S2 4K QD1, %d writes + %d reads each\n", N_OPS, N_OPS);
	const char *bdevs[] = { "Malloc0", "Aio0", "Uring0" };
	for (int i = 0; i < 3; i++) {
		bench(bdevs[i], "dma", dma);
		bench(bdevs[i], "heap", heap);
	}
	printf("S3 idle\n");
	idle_cpu();
	printf("S4 cross-thread\n");
	cross_thread(dma);
	printf("S5 DMA-ability of buffers (what an NVMe/PCIe bdev needs)\n");
	dma_check(dma, heap);
	printf("  RSS at end of I/O tests %ld MB\n", rss_kb() / 1024);

	spdk_dma_free(dma);
	free(heap);
	spdk_subsystem_fini(fini_done, NULL);
	run_until_flag();
	spdk_thread_exit(g_thread);
	while (!spdk_thread_is_exited(g_thread)) {
		spdk_thread_poll(g_thread, 0, 0);
	}
	spdk_thread_destroy(g_thread);
	spdk_thread_lib_fini();
	spdk_env_fini();
	printf("  clean shutdown ok\n");

	if (reinit) {
		printf("S6 re-init env in the same process\n");
		rc = spdk_env_init(NULL);
		printf("  spdk_env_init(NULL) after fini = %d\n", rc);
		if (!rc) {
			void *p = spdk_dma_zmalloc(BS, BS, NULL);
			printf("  dma alloc after re-init: %s\n", p ? "ok" : "FAILED");
			spdk_env_fini();
		}
	}
	return 0;
}
