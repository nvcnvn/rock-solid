/* SPDK NVMe interrupt-mode spike: does the NVMe driver complete I/O through an eventfd that our
 * own loop can sleep on, and what does that cost against busy polling?
 *
 * usage: nvme_intr [--interrupt] [--trust-request] [--bdev] [--no-huge] [--mem MB] [--base ADDR] [--n N] <trid>
 *   trid: "trtype:PCIe traddr:0000:00:04.0" or "trtype:VFIOUSER traddr:<dir>"
 *
 * Raw mode (default): spdk_nvme_probe + one poll group + one I/O qpair, driven from main().
 *   N1  attach: did enable_interrupts survive attach; admin and I/O qpair fds
 *   N2  4K QD1 write/read latency and CPU per op
 *   N3  idle CPU for 1 s
 *   N4  4K QD32 random read: IOPS and CPU per op
 * --bdev: bdev_nvme inside one spdk_thread (as in ../bypass-drivers), main() sleeps on the
 *   thread's interrupt fd.
 *   B1  attach through spdk_bdev_nvme_create
 *   B2  4K QD1 write/read latency and CPU per op
 *   B3  idle CPU for 1 s
 *
 * --trust-request keeps waiting on the fd when attach silently fell back to polling, to show
 * what an embedder that does not check spdk_nvme_ctrlr_get_opts() sees.
 *
 * CPU is reported twice: this process (getrusage) and the whole machine from /proc/stat, which
 * also counts hard and soft IRQ time that getrusage does not charge to the process.
 *
 * Interrupt mode waits in epoll on one fd (the poll group's fd group, or the spdk_thread fd),
 * which is how the daemon would register it with tokio. Poll mode spins.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/fd_group.h"
#include "spdk/init.h"
#include "spdk/log.h"
#include "spdk/module/bdev/nvme.h"
#include "spdk/nvme.h"
#include "spdk/thread.h"

#define BS 4096
#define QD_DEEP 32

static bool g_intr;
static bool g_trust_request;
static int g_n = 20000;
static int g_epfd = -1;
static uint64_t g_wakeups;

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

/* Busy jiffies of all CPUs (everything but idle and iowait), in ns. */
static uint64_t sys_busy_ns(void)
{
	unsigned long long v[10] = {};
	FILE *f = fopen("/proc/stat", "r");
	if (!f || fscanf(f, "cpu %llu %llu %llu %llu %llu %llu %llu %llu", &v[0], &v[1], &v[2], &v[3],
			 &v[4], &v[5], &v[6], &v[7]) != 8) {
		if (f) {
			fclose(f);
		}
		return 0;
	}
	fclose(f);
	uint64_t busy = v[0] + v[1] + v[2] + v[5] + v[6] + v[7];
	return busy * (1000000000ull / sysconf(_SC_CLK_TCK));
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

static void check(const char *what, long got, long want)
{
	printf("  [%s] %s got=%ld want=%ld\n", got == want ? "OK " : "FAIL", what, got, want);
}

static void epoll_add(int fd)
{
	struct epoll_event ev = { .events = EPOLLIN };
	if (g_epfd < 0) {
		g_epfd = epoll_create1(0);
	}
	if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev)) {
		printf("  epoll_ctl(%d): %s\n", fd, strerror(errno));
	}
}

static void sleep_on_fd(int timeout_ms)
{
	struct epoll_event ev;
	if (epoll_wait(g_epfd, &ev, 1, timeout_ms) > 0) {
		g_wakeups++;
	}
}

/* Sum of all CPUs' counts on /proc/interrupts lines owned by vfio (MSI-X vectors). */
static uint64_t vfio_irqs(int *vectors)
{
	char line[4096];
	uint64_t sum = 0;
	FILE *f = fopen("/proc/interrupts", "r");
	*vectors = 0;
	while (f && fgets(line, sizeof line, f)) {
		if (!strstr(line, "vfio-")) {
			continue;
		}
		(*vectors)++;
		char *p = strchr(line, ':');
		for (char *tok = strtok(p + 1, " \t"); tok; tok = strtok(NULL, " \t")) {
			char *end;
			unsigned long long v = strtoull(tok, &end, 10);
			if (end == tok || (*end && *end != '\n')) {
				break;
			}
			sum += v;
		}
	}
	if (f) {
		fclose(f);
	}
	return sum;
}

static int count_eventfds(void)
{
	char path[64], target[64];
	int n = 0;
	for (int fd = 0; fd < 1024; fd++) {
		snprintf(path, sizeof path, "/proc/self/fd/%d", fd);
		ssize_t len = readlink(path, target, sizeof target - 1);
		if (len > 0) {
			target[len] = 0;
			n += strstr(target, "eventfd") != NULL;
		}
	}
	return n;
}

static uint64_t g_sys0;

static void report(const char *name, uint64_t *wlat, uint64_t *rlat, uint64_t wall, uint64_t cpu, int fails)
{
	uint64_t sys = sys_busy_ns() - g_sys0;
	uint64_t *lat[2] = { wlat, rlat };
	qsort(lat[0], g_n, 8, cmp_u64);
	qsort(lat[1], g_n, 8, cmp_u64);
	printf("  %-6s write p50 %6.1f p99 %6.1f us | read p50 %6.1f p99 %6.1f us | CPU %5.1f%% of wall, %5.1f us/op (all CPUs %5.1f us/op) | wakeups/op %.2f | failed %d\n",
	       name, lat[0][g_n / 2] / 1e3, lat[0][g_n * 99 / 100] / 1e3, lat[1][g_n / 2] / 1e3,
	       lat[1][g_n * 99 / 100] / 1e3, 100.0 * cpu / wall, cpu / 1e3 / (2 * g_n),
	       sys / 1e3 / (2 * g_n), (double)g_wakeups / (2 * g_n), fails);
}

/* ---- raw NVMe driver ---- */

static struct spdk_nvme_ctrlr *g_ctrlr;
static struct spdk_nvme_ns *g_ns;
static struct spdk_nvme_qpair *g_qp;
static struct spdk_nvme_poll_group *g_pg;
static struct spdk_fd_group *g_fgrp;
static int g_outstanding, g_errors;

static bool probe_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		     struct spdk_nvme_ctrlr_opts *opts)
{
	opts->enable_interrupts = g_intr;
	return true;
}

static void attach_cb(void *ctx, const struct spdk_nvme_transport_id *trid,
		      struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	g_ctrlr = ctrlr;
}

static void io_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	g_outstanding--;
	g_errors += spdk_nvme_cpl_is_error(cpl);
}

static void disconnected_cb(struct spdk_nvme_qpair *qpair, void *ctx)
{
	printf("  qpair disconnected\n");
}

static void raw_wait_once(int timeout_ms)
{
	if (g_intr) {
		sleep_on_fd(timeout_ms);
		spdk_fd_group_wait(g_fgrp, 0);
	} else {
		spdk_nvme_poll_group_process_completions(g_pg, 0, disconnected_cb);
	}
}

static void raw_wait_all(void)
{
	while (g_outstanding > 0) {
		raw_wait_once(1000);
	}
}

static int raw_submit(void *buf, uint64_t lba, bool write)
{
	g_outstanding++;
	uint32_t n = BS / spdk_nvme_ns_get_sector_size(g_ns);
	int rc = write ? spdk_nvme_ns_cmd_write(g_ns, g_qp, buf, lba, n, io_cb, NULL, 0)
		 : spdk_nvme_ns_cmd_read(g_ns, g_qp, buf, lba, n, io_cb, NULL, 0);
	if (rc) {
		g_outstanding--;
		printf("  submit failed: %s\n", strerror(-rc));
	}
	return rc;
}

static void raw_qd1(void *buf)
{
	static uint64_t lat[2][1 << 20];
	uint64_t lbas_per_io = BS / spdk_nvme_ns_get_sector_size(g_ns);
	g_errors = 0;
	g_wakeups = 0;
	g_sys0 = sys_busy_ns();
	uint64_t cpu0 = cpu_ns(), t0 = now_ns();
	for (int i = 0; i < g_n; i++) {
		for (int w = 1; w >= 0; w--) {
			uint64_t s = now_ns();
			if (raw_submit(buf, (uint64_t)(i % 256) * lbas_per_io, w)) {
				return;
			}
			raw_wait_all();
			lat[!w][i] = now_ns() - s;
		}
	}
	report("nvme", lat[0], lat[1], now_ns() - t0, cpu_ns() - cpu0, g_errors);
}

static void raw_idle(void)
{
	uint64_t c0 = cpu_ns(), s0 = sys_busy_ns(), t0 = now_ns();
	while (now_ns() - t0 < 1000000000ull) {
		raw_wait_once(100);
	}
	printf("  idle 1 s: CPU %.1f%% (all CPUs %.1f%% of one CPU)\n", 100.0 * (cpu_ns() - c0) / (now_ns() - t0),
	       100.0 * (sys_busy_ns() - s0) / (now_ns() - t0));
}

static void raw_qd32(void **bufs)
{
	uint64_t lbas_per_io = BS / spdk_nvme_ns_get_sector_size(g_ns);
	uint64_t span = spdk_min(spdk_nvme_ns_get_num_sectors(g_ns) / lbas_per_io, 65536);
	uint64_t done = 0, seed = 1;
	g_errors = 0;
	g_wakeups = 0;
	uint64_t s0 = sys_busy_ns(), cpu0 = cpu_ns(), t0 = now_ns();
	while (now_ns() - t0 < 2000000000ull) {
		while (g_outstanding < QD_DEEP) {
			seed = seed * 6364136223846793005ull + 1442695040888963407ull;
			if (raw_submit(bufs[g_outstanding], (seed >> 33) % span * lbas_per_io, false)) {
				return;
			}
		}
		int before = g_outstanding;
		raw_wait_once(1000);
		done += before - g_outstanding;
	}
	raw_wait_all();
	uint64_t wall = now_ns() - t0, cpu = cpu_ns() - cpu0, sys = sys_busy_ns() - s0;
	printf("  4K randread QD%d: %.0f IOPS, CPU %5.1f%% of wall, %5.2f us/op (all CPUs %5.2f us/op), wakeups/op %.2f, failed %d\n",
	       QD_DEEP, done * 1e9 / wall, 100.0 * cpu / wall, cpu / 1e3 / done, sys / 1e3 / done,
	       (double)g_wakeups / done, g_errors);
}

static int run_raw(struct spdk_nvme_transport_id *trid)
{
	printf("N1 attach %s %s, enable_interrupts=%d\n", trid->trstring, trid->traddr, g_intr);
	int efd0 = count_eventfds();
	if (spdk_nvme_probe(trid, NULL, probe_cb, attach_cb, NULL) || !g_ctrlr) {
		printf("  [FAIL] spdk_nvme_probe found no controller\n");
		return 1;
	}
	const struct spdk_nvme_ctrlr_opts *o = spdk_nvme_ctrlr_get_opts(g_ctrlr);
	check("ctrlr opts.enable_interrupts after attach", o->enable_interrupts, g_intr);
	if (o->enable_interrupts != g_intr && !g_trust_request) {
		printf("  attach fell back to polling; running in poll mode\n");
		g_intr = o->enable_interrupts;
	}
	printf("  num_io_queues %u, admin qpair fd %d, eventfds opened by attach: %d\n",
	       o->num_io_queues, spdk_nvme_ctrlr_get_admin_qp_fd(g_ctrlr, NULL), count_eventfds() - efd0);
	g_ns = spdk_nvme_ctrlr_get_ns(g_ctrlr, spdk_nvme_ctrlr_get_first_active_ns(g_ctrlr));
	printf("  ns %u: %lu sectors of %u B\n", spdk_nvme_ns_get_id(g_ns),
	       spdk_nvme_ns_get_num_sectors(g_ns), spdk_nvme_ns_get_sector_size(g_ns));

	g_pg = spdk_nvme_poll_group_create(NULL, NULL);
	struct spdk_nvme_io_qpair_opts qo;
	spdk_nvme_ctrlr_get_default_io_qpair_opts(g_ctrlr, &qo, sizeof(qo));
	qo.create_only = true;
	qo.delay_cmd_submit = false;
	g_qp = spdk_nvme_ctrlr_alloc_io_qpair(g_ctrlr, &qo, sizeof(qo));
	int rc = g_qp ? spdk_nvme_poll_group_add(g_pg, g_qp) : -ENOMEM;
	rc = rc ? rc : spdk_nvme_ctrlr_connect_io_qpair(g_ctrlr, g_qp);
	if (rc) {
		printf("  [FAIL] I/O qpair: %s\n", strerror(-rc));
		return 1;
	}
	g_fgrp = spdk_nvme_poll_group_get_fd_group(g_pg);
	printf("  I/O qpair fd %d, poll group fd_group fd %d\n", spdk_nvme_qpair_get_fd(g_qp, NULL),
	       g_fgrp ? spdk_fd_group_get_fd(g_fgrp) : -1);
	if (g_intr) {
		check("I/O qpair has an eventfd", spdk_nvme_qpair_get_fd(g_qp, NULL) >= 0, 1);
		epoll_add(spdk_fd_group_get_fd(g_fgrp));
	}

	void *bufs[QD_DEEP];
	for (int i = 0; i < QD_DEEP; i++) {
		bufs[i] = spdk_dma_zmalloc(BS, BS, NULL);
		memset(bufs[i], 0x5a, BS);
	}
	uint64_t sz = BS;
	check("spdk_vtophys(dma buf) ok", spdk_vtophys(bufs[0], &sz) != SPDK_VTOPHYS_ERROR, 1);

	int vectors;
	uint64_t irq0 = vfio_irqs(&vectors);
	printf("  vfio interrupt vectors in /proc/interrupts: %d\n", vectors);
	printf("N2 4K QD1, %d writes + %d reads, mode %s\n", g_n, g_n, g_intr ? "interrupt" : "poll");
	raw_qd1(bufs[0]);
	printf("  vfio interrupts during N2: %.2f per I/O\n", (double)(vfio_irqs(&vectors) - irq0) / (2 * g_n));
	printf("N3 idle\n");
	raw_idle();
	printf("N4 throughput\n");
	irq0 = vfio_irqs(&vectors);
	raw_qd32(bufs);
	printf("  vfio interrupts during N4: %lu\n", vfio_irqs(&vectors) - irq0);

	for (int i = 0; i < QD_DEEP; i++) {
		spdk_dma_free(bufs[i]);
	}
	spdk_nvme_ctrlr_free_io_qpair(g_qp);
	spdk_nvme_poll_group_destroy(g_pg);
	struct spdk_nvme_detach_ctx *dctx = NULL;
	spdk_nvme_detach_async(g_ctrlr, &dctx);
	if (dctx) {
		spdk_nvme_detach_poll(dctx);
	}
	printf("  detach ok\n");
	return 0;
}

/* ---- bdev_nvme in one spdk_thread ---- */

static struct spdk_thread *g_thread;
static volatile bool g_flag;
static int g_rc;
static int g_io_ok;

static void thread_loop_once(int timeout_ms)
{
	if (g_intr) {
		sleep_on_fd(timeout_ms);
	}
	spdk_thread_poll(g_thread, 0, 0);
}

static void run_until_flag(void)
{
	while (!g_flag) {
		thread_loop_once(1000);
	}
	g_flag = false;
}

static void set_flag(int rc, void *arg)
{
	g_rc = rc;
	g_flag = true;
}

static void fini_done(void *arg)
{
	g_flag = true;
}

static void create_done(void *ctx, size_t count, int rc)
{
	printf("  spdk_bdev_nvme_create done: %zu bdevs, rc %d\n", count, rc);
	set_flag(rc, NULL);
}

static void delete_done(void *ctx, int rc)
{
	set_flag(rc, NULL);
}

static void bio_done(struct spdk_bdev_io *io, bool ok, void *arg)
{
	g_io_ok = ok;
	spdk_bdev_free_io(io);
	g_flag = true;
}

static void bdev_event(enum spdk_bdev_event_type t, struct spdk_bdev *b, void *ctx) {}

static int thread_op(struct spdk_thread *t, enum spdk_thread_op op)
{
	return 0;
}

static bool thread_op_supported(enum spdk_thread_op op)
{
	return true;
}

static int run_bdev(struct spdk_nvme_transport_id *trid)
{
	if (g_intr && spdk_interrupt_mode_enable()) {
		printf("  spdk_interrupt_mode_enable FAILED\n");
		return 1;
	}
	spdk_thread_lib_init_ext(thread_op, thread_op_supported, 0, SPDK_DEFAULT_MSG_MEMPOOL_SIZE);
	g_thread = spdk_thread_create("rs-disk", NULL);
	spdk_set_thread(g_thread);
	if (g_intr) {
		epoll_add(spdk_thread_get_interrupt_fd(g_thread));
	}
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

	printf("B1 spdk_bdev_nvme_create %s %s, thread mode %s\n", trid->trstring, trid->traddr,
	       g_intr ? "interrupt" : "poll");
	static const char *names[8];
	struct spdk_nvme_ctrlr_opts dopts = {};
	spdk_nvme_ctrlr_get_default_ctrlr_opts(&dopts, sizeof(dopts));
	struct spdk_bdev_nvme_ctrlr_opts bopts = {};
	spdk_bdev_nvme_get_default_ctrlr_opts(&bopts);
	int rc = spdk_bdev_nvme_create(trid, "Nvme0", names, 8, create_done, NULL, &dopts, &bopts);
	if (rc) {
		printf("  [FAIL] spdk_bdev_nvme_create returned %d (%s)\n", rc, strerror(-rc));
	} else {
		run_until_flag();
	}
	if (rc == 0 && g_rc == 0) {
		struct spdk_bdev_desc *desc;
		rc = spdk_bdev_open_ext("Nvme0n1", true, bdev_event, NULL, &desc);
		check("open Nvme0n1", rc, 0);
		struct spdk_io_channel *ch = rc ? NULL : spdk_bdev_get_io_channel(desc);
		if (ch) {
			void *buf = spdk_dma_zmalloc(BS, BS, NULL);
			static uint64_t lat[2][1 << 20];
			int fails = 0;
			g_wakeups = 0;
			g_sys0 = sys_busy_ns();
			printf("B2 4K QD1, %d writes + %d reads\n", g_n, g_n);
			uint64_t cpu0 = cpu_ns(), t0 = now_ns();
			for (int i = 0; i < g_n; i++) {
				for (int w = 1; w >= 0; w--) {
					uint64_t s = now_ns(), off = (uint64_t)(i % 256) * BS;
					rc = w ? spdk_bdev_write(desc, ch, buf, off, BS, bio_done, NULL)
					     : spdk_bdev_read(desc, ch, buf, off, BS, bio_done, NULL);
					if (rc) {
						printf("  submit failed: %s\n", strerror(-rc));
						break;
					}
					run_until_flag();
					fails += !g_io_ok;
					lat[!w][i] = now_ns() - s;
				}
			}
			report("bdev", lat[0], lat[1], now_ns() - t0, cpu_ns() - cpu0, fails);
			printf("B3 idle\n");
			uint64_t c0 = cpu_ns();
			t0 = now_ns();
			while (now_ns() - t0 < 1000000000ull) {
				thread_loop_once(100);
			}
			printf("  idle 1 s: CPU %.1f%%\n", 100.0 * (cpu_ns() - c0) / (now_ns() - t0));
			spdk_dma_free(buf);
			spdk_put_io_channel(ch);
		}
		if (!rc) {
			spdk_bdev_close(desc);
		}
		struct spdk_bdev_nvme_path_id path = {};
		path.trid = *trid;
		if (spdk_bdev_nvme_delete("Nvme0", &path, delete_done, NULL) == 0) {
			run_until_flag();
		}
	}
	spdk_subsystem_fini(fini_done, NULL);
	run_until_flag();
	spdk_thread_exit(g_thread);
	while (!spdk_thread_is_exited(g_thread)) {
		spdk_thread_poll(g_thread, 0, 0);
	}
	spdk_thread_destroy(g_thread);
	spdk_thread_lib_fini();
	printf("  clean shutdown ok\n");
	return 0;
}

int main(int argc, char **argv)
{
	int mem = 256;
	bool no_huge = false, bdev = false;
	uint64_t base = 0;
	const char *trstr = NULL;
	setvbuf(stdout, NULL, _IOLBF, 0);
	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "--interrupt")) {
			g_intr = true;
		} else if (!strcmp(argv[i], "--trust-request")) {
			g_trust_request = true;
		} else if (!strcmp(argv[i], "--bdev")) {
			bdev = true;
		} else if (!strcmp(argv[i], "--no-huge")) {
			no_huge = true;
		} else if (!strcmp(argv[i], "--mem")) {
			mem = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "--base")) {
			base = strtoull(argv[++i], NULL, 0);
		} else if (!strcmp(argv[i], "--n")) {
			g_n = atoi(argv[++i]);
			g_n = spdk_min(g_n, 1 << 20);
		} else {
			trstr = argv[i];
		}
	}
	if (!trstr) {
		fprintf(stderr, "usage: %s [--interrupt] [--trust-request] [--bdev] [--no-huge] [--mem MB] [--base ADDR] [--n N] <trid>\n",
			argv[0]);
		return 2;
	}
	struct spdk_nvme_transport_id trid = {};
	if (spdk_nvme_transport_id_parse(&trid, trstr)) {
		fprintf(stderr, "bad trid %s\n", trstr);
		return 2;
	}
	struct spdk_env_opts opts = { .opts_size = sizeof(opts) };
	spdk_env_opts_init(&opts);
	opts.name = "rs-nvme-intr";
	opts.core_mask = "0x1";
	opts.mem_size = mem;
	opts.no_huge = no_huge;
	opts.no_pci = trid.trtype != SPDK_NVME_TRANSPORT_PCIE;
	if (base) {
		opts.base_virtaddr = base;
	}
	struct spdk_pci_addr allow;
	if (!opts.no_pci && spdk_pci_addr_parse(&allow, trid.traddr) == 0) {
		opts.pci_allowed = &allow;
		opts.num_pci_addr = 1;
	}
	printf("env: uid=%d, %s, mem %d MB, pci %s\n", getuid(), no_huge ? "no_huge" : "hugepages",
	       mem, opts.no_pci ? "off" : trid.traddr);
	spdk_log_set_print_level(SPDK_LOG_NOTICE);
	if (spdk_env_init(&opts)) {
		printf("  [FAIL] spdk_env_init\n");
		return 1;
	}
	spdk_log_set_print_level(SPDK_LOG_WARN);
	int rc = bdev ? run_bdev(&trid) : run_raw(&trid);
	spdk_env_fini();
	return rc;
}
