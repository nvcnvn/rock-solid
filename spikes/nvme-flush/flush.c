// NVMe flush latency spike: O_DIRECT pwrite + fdatasync on a preallocated, fully written file,
// with device-side flush counts from /sys/block/<dev>/stat. Prints [OK ]/[FAIL] checks.
//
// usage: flush <test> <file> [key=value ...]
//   prep    size_mb=256 fill=1       fallocate and write the file once, so later writes are overwrites
//   qd1     bs=4096 n=2000 mode=fdatasync|dsync|fsync|empty|none gap_us=0 spin=0 seq=0 want_fpo_x100=100
//   sweep   n=..                     bytes per flush: 4K..4M written, then one fdatasync
//   gaps    n=..                     idle gap before each write+fdatasync, sleeping and spinning
//   split   n=300                    one 64K record made durable with 1, 16 or 2 flushes
//   threads secs=5 max=8 cpu=-1      1, 2, 4, 8 threads each doing write+fdatasync, optionally on one CPU
//   bg      secs=5                   flushes and writes other processes send while we are idle
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>


static const char *g_dev = "nvme0n1";
static int g_bg_secs = 3;
static char **g_argv;
static int g_argc;

static long arg(const char *key, long def)
{
	size_t k = strlen(key);
	for (int i = 3; i < g_argc; i++)
		if (!strncmp(g_argv[i], key, k) && g_argv[i][k] == '=')
			return strtol(g_argv[i] + k + 1, NULL, 0);
	return def;
}

static const char *sarg(const char *key, const char *def)
{
	size_t k = strlen(key);
	for (int i = 3; i < g_argc; i++)
		if (!strncmp(g_argv[i], key, k) && g_argv[i][k] == '=')
			return g_argv[i] + k + 1;
	return def;
}

static void die(const char *what)
{
	fprintf(stderr, "%s: %s\n", what, strerror(errno));
	exit(2);
}

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000u + ts.tv_nsec;
}

static uint64_t cpu_ns(int who)
{
	struct rusage ru;
	getrusage(who, &ru);
	return (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000000ull +
	       (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) * 1000ull;
}

struct dstat {
	uint64_t writes, wsectors, wticks, flushes, fticks;
	uint64_t t;
};

static struct dstat dstat_read(void)
{
	char path[128];
	unsigned long long f[17] = {0};
	struct dstat d = {0};
	snprintf(path, sizeof path, "/sys/block/%s/stat", g_dev);
	FILE *fp = fopen(path, "r");
	if (!fp)
		die(path);
	for (int i = 0; i < 17; i++)
		if (fscanf(fp, "%llu", &f[i]) != 1)
			break;
	fclose(fp);
	d.writes = f[4];
	d.wsectors = f[6];
	d.wticks = f[7];
	d.flushes = f[15];
	d.fticks = f[16];
	d.t = now_ns();
	return d;
}

static double g_bg_flush_rate, g_bg_write_rate;

static void loadavg(char *buf, size_t n)
{
	FILE *fp = fopen("/proc/loadavg", "r");
	buf[0] = 0;
	if (fp) {
		if (!fgets(buf, (int)n, fp))
			buf[0] = 0;
		fclose(fp);
	}
	buf[strcspn(buf, "\n")] = 0;
}

static void measure_bg(int secs)
{
	char la[128];
	struct dstat a = dstat_read();
	sleep(secs);
	struct dstat b = dstat_read();
	double s = (b.t - a.t) / 1e9;
	g_bg_flush_rate = (b.flushes - a.flushes) / s;
	g_bg_write_rate = (b.writes - a.writes) / s;
	loadavg(la, sizeof la);
	printf("    background over %d s idle: %.1f flushes/s, %.1f writes/s (other processes), loadavg %s\n",
	       secs, g_bg_flush_rate, g_bg_write_rate, la);
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

struct pct {
	double p50, p90, p99, p999, max, mean;
};

static struct pct percentiles(uint64_t *v, size_t n)
{
	struct pct p = {0};
	if (!n)
		return p;
	qsort(v, n, sizeof *v, cmp_u64);
	double sum = 0;
	for (size_t i = 0; i < n; i++)
		sum += v[i];
#define AT(q) (v[(size_t)fmin((double)n - 1, floor((q) * n))] / 1000.0)
	p.p50 = AT(0.50);
	p.p90 = AT(0.90);
	p.p99 = AT(0.99);
	p.p999 = AT(0.999);
#undef AT
	p.max = v[n - 1] / 1000.0;
	p.mean = sum / n / 1000.0;
	return p;
}

static void print_pct(const char *label, struct pct p)
{
	printf("    %-6s p50=%8.1f p90=%8.1f p99=%8.1f p99.9=%8.1f max=%8.1f mean=%8.1f us\n", label, p.p50,
	       p.p90, p.p99, p.p999, p.max, p.mean);
}

struct devdelta {
	double secs, fpo, fpo_adj, wpo, kbpo, flush_ms;
	uint64_t flushes;
};

static struct devdelta devdelta(struct dstat a, struct dstat b, size_t ops)
{
	struct devdelta d = {0};
	d.secs = (b.t - a.t) / 1e9;
	d.flushes = b.flushes - a.flushes;
	d.fpo = (double)d.flushes / ops;
	d.fpo_adj = ((double)d.flushes - g_bg_flush_rate * d.secs) / ops;
	d.wpo = (double)(b.writes - a.writes) / ops;
	d.flush_ms = d.flushes ? (double)(b.fticks - a.fticks) / d.flushes : 0;
	d.kbpo = (b.wsectors - a.wsectors) / 2.0 / ops;
	return d;
}

static void print_dev(struct devdelta d)
{
	printf("    device: %.2f flushes/op (%.2f after subtracting background), %.2f write requests/op "
	       "(an fdatasync's empty PREFLUSH write counts as one), %.1f KiB written/op, device time per flush %.3f ms, "
	       "%.1f s\n",
	       d.fpo, d.fpo_adj, d.wpo, d.kbpo, d.flush_ms, d.secs);
}

static int g_fail;

static void check(const char *id, const char *what, int ok, const char *got, const char *want)
{
	printf("[%s] %s %s got=%s want=%s\n", ok ? "OK " : "FAIL", id, what, got, want);
	if (!ok)
		g_fail = 1;
}

static __thread uint64_t g_rng = 0x9e3779b97f4a7c15ull;

static uint64_t rnd(void)
{
	g_rng ^= g_rng << 13;
	g_rng ^= g_rng >> 7;
	g_rng ^= g_rng << 17;
	return g_rng;
}

static char *alloc_buf(size_t n)
{
	char *b;
	if (posix_memalign((void **)&b, 4096, n))
		die("posix_memalign");
	for (size_t i = 0; i < n; i += 8) {
		uint64_t r = rnd();
		memcpy(b + i, &r, 8);
	}
	return b;
}

static off_t file_size(int fd)
{
	struct stat st;
	if (fstat(fd, &st))
		die("fstat");
	return st.st_size;
}

static int open_file(const char *path, int extra)
{
	int fd = open(path, O_RDWR | O_DIRECT | extra);
	if (fd < 0)
		die(path);
	return fd;
}

static void sleep_us(long us)
{
	struct timespec ts = {us / 1000000, (us % 1000000) * 1000};
	while (clock_nanosleep(CLOCK_MONOTONIC, 0, &ts, &ts) == EINTR)
		;
}

static void spin_us(long us)
{
	uint64_t end = now_ns() + (uint64_t)us * 1000;
	while (now_ns() < end)
		;
}

static int prep(const char *path)
{
	long mb = arg("size_mb", 256);
	off_t size = (off_t)mb << 20;
	int fd = open(path, O_RDWR | O_CREAT | O_DIRECT, 0644);
	if (fd < 0)
		die(path);
	if (file_size(fd) == size) {
		printf("    %s already %ld MiB, reusing\n", path, mb);
		close(fd);
		return 0;
	}
	if (ftruncate(fd, 0) || fallocate(fd, 0, 0, size))
		die("fallocate");
	if (!arg("fill", 1)) {
		printf("    fallocated %ld MiB, extents left unwritten\n", mb);
		close(fd);
		return 0;
	}
	char *buf = alloc_buf(1 << 20);
	uint64_t t0 = now_ns();
	for (off_t off = 0; off < size; off += 1 << 20)
		if (pwrite(fd, buf, 1 << 20, off) != 1 << 20)
			die("pwrite");
	if (fsync(fd))
		die("fsync");
	printf("    wrote %ld MiB in %.1f ms (fallocate, then every block once, then fsync)\n", mb,
	       (now_ns() - t0) / 1e6);
	close(fd);
	free(buf);
	return 0;
}

struct run {
	size_t n;
	uint64_t *wr, *fl, *tot;
	uint64_t cpu;
	struct devdelta dev;
};

enum mode { M_FDATASYNC, M_DSYNC, M_FSYNC, M_EMPTY, M_NONE };

static enum mode parse_mode(const char *s)
{
	if (!strcmp(s, "dsync"))
		return M_DSYNC;
	if (!strcmp(s, "fsync"))
		return M_FSYNC;
	if (!strcmp(s, "empty"))
		return M_EMPTY;
	if (!strcmp(s, "none"))
		return M_NONE;
	return M_FDATASYNC;
}

// One durable op per iteration: pwrite of bs bytes at a random bs-aligned offset, then the flush
// the mode asks for. Write and flush are timed separately.
static struct run qd1_run(const char *path, size_t bs, size_t n, enum mode m, long gap_us, int spin)
{
	struct run r = {.n = n};
	int fd = open_file(path, m == M_DSYNC ? O_DSYNC : 0);
	off_t slots = file_size(fd) / (off_t)bs;
	char *buf = alloc_buf(bs);
	r.wr = calloc(n, 8);
	r.fl = calloc(n, 8);
	r.tot = calloc(n, 8);
	if (pwrite(fd, buf, bs, 0) != (ssize_t)bs || fdatasync(fd))
		die("warmup");
	int seq = (int)arg("seq", 0);
	off_t start = (off_t)(rnd() % slots);
	struct dstat a = dstat_read();
	for (size_t i = 0; i < n; i++) {
		if (gap_us)
			spin ? spin_us(gap_us) : sleep_us(gap_us);
		off_t off = (seq ? (start + (off_t)i) % slots : (off_t)(rnd() % slots)) * (off_t)bs;
		memcpy(buf, &i, sizeof i);
		uint64_t c0 = cpu_ns(RUSAGE_THREAD);
		uint64_t t0 = now_ns();
		if (m != M_EMPTY && pwrite(fd, buf, bs, off) != (ssize_t)bs)
			die("pwrite");
		uint64_t t1 = now_ns();
		if (m == M_FDATASYNC || m == M_EMPTY) {
			if (fdatasync(fd))
				die("fdatasync");
		} else if (m == M_FSYNC) {
			if (fsync(fd))
				die("fsync");
		}
		uint64_t t2 = now_ns();
		r.cpu += cpu_ns(RUSAGE_THREAD) - c0;
		r.wr[i] = t1 - t0;
		r.fl[i] = t2 - t1;
		r.tot[i] = t2 - t0;
	}
	r.dev = devdelta(a, dstat_read(), n);
	close(fd);
	free(buf);
	return r;
}

static void run_free(struct run *r)
{
	free(r->wr);
	free(r->fl);
	free(r->tot);
}

static int qd1(const char *id, const char *path)
{
	size_t bs = arg("bs", 4096);
	size_t n = arg("n", 2000);
	const char *ms = sarg("mode", "fdatasync");
	long gap = arg("gap_us", 0);
	double want_fpo = arg("want_fpo_x100", 100) / 100.0;
	measure_bg(g_bg_secs);
	struct run r = qd1_run(path, bs, n, parse_mode(ms), gap, (int)arg("spin", 0));
	printf("    %s bs=%zu n=%zu mode=%s gap_us=%ld offsets=%s\n", id, bs, n, ms, gap,
	       arg("seq", 0) ? "sequential" : "random");
	print_pct("write", percentiles(r.wr, n));
	print_pct("flush", percentiles(r.fl, n));
	struct pct t = percentiles(r.tot, n);
	print_pct("total", t);
	printf("    cpu/op=%.1f us, %.0f durable ops/s\n", r.cpu / 1e3 / n, n / r.dev.secs);
	print_dev(r.dev);
	char got[64], want[64];
	snprintf(got, sizeof got, "%.2f", r.dev.fpo_adj);
	snprintf(want, sizeof want, "%.2f", want_fpo);
	check(id, "device flushes per op", fabs(r.dev.fpo_adj - want_fpo) < 0.15, got, want);
	run_free(&r);
	return 0;
}

static int sweep(const char *id, const char *path)
{
	static const size_t sizes[] = {4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20};
	double flush50[6], tot50[6];
	measure_bg(g_bg_secs);
	printf("    %-8s %6s %10s %10s %10s %10s %10s %10s %8s %9s\n", "bytes", "n", "write_p50", "flush_p50",
	       "flush_p99", "total_p50", "total_p99", "MB/s", "cpu/op", "dev_fl_ms");
	for (int i = 0; i < 6; i++) {
		size_t bs = sizes[i];
		long budget = arg("budget_mb", 64) << 20;
		size_t n = (size_t)fmax((double)arg("min_n", 32), fmin((double)arg("max_n", 2000), (double)budget / bs));
		struct run r = qd1_run(path, bs, n, M_FDATASYNC, 0, 0);
		struct pct w = percentiles(r.wr, n), f = percentiles(r.fl, n), t = percentiles(r.tot, n);
		flush50[i] = f.p50;
		tot50[i] = t.p50;
		printf("    %-8zu %6zu %10.1f %10.1f %10.1f %10.1f %10.1f %10.1f %8.1f %9.3f\n", bs, n, w.p50, f.p50,
		       f.p99, t.p50, t.p99, bs * n / 1e6 / r.dev.secs, r.cpu / 1e3 / n, r.dev.flush_ms);
		run_free(&r);
	}
	char got[64];
	snprintf(got, sizeof got, "%.2fx", flush50[3] / flush50[0]);
	check(id, "flush p50 at 256K vs 4K (flush cost roughly fixed)", flush50[3] / flush50[0] <= 2.0, got, "<=2x");
	snprintf(got, sizeof got, "%.2fx", flush50[5] / flush50[0]);
	check(id, "flush p50 at 4M vs 4K", flush50[5] / flush50[0] <= 2.0, got, "<=2x");
	snprintf(got, sizeof got, "%.2fx", tot50[3] / tot50[0]);
	check(id, "commit p50 at 256K vs 4K (group commit amortizes)", tot50[3] / tot50[0] <= 2.0, got, "<=2x");
	return 0;
}

static int gaps(const char *id, const char *path)
{
	static const long gap_us[] = {0, 1000, 10000, 100000, 1000000};
	double p50[2][5];
	measure_bg(g_bg_secs);
	printf("    %-6s %-5s %5s %10s %10s %10s %10s %10s %9s\n", "gap_us", "wait", "n", "write_p50", "flush_p50",
	       "total_p50", "total_p90", "total_max", "dev_fl_ms");
	for (int spin = 0; spin < 2; spin++)
		for (int i = 0; i < 5; i++) {
			long g = gap_us[i];
			if (spin && g == 0)
				continue;
			size_t n = g >= 1000000 ? (size_t)arg("n_1s", 20) : g >= 100000 ? 50 : 300;
			struct run r = qd1_run(path, 4096, n, M_FDATASYNC, g, spin);
			struct pct w = percentiles(r.wr, n), f = percentiles(r.fl, n), t = percentiles(r.tot, n);
			p50[spin][i] = t.p50;
			printf("    %-6ld %-5s %5zu %10.1f %10.1f %10.1f %10.1f %10.1f %9.3f\n", g, spin ? "spin" : "sleep",
			       n, w.p50, f.p50, t.p50, t.p90, t.max, r.dev.flush_ms);
			run_free(&r);
		}
	char got[64];
	snprintf(got, sizeof got, "%.2fx", p50[0][4] / p50[0][0]);
	check(id, "commit p50 after 1 s idle (sleep) vs back-to-back", p50[0][4] / p50[0][0] > 1.5, got, ">1.5x (APST wake-up)");
	snprintf(got, sizeof got, "%.2fx", p50[1][4] / p50[0][0]);
	check(id, "commit p50 after 1 s idle (spin) vs back-to-back", p50[1][4] / p50[0][0] > 1.5, got, ">1.5x (device-side)");
	return 0;
}

// One 64 KiB record made durable four ways: one write and one flush, 16 writes and one flush, a
// flush after each 4 KiB, and data then a separate header write with a flush after each.
static int split(const char *id, const char *path)
{
	size_t n = arg("n", 300), k = 16, bs = 4096;
	int fd = open_file(path, 0);
	off_t slots = file_size(fd) / (off_t)(k * bs);
	char *buf = alloc_buf(k * bs);
	static const char *names[] = {"1 write of 64K + 1 fdatasync", "16 writes of 4K + 1 fdatasync",
				      "16 x (4K write + fdatasync)", "60K data + fdatasync, 4K header + fdatasync"};
	double p50[4];
	measure_bg(g_bg_secs);
	printf("    %-45s %10s %10s %10s %8s %9s\n", "pattern", "commit_p50", "commit_p99", "commit_max", "fl/op",
	       "dev_fl_ms");
	for (int v = 0; v < 4; v++) {
		uint64_t *lat = calloc(n, 8);
		struct dstat a = dstat_read();
		for (size_t i = 0; i < n; i++) {
			off_t base = (off_t)(rnd() % slots) * (off_t)(k * bs);
			uint64_t t0 = now_ns();
			int bad = 0;
			if (v == 0) {
				bad |= pwrite(fd, buf, k * bs, base) != (ssize_t)(k * bs);
				bad |= fdatasync(fd);
			} else if (v == 1 || v == 2) {
				for (size_t j = 0; j < k; j++) {
					bad |= pwrite(fd, buf + j * bs, bs, base + (off_t)(j * bs)) != (ssize_t)bs;
					if (v == 2)
						bad |= fdatasync(fd);
				}
				if (v == 1)
					bad |= fdatasync(fd);
			} else {
				bad |= pwrite(fd, buf + bs, (k - 1) * bs, base + (off_t)bs) != (ssize_t)((k - 1) * bs);
				bad |= fdatasync(fd);
				bad |= pwrite(fd, buf, bs, base) != (ssize_t)bs;
				bad |= fdatasync(fd);
			}
			if (bad)
				die("split");
			lat[i] = now_ns() - t0;
		}
		struct devdelta d = devdelta(a, dstat_read(), n);
		struct pct p = percentiles(lat, n);
		p50[v] = p.p50;
		printf("    %-45s %10.1f %10.1f %10.1f %8.2f %9.3f\n", names[v], p.p50, p.p99, p.max, d.fpo_adj,
		       d.flush_ms);
		free(lat);
	}
	char got[64];
	snprintf(got, sizeof got, "%.2fx", p50[3] / p50[0]);
	check(id, "two flushes per commit vs one", p50[3] / p50[0] > 1.7, got, ">1.7x (each flush is a full cost)");
	snprintf(got, sizeof got, "%.2fx", p50[1] / p50[0]);
	check(id, "16 small writes + 1 flush vs 1 write + 1 flush", p50[1] / p50[0] < 1.3, got, "<1.3x");
	close(fd);
	free(buf);
	return 0;
}

struct tctx {
	const char *path;
	int fd;
	off_t base, slots;
	uint64_t end;
	size_t n, cap;
	uint64_t *lat;
};

static long g_pin_cpu = -1;

static void *tbody(void *p)
{
	struct tctx *c = p;
	if (g_pin_cpu >= 0) {
		cpu_set_t set;
		CPU_ZERO(&set);
		CPU_SET(g_pin_cpu, &set);
		pthread_setaffinity_np(pthread_self(), sizeof set, &set);
	}
	char *buf = alloc_buf(4096);
	while (now_ns() < c->end) {
		off_t off = c->base + (off_t)(rnd() % c->slots) * 4096;
		uint64_t t0 = now_ns();
		if (pwrite(c->fd, buf, 4096, off) != 4096 || fdatasync(c->fd))
			die("pwrite/fdatasync");
		if (c->n < c->cap)
			c->lat[c->n++] = now_ns() - t0;
	}
	free(buf);
	return NULL;
}

static int threads(const char *id, const char *path)
{
	long secs = arg("secs", 5), max = arg("max", 8);
	double ops1 = 0, fpo_at_max = 0, ops_at_max = 0;
	g_pin_cpu = arg("cpu", -1);
	measure_bg(g_bg_secs);
	printf("    threads %s\n", g_pin_cpu >= 0 ? "all pinned to one CPU (one blk-mq hardware queue)" : "unpinned");
	printf("    %-7s %8s %10s %10s %10s %10s %9s %9s %8s\n", "threads", "ops/s", "lat_p50", "lat_p99", "lat_max",
	       "fl/op", "fl/op_adj", "dev_fl_ms", "cpu/op");
	for (long t = 1; t <= max; t *= 2) {
		int fd = open_file(path, 0);
		off_t per = file_size(fd) / t / 4096;
		struct tctx c[64];
		pthread_t th[64];
		uint64_t end = now_ns() + (uint64_t)secs * 1000000000u;
		uint64_t c0 = cpu_ns(RUSAGE_SELF);
		struct dstat a = dstat_read();
		for (long i = 0; i < t; i++) {
			c[i] = (struct tctx){.path = path, .fd = fd, .base = i * per * 4096, .slots = per, .end = end,
					     .cap = 1 << 20};
			c[i].lat = malloc(c[i].cap * 8);
			pthread_create(&th[i], NULL, tbody, &c[i]);
		}
		size_t total = 0;
		for (long i = 0; i < t; i++) {
			pthread_join(th[i], NULL);
			total += c[i].n;
		}
		struct devdelta d = devdelta(a, dstat_read(), total);
		uint64_t cpu = cpu_ns(RUSAGE_SELF) - c0;
		uint64_t *all = malloc(total * 8);
		size_t k = 0;
		for (long i = 0; i < t; i++) {
			memcpy(all + k, c[i].lat, c[i].n * 8);
			k += c[i].n;
			free(c[i].lat);
		}
		struct pct p = percentiles(all, total);
		double ops = total / d.secs;
		printf("    %-7ld %8.0f %10.1f %10.1f %10.1f %10.2f %9.2f %9.3f %8.1f\n", t, ops, p.p50, p.p99, p.max, d.fpo,
		       d.fpo_adj, d.flush_ms, cpu / 1e3 / total);
		if (t == 1)
			ops1 = ops;
		fpo_at_max = d.fpo_adj;
		ops_at_max = ops;
		free(all);
		close(fd);
	}
	char got[64];
	snprintf(got, sizeof got, "%.2f", fpo_at_max);
	check(id, "device flushes per fdatasync at max threads (kernel merges flushes)", fpo_at_max < 0.9, got,
	      g_pin_cpu >= 0 ? "<0.9" : "<0.9 (expected before reading blk-flush.c)");
	snprintf(got, sizeof got, "%.2fx", ops_at_max / ops1);
	check(id, "durable ops/s at max threads vs 1 thread", ops_at_max / ops1 > 1.5, got, ">1.5x");
	return 0;
}

static int bg(void)
{
	measure_bg((int)arg("secs", 5));
	return 0;
}

int main(int argc, char **argv)
{
	if (argc < 3) {
		fprintf(stderr, "usage: %s <prep|qd1|sweep|split|gaps|threads|bg> <file> [key=value ...]\n", argv[0]);
		return 2;
	}
	g_argc = argc;
	g_argv = argv;
	if (getenv("DEV"))
		g_dev = getenv("DEV");
	g_bg_secs = (int)arg("bg_secs", 3);
	const char *id = sarg("id", argv[1]);
	const char *t = argv[1], *path = argv[2];
	int rc;
	if (!strcmp(t, "prep"))
		rc = prep(path);
	else if (!strcmp(t, "qd1"))
		rc = qd1(id, path);
	else if (!strcmp(t, "sweep"))
		rc = sweep(id, path);
	else if (!strcmp(t, "gaps"))
		rc = gaps(id, path);
	else if (!strcmp(t, "split"))
		rc = split(id, path);
	else if (!strcmp(t, "threads"))
		rc = threads(id, path);
	else if (!strcmp(t, "bg"))
		rc = bg();
	else {
		fprintf(stderr, "unknown test %s\n", t);
		return 2;
	}
	return rc ? rc : g_fail;
}
