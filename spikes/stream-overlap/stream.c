// Stream overlap spike: a hot stream of 4K records and a cold stream of large records on one disk,
// committed one round at a time (today's rule) or by two concurrent commit loops. Each commit is an
// O_DIRECT pwrite then fdatasync on a preallocated, fully written file; device flush counts come
// from /sys/block/<dev>/stat. Prints [OK ]/[FAIL] checks.
//
// usage: stream <test> <file> [key=value ...]
//   prep   size_mb=1024      fallocate and write the file once, so later writes are overwrites
//   bg     secs=5            flushes and writes other processes send while we are idle
//   suite  secs=5            H1-H5 for cold records of 64K, 256K and 1M
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define HOT_BS 4096
#define MAX_LAT (1 << 20)

static const char *g_dev = "nvme0n1";
static char **g_argv;
static int g_argc;
static const char *g_path;
static off_t g_size;
static int g_fail;
static double g_bg_flush_rate;

static long arg(const char *key, long def)
{
	size_t k = strlen(key);
	for (int i = 3; i < g_argc; i++)
		if (!strncmp(g_argv[i], key, k) && g_argv[i][k] == '=')
			return strtol(g_argv[i] + k + 1, NULL, 0);
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

static uint64_t thread_cpu_ns(void)
{
	struct rusage ru;
	getrusage(RUSAGE_THREAD, &ru);
	return (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000000ull +
	       (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) * 1000ull;
}

static void sleep_ns(uint64_t ns)
{
	struct timespec ts = {(time_t)(ns / 1000000000u), (long)(ns % 1000000000u)};
	while (nanosleep(&ts, &ts) && errno == EINTR)
		;
}

struct dstat {
	uint64_t writes, wsectors, flushes, fticks, t;
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
	d.flushes = f[15];
	d.fticks = f[16];
	d.t = now_ns();
	return d;
}

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

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

struct pct {
	double p50, p90, p99, max;
};

static struct pct percentiles(uint64_t *v, size_t n)
{
	struct pct p = {0};
	if (!n)
		return p;
	qsort(v, n, sizeof *v, cmp_u64);
#define AT(q) (v[(size_t)fmin((double)n - 1, floor((q) * n))] / 1000.0)
	p.p50 = AT(0.50);
	p.p90 = AT(0.90);
	p.p99 = AT(0.99);
#undef AT
	p.max = v[n - 1] / 1000.0;
	return p;
}

static void check(const char *id, const char *what, int ok, const char *got, const char *want)
{
	printf("[%s] %s %s got=%s want=%s\n", ok ? "OK " : "FAIL", id, what, got, want);
	if (!ok)
		g_fail = 1;
}

static void *aligned(size_t n)
{
	void *p;
	if (posix_memalign(&p, 4096, n))
		die("posix_memalign");
	for (size_t i = 0; i < n; i++)
		((unsigned char *)p)[i] = (unsigned char)(i * 131 + 7);
	return p;
}

static void prep(void)
{
	size_t mb = (size_t)arg("size_mb", 1024), chunk = 4u << 20;
	int fd = open(g_path, O_RDWR | O_CREAT | O_DIRECT, 0644);
	if (fd < 0)
		die(g_path);
	if (fallocate(fd, 0, 0, (off_t)mb << 20))
		die("fallocate");
	void *b = aligned(chunk);
	uint64_t t0 = now_ns();
	for (size_t off = 0; off < mb << 20; off += chunk)
		if (pwrite(fd, b, chunk, (off_t)off) != (ssize_t)chunk)
			die("pwrite");
	if (fdatasync(fd))
		die("fdatasync");
	printf("    prepared %zu MiB in %.0f ms (fallocate, then written in full once)\n", mb,
	       (now_ns() - t0) / 1e6);
	free(b);
	close(fd);
}

static void measure_bg(int secs)
{
	char la[128];
	struct dstat a = dstat_read();
	sleep((unsigned)secs);
	struct dstat b = dstat_read();
	g_bg_flush_rate = (b.flushes - a.flushes) / ((b.t - a.t) / 1e9);
	loadavg(la, sizeof la);
	printf("    background over %d s idle: %.1f flushes/s (other processes), loadavg %s\n", secs, g_bg_flush_rate,
	       la);
}

// A stream appends records sequentially into its own half of the file and wraps around, as a log
// stream appends into its open segment.
struct stream {
	int fd;
	size_t bs;
	off_t base, len, off;
	void *buf;
};

static struct stream stream_open(size_t bs, int cold)
{
	struct stream s = {0};
	s.fd = open(g_path, O_RDWR | O_DIRECT);
	if (s.fd < 0)
		die(g_path);
	s.bs = bs;
	s.len = g_size / 2;
	s.base = cold ? g_size / 2 : 0;
	s.buf = aligned(bs);
	return s;
}

static void stream_close(struct stream *s)
{
	close(s->fd);
	free(s->buf);
}

static void stream_write(struct stream *s)
{
	if (s->off + (off_t)s->bs > s->len)
		s->off = 0;
	if (pwrite(s->fd, s->buf, s->bs, s->base + s->off) != (ssize_t)s->bs)
		die("pwrite");
	s->off += (off_t)s->bs;
}

static void stream_flush(struct stream *s)
{
	if (fdatasync(s->fd))
		die("fdatasync");
}

struct result {
	uint64_t *lat, *wr, *fl;
	size_t n;
	uint64_t bytes, cpu_ns;
};

static struct result result_new(void)
{
	struct result r = {0};
	r.lat = calloc(MAX_LAT, sizeof *r.lat);
	r.wr = calloc(MAX_LAT, sizeof *r.wr);
	r.fl = calloc(MAX_LAT, sizeof *r.fl);
	if (!r.lat || !r.wr || !r.fl)
		die("calloc");
	return r;
}

static void result_free(struct result *r)
{
	free(r->lat);
	free(r->wr);
	free(r->fl);
}

static void record(struct result *r, uint64_t lat, uint64_t wr, uint64_t fl, size_t bytes)
{
	if (r->n < MAX_LAT) {
		r->lat[r->n] = lat;
		r->wr[r->n] = wr;
		r->fl[r->n] = fl;
	}
	r->n++;
	r->bytes += bytes;
}

enum mode {
	HOT_ONLY,
	COLD_ONLY,
	SERIAL,
	ONE_ROUND,
	CONCURRENT,
};

struct loop_arg {
	struct stream *s;
	struct result *r;
	volatile int *stop;
	int cpu;
	int duty;
};

static void pin(int cpu)
{
	if (cpu < 0)
		return;
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(cpu, &set);
	if (pthread_setaffinity_np(pthread_self(), sizeof set, &set))
		die("pthread_setaffinity_np");
}

// Each iteration is one closed-loop commit: write the record, flush, acknowledge. A cold loop below
// 100% duty idles for as long as its last commit took, scaled, so it uses that share of the time.
static void *commit_loop(void *p)
{
	struct loop_arg *a = p;
	pin(a->cpu);
	uint64_t c0 = thread_cpu_ns();
	while (!*a->stop) {
		uint64_t t0 = now_ns();
		stream_write(a->s);
		uint64_t t1 = now_ns();
		stream_flush(a->s);
		uint64_t t2 = now_ns();
		record(a->r, t2 - t0, t1 - t0, t2 - t1, a->s->bs);
		if (a->duty < 100)
			sleep_ns((t2 - t0) * (uint64_t)(100 - a->duty) / (uint64_t)a->duty);
	}
	a->r->cpu_ns = thread_cpu_ns() - c0;
	return NULL;
}

struct outcome {
	struct pct hot, hot_wr, hot_fl, cold;
	double hot_rate, cold_mbs, secs, flushes_per_round, hot_cpu_us, cold_cpu_us;
	size_t hot_n, cold_n;
};

static struct outcome run(enum mode m, size_t cold_bs, int secs, int hot_cpu, int cold_cpu, int duty)
{
	struct stream hot = stream_open(HOT_BS, 0), cold = stream_open(cold_bs ? cold_bs : HOT_BS, 1);
	struct result hr = result_new(), cr = result_new();
	volatile int stop = 0;
	struct dstat d0 = dstat_read();
	uint64_t t0 = now_ns(), end = t0 + (uint64_t)secs * 1000000000u;
	size_t rounds = 0;

	if (m == CONCURRENT) {
		struct loop_arg ha = {&hot, &hr, &stop, hot_cpu, 100}, ca = {&cold, &cr, &stop, cold_cpu, duty};
		pthread_t ht, ct;
		pthread_create(&ht, NULL, commit_loop, &ha);
		pthread_create(&ct, NULL, commit_loop, &ca);
		sleep_ns(end - now_ns());
		stop = 1;
		pthread_join(ht, NULL);
		pthread_join(ct, NULL);
		rounds = hr.n + cr.n;
	} else if (m == HOT_ONLY || m == COLD_ONLY) {
		struct loop_arg a = {m == HOT_ONLY ? &hot : &cold, m == HOT_ONLY ? &hr : &cr, &stop, hot_cpu, 100};
		pthread_t t;
		pthread_create(&t, NULL, commit_loop, &a);
		sleep_ns(end - now_ns());
		stop = 1;
		pthread_join(t, NULL);
		rounds = hr.n + cr.n;
	} else {
		// One commit loop with a hot request always waiting and a cold backlog. SERIAL alternates a
		// hot round and a cold round, each with its own flush, as rounds run today. ONE_ROUND writes
		// both streams' records and then flushes once. A hot request arrives as the previous one is
		// acknowledged, so its latency includes the cold work the loop does before it.
		uint64_t c0 = thread_cpu_ns(), arrive = now_ns();
		while (now_ns() < end) {
			uint64_t tc = now_ns();
			stream_write(&cold);
			uint64_t tc1 = now_ns();
			if (m == SERIAL) {
				stream_flush(&cold);
				record(&cr, now_ns() - tc, tc1 - tc, now_ns() - tc1, cold_bs);
				rounds++;
			}
			uint64_t th = now_ns();
			stream_write(&hot);
			uint64_t th1 = now_ns();
			stream_flush(&hot);
			uint64_t th2 = now_ns();
			if (m == ONE_ROUND)
				record(&cr, th2 - tc, tc1 - tc, th2 - th1, cold_bs);
			record(&hr, th2 - arrive, th1 - th, th2 - th1, HOT_BS);
			rounds++;
			arrive = th2;
		}
		hr.cpu_ns = thread_cpu_ns() - c0;
	}
	struct dstat d1 = dstat_read();
	struct outcome o = {0};
	o.secs = (d1.t - t0) / 1e9;
	o.hot_n = hr.n;
	o.cold_n = cr.n;
	size_t hn = hr.n < MAX_LAT ? hr.n : MAX_LAT, cn = cr.n < MAX_LAT ? cr.n : MAX_LAT;
	o.hot = percentiles(hr.lat, hn);
	o.hot_wr = percentiles(hr.wr, hn);
	o.hot_fl = percentiles(hr.fl, hn);
	o.cold = percentiles(cr.lat, cn);
	o.hot_rate = hr.n / o.secs;
	o.cold_mbs = cr.bytes / o.secs / 1e6;
	o.flushes_per_round =
		rounds ? ((double)(d1.flushes - d0.flushes) - g_bg_flush_rate * o.secs) / (double)rounds : 0;
	o.hot_cpu_us = hr.n ? hr.cpu_ns / 1e3 / hr.n : 0;
	o.cold_cpu_us = cr.n ? cr.cpu_ns / 1e3 / cr.n : 0;
	result_free(&hr);
	result_free(&cr);
	stream_close(&hot);
	stream_close(&cold);
	return o;
}

static void header(void)
{
	printf("    %-28s %7s %9s %9s %9s %9s %9s %9s %9s %8s %7s\n", "case", "cold", "hot/s", "hot_p50", "hot_p99",
	       "hot_wr50", "hot_fl50", "cold_p50", "cold_MB/s", "fl/round", "cpu/hot");
}

static void row(const char *name, size_t cold_bs, struct outcome o)
{
	char cb[24];
	if (cold_bs)
		snprintf(cb, sizeof cb, "%zuK", cold_bs >> 10);
	else
		snprintf(cb, sizeof cb, "-");
	printf("    %-28s %7s %9.0f %9.1f %9.1f %9.1f %9.1f %9.1f %9.1f %8.2f %7.1f\n", name, cb, o.hot_rate, o.hot.p50,
	       o.hot.p99, o.hot_wr.p50, o.hot_fl.p50, o.cold.p50, o.cold_mbs, o.flushes_per_round, o.hot_cpu_us);
	fflush(stdout);
}

static void section(const char *what)
{
	char la[128];
	loadavg(la, sizeof la);
	printf("\n### %s\nloadavg: %s\n", what, la);
	header();
}

static void suite(void)
{
	int secs = (int)arg("secs", 5);
	size_t sizes[] = {64u << 10, 256u << 10, 1u << 20};
	enum { NS = sizeof sizes / sizeof *sizes };
	struct outcome h1, h2[NS], h3[NS], h3b[NS], h4[NS], h4b, h5[NS];
	char got[96], want[96];
	int cpu = sched_getcpu();

	section("H1 hot stream alone: 4K record + fdatasync, QD1");
	h1 = run(HOT_ONLY, 0, secs, -1, -1, 100);
	row("H1 hot alone", 0, h1);

	section("H2 cold stream alone: one record of B bytes + fdatasync, back to back");
	for (int i = 0; i < NS; i++) {
		h2[i] = run(COLD_ONLY, sizes[i], secs, -1, -1, 100);
		row("H2 cold alone", sizes[i], h2[i]);
	}

	section("H3 one commit loop, rounds alternate: cold record + flush, then hot record + flush (today)");
	for (int i = 0; i < NS; i++) {
		h3[i] = run(SERIAL, sizes[i], secs, -1, -1, 100);
		row("H3 serial rounds", sizes[i], h3[i]);
	}

	section("H3b one commit loop, one round holds both streams' records: cold write, hot write, one flush");
	for (int i = 0; i < NS; i++) {
		h3b[i] = run(ONE_ROUND, sizes[i], secs, -1, -1, 100);
		row("H3b one round, one flush", sizes[i], h3b[i]);
	}

	section("H4 two concurrent commit loops (hot, cold), unpinned");
	for (int i = 0; i < NS; i++) {
		h4[i] = run(CONCURRENT, sizes[i], secs, -1, -1, 100);
		row("H4 concurrent", sizes[i], h4[i]);
	}

	section("H4b two concurrent commit loops, cold busy 50% of the time (a cleaner that keeps up), 1M");
	h4b = run(CONCURRENT, 1u << 20, secs, -1, -1, 50);
	row("H4b concurrent, cold 50%", 1u << 20, h4b);

	printf("\n    (H5 pins both loops to CPU %d, one blk-mq hardware queue, where the kernel folds flushes)\n",
	       cpu);
	section("H5 two concurrent commit loops pinned to one CPU");
	for (int i = 0; i < NS; i++) {
		h5[i] = run(CONCURRENT, sizes[i], secs, cpu, cpu, 100);
		row("H5 concurrent, one CPU", sizes[i], h5[i]);
	}

	printf("\n### checks (expected outcomes written before the run)\n");
	int b = NS - 1;
	snprintf(got, sizeof got, "%.0fus", h4[b].hot.p50);
	snprintf(want, sizeof want, "<=%.0fus (1.25x H1 %.0f)", 1.25 * h1.hot.p50, h1.hot.p50);
	check("H4", "hot p50 next to a 1M cold stream stays near hot alone", h4[b].hot.p50 <= 1.25 * h1.hot.p50, got,
	      want);
	snprintf(got, sizeof got, "%.0fMB/s", h4[b].cold_mbs);
	snprintf(want, sizeof want, ">=%.0fMB/s (0.8x H2 %.0f)", 0.8 * h2[b].cold_mbs, h2[b].cold_mbs);
	check("H4", "cold 1M throughput next to the hot stream", h4[b].cold_mbs >= 0.8 * h2[b].cold_mbs, got, want);
	snprintf(got, sizeof got, "%.0fus", h4[b].hot.p99);
	snprintf(want, sizeof want, "<%.0fus (H3 serial 1M)", h3[b].hot.p99);
	check("H4", "hot p99 concurrent beats serial rounds at 1M", h4[b].hot.p99 < h3[b].hot.p99, got, want);
	snprintf(got, sizeof got, "%.0fus", h3b[b].hot.p50);
	snprintf(want, sizeof want, "<%.0fus (H3 serial 1M)", h3[b].hot.p50);
	check("H3b", "one flush per round beats one per stream at 1M", h3b[b].hot.p50 < h3[b].hot.p50, got, want);
	snprintf(got, sizeof got, "%.0fus", h4b.hot.p50);
	snprintf(want, sizeof want, "<=%.0fus (1.25x H1)", 1.25 * h1.hot.p50);
	check("H4b", "hot p50 next to a cold stream at 50% duty", h4b.hot.p50 <= 1.25 * h1.hot.p50, got, want);
	snprintf(got, sizeof got, "%.2f", h5[b].flushes_per_round);
	check("H5", "flushes per commit with both loops on one CPU", h5[b].flushes_per_round < 0.9, got,
	      "<0.9 (folded, as in nvme-flush N7b)");
}

int main(int argc, char **argv)
{
	if (argc < 3) {
		fprintf(stderr, "usage: %s prep|bg|suite <file> [key=value ...]\n", argv[0]);
		return 2;
	}
	g_argc = argc;
	g_argv = argv;
	g_path = argv[2];
	if (getenv("DEV"))
		g_dev = getenv("DEV");
	if (!strcmp(argv[1], "prep")) {
		prep();
		return 0;
	}
	if (!strcmp(argv[1], "bg")) {
		measure_bg((int)arg("secs", 5));
		return 0;
	}
	struct stat st;
	if (stat(g_path, &st))
		die(g_path);
	g_size = st.st_size;
	if (!strcmp(argv[1], "suite")) {
		measure_bg(3);
		suite();
		return g_fail;
	}
	fprintf(stderr, "unknown test %s\n", argv[1]);
	return 2;
}
