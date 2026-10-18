// Open-loop load generator for the replicated 4K write servers (server-rust, server-go).
//
// Arrival times are fixed before the run (Poisson or uniform at `rate`) and requests go out
// round-robin over `conns` pipelined connections whenever they are due, whatever the responses.
// Latency runs from the scheduled time, not the send time, so a late send counts against the
// request (no coordinated omission). Requests due while `cap` are in flight are dropped and
// counted. Only requests scheduled in [warm, warm+secs) are measured; server and client CPU are
// read over the same window, the server's from /proc/<pid>/stat.
//
// usage: loadgen port=P pid=PID rate=R [secs=10] [warm=1] [conns=16] [cap=16384]
//                [arrivals=poisson|uniform] [seed=1] [label=x] [dump=path]
// dump writes "scheduled_ns latency_ns" per request (-1 unanswered, -2 dropped).
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define HDR 24
#define PAYLOAD 4096
#define FRAME (HDR + PAYLOAD)
#define KIND_WRITE 1
#define KIND_ACK 3
#define RBUF (256 << 10)

struct conn {
	int fd;
	char *wbuf;
	size_t wlen, wpos, wcap;
	char rbuf[RBUF];
	size_t rlen;
	int armed;
};

static int64_t now_ns(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static void die(const char *what) {
	perror(what);
	exit(1);
}

static uint32_t crc32c(const uint8_t *p, size_t n) {
	uint32_t c = ~0u;
	for (size_t i = 0; i < n; i++) {
		c ^= p[i];
		for (int k = 0; k < 8; k++)
			c = (c >> 1) ^ (0x82F63B78u & -(c & 1));
	}
	return ~c;
}

static double proc_cpu_s(int pid) {
	char path[64], buf[4096];
	snprintf(path, sizeof path, "/proc/%d/stat", pid);
	FILE *f = fopen(path, "r");
	if (!f)
		die("open /proc/pid/stat");
	size_t n = fread(buf, 1, sizeof buf - 1, f);
	fclose(f);
	buf[n] = 0;
	// Fields after the parenthesised command name; utime and stime are fields 14 and 15.
	char *p = strrchr(buf, ')') + 2;
	unsigned long long ut = 0, st = 0;
	sscanf(p, "%*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %llu %llu", &ut, &st);
	return (double)(ut + st) / (double)sysconf(_SC_CLK_TCK);
}

// Busy, softirq and steal time of the whole machine, in seconds summed over CPUs.
static void machine_cpu_s(double *busy, double *softirq, double *steal) {
	FILE *f = fopen("/proc/stat", "r");
	if (!f)
		die("open /proc/stat");
	unsigned long long v[8] = {0};
	if (fscanf(f, "cpu %llu %llu %llu %llu %llu %llu %llu %llu", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5],
		   &v[6], &v[7]) != 8)
		die("parse /proc/stat");
	fclose(f);
	double tck = (double)sysconf(_SC_CLK_TCK);
	*busy = (v[0] + v[1] + v[2] + v[5] + v[6] + v[7]) / tck;
	*softirq = v[6] / tck;
	*steal = v[7] / tck;
}

static double self_cpu_s(void) {
	struct rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	return ru.ru_utime.tv_sec + ru.ru_stime.tv_sec + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1e6;
}

static int cmp64(const void *a, const void *b) {
	int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
	return (x > y) - (x < y);
}

static void arm(int ep, struct conn *c, int tok, int out) {
	if (c->armed == out)
		return;
	c->armed = out;
	struct epoll_event ev = {.events = EPOLLIN | (out ? EPOLLOUT : 0), .data.u32 = tok};
	if (epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &ev))
		die("epoll_ctl");
}

static void flush(int ep, struct conn *c, int tok) {
	while (c->wpos < c->wlen) {
		ssize_t n = write(c->fd, c->wbuf + c->wpos, c->wlen - c->wpos);
		if (n < 0 && errno == EAGAIN)
			break;
		if (n < 0)
			die("write");
		c->wpos += n;
	}
	if (c->wpos == c->wlen)
		c->wpos = c->wlen = 0;
	arm(ep, c, tok, c->wpos < c->wlen);
}

int main(int argc, char **argv) {
	int port = 0, pid = 0, conns = 16, poisson = 1;
	double rate = 0, secs = 10, warm = 1;
	long cap = 16384;
	unsigned seed = 1;
	const char *label = "?", *dump = NULL;
	for (int i = 1; i < argc; i++) {
		char *v = strchr(argv[i], '=');
		if (!v) {
			fprintf(stderr, "bad arg %s\n", argv[i]);
			return 2;
		}
		*v++ = 0;
		if (!strcmp(argv[i], "port")) port = atoi(v);
		else if (!strcmp(argv[i], "pid")) pid = atoi(v);
		else if (!strcmp(argv[i], "rate")) rate = atof(v);
		else if (!strcmp(argv[i], "secs")) secs = atof(v);
		else if (!strcmp(argv[i], "warm")) warm = atof(v);
		else if (!strcmp(argv[i], "conns")) conns = atoi(v);
		else if (!strcmp(argv[i], "cap")) cap = atol(v);
		else if (!strcmp(argv[i], "seed")) seed = atoi(v);
		else if (!strcmp(argv[i], "label")) label = v;
		else if (!strcmp(argv[i], "dump")) dump = v;
		else if (!strcmp(argv[i], "arrivals")) poisson = !strcmp(v, "poisson");
		else {
			fprintf(stderr, "unknown %s\n", argv[i]);
			return 2;
		}
	}
	if (!port || !pid || rate <= 0) {
		fprintf(stderr, "need port, pid and rate\n");
		return 2;
	}
	prctl(PR_SET_TIMERSLACK, 1);

	int64_t warm_ns = (int64_t)(warm * 1e9), end_ns = warm_ns + (int64_t)(secs * 1e9);
	size_t total = (size_t)(rate * (warm + secs) * 1.1) + 1024, n_req = 0;
	int64_t *sched = malloc(total * sizeof *sched), *lat = malloc(total * sizeof *lat);
	srand48(seed);
	for (double t = 0;;) {
		t += poisson ? -log(1.0 - drand48()) / rate : 1.0 / rate;
		if (t * 1e9 >= end_ns || n_req == total)
			break;
		sched[n_req] = (int64_t)(t * 1e9);
		lat[n_req++] = -1;
	}

	uint8_t data[PAYLOAD];
	for (int i = 0; i < PAYLOAD; i++)
		data[i] = (uint8_t)(i * 7);
	uint8_t hdr[HDR] = {KIND_WRITE};
	uint32_t len = PAYLOAD, crc = crc32c(data, PAYLOAD);
	memcpy(hdr + 4, &len, 4);
	memcpy(hdr + 16, &crc, 4);

	int ep = epoll_create1(0);
	struct conn *cs = calloc(conns, sizeof *cs);
	struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(0x7f000001)};
	for (int i = 0; i < conns; i++) {
		struct conn *c = &cs[i];
		c->fd = socket(AF_INET, SOCK_STREAM, 0);
		if (connect(c->fd, (struct sockaddr *)&sa, sizeof sa))
			die("connect");
		int one = 1;
		setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
		fcntl(c->fd, F_SETFL, fcntl(c->fd, F_GETFL) | O_NONBLOCK);
		c->wcap = 1 << 20;
		c->wbuf = malloc(c->wcap);
		struct epoll_event ev = {.events = EPOLLIN, .data.u32 = i};
		if (epoll_ctl(ep, EPOLL_CTL_ADD, c->fd, &ev))
			die("epoll_ctl");
	}
	struct timespec settle = {0, 100000000};
	nanosleep(&settle, NULL);

	size_t next = 0, measured = 0, dropped = 0, done_in_window = 0, completed = 0;
	long inflight = 0;
	int64_t *lag = malloc(total * sizeof *lag);
	size_t n_lag = 0;
	double srv0 = 0, srv1 = 0, cli0 = 0, cli1 = 0, busy0 = 0, busy1 = 0, si0 = 0, si1 = 0, st0 = 0, st1 = 0;
	int phase = 0;
	struct epoll_event evs[64];
	int64_t t0 = now_ns(), drain_until = 0;
	for (;;) {
		int64_t now = now_ns() - t0;
		if (phase == 0 && now >= warm_ns) {
			srv0 = proc_cpu_s(pid);
			cli0 = self_cpu_s();
			machine_cpu_s(&busy0, &si0, &st0);
			phase = 1;
		}
		if (phase == 1 && now >= end_ns) {
			srv1 = proc_cpu_s(pid);
			cli1 = self_cpu_s();
			machine_cpu_s(&busy1, &si1, &st1);
			phase = 2;
			drain_until = now + 5000000000LL;
		}
		while (next < n_req && sched[next] <= now) {
			int in_window = sched[next] >= warm_ns;
			measured += in_window;
			if (inflight >= cap) {
				dropped += in_window;
				lat[next++] = -2;
				continue;
			}
			struct conn *c = &cs[next % conns];
			if (c->wlen + FRAME > c->wcap) {
				c->wcap *= 2;
				c->wbuf = realloc(c->wbuf, c->wcap);
			}
			uint64_t id = next;
			memcpy(c->wbuf + c->wlen, hdr, HDR);
			memcpy(c->wbuf + c->wlen + 8, &id, 8);
			memcpy(c->wbuf + c->wlen + HDR, data, PAYLOAD);
			c->wlen += FRAME;
			if (in_window)
				lag[n_lag++] = now - sched[next];
			inflight++;
			next++;
		}
		for (int i = 0; i < conns; i++)
			if (cs[i].wlen > cs[i].wpos && !cs[i].armed)
				flush(ep, &cs[i], i);
		if (phase == 2 && (inflight == 0 || now >= drain_until))
			break;
		int64_t wait = phase == 2 ? 10000000 : (next < n_req ? sched[next] : end_ns) - now;
		if (phase == 0 && warm_ns - now < wait)
			wait = warm_ns - now;
		if (phase == 1 && end_ns - now < wait)
			wait = end_ns - now;
		struct timespec ts = {0, 0};
		if (wait > 0) {
			ts.tv_sec = wait / 1000000000;
			ts.tv_nsec = wait % 1000000000;
		}
		int n = epoll_pwait2(ep, evs, 64, &ts, NULL);
		if (n < 0 && errno != EINTR)
			die("epoll_pwait2");
		int64_t at = now_ns() - t0;
		for (int e = 0; e < n; e++) {
			int tok = evs[e].data.u32;
			struct conn *c = &cs[tok];
			if (evs[e].events & EPOLLOUT)
				flush(ep, c, tok);
			if (!(evs[e].events & EPOLLIN))
				continue;
			while (c->rlen < RBUF) {
				ssize_t r = read(c->fd, c->rbuf + c->rlen, RBUF - c->rlen);
				if (r < 0 && errno == EAGAIN)
					break;
				if (r <= 0)
					die("read");
				c->rlen += r;
			}
			size_t pos = 0;
			for (; c->rlen - pos >= HDR; pos += HDR) {
				uint64_t id;
				uint32_t flen;
				memcpy(&flen, c->rbuf + pos + 4, 4);
				memcpy(&id, c->rbuf + pos + 8, 8);
				if (c->rbuf[pos] != KIND_ACK || flen != 0 || id >= next || lat[id] != -1) {
					fprintf(stderr, "bad ack kind=%d len=%u id=%llu\n", c->rbuf[pos], flen,
						(unsigned long long)id);
					return 1;
				}
				lat[id] = at - sched[id];
				inflight--;
				completed++;
				done_in_window += at >= warm_ns && at < end_ns;
			}
			memmove(c->rbuf, c->rbuf + pos, c->rlen - pos);
			c->rlen -= pos;
		}
	}
	for (int i = 0; i < conns; i++)
		close(cs[i].fd);

	if (dump) {
		FILE *f = fopen(dump, "w");
		if (!f)
			die("open dump");
		for (size_t i = 0; i < next; i++)
			fprintf(f, "%lld %lld\n", (long long)sched[i], (long long)lat[i]);
		fclose(f);
	}
	int64_t *ok = malloc((measured + 1) * sizeof *ok);
	size_t n_ok = 0, unanswered = 0;
	for (size_t i = 0; i < next; i++) {
		if (sched[i] < warm_ns)
			continue;
		if (lat[i] >= 0)
			ok[n_ok++] = lat[i];
		else if (lat[i] == -1) {
			unanswered++;
			ok[n_ok++] = INT64_MAX;
		}
	}
	qsort(ok, n_ok, sizeof *ok, cmp64);
	qsort(lag, n_lag, sizeof *lag, cmp64);
#define Q(a, n, q) ((n) ? (a)[(size_t)((n) * (q)) < (n) ? (size_t)((n) * (q)) : (n) - 1] / 1000.0 : 0.0)
	double win = secs, achieved = done_in_window / win;
	printf("LOAD lang=%s conns=%d arrivals=%s cap=%ld | offered=%.0f achieved=%.0f measured=%zu dropped=%zu "
	       "unanswered=%zu p50_us=%.1f p90_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f lag_p99_us=%.1f "
	       "srv_cores=%.3f srv_us_per_op=%.2f cli_cores=%.3f machine_cores=%.3f softirq_cores=%.3f steal_cores=%.3f\n",
	       label, conns, poisson ? "poisson" : "uniform", cap, measured / win, achieved, measured, dropped,
	       unanswered, Q(ok, n_ok, 0.5), Q(ok, n_ok, 0.9), Q(ok, n_ok, 0.99), Q(ok, n_ok, 0.999),
	       n_ok ? ok[n_ok - 1] / 1000.0 : 0.0, Q(lag, n_lag, 0.99), (srv1 - srv0) / win,
	       (srv1 - srv0) * 1e6 / (done_in_window ? done_in_window : 1), (cli1 - cli0) / win, (busy1 - busy0) / win, (si1 - si0) / win, (st1 - st0) / win);
	return 0;
}
