/* Shared helpers: read the clocks together and the kernel's NTP state. */
#define _GNU_SOURCE
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/timex.h>
#include <time.h>
#include <unistd.h>

static inline int64_t ns_of(clockid_t c)
{
	struct timespec ts;
	clock_gettime(c, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

/* One sample: RAW read before and after the others, so the midpoint brackets them. */
struct sample {
	int64_t raw, mono, boot, real, raw_span;
};

static inline struct sample take1(void)
{
	struct sample s;
	int64_t r1 = ns_of(CLOCK_MONOTONIC_RAW);
	s.mono = ns_of(CLOCK_MONOTONIC);
	s.boot = ns_of(CLOCK_BOOTTIME);
	s.real = ns_of(CLOCK_REALTIME);
	int64_t r2 = ns_of(CLOCK_MONOTONIC_RAW);
	s.raw = r1 + (r2 - r1) / 2;
	s.raw_span = r2 - r1;
	return s;
}

/* The tightest of several brackets, since a preemption between reads skews one. */
static inline struct sample take(void)
{
	struct sample best = take1();
	for (int i = 0; i < 6; i++) {
		struct sample s = take1();
		if (s.raw_span < best.raw_span)
			best = s;
	}
	return best;
}

/* Rate of a clock relative to CLOCK_MONOTONIC_RAW between two samples, in ppm. */
static inline double ppm(int64_t d, int64_t draw)
{
	return ((double)d / (double)draw - 1.0) * 1e6;
}

struct rates {
	double mono, boot, real;
	int64_t draw;
};

static inline struct rates between(struct sample a, struct sample b)
{
	struct rates r;
	r.draw = b.raw - a.raw;
	r.mono = ppm(b.mono - a.mono, r.draw);
	r.boot = ppm(b.boot - a.boot, r.draw);
	r.real = ppm(b.real - a.real, r.draw);
	return r;
}

/*
 * The rate the kernel's tick and frequency set for CLOCK_MONOTONIC against
 * CLOCK_MONOTONIC_RAW, in ppm: kernel/time/ntp.c ntp_update_frequency(),
 * second_length = tick * 1000 * USER_HZ + time_freq. The PLL offset and the
 * adjtime() offset add to this while they are being slewed.
 */
static inline double tick_freq_ppm(const struct timex *t)
{
	double hz = (double)sysconf(_SC_CLK_TCK);
	double nominal = 1e6 / hz;
	return ((double)t->tick - nominal) / nominal * 1e6 + (double)t->freq / 65536.0;
}

static inline int read_timex(struct timex *t)
{
	memset(t, 0, sizeof(*t));
	return adjtimex(t);
}

/* Outstanding adjtime() offset in µs, read without changing it. */
static inline long read_singleshot(void)
{
	struct timex t;
	memset(&t, 0, sizeof(t));
	t.modes = ADJ_OFFSET_SS_READ;
	if (adjtimex(&t) < 0)
		return 0;
	return t.offset;
}

static inline void print_timex(const char *tag, const struct timex *t, int state)
{
	printf("%s state=%d status=0x%04x%s%s%s%s tick=%ld freq=%ld (%.3f ppm) offset=%ld %s"
	       " constant=%ld maxerror=%ld esterror=%ld tolerance=%ld (%.0f ppm) "
	       "precision=%ld tick+freq rate=%.3f ppm adjtime_left=%ld us\n",
	       tag, state, t->status,
	       (t->status & STA_PLL) ? " PLL" : "",
	       (t->status & STA_UNSYNC) ? " UNSYNC" : "",
	       (t->status & STA_NANO) ? " NANO" : "",
	       (t->status & STA_FREQHOLD) ? " FREQHOLD" : "",
	       t->tick, t->freq, t->freq / 65536.0, t->offset,
	       (t->status & STA_NANO) ? "ns" : "us",
	       t->constant, t->maxerror, t->esterror, t->tolerance, t->tolerance / 65536.0,
	       t->precision, tick_freq_ppm(t), read_singleshot());
}
