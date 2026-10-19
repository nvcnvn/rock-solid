/*
 * Runs as init inside a disposable User-Mode Linux kernel, where changing the
 * clock touches nothing else. Applies each kind of NTP adjustment the kernel
 * accepts and measures how fast CLOCK_MONOTONIC and CLOCK_BOOTTIME then run
 * against CLOCK_MONOTONIC_RAW. Refuses to run as anything but PID 1 unless
 * SLEW_GUEST_I_AM_A_VM=1 is set, so it never changes a real machine's clock.
 */
#include <stdlib.h>
#include <sys/reboot.h>
#include <sys/timerfd.h>
#include <sys/utsname.h>
#include "clk.h"

#ifndef ADJ_SETOFFSET
#define ADJ_SETOFFSET 0x0100
#endif

static int fails;

static void check(const char *id, const char *what, double got, double want, double tol, const char *unit)
{
	int ok = got >= want - tol && got <= want + tol;
	if (!ok)
		fails++;
	printf("[%s] %s %s got=%+.1f want=%+.1f tol=%.1f %s\n", ok ? "OK " : "FAIL", id, what, got, want, tol, unit);
}

static int adj(struct timex *t)
{
	int r = adjtimex(t);
	if (r < 0)
		printf("adjtimex modes=0x%x: %s\n", t->modes, strerror(errno));
	return r;
}

/* Back to tick 10000, freq 0, PLL off, no outstanding offsets (as chronyd's initialise_timex). */
static void reset(void)
{
	struct timex t = { 0 };
	t.modes = ADJ_OFFSET | ADJ_STATUS | ADJ_NANO;
	t.status = STA_PLL;
	adj(&t);
	memset(&t, 0, sizeof(t));
	t.modes = ADJ_STATUS | ADJ_TICK | ADJ_FREQUENCY;
	t.status = STA_UNSYNC;
	t.tick = 1000000 / sysconf(_SC_CLK_TCK);
	adj(&t);
	memset(&t, 0, sizeof(t));
	t.modes = ADJ_OFFSET_SINGLESHOT;
	adj(&t);
	/* A PLL chunk already in tick_length lasts until the next second_overflow. */
	struct timespec d = { 1, 200000000 };
	nanosleep(&d, NULL);
}

/* Sleeps until 5 ms after the next CLOCK_REALTIME second, where second_overflow() runs. */
static void align(void)
{
	struct timespec now;
	clock_gettime(CLOCK_REALTIME, &now);
	struct timespec next = { now.tv_sec + 1, 5000000 };
	while (clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &next, NULL) == EINTR)
		;
}

/* Measures n windows of window_ms, aligned to realtime seconds when window_ms is 1000. */
static void measure(const char *id, int n, int window_ms, struct rates *out)
{
	if (window_ms == 1000)
		align();
	struct sample prev = take(), first = prev;
	for (int i = 0; i < n; i++) {
		if (window_ms == 1000) {
			align();
		} else {
			struct timespec d = { 0, window_ms * 1000000L };
			clock_nanosleep(CLOCK_MONOTONIC, 0, &d, NULL);
		}
		struct sample s = take();
		out[i] = between(prev, s);
		struct timex t;
		read_timex(&t);
		printf("  %s w%d raw_ms=%.3f mono=%+.1f boot=%+.1f real=%+.1f ppm (tick+freq %+.1f, pll offset %ld, adjtime left %ld us)\n",
		       id, i, out[i].draw / 1e6, out[i].mono, out[i].boot, out[i].real, tick_freq_ppm(&t), t.offset,
		       read_singleshot());
		prev = s;
	}
	struct rates all = between(first, prev);
	printf("  %s all raw_ms=%.3f mono=%+.1f boot=%+.1f ppm\n", id, all.draw / 1e6, all.mono, all.boot);
}

/* NTP state that second_overflow() changes once a second while an offset is slewed. */
static long ntp_phase(void)
{
	struct timex t;
	read_timex(&t);
	return t.offset ^ (read_singleshot() << 20);
}

static void wait_overflow(void)
{
	long before = ntp_phase();
	struct timespec d = { 0, 1000000 };
	while (ntp_phase() == before)
		nanosleep(&d, NULL);
}

/*
 * Measures n windows, each from one second_overflow() to the next, seen as a
 * change in the outstanding PLL or adjtime() offset. Window k runs with the
 * (k+1)-th chunk of the offset applied after the adjustment.
 */
static void measure_overflows(const char *id, int n, struct rates *out)
{
	wait_overflow();
	struct sample prev = take();
	for (int i = 0; i < n; i++) {
		wait_overflow();
		struct sample s = take();
		out[i] = between(prev, s);
		struct timex t;
		read_timex(&t);
		printf("  %s s%d raw_ms=%.3f mono=%+.1f boot=%+.1f real=%+.1f ppm (tick+freq %+.1f, pll offset %ld, adjtime left %ld us)\n",
		       id, i, out[i].draw / 1e6, out[i].mono, out[i].boot, out[i].real, tick_freq_ppm(&t), t.offset,
		       read_singleshot());
		prev = s;
	}
}

static double tol_for(double want)
{
	double t = want < 0 ? -want * 0.01 : want * 0.01;
	return t < 50 ? 50 : t;
}

static void set_tick_freq(long tick, double freq_ppm)
{
	struct timex t = { 0 };
	t.modes = ADJ_TICK | ADJ_FREQUENCY;
	t.tick = tick;
	t.freq = (long)(freq_ppm * 65536.0);
	adj(&t);
}

static void singleshot(long usec)
{
	struct timex t = { 0 };
	t.modes = ADJ_OFFSET_SINGLESHOT;
	t.offset = usec;
	adj(&t);
}

static void pll(long nsec, long constant)
{
	struct timex t = { 0 };
	t.modes = ADJ_STATUS | ADJ_NANO | ADJ_OFFSET | ADJ_TIMECONST;
	t.status = STA_PLL;
	t.offset = nsec;
	t.constant = constant;
	adj(&t);
}

static void steady(const char *id, const char *what, double want, int overflows)
{
	struct rates r[3];
	if (overflows)
		measure_overflows(id, 3, r);
	else
		measure(id, 3, 1000, r);
	for (int i = 0; i < 3; i++) {
		check(id, what, r[i].mono, want, tol_for(want), "ppm mono");
		check(id, "boot follows mono", r[i].boot - r[i].mono, 0, 20, "ppm boot-mono");
	}
}

/* Expected per-second slew of the kernel PLL: offset >> (SHIFT_PLL + constant), SHIFT_PLL = 2. */
static void pll_windows(const char *id, double offset_s, int constant, int n)
{
	struct rates r[8];
	double left = offset_s;
	pll((long)(offset_s * 1e9), constant);
	measure_overflows(id, n, r);
	for (int i = 0; i < n; i++) {
		double chunk = left / (double)(1 << (2 + constant));
		left -= chunk;
		check(id, "PLL second", r[i].mono, chunk * 1e6, tol_for(chunk * 1e6), "ppm mono");
	}
}

int main(void)
{
	if (getpid() != 1 && !getenv("SLEW_GUEST_I_AM_A_VM")) {
		fprintf(stderr, "refusing to change this machine's clock: run as init in a VM\n");
		return 2;
	}
	setvbuf(stdout, NULL, _IOLBF, 0);
	struct utsname u;
	uname(&u);
	printf("slew-guest on %s %s %s, USER_HZ=%ld\n", u.sysname, u.release, u.machine, sysconf(_SC_CLK_TCK));

	struct timex t;
	struct rates r[30];

	reset();
	read_timex(&t);
	print_timex("G0 initial", &t, 0);
	steady("G0", "no adjustment", 0, 0);

	reset();
	set_tick_freq(9000, 0);
	steady("G1", "tick=9000 (-10%)", -100000, 0);

	reset();
	set_tick_freq(11000, 0);
	steady("G2", "tick=11000 (+10%)", 100000, 0);

	reset();
	{
		struct timex a = { .modes = ADJ_TICK, .tick = 8999 };
		int r1 = adjtimex(&a), e1 = errno;
		struct timex b = { .modes = ADJ_TICK, .tick = 11001 };
		int r2 = adjtimex(&b), e2 = errno;
		check("G3", "tick=8999 rejected (EINVAL=22)", r1 < 0 ? e1 : 0, EINVAL, 0, "errno");
		check("G3", "tick=11001 rejected (EINVAL=22)", r2 < 0 ? e2 : 0, EINVAL, 0, "errno");
	}

	reset();
	set_tick_freq(10000, 1000);
	read_timex(&t);
	check("G4", "freq=+1000 ppm clamped to MAXFREQ", t.freq / 65536.0, 500, 0.01, "ppm read back");
	steady("G4", "freq clamped", 500, 0);

	reset();
	set_tick_freq(9000, -500);
	steady("G5", "tick=9000 freq=-500", -100500, 0);

	reset();
	singleshot(-500000);
	steady("G6", "adjtime(-0.5 s)", -500, 1);
	reset();
	singleshot(500000);
	steady("G6", "adjtime(+0.5 s)", 500, 1);

	reset();
	pll_windows("G7", 0.5, 0, 4);
	reset();
	pll_windows("G7", -0.5, 0, 4);

	reset();
	pll_windows("G8", -0.399, 1, 4);

	reset();
	set_tick_freq(10000 - 833, -33.333);
	steady("G9", "chrony maxslewrate 83333 ppm, slow", -83333, 0);
	reset();
	set_tick_freq(10000 + 833, 33.333);
	steady("G9", "chrony maxslewrate 83333 ppm, fast", 83333, 0);

	for (int sign = -1; sign <= 1; sign += 2) {
		reset();
		set_tick_freq(10000 + sign * 1000, sign * 500);
		singleshot(sign * 500000);
		pll(sign * 500000000L, 0);
		measure_overflows("G10", 2, r);
		double want = sign * (100000 + 500 + 500 + 125000);
		check("G10", sign < 0 ? "every adjustment at once, slow, first second" : "every adjustment at once, fast, first second",
		      r[0].mono, want, tol_for(want) * 2, "ppm mono");
	}

	reset();
	set_tick_freq(9000, 0);
	{
		clockid_t ids[2] = { CLOCK_BOOTTIME, CLOCK_MONOTONIC };
		const char *names[2] = { "timerfd CLOCK_BOOTTIME 1 s at tick=9000", "timerfd CLOCK_MONOTONIC 1 s at tick=9000" };
		for (int i = 0; i < 2; i++) {
			int fd = timerfd_create(ids[i], 0);
			struct itimerspec its = { .it_value = { 1, 0 } };
			int64_t r0 = ns_of(CLOCK_MONOTONIC_RAW);
			timerfd_settime(fd, 0, &its, NULL);
			uint64_t exp;
			if (read(fd, &exp, sizeof(exp)) < 0)
				perror("read timerfd");
			int64_t r1 = ns_of(CLOCK_MONOTONIC_RAW);
			check("G11", names[i], (r1 - r0) / 1e3, 1e6 / 0.9, 2000, "us raw");
			close(fd);
		}
		int fd = timerfd_create(CLOCK_MONOTONIC_RAW, 0);
		check("G11", "timerfd_create(CLOCK_MONOTONIC_RAW) rejected (EINVAL=22)", fd < 0 ? errno : 0, EINVAL, 0, "errno");
	}

	reset();
	{
		struct sample a = take();
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		now.tv_sec += 1;
		clock_settime(CLOCK_REALTIME, &now);
		struct timex s = { 0 };
		s.modes = ADJ_SETOFFSET | ADJ_NANO;
		s.time.tv_sec = 2;
		adj(&s);
		struct timespec d = { 0, 500000000 };
		nanosleep(&d, NULL);
		struct sample b = take();
		struct rates x = between(a, b);
		check("G12", "steps of +1 s and +2 s to CLOCK_REALTIME leave mono's rate", x.mono, 0, 50, "ppm mono");
		check("G12", "CLOCK_REALTIME jumped", (b.real - a.real - (b.raw - a.raw)) / 1e9, 3, 0.01, "s");
	}

	reset();
	measure("G13", 10, 100, r);
	{
		double worst = 0;
		for (int i = 0; i < 10; i++)
			if (r[i].boot > worst || -r[i].boot > worst)
				worst = r[i].boot < 0 ? -r[i].boot : r[i].boot;
		check("G13", "100 ms BOOTTIME/RAW windows, no adjustment: worst", worst, 0, 100, "ppm");
	}
	set_tick_freq(10000 - 833, -33.333);
	{
		struct timespec d = { 0, 50000000 };
		nanosleep(&d, NULL);
	}
	measure("G13", 10, 100, r);
	for (int i = 0; i < 10; i++)
		check("G13", "100 ms BOOTTIME/RAW window during a chrony slew", r[i].boot, -83333, 840, "ppm");

	reset();
	printf("slew-guest done, %d failures\n", fails);
	sync();
	reboot(RB_POWER_OFF);
	return fails != 0;
}
