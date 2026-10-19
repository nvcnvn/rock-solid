/*
 * Samples CLOCK_MONOTONIC, CLOCK_BOOTTIME and CLOCK_REALTIME against
 * CLOCK_MONOTONIC_RAW every window for a number of seconds and prints their
 * rates in ppm, next to the rate the kernel's tick and frequency predict and the
 * PLL offset still to slew. Given a PTP clock (a KVM guest's ptp_kvm, which
 * reads the host's CLOCK_REALTIME), also prints CLOCK_REALTIME's offset from it.
 * Usage: clockrate [seconds=30] [window_ms=1000] [/dev/ptpN]
 */
#include <fcntl.h>
#include <math.h>
#include <stdlib.h>
#include "clk.h"

#define FD_TO_CLOCKID(fd) ((~(clockid_t)(fd) << 3) | 3)

static double phc_offset_ms(clockid_t phc)
{
	if (phc == CLOCK_REALTIME)
		return NAN;
	int64_t r1 = ns_of(CLOCK_REALTIME), p = ns_of(phc), r2 = ns_of(CLOCK_REALTIME);
	return (double)(r1 + (r2 - r1) / 2 - p) / 1e6;
}

int main(int argc, char **argv)
{
	int seconds = argc > 1 ? atoi(argv[1]) : 30;
	int window_ms = argc > 2 ? atoi(argv[2]) : 1000;
	int n = seconds * 1000 / window_ms;
	clockid_t phc = CLOCK_REALTIME;
	if (argc > 3) {
		int fd = open(argv[3], O_RDONLY);
		if (fd < 0) {
			perror(argv[3]);
			return 1;
		}
		phc = FD_TO_CLOCKID(fd);
	}
	double lo = 1e18, hi = -1e18, boot_mono_max = 0;
	int64_t span_max = 0;

	struct timex t;
	read_timex(&t);
	print_timex("start", &t, read_timex(&t));

	struct sample first = take(), prev = first;
	for (int i = 0; i < n; i++) {
		struct timespec d = { window_ms / 1000, (window_ms % 1000) * 1000000L };
		clock_nanosleep(CLOCK_MONOTONIC, 0, &d, NULL);
		struct sample s = take();
		struct rates r = between(prev, s);
		read_timex(&t);
		printf("w%03d raw_ms=%.3f mono=%+.3f boot=%+.3f real=%+.3f ppm predicted=%+.3f ppm "
		       "pll_left_ms=%+.3f real-phc_ms=%+.3f span_ns=%lld\n",
		       i, r.draw / 1e6, r.mono, r.boot, r.real, tick_freq_ppm(&t),
		       t.offset / ((t.status & STA_NANO) ? 1e6 : 1e3), phc_offset_ms(phc),
		       (long long)s.raw_span);
		if (r.mono < lo) lo = r.mono;
		if (r.mono > hi) hi = r.mono;
		if (r.boot - r.mono > boot_mono_max) boot_mono_max = r.boot - r.mono;
		if (s.raw_span > span_max) span_max = s.raw_span;
		prev = s;
	}
	struct rates all = between(first, prev);
	read_timex(&t);
	print_timex("end", &t, read_timex(&t));
	printf("summary windows=%d window_ms=%d mono_vs_raw: mean=%+.3f min=%+.3f max=%+.3f ppm; "
	       "boot-mono max=%.3f ppm; real mean=%+.3f ppm; max sample span=%lld ns\n",
	       n, window_ms, all.mono, lo, hi, boot_mono_max, all.real, (long long)span_max);
	return 0;
}
