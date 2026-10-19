/*
 * Samples CLOCK_MONOTONIC, CLOCK_BOOTTIME and CLOCK_REALTIME against
 * CLOCK_MONOTONIC_RAW every window for a number of seconds and prints their
 * rates in ppm, next to the rate the kernel's tick and frequency predict.
 * Usage: clockrate [seconds=30] [window_ms=1000]
 */
#include <stdlib.h>
#include "clk.h"

int main(int argc, char **argv)
{
	int seconds = argc > 1 ? atoi(argv[1]) : 30;
	int window_ms = argc > 2 ? atoi(argv[2]) : 1000;
	int n = seconds * 1000 / window_ms;
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
		printf("w%03d raw_ms=%.3f mono=%+.3f boot=%+.3f real=%+.3f ppm predicted=%+.3f ppm span_ns=%lld\n",
		       i, r.draw / 1e6, r.mono, r.boot, r.real, tick_freq_ppm(&t), (long long)s.raw_span);
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
