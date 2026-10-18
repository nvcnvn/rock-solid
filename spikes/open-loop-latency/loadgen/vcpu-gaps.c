// Spins on the clock for `secs` and counts the gaps in which this thread did not run: on an
// otherwise idle machine, the hypervisor preempting the vCPU.
//
// usage: vcpu-gaps SECS
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int64_t now_ns(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

int main(int argc, char **argv) {
	double secs = argc > 1 ? atof(argv[1]) : 10;
	int64_t last = now_ns(), end = last + (int64_t)(secs * 1e9), lost = 0, max = 0;
	long b[5] = {0};
	for (;;) {
		int64_t n = now_ns(), g = n - last;
		last = n;
		if (n > end)
			break;
		if (g < 20000)
			continue;
		lost += g;
		max = g > max ? g : max;
		b[g < 100000 ? 0 : g < 500000 ? 1 : g < 1000000 ? 2 : g < 5000000 ? 3 : 4]++;
	}
	printf("GAPS secs=%.0f | g20_100us=%ld g100_500us=%ld g500us_1ms=%ld g1_5ms=%ld g5ms_up=%ld lost_ms=%.1f "
	       "max_us=%.0f\n",
	       secs, b[0], b[1], b[2], b[3], b[4], lost / 1e6, max / 1e3);
	return 0;
}
