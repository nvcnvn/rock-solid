/* Prints the kernel's NTP state with adjtimex(modes=0), which needs no privilege. */
#include "clk.h"

int main(void)
{
	struct timex t;
	int state = read_timex(&t);
	if (state < 0) {
		perror("adjtimex");
		return 1;
	}
	printf("USER_HZ=%ld nominal_tick=%ld\n", sysconf(_SC_CLK_TCK), 1000000 / sysconf(_SC_CLK_TCK));
	print_timex("adjtimex", &t, state);
	return 0;
}
