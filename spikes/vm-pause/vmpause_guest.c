/*
 * Runs as init (PID 1) in a QEMU guest with the vmclock device, or as a plain
 * process on the host for "bench".
 *
 * As init it mounts /dev, /proc and /sys, loads ptp_vmclock.ko.zst with
 * finit_module(MODULE_INIT_COMPRESSED_FILE), maps /dev/vmclock0 read-only and
 * runs the mode named by vp.mode= on the kernel command line:
 *
 *   monitor  prints a sample line every 100 ms (forever): the four clocks, the
 *            CMOS RTC and every non-constant VMClock field, so the host driver
 *            (run.py) can compare them with its own clock across a QMP stop,
 *            snapshot or migration.
 *   bench    prints the cost of reading the disruption marker and the clocks.
 *
 * On the host, "vmpause-guest bench" runs the same benchmark with an anonymous
 * page standing in for the VMClock page: under KVM the page is ordinary guest
 * RAM, so the guest's load is a cached memory load like this one.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/rtc.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/reboot.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#ifndef MODULE_INIT_COMPRESSED_FILE
#define MODULE_INIT_COMPRESSED_FILE 4
#endif

/* struct vmclock_abi from include/uapi/linux/vmclock-abi.h at Linux v7.0. */
struct vmclock_abi {
	uint32_t magic;
	uint32_t size;
	uint16_t version;
	uint8_t counter_id;
	uint8_t time_type;
	uint32_t seq_count;
	uint64_t disruption_marker;
	uint64_t flags;
	uint8_t pad[2];
	uint8_t clock_status;
	uint8_t leap_second_smearing_hint;
	uint16_t tai_offset_sec;
	uint8_t leap_indicator;
	uint8_t counter_period_shift;
	uint64_t counter_value;
	uint64_t counter_period_frac_sec;
	uint64_t counter_period_esterror_rate_frac_sec;
	uint64_t counter_period_maxerror_rate_frac_sec;
	uint64_t time_sec;
	uint64_t time_frac_sec;
	uint64_t time_esterror_nanosec;
	uint64_t time_maxerror_nanosec;
	uint64_t vm_generation_counter;
};

#define VMCLOCK_MAGIC 0x4b4c4356
#define VMCLOCK_FLAG_VM_GEN_COUNTER_PRESENT (1 << 8)
#define VMCLOCK_FLAG_NOTIFICATION_PRESENT (1 << 9)

static const volatile struct vmclock_abi *clk;
static int clk_fd = -1;

static int64_t now_ns(clockid_t id)
{
	struct timespec ts;
	clock_gettime(id, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static int64_t sys_now_ns(clockid_t id)
{
	struct timespec ts;
	syscall(SYS_clock_gettime, id, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

/* The seqlock read the kernel driver does in vmclock_miscdev_read. */
static uint64_t read_marker(uint32_t *seq_out)
{
	uint32_t seq;
	uint64_t m;
	do {
		seq = clk->seq_count & ~1U;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		m = clk->disruption_marker;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
	} while (seq != clk->seq_count);
	if (seq_out)
		*seq_out = seq;
	return m;
}

static int64_t rtc_read_s(int fd)
{
	struct rtc_time t;
	struct tm tm;
	if (fd < 0 || ioctl(fd, RTC_RD_TIME, &t) < 0)
		return -1;
	memset(&tm, 0, sizeof(tm));
	tm.tm_sec = t.tm_sec;
	tm.tm_min = t.tm_min;
	tm.tm_hour = t.tm_hour;
	tm.tm_mday = t.tm_mday;
	tm.tm_mon = t.tm_mon;
	tm.tm_year = t.tm_year;
	return (int64_t)timegm(&tm);
}

static void cat_line(const char *label, const char *path)
{
	char buf[256] = "";
	FILE *f = fopen(path, "r");
	if (f) {
		if (!fgets(buf, sizeof(buf), f))
			buf[0] = 0;
		fclose(f);
	}
	buf[strcspn(buf, "\n")] = 0;
	printf("# %s: %s\n", label, buf);
}

static int load_module(const char *path)
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return -errno;
	int r = syscall(SYS_finit_module, fd, "", MODULE_INIT_COMPRESSED_FILE);
	int e = errno;
	close(fd);
	return r < 0 && e != EEXIST ? -e : 0;
}

static int map_vmclock(void)
{
	for (int i = 0; i < 50 && access("/dev/vmclock0", F_OK) != 0; i++)
		usleep(100000);
	clk_fd = open("/dev/vmclock0", O_RDONLY | O_CLOEXEC);
	if (clk_fd < 0)
		return -errno;
	void *p = mmap(NULL, 4096, PROT_READ, MAP_SHARED, clk_fd, 0);
	if (p == MAP_FAILED)
		return -errno;
	clk = p;
	return 0;
}

static void describe_vmclock(void)
{
	struct stat st;
	fstat(clk_fd, &st);
	printf("# vmclock0: mode=%o magic=0x%x size=%u version=%u counter_id=0x%x "
	       "time_type=%u flags=0x%llx clock_status=%u seq=%u marker=%llu gen=%llu\n",
	       st.st_mode & 07777, clk->magic, clk->size, clk->version, clk->counter_id,
	       clk->time_type, (unsigned long long)clk->flags, clk->clock_status,
	       clk->seq_count, (unsigned long long)clk->disruption_marker,
	       (unsigned long long)clk->vm_generation_counter);
	printf("%s T0 magic got=0x%x want=0x%x\n",
	       clk->magic == VMCLOCK_MAGIC ? "[OK ]" : "[FAIL]", clk->magic, VMCLOCK_MAGIC);
	struct pollfd pfd = {.fd = clk_fd, .events = POLLIN};
	int n = poll(&pfd, 1, 0);
	printf("# vmclock0 poll: n=%d revents=0x%x (POLLHUP=0x%x: no notifications)\n", n,
	       pfd.revents, POLLHUP);
	char buf[sizeof(struct vmclock_abi)];
	ssize_t r = pread(clk_fd, buf, sizeof(buf), 0);
	printf("# vmclock0 read(): %zd bytes, marker=%llu\n", r,
	       r >= 24 ? (unsigned long long)((struct vmclock_abi *)buf)->disruption_marker : 0ULL);
}

static void monitor(void)
{
	int rtc = open("/dev/rtc0", O_RDONLY | O_CLOEXEC);
	printf("READY monitor\n");
	fflush(stdout);
	struct timespec next;
	clock_gettime(CLOCK_MONOTONIC, &next);
	for (uint64_t n = 0;; n++) {
		uint32_t seq;
		uint64_t marker = read_marker(&seq);
		int64_t raw = now_ns(CLOCK_MONOTONIC_RAW);
		int64_t boot = now_ns(CLOCK_BOOTTIME);
		int64_t mono = now_ns(CLOCK_MONOTONIC);
		int64_t real = now_ns(CLOCK_REALTIME);
		int64_t rtc_s = rtc_read_s(rtc);
		printf("S n=%llu raw=%lld boot=%lld mono=%lld real=%lld rtc=%lld seq=%u marker=%llu "
		       "gen=%llu flags=0x%llx\n",
		       (unsigned long long)n, (long long)raw, (long long)boot, (long long)mono,
		       (long long)real, (long long)rtc_s, seq, (unsigned long long)marker,
		       (unsigned long long)clk->vm_generation_counter,
		       (unsigned long long)clk->flags);
		fflush(stdout);
		/* After a jump of the clock, sample from now rather than in a burst. */
		int64_t due = (int64_t)next.tv_sec * 1000000000 + next.tv_nsec + 100000000;
		int64_t now = now_ns(CLOCK_MONOTONIC);
		if (due < now)
			due = now + 100000000;
		next.tv_sec = due / 1000000000;
		next.tv_nsec = due % 1000000000;
		clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
	}
}

static int cmp64(const void *a, const void *b)
{
	int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
	return x < y ? -1 : x > y;
}

#define BATCHES 201

/* Prints the p50 and p99 over BATCHES batches of the cost of one call, in ns. */
#define BENCH(name, per_batch, expr)                                                     \
	do {                                                                             \
		int64_t s[BATCHES];                                                      \
		volatile uint64_t sink = 0;                                              \
		for (int b = 0; b < BATCHES; b++) {                                      \
			int64_t t0 = now_ns(CLOCK_MONOTONIC_RAW);                        \
			for (int i = 0; i < (per_batch); i++)                            \
				sink += (uint64_t)(expr);                                \
			s[b] = (now_ns(CLOCK_MONOTONIC_RAW) - t0) * 1000 / (per_batch);  \
		}                                                                        \
		qsort(s, BATCHES, sizeof(s[0]), cmp64);                                  \
		printf("C %-28s p50=%8.1f ns p99=%8.1f ns (%d x %d)\n", name,            \
		       s[BATCHES / 2] / 1000.0, s[BATCHES * 99 / 100] / 1000.0, BATCHES, \
		       (per_batch));                                                     \
		(void)sink;                                                              \
	} while (0)

static void bench(void)
{
	int rtc = open("/dev/rtc0", O_RDONLY | O_CLOEXEC);
	char buf[64];
	printf("READY bench\n");
	BENCH("marker (mmap seqlock)", 100000, read_marker(NULL));
	BENCH("vdso CLOCK_MONOTONIC_RAW", 10000, now_ns(CLOCK_MONOTONIC_RAW));
	BENCH("vdso CLOCK_BOOTTIME", 10000, now_ns(CLOCK_BOOTTIME));
	BENCH("vdso CLOCK_MONOTONIC", 10000, now_ns(CLOCK_MONOTONIC));
	BENCH("syscall CLOCK_MONOTONIC_RAW", 1000, sys_now_ns(CLOCK_MONOTONIC_RAW));
	BENCH("syscall CLOCK_BOOTTIME", 1000, sys_now_ns(CLOCK_BOOTTIME));
	BENCH("syscall getppid", 1000, syscall(SYS_getppid));
	if (getpid() == 1)
		BENCH("pread /dev/vmclock0 (24 B)", 1000, pread(clk_fd, buf, 24, 0));
	if (rtc >= 0)
		BENCH("ioctl RTC_RD_TIME", 1, rtc_read_s(rtc));
	printf("DONE bench\n");
	fflush(stdout);
}

int main(int argc, char **argv)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	if (getpid() != 1) {
		if (argc == 2 && strcmp(argv[1], "bench") == 0) {
			void *p = mmap(NULL, 4096, PROT_READ | PROT_WRITE,
				       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			((struct vmclock_abi *)p)->disruption_marker = 1;
			clk = p;
			cat_line("clocksource",
				 "/sys/devices/system/clocksource/clocksource0/current_clocksource");
			bench();
			return 0;
		}
		fprintf(stderr, "usage: as init in the guest, or '%s bench' on the host\n", argv[0]);
		return 2;
	}
	mkdir("/dev", 0755);
	mkdir("/proc", 0755);
	mkdir("/sys", 0755);
	mount("devtmpfs", "/dev", "devtmpfs", 0, NULL);
	mount("proc", "/proc", "proc", 0, NULL);
	mount("sysfs", "/sys", "sysfs", 0, NULL);
	int fd = open("/dev/console", O_RDWR);
	if (fd >= 0) {
		dup2(fd, 0);
		dup2(fd, 1);
		dup2(fd, 2);
	}
	cat_line("kernel", "/proc/version");
	cat_line("cmdline", "/proc/cmdline");
	cat_line("clocksource",
		 "/sys/devices/system/clocksource/clocksource0/current_clocksource");
	int r = load_module("/lib/modules/ptp_vmclock.ko.zst");
	printf("# finit_module ptp_vmclock: %s\n", r ? strerror(-r) : "ok");
	r = map_vmclock();
	if (r) {
		printf("[FAIL] T0 /dev/vmclock0 mmap got=%s want=ok\n", strerror(-r));
		printf("READY none\n");
		fflush(stdout);
		for (;;)
			pause();
	}
	printf("[OK ] T0 /dev/vmclock0 mmap got=ok want=ok\n");
	describe_vmclock();
	char cmd[512] = "";
	FILE *f = fopen("/proc/cmdline", "r");
	if (f) {
		if (!fgets(cmd, sizeof(cmd), f))
			cmd[0] = 0;
		fclose(f);
	}
	if (strstr(cmd, "vp.mode=bench")) {
		bench();
		sync();
		reboot(RB_POWER_OFF);
	}
	monitor();
	return 0;
}
