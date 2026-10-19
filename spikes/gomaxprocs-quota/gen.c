// gen: open-loop load generator that stays outside the CPU cgroup.
//
// It creates one socketpair per server loop, forks, moves the child into the
// cgroup, execs the Go server with the sockets at fds 3.., and then sends
// 16-byte messages {send time CLOCK_MONOTONIC ns, seq} round-robin over the
// sockets at a fixed total rate. A full socket counts as a drop, never a wait,
// so the offered load does not depend on the server.
//
// usage: gen <cgroup dir or -> <ops/s> <seconds> <loops> <server> [args...]
// GEN_CGROUP names a cgroup for gen itself, so other load on the machine does not delay it.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static uint64_t now_ns(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

int main(int argc, char **argv) {
	if (argc < 6) {
		fprintf(stderr, "usage: gen cgroup ops/s secs loops server [args]\n");
		return 2;
	}
	const char *cg = argv[1];
	double rate = atof(argv[2]);
	double secs = atof(argv[3]);
	int loops = atoi(argv[4]);
	int ours[64];
	int theirs[64];
	for (int i = 0; i < loops; i++) {
		int sv[2];
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) { perror("socketpair"); return 1; }
		ours[i] = sv[0];
		theirs[i] = sv[1];
		int sz = 4 << 20;
		setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);
		setsockopt(sv[1], SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
		fcntl(sv[0], F_SETFL, O_NONBLOCK);
	}
	int ready[2];
	if (pipe(ready) < 0) { perror("pipe"); return 1; }
	pid_t pid = fork();
	if (pid == 0) {
		if (strcmp(cg, "-") != 0) {
			char path[512];
			snprintf(path, sizeof path, "%s/cgroup.procs", cg);
			FILE *f = fopen(path, "w");
			if (!f || fprintf(f, "%d\n", getpid()) < 0 || fclose(f) != 0) { perror(path); _exit(1); }
		}
		for (int i = 0; i < loops; i++) {
			close(ours[i]);
			if (dup2(theirs[i], 3 + i) < 0) { perror("dup2"); _exit(1); }
		}
		close(ready[0]);
		if (ready[1] != 3 + loops) { dup2(ready[1], 3 + loops); close(ready[1]); }
		execv(argv[5], argv + 5);
		perror("execv");
		_exit(1);
	}
	for (int i = 0; i < loops; i++) close(theirs[i]);
	close(ready[1]);
	char b;
	if (read(ready[0], &b, 1) != 1) { fprintf(stderr, "server did not start\n"); return 1; }

	const char *own = getenv("GEN_CGROUP");
	if (own) {
		char path[512];
		snprintf(path, sizeof path, "%s/cgroup.procs", own);
		FILE *f = fopen(path, "w");
		if (!f || fprintf(f, "%d\n", getpid()) < 0 || fclose(f) != 0) { perror(path); return 1; }
	}
	prctl(PR_SET_TIMERSLACK, 1);
	uint64_t start = now_ns();
	uint64_t end = start + (uint64_t)(secs * 1e9);
	uint64_t sent = 0, drops = 0, seq = 0;
	struct timespec nap = {0, 20000};
	for (;;) {
		uint64_t t = now_ns();
		if (t >= end) break;
		uint64_t due = (uint64_t)((double)(t - start) * rate / 1e9);
		while (seq < due) {
			uint64_t msg[2] = {now_ns(), seq};
			int s = ours[seq % loops];
			ssize_t n = send(s, msg, sizeof msg, MSG_DONTWAIT | MSG_NOSIGNAL);
			if (n == sizeof msg) sent++;
			else if (n < 0 && errno == EAGAIN) drops++;
			else { fprintf(stderr, "send: %zd %s\n", n, strerror(errno)); return 1; }
			seq++;
		}
		nanosleep(&nap, NULL);
	}
	for (int i = 0; i < loops; i++) {
		uint64_t stop[2] = {0, 0};
		int fl = fcntl(ours[i], F_GETFL);
		fcntl(ours[i], F_SETFL, fl & ~O_NONBLOCK);
		if (send(ours[i], stop, sizeof stop, MSG_NOSIGNAL) != sizeof stop) perror("stop");
	}
	int st;
	waitpid(pid, &st, 0);
	printf("GEN rate=%.0f secs=%.1f sent=%llu drops=%llu exit=%d\n", rate, secs,
	       (unsigned long long)sent, (unsigned long long)drops, WIFEXITED(st) ? WEXITSTATUS(st) : -1);
	return 0;
}
