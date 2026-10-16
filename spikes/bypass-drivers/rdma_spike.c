/* RDMA network-driver spike, run on Soft-RoCE (rdma_rxe). Two RC queue pairs on one device stand
 * in for two nodes; each "node" is its own pthread with its own completion queue.
 *
 * usage: rdma_spike            (set RS_IBVERBS=<lib> to override the library name)
 *   R0  dlopen libibverbs at runtime; no library or no device means "use TCP", not a crash
 *   R1  device facts: transport, RoCE v2 GID, fork safety, inline data limit
 *   R2  memory registration cost by size (decides per-I/O registration vs a registered pool)
 *   R3  RLIMIT_MEMLOCK: what a container's default limit does to registration
 *   R4  SEND/RECV round trips, busy-poll vs completion channel in epoll, vs kernel TCP loopback
 *   R5  buffers: unregistered memory, and inline sends that need no registration
 *   R6  failure surfacing: dead peer QP, peer with no receive posted
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <infiniband/verbs.h>

/* Only the non-inline entry points need symbols; post/poll/notify are inline calls through
 * context ops, so nothing links against libibverbs. */
static struct ibv_device **(*f_get_device_list)(int *);
static void (*f_free_device_list)(struct ibv_device **);
static const char *(*f_get_device_name)(struct ibv_device *);
static struct ibv_context *(*f_open_device)(struct ibv_device *);
static int (*f_query_device)(struct ibv_context *, struct ibv_device_attr *);
static int (*f_query_port)(struct ibv_context *, uint8_t, struct ibv_port_attr *);
static int (*f_query_gid)(struct ibv_context *, uint8_t, int, union ibv_gid *);
static struct ibv_pd *(*f_alloc_pd)(struct ibv_context *);
static struct ibv_mr *(*f_reg_mr)(struct ibv_pd *, void *, size_t, int);
static int (*f_dereg_mr)(struct ibv_mr *);
static struct ibv_comp_channel *(*f_create_comp_channel)(struct ibv_context *);
static struct ibv_cq *(*f_create_cq)(struct ibv_context *, int, void *, struct ibv_comp_channel *, int);
static int (*f_get_cq_event)(struct ibv_comp_channel *, struct ibv_cq **, void **);
static void (*f_ack_cq_events)(struct ibv_cq *, unsigned int);
static struct ibv_qp *(*f_create_qp)(struct ibv_pd *, struct ibv_qp_init_attr *);
static int (*f_destroy_qp)(struct ibv_qp *);
static int (*f_modify_qp)(struct ibv_qp *, struct ibv_qp_attr *, int);
static enum ibv_fork_status (*f_is_fork_initialized)(void);
static const char *(*f_wc_status_str)(enum ibv_wc_status);

static int load_verbs(void)
{
	const char *name = getenv("RS_IBVERBS") ? getenv("RS_IBVERBS") : "libibverbs.so.1";
	void *h = dlopen(name, RTLD_NOW);
	if (!h) {
		printf("  dlopen(%s) failed: %s\n  -> no RDMA on this node, transport = TCP\n", name, dlerror());
		return -1;
	}
#define SYM(f, s) if (!(*(void **)&f = dlsym(h, s))) { printf("  missing %s\n", s); return -1; }
	SYM(f_get_device_list, "ibv_get_device_list");
	SYM(f_free_device_list, "ibv_free_device_list");
	SYM(f_get_device_name, "ibv_get_device_name");
	SYM(f_open_device, "ibv_open_device");
	SYM(f_query_device, "ibv_query_device");
	SYM(f_query_port, "ibv_query_port");
	SYM(f_query_gid, "ibv_query_gid");
	SYM(f_alloc_pd, "ibv_alloc_pd");
	SYM(f_reg_mr, "ibv_reg_mr");
	SYM(f_dereg_mr, "ibv_dereg_mr");
	SYM(f_create_comp_channel, "ibv_create_comp_channel");
	SYM(f_create_cq, "ibv_create_cq");
	SYM(f_get_cq_event, "ibv_get_cq_event");
	SYM(f_ack_cq_events, "ibv_ack_cq_events");
	SYM(f_create_qp, "ibv_create_qp");
	SYM(f_destroy_qp, "ibv_destroy_qp");
	SYM(f_modify_qp, "ibv_modify_qp");
	SYM(f_is_fork_initialized, "ibv_is_fork_initialized");
	SYM(f_wc_status_str, "ibv_wc_status_str");
	printf("  dlopen(%s) ok\n", name);
	return 0;
}

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static uint64_t thread_cpu_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
	return ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

static struct ibv_context *g_ctx;
static struct ibv_pd *g_pd;
static int g_gid_idx = -1;
static union ibv_gid g_gid;
static enum ibv_mtu g_mtu;

#define MAXMSG (1 << 20)

struct node {
	const char *name;
	struct ibv_comp_channel *chan;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	struct ibv_mr *mr;
	char *buf; /* [0, MAXMSG) send, [MAXMSG, 2*MAXMSG) recv */
	int epfd;
	bool event_mode;
};

static void node_init(struct node *n, const char *name)
{
	memset(n, 0, sizeof(*n));
	n->name = name;
	n->chan = f_create_comp_channel(g_ctx);
	n->cq = f_create_cq(g_ctx, 256, NULL, n->chan, 0);
	n->buf = aligned_alloc(4096, 2 * MAXMSG);
	memset(n->buf, 0, 2 * MAXMSG);
	n->mr = f_reg_mr(g_pd, n->buf, 2 * MAXMSG, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
	n->epfd = epoll_create1(0);
	struct epoll_event ev = { .events = EPOLLIN };
	epoll_ctl(n->epfd, EPOLL_CTL_ADD, n->chan->fd, &ev);
}

static struct ibv_qp *qp_create(struct node *n)
{
	struct ibv_qp_init_attr a = {
		.send_cq = n->cq, .recv_cq = n->cq, .qp_type = IBV_QPT_RC,
		.cap = { .max_send_wr = 64, .max_recv_wr = 64, .max_send_sge = 1, .max_recv_sge = 1, .max_inline_data = 256 },
	};
	n->qp = f_create_qp(g_pd, &a);
	if (!n->qp) {
		perror("ibv_create_qp");
		exit(1);
	}
	return n->qp;
}

/* Out of band, the real driver would swap (QPN, GID) over the existing TCP connection. */
static int qp_connect(struct ibv_qp *qp, uint32_t remote_qpn, uint8_t timeout, uint8_t retry, uint8_t rnr_retry)
{
	struct ibv_qp_attr a = { .qp_state = IBV_QPS_INIT, .port_num = 1, .pkey_index = 0,
				 .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ };
	if (f_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
		return -1;
	}
	memset(&a, 0, sizeof(a));
	a.qp_state = IBV_QPS_RTR;
	a.path_mtu = g_mtu;
	a.dest_qp_num = remote_qpn;
	a.rq_psn = 0;
	a.max_dest_rd_atomic = 1;
	a.min_rnr_timer = 1; /* 0.01 ms */
	a.ah_attr.is_global = 1;
	a.ah_attr.grh.dgid = g_gid;
	a.ah_attr.grh.sgid_index = g_gid_idx;
	a.ah_attr.grh.hop_limit = 1;
	a.ah_attr.port_num = 1;
	if (f_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
			IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
		return -1;
	}
	memset(&a, 0, sizeof(a));
	a.qp_state = IBV_QPS_RTS;
	a.timeout = timeout;
	a.retry_cnt = retry;
	a.rnr_retry = rnr_retry;
	a.sq_psn = 0;
	a.max_rd_atomic = 1;
	return f_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
			   IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}

static void post_recv(struct node *n, uint32_t len)
{
	struct ibv_sge sge = { .addr = (uintptr_t)n->buf + MAXMSG, .length = len, .lkey = n->mr->lkey };
	struct ibv_recv_wr wr = { .sg_list = &sge, .num_sge = 1 }, *bad;
	if (ibv_post_recv(n->qp, &wr, &bad)) {
		abort();
	}
}

static int post_send(struct node *n, void *addr, uint32_t len, uint32_t lkey, int flags)
{
	struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
	struct ibv_send_wr wr = { .sg_list = &sge, .num_sge = 1, .opcode = IBV_WR_SEND,
				  .send_flags = IBV_SEND_SIGNALED | flags }, *bad;
	return ibv_post_send(n->qp, &wr, &bad);
}

/* Wait for one completion. Event mode is what a tokio driver would do: the channel fd sits in
 * epoll (AsyncFd), and we only sleep after re-arming and re-checking the CQ. */
static struct ibv_wc wait_wc(struct node *n)
{
	struct ibv_wc wc;
	for (;;) {
		if (ibv_poll_cq(n->cq, 1, &wc) > 0) {
			return wc;
		}
		if (!n->event_mode) {
			continue;
		}
		ibv_req_notify_cq(n->cq, 0);
		if (ibv_poll_cq(n->cq, 1, &wc) > 0) {
			return wc;
		}
		struct epoll_event ev;
		epoll_wait(n->epfd, &ev, 1, -1);
		struct ibv_cq *cq;
		void *ctx;
		f_get_cq_event(n->chan, &cq, &ctx);
		f_ack_cq_events(cq, 1);
	}
}

/* Poll for up to ms milliseconds; returns false on timeout. */
static bool wait_wc_for(struct node *n, struct ibv_wc *wc, int ms)
{
	uint64_t end = now_ns() + ms * 1000000ull;
	while (now_ns() < end) {
		if (ibv_poll_cq(n->cq, 1, wc) > 0) {
			return true;
		}
		usleep(50);
	}
	return false;
}

static void drain(struct node *n, const char *who)
{
	struct ibv_wc wc;
	while (wait_wc_for(n, &wc, 200)) {
		printf("      %s completion: opcode %d status %s (vendor_err %u)\n", who, wc.opcode,
		       f_wc_status_str(wc.status), wc.vendor_err);
	}
}

/* ---- R4 ping-pong ---- */

#define N_RT 20000
static struct node A, B;
static uint32_t g_len;
static uint64_t g_b_cpu;

static void *node_b(void *arg)
{
	uint64_t c0 = thread_cpu_ns();
	int pending_sends = 0;
	for (int i = 0; i < N_RT;) {
		struct ibv_wc wc = wait_wc(&B);
		if (wc.status != IBV_WC_SUCCESS) {
			printf("  B wc error %s\n", f_wc_status_str(wc.status));
			exit(1);
		}
		if (wc.opcode == IBV_WC_RECV) {
			post_recv(&B, MAXMSG);
			post_send(&B, B.buf, g_len, B.mr->lkey, 0);
			pending_sends++;
			i++;
		} else {
			pending_sends--;
		}
	}
	while (pending_sends-- > 0) {
		wait_wc(&B);
	}
	g_b_cpu = thread_cpu_ns() - c0;
	return NULL;
}

static void pingpong(uint32_t len, bool event_mode)
{
	static uint64_t lat[N_RT];
	g_len = len;
	A.event_mode = B.event_mode = event_mode;
	post_recv(&B, MAXMSG);
	pthread_t t;
	pthread_create(&t, NULL, node_b, NULL);
	uint64_t c0 = thread_cpu_ns(), t0 = now_ns();
	for (int i = 0; i < N_RT; i++) {
		uint64_t s = now_ns();
		post_recv(&A, MAXMSG);
		post_send(&A, A.buf, len, A.mr->lkey, 0);
		for (int got = 0; got < 2;) {
			struct ibv_wc wc = wait_wc(&A);
			if (wc.status != IBV_WC_SUCCESS) {
				printf("  A wc error %s\n", f_wc_status_str(wc.status));
				exit(1);
			}
			got++;
		}
		lat[i] = now_ns() - s;
	}
	uint64_t wall = now_ns() - t0, a_cpu = thread_cpu_ns() - c0;
	pthread_join(t, NULL);
	qsort(lat, N_RT, 8, cmp_u64);
	printf("  rxe  %-6s %7u B  RTT p50 %7.1f p99 %7.1f us | CPU/RT client %5.1f us server %5.1f us | %6.0f MB/s\n",
	       event_mode ? "epoll" : "poll", len, lat[N_RT / 2] / 1e3, lat[N_RT * 99 / 100] / 1e3,
	       a_cpu / 1e3 / N_RT, g_b_cpu / 1e3 / N_RT, 2.0 * len * N_RT / (wall / 1e3));
}

/* Kernel TCP loopback with the same message pattern, for scale. */
static int g_tcp_port;
static void *tcp_server(void *arg)
{
	int ls = socket(AF_INET, SOCK_STREAM, 0), one = 1;
	struct sockaddr_in sa = { .sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
	setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
	bind(ls, (void *)&sa, sizeof sa);
	socklen_t sl = sizeof sa;
	getsockname(ls, (void *)&sa, &sl);
	g_tcp_port = ntohs(sa.sin_port);
	listen(ls, 1);
	int s = accept(ls, NULL, NULL);
	setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
	char *buf = malloc(MAXMSG);
	uint64_t c0 = thread_cpu_ns();
	for (int i = 0; i < N_RT; i++) {
		for (uint32_t got = 0; got < g_len;) {
			got += read(s, buf + got, g_len - got);
		}
		for (uint32_t put = 0; put < g_len;) {
			put += write(s, buf + put, g_len - put);
		}
	}
	g_b_cpu = thread_cpu_ns() - c0;
	close(s);
	close(ls);
	free(buf);
	return NULL;
}

static void tcp_pingpong(uint32_t len)
{
	static uint64_t lat[N_RT];
	g_len = len;
	g_tcp_port = 0;
	pthread_t t;
	pthread_create(&t, NULL, tcp_server, NULL);
	while (!__atomic_load_n(&g_tcp_port, __ATOMIC_ACQUIRE)) {
		usleep(1000);
	}
	usleep(10000);
	int s = socket(AF_INET, SOCK_STREAM, 0), one = 1;
	struct sockaddr_in sa = { .sin_family = AF_INET, .sin_port = htons(g_tcp_port), .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
	if (connect(s, (void *)&sa, sizeof sa)) {
		perror("connect");
		return;
	}
	setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
	char *buf = malloc(MAXMSG);
	memset(buf, 1, MAXMSG);
	uint64_t c0 = thread_cpu_ns(), t0 = now_ns();
	for (int i = 0; i < N_RT; i++) {
		uint64_t st = now_ns();
		for (uint32_t put = 0; put < len;) {
			put += write(s, buf + put, len - put);
		}
		for (uint32_t got = 0; got < len;) {
			got += read(s, buf + got, len - got);
		}
		lat[i] = now_ns() - st;
	}
	uint64_t wall = now_ns() - t0, a_cpu = thread_cpu_ns() - c0;
	pthread_join(t, NULL);
	close(s);
	free(buf);
	qsort(lat, N_RT, 8, cmp_u64);
	printf("  tcp  %-6s %7u B  RTT p50 %7.1f p99 %7.1f us | CPU/RT client %5.1f us server %5.1f us | %6.0f MB/s\n",
	       "block", len, lat[N_RT / 2] / 1e3, lat[N_RT * 99 / 100] / 1e3,
	       a_cpu / 1e3 / N_RT, g_b_cpu / 1e3 / N_RT, 2.0 * len * N_RT / (wall / 1e3));
}

/* ---- R2 / R3 ---- */

static void reg_cost(void)
{
	size_t sizes[] = { 4096, 65536, 1 << 20, 16 << 20, 256 << 20 };
	for (int i = 0; i < 5; i++) {
		size_t sz = sizes[i];
		int iters = sz >= (16 << 20) ? 5 : 200;
		char *p = aligned_alloc(4096, sz);
		memset(p, 0, sz);
		uint64_t reg = 0, dereg = 0;
		for (int k = 0; k < iters; k++) {
			uint64_t t0 = now_ns();
			struct ibv_mr *mr = f_reg_mr(g_pd, p, sz, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
			uint64_t t1 = now_ns();
			if (!mr) {
				printf("  reg %zu failed: %s\n", sz, strerror(errno));
				break;
			}
			f_dereg_mr(mr);
			reg += t1 - t0;
			dereg += now_ns() - t1;
		}
		printf("  ibv_reg_mr %9zu B: reg %8.1f us, dereg %8.1f us\n", sz, reg / 1e3 / iters, dereg / 1e3 / iters);
		free(p);
	}
}

static void memlock_limit(void)
{
	struct rlimit rl;
	getrlimit(RLIMIT_MEMLOCK, &rl);
	printf("  this shell: RLIMIT_MEMLOCK soft %llu KB\n", (unsigned long long)rl.rlim_cur / 1024);
	size_t limits[] = { 64 << 10, 8 << 20 };
	for (int i = 0; i < 2; i++) {
		pid_t pid = fork();
		if (pid == 0) {
			struct rlimit l = { limits[i], limits[i] };
			setrlimit(RLIMIT_MEMLOCK, &l);
			size_t sizes[] = { 32 << 10, 1 << 20, 4 << 20, 16 << 20 };
			printf("  memlock %5zu KB:", limits[i] >> 10);
			for (int k = 0; k < 4; k++) {
				char *p = aligned_alloc(4096, sizes[k]);
				memset(p, 0, sizes[k]);
				struct ibv_mr *mr = f_reg_mr(g_pd, p, sizes[k], IBV_ACCESS_LOCAL_WRITE);
				printf("  reg %5zu KB %s", sizes[k] >> 10, mr ? "ok" : strerror(errno));
				if (mr) {
					f_dereg_mr(mr);
				}
				free(p);
			}
			printf("\n");
			fflush(stdout);
			_exit(0);
		}
		waitpid(pid, NULL, 0);
	}
}

/* ---- R5 / R6 ---- */

static void connect_pair(struct node *a, struct node *b, uint8_t timeout, uint8_t retry, uint8_t rnr)
{
	qp_create(a);
	qp_create(b);
	if (qp_connect(a->qp, b->qp->qp_num, timeout, retry, rnr) ||
	    qp_connect(b->qp, a->qp->qp_num, timeout, retry, rnr)) {
		perror("qp_connect");
		exit(1);
	}
}

static void buffers(void)
{
	struct node c, d;
	node_init(&c, "C");
	node_init(&d, "D");
	connect_pair(&c, &d, 14, 7, 7);
	char *heap = malloc(4096);
	memset(heap, 7, 4096);

	post_recv(&d, MAXMSG);
	int rc = post_send(&c, heap, 64, 0, IBV_SEND_INLINE);
	struct ibv_wc wc = wait_wc(&c);
	printf("  64 B inline send from unregistered heap: post %d, completion %s\n", rc, f_wc_status_str(wc.status));
	wait_wc(&d);

	post_recv(&d, MAXMSG);
	rc = post_send(&c, heap, 4096, c.mr->lkey, 0);
	printf("  4 KB send from unregistered heap with another MR's lkey: post %d\n", rc);
	drain(&c, "sender");
	drain(&d, "receiver");
	post_recv(&d, MAXMSG);
	rc = post_send(&c, c.buf, 64, c.mr->lkey, 0);
	printf("  next valid send on the same QP: post %d\n", rc);
	drain(&c, "sender");
	free(heap);
}

static void failure(const char *what, uint8_t timeout, uint8_t retry, uint8_t rnr, bool kill_peer)
{
	struct node c, d;
	node_init(&c, "C");
	node_init(&d, "D");
	connect_pair(&c, &d, timeout, retry, rnr);
	if (kill_peer) {
		f_destroy_qp(d.qp);
	}
	uint64_t t0 = now_ns();
	post_send(&c, c.buf, 4096, c.mr->lkey, 0);
	struct ibv_wc wc;
	bool got = wait_wc_for(&c, &wc, 15000);
	printf("  %-36s timeout=%2u retry=%u rnr_retry=%u -> %s after %.1f ms\n", what, timeout, retry, rnr,
	       got ? f_wc_status_str(wc.status) : "NO COMPLETION", (now_ns() - t0) / 1e6);
}

int main(void)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	printf("R0 load\n");
	if (load_verbs()) {
		return 0;
	}
	int n = 0;
	struct ibv_device **devs = f_get_device_list(&n);
	if (!devs || n == 0) {
		printf("  0 RDMA devices -> transport = TCP\n");
		return 0;
	}
	printf("R1 device\n");
	g_ctx = f_open_device(devs[0]);
	if (!g_ctx) {
		printf("  open %s failed: %s (check /dev/infiniband permissions)\n", f_get_device_name(devs[0]), strerror(errno));
		return 1;
	}
	struct ibv_device_attr da;
	f_query_device(g_ctx, &da);
	struct ibv_port_attr pa;
	memset(&pa, 0, sizeof(pa));
	f_query_port(g_ctx, 1, &pa);
	g_mtu = pa.active_mtu;
	for (int i = 0; i < da.phys_port_cnt * 16 && i < 16; i++) {
		union ibv_gid gid;
		if (f_query_gid(g_ctx, 1, i, &gid) == 0 && gid.raw[10] == 0xff && gid.raw[11] == 0xff) {
			g_gid_idx = i;
			g_gid = gid;
			break;
		}
	}
	printf("  %s: %d device(s), port state %d, active MTU %d, max_qp %d, max_mr_size %llu MB\n",
	       f_get_device_name(devs[0]), n, pa.state, 128 << pa.active_mtu, da.max_qp,
	       (unsigned long long)da.max_mr_size >> 20);
	if (g_gid_idx < 0) {
		printf("  no IPv4-mapped (RoCE v2) GID: is the netdev up with an IPv4 address?\n");
		return 1;
	}
	printf("  RoCE v2 GID index %d = %u.%u.%u.%u\n", g_gid_idx, g_gid.raw[12], g_gid.raw[13], g_gid.raw[14], g_gid.raw[15]);
	enum ibv_fork_status fs = f_is_fork_initialized();
	printf("  ibv_is_fork_initialized = %s\n", fs == IBV_FORK_UNNEEDED ? "UNNEEDED (kernel copes with fork)" :
	       fs == IBV_FORK_ENABLED ? "ENABLED" : "DISABLED (fork would corrupt registered memory)");
	g_pd = f_alloc_pd(g_ctx);

	if (getenv("RS_SKIP_PERF")) {
		goto tail;
	}
	printf("R2 registration cost\n");
	reg_cost();
	printf("R3 RLIMIT_MEMLOCK (no CAP_IPC_LOCK)\n");
	memlock_limit();

	printf("R4 round trips, %d each\n", N_RT);
	node_init(&A, "A");
	node_init(&B, "B");
	connect_pair(&A, &B, 14, 7, 7);
	uint32_t lens[] = { 64, 4096, 65536, 1 << 20 };
	for (int i = 0; i < 4; i++) {
		pingpong(lens[i], false);
		pingpong(lens[i], true);
		tcp_pingpong(lens[i]);
	}
tail:
	printf("R5 buffers\n");
	buffers();
	printf("R6 failures\n");
	if (getenv("RS_SWEEP")) {
		for (int t = 8; t <= 16; t++) {
			failure("peer QP destroyed (node died)", t, 1, 7, true);
		}
		for (int r = 5; r <= 7; r++) {
			failure("peer QP destroyed (node died)", 10, r, 7, true);
		}
	}
	failure("peer QP destroyed (node died)", 14, 7, 7, true);
	failure("peer QP destroyed (node died)", 10, 3, 7, true);
	failure("peer QP destroyed (node died)", 0, 7, 7, true);
	failure("peer alive, no receive posted", 14, 7, 3, false);
	f_free_device_list(devs);
	return 0;
}
