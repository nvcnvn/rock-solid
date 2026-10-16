// Spike: can a FUSE daemon force an already-open, page-cached handle to stop
// serving stale data? Two instances mount the same backing file (simulating two
// client nodes over shared storage). A control FIFO per instance lets the test
// driver trigger notify_inval_inode and change open/attr policy at runtime.
//
// usage: spikefs <mnt> <backing> <ctl_fifo> <ack_fifo> <log> [wb] [noautoinval]
#define FUSE_USE_VERSION 312
#include <fuse_lowlevel.h>
#include <errno.h>
#include <stdarg.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FILE_INO 2
static const char *FNAME = "f";

static int backing_fd, ver_fd;
static const char *ctl_path, *ack_path;
static FILE *logf;
static struct fuse_session *se;
static int opt_wb, opt_autoinval = 1;
static atomic_int open_direct = 0;   // FOPEN_DIRECT_IO for new opens
static atomic_int keep_cache = 1;    // FOPEN_KEEP_CACHE for new opens
static _Atomic double attr_ttl = 3600.0;
static atomic_long n_read, n_write, n_getattr;
static atomic_int churn; static atomic_long churn_ctr;

static void logm(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    vfprintf(logf, fmt, ap); fputc('\n', logf); fflush(logf);
    va_end(ap);
}

// Shared "data version": stands in for a MetaStore/primary version number.
static uint64_t ver_get(void) {
    uint64_t v = 0; pread(ver_fd, &v, sizeof v, 0); return v;
}
static void ver_bump(void) {
    flock(ver_fd, LOCK_EX);
    uint64_t v = ver_get() + 1; pwrite(ver_fd, &v, sizeof v, 0);
    flock(ver_fd, LOCK_UN);
}

static void fill_attr(fuse_ino_t ino, struct stat *st) {
    memset(st, 0, sizeof *st);
    st->st_ino = ino;
    if (ino == FUSE_ROOT_ID) { st->st_mode = S_IFDIR | 0755; st->st_nlink = 2; return; }
    struct stat b; fstat(backing_fd, &b);
    st->st_mode = S_IFREG | 0666; st->st_nlink = 1; st->st_size = b.st_size;
    uint64_t v = churn ? (uint64_t)atomic_fetch_add(&churn_ctr, 1) + 500000000 : ver_get();
    st->st_mtim.tv_sec = 1700000000; st->st_mtim.tv_nsec = (long)(v % 1000000000);
    st->st_ctim = st->st_mtim;
}

static void op_init(void *u, struct fuse_conn_info *c) {
    (void)u;
    if (opt_wb) c->want |= FUSE_CAP_WRITEBACK_CACHE; else c->want &= ~FUSE_CAP_WRITEBACK_CACHE;
    if (opt_autoinval) c->want |= FUSE_CAP_AUTO_INVAL_DATA; else c->want &= ~FUSE_CAP_AUTO_INVAL_DATA;
    logm("init wb=%d autoinval=%d", !!(c->want & FUSE_CAP_WRITEBACK_CACHE), !!(c->want & FUSE_CAP_AUTO_INVAL_DATA));
}

static void op_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    if (parent != FUSE_ROOT_ID || strcmp(name, FNAME)) { fuse_reply_err(req, ENOENT); return; }
    struct fuse_entry_param e = {0};
    e.ino = FILE_INO; e.attr_timeout = attr_ttl; e.entry_timeout = 3600;
    fill_attr(FILE_INO, &e.attr);
    fuse_reply_entry(req, &e);
}

static void op_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    (void)fi; atomic_fetch_add(&n_getattr, 1);
    struct stat st; fill_attr(ino, &st);
    fuse_reply_attr(req, &st, ino == FILE_INO ? attr_ttl : 3600);
}

static void op_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    if (ino != FILE_INO) { fuse_reply_err(req, EISDIR); return; }
    fi->direct_io = atomic_load(&open_direct);
    fi->keep_cache = atomic_load(&keep_cache);
    logm("open direct_io=%d keep_cache=%d", fi->direct_io, fi->keep_cache);
    fuse_reply_open(req, fi);
}

static void op_read(fuse_req_t req, fuse_ino_t ino, size_t sz, off_t off, struct fuse_file_info *fi) {
    (void)ino; (void)fi; atomic_fetch_add(&n_read, 1);
    char *buf = malloc(sz);
    ssize_t n = pread(backing_fd, buf, sz, off);
    fuse_reply_buf(req, buf, n < 0 ? 0 : n);
    free(buf);
}

static void op_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t sz, off_t off, struct fuse_file_info *fi) {
    (void)ino; (void)fi; atomic_fetch_add(&n_write, 1);
    ssize_t n = pwrite(backing_fd, buf, sz, off);
    ver_bump();
    logm("WRITE off=%ld len=%zu", (long)off, sz);
    if (n < 0) fuse_reply_err(req, errno); else fuse_reply_write(req, n);
}

static void op_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *a, int to_set, struct fuse_file_info *fi) {
    (void)fi;
    if (to_set & FUSE_SET_ATTR_SIZE) { ftruncate(backing_fd, a->st_size); ver_bump(); }
    struct stat st; fill_attr(ino, &st);
    fuse_reply_attr(req, &st, attr_ttl);
}

static void op_readdir(fuse_req_t req, fuse_ino_t ino, size_t sz, off_t off, struct fuse_file_info *fi) {
    (void)fi; (void)ino;
    char buf[512]; size_t p = 0; struct stat st = {0};
    const char *names[] = {".", "..", FNAME}; fuse_ino_t inos[] = {1, 1, FILE_INO};
    for (int i = (int)off; i < 3; i++) {
        st.st_ino = inos[i]; st.st_mode = i == 2 ? S_IFREG : S_IFDIR;
        size_t l = fuse_add_direntry(req, buf + p, sizeof buf - p, names[i], &st, i + 1);
        if (p + l > sz || p + l > sizeof buf) break;
        p += l;
    }
    fuse_reply_buf(req, buf, p);
}

static double now_us(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec * 1e6 + t.tv_nsec / 1e3;
}

// Control thread: one command per line on ctl FIFO, one reply line on ack FIFO.
static void *ctl_thread(void *u) {
    (void)u;
    for (;;) {
        FILE *in = fopen(ctl_path, "r");
        if (!in) return NULL;
        char line[128];
        while (fgets(line, sizeof line, in)) {
            char reply[256]; line[strcspn(line, "\n")] = 0;
            if (!strcmp(line, "inval")) {           // drop pages (launders dirty ones) + attrs
                long w0 = n_write; double t = now_us();
                int r = fuse_lowlevel_notify_inval_inode(se, FILE_INO, 0, 0);
                snprintf(reply, sizeof reply, "inval rc=%d us=%.1f writes_during=%ld", r, now_us() - t, n_write - w0);
            } else if (!strcmp(line, "inval_attr")) { // attrs only, keep pages
                int r = fuse_lowlevel_notify_inval_inode(se, FILE_INO, -1, 0);
                snprintf(reply, sizeof reply, "inval_attr rc=%d", r);
            } else if (!strncmp(line, "direct ", 7)) {
                open_direct = atoi(line + 7); snprintf(reply, sizeof reply, "direct=%d", open_direct);
            } else if (!strncmp(line, "keep ", 5)) {
                keep_cache = atoi(line + 5); snprintf(reply, sizeof reply, "keep=%d", keep_cache);
            } else if (!strncmp(line, "ttl ", 4)) {
                attr_ttl = atof(line + 4); snprintf(reply, sizeof reply, "ttl=%g", (double)attr_ttl);
            } else if (!strncmp(line, "churn ", 6)) {
                churn = atoi(line + 6); snprintf(reply, sizeof reply, "churn=%d", churn);
            } else if (!strcmp(line, "stats")) {
                snprintf(reply, sizeof reply, "read=%ld write=%ld getattr=%ld", n_read, n_write, n_getattr);
            } else if (!strcmp(line, "zero")) {
                n_read = n_write = n_getattr = 0; snprintf(reply, sizeof reply, "ok");
            } else snprintf(reply, sizeof reply, "unknown");
            FILE *out = fopen(ack_path, "w"); fprintf(out, "%s\n", reply); fclose(out);
            logm("ctl %s -> %s", line, reply);
        }
        fclose(in);
    }
}

int main(int argc, char **argv) {
    if (argc < 6) { fprintf(stderr, "usage\n"); return 2; }
    const char *mnt = argv[1];
    backing_fd = open(argv[2], O_RDWR | O_CREAT, 0644);
    char vp[512]; snprintf(vp, sizeof vp, "%s.ver", argv[2]);
    ver_fd = open(vp, O_RDWR | O_CREAT, 0644);
    ctl_path = argv[3]; ack_path = argv[4];
    logf = fopen(argv[5], "a");
    for (int i = 6; i < argc; i++) {
        if (!strcmp(argv[i], "wb")) opt_wb = 1;
        if (!strcmp(argv[i], "noautoinval")) opt_autoinval = 0;
    }
    static const struct fuse_lowlevel_ops ops = {
        .init = op_init, .lookup = op_lookup, .getattr = op_getattr, .open = op_open,
        .read = op_read, .write = op_write, .setattr = op_setattr, .readdir = op_readdir,
    };
    char *fargv[] = {argv[0], "-o", "fsname=spikefs", NULL};
    struct fuse_args args = FUSE_ARGS_INIT(3, fargv);
    se = fuse_session_new(&args, &ops, sizeof ops, NULL);
    if (!se || fuse_session_mount(se, mnt)) { fprintf(stderr, "mount failed\n"); return 1; }
    pthread_t t; pthread_create(&t, NULL, ctl_thread, NULL);
    struct fuse_loop_config *cfg = fuse_loop_cfg_create();
    int r = fuse_session_loop_mt(se, cfg);
    fuse_session_unmount(se);
    return r;
}
