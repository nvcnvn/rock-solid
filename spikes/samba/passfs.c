/* Counting passthrough FUSE fs: a stand-in for our FUSE frontend under Samba.
 *
 *   passfs BACKING MOUNTPOINT [locks] [noxattr] [ttl=SECONDS] [-- extra fuse args]
 *
 * Two mounts of one BACKING dir stand in for two nodes of the cluster filesystem.
 *   locks    implement FUSE lock/flock by forwarding to OFD locks on the backing file,
 *            so byte-range locks are shared between the mounts (what MetaStore locks would do).
 *            Without it the kernel keeps locks local to each mount.
 *   noxattr  answer every xattr request with ENOTSUP.
 *   ttl=N    attribute and entry timeout (default 1 s, as in the design's Tier 1).
 *
 * Reading MOUNTPOINT/.stats returns the per-op counts and the op sequence since the last read,
 * then clears them. .stats itself is not counted.
 */
#define FUSE_USE_VERSION 31
#define _GNU_SOURCE
#include <fuse.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <unistd.h>

static char *backing;
static int opt_noxattr;
static double opt_ttl = 1.0;

enum { OP_GETATTR, OP_READDIR, OP_OPENDIR, OP_CREATE, OP_OPEN, OP_READ, OP_WRITE, OP_FLUSH, OP_RELEASE, OP_FSYNC,
       OP_TRUNC, OP_UNLINK, OP_MKDIR, OP_RMDIR, OP_RENAME, OP_UTIMENS, OP_CHMOD, OP_CHOWN, OP_STATFS,
       OP_GETXATTR, OP_SETXATTR, OP_LISTXATTR, OP_RMXATTR, OP_LOCK, OP_FLOCK, OP_FALLOCATE, OP_ACCESS, OP_N };
static const char *names[OP_N] = {"getattr", "readdir", "opendir", "create", "open", "read", "write", "flush",
    "release", "fsync", "truncate", "unlink", "mkdir", "rmdir", "rename", "utimens", "chmod", "chown", "statfs",
    "getxattr", "setxattr", "listxattr", "removexattr", "lock", "flock", "fallocate", "access"};
static unsigned long counts[OP_N];
static char seq[16384];
static size_t seqlen;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;

static int is_stats(const char *p) { return strcmp(p, "/.stats") == 0; }

static void count(int op, const char *path, const char *extra)
{
    if (path && is_stats(path))
        return;
    pthread_mutex_lock(&mu);
    counts[op]++;
    if (seqlen < sizeof(seq) - 256)
        seqlen += snprintf(seq + seqlen, sizeof(seq) - seqlen, "%s %s%s%s\n", names[op], path ? path : "",
                           extra ? " " : "", extra ? extra : "");
    pthread_mutex_unlock(&mu);
}

static void bpath(char *out, const char *path) { snprintf(out, PATH_MAX, "%s%s", backing, path); }
#define B(p) char b[PATH_MAX]; bpath(b, p)

static int fs_getattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
    if (is_stats(path)) {
        memset(st, 0, sizeof(*st));
        st->st_mode = S_IFREG | 0444; st->st_nlink = 1; st->st_size = 0;
        st->st_uid = getuid(); st->st_gid = getgid(); st->st_ino = 1;
        return 0;
    }
    count(OP_GETATTR, path, fi ? "(fh)" : NULL);
    int r;
    if (fi) r = fstat(fi->fh, st);
    else { B(path); r = lstat(b, st); }
    return r ? -errno : 0;
}

static int fs_access(const char *path, int mask) { count(OP_ACCESS, path, NULL); B(path); return access(b, mask) ? -errno : 0; }

static int fs_opendir(const char *path, struct fuse_file_info *fi) { count(OP_OPENDIR, path, NULL); (void)fi; return 0; }

static int fs_readdir(const char *path, void *buf, fuse_fill_dir_t fill, off_t off, struct fuse_file_info *fi,
                      enum fuse_readdir_flags fl)
{
    (void)off; (void)fi; (void)fl;
    count(OP_READDIR, path, NULL);
    B(path);
    DIR *d = opendir(b);
    if (!d) return -errno;
    struct dirent *e;
    while ((e = readdir(d))) {
        struct stat st = {.st_ino = e->d_ino, .st_mode = (mode_t)DTTOIF(e->d_type)};
        if (fill(buf, e->d_name, &st, 0, 0)) break;
    }
    closedir(d);
    return 0;
}

static int fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    count(OP_CREATE, path, NULL);
    B(path);
    int fd = open(b, fi->flags | O_CREAT, mode);
    if (fd < 0) return -errno;
    fi->fh = fd;
    return 0;
}

static int fs_open(const char *path, struct fuse_file_info *fi)
{
    if (is_stats(path)) { fi->direct_io = 1; fi->fh = (uint64_t)-1; return 0; }
    char fl[32];
    snprintf(fl, sizeof(fl), "flags=%#o", fi->flags);
    count(OP_OPEN, path, fl);
    B(path);
    int fd = open(b, fi->flags & ~O_TRUNC);
    if (fd < 0) return -errno;
    if (fi->flags & O_TRUNC) { if (ftruncate(fd, 0)) { int e = errno; close(fd); return -e; } }
    fi->fh = fd;
    fi->keep_cache = 0; /* close-to-open: drop pages at open, as Tier 1 does for changed files */
    return 0;
}

static int fs_read(const char *path, char *buf, size_t n, off_t off, struct fuse_file_info *fi)
{
    if (is_stats(path)) {
        if (off) return 0;
        pthread_mutex_lock(&mu);
        size_t len = 0;
        for (int i = 0; i < OP_N; i++)
            if (counts[i]) len += snprintf(buf + len, n - len, "%s=%lu ", names[i], counts[i]);
        len += snprintf(buf + len, n - len, "\n%.*s", (int)(seqlen < n - len - 1 ? seqlen : n - len - 1), seq);
        memset(counts, 0, sizeof(counts)); seqlen = 0;
        pthread_mutex_unlock(&mu);
        return (int)len;
    }
    count(OP_READ, path, NULL);
    ssize_t r = pread(fi->fh, buf, n, off);
    return r < 0 ? -errno : (int)r;
}

static int fs_write(const char *path, const char *buf, size_t n, off_t off, struct fuse_file_info *fi)
{
    count(OP_WRITE, path, NULL);
    ssize_t r = pwrite(fi->fh, buf, n, off);
    return r < 0 ? -errno : (int)r;
}

static int fs_flush(const char *path, struct fuse_file_info *fi) { if (!is_stats(path)) count(OP_FLUSH, path, NULL); (void)fi; return 0; }

static int fs_release(const char *path, struct fuse_file_info *fi)
{
    if (is_stats(path)) return 0;
    count(OP_RELEASE, path, NULL);
    close(fi->fh);
    return 0;
}

static int fs_fsync(const char *path, int ds, struct fuse_file_info *fi)
{
    count(OP_FSYNC, path, NULL);
    return (ds ? fdatasync(fi->fh) : fsync(fi->fh)) ? -errno : 0;
}

static int fs_truncate(const char *path, off_t sz, struct fuse_file_info *fi)
{
    count(OP_TRUNC, path, NULL);
    int r;
    if (fi) r = ftruncate(fi->fh, sz);
    else { B(path); r = truncate(b, sz); }
    return r ? -errno : 0;
}

static int fs_unlink(const char *path) { count(OP_UNLINK, path, NULL); B(path); return unlink(b) ? -errno : 0; }
static int fs_mkdir(const char *path, mode_t m) { count(OP_MKDIR, path, NULL); B(path); return mkdir(b, m) ? -errno : 0; }
static int fs_rmdir(const char *path) { count(OP_RMDIR, path, NULL); B(path); return rmdir(b) ? -errno : 0; }

static int fs_rename(const char *from, const char *to, unsigned int flags)
{
    count(OP_RENAME, from, to);
    char a[PATH_MAX], b[PATH_MAX];
    bpath(a, from); bpath(b, to);
    return renameat2(AT_FDCWD, a, AT_FDCWD, b, flags) ? -errno : 0;
}

static int fs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
    count(OP_UTIMENS, path, NULL);
    int r;
    if (fi) r = futimens(fi->fh, tv);
    else { B(path); r = utimensat(AT_FDCWD, b, tv, AT_SYMLINK_NOFOLLOW); }
    return r ? -errno : 0;
}

static int fs_chmod(const char *path, mode_t m, struct fuse_file_info *fi)
{
    count(OP_CHMOD, path, NULL);
    int r;
    if (fi) r = fchmod(fi->fh, m);
    else { B(path); r = chmod(b, m); }
    return r ? -errno : 0;
}

static int fs_chown(const char *path, uid_t u, gid_t g, struct fuse_file_info *fi)
{
    count(OP_CHOWN, path, NULL);
    int r;
    if (fi) r = fchown(fi->fh, u, g);
    else { B(path); r = lchown(b, u, g); }
    return r ? -errno : 0;
}

static int fs_statfs(const char *path, struct statvfs *s) { count(OP_STATFS, path, NULL); B(path); return statvfs(b, s) ? -errno : 0; }

static int fs_getxattr(const char *path, const char *name, char *v, size_t n)
{
    count(OP_GETXATTR, path, name);
    if (opt_noxattr) return -ENOTSUP;
    B(path);
    ssize_t r = lgetxattr(b, name, v, n);
    return r < 0 ? -errno : (int)r;
}

static int fs_setxattr(const char *path, const char *name, const char *v, size_t n, int fl)
{
    count(OP_SETXATTR, path, name);
    if (opt_noxattr) return -ENOTSUP;
    B(path);
    return lsetxattr(b, name, v, n, fl) ? -errno : 0;
}

static int fs_listxattr(const char *path, char *l, size_t n)
{
    count(OP_LISTXATTR, path, NULL);
    if (opt_noxattr) return -ENOTSUP;
    B(path);
    ssize_t r = llistxattr(b, l, n);
    return r < 0 ? -errno : (int)r;
}

static int fs_removexattr(const char *path, const char *name)
{
    count(OP_RMXATTR, path, name);
    if (opt_noxattr) return -ENOTSUP;
    B(path);
    return lremovexattr(b, name) ? -errno : 0;
}

/* Byte-range locks forwarded to OFD locks on the backing fd: visible to the other mount. */
static int fs_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *l)
{
    char ex[96];
    snprintf(ex, sizeof(ex), "%s type=%d start=%lld len=%lld", cmd == F_GETLK ? "GETLK" : cmd == F_SETLK ? "SETLK" : "SETLKW",
             l->l_type, (long long)l->l_start, (long long)l->l_len);
    count(OP_LOCK, path, ex);
    int ocmd = cmd == F_GETLK ? F_OFD_GETLK : cmd == F_SETLK ? F_OFD_SETLK : F_OFD_SETLKW;
    l->l_pid = 0;
    if (fcntl(fi->fh, ocmd, l)) return -errno;
    return 0;
}

static int fs_flock(const char *path, struct fuse_file_info *fi, int op)
{
    count(OP_FLOCK, path, NULL);
    return flock(fi->fh, op) ? -errno : 0;
}

static int fs_fallocate(const char *path, int mode, off_t off, off_t len, struct fuse_file_info *fi)
{
    count(OP_FALLOCATE, path, NULL);
    return fallocate(fi->fh, mode, off, len) ? -errno : 0;
}

static void *fs_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    (void)conn;
    cfg->use_ino = 1;        /* stable inode numbers, same on every mount (like our inode IDs) */
    cfg->attr_timeout = opt_ttl;
    cfg->entry_timeout = opt_ttl;
    cfg->negative_timeout = 0;
    return NULL;
}

static struct fuse_operations ops = {
    .init = fs_init, .getattr = fs_getattr, .access = fs_access, .opendir = fs_opendir, .readdir = fs_readdir,
    .create = fs_create, .open = fs_open, .read = fs_read, .write = fs_write, .flush = fs_flush,
    .release = fs_release, .fsync = fs_fsync, .truncate = fs_truncate, .unlink = fs_unlink, .mkdir = fs_mkdir,
    .rmdir = fs_rmdir, .rename = fs_rename, .utimens = fs_utimens, .chmod = fs_chmod, .chown = fs_chown,
    .statfs = fs_statfs, .getxattr = fs_getxattr, .setxattr = fs_setxattr, .listxattr = fs_listxattr,
    .removexattr = fs_removexattr, .fallocate = fs_fallocate,
};

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s BACKING MOUNTPOINT [locks] [noxattr] [ttl=N] [-- fuse args]\n", argv[0]);
        return 2;
    }
    backing = realpath(argv[1], NULL);
    if (!backing) { perror(argv[1]); return 1; }
    char *fargv[32];
    int fargc = 0;
    fargv[fargc++] = argv[0];
    fargv[fargc++] = "-f";
    int i = 3;
    for (; i < argc && strcmp(argv[i], "--"); i++) {
        if (!strcmp(argv[i], "locks")) { ops.lock = fs_lock; ops.flock = fs_flock; }
        else if (!strcmp(argv[i], "noxattr")) opt_noxattr = 1;
        else if (!strncmp(argv[i], "ttl=", 4)) opt_ttl = atof(argv[i] + 4);
        else { fprintf(stderr, "unknown option %s\n", argv[i]); return 2; }
    }
    for (i++; i < argc && fargc < 30; i++) fargv[fargc++] = argv[i];
    fargv[fargc++] = argv[2];
    return fuse_main(fargc, fargv, &ops, NULL);
}
