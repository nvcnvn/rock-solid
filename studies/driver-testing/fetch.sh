#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

go=8af21751f066eced273ca3ce49506b366847c623     # go1.27.0
tb=0450c32b2c3c3ab6114f3402cc601341cc8192aa     # TigerBeetle main, 2026-10-14
fdb=487620d5aa41152b1f034f4e932005a87eb2947f    # FoundationDB main, 2026-10-16
gnet=441457f07983e2e2fbc12a33c05fa68ffdc5ee47   # gnet dev, 2026-07-03
pebble=a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23 # as in /Volumes/T5/Codes/upstream/pebble
crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff   # as in /Volumes/T5/Codes/upstream/cockroach
madsim=519950efb4711464f300ed7edf2967ed62d5f502 # madsim main, 2026-02-16
linux=7d0a66e4bb9081d75c82ec4957c50034cb0ea449  # v6.18

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get golang/go $go src/testing/synctest/synctest.go
get golang/go $go src/runtime/synctest.go
get golang/go $go src/runtime/time.go
get golang/go $go src/runtime/runtime2.go
get golang/go $go src/runtime/proc.go
get golang/go $go src/runtime/netpoll.go
get tigerbeetle/tigerbeetle $tb src/stdx/time.zig
get tigerbeetle/tigerbeetle $tb src/io.zig
get tigerbeetle/tigerbeetle $tb src/io/linux.zig
get tigerbeetle/tigerbeetle $tb src/io/test.zig
get tigerbeetle/tigerbeetle $tb src/testing/io.zig
get tigerbeetle/tigerbeetle $tb src/testing/cluster.zig
get tigerbeetle/tigerbeetle $tb src/tigerbeetle/main.zig
get tigerbeetle/tigerbeetle $tb src/vsr/replica.zig
get apple/foundationdb $fdb flow/include/flow/network.h
get apple/foundationdb $fdb flow/include/flow/TaskQueue.h
get apple/foundationdb $fdb flow/Net2.cpp
get apple/foundationdb $fdb fdbrpc/sim2.cpp
get panjf2000/gnet $gnet gnet_test.go
get panjf2000/gnet $gnet eventloop_unix.go
get panjf2000/gnet $gnet pkg/netpoll/poller_epoll_default.go
get cockroachdb/pebble $pebble wal/failover_manager.go
get cockroachdb/pebble $pebble wal/failover_manager_test.go
get cockroachdb/pebble $pebble internal/deletepacer/rate_calc.go
get cockroachdb/pebble $pebble vfs/errorfs/errorfs.go
get cockroachdb/pebble $pebble vfs/errorfs/latency.go
get cockroachdb/pebble $pebble vfs/mem_fs.go
get cockroachdb/cockroach $crdb pkg/util/timeutil/time_source.go
get cockroachdb/cockroach $crdb pkg/util/timeutil/manual_time.go
get cockroachdb/cockroach $crdb pkg/util/timeutil/timer.go
get cockroachdb/cockroach $crdb pkg/util/timeutil/synctest.go
get cockroachdb/cockroach $crdb pkg/util/hlc/hlc.go
get madsim-rs/madsim $madsim madsim/src/sim/time/system_time.rs
get torvalds/linux $linux kernel/time/namespace.c
get torvalds/linux $linux kernel/time/timekeeping.c
get torvalds/linux $linux fs/timerfd.c
get torvalds/linux $linux net/unix/af_unix.c

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -8
}

g=golang_go_src_
show ${g}testing_synctest_synctest.go "uses a fake clock|only advances when every goroutine|can be unblocked by an event from outside|blocking on I/O|^//   - system calls|Operating on a bubbled"
show ${g}runtime_synctest.go "running int // non-blocked|func \(bubble \*synctestBubble\) changegstatus|case _Gwaiting:|if bubble.running > 0 \|\| bubble.active > 0|synctestBaseTime ="
show ${g}runtime_time.go "func time_runtimeNow|omit the monotonic time|func time_runtimeNano"
show ${g}runtime_runtime2.go "var isIdleInSynctest|waitReasonSleep: +true|waitReasonIOWait +// "
show ${g}runtime_proc.go "If we have a bubble, we need to fall into casgstatus|forcePreemptNS = "
show ${g}runtime_netpoll.go "gopark\(netpollblockcommit"

t=tigerbeetle_tigerbeetle_src_
show ${t}stdx_time.zig "^pub const Time = struct|monotonic: \*const fn|CLOCK.BOOTTIME|regressed the monotonic clock|^pub const TimeSim|pub fn tick\(self: \*TimeSim\)|linear,$"
show ${t}io.zig "^pub const IO = switch"
show ${t}io_linux.zig "time: TimeOS = |pub fn run_for_ns"
show ${t}io_test.zig "^test \"timeout\"|assert\(elapsed.ns >= delay.ns\)|maybe\(elapsed.ns >= delay.ns \* 2\)|delays as large as 200ms"
show ${t}testing_io.zig "very simple mock IO"
show ${t}testing_cluster.zig "TimeSim = stdx.TimeSim|Time keeps ticking"
show ${t}tigerbeetle_main.zig "replica.tick\(\);|try io.run_for_ns"
show ${t}vsr_replica.zig "^pub fn ReplicaType|comptime Storage: type|time: Time,"

f=apple_foundationdb_
show ${f}flow_include_flow_network.h "virtual double now\(\) const = 0|virtual double timer\(\) = 0|virtual Future<class Void> delay|only updates in the run loop"
show ${f}flow_include_flow_TaskQueue.h "^class TaskQueue|double getSleepTime|void processReadyTimers"
show ${f}flow_Net2.cpp "TaskQueue<PromiseTask> taskQueue|void updateNow\(\)|^void Net2::run\(\)|taskQueue.processReadyTimers\(now\)|reactor.sleep\(sleepTime\)|reactor.react\(\)|^		updateNow\(\);"
show ${f}fdbrpc_sim2.cpp "double now\(\) const override \{ return time; \}|timerTime \+= deterministicRandom|static void runLoop|taskQueue.canSleep\(\)\) \{|self->time \+=|processReadyTimers\(self->time\)"

n=panjf2000_gnet_
show ${n}pkg_netpoll_poller_epoll_default.go "msec := -1|unix.EpollWait"
show ${n}eventloop_unix.go "func \(el \*eventloop\) ticker|time.NewTimer\(delay\)"
show ${n}gnet_test.go "^func testTick|250&time.Millisecond|bad ticker timing"

p=cockroachdb_pebble_
show ${p}wal_failover_manager.go "extracted from CockroachDB's timeutil|^type timeSource interface|func \(defaultTime\) now"
show ${p}wal_failover_manager_test.go "^type manualTime struct|func \(m \*manualTime\) advance|synctest.Test\(t|errorfs.RandomLatency"
show ${p}internal_deletepacer_rate_calc.go "problematic with synctest"
show ${p}vfs_errorfs_errorfs.go "^type Injector interface|^func Wrap"
show ${p}vfs_errorfs_latency.go "^func RandomLatency"
show ${p}vfs_mem_fs.go "^func NewCrashableMem|CrashClone\(cfg"

c=cockroachdb_cockroach_pkg_util_
show ${c}timeutil_time_source.go "^type TimeSource interface|NewTimer\(\) TimerI"
show ${c}timeutil_manual_time.go "^type ManualTime struct|func \(m \*ManualTime\) Advance\(|func \(m \*ManualTime\) AdvanceTo\("
show ${c}timeutil_timer.go "activeSynctestBubbles counts|prevents non-bubble timers|^var activeSynctestBubbles"
show ${c}timeutil_synctest.go "^func SyncTest|activeSynctestBubbles.Add"
show ${c}hlc_hlc.go "^type WallClock interface|^type HybridManualClock struct|func \(m \*HybridManualClock\) Pause"

show madsim-rs_madsim_madsim_src_sim_time_system_time.rs "Override the libc .clock_gettime.|RTLD_NEXT, c\"clock_gettime\""

l=torvalds_linux_
show ${l}kernel_time_namespace.c "ns->frozen_offsets = true|if \(time_ns->frozen_offsets\)|err = -EACCES"
show ${l}kernel_time_timekeeping.c "static inline void tk_update_sleep_time|tk->offs_boot, ktime_add|tk_update_sleep_time\(tk, timespec64"
show ${l}fs_timerfd.c "clockid != CLOCK_BOOTTIME &&|it_value = ktime_to_timespec64\(timerfd_get_remaining"
show ${l}net_unix_af_unix.c "^static int unix_stream_sendmsg\(struct socket \*sock|other->sk_data_ready\(other\);"
