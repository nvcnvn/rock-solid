#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

linux=7d0a66e4bb9081d75c82ec4957c50034cb0ea449 # v6.18
crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL --retry 4 --retry-delay 10 -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get torvalds/linux $linux arch/x86/kernel/tsc.c
get torvalds/linux $linux arch/x86/kernel/tsc_msr.c
get torvalds/linux $linux kernel/time/clocksource.c
get torvalds/linux $linux kernel/time/timekeeping.c
get torvalds/linux $linux arch/x86/kernel/kvmclock.c
get torvalds/linux $linux arch/x86/kernel/pvclock.c
get torvalds/linux $linux arch/x86/kvm/x86.c
get torvalds/linux $linux arch/x86/kernel/cpu/mshyperv.c
get torvalds/linux $linux arch/x86/kernel/cpu/vmware.c
[ -s clocksource-2017.c ] || curl -sSfL --retry 4 --retry-delay 10 -o clocksource-2017.c \
	https://raw.githubusercontent.com/torvalds/linux/b511203093489eb1829cb4de86e8214752205ac6/kernel/time/clocksource.c
[ -s skx.patch ] || curl -sSfL --retry 4 --retry-delay 10 -o skx.patch \
	https://github.com/torvalds/linux/commit/b511203093489eb1829cb4de86e8214752205ac6.patch

get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/doc.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/config.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/support_manager.go
get cockroachdb/cockroach $crdb pkg/rpc/clock_offset.go
get cockroachdb/cockroach $crdb pkg/rpc/peer.go
get cockroachdb/cockroach $crdb pkg/rpc/connection.go
get cockroachdb/cockroach $crdb pkg/server/clock_monotonicity.go
get cockroachdb/cockroach $crdb pkg/base/config.go
get cockroachdb/cockroach $crdb pkg/server/server.go

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -12
}

l=torvalds_linux_
show ${l}arch_x86_kernel_tsc.c "early_param\(\"tsc_early_khz\"|maximum error rate of 500ppm|most accurate one so far|X86_FEATURE_TSC_KNOWN_FREQ\);|inside a 10% window|Disable the clocksource watchdog when|Warn if the deviation exceeds 500 ppm|Make sure we're within 1%|retrieved via MSR or CPUID), we skip|tsc_khz = tsc_early_khz;"
show ${l}arch_x86_kernel_tsc_msr.c "does not contain a|lot of accuracy which leads to clock drift"
show ${l}kernel_time_clocksource.c "define WATCHDOG_INTERVAL |define WATCHDOG_THRESHOLD|based on NTP's limits|define MAX_SKEW_USEC|md = cs->uncertainty_margin \+ watchdog|at least 500ppm by default"
show ${l}kernel_time_timekeeping.c "tk->tkr_raw.mult = clock->mult|tk_setup_internals\(tks, new\)"
show ${l}arch_x86_kernel_kvmclock.c "static unsigned long kvm_get_tsc_khz|setup_force_cpu_cap\(X86_FEATURE_TSC_KNOWN_FREQ\)"
show ${l}arch_x86_kernel_pvclock.c "do_div\(pv_tsc_khz, src->tsc_to_system_mul\)"
show ${l}arch_x86_kvm_x86.c "with the frequency of the raw clock|tgt_tsc_khz = get_cpu_tsc_khz\(\)|kvm_get_time_scale\(NSEC_PER_SEC, tgt_tsc_khz"
show ${l}arch_x86_kernel_cpu_mshyperv.c "HV_X64_MSR_TSC_FREQUENCY, freq|calibrate_tsc = hv_get_tsc_khz|TSC_KNOWN_FREQ"
show ${l}arch_x86_kernel_cpu_vmware.c "if \(vmware_tsc_khz\)$|TSC_KNOWN_FREQ"
show clocksource-2017.c "define WATCHDOG_THRESHOLD"
show skx.patch "^Date:|-4.0% time drift|approximately\) -0.25%|1 second per"

c=cockroachdb_cockroach_
show ${c}pkg_kv_kvserver_storeliveness_doc.go "drawn from the requester's clock|increments its own epoch upon restart|Clock skew and drift do not affect"
show ${c}pkg_kv_kvserver_storeliveness_config.go "wait after restart before withdrawing|churn until the first heartbeats"
show ${c}pkg_kv_kvserver_storeliveness_support_manager.go "UpdateAndCheckMaxOffset|Wait out the previous maximum requested|minWithdrawalTS = |grace period hasn't elapsed"
show ${c}pkg_rpc_clock_offset.go "case definitelyHealthy, ambiguousHealth|healthyOffsetCount <= numClocks/2|pingDuration <= maximumPingDurationMult|offset.Uncertainty = \(pingDuration / 2\)"
show ${c}pkg_rpc_peer.go "runSingleHeartbeat: performs the first|onInitialHeartbeatSucceeded: signals|FatalOnOffsetViolation"
show ${c}pkg_rpc_connection.go "connFuture is signaled with success|initial heartbeat succeeds"
show ${c}pkg_server_clock_monotonicity.go "we wait up to MaxOffset|sleepUntil = startTime.UnixNano"
show ${c}pkg_base_config.go "STORE_LIVENESS_HEARTBEAT_INTERVAL\", time.Second|STORE_LIVENESS_SUPPORT_DURATION\", 3\*time.Second"
show ${c}pkg_server_server.go "FatalOnOffsetViolation = true"
