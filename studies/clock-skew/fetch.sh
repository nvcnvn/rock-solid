#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff
docs=27e64c5d4481a3c888f8f1f61c540fcba37e0594

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get cockroachdb/cockroach $crdb pkg/util/hlc/doc.go
get cockroachdb/cockroach $crdb pkg/util/hlc/hlc.go
get cockroachdb/cockroach $crdb pkg/server/clock_monotonicity.go
get cockroachdb/cockroach $crdb pkg/rpc/clock_offset.go
get cockroachdb/cockroach $crdb pkg/server/config.go
get cockroachdb/cockroach $crdb pkg/base/constants.go
get cockroachdb/cockroach $crdb pkg/base/config.go
get cockroachdb/cockroach $crdb pkg/cli/cliflags/flags.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/uncertainty/doc.go
get cockroachdb/cockroach $crdb pkg/kv/kvclient/kvcoord/txn_interceptor_span_refresher.go
get cockroachdb/cockroach $crdb pkg/kv/kvclient/kvcoord/txn_coord_sender.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/closedts/policy.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/kvserverpb/lease_status.proto
get cockroachdb/cockroach $crdb pkg/kv/kvserver/leases/status.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/doc.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/storelivenesspb/service.proto
get cockroachdb/cockroach $crdb docs/RFCS/20200811_non_blocking_txns.md
get cockroachdb/cockroach $crdb docs/RFCS/20160210_range_leases.md
get cockroachdb/cockroach $crdb docs/design.md
get cockroachdb/docs $docs src/current/v26.1/architecture/transaction-layer.md
get cockroachdb/docs $docs src/current/_includes/v26.1/faq/clock-synchronization-effects.md
get cockroachdb/docs $docs src/current/_includes/v26.1/misc/multiregion-max-offset.md
get cockroachdb/docs $docs src/current/v26.1/global-tables.md
get cockroachdb/docs $docs src/current/v26.1/transaction-retry-error-reference.md
get cockroachdb/docs $docs src/current/v26.1/frequently-asked-questions.md

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -6
}

c=cockroachdb_cockroach_
d=cockroachdb_docs_src_current_
show ${c}pkg_util_hlc_doc.go "coarsely-synchronized|waiting out|HAZARD: If the maximum clock offset|transaction isolation has no clock|stasis|minority self-terminates|completely untested"
show ${c}pkg_util_hlc_hlc.go "should be taken as|backward time jump detected|return c.maxOffset / 2"
show ${c}pkg_server_clock_monotonicity.go "we wait up to MaxOffset"
show ${c}pkg_rpc_clock_offset.go "healthyOffsetCount <= numClocks/2|ambiguousHealth:|offset.Uncertainty = \(pingDuration / 2\)|Cristian"
show ${c}pkg_server_config.go "toleratedOffsetMultiplier = 0.8"
show ${c}pkg_base_constants.go "DefaultMaxClockOffset = "
show ${c}pkg_base_config.go "Total latency|COCKROACH_RANGE_LEASE_DURATION|PingInterval = "
show ${c}pkg_cli_cliflags_flags.go "Name: \"(max-offset|disable-max-offset-check|clock-device)\"|smallest max-offset"
show ${c}pkg_kv_kvserver_uncertainty_doc.go "transaction's initial timestamp|ReadWithinUncertaintyIntervalError$|Any writes that the transaction may later see|stasis period before lease expiration"
show ${c}pkg_kv_kvclient_kvcoord_txn_interceptor_span_refresher.go "pessimistic write locks|1<<22"
show ${c}pkg_kv_kvclient_kvcoord_txn_coord_sender.go "Spanner performs a similar commit-wait|waitUntil = waitUntil.Add"
show ${c}pkg_kv_kvserver_closedts_policy.go "800ms|return maxTransportPropTime \+ maxClockOffset"
show ${c}pkg_kv_kvserver_kvserverpb_lease_status.proto "stasis period|UNUSABLE = 2"
show ${c}pkg_kv_kvserver_leases_status.go "stasis := expiration|stasis.LessEq|catch-all"
show ${c}pkg_kv_kvserver_storeliveness_doc.go "Clock skew and drift do not affect|MVCC domain|drawn from the requester's clock"
show ${c}pkg_kv_kvserver_storeliveness_storelivenesspb_service.proto "Support Disjointness Invariant of Store Liveness"
show ${c}docs_RFCS_20200811_non_blocking_txns.md "at least 2|weaker guarantee|wait out the uncertainty before"
show ${c}docs_RFCS_20160210_range_leases.md "liveness expiration must be at least the maximum clock offset"
show ${c}docs_design.md "wait out the$|maximum clock offset after committing|causality tokens"
show ${d}v26.1_architecture_transaction-layer.md "it crashes immediately|never encounter uncertainty"
show ${d}_includes_v26.1_faq_clock-synchronization-effects.md "spontaneously shuts down|vMotion"
show ${d}_includes_v26.1_misc_multiregion-max-offset.md "250ms"
show ${d}v26.1_global-tables.md "Writes incur higher latencies"
show ${d}v26.1_transaction-retry-error-reference.md "generally a sign of contention"
show ${d}v26.1_frequently-asked-questions.md "designed to work without atomic clocks"
