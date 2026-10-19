#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

ceph=b7e6787889ed0783df44063506dcacaaf672758f
vitastor=df593badb5dfdbfbacac97b0c310f6ea3d5c3dff
crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get ceph/ceph $ceph src/osd/OSD.cc
get ceph/ceph $ceph src/osd/OSDMap.cc
get ceph/ceph $ceph src/mon/OSDMonitor.cc
get ceph/ceph $ceph src/mon/PGMap.cc
get ceph/ceph $ceph src/common/options/global.yaml.in
get ceph/ceph $ceph src/common/options/mon.yaml.in
get ceph/ceph $ceph src/common/options/mgr.yaml.in
get ceph/ceph $ceph src/pybind/mgr/balancer/module.py
get ceph/ceph $ceph src/pybind/mgr/pg_autoscaler/module.py
get ceph/ceph $ceph doc/rados/configuration/mon-config-ref.rst
get ceph/ceph $ceph doc/rados/operations/balancer.rst
get vitalif/vitastor $vitastor mon/pg_gen.js
get vitalif/vitastor $vitastor mon/lp_optimizer/lp_optimizer.js
get vitalif/vitastor $vitastor mon/etcd_schema.js
get vitalif/vitastor $vitastor mon/prometheus.js
get vitalif/vitastor $vitastor docs/usage/cli.en.md
get vitalif/vitastor $vitastor docs/config/monitor.en.md
get cockroachdb/cockroach $crdb pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go
get cockroachdb/cockroach $crdb pkg/roachpb/metadata.go

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -6
}

c=ceph_ceph_
show ${c}src_osd_OSD.cc 'OSDService::recalc_full_state|pratio > failsafe_ratio|ratio > full_ratio|ratio > backfillfull_ratio|pratio > nearfull_ratio'
show ${c}src_osd_OSD.cc 'float OSDService::compute_adjusted_ratio|get_used_raw\(\)\) / '
show ${c}src_osd_OSDMap.cc 'void OSDMap::get_full_pools|get_pool_ids_by_osd\(cct, i, full\)'
show ${c}src_osd_OSDMap.cc '"OSD_FULL"|"OSD_BACKFILLFULL"|"OSD_NEARFULL"'
show ${c}src_osd_OSDMap.cc 'POOL_NO_REDUNDANCY|get_size\(\) == 1'
show ${c}src_osd_OSDMap.cc 'cur_max_deviation <= max_deviation|Using more_overfull|overfull = more_overfull|deviation <= max_deviation\)'
show ${c}src_osd_OSDMap.cc 'void OSDMap::fill_overfull_underfull|more_overfull.insert'
show ${c}src_mon_OSDMonitor.cc 'set per-pool near/backfill/full flag|flags \|= pg_pool_t::FLAG_FULL;|flags &= ~pg_pool_t::FLAG_NEARFULL;'
show ${c}src_mon_PGMap.cc 'PGMapDigest::get_pool_free_space|PGMap::get_rule_avail|std::max\(0.0, \(double\)osd_info|pool_raw_used_rate\(poolid\)'
show ${c}src_mon_PGMap.cc 'mon_pg_stuck_threshold|Low space hindering backfill|Full OSDs blocking recovery'
show ${c}src_mon_PGMap.cc 'quota_max_bytes\*crit_threshold|quota_max_bytes\*warn_threshold|"POOL_NEAR_FULL"'
show ${c}src_common_options_global.yaml.in 'name: mon_osd_(full|backfillfull|nearfull)_ratio|name: osd_failsafe_full_ratio|default: 0.9[57]?$|default: 0.85'
show ${c}src_common_options_global.yaml.in 'name: target_max_misplaced_ratio'
show ${c}src_common_options_mon.yaml.in 'name: mon_warn_on_pool_no_redundancy'
show ${c}src_common_options_mgr.yaml.in 'name: mon_pg_stuck_threshold|default: 1_min'
show ${c}src_pybind_mgr_balancer_module.py "name='(sleep_interval|upmap_max_optimizations|upmap_max_deviation)'|deviation below which no optimization"
show ${c}src_pybind_mgr_balancer_module.py 'Unable to find further optimization|no_optimization_needed = True|Too many objects|are degraded; try again'
show ${c}src_pybind_mgr_balancer_module.py 'def serve|self.event.wait\(sleep_interval\)'
show ${c}src_pybind_mgr_pg_autoscaler_module.py 'total_target_bytes \+= target_bytes \* osdmap.pool_raw_used_rate|s.capacity = capacity|overcommit available storage by|self.threshold = 3.0'
show ${c}doc_rados_configuration_mon-config-ref.rst 'nearfull`` ratio. This means|plan for a series of Ceph OSD|reasonable full ratio'
show ${c}doc_rados_operations_balancer.rst 'defaults to ``5``|shuffling data|not a percentage|differ in capacity|upmap_max_deviation 1'
show ${c}src_mon_OSDMonitor.cc 'pool min_size must be between'
show ${c}src_pybind_mgr_balancer_module.py 'override_toofull = backfill_toofull'
show ${c}src_osd_OSD.cc 'compute_adjusted_ratio\(adjusted_stat|pratio ='

v=vitalif_vitastor_
show ${v}mon_pg_gen.js 'total_raw_tb: optimize_result.space|space_efficiency: optimize_result.space'
show ${v}mon_lp_optimizer_lp_optimizer.js 'function pg_per_osd_space_efficiency|osd_sizes\[osd\] \* pg_count / per_osd\[osd\]|total_space: total_weight'
show ${v}mon_etcd_schema.js 'space_efficiency: float|total_raw_tb: float'
show ${v}mon_prometheus.js 'vitastor_pool_space_efficiency'
show ${v}docs_usage_cli.en.md 'EFFICIENCY|zero" efficiency'
show ${v}docs_config_monitor.en.md '## osd_out_time|Default: 600|restore data'

k=cockroachdb_cockroach_
show ${k}pkg_kv_kvserver_allocator_allocatorimpl_allocator_scorer.go 'defaultMaxDiskUtilizationThreshold *=|defaultRebalanceToMaxDiskUtilizationThreshold *=|FractionUsed\(\) < do\.'
show ${k}pkg_kv_kvserver_allocator_allocatorimpl_allocator_scorer.go 'greater than this value|ping pong'
show ${k}pkg_kv_kvserver_allocator_allocatorimpl_allocator_scorer.go 'kv.allocator.range_rebalance_threshold|func overfullRangeThreshold'
show ${k}pkg_roachpb_metadata.go 'func \(sc StoreCapacity\) FractionUsed|capacity is really just the available space plus|sc.Available\+sc.Used'
show ${k}pkg_kv_kvserver_allocator_allocatorimpl_allocator_scorer.go 'minRangeRebalanceThreshold = 2|always fire'
