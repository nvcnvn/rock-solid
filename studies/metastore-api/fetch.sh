#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-"$here/work"}
mkdir -p "$out"
cd "$out"

crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff
etcd=85651fa521731aaecad76ff81dee5450a766c874
raft=f35a02212416d045c574e30aa59d4b07c4b8d732
tikv=134cdb2ce9852aa30e878ade3849f877dd921018
kvp=f8ad079c8e9812ed5900b0c12b5f1d97171464c4
fdb=487620d5aa41152b1f034f4e932005a87eb2947f
jfs=29c60bbbeea410c6c0cb7a49540044e0c099dffb
lu=ee61b3f6dd57dd642c14469fcb9593cd20e7a81d
ceph=b7e6787889ed0783df44063506dcacaaf672758f
linux=7d0a66e4bb9081d75c82ec4957c50034cb0ea449
pjd=85a8aea9e685999ef0540392fd80535f873d7ff7
samba=1e3906f39e95bd283bb9a677327a5d5aac852845
aws=e5e6bbd2771d39e8ce3736849548a94b98d5094b

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

for p in pkg/kv/kvpb/api.proto pkg/kv/kvpb/errors.proto pkg/storage/mvcc.go \
	pkg/kv/kvserver/replica_evaluate.go pkg/kv/kvserver/replica_raft.go \
	pkg/kv/kvserver/kvserverbase/base.go pkg/kv/kvserver/kvserverpb/proposer_kv.proto \
	pkg/kv/kvclient/kvcoord/dist_sender.go pkg/kv/kvserver/addressing.go pkg/keys/constants.go \
	pkg/keys/sql.go pkg/roachpb/data.go pkg/util/encoding/encoding.go \
	docs/RFCS/20160420_proposer_evaluated_kv.md; do
	get cockroachdb/cockroach $crdb $p
done
for p in api/etcdserverpb/rpc.proto server/etcdserver/txn/txn.go \
	server/etcdserver/api/v3rpc/key.go server/embed/config.go; do
	get etcd-io/etcd $etcd $p
done
get etcd-io/raft $raft raft.go
get tikv/tikv $tikv components/raftstore/src/store/config.rs
get pingcap/kvproto $kvp proto/kvrpcpb.proto
get pingcap/kvproto $kvp proto/errorpb.proto
for p in api-common.rst.inc known-limitations.rst largeval.rst; do
	get apple/foundationdb $fdb documentation/sphinx/source/$p
done
get juicedata/juicefs $jfs pkg/meta/base.go
get juicedata/juicefs $jfs pkg/meta/tkv.go
for p in lustre/target/tgt_grant.c lustre/ofd/ofd_obd.c lustre/osc/osc_request.c lustre/osc/osc_cache.c; do
	get lustre/lustre-release $lu $p
done
for p in doc/cephfs/quota.rst src/client/Client.cc src/rgw/rgw_rest.h src/rgw/rgw_rest.cc src/mon/OSDMonitor.cc; do
	get ceph/ceph $ceph $p
done
for p in include/uapi/linux/limits.h fs/fuse/dev.c fs/fuse/fuse_i.h fs/ext4/xattr.c; do
	get torvalds/linux $linux $p
done
for p in tests/symlink/03.t tests/misc.sh tests/mkdir/00.t tests/open/00.t; do
	get pjd/pjdfstest $pjd $p
done
get samba-team/samba $samba docs-xml/manpages/vfs_streams_xattr.8.xml
get awsdocs/amazon-s3-developer-guide $aws doc_source/UsingMetadata.md

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -16
}

c=cockroachdb_cockroach_
show ${c}pkg_kv_kvpb_api.proto "Returns true and sets value if exp_bytes|returns a ConditionFailedError containing|key is expected to not exist|bool allow_if_does_not_exist|increments the value for key, and returns|int64 new_value = 2|message DeleteRangeRequest|bool use_range_tombstone|determine whether a span contains any keys|ascending scan of \[start,end\)|unbounded if zero|Span resume_span = 4|executed in-full according|int64 max_span_request_keys|int64 target_bytes = 15"
show ${c}pkg_storage_mvcc.go "Check for overflow and underflow|IntegerOverflowError\{"
show ${c}pkg_kv_kvserver_replica_evaluate.go "for index, union := range baReqs"
show ${c}pkg_kv_kvserver_replica_raft.go "command is too large"
show ${c}pkg_kv_kvserver_kvserverbase_base.go "MaxCommandSizeDefault = |kv.raft.command.max_size\""
show ${c}pkg_kv_kvserver_kvserverpb_proposer_kv.proto "counter has already reached|so a potential later replay will fail"
show ${c}pkg_kv_kvpb_errors.proto "If the lease holder is known|message NotLeaseHolderError|may have succeeded or"
show ${c}pkg_kv_kvclient_kvcoord_dist_sender.go "case \*kvpb.NotLeaseHolderError|Update the leaseholder in the range cache"
show ${c}pkg_kv_kvserver_addressing.go "meta1 addressing records cannot be split"
show ${c}pkg_roachpb_data.go "localPrefixByte = |LocalMaxByte = "
show ${c}pkg_keys_constants.go "meta2PrefixByte  =|systemPrefixByte =|tenantPrefixByte ="
show ${c}pkg_keys_sql.go "func \(e sqlEncoder\) TablePrefix|EncodeUvarintAscending\(k, uint64\(tableID\)\)"
show ${c}pkg_util_encoding_encoding.go "IntMin      = 0x80|intSmall    =|func EncodeUvarintAscending|escape                   byte = 0x00|escapedTerm              byte = 0x01|escaped00                byte = 0xff|terminated with the sequence"
show ${c}docs_RFCS_20160420_proposer_evaluated_kv.md "produce identical output even during migrations|computes? the effects on the key-value state|compute the effects"

e=etcd-io_etcd_
show ${e}api_etcdserverpb_rpc.proto "range_end is the upper bound on the requested range|treated as no limit|bool more = 3|range_end compares the given target to all keys|From google paxosdb paper|message TxnRequest|success requests will be processed in order"
show ${e}server_etcdserver_txn_txn.go "func applyCompares|func applyCompare|len\(rr.KVs\) == 0|Always fail if comparing a value"
show ${e}server_etcdserver_api_v3rpc_key.go "func checkTxnRequest|ErrGRPCTooManyOps|func checkIntervals|ErrGRPCDuplicateKey"
show ${e}server_embed_config.go "DefaultMaxTxnOps  *=|DefaultMaxRequestBytes  *="
show etcd-io_raft_raft.go "DisableProposalForwarding set to true|DisableProposalForwarding bool|var ErrProposalDropped|not forwarding to leader|no leader at term"

show tikv_tikv_components_raftstore_src_store_config.rs "raft_entry_max_size: ReadableSize::mb\(8\)"
show pingcap_kvproto_proto_kvrpcpb.proto "^enum Op \{|Insert = 4|CheckNotExists = 6|message RawCASRequest|previous value regardless|message RawScanRequest|message RawDeleteRangeRequest"
show pingcap_kvproto_proto_errorpb.proto "message NotLeader|Region leader of the requested region"

f=apple_foundationdb_documentation_sphinx_source_
show ${f}api-common.rst.inc "will not cause the transaction to conflict|ideal for operating on keys that are frequently modified|at a known offset in the value|larger of the existing value"
show ${f}known-limitations.rst "10,000,000 bytes|exceeds one megabyte|Keys cannot exceed 10,000 bytes"
show ${f}largeval.rst "limited to a size of 100 kB|split the values across multiple keys|above 10kb"

show juicedata_juicefs_pkg_meta_base.go "inodeBatch     = 1 << 10|inodeNeedPrefetch = |go m.prefetchInodes|func \(m \*baseMeta\) nextInode|incrCounter\(\"nextInode\", inodeBatch\)"
show juicedata_juicefs_pkg_meta_tkv.go "func \(m \*kvMeta\) updateStats|func \(m \*kvMeta\) doFlushStats|func \(m \*kvMeta\) incrCounter|tx.incrBy\(key, value\)"

l=lustre_lustre-release_lustre_
show ${l}target_tgt_grant.c "static u64 tgt_grant_space_left|tot_granted = tgd->tgd_tot_granted \+ reserved|left -= tot_granted|static long tgt_grant_alloc|curgrant >= want \|\||1/8th of the remaining|left >>= 3|Limit to grant_chunk|less than 25% of left space|void tgt_grant_discard|tgt_grant_check\(env, exp"
show ${l}ofd_ofd_obd.c "tgt_grant_discard\(exp\)|once we're sure no more"
show ${l}osc_osc_request.c "cl_avail_grant / 4|static int osc_shrink_grant\(|enough for a single RPC"
show ${l}osc_osc_cache.c "no grant space, fall back to sync i/o|fall back to synchronous I/O"

g=ceph_ceph_
show ${g}doc_cephfs_quota.rst "Quotas are imprecise|tens of seconds|amount of time, not the amount"
show ${g}src_client_Client.cc "bool Client::is_quota_bytes_exceeded|bool Client::is_quota_bytes_approaching|\(space >> 4\) < size|is_quota_bytes_exceeded\(in, endoff - in->size|is_quota_bytes_approaching\(in, f->actor_perms\)"
show ${g}src_rgw_rgw_rest.h "MAX_OBJ_NAME_LEN = 1024"
show ${g}src_rgw_rgw_rest.cc "at most 1024 bytes long|len > MAX_OBJ_NAME_LEN"
show ${g}src_mon_OSDMonitor.cc "lower bound of N-2|lacks CEPH_FEATURE_SERVER_SQUID"

k=torvalds_linux_
show ${k}include_uapi_linux_limits.h "NAME_MAX|PATH_MAX|XATTR_SIZE_MAX"
show ${k}fs_fuse_dev.c "static u64 fuse_get_unique_locked|fiq->reqctr \+= FUSE_REQ_ID_STEP"
show ${k}fs_fuse_fuse_i.h "^struct fuse_iqueue \{|u64 reqctr;|struct fuse_iqueue iq;"
show ${k}fs_ext4_xattr.c "Check whether we have enough space|free < EXT4_XATTR_LEN\(name_len\) \+ new_size"

p=pjd_pjdfstest_tests_
show ${p}symlink_03.t "exceeded \{PATH_MAX\}|expect 0 symlink \\$\{nx\} \\$\{n0\}"
show ${p}misc.sh "^dirgen_max|path_max -= 1"
show ${p}mkdir_00.t "st_mtime fields of the directory that contains|stat \. mtime"
show ${p}open_00.t "Update parent directory ctime/mtime"

show samba-team_samba_docs-xml_manpages_vfs_streams_xattr.8.xml "severe limitations|Linux has a limit|of 64kB per xattr"
show awsdocs_amazon-s3-developer-guide_doc_source_UsingMetadata.md "at most 1,024 bytes long"

r=$(cd "$here/../.." && pwd)
show "$r/internal/lsm/tree.go" "MaxKey = 1024|MaxValue = 1000|return k <= MaxKey"
show "$r/internal/lsm/memtable.go" "entries are at most half a page|func \(m \*memtable\) leafEntry"
show "$r/internal/lsm/page.go" "pageSize  = 4096|pageHdr   = 16|pageCap   = "
