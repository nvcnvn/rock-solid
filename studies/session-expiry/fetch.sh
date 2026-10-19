#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl, and pdftotext for the Chubby paper.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

ceph=b7e6787889ed0783df44063506dcacaaf672758f
linux=v6.18
zk=48f7cb00e2d48d4563e45bebcd0f4665c30772a2
etcd=22cc5d9acd4a5953dfdfc2594dbeb53da89bb0d4
hadoop=1097744bc490b65cad5ab2fbeafff35687971357
k8s=1fc565c2f557823a911f073dc577945230f27dd7
lustre=ee61b3f6dd57dd642c14469fcb9593cd20e7a81d
pwl=9b92363eac4d4fbea0f5fdc7766ef68d22037ca8

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get ceph/ceph $ceph doc/cephfs/eviction.rst
get ceph/ceph $ceph src/mds/Server.cc
get ceph/ceph $ceph src/mds/MDSRank.cc
get ceph/ceph $ceph src/mds/MDSMap.h
get ceph/ceph $ceph src/client/Client.cc
get ceph/ceph $ceph src/osd/OSD.cc
get ceph/ceph $ceph src/osd/PrimaryLogPG.cc
get ceph/ceph $ceph src/common/options/mds.yaml.in
get ceph/ceph $ceph src/common/options/mon.yaml.in
get torvalds/linux $linux fs/nfsd/nfs4state.c
get torvalds/linux $linux fs/fuse/dir.c
get apache/zookeeper $zk zookeeper-server/src/main/java/org/apache/zookeeper/server/SessionTrackerImpl.java
get apache/zookeeper $zk zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/LearnerSessionTracker.java
get apache/zookeeper $zk zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/LearnerHandler.java
get etcd-io/etcd $etcd server/lease/lessor.go
get etcd-io/etcd $etcd server/etcdserver/server.go
get etcd-io/etcd $etcd contrib/lock/README.md
get apache/hadoop $hadoop hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
get apache/hadoop $hadoop hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
get apache/hadoop $hadoop hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java
get kubernetes/website $k8s content/en/docs/concepts/architecture/leases.md
get kubernetes/website $k8s content/en/docs/concepts/architecture/nodes.md
get kubernetes/website $k8s content/en/docs/concepts/cluster-administration/node-shutdown.md
get lustre/lustre-release $lustre lustre/ldlm/ldlm_lockd.c
get lustre/lustre-release $lustre lustre/ptlrpc/pinger.c
get lustre/lustre-release $lustre lustre/include/obd_support.h
get lustre/lustre-release $lustre lustre/obdclass/class_obd.c
get papers-we-love/papers-we-love $pwl distributed_systems/the-chubby-lock-service-for-loosely-coupled-distributed-systems.pdf
chubby=papers-we-love_papers-we-love_distributed_systems_the-chubby-lock-service-for-loosely-coupled-distributed-systems
[ -s "$chubby.txt" ] || pdftotext "$chubby.pdf" "$chubby.txt"

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -6
}

show ceph_ceph_src_mds_Server.cc "don't kick clients if we've been laggy|last_cap_renew_span >= mds->mdsmap->get_session_autoclose|mds_session_blocklist_on_timeout"
show ceph_ceph_src_mds_MDSRank.cc "osd blocklist|wait_for_latest_osdmap|set_osd_epoch_barrier\(epoch\)"
show ceph_ceph_src_mds_MDSMap.h "__u32 session_(timeout|autoclose) ="
show ceph_ceph_src_mds_MDSRank.cc "void MDSRank::apply_blocklist"
show ceph_ceph_src_client_Client.cc "get_session_timeout\(\) / 3.0|last_cap_renew_request \+ mdsmap->get_session_timeout"
show ceph_ceph_src_osd_OSD.cc "m->get_min_epoch\(\) > osdmap->get_epoch\(\)"
show ceph_ceph_src_osd_PrimaryLogPG.cc "is_blocklisted\(m->get_source_addr"
show ceph_ceph_src_common_options_mds.yaml.in "name: (mds_session_blocklist_on_timeout|mds_cap_revoke_eviction_timeout|mds_reconnect_timeout)"
show ceph_ceph_src_common_options_mon.yaml.in "name: mon_osd_blocklist_default_expire"
show ceph_ceph_doc_cephfs_eviction.rst "propagates it to the others|epoch barrier is transmitted"
show torvalds_linux_fs_nfsd_nfs4state.c "clp->cl_time = ktime_get_boottime_seconds|cutoff = ktime_get_boottime_seconds\(\) - nn->nfsd4_lease|NFSD4_COURTESY;"
show torvalds_linux_fs_fuse_dir.c "return get_jiffies_64\(\) \+ timespec64_to_jiffies"
show apache_zookeeper_zookeeper-server_src_main_java_org_apache_zookeeper_server_SessionTrackerImpl.java "trackSession\(e.getKey|expirer.expire"
show apache_zookeeper_zookeeper-server_src_main_java_org_apache_zookeeper_server_quorum_LearnerSessionTracker.java "passed along"
show apache_zookeeper_zookeeper-server_src_main_java_org_apache_zookeeper_server_quorum_LearnerHandler.java "learnerMaster.touch"
show etcd-io_etcd_server_lease_lessor.go "two primary|func \(le \*lessor\) Promote|l.refresh\(extend\)"
show etcd-io_etcd_server_etcdserver_server.go "lessor.Promote\(s.Cfg.ElectionTimeout"
show apache_hadoop_hadoop-hdfs-project_hadoop-hdfs_src_main_java_org_apache_hadoop_hdfs_server_namenode_LeaseManager.java "A Lease governs all the locks"
show apache_hadoop_hadoop-hdfs-project_hadoop-hdfs_src_main_java_org_apache_hadoop_hdfs_server_namenode_FSNamesystem.java "leaseManager.renewAllLeases"
show kubernetes_website_content_en_docs_concepts_architecture_leases.md "every kubelet heartbeat is an"
show kubernetes_website_content_en_docs_concepts_cluster-administration_node-shutdown.md "might encounter data corruption|out-of-service"
show lustre_lustre-release_lustre_ldlm_ldlm_lockd.c "lock callback timer expired"
show lustre_lustre-release_lustre_ptlrpc_pinger.c "I think it's dead"
show etcd-io_etcd_server_lease_lessor.go "defaultLeaseCheckpointInterval =|defaultLeaseRevokeRate =|return -1, ErrNotPrimary"
show apache_hadoop_hadoop-hdfs-project_hadoop-hdfs-client_src_main_java_org_apache_hadoop_hdfs_client_impl_LeaseRenewer.java "renewal = HdfsConstants.LEASE_SOFTLIMIT_PERIOD / 2"
show kubernetes_website_content_en_docs_concepts_architecture_nodes.md "waits 5 minutes"
show kubernetes_website_content_en_docs_concepts_cluster-administration_node-shutdown.md "has not succeeded for 6 minutes"
show lustre_lustre-release_lustre_obdclass_class_obd.c "ping_interval = "
show lustre_lustre-release_lustre_ptlrpc_pinger.c "^static int suppress_pings"
show lustre_lustre-release_lustre_include_obd_support.h "define PING_EVICT_TIMEOUT"
show "$chubby.txt" "default extension is 12s|conservative approximation|45s by default|extended to the maximum|90,000 clients|lock-delay. Clients"
